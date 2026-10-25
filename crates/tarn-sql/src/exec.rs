//! Execution of SELECT plans: reading rows through access paths (inside a
//! transaction's streams, or as one lock-aware autocommit read), joins,
//! filtering, aggregation, sorting, projection, DISTINCT and LIMIT.

use crate::catalog::{IndexDef, TableDef};
use crate::cluster::Cluster;
use crate::expr::{EvalCtx, Expr};
use crate::plan::*;
use crate::txn::{select_rows, RangeRead, Row, Txn};
use crate::types::{key_val, sort_cmp, to_num, BaseType, KeyVal, SqlType};
use bytes::Bytes;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use tarn_common::{Column, SqlError, SqlResult, Value};
use tarn_iproto::Iter;

/// Error code used internally to say "this statement needs a transaction":
/// an autocommit read touched more than one shard or needed more than one
/// storage call, so it is re-run as an implicit read-only transaction.
pub const NEED_TXN: u16 = 65000;

pub fn need_txn() -> SqlError {
    SqlError::new(NEED_TXN, "HY000", "internal: statement needs a transaction")
}

pub enum ReadMode<'a> {
    /// Reads inside the transaction's streams (tracked, overlaid).
    Txn(&'a mut Txn),
    /// One lock-aware autocommit read (TarnCommit!SnapshotRead).
    Auto { calls: usize },
}

/// Convert a key value to the exact type stored in the index column, or
/// None if no stored value can equal it (e.g. 2.5 for an INT column).
pub fn coerce_key_part(ty: &SqlType, v: &Value) -> Option<Value> {
    if v.is_null() {
        return Some(Value::Null);
    }
    if ty.is_integer() {
        let i: i128 = match v {
            Value::Int(i) => *i as i128,
            Value::UInt(u) => *u as i128,
            Value::Float(f) if f.fract() == 0.0 && f.is_finite() => *f as i128,
            Value::Decimal(d) => {
                let f: f64 = d.parse().ok()?;
                if f.fract() != 0.0 {
                    return None;
                }
                f as i128
            }
            Value::Str(b) | Value::Bin(b) => {
                let s = std::str::from_utf8(b).ok()?.trim();
                match s.parse::<i128>() {
                    Ok(i) => i,
                    Err(_) => {
                        let f: f64 = s.parse().ok()?;
                        if f.fract() != 0.0 {
                            return None;
                        }
                        f as i128
                    }
                }
            }
            _ => return None,
        };
        if ty.unsigned {
            if i < 0 || i > u64::MAX as i128 {
                return None;
            }
            return Some(Value::UInt(i as u64));
        }
        if i < i64::MIN as i128 || i > i64::MAX as i128 {
            return None;
        }
        return Some(Value::Int(i as i64));
    }
    match ty.base {
        BaseType::Float | BaseType::Double => to_num(v).map(Value::Float),
        BaseType::Decimal => match v {
            Value::Decimal(_) => Some(v.clone()),
            Value::Int(i) => Some(Value::Decimal(i.to_string())),
            Value::UInt(u) => Some(Value::Decimal(u.to_string())),
            other => to_num(other).map(|f| Value::Decimal(format!("{}", f))),
        },
        BaseType::Binary | BaseType::VarBinary | BaseType::Blob => Some(Value::Bin(v.to_text().unwrap_or_default())),
        _ => Some(match v {
            Value::Str(b) | Value::Bin(b) => Value::Str(b.clone()),
            other => Value::Str(other.to_text().unwrap_or_default()),
        }),
    }
}

fn index_def(t: &TableDef, id: u32) -> &IndexDef {
    t.indexes.iter().find(|i| i.id == id).unwrap_or(&t.indexes[0])
}

/// Shard owning every row whose `index` key starts with `prefix`, if the
/// prefix determines the shard key.
pub fn route_prefix(cluster: &Cluster, t: &TableDef, idx: &IndexDef, prefix: &[Value]) -> Option<usize> {
    if cluster.nshards() == 1 {
        return Some(0);
    }
    let mut sk = Vec::with_capacity(t.shard_key.len());
    for &c in &t.shard_key {
        let p = idx.cols.iter().position(|&x| x == c)?;
        sk.push(prefix.get(p)?.clone());
    }
    Some(cluster.shard_of(&sk))
}

pub fn shard_of_row(cluster: &Cluster, t: &TableDef, row: &[Value]) -> usize {
    let sk: Vec<Value> = t.shard_key.iter().map(|&c| row[c].clone()).collect();
    cluster.shard_of(&sk)
}

fn eval_limit(e: &Option<Expr>, cx: &EvalCtx) -> SqlResult<Option<u64>> {
    match e {
        None => Ok(None),
        Some(e) => {
            let v = e.eval(&[], cx)?;
            let n = v.as_i64().ok_or_else(|| SqlError::parse("LIMIT must be an integer"))?;
            Ok(Some(n.max(0) as u64))
        }
    }
}

struct Call {
    shard: usize,
    key: Vec<Value>,
    iter: Iter,
    limit: Option<u32>,
    end: Option<(Vec<Value>, bool)>,
    range: Option<RangeRead>,
}

/// Read the rows of one table access (for the outer row `outer`).
pub async fn fetch(
    cluster: &Arc<Cluster>,
    mode: &mut ReadMode<'_>,
    ta: &TableAccess,
    outer: &[Value],
    cx: &EvalCtx<'_>,
) -> SqlResult<Vec<Row>> {
    let t = &ta.table;
    let limit = eval_limit(&ta.limit, cx)?.map(|l| l.min(u32::MAX as u64) as u32);
    let mut calls: Vec<Call> = Vec::new();
    let mut unique_point = false;
    match &ta.path {
        AccessPath::Point { index, keys } => {
            let idx = index_def(t, *index);
            unique_point = true;
            // `col IN (1, 1, 2)` must still return each row once.
            let mut seen: std::collections::HashSet<Bytes> = std::collections::HashSet::new();
            'keys: for k in keys {
                let mut key = Vec::with_capacity(k.len());
                for (i, e) in k.iter().enumerate() {
                    let v = e.eval(outer, cx)?;
                    if v.is_null() {
                        continue 'keys; // NULL never equals anything
                    }
                    match coerce_key_part(t.columns[idx.cols[i]].ty(), &v) {
                        Some(x) => key.push(x),
                        None => continue 'keys,
                    }
                }
                if keys.len() > 1 && !seen.insert(crate::txn::key_bytes(&key)) {
                    continue;
                }
                let shard = route_prefix(cluster, t, idx, &key).unwrap_or(0);
                calls.push(Call {
                    shard,
                    key,
                    iter: Iter::Eq,
                    limit: Some(1),
                    end: None,
                    range: None,
                });
            }
        }
        AccessPath::Range {
            index,
            eq,
            lo,
            hi,
            reverse: _,
        } => {
            let idx = index_def(t, *index);
            let mut prefix = Vec::with_capacity(eq.len() + 1);
            for (i, e) in eq.iter().enumerate() {
                let v = e.eval(outer, cx)?;
                match (v.is_null(), coerce_key_part(t.columns[idx.cols[i]].ty(), &v)) {
                    (false, Some(x)) => prefix.push(x),
                    _ => return Ok(overlay(mode, t, Vec::new())),
                }
            }
            let next_ty = idx.cols.get(eq.len()).map(|&c| t.columns[c].ty());
            let bound = |b: &Option<(Expr, bool)>| -> SqlResult<Option<(Value, bool)>> {
                match (b, next_ty) {
                    (Some((e, incl)), Some(ty)) => {
                        let v = e.eval(outer, cx)?;
                        if v.is_null() {
                            return Ok(Some((Value::Null, false))); // comparisons with NULL match nothing
                        }
                        // An inexact bound is dropped: the residual filter keeps exact semantics.
                        Ok(coerce_key_part(ty, &v).map(|x| (x, *incl)))
                    }
                    _ => Ok(None),
                }
            };
            let lo_v = bound(lo)?;
            let hi_v = bound(hi)?;
            if matches!(lo_v, Some((Value::Null, false))) || matches!(hi_v, Some((Value::Null, false))) {
                return Ok(overlay(mode, t, Vec::new()));
            }
            let (key, iter, lo_incl) = match &lo_v {
                Some((v, incl)) => {
                    let mut k = prefix.clone();
                    k.push(v.clone());
                    (k, if *incl { Iter::Ge } else { Iter::Gt }, *incl)
                }
                None => (
                    prefix.clone(),
                    if prefix.is_empty() { Iter::All } else { Iter::Ge },
                    true,
                ),
            };
            let end = match &hi_v {
                Some((v, incl)) => {
                    let mut k = prefix.clone();
                    k.push(v.clone());
                    Some((k, *incl))
                }
                None if !prefix.is_empty() => Some((prefix.clone(), true)),
                None => None,
            };
            let shards: Vec<usize> = match route_prefix(cluster, t, idx, &prefix) {
                Some(s) => vec![s],
                None => (0..cluster.nshards()).collect(),
            };
            let range = RangeRead {
                space: t.space_id,
                index: idx.id,
                lo: if key.is_empty() { None } else { Some(key.clone()) },
                hi: end.as_ref().map(|(k, _)| k.clone()),
                lo_incl,
                hi_incl: end.as_ref().map(|(_, i)| *i).unwrap_or(true),
            };
            for s in shards {
                calls.push(Call {
                    shard: s,
                    key: key.clone(),
                    iter,
                    limit,
                    end: end.clone(),
                    range: Some(range.clone()),
                });
            }
        }
        AccessPath::Full => {
            let range = RangeRead {
                space: t.space_id,
                index: 0,
                lo: None,
                hi: None,
                lo_incl: true,
                hi_incl: true,
            };
            for s in 0..cluster.nshards() {
                calls.push(Call {
                    shard: s,
                    key: Vec::new(),
                    iter: Iter::All,
                    limit,
                    end: None,
                    range: Some(range.clone()),
                });
            }
        }
    }
    let index = match &ta.path {
        AccessPath::Point { index, .. } | AccessPath::Range { index, .. } => *index,
        AccessPath::Full => 0,
    };
    let mut rows = Vec::new();
    match mode {
        ReadMode::Auto { calls: made } => {
            *made += calls.len();
            if *made > 1 {
                return Err(need_txn());
            }
            for c in calls {
                let client = cluster.shards[c.shard].client(cluster).await?;
                let end = c.end.as_ref().map(|(k, i)| (k.as_slice(), *i));
                rows.extend(select_rows(&client, 0, t, index, &c.key, c.iter, c.limit, end, true, false).await?);
            }
        }
        ReadMode::Txn(txn) => {
            let mut futs = Vec::new();
            for c in &calls {
                let st = txn.stream(cluster, c.shard).await?;
                futs.push(async move {
                    let end = c.end.as_ref().map(|(k, i)| (k.as_slice(), *i));
                    select_rows(
                        &st.client, st.id, t, index, &c.key, c.iter, c.limit, end, false, st.begin,
                    )
                    .await
                });
            }
            let results = futures::future::join_all(futs).await;
            for r in results {
                match r {
                    Ok(v) => rows.extend(v),
                    Err(e) => {
                        if e.code == 1213 {
                            txn.doomed = Some(e.clone());
                        }
                        return Err(e);
                    }
                }
            }
            for c in calls {
                txn.mark_begun(c.shard);
                if unique_point {
                    txn.note_point(c.shard, t.space_id, index, c.key);
                } else if let Some(r) = c.range {
                    txn.note_range(c.shard, r);
                }
            }
        }
    }
    Ok(overlay(mode, t, rows))
}

fn overlay(mode: &ReadMode<'_>, t: &TableDef, rows: Vec<Row>) -> Vec<Row> {
    match mode {
        ReadMode::Txn(txn) => txn.overlay(t, rows),
        ReadMode::Auto { .. } => rows,
    }
}

/// All rows produced by the FROM clause (joins applied, WHERE not yet).
async fn source_rows(
    cluster: &Arc<Cluster>,
    mode: &mut ReadMode<'_>,
    src: &Source,
    cx: &EvalCtx<'_>,
) -> SqlResult<Vec<Row>> {
    // Flatten the left-deep join tree.
    let mut steps: Vec<(&TableAccess, JoinKind, Option<&Expr>)> = Vec::new();
    let mut cur = src;
    loop {
        match cur {
            Source::Dual => return Ok(vec![Vec::new()]),
            Source::Table(t) => {
                steps.push((t, JoinKind::Inner, None));
                break;
            }
            Source::Join { left, right, kind, on } => {
                steps.push((right, *kind, on.as_ref()));
                cur = left;
            }
        }
    }
    steps.reverse();
    let mut rows = fetch(cluster, mode, steps[0].0, &[], cx).await?;
    for (ta, kind, on) in steps.iter().skip(1) {
        let width = ta.offset + ta.table.columns.len();
        let mut out = Vec::new();
        for left in rows {
            let right = fetch(cluster, mode, ta, &left, cx).await?;
            let mut matched = false;
            for r in right {
                let mut row = left.clone();
                row.resize(ta.offset, Value::Null);
                row.extend(r);
                if let Some(on) = on {
                    if !on.eval_bool(&row, cx)? {
                        continue;
                    }
                }
                matched = true;
                out.push(row);
            }
            if !matched && *kind == JoinKind::Left {
                let mut row = left;
                row.resize(width, Value::Null);
                out.push(row);
            }
        }
        rows = out;
    }
    Ok(rows)
}

enum AggState {
    Count(u64),
    SumInt(i128, bool),
    SumFloat(f64, bool),
    SumDec(f64, u32, bool),
    Avg(f64, u64, u32, bool),
    Min(Option<Value>),
    Max(Option<Value>),
    Any(Option<Value>),
    /// GROUP_CONCAT: (sort keys, text) per input row.
    Concat(Vec<(Vec<Value>, Bytes)>),
}

fn dec_scale(v: &Value) -> u32 {
    match v {
        Value::Decimal(d) => d.split_once('.').map(|(_, f)| f.len() as u32).unwrap_or(0),
        _ => 0,
    }
}

impl AggState {
    fn new(a: &AggCall) -> AggState {
        match a.func {
            AggFunc::CountStar | AggFunc::Count => AggState::Count(0),
            AggFunc::Sum => AggState::SumInt(0, false),
            AggFunc::Avg => AggState::Avg(0.0, 0, 0, false),
            AggFunc::Min => AggState::Min(None),
            AggFunc::Max => AggState::Max(None),
            AggFunc::AnyValue => AggState::Any(None),
            AggFunc::GroupConcat => AggState::Concat(Vec::new()),
        }
    }

    fn add(&mut self, func: AggFunc, v: Option<Value>) {
        let v = match (func, v) {
            (AggFunc::CountStar, _) => {
                if let AggState::Count(n) = self {
                    *n += 1;
                }
                return;
            }
            (_, None) => return,
            (_, Some(v)) => v,
        };
        if v.is_null() && func != AggFunc::AnyValue {
            return;
        }
        match self {
            AggState::Count(n) => *n += 1,
            AggState::SumInt(s, any) => match &v {
                Value::Int(i) => {
                    *s += *i as i128;
                    *any = true;
                }
                Value::UInt(u) => {
                    *s += *u as i128;
                    *any = true;
                }
                Value::Decimal(_) => {
                    *self = AggState::SumDec(*s as f64 + to_num(&v).unwrap_or(0.0), dec_scale(&v), true);
                }
                other => {
                    *self = AggState::SumFloat(*s as f64 + to_num(other).unwrap_or(0.0), true);
                }
            },
            AggState::SumFloat(s, any) => {
                *s += to_num(&v).unwrap_or(0.0);
                *any = true;
            }
            AggState::SumDec(s, sc, any) => {
                *s += to_num(&v).unwrap_or(0.0);
                *sc = (*sc).max(dec_scale(&v));
                *any = true;
            }
            AggState::Avg(s, n, sc, float) => {
                *s += to_num(&v).unwrap_or(0.0);
                *n += 1;
                *sc = (*sc).max(dec_scale(&v));
                if matches!(v, Value::Float(_) | Value::Str(_)) {
                    *float = true;
                }
            }
            AggState::Min(m) => {
                if m.as_ref().is_none_or(|x| sort_cmp(&v, x).is_lt()) {
                    *m = Some(v);
                }
            }
            AggState::Max(m) => {
                if m.as_ref().is_none_or(|x| sort_cmp(&v, x).is_gt()) {
                    *m = Some(v);
                }
            }
            AggState::Any(m) => {
                if m.is_none() {
                    *m = Some(v);
                }
            }
            AggState::Concat(parts) => parts.push((Vec::new(), v.to_text().unwrap_or_default())),
        }
    }

    fn finish(self, a: &AggCall) -> Value {
        match self {
            AggState::Count(n) => Value::Int(n as i64),
            AggState::SumInt(s, any) => {
                if any {
                    Value::Decimal(s.to_string())
                } else {
                    Value::Null
                }
            }
            AggState::SumFloat(s, any) => {
                if any {
                    Value::Float(s)
                } else {
                    Value::Null
                }
            }
            AggState::SumDec(s, sc, any) => {
                if any {
                    Value::Decimal(format!("{:.*}", sc as usize, s))
                } else {
                    Value::Null
                }
            }
            AggState::Avg(s, n, sc, float) => {
                if n == 0 {
                    Value::Null
                } else if float {
                    Value::Float(s / n as f64)
                } else {
                    Value::Decimal(format!("{:.*}", (sc + 4) as usize, s / n as f64))
                }
            }
            AggState::Min(m) | AggState::Max(m) | AggState::Any(m) => m.unwrap_or(Value::Null),
            AggState::Concat(mut parts) => {
                if parts.is_empty() {
                    Value::Null
                } else {
                    if !a.order.is_empty() {
                        parts.sort_by(|(x, _), (y, _)| {
                            for (i, (_, desc)) in a.order.iter().enumerate() {
                                let o = sort_cmp(&x[i], &y[i]);
                                let o = if *desc { o.reverse() } else { o };
                                if o.is_ne() {
                                    return o;
                                }
                            }
                            std::cmp::Ordering::Equal
                        });
                    }
                    let sep: &[u8] = a.sep.as_deref().unwrap_or(b",");
                    let mut out = Vec::new();
                    for (i, (_, t)) in parts.iter().enumerate() {
                        if i > 0 {
                            out.extend_from_slice(sep);
                        }
                        out.extend_from_slice(t);
                    }
                    Value::Str(Bytes::from(out))
                }
            }
        }
    }
}

fn aggregate(rows: Vec<Row>, ap: &AggPlan, cx: &EvalCtx) -> SqlResult<Vec<Row>> {
    let mut order: Vec<Vec<KeyVal>> = Vec::new();
    let mut groups: HashMap<Vec<KeyVal>, (Vec<Value>, Vec<AggState>, Vec<HashSet<KeyVal>>)> = HashMap::new();
    for row in &rows {
        let gv: Vec<Value> = ap.group.iter().map(|g| g.eval(row, cx)).collect::<SqlResult<_>>()?;
        let gk: Vec<KeyVal> = gv.iter().map(key_val).collect();
        let entry = groups.entry(gk.clone()).or_insert_with(|| {
            order.push(gk);
            (
                gv,
                ap.aggs.iter().map(AggState::new).collect(),
                ap.aggs.iter().map(|_| HashSet::new()).collect(),
            )
        });
        for (i, a) in ap.aggs.iter().enumerate() {
            let v = match &a.arg {
                Some(e) => Some(e.eval(row, cx)?),
                None => None,
            };
            if a.distinct {
                if let Some(x) = &v {
                    if x.is_null() || !entry.2[i].insert(key_val(x)) {
                        continue;
                    }
                }
            }
            if a.func == AggFunc::GroupConcat && !a.order.is_empty() {
                if let Some(x) = v {
                    if !x.is_null() {
                        let keys: Vec<Value> =
                            a.order.iter().map(|(e, _)| e.eval(row, cx)).collect::<SqlResult<_>>()?;
                        if let AggState::Concat(parts) = &mut entry.1[i] {
                            parts.push((keys, x.to_text().unwrap_or_default()));
                        }
                    }
                }
                continue;
            }
            entry.1[i].add(a.func, v);
        }
    }
    if groups.is_empty() && ap.group.is_empty() {
        // aggregate over an empty input still yields one row
        let row: Vec<Value> = ap.aggs.iter().map(|a| AggState::new(a).finish(a)).collect();
        return Ok(vec![row]);
    }
    let mut out = Vec::with_capacity(order.len());
    for k in order {
        let (gv, states, _) = groups.remove(&k).unwrap();
        let mut row = gv;
        row.extend(states.into_iter().zip(ap.aggs.iter()).map(|(s, a)| s.finish(a)));
        out.push(row);
    }
    Ok(out)
}

/// Substitute statement parameters into column names cached as templates.
pub fn render_columns(cols: &Arc<[Column]>, params: &[Value]) -> Arc<[Column]> {
    if params.is_empty() || !cols.iter().any(|c| c.name.contains('?')) {
        return cols.clone();
    }
    cols.iter()
        .map(|c| {
            let mut c = c.clone();
            c.name = render_name(&c.name, params);
            c
        })
        .collect::<Vec<_>>()
        .into()
}

pub fn render_name(s: &str, params: &[Value]) -> String {
    let b = s.as_bytes();
    // A bare literal is named by its value (SELECT 'abc' -> abc); inside an
    // expression, string literals keep their quotes as typed (LOWER('ABC')).
    let bare = b.len() > 1 && b[0] == b'?' && b[1..].iter().all(u8::is_ascii_digit);
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'?' {
            let mut j = i + 1;
            while j < b.len() && b[j].is_ascii_digit() {
                j += 1;
            }
            if let Ok(n) = s[i + 1..j].parse::<usize>() {
                if let Some(v) = n.checked_sub(1).and_then(|k| params.get(k)) {
                    match v {
                        Value::Str(x) if bare => out.push_str(&String::from_utf8_lossy(x)),
                        Value::Str(x) => {
                            out.push('\'');
                            out.push_str(&String::from_utf8_lossy(x).replace('\'', "''"));
                            out.push('\'');
                        }
                        other => out.push_str(&other.to_string()),
                    }
                    i = j;
                    continue;
                }
            }
        }
        let ch = s[i..].chars().next().unwrap();
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

/// Execute a SELECT and return its projected rows.
pub async fn run_select(
    cluster: &Arc<Cluster>,
    mode: &mut ReadMode<'_>,
    p: &SelectPlan,
    cx: &EvalCtx<'_>,
) -> SqlResult<Vec<Row>> {
    let mut rows = source_rows(cluster, mode, &p.source, cx).await?;
    if let Some(f) = &p.filter {
        let mut kept = Vec::with_capacity(rows.len());
        for r in rows {
            if f.eval_bool(&r, cx)? {
                kept.push(r);
            }
        }
        rows = kept;
    }
    if let Some(ap) = &p.agg {
        rows = aggregate(rows, ap, cx)?;
    }
    if let Some(h) = &p.having {
        let mut kept = Vec::with_capacity(rows.len());
        for r in rows {
            if h.eval_bool(&r, cx)? {
                kept.push(r);
            }
        }
        rows = kept;
    }
    if !p.order.is_empty() {
        let mut keyed: Vec<(Vec<Value>, Row)> = Vec::with_capacity(rows.len());
        for r in rows {
            let k: Vec<Value> = p.order.iter().map(|(e, _)| e.eval(&r, cx)).collect::<SqlResult<_>>()?;
            keyed.push((k, r));
        }
        keyed.sort_by(|a, b| {
            for (i, (_, desc)) in p.order.iter().enumerate() {
                let o = sort_cmp(&a.0[i], &b.0[i]);
                let o = if *desc { o.reverse() } else { o };
                if o != std::cmp::Ordering::Equal {
                    return o;
                }
            }
            std::cmp::Ordering::Equal
        });
        rows = keyed.into_iter().map(|(_, r)| r).collect();
    }
    let offset = eval_limit(&p.offset, cx)?.unwrap_or(0) as usize;
    let limit = eval_limit(&p.limit, cx)?;
    let mut out: Vec<Row> = Vec::new();
    let mut seen: HashSet<Vec<KeyVal>> = HashSet::new();
    let mut skipped = 0usize;
    for r in rows {
        let pr: Row = p.project.iter().map(|e| e.eval(&r, cx)).collect::<SqlResult<_>>()?;
        if p.distinct && !seen.insert(pr.iter().map(key_val).collect()) {
            continue;
        }
        if skipped < offset {
            skipped += 1;
            continue;
        }
        if let Some(l) = limit {
            if out.len() as u64 >= l {
                break;
            }
        }
        out.push(pr);
    }
    Ok(out)
}

/// Rows of a single table matching an UPDATE/DELETE (access + filter, then
/// ORDER BY/LIMIT), read through the transaction.
pub async fn matching_rows(
    cluster: &Arc<Cluster>,
    txn: &mut Txn,
    access: &TableAccess,
    filter: &Option<Expr>,
    order: &[(Expr, bool)],
    limit: &Option<Expr>,
    cx: &EvalCtx<'_>,
) -> SqlResult<Vec<Row>> {
    let mut mode = ReadMode::Txn(txn);
    let rows = fetch(cluster, &mut mode, access, &[], cx).await?;
    let mut out = Vec::new();
    for r in rows {
        if match filter {
            Some(f) => f.eval_bool(&r, cx)?,
            None => true,
        } {
            out.push(r);
        }
    }
    if !order.is_empty() {
        out.sort_by(|a, b| {
            for (e, desc) in order {
                let (x, y) = (
                    e.eval(a, cx).unwrap_or(Value::Null),
                    e.eval(b, cx).unwrap_or(Value::Null),
                );
                let o = sort_cmp(&x, &y);
                let o = if *desc { o.reverse() } else { o };
                if o != std::cmp::Ordering::Equal {
                    return o;
                }
            }
            std::cmp::Ordering::Equal
        });
    }
    if let Some(l) = eval_limit(limit, cx)? {
        out.truncate(l as usize);
    }
    // keep deterministic shard grouping for the write buffer
    let _ = BTreeMap::<usize, ()>::new();
    Ok(out)
}
