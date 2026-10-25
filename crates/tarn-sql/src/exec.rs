//! Execution of SELECT plans: reading rows through access paths (inside a
//! transaction's streams, or as one lock-aware autocommit read), joins,
//! filtering, aggregation, sorting, projection, DISTINCT and LIMIT.

use crate::catalog::{IndexDef, TableDef};
use crate::cluster::Cluster;
use crate::expr::{EvalCtx, Expr};
use crate::plan::*;
use crate::txn::{select_keys, select_rows, LockKey, LockOpts, Locked, RangeRead, Row, Txn};
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
    /// READ COMMITTED (TarnCommit!RCRead): every storage read sees the latest
    /// committed state -- waiting out prepared transactions' W locks, as
    /// autocommit reads do -- and is neither tracked nor validated; the
    /// transaction's own buffered writes are overlaid.
    Committed(&'a mut Txn),
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

pub(crate) fn eval_limit(e: &Option<Expr>, cx: &EvalCtx) -> SqlResult<Option<u64>> {
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

/// Lexicographic order of index keys (as Tarantool orders them).
fn key_cmp(a: &[Value], b: &[Value]) -> std::cmp::Ordering {
    for (x, y) in a.iter().zip(b) {
        let o = sort_cmp(x, y);
        if o != std::cmp::Ordering::Equal {
            return o;
        }
    }
    a.len().cmp(&b.len())
}

/// Evaluate the keys of a multi-key access: NULLs and values no stored key
/// can equal are dropped, duplicates removed, and the keys sorted, so rows
/// come back in index order as with MySQL's range access.
fn eval_keys(
    t: &TableDef,
    idx: &IndexDef,
    keys: &[Vec<Expr>],
    outer: &[Value],
    cx: &EvalCtx,
) -> SqlResult<Vec<Vec<Value>>> {
    let mut out = Vec::with_capacity(keys.len());
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
        out.push(key);
    }
    out.sort_by(|a, b| key_cmp(a, b));
    out.dedup_by(|a, b| key_cmp(a, b) == std::cmp::Ordering::Equal);
    Ok(out)
}

/// Read several keys of one index with one storage call per shard (issue
/// #10): each key goes to the shard it determines, or to every shard when
/// it does not contain the shard key (secondary indexes are per shard).
#[allow(clippy::too_many_arguments)]
async fn fetch_keys(
    cluster: &Arc<Cluster>,
    mode: &mut ReadMode<'_>,
    t: &TableDef,
    idx: &IndexDef,
    keys: Vec<Vec<Value>>,
    unique: bool,
    limit: Option<u32>,
) -> SqlResult<Vec<Row>> {
    let mut per_shard: BTreeMap<usize, Vec<Vec<Value>>> = BTreeMap::new();
    for k in keys {
        match route_prefix(cluster, t, idx, &k) {
            Some(s) => per_shard.entry(s).or_default().push(k),
            None => {
                for s in 0..cluster.nshards() {
                    per_shard.entry(s).or_default().push(k.clone());
                }
            }
        }
    }
    let mut rows = Vec::new();
    match mode {
        ReadMode::Auto { calls: made } => {
            *made += per_shard.len();
            if *made > 1 {
                return Err(need_txn());
            }
            for (s, ks) in &per_shard {
                let client = cluster.shards[*s].client(cluster).await?;
                rows.extend(select_keys(&client, 0, t, idx.id, ks, limit, true, false).await?);
            }
        }
        ReadMode::Committed(_) => {
            let mut futs = Vec::new();
            for (s, ks) in &per_shard {
                let client = cluster.shards[*s].client(cluster).await?;
                futs.push(async move { select_keys(&client, 0, t, idx.id, ks, limit, true, false).await });
            }
            for r in futures::future::join_all(futs).await {
                rows.extend(r?);
            }
        }
        ReadMode::Txn(txn) => {
            let mut futs = Vec::new();
            for (s, ks) in &per_shard {
                let st = txn.stream(cluster, *s).await?;
                futs.push(async move { select_keys(&st.client, st.id, t, idx.id, ks, limit, false, st.begin).await });
            }
            for r in futures::future::join_all(futs).await {
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
            for (s, ks) in per_shard.iter() {
                txn.mark_begun(*s);
                for k in ks {
                    if unique {
                        txn.note_point(*s, t.space_id, idx.id, k.clone());
                    } else {
                        txn.note_range(
                            *s,
                            RangeRead {
                                space: t.space_id,
                                index: idx.id,
                                lo: Some(k.clone()),
                                hi: Some(k.clone()),
                                lo_incl: true,
                                hi_incl: true,
                            },
                        );
                    }
                }
            }
        }
    }
    if per_shard.len() > 1 {
        // Index order across shards: by the index key, then the primary key.
        let pk = &t.indexes[0].cols;
        rows.sort_by(|a, b| {
            idx.cols
                .iter()
                .chain(pk.iter())
                .map(|&c| sort_cmp(&a[c], &b[c]))
                .find(|o| *o != std::cmp::Ordering::Equal)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    }
    Ok(rows)
}

/// Read the rows of one table access (for the outer row `outer`).
pub async fn fetch(
    cluster: &Arc<Cluster>,
    mode: &mut ReadMode<'_>,
    ta: &TableAccess,
    outer: &[Value],
    cx: &EvalCtx<'_>,
) -> SqlResult<Vec<Row>> {
    let rows = fetch_stored(cluster, mode, ta, outer, cx).await?;
    Ok(overlay(mode, &ta.table, rows))
}

/// Rows of one table access as stored (without the transaction's own
/// buffered writes).
async fn fetch_stored(
    cluster: &Arc<Cluster>,
    mode: &mut ReadMode<'_>,
    ta: &TableAccess,
    outer: &[Value],
    cx: &EvalCtx<'_>,
) -> SqlResult<Vec<Row>> {
    fetch_stored_from(cluster, mode, ta, outer, cx, None).await
}

/// `fetch_stored`, but a range scan (a `Range` path or a single prefix)
/// starts at `resume` (index key, inclusive?) instead of its lower bound.
async fn fetch_stored_from(
    cluster: &Arc<Cluster>,
    mode: &mut ReadMode<'_>,
    ta: &TableAccess,
    outer: &[Value],
    cx: &EvalCtx<'_>,
    resume: Option<(&[Value], bool)>,
) -> SqlResult<Vec<Row>> {
    let t = &ta.table;
    if t.is_virtual() {
        if resume.is_some() {
            return Ok(Vec::new());
        }
        // A table another SQL server has just created shows up at once, as
        // in SHOW TABLES.
        cluster.refresh_if_newer().await?;
        return Ok(crate::catalog::information_schema_rows(&cluster.catalog()));
    }
    let limit = eval_limit(&ta.limit, cx)?.map(|l| l.min(u32::MAX as u64) as u32);
    let mut calls: Vec<Call> = Vec::new();
    let mut unique_point = false;
    match &ta.path {
        AccessPath::Union(paths) => {
            // Each lookup separately, then one row per primary key, in
            // primary-key order (as MySQL's index_merge union).
            let mut by_pk: BTreeMap<Vec<u8>, Row> = BTreeMap::new();
            let pk = &t.indexes[0].cols;
            let mut rows_all: Vec<Row> = Vec::new();
            for p in paths {
                let sub = TableAccess {
                    path: p.clone(),
                    limit: None,
                    ..ta.clone()
                };
                let rows = Box::pin(fetch_stored(cluster, mode, &sub, outer, cx)).await?;
                for r in rows {
                    let k: Vec<Value> = pk.iter().map(|&c| r[c].clone()).collect();
                    by_pk.entry(crate::txn::key_bytes(&k).to_vec()).or_insert(r);
                }
            }
            rows_all.extend(by_pk.into_values());
            rows_all.sort_by(|a, b| {
                pk.iter()
                    .map(|&c| sort_cmp(&a[c], &b[c]))
                    .find(|o| *o != std::cmp::Ordering::Equal)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            return Ok(rows_all);
        }
        AccessPath::Point { index, keys } | AccessPath::Prefixes { index, keys } if keys.len() > 1 => {
            let idx = index_def(t, *index);
            let unique = matches!(ta.path, AccessPath::Point { .. });
            let ks = eval_keys(t, idx, keys, outer, cx)?;
            if ks.is_empty() {
                return Ok(Vec::new());
            }
            let rows = fetch_keys(cluster, mode, t, idx, ks, unique, if unique { Some(1) } else { limit }).await?;
            return Ok(rows);
        }
        AccessPath::Prefixes { index, keys } => {
            // One prefix: an ordinary equality range.
            let idx = index_def(t, *index);
            let ks = eval_keys(t, idx, keys, outer, cx)?;
            let Some(prefix) = ks.into_iter().next() else {
                return Ok(Vec::new());
            };
            let shards: Vec<usize> = match route_prefix(cluster, t, idx, &prefix) {
                Some(s) => vec![s],
                None => (0..cluster.nshards()).collect(),
            };
            let range = RangeRead {
                space: t.space_id,
                index: idx.id,
                lo: Some(prefix.clone()),
                hi: Some(prefix.clone()),
                lo_incl: true,
                hi_incl: true,
            };
            let (key, iter) = match resume {
                Some((k, incl)) => (k.to_vec(), if incl { Iter::Ge } else { Iter::Gt }),
                None => (prefix.clone(), Iter::Ge),
            };
            for s in shards {
                calls.push(Call {
                    shard: s,
                    key: key.clone(),
                    iter,
                    limit,
                    end: Some((prefix.clone(), true)),
                    range: Some(range.clone()),
                });
            }
        }
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
                    _ => return Ok(Vec::new()),
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
                return Ok(Vec::new());
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
            let (key, iter) = match resume {
                Some((k, incl)) => (k.to_vec(), if incl { Iter::Ge } else { Iter::Gt }),
                None => (key, iter),
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
        AccessPath::Point { index, .. } | AccessPath::Range { index, .. } | AccessPath::Prefixes { index, .. } => {
            *index
        }
        AccessPath::Full | AccessPath::Union(_) => 0,
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
        ReadMode::Committed(_) => {
            let mut futs = Vec::new();
            for c in &calls {
                let client = cluster.shards[c.shard].client(cluster).await?;
                futs.push(async move {
                    let end = c.end.as_ref().map(|(k, i)| (k.as_slice(), *i));
                    select_rows(&client, 0, t, index, &c.key, c.iter, c.limit, end, true, false).await
                });
            }
            for r in futures::future::join_all(futs).await {
                rows.extend(r?);
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
    Ok(rows)
}

fn overlay(mode: &ReadMode<'_>, t: &TableDef, rows: Vec<Row>) -> Vec<Row> {
    match mode {
        ReadMode::Txn(txn) | ReadMode::Committed(txn) => txn.overlay(t, rows),
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
    finish_select(rows, p, cx)
}

/// The rest of a SELECT over its filtered FROM rows: aggregation, HAVING,
/// ORDER BY, projection, DISTINCT, OFFSET/LIMIT.
fn finish_select(mut rows: Vec<Row>, p: &SelectPlan, cx: &EvalCtx<'_>) -> SqlResult<Vec<Row>> {
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

fn sort_rows(rows: &mut [Row], order: &[(Expr, bool)], cx: &EvalCtx<'_>) {
    if order.is_empty() {
        return;
    }
    rows.sort_by(|a, b| {
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

fn pk_of(t: &TableDef, row: &[Value], offset: usize) -> Vec<Value> {
    t.indexes[0].cols.iter().map(|&c| row[offset + c].clone()).collect()
}

/// One lock request of a batch: which candidate and table it belongs to.
struct LockItem {
    cand: usize,
    table: usize,
    shard: usize,
    key: LockKey,
}

/// Take the locks of a batch, shard by shard in order of first appearance;
/// returns the outcome of each item (same order).
async fn lock_batch(cluster: &Arc<Cluster>, txn: &mut Txn, items: &[LockItem], o: LockOpts) -> SqlResult<Vec<Locked>> {
    let mut shards: Vec<usize> = Vec::new();
    for it in items {
        if !shards.contains(&it.shard) {
            shards.push(it.shard);
        }
    }
    let mut out: Vec<Option<Locked>> = vec![None; items.len()];
    for s in shards {
        let idx: Vec<usize> = (0..items.len()).filter(|&i| items[i].shard == s).collect();
        let keys: Vec<LockKey> = idx
            .iter()
            .map(|&i| LockKey {
                table: items[i].key.table.clone(),
                index: items[i].key.index,
                key: items[i].key.key.clone(),
            })
            .collect();
        let res = txn.lock(cluster, s, &keys, o).await?;
        for (j, r) in idx.into_iter().zip(res) {
            out[j] = Some(r);
        }
    }
    Ok(out.into_iter().map(|r| r.unwrap_or(Locked::Granted)).collect())
}

/// Release locks taken for rows that did not qualify, grouped by shard.
async fn unlock_items(cluster: &Arc<Cluster>, txn: &Txn, items: Vec<(usize, LockKey)>) {
    let mut by: BTreeMap<usize, Vec<LockKey>> = BTreeMap::new();
    for (s, k) in items {
        by.entry(s).or_default().push(k);
    }
    for (s, keys) in by {
        txn.unlock(cluster, s, &keys).await;
    }
}

/// The latest version of a locked row for this transaction: its own
/// buffered version if it wrote the row (None: it deleted it), else the
/// committed row read under the lock.
fn own_or_latest(txn: &Txn, t: &TableDef, pk: &[Value], latest: Option<Row>) -> Option<Row> {
    match txn.buffered(t.space_id, pk) {
        Some(b) => b.cloned(),
        None => latest,
    }
}

/// Lock rows of one table by primary key, in the given order and in
/// batches, reading each under its lock: a row that no longer exists or no
/// longer matches `filter` (re-checked on the latest version, as InnoDB's
/// READ COMMITTED "semi-consistent" read does) has its lock released at
/// once. Stops after `want` qualifying rows. SKIP LOCKED leaves out rows
/// other transactions hold. Returns the latest versions of the rows kept.
#[allow(clippy::too_many_arguments)]
pub async fn lock_matching(
    cluster: &Arc<Cluster>,
    txn: &mut Txn,
    t: &Arc<TableDef>,
    cands: Vec<Row>,
    filter: &Option<Expr>,
    o: LockOpts,
    want: Option<usize>,
    cx: &EvalCtx<'_>,
) -> SqlResult<Vec<Row>> {
    let o = LockOpts { read: true, ..o };
    let mut kept = Vec::new();
    let mut i = 0;
    while i < cands.len() && want.is_none_or(|w| kept.len() < w) {
        let n = match want {
            Some(w) => (w - kept.len()).clamp(1, 64),
            None => 256,
        };
        let chunk = &cands[i..(i + n).min(cands.len())];
        i += chunk.len();
        let items: Vec<LockItem> = chunk
            .iter()
            .enumerate()
            .map(|(j, r)| LockItem {
                cand: j,
                table: 0,
                shard: shard_of_row(cluster, t, r),
                key: LockKey {
                    table: t.clone(),
                    index: 0,
                    key: pk_of(t, r, 0),
                },
            })
            .collect();
        let res = lock_batch(cluster, txn, &items, o).await?;
        let mut release = Vec::new();
        for (it, r) in items.into_iter().zip(res) {
            let latest = match r {
                Locked::Skipped => continue,
                Locked::Row(row) => own_or_latest(txn, t, &it.key.key, row),
                Locked::Granted => own_or_latest(txn, t, &it.key.key, Some(chunk[it.cand].clone())),
            };
            let ok = match &latest {
                None => false,
                Some(row) => match filter {
                    Some(f) => f.eval_bool(row, cx)?,
                    None => true,
                },
            };
            if ok && want.is_none_or(|w| kept.len() < w) {
                kept.push(latest.unwrap());
            } else if txn.buffered(t.space_id, &it.key.key).is_none() {
                release.push((it.shard, it.key));
            }
        }
        unlock_items(cluster, txn, release).await;
    }
    Ok(kept)
}

/// READ COMMITTED UPDATE/DELETE of rows named by their full primary key
/// (`WHERE pk = ?`, `pk IN (...)`, possibly with more conditions): the rows
/// are locked and read in one `tarn_lock` call per shard, with no candidate
/// read first, and WHERE is checked on the versions read under the locks, as
/// `lock_matching` does. A key with no row, or whose row does not match, has
/// its lock released at once. As in InnoDB, whose semi-consistent read does
/// not apply to unique searches, a key another transaction holds is waited
/// for. None: the statement is not of this form (the caller then reads
/// candidates with `candidate_rows` and locks them with `lock_matching`).
#[allow(clippy::too_many_arguments)]
pub async fn lock_pk_points(
    cluster: &Arc<Cluster>,
    txn: &mut Txn,
    access: &TableAccess,
    filter: &Option<Expr>,
    order: &[(Expr, bool)],
    o: LockOpts,
    want: Option<usize>,
    cx: &EvalCtx<'_>,
) -> SqlResult<Option<Vec<Row>>> {
    let AccessPath::Point { index: 0, keys } = &access.path else {
        return Ok(None);
    };
    // Several keys are taken in primary-key order: only without ORDER BY
    // and LIMIT, which make that order visible.
    if keys.len() > 1 && (!order.is_empty() || want.is_some()) {
        return Ok(None);
    }
    let t = &access.table;
    let pk = &t.indexes[0];
    let mut kept = Vec::new();
    if want == Some(0) {
        return Ok(Some(kept));
    }
    let mut items = Vec::new();
    for (j, key) in eval_keys(t, pk, keys, &[], cx)?.into_iter().enumerate() {
        let Some(shard) = route_prefix(cluster, t, pk, &key) else {
            return Ok(None);
        };
        items.push(LockItem {
            cand: j,
            table: 0,
            shard,
            key: LockKey {
                table: t.clone(),
                index: 0,
                key,
            },
        });
    }
    let res = lock_batch(cluster, txn, &items, LockOpts { read: true, ..o }).await?;
    let mut release = Vec::new();
    for (it, r) in items.into_iter().zip(res) {
        let latest = match r {
            Locked::Row(row) => own_or_latest(txn, t, &it.key.key, row),
            Locked::Skipped | Locked::Granted => return Err(crate::mpw::proto("tarn_lock read no row")),
        };
        let ok = match &latest {
            None => false,
            Some(row) => match filter {
                Some(f) => f.eval_bool(row, cx)?,
                None => true,
            },
        };
        if ok {
            kept.push(latest.unwrap());
        } else if txn.buffered(t.space_id, &it.key.key).is_none() {
            release.push((it.shard, it.key));
        }
    }
    unlock_items(cluster, txn, release).await;
    Ok(Some(kept))
}

/// Candidate rows of a single table for a READ COMMITTED UPDATE/DELETE: the
/// latest committed state (with the transaction's own writes), filtered and
/// ordered; locked and re-checked by `lock_matching`.
pub async fn candidate_rows(
    cluster: &Arc<Cluster>,
    txn: &mut Txn,
    access: &TableAccess,
    filter: &Option<Expr>,
    order: &[(Expr, bool)],
    cx: &EvalCtx<'_>,
) -> SqlResult<Vec<Row>> {
    let mut mode = ReadMode::Committed(txn);
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
    sort_rows(&mut out, order, cx);
    Ok(out)
}

/// SELECT ... FOR UPDATE / FOR SHARE [NOWAIT | SKIP LOCKED]: every base row
/// of each candidate is locked (in ORDER BY order, until LIMIT rows
/// qualify), as InnoDB does.
///
/// READ COMMITTED: the candidates come from the latest committed state;
/// single-table queries read each row under its lock and re-check WHERE on
/// that version (joins lock the rows they read). Serializable: the
/// candidates are read through the transaction (rows and ranges validated
/// at commit, like any read of it); the locks keep other writers off them
/// until the transaction ends, so the reads stay valid. (Rows SKIP LOCKED
/// leaves out were read too: their changes still abort the transaction.)
pub async fn run_locking_select(
    cluster: &Arc<Cluster>,
    txn: &mut Txn,
    p: &SelectPlan,
    cx: &EvalCtx<'_>,
    lock: RowLock,
) -> SqlResult<Vec<Row>> {
    let rc = txn.mode == crate::txn::Mode::ReadCommitted;
    let plain = p.agg.is_none() && p.having.is_none() && !p.distinct;
    let want = if plain {
        match eval_limit(&p.limit, cx)? {
            Some(l) => Some(l as usize + eval_limit(&p.offset, cx)?.unwrap_or(0) as usize),
            None => None,
        }
    } else {
        None
    };
    let filter = |rows: Vec<Row>| -> SqlResult<Vec<Row>> {
        let Some(f) = &p.filter else { return Ok(rows) };
        let mut kept = Vec::with_capacity(rows.len());
        for r in rows {
            if f.eval_bool(&r, cx)? {
                kept.push(r);
            }
        }
        Ok(kept)
    };
    // With LIMIT on one index range in index order, read the candidates a
    // page at a time (as InnoDB scans and locks as it goes) instead of the
    // whole range: SKIP LOCKED over a long queue stops at the first free rows.
    let page_size = |accepted: usize| {
        want.map(|w| (2 * w.saturating_sub(accepted)).clamp(16, 512))
            .unwrap_or(512)
    };
    let mut pager = match (&p.source, want) {
        (Source::Table(ta), Some(_)) => Pager::new(cluster, txn, ta, &p.order, cx)?,
        _ => None,
    };
    let mut rows = match pager.as_mut() {
        Some(pg) => filter(pg.next(cluster, txn, rc, cx, page_size(0)).await?)?,
        None => {
            let rows = if rc {
                let mut mode = ReadMode::Committed(txn);
                source_rows(cluster, &mut mode, &p.source, cx).await?
            } else {
                let mut mode = ReadMode::Txn(txn);
                source_rows(cluster, &mut mode, &p.source, cx).await?
            };
            let mut rows = filter(rows)?;
            if plain {
                sort_rows(&mut rows, &p.order, cx);
            }
            rows
        }
    };
    let o = LockOpts {
        exclusive: lock.exclusive,
        nowait: lock.nowait,
        skip: lock.skip,
        read: rc,
    };
    let tables: Vec<TableAccess> = p.source.tables().into_iter().cloned().collect();
    let single = tables.len() == 1;
    let mut accepted = Vec::new();
    let mut i = 0;
    while want.is_none_or(|w| accepted.len() < w) {
        if i >= rows.len() {
            match pager.as_mut() {
                Some(pg) if !pg.done => {
                    let more = pg.next(cluster, txn, rc, cx, page_size(accepted.len())).await?;
                    rows.extend(filter(more)?);
                    continue;
                }
                _ => break,
            }
        }
        let n = match want {
            Some(w) => (w - accepted.len()).clamp(1, 64),
            None => 256,
        };
        let chunk = &rows[i..(i + n).min(rows.len())];
        i += chunk.len();
        let mut items = Vec::new();
        for (j, r) in chunk.iter().enumerate() {
            for (k, ta) in tables.iter().enumerate() {
                let pk = pk_of(&ta.table, r, ta.offset);
                if pk.iter().all(|v| v.is_null()) {
                    continue; // LEFT JOIN without a match
                }
                let base: Row = r[ta.offset..ta.offset + ta.table.columns.len()].to_vec();
                items.push(LockItem {
                    cand: j,
                    table: k,
                    shard: shard_of_row(cluster, &ta.table, &base),
                    key: LockKey {
                        table: ta.table.clone(),
                        index: 0,
                        key: pk,
                    },
                });
            }
        }
        let res = lock_batch(cluster, txn, &items, o).await?;
        let mut cand_rows: Vec<Option<Row>> = chunk.iter().map(|r| Some(r.clone())).collect();
        let mut cand_items: Vec<Vec<(usize, LockKey)>> = (0..chunk.len()).map(|_| Vec::new()).collect();
        for (it, r) in items.into_iter().zip(res) {
            let ta = &tables[it.table];
            let row = match r {
                Locked::Skipped => {
                    cand_rows[it.cand] = None;
                    continue;
                }
                Locked::Row(latest) => own_or_latest(txn, &ta.table, &it.key.key, latest),
                Locked::Granted => Some(chunk[it.cand][ta.offset..ta.offset + ta.table.columns.len()].to_vec()),
            };
            match (row, cand_rows[it.cand].as_mut()) {
                (Some(latest), Some(cr)) => {
                    if single {
                        cr[ta.offset..ta.offset + ta.table.columns.len()].clone_from_slice(&latest);
                    }
                }
                (None, Some(_)) => cand_rows[it.cand] = None, // deleted meanwhile
                _ => {}
            }
            cand_items[it.cand].push((it.shard, it.key));
        }
        let mut release = Vec::new();
        for (j, cr) in cand_rows.into_iter().enumerate() {
            let ok = match &cr {
                None => false,
                Some(row) if single => match &p.filter {
                    Some(f) => f.eval_bool(row, cx)?,
                    None => true,
                },
                Some(_) => true,
            };
            if ok && want.is_none_or(|w| accepted.len() < w) {
                accepted.push(cr.unwrap());
            } else {
                for (s, k) in std::mem::take(&mut cand_items[j]) {
                    if txn.buffered(k.table.space_id, &k.key).is_none() {
                        release.push((s, k));
                    }
                }
            }
        }
        unlock_items(cluster, txn, release).await;
    }
    finish_select(accepted, p, cx)
}

/// Paged candidates of a locking read: a forward scan of one index range on
/// one shard, a page at a time, each page starting after the last row
/// returned. Only for scans whose index order is the statement's ORDER BY,
/// on a table where the transaction has at most deleted rows (those are left
/// out; rows it inserted or changed would need merging into index order).
struct Pager {
    ta: TableAccess,
    key_cols: Vec<usize>,
    /// The primary key: the next page starts strictly after the last key.
    /// Other indexes may repeat a key: the next page starts at it and skips
    /// the rows already returned with it.
    unique: bool,
    last: Option<Vec<Value>>,
    last_pks: HashSet<Bytes>,
    done: bool,
}

impl Pager {
    fn new(
        cluster: &Arc<Cluster>,
        txn: &Txn,
        ta: &TableAccess,
        order: &[(Expr, bool)],
        cx: &EvalCtx<'_>,
    ) -> SqlResult<Option<Pager>> {
        let t = &ta.table;
        let (index, prefix) = match &ta.path {
            AccessPath::Range {
                index,
                eq,
                reverse: false,
                ..
            } => (*index, eq),
            AccessPath::Prefixes { index, keys } if keys.len() == 1 => (*index, &keys[0]),
            _ => return Ok(None),
        };
        let idx = index_def(t, index);
        let Some(prefix) = eval_keys(t, idx, std::slice::from_ref(prefix), &[], cx)?.pop() else {
            return Ok(None);
        };
        let wrote_rows = txn
            .writes
            .range((t.space_id, Bytes::new())..)
            .take_while(|((sp, _), _)| *sp == t.space_id)
            .any(|(_, w)| w.row.is_some());
        if wrote_rows || route_prefix(cluster, t, idx, &prefix).is_none() {
            return Ok(None);
        }
        // ORDER BY: columns fixed by the equality prefix, then the next index
        // columns in order, ascending.
        let mut next = prefix.len();
        for (e, desc) in order {
            let Expr::Col(c) = e else { return Ok(None) };
            match idx.cols.iter().position(|x| x == c) {
                Some(pos) if pos < prefix.len() => {}
                Some(pos) if pos == next && !desc => next += 1,
                _ => return Ok(None),
            }
        }
        Ok(Some(Pager {
            ta: ta.clone(),
            key_cols: idx.cols.clone(),
            unique: idx.id == 0,
            last: None,
            last_pks: HashSet::new(),
            done: false,
        }))
    }

    async fn next(
        &mut self,
        cluster: &Arc<Cluster>,
        txn: &mut Txn,
        rc: bool,
        cx: &EvalCtx<'_>,
        n: usize,
    ) -> SqlResult<Vec<Row>> {
        let limit = n + if self.unique { 0 } else { self.last_pks.len() };
        let page = TableAccess {
            limit: Some(Expr::Lit(Value::Int(limit as i64))),
            ..self.ta.clone()
        };
        let resume = self.last.as_ref().map(|k| (k.as_slice(), !self.unique));
        let rows = if rc {
            let mut mode = ReadMode::Committed(txn);
            fetch_stored_from(cluster, &mut mode, &page, &[], cx, resume).await?
        } else {
            let mut mode = ReadMode::Txn(txn);
            fetch_stored_from(cluster, &mut mode, &page, &[], cx, resume).await?
        };
        if rows.len() < limit {
            self.done = true;
        }
        let t = &self.ta.table;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let key: Vec<Value> = self.key_cols.iter().map(|&c| r[c].clone()).collect();
            let pk_vals = pk_of(t, &r, 0);
            let pk = crate::txn::key_bytes(&pk_vals);
            let deleted = txn.buffered(t.space_id, &pk_vals).is_some();
            if self.last.as_ref() == Some(&key) {
                if !self.last_pks.insert(pk) {
                    continue; // returned by the previous page
                }
            } else {
                self.last = Some(key);
                self.last_pks.clear();
                self.last_pks.insert(pk);
            }
            if !deleted {
                out.push(r); // (rows this transaction deleted are left out)
            }
        }
        Ok(out)
    }
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
