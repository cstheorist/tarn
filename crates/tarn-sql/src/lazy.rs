//! Optional eventual secondary-index reads and causal session barriers.
//!
//! LazyIndex/ LazyBarrier refine durable delivery, not SQL snapshots. Every
//! candidate here is fetched from its current base owner, checked against
//! its complete canonical key and WHERE, then projected. Strong execution
//! remains the fallback; no partially collected result escapes this module.

use crate::catalog::{IndexDef, TableDef};
use crate::cluster::Cluster;
use crate::exec::{self, ReadMode};
use crate::expr::{EvalCtx, Expr, Func};
use crate::mpw::{lua_status, proto, Mp, Rd};
use crate::plan::{AccessPath, SelectPlan, Source};
use crate::txn::{apply_pending, key_bytes, CommitOutcome, PendingApply, Route, Row, Txn};
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tarn_common::{SqlError, SqlResult, Value};
use tarn_iproto::Iter;

pub const MAX_SEQ: u64 = (1 << 53) - 1;
pub const PAGE_ENTRIES: usize = 128;
pub const PAGE_BYTES: usize = 1_048_576;
const HISTORY_BUCKETS: usize = 4096;
const HISTORY_PENDING: usize = 128;
const HISTORY_BYTES: usize = 1_048_576;
const SCAN_ENTRIES: usize = 65_536;
const RESULT_BYTES: usize = 16 * 1_048_576;

pub fn flag(value: Option<&Value>) -> SqlResult<bool> {
    match value {
        None => Ok(false),
        Some(Value::Int(0) | Value::UInt(0)) => Ok(false),
        Some(Value::Int(1) | Value::UInt(1)) => Ok(true),
        Some(Value::Str(s)) if s.eq_ignore_ascii_case(b"off") || s.as_ref() == b"0" => Ok(false),
        Some(Value::Str(s)) if s.eq_ignore_ascii_case(b"on") || s.as_ref() == b"1" => Ok(true),
        Some(v) => Err(crate::admin::bad_value("tarn_lazy_read_your_writes", &v.to_string())),
    }
}

pub fn wait_seconds(value: Option<&Value>) -> SqlResult<u64> {
    let Some(v) = value else { return Ok(5) };
    let n = match v {
        Value::Int(n) => *n as i128,
        Value::UInt(n) => *n as i128,
        Value::Str(s) => std::str::from_utf8(s)
            .ok()
            .and_then(|s| s.trim().parse::<i128>().ok())
            .ok_or_else(|| {
                SqlError::new(
                    1232,
                    "42000",
                    "Incorrect argument type to variable 'tarn_lazy_wait_timeout'",
                )
            })?,
        _ => {
            return Err(SqlError::new(
                1232,
                "42000",
                "Incorrect argument type to variable 'tarn_lazy_wait_timeout'",
            ))
        }
    };
    if !(0..=60).contains(&n) {
        return Err(crate::admin::bad_value("tarn_lazy_wait_timeout", &v.to_string()));
    }
    Ok(n as u64)
}

#[derive(Default)]
pub struct History {
    buckets: BTreeSet<u32>,
    pending: Vec<PendingApply>,
    /// Unknown outcomes and bounded-history overflow cannot be repaired by
    /// forgetting them. This session must use the strong index thereafter.
    pub strong: bool,
}

impl History {
    pub fn pending_count(&self) -> usize {
        self.pending.iter().map(|p| p.participants.len()).sum()
    }
    pub fn bucket_count(&self) -> usize {
        self.buckets.len()
    }
    pub fn uncertain(&mut self) {
        self.strong = true;
        self.buckets.clear();
        self.pending.clear();
    }

    pub fn touch(&mut self, buckets: impl IntoIterator<Item = u32>) {
        if self.strong {
            return;
        }
        for b in buckets {
            if !self.buckets.contains(&b) && self.buckets.len() == HISTORY_BUCKETS {
                self.uncertain();
                return;
            }
            self.buckets.insert(b);
        }
        self.check_bound();
    }

    pub fn record(&mut self, outcome: CommitOutcome) {
        if self.strong {
            return;
        }
        self.touch(outcome.lazy_buckets);
        if self.strong {
            return;
        }
        if let Some(p) = outcome.pending_apply {
            let old = self.pending.iter().position(|x| x.txid == p.txid);
            let added = p.txid.len().saturating_add(p.participants.len().saturating_mul(4));
            let resident = self.pending.iter().fold(self.buckets.len() * 4, |n, p| {
                n.saturating_add(p.txid.len())
                    .saturating_add(p.participants.len().saturating_mul(4))
            });
            if (old.is_none() && self.pending.len() == HISTORY_PENDING)
                || resident.saturating_add(added) > HISTORY_BYTES
            {
                self.uncertain();
                return;
            }
            if let Some(old) = self.pending.iter_mut().find(|x| x.txid == p.txid) {
                old.participants.extend(p.participants);
                old.participants.sort_unstable();
                old.participants.dedup();
            } else {
                self.pending.push(p);
            }
        }
        self.check_bound();
    }

    fn check_bound(&mut self) {
        let bytes = self.buckets.len().saturating_mul(4)
            + self
                .pending
                .iter()
                .map(|p| p.txid.len() + p.participants.len().saturating_mul(4))
                .sum::<usize>();
        if self.buckets.len() > HISTORY_BUCKETS || self.pending.len() > HISTORY_PENDING || bytes > HISTORY_BYTES {
            self.uncertain();
        }
    }

    /// Complete before opening ANY lazy read view. Keep the ledger on
    /// timeout/cancellation. A new incarnation's baseline is not eligible
    /// for SQL reads until its separate all-source bootstrap barrier.
    pub async fn catch_up(&mut self, cluster: &Arc<Cluster>, seconds: u64) -> bool {
        if self.strong {
            return false;
        }
        if self.pending.is_empty() && self.buckets.is_empty() {
            return true;
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(seconds);
        let work = async {
            while !self.pending.is_empty() {
                for p in &mut self.pending {
                    p.participants = apply_pending(cluster, p).await?;
                }
                self.pending.retain(|p| !p.participants.is_empty());
                if !self.pending.is_empty() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
            // Counter capture happens AFTER own APPLY, never while its
            // committed row/outbox can still be absent.
            let goals = frontiers(cluster, &self.buckets).await?;
            loop {
                let current = frontiers(cluster, &self.buckets).await?;
                if goals
                    .iter()
                    .all(|(b, g)| current.get(b).is_some_and(|x| x.completed >= g.counter as i64))
                {
                    return Ok::<(), tarn_common::SqlError>(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        if matches!(tokio::time::timeout_at(deadline, work).await, Ok(Ok(()))) {
            self.buckets.clear();
            true
        } else {
            false
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Frontier {
    pub counter: u64,
    pub completed: i64,
}

fn unsigned(rd: &mut Rd<'_>, max: u64) -> SqlResult<u64> {
    let n = match rd.value()? {
        Value::UInt(n) => n,
        Value::Int(n) if n >= 0 => n as u64,
        _ => return Err(proto("invalid lazy unsigned integer")),
    };
    if n > max {
        return Err(proto("lazy integer exceeds bound"));
    }
    Ok(n)
}

fn ok_table<'a>(d: &'a [u8], fields: usize) -> SqlResult<Rd<'a>> {
    // Decode ordinary terminal errors, but require exact success shape.
    lua_status(d)?;
    let mut rd = Rd::new(d);
    if rd.array()? != 1 || rd.array()? != fields || rd.value()? != Value::str("ok") {
        return Err(proto("malformed lazy success envelope"));
    }
    Ok(rd)
}

fn end(rd: &Rd<'_>) -> SqlResult<()> {
    if !rd.buf.is_empty() {
        return Err(proto("trailing lazy response data"));
    }
    Ok(())
}

fn decode_frontiers(d: &[u8], wanted: &[u32]) -> SqlResult<BTreeMap<u32, Frontier>> {
    let mut rd = ok_table(d, 2)?;
    if rd.array()? != wanted.len() {
        return Err(proto("incomplete lazy frontier set"));
    }
    let mut out = BTreeMap::new();
    for _ in wanted {
        if rd.array()? != 3 {
            return Err(proto("malformed lazy frontier"));
        }
        let b = unsigned(&mut rd, u32::MAX as u64)? as u32;
        let counter = unsigned(&mut rd, MAX_SEQ)?;
        let completed = match rd.value()? {
            Value::Int(n) if n >= -1 && n <= counter as i64 => n,
            Value::UInt(n) if n <= counter => n as i64,
            _ => return Err(proto("invalid lazy completed frontier")),
        };
        if !wanted.contains(&b) || out.insert(b, Frontier { counter, completed }).is_some() {
            return Err(proto("foreign or duplicate lazy bucket"));
        }
    }
    end(&rd)?;
    Ok(out)
}

async fn frontiers(cluster: &Cluster, buckets: &BTreeSet<u32>) -> SqlResult<BTreeMap<u32, Frontier>> {
    let map = cluster.bucket_map();
    let mut groups: BTreeMap<usize, Vec<u32>> = BTreeMap::new();
    for &b in buckets {
        if b == 0 || b > map.count {
            return Err(proto("invalid remembered lazy bucket"));
        }
        groups.entry(map.shard(b)).or_default().push(b);
    }
    let mut out = BTreeMap::new();
    for (s, bs) in groups {
        for bs in bs.chunks(PAGE_ENTRIES) {
            let mut m = Mp::new();
            m.arr(2).arr(bs.len());
            for &b in bs {
                m.uint(b as u64);
            }
            Route::Buckets(bs.to_vec()).encode(&mut m);
            let d = cluster.call(s, "tarn_lazy_frontiers", &m.0).await?;
            out.extend(decode_frontiers(&d, bs)?);
        }
    }
    Ok(out)
}

#[derive(Debug)]
struct Candidate {
    key: Vec<Value>,
    raw_pk: Vec<Value>,
    pkid: Bytes,
    bucket: u32,
}

fn values(rd: &mut Rd<'_>, count: usize) -> SqlResult<Vec<Value>> {
    if rd.array()? != count {
        return Err(proto("invalid lazy key width"));
    }
    (0..count).map(|_| rd.value()).collect()
}

fn decode_page(
    d: &[u8],
    gen: u32,
    t: &TableDef,
    ix: &IndexDef,
    count: u32,
) -> SqlResult<(Vec<Candidate>, Option<Vec<u8>>)> {
    if d.len() > PAGE_BYTES {
        return Err(proto("lazy page exceeds byte bound"));
    }
    let mut rd = ok_table(d, 4)?;
    if unsigned(&mut rd, u32::MAX as u64)? != gen as u64 {
        return Err(proto("foreign lazy incarnation"));
    }
    let n = rd.array()?;
    if n > PAGE_ENTRIES {
        return Err(proto("lazy page exceeds entry bound"));
    }
    crate::budget::decoded(d.len(), n, ix.cols.len() + t.pk().cols.len() + 3)?;
    let mut rows = Vec::with_capacity(n);
    for _ in 0..n {
        if rd.array()? != 5 {
            return Err(proto("malformed lazy candidate"));
        }
        let key = values(&mut rd, ix.cols.len())?;
        let raw_pk = values(&mut rd, t.pk().cols.len())?;
        let pkid = match rd.value()? {
            Value::Bin(b) => b,
            _ => return Err(proto("invalid lazy PK identity")),
        };
        let bucket = unsigned(&mut rd, count as u64)? as u32;
        if bucket == 0 || raw_pk.iter().any(Value::is_null) {
            return Err(proto("invalid lazy base identity"));
        }
        unsigned(&mut rd, MAX_SEQ)?; // zero is valid for bootstrap entries
        rows.push(Candidate {
            key,
            raw_pk,
            pkid,
            bucket,
        });
    }
    let next = if rd.is_nil() {
        rd.skip()?;
        None
    } else {
        Some(rd.raw_value()?.to_vec())
    };
    end(&rd)?;
    if rows.is_empty() && next.is_some() {
        return Err(proto("empty nonterminal lazy page"));
    }
    Ok((rows, next))
}

fn stable(e: &Expr) -> bool {
    match e {
        Expr::Func(Func::Rand, _) => false,
        Expr::Func(_, a) => a.iter().all(stable),
        Expr::Cmp(a, _, b, _)
        | Expr::Arith(a, _, b)
        | Expr::And(a, b)
        | Expr::Or(a, b)
        | Expr::Xor(a, b)
        | Expr::Like(a, b, _, _) => stable(a) && stable(b),
        Expr::Not(a)
        | Expr::Neg(a)
        | Expr::IsNull(a, _)
        | Expr::IsTrue(a, _, _)
        | Expr::Cast(a, _)
        | Expr::Collate(a, _)
        | Expr::Key(a, _) => stable(a),
        Expr::InList(a, b, _, _) => stable(a) && b.iter().all(stable),
        Expr::Between(a, b, c, _, _) => stable(a) && stable(b) && stable(c),
        Expr::Case(a, b, c, _) => {
            a.as_ref().is_none_or(|a| stable(a))
                && b.iter().all(|(a, b)| stable(a) && stable(b))
                && c.as_ref().is_none_or(|c| stable(c))
        }
        _ => true,
    }
}

pub fn eligible(p: &SelectPlan) -> bool {
    let Source::Table(ta) = &p.source else { return false };
    let AccessPath::Range { index, eq, .. } = &ta.path else {
        return false;
    };
    let Some(ix) = ta.table.indexes.iter().find(|i| i.id == *index) else {
        return false;
    };
    ix.lazy
        .as_ref()
        .is_some_and(|l| l.ready && l.space_id != 0 && eq.len() >= l.route_parts as usize)
        && !ix.invisible
        && ta.offset == 0
        && !ta.table.is_virtual()
        && p.lock.is_none()
        && p.agg.is_none()
        && p.having.is_none()
        && !p.distinct
        && eq.iter().all(|e| e.is_constant() && stable(e))
        && p.filter.as_ref().is_none_or(stable)
        && p.project.iter().all(stable)
        && p.order.iter().all(|(e, _)| stable(e))
        && p.limit.as_ref().is_none_or(stable)
        && p.offset.as_ref().is_none_or(stable)
}

fn order_col(e: &Expr, t: &TableDef) -> Option<usize> {
    match e {
        Expr::Col(c) if t.layout.col_weight.get(*c) == Some(&None) => Some(*c),
        Expr::Key(e, coll) => match e.as_ref() {
            Expr::Col(c) if t.layout.colls.get(*c) == Some(coll) => Some(*c),
            _ => None,
        },
        _ => None,
    }
}

fn follows_order(p: &SelectPlan, ix: &IndexDef, t: &TableDef, eq: usize) -> bool {
    if p.order.is_empty() {
        return true;
    }
    let mut cols = ix.cols.clone();
    cols.extend(t.pk().cols.iter().filter(|c| !ix.cols.contains(c)));
    let descending = p.order[0].1;
    if !p.order.iter().all(|(_, d)| *d == descending) {
        return false;
    }
    (0..=eq).any(|start| {
        p.order
            .iter()
            .enumerate()
            .all(|(n, (e, _))| order_col(e, t).is_some_and(|c| cols.get(start + n) == Some(&c)))
    })
}

/// `None` means restart through the ordinary strong executor. The caller
/// invokes this only after actual Session autocommit/locking checks.
pub async fn select(cluster: &Arc<Cluster>, p: &SelectPlan, cx: &EvalCtx<'_>) -> SqlResult<Option<Vec<Row>>> {
    if !eligible(p) {
        return Ok(None);
    }
    let Source::Table(ta) = &p.source else { unreachable!() };
    let AccessPath::Range {
        index,
        eq,
        null_eq,
        reverse,
        ..
    } = &ta.path
    else {
        unreachable!()
    };
    let t = &ta.table;
    let ix = t.indexes.iter().find(|i| i.id == *index).unwrap();
    let l = ix.lazy.as_ref().unwrap();
    let mut key = Vec::new();
    for (n, e) in eq.iter().take(l.route_parts as usize).enumerate() {
        let raw = e.eval(&[], cx)?;
        if raw.is_null() && !null_eq.get(n).copied().unwrap_or(false) {
            return Ok(Some(Vec::new()));
        }
        let Some(v) = t.lookup_part(ix.cols[n], &raw) else {
            return Ok(Some(Vec::new()));
        };
        key.push(v);
    }
    let map = cluster.bucket_map();
    let bucket = crate::route::bucket_of(&key, map.count);
    let shard = map.shard(bucket);
    let ordered = follows_order(p, ix, t, eq.len()) && p.order.iter().all(|(_, d)| d == reverse);
    let offset = exec::eval_limit(&p.offset, cx)?.unwrap_or(0);
    let want = if ordered {
        exec::eval_limit(&p.limit, cx)?.map(|n| n.saturating_add(offset))
    } else {
        None
    };
    if want == Some(0) {
        return Ok(Some(Vec::new()));
    }
    let mut after: Option<Vec<u8>> = None;
    let mut seen = HashSet::new();
    let mut kept = Vec::new();
    let (mut scanned, mut bytes) = (0usize, 0usize);
    let mut dummy = Txn::new(cluster, false);
    loop {
        let mut m = Mp::new();
        m.arr(6)
            .uint(l.space_id as u64)
            .vals(&key)
            .uint(if *reverse { Iter::Req as u64 } else { Iter::Eq as u64 })
            .uint(PAGE_ENTRIES as u64);
        match &after {
            Some(a) => {
                m.raw(a);
            }
            None => {
                m.nil();
            }
        }
        let route = Route::Buckets(vec![bucket]);
        crate::observe::shard(shard);
        crate::observe::read(&route);
        route.encode(&mut m);
        let (d, _wire) = crate::budget::cluster_read(cluster, shard, "tarn_lazy_select", &m.0).await?;
        let (candidates, next) = decode_page(&d, l.space_id, t, ix, map.count)?;
        if next.is_some() && next == after {
            return Err(proto("lazy cursor did not advance"));
        }
        scanned += candidates.len();
        if scanned > SCAN_ENTRIES {
            return Ok(None);
        }
        let mut keys = Vec::with_capacity(candidates.len());
        for c in &candidates {
            let k = t
                .pk()
                .cols
                .iter()
                .zip(&c.raw_pk)
                .map(|(&col, v)| t.lookup_part(col, v).ok_or_else(|| proto("invalid lazy raw PK")))
                .collect::<SqlResult<Vec<_>>>()?;
            if key_bytes(&k).as_ref() != c.pkid.as_ref()
                || crate::route::bucket_of(&t.shard_key_of_pk(&k), map.count) != c.bucket
            {
                return Err(proto("lazy candidate identity mismatch"));
            }
            keys.push(k);
        }
        let rows = exec::fetch_keys(
            cluster,
            &mut ReadMode::Committed(&mut dummy),
            t,
            t.pk(),
            keys,
            true,
            Some(1),
        )
        .await?;
        let mut by_pk: HashMap<Bytes, Row> = rows.into_iter().map(|r| (key_bytes(&t.pk_key(&r)), r)).collect();
        for c in candidates {
            let Some(r) = by_pk.get(&c.pkid) else { continue };
            if key_bytes(&t.index_key(ix, r)) != key_bytes(&c.key) {
                continue;
            }
            if let Some(f) = &p.filter {
                if !f.eval_bool(r, cx)? {
                    continue;
                }
            }
            if !seen.insert(c.pkid.clone()) {
                continue;
            }
            bytes = bytes.saturating_add(key_bytes(r).len()).saturating_add(c.pkid.len());
            if bytes > RESULT_BYTES {
                return Ok(None);
            }
            kept.push(by_pk.remove(&c.pkid).unwrap());
            if want.is_some_and(|n| kept.len() as u64 >= n) {
                break;
            }
        }
        let current = cluster.bucket_map();
        if current.count != map.count
            || current.mver != map.mver
            || current.epochs != map.epochs
            || current.moving != map.moving
        {
            return Ok(None);
        }
        if want.is_some_and(|n| kept.len() as u64 >= n) || next.is_none() {
            break;
        }
        after = next;
    }
    exec::finish_select(kept, p, cx).map(Some)
}

/// Cached samples only; collecting them never adds a storage RPC to SHOW.
#[derive(Clone, Debug)]
pub struct StatsSample {
    pub at: Instant,
    pub values: BTreeMap<String, Value>,
}

fn capability(d: &[u8]) -> SqlResult<()> {
    let mut rd = ok_table(d, 3)?;
    if unsigned(&mut rd, u32::MAX as u64)? != 1 {
        return Err(proto("unsupported lazy storage version"));
    }
    let mut limits = BTreeMap::new();
    for _ in 0..rd.map()? {
        let key = rd.string()?;
        let value = unsigned(&mut rd, u64::MAX)?;
        if limits.insert(key, value).is_some() {
            return Err(proto("duplicate lazy capability"));
        }
    }
    end(&rd)?;
    if limits.get("max_entries").copied().unwrap_or(0) < PAGE_ENTRIES as u64
        || limits.get("max_bytes").copied().unwrap_or(0) < PAGE_BYTES as u64
        || limits.get("max_seq").copied() != Some(MAX_SEQ)
        || limits.get("aux_names").copied() != Some(1)
    {
        return Err(proto("incompatible lazy storage limits"));
    }
    Ok(())
}

impl Cluster {
    pub(crate) async fn lazy_capabilities_on(&self, replicas: &[String]) -> SqlResult<()> {
        for addr in replicas {
            self.reclaim_connections(false).await;
            let client = tarn_iproto::Client::connect(&self.opts(addr))
                .await
                .map_err(crate::mpw::from_iproto)?;
            let r = tokio::time::timeout(
                Duration::from_secs(5),
                client.call(0, "tarn_lazy_capabilities", &[0x90]),
            )
            .await
            .map_err(|_| SqlError::unsupported("lazy storage capability check timed out"))?
            .map_err(|e| SqlError::unsupported(format!("lazy indexes require storage V1 on every replica ({e})")))?;
            let data = r.data().ok_or_else(|| proto("missing lazy capability"))?;
            capability(&data)?;
        }
        Ok(())
    }

    pub(crate) async fn require_lazy_capabilities(&self) -> SqlResult<()> {
        for s in self.live_shards() {
            self.lazy_capabilities_on(self.shards[s].replicas()).await?;
        }
        Ok(())
    }

    pub(crate) async fn bootstrap_lazy(
        &self,
        job: &crate::cluster::Job,
        call: &[u8],
        shards: &[usize],
    ) -> SqlResult<()> {
        let (active, seconds) = job_lazy(&job.args)?;
        if !active {
            return Ok(());
        }
        self.require_lazy_capabilities().await?;
        let work = async {
            for &s in shards {
                loop {
                    let d = self.call(s, "tarn_lazy_bootstrap", call).await?;
                    let mut rd = ok_table(&d, 2)?;
                    let done = boolean(&mut rd)?;
                    end(&rd)?;
                    if done {
                        break;
                    }
                }
            }
            loop {
                let mut complete = true;
                for &s in shards {
                    let d = self.call(s, "tarn_lazy_bootstrap_status", call).await?;
                    let mut rd = ok_table(&d, 3)?;
                    let done = boolean(&mut rd)?;
                    let pending = unsigned(&mut rd, MAX_SEQ)?;
                    end(&rd)?;
                    complete &= done && pending == 0;
                }
                if complete {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            for &s in shards {
                let d = self.call(s, "tarn_lazy_ready", call).await?;
                let rd = ok_table(&d, 1)?;
                end(&rd)?;
            }
            Ok::<(), SqlError>(())
        };
        tokio::time::timeout(Duration::from_secs_f64(seconds), work)
            .await
            .map_err(|_| SqlError::new(1205, "HY000", "lazy index bootstrap timed out before publication"))?
    }
}

fn boolean(rd: &mut Rd<'_>) -> SqlResult<bool> {
    let b = match rd.buf.first() {
        Some(0xc2) => false,
        Some(0xc3) => true,
        _ => return Err(proto("invalid lazy boolean")),
    };
    rd.skip()?;
    Ok(b)
}

fn job_lazy(args: &[u8]) -> SqlResult<(bool, f64)> {
    let mut rd = Rd::new(args);
    let (mut active, mut timeout) = (false, 50.0f64);
    for _ in 0..rd.map()? {
        match rd.string()?.as_str() {
            "lazy_drop" => {
                let raw = rd.raw_value()?;
                active |= Rd::new(raw).array()? > 0;
            }
            "timeout" => {
                timeout = match rd.value()? {
                    Value::Float(f) => f,
                    Value::Int(n) => n as f64,
                    Value::UInt(n) => n as f64,
                    _ => return Err(proto("invalid lazy bootstrap timeout")),
                };
                if !timeout.is_finite() {
                    return Err(proto("invalid lazy bootstrap timeout"));
                }
            }
            "def" => {
                for _ in 0..rd.map()? {
                    if rd.string()? == "lazy" {
                        let raw = rd.raw_value()?;
                        active |= Rd::new(raw).array()? > 0;
                    } else {
                        rd.skip()?;
                    }
                }
            }
            _ => rd.skip()?,
        }
    }
    end(&rd)?;
    Ok((active, timeout.clamp(1.0, 60.0)))
}

fn decode_stats(d: &[u8]) -> SqlResult<StatsSample> {
    let mut rd = ok_table(d, 2)?;
    let mut values = BTreeMap::new();
    for _ in 0..rd.map()? {
        let k = rd.string()?;
        let v = rd.value()?;
        if values.insert(k, v).is_some() {
            return Err(proto("duplicate lazy statistic"));
        }
    }
    end(&rd)?;
    for k in [
        "depth",
        "bytes",
        "reserved_entries",
        "reserved_bytes",
        "oldest_age",
        "inflight",
    ] {
        match values.get(k) {
            Some(Value::UInt(_)) | Some(Value::Int(0..)) => {}
            Some(Value::Float(f)) if k == "oldest_age" && f.is_finite() && *f >= 0.0 => {}
            _ => return Err(proto("invalid lazy statistics")),
        }
    }
    Ok(StatsSample {
        at: Instant::now(),
        values,
    })
}

pub fn start_stats(cluster: &Arc<Cluster>) {
    let weak = Arc::downgrade(cluster);
    tokio::spawn(async move {
        let mut cursor = 0usize;
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let Some(c) = weak.upgrade() else { break };
            if !c.catalog().all().any(|t| t.has_lazy()) {
                continue;
            }
            for s in crate::topology::poll_shards(&c, &mut cursor) {
                let sample = tokio::time::timeout(Duration::from_secs(3), c.call(s, "tarn_lazy_stats", &[0x90]))
                    .await
                    .ok()
                    .and_then(Result::ok)
                    .and_then(|d| decode_stats(&d).ok());
                c.lazy_samples.lock().insert(c.shards[s].id, sample);
            }
        }
    });
}

pub fn stats(cluster: &Cluster) -> Vec<(String, String)> {
    let samples = cluster.lazy_samples.lock();
    let mut out = Vec::new();
    for s in cluster.live_shards() {
        let prefix = format!("Tarn_lazy_shard_{}", cluster.shards[s].id);
        let sample = samples.get(&cluster.shards[s].id).and_then(Option::as_ref);
        let age = sample.map(|s| s.at.elapsed());
        out.push((
            format!("{prefix}_sample"),
            match age {
                None => "unavailable",
                Some(d) if d > Duration::from_secs(5) => "stale",
                _ => "current",
            }
            .to_string(),
        ));
        out.push((
            format!("{prefix}_sample_age_ms"),
            age.map_or_else(|| "NULL".to_string(), |a| a.as_millis().to_string()),
        ));
        for k in [
            "depth",
            "bytes",
            "reserved_entries",
            "reserved_bytes",
            "oldest_age",
            "inflight",
        ] {
            out.push((
                format!("{prefix}_{k}"),
                sample
                    .and_then(|s| s.values.get(k))
                    .map_or_else(|| "NULL".to_string(), ToString::to_string),
            ));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Catalog;
    use crate::plan::Stmt;
    use crate::planner::{plan, PlanCtx};

    fn table(sql: &str) -> TableDef {
        let cat = Catalog::default();
        let cx = PlanCtx {
            statistics: None,
            catalog: &cat,
            db: Some("d"),
            params: &[],
            conn_coll: "utf8mb4_bin",
            default_coll: "utf8mb4_bin",
        };
        let Stmt::CreateTable { mut def, .. } = plan(sql, &cx).unwrap() else {
            panic!()
        };
        def.space_id = 512;
        for i in &mut def.indexes {
            if let Some(l) = &mut i.lazy {
                l.space_id = 10001;
                l.ready = true;
            }
        }
        *def
    }

    fn fixture() -> TableDef {
        table("CREATE TABLE t (tenant_id INT, id INT, k INT, n INT, v VARCHAR(16), PRIMARY KEY(tenant_id,id), KEY lk(k,n,id) COMMENT 'tarn:lazy') PARTITION BY KEY(tenant_id)")
    }

    fn select_plan(t: TableDef, sql: &str) -> Arc<SelectPlan> {
        let cat = Catalog::new(1, vec!["d".into()], vec![t]);
        let cx = PlanCtx {
            statistics: None,
            catalog: &cat,
            db: Some("d"),
            params: &[],
            conn_coll: "utf8mb4_bin",
            default_coll: "utf8mb4_bin",
        };
        let Stmt::Select(p) = plan(sql, &cx).unwrap() else {
            panic!()
        };
        p
    }

    #[test]
    fn explicit_metadata_not_legacy_comment_activates_lazy() {
        let t = fixture();
        assert!(t.has_lazy());
        let json = serde_json::to_string(&t).unwrap();
        let restored = crate::catalog::parse_def_json(&json).unwrap();
        assert_eq!(restored.indexes[1].lazy, t.indexes[1].lazy);
        let mut legacy: serde_json::Value = serde_json::from_str(&json).unwrap();
        legacy["indexes"][1].as_object_mut().unwrap().remove("lazy");
        let restored = crate::catalog::parse_def_json(&legacy.to_string()).unwrap();
        assert_eq!(restored.indexes[1].comment, "tarn:lazy");
        assert!(!restored.has_lazy());
        let ld = t.lua_def();
        assert_eq!(ld.lazy[0].gen, 10001);
        assert_eq!(ld.lazy[0].base_space_id, 512);
        assert_eq!(ld.lazy[0].key_fields, vec![3, 4, 2]);
        assert_eq!(ld.lazy[0].pk_fields, vec![1, 2]);
        let mut invalid: serde_json::Value = serde_json::from_str(&json).unwrap();
        invalid["indexes"][1]["lazy"]["route_parts"] = 2.into();
        assert!(crate::catalog::parse_def_json(&invalid.to_string()).is_err());
    }

    #[test]
    fn reject_unique_and_noncanonical_lazy_identity_only() {
        let cat = Catalog::default();
        let cx = PlanCtx {
            statistics: None,
            catalog: &cat,
            db: Some("d"),
            params: &[],
            conn_coll: "utf8mb4_bin",
            default_coll: "utf8mb4_bin",
        };
        for sql in [
            "CREATE TABLE t (id INT, k INT, PRIMARY KEY(id) COMMENT 'tarn:lazy')",
            "CREATE TABLE t (id INT PRIMARY KEY, k INT, UNIQUE KEY lk(k) COMMENT 'tarn:lazy')",
            "CREATE TABLE t (id INT PRIMARY KEY,k DOUBLE,KEY lk(k) COMMENT 'tarn:lazy')",
            "CREATE TABLE t (id INT PRIMARY KEY,k INT,n DECIMAL(20,0),KEY lk(k,n) COMMENT 'tarn:lazy')",
            "CREATE TABLE t (tenant_id INT,id FLOAT,k INT,PRIMARY KEY(tenant_id,id),KEY lk(k) COMMENT 'tarn:lazy') PARTITION BY KEY(tenant_id)",
            "CREATE TABLE t (id INT PRIMARY KEY,k INT,KEY lk(k) COMMENT 'tarn:lazy=2')",
            "CREATE TABLE t (id INT PRIMARY KEY,k JSON,KEY lk(k) COMMENT 'tarn:lazy')",
        ] { assert_eq!(plan(sql,&cx).unwrap_err().code,1235,"{sql}"); }
        assert!(plan("CREATE TABLE t (id INT PRIMARY KEY,k DOUBLE,KEY lk(k))", &cx).is_ok());
        let t = table("CREATE TABLE t (id INT PRIMARY KEY,k INT,KEY lk(k) COMMENT 'notarn:lazy')");
        assert!(!t.has_lazy());
    }

    #[test]
    fn select_eligibility_and_direction_are_conservative() {
        let p = select_plan(fixture(), "SELECT id,n FROM t WHERE k=7 ORDER BY n,id LIMIT 3");
        assert!(eligible(&p));
        let Source::Table(ta) = &p.source else { panic!() };
        assert!(follows_order(&p, &ta.table.indexes[1], &ta.table, 1));
        for sql in [
            "SELECT id FROM t WHERE n=7 ORDER BY id LIMIT 3",
            "SELECT COUNT(*) FROM t WHERE k=7",
            "SELECT DISTINCT id FROM t WHERE k=7",
            "SELECT id FROM t WHERE k=7 FOR UPDATE",
            "SELECT RAND() FROM t WHERE k=7",
            "SELECT a.id FROM t a JOIN t b ON a.id=b.id WHERE a.k=7",
        ] {
            assert!(!eligible(&select_plan(fixture(), sql)), "{sql}");
        }
        let mut unready = fixture();
        unready.indexes[1].lazy.as_mut().unwrap().ready = false;
        assert!(!eligible(&select_plan(unready, "SELECT id FROM t WHERE k=7")));
    }

    #[test]
    fn ci_raw_pk_and_canonical_fields_are_distinct() {
        let t=table("CREATE TABLE t (tenant_id INT,id VARCHAR(20) COLLATE utf8mb4_unicode_ci,k VARCHAR(20) COLLATE utf8mb4_general_ci, PRIMARY KEY(tenant_id,id),KEY lk(k) COMMENT 'tarn:lazy') PARTITION BY KEY(tenant_id)");
        let s = &t.lua_def().lazy[0];
        assert_ne!(s.pk_fields, s.pk_native_fields);
        assert_eq!(s.pk_types, vec!["integer", "varbinary"]);
        let raw = Value::str("É ");
        let native = t.lookup_part(1, &raw).unwrap();
        assert!(matches!(native, Value::Bin(_)));
        assert_eq!(t.lookup_part(1, &Value::str("e")), Some(native));
    }

    fn frontier(rows: &[(u32, u64, i64)]) -> Vec<u8> {
        let mut m = Mp::new();
        m.arr(1).arr(2).str("ok").arr(rows.len());
        for &(b, c, f) in rows {
            m.arr(3).uint(b as u64).uint(c).int(f);
        }
        m.0
    }

    #[test]
    fn frontier_requires_exact_hole_free_bucket_set_and_integer_bounds() {
        assert_eq!(
            decode_frontiers(&frontier(&[(7, 0, -1)]), &[7]).unwrap()[&7],
            Frontier {
                counter: 0,
                completed: -1
            }
        );
        assert_eq!(
            decode_frontiers(&frontier(&[(7, MAX_SEQ, MAX_SEQ as i64)]), &[7]).unwrap()[&7].counter,
            MAX_SEQ
        );
        for (rows, wanted) in [
            (vec![(7, 2, 3)], vec![7]),
            (vec![(7, 0, -2)], vec![7]),
            (vec![(7, MAX_SEQ + 1, 0)], vec![7]),
            (vec![(7, 1, 1), (7, 1, 1)], vec![7, 8]),
            (vec![(8, 1, 1)], vec![7]),
            (vec![], vec![7]),
        ] {
            assert!(decode_frontiers(&frontier(&rows), &wanted).is_err());
        }
        let mut d = frontier(&[(7, 1, 0)]);
        d.push(0xc0);
        assert!(decode_frontiers(&d, &[7]).is_err());
        // maxACK=2 is not completion while sequence1 is still pending.
        assert!(decode_frontiers(&frontier(&[(7, 2, 0)]), &[7]).unwrap()[&7].completed < 2);
    }

    #[test]
    fn cumulative_history_is_bounded_and_never_last_commit_only() {
        let mut h = History::default();
        h.record(CommitOutcome {
            lazy_buckets: BTreeSet::from([1]),
            pending_apply: Some(PendingApply {
                txid: "a".into(),
                participants: vec![3],
            }),
        });
        h.record(CommitOutcome {
            lazy_buckets: BTreeSet::from([2]),
            pending_apply: Some(PendingApply {
                txid: "b".into(),
                participants: vec![4],
            }),
        });
        assert_eq!(h.bucket_count(), 2);
        assert_eq!(h.pending_count(), 2);
        assert!(!h.strong);
        h.touch(1..=HISTORY_BUCKETS as u32 + 1);
        assert!(h.strong);
        assert_eq!(h.pending_count(), 0);
        assert_eq!(h.bucket_count(), 0);
        h.record(CommitOutcome {
            lazy_buckets: BTreeSet::from([1]),
            pending_apply: None,
        });
        assert!(h.strong);
        assert_eq!(h.bucket_count(), 0);
    }

    #[test]
    fn session_variable_boundaries() {
        assert!(!flag(None).unwrap());
        assert!(flag(Some(&Value::str("ON"))).unwrap());
        assert!(!flag(Some(&Value::UInt(0))).unwrap());
        for v in [Value::Int(-1), Value::Int(2), Value::Null, Value::str("maybe")] {
            assert!(flag(Some(&v)).is_err());
        }
        assert_eq!(wait_seconds(None).unwrap(), 5);
        for n in [0, 5, 60] {
            assert_eq!(wait_seconds(Some(&Value::Int(n))).unwrap(), n as u64);
        }
        for v in [Value::Int(-1), Value::UInt(61)] {
            assert_eq!(wait_seconds(Some(&v)).unwrap_err().code, 1231);
        }
        for v in [
            Value::Float(0.5),
            Value::str("NaN"),
            Value::str("Inf"),
            Value::str("1.5"),
            Value::Null,
        ] {
            assert_eq!(wait_seconds(Some(&v)).unwrap_err().code, 1232);
        }
    }

    #[test]
    fn capability_requires_explicit_matching_protocol_and_bounds() {
        let make = |version, seq, aux: Option<u64>| {
            let mut m = Mp::new();
            m.arr(1)
                .arr(3)
                .str("ok")
                .uint(version)
                .map(3 + aux.is_some() as usize)
                .str("max_entries")
                .uint(128)
                .str("max_bytes")
                .uint(1048576)
                .str("max_seq")
                .uint(seq);
            if let Some(aux) = aux {
                m.str("aux_names").uint(aux);
            }
            m.0
        };
        assert!(capability(&make(1, MAX_SEQ, Some(1))).is_ok());
        assert!(capability(&make(0, MAX_SEQ, Some(1))).is_err());
        assert!(capability(&make(1, MAX_SEQ - 1, Some(1))).is_err());
        assert!(capability(&make(1, MAX_SEQ, None)).is_err());
        assert!(capability(&make(1, MAX_SEQ, Some(0))).is_err());
        let mut d = make(1, MAX_SEQ, Some(1));
        d.push(0xc0);
        assert!(capability(&d).is_err());
    }

    #[test]
    fn canonical_integer_keys_ignore_value_enum_representation() {
        for n in [0, 127, 128, 32768, i64::MAX] {
            assert_eq!(key_bytes(&[Value::Int(n)]), key_bytes(&[Value::UInt(n as u64)]));
        }
        let t = fixture();
        let findings = crate::lint::check_table(&|_, _| None, &t);
        assert!(findings.iter().all(|f| !f.message.contains("routing prefix")));
        let redundant = table("CREATE TABLE t (id INT PRIMARY KEY,k INT,KEY lk(id,k) COMMENT 'tarn:lazy')");
        assert!(crate::lint::check_table(&|_, _| None, &redundant)
            .iter()
            .any(|f| f.message.contains("routing prefix")));
    }

    #[test]
    fn candidate_page_rejects_wrong_generation_width_and_trailing_data() {
        let t = fixture();
        let ix = &t.indexes[1];
        let mut m = Mp::new();
        m.arr(1)
            .arr(4)
            .str("ok")
            .uint(10001)
            .arr(1)
            .arr(5)
            .vals(&[Value::Int(7), Value::Int(1), Value::Int(2)])
            .vals(&[Value::Int(1), Value::Int(2)])
            .val(&Value::Bin(key_bytes(&[Value::Int(1), Value::Int(2)])))
            .uint(1)
            .uint(0)
            .nil();
        assert_eq!(decode_page(&m.0, 10001, &t, ix, 30000).unwrap().0.len(), 1);
        assert!(decode_page(&m.0, 10002, &t, ix, 30000).is_err());
        let mut narrow = ix.clone();
        narrow.cols.pop();
        assert!(decode_page(&m.0, 10001, &t, &narrow, 30000).is_err());
        m.nil();
        assert!(decode_page(&m.0, 10001, &t, ix, 30000).is_err());
        let mut empty = Mp::new();
        empty.arr(1).arr(4).str("ok").uint(10001).arr(0).arr(1).uint(1);
        assert!(decode_page(&empty.0, 10001, &t, ix, 30000).is_err());
    }
}
