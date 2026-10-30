//! Execution of SELECT plans: reading rows through access paths (inside a
//! transaction's streams, or as one lock-aware autocommit read), joins,
//! filtering, aggregation, sorting, projection, DISTINCT and LIMIT.

use crate::catalog::{IndexDef, TableDef};
use crate::cluster::Cluster;
use crate::collation::Collation;
use crate::expr::CmpOp;
use crate::expr::{EvalCtx, Expr};
use crate::normalize::LitName;
use crate::plan::*;
use crate::txn::{
    count_rows, select_keys, select_rows, LockKey, LockOpts, LockRange, Locked, PredOp, Projection, RangeRead, Route,
    Row, StoragePred, Txn,
};
use crate::types::{key_val_coll, sort_cmp, sort_cmp_coll, to_num, BaseType, KeyVal, SqlType};
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::Ordering as AtomicOrdering;
use std::sync::Arc;
use tarn_common::{Column, SqlError, SqlResult, Value};
use tarn_iproto::Iter;

mod colocated;
pub(crate) mod diagnostics;
mod pushdown;
mod topk;

async fn read_client(cluster: &Cluster, shard: usize) -> SqlResult<tarn_iproto::Client> {
    crate::observe::shard(shard);
    cluster.shards[shard].client(cluster).await
}

/// Error code used internally to say "this statement needs a transaction":
/// an autocommit read touched more than one shard or needed more than one
/// storage call, so it is re-run as an implicit read-only transaction.
pub const NEED_TXN: u16 = 65000;

pub fn need_txn() -> SqlError {
    SqlError::new(NEED_TXN, "HY000", "internal: statement needs a transaction")
}

/// Error code used internally to say "this READ COMMITTED read needs more
/// than one storage page": the statement is read again in a snapshot (see
/// [`committed_snapshot`]).
const NEED_SNAPSHOT: u16 = 65001;

fn need_snapshot() -> SqlError {
    SqlError::new(NEED_SNAPSHOT, "HY000", "internal: read needs a snapshot")
}

/// Whether every page of a storage read is fetched, or only one (a read that
/// has more fails with this error, and its statement reads differently).
#[derive(Clone, Copy)]
enum Pages {
    All,
    One(fn() -> SqlError),
}

/// What a read keeps of the stored rows of each page as they arrive, when
/// the statement's WHERE (`filter`) is pushed down to it: the rows that
/// satisfy it, leaving out first the stored versions of the rows the
/// transaction wrote (`written`; the overlay replaces those, and WHERE
/// must not see them). Each row is evaluated once, which matters for
/// RAND() and for expressions that fail on some rows.
#[derive(Clone, Copy)]
struct Keep<'a, 'c> {
    topk: Option<topk::Spec<'a>>,
    filter: Option<&'a Expr>,
    cx: &'a EvalCtx<'c>,
    t: &'a TableDef,
    written: &'a BTreeSet<Bytes>,
    project: Option<&'a Projection>,
}

impl Keep<'_, '_> {
    fn extend(&self, out: &mut Vec<Row>, rows: Vec<Row>) -> SqlResult<()> {
        match self.filter {
            None => out.extend(rows),
            Some(f) => {
                for r in rows {
                    if !self.written.is_empty() && self.written.contains(&crate::txn::key_bytes(&self.t.pk_key(&r))) {
                        continue;
                    }
                    if crate::budget::Frame::new().scope_sync(|| f.eval_bool(&r, self.cx))? {
                        out.push(r);
                    }
                }
            }
        }
        Ok(())
    }
}

/// The rows one index scan kept, with how many it read and the last row it
/// read (before `Keep` filtered them): what the scan covered.
struct Scanned {
    _last_guard: Option<tarn_common::ResourceGuard>,
    rows: Vec<Row>,
    read: usize,
    last: Option<Row>,
}

/// Every page of one index scan (`select_rows`) on one shard; in a stream,
/// the first page begins its transaction if `begin`.
#[allow(clippy::too_many_arguments)]
async fn scan_pages(
    client: &tarn_iproto::Client,
    stream: u64,
    mut begin: bool,
    t: &TableDef,
    index: u32,
    c: &Call,
    lock_check: bool,
    pages: Pages,
    keep: Keep<'_, '_>,
    reads: &std::sync::atomic::AtomicU64,
    route: &Route,
) -> SqlResult<Scanned> {
    let end = c.end.as_ref().map(|(k, i)| (k.as_slice(), *i));
    let mut out = Scanned {
        _last_guard: None,
        rows: Vec::new(),
        read: 0,
        last: None,
    };
    let mut after: Option<Vec<u8>> = None;
    let mut left = c.limit;
    let mut top = keep.topk.map(topk::TopK::new).transpose()?;
    loop {
        // Projection and top-K have disjoint eligibility. Keep the ordinary
        // top-K page lifetime if a future caller supplies both options.
        if top.is_none() {
            if let Some(project) = keep.project {
                reads.fetch_add(1, AtomicOrdering::Relaxed);
                if let Some(page) = crate::txn::project_rows(
                    client,
                    stream,
                    t,
                    index,
                    &c.key,
                    c.iter,
                    end,
                    lock_check,
                    begin,
                    after.as_deref(),
                    route,
                    project,
                )
                .await?
                {
                    begin = false;
                    out.read += page.read;
                    if let Some(r) = page.last {
                        out.last = Some(r);
                    }
                    keep.extend(&mut out.rows, page.rows)?;
                    match (page.next, pages) {
                        (None, _) => return Ok(out),
                        (Some(_), Pages::One(e)) => return Err(e()),
                        (Some(n), Pages::All) => {
                            after = Some(n);
                            continue;
                        }
                    }
                }
            }
        }
        let frame = top.as_ref().map(|_| crate::budget::Frame::new());
        let page = async {
            reads.fetch_add(1, AtomicOrdering::Relaxed);
            let (rows, next) = select_rows(
                client,
                stream,
                t,
                index,
                &c.key,
                c.iter,
                left,
                end,
                lock_check,
                begin,
                after.as_deref(),
                route,
            )
            .await?;
            begin = false;
            if let Some(l) = left.as_mut() {
                *l = l.saturating_sub(rows.len() as u32);
            }
            out.read += rows.len();
            if let Some(r) = rows.last() {
                if top.is_some() {
                    out._last_guard = crate::budget::retain(crate::budget::row_bytes(r) * 2)?;
                }
                out.last = Some(if top.is_some() {
                    crate::budget::own_row(r.clone())
                } else {
                    r.clone()
                });
            }
            if let Some(top) = &mut top {
                for row in rows {
                    let row_frame = crate::budget::Frame::new();
                    row_frame.scope_sync(|| -> SqlResult<()> {
                        if keep
                            .filter
                            .map(|f| f.eval_bool(&row, keep.cx))
                            .transpose()?
                            .unwrap_or(true)
                        {
                            top.push(row, keep.cx)?;
                        }
                        Ok(())
                    })?;
                }
            } else {
                keep.extend(&mut out.rows, rows)?;
            }
            Ok::<_, SqlError>(next)
        };
        let next = match &frame {
            Some(frame) => frame.scope(page).await?,
            None => page.await?,
        };
        drop(frame);
        match (next, pages) {
            (None, _) => break,
            (Some(_), _) if left == Some(0) => break,
            (Some(_), Pages::One(e)) => return Err(e()),
            (Some(n), Pages::All) => after = Some(n),
        }
    }
    if let Some(top) = top {
        out.rows = top.finish()?;
    }
    Ok(out)
}

/// Every page of one multi-key read (`select_keys`) on one shard.
#[allow(clippy::too_many_arguments)]
async fn keys_pages(
    client: &tarn_iproto::Client,
    stream: u64,
    mut begin: bool,
    t: &TableDef,
    index: u32,
    keys: &[Vec<Value>],
    limit: Option<u32>,
    lock_check: bool,
    pages: Pages,
    reads: &std::sync::atomic::AtomicU64,
    route: &Route,
    native_batch: bool,
) -> SqlResult<Vec<Row>> {
    let mut out = Vec::new();
    let mut after: Option<Vec<u8>> = None;
    loop {
        reads.fetch_add(1, AtomicOrdering::Relaxed);
        let (rows, next) = select_keys(
            client,
            stream,
            t,
            index,
            keys,
            limit,
            lock_check,
            begin,
            after.as_deref(),
            route,
            native_batch,
        )
        .await?;
        begin = false;
        out.extend(rows);
        match (next, pages) {
            (None, _) => return Ok(out),
            (Some(_), Pages::One(e)) => return Err(e()),
            (Some(n), Pages::All) => after = Some(n),
        }
    }
}

impl ReadMode<'_> {
    /// How many pages each storage read of this mode fetches.
    fn pages(&self) -> Pages {
        match self {
            ReadMode::Auto { .. } => Pages::One(need_txn),
            ReadMode::Statement(_) => Pages::One(need_snapshot),
            ReadMode::Txn(_) | ReadMode::Committed(_) => Pages::All,
        }
    }
}

/// A READ COMMITTED SELECT that needs more than one storage page per read
/// (issue #77), run again in a read-only serializable transaction that
/// overlays `txn`'s buffered writes: the statement sees one state of the
/// shards, as InnoDB's sees one read view, where page-by-page reads of the
/// latest state could miss a row that moved within an index meanwhile, or
/// return it twice. Its reads are validated when it ends, and it is retried
/// after a conflict; under writes that keep changing what it reads it fails
/// with 1213 after the retries, like an autocommit read.
async fn committed_snapshot(
    cluster: &Arc<Cluster>,
    txn: &Txn,
    p: &SelectPlan,
    cx: &EvalCtx<'_>,
) -> SqlResult<Vec<Row>> {
    let mut attempt = 0;
    loop {
        let frame = crate::budget::Frame::new();
        let res = frame
            .scope(Box::pin(async {
                let mut snap = Txn::new(cluster, false);
                crate::budget::charge(
                    txn.writes
                        .values()
                        .fold(0u64, |n, w| {
                            n.saturating_add(
                                crate::budget::row_bytes(&w.pk)
                                    + w.row.as_ref().map_or(0, |r| crate::budget::row_bytes(r)),
                            )
                        })
                        .saturating_mul(2),
                )?;
                snap.writes = txn.writes.clone(); // overlaid, never written
                let res = select_once(cluster, &mut ReadMode::Txn(&mut snap), p, cx).await;
                snap.writes.clear();
                match res {
                    Ok(v) => snap.commit(cluster).await.map(|_| v),
                    Err(e) => {
                        snap.rollback(cluster);
                        Err(e)
                    }
                }
            }))
            .await;
        if res.is_ok() {
            frame.retain_all();
        }
        drop(frame);
        match res {
            Err(e) if e.code == 1213 && !e.moved && attempt < crate::session::MAX_RETRIES => {
                attempt += 1;
                cluster.stats.retries.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                crate::session::backoff(attempt).await;
            }
            other => return other,
        }
    }
}

pub enum ReadMode<'a> {
    /// Reads inside the transaction's streams (tracked, overlaid).
    Txn(&'a mut Txn),
    /// One lock-aware autocommit read (TarnCommit!SnapshotRead): one storage
    /// call returning one page; a longer read needs a transaction.
    Auto { calls: usize },
    /// READ COMMITTED (TarnCommit!RCRead): every storage read sees the latest
    /// committed state -- waiting out prepared transactions' W locks, as
    /// autocommit reads do -- and is neither tracked nor validated; the
    /// transaction's own buffered writes are overlaid. Reads go on page by
    /// page (UPDATE and DELETE candidates, which are read again under
    /// their locks).
    Committed(&'a mut Txn),
    /// A READ COMMITTED SELECT: as `Committed`, but in one page per storage
    /// read; one that needs more reads in a snapshot ([`committed_snapshot`]).
    Statement(&'a mut Txn),
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
/// prefix determines the shard key. (Also on a cluster of one shard: shards
/// may join it, and buckets move to them.)
pub fn route_prefix(cluster: &Cluster, t: &TableDef, idx: &IndexDef, prefix: &[Value]) -> Option<usize> {
    route_bucket(cluster, t, idx, prefix).map(|(s, _)| s)
}

/// The shard and the bucket of every row whose `index` key starts with
/// `prefix`, if the prefix determines the shard key.
pub fn route_bucket(cluster: &Cluster, t: &TableDef, idx: &IndexDef, prefix: &[Value]) -> Option<(usize, u32)> {
    route_bucket_in(&cluster.bucket_map(), t, idx, prefix)
}

fn route_bucket_in(
    map: &crate::buckets::BucketMap,
    t: &TableDef,
    idx: &IndexDef,
    prefix: &[Value],
) -> Option<(usize, u32)> {
    let mut sk = Vec::with_capacity(t.shard_key.len());
    for &c in &t.shard_key {
        let p = idx.cols.iter().position(|&x| x == c)?;
        sk.push(prefix.get(p)?.clone());
    }
    let b = crate::route::bucket_of(&sk, map.count);
    Some((map.shard(b), b))
}

/// What the shard of a storage read checks (lua/tarn.lua "Buckets"): the
/// buckets it was routed by; else, outside a transaction's streams, the
/// shard's epoch in `map` (a read of every shard). The reads of one scan of
/// every shard take their epochs from one map: each passes only if its
/// shard's buckets have not changed since the map was read, so together
/// they read each bucket on the one shard that owned it then. (Epochs from
/// two maps could read a bucket on the shard it left and, later, on the
/// one it went to.)
fn read_route(
    map: &crate::buckets::BucketMap,
    in_stream: bool,
    shard: usize,
    buckets: &[Option<u32>],
) -> SqlResult<Route> {
    if !buckets.is_empty() && buckets.iter().all(|b| b.is_some()) {
        let mut v: Vec<u32> = buckets.iter().flatten().copied().collect();
        v.sort_unstable();
        v.dedup();
        return Ok(Route::Buckets(v));
    }
    if in_stream {
        return Ok(Route::None);
    }
    Ok(Route::Epoch(map.epoch_of(shard)?, map.mver))
}

pub fn shard_of_row(cluster: &Cluster, t: &TableDef, row: &[Value]) -> usize {
    cluster.shard_of(&t.shard_key_of(row))
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
    /// The bucket the call was routed by (None: one of the calls to every
    /// shard).
    bucket: Option<u32>,
    key: Vec<Value>,
    iter: Iter,
    limit: Option<u32>,
    end: Option<(Vec<Value>, bool)>,
    range: Option<RangeRead>,
}

/// Lexicographic order of index keys as the index holds them (as
/// Tarantool orders them: a case-insensitive part is a binary weight string).
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
            match t.lookup_part(idx.cols[i], &v) {
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
pub(crate) async fn fetch_keys(
    cluster: &Arc<Cluster>,
    mode: &mut ReadMode<'_>,
    t: &TableDef,
    idx: &IndexDef,
    keys: Vec<Vec<Value>>,
    unique: bool,
    limit: Option<u32>,
) -> SqlResult<Vec<Row>> {
    // Capture routing before grouping any key. A later epoch/membership
    // must not validate a scatter group that omitted a newly joined shard.
    let map = cluster.bucket_map();
    let mut live: Option<Vec<usize>> = None;
    // By shard: the keys, and the bucket each was routed by.
    let mut per_shard: BTreeMap<usize, (Vec<Vec<Value>>, Vec<Option<u32>>)> = BTreeMap::new();
    for k in keys {
        match route_bucket_in(&map, t, idx, &k) {
            Some((s, b)) => {
                let e = per_shard.entry(s).or_default();
                e.0.push(k);
                e.1.push(Some(b));
            }
            None => {
                let shards = live.get_or_insert_with(|| cluster.live_shards().collect());
                for &s in shards.iter() {
                    let e = per_shard.entry(s).or_default();
                    e.0.push(k.clone());
                    e.1.push(None);
                }
            }
        }
    }
    let mut rows = Vec::new();
    let pages = mode.pages();
    let reads = &cluster.stats.storage_reads;
    let in_stream = matches!(mode, ReadMode::Txn(_));
    let native_batch = cluster.cfg.native_batch_reads;
    let mut routes = BTreeMap::new();
    for (s, (_, bs)) in &per_shard {
        routes.insert(*s, read_route(&map, in_stream, *s, bs)?);
    }
    match mode {
        ReadMode::Auto { calls: made } => {
            *made += per_shard.len();
            if *made > 1 {
                return Err(need_txn());
            }
            for (s, (ks, _)) in &per_shard {
                let client = read_client(cluster, *s).await?;
                rows.extend(
                    keys_pages(
                        &client,
                        0,
                        false,
                        t,
                        idx.id,
                        ks,
                        limit,
                        true,
                        pages,
                        reads,
                        &routes[s],
                        native_batch,
                    )
                    .await?,
                );
            }
        }
        ReadMode::Committed(_) | ReadMode::Statement(_) => {
            let mut futs = Vec::new();
            for (s, (ks, _)) in &per_shard {
                let client = read_client(cluster, *s).await?;
                let route = &routes[s];
                futs.push(async move {
                    keys_pages(
                        &client,
                        0,
                        false,
                        t,
                        idx.id,
                        ks,
                        limit,
                        true,
                        pages,
                        reads,
                        route,
                        native_batch,
                    )
                    .await
                });
            }
            for r in futures::future::join_all(futs).await {
                rows.extend(r?);
            }
        }
        ReadMode::Txn(txn) => {
            let mut futs = Vec::new();
            for (s, (ks, _)) in &per_shard {
                let st = txn.stream(cluster, *s).await?;
                let route = &routes[s];
                futs.push(async move {
                    keys_pages(
                        &st.client, st.id, st.begin, t, idx.id, ks, limit, false, pages, reads, route, false,
                    )
                    .await
                });
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
            for (s, (ks, bs)) in per_shard.iter() {
                txn.mark_begun(*s);
                for (k, b) in ks.iter().zip(bs) {
                    if unique {
                        txn.note_point(*s, t.space_id, idx.id, k.clone(), *b)?;
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
                                bucket: *b,
                            },
                        )?;
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
                .map(|&c| t.key_cmp(c, &a[c], &b[c]))
                .find(|o| *o != std::cmp::Ordering::Equal)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    }
    cluster
        .stats
        .rows_read
        .fetch_add(rows.len() as u64, AtomicOrdering::Relaxed);
    Ok(rows)
}

/// The global index of the unique index with Tarantool id `index`, if it has
/// one (a unique key without the shard key, see `catalog`).
fn global_index(t: &TableDef, index: u32) -> Option<&crate::catalog::GlobalIndex> {
    t.indexes
        .iter()
        .position(|i| i.id == index)
        .and_then(|p| t.global_of(p))
}

/// Rows of `t` with the full keys `keys` (as the index holds them) of a
/// unique key that has global index `g`: the entries on the keys' shards,
/// then the rows by primary key on theirs -- one call per shard the keys
/// (then the rows) live on, instead of one per shard. Reads are tracked as
/// any reads of `mode` (both validated in a serializable transaction). A
/// READ COMMITTED statement whose row no longer holds its entry's key (the
/// value moved between the two reads) is read again in a snapshot.
async fn global_lookup(
    cluster: &Arc<Cluster>,
    mode: &mut ReadMode<'_>,
    t: &TableDef,
    g: &crate::catalog::GlobalIndex,
    keys: Vec<Vec<Value>>,
) -> SqlResult<Vec<Row>> {
    if matches!(mode, ReadMode::Auto { .. }) {
        return Err(need_txn()); // two reads: never one autocommit call
    }
    // A key dropped meanwhile (its space gone): the catalog changed (1412).
    let gone = |e: SqlError| {
        if e.code == 1146 {
            crate::txn::definition_changed()
        } else {
            e
        }
    };
    let entries = fetch_keys(cluster, mode, &g.table, &g.table.indexes[0], keys, true, Some(1))
        .await
        .map_err(gone)?;
    let mut pks: Vec<Vec<Value>> = entries.iter().map(|e| g.holder_pk(t, e)).collect();
    if pks.is_empty() {
        return Ok(Vec::new());
    }
    pks.sort_by(|a, b| key_cmp(a, b));
    pks.dedup_by(|a, b| key_cmp(a, b) == std::cmp::Ordering::Equal);
    let mut rows = fetch_keys(cluster, mode, t, &t.indexes[0], pks, true, Some(1)).await?;
    if matches!(mode, ReadMode::Statement(_)) {
        let k = g.key_len();
        let held: HashSet<Bytes> = rows
            .iter()
            .filter_map(|r| g.entry(t, r))
            .map(|e| crate::txn::key_bytes(&e[..k]))
            .collect();
        if entries.iter().any(|e| !held.contains(&crate::txn::key_bytes(&e[..k]))) {
            return Err(need_snapshot());
        }
    }
    // in the unique key's order, as a lookup through its index returns them
    let idx = &t.indexes[g.pos];
    rows.sort_by(|a, b| {
        idx.cols
            .iter()
            .map(|&c| t.key_cmp(c, &a[c], &b[c]))
            .find(|o| *o != std::cmp::Ordering::Equal)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
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
    fetch_stored_from(cluster, mode, ta, outer, cx, None, None).await
}

/// `fetch_stored`, but a range scan (a `Range` path or a single prefix)
/// starts at `resume` (index key, inclusive?) instead of its lower bound,
/// and the rows of its scans are kept only if they satisfy `filter`, page
/// by page (the stored versions of rows the transaction wrote are dropped
/// by the overlay anyway, so filtering them first changes nothing).
async fn fetch_stored_from(
    cluster: &Arc<Cluster>,
    mode: &mut ReadMode<'_>,
    ta: &TableAccess,
    outer: &[Value],
    cx: &EvalCtx<'_>,
    resume: Option<(&[Value], bool)>,
    filter: Option<&Expr>,
) -> SqlResult<Vec<Row>> {
    let t = &ta.table;
    if t.is_virtual() {
        if resume.is_some() {
            return Ok(Vec::new());
        }
        if t.name == "TARN_MOVES" {
            return Ok(cluster
                .rebalance_status()
                .await?
                .map(|r| r.moves)
                .unwrap_or_default()
                .into_iter()
                .map(|x| {
                    vec![
                        Value::UInt(x.bucket as u64),
                        Value::UInt(x.from as u64),
                        Value::UInt(x.to as u64),
                        Value::UInt(x.bytes),
                        Value::str(&x.state),
                        if x.error.is_empty() {
                            Value::Null
                        } else {
                            Value::str(&x.error)
                        },
                    ]
                })
                .collect());
        }
        // A table another SQL server has just created shows up at once, as
        // in SHOW TABLES.
        cluster.refresh_if_newer().await?;
        return Ok(crate::catalog::information_schema_rows(&cluster.catalog()));
    }
    let map = cluster.bucket_map();
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
                    .map(|&c| t.key_cmp(c, &a[c], &b[c]))
                    .find(|o| *o != std::cmp::Ordering::Equal)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            return Ok(rows_all);
        }
        // (Not for READ COMMITTED UPDATE / DELETE candidates, which page
        // outside any snapshot: a value moving between rows between the two
        // reads would be missed; they ask every shard.)
        AccessPath::Point { index, keys }
            if resume.is_none()
                && cluster.nlive() > 1
                && !matches!(mode, ReadMode::Committed(_))
                && global_index(t, *index).is_some() =>
        {
            let idx = index_def(t, *index);
            let ks = eval_keys(t, idx, keys, outer, cx)?;
            if ks.is_empty() {
                return Ok(Vec::new());
            }
            return global_lookup(cluster, mode, t, global_index(t, *index).unwrap(), ks).await;
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
        AccessPath::Prefixes { .. } | AccessPath::Range { .. } | AccessPath::Full => {
            let shards: Vec<usize> = cluster.live_shards().collect();
            calls = range_calls_in(&map, &shards, t, &ta.path, outer, cx, resume, limit)?;
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
                    match t.lookup_part(idx.cols[i], &v) {
                        Some(x) => key.push(x),
                        None => continue 'keys,
                    }
                }
                if keys.len() > 1 && !seen.insert(crate::txn::key_bytes(&key)) {
                    continue;
                }
                // A unique key without the shard key (a global index, see
                // catalog) does not say which shard holds the row: ask each.
                let shards: Vec<(usize, Option<u32>)> = match route_bucket_in(&map, t, idx, &key) {
                    Some((s, b)) => vec![(s, Some(b))],
                    None => cluster.live_shards().map(|s| (s, None)).collect(),
                };
                for (shard, bucket) in shards {
                    calls.push(Call {
                        shard,
                        bucket,
                        key: key.clone(),
                        iter: Iter::Eq,
                        limit: Some(1),
                        end: None,
                        range: None,
                    });
                }
            }
        }
    }
    let index = path_index(&ta.path);
    // (The rows this transaction wrote are left out before WHERE sees them.)
    let written = match (&*mode, filter) {
        (ReadMode::Txn(txn) | ReadMode::Committed(txn) | ReadMode::Statement(txn), Some(_)) => txn.written_keys(t),
        _ => BTreeSet::new(),
    };
    let keep = Keep {
        topk: None,
        filter,
        cx,
        t,
        written: &written,
        project: None,
    };
    let scans = read_calls(cluster, &map, mode, t, index, calls, unique_point, keep).await?;
    Ok(scans.into_iter().flat_map(|s| s.rows).collect())
}

/// Run the index scans `calls` of `t`'s index `index`, those of different
/// shards concurrently (one autocommit read makes at most one call), and
/// return what each scan kept, in the order of `calls`. In a transaction
/// each scan's range is recorded for validation, only up to its last row
/// when its limit cut it short; `unique_point`: the calls are point lookups
/// of a unique key, recorded as points.
#[allow(clippy::too_many_arguments)]
async fn read_calls(
    cluster: &Arc<Cluster>,
    map: &crate::buckets::BucketMap,
    mode: &mut ReadMode<'_>,
    t: &TableDef,
    index: u32,
    calls: Vec<Call>,
    unique_point: bool,
    keep: Keep<'_, '_>,
) -> SqlResult<Vec<Scanned>> {
    let mut out = Vec::with_capacity(calls.len());
    let mut read = 0;
    let pages = mode.pages();
    let reads = &cluster.stats.storage_reads;
    let in_stream = matches!(mode, ReadMode::Txn(_));
    let routes = calls
        .iter()
        .map(|c| read_route(map, in_stream, c.shard, &[c.bucket]))
        .collect::<SqlResult<Vec<Route>>>()?;
    match mode {
        ReadMode::Auto { calls: made } => {
            *made += calls.len();
            if *made > 1 {
                return Err(need_txn());
            }
            for (c, route) in calls.iter().zip(&routes) {
                let client = read_client(cluster, c.shard).await?;
                let s = scan_pages(&client, 0, false, t, index, c, true, pages, keep, reads, route).await?;
                read += s.read;
                out.push(s);
            }
        }
        ReadMode::Committed(_) | ReadMode::Statement(_) => {
            let mut futs = Vec::new();
            for (c, route) in calls.iter().zip(&routes) {
                let client = read_client(cluster, c.shard).await?;
                futs.push(
                    async move { scan_pages(&client, 0, false, t, index, c, true, pages, keep, reads, route).await },
                );
            }
            for r in futures::future::join_all(futs).await {
                let s = r?;
                read += s.read;
                out.push(s);
            }
        }
        ReadMode::Txn(txn) => {
            let mut futs = Vec::new();
            for (c, route) in calls.iter().zip(&routes) {
                let st = txn.stream(cluster, c.shard).await?;
                futs.push(async move {
                    scan_pages(
                        &st.client, st.id, st.begin, t, index, c, false, pages, keep, reads, route,
                    )
                    .await
                });
            }
            let results = futures::future::join_all(futs).await;
            // A call that a limit cut short read its range only up to the
            // last row it returned: record that much (issue #41), so that
            // writes to the unread rest of the range do not conflict.
            // (As the index holds the key: weight strings for a
            // case-insensitive column.)
            let idx = index_def(t, index);
            let mut read_to: Vec<Option<Vec<Value>>> = Vec::with_capacity(calls.len());
            for (r, c) in results.into_iter().zip(&calls) {
                match r {
                    Ok(s) => {
                        // (What the scan read, before WHERE: what it covered.)
                        read_to.push(match (c.limit, &s.last) {
                            (Some(l), Some(last)) if s.read >= l as usize => Some(t.index_key(idx, last)),
                            _ => None,
                        });
                        read += s.read;
                        out.push(s);
                    }
                    Err(e) => {
                        if e.code == 1213 {
                            txn.doomed = Some(e.clone());
                        }
                        return Err(e);
                    }
                }
            }
            for (c, to) in calls.into_iter().zip(read_to) {
                txn.mark_begun(c.shard);
                if unique_point {
                    txn.note_point(c.shard, t.space_id, index, c.key, c.bucket)?;
                } else if let Some(mut r) = c.range {
                    match to {
                        Some(k) if matches!(c.iter, Iter::Le | Iter::Lt) => {
                            r.lo = Some(k);
                            r.lo_incl = true;
                        }
                        Some(k) => {
                            r.hi = Some(k);
                            r.hi_incl = true;
                        }
                        None => {}
                    }
                    txn.note_range(c.shard, r)?;
                }
            }
        }
    }
    cluster.stats.rows_read.fetch_add(read as u64, AtomicOrdering::Relaxed);
    Ok(out)
}

/// Whether `fetch_stored_from` applies a filter to the rows of `path`:
/// those read by index scans (`scan_pages`); other paths return their rows
/// unfiltered.
fn pushes_filter(path: &AccessPath) -> bool {
    match path {
        AccessPath::Range { .. } | AccessPath::Full => true,
        AccessPath::Prefixes { keys, .. } => keys.len() == 1,
        AccessPath::Point { .. } | AccessPath::Union(_) => false,
    }
}

fn path_index(path: &AccessPath) -> u32 {
    match path {
        AccessPath::Point { index, .. } | AccessPath::Range { index, .. } | AccessPath::Prefixes { index, .. } => {
            *index
        }
        AccessPath::Full | AccessPath::Union(_) => 0,
    }
}

/// The index scans of a range access (`Range`, a `Prefixes` path with one
/// key, `Full`): one per shard it may touch, none when no row can match. A
/// scan starts at `resume` (index key, inclusive?) instead of its lower
/// bound when given.
fn range_calls(
    cluster: &Arc<Cluster>,
    t: &TableDef,
    path: &AccessPath,
    outer: &[Value],
    cx: &EvalCtx<'_>,
    resume: Option<(&[Value], bool)>,
    limit: Option<u32>,
) -> SqlResult<Vec<Call>> {
    range_calls_in(
        &cluster.bucket_map(),
        &cluster.live_shards().collect::<Vec<_>>(),
        t,
        path,
        outer,
        cx,
        resume,
        limit,
    )
}

/// Build every page against the same ownership and membership snapshot.
/// A pager must not switch owners or add lanes while retaining older pages.
#[allow(clippy::too_many_arguments)]
fn range_calls_in(
    map: &crate::buckets::BucketMap,
    shards: &[usize],
    t: &TableDef,
    path: &AccessPath,
    outer: &[Value],
    cx: &EvalCtx<'_>,
    resume: Option<(&[Value], bool)>,
    limit: Option<u32>,
) -> SqlResult<Vec<Call>> {
    let mut calls = Vec::new();
    match path {
        AccessPath::Prefixes { index, keys } => {
            // One prefix: an ordinary equality range.
            let idx = index_def(t, *index);
            let ks = eval_keys(t, idx, keys, outer, cx)?;
            let Some(prefix) = ks.into_iter().next() else {
                return Ok(calls);
            };
            let shards: Vec<(usize, Option<u32>)> = match route_bucket_in(map, t, idx, &prefix) {
                Some((s, b)) => vec![(s, Some(b))],
                None => shards.iter().map(|&s| (s, None)).collect(),
            };
            let (key, iter) = match resume {
                Some((k, incl)) => (k.to_vec(), if incl { Iter::Ge } else { Iter::Gt }),
                None => (prefix.clone(), Iter::Ge),
            };
            for (s, bucket) in shards {
                calls.push(Call {
                    shard: s,
                    bucket,
                    key: key.clone(),
                    iter,
                    limit,
                    end: Some((prefix.clone(), true)),
                    range: Some(RangeRead {
                        space: t.space_id,
                        index: idx.id,
                        lo: Some(prefix.clone()),
                        hi: Some(prefix.clone()),
                        lo_incl: true,
                        hi_incl: true,
                        bucket,
                    }),
                });
            }
        }
        AccessPath::Range {
            index,
            eq,
            null_eq,
            lo,
            hi,
            reverse,
        } => {
            let idx = index_def(t, *index);
            let Some(prefix) = range_prefix(t, idx, eq, null_eq, outer, cx)? else {
                return Ok(calls);
            };
            let next_col = idx.cols.get(eq.len()).copied();
            let bound = |b: &Option<(Expr, bool)>| -> SqlResult<Option<(Value, bool)>> {
                match (b, next_col) {
                    (Some((e, incl)), Some(c)) => {
                        let v = e.eval(outer, cx)?;
                        if v.is_null() {
                            return Ok(Some((Value::Null, false))); // comparisons with NULL match nothing
                        }
                        // An inexact bound is dropped: the residual filter keeps exact semantics.
                        Ok(t.lookup_part(c, &v).map(|x| (x, *incl)))
                    }
                    _ => Ok(None),
                }
            };
            let lo_v = bound(lo)?;
            let hi_v = bound(hi)?;
            if matches!(lo_v, Some((Value::Null, false))) || matches!(hi_v, Some((Value::Null, false))) {
                return Ok(calls);
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
            let shards: Vec<(usize, Option<u32>)> = match route_bucket_in(map, t, idx, &prefix) {
                Some((s, b)) => vec![(s, Some(b))],
                None => shards.iter().map(|&s| (s, None)).collect(),
            };
            let range = RangeRead {
                space: t.space_id,
                index: idx.id,
                lo: if key.is_empty() { None } else { Some(key.clone()) },
                hi: end.as_ref().map(|(k, _)| k.clone()),
                lo_incl,
                hi_incl: end.as_ref().map(|(_, i)| *i).unwrap_or(true),
                bucket: None,
            };
            // Backwards (ORDER BY ... DESC, issue #50): from the upper bound
            // down to the lower one.
            let (key, iter, end) = match (*reverse, resume) {
                (false, Some((k, incl))) => (k.to_vec(), if incl { Iter::Ge } else { Iter::Gt }, end),
                (false, None) => (key, iter, end),
                (true, Some((k, incl))) => (k.to_vec(), if incl { Iter::Le } else { Iter::Lt }, lower_end(&range)),
                (true, None) => match &end {
                    Some((k, incl)) => (k.clone(), if *incl { Iter::Le } else { Iter::Lt }, lower_end(&range)),
                    None => (Vec::new(), Iter::Le, lower_end(&range)),
                },
            };
            for (s, bucket) in shards {
                calls.push(Call {
                    shard: s,
                    bucket,
                    key: key.clone(),
                    iter,
                    limit,
                    end: end.clone(),
                    range: Some(RangeRead {
                        bucket,
                        ..range.clone()
                    }),
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
                bucket: None,
            };
            for &s in shards {
                calls.push(Call {
                    shard: s,
                    bucket: None,
                    key: Vec::new(),
                    iter: Iter::All,
                    limit,
                    end: None,
                    range: Some(range.clone()),
                });
            }
        }
        AccessPath::Point { .. } | AccessPath::Union(_) => unreachable!("not a range access"),
    }
    Ok(calls)
}

/// The equality prefix of a `Range` path, or None when no row can match: a
/// NULL matches nothing, except where `null_eq` marks the part (`IS NULL`,
/// `<=>`) and the column stores NULLs (issue #51).
fn range_prefix(
    t: &TableDef,
    idx: &IndexDef,
    eq: &[Expr],
    null_eq: &[bool],
    outer: &[Value],
    cx: &EvalCtx,
) -> SqlResult<Option<Vec<Value>>> {
    let mut prefix = Vec::with_capacity(eq.len() + 1);
    for (i, e) in eq.iter().enumerate() {
        let v = e.eval(outer, cx)?;
        let col = &t.columns[idx.cols[i]];
        if v.is_null() {
            if !(null_eq[i] && col.storage_nullable()) {
                return Ok(None);
            }
            prefix.push(Value::Null);
            continue;
        }
        match t.lookup_part(idx.cols[i], &v) {
            Some(x) => prefix.push(x),
            None => return Ok(None),
        }
    }
    Ok(Some(prefix))
}

/// Where a backwards scan of `r` stops: its lower bound (None: unbounded).
fn lower_end(r: &RangeRead) -> Option<(Vec<Value>, bool)> {
    r.lo.as_ref().map(|k| (k.clone(), r.lo_incl))
}

fn overlay(mode: &ReadMode<'_>, t: &TableDef, rows: Vec<Row>) -> Vec<Row> {
    match mode {
        ReadMode::Txn(txn) | ReadMode::Committed(txn) | ReadMode::Statement(txn) => txn.overlay(t, rows),
        ReadMode::Auto { .. } => rows,
    }
}

fn append_join(
    out: &mut Vec<Row>,
    left: Row,
    right: Vec<Row>,
    ta: &TableAccess,
    kind: JoinKind,
    on: Option<&Expr>,
    cx: &EvalCtx<'_>,
) -> SqlResult<()> {
    let mut matched = false;
    for r in right {
        let frame = crate::budget::Frame::new();
        frame.scope_sync(|| -> SqlResult<()> {
            crate::budget::work(1)?;
            crate::budget::charge(
                (crate::budget::row_bytes(&left)
                    + crate::budget::row_bytes(&r)
                    + (ta.offset as u64).saturating_mul(std::mem::size_of::<Value>() as u64))
                .saturating_mul(2),
            )?;
            let mut row = left.clone();
            row.resize(ta.offset, Value::Null);
            row.extend(r);
            if on.map(|on| on.eval_bool(&row, cx)).transpose()?.unwrap_or(true) {
                // The enclosing lookup page can now be released. Retained
                // join rows own their payload independently of that page.
                crate::budget::persistent(crate::budget::row_bytes(&row).saturating_mul(4))?;
                out.push(crate::budget::own_row(row));
                matched = true;
            }
            Ok(())
        })?;
    }
    if !matched && kind == JoinKind::Left {
        crate::budget::charge(
            (crate::budget::row_bytes(&left)
                + ((ta.offset + ta.table.columns.len()) as u64).saturating_mul(std::mem::size_of::<Value>() as u64))
            .saturating_mul(2),
        )?;
        let mut row = left;
        row.resize(ta.offset + ta.table.columns.len(), Value::Null);
        crate::budget::persistent(crate::budget::row_bytes(&row).saturating_mul(4))?;
        out.push(crate::budget::own_row(row));
    }
    Ok(())
}

/// Fetch every candidate rowset of an eligible join in one shard RPC.
/// Buffered parent writes need the ordinary executor: storage cannot derive
/// their child prefixes from rows that exist only in the coordinator.
async fn colocated_rows(
    cluster: &Arc<Cluster>,
    mode: &mut ReadMode<'_>,
    steps: &[(&TableAccess, JoinKind, Option<&Expr>)],
    cx: &EvalCtx<'_>,
) -> SqlResult<Option<Vec<Row>>> {
    use crate::txn::{JoinPart, JoinRead, JoinedRead};

    match &*mode {
        ReadMode::Committed(_) => return Ok(None),
        ReadMode::Txn(txn) | ReadMode::Statement(txn)
            if txn
                .writes
                .keys()
                .any(|(space, _)| steps.iter().any(|(t, _, _)| t.table.space_id == *space)) =>
        {
            return Ok(None);
        }
        _ => {}
    }
    let Some(plan) = colocated::plan(steps, cx)? else {
        return Ok(None);
    };
    let map = cluster.bucket_map();
    let bucket = crate::route::bucket_of(&plan.shard_key, map.count);
    let shard = map.shard(bucket);
    let route = Route::Buckets(vec![bucket]);
    let reads: Vec<JoinRead<'_>> = plan
        .reads
        .iter()
        .map(|r| JoinRead {
            table: r.table,
            parent: r.parent,
            parts: r
                .bindings
                .iter()
                .map(|b| match b {
                    colocated::Binding::Const(v) => JoinPart::Const(v.clone()),
                    colocated::Binding::Field(f) => JoinPart::Field(*f),
                })
                .collect(),
        })
        .collect();
    if matches!(mode, ReadMode::Auto { calls } if *calls > 0) {
        return Err(need_txn());
    }
    let result = match mode {
        ReadMode::Txn(txn) => {
            let st = txn.stream(cluster, shard).await?;
            cluster.stats.storage_reads.fetch_add(1, AtomicOrdering::Relaxed);
            crate::txn::select_join(&st.client, st.id, &reads, false, st.begin, &route).await
        }
        _ => {
            let client = read_client(cluster, shard).await?;
            cluster.stats.storage_reads.fetch_add(1, AtomicOrdering::Relaxed);
            crate::txn::select_join(&client, 0, &reads, true, false, &route).await
        }
    };
    let result = match result {
        Ok(JoinedRead::Unavailable) => return Ok(None),
        Ok(result) => result,
        Err(e) => {
            if e.code == 1213 {
                if let ReadMode::Txn(txn) = mode {
                    txn.doomed = Some(e.clone());
                }
            }
            return Err(e);
        }
    };
    // A successful incomplete RPC may already have opened the stream.
    // An unavailable procedure did nothing and must not mark it begun.
    match mode {
        ReadMode::Txn(txn) => txn.mark_begun(shard),
        ReadMode::Auto { calls } => *calls += 1,
        _ => {}
    }
    let JoinedRead::Complete(groups) = result else {
        return Ok(None);
    };
    crate::observe::grouped();
    let count: usize = groups.iter().map(|g| g.rows.len()).sum();
    cluster.stats.rows_read.fetch_add(count as u64, AtomicOrdering::Relaxed);
    if let ReadMode::Txn(txn) = mode {
        for (r, group) in reads.iter().zip(&groups) {
            for key in &group.keys {
                if key.len() == r.table.indexes[0].cols.len() {
                    txn.note_point(shard, r.table.space_id, 0, key.clone(), Some(bucket))?;
                } else {
                    txn.note_range(
                        shard,
                        RangeRead {
                            space: r.table.space_id,
                            index: 0,
                            lo: Some(key.clone()),
                            hi: Some(key.clone()),
                            lo_incl: true,
                            hi_incl: true,
                            bucket: Some(bucket),
                        },
                    )?;
                }
            }
        }
    }
    let mut groups = groups.into_iter();
    let mut rows = groups
        .next()
        .ok_or_else(|| SqlError::internal("empty co-located join"))?
        .rows;
    for ((ta, kind, on), group) in steps.iter().skip(1).zip(groups) {
        let lk = Lookup::of(cluster.nlive(), false, ta)
            .ok_or_else(|| SqlError::internal("co-located lookup no longer eligible"))?;
        let idx = index_def(&ta.table, lk.index);
        let wanted = rows
            .iter()
            .map(|left| lk.keys_of(&ta.table, idx, left, cx))
            .collect::<SqlResult<Vec<_>>>()?;
        let rights = distribute(&ta.table, idx, lk.len, wanted, group.rows, &[])?;
        let mut out = Vec::new();
        for (left, right) in rows.into_iter().zip(rights) {
            append_join(&mut out, left, right, ta, *kind, *on, cx)?;
        }
        rows = out;
    }
    Ok(Some(rows))
}

/// All rows produced by the FROM clause (joins applied, WHERE not yet).
async fn source_rows(
    cluster: &Arc<Cluster>,
    mode: &mut ReadMode<'_>,
    src: &Source,
    cx: &EvalCtx<'_>,
    allow_colocated: bool,
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
    if allow_colocated {
        if let Some(rows) = colocated_rows(cluster, mode, &steps, cx).await? {
            return Ok(rows);
        }
    }
    let mut rows = fetch(cluster, mode, steps[0].0, &[], cx).await?;
    for (ta, kind, on) in steps.iter().skip(1) {
        let mut out = Vec::new();
        let mut join = |left: Row, right: Vec<Row>| append_join(&mut out, left, right, ta, *kind, *on, cx);
        match Lookup::of(cluster.nlive(), matches!(mode, ReadMode::Committed(_)), ta) {
            None => {
                for left in rows {
                    let frame = crate::budget::Frame::new();
                    frame
                        .scope(Box::pin(async {
                            let right = fetch(cluster, mode, ta, &left, cx).await?;
                            join(left, right)
                        }))
                        .await?;
                }
            }
            Some(lk) => {
                // (fewer outer rows when each looks up several keys)
                let n = (JOIN_BATCH / lk.keys_per_row()).max(1);
                let mut rest = rows.into_iter().peekable();
                while rest.peek().is_some() {
                    let batch: Vec<Row> = rest.by_ref().take(n).collect();
                    let frame = crate::budget::Frame::new();
                    frame
                        .scope(Box::pin(async {
                            let rights = match lookup_batch(cluster, mode, ta, &lk, &batch, cx).await {
                                // A batch longer than one storage page at READ
                                // COMMITTED: each outer row's lookup on its own,
                                // as before, rather than the statement in a snapshot.
                                Err(e) if e.code == NEED_SNAPSHOT && matches!(mode, ReadMode::Statement(_)) => {
                                    let mut v = Vec::with_capacity(batch.len());
                                    for left in &batch {
                                        v.push(fetch(cluster, mode, ta, left, cx).await?);
                                    }
                                    v
                                }
                                r => r?,
                            };
                            for (left, right) in batch.into_iter().zip(rights) {
                                join(left, right)?;
                            }
                            Ok::<_, SqlError>(())
                        }))
                        .await?;
                }
            }
        }
        rows = out;
    }
    Ok(rows)
}

/// Outer rows whose inner rows one batched lookup reads (issue #84).
pub(crate) const JOIN_BATCH: usize = 256;

/// How the inner table of a join is reached by equality on an index key or
/// key prefix (the usual join): the lookups of a batch of outer rows can then
/// be read together, with one call per shard.
struct Lookup<'a> {
    index: u32,
    /// Number of index parts each key fixes.
    len: usize,
    unique: bool,
    /// Through the unique key's global index (`global_lookup`).
    global: bool,
    keys: LookupKeys<'a>,
}

enum LookupKeys<'a> {
    /// Several full or prefix keys (`Point`, `Prefixes`).
    Keys(&'a [Vec<Expr>]),
    /// One equality prefix (a `Range` without bounds or NULL-matching parts).
    Prefix(&'a [Expr]),
}

impl<'a> Lookup<'a> {
    /// None when the inner table is read otherwise (a range, a scan, a
    /// limit), each outer row then on its own. `committed`: for READ
    /// COMMITTED UPDATE / DELETE candidates, which skip global indexes.
    fn of(nlive: usize, committed: bool, ta: &'a TableAccess) -> Option<Lookup<'a>> {
        let t = &ta.table;
        if t.is_virtual() || ta.limit.is_some() {
            return None;
        }
        let lookup = match &ta.path {
            AccessPath::Point { index, keys } if !keys.is_empty() => Some(Lookup {
                index: *index,
                len: keys[0].len(),
                unique: true,
                // (as `fetch_stored_from` chooses)
                global: nlive > 1 && !committed && global_index(t, *index).is_some(),
                keys: LookupKeys::Keys(keys),
            }),
            AccessPath::Prefixes { index, keys } if !keys.is_empty() => Some(Lookup {
                index: *index,
                len: keys[0].len(),
                unique: false,
                global: false,
                keys: LookupKeys::Keys(keys),
            }),
            AccessPath::Range {
                index,
                eq,
                null_eq,
                lo: None,
                hi: None,
                ..
            } if !eq.is_empty() && !null_eq.contains(&true) => Some(Lookup {
                index: *index,
                len: eq.len(),
                unique: false,
                global: false,
                keys: LookupKeys::Prefix(eq),
            }),
            _ => None,
        }?;
        // Matching batches must distinguish the exact native index keys.
        // The SQL comparator uses f64 for DECIMAL, so adjacent large values
        // can compare equal and would be sent to the wrong outer row. Global
        // lookups also deduplicate base PKs with that comparator. Preserve
        // the per-row native lookup until exact association supports these.
        let decimal = |cols: &[usize]| cols.iter().any(|&c| t.columns[c].ty().base == BaseType::Decimal);
        let idx = index_def(t, lookup.index);
        if decimal(&idx.cols[..lookup.len]) || (lookup.global && decimal(&t.indexes[0].cols)) {
            return None;
        }
        Some(lookup)
    }

    fn keys_per_row(&self) -> usize {
        match self.keys {
            LookupKeys::Keys(keys) => keys.len(),
            LookupKeys::Prefix(_) => 1,
        }
    }

    /// The keys (as the index holds them) one outer row looks up, sorted.
    fn keys_of(&self, t: &TableDef, idx: &IndexDef, outer: &[Value], cx: &EvalCtx<'_>) -> SqlResult<Vec<Vec<Value>>> {
        match self.keys {
            LookupKeys::Keys(keys) => eval_keys(t, idx, keys, outer, cx),
            LookupKeys::Prefix(eq) => Ok(range_prefix(t, idx, eq, &vec![false; eq.len()], outer, cx)?
                .into_iter()
                .collect()),
        }
    }
}

/// The inner rows of a join for each outer row of `batch`, as `fetch` reads
/// them one outer row at a time, with one storage read for the whole batch
/// (issue #84): the distinct keys of every outer row, each sent to the shard
/// it determines (to every shard when it does not contain the shard key), in
/// one call per shard, in parallel. The reads are those of the separate
/// lookups, recorded in the transaction's read set alike; each outer row
/// gets its keys' rows in index order, then the transaction's own rows of
/// the table, which ON and WHERE filter as before. An outer row whose key
/// is NULL looks nothing up.
async fn lookup_batch(
    cluster: &Arc<Cluster>,
    mode: &mut ReadMode<'_>,
    ta: &TableAccess,
    lk: &Lookup<'_>,
    batch: &[Row],
    cx: &EvalCtx<'_>,
) -> SqlResult<Vec<Vec<Row>>> {
    let t = &ta.table;
    let idx = index_def(t, lk.index);
    let wanted: Vec<Vec<Vec<Value>>> = batch
        .iter()
        .map(|o| lk.keys_of(t, idx, o, cx))
        .collect::<SqlResult<_>>()?;
    // (Distinct by their bytes: values that only compare equal here, such
    // as DECIMALs equal as doubles, are each looked up; a row that two of
    // them find is kept once.)
    let mut seen: HashSet<Bytes> = HashSet::new();
    let mut keys: Vec<Vec<Value>> = Vec::new();
    for k in wanted.iter().flatten() {
        if seen.insert(crate::txn::key_bytes(k)) {
            keys.push(k.clone());
        }
    }
    keys.sort_by(|a, b| key_cmp(a, b));
    let stored = if keys.is_empty() {
        Vec::new()
    } else if lk.global {
        let g = global_index(t, lk.index).expect("a global lookup has a global index");
        global_lookup(cluster, mode, t, g, keys).await?
    } else {
        let mut pks: HashSet<Bytes> = HashSet::new();
        let rows = fetch_keys(cluster, mode, t, idx, keys, lk.unique, lk.unique.then_some(1)).await?;
        rows.into_iter()
            .filter(|r| pks.insert(crate::txn::key_bytes(&t.pk_key(r))))
            .collect()
    };
    let (stored, own): (Vec<Row>, Vec<Row>) = match &*mode {
        ReadMode::Txn(txn) | ReadMode::Committed(txn) | ReadMode::Statement(txn) => {
            (txn.unwritten(t, stored), txn.own_rows(t.space_id).cloned().collect())
        }
        ReadMode::Auto { .. } => (stored, Vec::new()),
    };
    distribute(t, idx, lk.len, wanted, stored, &own)
}

/// Each outer row's inner rows, from the rows a batched lookup read
/// (`stored`) and the keys each outer row looks up (`wanted`): the rows whose
/// first `len` index parts compare equal to one of its keys, in index order,
/// then `own`. Values that compare equal without being the same (DECIMALs
/// equal as doubles) may give a row more than its lookup would have: ON and
/// WHERE, which hold the equalities the lookup came from, filter them.
fn distribute(
    t: &TableDef,
    idx: &IndexDef,
    len: usize,
    wanted: Vec<Vec<Vec<Value>>>,
    stored: Vec<Row>,
    own: &[Row],
) -> SqlResult<Vec<Vec<Row>>> {
    crate::budget::decoded(0, stored.len(), len)?;
    let mut stored: Vec<(Vec<Value>, Row)> = stored
        .into_iter()
        .map(|r| {
            let mut k = t.index_key(idx, &r);
            k.truncate(len);
            (k, r)
        })
        .collect();
    stored.sort_by(|a, b| key_cmp(&a.0, &b.0)); // (stable: index order within a key)
    wanted
        .into_iter()
        .map(|ks| {
            let mut v: Vec<Row> = Vec::new();
            for k in ks {
                let lo = stored.partition_point(|(x, _)| key_cmp(x, &k) == std::cmp::Ordering::Less);
                let hi = stored.partition_point(|(x, _)| key_cmp(x, &k) != std::cmp::Ordering::Greater);
                crate::budget::charge(stored[lo..hi].iter().fold(0u64, |n, (_, r)| {
                    n.saturating_add(crate::budget::row_bytes(r).saturating_mul(2))
                }))?;
                v.extend(stored[lo..hi].iter().map(|(_, r)| r.clone()));
            }
            crate::budget::charge(own.iter().fold(0u64, |n, r| {
                n.saturating_add(crate::budget::row_bytes(r).saturating_mul(2))
            }))?;
            v.extend(own.iter().cloned());
            Ok(v)
        })
        .collect()
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

    fn add(&mut self, func: AggFunc, coll: crate::collation::Collation, v: Option<Value>) {
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
                if m.as_ref().is_none_or(|x| sort_cmp_coll(&v, x, coll).is_lt()) {
                    *m = Some(v);
                }
            }
            AggState::Max(m) => {
                if m.as_ref().is_none_or(|x| sort_cmp_coll(&v, x, coll).is_gt()) {
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
    let n = rows.len() as u64;
    crate::budget::work(
        n.saturating_mul(n.max(1).ilog2() as u64 + 1)
            .saturating_mul(ap.aggs.iter().map(|a| a.order.len() as u64).sum::<u64>() + 1)
            .saturating_mul(4),
    )?;
    let mut order: Vec<Vec<KeyVal>> = Vec::new();
    let mut groups: HashMap<Vec<KeyVal>, (Vec<Value>, Vec<AggState>, Vec<HashSet<KeyVal>>)> = HashMap::new();
    for row in &rows {
        let frame = crate::budget::Frame::new();
        frame.scope_sync(|| -> SqlResult<()> {
            crate::budget::work(1)?;
            crate::budget::charge(
                crate::budget::row_bytes(row)
                    .saturating_mul(16)
                    .saturating_add((ap.aggs.len() as u64).saturating_mul(512)),
            )?;
            let gv: Vec<Value> = ap.group.iter().map(|g| g.eval(row, cx)).collect::<SqlResult<_>>()?;
            let gk: Vec<KeyVal> = gv
                .iter()
                .zip(&ap.group_coll)
                .map(|(v, c)| key_val_coll(v, *c))
                .collect();
            if !groups.contains_key(&gk) {
                crate::budget::persistent(
                    crate::budget::row_bytes(&gv)
                        .saturating_mul(16)
                        .saturating_add((ap.aggs.len() as u64).saturating_mul(1024)),
                )?;
            }
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
                        let key = key_val_coll(x, a.coll);
                        if x.is_null() || entry.2[i].contains(&key) {
                            continue;
                        }
                        crate::budget::persistent(crate::budget::value_bytes(x).saturating_mul(16))?;
                        entry.2[i].insert(key);
                    }
                }
                if matches!(
                    a.func,
                    AggFunc::Min | AggFunc::Max | AggFunc::AnyValue | AggFunc::GroupConcat
                ) {
                    if let Some(x) = &v {
                        crate::budget::persistent(
                            crate::budget::value_bytes(x)
                                .saturating_mul(4)
                                .saturating_add((a.order.len() as u64).saturating_mul(128)),
                        )?;
                    }
                }
                if a.func == AggFunc::GroupConcat && !a.order.is_empty() {
                    if let Some(x) = v {
                        if !x.is_null() {
                            let keys: Vec<Value> =
                                a.order.iter().map(|(e, _)| e.eval(row, cx)).collect::<SqlResult<_>>()?;
                            crate::budget::persistent(crate::budget::row_bytes(&keys).saturating_mul(4))?;
                            if let AggState::Concat(parts) = &mut entry.1[i] {
                                parts.push((keys, x.to_text().unwrap_or_default()));
                            }
                        }
                    }
                    continue;
                }
                entry.1[i].add(a.func, a.coll, v);
            }
            Ok(())
        })?;
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
/// The columns of a plan, named with the values of its parameters. `names`:
/// the parameters that were literals naming a column by their text
/// (`crate::normalize::LitName`).
pub fn render_columns(cols: &Arc<[Column]>, params: &[Value], names: &[LitName]) -> Arc<[Column]> {
    if params.is_empty() || !cols.iter().any(|c| c.name.contains('?')) {
        return cols.clone();
    }
    cols.iter()
        .map(|c| {
            let mut c = c.clone();
            c.name = render_name(&c.name, params, names);
            c
        })
        .collect::<Vec<_>>()
        .into()
}

pub fn render_name(s: &str, params: &[Value], names: &[LitName]) -> String {
    let b = s.as_bytes();
    // A bare literal is named by its value (SELECT 'abc' -> abc); inside an
    // expression, string literals keep their quotes as typed (LOWER('ABC')).
    // A hexadecimal literal is named by its text (SELECT X'41' -> X'41'), and
    // so is a string with an introducer inside an expression.
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
                    let lit = names.iter().find(|l| l.param + 1 == n);
                    match v {
                        _ if lit.is_some_and(|l| !bare || l.bare_text) => out.push_str(&lit.unwrap().text),
                        Value::Str(x) | Value::Bin(x) if bare => out.push_str(&String::from_utf8_lossy(x)),
                        Value::Str(x) => {
                            out.push('\'');
                            out.push_str(&String::from_utf8_lossy(x).replace('\'', "''"));
                            out.push('\'');
                        }
                        Value::Bin(x) => {
                            out.push_str("X'");
                            for c in x.iter() {
                                out.push_str(&format!("{:02X}", c));
                            }
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

/// The kind of comparison a shard can evaluate for a column of type `ty`
/// against `v` exactly as [`crate::types::compare`] would: integers
/// (`Some(true)`) or strings by bytes (`Some(false)`).
fn storage_cmp_kind(ty: &SqlType, v: &Value) -> Option<bool> {
    use BaseType::*;
    if ty.is_integer() {
        return matches!(v, Value::Int(_) | Value::UInt(_)).then_some(true);
    }
    // Binary columns are left out (Lua sees their fields as varbinary), and
    // so are temporal ones, whose comparisons are not plain byte order.
    let text = matches!(ty.base, Char | VarChar | Text);
    (text && matches!(v, Value::Str(_))).then_some(false)
}

/// One comparison of a WHERE conjunct: column, operator, operand.
struct Atom<'e> {
    col: usize,
    op: CmpOp,
    arg: &'e Expr,
}

/// The comparisons a conjunct consists of, if it is a column compared with
/// constants by bytes (not under a case-insensitive collation); `IS [NOT]
/// NULL` and `IN` lists come back as storage predicates directly.
fn conjunct_atoms<'e>(e: &'e Expr, t: &TableDef) -> Option<(Vec<Atom<'e>>, Option<(usize, bool, &'e [Expr])>)> {
    let col = |x: &Expr| match x {
        Expr::Col(c) if *c < t.columns.len() => Some(*c),
        _ => None,
    };
    // Numeric comparisons ignore the connection's fallback collation.
    // Text still needs the exact byte-comparison contract below.
    let allowed = |c: usize, coll: &Collation| t.columns[c].ty().is_integer() || !coll.is_ci();
    match e {
        Expr::Cmp(a, op, b, coll) => {
            let (c, arg, op) = match (col(a), col(b)) {
                (Some(c), None) if b.is_constant() => (c, &**b, *op),
                (None, Some(c)) if a.is_constant() => (
                    c,
                    &**a,
                    match op {
                        CmpOp::Lt => CmpOp::Gt,
                        CmpOp::LtEq => CmpOp::GtEq,
                        CmpOp::Gt => CmpOp::Lt,
                        CmpOp::GtEq => CmpOp::LtEq,
                        o => *o,
                    },
                ),
                _ => return None,
            };
            if !allowed(c, coll) {
                return None;
            }
            Some((vec![Atom { col: c, op, arg }], None))
        }
        Expr::Between(x, lo, hi, false, coll) if lo.is_constant() && hi.is_constant() => {
            let c = col(x)?;
            if !allowed(c, coll) {
                return None;
            }
            Some((
                vec![
                    Atom {
                        col: c,
                        op: CmpOp::GtEq,
                        arg: lo,
                    },
                    Atom {
                        col: c,
                        op: CmpOp::LtEq,
                        arg: hi,
                    },
                ],
                None,
            ))
        }
        Expr::InList(x, list, false, coll) if list.iter().all(|v| v.is_constant()) => {
            let c = col(x)?;
            if !allowed(c, coll) {
                return None;
            }
            if list.len() == 1 {
                return Some((
                    vec![Atom {
                        col: c,
                        op: CmpOp::Eq,
                        arg: &list[0],
                    }],
                    None,
                ));
            }
            Some((Vec::new(), Some((c, false, list.as_slice()))))
        }
        Expr::IsNull(x, negated) => Some((Vec::new(), Some((col(x)?, *negated, &[][..])))),
        _ => None,
    }
}

/// The comparisons the index scans of a range access stand for: equality
/// on the prefix columns, the bounds on the next one.
fn path_atoms<'e>(t: &TableDef, path: &'e AccessPath) -> Vec<Atom<'e>> {
    let mut out = Vec::new();
    let (index, eq, null_eq, lo, hi) = match path {
        AccessPath::Range {
            index,
            eq,
            null_eq,
            lo,
            hi,
            ..
        } => (*index, eq.as_slice(), null_eq.as_slice(), lo.as_ref(), hi.as_ref()),
        AccessPath::Prefixes { index, keys } if keys.len() == 1 => (*index, keys[0].as_slice(), &[][..], None, None),
        _ => return out,
    };
    let idx = index_def(t, index);
    for (i, e) in eq.iter().enumerate() {
        if null_eq.get(i) == Some(&true) {
            continue; // `IS NULL` / `<=>`: not `col = e`, left to the residual
        }
        out.push(Atom {
            col: idx.cols[i],
            op: CmpOp::Eq,
            arg: e,
        });
    }
    if let Some(&c) = idx.cols.get(eq.len()) {
        if let Some((e, incl)) = lo {
            let op = if *incl { CmpOp::GtEq } else { CmpOp::Gt };
            out.push(Atom { col: c, op, arg: e });
        }
        // Without a lower bound the scan starts at the prefix, and NULLs,
        // which the index orders first, are inside it: the upper bound
        // selects them out only when the column cannot hold one.
        let col = &t.columns[c];
        let nullable = col.nullable || col.storage_nullable();
        if let (Some((e, incl)), false) = (hi, lo.is_none() && nullable) {
            let op = if *incl { CmpOp::LtEq } else { CmpOp::Lt };
            out.push(Atom { col: c, op, arg: e });
        }
    }
    out
}

/// The storage side of a count pushed down to the shards (issue #52): the
/// predicates each shard evaluates on the rows its scans cover, or None
/// when the statement is not such a count. It is one when the SELECT reads
/// one table through a range access, has no GROUP BY, computes only
/// `COUNT(*)`, `COUNT(constant)` or `COUNT(col)` (all the same count), and
/// every WHERE conjunct either is what the index scan already selects or a
/// column compared with constants that the shard can evaluate exactly as
/// SQL does here (integers, and strings by bytes).
fn count_preds(p: &SelectPlan, cx: &EvalCtx<'_>) -> SqlResult<Option<Vec<StoragePred>>> {
    let (Some(ap), Source::Table(ta), None) = (&p.agg, &p.source, p.lock) else {
        return Ok(None);
    };
    if !ap.group.is_empty()
        || ap.aggs.is_empty()
        || !matches!(ta.path, AccessPath::Range { .. } | AccessPath::Full)
            && !matches!(&ta.path, AccessPath::Prefixes { keys, .. } if keys.len() == 1)
    {
        return Ok(None);
    }
    let t = &ta.table;
    if t.is_virtual() {
        return Ok(None); // information_schema: rows from the catalog, not the shards
    }
    let ncols = t.columns.len();
    // The column every aggregate needs non-NULL, if any (COUNT of a
    // nullable column).
    let mut counted: Option<Option<usize>> = None;
    for a in &ap.aggs {
        let need = match (a.func, &a.arg, a.distinct) {
            (AggFunc::CountStar, _, _) => None,
            (AggFunc::Count, Some(e), false) if e.is_constant() && !e.eval(&[], cx)?.is_null() => None,
            (AggFunc::Count, Some(Expr::Col(c)), false) if *c < ncols => {
                let col = &t.columns[*c];
                (col.nullable || col.storage_nullable()).then_some(*c)
            }
            _ => return Ok(None),
        };
        if counted.is_some_and(|x| x != need) {
            return Ok(None);
        }
        counted = Some(need);
    }
    let mut preds = Vec::new();
    if let Some(Some(c)) = counted {
        preds.push(StoragePred {
            col: c,
            op: PredOp::NotNull,
            int: false,
        });
    }
    let Some(residual) = storage_preds(p, cx)? else {
        return Ok(None);
    };
    preds.extend(residual);
    Ok(Some(preds))
}

/// Exact, deterministic residual predicates shared by COUNT, projection,
/// and typed partial aggregates. An unsupported conjunct rejects the whole
/// pushdown; no partially filtered result can replace ordinary execution.
fn storage_preds(p: &SelectPlan, cx: &EvalCtx<'_>) -> SqlResult<Option<Vec<StoragePred>>> {
    let Source::Table(ta) = &p.source else { return Ok(None) };
    let t = &ta.table;
    let mut preds = Vec::new();
    let mut conj = Vec::new();
    if let Some(f) = &p.filter {
        split_conj(f, &mut conj);
    }
    let covered = path_atoms(t, &ta.path);
    for e in conj {
        let Some((atoms, other)) = conjunct_atoms(e, t) else {
            return Ok(None);
        };
        if let Some((c, not_null, list)) = other {
            if list.is_empty() {
                let op = if not_null { PredOp::NotNull } else { PredOp::IsNull };
                preds.push(StoragePred { col: c, op, int: false });
                continue;
            }
            let ty = t.columns[c].ty();
            let mut vs = Vec::with_capacity(list.len());
            let mut int = None;
            for x in list {
                let v = x.eval(&[], cx)?;
                if v.is_null() {
                    continue; // never equal; the row matches by another value or not at all
                }
                match storage_cmp_kind(ty, &v) {
                    Some(k) if int.is_none_or(|i| i == k) => int = Some(k),
                    _ => return Ok(None),
                }
                vs.push(v);
            }
            let Some(int) = int else { return Ok(None) };
            preds.push(StoragePred {
                col: c,
                op: PredOp::In(vs),
                int,
            });
            continue;
        }
        for a in atoms {
            let v = a.arg.eval(&[], cx)?;
            if a.op == CmpOp::NullSafeEq && v.is_null() {
                preds.push(StoragePred {
                    col: a.col,
                    op: PredOp::IsNull,
                    int: false,
                });
                continue;
            }
            let Some(int) = storage_cmp_kind(t.columns[a.col].ty(), &v) else {
                return Ok(None);
            };
            let op = match a.op {
                CmpOp::Eq | CmpOp::NullSafeEq => "eq",
                CmpOp::NotEq => "ne",
                CmpOp::Lt => "lt",
                CmpOp::LtEq => "le",
                CmpOp::Gt => "gt",
                CmpOp::GtEq => "ge",
            };
            // What the index scan selects needs no second look: its key is
            // this very value, which the column's type holds exactly (an
            // inexact bound is dropped from the scan).
            let same_op = |x: CmpOp| x == a.op || (x == CmpOp::Eq && a.op == CmpOp::NullSafeEq);
            // (A case-insensitive index holds weight strings, and an equality
            // compared by bytes may use it: its scan is wider than the test.)
            let exact = coerce_key_part(t.columns[a.col].ty(), &v).is_some() && !t.layout.colls[a.col].is_ci();
            if exact
                && covered
                    .iter()
                    .any(|k| k.col == a.col && same_op(k.op) && format!("{:?}", k.arg) == format!("{:?}", a.arg))
            {
                continue;
            }
            preds.push(StoragePred {
                col: a.col,
                op: PredOp::Cmp(op, v),
                int,
            });
        }
    }
    Ok(Some(preds))
}

fn split_conj<'e>(e: &'e Expr, out: &mut Vec<&'e Expr>) {
    match e {
        Expr::And(a, b) => {
            split_conj(a, out);
            split_conj(b, out);
        }
        other => out.push(other),
    }
}

/// A count computed by the shards (see [`count_preds`]), or None when the
/// statement must read its rows. Each shard counts what its index scans
/// cover; the transaction's own buffered writes are counted here: the
/// shards leave out the stored versions of the rows it wrote, and the rows
/// it wrote count when they satisfy the WHERE clause, as with the rows
/// [`Txn::overlay`] adds. Inside a transaction the scanned ranges are
/// recorded for validation, as reading the rows would.
async fn storage_count(
    cluster: &Arc<Cluster>,
    mode: &mut ReadMode<'_>,
    p: &SelectPlan,
    cx: &EvalCtx<'_>,
) -> SqlResult<Option<u64>> {
    let Some(preds) = count_preds(p, cx)? else {
        return Ok(None);
    };
    let Source::Table(ta) = &p.source else { unreachable!() };
    let t = &ta.table;
    let index = path_index(&ta.path);
    let mut calls = range_calls(cluster, t, &ta.path, &[], cx, None, None)?;
    let called = !calls.is_empty();
    for c in &mut calls {
        // A scan of one equality prefix: the shard counts it in its index.
        if c.iter == Iter::Ge && c.end.as_ref().is_some_and(|(k, incl)| *incl && *k == c.key) {
            c.iter = Iter::Eq;
            c.end = None;
        }
    }
    let (mut n, written): (u64, Vec<(usize, Vec<Value>)>) = match mode {
        ReadMode::Txn(txn) | ReadMode::Committed(txn) | ReadMode::Statement(txn) => {
            let mut mine = Vec::new();
            let mut n = 0;
            for (_, w) in txn
                .writes
                .range((t.space_id, Bytes::new())..)
                .take_while(|((s, _), _)| *s == t.space_id)
            {
                mine.push((w.shard, w.pk.clone()));
                if let Some(r) = &w.row {
                    let counted = match &p.filter {
                        Some(f) => f.eval_bool(r, cx)?,
                        None => true,
                    };
                    if counted && preds.iter().all(|q| q.op != PredOp::NotNull || !r[q.col].is_null()) {
                        n += 1;
                    }
                }
            }
            (n, mine)
        }
        ReadMode::Auto { .. } => (0, Vec::new()),
    };
    let exclude = |shard: usize| -> Vec<&[Value]> {
        written
            .iter()
            .filter(|(s, _)| *s == shard)
            .map(|(_, pk)| pk.as_slice())
            .collect()
    };
    let excl: Vec<Vec<&[Value]>> = calls.iter().map(|c| exclude(c.shard)).collect();
    let in_stream = matches!(mode, ReadMode::Txn(_));
    let map = cluster.bucket_map();
    let routes = calls
        .iter()
        .map(|c| read_route(&map, in_stream, c.shard, &[c.bucket]))
        .collect::<SqlResult<Vec<Route>>>()?;
    match mode {
        ReadMode::Auto { calls: made } => {
            *made += calls.len();
            if *made > 1 {
                return Err(need_txn());
            }
            for (c, route) in calls.iter().zip(&routes) {
                let client = read_client(cluster, c.shard).await?;
                let end = c.end.as_ref().map(|(k, i)| (k.as_slice(), *i));
                n += count_rows(
                    &client,
                    0,
                    t,
                    index,
                    &c.key,
                    c.iter,
                    end,
                    &preds,
                    &[],
                    true,
                    false,
                    route,
                )
                .await?;
            }
        }
        ReadMode::Committed(_) | ReadMode::Statement(_) => {
            let mut futs = Vec::new();
            for ((c, ex), route) in calls.iter().zip(&excl).zip(&routes) {
                let client = read_client(cluster, c.shard).await?;
                let preds = &preds;
                futs.push(async move {
                    let end = c.end.as_ref().map(|(k, i)| (k.as_slice(), *i));
                    count_rows(&client, 0, t, index, &c.key, c.iter, end, preds, ex, true, false, route).await
                });
            }
            for r in futures::future::join_all(futs).await {
                n += r?;
            }
        }
        ReadMode::Txn(txn) => {
            let mut futs = Vec::new();
            for ((c, ex), route) in calls.iter().zip(&excl).zip(&routes) {
                let st = txn.stream(cluster, c.shard).await?;
                let preds = &preds;
                futs.push(async move {
                    let end = c.end.as_ref().map(|(k, i)| (k.as_slice(), *i));
                    count_rows(
                        &st.client, st.id, t, index, &c.key, c.iter, end, preds, ex, false, st.begin, route,
                    )
                    .await
                });
            }
            for r in futures::future::join_all(futs).await {
                match r {
                    Ok(k) => n += k,
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
                if let Some(r) = c.range {
                    txn.note_range(c.shard, r)?;
                }
            }
        }
    }
    if called {
        cluster
            .stats
            .storage_counts
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    Ok(Some(n))
}

/// Execute a SELECT and return its projected rows.
pub async fn run_select(
    cluster: &Arc<Cluster>,
    mode: &mut ReadMode<'_>,
    p: &SelectPlan,
    cx: &EvalCtx<'_>,
) -> SqlResult<Vec<Row>> {
    let frame = crate::budget::Frame::new();
    let result = frame.scope(select_once(cluster, mode, p, cx)).await;
    if result.is_ok() {
        frame.retain_all();
    }
    drop(frame);
    match result {
        Err(e) if e.code == NEED_SNAPSHOT => match mode {
            ReadMode::Statement(txn) => committed_snapshot(cluster, txn, p, cx).await,
            _ => Err(SqlError::internal("a read asked for a snapshot outside READ COMMITTED")),
        },
        other => other,
    }
}

async fn select_once(
    cluster: &Arc<Cluster>,
    mode: &mut ReadMode<'_>,
    p: &SelectPlan,
    cx: &EvalCtx<'_>,
) -> SqlResult<Vec<Row>> {
    if let Some(n) = storage_count(cluster, mode, p, cx).await? {
        let ap = p.agg.as_ref().expect("a count is an aggregate");
        let row: Row = ap.aggs.iter().map(|_| Value::Int(n as i64)).collect();
        return finish_aggregated(vec![row], p, cx);
    }
    if cluster.cfg.query_pushdown {
        if let Some(row) = pushdown::reduce(cluster, mode, p, cx).await? {
            return finish_aggregated(vec![row], p, cx);
        }
        if let Some(rows) = pushdown::project(cluster, mode, p, cx).await? {
            return finish_select(rows, p, cx);
        }
    }
    let ordered = ordered_instead(cluster, p, cx).await?;
    if let Some(rows) = paged_select(cluster, mode, p, ordered.as_ref(), cx).await? {
        return Ok(rows);
    }
    if let Some(rows) = topk::select(cluster, mode, p, cx).await? {
        return Ok(rows);
    }
    let filter = |rows: Vec<Row>| -> SqlResult<Vec<Row>> {
        let Some(f) = &p.filter else { return Ok(rows) };
        let mut kept = Vec::with_capacity(rows.len());
        for r in rows {
            if crate::budget::Frame::new().scope_sync(|| f.eval_bool(&r, cx))? {
                kept.push(r);
            }
        }
        Ok(kept)
    };
    let rows = match &p.source {
        // One table read by index scans: WHERE is applied to each page as it
        // arrives, so a statement holds only the stored rows that match
        // (issue #77); then to the rows the transaction wrote. Each row is
        // evaluated once.
        Source::Table(ta) if pushes_filter(&ta.path) && !ta.table.is_virtual() => {
            let t = &ta.table;
            let mut rows = fetch_stored_from(cluster, mode, ta, &[], cx, None, p.filter.as_ref()).await?;
            if let ReadMode::Txn(txn) | ReadMode::Committed(txn) | ReadMode::Statement(txn) = mode {
                rows = txn.unwritten(t, rows); // (without WHERE the scan kept them)
                rows.extend(filter(txn.own_rows(t.space_id).cloned().collect())?);
            }
            rows
        }
        src => filter(source_rows(cluster, mode, src, cx, true).await?)?,
    };
    finish_select(rows, p, cx)
}

/// A single-table SELECT with LIMIT whose ORDER BY follows the index range
/// it reads (issue #41): read the range a page at a time, filter each page
/// and stop once LIMIT + OFFSET rows qualify, instead of reading, filtering
/// and sorting the whole range. Rows the transaction wrote are merged in.
/// None when the plan does not qualify.
///
/// Serializable reads run in one stream transaction, where pages continue
/// after the last row, and record each page's range up to its last row; an
/// autocommit read that needs a second page is retried in a read-only
/// transaction. READ COMMITTED reads see one committed state per storage
/// call, so a later page could miss a row that an update moved into the part
/// already read: there each attempt reads again from the start of the range
/// with a larger limit, and only the last one counts.
///
/// A range whose prefix does not fix the shard key is paged on every shard
/// at once (issue #86), and the shards' pages are merged in index order
/// (then primary-key order, as each index holds ties): only a shard whose
/// page the merge has used up, and whose next rows could still come first,
/// is asked for its next page. Each shard's pages are paged (or, under
/// READ COMMITTED, read again) as a single-shard range is.
///
/// `instead`: the access to page in place of the plan's (issue #81).
async fn paged_select(
    cluster: &Arc<Cluster>,
    mode: &mut ReadMode<'_>,
    p: &SelectPlan,
    instead: Option<&TableAccess>,
    cx: &EvalCtx<'_>,
) -> SqlResult<Option<Vec<Row>>> {
    let ta = match (instead, &p.source) {
        (Some(ta), _) | (None, Source::Table(ta)) => ta,
        _ => return Ok(None),
    };
    if p.order.is_empty() || p.agg.is_some() || p.having.is_some() || p.distinct || ta.limit.is_some() {
        return Ok(None);
    }
    let Some(limit) = eval_limit(&p.limit, cx)? else {
        return Ok(None);
    };
    let want = limit.saturating_add(eval_limit(&p.offset, cx)?.unwrap_or(0)) as usize;
    let Some(mut pager) = Pager::new(cluster, ta, &p.order, cx)? else {
        return Ok(None);
    };
    let t = &ta.table;
    let keep = |r: &Row| -> SqlResult<bool> {
        match &p.filter {
            Some(f) => f.eval_bool(r, cx),
            None => Ok(true),
        }
    };
    // (A page longer than one storage page reads the statement in a
    // snapshot instead: see `committed_snapshot`.)
    let restart = matches!(mode, ReadMode::Committed(_) | ReadMode::Statement(_));
    let lanes = pager.lanes.len();
    // Per shard: the rows kept so far, in index order, with their sort keys.
    let mut got: Vec<Vec<(Vec<Value>, Row)>> = (0..lanes).map(|_| Vec::new()).collect();
    let mut seen: Vec<HashSet<Bytes>> = (0..lanes).map(|_| HashSet::new()).collect();
    let mut page = vec![0usize; lanes];
    let mut need: Vec<usize> = if want == 0 { Vec::new() } else { (0..lanes).collect() };
    while !need.is_empty() {
        // The first page is four times what LIMIT needs (WHERE may reject
        // some rows), each later one at least twice the last, so a selective
        // predicate still finishes in a few calls. With several shards, a
        // first page of LIMIT + OFFSET rows per shard is what the merge
        // could use from it; a shard whose page WHERE thins is read again.
        let missing = want - merged(&pager, &got, want).0.len();
        let mut reads = Vec::with_capacity(need.len());
        for &l in &need {
            page[l] = match page[l] {
                0 if lanes > 1 => want,
                0 => want.saturating_mul(4),
                n if restart => n.saturating_mul(2).saturating_add(missing.saturating_mul(2)),
                n => n.saturating_mul(2).max(missing.saturating_mul(2)),
            }
            .clamp(1, u32::MAX as usize / 2);
            if restart && pager.lanes[l].last.is_some() {
                pager.lanes[l].restart();
                got[l].clear();
                seen[l].clear();
            }
            reads.push((l, page[l]));
        }
        for (&(l, _), more) in reads.iter().zip(pager.pages(cluster, mode, cx, &reads).await?) {
            for r in more {
                if seen[l].insert(crate::txn::key_bytes(&pk_of(t, &r, 0))) && keep(&r)? {
                    got[l].push((pager.sort_key(&r), r));
                }
            }
        }
        need = merged(&pager, &got, want).1;
    }
    let mut rows: Vec<Row> = merged(&pager, &got, want).0.into_iter().cloned().collect();
    if let ReadMode::Txn(txn) | ReadMode::Committed(txn) | ReadMode::Statement(txn) = mode {
        for r in txn.own_rows(t.space_id) {
            if keep(r)? {
                rows.push(r.clone());
            }
        }
    }
    // Stored rows are in ORDER BY order already; the (stable) sort places
    // the transaction's own rows among them.
    finish_select(rows, p, cx).map(Some)
}

/// The first `want` rows of the shards' pages `got` in index order, and,
/// when there are fewer, the shards whose next page could hold the next
/// row: those whose pages are used up, unless the next row comes before
/// the last key the shard read (its next page starts at that key).
fn merged<'r>(pager: &Pager, got: &'r [Vec<(Vec<Value>, Row)>], want: usize) -> (Vec<&'r Row>, Vec<usize>) {
    let t = &pager.ta.table;
    let before = |a: &[Value], b: &[Value]| {
        let o = key_cmp(a, b);
        if pager.reverse {
            o.is_gt()
        } else {
            o.is_lt()
        }
    };
    let mut at = vec![0usize; got.len()];
    let mut out = Vec::with_capacity(want.min(1024));
    while out.len() < want {
        let head = (0..got.len()).filter(|&l| at[l] < got[l].len()).reduce(|a, b| {
            if before(&got[b][at[b]].0, &got[a][at[a]].0) {
                b
            } else {
                a
            }
        });
        let dry = (0..got.len()).filter(|&l| at[l] == got[l].len() && !pager.lanes[l].done);
        let blocked: Vec<usize> = match head {
            None => dry.collect(),
            Some(h) => {
                let key = &got[h][at[h]].1;
                let ikey: Vec<Value> = pager.key_cols.iter().map(|&c| t.key_part(c, &key[c])).collect();
                dry.filter(|&l| pager.lanes[l].last.as_ref().is_none_or(|last| !before(&ikey, last)))
                    .collect()
            }
        };
        match head {
            Some(h) if blocked.is_empty() => {
                out.push(&got[h][at[h]].1);
                at[h] += 1;
            }
            _ => return (out, blocked),
        }
    }
    (out, Vec::new())
}

/// The access to page instead of the planned one (issue #81), when the plan
/// reads a range A by a longer equality prefix that it must read whole and
/// sort, and offers a range B, of an index that follows ORDER BY, that
/// would stop after LIMIT + OFFSET rows qualify. With n_a rows in A and n_b
/// in B, and the rows of A spread over B, paging B reads about want * n_b /
/// n_a rows: B wins when that is fewer than n_a. (On vinyl n_b is the size
/// of the index, an upper bound: the choice may then read A where B would
/// have been faster, as before, but not the other way. Where it can, vinyl
/// measures what paging B reads from B's first page instead, issue #93,
/// with the assumption of the counts: the rows of A are what B filters on,
/// so a selective condition outside A's index can make paging slower.)
/// Both prefixes must route to one shard, which counts them
/// (`tarn_estimate`); A must have no bound on its next column, which its
/// prefix count would overstate. WHERE
/// filters either way, so the choice changes only how many rows are read.
/// (A read retried in a transaction or a snapshot counts again.)
async fn ordered_instead(cluster: &Arc<Cluster>, p: &SelectPlan, cx: &EvalCtx<'_>) -> SqlResult<Option<TableAccess>> {
    let (Some(b_path), Source::Table(ta)) = (&p.ordered, &p.source) else {
        return Ok(None);
    };
    let (
        AccessPath::Range {
            index: a_index,
            eq: a_eq,
            null_eq: a_null,
            lo: None,
            hi: None,
            ..
        },
        AccessPath::Range {
            index: b_index,
            eq: b_eq,
            null_eq: b_null,
            ..
        },
    ) = (&ta.path, b_path)
    else {
        return Ok(None);
    };
    if p.agg.is_some() || p.having.is_some() || p.distinct || p.order.is_empty() {
        return Ok(None);
    }
    let Some(limit) = eval_limit(&p.limit, cx)? else {
        return Ok(None);
    };
    let want = limit.saturating_add(eval_limit(&p.offset, cx)?.unwrap_or(0));
    if want == 0 {
        return Ok(None);
    }
    let t = &ta.table;
    let (a_idx, b_idx) = (index_def(t, *a_index), index_def(t, *b_index));
    let b_whole = matches!(b_path, AccessPath::Range { lo: None, hi: None, .. });
    let b_reverse = matches!(b_path, AccessPath::Range { reverse: true, .. });
    let (Some(a_key), Some(b_key)) = (
        range_prefix(t, a_idx, a_eq, a_null, &[], cx)?,
        range_prefix(t, b_idx, b_eq, b_null, &[], cx)?,
    ) else {
        return Ok(None); // no row can match: nothing to choose
    };
    let shard = match (
        route_prefix(cluster, t, a_idx, &a_key),
        route_prefix(cluster, t, b_idx, &b_key),
    ) {
        (Some(a), Some(b)) if a == b => a,
        _ => return Ok(None),
    };
    // (A count that fails leaves the plan as it is: the read reports what
    // is wrong, if anything still is. A sample of B reads B as the page
    // would: its whole prefix, in the page's direction.)
    let sample = b_whole.then_some(if b_reverse { Iter::Req } else { Iter::Eq });
    let Ok(e) = estimate(cluster, shard, t, a_idx.id, &a_key, b_idx.id, &b_key, want, sample).await else {
        return Ok(None);
    };
    cluster.stats.path_estimates.fetch_add(1, AtomicOrdering::Relaxed);
    let pages = match e.cost {
        Some(cost) => e.n_a > cost,
        None => pages_fewer(want, e.n_a, e.n_b),
    };
    if !pages {
        return Ok(None);
    }
    Ok(Some(TableAccess {
        path: b_path.clone(),
        ..ta.clone()
    }))
}

/// Whether paging a range of `n_b` rows until `want` rows qualify reads
/// fewer rows than reading the `n_a` rows of the other range (and sorting
/// them), when the rows that qualify are those of the other range spread
/// evenly over the paged one: want * n_b / n_a < n_a.
fn pages_fewer(want: u64, n_a: u64, n_b: u64) -> bool {
    n_a > 0 && (want as u128) * (n_b as u128) < (n_a as u128) * (n_a as u128)
}

/// What `tarn_estimate` found: the rows of range A (counted up to where the
/// answer is certain), of range B, or (vinyl, from a sample of B) the rows
/// paging B would read.
struct Estimate {
    n_a: u64,
    n_b: u64,
    cost: Option<u64>,
}

/// `tarn_estimate` on `shard` for the `a_key` prefix of index `a_index` and
/// the `b_key` prefix of `b_index`, sampling B in the direction `sample`
/// when B is that whole prefix (see there).
#[allow(clippy::too_many_arguments)]
async fn estimate(
    cluster: &Arc<Cluster>,
    shard: usize,
    t: &TableDef,
    a_index: u32,
    a_key: &[Value],
    b_index: u32,
    b_key: &[Value],
    budget: u64,
    sample: Option<Iter>,
) -> SqlResult<Estimate> {
    let mut m = crate::mpw::Mp::new();
    m.arr(7)
        .uint(t.space_id as u64)
        .uint(a_index as u64)
        .vals(a_key)
        .uint(b_index as u64)
        .vals(b_key)
        .uint(budget);
    match sample {
        Some(Iter::Req) => m.str("REQ"),
        Some(_) => m.str("EQ"),
        None => m.nil(),
    };
    let (d, _wire) = crate::budget::cluster_read(cluster, shard, "tarn_estimate", &m.0).await?;
    let (_, mut rd) = crate::mpw::lua_status(&d)?;
    let mut n = || -> SqlResult<Option<u64>> {
        match rd.value()? {
            Value::Int(n) if n >= 0 => Ok(Some(n as u64)),
            Value::UInt(n) => Ok(Some(n)),
            Value::Null => Ok(None),
            other => Err(crate::mpw::proto(format!("bad estimate {:?}", other))),
        }
    };
    let bad = || crate::mpw::proto("bad estimate");
    let n_a = n()?.ok_or_else(bad)?;
    let n_b = n()?.ok_or_else(bad)?;
    Ok(Estimate { n_a, n_b, cost: n()? })
}

/// The rest of a SELECT over its filtered FROM rows: aggregation, HAVING,
/// ORDER BY, projection, DISTINCT, OFFSET/LIMIT.
pub(crate) fn finish_select(mut rows: Vec<Row>, p: &SelectPlan, cx: &EvalCtx<'_>) -> SqlResult<Vec<Row>> {
    if let Some(ap) = &p.agg {
        rows = aggregate(rows, ap, cx)?;
    }
    finish_aggregated(rows, p, cx)
}

/// `finish_select` after aggregation: HAVING, ORDER BY, projection,
/// DISTINCT, OFFSET/LIMIT.
fn finish_aggregated(mut rows: Vec<Row>, p: &SelectPlan, cx: &EvalCtx<'_>) -> SqlResult<Vec<Row>> {
    if let Some(h) = &p.having {
        let mut kept = Vec::with_capacity(rows.len());
        for r in rows {
            if crate::budget::Frame::new().scope_sync(|| h.eval_bool(&r, cx))? {
                kept.push(r);
            }
        }
        rows = kept;
    }
    if !p.order.is_empty() {
        let n = rows.len() as u64;
        crate::budget::work(
            n.saturating_mul(n.max(1).ilog2() as u64 + 1)
                .saturating_mul(p.order.len() as u64)
                .saturating_mul(4),
        )?;
        // Rows move into the sort without cloning their payload. Reserve
        // key vectors, indices and stable-sort scratch, not the input again.
        crate::budget::charge(n.saturating_mul(192))?;
        let mut keyed: Vec<(Vec<Value>, Row)> = Vec::with_capacity(rows.len());
        for r in rows {
            crate::budget::charge((p.order.len() as u64).saturating_mul(128))?;
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
        crate::budget::work(1)?;
        crate::budget::charge((p.project.len() as u64).saturating_mul(128))?;
        let pr: Row = p.project.iter().map(|e| e.eval(&r, cx)).collect::<SqlResult<_>>()?;
        crate::budget::row(&pr)?;
        if p.distinct {
            crate::budget::charge(crate::budget::row_bytes(&pr).saturating_mul(16))?;
        }
        if p.distinct
            && !seen.insert(
                pr.iter()
                    .enumerate()
                    .map(|(i, v)| key_val_coll(v, p.distinct_coll.get(i).copied().unwrap_or_default()))
                    .collect(),
            )
        {
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

fn sort_rows(rows: &mut [Row], order: &[(Expr, bool)], cx: &EvalCtx<'_>) -> SqlResult<()> {
    if order.is_empty() {
        return Ok(());
    }
    let n = rows.len() as u64;
    crate::budget::charge(n.saturating_mul(64 + (order.len() as u64).saturating_mul(128)))?;
    crate::budget::work(
        n.saturating_mul(n.max(1).ilog2() as u64 + 1)
            .saturating_mul(order.len() as u64)
            .saturating_mul(4),
    )?;
    let mut keyed = rows
        .iter()
        .enumerate()
        .map(|(i, row)| {
            let keys = order
                .iter()
                .map(|(e, _)| e.eval(row, cx))
                .collect::<SqlResult<Vec<_>>>()?;
            Ok((keys, i))
        })
        .collect::<SqlResult<Vec<_>>>()?;
    keyed.sort_by(|a, b| {
        for (i, (_, desc)) in order.iter().enumerate() {
            let o = sort_cmp(&a.0[i], &b.0[i]);
            let o = if *desc { o.reverse() } else { o };
            if o.is_ne() {
                return o;
            }
        }
        std::cmp::Ordering::Equal
    });
    let reordered = keyed
        .into_iter()
        .map(|(_, i)| std::mem::take(&mut rows[i]))
        .collect::<Vec<_>>();
    for (row, new) in rows.iter_mut().zip(reordered) {
        *row = new;
    }
    Ok(())
}

/// Primary key of the row of `t` at `offset` in `row`, as the index holds
/// it (row locks, the write buffer).
fn pk_of(t: &TableDef, row: &[Value], offset: usize) -> Vec<Value> {
    t.indexes[0]
        .cols
        .iter()
        .map(|&c| t.key_part(c, &row[offset + c]))
        .collect()
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
    sort_rows(&mut out, order, cx)?;
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
    if let Some(mut rows) = run_locking_selects(cluster, txn, &[(p, cx, lock)]).await? {
        return Ok(rows.pop().unwrap_or_default());
    }
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
            if crate::budget::Frame::new().scope_sync(|| f.eval_bool(&r, cx))? {
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
    // (Only when the transaction wrote no rows of the table: rows it
    // inserted or changed would need merging into index order. And only on
    // one shard: rows of several would need merging too.)
    let mut pager = match (&p.source, want) {
        (Source::Table(ta), Some(_)) if txn.own_rows(ta.table.space_id).next().is_none() => {
            Pager::new(cluster, ta, &p.order, cx)?.filter(|pg| pg.lanes.len() == 1)
        }
        _ => None,
    };
    let mut rows = match pager.as_mut() {
        Some(pg) => filter(pg.next_in(cluster, txn, rc, cx, page_size(0)).await?)?,
        None => {
            let rows = if rc {
                let mut mode = ReadMode::Committed(txn);
                source_rows(cluster, &mut mode, &p.source, cx, false).await?
            } else {
                let mut mode = ReadMode::Txn(txn);
                source_rows(cluster, &mut mode, &p.source, cx, false).await?
            };
            let mut rows = filter(rows)?;
            if plain {
                sort_rows(&mut rows, &p.order, cx)?;
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
                Some(pg) if !pg.lanes[0].done => {
                    let more = pg.next_in(cluster, txn, rc, cx, page_size(accepted.len())).await?;
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

/// The locking reads of one transaction's statements (one, or the parts of a
/// UNION ALL) in as few storage calls as possible, when each is a READ
/// COMMITTED `WHERE <primary key prefix> [ORDER BY <primary key>] LIMIT n
/// FOR ...` that `tarn_lock_range` does on its own (issue #49): the ranges
/// of one shard with the same options go in one call, in statement order,
/// and calls to different shards run concurrently when none of them waits
/// (`Txn::lock_ranges`). None when any of them needs the general path.
pub async fn run_locking_selects(
    cluster: &Arc<Cluster>,
    txn: &mut Txn,
    parts: &[(&SelectPlan, &EvalCtx<'_>, RowLock)],
) -> SqlResult<Option<Vec<Vec<Row>>>> {
    let mut ranges = Vec::with_capacity(parts.len());
    for &(p, cx, lock) in parts {
        match lock_range_of(cluster, txn, p, cx, lock)? {
            Some(r) => ranges.push(r),
            None => return Ok(None),
        }
    }
    // The ranges of one shard with the same options share a call, in
    // statement order. (Ranges of different calls lock independently: a
    // statement's parts may take their locks in any order, as concurrent
    // statements do.)
    let mut reqs: Vec<(usize, LockOpts, Vec<LockRange>)> = Vec::new();
    let mut at = Vec::with_capacity(ranges.len());
    for (s, o, r) in &ranges {
        let i = match reqs.iter().position(|(rs, ro, _)| rs == s && ro == o) {
            Some(i) => i,
            None => {
                reqs.push((*s, *o, Vec::new()));
                reqs.len() - 1
            }
        };
        at.push((i, reqs[i].2.len()));
        reqs[i].2.push(r.clone());
    }
    let mut got = txn.lock_ranges(cluster, &reqs).await?;
    let mut out = Vec::with_capacity(parts.len());
    for (k, &(p, cx, _)) in parts.iter().enumerate() {
        let (i, j) = at[k];
        let rows = got
            .get_mut(i)
            .and_then(|g| g.get_mut(j))
            .map(std::mem::take)
            .unwrap_or_default();
        let (shard, _, r) = &ranges[k];
        out.push(finish_range_lock(cluster, txn, p, cx, *shard, &r.table, rows).await?);
    }
    Ok(Some(out))
}

/// The most rows one `tarn_lock_range` call locks: a call runs on the
/// shard's transaction thread, yielding only between pages.
const RANGE_LOCK_MAX: usize = 256;

/// The `tarn_lock_range` request of a locking read, if it can do it: READ
/// COMMITTED; one table, read by an equality prefix of its primary key that
/// routes to one shard; a WHERE clause of nothing but that prefix (so that
/// no row the statement would not read is locked, or waited for); ORDER BY,
/// if any, the primary key's order; a LIMIT (with OFFSET) of at most
/// `RANGE_LOCK_MAX`; and no rows of the table
/// inserted or changed by the transaction (rows it deleted are left out).
fn lock_range_of(
    cluster: &Cluster,
    txn: &Txn,
    p: &SelectPlan,
    cx: &EvalCtx<'_>,
    lock: RowLock,
) -> SqlResult<Option<(usize, LockOpts, LockRange)>> {
    if txn.mode != crate::txn::Mode::ReadCommitted || p.agg.is_some() || p.having.is_some() || p.distinct {
        return Ok(None);
    }
    let Some(limit) = eval_limit(&p.limit, cx)? else {
        return Ok(None);
    };
    let n = limit as usize + eval_limit(&p.offset, cx)?.unwrap_or(0) as usize;
    if n > RANGE_LOCK_MAX {
        return Ok(None); // a long locking read pages from here, a batch per call
    }
    let Source::Table(ta) = &p.source else {
        return Ok(None);
    };
    let t = &ta.table;
    let eq = match &ta.path {
        AccessPath::Range {
            index: 0,
            eq,
            null_eq,
            lo: None,
            hi: None,
            reverse: false,
        } if !null_eq.contains(&true) => eq,
        AccessPath::Prefixes { index: 0, keys } if keys.len() == 1 => &keys[0],
        _ => return Ok(None),
    };
    let idx = index_def(t, 0);
    let Some(prefix) = eval_keys(t, idx, std::slice::from_ref(eq), &[], cx)?.pop() else {
        return Ok(None);
    };
    let text_colls: Vec<Option<Collation>> = t.columns.iter().map(|c| c.ty().is_text().then(|| c.coll())).collect();
    if n == 0 || prefix.is_empty() || !only_prefix(&p.filter, &idx.cols[..prefix.len()], &text_colls) {
        return Ok(None);
    }
    let Some(shard) = route_prefix(cluster, t, idx, &prefix) else {
        return Ok(None);
    };
    let mut next = prefix.len();
    for (e, desc) in &p.order {
        let Expr::Col(c) = e else { return Ok(None) };
        match idx.cols.iter().position(|x| x == c) {
            Some(pos) if pos < prefix.len() => {}
            Some(pos) if pos == next && !desc => next += 1,
            _ => return Ok(None),
        }
    }
    let mut skip = Vec::new();
    for (_, w) in txn
        .writes
        .range((t.space_id, Bytes::new())..)
        .take_while(|((sp, _), _)| *sp == t.space_id)
    {
        if w.row.is_some() {
            return Ok(None);
        }
        skip.push(w.pk.clone());
    }
    let o = LockOpts {
        exclusive: lock.exclusive,
        nowait: lock.nowait,
        skip: lock.skip,
        read: true,
    };
    Ok(Some((
        shard,
        o,
        LockRange {
            table: t.clone(),
            prefix,
            n,
            skip,
        },
    )))
}

/// Is the WHERE clause only equalities of the columns `cols` with constants,
/// each column once,
/// a text column compared under its own collation (`colls[c]`, None for
/// other columns: a comparison that only narrows the index range, as
/// `x = 'a' COLLATE utf8mb4_bin` on a case-insensitive column does, would
/// leave rows to lock that it then filters out)?
fn only_prefix(filter: &Option<Expr>, cols: &[usize], colls: &[Option<Collation>]) -> bool {
    fn conj(e: &Expr, cols: &[usize], colls: &[Option<Collation>], seen: &mut Vec<usize>) -> bool {
        match e {
            Expr::And(a, b) => conj(a, cols, colls, seen) && conj(b, cols, colls, seen),
            Expr::Cmp(a, CmpOp::Eq, b, coll) => match (&**a, &**b) {
                (Expr::Col(c), k) | (k, Expr::Col(c)) => {
                    // A column fixed twice (`k = 1 AND k = 2`) is left to the
                    // general path, which locks nothing for it.
                    let once = !seen.contains(c);
                    seen.push(*c);
                    once && cols.contains(c)
                        && k.is_constant()
                        && colls.get(*c).copied().flatten().is_none_or(|own| own == *coll)
                }
                _ => false,
            },
            _ => false,
        }
    }
    filter.as_ref().is_none_or(|f| conj(f, cols, colls, &mut Vec::new()))
}

/// The rows `tarn_lock_range` locked for a statement: WHERE checked again on
/// the versions read under the locks (a row that no longer matches is
/// unlocked), then ORDER BY, projection and LIMIT.
async fn finish_range_lock(
    cluster: &Arc<Cluster>,
    txn: &mut Txn,
    p: &SelectPlan,
    cx: &EvalCtx<'_>,
    shard: usize,
    t: &Arc<TableDef>,
    locked: Vec<Row>,
) -> SqlResult<Vec<Row>> {
    let mut kept = Vec::with_capacity(locked.len());
    let mut release = Vec::new();
    for row in locked {
        let ok = match &p.filter {
            Some(f) => f.eval_bool(&row, cx)?,
            None => true,
        };
        if ok {
            kept.push(row);
        } else {
            let key = pk_of(t, &row, 0);
            if txn.buffered(t.space_id, &key).is_none() {
                release.push((
                    shard,
                    LockKey {
                        table: t.clone(),
                        index: 0,
                        key,
                    },
                ));
            }
        }
    }
    unlock_items(cluster, txn, release).await;
    finish_select(kept, p, cx)
}

/// Paged reads of one index range, forwards or backwards, a page at a time,
/// each page starting after the last row returned. Only for scans whose
/// index order is the statement's ORDER BY. A range whose prefix does not
/// fix the shard key is read on every shard, a lane per shard, each paged
/// on its own (issue #86). Rows the transaction wrote or deleted are left
/// out; callers merge in its own versions.
struct Pager {
    /// Retained pages and refills must describe the same routing snapshot.
    /// In particular, READ COMMITTED scatter reads pin epoch and membership
    /// checks across rounds, even when the cluster currently has one shard.
    map: Arc<crate::buckets::BucketMap>,
    shards: Vec<usize>,
    ta: TableAccess,
    key_cols: Vec<usize>,
    /// The primary key: the next page starts strictly after the last key.
    /// Other indexes may repeat a key: the next page starts at it and skips
    /// the rows already returned with it.
    unique: bool,
    reverse: bool,
    lanes: Vec<Lane>,
}

/// Where the pages of one shard of a `Pager` have got to.
struct Lane {
    shard: usize,
    last: Option<Vec<Value>>,
    last_pks: HashSet<Bytes>,
    done: bool,
}

impl Lane {
    /// Read again from the start of the range.
    fn restart(&mut self) {
        self.last = None;
        self.last_pks.clear();
        self.done = false;
    }
}

impl Pager {
    fn new(
        cluster: &Arc<Cluster>,
        ta: &TableAccess,
        order: &[(Expr, bool)],
        cx: &EvalCtx<'_>,
    ) -> SqlResult<Option<Pager>> {
        let t = &ta.table;
        let (index, prefix, reverse) = match &ta.path {
            AccessPath::Range {
                index,
                eq,
                null_eq,
                reverse,
                ..
            } => {
                let idx = index_def(t, *index);
                (*index, range_prefix(t, idx, eq, null_eq, &[], cx)?, *reverse)
            }
            AccessPath::Prefixes { index, keys } if keys.len() == 1 => {
                let idx = index_def(t, *index);
                (
                    *index,
                    eval_keys(t, idx, std::slice::from_ref(&keys[0]), &[], cx)?.pop(),
                    false,
                )
            }
            _ => return Ok(None),
        };
        let idx = index_def(t, index);
        let Some(prefix) = prefix else {
            return Ok(None);
        };
        // Capture the map first: membership appends shards before publishing
        // a newer map. A newer map with fewer lanes could miss a joined shard.
        // Keep live positions too, so fresh scans skip removed shards and
        // refills never change the set of retained lanes.
        let map = cluster.bucket_map();
        let shards: Vec<usize> = cluster.live_shards().collect();
        let lanes: Vec<usize> = match route_bucket_in(&map, t, idx, &prefix) {
            Some((s, _)) => vec![s],
            None => shards.clone(),
        };
        // ORDER BY: columns fixed by the equality prefix, then the next index
        // columns in order, in the direction of the scan.
        let mut next = prefix.len();
        for (e, desc) in order {
            // (a case-insensitive column sorts by its key, as its index does)
            let c = match e {
                Expr::Col(c) => c,
                Expr::Key(k, coll) => match &**k {
                    Expr::Col(c) if t.layout.colls.get(*c) == Some(coll) => c,
                    _ => return Ok(None),
                },
                _ => return Ok(None),
            };
            match idx.cols.iter().position(|x| x == c) {
                Some(pos) if pos < prefix.len() => {}
                Some(pos) if pos == next && *desc == reverse => next += 1,
                _ => return Ok(None),
            }
        }
        Ok(Some(Pager {
            map,
            shards,
            ta: ta.clone(),
            key_cols: idx.cols.clone(),
            unique: idx.id == 0,
            reverse,
            lanes: lanes
                .into_iter()
                .map(|shard| Lane {
                    shard,
                    last: None,
                    last_pks: HashSet::new(),
                    done: false,
                })
                .collect(),
        }))
    }

    /// The next page of the only lane (a range on one shard).
    async fn next(
        &mut self,
        cluster: &Arc<Cluster>,
        mode: &mut ReadMode<'_>,
        cx: &EvalCtx<'_>,
        n: usize,
    ) -> SqlResult<Vec<Row>> {
        Ok(self
            .pages(cluster, mode, cx, &[(0, n)])
            .await?
            .pop()
            .unwrap_or_default())
    }

    /// The next page of each lane in `want` (lane, rows), read concurrently.
    async fn pages(
        &mut self,
        cluster: &Arc<Cluster>,
        mode: &mut ReadMode<'_>,
        cx: &EvalCtx<'_>,
        want: &[(usize, usize)],
    ) -> SqlResult<Vec<Vec<Row>>> {
        let t = &self.ta.table;
        let mut calls = Vec::with_capacity(want.len());
        let mut limits = Vec::with_capacity(want.len());
        for &(l, n) in want {
            let lane = &self.lanes[l];
            let limit = (n + if self.unique { 0 } else { lane.last_pks.len() }).min(u32::MAX as usize);
            let resume = lane.last.as_ref().map(|k| (k.as_slice(), !self.unique));
            let call = range_calls_in(
                &self.map,
                &self.shards,
                t,
                &self.ta.path,
                &[],
                cx,
                resume,
                Some(limit as u32),
            )?
            .into_iter()
            .find(|c| c.shard == lane.shard);
            match call {
                Some(c) => calls.push(c),
                None => {
                    // no row can match
                    self.lanes.iter_mut().for_each(|l| l.done = true);
                    return Ok(want.iter().map(|_| Vec::new()).collect());
                }
            }
            limits.push(limit);
        }
        let none = BTreeSet::new();
        let keep = Keep {
            topk: None,
            filter: None,
            cx,
            t,
            written: &none,
            project: None,
        };
        let index = path_index(&self.ta.path);
        let scans = read_calls(cluster, &self.map, mode, t, index, calls, false, keep).await?;
        let txn = match mode {
            ReadMode::Txn(txn) | ReadMode::Committed(txn) | ReadMode::Statement(txn) => Some(&**txn),
            ReadMode::Auto { .. } => None,
        };
        let mut out = Vec::with_capacity(want.len());
        for ((&(l, _), limit), s) in want.iter().zip(limits).zip(scans) {
            let lane = &mut self.lanes[l];
            if s.rows.len() < limit {
                lane.done = true;
            }
            let mut kept = Vec::with_capacity(s.rows.len());
            for r in s.rows {
                let key: Vec<Value> = self.key_cols.iter().map(|&c| t.key_part(c, &r[c])).collect();
                let pk_vals = pk_of(t, &r, 0);
                let pk = crate::txn::key_bytes(&pk_vals);
                let deleted = txn.is_some_and(|txn| txn.buffered(t.space_id, &pk_vals).is_some());
                if lane.last.as_ref() == Some(&key) {
                    if !lane.last_pks.insert(pk) {
                        continue; // returned by the previous page
                    }
                } else {
                    lane.last = Some(key);
                    lane.last_pks.clear();
                    lane.last_pks.insert(pk);
                }
                if !deleted {
                    kept.push(r); // (rows this transaction wrote or deleted are left out)
                }
            }
            out.push(kept);
        }
        Ok(out)
    }

    /// Where `r` sorts among the rows of every lane: its index key, then
    /// its primary key, as each shard's index orders them.
    fn sort_key(&self, r: &Row) -> Vec<Value> {
        let t = &self.ta.table;
        let mut k: Vec<Value> = self.key_cols.iter().map(|&c| t.key_part(c, &r[c])).collect();
        k.extend(pk_of(t, r, 0));
        k
    }

    /// `next` through the transaction: its streams, or (READ COMMITTED) the
    /// latest committed state.
    async fn next_in(
        &mut self,
        cluster: &Arc<Cluster>,
        txn: &mut Txn,
        rc: bool,
        cx: &EvalCtx<'_>,
        n: usize,
    ) -> SqlResult<Vec<Row>> {
        let mut mode = if rc {
            ReadMode::Committed(txn)
        } else {
            ReadMode::Txn(txn)
        };
        self.next(cluster, &mut mode, cx, n).await
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn columns_of_literals_are_named_as_mariadb_names_them() {
        use crate::normalize::{normalize, Mode};
        let names = |sql: &str, cols: &[&str]| -> Vec<String> {
            let n = normalize(sql, Mode::Literals).unwrap();
            cols.iter().map(|c| render_name(c, &n.params, &n.names)).collect()
        };
        assert_eq!(
            names(
                "SELECT X'41', x'4142', 'ab', HEX(X'00ff'), _binary'xy', HEX(_binary'xy'), N'cd'",
                &["?1", "?2", "?3", "HEX(?4)", "?5", "HEX(?6)", "?7"]
            ),
            vec!["X'41'", "x'4142'", "ab", "HEX(X'00ff')", "xy", "HEX(_binary'xy')", "cd"]
        );
        assert_eq!(names("SELECT X'61' = 'A'", &["?1 = ?2"]), vec!["X'61' = 'A'"]);
        // binary values without their text (statements made again from an
        // AST): in hexadecimal
        let p = [Value::Bin(Bytes::from_static(b"A"))];
        assert_eq!(
            (render_name("?1", &p, &[]), render_name("HEX(?1)", &p, &[])),
            ("A".into(), "HEX(X'41')".into())
        );
    }
    use crate::catalog::Catalog;
    use crate::normalize::{normalize, Mode};
    use crate::planner::{plan, PlanCtx};

    fn catalog() -> Catalog {
        let ct = "CREATE TABLE o (tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, closed_at DATETIME DEFAULT NULL, \
                  deleted TINYINT(1) NOT NULL DEFAULT '0', status VARCHAR(20), note VARCHAR(20) NOT NULL DEFAULT '', \
                  price DECIMAL(10,2), PRIMARY KEY (tenant_id, id), KEY st (tenant_id, closed_at, deleted))";
        let c0 = Catalog::default();
        let ctx = PlanCtx {
            statistics: None,
            catalog: &c0,
            db: Some("d"),
            params: &[],
            conn_coll: "utf8mb4_bin",
            default_coll: "utf8mb4_bin",
        };
        let Ok(Stmt::CreateTable { mut def, .. }) = plan(ct, &ctx) else {
            panic!()
        };
        def.space_id = 512;
        Catalog::new(1, vec!["d".into()], vec![*def])
    }

    /// The storage predicates of a pushed-down count, or None.
    fn preds(sql: &str) -> Option<Vec<StoragePred>> {
        let cat = catalog();
        let n = normalize(sql, Mode::Literals).unwrap();
        let ctx = PlanCtx {
            statistics: None,
            catalog: &cat,
            db: Some("d"),
            params: &n.params,
            conn_coll: "utf8mb4_bin",
            default_coll: "utf8mb4_bin",
        };
        let Ok(Stmt::Select(p)) = plan(&n.template, &ctx) else {
            panic!()
        };
        let cx = EvalCtx {
            params: &n.params,
            db: Some("d"),
            user: "u",
            conn_id: 1,
            last_insert_id: 0,
            found_rows: 0,
            row_count: 0,
            now: "2026-01-02 03:04:05",
        };
        count_preds(&p, &cx).unwrap()
    }

    #[test]
    fn numeric_storage_atoms_ignore_the_connection_collation() {
        let t = catalog().get("d", "o").unwrap();
        let ci = Collation::General { pad: true };
        let lit = || Box::new(Expr::Lit(Value::Int(1)));
        let integer = || Box::new(Expr::Col(1));
        for e in [
            Expr::Cmp(integer(), CmpOp::Eq, lit(), ci),
            Expr::Between(integer(), lit(), lit(), false, ci),
            Expr::InList(integer(), vec![Expr::Lit(Value::Int(1))], false, ci),
        ] {
            assert!(conjunct_atoms(&e, &t).is_some());
        }
        let text = Expr::Cmp(
            Box::new(Expr::Col(4)),
            CmpOp::Eq,
            Box::new(Expr::Lit(Value::str("x"))),
            ci,
        );
        assert!(conjunct_atoms(&text, &t).is_none());
    }

    #[test]
    fn count_pushdown() {
        // The index scan selects everything: nothing left for the shard.
        assert_eq!(preds("SELECT COUNT(*) FROM o WHERE tenant_id = 1"), Some(vec![]));
        assert_eq!(preds("SELECT COUNT(*) FROM o"), Some(vec![]));
        assert_eq!(
            preds("SELECT COUNT(id), COUNT(1) FROM o WHERE tenant_id = 1 AND id > 5"),
            Some(vec![])
        );
        // Residuals the shard evaluates.
        let p = preds("SELECT COUNT(*) FROM o WHERE tenant_id = 1 AND deleted = 0").unwrap();
        assert_eq!(p.len(), 1);
        assert!(p[0].col == 3 && p[0].int && p[0].op == PredOp::Cmp("eq", Value::Int(0)));
        let p =
            preds("SELECT COUNT(*) FROM o WHERE tenant_id = 1 AND closed_at IS NULL AND status IN ('a', 'b')").unwrap();
        assert_eq!(p.len(), 2);
        assert!(
            p[0].op == PredOp::IsNull && p[1].op == PredOp::In(vec![Value::str("a"), Value::str("b")]) && !p[1].int
        );
        let p = preds("SELECT COUNT(*) FROM o WHERE tenant_id = 1 AND 'x' < note").unwrap();
        assert!(p[0].col == 5 && p[0].op == PredOp::Cmp("gt", Value::str("x")));
        // COUNT(nullable column) counts non-NULL values.
        let p = preds("SELECT COUNT(status) FROM o WHERE tenant_id = 1").unwrap();
        assert!(p.len() == 1 && p[0].col == 4 && p[0].op == PredOp::NotNull);
        // Anything else reads the rows.
        for sql in [
            "SELECT COUNT(*) FROM o WHERE tenant_id = 1 GROUP BY deleted",
            "SELECT COUNT(DISTINCT status) FROM o WHERE tenant_id = 1",
            "SELECT COUNT(*), SUM(id) FROM o WHERE tenant_id = 1",
            "SELECT COUNT(status), COUNT(*) FROM o WHERE tenant_id = 1",
            "SELECT COUNT(*) FROM o WHERE tenant_id = 1 AND status LIKE 'a%'",
            "SELECT COUNT(*) FROM o WHERE tenant_id = 1 AND (status = 'a' OR id = 3)",
            "SELECT COUNT(*) FROM o WHERE tenant_id = 1 AND deleted = '0'",
            "SELECT COUNT(*) FROM o WHERE tenant_id = 1 AND price > 3",
            "SELECT COUNT(*) FROM o WHERE tenant_id = 1 AND id + 1 = 3",
            "SELECT COUNT(*) FROM o WHERE tenant_id = 1 AND id = tenant_id",
            "SELECT COUNT(*) FROM o WHERE tenant_id IN (1, 2)",
            "SELECT COUNT(*) FROM o WHERE tenant_id = 1 FOR UPDATE",
            "SELECT id FROM o WHERE tenant_id = 1",
        ] {
            assert_eq!(preds(sql), None, "{sql}");
        }
        // NULLs sort first in an index: an upper bound alone on a nullable
        // column stays a residual (a NOT NULL one is the scan's).
        let p = preds("SELECT COUNT(*) FROM o WHERE tenant_id < 3").unwrap();
        assert!(p.is_empty(), "tenant_id is NOT NULL: {p:?}");
        let cat = catalog();
        let t = cat.get("d", "o").unwrap();
        let k = Expr::Lit(Value::Int(5));
        let path = AccessPath::Range {
            index: t.index_by_name("st").unwrap().id,
            eq: vec![k.clone()],
            null_eq: vec![false],
            lo: None,
            hi: Some((Expr::Lit(Value::str("2025-01-01")), false)),
            reverse: false,
        };
        assert_eq!(
            path_atoms(&t, &path).len(),
            1,
            "an upper bound alone on nullable closed_at is not covered"
        );
        let path = AccessPath::Range {
            index: t.index_by_name("st").unwrap().id,
            eq: vec![k],
            null_eq: vec![false],
            lo: Some((Expr::Lit(Value::str("2024-01-01")), true)),
            hi: Some((Expr::Lit(Value::str("2025-01-01")), false)),
            reverse: false,
        };
        assert_eq!(path_atoms(&t, &path).len(), 3, "with a lower bound it is");
        // A bound the scan could not use exactly stays a residual.
        assert_eq!(preds("SELECT COUNT(*) FROM o WHERE tenant_id = 1 AND id > 2.5"), None);
    }

    fn eq(c: usize, v: i64) -> Expr {
        Expr::Cmp(
            Box::new(Expr::Col(c)),
            CmpOp::Eq,
            Box::new(Expr::Lit(Value::Int(v))),
            Collation::default(),
        )
    }

    fn cmp(a: Expr, op: CmpOp, b: Expr) -> Expr {
        Expr::Cmp(Box::new(a), op, Box::new(b), Collation::default())
    }

    #[test]
    fn only_prefix_accepts_equalities_of_the_prefix_columns() {
        assert!(only_prefix(&None, &[0], &[]));
        assert!(only_prefix(&Some(eq(0, 1)), &[0], &[]));
        let both = Expr::And(
            Box::new(eq(0, 1)),
            Box::new(cmp(Expr::Param(0), CmpOp::Eq, Expr::Col(1))),
        );
        assert!(only_prefix(&Some(both), &[0, 1], &[]));
    }

    #[test]
    fn only_prefix_refuses_anything_else() {
        // another column, a comparison other than =, a column on both sides, OR
        assert!(!only_prefix(&Some(eq(2, 1)), &[0, 1], &[]));
        let lt = cmp(Expr::Col(0), CmpOp::Lt, Expr::Lit(Value::Int(1)));
        assert!(!only_prefix(&Some(lt), &[0], &[]));
        let cols = cmp(Expr::Col(0), CmpOp::Eq, Expr::Col(1));
        assert!(!only_prefix(&Some(cols), &[0, 1], &[]));
        assert!(!only_prefix(
            &Some(Expr::Or(Box::new(eq(0, 1)), Box::new(eq(0, 2)))),
            &[0],
            &[]
        ));
        let partial = Expr::And(Box::new(eq(0, 1)), Box::new(eq(3, 2)));
        assert!(!only_prefix(&Some(partial), &[0], &[]));
        // a collation other than the column's own
        let ci = Collation::from_id(224).unwrap();
        assert!(only_prefix(&Some(eq(0, 1)), &[0], &[Some(Collation::default())]));
        assert!(!only_prefix(&Some(eq(0, 1)), &[0], &[Some(ci)]));
        assert!(only_prefix(&Some(eq(0, 1)), &[0], &[None]));
        // a column fixed twice
        let twice = Expr::And(Box::new(eq(0, 1)), Box::new(eq(0, 2)));
        assert!(!only_prefix(&Some(twice), &[0], &[]));
    }

    /// The inner table access of the join in `sql` over `o` and `l`.
    fn inner_access(sql: &str) -> TableAccess {
        let c0 = Catalog::default();
        let mut defs = Vec::new();
        for (i, ct) in [
            "CREATE TABLE o (id INT PRIMARY KEY, r INT, g INT)",
            "CREATE TABLE l (r INT NOT NULL, n INT NOT NULL, v INT, PRIMARY KEY (r, n), KEY kv (v))",
        ]
        .iter()
        .enumerate()
        {
            let ctx = PlanCtx {
                statistics: None,
                catalog: &c0,
                db: Some("d"),
                params: &[],
                conn_coll: "utf8mb4_bin",
                default_coll: "utf8mb4_bin",
            };
            let Ok(Stmt::CreateTable { mut def, .. }) = plan(ct, &ctx) else {
                panic!()
            };
            def.space_id = 512 + i as u32;
            defs.push(*def);
        }
        let cat = Catalog::new(1, vec!["d".into()], defs);
        let ctx = PlanCtx {
            statistics: None,
            catalog: &cat,
            db: Some("d"),
            params: &[],
            conn_coll: "utf8mb4_bin",
            default_coll: "utf8mb4_bin",
        };
        let Ok(Stmt::Select(p)) = plan(sql, &ctx) else {
            panic!("{sql}")
        };
        match &p.source {
            Source::Join { right, .. } => right.clone(),
            _ => panic!("{sql}: not a join"),
        }
    }

    #[test]
    fn batched_join_lookups() {
        // issue #84: equality on a key or a key prefix of the inner table
        let batched = |sql: &str| {
            let ta = inner_access(sql);
            Lookup::of(2, false, &ta).map(|lk| (lk.index, lk.len, lk.unique))
        };
        let pk = "SELECT * FROM o JOIN l ON l.r = o.r AND l.n = o.g";
        assert_eq!(batched(pk), Some((0, 2, true)));
        let prefix = "SELECT * FROM o LEFT JOIN l ON l.r = o.r";
        assert_eq!(batched(prefix), Some((0, 1, false)));
        assert_eq!(batched("SELECT * FROM o JOIN l ON l.v = o.g").map(|b| b.1), Some(1));
        assert_eq!(
            batched("SELECT * FROM o JOIN l ON l.r = o.r AND l.n IN (1, 2)"),
            Some((0, 2, true))
        );
        // a range, a scan or NULL-matching equality: each outer row on its own
        assert_eq!(batched("SELECT * FROM o JOIN l ON l.r = o.r AND l.n > o.g"), None);
        assert_eq!(batched("SELECT * FROM o JOIN l ON l.n = o.g"), None);
        assert_eq!(batched("SELECT * FROM o JOIN l ON l.v <=> o.g"), None);
    }

    #[test]
    fn batched_lookup_rows_go_to_their_outer_rows() {
        let ta = inner_access("SELECT * FROM o LEFT JOIN l ON l.r = o.r");
        let t = &ta.table;
        let idx = &t.indexes[0];
        let row = |r: i64, n: i64| vec![Value::Int(r), Value::Int(n), Value::Null];
        let k = |r: i64| vec![Value::Int(r)];
        // outer rows: r = 2, r = 1, r = NULL (no key), r = 2 again, r = 3
        // (no rows); stored rows arrive in any order (several shards)
        let wanted = vec![vec![k(2)], vec![k(1)], vec![], vec![k(2)], vec![k(3)]];
        let stored = vec![row(2, 1), row(1, 5), row(2, 0), row(1, 4)];
        let own = vec![row(9, 9)];
        let got = distribute(t, idx, 1, wanted, stored, &own).unwrap();
        assert_eq!(
            got,
            vec![
                vec![row(2, 1), row(2, 0), row(9, 9)], // (stable: as read within a key)
                vec![row(1, 5), row(1, 4), row(9, 9)],
                vec![row(9, 9)],
                vec![row(2, 1), row(2, 0), row(9, 9)],
                vec![row(9, 9)],
            ]
        );
        // several keys of one outer row: each key's rows, in key order
        let got = distribute(
            t,
            idx,
            2,
            vec![vec![
                vec![Value::Int(1), Value::Int(4)],
                vec![Value::Int(2), Value::Int(1)],
            ]],
            vec![row(2, 1), row(1, 4), row(1, 5)],
            &[],
        )
        .unwrap();
        assert_eq!(got, vec![vec![row(1, 4), row(2, 1)]]);
    }

    #[test]
    fn paging_the_ordered_index_or_reading_the_prefix() {
        // issue #81: 112,726 open rows of 300,000, a page of 51: paging
        // created_at reads ~136 rows, the status prefix 112,726
        assert!(pages_fewer(51, 112_726, 300_000));
        // 60 rows of one customer: reading them beats paging the tenant
        assert!(!pages_fewer(51, 60, 300_000));
        // an empty prefix is read in no time
        assert!(!pages_fewer(51, 0, 300_000));
        // the break-even point: want * n_b = n_a^2
        assert!(!pages_fewer(100, 100, 100));
        assert!(pages_fewer(99, 100, 100));
        assert!(!pages_fewer(u64::MAX, u64::MAX, u64::MAX));
    }

    #[test]
    fn merging_the_pages_of_several_shards() {
        // issue #86: shard pages merged in index order; a shard whose page
        // is used up is asked again only when its next rows could come next
        let cat = catalog();
        let t = cat.get("d", "o").unwrap();
        let row = |id: i64| -> Row {
            let mut r = vec![Value::Null; t.columns.len()];
            r[0] = Value::Int(1);
            r[1] = Value::Int(id);
            r
        };
        let pager = |reverse: bool, lanes: &[(Option<i64>, bool)]| Pager {
            map: Arc::new(crate::buckets::BucketMap::initial(3000, lanes.len())),
            shards: (0..lanes.len()).collect(),
            ta: TableAccess {
                table: t.clone(),
                alias: "o".into(),
                path: AccessPath::Full,
                offset: 0,
                limit: None,
            },
            key_cols: t.indexes[0].cols.clone(),
            unique: true,
            reverse,
            lanes: lanes
                .iter()
                .enumerate()
                .map(|(shard, &(last, done))| Lane {
                    shard,
                    last: last.map(|id| vec![Value::Int(1), Value::Int(id)]),
                    last_pks: HashSet::new(),
                    done,
                })
                .collect(),
        };
        let got = |pg: &Pager, ids: &[&[i64]]| -> Vec<Vec<(Vec<Value>, Row)>> {
            ids.iter()
                .map(|l| l.iter().map(|&id| (pg.sort_key(&row(id)), row(id))).collect())
                .collect()
        };
        let ids = |rows: Vec<&Row>| -> Vec<i64> {
            rows.iter()
                .map(|r| match r[1] {
                    Value::Int(i) => i,
                    _ => panic!(),
                })
                .collect()
        };
        // shard 0 has no more rows, shard 1 has
        let pg = pager(false, &[(Some(3), true), (Some(9), false)]);
        let g = got(&pg, &[&[1, 3], &[2, 9]]);
        let (m, more) = merged(&pg, &g, 3);
        assert_eq!((ids(m), more), (vec![1, 2, 3], vec![]));
        let (m, more) = merged(&pg, &g, 5);
        assert_eq!((ids(m), more), (vec![1, 2, 3, 9], vec![1]));
        // shard 1's page is used up at 2: 5 could follow from it
        let pg = pager(false, &[(Some(5), false), (Some(2), false)]);
        let g = got(&pg, &[&[5], &[2]]);
        let (m, more) = merged(&pg, &g, 2);
        assert_eq!((ids(m), more), (vec![2], vec![1]));
        // ... but not when it read up to 7 (WHERE rejected the rest)
        let pg = pager(false, &[(Some(5), false), (Some(7), false)]);
        let (m, more) = merged(&pg, &g, 2);
        assert_eq!((ids(m), more), (vec![2, 5], vec![]));
        // backwards
        let pg = pager(true, &[(Some(4), false), (Some(1), true)]);
        let g = got(&pg, &[&[8, 4], &[6, 1]]);
        let (m, more) = merged(&pg, &g, 3);
        assert_eq!((ids(m), more), (vec![8, 6, 4], vec![]));
        let (m, more) = merged(&pg, &g, 4);
        assert_eq!((ids(m), more), (vec![8, 6, 4], vec![0]));
        // no rows anywhere
        let pg = pager(false, &[(None, true), (None, true)]);
        let g = got(&pg, &[&[], &[]]);
        let (m, more) = merged(&pg, &g, 5);
        assert_eq!((ids(m), more), (vec![], vec![]));
    }

    #[test]
    fn paging_keeps_a_routed_lane_after_ownership_refresh() {
        use crate::buckets::{BucketMap, Report};

        let cat = catalog();
        let mut t = (*cat.get("d", "o").unwrap()).clone();
        t.shard_key = vec![0]; // a range of one tenant, with more than one primary key
        let cx = EvalCtx {
            params: &[],
            db: Some("d"),
            user: "u",
            conn_id: 1,
            last_insert_id: 0,
            found_rows: 0,
            row_count: 0,
            now: "2026-01-02 03:04:05",
        };
        let map = |owner: usize| {
            let reports = (0..2)
                .map(|s| Report {
                    count: 8,
                    epoch: 10 + owner as u64,
                    owned: if s == owner { vec![(1, 8)] } else { vec![] },
                    ..Report::default()
                })
                .collect();
            BucketMap::from_reports(reports, &HashMap::new()).unwrap()
        };
        let pinned = Arc::new(map(0));
        let refreshed = map(1);
        let prefix = vec![Value::Int(7)];
        let (_, bucket) = route_bucket_in(&pinned, &t, &t.indexes[0], &prefix).unwrap();
        assert_eq!(
            route_bucket_in(&refreshed, &t, &t.indexes[0], &prefix),
            Some((1, bucket))
        );
        let path = AccessPath::Range {
            index: 0,
            eq: vec![Expr::Lit(prefix[0].clone())],
            null_eq: vec![false],
            lo: None,
            hi: None,
            reverse: false,
        };
        let first = range_calls_in(&pinned, &[0, 1], &t, &path, &[], &cx, None, Some(20)).unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].shard, 0);
        let resume = vec![Value::Int(7), Value::Int(99)];
        let refill = range_calls_in(&pinned, &[0, 1], &t, &path, &[], &cx, Some((&resume, false)), Some(40)).unwrap();
        assert_eq!(refill.len(), 1, "a map refresh must not make the old lane disappear");
        assert_eq!(refill[0].shard, first[0].shard);
        assert_eq!(refill[0].key, resume);
        assert!(matches!(refill[0].iter, Iter::Gt));
        let changed = range_calls_in(&refreshed, &[0, 1], &t, &path, &[], &cx, None, Some(20)).unwrap();
        assert_eq!(changed[0].shard, 1, "the replacement map really changes the route");
        for c in first.iter().chain(&refill) {
            assert_eq!(c.bucket, Some(bucket));
            assert_eq!(c.range.as_ref().unwrap().bucket, Some(bucket));
            for in_stream in [false, true] {
                assert_eq!(
                    read_route(&pinned, in_stream, c.shard, &[c.bucket]).unwrap(),
                    Route::Buckets(vec![bucket])
                );
            }
        }
    }

    #[test]
    fn paging_keeps_scatter_epochs_and_membership() {
        use crate::buckets::BucketMap;

        let cat = catalog();
        let t = cat.get("d", "o").unwrap();
        let cx = EvalCtx {
            params: &[],
            db: Some("d"),
            user: "u",
            conn_id: 1,
            last_insert_id: 0,
            found_rows: 0,
            row_count: 0,
            now: "2026-01-02 03:04:05",
        };
        // No shard-key equality: one lane is still a scatter read when
        // the membership currently has only one shard.
        let path = AccessPath::Range {
            index: 0,
            eq: vec![],
            null_eq: vec![],
            lo: None,
            hi: None,
            reverse: false,
        };
        for nshards in [1, 2] {
            let shards: Vec<usize> = (0..nshards).collect();
            let mut old = BucketMap::initial(8, nshards);
            old.epochs = (0..nshards).map(|s| 10 + s as u64).collect();
            old.mver = 4;
            let pinned = Arc::new(old);
            let mut refreshed = BucketMap::initial(8, nshards + 1);
            refreshed.epochs = (0..=nshards).map(|s| 20 + s as u64).collect();
            refreshed.mver = 5;
            let first = range_calls_in(&pinned, &shards, &t, &path, &[], &cx, None, Some(20)).unwrap();
            let resume = vec![Value::Int(7), Value::Int(99)];
            let refill =
                range_calls_in(&pinned, &shards, &t, &path, &[], &cx, Some((&resume, false)), Some(40)).unwrap();
            assert_eq!(first.len(), nshards);
            assert_eq!(refill.len(), nshards);
            for (initial, later) in first.iter().zip(&refill) {
                assert_eq!(initial.shard, later.shard);
                assert_eq!(later.key, resume);
                for c in [initial, later] {
                    assert_eq!(c.bucket, None);
                    assert_eq!(
                        c.range.as_ref().unwrap().bucket,
                        None,
                        "commit must retain its all-shard membership fence"
                    );
                    let route = read_route(&pinned, false, c.shard, &[c.bucket]).unwrap();
                    assert_eq!(route, Route::Epoch(10 + c.shard as u64, 4));
                    assert_ne!(route, read_route(&refreshed, false, c.shard, &[c.bucket]).unwrap());
                    assert_eq!(read_route(&pinned, true, c.shard, &[c.bucket]).unwrap(), Route::None);
                }
            }
            // A member added after the map snapshot must fail closed if
            // included before the map catches up, rather than inventing an epoch.
            assert!(read_route(&pinned, false, nshards, &[None]).unwrap_err().moved);
        }
        // Removed positions stay in the map, but a new pager must not
        // contact their stopped storage. Live lanes need not be contiguous.
        let reports = vec![
            crate::buckets::Report {
                count: 8,
                epoch: 2,
                owned: vec![(1, 4)],
                ..Default::default()
            },
            crate::buckets::Report {
                count: 8,
                ..Default::default()
            },
            crate::buckets::Report {
                count: 8,
                epoch: 3,
                owned: vec![(5, 8)],
                ..Default::default()
            },
        ];
        let map = BucketMap::from_reports(reports, &HashMap::new()).unwrap();
        let calls = range_calls_in(&map, &[0, 2], &t, &path, &[], &cx, None, Some(20)).unwrap();
        assert_eq!(calls.iter().map(|c| c.shard).collect::<Vec<_>>(), vec![0, 2]);
        assert_eq!(
            read_route(&map, false, calls[0].shard, &[None]).unwrap(),
            Route::Epoch(2, 0)
        );
        assert_eq!(
            read_route(&map, false, calls[1].shard, &[None]).unwrap(),
            Route::Epoch(3, 0)
        );
    }

    /// A resolved table access, including the metadata allocated for its
    /// global indexes at CREATE, without opening any storage connections.
    fn decimal_join_access(create: &str, index: &str, len: usize, point: bool) -> TableAccess {
        let cat = Catalog::default();
        let ctx = PlanCtx {
            statistics: None,
            catalog: &cat,
            db: Some("d"),
            params: &[],
            conn_coll: "utf8mb4_bin",
            default_coll: "utf8mb4_bin",
        };
        let Stmt::CreateTable { mut def, .. } = plan(create, &ctx).unwrap() else {
            panic!("{create}")
        };
        def.space_id = 512;
        for i in 0..def.indexes.len() {
            if def.needs_global(&def.indexes[i]) {
                def.indexes[i].global = Some(513 + i as u32);
            }
        }
        def.resolve_types().unwrap();
        let index = def.index_by_name(index).unwrap().id;
        let eq: Vec<Expr> = (0..len).map(Expr::Col).collect();
        let path = if point {
            AccessPath::Point { index, keys: vec![eq] }
        } else {
            AccessPath::Range {
                index,
                eq,
                null_eq: vec![false; len],
                lo: None,
                hi: None,
                reverse: false,
            }
        };
        TableAccess {
            table: Arc::from(def),
            alias: "l".into(),
            path,
            offset: 0,
            limit: None,
        }
    }

    #[test]
    fn batched_join_decimal_probes_keep_the_native_lookup() {
        let create = "CREATE TABLE l (d DECIMAL(20,0) PRIMARY KEY, v INT)";
        for point in [false, true] {
            let ta = decimal_join_access(create, "PRIMARY", 1, point);
            let t = &ta.table;
            // These adjacent integers need distinct native probes; neither
            // coercion nor key serialization may round them into one key.
            let keys: Vec<Vec<Value>> = ["9007199254740992", "9007199254740993"]
                .iter()
                .map(|d| {
                    let v = Value::Decimal((*d).into());
                    let native = t.lookup_part(0, &v).unwrap();
                    assert_eq!(native, v);
                    vec![native]
                })
                .collect();
            assert_ne!(crate::txn::key_bytes(&keys[0]), crate::txn::key_bytes(&keys[1]));
            for (nlive, committed) in [(1, false), (2, false), (2, true)] {
                assert!(
                    Lookup::of(nlive, committed, &ta).is_none(),
                    "each outer row must retain its exact native DECIMAL probe"
                );
            }
        }
    }

    #[test]
    fn batched_join_can_leave_a_decimal_suffix_unprobed() {
        let create = "CREATE TABLE l (r INT, d DECIMAL(20,0), PRIMARY KEY (r, d)) PARTITION BY KEY(r)";
        let prefix = decimal_join_access(create, "PRIMARY", 1, false);
        let lookup = Lookup::of(2, false, &prefix).expect("the probed INT prefix is exact");
        assert_eq!(
            (lookup.index, lookup.len, lookup.unique, lookup.global),
            (0, 1, false, false)
        );
        // The same DECIMAL suffix becomes unsafe once the lookup fixes it,
        // whether the access is represented as a prefix or as a full point.
        for point in [false, true] {
            let full = decimal_join_access(create, "PRIMARY", 2, point);
            assert!(Lookup::of(2, false, &full).is_none());
        }
    }

    #[test]
    fn batched_join_decimal_pk_guard_applies_only_to_global_lookups() {
        let ta = decimal_join_access(
            "CREATE TABLE l (id DECIMAL(20,0) PRIMARY KEY, h VARCHAR(10), UNIQUE KEY uh (h))",
            "uh",
            1,
            true,
        );
        let AccessPath::Point { index, .. } = &ta.path else {
            panic!()
        };
        assert!(
            global_index(&ta.table, *index).is_some(),
            "exercise a real global-index definition"
        );
        assert!(
            Lookup::of(2, false, &ta).is_none(),
            "global PK deduplication cannot collapse DECIMAL holders"
        );
        // A singleton cluster and RC write candidates read the physical
        // text index instead, where the base PKs are deduplicated by bytes.
        for (nlive, committed) in [(1, false), (1, true), (2, true)] {
            let lookup = Lookup::of(nlive, committed, &ta).expect("the local text-key lookup stays batchable");
            assert!(!lookup.global);
            assert_eq!((lookup.len, lookup.unique), (1, true));
        }
        let exact_pk = decimal_join_access(
            "CREATE TABLE l (id BIGINT PRIMARY KEY, h VARCHAR(10), UNIQUE KEY uh (h))",
            "uh",
            1,
            true,
        );
        assert!(
            Lookup::of(2, false, &exact_pk).unwrap().global,
            "ordinary global lookups remain batched"
        );
    }
}
