//! Transactions: the coordinator side of tla/TarnCommit.tla.
//!
//! Execution phase: reads go to each shard inside an IPROTO stream (one per
//! participant, opened lazily with BEGIN), where Tarantool's MVCC tracks
//! them; the read set is also recorded here because commit-time lock checks
//! need it. Writes are buffered in `writes` and overlaid on reads.
//!
//! Commit (`commit`):
//!   * one shard, writes      -> `tarn_commit_local` (TarnCommit!CommitLocal)
//!   * no writes              -> `tarn_validate` on every participant
//!                               (TarnCommit!ValidateRO / SnapshotRead)
//!   * several shards, writes -> parallel-commit 2PC: `tarn_prepare` on all
//!                               participants (the decision shard also writes
//!                               the STAGING record), then `tarn_apply`
//!                               (TarnCommit!Prepare, ApplyAt). The client is
//!                               acknowledged after APPLY unless
//!                               `ack_on_prepare` is set.
//! The pure decision logic (what to do with a set of votes, when it is safe
//! to apply/abort) is `tarn_verified::coord`, proven in Verus to only decide
//! COMMIT on unanimous YES and never to decide both ways.

use crate::catalog::TableDef;
use crate::cluster::Cluster;
use crate::mpw::{from_iproto, lua_status, Mp, Rd};
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tarn_common::{SqlError, SqlResult, Value};
use tarn_iproto::{Client, Iter, TxnIsolation};
use tarn_verified::coord::{self, Decision, Vote};

pub type Row = Vec<Value>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WOp {
    Insert,
    Replace,
    Delete,
}

#[derive(Clone, Debug)]
pub struct BufWrite {
    pub shard: usize,
    pub table: Arc<TableDef>,
    pub op: WOp,
    /// Full new row (columns only) for insert/replace.
    pub row: Option<Row>,
    pub pk: Vec<Value>,
}

#[derive(Clone, Debug)]
pub struct RangeRead {
    pub space: u32,
    pub index: u32,
    pub lo: Option<Vec<Value>>,
    pub hi: Option<Vec<Value>>,
    pub lo_incl: bool,
    pub hi_incl: bool,
}

pub struct Stream {
    pub client: Client,
    pub id: u64,
}

pub struct Txn {
    pub id: String,
    pub ts: f64,
    pub explicit: bool,
    pub streams: BTreeMap<usize, Stream>,
    pub writes: BTreeMap<(u32, Bytes), BufWrite>,
    pub rpoints: BTreeMap<usize, Vec<(u32, u32, Vec<Value>)>>,
    pub rranges: BTreeMap<usize, Vec<RangeRead>>,
    /// Set after an error that leaves the transaction unusable (e.g. a
    /// conflict detected mid-transaction); COMMIT then fails.
    pub doomed: Option<SqlError>,
}

/// msgpack bytes of a key: the canonical identity of a row in the buffer
/// (identical to Lua's msgpack.encode of the key array on the shard).
pub fn key_bytes(key: &[Value]) -> Bytes {
    let mut m = Mp::new();
    m.vals(key);
    m.bytes()
}

impl Txn {
    pub fn new(cluster: &Cluster, explicit: bool) -> Txn {
        Txn {
            id: cluster.new_txid(),
            ts: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs_f64()).unwrap_or(0.0),
            explicit,
            streams: BTreeMap::new(),
            writes: BTreeMap::new(),
            rpoints: BTreeMap::new(),
            rranges: BTreeMap::new(),
            doomed: None,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.streams.is_empty() && self.writes.is_empty()
    }

    /// Stream on `shard`, opened with BEGIN on first use.
    pub async fn stream(&mut self, cluster: &Cluster, shard: usize) -> SqlResult<(Client, u64)> {
        if let Some(s) = self.streams.get(&shard) {
            return Ok((s.client.clone(), s.id));
        }
        let client = cluster.shards[shard].client(cluster).await?;
        let id = client.next_stream_id();
        client.begin(id, Some(60.0), Some(TxnIsolation::ReadConfirmed)).await.map_err(from_iproto)?;
        self.streams.insert(shard, Stream { client: client.clone(), id });
        Ok((client, id))
    }

    pub fn note_point(&mut self, shard: usize, space: u32, index: u32, key: Vec<Value>) {
        self.rpoints.entry(shard).or_default().push((space, index, key));
    }

    pub fn note_range(&mut self, shard: usize, r: RangeRead) {
        self.rranges.entry(shard).or_default().push(r);
    }

    pub fn buffer(&mut self, w: BufWrite) {
        let k = (w.table.space_id, key_bytes(&w.pk));
        // An insert after a buffered delete of the same key is a replace.
        let w = match (self.writes.get(&k), w.op) {
            (Some(prev), WOp::Insert) if prev.op == WOp::Delete => BufWrite { op: WOp::Replace, ..w },
            (Some(prev), WOp::Replace) if prev.op == WOp::Insert => BufWrite { op: WOp::Insert, ..w },
            _ => w,
        };
        self.writes.insert(k, w);
    }

    /// The buffered version of a row: Some(Some(row)) if written, Some(None)
    /// if deleted, None if untouched by this transaction.
    pub fn buffered(&self, space: u32, pk: &[Value]) -> Option<Option<&Row>> {
        self.writes.get(&(space, key_bytes(pk))).map(|w| w.row.as_ref())
    }

    /// Apply this transaction's writes to rows read from storage for table
    /// `t`: drop rows it overwrote/deleted, add every live row it wrote.
    /// Callers re-check predicates, so including extra rows is harmless.
    pub fn overlay(&self, t: &TableDef, rows: Vec<Row>) -> Vec<Row> {
        let lo = (t.space_id, Bytes::new());
        let mine: Vec<&BufWrite> = self
            .writes
            .range(lo..)
            .take_while(|((s, _), _)| *s == t.space_id)
            .map(|(_, w)| w)
            .collect();
        if mine.is_empty() {
            return rows;
        }
        let pk_cols = &t.indexes[0].cols;
        let written: BTreeSet<Bytes> = mine.iter().map(|w| key_bytes(&w.pk)).collect();
        let mut out: Vec<Row> = rows
            .into_iter()
            .filter(|r| {
                let pk: Vec<Value> = pk_cols.iter().map(|&c| r[c].clone()).collect();
                !written.contains(&key_bytes(&pk))
            })
            .collect();
        for w in mine {
            if let Some(r) = &w.row {
                out.push(r.clone());
            }
        }
        out
    }

    fn participants(&self) -> BTreeSet<usize> {
        let mut p: BTreeSet<usize> = self.streams.keys().copied().collect();
        for w in self.writes.values() {
            p.insert(w.shard);
        }
        p
    }

    fn encode_writes(&self, cluster: &Cluster, shard: usize, m: &mut Mp) {
        let ws: Vec<&BufWrite> = self.writes.values().filter(|w| w.shard == shard).collect();
        m.arr(ws.len());
        for w in ws {
            m.arr(3);
            m.str(match w.op {
                WOp::Insert => "insert",
                WOp::Replace => "replace",
                WOp::Delete => "delete",
            });
            m.uint(w.table.space_id as u64);
            match &w.row {
                Some(r) if w.op != WOp::Delete => {
                    m.arr(r.len() + 1);
                    for v in r {
                        m.val(v);
                    }
                    let sk: Vec<Value> = w.table.shard_key.iter().map(|&c| r[c].clone()).collect();
                    m.uint(cluster.bucket_of(&sk) as u64);
                }
                _ => {
                    m.vals(&w.pk);
                }
            }
        }
    }

    fn encode_reads(&self, shard: usize, m: &mut Mp) {
        let pts = self.rpoints.get(&shard).map(|v| v.as_slice()).unwrap_or(&[]);
        m.arr(pts.len());
        for (s, i, k) in pts {
            m.arr(3).uint(*s as u64).uint(*i as u64).vals(k);
        }
        let rs = self.rranges.get(&shard).map(|v| v.as_slice()).unwrap_or(&[]);
        m.arr(rs.len());
        for r in rs {
            m.arr(6).uint(r.space as u64).uint(r.index as u64);
            m.opt_vals(&r.lo).opt_vals(&r.hi).bool(r.lo_incl).bool(r.hi_incl);
        }
    }

    /// Roll back every open stream (best effort, nothing to wait for).
    pub fn rollback(self) {
        for (_, s) in self.streams {
            tokio::spawn(async move {
                let _ = s.client.rollback(s.id).await;
            });
        }
    }

    pub async fn commit(mut self, cluster: &Arc<Cluster>) -> SqlResult<()> {
        if let Some(e) = self.doomed.take() {
            self.rollback();
            return Err(e);
        }
        let parts = self.participants();
        if parts.is_empty() {
            return Ok(());
        }
        let res = if self.writes.is_empty() {
            self.commit_read_only(cluster, &parts).await
        } else if parts.len() == 1 {
            self.commit_local(cluster, *parts.iter().next().unwrap()).await
        } else {
            self.commit_2pc(cluster, &parts).await
        };
        if let Err(e) = &res {
            if e.code == 1213 {
                cluster.stats.conflicts.fetch_add(1, Ordering::Relaxed);
            }
        }
        res
    }

    async fn commit_read_only(&mut self, cluster: &Arc<Cluster>, parts: &BTreeSet<usize>) -> SqlResult<()> {
        let mut futs = Vec::new();
        for &p in parts {
            let s = self.streams.get(&p).expect("read-only participants have streams");
            let mut m = Mp::new();
            m.arr(3).str(&self.id);
            self.encode_reads(p, &mut m);
            let (client, id) = (s.client.clone(), s.id);
            futs.push(async move {
                let r = client.call(id, "tarn_validate", &m.0).await.map_err(from_iproto)?;
                let d = r.data().ok_or_else(|| crate::mpw::proto("no data"))?;
                lua_status(&d).map(|_| ())
            });
        }
        let results = futures::future::join_all(futs).await;
        cluster.stats.commits_ro.fetch_add(1, Ordering::Relaxed);
        let mut err = None;
        for r in results {
            if let Err(e) = r {
                err.get_or_insert(e);
            }
        }
        match err {
            None => Ok(()),
            Some(e) => {
                let t = std::mem::replace(&mut self.streams, BTreeMap::new());
                for (_, s) in t {
                    let _ = s.client.rollback(s.id).await;
                }
                Err(e)
            }
        }
    }

    async fn commit_local(&mut self, cluster: &Arc<Cluster>, shard: usize) -> SqlResult<()> {
        let (client, id) = self.stream(cluster, shard).await?;
        let mut m = Mp::new();
        m.arr(4).str(&self.id);
        self.encode_writes(cluster, shard, &mut m);
        self.encode_reads(shard, &mut m);
        let r = client.call(id, "tarn_commit_local", &m.0).await.map_err(from_iproto);
        cluster.stats.commits_local.fetch_add(1, Ordering::Relaxed);
        let r = r.and_then(|r| r.data().ok_or_else(|| crate::mpw::proto("no data")));
        match r {
            Ok(d) => lua_status(&d).map(|_| ()),
            Err(e) => {
                let _ = client.rollback(id).await;
                Err(e)
            }
        }
    }

    async fn commit_2pc(&mut self, cluster: &Arc<Cluster>, parts: &BTreeSet<usize>) -> SqlResult<()> {
        cluster.stats.commits_2pc.fetch_add(1, Ordering::Relaxed);
        // Decision shard: the lowest participant that writes.
        let dshard = self.writes.values().map(|w| w.shard).min().unwrap();
        let did = cluster.shards[dshard].id;
        let part_ids: Vec<u32> = parts.iter().map(|&p| cluster.shards[p].id).collect();
        for &p in parts {
            self.stream(cluster, p).await?;
        }
        // PREPARE everywhere in parallel.
        let mut futs = Vec::new();
        for &p in parts {
            let s = &self.streams[&p];
            let mut m = Mp::new();
            m.arr(7).str(&self.id).uint(did as u64);
            m.arr(part_ids.len());
            for &x in &part_ids {
                m.uint(x as u64);
            }
            self.encode_writes(cluster, p, &mut m);
            self.encode_reads(p, &mut m);
            m.f64(self.ts);
            let (client, id) = (s.client.clone(), s.id);
            futs.push(async move {
                match client.call(id, "tarn_prepare", &m.0).await {
                    Ok(r) => match r.data() {
                        Some(d) => match lua_status(&d) {
                            Ok(_) => (Vote::Yes, None),
                            Err(e) => (Vote::No, Some(e)),
                        },
                        None => (Vote::Unknown, Some(crate::mpw::proto("no data"))),
                    },
                    Err(e) => (Vote::Unknown, Some(from_iproto(e))),
                }
            });
        }
        let results = futures::future::join_all(futs).await;
        let votes: Vec<Vote> = results.iter().map(|(v, _)| *v).collect();
        let first_err = results.into_iter().find_map(|(_, e)| e);
        self.streams.clear(); // every stream's transaction ended inside tarn_prepare
        let parts_v: Vec<usize> = parts.iter().copied().collect();
        match coord::decide(&votes) {
            Decision::Commit => {
                if cluster.cfg.ack_on_prepare {
                    let c = cluster.clone();
                    let id = self.id.clone();
                    tokio::spawn(async move {
                        let _ = apply_all(&c, &id, &parts_v).await;
                    });
                    Ok(())
                } else {
                    apply_all(cluster, &self.id, &parts_v).await
                }
            }
            Decision::Abort => {
                abort_all(cluster, &self.id, did, &part_ids, &parts_v).await;
                Err(first_err.unwrap_or_else(|| SqlError::conflict("prepare refused")))
            }
            Decision::Unknown => {
                // Some PREPARE replies were lost: resolve exactly like the
                // recovery agents do (TarnCommit!RecoverDecide), via the
                // decision shard's record and the participants' states.
                match resolve(cluster, &self.id, dshard, &parts_v).await {
                    Ok(true) => Ok(()),
                    Ok(false) => Err(first_err.unwrap_or_else(|| SqlError::conflict("transaction aborted"))),
                    Err(e) => Err(SqlError::new(
                        2013,
                        "HY000",
                        format!("transaction {} outcome unknown ({}); it will be resolved by recovery", self.id, e),
                    )),
                }
            }
        }
    }
}

async fn apply_all(cluster: &Arc<Cluster>, txid: &str, parts: &[usize]) -> SqlResult<()> {
    let mut m = Mp::new();
    m.arr(1).str(txid);
    let args = m.0;
    let futs = parts.iter().map(|&p| cluster.call(p, "tarn_apply", &args));
    for r in futures::future::join_all(futs).await {
        // A failed APPLY is retried by recovery; the transaction is committed.
        if let Err(e) = r.and_then(|d| lua_status(&d).map(|_| ())) {
            tracing::warn!("apply of {} failed (recovery will retry): {}", txid, e);
        }
    }
    Ok(())
}

async fn abort_all(cluster: &Arc<Cluster>, txid: &str, did: u32, part_ids: &[u32], parts: &[usize]) {
    let mut m = Mp::new();
    m.arr(3).str(txid).uint(did as u64).arr(part_ids.len());
    for &x in part_ids {
        m.uint(x as u64);
    }
    let args = m.0;
    let futs = parts.iter().map(|&p| cluster.call(p, "tarn_abort", &args));
    let _ = futures::future::join_all(futs).await;
}

async fn call_str(cluster: &Arc<Cluster>, shard: usize, f: &str, args: &[u8]) -> SqlResult<String> {
    let d = cluster.call(shard, f, args).await?;
    let mut rd = Rd::new(&d);
    rd.array()?;
    rd.string()
}

/// Determine the outcome of a transaction whose votes are not all known.
/// Ok(true) = committed (and applied), Ok(false) = aborted.
pub async fn resolve(cluster: &Arc<Cluster>, txid: &str, dshard: usize, parts: &[usize]) -> SqlResult<bool> {
    let part_ids: Vec<u32> = parts.iter().map(|&p| cluster.shards[p].id).collect();
    let did = cluster.shards[dshard].id;
    let mut idm = Mp::new();
    idm.arr(1).str(txid);
    for _ in 0..4 {
        let status = call_str(cluster, dshard, "tarn_txn_status", &idm.0).await?;
        let mut states = Vec::new();
        for &p in parts {
            states.push(call_str(cluster, p, "tarn_part_status", &idm.0).await?);
        }
        let states: Vec<coord::PartState> = states.iter().map(|s| coord::PartState::parse(s)).collect();
        match coord::resolve_step(coord::RecStatus::parse(&status), &states) {
            coord::Step::Committed => {
                apply_all(cluster, txid, parts).await?;
                return Ok(true);
            }
            coord::Step::Aborted => {
                abort_all(cluster, txid, did, &part_ids, parts).await;
                return Ok(false);
            }
            coord::Step::DecideCommit => {
                let mut m = Mp::new();
                m.arr(3).str(txid).str("staging").str("committed");
                cluster.call(dshard, "tarn_txn_cas", &m.0).await?;
            }
            coord::Step::DecideAbort => {
                let mut m = Mp::new();
                m.arr(3).str(txid).str("staging").str("aborted");
                cluster.call(dshard, "tarn_txn_cas", &m.0).await?;
            }
            coord::Step::FenceRecord => {
                let mut m = Mp::new();
                m.arr(4).str(txid).str("none").str("aborted").arr(part_ids.len());
                for &x in &part_ids {
                    m.uint(x as u64);
                }
                cluster.call(dshard, "tarn_txn_cas", &m.0).await?;
            }
            coord::Step::FenceParticipants(missing) => {
                for i in missing {
                    let mut m = Mp::new();
                    m.arr(3).str(txid).uint(did as u64).arr(part_ids.len());
                    for &x in &part_ids {
                        m.uint(x as u64);
                    }
                    cluster.call(parts[i], "tarn_fence", &m.0).await?;
                }
            }
        }
    }
    Err(SqlError::internal("could not resolve transaction"))
}

/// Read rows of `t` from one shard, optionally inside a stream.
/// `key`/`iter`/`end` describe a bounded index scan (see tarn_select).
#[allow(clippy::too_many_arguments)]
pub async fn select_rows(
    client: &Client,
    stream: u64,
    t: &TableDef,
    index: u32,
    key: &[Value],
    iter: Iter,
    limit: Option<u32>,
    end: Option<(&[Value], bool)>,
    lock_check: bool,
) -> SqlResult<Vec<Row>> {
    let ncols = t.columns.len();
    // Unbounded point/prefix lookups inside a stream use plain IPROTO SELECT.
    if end.is_none() && !lock_check {
        let mut k = Mp::new();
        k.vals(key);
        let r = client
            .select(stream, t.space_id, index, iter, 0, limit.unwrap_or(u32::MAX), &k.0)
            .await
            .map_err(from_iproto)?;
        let mut out = Vec::new();
        for tup in r.tuples().map_err(crate::mpw::proto)? {
            out.push(Rd::new(&tup).row(ncols)?);
        }
        return Ok(out);
    }
    let mut m = Mp::new();
    m.arr(8).uint(t.space_id as u64).uint(index as u64).vals(key).uint(iter as u64);
    match limit {
        Some(l) => m.uint(l as u64),
        None => m.nil(),
    };
    match end {
        Some((e, incl)) => {
            m.vals(e).bool(incl);
        }
        None => {
            m.nil().bool(true);
        }
    }
    m.bool(lock_check);
    let r = client.call(stream, "tarn_select", &m.0).await.map_err(from_iproto)?;
    let d = r.data().ok_or_else(|| crate::mpw::proto("no data"))?;
    let (_, mut rd) = lua_status(&d)?;
    let n = rd.array()?;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push(rd.row(ncols)?);
    }
    Ok(out)
}
