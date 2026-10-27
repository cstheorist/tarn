//! Transactions: the coordinator side of tla/TarnCommit.tla.
//!
//! Execution phase: reads go to each shard inside an IPROTO stream (one per
//! participant, opened lazily with BEGIN), where Tarantool's MVCC tracks
//! them; the read set is also recorded here because commit-time lock checks
//! need it. Writes are buffered in `writes` and overlaid on reads.
//!
//! Commit (`commit`):
//! * one shard, writes: `tarn_commit_local` (TarnCommit!CommitLocal);
//! * no writes: `tarn_validate` on every participant (TarnCommit!ValidateRO,
//!   SnapshotRead);
//! * several shards, writes: parallel-commit 2PC, `tarn_prepare` on all
//!   participants (the decision shard also writes the STAGING record), then
//!   `tarn_apply` (TarnCommit!Prepare, ApplyAt). The client is acknowledged
//!   after APPLY unless `ack_on_prepare` is set.
//!
//! READ COMMITTED transactions (`Mode::ReadCommitted`, issue #17) read the
//! latest committed state outside any stream and record no read set. Their
//! writes and locking reads take row locks on the owning shard (`lock`,
//! `tarn_lock`), which queue behind other holders and read the latest
//! version under the lock. They commit with `tarn_commit_rc` (one shard) or
//! the same 2PC (several), passing the generation of their locks on each
//! shard (see [`ShardLocks`]) so that a write is never applied after its
//! lock was lost (TarnCommit: CommitRC, Prepare for t \in RC).
//!
//! The pure decision logic (what to do with a set of votes, when it is safe
//! to apply/abort) is `tarn_verified::coord`, proven in Verus to only decide
//! COMMIT on unanimous YES and never to decide both ways.

use crate::catalog::TableDef;
use crate::cluster::Cluster;
use crate::mpw::{ambiguous, from_iproto, from_iproto_read, lua_status, Mp, Rd};
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tarn_common::{SqlError, SqlResult, Value};
use tarn_iproto::{Client, Iter};
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
    /// Primary key as the primary index holds it (`TableDef::pk_key`: a
    /// case-insensitive part is its weight string).
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
    /// The bucket the read was routed by (None: a read of every shard).
    pub bucket: Option<u32>,
}

/// What a shard checks before it serves a read (lua/tarn.lua "Buckets").
#[derive(Clone, Debug, PartialEq)]
pub enum Route {
    /// Nothing: a read of every shard in a transaction's stream (its
    /// validation covers it), or for a storage node that predates buckets.
    None,
    /// The buckets the read was routed by: the shard must own them.
    Buckets(Vec<u32>),
    /// A read of every shard outside a transaction: the shard's epoch in
    /// this server's map, which it refuses if its buckets changed since,
    /// and the membership version the read was routed with (lua/tarn.lua
    /// "Membership"), which it refuses if a shard joined since.
    Epoch(u64, u64),
}

impl Route {
    /// A read in a transaction's stream: by its bucket, if it has one.
    pub fn in_stream(bucket: Option<u32>) -> Route {
        bucket.map_or(Route::None, |b| Route::Buckets(vec![b]))
    }

    pub fn encode(&self, m: &mut Mp) {
        match self {
            Route::None => {
                m.nil();
            }
            Route::Buckets(b) if b.len() == 1 => {
                m.uint(b[0] as u64);
            }
            Route::Buckets(b) => {
                m.arr(b.len());
                for &x in b {
                    m.uint(x as u64);
                }
            }
            Route::Epoch(e, v) => {
                m.map(2).str("e").uint(*e).str("m").uint(*v);
            }
        }
    }
}

pub struct Stream {
    pub client: Client,
    pub id: u64,
    /// A request that began the stream's transaction has succeeded.
    pub begun: bool,
}

/// A stream handle for one request: `begin` is true for the first request
/// of a fresh stream, which must start the transaction itself (tarn_select
/// with its begin flag) -- this saves the separate IPROTO BEGIN round trip.
pub struct StreamRef {
    pub client: Client,
    pub id: u64,
    pub begin: bool,
}

/// How a transaction isolates itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// Strictly serializable: optimistic, reads validated at commit.
    Serializable,
    /// READ COMMITTED as InnoDB implements it (issue #17).
    ReadCommitted,
}

/// A row lock to take: a row by primary key (`index` 0) or a unique-key
/// value, as the index holds it (`TableDef::index_key`).
pub struct LockKey {
    pub table: Arc<TableDef>,
    pub index: u32,
    pub key: Vec<Value>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LockOpts {
    pub exclusive: bool,
    pub nowait: bool,
    pub skip: bool,
    /// Return the latest committed row under each lock.
    pub read: bool,
}

/// Row locks a transaction holds on one shard. The shard ties row locks to
/// the session (connection) that took them -- they are released when it
/// closes -- so every later lock request and the commit of the transaction
/// on that shard go over the same connection, and `gen` (the lock table's
/// generation and that session) lets the commit check that they survived.
pub struct ShardLocks {
    pub client: Client,
    pub gen: String,
}

/// One range of a locking read in one storage call (`tarn_lock_range`):
/// the first `n` rows of `table` whose primary key starts with `prefix` and
/// that it can lock, leaving out the primary keys `skip`.
#[derive(Clone)]
pub struct LockRange {
    pub table: Arc<TableDef>,
    pub prefix: Vec<Value>,
    pub n: usize,
    pub skip: Vec<Vec<Value>>,
}

/// Outcome of one requested lock.
#[derive(Clone, Debug, PartialEq)]
pub enum Locked {
    /// Granted; with `read`, the latest committed row (None: no such row).
    Row(Option<Row>),
    /// Granted, nothing read.
    Granted,
    /// Held by another transaction (SKIP LOCKED).
    Skipped,
}

pub struct Txn {
    pub id: String,
    pub ts: f64,
    pub explicit: bool,
    pub mode: Mode,
    /// Lock wait timeout (innodb_lock_wait_timeout), seconds.
    pub lock_timeout: f64,
    /// Shards where this transaction holds row locks.
    pub plocks: BTreeMap<usize, ShardLocks>,
    /// SAVEPOINTs: name and the write buffer when it was set.
    pub savepoints: Vec<(String, BTreeMap<(u32, Bytes), BufWrite>)>,
    pub streams: BTreeMap<usize, Stream>,
    pub writes: BTreeMap<(u32, Bytes), BufWrite>,
    /// Point reads by shard: (space, index, key, the bucket they were
    /// routed by).
    pub rpoints: BTreeMap<usize, Vec<(u32, u32, Vec<Value>, Option<u32>)>>,
    pub rranges: BTreeMap<usize, Vec<RangeRead>>,
    /// Definition version (`TableDef::ver`) each table was written with,
    /// by space id (see [`Txn::check_def`]).
    pub defs: BTreeMap<u32, u64>,
    /// Set after an error that leaves the transaction unusable (e.g. a
    /// conflict detected mid-transaction); COMMIT then fails.
    pub doomed: Option<SqlError>,
    /// The membership version (members.rs) of this server when the
    /// transaction began. Its reads of every shard covered at least that
    /// membership's shards (shards only join this server's list, before
    /// it takes their version): when it made such reads, its commit
    /// requests name the version, and a shard that knows a newer one refuses
    /// them (lua/tarn.lua "Membership").
    pub mver: u64,
}

pub fn definition_changed() -> SqlError {
    SqlError::new(1412, "HY000", "Table definition has changed, please retry transaction")
}

/// The generation in a failed `tarn_lock` reply (`[[status, code, message,
/// generation]]`): present when locks granted before the error stay held.
fn failed_lock_gen(d: &[u8]) -> Option<String> {
    let mut rd = Rd::new(d);
    rd.array().ok()?;
    if rd.array().ok()? < 4 {
        return None;
    }
    for _ in 0..3 {
        rd.skip().ok()?;
    }
    rd.string().ok()
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
        Txn::with_mode(cluster, explicit, Mode::Serializable, 50.0)
    }

    pub fn with_mode(cluster: &Cluster, explicit: bool, mode: Mode, lock_timeout: f64) -> Txn {
        Txn {
            id: cluster.new_txid(),
            ts: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs_f64())
                .unwrap_or(0.0),
            explicit,
            mode,
            lock_timeout,
            plocks: BTreeMap::new(),
            savepoints: Vec::new(),
            streams: BTreeMap::new(),
            writes: BTreeMap::new(),
            rpoints: BTreeMap::new(),
            rranges: BTreeMap::new(),
            defs: BTreeMap::new(),
            doomed: None,
            mver: cluster.member_ver(),
        }
    }

    /// Rows in the write buffer are in the column order of the definition
    /// they were made with, so a statement planned with another definition
    /// of the table (an ALTER TABLE committed since) must not see them: the
    /// transaction has to start over (1412). Tables it only read need no
    /// check: a statement decodes what it reads with its own definition, the
    /// shards refuse writes made with a replaced one (the fence), and the
    /// ALTER's DDL aborts the stream transactions that read the table there
    /// (memtx MVCC invalidates an altered space).
    pub fn check_def(&self, t: &TableDef) -> SqlResult<()> {
        match self.defs.get(&t.space_id) {
            Some(&v) if v != t.ver => Err(definition_changed()),
            _ => Ok(()),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.streams.is_empty() && self.writes.is_empty() && self.plocks.is_empty()
    }

    /// Take row locks on one shard, in order (`tarn_lock`). With `read`, each
    /// granted lock comes with the latest committed row. Errors: 1205 (lock
    /// wait timeout), 1213 (deadlock, or the transaction's locks were lost),
    /// 3572 (NOWAIT). Locks granted before an error stay held.
    pub async fn lock(
        &mut self,
        cluster: &Arc<Cluster>,
        shard: usize,
        keys: &[LockKey],
        o: LockOpts,
    ) -> SqlResult<Vec<Locked>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let client = self.conn(cluster, shard).await?;
        // The other shards where this transaction holds row locks, for the
        // cross-shard deadlock detectors: this server's (registered below)
        // and the others' (sent with the request, reported by
        // tarn_lock_waits). A transaction makes one lock request at a time
        // and takes no other lock while it waits, so the list stays exact.
        let held: Vec<usize> = self.plocks.keys().copied().filter(|&s| s != shard).collect();
        let mut m = Mp::new();
        m.arr(4).str(&self.id).arr(keys.len());
        for k in keys {
            m.arr(4).uint(k.table.space_id as u64).uint(k.index as u64).vals(&k.key);
            opt_uint(&mut m, cluster.bucket_of_index_key(&k.table, k.index, &k.key));
        }
        self.encode_lock_opts(&mut m, cluster, o, &held);
        // NOWAIT and SKIP LOCKED requests never queue.
        let _waiting = (!o.nowait && !o.skip).then(|| cluster.lock_request(&self.id, shard, held));
        // Lock requests change nothing that a resend could duplicate: a lost
        // reply just fails the statement (the locks are released with the
        // transaction).
        let r = client.call(0, "tarn_lock", &m.0).await.map_err(from_iproto_read)?;
        let d = r.data().ok_or_else(|| crate::mpw::proto("no data"))?;
        let rd = self.lock_status(shard, &client, &d)?;
        let mut rd = Rd::shared_at(&d, rd);
        let gen = rd.string()?;
        self.note_locks(shard, client, gen)?;
        let n = rd.array()?;
        let mut out = Vec::with_capacity(n);
        for k in keys.iter().take(n) {
            out.push(match rd.buf.first() {
                Some(0xc0) => {
                    rd.skip()?;
                    Locked::Row(None)
                }
                Some(0xc2) => {
                    rd.skip()?;
                    Locked::Skipped
                }
                Some(0xc3) => {
                    rd.skip()?;
                    Locked::Granted
                }
                _ => Locked::Row(Some(rd.row_of(&k.table)?)),
            });
        }
        Ok(out)
    }

    /// Mode and options of a `tarn_lock` / `tarn_lock_range` request;
    /// `held` are the other shards where the transaction holds row locks.
    fn encode_lock_opts(&self, m: &mut Mp, cluster: &Cluster, o: LockOpts, held: &[usize]) {
        m.str(if o.exclusive { "X" } else { "S" });
        m.map(5)
            .str("timeout")
            .f64(self.lock_timeout)
            .str("nowait")
            .bool(o.nowait)
            .str("skip")
            .bool(o.skip)
            .str("read")
            .bool(o.read)
            .str("held")
            .arr(held.len());
        for &s in held {
            m.uint(cluster.shards[s].id as u64);
        }
    }

    /// The status of a lock request's reply. Locks granted before an error
    /// stay held until the transaction ends; the reply then names their
    /// generation, so that they are released with the others even if this
    /// was the first request on the shard.
    fn lock_status<'d>(&mut self, shard: usize, client: &Client, d: &'d [u8]) -> SqlResult<Rd<'d>> {
        match lua_status(d) {
            Ok((_, rd)) => Ok(rd),
            Err(e) => {
                if let Some(gen) = failed_lock_gen(d) {
                    self.plocks.entry(shard).or_insert(ShardLocks {
                        client: client.clone(),
                        gen,
                    });
                }
                Err(e)
            }
        }
    }

    /// After a lock request on `shard` succeeded with lock generation `gen`:
    /// remember the connection, or fail if earlier locks there were lost.
    fn note_locks(&mut self, shard: usize, client: Client, gen: String) -> SqlResult<()> {
        match self.plocks.get(&shard) {
            Some(l) if l.gen != gen => Err(SqlError::conflict(
                "the transaction lost its row locks (shard leader change or lost connection)",
            )),
            Some(_) => Ok(()),
            None => {
                self.plocks.insert(shard, ShardLocks { client, gen });
                Ok(())
            }
        }
    }

    /// Locking reads of primary-key ranges (`tarn_lock_range`): each request
    /// is (shard, options, ranges); for each range come back the rows locked
    /// in it, read under their locks, in one storage call per request. When
    /// no request can wait (SKIP LOCKED, NOWAIT) they all run at once;
    /// otherwise one after another, since the cross-shard deadlock detector
    /// follows a transaction waiting on one shard at a time. Errors as
    /// [`Txn::lock`]; locks taken before an error stay held.
    pub async fn lock_ranges(
        &mut self,
        cluster: &Arc<Cluster>,
        reqs: &[(usize, LockOpts, Vec<LockRange>)],
    ) -> SqlResult<Vec<Vec<Vec<Row>>>> {
        let encode = |txn: &Txn, (shard, o, ranges): &(usize, LockOpts, Vec<LockRange>)| {
            let held: Vec<usize> = txn.plocks.keys().copied().filter(|s| s != shard).collect();
            let mut m = Mp::new();
            m.arr(4).str(&txn.id).arr(ranges.len());
            for r in ranges {
                m.arr(5).uint(r.table.space_id as u64).vals(&r.prefix).uint(r.n as u64);
                m.arr(r.skip.len());
                for k in &r.skip {
                    m.vals(k);
                }
                opt_uint(&mut m, cluster.bucket_of_index_key(&r.table, 0, &r.prefix));
            }
            txn.encode_lock_opts(&mut m, cluster, *o, &held);
            (m, held)
        };
        let mut out = Vec::with_capacity(reqs.len());
        if reqs.iter().any(|(_, o, _)| !o.nowait && !o.skip) {
            for req in reqs {
                let client = self.conn(cluster, req.0).await?;
                let (m, held) = encode(self, req);
                let r = {
                    let _waiting = cluster.lock_request(&self.id, req.0, held);
                    client.call(0, "tarn_lock_range", &m.0).await
                };
                out.push(self.decode_ranges(req.0, &client, r, &req.2)?);
            }
            return Ok(out);
        }
        // One connection per shard: the locks of a transaction on a shard
        // belong to one session.
        let mut clients: BTreeMap<usize, Client> = BTreeMap::new();
        let mut calls = Vec::with_capacity(reqs.len());
        for req in reqs {
            let client = match clients.get(&req.0) {
                Some(c) => c.clone(),
                None => {
                    let c = self.conn(cluster, req.0).await?;
                    clients.insert(req.0, c.clone());
                    c
                }
            };
            calls.push((client, encode(self, req).0));
        }
        let replies =
            futures::future::join_all(calls.iter().map(|(client, m)| client.call(0, "tarn_lock_range", &m.0))).await;
        let mut first_err = None;
        for ((req, (client, _)), r) in reqs.iter().zip(&calls).zip(replies) {
            match self.decode_ranges(req.0, client, r, &req.2) {
                Ok(v) => out.push(v),
                Err(e) => {
                    out.push(Vec::new());
                    // An error that ends the transaction (1213) wins over
                    // one that only fails the statement.
                    if first_err
                        .as_ref()
                        .is_none_or(|f: &SqlError| f.code != 1213 && e.code == 1213)
                    {
                        first_err = Some(e);
                    }
                }
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(out),
        }
    }

    fn decode_ranges(
        &mut self,
        shard: usize,
        client: &Client,
        r: Result<tarn_iproto::Response, tarn_iproto::Error>,
        ranges: &[LockRange],
    ) -> SqlResult<Vec<Vec<Row>>> {
        let r = r.map_err(from_iproto_read)?;
        let d = r.data().ok_or_else(|| crate::mpw::proto("no data"))?;
        let rd = self.lock_status(shard, client, &d)?;
        let mut rd = Rd::shared_at(&d, rd);
        let gen = rd.string()?;
        self.note_locks(shard, client.clone(), gen)?;
        let n = rd.array()?;
        let mut out = Vec::with_capacity(n);
        for rg in ranges.iter().take(n) {
            let nrows = rd.array()?;
            let mut rows = Vec::with_capacity(nrows);
            for _ in 0..nrows {
                rows.push(rd.row_of(&rg.table)?);
            }
            out.push(rows);
        }
        Ok(out)
    }

    /// Release some row locks early (rows that turned out not to match).
    pub async fn unlock(&self, cluster: &Arc<Cluster>, shard: usize, keys: &[LockKey]) {
        if keys.is_empty() {
            return;
        }
        let mut m = Mp::new();
        m.arr(2).str(&self.id).arr(keys.len());
        for k in keys {
            m.arr(3).uint(k.table.space_id as u64).uint(k.index as u64).vals(&k.key);
        }
        let _ = cluster.call(shard, "tarn_unlock", &m.0).await;
    }

    /// Release every row lock of this transaction (best effort).
    async fn release_plocks(&mut self, cluster: &Arc<Cluster>) {
        let shards: Vec<usize> = std::mem::take(&mut self.plocks).into_keys().collect();
        let mut m = Mp::new();
        m.arr(1).str(&self.id);
        let futs = shards.iter().map(|&s| cluster.call(s, "tarn_unlock", &m.0));
        let _ = futures::future::join_all(futs).await;
    }

    /// Generation of this transaction's row locks on `shard` (nil if none).
    /// For each table this transaction writes on `shard`: its space id and
    /// the oldest definition version a write was encoded with (the shard
    /// refuses writes made with a replaced definition, 1412).
    fn encode_fence(&self, shard: usize, m: &mut Mp) {
        let mut f: BTreeMap<u32, u64> = BTreeMap::new();
        for w in self.writes.values().filter(|w| w.shard == shard) {
            // A global index entry is fenced with its table's definition: a
            // TRUNCATE, DROP or ALTER of the table refuses it too.
            let (sid, ver) = w.table.base.unwrap_or((w.table.space_id, w.table.ver));
            let v = f.entry(sid).or_insert(ver);
            *v = (*v).min(ver);
        }
        m.arr(f.len() * 2);
        for (sid, ver) in f {
            m.uint(sid as u64).uint(ver);
        }
    }

    fn encode_gen(&self, shard: usize, m: &mut Mp) {
        match self.plocks.get(&shard) {
            Some(l) => m.str(&l.gen),
            None => m.nil(),
        };
    }

    /// The connection for this transaction's requests to `shard`: the one
    /// its stream or row locks there use (see [`ShardLocks`]), else any.
    async fn conn(&self, cluster: &Cluster, shard: usize) -> SqlResult<Client> {
        if let Some(s) = self.streams.get(&shard) {
            return Ok(s.client.clone());
        }
        if let Some(l) = self.plocks.get(&shard) {
            return Ok(l.client.clone());
        }
        cluster.shards[shard].client(cluster).await
    }

    /// Stream on `shard`. A fresh stream has no transaction yet: the first
    /// request sent on it must begin one (see [`StreamRef::begin`]); requests
    /// on a stream are processed in order, so later ones run inside it.
    pub async fn stream(&mut self, cluster: &Cluster, shard: usize) -> SqlResult<StreamRef> {
        if let Some(s) = self.streams.get(&shard) {
            // Until a beginning request has succeeded, keep asking requests
            // to begin (idempotent: tarn_select begins only outside a txn).
            return Ok(StreamRef {
                client: s.client.clone(),
                id: s.id,
                begin: !s.begun,
            });
        }
        let client = self.conn(cluster, shard).await?;
        let id = client.next_stream_id();
        self.streams.insert(
            shard,
            Stream {
                client: client.clone(),
                id,
                begun: false,
            },
        );
        Ok(StreamRef {
            client,
            id,
            begin: true,
        })
    }

    /// Record that a request with the begin flag succeeded on `shard`.
    pub fn mark_begun(&mut self, shard: usize) {
        if let Some(s) = self.streams.get_mut(&shard) {
            s.begun = true;
        }
    }

    pub fn note_point(&mut self, shard: usize, space: u32, index: u32, key: Vec<Value>, bucket: Option<u32>) {
        self.rpoints.entry(shard).or_default().push((space, index, key, bucket));
    }

    pub fn note_range(&mut self, shard: usize, r: RangeRead) {
        self.rranges.entry(shard).or_default().push(r);
    }

    pub fn buffer(&mut self, w: BufWrite) -> SqlResult<()> {
        self.check_def(&w.table)?;
        self.defs.insert(w.table.space_id, w.table.ver);
        let k = (w.table.space_id, key_bytes(&w.pk));
        if w.table.base.is_some() {
            // A global index entry (catalog: unique keys without the shard
            // key). Its key may already be taken by another row this
            // transaction wrote; and an entry it inserted and then deleted
            // was never there, so a later insert must still find the key
            // free (it may not turn into a replace).
            match (self.writes.get(&k).map(|p| p.op), w.op) {
                (Some(WOp::Insert | WOp::Replace), WOp::Insert) => {
                    let shown: Vec<String> = w.pk.iter().map(|v| v.to_string()).collect();
                    return Err(SqlError::dup_key(&shown.join("-"), &w.table.indexes[0].name));
                }
                (Some(WOp::Insert), WOp::Delete) => {
                    self.writes.remove(&k);
                    return Ok(());
                }
                _ => {}
            }
        }
        // An insert after a buffered delete of the same key is a replace.
        let w = match (self.writes.get(&k), w.op) {
            (Some(prev), WOp::Insert) if prev.op == WOp::Delete => BufWrite { op: WOp::Replace, ..w },
            (Some(prev), WOp::Replace) if prev.op == WOp::Insert => BufWrite { op: WOp::Insert, ..w },
            _ => w,
        };
        self.writes.insert(k, w);
        Ok(())
    }

    /// The buffered version of a row: Some(Some(row)) if written, Some(None)
    /// if deleted, None if untouched by this transaction.
    pub fn buffered(&self, space: u32, pk: &[Value]) -> Option<Option<&Row>> {
        self.writes.get(&(space, key_bytes(pk))).map(|w| w.row.as_ref())
    }

    /// The live rows this transaction wrote (inserted or changed) in a table.
    pub fn own_rows(&self, space: u32) -> impl Iterator<Item = &Row> {
        self.writes
            .range((space, Bytes::new())..)
            .take_while(move |((s, _), _)| *s == space)
            .filter_map(|(_, w)| w.row.as_ref())
    }

    /// Primary keys (`key_bytes`) of the rows of `t` this transaction wrote:
    /// their stored versions are not its to read.
    pub fn written_keys(&self, t: &TableDef) -> BTreeSet<Bytes> {
        self.writes
            .range((t.space_id, Bytes::new())..)
            .take_while(|((s, _), _)| *s == t.space_id)
            .map(|(_, w)| key_bytes(&w.pk))
            .collect()
    }

    /// The stored rows of `rows` (of `t`) this transaction has not written:
    /// [`Txn::overlay`] without the rows it adds.
    pub fn unwritten(&self, t: &TableDef, rows: Vec<Row>) -> Vec<Row> {
        let written = self.written_keys(t);
        if written.is_empty() {
            return rows;
        }
        rows.into_iter()
            .filter(|r| !written.contains(&key_bytes(&t.pk_key(r))))
            .collect()
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
        let written: BTreeSet<Bytes> = mine.iter().map(|w| key_bytes(&w.pk)).collect();
        let mut out: Vec<Row> = rows
            .into_iter()
            .filter(|r| !written.contains(&key_bytes(&t.pk_key(r))))
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
        // Shards holding row locks check at commit that the locks survived.
        p.extend(self.plocks.keys().copied());
        p
    }

    /// The writes to `shard`, and the bucket of each (`wb`, in their order).
    fn encode_writes(&self, cluster: &Cluster, shard: usize, m: &mut Mp) -> Vec<u32> {
        // Deletes first: a row inserted in place of a deleted one (REPLACE
        // over a unique key, delete-then-insert) may reuse its unique values.
        let mut ws: Vec<&BufWrite> = self.writes.values().filter(|w| w.shard == shard).collect();
        ws.sort_by_key(|w| w.op != WOp::Delete);
        m.arr(ws.len());
        let mut wb = Vec::with_capacity(ws.len());
        for w in ws {
            let bucket = cluster.bucket_of(&w.table.shard_key_of_pk(&w.pk));
            wb.push(bucket);
            m.arr(3);
            m.str(match w.op {
                WOp::Insert => "insert",
                WOp::Replace => "replace",
                WOp::Delete => "delete",
            });
            m.uint(w.table.space_id as u64);
            match &w.row {
                Some(r) if w.op != WOp::Delete => {
                    w.table.encode_tuple(r, bucket as u64, m);
                }
                _ => {
                    m.vals(&w.pk);
                }
            }
        }
        wb
    }

    /// Whether the transaction read every shard (a read routed by no
    /// bucket).
    fn scanned(&self) -> bool {
        self.rpoints.values().flatten().any(|p| p.3.is_none())
            || self.rranges.values().flatten().any(|r| r.bucket.is_none())
    }

    /// The membership version its commit requests name (`mver` of the
    /// commit procedures): nil unless it read every shard.
    fn encode_mver(&self, m: &mut Mp) {
        if self.scanned() {
            m.uint(self.mver);
        } else {
            m.nil();
        }
    }

    fn encode_reads(&self, shard: usize, m: &mut Mp) {
        let pts = self.rpoints.get(&shard).map(|v| v.as_slice()).unwrap_or(&[]);
        m.arr(pts.len());
        for (s, i, k, b) in pts {
            m.arr(4).uint(*s as u64).uint(*i as u64).vals(k);
            opt_uint(m, *b);
        }
        let rs = self.rranges.get(&shard).map(|v| v.as_slice()).unwrap_or(&[]);
        m.arr(rs.len());
        for r in rs {
            m.arr(7).uint(r.space as u64).uint(r.index as u64);
            m.opt_vals(&r.lo).opt_vals(&r.hi).bool(r.lo_incl).bool(r.hi_incl);
            opt_uint(m, r.bucket);
        }
    }

    /// Roll back every open stream and release the row locks (best effort,
    /// nothing to wait for).
    pub fn rollback(self, cluster: &Arc<Cluster>) {
        for (_, s) in self.streams {
            tokio::spawn(async move {
                let _ = s.client.rollback(s.id).await;
            });
        }
        if !self.plocks.is_empty() {
            let c = cluster.clone();
            let id = self.id.clone();
            let shards: Vec<usize> = self.plocks.into_keys().collect();
            tokio::spawn(async move {
                let mut m = Mp::new();
                m.arr(1).str(&id);
                let futs = shards.iter().map(|&s| c.call(s, "tarn_unlock", &m.0));
                let _ = futures::future::join_all(futs).await;
            });
        }
    }

    pub async fn commit(mut self, cluster: &Arc<Cluster>) -> SqlResult<()> {
        if let Some(e) = self.doomed.take() {
            self.rollback(cluster);
            return Err(e);
        }
        let parts = self.participants();
        if parts.is_empty() {
            return Ok(());
        }
        let res = if self.mode == Mode::ReadCommitted && self.writes.is_empty() {
            // Nothing to validate: just end the locks.
            self.release_plocks(cluster).await;
            Ok(())
        } else if self.writes.is_empty() {
            self.commit_read_only(cluster, &parts).await
        } else if parts.len() == 1 && self.mode == Mode::ReadCommitted {
            self.commit_rc(cluster, *parts.iter().next().unwrap()).await
        } else if parts.len() == 1 {
            self.commit_local(cluster, *parts.iter().next().unwrap()).await
        } else {
            self.commit_2pc(cluster, &parts).await
        };
        if res.is_ok() {
            // Every commit path releases the row locks on the shards
            // (tarn_commit_local/_rc, tarn_validate, APPLY).
            self.plocks.clear();
        } else if !self.plocks.is_empty() {
            self.release_plocks(cluster).await;
        }
        if let Err(e) = &res {
            if e.code == 1213 {
                cluster.stats.conflicts.fetch_add(1, Ordering::Relaxed);
            }
        }
        res
    }

    async fn commit_read_only(&mut self, cluster: &Arc<Cluster>, parts: &BTreeSet<usize>) -> SqlResult<()> {
        let mut futs = Vec::new();
        let cross = parts.len() > 1;
        for &p in parts {
            // A shard where the transaction only holds row locks has no stream.
            let (client, id) = match self.streams.get(&p) {
                Some(s) => (s.client.clone(), s.id),
                None => (self.conn(cluster, p).await?, 0),
            };
            let mut m = Mp::new();
            m.arr(6).str(&self.id);
            self.encode_reads(p, &mut m);
            m.bool(cross);
            self.encode_gen(p, &mut m);
            self.encode_mver(&mut m);
            futs.push(async move {
                // Read-only: a lost connection or a demoted leader just
                // means "retry" (nothing was written).
                let r = client.call(id, "tarn_validate", &m.0).await.map_err(from_iproto_read)?;
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
                let t = std::mem::take(&mut self.streams);
                for (_, s) in t {
                    let _ = s.client.rollback(s.id).await;
                }
                Err(e)
            }
        }
    }

    async fn commit_local(&mut self, cluster: &Arc<Cluster>, shard: usize) -> SqlResult<()> {
        // Without reads on this shard there is no stream: tarn_commit_local
        // then runs as its own local transaction (no BEGIN round trip).
        let (client, id) = match self.streams.get(&shard) {
            Some(s) => (s.client.clone(), s.id),
            None => (self.conn(cluster, shard).await?, 0),
        };
        let mut m = Mp::new();
        m.arr(8).str(&self.id);
        let wb = self.encode_writes(cluster, shard, &mut m);
        self.encode_reads(shard, &mut m);
        self.encode_gen(shard, &mut m);
        self.encode_fence(shard, &mut m);
        encode_buckets(&mut m, &wb);
        self.encode_mver(&mut m);
        let shard_id = cluster.shards[shard].id;
        let r = client.call(id, "tarn_commit_local", &m.0).await.map_err(|e| match e {
            // The commit may have happened: report the outcome as unknown.
            e @ (tarn_iproto::Error::Closed | tarn_iproto::Error::Io(_)) => ambiguous(shard_id, &e),
            e => from_iproto(e),
        });
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

    /// READ COMMITTED, one shard: `tarn_commit_rc` checks that the row locks
    /// survived, applies the writes and releases the locks.
    async fn commit_rc(&mut self, cluster: &Arc<Cluster>, shard: usize) -> SqlResult<()> {
        let client = self.conn(cluster, shard).await?;
        if client.is_closed() {
            // The shard released this transaction's row locks with the
            // connection; nothing was sent, so nothing was written.
            return Err(SqlError::conflict(
                "the transaction lost its row locks (connection to the shard lost)",
            ));
        }
        let mut m = Mp::new();
        m.arr(5).str(&self.id);
        let wb = self.encode_writes(cluster, shard, &mut m);
        self.encode_gen(shard, &mut m);
        self.encode_fence(shard, &mut m);
        encode_buckets(&mut m, &wb);
        let shard_id = cluster.shards[shard].id;
        let r = client.call(0, "tarn_commit_rc", &m.0).await.map_err(|e| match e {
            e @ (tarn_iproto::Error::Closed | tarn_iproto::Error::Io(_)) => ambiguous(shard_id, &e),
            e => from_iproto(e),
        });
        cluster.stats.commits_local.fetch_add(1, Ordering::Relaxed);
        let d = r?.data().ok_or_else(|| crate::mpw::proto("no data"))?;
        lua_status(&d).map(|_| ())?;
        self.plocks.remove(&shard); // released by the commit
        Ok(())
    }

    async fn commit_2pc(&mut self, cluster: &Arc<Cluster>, parts: &BTreeSet<usize>) -> SqlResult<()> {
        cluster.stats.commits_2pc.fetch_add(1, Ordering::Relaxed);
        // A READ COMMITTED transaction holds no stream transactions and may
        // run (and wait for locks) for long: its PREPAREs carry the time its
        // commit starts. The expiry only needs a timestamp fixed before the
        // first PREPARE (TarnCommit: Expire may happen at any moment).
        if self.mode == Mode::ReadCommitted {
            self.ts = now_secs();
        }
        // Participants refuse PREPAREs older than max_txn_age; do not start
        // a commit that they would refuse.
        if now_secs() - self.ts > cluster.cfg.max_txn_age {
            let t = std::mem::take(&mut self.streams);
            for (_, s) in t {
                let _ = s.client.rollback(s.id).await;
            }
            return Err(SqlError::conflict(
                "transaction too old (max_txn_age exceeded); retry it",
            ));
        }
        // Decision shard: the lowest participant that writes and is not
        // leaving the cluster (participants refuse one that is: lua/tarn.lua
        // BK.prep).
        let writers: BTreeSet<usize> = self.writes.values().map(|w| w.shard).collect();
        let dshard = writers
            .iter()
            .copied()
            .find(|&p| cluster.shard_active(p))
            .unwrap_or_else(|| *writers.iter().next().unwrap());
        let did = cluster.shards[dshard].id;
        let part_ids: Vec<u32> = parts.iter().map(|&p| cluster.shards[p].id).collect();
        // Write-only participants have no stream: their PREPARE runs as its
        // own local transaction.
        let mut targets = Vec::new();
        for &p in parts {
            targets.push(match self.streams.get(&p) {
                Some(s) => (s.client.clone(), s.id),
                None => (self.conn(cluster, p).await?, 0),
            });
        }
        // PREPARE everywhere in parallel.
        let mut futs = Vec::new();
        for (&p, (client, id)) in parts.iter().zip(targets) {
            let mut m = Mp::new();
            m.arr(11).str(&self.id).uint(did as u64);
            m.arr(part_ids.len());
            for &x in &part_ids {
                m.uint(x as u64);
            }
            let wb = self.encode_writes(cluster, p, &mut m);
            self.encode_reads(p, &mut m);
            m.f64(self.ts);
            self.encode_gen(p, &mut m);
            self.encode_fence(p, &mut m);
            encode_buckets(&mut m, &wb);
            self.encode_mver(&mut m);
            futs.push(async move {
                match client.call(id, "tarn_prepare", &m.0).await {
                    Ok(r) => match r.data() {
                        // 'expired' is neither YES nor NO (TarnCommit: an
                        // expired PREPARE has no effect and sends no vote).
                        Some(d) if crate::mpw::status_is(&d, "expired") => (
                            Vote::Unknown,
                            Some(SqlError::conflict("transaction too old (max_txn_age exceeded)")),
                        ),
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
                        apply_all(&c, &id, &parts_v).await;
                    });
                } else {
                    apply_all(cluster, &self.id, &parts_v).await;
                }
                Ok(())
            }
            Decision::Abort => {
                abort_all(cluster, &self.id, did, &part_ids, &parts_v).await;
                Err(first_err.unwrap_or_else(|| SqlError::conflict("prepare refused")))
            }
            Decision::Unknown => {
                // Some PREPARE replies were lost: resolve exactly like the
                // recovery agents do (TarnCommit!RecoverDecide), via the
                // decision shard's record and the participants' states.
                let expired = now_secs() - self.ts > cluster.cfg.max_txn_age;
                match resolve(cluster, &self.id, dshard, &parts_v, expired).await {
                    Ok(Some(true)) => Ok(()),
                    // Aborted for certain: retryable.
                    Ok(Some(false)) => Err(SqlError::conflict(match first_err {
                        Some(e) => format!("transaction aborted ({})", e.message),
                        None => "transaction aborted".to_string(),
                    })),
                    Ok(None) => Err(ambiguous(did, &"transaction expired before its outcome was known")),
                    Err(e) => Err(ambiguous(
                        did,
                        &format!("{}; recovery will finish transaction {}", e, self.id),
                    )),
                }
            }
        }
    }
}

/// A bucket, or nil.
fn opt_uint(m: &mut Mp, b: Option<u32>) {
    match b {
        Some(b) => m.uint(b as u64),
        None => m.nil(),
    };
}

/// The bucket of each write (`wb` of the commit procedures).
fn encode_buckets(m: &mut Mp, wb: &[u32]) {
    m.arr(wb.len());
    for &b in wb {
        m.uint(b as u64);
    }
}

fn now_secs() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

/// APPLY a committed transaction everywhere (TarnCommit!ApplyAt); the
/// decision shard's APPLY also marks the record COMMITTED. A participant
/// counts as done when it reports `applied` (or `none`: applied earlier and
/// garbage-collected). Failed APPLYs are retried a few times; if a shard
/// stays unreachable the commit is acknowledged anyway -- it is decided, SQL
/// readers wait on its W locks, and recovery applies it -- which gives up
/// direct read-your-writes for that transaction, exactly as ack-on-prepare
/// does (TarnCommit: Tradeoff_AckOnPrepare_DirectRYW).
async fn apply_all(cluster: &Arc<Cluster>, txid: &str, parts: &[usize]) {
    let mut m = Mp::new();
    m.arr(1).str(txid);
    let args = m.0;
    let mut todo: Vec<usize> = parts.to_vec();
    for attempt in 0..4u64 {
        if attempt > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(50 << attempt)).await;
        }
        let futs = todo.iter().map(|&p| cluster.call(p, "tarn_apply", &args));
        let results = futures::future::join_all(futs).await;
        let mut left = Vec::new();
        for (&p, r) in todo.iter().zip(results) {
            let r = r.and_then(|d| lua_status(&d).and_then(|(_, mut rd)| rd.string()));
            match r {
                Ok(st) if st == "applied" || st == "none" => {}
                Ok(st) => {
                    tracing::error!("APPLY of {} on shard {} returned '{}'", txid, cluster.shards[p].id, st);
                    left.push(p);
                }
                Err(e) => {
                    tracing::warn!("APPLY of {} on shard {} failed: {}", txid, cluster.shards[p].id, e);
                    left.push(p);
                }
            }
        }
        if left.is_empty() {
            return;
        }
        todo = left;
    }
    tracing::warn!(
        "APPLY of {} still pending on {} shard(s); recovery will finish it",
        txid,
        todo.len()
    );
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
/// Ok(Some(true)) = committed (and applied), Ok(Some(false)) = aborted,
/// Ok(None) = cannot be known any more (expired, records gone).
pub async fn resolve(
    cluster: &Arc<Cluster>,
    txid: &str,
    dshard: usize,
    parts: &[usize],
    expired: bool,
) -> SqlResult<Option<bool>> {
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
        match coord::resolve_step(coord::RecStatus::parse(&status), &states, expired) {
            coord::Step::Committed => {
                apply_all(cluster, txid, parts).await;
                return Ok(Some(true));
            }
            coord::Step::Aborted => {
                abort_all(cluster, txid, did, &part_ids, parts).await;
                return Ok(Some(false));
            }
            coord::Step::Gone => return Ok(None),
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

/// The most rows one storage read returns (`scan_page_rows` of lua/tarn.lua;
/// a page also ends at `scan_page_bytes`). A longer read continues where
/// the page ended (issue #77).
pub const PAGE_ROWS: u32 = 4096;

/// One page of a storage read: its rows and, when the read goes on, where
/// the next page starts (an opaque MessagePack value, passed back as is).
pub type Page = (Vec<Row>, Option<Vec<u8>>);

/// One native primary-index key part of a co-located read. A field is a
/// one-based physical field of a tuple from the earlier parent slot.
#[derive(Clone, Debug)]
pub enum JoinPart {
    Const(Value),
    Field(u32),
}

#[derive(Debug)]
pub struct JoinRead<'a> {
    pub table: &'a TableDef,
    /// Zero-based earlier slot; None for a constant prefix.
    pub parent: Option<usize>,
    pub parts: Vec<JoinPart>,
}

#[derive(Debug)]
pub struct JoinRows {
    /// Every native prefix actually probed, including those with no rows.
    pub keys: Vec<Vec<Value>>,
    pub rows: Vec<Row>,
}

#[derive(Debug)]
pub enum JoinedRead {
    /// An older storage node has not installed the optional read procedure.
    Unavailable,
    /// The shared reply budget was exhausted; no partial result is usable.
    Incomplete,
    Complete(Vec<JoinRows>),
}

/// Read dependent primary prefixes in one shard call. The planner proves
/// their co-location; storage supplies one snapshot and checks the union
/// of their prepared-write/DDL claims (TarnCommit!SnapshotRead).
pub async fn select_join(
    client: &Client,
    stream: u64,
    reads: &[JoinRead<'_>],
    lock_check: bool,
    begin: bool,
    route: &Route,
) -> SqlResult<JoinedRead> {
    let mut m = Mp::new();
    m.arr(4).arr(reads.len());
    for r in reads {
        m.arr(3)
            .uint(r.table.space_id as u64)
            .uint(r.parent.map_or(0, |p| p as u64 + 1))
            .arr(r.parts.len());
        for part in &r.parts {
            m.arr(2);
            match part {
                JoinPart::Const(v) => {
                    m.uint(0).val(v);
                }
                JoinPart::Field(f) => {
                    m.uint(1).uint(*f as u64);
                }
            }
        }
    }
    m.bool(lock_check).bool(begin);
    route.encode(&mut m);
    let r = match client.call(stream, "tarn_select_join", &m.0).await {
        Ok(r) => r,
        Err(tarn_iproto::Error::Server { code, .. }) if code == tarn_iproto::errcode::ER_NO_SUCH_PROC => {
            return Ok(JoinedRead::Unavailable);
        }
        Err(e) => return Err(from_iproto_read(e)),
    };
    let d = r.data().ok_or_else(|| crate::mpw::proto("no data"))?;
    join_rows_of(&d, reads)
}

fn join_rows_of(d: &Bytes, reads: &[JoinRead<'_>]) -> SqlResult<JoinedRead> {
    let (status, rd) = lua_status(d)?;
    if status != "ok" {
        return Err(crate::mpw::proto("unexpected co-located read status"));
    }
    let mut header = Rd::new(d);
    if header.array()? != 1 || header.array()? != 3 {
        return Err(crate::mpw::proto("invalid co-located read envelope"));
    }
    let mut rd = Rd::shared_at(d, rd);
    let n = rd.array()?;
    if n != 0 && n != reads.len() {
        return Err(crate::mpw::proto("co-located read slot count changed"));
    }
    let mut out = Vec::with_capacity(n);
    for r in reads.iter().take(n) {
        if rd.array()? != 2 {
            return Err(crate::mpw::proto("invalid co-located read slot"));
        }
        let nkeys = rd.array()?;
        let mut keys = Vec::with_capacity(nkeys);
        for _ in 0..nkeys {
            // Read-set keys outlive these result rows. Copy their strings
            // instead of retaining the whole reply buffer through slices.
            let mut key_rd = Rd::new(rd.raw_value()?);
            let parts = key_rd.array()?;
            if parts != r.parts.len() || parts == 0 {
                return Err(crate::mpw::proto("co-located read prefix width changed"));
            }
            let mut key = Vec::with_capacity(parts);
            for _ in 0..parts {
                key.push(key_rd.value()?);
            }
            keys.push(key);
        }
        let nrows = rd.array()?;
        let mut rows = Vec::with_capacity(nrows);
        for _ in 0..nrows {
            rows.push(rd.row_of(r.table)?);
        }
        out.push(JoinRows { keys, rows });
    }
    let complete = match rd.buf.first() {
        Some(0xc3) => true,
        Some(0xc2) => false,
        _ => return Err(crate::mpw::proto("missing co-located read completion marker")),
    };
    rd.skip()?;
    if !rd.buf.is_empty() || (complete && n != reads.len()) || (!complete && n != 0) {
        return Err(crate::mpw::proto("partial or malformed co-located read reply"));
    }
    Ok(if complete {
        JoinedRead::Complete(out)
    } else {
        JoinedRead::Incomplete
    })
}

/// The rows of a `{'ok', rows[, next]}` reply.
fn page_of(d: &Bytes, t: &TableDef) -> SqlResult<Page> {
    let (_, rd) = lua_status(d)?;
    let mut rd = Rd::shared_at(d, rd);
    let n = rd.array()?;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push(rd.row_of(t)?);
    }
    let next = if rd.buf.is_empty() || rd.is_nil() {
        None
    } else {
        Some(rd.raw_value()?.to_vec())
    };
    Ok((out, next))
}

/// Rows of several index keys in one call (`tarn_select_keys`): for each key,
/// the row with that full unique key, or every row whose index key starts
/// with it (at most `limit` per key); rows come back key by key. One page,
/// from `after` (the `next` of the page before) when given.
#[allow(clippy::too_many_arguments)]
pub async fn select_keys(
    client: &Client,
    stream: u64,
    t: &TableDef,
    index: u32,
    keys: &[Vec<Value>],
    limit: Option<u32>,
    lock_check: bool,
    begin: bool,
    after: Option<&[u8]>,
    route: &Route,
) -> SqlResult<Page> {
    let mut m = Mp::new();
    m.arr(8).uint(t.space_id as u64).uint(index as u64).arr(keys.len());
    for k in keys {
        m.vals(k);
    }
    match limit {
        Some(l) => m.uint(l as u64),
        None => m.nil(),
    };
    m.bool(lock_check).bool(begin);
    match after {
        Some(a) => m.raw(a),
        None => m.nil(),
    };
    route.encode(&mut m);
    let r = client
        .call(stream, "tarn_select_keys", &m.0)
        .await
        .map_err(from_iproto_read)?;
    let d = r.data().ok_or_else(|| crate::mpw::proto("no data"))?;
    page_of(&d, t)
}

/// A predicate a shard evaluates on stored tuples while counting
/// (`tarn_count`): a column against constants, as SQL compares them.
#[derive(Clone, Debug, PartialEq)]
pub struct StoragePred {
    pub col: usize,
    pub op: PredOp,
    /// Integers compare exactly; otherwise strings, by bytes.
    pub int: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub enum PredOp {
    Cmp(&'static str, Value),
    In(Vec<Value>),
    IsNull,
    NotNull,
}

/// Count rows of `t` on one shard (`tarn_count`): those the index scan
/// `key`/`iter`/`end` covers that satisfy `preds`, leaving out the stored
/// versions of the rows whose primary keys are in `exclude`.
#[allow(clippy::too_many_arguments)]
pub async fn count_rows(
    client: &Client,
    stream: u64,
    t: &TableDef,
    index: u32,
    key: &[Value],
    iter: Iter,
    end: Option<(&[Value], bool)>,
    preds: &[StoragePred],
    exclude: &[&[Value]],
    lock_check: bool,
    begin: bool,
    route: &Route,
) -> SqlResult<u64> {
    let mut m = Mp::new();
    m.arr(11)
        .uint(t.space_id as u64)
        .uint(index as u64)
        .vals(key)
        .uint(iter as u64);
    match end {
        Some((e, incl)) => m.vals(e).bool(incl),
        None => m.nil().bool(true),
    };
    m.arr(preds.len());
    for p in preds {
        m.arr(5).uint(t.field_of(p.col) as u64 + 1);
        match &p.op {
            PredOp::Cmp(op, v) => m.str(op).val(v),
            PredOp::In(vs) => m.str("in").vals(vs),
            PredOp::IsNull => m.str("null").nil(),
            PredOp::NotNull => m.str("notnull").nil(),
        };
        m.val(&t.layout.fills[p.col]).bool(p.int);
    }
    m.arr(exclude.len());
    for k in exclude {
        m.vals(k);
    }
    m.bool(lock_check).bool(begin);
    route.encode(&mut m);
    let r = client
        .call(stream, "tarn_count", &m.0)
        .await
        .map_err(from_iproto_read)?;
    let d = r.data().ok_or_else(|| crate::mpw::proto("no data"))?;
    let (_, mut rd) = lua_status(&d)?;
    match rd.value()? {
        Value::Int(n) if n >= 0 => Ok(n as u64),
        Value::UInt(n) => Ok(n),
        other => Err(crate::mpw::proto(format!("bad count {:?}", other))),
    }
}

/// Read rows of `t` from one shard, optionally inside a stream.
/// `key`/`iter`/`end` describe a bounded index scan (see tarn_select). One
/// page, from `after` (the `next` of the page before) when given.
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
    begin: bool,
    after: Option<&[u8]>,
    route: &Route,
) -> SqlResult<Page> {
    // Unbounded point/prefix lookups inside a running stream transaction use
    // plain IPROTO SELECT; a full page goes on after its last tuple (which
    // tarn_select takes as a position too). (Nothing checks their bucket
    // here: the transaction's validation does, as they are in its read set.)
    if end.is_none() && !lock_check && !begin && after.is_none() {
        let mut k = Mp::new();
        k.vals(key);
        let n = limit.unwrap_or(u32::MAX).min(PAGE_ROWS);
        let r = client
            .select(stream, t.space_id, index, iter, 0, n, &k.0)
            .await
            .map_err(from_iproto_read)?;
        let tuples = r.tuples().map_err(crate::mpw::proto)?;
        let mut out = Vec::with_capacity(tuples.len());
        for tup in &tuples {
            out.push(Rd::shared(tup).row_of(t)?);
        }
        let next = match tuples.last() {
            Some(last) if tuples.len() == n as usize && Some(n) != limit => Some(last.to_vec()),
            _ => None,
        };
        return Ok((out, next));
    }
    let mut m = Mp::new();
    m.arr(12)
        .uint(t.space_id as u64)
        .uint(index as u64)
        .vals(key)
        .uint(iter as u64);
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
    m.nil().bool(begin); // offset, begin
    match after {
        Some(a) => m.raw(a),
        None => m.nil(),
    };
    route.encode(&mut m);
    let r = client
        .call(stream, "tarn_select", &m.0)
        .await
        .map_err(from_iproto_read)?;
    let d = r.data().ok_or_else(|| crate::mpw::proto("no data"))?;
    page_of(&d, t)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn join_table() -> TableDef {
        use crate::catalog::Catalog;
        use crate::plan::Stmt;
        use crate::planner::{plan, PlanCtx};

        let catalog = Catalog::default();
        let cx = PlanCtx {
            catalog: &catalog,
            db: Some("d"),
            params: &[],
            conn_coll: "utf8mb4_bin",
            default_coll: "utf8mb4_bin",
        };
        let Stmt::CreateTable { mut def, .. } = plan(
            "CREATE TABLE t (tenant VARCHAR(20) NOT NULL, id INT NOT NULL, label VARCHAR(20), \
             PRIMARY KEY (tenant, id)) PARTITION BY KEY(tenant)",
            &cx,
        )
        .unwrap() else {
            panic!()
        };
        def.space_id = 512;
        *def
    }

    fn join_reply(slot: bool, complete: bool) -> Bytes {
        let mut m = Mp::new();
        m.arr(1).arr(3).str("ok").arr(usize::from(slot));
        if slot {
            m.arr(2).arr(1).vals(&[Value::str("tenant")]).arr(1);
            // A short, physically reordered tuple: id then tenant, with
            // the label absent (the decoder must use its SQL fill).
            m.vals(&[Value::Int(4), Value::str("tenant")]);
        }
        m.bool(complete);
        m.bytes()
    }

    #[test]
    fn grouped_reply_decodes_physical_rows_and_detaches_read_keys() {
        let mut t = join_table();
        t.layout.identity = false;
        t.layout.col_field.swap(0, 1);
        t.layout.field_col[0] = Some(1);
        t.layout.field_col[1] = Some(0);
        t.layout.fills[2] = Value::str("filled");
        let reads = [JoinRead {
            table: &t,
            parent: None,
            parts: vec![JoinPart::Const(Value::str("tenant"))],
        }];
        let data = join_reply(true, true);
        let JoinedRead::Complete(groups) = join_rows_of(&data, &reads).unwrap() else {
            panic!()
        };
        assert_eq!(groups[0].keys, vec![vec![Value::str("tenant")]]);
        assert_eq!(
            groups[0].rows,
            vec![vec![Value::str("tenant"), Value::Int(4), Value::str("filled")]]
        );
        let Value::Str(key) = &groups[0].keys[0][0] else {
            panic!()
        };
        let buffer = data.as_ptr() as usize..data.as_ptr() as usize + data.len();
        assert!(
            !buffer.contains(&(key.as_ptr() as usize)),
            "read sets must not retain the whole result buffer"
        );
    }

    #[test]
    fn grouped_reply_rejects_partial_or_wrong_envelopes() {
        let t = join_table();
        let reads = [JoinRead {
            table: &t,
            parent: None,
            parts: vec![JoinPart::Const(Value::str("tenant"))],
        }];
        assert!(matches!(
            join_rows_of(&join_reply(false, false), &reads).unwrap(),
            JoinedRead::Incomplete
        ));
        assert!(join_rows_of(&join_reply(true, false), &reads).is_err());
        assert!(join_rows_of(&join_reply(false, true), &reads).is_err());
        let mut wrong = join_reply(true, true).to_vec();
        wrong[0] = 0x92; // claims two CALL return values, but has only one
        assert!(join_rows_of(&Bytes::from(wrong), &reads).is_err());
        // Complete is outside the status table: valid MessagePack, wrong API.
        let mut wrong = Mp::new();
        wrong.arr(2).arr(2).str("ok").arr(0).bool(true);
        assert!(join_rows_of(&wrong.bytes(), &reads).is_err());
        let mut truncated = join_reply(true, true).to_vec();
        truncated.pop();
        assert!(join_rows_of(&Bytes::from(truncated), &reads).is_err());
    }

    #[test]
    fn failed_lock_gen_reads_the_fourth_field() {
        let reply = |fields: &[&str]| {
            let mut m = Mp::new();
            m.arr(1).arr(fields.len()).str(fields[0]);
            if fields.len() > 1 {
                m.uint(1205);
            }
            for f in &fields[2..] {
                m.str(f);
            }
            m.0
        };
        assert_eq!(
            failed_lock_gen(&reply(&["timeout", "1205", "Lock wait timeout exceeded", "g/7"])),
            Some("g/7".to_string())
        );
        // No locks were left held: no generation.
        assert_eq!(
            failed_lock_gen(&reply(&["timeout", "1205", "Lock wait timeout exceeded"])),
            None
        );
    }
}
