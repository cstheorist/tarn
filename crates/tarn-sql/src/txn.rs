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
//!   after bounded APPLY attempts unless `ack_on_prepare` is set. A successful
//!   SQL commit can therefore still have pending APPLYs. `CommitOutcome`
//!   preserves them for the lazy-index session barrier (LazyBarrier.tla).
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
//! Query diagnostics only observe routed targets and commit cardinality.
//! They do not change TarnCommit state, messages, retry decisions, or locks;
//! no observer borrow is held across an await. They are outside the TLA+
//! safety state and are bounded and tested separately (observe.rs).
//!
//! The pure decision logic (what to do with a set of votes, when it is safe
//! to apply/abort) is `tarn_verified::coord`, proven in Verus to only decide
//! COMMIT on unanimous YES and never to decide both ways.

use crate::catalog::TableDef;
use crate::cluster::Cluster;
use crate::mpw::{ambiguous, from_iproto, from_iproto_read, lua_status, Mp, Rd};
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tarn_common::{SqlError, SqlResult, Value};
use tarn_iproto::{Client, Iter};
use tarn_verified::coord::{self, Decision, Vote};

pub type Row = Vec<Value>;

/// Only a complete, explicit durable NO can justify aborting a parallel
/// commit. Expiry, unavailable freshness, malformed and future statuses
/// are no vote: a previous PREPARE might already have committed.
fn prepare_reply(data: &[u8]) -> (Vote, Option<SqlError>) {
    let parsed = (|| -> SqlResult<(Vote, Option<SqlError>)> {
        let mut rd = Rd::new(data);
        if rd.array()? != 1 {
            return Err(crate::mpw::proto("PREPARE must return exactly one status"));
        }
        let n = rd.array()?;
        let Value::Str(tag) = rd.value()? else {
            return Err(crate::mpw::proto("PREPARE status must be a string"));
        };
        let tag: &[u8] = &tag;
        let result = match tag {
            b"yes" if n == 1 => (Vote::Yes, None),
            b"no" | b"moved" | b"expired" | b"clock_unknown" if n == 3 => {
                let code = rd.value()?;
                if !matches!(code, Value::Int(c) if c > 0 && c <= u16::MAX as i64)
                    && !matches!(code, Value::UInt(c) if c > 0 && c <= u16::MAX as u64)
                {
                    return Err(crate::mpw::proto("invalid PREPARE error code"));
                }
                let Value::Str(message) = rd.value()? else {
                    return Err(crate::mpw::proto("PREPARE error message must be a string"));
                };
                if matches!(tag, b"expired" | b"clock_unknown") {
                    (
                        Vote::Unknown,
                        Some(SqlError::conflict(String::from_utf8_lossy(&message))),
                    )
                } else {
                    let err = lua_status(data)
                        .err()
                        .ok_or_else(|| crate::mpw::proto("invalid PREPARE NO"))?;
                    (Vote::No, Some(err))
                }
            }
            _ => return Err(crate::mpw::proto("unsupported or incomplete PREPARE status")),
        };
        if !rd.buf.is_empty() {
            return Err(crate::mpw::proto("trailing PREPARE reply data"));
        }
        Ok(result)
    })();
    parsed.unwrap_or_else(|e| (Vote::Unknown, Some(e)))
}

/// PREPARE is sent once. Only a complete no-vote reply authorizes cleaning
/// up this gateway's preserved read stream; transport uncertainty must be
/// resolved from durable records, without cancelling or resending PREPARE.
async fn prepare_once(client: &Client, stream: u64, args: &[u8]) -> (Vote, Option<SqlError>) {
    match client.call(stream, "tarn_prepare", args).await {
        Ok(r) => match r.data() {
            Some(data) => {
                let parsed = prepare_reply(&data);
                if stream != 0
                    && matches!(parsed.0, Vote::Unknown)
                    && parsed.1.as_ref().is_some_and(|e| e.code == 1213)
                    && (crate::mpw::status_is(&data, "expired") || crate::mpw::status_is(&data, "clock_unknown"))
                {
                    // These tags certify no new vote. If cleanup cannot be
                    // confirmed, retire the connection so its read streams
                    // are released rather than lost from our bookkeeping.
                    let cleaned =
                        match tokio::time::timeout(std::time::Duration::from_secs(1), client.rollback(stream)).await {
                            Ok(Ok(())) => true,
                            Ok(Err(tarn_iproto::Error::Server { code, .. }))
                                if code == tarn_iproto::errcode::ER_NO_TRANSACTION =>
                            {
                                true
                            }
                            _ => false,
                        };
                    if !cleaned {
                        client.close();
                    }
                }
                parsed
            }
            None => (Vote::Unknown, Some(crate::mpw::proto("no data"))),
        },
        Err(e) => (Vote::Unknown, Some(from_iproto(e))),
    }
}

/// A known-committed transaction whose physical APPLY is not yet confirmed
/// everywhere. Participant identifiers are stable shard IDs, not positions
/// in a gateway's current routing map. This is never an unknown decision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingApply {
    pub txid: String,
    pub participants: Vec<u32>,
}

/// Cumulative session catch-up obligations from a successful SQL commit.
/// Keep these even while read-your-writes through lazy indexes is disabled:
/// enabling it later must cover the session's earlier commits too.
#[must_use = "a session must retain successful writes for its lazy-index barrier"]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CommitOutcome {
    pub lazy_buckets: BTreeSet<u32>,
    pub pending_apply: Option<PendingApply>,
}

/// A recovered decision is separate from the physical completion of its
/// participants. Missing expired records cannot certify an unknown commit.
#[derive(Debug, PartialEq, Eq)]
pub enum Resolved {
    Committed { pending_apply: Vec<u32> },
    Aborted,
    Gone,
}

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

struct CancelCleanup {
    streams: Vec<(Client, u64)>,
    locks: Vec<Client>,
    active: bool,
}

impl Drop for CancelCleanup {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        // A cancelled acquisition/read may execute after its caller drops.
        // Retire the physical session so neither a late grant nor a late
        // BEGIN can outlive cleanup. Other sent mutations remain unknown.
        for (client, _) in &self.streams {
            client.close();
        }
        for client in &self.locks {
            client.close();
        }
    }
}

pub struct Txn {
    resources: Option<crate::budget::Account>,
    cleanup: Option<CancelCleanup>,
    pub id: String,
    pub ts: f64,
    pub explicit: bool,
    pub mode: Mode,
    /// Lock wait timeout (innodb_lock_wait_timeout), seconds.
    pub lock_timeout: f64,
    /// Shards where this transaction holds row locks.
    pub plocks: BTreeMap<usize, ShardLocks>,
    /// Shards its row lock requests went to, whatever came of them: there
    /// the shard may keep row-lock state of the transaction even when it
    /// granted nothing (a refused request, a lost reply), which ends with
    /// the transaction (`tarn_unlock`, or the Lua procedures' commits).
    pub lock_shards: BTreeSet<usize>,
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
        let id = cluster.new_txid();
        Txn {
            resources: Some(cluster.resources.account(cluster.cfg.resources.transaction_bytes)),
            cleanup: Some(CancelCleanup {
                streams: Vec::new(),
                locks: Vec::new(),
                active: true,
            }),
            id,
            ts: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs_f64())
                .unwrap_or(0.0),
            explicit,
            mode,
            lock_timeout,
            plocks: BTreeMap::new(),
            lock_shards: BTreeSet::new(),
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
        self.charge(4096 + keys.iter().map(|k| crate::budget::row_bytes(&k.key) * 8).sum::<u64>())?;
        if let Some(cleanup) = &mut self.cleanup {
            cleanup.locks.push(client.clone());
        }
        self.lock_shards.insert(shard);
        // Lock requests change nothing that a resend could duplicate: a lost
        // reply just fails the statement (the locks are released with the
        // transaction).
        let _wire = crate::budget::retain(crate::budget::WIRE_BYTES)?;
        let r = client.call(0, "tarn_lock", &m.0).await.map_err(from_iproto_read)?;
        let d = r.data().ok_or_else(|| crate::mpw::proto("no data"))?;
        let rd = self.lock_status(shard, &client, &d)?;
        let mut rd = Rd::shared_at(&d, rd);
        let gen = rd.string()?;
        self.note_locks(shard, client, gen)?;
        let n = rd.array()?;
        crate::budget::decoded(
            d.len(),
            n,
            keys.iter().map(|k| k.table.columns.len()).max().unwrap_or(0),
        )?;
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
                self.charge(
                    4096 + req
                        .2
                        .iter()
                        .map(|r| crate::budget::row_bytes(&r.prefix) * 8)
                        .sum::<u64>(),
                )?;
                if let Some(cleanup) = &mut self.cleanup {
                    cleanup.locks.push(client.clone());
                }
                self.lock_shards.insert(req.0);
                let _wire = crate::budget::retain(crate::budget::WIRE_BYTES)?;
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
            self.charge(
                4096 + req
                    .2
                    .iter()
                    .map(|r| crate::budget::row_bytes(&r.prefix) * 8)
                    .sum::<u64>(),
            )?;
            if let Some(cleanup) = &mut self.cleanup {
                cleanup.locks.push(client.clone());
            }
            calls.push((client, encode(self, req).0));
        }
        self.lock_shards.extend(reqs.iter().map(|r| r.0));
        let replies = futures::future::join_all(calls.iter().map(|(client, m)| async move {
            let guard = crate::budget::retain(crate::budget::WIRE_BYTES)?;
            Ok((client.call(0, "tarn_lock_range", &m.0).await, guard))
        }))
        .await;
        let mut first_err = None;
        for ((req, (client, _)), r) in reqs.iter().zip(&calls).zip(replies) {
            let decoded = match r {
                Ok((reply, _wire)) => self.decode_ranges(req.0, client, reply, &req.2),
                Err(error) => Err(error),
            };
            match decoded {
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
        if n != ranges.len() {
            return Err(crate::mpw::proto("lock range count changed"));
        }
        crate::budget::decoded(d.len(), n, 0)?;
        let mut out = Vec::with_capacity(n);
        for rg in ranges.iter().take(n) {
            let nrows = rd.array()?;
            crate::budget::decoded(0, nrows, rg.table.columns.len())?;
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

    /// The shards where this transaction may hold row locks, or row-lock
    /// state (see `lock_shards`), forgotten here: the caller releases them.
    fn take_lock_shards(&mut self) -> Vec<usize> {
        let mut s = std::mem::take(&mut self.lock_shards);
        s.extend(std::mem::take(&mut self.plocks).into_keys());
        s.into_iter().collect()
    }

    /// Only request-only nonparticipants need an explicit unlock after a
    /// successful commit. Participants either released their locks already
    /// or still need them until recovery finishes the known-committed APPLY.
    fn take_extra_lock_shards(&mut self, parts: &BTreeSet<usize>) -> Vec<usize> {
        self.take_lock_shards()
            .into_iter()
            .filter(|p| !parts.contains(p))
            .collect()
    }

    /// Release every row lock of this transaction (best effort).
    async fn release_plocks(&mut self, cluster: &Arc<Cluster>) {
        let shards = self.take_lock_shards();
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
        crate::observe::shard(shard);
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
        crate::observe::shard(shard);
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
        self.charge(4096)?;
        if let Some(cleanup) = &mut self.cleanup {
            cleanup.streams.push((client.clone(), id));
        }
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

    pub fn note_point(
        &mut self,
        shard: usize,
        space: u32,
        index: u32,
        key: Vec<Value>,
        bucket: Option<u32>,
    ) -> SqlResult<()> {
        self.charge(crate::budget::row_bytes(&key).saturating_mul(8))?;
        let key = crate::budget::own_row(key);
        self.rpoints.entry(shard).or_default().push((space, index, key, bucket));
        Ok(())
    }

    pub fn note_range(&mut self, shard: usize, mut r: RangeRead) -> SqlResult<()> {
        self.charge(
            (256 + crate::budget::row_bytes(r.lo.as_deref().unwrap_or(&[]))
                + crate::budget::row_bytes(r.hi.as_deref().unwrap_or(&[])))
            .saturating_mul(8),
        )?;
        r.lo = r.lo.map(crate::budget::own_row);
        r.hi = r.hi.map(crate::budget::own_row);
        self.rranges.entry(shard).or_default().push(r);
        Ok(())
    }

    fn charge(&mut self, bytes: u64) -> SqlResult<()> {
        self.resources.as_mut().map_or(Ok(()), |account| account.charge(bytes))
    }

    pub fn snapshot_charge(&mut self) -> SqlResult<()> {
        let cost = self.writes.values().fold(128u64, |n, write| {
            n.saturating_add(
                crate::budget::row_bytes(&write.pk) + write.row.as_ref().map_or(0, |r| crate::budget::row_bytes(r)),
            )
        });
        self.charge(cost.saturating_mul(8))?;
        crate::budget::charge(cost.saturating_mul(2))
    }

    pub fn savepoint_name_charge(&mut self, bytes: usize) -> SqlResult<()> {
        self.charge((bytes as u64).saturating_mul(4))
    }

    pub fn buffer(&mut self, mut w: BufWrite) -> SqlResult<()> {
        self.charge(
            (256 + crate::budget::row_bytes(&w.pk) + w.row.as_ref().map_or(0, |r| crate::budget::row_bytes(r)))
                .saturating_mul(8),
        )?;
        self.check_def(&w.table)?;
        w.pk = crate::budget::own_row(w.pk);
        w.row = w.row.map(crate::budget::own_row);
        crate::observe::shard(w.shard);
        crate::observe::bucket(None);
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
    pub fn rollback(mut self, cluster: &Arc<Cluster>) {
        if let Some(cleanup) = &mut self.cleanup {
            cleanup.active = false;
        }
        for (_, s) in std::mem::take(&mut self.streams) {
            tokio::spawn(async move {
                let _ = s.client.rollback(s.id).await;
            });
        }
        let shards = self.take_lock_shards();
        self.unlock_later(cluster, shards);
    }

    /// Release this transaction's row locks on `shards` in the background
    /// (best effort, nothing to wait for).
    fn unlock_later(&self, cluster: &Arc<Cluster>, shards: Vec<usize>) {
        if shards.is_empty() {
            return;
        }
        let c = cluster.clone();
        let id = self.id.clone();
        tokio::spawn(async move {
            let mut m = Mp::new();
            m.arr(1).str(&id);
            let futs = shards.iter().map(|&s| c.call(s, "tarn_unlock", &m.0));
            let _ = futures::future::join_all(futs).await;
        });
    }

    pub async fn commit(self, cluster: &Arc<Cluster>) -> SqlResult<CommitOutcome> {
        // Completion owns the transaction and query reservations even if the
        // SQL caller disconnects. No new execution budget is acquired here.
        let guard = crate::budget::current().map(|q| q.guard());
        let cluster = cluster.clone();
        tokio::spawn(crate::observe::inherit(async move {
            let result = self.commit_inner(&cluster).await;
            drop(guard);
            result
        }))
        .await
        .map_err(|_| SqlError::new(1180, "40003", "Commit worker stopped; outcome may be unknown"))?
    }

    async fn commit_inner(mut self, cluster: &Arc<Cluster>) -> SqlResult<CommitOutcome> {
        if let Some(e) = self.doomed.take() {
            self.rollback(cluster);
            return Err(e);
        }
        // Resource checks end before an irrevocable decision. Cancellation
        // during commit is owned by the existing durable recovery protocol.
        if let Some(cleanup) = &mut self.cleanup {
            cleanup.active = false;
        }
        let parts = self.participants();
        crate::observe::commit(parts.len());
        for &part in &parts {
            crate::observe::shard(part);
        }
        if parts.is_empty() {
            // (lock requests that granted nothing, if any)
            let shards = self.take_lock_shards();
            self.unlock_later(cluster, shards);
            return Ok(CommitOutcome::default());
        }
        let lazy_buckets = self
            .writes
            .values()
            .filter(|w| w.table.has_lazy())
            .map(|w| cluster.bucket_of(&w.table.shard_key_of_pk(&w.pk)))
            .collect();
        let res = if self.mode == Mode::ReadCommitted && self.writes.is_empty() {
            // Nothing to validate: just end the locks.
            self.release_plocks(cluster).await;
            Ok(Vec::new())
        } else if self.writes.is_empty() {
            self.commit_read_only(cluster, &parts).await.map(|()| Vec::new())
        } else if parts.len() == 1 && self.mode == Mode::ReadCommitted {
            self.commit_rc(cluster, *parts.iter().next().unwrap())
                .await
                .map(|()| Vec::new())
        } else if parts.len() == 1 {
            self.commit_local(cluster, *parts.iter().next().unwrap())
                .await
                .map(|()| Vec::new())
        } else {
            self.commit_2pc(cluster, &parts).await
        };
        if res.is_ok() {
            // Durable recovery owns any participant locks whose APPLY is
            // pending; only extra lock requests may be unlocked here.
            let rest = self.take_extra_lock_shards(&parts);
            self.unlock_later(cluster, rest);
        } else if !self.plocks.is_empty() || !self.lock_shards.is_empty() {
            self.release_plocks(cluster).await;
        }
        if let Err(e) = &res {
            if e.code == 1213 {
                cluster.stats.conflicts.fetch_add(1, Ordering::Relaxed);
            }
        }
        res.map(|pending| CommitOutcome {
            lazy_buckets,
            pending_apply: (!pending.is_empty()).then(|| PendingApply {
                txid: self.id.clone(),
                participants: pending,
            }),
        })
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
            // The shard's native procedure validates in the stream what it
            // can without Lua (crates/tarn-storage/src/procs.rs `validate`)
            // and hands the rest back ("lua"), having changed nothing. Not
            // sent where the transaction has no stream, or sent row lock
            // requests: their row-lock state only the Lua procedure ends.
            let native = id != 0 && !self.lock_shards.contains(&p) && !client.marked(MARK_NO_NATIVE_VALIDATE);
            futs.push(async move {
                use tarn_iproto::errcode::{
                    ER_ACCESS_DENIED, ER_LOAD_FUNCTION, ER_LOAD_MODULE, ER_NO_SUCH_FUNCTION, ER_NO_SUCH_PROC,
                };
                // Read-only: a lost connection or a demoted leader just
                // means "retry" (nothing was written).
                if native {
                    match client.call(id, "tarn_storage.ts_validate", &m.0).await {
                        Ok(r) => {
                            let d = r.data().ok_or_else(|| crate::mpw::proto("no data"))?;
                            if !crate::mpw::status_is(&d, "lua") {
                                return lua_status(&d).map(|_| ());
                            }
                        }
                        Err(tarn_iproto::Error::Server {
                            code:
                                ER_NO_SUCH_PROC | ER_NO_SUCH_FUNCTION | ER_LOAD_FUNCTION | ER_LOAD_MODULE | ER_ACCESS_DENIED,
                            message,
                        }) => {
                            tracing::debug!("native validation unavailable on this shard: {}", message);
                            client.mark(MARK_NO_NATIVE_VALIDATE);
                        }
                        Err(e) => return Err(from_iproto_read(e)),
                    }
                }
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
        let r = match self.call_native_commit(&client, id, shard, &m.0).await {
            Some(r) => r,
            None => client.call(id, "tarn_commit_local", &m.0).await,
        };
        let r = r.map_err(|e| match e {
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

    /// The shard's native procedure makes the one-shard commits it can
    /// without Lua (crates/tarn-storage/src/procs.rs `commit_local`), also in
    /// the transaction's stream, and hands the others back ("lua") having
    /// written nothing, its stream transaction and reads as they were: its
    /// reply, or None for a commit handed back, or not sent to it -- one that
    /// writes a vinyl table or needs lazy outbox capture; one that sent row
    /// lock requests to the shard, whose row-lock state there only the Lua
    /// procedure releases (it hands back those that hold locks); or to a
    /// shard without it, which the connection notes. Its errors are
    /// tarn_commit_local's.
    async fn call_native_commit(
        &self,
        client: &Client,
        stream: u64,
        shard: usize,
        args: &[u8],
    ) -> Option<Result<tarn_iproto::Response, tarn_iproto::Error>> {
        use tarn_iproto::errcode::{
            ER_ACCESS_DENIED, ER_LOAD_FUNCTION, ER_LOAD_MODULE, ER_NO_SUCH_FUNCTION, ER_NO_SUCH_PROC,
        };
        if client.marked(MARK_NO_NATIVE_COMMIT) || !self.native_commit_eligible(shard) {
            return None;
        }
        match client.call(stream, "tarn_storage.ts_commit_local", args).await {
            Ok(r) => match r.data() {
                Some(d) if crate::mpw::status_is(&d, "lua") => None,
                _ => Some(Ok(r)),
            },
            Err(tarn_iproto::Error::Server {
                code: ER_NO_SUCH_PROC | ER_NO_SUCH_FUNCTION | ER_LOAD_FUNCTION | ER_LOAD_MODULE | ER_ACCESS_DENIED,
                message,
            }) => {
                tracing::debug!("native commits unavailable on this shard: {}", message);
                client.mark(MARK_NO_NATIVE_COMMIT);
                None
            }
            Err(e) => Some(Err(e)),
        }
    }

    fn native_commit_eligible(&self, shard: usize) -> bool {
        !self.lock_shards.contains(&shard)
            && !self.writes.values().any(|w| {
                w.shard == shard && (!matches!(w.table.engine, crate::catalog::Engine::Memtx) || w.table.has_lazy())
            })
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

    async fn commit_2pc(&mut self, cluster: &Arc<Cluster>, parts: &BTreeSet<usize>) -> SqlResult<Vec<u32>> {
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
        // Freeze one decision writer using the transaction identity and a
        // single membership snapshot. Canonical durable IDs spread records
        // across a common writer set independently of gateway shard order.
        // PREPARE still fences membership/leaving changes (lua BK.prep), and
        // every durable record and recovery call uses this same `did`.
        let writers: BTreeSet<usize> = self.writes.values().map(|w| w.shard).collect();
        let dshard = {
            let members = cluster.members.read();
            let inactive: HashSet<u32> = members
                .shards
                .iter()
                .filter(|s| s.state != "active")
                .map(|s| s.id)
                .collect();
            let candidates = writers
                .iter()
                .map(|&position| {
                    let id = cluster.shards[position].id;
                    crate::placement::Writer {
                        id,
                        position,
                        active: !inactive.contains(&id),
                    }
                })
                .collect();
            crate::placement::decision_writer(&self.id, candidates)
                .expect("a writing transaction has a writer")
                .position
        };
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
            futs.push(async move { prepare_once(&client, id, &m.0).await });
        }
        let results = futures::future::join_all(futs).await;
        let votes: Vec<Vote> = results.iter().map(|(v, _)| *v).collect();
        let first_err = results.into_iter().find_map(|(_, e)| e);
        self.streams.clear(); // consumed by PREPARE, or explicitly cleaned after a no-vote
        let parts_v: Vec<usize> = parts.iter().copied().collect();
        match coord::decide(&votes) {
            Decision::Commit => {
                if cluster.cfg.ack_on_prepare {
                    let c = cluster.clone();
                    let id = self.id.clone();
                    tokio::spawn(async move {
                        let _ = apply_all(&c, &id, &parts_v).await;
                    });
                    // The background task may finish immediately, but no
                    // completion has been observed on this session yet.
                    Ok(part_ids)
                } else {
                    let pending = apply_all(cluster, &self.id, &parts_v).await;
                    Ok(pending.iter().map(|&p| cluster.shards[p].id).collect())
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
                let expired = now_secs() - self.ts > cluster.cfg.max_txn_age;
                match resolve(cluster, &self.id, dshard, &parts_v, expired).await {
                    Ok(Resolved::Committed { pending_apply }) => Ok(pending_apply),
                    // Aborted for certain: retryable.
                    Ok(Resolved::Aborted) => Err(SqlError::conflict(match first_err {
                        Some(e) => format!("transaction aborted ({})", e.message),
                        None => "transaction aborted".to_string(),
                    })),
                    Ok(Resolved::Gone) => Err(ambiguous(did, &"transaction expired before its outcome was known")),
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
/// stays unreachable the commit is acknowledged anyway -- it is decided,
/// strong SQL readers wait on its W locks, and recovery applies it. Return
/// the exact remaining participants so a lazy read cannot mistake that
/// acknowledgement for an already-applied row/outbox (LazyBarrier!Apply).
async fn apply_all(cluster: &Arc<Cluster>, txid: &str, parts: &[usize]) -> Vec<usize> {
    let mut todo: Vec<usize> = parts.to_vec();
    for attempt in 0..4u64 {
        if attempt > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(50 << attempt)).await;
        }
        todo = apply_positions_once(cluster, txid, &todo).await;
        if todo.is_empty() {
            return todo;
        }
    }
    tracing::warn!(
        "APPLY of {} still pending on {} shard(s); recovery will finish it",
        txid,
        todo.len()
    );
    todo
}

/// Only this exact terminal reply can discharge a known-committed APPLY.
/// In particular, `yes`, an empty/malformed reply or a partial status table
/// cannot erase a session obligation. `none` is safe only for this API's
/// known-committed transactions: their YES record can disappear only after
/// APPLY and the existing participant-GC protections (TarnCommit!GCPart).
fn applied_reply(data: &[u8]) -> SqlResult<()> {
    lua_status(data)?;
    let mut rd = Rd::new(data);
    if rd.array()? != 1 || rd.array()? != 2 || rd.value()? != Value::str("ok") {
        return Err(crate::mpw::proto("invalid APPLY acknowledgement"));
    }
    let status = rd.value()?;
    if !matches!(status, Value::Str(s) if s.as_ref() == b"applied" || s.as_ref() == b"none") || !rd.buf.is_empty() {
        return Err(crate::mpw::proto("nonterminal or malformed APPLY acknowledgement"));
    }
    Ok(())
}

async fn apply_positions_once(cluster: &Arc<Cluster>, txid: &str, parts: &[usize]) -> Vec<usize> {
    let mut m = Mp::new();
    m.arr(1).str(txid);
    let args = m.0;
    let futs = parts.iter().map(|&p| cluster.call(p, "tarn_apply", &args));
    let results = futures::future::join_all(futs).await;
    let mut left = Vec::new();
    for (&p, r) in parts.iter().zip(results) {
        if let Err(e) = r.and_then(|d| applied_reply(&d)) {
            tracing::warn!("APPLY of {} on shard {} failed: {}", txid, cluster.shards[p].id, e);
            left.push(p);
        }
    }
    left
}

/// Make one idempotent APPLY attempt for a known-committed session
/// obligation. Errors and malformed acknowledgements remain pending. The
/// caller owns the original read deadline, retries and safe fallback; this
/// helper neither replays SQL nor starts an unbounded retry loop.
pub async fn apply_pending(cluster: &Arc<Cluster>, pending: &PendingApply) -> SqlResult<Vec<u32>> {
    if pending.txid.is_empty() {
        return Err(crate::mpw::proto("empty transaction ID in pending APPLY"));
    }
    let positions: Vec<usize> = pending
        .participants
        .iter()
        .map(|&id| {
            cluster
                .shard_position(id)
                .ok_or_else(|| crate::mpw::proto(format!("unknown pending APPLY shard {}", id)))
        })
        .collect::<SqlResult<_>>()?;
    Ok(apply_positions_once(cluster, &pending.txid, &positions)
        .await
        .into_iter()
        .map(|p| cluster.shards[p].id)
        .collect())
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
/// A committed decision retains the exact participants still awaiting
/// confirmed APPLY. Gone records cannot certify a previously unknown
/// decision, even if they would be safe terminal replies for a transaction
/// already known to have committed.
pub async fn resolve(
    cluster: &Arc<Cluster>,
    txid: &str,
    dshard: usize,
    parts: &[usize],
    expired: bool,
) -> SqlResult<Resolved> {
    if parts.is_empty() {
        return Err(crate::mpw::proto("recovery has no participants"));
    }
    let part_ids: Vec<u32> = parts.iter().map(|&p| cluster.shards[p].id).collect();
    let did = cluster.shards[dshard].id;
    let mut idm = Mp::new();
    idm.arr(1).str(txid);
    for _ in 0..4 {
        let status = call_str(cluster, dshard, "tarn_txn_status", &idm.0).await?;
        let rec = coord::RecStatus::parse(&status);
        // resolve_step proves terminal decisions independent of participant
        // state. Do not let an unavailable participant turn an authoritative
        // COMMITTED decision into an unknown outcome before apply_all can
        // retain it. Placeholder states are never used for an open decision.
        let mut states = vec![coord::PartState::None; parts.len()];
        if !matches!(rec, coord::RecStatus::Committed | coord::RecStatus::Aborted) {
            for (&p, state) in parts.iter().zip(&mut states) {
                let s = call_str(cluster, p, "tarn_part_status", &idm.0).await?;
                *state = coord::PartState::parse(&s);
            }
        }
        match coord::resolve_step(rec, &states, expired) {
            coord::Step::Committed => {
                let pending = apply_all(cluster, txid, parts).await;
                return Ok(Resolved::Committed {
                    pending_apply: pending.into_iter().map(|p| cluster.shards[p].id).collect(),
                });
            }
            coord::Step::Aborted => {
                abort_all(cluster, txid, did, &part_ids, parts).await;
                return Ok(Resolved::Aborted);
            }
            coord::Step::Gone => return Ok(Resolved::Gone),
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
    crate::observe::read(route);
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
    let (reply, _wire) = crate::budget::call_read(client, stream, "tarn_select_join", &m.0).await?;
    let r = match reply {
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
    crate::budget::decoded(d.len(), n, 0)?;
    let mut out = Vec::with_capacity(n);
    for r in reads.iter().take(n) {
        if rd.array()? != 2 {
            return Err(crate::mpw::proto("invalid co-located read slot"));
        }
        let nkeys = rd.array()?;
        crate::budget::decoded(0, nkeys, r.parts.len())?;
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
        crate::budget::decoded(0, nrows, r.table.columns.len())?;
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
    if complete {
        crate::observe::storage_rows(out.iter().map(|s| s.rows.len()).sum());
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
    crate::budget::decoded(d.len(), n, t.columns.len())?;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push(rd.row_of(t)?);
    }
    let next = if rd.buf.is_empty() || rd.is_nil() {
        None
    } else {
        Some(rd.raw_value()?.to_vec())
    };
    crate::observe::storage_rows(out.len());
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
    native_batch: bool,
) -> SqlResult<Page> {
    crate::observe::read(route);
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
    let full_primary = t
        .indexes
        .first()
        .is_some_and(|pk| pk.id == 0 && pk.unique && keys.iter().all(|key| key.len() == pk.cols.len()));
    if native_batch
        && stream == 0
        && lock_check
        && !begin
        && index == 0
        && matches!(t.engine, crate::catalog::Engine::Memtx)
        && after.is_none()
        && limit != Some(0)
        && (1..=128).contains(&keys.len())
        && full_primary
        && native_batch_available(client).await?
    {
        let (reply, _wire) = crate::budget::call_read(client, 0, "tarn_storage.ts_select", &m.0).await?;
        match reply {
            Ok(r) => {
                let d = r.data().ok_or_else(|| crate::mpw::proto("no native batch data"))?;
                if let Some(page) = native_batch_reply(&d, t, keys.len())? {
                    crate::observe::storage_rows(page.0.len());
                    return Ok(page);
                }
            }
            Err(tarn_iproto::Error::Server { code, message }) if missing_native_read(code) => {
                tracing::debug!("native batch reads unavailable on this shard: {}", message);
                client.mark(MARK_NO_NATIVE_SELECT);
                client.mark(MARK_NO_NATIVE_BATCH);
            }
            Err(e) => return Err(from_iproto_read(e)),
        }
    }
    let (result, _wire) = crate::budget::call_read(client, stream, "tarn_select_keys", &m.0).await?;
    let r = result.map_err(from_iproto_read)?;
    let d = r.data().ok_or_else(|| crate::mpw::proto("no data"))?;
    page_of(&d, t)
}

fn missing_native_read(code: u32) -> bool {
    use tarn_iproto::errcode::{
        ER_ACCESS_DENIED, ER_LOAD_FUNCTION, ER_LOAD_MODULE, ER_NO_SUCH_FUNCTION, ER_NO_SUCH_PROC,
    };
    matches!(
        code,
        ER_NO_SUCH_PROC | ER_NO_SUCH_FUNCTION | ER_LOAD_FUNCTION | ER_LOAD_MODULE | ER_ACCESS_DENIED
    )
}

/// Capability is cached separately from per-request handback. An older
/// module rejects this empty argument list before reading any user state;
/// its ordinary twelve-argument native reads remain available.
async fn native_batch_available(client: &Client) -> SqlResult<bool> {
    if client.marked(MARK_NO_NATIVE_SELECT) || client.marked(MARK_NO_NATIVE_BATCH) {
        return Ok(false);
    }
    if client.marked(MARK_NATIVE_BATCH) {
        return Ok(true);
    }
    let (result, _wire) = crate::budget::call_read(client, 0, "tarn_storage.ts_select", &[0x90]).await?;
    let reply = match result {
        Ok(reply) => reply,
        Err(tarn_iproto::Error::Server { code, .. }) if missing_native_read(code) => {
            client.mark(MARK_NO_NATIVE_SELECT);
            client.mark(MARK_NO_NATIVE_BATCH);
            return Ok(false);
        }
        Err(e) => return Err(from_iproto_read(e)),
    };
    let data = reply
        .data()
        .ok_or_else(|| crate::mpw::proto("no native batch capability data"))?;
    let supported = native_batch_capability(&data)?;
    client.mark(if supported {
        MARK_NATIVE_BATCH
    } else {
        MARK_NO_NATIVE_BATCH
    });
    Ok(supported)
}

fn native_batch_capability(data: &[u8]) -> SqlResult<bool> {
    let mut rd = Rd::new(data);
    if rd.array()? != 1 {
        return Err(crate::mpw::proto("invalid native batch capability result"));
    }
    let fields = rd.array()?;
    let status = rd.string()?;
    let supported = match (fields, status.as_str()) {
        (1, "lua") => false,
        (2, "native_batch_keys") => match rd.value()? {
            Value::UInt(version) => version == 1,
            Value::Int(version) if version >= 0 => version == 1,
            _ => return Err(crate::mpw::proto("invalid native batch capability version")),
        },
        _ => return Err(crate::mpw::proto("invalid native batch capability status")),
    };
    if !rd.buf.is_empty() {
        return Err(crate::mpw::proto("trailing native batch capability data"));
    }
    Ok(supported)
}

/// Unlike Lua's general pager, a served batch must be complete in one reply.
/// Only an exact handback permits calling Lua; malformed replies are errors.
fn native_batch_reply(data: &Bytes, t: &TableDef, requested: usize) -> SqlResult<Option<Page>> {
    let mut rd = Rd::shared(data);
    if rd.array()? != 1 {
        return Err(crate::mpw::proto("invalid native batch envelope"));
    }
    let fields = rd.array()?;
    let status = rd.string()?;
    let page = match (fields, status.as_str()) {
        (1, "lua") => None,
        (2, "ok") => {
            let n = rd.array()?;
            if n > requested {
                return Err(crate::mpw::proto("oversized native key batch"));
            }
            crate::budget::decoded(data.len(), n, t.columns.len())?;
            let mut rows = Vec::with_capacity(n);
            for _ in 0..n {
                rows.push(rd.row_of(t)?);
            }
            Some((rows, None))
        }
        _ => return Err(crate::mpw::proto("invalid native batch status or completion")),
    };
    if !rd.buf.is_empty() {
        return Err(crate::mpw::proto("trailing native batch data"));
    }
    Ok(page)
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
    crate::observe::read(route);
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
    let (result, _wire) = crate::budget::call_read(client, stream, "tarn_count", &m.0).await?;
    let r = result.map_err(from_iproto_read)?;
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
    crate::observe::read(route);
    // Unbounded point/prefix lookups inside a running stream transaction use
    // plain IPROTO SELECT; a full page goes on after its last tuple (which
    // tarn_select takes as a position too). (Nothing checks their bucket
    // here: the transaction's validation does, as they are in its read set.)
    let mut raw_space_missing = false;
    if end.is_none() && !lock_check && !begin && after.is_none() {
        let mut k = Mp::new();
        k.vals(key);
        let n = limit.unwrap_or(u32::MAX).min(PAGE_ROWS);
        let (reply, _wire) = crate::budget::select_read(client, stream, t.space_id, index, iter, 0, n, &k.0).await?;
        match reply {
            Ok(r) => {
                let data = r.data().ok_or_else(|| crate::mpw::proto("no SELECT data"))?;
                let mut rd = Rd::new(&data);
                let count = rd.array()?;
                crate::budget::decoded(data.len(), count, t.columns.len())?;
                let mut out = Vec::with_capacity(count);
                let mut last = None;
                for _ in 0..count {
                    let tuple = rd.raw_value()?;
                    out.push(Rd::shared_at(&data, Rd::new(tuple)).row_of(t)?);
                    last = Some(tuple);
                }
                let next = if count == n as usize && Some(n) != limit {
                    last.map(|r| r.to_vec())
                } else {
                    None
                };
                crate::observe::storage_rows(out.len());
                return Ok((out, next));
            }
            Err(tarn_iproto::Error::Server {
                code: tarn_iproto::errcode::ER_NO_SUCH_SPACE,
                ..
            }) => {
                // A physical engine replacement can retire this cached SID.
                // Lua distinguishes that identity (1412) from a genuinely
                // missing table (1146). Reuse the same read stream and
                // arguments; never restart its transaction or replay a
                // mutation/transport failure. Commit still validates the
                // stream and refuses one whose transaction was lost.
                tracing::debug!(
                    space_id = t.space_id,
                    index,
                    stream,
                    "raw SELECT found no space; checking the same read through Lua"
                );
                raw_space_missing = true;
            }
            Err(e) => return Err(from_iproto_read(e)),
        }
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
    // The shard's native procedure serves the reads it can decide without
    // Lua (crates/tarn-storage/src/select.rs), also in a stream's
    // transaction, whose first read begins it, and hands the others back
    // ("lua"); a shard without it is noted on the connection. (Reads it
    // always hands back are not sent to it: vinyl tables, and the pages
    // after the first.)
    if !raw_space_missing
        && matches!(t.engine, crate::catalog::Engine::Memtx)
        && after.is_none()
        && limit != Some(0)
        && !client.marked(MARK_NO_NATIVE_SELECT)
    {
        use tarn_iproto::errcode::{
            ER_ACCESS_DENIED, ER_LOAD_FUNCTION, ER_LOAD_MODULE, ER_NO_SUCH_FUNCTION, ER_NO_SUCH_PROC,
        };
        let (reply, _wire) = crate::budget::call_read(client, stream, "tarn_storage.ts_select", &m.0).await?;
        match reply {
            Ok(r) => {
                let d = r.data().ok_or_else(|| crate::mpw::proto("no data"))?;
                if !crate::mpw::status_is(&d, "lua") {
                    return page_of(&d, t);
                }
            }
            Err(tarn_iproto::Error::Server {
                code: ER_NO_SUCH_PROC | ER_NO_SUCH_FUNCTION | ER_LOAD_FUNCTION | ER_LOAD_MODULE | ER_ACCESS_DENIED,
                message,
            }) => {
                tracing::debug!("native reads unavailable on this shard: {}", message);
                client.mark(MARK_NO_NATIVE_SELECT);
            }
            Err(e) => return Err(from_iproto_read(e)),
        }
    }
    let (result, _wire) = crate::budget::call_read(client, stream, "tarn_select", &m.0).await?;
    let r = result.map_err(from_iproto_read)?;
    let d = r.data().ok_or_else(|| crate::mpw::proto("no data"))?;
    page_of(&d, t)
}

/// The mark (`Client::mark`) of a connection to a shard without the native
/// read procedure.
const MARK_NO_NATIVE_SELECT: u32 = 0;

/// The mark of a connection to a shard without the native commit procedure
/// (session.rs has 1, for the native autocommit procedure).
const MARK_NO_NATIVE_COMMIT: u32 = 2;

/// The mark of a connection to a shard without the native validation
/// procedure.
const MARK_NO_NATIVE_VALIDATE: u32 = 3;

const MARK_NATIVE_BATCH: u32 = 4;
const MARK_NO_NATIVE_BATCH: u32 = 5;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prepare_uncertainty_is_never_a_no_vote() {
        for status in ["expired", "clock_unknown", "future_status", "error"] {
            let mut m = Mp::new();
            m.arr(1).arr(3).str(status).uint(1213).str("freshness unavailable");
            let (vote, error) = prepare_reply(&m.0);
            assert!(matches!(vote, Vote::Unknown), "{status}");
            assert!(error.is_some());
            assert!(matches!(coord::decide(&[Vote::Yes, vote]), Decision::Unknown));
        }
        for status in ["no", "moved"] {
            let mut m = Mp::new();
            m.arr(1).arr(3).str(status).uint(1213).str("confirmed refusal");
            let (vote, error) = prepare_reply(&m.0);
            assert!(matches!(vote, Vote::No));
            assert!(error.is_some());
            assert!(matches!(coord::decide(&[Vote::Yes, vote]), Decision::Abort));
        }
    }

    #[test]
    fn prepare_votes_require_exact_complete_envelopes() {
        let mut yes = Mp::new();
        yes.arr(1).arr(1).str("yes");
        assert!(matches!(prepare_reply(&yes.0).0, Vote::Yes));
        for n in 0..yes.0.len() {
            assert!(matches!(prepare_reply(&yes.0[..n]).0, Vote::Unknown));
        }
        yes.nil();
        assert!(matches!(prepare_reply(&yes.0).0, Vote::Unknown));
        let mut no = Mp::new();
        no.arr(1).arr(4).str("no").uint(1213).str("refusal").nil();
        assert!(matches!(prepare_reply(&no.0).0, Vote::Unknown));
        let mut ok = Mp::new();
        ok.arr(1).arr(1).str("ok");
        assert!(matches!(prepare_reply(&ok.0).0, Vote::Unknown));
    }

    #[tokio::test]
    async fn prepare_only_cleans_owned_stream_after_complete_explicit_no_vote() {
        use crate::cluster::wire_test::{accept, reply, request};
        use tokio::io::AsyncReadExt;
        use tokio::net::TcpListener;
        use tokio::time::{timeout, Duration};

        // expired, clock_unknown, YES, NO, malformed, future, lost reply,
        // failed cleanup, timed-out cleanup, no owned stream, server error,
        // and a stream that storage already ended.
        for case in 0..12 {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let stream = if case == 9 { 0 } else { 73 };
            let peer = tokio::spawn(async move {
                let mut sock = accept(&listener).await;
                let (kind, sync, seen_stream, body) = request(&mut sock).await;
                assert_eq!((kind, seen_stream), (10, stream));
                let mut rd = Rd::new(&body);
                assert_eq!(rd.map().unwrap(), 2);
                assert_eq!(rd.value().unwrap().as_i64(), Some(0x22));
                assert_eq!(rd.string().unwrap(), "tarn_prepare");
                assert_eq!(rd.value().unwrap().as_i64(), Some(0x21));
                assert_eq!(rd.array().unwrap(), 0);
                assert!(rd.buf.is_empty());
                if case == 6 {
                    return 1; // a lost PREPARE reply cannot authorize ROLLBACK
                }
                let mut body = Mp::new();
                let code = if case == 10 {
                    body.map(1).uint(0x31).str("controlled server error");
                    0x8000 | 7
                } else {
                    body.map(1).uint(0x30).arr(1);
                    match case {
                        2 => {
                            body.arr(1).str("yes");
                        }
                        3 => {
                            body.arr(3).str("no").uint(1213).str("confirmed refusal");
                        }
                        4 => {
                            body.arr(3).str("expired").uint(1213).uint(1);
                        }
                        5 => {
                            body.arr(3).str("future_status").uint(1213).str("unknown status");
                        }
                        _ => {
                            body.arr(3)
                                .str(if case == 1 { "clock_unknown" } else { "expired" })
                                .uint(1213)
                                .str("no vote");
                        }
                    }
                    0
                };
                reply(&mut sock, sync, code, &body.0).await;
                let cleanup = matches!(case, 0 | 1 | 7 | 8 | 11);
                if cleanup {
                    let (kind, sync, seen_stream, body) = request(&mut sock).await;
                    assert_eq!((kind, seen_stream), (16, stream));
                    assert!(body.is_empty());
                    if case != 8 {
                        let mut body = Mp::new();
                        let code = if case == 7 || case == 11 {
                            body.map(1).uint(0x31).str("cleanup refused");
                            0x8000
                                | if case == 11 {
                                    tarn_iproto::errcode::ER_NO_TRANSACTION as u64
                                } else {
                                    97
                                }
                        } else {
                            body.map(0);
                            0
                        };
                        reply(&mut sock, sync, code, &body.0).await;
                    }
                }
                // No replay of PREPARE, and no ROLLBACK after uncertainty.
                assert!(timeout(Duration::from_secs(3), sock.read_u8()).await.unwrap().is_err());
                if cleanup {
                    2
                } else {
                    1
                }
            });
            let client = Client::connect(&tarn_iproto::Opts::new(addr.to_string()))
                .await
                .unwrap();
            let (vote, error) = timeout(Duration::from_secs(3), prepare_once(&client, stream, &[0x90]))
                .await
                .unwrap();
            assert_eq!(matches!(vote, Vote::Yes), case == 2);
            assert_eq!(matches!(vote, Vote::No), case == 3);
            assert_eq!(error.is_none(), case == 2);
            if matches!(case, 6..=8) {
                assert!(client.is_closed(), "case {case}");
            } else if matches!(case, 0 | 1 | 11) {
                assert!(!client.is_closed(), "confirmed cleanup must keep a healthy socket");
            }
            client.close();
            let count = timeout(Duration::from_secs(4), peer).await.unwrap().unwrap();
            assert_eq!(count, if matches!(case, 0 | 1 | 7 | 8 | 11) { 2 } else { 1 });
        }
    }

    #[test]
    fn batch_capability_is_versioned_and_complete() {
        for version in [0, 1, 2] {
            let mut m = Mp::new();
            m.arr(1).arr(2).str("native_batch_keys").uint(version);
            assert_eq!(native_batch_capability(&m.0).unwrap(), version == 1);
            m.nil();
            assert!(native_batch_capability(&m.0).is_err());
        }
        let mut old = Mp::new();
        old.arr(1).arr(1).str("lua");
        assert!(!native_batch_capability(&old.0).unwrap());
        let mut bad = Mp::new();
        bad.arr(1).arr(2).str("native_batch_keys").str("1");
        assert!(native_batch_capability(&bad.0).is_err());
    }

    #[test]
    fn native_key_batch_reply_preserves_physical_rows_and_requires_completion() {
        let mut table = join_table();
        table.layout.identity = false;
        table.layout.col_field.swap(0, 1);
        table.layout.field_col[0] = Some(1);
        table.layout.field_col[1] = Some(0);
        table.layout.fills[2] = Value::str("filled");
        let mut m = Mp::new();
        m.arr(1).arr(2).str("ok").arr(2);
        for _ in 0..2 {
            m.vals(&[Value::Int(7), Value::str("tenant")]);
        }
        let data = m.bytes();
        let page = native_batch_reply(&data, &table, 3).unwrap().unwrap();
        assert_eq!(
            page.0,
            vec![vec![Value::str("tenant"), Value::Int(7), Value::str("filled")]; 2]
        );
        assert!(page.1.is_none());
        assert!(native_batch_reply(&data, &table, 1).is_err());
        for end in 0..data.len() {
            assert!(native_batch_reply(&data.slice(..end), &table, 3).is_err());
        }
        let mut trailing = data.to_vec();
        trailing.push(0xc0);
        assert!(native_batch_reply(&Bytes::from(trailing), &table, 3).is_err());
        let mut handback = Mp::new();
        handback.arr(1).arr(1).str("lua");
        assert!(native_batch_reply(&handback.bytes(), &table, 3).unwrap().is_none());
    }

    #[tokio::test]
    async fn native_key_batch_negotiates_once_and_preserves_fallback_calls() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::{TcpListener, TcpStream};
        use tokio::time::{timeout, Duration};

        async fn request(sock: &mut TcpStream) -> (u64, u64, u64, Vec<u8>) {
            assert_eq!(sock.read_u8().await.unwrap(), 0xce);
            let size = sock.read_u32().await.unwrap();
            assert!(size < 4096);
            let mut packet = vec![0; size as usize];
            sock.read_exact(&mut packet).await.unwrap();
            let mut rd = Rd::new(&packet);
            let (mut kind, mut sync, mut stream) = (0, 0, 0);
            for _ in 0..rd.map().unwrap() {
                let key = rd.value().unwrap().as_i64().unwrap();
                let value = rd.value().unwrap().as_i64().unwrap() as u64;
                match key {
                    0 => kind = value,
                    1 => sync = value,
                    10 => stream = value,
                    _ => {}
                }
            }
            (kind, sync, stream, rd.buf.to_vec())
        }
        async fn reply(sock: &mut TcpStream, sync: u64, code: u64, status: Option<(&str, Option<u64>)>) {
            let mut packet = Mp::new();
            packet.map(2).uint(0).uint(code).uint(1).uint(sync);
            if code == 0 {
                let (status, version) = status.unwrap();
                packet.map(1).uint(0x30).arr(1);
                match (status, version) {
                    ("rows", _) => {
                        packet.arr(2).str("ok").arr(2);
                        for _ in 0..2 {
                            packet.vals(&[Value::str("tenant"), Value::Int(7), Value::str("label"), Value::UInt(1)]);
                        }
                    }
                    ("bad-page", _) => {
                        packet.arr(4).str("ok").arr(0).nil().bool(false);
                    }
                    ("yes", _) => {
                        packet.arr(2).str("yes").arr(0);
                    }
                    ("bad-lua", _) => {
                        packet.arr(2).str("lua").nil();
                    }
                    ("large-page", _) => {
                        packet.arr(2).str("ok").arr(4);
                        for _ in 0..4 {
                            packet.vals(&[Value::str("tenant"), Value::Int(7), Value::str("label"), Value::UInt(1)]);
                        }
                    }
                    ("ok", _) => {
                        packet.arr(2).str(status).arr(0);
                    }
                    (_, Some(version)) => {
                        packet.arr(2).str(status).uint(version);
                    }
                    (_, None) => {
                        packet.arr(1).str(status);
                    }
                }
            } else {
                packet.map(1).uint(0x31).str("controlled batch error");
            }
            sock.write_u8(0xce).await.unwrap();
            sock.write_u32(packet.0.len() as u32).await.unwrap();
            sock.write_all(&packet.0).await.unwrap();
        }
        fn call(body: &[u8]) -> (String, Vec<u8>) {
            let mut rd = Rd::new(body);
            let (mut name, mut args) = (None, None);
            for _ in 0..rd.map().unwrap() {
                match rd.value().unwrap().as_i64().unwrap() {
                    0x22 => name = Some(rd.string().unwrap()),
                    0x21 => args = Some(rd.raw_value().unwrap().to_vec()),
                    other => panic!("unexpected CALL field {other}"),
                }
            }
            assert!(rd.buf.is_empty());
            (name.unwrap(), args.unwrap())
        }
        fn batch_args(args: &[u8], in_stream: bool, prefix: bool) {
            let mut rd = Rd::new(args);
            assert_eq!(rd.array().unwrap(), 8);
            assert_eq!(rd.value().unwrap().as_i64(), Some(512));
            assert_eq!(rd.value().unwrap().as_i64(), Some(0));
            assert_eq!(rd.array().unwrap(), 3);
            for id in [7, 99, 7] {
                assert_eq!(rd.array().unwrap(), if prefix { 1 } else { 2 });
                assert_eq!(rd.string().unwrap(), "tenant");
                if !prefix {
                    assert_eq!(rd.value().unwrap().as_i64(), Some(id));
                }
            }
            assert_eq!(rd.raw_value().unwrap(), &[0xc0]); // per-key limit
            assert_eq!(rd.raw_value().unwrap(), &[if in_stream { 0xc2 } else { 0xc3 }]);
            assert_eq!(rd.raw_value().unwrap(), &[0xc2]);
            assert_eq!(rd.raw_value().unwrap(), &[0xc0]); // continuation
            assert_eq!(rd.array().unwrap(), 3);
            for _ in 0..3 {
                assert_eq!(rd.value().unwrap().as_i64(), Some(1));
            }
            assert!(rd.buf.is_empty());
        }

        // 0: supported, 1: older module, 2: per-request handback,
        // 3: native data error, 4: switch disabled, 5: missing module,
        // 6: existing stream, 7: primary-key prefixes; 8..12: malformed
        // completion/status/handback/row-count replies must never call Lua.
        for case in 0..12 {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let peer = tokio::spawn(async move {
                let (mut sock, _) = listener.accept().await.unwrap();
                let mut greeting = [b' '; 128];
                let first = b"Tarantool 3.9.0 (Binary)";
                greeting[..first.len()].copy_from_slice(first);
                greeting[63] = b'\n';
                greeting[64..108].copy_from_slice(b"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=");
                greeting[127] = b'\n';
                sock.write_all(&greeting).await.unwrap();
                let (kind, sync, _, _) = request(&mut sock).await;
                assert_eq!(kind, 73);
                let mut id = Mp::new();
                id.map(2).uint(0x54).uint(6).uint(0x55).arr(2).uint(0).uint(1);
                let mut packet = Mp::new();
                packet.map(2).uint(0).uint(0).uint(1).uint(sync).raw(&id.0);
                sock.write_u8(0xce).await.unwrap();
                sock.write_u32(packet.0.len() as u32).await.unwrap();
                sock.write_all(&packet.0).await.unwrap();
                let mut names = Vec::new();
                if !matches!(case, 4 | 6 | 7) {
                    let (kind, sync, stream, body) = request(&mut sock).await;
                    assert_eq!((kind, stream), (10, 0));
                    let (name, args) = call(&body);
                    assert_eq!(name, "tarn_storage.ts_select");
                    assert_eq!(args, [0x90]);
                    names.push("caps");
                    if case == 5 {
                        reply(
                            &mut sock,
                            sync,
                            0x8000 | tarn_iproto::errcode::ER_NO_SUCH_PROC as u64,
                            None,
                        )
                        .await;
                    } else {
                        let cap = if case == 1 {
                            ("lua", None)
                        } else {
                            ("native_batch_keys", Some(1))
                        };
                        reply(&mut sock, sync, 0, Some(cap)).await;
                    }
                }
                for _ in 0..2 {
                    let (kind, sync, stream, body) = request(&mut sock).await;
                    assert_eq!((kind, stream), (10, if case == 6 { 73 } else { 0 }));
                    let (name, args) = call(&body);
                    batch_args(&args, case == 6, case == 7);
                    if matches!(case, 1 | 4 | 5 | 6 | 7) {
                        assert_eq!(name, "tarn_select_keys");
                        names.push("lua");
                        reply(&mut sock, sync, 0, Some(("ok", None))).await;
                    } else {
                        assert_eq!(name, "tarn_storage.ts_select");
                        names.push("native");
                        if case >= 8 {
                            let status = ["bad-page", "yes", "bad-lua", "large-page"][case - 8];
                            reply(&mut sock, sync, 0, Some((status, None))).await;
                        } else if case == 3 {
                            reply(&mut sock, sync, 0x8000 | 35, None).await;
                        } else if case == 2 {
                            reply(&mut sock, sync, 0, Some(("lua", None))).await;
                            let (kind, sync, stream, body) = request(&mut sock).await;
                            assert_eq!((kind, stream), (10, 0));
                            let (name, fallback_args) = call(&body);
                            assert_eq!(name, "tarn_select_keys");
                            assert_eq!(fallback_args, args);
                            names.push("lua");
                            reply(&mut sock, sync, 0, Some(("ok", None))).await;
                        } else {
                            reply(&mut sock, sync, 0, Some(("rows", None))).await;
                        }
                    }
                }
                assert!(sock.read_u8().await.is_err());
                names
            });
            let client = Client::connect(&tarn_iproto::Opts::new(addr.to_string()))
                .await
                .unwrap();
            let table = join_table();
            let keys: Vec<_> = [7, 99, 7]
                .into_iter()
                .map(|id| {
                    if case == 7 {
                        vec![Value::str("tenant")]
                    } else {
                        vec![Value::str("tenant"), Value::Int(id)]
                    }
                })
                .collect();
            let metrics = crate::observe::Metrics::default();
            for _ in 0..2 {
                let result = timeout(
                    Duration::from_secs(3),
                    crate::observe::track(&metrics, async {
                        crate::observe::describe(crate::observe::Descriptor::new(None, "SELECT native_batch"));
                        select_keys(
                            &client,
                            if case == 6 { 73 } else { 0 },
                            &table,
                            0,
                            &keys,
                            None,
                            case != 6,
                            false,
                            None,
                            &Route::Buckets(vec![1, 1, 1]),
                            case != 4,
                        )
                        .await
                    }),
                )
                .await
                .unwrap();
                if case == 3 || case >= 8 {
                    assert!(result.is_err());
                } else {
                    let page = result.unwrap();
                    assert_eq!(page.0.len(), if case == 0 { 2 } else { 0 });
                    assert!(page.1.is_none());
                }
            }
            assert_eq!(
                metrics.rows()[0].last(),
                Some(&Value::UInt(if case == 0 { 4 } else { 0 }))
            );
            if case == 1 {
                assert!(!client.marked(MARK_NO_NATIVE_SELECT));
            }
            client.close();
            let names = timeout(Duration::from_secs(3), peer).await.unwrap().unwrap();
            let expected = match case {
                0 | 3 | 8..=11 => vec!["caps", "native", "native"],
                1 | 5 => vec!["caps", "lua", "lua"],
                2 => vec!["caps", "native", "lua", "native", "lua"],
                4 | 6 | 7 => vec!["lua", "lua"],
                _ => unreachable!(),
            };
            assert_eq!(names, expected);
        }
    }

    #[tokio::test]
    async fn raw_missing_space_read_keeps_its_stream_and_only_falls_back_for_error_36() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::{TcpListener, TcpStream};
        use tokio::time::{timeout, Duration};

        async fn request(sock: &mut TcpStream) -> (u64, u64, u64, Vec<u8>) {
            assert_eq!(sock.read_u8().await.unwrap(), 0xce);
            let len = sock.read_u32().await.unwrap();
            assert!(len < 4096);
            let mut packet = vec![0; len as usize];
            sock.read_exact(&mut packet).await.unwrap();
            let mut rd = Rd::new(&packet);
            let (mut kind, mut sync, mut stream) = (0, 0, 0);
            for _ in 0..rd.map().unwrap() {
                let key = rd.value().unwrap().as_i64().unwrap();
                let value = match rd.value().unwrap() {
                    Value::UInt(n) => n,
                    Value::Int(n) if n >= 0 => n as u64,
                    other => panic!("unexpected numeric header: {other:?}"),
                };
                match key {
                    0 => kind = value,
                    1 => sync = value,
                    10 => stream = value,
                    _ => {}
                }
            }
            (kind, sync, stream, rd.buf.to_vec())
        }
        async fn reply(sock: &mut TcpStream, sync: u64, code: u64, body: &[u8]) {
            let mut packet = Mp::new();
            packet.map(2).uint(0).uint(code).uint(1).uint(sync);
            packet.0.extend_from_slice(body);
            sock.write_u8(0xce).await.unwrap();
            sock.write_u32(packet.0.len() as u32).await.unwrap();
            sock.write_all(&packet.0).await.unwrap();
        }
        // None closes the transport. Zero is a healthy SELECT; 35 is a
        // different server error. Neither may produce a second read RPC.
        for code in [None, Some(0u64), Some(35), Some(36)] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let peer = tokio::spawn(async move {
                let (mut sock, _) = listener.accept().await.unwrap();
                let mut greeting = [b' '; 128];
                let first = b"Tarantool 3.9.0 (Binary)";
                greeting[..first.len()].copy_from_slice(first);
                greeting[63] = b'\n';
                greeting[64..108].copy_from_slice(b"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=");
                greeting[127] = b'\n';
                sock.write_all(&greeting).await.unwrap();
                let (kind, sync, _, _) = request(&mut sock).await;
                assert_eq!(kind, 73); // IPROTO_ID
                let mut id = Mp::new();
                id.map(2).uint(0x54).uint(6).uint(0x55).arr(2).uint(0).uint(1);
                reply(&mut sock, sync, 0, &id.0).await;
                let (kind, sync, stream, _) = request(&mut sock).await;
                assert_eq!((kind, stream), (1, 73)); // SELECT in the existing stream
                let mut kinds = vec![kind];
                let Some(code) = code else { return kinds };
                let mut body = Mp::new();
                if code == 0 {
                    body.map(1).uint(0x30).arr(0);
                } else {
                    body.map(1).uint(0x31).str("controlled read error");
                }
                reply(&mut sock, sync, if code == 0 { 0 } else { 0x8000 | code }, &body.0).await;
                if code == 36 {
                    let (kind, sync, fallback_stream, body) = request(&mut sock).await;
                    assert_eq!((kind, fallback_stream), (10, stream)); // CALL, not BEGIN or a write
                    kinds.push(kind);
                    let mut rd = Rd::new(&body);
                    let mut saw_name = false;
                    let mut saw_args = false;
                    let mut expected = Mp::new();
                    expected
                        .arr(12)
                        .uint(10030)
                        .uint(0)
                        .arr(1)
                        .uint(7)
                        .uint(0)
                        .uint(1)
                        .nil()
                        .bool(true)
                        .bool(false)
                        .nil()
                        .bool(false)
                        .nil()
                        .nil();
                    for _ in 0..rd.map().unwrap() {
                        match rd.value().unwrap().as_i64().unwrap() {
                            0x22 => {
                                assert_eq!(rd.string().unwrap(), "tarn_select");
                                saw_name = true;
                            }
                            0x21 => {
                                assert_eq!(rd.raw_value().unwrap(), expected.0);
                                saw_args = true;
                            }
                            other => panic!("unexpected CALL field {other}"),
                        }
                    }
                    assert!(saw_name && saw_args && rd.buf.is_empty());
                    let mut body = Mp::new();
                    body.map(1)
                        .uint(0x30)
                        .arr(1)
                        .arr(3)
                        .str("error")
                        .uint(1412)
                        .str("retired identity");
                    reply(&mut sock, sync, 0, &body.0).await;
                }
                // Closing the client must reveal no unexpected extra RPC.
                assert!(sock.read_u8().await.is_err());
                kinds
            });
            let client = Client::connect(&tarn_iproto::Opts::new(addr.to_string()))
                .await
                .unwrap();
            let mut table = join_table();
            table.space_id = 10030;
            let result = timeout(
                Duration::from_secs(3),
                select_rows(
                    &client,
                    73,
                    &table,
                    0,
                    &[Value::Int(7)],
                    Iter::Eq,
                    Some(1),
                    None,
                    false,
                    false,
                    None,
                    &Route::None,
                ),
            )
            .await
            .unwrap();
            match code {
                Some(36) => assert_eq!(result.unwrap_err().code, 1412),
                Some(0) => assert!(result.unwrap().0.is_empty()),
                _ => assert!(result.is_err()),
            }
            client.close();
            let kinds = timeout(Duration::from_secs(3), peer).await.unwrap().unwrap();
            assert_eq!(kinds, if code == Some(36) { vec![1, 10] } else { vec![1] });
        }
    }

    fn empty_txn() -> Txn {
        Txn {
            resources: None,
            cleanup: None,
            id: "test/1".into(),
            ts: 0.0,
            explicit: true,
            mode: Mode::Serializable,
            lock_timeout: 0.0,
            plocks: BTreeMap::new(),
            lock_shards: BTreeSet::new(),
            savepoints: Vec::new(),
            streams: BTreeMap::new(),
            writes: BTreeMap::new(),
            rpoints: BTreeMap::new(),
            rranges: BTreeMap::new(),
            defs: BTreeMap::new(),
            doomed: None,
            mver: 0,
        }
    }

    #[test]
    fn successful_commit_unlocks_only_extra_lock_requests() {
        let mut txn = empty_txn();
        // Participants 1 and 3 may still hold prepared locks awaiting APPLY.
        // A refused request on shard 2 must still release its row-lock state.
        txn.lock_shards.extend([1, 2, 3]);
        let parts = BTreeSet::from([1, 3]);
        assert_eq!(txn.take_extra_lock_shards(&parts), vec![2]);
        assert!(txn.lock_shards.is_empty());
        assert!(txn.take_lock_shards().is_empty());

        txn.lock_shards.insert(2);
        assert_eq!(txn.take_extra_lock_shards(&BTreeSet::new()), vec![2]);
    }

    #[test]
    fn native_commit_excludes_lazy_writes_and_lock_requests() {
        let mut txn = empty_txn();
        let table = Arc::new(join_table());
        let key = (table.space_id, key_bytes(&[Value::str("tenant"), Value::Int(4)]));
        let write = BufWrite {
            shard: 1,
            table: table.clone(),
            op: WOp::Insert,
            row: Some(vec![Value::str("tenant"), Value::Int(4), Value::str("label")]),
            pk: vec![Value::str("tenant"), Value::Int(4)],
        };
        txn.writes.insert(key.clone(), write);
        assert!(txn.native_commit_eligible(1));
        txn.lock_shards.insert(2);
        assert!(txn.native_commit_eligible(1));
        txn.lock_shards.insert(1);
        assert!(!txn.native_commit_eligible(1));
        txn.lock_shards.clear();

        let mut lazy_table = (*table).clone();
        let mut index = lazy_table.indexes[0].clone();
        index.name = "by_label".into();
        index.id = 1;
        index.cols = vec![2];
        index.unique = false;
        index.lazy = Some(crate::catalog::LazyDef {
            space_id: 0,
            route_parts: 1,
            ready: false,
        });
        lazy_table.indexes.push(index);
        txn.writes.get_mut(&key).unwrap().table = Arc::new(lazy_table.clone());
        assert!(txn.native_commit_eligible(1), "unactivated metadata needs no capture");
        lazy_table.indexes[1].lazy.as_mut().unwrap().space_id = 513;
        txn.writes.get_mut(&key).unwrap().table = Arc::new(lazy_table);
        for op in [WOp::Insert, WOp::Replace, WOp::Delete] {
            txn.writes.get_mut(&key).unwrap().op = op;
            assert!(!txn.native_commit_eligible(1), "every lazy write needs Lua capture");
        }
        assert!(txn.native_commit_eligible(2), "another shard's writes are not local");

        let mut vinyl_table = (*table).clone();
        vinyl_table.engine = crate::catalog::Engine::Vinyl;
        txn.writes.get_mut(&key).unwrap().table = Arc::new(vinyl_table);
        assert!(!txn.native_commit_eligible(1));
    }

    #[test]
    fn apply_acknowledgement_must_certify_physical_completion() {
        let reply = |status: &str, detail: &str| {
            let mut m = Mp::new();
            m.arr(1).arr(2).str(status).str(detail);
            m.0
        };
        for terminal in ["applied", "none"] {
            assert!(applied_reply(&reply("ok", terminal)).is_ok());
        }
        // A YES vote is a durable promise, not an applied row/outbox.
        for (status, detail) in [("yes", "applied"), ("ok", "prepared"), ("ok", "staging")] {
            assert!(applied_reply(&reply(status, detail)).is_err());
        }
        let mut missing = Mp::new();
        missing.arr(1).arr(1).str("ok");
        assert!(applied_reply(&missing.0).is_err());
        let mut extra_return = Mp::new();
        extra_return.arr(2).arr(2).str("ok").str("applied").nil();
        assert!(applied_reply(&extra_return.0).is_err());
        let mut extra_field = Mp::new();
        extra_field.arr(1).arr(3).str("ok").str("applied").nil();
        assert!(applied_reply(&extra_field.0).is_err());
        let mut trailing = reply("ok", "applied");
        trailing.push(0xc0);
        assert!(applied_reply(&trailing).is_err());
        let mut binary_status = Mp::new();
        binary_status
            .arr(1)
            .arr(2)
            .val(&Value::Bin(Bytes::from_static(b"ok")))
            .str("applied");
        assert!(applied_reply(&binary_status.0).is_err());
        let mut truncated = reply("ok", "applied");
        truncated.pop();
        assert!(applied_reply(&truncated).is_err());
        let mut error = Mp::new();
        error.arr(1).arr(3).str("error").uint(1213).str("retry");
        assert_eq!(applied_reply(&error.0).unwrap_err().code, 1213);
    }

    fn join_table() -> TableDef {
        use crate::catalog::Catalog;
        use crate::plan::Stmt;
        use crate::planner::{plan, PlanCtx};

        let catalog = Catalog::default();
        let cx = PlanCtx {
            statistics: None,
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
