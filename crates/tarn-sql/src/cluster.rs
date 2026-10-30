//! Cluster topology, connections to shard leaders, routing, catalog cache,
//! DDL broadcast and AUTO_INCREMENT allocation. One `Cluster` is shared by
//! every client session of a Tarn server.

use crate::buckets::BucketMap;
use crate::catalog::{self, Catalog, TableDef};
use crate::mpw::{from_iproto, lua_status, Mp, Rd};
use crate::route;
use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use serde::Deserialize;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tarn_common::{SqlError, SqlResult, Value};
use tarn_iproto::{Client, Opts};

fn default_user() -> String {
    "tarn".into()
}
fn default_pool() -> usize {
    4
}
fn default_max_txn_age() -> f64 {
    120.0
}
fn default_isolation() -> String {
    "SERIALIZABLE".into()
}
fn default_lock_wait() -> f64 {
    50.0
}

#[derive(Clone, Debug, Deserialize)]
pub struct ShardConfig {
    /// Shard number (1..N), as configured on its storage nodes (TARN_SHARD_ID).
    pub id: u32,
    /// IPROTO addresses of every replica; the writable one is the leader.
    pub replicas: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ClusterConfig {
    /// Query/transaction and gateway-wide SQL resource budgets.
    #[serde(default)]
    pub resources: crate::budget::Limits,
    pub shards: Vec<ShardConfig>,
    /// The number of buckets of a new cluster (default
    /// `buckets::NEW_CLUSTER_BUCKETS`), fixed when the first SQL server
    /// makes the bucket map; from then on the map's count is used. See
    /// `buckets::new_map_count`.
    #[serde(default)]
    pub bucket_count: Option<u32>,
    #[serde(default = "default_user")]
    pub user: String,
    #[serde(default = "default_user")]
    pub password: String,
    /// Connections per shard leader (streams are multiplexed on them).
    #[serde(default = "default_pool")]
    pub pool_size: usize,
    /// Opt into bounded native primary-key batches. Other native paths
    /// remain enabled when this is false, for isolated profiling.
    #[serde(default)]
    pub native_batch_reads: bool,
    /// Distinguishes Tarn servers in transaction ids.
    #[serde(default)]
    pub node_id: u32,
    /// Acknowledge cross-shard commits once every PREPARE succeeded (one
    /// round trip) instead of after APPLY. Keeps SQL strict serializability
    /// (checked in TLA+); gives up read-your-writes for direct KV readers.
    #[serde(default)]
    pub ack_on_prepare: bool,
    /// Storage nodes refuse PREPAREs of transactions older than this
    /// (seconds; must match `max_txn_age` of lua/tarn.lua). A coordinator
    /// whose transaction is older no longer reads a missing record as
    /// "never prepared" (it may have been garbage-collected).
    #[serde(default = "default_max_txn_age")]
    pub max_txn_age: f64,
    /// Default transaction isolation of new sessions (`SET GLOBAL
    /// transaction_isolation` changes it at run time): READ-COMMITTED (or
    /// READ-UNCOMMITTED) runs READ COMMITTED with row locks; REPEATABLE-READ
    /// and SERIALIZABLE run strictly serializable.
    #[serde(default = "default_isolation")]
    pub isolation: String,
    /// Default innodb_lock_wait_timeout of new sessions, seconds.
    #[serde(default = "default_lock_wait")]
    pub lock_wait_timeout: f64,
    /// Seconds between runs of the cross-shard deadlock detector (0: off).
    #[serde(default = "default_deadlock_interval")]
    pub deadlock_interval: f64,
    /// Engine of tables created without `ENGINE = memtx | vinyl` (MySQL
    /// dumps say `ENGINE=InnoDB`).
    #[serde(default)]
    pub default_engine: crate::catalog::Engine,
    /// A schema change whose server has not touched it for this long
    /// (seconds) is taken over and finished by another server.
    #[serde(default = "default_ddl_job_stale")]
    pub ddl_job_stale: f64,
    /// Collation of text columns in tables that name none (no COLLATE or
    /// CHARSET option): `collation_server`.
    #[serde(default = "default_collation")]
    pub default_collation: String,
    /// How long a bucket move waits, at each step, for the transactions
    /// that hold locks on the bucket's rows (seconds); a move that waits
    /// longer is given up.
    #[serde(default = "default_move_timeout")]
    pub move_timeout: f64,
    /// Bounded IPROTO admission and physical connection lifetimes.
    #[serde(default)]
    pub rpc: crate::RpcConfig,
}

fn default_move_timeout() -> f64 {
    10.0
}

fn default_ddl_job_stale() -> f64 {
    30.0
}

fn default_collation() -> String {
    "utf8mb4_bin".into()
}

fn default_deadlock_interval() -> f64 {
    1.0
}

/// What a job does (lua/tarn.lua "Jobs"): a schema change, or a bucket
/// move (buckets.rs).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum JobKind {
    Alter,
    Create,
    Drop,
    Truncate,
    Move,
    /// A change of the cluster's shards (members.rs).
    Members,
}

impl JobKind {
    fn name(self) -> &'static str {
        match self {
            JobKind::Alter => "alter",
            JobKind::Create => "create",
            JobKind::Drop => "drop",
            JobKind::Truncate => "truncate",
            JobKind::Move => "move",
            JobKind::Members => "members",
        }
    }

    fn parse(s: &str) -> Option<JobKind> {
        Some(match s {
            "alter" => JobKind::Alter,
            "create" => JobKind::Create,
            "drop" => JobKind::Drop,
            "truncate" => JobKind::Truncate,
            "move" => JobKind::Move,
            "members" => JobKind::Members,
            _ => return None,
        })
    }

    /// The storage procedure of a step: build (ALTER TABLE only), prepare,
    /// apply or abort.
    fn proc(self, step: &str) -> String {
        format!("tarn_{}_{}", self.name(), step)
    }

    fn statement(self) -> &'static str {
        match self {
            JobKind::Alter => "ALTER TABLE",
            JobKind::Create => "CREATE TABLE",
            JobKind::Drop => "DROP TABLE",
            JobKind::Truncate => "TRUNCATE",
            JobKind::Move => "TARN MOVE BUCKET",
            JobKind::Members => "TARN ADD SHARD / REMOVE SHARD",
        }
    }
}

/// A job in progress: a schema change -- CREATE, DROP, TRUNCATE or ALTER
/// TABLE -- or a bucket move (its records on the first shard, one per name
/// it touches: a table name, or 'bucket:<n>').
pub(crate) struct Job {
    pub(crate) owner: String,
    pub(crate) age: f64,
    pub(crate) kind: JobKind,
    pub(crate) space: String,
    /// "prepare" until every shard prepared, then "apply" or "abort" ("run":
    /// an ALTER TABLE begun by a server from before phases); a move:
    /// "prepare", then "flip" from its send on
    pub(crate) phase: String,
    /// msgpack of its arguments (`a` of the storage procedures)
    pub(crate) args: Vec<u8>,
    /// Which run of the job this is: 1, plus one per takeover
    /// (`tarn_job_touch`). Its steps carry it (see lua/tarn.lua UIX.filling).
    pub(crate) gen: u64,
}

pub(crate) const MAX_SPACE_ID: u64 = 2_147_483_646;

fn allocated_space_start(start: u64, n: u64) -> SqlResult<u32> {
    let first = start
        .checked_add(10_000)
        .filter(|_| start > 0 && n > 0)
        .ok_or_else(|| SqlError::internal("space identifier allocation exhausted"))?;
    let last = first
        .checked_add(n - 1)
        .filter(|&last| last <= MAX_SPACE_ID)
        .ok_or_else(|| SqlError::internal("space identifier allocation exhausted"))?;
    let _ = last;
    Ok(first as u32)
}

/// From the arguments of an ALTER TABLE job: the space ids of its new
/// global indexes (`uix`), its new definition (`def.json`), the table's
/// space id (`def.id`) and the change's version (`ver`).
fn alter_uix_args(args: &[u8]) -> SqlResult<(Vec<u32>, String, u32, u64)> {
    let mut rd = Rd::new(args);
    let n = tarn_iproto::mp::read_map_len(&mut rd.buf).map_err(crate::mpw::proto)?;
    let (mut ids, mut json, mut sid, mut ver) = (Vec::new(), String::new(), 0, 0);
    for _ in 0..n {
        match rd.string()?.as_str() {
            "ver" => ver = rd.value()?.as_i64().unwrap_or(0) as u64,
            "uix" => {
                for _ in 0..rd.array()? {
                    let m = tarn_iproto::mp::read_map_len(&mut rd.buf).map_err(crate::mpw::proto)?;
                    for _ in 0..m {
                        if rd.string()? == "id" {
                            ids.push(rd.value()?.as_i64().unwrap_or(0) as u32);
                        } else {
                            rd.skip()?;
                        }
                    }
                }
            }
            "def" => {
                let m = tarn_iproto::mp::read_map_len(&mut rd.buf).map_err(crate::mpw::proto)?;
                for _ in 0..m {
                    match rd.string()?.as_str() {
                        "json" => json = rd.string()?,
                        "id" => sid = rd.value()?.as_i64().unwrap_or(0) as u32,
                        _ => rd.skip()?,
                    }
                }
            }
            _ => rd.skip()?,
        }
    }
    Ok((ids, json, sid, ver))
}

/// `{'ok', 'applied'}`: a step found its job already applied on that shard.
fn applied_reply(d: &[u8]) -> bool {
    let mut rd = Rd::new(d);
    matches!(rd.array(), Ok(n) if n > 0)
        && matches!(rd.array(), Ok(m) if m > 1)
        && rd.string().is_ok_and(|s| s == "ok")
        && rd.string().is_ok_and(|s| s == "applied")
}

/// Server-wide defaults for new sessions (SET GLOBAL).
pub struct Globals {
    pub isolation: crate::admin::Isolation,
    pub lock_wait_timeout: f64,
}

struct Pool {
    addr: String,
    clients: Vec<Client>,
    next: AtomicUsize,
}

/// A storage version is not eligible for SQL merely because it is writable.
/// This capability describes code, not readiness or a reusable read barrier.
fn strong_read_contract(data: &[u8]) -> bool {
    let mut rd = Rd::new(data);
    (|| -> SqlResult<bool> {
        Ok(rd.array()? == 1
            && rd.array()? == 2
            && matches!(rd.value()?, Value::Str(s) if s.as_ref() == b"strong_reads")
            && matches!(rd.value()?, Value::Int(1) | Value::UInt(1))
            && rd.buf.is_empty())
    })()
    .unwrap_or(false)
}

async fn require_strong_reads(client: &Client, completion: bool) -> SqlResult<()> {
    // Discovery for an existing completion must preserve its reserved lane
    // through capability negotiation as well as the writable-node probe.
    let response = tokio::time::timeout(Duration::from_secs(5), async {
        if completion {
            client.call_control(0, "tarn_read_contract", &[0x90]).await
        } else {
            client.call(0, "tarn_read_contract", &[0x90]).await
        }
    })
    .await;
    let valid = match response {
        Ok(Ok(r)) => r.data().is_some_and(|data| strong_read_contract(&data)),
        _ => false,
    };
    if valid {
        return Ok(());
    }
    // In particular retire a timed-out capability request rather than leave
    // its cancelled waiter on a socket that will never be used by the pool.
    client.close();
    Err(SqlError::unsupported(
        "SQL requires storage strong-read protocol V1; upgrade the storage Lua module before this gateway",
    ))
}

impl Pool {
    fn pick(&self) -> Option<Client> {
        let n = self.clients.len();
        let start = self.next.fetch_add(1, Ordering::Relaxed);
        for i in 0..n {
            let c = &self.clients[(start + i) % n];
            // A stale connection answered ER_READONLY: its server is no
            // longer the leader, although the connection survived.
            if !c.is_closed() && !c.is_stale() {
                return Some(c.clone());
            }
        }
        None
    }
}

pub struct Shard {
    /// Configured shard number (1-based).
    pub id: u32,
    replicas: Vec<String>,
    leader: RwLock<Option<Arc<Pool>>>,
    discover: tokio::sync::Mutex<()>,
    rpc_budget: std::sync::OnceLock<tarn_iproto::Budget>,
    /// The shard left the cluster: it owns no bucket, and fan-outs and
    /// schema changes skip it.
    removed: std::sync::atomic::AtomicBool,
}

/// The most shards a cluster may have.
pub const MAX_SHARDS: usize = 1024;

/// The shards a server knows, by position (`Cluster::shards`). Positions
/// never change while the server runs: a shard that joins the cluster is
/// added at the end, and one that leaves stays, marked removed, so that the
/// positions in transactions, bucket maps and lock requests stay valid.
pub struct ShardList {
    slots: Box<[std::sync::OnceLock<Shard>]>,
    len: AtomicUsize,
    add: Mutex<()>,
}

impl ShardList {
    fn new(shards: Vec<Shard>) -> ShardList {
        let slots: Box<[std::sync::OnceLock<Shard>]> = (0..MAX_SHARDS).map(|_| std::sync::OnceLock::new()).collect();
        let n = shards.len();
        for (i, s) in shards.into_iter().enumerate() {
            let _ = slots[i].set(s);
        }
        ShardList {
            slots,
            len: AtomicUsize::new(n),
            add: Mutex::new(()),
        }
    }

    pub fn len(&self) -> usize {
        self.len.load(Ordering::Acquire)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn iter(&self) -> impl Iterator<Item = &Shard> {
        (0..self.len()).map(move |i| &self[i])
    }

    /// Add a shard at the end: its position.
    fn push(&self, s: Shard) -> SqlResult<usize> {
        let _g = self.add.lock();
        let n = self.len();
        if n == MAX_SHARDS {
            return Err(SqlError::internal(format!(
                "a cluster has at most {} shards",
                MAX_SHARDS
            )));
        }
        let _ = self.slots[n].set(s);
        self.len.store(n + 1, Ordering::Release);
        Ok(n)
    }
}

impl std::ops::Index<usize> for ShardList {
    type Output = Shard;
    fn index(&self, i: usize) -> &Shard {
        self.slots[i].get().expect("a shard position")
    }
}

impl<'a> IntoIterator for &'a ShardList {
    type Item = &'a Shard;
    type IntoIter = Box<dyn Iterator<Item = &'a Shard> + 'a>;
    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.iter())
    }
}

impl Shard {
    pub(crate) fn rpc_budget(&self, cluster: &Cluster) -> tarn_iproto::Budget {
        self.rpc_budget
            .get_or_init(|| {
                tarn_iproto::Budget::new("shard", cluster.cfg.rpc.shard)
                    .expect("RPC configuration validated before connect")
            })
            .clone()
    }
    pub(crate) fn replicas(&self) -> &[String] {
        &self.replicas
    }
    pub(crate) fn new(id: u32, replicas: Vec<String>) -> Shard {
        Shard {
            id,
            replicas,
            leader: RwLock::new(None),
            discover: tokio::sync::Mutex::new(()),
            rpc_budget: std::sync::OnceLock::new(),
            removed: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Whether the shard left the cluster.
    pub fn removed(&self) -> bool {
        self.removed.load(Ordering::Acquire)
    }

    pub(crate) fn set_removed(&self) {
        self.removed.store(true, Ordering::Release);
    }

    pub async fn client(&self, cluster: &Cluster) -> SqlResult<Client> {
        self.client_for(cluster, false).await
    }

    pub(crate) async fn client_control(&self, cluster: &Cluster) -> SqlResult<Client> {
        self.client_for(cluster, true).await
    }

    async fn client_for(&self, cluster: &Cluster, completion: bool) -> SqlResult<Client> {
        if let Some(p) = self.leader.read().clone() {
            if let Some(c) = p.pick() {
                return Ok(c);
            }
        }
        self.discover(cluster, completion).await
    }

    pub fn invalidate(&self) {
        *self.leader.write() = None;
    }

    pub(crate) fn retire_connections(&self) {
        if let Some(pool) = self.leader.write().take() {
            for client in &pool.clients {
                client.close();
            }
        }
    }

    pub fn leader_addr(&self) -> Option<String> {
        self.leader.read().as_ref().map(|p| p.addr.clone())
    }

    async fn discover(&self, cluster: &Cluster, completion: bool) -> SqlResult<Client> {
        let _g = self.discover.lock().await;
        if let Some(p) = self.leader.read().clone() {
            if let Some(c) = p.pick() {
                return Ok(c);
            }
        }
        let mut last_err = String::from("no replicas configured");
        for addr in &self.replicas {
            let opts = cluster.opts_for_shard(self, addr);
            let c = match Client::connect(&opts).await {
                Ok(c) => c,
                Err(e) => {
                    last_err = format!("{}: {}", addr, e);
                    continue;
                }
            };
            // Routing a completion must not first spend ordinary capacity
            // on its leader probe, or normal overload could strand it.
            let probe = if completion {
                c.eval_control(0, "return box.info.ro", &[0x90]).await
            } else {
                c.eval(0, "return box.info.ro", &[0x90]).await
            };
            let ro = match probe {
                // DATA = [ro]; Tarantool may use any array header width.
                Ok(r) => r
                    .data()
                    .and_then(|d| {
                        let mut rd = Rd::new(&d);
                        let n = rd.array().ok()?;
                        if n == 0 {
                            return None;
                        }
                        rd.value().ok().map(|v| v.as_i64() != Some(0))
                    })
                    .unwrap_or(true),
                Err(e) => {
                    last_err = format!("{}: {}", addr, e);
                    continue;
                }
            };
            if ro {
                last_err = format!("{} is read-only", addr);
                continue;
            }
            crate::compat::admit(&c, self.id, cluster.cfg.max_txn_age, completion).await?;
            if let Err(e) = require_strong_reads(&c, completion).await {
                last_err = format!("{}: {}", addr, e);
                continue;
            }
            let mut clients = vec![c.clone()];
            for _ in 1..cluster.cfg.pool_size.max(1) {
                match Client::connect(&opts).await {
                    Ok(x) => {
                        crate::compat::admit(&x, self.id, cluster.cfg.max_txn_age, completion).await?;
                        if let Err(e) = require_strong_reads(&x, completion).await {
                            tracing::warn!(shard = self.id, "extra storage connection is incompatible: {}", e);
                            break;
                        }
                        clients.push(x);
                    }
                    Err(e) => {
                        tracing::warn!(shard = self.id, "extra connection to {}: {}", addr, e);
                        break;
                    }
                }
            }
            let pool = Arc::new(Pool {
                addr: addr.clone(),
                clients,
                next: AtomicUsize::new(0),
            });
            *self.leader.write() = Some(pool);
            tracing::info!(shard = self.id, leader = %addr, "connected to shard leader");
            return Ok(c);
        }
        Err(SqlError::new(
            2003,
            "HY000",
            format!("shard {} has no reachable leader ({})", self.id, last_err),
        ))
    }
}

#[derive(Default)]
pub struct Stats {
    pub topk_scans: AtomicU64,
    pub(crate) query_digests: crate::observe::Metrics,
    pub queries: AtomicU64,
    pub commits_local: AtomicU64,
    pub commits_2pc: AtomicU64,
    pub commits_ro: AtomicU64,
    pub conflicts: AtomicU64,
    pub retries: AtomicU64,
    pub plan_cache_hits: AtomicU64,
    pub plan_cache_misses: AtomicU64,
    /// Deadlocks through several shards broken by `detect_deadlocks`.
    pub deadlocks: AtomicU64,
    /// Storage calls made by `detect_deadlocks`.
    pub deadlock_calls: AtomicU64,
    /// COUNTs the shards computed without returning rows (issue #52).
    pub storage_counts: AtomicU64,
    /// Rows read from storage by SELECT, UPDATE and DELETE.
    pub rows_read: AtomicU64,
    /// Storage reads (pages) of index scans and multi-key lookups that
    /// returned those rows.
    pub storage_reads: AtomicU64,
    /// Row counts taken to choose the access path of an ordered read with
    /// LIMIT (issue #81).
    pub path_estimates: AtomicU64,
    /// Buckets this server moved (issue #24).
    pub bucket_moves: AtomicU64,
    pub lazy_reads: AtomicU64,
    pub lazy_fallbacks: AtomicU64,
}

pub struct CachedPlan {
    pub resource_guard: tarn_common::ResourceGuard,
    pub(crate) statistics_generation: Option<u64>,
    pub version: u64,
    pub stmt: crate::plan::Stmt,
    pub(crate) diagnostic: Option<crate::observe::Descriptor>,
}

pub struct Cluster {
    pub resources: Arc<crate::budget::Pool>,
    pub statistics: crate::statistics::Statistics,
    pub(crate) rpc_global: tarn_iproto::Budget,
    pub cfg: ClusterConfig,
    pub shards: ShardList,
    catalog: RwLock<Arc<Catalog>>,
    plans: RwLock<HashMap<(String, String), Arc<CachedPlan>>>,
    /// Blocks of counter values reserved by this server (AUTO_INCREMENT,
    /// hidden row ids), by counter name: (next, last).
    auto_inc: Mutex<HashMap<String, (u64, u64)>>,
    txn_seq: AtomicU64,
    boot: u32,
    pub stats: Stats,
    pub globals: Mutex<Globals>,
    /// Catalog reloads so far, and the lock that makes concurrent
    /// `refresh_catalog` calls share one reload.
    reloads: AtomicU64,
    reload_mu: tokio::sync::Mutex<()>,
    /// Row-lock requests in flight, by transaction (each makes one at a
    /// time): where the cross-shard deadlock detector starts.
    lock_requests: Mutex<HashMap<String, LockRequest>>,
    /// Position in `shards` of each shard id.
    pub(crate) shard_pos: RwLock<HashMap<u32, usize>>,
    /// Which shard owns each bucket (buckets.rs), the number of times it was
    /// read, and the lock that makes concurrent reads share one.
    buckets: RwLock<Arc<BucketMap>>,
    bucket_reads: AtomicU64,
    pub(crate) bucket_mu: tokio::sync::Mutex<()>,
    /// The membership this server knows (members.rs; version 0: its
    /// configuration's), and the lock that makes reads of it one at a time.
    pub(crate) members: RwLock<crate::members::Members>,
    members_ver: AtomicU64,
    pub(crate) members_mu: tokio::sync::Mutex<()>,
    pub(crate) lazy_samples: Mutex<std::collections::BTreeMap<u32, Option<crate::lazy::StatsSample>>>,
    pub(crate) vinyl_samples: Mutex<std::collections::BTreeMap<u32, crate::vinyl::StatsSample>>,
}

/// A row-lock request in flight (`Txn::lock`).
struct LockRequest {
    /// Shard (position in `Cluster::shards`) it may wait on.
    shard: usize,
    /// The other shards where its transaction holds row locks.
    held: Vec<usize>,
    since: Instant,
}

/// Keeps a request in `Cluster::lock_requests` until dropped.
pub struct LockRequestGuard<'a> {
    cluster: &'a Cluster,
    txid: String,
}

impl Drop for LockRequestGuard<'_> {
    fn drop(&mut self) {
        self.cluster.lock_requests.lock().remove(&self.txid);
    }
}

/// A lock request younger than this is left out of a detection round: it
/// is most likely about to be granted, and the next round looks at it again
/// if it still waits.
const DEADLOCK_MIN_WAIT: Duration = Duration::from_millis(50);

/// A pending row-lock wait, as its shard reports it (`tarn_lock_waits`).
#[derive(Clone, Debug, PartialEq)]
struct Wait {
    /// Shard (position in `Cluster::shards`) where it waits.
    shard: usize,
    waiter: String,
    /// The shard's id for this wait: a later wait of the transaction has
    /// another one.
    id: u64,
    /// The transactions it waits for: holders of the lock in a conflicting
    /// mode, and conflicting requests queued before it.
    holders: Vec<String>,
    /// The other shards where the waiter holds row locks (sent with its
    /// request).
    held: Vec<usize>,
}

/// `tarn_lock_waits` on each of `shards`, in parallel. A shard that does
/// not answer contributes nothing.
async fn lock_waits(c: &Cluster, shards: &[usize]) -> Vec<Wait> {
    let futs = shards.iter().map(|&s| c.call(s, "tarn_lock_waits", &[0x90]));
    let replies = futures::future::join_all(futs).await;
    c.stats.deadlock_calls.fetch_add(shards.len() as u64, Ordering::Relaxed);
    let mut out = Vec::new();
    for (&s, r) in shards.iter().zip(replies) {
        if let Ok(v) = r.and_then(|d| parse_waits(&d, s, &c.shard_pos.read())) {
            out.extend(v);
        }
    }
    out
}

/// The waits in a `tarn_lock_waits` reply from shard `s`. Storage nodes
/// that predate the held shards (issue #26) send three fields per wait.
fn parse_waits(d: &[u8], s: usize, shard_pos: &HashMap<u32, usize>) -> SqlResult<Vec<Wait>> {
    let mut rd = Rd::new(d);
    let mut v = Vec::new();
    rd.array()?; // CALL results
    let n = rd.array()?;
    for _ in 0..n {
        let fields = rd.array()?;
        let waiter = rd.string()?;
        let id = rd.value()?.as_i64().unwrap_or(0) as u64;
        let m = rd.array()?;
        let mut holders = Vec::with_capacity(m);
        for _ in 0..m {
            holders.push(rd.string()?);
        }
        let mut held = Vec::new();
        if fields > 3 {
            for _ in 0..rd.array()? {
                let id = rd.value()?.as_i64().unwrap_or(0);
                if let Some(&p) = u32::try_from(id).ok().and_then(|id| shard_pos.get(&id)) {
                    held.push(p);
                }
            }
        }
        for _ in 4..fields {
            rd.skip()?;
        }
        v.push(Wait {
            shard: s,
            waiter,
            id,
            holders,
            held,
        });
    }
    Ok(v)
}

/// The first collection of a detection round. It starts from this server's
/// waiting transactions, visiting the shards where they wait or hold row
/// locks, and follows the wait-for edges backwards: a transaction found
/// waiting for one already reached is reached too, and the shards where it
/// holds row locks are visited next, since its own waiters can only be
/// there or on the shard where it waits, already visited. A transaction
/// waits for one lock at a time and takes no lock while it waits, so the
/// held shards its request carried are still exact. Every shard is visited
/// at most once.
struct Walk {
    reached: HashSet<String>,
    visited: BTreeSet<usize>,
    next: BTreeSet<usize>,
    waits: Vec<Wait>,
}

impl Walk {
    /// `starts`: (transaction, shard it waits on, shards where it holds
    /// row locks).
    fn new(starts: &[(String, usize, Vec<usize>)]) -> Walk {
        let mut w = Walk {
            reached: HashSet::new(),
            visited: BTreeSet::new(),
            next: BTreeSet::new(),
            waits: Vec::new(),
        };
        for (txid, shard, held) in starts {
            w.reached.insert(txid.clone());
            w.next.insert(*shard);
            w.next.extend(held.iter().copied());
        }
        w
    }

    /// The shards to visit in the next step (none: the walk is over).
    fn step(&mut self) -> Vec<usize> {
        let out: Vec<usize> = std::mem::take(&mut self.next).into_iter().collect();
        self.visited.extend(out.iter().copied());
        out
    }

    /// Add the waits reported by the shards of the last step.
    fn add(&mut self, got: Vec<Wait>) {
        self.waits.extend(got);
        let mut by_holder: HashMap<&str, Vec<usize>> = HashMap::new();
        for (i, w) in self.waits.iter().enumerate() {
            for h in &w.holders {
                by_holder.entry(h.as_str()).or_default().push(i);
            }
        }
        let mut todo: Vec<String> = self.reached.iter().cloned().collect();
        while let Some(t) = todo.pop() {
            for &i in by_holder.get(t.as_str()).map(|v| v.as_slice()).unwrap_or(&[]) {
                let w = &self.waits[i];
                if self.reached.insert(w.waiter.clone()) {
                    todo.push(w.waiter.clone());
                }
            }
        }
        for w in &self.waits {
            if self.reached.contains(&w.waiter) {
                self.next.extend(w.held.iter().filter(|s| !self.visited.contains(s)));
            }
        }
    }
}

/// Cycles in the wait-for graph of `waits`, each as its waits in order:
/// each one waits for the waiter of the next (the last for the first's).
fn wait_cycles(waits: &[Wait]) -> Vec<Vec<Wait>> {
    let mut adj: HashMap<&str, Vec<(&str, &Wait)>> = HashMap::new();
    for w in waits {
        for h in &w.holders {
            adj.entry(w.waiter.as_str()).or_default().push((h.as_str(), w));
        }
    }
    let mut cycles: Vec<Vec<Wait>> = Vec::new();
    let mut seen_sets: Vec<Vec<String>> = Vec::new();
    let mut done: HashSet<&str> = HashSet::new();
    for start in adj.keys().copied().collect::<Vec<_>>() {
        if done.contains(start) {
            continue;
        }
        // Iterative DFS keeping the path of (node, edge into it).
        let mut path: Vec<(&str, Option<&Wait>)> = vec![(start, None)];
        let mut iters: Vec<usize> = vec![0];
        while let Some(&(node, _)) = path.last() {
            let i = *iters.last().unwrap();
            let next = adj.get(node).and_then(|v| v.get(i)).copied();
            *iters.last_mut().unwrap() += 1;
            match next {
                None => {
                    done.insert(node);
                    path.pop();
                    iters.pop();
                }
                Some((h, w)) => {
                    if let Some(pos) = path.iter().position(|(n, _)| *n == h) {
                        let mut cyc: Vec<Wait> = path[pos + 1..].iter().filter_map(|(_, e)| e.cloned()).collect();
                        cyc.push(w.clone());
                        let mut members: Vec<String> = cyc.iter().map(|w| w.waiter.clone()).collect();
                        members.sort();
                        if !seen_sets.contains(&members) {
                            seen_sets.push(members);
                            cycles.push(cyc);
                        }
                    } else if !done.contains(h) && adj.contains_key(h) {
                        path.push((h, Some(w)));
                        iters.push(0);
                    }
                }
            }
        }
    }
    cycles
}

/// Is every wait of `cyc` still pending in `second`, with the same id and
/// still waiting for the next transaction of the cycle? Each edge was then
/// there at both observations and all the time in between (a transaction
/// that leaves a waits-for set can only queue behind the waiter again), and
/// every first observation came before every second one: at any moment
/// between the two collections, every edge of the cycle was there at once.
fn still_deadlocked(cyc: &[Wait], second: &[Wait]) -> bool {
    cyc.iter().enumerate().all(|(i, w)| {
        let next = &cyc[(i + 1) % cyc.len()].waiter;
        second
            .iter()
            .any(|v| v.shard == w.shard && v.waiter == w.waiter && v.id == w.id && v.holders.contains(next))
    })
}

/// Cross-shard deadlock detection (issues #17 and #26). Each shard fails a
/// request that would close a cycle among its own waits at once (1213); a
/// cycle through several shards is found here. Every `deadlock_interval`
/// seconds, if some of this server's lock requests have been waiting for
/// `DEADLOCK_MIN_WAIT`, a `Walk` from them collects the wait-for edges that
/// lead to them; a cycle found there is a real deadlock if a second
/// collection, from the shards of its waits, still has each of them pending
/// with the same id and waiting for the same next transaction
/// (`still_deadlocked`). Its transaction with the largest id (the same
/// choice on every SQL server) is failed with 1213 on the shard where it
/// waits. Every transaction of a cycle waits, and its SQL server walks from
/// it, so every deadlock is found; a server with no waiting request makes
/// no call.
pub async fn detect_deadlocks(c: &Arc<Cluster>) {
    let starts: Vec<(String, usize, Vec<usize>)> = c
        .lock_requests
        .lock()
        .iter()
        .filter(|(_, r)| r.since.elapsed() >= DEADLOCK_MIN_WAIT)
        .map(|(t, r)| (t.clone(), r.shard, r.held.clone()))
        .collect();
    if starts.is_empty() {
        return;
    }
    let mut walk = Walk::new(&starts);
    loop {
        let shards = walk.step();
        if shards.is_empty() {
            break;
        }
        let got = lock_waits(c, &shards).await;
        walk.add(got);
    }
    let cycles = wait_cycles(&walk.waits);
    if cycles.is_empty() {
        return;
    }
    let shards: BTreeSet<usize> = cycles.iter().flatten().map(|w| w.shard).collect();
    let second = lock_waits(c, &shards.into_iter().collect::<Vec<_>>()).await;
    for cyc in cycles {
        if !still_deadlocked(&cyc, &second) {
            continue;
        }
        let victim = cyc.iter().max_by(|a, b| a.waiter.cmp(&b.waiter)).unwrap();
        let mut m = Mp::new();
        m.arr(2).str(&victim.waiter).uint(victim.id);
        // true when this call failed the wait; false when it was already
        // over (another SQL server's detector may have got there first)
        c.stats.deadlock_calls.fetch_add(1, Ordering::Relaxed);
        let broke = match c.call(victim.shard, "tarn_lock_victim", &m.0).await {
            Ok(d) => {
                let mut rd = Rd::new(&d);
                rd.array().is_ok() && matches!(rd.value(), Ok(Value::Int(1)))
            }
            Err(_) => false,
        };
        if broke {
            c.stats.deadlocks.fetch_add(1, Ordering::Relaxed);
            tracing::info!(
                "deadlock across shards among {} transactions; rolled back {}",
                cyc.len(),
                victim.waiter
            );
        }
    }
}

impl Cluster {
    pub async fn connect(cfg: ClusterConfig) -> SqlResult<Arc<Cluster>> {
        cfg.rpc.validate()?;
        let rpc_global =
            tarn_iproto::Budget::new("global", cfg.rpc.global).map_err(|e| SqlError::internal(e.to_string()))?;
        if cfg.shards.is_empty() {
            return Err(SqlError::internal("cluster config has no shards"));
        }
        let shards = ShardList::new(
            cfg.shards
                .iter()
                .map(|s| Shard::new(s.id, s.replicas.clone()))
                .collect(),
        );
        let boot = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| (d.as_nanos() & 0xFFFF_FFFF) as u32)
            .unwrap_or(1);
        let isolation = crate::admin::Isolation::parse(&cfg.isolation).ok_or_else(|| {
            SqlError::internal(format!(
                "unknown isolation level '{}' in the configuration",
                cfg.isolation
            ))
        })?;
        let globals = Mutex::new(Globals {
            isolation,
            lock_wait_timeout: cfg.lock_wait_timeout,
        });
        let shard_pos = cfg.shards.iter().enumerate().map(|(i, s)| (s.id, i)).collect();
        // (until the map is read, below)
        let (bucket_count, nshards) = (
            cfg.bucket_count.unwrap_or(crate::buckets::LEGACY_BUCKETS),
            cfg.shards.len(),
        );
        let c = Arc::new(Cluster {
            resources: crate::budget::Pool::new(cfg.resources.clone())?,
            statistics: crate::statistics::Statistics::default(),
            rpc_global,
            cfg,
            shards,
            globals,
            catalog: RwLock::new(Arc::new(Catalog::default())),
            plans: RwLock::new(HashMap::new()),
            auto_inc: Mutex::new(HashMap::new()),
            txn_seq: AtomicU64::new(1),
            boot,
            stats: Stats::default(),
            reloads: AtomicU64::new(0),
            reload_mu: tokio::sync::Mutex::new(()),
            lock_requests: Mutex::new(HashMap::new()),
            shard_pos: RwLock::new(shard_pos),
            buckets: RwLock::new(Arc::new(BucketMap::initial(bucket_count, nshards))),
            bucket_reads: AtomicU64::new(0),
            bucket_mu: tokio::sync::Mutex::new(()),
            members: RwLock::new(crate::members::Members::default()),
            members_ver: AtomicU64::new(0),
            members_mu: tokio::sync::Mutex::new(()),
            lazy_samples: Mutex::new(Default::default()),
            vinyl_samples: Mutex::new(Default::default()),
        });
        // The first shard (jobs, catalog) must answer, and every shard in
        // the cluster; one of the configuration that left the cluster need
        // not (the membership the others keep says it left).
        c.shards[0].client(&c).await?;
        c.load_members().await?;
        for i in c.live_shards().collect::<Vec<_>>() {
            c.shards[i].client(&c).await?;
        }
        c.reload_buckets().await?;
        if let Some(n) = c.cfg.bucket_count {
            let count = c.bucket_map().count;
            if n != count {
                tracing::warn!(
                    "bucket_count is {} in the configuration, but the cluster has {} buckets (fixed when it was made): using {}",
                    n,
                    count,
                    count
                );
            }
        }
        if let Err(e) = c.finish_stale_jobs().await {
            tracing::warn!("finishing abandoned schema changes: {}", e);
        }
        c.reload_catalog().await?;
        crate::lazy::start_stats(&c);
        crate::vinyl::start_stats(&c);
        {
            // Schema changes whose server stopped are finished by whichever
            // server finds them first.
            let weak = Arc::downgrade(&c);
            let every = Duration::from_secs_f64((c.cfg.ddl_job_stale / 2.0).clamp(0.1, 10.0));
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(every).await;
                    let Some(c) = weak.upgrade() else { break };
                    if let Err(e) = c.finish_stale_jobs().await {
                        tracing::debug!("looking for abandoned schema changes: {}", e);
                    }
                    // (and a rebalance whose server stopped)
                    if let Err(e) = c.rebalance_takeover().await {
                        tracing::debug!("looking for an abandoned rebalance: {}", e);
                    }
                }
            });
        }
        if c.cfg.deadlock_interval > 0.0 {
            let weak = Arc::downgrade(&c);
            let every = Duration::from_secs_f64(c.cfg.deadlock_interval);
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(every).await;
                    let Some(c) = weak.upgrade() else { break };
                    // (shards may join a cluster of one)
                    if c.nlive() > 1 {
                        detect_deadlocks(&c).await;
                    }
                }
            });
        }
        let weak = Arc::downgrade(&c);
        tokio::spawn(async move {
            let mut known = 0u64;
            loop {
                tokio::time::sleep(Duration::from_millis(300)).await;
                let Some(c) = weak.upgrade() else { break };
                if let Ok(v) = c.catalog_version().await {
                    if v != known {
                        known = v;
                        if let Err(e) = c.reload_catalog().await {
                            tracing::warn!("catalog reload failed: {}", e);
                        }
                    }
                }
            }
        });
        Ok(c)
    }

    pub fn opts(&self, addr: &str) -> Opts {
        let shard = self.shards.iter().find(|s| s.replicas.iter().any(|a| a == addr));
        self.rpc_opts(shard, addr)
    }

    fn opts_for_shard(&self, shard: &Shard, addr: &str) -> Opts {
        self.rpc_opts(Some(shard), addr)
    }

    fn rpc_opts(&self, shard: Option<&Shard>, addr: &str) -> Opts {
        let mut admission = Vec::with_capacity(2);
        if let Some(s) = shard {
            admission.push(s.rpc_budget(self));
        }
        admission.push(self.rpc_global.clone());
        Opts {
            addr: addr.to_string(),
            user: Some(self.cfg.user.clone()),
            password: Some(self.cfg.password.clone()),
            connect_timeout: Duration::from_secs(3),
            max_response_bytes: crate::budget::STORAGE_REPLY_BYTES,
            request_limits: self.cfg.rpc.connection,
            request_timeout: Duration::from_secs_f64(self.cfg.rpc.timeout_secs),
            admission,
            observation_scope: shard.map(|s| s.id),
        }
    }

    /// The number of shard positions (`shards`), removed shards included.
    pub fn nshards(&self) -> usize {
        self.shards.len()
    }

    /// The positions of the shards in the cluster (not removed): those a
    /// read of every shard reads, and a schema change acts on.
    pub fn live_shards(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.shards.len()).filter(move |&i| !self.shards[i].removed())
    }

    /// The number of shards in the cluster.
    pub fn nlive(&self) -> usize {
        self.live_shards().count()
    }

    /// Register a row-lock request of `txid` that may wait on `shard`, for
    /// the cross-shard deadlock detector, until the guard is dropped; `held`
    /// are the other shards where the transaction holds row locks.
    pub fn lock_request(&self, txid: &str, shard: usize, held: Vec<usize>) -> LockRequestGuard<'_> {
        let r = LockRequest {
            shard,
            held,
            since: Instant::now(),
        };
        self.lock_requests.lock().insert(txid.to_string(), r);
        LockRequestGuard {
            cluster: self,
            txid: txid.to_string(),
        }
    }

    pub fn catalog(&self) -> Arc<Catalog> {
        self.catalog.read().clone()
    }

    pub fn new_txid(&self) -> String {
        format!(
            "{}-{:08x}-{}",
            self.cfg.node_id,
            self.boot,
            self.txn_seq.fetch_add(1, Ordering::Relaxed)
        )
    }

    /// Shard index (0-based position in `shards`) owning a row/key, given the
    /// values of the table's shard-key columns.
    pub fn shard_of(&self, shard_key: &[Value]) -> usize {
        self.route(shard_key).0
    }

    /// The shard (position) and the bucket of a row/key, given the values
    /// of the table's shard-key columns.
    pub fn route(&self, shard_key: &[Value]) -> (usize, u32) {
        let m = self.buckets.read();
        let b = route::bucket_of(shard_key, m.count);
        (m.shard(b), b)
    }

    pub fn bucket_of(&self, shard_key: &[Value]) -> u32 {
        route::bucket_of(shard_key, self.buckets.read().count)
    }

    /// The bucket of the rows whose key in index `index` of `t` is `key` (a
    /// prefix of it), if the key determines the shard key.
    pub fn bucket_of_index_key(&self, t: &TableDef, index: u32, key: &[Value]) -> Option<u32> {
        let idx = t.indexes.iter().find(|i| i.id == index)?;
        crate::exec::route_bucket(self, t, idx, key).map(|(_, b)| b)
    }

    /// The bucket map this server routes by.
    pub fn bucket_map(&self) -> Arc<BucketMap> {
        self.buckets.read().clone()
    }

    /// Route by `map` from now on.
    pub(crate) fn install_buckets(&self, map: BucketMap) {
        *self.buckets.write() = Arc::new(map);
        self.bucket_reads.fetch_add(1, Ordering::AcqRel);
    }

    pub fn bucket_loads(&self) -> u64 {
        self.bucket_reads.load(Ordering::Acquire)
    }

    /// Read the bucket map from the shards now (one read at a time).
    pub async fn reload_buckets(&self) -> SqlResult<()> {
        let _g = self.bucket_mu.lock().await;
        self.load_buckets().await
    }

    /// The position in `shards` of the shard with id `id`.
    pub fn shard_position(&self, id: u32) -> Option<usize> {
        self.shard_pos.read().get(&id).copied()
    }

    /// The membership version this server routes with (members.rs).
    pub fn member_ver(&self) -> u64 {
        self.members_ver.load(Ordering::Acquire)
    }

    /// Take membership `m` if it is not older than the one this server
    /// has. (Its shards must be in `shards` already.)
    pub(crate) fn set_members(&self, m: crate::members::Members) {
        let mut cur = self.members.write();
        if m.ver < cur.ver {
            return;
        }
        self.members_ver.fetch_max(m.ver, Ordering::AcqRel);
        *cur = m;
    }

    /// Add a shard that joined the cluster at the end of `shards`: its
    /// position.
    pub(crate) fn add_known_shard(&self, s: Shard) -> SqlResult<usize> {
        let id = s.id;
        let mut pos = self.shard_pos.write();
        if let Some(&p) = pos.get(&id) {
            return Ok(p);
        }
        let p = self.shards.push(s)?;
        pos.insert(id, p);
        Ok(p)
    }

    /// CALL an idempotent Lua procedure on a shard leader outside any
    /// stream. Retried on a new leader after ER_READONLY (the procedure did
    /// not run) and once after a lost connection (it may have run: callers
    /// guarantee that running it twice is harmless).
    pub async fn call(&self, shard: usize, func: &str, args: &[u8]) -> SqlResult<Bytes> {
        self.call_inner(shard, func, args, true, None).await
    }

    /// CALL a procedure that must not run twice (autocommit DML). Retried
    /// only after ER_READONLY; a connection lost while the request was in
    /// flight leaves the outcome unknown, reported as such.
    pub async fn call_once(&self, shard: usize, func: &str, args: &[u8]) -> SqlResult<Bytes> {
        self.call_inner(shard, func, args, false, None).await
    }

    /// CALL a native autocommit procedure, or its Lua fallback, as
    /// `call_once`. An unavailable native procedure or a matched ["lua"]
    /// handback leaves the operation unwritten: use the selected physical
    /// client for Lua too, without advancing the pool again. Only an
    /// explicit ER_READONLY discards that selection and discovers a leader.
    /// Native unavailability and ["lua", "all"] are cached with `mark`.
    pub async fn call_native_or_lua_once(
        &self,
        shard: usize,
        func: &str,
        lua: &str,
        args: &[u8],
        mark: u32,
    ) -> SqlResult<Bytes> {
        crate::observe::shard(shard);
        use tarn_iproto::errcode::{
            ER_ACCESS_DENIED, ER_LOAD_FUNCTION, ER_LOAD_MODULE, ER_NO_SUCH_FUNCTION, ER_NO_SUCH_PROC,
        };
        let s = &self.shards[shard];
        let c = s.client(self).await?;
        if !c.marked(mark) {
            match c.call(0, func, args).await {
                Ok(r) => {
                    let d = r
                        .data()
                        .ok_or_else(|| crate::mpw::ambiguous(s.id, &"CALL reply without data"))?;
                    let handback = crate::mpw::native_write_handback(&d);
                    if handback == Some(true) {
                        tracing::debug!(shard = s.id, "{} hands back every call", func);
                        c.mark(mark);
                    }
                    if handback.is_none() {
                        return Ok(d);
                    }
                }
                Err(tarn_iproto::Error::Server {
                    code: ER_NO_SUCH_PROC | ER_NO_SUCH_FUNCTION | ER_LOAD_FUNCTION | ER_LOAD_MODULE | ER_ACCESS_DENIED,
                    message,
                }) => {
                    tracing::debug!(shard = s.id, "{} unavailable: {}", func, message);
                    c.mark(mark);
                }
                // A matched before-effect refusal permits Lua on a new leader.
                Err(tarn_iproto::Error::Server { code, .. }) if code == tarn_iproto::ER_READONLY => {
                    s.invalidate();
                    return self.call_once(shard, lua, args).await;
                }
                Err(e @ (tarn_iproto::Error::Closed | tarn_iproto::Error::Io(_))) => {
                    s.invalidate();
                    return Err(crate::mpw::ambiguous(s.id, &e));
                }
                Err(e) => return Err(from_iproto(e)),
            }
        }
        self.call_inner(shard, lua, args, false, Some(c)).await
    }

    async fn call_inner(
        &self,
        shard: usize,
        func: &str,
        args: &[u8],
        idempotent: bool,
        mut selected: Option<Client>,
    ) -> SqlResult<Bytes> {
        if matches!(
            func,
            "tarn_autocommit" | "tarn_apply" | "tarn_abort" | "tarn_fence" | "tarn_txn_cas"
        ) {
            crate::observe::shard(shard);
        }
        let s = &self.shards[shard];
        let mut tries = 0;
        loop {
            let completion = matches!(
                func,
                "tarn_apply"
                    | "tarn_abort"
                    | "tarn_fence"
                    | "tarn_txn_cas"
                    | "tarn_txn_status"
                    | "tarn_part_status"
                    | "tarn_unlock"
                    | "tarn_lock_waits"
                    | "tarn_lock_victim"
            );
            let c = match selected.take() {
                Some(c) => c,
                None if completion => s.client_control(self).await?,
                None => s.client(self).await?,
            };
            let reply = if completion {
                c.call_control(0, func, args).await
            } else {
                c.call(0, func, args).await
            };
            match reply {
                Ok(r) => {
                    return r.data().ok_or_else(|| {
                        if idempotent {
                            crate::mpw::proto("CALL reply without data")
                        } else {
                            crate::mpw::ambiguous(s.id, &"CALL reply without data")
                        }
                    });
                }
                Err(tarn_iproto::Error::Server { code, message }) if code == tarn_iproto::ER_READONLY && tries < 3 => {
                    tracing::warn!(shard = s.id, "leader changed: {}", message);
                    s.invalidate();
                }
                Err(e @ (tarn_iproto::Error::Closed | tarn_iproto::Error::Io(_))) if !idempotent => {
                    s.invalidate();
                    return Err(crate::mpw::ambiguous(s.id, &e));
                }
                Err(e @ (tarn_iproto::Error::Closed | tarn_iproto::Error::Io(_))) if tries < 1 => {
                    tracing::warn!(shard = s.id, "connection lost: {}", e);
                    s.invalidate();
                }
                Err(e) => return Err(from_iproto(e)),
            }
            tries += 1;
            tokio::time::sleep(Duration::from_millis(50 * tries)).await;
        }
    }

    /// CALL `func` on every shard in the cluster: the replies by position
    /// (None for a removed shard).
    pub async fn call_all(&self, func: &str, args: &[u8]) -> SqlResult<Vec<Option<Bytes>>> {
        let live: Vec<usize> = self.live_shards().collect();
        let replies = futures::future::try_join_all(live.iter().map(|&i| self.call(i, func, args))).await?;
        let mut out = vec![None; self.shards.len()];
        for (i, d) in live.into_iter().zip(replies) {
            out[i] = Some(d);
        }
        Ok(out)
    }

    async fn catalog_version(&self) -> SqlResult<u64> {
        let d = self.call(0, "tarn_catalog_version", &[0x90]).await?;
        let mut rd = Rd::new(&d);
        rd.array()?;
        Ok(rd.value()?.as_i64().unwrap_or(0) as u64)
    }

    /// Reload the catalog if the shards have a newer one (true if so).
    pub async fn refresh_if_newer(&self) -> SqlResult<bool> {
        let local = self.catalog().version;
        self.catch_up(local).await
    }

    /// Make the catalog at least as new as the shards' at this moment, and
    /// tell whether theirs is newer than version `than` (the catalog a
    /// statement was planned with: another reload may have replaced it since).
    pub async fn catch_up(&self, than: u64) -> SqlResult<bool> {
        let v = self.catalog_version().await?;
        if self.catalog().version < v {
            self.refresh_catalog().await?;
            // A reload that finished meanwhile may have read an older one.
            if self.catalog().version < v {
                self.reload_catalog().await?;
            }
        }
        Ok(v > than)
    }

    /// Reload the catalog after a statement met a newer table definition
    /// (1412); concurrent callers share one reload.
    pub async fn refresh_catalog(&self) -> SqlResult<()> {
        let seen = self.reloads.load(Ordering::Acquire);
        let _g = self.reload_mu.lock().await;
        if self.reloads.load(Ordering::Acquire) != seen {
            return Ok(());
        }
        self.reload_catalog().await
    }

    pub async fn reload_catalog(&self) -> SqlResult<()> {
        let version = self.catalog_version().await?;
        let d = self.call(0, "tarn_catalog", &[0x90]).await?;
        let mut rd = Rd::new(&d);
        rd.array()?;
        let n = rd.array()?;
        let mut defs = Vec::new();
        for _ in 0..n {
            let m = rd.array()?;
            let _name = rd.string()?;
            let id = rd.value()?.as_i64().unwrap_or(0) as u32;
            let json = rd.string()?;
            let bucket_index = if m > 3 {
                rd.value()?.as_i64().map(|v| v as u32)
            } else {
                None
            };
            for _ in 4..m {
                rd.skip()?;
            }
            let mut def = catalog::parse_def_json(&json)?;
            def.space_id = id;
            def.bucket_index = bucket_index;
            def.resolve_globals()?;
            defs.push(def);
        }
        let d = self.call(0, "tarn_db_list", &[0x90]).await?;
        let mut rd = Rd::new(&d);
        rd.array()?;
        let n = rd.array()?;
        let mut dbs = Vec::new();
        for _ in 0..n {
            dbs.push(rd.string()?);
        }
        dbs.sort();
        {
            // Reloads run concurrently (the poller, statements that met a
            // stale catalog, DDL); one that read an older catalog must not
            // replace a newer one.
            let mut cur = self.catalog.write();
            if version < cur.version {
                return Ok(());
            }
            *cur = Arc::new(Catalog::new(version, dbs, defs));
        }
        self.plans.write().clear();
        self.reloads.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    pub fn cached_plan(&self, db: &str, template: &str) -> Option<Arc<CachedPlan>> {
        let v = self.catalog.read().version;
        let p = self
            .plans
            .read()
            .get(&(db.to_string(), template.to_string()))
            .cloned()?;
        if p.version == v
            && p.statistics_generation
                .is_none_or(|g| self.statistics.snapshot(v).generation == g)
        {
            self.stats.plan_cache_hits.fetch_add(1, Ordering::Relaxed);
            Some(p)
        } else {
            None
        }
    }

    pub fn cache_plan(&self, db: &str, template: &str, plan: Arc<CachedPlan>) {
        self.stats.plan_cache_misses.fetch_add(1, Ordering::Relaxed);
        let mut m = self.plans.write();
        if m.len() > 20_000 {
            m.clear();
        }
        m.insert((db.to_string(), template.to_string()), plan);
    }

    async fn bump_catalog(&self) -> SqlResult<()> {
        let mut a = Mp::new();
        a.arr(2).str("catalog_version").uint(1);
        self.call(0, "tarn_counter_alloc", &a.0).await?;
        self.reload_catalog().await
    }

    // ---- DDL ----------------------------------------------------------------

    pub async fn create_database(&self, name: &str, if_not_exists: bool) -> SqlResult<u64> {
        if self.catalog().has_db(name) {
            if if_not_exists {
                return Ok(0);
            }
            return Err(SqlError::new(
                1007,
                "HY000",
                format!("Can't create database '{}'; database exists", name),
            ));
        }
        let mut a = Mp::new();
        a.arr(1).str(name);
        lua_status(&self.call(0, "tarn_db_create", &a.0).await?)?;
        self.bump_catalog().await?;
        Ok(1)
    }

    pub async fn drop_database(&self, name: &str, if_exists: bool, lock_timeout: f64) -> SqlResult<u64> {
        let cat = self.catalog();
        if !cat.has_db(name) {
            if if_exists {
                return Ok(0);
            }
            return Err(SqlError::new(
                1008,
                "HY000",
                format!("Can't drop database '{}'; database doesn't exist", name),
            ));
        }
        let tables = cat.tables_in(name);
        let mut n = 0;
        for t in &tables {
            if self.drop_table(&t.db, &t.name, lock_timeout).await? {
                n += 1;
            }
        }
        let mut a = Mp::new();
        a.arr(1).str(name);
        lua_status(&self.call(0, "tarn_db_drop", &a.0).await?)?;
        self.bump_catalog().await?;
        Ok(n)
    }

    pub async fn create_table(
        &self,
        mut def: TableDef,
        if_not_exists: bool,
        auto_increment: Option<u64>,
        lock_timeout: f64,
    ) -> SqlResult<()> {
        let cat = self.catalog();
        if !cat.has_db(&def.db) {
            return Err(SqlError::new(1049, "42000", format!("Unknown database '{}'", def.db)));
        }
        let table_name = def.name.clone();
        let exists = || {
            if if_not_exists {
                Ok(())
            } else {
                Err(SqlError::new(
                    1050,
                    "42S01",
                    format!("Table '{}' already exists", table_name),
                ))
            }
        };
        if cat.get(&def.db, &def.name).is_some() {
            return exists();
        }
        for &c in &def.shard_key {
            let t = def.columns[c].ty();
            if !(t.is_integer() || matches!(t.tarantool_type(), "string" | "varbinary")) {
                return Err(SqlError::unsupported(
                    "shard key columns other than integer and string types",
                ));
            }
        }
        // Space ids: the table's, then one per global index (unique keys
        // without the shard key).
        let globals: Vec<usize> = (0..def.indexes.len())
            .filter(|&i| def.needs_global(&def.indexes[i]))
            .collect();
        let lazies: Vec<usize> = (0..def.indexes.len())
            .filter(|&i| def.indexes[i].lazy.is_some())
            .collect();
        if !lazies.is_empty() {
            self.require_lazy_capabilities().await?;
        }
        let first = self
            .alloc_space_ids(1 + globals.len() as u64 + lazies.len() as u64)
            .await?;
        def.space_id = first;
        for (k, &i) in globals.iter().enumerate() {
            def.indexes[i].global = Some(first + 1 + k as u32);
        }
        for (k, &i) in lazies.iter().enumerate() {
            let l = def.indexes[i].lazy.as_mut().unwrap();
            l.space_id = first + 1 + globals.len() as u32 + k as u32;
            l.ready = true; // published only AFTER bootstrap's all-source barrier
        }
        def.resolve_types()?;
        let ld = def.lua_def();
        // The arguments of the job's steps: {def = the catalog entry}.
        let mut m = Mp::new();
        m.map(1).str("def");
        crate::alter::lua_def_entry(&mut m, &ld, None);
        let name = def.space_name();
        let deadline = std::time::Instant::now() + Duration::from_secs_f64(lock_timeout.max(1.0));
        let job = self
            .job_begin(JobKind::Create, &name, std::slice::from_ref(&name), 0, &m.0, deadline)
            .await?;
        // Another server may have created it before the job began.
        self.reload_catalog().await?;
        if self.catalog().get(&def.db, &def.name).is_some() {
            self.job_end(&job.owner).await?;
            return exists();
        }
        self.run_job(&job).await?;
        if let Some(n) = auto_increment.filter(|&n| n > 1) {
            if def.auto_inc.is_some() {
                self.note_auto_inc(&def, n - 1).await?;
            }
        }
        Ok(())
    }

    pub async fn drop_tables(&self, tables: &[(String, String)], if_exists: bool, lock_timeout: f64) -> SqlResult<()> {
        let cat = self.catalog();
        for (db, t) in tables {
            if cat.get(db, t).is_none() && !if_exists {
                return Err(SqlError::new(1051, "42S02", format!("Unknown table '{}.{}'", db, t)));
            }
        }
        for (db, t) in tables {
            if !self.drop_table(db, t, lock_timeout).await? && !if_exists {
                // dropped by another statement meanwhile
                return Err(SqlError::new(1051, "42S02", format!("Unknown table '{}.{}'", db, t)));
            }
        }
        Ok(())
    }

    /// DROP TABLE of one table, as a job: false if it does not exist.
    async fn drop_table(&self, db: &str, table: &str, lock_timeout: f64) -> SqlResult<bool> {
        let deadline = std::time::Instant::now() + Duration::from_secs_f64(lock_timeout.max(1.0));
        loop {
            let Some(def) = self.catalog().get(db, table) else {
                return Ok(false);
            };
            let lazy_drop: Vec<u32> = def
                .indexes
                .iter()
                .filter_map(|i| i.lazy.as_ref().map(|l| l.space_id))
                .collect();
            if !lazy_drop.is_empty() {
                self.require_lazy_capabilities().await?;
            }
            let ver = self.next_catalog_version().await?;
            let mut a = Mp::new();
            a.map(3 + !lazy_drop.is_empty() as usize)
                .str("ver")
                .uint(ver)
                .str("timeout")
                .f64(lock_timeout)
                .str("id");
            a.uint(def.space_id as u64);
            if !lazy_drop.is_empty() {
                a.str("lazy_drop").arr(lazy_drop.len());
                for id in lazy_drop {
                    a.uint(id as u64);
                }
            }
            let name = def.space_name();
            let job = self
                .job_begin(JobKind::Drop, &name, std::slice::from_ref(&name), ver, &a.0, deadline)
                .await?;
            if !self.job_still(&job, &def).await? {
                continue;
            }
            self.run_job(&job).await?;
            return Ok(true);
        }
    }

    pub async fn truncate(&self, db: &str, table: &str, lock_timeout: f64) -> SqlResult<()> {
        let deadline = std::time::Instant::now() + Duration::from_secs_f64(lock_timeout.max(1.0));
        loop {
            let def = self
                .catalog()
                .get(db, table)
                .ok_or_else(|| SqlError::no_table(db, table))?;
            // The table's definition, under a new version: the shards refuse
            // writes encoded before the TRUNCATE (1412).
            let ver = self.next_catalog_version().await?;
            let mut nd = (*def).clone();
            nd.ver = ver;
            let mut lazy_drop = Vec::new();
            if def.has_lazy() {
                self.require_lazy_capabilities().await?;
                let count = nd.indexes.iter().filter(|i| i.lazy.is_some()).count();
                let first = self.alloc_space_ids(count as u64).await?;
                for (n, i) in nd.indexes.iter_mut().filter(|i| i.lazy.is_some()).enumerate() {
                    let l = i.lazy.as_mut().unwrap();
                    lazy_drop.push(l.space_id);
                    l.space_id = first + n as u32;
                    l.ready = true;
                }
            }
            let mut a = Mp::new();
            a.map(4 + if lazy_drop.is_empty() { 0 } else { 2 })
                .str("ver")
                .uint(ver)
                .str("timeout")
                .f64(lock_timeout)
                .str("id");
            a.uint(def.space_id as u64).str("def");
            crate::alter::catalog_entry(&mut a, &nd, ver);
            if !lazy_drop.is_empty() {
                a.str("lazy_empty").bool(true).str("lazy_drop").arr(lazy_drop.len());
                for id in lazy_drop {
                    a.uint(id as u64);
                }
            }
            let name = def.space_name();
            let job = self
                .job_begin(
                    JobKind::Truncate,
                    &name,
                    std::slice::from_ref(&name),
                    ver,
                    &a.0,
                    deadline,
                )
                .await?;
            if !self.job_still(&job, &def).await? {
                continue;
            }
            return self.run_job(&job).await;
        }
    }

    /// `n` consecutive unused space ids.
    async fn alloc_space_ids(&self, n: u64) -> SqlResult<u32> {
        let mut a = Mp::new();
        a.arr(2).str("space_id").uint(n);
        let d = self.call(0, "tarn_counter_alloc", &a.0).await?;
        let mut rd = Rd::new(&d);
        if rd.array()? != 1 {
            return Err(crate::mpw::proto("invalid space allocation envelope"));
        }
        let start = match rd.value()? {
            Value::UInt(n) => n,
            Value::Int(n) if n >= 0 => n as u64,
            _ => return Err(crate::mpw::proto("invalid space allocation counter")),
        };
        if !rd.buf.is_empty() {
            return Err(crate::mpw::proto("trailing space allocation data"));
        }
        allocated_space_start(start, n)
    }

    /// The current catalog version, plus one: the version of a schema change.
    async fn next_catalog_version(&self) -> SqlResult<u64> {
        let mut a = Mp::new();
        a.arr(2).str("catalog_version").uint(1);
        let d = self.call(0, "tarn_counter_alloc", &a.0).await?;
        let mut rd = Rd::new(&d);
        rd.array()?;
        Ok(rd.value()?.as_i64().unwrap_or(0) as u64)
    }

    /// ALTER TABLE (and RENAME TABLE, CREATE / DROP INDEX), online: see
    /// `alter` and lua/tarn.lua "Schema changes". It runs as a job on the
    /// table (and on its new name), after any other schema change of them.
    pub async fn alter_table(&self, plan: &crate::alter::AlterPlan, lock_timeout: f64) -> SqlResult<()> {
        let deadline = std::time::Instant::now() + Duration::from_secs_f64(lock_timeout.max(1.0));
        loop {
            self.reload_catalog().await?;
            let cat = self.catalog();
            let def = cat
                .get(&plan.db, &plan.table)
                .ok_or_else(|| SqlError::no_table(&plan.db, &plan.table))?;
            if def.bucket_index.is_none() {
                return Err(SqlError::internal(format!(
                    "table {} has no bucket_id index",
                    def.space_name()
                )));
            }
            let mut ch = crate::alter::apply(&def, plan, &cat, &crate::expr::now_string())?;
            if !ch.changed {
                return self.alter_auto_inc(&def, &ch).await;
            }
            if ch.engine_change.is_some() {
                // A replica must be able to resume this job after promotion.
                // Check before allocating identities or publishing any job.
                self.require_engine_capabilities().await?;
                ch.def.counter_id = Some(def.counter_space_id());
                ch.def.space_id = self.alloc_space_ids(1).await?;
                for ix in &mut ch.def.indexes {
                    if let Some(old) = ix.global {
                        ch.uix_drop.push(old);
                        ix.global = Some(0);
                    }
                    if let Some(lazy) = &mut ix.lazy {
                        ch.lazy_drop.push(lazy.space_id);
                        lazy.space_id = 0;
                        lazy.ready = false;
                    }
                }
            }
            let ver = self.next_catalog_version().await?;
            ch.def.ver = ver;
            if def.has_lazy() || ch.def.indexes.iter().any(|i| i.lazy.is_some()) {
                self.require_lazy_capabilities().await?;
            }
            // New unique keys without the shard key: space ids for their
            // global indexes, which the job fills from the existing rows.
            let need: Vec<usize> = (0..ch.def.indexes.len())
                .filter(|&i| ch.def.indexes[i].global == Some(0))
                .collect();
            if !need.is_empty() {
                let first = self.alloc_space_ids(need.len() as u64).await?;
                for (k, &i) in need.iter().enumerate() {
                    ch.def.indexes[i].global = Some(first + k as u32);
                    ch.uix_new.push(first + k as u32);
                }
            }
            let lazy_need: Vec<usize> = (0..ch.def.indexes.len())
                .filter(|&i| ch.def.indexes[i].lazy.as_ref().is_some_and(|l| l.space_id == 0))
                .collect();
            if !lazy_need.is_empty() {
                let first = self.alloc_space_ids(lazy_need.len() as u64).await?;
                for (k, &i) in lazy_need.iter().enumerate() {
                    let l = ch.def.indexes[i].lazy.as_mut().unwrap();
                    l.space_id = first + k as u32;
                    l.ready = true;
                    ch.lazy_new.push(l.space_id);
                }
            }
            ch.def.resolve_globals()?;
            let args = crate::alter::lua_args(&ch, ver, lock_timeout);
            let space = def.space_name();
            let mut names = vec![space.clone()];
            names.extend(ch.rename.iter().cloned());
            let job = self
                .job_begin(JobKind::Alter, &space, &names, ver, &args, deadline)
                .await?;
            if !self.job_still(&job, &def).await? {
                continue;
            }
            self.run_job(&job).await?;
            return self.alter_auto_inc(&ch.def, &ch).await;
        }
    }

    /// Engine replacement changes physical identities. Every potential
    /// leader must understand the owned-copy protocol before it can start.
    async fn require_engine_capabilities(&self) -> SqlResult<()> {
        for shard in self.live_shards() {
            for addr in self.shards[shard].replicas() {
                let client = tarn_iproto::Client::connect(&self.opts(addr))
                    .await
                    .map_err(|e| SqlError::unsupported(format!("engine-copy storage capability unavailable: {e}")))?;
                let response = tokio::time::timeout(
                    Duration::from_secs(5),
                    client.call(0, "tarn_engine_capabilities", &[0x90]),
                )
                .await
                .map_err(|_| SqlError::unsupported("engine-copy storage capability check timed out"))?
                .map_err(|e| {
                    SqlError::unsupported(format!(
                        "online engine changes require engine-copy protocol V1 on every replica ({e})"
                    ))
                })?;
                let valid = response.data().is_some_and(|data| {
                    let mut rd = Rd::new(&data);
                    (|| -> SqlResult<bool> {
                        Ok(rd.array()? == 1
                            && rd.array()? == 2
                            && rd.string()? == "ok"
                            && matches!(rd.value()?, Value::Int(1) | Value::UInt(1))
                            && rd.buf.is_empty())
                    })()
                    .unwrap_or(false)
                });
                if !valid {
                    return Err(SqlError::unsupported(
                        "online engine changes require engine-copy protocol V1 on every replica",
                    ));
                }
            }
        }
        Ok(())
    }

    // ---- schema change jobs (lua/tarn.lua "Jobs") ---------------------------

    pub(crate) fn job_owner(&self) -> String {
        format!(
            "{}-{:08x}-{}",
            self.cfg.node_id,
            self.boot,
            self.txn_seq.fetch_add(1, Ordering::Relaxed)
        )
    }

    /// Begin a job on table `space` and the other `names` it touches,
    /// waiting while another job holds one of them (an abandoned one is
    /// finished first), up to `deadline` (1205).
    async fn job_begin(
        &self,
        kind: JobKind,
        space: &str,
        names: &[String],
        ver: u64,
        args: &[u8],
        deadline: std::time::Instant,
    ) -> SqlResult<Job> {
        let owner = self.job_owner();
        self.job_begin_as(&owner, kind, space, names, ver, args, deadline).await
    }

    /// `job_begin` with the job's owner (`job_owner`) given.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn job_begin_as(
        &self,
        owner: &str,
        kind: JobKind,
        space: &str,
        names: &[String],
        ver: u64,
        args: &[u8],
        deadline: std::time::Instant,
    ) -> SqlResult<Job> {
        let owner = owner.to_string();
        loop {
            let wait = deadline
                .saturating_duration_since(std::time::Instant::now())
                .as_secs_f64()
                .min(2.0);
            let mut m = Mp::new();
            m.arr(2).map(7);
            m.str("owner").str(&owner);
            m.str("kind").str(kind.name());
            m.str("space").str(space);
            m.str("names").arr(names.len());
            for n in names {
                m.str(n);
            }
            m.str("ver").uint(ver);
            m.str("a");
            m.0.extend_from_slice(args);
            m.str("phase").str("prepare");
            m.f64(wait);
            let d = self.call(0, "tarn_job_begin", &m.0).await?;
            if crate::mpw::status_is(&d, "ok") {
                // {'ok', mver}: the first shard's membership version. A job
                // acts on every shard: a newer membership than this server
                // knows (a shard joined) is read first. (Shards join in a
                // job of their own, which other jobs wait for and which
                // ends once every shard has the new membership.)
                if crate::mpw::status_len(&d) > 1 {
                    let (_, mut rd) = lua_status(&d)?;
                    let v = rd.value()?.as_i64().unwrap_or(0).max(0) as u64;
                    if v > self.member_ver() {
                        if let Err(e) = self.load_members().await {
                            let _ = self.job_end(&owner).await;
                            return Err(e);
                        }
                    }
                }
                return Ok(Job {
                    owner,
                    age: 0.0,
                    kind,
                    space: space.to_string(),
                    phase: "prepare".to_string(),
                    args: args.to_vec(),
                    gen: 1,
                });
            }
            // {'busy', owner, age} of the job in the way
            let mut rd = Rd::new(&d);
            rd.array()?;
            let n = rd.array()?;
            let _busy = rd.string()?;
            let other = if n > 1 { rd.string()? } else { String::new() };
            let age = if n > 2 {
                rd.value()?.as_f64().unwrap_or(0.0)
            } else {
                0.0
            };
            if age > self.cfg.ddl_job_stale && !other.is_empty() {
                self.finish_job(&other).await;
                continue;
            }
            if std::time::Instant::now() >= deadline {
                return Err(SqlError::new(
                    1205,
                    "HY000",
                    if kind == JobKind::Move {
                        "Lock wait timeout exceeded; try restarting transaction (a schema change or another move of the bucket is running)"
                    } else {
                        "Lock wait timeout exceeded; try restarting transaction (another schema change of the table, or a bucket move, is running)"
                    },
                ));
            }
            if wait <= 0.0 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }

    /// After `job` began: is `def` still the table's definition? Another
    /// schema change may have finished between reading the catalog and the
    /// job's start; then the job ends and the statement starts over.
    async fn job_still(&self, job: &Job, def: &TableDef) -> SqlResult<bool> {
        self.reload_catalog().await?;
        let same = self
            .catalog()
            .get(&def.db, &def.name)
            .is_some_and(|d| d.space_id == def.space_id && d.ver == def.ver);
        if !same {
            self.job_end(&job.owner).await?;
        }
        Ok(same)
    }

    pub(crate) async fn job_end(&self, owner: &str) -> SqlResult<()> {
        let mut a = Mp::new();
        a.arr(1).str(owner);
        lua_status(&self.call(0, "tarn_job_end", &a.0).await?).map(|_| ())
    }

    /// Record the job's arguments (a move learns them once it holds its
    /// bucket).
    pub(crate) async fn job_args(&self, job: &Job) -> SqlResult<()> {
        let mut a = Mp::new();
        a.arr(2).str(&job.owner);
        a.0.extend_from_slice(&job.args);
        let d = self.call(0, "tarn_job_args", &a.0).await?;
        if crate::mpw::status_is(&d, "ok") {
            return Ok(());
        }
        Err(SqlError::internal(format!(
            "{} of {}: the job is gone",
            job.kind.statement(),
            job.space
        )))
    }

    /// Run `work` while keeping the job of `owner` alive (`tarn_job_touch`
    /// through whichever node leads the first shard now, so that a leader
    /// change there does not let another server take a live job over).
    pub(crate) async fn with_job_alive<T>(&self, owner: &str, work: impl std::future::Future<Output = T>) -> T {
        let every = Duration::from_secs_f64((self.cfg.ddl_job_stale / 3.0).clamp(0.05, 5.0));
        let mut t = Mp::new();
        t.arr(1).str(owner);
        let touch = async {
            loop {
                tokio::time::sleep(every).await;
                if let Err(e) = self.call(0, "tarn_job_touch", &t.0).await {
                    tracing::debug!("keeping job {} alive: {}", owner, e.message);
                }
            }
        };
        tokio::pin!(work);
        tokio::select! {
            r = &mut work => r,
            _ = touch => unreachable!("the keep-alive loop never ends"),
        }
    }

    /// Record the job's decision: from now on, whoever finishes it goes
    /// this way.
    pub(crate) async fn job_phase(&self, job: &Job, phase: &str) -> SqlResult<()> {
        let mut a = Mp::new();
        a.arr(2).str(&job.owner).str(phase);
        let d = self.call(0, "tarn_job_phase", &a.0).await?;
        if crate::mpw::status_is(&d, "ok") {
            return Ok(());
        }
        Err(SqlError::new(
            1180,
            "40003",
            format!(
                "{} of {}: another server took the schema change over; it finishes it",
                job.kind.statement(),
                job.space
            ),
        ))
    }

    /// The jobs in progress.
    pub(crate) async fn jobs(&self) -> SqlResult<Vec<Job>> {
        let d = self.call(0, "tarn_jobs", &[0x90]).await?;
        let mut rd = Rd::new(&d);
        rd.array()?;
        let n = rd.array()?;
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            // {owner, age, kind, space, phase, ver, a, names}
            let k = rd.array()?;
            let owner = rd.string()?;
            let age = rd.value()?.as_f64().unwrap_or(0.0);
            let kind = rd.string()?;
            let space = rd.string()?;
            let phase = rd.string()?;
            let _ver = rd.value()?;
            let args = rd.raw_value()?.to_vec();
            for _ in 7..k {
                rd.raw_value()?;
            }
            if let Some(kind) = JobKind::parse(&kind) {
                out.push(Job {
                    owner,
                    age,
                    kind,
                    space,
                    phase,
                    args,
                    gen: 0,
                });
            }
        }
        Ok(out)
    }

    /// Take over the job of `owner` if it is still abandoned, and finish it
    /// (failures are logged: the next look tries again).
    async fn finish_job(&self, owner: &str) {
        let r: SqlResult<()> = async {
            let Some(mut job) = self.jobs().await?.into_iter().find(|j| j.owner == owner) else {
                return Ok(());
            };
            if job.age <= self.cfg.ddl_job_stale {
                return Ok(());
            }
            let me = self.job_owner();
            let mut t = Mp::new();
            t.arr(2).str(&me).str(owner);
            let d = self.call(0, "tarn_job_touch", &t.0).await?;
            if !crate::mpw::status_is(&d, "ok") {
                return Ok(()); // another server took it over
            }
            // {'ok', generation}: this run's
            let mut rd = Rd::new(&d);
            rd.array()?;
            if rd.array()? > 1 {
                rd.skip()?;
                job.gen = rd.value()?.as_i64().unwrap_or(0) as u64;
            }
            tracing::warn!(
                "finishing the {} of {} that {} left in phase {}",
                job.kind.statement(),
                job.space,
                owner,
                job.phase
            );
            job.owner = me;
            // (The job acts on every shard: this server may not know them
            // all yet. No shard joins while the job is in progress.)
            self.load_members().await?;
            if job.kind == JobKind::Move {
                return self.finish_move(&job).await;
            }
            if job.kind == JobKind::Members {
                return self.finish_members(&job).await;
            }
            match self.run_job(&job).await {
                // (a job rolled back reports the reason as an error)
                Err(e) if e.code != 1180 => {
                    tracing::warn!("{} of {} rolled back: {}", job.kind.statement(), job.space, e.message);
                    Ok(())
                }
                r => r,
            }
        }
        .await;
        if let Err(e) = r {
            tracing::warn!("finishing an abandoned schema change: {}", e);
        }
    }

    /// Finish the schema changes whose servers stopped (at startup, and
    /// periodically: every ddl_job_stale / 2, at most every 10 s).
    pub async fn finish_stale_jobs(&self) -> SqlResult<()> {
        for job in self.jobs().await? {
            if job.age > self.cfg.ddl_job_stale {
                self.finish_job(&job.owner).await;
            }
        }
        Ok(())
    }

    /// Run a job to its end, keeping it alive meanwhile.
    async fn run_job(&self, job: &Job) -> SqlResult<()> {
        self.with_job_alive(&job.owner, self.run_job_steps(job)).await
    }

    /// BUILD everywhere (ALTER TABLE: new indexes, while writes go on); then
    /// PREPARE everywhere; then, recorded in the job first, APPLY everywhere,
    /// or ABORT everywhere if a shard did not prepare; then publish the new
    /// catalog. Every step may be repeated: a job that another server
    /// finishes runs again from its phase. A job applied on some shard can
    /// only go forward.
    async fn run_job_steps(&self, job: &Job) -> SqlResult<()> {
        // (name, a, generation): ALTER TABLE's PREPARE records the
        // generation, whose run alone may fill new global indexes; other
        // steps ignore it.
        let mut call = Mp::new();
        call.arr(3).str(&job.space);
        call.0.extend_from_slice(&job.args);
        call.uint(job.gen);
        // (every shard in the cluster; the first shard is never removed)
        let live: Vec<usize> = self.live_shards().collect();
        let step = |i: usize, proc: String| {
            let call = &call;
            async move {
                self.call(i, &proc, &call.0)
                    .await
                    .and_then(|d| lua_status(&d).map(|_| d.clone()))
            }
        };
        let mut failed: Option<SqlError> = None;
        let mut phase = job.phase.as_str();
        if phase == "prepare" || phase == "run" {
            if job.kind == JobKind::Alter {
                for r in futures::future::join_all(live.iter().map(|&i| step(i, job.kind.proc("build")))).await {
                    match r {
                        Ok(_) => {}
                        // a storage node from before BUILD existed: its PREPARE builds them
                        Err(e) if e.message.contains("'tarn_alter_build' is not defined") => {}
                        Err(e) => {
                            failed.get_or_insert(e);
                        }
                    }
                }
            }
            let mut applied = false;
            if failed.is_none() {
                for r in futures::future::join_all(live.iter().map(|&i| step(i, job.kind.proc("prepare")))).await {
                    match r {
                        Ok(d) => applied |= applied_reply(&d),
                        Err(e) => {
                            failed.get_or_insert(e);
                        }
                    }
                }
            }
            // New global indexes are filled once every shard has prepared:
            // the table's writes are fenced everywhere, so the rows stay put.
            if failed.is_none() && !applied && job.kind == JobKind::Alter {
                if let Err(e) = self.fill_globals(&job.args, job.gen).await {
                    failed = Some(e);
                }
            }
            if failed.is_none() && !applied && matches!(job.kind, JobKind::Alter | JobKind::Create | JobKind::Truncate)
            {
                if let Err(e) = self.bootstrap_lazy(job, &call.0, &live).await {
                    failed = Some(e);
                }
            }
            phase = if failed.is_some() && !applied { "abort" } else { "apply" };
            self.job_phase(job, phase).await?;
        }
        if phase == "abort" {
            for r in futures::future::join_all(live.iter().map(|&i| step(i, job.kind.proc("abort")))).await {
                if let Err(e) = r {
                    // The job stays: whoever finishes it aborts again.
                    tracing::warn!("{} of {}: abort: {}", job.kind.statement(), job.space, e);
                    return Err(failed.unwrap_or(e));
                }
            }
            self.job_end(&job.owner).await?;
            return Err(failed.unwrap_or_else(|| {
                SqlError::new(
                    1213,
                    "40001",
                    format!("{} of {} was rolled back", job.kind.statement(), job.space),
                )
            }));
        }
        // CREATE TABLE names the table in the first shard's catalog, which
        // the SQL servers read, last.
        let groups: Vec<Vec<usize>> = if job.kind == JobKind::Create && live.len() > 1 {
            vec![live.iter().copied().filter(|&i| i != 0).collect(), vec![0]]
        } else {
            vec![live.clone()]
        };
        for g in groups {
            for r in futures::future::join_all(g.iter().map(|&i| step(i, job.kind.proc("apply")))).await {
                if let Err(e) = r {
                    // Some shards may have applied it: the job stays, and a
                    // server finishes it (finish_stale_jobs).
                    return Err(SqlError::new(
                        1180,
                        "40003",
                        format!(
                            "{} did not finish on every shard ({}); it completes in the background",
                            job.kind.statement(),
                            e.message
                        ),
                    ));
                }
            }
        }
        self.bump_catalog().await?;
        self.job_end(&job.owner).await
    }

    /// Fill the new global indexes of an ALTER TABLE (`uix` in its job
    /// arguments) from the table's rows on every shard, read a page at a
    /// time and written in batches to the entries' shards. A value two rows
    /// hold fails the change with 1062. Runs while every shard fences the
    /// table's writes, so every fill of the change (again after a takeover)
    /// writes the same entries.
    async fn fill_globals(&self, args: &[u8], gen: u64) -> SqlResult<()> {
        let (ids, json, sid, ver) = alter_uix_args(args)?;
        if ids.is_empty() {
            return Ok(());
        }
        let mut nd = catalog::parse_def_json(&json)?;
        nd.space_id = sid;
        nd.resolve_globals()?;
        let globals: Vec<&catalog::GlobalIndex> =
            nd.globals.iter().filter(|g| ids.contains(&g.table.space_id)).collect();
        // (No emptying first: every fill of this change writes the same
        // entries, see tarn_uix_fill.)
        const PAGE: u32 = 1000;
        // per global index and target shard: (entry, the row's key values shown in 1062)
        let mut pending: Vec<HashMap<usize, Vec<(Vec<Value>, String)>>> = vec![HashMap::new(); globals.len()];
        for shard in self.live_shards().collect::<Vec<_>>() {
            let client = self.shards[shard].client(self).await?;
            let mut after: Option<Vec<Value>> = None;
            loop {
                let mut key = Mp::new();
                let iter = match &after {
                    Some(k) => {
                        key.vals(k);
                        tarn_iproto::Iter::Gt
                    }
                    None => {
                        key.arr(0);
                        tarn_iproto::Iter::All
                    }
                };
                let r = client
                    .select(0, sid, 0, iter, 0, PAGE, &key.0)
                    .await
                    .map_err(from_iproto)?;
                let mut n = 0;
                for tup in r.tuples().map_err(crate::mpw::proto)? {
                    let row = Rd::shared(&tup).row_of(&nd)?;
                    n += 1;
                    for (gi, g) in globals.iter().enumerate() {
                        if let Some(e) = g.entry(&nd, &row) {
                            let shown: Vec<String> = g.src[..g.key_len()].iter().map(|&c| row[c].to_string()).collect();
                            let to = self.shard_of(&e[..g.key_len()]);
                            let batch = pending[gi].entry(to).or_default();
                            batch.push((e, shown.join("-")));
                            if batch.len() >= PAGE as usize {
                                let b = std::mem::take(batch);
                                self.fill_batch(g, &nd, ver, gen, to, b).await?;
                            }
                        }
                    }
                    after = Some(nd.pk_key(&row));
                }
                if n < PAGE as usize {
                    break;
                }
            }
        }
        for (gi, g) in globals.iter().enumerate() {
            for (to, b) in std::mem::take(&mut pending[gi]) {
                if !b.is_empty() {
                    self.fill_batch(g, &nd, ver, gen, to, b).await?;
                }
            }
        }
        Ok(())
    }

    async fn fill_batch(
        &self,
        g: &catalog::GlobalIndex,
        t: &TableDef,
        ver: u64,
        gen: u64,
        shard: usize,
        batch: Vec<(Vec<Value>, String)>,
    ) -> SqlResult<()> {
        let k = g.key_len();
        let mut m = Mp::new();
        m.arr(5).uint(g.table.space_id as u64).arr(batch.len());
        for (e, _) in &batch {
            g.table.encode_tuple(e, self.bucket_of(&e[..k]) as u64, &mut m);
        }
        m.uint(t.space_id as u64).uint(ver).uint(gen);
        let d = self.call(shard, "tarn_uix_fill", &m.0).await?;
        if crate::mpw::status_is(&d, "duplicate") {
            let mut rd = Rd::new(&d);
            rd.array()?;
            rd.array()?;
            rd.skip()?;
            let i = rd.value()?.as_i64().unwrap_or(1).max(1) as usize;
            let shown = batch.get(i - 1).map(|(_, s)| s.as_str()).unwrap_or("");
            return Err(SqlError::dup_key(shown, &t.indexes[g.pos].name));
        }
        lua_status(&d).map(|_| ())
    }

    /// AUTO_INCREMENT = n, and a column that became AUTO_INCREMENT: the
    /// generator continues after both.
    async fn alter_auto_inc(&self, def: &TableDef, ch: &crate::alter::Change) -> SqlResult<()> {
        let fresh = self.catalog();
        let t = fresh.get(&def.db, &def.name).unwrap_or_else(|| Arc::new(def.clone()));
        if t.auto_inc.is_none() {
            return Ok(());
        }
        let mut floor = ch.auto_increment.map(|n| n.saturating_sub(1)).unwrap_or(0);
        if let Some(ix) = ch.auto_inc_from {
            let ai = t.auto_inc.unwrap();
            for i in self.live_shards().collect::<Vec<_>>() {
                let c = self.shards[i].client(self).await?;
                let r = c
                    .select(0, t.space_id, ix, tarn_iproto::Iter::Le, 0, 1, &[0x90])
                    .await
                    .map_err(from_iproto)?;
                for tup in r.tuples().map_err(crate::mpw::proto)? {
                    let row = Rd::shared(&tup).row_of(&t)?;
                    if let Some(v) = row[ai].as_i64() {
                        floor = floor.max(v.max(0) as u64);
                    }
                }
            }
        }
        if floor > 0 {
            self.note_auto_inc(&t, floor).await?;
        }
        Ok(())
    }

    /// Next AUTO_INCREMENT value(s) for a table; allocated from a counter on
    /// shard 1 in batches so most inserts never leave this server.
    pub async fn next_auto_inc(&self, t: &TableDef, n: u64) -> SqlResult<u64> {
        self.next_counter(format!("ai:{}", t.counter_space_id()), n).await
    }

    /// Next hidden row id(s) of a table without a primary key, from its own
    /// counter: InnoDB's DB_ROW_ID, never reused.
    pub async fn next_row_id(&self, t: &TableDef, n: u64) -> SqlResult<u64> {
        self.next_counter(format!("rid:{}", t.counter_space_id()), n).await
    }

    async fn next_counter(&self, name: String, n: u64) -> SqlResult<u64> {
        {
            let mut m = self.auto_inc.lock();
            if let Some((next, end)) = m.get_mut(&name) {
                if *next + n - 1 <= *end {
                    let v = *next;
                    *next += n;
                    return Ok(v);
                }
            }
        }
        let batch = n.max(1000);
        let mut a = Mp::new();
        a.arr(2).str(&name).uint(batch);
        let d = self.call(0, "tarn_counter_alloc", &a.0).await?;
        let mut rd = Rd::new(&d);
        rd.array()?;
        let start = rd.value()?.as_i64().unwrap_or(1) as u64;
        let mut m = self.auto_inc.lock();
        m.insert(name, (start + n, start + batch - 1));
        Ok(start)
    }

    /// An explicit value was inserted into an AUTO_INCREMENT column: make
    /// sure future generated values are larger.
    pub async fn note_auto_inc(&self, t: &TableDef, v: u64) -> SqlResult<()> {
        let name = format!("ai:{}", t.counter_space_id());
        {
            let mut m = self.auto_inc.lock();
            if let Some((next, end)) = m.get_mut(&name) {
                if v < *next {
                    return Ok(());
                }
                if v < *end {
                    // Inside this server's reserved block: skip past it, as a
                    // single MySQL server would (next value v + 1).
                    *next = v + 1;
                    return Ok(());
                }
            }
        }
        let mut a = Mp::new();
        a.arr(2).str(&name).uint(v);
        self.call(0, "tarn_counter_bump", &a.0).await?;
        self.auto_inc.lock().remove(&name);
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod wire_test {
    use super::{Mp, Rd};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    pub async fn request(sock: &mut TcpStream) -> (u64, u64, u64, Vec<u8>) {
        assert_eq!(sock.read_u8().await.unwrap(), 0xce);
        let size = sock.read_u32().await.unwrap();
        assert!(size < 64 * 1024);
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

    pub async fn reply(sock: &mut TcpStream, sync: u64, code: u64, body: &[u8]) {
        let mut packet = Mp::new();
        packet.map(2).uint(0).uint(code).uint(1).uint(sync).raw(body);
        sock.write_u8(0xce).await.unwrap();
        sock.write_u32(packet.0.len() as u32).await.unwrap();
        sock.write_all(&packet.0).await.unwrap();
    }

    pub async fn accept(listener: &TcpListener) -> TcpStream {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut greeting = [b' '; 128];
        let first = b"Tarantool 3.9.0 (Binary)";
        greeting[..first.len()].copy_from_slice(first);
        greeting[63] = b'\n';
        greeting[64..108].copy_from_slice(b"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=");
        greeting[127] = b'\n';
        sock.write_all(&greeting).await.unwrap();
        let (kind, sync, stream, _) = request(&mut sock).await;
        assert_eq!((kind, stream), (73, 0));
        let mut body = Mp::new();
        body.map(2).uint(0x54).uint(6).uint(0x55).arr(2).uint(0).uint(1);
        reply(&mut sock, sync, 0, &body.0).await;
        sock
    }
}
#[cfg(test)]
#[path = "cluster_fallback_tests.rs"]
mod fallback_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strong_read_capability_requires_supported_complete_contract() {
        let mut good = Mp::new();
        good.arr(1).arr(2).str("strong_reads").uint(1);
        assert!(strong_read_contract(&good.0));
        for n in 0..good.0.len() {
            assert!(!strong_read_contract(&good.0[..n]));
        }
        good.nil();
        assert!(!strong_read_contract(&good.0));
        for version in [0, 2] {
            let mut other = Mp::new();
            other.arr(1).arr(2).str("strong_reads").uint(version);
            assert!(!strong_read_contract(&other.0));
        }
        let mut old = Mp::new();
        old.arr(1).arr(1).str("lua");
        assert!(!strong_read_contract(&old.0));
        let mut wrong_type = Mp::new();
        wrong_type.arr(1).arr(2).str("strong_reads").str("1");
        assert!(!strong_read_contract(&wrong_type.0));
    }

    #[tokio::test]
    async fn strong_read_contract_is_negotiated_over_tcp_and_retires_rejected_sockets() {
        use super::wire_test::{accept, reply, request};
        use tokio::io::AsyncReadExt;
        use tokio::net::TcpListener;

        // Supported, missing procedure, unsupported version, wrong shape,
        // transport loss, and a capability RPC that never replies.
        for case in 0..6 {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let peer = tokio::spawn(async move {
                let mut sock = accept(&listener).await;
                let (kind, sync, stream, body) = request(&mut sock).await;
                assert_eq!((kind, stream), (10, 0));
                let mut rd = Rd::new(&body);
                assert_eq!(rd.map().unwrap(), 2);
                assert_eq!(rd.value().unwrap().as_i64(), Some(0x22));
                assert_eq!(rd.string().unwrap(), "tarn_read_contract");
                assert_eq!(rd.value().unwrap().as_i64(), Some(0x21));
                assert_eq!(rd.array().unwrap(), 0);
                assert!(rd.buf.is_empty());
                if case == 4 {
                    return; // no response: discovery cannot infer support
                }
                if case != 5 {
                    let mut body = Mp::new();
                    let code = if case == 1 {
                        body.map(1).uint(0x31).str("missing tarn_read_contract");
                        0x8000 | tarn_iproto::errcode::ER_NO_SUCH_PROC as u64
                    } else {
                        body.map(1)
                            .uint(0x30)
                            .arr(if case == 3 { 2 } else { 1 })
                            .arr(2)
                            .str("strong_reads")
                            .uint(if case == 2 { 2 } else { 1 });
                        if case == 3 {
                            body.nil();
                        }
                        0
                    };
                    reply(&mut sock, sync, code, &body.0).await;
                }
                assert!(tokio::time::timeout(Duration::from_secs(7), sock.read_u8())
                    .await
                    .unwrap()
                    .is_err());
            });
            let client = Client::connect(&Opts::new(addr.to_string())).await.unwrap();
            let result = require_strong_reads(&client, false).await;
            assert_eq!(result.is_ok(), case == 0);
            if case == 0 {
                assert!(!client.is_closed());
                client.close();
            } else {
                assert!(client.is_closed());
                assert!(result.unwrap_err().message.contains("strong-read protocol V1"));
            }
            tokio::time::timeout(Duration::from_secs(8), peer)
                .await
                .unwrap()
                .unwrap();
        }
    }

    #[tokio::test]
    async fn completion_discovery_negotiates_capability_through_reserved_admission() {
        use super::wire_test::{accept, reply, request};
        use tokio::io::AsyncReadExt;
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            let mut sock = accept(&listener).await;
            let (kind, held_sync, stream, _) = request(&mut sock).await;
            assert_eq!((kind, stream), (10, 0));
            let (kind, sync, stream, body) = request(&mut sock).await;
            assert_eq!((kind, stream), (10, 0));
            let mut rd = Rd::new(&body);
            assert_eq!(rd.map().unwrap(), 2);
            assert_eq!(rd.value().unwrap().as_i64(), Some(0x22));
            assert_eq!(rd.string().unwrap(), "tarn_compatibility");
            let mut body = Mp::new();
            body.map(1)
                .uint(0x30)
                .arr(1)
                .arr(8)
                .str("ok")
                .uint(1)
                .uint(1)
                .uint(1)
                .str("3.9.0")
                .uint(14)
                .str("ready")
                .f64(120.0);
            reply(&mut sock, sync, 0, &body.0).await;
            let (kind, sync, stream, body) = request(&mut sock).await;
            assert_eq!((kind, stream), (10, 0));
            let mut rd = Rd::new(&body);
            assert_eq!(rd.map().unwrap(), 2);
            assert_eq!(rd.value().unwrap().as_i64(), Some(0x22));
            assert_eq!(rd.string().unwrap(), "tarn_read_contract");
            let mut body = Mp::new();
            body.map(1).uint(0x30).arr(1).arr(2).str("strong_reads").uint(1);
            reply(&mut sock, sync, 0, &body.0).await;
            let mut body = Mp::new();
            body.map(1).uint(0x30).arr(0);
            reply(&mut sock, held_sync, 0, &body.0).await;
            assert!(tokio::time::timeout(Duration::from_secs(3), sock.read_u8())
                .await
                .unwrap()
                .is_err());
        });
        let limits = tarn_iproto::Limits {
            requests: 2,
            bytes: 4096,
            reserved_requests: 1,
            reserved_bytes: 512,
        };
        let budget = tarn_iproto::Budget::new("gateway", limits).unwrap();
        let opts = Opts {
            request_limits: limits,
            admission: vec![budget.clone()],
            ..Opts::new(addr.to_string())
        };
        let client = Client::connect(&opts).await.unwrap();
        let held = client.call(0, "held", &[0x90]);
        tokio::pin!(held);
        assert!(futures::poll!(held.as_mut()).is_pending());
        assert_eq!(budget.stats().normal_pending, 1);
        assert!(matches!(
            client.call(0, "ordinary", &[0x90]).await,
            Err(tarn_iproto::Error::Admission { .. })
        ));
        tokio::time::timeout(Duration::from_secs(2), async {
            crate::compat::admit(&client, 1, 120.0, true).await?;
            require_strong_reads(&client, true).await
        })
        .await
        .unwrap()
        .unwrap();
        held.await.unwrap();
        assert_eq!(budget.stats().pending, 0);
        assert!(!client.is_closed());
        client.close();
        peer.await.unwrap();
    }

    #[test]
    fn space_allocations_never_wrap_or_reuse_an_incarnation() {
        assert_eq!(allocated_space_start(1, 1).unwrap(), 10001);
        assert_eq!(
            allocated_space_start(MAX_SPACE_ID - 10000, 1).unwrap(),
            MAX_SPACE_ID as u32
        );
        assert_eq!(
            allocated_space_start(MAX_SPACE_ID - 10001, 2).unwrap(),
            MAX_SPACE_ID as u32 - 1
        );
        for (start, n) in [
            (0, 0),
            (0, 1),
            (MAX_SPACE_ID - 10000, 2),
            (MAX_SPACE_ID - 9999, 1),
            (u64::MAX, 1),
            (1, u64::MAX),
        ] {
            assert!(allocated_space_start(start, n).is_err());
        }
    }

    fn w(shard: usize, waiter: &str, id: u64, holders: &[&str], held: &[usize]) -> Wait {
        Wait {
            shard,
            waiter: waiter.into(),
            id,
            holders: holders.iter().map(|h| h.to_string()).collect(),
            held: held.to_vec(),
        }
    }

    fn members(c: &[Wait]) -> Vec<String> {
        let mut m: Vec<String> = c.iter().map(|w| w.waiter.clone()).collect();
        m.sort();
        m
    }

    /// Run a walk over simulated shards; returns it and the shards of each step.
    fn walk(starts: &[(&str, usize, &[usize])], shards: &[Vec<Wait>]) -> (Walk, Vec<Vec<usize>>) {
        let starts: Vec<(String, usize, Vec<usize>)> =
            starts.iter().map(|(t, s, h)| (t.to_string(), *s, h.to_vec())).collect();
        let mut wk = Walk::new(&starts);
        let mut steps = Vec::new();
        loop {
            let step = wk.step();
            if step.is_empty() {
                break;
            }
            let got = step.iter().flat_map(|&s| shards[s].clone()).collect();
            steps.push(step);
            wk.add(got);
        }
        (wk, steps)
    }

    #[test]
    fn finds_cycles_through_shards() {
        // A waits on shard 0 for B, B on shard 1 for A: one cycle, both waits.
        let c = wait_cycles(&[w(0, "A", 1, &["B"], &[]), w(1, "B", 7, &["A"], &[])]);
        assert_eq!(c.len(), 1);
        assert_eq!(members(&c[0]), ["A", "B"]);
        assert!(c[0].iter().any(|x| x.shard == 0 && x.id == 1) && c[0].iter().any(|x| x.shard == 1 && x.id == 7));
        // A chain is not a cycle.
        assert!(wait_cycles(&[w(0, "A", 1, &["B"], &[]), w(1, "B", 2, &["C"], &[])]).is_empty());
        // Three transactions, with a bystander waiting on the cycle.
        let c = wait_cycles(&[
            w(0, "A", 1, &["B"], &[]),
            w(1, "B", 2, &["C"], &[]),
            w(0, "C", 3, &["A", "D"], &[]),
            w(1, "E", 4, &["A"], &[]),
        ]);
        assert_eq!(c.len(), 1);
        assert_eq!(members(&c[0]), ["A", "B", "C"]);
        // Each wait of a cycle waits for the next one's waiter.
        for (i, x) in c[0].iter().enumerate() {
            assert!(x.holders.contains(&c[0][(i + 1) % c[0].len()].waiter));
        }
        // Two separate cycles are both reported, each once.
        let c = wait_cycles(&[
            w(0, "A", 1, &["B"], &[]),
            w(1, "B", 2, &["A"], &[]),
            w(0, "C", 3, &["D"], &[]),
            w(1, "D", 4, &["C"], &[]),
        ]);
        let mut all: Vec<Vec<String>> = c.iter().map(|x| members(x)).collect();
        all.sort();
        assert_eq!(all, vec![vec!["A", "B"], vec!["C", "D"]]);
    }

    #[test]
    fn walk_follows_waits_backwards() {
        // A (this server's) waits on shard 0 for B and holds locks on shard
        // 1. D waits for A on shard 1, C for D on shard 2, B for C on shard
        // 3: a cycle through four shards. X waits on shard 1 for Y, which
        // nobody here waits for, and E waits for F on shard 4.
        let shards = vec![
            vec![w(0, "A", 1, &["B"], &[1])],
            vec![w(1, "D", 2, &["A"], &[2]), w(1, "X", 3, &["Y"], &[5])],
            vec![w(2, "C", 4, &["D"], &[3])],
            vec![w(3, "B", 5, &["C"], &[0])],
            vec![w(4, "E", 6, &["F"], &[])],
            vec![],
        ];
        let (wk, steps) = walk(&[("A", 0, &[1])], &shards);
        // Shards where A waits or holds locks, then where D, then C holds
        // locks; B holds locks only where the walk has been.
        assert_eq!(steps, vec![vec![0, 1], vec![2], vec![3]]);
        let reached: BTreeSet<&str> = wk.reached.iter().map(|s| s.as_str()).collect();
        assert_eq!(reached, ["A", "B", "C", "D"].into_iter().collect());
        let c = wait_cycles(&wk.waits);
        assert_eq!(c.len(), 1);
        assert_eq!(members(&c[0]), ["A", "B", "C", "D"]);
    }

    #[test]
    fn walk_follows_queued_requests() {
        // U (this server's) waits on shard 0 for H; V is queued behind U
        // there and holds locks on shard 1, where W waits for V.
        let shards = vec![
            vec![w(0, "U", 1, &["H"], &[]), w(0, "V", 2, &["H", "U"], &[1])],
            vec![w(1, "W", 3, &["V"], &[2])],
            vec![],
        ];
        let (wk, steps) = walk(&[("U", 0, &[])], &shards);
        assert_eq!(steps, vec![vec![0], vec![1], vec![2]]);
        assert!(wk.reached.contains("W"));
        assert!(wait_cycles(&wk.waits).is_empty());
    }

    #[test]
    fn walk_without_waiters() {
        // Nobody waits for A: only the shards where it waits or holds locks
        // are visited, once.
        let shards = vec![
            vec![],
            vec![w(1, "A", 1, &["B"], &[0])],
            vec![w(2, "C", 2, &["D"], &[])],
        ];
        let (wk, steps) = walk(&[("A", 1, &[0])], &shards);
        assert_eq!(steps, vec![vec![0, 1]]);
        assert_eq!(wk.reached.len(), 1);
        // Several starts share one step per shard.
        let (_, steps) = walk(&[("A", 1, &[0]), ("C", 2, &[0, 1])], &shards);
        assert_eq!(steps, vec![vec![0, 1, 2]]);
    }

    #[test]
    fn second_collection_needs_the_same_edges() {
        let cyc = vec![w(0, "A", 1, &["B"], &[1]), w(1, "B", 7, &["A"], &[0])];
        assert!(still_deadlocked(&cyc, &cyc));
        // Each wait is matched on its own shard.
        assert!(still_deadlocked(
            &cyc,
            &[w(1, "B", 7, &["A"], &[]), w(0, "A", 1, &["B", "C"], &[])]
        ));
        // A wait that ended (a later wait of A has another id).
        assert!(!still_deadlocked(
            &cyc,
            &[w(0, "A", 2, &["B"], &[1]), w(1, "B", 7, &["A"], &[0])]
        ));
        // A still waits with the same id, but no longer for B (B released
        // the lock, and A waits for another holder): no proof of a cycle.
        assert!(!still_deadlocked(
            &cyc,
            &[w(0, "A", 1, &["C"], &[1]), w(1, "B", 7, &["A"], &[0])]
        ));
        assert!(!still_deadlocked(&cyc, &[w(0, "A", 1, &["B"], &[1])]));
        assert!(!still_deadlocked(
            &cyc,
            &[w(1, "A", 1, &["B"], &[1]), w(1, "B", 7, &["A"], &[0])]
        ));
    }

    #[test]
    fn parses_lock_waits() {
        // CALL reply: [[wait, ...]]; shard ids 1 and 2 are positions 0 and 1.
        let mut m = Mp::new();
        m.arr(1).arr(3);
        m.arr(4).str("A").uint(5).arr(1).str("B").arr(3).uint(2).uint(9).uint(1);
        m.arr(3).str("C").uint(6).arr(2).str("D").str("E");
        m.arr(5).str("F").uint(7).arr(0).arr(1).uint(1).str("later field");
        let pos: HashMap<u32, usize> = [(1, 0), (2, 1)].into_iter().collect();
        let v = parse_waits(&m.0, 1, &pos).unwrap();
        // An unknown shard id is left out; a storage node that sends no held
        // shards (an older one) gives an empty list; later fields are skipped.
        assert_eq!(
            v,
            vec![
                w(1, "A", 5, &["B"], &[1, 0]),
                w(1, "C", 6, &["D", "E"], &[]),
                w(1, "F", 7, &[], &[0]),
            ]
        );
    }
}
