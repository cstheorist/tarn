//! Cluster topology, connections to shard leaders, routing, catalog cache,
//! DDL broadcast and AUTO_INCREMENT allocation. One `Cluster` is shared by
//! every client session of a Tarn server.

use crate::catalog::{self, Catalog, TableDef};
use crate::mpw::{from_iproto, lua_status, Mp, Rd};
use crate::route;
use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tarn_common::{SqlError, SqlResult, Value};
use tarn_iproto::{Client, Opts};

fn default_buckets() -> u32 {
    3000
}
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
    pub shards: Vec<ShardConfig>,
    #[serde(default = "default_buckets")]
    pub bucket_count: u32,
    #[serde(default = "default_user")]
    pub user: String,
    #[serde(default = "default_user")]
    pub password: String,
    /// Connections per shard leader (streams are multiplexed on them).
    #[serde(default = "default_pool")]
    pub pool_size: usize,
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
}

fn default_deadlock_interval() -> f64 {
    1.0
}

/// A schema change whose owner has not touched it for this long (seconds)
/// is taken over and finished by another server.
const DDL_JOB_STALE: f64 = 30.0;

/// A schema change in progress (the job record on the first shard).
struct DdlJob {
    owner: String,
    age: f64,
    space: String,
    /// msgpack argument of tarn_alter_prepare / _apply / _abort
    args: Vec<u8>,
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
}

impl Shard {
    pub async fn client(&self, cluster: &Cluster) -> SqlResult<Client> {
        if let Some(p) = self.leader.read().clone() {
            if let Some(c) = p.pick() {
                return Ok(c);
            }
        }
        self.discover(cluster).await
    }

    pub fn invalidate(&self) {
        *self.leader.write() = None;
    }

    pub fn leader_addr(&self) -> Option<String> {
        self.leader.read().as_ref().map(|p| p.addr.clone())
    }

    async fn discover(&self, cluster: &Cluster) -> SqlResult<Client> {
        let _g = self.discover.lock().await;
        if let Some(p) = self.leader.read().clone() {
            if let Some(c) = p.pick() {
                return Ok(c);
            }
        }
        let mut last_err = String::from("no replicas configured");
        for addr in &self.replicas {
            let opts = cluster.opts(addr);
            let c = match Client::connect(&opts).await {
                Ok(c) => c,
                Err(e) => {
                    last_err = format!("{}: {}", addr, e);
                    continue;
                }
            };
            let ro = match c.eval(0, "return box.info.ro", &[0x90]).await {
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
            let mut clients = vec![c.clone()];
            for _ in 1..cluster.cfg.pool_size.max(1) {
                match Client::connect(&opts).await {
                    Ok(x) => clients.push(x),
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
}

pub struct CachedPlan {
    pub version: u64,
    pub stmt: crate::plan::Stmt,
}

pub struct Cluster {
    pub cfg: ClusterConfig,
    pub shards: Vec<Shard>,
    catalog: RwLock<Arc<Catalog>>,
    plans: RwLock<HashMap<(String, String), Arc<CachedPlan>>>,
    auto_inc: Mutex<HashMap<u32, (u64, u64)>>,
    txn_seq: AtomicU64,
    boot: u32,
    pub stats: Stats,
    pub globals: Mutex<Globals>,
    /// Catalog reloads so far, and the lock that makes concurrent
    /// `refresh_catalog` calls share one reload.
    reloads: AtomicU64,
    reload_mu: tokio::sync::Mutex<()>,
}

/// A pending lock wait on one shard: (shard, waiter, wait id, holders).
type Wait = (usize, String, u64, Vec<String>);

async fn lock_waits(c: &Arc<Cluster>) -> Vec<Wait> {
    let mut args = crate::mpw::Mp::new();
    args.arr(0);
    let futs = (0..c.shards.len()).map(|s| c.call(s, "tarn_lock_waits", &args.0));
    let mut out = Vec::new();
    for (s, r) in futures::future::join_all(futs).await.into_iter().enumerate() {
        let Ok(d) = r else { continue };
        let mut rd = crate::mpw::Rd::new(&d);
        let parse = |rd: &mut crate::mpw::Rd| -> SqlResult<Vec<Wait>> {
            let mut v = Vec::new();
            rd.array()?; // CALL results
            let n = rd.array()?;
            for _ in 0..n {
                rd.array()?;
                let waiter = rd.string()?;
                let id = rd.value()?.as_i64().unwrap_or(0) as u64;
                let m = rd.array()?;
                let mut hs = Vec::with_capacity(m);
                for _ in 0..m {
                    hs.push(rd.string()?);
                }
                v.push((s, waiter, id, hs));
            }
            Ok(v)
        };
        if let Ok(v) = parse(&mut rd) {
            out.extend(v);
        }
    }
    out
}

/// Cycles in the wait-for graph of `waits` (each as the waits on it).
fn wait_cycles(waits: &[Wait]) -> Vec<Vec<Wait>> {
    let mut adj: HashMap<&str, Vec<(&str, &Wait)>> = HashMap::new();
    for w in waits {
        for h in &w.3 {
            adj.entry(w.1.as_str()).or_default().push((h.as_str(), w));
        }
    }
    let mut cycles: Vec<Vec<Wait>> = Vec::new();
    let mut seen_sets: Vec<Vec<String>> = Vec::new();
    let mut done: std::collections::HashSet<&str> = std::collections::HashSet::new();
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
                        let mut members: Vec<String> = cyc.iter().map(|w| w.1.clone()).collect();
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

/// Cross-shard deadlock detection (issue #17). Each shard fails a request
/// that would close a cycle among its own waits at once (1213); a cycle
/// through several shards is visible only here. Every `deadlock_interval`
/// seconds the wait-for edges of all shards are collected; a cycle whose
/// waits are all still pending, with the same wait ids, in a second
/// collection is a real deadlock -- each of its waits was pending from its
/// first observation to its second, so all were pending at once -- and its
/// transaction with the largest id (the same choice on every SQL server)
/// is failed with 1213 on the shard where it waits.
pub async fn detect_deadlocks(c: &Arc<Cluster>) {
    let first = lock_waits(c).await;
    if first.len() < 2 {
        return;
    }
    let cycles = wait_cycles(&first);
    if cycles.is_empty() {
        return;
    }
    let second = lock_waits(c).await;
    for cyc in cycles {
        let still = cyc
            .iter()
            .all(|w| second.iter().any(|v| v.0 == w.0 && v.1 == w.1 && v.2 == w.2));
        if !still {
            continue;
        }
        let victim = cyc.iter().max_by(|a, b| a.1.cmp(&b.1)).unwrap();
        let mut m = crate::mpw::Mp::new();
        m.arr(2).str(&victim.1).uint(victim.2);
        // true when this call failed the wait; false when it was already
        // over (another SQL server's detector may have got there first)
        let broke = match c.call(victim.0, "tarn_lock_victim", &m.0).await {
            Ok(d) => {
                let mut rd = crate::mpw::Rd::new(&d);
                rd.array().is_ok() && matches!(rd.value(), Ok(Value::Int(1)))
            }
            Err(_) => false,
        };
        if broke {
            c.stats.deadlocks.fetch_add(1, Ordering::Relaxed);
            tracing::info!(
                "deadlock across shards among {} transactions; rolled back {}",
                cyc.len(),
                victim.1
            );
        }
    }
}

impl Cluster {
    pub async fn connect(cfg: ClusterConfig) -> SqlResult<Arc<Cluster>> {
        if cfg.shards.is_empty() {
            return Err(SqlError::internal("cluster config has no shards"));
        }
        let shards = cfg
            .shards
            .iter()
            .map(|s| Shard {
                id: s.id,
                replicas: s.replicas.clone(),
                leader: RwLock::new(None),
                discover: tokio::sync::Mutex::new(()),
            })
            .collect();
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
        let c = Arc::new(Cluster {
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
        });
        for s in &c.shards {
            s.client(&c).await?;
        }
        if let Err(e) = c.finish_pending_ddl().await {
            tracing::warn!("finishing a pending schema change: {}", e);
        }
        c.reload_catalog().await?;
        if c.shards.len() > 1 && c.cfg.deadlock_interval > 0.0 {
            let weak = Arc::downgrade(&c);
            let every = Duration::from_secs_f64(c.cfg.deadlock_interval);
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(every).await;
                    let Some(c) = weak.upgrade() else { break };
                    detect_deadlocks(&c).await;
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
        Opts {
            addr: addr.to_string(),
            user: Some(self.cfg.user.clone()),
            password: Some(self.cfg.password.clone()),
            connect_timeout: Duration::from_secs(3),
        }
    }

    pub fn nshards(&self) -> usize {
        self.shards.len()
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
        let b = route::bucket_of(shard_key, self.cfg.bucket_count);
        route::shard_of_bucket(b, self.cfg.bucket_count, self.shards.len() as u32) as usize
    }

    pub fn bucket_of(&self, shard_key: &[Value]) -> u32 {
        route::bucket_of(shard_key, self.cfg.bucket_count)
    }

    /// CALL an idempotent Lua procedure on a shard leader outside any
    /// stream. Retried on a new leader after ER_READONLY (the procedure did
    /// not run) and once after a lost connection (it may have run: callers
    /// guarantee that running it twice is harmless).
    pub async fn call(&self, shard: usize, func: &str, args: &[u8]) -> SqlResult<Bytes> {
        self.call_inner(shard, func, args, true).await
    }

    /// CALL a procedure that must not run twice (autocommit DML). Retried
    /// only after ER_READONLY; a connection lost while the request was in
    /// flight leaves the outcome unknown, reported as such.
    pub async fn call_once(&self, shard: usize, func: &str, args: &[u8]) -> SqlResult<Bytes> {
        self.call_inner(shard, func, args, false).await
    }

    async fn call_inner(&self, shard: usize, func: &str, args: &[u8], idempotent: bool) -> SqlResult<Bytes> {
        let s = &self.shards[shard];
        let mut tries = 0;
        loop {
            let c = s.client(self).await?;
            match c.call(0, func, args).await {
                Ok(r) => return r.data().ok_or_else(|| crate::mpw::proto("CALL reply without data")),
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

    pub async fn call_all(&self, func: &str, args: &[u8]) -> SqlResult<Vec<Bytes>> {
        let futs = (0..self.shards.len()).map(|i| self.call(i, func, args));
        futures::future::try_join_all(futs).await
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
        if p.version == v {
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

    pub async fn create_database(&self, name: &str, if_not_exists: bool) -> SqlResult<()> {
        if self.catalog().has_db(name) {
            if if_not_exists {
                return Ok(());
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
        self.bump_catalog().await
    }

    pub async fn drop_database(&self, name: &str, if_exists: bool) -> SqlResult<u64> {
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
        for t in &tables {
            self.drop_table_raw(&t.space_name()).await?;
        }
        let mut a = Mp::new();
        a.arr(1).str(name);
        lua_status(&self.call(0, "tarn_db_drop", &a.0).await?)?;
        self.bump_catalog().await?;
        Ok(tables.len() as u64)
    }

    pub async fn create_table(
        &self,
        mut def: TableDef,
        if_not_exists: bool,
        auto_increment: Option<u64>,
    ) -> SqlResult<()> {
        let cat = self.catalog();
        if !cat.has_db(&def.db) {
            return Err(SqlError::new(1049, "42000", format!("Unknown database '{}'", def.db)));
        }
        if cat.get(&def.db, &def.name).is_some() {
            if if_not_exists {
                return Ok(());
            }
            return Err(SqlError::new(
                1050,
                "42S01",
                format!("Table '{}' already exists", def.name),
            ));
        }
        for &c in &def.shard_key {
            let t = def.columns[c].ty();
            if !(t.is_integer() || matches!(t.tarantool_type(), "string" | "varbinary")) {
                return Err(SqlError::unsupported(
                    "shard key columns other than integer and string types",
                ));
            }
        }
        let mut a = Mp::new();
        a.arr(2).str("space_id").uint(1);
        let d = self.call(0, "tarn_counter_alloc", &a.0).await?;
        let mut rd = Rd::new(&d);
        rd.array()?;
        def.space_id = 10_000 + rd.value()?.as_i64().unwrap_or(0) as u32;
        let ld = def.lua_def();
        let mut m = Mp::new();
        m.arr(1).map(6);
        m.str("name").str(&ld.name);
        m.str("id").uint(ld.id as u64);
        m.str("engine").str(ld.engine);
        m.str("format").arr(ld.format.len());
        for (n, t, nullable) in &ld.format {
            m.map(3)
                .str("name")
                .str(n)
                .str("type")
                .str(t)
                .str("is_nullable")
                .bool(*nullable);
        }
        m.str("indexes").arr(ld.indexes.len());
        for (n, unique, parts) in &ld.indexes {
            m.map(3)
                .str("name")
                .str(n)
                .str("unique")
                .bool(*unique)
                .str("parts")
                .arr(parts.len());
            for (f, t, nullable) in parts {
                m.map(3)
                    .str("field")
                    .str(f)
                    .str("type")
                    .str(t)
                    .str("is_nullable")
                    .bool(*nullable);
            }
        }
        m.str("json").str(&ld.json);
        for d in self.call_all("tarn_create_table", &m.0).await? {
            lua_status(&d)?;
        }
        if let Some(n) = auto_increment.filter(|&n| n > 1) {
            if def.auto_inc.is_some() {
                self.note_auto_inc(&def, n - 1).await?;
            }
        }
        self.bump_catalog().await
    }

    async fn drop_table_raw(&self, space: &str) -> SqlResult<()> {
        let mut a = Mp::new();
        a.arr(1).str(space);
        for d in self.call_all("tarn_drop_table", &a.0).await? {
            lua_status(&d)?;
        }
        Ok(())
    }

    pub async fn drop_tables(&self, tables: &[(String, String)], if_exists: bool) -> SqlResult<()> {
        let cat = self.catalog();
        for (db, t) in tables {
            if cat.get(db, t).is_none() && !if_exists {
                return Err(SqlError::new(1051, "42S02", format!("Unknown table '{}.{}'", db, t)));
            }
        }
        for (db, t) in tables {
            if let Some(def) = cat.get(db, t) {
                self.drop_table_raw(&def.space_name()).await?;
            }
        }
        self.bump_catalog().await
    }

    pub async fn truncate(&self, db: &str, table: &str) -> SqlResult<()> {
        let def = self
            .catalog()
            .get(db, table)
            .ok_or_else(|| SqlError::no_table(db, table))?;
        let mut a = Mp::new();
        a.arr(1).str(&def.space_name());
        for d in self.call_all("tarn_truncate", &a.0).await? {
            lua_status(&d)?;
        }
        Ok(())
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
    /// `alter` and lua/tarn.lua "Schema changes". One schema change runs at a
    /// time (the job record on the first shard); a pending one left by a
    /// failed or crashed server is finished first.
    pub async fn alter_table(&self, plan: &crate::alter::AlterPlan, lock_timeout: f64) -> SqlResult<()> {
        let owner = format!(
            "{}-{:08x}-{}",
            self.cfg.node_id,
            self.boot,
            self.txn_seq.fetch_add(1, Ordering::Relaxed)
        );
        let deadline = std::time::Instant::now() + Duration::from_secs_f64(lock_timeout.max(1.0));
        // Take the job slot; finish an abandoned job on the way.
        loop {
            if let Some(job) = self.ddl_job().await? {
                if job.age > DDL_JOB_STALE {
                    tracing::warn!("finishing schema change of {} abandoned by {}", job.space, job.owner);
                    let mut t = Mp::new();
                    t.arr(2).str(&owner).str(&job.owner);
                    let d = self.call(0, "tarn_ddl_touch", &t.0).await?;
                    if crate::mpw::status_is(&d, "ok") {
                        let r = self.run_ddl_job(&job.space, &job.args, &owner).await;
                        self.ddl_end(&owner).await?;
                        r.map_err(|mut e| {
                            e.message = format!("{} (finishing an earlier schema change)", e.message);
                            e
                        })?;
                    }
                    continue;
                }
                if std::time::Instant::now() > deadline {
                    return Err(SqlError::new(
                        1205,
                        "HY000",
                        "Lock wait timeout exceeded; try restarting transaction (another schema change is running)",
                    ));
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
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
            let ver = self.next_catalog_version().await?;
            ch.def.ver = ver;
            let args = crate::alter::lua_args(&ch, ver, lock_timeout);
            let space = def.space_name();
            let mut j = Mp::new();
            j.arr(1).arr(4).str(&owner).str(&space).uint(ver);
            j.0.extend_from_slice(&args);
            let d = self.call(0, "tarn_ddl_begin", &j.0).await?;
            if crate::mpw::status_is(&d, "busy") {
                // Someone else started one meanwhile (perhaps not yet
                // visible to tarn_ddl_job: give it time).
                tokio::time::sleep(Duration::from_millis(20)).await;
                continue;
            }
            lua_status(&d)?;
            // Keep the job alive while it runs.
            let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();
            let toucher = {
                let owner = owner.clone();
                let s0 = self.shards[0].client(self).await?;
                tokio::spawn(async move {
                    let mut t = Mp::new();
                    t.arr(1).str(&owner);
                    loop {
                        tokio::select! {
                            _ = &mut stop_rx => break,
                            _ = tokio::time::sleep(Duration::from_secs(5)) => {
                                let _ = s0.call(0, "tarn_ddl_touch", &t.0).await;
                            }
                        }
                    }
                })
            };
            let r = self.run_ddl_job(&space, &args, &owner).await;
            let _ = stop_tx.send(());
            let _ = toucher.await;
            if r.is_ok() || r.as_ref().is_err_and(|e| e.code != 1180) {
                self.ddl_end(&owner).await?;
            }
            r?;
            return self.alter_auto_inc(&ch.def, &ch).await;
        }
    }

    /// PREPARE everywhere; then APPLY everywhere, or ABORT everywhere if a
    /// PREPARE failed; then publish the new catalog. Every step may be
    /// repeated (a job finished by another server runs it again).
    async fn run_ddl_job(&self, space: &str, args: &[u8], _owner: &str) -> SqlResult<()> {
        let mut call = Mp::new();
        call.arr(2).str(space);
        call.0.extend_from_slice(args);
        let n = self.shards.len();
        let prep = futures::future::join_all((0..n).map(|i| self.call(i, "tarn_alter_prepare", &call.0))).await;
        let mut failed = None;
        for r in prep {
            if let Err(e) = r.and_then(|d| lua_status(&d).map(|_| ())) {
                failed.get_or_insert(e);
            }
        }
        if let Some(e) = failed {
            for r in futures::future::join_all((0..n).map(|i| self.call(i, "tarn_alter_abort", &call.0))).await {
                if let Err(x) = r.and_then(|d| lua_status(&d).map(|_| ())) {
                    tracing::warn!("schema change abort of {}: {}", space, x);
                }
            }
            return Err(e);
        }
        for r in futures::future::join_all((0..n).map(|i| self.call(i, "tarn_alter_apply", &call.0))).await {
            if let Err(e) = r.and_then(|d| lua_status(&d).map(|_| ())) {
                // Some shards may have the new definition: the job stays and
                // the next schema change (or a restart) finishes it.
                return Err(SqlError::new(
                    1180,
                    "40003",
                    format!(
                        "ALTER TABLE did not finish on every shard ({}); the next schema change statement completes it",
                        e.message
                    ),
                ));
            }
        }
        self.bump_catalog().await
    }

    async fn ddl_end(&self, owner: &str) -> SqlResult<()> {
        let mut a = Mp::new();
        a.arr(1).str(owner);
        lua_status(&self.call(0, "tarn_ddl_end", &a.0).await?).map(|_| ())
    }

    /// The pending schema change, if any.
    async fn ddl_job(&self) -> SqlResult<Option<DdlJob>> {
        let d = self.call(0, "tarn_ddl_job", &[0x90]).await?;
        let mut rd = Rd::new(&d);
        rd.array()?;
        let n = rd.array()?;
        let _ok = rd.string()?;
        if n < 6 {
            return Ok(None);
        }
        let owner = rd.string()?;
        let age = rd.value()?.as_f64().unwrap_or(0.0);
        let space = rd.string()?;
        let _ver = rd.value()?;
        let args = rd.raw_value()?.to_vec();
        Ok(Some(DdlJob {
            owner,
            age,
            space,
            args,
        }))
    }

    /// Finish a schema change a crashed server left (at startup).
    pub async fn finish_pending_ddl(&self) -> SqlResult<()> {
        if let Some(job) = self.ddl_job().await? {
            if job.age > DDL_JOB_STALE {
                let owner = format!("{}-{:08x}-recovery", self.cfg.node_id, self.boot);
                let mut t = Mp::new();
                t.arr(2).str(&owner).str(&job.owner);
                let d = self.call(0, "tarn_ddl_touch", &t.0).await?;
                if crate::mpw::status_is(&d, "ok") {
                    let r = self.run_ddl_job(&job.space, &job.args, &owner).await;
                    self.ddl_end(&owner).await?;
                    r?;
                }
            }
        }
        Ok(())
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
            for i in 0..self.shards.len() {
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
        {
            let mut m = self.auto_inc.lock();
            if let Some((next, end)) = m.get_mut(&t.space_id) {
                if *next + n - 1 <= *end {
                    let v = *next;
                    *next += n;
                    return Ok(v);
                }
            }
        }
        let batch = n.max(1000);
        let mut a = Mp::new();
        a.arr(2).str(&format!("ai:{}", t.space_id)).uint(batch);
        let d = self.call(0, "tarn_counter_alloc", &a.0).await?;
        let mut rd = Rd::new(&d);
        rd.array()?;
        let start = rd.value()?.as_i64().unwrap_or(1) as u64;
        let mut m = self.auto_inc.lock();
        m.insert(t.space_id, (start + n, start + batch - 1));
        Ok(start)
    }

    /// An explicit value was inserted into an AUTO_INCREMENT column: make
    /// sure future generated values are larger.
    pub async fn note_auto_inc(&self, t: &TableDef, v: u64) -> SqlResult<()> {
        {
            let mut m = self.auto_inc.lock();
            if let Some((next, end)) = m.get_mut(&t.space_id) {
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
        a.arr(2).str(&format!("ai:{}", t.space_id)).uint(v);
        self.call(0, "tarn_counter_bump", &a.0).await?;
        self.auto_inc.lock().remove(&t.space_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn w(shard: usize, waiter: &str, id: u64, holders: &[&str]) -> Wait {
        (
            shard,
            waiter.into(),
            id,
            holders.iter().map(|h| h.to_string()).collect(),
        )
    }

    fn members(c: &[Wait]) -> Vec<String> {
        let mut m: Vec<String> = c.iter().map(|w| w.1.clone()).collect();
        m.sort();
        m
    }

    #[test]
    fn finds_cycles_through_shards() {
        // A waits on shard 0 for B, B on shard 1 for A: one cycle, both waits.
        let c = wait_cycles(&[w(0, "A", 1, &["B"]), w(1, "B", 7, &["A"])]);
        assert_eq!(c.len(), 1);
        assert_eq!(members(&c[0]), ["A", "B"]);
        assert!(c[0].iter().any(|x| x.0 == 0 && x.2 == 1) && c[0].iter().any(|x| x.0 == 1 && x.2 == 7));
        // A chain is not a cycle.
        assert!(wait_cycles(&[w(0, "A", 1, &["B"]), w(1, "B", 2, &["C"])]).is_empty());
        // Three transactions, with a bystander waiting on the cycle.
        let c = wait_cycles(&[
            w(0, "A", 1, &["B"]),
            w(1, "B", 2, &["C"]),
            w(0, "C", 3, &["A", "D"]),
            w(1, "E", 4, &["A"]),
        ]);
        assert_eq!(c.len(), 1);
        assert_eq!(members(&c[0]), ["A", "B", "C"]);
        // Two separate cycles are both reported, each once.
        let c = wait_cycles(&[
            w(0, "A", 1, &["B"]),
            w(1, "B", 2, &["A"]),
            w(0, "C", 3, &["D"]),
            w(1, "D", 4, &["C"]),
        ]);
        let mut all: Vec<Vec<String>> = c.iter().map(|x| members(x)).collect();
        all.sort();
        assert_eq!(all, vec![vec!["A", "B"], vec!["C", "D"]]);
    }
}
