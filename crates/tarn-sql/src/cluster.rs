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
        let c = Arc::new(Cluster {
            cfg,
            shards,
            catalog: RwLock::new(Arc::new(Catalog::default())),
            plans: RwLock::new(HashMap::new()),
            auto_inc: Mutex::new(HashMap::new()),
            txn_seq: AtomicU64::new(1),
            boot,
            stats: Stats::default(),
        });
        for s in &c.shards {
            s.client(&c).await?;
        }
        c.reload_catalog().await?;
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
            for _ in 3..m {
                rd.skip()?;
            }
            let mut def = catalog::parse_def_json(&json)?;
            def.space_id = id;
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
        *self.catalog.write() = Arc::new(Catalog::new(version, dbs, defs));
        self.plans.write().clear();
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

    pub async fn create_table(&self, mut def: TableDef, if_not_exists: bool) -> SqlResult<()> {
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
        m.arr(1).map(5);
        m.str("name").str(&ld.name);
        m.str("id").uint(ld.id as u64);
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

    pub async fn create_index(
        &self,
        db: &str,
        table: &str,
        name: &str,
        cols: &[String],
        unique: bool,
        if_not_exists: bool,
    ) -> SqlResult<()> {
        let def = self
            .catalog()
            .get(db, table)
            .ok_or_else(|| SqlError::no_table(db, table))?;
        if def.index_by_name(name).is_some() {
            if if_not_exists {
                return Ok(());
            }
            return Err(SqlError::new(1061, "42000", format!("Duplicate key name '{}'", name)));
        }
        let mut ix = Vec::new();
        for c in cols {
            ix.push(
                def.col_index(c).ok_or_else(|| {
                    SqlError::new(1072, "42000", format!("Key column '{}' doesn't exist in table", c))
                })?,
            );
        }
        let mut nd = (*def).clone();
        // the bucket_id index occupies the last Tarantool index id; new
        // secondary indexes get ids after it
        let id = nd.indexes.len() as u32 + 1;
        nd.indexes.push(catalog::IndexDef {
            name: name.to_string(),
            id,
            cols: ix.clone(),
            unique,
        });
        catalog::validate(&nd)?;
        let mut m = Mp::new();
        m.arr(2).str(&def.space_name());
        m.map(4)
            .str("name")
            .str(name)
            .str("id")
            .uint(id as u64)
            .str("unique")
            .bool(unique)
            .str("parts")
            .arr(ix.len());
        for &c in &ix {
            let col = &def.columns[c];
            m.map(3)
                .str("field")
                .str(&col.name)
                .str("type")
                .str(col.ty().tarantool_type())
                .str("is_nullable")
                .bool(col.nullable);
        }
        for d in self.call_all("tarn_create_index", &m.0).await? {
            lua_status(&d)?;
        }
        // store the updated definition
        let mut j = Mp::new();
        j.arr(2)
            .str(&def.space_name())
            .str(&serde_json::to_string(&nd).unwrap());
        for d in self.call_all("tarn_set_def_json", &j.0).await? {
            lua_status(&d)?;
        }
        self.bump_catalog().await
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
