//! tarn-server: accepts MySQL clients and runs their statements against the
//! Tarantool shards listed in a cluster config.
//!
//! ```text
//! tarn-server --config cluster.json [--listen 0.0.0.0:3306] [--threads N]
//!             [--user name:password]... [--allow-all]
//! tarn-server --shard 127.0.0.1:3301,127.0.0.1:3311 --shard 127.0.0.1:3302 ...
//! ```
//! `--shard` (repeatable, comma-separated replicas, leader found
//! automatically) is shorthand for a config with default settings.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tarn_common::{SqlResult, Value};
use tarn_mysql::{AuthConfig, Backend, PrepareInfo, QueryResult, ServerOpts};
use tarn_sql::{Cluster, ClusterConfig, Output, Session, ShardConfig};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

struct Conn {
    s: Session,
}

/// A statement's output as the protocol sends it; `warnings`: how many
/// warnings and notes it left for SHOW WARNINGS.
fn convert(o: Output, warnings: u16) -> QueryResult {
    match o {
        Output::Ok {
            affected,
            last_insert_id,
            info,
        } => QueryResult::Ok {
            affected_rows: affected,
            last_insert_id,
            info,
            warnings,
        },
        Output::Rows { columns, rows } => QueryResult::Rows { columns, rows },
    }
}

impl Backend for Conn {
    async fn on_connect(
        &mut self,
        user: &str,
        database: Option<&str>,
        found_rows: bool,
        collation: u8,
    ) -> SqlResult<()> {
        self.s.user = user.to_string();
        self.s.found_rows_flag = found_rows;
        self.s.set_client_collation(collation);
        if let Some(db) = database {
            if !db.is_empty() {
                self.s.use_db_fresh(db).await?;
            }
        }
        Ok(())
    }

    async fn on_query(&mut self, sql: &str) -> SqlResult<QueryResult> {
        let r = self.s.query(sql).await;
        r.map(|o| convert(o, self.s.warning_count()))
    }

    async fn on_prepare(&mut self, stmt_id: u32, sql: &str) -> SqlResult<PrepareInfo> {
        let (n, columns) = self.s.prepare(stmt_id, sql)?;
        Ok(PrepareInfo {
            num_params: n as u16,
            columns,
        })
    }

    async fn on_execute(&mut self, stmt_id: u32, params: &[Value]) -> SqlResult<QueryResult> {
        let r = self.s.execute(stmt_id, params).await;
        r.map(|o| convert(o, self.s.warning_count()))
    }

    async fn on_close_stmt(&mut self, stmt_id: u32) {
        self.s.close(stmt_id)
    }

    async fn on_init_db(&mut self, db: &str) -> SqlResult<()> {
        self.s.use_db_fresh(db).await
    }

    fn status_flags(&self) -> u16 {
        self.s.status_flags()
    }

    async fn on_disconnect(&mut self) {
        self.s.disconnect()
    }
}

struct Args {
    config: Option<String>,
    shards: Vec<String>,
    listen: String,
    threads: usize,
    users: HashMap<String, String>,
    allow_all: bool,
    ack_on_prepare: bool,
    pool_size: Option<usize>,
    node_id: u32,
    isolation: Option<String>,
    lock_wait_timeout: Option<f64>,
    default_engine: Option<String>,
    default_collation: Option<String>,
    /// `--check-schema FILE`: check the table trees a schema file declares
    /// and exit, without a cluster.
    check_schema: Option<String>,
    /// `--database DB`: the database of a schema file's tables before any USE.
    database: Option<String>,
}

fn usage() -> ! {
    eprintln!(
        "usage: tarn-server (--config FILE | --shard ADDR[,ADDR..] ...) [--listen HOST:PORT] \
         [--threads N] [--user NAME:PASSWORD]... [--allow-all] [--ack-on-prepare] [--pool N] [--node-id N] \
         [--isolation READ-COMMITTED|REPEATABLE-READ|SERIALIZABLE] [--lock-wait-timeout SECONDS] \
         [--default-engine memtx|vinyl] [--default-collation NAME]\n       \
         tarn-server --check-schema FILE [--database DB] [--default-collation NAME]"
    );
    std::process::exit(2)
}

fn parse_args() -> Args {
    let mut a = Args {
        config: None,
        shards: Vec::new(),
        listen: "127.0.0.1:3306".into(),
        threads: std::thread::available_parallelism().map(|n| n.get()).unwrap_or(2),
        users: HashMap::new(),
        allow_all: false,
        ack_on_prepare: false,
        pool_size: None,
        node_id: std::process::id(),
        isolation: None,
        lock_wait_timeout: None,
        default_engine: None,
        default_collation: None,
        check_schema: None,
        database: None,
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        let mut val = || it.next().unwrap_or_else(|| usage());
        match arg.as_str() {
            "--config" => a.config = Some(val()),
            "--shard" => a.shards.push(val()),
            "--listen" => a.listen = val(),
            "--threads" => a.threads = val().parse().unwrap_or_else(|_| usage()),
            "--user" => {
                let v = val();
                let (u, p) = v.split_once(':').unwrap_or((v.as_str(), ""));
                a.users.insert(u.to_string(), p.to_string());
            }
            "--allow-all" => a.allow_all = true,
            "--ack-on-prepare" => a.ack_on_prepare = true,
            "--pool" => a.pool_size = Some(val().parse().unwrap_or_else(|_| usage())),
            "--node-id" => a.node_id = val().parse().unwrap_or_else(|_| usage()),
            "--isolation" => a.isolation = Some(val()),
            "--lock-wait-timeout" => a.lock_wait_timeout = Some(val().parse().unwrap_or_else(|_| usage())),
            "--default-engine" => a.default_engine = Some(val()),
            "--default-collation" => a.default_collation = Some(val()),
            "--check-schema" => a.check_schema = Some(val()),
            "--database" => a.database = Some(val()),
            "-h" | "--help" => usage(),
            _ => {
                eprintln!("unknown argument {arg}");
                usage()
            }
        }
    }
    if a.config.is_none() && a.shards.is_empty() && a.check_schema.is_none() {
        usage()
    }
    a
}

/// `--check-schema FILE`: the checks of declared table trees (issue #85)
/// over a schema file such as a dump, without a cluster: prints each
/// finding and returns the exit code, 1 when there is a warning, 2 when the
/// file cannot be read or a `CREATE TABLE` in it cannot be planned.
fn check_schema(path: &str, db: Option<&str>, collation: &str) -> i32 {
    let text = match std::fs::read_to_string(path) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("cannot read {path}: {e}");
            return 2;
        }
    };
    let statements = tarn_mysql::split_statements(&text);
    match tarn_sql::lint::check_schema(&statements, db, collation) {
        Ok(found) => {
            for f in &found {
                println!("{}: {}.{}: {}", f.level.name(), f.db, f.table, f.message);
            }
            i32::from(found.iter().any(|f| f.level == tarn_sql::lint::Level::Warning))
        }
        Err(e) => {
            let hint = if e.code == 1046 { " (pass --database DB)" } else { "" };
            eprintln!("{path}: {}{hint}", e.message);
            2
        }
    }
}

fn load_config(a: &Args) -> ClusterConfig {
    let mut cfg: ClusterConfig = match &a.config {
        Some(path) => {
            let text = std::fs::read_to_string(path).unwrap_or_else(|e| {
                eprintln!("cannot read {path}: {e}");
                std::process::exit(1)
            });
            serde_json::from_str(&text).unwrap_or_else(|e| {
                eprintln!("bad config {path}: {e}");
                std::process::exit(1)
            })
        }
        None => {
            let shards: Vec<ShardConfig> = a
                .shards
                .iter()
                .enumerate()
                .map(|(i, s)| ShardConfig {
                    id: i as u32 + 1,
                    replicas: s.split(',').map(|r| r.trim().to_string()).collect(),
                })
                .collect();
            serde_json::from_value(serde_json::json!({ "shards": [] }))
                .map(|mut c: ClusterConfig| {
                    c.shards = shards;
                    c
                })
                .expect("default config")
        }
    };
    if a.ack_on_prepare {
        cfg.ack_on_prepare = true;
    }
    if let Some(n) = a.pool_size {
        cfg.pool_size = n;
    }
    if cfg.node_id == 0 {
        cfg.node_id = a.node_id;
    }
    if let Some(i) = &a.isolation {
        cfg.isolation = i.clone();
    }
    if let Some(t) = a.lock_wait_timeout {
        cfg.lock_wait_timeout = t;
    }
    if let Some(e) = &a.default_engine {
        cfg.default_engine = tarn_sql::catalog::Engine::parse(e).unwrap_or_else(|| {
            eprintln!("--default-engine: memtx or vinyl, not {e}");
            std::process::exit(2)
        });
    }
    if let Some(c) = &a.default_collation {
        cfg.default_collation = c.to_ascii_lowercase();
    }
    if tarn_sql::collation::Collation::from_name(&cfg.default_collation).is_none() {
        eprintln!("unknown default collation {}", cfg.default_collation);
        std::process::exit(2)
    }
    cfg
}

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("TARN_LOG")
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();
    let args = parse_args();
    if let Some(path) = &args.check_schema {
        let collation = args
            .default_collation
            .as_deref()
            .unwrap_or("utf8mb4_bin")
            .to_ascii_lowercase();
        std::process::exit(check_schema(path, args.database.as_deref(), &collation));
    }
    let cfg = load_config(&args);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.threads.max(1))
        .enable_all()
        .build()
        .expect("tokio runtime");
    rt.block_on(run(args, cfg));
}

async fn run(args: Args, cfg: ClusterConfig) {
    let nshards = cfg.shards.len();
    let cluster = match Cluster::connect(cfg).await {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("cannot connect to the cluster: {}", e.message);
            std::process::exit(1)
        }
    };
    let auth = if args.allow_all || args.users.is_empty() {
        AuthConfig::AllowAll
    } else {
        AuthConfig::Users(args.users.clone())
    };
    let opts = Arc::new(ServerOpts {
        server_version: tarn_sql::SERVER_VERSION.to_string(),
        auth,
    });
    let listener = match tokio::net::TcpListener::bind(&args.listen).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!("cannot listen on {}: {e}", args.listen);
            std::process::exit(1)
        }
    };
    tracing::info!(
        "tarn-server listening on {} ({} shards, {} threads)",
        args.listen,
        nshards,
        args.threads
    );
    let next_id = AtomicU32::new(1);
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(x) => x,
            Err(e) => {
                tracing::warn!("accept: {e}");
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                continue;
            }
        };
        let _ = stream.set_nodelay(true);
        let id = next_id.fetch_add(1, Ordering::Relaxed);
        let backend = Conn {
            s: Session::new(cluster.clone(), id),
        };
        let opts = opts.clone();
        tokio::spawn(async move {
            if let Err(e) = tarn_mysql::serve_connection(stream, id, opts, backend).await {
                tracing::debug!("connection {id} from {peer}: {e}");
            }
        });
    }
}
