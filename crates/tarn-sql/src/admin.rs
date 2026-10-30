//! Session-level statements (transaction control, SET, SHOW, USE, @@vars)
//! recognized directly from the SQL text. Clients and drivers send a lot of
//! these at connect time; handling them here keeps them off the planner.

use tarn_common::{SqlError, SqlResult, Value};

#[derive(Clone, Debug, PartialEq)]
pub enum Admin {
    Begin {
        read_only: bool,
    },
    Commit,
    Rollback,
    /// SET ...: the assignments that matter to Tarn (others -- names,
    /// sql_mode, ... -- are accepted and ignored).
    Set(Vec<SetItem>),
    Savepoint(String),
    RollbackToSavepoint(String),
    ReleaseSavepoint(String),
    Use(String),
    ShowDatabases {
        like: Option<String>,
    },
    ShowTarnIntegrity {
        db: Option<String>,
    },
    ShowTables {
        db: Option<String>,
        full: bool,
        like: Option<String>,
    },
    ShowColumns {
        db: Option<String>,
        table: String,
        full: bool,
    },
    ShowCreateTable {
        db: Option<String>,
        table: String,
    },
    ShowIndex {
        db: Option<String>,
        table: String,
    },
    ShowVariables {
        global: bool,
        like: Option<String>,
    },
    ShowStatus {
        like: Option<String>,
    },
    /// SHOW WARNINGS, or SHOW ERRORS (`errors`).
    ShowWarnings {
        errors: bool,
    },
    ShowEngines,
    ShowCollation,
    ShowCharset,
    ShowProcesslist,
    ShowTableStatus {
        db: Option<String>,
        like: Option<String>,
    },
    ShowTarnStatus,
    /// SHOW TARN QUERIES: bounded, redacted digest/routing observations.
    ShowTarnQueries,
    /// SHOW TARN PLAN FOR a named SQL PREPARE or binary statement ID.
    ShowTarnPlan(String),
    Analyze(String),
    ShowTarnStatistics,
    ShowTarnRpcs,
    /// SHOW TARN BUCKETS: which shard owns which buckets, and the moves in
    /// progress.
    ShowTarnBuckets,
    /// TARN MOVE BUCKET n TO SHARD id, TARN MOVE BUCKETS BETWEEN n AND m TO
    /// SHARD id: move buckets first..=last to the shard with that id (those
    /// it owns already stay).
    MoveBuckets {
        first: u32,
        last: u32,
        to: u32,
    },
    /// TARN ADD SHARD id 'replica[,replica...]': the shard with that id
    /// joins the cluster, with no buckets (they move to it afterwards).
    AddShard {
        id: u32,
        replicas: Vec<String>,
    },
    /// TARN REMOVE SHARD id: the shard leaves the cluster (its buckets move
    /// to the others first).
    RemoveShard {
        id: u32,
    },
    /// SHOW TARN SHARDS: the cluster's shards (its membership).
    ShowTarnShards,
    /// TARN REBALANCE [BY SIZE | BY LOAD] [MAX MOVES n] [MAX RATE bytes]:
    /// move buckets toward an even spread, in the background.
    Rebalance {
        by: String,
        max_moves: u32,
        max_rate: u64,
    },
    /// TARN REBALANCE PAUSE | RESUME | CANCEL.
    RebalanceControl(String),
    /// SHOW TARN REBALANCE: the rebalancer's state.
    ShowTarnRebalance,
    /// SHOW TARN MOVES: the moves of its current (or last) round.
    ShowTarnMoves,
    /// SELECT @@a [AS x], @@b ... [LIMIT n]
    SelectVars(Vec<(String, String)>),
    Describe {
        db: Option<String>,
        table: String,
    },
    LockTables,
    /// SQL-level prepared statements (`PREPARE name FROM ...`, `EXECUTE name
    /// [USING @a, ...]`, `{DEALLOCATE | DROP} PREPARE name`), distinct from
    /// the binary protocol's COM_STMT_PREPARE. Names are lower-cased.
    Prepare {
        name: String,
        source: PrepareSource,
    },
    Execute {
        name: String,
        /// User variables (lower-cased, without `@`) bound to the `?` markers.
        using: Vec<String>,
    },
    Deallocate(String),
    Noop,
}

/// The text of a `PREPARE`: a string literal or a user variable.
#[derive(Clone, Debug, PartialEq)]
pub enum PrepareSource {
    Text(String),
    Var(String),
}

impl Admin {
    /// A statement that reports table definitions from the catalog.
    pub fn reads_catalog(&self) -> bool {
        matches!(
            self,
            Admin::ShowDatabases { .. }
                | Admin::ShowTables { .. }
                | Admin::ShowColumns { .. }
                | Admin::ShowCreateTable { .. }
                | Admin::ShowIndex { .. }
                | Admin::ShowTableStatus { .. }
                | Admin::Describe { .. }
                | Admin::ShowTarnIntegrity { .. }
        )
    }
}

pub const SERVER_VERSION_COMMENT: &str = "Tarn: distributed MySQL on Tarantool";
pub const MAX_ERROR_COUNT: usize = 64;

/// Transaction isolation levels as MySQL names them. READ UNCOMMITTED and
/// READ COMMITTED run as READ COMMITTED (row locks, plain reads never
/// validated); REPEATABLE READ and SERIALIZABLE run strictly serializable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Isolation {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

impl Isolation {
    /// 'READ-COMMITTED', 'read committed', 'READ_COMMITTED', ...
    pub fn parse(s: &str) -> Option<Isolation> {
        let n: String = s
            .trim()
            .chars()
            .map(|c| {
                if c == '_' || c == ' ' {
                    '-'
                } else {
                    c.to_ascii_uppercase()
                }
            })
            .collect();
        Some(match n.as_str() {
            "READ-UNCOMMITTED" => Isolation::ReadUncommitted,
            "READ-COMMITTED" => Isolation::ReadCommitted,
            "REPEATABLE-READ" => Isolation::RepeatableRead,
            "SERIALIZABLE" => Isolation::Serializable,
            _ => return None,
        })
    }
    pub fn name(self) -> &'static str {
        match self {
            Isolation::ReadUncommitted => "READ-UNCOMMITTED",
            Isolation::ReadCommitted => "READ-COMMITTED",
            Isolation::RepeatableRead => "REPEATABLE-READ",
            Isolation::Serializable => "SERIALIZABLE",
        }
    }
    pub fn mode(self) -> crate::txn::Mode {
        match self {
            Isolation::ReadUncommitted | Isolation::ReadCommitted => crate::txn::Mode::ReadCommitted,
            Isolation::RepeatableRead | Isolation::Serializable => crate::txn::Mode::Serializable,
        }
    }
    pub fn effective_name(self) -> &'static str {
        match self.mode() {
            crate::txn::Mode::ReadCommitted => "READ-COMMITTED",
            crate::txn::Mode::Serializable => "STRICT-SERIALIZABLE",
        }
    }
}

/// Scope of a SET: the session, the server default for new sessions
/// (GLOBAL), or only the next transaction (SET TRANSACTION ...).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VarScope {
    Session,
    Global,
    Next,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SetItem {
    Autocommit(bool),
    Isolation(VarScope, Isolation),
    /// innodb_lock_wait_timeout in seconds; None: DEFAULT.
    LockWaitTimeout(VarScope, Option<f64>),
    /// Any other assignment: a user variable (`name` starts with `@`) or a
    /// system variable (lower-cased, without `@@` and scope; `names` for SET
    /// NAMES / CHARACTER SET), with the assigned expression as written. The
    /// session evaluates it.
    Var {
        scope: VarScope,
        name: String,
        rhs: String,
    },
}

/// Session state that system variables report.
pub struct SessionVars<'a> {
    pub autocommit: bool,
    pub db: Option<&'a str>,
    pub isolation: Isolation,
    pub global_isolation: Isolation,
    pub global_lock_wait_timeout: f64,
    pub next_isolation: Option<Isolation>,
    pub active_mode: Option<crate::txn::Mode>,
    pub lock_wait_timeout: f64,
    /// Values the session SET for variables Tarn reports but does not act
    /// on (character sets, sql_mode, time_zone, foreign_key_checks, ...).
    pub overrides: Option<&'a std::collections::HashMap<String, Value>>,
    /// collation_connection: the collation of literals.
    pub collation_connection: &'a str,
    /// collation_server / collation_database: the default of new tables.
    pub collation_server: &'a str,
    /// Warnings and notes of the last statement (warning_count).
    pub warning_count: usize,
}

/// A system variable name without `@@` and scope prefix, lower-cased.
pub fn sysvar_name(name: &str) -> String {
    let n = name.trim_start_matches('@').to_ascii_lowercase();
    n.strip_prefix("session.")
        .or_else(|| n.strip_prefix("global."))
        .or_else(|| n.strip_prefix("local."))
        .unwrap_or(&n)
        .to_string()
}

/// Variables reported by SELECT @@x / SHOW VARIABLES.
pub fn system_var(name: &str, sv: &SessionVars) -> Option<Value> {
    let (autocommit, db) = (sv.autocommit, sv.db);
    let n = sysvar_name(name);
    let global = name.trim_start_matches('@').to_ascii_lowercase().starts_with("global.");
    if global {
        match n.as_str() {
            "tarn_isolation_contract"
            | "tarn_effective_isolation"
            | "tarn_next_isolation"
            | "tarn_active_isolation" => return None,
            "tx_isolation" | "transaction_isolation" => return Some(Value::str(sv.global_isolation.name())),
            "innodb_lock_wait_timeout" => return Some(Value::Int(sv.global_lock_wait_timeout.round() as i64)),
            _ => {}
        }
    }
    if let Some(v) = sv.overrides.filter(|_| !global).and_then(|o| o.get(&n)) {
        return Some(v.clone());
    }
    Some(match n.as_str() {
        "version" => Value::str(crate::SERVER_VERSION),
        "version_comment" => Value::str(SERVER_VERSION_COMMENT),
        "version_compile_os" => Value::str("Linux"),
        "version_compile_machine" => Value::str("x86_64"),
        "autocommit" => Value::Int((global || autocommit) as i64),
        "tx_isolation" | "transaction_isolation" => Value::str(sv.isolation.name()),
        "tarn_isolation_contract" => Value::str("TARN"),
        "tarn_effective_isolation" => Value::str(sv.isolation.effective_name()),
        "tarn_next_isolation" => sv.next_isolation.map(|i| Value::str(i.name())).unwrap_or(Value::Null),
        "tarn_active_isolation" => match sv.active_mode {
            Some(crate::txn::Mode::ReadCommitted) => Value::str("READ-COMMITTED"),
            Some(crate::txn::Mode::Serializable) => Value::str("STRICT-SERIALIZABLE"),
            None => Value::Null,
        },
        "tx_read_only" | "transaction_read_only" | "read_only" | "super_read_only" => Value::Int(0),
        "max_allowed_packet" => Value::Int(67_108_864),
        "max_error_count" => Value::Int(MAX_ERROR_COUNT as i64),
        "net_buffer_length" => Value::Int(16384),
        "character_set_client"
        | "character_set_connection"
        | "character_set_results"
        | "character_set_server"
        | "character_set_database"
        | "character_set_system" => Value::str("utf8mb4"),
        "collation_connection" => Value::str(if global {
            sv.collation_server
        } else {
            sv.collation_connection
        }),
        "collation_server" | "collation_database" => Value::str(sv.collation_server),
        "sql_mode" => Value::str("STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION"),
        "time_zone" => Value::str("SYSTEM"),
        "system_time_zone" => Value::str("UTC"),
        "lower_case_table_names" => Value::Int(0),
        "auto_increment_increment" | "auto_increment_offset" => Value::Int(1),
        "interactive_timeout" | "wait_timeout" => Value::Int(28800),
        "net_write_timeout" => Value::Int(60),
        "net_read_timeout" => Value::Int(30),
        "query_cache_size" | "query_cache_type" => Value::Int(0),
        "init_connect" => Value::str(""),
        "license" => Value::str("MIT OR Apache-2.0"),
        "performance_schema" => Value::Int(0),
        "have_ssl" | "have_openssl" => Value::str("DISABLED"),
        "max_connections" => Value::Int(100_000),
        "innodb_lock_wait_timeout" => Value::Int(sv.lock_wait_timeout.round() as i64),
        "lock_wait_timeout" => Value::Int(31_536_000),
        "innodb_rollback_on_timeout" => Value::Int(0),
        "sql_select_limit" => Value::UInt(u64::MAX),
        "default_storage_engine" | "storage_engine" => Value::str("memtx"),
        "port" => Value::Int(3306),
        "hostname" => Value::str("tarn"),
        "database" => db.map(Value::str).unwrap_or(Value::Null),
        "sql_auto_is_null" | "foreign_key_checks" | "unique_checks" => Value::Int(1),
        "div_precision_increment" => Value::Int(4),
        "default_week_format" => Value::Int(0),
        "explicit_defaults_for_timestamp" => Value::Int(1),
        "event_scheduler" => Value::str("OFF"),
        "binlog_format" => Value::str("ROW"),
        "log_bin" => Value::Int(0),
        "server_id" => Value::Int(1),
        "warning_count" => Value::Int(sv.warning_count as i64),
        "error_count" => Value::Int(0),
        "tarn_tree_checks" => Value::str(TREE_CHECKS_DEFAULT),
        "tarn_legacy_integrity" => Value::str("REJECT"),
        "tarn_lazy_read_your_writes" => Value::Int(0),
        "tarn_lazy_wait_timeout" => Value::Int(5),
        "default_authentication_plugin" => Value::str("mysql_native_password"),
        "sql_notes" | "sql_warnings" => Value::Int(1),
        "sql_safe_updates" | "sql_big_selects" => Value::Int(0),
        "max_execution_time" => Value::Int(0),
        _ => return None,
    })
}

pub const SHOWN_VARIABLES: &[&str] = &[
    "tarn_legacy_integrity",
    "autocommit",
    "character_set_client",
    "character_set_connection",
    "character_set_results",
    "character_set_server",
    "collation_connection",
    "collation_server",
    "default_storage_engine",
    "div_precision_increment",
    "init_connect",
    "innodb_lock_wait_timeout",
    "interactive_timeout",
    "license",
    "lower_case_table_names",
    "max_allowed_packet",
    "max_error_count",
    "max_connections",
    "net_buffer_length",
    "net_write_timeout",
    "performance_schema",
    "sql_mode",
    "system_time_zone",
    "tarn_tree_checks",
    "tarn_lazy_read_your_writes",
    "tarn_lazy_wait_timeout",
    "tarn_isolation_contract",
    "tarn_effective_isolation",
    "tarn_next_isolation",
    "tarn_active_isolation",
    "time_zone",
    "transaction_isolation",
    "transaction_read_only",
    "tx_isolation",
    "version",
    "version_comment",
    "wait_timeout",
];

struct Toks {
    t: Vec<String>,
    /// Byte range of each token in the text.
    pos: Vec<(usize, usize)>,
    i: usize,
}

impl Toks {
    fn new(sql: &str) -> SqlResult<Toks> {
        let mut t = Vec::new();
        let mut pos = Vec::new();
        let b = sql.as_bytes();
        let mut i = 0;
        while i < b.len() {
            let c = b[i];
            if c.is_ascii_whitespace() || c == b';' {
                i += 1;
            } else if let Some(end) = crate::prep::line_comment_end(b, i) {
                i = end;
            } else if c == b'/' && b.get(i + 1) == Some(&b'*') {
                // Keep original offsets: SET expressions are sliced from SQL,
                // not reconstructed from tokens or literal-normalized plans.
                i += 2;
                while i + 1 < b.len() && !(b[i] == b'*' && b[i + 1] == b'/') {
                    i += 1;
                }
                i = (i + 2).min(b.len());
            } else if c == b'\'' || c == b'"' || c == b'`' {
                let q = c;
                let mut j = i + 1;
                // Quotes and escapes are ASCII byte boundaries. Copy all other
                // bytes together so a UTF-8 character never becomes Latin-1.
                // Keep the opening marker: unquote and SET parsing use it to
                // distinguish an identifier/string from an unquoted word.
                let mut s = vec![q];
                loop {
                    if j >= b.len() {
                        return Err(SqlError::parse("unterminated quoted token"));
                    }
                    if b[j] == b'\\' && q != b'`' && j + 1 < b.len() {
                        let escaped = match b[j + 1] {
                            b'b' => 8,
                            b'n' => b'\n',
                            b'r' => b'\r',
                            b't' => b'\t',
                            b'0' => 0,
                            b'Z' => 26,
                            x @ (b'%' | b'_') => {
                                s.push(b'\\');
                                x
                            }
                            x => x,
                        };
                        s.push(escaped);
                        j += 2;
                        continue;
                    }
                    if b[j] == q {
                        if b.get(j + 1) == Some(&q) {
                            s.push(q);
                            j += 2;
                            continue;
                        }
                        break;
                    }
                    s.push(b[j]);
                    j += 1;
                }
                t.push(String::from_utf8(s).map_err(|_| SqlError::parse("invalid UTF-8 in quoted token"))?);
                pos.push((i, j + 1));
                i = j + 1;
            } else if c.is_ascii_alphanumeric() || c == b'_' || c == b'@' || c == b'$' || c == b'.' || c >= 0x80 {
                let mut j = i;
                while j < b.len()
                    && (b[j].is_ascii_alphanumeric()
                        || b[j] == b'_'
                        || b[j] == b'@'
                        || b[j] == b'$'
                        || b[j] == b'.'
                        || b[j] >= 0x80)
                {
                    j += 1;
                }
                t.push(sql[i..j].to_string());
                pos.push((i, j));
                i = j;
            } else {
                t.push((c as char).to_string());
                pos.push((i, i + 1));
                i += 1;
            }
        }
        Ok(Toks { t, pos, i: 0 })
    }
    /// Byte offset where token `k` starts (the text length past the end).
    fn offset(&self, k: usize, len: usize) -> usize {
        self.pos.get(k).map(|p| p.0).unwrap_or(len)
    }
    fn peek_up(&self, k: usize) -> String {
        self.t
            .get(self.i + k)
            .map(|s| s.to_ascii_uppercase())
            .unwrap_or_default()
    }
    fn is(&mut self, w: &str) -> bool {
        if self.peek_up(0) == w {
            self.i += 1;
            true
        } else {
            false
        }
    }
    fn next(&mut self) -> Option<String> {
        let v = self.t.get(self.i).cloned();
        self.i += 1;
        v
    }
    fn done(&self) -> bool {
        self.i >= self.t.len()
    }
}

fn unquote(s: &str) -> String {
    let b = s.as_bytes();
    if !b.is_empty() && (b[0] == b'\'' || b[0] == b'"' || b[0] == b'`') {
        s[1..].to_string()
    } else {
        s.to_string()
    }
}

fn split_qualified(name: &str) -> (Option<String>, String) {
    let n = unquote(name);
    match n.split_once('.') {
        Some((d, t)) => (Some(unquote(d)), unquote(t)),
        None => (None, n),
    }
}

fn like_clause(t: &mut Toks) -> Option<String> {
    if t.is("LIKE") {
        t.next().map(|s| unquote(&s))
    } else {
        None
    }
}

fn from_db(t: &mut Toks) -> Option<String> {
    if t.is("FROM") || t.is("IN") {
        t.next().map(|s| unquote(&s))
    } else {
        None
    }
}

fn literal_value(tok: &str) -> Value {
    let u = tok.to_ascii_uppercase();
    if tok.starts_with('\'') || tok.starts_with('"') {
        return Value::str(&unquote(tok));
    }
    match u.as_str() {
        "ON" | "TRUE" => Value::Int(1),
        "OFF" | "FALSE" => Value::Int(0),
        "NULL" | "DEFAULT" => Value::Null,
        _ => tok.parse::<i64>().map(Value::Int).unwrap_or_else(|_| Value::str(tok)),
    }
}

/// Recognize a session-level statement. `None` means "not an admin statement,
/// hand it to the planner".
pub fn recognize(sql: &str) -> SqlResult<Option<Admin>> {
    let mut t = Toks::new(sql)?;
    let first = t.peek_up(0);
    let r = match first.as_str() {
        "BEGIN" => {
            t.i += 1;
            let _ = t.is("WORK");
            Admin::Begin { read_only: false }
        }
        "START" if t.peek_up(1) == "TRANSACTION" => {
            t.i += 2;
            let mut read_only = false;
            while !t.done() {
                let w = t.next().unwrap_or_default().to_ascii_uppercase();
                if w == "ONLY" {
                    read_only = true;
                }
            }
            Admin::Begin { read_only }
        }
        "COMMIT" => Admin::Commit,
        "ROLLBACK" => {
            t.i += 1;
            let _ = t.is("WORK");
            if t.is("TO") {
                let _ = t.is("SAVEPOINT");
                Admin::RollbackToSavepoint(unquote(
                    &t.next()
                        .ok_or_else(|| SqlError::parse("ROLLBACK TO needs a savepoint name"))?,
                ))
            } else {
                Admin::Rollback
            }
        }
        "SAVEPOINT" => {
            t.i += 1;
            Admin::Savepoint(unquote(
                &t.next().ok_or_else(|| SqlError::parse("SAVEPOINT needs a name"))?,
            ))
        }
        "RELEASE" if t.peek_up(1) == "SAVEPOINT" => {
            t.i += 2;
            Admin::ReleaseSavepoint(unquote(
                &t.next()
                    .ok_or_else(|| SqlError::parse("RELEASE SAVEPOINT needs a name"))?,
            ))
        }
        "USE" => {
            t.i += 1;
            Admin::Use(unquote(
                &t.next().ok_or_else(|| SqlError::parse("USE needs a database"))?,
            ))
        }
        "SET" => {
            t.i += 1;
            parse_set(&mut t, sql)?
        }
        "SHOW" => {
            t.i += 1;
            parse_show(&mut t)?
        }
        "DESCRIBE" | "DESC" | "EXPLAIN" if t.t.len() == 2 => {
            t.i += 1;
            let (db, table) = split_qualified(&t.next().unwrap_or_default());
            Admin::Describe { db, table }
        }
        "SELECT" if t.t.get(1).is_some_and(|s| s.starts_with("@@")) => match parse_select_vars(&mut t) {
            Some(v) => Admin::SelectVars(v),
            None => return Ok(None),
        },
        // LOCK TABLES commits the open transaction, as in MySQL; the table
        // locks themselves are not taken (as TiDB without table locks).
        "LOCK" if matches!(t.peek_up(1).as_str(), "TABLE" | "TABLES") => Admin::LockTables,
        "PREPARE" => {
            t.i += 1;
            let name = statement_name(&mut t)?;
            if !t.is("FROM") {
                return Err(SqlError::parse("PREPARE needs FROM 'statement' or FROM @variable"));
            }
            let k = t.i;
            let tok = t.next().ok_or_else(|| SqlError::parse("PREPARE needs a statement"))?;
            let source = if let Some(v) = tok.strip_prefix('@').filter(|v| !v.starts_with('@')) {
                if !t.done() {
                    return Err(SqlError::parse("unexpected text after PREPARE ... FROM @variable"));
                }
                PrepareSource::Var(unquote(v).to_ascii_lowercase())
            } else {
                // the rest of the text is one literal ('it''s' is one token
                // here, not two)
                let rest = sql[t.pos[k].0..].trim_end().trim_end_matches(';').trim_end();
                PrepareSource::Text(
                    crate::prep::string_literal(rest)
                        .ok_or_else(|| SqlError::parse("PREPARE needs FROM 'statement' or FROM @variable"))?,
                )
            };
            Admin::Prepare { name, source }
        }
        "EXECUTE" => {
            t.i += 1;
            let name = statement_name(&mut t)?;
            let mut using = Vec::new();
            if t.is("USING") {
                loop {
                    let v = t.next().unwrap_or_default();
                    match v.strip_prefix('@').filter(|v| !v.is_empty() && !v.starts_with('@')) {
                        Some(v) => using.push(unquote(v).to_ascii_lowercase()),
                        None => return Err(SqlError::parse("EXECUTE ... USING takes user variables")),
                    }
                    if !t.is(",") {
                        break;
                    }
                }
            }
            if !t.done() {
                return Err(SqlError::parse("unexpected text after EXECUTE"));
            }
            Admin::Execute { name, using }
        }
        "DEALLOCATE" | "DROP" if t.peek_up(1) == "PREPARE" => {
            t.i += 2;
            Admin::Deallocate(statement_name(&mut t)?)
        }
        "TARN" if t.peek_up(1) == "MOVE" => {
            t.i += 2;
            parse_move(&mut t)?
        }
        "TARN" if t.peek_up(1) == "ADD" => {
            t.i += 2;
            parse_add_shard(&mut t)?
        }
        "TARN" if t.peek_up(1) == "REBALANCE" => {
            t.i += 2;
            parse_rebalance(&mut t)?
        }
        "TARN" if t.peek_up(1) == "REMOVE" => {
            t.i += 2;
            let usage = || SqlError::parse("expected TARN REMOVE SHARD id");
            if !t.is("SHARD") {
                return Err(usage());
            }
            let id = t
                .next()
                .and_then(|n| n.parse::<u32>().ok())
                .filter(|&n| n > 0)
                .ok_or_else(usage)?;
            if !t.done() {
                return Err(usage());
            }
            Admin::RemoveShard { id }
        }
        "ANALYZE" => Admin::Analyze(sql.to_string()),
        "LOCK" | "UNLOCK" | "FLUSH" | "OPTIMIZE" | "CHECKSUM" | "DO" => Admin::Noop,
        _ => return Ok(None),
    };
    Ok(Some(r))
}

/// `TARN MOVE BUCKET n TO SHARD id` | `TARN MOVE BUCKETS BETWEEN n AND m TO
/// SHARD id` (after `TARN MOVE`).
fn parse_move(t: &mut Toks) -> SqlResult<Admin> {
    let usage =
        || SqlError::parse("expected TARN MOVE BUCKET n TO SHARD id, or TARN MOVE BUCKETS BETWEEN n AND m TO SHARD id");
    let num = |t: &mut Toks| -> SqlResult<u32> { t.next().and_then(|n| n.parse::<u32>().ok()).ok_or_else(usage) };
    let (first, last) = if t.is("BUCKET") {
        let b = num(t)?;
        (b, b)
    } else if t.is("BUCKETS") {
        if !t.is("BETWEEN") {
            return Err(usage());
        }
        let a = num(t)?;
        if !t.is("AND") {
            return Err(usage());
        }
        (a, num(t)?)
    } else {
        return Err(usage());
    };
    if !t.is("TO") || !t.is("SHARD") {
        return Err(usage());
    }
    let to = num(t)?;
    if !t.done() || first == 0 || last < first {
        return Err(usage());
    }
    Ok(Admin::MoveBuckets { first, last, to })
}

/// `TARN ADD SHARD id 'replica[,replica...]'` (after `TARN ADD`): the
/// replicas as `--shard` takes them (host:port, comma-separated).
fn parse_add_shard(t: &mut Toks) -> SqlResult<Admin> {
    let usage = || SqlError::parse("expected TARN ADD SHARD id 'host:port[,host:port...]'");
    if !t.is("SHARD") {
        return Err(usage());
    }
    let id = t
        .next()
        .and_then(|n| n.parse::<u32>().ok())
        .filter(|&n| n > 0)
        .ok_or_else(usage)?;
    let lit = t.next().ok_or_else(usage)?;
    if !lit.starts_with('\'') && !lit.starts_with('"') {
        return Err(usage());
    }
    let replicas: Vec<String> = unquote(&lit)
        .split(',')
        .map(|r| r.trim().to_string())
        .filter(|r| !r.is_empty())
        .collect();
    if replicas.is_empty() || !t.done() {
        return Err(usage());
    }
    Ok(Admin::AddShard { id, replicas })
}

/// `TARN REBALANCE [BY SIZE | BY LOAD] [MAX MOVES n] [MAX RATE bytes]` and
/// `TARN REBALANCE PAUSE | RESUME | CANCEL` (after `TARN REBALANCE`). By
/// default by size, four moves at a time, at any rate.
fn parse_rebalance(t: &mut Toks) -> SqlResult<Admin> {
    let usage = || {
        SqlError::parse(
            "expected TARN REBALANCE [BY SIZE | BY LOAD] [MAX MOVES n] [MAX RATE bytes_per_second], \
             or TARN REBALANCE PAUSE | RESUME | CANCEL",
        )
    };
    for (w, a) in [("PAUSE", "pause"), ("RESUME", "resume"), ("CANCEL", "cancel")] {
        if t.is(w) {
            return if t.done() {
                Ok(Admin::RebalanceControl(a.into()))
            } else {
                Err(usage())
            };
        }
    }
    let (mut by, mut max_moves, mut max_rate) = ("size".to_string(), 4u32, 0u64);
    while !t.done() {
        if t.is("BY") {
            by = if t.is("SIZE") {
                "size".into()
            } else if t.is("LOAD") {
                "load".into()
            } else {
                return Err(usage());
            };
        } else if t.is("MAX") {
            let n = |t: &mut Toks| t.next().and_then(|n| n.parse::<u64>().ok()).ok_or_else(usage);
            if t.is("MOVES") {
                max_moves = n(t)?.clamp(1, 64) as u32;
            } else if t.is("RATE") {
                max_rate = n(t)?;
            } else {
                return Err(usage());
            }
        } else {
            return Err(usage());
        }
    }
    Ok(Admin::Rebalance {
        by,
        max_moves,
        max_rate,
    })
}

/// The name of an SQL-level prepared statement (case-insensitive).
fn statement_name(t: &mut Toks) -> SqlResult<String> {
    match t.next() {
        Some(n) if !n.starts_with('\'') && !n.starts_with('"') && !n.starts_with('@') => {
            Ok(unquote(&n).to_ascii_lowercase())
        }
        _ => Err(SqlError::parse("expected a prepared statement name")),
    }
}

/// `tarn_tree_checks`: what the checks of declared table trees (`lint`) do
/// with a finding about the table a statement creates or alters: report it
/// as a warning (WARN), refuse the statement (ERROR), or nothing (OFF).
pub const TREE_CHECKS_DEFAULT: &str = "WARN";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TreeChecks {
    Off,
    Warn,
    Error,
}

impl TreeChecks {
    /// A value SET may give the variable (case-insensitive), normalized.
    pub fn parse(v: &str) -> Option<TreeChecks> {
        let v = v.trim().trim_matches(|c| c == '\'' || c == '"');
        Some(if v.eq_ignore_ascii_case("OFF") || v == "0" {
            TreeChecks::Off
        } else if v.eq_ignore_ascii_case("WARN") || v.eq_ignore_ascii_case("ON") || v == "1" {
            TreeChecks::Warn
        } else if v.eq_ignore_ascii_case("ERROR") || v.eq_ignore_ascii_case("STRICT") {
            TreeChecks::Error
        } else {
            return None;
        })
    }
    pub fn name(self) -> &'static str {
        match self {
            TreeChecks::Off => "OFF",
            TreeChecks::Warn => "WARN",
            TreeChecks::Error => "ERROR",
        }
    }
}

/// Statements that read the last statement's diagnostics, which they must
/// not clear: SHOW WARNINGS / ERRORS / COUNT(*) ..., @@warning_count.
pub fn reads_diagnostics(sql: &str) -> bool {
    let expanded = crate::prep::expand_comments(sql);
    if matches!(recognize(&expanded), Ok(Some(Admin::ShowWarnings { .. }))) {
        return true;
    }
    let mut diagnostic = false;
    crate::prep::substitute_vars(&expanded, false, &mut |reference| {
        if let crate::prep::VarRef::System(name) = reference {
            diagnostic |= matches!(sysvar_name(name).as_str(), "warning_count" | "error_count");
        }
        None
    });
    diagnostic
}

pub(crate) fn bad_value(var: &str, v: &str) -> SqlError {
    SqlError::new(
        1231,
        "42000",
        format!("Variable '{}' can't be set to the value of '{}'", var, v),
    )
}

/// Skip the rest of one SET assignment: up to the next comma outside
/// parentheses (sql_mode = CONCAT(@@sql_mode, ',X') has commas inside).
fn skip_assignment(t: &mut Toks) {
    let mut depth = 0i32;
    while !t.done() {
        match t.peek_up(0).as_str() {
            "(" => depth += 1,
            ")" => depth -= 1,
            "," if depth <= 0 => break,
            _ => {}
        }
        t.i += 1;
    }
}

/// ISOLATION LEVEL READ UNCOMMITTED | READ COMMITTED | REPEATABLE READ | SERIALIZABLE
fn isolation_level(t: &mut Toks) -> SqlResult<Isolation> {
    let lvl = if t.is("SERIALIZABLE") {
        Isolation::Serializable
    } else if t.is("REPEATABLE") && t.is("READ") {
        Isolation::RepeatableRead
    } else if t.is("READ") {
        if t.is("COMMITTED") {
            Isolation::ReadCommitted
        } else if t.is("UNCOMMITTED") {
            Isolation::ReadUncommitted
        } else {
            return Err(SqlError::parse("unknown isolation level"));
        }
    } else {
        return Err(SqlError::parse("unknown isolation level"));
    };
    Ok(lvl)
}

fn parse_set(t: &mut Toks, sql: &str) -> SqlResult<Admin> {
    let mut items = Vec::new();
    loop {
        if t.done() {
            break;
        }
        let mut scope = VarScope::Session;
        let mut scoped = false;
        if t.is("GLOBAL") || t.is("@@GLOBAL") || t.is("PERSIST") || t.is("PERSIST_ONLY") {
            scope = VarScope::Global;
            scoped = true;
        } else if t.is("SESSION") || t.is("LOCAL") || t.is("@@SESSION") || t.is("@@LOCAL") {
            scoped = true;
        }
        let w = t.peek_up(0);
        if w == "TRANSACTION" {
            // SET [GLOBAL | SESSION] TRANSACTION characteristic [, ...]:
            // without a scope it applies to the next transaction only.
            t.i += 1;
            let sc = if scoped { scope } else { VarScope::Next };
            loop {
                if t.is("ISOLATION") {
                    if !t.is("LEVEL") {
                        return Err(SqlError::parse("expected LEVEL after ISOLATION"));
                    }
                    items.push(SetItem::Isolation(sc, isolation_level(t)?));
                } else if t.is("READ") {
                    let _ = t.is("WRITE") || t.is("ONLY");
                } else {
                    break;
                }
                if !t.is(",") {
                    break;
                }
            }
            break;
        }
        if matches!(w.as_str(), "NAMES" | "CHARACTER" | "CHARSET") {
            // SET NAMES cs [COLLATE co] | SET CHARACTER SET cs | SET CHARSET cs
            t.i += 1;
            if w == "CHARACTER" {
                let _ = t.is("SET");
            }
            let from = t.offset(t.i, sql.len());
            skip_assignment(t);
            let to = t.offset(t.i, sql.len());
            items.push(SetItem::Var {
                scope: VarScope::Session,
                name: "names".into(),
                rhs: sql[from..to].trim().to_string(),
            });
            if !t.is(",") {
                break;
            }
            continue;
        }
        let mut name = match t.next() {
            Some(n) => n,
            None => break,
        };
        if name == "@" {
            // @`quoted name`, @'quoted name'
            let q = t.next().ok_or_else(|| SqlError::parse("syntax error near SET @"))?;
            name = format!("@{}", unquote(&q));
        }
        let lower = name.to_ascii_lowercase();
        let user_var = lower.starts_with('@') && !lower.starts_with("@@");
        // `SET @@transaction_isolation = ...` (no scope at all) sets the
        // next transaction's level, as SET TRANSACTION does (MySQL 8.0).
        let bare_sysvar = lower.starts_with("@@") && !scoped && !lower.contains('.');
        let mut n = lower.trim_start_matches('@').to_string();
        if user_var {
            n = name[1..].to_ascii_lowercase();
        } else if let Some(rest) = n.strip_prefix("global.").or_else(|| n.strip_prefix("persist.")) {
            scope = VarScope::Global;
            n = rest.to_string();
        } else if let Some(rest) = n.strip_prefix("session.").or_else(|| n.strip_prefix("local.")) {
            n = rest.to_string();
        }
        if !(t.is("=")
            || (t.peek_up(0) == ":" && t.peek_up(1) == "=" && {
                t.i += 2;
                true
            }))
        {
            return Err(SqlError::parse(format!("syntax error near SET {}", name)));
        }
        let first = t.i;
        let from = t.offset(first, sql.len());
        skip_assignment(t);
        let to = t.offset(t.i, sql.len());
        let rhs = sql[from..to].trim().to_string();
        // A single literal token (or - number): typed here; anything else
        // (expressions, variables) is evaluated by the session.
        let ntok = t.i - first;
        let simple = (ntok == 1 && !t.t[first].starts_with('@'))
            || (ntok == 2 && t.t[first] == "-" && t.t[first + 1].bytes().all(|c| c.is_ascii_digit()));
        if user_var {
            items.push(SetItem::Var {
                scope: VarScope::Session,
                name: format!("@{}", n),
                rhs,
            });
        } else if simple
            && matches!(
                n.as_str(),
                "autocommit" | "transaction_isolation" | "tx_isolation" | "innodb_lock_wait_timeout"
            )
        {
            let val = if ntok == 2 {
                Value::str(&rhs)
            } else {
                literal_value(&t.t[first])
            };
            items.push(typed_item(scope, &n, &val, bare_sysvar)?.expect("typed variable"));
        } else {
            items.push(SetItem::Var { scope, name: n, rhs });
        }
        if !t.is(",") {
            break;
        }
    }
    Ok(Admin::Set(items))
}

/// The typed SET item for a variable Tarn acts on, from its value; None
/// for other variables. `bare_sysvar`: written `@@name` without a scope.
pub fn typed_item(scope: VarScope, name: &str, val: &Value, bare_sysvar: bool) -> SqlResult<Option<SetItem>> {
    let raw = match val {
        Value::Null => "NULL".to_string(),
        v => v.to_string(),
    };
    Ok(Some(match name {
        "autocommit" => {
            if scope == VarScope::Global {
                return Err(SqlError::unsupported(
                    "SET GLOBAL autocommit (only session autocommit is supported)",
                ));
            }
            let on = match val {
                Value::Int(i) => *i != 0,
                Value::UInt(u) => *u != 0,
                Value::Str(s) => s.eq_ignore_ascii_case(b"on") || s.as_ref() == b"1",
                Value::Null => return Err(bad_value(name, "NULL")),
                _ => true,
            };
            SetItem::Autocommit(on)
        }
        "transaction_isolation" | "tx_isolation" => {
            let lvl = Isolation::parse(&raw).ok_or_else(|| bad_value(name, &raw))?;
            SetItem::Isolation(if bare_sysvar { VarScope::Next } else { scope }, lvl)
        }
        "innodb_lock_wait_timeout" => {
            // An integer, clamped to 0..=1073741824 (0: do not wait), as
            // MariaDB does; NULL / DEFAULT: the default.
            let secs = match val {
                Value::Null => None,
                Value::Int(i) => Some((*i).clamp(0, 1_073_741_824) as f64),
                Value::UInt(u) => Some((*u).min(1_073_741_824) as f64),
                Value::Str(s) if s.eq_ignore_ascii_case(b"default") => None,
                _ => {
                    let v = raw.trim().replace(' ', "").parse::<i64>().map_err(|_| {
                        SqlError::new(1232, "42000", format!("Incorrect argument type to variable '{}'", name))
                    })?;
                    Some(v.clamp(0, 1_073_741_824) as f64)
                }
            };
            SetItem::LockWaitTimeout(scope, secs)
        }
        _ => return Ok(None),
    }))
}

fn parse_show(t: &mut Toks) -> SqlResult<Admin> {
    let full = t.is("FULL");
    let global = t.is("GLOBAL");
    if !global {
        t.is("SESSION");
    }
    let w = t.next().unwrap_or_default().to_ascii_uppercase();
    Ok(match w.as_str() {
        "DATABASES" | "SCHEMAS" => Admin::ShowDatabases { like: like_clause(t) },
        "TABLES" => {
            let db = from_db(t);
            Admin::ShowTables {
                db,
                full,
                like: like_clause(t),
            }
        }
        "TABLE" if t.is("STATUS") => {
            let db = from_db(t);
            Admin::ShowTableStatus {
                db,
                like: like_clause(t),
            }
        }
        "COLUMNS" | "FIELDS" => {
            let _ = t.is("FROM") || t.is("IN");
            let (mut db, table) = split_qualified(&t.next().unwrap_or_default());
            if let Some(d) = from_db(t) {
                db = Some(d);
            }
            Admin::ShowColumns { db, table, full }
        }
        "CREATE" => {
            let what = t.next().unwrap_or_default().to_ascii_uppercase();
            if what == "TABLE" {
                let (db, table) = split_qualified(&t.next().unwrap_or_default());
                Admin::ShowCreateTable { db, table }
            } else {
                return Err(SqlError::unsupported(format!("SHOW CREATE {}", what)));
            }
        }
        "INDEX" | "INDEXES" | "KEYS" => {
            let _ = t.is("FROM") || t.is("IN");
            let (mut db, table) = split_qualified(&t.next().unwrap_or_default());
            if let Some(d) = from_db(t) {
                db = Some(d);
            }
            Admin::ShowIndex { db, table }
        }
        "VARIABLES" => Admin::ShowVariables {
            global,
            like: like_clause(t),
        },
        "STATUS" => Admin::ShowStatus { like: like_clause(t) },
        "WARNINGS" => Admin::ShowWarnings { errors: false },
        "ERRORS" => Admin::ShowWarnings { errors: true },
        "ENGINES" => Admin::ShowEngines,
        "COLLATION" => Admin::ShowCollation,
        "CHARSET" | "CHARACTER" => Admin::ShowCharset,
        "PROCESSLIST" => Admin::ShowProcesslist,
        "TARN" if t.is("INTEGRITY") => {
            let db = from_db(t);
            if !t.done() {
                return Err(SqlError::parse("SHOW TARN INTEGRITY [FROM database]"));
            }
            Admin::ShowTarnIntegrity { db }
        }
        "TARN" if t.is("STATISTICS") => {
            if !t.done() {
                return Err(SqlError::parse("unexpected text after SHOW TARN STATISTICS"));
            }
            Admin::ShowTarnStatistics
        }
        "TARN" if t.is("QUERIES") => {
            if !t.done() {
                return Err(SqlError::parse("unexpected text after SHOW TARN QUERIES"));
            }
            Admin::ShowTarnQueries
        }
        "TARN" if t.is("PLAN") => {
            if !t.is("FOR") {
                return Err(SqlError::parse("expected SHOW TARN PLAN FOR statement"));
            }
            let name =
                unquote(&t.next().ok_or_else(|| SqlError::parse("expected prepared statement"))?).to_ascii_lowercase();
            if !t.done() {
                return Err(SqlError::parse("unexpected text after SHOW TARN PLAN"));
            }
            Admin::ShowTarnPlan(name)
        }
        "TARN" if t.is("RPCS") => {
            if !t.done() {
                return Err(SqlError::parse("unexpected text after SHOW TARN RPCS"));
            }
            Admin::ShowTarnRpcs
        }
        "TARN" if t.is("BUCKETS") => Admin::ShowTarnBuckets,
        "TARN" if t.is("SHARDS") => Admin::ShowTarnShards,
        "TARN" if t.is("REBALANCE") => Admin::ShowTarnRebalance,
        "TARN" if t.is("MOVES") => Admin::ShowTarnMoves,
        "TARN" => Admin::ShowTarnStatus,
        "ENGINE" | "PLUGINS" | "MASTER" | "SLAVE" | "REPLICA" | "BINARY" | "GRANTS" | "PRIVILEGES" | "TRIGGERS"
        | "EVENTS" | "PROCEDURE" | "FUNCTION" | "OPEN" => Admin::Noop,
        other => return Err(SqlError::unsupported(format!("SHOW {}", other))),
    })
}

fn parse_select_vars(t: &mut Toks) -> Option<Vec<(String, String)>> {
    t.i = 1;
    let mut out = Vec::new();
    loop {
        let var = t.next()?;
        if !var.starts_with("@@") {
            return None;
        }
        let mut alias = var.clone();
        if t.is("AS") {
            alias = unquote(&t.next()?);
        }
        out.push((alias, var));
        if t.is(",") {
            continue;
        }
        break;
    }
    if t.is("LIMIT") {
        t.next();
    }
    if t.done() {
        Some(out)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diagnostic_lifetime_uses_statement_kind_not_substring() {
        for sql in [
            "SHOW WARNINGS",
            "SHOW ERRORS",
            "SELECT @@warning_count",
            "SELECT @@autocommit,@@warning_count",
            "SET @wc=@@warning_count",
            "SELECT @@warning_count+0",
            "/*!80000 SELECT @@warning_count */",
        ] {
            assert!(reads_diagnostics(sql), "{sql}");
        }
        for sql in [
            "SELECT 'warning_count'",
            "INSERT INTO warning_count VALUES(1)",
            "SELECT 1 /* error_count */",
        ] {
            assert!(!reads_diagnostics(sql), "{sql}");
        }
    }

    #[test]
    fn isolation_contract_preserves_scope_and_reports_active_mode() {
        let sv = SessionVars {
            autocommit: true,
            db: None,
            isolation: Isolation::RepeatableRead,
            global_isolation: Isolation::ReadCommitted,
            global_lock_wait_timeout: 7.0,
            next_isolation: Some(Isolation::Serializable),
            active_mode: Some(crate::txn::Mode::ReadCommitted),
            lock_wait_timeout: 2.0,
            overrides: None,
            collation_connection: "utf8mb4_bin",
            collation_server: "utf8mb4_bin",
            warning_count: 0,
        };
        for (name, value) in [
            ("@@GLOBAL.transaction_isolation", Value::str("READ-COMMITTED")),
            ("@@session.transaction_isolation", Value::str("REPEATABLE-READ")),
            ("@@global.innodb_lock_wait_timeout", Value::Int(7)),
            ("@@innodb_lock_wait_timeout", Value::Int(2)),
            ("@@tarn_effective_isolation", Value::str("STRICT-SERIALIZABLE")),
            ("@@tarn_active_isolation", Value::str("READ-COMMITTED")),
            ("@@tarn_next_isolation", Value::str("SERIALIZABLE")),
            ("@@tarn_isolation_contract", Value::str("TARN")),
        ] {
            assert_eq!(system_var(name, &sv), Some(value), "{name}");
        }
        assert_eq!(Isolation::ReadUncommitted.effective_name(), "READ-COMMITTED");
        assert_eq!(Isolation::Serializable.effective_name(), "STRICT-SERIALIZABLE");
        assert_eq!(recognize("SET GLOBAL autocommit=1").unwrap_err().code, 1235);
        assert_eq!(
            recognize("SHOW GLOBAL VARIABLES LIKE 'transaction_isolation'").unwrap(),
            Some(Admin::ShowVariables {
                global: true,
                like: Some("transaction_isolation".into())
            })
        );
    }

    #[test]
    fn quoted_admin_tokens_preserve_utf8_and_escapes() {
        for name in ["t_雪", "t_éΩ", "t_雪`fin", "t_雪\\fin"] {
            let quoted = format!("`{}`", name.replace('`', "``"));
            assert_eq!(
                recognize(&format!("SHOW CREATE TABLE {quoted}")).unwrap(),
                Some(Admin::ShowCreateTable {
                    db: None,
                    table: name.into()
                })
            );
        }
        for (literal, want) in [
            ("'t_雪🦀%'", "t_雪🦀%"),
            ("\"t_éΩ%\"", "t_éΩ%"),
            ("'t_雪''fin'", "t_雪'fin"),
            (r"'t_雪\'fin'", "t_雪'fin"),
            (r"'t\_雪\%fin'", r"t\_雪\%fin"),
            (r"'t_雪\\fin'", r"t_雪\fin"),
            (r"'\雪%'", "雪%"),
        ] {
            assert_eq!(
                recognize(&format!("SHOW TABLE STATUS LIKE {literal}")).unwrap(),
                Some(Admin::ShowTableStatus {
                    db: None,
                    like: Some(want.into())
                }),
                "{literal}"
            );
        }
        assert_eq!(recognize("USE `db_雪`").unwrap(), Some(Admin::Use("db_雪".into())));
        assert_eq!(
            recognize("SAVEPOINT `sp_雪`").unwrap(),
            Some(Admin::Savepoint("sp_雪".into()))
        );
        assert_eq!(
            recognize("SET @`名_雪` = 'é''雪', @tail = 2").unwrap(),
            Some(Admin::Set(vec![
                SetItem::Var {
                    scope: VarScope::Session,
                    name: "@名_雪".into(),
                    rhs: "'é''雪'".into()
                },
                SetItem::Var {
                    scope: VarScope::Session,
                    name: "@tail".into(),
                    rhs: "2".into()
                },
            ]))
        );
        for sql in [
            "SHOW TABLES LIKE '雪",
            "SHOW TABLES LIKE '雪''",
            "SHOW CREATE TABLE `雪",
            "SHOW TABLES LIKE '雪\\",
        ] {
            assert_eq!(recognize(sql).unwrap_err().code, 1064, "{sql}");
        }
    }

    #[test]
    fn bucket_statements() {
        assert_eq!(recognize("SHOW TARN BUCKETS").unwrap(), Some(Admin::ShowTarnBuckets));
        assert_eq!(recognize("show tarn status").unwrap(), Some(Admin::ShowTarnStatus));
        assert_eq!(
            recognize("TARN MOVE BUCKET 17 TO SHARD 2").unwrap(),
            Some(Admin::MoveBuckets {
                first: 17,
                last: 17,
                to: 2
            })
        );
        assert_eq!(
            recognize("tarn move buckets between 1 and 100 to shard 3;").unwrap(),
            Some(Admin::MoveBuckets {
                first: 1,
                last: 100,
                to: 3
            })
        );
        for bad in [
            "TARN MOVE BUCKET 0 TO SHARD 2",
            "TARN MOVE BUCKET x TO SHARD 2",
            "TARN MOVE BUCKETS BETWEEN 9 AND 3 TO SHARD 2",
            "TARN MOVE BUCKET 5 TO 2",
            "TARN MOVE BUCKET 5 TO SHARD 2 NOW",
        ] {
            assert!(recognize(bad).is_err(), "{}", bad);
        }
    }

    #[test]
    fn rebalance_statements() {
        assert_eq!(
            recognize("TARN REBALANCE").unwrap(),
            Some(Admin::Rebalance {
                by: "size".into(),
                max_moves: 4,
                max_rate: 0
            })
        );
        assert_eq!(
            recognize("tarn rebalance by load max moves 2 max rate 1000000").unwrap(),
            Some(Admin::Rebalance {
                by: "load".into(),
                max_moves: 2,
                max_rate: 1_000_000
            })
        );
        assert_eq!(
            recognize("TARN REBALANCE CANCEL").unwrap(),
            Some(Admin::RebalanceControl("cancel".into()))
        );
        assert_eq!(
            recognize("SHOW TARN REBALANCE").unwrap(),
            Some(Admin::ShowTarnRebalance)
        );
        assert_eq!(recognize("SHOW TARN MOVES").unwrap(), Some(Admin::ShowTarnMoves));
        for bad in [
            "TARN REBALANCE BY COUNT",
            "TARN REBALANCE MAX MOVES",
            "TARN REBALANCE MAX MOVES x",
            "TARN REBALANCE PAUSE NOW",
            "TARN REBALANCE NOW",
        ] {
            assert!(recognize(bad).is_err(), "{}", bad);
        }
    }

    #[test]
    fn shard_statements() {
        assert_eq!(recognize("SHOW TARN SHARDS").unwrap(), Some(Admin::ShowTarnShards));
        assert_eq!(
            recognize("TARN ADD SHARD 3 '127.0.0.1:3331, 127.0.0.1:3332'").unwrap(),
            Some(Admin::AddShard {
                id: 3,
                replicas: vec!["127.0.0.1:3331".into(), "127.0.0.1:3332".into()]
            })
        );
        assert_eq!(
            recognize("tarn add shard 4 \"h:1\";").unwrap(),
            Some(Admin::AddShard {
                id: 4,
                replicas: vec!["h:1".into()]
            })
        );
        assert_eq!(
            recognize("TARN REMOVE SHARD 2").unwrap(),
            Some(Admin::RemoveShard { id: 2 })
        );
        for bad in [
            "TARN REMOVE SHARD",
            "TARN REMOVE SHARD 0",
            "TARN REMOVE 2",
            "TARN REMOVE SHARD 2 NOW",
            "TARN ADD SHARD 0 'h:1'",
            "TARN ADD SHARD x 'h:1'",
            "TARN ADD SHARD 3",
            "TARN ADD SHARD 3 ''",
            "TARN ADD SHARD 3 h",
            "TARN ADD 3 'h:1'",
            "TARN ADD SHARD 3 'h:1' NOW",
        ] {
            assert!(recognize(bad).is_err(), "{}", bad);
        }
    }

    #[test]
    fn leading_comments_preserve_set_expressions() {
        let statement = "SET @x = 1, @text = '雪? /* literal */ # --', @nil = NULL, @sum := CONCAT('a,b', 'c')";
        let expected = Some(Admin::Set(vec![
            SetItem::Var {
                scope: VarScope::Session,
                name: "@x".into(),
                rhs: "1".into(),
            },
            SetItem::Var {
                scope: VarScope::Session,
                name: "@text".into(),
                rhs: "'雪? /* literal */ # --'".into(),
            },
            SetItem::Var {
                scope: VarScope::Session,
                name: "@nil".into(),
                rhs: "NULL".into(),
            },
            SetItem::Var {
                scope: VarScope::Session,
                name: "@sum".into(),
                rhs: "CONCAT('a,b', 'c')".into(),
            },
        ]));
        for prefix in [
            "",
            "/* comment ? */ ",
            "-- comment ?\n",
            "# comment ?\n",
            "--\tcomment\r\n",
            " \t# first\n/* second */ -- third\n",
        ] {
            assert_eq!(
                recognize(&format!("{prefix}{statement}")).unwrap(),
                expected,
                "{prefix:?}"
            );
        }
    }

    #[test]
    fn interleaved_comments_keep_original_set_rhs_offsets() {
        let sql = "/* first */ SET /* second */ @x /* target */ = /* value */ 1, # next\n\
                   @text := '雪? /* literal */', @tail = 2 /* trailing ? */";
        assert_eq!(
            recognize(sql).unwrap(),
            Some(Admin::Set(vec![
                SetItem::Var {
                    scope: VarScope::Session,
                    name: "@x".into(),
                    rhs: "1".into()
                },
                SetItem::Var {
                    scope: VarScope::Session,
                    name: "@text".into(),
                    rhs: "'雪? /* literal */'".into()
                },
                SetItem::Var {
                    scope: VarScope::Session,
                    name: "@tail".into(),
                    rhs: "2 /* trailing ? */".into()
                },
            ]))
        );
        // Comment markers inside quoted assignment names are ordinary text.
        assert_eq!(
            recognize("/* leading */ SET @`a#/*--` = 'x', @'b?/*' := 2").unwrap(),
            Some(Admin::Set(vec![
                SetItem::Var {
                    scope: VarScope::Session,
                    name: "@a#/*--".into(),
                    rhs: "'x'".into()
                },
                SetItem::Var {
                    scope: VarScope::Session,
                    name: "@b?/*".into(),
                    rhs: "2".into()
                },
            ]))
        );
    }

    #[test]
    fn admin_dash_comments_require_mysql_whitespace_or_control() {
        for prefix in ["-- ", "--\t", "--\r", "--\u{1}"] {
            assert!(matches!(
                recognize(&format!("{prefix} ignored\nSET @x = 1")).unwrap(),
                Some(Admin::Set(_))
            ));
        }
        for sql in [
            "--not_a_comment\nSET @x = 1",
            "--雪\nSET @x = 1",
            "--",
            "# comment only",
            "/* comment only */",
        ] {
            assert_eq!(recognize(sql).unwrap(), None, "{sql:?}");
        }
    }

    #[test]
    fn prepared_normalization_distinguishes_markers_from_comment_and_literal_text() {
        use crate::normalize::{normalize, Mode};
        let literal = "/* ? */ SET @text = '?', @x = 1 -- ?\n";
        assert!(matches!(recognize(literal).unwrap(), Some(Admin::Set(_))));
        assert_eq!(crate::prep::count_markers(literal), 0);
        let normalized = normalize(literal, Mode::Placeholders).unwrap();
        assert_eq!(normalized.n_params, 0);
        assert!(normalized.params.is_empty());
        assert!(normalized.template.contains("'?'"));
        let marker = "# ?\nSET @x = ?, @text = '?' /* ? */";
        assert_eq!(crate::prep::count_markers(marker), 1);
        let normalized = normalize(marker, Mode::Placeholders).unwrap();
        assert_eq!(normalized.n_params, 1);
        assert!(normalized.template.contains("@x = ?1"));
        assert!(normalized.template.contains("'?'"));
        assert_eq!(normalize(marker, Mode::Literals).unwrap_err().code, 1064);
    }

    #[test]
    fn sql_prepared_statements() {
        // issue #34: what mysqldump writes around its MyRocks probes
        assert_eq!(
            recognize("PREPARE s FROM @rocksdb_get_is_supported").unwrap(),
            Some(Admin::Prepare {
                name: "s".into(),
                source: PrepareSource::Var("rocksdb_get_is_supported".into())
            })
        );
        assert_eq!(
            recognize("prepare `St` from 'SELECT ?, ''x'''").unwrap(),
            Some(Admin::Prepare {
                name: "st".into(),
                source: PrepareSource::Text("SELECT ?, 'x'".into())
            })
        );
        assert_eq!(
            recognize("EXECUTE s").unwrap(),
            Some(Admin::Execute {
                name: "s".into(),
                using: vec![]
            })
        );
        assert_eq!(
            recognize("EXECUTE s USING @A, @b").unwrap(),
            Some(Admin::Execute {
                name: "s".into(),
                using: vec!["a".into(), "b".into()]
            })
        );
        assert_eq!(
            recognize("DEALLOCATE PREPARE s").unwrap(),
            Some(Admin::Deallocate("s".into()))
        );
        assert_eq!(
            recognize("DROP PREPARE S").unwrap(),
            Some(Admin::Deallocate("s".into()))
        );
        assert_eq!(recognize("DROP TABLE s").unwrap(), None);
        for bad in [
            "PREPARE s",
            "PREPARE s FROM 1",
            "EXECUTE s USING 1",
            "PREPARE 'x' FROM 'SELECT 1'",
        ] {
            assert_eq!(recognize(bad).err().map(|e| e.code), Some(1064), "{bad}");
        }
    }

    #[test]
    fn recognizes() {
        assert_eq!(recognize("BEGIN").unwrap(), Some(Admin::Begin { read_only: false }));
        assert_eq!(
            recognize("start transaction read only").unwrap(),
            Some(Admin::Begin { read_only: true })
        );
        assert_eq!(
            recognize("SET autocommit=0").unwrap(),
            Some(Admin::Set(vec![SetItem::Autocommit(false)]))
        );
        assert_eq!(
            recognize("SET @@session.autocommit = ON").unwrap(),
            Some(Admin::Set(vec![SetItem::Autocommit(true)]))
        );
        use Isolation::*;
        use VarScope::*;
        let var = |scope, name: &str, rhs: &str| SetItem::Var {
            scope,
            name: name.into(),
            rhs: rhs.into(),
        };
        assert_eq!(
            recognize("SET NAMES utf8mb4").unwrap(),
            Some(Admin::Set(vec![var(Session, "names", "utf8mb4")]))
        );
        assert_eq!(
            recognize("SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO', @`b c` := 1 + 2").unwrap(),
            Some(Admin::Set(vec![
                var(Session, "@old_sql_mode", "@@SQL_MODE"),
                var(Session, "sql_mode", "'NO_AUTO_VALUE_ON_ZERO'"),
                var(Session, "@b c", "1 + 2"),
            ]))
        );
        assert_eq!(
            recognize("SET character_set_client = @saved_cs_client, GLOBAL time_zone = '+00:00'").unwrap(),
            Some(Admin::Set(vec![
                var(Session, "character_set_client", "@saved_cs_client"),
                var(Global, "time_zone", "'+00:00'"),
            ]))
        );
        assert_eq!(
            recognize("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED").unwrap(),
            Some(Admin::Set(vec![SetItem::Isolation(Session, ReadCommitted)]))
        );
        assert_eq!(
            recognize("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ WRITE").unwrap(),
            Some(Admin::Set(vec![SetItem::Isolation(Next, Serializable)]))
        );
        assert_eq!(
            recognize("SET GLOBAL TRANSACTION ISOLATION LEVEL REPEATABLE READ").unwrap(),
            Some(Admin::Set(vec![SetItem::Isolation(Global, RepeatableRead)]))
        );
        // the shape of Rails' connection setup
        assert_eq!(
            recognize(
                "SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci, @@SESSION.sql_mode = CONCAT(@@sql_mode, ',STRICT_ALL_TABLES'), \
                 @@SESSION.sql_auto_is_null = 0, @@SESSION.transaction_isolation = 'READ-COMMITTED', \
                 @@SESSION.innodb_lock_wait_timeout = 7"
            )
            .unwrap(),
            Some(Admin::Set(vec![
                var(Session, "names", "utf8mb4 COLLATE utf8mb4_unicode_ci"),
                var(Session, "sql_mode", "CONCAT(@@sql_mode, ',STRICT_ALL_TABLES')"),
                var(Session, "sql_auto_is_null", "0"),
                SetItem::Isolation(Session, ReadCommitted),
                SetItem::LockWaitTimeout(Session, Some(7.0))
            ]))
        );
        assert_eq!(
            recognize("SET @@GLOBAL.tx_isolation = 'read-uncommitted'").unwrap(),
            Some(Admin::Set(vec![SetItem::Isolation(Global, ReadUncommitted)]))
        );
        assert_eq!(
            recognize("SET @@transaction_isolation = 'READ-COMMITTED', tx_isolation = 'SERIALIZABLE'").unwrap(),
            Some(Admin::Set(vec![
                SetItem::Isolation(Next, ReadCommitted),
                SetItem::Isolation(Session, Serializable)
            ]))
        );
        assert_eq!(
            recognize("SET transaction_isolation = 'SNAPSHOT'").unwrap_err().code,
            1231
        );
        assert_eq!(
            recognize("SET innodb_lock_wait_timeout = -3, @@global.innodb_lock_wait_timeout = DEFAULT").unwrap(),
            Some(Admin::Set(vec![
                SetItem::LockWaitTimeout(Session, Some(0.0)),
                SetItem::LockWaitTimeout(Global, None)
            ]))
        );
        assert_eq!(recognize("SET innodb_lock_wait_timeout = 1.5").unwrap_err().code, 1232);
        assert_eq!(
            recognize("SAVEPOINT sp1").unwrap(),
            Some(Admin::Savepoint("sp1".into()))
        );
        assert_eq!(
            recognize("ROLLBACK TO SAVEPOINT `active_record_1`").unwrap(),
            Some(Admin::RollbackToSavepoint("active_record_1".into()))
        );
        assert_eq!(
            recognize("ROLLBACK WORK TO sp1").unwrap(),
            Some(Admin::RollbackToSavepoint("sp1".into()))
        );
        assert_eq!(
            recognize("RELEASE SAVEPOINT sp1").unwrap(),
            Some(Admin::ReleaseSavepoint("sp1".into()))
        );
        assert_eq!(recognize("ROLLBACK").unwrap(), Some(Admin::Rollback));
        assert_eq!(
            recognize("select @@version_comment limit 1").unwrap(),
            Some(Admin::SelectVars(vec![(
                "@@version_comment".into(),
                "@@version_comment".into()
            )]))
        );
        assert_eq!(
            recognize("SHOW TABLES FROM sbtest LIKE 'sb%'").unwrap(),
            Some(Admin::ShowTables {
                db: Some("sbtest".into()),
                full: false,
                like: Some("sb%".into())
            })
        );
        assert_eq!(recognize("SELECT 1").unwrap(), None);
        assert_eq!(recognize("USE `sbtest`").unwrap(), Some(Admin::Use("sbtest".into())));
    }
}
