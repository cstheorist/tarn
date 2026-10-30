//! One client connection: statement dispatch, prepared statements, explicit
//! and implicit transactions, DML fast paths, SHOW/SET handling and DDL.

use crate::admin::{self, Admin, Isolation, SetItem, VarScope};
use crate::catalog::{Engine, TableDef};
use crate::cluster::{CachedPlan, Cluster};
use crate::collation::Collation;
use crate::exec::{self, matching_rows, run_select, shard_of_row, ReadMode, NEED_TXN};
use crate::expr::{now_string, EvalCtx, Expr};
use crate::lint;
use crate::mpw::{lua_status, Mp, Rd};
use crate::normalize::{normalize, Mode};
use crate::plan::*;
use crate::planner::{self, PlanCtx};
use crate::txn::{key_bytes, select_rows, BufWrite, LockKey, LockOpts, Locked, Mode as TxnMode, Route, Row, Txn, WOp};
use crate::types::Arith;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tarn_common::{Column, ColumnType, SqlError, SqlResult, Value};
use tarn_iproto::Iter;

#[derive(Debug)]
pub enum Output {
    Ok {
        affected: u64,
        last_insert_id: u64,
        info: String,
    },
    Rows {
        columns: Arc<[Column]>,
        rows: Vec<Vec<Value>>,
        resource_guard: Option<tarn_common::ResourceGuard>,
    },
}

impl Output {
    fn ok(affected: u64) -> Output {
        Output::Ok {
            affected,
            last_insert_id: 0,
            info: String::new(),
        }
    }
}

fn autocommit_count(data: &[u8]) -> SqlResult<u64> {
    let mut rd = Rd::new(data);
    if rd.array()? != 1 {
        return Err(crate::mpw::proto("invalid autocommit acknowledgement"));
    }
    let fields = rd.array()?;
    if !matches!(fields, 2 | 3) || rd.value()? != Value::str("ok") {
        return Err(crate::mpw::proto("invalid autocommit acknowledgement"));
    }
    let n = match rd.value()? {
        Value::UInt(n) => n,
        Value::Int(n) if n >= 0 => n as u64,
        _ => return Err(crate::mpw::proto("invalid write count")),
    };
    // Lua and the native writer may return the last affected tuple. It
    // is optional metadata, not an additional result or acknowledgement.
    if fields == 3 {
        if !matches!(rd.buf.first(), Some(0x90..=0x9f | 0xdc | 0xdd | 0xc0)) {
            return Err(crate::mpw::proto("invalid autocommit last tuple"));
        }
        rd.skip()?;
    }
    if !rd.buf.is_empty() {
        return Err(crate::mpw::proto("trailing autocommit acknowledgement"));
    }
    Ok(n)
}

fn autocommit_ack(data: &[u8], shard: u32) -> SqlResult<u64> {
    // The request has already run. A broken success envelope cannot prove
    // a rollback, and must never enter the statement's retry path.
    autocommit_count(data).map_err(|e| crate::mpw::ambiguous(shard, &e))
}

fn known_write_rollback(code: u16) -> bool {
    // These write/commit APIs emit these errors only after refusing or
    // rolling back the local transaction (or proving 2PC ABORT). This is
    // session-history classification, NOT permission to replay a mutation.
    matches!(
        code,
        1041 | 1048 | 1062 | 1146 | 1205 | 1213 | 1264 | 1265 | 1406 | 1412 | 1690
    )
}

fn note_lazy_write_error(history: &mut crate::lazy::History, error: &SqlError) {
    if !known_write_rollback(error.code) {
        history.uncertain();
    }
}

pub struct Prepared {
    template: String,
    diagnostic: Option<crate::observe::Descriptor>,
    pub n_params: usize,
    pub columns: Arc<[Column]>,
    plan: Option<((u64, Option<u64>), Stmt)>,
    admin: Option<Admin>,
    /// Statement with subqueries/UNION: the parsed template (placeholders
    /// ?1..?N), bound and run through `run_complex` at execute time.
    complex: Option<sqlparser::ast::Statement>,
}

pub struct Session {
    retained: crate::budget::Account,
    pub cluster: Arc<Cluster>,
    pub db: Option<String>,
    pub user: String,
    pub conn_id: u32,
    pub autocommit: bool,
    txn: Option<Txn>,
    pub last_insert_id: u64,
    pub(crate) found_rows: u64,
    row_count: i64,
    prepared: HashMap<u32, Prepared>,
    /// While a text query runs: its parameters that were literals naming a
    /// column by their text (`X'41'`).
    lit_names: Vec<crate::normalize::LitName>,
    /// Session isolation level (transaction_isolation).
    pub isolation: Isolation,
    /// SET TRANSACTION ISOLATION LEVEL ...: the next transaction only.
    next_isolation: Option<Isolation>,
    /// innodb_lock_wait_timeout, seconds.
    pub lock_wait_timeout: f64,
    /// The client set CLIENT_FOUND_ROWS: UPDATE (and an upsert that changes
    /// nothing) reports the rows it matched, not only those it changed.
    pub found_rows_flag: bool,
    /// User-defined variables (`@name`, lower-cased).
    user_vars: HashMap<String, Value>,
    /// SQL-level prepared statements by name (lower-cased): their text and
    /// number of `?` markers.
    sql_prepared: HashMap<String, (String, usize)>,
    /// System variables the session set that Tarn reports but does not act
    /// on (character sets, sql_mode, foreign_key_checks, ...).
    sys_vars: HashMap<String, Value>,
    /// sql_mode contains NO_AUTO_VALUE_ON_ZERO: an explicit 0 in an
    /// AUTO_INCREMENT column is stored, not replaced by a generated value.
    no_auto_value_on_zero: bool,
    /// collation_connection (the collation of literals): the one the client
    /// asked for in its handshake, until SET NAMES / SET collation_connection.
    collation_connection: String,
    client_collation: String,
    /// Warnings and notes of the last statement (SHOW WARNINGS).
    warnings: Vec<Warning>,
    omitted_warnings: usize,
    warning_bytes: u64,
    lazy_history: crate::lazy::History,
    lazy_read_your_writes: bool,
    lazy_wait_timeout: u64,
}

/// A warning or note of a statement, as SHOW WARNINGS lists it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Warning {
    pub level: &'static str,
    pub code: u16,
    pub message: String,
}

/// Keep diagnostic storage bounded while preserving the full warning count.
struct DmlWarnings {
    messages: Vec<Warning>,
    total: usize,
    retained: crate::budget::Account,
}

impl DmlWarnings {
    fn new(pool: &Arc<crate::budget::Pool>, available: u64) -> Self {
        Self {
            messages: Vec::new(),
            total: 0,
            retained: pool.account(available),
        }
    }

    fn duplicate(&mut self, bytes: u64, error: impl FnOnce() -> SqlError) -> SqlResult<()> {
        if self.messages.len() < admin::MAX_ERROR_COUNT {
            // Both admissions precede formatting and COMMIT. Failed attempts
            // drop this account; a successful one transfers it without refusal.
            self.retained.charge(bytes)?;
            crate::budget::charge(bytes)?;
            let error = error();
            self.messages.push(Warning {
                level: "Warning",
                code: error.code,
                message: error.message,
            });
        }
        self.total += 1;
        Ok(())
    }
}

/// A finding of the tree checks as a warning or error message.
fn tree_message(f: &lint::Finding) -> String {
    format!("{}.{}: {}", f.db, f.table, f.message)
}

/// Name of the collation a handshake asks for by id (MySQL / MariaDB ids of
/// utf8 collations; others: the server's default).
fn client_collation_name(id: u8) -> Option<&'static str> {
    Some(match id {
        33 => "utf8mb3_general_ci",
        45 => "utf8mb4_general_ci",
        46 => "utf8mb4_bin",
        83 => "utf8mb3_bin",
        192 => "utf8mb3_unicode_ci",
        224 => "utf8mb4_unicode_ci",
        246 => "utf8mb4_unicode_520_ci",
        255 => "utf8mb4_0900_ai_ci",
        _ => return None,
    })
}

pub(crate) const MAX_RETRIES: usize = 20;

/// The mark (`Client::mark`) of a connection to a shard without the native
/// autocommit procedure.
pub(crate) const MARK_NO_NATIVE_WRITE: u32 = 1;

/// Statements that met a stale catalog: a table definition an ALTER TABLE
/// replaced (1412; the shards refuse writes made with it while the ALTER
/// runs and afterwards), or a table, column or database another server
/// created (1146 / 1054 / 1049 from a catalog older than the shards').
/// Outside a transaction, or as the first statement of one, nothing that
/// depends on the old catalog took effect, so the statement runs again with
/// the new catalog -- for 1412 until the ALTER is done, up to the lock wait
/// timeout, as MySQL waits for a metadata lock. Later statements of a
/// transaction end it with 1412.
struct StaleRetry {
    deadline: std::time::Instant,
    attempt: u32,
    /// Catalog version when the current attempt started.
    seen: u64,
}

impl StaleRetry {
    fn new(lock_wait_timeout: f64) -> StaleRetry {
        StaleRetry {
            deadline: std::time::Instant::now() + std::time::Duration::from_secs_f64(lock_wait_timeout.max(1.0)),
            attempt: 0,
            seen: 0,
        }
    }

    /// Call before each attempt.
    fn start(&mut self, cluster: &Cluster) {
        self.seen = cluster.catalog().version;
    }

    async fn again<T>(&mut self, cluster: &Cluster, r: &SqlResult<T>, autocommit: bool) -> SqlResult<Stale> {
        let Err(e) = r else { return Ok(Stale::Done) };
        if e.moved {
            // A shard refused a bucket the statement was routed by: it did
            // nothing. Read the map again; a statement that can start over
            // runs again with it, until the move lands (between a move's
            // send and its receive, its bucket is nowhere for a moment).
            cluster.refresh_buckets().await?;
            if !autocommit || std::time::Instant::now() > self.deadline {
                return Ok(Stale::Done);
            }
            self.attempt += 1;
            cluster.stats.retries.fetch_add(1, Ordering::Relaxed);
            let ms = (5u64 << self.attempt.min(4)).min(100);
            tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
            return Ok(Stale::Retry);
        }
        match e.code {
            1412 => {
                cluster.refresh_catalog().await?;
                if !autocommit || std::time::Instant::now() > self.deadline {
                    return Ok(Stale::Done);
                }
                self.attempt += 1;
                cluster.stats.retries.fetch_add(1, Ordering::Relaxed);
                let ms = (5u64 << self.attempt.min(4)).min(100);
                tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
                Ok(Stale::Retry)
            }
            1146 | 1054 | 1049 => {
                // The attempt was planned with catalog `seen`: when the
                // shards' (or one another reload installed since) is newer,
                // the statement runs again with it.
                let newer = cluster.catalog().version > self.seen || cluster.catch_up(self.seen).await?;
                if !newer {
                    return Ok(Stale::Done);
                }
                self.attempt += 1;
                // A catalog that keeps changing: report it as such (1412)
                // rather than the error the stale one produced.
                Ok(if autocommit && self.attempt <= 3 {
                    Stale::Retry
                } else {
                    Stale::Changed
                })
            }
            // A shard that cannot be reached may have left the cluster: the
            // statement runs again once this server learns that it has (the
            // others' membership says so).
            2003 | 2013 => {
                let v = cluster.member_ver();
                if !autocommit || std::time::Instant::now() > self.deadline || self.attempt >= 3 {
                    return Ok(Stale::Done);
                }
                if cluster.load_members().await.is_err() || cluster.member_ver() == v {
                    return Ok(Stale::Done);
                }
                cluster.refresh_buckets().await?;
                self.attempt += 1;
                cluster.stats.retries.fetch_add(1, Ordering::Relaxed);
                Ok(Stale::Retry)
            }
            _ => Ok(Stale::Done),
        }
    }
}

enum Stale {
    /// run the statement again
    Retry,
    /// return its result
    Done,
    /// inside a transaction: the catalog changed under it (1412)
    Changed,
}

/// The transaction state a statement started in (see [`Session::restart`]).
struct Restart {
    /// The statement runs outside any transaction.
    autocommit: bool,
    /// It is the first statement of its transaction: nothing the transaction
    /// did depends on the catalog it met, so it may start over.
    first: bool,
    /// The transaction's settings (explicit, isolation, lock wait timeout).
    txn: Option<(bool, TxnMode, f64)>,
    next_isolation: Option<Isolation>,
}

impl Restart {
    fn can_retry(&self) -> bool {
        self.autocommit || self.first
    }
}

/// The tables a SELECT reads.
pub(crate) fn select_tables(p: &SelectPlan) -> Vec<&TableDef> {
    p.source.tables().into_iter().map(|a| &*a.table).collect()
}

/// The tables a DML statement reads or writes.
fn stmt_tables(stmt: &Stmt) -> Vec<&TableDef> {
    match stmt {
        Stmt::Select(p) => select_tables(p),
        Stmt::Insert(p) => {
            let mut v = vec![&*p.table];
            if let InsertSource::Select(s, _) = &p.source {
                v.extend(select_tables(s));
            }
            v
        }
        Stmt::Update(p) => vec![&*p.access.table],
        Stmt::Delete(p) => vec![&*p.access.table],
        _ => Vec::new(),
    }
}

/// A statement inside a transaction must use the definitions its earlier
/// writes were made with (see [`Txn::check_def`]).
pub(crate) fn check_defs(txn: &Txn, tables: Vec<&TableDef>) -> SqlResult<()> {
    tables.into_iter().try_for_each(|t| txn.check_def(t))
}

/// Character sets a client may declare for its connection: Tarn stores and
/// sends text as UTF-8, so only encodings that agree with it on the bytes a
/// client sends are accepted.
fn utf8_compatible_charset(cs: &str) -> bool {
    matches!(
        cs.to_ascii_lowercase()
            .trim_matches(|c| c == '\'' || c == '"' || c == '`'),
        "utf8mb4" | "utf8mb3" | "utf8" | "ascii" | "binary" | "default" | "null"
    )
}

/// Time zones that agree with Tarn's clock and storage (UTC).
fn utc_time_zone(tz: &str) -> bool {
    matches!(
        tz.to_ascii_uppercase().as_str(),
        "+00:00" | "-00:00" | "+0:00" | "-0:00" | "UTC" | "GMT" | "ETC/UTC" | "ETC/GMT" | "Z" | "SYSTEM"
    )
}

/// First keyword of a statement, skipping whitespace and comments.
fn first_keyword(sql: &str) -> &str {
    let mut s = sql;
    loop {
        s = s.trim_start();
        if let Some(r) = s.strip_prefix("/*") {
            if let Some(body) = r.strip_prefix('!') {
                // MySQL versioned comment: its content is executable SQL
                s = body.trim_start_matches(|c: char| c.is_ascii_digit());
                continue;
            }
            match r.find("*/") {
                Some(i) => s = &r[i + 2..],
                None => return "",
            }
        } else if s.starts_with("-- ") || s.starts_with('#') {
            match s.find('\n') {
                Some(i) => s = &s[i + 1..],
                None => return "",
            }
        } else {
            break;
        }
    }
    let end = s.find(|c: char| !c.is_ascii_alphabetic()).unwrap_or(s.len());
    &s[..end]
}

fn is_ddl(sql: &str) -> bool {
    let k = first_keyword(sql);
    ["CREATE", "ALTER", "DROP", "TRUNCATE", "RENAME"]
        .iter()
        .any(|d| k.eq_ignore_ascii_case(d))
}

fn cols(names: &[(&str, ColumnType)]) -> Arc<[Column]> {
    names.iter().map(|(n, t)| Column::new(n, *t)).collect::<Vec<_>>().into()
}

fn analyze_columns() -> Arc<[Column]> {
    cols(&[
        ("Table", ColumnType::VarString),
        ("Op", ColumnType::VarString),
        ("Msg_type", ColumnType::VarString),
        ("Msg_text", ColumnType::VarString),
    ])
}

fn statistics_columns() -> Arc<[Column]> {
    cols(&[
        ("Table", ColumnType::VarString),
        ("Index", ColumnType::VarString),
        ("Prefix_parts", ColumnType::LongLong),
        ("Estimated_table_rows", ColumnType::Double),
        ("Sample_rows", ColumnType::LongLong),
        ("Observed_distinct", ColumnType::LongLong),
        ("Estimated_distinct", ColumnType::Double),
        ("Age_ms", ColumnType::LongLong),
        ("Generation", ColumnType::LongLong),
    ])
}

fn like_ci(s: &str, pat: &Option<String>) -> bool {
    match pat {
        None => true,
        Some(p) => crate::expr::like_match(s.to_ascii_lowercase().as_bytes(), p.to_ascii_lowercase().as_bytes()),
    }
}

impl Session {
    pub fn new(cluster: Arc<Cluster>, conn_id: u32) -> Session {
        let (isolation, lock_wait_timeout) = {
            let g = cluster.globals.lock();
            (g.isolation, g.lock_wait_timeout)
        };
        Session {
            retained: cluster.resources.account(cluster.cfg.resources.query_bytes),
            db: None,
            user: "root".into(),
            conn_id,
            autocommit: true,
            txn: None,
            last_insert_id: 0,
            found_rows: 0,
            row_count: -1,
            prepared: HashMap::new(),
            lit_names: Vec::new(),
            isolation,
            next_isolation: None,
            lock_wait_timeout,
            found_rows_flag: false,
            user_vars: HashMap::new(),
            sql_prepared: HashMap::new(),
            sys_vars: HashMap::new(),
            no_auto_value_on_zero: false,
            collation_connection: cluster.cfg.default_collation.clone(),
            client_collation: cluster.cfg.default_collation.clone(),
            warnings: Vec::new(),
            omitted_warnings: 0,
            warning_bytes: 0,
            lazy_history: crate::lazy::History::default(),
            lazy_read_your_writes: false,
            lazy_wait_timeout: 5,
            cluster,
        }
    }

    /// Number of warnings and notes of the last statement (the count OK and
    /// EOF packets carry).
    pub fn warning_count(&self) -> u16 {
        (self.warnings.len() + self.omitted_warnings).min(u16::MAX as usize) as u16
    }

    pub fn warnings(&self) -> &[Warning] {
        &self.warnings
    }

    /// The character set and collation id from the client's handshake:
    /// literals compare under that collation (collation_connection).
    pub fn set_client_collation(&mut self, id: u8) {
        if let Some(n) = client_collation_name(id) {
            self.client_collation = n.to_string();
            self.collation_connection = n.to_string();
        }
    }

    /// A new transaction of this session: explicit (BEGIN, autocommit off)
    /// takes the isolation set for the next transaction, if any.
    pub(crate) fn new_txn(&mut self, explicit: bool) -> Txn {
        let lvl = if explicit {
            self.next_isolation.take().unwrap_or(self.isolation)
        } else {
            self.isolation
        };
        Txn::with_mode(&self.cluster, explicit, lvl.mode(), self.lock_wait_timeout)
    }

    fn session_vars(&self) -> admin::SessionVars<'_> {
        let globals = self.cluster.globals.lock();
        admin::SessionVars {
            autocommit: self.autocommit,
            db: self.db.as_deref(),
            isolation: self.isolation,
            global_isolation: globals.isolation,
            global_lock_wait_timeout: globals.lock_wait_timeout,
            next_isolation: self.next_isolation,
            active_mode: self.txn.as_ref().map(|t| t.mode),
            lock_wait_timeout: self.lock_wait_timeout,
            overrides: Some(&self.sys_vars),
            collation_connection: &self.collation_connection,
            collation_server: &self.cluster.cfg.default_collation,
            warning_count: self.warnings.len() + self.omitted_warnings,
        }
    }

    /// Replace `@user` and `@@system` variable references by literals
    /// (`keep_targets`: not the variables a SET assigns). An unknown system
    /// variable is error 1193. None when the text has no references.
    fn substitute_vars(&self, sql: &str, keep_targets: bool) -> SqlResult<Option<String>> {
        if !crate::prep::has_var_refs(sql) {
            return Ok(None);
        }
        let sv = self.session_vars();
        let mut unknown: Option<String> = None;
        let out = crate::prep::substitute_vars(sql, keep_targets, &mut |r| match r {
            crate::prep::VarRef::User(n) => Some(crate::prep::literal(self.user_vars.get(n).unwrap_or(&Value::Null))),
            crate::prep::VarRef::System(n) => match admin::system_var(n, &sv) {
                Some(v) => Some(crate::prep::literal(&v)),
                None => {
                    unknown.get_or_insert_with(|| admin::sysvar_name(n));
                    None
                }
            },
        });
        if let Some(n) = unknown {
            return Err(SqlError::new(1193, "HY000", format!("Unknown system variable '{}'", n)));
        }
        Ok(out)
    }

    /// The value of the expression a SET assigns. `sysvar`: assigned to a
    /// system variable, where a bare word is a string (SET sql_mode =
    /// TRADITIONAL). Returns None for DEFAULT.
    async fn eval_set_value(&mut self, rhs: &str, sysvar: bool) -> SqlResult<Option<Value>> {
        let rhs = rhs.trim();
        if sysvar && rhs.eq_ignore_ascii_case("DEFAULT") {
            return Ok(None);
        }
        let text = self.substitute_vars(rhs, false)?.unwrap_or_else(|| rhs.to_string());
        let t = text.trim();
        let word = !t.is_empty() && t.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_');
        if word {
            let u = t.to_ascii_uppercase();
            return Ok(Some(match u.as_str() {
                "ON" | "TRUE" => Value::Int(1),
                "OFF" | "FALSE" => Value::Int(0),
                "NULL" => Value::Null,
                _ if t.bytes().all(|c| c.is_ascii_digit()) => t
                    .parse::<i64>()
                    .map(Value::Int)
                    .unwrap_or_else(|_| Value::Decimal(t.into())),
                _ if sysvar => Value::str(t),
                _ => {
                    return Err(SqlError::new(
                        1054,
                        "42S22",
                        format!("Unknown column '{}' in 'field list'", t),
                    ))
                }
            }));
        }
        // Anything else is evaluated as a SELECT expression.
        let out = Box::pin(self.query(&format!("SELECT {}", t))).await?;
        Ok(Some(match out {
            Output::Rows { rows, .. } => rows
                .into_iter()
                .next()
                .and_then(|r| r.into_iter().next())
                .unwrap_or(Value::Null),
            Output::Ok { .. } => Value::Null,
        }))
    }

    /// SET of a variable Tarn does not type itself (see `SetItem::Var`).
    async fn set_var(&mut self, scope: VarScope, name: &str, rhs: &str) -> SqlResult<()> {
        if let Some(user) = name.strip_prefix('@') {
            let v = self.eval_set_value(rhs, false).await?.unwrap_or(Value::Null);
            self.set_user_value(user.to_string(), v)?;
            return Ok(());
        }
        // Unsupported GLOBAL assignments must not mutate session behavior
        // (notably sql_mode) or report an unimplemented server default.
        if scope == VarScope::Global
            && !name.starts_with("tarn_")
            && !matches!(
                name,
                "transaction_isolation" | "tx_isolation" | "innodb_lock_wait_timeout"
            )
        {
            return Err(SqlError::unsupported(format!(
                "SET GLOBAL {} (no mutable server default)",
                name
            )));
        }
        if name == "names" {
            // SET NAMES charset [COLLATE collation]
            let mut words = rhs.split_whitespace();
            let cs = words
                .next()
                .unwrap_or("")
                .trim_matches(|c| c == '\'' || c == '"' || c == '`')
                .to_string();
            if !utf8_compatible_charset(&cs) {
                return Err(SqlError::unsupported(format!(
                    "character set '{}' (connections are utf8mb4)",
                    cs
                )));
            }
            if cs.eq_ignore_ascii_case("default") {
                for v in [
                    "character_set_client",
                    "character_set_connection",
                    "character_set_results",
                ] {
                    self.sys_vars.remove(v);
                }
                self.collation_connection = self.client_collation.clone();
                return Ok(());
            }
            let mut co = planner::charset_default_collation(&cs)
                .unwrap_or("utf8mb4_general_ci")
                .to_string();
            if words.next().is_some_and(|w| w.eq_ignore_ascii_case("COLLATE")) {
                if let Some(c) = words.next() {
                    co = planner::known_collation(c)?;
                }
            }
            for v in [
                "character_set_client",
                "character_set_connection",
                "character_set_results",
            ] {
                self.set_system_value(v.into(), Value::str(&cs))?;
            }
            self.collation_connection = co;
            return Ok(());
        }
        let value = self.eval_set_value(rhs, true).await?;
        let bare = false;
        if let Some(item) = admin::typed_item(scope, name, value.as_ref().unwrap_or(&Value::Null), bare)? {
            return Box::pin(self.run_admin(Admin::Set(vec![item]))).await.map(|_| ());
        }
        let text = value.as_ref().map(|v| v.to_string()).unwrap_or_default();
        match name {
            "max_error_count" => {
                if value.is_none() || value.as_ref().and_then(Value::as_i64) == Some(admin::MAX_ERROR_COUNT as i64) {
                    return Ok(());
                }
                return Err(SqlError::unsupported("max_error_count values other than 64"));
            }
            "tarn_isolation_contract" => {
                if scope == VarScope::Global {
                    return Err(SqlError::new(
                        1228,
                        "HY000",
                        "tarn_isolation_contract is a SESSION variable",
                    ));
                }
                if text.eq_ignore_ascii_case("MYSQL") {
                    return Err(SqlError::unsupported(
                        "exact MySQL isolation; use Tarn's documented isolation contract (REPEATABLE READ is strictly serializable)",
                    ));
                }
                if value.is_none() || text.eq_ignore_ascii_case("TARN") {
                    return Ok(());
                }
                return Err(SqlError::new(
                    1231,
                    "42000",
                    "tarn_isolation_contract accepts TARN; exact MYSQL is unsupported",
                ));
            }
            "tarn_effective_isolation" | "tarn_next_isolation" | "tarn_active_isolation" => {
                return Err(SqlError::new(
                    1238,
                    "HY000",
                    format!("Variable '{}' is a read only variable", name),
                ));
            }
            "tarn_legacy_integrity" => {
                if scope == VarScope::Global {
                    return Err(SqlError::new(
                        1228,
                        "HY000",
                        "tarn_legacy_integrity is a SESSION variable",
                    ));
                }
                let policy = match value.as_ref() {
                    None => "REJECT",
                    Some(Value::Str(v)) if v.eq_ignore_ascii_case(b"ALLOW") => "ALLOW",
                    Some(Value::Str(v)) if v.eq_ignore_ascii_case(b"REJECT") => "REJECT",
                    _ => return Err(admin::bad_value(name, &text)),
                };
                self.sys_vars.insert(name.into(), Value::str(policy));
                return Ok(());
            }
            "tarn_lazy_read_your_writes" | "tarn_lazy_wait_timeout" => {
                if scope == VarScope::Global {
                    return Err(SqlError::new(
                        1228,
                        "HY000",
                        format!(
                            "Variable '{}' is a SESSION variable and can't be used with SET GLOBAL",
                            name
                        ),
                    ));
                }
                let v = if name == "tarn_lazy_read_your_writes" {
                    self.lazy_read_your_writes = crate::lazy::flag(value.as_ref())?;
                    Value::Int(self.lazy_read_your_writes as i64)
                } else {
                    self.lazy_wait_timeout = crate::lazy::wait_seconds(value.as_ref())?;
                    Value::Int(self.lazy_wait_timeout as i64)
                };
                self.set_system_value(name.to_string(), v)?;
                return Ok(());
            }
            n if n.starts_with("character_set_") && n != "character_set_system" => {
                if value.is_some() && !matches!(value, Some(Value::Null)) && !utf8_compatible_charset(&text) {
                    return Err(SqlError::unsupported(format!(
                        "character set '{}' (connections are utf8mb4)",
                        text
                    )));
                }
                if n == "character_set_connection" && scope != VarScope::Global {
                    self.collation_connection = match value.as_ref() {
                        Some(Value::Null) | None => self.client_collation.clone(),
                        _ => planner::charset_default_collation(&text)
                            .unwrap_or("utf8mb4_general_ci")
                            .to_string(),
                    };
                }
            }
            "collation_connection" => {
                if scope != VarScope::Global {
                    self.collation_connection = match value.as_ref() {
                        Some(Value::Null) | None => self.client_collation.clone(),
                        _ => planner::known_collation(&text)?,
                    };
                }
                return Ok(());
            }
            "collation_server" | "collation_database" => {
                // per table, from its definition: accepted, not kept
                if value.is_some() && !matches!(value, Some(Value::Null)) {
                    planner::known_collation(&text)?;
                }
                return Ok(());
            }
            "time_zone" => {
                if value.is_some() && !utc_time_zone(&text) {
                    return Err(SqlError::unsupported(format!(
                        "time zone '{}' (Tarn runs in UTC)",
                        text
                    )));
                }
            }
            "tarn_tree_checks" => {
                if scope == VarScope::Global {
                    return Err(SqlError::new(
                        1228,
                        "HY000",
                        "Variable 'tarn_tree_checks' is a SESSION variable and can't be used with SET GLOBAL",
                    ));
                }
                match value.as_ref() {
                    None | Some(Value::Null) => {
                        self.sys_vars.remove(name);
                    }
                    Some(_) => {
                        let v = admin::TreeChecks::parse(&text).ok_or_else(|| admin::bad_value(name, &text))?;
                        self.set_system_value(name.to_string(), Value::str(v.name()))?;
                    }
                }
                return Ok(());
            }
            "sql_mode" => {
                self.no_auto_value_on_zero = value.is_some()
                    && text
                        .split(',')
                        .any(|m| m.trim().eq_ignore_ascii_case("NO_AUTO_VALUE_ON_ZERO"));
            }
            _ => {}
        }
        if scope == VarScope::Global {
            // Only the variables typed above have server-wide defaults.
            return Err(SqlError::unsupported(format!(
                "SET GLOBAL {} (no mutable server default)",
                name
            )));
        }
        match value {
            None => {
                self.sys_vars.remove(name);
            }
            Some(v) => {
                self.set_system_value(name.to_string(), v)?;
            }
        }
        Ok(())
    }

    pub(crate) fn has_txn(&self) -> bool {
        self.txn.is_some()
    }

    /// Install (or take) the current transaction; returns the previous one.
    pub(crate) fn set_txn(&mut self, t: Option<Txn>) -> Option<Txn> {
        std::mem::replace(&mut self.txn, t)
    }

    pub fn in_transaction(&self) -> bool {
        self.txn.as_ref().is_some_and(|t| t.explicit)
    }

    pub fn use_db(&mut self, db: &str) -> SqlResult<()> {
        if !self.cluster.catalog().has_db(db) {
            return Err(SqlError::new(1049, "42000", format!("Unknown database '{}'", db)));
        }
        self.db = Some(db.to_string());
        Ok(())
    }

    /// USE / the database of a new connection: a database another server
    /// has just created may not be in this server's catalog yet.
    pub async fn use_db_fresh(&mut self, db: &str) -> SqlResult<()> {
        match self.use_db(db) {
            Err(e) if e.code == 1049 && self.cluster.refresh_if_newer().await? => self.use_db(db),
            r => r,
        }
    }

    // ---- entry points -------------------------------------------------------

    pub async fn query(&mut self, sql: &str) -> SqlResult<Output> {
        let cluster = self.cluster.clone();
        let query = crate::budget::current().unwrap_or_else(|| self.cluster.resources.query());
        crate::budget::scope(query.clone(), async {
            let result = crate::observe::track(&cluster.stats.query_digests, async {
                crate::budget::charge((sql.len() as u64).saturating_mul(128).saturating_add(4096))?;
                crate::budget::work(sql.len() as u64)?;
                Box::pin(self.query_budgeted(sql)).await
            })
            .await;
            self.finish_budgeted(result, &query)
        })
        .await
    }

    fn finish_budgeted(
        &mut self,
        mut result: SqlResult<Output>,
        query: &Arc<crate::budget::Query>,
    ) -> SqlResult<Output> {
        if result.as_ref().is_err_and(crate::budget::is_exhausted) {
            if let Some(txn) = self.txn.take() {
                txn.rollback(&self.cluster);
            }
        }
        if let Ok(Output::Rows { resource_guard, .. }) = &mut result {
            *resource_guard = Some(query.guard());
        }
        result
    }

    async fn query_budgeted(&mut self, sql: &str) -> SqlResult<Output> {
        self.cluster.stats.queries.fetch_add(1, Ordering::Relaxed);
        let sql = crate::prep::expand_comments(sql);
        let captured;
        let sql = if admin::reads_diagnostics(&sql) {
            sql.as_ref()
        } else {
            // Release old message ownership before this statement can admit
            // new diagnostics. The captured scalars survive stale-plan retries.
            captured = admin::capture_diagnostics(&sql, self.warnings.len() + self.omitted_warnings);
            self.clear_warnings();
            captured.as_deref().unwrap_or(&sql)
        };
        // A statement outside any transaction that met a table definition an
        // ALTER TABLE replaced (1412) took no effect: re-plan and run it
        // again with the new catalog.
        let rp = self.restart_point();
        let mut retry = StaleRetry::new(self.lock_wait_timeout);
        loop {
            retry.start(&self.cluster);
            let r = self.query_once(sql).await;
            match retry.again(&self.cluster, &r, rp.can_retry()).await? {
                Stale::Retry => self.restart(&rp),
                Stale::Done => return r,
                Stale::Changed => return Err(self.definition_changed()),
            }
        }
    }

    fn restart_point(&self) -> Restart {
        Restart {
            autocommit: !self.txn_needed(),
            first: self.txn.as_ref().is_none_or(|t| t.is_empty()),
            txn: self.txn.as_ref().map(|t| (t.explicit, t.mode, t.lock_timeout)),
            next_isolation: self.next_isolation,
        }
    }

    /// Before the first statement of a transaction runs again: undo what it
    /// did, in a fresh transaction with the same settings.
    fn restart(&mut self, rp: &Restart) {
        if rp.autocommit {
            return;
        }
        if let Some(t) = self.txn.take() {
            t.rollback(&self.cluster);
        }
        self.next_isolation = rp.next_isolation;
        self.txn = rp
            .txn
            .map(|(explicit, mode, lock_timeout)| Txn::with_mode(&self.cluster, explicit, mode, lock_timeout));
    }

    /// A statement of a transaction met a table definition newer than the
    /// one its transaction started with: the transaction ends (1412).
    fn definition_changed(&mut self) -> SqlError {
        if let Some(t) = self.txn.take() {
            t.rollback(&self.cluster);
        }
        SqlError::new(1412, "HY000", "Table definition has changed, please retry transaction")
    }

    async fn query_once(&mut self, sql: &str) -> SqlResult<Output> {
        let sql = crate::prep::expand_comments(sql);
        if let Some(a) = admin::recognize(&sql)? {
            if matches!(a, Admin::Commit) {
                crate::observe::describe(crate::observe::Descriptor::new(self.db.as_deref(), "COMMIT"));
            } else if !matches!(a, Admin::Execute { .. }) {
                crate::observe::disable();
            }
            return self.run_admin(a).await;
        }
        if let Some((select, vars)) = crate::prep::split_select_into(&sql) {
            if let Some(diagnostic) = crate::observe::Descriptor::sql(self.db.as_deref(), &sql) {
                crate::observe::describe(diagnostic);
            }
            return self.select_into(&select, &vars).await;
        }
        let substituted = self.substitute_vars(&sql, false)?;
        let sql: &str = substituted.as_deref().unwrap_or(&sql);
        if crate::complex::may_need(sql) {
            // (the parser takes X'123' for a hexadecimal literal, which MariaDB
            // refuses, as the normalizer does)
            normalize(sql, Mode::Placeholders)?;
            if let Some(stmt) = crate::complex::parse_if_needed(sql)? {
                crate::observe::disable();
                return self.run_complex(stmt).await;
            }
        }
        if is_ddl(sql) {
            crate::observe::disable();
            // Literals in DDL are syntax (VARCHAR(20), DEFAULT '0'), not values.
            let stmt = self.plan_cached(sql, &[])?;
            return self.run(&stmt, &[]).await;
        }
        let n = normalize(sql, Mode::Literals)?;
        let stmt = self.plan_cached(&n.template, &n.params)?;
        // (the literals that name columns by their text, while it runs)
        self.lit_names = n.names;
        let r = self.run(&stmt, &n.params).await;
        self.lit_names.clear();
        r
    }

    /// `SELECT ... INTO @a, ...`: at most one row, whose values the
    /// variables take; with no row they keep their values (MySQL warns 1329).
    async fn select_into(&mut self, select: &str, vars: &[String]) -> SqlResult<Output> {
        let rows = match Box::pin(self.query(select)).await? {
            Output::Rows { rows, .. } => rows,
            Output::Ok { .. } => return Err(SqlError::internal("SELECT ... INTO without rows")),
        };
        if rows.len() > 1 {
            return Err(SqlError::new(1172, "42000", "Result consisted of more than one row"));
        }
        let Some(row) = rows.into_iter().next() else {
            return Ok(Output::ok(0));
        };
        if row.len() != vars.len() {
            return Err(SqlError::new(
                1222,
                "21000",
                "The used SELECT statements have a different number of columns",
            ));
        }
        for (v, x) in vars.iter().zip(row) {
            self.set_user_value(v.clone(), x)?;
        }
        Ok(Output::ok(1))
    }

    pub fn prepare(&mut self, id: u32, sql: &str) -> SqlResult<(usize, Arc<[Column]>)> {
        let query = self.cluster.resources.query();
        crate::budget::scope_sync(query, || {
            let old = self.retained.used();
            self.retained.resize(
                old.saturating_add((sql.len() as u64).saturating_mul(128))
                    .saturating_add(4096),
            )?;
            let result = self.prepare_budgeted(id, sql);
            if result.is_err() {
                self.retained.resize(old)?;
            }
            result
        })
    }

    fn prepare_budgeted(&mut self, id: u32, sql: &str) -> SqlResult<(usize, Arc<[Column]>)> {
        let sql = crate::prep::expand_comments(sql);
        let sql: &str = &sql;
        if let Some(a) = admin::recognize(sql)? {
            let columns = match &a {
                Admin::Analyze(text) => {
                    let catalog = self.cluster.catalog();
                    let ctx = PlanCtx {
                        statistics: None,
                        catalog: &catalog,
                        db: self.db.as_deref(),
                        params: &[],
                        conn_coll: &self.collation_connection,
                        default_coll: &self.cluster.cfg.default_collation,
                    };
                    planner::analyze_target(text, &ctx)?;
                    analyze_columns()
                }
                Admin::ShowTarnStatistics => statistics_columns(),
                _ => Arc::from(Vec::new()),
            };
            self.prepared.insert(
                id,
                Prepared {
                    template: sql.into(),
                    diagnostic: matches!(a, Admin::Commit)
                        .then(|| crate::observe::Descriptor::new(self.db.as_deref(), "COMMIT")),
                    n_params: 0,
                    columns: columns.clone(),
                    plan: None,
                    admin: Some(a),
                    complex: None,
                },
            );
            return Ok((0, columns));
        }
        if crate::prep::has_var_refs(sql) {
            return Err(SqlError::unsupported("variables in prepared statements"));
        }
        let n = normalize(sql, Mode::Placeholders)?;
        if crate::complex::may_need(&n.template) {
            if let Some(stmt) = crate::complex::parse_if_needed(&n.template)? {
                self.prepared.insert(
                    id,
                    Prepared {
                        template: n.template,
                        diagnostic: None,
                        n_params: n.n_params,
                        columns: Arc::from(Vec::new()),
                        plan: None,
                        admin: None,
                        complex: Some(stmt),
                    },
                );
                return Ok((n.n_params, Arc::from(Vec::new())));
            }
        }
        let stmt = self.plan_cached(&n.template, &[])?;
        let columns = match &stmt {
            Stmt::Select(p) => p.columns.clone(),
            Stmt::Explain { .. } => vec![Column::new("EXPLAIN", ColumnType::VarString)].into(),
            _ => Arc::from(Vec::new()),
        };
        let version = self.plan_version(&stmt);
        let diagnostic = if matches!(
            stmt,
            Stmt::Select(_) | Stmt::Insert(_) | Stmt::Update(_) | Stmt::Delete(_)
        ) {
            crate::observe::Descriptor::sql(self.db.as_deref(), &n.template)
        } else {
            None
        };
        self.prepared.insert(
            id,
            Prepared {
                template: n.template,
                diagnostic,
                n_params: n.n_params,
                columns: columns.clone(),
                plan: Some((version, stmt)),
                admin: None,
                complex: None,
            },
        );
        Ok((n.n_params, columns))
    }

    pub async fn execute(&mut self, id: u32, params: &[Value]) -> SqlResult<Output> {
        let cluster = self.cluster.clone();
        let query = crate::budget::current().unwrap_or_else(|| self.cluster.resources.query());
        crate::budget::scope(query.clone(), async {
            let result = crate::observe::track(&cluster.stats.query_digests, async {
                crate::budget::charge(crate::budget::row_bytes(params).saturating_mul(4))?;
                self.execute_budgeted(id, params).await
            })
            .await;
            self.finish_budgeted(result, &query)
        })
        .await
    }

    async fn execute_budgeted(&mut self, id: u32, params: &[Value]) -> SqlResult<Output> {
        match self.prepared.get(&id).and_then(|p| p.diagnostic.clone()) {
            Some(diagnostic) => crate::observe::describe(diagnostic),
            None => crate::observe::disable(),
        }
        self.cluster.stats.queries.fetch_add(1, Ordering::Relaxed);
        if !self
            .prepared
            .get(&id)
            .is_some_and(|p| matches!(&p.admin, Some(Admin::ShowWarnings { .. }) | Some(Admin::SelectVars(_))))
        {
            self.clear_warnings();
        }
        let rp = self.restart_point();
        let mut retry = StaleRetry::new(self.lock_wait_timeout);
        loop {
            retry.start(&self.cluster);
            let r = self.execute_once(id, params).await;
            match retry.again(&self.cluster, &r, rp.can_retry()).await? {
                Stale::Retry => self.restart(&rp),
                Stale::Done => return r,
                Stale::Changed => return Err(self.definition_changed()),
            }
        }
    }

    async fn execute_once(&mut self, id: u32, params: &[Value]) -> SqlResult<Output> {
        if let Some(c) = self.prepared.get(&id).and_then(|p| p.complex.clone()) {
            if params.len() != self.prepared[&id].n_params {
                return Err(SqlError::new(
                    1210,
                    "HY000",
                    "Incorrect arguments to mysqld_stmt_execute",
                ));
            }
            return self.run_complex(crate::complex::bind_params(c, params)).await;
        }
        let (admin, stmt) = {
            let p = self
                .prepared
                .get(&id)
                .ok_or_else(|| SqlError::new(1243, "HY000", format!("Unknown prepared statement handler ({})", id)))?;
            if params.len() != p.n_params {
                return Err(SqlError::new(
                    1210,
                    "HY000",
                    "Incorrect arguments to mysqld_stmt_execute",
                ));
            }
            let stmt = match &p.plan {
                Some((v, s)) if *v == self.plan_version(s) => Some(s.clone()),
                _ => None,
            };
            (p.admin.clone(), stmt)
        };
        if let Some(a) = admin {
            return self.run_admin(a).await;
        }
        let stmt = match stmt {
            // A binary value is typed, and compares, as a binary string: its
            // statement is planned for the kinds of its parameters.
            _ if params.iter().any(|v| matches!(v, Value::Bin(_))) => {
                let template = self.prepared[&id].template.clone();
                self.plan_cached(&template, params)?
            }
            Some(Stmt::Select(p))
                if !params.is_empty()
                    && matches!(&p.source,Source::Join { left, .. } if matches!(left.as_ref(),Source::Table(_)))
                    && p.project.iter().all(|e| matches!(e, Expr::Col(_) | Expr::Lit(_)))
                    && p.agg.is_none()
                    && p.limit.is_none()
                    && !p.distinct
                    && p.source.tables().iter().all(|a| {
                        self.cluster
                            .statistics
                            .snapshot(self.cluster.catalog().version)
                            .tables
                            .contains_key(&a.table.space_id)
                    }) =>
            {
                // The projection's metadata is independent of parameters.
                // Use the shared type-keyed cache so bound integer predicates
                // can use hints, while unbound/noninteger plans stay conservative.
                let template = self.prepared[&id].template.clone();
                self.plan_cached(&template, params)?
            }
            Some(s) => s,
            None => {
                // catalog changed since PREPARE: re-plan
                let template = self.prepared[&id].template.clone();
                let s = self.plan_cached(&template, &[])?;
                let version = self.plan_version(&s);
                self.prepared.get_mut(&id).unwrap().plan = Some((version, s.clone()));
                s
            }
        };
        self.run(&stmt, params).await
    }

    pub fn close(&mut self, id: u32) {
        self.prepared.remove(&id);
        let _ = self.retained.resize(self.retained_size());
    }

    fn retained_size(&self) -> u64 {
        let vars = self
            .user_vars
            .iter()
            .chain(self.sys_vars.iter())
            .fold(0u64, |n, (k, v)| {
                n.saturating_add((256 + k.len() as u64 + crate::budget::value_bytes(v)).saturating_mul(8))
            });
        let plans = self.prepared.values().fold(0u64, |n, p| {
            n.saturating_add(4096 + (p.template.len() as u64).saturating_mul(128))
        });
        self.sql_prepared.iter().fold(
            vars.saturating_add(plans).saturating_add(self.warning_bytes),
            |n, (name, (sql, _))| n.saturating_add(4096 + (name.len() as u64 + sql.len() as u64).saturating_mul(128)),
        )
    }

    fn set_user_value(&mut self, name: String, value: Value) -> SqlResult<()> {
        let old = self
            .user_vars
            .get(&name)
            .map_or(0, |v| (256 + name.len() as u64 + crate::budget::value_bytes(v)) * 8);
        let desired = self
            .retained_size()
            .saturating_sub(old)
            .saturating_add((256 + name.len() as u64 + crate::budget::value_bytes(&value)) * 8);
        self.retained.resize(self.retained.used().max(desired))?;
        drop(self.user_vars.insert(name, crate::budget::own_value(value)));
        self.retained.resize(desired)
    }

    fn set_system_value(&mut self, name: String, value: Value) -> SqlResult<()> {
        let old = self
            .sys_vars
            .get(&name)
            .map_or(0, |v| (256 + name.len() as u64 + crate::budget::value_bytes(v)) * 8);
        let desired = self
            .retained_size()
            .saturating_sub(old)
            .saturating_add((256 + name.len() as u64 + crate::budget::value_bytes(&value)) * 8);
        self.retained.resize(self.retained.used().max(desired))?;
        drop(self.sys_vars.insert(name, crate::budget::own_value(value)));
        self.retained.resize(desired)
    }

    pub fn disconnect(&mut self) {
        if let Some(t) = self.txn.take() {
            t.rollback(&self.cluster);
        }
    }

    pub fn status_flags(&self) -> u16 {
        let mut f = 0;
        if self.in_transaction() {
            f |= 1;
        }
        if self.autocommit {
            f |= 2;
        }
        f
    }

    pub(crate) fn plan_cached(&self, template: &str, params: &[Value]) -> SqlResult<Stmt> {
        let db = self.db.clone().unwrap_or_default();
        // Plans depend on collation_connection (literals compare under it),
        // and on the kinds of the parameters' values, by which their
        // expressions are typed: a binary string compares by bytes, and
        // `SELECT ?1` is an integer column for 1 but a string one for 'abc'.
        let mut key = format!("{}\0{}", self.collation_connection, template);
        if !params.is_empty() {
            key.push('\0');
            key.extend(params.iter().map(|v| match v {
                Value::Null => 'n',
                Value::Int(_) => 'i',
                Value::UInt(_) => 'u',
                Value::Float(_) => 'f',
                Value::Decimal(_) => 'd',
                Value::Str(_) => 's',
                Value::Bin(_) => 'b',
            }));
        }
        if let Some(p) = self.cluster.cached_plan(&db, &key) {
            if let Some(diagnostic) = &p.diagnostic {
                crate::observe::describe(diagnostic.clone());
            }
            return Ok(p.stmt.clone());
        }
        let catalog = self.cluster.catalog();
        let statistics = self.cluster.statistics.snapshot(catalog.version);
        let ctx = PlanCtx {
            statistics: Some(&statistics),
            catalog: &catalog,
            db: self.db.as_deref(),
            params,
            conn_coll: &self.collation_connection,
            default_coll: &self.cluster.cfg.default_collation,
        };
        let stmt = planner::plan(template, &ctx)?;
        let cache_guard = tarn_common::ResourceMemory::reserve(
            self.cluster.resources.clone(),
            4096 + (template.len() as u64 + key.len() as u64).saturating_mul(128),
        )
        .ok();
        if matches!(
            stmt,
            Stmt::Select(_) | Stmt::Insert(_) | Stmt::Update(_) | Stmt::Delete(_)
        ) {
            let diagnostic = crate::observe::Descriptor::sql(self.db.as_deref(), template);
            if let Some(diagnostic) = &diagnostic {
                crate::observe::describe(diagnostic.clone());
            }
            if let Some(cache_guard) = cache_guard {
                self.cluster.cache_plan(
                    &db,
                    &key,
                    Arc::new(CachedPlan {
                        resource_guard: cache_guard,
                        statistics_generation: stmt.uses_statistics().then_some(statistics.generation),
                        version: catalog.version,
                        stmt: stmt.clone(),
                        diagnostic,
                    }),
                );
            }
        }
        Ok(stmt)
    }

    fn plan_version(&self, stmt: &Stmt) -> (u64, Option<u64>) {
        let v = self.cluster.catalog().version;
        (
            v,
            stmt.uses_statistics()
                .then(|| self.cluster.statistics.snapshot(v).generation),
        )
    }

    // ---- dispatch -------------------------------------------------------------

    async fn run(&mut self, stmt: &Stmt, params: &[Value]) -> SqlResult<Output> {
        match stmt {
            Stmt::Explain {
                statement,
                catalog_version,
            } => self.explain(statement, *catalog_version, params),
            Stmt::Select(p) => self.run_select(p, params).await,
            Stmt::Insert(_) | Stmt::Update(_) | Stmt::Delete(_) => self.run_dml(stmt, params).await,
            Stmt::Admin(a) => self.run_admin(a.clone()).await,
            ddl => {
                self.implicit_commit().await?;
                self.run_ddl(ddl).await
            }
        }
    }

    fn explain(&self, stmt: &Stmt, version: u64, params: &[Value]) -> SqlResult<Output> {
        let now = now_string();
        let cx = self.cx(params, &now);
        let next = if self.autocommit {
            self.isolation
        } else {
            self.next_isolation.unwrap_or(self.isolation)
        };
        let committed = self.txn.as_ref().map_or(next.mode(), |t| t.mode) == TxnMode::ReadCommitted;
        let buffered = self
            .txn
            .as_ref()
            .map(|t| t.writes.keys().map(|(space, _)| *space).collect())
            .unwrap_or_default();
        let value = exec::diagnostics::render(&self.cluster, stmt, version, &cx, committed, &buffered)?;
        let text = serde_json::to_string(&value).map_err(|e| SqlError::internal(e.to_string()))?;
        Ok(Output::Rows {
            resource_guard: None,
            columns: vec![Column::new("EXPLAIN", ColumnType::VarString)].into(),
            rows: vec![vec![Value::str(&text)]],
        })
    }

    async fn implicit_commit(&mut self) -> SqlResult<()> {
        if let Some(t) = self.txn.take() {
            self.commit_tracked(t).await?;
        }
        Ok(())
    }

    pub(crate) async fn commit_tracked(&mut self, t: Txn) -> SqlResult<()> {
        let lazy = t.writes.values().any(|w| w.table.has_lazy());
        match t.commit(&self.cluster).await {
            Ok(outcome) => {
                self.lazy_history.record(outcome);
                Ok(())
            }
            Err(e) => {
                // A malformed post-write acknowledgement can be uncertain
                // even when it was not classified as a transport error.
                if lazy {
                    note_lazy_write_error(&mut self.lazy_history, &e);
                }
                Err(e)
            }
        }
    }

    pub(crate) fn txn_needed(&self) -> bool {
        self.txn.is_some() || !self.autocommit
    }

    pub(crate) fn ensure_txn(&mut self) {
        if self.txn.is_none() {
            self.txn = Some(self.new_txn(true));
        }
    }

    // ---- SELECT ----------------------------------------------------------------

    async fn run_select(&mut self, p: &Arc<SelectPlan>, params: &[Value]) -> SqlResult<Output> {
        let now = now_string();
        let columns = exec::render_columns(&p.columns, params, &self.lit_names);
        if let Some(lock) = p.lock {
            return self.run_locking_select(p, params, lock, columns).await;
        }
        if self.txn_needed() {
            self.ensure_txn();
            let mut txn = self.txn.take().unwrap();
            let res = match check_defs(&txn, select_tables(p)) {
                Err(e) => Err(e),
                Ok(()) => {
                    let cx = self.cx(params, &now);
                    if txn.mode == TxnMode::ReadCommitted {
                        let mut mode = ReadMode::Statement(&mut txn);
                        run_select(&self.cluster, &mut mode, p, &cx).await
                    } else {
                        let mut mode = ReadMode::Txn(&mut txn);
                        run_select(&self.cluster, &mut mode, p, &cx).await
                    }
                }
            };
            return self.finish_txn_stmt(
                txn,
                res.map(|rows| Output::Rows {
                    columns,
                    rows,
                    resource_guard: None,
                }),
            );
        }
        if crate::lazy::eligible(p) && !self.lazy_history.strong {
            if self.lazy_read_your_writes && !self.lazy_history.catch_up(&self.cluster, self.lazy_wait_timeout).await {
                return Err(SqlError::new(
                    1205,
                    "HY000",
                    "Lazy index catch-up could not complete; retry the read",
                ));
            }
            let cx = self.cx(params, &now);
            if let Some(rows) = crate::lazy::select(&self.cluster, p, &cx).await? {
                self.cluster.stats.lazy_reads.fetch_add(1, Ordering::Relaxed);
                self.found_rows = rows.len() as u64;
                return Ok(Output::Rows {
                    columns,
                    rows,
                    resource_guard: None,
                });
            }
        }
        if crate::lazy::eligible(p) {
            self.cluster.stats.lazy_fallbacks.fetch_add(1, Ordering::Relaxed);
        }
        if self.isolation.mode() == TxnMode::ReadCommitted {
            // READ COMMITTED autocommit: every storage read sees the latest
            // committed state; nothing to validate, so nothing can abort.
            let mut txn = self.new_txn(false);
            let rows = {
                let cx = self.cx(params, &now);
                let mut mode = ReadMode::Statement(&mut txn);
                run_select(&self.cluster, &mut mode, p, &cx).await?
            };
            self.found_rows = rows.len() as u64;
            return Ok(Output::Rows {
                columns,
                rows,
                resource_guard: None,
            });
        }
        // Autocommit: one lock-aware storage read if the plan allows it,
        // otherwise an implicit read-only transaction (validated at the end).
        {
            let cx = self.cx(params, &now);
            let mut mode = ReadMode::Auto { calls: 0 };
            match run_select(&self.cluster, &mut mode, p, &cx).await {
                Ok(rows) => {
                    self.found_rows = rows.len() as u64;
                    return Ok(Output::Rows {
                        columns,
                        rows,
                        resource_guard: None,
                    });
                }
                Err(e) if e.code == NEED_TXN => {}
                Err(e) => return Err(e),
            }
        }
        let mut attempt = 0;
        loop {
            let mut txn = self.new_txn(false);
            let rows = {
                let cx = self.cx(params, &now);
                let mut mode = ReadMode::Txn(&mut txn);
                run_select(&self.cluster, &mut mode, p, &cx).await
            };
            let res = match rows {
                Ok(rows) => txn.commit(&self.cluster).await.map(|_| rows),
                Err(e) => {
                    txn.rollback(&self.cluster);
                    Err(e)
                }
            };
            match res {
                Ok(rows) => {
                    self.found_rows = rows.len() as u64;
                    return Ok(Output::Rows {
                        columns,
                        rows,
                        resource_guard: None,
                    });
                }
                Err(e) if e.code == 1213 && !e.moved && attempt < MAX_RETRIES => {
                    attempt += 1;
                    self.cluster.stats.retries.fetch_add(1, Ordering::Relaxed);
                    backoff(attempt).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// SELECT ... FOR UPDATE / FOR SHARE: row locks in the session's
    /// transaction, or (autocommit) in a statement-long one that releases
    /// them at its end.
    async fn run_locking_select(
        &mut self,
        p: &Arc<SelectPlan>,
        params: &[Value],
        lock: RowLock,
        columns: Arc<[Column]>,
    ) -> SqlResult<Output> {
        let now = now_string();
        if self.txn_needed() {
            self.ensure_txn();
            let mut txn = self.txn.take().unwrap();
            let res = match check_defs(&txn, select_tables(p)) {
                Err(e) => Err(e),
                Ok(()) => {
                    let cx = self.cx(params, &now);
                    exec::run_locking_select(&self.cluster, &mut txn, p, &cx, lock).await
                }
            };
            if let Ok(rows) = &res {
                self.found_rows = rows.len() as u64;
            }
            return self.finish_txn_stmt(
                txn,
                res.map(|rows| Output::Rows {
                    columns,
                    rows,
                    resource_guard: None,
                }),
            );
        }
        let mut txn = self.new_txn(false);
        let res = {
            let cx = self.cx(params, &now);
            exec::run_locking_select(&self.cluster, &mut txn, p, &cx, lock).await
        };
        match res {
            Ok(rows) => {
                self.commit_tracked(txn).await?;
                self.found_rows = rows.len() as u64;
                Ok(Output::Rows {
                    columns,
                    rows,
                    resource_guard: None,
                })
            }
            Err(e) => {
                txn.rollback(&self.cluster);
                Err(e)
            }
        }
    }

    pub(crate) fn cx<'a>(&'a self, params: &'a [Value], now: &'a str) -> EvalCtx<'a> {
        EvalCtx {
            params,
            db: self.db.as_deref(),
            user: &self.user,
            conn_id: self.conn_id,
            last_insert_id: self.last_insert_id,
            found_rows: self.found_rows,
            row_count: self.row_count,
            now,
        }
    }

    /// After a statement inside an explicit/implicit-long transaction:
    /// a conflict ends the transaction (as a MySQL deadlock does).
    pub(crate) fn finish_txn_stmt(&mut self, txn: Txn, res: SqlResult<Output>) -> SqlResult<Output> {
        match res {
            Err(e) if e.code == 1213 || e.code == 1412 || crate::budget::is_exhausted(&e) || txn.doomed.is_some() => {
                txn.rollback(&self.cluster);
                Err(e)
            }
            other => {
                self.txn = Some(txn);
                other
            }
        }
    }

    // ---- DML -------------------------------------------------------------------

    async fn run_dml(&mut self, stmt: &Stmt, params: &[Value]) -> SqlResult<Output> {
        let table = match stmt {
            Stmt::Insert(p) => &p.table,
            Stmt::Update(p) => &p.access.table,
            Stmt::Delete(p) => &p.access.table,
            _ => unreachable!("DML target"),
        };
        if !table.current_integrity_policy() {
            let legacy_allowed =
                matches!(self.sys_vars.get("tarn_legacy_integrity"), Some(Value::Str(v)) if v.as_ref() == b"ALLOW");
            crate::integrity::check_write(table, legacy_allowed)?;
        }
        let now = now_string();
        if self.txn_needed() {
            self.ensure_txn();
            let mut txn = self.txn.take().unwrap();
            if let Err(error) = txn.snapshot_charge() {
                txn.rollback(&self.cluster);
                return Err(error);
            }
            let snapshot = txn.writes.clone();
            let mut warnings = DmlWarnings::new(
                &self.cluster.resources,
                self.cluster
                    .cfg
                    .resources
                    .query_bytes
                    .saturating_sub(self.retained.used()),
            );
            let res = match check_defs(&txn, stmt_tables(stmt)) {
                Err(e) => Err(e),
                Ok(()) => self.dml_in_txn(&mut txn, stmt, params, &now, None, &mut warnings).await,
            };
            if res.is_err() {
                txn.writes = snapshot; // statement atomicity
            } else {
                self.note_dml_warnings(warnings);
            }
            let res = res.map(|(n, id)| self.dml_output(n, id));
            return self.finish_txn_stmt(txn, res);
        }
        // INSERT ... VALUES: evaluate the rows (and draw AUTO_INCREMENT values)
        // once, so a fast-path fallback or a conflict retry reuses the same ids.
        let pre = match stmt {
            Stmt::Insert(p) if matches!(p.source, InsertSource::Values(_)) => {
                let cx = self.cx(params, &now);
                Some(self.insert_rows(p, &cx, None).await?)
            }
            _ => None,
        };
        if let Some(out) = self.dml_fast_path(stmt, params, &now, pre.as_ref()).await? {
            return Ok(out);
        }
        let mut attempt = 0;
        loop {
            let mut txn = self.new_txn(false);
            let mut warnings = DmlWarnings::new(
                &self.cluster.resources,
                self.cluster
                    .cfg
                    .resources
                    .query_bytes
                    .saturating_sub(self.retained.used()),
            );
            let res = match self
                .dml_in_txn(&mut txn, stmt, params, &now, pre.as_ref(), &mut warnings)
                .await
            {
                Ok(r) => self.commit_tracked(txn).await.map(|_| r),
                Err(e) => {
                    txn.rollback(&self.cluster);
                    Err(e)
                }
            };
            match res {
                Ok((n, id)) => {
                    self.note_dml_warnings(warnings);
                    return Ok(self.dml_output(n, id));
                }
                Err(e) if e.code == 1213 && !e.moved && attempt < MAX_RETRIES => {
                    attempt += 1;
                    self.cluster.stats.retries.fetch_add(1, Ordering::Relaxed);
                    backoff(attempt).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn dml_output(&mut self, affected: u64, insert_id: Option<u64>) -> Output {
        if let Some(id) = insert_id {
            self.last_insert_id = id;
        }
        self.row_count = affected as i64;
        Output::Ok {
            affected,
            last_insert_id: insert_id.unwrap_or(0),
            info: String::new(),
        }
    }

    fn clear_warnings(&mut self) {
        self.warnings.clear();
        self.omitted_warnings = 0;
        let retained = self.retained.used() - self.warning_bytes;
        self.retained.resize(retained).expect("shrinking diagnostic ownership");
        self.warning_bytes = 0;
    }

    fn note_dml_warnings(&mut self, warnings: DmlWarnings) {
        self.warning_bytes += warnings.retained.used();
        self.retained.absorb(warnings.retained);
        self.omitted_warnings += warnings.total - warnings.messages.len();
        for warning in warnings.messages {
            self.note_warning(warning);
        }
    }

    fn note_warning(&mut self, warning: Warning) {
        if self.warnings.len() < admin::MAX_ERROR_COUNT {
            self.warnings.push(warning);
        } else {
            self.omitted_warnings += 1;
        }
    }

    /// Single-shard autocommit DML that needs no read round trip: one call to
    /// `tarn_autocommit` (one local transaction on the owning shard).
    async fn dml_fast_path(
        &mut self,
        stmt: &Stmt,
        params: &[Value],
        now: &str,
        pre: Option<&(Vec<Row>, Option<u64>)>,
    ) -> SqlResult<Option<Output>> {
        let cx = self.cx(params, now);
        // wb: the bucket of each write (lua/tarn.lua "Buckets")
        let (shard, writes, insert_id, table, wb) = match stmt {
            // REPLACE on a table with another unique key may have to delete
            // other rows (those it collides with on that key): not here.
            // Nor writes that change a global index (catalog: unique keys
            // without the shard key), whose entries live on other shards.
            Stmt::Insert(p)
                if p.on_dup.is_empty()
                    && !p.ignore
                    && p.table.globals.is_empty()
                    && !(p.replace && p.table.indexes.iter().skip(1).any(|i| i.unique)) =>
            {
                let InsertSource::Values(_) = &p.source else {
                    return Ok(None);
                };
                let (rows, id) = match pre {
                    Some((r, id)) => (r.clone(), *id),
                    None => self.insert_rows(p, &cx, None).await?,
                };
                if rows.is_empty() {
                    return Ok(None);
                }
                let shard = shard_of_row(&self.cluster, &p.table, &rows[0]);
                if rows.iter().any(|r| shard_of_row(&self.cluster, &p.table, r) != shard) {
                    return Ok(None);
                }
                let mut m = Mp::new();
                m.arr(2).arr(rows.len());
                let mut wb = Vec::with_capacity(rows.len());
                for r in &rows {
                    m.arr(3)
                        .str(if p.replace { "replace" } else { "insert" })
                        .uint(p.table.space_id as u64);
                    wb.push(self.cluster.bucket_of(&p.table.shard_key_of(r)));
                    self.encode_tuple(&p.table, r, &mut m);
                }
                (shard, m, id, p.table.clone(), wb)
            }
            Stmt::Update(p) => {
                let Some((pk, ops)) = self.simple_update(p, &cx)? else {
                    return Ok(None);
                };
                let t = &p.access.table;
                let idx = &t.indexes[0];
                let shard = exec::route_prefix(&self.cluster, t, idx, &pk).unwrap_or(0);
                let wb = vec![self.cluster.bucket_of(&t.shard_key_of_pk(&pk))];
                let mut m = Mp::new();
                m.arr(2).arr(1).arr(6).str("update").uint(t.space_id as u64).vals(&pk);
                // Fields by number (1-based): rows written before an ALTER
                // TABLE carry their old format, whose names may differ.
                let fieldno = |name: &str| t.col_index(name).map(|c| t.layout.col_field[c] as u64 + 1).unwrap_or(0);
                // An assigned case-insensitive column in an index: its
                // weight field too.
                let weights: Vec<(u64, Value)> = ops
                    .iter()
                    .filter(|(op, _, _)| *op == "=")
                    .filter_map(|(_, f, v)| {
                        let c = t.col_index(f)?;
                        t.layout.col_weight[c].map(|w| (w as u64 + 1, t.key_part(c, v)))
                    })
                    .collect();
                m.arr(ops.len() + weights.len());
                for (op, field, v) in &ops {
                    m.arr(3).str(op).uint(fieldno(field)).val(v);
                }
                for (field, v) in &weights {
                    m.arr(3).str("=").uint(*field).val(v);
                }
                // Range of every integer column changed by an arithmetic op:
                // the storage side refuses results outside it (MySQL error 1264).
                let ranges: Vec<(&String, (i128, i128))> = ops
                    .iter()
                    .filter(|(op, _, _)| *op == "+")
                    .filter_map(|(_, f, _)| t.col_index(f).map(|c| (f, t.columns[c].ty())))
                    .filter(|(_, ty)| ty.is_integer())
                    .map(|(f, ty)| (f, crate::types::int_range(ty)))
                    .collect();
                m.arr(ranges.len());
                for (f, (lo, hi)) in ranges {
                    m.arr(4).uint(fieldno(f));
                    if lo < 0 {
                        m.int(lo.max(i64::MIN as i128) as i64);
                    } else {
                        m.uint(lo as u64);
                    }
                    if hi > i64::MAX as i128 {
                        m.uint(hi.min(u64::MAX as i128) as u64);
                    } else {
                        m.int(hi as i64);
                    }
                    m.str(f);
                }
                // Rows older than an added column end early: their values
                // of the later fields, for the update to fill in.
                match t.update_pad() {
                    Some((n, pad)) => {
                        m.arr(pad.len() + 1).uint(n as u64);
                        for v in &pad {
                            m.val(v);
                        }
                    }
                    None => {
                        m.nil();
                    }
                }
                (shard, m, None, t.clone(), wb)
            }
            Stmt::Delete(p) if p.access.table.globals.is_empty() => {
                let Some(pk) = self.simple_pk_target(&p.access, &p.filter, &p.order, &cx)? else {
                    return Ok(None);
                };
                let idx = &p.access.table.indexes[0];
                let shard = exec::route_prefix(&self.cluster, &p.access.table, idx, &pk).unwrap_or(0);
                let wb = vec![self.cluster.bucket_of(&p.access.table.shard_key_of_pk(&pk))];
                let mut m = Mp::new();
                m.arr(2)
                    .arr(1)
                    .arr(3)
                    .str("delete")
                    .uint(p.access.table.space_id as u64)
                    .vals(&pk);
                (shard, m, None, p.access.table.clone(), wb)
            }
            _ => return Ok(None),
        };
        // Rows under another transaction's row lock are waited for, up to
        // the session's innodb_lock_wait_timeout (then 1205); the shard
        // refuses writes made with a replaced table definition (1412).
        let mut writes = writes;
        writes.map(4).str("lock_timeout").f64(self.lock_wait_timeout);
        writes.str("found_rows").bool(self.found_rows_flag);
        writes.str("fence").arr(2).uint(table.space_id as u64).uint(table.ver);
        writes.str("wb").arr(wb.len());
        for &b in &wb {
            writes.uint(b as u64);
        }
        let mut attempt = 0;
        let mut admission_retry = crate::vinyl::RetryBudget::default();
        loop {
            crate::observe::commit(1);
            for &bucket in &wb {
                crate::observe::bucket(Some(bucket));
            }
            // Not idempotent (e.g. SET v = v + 1): never re-sent blindly.
            // Lazy sources require Lua capture. Avoid a predictable extra
            // native call; the shard also enforces capture exclusions for
            // stale gateways that do not yet know this metadata.
            let result = crate::budget::autocommit(
                &self.cluster,
                shard,
                matches!(table.engine, crate::catalog::Engine::Memtx) && !table.has_lazy(),
                &writes.0,
                self.lock_wait_timeout,
            )
            .await;
            let (d, _wire) = match result {
                Ok(d) => d,
                Err(e) => {
                    if table.has_lazy() {
                        note_lazy_write_error(&mut self.lazy_history, &e);
                    }
                    return Err(e);
                }
            };
            match admission_retry.pause(&d).await {
                Ok(true) => {
                    self.cluster.stats.retries.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                Ok(false) => {}
                Err(e) => {
                    if table.has_lazy() {
                        note_lazy_write_error(&mut self.lazy_history, &e);
                    }
                    return Err(e);
                }
            }
            match lua_status(&d) {
                Ok(_) => {
                    let n = match autocommit_ack(&d, self.cluster.shards[shard].id) {
                        Ok(n) => n,
                        Err(e) => {
                            if table.has_lazy() {
                                self.lazy_history.uncertain();
                            }
                            return Err(e);
                        }
                    };
                    if table.has_lazy() {
                        self.lazy_history.touch(wb.iter().copied());
                    }
                    return Ok(Some(self.dml_output(n, insert_id)));
                }
                // (A bucket refused: nothing was written; the statement
                // runs again, routed by the map read again.)
                Err(e) if e.code == 1213 && !e.moved && attempt < MAX_RETRIES => {
                    attempt += 1;
                    self.cluster.stats.retries.fetch_add(1, Ordering::Relaxed);
                    backoff(attempt).await;
                }
                Err(e) => {
                    if table.has_lazy() {
                        note_lazy_write_error(&mut self.lazy_history, &e);
                    }
                    return Err(e);
                }
            }
        }
    }

    /// The primary key targeted by an UPDATE/DELETE whose WHERE clause is
    /// exactly an equality on every primary-key column.
    fn simple_pk_target(
        &self,
        access: &TableAccess,
        filter: &Option<Expr>,
        order: &[(Expr, bool)],
        cx: &EvalCtx,
    ) -> SqlResult<Option<Vec<Value>>> {
        let AccessPath::Point { index: 0, keys } = &access.path else {
            return Ok(None);
        };
        if keys.len() != 1 || !order.is_empty() {
            return Ok(None);
        }
        let t = &access.table;
        let pk_cols = &t.indexes[0].cols;
        let mut conj = Vec::new();
        if let Some(f) = filter {
            split(f, &mut conj);
        }
        if conj.len() != pk_cols.len() {
            return Ok(None);
        }
        for c in &conj {
            match c {
                Expr::Cmp(a, crate::expr::CmpOp::Eq, b, coll) => {
                    let col = match (&**a, &**b) {
                        (Expr::Col(x), o) | (o, Expr::Col(x)) if o.is_constant() => *x,
                        _ => return Ok(None),
                    };
                    if !pk_cols.contains(&col) {
                        return Ok(None);
                    }
                    // Nothing re-checks the row: the lookup must match exactly.
                    if t.columns[col].ty().is_text() && t.layout.colls[col] != *coll {
                        return Ok(None);
                    }
                }
                _ => return Ok(None),
            }
        }
        let mut key = Vec::new();
        for (i, e) in keys[0].iter().enumerate() {
            let v = e.eval(&[], cx)?;
            match t.lookup_part(pk_cols[i], &v) {
                Some(x) if !x.is_null() => key.push(x),
                _ => return Ok(None),
            }
        }
        Ok(Some(key))
    }

    /// UPDATE t SET c = const | c = c +/- const WHERE pk = ...: Tarantool
    /// update ops applied in one local transaction, no read round trip.
    fn simple_update(
        &self,
        p: &UpdatePlan,
        cx: &EvalCtx,
    ) -> SqlResult<Option<(Vec<Value>, Vec<(&'static str, String, Value)>)>> {
        let t = &p.access.table;
        if p.limit.is_some() {
            return Ok(None);
        }
        let Some(pk) = self.simple_pk_target(&p.access, &p.filter, &p.order, cx)? else {
            return Ok(None);
        };
        let mut ops = Vec::new();
        for (c, e) in &p.set {
            if t.is_pk_col(*c) || t.shard_key.contains(c) || t.in_global(*c) {
                return Ok(None);
            }
            let col = &t.columns[*c];
            let ty = col.ty();
            match e {
                Expr::Arith(a, op, b)
                    if matches!(op.base(), Arith::Add | Arith::Sub)
                        && matches!(&**a, Expr::Col(x) if x == c)
                        && b.is_constant()
                        && ty.is_integer() =>
                {
                    let d = b.eval(&[], cx)?;
                    let Some(d) = d.as_i64() else { return Ok(None) };
                    let d = if op.base() == Arith::Sub {
                        let Some(n) = d.checked_neg() else { return Ok(None) };
                        n
                    } else {
                        d
                    };
                    ops.push(("+", col.name.clone(), Value::Int(d)));
                }
                e if e.is_constant() => {
                    let v = ty.coerce(e.eval(&[], cx)?, &col.name)?;
                    if v.is_null() && !col.nullable {
                        return Err(SqlError::new(
                            1048,
                            "23000",
                            format!("Column '{}' cannot be null", col.name),
                        ));
                    }
                    ops.push(("=", col.name.clone(), v));
                }
                _ => return Ok(None),
            }
        }
        Ok(Some((pk, ops)))
    }

    fn encode_tuple(&self, t: &TableDef, r: &Row, m: &mut Mp) {
        t.encode_tuple(r, self.cluster.bucket_of(&t.shard_key_of(r)) as u64, m);
    }

    /// Materialize INSERT rows: defaults, AUTO_INCREMENT, type coercion.
    async fn insert_rows(
        &self,
        p: &InsertPlan,
        cx: &EvalCtx<'_>,
        from_select: Option<Vec<Row>>,
    ) -> SqlResult<(Vec<Row>, Option<u64>)> {
        let t = &p.table;
        let ncols = t.columns.len();
        let mut raw: Vec<Vec<Option<Value>>> = Vec::new();
        match (&p.source, from_select) {
            (InsertSource::Values(rows), _) => {
                for r in rows {
                    let mut out = Vec::with_capacity(ncols);
                    for e in r {
                        out.push(match e {
                            Some(e) => Some(e.eval(&[], cx)?),
                            None => None,
                        });
                    }
                    raw.push(out);
                }
            }
            (InsertSource::Select(_, targets), Some(rows)) => {
                for r in rows {
                    let mut out = vec![None; ncols];
                    for (i, &c) in targets.iter().enumerate() {
                        out[c] = Some(r[i].clone());
                    }
                    raw.push(out);
                }
            }
            _ => return Err(SqlError::internal("INSERT ... SELECT without rows")),
        }
        // AUTO_INCREMENT: missing, NULL or 0 (unless NO_AUTO_VALUE_ON_ZERO)
        // get generated values
        let mut first_id = None;
        let zero_generates = !self.no_auto_value_on_zero;
        if let Some(ai) = t.auto_inc {
            let need = raw
                .iter()
                .filter(|r| match &r[ai] {
                    None | Some(Value::Null) => true,
                    Some(v) => zero_generates && v.as_i64() == Some(0),
                })
                .count() as u64;
            let mut next = if need > 0 {
                self.cluster.next_auto_inc(t, need).await?
            } else {
                0
            };
            let mut max_explicit = 0u64;
            for r in raw.iter_mut() {
                let gen = match &r[ai] {
                    None | Some(Value::Null) => true,
                    Some(v) => zero_generates && v.as_i64() == Some(0),
                };
                if gen {
                    first_id.get_or_insert(next);
                    r[ai] = Some(Value::UInt(next));
                    next += 1;
                } else if let Some(v) = r[ai].as_ref().and_then(|v| v.as_i64()) {
                    max_explicit = max_explicit.max(v.max(0) as u64);
                }
            }
            if max_explicit > 0 {
                self.cluster.note_auto_inc(t, max_explicit).await?;
            }
        }
        // The hidden row id of a table without a primary key: never given by
        // statements, always generated.
        if let Some(rid) = t.row_id_col() {
            let mut next = self.cluster.next_row_id(t, raw.len() as u64).await?;
            for r in raw.iter_mut() {
                r[rid] = Some(Value::UInt(next));
                next += 1;
            }
        }
        let mut rows = Vec::with_capacity(raw.len());
        for r in raw {
            let mut row = Vec::with_capacity(ncols);
            for (c, v) in r.into_iter().enumerate() {
                let col = &t.columns[c];
                let v = match v {
                    Some(v) => v,
                    None => t.default_value(c).ok_or_else(|| {
                        SqlError::new(
                            1364,
                            "HY000",
                            format!("Field '{}' doesn't have a default value", col.name),
                        )
                    })?,
                };
                let v = col.ty().coerce(v, &col.name)?;
                if v.is_null() && !col.nullable {
                    return Err(SqlError::new(
                        1048,
                        "23000",
                        format!("Column '{}' cannot be null", col.name),
                    ));
                }
                row.push(v);
            }
            rows.push(row);
        }
        Ok((rows, first_id))
    }

    /// Buffer write `w` of a table together with the changes it makes to the
    /// table's global indexes (unique keys without the shard key, see
    /// `catalog`): `old` is the row it replaces or deletes (None: none). The
    /// entries commit with the row, so the commit refuses a key another row
    /// holds (1062 under the key's name). READ COMMITTED locks the key of
    /// each entry it deletes; the keys it inserts were locked when their
    /// holders were looked up.
    async fn stage(&self, txn: &mut Txn, w: BufWrite, old: Option<&Row>) -> SqlResult<()> {
        let t = w.table.clone();
        for g in &t.globals {
            let new = if w.op == WOp::Delete { None } else { w.row.as_deref() };
            let (before, after) = (old.and_then(|r| g.entry(&t, r)), new.and_then(|r| g.entry(&t, r)));
            let d = tarn_verified::uix::index_delta(
                old.is_some(),
                old.is_some() && before.is_none(),
                new.is_some(),
                new.is_some() && after.is_none(),
                before.is_some() && before == after,
            );
            let k = g.key_len();
            if let (true, Some(e)) = (d.delete_old, &before) {
                let key = e[..k].to_vec();
                let shard = self.cluster.shard_of(&key);
                if txn.mode == TxnMode::ReadCommitted {
                    let x = LockOpts {
                        exclusive: true,
                        ..Default::default()
                    };
                    let lk = LockKey {
                        table: g.table.clone(),
                        index: 0,
                        key: key.clone(),
                    };
                    txn.lock(&self.cluster, shard, &[lk], x).await?;
                }
                txn.buffer(BufWrite {
                    shard,
                    table: g.table.clone(),
                    op: WOp::Delete,
                    row: None,
                    pk: key,
                })?;
            }
            if let (true, Some(e)) = (d.insert_new, after) {
                let key = e[..k].to_vec();
                txn.buffer(BufWrite {
                    shard: self.cluster.shard_of(&key),
                    table: g.table.clone(),
                    op: WOp::Insert,
                    row: Some(e),
                    pk: key,
                })
                .map_err(|e| {
                    // another row of this transaction holds the value: name
                    // it by the row's values (not by weight strings)
                    if e.code != 1062 {
                        return e;
                    }
                    let row = w.row.as_deref().unwrap_or_default();
                    let shown: Vec<String> = g.src[..k].iter().map(|&c| row[c].to_string()).collect();
                    SqlError::dup_key(&shown.join("-"), &t.indexes[g.pos].name)
                })?;
            }
        }
        txn.buffer(w)
    }

    /// The row holding `key` of global index `g` of table `t` (None: no
    /// row): its entry, read on the entry's shard, then the row by primary
    /// key. Serializable reads both through the transaction (validated at
    /// commit); READ COMMITTED locks the key and the row (X) and reads them
    /// under the locks, so concurrent writers of one value queue.
    async fn global_holder(
        &self,
        txn: &mut Txn,
        t: &Arc<TableDef>,
        g: &crate::catalog::GlobalIndex,
        key: Vec<Value>,
    ) -> SqlResult<Option<Row>> {
        // A key dropped meanwhile (its space gone): the catalog changed.
        self.global_holder_once(txn, t, g, key).await.map_err(|e| {
            if e.code == 1146 {
                crate::txn::definition_changed()
            } else {
                e
            }
        })
    }

    async fn global_holder_once(
        &self,
        txn: &mut Txn,
        t: &Arc<TableDef>,
        g: &crate::catalog::GlobalIndex,
        key: Vec<Value>,
    ) -> SqlResult<Option<Row>> {
        let shard = self.cluster.shard_of(&key);
        let entry = if txn.mode == TxnMode::ReadCommitted {
            let x = LockOpts {
                exclusive: true,
                read: true,
                ..Default::default()
            };
            let lk = LockKey {
                table: g.table.clone(),
                index: 0,
                key,
            };
            match txn.lock(&self.cluster, shard, &[lk], x).await?.pop() {
                Some(Locked::Row(r)) => r,
                _ => None,
            }
        } else {
            let st = txn.stream(&self.cluster, shard).await?;
            let bucket = Some(self.cluster.bucket_of(&key));
            let (rows, _) = select_rows(
                &st.client,
                st.id,
                &g.table,
                0,
                &key,
                Iter::Eq,
                Some(1),
                None,
                false,
                st.begin,
                None,
                &Route::in_stream(bucket),
            )
            .await?;
            txn.mark_begun(shard);
            txn.note_point(shard, g.table.space_id, 0, key, bucket)?;
            rows.into_iter().next()
        };
        let Some(entry) = entry else { return Ok(None) };
        let pk = g.holder_pk(t, &entry);
        let row_shard = exec::route_prefix(&self.cluster, t, &t.indexes[0], &pk).unwrap_or(0);
        if txn.mode == TxnMode::ReadCommitted {
            let x = LockOpts {
                exclusive: true,
                read: true,
                ..Default::default()
            };
            let lk = LockKey {
                table: t.clone(),
                index: 0,
                key: pk,
            };
            return Ok(match txn.lock(&self.cluster, row_shard, &[lk], x).await?.pop() {
                Some(Locked::Row(r)) => r,
                _ => None,
            });
        }
        self.row_exists(txn, t, &pk, row_shard).await
    }

    async fn row_exists(&self, txn: &mut Txn, t: &Arc<TableDef>, pk: &[Value], shard: usize) -> SqlResult<Option<Row>> {
        if let Some(b) = txn.buffered(t.space_id, pk) {
            return Ok(b.cloned());
        }
        let st = txn.stream(&self.cluster, shard).await?;
        let bucket = Some(self.cluster.bucket_of(&t.shard_key_of_pk(pk)));
        let (rows, _) = select_rows(
            &st.client,
            st.id,
            t,
            0,
            pk,
            Iter::Eq,
            Some(1),
            None,
            false,
            st.begin,
            None,
            &Route::in_stream(bucket),
        )
        .await?;
        txn.mark_begun(shard);
        txn.note_point(shard, t.space_id, 0, pk.to_vec(), bucket)?;
        Ok(rows.into_iter().next())
    }

    async fn dml_in_txn(
        &self,
        txn: &mut Txn,
        stmt: &Stmt,
        params: &[Value],
        now: &str,
        pre: Option<&(Vec<Row>, Option<u64>)>,
        warnings: &mut DmlWarnings,
    ) -> SqlResult<(u64, Option<u64>)> {
        let cx = self.cx(params, now);
        match stmt {
            Stmt::Insert(p) => {
                let t = &p.table;
                let selected = match &p.source {
                    InsertSource::Select(sp, _) => {
                        if txn.mode == TxnMode::ReadCommitted {
                            let mut mode = ReadMode::Statement(txn);
                            Some(run_select(&self.cluster, &mut mode, sp, &cx).await?)
                        } else {
                            let mut mode = ReadMode::Txn(txn);
                            Some(run_select(&self.cluster, &mut mode, sp, &cx).await?)
                        }
                    }
                    _ => None,
                };
                let (rows, id) = match pre {
                    Some((r, id)) => (r.clone(), *id),
                    None => self.insert_rows(p, &cx, selected).await?,
                };
                let pk_cols = &t.indexes[0].cols;
                let mut affected = 0;
                let plain = p.on_dup.is_empty() && !p.replace && !p.ignore;
                // (Not for a global index on a case-insensitive column: the
                // shard would report a duplicate by its weight string, so the
                // holder is looked up first and 1062 shows the value.)
                if plain && !txn.explicit && txn.mode == TxnMode::Serializable && !t.global_on_weights() {
                    // A single-statement transaction: the shards refuse an
                    // existing key when the INSERT commits (local commit or
                    // PREPARE votes NO with 1062), failing the statement as a
                    // whole -- no read round trip per row.
                    for row in rows {
                        let pk = t.pk_key(&row);
                        if txn.buffered(t.space_id, &pk).is_some() {
                            let shown: Vec<String> = pk_cols.iter().map(|&c| row[c].to_string()).collect();
                            return Err(SqlError::dup_key(&shown.join("-"), &t.pk().name));
                        }
                        let shard = shard_of_row(&self.cluster, t, &row);
                        let w = BufWrite {
                            shard,
                            table: t.clone(),
                            op: WOp::Insert,
                            row: Some(row),
                            pk,
                        };
                        self.stage(txn, w, None).await?;
                        affected += 1;
                    }
                    return Ok((affected, id));
                }
                // The stored rows each new row collides with, by primary key
                // and by every unique key: read through the transaction
                // (serializable) or under row locks on the key values (READ
                // COMMITTED: concurrent inserts of one value wait).
                let stored = self.stored_conflicts(txn, t, &rows).await?;
                for (row, st) in rows.into_iter().zip(stored) {
                    let pk = t.pk_key(&row);
                    let shard = shard_of_row(&self.cluster, t, &row);
                    let (pk_row, uniq) = current_conflicts(txn, t, &row, &pk, st);
                    let conflict = pk_row.clone().or_else(|| uniq.first().map(|(_, r)| r.clone()));
                    match conflict {
                        Some(old) if !p.on_dup.is_empty() => {
                            // ON DUPLICATE KEY UPDATE applies to the row it
                            // collided with; expressions see old row ++ proposed.
                            let mut both = old.clone();
                            both.extend(row.iter().cloned());
                            let mut new = old.clone();
                            for (c, e) in &p.on_dup {
                                let col = &t.columns[*c];
                                new[*c] = col.ty().coerce(e.eval(&both, &cx)?, &col.name)?;
                                both[*c] = new[*c].clone();
                            }
                            if new != old {
                                let old_shard = shard_of_row(&self.cluster, t, &old);
                                self.buffer_row_change(txn, t, &old, new, old_shard).await?;
                                affected += 2;
                            } else if self.found_rows_flag {
                                affected += 1;
                            }
                        }
                        Some(_) if p.replace => {
                            // REPLACE deletes every row it collides with.
                            let mut deleted = 0;
                            for (_, r) in &uniq {
                                let rpk = t.pk_key(r);
                                if rpk != pk {
                                    let w = BufWrite {
                                        shard: shard_of_row(&self.cluster, t, r),
                                        table: t.clone(),
                                        op: WOp::Delete,
                                        row: None,
                                        pk: rpk,
                                    };
                                    self.stage(txn, w, Some(r)).await?;
                                    deleted += 1;
                                }
                            }
                            let op = if pk_row.is_some() { WOp::Replace } else { WOp::Insert };
                            let w = BufWrite {
                                shard,
                                table: t.clone(),
                                op,
                                row: Some(row),
                                pk,
                            };
                            self.stage(txn, w, pk_row.as_ref()).await?;
                            affected += 1 + deleted + pk_row.is_some() as u64;
                        }
                        Some(_) if p.ignore => {
                            let idx = if pk_row.is_some() {
                                t.pk()
                            } else {
                                &t.indexes[uniq[0].0]
                            };
                            let bytes = idx
                                .cols
                                .iter()
                                .fold(256 + idx.name.len() as u64, |n, &c| {
                                    n.saturating_add(crate::budget::value_bytes(&row[c]))
                                })
                                .saturating_mul(8);
                            warnings.duplicate(bytes, || {
                                let shown: Vec<String> = idx.cols.iter().map(|&c| row[c].to_string()).collect();
                                SqlError::dup_key(&shown.join("-"), &idx.name)
                            })?;
                        }
                        Some(_) => {
                            return Err(match (&pk_row, uniq.first()) {
                                (Some(_), _) | (None, None) => {
                                    let shown: Vec<String> = pk_cols.iter().map(|&c| row[c].to_string()).collect();
                                    SqlError::dup_key(&shown.join("-"), &t.pk().name)
                                }
                                (None, Some((ix, _))) => {
                                    let idx = &t.indexes[*ix];
                                    let shown: Vec<String> = idx.cols.iter().map(|&c| row[c].to_string()).collect();
                                    SqlError::dup_key(&shown.join("-"), &idx.name)
                                }
                            });
                        }
                        None => {
                            let w = BufWrite {
                                shard,
                                table: t.clone(),
                                op: WOp::Insert,
                                row: Some(row),
                                pk,
                            };
                            self.stage(txn, w, None).await?;
                            affected += 1;
                        }
                    }
                }
                Ok((affected, id))
            }
            Stmt::Update(p) if txn.mode == TxnMode::ReadCommitted => {
                // READ COMMITTED: candidates from the latest committed state,
                // then each row locked, re-read and re-checked under its lock
                // (TarnCommit!RCLock): SET x = x - 1 starts from the newest
                // version and never loses a concurrent update.
                let t = &p.access.table;
                let rows = self
                    .rc_lock_rows(txn, &p.access, &p.filter, &p.order, &p.limit, &cx)
                    .await?;
                let matched = rows.len() as u64;
                let mut changed = 0;
                for old in rows {
                    let new = self.updated_row(p, t, &old, &cx)?;
                    if new != old {
                        let shard = shard_of_row(&self.cluster, t, &old);
                        self.buffer_row_change(txn, t, &old, new, shard).await?;
                        changed += 1;
                    }
                }
                Ok((if self.found_rows_flag { matched } else { changed }, None))
            }
            Stmt::Delete(p) if txn.mode == TxnMode::ReadCommitted => {
                let t = &p.access.table;
                let rows = self
                    .rc_lock_rows(txn, &p.access, &p.filter, &p.order, &p.limit, &cx)
                    .await?;
                let n = rows.len() as u64;
                for old in rows {
                    let pk = t.pk_key(&old);
                    let shard = shard_of_row(&self.cluster, t, &old);
                    let w = BufWrite {
                        shard,
                        table: t.clone(),
                        op: WOp::Delete,
                        row: None,
                        pk,
                    };
                    self.stage(txn, w, Some(&old)).await?;
                }
                Ok((n, None))
            }
            Stmt::Update(p) => {
                let t = &p.access.table;
                let rows = matching_rows(&self.cluster, txn, &p.access, &p.filter, &p.order, &p.limit, &cx).await?;
                let matched = rows.len() as u64;
                let mut changed = 0;
                for old in rows {
                    let new = self.updated_row(p, t, &old, &cx)?;
                    if new != old {
                        let shard = shard_of_row(&self.cluster, t, &old);
                        self.buffer_row_change(txn, t, &old, new, shard).await?;
                        changed += 1;
                    }
                }
                Ok((if self.found_rows_flag { matched } else { changed }, None))
            }
            Stmt::Delete(p) => {
                let t = &p.access.table;
                let rows = matching_rows(&self.cluster, txn, &p.access, &p.filter, &p.order, &p.limit, &cx).await?;
                let n = rows.len() as u64;
                for old in rows {
                    let pk = t.pk_key(&old);
                    let shard = shard_of_row(&self.cluster, t, &old);
                    let w = BufWrite {
                        shard,
                        table: t.clone(),
                        op: WOp::Delete,
                        row: None,
                        pk,
                    };
                    self.stage(txn, w, Some(&old)).await?;
                }
                Ok((n, None))
            }
            _ => Err(SqlError::internal("not a DML statement")),
        }
    }

    /// The row after an UPDATE's SET list (assignments see earlier ones).
    fn updated_row(&self, p: &UpdatePlan, t: &TableDef, old: &Row, cx: &EvalCtx) -> SqlResult<Row> {
        let mut new = old.clone();
        for (c, e) in &p.set {
            let col = &t.columns[*c];
            let v = col.ty().coerce(e.eval(&new, cx)?, &col.name)?;
            if v.is_null() && !col.nullable {
                return Err(SqlError::new(
                    1048,
                    "23000",
                    format!("Column '{}' cannot be null", col.name),
                ));
            }
            new[*c] = v;
        }
        Ok(new)
    }

    /// For each new row: the stored row with its primary key and, for each
    /// unique secondary index, the stored row holding its value (as read,
    /// before this transaction's own writes are taken into account).
    async fn stored_conflicts(
        &self,
        txn: &mut Txn,
        t: &Arc<TableDef>,
        rows: &[Row],
    ) -> SqlResult<Vec<StoredConflicts>> {
        let uniq: Vec<usize> = (1..t.indexes.len()).filter(|&i| t.indexes[i].unique).collect();
        // (row, slot: 0 = primary key, k = uniq[k - 1]) -> lookup
        let mut lookups: Vec<(usize, usize, usize, u32, Vec<Value>)> = Vec::new(); // row, slot, shard, index, key
        for (i, row) in rows.iter().enumerate() {
            let shard = shard_of_row(&self.cluster, t, row);
            lookups.push((i, 0, shard, 0, t.pk_key(row)));
            for (k, &ix) in uniq.iter().enumerate() {
                if t.indexes[ix].cols.iter().any(|&c| row[c].is_null()) {
                    continue; // NULLs never collide in a unique index
                }
                if t.global_of(ix).is_some() {
                    continue; // below, through the global index
                }
                lookups.push((i, k + 1, shard, t.indexes[ix].id, t.index_key(&t.indexes[ix], row)));
            }
        }
        let mut out: Vec<StoredConflicts> = rows
            .iter()
            .map(|_| StoredConflicts {
                pk: None,
                uniq: vec![None; uniq.len()],
            })
            .collect();
        let place = |out: &mut Vec<StoredConflicts>, i: usize, slot: usize, r: Option<Row>| {
            if slot == 0 {
                out[i].pk = r;
            } else {
                out[i].uniq[slot - 1] = r.map(|r| (uniq[slot - 1], r));
            }
        };
        if txn.mode == TxnMode::ReadCommitted {
            let mut by: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
            for (j, l) in lookups.iter().enumerate() {
                by.entry(l.2).or_default().push(j);
            }
            let x = LockOpts {
                exclusive: true,
                read: true,
                ..Default::default()
            };
            for (shard, js) in by {
                let keys: Vec<LockKey> = js
                    .iter()
                    .map(|&j| LockKey {
                        table: t.clone(),
                        index: lookups[j].3,
                        key: lookups[j].4.clone(),
                    })
                    .collect();
                let res = txn.lock(&self.cluster, shard, &keys, x).await?;
                for (&j, r) in js.iter().zip(res) {
                    if let Locked::Row(r) = r {
                        place(&mut out, lookups[j].0, lookups[j].1, r);
                    }
                }
            }
        } else {
            let mut pending = Vec::new();
            for l in &lookups {
                if l.1 == 0 && txn.buffered(t.space_id, &l.4).is_some() {
                    continue;
                }
                pending.push((l, txn.stream(&self.cluster, l.2).await?));
            }
            let buckets: Vec<Option<u32>> = pending
                .iter()
                .map(|(l, _)| self.cluster.bucket_of_index_key(t, l.3, &l.4))
                .collect();
            let routes: Vec<Route> = buckets.iter().map(|&b| Route::in_stream(b)).collect();
            let futs = pending.iter().zip(&routes).map(|((l, st), route)| {
                select_rows(
                    &st.client,
                    st.id,
                    t,
                    l.3,
                    &l.4,
                    Iter::Eq,
                    Some(1),
                    None,
                    false,
                    st.begin,
                    None,
                    route,
                )
            });
            let results = futures::future::join_all(futs).await;
            for (((l, _), r), b) in pending.into_iter().zip(results).zip(buckets) {
                let (r, _) = r?;
                txn.mark_begun(l.2);
                txn.note_point(l.2, t.space_id, l.3, l.4.clone(), b)?;
                place(&mut out, l.0, l.1, r.into_iter().next());
            }
        }
        // Unique keys without the shard key: their holders through the
        // global indexes.
        for (i, row) in rows.iter().enumerate() {
            for (k, &ix) in uniq.iter().enumerate() {
                let Some(g) = t.global_of(ix) else { continue };
                let Some(e) = g.entry(t, row) else { continue };
                let r = self.global_holder(txn, t, g, e[..g.key_len()].to_vec()).await?;
                place(&mut out, i, k + 1, r);
            }
        }
        Ok(out)
    }

    /// Before a row's unique-key values change (UPDATE, ON DUPLICATE KEY
    /// UPDATE): the unique index whose new value another row holds, if any,
    /// as MySQL checks row by row. READ COMMITTED locks each new value (X)
    /// and reads its holder under the lock, so concurrent writers of one
    /// value wait for each other; serializable reads it through the
    /// transaction (validated at commit).
    async fn changed_unique_conflict(
        &self,
        txn: &mut Txn,
        t: &Arc<TableDef>,
        old: &Row,
        new: &Row,
        old_pk: &[Value],
    ) -> SqlResult<Option<usize>> {
        // (A value that changes only in case keeps its key under a
        // case-insensitive collation.)
        let changed: Vec<usize> = (1..t.indexes.len())
            .filter(|&i| t.indexes[i].unique)
            .filter(|&i| t.index_key(&t.indexes[i], old) != t.index_key(&t.indexes[i], new))
            .filter(|&i| t.indexes[i].cols.iter().all(|&c| !new[c].is_null()))
            .collect();
        if changed.is_empty() {
            return Ok(None);
        }
        let shard = shard_of_row(&self.cluster, t, new);
        let key_of = |i: usize| -> Vec<Value> { t.index_key(&t.indexes[i], new) };
        // Unique keys without the shard key are looked up through their
        // global indexes, the others on the row's shard.
        let (globals, changed): (Vec<usize>, Vec<usize>) = changed.into_iter().partition(|&i| t.global_of(i).is_some());
        let mut stored: Vec<Option<Row>> = Vec::with_capacity(changed.len() + globals.len());
        if !changed.is_empty() && txn.mode == TxnMode::ReadCommitted {
            let keys: Vec<LockKey> = changed
                .iter()
                .map(|&i| LockKey {
                    table: t.clone(),
                    index: t.indexes[i].id,
                    key: key_of(i),
                })
                .collect();
            let x = LockOpts {
                exclusive: true,
                read: true,
                ..Default::default()
            };
            for r in txn.lock(&self.cluster, shard, &keys, x).await? {
                stored.push(match r {
                    Locked::Row(r) => r,
                    _ => None,
                });
            }
        } else {
            for &i in &changed {
                let st = txn.stream(&self.cluster, shard).await?;
                let key = key_of(i);
                let id = t.indexes[i].id;
                let bucket = self.cluster.bucket_of_index_key(t, id, &key);
                let (rows, _) = select_rows(
                    &st.client,
                    st.id,
                    t,
                    id,
                    &key,
                    Iter::Eq,
                    Some(1),
                    None,
                    false,
                    st.begin,
                    None,
                    &Route::in_stream(bucket),
                )
                .await?;
                txn.mark_begun(shard);
                txn.note_point(shard, t.space_id, id, key, bucket)?;
                stored.push(rows.into_iter().next());
            }
        }
        for &i in &globals {
            let g = t.global_of(i).unwrap();
            stored.push(self.global_holder(txn, t, g, key_of(i)).await?);
        }
        // in index order, as MySQL checks them (the first collision names
        // the key in 1062)
        let mut found: Vec<(usize, Option<Row>)> = changed.into_iter().chain(globals).zip(stored).collect();
        found.sort_by_key(|(i, _)| *i);
        let pk_of = |r: &Row| -> Vec<Value> { t.pk_key(r) };
        for (i, holder) in found {
            let same = |r: &Row| t.same_key(&t.indexes[i], r, new);
            // A row this transaction wrote with that value ...
            let mine = txn
                .writes
                .range((t.space_id, bytes::Bytes::new())..)
                .take_while(|((sp, _), _)| *sp == t.space_id)
                .filter_map(|(_, w)| w.row.as_ref())
                .any(|r| same(r) && pk_of(r) != old_pk);
            if mine {
                return Ok(Some(i));
            }
            // ... or a stored one, unless this transaction rewrote it.
            if let Some(r) = holder {
                let rpk = pk_of(&r);
                if rpk != old_pk {
                    match txn.buffered(t.space_id, &rpk) {
                        None => return Ok(Some(i)),
                        Some(Some(b)) if same(b) => return Ok(Some(i)),
                        _ => {}
                    }
                }
            }
        }
        Ok(None)
    }

    /// The rows a READ COMMITTED UPDATE/DELETE changes, each locked (X) and
    /// read under its lock, WHERE re-checked on that version. Rows named by
    /// their full primary key are locked and read in one storage call per
    /// shard (`lock_pk_points`); others are read as candidates from the
    /// latest committed state first (`candidate_rows`, `lock_matching`).
    async fn rc_lock_rows(
        &self,
        txn: &mut Txn,
        access: &TableAccess,
        filter: &Option<Expr>,
        order: &[(Expr, bool)],
        limit: &Option<Expr>,
        cx: &EvalCtx<'_>,
    ) -> SqlResult<Vec<Row>> {
        let want = exec::eval_limit(limit, cx)?.map(|l| l as usize);
        let x = LockOpts {
            exclusive: true,
            ..Default::default()
        };
        if let Some(rows) = exec::lock_pk_points(&self.cluster, txn, access, filter, order, x, want, cx).await? {
            return Ok(rows);
        }
        let cands = exec::candidate_rows(&self.cluster, txn, access, filter, order, cx).await?;
        exec::lock_matching(&self.cluster, txn, &access.table, cands, filter, x, want, cx).await
    }

    async fn buffer_row_change(
        &self,
        txn: &mut Txn,
        t: &Arc<TableDef>,
        old: &Row,
        new: Row,
        shard: usize,
    ) -> SqlResult<()> {
        let pk_cols = &t.indexes[0].cols;
        let old_pk = t.pk_key(old);
        let new_pk = t.pk_key(&new);
        if let Some(ix) = self.changed_unique_conflict(txn, t, old, &new, &old_pk).await? {
            let idx = &t.indexes[ix];
            let shown: Vec<String> = idx.cols.iter().map(|&c| new[c].to_string()).collect();
            return Err(SqlError::dup_key(&shown.join("-"), &idx.name));
        }
        if key_bytes(&old_pk) == key_bytes(&new_pk) {
            let w = BufWrite {
                shard,
                table: t.clone(),
                op: WOp::Replace,
                row: Some(new),
                pk: new_pk,
            };
            return self.stage(txn, w, Some(old)).await;
        }
        // primary key (and possibly shard) changes: delete + insert
        let new_shard = shard_of_row(&self.cluster, t, &new);
        let exists = if txn.mode == TxnMode::ReadCommitted {
            // Lock the new key value: a concurrent insert of it waits.
            let x = LockOpts {
                exclusive: true,
                read: true,
                ..Default::default()
            };
            let key = LockKey {
                table: t.clone(),
                index: 0,
                key: new_pk.clone(),
            };
            let r = txn.lock(&self.cluster, new_shard, &[key], x).await?;
            match txn.buffered(t.space_id, &new_pk) {
                Some(b) => b.is_some(),
                None => matches!(r.first(), Some(Locked::Row(Some(_)))),
            }
        } else {
            self.row_exists(txn, t, &new_pk, new_shard).await?.is_some()
        };
        if exists {
            let shown: Vec<String> = pk_cols.iter().map(|&c| new[c].to_string()).collect();
            return Err(SqlError::dup_key(&shown.join("-"), &t.pk().name));
        }
        let del = BufWrite {
            shard,
            table: t.clone(),
            op: WOp::Delete,
            row: None,
            pk: old_pk,
        };
        self.stage(txn, del, Some(old)).await?;
        let ins = BufWrite {
            shard: new_shard,
            table: t.clone(),
            op: WOp::Insert,
            row: Some(new),
            pk: new_pk,
        };
        self.stage(txn, ins, None).await?;
        Ok(())
    }

    // ---- DDL ---------------------------------------------------------------------

    async fn run_ddl(&mut self, stmt: &Stmt) -> SqlResult<Output> {
        let c = self.cluster.clone();
        match stmt {
            Stmt::CreateTable {
                def,
                if_not_exists,
                auto_increment,
                engine,
            } => {
                let mut def = (**def).clone();
                def.engine = engine.unwrap_or(c.cfg.default_engine);
                let mut found = Vec::new();
                if self.tree_checks() != admin::TreeChecks::Off {
                    c.refresh_if_newer().await?;
                    // IF NOT EXISTS of an existing table changes nothing
                    if !(*if_not_exists && c.catalog().get(&def.db, &def.name).is_some()) {
                        found = self.tree_findings(vec![def.clone()], &[]);
                    }
                }
                self.refuse_tree(&found, &def.db, &def.name)?;
                c.create_table(def, *if_not_exists, *auto_increment, self.lock_wait_timeout)
                    .await?;
                self.note_tree(found);
            }
            Stmt::Alter(plans) => {
                for p in plans {
                    let mut found = Vec::new();
                    if self.tree_checks() != admin::TreeChecks::Off {
                        c.refresh_if_newer().await?;
                        let cat = c.catalog();
                        if let Some(old) = cat.get(&p.db, &p.table) {
                            // an ALTER TABLE that would fail reports its own error
                            if let Ok(ch) = crate::alter::apply(&old, p, &cat, &now_string()) {
                                let new = ch.def;
                                let gone =
                                    !(new.db.eq_ignore_ascii_case(&p.db) && new.name.eq_ignore_ascii_case(&p.table));
                                let removed = if gone {
                                    vec![(p.db.clone(), p.table.clone())]
                                } else {
                                    vec![]
                                };
                                let before = self.tree_findings(vec![(*old).clone()], &[]);
                                let (db, name) = (new.db.clone(), new.name.clone());
                                found = self.tree_findings(vec![new], &removed);
                                found.retain(|f| !before.contains(f));
                                self.refuse_tree(&found, &db, &name)?;
                            }
                        }
                    }
                    c.alter_table(p, self.lock_wait_timeout).await?;
                    self.note_tree(found);
                }
            }
            Stmt::DropTable { tables, if_exists } => {
                let mut found = Vec::new();
                if self.tree_checks() != admin::TreeChecks::Off {
                    c.refresh_if_newer().await?;
                    let cat = c.catalog();
                    let dropped: Vec<TableDef> = tables
                        .iter()
                        .filter_map(|(d, n)| cat.get(d, n))
                        .map(|t| (*t).clone())
                        .collect();
                    let before = self.tree_findings(dropped, &[]);
                    found = self.tree_findings(Vec::new(), tables);
                    found.retain(|f| !before.contains(f));
                }
                c.drop_tables(tables, *if_exists, self.lock_wait_timeout).await?;
                self.note_tree(found);
            }
            Stmt::Truncate { db, table } => c.truncate(db, table, self.lock_wait_timeout).await?,
            Stmt::CreateDatabase { name, if_not_exists } => {
                return Ok(Output::ok(c.create_database(name, *if_not_exists).await?));
            }
            Stmt::DropDatabase { name, if_exists } => {
                let n = c.drop_database(name, *if_exists, self.lock_wait_timeout).await?;
                if self.db.as_deref().is_some_and(|d| d.eq_ignore_ascii_case(name)) {
                    self.db = None;
                }
                return Ok(Output::ok(n));
            }
            _ => return Err(SqlError::internal("not DDL")),
        }
        Ok(Output::ok(0))
    }

    /// `tarn_tree_checks` of this session.
    fn tree_checks(&self) -> admin::TreeChecks {
        self.sys_vars
            .get("tarn_tree_checks")
            .and_then(|v| admin::TreeChecks::parse(&v.to_string()))
            .unwrap_or(admin::TreeChecks::Warn)
    }

    /// The checks of declared table trees (issue #85) around a schema
    /// change: the tables it creates or alters, as they will be (`changed`),
    /// and the tables that name one of those, or one it drops or renames
    /// away (`removed`), as their parent.
    fn tree_findings(&self, changed: Vec<TableDef>, removed: &[(String, String)]) -> Vec<lint::Finding> {
        let cat = self.cluster.catalog();
        let changed: Vec<Arc<TableDef>> = changed
            .into_iter()
            .filter_map(|mut d| d.resolve_types().ok().map(|_| Arc::new(d)))
            .collect();
        let look = lint::lookup_with(&cat, &changed, removed);
        let mut out: Vec<lint::Finding> = changed.iter().flat_map(|t| lint::check_table(&look, t)).collect();
        let is = |t: &TableDef, (d, n): (&str, &str)| t.db.eq_ignore_ascii_case(d) && t.name.eq_ignore_ascii_case(n);
        let names: Vec<(String, String)> = changed
            .iter()
            .map(|t| (t.db.clone(), t.name.clone()))
            .chain(removed.iter().cloned())
            .collect();
        let mut checked: Vec<Arc<TableDef>> = Vec::new();
        for (db, name) in &names {
            for child in lint::children_of(&cat, db, name) {
                let skip = names.iter().any(|(d, n)| is(&child, (d, n)))
                    || checked.iter().any(|t| is(t, (&child.db, &child.name)));
                if !skip {
                    out.extend(lint::check_table(&look, &child));
                    checked.push(child);
                }
            }
        }
        out
    }

    /// Under `tarn_tree_checks = ERROR`, a finding about the table a
    /// statement creates or alters refuses the statement.
    fn refuse_tree(&self, found: &[lint::Finding], db: &str, name: &str) -> SqlResult<()> {
        if self.tree_checks() != admin::TreeChecks::Error {
            return Ok(());
        }
        match found
            .iter()
            .find(|f| f.strict && f.db.eq_ignore_ascii_case(db) && f.table.eq_ignore_ascii_case(name))
        {
            Some(f) => Err(SqlError::new(lint::CODE, "HY000", tree_message(f))),
            None => Ok(()),
        }
    }

    fn note_tree(&mut self, found: Vec<lint::Finding>) {
        for f in &found {
            self.note_warning(Warning {
                level: f.level.name(),
                code: lint::CODE,
                message: tree_message(f),
            });
        }
    }

    // ---- session statements --------------------------------------------------

    async fn run_admin(&mut self, a: Admin) -> SqlResult<Output> {
        if a.reads_catalog() {
            // A schema change that returned on another SQL server is visible
            // here at once, as in MySQL (not only after the next poll).
            self.cluster.refresh_if_newer().await?;
        }
        let catalog = self.cluster.catalog();
        let rows_out = |columns: Arc<[Column]>, rows: Vec<Vec<Value>>| {
            Ok(Output::Rows {
                columns,
                rows,
                resource_guard: None,
            })
        };
        match a {
            Admin::Begin { .. } => {
                self.implicit_commit().await?;
                self.txn = Some(self.new_txn(true));
                Ok(Output::ok(0))
            }
            Admin::Commit => {
                if let Some(t) = self.txn.take() {
                    self.commit_tracked(t).await?;
                }
                Ok(Output::ok(0))
            }
            Admin::Rollback => {
                if let Some(t) = self.txn.take() {
                    t.rollback(&self.cluster);
                }
                Ok(Output::ok(0))
            }
            Admin::Set(items) => {
                for it in items {
                    match it {
                        SetItem::Autocommit(on) => {
                            if on && !self.autocommit {
                                self.implicit_commit().await?;
                            }
                            self.autocommit = on;
                        }
                        SetItem::Isolation(VarScope::Session, lvl) => self.isolation = lvl,
                        SetItem::Isolation(VarScope::Global, lvl) => self.cluster.globals.lock().isolation = lvl,
                        SetItem::Isolation(VarScope::Next, lvl) => {
                            if self.in_transaction() {
                                return Err(SqlError::new(
                                    1568,
                                    "25001",
                                    "Transaction characteristics can't be changed while a transaction is in progress",
                                ));
                            }
                            self.next_isolation = Some(lvl);
                        }
                        SetItem::LockWaitTimeout(VarScope::Global, t) => {
                            let t = t.unwrap_or(self.cluster.cfg.lock_wait_timeout);
                            self.cluster.globals.lock().lock_wait_timeout = t
                        }
                        SetItem::LockWaitTimeout(_, t) => {
                            let t = t.unwrap_or_else(|| self.cluster.globals.lock().lock_wait_timeout);
                            // Read at each lock wait, as InnoDB does.
                            self.lock_wait_timeout = t;
                            if let Some(txn) = self.txn.as_mut() {
                                txn.lock_timeout = t;
                            }
                        }
                        SetItem::Var { scope, name, rhs } => self.set_var(scope, &name, &rhs).await?,
                    }
                }
                Ok(Output::ok(0))
            }
            Admin::Savepoint(name) => {
                if self.txn.is_none() && !self.autocommit {
                    self.ensure_txn();
                }
                if let Some(t) = self.txn.as_mut() {
                    // A savepoint of the same name is replaced.
                    t.savepoints.retain(|(n, _)| !n.eq_ignore_ascii_case(&name));
                    t.snapshot_charge()?;
                    t.savepoint_name_charge(name.len())?;
                    t.savepoints.push((name, t.writes.clone()));
                }
                Ok(Output::ok(0))
            }
            Admin::RollbackToSavepoint(name) => {
                let missing = || SqlError::new(1305, "42000", format!("SAVEPOINT {} does not exist", name));
                let t = self.txn.as_mut().ok_or_else(missing)?;
                let i = t
                    .savepoints
                    .iter()
                    .rposition(|(n, _)| n.eq_ignore_ascii_case(&name))
                    .ok_or_else(missing)?;
                // Undo the writes since the savepoint; keep the row locks and
                // the read set (as InnoDB keeps the locks).
                t.snapshot_charge()?;
                t.writes = t.savepoints[i].1.clone();
                t.savepoints.truncate(i + 1);
                Ok(Output::ok(0))
            }
            Admin::ReleaseSavepoint(name) => {
                let missing = || SqlError::new(1305, "42000", format!("SAVEPOINT {} does not exist", name));
                let t = self.txn.as_mut().ok_or_else(missing)?;
                let i = t
                    .savepoints
                    .iter()
                    .rposition(|(n, _)| n.eq_ignore_ascii_case(&name))
                    .ok_or_else(missing)?;
                t.savepoints.truncate(i);
                Ok(Output::ok(0))
            }
            Admin::Prepare { name, source } => {
                // A failed PREPARE leaves no statement of that name, as in MySQL.
                self.sql_prepared.remove(&name);
                let _ = self.retained.resize(self.retained_size());
                let text = match source {
                    admin::PrepareSource::Text(s) => s,
                    admin::PrepareSource::Var(v) => match self.user_vars.get(&v) {
                        Some(Value::Str(b) | Value::Bin(b)) => String::from_utf8_lossy(b).into_owned(),
                        Some(Value::Null) | None => "NULL".into(),
                        Some(other) => other.to_string(),
                    },
                };
                let text = crate::prep::expand_comments(&text)
                    .trim()
                    .trim_end_matches(';')
                    .to_string();
                let first = first_keyword(&text).to_ascii_uppercase();
                if matches!(first.as_str(), "PREPARE" | "EXECUTE" | "DEALLOCATE")
                    || (first == "DROP" && admin::recognize(&text)?.is_some())
                {
                    return Err(SqlError::new(
                        1295,
                        "HY000",
                        "This command is not supported in the prepared statement protocol yet",
                    ));
                }
                if text.is_empty() || text.eq_ignore_ascii_case("NULL") {
                    return Err(SqlError::parse("You have an error in your SQL syntax: empty statement"));
                }
                let n = crate::prep::count_markers(&text);
                let additional = 4096 + (name.len() as u64 + text.len() as u64).saturating_mul(128);
                self.retained.charge(additional)?;
                self.sql_prepared.insert(name, (text, n));
                Ok(Output::Ok {
                    affected: 0,
                    last_insert_id: 0,
                    info: "Statement prepared".into(),
                })
            }
            Admin::Execute { name, using } => {
                let (text, n) = self.sql_prepared.get(&name).cloned().ok_or_else(|| {
                    SqlError::new(
                        1243,
                        "HY000",
                        format!("Unknown prepared statement handler ({}) given to EXECUTE", name),
                    )
                })?;
                if using.len() != n {
                    return Err(SqlError::new(1210, "HY000", "Incorrect arguments to EXECUTE"));
                }
                let args: Vec<String> = using
                    .iter()
                    .map(|v| crate::prep::literal(self.user_vars.get(v).unwrap_or(&Value::Null)))
                    .collect();
                let sql = crate::prep::bind_markers(&text, &args);
                Box::pin(self.query(&sql)).await
            }
            Admin::Deallocate(name) => match self.sql_prepared.remove(&name) {
                Some(_) => Ok(Output::ok(0)),
                None => Err(SqlError::new(
                    1243,
                    "HY000",
                    format!(
                        "Unknown prepared statement handler ({}) given to DEALLOCATE PREPARE",
                        name
                    ),
                )),
            },
            Admin::Noop => Ok(Output::ok(0)),
            Admin::Analyze(sql) => {
                // As in MariaDB, ANALYZE is an implicit-commit statement.
                self.cluster.refresh_if_newer().await?;
                let catalog = self.cluster.catalog();
                let ctx = PlanCtx {
                    statistics: None,
                    catalog: &catalog,
                    db: self.db.as_deref(),
                    params: &[],
                    conn_coll: &self.collation_connection,
                    default_coll: &self.cluster.cfg.default_collation,
                };
                let t = planner::analyze_target(&sql, &ctx)?;
                self.implicit_commit().await?;
                let _collector = self.cluster.statistics.collecting.try_lock().map_err(|_| {
                    SqlError::new(1205, "HY000", "ANALYZE collector is already running on this gateway")
                })?;
                let stats = crate::statistics::collect(&self.cluster, &t).await?;
                self.cluster.refresh_if_newer().await?;
                if self.cluster.catalog().version != catalog.version {
                    return Err(SqlError::new(
                        1412,
                        "HY000",
                        "Table definition changed during ANALYZE; retry",
                    ));
                }
                self.cluster.statistics.publish(catalog.version, vec![stats]);
                rows_out(
                    analyze_columns(),
                    vec![vec![
                        Value::str(&format!("{}.{}", t.db, t.name)),
                        Value::str("analyze"),
                        Value::str("status"),
                        Value::str("OK"),
                    ]],
                )
            }
            Admin::ShowTarnStatistics => {
                let snapshot = self.cluster.statistics.snapshot(catalog.version);
                let mut tables: Vec<_> = snapshot.tables.values().collect();
                tables.sort_by_key(|t| t.space);
                let mut rows = Vec::new();
                for s in tables {
                    let t = catalog.all().find(|t| t.space_id == s.space);
                    let Some(t) = t else { continue };
                    for p in s.prefixes.iter().map(Some).chain(s.prefixes.is_empty().then_some(None)) {
                        let index = t
                            .indexes
                            .iter()
                            .find(|i| p.is_some_and(|p| i.id == p.index))
                            .map(|i| i.name.as_str())
                            .unwrap_or("");
                        rows.push(vec![
                            Value::str(&format!("{}.{}", t.db, t.name)),
                            if p.is_some() { Value::str(index) } else { Value::Null },
                            Value::UInt(p.map_or(0, |p| p.parts as u64)),
                            Value::Float(s.rows),
                            Value::UInt(s.sampled as u64),
                            p.map_or(Value::Null, |p| Value::UInt(p.observed_distinct as u64)),
                            p.map_or(Value::Null, |p| Value::Float(p.distinct)),
                            Value::UInt(s.collected.elapsed().as_millis().min(u64::MAX as u128) as u64),
                            Value::UInt(snapshot.generation),
                        ]);
                    }
                }
                rows_out(statistics_columns(), rows)
            }
            Admin::LockTables => {
                self.implicit_commit().await?;
                Ok(Output::ok(0))
            }
            Admin::Use(db) => {
                self.use_db_fresh(&db).await?;
                Ok(Output::ok(0))
            }
            Admin::SelectVars(vars) => {
                let mut columns = Vec::new();
                let mut row = Vec::new();
                for (alias, var) in &vars {
                    let v = admin::system_var(var, &self.session_vars()).ok_or_else(|| {
                        SqlError::new(
                            1193,
                            "HY000",
                            format!("Unknown system variable '{}'", admin::sysvar_name(var)),
                        )
                    })?;
                    let ty = match &v {
                        Value::Int(_) | Value::UInt(_) => ColumnType::LongLong,
                        _ => ColumnType::VarString,
                    };
                    columns.push(Column::new(alias, ty));
                    row.push(v);
                }
                // SELECT @@warning_count reads the previous total, then
                // clears the message list. SHOW WARNINGS preserves it.
                self.clear_warnings();
                rows_out(columns.into(), vec![row])
            }
            Admin::ShowDatabases { like } => {
                let name = match &like {
                    Some(pattern) => format!("Database ({pattern})"),
                    None => "Database".into(),
                };
                let mut rows = Vec::new();
                if like_ci("information_schema", &like) {
                    rows.push(vec![Value::str("information_schema")]);
                }
                for d in &catalog.databases {
                    if like_ci(d, &like) {
                        rows.push(vec![Value::str(d)]);
                    }
                }
                rows_out(cols(&[(&name, ColumnType::VarString)]), rows)
            }
            Admin::ShowTarnIntegrity { db } => {
                let db = self.resolve_db(db)?;
                let rows = catalog
                    .tables_in(&db)
                    .iter()
                    .map(|t| {
                        vec![
                            Value::str(&t.name),
                            Value::str(if t.current_integrity_policy() {
                                "REJECT_UNSUPPORTED_V1"
                            } else {
                                "LEGACY_UNENFORCED"
                            }),
                            Value::UInt(t.foreign_keys.len() as u64),
                            Value::str(if t.current_integrity_policy() {
                                "SUPPORTED_DECLARATIONS_ONLY"
                            } else {
                                "UNKNOWN_CHECK_ENUM_SET"
                            }),
                            Value::str("NOT_CERTIFIED"),
                        ]
                    })
                    .collect();
                rows_out(
                    cols(&[
                        ("Table", ColumnType::VarString),
                        ("Declaration_policy", ColumnType::VarString),
                        ("Legacy_foreign_keys", ColumnType::LongLong),
                        ("Historical_declarations", ColumnType::VarString),
                        ("Data_validation", ColumnType::VarString),
                    ]),
                    rows,
                )
            }
            Admin::ShowTables { db, full, like } => {
                let db = self.resolve_db(db)?;
                let name = format!("Tables_in_{}", db);
                let mut rows = Vec::new();
                for t in catalog.tables_in(&db) {
                    if like_ci(&t.name, &like) {
                        let mut r = vec![Value::str(&t.name)];
                        if full {
                            r.push(Value::str("BASE TABLE"));
                        }
                        rows.push(r);
                    }
                }
                let c = if full {
                    cols(&[(&name, ColumnType::VarString), ("Table_type", ColumnType::VarString)])
                } else {
                    cols(&[(&name, ColumnType::VarString)])
                };
                rows_out(c, rows)
            }
            Admin::ShowColumns { db, table, full } => {
                let db = self.resolve_db(db)?;
                let t = catalog
                    .get(&db, &table)
                    .ok_or_else(|| SqlError::no_table(&db, &table))?;
                let out = describe(&t, full);
                rows_out(out.0, out.1)
            }
            Admin::Describe { db, table } => {
                let db = self.resolve_db(db)?;
                let t = catalog
                    .get(&db, &table)
                    .ok_or_else(|| SqlError::no_table(&db, &table))?;
                let out = describe(&t, false);
                rows_out(out.0, out.1)
            }
            Admin::ShowCreateTable { db, table } => {
                let db = self.resolve_db(db)?;
                let t = catalog
                    .get(&db, &table)
                    .ok_or_else(|| SqlError::no_table(&db, &table))?;
                rows_out(
                    cols(&[
                        ("Table", ColumnType::VarString),
                        ("Create Table", ColumnType::VarString),
                    ]),
                    vec![vec![Value::str(&t.name), Value::str(&show_create(&t))]],
                )
            }
            Admin::ShowIndex { db, table } => {
                let db = self.resolve_db(db)?;
                let t = catalog
                    .get(&db, &table)
                    .ok_or_else(|| SqlError::no_table(&db, &table))?;
                let mut rows = Vec::new();
                for ix in t
                    .indexes
                    .iter()
                    .filter(|ix| ix.cols.iter().all(|&c| !t.columns[c].hidden))
                {
                    for (seq, &c) in ix.cols.iter().enumerate() {
                        rows.push(vec![
                            Value::str(&t.name),
                            Value::Int(!ix.unique as i64),
                            Value::str(&ix.name),
                            Value::Int(seq as i64 + 1),
                            Value::str(&t.columns[c].name),
                            Value::str("A"),
                            Value::Null,
                            Value::Null,
                            Value::Null,
                            Value::str(if t.columns[c].nullable { "YES" } else { "" }),
                            Value::str("TREE"),
                            Value::str(""),
                            Value::str(&ix.comment),
                            Value::str(if ix.invisible { "NO" } else { "YES" }),
                            Value::Null,
                        ]);
                    }
                }
                let names = [
                    "Table",
                    "Non_unique",
                    "Key_name",
                    "Seq_in_index",
                    "Column_name",
                    "Collation",
                    "Cardinality",
                    "Sub_part",
                    "Packed",
                    "Null",
                    "Index_type",
                    "Comment",
                    "Index_comment",
                    "Visible",
                    "Expression",
                ];
                let c: Vec<(&str, ColumnType)> = names
                    .iter()
                    .map(|n| {
                        let ty = if matches!(*n, "Non_unique" | "Seq_in_index") {
                            ColumnType::LongLong
                        } else {
                            ColumnType::VarString
                        };
                        (*n, ty)
                    })
                    .collect();
                rows_out(cols(&c), rows)
            }
            Admin::ShowVariables { global, like } => {
                let mut rows = Vec::new();
                for name in admin::SHOWN_VARIABLES {
                    if like_ci(name, &like) && !(global && name.starts_with("tarn_")) {
                        let scoped = if global {
                            format!("global.{name}")
                        } else {
                            name.to_string()
                        };
                        let v = admin::system_var(&scoped, &self.session_vars()).unwrap_or(Value::Null);
                        rows.push(vec![Value::str(name), Value::Str(v.to_text().unwrap_or_default())]);
                    }
                }
                rows_out(
                    cols(&[
                        ("Variable_name", ColumnType::VarString),
                        ("Value", ColumnType::VarString),
                    ]),
                    rows,
                )
            }
            Admin::ShowStatus { like } => Ok(self.show_status(&like)),
            Admin::ShowTarnStatus => Ok(self.show_status(&None)),
            Admin::ShowTarnQueries => {
                let mut columns = cols(&[
                    ("Digest", ColumnType::VarString),
                    ("Database", ColumnType::VarString),
                    ("Query", ColumnType::VarString),
                ])
                .to_vec();
                for name in [
                    "Calls",
                    "Errors",
                    "No_data",
                    "Local",
                    "Distributed",
                    "Max_shards",
                    "Shard_targets",
                    "Known_bucket_targets",
                    "Unknown_bucket_calls",
                    "Read_attempts",
                    "Grouped_reads",
                    "Local_commits",
                    "Distributed_commits",
                    "Commit_errors",
                    "Total_us",
                    "Storage_rows",
                ] {
                    columns.push(Column::new(name, ColumnType::LongLong));
                }
                rows_out(columns.into(), self.cluster.stats.query_digests.rows())
            }
            Admin::ShowTarnPlan(name) => {
                let template = self
                    .sql_prepared
                    .get(&name)
                    .map(|(text, _)| text.clone())
                    .or_else(|| {
                        name.parse::<u32>()
                            .ok()
                            .and_then(|id| self.prepared.get(&id).map(|p| p.template.clone()))
                    })
                    .ok_or_else(|| {
                        SqlError::new(1243, "HY000", format!("Unknown prepared statement handler ({name})"))
                    })?;
                let normalized = normalize(&template, Mode::Placeholders)?;
                let stmt = self.plan_cached(&normalized.template, &[])?;
                self.explain(&stmt, self.cluster.catalog().version, &[])
            }
            Admin::ShowTarnRpcs => {
                let mut columns = cols(&[
                    ("Scope", ColumnType::VarString),
                    ("Shard", ColumnType::LongLong),
                    ("Connection", ColumnType::LongLong),
                    ("Closed", ColumnType::LongLong),
                ])
                .to_vec();
                for name in [
                    "Pending",
                    "Charged_bytes",
                    "Wire_bytes",
                    "Normal_pending",
                    "Abandoned",
                    "Oldest_ms",
                    "Refusals",
                    "Deadline_retirements",
                    "Peak_pending",
                    "Peak_bytes",
                    "Observation_drops",
                ] {
                    columns.push(Column::new(name, ColumnType::LongLong));
                }
                rows_out(columns.into(), crate::rpcs::rows(&self.cluster))
            }
            Admin::ShowTarnBuckets => {
                let rows = self
                    .cluster
                    .show_buckets()
                    .await?
                    .into_iter()
                    .map(|(id, n, ranges, epoch, moving)| {
                        vec![
                            Value::Int(id as i64),
                            Value::Int(n as i64),
                            Value::str(&ranges),
                            Value::Int(epoch as i64),
                            Value::str(&moving),
                        ]
                    })
                    .collect();
                rows_out(
                    cols(&[
                        ("Shard", ColumnType::Long),
                        ("Buckets", ColumnType::Long),
                        ("Ranges", ColumnType::VarString),
                        ("Epoch", ColumnType::LongLong),
                        ("Moving", ColumnType::VarString),
                    ]),
                    rows,
                )
            }
            Admin::AddShard { id, replicas } => {
                self.implicit_commit().await?;
                self.cluster.add_shard(id, replicas).await?;
                Ok(Output::Ok {
                    affected: 0,
                    last_insert_id: 0,
                    info: format!("Shard {} joined the cluster (with no buckets)", id),
                })
            }
            Admin::RemoveShard { id } => {
                self.implicit_commit().await?;
                let moved = self.cluster.remove_shard(id).await?;
                Ok(Output::Ok {
                    affected: moved,
                    last_insert_id: 0,
                    info: format!("Shard {} left the cluster ({} buckets moved off it)", id, moved),
                })
            }
            Admin::Rebalance {
                by,
                max_moves,
                max_rate,
            } => {
                self.implicit_commit().await?;
                let info = self.cluster.rebalance_start(&by, max_moves, max_rate).await?;
                Ok(Output::Ok {
                    affected: 0,
                    last_insert_id: 0,
                    info,
                })
            }
            Admin::RebalanceControl(action) => {
                self.implicit_commit().await?;
                let info = self.cluster.rebalance_control(&action).await?;
                Ok(Output::Ok {
                    affected: 0,
                    last_insert_id: 0,
                    info,
                })
            }
            Admin::ShowTarnRebalance => {
                let r = self.cluster.rebalance_status().await?;
                let rows = r
                    .into_iter()
                    .map(|r| {
                        let count = |st: &[&str]| r.moves.iter().filter(|x| st.contains(&x.state.as_str())).count();
                        vec![
                            Value::str(&r.status),
                            Value::str(&r.by),
                            Value::str(&r.owner),
                            Value::Float(r.started),
                            Value::UInt(r.done),
                            Value::UInt(r.failed),
                            Value::UInt(count(&["planned"]) as u64),
                            Value::UInt(count(&["moving", "copying", "fenced"]) as u64),
                            Value::UInt(r.bytes),
                            Value::UInt(r.max_moves as u64),
                            Value::UInt(r.max_rate),
                            Value::str(&r.message),
                        ]
                    })
                    .collect();
                rows_out(
                    cols(&[
                        ("Status", ColumnType::VarString),
                        ("By", ColumnType::VarString),
                        ("Owner", ColumnType::VarString),
                        ("Started", ColumnType::Double),
                        ("Moves_done", ColumnType::LongLong),
                        ("Moves_failed", ColumnType::LongLong),
                        ("Planned", ColumnType::LongLong),
                        ("Moving", ColumnType::LongLong),
                        ("Bytes_moved", ColumnType::LongLong),
                        ("Max_moves", ColumnType::LongLong),
                        ("Max_rate", ColumnType::LongLong),
                        ("Message", ColumnType::VarString),
                    ]),
                    rows,
                )
            }
            Admin::ShowTarnMoves => {
                let moves = self
                    .cluster
                    .rebalance_status()
                    .await?
                    .map(|r| r.moves)
                    .unwrap_or_default();
                rows_out(
                    cols(&[
                        ("Bucket", ColumnType::LongLong),
                        ("From_shard", ColumnType::LongLong),
                        ("To_shard", ColumnType::LongLong),
                        ("Bytes", ColumnType::LongLong),
                        ("State", ColumnType::VarString),
                        ("Error", ColumnType::VarString),
                    ]),
                    moves
                        .into_iter()
                        .map(|x| {
                            vec![
                                Value::UInt(x.bucket as u64),
                                Value::UInt(x.from as u64),
                                Value::UInt(x.to as u64),
                                Value::UInt(x.bytes),
                                Value::str(&x.state),
                                Value::str(&x.error),
                            ]
                        })
                        .collect(),
                )
            }
            Admin::ShowTarnShards => {
                let m = self.cluster.show_members().await?;
                rows_out(
                    cols(&[
                        ("Shard", ColumnType::Long),
                        ("Replicas", ColumnType::VarString),
                        ("State", ColumnType::VarString),
                    ]),
                    m.shards
                        .iter()
                        .map(|s| {
                            vec![
                                Value::Int(s.id as i64),
                                Value::str(&s.replicas.join(",")),
                                Value::str(&s.state),
                            ]
                        })
                        .collect(),
                )
            }
            Admin::MoveBuckets { first, last, to } => {
                self.implicit_commit().await?;
                // Buckets move to members only: every server knows them
                // (TARN ADD SHARD), or, before any joined, they are the
                // shards of the configuration.
                let is_member = |c: &Cluster| c.members().shards.iter().any(|s| s.id == to && s.state == "active");
                if !is_member(&self.cluster) {
                    // (one that joined since this server last looked)
                    self.cluster.load_members().await?;
                }
                let member = is_member(&self.cluster);
                let to_pos = self
                    .cluster
                    .shard_position(to)
                    .filter(|&p| member && !self.cluster.shards[p].removed())
                    .ok_or_else(|| {
                        SqlError::new(
                            1210,
                            "HY000",
                            format!("there is no shard {} in the cluster (TARN ADD SHARD adds one)", to),
                        )
                    })?;
                let count = self.cluster.bucket_map().count;
                if last > count {
                    return Err(SqlError::new(
                        1210,
                        "HY000",
                        format!("bucket {} does not exist (there are {})", last, count),
                    ));
                }
                let mut moved = 0u64;
                for b in first..=last {
                    if self
                        .cluster
                        .move_bucket(b, to_pos, self.cluster.cfg.move_timeout)
                        .await?
                    {
                        moved += 1;
                    }
                }
                let total = (last - first + 1) as u64;
                Ok(Output::Ok {
                    affected: moved,
                    last_insert_id: 0,
                    info: format!("Buckets: {} moved, {} there already", moved, total - moved),
                })
            }
            Admin::ShowWarnings { errors } => rows_out(
                cols(&[
                    ("Level", ColumnType::VarString),
                    ("Code", ColumnType::Long),
                    ("Message", ColumnType::VarString),
                ]),
                self.warnings
                    .iter()
                    .filter(|w| !errors || w.level == "Error")
                    .map(|w| vec![Value::str(w.level), Value::Int(w.code as i64), Value::str(&w.message)])
                    .collect(),
            ),
            Admin::ShowEngines => rows_out(
                cols(&[
                    ("Engine", ColumnType::VarString),
                    ("Support", ColumnType::VarString),
                    ("Comment", ColumnType::VarString),
                    ("Transactions", ColumnType::VarString),
                    ("XA", ColumnType::VarString),
                    ("Savepoints", ColumnType::VarString),
                ]),
                [
                    (Engine::Memtx, "Tarantool in-memory engine, sharded"),
                    (Engine::Vinyl, "Tarantool on-disk LSM engine, sharded"),
                ]
                .iter()
                .map(|&(e, comment)| {
                    vec![
                        Value::str(e.name()),
                        Value::str(if e == self.cluster.cfg.default_engine {
                            "DEFAULT"
                        } else {
                            "YES"
                        }),
                        Value::str(comment),
                        Value::str("YES"),
                        Value::str("NO"),
                        Value::str("YES"),
                    ]
                })
                .collect(),
            ),
            Admin::ShowCollation => rows_out(
                cols(&[
                    ("Collation", ColumnType::VarString),
                    ("Charset", ColumnType::VarString),
                    ("Id", ColumnType::LongLong),
                    ("Default", ColumnType::VarString),
                    ("Compiled", ColumnType::VarString),
                    ("Sortlen", ColumnType::LongLong),
                ]),
                [
                    Collation::General { pad: true },
                    Collation::Utf8Bin,
                    Collation::Unicode { pad: true },
                    Collation::General { pad: false },
                    Collation::Unicode { pad: false },
                    Collation::Binary,
                ]
                .iter()
                .map(|c| {
                    vec![
                        Value::str(c.name()),
                        Value::str(if *c == Collation::Binary { "binary" } else { "utf8mb4" }),
                        Value::Int(c.id() as i64),
                        Value::str(if matches!(c, Collation::General { pad: true } | Collation::Binary) {
                            "Yes"
                        } else {
                            ""
                        }),
                        Value::str("Yes"),
                        Value::Int(if matches!(c, Collation::Unicode { .. }) { 8 } else { 1 }),
                    ]
                })
                .collect(),
            ),
            Admin::ShowCharset => rows_out(
                cols(&[
                    ("Charset", ColumnType::VarString),
                    ("Description", ColumnType::VarString),
                    ("Default collation", ColumnType::VarString),
                    ("Maxlen", ColumnType::LongLong),
                ]),
                vec![vec![
                    Value::str("utf8mb4"),
                    Value::str("UTF-8 Unicode"),
                    Value::str("utf8mb4_general_ci"),
                    Value::Int(4),
                ]],
            ),
            Admin::ShowProcesslist => rows_out(
                cols(&[
                    ("Id", ColumnType::LongLong),
                    ("User", ColumnType::VarString),
                    ("Host", ColumnType::VarString),
                    ("db", ColumnType::VarString),
                    ("Command", ColumnType::VarString),
                    ("Time", ColumnType::LongLong),
                    ("State", ColumnType::VarString),
                    ("Info", ColumnType::VarString),
                ]),
                vec![vec![
                    Value::UInt(self.conn_id as u64),
                    Value::str(&self.user),
                    Value::str("client"),
                    self.db.as_deref().map(Value::str).unwrap_or(Value::Null),
                    Value::str("Query"),
                    Value::Int(0),
                    Value::str(""),
                    Value::str("SHOW PROCESSLIST"),
                ]],
            ),
            Admin::ShowTableStatus { db, like } => {
                let db = self.resolve_db(db)?;
                let mut rows = Vec::new();
                for t in catalog.tables_in(&db) {
                    if like_ci(&t.name, &like) {
                        rows.push(vec![
                            Value::str(&t.name),
                            Value::str(t.engine.name()),
                            Value::Int(10),
                            Value::str("Fixed"),
                            Value::Null,
                            Value::Null,
                        ]);
                    }
                }
                rows_out(
                    cols(&[
                        ("Name", ColumnType::VarString),
                        ("Engine", ColumnType::VarString),
                        ("Version", ColumnType::LongLong),
                        ("Row_format", ColumnType::VarString),
                        ("Rows", ColumnType::LongLong),
                        ("Comment", ColumnType::VarString),
                    ]),
                    rows,
                )
            }
        }
    }

    fn show_status(&self, like: &Option<String>) -> Output {
        let s = &self.cluster.stats;
        let mut stats = vec![
            ("Tarn_queries", s.queries.load(Ordering::Relaxed)),
            ("Tarn_commits_local", s.commits_local.load(Ordering::Relaxed)),
            ("Tarn_commits_2pc", s.commits_2pc.load(Ordering::Relaxed)),
            ("Tarn_commits_read_only", s.commits_ro.load(Ordering::Relaxed)),
            ("Tarn_conflicts", s.conflicts.load(Ordering::Relaxed)),
            ("Tarn_retries", s.retries.load(Ordering::Relaxed)),
            ("Tarn_plan_cache_hits", s.plan_cache_hits.load(Ordering::Relaxed)),
            ("Tarn_plan_cache_misses", s.plan_cache_misses.load(Ordering::Relaxed)),
            ("Tarn_cross_shard_deadlocks", s.deadlocks.load(Ordering::Relaxed)),
            ("Tarn_deadlock_detector_calls", s.deadlock_calls.load(Ordering::Relaxed)),
            ("Tarn_storage_counts", s.storage_counts.load(Ordering::Relaxed)),
            ("Tarn_rows_read", s.rows_read.load(Ordering::Relaxed)),
            ("Tarn_storage_reads", s.storage_reads.load(Ordering::Relaxed)),
            ("Tarn_path_estimates", s.path_estimates.load(Ordering::Relaxed)),
            ("Tarn_bucket_moves", s.bucket_moves.load(Ordering::Relaxed)),
            ("Tarn_bucket_map_reads", self.cluster.bucket_loads()),
            ("Tarn_buckets", self.cluster.bucket_map().count as u64),
            ("Tarn_shards", self.cluster.nlive() as u64),
            ("Tarn_lazy_reads", s.lazy_reads.load(Ordering::Relaxed)),
            ("Tarn_lazy_fallbacks", s.lazy_fallbacks.load(Ordering::Relaxed)),
            (
                "Tarn_lazy_session_pending_apply",
                self.lazy_history.pending_count() as u64,
            ),
            (
                "Tarn_lazy_session_touched_buckets",
                self.lazy_history.bucket_count() as u64,
            ),
            ("Tarn_lazy_session_strong_fallback", self.lazy_history.strong as u64),
        ];
        let resources = self.cluster.resources.stats();
        stats.extend([
            ("Tarn_resource_bytes", resources[0]),
            ("Tarn_resource_peak_bytes", resources[1]),
            ("Tarn_resource_work", resources[2]),
            ("Tarn_resource_peak_work", resources[3]),
            ("Tarn_resource_queries", resources[4]),
            ("Tarn_resource_refusals", resources[5]),
            ("Tarn_resource_read_deadlines", resources[6]),
            ("Tarn_topk_scans", self.cluster.stats.topk_scans.load(Ordering::Relaxed)),
        ]);
        stats.extend(s.query_digests.counters());
        stats.extend(crate::rpcs::counters(&self.cluster));
        stats.retain(|(n, _)| like_ci(n, like));
        let mut rows: Vec<Row> = stats
            .into_iter()
            .map(|(n, v)| vec![Value::str(n), Value::str(&v.to_string())])
            .collect();
        rows.extend(
            crate::lazy::stats(&self.cluster)
                .into_iter()
                .filter(|(n, _)| like_ci(n, like))
                .map(|(n, v)| vec![Value::str(&n), Value::str(&v)]),
        );
        rows.extend(
            crate::vinyl::stats(&self.cluster)
                .into_iter()
                .filter(|(n, _)| like_ci(n, like))
                .map(|(n, v)| vec![Value::str(&n), Value::str(&v)]),
        );
        Output::Rows {
            resource_guard: None,
            columns: cols(&[
                ("Variable_name", ColumnType::VarString),
                ("Value", ColumnType::VarString),
            ]),
            rows,
        }
    }

    fn resolve_db(&self, db: Option<String>) -> SqlResult<String> {
        db.or_else(|| self.db.clone())
            .ok_or_else(|| SqlError::new(1046, "3D000", "No database selected"))
    }
}

fn split(e: &Expr, out: &mut Vec<Expr>) {
    match e {
        Expr::And(a, b) => {
            split(a, out);
            split(b, out);
        }
        other => out.push(other.clone()),
    }
}

/// What an inserted row collides with in storage: the row with its primary
/// key, and per unique secondary index the (index position, row) holding
/// its value.
struct StoredConflicts {
    pk: Option<Row>,
    uniq: Vec<Option<(usize, Row)>>,
}

/// The rows a new row collides with now, taking the transaction's own
/// writes into account: (row with its primary key, [(index position, row)]
/// for unique keys whose value another row holds).
fn current_conflicts(
    txn: &Txn,
    t: &TableDef,
    row: &Row,
    pk: &[Value],
    st: StoredConflicts,
) -> (Option<Row>, Vec<(usize, Row)>) {
    let pk_row = match txn.buffered(t.space_id, pk) {
        Some(b) => b.cloned(),
        None => st.pk,
    };
    let mut uniq: Vec<(usize, Row)> = Vec::new();
    let pk_of = |r: &Row| -> Vec<Value> { t.pk_key(r) };
    for (slot, stored) in st.uniq.into_iter().enumerate() {
        let ix = (1..t.indexes.len()).filter(|&i| t.indexes[i].unique).nth(slot).unwrap();
        if t.indexes[ix].cols.iter().any(|&c| row[c].is_null()) {
            continue;
        }
        let same = |r: &Row| t.same_key(&t.indexes[ix], r, row);
        // A live row this transaction wrote with that value comes first.
        let mut found = txn
            .writes
            .range((t.space_id, bytes::Bytes::new())..)
            .take_while(|((sp, _), _)| *sp == t.space_id)
            .filter_map(|(_, w)| w.row.as_ref())
            .find(|r| same(r))
            .cloned();
        if found.is_none() {
            if let Some((_, r)) = stored {
                // A stored holder counts unless this transaction rewrote it.
                found = match txn.buffered(t.space_id, &pk_of(&r)) {
                    None => Some(r),
                    Some(Some(b)) if same(b) => Some(b.clone()),
                    _ => None,
                };
            }
        }
        if let Some(r) = found {
            if pk_of(&r) != pk && !uniq.iter().any(|(_, u)| pk_of(u) == pk_of(&r)) {
                uniq.push((ix, r));
            }
        }
    }
    (pk_row, uniq)
}

pub(crate) async fn backoff(attempt: usize) {
    let base = 200u64 << attempt.min(6);
    let jitter = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0) as u64)
        % base.max(1);
    tokio::time::sleep(std::time::Duration::from_micros(base + jitter)).await;
}

/// SHOW [FULL] COLUMNS: (columns, rows).
fn describe(t: &TableDef, full: bool) -> (Arc<[Column]>, Vec<Vec<Value>>) {
    let mut rows = Vec::new();
    for (i, c) in t.visible_columns() {
        let key = if t.is_pk_col(i) {
            "PRI"
        } else if t.indexes[1..].iter().any(|ix| ix.cols.first() == Some(&i) && ix.unique) {
            "UNI"
        } else if t.indexes[1..].iter().any(|ix| ix.cols.first() == Some(&i)) {
            "MUL"
        } else {
            ""
        };
        let mut r = vec![Value::str(&c.name), Value::str(&c.ty().display())];
        if full {
            r.push(if c.ty().is_text() {
                Value::str(c.collation_name())
            } else {
                Value::Null
            });
        }
        r.extend([
            Value::str(if c.nullable { "YES" } else { "NO" }),
            Value::str(key),
            c.default.as_deref().map(Value::str).unwrap_or(Value::Null),
            Value::str(if c.auto_increment { "auto_increment" } else { "" }),
        ]);
        if full {
            r.push(Value::str("select,insert,update,references"));
            r.push(Value::str(""));
        }
        rows.push(r);
    }
    let mut names = vec![("Field", ColumnType::VarString), ("Type", ColumnType::VarString)];
    if full {
        names.push(("Collation", ColumnType::VarString));
    }
    names.extend([
        ("Null", ColumnType::VarString),
        ("Key", ColumnType::VarString),
        ("Default", ColumnType::VarString),
        ("Extra", ColumnType::VarString),
    ]);
    if full {
        names.push(("Privileges", ColumnType::VarString));
        names.push(("Comment", ColumnType::VarString));
    }
    (cols(&names), rows)
}

/// Character set of a collation name ("utf8mb4_bin" -> "utf8mb4").
fn charset_of(collation: &str) -> &str {
    match collation {
        "binary" => "binary",
        c => match c.split('_').next() {
            Some("utf8") => "utf8mb3",
            Some("uca1400") | None => "utf8mb4",
            Some(cs) => cs,
        },
    }
}

/// `s` as the body of a quoted SQL string, as SHOW CREATE TABLE writes it:
/// NUL, LF and CR escaped, `\` and `'` doubled; other characters stay raw.
fn sql_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\0' => out.push_str("\\0"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\\' => out.push_str("\\\\"),
            '\'' => out.push_str("''"),
            c => out.push(c),
        }
    }
    out
}

fn show_create(t: &TableDef) -> String {
    let mut s = format!("CREATE TABLE `{}` (\n", t.name);
    let mut lines = Vec::new();
    let table_coll = t.collation_name();
    for (_, c) in t.visible_columns() {
        let mut l = format!("  `{}` {}", c.name, c.ty().display());
        if c.ty().is_text() && !c.collation_name().eq_ignore_ascii_case(table_coll) {
            if charset_of(c.collation_name()) != charset_of(table_coll) {
                l.push_str(&format!(" CHARACTER SET {}", charset_of(c.collation_name())));
            }
            l.push_str(&format!(" COLLATE {}", c.collation_name()));
        }
        if !c.nullable {
            l.push_str(" NOT NULL");
        }
        if let Some(d) = &c.default {
            if d == "CURRENT_TIMESTAMP" {
                l.push_str(" DEFAULT CURRENT_TIMESTAMP");
            } else {
                l.push_str(&format!(" DEFAULT '{}'", sql_str(d)));
            }
        } else if c.nullable {
            l.push_str(" DEFAULT NULL");
        }
        if c.auto_increment {
            l.push_str(" AUTO_INCREMENT");
        }
        lines.push(l);
    }
    for ix in &t.indexes {
        if ix.cols.iter().any(|&c| t.columns[c].hidden) {
            continue;
        }
        let colnames: Vec<String> = ix.cols.iter().map(|&c| format!("`{}`", t.columns[c].name)).collect();
        let comment = if ix.comment.is_empty() {
            String::new()
        } else {
            format!(" COMMENT '{}'", sql_str(&ix.comment))
        };
        if ix.id == 0 && t.declared_pk() {
            lines.push(format!("  PRIMARY KEY ({}){}", colnames.join(","), comment));
        } else {
            lines.push(format!(
                "  {}KEY `{}` ({}){}{}",
                if ix.unique { "UNIQUE " } else { "" },
                ix.name,
                colnames.join(","),
                comment,
                if ix.invisible { " /*!80000 INVISIBLE */" } else { "" }
            ));
        }
    }
    let quoted = |v: &[String]| v.iter().map(|c| format!("`{}`", c)).collect::<Vec<_>>().join(", ");
    for fk in &t.foreign_keys {
        let mut l = format!(
            "  CONSTRAINT `{}` FOREIGN KEY ({}) REFERENCES `{}` ({})",
            fk.name,
            quoted(&fk.cols),
            if fk.ref_db.eq_ignore_ascii_case(&t.db) {
                fk.ref_table.clone()
            } else {
                format!("{}`.`{}", fk.ref_db, fk.ref_table)
            },
            quoted(&fk.ref_cols)
        );
        if let Some(a) = &fk.on_delete {
            l.push_str(&format!(" ON DELETE {}", a));
        }
        if let Some(a) = &fk.on_update {
            l.push_str(&format!(" ON UPDATE {}", a));
        }
        lines.push(l);
    }
    s.push_str(&lines.join(",\n"));
    let sk: Vec<String> = t
        .shard_key
        .iter()
        .map(|&c| format!("`{}`", t.columns[c].name))
        .collect();
    s.push_str(&format!(
        "\n) ENGINE={} DEFAULT CHARSET={} COLLATE={}",
        t.engine.name(),
        charset_of(table_coll),
        table_coll
    ));
    if !t.comment.is_empty() {
        s.push_str(&format!(" COMMENT='{}'", sql_str(&t.comment)));
    }
    if !t.current_integrity_policy() {
        s.push_str(crate::integrity::LEGACY_NOTICE);
    }
    if t.shard_key.iter().all(|&c| !t.columns[c].hidden) {
        s.push_str(&format!("\n/*!50100 PARTITION BY KEY ({}) */", sk.join(",")));
    }
    s
}

/// Unused helper kept for API symmetry with Rd-based decoders.
#[allow(dead_code)]
fn first_value(d: &[u8]) -> SqlResult<Value> {
    let mut rd = Rd::new(d);
    rd.array()?;
    rd.value()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duplicate_diagnostics_admit_before_formatting_and_keep_retained_ownership() {
        let pool = crate::budget::Pool::new(crate::budget::Limits {
            query_bytes: 1000,
            server_bytes: 2000,
            ..Default::default()
        })
        .unwrap();
        let mut session = pool.account(1000);
        session.charge(600).unwrap();
        let query = pool.query();
        let mut warnings = DmlWarnings::new(&pool, 400);
        crate::budget::scope_sync(query.clone(), || {
            warnings.duplicate(300, || SqlError::dup_key("x", "PRIMARY")).unwrap();
            assert!(crate::budget::is_exhausted(
                &warnings
                    .duplicate(101, || {
                        panic!("refused diagnostics must not format or reach commit")
                    })
                    .unwrap_err()
            ));
        });
        assert_eq!((warnings.total, warnings.messages.len()), (1, 1));
        // Completing a query releases temporary bytes, while its retained
        // diagnostic owner survives. Publication performs no admission.
        drop(query);
        assert_eq!(pool.stats()[0], 900);
        session.absorb(warnings.retained);
        assert_eq!((session.used(), pool.stats()[0]), (900, 900));
        assert!(session.charge(101).is_err());
        session.resize(600).unwrap();
        assert_eq!(pool.stats()[0], 600);
        drop(session);
        assert_eq!(pool.stats()[0], 0);
    }

    #[test]
    fn autocommit_ack_requires_exact_count_and_terminal_envelope() {
        let mut ok = Mp::new();
        ok.arr(1).arr(2).str("ok").uint(2);
        assert_eq!(autocommit_count(&ok.0).unwrap(), 2);
        for value in [Value::Null, Value::Float(2.0), Value::str("2"), Value::Int(-1)] {
            let mut bad = Mp::new();
            bad.arr(1).arr(2).str("ok").val(&value);
            assert!(autocommit_count(&bad.0).is_err());
        }
        let mut row = Mp::new();
        row.arr(1).arr(3).str("ok").uint(1).arr(2).uint(1).str("雪");
        assert_eq!(autocommit_count(&row.0).unwrap(), 1);
        let mut none = Mp::new();
        none.arr(1).arr(3).str("ok").uint(0).nil();
        assert_eq!(autocommit_count(&none.0).unwrap(), 0);
        for fields in [1, 3, 4] {
            let mut bad = Mp::new();
            bad.arr(1).arr(fields).str("ok");
            if fields >= 3 {
                bad.uint(2).str("unexpected metadata");
            }
            assert!(autocommit_count(&bad.0).is_err());
        }
        ok.nil();
        assert!(autocommit_count(&ok.0).is_err());
        let ambiguous = autocommit_ack(&ok.0, 7).unwrap_err();
        assert_eq!(ambiguous.code, 1180);
        assert_eq!(&ambiguous.sqlstate, b"40003");
        assert!(!ambiguous.moved);
    }

    #[test]
    fn known_write_failures_preserve_cumulative_lazy_history() {
        for code in [1041, 1048, 1062, 1146, 1205, 1213, 1264, 1265, 1406, 1412, 1690] {
            let mut history = crate::lazy::History::default();
            history.record(crate::txn::CommitOutcome {
                lazy_buckets: std::collections::BTreeSet::from([11, 29]),
                pending_apply: Some(crate::txn::PendingApply {
                    txid: "prior-commit".into(),
                    participants: vec![2],
                }),
            });
            note_lazy_write_error(&mut history, &SqlError::new(code, "HY000", "known rollback"));
            assert!(!history.strong, "{code}");
            assert_eq!(history.bucket_count(), 2);
            assert_eq!(history.pending_count(), 1);
        }
        for code in [1105, 1180, 2003, 2013] {
            let mut history = crate::lazy::History::default();
            history.touch([11]);
            note_lazy_write_error(&mut history, &SqlError::new(code, "HY000", "uncertain"));
            assert!(history.strong, "{code}");
        }
    }

    #[test]
    fn sql_string_format_matches_mariadb_bytes() {
        for (value, encoded) in [
            ("\0", r"\0"),
            ("\n", r"\n"),
            ("\r", r"\r"),
            ("\t", "\t"),
            ("\u{8}", "\u{8}"),
            ("\u{1a}", "\u{1a}"),
            ("\\", r"\\"),
            ("'", "''"),
            ("\"", "\""),
            (r"\0\n\r", r"\\0\\n\\r"),
            ("雪\0\n\r\t\u{8}\u{1a}\\'\"", "雪\\0\\n\\r\t\u{8}\u{1a}\\\\''\""),
        ] {
            assert_eq!(sql_str(value).as_bytes(), encoded.as_bytes(), "{value:?}");
        }
    }

    #[test]
    fn show_create_control_strings_round_trip() {
        let cat = crate::catalog::Catalog::new(1, vec!["db".into()], vec![]);
        let ctx = PlanCtx {
            statistics: None,
            catalog: &cat,
            db: Some("db"),
            params: &[],
            conn_coll: "utf8mb4_bin",
            default_coll: "utf8mb4_bin",
        };
        let parse = |sql: &str| {
            let Stmt::CreateTable { def, .. } = crate::planner::plan(sql, &ctx).unwrap() else {
                panic!("not CREATE TABLE");
            };
            def
        };
        let mut def = parse("CREATE TABLE esc (id INT PRIMARY KEY, v VARCHAR(32), KEY kv (v))");
        let value = "雪\0\n\r\t\u{8}\u{1a}\\'\"";
        let encoded = "雪\\0\\n\\r\t\u{8}\u{1a}\\\\''\"";
        def.columns[1].default = Some(value.into());
        def.comment = value.into();
        for ix in &mut def.indexes {
            ix.comment = value.into();
        }
        let ddl = show_create(&def);
        for fragment in [
            format!("DEFAULT '{encoded}'"),
            format!("PRIMARY KEY (`id`) COMMENT '{encoded}'"),
            format!("KEY `kv` (`v`) COMMENT '{encoded}'"),
            format!("COMMENT='{encoded}'"),
        ] {
            assert!(ddl.contains(&fragment), "{ddl:?} lacks {fragment:?}");
        }
        let reloaded = parse(&ddl);
        assert_eq!(reloaded.columns[1].default.as_deref(), Some(value));
        assert_eq!(reloaded.comment.as_bytes(), value.as_bytes());
        assert!(reloaded
            .indexes
            .iter()
            .all(|ix| ix.comment.as_bytes() == value.as_bytes()));
        assert_eq!(show_create(&reloaded).as_bytes(), ddl.as_bytes());
    }

    #[test]
    fn schema_output_exposes_legacy_integrity_without_certifying_rows() {
        let cat = crate::catalog::Catalog::new(1, vec!["db".into()], vec![]);
        let ctx = PlanCtx {
            statistics: None,
            catalog: &cat,
            db: Some("db"),
            params: &[],
            conn_coll: "utf8mb4_bin",
            default_coll: "utf8mb4_bin",
        };
        let Stmt::CreateTable { mut def, .. } = planner::plan("CREATE TABLE t(id INT PRIMARY KEY)", &ctx).unwrap()
        else {
            panic!()
        };
        assert!(!show_create(&def).contains("LEGACY_UNENFORCED"));
        def.integrity_policy = 0;
        assert!(show_create(&def).contains("LEGACY_UNENFORCED"));
        // Unknown future provenance cannot become a current certificate.
        def.integrity_policy = 2;
        assert!(show_create(&def).contains("LEGACY_UNENFORCED"));
    }
}
