//! Session-level statements (transaction control, SET, SHOW, USE, @@vars)
//! recognized directly from the SQL text. Clients and drivers send a lot of
//! these at connect time; handling them here keeps them off the planner.

use tarn_common::{SqlError, SqlResult, Value};

#[derive(Clone, Debug, PartialEq)]
pub enum Admin {
    Begin { read_only: bool },
    Commit,
    Rollback,
    SetAutocommit(bool),
    /// SET ... statements with no effect on Tarn (names, isolation, sql_mode...).
    SetIgnored,
    SetVar { name: String, value: Value },
    Use(String),
    ShowDatabases { like: Option<String> },
    ShowTables { db: Option<String>, full: bool, like: Option<String> },
    ShowColumns { db: Option<String>, table: String, full: bool },
    ShowCreateTable { db: Option<String>, table: String },
    ShowIndex { db: Option<String>, table: String },
    ShowVariables { like: Option<String> },
    ShowStatus { like: Option<String> },
    ShowWarnings,
    ShowEngines,
    ShowCollation,
    ShowCharset,
    ShowProcesslist,
    ShowTableStatus { db: Option<String>, like: Option<String> },
    ShowTarnStatus,
    /// SELECT @@a [AS x], @@b ... [LIMIT n]
    SelectVars(Vec<(String, String)>),
    Describe { db: Option<String>, table: String },
    Noop,
}

pub const SERVER_VERSION_COMMENT: &str = "Tarn: distributed MySQL on Tarantool";

/// Variables reported by SELECT @@x / SHOW VARIABLES.
pub fn system_var(name: &str, autocommit: bool, db: Option<&str>) -> Option<Value> {
    let n = name.trim_start_matches('@').to_ascii_lowercase();
    let n = n
        .strip_prefix("session.")
        .or_else(|| n.strip_prefix("global."))
        .or_else(|| n.strip_prefix("local."))
        .unwrap_or(&n)
        .to_string();
    Some(match n.as_str() {
        "version" => Value::str(crate::SERVER_VERSION),
        "version_comment" => Value::str(SERVER_VERSION_COMMENT),
        "version_compile_os" => Value::str("Linux"),
        "version_compile_machine" => Value::str("x86_64"),
        "autocommit" => Value::Int(autocommit as i64),
        "tx_isolation" | "transaction_isolation" => Value::str("SERIALIZABLE"),
        "tx_read_only" | "transaction_read_only" | "read_only" | "super_read_only" => Value::Int(0),
        "max_allowed_packet" => Value::Int(67_108_864),
        "net_buffer_length" => Value::Int(16384),
        "character_set_client" | "character_set_connection" | "character_set_results" | "character_set_server"
        | "character_set_database" | "character_set_system" => Value::str("utf8mb4"),
        "collation_connection" | "collation_server" | "collation_database" => Value::str("utf8mb4_bin"),
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
        "license" => Value::str("Apache-2.0"),
        "performance_schema" => Value::Int(0),
        "have_ssl" | "have_openssl" => Value::str("DISABLED"),
        "max_connections" => Value::Int(100_000),
        "innodb_lock_wait_timeout" | "lock_wait_timeout" => Value::Int(1),
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
        "warning_count" | "error_count" => Value::Int(0),
        "default_authentication_plugin" => Value::str("mysql_native_password"),
        _ => return None,
    })
}

pub const SHOWN_VARIABLES: &[&str] = &[
    "autocommit", "character_set_client", "character_set_connection", "character_set_results", "character_set_server",
    "collation_connection", "collation_server", "default_storage_engine", "div_precision_increment", "init_connect",
    "innodb_lock_wait_timeout", "interactive_timeout", "license", "lower_case_table_names", "max_allowed_packet",
    "max_connections", "net_buffer_length", "net_write_timeout", "performance_schema", "sql_mode", "system_time_zone",
    "time_zone", "transaction_isolation", "transaction_read_only", "tx_isolation", "version", "version_comment",
    "wait_timeout",
];

struct Toks {
    t: Vec<String>,
    i: usize,
}

impl Toks {
    fn new(sql: &str) -> Toks {
        let mut t = Vec::new();
        let b = sql.as_bytes();
        let mut i = 0;
        while i < b.len() {
            let c = b[i];
            if c.is_ascii_whitespace() || c == b';' {
                i += 1;
            } else if c == b'\'' || c == b'"' || c == b'`' {
                let q = c;
                let mut j = i + 1;
                let mut s = String::new();
                s.push(q as char);
                while j < b.len() && b[j] != q {
                    if b[j] == b'\\' && j + 1 < b.len() {
                        j += 1;
                    }
                    s.push(b[j] as char);
                    j += 1;
                }
                t.push(s);
                i = j + 1;
            } else if c.is_ascii_alphanumeric() || c == b'_' || c == b'@' || c == b'$' || c == b'.' || c >= 0x80 {
                let mut j = i;
                while j < b.len() && (b[j].is_ascii_alphanumeric() || b[j] == b'_' || b[j] == b'@' || b[j] == b'$' || b[j] == b'.' || b[j] >= 0x80) {
                    j += 1;
                }
                t.push(sql[i..j].to_string());
                i = j;
            } else {
                t.push((c as char).to_string());
                i += 1;
            }
        }
        Toks { t, i: 0 }
    }
    fn peek_up(&self, k: usize) -> String {
        self.t.get(self.i + k).map(|s| s.to_ascii_uppercase()).unwrap_or_default()
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
    if b.len() >= 1 && (b[0] == b'\'' || b[0] == b'"' || b[0] == b'`') {
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
    let mut t = Toks::new(sql);
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
            if t.peek_up(1) == "TO" {
                return Err(SqlError::unsupported("ROLLBACK TO SAVEPOINT"));
            }
            Admin::Rollback
        }
        "SAVEPOINT" | "RELEASE" => return Err(SqlError::unsupported("savepoints")),
        "USE" => {
            t.i += 1;
            Admin::Use(unquote(&t.next().ok_or_else(|| SqlError::parse("USE needs a database"))?))
        }
        "SET" => {
            t.i += 1;
            parse_set(&mut t)?
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
        "SELECT" if t.t.get(1).map_or(false, |s| s.starts_with("@@")) => match parse_select_vars(&mut t) {
            Some(v) => Admin::SelectVars(v),
            None => return Ok(None),
        },
        "LOCK" | "UNLOCK" | "FLUSH" | "ANALYZE" | "OPTIMIZE" | "CHECKSUM" | "DO" => Admin::Noop,
        _ => return Ok(None),
    };
    Ok(Some(r))
}

fn parse_set(t: &mut Toks) -> SqlResult<Admin> {
    let w = t.peek_up(0);
    match w.as_str() {
        "NAMES" | "CHARACTER" | "CHARSET" => return Ok(Admin::SetIgnored),
        "TRANSACTION" => return Ok(Admin::SetIgnored),
        "SESSION" | "GLOBAL" | "LOCAL" | "@@SESSION" | "@@GLOBAL" if t.peek_up(1) == "TRANSACTION" => {
            return Ok(Admin::SetIgnored)
        }
        _ => {}
    }
    // SET [SESSION|GLOBAL] name = value [, ...]; only autocommit matters.
    let mut result = Admin::SetIgnored;
    loop {
        let _ = t.is("SESSION") || t.is("GLOBAL") || t.is("LOCAL");
        let name = match t.next() {
            Some(n) => n,
            None => break,
        };
        let name = name.trim_start_matches('@').to_ascii_lowercase();
        let name = name
            .strip_prefix("session.")
            .or_else(|| name.strip_prefix("global."))
            .or_else(|| name.strip_prefix("local."))
            .unwrap_or(&name)
            .to_string();
        if !(t.is("=") || (t.peek_up(0) == ":" && t.peek_up(1) == "=" && {
            t.i += 2;
            true
        })) {
            return Err(SqlError::parse(format!("syntax error near SET {}", name)));
        }
        let val = t.next().map(|s| literal_value(&s)).unwrap_or(Value::Null);
        if name == "autocommit" {
            let on = match &val {
                Value::Int(i) => *i != 0,
                Value::Str(s) => s.eq_ignore_ascii_case(b"on") || s.as_ref() == b"1",
                _ => true,
            };
            result = Admin::SetAutocommit(on);
        } else if matches!(result, Admin::SetIgnored) && name.starts_with('@') {
            result = Admin::SetVar { name, value: val };
        }
        // skip the rest of this assignment (expressions are not evaluated)
        while !t.done() && t.peek_up(0) != "," {
            t.i += 1;
        }
        if !t.is(",") {
            break;
        }
    }
    Ok(result)
}

fn parse_show(t: &mut Toks) -> SqlResult<Admin> {
    let full = t.is("FULL");
    let _ = t.is("GLOBAL") || t.is("SESSION");
    let w = t.next().unwrap_or_default().to_ascii_uppercase();
    Ok(match w.as_str() {
        "DATABASES" | "SCHEMAS" => Admin::ShowDatabases { like: like_clause(t) },
        "TABLES" => {
            let db = from_db(t);
            Admin::ShowTables { db, full, like: like_clause(t) }
        }
        "TABLE" if t.is("STATUS") => {
            let db = from_db(t);
            Admin::ShowTableStatus { db, like: like_clause(t) }
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
        "VARIABLES" => Admin::ShowVariables { like: like_clause(t) },
        "STATUS" => Admin::ShowStatus { like: like_clause(t) },
        "WARNINGS" | "ERRORS" => Admin::ShowWarnings,
        "ENGINES" => Admin::ShowEngines,
        "COLLATION" => Admin::ShowCollation,
        "CHARSET" | "CHARACTER" => Admin::ShowCharset,
        "PROCESSLIST" => Admin::ShowProcesslist,
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
    fn recognizes() {
        assert_eq!(recognize("BEGIN").unwrap(), Some(Admin::Begin { read_only: false }));
        assert_eq!(recognize("start transaction read only").unwrap(), Some(Admin::Begin { read_only: true }));
        assert_eq!(recognize("SET autocommit=0").unwrap(), Some(Admin::SetAutocommit(false)));
        assert_eq!(recognize("SET @@session.autocommit = ON").unwrap(), Some(Admin::SetAutocommit(true)));
        assert_eq!(recognize("SET NAMES utf8mb4").unwrap(), Some(Admin::SetIgnored));
        assert_eq!(recognize("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED").unwrap(), Some(Admin::SetIgnored));
        assert_eq!(
            recognize("select @@version_comment limit 1").unwrap(),
            Some(Admin::SelectVars(vec![("@@version_comment".into(), "@@version_comment".into())]))
        );
        assert_eq!(recognize("SHOW TABLES FROM sbtest LIKE 'sb%'").unwrap(), Some(Admin::ShowTables { db: Some("sbtest".into()), full: false, like: Some("sb%".into()) }));
        assert_eq!(recognize("SELECT 1").unwrap(), None);
        assert_eq!(recognize("USE `sbtest`").unwrap(), Some(Admin::Use("sbtest".into())));
    }
}
