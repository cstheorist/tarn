//! ALTER TABLE, RENAME TABLE, CREATE INDEX and DROP INDEX.
//!
//! `plan` parses the statement: MySQL's ALTER TABLE grammar is wider than
//! the parser's, so the list of alterations is split and recognized here,
//! and column, index and foreign key definitions go through sqlparser.
//! `apply` turns the alterations into the new table definition and the
//! storage steps it needs (`Change`), against the current definition; the
//! cluster runs those steps on every shard (`Cluster::alter_table`, and the
//! protocol in lua/tarn.lua "Schema changes").
//!
//! An added column gets a field after the last one (older rows hold its fill
//! value), and a dropped one keeps its field. Indexing an omitted non-NULL
//! fill materializes the missing tuple suffix under a durable fence, with
//! undo until the change commits. A type change must keep the Tarantool
//! field type (integer and unsigned may change into each other); existing
//! values are checked against the new type. Changes that need every value
//! converted -- INT to VARCHAR, a different DECIMAL scale -- are refused.
//!
//! An engine change runs separately from other alterations. It binds the
//! original physical identity, while the cluster allocates an owned shadow
//! and fresh auxiliary generations (`EngineCopy` and `EngineCutover`).
//! The same copy rebuilds a table in its own engine when a change to its
//! secondary indexes would otherwise build them in place on a replicated
//! cluster (`rebuild_eligible`, issue #267).

use crate::catalog::{self, Catalog, ColumnDef, Fill, IndexDef, TableDef};
use crate::mpw::Mp;
use crate::planner::{self, PlanCtx};
use crate::types::{int_range, BaseType, SqlType};
use sqlparser::ast as A;
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use std::sync::Arc;
use tarn_common::{SqlError, SqlResult, Value};

/// Column position of ADD / MODIFY / CHANGE.
#[derive(Clone, Debug, PartialEq)]
pub enum Pos {
    First,
    After(String),
}

#[derive(Clone, Debug, PartialEq)]
pub enum DefaultChange {
    /// SET DEFAULT: (text, is NULL) as in `ColumnDef`
    Set(Option<String>, bool),
    Drop,
}

#[derive(Clone, Debug)]
pub enum AlterOp {
    AddColumn {
        def: ColumnDef,
        primary: bool,
        unique: bool,
        pos: Option<Pos>,
        if_not_exists: bool,
    },
    DropColumn {
        name: String,
        if_exists: bool,
    },
    /// MODIFY (old = the column's name) and CHANGE
    ChangeColumn {
        old: String,
        def: ColumnDef,
        primary: bool,
        unique: bool,
        pos: Option<Pos>,
        if_exists: bool,
    },
    RenameColumn {
        old: String,
        new: String,
    },
    AlterDefault {
        col: String,
        change: DefaultChange,
    },
    AddIndex {
        name: Option<String>,
        cols: Vec<String>,
        unique: bool,
        invisible: bool,
        comment: String,
        if_not_exists: bool,
    },
    DropIndex {
        name: String,
        if_exists: bool,
    },
    RenameIndex {
        old: String,
        new: String,
    },
    IndexVisibility {
        name: String,
        invisible: bool,
    },
    DropForeignKey {
        name: String,
        if_exists: bool,
    },
    /// DROP CONSTRAINT / DROP CHECK: a foreign key, a unique key, or (not
    /// recorded) a CHECK constraint
    DropConstraint {
        name: String,
    },
    RenameTable {
        db: String,
        name: String,
    },
    AutoIncrement(u64),
    Comment(String),
    /// ENGINE = memtx | vinyl
    Engine(crate::catalog::Engine),
    /// [DEFAULT] CHARSET / COLLATE: the table's default collation; with
    /// `convert` (CONVERT TO CHARACTER SET ...) every text column's too.
    TableCollation {
        collation: String,
        convert: bool,
    },
    /// ADD / DROP PRIMARY KEY: the primary key decides where rows live
    PrimaryKey,
    /// Accepted without effect (ENGINE, ALGORITHM, LOCK, DISABLE KEYS, ...)
    Ignored,
}

#[derive(Clone, Debug)]
pub struct AlterPlan {
    pub db: String,
    pub table: String,
    pub ops: Vec<AlterOp>,
}

// ---------------------------------------------------------------------------
// Lexing and splitting
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
enum T {
    /// identifier or keyword as written
    Word(String),
    /// `quoted` identifier, unquoted
    Quoted(String),
    /// 'string' / "string", unquoted
    Str(String),
    Num(String),
    P(char),
}

#[derive(Clone, Debug)]
struct Tok {
    t: T,
    start: usize,
    end: usize,
}

fn lex(sql: &str) -> SqlResult<Vec<Tok>> {
    let b = sql.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if c == b'#' || (c == b'-' && b.get(i + 1) == Some(&b'-') && b.get(i + 2).is_none_or(|x| *x <= b' ')) {
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if c == b'/' && b.get(i + 1) == Some(&b'*') {
            let mut j = i + 2;
            while j + 1 < b.len() && !(b[j] == b'*' && b[j + 1] == b'/') {
                j += 1;
            }
            i = (j + 2).min(b.len());
            continue;
        }
        if c == b'\'' || c == b'"' || c == b'`' {
            let q = c;
            let mut j = i + 1;
            let mut s = Vec::new();
            loop {
                if j >= b.len() {
                    return Err(SqlError::parse("unterminated quoted string"));
                }
                if b[j] == b'\\' && q != b'`' && j + 1 < b.len() {
                    let escaped = match b[j + 1] {
                        b'b' => 8,
                        b'n' => b'\n',
                        b'r' => b'\r',
                        b't' => b'\t',
                        b'0' => 0,
                        b'Z' => 26,
                        // These retain their backslash outside LIKE patterns.
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
            let text = String::from_utf8_lossy(&s).into_owned();
            out.push(Tok {
                t: if q == b'`' { T::Quoted(text) } else { T::Str(text) },
                start: i,
                end: j + 1,
            });
            i = j + 1;
            continue;
        }
        if c.is_ascii_digit() {
            let mut j = i;
            while j < b.len() && (b[j].is_ascii_alphanumeric() || b[j] == b'.' || b[j] == b'_') {
                j += 1;
            }
            let w = &sql[i..j];
            out.push(Tok {
                t: if w.bytes().all(|x| x.is_ascii_digit() || x == b'.') {
                    T::Num(w.to_string())
                } else {
                    T::Word(w.to_string())
                },
                start: i,
                end: j,
            });
            i = j;
            continue;
        }
        if c.is_ascii_alphabetic() || c == b'_' || c == b'$' || c >= 0x80 {
            let mut j = i;
            while j < b.len() && (b[j].is_ascii_alphanumeric() || b[j] == b'_' || b[j] == b'$' || b[j] >= 0x80) {
                j += 1;
            }
            out.push(Tok {
                t: T::Word(sql[i..j].to_string()),
                start: i,
                end: j,
            });
            i = j;
            continue;
        }
        out.push(Tok {
            t: T::P(c as char),
            start: i,
            end: i + 1,
        });
        i += 1;
    }
    Ok(out)
}

/// A cursor over a token slice of `sql`.
struct Cur<'a> {
    sql: &'a str,
    t: &'a [Tok],
    i: usize,
}

impl<'a> Cur<'a> {
    fn new(sql: &'a str, t: &'a [Tok]) -> Cur<'a> {
        Cur { sql, t, i: 0 }
    }
    fn done(&self) -> bool {
        self.i >= self.t.len()
    }
    fn kw_at(&self, k: usize) -> Option<String> {
        match self.t.get(self.i + k).map(|x| &x.t) {
            Some(T::Word(w)) => Some(w.to_ascii_uppercase()),
            _ => None,
        }
    }
    fn peek_kw(&self, w: &str) -> bool {
        self.kw_at(0).as_deref() == Some(w)
    }
    fn eat(&mut self, w: &str) -> bool {
        if self.peek_kw(w) {
            self.i += 1;
            true
        } else {
            false
        }
    }
    fn eat_p(&mut self, p: char) -> bool {
        if matches!(self.t.get(self.i).map(|x| &x.t), Some(T::P(c)) if *c == p) {
            self.i += 1;
            true
        } else {
            false
        }
    }
    fn expect(&mut self, w: &str) -> SqlResult<()> {
        if self.eat(w) {
            Ok(())
        } else {
            Err(self.syntax())
        }
    }
    fn syntax(&self) -> SqlError {
        let near = self.t.get(self.i).map(|x| &self.sql[x.start..]).unwrap_or("");
        SqlError::new(
            1064,
            "42000",
            format!(
                "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near '{}'",
                near.chars().take(80).collect::<String>()
            ),
        )
    }
    /// An identifier (plain or quoted).
    fn ident(&mut self) -> SqlResult<String> {
        match self.t.get(self.i).map(|x| &x.t) {
            Some(T::Word(w)) => {
                self.i += 1;
                Ok(w.clone())
            }
            Some(T::Quoted(w)) => {
                self.i += 1;
                Ok(w.clone())
            }
            _ => Err(self.syntax()),
        }
    }
    /// `name` or `db.name`
    fn qualified(&mut self, ctx: &PlanCtx) -> SqlResult<(String, String)> {
        let a = self.ident()?;
        if self.eat_p('.') {
            let b = self.ident()?;
            Ok((a, b))
        } else {
            let db = ctx
                .db
                .ok_or_else(|| SqlError::new(1046, "3D000", "No database selected"))?;
            Ok((db.to_string(), a))
        }
    }
    /// Text of the tokens from `from` up to (not including) the current one.
    fn text_from(&self, from: usize) -> &'a str {
        if from >= self.t.len() || from >= self.i {
            return "";
        }
        &self.sql[self.t[from].start..self.t[self.i - 1].end]
    }
    /// Text of the remaining tokens.
    fn rest(&self) -> &'a str {
        if self.done() {
            ""
        } else {
            &self.sql[self.t[self.i].start..self.t[self.t.len() - 1].end]
        }
    }
    /// Optional `=`
    fn eq(&mut self) {
        let _ = self.eat_p('=');
    }
}

/// Split tokens at commas outside parentheses.
fn split_commas(t: &[Tok]) -> Vec<&[Tok]> {
    let mut out = Vec::new();
    let (mut depth, mut start) = (0i32, 0usize);
    for (i, x) in t.iter().enumerate() {
        match x.t {
            T::P('(') => depth += 1,
            T::P(')') => depth -= 1,
            T::P(',') if depth == 0 => {
                out.push(&t[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    out.push(&t[start..]);
    out.into_iter().filter(|s| !s.is_empty()).collect()
}

fn parse_create_table_body(body: &str) -> SqlResult<A::CreateTable> {
    let sql = format!("CREATE TABLE _tarn_alter ({})", body);
    match Parser::parse_sql(&MySqlDialect {}, &sql) {
        Ok(mut v) if v.len() == 1 => match v.pop().unwrap() {
            A::Statement::CreateTable(ct) => Ok(ct),
            _ => Err(SqlError::parse("expected a definition")),
        },
        Ok(_) => Err(SqlError::parse("expected a definition")),
        Err(e) => Err(SqlError::parse(format!("{}", e))),
    }
}

/// A column definition as written in ALTER TABLE.
fn column_def(text: &str) -> SqlResult<planner::ColumnSpec> {
    let ct = parse_create_table_body(text)?;
    match ct.columns.as_slice() {
        [c] => planner::column_spec(c),
        _ => Err(SqlError::parse(format!("bad column definition: {}", text))),
    }
}

/// Strip a trailing `FIRST` / `AFTER col` from a column definition.
fn position(spec: &[Tok]) -> SqlResult<(usize, Option<Pos>)> {
    let n = spec.len();
    let word = |k: usize| match spec.get(k).map(|x| &x.t) {
        Some(T::Word(w)) => Some(w.to_ascii_uppercase()),
        _ => None,
    };
    if n >= 1 && word(n - 1).as_deref() == Some("FIRST") {
        return Ok((n - 1, Some(Pos::First)));
    }
    if n >= 2 && word(n - 2).as_deref() == Some("AFTER") {
        let col = match &spec[n - 1].t {
            T::Word(w) | T::Quoted(w) => w.clone(),
            _ => return Err(SqlError::parse("AFTER needs a column name")),
        };
        return Ok((n - 2, Some(Pos::After(col))));
    }
    Ok((n, None))
}

fn span(sql: &str, t: &[Tok]) -> String {
    if t.is_empty() {
        String::new()
    } else {
        sql[t[0].start..t[t.len() - 1].end].to_string()
    }
}

/// Remove VISIBLE / INVISIBLE words (index options); returns whether
/// INVISIBLE was among them.
fn strip_visibility(t: &[Tok]) -> (Vec<Tok>, bool) {
    let mut invisible = false;
    let kept = t
        .iter()
        .filter(|x| match &x.t {
            T::Word(w) if w.eq_ignore_ascii_case("INVISIBLE") => {
                invisible = true;
                false
            }
            T::Word(w) if w.eq_ignore_ascii_case("VISIBLE") => false,
            _ => true,
        })
        .cloned()
        .collect();
    (kept, invisible)
}

/// Rebuild text from tokens (spans), keeping the original spelling.
fn tokens_text(sql: &str, t: &[Tok]) -> String {
    t.iter().map(|x| &sql[x.start..x.end]).collect::<Vec<_>>().join(" ")
}

/// ADD <constraint or index definition> through the CREATE TABLE parser.
fn table_element(sql: &str, spec: &[Tok], _ctx: &PlanCtx) -> SqlResult<AlterOp> {
    let (spec, invisible) = strip_visibility(spec);
    let text = tokens_text(sql, &spec);
    let ct = parse_create_table_body(&format!("_tarn_x INT, {}", text))?;
    let con = ct
        .constraints
        .first()
        .ok_or_else(|| SqlError::parse(format!("bad index definition: {}", text)))?;
    Ok(match con {
        A::TableConstraint::PrimaryKey(_) => AlterOp::PrimaryKey,
        A::TableConstraint::Unique(u) => AlterOp::AddIndex {
            name: u.name.as_ref().or(u.index_name.as_ref()).map(|n| n.value.clone()),
            cols: planner::index_col_names(&u.columns)?,
            unique: true,
            invisible,
            comment: planner::index_comment(&u.index_options),
            if_not_exists: false,
        },
        A::TableConstraint::Index(ix) => AlterOp::AddIndex {
            name: ix.name.as_ref().map(|n| n.value.clone()),
            cols: planner::index_col_names(&ix.columns)?,
            unique: false,
            invisible,
            comment: planner::index_comment(&ix.index_options),
            if_not_exists: false,
        },
        A::TableConstraint::ForeignKey(_) => return Err(crate::integrity::reject("FOREIGN KEY")),
        A::TableConstraint::Check(_) => return Err(crate::integrity::reject("CHECK")),
        other => return Err(SqlError::unsupported(format!("ALTER TABLE ADD {}", other))),
    })
}

fn default_change(expr_text: &str) -> SqlResult<DefaultChange> {
    let e = Parser::new(&MySqlDialect {})
        .try_with_sql(expr_text)
        .and_then(|mut p| p.parse_expr())
        .map_err(|e| SqlError::parse(format!("{}", e)))?;
    let e = match e {
        A::Expr::Nested(x) => *x,
        x => x,
    };
    Ok(match e {
        A::Expr::Value(v) if matches!(v.value, A::Value::Null) => DefaultChange::Set(None, true),
        A::Expr::Value(v) => DefaultChange::Set(
            Some(match &v.value {
                A::Value::SingleQuotedString(s) | A::Value::DoubleQuotedString(s) => s.clone(),
                A::Value::Number(n, _) => n.clone(),
                A::Value::Boolean(b) => (*b as i32).to_string(),
                other => other.to_string(),
            }),
            false,
        ),
        A::Expr::UnaryOp {
            op: A::UnaryOperator::Minus,
            expr,
        } => DefaultChange::Set(Some(format!("-{}", expr)), false),
        other => {
            let s = other.to_string().to_ascii_uppercase();
            if s.starts_with("CURRENT_TIMESTAMP") || s.starts_with("NOW") {
                DefaultChange::Set(Some("CURRENT_TIMESTAMP".into()), false)
            } else {
                return Err(SqlError::unsupported(format!("DEFAULT {}", other)));
            }
        }
    })
}

/// One alteration of ALTER TABLE.
fn alteration(sql: &str, spec: &[Tok], ctx: &PlanCtx, out: &mut Vec<AlterOp>) -> SqlResult<()> {
    let mut c = Cur::new(sql, spec);
    let first = c.kw_at(0).unwrap_or_default();
    match first.as_str() {
        "ADD" => {
            c.i += 1;
            let k = c.kw_at(0).unwrap_or_default();
            match k.as_str() {
                "INDEX" | "KEY" | "UNIQUE" | "CONSTRAINT" | "FOREIGN" | "PRIMARY" | "CHECK" => {
                    out.push(table_element(sql, &spec[c.i..], ctx)?);
                }
                "FULLTEXT" | "SPATIAL" => return Err(SqlError::unsupported(format!("{} indexes", k))),
                "PARTITION" => return Err(SqlError::unsupported("partitions")),
                _ => {
                    let _ = c.eat("COLUMN");
                    let mut if_not_exists = false;
                    if c.peek_kw("IF") && c.kw_at(1).as_deref() == Some("NOT") {
                        c.i += 2;
                        c.expect("EXISTS")?;
                        if_not_exists = true;
                    }
                    if c.eat_p('(') {
                        // ADD [COLUMN] (c1 def, c2 def, ...)
                        let inner = &spec[c.i..spec.len().saturating_sub(1)];
                        if !matches!(spec.last().map(|x| &x.t), Some(T::P(')'))) {
                            return Err(c.syntax());
                        }
                        for part in split_commas(inner) {
                            let s = column_def(&span(sql, part))?;
                            out.push(AlterOp::AddColumn {
                                def: s.def,
                                primary: s.primary,
                                unique: s.unique,
                                pos: None,
                                if_not_exists,
                            });
                        }
                    } else {
                        let rest = &spec[c.i..];
                        let (end, pos) = position(rest)?;
                        let s = column_def(&span(sql, &rest[..end]))?;
                        out.push(AlterOp::AddColumn {
                            def: s.def,
                            primary: s.primary,
                            unique: s.unique,
                            pos,
                            if_not_exists,
                        });
                    }
                }
            }
        }
        "DROP" => {
            c.i += 1;
            let if_exists = |c: &mut Cur| -> SqlResult<bool> {
                if c.peek_kw("IF") && c.kw_at(1).as_deref() == Some("EXISTS") {
                    c.i += 2;
                    Ok(true)
                } else {
                    Ok(false)
                }
            };
            if c.eat("INDEX") || c.eat("KEY") {
                let ie = if_exists(&mut c)?;
                out.push(AlterOp::DropIndex {
                    name: c.ident()?,
                    if_exists: ie,
                });
            } else if c.eat("PRIMARY") {
                c.expect("KEY")?;
                out.push(AlterOp::PrimaryKey);
            } else if c.eat("FOREIGN") {
                c.expect("KEY")?;
                let ie = if_exists(&mut c)?;
                out.push(AlterOp::DropForeignKey {
                    name: c.ident()?,
                    if_exists: ie,
                });
            } else if c.eat("CHECK") || c.eat("CONSTRAINT") {
                let _ = if_exists(&mut c)?;
                out.push(AlterOp::DropConstraint { name: c.ident()? });
            } else if c.peek_kw("PARTITION") {
                return Err(SqlError::unsupported("partitions"));
            } else {
                let _ = c.eat("COLUMN");
                let ie = if_exists(&mut c)?;
                out.push(AlterOp::DropColumn {
                    name: c.ident()?,
                    if_exists: ie,
                });
            }
            if !c.done() && !c.peek_kw("CASCADE") && !c.peek_kw("RESTRICT") {
                return Err(c.syntax());
            }
        }
        "MODIFY" | "CHANGE" => {
            c.i += 1;
            let _ = c.eat("COLUMN");
            let mut if_exists = false;
            if c.peek_kw("IF") && c.kw_at(1).as_deref() == Some("EXISTS") {
                c.i += 2;
                if_exists = true;
            }
            let old = if first == "CHANGE" { Some(c.ident()?) } else { None };
            let rest = &spec[c.i..];
            let (end, pos) = position(rest)?;
            let s = column_def(&span(sql, &rest[..end]))?;
            out.push(AlterOp::ChangeColumn {
                old: old.unwrap_or_else(|| s.def.name.clone()),
                def: s.def,
                primary: s.primary,
                unique: s.unique,
                pos,
                if_exists,
            });
        }
        "ALTER" => {
            c.i += 1;
            if c.eat("INDEX") || c.eat("KEY") {
                let name = c.ident()?;
                let invisible = if c.eat("INVISIBLE") {
                    true
                } else if c.eat("VISIBLE") {
                    false
                } else {
                    return Err(c.syntax());
                };
                out.push(AlterOp::IndexVisibility { name, invisible });
            } else if c.eat("CHECK") || c.eat("CONSTRAINT") {
                return Err(crate::integrity::reject("CHECK"));
            } else {
                let _ = c.eat("COLUMN");
                let col = c.ident()?;
                if c.eat("SET") {
                    if c.eat("DEFAULT") {
                        out.push(AlterOp::AlterDefault {
                            col,
                            change: default_change(c.rest())?,
                        });
                    } else if c.eat("VISIBLE") || c.eat("INVISIBLE") {
                        return Err(SqlError::unsupported("invisible columns"));
                    } else {
                        return Err(c.syntax());
                    }
                } else if c.eat("DROP") {
                    c.expect("DEFAULT")?;
                    out.push(AlterOp::AlterDefault {
                        col,
                        change: DefaultChange::Drop,
                    });
                } else {
                    return Err(c.syntax());
                }
            }
        }
        "RENAME" => {
            c.i += 1;
            if c.eat("COLUMN") {
                let old = c.ident()?;
                c.expect("TO")?;
                out.push(AlterOp::RenameColumn { old, new: c.ident()? });
            } else if c.eat("INDEX") || c.eat("KEY") {
                let old = c.ident()?;
                c.expect("TO")?;
                out.push(AlterOp::RenameIndex { old, new: c.ident()? });
            } else {
                let _ = c.eat("TO") || c.eat("AS");
                let (db, name) = c.qualified(ctx)?;
                out.push(AlterOp::RenameTable { db, name });
            }
        }
        "AUTO_INCREMENT" => {
            c.i += 1;
            c.eq();
            let n = match spec.get(c.i).map(|x| &x.t) {
                Some(T::Num(n)) => n.parse::<u64>().map_err(|_| c.syntax())?,
                _ => return Err(c.syntax()),
            };
            out.push(AlterOp::AutoIncrement(n));
        }
        "COMMENT" => {
            c.i += 1;
            c.eq();
            match spec.get(c.i).map(|x| &x.t) {
                Some(T::Str(s)) => out.push(AlterOp::Comment(s.clone())),
                _ => return Err(c.syntax()),
            }
        }
        "ENGINE" => {
            c.i += 1;
            c.eq();
            let name = c.ident()?;
            // memtx and vinyl name an engine; MySQL's own names (InnoDB,
            // ...) are accepted without effect, as `ALTER TABLE t
            // ENGINE=InnoDB` is MySQL's way to rebuild a table.
            match crate::catalog::Engine::from_option(&name)? {
                Some(e) => out.push(AlterOp::Engine(e)),
                None => out.push(AlterOp::Ignored),
            }
        }
        "CONVERT" => {
            // CONVERT TO CHARACTER SET cs [COLLATE co]: text is utf8mb4
            let words: Vec<String> = spec
                .iter()
                .filter_map(|x| match &x.t {
                    T::Word(w) | T::Quoted(w) | T::Str(w) => Some(w.to_ascii_lowercase()),
                    _ => None,
                })
                .collect();
            let cs = words
                .iter()
                .skip_while(|w| *w != "set" && *w != "charset")
                .nth(1)
                .cloned()
                .unwrap_or_default();
            if !matches!(cs.as_str(), "utf8mb4" | "utf8mb3" | "utf8" | "default") {
                return Err(SqlError::unsupported(format!(
                    "character set '{}' (text is stored as utf8mb4)",
                    cs
                )));
            }
            match table_collation(spec)? {
                Some(collation) => out.push(AlterOp::TableCollation {
                    collation,
                    convert: true,
                }),
                None => out.push(AlterOp::Ignored),
            }
        }
        "DEFAULT" | "CHARACTER" | "CHARSET" | "COLLATE" if !c.peek_kw_at(1, "KEYS") => match table_collation(spec)? {
            Some(collation) => out.push(AlterOp::TableCollation {
                collation,
                convert: false,
            }),
            None => out.push(AlterOp::Ignored),
        },
        "DEFAULT" | "CHARACTER" | "CHARSET" | "COLLATE" | "ROW_FORMAT" | "KEY_BLOCK_SIZE" | "AVG_ROW_LENGTH"
        | "CHECKSUM" | "PAGE_CHECKSUM" | "COMPRESSION" | "DELAY_KEY_WRITE" | "ENCRYPTION" | "INSERT_METHOD"
        | "MAX_ROWS" | "MIN_ROWS" | "PACK_KEYS" | "STATS_AUTO_RECALC" | "STATS_PERSISTENT" | "STATS_SAMPLE_PAGES"
        | "TRANSACTIONAL" | "ALGORITHM" | "LOCK" | "FORCE" | "ORDER" | "DISABLE" | "ENABLE" | "WITH" | "WITHOUT"
        | "TABLESPACE" => {
            if (first == "DISABLE" || first == "ENABLE") && !c.peek_kw_at(1, "KEYS") {
                return Err(SqlError::unsupported(format!("ALTER TABLE {}", span(sql, spec))));
            }
            out.push(AlterOp::Ignored);
        }
        "PARTITION" | "REMOVE" | "COALESCE" | "REORGANIZE" | "EXCHANGE" | "ANALYZE" | "CHECK" | "OPTIMIZE"
        | "REBUILD" | "REPAIR" | "TRUNCATE" | "DISCARD" | "IMPORT" | "UPGRADE" => {
            return Err(SqlError::unsupported(format!("ALTER TABLE ... {}", first)));
        }
        _ => return Err(c.syntax()),
    }
    Ok(())
}

impl Cur<'_> {
    fn peek_kw_at(&self, k: usize, w: &str) -> bool {
        self.kw_at(k).as_deref() == Some(w)
    }
}

/// The collation table options in `spec` name: `[DEFAULT] COLLATE [=] co`,
/// else the default of `[DEFAULT] {CHARACTER SET | CHARSET} [=] cs`.
fn table_collation(spec: &[Tok]) -> SqlResult<Option<String>> {
    let words: Vec<String> = spec
        .iter()
        .filter_map(|x| match &x.t {
            T::Word(w) | T::Quoted(w) | T::Str(w) => Some(w.clone()),
            _ => None,
        })
        .collect();
    let after = |k: &str| -> Option<&String> {
        let i = words.iter().position(|w| w.eq_ignore_ascii_case(k))?;
        let j = if k.eq_ignore_ascii_case("CHARACTER") {
            i + 2
        } else {
            i + 1
        };
        words.get(j)
    };
    if let Some(co) = after("COLLATE") {
        return Ok(Some(planner::known_collation(co)?));
    }
    match after("CHARSET").or_else(|| after("CHARACTER")) {
        Some(cs) if cs.eq_ignore_ascii_case("DEFAULT") => Ok(None),
        Some(cs) => Ok(Some(
            planner::charset_default_collation(cs)
                .ok_or_else(|| SqlError::new(1115, "42000", format!("Unknown character set: '{}'", cs)))?
                .to_string(),
        )),
        None => Ok(None),
    }
}

/// Plan ALTER TABLE, RENAME TABLE, CREATE INDEX or DROP INDEX; None for
/// other statements.
pub fn plan(sql: &str, ctx: &PlanCtx) -> SqlResult<Option<Vec<AlterPlan>>> {
    let toks = lex(sql)?;
    let toks: Vec<Tok> = match toks.last() {
        Some(Tok { t: T::P(';'), .. }) => toks[..toks.len() - 1].to_vec(),
        _ => toks,
    };
    let mut c = Cur::new(sql, &toks);
    let first = c.kw_at(0).unwrap_or_default();
    match first.as_str() {
        "ALTER" => {
            c.i += 1;
            while c.eat("ONLINE") || c.eat("OFFLINE") || c.eat("IGNORE") {}
            if !c.eat("TABLE") {
                return Ok(None);
            }
            let (db, table) = c.qualified(ctx)?;
            let mut ops = Vec::new();
            for spec in split_commas(&toks[c.i..]) {
                alteration(sql, spec, ctx, &mut ops)?;
            }
            Ok(Some(vec![AlterPlan { db, table, ops }]))
        }
        "RENAME" => {
            c.i += 1;
            if !(c.eat("TABLE") || c.eat("TABLES")) {
                return Ok(None);
            }
            let mut plans = Vec::new();
            for spec in split_commas(&toks[c.i..]) {
                let mut s = Cur::new(sql, spec);
                let (db, table) = s.qualified(ctx)?;
                s.expect("TO")?;
                let (ndb, name) = s.qualified(ctx)?;
                if !s.done() {
                    return Err(s.syntax());
                }
                plans.push(AlterPlan {
                    db,
                    table,
                    ops: vec![AlterOp::RenameTable { db: ndb, name }],
                });
            }
            Ok(Some(plans))
        }
        "DROP" if c.kw_at(1).as_deref() == Some("INDEX") => {
            c.i += 2;
            let name = c.ident()?;
            c.expect("ON")?;
            let (db, table) = c.qualified(ctx)?;
            // [ALGORITHM [=] x] [LOCK [=] y]: accepted
            let ops = if name.eq_ignore_ascii_case("PRIMARY") {
                vec![AlterOp::PrimaryKey]
            } else {
                vec![AlterOp::DropIndex { name, if_exists: false }]
            };
            Ok(Some(vec![AlterPlan { db, table, ops }]))
        }
        "CREATE" => {
            c.i += 1;
            let unique = c.eat("UNIQUE");
            if c.eat("FULLTEXT") || c.eat("SPATIAL") {
                return Err(SqlError::unsupported("FULLTEXT and SPATIAL indexes"));
            }
            if !c.eat("INDEX") {
                return Ok(None);
            }
            let mut if_not_exists = false;
            if c.peek_kw("IF") && c.kw_at(1).as_deref() == Some("NOT") {
                c.i += 2;
                c.expect("EXISTS")?;
                if_not_exists = true;
            }
            let name = c.ident()?;
            if c.eat("USING") {
                c.ident()?;
            }
            c.expect("ON")?;
            let (db, table) = c.qualified(ctx)?;
            if !c.eat_p('(') {
                return Err(c.syntax());
            }
            let start = c.i;
            let mut depth = 1;
            while !c.done() {
                match c.t[c.i].t {
                    T::P('(') => depth += 1,
                    T::P(')') => {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                    }
                    _ => {}
                }
                c.i += 1;
            }
            let cols_text = c.text_from(start).to_string();
            c.i += 1;
            let (rest, invisible) = strip_visibility(&toks[c.i.min(toks.len())..]);
            // USING, ALGORITHM, LOCK: accepted
            let comment = rest
                .windows(2)
                .rev()
                .find_map(|w| match (&w[0].t, &w[1].t) {
                    (T::Word(k), T::Str(s)) if k.eq_ignore_ascii_case("COMMENT") => Some(s.clone()),
                    _ => None,
                })
                .unwrap_or_default();
            let ct = parse_create_table_body(&format!("_tarn_x INT, KEY _tarn_k ({})", cols_text))?;
            let cols = match ct.constraints.first() {
                Some(A::TableConstraint::Index(ix)) => planner::index_col_names(&ix.columns)?,
                _ => return Err(SqlError::parse("bad index column list")),
            };
            Ok(Some(vec![AlterPlan {
                db,
                table,
                ops: vec![AlterOp::AddIndex {
                    name: Some(name),
                    cols,
                    unique,
                    invisible,
                    comment,
                    if_not_exists,
                }],
            }]))
        }
        _ => Ok(None),
    }
}

// ---------------------------------------------------------------------------
// Applying alterations to a definition
// ---------------------------------------------------------------------------

/// A check of existing values on the shards (lua `check_value`).
#[derive(Clone, Debug, PartialEq)]
pub enum CheckKind {
    NotNull,
    Chars(u32),
    Range(i128, i128),
    Digits(u32),
    /// A native index cannot substitute a non-NULL fill for a missing field.
    IndexFieldPresent,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Check {
    /// Tarantool field number, 1-based
    pub field: u32,
    pub col: String,
    pub kind: CheckKind,
    /// The column's value in rows whose tuple ends before the field
    pub fill: Value,
}

/// A new index for the shards to build: Tarantool id, name, unique,
/// parts (1-based field, type, nullable).
#[derive(Clone, Debug)]
pub struct Build {
    pub id: u32,
    pub name: String,
    pub unique: bool,
    pub parts: Vec<(u32, String, bool)>,
}

/// The original identity bound by an owned engine replacement.
#[derive(Clone, Debug)]
pub struct EngineChange {
    pub old_id: u32,
    pub old_ver: u64,
    pub old_engine: catalog::Engine,
    pub old_name: String,
    /// A rebuild in the same engine, with the change's index builds, drops
    /// and renames, rather than a change of engine.
    pub rebuild: bool,
    /// The copy keeps its capture durably too (issue #184), so that one
    /// lost by a leader change or a restart resumes instead of starting
    /// over: only when every replica answers
    /// `tarn_engine_durable_capabilities`, since a leader that does not note
    /// its writes there would leave a resume after it short of them.
    pub durable: bool,
}

/// The result of `apply`: the new definition and what the shards do.
#[derive(Clone, Debug)]
pub struct Change {
    pub def: TableDef,
    /// The original physical identity. The cluster allocates a replacement
    /// base and fresh auxiliary generations before journaling the job.
    pub engine_change: Option<EngineChange>,
    /// The new space format, if any field changes.
    pub format: Option<Vec<(String, String, bool)>>,
    /// Tarantool ids of indexes to drop.
    pub drop: Vec<u32>,
    /// (Tarantool id, new name) of kept indexes that are renamed.
    pub renames: Vec<(u32, String)>,
    pub builds: Vec<Build>,
    pub checks: Vec<Check>,
    /// An index needs physical fields that old tuples may omit. The
    /// indexed-presence checks remain on the wire for older storage nodes.
    pub materialize: bool,
    /// Width of the committed layout, before this change allocates fields.
    pub old_width: u32,
    /// Complete suffix defaults: (number of fields always present, values
    /// after those fields). Existing slots use the OLD definition's fills
    /// so readers of it cannot observe a proposed DROP/MODIFY during PREPARE.
    pub pad: Option<(usize, Vec<Value>)>,
    /// New space name.
    pub rename: Option<String>,
    /// AUTO_INCREMENT = n.
    pub auto_increment: Option<u64>,
    /// A column became AUTO_INCREMENT: the generator must pass its largest
    /// value (read through this index, whose first column it is).
    pub auto_inc_from: Option<u32>,
    /// The definition changed (else nothing to do on the shards).
    pub changed: bool,
    /// Space ids of global indexes this change drops (their unique keys go).
    pub uix_drop: Vec<u32>,
    /// Space ids of global indexes it adds (allocated by `Cluster::alter_table`).
    pub uix_new: Vec<u32>,
    pub lazy_drop: Vec<u32>,
    pub lazy_new: Vec<u32>,
    /// A step of an online build of global indexes (`online_steps`): the
    /// shards store its fence, and fill none of its new spaces.
    pub building: bool,
    /// Conflicts spaces of building global indexes that must be empty on
    /// each shard behind its fence (the step that makes them enforcing).
    pub uix_check: Vec<u32>,
    /// The first step of an online build whose last would build the keys'
    /// local indexes in place on replicated shards: the shards refuse it
    /// above `alter_check_rows` rows (1846), as they refuse such a fill.
    pub building_limit: bool,
}

fn not_found_col(c: &str) -> SqlError {
    SqlError::new(1054, "42S22", format!("Unknown column '{}' in 'table'", c))
}

/// Shift logical column indexes after inserting a column at `at` (or
/// removing one, `removed`).
fn remap_columns(def: &mut TableDef, f: &dyn Fn(usize) -> Option<usize>) {
    for ix in &mut def.indexes {
        ix.cols = ix.cols.iter().filter_map(|&c| f(c)).collect();
    }
    def.shard_key = def.shard_key.iter().filter_map(|&c| f(c)).collect();
    def.auto_inc = def.auto_inc.and_then(f);
}

fn insert_column(def: &mut TableDef, col: ColumnDef, pos: &Option<Pos>) -> SqlResult<usize> {
    let at = match pos {
        None => def.columns.len(),
        Some(Pos::First) => 0,
        Some(Pos::After(c)) => def.col_index(c).ok_or_else(|| not_found_col(c))? + 1,
    };
    remap_columns(def, &|c| Some(if c >= at { c + 1 } else { c }));
    def.columns.insert(at, col);
    Ok(at)
}

fn move_column(def: &mut TableDef, from: usize, pos: &Pos) -> SqlResult<()> {
    let col = def.columns.remove(from);
    remap_columns(def, &|c| match c.cmp(&from) {
        std::cmp::Ordering::Less => Some(c),
        std::cmp::Ordering::Equal => Some(usize::MAX),
        std::cmp::Ordering::Greater => Some(c - 1),
    });
    let at = match pos {
        Pos::First => 0,
        Pos::After(c) => def.col_index(c).ok_or_else(|| not_found_col(c))? + 1,
    };
    remap_columns(def, &|c| {
        Some(if c == usize::MAX {
            at
        } else if c >= at {
            c + 1
        } else {
            c
        })
    });
    def.columns.insert(at, col);
    Ok(())
}

/// The value MySQL gives existing rows for a NOT NULL column added without
/// a default.
fn implicit_default(t: &SqlType) -> String {
    use BaseType::*;
    match t.base {
        TinyInt | SmallInt | MediumInt | Int | BigInt | Bool | Float | Double | Decimal => "0".into(),
        Year => "0".into(),
        Date => "0000-00-00".into(),
        DateTime | Timestamp => "0000-00-00 00:00:00".into(),
        Time => "00:00:00".into(),
        _ => String::new(),
    }
}

/// Families of types whose values keep their stored form across a change.
fn family(t: &SqlType) -> u8 {
    use BaseType::*;
    match t.base {
        TinyInt | SmallInt | MediumInt | Int | BigInt | Bool => 1,
        Year => 2,
        Float | Double => 3,
        Decimal => 4,
        Char | VarChar | Text | Json => 5,
        Binary | VarBinary | Blob => 6,
        Date => 7,
        DateTime | Timestamp => 8,
        Time => 9,
    }
}

fn max_chars(t: &SqlType) -> Option<u32> {
    match t.base {
        BaseType::Char | BaseType::VarChar => Some(t.len.unwrap_or(if t.base == BaseType::Char { 1 } else { 255 })),
        _ => None,
    }
}

/// Checks needed to change a column from `old` to `new` type and
/// nullability; error if the change would need rows rewritten.
fn type_change(old: &ColumnDef, new: &ColumnDef, field: u32, fill: &Value, pk: bool) -> SqlResult<Vec<Check>> {
    let (a, b) = (old.ty(), new.ty());
    let mut checks = Vec::new();
    let refuse = || {
        SqlError::unsupported(format!(
            "changing column '{}' from {} to {} (every row would have to be rewritten)",
            old.name,
            a.display(),
            b.display()
        ))
    };
    if family(a) != family(b) {
        return Err(refuse());
    }
    let (sa, sb) = (a.tarantool_type(), b.tarantool_type());
    if sa != sb && pk {
        return Err(SqlError::unsupported(format!(
            "changing the storage type of primary key column '{}'",
            old.name
        )));
    }
    let check = |kind| Check {
        field,
        col: new.name.clone(),
        kind,
        fill: fill.clone(),
    };
    match family(a) {
        1 | 2 => {
            let (alo, ahi) = int_range(a);
            let (blo, bhi) = int_range(b);
            if blo > alo || bhi < ahi {
                checks.push(check(CheckKind::Range(blo, bhi)));
            }
        }
        8 => {
            // stored values carry exactly fsp fractional digits (#37)
            if a.fsp() != b.fsp() {
                return Err(refuse());
            }
        }
        4 => {
            if a.scale.unwrap_or(0) != b.scale.unwrap_or(0) {
                return Err(refuse());
            }
            let s = b.scale.unwrap_or(0);
            let (pa, pb) = (a.len.unwrap_or(10), b.len.unwrap_or(10));
            if pb < pa {
                checks.push(check(CheckKind::Digits(pb.saturating_sub(s))));
            }
        }
        5 => {
            if let Some(nb) = max_chars(b) {
                if max_chars(a).is_none_or(|na| nb < na) {
                    checks.push(check(CheckKind::Chars(nb)));
                }
            }
        }
        _ => {}
    }
    if old.nullable && !new.nullable {
        checks.push(check(CheckKind::NotNull));
    }
    Ok(checks)
}

/// A column's collation may change only where no index orders by it: an
/// index (or the weight field kept for one) would have to be rebuilt in a
/// new order from rewritten rows (1235).
fn check_collation_change(def: &TableDef, i: usize, new: &ColumnDef) -> SqlResult<()> {
    let cur = &def.columns[i];
    if !cur.ty().is_text() && !new.ty().is_text() {
        return Ok(());
    }
    let (a, b) = (cur.coll(), new.coll());
    let ordered = cur.weight.is_some() || def.indexes.iter().any(|ix| ix.cols.contains(&i));
    if a != b && ordered {
        return Err(SqlError::unsupported(format!(
            "changing the collation of indexed column '{}' from {} to {} (its index order would change)",
            cur.name,
            cur.collation_name(),
            new.collation_name()
        )));
    }
    Ok(())
}

/// Apply the alterations of `plan` to `def` (the current definition).
pub fn apply(def: &TableDef, plan: &AlterPlan, cat: &Catalog, now: &str) -> SqlResult<Change> {
    let requested_engine = plan
        .ops
        .iter()
        .filter_map(|op| match op {
            AlterOp::Engine(engine) => Some(*engine),
            _ => None,
        })
        .next_back()
        .unwrap_or(def.engine);
    let engine_change = requested_engine != def.engine;
    let engine_only = plan
        .ops
        .iter()
        .all(|op| matches!(op, AlterOp::Engine(_) | AlterOp::Ignored));
    if engine_change && !engine_only {
        return Err(SqlError::unsupported(
            "combining an engine change with other schema changes; run ALTER TABLE ... ENGINE separately",
        ));
    }
    let mut nd = def.clone();
    // Make the layout explicit: from now on fields are not positions.
    for (i, c) in nd.columns.iter_mut().enumerate() {
        if c.field.is_none() {
            c.field = Some(i as u32);
        }
    }
    if nd.bucket.is_none() {
        nd.bucket = Some(def.columns.len() as u32);
    }
    let mut next_field = def.layout.width as u32;
    let mut checks: Vec<Check> = Vec::new();
    // Old columns by field, for the storage diff.
    let mut rename = None;
    let mut auto_increment = None;
    let mut auto_inc_from = None;
    let mut changed = false;
    let mut became_auto_inc = None;
    // Index ids already allocated by this statement.
    let mut next_id = planner::next_index_id(&nd);
    let alloc_id = |next_id: &mut u32| {
        let id = *next_id;
        *next_id += 1;
        id
    };
    let check_index_name = |n: &str| -> SqlResult<()> {
        if n.eq_ignore_ascii_case("PRIMARY") || n.eq_ignore_ascii_case(catalog::BUCKET_FIELD) || n.starts_with("_tarn")
        {
            return Err(SqlError::new(1280, "42000", format!("Incorrect index name '{}'", n)));
        }
        Ok(())
    };
    for op in &plan.ops {
        match op {
            AlterOp::Ignored => {}
            AlterOp::PrimaryKey => {
                return Err(SqlError::unsupported(
                    "changing the primary key (it decides which shard holds each row)",
                ))
            }
            AlterOp::AddColumn {
                def: col,
                primary,
                unique,
                pos,
                if_not_exists,
            } => {
                if nd.col_index(&col.name).is_some() {
                    if *if_not_exists {
                        continue;
                    }
                    return Err(SqlError::new(
                        1060,
                        "42S21",
                        format!("Duplicate column name '{}'", col.name),
                    ));
                }
                if *primary {
                    return Err(SqlError::new(1068, "42000", "Multiple primary key defined"));
                }
                if col.auto_increment {
                    return Err(SqlError::unsupported("adding an AUTO_INCREMENT column"));
                }
                let mut col = col.clone();
                if col.ty().is_text() && col.collation.is_none() {
                    col.collation = Some(nd.collation_name().to_string());
                }
                col.field = Some(next_field);
                next_field += 1;
                col.storage_nullable = Some(true);
                let fill = if col.default_null {
                    None
                } else {
                    match &col.default {
                        Some(d) if d == "CURRENT_TIMESTAMP" => Some(now.to_string()),
                        Some(d) => Some(d.clone()),
                        None if col.nullable => None,
                        None => Some(implicit_default(col.ty())),
                    }
                };
                col.fill = Some(Fill { value: fill });
                let name = col.name.clone();
                let at = insert_column(&mut nd, col, pos)?;
                if *unique {
                    let n = planner::generated_index_name(&name, &|x| nd.index_by_name(x).is_some());
                    nd.indexes.push(IndexDef {
                        name: n,
                        id: alloc_id(&mut next_id),
                        cols: vec![at],
                        unique: true,
                        invisible: false,
                        global: None,
                        lazy: None,
                        comment: String::new(),
                        building: None,
                    });
                }
                changed = true;
            }
            AlterOp::DropColumn { name, if_exists } => {
                let Some(i) = nd.col_index(name) else {
                    if *if_exists {
                        continue;
                    }
                    return Err(SqlError::new(
                        1091,
                        "42000",
                        format!("Can't DROP COLUMN `{}`; check that it exists", name),
                    ));
                };
                if nd.visible_columns().count() == 1 {
                    return Err(SqlError::new(
                        1090,
                        "42000",
                        "You can't delete all columns with ALTER TABLE; use DROP TABLE instead",
                    ));
                }
                if nd.is_pk_col(i) || nd.shard_key.contains(&i) {
                    return Err(SqlError::unsupported(format!(
                        "dropping primary key column '{}' (it decides which shard holds each row)",
                        nd.columns[i].name
                    )));
                }
                let cname = nd.columns[i].name.clone();
                if let Some(fk) = nd
                    .foreign_keys
                    .iter()
                    .find(|f| f.cols.iter().any(|c| c.eq_ignore_ascii_case(&cname)))
                {
                    // As MariaDB: the index the foreign key needs is named
                    // first, if there is one.
                    let fcols: Vec<usize> = fk.cols.iter().filter_map(|c| nd.col_index(c)).collect();
                    if let Some(ix) = nd
                        .indexes
                        .iter()
                        .find(|ix| ix.id != 0 && ix.cols.len() >= fcols.len() && ix.cols[..fcols.len()] == fcols[..])
                    {
                        return Err(SqlError::new(
                            1553,
                            "HY000",
                            format!("Cannot drop index '{}': needed in a foreign key constraint", ix.name),
                        ));
                    }
                    return Err(SqlError::new(
                        1828,
                        "HY000",
                        format!(
                            "Cannot drop column '{}': needed in a foreign key constraint '{}'",
                            cname, fk.name
                        ),
                    ));
                }
                // Indexes lose the column; an index left with none goes, the
                // others are rebuilt (new id).
                let mut kept = Vec::new();
                for mut ix in std::mem::take(&mut nd.indexes) {
                    if ix.cols.contains(&i) && ix.id != 0 {
                        ix.cols.retain(|&c| c != i);
                        if ix.cols.is_empty() {
                            continue;
                        }
                        ix.id = alloc_id(&mut next_id);
                    }
                    kept.push(ix);
                }
                nd.indexes = kept;
                let field = nd.columns[i].field.unwrap_or(i as u32);
                nd.dropped.push(field);
                if let Some(w) = nd.columns[i].weight {
                    nd.dropped.push(w);
                }
                nd.columns.remove(i);
                remap_columns(&mut nd, &|c| match c.cmp(&i) {
                    std::cmp::Ordering::Less => Some(c),
                    std::cmp::Ordering::Equal => None,
                    std::cmp::Ordering::Greater => Some(c - 1),
                });
                changed = true;
            }
            AlterOp::ChangeColumn {
                old,
                def: col,
                primary,
                unique,
                pos,
                if_exists,
            } => {
                let Some(i) = nd.col_index(old) else {
                    if *if_exists {
                        continue;
                    }
                    return Err(not_found_col(old));
                };
                if *primary {
                    return Err(SqlError::new(1068, "42000", "Multiple primary key defined"));
                }
                if !col.name.eq_ignore_ascii_case(old) && nd.col_index(&col.name).is_some() {
                    return Err(SqlError::new(
                        1060,
                        "42S21",
                        format!("Duplicate column name '{}'", col.name),
                    ));
                }
                let cur = nd.columns[i].clone();
                let pk = nd.is_pk_col(i) || nd.shard_key.contains(&i);
                let mut col = col.clone();
                if pk && col.nullable {
                    return Err(SqlError::new(
                        1171,
                        "42000",
                        "All parts of a PRIMARY KEY must be NOT NULL; if you need NULL in a key, use UNIQUE instead",
                    ));
                }
                if col.ty().is_text() && col.collation.is_none() {
                    // MODIFY / CHANGE without COLLATE: the table's default
                    col.collation = Some(nd.collation_name().to_string());
                }
                check_collation_change(&nd, i, &col)?;
                let field = cur.field.unwrap_or(i as u32);
                let fill_v = cur
                    .fill
                    .as_ref()
                    .and_then(|f| f.value.as_ref())
                    .map(|v| cur.ty().coerce(Value::str(v), &cur.name))
                    .transpose()?
                    .unwrap_or(Value::Null);
                checks.extend(type_change(&cur, &col, field + 1, &fill_v, pk)?);
                col.field = Some(field);
                col.weight = cur.weight;
                // A field never becomes NOT NULL in the format again: the SQL
                // layer enforces NOT NULL.
                col.storage_nullable = Some(cur.storage_nullable() || col.nullable);
                col.fill = cur.fill.clone();
                if let Some(Fill { value: Some(v) }) = &col.fill {
                    // A fill the new type cannot hold matters only if rows
                    // still end before the field; the checks find those.
                    if col.ty().coerce(Value::str(v), &col.name).is_err() {
                        col.fill = Some(Fill { value: None });
                    }
                }
                if col.auto_increment && nd.auto_inc != Some(i) {
                    if nd.auto_inc.is_some() {
                        return Err(SqlError::new(
                            1075,
                            "42000",
                            "Incorrect table definition; there can be only one auto column and it must be defined as a key",
                        ));
                    }
                    became_auto_inc = Some(col.name.clone());
                }
                if !col.auto_increment && nd.auto_inc == Some(i) {
                    nd.auto_inc = None;
                }
                // Foreign keys follow a rename.
                if !col.name.eq(&cur.name) {
                    for fk in &mut nd.foreign_keys {
                        for c in &mut fk.cols {
                            if c.eq_ignore_ascii_case(&cur.name) {
                                *c = col.name.clone();
                            }
                        }
                    }
                }
                // Indexes on a column whose stored type or nullability is
                // relaxed are rebuilt (their parts would refuse new values);
                // integer -> unsigned keeps them.
                let relaxed = (cur.ty().tarantool_type() != col.ty().tarantool_type()
                    && !(cur.ty().tarantool_type() == "integer" && col.ty().tarantool_type() == "unsigned"))
                    || (!cur.storage_nullable() && col.storage_nullable());
                let name = col.name.clone();
                nd.columns[i] = col;
                if relaxed {
                    for ix in nd.indexes.iter_mut() {
                        if ix.id != 0 && ix.cols.contains(&i) {
                            ix.id = alloc_id(&mut next_id);
                        }
                    }
                }
                if let Some(p) = pos {
                    move_column(&mut nd, i, p)?;
                }
                if *unique {
                    let at = nd.col_index(&name).unwrap();
                    let n = planner::generated_index_name(&name, &|x| nd.index_by_name(x).is_some());
                    nd.indexes.push(IndexDef {
                        name: n,
                        id: alloc_id(&mut next_id),
                        cols: vec![at],
                        unique: true,
                        invisible: false,
                        global: None,
                        lazy: None,
                        comment: String::new(),
                        building: None,
                    });
                }
                changed = true;
            }
            AlterOp::RenameColumn { old, new } => {
                let i = nd.col_index(old).ok_or_else(|| not_found_col(old))?;
                if !new.eq_ignore_ascii_case(old) && nd.col_index(new).is_some() {
                    return Err(SqlError::new(1060, "42S21", format!("Duplicate column name '{}'", new)));
                }
                let cur = nd.columns[i].name.clone();
                for fk in &mut nd.foreign_keys {
                    for c in &mut fk.cols {
                        if c.eq_ignore_ascii_case(&cur) {
                            *c = new.clone();
                        }
                    }
                }
                nd.columns[i].name = new.clone();
                changed = true;
            }
            AlterOp::AlterDefault { col, change } => {
                let i = nd.col_index(col).ok_or_else(|| not_found_col(col))?;
                let c = &mut nd.columns[i];
                match change {
                    DefaultChange::Drop => {
                        c.default = None;
                        c.default_null = false;
                    }
                    DefaultChange::Set(v, null) => {
                        if *null && !c.nullable {
                            return Err(SqlError::new(
                                1067,
                                "42000",
                                format!("Invalid default value for '{}'", c.name),
                            ));
                        }
                        if let Some(v) = v {
                            if v != "CURRENT_TIMESTAMP" && c.ty().coerce(Value::str(v), &c.name).is_err() {
                                return Err(SqlError::new(
                                    1067,
                                    "42000",
                                    format!("Invalid default value for '{}'", c.name),
                                ));
                            }
                        }
                        c.default = v.clone();
                        c.default_null = *null;
                    }
                }
                changed = true;
            }
            AlterOp::AddIndex {
                name,
                cols,
                unique,
                invisible,
                comment,
                if_not_exists,
            } => {
                let mut ix = Vec::new();
                for c in cols {
                    ix.push(nd.col_index(c).ok_or_else(|| {
                        SqlError::new(1072, "42000", format!("Key column '{}' doesn't exist in table", c))
                    })?);
                }
                let name = match name {
                    Some(n) => {
                        if nd.index_by_name(n).is_some() {
                            if *if_not_exists {
                                continue;
                            }
                            return Err(SqlError::new(1061, "42000", format!("Duplicate key name '{}'", n)));
                        }
                        check_index_name(n)?;
                        n.clone()
                    }
                    None => planner::generated_index_name(&nd.columns[ix[0]].name, &|x| nd.index_by_name(x).is_some()),
                };
                nd.indexes.push(IndexDef {
                    name,
                    id: alloc_id(&mut next_id),
                    cols: ix,
                    unique: *unique,
                    invisible: *invisible,
                    global: None,
                    lazy: None,
                    comment: comment.clone(),
                    building: None,
                });
                changed = true;
            }
            AlterOp::DropIndex { name, if_exists } => {
                match nd.indexes.iter().position(|i| i.name.eq_ignore_ascii_case(name)) {
                    Some(0) => {
                        return Err(SqlError::unsupported(
                            "changing the primary key (it decides which shard holds each row)",
                        ))
                    }
                    Some(k) => {
                        nd.indexes.remove(k);
                        changed = true;
                    }
                    None if *if_exists => {}
                    None => {
                        return Err(SqlError::new(
                            1091,
                            "42000",
                            format!("Can't DROP INDEX `{}`; check that it exists", name),
                        ))
                    }
                }
            }
            AlterOp::RenameIndex { old, new } => {
                let k = nd
                    .indexes
                    .iter()
                    .position(|i| i.name.eq_ignore_ascii_case(old))
                    .ok_or_else(|| {
                        SqlError::new(
                            1176,
                            "42000",
                            format!("Key '{}' doesn't exist in table '{}'", old, nd.name),
                        )
                    })?;
                if k == 0 {
                    return Err(SqlError::new(1280, "42000", format!("Incorrect index name '{}'", old)));
                }
                if !old.eq_ignore_ascii_case(new) && nd.index_by_name(new).is_some() {
                    return Err(SqlError::new(1061, "42000", format!("Duplicate key name '{}'", new)));
                }
                check_index_name(new)?;
                nd.indexes[k].name = new.clone();
                changed = true;
            }
            AlterOp::IndexVisibility { name, invisible } => {
                let k = nd
                    .indexes
                    .iter()
                    .position(|i| i.name.eq_ignore_ascii_case(name))
                    .ok_or_else(|| {
                        SqlError::new(
                            1176,
                            "42000",
                            format!("Key '{}' doesn't exist in table '{}'", name, nd.name),
                        )
                    })?;
                if k == 0 && *invisible {
                    return Err(SqlError::new(3522, "HY000", "A primary key index cannot be invisible"));
                }
                nd.indexes[k].invisible = *invisible;
                changed = true;
            }
            AlterOp::DropForeignKey { name, if_exists } => {
                match nd.foreign_keys.iter().position(|f| f.name.eq_ignore_ascii_case(name)) {
                    Some(k) => {
                        nd.foreign_keys.remove(k);
                        changed = true;
                    }
                    None if *if_exists => {}
                    None => {
                        return Err(SqlError::new(
                            1091,
                            "42000",
                            format!("Can't DROP FOREIGN KEY `{}`; check that it exists", name),
                        ))
                    }
                }
            }
            AlterOp::DropConstraint { name } => {
                if let Some(k) = nd.foreign_keys.iter().position(|f| f.name.eq_ignore_ascii_case(name)) {
                    nd.foreign_keys.remove(k);
                    changed = true;
                } else if let Some(k) = nd
                    .indexes
                    .iter()
                    .position(|i| i.unique && i.name.eq_ignore_ascii_case(name))
                {
                    if k == 0 {
                        return Err(SqlError::unsupported("changing the primary key"));
                    }
                    nd.indexes.remove(k);
                    changed = true;
                }
                // otherwise a CHECK constraint, which is not recorded
            }
            AlterOp::RenameTable { db, name } => {
                if !cat.has_db(db) {
                    return Err(SqlError::new(1049, "42000", format!("Unknown database '{}'", db)));
                }
                let same = db.eq_ignore_ascii_case(&def.db) && name.eq_ignore_ascii_case(&def.name);
                if !same && cat.get(db, name).is_some() {
                    return Err(SqlError::new(1050, "42S01", format!("Table '{}' already exists", name)));
                }
                if !same {
                    nd.db = db.clone();
                    nd.name = name.clone();
                    rename = Some(catalog::space_name(db, name));
                    changed = true;
                }
            }
            AlterOp::AutoIncrement(n) => auto_increment = Some(*n),
            AlterOp::Comment(c) => {
                nd.comment = c.clone();
                changed = true;
            }
            AlterOp::TableCollation { collation, convert } => {
                if *convert {
                    for i in 0..nd.columns.len() {
                        if nd.columns[i].ty().is_text() {
                            let mut col = nd.columns[i].clone();
                            col.collation = Some(collation.clone());
                            check_collation_change(&nd, i, &col)?;
                            nd.columns[i].collation = Some(collation.clone());
                        }
                    }
                }
                nd.collation = Some(collation.clone());
                changed = true;
            }
            AlterOp::Engine(e) => {
                nd.engine = *e;
            }
        }
    }
    if engine_only {
        changed = engine_change;
    }
    if let Some(name) = became_auto_inc {
        let i = nd.col_index(&name).unwrap();
        let Some(ix) = nd.indexes.iter().find(|ix| ix.cols.first() == Some(&i)) else {
            return Err(SqlError::new(
                1075,
                "42000",
                "Incorrect table definition; there can be only one auto column and it must be defined as a key",
            ));
        };
        nd.auto_inc = Some(i);
        auto_inc_from = Some(ix.id);
    }
    if nd.columns.iter().filter(|c| c.auto_increment).count() > 1 {
        return Err(SqlError::new(
            1075,
            "42000",
            "Incorrect table definition; there can be only one auto column and it must be defined as a key",
        ));
    }
    for (k, c) in nd.columns.iter_mut().enumerate() {
        c.auto_increment = nd.auto_inc == Some(k);
    }
    nd.resolve_types()?;
    // Case-insensitive columns in an index need weight fields: a column
    // added by this statement gets one (its older rows end before it and
    // take the weight of its fill); an existing column without one would
    // need every row rewritten (1235).
    let old_width = def.layout.width as u32;
    nd.assign_weight_fields(&|c| c.field.is_some_and(|f| f >= old_width))?;
    nd.declare_lazy(Some(def))?;
    catalog::validate(&nd)?;
    let uix_drop = check_globals(def, &mut nd)?;
    let lazy_drop = check_lazy(def, &nd)?;
    // Storage: format and indexes, by difference.
    let old_format = def.lua_format();
    let new_format = nd.lua_format();
    let format = (old_format != new_format).then_some(new_format);
    let old_ids: Vec<u32> = def.indexes.iter().map(|i| i.id).collect();
    let new_ids: Vec<u32> = nd.indexes.iter().map(|i| i.id).collect();
    let drop: Vec<u32> = old_ids
        .iter()
        .copied()
        .filter(|id| *id != 0 && !new_ids.contains(id))
        .collect();
    let mut renames = Vec::new();
    let mut builds = Vec::new();
    for ix in &nd.indexes {
        match def.indexes.iter().find(|o| o.id == ix.id) {
            Some(o) => {
                if o.id != 0 && o.name != ix.name {
                    renames.push((ix.id, ix.name.clone()));
                }
            }
            None => builds.push(Build {
                id: ix.id,
                name: ix.name.clone(),
                unique: ix.unique,
                parts: nd.index_part_fields(ix),
            }),
        }
    }
    // Instant ADD leaves short tuples in storage: SQL supplies their frozen
    // fills, but a native index sees NULL for a missing field. Check before
    // publishing any new index that could disagree, including weight fields.
    // NULL fills need no check; a missing field already indexes as NULL.
    for ix in nd.indexes.iter().filter(|ix| builds.iter().any(|b| b.id == ix.id)) {
        for &c in &ix.cols {
            if nd.columns[c].fill.is_none() || nd.layout.fills[c] == Value::Null {
                continue;
            }
            let field = nd.layout.col_weight[c].unwrap_or(nd.layout.col_field[c]) as u32 + 1;
            if !checks
                .iter()
                .any(|ch| ch.field == field && ch.kind == CheckKind::IndexFieldPresent)
            {
                checks.push(Check {
                    field,
                    col: nd.columns[c].name.clone(),
                    kind: CheckKind::IndexFieldPresent,
                    fill: nd.layout.fills[c].clone(),
                });
            }
        }
    }
    let materialize = checks.iter().any(|c| c.kind == CheckKind::IndexFieldPresent);
    let pad = if materialize {
        let (base, mut values) = nd
            .update_pad()
            .ok_or_else(|| SqlError::internal("indexed instant fill has no tuple suffix"))?;
        if let Some((old_base, old_values)) = def.update_pad() {
            if old_base != base || old_values.len() > values.len() {
                return Err(SqlError::internal("ALTER changed the committed tuple prefix"));
            }
            // The old catalog stays public throughout PREPARE. In
            // particular, a column this change drops still has its old
            // fill, rather than the new layout's anonymous NULL slot.
            values[..old_values.len()].clone_from_slice(&old_values);
        }
        Some((base, values))
    } else {
        None
    };
    Ok(Change {
        def: nd,
        engine_change: engine_change.then(|| EngineChange {
            old_id: def.space_id,
            old_ver: def.ver,
            old_engine: def.engine,
            old_name: def.space_name(),
            rebuild: false,
            durable: false,
        }),
        format,
        drop,
        renames,
        builds,
        checks,
        materialize,
        old_width,
        pad,
        rename,
        auto_increment,
        auto_inc_from,
        changed,
        uix_drop,
        uix_new: Vec::new(),
        lazy_drop,
        lazy_new: Vec::new(),
        building: false,
        uix_check: Vec::new(),
        building_limit: false,
    })
}

/// Whether change `ch` of `def` adds unique keys without the shard key, and
/// changes nothing else, so that their global indexes are built online
/// (`online_steps`, issue #292) rather than filled behind the fence, which
/// refuses tables above `alter_check_rows` rows per shard.
pub fn builds_globals_online(def: &TableDef, ch: &Change) -> bool {
    let n = def.indexes.len();
    if ch.def.indexes.len() <= n || def.indexes.iter().any(|i| i.building.is_some()) {
        return false;
    }
    let new = &ch.def.indexes[n..];
    // (The rest of the definition as it was: its version aside, and the
    // fields of its columns and bucket, which `apply` writes out, compared
    // as laid out.)
    let rest = |t: &TableDef| {
        let mut t = t.clone();
        t.indexes.truncate(n);
        t.ver = 0;
        let mut v = serde_json::to_value(&t).ok()?;
        let o = v.as_object_mut()?;
        o.remove("bucket");
        for c in o.get_mut("columns")?.as_array_mut()? {
            c.as_object_mut()?.remove("field");
        }
        Some((v, t.layout.col_field.clone(), t.layout.bucket))
    };
    ch.changed
        && ch.engine_change.is_none()
        && new
            .iter()
            .all(|i| i.global == Some(0) && i.lazy.is_none() && ch.def.needs_global(i))
        && rest(&ch.def) == rest(def)
        && ch.format.is_none()
        && ch.drop.is_empty()
        && ch.renames.is_empty()
        && ch.checks.is_empty()
        && !ch.materialize
        && ch.pad.is_none()
        && ch.rename.is_none()
        && ch.auto_increment.is_none()
        && ch.auto_inc_from.is_none()
        && ch.uix_drop.is_empty()
        && ch.lazy_drop.is_empty()
        && ch.builds.len() == new.len()
        && ch.builds.iter().all(|b| new.iter().any(|i| i.id == b.id))
}

/// The steps of an online build of the global indexes change `ch` of `def`
/// adds (`builds_globals_online`; tla/GlobalUniqueBuild.tla), at versions
/// `vers`, with the space ids of each new index's entries and conflicts
/// (`ids`, in index order):
/// 1. the building version: the indexes building, their spaces made, no
///    local index yet;
/// 2. the enforcing version: the local unique indexes built, the conflicts
///    checked empty behind the fence and dropped;
/// 3. if the second fails: the original definition, the spaces dropped.
pub fn online_steps(def: &TableDef, ch: &Change, vers: [u64; 3], ids: &[(u32, u32)]) -> SqlResult<[Change; 3]> {
    let n = def.indexes.len();
    let mut building = ch.clone();
    building.def.ver = vers[0];
    building.builds = Vec::new();
    building.building = true;
    let mut enforcing = ch.clone();
    enforcing.def.ver = vers[1];
    enforcing.building = true;
    for (k, &(entries, conflicts)) in ids.iter().enumerate() {
        let declared = &mut enforcing.def.indexes[n + k];
        declared.global = Some(entries);
        let ix = &mut building.def.indexes[n + k];
        ix.global = Some(entries);
        ix.building = Some(catalog::Building {
            conflicts,
            invisible: ix.invisible,
        });
        ix.unique = false;
        ix.invisible = true;
        building.uix_new.extend([entries, conflicts]);
        enforcing.uix_check.push(conflicts);
        enforcing.uix_drop.push(conflicts);
    }
    building.def.resolve_globals()?;
    enforcing.def.resolve_globals()?;
    let mut dropped = ch.clone();
    dropped.def = def.clone();
    dropped.def.ver = vers[2];
    dropped.builds = Vec::new();
    dropped.building = true;
    dropped.uix_drop = ids.iter().flat_map(|&(e, c)| [e, c]).collect();
    dropped.def.resolve_globals()?;
    Ok([building, enforcing, dropped])
}

/// Whether change `ch` of `def` can build its new indexes by rebuilding the
/// table into a new space whose indexes exist before the rows are copied in
/// (lua/tarn_engine.lua), rather than in place. In place, each replica of a
/// replicated shard builds an index on a populated space in its applier,
/// which acknowledges nothing until it finishes: the shard's synchronous
/// writes wait, and past twice `replication_timeout` the leader resigns
/// (issue #267). The rebuild takes changes that build secondary indexes --
/// new ones, with drops and renames of others -- also when the same
/// statement adds, renames or drops columns: the new space takes the new
/// format, and the copy pads every row it writes with the fills its indexes
/// need (`pad`), which PREPARE would otherwise materialize behind the fence.
/// It also takes a change that checks rows against a narrower type or NOT
/// NULL: the copy checks every row it writes (`rebuild_checks_rows`, issue
/// #184), with no row limit, rather than PREPARE behind the fence. It keeps
/// the primary key and the table's name. A
/// table's global unique indexes are kept as they are
/// (`rebuild_keeps_globals`), when the change adds and changes none of
/// them, and so are its lazy indexes (`rebuild_keeps_lazy`), when the
/// change changes none of them either: each keeps its generation, whose
/// descriptor differs only in the space it names. Those the change drops
/// are retired as the copy is published (`rebuild_drops_indexes`, issue
/// #184), as an engine change retires all of them. A lazy index the
/// change makes (`rebuild_makes_lazy`) is built online against the old
/// space before the copy, as in place, from its rows as they are stored:
/// it may read no column whose stored field, weight, type or collation the
/// change alters, and the change may materialize no field (an index on a
/// column added with a non-NULL fill, which old rows may lack). A lazy
/// index that the change renews (a new generation for an index on a column
/// made nullable, the old one dropped) is one it makes.
pub fn rebuild_eligible(def: &TableDef, ch: &Change) -> bool {
    // A lazy index not ready, or a global index building (an unfinished
    // change of the table's).
    let unsettled = |t: &TableDef| {
        t.indexes
            .iter()
            .any(|i| i.lazy.as_ref().is_some_and(|l| l.space_id != 0 && !l.ready) || i.building.is_some())
    };
    // Each lazy index the change makes (no generation yet) reads columns
    // the table has, stored as they were, in rows the change does not pad.
    let shape = |t: &TableDef, c: usize| {
        (
            t.layout.col_field[c],
            t.layout.col_weight[c],
            t.columns[c].ty.clone(),
            t.layout.colls[c],
        )
    };
    let made = |i: &IndexDef| i.lazy.as_ref().is_some_and(|l| l.space_id == 0);
    let made_online = !ch.def.indexes.iter().any(made)
        || (!ch.materialize
            && ch.def.layout.bucket == def.layout.bucket
            && ch.def.indexes.iter().filter(|i| made(i)).all(|i| {
                i.cols.iter().chain(&ch.def.pk().cols).all(|&c| {
                    def.col_index(&ch.def.columns[c].name)
                        .is_some_and(|o| shape(def, o) == shape(&ch.def, c))
                })
            }));
    // The lazy generations and global index spaces of a definition, but
    // those dropped: the old definition's kept ones are the new one's.
    let lazy = |t: &TableDef, dropped: &[u32]| {
        let mut specs: Vec<catalog::LazySpec> = t
            .lazy_specs()
            .into_iter()
            .filter(|s| !dropped.contains(&s.gen))
            .map(|mut s| {
                s.base_space_id = 0;
                s
            })
            .collect();
        specs.sort_by_key(|s| s.gen);
        specs
    };
    let globals = |t: &TableDef, dropped: &[u32]| {
        let mut ids: Vec<u32> = t
            .indexes
            .iter()
            .filter_map(|i| i.global)
            .filter(|g| !dropped.contains(g))
            .collect();
        ids.sort_unstable();
        ids
    };
    ch.changed
        && ch.engine_change.is_none()
        && !ch.builds.is_empty()
        && ch.builds.iter().all(|b| b.id != 0)
        && !ch.drop.contains(&0)
        && ch.materialize == ch.pad.is_some()
        && ch.rename.is_none()
        && ch.def.engine == def.engine
        && ch.def.space_id == def.space_id
        && !unsettled(def)
        && !unsettled(&ch.def)
        && ch.lazy_drop.iter().all(|g| lazy(def, &[]).iter().any(|s| s.gen == *g))
        && lazy(def, &ch.lazy_drop) == lazy(&ch.def, &[])
        && made_online
        && ch.uix_new.is_empty()
        && !globals(&ch.def, &[]).contains(&0)
        && ch.uix_drop.iter().all(|g| globals(def, &[]).contains(g))
        && globals(def, &ch.uix_drop) == globals(&ch.def, &[])
}

/// Whether a rebuild of change `ch` drops global or lazy indexes of its
/// table (issue #184). Until PREPARE they take the table's writes; PREPARE
/// holds their spaces with the table's, publication dooms their readers
/// and retires them (a lazy generation in the catalog's transaction), and
/// cleanup drops a global index's space; ABORT leaves them as they were.
/// The storage must know such rebuilds
/// (`tarn_engine_rebuild_drops_capabilities`); one from before refuses
/// them.
pub fn rebuild_drops_indexes(ch: &Change) -> bool {
    !ch.uix_drop.is_empty() || !ch.lazy_drop.is_empty()
}

/// Whether a rebuild checks rows as it copies them: a narrower type or NOT
/// NULL (issue #184). The storage must know such rebuilds
/// (`tarn_engine_rebuild_checks_capabilities`); one from before refuses
/// them.
pub fn rebuild_checks_rows(ch: &Change) -> bool {
    ch.checks.iter().any(|c| c.kind != CheckKind::IndexFieldPresent)
}

/// Whether rebuild `ch` copies a table with global unique indexes: their
/// spaces stay, under the new base's names, but for those the change drops
/// (`rebuild_drops_indexes`). The storage must know such rebuilds
/// (`tarn_engine_rebuild_aux_capabilities`).
pub fn rebuild_keeps_globals(ch: &Change) -> bool {
    ch.def.indexes.iter().any(|i| i.global.is_some()) || !ch.uix_drop.is_empty()
}

/// Whether rebuild `ch` copies a table with lazy indexes: their
/// generations stay, bound to the new space as it is published, but for
/// those the change drops (`rebuild_drops_indexes`). The storage must know
/// such rebuilds (`tarn_engine_rebuild_lazy_capabilities`), whose ABORT
/// retires none of the table's generations. Also when the table's first
/// lazy indexes are ones the rebuild makes.
pub fn rebuild_keeps_lazy(ch: &Change) -> bool {
    ch.def.has_lazy() || !ch.lazy_drop.is_empty()
}

/// Whether a rebuild of change `ch` makes lazy indexes: new generations,
/// built online against the old space before the copy and published with
/// it (`lazy_made`), which the storage must know
/// (`tarn_engine_rebuild_lazy_made_capabilities`).
pub fn rebuild_makes_lazy(ch: &Change) -> bool {
    !ch.lazy_new.is_empty()
        || ch
            .def
            .indexes
            .iter()
            .any(|i| i.lazy.as_ref().is_some_and(|l| l.space_id == 0))
}

/// Whether a rebuild of change `ch` changes the stored layout: the new
/// space's format, or rows padded to fills. Storage from before such
/// rebuilds would copy the old layout under the new definition, so each
/// replica must answer `tarn_engine_rebuild_format_capabilities` first.
pub fn rebuild_changes_layout(ch: &Change) -> bool {
    ch.format.is_some() || ch.pad.is_some()
}

fn check_lazy(def: &TableDef, nd: &TableDef) -> SqlResult<Vec<u32>> {
    let mut dropped = Vec::new();
    let shape = |t: &TableDef, ix: &IndexDef| {
        ix.cols
            .iter()
            .chain(&t.pk().cols)
            .map(|&c| {
                (
                    t.layout.col_field[c],
                    t.layout.col_weight[c],
                    t.columns[c].ty.clone(),
                    t.layout.colls[c],
                )
            })
            .collect::<Vec<_>>()
    };
    for old in def.indexes.iter().filter(|i| i.lazy.is_some()) {
        let l = old.lazy.as_ref().unwrap();
        match nd
            .indexes
            .iter()
            .find(|i| i.lazy.as_ref().is_some_and(|n| n.space_id == l.space_id))
        {
            None => dropped.push(l.space_id),
            Some(new) if old.cols.len() != new.cols.len() || shape(def, old) != shape(nd, new) => {
                return Err(SqlError::unsupported(
                    "changing a lazy index's identity columns; drop and recreate it",
                ));
            }
            Some(_) => {}
        }
    }
    Ok(dropped)
}

/// Global indexes (unique keys without the shard key, see `catalog`): a new
/// unique key that needs one is marked `global: Some(0)` (the space id is
/// allocated when the change runs, `Cluster::alter_table`, and the shards
/// fill it from the existing rows); a dropped one is returned (its space
/// id). Renaming one or changing its columns or their types is refused
/// (1846): its entries would have to be rewritten.
fn check_globals(def: &TableDef, nd: &mut TableDef) -> SqlResult<Vec<u32>> {
    for k in 0..nd.indexes.len() {
        if nd.needs_global(&nd.indexes[k]) && nd.indexes[k].global.is_none() {
            nd.indexes[k].global = Some(0);
        }
    }
    let entry_cols = |t: &TableDef, g: &catalog::GlobalIndex| -> Vec<(String, String)> {
        g.src
            .iter()
            .map(|&c| (t.columns[c].name.clone(), t.columns[c].ty.clone()))
            .collect()
    };
    let mut dropped = Vec::new();
    for g in &def.globals {
        let old = &def.indexes[g.pos];
        let Some(n) = nd.globals.iter().find(|n| n.table.space_id == g.table.space_id) else {
            dropped.push(g.table.space_id);
            continue;
        };
        let new = &nd.indexes[n.pos];
        if new.name != old.name || entry_cols(nd, n) != entry_cols(def, g) {
            return Err(SqlError::new(
                1846,
                "0A000",
                format!(
                    "ALTER TABLE is not supported. Reason: unique key '{}' does not contain the shard key; \
                     its columns and name cannot change yet",
                    old.name
                ),
            ));
        }
    }
    Ok(dropped)
}

/// The argument of `tarn_alter_prepare` / `_apply` / `_abort` (`ch.def`
/// carries version `ver`).
/// Whether an ALTER's job arguments (`lua_args`) copy the table: they name
/// an `engine_change`, which an engine change has, and a rebuild that copies
/// a table of replicated shards.
pub(crate) fn job_copies(args: &[u8]) -> SqlResult<bool> {
    let mut rd = crate::mpw::Rd::new(args);
    for _ in 0..rd.map()? {
        if rd.string()? == "engine_change" {
            return Ok(true);
        }
        rd.skip()?;
    }
    Ok(false)
}

pub fn lua_args(ch: &Change, ver: u64, timeout: f64) -> Vec<u8> {
    let mut m = Mp::new();
    // A rebuild names the lazy generations it makes, which it builds online
    // against the old space before its copy (`rebuild_makes_lazy`).
    let made = ch.engine_change.as_ref().is_some_and(|e| e.rebuild) && !ch.lazy_new.is_empty();
    m.map(
        14 + !ch.lazy_drop.is_empty() as usize
            + ch.engine_change.is_some() as usize
            + made as usize
            + ch.building as usize
            + !ch.uix_check.is_empty() as usize
            + ch.building_limit as usize,
    );
    // (Storage from before ignores them: the SQL server builds a global
    // index online only when every replica answers
    // tarn_uix_building_capabilities.)
    if ch.building {
        m.str("building").bool(true);
    }
    if ch.building_limit {
        m.str("building_limit").bool(true);
    }
    if !ch.uix_check.is_empty() {
        m.str("uix_check").arr(ch.uix_check.len());
        for &id in &ch.uix_check {
            m.uint(id as u64);
        }
    }
    if let Some(engine) = &ch.engine_change {
        m.str("engine_change")
            .map(4 + engine.rebuild as usize + engine.durable as usize)
            .str("old_id")
            .uint(engine.old_id as u64)
            .str("old_ver")
            .uint(engine.old_ver)
            .str("old_engine")
            .str(engine.old_engine.name())
            .str("old_name")
            .str(&engine.old_name);
        // A rebuild that keeps lazy or global indexes, or changes the stored
        // layout, says so: storage that predates one refuses a value it does
        // not know ('lazy' keeps global indexes too, and 'lazy' and 'aux' may
        // change the layout). So does one that drops all of them.
        if engine.rebuild && rebuild_keeps_lazy(ch) {
            m.str("rebuild").str("lazy");
        } else if engine.rebuild && rebuild_keeps_globals(ch) {
            m.str("rebuild").str("aux");
        } else if engine.rebuild && rebuild_changes_layout(ch) {
            m.str("rebuild").str("layout");
        } else if engine.rebuild {
            m.str("rebuild").bool(true);
        }
        // (Storage from before ignores it, and keeps its capture in memory.)
        if engine.durable {
            m.str("durable").bool(true);
        }
    }
    if !ch.lazy_drop.is_empty() {
        m.str("lazy_drop").arr(ch.lazy_drop.len());
        for &id in &ch.lazy_drop {
            m.uint(id as u64);
        }
    }
    if made {
        m.str("lazy_made").arr(ch.lazy_new.len());
        for &id in &ch.lazy_new {
            m.uint(id as u64);
        }
    }
    // Global indexes: the spaces of new ones (PREPARE creates them, the SQL
    // server fills them, `Cluster::fill_globals`) and the ids of dropped
    // ones (APPLY drops them).
    let new_uix: Vec<&Arc<TableDef>> = ch
        .def
        .globals
        .iter()
        .flat_map(|g| std::iter::once(&g.table).chain(&g.conflicts))
        .filter(|t| ch.uix_new.contains(&t.space_id))
        .collect();
    m.str("uix").arr(new_uix.len());
    for t in &new_uix {
        lua_def_entry(&mut m, &t.lua_def(), None);
    }
    m.str("uix_drop").arr(ch.uix_drop.len());
    for id in &ch.uix_drop {
        m.uint(*id as u64);
    }
    m.str("ver").uint(ver);
    m.str("timeout").f64(timeout);
    m.str("materialize").bool(ch.materialize);
    m.str("old_width").uint(ch.old_width as u64);
    m.str("pad");
    match &ch.pad {
        Some((base, values)) => {
            m.arr(values.len() + 1).uint(*base as u64);
            for v in values {
                m.val(v);
            }
        }
        None => {
            m.nil();
        }
    }
    m.str("checks").arr(ch.checks.len());
    for c in &ch.checks {
        let n = match c.kind {
            CheckKind::NotNull | CheckKind::IndexFieldPresent => 3,
            CheckKind::Chars(_) | CheckKind::Digits(_) => 4,
            CheckKind::Range(..) => 5,
        };
        m.arr(6).uint(c.field as u64).str(&c.col);
        match &c.kind {
            CheckKind::NotNull => {
                m.str("notnull");
            }
            CheckKind::IndexFieldPresent => {
                m.str("indexed");
            }
            CheckKind::Chars(x) => {
                m.str("chars").uint(*x as u64);
            }
            CheckKind::Digits(x) => {
                m.str("digits").uint(*x as u64);
            }
            CheckKind::Range(lo, hi) => {
                m.str("range");
                int(&mut m, *lo);
                int(&mut m, *hi);
            }
        }
        for _ in n..5 {
            m.nil();
        }
        m.val(&c.fill);
    }
    m.str("builds").arr(ch.builds.len());
    for b in &ch.builds {
        m.arr(4)
            .uint(b.id as u64)
            .str(&b.name)
            .bool(b.unique)
            .arr(b.parts.len());
        for (f, t, n) in &b.parts {
            m.map(3)
                .str("field")
                .uint(*f as u64)
                .str("type")
                .str(t)
                .str("is_nullable")
                .bool(*n);
        }
    }
    m.str("drop").arr(ch.drop.len());
    for id in &ch.drop {
        m.uint(*id as u64);
    }
    m.str("renames").arr(ch.renames.len());
    for (id, n) in &ch.renames {
        m.arr(2).uint(*id as u64).str(n);
    }
    m.str("format");
    match &ch.format {
        Some(f) => {
            format_list(&mut m, f);
        }
        None => {
            m.nil();
        }
    }
    m.str("rename");
    match &ch.rename {
        Some(n) => m.str(n),
        None => m.nil(),
    };
    m.str("def");
    catalog_entry(&mut m, &ch.def, ver);
    m.0
}

/// A table's catalog entry (`_tarn_catalog`), as tarn_create_table stores
/// it, under definition version `ver`.
pub fn catalog_entry(m: &mut Mp, def: &TableDef, ver: u64) {
    lua_def_entry(m, &def.lua_def(), Some(ver));
}

/// `ld` as the storage takes it: name, id, engine, format, indexes, the
/// global index spaces (`uix`, when there are any), json and `ver`.
pub fn lua_def_entry(m: &mut Mp, ld: &crate::catalog::LuaDef, ver: Option<u64>) {
    m.map(7 + ver.is_some() as usize + !ld.uix.is_empty() as usize + ld.building as usize);
    m.str("name").str(&ld.name);
    m.str("id").uint(ld.id as u64);
    m.str("engine").str(ld.engine);
    m.str("format");
    format_list(m, &ld.format);
    m.str("indexes").arr(ld.indexes.len());
    for (n, unique, parts) in &ld.indexes {
        m.map(3).str("name").str(n).str("unique").bool(*unique).str("parts");
        format_parts(m, parts);
    }
    if !ld.uix.is_empty() {
        m.str("uix").arr(ld.uix.len());
        for u in &ld.uix {
            lua_def_entry(m, u, None);
        }
    }
    // Presence is a capability statement, including when this definition
    // removes the final lazy index. Older gateways omit it and fail closed.
    m.str("lazy").arr(ld.lazy.len());
    for l in &ld.lazy {
        m.map(10)
            .str("gen")
            .uint(l.gen as u64)
            .str("base_space_id")
            .uint(l.base_space_id as u64)
            .str("local_index_id")
            .uint(l.local_index_id as u64)
            .str("route_parts")
            .uint(l.route_parts as u64)
            .str("bucket_field")
            .uint(l.bucket_field as u64);
        for (name, fs) in [
            ("key_fields", &l.key_fields),
            ("pk_fields", &l.pk_fields),
            ("pk_native_fields", &l.pk_native_fields),
        ] {
            m.str(name).arr(fs.len());
            for &f in fs {
                m.uint(f as u64);
            }
        }
        for (name, ts) in [("key_types", &l.key_types), ("pk_types", &l.pk_types)] {
            m.str(name).arr(ts.len());
            for t in ts {
                m.str(t);
            }
        }
    }
    m.str("json").str(&ld.json);
    if let Some(v) = ver {
        m.str("ver").uint(v);
    }
    if ld.building {
        m.str("building").bool(true);
    }
}

fn int(m: &mut Mp, v: i128) {
    if v < 0 {
        m.int(v as i64);
    } else {
        m.uint(v as u64);
    }
}

fn format_list(m: &mut Mp, f: &[(String, String, bool)]) {
    m.arr(f.len());
    for (n, t, nullable) in f {
        m.map(3)
            .str("name")
            .str(n)
            .str("type")
            .str(t)
            .str("is_nullable")
            .bool(*nullable);
    }
}

fn format_parts(m: &mut Mp, parts: &[(String, String, bool)]) {
    m.arr(parts.len());
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mpw::Rd;
    use crate::plan::Stmt;

    #[test]
    fn lazy_incarnation_survives_metadata_changes_and_is_retired_on_drop() {
        let (mut def,_)=table("CREATE TABLE t (tenant_id INT,id INT,k INT,PRIMARY KEY(tenant_id,id),KEY lk(k) COMMENT 'tarn:lazy') PARTITION BY KEY(tenant_id)");
        let l = def.indexes[1].lazy.as_mut().unwrap();
        l.space_id = 10001;
        l.ready = true;
        let cat = Catalog::new(1, vec!["db".into()], vec![def.clone()]);
        let changed = apply(
            &def,
            &plan1("ALTER TABLE t ADD COLUMN n INT DEFAULT 5,RENAME INDEX lk TO lk2", &cat),
            &cat,
            "2026-01-01 00:00:00",
        )
        .unwrap();
        assert_eq!(changed.def.indexes[1].lazy, def.indexes[1].lazy);
        assert_eq!(
            changed.def.lua_def().lazy[0].key_fields,
            def.lua_def().lazy[0].key_fields
        );
        assert!(changed.lazy_drop.is_empty());
        let dropped = apply(
            &def,
            &plan1("ALTER TABLE t DROP INDEX lk", &cat),
            &cat,
            "2026-01-01 00:00:00",
        )
        .unwrap();
        assert_eq!(dropped.lazy_drop, vec![10001]);
        assert!(!dropped.def.has_lazy());
        let mut wire = Mp::new();
        catalog_entry(&mut wire, &dropped.def, 2);
        let mut rd = Rd::new(&wire.0);
        let mut empty_capability = false;
        for _ in 0..rd.map().unwrap() {
            if rd.string().unwrap() == "lazy" {
                assert_eq!(rd.array().unwrap(), 0);
                empty_capability = true;
            } else {
                rd.skip().unwrap();
            }
        }
        assert!(empty_capability);
        assert!(rd.buf.is_empty());
        assert_eq!(
            apply(
                &def,
                &plan1("ALTER TABLE t MODIFY k BIGINT", &cat),
                &cat,
                "2026-01-01 00:00:00"
            )
            .unwrap_err()
            .code,
            1235
        );
        // Merely reloading/changing a pre-feature table must not activate a
        // preexisting comment without a fresh fenced bootstrap.
        def.indexes[1].lazy = None;
        let legacy = Catalog::new(1, vec!["db".into()], vec![def.clone()]);
        let changed = apply(
            &def,
            &plan1("ALTER TABLE t ADD COLUMN n INT", &legacy),
            &legacy,
            "2026-01-01 00:00:00",
        )
        .unwrap();
        assert!(!changed.def.has_lazy());
    }

    fn table(sql: &str) -> (TableDef, Catalog) {
        let c0 = Catalog::new(1, vec!["db".into()], vec![]);
        let ctx = PlanCtx {
            statistics: None,
            catalog: &c0,
            db: Some("db"),
            params: &[],
            conn_coll: "utf8mb4_bin",
            default_coll: "utf8mb4_bin",
        };
        let Stmt::CreateTable { def, .. } = crate::planner::plan(sql, &ctx).unwrap() else {
            panic!("not a CREATE TABLE")
        };
        let mut def = *def;
        def.space_id = 512;
        def.bucket_index = Some(def.indexes.len() as u32);
        let cat = Catalog::new(1, vec!["db".into(), "other".into()], vec![def.clone()]);
        (def, cat)
    }

    fn plan1(sql: &str, cat: &Catalog) -> AlterPlan {
        let ctx = PlanCtx {
            statistics: None,
            catalog: cat,
            db: Some("db"),
            params: &[],
            conn_coll: "utf8mb4_bin",
            default_coll: "utf8mb4_bin",
        };
        let mut v = plan(sql, &ctx).unwrap().expect("a schema change");
        assert_eq!(v.len(), 1);
        v.pop().unwrap()
    }

    fn alter(def: &TableDef, cat: &Catalog, sql: &str) -> SqlResult<Change> {
        let ctx = PlanCtx {
            statistics: None,
            catalog: cat,
            db: Some("db"),
            params: &[],
            conn_coll: "utf8mb4_bin",
            default_coll: "utf8mb4_bin",
        };
        let mut plans = plan(sql, &ctx)?.expect("a schema change");
        assert_eq!(plans.len(), 1);
        apply(def, &plans.remove(0), cat, "2026-10-25 12:00:00")
    }

    const T: &str = "CREATE TABLE t (id INT PRIMARY KEY, a INT, s VARCHAR(10), d DECIMAL(6,2), KEY ka (a))";

    // (A map of a step's storage arguments, `lua_args`: the value of `key`.)
    fn arg(args: &[u8], key: &str) -> Option<Vec<u8>> {
        let mut rd = crate::mpw::Rd::new(args);
        for _ in 0..rd.map().unwrap() {
            let k = rd.string().unwrap();
            let v = rd.raw_value().unwrap().to_vec();
            if k == key {
                return Some(v);
            }
        }
        None
    }

    #[test]
    fn new_global_indexes_alone_build_online() {
        let (def, cat) = table(
            "CREATE TABLE t (tenant_id INT, id INT, a INT, b INT, PRIMARY KEY (tenant_id, id)) \
             PARTITION BY KEY(tenant_id)",
        );
        let online = |sql: &str| builds_globals_online(&def, &alter(&def, &cat, sql).unwrap());
        assert!(online("ALTER TABLE t ADD UNIQUE INDEX ua (a)"));
        assert!(online("CREATE UNIQUE INDEX ua ON t (a)"));
        assert!(online(
            "ALTER TABLE t ADD UNIQUE INDEX ua (a), ADD UNIQUE INDEX ub (b, a)"
        ));
        // A unique key with the shard key is local; other changes along
        // with it fill the global index behind the fence.
        assert!(!online("ALTER TABLE t ADD UNIQUE INDEX ua (a, tenant_id)"));
        assert!(!online("ALTER TABLE t ADD INDEX ka (a)"));
        assert!(!online("ALTER TABLE t ADD UNIQUE INDEX ua (a), ADD INDEX kb (b)"));
        assert!(!online("ALTER TABLE t ADD COLUMN c INT, ADD UNIQUE INDEX ua (a)"));
        assert!(!online("ALTER TABLE t ADD UNIQUE INDEX ua (a), COMMENT 'x'"));
        assert!(!online("ALTER TABLE t ADD UNIQUE INDEX ua (a), RENAME TO t2"));
    }

    #[test]
    fn online_steps_build_then_enforce_or_drop() {
        let (def, cat) = table(
            "CREATE TABLE t (tenant_id INT, id INT, a INT, b INT, PRIMARY KEY (tenant_id, id)) \
             PARTITION BY KEY(tenant_id)",
        );
        let ch = alter(&def, &cat, "ALTER TABLE t ADD UNIQUE INDEX ua (a) INVISIBLE").unwrap();
        let [building, enforcing, dropped] = online_steps(&def, &ch, [11, 12, 13], &[(701, 702)]).unwrap();
        let n = def.indexes.len();
        // 1. Building: neither unique nor visible to anything that would read
        // or enforce it, with no local index, its spaces made.
        let ix = &building.def.indexes[n];
        assert_eq!((ix.unique, ix.invisible, ix.global), (false, true, Some(701)));
        assert_eq!(
            ix.building,
            Some(catalog::Building {
                conflicts: 702,
                invisible: true
            })
        );
        assert!(building.builds.is_empty() && building.building && building.def.ver == 11);
        assert_eq!(building.uix_new, [701, 702]);
        let g = &building.def.globals[0];
        let c = g.conflicts.as_ref().expect("building");
        assert!(g.building() && c.name == format!("{}c", g.table.name) && c.space_id == 702);
        assert_eq!(c.indexes[0].cols, [0, 1, 2]);
        assert_eq!((c.shard_key.clone(), c.base), (vec![0], Some((512, 11))));
        let ld = building.def.lua_def();
        assert!(ld.building && ld.indexes.iter().all(|i| i.0 != "ua"));
        assert_eq!(ld.uix.iter().map(|u| u.id).collect::<Vec<_>>(), [701, 702]);
        assert_eq!(building.def.shown_indexes().count(), n);
        assert!(building.def.usable_indexes().all(|i| i.name != "ua"));
        let args = lua_args(&building, 11, 5.0);
        assert_eq!(arg(&args, "building"), Some(vec![0xc3]));
        assert_eq!(Rd::new(&arg(&args, "uix").unwrap()).array().unwrap(), 2);
        // 2. Enforcing, as declared: its local index built, its conflicts
        // checked empty behind the fence and dropped.
        let ix = &enforcing.def.indexes[n];
        assert_eq!((ix.unique, ix.invisible, ix.global), (true, true, Some(701)));
        assert!(ix.building.is_none() && !enforcing.def.lua_def().building);
        assert_eq!(enforcing.builds.len(), 1);
        assert_eq!(enforcing.builds[0].id, ix.id);
        assert_eq!(
            (enforcing.uix_check.clone(), enforcing.uix_drop.clone()),
            (vec![702], vec![702])
        );
        assert!(enforcing.uix_new.is_empty() && enforcing.def.ver == 12);
        let args = lua_args(&enforcing, 12, 5.0);
        let check = arg(&args, "uix_check").unwrap();
        let mut rd = Rd::new(&check);
        assert_eq!(rd.array().unwrap(), 1);
        assert_eq!(rd.value().unwrap().as_i64(), Some(702));
        // 3. Dropped: the table as it was, both spaces dropped.
        assert_eq!(dropped.def.indexes.len(), n);
        assert_eq!(dropped.uix_drop, [701, 702]);
        assert!(dropped.builds.is_empty() && dropped.uix_new.is_empty() && dropped.def.ver == 13);
        // The building definition goes through the catalog's JSON intact.
        let back = catalog::parse_def_json(&building.def.lua_def().json).unwrap();
        assert_eq!(back.indexes[n].building, building.def.indexes[n].building);
    }

    #[test]
    fn rebuild_takes_secondary_index_changes_alone() {
        let (def, cat) = table(T);
        let eligible = |sql: &str| {
            let ch = alter(&def, &cat, sql).unwrap();
            rebuild_eligible(&def, &ch)
        };
        let layout = |sql: &str| rebuild_changes_layout(&alter(&def, &cat, sql).unwrap());
        assert!(eligible("ALTER TABLE t ADD INDEX ks (s)"));
        assert!(eligible("CREATE INDEX kd ON t (d, a)"));
        assert!(eligible("ALTER TABLE t ADD UNIQUE INDEX us (s, id)"));
        assert!(eligible("ALTER TABLE t DROP INDEX ka, ADD INDEX ka (a, s)"));
        assert!(eligible("ALTER TABLE t ADD INDEX ks (s), RENAME INDEX ka TO ka2"));
        assert!(!layout("ALTER TABLE t ADD INDEX ks (s)"));
        // Nothing to build: a drop or a rename alone stays in place.
        assert!(!eligible("ALTER TABLE t DROP INDEX ka"));
        assert!(!eligible("ALTER TABLE t RENAME INDEX ka TO ka2"));
        assert!(!eligible("ALTER TABLE t ADD COLUMN n INT DEFAULT 3"));
        // Columns the same statement adds, renames or drops: the copy takes
        // the new format, and pads rows when an index needs a fill.
        for sql in [
            "ALTER TABLE t ADD COLUMN n INT, ADD INDEX kn (n)",
            "ALTER TABLE t ADD COLUMN n INT DEFAULT 3, ADD INDEX kn (n)",
            "ALTER TABLE t ADD COLUMN n INT NOT NULL, ADD UNIQUE INDEX un (n, id)",
            "ALTER TABLE t ADD COLUMN n VARCHAR(10) DEFAULT 'x', ADD INDEX kn (n, a)",
            "ALTER TABLE t RENAME COLUMN s TO s2, ADD INDEX ks (s2)",
            "ALTER TABLE t DROP COLUMN d, ADD INDEX ks (s)",
        ] {
            assert!(eligible(sql), "{}", sql);
            assert!(layout(sql), "{}", sql);
        }
        let padded = alter(&def, &cat, "ALTER TABLE t ADD COLUMN n INT DEFAULT 3, ADD INDEX kn (n)").unwrap();
        assert!(padded.materialize && padded.pad.is_some() && padded.format.is_some());
        let short = alter(&def, &cat, "ALTER TABLE t ADD COLUMN n INT, ADD INDEX kn (n)").unwrap();
        assert!(!short.materialize && short.pad.is_none() && short.format.is_some());
        // Rows checked against a narrower type or NOT NULL: the copy checks
        // each row it writes (issue #184), when the storage knows to.
        let checks = |sql: &str| rebuild_checks_rows(&alter(&def, &cat, sql).unwrap());
        for sql in [
            "ALTER TABLE t MODIFY a SMALLINT, ADD INDEX ks (s)",
            "ALTER TABLE t MODIFY s VARCHAR(5), ADD COLUMN n INT DEFAULT 3, ADD INDEX kn (n)",
            "ALTER TABLE t MODIFY a INT NOT NULL, ADD INDEX ka2 (a, s)",
        ] {
            assert!(eligible(sql), "{}", sql);
            assert!(checks(sql), "{}", sql);
        }
        assert!(!checks("ALTER TABLE t ADD INDEX ks (s)"));
        assert!(!checks("ALTER TABLE t ADD COLUMN n INT DEFAULT 3, ADD INDEX kn (n)"));
        // Nothing to build: rows checked alone stay in place.
        assert!(!eligible("ALTER TABLE t MODIFY a SMALLINT"));
        // A renamed table, and a unique key needing a global index.
        assert!(!eligible("ALTER TABLE t ADD INDEX ks (s), RENAME TO t2"));
        let (sharded, scat) = table(
            "CREATE TABLE t (tenant_id INT, id INT, a INT, PRIMARY KEY (tenant_id, id)) PARTITION BY KEY(tenant_id)",
        );
        let global = alter(&sharded, &scat, "ALTER TABLE t ADD UNIQUE INDEX ua (a)").unwrap();
        assert!(!rebuild_eligible(&sharded, &global));
        let local = alter(&sharded, &scat, "ALTER TABLE t ADD INDEX ka (a)").unwrap();
        assert!(rebuild_eligible(&sharded, &local));
        // An engine change is a copy of its own.
        let engine = alter(&def, &cat, "ALTER TABLE t ENGINE = vinyl").unwrap();
        assert!(!rebuild_eligible(&def, &engine));
        // A rebuild's job arguments bind the original identity in its engine.
        let mut ch = alter(&def, &cat, "ALTER TABLE t ADD INDEX ks (s)").unwrap();
        ch.engine_change = Some(EngineChange {
            old_id: def.space_id,
            old_ver: def.ver,
            old_engine: def.engine,
            old_name: def.space_name(),
            rebuild: true,
            durable: false,
        });
        let args = lua_args(&ch, 7, 5.0);
        let mut rd = Rd::new(&args);
        let mut seen = false;
        for _ in 0..rd.map().unwrap() {
            if rd.string().unwrap() == "engine_change" {
                let n = rd.map().unwrap();
                assert_eq!(n, 5);
                let mut fields = std::collections::HashMap::new();
                for _ in 0..n {
                    let k = rd.string().unwrap().to_string();
                    fields.insert(k, rd.raw_value().unwrap().to_vec());
                }
                assert_eq!(fields["rebuild"], vec![0xc3]);
                assert_eq!(fields["old_engine"][1..], *def.engine.name().as_bytes());
                assert!(!fields.contains_key("durable"));
                seen = true;
            } else {
                rd.skip().unwrap();
            }
        }
        assert!(seen && rd.buf.is_empty());
        // The coordinator sends such a job's BUILD again before PREPARE (a
        // catch-up round, issue #184), and no other's.
        assert!(job_copies(&args).unwrap());
        assert!(job_copies(&lua_args(&engine, 7, 5.0)).unwrap());
        let in_place = alter(&def, &cat, "ALTER TABLE t ADD INDEX ks (s)").unwrap();
        assert!(!job_copies(&lua_args(&in_place, 7, 5.0)).unwrap());
        // A copy that keeps its capture durably says so (issue #184).
        ch.engine_change.as_mut().unwrap().durable = true;
        let args = lua_args(&ch, 7, 5.0);
        let mut rd = Rd::new(&args);
        let mut durable = None;
        for _ in 0..rd.map().unwrap() {
            if rd.string().unwrap() == "engine_change" {
                let n = rd.map().unwrap();
                assert_eq!(n, 6);
                for _ in 0..n {
                    let k = rd.string().unwrap().to_string();
                    let v = rd.raw_value().unwrap().to_vec();
                    if k == "durable" {
                        durable = Some(v);
                    }
                }
            } else {
                rd.skip().unwrap();
            }
        }
        assert_eq!(durable, Some(vec![0xc3]));
        // A rebuild that changes the stored layout sends its format and pad.
        let mut ch = alter(&def, &cat, "ALTER TABLE t ADD COLUMN n INT DEFAULT 3, ADD INDEX kn (n)").unwrap();
        ch.engine_change = Some(EngineChange {
            old_id: def.space_id,
            old_ver: def.ver,
            old_engine: def.engine,
            old_name: def.space_name(),
            rebuild: true,
            durable: false,
        });
        let args = lua_args(&ch, 7, 5.0);
        let mut rd = Rd::new(&args);
        let mut fields = std::collections::HashMap::new();
        for _ in 0..rd.map().unwrap() {
            let k = rd.string().unwrap().to_string();
            fields.insert(k, rd.raw_value().unwrap().to_vec());
        }
        let (base, values) = ch.pad.as_ref().unwrap();
        let mut pad = Rd::new(&fields["pad"]);
        assert_eq!(pad.array().unwrap(), values.len() + 1);
        assert_eq!(pad.value().unwrap().as_i64(), Some(*base as i64));
        assert_eq!(
            Rd::new(&fields["format"]).array().unwrap(),
            ch.format.as_ref().unwrap().len()
        );
        assert_eq!(fields["materialize"], vec![0xc3]);
        let mut rebuild = None;
        let mut rd = Rd::new(&fields["engine_change"]);
        for _ in 0..rd.map().unwrap() {
            if rd.string().unwrap() == "rebuild" {
                rebuild = Some(rd.string().unwrap().to_string());
            } else {
                rd.skip().unwrap();
            }
        }
        assert_eq!(rebuild.as_deref(), Some("layout"));
        // An index on a column an earlier ALTER added with a default pads
        // rows without changing the format: a change of layout all the same.
        let added = alter(&def, &cat, "ALTER TABLE t ADD COLUMN n INT DEFAULT 3")
            .unwrap()
            .def;
        let ch = alter(&added, &cat, "ALTER TABLE t ADD INDEX kn (n)").unwrap();
        assert!(ch.format.is_none() && ch.pad.is_some() && ch.materialize);
        assert!(rebuild_eligible(&added, &ch) && rebuild_changes_layout(&ch));
    }

    #[test]
    fn rebuild_keeps_global_indexes() {
        // A unique key without the shard key has a global index (issue #14).
        let (mut def, _) = table(
            "CREATE TABLE t (tenant_id INT, id INT, a INT, b INT, PRIMARY KEY (tenant_id, id), \
             UNIQUE KEY ua (a)) PARTITION BY KEY(tenant_id)",
        );
        let ua = def.indexes.iter().position(|i| i.name == "ua").unwrap();
        def.indexes[ua].global = Some(600);
        def.resolve_globals().unwrap();
        let cat = Catalog::new(1, vec!["db".into(), "other".into()], vec![def.clone()]);
        let eligible = |sql: &str| rebuild_eligible(&def, &alter(&def, &cat, sql).unwrap());
        // New local indexes, also with new columns, keep it as it is.
        assert!(eligible("ALTER TABLE t ADD INDEX kb (b)"));
        assert!(eligible("ALTER TABLE t ADD UNIQUE INDEX ub (tenant_id, b)"));
        assert!(eligible("ALTER TABLE t ADD COLUMN n INT DEFAULT 3, ADD INDEX kn (n)"));
        // One that makes a global index builds in place, also when it drops
        // one with the same columns.
        assert!(!eligible("ALTER TABLE t ADD UNIQUE INDEX ub (b)"));
        assert!(!eligible("ALTER TABLE t DROP INDEX ua, ADD UNIQUE INDEX ua2 (a)"));
        // One that drops one copies too (issue #184): publication retires
        // it. The rebuild still says the table has global indexes.
        assert!(eligible("ALTER TABLE t DROP INDEX ua, ADD INDEX kb (b)"));
        let dropped = alter(&def, &cat, "ALTER TABLE t DROP INDEX ua, ADD INDEX kb (b)").unwrap();
        assert!(dropped.uix_drop == [600] && rebuild_drops_indexes(&dropped) && rebuild_keeps_globals(&dropped));
        assert!(!dropped.def.indexes.iter().any(|i| i.global.is_some()));
        // Nothing to build: a drop alone stays in place.
        assert!(!eligible("ALTER TABLE t DROP INDEX ua"));
        // A drop of a space that is not the table's global index is refused.
        let mut stray = alter(&def, &cat, "ALTER TABLE t DROP INDEX ua, ADD INDEX kb (b)").unwrap();
        stray.uix_drop = vec![601];
        assert!(!rebuild_eligible(&def, &stray));
        // As Cluster::alter_table sets the rebuild up: a new space, whose
        // definition names the kept global index after it.
        let mut ch = alter(&def, &cat, "ALTER TABLE t ADD INDEX kb (b)").unwrap();
        assert!(rebuild_keeps_globals(&ch) && !rebuild_changes_layout(&ch) && !rebuild_drops_indexes(&ch));
        let (plain, plain_cat) = table(T);
        assert!(!rebuild_keeps_globals(
            &alter(&plain, &plain_cat, "ALTER TABLE t ADD INDEX ks (s)").unwrap()
        ));
        ch.engine_change = Some(EngineChange {
            old_id: def.space_id,
            old_ver: def.ver,
            old_engine: def.engine,
            old_name: def.space_name(),
            rebuild: true,
            durable: false,
        });
        ch.def.space_id = 700;
        ch.def.resolve_globals().unwrap();
        let args = lua_args(&ch, 7, 5.0);
        let mut rd = Rd::new(&args);
        let mut fields = std::collections::HashMap::new();
        for _ in 0..rd.map().unwrap() {
            let k = rd.string().unwrap().to_string();
            fields.insert(k, rd.raw_value().unwrap().to_vec());
        }
        let mut rebuild = None;
        let mut rd = Rd::new(&fields["engine_change"]);
        for _ in 0..rd.map().unwrap() {
            if rd.string().unwrap() == "rebuild" {
                rebuild = Some(rd.string().unwrap().to_string());
            } else {
                rd.skip().unwrap();
            }
        }
        assert_eq!(rebuild.as_deref(), Some("aux"));
        assert_eq!(Rd::new(&fields["uix"]).array().unwrap(), 0);
        assert_eq!(Rd::new(&fields["uix_drop"]).array().unwrap(), 0);
        let mut kept = Vec::new();
        let mut rd = Rd::new(&fields["def"]);
        for _ in 0..rd.map().unwrap() {
            if rd.string().unwrap() != "uix" {
                rd.skip().unwrap();
                continue;
            }
            for _ in 0..rd.array().unwrap() {
                let (mut name, mut id) = (String::new(), None);
                for _ in 0..rd.map().unwrap() {
                    match rd.string().unwrap().as_str() {
                        "name" => name = rd.string().unwrap().to_string(),
                        "id" => id = rd.value().unwrap().as_i64(),
                        _ => rd.skip().unwrap(),
                    }
                }
                kept.push((name, id));
            }
        }
        let n = def.indexes[ua].id;
        assert_eq!(kept, vec![(format!("_tarn_uix_700_{n}"), Some(600))]);
        // A rebuild that drops it says so, and that the table has a global
        // index, which the storage checks it keeps or drops.
        let mut ch = dropped;
        ch.engine_change = Some(EngineChange {
            old_id: def.space_id,
            old_ver: def.ver,
            old_engine: def.engine,
            old_name: def.space_name(),
            rebuild: true,
            durable: false,
        });
        ch.def.space_id = 700;
        ch.def.resolve_globals().unwrap();
        let args = lua_args(&ch, 7, 5.0);
        let mut rd = Rd::new(&args);
        let mut fields = std::collections::HashMap::new();
        for _ in 0..rd.map().unwrap() {
            let k = rd.string().unwrap().to_string();
            fields.insert(k, rd.raw_value().unwrap().to_vec());
        }
        let mut rebuild = None;
        let mut rd = Rd::new(&fields["engine_change"]);
        for _ in 0..rd.map().unwrap() {
            if rd.string().unwrap() == "rebuild" {
                rebuild = Some(rd.string().unwrap().to_string());
            } else {
                rd.skip().unwrap();
            }
        }
        assert_eq!(rebuild.as_deref(), Some("aux"));
        let mut drops = Rd::new(&fields["uix_drop"]);
        assert_eq!(drops.array().unwrap(), 1);
        assert_eq!(drops.value().unwrap().as_i64(), Some(600));
        assert_eq!(Rd::new(&fields["uix"]).array().unwrap(), 0);

        // Making the key's column nullable gives its index a new id, so the
        // kept space is named after that id: the storage renames it so.
        let (mut nn, _) = table(
            "CREATE TABLE t (tenant_id INT, id INT, a INT NOT NULL, PRIMARY KEY (tenant_id, id), \
             UNIQUE KEY ua (a)) PARTITION BY KEY(tenant_id)",
        );
        let ua = nn.indexes.iter().position(|i| i.name == "ua").unwrap();
        nn.indexes[ua].global = Some(600);
        nn.resolve_globals().unwrap();
        let ncat = Catalog::new(1, vec!["db".into(), "other".into()], vec![nn.clone()]);
        let mut ch = alter(&nn, &ncat, "ALTER TABLE t MODIFY a INT NULL").unwrap();
        assert!(rebuild_eligible(&nn, &ch));
        let renumbered = ch.def.indexes.iter().find(|i| i.name == "ua").unwrap();
        assert!(renumbered.id != nn.indexes[ua].id && renumbered.global == Some(600));
        let id = renumbered.id;
        ch.def.space_id = 700;
        ch.def.resolve_globals().unwrap();
        assert_eq!(ch.def.globals[0].table.name, format!("_tarn_uix_700_{id}"));
    }

    #[test]
    fn rebuild_keeps_lazy_indexes() {
        let (mut def, _) = table(
            "CREATE TABLE t (tenant_id INT, id INT, k INT NOT NULL, b INT, PRIMARY KEY (tenant_id, id), \
             KEY by_k (k, id) COMMENT 'tarn:lazy') PARTITION BY KEY(tenant_id)",
        );
        let by_k = def.indexes.iter().position(|i| i.name == "by_k").unwrap();
        let l = def.indexes[by_k].lazy.as_mut().expect("a lazy index");
        (l.space_id, l.ready) = (700, true);
        assert!(def.has_lazy());
        let cat = Catalog::new(1, vec!["db".into(), "other".into()], vec![def.clone()]);
        let eligible = |sql: &str| rebuild_eligible(&def, &alter(&def, &cat, sql).unwrap());
        // New local indexes, also with a new column, keep its generation.
        assert!(eligible("ALTER TABLE t ADD INDEX kb (b)"));
        assert!(eligible("ALTER TABLE t ADD COLUMN n INT DEFAULT 3, ADD INDEX kn (n)"));
        // A change that drops a lazy index copies too (issue #184), and so
        // does one that renews it: an index on a column made nullable gets a
        // new id, and so a new generation, which the copy makes, the old one
        // dropped. Publication retires the dropped generation.
        assert!(eligible("ALTER TABLE t DROP INDEX by_k, ADD INDEX kb (b)"));
        assert!(eligible("ALTER TABLE t MODIFY k INT NULL, ADD INDEX kb (b)"));
        let dropped = alter(&def, &cat, "ALTER TABLE t DROP INDEX by_k, ADD INDEX kb (b)").unwrap();
        assert!(dropped.lazy_drop == [700] && rebuild_drops_indexes(&dropped) && rebuild_keeps_lazy(&dropped));
        assert!(!dropped.def.has_lazy() && !rebuild_makes_lazy(&dropped));
        let renewed = alter(&def, &cat, "ALTER TABLE t MODIFY k INT NULL, ADD INDEX kb (b)").unwrap();
        assert!(renewed.lazy_drop == [700] && rebuild_makes_lazy(&renewed));
        // Nothing to build: a drop alone stays in place.
        assert!(!eligible("ALTER TABLE t DROP INDEX by_k"));
        // A drop of a generation that is not the table's is refused.
        let mut stray = alter(&def, &cat, "ALTER TABLE t DROP INDEX by_k, ADD INDEX kb (b)").unwrap();
        stray.lazy_drop = vec![701];
        assert!(!rebuild_eligible(&def, &stray));
        // As Cluster::alter_table sets the rebuild up: the descriptor keeps
        // the generation, naming the new space.
        let mut ch = alter(&def, &cat, "ALTER TABLE t ADD INDEX kb (b)").unwrap();
        let (plain, plain_cat) = table(T);
        let plain_ch = alter(&plain, &plain_cat, "ALTER TABLE t ADD INDEX ks (s)").unwrap();
        assert!(rebuild_keeps_lazy(&ch) && !rebuild_keeps_globals(&ch) && !rebuild_keeps_lazy(&plain_ch));
        assert!(!rebuild_drops_indexes(&ch));
        ch.engine_change = Some(EngineChange {
            old_id: def.space_id,
            old_ver: def.ver,
            old_engine: def.engine,
            old_name: def.space_name(),
            rebuild: true,
            durable: false,
        });
        ch.def.space_id = 800;
        ch.def.resolve_globals().unwrap();
        assert!(ch.lazy_drop.is_empty() && ch.lazy_new.is_empty());
        let specs = ch.def.lazy_specs();
        assert!(specs.len() == 1 && specs[0].gen == 700 && specs[0].base_space_id == 800);
        let args = lua_args(&ch, 7, 5.0);
        let mut rd = Rd::new(&args);
        let mut fields = std::collections::HashMap::new();
        for _ in 0..rd.map().unwrap() {
            let k = rd.string().unwrap().to_string();
            fields.insert(k, rd.raw_value().unwrap().to_vec());
        }
        let mut rebuild = None;
        let mut rd = Rd::new(&fields["engine_change"]);
        for _ in 0..rd.map().unwrap() {
            if rd.string().unwrap() == "rebuild" {
                rebuild = Some(rd.string().unwrap().to_string());
            } else {
                rd.skip().unwrap();
            }
        }
        assert_eq!(rebuild.as_deref(), Some("lazy"));
        assert!(!fields.contains_key("lazy_drop"));
        // Its job waits for no lazy bootstrap: the generation is built. In
        // place, the same change's job names the generation as one to wait
        // for, which the storage finds ready.
        assert!(!crate::lazy::job_lazy(&args).unwrap().0);
        let in_place = alter(&def, &cat, "ALTER TABLE t ADD INDEX kb (b)").unwrap();
        assert!(crate::lazy::job_lazy(&lua_args(&in_place, 7, 5.0)).unwrap().0);
        // A rebuild that drops the table's only lazy index says that the
        // table has one, and names the generation it drops.
        let mut ch = dropped;
        ch.engine_change = Some(EngineChange {
            old_id: def.space_id,
            old_ver: def.ver,
            old_engine: def.engine,
            old_name: def.space_name(),
            rebuild: true,
            durable: false,
        });
        ch.def.space_id = 800;
        ch.def.resolve_globals().unwrap();
        let args = lua_args(&ch, 7, 5.0);
        let mut rd = Rd::new(&args);
        let mut fields = std::collections::HashMap::new();
        for _ in 0..rd.map().unwrap() {
            let k = rd.string().unwrap().to_string();
            fields.insert(k, rd.raw_value().unwrap().to_vec());
        }
        let mut rebuild = None;
        let mut rd = Rd::new(&fields["engine_change"]);
        for _ in 0..rd.map().unwrap() {
            if rd.string().unwrap() == "rebuild" {
                rebuild = Some(rd.string().unwrap().to_string());
            } else {
                rd.skip().unwrap();
            }
        }
        assert_eq!(rebuild.as_deref(), Some("lazy"));
        let mut drops = Rd::new(&fields["lazy_drop"]);
        assert_eq!(drops.array().unwrap(), 1);
        assert_eq!(drops.value().unwrap().as_i64(), Some(700));
        assert!(!fields.contains_key("lazy_made"));
    }

    #[test]
    fn rebuild_makes_lazy_indexes() {
        let (mut def, _) = table(
            "CREATE TABLE t (tenant_id INT, id INT, k INT NOT NULL, b INT, s VARCHAR(8) COLLATE utf8mb4_unicode_ci, \
             PRIMARY KEY (tenant_id, id), KEY by_k (k, id) COMMENT 'tarn:lazy') PARTITION BY KEY(tenant_id)",
        );
        let by_k = def.indexes.iter().position(|i| i.name == "by_k").unwrap();
        let l = def.indexes[by_k].lazy.as_mut().expect("a lazy index");
        (l.space_id, l.ready) = (700, true);
        let cat = Catalog::new(1, vec!["db".into(), "other".into()], vec![def.clone()]);
        let eligible = |d: &TableDef, c: &Catalog, sql: &str| rebuild_eligible(d, &alter(d, c, sql).unwrap());
        // A new lazy index on columns the table has, stored as they were,
        // is built online against the old space before the copy, which
        // keeps the existing one. So is the first lazy index of a table.
        assert!(eligible(
            &def,
            &cat,
            "ALTER TABLE t ADD INDEX kb (b, id) COMMENT 'tarn:lazy'"
        ));
        assert!(eligible(
            &def,
            &cat,
            "ALTER TABLE t ADD INDEX kl (b) COMMENT 'tarn:lazy', ADD INDEX kb (b, k)"
        ));
        let (plain, plain_cat) = table(T);
        assert!(eligible(
            &plain,
            &plain_cat,
            "ALTER TABLE t ADD INDEX la (a) COMMENT 'tarn:lazy'"
        ));
        // Not on a column the change adds, or retypes.
        assert!(!eligible(
            &def,
            &cat,
            "ALTER TABLE t ADD COLUMN n INT DEFAULT 3, ADD INDEX kn (n) COMMENT 'tarn:lazy'"
        ));
        assert!(!eligible(
            &def,
            &cat,
            "ALTER TABLE t MODIFY b BIGINT, ADD INDEX kb (b) COMMENT 'tarn:lazy'"
        ));
        // Nor with fields the change materializes: old rows may lack a
        // column an earlier ALTER added with a default, and the build reads
        // them as they are stored. A column added without one reads as NULL.
        let added = alter(&def, &cat, "ALTER TABLE t ADD COLUMN n INT DEFAULT 3")
            .unwrap()
            .def;
        assert!(!eligible(
            &added,
            &cat,
            "ALTER TABLE t ADD INDEX kn (n) COMMENT 'tarn:lazy'"
        ));
        assert!(!eligible(
            &added,
            &cat,
            "ALTER TABLE t ADD INDEX kn (n), ADD INDEX kb (b, id) COMMENT 'tarn:lazy'"
        ));
        assert!(eligible(&added, &cat, "ALTER TABLE t ADD INDEX kn (n)"));
        let added = alter(&def, &cat, "ALTER TABLE t ADD COLUMN n INT DEFAULT NULL")
            .unwrap()
            .def;
        assert!(eligible(
            &added,
            &cat,
            "ALTER TABLE t ADD INDEX kn (n) COMMENT 'tarn:lazy'"
        ));
        // As Cluster::alter_table sets the rebuild up: the new generation is
        // allocated, and the descriptor names it among those made.
        let mut ch = alter(&def, &cat, "ALTER TABLE t ADD INDEX kb (b, id) COMMENT 'tarn:lazy'").unwrap();
        assert!(
            rebuild_makes_lazy(&ch)
                && !rebuild_makes_lazy(&alter(&def, &cat, "ALTER TABLE t ADD INDEX kb (b)").unwrap())
        );
        ch.engine_change = Some(EngineChange {
            old_id: def.space_id,
            old_ver: def.ver,
            old_engine: def.engine,
            old_name: def.space_name(),
            rebuild: true,
            durable: false,
        });
        ch.def.space_id = 800;
        for ix in &mut ch.def.indexes {
            if let Some(l) = ix.lazy.as_mut().filter(|l| l.space_id == 0) {
                (l.space_id, l.ready) = (701, true);
                ch.lazy_new.push(701);
            }
        }
        ch.def.resolve_globals().unwrap();
        let mut gens: Vec<(u32, u32)> = ch.def.lazy_specs().iter().map(|s| (s.gen, s.base_space_id)).collect();
        gens.sort_unstable();
        assert_eq!(gens, vec![(700, 800), (701, 800)]);
        let args = lua_args(&ch, 7, 5.0);
        let mut rd = Rd::new(&args);
        let mut fields = std::collections::HashMap::new();
        for _ in 0..rd.map().unwrap() {
            let k = rd.string().unwrap().to_string();
            fields.insert(k, rd.raw_value().unwrap().to_vec());
        }
        let mut made = Rd::new(&fields["lazy_made"]);
        assert_eq!(made.array().unwrap(), 1);
        assert_eq!(made.value().unwrap().as_i64(), Some(701));
        // Its job builds the made generation, online, before the copy.
        assert!(crate::lazy::job_lazy(&args).unwrap().0 && crate::lazy::job_makes_lazy(&args).unwrap());
        // In place, the same change names no generation as made.
        let mut in_place = alter(&def, &cat, "ALTER TABLE t ADD INDEX kb (b, id) COMMENT 'tarn:lazy'").unwrap();
        in_place.lazy_new.push(701);
        let args = lua_args(&in_place, 7, 5.0);
        assert!(crate::lazy::job_lazy(&args).unwrap().0 && !crate::lazy::job_makes_lazy(&args).unwrap());
    }

    #[test]
    fn indexes_require_stored_non_null_fills() {
        let (def, cat) = table("CREATE TABLE t (id INT PRIMARY KEY, v INT)");
        for spec in ["c INT DEFAULT 3", "c INT NOT NULL", "c VARCHAR(10) DEFAULT ''"] {
            let ch = alter(
                &def,
                &cat,
                &format!("ALTER TABLE t ADD {spec}, ADD INDEX ic (c, id), ADD INDEX ivc (v, c)"),
            )
            .unwrap();
            let c = ch.def.col_index("c").unwrap();
            let present: Vec<_> = ch
                .checks
                .iter()
                .filter(|c| c.kind == CheckKind::IndexFieldPresent)
                .collect();
            assert_eq!(present.len(), 1, "one check even with several indexes: {spec}");
            assert_eq!(present[0].field, ch.def.layout.col_field[c] as u32 + 1);
            assert_eq!(present[0].fill, ch.def.layout.fills[c]);
            assert!(ch.materialize);
            assert_eq!(ch.old_width as usize, def.layout.width);
            let (base, pad) = ch.pad.as_ref().unwrap();
            assert_eq!(pad[ch.def.layout.col_field[c] - base], ch.def.layout.fills[c]);
        }
        // Missing NULL and physically stored NULL have the same native key.
        let ch = alter(&def, &cat, "ALTER TABLE t ADD c INT DEFAULT NULL, ADD INDEX ic (c)").unwrap();
        assert!(ch.checks.is_empty());
        assert!(!ch.materialize && ch.pad.is_none());
        let ch = alter(&def, &cat, "ALTER TABLE t ADD INDEX iv (v)").unwrap();
        assert!(ch.checks.is_empty());
        assert!(!ch.materialize && ch.pad.is_none());

        // A later DEFAULT change does not change what the old short rows hold.
        let added = alter(&def, &cat, "ALTER TABLE t ADD c INT DEFAULT 3").unwrap().def;
        let ch = alter(&added, &cat, "ALTER TABLE t ALTER c SET DEFAULT 9, ADD INDEX ic (c)").unwrap();
        assert_eq!(ch.checks.len(), 1);
        assert_eq!(ch.checks[0].fill, Value::Int(3));
        let (base, pad) = ch.pad.as_ref().unwrap();
        assert_eq!(pad[ch.def.layout.col_field[2] - base], Value::Int(3));
        let added_null = alter(&def, &cat, "ALTER TABLE t ADD c INT DEFAULT NULL").unwrap().def;
        let ch = alter(
            &added_null,
            &cat,
            "ALTER TABLE t ALTER c SET DEFAULT 9, ADD INDEX ic (c)",
        )
        .unwrap();
        assert!(
            ch.checks.is_empty(),
            "a current non-NULL default does not change the frozen NULL fill"
        );
        assert!(!ch.materialize && ch.pad.is_none());

        // Collated indexes store a weight field, not the visible text field.
        let ch = alter(
            &def,
            &cat,
            "ALTER TABLE t ADD c VARCHAR(10) COLLATE utf8mb4_general_ci DEFAULT 'X', ADD UNIQUE INDEX ic (c, id)",
        )
        .unwrap();
        let c = ch.def.col_index("c").unwrap();
        assert_eq!(ch.checks[0].field, ch.def.layout.col_weight[c].unwrap() as u32 + 1);
        let (base, pad) = ch.pad.as_ref().unwrap();
        assert_eq!(
            pad[ch.def.layout.col_weight[c].unwrap() - base],
            ch.def.key_part(c, &Value::str("X"))
        );

        // The storage check is carried to every DDL stage in the persisted job.
        let encoded = lua_args(&ch, 2, 5.0);
        let mut rd = Rd::new(&encoded);
        let count = rmp::decode::read_map_len(&mut rd.buf).unwrap();
        let mut found = false;
        for _ in 0..count {
            if rd.string().unwrap() == "checks" {
                assert_eq!(rd.array().unwrap(), 1);
                assert_eq!(rd.array().unwrap(), 6);
                rd.skip().unwrap();
                assert_eq!(rd.string().unwrap(), "c");
                assert_eq!(rd.string().unwrap(), "indexed");
                for _ in 0..3 {
                    rd.skip().unwrap();
                }
                found = true;
            } else {
                rd.skip().unwrap();
            }
        }
        assert!(found && rd.buf.is_empty());
    }

    #[test]
    fn materialization_pad_preserves_old_dropped_values_and_weights() {
        let (def, cat) = table("CREATE TABLE t (id INT PRIMARY KEY, v INT)");
        let old = alter(
            &def,
            &cat,
            "ALTER TABLE t ADD b INT DEFAULT 3 FIRST, ADD n INT DEFAULT NULL, \
             ADD s VARCHAR(10) COLLATE utf8mb4_unicode_ci DEFAULT 'Á ', ADD INDEX is_s (s, id)",
        )
        .unwrap()
        .def;
        let b = old.col_index("b").unwrap();
        let s = old.col_index("s").unwrap();
        let ch = alter(
            &old,
            &cat,
            "ALTER TABLE t DROP b, DROP s, ADD c INT DEFAULT 5 FIRST, ADD INDEX ic (c, id)",
        )
        .unwrap();
        let (base, pad) = ch.pad.as_ref().unwrap();
        assert!(ch.materialize);
        assert_eq!(ch.old_width as usize, old.layout.width);
        assert_eq!(*base, old.bucket_field() + 1);
        let (_, old_pad) = old.update_pad().unwrap();
        assert_eq!(&pad[..old_pad.len()], old_pad.as_slice());
        assert_eq!(pad[old.layout.col_field[b] - base], Value::Int(3));
        assert_eq!(pad[old.layout.col_field[s] - base], Value::str("Á "));
        assert_eq!(
            pad[old.layout.col_weight[s].unwrap() - base],
            old.key_part(s, &Value::str("Á "))
        );
        let c = ch.def.col_index("c").unwrap();
        assert_eq!(
            ch.def.layout.col_field[c], old.layout.width,
            "new slots are appended after old ones"
        );
        assert_eq!(pad[ch.def.layout.col_field[c] - base], Value::Int(5));
        // Blindly using the proposed layout would expose NULL through the
        // still-public old b and s columns while PREPARE runs.
        let (_, proposed) = ch.def.update_pad().unwrap();
        assert_eq!(proposed[old.layout.col_field[b] - base], Value::Null);
        assert_eq!(proposed[old.layout.col_weight[s].unwrap() - base], Value::Null);
    }

    #[test]
    fn materialization_wire_keeps_old_storage_presence_checks() {
        fn fields(ch: &Change) -> std::collections::HashMap<String, Vec<u8>> {
            let encoded = lua_args(ch, 7, 5.0);
            let mut rd = Rd::new(&encoded);
            let count = rd.map().unwrap();
            let mut out = std::collections::HashMap::new();
            for _ in 0..count {
                out.insert(rd.string().unwrap(), rd.raw_value().unwrap().to_vec());
            }
            assert!(rd.buf.is_empty());
            out
        }
        let (def, cat) = table("CREATE TABLE t (id INT PRIMARY KEY, v INT)");
        let ch = alter(&def, &cat, "ALTER TABLE t ADD c INT DEFAULT 3, ADD INDEX ic (c)").unwrap();
        let f = fields(&ch);
        assert_eq!(f["materialize"], [0xc3]);
        assert_eq!(
            Rd::new(&f["old_width"]).value().unwrap().as_i64(),
            Some(def.layout.width as i64)
        );
        let mut pad = Rd::new(&f["pad"]);
        assert_eq!(pad.array().unwrap(), 2);
        assert_eq!(pad.value().unwrap().as_i64(), Some(def.bucket_field() as i64 + 1));
        assert_eq!(pad.value().unwrap().as_i64(), Some(3));
        assert!(pad.buf.is_empty());
        let mut checks = Rd::new(&f["checks"]);
        assert_eq!(checks.array().unwrap(), 1);
        assert_eq!(checks.array().unwrap(), 6);
        assert_eq!(checks.value().unwrap().as_i64(), Some(def.layout.width as i64 + 1));
        assert_eq!(checks.string().unwrap(), "c");
        assert_eq!(
            checks.string().unwrap(),
            "indexed",
            "old storage must still reject missing fields"
        );
        for _ in 0..3 {
            checks.skip().unwrap();
        }
        assert!(checks.buf.is_empty());

        let ch = alter(&def, &cat, "ALTER TABLE t ADD c INT DEFAULT NULL, ADD INDEX ic (c)").unwrap();
        let f = fields(&ch);
        assert_eq!(f["materialize"], [0xc2]);
        assert_eq!(f["pad"], [0xc0]);
        assert_eq!(Rd::new(&f["checks"]).array().unwrap(), 0);
    }

    #[test]
    fn collations_and_weight_fields() {
        // issue #13: a case-insensitive column in an index has a weight field
        let (def, cat) = table(
            "CREATE TABLE t (id INT PRIMARY KEY, h VARCHAR(10), n VARCHAR(10), b VARCHAR(10) COLLATE utf8mb4_bin, \
             UNIQUE KEY uh (h, id)) DEFAULT CHARSET=utf8mb4",
        );
        assert_eq!(def.collation.as_deref(), Some("utf8mb4_general_ci"));
        assert_eq!(def.columns[1].weight, Some(5)); // after the 4 columns and bucket_id
        assert_eq!(def.columns[2].weight, None); // not in an index
        assert_eq!(def.columns[3].collation.as_deref(), Some("utf8mb4_bin"));
        // an existing case-insensitive column cannot get an index: no weights
        assert_eq!(
            alter(&def, &cat, "ALTER TABLE t ADD INDEX ix (n)").unwrap_err().code,
            1235
        );
        // nor change collation while indexed; unindexed it may
        assert_eq!(
            alter(&def, &cat, "ALTER TABLE t MODIFY h VARCHAR(10) COLLATE utf8mb4_bin")
                .unwrap_err()
                .code,
            1235
        );
        let ch = alter(
            &def,
            &cat,
            "ALTER TABLE t MODIFY n VARCHAR(10) COLLATE utf8mb4_unicode_ci",
        )
        .unwrap();
        assert_eq!(ch.def.columns[2].collation.as_deref(), Some("utf8mb4_unicode_ci"));
        assert!(ch.builds.is_empty());
        // MODIFY without COLLATE takes the table's default: same as h's
        assert!(alter(&def, &cat, "ALTER TABLE t MODIFY h VARCHAR(20)").is_ok());
        // a column added with its index gets a weight field
        let ch = alter(
            &def,
            &cat,
            "ALTER TABLE t ADD COLUMN e VARCHAR(10) DEFAULT 'X', ADD UNIQUE KEY ue (e, id)",
        )
        .unwrap();
        let e = ch.def.col_index("e").unwrap();
        let w = ch.def.columns[e].weight.unwrap() as usize;
        assert_eq!(w, 7); // field 6 is the new column
        let b = ch.builds.iter().find(|b| b.name == "ue").unwrap();
        assert_eq!(b.parts[0], (w as u32 + 1, "varbinary".to_string(), true));
        let (n, pad) = ch.def.update_pad().unwrap();
        assert_eq!(n, 5);
        assert_eq!(pad[w - 5], Value::Bin(ch.def.layout.colls[e].key(b"X")));
        // dropping it drops its weight field too
        let ch2 = apply(
            &ch.def,
            &plan1("ALTER TABLE t DROP COLUMN h", &cat),
            &cat,
            "2026-10-25 12:00:00",
        )
        .unwrap();
        assert!(ch2.def.dropped.contains(&1) && ch2.def.dropped.contains(&5));
        // CONVERT TO changes every text column: refused for an indexed one
        assert_eq!(
            alter(
                &def,
                &cat,
                "ALTER TABLE t CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            )
            .unwrap_err()
            .code,
            1235
        );
        let ch = alter(
            &def,
            &cat,
            "ALTER TABLE t DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci",
        )
        .unwrap();
        assert_eq!(ch.def.collation.as_deref(), Some("utf8mb4_unicode_ci"));
        assert_eq!(ch.def.columns[1].collation.as_deref(), Some("utf8mb4_general_ci"));
    }

    #[test]
    fn parses_mysql_alterations() {
        let (_, cat) = table(T);
        let p = plan1(
            "ALTER TABLE t ADD COLUMN x INT NOT NULL DEFAULT 5 AFTER a, ADD y VARCHAR(3) FIRST, DROP COLUMN s, \
             MODIFY d DECIMAL(8,2), CHANGE a b BIGINT, RENAME COLUMN b TO c, ALTER COLUMN x SET DEFAULT 6, \
             ALTER x DROP DEFAULT, ADD INDEX ix (x) INVISIBLE, ADD UNIQUE KEY ux (id, x), DROP INDEX ka, \
             RENAME INDEX ix TO iy, ALTER INDEX iy VISIBLE, DROP KEY iy, ENGINE=InnoDB, DEFAULT CHARSET=utf8mb4, \
             COMMENT='hi', AUTO_INCREMENT=10, ALGORITHM=INPLACE, LOCK=NONE, DISABLE KEYS, RENAME TO u",
            &cat,
        );
        let kinds: Vec<String> = p
            .ops
            .iter()
            .map(|o| format!("{:?}", o).split([' ', '(', '{']).next().unwrap().to_string())
            .collect();
        assert_eq!(
            kinds,
            [
                "AddColumn",
                "AddColumn",
                "DropColumn",
                "ChangeColumn",
                "ChangeColumn",
                "RenameColumn",
                "AlterDefault",
                "AlterDefault",
                "AddIndex",
                "AddIndex",
                "DropIndex",
                "RenameIndex",
                "IndexVisibility",
                "DropIndex",
                "Ignored",
                "TableCollation",
                "Comment",
                "AutoIncrement",
                "Ignored",
                "Ignored",
                "Ignored",
                "RenameTable"
            ]
        );
        assert!(matches!(&p.ops[0], AlterOp::AddColumn { pos: Some(Pos::After(c)), .. } if c == "a"));
        assert!(matches!(&p.ops[8], AlterOp::AddIndex { invisible: true, .. }));
        let ctx = PlanCtx {
            statistics: None,
            catalog: &cat,
            db: Some("db"),
            params: &[],
            conn_coll: "utf8mb4_bin",
            default_coll: "utf8mb4_bin",
        };
        let v = plan("RENAME TABLE t TO t2, other.x TO db.y", &ctx).unwrap().unwrap();
        assert_eq!((v.len(), v[1].db.as_str(), v[1].table.as_str()), (2, "other", "x"));
        let v = plan("CREATE UNIQUE INDEX u1 ON t (id, a) USING BTREE", &ctx)
            .unwrap()
            .unwrap();
        assert!(matches!(&v[0].ops[0], AlterOp::AddIndex { unique: true, cols, .. } if cols == &["id", "a"]));
        let v = plan("DROP INDEX ka ON db.t ALGORITHM=INPLACE LOCK=NONE", &ctx)
            .unwrap()
            .unwrap();
        assert!(matches!(&v[0].ops[0], AlterOp::DropIndex { name, .. } if name == "ka"));
        assert!(plan("SELECT 1", &ctx).unwrap().is_none());
        assert!(plan("ALTER TABLE t PARTITION BY KEY(a)", &ctx).is_err());
        assert_eq!(plan("ALTER TABLE t FROBNICATE", &ctx).unwrap_err().code, 1064);
    }

    #[test]
    fn index_comments_are_kept() {
        let (def, cat) = table(
            "CREATE TABLE t (id INT, a INT, b INT, PRIMARY KEY (id) COMMENT 'pk', \
             KEY ka (a) USING BTREE COMMENT 'by a', UNIQUE KEY ub (b) COMMENT 'it''s b')",
        );
        let comments = |d: &TableDef| d.indexes.iter().map(|i| i.comment.clone()).collect::<Vec<_>>();
        assert_eq!(comments(&def), ["pk", "by a", "it's b"]);
        let ch = alter(
            &def,
            &cat,
            "ALTER TABLE t ADD INDEX kb (b) COMMENT 'by b', RENAME INDEX ka TO ka2, ADD COLUMN c INT",
        )
        .unwrap();
        assert_eq!(comments(&ch.def), ["pk", "by a", "it's b", "by b"]);
        let ctx = PlanCtx {
            statistics: None,
            catalog: &cat,
            db: Some("db"),
            params: &[],
            conn_coll: "utf8mb4_bin",
            default_coll: "utf8mb4_bin",
        };
        let v = plan(
            "CREATE INDEX kab ON t (a, b) USING BTREE COMMENT 'pair' ALGORITHM=INPLACE",
            &ctx,
        )
        .unwrap()
        .unwrap();
        assert!(matches!(&v[0].ops[0], AlterOp::AddIndex { comment, .. } if comment == "pair"));
    }

    #[test]
    fn create_index_comment_decodes_mysql_string_escapes() {
        let (def, cat) = table(T);
        for (escaped, expected) in [
            (r"\b", "\u{8}"),
            (r"\r", "\r"),
            (r"\Z", "\u{1a}"),
            (r"\0\n\t", "\0\n\t"),
            (r"\\", "\\"),
            (r#"\'\""#, "'\""),
            (r"\%\_", r"\%\_"),
            (r"\q\B\z\a\f", "qBzaf"),
            ("雪\\r☃\\Z", "雪\r☃\u{1a}"),
        ] {
            for quote in ['\'', '"'] {
                let sql = format!("CREATE INDEX kec ON t (a) COMMENT {quote}{escaped}{quote}");
                let ch = alter(&def, &cat, &sql).unwrap();
                assert_eq!(ch.def.indexes.last().unwrap().comment, expected, "{sql}");
            }
        }
    }

    #[test]
    fn quoted_lexer_keeps_backtick_escapes_and_utf8() {
        let tokens = lex(r#"`雪\b\r\Z\%\_``末` '雪''☃' "雪""☃""#).unwrap();
        assert_eq!(tokens.len(), 3);
        assert!(matches!(&tokens[0].t, T::Quoted(s) if s == r"雪\b\r\Z\%\_`末"));
        assert!(matches!(&tokens[1].t, T::Str(s) if s == "雪'☃"));
        assert!(matches!(&tokens[2].t, T::Str(s) if s == "雪\"☃"));
    }

    #[test]
    fn floats_stored_as_integers_read_as_doubles() {
        // (the storage procedures store a FLOAT or DOUBLE that holds an
        // integer as that integer)
        let (def, cat) = table("CREATE TABLE f (id INT PRIMARY KEY, d DOUBLE, g FLOAT, n INT)");
        let mut m = Mp::new();
        m.arr(5).int(1).uint(8).int(-3).int(4).uint(7);
        let row = Rd::new(&m.0).row_of(&def).unwrap();
        assert_eq!(row[1..3], [Value::Float(8.0), Value::Float(-3.0)]);
        assert_eq!(row[3], Rd::new(&[0x04]).value().unwrap());
        // ... also after an ALTER TABLE, and in the fill of a row from
        // before it
        let ch = alter(&def, &cat, "ALTER TABLE f ADD COLUMN e DOUBLE DEFAULT 2 AFTER id").unwrap();
        let mut m = Mp::new();
        m.arr(5).int(1).uint(8).nil().int(4).uint(7);
        let row = Rd::new(&m.0).row_of(&ch.def).unwrap();
        assert_eq!(row[1..4], [Value::Float(2.0), Value::Float(8.0), Value::Null]);
    }

    #[test]
    fn added_columns_live_after_bucket_id() {
        let (def, cat) = table(T);
        assert!(def.layout.identity);
        let ch = alter(
            &def,
            &cat,
            "ALTER TABLE t ADD COLUMN x INT NOT NULL AFTER id, ADD z VARCHAR(4) DEFAULT 'zz'",
        )
        .unwrap();
        let nd = &ch.def;
        assert_eq!(
            nd.columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
            ["id", "x", "a", "s", "d", "z"]
        );
        assert!(!nd.layout.identity);
        assert_eq!((nd.layout.bucket, nd.layout.width), (4, 7));
        assert_eq!(nd.layout.col_field, [0, 5, 1, 2, 3, 6]);
        // existing rows: x = 0 (NOT NULL without a default), z = 'zz'
        assert_eq!(nd.layout.fills[1], Value::Int(0));
        assert_eq!(nd.layout.fills[5], Value::str("zz"));
        assert_eq!(nd.indexes[1].cols, [2]); // ka follows column a to position 2
        let f = ch.format.unwrap();
        assert_eq!(f[5], ("x".into(), "integer".into(), true));
        assert_eq!(f[4].0, "bucket_id");
        assert!(ch.builds.is_empty() && ch.drop.is_empty() && ch.checks.is_empty());
        // a row written before the ALTER (5 fields) and one written after
        let mut m = Mp::new();
        m.arr(5).int(7).int(1).str("s").nil().uint(99);
        let old = Rd::new(&m.0).row_of(nd).unwrap();
        assert_eq!(
            old,
            [
                Value::Int(7),
                Value::Int(0),
                Value::Int(1),
                Value::str("s"),
                Value::Null,
                Value::str("zz")
            ]
        );
        let row = vec![
            Value::Int(8),
            Value::Int(3),
            Value::Int(2),
            Value::Null,
            Value::Null,
            Value::str("q"),
        ];
        let mut m = Mp::new();
        nd.encode_tuple(&row, 42, &mut m);
        assert_eq!(Rd::new(&m.0).row_of(nd).unwrap(), row);
        assert_eq!(nd.update_pad(), Some((5, vec![Value::Int(0), Value::str("zz")])));
    }

    #[test]
    fn dropped_columns_keep_their_field() {
        let (def, cat) = table(T);
        let ch = alter(&def, &cat, "ALTER TABLE t DROP COLUMN a").unwrap();
        let nd = &ch.def;
        assert_eq!(nd.dropped, [1]);
        assert_eq!(nd.layout.col_field, [0, 2, 3]);
        assert_eq!(ch.format.unwrap()[1], ("_dropped_1".into(), "any".into(), true));
        // ka lost its only column
        assert_eq!(ch.drop, [1]);
        assert_eq!(nd.indexes.len(), 1);
        assert_eq!(
            alter(&def, &cat, "ALTER TABLE t DROP COLUMN id").unwrap_err().code,
            1235
        );
        assert_eq!(
            alter(&def, &cat, "ALTER TABLE t DROP COLUMN nope").unwrap_err().code,
            1091
        );
        assert!(alter(&def, &cat, "ALTER TABLE t DROP COLUMN IF EXISTS nope").is_ok());
    }

    #[test]
    fn type_changes_are_checked_or_refused() {
        let (def, cat) = table(T);
        let ch = alter(
            &def,
            &cat,
            "ALTER TABLE t MODIFY a SMALLINT NOT NULL, MODIFY s VARCHAR(4), MODIFY d DECIMAL(5,2)",
        )
        .unwrap();
        let kinds: Vec<&CheckKind> = ch.checks.iter().map(|c| &c.kind).collect();
        assert_eq!(
            kinds,
            [
                &CheckKind::Range(-32768, 32767),
                &CheckKind::NotNull,
                &CheckKind::Chars(4),
                &CheckKind::Digits(3)
            ]
        );
        assert_eq!(ch.checks[0].field, 2);
        assert!(ch.format.is_none(), "no field changes its stored type");
        // widening needs no check
        let ch = alter(
            &def,
            &cat,
            "ALTER TABLE t MODIFY a BIGINT, MODIFY s TEXT, MODIFY d DECIMAL(9,2)",
        )
        .unwrap();
        assert!(ch.checks.is_empty());
        // signed -> unsigned: a range check and a stricter format
        let ch = alter(&def, &cat, "ALTER TABLE t MODIFY a INT UNSIGNED").unwrap();
        assert_eq!(ch.checks[0].kind, CheckKind::Range(0, 4294967295));
        assert_eq!(ch.format.unwrap()[1].1, "unsigned");
        assert!(ch.builds.is_empty(), "integer index parts accept unsigned values");
        for sql in [
            "ALTER TABLE t MODIFY a VARCHAR(10)",
            "ALTER TABLE t MODIFY s INT",
            "ALTER TABLE t MODIFY d DECIMAL(6,3)",
            "ALTER TABLE t MODIFY id BIGINT UNSIGNED NOT NULL",
            "ALTER TABLE t ADD PRIMARY KEY (a)",
        ] {
            assert_eq!(alter(&def, &cat, sql).unwrap_err().code, 1235, "{}", sql);
        }
        // a nullable index column made NOT NULL keeps its index; NULL again
        // after NOT NULL rebuilds nothing (the field stays nullable)
        let ch = alter(&def, &cat, "ALTER TABLE t MODIFY a INT NOT NULL").unwrap();
        assert!(ch.builds.is_empty() && ch.drop.is_empty());
    }

    #[test]
    fn indexes_and_names() {
        let (def, cat) = table(T);
        let ch = alter(
            &def,
            &cat,
            "ALTER TABLE t ADD UNIQUE KEY us (id, s), ADD INDEX (a), RENAME INDEX ka TO kb",
        )
        .unwrap();
        // ids past the bucket_id index (2)
        assert_eq!(
            ch.builds.iter().map(|b| (b.id, b.name.as_str())).collect::<Vec<_>>(),
            [(3, "us"), (4, "a")]
        );
        assert_eq!(
            ch.builds[0].parts,
            [(1, "integer".into(), false), (3, "string".into(), true)]
        );
        assert_eq!(ch.renames, [(1, "kb".to_string())]);
        // a unique key without the shard key gets a global index (#88):
        // its space id comes from the cluster when the change runs
        let ch = alter(&def, &cat, "ALTER TABLE t ADD UNIQUE KEY us (s)").unwrap();
        let us = ch.def.index_by_name("us").unwrap();
        assert_eq!(us.global, Some(0));
        assert!(ch.uix_drop.is_empty());
        assert_eq!(
            alter(&def, &cat, "ALTER TABLE t ADD INDEX ka (s)").unwrap_err().code,
            1061
        );
        assert_eq!(
            alter(&def, &cat, "ALTER TABLE t ADD INDEX bucket_id (s)")
                .unwrap_err()
                .code,
            1280
        );
        assert_eq!(
            alter(&def, &cat, "ALTER TABLE t DROP INDEX PRIMARY").unwrap_err().code,
            1235
        );
        assert_eq!(
            alter(
                &def,
                &cat,
                "ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (s) REFERENCES other.p (x)"
            )
            .unwrap_err()
            .code,
            1235
        );
        let ch = alter(&def, &cat, "ALTER TABLE t RENAME TO other.t9").unwrap();
        assert_eq!(ch.rename.as_deref(), Some("other.t9"));
        assert_eq!(
            alter(&def, &cat, "ALTER TABLE t RENAME TO nodb.t9").unwrap_err().code,
            1049
        );
        let ch = alter(&def, &cat, "ALTER TABLE t ENGINE=InnoDB, DISABLE KEYS").unwrap();
        assert!(!ch.changed);
        // A real move retains the source identity for the copy protocol.
        assert!(!alter(&def, &cat, "ALTER TABLE t ENGINE = memtx").unwrap().changed);
        let moved = alter(&def, &cat, "ALTER TABLE t ENGINE=vinyl").unwrap();
        assert!(moved.changed);
        assert_eq!(moved.def.engine, catalog::Engine::Vinyl);
        let source = moved.engine_change.unwrap();
        assert_eq!((source.old_id, source.old_ver), (def.space_id, def.ver));
        assert_eq!(source.old_name, def.space_name());
        assert_eq!(source.old_engine, def.engine);
        assert_eq!(
            alter(&def, &cat, "ALTER TABLE t ENGINE=vinyl, ADD COLUMN extra INT")
                .unwrap_err()
                .code,
            1235,
        );
        let ctx = PlanCtx {
            statistics: None,
            catalog: &cat,
            db: Some("db"),
            params: &[],
            conn_coll: "utf8mb4_bin",
            default_coll: "utf8mb4_bin",
        };
        assert_eq!(plan("ALTER TABLE t ENGINE=Falcon", &ctx).unwrap_err().code, 1286);
    }

    #[test]
    fn lua_args_encode() {
        let (def, cat) = table(T);
        let mut ch = alter(&def, &cat, "ALTER TABLE t MODIFY a TINYINT NOT NULL, ADD INDEX ix (s)").unwrap();
        ch.def.ver = 9;
        let a = lua_args(&ch, 9, 5.0);
        let mut rd = Rd::new(&a);
        let v = tarn_iproto::mp::decode_value(&mut rd.buf);
        assert!(v.is_ok());
        assert!(rd.buf.is_empty(), "one msgpack map");
    }
}
