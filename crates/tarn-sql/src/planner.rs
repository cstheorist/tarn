//! SQL text -> physical plan.
//!
//! Statements arrive normalized (literals replaced by `?N`, see normalize.rs),
//! are parsed with sqlparser's MySQL dialect, and bound against the catalog.
//! Access-path selection looks for equality/range predicates on index
//! prefixes so that point lookups and bounded index scans reach Tarantool
//! instead of full scans.

use crate::admin::Admin;
use crate::catalog::{self, Catalog, ColumnDef, ForeignKeyDef, IndexDef, TableDef};
use crate::expr::{column_expr_type, infer, CastTo, CmpOp, Expr, ExprType, Func};
use crate::plan::*;
use crate::types::{Arith, SqlType};
use sqlparser::ast as A;
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use std::sync::Arc;
use tarn_common::{Column, ColumnType, SqlError, SqlResult, Value};

/// Column references >= AGG_BASE stand for aggregate results while binding.
const AGG_BASE: usize = 1 << 24;

pub struct PlanCtx<'a> {
    pub catalog: &'a Catalog,
    pub db: Option<&'a str>,
    /// Parameter values when known (text queries), for metadata inference.
    pub params: &'a [Value],
}

#[derive(Clone)]
struct ScopeTable {
    alias: String,
    table: Arc<TableDef>,
    offset: usize,
}

#[derive(Clone, Default)]
struct Scope {
    tables: Vec<ScopeTable>,
}

impl Scope {
    fn width(&self) -> usize {
        self.tables
            .last()
            .map(|t| t.offset + t.table.columns.len())
            .unwrap_or(0)
    }
    fn types(&self) -> Vec<ExprType> {
        let mut v = Vec::new();
        for t in &self.tables {
            for c in &t.table.columns {
                v.push(column_expr_type(c.ty(), c.nullable));
            }
        }
        v
    }
}

fn ident(i: &A::Ident) -> String {
    i.value.clone()
}

fn object_name_parts(n: &A::ObjectName) -> Vec<String> {
    n.0.iter()
        .map(|p| match p {
            A::ObjectNamePart::Identifier(i) => i.value.clone(),
            other => other.to_string(),
        })
        .collect()
}

fn table_name(n: &A::ObjectName, ctx: &PlanCtx) -> SqlResult<(String, String)> {
    let parts = object_name_parts(n);
    match parts.as_slice() {
        [t] => Ok((
            ctx.db
                .ok_or_else(|| SqlError::new(1046, "3D000", "No database selected"))?
                .to_string(),
            t.clone(),
        )),
        [d, t] => Ok((d.clone(), t.clone())),
        _ => Err(SqlError::parse(format!("bad table name {}", n))),
    }
}

fn lookup_table(n: &A::ObjectName, ctx: &PlanCtx) -> SqlResult<Arc<TableDef>> {
    let (db, t) = table_name(n, ctx)?;
    ctx.catalog.get(&db, &t).ok_or_else(|| SqlError::no_table(&db, &t))
}

pub fn parse_one(sql: &str) -> SqlResult<A::Statement> {
    let mut stmts = Parser::parse_sql(&MySqlDialect {}, sql).map_err(|e| SqlError::parse(format!("{}", e)))?;
    if stmts.len() != 1 {
        return Err(SqlError::parse("expected exactly one statement"));
    }
    Ok(stmts.pop().unwrap())
}

/// Plan a (normalized) statement.
pub fn plan(sql: &str, ctx: &PlanCtx) -> SqlResult<Stmt> {
    if let Some(a) = crate::admin::recognize(sql)? {
        return Ok(Stmt::Admin(a));
    }
    if let Some(plans) = crate::alter::plan(sql, ctx)? {
        return Ok(Stmt::Alter(plans));
    }
    let (sql, partition_key) = extract_partition_by_key(sql);
    let (sql, invisible) = if first_word_is(&sql, "CREATE") {
        strip_index_visibility(&sql)
    } else {
        (sql, Vec::new())
    };
    let stmt = parse_one(&sql)?;
    match stmt {
        A::Statement::Query(q) => Ok(Stmt::Select(Arc::new(plan_query(&q, ctx)?))),
        A::Statement::Insert(ins) => Ok(Stmt::Insert(Arc::new(plan_insert(&ins, ctx)?))),
        A::Statement::Update(u) => Ok(Stmt::Update(Arc::new(plan_update(&u, ctx)?))),
        A::Statement::Delete(d) => Ok(Stmt::Delete(Arc::new(plan_delete(&d, ctx)?))),
        A::Statement::CreateTable(ct) => plan_create_table(&ct, partition_key, &invisible, ctx),
        A::Statement::Drop {
            object_type,
            if_exists,
            names,
            ..
        } => match object_type {
            A::ObjectType::Table => {
                let tables = names
                    .iter()
                    .map(|n| table_name(n, ctx))
                    .collect::<SqlResult<Vec<_>>>()?;
                Ok(Stmt::DropTable { tables, if_exists })
            }
            A::ObjectType::Schema | A::ObjectType::Database => {
                let name = names
                    .first()
                    .map(|n| object_name_parts(n).join("."))
                    .unwrap_or_default();
                Ok(Stmt::DropDatabase { name, if_exists })
            }
            other => Err(SqlError::unsupported(format!("DROP {}", other))),
        },
        A::Statement::Truncate(t) => {
            let target = t
                .table_names
                .first()
                .ok_or_else(|| SqlError::parse("TRUNCATE needs a table"))?;
            let (db, table) = table_name(&target.name, ctx)?;
            Ok(Stmt::Truncate { db, table })
        }
        A::Statement::CreateDatabase {
            db_name, if_not_exists, ..
        } => Ok(Stmt::CreateDatabase {
            name: object_name_parts(&db_name).join("."),
            if_not_exists,
        }),
        A::Statement::CreateSchema {
            schema_name,
            if_not_exists,
            ..
        } => {
            let name = match schema_name {
                A::SchemaName::Simple(n) | A::SchemaName::NamedAuthorization(n, _) => object_name_parts(&n).join("."),
                A::SchemaName::UnnamedAuthorization(i) => ident(&i),
            };
            Ok(Stmt::CreateDatabase { name, if_not_exists })
        }
        A::Statement::StartTransaction { .. } => Ok(Stmt::Admin(Admin::Begin { read_only: false })),
        A::Statement::Commit { .. } => Ok(Stmt::Admin(Admin::Commit)),
        A::Statement::Rollback { .. } => Ok(Stmt::Admin(Admin::Rollback)),
        other => Err(SqlError::unsupported(statement_kind(&other))),
    }
}

fn first_word_is(sql: &str, w: &str) -> bool {
    sql.trim_start()
        .get(..w.len())
        .is_some_and(|x| x.eq_ignore_ascii_case(w))
}

fn statement_kind(s: &A::Statement) -> String {
    let t = s.to_string();
    t.split_whitespace().take(2).collect::<Vec<_>>().join(" ")
}

/// `PARTITION BY KEY(a, b) [PARTITIONS n]` declares the shard key. sqlparser
/// does not parse MySQL partitioning, so it is cut out of the text first.
fn extract_partition_by_key(sql: &str) -> (String, Option<Vec<String>>) {
    let up = sql.to_ascii_uppercase();
    let Some(pos) = up.find("PARTITION BY") else {
        return (sql.to_string(), None);
    };
    let rest = &up[pos + "PARTITION BY".len()..];
    let rest_trim = rest.trim_start();
    let kw_len = if rest_trim.starts_with("LINEAR KEY") {
        "LINEAR KEY".len()
    } else if rest_trim.starts_with("KEY") {
        3
    } else {
        return (sql.to_string(), None);
    };
    let start = pos + "PARTITION BY".len() + (rest.len() - rest_trim.len()) + kw_len;
    let Some(open) = sql[start..].find('(') else {
        return (sql.to_string(), None);
    };
    let Some(close) = sql[start + open..].find(')') else {
        return (sql.to_string(), None);
    };
    let cols: Vec<String> = sql[start + open + 1..start + open + close]
        .split(',')
        .map(|c| c.trim().trim_matches('`').to_string())
        .filter(|c| !c.is_empty())
        .collect();
    let mut end = start + open + close + 1;
    let tail = up[end..].trim_start();
    if tail.starts_with("PARTITIONS") {
        let skip = up[end..].len() - tail.len() + "PARTITIONS".len();
        end += skip;
        let digits = up[end..].trim_start();
        end += up[end..].len() - digits.len();
        end += digits.chars().take_while(|c| c.is_ascii_digit()).count();
    }
    let mut out = sql[..pos].to_string();
    out.push_str(&sql[end..]);
    (out, Some(cols))
}

// ---------------------------------------------------------------------------
// Expression binding
// ---------------------------------------------------------------------------

struct Binder<'a, 'c> {
    scope: &'a Scope,
    #[allow(dead_code)]
    ctx: &'a PlanCtx<'c>,
    aggs: Option<&'a mut Vec<AggCall>>,
    /// For ON DUPLICATE KEY UPDATE: VALUES(col) refers to col + values_offset.
    values_offset: Option<usize>,
    /// `INSERT ... AS alias [(cols)] ON DUPLICATE KEY UPDATE`: the alias
    /// names the proposed row too.
    row_alias: Option<&'a RowAlias>,
}

/// The row alias of `INSERT ... VALUES (...) AS alias [(c1, c2, ...)]`
/// (MySQL 8.0.19): `alias.col`, and with column aliases also the bare alias
/// names, refer to the proposed row in ON DUPLICATE KEY UPDATE.
struct RowAlias {
    name: String,
    /// (name, table column): every table column, or the column aliases
    /// (which name the INSERT's columns in order).
    cols: Vec<(String, usize)>,
    explicit: bool,
}

impl RowAlias {
    fn col(&self, name: &str) -> Option<usize> {
        self.cols
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, c)| *c)
    }
}

fn unknown_col(name: &str) -> SqlError {
    SqlError::new(1054, "42S22", format!("Unknown column '{}' in 'field list'", name))
}

impl<'a, 'c> Binder<'a, 'c> {
    fn new(scope: &'a Scope, ctx: &'a PlanCtx<'c>) -> Self {
        Binder {
            scope,
            ctx,
            aggs: None,
            values_offset: None,
            row_alias: None,
        }
    }

    /// Is `e` an unsigned integer: an UNSIGNED integer column, an unsigned
    /// literal above BIGINT's range, CAST(... AS UNSIGNED), or unsigned
    /// arithmetic?
    fn unsigned_int(&self, e: &Expr) -> bool {
        match e {
            Expr::Col(i) => {
                let mut i = *i;
                let width = self.scope.width();
                if i >= width {
                    match self.values_offset {
                        Some(off) if i >= off => i -= off,
                        _ => return false,
                    }
                }
                self.scope.tables.iter().any(|t| {
                    i >= t.offset
                        && i < t.offset + t.table.columns.len()
                        && t.table.columns[i - t.offset].ty().is_integer()
                        && t.table.columns[i - t.offset].ty().unsigned
                })
            }
            Expr::Lit(Value::UInt(u)) => *u > i64::MAX as u64,
            Expr::Cast(_, CastTo::Unsigned) => true,
            Expr::Arith(_, op, _) => op.is_unsigned(),
            _ => false,
        }
    }

    fn resolve(&self, table: Option<&str>, col: &str) -> SqlResult<usize> {
        let mut found = None;
        for t in &self.scope.tables {
            if let Some(tn) = table {
                if !t.alias.eq_ignore_ascii_case(tn) && !t.table.name.eq_ignore_ascii_case(tn) {
                    continue;
                }
            }
            if let Some(c) = t.table.col_index(col) {
                if found.is_some() {
                    return Err(SqlError::new(
                        1052,
                        "23000",
                        format!("Column '{}' in field list is ambiguous", col),
                    ));
                }
                found = Some(t.offset + c);
            }
        }
        found.ok_or_else(|| {
            unknown_col(&match table {
                Some(t) => format!("{}.{}", t, col),
                None => col.to_string(),
            })
        })
    }

    fn bind(&mut self, e: &A::Expr) -> SqlResult<Expr> {
        use A::Expr as E;
        Ok(match e {
            E::Identifier(i) => match (self.row_alias, self.values_offset) {
                (Some(ra), Some(off)) if ra.explicit && self.resolve(None, &i.value).is_err() => {
                    Expr::Col(off + ra.col(&i.value).ok_or_else(|| unknown_col(&i.value))?)
                }
                _ => Expr::Col(self.resolve(None, &i.value)?),
            },
            E::CompoundIdentifier(parts) => {
                let n = parts.len();
                match (self.row_alias, self.values_offset) {
                    (Some(ra), Some(off)) if n == 2 && parts[0].value.eq_ignore_ascii_case(&ra.name) => {
                        let c = ra
                            .col(&parts[1].value)
                            .ok_or_else(|| unknown_col(&format!("{}.{}", parts[0].value, parts[1].value)))?;
                        Expr::Col(off + c)
                    }
                    _ if n >= 2 => Expr::Col(self.resolve(Some(&parts[n - 2].value), &parts[n - 1].value)?),
                    _ => Expr::Col(self.resolve(None, &parts[0].value)?),
                }
            }
            E::Value(v) => self.bind_value(&v.value)?,
            E::Nested(inner) => self.bind(inner)?,
            E::BinaryOp {
                left,
                op: op @ (A::BinaryOperator::Eq | A::BinaryOperator::NotEq),
                right,
            } if matches!(**left, E::Tuple(_)) && matches!(**right, E::Tuple(_)) => {
                let (E::Tuple(l), E::Tuple(r)) = (&**left, &**right) else {
                    unreachable!()
                };
                let lhs: Vec<Expr> = l.iter().map(|x| self.bind(x)).collect::<SqlResult<_>>()?;
                let rhs: Vec<&A::Expr> = r.iter().collect();
                let e = self.row_eq(&lhs, &rhs)?;
                if matches!(op, A::BinaryOperator::NotEq) {
                    Expr::Not(Box::new(e))
                } else {
                    e
                }
            }
            E::BinaryOp { left, op, right } => {
                let (l, r) = (Box::new(self.bind(left)?), Box::new(self.bind(right)?));
                // MySQL: + - * with an unsigned operand are BIGINT UNSIGNED.
                let dom = |o: Arith| {
                    if self.unsigned_int(&l) || self.unsigned_int(&r) {
                        o.unsigned()
                    } else {
                        o
                    }
                };
                let (add, sub, mul) = (dom(Arith::Add), dom(Arith::Sub), dom(Arith::Mul));
                use A::BinaryOperator as B;
                match op {
                    B::Plus => Expr::Arith(l, add, r),
                    B::Minus => Expr::Arith(l, sub, r),
                    B::Multiply => Expr::Arith(l, mul, r),
                    B::Divide => Expr::Arith(l, Arith::Div, r),
                    B::Modulo => Expr::Arith(l, Arith::Mod, r),
                    B::MyIntegerDivide | B::DuckIntegerDivide => Expr::Arith(l, Arith::IntDiv, r),
                    B::Eq => Expr::Cmp(l, CmpOp::Eq, r),
                    B::NotEq => Expr::Cmp(l, CmpOp::NotEq, r),
                    B::Lt => Expr::Cmp(l, CmpOp::Lt, r),
                    B::LtEq => Expr::Cmp(l, CmpOp::LtEq, r),
                    B::Gt => Expr::Cmp(l, CmpOp::Gt, r),
                    B::GtEq => Expr::Cmp(l, CmpOp::GtEq, r),
                    B::Spaceship => Expr::Cmp(l, CmpOp::NullSafeEq, r),
                    B::And => Expr::And(l, r),
                    B::Or => Expr::Or(l, r),
                    B::Xor => Expr::Xor(l, r),
                    B::StringConcat => Expr::Or(l, r), // MySQL: || is OR unless PIPES_AS_CONCAT
                    other => return Err(SqlError::unsupported(format!("operator {}", other))),
                }
            }
            E::UnaryOp { op, expr } => {
                let x = Box::new(self.bind(expr)?);
                match op {
                    A::UnaryOperator::Not | A::UnaryOperator::BangNot => Expr::Not(x),
                    A::UnaryOperator::Minus => match *x {
                        Expr::Lit(Value::Int(i)) => Expr::Lit(Value::Int(-i)),
                        other => Expr::Neg(Box::new(other)),
                    },
                    A::UnaryOperator::Plus => *x,
                    other => return Err(SqlError::unsupported(format!("operator {}", other))),
                }
            }
            E::IsNull(x) => Expr::IsNull(Box::new(self.bind(x)?), false),
            E::IsNotNull(x) => Expr::IsNull(Box::new(self.bind(x)?), true),
            E::IsTrue(x) => Expr::IsTrue(Box::new(self.bind(x)?), true, false),
            E::IsNotTrue(x) => Expr::IsTrue(Box::new(self.bind(x)?), true, true),
            E::IsFalse(x) => Expr::IsTrue(Box::new(self.bind(x)?), false, false),
            E::IsNotFalse(x) => Expr::IsTrue(Box::new(self.bind(x)?), false, true),
            E::InList { expr, list, negated } if matches!(**expr, E::Tuple(_)) => {
                let E::Tuple(lhs) = &**expr else { unreachable!() };
                let lhs: Vec<Expr> = lhs.iter().map(|x| self.bind(x)).collect::<SqlResult<_>>()?;
                let mut disj: Option<Expr> = None;
                for item in list {
                    let rhs: Vec<&A::Expr> = match item {
                        E::Tuple(r) => r.iter().collect(),
                        E::Nested(inner) => vec![&**inner],
                        other => vec![other],
                    };
                    let conj = self.row_eq(&lhs, &rhs)?;
                    disj = Some(match disj {
                        None => conj,
                        Some(d) => Expr::Or(Box::new(d), Box::new(conj)),
                    });
                }
                let e = disj.unwrap_or(Expr::Lit(Value::Int(0)));
                if *negated {
                    Expr::Not(Box::new(e))
                } else {
                    e
                }
            }
            E::InList { expr, list, negated } => Expr::InList(
                Box::new(self.bind(expr)?),
                list.iter().map(|x| self.bind(x)).collect::<SqlResult<_>>()?,
                *negated,
            ),
            E::Between {
                expr,
                negated,
                low,
                high,
            } => Expr::Between(
                Box::new(self.bind(expr)?),
                Box::new(self.bind(low)?),
                Box::new(self.bind(high)?),
                *negated,
            ),
            E::Like {
                negated, expr, pattern, ..
            }
            | E::ILike {
                negated, expr, pattern, ..
            } => Expr::Like(Box::new(self.bind(expr)?), Box::new(self.bind(pattern)?), *negated),
            E::Case {
                operand,
                conditions,
                else_result,
                ..
            } => Expr::Case(
                match operand {
                    Some(o) => Some(Box::new(self.bind(o)?)),
                    None => None,
                },
                conditions
                    .iter()
                    .map(|w| Ok((self.bind(&w.condition)?, self.bind(&w.result)?)))
                    .collect::<SqlResult<_>>()?,
                match else_result {
                    Some(x) => Some(Box::new(self.bind(x)?)),
                    None => None,
                },
            ),
            E::Cast { expr, data_type, .. } => {
                Expr::Cast(Box::new(self.bind(expr)?), cast_target(&data_type.to_string())?)
            }
            E::Convert {
                expr,
                data_type: Some(dt),
                ..
            } => Expr::Cast(Box::new(self.bind(expr)?), cast_target(&dt.to_string())?),
            E::Convert {
                expr, data_type: None, ..
            } => self.bind(expr)?,
            E::Substring {
                expr,
                substring_from,
                substring_for,
                ..
            } => {
                let mut args = vec![self.bind(expr)?];
                args.push(match substring_from {
                    Some(f) => self.bind(f)?,
                    None => Expr::Lit(Value::Int(1)),
                });
                if let Some(f) = substring_for {
                    args.push(self.bind(f)?);
                }
                Expr::Func(Func::Substring, args)
            }
            E::Trim {
                expr,
                trim_where,
                trim_what,
                ..
            } => {
                if trim_what.is_some() {
                    return Err(SqlError::unsupported("TRIM(... FROM ...)"));
                }
                let f = match trim_where {
                    Some(A::TrimWhereField::Leading) => Func::LTrim,
                    Some(A::TrimWhereField::Trailing) => Func::RTrim,
                    _ => Func::Trim,
                };
                Expr::Func(f, vec![self.bind(expr)?])
            }
            E::Ceil { expr, .. } => Expr::Func(Func::Ceil, vec![self.bind(expr)?]),
            E::Floor { expr, .. } => Expr::Func(Func::Floor, vec![self.bind(expr)?]),
            E::Position { expr, r#in } => Expr::Func(Func::Locate, vec![self.bind(expr)?, self.bind(r#in)?]),
            E::Function(f) => self.bind_function(f)?,
            E::Collate { expr, .. } => self.bind(expr)?,
            E::TypedString(ts) => self.bind_value(&ts.value.value)?,
            E::Tuple(items) if items.len() == 1 => self.bind(&items[0])?,
            E::Subquery(_) | E::InSubquery { .. } | E::Exists { .. } => {
                return Err(SqlError::unsupported("subqueries"))
            }
            other => return Err(SqlError::unsupported(format!("expression {}", other))),
        })
    }

    fn bind_value(&self, v: &A::Value) -> SqlResult<Expr> {
        use A::Value as V;
        Ok(Expr::Lit(match v {
            V::Number(s, _) => crate::normalize::normalize(s, crate::normalize::Mode::Literals)
                .ok()
                .and_then(|n| n.params.into_iter().next())
                .unwrap_or_else(|| Value::Decimal(s.clone())),
            V::SingleQuotedString(s)
            | V::DoubleQuotedString(s)
            | V::NationalStringLiteral(s)
            | V::EscapedStringLiteral(s) => Value::str(s),
            V::HexStringLiteral(h) => Value::Bin(
                (0..h.len() / 2)
                    .filter_map(|i| u8::from_str_radix(&h[2 * i..2 * i + 2], 16).ok())
                    .collect::<Vec<u8>>()
                    .into(),
            ),
            V::Boolean(b) => Value::Int(*b as i64),
            V::Null => Value::Null,
            V::Placeholder(p) => {
                let n: usize = p
                    .trim_start_matches('?')
                    .parse()
                    .map_err(|_| SqlError::parse(format!("bad placeholder {}", p)))?;
                return Ok(Expr::Param(n - 1));
            }
            other => return Err(SqlError::unsupported(format!("literal {}", other))),
        }))
    }

    fn func_args(&mut self, f: &A::Function) -> SqlResult<(Vec<Expr>, bool, bool)> {
        let mut out = Vec::new();
        let mut star = false;
        let mut distinct = false;
        if let A::FunctionArguments::List(list) = &f.args {
            distinct = matches!(list.duplicate_treatment, Some(A::DuplicateTreatment::Distinct));
            for a in &list.args {
                match a {
                    A::FunctionArg::Unnamed(A::FunctionArgExpr::Expr(e)) => out.push(self.bind(e)?),
                    A::FunctionArg::Unnamed(A::FunctionArgExpr::Wildcard) => star = true,
                    other => return Err(SqlError::unsupported(format!("function argument {}", other))),
                }
            }
        }
        Ok((out, star, distinct))
    }

    /// `(l1, l2, ...) = (r1, r2, ...)` as a conjunction of comparisons.
    fn row_eq(&mut self, lhs: &[Expr], rhs: &[&A::Expr]) -> SqlResult<Expr> {
        if lhs.len() != rhs.len() {
            return Err(SqlError::new(
                1241,
                "21000",
                format!("Operand should contain {} column(s)", lhs.len()),
            ));
        }
        let mut conj: Option<Expr> = None;
        for (l, r) in lhs.iter().zip(rhs) {
            let c = Expr::Cmp(Box::new(l.clone()), CmpOp::Eq, Box::new(self.bind(r)?));
            conj = Some(match conj {
                None => c,
                Some(x) => Expr::And(Box::new(x), Box::new(c)),
            });
        }
        conj.ok_or_else(|| SqlError::parse("empty row value"))
    }

    /// ORDER BY / SEPARATOR clauses inside an aggregate call (GROUP_CONCAT).
    fn agg_clauses(&mut self, f: &A::Function) -> SqlResult<(Vec<(Expr, bool)>, Option<bytes::Bytes>)> {
        let mut order = Vec::new();
        let mut sep = None;
        if let A::FunctionArguments::List(list) = &f.args {
            for c in &list.clauses {
                match c {
                    A::FunctionArgumentClause::OrderBy(obs) => {
                        for o in obs {
                            let desc = matches!(o.options.sort, Some(A::OrderBySort::Desc));
                            order.push((self.bind(&o.expr)?, desc));
                        }
                    }
                    A::FunctionArgumentClause::Separator(v) => match &v.value {
                        A::Value::SingleQuotedString(s) | A::Value::DoubleQuotedString(s) => {
                            sep = Some(bytes::Bytes::copy_from_slice(s.as_bytes()))
                        }
                        other => return Err(SqlError::unsupported(format!("SEPARATOR {}", other))),
                    },
                    other => return Err(SqlError::unsupported(format!("aggregate clause {}", other))),
                }
            }
        }
        Ok((order, sep))
    }

    fn bind_function(&mut self, f: &A::Function) -> SqlResult<Expr> {
        let name = object_name_parts(&f.name)
            .last()
            .cloned()
            .unwrap_or_default()
            .to_ascii_uppercase();
        let agg = match name.as_str() {
            "COUNT" => Some(AggFunc::Count),
            "SUM" => Some(AggFunc::Sum),
            "AVG" => Some(AggFunc::Avg),
            "MIN" => Some(AggFunc::Min),
            "MAX" => Some(AggFunc::Max),
            "ANY_VALUE" => Some(AggFunc::AnyValue),
            "GROUP_CONCAT" => Some(AggFunc::GroupConcat),
            _ => None,
        };
        if let Some(func) = agg {
            if self.aggs.is_none() {
                return Err(SqlError::new(1111, "HY000", "Invalid use of group function"));
            }
            // aggregate arguments are bound without aggregate context
            let saved = self.aggs.take();
            let r = self.func_args(f).and_then(|a| Ok((a, self.agg_clauses(f)?)));
            self.aggs = saved;
            let ((mut args, star, distinct), (order, sep)) = r?;
            let func = if func == AggFunc::Count && (star || args.is_empty()) {
                AggFunc::CountStar
            } else {
                func
            };
            let arg = if func == AggFunc::GroupConcat && args.len() > 1 {
                // GROUP_CONCAT(a, b, ...) concatenates its arguments per row
                Some(Expr::Func(Func::Concat, std::mem::take(&mut args)))
            } else {
                args.into_iter().next()
            };
            let aggs = self.aggs.as_mut().unwrap();
            aggs.push(AggCall {
                func,
                arg,
                distinct,
                order,
                sep,
            });
            return Ok(Expr::Col(AGG_BASE + aggs.len() - 1));
        }
        if name == "VALUES" {
            let (args, _, _) = self.func_args(f)?;
            if let (Some(off), Some(Expr::Col(c))) = (self.values_offset, args.first()) {
                return Ok(Expr::Col(off + c));
            }
            return Err(SqlError::unsupported("VALUES() outside ON DUPLICATE KEY UPDATE"));
        }
        let func = Func::lookup(&name).ok_or_else(|| {
            SqlError::new(
                1305,
                "42000",
                format!("FUNCTION {} does not exist", name.to_ascii_lowercase()),
            )
        })?;
        let (args, _, _) = self.func_args(f)?;
        Ok(Expr::Func(func, args))
    }
}

fn cast_target(t: &str) -> SqlResult<CastTo> {
    let u = t.to_ascii_uppercase();
    Ok(
        if u.starts_with("SIGNED") || u.starts_with("INT") || u.starts_with("BIGINT") {
            CastTo::Signed
        } else if u.starts_with("UNSIGNED") {
            CastTo::Unsigned
        } else if u.starts_with("CHAR") || u.starts_with("VARCHAR") || u.starts_with("NCHAR") || u.starts_with("TEXT") {
            CastTo::Char
        } else if u.starts_with("DECIMAL") || u.starts_with("NUMERIC") {
            CastTo::Decimal(SqlType::parse(t).ok().and_then(|s| s.scale).unwrap_or(0))
        } else if u.starts_with("DOUBLE") || u.starts_with("FLOAT") || u.starts_with("REAL") {
            CastTo::Double
        } else if u.starts_with("DATETIME") || u.starts_with("TIMESTAMP") {
            CastTo::DateTime
        } else if u.starts_with("DATE") {
            CastTo::Date
        } else if u.starts_with("BINARY") {
            CastTo::Binary
        } else {
            return Err(SqlError::unsupported(format!("CAST AS {}", t)));
        },
    )
}

fn split_and(e: Expr, out: &mut Vec<Expr>) {
    match e {
        Expr::And(a, b) => {
            split_and(*a, out);
            split_and(*b, out);
        }
        other => out.push(other),
    }
}

fn join_and(mut v: Vec<Expr>) -> Option<Expr> {
    let first = if v.is_empty() { return None } else { v.remove(0) };
    Some(v.into_iter().fold(first, |a, b| Expr::And(Box::new(a), Box::new(b))))
}

// ---------------------------------------------------------------------------
// Access paths
// ---------------------------------------------------------------------------

enum Pred {
    Eq(Expr),
    In(Vec<Expr>),
    Range(Option<(Expr, bool)>, Option<(Expr, bool)>),
}

/// Column refs of `e` all lie outside [lo, hi): computable before this table.
fn independent_of(e: &Expr, lo: usize, hi: usize, max_allowed: usize) -> bool {
    let mut cols = Vec::new();
    e.columns(&mut cols);
    cols.iter().all(|&c| (c < lo || c >= hi) && c < max_allowed)
}

/// Choose an access path for the table at [offset, offset+ncols) given
/// conjuncts; columns below `offset` (tables to the left) are available.
/// Upper bound on the keys of one multi-key lookup; beyond it the planner
/// keeps its other paths.
const MAX_KEYS: usize = 4096;

/// Column equalities of one disjunct: (table column, value expression).
type EqAlt = Vec<(usize, Expr)>;

/// The equality alternatives a predicate imposes on the table's columns, one
/// per disjunct (`c IN (1, 2)` -> [[c=1], [c=2]]; `(a=1 AND b=2) OR a=3` ->
/// [[a=1, b=2], [a=3]]). Parts that are not equalities are dropped from a
/// disjunct, which only widens it: the full predicate stays in the filter.
/// None when some disjunct constrains nothing.
fn eq_alternatives(
    e: &Expr,
    local: &dyn Fn(&Expr) -> Option<usize>,
    indep: &dyn Fn(&Expr) -> bool,
) -> Option<Vec<EqAlt>> {
    match e {
        Expr::Cmp(a, CmpOp::Eq, b) => match (local(a), local(b)) {
            (Some(c), None) if indep(b) => Some(vec![vec![(c, (**b).clone())]]),
            (None, Some(c)) if indep(a) => Some(vec![vec![(c, (**a).clone())]]),
            _ => None,
        },
        Expr::InList(x, list, false) => {
            let c = local(x)?;
            if list.is_empty() || list.len() > MAX_KEYS || !list.iter().all(|v| v.is_constant()) {
                return None;
            }
            Some(list.iter().map(|v| vec![(c, v.clone())]).collect())
        }
        Expr::Or(a, b) => {
            let mut l = eq_alternatives(a, local, indep)?;
            l.extend(eq_alternatives(b, local, indep)?);
            (l.len() <= MAX_KEYS).then_some(l)
        }
        Expr::And(a, b) => match (eq_alternatives(a, local, indep), eq_alternatives(b, local, indep)) {
            (Some(x), Some(y)) => cross(&x, &y),
            (Some(x), None) | (None, Some(x)) => Some(x),
            (None, None) => None,
        },
        _ => None,
    }
}

/// Every combination of one alternative from each side, or None if there
/// would be more than MAX_KEYS.
fn cross(x: &[EqAlt], y: &[EqAlt]) -> Option<Vec<EqAlt>> {
    if x.len().saturating_mul(y.len()) > MAX_KEYS {
        return None;
    }
    let mut out = Vec::with_capacity(x.len() * y.len());
    for a in x {
        for b in y {
            let mut ab = a.clone();
            ab.extend(b.iter().cloned());
            out.push(ab);
        }
    }
    Some(out)
}

/// Alternatives of a conjunction: the cross product over its conjuncts
/// (conjuncts that constrain nothing are skipped).
fn conj_alternatives(
    conj: &[Expr],
    local: &dyn Fn(&Expr) -> Option<usize>,
    indep: &dyn Fn(&Expr) -> bool,
) -> Option<Vec<EqAlt>> {
    let mut acc: Option<Vec<EqAlt>> = None;
    for p in conj {
        if let Some(a) = eq_alternatives(p, local, indep) {
            acc = Some(match acc {
                None => a,
                Some(prev) => cross(&prev, &a)?,
            });
        }
    }
    acc
}

/// Index merge: when no index covers every alternative, look each one up
/// through the index whose prefix it fixes best (a full unique key first,
/// then the longest prefix), one lookup path per index. None if some
/// alternative fixes no index prefix at all.
fn union_path(table: &TableDef, alts: &[EqAlt]) -> Option<AccessPath> {
    let mut groups: Vec<(u32, usize, bool, Vec<Vec<Expr>>)> = Vec::new();
    for a in alts {
        let mut pick: Option<(i32, &crate::catalog::IndexDef, usize)> = None;
        for idx in table.usable_indexes() {
            let k = idx.cols.iter().take_while(|c| a.iter().any(|(x, _)| x == *c)).count();
            if k == 0 {
                continue;
            }
            let full = idx.unique && k == idx.cols.len();
            let score = if full { 1000 } else { 10 * k as i32 } + if idx.id == 0 { 1 } else { 0 };
            if pick.as_ref().is_none_or(|p| score > p.0) {
                pick = Some((score, idx, k));
            }
        }
        let (_, idx, k) = pick?;
        let key: Vec<Expr> = idx.cols[..k]
            .iter()
            .map(|c| a.iter().find(|(x, _)| x == c).map(|(_, e)| e.clone()).unwrap())
            .collect();
        let full = idx.unique && k == idx.cols.len();
        match groups.iter_mut().find(|g| g.0 == idx.id && g.1 == k) {
            Some(g) => g.3.push(key),
            None => groups.push((idx.id, k, full, vec![key])),
        }
    }
    if groups.len() < 2 {
        return None; // one index covers everything: the plain paths handle it
    }
    Some(AccessPath::Union(
        groups
            .into_iter()
            .map(|(index, _, full, keys)| {
                if full {
                    AccessPath::Point { index, keys }
                } else {
                    AccessPath::Prefixes { index, keys }
                }
            })
            .collect(),
    ))
}

fn choose_path(table: &TableDef, offset: usize, conj: &[Expr]) -> AccessPath {
    let hi = offset + table.columns.len();
    let mut preds: Vec<Vec<Pred>> = (0..table.columns.len()).map(|_| Vec::new()).collect();
    let local = |e: &Expr| -> Option<usize> {
        match e {
            Expr::Col(c) if *c >= offset && *c < hi => Some(*c - offset),
            _ => None,
        }
    };
    for p in conj {
        match p {
            Expr::Cmp(a, op, b) => {
                let (col, other, op) = match (local(a), local(b)) {
                    (Some(c), None) if independent_of(b, offset, hi, offset) => (c, b, *op),
                    (None, Some(c)) if independent_of(a, offset, hi, offset) => (
                        c,
                        a,
                        match op {
                            CmpOp::Lt => CmpOp::Gt,
                            CmpOp::LtEq => CmpOp::GtEq,
                            CmpOp::Gt => CmpOp::Lt,
                            CmpOp::GtEq => CmpOp::LtEq,
                            o => *o,
                        },
                    ),
                    _ => continue,
                };
                let o = (**other).clone();
                preds[col].push(match op {
                    CmpOp::Eq | CmpOp::NullSafeEq => Pred::Eq(o),
                    CmpOp::Lt => Pred::Range(None, Some((o, false))),
                    CmpOp::LtEq => Pred::Range(None, Some((o, true))),
                    CmpOp::Gt => Pred::Range(Some((o, false)), None),
                    CmpOp::GtEq => Pred::Range(Some((o, true)), None),
                    CmpOp::NotEq => continue,
                });
            }
            Expr::Between(x, lo, hi2, false) => {
                if let Some(c) = local(x) {
                    if independent_of(lo, offset, hi, offset) && independent_of(hi2, offset, hi, offset) {
                        preds[c].push(Pred::Range(Some(((**lo).clone(), true)), Some(((**hi2).clone(), true))));
                    }
                }
            }
            Expr::InList(x, list, false) => {
                if let Some(c) = local(x) {
                    if list.len() == 1 && list[0].is_constant() {
                        preds[c].push(Pred::Eq(list[0].clone()));
                    } else if list.iter().all(|e| e.is_constant()) && !list.is_empty() {
                        preds[c].push(Pred::In(list.clone()));
                    }
                }
            }
            _ => {}
        }
    }
    let mut best: (i32, AccessPath) = (0, AccessPath::Full);
    // Several equality keys (IN lists, ORs of equalities, row-value IN):
    // look each one up instead of scanning (issue #10).
    let indep = |e: &Expr| independent_of(e, offset, hi, offset);
    if let Some(alts) = conj_alternatives(conj, &local, &indep) {
        if alts.len() > 1 {
            if let Some(u) = union_path(table, &alts) {
                best = (8, u);
            }
            for idx in table.usable_indexes() {
                // Longest index prefix that every alternative fixes.
                let k = alts
                    .iter()
                    .map(|a| idx.cols.iter().take_while(|c| a.iter().any(|(x, _)| x == *c)).count())
                    .min()
                    .unwrap_or(0);
                if k == 0 {
                    continue;
                }
                let keys: Vec<Vec<Expr>> = alts
                    .iter()
                    .map(|a| {
                        idx.cols[..k]
                            .iter()
                            .map(|c| a.iter().find(|(x, _)| x == c).map(|(_, e)| e.clone()).unwrap())
                            .collect()
                    })
                    .collect();
                let pk_bonus = if idx.id == 0 { 1 } else { 0 };
                let (score, path) = if idx.unique && k == idx.cols.len() {
                    (900 + pk_bonus, AccessPath::Point { index: idx.id, keys })
                } else {
                    (
                        10 * k as i32 - 1 + pk_bonus,
                        AccessPath::Prefixes { index: idx.id, keys },
                    )
                };
                if score > best.0 {
                    best = (score, path);
                }
            }
        }
    }
    for idx in table.usable_indexes() {
        let mut eq: Vec<Expr> = Vec::new();
        let mut in_list: Option<(usize, Vec<Expr>)> = None;
        let mut range: Option<(Option<(Expr, bool)>, Option<(Expr, bool)>)> = None;
        for (pos, &c) in idx.cols.iter().enumerate() {
            if let Some(Pred::Eq(e)) = preds[c].iter().find(|p| matches!(p, Pred::Eq(_))) {
                eq.push(e.clone());
                continue;
            }
            if in_list.is_none() {
                if let Some(Pred::In(l)) = preds[c].iter().find(|p| matches!(p, Pred::In(_))) {
                    in_list = Some((pos, l.clone()));
                    eq.push(Expr::Lit(Value::Null)); // placeholder, replaced per key
                    continue;
                }
            }
            let mut lo = None;
            let mut hi_b = None;
            for p in &preds[c] {
                if let Pred::Range(l, h) = p {
                    if lo.is_none() {
                        lo = l.clone();
                    }
                    if hi_b.is_none() {
                        hi_b = h.clone();
                    }
                }
            }
            if lo.is_some() || hi_b.is_some() {
                range = Some((lo, hi_b));
            }
            break;
        }
        let full = eq.len() == idx.cols.len();
        let pk_bonus = if idx.id == 0 { 1 } else { 0 };
        let (score, path) = if full && idx.unique {
            let keys = match &in_list {
                None => vec![eq.clone()],
                Some((pos, list)) => list
                    .iter()
                    .map(|v| {
                        let mut k = eq.clone();
                        k[*pos] = v.clone();
                        k
                    })
                    .collect(),
            };
            (
                if in_list.is_some() { 900 } else { 1000 } + pk_bonus,
                AccessPath::Point { index: idx.id, keys },
            )
        } else if in_list.is_some() {
            continue; // IN on a non-unique / partial key: fall back to other paths
        } else if !eq.is_empty() || range.is_some() {
            let (lo, hi2) = range.unwrap_or((None, None));
            let s = 10 * eq.len() as i32 + if lo.is_some() || hi2.is_some() { 5 } else { 0 } + pk_bonus;
            (
                s,
                AccessPath::Range {
                    index: idx.id,
                    eq,
                    lo,
                    hi: hi2,
                    reverse: false,
                },
            )
        } else {
            continue;
        };
        if score > best.0 {
            best = (score, path);
        }
    }
    best.1
}

// ---------------------------------------------------------------------------
// SELECT
// ---------------------------------------------------------------------------

fn build_scope(from: &[A::TableWithJoins], ctx: &PlanCtx) -> SqlResult<(Scope, Vec<(JoinKind, Option<A::Expr>)>)> {
    let mut scope = Scope::default();
    let mut joins = Vec::new();
    let add = |tf: &A::TableFactor, scope: &mut Scope| -> SqlResult<()> {
        match tf {
            A::TableFactor::Table { name, alias, .. } => {
                let table = lookup_table(name, ctx)?;
                let alias = alias
                    .as_ref()
                    .map(|a| ident(&a.name))
                    .unwrap_or_else(|| table.name.clone());
                let offset = scope.width();
                scope.tables.push(ScopeTable { alias, table, offset });
                Ok(())
            }
            A::TableFactor::Derived { .. } => Err(SqlError::unsupported("derived tables")),
            other => Err(SqlError::unsupported(format!("FROM {}", other))),
        }
    };
    for (i, twj) in from.iter().enumerate() {
        add(&twj.relation, &mut scope)?;
        if i > 0 {
            joins.push((JoinKind::Inner, None)); // comma join; conditions come from WHERE
        }
        for j in &twj.joins {
            add(&j.relation, &mut scope)?;
            let (kind, cons) = match &j.join_operator {
                A::JoinOperator::Join(c) | A::JoinOperator::Inner(c) | A::JoinOperator::StraightJoin(c) => {
                    (JoinKind::Inner, c)
                }
                A::JoinOperator::Left(c) | A::JoinOperator::LeftOuter(c) => (JoinKind::Left, c),
                A::JoinOperator::CrossJoin(c) => (JoinKind::Inner, c),
                other => {
                    return Err(SqlError::unsupported(format!(
                        "join {:?}",
                        std::mem::discriminant(other)
                    )))
                }
            };
            let on = match cons {
                A::JoinConstraint::On(e) => Some(e.clone()),
                A::JoinConstraint::None => None,
                A::JoinConstraint::Using(cols) => {
                    let l = &scope.tables[scope.tables.len() - 2];
                    let r = scope.tables.last().unwrap();
                    let mut conds: Vec<String> = Vec::new();
                    for c in cols {
                        let n = object_name_parts(c).join(".");
                        conds.push(format!("`{}`.`{}` = `{}`.`{}`", l.alias, n, r.alias, n));
                    }
                    let sql = format!("SELECT 1 WHERE {}", conds.join(" AND "));
                    match parse_one(&sql)? {
                        A::Statement::Query(q) => match *q.body {
                            A::SetExpr::Select(s) => s.selection.clone(),
                            _ => None,
                        },
                        _ => None,
                    }
                }
                A::JoinConstraint::Natural => return Err(SqlError::unsupported("NATURAL JOIN")),
            };
            joins.push((kind, on));
        }
    }
    Ok((scope, joins))
}

fn expr_name(e: &A::Expr, params: &[Value]) -> String {
    let s = match e {
        A::Expr::Identifier(i) => i.value.clone(),
        A::Expr::CompoundIdentifier(p) => p.last().map(|i| i.value.clone()).unwrap_or_default(),
        other => other.to_string(),
    };
    // Show literals rather than their placeholders in column names.
    if !s.contains('?') {
        return s;
    }
    let mut out = String::new();
    let b = s.as_bytes();
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'?' {
            let mut j = i + 1;
            while j < b.len() && b[j].is_ascii_digit() {
                j += 1;
            }
            if let Ok(n) = s[i + 1..j].parse::<usize>() {
                match n.checked_sub(1).and_then(|k| params.get(k)) {
                    Some(Value::Str(v)) => out.push_str(&String::from_utf8_lossy(v)),
                    Some(v) => out.push_str(&v.to_string()),
                    None => out.push_str(&s[i..j]),
                }
                i = j;
                continue;
            }
        }
        out.push(b[i] as char);
        i += 1;
    }
    out
}

fn same_expr(a: &Expr, b: &Expr) -> bool {
    format!("{:?}", a) == format!("{:?}", b)
}

/// Rewrite an expression bound over the source row (with aggregate markers)
/// onto the aggregation output layout [groups..., aggs...]. Bare source
/// columns that are not grouped become ANY_VALUE aggregates (MySQL without
/// ONLY_FULL_GROUP_BY).
fn post_agg(e: &Expr, groups: &[Expr], aggs: &mut Vec<AggCall>) -> Expr {
    for (i, g) in groups.iter().enumerate() {
        if same_expr(e, g) {
            return Expr::Col(i);
        }
    }
    let ng = groups.len();
    match e {
        Expr::Col(c) if *c >= AGG_BASE => Expr::Col(ng + (*c - AGG_BASE)),
        Expr::Col(c) => {
            aggs.push(AggCall::simple(AggFunc::AnyValue, Some(Expr::Col(*c)), false));
            Expr::Col(ng + aggs.len() - 1)
        }
        Expr::Lit(_) | Expr::Param(_) | Expr::Var(_) => e.clone(),
        _ => {
            // rebuild children
            let mut slot = |x: &Expr| Box::new(post_agg(x, groups, aggs));
            match e {
                Expr::Cmp(a, o, b) => Expr::Cmp(slot(a), *o, slot(b)),
                Expr::Arith(a, o, b) => Expr::Arith(slot(a), *o, slot(b)),
                Expr::And(a, b) => Expr::And(slot(a), slot(b)),
                Expr::Or(a, b) => Expr::Or(slot(a), slot(b)),
                Expr::Xor(a, b) => Expr::Xor(slot(a), slot(b)),
                Expr::Not(a) => Expr::Not(slot(a)),
                Expr::Neg(a) => Expr::Neg(slot(a)),
                Expr::IsNull(a, n) => Expr::IsNull(slot(a), *n),
                Expr::IsTrue(a, t, n) => Expr::IsTrue(slot(a), *t, *n),
                Expr::Like(a, b, n) => Expr::Like(slot(a), slot(b), *n),
                Expr::Between(a, b, c, n) => Expr::Between(slot(a), slot(b), slot(c), *n),
                Expr::Cast(a, t) => Expr::Cast(slot(a), *t),
                Expr::InList(a, l, n) => {
                    let a2 = slot(a);
                    Expr::InList(a2, l.iter().map(|x| post_agg(x, groups, aggs)).collect(), *n)
                }
                Expr::Func(f, args) => Expr::Func(*f, args.iter().map(|x| post_agg(x, groups, aggs)).collect()),
                Expr::Case(o, w, el) => {
                    let o2 = o.as_ref().map(|x| Box::new(post_agg(x, groups, aggs)));
                    let w2 = w
                        .iter()
                        .map(|(a, b)| (post_agg(a, groups, aggs), post_agg(b, groups, aggs)))
                        .collect();
                    let e2 = el.as_ref().map(|x| Box::new(post_agg(x, groups, aggs)));
                    Expr::Case(o2, w2, e2)
                }
                _ => e.clone(),
            }
        }
    }
}

fn limit_exprs(q: &A::Query, b: &mut Binder) -> SqlResult<(Option<Expr>, Option<Expr>)> {
    Ok(match &q.limit_clause {
        None => (None, None),
        Some(A::LimitClause::LimitOffset { limit, offset, .. }) => (
            match limit {
                Some(l) => Some(b.bind(l)?),
                None => None,
            },
            match offset {
                Some(o) => Some(b.bind(&o.value)?),
                None => None,
            },
        ),
        Some(A::LimitClause::OffsetCommaLimit { offset, limit }) => (Some(b.bind(limit)?), Some(b.bind(offset)?)),
    })
}

pub fn plan_query(q: &A::Query, ctx: &PlanCtx) -> SqlResult<SelectPlan> {
    let sel = match q.body.as_ref() {
        A::SetExpr::Select(s) => s,
        A::SetExpr::Query(inner) => return plan_query(inner, ctx),
        A::SetExpr::SetOperation { .. } => return Err(SqlError::unsupported("UNION")),
        other => return Err(SqlError::unsupported(format!("query {}", other))),
    };
    let (scope, joins) = build_scope(&sel.from, ctx)?;
    let mut b = Binder::new(&scope, ctx);

    // WHERE
    let mut conj = Vec::new();
    if let Some(w) = &sel.selection {
        split_and(b.bind(w)?, &mut conj);
    }
    // JOIN ... ON conditions
    let mut on_exprs: Vec<Option<Expr>> = Vec::new();
    for (_, on) in &joins {
        on_exprs.push(match on {
            Some(e) => Some(b.bind(e)?),
            None => None,
        });
    }

    // Projection (with aggregate collection)
    let mut aggs: Vec<AggCall> = Vec::new();
    let mut proj: Vec<Expr> = Vec::new();
    let mut names: Vec<(String, Option<(Arc<TableDef>, usize)>)> = Vec::new();
    {
        let mut pb = Binder {
            scope: &scope,
            ctx,
            aggs: Some(&mut aggs),
            values_offset: None,
            row_alias: None,
        };
        for item in &sel.projection {
            match item {
                A::SelectItem::Wildcard(_) => {
                    for t in &scope.tables {
                        for (i, c) in t.table.columns.iter().enumerate() {
                            proj.push(Expr::Col(t.offset + i));
                            names.push((c.name.clone(), Some((t.table.clone(), i))));
                        }
                    }
                }
                A::SelectItem::QualifiedWildcard(kind, _) => {
                    let qual = match kind {
                        A::SelectItemQualifiedWildcardKind::ObjectName(n) => {
                            object_name_parts(n).last().cloned().unwrap_or_default()
                        }
                        other => other.to_string(),
                    };
                    let t = scope
                        .tables
                        .iter()
                        .find(|t| t.alias.eq_ignore_ascii_case(&qual) || t.table.name.eq_ignore_ascii_case(&qual))
                        .ok_or_else(|| SqlError::new(1051, "42S02", format!("Unknown table '{}'", qual)))?;
                    for (i, c) in t.table.columns.iter().enumerate() {
                        proj.push(Expr::Col(t.offset + i));
                        names.push((c.name.clone(), Some((t.table.clone(), i))));
                    }
                }
                A::SelectItem::UnnamedExpr(e) => {
                    let be = pb.bind(e)?;
                    let src = match &be {
                        Expr::Col(c) if *c < AGG_BASE => col_source(&scope, *c),
                        _ => None,
                    };
                    names.push((expr_name(e, &[]), src));
                    proj.push(be);
                }
                A::SelectItem::ExprWithAlias { expr, alias } => {
                    let be = pb.bind(expr)?;
                    let src = match &be {
                        Expr::Col(c) if *c < AGG_BASE => col_source(&scope, *c),
                        _ => None,
                    };
                    names.push((alias.value.clone(), src));
                    proj.push(be);
                }
                other => return Err(SqlError::unsupported(format!("select item {}", other))),
            }
        }
    }

    // GROUP BY (may reference select aliases / positions)
    let mut groups: Vec<Expr> = Vec::new();
    if let A::GroupByExpr::Expressions(exprs, _) = &sel.group_by {
        for g in exprs {
            groups.push(resolve_alias_or_position(g, &sel.projection, &proj, &mut b, &mut None)?);
        }
    } else {
        return Err(SqlError::unsupported("GROUP BY ALL"));
    }
    if groups.iter().any(|g| {
        let mut c = Vec::new();
        g.columns(&mut c);
        c.iter().any(|&x| x >= AGG_BASE)
    }) {
        return Err(SqlError::new(1056, "42000", "Can't group on an aggregate"));
    }

    // HAVING and ORDER BY may use aggregates too.
    let mut having = None;
    let mut order: Vec<(Expr, bool)> = Vec::new();
    {
        let mut hb = Binder {
            scope: &scope,
            ctx,
            aggs: Some(&mut aggs),
            values_offset: None,
            row_alias: None,
        };
        if let Some(h) = &sel.having {
            having = Some(resolve_alias_or_position(
                h,
                &sel.projection,
                &proj,
                &mut hb,
                &mut Some(()),
            )?);
        }
        if let Some(ob) = &q.order_by {
            if let A::OrderByKind::Expressions(list) = &ob.kind {
                for o in list {
                    let e = resolve_alias_or_position(&o.expr, &sel.projection, &proj, &mut hb, &mut Some(()))?;
                    let desc = matches!(o.options.sort, Some(A::OrderBySort::Desc));
                    order.push((e, desc));
                }
            }
        }
    }

    let aggregated = !aggs.is_empty() || !groups.is_empty();
    let mut agg_plan = None;
    if aggregated {
        let mut all_aggs = aggs.clone();
        let proj2: Vec<Expr> = proj.iter().map(|e| post_agg(e, &groups, &mut all_aggs)).collect();
        let having2 = having.as_ref().map(|h| post_agg(h, &groups, &mut all_aggs));
        let order2: Vec<(Expr, bool)> = order
            .iter()
            .map(|(e, d)| (post_agg(e, &groups, &mut all_aggs), *d))
            .collect();
        proj = proj2;
        having = having2;
        order = order2;
        agg_plan = Some(AggPlan {
            group: groups,
            aggs: all_aggs,
        });
    } else if having.is_some() {
        // HAVING without aggregation behaves like WHERE on the projection
    }

    // Result metadata
    let types_in: Vec<ExprType> = if let Some(ap) = &agg_plan {
        let src = scope.types();
        let mut t: Vec<ExprType> = ap.group.iter().map(|g| infer(g, &src, ctx.params)).collect();
        for a in &ap.aggs {
            t.push(agg_type(a, &src, ctx.params));
        }
        t
    } else {
        scope.types()
    };
    let mut columns = Vec::new();
    for (i, e) in proj.iter().enumerate() {
        let (name, src) = &names[i];
        let mut c = match src {
            Some((t, ci)) => {
                let cd = &t.columns[*ci];
                cd.ty().result_column(name, &t.name, cd.nullable, t.is_pk_col(*ci))
            }
            None => {
                let et = infer(e, &types_in, ctx.params);
                let mut c = match &et.sql {
                    Some(st) => st.result_column(name, "", et.nullable, false),
                    None => Column::new(name, et.ty),
                };
                c.decimals = et.decimals;
                c
            }
        };
        if let Some((t, _)) = src {
            c.schema = t.db.clone();
        }
        columns.push(c);
    }

    let (limit, offset) = {
        let empty = Scope::default();
        let mut lb = Binder::new(&empty, ctx);
        limit_exprs(q, &mut lb)?
    };

    // FROM: access paths
    let source = build_source(&scope, &joins, &on_exprs, &conj)?;
    let filter = join_and(conj.clone());

    // Push LIMIT to storage when every row read is a result row.
    let mut source = source;
    if let Source::Table(t) = &mut source {
        if filter.is_none() && !aggregated && order.is_empty() && !matches!(sel.distinct, Some(A::Distinct::Distinct)) {
            if let Some(l) = &limit {
                t.limit = Some(match &offset {
                    Some(o) => Expr::Arith(Box::new(l.clone()), Arith::Add, Box::new(o.clone())),
                    None => l.clone(),
                });
            }
        }
    }

    Ok(SelectPlan {
        source,
        filter,
        agg: agg_plan,
        having,
        order,
        project: proj,
        columns: columns.into(),
        distinct: matches!(sel.distinct, Some(A::Distinct::Distinct)),
        limit,
        offset,
        lock: q.locks.first().map(|l| RowLock {
            exclusive: q.locks.iter().any(|l| matches!(l.lock_type, A::LockType::Update)),
            nowait: matches!(l.nonblock, Some(A::NonBlock::Nowait)),
            skip: matches!(l.nonblock, Some(A::NonBlock::SkipLocked)),
        }),
    })
}

fn col_source(scope: &Scope, c: usize) -> Option<(Arc<TableDef>, usize)> {
    scope
        .tables
        .iter()
        .find(|t| c >= t.offset && c < t.offset + t.table.columns.len())
        .map(|t| (t.table.clone(), c - t.offset))
}

fn agg_type(a: &AggCall, src: &[ExprType], params: &[Value]) -> ExprType {
    use ColumnType as C;
    let arg = a.arg.as_ref().map(|e| infer(e, src, params));
    match a.func {
        AggFunc::CountStar | AggFunc::Count => {
            let mut t = ExprType::of(C::LongLong);
            t.nullable = false;
            t
        }
        AggFunc::Sum => match &arg {
            Some(t) if matches!(t.ty, C::Double | C::Float) => ExprType::of(C::Double),
            Some(t) => {
                let mut r = ExprType::of(C::NewDecimal);
                r.decimals = t.decimals;
                r
            }
            None => ExprType::of(C::NewDecimal),
        },
        AggFunc::Avg => match &arg {
            Some(t) if matches!(t.ty, C::Double | C::Float) => ExprType::of(C::Double),
            Some(t) => {
                let mut r = ExprType::of(C::NewDecimal);
                r.decimals = t.decimals + 4;
                r
            }
            None => ExprType::of(C::NewDecimal),
        },
        AggFunc::Min | AggFunc::Max | AggFunc::AnyValue => arg.unwrap_or_else(|| ExprType::of(C::VarString)),
        AggFunc::GroupConcat => ExprType::of(C::Blob),
    }
}

/// ORDER BY / GROUP BY / HAVING items may name a select alias or position.
fn resolve_alias_or_position(
    e: &A::Expr,
    items: &[A::SelectItem],
    proj: &[Expr],
    b: &mut Binder,
    _agg_ok: &mut Option<()>,
) -> SqlResult<Expr> {
    if let A::Expr::Value(v) = e {
        if let A::Value::Number(n, _) = &v.value {
            let pos: usize = n.parse().map_err(|_| SqlError::parse("bad ORDER BY position"))?;
            return proj
                .get(pos.wrapping_sub(1))
                .cloned()
                .ok_or_else(|| SqlError::new(1054, "42S22", format!("Unknown column '{}' in 'order clause'", pos)));
        }
    }
    if let A::Expr::Identifier(i) = e {
        // a select alias wins over a source column of the same name only if
        // the name is not a plain column of the FROM tables
        let mut k = 0usize;
        for item in items {
            match item {
                A::SelectItem::ExprWithAlias { alias, .. } => {
                    if alias.value.eq_ignore_ascii_case(&i.value) && b.resolve(None, &i.value).is_err() {
                        return Ok(proj[k].clone());
                    }
                    k += 1;
                }
                A::SelectItem::UnnamedExpr(_) => k += 1,
                _ => return b.bind(e),
            }
        }
    }
    // Aliases nested inside an expression (HAVING n > 7, ORDER BY n + 1).
    let e = subst_aliases(e, items, b);
    b.bind(&e)
}

/// Replace identifiers that name a select-list alias (and no source column)
/// by the aliased expression.
fn subst_aliases(e: &A::Expr, items: &[A::SelectItem], b: &Binder) -> A::Expr {
    let mut e = e.clone();
    let _ = A::visit_expressions_mut(&mut e, |x| {
        if let A::Expr::Identifier(i) = x {
            if b.resolve(None, &i.value).is_err() {
                let sub = items.iter().find_map(|it| match it {
                    A::SelectItem::ExprWithAlias { expr, alias } if alias.value.eq_ignore_ascii_case(&i.value) => {
                        Some(expr.clone())
                    }
                    _ => None,
                });
                if let Some(sub) = sub {
                    *x = A::Expr::Nested(Box::new(sub));
                }
            }
        }
        std::ops::ControlFlow::<()>::Continue(())
    });
    e
}

fn build_source(
    scope: &Scope,
    joins: &[(JoinKind, Option<A::Expr>)],
    on: &[Option<Expr>],
    conj: &[Expr],
) -> SqlResult<Source> {
    if scope.tables.is_empty() {
        return Ok(Source::Dual);
    }
    let t0 = &scope.tables[0];
    let mut src = Source::Table(TableAccess {
        table: t0.table.clone(),
        alias: t0.alias.clone(),
        path: choose_path(&t0.table, 0, conj),
        offset: 0,
        limit: None,
    });
    for (i, t) in scope.tables.iter().enumerate().skip(1) {
        let (kind, _) = joins[i - 1];
        let on_e = on[i - 1].clone();
        // inner joins may use WHERE conjuncts for the lookup; left joins only ON
        let mut cands = Vec::new();
        if let Some(o) = &on_e {
            split_and(o.clone(), &mut cands);
        }
        if kind == JoinKind::Inner {
            cands.extend(conj.iter().cloned());
        }
        let path = choose_path(&t.table, t.offset, &cands);
        src = Source::Join {
            left: Box::new(src),
            right: TableAccess {
                table: t.table.clone(),
                alias: t.alias.clone(),
                path,
                offset: t.offset,
                limit: None,
            },
            kind,
            on: on_e,
        };
    }
    Ok(src)
}

// ---------------------------------------------------------------------------
// DML
// ---------------------------------------------------------------------------

fn single_table_scope(table: Arc<TableDef>, alias: Option<String>) -> Scope {
    Scope {
        tables: vec![ScopeTable {
            alias: alias.unwrap_or_else(|| table.name.clone()),
            table,
            offset: 0,
        }],
    }
}

fn plan_insert(ins: &A::Insert, ctx: &PlanCtx) -> SqlResult<InsertPlan> {
    let name = match &ins.table {
        A::TableObject::TableName(n) => n,
        _ => return Err(SqlError::unsupported("INSERT target")),
    };
    let table = lookup_table(name, ctx)?;
    let ncols = table.columns.len();
    let targets: Vec<usize> = if ins.columns.is_empty() {
        (0..ncols).collect()
    } else {
        ins.columns
            .iter()
            .map(|c| {
                let n = object_name_parts(c).last().cloned().unwrap_or_default();
                table.col_index(&n).ok_or_else(|| unknown_col(&n))
            })
            .collect::<SqlResult<_>>()?
    };
    let empty = Scope::default();
    let source = match ins.source.as_deref() {
        Some(q) => match q.body.as_ref() {
            A::SetExpr::Values(v) => {
                let mut rows = Vec::new();
                let mut b = Binder::new(&empty, ctx);
                for r in &v.rows {
                    let vals = &r.content;
                    if vals.len() != targets.len() {
                        return Err(SqlError::new(
                            1136,
                            "21S01",
                            "Column count doesn't match value count at row 1",
                        ));
                    }
                    let mut row: Vec<Option<Expr>> = vec![None; ncols];
                    for (k, e) in vals.iter().enumerate() {
                        let is_default = matches!(e, A::Expr::Identifier(i) if i.value.eq_ignore_ascii_case("DEFAULT"));
                        row[targets[k]] = if is_default { None } else { Some(b.bind(e)?) };
                    }
                    rows.push(row);
                }
                InsertSource::Values(rows)
            }
            _ => {
                let sp = plan_query(q, ctx)?;
                if sp.project.len() != targets.len() {
                    return Err(SqlError::new(
                        1136,
                        "21S01",
                        "Column count doesn't match value count at row 1",
                    ));
                }
                InsertSource::Select(Box::new(sp), targets.clone())
            }
        },
        None => {
            if ins.assignments.is_empty() {
                return Err(SqlError::parse("INSERT without values"));
            }
            // INSERT ... SET a = 1, b = 2
            let mut row: Vec<Option<Expr>> = vec![None; ncols];
            let mut b = Binder::new(&empty, ctx);
            for a in &ins.assignments {
                let c = assignment_col(&table, &a.target)?;
                row[c] = Some(b.bind(&a.value)?);
            }
            InsertSource::Values(vec![row])
        }
    };
    let mut on_dup = Vec::new();
    if let Some(A::OnInsert::DuplicateKeyUpdate(assigns)) = &ins.on {
        let scope = single_table_scope(table.clone(), None);
        let row_alias = match &ins.insert_alias {
            Some(a) => Some(RowAlias {
                name: object_name_parts(&a.row_alias).last().cloned().unwrap_or_default(),
                explicit: a.col_aliases.as_ref().is_some_and(|cs| !cs.is_empty()),
                cols: match &a.col_aliases {
                    Some(cs) if !cs.is_empty() => {
                        if cs.len() != targets.len() {
                            return Err(SqlError::new(
                                1136,
                                "21S01",
                                "Column count doesn't match value count at row 1",
                            ));
                        }
                        cs.iter()
                            .map(|c| c.value.clone())
                            .zip(targets.iter().copied())
                            .collect()
                    }
                    _ => table
                        .columns
                        .iter()
                        .enumerate()
                        .map(|(i, c)| (c.name.clone(), i))
                        .collect(),
                },
            }),
            None => None,
        };
        let mut b = Binder {
            scope: &scope,
            ctx,
            aggs: None,
            values_offset: Some(ncols),
            row_alias: row_alias.as_ref(),
        };
        for a in assigns {
            let c = assignment_col(&table, &a.target)?;
            on_dup.push((c, b.bind(&a.value)?));
        }
    }
    Ok(InsertPlan {
        table,
        source,
        replace: ins.replace_into,
        ignore: ins.ignore,
        on_dup,
    })
}

fn assignment_col(table: &TableDef, t: &A::AssignmentTarget) -> SqlResult<usize> {
    match t {
        A::AssignmentTarget::ColumnName(n) => {
            let name = object_name_parts(n).last().cloned().unwrap_or_default();
            table.col_index(&name).ok_or_else(|| unknown_col(&name))
        }
        A::AssignmentTarget::Tuple(_) => Err(SqlError::unsupported("tuple assignment")),
    }
}

fn order_by_list(list: &[A::OrderByExpr], b: &mut Binder) -> SqlResult<Vec<(Expr, bool)>> {
    list.iter()
        .map(|o| Ok((b.bind(&o.expr)?, matches!(o.options.sort, Some(A::OrderBySort::Desc)))))
        .collect()
}

fn plan_update(u: &A::Update, ctx: &PlanCtx) -> SqlResult<UpdatePlan> {
    if !u.table.joins.is_empty() || u.from.is_some() {
        return Err(SqlError::unsupported("multi-table UPDATE"));
    }
    let (table, alias) = match &u.table.relation {
        A::TableFactor::Table { name, alias, .. } => (lookup_table(name, ctx)?, alias.as_ref().map(|a| ident(&a.name))),
        _ => return Err(SqlError::unsupported("UPDATE target")),
    };
    let scope = single_table_scope(table.clone(), alias.clone());
    let mut b = Binder::new(&scope, ctx);
    let mut conj = Vec::new();
    if let Some(w) = &u.selection {
        split_and(b.bind(w)?, &mut conj);
    }
    let mut set = Vec::new();
    for a in &u.assignments {
        let c = assignment_col(&table, &a.target)?;
        set.push((c, b.bind(&a.value)?));
    }
    let order = order_by_list(&u.order_by, &mut b)?;
    let limit = match &u.limit {
        Some(l) => Some(b.bind(l)?),
        None => None,
    };
    let path = choose_path(&table, 0, &conj);
    Ok(UpdatePlan {
        access: TableAccess {
            table: table.clone(),
            alias: alias.unwrap_or_else(|| table.name.clone()),
            path,
            offset: 0,
            limit: None,
        },
        filter: join_and(conj),
        set,
        order,
        limit,
    })
}

fn plan_delete(d: &A::Delete, ctx: &PlanCtx) -> SqlResult<DeletePlan> {
    let from = match &d.from {
        A::FromTable::WithFromKeyword(f) | A::FromTable::WithoutKeyword(f) => f,
    };
    if from.len() != 1 || !from[0].joins.is_empty() || !d.tables.is_empty() || d.using.is_some() {
        return Err(SqlError::unsupported("multi-table DELETE"));
    }
    let (table, alias) = match &from[0].relation {
        A::TableFactor::Table { name, alias, .. } => (lookup_table(name, ctx)?, alias.as_ref().map(|a| ident(&a.name))),
        _ => return Err(SqlError::unsupported("DELETE target")),
    };
    let scope = single_table_scope(table.clone(), alias.clone());
    let mut b = Binder::new(&scope, ctx);
    let mut conj = Vec::new();
    if let Some(w) = &d.selection {
        split_and(b.bind(w)?, &mut conj);
    }
    let order = order_by_list(&d.order_by, &mut b)?;
    let limit = match &d.limit {
        Some(l) => Some(b.bind(l)?),
        None => None,
    };
    let path = choose_path(&table, 0, &conj);
    Ok(DeletePlan {
        access: TableAccess {
            table: table.clone(),
            alias: alias.unwrap_or_else(|| table.name.clone()),
            path,
            offset: 0,
            limit: None,
        },
        filter: join_and(conj),
        order,
        limit,
    })
}

// ---------------------------------------------------------------------------
// DDL
// ---------------------------------------------------------------------------

/// One column definition of CREATE TABLE or ALTER TABLE.
pub(crate) struct ColumnSpec {
    pub def: ColumnDef,
    /// PRIMARY KEY column option
    pub primary: bool,
    /// UNIQUE column option
    pub unique: bool,
}

/// Convert a parsed column definition.
pub(crate) fn column_spec(c: &A::ColumnDef) -> SqlResult<ColumnSpec> {
    let ty = SqlType::parse(&c.data_type.to_string())?;
    let mut nullable = true;
    let mut default = None;
    let mut default_null = false;
    let mut ai = false;
    let (mut primary, mut unique) = (false, false);
    for o in &c.options {
        match &o.option {
            A::ColumnOption::NotNull => nullable = false,
            A::ColumnOption::Null => nullable = true,
            A::ColumnOption::Default(e) => {
                default = None;
                default_null = false;
                match e {
                    A::Expr::Value(v) if matches!(v.value, A::Value::Null) => default_null = true,
                    A::Expr::Value(v) => {
                        default = Some(match &v.value {
                            A::Value::SingleQuotedString(s) | A::Value::DoubleQuotedString(s) => s.clone(),
                            A::Value::Number(n, _) => n.clone(),
                            A::Value::Boolean(b) => (*b as i32).to_string(),
                            other => other.to_string(),
                        })
                    }
                    A::Expr::UnaryOp {
                        op: A::UnaryOperator::Minus,
                        expr,
                    } => default = Some(format!("-{}", expr)),
                    other => {
                        let s = other.to_string().to_ascii_uppercase();
                        if s.starts_with("CURRENT_TIMESTAMP") || s.starts_with("NOW") {
                            default = Some("CURRENT_TIMESTAMP".into());
                        } else {
                            return Err(SqlError::unsupported(format!("DEFAULT {}", other)));
                        }
                    }
                }
            }
            A::ColumnOption::PrimaryKey(_) => {
                primary = true;
                nullable = false;
            }
            A::ColumnOption::Unique(_) => unique = true,
            A::ColumnOption::DialectSpecific(toks) => {
                let s: String = toks
                    .iter()
                    .map(|t| t.to_string())
                    .collect::<Vec<_>>()
                    .join(" ")
                    .to_ascii_uppercase();
                if s.contains("AUTO_INCREMENT") {
                    ai = true;
                }
            }
            A::ColumnOption::Comment(_)
            | A::ColumnOption::CharacterSet(_)
            | A::ColumnOption::Collation(_)
            | A::ColumnOption::OnUpdate(_) => {}
            other => return Err(SqlError::unsupported(format!("column option {}", other))),
        }
    }
    if ai && !ty.is_integer() {
        return Err(SqlError::new(
            1063,
            "42000",
            format!("Incorrect column specifier for column '{}'", c.name.value),
        ));
    }
    if let Some(d) = &default {
        if d != "CURRENT_TIMESTAMP" {
            ty.coerce(Value::str(d), &c.name.value)
                .map_err(|_| SqlError::new(1067, "42000", format!("Invalid default value for '{}'", c.name.value)))?;
        }
    }
    if default_null && !nullable {
        return Err(SqlError::new(
            1067,
            "42000",
            format!("Invalid default value for '{}'", c.name.value),
        ));
    }
    Ok(ColumnSpec {
        def: ColumnDef {
            name: c.name.value.clone(),
            ty: ty.display(),
            nullable,
            default,
            default_null,
            auto_increment: ai,
            field: None,
            storage_nullable: None,
            fill: None,
            sql_type: Some(ty),
        },
        primary,
        unique,
    })
}

/// Column names of an index definition (`col(10)` prefixes index the
/// whole column).
pub(crate) fn index_col_names(list: &[A::IndexColumn]) -> SqlResult<Vec<String>> {
    list.iter()
        .map(|ic| match &ic.column.expr {
            A::Expr::Identifier(i) => Ok(i.value.clone()),
            A::Expr::Function(f) => Ok(object_name_parts(&f.name).join(".")),
            other => Err(SqlError::unsupported(format!("index expression {}", other))),
        })
        .collect()
}

/// A FOREIGN KEY constraint as written (name None: generated).
pub(crate) fn foreign_key_spec(
    fk: &A::ForeignKeyConstraint,
    ctx: &PlanCtx,
) -> SqlResult<(Option<String>, ForeignKeyDef)> {
    let (ref_db, ref_table) = table_name(&fk.foreign_table, ctx)?;
    let action = |a: &Option<A::ReferentialAction>| a.as_ref().map(|a| a.to_string());
    let name = fk.name.as_ref().or(fk.index_name.as_ref()).map(ident);
    Ok((
        name.clone(),
        ForeignKeyDef {
            name: name.unwrap_or_default(),
            cols: fk.columns.iter().map(ident).collect(),
            ref_db,
            ref_table,
            ref_cols: fk.referred_columns.iter().map(ident).collect(),
            on_delete: action(&fk.on_delete),
            on_update: action(&fk.on_update),
        },
    ))
}

/// MySQL's name for an index nobody named: its first column, made unique
/// with _2, _3, ...
pub(crate) fn generated_index_name(first_col: &str, taken: &dyn Fn(&str) -> bool) -> String {
    if !taken(first_col) && !first_col.eq_ignore_ascii_case("PRIMARY") {
        return first_col.to_string();
    }
    (2..)
        .map(|i| format!("{}_{}", first_col, i))
        .find(|n| !taken(n))
        .expect("a free name")
}

/// Record a foreign key on `def`: names an unnamed one `<table>_ibfk_<n>`
/// and, as MySQL does, adds an index on its columns unless one starts with
/// them. Not enforced.
pub(crate) fn add_foreign_key(def: &mut TableDef, name: Option<String>, mut fk: ForeignKeyDef) -> SqlResult<()> {
    let mut cols = Vec::new();
    for k in 0..fk.cols.len() {
        let c = def.col_index(&fk.cols[k]).ok_or_else(|| {
            SqlError::new(
                1072,
                "42000",
                format!("Key column '{}' doesn't exist in table", fk.cols[k]),
            )
        })?;
        fk.cols[k] = def.columns[c].name.clone();
        cols.push(c);
    }
    fk.name = match name {
        Some(n) => {
            if def.foreign_keys.iter().any(|f| f.name.eq_ignore_ascii_case(&n)) {
                return Err(SqlError::new(
                    1826,
                    "HY000",
                    format!("Duplicate foreign key constraint name '{}'", n),
                ));
            }
            n
        }
        None => {
            let prefix = format!("{}_ibfk_", def.name);
            let n = def
                .foreign_keys
                .iter()
                .filter_map(|f| f.name.strip_prefix(&prefix).and_then(|x| x.parse::<u32>().ok()))
                .max()
                .unwrap_or(0);
            format!("{}{}", prefix, n + 1)
        }
    };
    let covered = def
        .indexes
        .iter()
        .any(|ix| ix.cols.len() >= cols.len() && ix.cols[..cols.len()] == cols[..]);
    if !covered {
        let explicit = !fk.name.starts_with(&format!("{}_ibfk_", def.name));
        let iname = if explicit && def.index_by_name(&fk.name).is_none() {
            fk.name.clone()
        } else {
            let first = def.columns[cols[0]].name.clone();
            generated_index_name(&first, &|n| def.index_by_name(n).is_some())
        };
        let id = next_index_id(def);
        def.indexes.push(IndexDef {
            name: iname,
            id,
            cols,
            unique: false,
            invisible: false,
        });
    }
    def.foreign_keys.push(fk);
    Ok(())
}

/// Tarantool id for a new index: past every index id in use, including the
/// bucket_id index.
pub(crate) fn next_index_id(def: &TableDef) -> u32 {
    let bucket_ix = def.bucket_index.unwrap_or(0);
    def.indexes
        .iter()
        .map(|i| i.id)
        .chain(std::iter::once(bucket_ix))
        .max()
        .unwrap_or(0)
        + 1
}

/// Remove MySQL's index visibility options (`KEY k (a) INVISIBLE`), which
/// the parser does not know, from a CREATE TABLE; returns the text and the
/// names of the invisible indexes.
pub(crate) fn strip_index_visibility(sql: &str) -> (String, Vec<String>) {
    let up = sql.to_ascii_uppercase();
    if !up.contains("VISIBLE") {
        return (sql.to_string(), Vec::new());
    }
    let b = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut invisible = Vec::new();
    let (mut i, mut copied, mut depth) = (0usize, 0usize, 0i32);
    // Words of the current element of the table body (depth 1).
    let mut elem: Vec<String> = Vec::new();
    while i < b.len() {
        let c = b[i];
        if c == b'\'' || c == b'"' || c == b'`' {
            let q = c;
            let mut j = i + 1;
            while j < b.len() && b[j] != q {
                if b[j] == b'\\' {
                    j += 1;
                }
                j += 1;
            }
            if depth == 1 && q == b'`' {
                elem.push(sql[i + 1..j.min(b.len())].to_string());
            }
            i = j + 1;
            continue;
        }
        if c.is_ascii_alphanumeric() || c == b'_' {
            let s0 = i;
            while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_' || b[i] == b'$') {
                i += 1;
            }
            let w = &up[s0..i];
            if depth == 1 {
                let is_index = elem.first().is_some_and(|f| {
                    matches!(
                        f.to_ascii_uppercase().as_str(),
                        "KEY" | "INDEX" | "UNIQUE" | "CONSTRAINT" | "FULLTEXT" | "SPATIAL" | "PRIMARY"
                    )
                });
                if is_index && (w == "VISIBLE" || w == "INVISIBLE") {
                    if w == "INVISIBLE" {
                        // the name follows KEY / INDEX (UNIQUE [KEY|INDEX])
                        let mut it = elem.iter().skip_while(|x| {
                            matches!(
                                x.to_ascii_uppercase().as_str(),
                                "UNIQUE" | "KEY" | "INDEX" | "CONSTRAINT"
                            )
                        });
                        if let Some(n) = it.next() {
                            invisible.push(n.clone());
                        }
                    }
                    out.push_str(&sql[copied..s0]);
                    copied = i;
                    continue;
                }
                elem.push(sql[s0..i].to_string());
            }
            continue;
        }
        match c {
            b'(' => depth += 1,
            b')' => depth -= 1,
            b',' if depth == 1 => elem.clear(),
            _ => {}
        }
        i += 1;
    }
    out.push_str(&sql[copied..]);
    (out, invisible)
}

fn plan_create_table(
    ct: &A::CreateTable,
    partition_key: Option<Vec<String>>,
    invisible: &[String],
    ctx: &PlanCtx,
) -> SqlResult<Stmt> {
    if ct.query.is_some() || ct.like.is_some() {
        return Err(SqlError::unsupported("CREATE TABLE ... AS / LIKE"));
    }
    let (db, name) = table_name(&ct.name, ctx)?;
    let mut columns: Vec<ColumnDef> = Vec::new();
    let mut pk: Option<Vec<usize>> = None;
    let mut secondary: Vec<(Option<String>, Vec<usize>, bool)> = Vec::new();
    let mut auto_inc = None;
    for (i, c) in ct.columns.iter().enumerate() {
        let spec = column_spec(c)?;
        if columns.iter().any(|x| x.name.eq_ignore_ascii_case(&spec.def.name)) {
            return Err(SqlError::new(
                1060,
                "42S21",
                format!("Duplicate column name '{}'", spec.def.name),
            ));
        }
        if spec.primary {
            pk = Some(vec![i]);
        }
        if spec.unique {
            secondary.push((Some(spec.def.name.clone()), vec![i], true));
        }
        if spec.def.auto_increment {
            if auto_inc.is_some() {
                return Err(SqlError::new(
                    1075,
                    "42000",
                    "Incorrect table definition; there can be only one auto column and it must be defined as a key",
                ));
            }
            auto_inc = Some(i);
        }
        columns.push(spec.def);
    }
    let col_ix = |n: &str, cols: &[ColumnDef]| -> SqlResult<usize> {
        cols.iter()
            .position(|c| c.name.eq_ignore_ascii_case(n))
            .ok_or_else(|| SqlError::new(1072, "42000", format!("Key column '{}' doesn't exist in table", n)))
    };
    let index_cols = |list: &[A::IndexColumn], cols: &[ColumnDef]| -> SqlResult<Vec<usize>> {
        index_col_names(list)?.iter().map(|n| col_ix(n, cols)).collect()
    };
    let mut fks = Vec::new();
    for con in &ct.constraints {
        match con {
            A::TableConstraint::PrimaryKey(p) => pk = Some(index_cols(&p.columns, &columns)?),
            A::TableConstraint::Unique(u) => {
                let name = u.name.as_ref().or(u.index_name.as_ref()).map(|n| n.value.clone());
                secondary.push((name, index_cols(&u.columns, &columns)?, true));
            }
            A::TableConstraint::Index(ix) => {
                secondary.push((
                    ix.name.as_ref().map(|n| n.value.clone()),
                    index_cols(&ix.columns, &columns)?,
                    false,
                ));
            }
            A::TableConstraint::ForeignKey(fk) => fks.push(foreign_key_spec(fk, ctx)?),
            A::TableConstraint::Check(_) => {} // accepted, not enforced
            other => return Err(SqlError::unsupported(format!("table constraint {}", other))),
        }
    }
    let pk = pk.ok_or_else(|| SqlError::new(1173, "42000", "Tarn tables require a PRIMARY KEY"))?;
    for &c in &pk {
        columns[c].nullable = false;
    }
    let mut indexes = vec![IndexDef {
        name: "PRIMARY".into(),
        id: 0,
        cols: pk.clone(),
        unique: true,
        invisible: false,
    }];
    for (n, cols, unique) in secondary.into_iter() {
        let name = match n {
            Some(n) => {
                if indexes.iter().any(|i| i.name.eq_ignore_ascii_case(&n)) {
                    return Err(SqlError::new(1061, "42000", format!("Duplicate key name '{}'", n)));
                }
                n
            }
            None => generated_index_name(&columns[cols[0]].name, &|x| {
                indexes.iter().any(|i| i.name.eq_ignore_ascii_case(x))
            }),
        };
        let invisible = invisible.iter().any(|x| x.eq_ignore_ascii_case(&name));
        indexes.push(IndexDef {
            name,
            id: indexes.len() as u32,
            cols,
            unique,
            invisible,
        });
    }
    let shard_key = match partition_key {
        Some(names) => names
            .iter()
            .map(|n| col_ix(n, &columns))
            .collect::<SqlResult<Vec<_>>>()?,
        None => pk.clone(),
    };
    let mut auto_increment = None;
    let mut comment = String::new();
    if let A::CreateTableOptions::Plain(opts) = &ct.table_options {
        for o in opts {
            match o {
                A::SqlOption::KeyValue { key, value } if key.value.eq_ignore_ascii_case("AUTO_INCREMENT") => {
                    auto_increment = value.to_string().trim_matches('\'').parse::<u64>().ok();
                }
                A::SqlOption::Comment(A::CommentDef::WithEq(c) | A::CommentDef::WithoutEq(c)) => comment = c.clone(),
                _ => {}
            }
        }
    }
    if let Some(A::CommentDef::WithEq(c) | A::CommentDef::WithoutEq(c)) = &ct.comment {
        comment = c.clone();
    }
    let mut def = TableDef {
        db,
        name,
        space_id: 0,
        columns,
        indexes,
        shard_key,
        auto_inc,
        comment,
        ver: 0,
        bucket: None,
        dropped: Vec::new(),
        foreign_keys: Vec::new(),
        bucket_index: None,
        layout: Default::default(),
    };
    // Index ids so far are 0..n; the bucket_id index takes n at creation,
    // so indexes a foreign key adds come after it.
    def.bucket_index = Some(def.indexes.len() as u32);
    let n_declared = def.indexes.len();
    for (n, fk) in fks {
        add_foreign_key(&mut def, n, fk)?;
    }
    // Implicit foreign key indexes are ordinary secondary indexes created
    // with the table: renumber them before the bucket_id index.
    for (k, ix) in def.indexes.iter_mut().enumerate().skip(n_declared) {
        ix.id = k as u32;
    }
    def.bucket_index = None;
    if let Some(ai) = def.auto_inc {
        if !def.indexes.iter().any(|ix| ix.cols.first() == Some(&ai)) {
            return Err(SqlError::new(
                1075,
                "42000",
                "Incorrect table definition; there can be only one auto column and it must be defined as a key",
            ));
        }
    }
    def.resolve_types()?;
    catalog::validate(&def)?;
    Ok(Stmt::CreateTable {
        def: Box::new(def),
        if_not_exists: ct.if_not_exists,
        auto_increment,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::normalize::{normalize, Mode};

    fn catalog() -> Catalog {
        let ct = "CREATE TABLE sbtest1 (id INTEGER NOT NULL AUTO_INCREMENT, k INTEGER DEFAULT '0' NOT NULL, \
                  c CHAR(120) DEFAULT '' NOT NULL, pad CHAR(60) DEFAULT '' NOT NULL, PRIMARY KEY (id), KEY k_1 (k))";
        let c0 = Catalog::default();
        let ctx = PlanCtx {
            catalog: &c0,
            db: Some("sbtest"),
            params: &[],
        };
        let Stmt::CreateTable { mut def, .. } = plan(ct, &ctx).unwrap() else {
            panic!()
        };
        def.space_id = 512;
        Catalog::new(1, vec!["sbtest".into()], vec![*def])
    }

    fn plan_sel(sql: &str) -> SelectPlan {
        let cat = catalog();
        let n = normalize(sql, Mode::Literals).unwrap();
        let ctx = PlanCtx {
            catalog: &cat,
            db: Some("sbtest"),
            params: &n.params,
        };
        match plan(&n.template, &ctx).unwrap() {
            Stmt::Select(s) => (*s).clone(),
            _ => panic!(),
        }
    }

    fn path_of(sql: &str) -> AccessPath {
        match plan_sel(sql).source {
            Source::Table(t) => t.path,
            _ => panic!(),
        }
    }

    #[test]
    fn upsert_row_alias() {
        let cat = catalog();
        let ctx = PlanCtx {
            catalog: &cat,
            db: Some("sbtest"),
            params: &[],
        };
        let ins = |sql: &str| match plan(sql, &ctx) {
            Ok(Stmt::Insert(p)) => Ok(p.on_dup.clone()),
            Ok(_) => panic!(),
            Err(e) => Err(e.code),
        };
        // columns: id 0, k 1, c 2, pad 3; the proposed row starts at 4
        let d = ins("INSERT INTO sbtest1 (id, k) VALUES (1, 2) AS new ON DUPLICATE KEY UPDATE k = sbtest1.k + new.k")
            .unwrap();
        assert!(matches!(&d[0].1, Expr::Arith(a, _, b) if matches!(**a, Expr::Col(1)) && matches!(**b, Expr::Col(5))));
        let d = ins("INSERT INTO sbtest1 (id, k) VALUES (1, 2) AS v(i, kk) ON DUPLICATE KEY UPDATE k = kk").unwrap();
        assert!(matches!(d[0].1, Expr::Col(5)));
        assert_eq!(
            ins("INSERT INTO sbtest1 (id, k) VALUES (1, 2) AS v ON DUPLICATE KEY UPDATE k = v.nope").err(),
            Some(1054)
        );
        assert_eq!(
            ins("INSERT INTO sbtest1 (id, k) VALUES (1, 2) AS v(i) ON DUPLICATE KEY UPDATE k = i").err(),
            Some(1136)
        );
    }

    #[test]
    fn multi_key_paths() {
        // issue #10: IN, OR and row-value IN become lookups, not scans
        assert!(matches!(path_of("SELECT c FROM sbtest1 WHERE k IN (1, 2, 3)"),
                         AccessPath::Prefixes { index: 1, keys } if keys.len() == 3));
        assert!(matches!(path_of("SELECT c FROM sbtest1 WHERE k = 1 OR k = 2"),
                         AccessPath::Prefixes { index: 1, keys } if keys.len() == 2));
        assert!(
            matches!(path_of("SELECT c FROM sbtest1 WHERE (id = 1 OR id = 9) AND k > 0"),
                         AccessPath::Point { index: 0, keys } if keys.len() == 2)
        );
        assert!(
            matches!(path_of("SELECT c FROM sbtest1 WHERE (id, k) IN ((1, 2), (3, 4))"),
                         AccessPath::Point { index: 0, keys } if keys.len() == 2)
        );
        assert!(matches!(
            path_of("SELECT c FROM sbtest1 WHERE k IN (5)"),
            AccessPath::Range { index: 1, .. }
        ));
        // OR across indexes: index-merge union
        match path_of("SELECT c FROM sbtest1 WHERE id = 1 OR k IN (2, 3)") {
            AccessPath::Union(p) => assert_eq!(p.len(), 2),
            other => panic!("{other:?}"),
        }
        // a disjunct that fixes no index prefix leaves a full scan
        assert!(matches!(
            path_of("SELECT c FROM sbtest1 WHERE k = 1 OR c = 'x'"),
            AccessPath::Full
        ));
    }

    #[test]
    fn create_table_layout() {
        let cat = catalog();
        let t = cat.get("sbtest", "sbtest1").unwrap();
        assert_eq!(t.columns.len(), 4);
        assert_eq!(t.auto_inc, Some(0));
        assert_eq!(t.indexes[1].name, "k_1");
        assert_eq!(t.shard_key, vec![0]);
    }

    #[test]
    fn point_select_path() {
        let p = plan_sel("SELECT c FROM sbtest1 WHERE id=7");
        match &p.source {
            Source::Table(t) => assert!(matches!(&t.path, AccessPath::Point { index: 0, keys } if keys.len() == 1)),
            _ => panic!(),
        }
        assert_eq!(&*p.columns[0].name, "c");
    }

    #[test]
    fn range_and_agg() {
        let p = plan_sel("SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN 1 AND 100");
        match &p.source {
            Source::Table(t) => assert!(matches!(
                &t.path,
                AccessPath::Range {
                    index: 0,
                    lo: Some(_),
                    hi: Some(_),
                    ..
                }
            )),
            _ => panic!(),
        }
        assert!(p.agg.is_some());
        assert_eq!(p.columns[0].name, "SUM(k)");
        let p = plan_sel("SELECT DISTINCT c FROM sbtest1 WHERE id BETWEEN 5 AND 9 ORDER BY c");
        assert!(p.distinct && p.order.len() == 1);
        let p = plan_sel("SELECT c FROM sbtest1 WHERE k = 3");
        match &p.source {
            Source::Table(t) => assert!(matches!(&t.path, AccessPath::Range { index: 1, .. })),
            _ => panic!(),
        }
        let p = plan_sel("SELECT id FROM sbtest1 WHERE id IN (1, 2, 3)");
        match &p.source {
            Source::Table(t) => assert!(matches!(&t.path, AccessPath::Point { keys, .. } if keys.len() == 3)),
            _ => panic!(),
        }
    }

    #[test]
    fn partition_by_key() {
        let (sql, key) = extract_partition_by_key(
            "CREATE TABLE o (tenant_id INT, id INT, PRIMARY KEY (tenant_id, id)) PARTITION BY KEY (tenant_id) PARTITIONS 16",
        );
        assert_eq!(key, Some(vec!["tenant_id".to_string()]));
        assert!(!sql.to_uppercase().contains("PARTITION"));
        let c0 = Catalog::default();
        let ctx = PlanCtx {
            catalog: &c0,
            db: Some("d"),
            params: &[],
        };
        let bad = "CREATE TABLE o (tenant_id INT, id INT, email VARCHAR(10), PRIMARY KEY (tenant_id, id), UNIQUE KEY (email)) PARTITION BY KEY (tenant_id)";
        assert!(plan(bad, &ctx).is_err());
    }
}
