//! Statements the planner does not handle directly: uncorrelated subqueries
//! (`x IN (SELECT ...)`, `EXISTS (SELECT ...)`, scalar `(SELECT ...)`) and
//! `UNION [ALL]`.
//!
//! The inner SELECTs run first, through the normal query path, inside the
//! statement's transaction (an implicit one for autocommit statements, so the
//! whole statement stays atomic and serializable); their results are then
//! substituted as literals and the rewritten statement runs normally, so the
//! planner still picks index lookups for e.g. `WHERE id IN (SELECT ...)`.
//! Correlated subqueries are not supported: their inner SELECT fails to
//! resolve the outer column.

use crate::session::{Output, Session};
use crate::txn::Txn;
use sqlparser::ast::{self as A, Visit, VisitMut, Visitor, VisitorMut};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use std::collections::HashSet;
use std::future::Future;
use std::ops::ControlFlow;
use std::pin::Pin;
use std::sync::Arc;
use tarn_common::{Column, ColumnType, SqlError, SqlResult, Value};

type BoxFut<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Cheap pre-check on the raw text: a SELECT nested in another statement
/// (two SELECTs, or one in UPDATE/DELETE/INSERT/...), or UNION/EXISTS.
pub fn may_need(sql: &str) -> bool {
    let b = sql.as_bytes();
    let starts_with_select = sql
        .trim_start()
        .get(..6)
        .is_some_and(|w| w.eq_ignore_ascii_case("select"));
    let mut selects = 0;
    let mut i = 0;
    while i + 5 < b.len() {
        let c = b[i] | 0x20;
        if c == b's' && b[i..i + 6].eq_ignore_ascii_case(b"select") {
            selects += 1;
            if selects > 1 || !starts_with_select {
                return true;
            }
            i += 6;
            continue;
        }
        if c == b'u' && b[i..i + 5].eq_ignore_ascii_case(b"union") {
            return true;
        }
        if c == b'e' && i + 6 <= b.len() && b[i..i + 6].eq_ignore_ascii_case(b"exists") {
            // also matches IF EXISTS in DDL; parsing decides
            return true;
        }
        i += 1;
    }
    false
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Kind {
    In,
    Exists,
    Scalar,
}

/// Collects the outermost subqueries in visiting order.
struct Collect {
    depth: usize,
    found: Vec<(Kind, A::Query)>,
}

fn subquery_of(e: &A::Expr) -> Option<(Kind, &A::Query)> {
    match e {
        A::Expr::InSubquery { subquery, .. } => Some((Kind::In, subquery)),
        A::Expr::Exists { subquery, .. } => Some((Kind::Exists, subquery)),
        A::Expr::Subquery(q) => Some((Kind::Scalar, q)),
        _ => None,
    }
}

impl Visitor for Collect {
    type Break = ();
    fn pre_visit_expr(&mut self, e: &A::Expr) -> ControlFlow<()> {
        if let Some((k, q)) = subquery_of(e) {
            if self.depth == 0 {
                self.found.push((k, q.clone()));
            }
            self.depth += 1;
        }
        ControlFlow::Continue(())
    }
    fn post_visit_expr(&mut self, e: &A::Expr) -> ControlFlow<()> {
        if subquery_of(e).is_some() {
            self.depth -= 1;
        }
        ControlFlow::Continue(())
    }
}

/// Replaces the outermost subqueries, in the same order, by literals.
struct Replace {
    depth: usize,
    next: usize,
    results: Vec<Vec<Vec<Value>>>,
    err: Option<SqlError>,
}

impl VisitorMut for Replace {
    type Break = ();
    fn pre_visit_expr(&mut self, e: &mut A::Expr) -> ControlFlow<()> {
        if subquery_of(e).is_some() {
            self.depth += 1;
        }
        ControlFlow::Continue(())
    }
    fn post_visit_expr(&mut self, e: &mut A::Expr) -> ControlFlow<()> {
        if subquery_of(e).is_none() {
            return ControlFlow::Continue(());
        }
        self.depth -= 1;
        if self.depth > 0 {
            return ControlFlow::Continue(());
        }
        let rows = std::mem::take(&mut self.results[self.next]);
        self.next += 1;
        let new = match std::mem::replace(e, A::Expr::Value(A::Value::Null.into())) {
            A::Expr::InSubquery { expr, negated, .. } => {
                if rows.is_empty() {
                    // x IN (empty) is FALSE even for NULL x
                    literal(&Value::Int(negated as i64))
                } else {
                    let mut list = Vec::with_capacity(rows.len());
                    for r in &rows {
                        if r.len() != 1 {
                            self.err = Some(SqlError::new(1241, "21000", "Operand should contain 1 column(s)"));
                            return ControlFlow::Break(());
                        }
                        list.push(literal(&r[0]));
                    }
                    A::Expr::InList { expr, list, negated }
                }
            }
            A::Expr::Exists { negated, .. } => literal(&Value::Int((rows.is_empty() == negated) as i64)),
            A::Expr::Subquery(_) => {
                if rows.len() > 1 {
                    self.err = Some(SqlError::new(1242, "21000", "Subquery returns more than 1 row"));
                    return ControlFlow::Break(());
                }
                match rows.first() {
                    None => literal(&Value::Null),
                    Some(r) if r.len() == 1 => literal(&r[0]),
                    Some(_) => {
                        self.err = Some(SqlError::new(1241, "21000", "Operand should contain 1 column(s)"));
                        return ControlFlow::Break(());
                    }
                }
            }
            other => other,
        };
        *e = A::Expr::Nested(Box::new(new));
        ControlFlow::Continue(())
    }
}

/// A SQL literal for a value. Strings that would need escaping are written
/// as hex literals so that the rewritten text parses back to the same bytes.
fn literal(v: &Value) -> A::Expr {
    let val = match v {
        Value::Null => A::Value::Null,
        Value::Int(i) => A::Value::Number(i.to_string(), false),
        Value::UInt(u) => A::Value::Number(u.to_string(), false),
        Value::Float(f) => {
            if f.is_finite() {
                A::Value::Number(format!("{:e}", f), false)
            } else {
                A::Value::Null
            }
        }
        Value::Decimal(d) => A::Value::Number(d.clone(), false),
        Value::Str(b) => match std::str::from_utf8(b) {
            Ok(s) if !s.contains('\\') && !s.chars().any(|c| c.is_control()) => {
                A::Value::SingleQuotedString(s.to_string())
            }
            _ => A::Value::HexStringLiteral(hex(b)),
        },
        Value::Bin(b) => A::Value::HexStringLiteral(hex(b)),
    };
    A::Expr::Value(val.into())
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{:02X}", x)).collect()
}

/// Substitute statement parameters (`?1`..`?N` placeholders) by literals.
pub fn bind_params(mut stmt: A::Statement, params: &[Value]) -> A::Statement {
    let _ = A::visit_expressions_mut(&mut stmt, |e| {
        if let A::Expr::Value(v) = e {
            if let A::Value::Placeholder(p) = &v.value {
                if let Some(x) = p
                    .trim_start_matches('?')
                    .parse::<usize>()
                    .ok()
                    .and_then(|n| n.checked_sub(1))
                    .and_then(|i| params.get(i))
                {
                    *e = literal(x);
                }
            }
        }
        ControlFlow::<()>::Continue(())
    });
    stmt
}

/// Parse `sql` and return it if it needs this module (subqueries or UNION).
pub fn parse_if_needed(sql: &str) -> SqlResult<Option<A::Statement>> {
    let Ok(mut stmts) = Parser::parse_sql(&MySqlDialect {}, sql) else {
        return Ok(None); // let the normal path report the error
    };
    if stmts.len() != 1 {
        return Ok(None);
    }
    let stmt = stmts.pop().unwrap();
    if let A::Statement::Query(q) = &stmt {
        if matches!(*q.body, A::SetExpr::SetOperation { .. }) {
            return Ok(Some(stmt));
        }
    }
    let mut c = Collect {
        depth: 0,
        found: Vec::new(),
    };
    let _ = stmt.visit(&mut c);
    Ok(if c.found.is_empty() { None } else { Some(stmt) })
}

fn rows_of(out: Output) -> SqlResult<(Arc<[Column]>, Vec<Vec<Value>>)> {
    match out {
        Output::Rows { columns, rows } => Ok((columns, rows)),
        Output::Ok { .. } => Err(SqlError::internal("subquery did not return rows")),
    }
}

impl Session {
    /// Run a statement with subqueries or UNION (see the module docs).
    pub(crate) async fn run_complex(&mut self, stmt: A::Statement) -> SqlResult<Output> {
        let implicit = !self.has_txn() && self.autocommit;
        let mut attempt = 0;
        loop {
            if implicit {
                self.set_txn(Some(Txn::new(&self.cluster, false)));
            }
            let res = self.run_complex_once(stmt.clone()).await;
            if !implicit {
                return res;
            }
            let res = match (res, self.set_txn(None)) {
                (Ok(out), Some(t)) => t.commit(&self.cluster).await.map(|_| out),
                (Ok(out), None) => Ok(out),
                (Err(e), Some(t)) => {
                    t.rollback();
                    Err(e)
                }
                (Err(e), None) => Err(e),
            };
            match res {
                Err(e) if e.code == 1213 && attempt < 20 => {
                    attempt += 1;
                    tokio::time::sleep(std::time::Duration::from_micros(200u64 << attempt.min(6))).await;
                }
                other => return other,
            }
        }
    }

    fn run_complex_once(&mut self, mut stmt: A::Statement) -> BoxFut<'_, SqlResult<Output>> {
        Box::pin(async move {
            if let A::Statement::Query(q) = &stmt {
                if matches!(*q.body, A::SetExpr::SetOperation { .. }) {
                    let q = q.clone();
                    return self.run_union(&q).await;
                }
            }
            let mut c = Collect {
                depth: 0,
                found: Vec::new(),
            };
            let _ = stmt.visit(&mut c);
            let mut results = Vec::with_capacity(c.found.len());
            for (kind, q) in c.found {
                let mut q = q;
                if kind == Kind::Exists && q.limit_clause.is_none() {
                    // only existence matters
                    q.limit_clause = Some(A::LimitClause::LimitOffset {
                        limit: Some(A::Expr::Value(A::Value::Number("1".into(), false).into())),
                        offset: None,
                        limit_by: vec![],
                    });
                }
                let (_, rows) = rows_of(self.query_boxed(q.to_string()).await?)?;
                results.push(rows);
            }
            let mut r = Replace {
                depth: 0,
                next: 0,
                results,
                err: None,
            };
            let _ = VisitMut::visit(&mut stmt, &mut r);
            if let Some(e) = r.err {
                return Err(e);
            }
            self.query_boxed(stmt.to_string()).await
        })
    }

    fn query_boxed(&mut self, sql: String) -> BoxFut<'_, SqlResult<Output>> {
        Box::pin(async move { self.query(&sql).await })
    }

    fn eval_set<'a>(&'a mut self, e: &'a A::SetExpr) -> BoxFut<'a, SqlResult<(Arc<[Column]>, Vec<Vec<Value>>)>> {
        Box::pin(async move {
            match e {
                A::SetExpr::Select(s) => rows_of(self.query_boxed(s.to_string()).await?),
                A::SetExpr::Query(q) => rows_of(self.query_boxed(q.to_string()).await?),
                A::SetExpr::SetOperation {
                    op: A::SetOperator::Union,
                    set_quantifier,
                    left,
                    right,
                } => {
                    let (lc, mut lr) = self.eval_set(left).await?;
                    let (rc, rr) = self.eval_set(right).await?;
                    if lc.len() != rc.len() {
                        return Err(SqlError::new(
                            1222,
                            "21000",
                            "The used SELECT statements have a different number of columns",
                        ));
                    }
                    // A column keeps its type only if every branch agrees.
                    let cols: Arc<[Column]> = lc
                        .iter()
                        .zip(rc.iter())
                        .map(|(a, b)| {
                            if a.ty == b.ty {
                                a.clone()
                            } else {
                                let mut c = Column::new(&a.name, ColumnType::VarString);
                                c.schema = a.schema.clone();
                                c
                            }
                        })
                        .collect::<Vec<_>>()
                        .into();
                    lr.extend(rr);
                    let distinct = !matches!(set_quantifier, A::SetQuantifier::All | A::SetQuantifier::AllByName);
                    if distinct {
                        let mut seen = HashSet::new();
                        lr.retain(|row| seen.insert(row.iter().map(|v| format!("{:?}", v)).collect::<Vec<_>>()));
                    }
                    Ok((cols, lr))
                }
                other => Err(SqlError::unsupported(format!("set operation {}", other))),
            }
        })
    }

    async fn run_union(&mut self, q: &A::Query) -> SqlResult<Output> {
        let (columns, mut rows) = self.eval_set(&q.body).await?;
        if let Some(ob) = &q.order_by {
            if let A::OrderByKind::Expressions(list) = &ob.kind {
                let mut keys: Vec<(usize, bool)> = Vec::new();
                for o in list {
                    let idx = match &o.expr {
                        A::Expr::Value(v) => match &v.value {
                            A::Value::Number(n, _) => n.parse::<usize>().ok().and_then(|p| p.checked_sub(1)),
                            _ => None,
                        },
                        A::Expr::Identifier(i) => columns.iter().position(|c| c.name.eq_ignore_ascii_case(&i.value)),
                        A::Expr::CompoundIdentifier(p) => p
                            .last()
                            .and_then(|i| columns.iter().position(|c| c.name.eq_ignore_ascii_case(&i.value))),
                        _ => None,
                    }
                    .filter(|&i| i < columns.len())
                    .ok_or_else(|| {
                        SqlError::new(1054, "42S22", format!("Unknown column '{}' in 'order clause'", o.expr))
                    })?;
                    keys.push((idx, matches!(o.options.sort, Some(A::OrderBySort::Desc))));
                }
                rows.sort_by(|a, b| {
                    for &(i, desc) in &keys {
                        let ord = crate::types::sort_cmp(&a[i], &b[i]);
                        let ord = if desc { ord.reverse() } else { ord };
                        if ord.is_ne() {
                            return ord;
                        }
                    }
                    std::cmp::Ordering::Equal
                });
            }
        }
        let num = |e: &A::Expr| -> SqlResult<usize> {
            match e {
                A::Expr::Value(v) => match &v.value {
                    A::Value::Number(n, _) => n.parse().map_err(|_| SqlError::parse("bad LIMIT")),
                    _ => Err(SqlError::parse("LIMIT must be a number")),
                },
                _ => Err(SqlError::parse("LIMIT must be a number")),
            }
        };
        if let Some(lc) = &q.limit_clause {
            let (limit, offset) = match lc {
                A::LimitClause::LimitOffset { limit, offset, .. } => (
                    limit.as_ref().map(num).transpose()?,
                    offset.as_ref().map(|o| num(&o.value)).transpose()?,
                ),
                A::LimitClause::OffsetCommaLimit { offset, limit } => (Some(num(limit)?), Some(num(offset)?)),
            };
            let off = offset.unwrap_or(0).min(rows.len());
            rows.drain(..off);
            if let Some(l) = limit {
                rows.truncate(l);
            }
        }
        Ok(Output::Rows { columns, rows })
    }
}
