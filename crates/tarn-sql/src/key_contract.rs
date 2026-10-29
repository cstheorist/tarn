//! Opt-in, deliberately bounded MySQL key semantics. Legacy names keep
//! their stored encodings; selecting MYSQL never reinterprets an old index.
use crate::catalog::{Catalog, TableDef};
use crate::expr::{column_expr_type, infer, CastTo, Expr, ExprType};
use crate::plan::{AggFunc, InsertSource, SelectPlan, Source, Stmt};
use crate::types::BaseType;
use sqlparser::dialect::MySqlDialect;
use sqlparser::tokenizer::{Token, Tokenizer};
use tarn_common::{ColumnType, SqlError, SqlResult, Value};

fn unsupported(what: impl std::fmt::Display) -> SqlError {
    SqlError::unsupported(format!(
        "MYSQL key contract: {what}; use an explicitly supported definition or LEGACY"
    ))
}

pub(crate) fn collation(name: &str) -> SqlResult<()> {
    if name.eq_ignore_ascii_case("utf8mb4_0900_bin") || name.eq_ignore_ascii_case("binary") {
        Ok(())
    } else {
        Err(unsupported(format!("collation '{name}' is not exact")))
    }
}

/// Check original names before planning can map them to a legacy family.
/// Tokenization keeps comments/literals/quoted identifiers out of keywords.
pub(crate) fn sql(text: &str) -> SqlResult<()> {
    let tokens = Tokenizer::new(&MySqlDialect {}, text)
        .tokenize()
        .map_err(|e| SqlError::parse(e.to_string()))?;
    let tokens: Vec<_> = tokens
        .into_iter()
        .filter(|t| !matches!(t, Token::Whitespace(_)))
        .collect();
    let word = |i: usize, name: &str| matches!(tokens.get(i), Some(Token::Word(w)) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case(name));
    let name = |i: usize| match tokens.get(i) {
        Some(Token::Word(w)) => Some(w.value.as_str()),
        Some(Token::SingleQuotedString(s)) => Some(s.as_str()),
        _ => None,
    };
    for i in 0..tokens.len() {
        if let Some(Token::Word(w)) = tokens.get(i) {
            if matches!(tokens.get(i + 1), Some(Token::LParen))
                && matches!(tokens.get(i + 2), Some(Token::Number(_, _)))
                && matches!(tokens.get(i + 3), Some(Token::RParen))
                && (w.quote_style.is_some()
                    || !matches!(
                        w.value.to_ascii_uppercase().as_str(),
                        "VARCHAR"
                            | "VARBINARY"
                            | "CHAR"
                            | "BINARY"
                            | "DECIMAL"
                            | "NUMERIC"
                            | "TINYINT"
                            | "SMALLINT"
                            | "MEDIUMINT"
                            | "INT"
                            | "INTEGER"
                            | "BIGINT"
                            | "DATETIME"
                            | "TIMESTAMP"
                            | "TIME"
                            | "FLOAT"
                            | "DOUBLE"
                            | "BIT"
                    ))
            {
                return Err(unsupported("prefix index or computed numeric call"));
            }
        }
        if word(i, "COLLATE") {
            let at = i + 1 + usize::from(matches!(tokens.get(i + 1), Some(Token::Eq)));
            if let Some(n) = name(at) {
                collation(n)?;
            }
        }
        let at = if word(i, "CHARSET") {
            Some(i + 1)
        } else if word(i, "CHARACTER") && word(i + 1, "SET") {
            Some(i + 2)
        } else {
            None
        };
        if let Some(at) = at {
            let at = at + usize::from(matches!(tokens.get(at), Some(Token::Eq)));
            if let Some(n) = name(at) {
                if !n.eq_ignore_ascii_case("utf8mb4") && !n.eq_ignore_ascii_case("binary") {
                    return Err(unsupported(format!("character set '{n}'")));
                }
            }
        }
    }
    Ok(())
}

fn table(t: &TableDef) -> SqlResult<()> {
    for &c in &t.shard_key {
        let ty = t.columns[c].ty();
        if !(ty.is_integer() || matches!(ty.tarantool_type(), "string" | "varbinary")) {
            return Err(unsupported("shard key columns other than integers and strings"));
        }
    }
    for c in &t.columns {
        use BaseType::*;
        let ty = c.ty();
        match ty.base {
            TinyInt | SmallInt | MediumInt | Int | BigInt | Bool | VarBinary => {}
            VarChar if c.collation_name().eq_ignore_ascii_case("utf8mb4_0900_bin") => {}
            Decimal
                if (1..=38).contains(&ty.len.unwrap_or(10))
                    && ty.scale.unwrap_or(0) <= 30
                    && ty.scale.unwrap_or(0) <= ty.len.unwrap_or(10) => {}
            // Other types stay available under their documented legacy
            // behavior; this gate does not certify padding, time zones,
            // floating point or invalid-date conversion as exact.
            _ => return Err(unsupported(format!("column '{}.{}' type {}", t.name, c.name, c.ty))),
        }
    }
    Ok(())
}

fn input(t: &TableDef) -> Vec<ExprType> {
    t.columns.iter().map(|c| column_expr_type(c.ty(), c.nullable)).collect()
}

fn numeric(t: &ExprType) -> bool {
    matches!(
        t.ty,
        ColumnType::Tiny
            | ColumnType::Short
            | ColumnType::Int24
            | ColumnType::Long
            | ColumnType::LongLong
            | ColumnType::NewDecimal
            | ColumnType::Decimal
    )
}

fn expression(e: &Expr, cols: &[ExprType], params: Option<&[Value]>) -> SqlResult<()> {
    let pair = |a: &Expr, b: &Expr| -> SqlResult<()> {
        expression(a, cols, params)?;
        expression(b, cols, params)?;
        if let Some(params) = params {
            let (a, b) = (infer(a, cols, params), infer(b, cols, params));
            if a.ty != ColumnType::Null && b.ty != ColumnType::Null && numeric(&a) != numeric(&b) {
                return Err(unsupported("implicit comparison between numeric and string values"));
            }
        }
        Ok(())
    };
    match e {
        Expr::Lit(Value::Float(_)) => Err(unsupported("floating-point expression")),
        Expr::Param(i) if params.is_some_and(|p| matches!(p.get(*i), Some(Value::Float(_)))) => {
            Err(unsupported("floating-point parameter"))
        }
        Expr::Col(_) | Expr::Lit(_) | Expr::Param(_) | Expr::Var(_) => Ok(()),
        Expr::Cmp(a, _, b, c) => {
            pair(a, b)?;
            if let Some(params) = params {
                let (a, b) = (infer(a, cols, params), infer(b, cols, params));
                if !numeric(&a) && !numeric(&b) && a.ty != ColumnType::Null && b.ty != ColumnType::Null {
                    collation(c.name())?;
                }
            }
            Ok(())
        }
        Expr::And(a, b) | Expr::Or(a, b) | Expr::Xor(a, b) => {
            expression(a, cols, params)?;
            expression(b, cols, params)
        }
        Expr::InList(a, bs, _, _) => {
            for b in bs {
                pair(a, b)?;
            }
            Ok(())
        }
        Expr::Between(a, b, c, _, _) => {
            pair(a, b)?;
            pair(a, c)
        }
        Expr::Not(a) | Expr::IsNull(a, _) | Expr::IsTrue(a, _, _) => expression(a, cols, params),
        Expr::Neg(a) => {
            expression(a, cols, params)?;
            if params.is_some_and(|p| !numeric(&infer(a, cols, p))) {
                return Err(unsupported("negation of a nonnumeric value"));
            }
            Ok(())
        }
        Expr::Key(a, _) => expression(a, cols, params),
        Expr::Collate(a, c) => {
            collation(c.name())?;
            expression(a, cols, params)
        }
        Expr::Cast(a, CastTo::TemporalKey(_)) => expression(a, cols, params),
        // Numeric expressions and conversions have a separate compatibility
        // surface. Exact key mode refuses them instead of promising that
        // the legacy decimal arithmetic/aggregate path is exact.
        _ => Err(unsupported("computed expression or conversion outside the key subset")),
    }
}

fn select(p: &SelectPlan, params: Option<&[Value]>) -> SqlResult<()> {
    let mut cols = Vec::new();
    for access in p.source.tables() {
        table(&access.table)?;
        cols.extend(input(&access.table));
    }
    fn joins(s: &Source, cols: &[ExprType], params: Option<&[Value]>) -> SqlResult<()> {
        if let Source::Join { left, on, .. } = s {
            joins(left, cols, params)?;
            if let Some(e) = on {
                expression(e, cols, params)?;
            }
        }
        Ok(())
    }
    joins(&p.source, &cols, params)?;
    for e in p.filter.iter().chain(p.order.iter().map(|(e, _)| e)) {
        expression(e, &cols, params)?;
    }
    let mut projected_cols = cols.clone();
    if let Some(agg) = &p.agg {
        projected_cols.clear();
        for e in &agg.group {
            expression(e, &cols, params)?;
            projected_cols.push(infer(e, &cols, params.unwrap_or(&[])));
        }
        for a in &agg.aggs {
            if !matches!(
                a.func,
                AggFunc::Count | AggFunc::CountStar | AggFunc::Min | AggFunc::Max
            ) {
                return Err(unsupported("aggregate outside COUNT/MIN/MAX"));
            }
            if let Some(e) = &a.arg {
                expression(e, &cols, params)?;
            }
            projected_cols.push(if matches!(a.func, AggFunc::Count | AggFunc::CountStar) {
                ExprType::of(ColumnType::LongLong)
            } else {
                infer(a.arg.as_ref().unwrap(), &cols, params.unwrap_or(&[]))
            });
        }
    }
    for e in p
        .project
        .iter()
        .chain(p.having.iter())
        .chain(p.limit.iter())
        .chain(p.offset.iter())
    {
        expression(e, &projected_cols, params)?;
    }
    Ok(())
}

pub(crate) fn statement(stmt: &Stmt, cat: &Catalog, params: Option<&[Value]>) -> SqlResult<()> {
    match stmt {
        Stmt::Explain { statement: s, .. } => statement(s, cat, params),
        Stmt::Select(p) => select(p, params),
        Stmt::Insert(p) => {
            table(&p.table)?;
            let mut cols = input(&p.table);
            cols.extend(input(&p.table));
            for (_, e) in &p.on_dup {
                expression(e, &cols, params)?;
            }
            match &p.source {
                InsertSource::Values(rows) => {
                    for e in rows.iter().flatten().flatten() {
                        expression(e, &[], params)?;
                    }
                    Ok(())
                }
                InsertSource::Select(p, _) => select(p, params),
            }
        }
        Stmt::Update(p) => {
            table(&p.access.table)?;
            let cols = input(&p.access.table);
            for e in p
                .filter
                .iter()
                .chain(p.set.iter().map(|(_, e)| e))
                .chain(p.order.iter().map(|(e, _)| e))
                .chain(p.limit.iter())
            {
                expression(e, &cols, params)?;
            }
            Ok(())
        }
        Stmt::Delete(p) => {
            table(&p.access.table)?;
            let cols = input(&p.access.table);
            for e in p
                .filter
                .iter()
                .chain(p.order.iter().map(|(e, _)| e))
                .chain(p.limit.iter())
            {
                expression(e, &cols, params)?;
            }
            Ok(())
        }
        Stmt::CreateTable { def, .. } => table(def),
        Stmt::Alter(plans) => {
            for p in plans {
                let old = cat
                    .get(&p.db, &p.table)
                    .ok_or_else(|| unsupported("missing ALTER target"))?;
                table(&old)?;
                table(&crate::alter::apply(&old, p, cat, &crate::expr::now_string())?.def)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

pub(crate) fn describe(stmt: &Stmt) -> serde_json::Value {
    let tables: Vec<&TableDef> = match stmt {
        Stmt::Select(p) => p.source.tables().into_iter().map(|a| a.table.as_ref()).collect(),
        Stmt::Insert(p) => vec![p.table.as_ref()],
        Stmt::Update(p) => vec![p.access.table.as_ref()],
        Stmt::Delete(p) => vec![p.access.table.as_ref()],
        Stmt::Explain { statement, .. } => return describe(statement),
        _ => Vec::new(),
    };
    serde_json::Value::Array(
        tables
            .into_iter()
            .map(|t| {
                serde_json::json!({
                    "table": t.name,
                    "mysql_key_definition": table(t).is_ok(),
                    "columns": t.columns.iter().filter(|c| c.ty().is_text()).map(|c| serde_json::json!({
                        "column": c.name,
                        "declared": c.collation_name(),
                        "effective": c.coll().name(),
                        "key_encoding": if c.coll().is_ci() { "legacy_weight_bytes" } else { "stored_utf8_bytes" },
                        "mysql_exact": c.collation_name().eq_ignore_ascii_case("utf8mb4_0900_bin"),
                    })).collect::<Vec<_>>(),
                })
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::{plan, PlanCtx};

    #[test]
    fn mysql_declarations_refuse_approximation_without_reinterpreting_legacy() {
        let cat = Catalog::default();
        let ctx = PlanCtx {
            catalog: &cat,
            db: Some("d"),
            params: &[],
            conn_coll: "utf8mb4_0900_bin",
            default_coll: "utf8mb4_0900_bin",
        };
        for ddl in [
            "CREATE TABLE t (id INT PRIMARY KEY,k VARCHAR(16))",
            "CREATE TABLE t (id INT PRIMARY KEY,k DECIMAL(38,30),b VARBINARY(8))",
        ] {
            sql(ddl).unwrap();
            statement(&plan(ddl, &ctx).unwrap(), &cat, None).unwrap();
        }
        for ddl in [
            "CREATE TABLE t (id INT PRIMARY KEY,k VARCHAR(16) COLLATE utf8mb4_bin)",
            "CREATE TABLE t (id INT PRIMARY KEY,k VARCHAR(16) COLLATE utf8mb4_0900_ai_ci)",
            "CREATE TABLE t (id INT PRIMARY KEY,k VARCHAR(16) CHARACTER SET latin1)",
            "CREATE TABLE t (id INT PRIMARY KEY,k VARCHAR(16),KEY k(k(3)))",
            "CREATE TABLE t (id DECIMAL(38,2) PRIMARY KEY)",
            "CREATE TABLE t (id INT PRIMARY KEY,k DECIMAL(65,30))",
            "CREATE TABLE t (id INT PRIMARY KEY,k CHAR(8))",
        ] {
            assert!(
                sql(ddl)
                    .and_then(|()| statement(&plan(ddl, &ctx)?, &cat, None))
                    .is_err(),
                "{ddl}"
            );
        }
        sql("SELECT 'COLLATE latin1_swedish_ci', `COLLATE` FROM t /* COLLATE utf8mb4_bin */").unwrap();
        assert_eq!(
            crate::collation::Collation::from_name("utf8mb4_0900_ai_ci"),
            Some(crate::collation::Collation::Unicode { pad: false })
        );
    }
}
