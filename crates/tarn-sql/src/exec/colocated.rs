//! Eligibility for one-shard, dependent primary-prefix join reads. This
//! proves equal *hash inputs*, rather than guessing from current shard
//! destinations. The caller still checks writes/locking and evaluates the
//! original joins: these reads only supply complete candidate rowsets.

use super::*;

#[derive(Debug)]
pub(super) struct Plan<'a> {
    pub(super) shard_key: Vec<Value>,
    pub(super) reads: Vec<Read<'a>>,
}

#[derive(Debug)]
pub(super) struct Read<'a> {
    pub(super) table: &'a TableDef,
    /// Earlier read whose native rows supply every Field binding.
    pub(super) parent: Option<usize>,
    pub(super) bindings: Vec<Binding>,
}

#[derive(Clone, Debug, PartialEq)]
pub(super) enum Binding {
    Const(Value),
    /// One-based native tuple field, already in the index's representation.
    Field(u32),
}

/// An ordinary primary point or one unbounded equality prefix. Limits,
/// alternative keys and nullable equality need their existing executor.
fn prefix(access: &TableAccess) -> Option<&[Expr]> {
    let t = &access.table;
    if t.is_virtual() || t.base.is_some() || access.limit.is_some() {
        return None;
    }
    let pk = t.indexes.first()?;
    if pk.id != 0 || !pk.unique || pk.global.is_some() {
        return None;
    }
    match &access.path {
        AccessPath::Point { index: 0, keys } if keys.len() == 1 && keys[0].len() == pk.cols.len() => Some(&keys[0]),
        AccessPath::Range {
            index: 0,
            eq,
            null_eq,
            lo: None,
            hi: None,
            reverse: false,
        } if eq.len() <= pk.cols.len() && null_eq.len() == eq.len() && !null_eq.contains(&true) => Some(eq),
        _ => None,
    }
}

fn exact_part(t: &TableDef, c: usize) -> bool {
    t.columns.get(c).is_some_and(|col| {
        !matches!(col.ty().base, BaseType::Decimal | BaseType::Float | BaseType::Double)
            && (!(col.ty().is_text() && col.coll().is_ci()) || t.layout.col_weight[c].is_some())
    })
}

fn same_part(a: &TableDef, ac: usize, b: &TableDef, bc: usize) -> bool {
    let (at, bt) = (&a.columns[ac], &b.columns[bc]);
    at.ty() == bt.ty()
        && at.coll() == bt.coll()
        && a.layout.col_weight[ac].is_some() == b.layout.col_weight[bc].is_some()
}

#[derive(Clone, Copy)]
struct Field {
    alias: usize,
    col: usize,
    keyed: bool,
}

/// Only representation-preserving wrappers of a column are admitted.
/// COLLATE on a number converts it to text and is deliberately excluded.
fn field(e: &Expr, steps: &[(&TableAccess, JoinKind, Option<&Expr>)]) -> Option<Field> {
    match e {
        Expr::Col(c) => steps.iter().enumerate().find_map(|(alias, (a, _, _))| {
            c.checked_sub(a.offset)
                .filter(|&c| c < a.table.columns.len())
                .map(|col| Field {
                    alias,
                    col,
                    keyed: false,
                })
        }),
        Expr::Collate(inner, coll) | Expr::Key(inner, coll) => {
            let mut f = field(inner, steps)?;
            let c = &steps[f.alias].0.table.columns[f.col];
            if f.keyed || *coll != c.coll() || (!c.ty().is_text() && !c.ty().is_binary()) {
                return None;
            }
            f.keyed = matches!(e, Expr::Key(_, _)) && coll.is_ci();
            Some(f)
        }
        _ => None,
    }
}

fn constant(e: &Expr, t: &TableDef, c: usize, cx: &EvalCtx<'_>) -> SqlResult<Option<Value>> {
    let raw = match e {
        Expr::Lit(_) | Expr::Param(_) => e.eval(&[], cx)?,
        Expr::Collate(inner, coll)
            if *coll == t.columns[c].coll() && (t.columns[c].ty().is_text() || t.columns[c].ty().is_binary()) =>
        {
            return constant(inner, t, c, cx);
        }
        // A non-CI Key is the identity. CI keys are already binary: sending
        // them through lookup_part again would weight them twice.
        Expr::Key(inner, coll) if *coll == t.columns[c].coll() && !coll.is_ci() => {
            return constant(inner, t, c, cx);
        }
        _ => return Ok(None),
    };
    Ok(if raw.is_null() { None } else { t.lookup_part(c, &raw) })
}

/// Prove a right shard part equal to the same-position shard part of an
/// already admitted alias. OR branches and NULL-safe equality prove nothing.
fn equated(e: &Expr, alias: usize, position: usize, steps: &[(&TableAccess, JoinKind, Option<&Expr>)]) -> bool {
    match e {
        Expr::And(a, b) => equated(a, alias, position, steps) || equated(b, alias, position, steps),
        Expr::Cmp(a, CmpOp::Eq, b, coll) => {
            let (Some(a), Some(b)) = (field(a, steps), field(b, steps)) else {
                return false;
            };
            let (right, prior) = if a.alias == alias && b.alias < alias {
                (a, b)
            } else if b.alias == alias && a.alias < alias {
                (b, a)
            } else {
                return false;
            };
            let rt = &steps[alias].0.table;
            let pt = &steps[prior.alias].0.table;
            right.col == rt.shard_key[position]
                && prior.col == pt.shard_key[position]
                && right.keyed == prior.keyed
                && same_part(rt, right.col, pt, prior.col)
                && *coll == rt.columns[right.col].coll()
        }
        _ => false,
    }
}

pub(super) fn plan<'a>(
    steps: &[(&'a TableAccess, JoinKind, Option<&'a Expr>)],
    cx: &EvalCtx<'_>,
) -> SqlResult<Option<Plan<'a>>> {
    Ok(analyze(steps, cx)?.ok())
}

/// The executor and EXPLAIN use the same eligibility guards. Reasons are
/// static labels; this adds no storage work or SQL expression evaluation.
pub(super) fn analyze<'a>(
    steps: &[(&'a TableAccess, JoinKind, Option<&'a Expr>)],
    cx: &EvalCtx<'_>,
) -> SqlResult<Result<Plan<'a>, &'static str>> {
    // Storage bounds the number of slots; larger joins keep their existing
    // executor rather than turning the optimization's cap into a SQL error.
    if !(2..=16).contains(&steps.len()) || steps[0].1 != JoinKind::Inner || steps[0].2.is_some() {
        return Ok(Err("requires two to sixteen tables in a left-deep join"));
    }
    let root = &steps[0].0.table;
    let n = root.shard_key.len();
    if n == 0 {
        return Ok(Err("the root has no shard key"));
    }
    let mut reads = Vec::with_capacity(steps.len());
    let mut shard_key = Vec::new();
    let mut offset = 0;
    for (alias, (access, _, on)) in steps.iter().enumerate() {
        let t = &access.table;
        let Some(exprs) = prefix(access) else {
            return Ok(Err(
                "each access must be one unlimited primary-key point or equality prefix",
            ));
        };
        if access.offset != offset
            || t.shard_key.len() != n
            || exprs.len() < n
            || t.pk().cols.get(..n) != Some(t.shard_key.as_slice())
            || !t.pk().cols[..exprs.len()].iter().all(|&c| exact_part(t, c))
            || !(0..n).all(|i| same_part(root, root.shard_key[i], t, t.shard_key[i]))
        {
            return Ok(Err(
                "ordered shard keys must match in type/collation and prefix each primary key",
            ));
        }
        let Some(next_offset) = offset.checked_add(t.columns.len()) else {
            return Ok(Err("the combined native tuple layout exceeds supported bounds"));
        };
        offset = next_offset;
        if alias > 0 && !(0..n).all(|i| on.is_some_and(|on| equated(on, alias, i, steps))) {
            return Ok(Err("ON must equate every shard-key component to an earlier alias"));
        }
        let mut parent = None;
        let mut bindings = Vec::with_capacity(exprs.len());
        for (position, expr) in exprs.iter().enumerate() {
            let col = t.pk().cols[position];
            if let Some(f) = field(expr, steps) {
                let source = &steps[f.alias].0.table;
                if alias == 0
                    // Distribution reuses Lookup::keys_of: evaluating a CI
                    // Key here would weight its binary result a second time.
                    // Ordinary Col/COLLATE bindings are normalized once.
                    || f.keyed
                    || f.alias >= alias
                    || parent.is_some_and(|p| p != f.alias)
                    || !source.pk().cols.contains(&f.col)
                    || !same_part(t, col, source, f.col)
                    || (position < n && f.col != source.shard_key[position])
                {
                    return Ok(Err(
                        "dependent key fields must come from one earlier primary key with identical representation",
                    ));
                }
                parent = Some(f.alias);
                let native = source.layout.col_weight[f.col].unwrap_or(source.layout.col_field[f.col]);
                let Some(native) = native.checked_add(1).and_then(|f| u32::try_from(f).ok()) else {
                    return Ok(Err("a native field position exceeds supported bounds"));
                };
                bindings.push(Binding::Field(native));
            } else {
                let Some(value) = constant(expr, t, col, cx)? else {
                    return Ok(Err(
                        "constant key bindings must be non-NULL, representable literals or bound parameters",
                    ));
                };
                if alias > 0 && position < n && value != shard_key[position] {
                    return Ok(Err("bound shard-key inputs differ between aliases"));
                }
                bindings.push(Binding::Const(value));
            }
        }
        if alias == 0 {
            shard_key = bindings[..n]
                .iter()
                .map(|b| match b {
                    Binding::Const(v) => v.clone(),
                    Binding::Field(_) => unreachable!("root bindings were checked constant"),
                })
                .collect();
        }
        reads.push(Read {
            table: t,
            parent,
            bindings,
        });
    }
    Ok(Ok(Plan { shard_key, reads }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Catalog;
    use crate::expr::CastTo;
    use crate::planner::{plan as sql_plan, PlanCtx};
    use crate::types::Arith;

    fn table(name: &str, cols: &str, pk: &str, shard: &str) -> Arc<TableDef> {
        let cat = Catalog::default();
        let ctx = PlanCtx {
            statistics: None,
            catalog: &cat,
            db: Some("d"),
            params: &[],
            conn_coll: "utf8mb4_bin",
            default_coll: "utf8mb4_bin",
        };
        let sql = format!("CREATE TABLE {name} ({cols}, PRIMARY KEY ({pk})) PARTITION BY KEY({shard}) PARTITIONS 2");
        let Stmt::CreateTable { mut def, .. } = sql_plan(&sql, &ctx).unwrap() else {
            panic!()
        };
        def.space_id = 512;
        Arc::from(def)
    }

    fn ints(name: &str) -> Arc<TableDef> {
        table(
            name,
            "tenant_id BIGINT, id BIGINT, n BIGINT",
            "tenant_id,id",
            "tenant_id",
        )
    }

    fn cx(params: &[Value]) -> EvalCtx<'_> {
        EvalCtx {
            params,
            db: Some("d"),
            user: "u",
            conn_id: 1,
            last_insert_id: 0,
            found_rows: 0,
            row_count: 0,
            now: "2026-01-01 00:00:00",
        }
    }

    fn access(table: Arc<TableDef>, offset: usize, eq: Vec<Expr>) -> TableAccess {
        TableAccess {
            alias: table.name.clone(),
            table,
            offset,
            limit: None,
            path: AccessPath::Range {
                index: 0,
                null_eq: vec![false; eq.len()],
                eq,
                lo: None,
                hi: None,
                reverse: false,
            },
        }
    }

    fn cmp(a: Expr, op: CmpOp, b: Expr, coll: Collation) -> Expr {
        Expr::Cmp(Box::new(a), op, Box::new(b), coll)
    }

    fn eq(a: usize, b: usize) -> Expr {
        cmp(Expr::Col(a), CmpOp::Eq, Expr::Col(b), Collation::default())
    }

    fn and(a: Expr, b: Expr) -> Expr {
        Expr::And(Box::new(a), Box::new(b))
    }

    fn two<'a>(
        a: &'a TableAccess,
        b: &'a TableAccess,
        on: &'a Expr,
    ) -> [(&'a TableAccess, JoinKind, Option<&'a Expr>); 2] {
        [(a, JoinKind::Inner, None), (b, JoinKind::Left, Some(on))]
    }

    #[test]
    fn planner_primary_point_and_dependent_prefix_chain() {
        let cat = Catalog::new(1, vec!["d".into()], vec![(*ints("t")).clone()]);
        let params = [Value::UInt(7), Value::Int(5)];
        let ctx = PlanCtx {
            statistics: None,
            catalog: &cat,
            db: Some("d"),
            params: &params,
            conn_coll: "utf8mb4_bin",
            default_coll: "utf8mb4_bin",
        };
        let sql = "SELECT a.id,b.id,c.id FROM t a \
                   LEFT JOIN t b ON a.tenant_id=b.tenant_id AND a.id=b.id AND b.n>0 \
                   JOIN t c ON b.tenant_id=c.tenant_id WHERE a.tenant_id=?1 AND a.id=?2";
        let Stmt::Select(p) = sql_plan(sql, &ctx).unwrap() else {
            panic!()
        };
        fn steps<'a>(s: &'a Source, out: &mut Vec<(&'a TableAccess, JoinKind, Option<&'a Expr>)>) {
            match s {
                Source::Table(t) => out.push((t, JoinKind::Inner, None)),
                Source::Join { left, right, kind, on } => {
                    steps(left, out);
                    out.push((right, *kind, on.as_ref()));
                }
                Source::Dual => panic!(),
            }
        }
        let mut inputs = Vec::new();
        steps(&p.source, &mut inputs);
        let result = plan(&inputs, &cx(&params)).unwrap().unwrap();
        assert_eq!(result.shard_key, vec![Value::Int(7)]);
        assert_eq!(
            result.reads[0].bindings,
            vec![Binding::Const(Value::Int(7)), Binding::Const(Value::Int(5))]
        );
        assert_eq!(result.reads[1].parent, Some(0));
        assert_eq!(result.reads[1].bindings, vec![Binding::Field(1), Binding::Field(2)]);
        assert_eq!(result.reads[2].parent, Some(1));
        assert_eq!(result.reads[2].bindings, vec![Binding::Field(1)]);
    }

    #[test]
    fn dependent_primary_prefixes_and_constants() {
        let a = access(ints("a"), 0, vec![Expr::Param(0)]);
        let b = access(ints("b"), 3, vec![Expr::Col(0), Expr::Col(1)]);
        let c = access(ints("c"), 6, vec![Expr::Col(3), Expr::Lit(Value::Int(9))]);
        let ab = and(
            eq(0, 3),
            cmp(Expr::Col(2), CmpOp::Gt, Expr::Col(5), Collation::default()),
        );
        let bc = eq(3, 6);
        let steps = [
            (&a, JoinKind::Inner, None),
            (&b, JoinKind::Left, Some(&ab)),
            (&c, JoinKind::Inner, Some(&bc)),
        ];
        // Signed key normalization follows lookup_part, not the parameter's
        // wire type, and the residual remains the caller's responsibility.
        let p = plan(&steps, &cx(&[Value::UInt(7)])).unwrap().unwrap();
        assert_eq!(p.shard_key, vec![Value::Int(7)]);
        assert_eq!(p.reads[0].parent, None);
        assert_eq!(p.reads[0].bindings, vec![Binding::Const(Value::Int(7))]);
        assert_eq!(p.reads[1].parent, Some(0));
        assert_eq!(p.reads[1].bindings, vec![Binding::Field(1), Binding::Field(2)]);
        assert_eq!(p.reads[2].parent, Some(1));
        assert_eq!(
            p.reads[2].bindings,
            vec![Binding::Field(1), Binding::Const(Value::Int(9))]
        );
        assert_eq!(p.reads[2].table.name, "c");
        assert_eq!(plan(&steps, &cx(&[])).unwrap_err().code, 1210);
        for v in [Value::Null, Value::str("not a number")] {
            assert!(plan(&steps, &cx(&[v])).unwrap().is_none());
        }
    }

    #[test]
    fn composite_shard_positions_must_be_equated_in_hash_order() {
        let t = table(
            "t",
            "tenant_id INT, region_id INT, id INT",
            "tenant_id,region_id,id",
            "tenant_id,region_id",
        );
        let a = access(t.clone(), 0, vec![Expr::Lit(Value::Int(4)), Expr::Lit(Value::Int(8))]);
        let mut b = access(t.clone(), 3, vec![Expr::Col(0), Expr::Col(1)]);
        let on = and(eq(0, 3), eq(1, 4));
        let p = plan(&two(&a, &b, &on), &cx(&[])).unwrap().unwrap();
        assert_eq!(p.shard_key, vec![Value::Int(4), Value::Int(8)]);
        assert_eq!(p.reads[1].bindings, vec![Binding::Field(1), Binding::Field(2)]);
        let swapped = and(eq(1, 3), eq(0, 4));
        assert!(plan(&two(&a, &b, &swapped), &cx(&[])).unwrap().is_none());
        b.path = AccessPath::Range {
            index: 0,
            eq: vec![Expr::Col(1), Expr::Col(0)],
            null_eq: vec![false; 2],
            lo: None,
            hi: None,
            reverse: false,
        };
        assert!(plan(&two(&a, &b, &on), &cx(&[])).unwrap().is_none());
        let reordered = table(
            "t",
            "tenant_id INT, region_id INT, id INT",
            "tenant_id,region_id,id",
            "region_id,tenant_id",
        );
        b.table = reordered;
        assert!(plan(&two(&a, &b, &on), &cx(&[])).unwrap().is_none());
        let partial = access(t, 0, vec![Expr::Lit(Value::Int(4))]);
        assert!(plan(&two(&partial, &b, &on), &cx(&[])).unwrap().is_none());
    }

    #[test]
    fn collation_keys_use_physical_fields_once() {
        let mut def = (*table(
            "t",
            "tenant_id VARCHAR(20) COLLATE utf8mb4_unicode_ci, id INT",
            "tenant_id,id",
            "tenant_id",
        ))
        .clone();
        // ALTER can reorder SQL columns without moving native fields.
        def.columns[0].field = Some(7);
        def.columns[0].weight = Some(8);
        def.resolve_types().unwrap();
        let t = Arc::new(def);
        let a = access(t.clone(), 0, vec![Expr::Lit(Value::str("Á "))]);
        let mut b = access(t.clone(), 2, vec![Expr::Col(0)]);
        let coll = t.columns[0].coll();
        let on = cmp(Expr::Col(0), CmpOp::Eq, Expr::Col(2), coll);
        let p = plan(&two(&a, &b, &on), &cx(&[])).unwrap().unwrap();
        assert_eq!(p.shard_key, vec![Value::Bin(coll.key("a".as_bytes()))]);
        assert_eq!(p.reads[1].bindings, vec![Binding::Field(9)]);
        b.path = AccessPath::Range {
            index: 0,
            eq: vec![Expr::Key(Box::new(Expr::Col(0)), coll)],
            null_eq: vec![false],
            lo: None,
            hi: None,
            reverse: false,
        };
        assert!(plan(&two(&a, &b, &on), &cx(&[])).unwrap().is_none());
        b.path = AccessPath::Range {
            index: 0,
            eq: vec![Expr::Collate(Box::new(Expr::Col(0)), coll)],
            null_eq: vec![false],
            lo: None,
            hi: None,
            reverse: false,
        };
        assert_eq!(
            plan(&two(&a, &b, &on), &cx(&[])).unwrap().unwrap().reads[1].bindings,
            vec![Binding::Field(9)]
        );
        let wrapped = cmp(
            Expr::Key(Box::new(Expr::Col(0)), coll),
            CmpOp::Eq,
            Expr::Key(Box::new(Expr::Col(2)), coll),
            coll,
        );
        assert!(plan(&two(&a, &b, &wrapped), &cx(&[])).unwrap().is_some());
        let mixed = cmp(Expr::Key(Box::new(Expr::Col(0)), coll), CmpOp::Eq, Expr::Col(2), coll);
        assert!(plan(&two(&a, &b, &mixed), &cx(&[])).unwrap().is_none());
        let binary = cmp(Expr::Col(0), CmpOp::Eq, Expr::Col(2), Collation::Binary);
        assert!(plan(&two(&a, &b, &binary), &cx(&[])).unwrap().is_none());
    }

    #[test]
    fn slot_limit_falls_back_without_changing_sql_support() {
        let t = ints("t");
        let accesses: Vec<_> = (0..17)
            .map(|i| {
                access(
                    t.clone(),
                    i * 3,
                    vec![if i == 0 { Expr::Lit(Value::Int(1)) } else { Expr::Col(0) }],
                )
            })
            .collect();
        let ons: Vec<_> = (0..17).map(|i| eq(0, i * 3)).collect();
        let steps: Vec<_> = accesses
            .iter()
            .enumerate()
            .map(|(i, a)| (a, JoinKind::Inner, if i == 0 { None } else { Some(&ons[i]) }))
            .collect();
        assert!(plan(&steps[..16], &cx(&[])).unwrap().is_some());
        assert!(plan(&steps, &cx(&[])).unwrap().is_none());
        assert!(plan(&steps[..1], &cx(&[])).unwrap().is_none());
        assert!(plan(&[], &cx(&[])).unwrap().is_none());
    }

    #[test]
    fn types_collations_and_inexact_numeric_parts_fall_back() {
        for ty in ["INT", "BIGINT UNSIGNED", "DECIMAL(20,0)", "FLOAT", "DOUBLE"] {
            let a = access(ints("a"), 0, vec![Expr::Lit(Value::Int(1))]);
            let b = access(
                table(
                    "b",
                    &format!("tenant_id {ty}, id BIGINT, n BIGINT"),
                    "tenant_id,id",
                    "tenant_id",
                ),
                3,
                vec![Expr::Col(0)],
            );
            assert!(plan(&two(&a, &b, &eq(0, 3)), &cx(&[])).unwrap().is_none(), "{ty}");
        }
        for ty in ["DECIMAL(20,0)", "FLOAT", "DOUBLE"] {
            let t = table("t", &format!("tenant_id BIGINT, id {ty}"), "tenant_id,id", "tenant_id");
            let a = access(t.clone(), 0, vec![Expr::Lit(Value::Int(1)), Expr::Lit(Value::Int(2))]);
            let b = access(t, 2, vec![Expr::Col(0), Expr::Col(1)]);
            assert!(
                plan(&two(&a, &b, &eq(0, 2)), &cx(&[])).unwrap().is_none(),
                "probed {ty}"
            );
        }
        let a = access(
            table(
                "a",
                "tenant_id VARCHAR(20) COLLATE utf8mb4_unicode_ci, id INT",
                "tenant_id,id",
                "tenant_id",
            ),
            0,
            vec![Expr::Lit(Value::str("a"))],
        );
        for coll in ["utf8mb4_bin", "utf8mb4_general_ci", "utf8mb4_unicode_nopad_ci"] {
            let b = access(
                table(
                    "b",
                    &format!("tenant_id VARCHAR(20) COLLATE {coll}, id INT"),
                    "tenant_id,id",
                    "tenant_id",
                ),
                2,
                vec![Expr::Col(0)],
            );
            assert!(plan(&two(&a, &b, &eq(0, 2)), &cx(&[])).unwrap().is_none(), "{coll}");
        }
    }

    #[test]
    fn proof_requires_ordinary_conjunctive_equalities() {
        let a = access(ints("a"), 0, vec![Expr::Lit(Value::Int(1))]);
        let b = access(ints("b"), 3, vec![Expr::Col(0)]);
        for on in [
            cmp(Expr::Col(0), CmpOp::NullSafeEq, Expr::Col(3), Collation::default()),
            Expr::Or(Box::new(eq(0, 3)), Box::new(eq(1, 4))),
            eq(1, 3),
            cmp(
                Expr::Cast(Box::new(Expr::Col(0)), CastTo::Signed),
                CmpOp::Eq,
                Expr::Col(3),
                Collation::default(),
            ),
        ] {
            assert!(plan(&two(&a, &b, &on), &cx(&[])).unwrap().is_none(), "{on:?}");
        }
        let no_on = [(&a, JoinKind::Inner, None), (&b, JoinKind::Inner, None)];
        assert!(plan(&no_on, &cx(&[])).unwrap().is_none());
    }

    #[test]
    fn dependent_prefix_has_one_earlier_primary_parent() {
        let a = access(ints("a"), 0, vec![Expr::Lit(Value::Int(1))]);
        let b = access(ints("b"), 3, vec![Expr::Col(0)]);
        let ab = eq(0, 3);
        let bc = eq(3, 6);
        for exprs in [
            vec![Expr::Col(3), Expr::Col(1)], // two parents
            vec![Expr::Col(3), Expr::Col(5)], // non-primary parent column
            vec![Expr::Col(3), Expr::Col(7)], // current / forward reference
            vec![
                Expr::Col(3),
                Expr::Arith(Box::new(Expr::Col(4)), Arith::Add, Box::new(Expr::Lit(Value::Int(0)))),
            ],
            vec![Expr::Col(3), Expr::Cast(Box::new(Expr::Col(4)), CastTo::Signed)],
            vec![Expr::Lit(Value::Int(2))], // a different hash input
        ] {
            let c = access(ints("c"), 6, exprs);
            let steps = [
                (&a, JoinKind::Inner, None),
                (&b, JoinKind::Inner, Some(&ab)),
                (&c, JoinKind::Inner, Some(&bc)),
            ];
            assert!(plan(&steps, &cx(&[])).unwrap().is_none());
        }
        let c = access(ints("c"), 6, vec![Expr::Lit(Value::UInt(1)), Expr::Lit(Value::Int(9))]);
        let steps = [
            (&a, JoinKind::Inner, None),
            (&b, JoinKind::Inner, Some(&ab)),
            (&c, JoinKind::Inner, Some(&bc)),
        ];
        assert_eq!(plan(&steps, &cx(&[])).unwrap().unwrap().reads[2].parent, None);
    }

    #[test]
    fn alternative_paths_bounds_limits_and_virtual_tables_fall_back() {
        let a = access(ints("a"), 0, vec![Expr::Lit(Value::Int(1))]);
        let mut b = access(ints("b"), 3, vec![Expr::Col(0)]);
        let on = eq(0, 3);
        for path in [
            AccessPath::Full,
            AccessPath::Point {
                index: 1,
                keys: vec![vec![Expr::Col(0), Expr::Col(1)]],
            },
            AccessPath::Point {
                index: 0,
                keys: vec![vec![Expr::Col(0), Expr::Col(1)]; 2],
            },
            AccessPath::Point {
                index: 0,
                keys: vec![vec![Expr::Col(0)]],
            },
            AccessPath::Prefixes {
                index: 0,
                keys: vec![vec![Expr::Col(0)]],
            },
            AccessPath::Union(vec![b.path.clone()]),
            AccessPath::Range {
                index: 0,
                eq: vec![Expr::Col(0)],
                null_eq: vec![false],
                lo: Some((Expr::Lit(Value::Int(1)), true)),
                hi: None,
                reverse: false,
            },
            AccessPath::Range {
                index: 0,
                eq: vec![Expr::Col(0)],
                null_eq: vec![true],
                lo: None,
                hi: None,
                reverse: false,
            },
        ] {
            b.path = path;
            assert!(plan(&two(&a, &b, &on), &cx(&[])).unwrap().is_none(), "{:?}", b.path);
        }
        b = access(ints("b"), 3, vec![Expr::Col(0)]);
        b.limit = Some(Expr::Lit(Value::Int(1)));
        assert!(plan(&two(&a, &b, &on), &cx(&[])).unwrap().is_none());
        b.limit = None;
        Arc::make_mut(&mut b.table).space_id = crate::catalog::INFORMATION_SCHEMA_SPACE;
        assert!(plan(&two(&a, &b, &on), &cx(&[])).unwrap().is_none());
        // A real primary point and a self-join use exactly the same plan.
        b = access(a.table.clone(), 3, vec![Expr::Col(0), Expr::Col(1)]);
        b.path = AccessPath::Point {
            index: 0,
            keys: vec![vec![Expr::Col(0), Expr::Col(1)]],
        };
        assert!(plan(&two(&a, &b, &on), &cx(&[])).unwrap().is_some());
    }
}
