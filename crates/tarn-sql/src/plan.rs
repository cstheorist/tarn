//! Physical plans produced by the planner and consumed by the executor.

use crate::catalog::TableDef;
use crate::expr::Expr;
use std::sync::Arc;
use tarn_common::Column;

/// How one table of a FROM clause is read. Key expressions may reference
/// parameters, constants, or (inside a join) columns of tables to the left.
#[derive(Clone, Debug)]
pub enum AccessPath {
    /// Equality on every column of a unique index: one lookup per key.
    /// Several keys come from `col IN (...)`.
    Point { index: u32, keys: Vec<Vec<Expr>> },
    /// Equality on a prefix of an index, optionally bounded on the next column.
    Range {
        index: u32,
        eq: Vec<Expr>,
        lo: Option<(Expr, bool)>,
        hi: Option<(Expr, bool)>,
        reverse: bool,
    },
    /// Equality on the same prefix of an index for several keys -- `col IN
    /// (...)` on a non-unique or partial key, an OR of equalities, a
    /// row-value IN: every row whose index key starts with one of `keys`
    /// (all of the same length), read in one call per shard.
    Prefixes { index: u32, keys: Vec<Vec<Expr>> },
    /// Union of single-index lookups (an OR whose disjuncts fix prefixes of
    /// different indexes, like MySQL's index_merge union): rows are
    /// deduplicated by primary key and returned in primary-key order.
    Union(Vec<AccessPath>),
    /// Every row (all shards).
    Full,
}

#[derive(Clone, Debug)]
pub struct TableAccess {
    pub table: Arc<TableDef>,
    pub alias: String,
    pub path: AccessPath,
    /// Position of this table's first column in the combined row.
    pub offset: usize,
    /// Rows may stop after this many (pushed-down LIMIT, only when safe).
    pub limit: Option<Expr>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
}

#[derive(Clone, Debug)]
pub enum Source {
    /// No FROM clause: one empty row.
    Dual,
    Table(TableAccess),
    /// Nested-loop join: for every row of `left`, read `right` with its
    /// access path evaluated against that row.
    Join {
        left: Box<Source>,
        right: TableAccess,
        kind: JoinKind,
        on: Option<Expr>,
    },
}

impl Source {
    pub fn width(&self) -> usize {
        match self {
            Source::Dual => 0,
            Source::Table(t) => t.offset + t.table.columns.len(),
            Source::Join { right, .. } => right.offset + right.table.columns.len(),
        }
    }
    pub fn tables(&self) -> Vec<&TableAccess> {
        match self {
            Source::Dual => vec![],
            Source::Table(t) => vec![t],
            Source::Join { left, right, .. } => {
                let mut v = left.tables();
                v.push(right);
                v
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggFunc {
    CountStar,
    Count,
    Sum,
    Avg,
    Min,
    Max,
    AnyValue,
    GroupConcat,
}

#[derive(Clone, Debug)]
pub struct AggCall {
    pub func: AggFunc,
    pub arg: Option<Expr>,
    pub distinct: bool,
    /// GROUP_CONCAT(... ORDER BY ...): sort keys over the input row.
    pub order: Vec<(Expr, bool)>,
    /// GROUP_CONCAT(... SEPARATOR s); default ",".
    pub sep: Option<bytes::Bytes>,
}

impl AggCall {
    pub fn simple(func: AggFunc, arg: Option<Expr>, distinct: bool) -> AggCall {
        AggCall {
            func,
            arg,
            distinct,
            order: Vec::new(),
            sep: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct AggPlan {
    /// Output row layout: group values followed by aggregate results.
    pub group: Vec<Expr>,
    pub aggs: Vec<AggCall>,
}

#[derive(Clone, Debug)]
pub struct SelectPlan {
    pub source: Source,
    pub filter: Option<Expr>,
    pub agg: Option<AggPlan>,
    pub having: Option<Expr>,
    /// Sort keys over the (post-aggregation) row, evaluated before projection.
    pub order: Vec<(Expr, bool)>,
    pub project: Vec<Expr>,
    pub columns: Arc<[Column]>,
    pub distinct: bool,
    pub limit: Option<Expr>,
    pub offset: Option<Expr>,
    pub for_update: bool,
}

#[derive(Clone, Debug)]
pub enum InsertSource {
    /// Rows of expressions, one per table column (defaults already filled in;
    /// `None` = the column's default / auto-increment).
    Values(Vec<Vec<Option<Expr>>>),
    Select(Box<SelectPlan>, Vec<usize>),
}

#[derive(Clone, Debug)]
pub struct InsertPlan {
    pub table: Arc<TableDef>,
    pub source: InsertSource,
    pub replace: bool,
    pub ignore: bool,
    /// ON DUPLICATE KEY UPDATE assignments: (column, expr over the old row
    /// followed by the proposed new row).
    pub on_dup: Vec<(usize, Expr)>,
}

#[derive(Clone, Debug)]
pub struct UpdatePlan {
    pub access: TableAccess,
    pub filter: Option<Expr>,
    /// (column, new value expression over the old row)
    pub set: Vec<(usize, Expr)>,
    pub order: Vec<(Expr, bool)>,
    pub limit: Option<Expr>,
}

#[derive(Clone, Debug)]
pub struct DeletePlan {
    pub access: TableAccess,
    pub filter: Option<Expr>,
    pub order: Vec<(Expr, bool)>,
    pub limit: Option<Expr>,
}

#[derive(Clone, Debug)]
pub enum Stmt {
    Select(Arc<SelectPlan>),
    Insert(Arc<InsertPlan>),
    Update(Arc<UpdatePlan>),
    Delete(Arc<DeletePlan>),
    CreateTable {
        def: crate::catalog::TableDef,
        if_not_exists: bool,
    },
    CreateIndex {
        db: String,
        table: String,
        name: String,
        cols: Vec<String>,
        unique: bool,
        if_not_exists: bool,
    },
    DropTable {
        tables: Vec<(String, String)>,
        if_exists: bool,
    },
    Truncate {
        db: String,
        table: String,
    },
    CreateDatabase {
        name: String,
        if_not_exists: bool,
    },
    DropDatabase {
        name: String,
        if_exists: bool,
    },
    Admin(crate::admin::Admin),
}

impl Stmt {
    pub fn is_read_only(&self) -> bool {
        matches!(self, Stmt::Select(_))
    }
}
