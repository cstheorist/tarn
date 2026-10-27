//! Table definitions and their Tarantool layout.
//!
//! A SQL table `db.t` is the Tarantool space named `"db.t"`, with one field
//! per column (same order, same name) followed by a `bucket_id` field, the
//! primary key as index 0 (`primary`), secondary indexes as 1..n, and a
//! non-unique `bucket_id` index last (the layout vshard and crud expect).
//! The Tarn-side definition is stored as JSON inside `_tarn_catalog` on every
//! shard, next to the space format the Lua side needs.
//!
//! ALTER TABLE changes no stored row. A column it adds gets a new field
//! after the last one (its value in older, shorter tuples is the column's
//! `fill`); a column it drops keeps its field, renamed `_dropped_<n>` and
//! written as nil from then on; renames and reordering change only the
//! definition. So a column's field (`ColumnDef::field`) is not always its
//! position, and `Layout` maps between the two.
//!
//! A case-insensitive text column (issue #13) that is part of an index has a
//! second field, its *weight field* (`_weight_<field>`, varbinary): the
//! column's comparison key under its collation (`Collation::key`), written
//! with every row. Its indexes are built on that field instead of the
//! column's, so the storage orders, compares and enforces uniqueness exactly
//! as the SQL layer's collation does, and the shard key hashes it. Index
//! keys (lookups, read sets, locks, the write buffer) are therefore in that
//! form: `TableDef::index_key`.
//!
//! A unique key that does not contain the shard key (issue #14) cannot be
//! enforced by one shard. It gets a *global index*: a hidden space
//! `_tarn_uix_<table space id>_<index id>` (`TableDef::globals`) with
//! one entry per row whose key has no NULL part: the key's columns followed
//! by the row's primary-key columns, keyed by the key and sharded by it. Its
//! entries are written in the same transaction as the rows, so the commit
//! protocol keeps them exact and the entry's primary key enforces the
//! constraint across shards. The table keeps its own unique index too, which
//! is unique within each shard.

use crate::collation::Collation;
use crate::mpw::Mp;
use crate::types::SqlType;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tarn_common::{SqlError, SqlResult, Value};

pub const BUCKET_FIELD: &str = "bucket_id";

/// The hidden column that keys a table declared without a primary key or a
/// unique NOT NULL key (InnoDB's implicit row id, under InnoDB's reserved
/// name).
pub const ROW_ID: &str = "DB_ROW_ID";

/// The Tarantool storage engine of a table: memtx keeps every row in RAM,
/// vinyl is an LSM tree on disk with a RAM cache (issue #23). One
/// Tarantool transaction may write both (memtx MVCC is on), so the commit
/// protocol's own spaces stay in memtx whatever the tables use.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Engine {
    #[default]
    Memtx,
    Vinyl,
}

impl Engine {
    pub fn name(self) -> &'static str {
        match self {
            Engine::Memtx => "memtx",
            Engine::Vinyl => "vinyl",
        }
    }

    /// The engine a table option `ENGINE = name` asks for: Tarantool's own
    /// names, MySQL's in-memory engine as memtx, and the engines MySQL
    /// dumps name (InnoDB, MyISAM, ...) as the cluster's default (None).
    /// Anything else is MySQL's 1286.
    pub fn from_option(name: &str) -> SqlResult<Option<Engine>> {
        match name.to_ascii_lowercase().as_str() {
            "memtx" | "memory" | "heap" => Ok(Some(Engine::Memtx)),
            "vinyl" => Ok(Some(Engine::Vinyl)),
            "innodb" | "myisam" | "aria" | "rocksdb" | "tokudb" | "default" => Ok(None),
            _ => Err(SqlError::new(
                1286,
                "42000",
                format!("Unknown storage engine '{}'", name),
            )),
        }
    }

    pub fn parse(name: &str) -> Option<Engine> {
        match name.to_ascii_lowercase().as_str() {
            "memtx" => Some(Engine::Memtx),
            "vinyl" => Some(Engine::Vinyl),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    /// SQL type in canonical text form (see `SqlType::display`).
    pub ty: String,
    pub nullable: bool,
    /// Default as text (`None` = no default); "CURRENT_TIMESTAMP" is special.
    pub default: Option<String>,
    #[serde(default)]
    pub default_null: bool,
    pub auto_increment: bool,
    /// Tarantool field number (0-based). None: the column's position (every
    /// definition written before ALTER TABLE existed).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field: Option<u32>,
    /// Nullability of the field in the Tarantool format, when it differs
    /// from the column's: fields added by ALTER TABLE are nullable there (old
    /// tuples end before them) and a field stays nullable once ALTER TABLE
    /// has made it so. NOT NULL is enforced by the SQL layer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_nullable: Option<bool>,
    /// A column added by ALTER TABLE: its value in rows written before it
    /// existed, whose tuples end before its field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fill: Option<Fill>,
    /// The implicit row id (`ROW_ID`): stored and indexed, but not part of
    /// `SELECT *`, `INSERT` without a column list, `DESCRIBE` or `SHOW
    /// CREATE TABLE`, and statements cannot name it.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub hidden: bool,
    /// Collation of a text column, as declared or inherited (lower case).
    /// None: utf8mb4_bin (also every definition written before collations
    /// existed).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collation: Option<String>,
    /// Field holding the column's weight string: a case-insensitive column
    /// that is part of an index (see the module comment).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub weight: Option<u32>,
    #[serde(skip)]
    pub sql_type: Option<SqlType>,
}

/// The value of an added column in older rows: text as for `default` (None:
/// NULL).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Fill {
    pub value: Option<String>,
}

impl ColumnDef {
    pub fn ty(&self) -> &SqlType {
        self.sql_type.as_ref().expect("catalog types are resolved on load")
    }
    pub fn storage_nullable(&self) -> bool {
        self.storage_nullable.unwrap_or(self.nullable)
    }
    /// The collation values of this column compare under: its own for text,
    /// `Binary` for binary strings, utf8mb4_bin (unused) otherwise.
    pub fn coll(&self) -> Collation {
        let t = self.ty();
        if t.is_binary() {
            Collation::Binary
        } else if t.is_text() {
            self.collation
                .as_deref()
                .and_then(Collation::from_name)
                .unwrap_or(Collation::Utf8Bin)
        } else {
            Collation::Utf8Bin
        }
    }
    /// Name of the collation (text columns).
    pub fn collation_name(&self) -> &str {
        self.collation.as_deref().unwrap_or("utf8mb4_bin")
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexDef {
    pub name: String,
    /// Tarantool index id (0 = primary).
    pub id: u32,
    pub cols: Vec<usize>,
    pub unique: bool,
    /// INVISIBLE: maintained, but the planner does not use it.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub invisible: bool,
    /// A unique key without the shard key: the space id of its global index
    /// (see the module comment); 0 until CREATE TABLE allocates it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub global: Option<u32>,
    /// The index's COMMENT.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub comment: String,
}

/// The global index of a unique key (`IndexDef::global`).
#[derive(Clone, Debug)]
pub struct GlobalIndex {
    /// Position of the unique key in `TableDef::indexes`.
    pub pos: usize,
    /// The hidden space, as a table: its primary key is the unique key.
    pub table: Arc<TableDef>,
    /// The table's column held by each entry column.
    pub src: Vec<usize>,
}

impl GlobalIndex {
    /// Number of key columns (the rest of an entry is the row's primary key).
    pub fn key_len(&self) -> usize {
        self.table.indexes[0].cols.len()
    }
    /// The entry of row `row` of table `t`, its values as the table's
    /// indexes hold them (`TableDef::key_part`); None when the key has a
    /// NULL part (MySQL allows any number of those).
    pub fn entry(&self, t: &TableDef, row: &[Value]) -> Option<Vec<Value>> {
        if self.src[..self.key_len()].iter().any(|&c| row[c].is_null()) {
            return None;
        }
        Some(self.src.iter().map(|&c| t.key_part(c, &row[c])).collect())
    }
    /// The primary key of the row an entry names, in the table's key order
    /// (as `TableDef::pk_key` gives it).
    pub fn holder_pk(&self, t: &TableDef, entry: &[Value]) -> Vec<Value> {
        t.indexes[0]
            .cols
            .iter()
            .map(|c| {
                entry[self
                    .src
                    .iter()
                    .position(|s| s == c)
                    .expect("entry holds the primary key")]
                .clone()
            })
            .collect()
    }
}

/// Name of a global index's hidden space (without the database).
pub const GLOBAL_PREFIX: &str = "_tarn_uix_";

/// A FOREIGN KEY constraint: recorded (SHOW CREATE TABLE, DROP FOREIGN
/// KEY), not enforced.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ForeignKeyDef {
    pub name: String,
    pub cols: Vec<String>,
    pub ref_db: String,
    pub ref_table: String,
    pub ref_cols: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_delete: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_update: Option<String>,
}

/// Where each column lives in a stored tuple (see the module comment).
#[derive(Clone, Debug, Default)]
pub struct Layout {
    /// Every column at its own position, bucket_id right after them, and no
    /// other field: tuples map to rows directly.
    pub identity: bool,
    /// Number of fields of a tuple written now.
    pub width: usize,
    /// The bucket_id field.
    pub bucket: usize,
    /// Field of each column.
    pub col_field: Vec<usize>,
    /// Column of each field (None: bucket_id or a dropped column).
    pub field_col: Vec<Option<usize>>,
    /// Value of each column when the tuple ends before its field.
    pub fills: Vec<Value>,
    /// Collation of each column (see `ColumnDef::coll`).
    pub colls: Vec<Collation>,
    /// Weight field of each column (case-insensitive columns in an index).
    pub col_weight: Vec<Option<usize>>,
    /// Column whose weight string each field holds.
    pub field_weight: Vec<Option<usize>>,
    /// The FLOAT and DOUBLE columns: a value that is an integer is stored
    /// as that integer (the storage procedures store what Lua's serializer
    /// makes of a double), and read as a double (`Rd::row_of`).
    pub floats: Vec<usize>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TableDef {
    pub db: String,
    pub name: String,
    pub space_id: u32,
    pub columns: Vec<ColumnDef>,
    /// Index 0 is the primary key; the rest are secondary indexes.
    pub indexes: Vec<IndexDef>,
    /// Columns hashed into the bucket id (default: the primary key).
    pub shard_key: Vec<usize>,
    pub auto_inc: Option<usize>,
    #[serde(default)]
    pub comment: String,
    #[serde(default)]
    pub engine: Engine,
    /// Catalog version of the last ALTER TABLE: the shards refuse writes
    /// made with an older definition (1412).
    #[serde(default)]
    pub ver: u64,
    /// The bucket_id field. None: right after the columns (no ALTER TABLE).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bucket: Option<u32>,
    /// Fields of dropped columns (named `_dropped_<n>` in the format).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dropped: Vec<u32>,
    /// Default collation of the table's text columns (DEFAULT CHARSET /
    /// COLLATE). None: utf8mb4_bin.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collation: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub foreign_keys: Vec<ForeignKeyDef>,
    /// Tarantool id of the bucket_id index (from the shard at load).
    #[serde(skip)]
    pub bucket_index: Option<u32>,
    #[serde(skip)]
    pub layout: Layout,
    /// The global indexes of unique keys without the shard key.
    #[serde(skip)]
    pub globals: Vec<GlobalIndex>,
    /// A global index's space: the table it indexes (its space id and
    /// definition version, which the entries' writes are fenced with).
    #[serde(skip)]
    pub base: Option<(u32, u64)>,
}

impl TableDef {
    pub fn space_name(&self) -> String {
        space_name(&self.db, &self.name)
    }
    pub fn pk(&self) -> &IndexDef {
        &self.indexes[0]
    }
    /// A table of `information_schema` (read from the catalog, never written).
    pub fn is_virtual(&self) -> bool {
        self.space_id == INFORMATION_SCHEMA_SPACE
    }
    pub fn bucket_field(&self) -> usize {
        self.bucket.map(|b| b as usize).unwrap_or(self.columns.len())
    }
    pub fn field_of(&self, c: usize) -> usize {
        self.columns[c].field.map(|f| f as usize).unwrap_or(c)
    }
    /// Indexes the planner may use (not INVISIBLE).
    pub fn usable_indexes(&self) -> impl Iterator<Item = &IndexDef> {
        self.indexes.iter().filter(|i| !i.invisible)
    }
    /// The column a statement names (never the hidden row id).
    pub fn col_index(&self, name: &str) -> Option<usize> {
        self.columns
            .iter()
            .position(|c| !c.hidden && c.name.eq_ignore_ascii_case(name))
    }
    /// The columns statements see, with their positions.
    pub fn visible_columns(&self) -> impl Iterator<Item = (usize, &ColumnDef)> {
        self.columns.iter().enumerate().filter(|(_, c)| !c.hidden)
    }
    /// The hidden row id column, if the table has one.
    pub fn row_id_col(&self) -> Option<usize> {
        self.columns.iter().position(|c| c.hidden)
    }
    /// The primary key was declared (`PRIMARY KEY`), rather than taken from
    /// a unique NOT NULL key or the hidden row id.
    pub fn declared_pk(&self) -> bool {
        self.indexes[0].name == "PRIMARY"
    }
    pub fn index_by_name(&self, name: &str) -> Option<&IndexDef> {
        self.indexes.iter().find(|i| i.name.eq_ignore_ascii_case(name))
    }
    /// Whether unique index `idx` needs a global index: the shard key is
    /// not part of it, so one shard cannot enforce it.
    pub fn needs_global(&self, idx: &IndexDef) -> bool {
        idx.id != 0 && idx.unique && !self.shard_key.iter().all(|c| idx.cols.contains(c))
    }
    /// The global index of the unique key at position `pos`, if it has one.
    pub fn global_of(&self, pos: usize) -> Option<&GlobalIndex> {
        self.globals.iter().find(|g| g.pos == pos)
    }
    /// Whether a global index holds a case-insensitive column (by its
    /// weight string).
    pub fn global_on_weights(&self) -> bool {
        self.globals
            .iter()
            .any(|g| g.src[..g.key_len()].iter().any(|&c| self.columns[c].weight.is_some()))
    }
    /// Whether column `c` is held by a global index.
    pub fn in_global(&self, c: usize) -> bool {
        self.globals.iter().any(|g| g.src.contains(&c))
    }
    /// The columns of the entries of unique key `idx`: its own, then the
    /// primary key's that it lacks.
    pub fn global_cols(&self, idx: &IndexDef) -> Vec<usize> {
        let mut cols = idx.cols.clone();
        for &c in &self.indexes[0].cols {
            if !cols.contains(&c) {
                cols.push(c);
            }
        }
        cols
    }
    /// The hidden space of the global index at position `pos`, as a table.
    fn global_table(&self, pos: usize) -> SqlResult<GlobalIndex> {
        let idx = &self.indexes[pos];
        let src = self.global_cols(idx);
        let columns = src
            .iter()
            .map(|&c| {
                let s = &self.columns[c];
                ColumnDef {
                    name: s.name.clone(),
                    // entries hold index keys: a case-insensitive column's
                    // weight string, so its collation decides uniqueness
                    ty: if s.weight.is_some() {
                        "varbinary(3072)".to_string()
                    } else {
                        s.ty.clone()
                    },
                    // entries never have a NULL key part
                    nullable: false,
                    default: None,
                    default_null: false,
                    auto_increment: false,
                    field: None,
                    storage_nullable: None,
                    fill: None,
                    hidden: false,
                    collation: None,
                    weight: None,
                    sql_type: None,
                }
            })
            .collect();
        let k = idx.cols.len();
        let mut t = TableDef {
            db: self.db.clone(),
            name: format!("{}{}_{}", GLOBAL_PREFIX, self.space_id, idx.id),
            space_id: idx.global.unwrap_or(0),
            columns,
            indexes: vec![IndexDef {
                name: idx.name.clone(),
                id: 0,
                cols: (0..k).collect(),
                unique: true,
                invisible: false,
                global: None,
                comment: String::new(),
            }],
            shard_key: (0..k).collect(),
            auto_inc: None,
            comment: String::new(),
            engine: self.engine,
            ver: self.ver,
            bucket: None,
            dropped: Vec::new(),
            collation: None,
            foreign_keys: Vec::new(),
            bucket_index: Some(1),
            layout: Default::default(),
            globals: Vec::new(),
            base: Some((self.space_id, self.ver)),
        };
        t.resolve_types()?;
        Ok(GlobalIndex {
            pos,
            table: Arc::new(t),
            src,
        })
    }
    /// Resolve column types and compute the layout; called on every
    /// definition loaded or built.
    pub fn resolve_types(&mut self) -> SqlResult<()> {
        for c in &mut self.columns {
            c.sql_type = Some(SqlType::parse(&c.ty)?);
        }
        let n = self.columns.len();
        let col_field: Vec<usize> = (0..n).map(|c| self.field_of(c)).collect();
        let col_weight: Vec<Option<usize>> = self.columns.iter().map(|c| c.weight.map(|f| f as usize)).collect();
        let bucket = self.bucket_field();
        let width = col_field
            .iter()
            .copied()
            .chain(std::iter::once(bucket))
            .chain(self.dropped.iter().map(|&f| f as usize))
            .chain(col_weight.iter().flatten().copied())
            .max()
            .map(|m| m + 1)
            .unwrap_or(1);
        let mut field_col = vec![None; width];
        for (c, &f) in col_field.iter().enumerate() {
            field_col[f] = Some(c);
        }
        let mut field_weight = vec![None; width];
        for (c, f) in col_weight.iter().enumerate() {
            if let Some(f) = f {
                field_weight[*f] = Some(c);
            }
        }
        let mut fills = Vec::with_capacity(n);
        for c in &self.columns {
            fills.push(match &c.fill {
                Some(Fill { value: Some(v) }) => c.ty().coerce(Value::str(v), &c.name)?,
                _ => Value::Null,
            });
        }
        let colls: Vec<Collation> = self.columns.iter().map(|c| c.coll()).collect();
        let identity = bucket == n && width == n + 1 && col_field.iter().enumerate().all(|(c, &f)| c == f);
        let floats = (0..n)
            .filter(|&c| self.columns[c].ty().tarantool_type() == "double")
            .collect();
        self.layout = Layout {
            identity,
            width,
            bucket,
            col_field,
            field_col,
            fills,
            colls,
            col_weight,
            field_weight,
            floats,
        };
        self.resolve_globals()
    }

    /// Build `globals` from the indexes (again once `space_id` is known).
    pub fn resolve_globals(&mut self) -> SqlResult<()> {
        self.globals = (0..self.indexes.len())
            .filter(|&i| self.indexes[i].global.is_some_and(|g| g != 0))
            .map(|i| self.global_table(i))
            .collect::<SqlResult<_>>()?;
        Ok(())
    }

    /// The value an index holds for column `c` given the column's value:
    /// the weight string of a column with a weight field, else the value.
    pub fn key_part(&self, c: usize, v: &Value) -> Value {
        match (self.layout.col_weight.get(c), v) {
            (Some(Some(_)), Value::Str(b)) => Value::Bin(self.layout.colls[c].key(b)),
            _ => v.clone(),
        }
    }

    /// A lookup value for column `c` of an index, converted to what the
    /// index holds; None if no stored value can equal it.
    pub fn lookup_part(&self, c: usize, v: &Value) -> Option<Value> {
        crate::exec::coerce_key_part(self.columns[c].ty(), v).map(|x| self.key_part(c, &x))
    }

    /// Index key of a row (as the index holds it).
    pub fn index_key(&self, idx: &IndexDef, row: &[Value]) -> Vec<Value> {
        idx.cols.iter().map(|&c| self.key_part(c, &row[c])).collect()
    }

    /// Primary key of a row, as the primary index holds it: the identity of
    /// the row in the write buffer and in row locks.
    pub fn pk_key(&self, row: &[Value]) -> Vec<Value> {
        self.index_key(&self.indexes[0], row)
    }

    /// The values a row's shard key hashes (see route.rs).
    pub fn shard_key_of(&self, row: &[Value]) -> Vec<Value> {
        self.shard_key.iter().map(|&c| self.key_part(c, &row[c])).collect()
    }

    /// The shard key of the row with primary key `pk` (as `pk_key` gives
    /// it): the shard key is part of the primary key.
    pub fn shard_key_of_pk(&self, pk: &[Value]) -> Vec<Value> {
        let cols = &self.indexes[0].cols;
        self.shard_key
            .iter()
            .map(|c| {
                cols.iter()
                    .position(|x| x == c)
                    .and_then(|p| pk.get(p))
                    .cloned()
                    .unwrap_or(Value::Null)
            })
            .collect()
    }

    /// Order of two values of column `c` as its indexes order them.
    pub fn key_cmp(&self, c: usize, a: &Value, b: &Value) -> std::cmp::Ordering {
        crate::types::sort_cmp_coll(a, b, self.layout.colls[c])
    }

    /// Do two rows hold the same value of unique key `idx`? (A NULL part
    /// never equals anything.)
    pub fn same_key(&self, idx: &IndexDef, a: &[Value], b: &[Value]) -> bool {
        idx.cols
            .iter()
            .all(|&c| !a[c].is_null() && !b[c].is_null() && self.key_cmp(c, &a[c], &b[c]) == std::cmp::Ordering::Equal)
    }

    /// Give every case-insensitive column in an index a weight field (after
    /// the last field). Columns with `fresh(c)` false must have one already
    /// (their stored rows would need rewriting): error 1235 otherwise.
    pub fn assign_weight_fields(&mut self, fresh: &dyn Fn(&ColumnDef) -> bool) -> SqlResult<()> {
        let mut next = self.layout.width.max(self.bucket_field() + 1) as u32;
        for c in &self.columns {
            if let Some(f) = c.field {
                next = next.max(f + 1);
            }
            if let Some(f) = c.weight {
                next = next.max(f + 1);
            }
        }
        for &f in &self.dropped {
            next = next.max(f + 1);
        }
        let mut need: Vec<usize> = Vec::new();
        for idx in &self.indexes {
            for &c in &idx.cols {
                if self.columns[c].coll().is_ci() && self.columns[c].weight.is_none() && !need.contains(&c) {
                    need.push(c);
                }
            }
        }
        need.sort();
        for c in need {
            if !fresh(&self.columns[c]) {
                return Err(SqlError::unsupported(format!(
                    "an index on case-insensitive column '{}' that has none yet (its rows would have to be rewritten)",
                    self.columns[c].name
                )));
            }
            self.columns[c].weight = Some(next);
            next += 1;
        }
        self.resolve_types()
    }

    /// Write a row as a stored tuple (columns at their fields, bucket_id,
    /// nil for dropped columns).
    pub fn encode_tuple(&self, r: &[Value], bucket_id: u64, m: &mut Mp) {
        let l = &self.layout;
        if l.identity {
            m.arr(r.len() + 1);
            for v in r {
                m.val(v);
            }
            m.uint(bucket_id);
            return;
        }
        m.arr(l.width);
        for f in 0..l.width {
            if f == l.bucket {
                m.uint(bucket_id);
            } else if let Some(c) = l.field_col[f] {
                m.val(&r[c]);
            } else if let Some(c) = l.field_weight[f] {
                m.val(&self.key_part(c, &r[c]));
            } else {
                m.nil();
            }
        }
    }

    /// For Tarantool update ops on rows that may predate added columns: the
    /// number of fields every stored tuple has, and the values of the later
    /// fields for tuples that end early (None: no field was added).
    pub fn update_pad(&self) -> Option<(usize, Vec<Value>)> {
        let l = &self.layout;
        if l.width <= l.bucket + 1 {
            return None;
        }
        let pad = (l.bucket + 1..l.width)
            .map(|f| match (l.field_col[f], l.field_weight[f]) {
                (Some(c), _) => l.fills[c].clone(),
                (None, Some(c)) => self.key_part(c, &l.fills[c]),
                _ => Value::Null,
            })
            .collect();
        Some((l.bucket + 1, pad))
    }

    /// The Tarantool space format: (field name, type, nullable) per field.
    pub fn lua_format(&self) -> Vec<(String, String, bool)> {
        let l = &self.layout;
        (0..l.width)
            .map(|f| {
                if f == l.bucket {
                    (BUCKET_FIELD.to_string(), "unsigned".to_string(), false)
                } else if let Some(c) = l.field_col[f] {
                    let col = &self.columns[c];
                    (
                        col.name.clone(),
                        col.ty().tarantool_type().to_string(),
                        col.storage_nullable(),
                    )
                } else if let Some(c) = l.field_weight[f] {
                    (
                        weight_field_name(f),
                        "varbinary".to_string(),
                        self.columns[c].storage_nullable(),
                    )
                } else {
                    (dropped_field_name(f), "any".to_string(), true)
                }
            })
            .collect()
    }

    /// Tarantool parts of an index: (field name, type, nullable). A column
    /// with a weight field is indexed by that field.
    pub fn index_parts(&self, idx: &IndexDef) -> Vec<(String, String, bool)> {
        idx.cols
            .iter()
            .map(|&c| {
                let col = &self.columns[c];
                match self.layout.col_weight[c] {
                    Some(f) => (weight_field_name(f), "varbinary".to_string(), col.storage_nullable()),
                    None => (
                        col.name.clone(),
                        col.ty().tarantool_type().to_string(),
                        col.storage_nullable(),
                    ),
                }
            })
            .collect()
    }

    /// Like `index_parts`, by 1-based field number (ALTER TABLE builds).
    pub fn index_part_fields(&self, idx: &IndexDef) -> Vec<(u32, String, bool)> {
        idx.cols
            .iter()
            .map(|&c| {
                let col = &self.columns[c];
                match self.layout.col_weight[c] {
                    Some(f) => (f as u32 + 1, "varbinary".to_string(), col.storage_nullable()),
                    None => (
                        self.layout.col_field[c] as u32 + 1,
                        col.ty().tarantool_type().to_string(),
                        col.storage_nullable(),
                    ),
                }
            })
            .collect()
    }

    /// Name of the table's default collation.
    pub fn collation_name(&self) -> &str {
        self.collation.as_deref().unwrap_or("utf8mb4_bin")
    }
    pub fn is_pk_col(&self, c: usize) -> bool {
        self.indexes[0].cols.contains(&c)
    }
    pub fn default_value(&self, c: usize) -> Option<Value> {
        let col = &self.columns[c];
        if col.default_null {
            return Some(Value::Null);
        }
        match &col.default {
            None => {
                if col.nullable {
                    Some(Value::Null)
                } else {
                    None
                }
            }
            Some(d) if d.eq_ignore_ascii_case("CURRENT_TIMESTAMP") => Some(Value::str(&crate::expr::now_string())),
            Some(d) => Some(Value::str(d)),
        }
    }

    /// The map handed to the Lua `tarn_create_table` (msgpack-encoded by the
    /// caller): name, id, format, indexes, plus this definition as JSON.
    pub fn lua_def(&self) -> LuaDef {
        let format = self.lua_format();
        let mut indexes = Vec::new();
        for idx in &self.indexes {
            let parts = self.index_parts(idx);
            indexes.push((
                // A global index's space names its primary key after the
                // unique key, for the storage's duplicate-key messages.
                if idx.id == 0 && self.base.is_none() {
                    "primary".to_string()
                } else {
                    idx.name.clone()
                },
                idx.unique,
                parts,
            ));
        }
        indexes.push((
            BUCKET_FIELD.to_string(),
            false,
            vec![(BUCKET_FIELD.to_string(), "unsigned".to_string(), false)],
        ));
        LuaDef {
            // (a global index's space has no database in its name)
            name: if self.base.is_some() {
                self.name.clone()
            } else {
                self.space_name()
            },
            id: self.space_id,
            engine: self.engine.name(),
            format,
            indexes,
            json: serde_json::to_string(self).expect("table def serializes"),
            uix: self.globals.iter().map(|g| g.table.lua_def()).collect(),
        }
    }
}

/// What `tarn_create_table` receives. Encoded to msgpack by the server crate.
pub struct LuaDef {
    pub name: String,
    pub id: u32,
    pub engine: &'static str,
    /// (field name, tarantool type, nullable)
    pub format: Vec<(String, String, bool)>,
    /// (index name, unique, parts (field, type, nullable))
    pub indexes: Vec<(String, bool, Vec<(String, String, bool)>)>,
    pub json: String,
    /// The hidden spaces of the global indexes (their `json` is unused).
    pub uix: Vec<LuaDef>,
}

pub fn space_name(db: &str, table: &str) -> String {
    format!("{}.{}", db, table)
}

/// Format name of the field of a dropped column.
pub fn dropped_field_name(field: usize) -> String {
    format!("_dropped_{}", field)
}

/// Format name of a weight field.
pub fn weight_field_name(field: usize) -> String {
    format!("_weight_{}", field)
}

/// Immutable snapshot of all table definitions; replaced wholesale on DDL.
#[derive(Default, Clone)]
pub struct Catalog {
    pub version: u64,
    pub databases: Vec<String>,
    tables: HashMap<(String, String), Arc<TableDef>>,
}

impl Catalog {
    pub fn new(version: u64, databases: Vec<String>, defs: Vec<TableDef>) -> Catalog {
        let mut tables = HashMap::new();
        for d in defs {
            tables.insert((d.db.to_ascii_lowercase(), d.name.to_ascii_lowercase()), Arc::new(d));
        }
        Catalog {
            version,
            databases,
            tables,
        }
    }
    pub fn get(&self, db: &str, table: &str) -> Option<Arc<TableDef>> {
        self.tables
            .get(&(db.to_ascii_lowercase(), table.to_ascii_lowercase()))
            .cloned()
    }
    pub fn tables_in(&self, db: &str) -> Vec<Arc<TableDef>> {
        let mut v: Vec<_> = self
            .tables
            .values()
            .filter(|t| t.db.eq_ignore_ascii_case(db))
            .cloned()
            .collect();
        v.sort_by(|a, b| a.name.cmp(&b.name));
        v
    }
    pub fn all(&self) -> impl Iterator<Item = &Arc<TableDef>> {
        self.tables.values()
    }
    pub fn has_db(&self, db: &str) -> bool {
        self.databases.iter().any(|d| d.eq_ignore_ascii_case(db))
    }
}

/// Space id of the tables of `information_schema`: virtual, read from the
/// catalog instead of storage (`information_schema_rows`), never written.
pub const INFORMATION_SCHEMA_SPACE: u32 = u32::MAX;

pub const INFORMATION_SCHEMA: &str = "information_schema";

/// The columns of `information_schema.TABLES`, as MySQL 8 has them.
const IS_TABLES: &[(&str, &str)] = &[
    ("TABLE_CATALOG", "varchar(64)"),
    ("TABLE_SCHEMA", "varchar(64)"),
    ("TABLE_NAME", "varchar(64)"),
    ("TABLE_TYPE", "varchar(64)"),
    ("ENGINE", "varchar(64)"),
    ("VERSION", "bigint unsigned"),
    ("ROW_FORMAT", "varchar(10)"),
    ("TABLE_ROWS", "bigint unsigned"),
    ("AVG_ROW_LENGTH", "bigint unsigned"),
    ("DATA_LENGTH", "bigint unsigned"),
    ("MAX_DATA_LENGTH", "bigint unsigned"),
    ("INDEX_LENGTH", "bigint unsigned"),
    ("DATA_FREE", "bigint unsigned"),
    ("AUTO_INCREMENT", "bigint unsigned"),
    ("CREATE_TIME", "datetime"),
    ("UPDATE_TIME", "datetime"),
    ("CHECK_TIME", "datetime"),
    ("TABLE_COLLATION", "varchar(64)"),
    ("CHECKSUM", "bigint unsigned"),
    ("CREATE_OPTIONS", "varchar(256)"),
    ("TABLE_COMMENT", "varchar(2048)"),
];

/// The columns of `information_schema.TARN_MOVES`: the moves of the current
/// (or last) round of the rebalancer (`SHOW TARN MOVES`).
pub const IS_TARN_MOVES: &[(&str, &str)] = &[
    ("BUCKET", "bigint unsigned"),
    ("FROM_SHARD", "bigint unsigned"),
    ("TO_SHARD", "bigint unsigned"),
    ("BYTES", "bigint unsigned"),
    ("STATE", "varchar(16)"),
    ("ERROR", "varchar(1024)"),
];

/// The definition of an `information_schema` table (`TABLES`, and
/// `TARN_MOVES`): error 1109 for the others, as in MySQL.
pub fn information_schema_table(name: &str) -> SqlResult<TableDef> {
    let moves = name.eq_ignore_ascii_case("TARN_MOVES");
    if !name.eq_ignore_ascii_case("TABLES") && !moves {
        return Err(SqlError::new(
            1109,
            "42S02",
            format!("Unknown table '{}' in {}", name, INFORMATION_SCHEMA),
        ));
    }
    let (spec, key): (&[(&str, &str)], Vec<usize>) = if moves {
        (IS_TARN_MOVES, vec![0])
    } else {
        (IS_TABLES, vec![1, 2])
    };
    let columns = spec
        .iter()
        .map(|(n, ty)| ColumnDef {
            name: n.to_string(),
            ty: ty.to_string(),
            nullable: !matches!(
                *n,
                "TABLE_CATALOG" | "TABLE_SCHEMA" | "TABLE_NAME" | "TABLE_TYPE" | "BUCKET" | "STATE"
            ),
            default: None,
            default_null: false,
            auto_increment: false,
            field: None,
            storage_nullable: None,
            fill: None,
            hidden: false,
            collation: None,
            weight: None,
            sql_type: None,
        })
        .collect();
    let mut def = TableDef {
        db: INFORMATION_SCHEMA.into(),
        name: if moves { "TARN_MOVES" } else { "TABLES" }.into(),
        space_id: INFORMATION_SCHEMA_SPACE,
        columns,
        indexes: vec![IndexDef {
            name: "PRIMARY".into(),
            id: 0,
            cols: key.clone(),
            unique: true,
            invisible: false,
            global: None,
            comment: String::new(),
        }],
        shard_key: key,
        auto_inc: None,
        comment: String::new(),
        engine: Engine::Memtx,
        ver: 0,
        bucket: None,
        dropped: Vec::new(),
        foreign_keys: Vec::new(),
        bucket_index: None,
        collation: None,
        layout: Default::default(),
        globals: Vec::new(),
        base: None,
    };
    def.resolve_types()?;
    Ok(def)
}

/// The rows of `information_schema.TABLES`: one per table, and one for the
/// view itself. Sizes and row counts are not tracked (NULL).
pub fn information_schema_rows(cat: &Catalog) -> Vec<Vec<Value>> {
    let mut tables: Vec<&Arc<TableDef>> = cat.all().collect();
    tables.sort_by(|a, b| (&a.db, &a.name).cmp(&(&b.db, &b.name)));
    let mut rows = Vec::with_capacity(tables.len() + 1);
    let mut row = |db: &str, name: &str, ty: &str, t: Option<&TableDef>| {
        let mut r = vec![Value::Null; IS_TABLES.len()];
        r[0] = Value::str("def");
        r[1] = Value::str(db);
        r[2] = Value::str(name);
        r[3] = Value::str(ty);
        if let Some(t) = t {
            r[4] = Value::str(t.engine.name());
            r[5] = Value::UInt(10);
            r[6] = Value::str("Fixed");
            r[17] = Value::str("utf8mb4_bin");
            r[19] = Value::str("");
            r[20] = Value::str(&t.comment);
        } else {
            r[20] = Value::str("");
        }
        rows.push(r);
    };
    row(INFORMATION_SCHEMA, "TABLES", "SYSTEM VIEW", None);
    for t in tables {
        row(&t.db, &t.name, "BASE TABLE", Some(t));
    }
    rows
}

pub fn parse_def_json(json: &str) -> SqlResult<TableDef> {
    let mut d: TableDef =
        serde_json::from_str(json).map_err(|e| SqlError::internal(format!("bad catalog entry: {}", e)))?;
    d.resolve_types()?;
    Ok(d)
}

/// Check the invariants Tarn relies on: no column takes the hidden row id's
/// name (1166, as in InnoDB), a primary key exists and is NOT NULL, and the
/// shard key is part of the primary key. (A unique key without the shard key
/// gets a global index: see the module comment.)
pub fn validate(def: &TableDef) -> SqlResult<()> {
    if def.indexes.is_empty() || def.indexes[0].id != 0 || def.indexes[0].cols.is_empty() {
        return Err(SqlError::new(1173, "42000", "This table type requires a primary key"));
    }
    if let Some(c) = def
        .columns
        .iter()
        .find(|c| !c.hidden && c.name.eq_ignore_ascii_case(ROW_ID))
    {
        return Err(SqlError::new(
            1166,
            "42000",
            format!("Incorrect column name '{}'", c.name),
        ));
    }
    for &c in &def.indexes[0].cols {
        if def.columns[c].nullable {
            return Err(SqlError::new(
                1171,
                "42000",
                "All parts of a PRIMARY KEY must be NOT NULL; if you need NULL in a key, use UNIQUE instead",
            ));
        }
    }
    for &c in &def.shard_key {
        if !def.indexes[0].cols.contains(&c) {
            return Err(SqlError::new(
                1503,
                "HY000",
                "A PRIMARY KEY must include all columns in the table's partitioning function",
            ));
        }
    }
    for idx in &def.indexes {
        for &c in &idx.cols {
            if def.columns[c].coll().is_ci() && def.columns[c].weight.is_none() {
                return Err(SqlError::internal(format!(
                    "index {} on case-insensitive column {} without a weight field",
                    idx.name, def.columns[c].name
                )));
            }
        }
    }
    Ok(())
}
