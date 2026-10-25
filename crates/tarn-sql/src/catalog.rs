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
}

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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub foreign_keys: Vec<ForeignKeyDef>,
    /// Tarantool id of the bucket_id index (from the shard at load).
    #[serde(skip)]
    pub bucket_index: Option<u32>,
    #[serde(skip)]
    pub layout: Layout,
}

impl TableDef {
    pub fn space_name(&self) -> String {
        space_name(&self.db, &self.name)
    }
    pub fn pk(&self) -> &IndexDef {
        &self.indexes[0]
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
    /// Resolve column types and compute the layout; called on every
    /// definition loaded or built.
    pub fn resolve_types(&mut self) -> SqlResult<()> {
        for c in &mut self.columns {
            c.sql_type = Some(SqlType::parse(&c.ty)?);
        }
        let n = self.columns.len();
        let col_field: Vec<usize> = (0..n).map(|c| self.field_of(c)).collect();
        let bucket = self.bucket_field();
        let width = col_field
            .iter()
            .copied()
            .chain(std::iter::once(bucket))
            .chain(self.dropped.iter().map(|&f| f as usize))
            .max()
            .map(|m| m + 1)
            .unwrap_or(1);
        let mut field_col = vec![None; width];
        for (c, &f) in col_field.iter().enumerate() {
            field_col[f] = Some(c);
        }
        let mut fills = Vec::with_capacity(n);
        for c in &self.columns {
            fills.push(match &c.fill {
                Some(Fill { value: Some(v) }) => c.ty().coerce(Value::str(v), &c.name)?,
                _ => Value::Null,
            });
        }
        let identity = bucket == n && width == n + 1 && col_field.iter().enumerate().all(|(c, &f)| c == f);
        self.layout = Layout {
            identity,
            width,
            bucket,
            col_field,
            field_col,
            fills,
        };
        Ok(())
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
            } else {
                match l.field_col[f] {
                    Some(c) => m.val(&r[c]),
                    None => m.nil(),
                };
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
            .map(|f| l.field_col[f].map(|c| l.fills[c].clone()).unwrap_or(Value::Null))
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
                } else {
                    (dropped_field_name(f), "any".to_string(), true)
                }
            })
            .collect()
    }

    /// Tarantool parts of an index: (field name, type, nullable).
    pub fn index_parts(&self, idx: &IndexDef) -> Vec<(String, String, bool)> {
        idx.cols
            .iter()
            .map(|&c| {
                let col = &self.columns[c];
                (
                    col.name.clone(),
                    col.ty().tarantool_type().to_string(),
                    col.storage_nullable(),
                )
            })
            .collect()
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
                if idx.id == 0 {
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
            name: self.space_name(),
            id: self.space_id,
            engine: self.engine.name(),
            format,
            indexes,
            json: serde_json::to_string(self).expect("table def serializes"),
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
}

pub fn space_name(db: &str, table: &str) -> String {
    format!("{}.{}", db, table)
}

/// Format name of the field of a dropped column.
pub fn dropped_field_name(field: usize) -> String {
    format!("_dropped_{}", field)
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

pub fn parse_def_json(json: &str) -> SqlResult<TableDef> {
    let mut d: TableDef =
        serde_json::from_str(json).map_err(|e| SqlError::internal(format!("bad catalog entry: {}", e)))?;
    d.resolve_types()?;
    Ok(d)
}

/// Check the invariants Tarn relies on: no column takes the hidden row id's
/// name (1166, as in InnoDB), a primary key exists and is NOT NULL,
/// the shard key is part of the primary key, and every unique index contains
/// the shard key (MySQL's rule for partitioned tables), so uniqueness can be
/// enforced by a single shard.
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
    for idx in &def.indexes[1..] {
        if idx.unique && !def.shard_key.iter().all(|c| idx.cols.contains(c)) {
            return Err(SqlError::new(
                1503,
                "HY000",
                format!(
                    "A UNIQUE INDEX must include all columns in the table's partitioning function ({})",
                    idx.name
                ),
            ));
        }
    }
    Ok(())
}
