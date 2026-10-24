//! Table definitions and their Tarantool layout.
//!
//! A SQL table `db.t` is the Tarantool space named `"db.t"`, with one field
//! per column (same order, same name) followed by a `bucket_id` field, the
//! primary key as index 0 (`primary`), secondary indexes as 1..n, and a
//! non-unique `bucket_id` index last (the layout vshard and crud expect).
//! The Tarn-side definition is stored as JSON inside `_tarn_catalog` on every
//! shard, next to the space format the Lua side needs.

use crate::types::SqlType;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tarn_common::{SqlError, SqlResult, Value};

pub const BUCKET_FIELD: &str = "bucket_id";

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
    #[serde(skip)]
    pub sql_type: Option<SqlType>,
}

impl ColumnDef {
    pub fn ty(&self) -> &SqlType {
        self.sql_type.as_ref().expect("catalog types are resolved on load")
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexDef {
    pub name: String,
    /// Tarantool index id (0 = primary).
    pub id: u32,
    pub cols: Vec<usize>,
    pub unique: bool,
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
}

impl TableDef {
    pub fn space_name(&self) -> String {
        space_name(&self.db, &self.name)
    }
    pub fn pk(&self) -> &IndexDef {
        &self.indexes[0]
    }
    pub fn bucket_field(&self) -> usize {
        self.columns.len()
    }
    pub fn col_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name.eq_ignore_ascii_case(name))
    }
    pub fn index_by_name(&self, name: &str) -> Option<&IndexDef> {
        self.indexes.iter().find(|i| i.name.eq_ignore_ascii_case(name))
    }
    pub fn resolve_types(&mut self) -> SqlResult<()> {
        for c in &mut self.columns {
            c.sql_type = Some(SqlType::parse(&c.ty)?);
        }
        Ok(())
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
        let mut format = Vec::new();
        for c in &self.columns {
            format.push((c.name.clone(), c.ty().tarantool_type().to_string(), c.nullable));
        }
        format.push((BUCKET_FIELD.to_string(), "unsigned".to_string(), false));
        let mut indexes = Vec::new();
        for idx in &self.indexes {
            let parts = idx
                .cols
                .iter()
                .map(|&c| {
                    let col = &self.columns[c];
                    (col.name.clone(), col.ty().tarantool_type().to_string(), col.nullable)
                })
                .collect();
            indexes.push((if idx.id == 0 { "primary".to_string() } else { idx.name.clone() }, idx.unique, parts));
        }
        indexes.push((BUCKET_FIELD.to_string(), false, vec![(BUCKET_FIELD.to_string(), "unsigned".to_string(), false)]));
        LuaDef {
            name: self.space_name(),
            id: self.space_id,
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
    /// (field name, tarantool type, nullable)
    pub format: Vec<(String, String, bool)>,
    /// (index name, unique, parts (field, type, nullable))
    pub indexes: Vec<(String, bool, Vec<(String, String, bool)>)>,
    pub json: String,
}

pub fn space_name(db: &str, table: &str) -> String {
    format!("{}.{}", db, table)
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
        Catalog { version, databases, tables }
    }
    pub fn get(&self, db: &str, table: &str) -> Option<Arc<TableDef>> {
        self.tables.get(&(db.to_ascii_lowercase(), table.to_ascii_lowercase())).cloned()
    }
    pub fn tables_in(&self, db: &str) -> Vec<Arc<TableDef>> {
        let mut v: Vec<_> = self.tables.values().filter(|t| t.db.eq_ignore_ascii_case(db)).cloned().collect();
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

/// Check the invariants Tarn relies on: a primary key exists and is NOT NULL,
/// the shard key is part of the primary key, and every unique index contains
/// the shard key (MySQL's rule for partitioned tables), so uniqueness can be
/// enforced by a single shard.
pub fn validate(def: &TableDef) -> SqlResult<()> {
    if def.indexes.is_empty() || def.indexes[0].id != 0 || def.indexes[0].cols.is_empty() {
        return Err(SqlError::new(1173, "42000", "This table type requires a primary key"));
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
                format!("A UNIQUE INDEX must include all columns in the table's partitioning function ({})", idx.name),
            ));
        }
    }
    Ok(())
}
