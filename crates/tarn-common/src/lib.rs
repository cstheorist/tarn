//! Types shared by every Tarn crate: SQL values, MySQL column metadata,
//! and the error type that maps onto MySQL error packets.

use bytes::Bytes;
use std::fmt;

/// A SQL value as it flows between the MySQL protocol layer, the executor
/// and the Tarantool storage layer.
///
/// Strings and binary strings are both `Bytes` so rows decoded from msgpack
/// can be passed through without copying. `Str` must hold UTF-8.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Int(i64),
    UInt(u64),
    Float(f64),
    /// Exact decimal in canonical text form, e.g. "-12.340".
    Decimal(String),
    Str(Bytes),
    Bin(Bytes),
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
    pub fn str(s: &str) -> Value {
        Value::Str(Bytes::copy_from_slice(s.as_bytes()))
    }
    /// Best-effort conversion used for integer contexts (LIMIT, ids, ...).
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int(i) => Some(*i),
            Value::UInt(u) => i64::try_from(*u).ok(),
            Value::Float(f) if f.fract() == 0.0 => Some(*f as i64),
            Value::Decimal(d) => d.parse::<f64>().ok().map(|f| f as i64),
            Value::Str(b) | Value::Bin(b) => std::str::from_utf8(b).ok()?.trim().parse().ok(),
            _ => None,
        }
    }
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Int(i) => Some(*i as f64),
            Value::UInt(u) => Some(*u as f64),
            Value::Float(f) => Some(*f),
            Value::Decimal(d) => d.parse().ok(),
            Value::Str(b) | Value::Bin(b) => std::str::from_utf8(b).ok()?.trim().parse().ok(),
            Value::Null => None,
        }
    }
    /// Text rendering used by the MySQL text protocol. `None` for NULL.
    pub fn to_text(&self) -> Option<Bytes> {
        match self {
            Value::Null => None,
            Value::Int(i) => Some(Bytes::from(i.to_string())),
            Value::UInt(u) => Some(Bytes::from(u.to_string())),
            Value::Float(f) => Some(Bytes::from(format_float(*f))),
            Value::Decimal(d) => Some(Bytes::from(d.clone())),
            Value::Str(b) | Value::Bin(b) => Some(b.clone()),
        }
    }
}

/// MySQL prints doubles with the shortest round-trip representation and
/// without a trailing ".0" for integral values.
pub fn format_float(f: f64) -> String {
    if f.is_finite() && f.fract() == 0.0 && f.abs() < 1e15 {
        format!("{}", f as i64)
    } else {
        format!("{}", f)
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.to_text() {
            None => write!(f, "NULL"),
            Some(b) => write!(f, "{}", String::from_utf8_lossy(&b)),
        }
    }
}

/// MySQL protocol column type codes (`enum_field_types`).
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColumnType {
    Decimal = 0,
    Tiny = 1,
    Short = 2,
    Long = 3,
    Float = 4,
    Double = 5,
    Null = 6,
    Timestamp = 7,
    LongLong = 8,
    Int24 = 9,
    Date = 10,
    Time = 11,
    DateTime = 12,
    Year = 13,
    VarChar = 15,
    Bit = 16,
    Json = 245,
    NewDecimal = 246,
    Blob = 252,
    VarString = 253,
    String = 254,
}

impl ColumnType {
    pub fn from_u8(b: u8) -> Option<ColumnType> {
        use ColumnType::*;
        Some(match b {
            0 => Decimal,
            1 => Tiny,
            2 => Short,
            3 => Long,
            4 => Float,
            5 => Double,
            6 => Null,
            7 => Timestamp,
            8 => LongLong,
            9 => Int24,
            10 => Date,
            11 => Time,
            12 => DateTime,
            13 => Year,
            15 => VarChar,
            16 => Bit,
            245 => Json,
            246 => NewDecimal,
            249..=252 => Blob,
            253 => VarString,
            254 => String,
            _ => return None,
        })
    }
}

/// Column flags (subset of MySQL's `*_FLAG` constants).
pub mod column_flags {
    pub const NOT_NULL: u16 = 1;
    pub const PRI_KEY: u16 = 2;
    pub const UNIQUE_KEY: u16 = 4;
    pub const MULTIPLE_KEY: u16 = 8;
    pub const BLOB: u16 = 16;
    pub const UNSIGNED: u16 = 32;
    pub const BINARY: u16 = 128;
    pub const AUTO_INCREMENT: u16 = 512;
    pub const NUM: u16 = 32768;
}

/// utf8mb4_general_ci; what MySQL 8 clients expect for text columns.
pub const CHARSET_UTF8MB4: u16 = 45;
/// The "binary" pseudo-charset used for numeric and blob columns.
pub const CHARSET_BINARY: u16 = 63;

/// Result-set column metadata (Protocol::ColumnDefinition41).
#[derive(Clone, Debug)]
pub struct Column {
    pub schema: String,
    pub table: String,
    pub name: String,
    pub ty: ColumnType,
    pub flags: u16,
    pub charset: u16,
    pub length: u32,
    pub decimals: u8,
}

impl Column {
    pub fn new(name: &str, ty: ColumnType) -> Column {
        let numeric = matches!(
            ty,
            ColumnType::Tiny
                | ColumnType::Short
                | ColumnType::Long
                | ColumnType::LongLong
                | ColumnType::Int24
                | ColumnType::Float
                | ColumnType::Double
                | ColumnType::NewDecimal
                | ColumnType::Decimal
        );
        Column {
            schema: String::new(),
            table: String::new(),
            name: name.to_string(),
            ty,
            flags: if numeric { column_flags::NUM } else { 0 },
            charset: if numeric { CHARSET_BINARY } else { CHARSET_UTF8MB4 },
            length: match ty {
                ColumnType::LongLong => 20,
                ColumnType::Long => 11,
                ColumnType::Double => 22,
                _ => 255,
            },
            decimals: match ty {
                ColumnType::Double | ColumnType::Float => 31,
                _ => 0,
            },
        }
    }
}

/// Error surfaced to MySQL clients as an ERR packet.
#[derive(Clone, Debug, thiserror::Error)]
#[error("{code} ({}): {message}", String::from_utf8_lossy(sqlstate))]
pub struct SqlError {
    pub code: u16,
    pub sqlstate: [u8; 5],
    pub message: String,
    /// A shard refused the request: it does not own the bucket the request
    /// was routed by, or a move fences it (lua/tarn.lua "Buckets"). Nothing
    /// was done; the server loads the bucket map again, and a statement that
    /// can start over runs again with it.
    pub moved: bool,
}

impl SqlError {
    pub fn new(code: u16, sqlstate: &str, message: impl Into<String>) -> SqlError {
        let mut s = *b"HY000";
        if sqlstate.len() == 5 {
            s.copy_from_slice(sqlstate.as_bytes());
        }
        SqlError {
            code,
            sqlstate: s,
            message: message.into(),
            moved: false,
        }
    }
    /// ER_LOCK_DEADLOCK: the conventional "retry your transaction" signal.
    pub fn conflict(detail: impl Into<String>) -> SqlError {
        SqlError::new(
            1213,
            "40001",
            format!(
                "Deadlock found when trying to get lock; try restarting transaction ({})",
                detail.into()
            ),
        )
    }
    /// A request refused for its bucket (see `moved`): a retryable 1213.
    pub fn moved(detail: impl Into<String>) -> SqlError {
        let mut e = SqlError::conflict(detail);
        e.moved = true;
        e
    }
    pub fn parse(msg: impl Into<String>) -> SqlError {
        SqlError::new(1064, "42000", msg)
    }
    pub fn unsupported(msg: impl Into<String>) -> SqlError {
        SqlError::new(
            1235,
            "42000",
            format!("This version of Tarn doesn't yet support '{}'", msg.into()),
        )
    }
    pub fn no_table(db: &str, t: &str) -> SqlError {
        SqlError::new(1146, "42S02", format!("Table '{}.{}' doesn't exist", db, t))
    }
    pub fn dup_key(entry: &str, key: &str) -> SqlError {
        SqlError::new(1062, "23000", format!("Duplicate entry '{}' for key '{}'", entry, key))
    }
    pub fn internal(msg: impl Into<String>) -> SqlError {
        SqlError::new(1105, "HY000", msg)
    }
}

pub type SqlResult<T> = Result<T, SqlError>;
