//! SQL column types, their Tarantool and MySQL-protocol mappings, and the
//! value semantics (coercion, comparison, arithmetic) the executor uses.

use crate::collation::Collation;
use bytes::Bytes;
use std::cmp::Ordering;
use tarn_common::{column_flags, Column, ColumnType, SqlError, SqlResult, Value};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BaseType {
    TinyInt,
    SmallInt,
    MediumInt,
    Int,
    BigInt,
    Float,
    Double,
    Decimal,
    Char,
    VarChar,
    Text,
    Binary,
    VarBinary,
    Blob,
    Date,
    DateTime,
    Timestamp,
    Time,
    Year,
    Json,
    Bool,
}

/// A declared SQL column type.
#[derive(Clone, Debug, PartialEq)]
pub struct SqlType {
    pub base: BaseType,
    pub unsigned: bool,
    /// Display width / string length / decimal precision.
    pub len: Option<u32>,
    /// Decimal scale.
    pub scale: Option<u32>,
}

impl SqlType {
    pub fn new(base: BaseType) -> SqlType {
        SqlType {
            base,
            unsigned: false,
            len: None,
            scale: None,
        }
    }

    /// Parse a MySQL type as rendered by sqlparser, e.g. "INT(11) UNSIGNED",
    /// "VARCHAR(255)", "DECIMAL(10,2)", "BIGINT UNSIGNED", "CHAR(120)".
    pub fn parse(text: &str) -> SqlResult<SqlType> {
        let up = text.trim().to_ascii_uppercase();
        let unsigned = up.contains("UNSIGNED");
        let name_end = up.find(['(', ' ']).unwrap_or(up.len());
        let name = &up[..name_end];
        let (mut len, mut scale) = (None, None);
        if let (Some(a), Some(b)) = (up.find('('), up.find(')')) {
            let inner = &up[a + 1..b];
            let mut parts = inner.split(',').map(|p| p.trim().parse::<u32>().ok());
            len = parts.next().flatten();
            scale = parts.next().flatten();
        }
        use BaseType::*;
        let base = match name {
            "TINYINT" | "INT1" => {
                if len == Some(1) && !unsigned {
                    Bool
                } else {
                    TinyInt
                }
            }
            "BOOL" | "BOOLEAN" => Bool,
            "SMALLINT" | "INT2" => SmallInt,
            "MEDIUMINT" | "INT3" => MediumInt,
            "INT" | "INTEGER" | "INT4" => Int,
            "BIGINT" | "INT8" | "SERIAL" => BigInt,
            "FLOAT" | "FLOAT4" => Float,
            "DOUBLE" | "REAL" | "FLOAT8" => Double,
            "DECIMAL" | "NUMERIC" | "DEC" | "FIXED" => Decimal,
            "CHAR" | "CHARACTER" | "NCHAR" => Char,
            "VARCHAR" | "NVARCHAR" | "CHARACTER VARYING" => VarChar,
            "TEXT" | "TINYTEXT" | "MEDIUMTEXT" | "LONGTEXT" | "ENUM" | "SET" => Text,
            "BINARY" => Binary,
            "VARBINARY" => VarBinary,
            "BLOB" | "TINYBLOB" | "MEDIUMBLOB" | "LONGBLOB" | "BYTEA" => Blob,
            "DATE" => Date,
            "DATETIME" => DateTime,
            "TIMESTAMP" => Timestamp,
            "TIME" => Time,
            "YEAR" => Year,
            "JSON" => Json,
            "BIT" => BigInt,
            other => return Err(SqlError::unsupported(format!("column type {}", other))),
        };
        if name == "SERIAL" {
            return Ok(SqlType {
                base,
                unsigned: true,
                len,
                scale,
            });
        }
        Ok(SqlType {
            base,
            unsigned,
            len,
            scale,
        })
    }

    /// Fractional-second digits of a DATETIME / TIMESTAMP (0 to 6).
    pub fn fsp(&self) -> u32 {
        match self.base {
            BaseType::DateTime | BaseType::Timestamp => self.len.unwrap_or(0).min(6),
            _ => 0,
        }
    }

    pub fn is_integer(&self) -> bool {
        use BaseType::*;
        matches!(self.base, TinyInt | SmallInt | MediumInt | Int | BigInt | Bool | Year)
    }
    pub fn is_numeric(&self) -> bool {
        self.is_integer() || matches!(self.base, BaseType::Float | BaseType::Double | BaseType::Decimal)
    }
    pub fn is_binary(&self) -> bool {
        matches!(self.base, BaseType::Binary | BaseType::VarBinary | BaseType::Blob)
    }
    /// Text that has a collation (CHAR, VARCHAR, TEXT, ENUM, SET).
    pub fn is_text(&self) -> bool {
        matches!(self.base, BaseType::Char | BaseType::VarChar | BaseType::Text)
    }

    /// Tarantool field type used in the space format.
    pub fn tarantool_type(&self) -> &'static str {
        use BaseType::*;
        match self.base {
            TinyInt | SmallInt | MediumInt | Int | BigInt | Bool | Year => {
                if self.unsigned {
                    "unsigned"
                } else {
                    "integer"
                }
            }
            Float | Double => "double",
            Decimal => "decimal",
            Binary | VarBinary | Blob => "varbinary",
            _ => "string",
        }
    }

    /// MySQL protocol column type for result sets.
    pub fn column_type(&self) -> ColumnType {
        use BaseType::*;
        match self.base {
            TinyInt | Bool => ColumnType::Tiny,
            SmallInt => ColumnType::Short,
            MediumInt => ColumnType::Int24,
            Int => ColumnType::Long,
            BigInt => ColumnType::LongLong,
            Float => ColumnType::Float,
            Double => ColumnType::Double,
            Decimal => ColumnType::NewDecimal,
            Char => ColumnType::String,
            VarChar | Binary | VarBinary => ColumnType::VarString,
            Text | Blob | Json => ColumnType::Blob,
            Date => ColumnType::Date,
            DateTime => ColumnType::DateTime,
            Timestamp => ColumnType::Timestamp,
            Time => ColumnType::Time,
            Year => ColumnType::Year,
        }
    }

    pub fn display(&self) -> String {
        use BaseType::*;
        let mut s = match self.base {
            TinyInt => "tinyint".to_string(),
            Bool => "tinyint(1)".to_string(),
            SmallInt => "smallint".to_string(),
            MediumInt => "mediumint".to_string(),
            Int => "int".to_string(),
            BigInt => "bigint".to_string(),
            Float => "float".to_string(),
            Double => "double".to_string(),
            Decimal => format!("decimal({},{})", self.len.unwrap_or(10), self.scale.unwrap_or(0)),
            Char => format!("char({})", self.len.unwrap_or(1)),
            VarChar => format!("varchar({})", self.len.unwrap_or(255)),
            Text => "text".to_string(),
            Binary => format!("binary({})", self.len.unwrap_or(1)),
            VarBinary => format!("varbinary({})", self.len.unwrap_or(255)),
            Blob => "blob".to_string(),
            Date => "date".to_string(),
            DateTime if self.fsp() > 0 => format!("datetime({})", self.fsp()),
            DateTime => "datetime".to_string(),
            Timestamp if self.fsp() > 0 => format!("timestamp({})", self.fsp()),
            Timestamp => "timestamp".to_string(),
            Time => "time".to_string(),
            Year => "year".to_string(),
            Json => "json".to_string(),
        };
        if self.unsigned {
            s.push_str(" unsigned");
        }
        s
    }

    pub fn result_column(&self, name: &str, table: &str, nullable: bool, pk: bool) -> Column {
        let mut c = Column::new(name, self.column_type());
        c.table = table.to_string();
        c.length = match self.base {
            BaseType::Char | BaseType::VarChar | BaseType::Binary | BaseType::VarBinary => self.len.unwrap_or(255) * 4,
            BaseType::Decimal => self.len.unwrap_or(10) + 2,
            _ => c.length,
        };
        if self.base == BaseType::Decimal {
            c.decimals = self.scale.unwrap_or(0) as u8;
        }
        if self.unsigned {
            c.flags |= column_flags::UNSIGNED;
        }
        if !nullable {
            c.flags |= column_flags::NOT_NULL;
        }
        if pk {
            c.flags |= column_flags::PRI_KEY;
        }
        if self.is_binary() {
            c.flags |= column_flags::BINARY;
            c.charset = tarn_common::CHARSET_BINARY;
        }
        c
    }

    /// Convert a value into this column's storage representation (what is
    /// written to Tarantool). Mirrors MySQL's strict-mode assignment rules.
    pub fn coerce(&self, v: Value, col: &str) -> SqlResult<Value> {
        if v.is_null() {
            return Ok(Value::Null);
        }
        let shown = v.to_string();
        let bad = |what: &str| {
            SqlError::new(
                1366,
                "HY000",
                format!("Incorrect {} value: '{}' for column '{}'", what, shown, col),
            )
        };
        let out_of_range = || {
            SqlError::new(
                1264,
                "22003",
                format!("Out of range value for column '{}' at row 1", col),
            )
        };
        let exact_integer = |s: &str| {
            let decimal = crate::decimal::Decimal::parse(s).ok_or_else(|| bad("integer"))?;
            decimal
                .rescale(0)
                .and_then(|s| s.parse::<i128>().ok())
                .ok_or_else(out_of_range)
        };
        use BaseType::*;
        match self.base {
            TinyInt | SmallInt | MediumInt | Int | BigInt | Bool | Year => {
                let i: i128 = match &v {
                    Value::Int(i) => *i as i128,
                    Value::UInt(u) => *u as i128,
                    Value::Float(f) => f.round() as i128,
                    Value::Decimal(d) => exact_integer(d)?,
                    Value::Str(b) | Value::Bin(b) => {
                        let s = std::str::from_utf8(b).map_err(|_| bad("integer"))?.trim();
                        if let Ok(i) = s.parse::<i128>() {
                            i
                        } else {
                            exact_integer(s)?
                        }
                    }
                    Value::Null => unreachable!(),
                };
                let (lo, hi) = int_range(self);
                if i < lo || i > hi {
                    return Err(out_of_range());
                }
                Ok(if self.unsigned {
                    Value::UInt(i as u64)
                } else {
                    Value::Int(i as i64)
                })
            }
            Float | Double => match &v {
                Value::Str(b) | Value::Bin(b) => std::str::from_utf8(b)
                    .ok()
                    .and_then(|s| s.trim().parse::<f64>().ok())
                    .map(Value::Float)
                    .ok_or_else(|| bad("double")),
                other => other.as_f64().map(Value::Float).ok_or_else(|| bad("double")),
            },
            Decimal => {
                let scale = self.scale.unwrap_or(0);
                let s = match &v {
                    Value::Int(i) => i.to_string(),
                    Value::UInt(u) => u.to_string(),
                    Value::Float(f) => format!("{:.*}", scale as usize, f),
                    Value::Decimal(d) => d.clone(),
                    Value::Str(b) | Value::Bin(b) => {
                        let s = std::str::from_utf8(b).map_err(|_| bad("decimal"))?.trim().to_string();
                        if s.parse::<f64>().is_err() {
                            return Err(bad("decimal"));
                        }
                        s
                    }
                    Value::Null => unreachable!(),
                };
                let d = rescale_decimal(&s, scale).ok_or_else(|| bad("decimal"))?;
                // At most precision - scale digits before the point.
                let precision = self.len.unwrap_or(10);
                let int_digits = d
                    .trim_start_matches('-')
                    .split('.')
                    .next()
                    .unwrap_or("")
                    .trim_start_matches('0')
                    .len();
                if int_digits as u32 > precision.saturating_sub(scale) || (self.unsigned && d.starts_with('-')) {
                    return Err(SqlError::new(
                        1264,
                        "22003",
                        format!("Out of range value for column '{}' at row 1", col),
                    ));
                }
                Ok(Value::Decimal(d))
            }
            Binary | VarBinary | Blob => {
                let value = match v {
                    Value::Str(b) | Value::Bin(b) => Value::Bin(b),
                    other => Value::Bin(other.to_text().unwrap_or_default()),
                };
                if self.base == VarBinary
                    && self
                        .len
                        .is_some_and(|max| value.to_text().unwrap_or_default().len() > max as usize)
                {
                    return Err(SqlError::new(
                        1406,
                        "22001",
                        format!("Data too long for column '{}' at row 1", col),
                    ));
                }
                Ok(value)
            }
            Char | VarChar | Text | Json | Date | DateTime | Timestamp | Time => {
                let b = match v {
                    Value::Str(b) | Value::Bin(b) => b,
                    other => other.to_text().unwrap_or_default(),
                };
                if std::str::from_utf8(&b).is_err() {
                    return Err(bad("string"));
                }
                if let Some(max) = self.len {
                    if matches!(self.base, Char | VarChar) {
                        let n = std::str::from_utf8(&b).map(|s| s.chars().count()).unwrap_or(0);
                        if n > max as usize {
                            return Err(SqlError::new(
                                1406,
                                "22001",
                                format!("Data too long for column '{}' at row 1", col),
                            ));
                        }
                    }
                }
                let b = match self.base {
                    Date | DateTime | Timestamp => {
                        normalize_datetime(&b, self.base == Date, self.fsp() as usize).ok_or_else(|| bad("datetime"))?
                    }
                    _ => b,
                };
                Ok(Value::Str(b))
            }
        }
    }
}

pub fn int_range(t: &SqlType) -> (i128, i128) {
    use BaseType::*;
    let bits = match t.base {
        TinyInt | Bool => 8,
        SmallInt => 16,
        MediumInt => 24,
        Int => 32,
        Year => 16,
        _ => 64,
    };
    if t.unsigned {
        (0, (1i128 << bits) - 1)
    } else {
        (-(1i128 << (bits - 1)), (1i128 << (bits - 1)) - 1)
    }
}

/// Rewrite a decimal string to exactly `scale` fractional digits (rounding half
/// away from zero), e.g. ("1.005", 2) -> "1.01", ("7", 2) -> "7.00".
pub fn rescale_decimal(s: &str, scale: u32) -> Option<String> {
    crate::decimal::Decimal::parse(s)?.rescale(scale)
}

/// A date or datetime literal, split into its parts.
#[derive(Clone, Debug, PartialEq)]
struct Temporal {
    y: i64,
    mo: u32,
    d: u32,
    h: u32,
    mi: u32,
    s: u32,
    /// Fractional-second digits as written.
    frac: String,
}

/// Parse 'YYYY-MM-DD', 'YYYY-MM-DD[ T]HH:MM[:SS[.frac]]' (one- or two-digit
/// parts after the year) and 'YYYYMMDD[HHMMSS[.frac]]'. Zero dates are
/// accepted, as MySQL accepts them.
fn parse_temporal(s: &str) -> Option<Temporal> {
    let s = s.trim();
    let num = |p: &str| -> Option<u32> {
        (!p.is_empty() && p.len() <= 2 && p.bytes().all(|c| c.is_ascii_digit())).then(|| p.parse().ok())?
    };
    let (date, time) = match s.find([' ', 'T']) {
        Some(i) => (&s[..i], Some(s[i + 1..].trim())),
        None => (s, None),
    };
    let mut t = if date.contains('-') {
        let mut it = date.split('-');
        let (y, mo, d) = (it.next()?, it.next()?, it.next()?);
        if it.next().is_some() || y.len() != 4 || !y.bytes().all(|c| c.is_ascii_digit()) {
            return None;
        }
        Temporal {
            y: y.parse().ok()?,
            mo: num(mo)?,
            d: num(d)?,
            h: 0,
            mi: 0,
            s: 0,
            frac: String::new(),
        }
    } else {
        // YYYYMMDD[HHMMSS][.frac], as a string or a number
        if time.is_some() {
            return None;
        }
        let (digits, frac) = date.split_once('.').unwrap_or((date, ""));
        let ok_len = digits.len() == 14 || (digits.len() == 8 && frac.is_empty());
        if !ok_len || !digits.bytes().all(|c| c.is_ascii_digit()) || !frac.bytes().all(|c| c.is_ascii_digit()) {
            return None;
        }
        let p = |a: usize, b: usize| digits.get(a..b).map(|x| x.parse::<u32>().unwrap_or(0)).unwrap_or(0);
        Temporal {
            y: p(0, 4) as i64,
            mo: p(4, 6),
            d: p(6, 8),
            h: p(8, 10),
            mi: p(10, 12),
            s: p(12, 14),
            frac: frac.to_string(),
        }
    };
    if let Some(time) = time {
        let (hms, frac) = time.split_once('.').unwrap_or((time, ""));
        if !frac.bytes().all(|c| c.is_ascii_digit()) {
            return None;
        }
        let mut it = hms.split(':');
        t.h = num(it.next()?)?;
        t.mi = num(it.next()?)?;
        t.s = match it.next() {
            Some(x) => num(x)?,
            None if frac.is_empty() => 0,
            None => return None,
        };
        if it.next().is_some() {
            return None;
        }
        t.frac = frac.to_string();
    }
    if t.mo > 12 || t.d > 31 || t.h > 23 || t.mi > 59 || t.s > 59 {
        return None;
    }
    Some(t)
}

impl Temporal {
    fn date(&self) -> String {
        format!("{:04}-{:02}-{:02}", self.y, self.mo, self.d)
    }
    fn time_is_zero(&self) -> bool {
        self.h == 0 && self.mi == 0 && self.s == 0 && self.frac.bytes().all(|c| c == b'0')
    }
    /// 'YYYY-MM-DD HH:MM:SS', then `.frac` when `frac` is not empty.
    fn datetime(&self, frac: &str) -> String {
        let mut out = format!("{} {:02}:{:02}:{:02}", self.date(), self.h, self.mi, self.s);
        if !frac.is_empty() {
            out.push('.');
            out.push_str(frac);
        }
        out
    }
    /// Rounded to `fsp` fractional digits, as MySQL stores a DATETIME(fsp).
    fn rounded(mut self, fsp: usize) -> Temporal {
        if self.frac.len() <= fsp {
            return self;
        }
        let up = self.frac.as_bytes()[fsp] >= b'5';
        self.frac.truncate(fsp);
        if !up {
            return self;
        }
        // add one unit in the last kept digit, carrying into the seconds
        let mut digits = self.frac.clone().into_bytes();
        let mut carry = true;
        for c in digits.iter_mut().rev() {
            if *c == b'9' {
                *c = b'0';
            } else {
                *c += 1;
                carry = false;
                break;
            }
        }
        self.frac = String::from_utf8(digits).expect("ascii digits");
        if carry && self.mo != 0 && self.d != 0 {
            let secs =
                days_from_civil(self.y, self.mo, self.d) * 86_400 + (self.h * 3600 + self.mi * 60 + self.s) as i64 + 1;
            let s = crate::expr::format_datetime(secs);
            if let Some(t) = parse_temporal(&s) {
                self = Temporal { frac: self.frac, ..t };
            }
        }
        self
    }
}

/// Days since 1970-01-01 of a civil date (Howard Hinnant).
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let m = m as i64;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The text a DATE (`fsp` None) or DATETIME / TIMESTAMP(fsp) column stores
/// for a value: 'YYYY-MM-DD', or 'YYYY-MM-DD HH:MM:SS' with exactly `fsp`
/// fractional digits (rounded, as MySQL rounds). One spelling per value, so
/// stored values compare, sort, group and index as bytes in time order.
fn canonical_temporal(t: Temporal, fsp: Option<usize>) -> String {
    match fsp {
        None => t.date(),
        Some(fsp) => {
            let t = t.rounded(fsp);
            let mut frac = t.frac.clone();
            while frac.len() < fsp {
                frac.push('0');
            }
            t.datetime(&frac)
        }
    }
}

/// A value compared with a DATE (`fsp` None) or DATETIME(fsp) column,
/// written so that comparing it as bytes with the column's stored text
/// compares the temporal values (issue #37): padded to the column's
/// fractional digits, with any further digits kept only up to the last
/// non-zero one, and for a DATE a time part only when it is not midnight.
/// (A stored text that is a prefix of the key is then smaller, as its time
/// is.) None when the value is not a date or datetime.
pub fn temporal_key(b: &[u8], fsp: Option<u32>) -> Option<String> {
    let t = parse_temporal(std::str::from_utf8(b).ok()?)?;
    let keep = match fsp {
        None if t.time_is_zero() => return Some(t.date()),
        None => 0,
        Some(f) => f as usize,
    };
    let mut frac = t.frac.clone();
    while frac.len() < keep {
        frac.push('0');
    }
    while frac.len() > keep && frac.ends_with('0') {
        frac.pop();
    }
    Some(t.datetime(&frac))
}

/// Accept 'YYYY-MM-DD', 'YYYY-MM-DD HH:MM:SS[.frac]', 'YYYYMMDD...' and return
/// the canonical text form (which sorts chronologically as bytes): see
/// `canonical_temporal`.
fn normalize_datetime(b: &Bytes, date_only: bool, fsp: usize) -> Option<Bytes> {
    let t = parse_temporal(std::str::from_utf8(b).ok()?)?;
    Some(Bytes::from(canonical_temporal(t, (!date_only).then_some(fsp))))
}

// ---------------------------------------------------------------------------
// Value semantics
// ---------------------------------------------------------------------------

/// Numeric view of a value for MySQL's mixed-type comparison and arithmetic:
/// strings are converted by parsing their numeric prefix.
pub fn to_num(v: &Value) -> Option<f64> {
    match v {
        Value::Null => None,
        Value::Str(b) | Value::Bin(b) => {
            let s = std::str::from_utf8(b).unwrap_or("").trim();
            let end = s
                .char_indices()
                .take_while(|(i, c)| {
                    c.is_ascii_digit() || *c == '.' || ((*c == '-' || *c == '+') && *i == 0) || *c == 'e' || *c == 'E'
                })
                .last()
                .map(|(i, c)| i + c.len_utf8())
                .unwrap_or(0);
            let mut t = &s[..end];
            loop {
                if let Ok(f) = t.parse::<f64>() {
                    return Some(f);
                }
                if t.is_empty() {
                    return Some(0.0);
                }
                t = &t[..t.len() - 1];
            }
        }
        other => other.as_f64(),
    }
}

fn is_numeric_value(v: &Value) -> bool {
    matches!(v, Value::Int(_) | Value::UInt(_) | Value::Float(_) | Value::Decimal(_))
}

/// MySQL comparison: NULL if either side is NULL. Numbers compare
/// numerically; a number against a string compares numerically; strings
/// compare as bytes (utf8mb4_bin; see `compare_coll`).
pub fn compare(a: &Value, b: &Value) -> Option<Ordering> {
    if a.is_null() || b.is_null() {
        return None;
    }
    match (a, b) {
        (Value::Int(x), Value::Int(y)) => Some(x.cmp(y)),
        (Value::UInt(x), Value::UInt(y)) => Some(x.cmp(y)),
        (Value::Int(x), Value::UInt(y)) => Some((*x as i128).cmp(&(*y as i128))),
        (Value::UInt(x), Value::Int(y)) => Some((*x as i128).cmp(&(*y as i128))),
        (Value::Str(x), Value::Str(y))
        | (Value::Bin(x), Value::Bin(y))
        | (Value::Str(x), Value::Bin(y))
        | (Value::Bin(x), Value::Str(y)) => Some(x.as_ref().cmp(y.as_ref())),
        (Value::Decimal(x), Value::Decimal(y)) => Some(cmp_decimal_str(x, y)),
        (Value::Decimal(x), Value::Int(y)) => Some(cmp_decimal_str(x, &y.to_string())),
        (Value::Decimal(x), Value::UInt(y)) => Some(cmp_decimal_str(x, &y.to_string())),
        (Value::Int(x), Value::Decimal(y)) => Some(cmp_decimal_str(&x.to_string(), y)),
        (Value::UInt(x), Value::Decimal(y)) => Some(cmp_decimal_str(&x.to_string(), y)),
        _ if is_numeric_value(a) || is_numeric_value(b) => {
            let (x, y) = (to_num(a)?, to_num(b)?);
            x.partial_cmp(&y)
        }
        _ => None,
    }
}

/// `compare` under a collation: two text values compare by the
/// collation's weights; a binary string (`Value::Bin`) makes it a byte
/// comparison, as in MySQL.
pub fn compare_coll(a: &Value, b: &Value, coll: Collation) -> Option<Ordering> {
    match (a, b) {
        // (a binary string compared under a text collation was converted to
        // its character set: ci_col = X'41')
        (Value::Str(x) | Value::Bin(x), Value::Str(y) | Value::Bin(y)) if coll.is_ci() => Some(coll.cmp(x, y)),
        _ => compare(a, b),
    }
}

/// `sort_cmp` under a collation.
pub fn sort_cmp_coll(a: &Value, b: &Value, coll: Collation) -> Ordering {
    match (a.is_null(), b.is_null()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
        _ => compare_coll(a, b, coll).unwrap_or(Ordering::Equal),
    }
}

/// `key_val` under a collation: text values equal under it get one key.
pub fn key_val_coll(v: &Value, coll: Collation) -> KeyVal {
    match v {
        Value::Str(b) | Value::Bin(b) if coll.is_ci() => KeyVal::Bytes(coll.key(b)),
        other => key_val(other),
    }
}

fn cmp_decimal_str(a: &str, b: &str) -> Ordering {
    match (crate::decimal::Decimal::parse(a), crate::decimal::Decimal::parse(b)) {
        (Some(x), Some(y)) => x.compare(&y),
        _ => a.cmp(b),
    }
}

/// Total order used by ORDER BY / GROUP BY / DISTINCT: NULLs first.
pub fn sort_cmp(a: &Value, b: &Value) -> Ordering {
    match (a.is_null(), b.is_null()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
        _ => compare(a, b).unwrap_or(Ordering::Equal),
    }
}

/// Truthiness for WHERE/HAVING/IF: NULL -> None.
pub fn truth(v: &Value) -> Option<bool> {
    match v {
        Value::Null => None,
        Value::Int(i) => Some(*i != 0),
        Value::UInt(u) => Some(*u != 0),
        Value::Float(f) => Some(*f != 0.0),
        other => to_num(other).map(|f| f != 0.0),
    }
}

pub fn bool_val(b: Option<bool>) -> Value {
    match b {
        None => Value::Null,
        Some(true) => Value::Int(1),
        Some(false) => Value::Int(0),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Arith {
    Add,
    Sub,
    Mul,
    Div,
    IntDiv,
    Mod,
    /// `+`, `-`, `*` with an unsigned integer operand: MySQL computes these
    /// in BIGINT UNSIGNED and fails with 1690 when the result is negative.
    UAdd,
    USub,
    UMul,
}

impl Arith {
    /// The operation without the unsigned domain.
    pub fn base(self) -> Arith {
        match self {
            Arith::UAdd => Arith::Add,
            Arith::USub => Arith::Sub,
            Arith::UMul => Arith::Mul,
            o => o,
        }
    }
    pub fn is_unsigned(self) -> bool {
        matches!(self, Arith::UAdd | Arith::USub | Arith::UMul)
    }
    /// `self` in the unsigned domain (only `+`, `-` and `*` have one).
    pub fn unsigned(self) -> Arith {
        match self {
            Arith::Add => Arith::UAdd,
            Arith::Sub => Arith::USub,
            Arith::Mul => Arith::UMul,
            o => o,
        }
    }
}

/// MySQL arithmetic. Integer + - * stay integral (error on overflow), `/`
/// yields a decimal with 4 extra fractional digits, anything involving a
/// float is computed as a double.
pub fn arith(op: Arith, a: &Value, b: &Value) -> SqlResult<Value> {
    if a.is_null() || b.is_null() {
        return Ok(Value::Null);
    }
    if op.is_unsigned() {
        let int = |v: &Value| match v {
            Value::Int(i) => Some(*i as i128),
            Value::UInt(u) => Some(*u as i128),
            _ => None,
        };
        if let (Some(x), Some(y)) = (int(a), int(b)) {
            let r = match op.base() {
                Arith::Add => Some(x + y),
                Arith::Sub => Some(x - y),
                _ => x.checked_mul(y),
            };
            return match r {
                Some(r) if (0..=u64::MAX as i128).contains(&r) => Ok(Value::UInt(r as u64)),
                _ => Err(SqlError::new(1690, "22003", "BIGINT UNSIGNED value is out of range")),
            };
        }
        return arith(op.base(), a, b);
    }
    let ints = |v: &Value| -> Option<i128> {
        match v {
            Value::Int(i) => Some(*i as i128),
            Value::UInt(u) => Some(*u as i128),
            _ => None,
        }
    };
    let overflow = || SqlError::new(1690, "22003", "BIGINT value is out of range");
    if let (Some(x), Some(y)) = (ints(a), ints(b)) {
        let unsigned = matches!(a, Value::UInt(_)) || matches!(b, Value::UInt(_));
        let r: i128 = match op.base() {
            Arith::Add => x + y,
            Arith::Sub => x - y,
            Arith::Mul => x.checked_mul(y).ok_or_else(overflow)?,
            Arith::IntDiv => {
                if y == 0 {
                    return Ok(Value::Null);
                }
                x / y
            }
            Arith::Mod => {
                if y == 0 {
                    return Ok(Value::Null);
                }
                x % y
            }
            Arith::Div => {
                if y == 0 {
                    return Ok(Value::Null);
                }
                // scale 4 decimal, rounded half away from zero
                let scaled = x * 10_000 * 10;
                let q = scaled / y;
                let q = if q >= 0 { (q + 5) / 10 } else { (q - 5) / 10 };
                return Ok(Value::Decimal(fmt_scaled(q, 4)));
            }
            _ => unreachable!("base() is never unsigned"),
        };
        if unsigned && r >= 0 && r <= u64::MAX as i128 && r > i64::MAX as i128 {
            return Ok(Value::UInt(r as u64));
        }
        if r < i64::MIN as i128 || r > i64::MAX as i128 {
            return Err(overflow());
        }
        return Ok(Value::Int(r as i64));
    }
    let decimal_involved = matches!(a, Value::Decimal(_)) || matches!(b, Value::Decimal(_));
    let float_involved = matches!(a, Value::Float(_)) || matches!(b, Value::Float(_));
    let (x, y) = (to_num(a).unwrap_or(0.0), to_num(b).unwrap_or(0.0));
    let r = match op.base() {
        Arith::Add => x + y,
        Arith::Sub => x - y,
        Arith::Mul => x * y,
        Arith::Div => {
            if y == 0.0 {
                return Ok(Value::Null);
            }
            x / y
        }
        Arith::IntDiv => {
            if y == 0.0 {
                return Ok(Value::Null);
            }
            return Ok(Value::Int((x / y).trunc() as i64));
        }
        Arith::Mod => {
            if y == 0.0 {
                return Ok(Value::Null);
            }
            x % y
        }
        _ => unreachable!("base() is never unsigned"),
    };
    if decimal_involved && !float_involved {
        let sa = dec_scale(a);
        let sb = dec_scale(b);
        let scale = match op.base() {
            Arith::Add | Arith::Sub | Arith::Mod => sa.max(sb),
            Arith::Mul => sa + sb,
            Arith::Div => sa + 4,
            _ => 0,
        };
        return Ok(Value::Decimal(format!("{:.*}", scale as usize, r)));
    }
    Ok(Value::Float(r))
}

fn dec_scale(v: &Value) -> u32 {
    match v {
        Value::Decimal(d) => d.split_once('.').map(|(_, f)| f.len() as u32).unwrap_or(0),
        _ => 0,
    }
}

pub fn fmt_scaled(q: i128, scale: u32) -> String {
    let neg = q < 0;
    let a = q.unsigned_abs();
    let p = 10u128.pow(scale);
    let s = format!("{}.{:0width$}", a / p, a % p, width = scale as usize);
    if neg {
        format!("-{}", s)
    } else {
        s
    }
}

/// Hashable/ordered key form of a value for GROUP BY / DISTINCT / joins.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum KeyVal {
    Decimal(String),
    Null,
    Int(i128),
    Float(u64),
    Bytes(Bytes),
}

pub fn key_val(v: &Value) -> KeyVal {
    match v {
        Value::Null => KeyVal::Null,
        Value::Int(i) => KeyVal::Int(*i as i128),
        Value::UInt(u) => KeyVal::Int(*u as i128),
        Value::Float(f) => {
            if f.fract() == 0.0 && f.abs() < 1e18 {
                KeyVal::Int(*f as i128)
            } else {
                KeyVal::Float(f.to_bits())
            }
        }
        Value::Decimal(d) => match crate::decimal::Decimal::parse(d) {
            Some(decimal) => match decimal.integer() {
                Some(i) => KeyVal::Int(i),
                None => KeyVal::Decimal(decimal.canonical()),
            },
            None => KeyVal::Bytes(Bytes::copy_from_slice(d.as_bytes())),
        },
        Value::Str(b) | Value::Bin(b) => KeyVal::Bytes(b.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_types() {
        let t = SqlType::parse("INT(11) UNSIGNED").unwrap();
        assert_eq!(t.base, BaseType::Int);
        assert!(t.unsigned);
        let t = SqlType::parse("DECIMAL(10,2)").unwrap();
        assert_eq!((t.len, t.scale), (Some(10), Some(2)));
        assert_eq!(SqlType::parse("CHAR(120)").unwrap().len, Some(120));
        assert_eq!(SqlType::parse("tinyint(1)").unwrap().base, BaseType::Bool);
    }

    #[test]
    fn decimals() {
        assert_eq!(rescale_decimal("1.005", 2).unwrap(), "1.01");
        assert_eq!(rescale_decimal("7", 2).unwrap(), "7.00");
        assert_eq!(rescale_decimal("-0.004", 2).unwrap(), "0.00");
        assert_eq!(rescale_decimal("9.999", 2).unwrap(), "10.00");
        assert_eq!(rescale_decimal("-12.5", 0).unwrap(), "-13");
    }

    #[test]
    fn arithmetic() {
        assert_eq!(
            arith(Arith::Div, &Value::Int(1), &Value::Int(3)).unwrap(),
            Value::Decimal("0.3333".into())
        );
        assert_eq!(
            arith(Arith::Div, &Value::Int(-2), &Value::Int(3)).unwrap(),
            Value::Decimal("-0.6667".into())
        );
        assert_eq!(
            arith(Arith::Add, &Value::Int(2), &Value::Int(3)).unwrap(),
            Value::Int(5)
        );
        assert!(arith(Arith::Mul, &Value::Int(i64::MAX), &Value::Int(2)).is_err());
        // issue #11: signed results may be negative; unsigned ones may not
        assert_eq!(
            arith(Arith::Sub, &Value::UInt(0), &Value::Int(1)).unwrap(),
            Value::Int(-1)
        );
        assert_eq!(
            arith(Arith::USub, &Value::UInt(5), &Value::Int(1)).unwrap(),
            Value::UInt(4)
        );
        assert_eq!(
            arith(Arith::USub, &Value::UInt(0), &Value::Int(1)).unwrap_err().code,
            1690
        );
        assert_eq!(
            arith(Arith::UAdd, &Value::UInt(u64::MAX), &Value::Int(1))
                .unwrap_err()
                .code,
            1690
        );
        assert_eq!(
            arith(Arith::UAdd, &Value::UInt(i64::MAX as u64), &Value::Int(1)).unwrap(),
            Value::UInt(i64::MAX as u64 + 1)
        );
        assert_eq!(arith(Arith::UMul, &Value::UInt(3), &Value::Null).unwrap(), Value::Null);
        assert_eq!(arith(Arith::Div, &Value::Int(1), &Value::Int(0)).unwrap(), Value::Null);
    }

    #[test]
    fn comparisons() {
        assert_eq!(compare(&Value::Int(10), &Value::str("9")), Some(Ordering::Greater));
        assert_eq!(compare(&Value::str("10"), &Value::str("9")), Some(Ordering::Less));
        assert_eq!(compare(&Value::Null, &Value::Int(1)), None);
        assert_eq!(
            compare(&Value::UInt(u64::MAX), &Value::Int(-1)),
            Some(Ordering::Greater)
        );
    }

    #[test]
    fn exact_decimal_keys_above_double_precision() {
        let d = |s: &str| Value::Decimal(s.into());
        let a = d("9007199254740992.01");
        let b = d("9007199254740992.02");
        assert_eq!(compare(&a, &b), Some(Ordering::Less));
        assert_ne!(key_val(&a), key_val(&b));
        assert_eq!(key_val(&a), key_val(&d("09007199254740992.0100")));
        assert_eq!(
            compare(&d("18446744073709551614.9"), &Value::UInt(u64::MAX)),
            Some(Ordering::Less)
        );
        assert_eq!(
            compare(&Value::Int(i64::MIN), &d("-9223372036854775807.9")),
            Some(Ordering::Less)
        );
        assert_eq!(key_val(&d("18446744073709551615.00")), key_val(&Value::UInt(u64::MAX)));
        let int = SqlType::parse("BIGINT").unwrap();
        assert_eq!(
            int.coerce(d("9007199254740993.0"), "id").unwrap(),
            Value::Int(9007199254740993)
        );
        assert_eq!(
            int.coerce(Value::str("9007199254740993.0"), "id").unwrap(),
            Value::Int(9007199254740993)
        );
        assert_eq!(rescale_decimal("9007199254740993e0", 2).unwrap(), "9007199254740993.00");
        for (a, b, ordering) in [
            ("-0.000", "0e99", Ordering::Equal),
            ("0", "0.000001", Ordering::Less),
            ("-0.000001", "0", Ordering::Less),
            ("-99", "-100", Ordering::Greater),
            ("1.000", "100e-2", Ordering::Equal),
            ("1e1000", "9e999", Ordering::Greater),
        ] {
            assert_eq!(compare(&d(a), &d(b)), Some(ordering), "{a} vs {b}");
            if ordering == Ordering::Equal {
                assert_eq!(key_val(&d(a)), key_val(&d(b)));
            }
        }
        for (value, scale, expected) in [
            (".0005", 3, "0.001"),
            ("-5e-3", 2, "-0.01"),
            ("1e-999", 2, "0.00"),
            ("9.995", 2, "10.00"),
        ] {
            assert_eq!(rescale_decimal(value, scale).unwrap(), expected);
        }
        for invalid in ["", ".", "NaN", "Inf", "1e", "--1", "1.2.3"] {
            assert!(rescale_decimal(invalid, 2).is_none(), "{invalid}");
        }
        assert!(rescale_decimal("1e99999", 2).is_none());
        let decimal = SqlType::parse("DECIMAL(38,2)").unwrap();
        let key = crate::exec::coerce_key_part(&decimal, &d("9007199254740992.0100")).unwrap();
        assert_eq!(key, d("9007199254740992.01"));
        assert!(crate::exec::coerce_key_part(&decimal, &d("9007199254740992.011")).is_none());
        assert!(crate::exec::coerce_key_part(&decimal, &d("100000000000000000000000000000000000000000")).is_none());
        assert_eq!(
            crate::exec::coerce_key_part(&decimal, &Value::str("7tail")),
            Some(d("7.00"))
        );
        assert_eq!(
            crate::route::bucket_of(&[key], 30000),
            crate::route::bucket_of(&[d("9007199254740992.01")], 30000)
        );
    }

    #[test]
    fn coercion() {
        let t = SqlType::parse("INT").unwrap();
        assert_eq!(t.coerce(Value::str("42"), "c").unwrap(), Value::Int(42));
        assert!(t.coerce(Value::Int(1 << 40), "c").is_err());
        let t = SqlType::parse("DOUBLE").unwrap();
        assert_eq!(t.coerce(Value::Int(3), "c").unwrap(), Value::Float(3.0));
        let t = SqlType::parse("VARCHAR(3)").unwrap();
        assert!(t.coerce(Value::str("abcd"), "c").is_err());
        let t = SqlType::parse("DATETIME").unwrap();
        assert_eq!(
            t.coerce(Value::str("2024-01-02"), "c").unwrap(),
            Value::str("2024-01-02 00:00:00")
        );
    }

    #[test]
    fn temporal_spellings() {
        // issue #37: one stored spelling per value, rounded to the precision
        let c = |ty: &str, v: &str| match SqlType::parse(ty).unwrap().coerce(Value::str(v), "c") {
            Ok(Value::Str(b)) => String::from_utf8(b.to_vec()).unwrap(),
            other => panic!("{other:?}"),
        };
        assert_eq!(SqlType::parse("DATETIME(6)").unwrap().display(), "datetime(6)");
        assert_eq!(SqlType::parse("TIMESTAMP(3)").unwrap().display(), "timestamp(3)");
        assert_eq!(c("DATETIME", "2025-01-01 00:00:01.000000"), "2025-01-01 00:00:01");
        assert_eq!(c("DATETIME", "2025-01-01 00:00:01.5"), "2025-01-01 00:00:02");
        assert_eq!(c("DATETIME", "2025-12-31 23:59:59.7"), "2026-01-01 00:00:00");
        assert_eq!(c("DATETIME(6)", "2025-01-01 00:00:01"), "2025-01-01 00:00:01.000000");
        assert_eq!(c("DATETIME(3)", "2025-01-01T00:00:01.12349"), "2025-01-01 00:00:01.123");
        assert_eq!(c("DATETIME(2)", "2025-02-28 23:59:59.999"), "2025-03-01 00:00:00.00");
        assert_eq!(c("DATETIME", "2025-1-2 3:04:05"), "2025-01-02 03:04:05");
        assert_eq!(c("DATETIME", "20250102030405"), "2025-01-02 03:04:05");
        assert_eq!(c("DATETIME(6)", "0000-00-00 00:00:00"), "0000-00-00 00:00:00.000000");
        assert_eq!(c("DATE", "2025-01-02 10:00:00"), "2025-01-02");
        assert!(SqlType::parse("DATETIME")
            .unwrap()
            .coerce(Value::str("2025-13-01"), "c")
            .is_err());
        // comparison keys: equal values spell alike, longer ones only when
        // they differ
        let k = |v: &str, fsp| temporal_key(v.as_bytes(), fsp);
        assert_eq!(k("2025-01-01 00:00:01", Some(6)).unwrap(), "2025-01-01 00:00:01.000000");
        assert_eq!(k("2025-01-01 00:00:01.000000", Some(0)).unwrap(), "2025-01-01 00:00:01");
        assert_eq!(k("2025-01-01 00:00:01.50", Some(0)).unwrap(), "2025-01-01 00:00:01.5");
        assert_eq!(k("2025-01-01 00:00:00", None).unwrap(), "2025-01-01");
        assert_eq!(k("2025-01-01 00:00:01", None).unwrap(), "2025-01-01 00:00:01");
        assert_eq!(k("not a date", Some(0)), None);
        // ... and compare as bytes in time order against stored values
        assert!("2025-01-01 00:00:01" < k("2025-01-01 00:00:01.5", Some(0)).unwrap().as_str());
        assert!("2025-01-01" < k("2025-01-01 00:00:01", None).unwrap().as_str());
        assert!("2025-01-02" > k("2025-01-01 23:59:59", None).unwrap().as_str());
    }
}
