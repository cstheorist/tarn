//! SQL column types, their Tarantool and MySQL-protocol mappings, and the
//! value semantics (coercion, comparison, arithmetic) the executor uses.

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
            DateTime => "datetime".to_string(),
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
        use BaseType::*;
        match self.base {
            TinyInt | SmallInt | MediumInt | Int | BigInt | Bool | Year => {
                let i: i128 = match &v {
                    Value::Int(i) => *i as i128,
                    Value::UInt(u) => *u as i128,
                    Value::Float(f) => f.round() as i128,
                    Value::Decimal(d) => parse_decimal_round(d).ok_or_else(|| bad("integer"))?,
                    Value::Str(b) | Value::Bin(b) => {
                        let s = std::str::from_utf8(b).map_err(|_| bad("integer"))?.trim();
                        if let Ok(i) = s.parse::<i128>() {
                            i
                        } else if let Ok(f) = s.parse::<f64>() {
                            f.round() as i128
                        } else {
                            return Err(bad("integer"));
                        }
                    }
                    Value::Null => unreachable!(),
                };
                let (lo, hi) = int_range(self);
                if i < lo || i > hi {
                    return Err(SqlError::new(
                        1264,
                        "22003",
                        format!("Out of range value for column '{}' at row 1", col),
                    ));
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
                Ok(Value::Decimal(
                    rescale_decimal(&s, scale).ok_or_else(|| bad("decimal"))?,
                ))
            }
            Binary | VarBinary | Blob => Ok(match v {
                Value::Str(b) | Value::Bin(b) => Value::Bin(b),
                other => Value::Bin(other.to_text().unwrap_or_default()),
            }),
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
                        normalize_datetime(&b, self.base == Date).ok_or_else(|| bad("datetime"))?
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

fn parse_decimal_round(d: &str) -> Option<i128> {
    d.trim().parse::<f64>().ok().map(|f| f.round() as i128)
}

/// Rewrite a decimal string to exactly `scale` fractional digits (rounding half
/// away from zero), e.g. ("1.005", 2) -> "1.01", ("7", 2) -> "7.00".
pub fn rescale_decimal(s: &str, scale: u32) -> Option<String> {
    let s = s.trim();
    let (neg, digits) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let (ip, fp) = match digits.split_once('.') {
        Some((a, b)) => (a, b),
        None => (digits, ""),
    };
    if digits.contains('e') || digits.contains('E') {
        let f: f64 = s.parse().ok()?;
        return Some(format!("{:.*}", scale as usize, f));
    }
    if !ip.chars().all(|c| c.is_ascii_digit()) || !fp.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let ip = if ip.is_empty() { "0" } else { ip };
    let scale = scale as usize;
    let mut frac: Vec<u8> = fp.bytes().collect();
    let round_up = frac.len() > scale && frac[scale] >= b'5';
    frac.resize(scale, b'0');
    let mut all: Vec<u8> = ip.bytes().chain(frac).collect();
    if round_up {
        let mut i = all.len();
        loop {
            if i == 0 {
                all.insert(0, b'1');
                break;
            }
            i -= 1;
            if all[i] == b'9' {
                all[i] = b'0';
            } else {
                all[i] += 1;
                break;
            }
        }
    }
    let int_len = all.len() - scale;
    let mut ints = String::from_utf8(all[..int_len].to_vec()).ok()?;
    let trimmed = ints.trim_start_matches('0');
    ints = if trimmed.is_empty() {
        "0".to_string()
    } else {
        trimmed.to_string()
    };
    let fracs = String::from_utf8(all[int_len..].to_vec()).ok()?;
    let zero = ints == "0" && fracs.chars().all(|c| c == '0');
    let mut out = String::new();
    if neg && !zero {
        out.push('-');
    }
    out.push_str(&ints);
    if scale > 0 {
        out.push('.');
        out.push_str(&fracs);
    }
    Some(out)
}

/// Accept 'YYYY-MM-DD', 'YYYY-MM-DD HH:MM:SS[.frac]', 'YYYYMMDD...' and return
/// the canonical text form (which sorts chronologically as bytes).
fn normalize_datetime(b: &Bytes, date_only: bool) -> Option<Bytes> {
    let s = std::str::from_utf8(b).ok()?.trim();
    let digits: String = s.chars().filter(|c| c.is_ascii_digit()).collect();
    if s.len() >= 10 && s.as_bytes()[4] == b'-' && s.as_bytes()[7] == b'-' {
        if date_only {
            return Some(Bytes::copy_from_slice(&s.as_bytes()[..10]));
        }
        if s.len() == 10 {
            return Some(Bytes::from(format!("{} 00:00:00", s)));
        }
        return Some(Bytes::copy_from_slice(s.replacen('T', " ", 1).as_bytes()));
    }
    if digits.len() == 8 || digits.len() == 14 {
        let d = format!("{}-{}-{}", &digits[0..4], &digits[4..6], &digits[6..8]);
        if date_only {
            return Some(Bytes::from(d));
        }
        let t = if digits.len() == 14 {
            format!("{}:{}:{}", &digits[8..10], &digits[10..12], &digits[12..14])
        } else {
            "00:00:00".to_string()
        };
        return Some(Bytes::from(format!("{} {}", d, t)));
    }
    None
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
/// compare as bytes (Tarn uses binary collation).
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
        _ if is_numeric_value(a) || is_numeric_value(b) => {
            let (x, y) = (to_num(a)?, to_num(b)?);
            x.partial_cmp(&y)
        }
        _ => None,
    }
}

fn cmp_decimal_str(a: &str, b: &str) -> Ordering {
    match (a.parse::<f64>(), b.parse::<f64>()) {
        (Ok(x), Ok(y)) => x.partial_cmp(&y).unwrap_or(Ordering::Equal),
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
        Value::Decimal(d) => match d.parse::<f64>() {
            Ok(f) if f.fract() == 0.0 && f.abs() < 1e18 => KeyVal::Int(f as i128),
            Ok(f) => KeyVal::Float(f.to_bits()),
            Err(_) => KeyVal::Bytes(Bytes::copy_from_slice(d.as_bytes())),
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
}
