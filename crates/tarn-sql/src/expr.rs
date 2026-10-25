//! Compiled scalar expressions and their evaluation with MySQL semantics.

use crate::types::{arith, bool_val, compare, to_num, truth, Arith, BaseType, SqlType};
use bytes::Bytes;
use std::cmp::Ordering;
use tarn_common::{ColumnType, SqlError, SqlResult, Value};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    NullSafeEq,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Func {
    Concat,
    ConcatWs,
    Length,
    CharLength,
    Lower,
    Upper,
    Substring,
    Trim,
    LTrim,
    RTrim,
    Replace,
    Abs,
    Ceil,
    Floor,
    Round,
    Mod,
    Coalesce,
    IfNull,
    NullIf,
    If,
    Greatest,
    Least,
    Now,
    CurDate,
    UnixTimestamp,
    Database,
    Version,
    ConnectionId,
    LastInsertId,
    User,
    Rand,
    Hex,
    Left,
    Right,
    Lpad,
    Rpad,
    Repeat,
    Sign,
    Sqrt,
    Pow,
    Locate,
    Ascii,
    FoundRows,
    RowCount,
}

impl Func {
    pub fn lookup(name: &str) -> Option<Func> {
        use Func::*;
        Some(match name.to_ascii_uppercase().as_str() {
            "CONCAT" => Concat,
            "CONCAT_WS" => ConcatWs,
            "LENGTH" | "OCTET_LENGTH" => Length,
            "CHAR_LENGTH" | "CHARACTER_LENGTH" => CharLength,
            "LOWER" | "LCASE" => Lower,
            "UPPER" | "UCASE" => Upper,
            "SUBSTRING" | "SUBSTR" | "MID" => Substring,
            "TRIM" => Trim,
            "LTRIM" => LTrim,
            "RTRIM" => RTrim,
            "REPLACE" => Replace,
            "ABS" => Abs,
            "CEIL" | "CEILING" => Ceil,
            "FLOOR" => Floor,
            "ROUND" => Round,
            "MOD" => Mod,
            "COALESCE" => Coalesce,
            "IFNULL" => IfNull,
            "NULLIF" => NullIf,
            "IF" => If,
            "GREATEST" => Greatest,
            "LEAST" => Least,
            "NOW" | "CURRENT_TIMESTAMP" | "SYSDATE" | "LOCALTIME" | "LOCALTIMESTAMP" | "UTC_TIMESTAMP" => Now,
            "CURDATE" | "CURRENT_DATE" | "UTC_DATE" => CurDate,
            "UNIX_TIMESTAMP" => UnixTimestamp,
            "DATABASE" | "SCHEMA" => Database,
            "VERSION" => Version,
            "CONNECTION_ID" => ConnectionId,
            "LAST_INSERT_ID" => LastInsertId,
            "USER" | "CURRENT_USER" | "SESSION_USER" | "SYSTEM_USER" => User,
            "RAND" => Rand,
            "HEX" => Hex,
            "LEFT" => Left,
            "RIGHT" => Right,
            "LPAD" => Lpad,
            "RPAD" => Rpad,
            "REPEAT" => Repeat,
            "SIGN" => Sign,
            "SQRT" => Sqrt,
            "POW" | "POWER" => Pow,
            "LOCATE" | "INSTR" | "POSITION" => Locate,
            "ASCII" => Ascii,
            "FOUND_ROWS" => FoundRows,
            "ROW_COUNT" => RowCount,
            _ => return None,
        })
    }
}

#[derive(Clone, Debug)]
pub enum Expr {
    /// Column of the input row.
    Col(usize),
    Lit(Value),
    /// Statement parameter (0-based).
    Param(usize),
    Cmp(Box<Expr>, CmpOp, Box<Expr>),
    Arith(Box<Expr>, Arith, Box<Expr>),
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Xor(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
    Neg(Box<Expr>),
    IsNull(Box<Expr>, bool),
    IsTrue(Box<Expr>, bool, bool),
    InList(Box<Expr>, Vec<Expr>, bool),
    Between(Box<Expr>, Box<Expr>, Box<Expr>, bool),
    Like(Box<Expr>, Box<Expr>, bool),
    Func(Func, Vec<Expr>),
    Case(Option<Box<Expr>>, Vec<(Expr, Expr)>, Option<Box<Expr>>),
    Cast(Box<Expr>, CastTo),
    /// Session/system variable rendered at plan time.
    Var(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CastTo {
    Signed,
    Unsigned,
    Char,
    Decimal(u32),
    Double,
    Date,
    DateTime,
    Binary,
}

/// Per-statement evaluation context.
pub struct EvalCtx<'a> {
    pub params: &'a [Value],
    pub db: Option<&'a str>,
    pub user: &'a str,
    pub conn_id: u32,
    pub last_insert_id: u64,
    pub found_rows: u64,
    pub row_count: i64,
    pub now: &'a str,
}

pub fn now_string() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format_datetime(secs as i64)
}

/// Civil-from-days (Howard Hinnant), UTC.
pub fn format_datetime(secs: i64) -> String {
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        y,
        m,
        d,
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60
    )
}

fn text(v: &Value) -> Option<Bytes> {
    v.to_text()
}

fn text_str(v: &Value) -> String {
    v.to_text()
        .map(|b| String::from_utf8_lossy(&b).into_owned())
        .unwrap_or_default()
}

fn int_arg(v: &Value) -> Option<i64> {
    match v {
        Value::Null => None,
        Value::Int(i) => Some(*i),
        Value::UInt(u) => Some(*u as i64),
        other => to_num(other).map(|f| f.round() as i64),
    }
}

/// MySQL LIKE with `%`, `_` and `\` escapes; binary (case-sensitive) collation.
pub fn like_match(s: &[u8], p: &[u8]) -> bool {
    fn rec(s: &[u8], p: &[u8]) -> bool {
        let (mut si, mut pi) = (0usize, 0usize);
        let (mut star_p, mut star_s) = (usize::MAX, 0usize);
        while si < s.len() {
            if pi < p.len() && p[pi] == b'\\' && pi + 1 < p.len() {
                if s[si] == p[pi + 1] {
                    si += 1;
                    pi += 2;
                    continue;
                }
            } else if pi < p.len() && p[pi] == b'%' {
                star_p = pi;
                star_s = si;
                pi += 1;
                continue;
            } else if pi < p.len() && (p[pi] == b'_' || p[pi] == s[si]) {
                if p[pi] == b'_' {
                    // advance one UTF-8 character
                    let len = match s[si] {
                        0x00..=0x7F => 1,
                        0xC0..=0xDF => 2,
                        0xE0..=0xEF => 3,
                        _ => 4,
                    };
                    si = (si + len).min(s.len());
                } else {
                    si += 1;
                }
                pi += 1;
                continue;
            }
            if star_p != usize::MAX {
                pi = star_p + 1;
                star_s += 1;
                si = star_s;
                continue;
            }
            return false;
        }
        while pi < p.len() && p[pi] == b'%' {
            pi += 1;
        }
        pi == p.len()
    }
    rec(s, p)
}

impl Expr {
    pub fn lit(v: Value) -> Expr {
        Expr::Lit(v)
    }

    /// Does the expression depend only on parameters/literals (no columns)?
    pub fn is_constant(&self) -> bool {
        match self {
            Expr::Col(_) => false,
            Expr::Lit(_) | Expr::Param(_) | Expr::Var(_) => true,
            Expr::Func(f, args) => !matches!(f, Func::Rand) && args.iter().all(|a| a.is_constant()),
            Expr::Cmp(a, _, b)
            | Expr::Arith(a, _, b)
            | Expr::And(a, b)
            | Expr::Or(a, b)
            | Expr::Xor(a, b)
            | Expr::Like(a, b, _) => a.is_constant() && b.is_constant(),
            Expr::Not(a) | Expr::Neg(a) | Expr::IsNull(a, _) | Expr::IsTrue(a, _, _) | Expr::Cast(a, _) => {
                a.is_constant()
            }
            Expr::InList(a, l, _) => a.is_constant() && l.iter().all(|x| x.is_constant()),
            Expr::Between(a, b, c, _) => a.is_constant() && b.is_constant() && c.is_constant(),
            Expr::Case(o, w, e) => {
                o.as_ref().is_none_or(|x| x.is_constant())
                    && w.iter().all(|(a, b)| a.is_constant() && b.is_constant())
                    && e.as_ref().is_none_or(|x| x.is_constant())
            }
        }
    }

    /// Rewrite column references through `map` (used when re-basing an
    /// expression onto a different row layout).
    pub fn remap(&self, map: &dyn Fn(usize) -> Expr) -> Expr {
        let r = |e: &Expr| Box::new(e.remap(map));
        match self {
            Expr::Col(i) => map(*i),
            Expr::Lit(_) | Expr::Param(_) | Expr::Var(_) => self.clone(),
            Expr::Cmp(a, o, b) => Expr::Cmp(r(a), *o, r(b)),
            Expr::Arith(a, o, b) => Expr::Arith(r(a), *o, r(b)),
            Expr::And(a, b) => Expr::And(r(a), r(b)),
            Expr::Or(a, b) => Expr::Or(r(a), r(b)),
            Expr::Xor(a, b) => Expr::Xor(r(a), r(b)),
            Expr::Not(a) => Expr::Not(r(a)),
            Expr::Neg(a) => Expr::Neg(r(a)),
            Expr::IsNull(a, n) => Expr::IsNull(r(a), *n),
            Expr::IsTrue(a, t, n) => Expr::IsTrue(r(a), *t, *n),
            Expr::InList(a, l, n) => Expr::InList(r(a), l.iter().map(|x| x.remap(map)).collect(), *n),
            Expr::Between(a, b, c, n) => Expr::Between(r(a), r(b), r(c), *n),
            Expr::Like(a, b, n) => Expr::Like(r(a), r(b), *n),
            Expr::Func(f, args) => Expr::Func(*f, args.iter().map(|x| x.remap(map)).collect()),
            Expr::Case(o, w, e) => Expr::Case(
                o.as_ref().map(|x| r(x)),
                w.iter().map(|(a, b)| (a.remap(map), b.remap(map))).collect(),
                e.as_ref().map(|x| r(x)),
            ),
            Expr::Cast(a, t) => Expr::Cast(r(a), *t),
        }
    }

    pub fn columns(&self, out: &mut Vec<usize>) {
        match self {
            Expr::Col(i) => out.push(*i),
            Expr::Lit(_) | Expr::Param(_) | Expr::Var(_) => {}
            Expr::Cmp(a, _, b)
            | Expr::Arith(a, _, b)
            | Expr::And(a, b)
            | Expr::Or(a, b)
            | Expr::Xor(a, b)
            | Expr::Like(a, b, _) => {
                a.columns(out);
                b.columns(out);
            }
            Expr::Not(a) | Expr::Neg(a) | Expr::IsNull(a, _) | Expr::IsTrue(a, _, _) | Expr::Cast(a, _) => {
                a.columns(out)
            }
            Expr::InList(a, l, _) => {
                a.columns(out);
                l.iter().for_each(|x| x.columns(out));
            }
            Expr::Between(a, b, c, _) => {
                a.columns(out);
                b.columns(out);
                c.columns(out);
            }
            Expr::Func(_, args) => args.iter().for_each(|x| x.columns(out)),
            Expr::Case(o, w, e) => {
                if let Some(o) = o {
                    o.columns(out);
                }
                for (a, b) in w {
                    a.columns(out);
                    b.columns(out);
                }
                if let Some(e) = e {
                    e.columns(out);
                }
            }
        }
    }

    pub fn eval(&self, row: &[Value], cx: &EvalCtx) -> SqlResult<Value> {
        Ok(match self {
            Expr::Col(i) => row.get(*i).cloned().unwrap_or(Value::Null),
            Expr::Lit(v) => v.clone(),
            Expr::Param(i) => cx
                .params
                .get(*i)
                .cloned()
                .ok_or_else(|| SqlError::new(1210, "HY000", "Incorrect arguments to mysqld_stmt_execute"))?,
            Expr::Var(name) => Value::str(name),
            Expr::Cmp(a, op, b) => {
                let (x, y) = (a.eval(row, cx)?, b.eval(row, cx)?);
                if *op == CmpOp::NullSafeEq {
                    return Ok(Value::Int(match (x.is_null(), y.is_null()) {
                        (true, true) => 1,
                        (true, false) | (false, true) => 0,
                        _ => (compare(&x, &y) == Some(Ordering::Equal)) as i64,
                    }));
                }
                bool_val(compare(&x, &y).map(|o| match op {
                    CmpOp::Eq => o == Ordering::Equal,
                    CmpOp::NotEq => o != Ordering::Equal,
                    CmpOp::Lt => o == Ordering::Less,
                    CmpOp::LtEq => o != Ordering::Greater,
                    CmpOp::Gt => o == Ordering::Greater,
                    CmpOp::GtEq => o != Ordering::Less,
                    CmpOp::NullSafeEq => unreachable!(),
                }))
            }
            Expr::Arith(a, op, b) => arith(*op, &a.eval(row, cx)?, &b.eval(row, cx)?)?,
            Expr::And(a, b) => {
                let x = truth(&a.eval(row, cx)?);
                if x == Some(false) {
                    return Ok(Value::Int(0));
                }
                let y = truth(&b.eval(row, cx)?);
                bool_val(match (x, y) {
                    (_, Some(false)) => Some(false),
                    (Some(true), Some(true)) => Some(true),
                    _ => None,
                })
            }
            Expr::Or(a, b) => {
                let x = truth(&a.eval(row, cx)?);
                if x == Some(true) {
                    return Ok(Value::Int(1));
                }
                let y = truth(&b.eval(row, cx)?);
                bool_val(match (x, y) {
                    (_, Some(true)) => Some(true),
                    (Some(false), Some(false)) => Some(false),
                    _ => None,
                })
            }
            Expr::Xor(a, b) => {
                let (x, y) = (truth(&a.eval(row, cx)?), truth(&b.eval(row, cx)?));
                bool_val(match (x, y) {
                    (Some(p), Some(q)) => Some(p != q),
                    _ => None,
                })
            }
            Expr::Not(a) => bool_val(truth(&a.eval(row, cx)?).map(|b| !b)),
            Expr::Neg(a) => match a.eval(row, cx)? {
                Value::Null => Value::Null,
                Value::Int(i) => Value::Int(
                    i.checked_neg()
                        .ok_or_else(|| SqlError::new(1690, "22003", "BIGINT value is out of range"))?,
                ),
                Value::UInt(u) => {
                    if u <= i64::MAX as u64 + 1 {
                        Value::Int((u as i128).wrapping_neg() as i64)
                    } else {
                        return Err(SqlError::new(1690, "22003", "BIGINT value is out of range"));
                    }
                }
                Value::Decimal(d) => Value::Decimal(match d.strip_prefix('-') {
                    Some(p) => p.to_string(),
                    None => format!("-{}", d),
                }),
                other => Value::Float(-to_num(&other).unwrap_or(0.0)),
            },
            Expr::IsNull(a, negated) => Value::Int((a.eval(row, cx)?.is_null() != *negated) as i64),
            Expr::IsTrue(a, want, negated) => {
                let t = truth(&a.eval(row, cx)?);
                Value::Int(((t == Some(*want)) != *negated) as i64)
            }
            Expr::InList(a, list, negated) => {
                let x = a.eval(row, cx)?;
                if x.is_null() {
                    return Ok(Value::Null);
                }
                let mut saw_null = false;
                for e in list {
                    let y = e.eval(row, cx)?;
                    match compare(&x, &y) {
                        Some(Ordering::Equal) => return Ok(Value::Int((!negated) as i64)),
                        None => saw_null = true,
                        _ => {}
                    }
                }
                if saw_null {
                    Value::Null
                } else {
                    Value::Int(*negated as i64)
                }
            }
            Expr::Between(a, lo, hi, negated) => {
                let x = a.eval(row, cx)?;
                let (l, h) = (lo.eval(row, cx)?, hi.eval(row, cx)?);
                let ge = compare(&x, &l).map(|o| o != Ordering::Less);
                let le = compare(&x, &h).map(|o| o != Ordering::Greater);
                bool_val(match (ge, le) {
                    (Some(p), Some(q)) => Some((p && q) != *negated),
                    (Some(false), _) | (_, Some(false)) => Some(*negated),
                    _ => None,
                })
            }
            Expr::Like(a, p, negated) => {
                let (x, y) = (a.eval(row, cx)?, p.eval(row, cx)?);
                match (text(&x), text(&y)) {
                    (Some(s), Some(p)) => Value::Int((like_match(&s, &p) != *negated) as i64),
                    _ => Value::Null,
                }
            }
            Expr::Case(operand, whens, els) => {
                let base = match operand {
                    Some(o) => Some(o.eval(row, cx)?),
                    None => None,
                };
                for (w, r) in whens {
                    let c = w.eval(row, cx)?;
                    let hit = match &base {
                        Some(b) => compare(b, &c) == Some(Ordering::Equal),
                        None => truth(&c) == Some(true),
                    };
                    if hit {
                        return r.eval(row, cx);
                    }
                }
                match els {
                    Some(e) => e.eval(row, cx)?,
                    None => Value::Null,
                }
            }
            Expr::Cast(a, to) => cast(a.eval(row, cx)?, *to)?,
            Expr::Func(f, args) => eval_func(*f, args, row, cx)?,
        })
    }

    pub fn eval_bool(&self, row: &[Value], cx: &EvalCtx) -> SqlResult<bool> {
        Ok(truth(&self.eval(row, cx)?) == Some(true))
    }
}

/// Integer value of the longest numeric prefix of a string ("12abc" -> 12),
/// saturating at the i64 range like MySQL.
fn int_prefix(b: &[u8]) -> i64 {
    let s = std::str::from_utf8(b).unwrap_or("").trim_start();
    let bytes = s.as_bytes();
    let (neg, mut i) = match bytes.first() {
        Some(b'-') => (true, 1),
        Some(b'+') => (false, 1),
        _ => (false, 0),
    };
    let start = i;
    let mut v: i128 = 0;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        v = (v * 10 + (bytes[i] - b'0') as i128).min(i64::MAX as i128 + 1);
        i += 1;
    }
    if i == start {
        return 0;
    }
    let v = if neg { -v } else { v };
    v.clamp(i64::MIN as i128, i64::MAX as i128) as i64
}

fn cast(v: Value, to: CastTo) -> SqlResult<Value> {
    if v.is_null() {
        return Ok(Value::Null);
    }
    Ok(match to {
        // Numbers round half away from zero; strings convert their leading
        // integer prefix (CAST('3.7' AS SIGNED) = 3), as MySQL does.
        CastTo::Signed => Value::Int(match &v {
            Value::Int(i) => *i,
            Value::UInt(u) => *u as i64,
            Value::Str(b) | Value::Bin(b) => int_prefix(b),
            other => to_num(other).unwrap_or(0.0).round() as i64,
        }),
        CastTo::Unsigned => Value::UInt(match &v {
            Value::Int(i) => *i as u64,
            Value::UInt(u) => *u,
            Value::Str(b) | Value::Bin(b) => int_prefix(b) as u64,
            other => to_num(other).unwrap_or(0.0).round() as u64,
        }),
        CastTo::Char | CastTo::Date | CastTo::DateTime => Value::Str(v.to_text().unwrap_or_default()),
        CastTo::Binary => Value::Bin(v.to_text().unwrap_or_default()),
        CastTo::Double => Value::Float(to_num(&v).unwrap_or(0.0)),
        CastTo::Decimal(scale) => {
            let s = match &v {
                Value::Decimal(d) => d.clone(),
                other => format!("{}", to_num(other).unwrap_or(0.0)),
            };
            Value::Decimal(crate::types::rescale_decimal(&s, scale).unwrap_or_else(|| "0".into()))
        }
    })
}

fn eval_func(f: Func, args: &[Expr], row: &[Value], cx: &EvalCtx) -> SqlResult<Value> {
    use Func::*;
    let arg = |i: usize| -> SqlResult<Value> { args.get(i).map(|a| a.eval(row, cx)).unwrap_or(Ok(Value::Null)) };
    let all = || -> SqlResult<Vec<Value>> { args.iter().map(|a| a.eval(row, cx)).collect() };
    Ok(match f {
        Concat => {
            let vals = all()?;
            if vals.iter().any(|v| v.is_null()) {
                return Ok(Value::Null);
            }
            let mut out = Vec::new();
            for v in vals {
                out.extend_from_slice(&v.to_text().unwrap_or_default());
            }
            Value::Str(Bytes::from(out))
        }
        ConcatWs => {
            let vals = all()?;
            let sep = match vals.first().and_then(|v| v.to_text()) {
                Some(s) => s,
                None => return Ok(Value::Null),
            };
            let parts: Vec<Bytes> = vals[1..].iter().filter_map(|v| v.to_text()).collect();
            Value::Str(Bytes::from(parts.join(&sep[..])))
        }
        Length => match text(&arg(0)?) {
            Some(b) => Value::Int(b.len() as i64),
            None => Value::Null,
        },
        CharLength => match text(&arg(0)?) {
            Some(b) => Value::Int(String::from_utf8_lossy(&b).chars().count() as i64),
            None => Value::Null,
        },
        Lower | Upper => match arg(0)? {
            Value::Null => Value::Null,
            v => {
                let s = text_str(&v);
                Value::str(&if f == Lower { s.to_lowercase() } else { s.to_uppercase() })
            }
        },
        Substring => {
            let s = arg(0)?;
            if s.is_null() {
                return Ok(Value::Null);
            }
            let chars: Vec<char> = text_str(&s).chars().collect();
            let pos = int_arg(&arg(1)?).unwrap_or(1);
            let n = chars.len() as i64;
            let start = if pos > 0 {
                pos - 1
            } else if pos < 0 {
                n + pos
            } else {
                return Ok(Value::str(""));
            };
            if start < 0 || start >= n {
                return Ok(Value::str(""));
            }
            let len = if args.len() > 2 {
                int_arg(&arg(2)?).unwrap_or(0).max(0)
            } else {
                n - start
            };
            let end = (start + len).min(n);
            Value::str(&chars[start as usize..end as usize].iter().collect::<String>())
        }
        Trim | LTrim | RTrim => match arg(0)? {
            Value::Null => Value::Null,
            v => {
                let s = text_str(&v);
                Value::str(match f {
                    Trim => s.trim_matches(' '),
                    LTrim => s.trim_start_matches(' '),
                    _ => s.trim_end_matches(' '),
                })
            }
        },
        Replace => {
            let (s, from, to) = (arg(0)?, arg(1)?, arg(2)?);
            if s.is_null() || from.is_null() || to.is_null() {
                return Ok(Value::Null);
            }
            let from = text_str(&from);
            if from.is_empty() {
                return Ok(s);
            }
            Value::str(&text_str(&s).replace(&from, &text_str(&to)))
        }
        Abs => match arg(0)? {
            Value::Null => Value::Null,
            Value::Int(i) => Value::Int(i.abs()),
            Value::UInt(u) => Value::UInt(u),
            Value::Decimal(d) => Value::Decimal(d.trim_start_matches('-').to_string()),
            v => Value::Float(to_num(&v).unwrap_or(0.0).abs()),
        },
        Ceil | Floor => match arg(0)? {
            Value::Null => Value::Null,
            v @ (Value::Int(_) | Value::UInt(_)) => v,
            v => {
                let x = to_num(&v).unwrap_or(0.0);
                Value::Int(if f == Ceil { x.ceil() } else { x.floor() } as i64)
            }
        },
        Round => {
            let v = arg(0)?;
            if v.is_null() {
                return Ok(Value::Null);
            }
            let d = if args.len() > 1 {
                int_arg(&arg(1)?).unwrap_or(0)
            } else {
                0
            };
            match v {
                Value::Int(_) | Value::UInt(_) if d >= 0 => v,
                Value::Decimal(s) => Value::Decimal(crate::types::rescale_decimal(&s, d.max(0) as u32).unwrap_or(s)),
                other => {
                    let x = to_num(&other).unwrap_or(0.0);
                    let m = 10f64.powi(d as i32);
                    let r = (x * m).round() / m;
                    if d <= 0 {
                        Value::Int(r as i64)
                    } else {
                        Value::Float(r)
                    }
                }
            }
        }
        Mod => arith(Arith::Mod, &arg(0)?, &arg(1)?)?,
        Coalesce => {
            for a in args {
                let v = a.eval(row, cx)?;
                if !v.is_null() {
                    return Ok(v);
                }
            }
            Value::Null
        }
        IfNull => {
            let v = arg(0)?;
            if v.is_null() {
                arg(1)?
            } else {
                v
            }
        }
        NullIf => {
            let (a, b) = (arg(0)?, arg(1)?);
            if compare(&a, &b) == Some(Ordering::Equal) {
                Value::Null
            } else {
                a
            }
        }
        If => {
            if truth(&arg(0)?) == Some(true) {
                arg(1)?
            } else {
                arg(2)?
            }
        }
        Greatest | Least => {
            let vals = all()?;
            if vals.iter().any(|v| v.is_null()) {
                return Ok(Value::Null);
            }
            let mut best = vals[0].clone();
            for v in &vals[1..] {
                let o = compare(v, &best).unwrap_or(Ordering::Equal);
                if (f == Greatest && o == Ordering::Greater) || (f == Least && o == Ordering::Less) {
                    best = v.clone();
                }
            }
            best
        }
        Now => Value::str(cx.now),
        CurDate => Value::str(&cx.now[..10]),
        UnixTimestamp => Value::Int(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0),
        ),
        Database => cx.db.map(Value::str).unwrap_or(Value::Null),
        Version => Value::str(crate::SERVER_VERSION),
        ConnectionId => Value::UInt(cx.conn_id as u64),
        LastInsertId => Value::UInt(cx.last_insert_id),
        User => Value::str(&format!("{}@%", cx.user)),
        Rand => Value::Float(rand_f64()),
        Hex => match arg(0)? {
            Value::Null => Value::Null,
            Value::Int(i) => Value::str(&format!("{:X}", i)),
            Value::UInt(u) => Value::str(&format!("{:X}", u)),
            v => Value::str(
                &v.to_text()
                    .unwrap_or_default()
                    .iter()
                    .map(|b| format!("{:02X}", b))
                    .collect::<String>(),
            ),
        },
        Left | Right => {
            let s = arg(0)?;
            let n = int_arg(&arg(1)?);
            match (s.is_null(), n) {
                (false, Some(n)) => {
                    let chars: Vec<char> = text_str(&s).chars().collect();
                    let n = (n.max(0) as usize).min(chars.len());
                    let slice = if f == Left {
                        &chars[..n]
                    } else {
                        &chars[chars.len() - n..]
                    };
                    Value::str(&slice.iter().collect::<String>())
                }
                _ => Value::Null,
            }
        }
        Lpad | Rpad => {
            let (s, n, p) = (arg(0)?, int_arg(&arg(1)?), arg(2)?);
            match (s.is_null() || p.is_null(), n) {
                (false, Some(n)) => {
                    let s: Vec<char> = text_str(&s).chars().collect();
                    let p: Vec<char> = text_str(&p).chars().collect();
                    let n = n.max(0) as usize;
                    if s.len() >= n {
                        Value::str(&s[..n].iter().collect::<String>())
                    } else if p.is_empty() {
                        Value::Null
                    } else {
                        let pad: String = p.iter().cycle().take(n - s.len()).collect();
                        let s: String = s.iter().collect();
                        Value::str(&if f == Lpad { pad + &s } else { s + &pad })
                    }
                }
                _ => Value::Null,
            }
        }
        Repeat => {
            let (s, n) = (arg(0)?, int_arg(&arg(1)?));
            match (s.is_null(), n) {
                (false, Some(n)) => Value::str(&text_str(&s).repeat(n.clamp(0, 1 << 20) as usize)),
                _ => Value::Null,
            }
        }
        Sign => match to_num(&arg(0)?) {
            Some(x) => Value::Int(if x > 0.0 {
                1
            } else if x < 0.0 {
                -1
            } else {
                0
            }),
            None => Value::Null,
        },
        Sqrt => match to_num(&arg(0)?) {
            Some(x) if x >= 0.0 => Value::Float(x.sqrt()),
            _ => Value::Null,
        },
        Pow => match (to_num(&arg(0)?), to_num(&arg(1)?)) {
            (Some(x), Some(y)) => Value::Float(x.powf(y)),
            _ => Value::Null,
        },
        Locate => {
            let (a, b) = (arg(0)?, arg(1)?);
            if a.is_null() || b.is_null() {
                return Ok(Value::Null);
            }
            let (needle, hay) = (text_str(&a), text_str(&b));
            Value::Int(
                hay.find(&needle)
                    .map(|i| hay[..i].chars().count() as i64 + 1)
                    .unwrap_or(0),
            )
        }
        Ascii => match text(&arg(0)?) {
            Some(b) => Value::Int(b.first().copied().unwrap_or(0) as i64),
            None => Value::Null,
        },
        FoundRows => Value::UInt(cx.found_rows),
        RowCount => Value::Int(cx.row_count),
    })
}

fn rand_f64() -> f64 {
    use std::cell::Cell;
    thread_local!(static S: Cell<u64> = Cell::new(0x9E37_79B9_7F4A_7C15 ^ std::process::id() as u64));
    S.with(|s| {
        let mut x = s.get();
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        s.set(x);
        (x >> 11) as f64 / (1u64 << 53) as f64
    })
}

/// Static result type of an expression, for result-set metadata.
#[derive(Clone, Debug)]
pub struct ExprType {
    pub ty: ColumnType,
    pub sql: Option<SqlType>,
    pub nullable: bool,
    pub decimals: u8,
}

impl ExprType {
    pub fn of(ty: ColumnType) -> ExprType {
        ExprType {
            ty,
            sql: None,
            nullable: true,
            decimals: match ty {
                ColumnType::Double | ColumnType::Float => 31,
                _ => 0,
            },
        }
    }
}

pub fn infer(e: &Expr, input: &[ExprType], params: &[Value]) -> ExprType {
    use ColumnType as C;
    match e {
        Expr::Col(i) => input.get(*i).cloned().unwrap_or_else(|| ExprType::of(C::VarString)),
        Expr::Lit(v) => value_type(v),
        Expr::Param(i) => params
            .get(*i)
            .map(value_type)
            .unwrap_or_else(|| ExprType::of(C::VarString)),
        Expr::Var(_) => ExprType::of(C::VarString),
        Expr::Cmp(..)
        | Expr::And(..)
        | Expr::Or(..)
        | Expr::Xor(..)
        | Expr::Not(_)
        | Expr::IsNull(..)
        | Expr::IsTrue(..)
        | Expr::InList(..)
        | Expr::Between(..)
        | Expr::Like(..) => ExprType::of(C::LongLong),
        Expr::Arith(a, op, b) => {
            let (x, y) = (infer(a, input, params), infer(b, input, params));
            let int = |t: &ExprType| matches!(t.ty, C::Tiny | C::Short | C::Long | C::LongLong | C::Int24 | C::Year);
            let dbl = |t: &ExprType| matches!(t.ty, C::Double | C::Float | C::VarString | C::String | C::Blob);
            if *op == Arith::IntDiv {
                ExprType::of(C::LongLong)
            } else if dbl(&x) || dbl(&y) {
                ExprType::of(C::Double)
            } else if *op == Arith::Div {
                let mut t = ExprType::of(C::NewDecimal);
                t.decimals = (x.decimals + 4).min(30);
                t
            } else if int(&x) && int(&y) {
                ExprType::of(C::LongLong)
            } else {
                let mut t = ExprType::of(C::NewDecimal);
                t.decimals = x.decimals.max(y.decimals);
                t
            }
        }
        Expr::Neg(a) => infer(a, input, params),
        Expr::Cast(_, to) => ExprType::of(match to {
            CastTo::Signed | CastTo::Unsigned => C::LongLong,
            CastTo::Double => C::Double,
            CastTo::Decimal(_) => C::NewDecimal,
            _ => C::VarString,
        }),
        Expr::Case(_, w, e) => w
            .first()
            .map(|(_, r)| infer(r, input, params))
            .or_else(|| e.as_ref().map(|x| infer(x, input, params)))
            .unwrap_or_else(|| ExprType::of(C::VarString)),
        Expr::Func(f, args) => {
            use Func::*;
            match f {
                Length | CharLength | ConnectionId | LastInsertId | UnixTimestamp | Locate | Ascii | Sign
                | FoundRows | RowCount => ExprType::of(C::LongLong),
                Ceil | Floor => ExprType::of(C::LongLong),
                Abs | Round | Mod | Greatest | Least | Coalesce | IfNull | NullIf => args
                    .first()
                    .map(|a| infer(a, input, params))
                    .unwrap_or_else(|| ExprType::of(C::VarString)),
                If => args
                    .get(1)
                    .map(|a| infer(a, input, params))
                    .unwrap_or_else(|| ExprType::of(C::VarString)),
                Rand | Sqrt | Pow => ExprType::of(C::Double),
                Now => ExprType::of(C::DateTime),
                CurDate => ExprType::of(C::Date),
                _ => ExprType::of(C::VarString),
            }
        }
    }
}

pub fn value_type(v: &Value) -> ExprType {
    use ColumnType as C;
    match v {
        Value::Null => ExprType::of(C::Null),
        Value::Int(_) | Value::UInt(_) => ExprType {
            ty: C::LongLong,
            sql: None,
            nullable: false,
            decimals: 0,
        },
        Value::Float(_) => ExprType {
            ty: C::Double,
            sql: None,
            nullable: false,
            decimals: 31,
        },
        Value::Decimal(d) => ExprType {
            ty: C::NewDecimal,
            sql: None,
            nullable: false,
            decimals: d.split_once('.').map(|(_, f)| f.len() as u8).unwrap_or(0),
        },
        Value::Str(_) => ExprType {
            ty: C::VarString,
            sql: None,
            nullable: false,
            decimals: 0,
        },
        Value::Bin(_) => ExprType {
            ty: C::Blob,
            sql: None,
            nullable: false,
            decimals: 0,
        },
    }
}

pub fn column_expr_type(t: &SqlType, nullable: bool) -> ExprType {
    ExprType {
        ty: t.column_type(),
        sql: Some(t.clone()),
        nullable,
        decimals: match t.base {
            BaseType::Decimal => t.scale.unwrap_or(0) as u8,
            BaseType::Double | BaseType::Float => 31,
            _ => 0,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cx<'a>(p: &'a [Value]) -> EvalCtx<'a> {
        EvalCtx {
            params: p,
            db: Some("d"),
            user: "u",
            conn_id: 7,
            last_insert_id: 0,
            found_rows: 0,
            row_count: 0,
            now: "2026-01-02 03:04:05",
        }
    }

    #[test]
    fn like() {
        assert!(like_match(b"hello", b"h%o"));
        assert!(like_match(b"hello", b"_ello"));
        assert!(!like_match(b"hello", b"h%x"));
        assert!(like_match(b"50%", b"50\\%"));
        assert!(like_match(b"", b"%"));
    }

    #[test]
    fn three_valued_logic() {
        let p = [];
        let c = cx(&p);
        let null = Expr::Lit(Value::Null);
        let t = Expr::Lit(Value::Int(1));
        let f = Expr::Lit(Value::Int(0));
        assert_eq!(
            Expr::And(Box::new(null.clone()), Box::new(f.clone()))
                .eval(&[], &c)
                .unwrap(),
            Value::Int(0)
        );
        assert_eq!(
            Expr::Or(Box::new(null.clone()), Box::new(t.clone()))
                .eval(&[], &c)
                .unwrap(),
            Value::Int(1)
        );
        assert_eq!(
            Expr::And(Box::new(null.clone()), Box::new(t)).eval(&[], &c).unwrap(),
            Value::Null
        );
        let inl = Expr::InList(
            Box::new(Expr::Lit(Value::Int(3))),
            vec![Expr::Lit(Value::Int(1)), null],
            false,
        );
        assert_eq!(inl.eval(&[], &c).unwrap(), Value::Null);
    }

    #[test]
    fn functions() {
        let p = [];
        let c = cx(&p);
        let call = |f: Func, a: Vec<Value>| {
            Expr::Func(f, a.into_iter().map(Expr::Lit).collect())
                .eval(&[], &c)
                .unwrap()
        };
        assert_eq!(
            call(Func::Concat, vec![Value::str("a"), Value::Int(1)]),
            Value::str("a1")
        );
        assert_eq!(
            call(
                Func::Substring,
                vec![Value::str("abcdef"), Value::Int(2), Value::Int(3)]
            ),
            Value::str("bcd")
        );
        assert_eq!(
            call(Func::Substring, vec![Value::str("abcdef"), Value::Int(-2)]),
            Value::str("ef")
        );
        assert_eq!(call(Func::Coalesce, vec![Value::Null, Value::Int(5)]), Value::Int(5));
        assert_eq!(
            call(Func::Round, vec![Value::Float(2.567), Value::Int(1)]),
            Value::Float(2.6)
        );
        assert_eq!(
            call(Func::Lpad, vec![Value::str("7"), Value::Int(3), Value::str("0")]),
            Value::str("007")
        );
    }

    #[test]
    fn datetime_format() {
        assert_eq!(format_datetime(0), "1970-01-01 00:00:00");
        assert_eq!(format_datetime(1_700_000_000), "2023-11-14 22:13:20");
    }
}
