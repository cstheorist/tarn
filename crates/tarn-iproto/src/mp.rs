//! MessagePack helpers for the IPROTO hot path.
//!
//! * `write_*` functions append the smallest MessagePack encoding of a scalar
//!   to a `Vec<u8>` (no `Result`, no intermediate allocation).
//! * `read_*` / `decode_*` functions take `&mut &[u8]` and advance the slice
//!   past what they consumed.
//! * [`encode_value`] / [`decode_value`] map between MessagePack and
//!   [`tarn_common::Value`], including Tarantool's `MP_DECIMAL` (ext 1) and
//!   `MP_DATETIME` (ext 4) extensions.
//! * [`decode_tuple_bytes`] / [`decode_value_bytes`] are zero-copy variants:
//!   strings and binaries in the result are slices of the source `Bytes`
//!   instead of fresh allocations.

use bytes::Bytes;
use tarn_common::Value;

use crate::Error;

/// Tarantool MsgPack extension type ids (`src/lib/core/mp_extension_types.h`).
pub const EXT_DECIMAL: i8 = 1;
pub const EXT_UUID: i8 = 2;
pub const EXT_ERROR: i8 = 3;
pub const EXT_DATETIME: i8 = 4;
pub const EXT_COMPRESSION: i8 = 5;
pub const EXT_INTERVAL: i8 = 6;
pub const EXT_TUPLE: i8 = 7;

/// Maximum number of significant digits Tarantool accepts in a decimal
/// (`DECIMAL_MAX_DIGITS` in `src/lib/core/decimal.h`).
pub const DECIMAL_MAX_DIGITS: usize = 76;

/// Decimals whose scale is outside this range are rendered in scientific
/// notation instead of plain notation, to bound the string size.
const PLAIN_SCALE_LIMIT: i64 = 1000;

#[cold]
fn err(msg: &str) -> Error {
    Error::Protocol(format!("msgpack: {msg}"))
}

#[cold]
fn eof() -> Error {
    err("unexpected end of data")
}

// ---------------------------------------------------------------------------
// Writers
// ---------------------------------------------------------------------------

#[inline]
pub fn write_nil(out: &mut Vec<u8>) {
    out.push(0xc0);
}

#[inline]
pub fn write_bool(out: &mut Vec<u8>, v: bool) {
    out.push(if v { 0xc3 } else { 0xc2 });
}

/// Smallest unsigned encoding (positive fixint, uint8/16/32/64).
#[inline]
pub fn write_uint(out: &mut Vec<u8>, v: u64) {
    if v < 0x80 {
        out.push(v as u8);
    } else if v <= 0xff {
        out.extend_from_slice(&[0xcc, v as u8]);
    } else if v <= 0xffff {
        let b = (v as u16).to_be_bytes();
        out.extend_from_slice(&[0xcd, b[0], b[1]]);
    } else if v <= 0xffff_ffff {
        let b = (v as u32).to_be_bytes();
        out.extend_from_slice(&[0xce, b[0], b[1], b[2], b[3]]);
    } else {
        out.push(0xcf);
        out.extend_from_slice(&v.to_be_bytes());
    }
}

/// Smallest signed encoding. Non-negative values use the unsigned family,
/// as Tarantool expects (an `unsigned` field rejects `MP_INT`).
#[inline]
pub fn write_int(out: &mut Vec<u8>, v: i64) {
    if v >= 0 {
        write_uint(out, v as u64);
    } else if v >= -32 {
        out.push(v as u8);
    } else if v >= i8::MIN as i64 {
        out.extend_from_slice(&[0xd0, v as u8]);
    } else if v >= i16::MIN as i64 {
        let b = (v as i16).to_be_bytes();
        out.extend_from_slice(&[0xd1, b[0], b[1]]);
    } else if v >= i32::MIN as i64 {
        let b = (v as i32).to_be_bytes();
        out.extend_from_slice(&[0xd2, b[0], b[1], b[2], b[3]]);
    } else {
        out.push(0xd3);
        out.extend_from_slice(&v.to_be_bytes());
    }
}

#[inline]
pub fn write_f64(out: &mut Vec<u8>, v: f64) {
    out.push(0xcb);
    out.extend_from_slice(&v.to_bits().to_be_bytes());
}

#[inline]
pub fn write_f32(out: &mut Vec<u8>, v: f32) {
    out.push(0xca);
    out.extend_from_slice(&v.to_bits().to_be_bytes());
}

/// String header + bytes. The caller is responsible for UTF-8 validity.
#[inline]
pub fn write_str(out: &mut Vec<u8>, s: &[u8]) {
    let n = s.len();
    if n < 32 {
        out.push(0xa0 | n as u8);
    } else if n <= 0xff {
        out.extend_from_slice(&[0xd9, n as u8]);
    } else if n <= 0xffff {
        let b = (n as u16).to_be_bytes();
        out.extend_from_slice(&[0xda, b[0], b[1]]);
    } else {
        let b = (n as u32).to_be_bytes();
        out.extend_from_slice(&[0xdb, b[0], b[1], b[2], b[3]]);
    }
    out.extend_from_slice(s);
}

#[inline]
pub fn write_bin(out: &mut Vec<u8>, s: &[u8]) {
    let n = s.len();
    if n <= 0xff {
        out.extend_from_slice(&[0xc4, n as u8]);
    } else if n <= 0xffff {
        let b = (n as u16).to_be_bytes();
        out.extend_from_slice(&[0xc5, b[0], b[1]]);
    } else {
        let b = (n as u32).to_be_bytes();
        out.extend_from_slice(&[0xc6, b[0], b[1], b[2], b[3]]);
    }
    out.extend_from_slice(s);
}

#[inline]
pub fn write_array_len(out: &mut Vec<u8>, n: u32) {
    if n < 16 {
        out.push(0x90 | n as u8);
    } else if n <= 0xffff {
        let b = (n as u16).to_be_bytes();
        out.extend_from_slice(&[0xdc, b[0], b[1]]);
    } else {
        let b = n.to_be_bytes();
        out.extend_from_slice(&[0xdd, b[0], b[1], b[2], b[3]]);
    }
}

#[inline]
pub fn write_map_len(out: &mut Vec<u8>, n: u32) {
    if n < 16 {
        out.push(0x80 | n as u8);
    } else if n <= 0xffff {
        let b = (n as u16).to_be_bytes();
        out.extend_from_slice(&[0xde, b[0], b[1]]);
    } else {
        let b = n.to_be_bytes();
        out.extend_from_slice(&[0xdf, b[0], b[1], b[2], b[3]]);
    }
}

/// Extension header; uses fixext for payloads of 1/2/4/8/16 bytes like
/// msgpuck's `mp_encode_extl`.
#[inline]
pub fn write_ext_header(out: &mut Vec<u8>, ty: i8, len: u32) {
    let t = ty as u8;
    match len {
        1 => out.extend_from_slice(&[0xd4, t]),
        2 => out.extend_from_slice(&[0xd5, t]),
        4 => out.extend_from_slice(&[0xd6, t]),
        8 => out.extend_from_slice(&[0xd7, t]),
        16 => out.extend_from_slice(&[0xd8, t]),
        n if n <= 0xff => out.extend_from_slice(&[0xc7, n as u8, t]),
        n if n <= 0xffff => {
            let b = (n as u16).to_be_bytes();
            out.extend_from_slice(&[0xc8, b[0], b[1], t]);
        }
        n => {
            let b = n.to_be_bytes();
            out.extend_from_slice(&[0xc9, b[0], b[1], b[2], b[3], t]);
        }
    }
}

#[inline]
fn uint_size(v: u64) -> usize {
    if v < 0x80 {
        1
    } else if v <= 0xff {
        2
    } else if v <= 0xffff {
        3
    } else if v <= 0xffff_ffff {
        5
    } else {
        9
    }
}

#[inline]
fn int_size(v: i64) -> usize {
    if v >= 0 {
        uint_size(v as u64)
    } else if v >= -32 {
        1
    } else if v >= i8::MIN as i64 {
        2
    } else if v >= i16::MIN as i64 {
        3
    } else if v >= i32::MIN as i64 {
        5
    } else {
        9
    }
}

/// Encode a [`Value`]: smallest integer encodings, `Float` as float64,
/// `Decimal` as `MP_DECIMAL`, `Str` as str, `Bin` as bin.
///
/// A `Decimal` whose text cannot be parsed (or has more than
/// [`DECIMAL_MAX_DIGITS`] significant digits) is encoded as a string so the
/// server rejects it with a type error instead of storing a wrong number;
/// use [`encode_decimal`] to get the parse error instead.
#[inline]
pub fn encode_value(out: &mut Vec<u8>, v: &Value) {
    match v {
        Value::Null => write_nil(out),
        Value::Int(i) => write_int(out, *i),
        Value::UInt(u) => write_uint(out, *u),
        Value::Float(f) => write_f64(out, *f),
        Value::Decimal(s) => {
            if encode_decimal(out, s).is_err() {
                write_str(out, s.as_bytes());
            }
        }
        Value::Str(b) => write_str(out, b),
        Value::Bin(b) => write_bin(out, b),
    }
}

/// Array header followed by each value.
pub fn encode_tuple(out: &mut Vec<u8>, vals: &[Value]) {
    write_array_len(out, vals.len() as u32);
    for v in vals {
        encode_value(out, v);
    }
}

// ---------------------------------------------------------------------------
// MP_DECIMAL
// ---------------------------------------------------------------------------

/// Encode decimal text (`[+-]digits[.digits][(e|E)[+-]digits]`) as a Tarantool
/// `MP_DECIMAL` extension: `scale` (MP_UINT, or MP_INT when negative) followed
/// by packed BCD digits with a trailing sign nibble (`0xC` plus, `0xD` minus),
/// exactly like `decimal_pack()` / `decPackedFromNumber()`.
///
/// Trailing fractional zeros are significant (`"1.50"` keeps scale 2). Zero is
/// always encoded as positive. Nothing is written on error.
pub fn encode_decimal(out: &mut Vec<u8>, text: &str) -> Result<(), Error> {
    let s = text.trim().as_bytes();
    let mut i = 0;
    let mut neg = false;
    match s.first() {
        Some(b'-') => {
            neg = true;
            i = 1;
        }
        Some(b'+') => i = 1,
        _ => {}
    }
    let mut digits = [0u8; DECIMAL_MAX_DIGITS];
    let mut n = 0usize;
    let mut frac: i64 = 0;
    let mut seen_digit = false;
    let mut seen_dot = false;
    while i < s.len() {
        let c = s[i];
        match c {
            b'0'..=b'9' => {
                seen_digit = true;
                if seen_dot {
                    frac += 1;
                }
                if n > 0 || c != b'0' {
                    if n == DECIMAL_MAX_DIGITS {
                        return Err(err("decimal has too many digits"));
                    }
                    digits[n] = c - b'0';
                    n += 1;
                }
            }
            b'.' if !seen_dot => seen_dot = true,
            b'e' | b'E' => break,
            _ => return Err(err("invalid decimal literal")),
        }
        i += 1;
    }
    if !seen_digit {
        return Err(err("invalid decimal literal"));
    }
    let mut exp: i64 = 0;
    if i < s.len() {
        // s[i] is the exponent marker.
        i += 1;
        let mut eneg = false;
        match s.get(i) {
            Some(b'-') => {
                eneg = true;
                i += 1;
            }
            Some(b'+') => i += 1,
            _ => {}
        }
        if i >= s.len() {
            return Err(err("invalid decimal exponent"));
        }
        while i < s.len() {
            let c = s[i];
            if !c.is_ascii_digit() {
                return Err(err("invalid decimal exponent"));
            }
            exp = exp * 10 + (c - b'0') as i64;
            if exp > i32::MAX as i64 {
                return Err(err("decimal exponent out of range"));
            }
            i += 1;
        }
        if eneg {
            exp = -exp;
        }
    }
    let mut scale = frac - exp;
    if n == 0 {
        // Zero: one zero digit, never negative, no negative scale.
        digits[0] = 0;
        n = 1;
        neg = false;
        scale = scale.max(0);
    }
    if scale > i32::MAX as i64 || scale < i32::MIN as i64 {
        return Err(err("decimal scale out of range"));
    }

    let scale_len = int_size(scale);
    let bcd_len = n / 2 + 1;
    write_ext_header(out, EXT_DECIMAL, (scale_len + bcd_len) as u32);
    write_int(out, scale);
    // Digits are right-aligned in front of the sign nibble; at most one
    // leading pad nibble.
    let pad = 2 * bcd_len - 1 - n;
    let sign = if neg { 0x0d } else { 0x0c };
    let nib = |k: usize| -> u8 {
        if k < pad {
            0
        } else if k < 2 * bcd_len - 1 {
            digits[k - pad]
        } else {
            sign
        }
    };
    out.reserve(bcd_len);
    for j in 0..bcd_len {
        out.push((nib(2 * j) << 4) | nib(2 * j + 1));
    }
    Ok(())
}

/// Decode the payload of an `MP_DECIMAL` extension (without the ext header)
/// into canonical plain text: `"-12.340"`, `"1000"` (for `1E+3`),
/// `"0.0000001"` (for `1E-7`). Negative zero is rendered as `"0"`. Values
/// whose scale is beyond ±1000 are rendered in scientific notation.
pub fn decode_decimal(data: &[u8]) -> Result<String, Error> {
    let mut p = data;
    let first = *p.first().ok_or_else(eof)?;
    let scale: i64 = match first {
        0x00..=0x7f | 0xcc..=0xcf => {
            let v = read_uint(&mut p)?;
            if v > i32::MAX as u64 {
                return Err(err("decimal scale out of range"));
            }
            v as i64
        }
        0xd0..=0xd3 | 0xe0..=0xff => {
            let v = read_int(&mut p)?;
            if v < i32::MIN as i64 || v > i32::MAX as i64 {
                return Err(err("decimal scale out of range"));
            }
            v
        }
        _ => return Err(err("decimal scale is not an integer")),
    };
    let bcd = p;
    let Some(&last) = bcd.last() else {
        return Err(err("decimal has no digits"));
    };
    let neg = match last & 0x0f {
        0x0a | 0x0c | 0x0e | 0x0f => false,
        0x0b | 0x0d => true,
        _ => return Err(err("decimal has no sign nibble")),
    };
    let total = bcd.len() * 2 - 1;
    let nib = |k: usize| -> u8 {
        let byte = bcd[k >> 1];
        if k & 1 == 0 {
            byte >> 4
        } else {
            byte & 0x0f
        }
    };
    let mut start = total;
    for k in 0..total {
        let d = nib(k);
        if d > 9 {
            return Err(err("invalid BCD digit in decimal"));
        }
        if start == total && d != 0 {
            start = k;
        }
    }
    let n = total - start;
    Ok(render_decimal(neg, n, |i| b'0' + nib(start + i), scale))
}

fn render_decimal(neg: bool, n: usize, digit: impl Fn(usize) -> u8, scale: i64) -> String {
    let mut s = String::new();
    if n == 0 {
        s.push('0');
        if scale > 0 && scale <= PLAIN_SCALE_LIMIT {
            s.push('.');
            for _ in 0..scale {
                s.push('0');
            }
        }
        return s;
    }
    if neg {
        s.push('-');
    }
    let push_digits = |s: &mut String, from: usize, to: usize| {
        for i in from..to {
            s.push(digit(i) as char);
        }
    };
    if !(-PLAIN_SCALE_LIMIT..=PLAIN_SCALE_LIMIT).contains(&scale) {
        // d.dddE+x
        s.push(digit(0) as char);
        if n > 1 {
            s.push('.');
            push_digits(&mut s, 1, n);
        }
        let adj = (n as i64 - 1) - scale;
        s.push('E');
        if adj >= 0 {
            s.push('+');
        }
        s.push_str(&adj.to_string());
        return s;
    }
    if scale <= 0 {
        s.reserve(n + (-scale) as usize);
        push_digits(&mut s, 0, n);
        for _ in 0..(-scale) {
            s.push('0');
        }
    } else if (scale as usize) < n {
        let int_digits = n - scale as usize;
        s.reserve(n + 1);
        push_digits(&mut s, 0, int_digits);
        s.push('.');
        push_digits(&mut s, int_digits, n);
    } else {
        s.reserve(scale as usize + 2);
        s.push_str("0.");
        for _ in 0..(scale as usize - n) {
            s.push('0');
        }
        push_digits(&mut s, 0, n);
    }
    s
}

// ---------------------------------------------------------------------------
// MP_DATETIME
// ---------------------------------------------------------------------------

/// Decode the payload of an `MP_DATETIME` extension (8 bytes of little-endian
/// epoch seconds, optionally followed by `nsec: i32`, `tzoffset: i16`,
/// `tzindex: i16`) into `"YYYY-MM-DD HH:MM:SS[.ffffff]"`. The epoch is UTC;
/// the timezone fields are ignored. Microseconds are printed only when
/// non-zero (sub-microsecond precision is truncated).
pub fn decode_datetime(data: &[u8]) -> Result<String, Error> {
    if data.len() != 8 && data.len() != 16 {
        return Err(err("invalid datetime length"));
    }
    let secs = i64::from_le_bytes(data[0..8].try_into().unwrap());
    let nsec = if data.len() == 16 {
        i32::from_le_bytes(data[8..12].try_into().unwrap())
    } else {
        0
    };
    if !(0..1_000_000_000).contains(&nsec) {
        return Err(err("invalid datetime nanoseconds"));
    }
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    let (hh, mm, ss) = (sod / 3600, (sod / 60) % 60, sod % 60);
    let micros = nsec / 1000;
    Ok(if micros != 0 {
        format!("{y:04}-{m:02}-{d:02} {hh:02}:{mm:02}:{ss:02}.{micros:06}")
    } else {
        format!("{y:04}-{m:02}-{d:02} {hh:02}:{mm:02}:{ss:02}")
    })
}

/// Days since 1970-01-01 to (year, month, day) in the proleptic Gregorian
/// calendar (H. Hinnant's `civil_from_days`).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

// ---------------------------------------------------------------------------
// Readers
// ---------------------------------------------------------------------------

#[inline]
fn take<'a>(buf: &mut &'a [u8], n: usize) -> Result<&'a [u8], Error> {
    if buf.len() < n {
        return Err(eof());
    }
    let (head, tail) = buf.split_at(n);
    *buf = tail;
    Ok(head)
}

#[inline]
fn byte(buf: &mut &[u8]) -> Result<u8, Error> {
    match buf.split_first() {
        Some((&b, rest)) => {
            *buf = rest;
            Ok(b)
        }
        None => Err(eof()),
    }
}

#[inline]
fn be16(buf: &mut &[u8]) -> Result<u16, Error> {
    let b = take(buf, 2)?;
    Ok(u16::from_be_bytes([b[0], b[1]]))
}

#[inline]
fn be32(buf: &mut &[u8]) -> Result<u32, Error> {
    let b = take(buf, 4)?;
    Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
}

#[inline]
fn be64(buf: &mut &[u8]) -> Result<u64, Error> {
    let b = take(buf, 8)?;
    Ok(u64::from_be_bytes(b.try_into().unwrap()))
}

/// Read an array header (fixarray, array16, array32).
#[inline]
pub fn read_array_len(buf: &mut &[u8]) -> Result<u32, Error> {
    let b = *buf.first().ok_or_else(eof)?;
    match b {
        0x90..=0x9f => {
            *buf = &buf[1..];
            Ok((b & 0x0f) as u32)
        }
        0xdc => {
            *buf = &buf[1..];
            Ok(be16(buf)? as u32)
        }
        0xdd => {
            *buf = &buf[1..];
            be32(buf)
        }
        _ => Err(err("expected array")),
    }
}

/// Read a map header (fixmap, map16, map32).
#[inline]
pub fn read_map_len(buf: &mut &[u8]) -> Result<u32, Error> {
    let b = *buf.first().ok_or_else(eof)?;
    match b {
        0x80..=0x8f => {
            *buf = &buf[1..];
            Ok((b & 0x0f) as u32)
        }
        0xde => {
            *buf = &buf[1..];
            Ok(be16(buf)? as u32)
        }
        0xdf => {
            *buf = &buf[1..];
            be32(buf)
        }
        _ => Err(err("expected map")),
    }
}

/// Read a non-negative integer (any unsigned encoding, or a signed encoding
/// holding a non-negative value).
#[inline]
pub fn read_uint(buf: &mut &[u8]) -> Result<u64, Error> {
    let b = *buf.first().ok_or_else(eof)?;
    let v = match b {
        0x00..=0x7f => {
            *buf = &buf[1..];
            return Ok(b as u64);
        }
        0xcc..=0xcf => {
            *buf = &buf[1..];
            match b {
                0xcc => byte(buf)? as u64,
                0xcd => be16(buf)? as u64,
                0xce => be32(buf)? as u64,
                _ => be64(buf)?,
            }
        }
        0xd0..=0xd3 | 0xe0..=0xff => {
            let mut p = *buf;
            let v = read_int(&mut p)?;
            if v < 0 {
                return Err(err("expected unsigned integer"));
            }
            *buf = p;
            v as u64
        }
        _ => return Err(err("expected unsigned integer")),
    };
    Ok(v)
}

/// Read an integer that fits in `i64` (any integer encoding).
#[inline]
pub fn read_int(buf: &mut &[u8]) -> Result<i64, Error> {
    let b = *buf.first().ok_or_else(eof)?;
    match b {
        0x00..=0x7f => {
            *buf = &buf[1..];
            Ok(b as i64)
        }
        0xe0..=0xff => {
            *buf = &buf[1..];
            Ok(b as i8 as i64)
        }
        0xcc..=0xcf => {
            let v = read_uint(buf)?;
            i64::try_from(v).map_err(|_| err("integer out of i64 range"))
        }
        0xd0 => {
            *buf = &buf[1..];
            Ok(byte(buf)? as i8 as i64)
        }
        0xd1 => {
            *buf = &buf[1..];
            Ok(be16(buf)? as i16 as i64)
        }
        0xd2 => {
            *buf = &buf[1..];
            Ok(be32(buf)? as i32 as i64)
        }
        0xd3 => {
            *buf = &buf[1..];
            Ok(be64(buf)? as i64)
        }
        _ => Err(err("expected integer")),
    }
}

/// Read a string and return its bytes (not validated as UTF-8).
#[inline]
pub fn read_str<'a>(buf: &mut &'a [u8]) -> Result<&'a [u8], Error> {
    let b = *buf.first().ok_or_else(eof)?;
    let n = match b {
        0xa0..=0xbf => {
            *buf = &buf[1..];
            (b & 0x1f) as usize
        }
        0xd9 => {
            *buf = &buf[1..];
            byte(buf)? as usize
        }
        0xda => {
            *buf = &buf[1..];
            be16(buf)? as usize
        }
        0xdb => {
            *buf = &buf[1..];
            be32(buf)? as usize
        }
        _ => return Err(err("expected string")),
    };
    take(buf, n)
}

/// Read a binary string.
#[inline]
pub fn read_bin<'a>(buf: &mut &'a [u8]) -> Result<&'a [u8], Error> {
    let b = *buf.first().ok_or_else(eof)?;
    let n = match b {
        0xc4 => {
            *buf = &buf[1..];
            byte(buf)? as usize
        }
        0xc5 => {
            *buf = &buf[1..];
            be16(buf)? as usize
        }
        0xc6 => {
            *buf = &buf[1..];
            be32(buf)? as usize
        }
        _ => return Err(err("expected binary")),
    };
    take(buf, n)
}

#[inline]
pub fn read_bool(buf: &mut &[u8]) -> Result<bool, Error> {
    match buf.first() {
        Some(0xc2) => {
            *buf = &buf[1..];
            Ok(false)
        }
        Some(0xc3) => {
            *buf = &buf[1..];
            Ok(true)
        }
        Some(_) => Err(err("expected boolean")),
        None => Err(eof()),
    }
}

/// Read a float32/float64 (or an integer) as `f64`.
#[inline]
pub fn read_f64(buf: &mut &[u8]) -> Result<f64, Error> {
    match buf.first() {
        Some(0xca) => {
            *buf = &buf[1..];
            Ok(f32::from_bits(be32(buf)?) as f64)
        }
        Some(0xcb) => {
            *buf = &buf[1..];
            Ok(f64::from_bits(be64(buf)?))
        }
        Some(_) => match read_int(buf) {
            Ok(v) => Ok(v as f64),
            Err(_) => Err(err("expected float")),
        },
        None => Err(eof()),
    }
}

/// Skip one complete value (including nested arrays/maps). Iterative, so
/// deeply nested input cannot overflow the stack.
pub fn skip_value(buf: &mut &[u8]) -> Result<(), Error> {
    let mut remaining: u64 = 1;
    while remaining > 0 {
        remaining -= 1;
        let b = byte(buf)?;
        let skip = match b {
            0x00..=0x7f | 0xe0..=0xff | 0xc0 | 0xc2 | 0xc3 => 0,
            0x80..=0x8f => {
                remaining += 2 * (b & 0x0f) as u64;
                0
            }
            0x90..=0x9f => {
                remaining += (b & 0x0f) as u64;
                0
            }
            0xa0..=0xbf => (b & 0x1f) as usize,
            0xc4 | 0xd9 => byte(buf)? as usize,
            0xc5 | 0xda => be16(buf)? as usize,
            0xc6 | 0xdb => be32(buf)? as usize,
            0xc7 => byte(buf)? as usize + 1,
            0xc8 => be16(buf)? as usize + 1,
            0xc9 => be32(buf)? as usize + 1,
            0xca => 4,
            0xcb => 8,
            0xcc | 0xd0 => 1,
            0xcd | 0xd1 => 2,
            0xce | 0xd2 => 4,
            0xcf | 0xd3 => 8,
            0xd4 => 2,
            0xd5 => 3,
            0xd6 => 5,
            0xd7 => 9,
            0xd8 => 17,
            0xdc => {
                remaining += be16(buf)? as u64;
                0
            }
            0xdd => {
                remaining += be32(buf)? as u64;
                0
            }
            0xde => {
                remaining += 2 * be16(buf)? as u64;
                0
            }
            0xdf => {
                remaining += 2 * be32(buf)? as u64;
                0
            }
            _ => return Err(err("reserved byte 0xc1")),
        };
        if skip > 0 {
            take(buf, skip)?;
        }
    }
    Ok(())
}

/// Skip one value and return the raw bytes it occupied.
#[inline]
pub fn read_raw<'a>(buf: &mut &'a [u8]) -> Result<&'a [u8], Error> {
    let start = *buf;
    skip_value(buf)?;
    Ok(&start[..start.len() - buf.len()])
}

// ---------------------------------------------------------------------------
// Value decoding
// ---------------------------------------------------------------------------

/// How decoded strings/binaries obtain their `Bytes`.
trait Sink {
    fn bytes(&self, s: &[u8]) -> Bytes;
}

struct CopySink;

impl Sink for CopySink {
    #[inline]
    fn bytes(&self, s: &[u8]) -> Bytes {
        Bytes::copy_from_slice(s)
    }
}

struct ShareSink<'a>(&'a Bytes);

impl Sink for ShareSink<'_> {
    #[inline]
    fn bytes(&self, s: &[u8]) -> Bytes {
        let base = self.0.as_ptr() as usize;
        let p = s.as_ptr() as usize;
        if p >= base && p + s.len() <= base + self.0.len() {
            self.0.slice(p - base..p - base + s.len())
        } else {
            Bytes::copy_from_slice(s)
        }
    }
}

#[inline]
fn str_value<S: Sink>(sink: &S, s: &[u8]) -> Value {
    if std::str::from_utf8(s).is_ok() {
        Value::Str(sink.bytes(s))
    } else {
        Value::Bin(sink.bytes(s))
    }
}

fn ext_value<S: Sink>(buf: &mut &[u8], start: &[u8], len: usize, sink: &S) -> Result<Value, Error> {
    let ty = byte(buf)? as i8;
    let data = take(buf, len)?;
    match ty {
        EXT_DECIMAL => Ok(Value::Decimal(decode_decimal(data)?)),
        EXT_DATETIME => Ok(Value::Str(Bytes::from(decode_datetime(data)?))),
        _ => Ok(Value::Bin(sink.bytes(&start[..start.len() - buf.len()]))),
    }
}

#[inline]
fn decode_with<S: Sink>(buf: &mut &[u8], sink: &S) -> Result<Value, Error> {
    let start: &[u8] = buf;
    let b = byte(buf)?;
    Ok(match b {
        0x00..=0x7f => Value::Int(b as i64),
        0xe0..=0xff => Value::Int(b as i8 as i64),
        0xa0..=0xbf => {
            let s = take(buf, (b & 0x1f) as usize)?;
            str_value(sink, s)
        }
        0x80..=0x9f | 0xdc..=0xdf => {
            *buf = start;
            let raw = read_raw(buf)?;
            Value::Bin(sink.bytes(raw))
        }
        0xc0 => Value::Null,
        0xc1 => return Err(err("reserved byte 0xc1")),
        0xc2 => Value::Int(0),
        0xc3 => Value::Int(1),
        0xc4 => {
            let n = byte(buf)? as usize;
            Value::Bin(sink.bytes(take(buf, n)?))
        }
        0xc5 => {
            let n = be16(buf)? as usize;
            Value::Bin(sink.bytes(take(buf, n)?))
        }
        0xc6 => {
            let n = be32(buf)? as usize;
            Value::Bin(sink.bytes(take(buf, n)?))
        }
        0xc7 => {
            let n = byte(buf)? as usize;
            ext_value(buf, start, n, sink)?
        }
        0xc8 => {
            let n = be16(buf)? as usize;
            ext_value(buf, start, n, sink)?
        }
        0xc9 => {
            let n = be32(buf)? as usize;
            ext_value(buf, start, n, sink)?
        }
        0xca => Value::Float(f32::from_bits(be32(buf)?) as f64),
        0xcb => Value::Float(f64::from_bits(be64(buf)?)),
        0xcc => Value::Int(byte(buf)? as i64),
        0xcd => Value::Int(be16(buf)? as i64),
        0xce => Value::Int(be32(buf)? as i64),
        0xcf => {
            let v = be64(buf)?;
            if v <= i64::MAX as u64 {
                Value::Int(v as i64)
            } else {
                Value::UInt(v)
            }
        }
        0xd0 => Value::Int(byte(buf)? as i8 as i64),
        0xd1 => Value::Int(be16(buf)? as i16 as i64),
        0xd2 => Value::Int(be32(buf)? as i32 as i64),
        0xd3 => Value::Int(be64(buf)? as i64),
        0xd4 => ext_value(buf, start, 1, sink)?,
        0xd5 => ext_value(buf, start, 2, sink)?,
        0xd6 => ext_value(buf, start, 4, sink)?,
        0xd7 => ext_value(buf, start, 8, sink)?,
        0xd8 => ext_value(buf, start, 16, sink)?,
        0xd9 => {
            let n = byte(buf)? as usize;
            str_value(sink, take(buf, n)?)
        }
        0xda => {
            let n = be16(buf)? as usize;
            str_value(sink, take(buf, n)?)
        }
        0xdb => {
            let n = be32(buf)? as usize;
            str_value(sink, take(buf, n)?)
        }
    })
}

/// Decode one value and advance `buf`.
///
/// nil → `Null`; bool → `Int(0/1)`; uint ≤ `i64::MAX` → `Int`, larger →
/// `UInt`; negative → `Int`; float32/64 → `Float`; str → `Str` (or `Bin` if
/// the bytes are not valid UTF-8); bin → `Bin`; `MP_DECIMAL` → `Decimal`
/// (see [`decode_decimal`]); `MP_DATETIME` → `Str` (see
/// [`decode_datetime`]); arrays, maps and other extensions → `Bin` holding
/// the raw MessagePack of that value. Strings are copied; see
/// [`decode_value_bytes`] for the zero-copy variant.
#[inline]
pub fn decode_value(buf: &mut &[u8]) -> Result<Value, Error> {
    decode_with(buf, &CopySink)
}

/// Decode an array of values.
pub fn decode_tuple(buf: &mut &[u8]) -> Result<Vec<Value>, Error> {
    let n = read_array_len(buf)? as usize;
    let mut out = Vec::with_capacity(n.min(buf.len()));
    for _ in 0..n {
        out.push(decode_with(buf, &CopySink)?);
    }
    Ok(out)
}

/// Zero-copy [`decode_value`]: `buf` must be a sub-slice of `src`; string and
/// binary results are slices of `src` (bytes outside `src` are copied).
#[inline]
pub fn decode_value_bytes(src: &Bytes, buf: &mut &[u8]) -> Result<Value, Error> {
    decode_with(buf, &ShareSink(src))
}

/// Zero-copy [`decode_tuple`] of the array at `buf` (a sub-slice of `src`).
pub fn decode_tuple_shared(src: &Bytes, buf: &mut &[u8]) -> Result<Vec<Value>, Error> {
    let sink = ShareSink(src);
    let n = read_array_len(buf)? as usize;
    let mut out = Vec::with_capacity(n.min(buf.len()));
    for _ in 0..n {
        out.push(decode_with(buf, &sink)?);
    }
    Ok(out)
}

/// Zero-copy decode of the tuple (array) at the start of `src`.
pub fn decode_tuple_bytes(src: &Bytes) -> Result<Vec<Value>, Error> {
    let mut buf: &[u8] = src;
    decode_tuple_shared(src, &mut buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(s: &str) -> Vec<u8> {
        s.split_whitespace().map(|b| u8::from_str_radix(b, 16).unwrap()).collect()
    }

    fn enc(v: &Value) -> Vec<u8> {
        let mut out = Vec::new();
        encode_value(&mut out, v);
        out
    }

    fn dec(bytes: &[u8]) -> Value {
        let mut p = bytes;
        let v = decode_value(&mut p).unwrap();
        assert!(p.is_empty(), "trailing bytes after {v:?}");
        v
    }

    /// (decimal text, bytes produced by Tarantool 3.9 `msgpack.encode(decimal.new(text))`,
    /// canonical text we decode it to)
    const TARANTOOL_DECIMALS: &[(&str, &str, &str)] = &[
        ("-12.340", "d6 01 03 12 34 0d", "-12.340"),
        ("0", "d5 01 00 0c", "0"),
        ("0.00", "d5 01 02 0c", "0.00"),
        ("1000", "d6 01 00 01 00 0c", "1000"),
        ("1E+3", "d5 01 fd 1c", "1000"),
        ("1.5e3", "c7 03 01 fe 01 5c", "1500"),
        (
            "123456789012345678901234567890.123456",
            "c7 14 01 06 01 23 45 67 89 01 23 45 67 89 01 23 45 67 89 01 23 45 6c",
            "123456789012345678901234567890.123456",
        ),
        ("0.0000001", "d5 01 07 1c", "0.0000001"),
        ("-1", "d5 01 00 1d", "-1"),
        ("99", "c7 03 01 00 09 9c", "99"),
        ("1234", "d6 01 00 01 23 4c", "1234"),
    ];

    #[test]
    fn decimal_encoding_matches_tarantool() {
        for (text, bytes, _) in TARANTOOL_DECIMALS {
            assert_eq!(enc(&Value::Decimal(text.to_string())), hex(bytes), "encoding {text}");
        }
    }

    #[test]
    fn decimal_decoding_matches_tarantool() {
        for (text, bytes, canon) in TARANTOOL_DECIMALS {
            assert_eq!(dec(&hex(bytes)), Value::Decimal(canon.to_string()), "decoding {text}");
        }
        // Tarantool keeps the sign of zero ("-0" → d5 01 00 0d); we print "0".
        assert_eq!(dec(&hex("d5 01 00 0d")), Value::Decimal("0".into()));
        // Alternate sign nibbles (0xB minus, 0xF unsigned) are accepted.
        assert_eq!(dec(&hex("d5 01 01 5b")), Value::Decimal("-0.5".into()));
        assert_eq!(dec(&hex("d5 01 00 7f")), Value::Decimal("7".into()));
    }

    #[test]
    fn decimal_text_forms() {
        let cases = [
            ("+1.50", "1.50"),
            ("-0.000", "0.000"),
            ("007.10", "7.10"),
            (".5", "0.5"),
            ("5.", "5"),
            ("-1.5E-3", "-0.0015"),
            ("12e2", "1200"),
            ("0e5", "0"),
            ("  42 ", "42"),
            ("99999999999999999999999999999999999999", "99999999999999999999999999999999999999"),
            ("-0.00000000000000000000000000000000000001", "-0.00000000000000000000000000000000000001"),
        ];
        for (text, canon) in cases {
            let bytes = enc(&Value::Decimal(text.into()));
            assert!(matches!(bytes[0], 0xc7 | 0xd4..=0xd8), "{text} is not an ext");
            assert_eq!(dec(&bytes), Value::Decimal(canon.into()), "{text}");
            // The canonical form re-encodes identically, except that a
            // positive exponent is kept as a negative scale (like decNumber
            // does): "12e2" is digits 12 / scale -2, "1200" is 1200 / 0.
            let again = enc(&Value::Decimal(canon.into()));
            if text.contains("e2") {
                assert_eq!(dec(&again), Value::Decimal(canon.into()));
            } else {
                assert_eq!(again, bytes, "{canon}");
            }
        }
        let max = "9".repeat(DECIMAL_MAX_DIGITS);
        assert_eq!(dec(&enc(&Value::Decimal(max.clone()))), Value::Decimal(max));
    }

    #[test]
    fn bad_decimals_fall_back_to_string() {
        for bad in ["", "-", ".", "1.2.3", "abc", "1e", "1e+", "--1", "1,5", &"1".repeat(DECIMAL_MAX_DIGITS + 1)] {
            let mut out = Vec::new();
            assert!(encode_decimal(&mut out, bad).is_err(), "{bad:?}");
            assert!(out.is_empty());
            let mut expect = Vec::new();
            write_str(&mut expect, bad.as_bytes());
            assert_eq!(enc(&Value::Decimal(bad.to_string())), expect);
        }
    }

    #[test]
    fn huge_scales_render_scientific() {
        let mut out = Vec::new();
        encode_decimal(&mut out, "1.25E+2000").unwrap();
        assert_eq!(dec(&out), Value::Decimal("1.25E+2000".into()));
        out.clear();
        encode_decimal(&mut out, "-7E-5000").unwrap();
        assert_eq!(dec(&out), Value::Decimal("-7E-5000".into()));
    }

    #[test]
    fn datetimes_written_by_tarantool() {
        // datetime.new{year=2024, month=2, day=29, hour=13, min=14, sec=15,
        //              nsec=123456789, tzoffset=180}: UTC is 10:14:15.
        let v = dec(&hex("d8 04 f7 58 e0 65 00 00 00 00 15 cd 5b 07 b4 00 00 00"));
        assert_eq!(v, Value::str("2024-02-29 10:14:15.123456"));
        // datetime.new{year=1969, month=12, day=31, hour=23, min=59, sec=59}
        let v = dec(&hex("d7 04 ff ff ff ff ff ff ff ff"));
        assert_eq!(v, Value::str("1969-12-31 23:59:59"));
        let mut b = vec![0xd7, 0x04];
        b.extend_from_slice(&0i64.to_le_bytes());
        assert_eq!(dec(&b), Value::str("1970-01-01 00:00:00"));
    }

    #[test]
    fn civil_dates() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(-1), (1969, 12, 31));
        assert_eq!(civil_from_days(19_782), (2024, 2, 29));
        assert_eq!(civil_from_days(-719_468), (0, 3, 1));
        assert_eq!(civil_from_days(2_932_896), (9999, 12, 31));
    }

    #[test]
    fn integer_encodings_are_minimal() {
        let cases: [(i64, &str); 12] = [
            (0, "00"),
            (127, "7f"),
            (128, "cc 80"),
            (255, "cc ff"),
            (256, "cd 01 00"),
            (65_536, "ce 00 01 00 00"),
            (4_294_967_296, "cf 00 00 00 01 00 00 00 00"),
            (-1, "ff"),
            (-32, "e0"),
            (-33, "d0 df"),
            (-129, "d1 ff 7f"),
            (i64::MIN, "d3 80 00 00 00 00 00 00 00"),
        ];
        for (v, bytes) in cases {
            assert_eq!(enc(&Value::Int(v)), hex(bytes), "{v}");
            assert_eq!(dec(&hex(bytes)), Value::Int(v));
        }
        assert_eq!(enc(&Value::UInt(u64::MAX)), hex("cf ff ff ff ff ff ff ff ff"));
        assert_eq!(dec(&hex("cf ff ff ff ff ff ff ff ff")), Value::UInt(u64::MAX));
        assert_eq!(dec(&hex("cf 7f ff ff ff ff ff ff ff")), Value::Int(i64::MAX));
        assert_eq!(dec(&hex("d0 05")), Value::Int(5));
        assert_eq!(dec(&hex("d2 ff ff ff fe")), Value::Int(-2));
    }

    #[test]
    fn scalars_round_trip() {
        let long = "x".repeat(70_000);
        let vals = vec![
            Value::Null,
            Value::Int(-1_000_000_007),
            Value::UInt(1 << 63),
            Value::Float(-2.5e-300),
            Value::Float(f64::INFINITY),
            Value::Decimal("-12.340".into()),
            Value::str(""),
            Value::str("héllo"),
            Value::str(&"y".repeat(31)),
            Value::str(&"y".repeat(32)),
            Value::str(&"z".repeat(300)),
            Value::str(&long),
            Value::Bin(Bytes::from_static(b"\x00\xff")),
            Value::Bin(Bytes::from(vec![7u8; 300])),
            Value::Bin(Bytes::from(vec![9u8; 70_000])),
        ];
        let mut out = Vec::new();
        encode_tuple(&mut out, &vals);
        let mut p = &out[..];
        assert_eq!(decode_tuple(&mut p).unwrap(), vals);
        assert!(p.is_empty());
        let src = Bytes::from(out);
        assert_eq!(decode_tuple_bytes(&src).unwrap(), vals);
    }

    #[test]
    fn decode_special_types() {
        assert_eq!(dec(&hex("c2")), Value::Int(0));
        assert_eq!(dec(&hex("c3")), Value::Int(1));
        assert_eq!(dec(&hex("ca 3f c0 00 00")), Value::Float(1.5));
        // Invalid UTF-8 in a str becomes Bin.
        assert_eq!(dec(&hex("a2 ff fe")), Value::Bin(Bytes::from_static(b"\xff\xfe")));
        // Arrays, maps and unknown extensions come back as raw MessagePack.
        for raw in ["92 01 a1 61", "81 a1 6b 93 01 02 03", "dc 00 01 c0", "d8 02 f8 1d 4f ae 7d ec 11 d0 a7 65 00 a0 c9 1e 6b f6"] {
            assert_eq!(dec(&hex(raw)), Value::Bin(Bytes::from(hex(raw))), "{raw}");
        }
    }

    #[test]
    fn skip_and_raw() {
        let msg = hex("93 81 a1 61 92 c3 cb 00 00 00 00 00 00 00 00 c7 03 01 00 09 9c d9 03 61 62 63 ff");
        let mut p = &msg[..];
        let raw = read_raw(&mut p).unwrap();
        assert_eq!(raw.len(), msg.len() - 1);
        assert_eq!(p, &[0xff]);
        // Truncated input is an error, never a panic.
        for cut in 0..msg.len() - 1 {
            let mut q = &msg[..cut];
            assert!(skip_value(&mut q).is_err(), "cut at {cut}");
            let mut q = &msg[..cut];
            assert!(decode_value(&mut q).is_err(), "cut at {cut}");
        }
        assert!(skip_value(&mut &[0xc1][..]).is_err());
    }

    #[test]
    fn headers_and_readers() {
        let mut out = Vec::new();
        write_array_len(&mut out, 15);
        write_array_len(&mut out, 16);
        write_array_len(&mut out, 70_000);
        write_map_len(&mut out, 3);
        write_map_len(&mut out, 300);
        write_bool(&mut out, true);
        write_str(&mut out, b"key");
        write_bin(&mut out, b"bin");
        write_f32(&mut out, 0.5);
        write_int(&mut out, 300);
        let mut p = &out[..];
        assert_eq!(read_array_len(&mut p).unwrap(), 15);
        assert_eq!(read_array_len(&mut p).unwrap(), 16);
        assert_eq!(read_array_len(&mut p).unwrap(), 70_000);
        assert_eq!(read_map_len(&mut p).unwrap(), 3);
        assert_eq!(read_map_len(&mut p).unwrap(), 300);
        assert!(read_bool(&mut p).unwrap());
        assert_eq!(read_str(&mut p).unwrap(), b"key");
        assert_eq!(read_bin(&mut p).unwrap(), b"bin");
        assert_eq!(read_f64(&mut p).unwrap(), 0.5);
        assert_eq!(read_uint(&mut p).unwrap(), 300);
        assert!(p.is_empty());
        assert!(read_array_len(&mut &[0x80][..]).is_err());
        assert!(read_uint(&mut &[0xff][..]).is_err());
    }
}
