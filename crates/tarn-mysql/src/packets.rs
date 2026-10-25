//! Payload encoders and decoders for the MySQL client/server protocol
//! (protocol 41). Pure functions over byte buffers; no I/O.

use crate::codec::{read_lenenc_int, read_lenenc_str, write_lenenc_int, write_lenenc_str};
use bytes::Bytes;
use sha1::{Digest, Sha1};
use std::collections::HashMap;
use tarn_common::{format_float, Column, ColumnType, SqlError, Value, CHARSET_BINARY};

/// Capability flags (`CLIENT_*`).
pub mod cap {
    pub const LONG_PASSWORD: u32 = 1;
    pub const FOUND_ROWS: u32 = 1 << 1;
    pub const LONG_FLAG: u32 = 1 << 2;
    pub const CONNECT_WITH_DB: u32 = 1 << 3;
    pub const PROTOCOL_41: u32 = 1 << 9;
    pub const INTERACTIVE: u32 = 1 << 10;
    pub const SSL: u32 = 1 << 11;
    pub const TRANSACTIONS: u32 = 1 << 13;
    pub const SECURE_CONNECTION: u32 = 1 << 15;
    pub const MULTI_STATEMENTS: u32 = 1 << 16;
    pub const MULTI_RESULTS: u32 = 1 << 17;
    pub const PS_MULTI_RESULTS: u32 = 1 << 18;
    pub const PLUGIN_AUTH: u32 = 1 << 19;
    pub const CONNECT_ATTRS: u32 = 1 << 20;
    pub const PLUGIN_AUTH_LENENC_CLIENT_DATA: u32 = 1 << 21;
    pub const SESSION_TRACK: u32 = 1 << 23;
    pub const DEPRECATE_EOF: u32 = 1 << 24;
}

/// What the server offers. No SSL, compression, multi-statements, session
/// tracking or found-rows semantics.
pub const SERVER_CAPS: u32 = cap::LONG_PASSWORD
    | cap::LONG_FLAG
    | cap::CONNECT_WITH_DB
    | cap::PROTOCOL_41
    | cap::INTERACTIVE
    | cap::TRANSACTIONS
    | cap::SECURE_CONNECTION
    | cap::MULTI_RESULTS
    | cap::PS_MULTI_RESULTS
    | cap::PLUGIN_AUTH
    | cap::CONNECT_ATTRS
    | cap::PLUGIN_AUTH_LENENC_CLIENT_DATA
    | cap::DEPRECATE_EOF;

pub const SERVER_STATUS_IN_TRANS: u16 = 1;
pub const SERVER_STATUS_AUTOCOMMIT: u16 = 2;

/// utf8mb4_general_ci
pub const DEFAULT_COLLATION: u8 = 45;
pub const NATIVE_PASSWORD: &str = "mysql_native_password";

// ---------------------------------------------------------------------------
// Reading
// ---------------------------------------------------------------------------

/// Bounds-checked cursor over a payload.
pub struct Reader<'a> {
    pub buf: &'a [u8],
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Reader<'a> {
        Reader { buf }
    }
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }
    pub fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        if self.buf.len() < n {
            return None;
        }
        let (a, b) = self.buf.split_at(n);
        self.buf = b;
        Some(a)
    }
    pub fn u8(&mut self) -> Option<u8> {
        self.take(1).map(|b| b[0])
    }
    pub fn u16(&mut self) -> Option<u16> {
        self.take(2).map(|b| u16::from_le_bytes([b[0], b[1]]))
    }
    pub fn u32(&mut self) -> Option<u32> {
        self.take(4).map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }
    pub fn u64(&mut self) -> Option<u64> {
        self.take(8).map(|b| u64::from_le_bytes(b.try_into().unwrap()))
    }
    /// NUL-terminated string (the NUL is consumed); the rest of the buffer if
    /// there is no NUL (some clients omit the final terminator).
    pub fn cstr(&mut self) -> &'a [u8] {
        match self.buf.iter().position(|&b| b == 0) {
            Some(i) => {
                let s = &self.buf[..i];
                self.buf = &self.buf[i + 1..];
                s
            }
            None => std::mem::take(&mut self.buf),
        }
    }
    pub fn lenenc_int(&mut self) -> Option<u64> {
        let (v, n) = read_lenenc_int(self.buf)?;
        self.buf = &self.buf[n..];
        Some(v)
    }
    pub fn lenenc_str(&mut self) -> Option<&'a [u8]> {
        let (s, n) = read_lenenc_str(self.buf)?;
        self.buf = &self.buf[n..];
        Some(s)
    }
    pub fn rest(&mut self) -> &'a [u8] {
        std::mem::take(&mut self.buf)
    }
}

// ---------------------------------------------------------------------------
// Connection phase
// ---------------------------------------------------------------------------

/// Protocol::HandshakeV10, advertising mysql_native_password.
pub fn handshake_v10(out: &mut Vec<u8>, server_version: &str, conn_id: u32, scramble: &[u8; 20], status: u16) {
    out.push(10);
    out.extend_from_slice(server_version.as_bytes());
    out.push(0);
    out.extend_from_slice(&conn_id.to_le_bytes());
    out.extend_from_slice(&scramble[..8]);
    out.push(0);
    out.extend_from_slice(&(SERVER_CAPS as u16).to_le_bytes());
    out.push(DEFAULT_COLLATION);
    out.extend_from_slice(&status.to_le_bytes());
    out.extend_from_slice(&((SERVER_CAPS >> 16) as u16).to_le_bytes());
    out.push(21);
    out.extend_from_slice(&[0u8; 10]);
    out.extend_from_slice(&scramble[8..]);
    out.push(0);
    out.extend_from_slice(NATIVE_PASSWORD.as_bytes());
    out.push(0);
}

#[derive(Debug)]
pub struct HandshakeResponse<'a> {
    pub caps: u32,
    pub max_packet: u32,
    pub collation: u8,
    pub user: &'a [u8],
    pub auth: &'a [u8],
    pub database: Option<&'a [u8]>,
    pub plugin: Option<&'a [u8]>,
}

/// Protocol::HandshakeResponse41. None if malformed or pre-4.1.
pub fn parse_handshake_response(p: &[u8]) -> Option<HandshakeResponse<'_>> {
    let mut r = Reader::new(p);
    let caps = r.u32()?;
    if caps & cap::PROTOCOL_41 == 0 {
        return None;
    }
    let max_packet = r.u32()?;
    let collation = r.u8()?;
    r.take(23)?;
    let user = r.cstr();
    let auth = if caps & cap::PLUGIN_AUTH_LENENC_CLIENT_DATA != 0 {
        r.lenenc_str()?
    } else if caps & cap::SECURE_CONNECTION != 0 {
        let n = r.u8()? as usize;
        r.take(n)?
    } else {
        r.cstr()
    };
    let database = if caps & cap::CONNECT_WITH_DB != 0 && !r.is_empty() {
        Some(r.cstr())
    } else {
        None
    };
    let plugin = if caps & cap::PLUGIN_AUTH != 0 && !r.is_empty() {
        Some(r.cstr())
    } else {
        None
    };
    // CLIENT_CONNECT_ATTRS: lenenc-length block of key/value pairs, ignored.
    Some(HandshakeResponse {
        caps,
        max_packet,
        collation,
        user,
        auth,
        database,
        plugin,
    })
}

/// Protocol::AuthSwitchRequest to mysql_native_password.
pub fn auth_switch(out: &mut Vec<u8>, scramble: &[u8; 20]) {
    out.push(0xFE);
    out.extend_from_slice(NATIVE_PASSWORD.as_bytes());
    out.push(0);
    out.extend_from_slice(scramble);
    out.push(0);
}

fn sha1(parts: &[&[u8]]) -> [u8; 20] {
    let mut h = Sha1::new();
    for p in parts {
        h.update(p);
    }
    h.finalize().into()
}

/// The client's mysql_native_password response for `password`.
pub fn native_password_scramble(password: &[u8], scramble: &[u8]) -> Vec<u8> {
    if password.is_empty() {
        return Vec::new();
    }
    let stage1 = sha1(&[password]);
    let stage2 = sha1(&[&stage1]);
    let mix = sha1(&[scramble, &stage2]);
    stage1.iter().zip(mix.iter()).map(|(a, b)| a ^ b).collect()
}

/// Verify a mysql_native_password response:
/// response = SHA1(pw) XOR SHA1(scramble + SHA1(SHA1(pw))).
pub fn native_password_ok(password: &str, scramble: &[u8; 20], response: &[u8]) -> bool {
    if password.is_empty() {
        return response.is_empty();
    }
    if response.len() != 20 {
        return false;
    }
    let stage1 = sha1(&[password.as_bytes()]);
    let stage2 = sha1(&[&stage1]);
    let mix = sha1(&[scramble, &stage2]);
    let mut candidate = [0u8; 20];
    for i in 0..20 {
        candidate[i] = mix[i] ^ response[i];
    }
    // constant-time comparison of SHA1(candidate) with stage2
    let check = sha1(&[&candidate]);
    check.iter().zip(stage2.iter()).fold(0u8, |acc, (a, b)| acc | (a ^ b)) == 0
}

// ---------------------------------------------------------------------------
// Generic responses
// ---------------------------------------------------------------------------

/// OK packet (header 0x00), or the OK that replaces EOF (header 0xFE) when
/// CLIENT_DEPRECATE_EOF is in effect.
pub fn ok(out: &mut Vec<u8>, header: u8, affected: u64, last_insert_id: u64, status: u16, warnings: u16, info: &str) {
    out.push(header);
    write_lenenc_int(out, affected);
    write_lenenc_int(out, last_insert_id);
    out.extend_from_slice(&status.to_le_bytes());
    out.extend_from_slice(&warnings.to_le_bytes());
    out.extend_from_slice(info.as_bytes());
}

pub fn eof(out: &mut Vec<u8>, status: u16) {
    out.push(0xFE);
    out.extend_from_slice(&0u16.to_le_bytes());
    out.extend_from_slice(&status.to_le_bytes());
}

pub fn err(out: &mut Vec<u8>, e: &SqlError) {
    out.push(0xFF);
    out.extend_from_slice(&e.code.to_le_bytes());
    out.push(b'#');
    out.extend_from_slice(&e.sqlstate);
    out.extend_from_slice(e.message.as_bytes());
}

/// Protocol::ColumnDefinition41.
pub fn column_def(out: &mut Vec<u8>, c: &Column) {
    write_lenenc_str(out, b"def");
    write_lenenc_str(out, c.schema.as_bytes());
    write_lenenc_str(out, c.table.as_bytes());
    write_lenenc_str(out, c.table.as_bytes());
    write_lenenc_str(out, c.name.as_bytes());
    write_lenenc_str(out, c.name.as_bytes());
    out.push(0x0c);
    out.extend_from_slice(&c.charset.to_le_bytes());
    out.extend_from_slice(&c.length.to_le_bytes());
    out.push(c.ty as u8);
    out.extend_from_slice(&c.flags.to_le_bytes());
    out.push(c.decimals);
    out.extend_from_slice(&[0, 0]);
}

/// Definition sent for each `?` of a prepared statement.
pub fn param_def(out: &mut Vec<u8>) {
    let c = Column {
        schema: String::new(),
        table: String::new(),
        name: "?".to_string(),
        ty: ColumnType::VarString,
        flags: 0,
        charset: CHARSET_BINARY,
        length: 0,
        decimals: 0,
    };
    column_def(out, &c)
}

/// COM_STMT_PREPARE_OK.
pub fn stmt_prepare_ok(out: &mut Vec<u8>, id: u32, ncols: u16, nparams: u16) {
    out.push(0);
    out.extend_from_slice(&id.to_le_bytes());
    out.extend_from_slice(&ncols.to_le_bytes());
    out.extend_from_slice(&nparams.to_le_bytes());
    out.push(0);
    out.extend_from_slice(&0u16.to_le_bytes());
}

// ---------------------------------------------------------------------------
// Rows
// ---------------------------------------------------------------------------

fn write_int_text(out: &mut Vec<u8>, neg: bool, mut u: u64) {
    let mut buf = [0u8; 21];
    let mut i = buf.len();
    loop {
        i -= 1;
        buf[i] = b'0' + (u % 10) as u8;
        u /= 10;
        if u == 0 {
            break;
        }
    }
    if neg {
        i -= 1;
        buf[i] = b'-';
    }
    write_lenenc_str(out, &buf[i..]);
}

fn write_text_value(out: &mut Vec<u8>, v: &Value) {
    match v {
        Value::Null => out.push(0xFB),
        Value::Int(i) => write_int_text(out, *i < 0, i.unsigned_abs()),
        Value::UInt(u) => write_int_text(out, false, *u),
        Value::Float(f) => write_lenenc_str(out, format_float(*f).as_bytes()),
        Value::Decimal(d) => write_lenenc_str(out, d.as_bytes()),
        Value::Str(b) | Value::Bin(b) => write_lenenc_str(out, b),
    }
}

/// Text-protocol row (COM_QUERY).
pub fn text_row(out: &mut Vec<u8>, ncols: usize, row: &[Value]) {
    for i in 0..ncols {
        write_text_value(out, row.get(i).unwrap_or(&Value::Null));
    }
}

fn as_i64(v: &Value) -> i64 {
    match v {
        Value::Int(i) => *i,
        Value::UInt(u) => *u as i64,
        Value::Float(f) => f.round() as i64,
        Value::Decimal(d) => d.trim().parse::<f64>().map(|f| f.round() as i64).unwrap_or(0),
        Value::Str(b) | Value::Bin(b) => {
            let s = std::str::from_utf8(b).unwrap_or("").trim();
            s.parse::<i64>()
                .ok()
                .or_else(|| s.parse::<f64>().ok().map(|f| f.round() as i64))
                .unwrap_or(0)
        }
        Value::Null => 0,
    }
}

fn as_f64(v: &Value) -> f64 {
    v.as_f64().unwrap_or(0.0)
}

fn text_of(v: &Value) -> Bytes {
    v.to_text().unwrap_or_default()
}

fn digits(s: &[u8]) -> Option<u32> {
    if s.is_empty() || !s.iter().all(u8::is_ascii_digit) {
        return None;
    }
    std::str::from_utf8(s).ok()?.parse().ok()
}

/// Parse "YYYY-MM-DD[ HH:MM:SS[.ffffff]]".
pub fn parse_datetime(s: &[u8]) -> Option<(u16, u8, u8, u8, u8, u8, u32)> {
    let s = std::str::from_utf8(s).ok()?.trim();
    let (date, time) = match s.split_once([' ', 'T']) {
        Some((d, t)) => (d, Some(t)),
        None => (s, None),
    };
    let mut dp = date.split('-');
    let y = digits(dp.next()?.as_bytes())?;
    let mo = digits(dp.next()?.as_bytes())?;
    let d = digits(dp.next()?.as_bytes())?;
    if dp.next().is_some() || y > 9999 || mo > 12 || d > 31 {
        return None;
    }
    let (mut h, mut mi, mut se, mut us) = (0, 0, 0, 0);
    if let Some(t) = time {
        let (hms, frac) = match t.split_once('.') {
            Some((a, b)) => (a, Some(b)),
            None => (t, None),
        };
        let mut tp = hms.split(':');
        h = digits(tp.next()?.as_bytes())?;
        mi = digits(tp.next()?.as_bytes())?;
        se = digits(tp.next().unwrap_or("0").as_bytes())?;
        if h > 23 || mi > 59 || se > 59 {
            return None;
        }
        if let Some(f) = frac {
            let f = &f.as_bytes()[..f.len().min(6)];
            let mut v = digits(f)?;
            for _ in f.len()..6 {
                v *= 10;
            }
            us = v;
        }
    }
    Some((y as u16, mo as u8, d as u8, h as u8, mi as u8, se as u8, us))
}

/// Parse "[-][D ]HHH:MM:SS[.ffffff]" into (negative, days, h, m, s, us).
pub fn parse_time(s: &[u8]) -> Option<(bool, u32, u8, u8, u8, u32)> {
    let s = std::str::from_utf8(s).ok()?.trim();
    let (neg, s) = match s.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, s),
    };
    let (days, s) = match s.split_once(' ') {
        Some((d, r)) => (digits(d.as_bytes())?, r),
        None => (0, s),
    };
    let (hms, frac) = match s.split_once('.') {
        Some((a, b)) => (a, Some(b)),
        None => (s, None),
    };
    let mut tp = hms.split(':');
    let h = digits(tp.next()?.as_bytes())?;
    let mi = digits(tp.next().unwrap_or("0").as_bytes())?;
    let se = digits(tp.next().unwrap_or("0").as_bytes())?;
    let total_h = days * 24 + h;
    let mut us = 0;
    if let Some(f) = frac {
        let f = &f.as_bytes()[..f.len().min(6)];
        let mut v = digits(f)?;
        for _ in f.len()..6 {
            v *= 10;
        }
        us = v;
    }
    Some((neg, total_h / 24, (total_h % 24) as u8, mi as u8, se as u8, us))
}

fn write_binary_datetime(out: &mut Vec<u8>, s: &[u8]) {
    match parse_datetime(s) {
        Some((y, mo, d, h, mi, se, us)) => {
            let len = if us != 0 {
                11
            } else if h != 0 || mi != 0 || se != 0 {
                7
            } else if y != 0 || mo != 0 || d != 0 {
                4
            } else {
                0
            };
            out.push(len);
            if len >= 4 {
                out.extend_from_slice(&y.to_le_bytes());
                out.push(mo);
                out.push(d);
            }
            if len >= 7 {
                out.extend_from_slice(&[h, mi, se]);
            }
            if len == 11 {
                out.extend_from_slice(&us.to_le_bytes());
            }
        }
        None => out.push(0),
    }
}

fn write_binary_time(out: &mut Vec<u8>, s: &[u8]) {
    match parse_time(s) {
        Some((neg, days, h, mi, se, us)) => {
            if !neg && days == 0 && h == 0 && mi == 0 && se == 0 && us == 0 {
                out.push(0);
                return;
            }
            out.push(if us != 0 { 12 } else { 8 });
            out.push(neg as u8);
            out.extend_from_slice(&days.to_le_bytes());
            out.extend_from_slice(&[h, mi, se]);
            if us != 0 {
                out.extend_from_slice(&us.to_le_bytes());
            }
        }
        None => out.push(0),
    }
}

/// One non-NULL value in the binary protocol, coerced to the column type.
pub fn write_binary_value(out: &mut Vec<u8>, c: &Column, v: &Value) {
    use ColumnType as T;
    match c.ty {
        T::Tiny => out.push(as_i64(v) as u8),
        T::Short | T::Year => out.extend_from_slice(&(as_i64(v) as u16).to_le_bytes()),
        T::Long | T::Int24 => out.extend_from_slice(&(as_i64(v) as u32).to_le_bytes()),
        T::LongLong => {
            let bits = match v {
                Value::UInt(u) => *u,
                other => as_i64(other) as u64,
            };
            out.extend_from_slice(&bits.to_le_bytes())
        }
        T::Float => out.extend_from_slice(&(as_f64(v) as f32).to_le_bytes()),
        T::Double => out.extend_from_slice(&as_f64(v).to_le_bytes()),
        T::Date | T::DateTime | T::Timestamp => write_binary_datetime(out, &text_of(v)),
        T::Time => write_binary_time(out, &text_of(v)),
        T::Null => {}
        _ => write_lenenc_str(out, &text_of(v)),
    }
}

/// Binary-protocol row (COM_STMT_EXECUTE): 0x00, NULL bitmap (offset 2),
/// then the non-NULL values.
pub fn binary_row(out: &mut Vec<u8>, cols: &[Column], row: &[Value]) {
    out.push(0x00);
    let start = out.len();
    out.resize(start + (cols.len() + 9) / 8, 0);
    for (i, c) in cols.iter().enumerate() {
        match row.get(i) {
            None | Some(Value::Null) => {
                let bit = i + 2;
                out[start + bit / 8] |= 1 << (bit % 8);
            }
            Some(_) if c.ty == ColumnType::Null => {
                let bit = i + 2;
                out[start + bit / 8] |= 1 << (bit % 8);
            }
            Some(v) => write_binary_value(out, c, v),
        }
    }
}

// ---------------------------------------------------------------------------
// Prepared statement parameters
// ---------------------------------------------------------------------------

fn string_value(t: u8, b: &[u8]) -> Value {
    match t {
        // TINY/MEDIUM/LONG/BLOB, GEOMETRY, BIT: binary
        249..=252 | 255 | 16 => Value::Bin(Bytes::copy_from_slice(b)),
        _ => match std::str::from_utf8(b) {
            Ok(_) => Value::Str(Bytes::copy_from_slice(b)),
            Err(_) => Value::Bin(Bytes::copy_from_slice(b)),
        },
    }
}

/// A parameter supplied with COM_STMT_SEND_LONG_DATA.
pub fn long_data_value(t: u8, data: Vec<u8>) -> Value {
    match t {
        249..=252 | 255 | 16 => Value::Bin(Bytes::from(data)),
        _ => match String::from_utf8(data) {
            Ok(s) => Value::Str(Bytes::from(s)),
            Err(e) => Value::Bin(Bytes::from(e.into_bytes())),
        },
    }
}

fn decode_date(r: &mut Reader, t: u8) -> Option<Value> {
    let len = r.u8()? as usize;
    let d = r.take(len)?;
    let (y, mo, da) = if len >= 4 {
        (u16::from_le_bytes([d[0], d[1]]), d[2], d[3])
    } else {
        (0, 0, 0)
    };
    let (h, mi, s) = if len >= 7 { (d[4], d[5], d[6]) } else { (0, 0, 0) };
    let us = if len >= 11 {
        u32::from_le_bytes([d[7], d[8], d[9], d[10]])
    } else {
        0
    };
    let text = if t == ColumnType::Date as u8 {
        format!("{:04}-{:02}-{:02}", y, mo, da)
    } else if us != 0 {
        format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}", y, mo, da, h, mi, s, us)
    } else {
        format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}", y, mo, da, h, mi, s)
    };
    Some(Value::Str(Bytes::from(text)))
}

fn decode_time(r: &mut Reader) -> Option<Value> {
    let len = r.u8()? as usize;
    let d = r.take(len)?;
    if len < 8 {
        return Some(Value::str("00:00:00"));
    }
    let neg = d[0] != 0;
    let days = u32::from_le_bytes([d[1], d[2], d[3], d[4]]);
    let hours = days as u64 * 24 + d[5] as u64;
    let us = if len >= 12 {
        u32::from_le_bytes([d[8], d[9], d[10], d[11]])
    } else {
        0
    };
    let mut s = format!("{}{:02}:{:02}:{:02}", if neg { "-" } else { "" }, hours, d[6], d[7]);
    if us != 0 {
        s.push_str(&format!(".{:06}", us));
    }
    Some(Value::Str(Bytes::from(s)))
}

/// Decode one binary-protocol parameter of MySQL type `t`.
pub fn decode_param(r: &mut Reader, t: u8, unsigned: bool) -> Option<Value> {
    Some(match t {
        1 => {
            let b = r.u8()?;
            Value::Int(if unsigned { b as i64 } else { b as i8 as i64 })
        }
        2 | 13 => {
            let v = r.u16()?;
            Value::Int(if unsigned { v as i64 } else { v as i16 as i64 })
        }
        3 | 9 => {
            let v = r.u32()?;
            Value::Int(if unsigned { v as i64 } else { v as i32 as i64 })
        }
        8 => {
            let v = r.u64()?;
            if unsigned && v > i64::MAX as u64 {
                Value::UInt(v)
            } else {
                Value::Int(v as i64)
            }
        }
        4 => Value::Float(f32::from_le_bytes(r.take(4)?.try_into().unwrap()) as f64),
        5 => Value::Float(f64::from_le_bytes(r.take(8)?.try_into().unwrap())),
        0 | 246 => {
            let s = r.lenenc_str()?;
            Value::Decimal(String::from_utf8_lossy(s).trim().to_string())
        }
        7 | 10 | 12 => decode_date(r, t)?,
        11 => decode_time(r)?,
        6 => Value::Null,
        _ => {
            let s = r.lenenc_str()?;
            string_value(t, s)
        }
    })
}

/// Decode the parameter block of COM_STMT_EXECUTE (after stmt id, flags and
/// iteration count). `types` keeps the types bound by an earlier execute
/// (new-params-bound-flag = 0 reuses them); `long_data` holds parameters sent
/// with COM_STMT_SEND_LONG_DATA, which are absent from the value list.
pub fn decode_params(
    r: &mut Reader,
    n: usize,
    types: &mut Vec<(u8, bool)>,
    long_data: &mut HashMap<u16, Vec<u8>>,
) -> Option<Vec<Value>> {
    if n == 0 {
        return Some(Vec::new());
    }
    let bitmap = r.take(n.div_ceil(8))?;
    let bound = r.u8()?;
    if bound == 1 {
        types.clear();
        for _ in 0..n {
            let t = r.u8()?;
            let flags = r.u8()?;
            types.push((t, flags & 0x80 != 0));
        }
    } else if types.len() != n {
        return None;
    }
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        if bitmap[i / 8] & (1 << (i % 8)) != 0 {
            out.push(Value::Null);
            continue;
        }
        let (t, unsigned) = types[i];
        if let Some(data) = long_data.remove(&(i as u16)) {
            out.push(long_data_value(t, data));
            continue;
        }
        out.push(decode_param(r, t, unsigned)?);
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn native_password_round_trip() {
        let scramble: [u8; 20] = *b"abcdefghij0123456789";
        let resp = native_password_scramble(b"secret", &scramble);
        assert!(native_password_ok("secret", &scramble, &resp));
        assert!(!native_password_ok("other", &scramble, &resp));
        assert!(native_password_ok("", &scramble, &[]));
        assert!(!native_password_ok("", &scramble, &resp));
        assert!(!native_password_ok("secret", &scramble, &[]));
    }

    #[test]
    fn handshake_response_parsing() {
        let mut p = Vec::new();
        let caps = cap::PROTOCOL_41
            | cap::SECURE_CONNECTION
            | cap::PLUGIN_AUTH
            | cap::CONNECT_WITH_DB
            | cap::PLUGIN_AUTH_LENENC_CLIENT_DATA
            | cap::CONNECT_ATTRS;
        p.extend_from_slice(&caps.to_le_bytes());
        p.extend_from_slice(&(1u32 << 24).to_le_bytes());
        p.push(45);
        p.extend_from_slice(&[0; 23]);
        p.extend_from_slice(b"root\0");
        write_lenenc_str(&mut p, &[1, 2, 3]);
        p.extend_from_slice(b"db1\0caching_sha2_password\0");
        write_lenenc_str(&mut p, b"\x04_pid\x0212");
        let h = parse_handshake_response(&p).unwrap();
        assert_eq!(h.user, b"root");
        assert_eq!(h.auth, &[1, 2, 3]);
        assert_eq!(h.database, Some(&b"db1"[..]));
        assert_eq!(h.plugin, Some(&b"caching_sha2_password"[..]));
        assert!(parse_handshake_response(&p[..20]).is_none());
    }

    #[test]
    fn params_all_types() {
        let mut p = Vec::new();
        let n = 9;
        p.extend_from_slice(&[0b0100_0000, 0]); // param 6 is NULL
        p.push(1);
        for (t, f) in [
            (1u8, 0u8),
            (2, 0x80),
            (3, 0),
            (8, 0x80),
            (5, 0),
            (253, 0),
            (3, 0),
            (12, 0),
            (246, 0),
        ] {
            p.extend_from_slice(&[t, f]);
        }
        p.push(0xFF); // TINY -1
        p.extend_from_slice(&65535u16.to_le_bytes()); // unsigned SHORT
        p.extend_from_slice(&(-7i32).to_le_bytes());
        p.extend_from_slice(&u64::MAX.to_le_bytes()); // unsigned LONGLONG
        p.extend_from_slice(&2.5f64.to_le_bytes());
        write_lenenc_str(&mut p, b"hello");
        // param 6 NULL: no bytes
        p.extend_from_slice(&[7, 0xE8, 0x07, 1, 2, 3, 4, 5]); // 2024-01-02 03:04:05
        write_lenenc_str(&mut p, b"-12.340");
        let mut types = Vec::new();
        let mut ld = HashMap::new();
        let v = decode_params(&mut Reader::new(&p), n, &mut types, &mut ld).unwrap();
        assert_eq!(
            v,
            vec![
                Value::Int(-1),
                Value::Int(65535),
                Value::Int(-7),
                Value::UInt(u64::MAX),
                Value::Float(2.5),
                Value::str("hello"),
                Value::Null,
                Value::str("2024-01-02 03:04:05"),
                Value::Decimal("-12.340".into()),
            ]
        );
        // re-execute without rebinding types
        let mut p2 = vec![0xFF, 0x01, 0];
        p2.push(0);
        let v2 = decode_params(&mut Reader::new(&p2[..]), n, &mut types, &mut ld);
        assert!(v2.is_some());
        // truncated input never panics
        for cut in 0..p.len() {
            let mut t = Vec::new();
            let _ = decode_params(&mut Reader::new(&p[..cut]), n, &mut t, &mut ld);
        }
    }

    #[test]
    fn binary_rows() {
        let cols: Arc<[Column]> = vec![
            Column::new("a", ColumnType::LongLong),
            Column::new("b", ColumnType::VarString),
            Column::new("c", ColumnType::Double),
            Column::new("d", ColumnType::DateTime),
            Column::new("e", ColumnType::Long),
        ]
        .into();
        let mut out = Vec::new();
        binary_row(
            &mut out,
            &cols,
            &[
                Value::Int(5),
                Value::Null,
                Value::Int(2),
                Value::str("2024-01-02 03:04:05"),
                Value::str("12"),
            ],
        );
        assert_eq!(out[0], 0);
        assert_eq!(out[1], 0b0000_1000); // (5 + 9) / 8 = 1 bitmap byte; column 1 (bit 3) is NULL
        assert_eq!(&out[2..10], &5i64.to_le_bytes());
        assert_eq!(&out[10..18], &2.0f64.to_le_bytes());
        assert_eq!(&out[18..26], &[7, 0xE8, 0x07, 1, 2, 3, 4, 5]);
        assert_eq!(&out[26..30], &12u32.to_le_bytes());
        assert_eq!(out.len(), 30);
    }

    #[test]
    fn text_rows() {
        let mut out = Vec::new();
        text_row(
            &mut out,
            4,
            &[Value::Int(-42), Value::Null, Value::UInt(7), Value::Float(1.5)],
        );
        assert_eq!(out, b"\x03-42\xfb\x017\x031.5");
    }

    #[test]
    fn datetime_parsing() {
        assert_eq!(parse_datetime(b"2024-01-02"), Some((2024, 1, 2, 0, 0, 0, 0)));
        assert_eq!(
            parse_datetime(b"2024-01-02 10:20:30.5"),
            Some((2024, 1, 2, 10, 20, 30, 500000))
        );
        assert_eq!(parse_datetime(b"garbage"), None);
        assert_eq!(parse_time(b"-25:00:01"), Some((true, 1, 1, 0, 1, 0)));
    }
}
