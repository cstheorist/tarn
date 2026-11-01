//! Small msgpack writer/reader helpers for building IPROTO bodies and Lua
//! call arguments, and for decoding the replies of the Tarn Lua procedures.

use bytes::Bytes;
use tarn_common::{SqlError, SqlResult, Value};
use tarn_iproto::mp::{read_str, read_uint};

#[derive(Default)]
pub struct Mp(pub Vec<u8>);

impl Mp {
    pub fn new() -> Mp {
        Mp(Vec::with_capacity(64))
    }
    pub fn arr(&mut self, n: usize) -> &mut Self {
        rmp::encode::write_array_len(&mut self.0, n as u32).unwrap();
        self
    }
    pub fn map(&mut self, n: usize) -> &mut Self {
        rmp::encode::write_map_len(&mut self.0, n as u32).unwrap();
        self
    }
    pub fn str(&mut self, s: &str) -> &mut Self {
        rmp::encode::write_str(&mut self.0, s).unwrap();
        self
    }
    pub fn uint(&mut self, u: u64) -> &mut Self {
        rmp::encode::write_uint(&mut self.0, u).unwrap();
        self
    }
    pub fn int(&mut self, i: i64) -> &mut Self {
        rmp::encode::write_sint(&mut self.0, i).unwrap();
        self
    }
    pub fn f64(&mut self, f: f64) -> &mut Self {
        rmp::encode::write_f64(&mut self.0, f).unwrap();
        self
    }
    pub fn bool(&mut self, b: bool) -> &mut Self {
        rmp::encode::write_bool(&mut self.0, b).unwrap();
        self
    }
    pub fn nil(&mut self) -> &mut Self {
        rmp::encode::write_nil(&mut self.0).unwrap();
        self
    }
    pub fn val(&mut self, v: &Value) -> &mut Self {
        tarn_iproto::mp::encode_value(&mut self.0, v);
        self
    }
    /// An array of values (a tuple or a key).
    pub fn vals(&mut self, vs: &[Value]) -> &mut Self {
        self.arr(vs.len());
        for v in vs {
            self.val(v);
        }
        self
    }
    pub fn opt_vals(&mut self, vs: &Option<Vec<Value>>) -> &mut Self {
        match vs {
            Some(v) => self.vals(v),
            None => self.nil(),
        }
    }
    pub fn raw(&mut self, b: &[u8]) -> &mut Self {
        self.0.extend_from_slice(b);
        self
    }
    pub fn bytes(self) -> Bytes {
        Bytes::from(self.0)
    }
}

/// Reader over a msgpack buffer.
pub struct Rd<'a> {
    pub buf: &'a [u8],
    /// When set, `buf` is a sub-slice of this buffer and strings are
    /// returned as zero-copy slices of it.
    src: Option<&'a bytes::Bytes>,
}

impl<'a> Rd<'a> {
    pub fn new(buf: &'a [u8]) -> Rd<'a> {
        Rd { buf, src: None }
    }
    /// Continue `rd` (a reader over `src`) with zero-copy string values.
    pub fn shared_at(src: &'a bytes::Bytes, rd: Rd<'a>) -> Rd<'a> {
        Rd {
            buf: rd.buf,
            src: Some(src),
        }
    }
    /// Reader over a whole response buffer with zero-copy string values.
    pub fn shared(src: &'a bytes::Bytes) -> Rd<'a> {
        Rd {
            buf: src,
            src: Some(src),
        }
    }
    pub fn array(&mut self) -> SqlResult<usize> {
        tarn_iproto::mp::read_array_len(&mut self.buf)
            .map(|n| n as usize)
            .map_err(proto)
    }
    pub fn value(&mut self) -> SqlResult<Value> {
        match self.src {
            Some(src) => tarn_iproto::mp::decode_value_bytes(src, &mut self.buf).map_err(proto),
            None => tarn_iproto::mp::decode_value(&mut self.buf).map_err(proto),
        }
    }
    pub fn skip(&mut self) -> SqlResult<()> {
        tarn_iproto::mp::skip_value(&mut self.buf).map_err(proto)
    }
    /// The next value's msgpack encoding, unparsed.
    pub fn raw_value(&mut self) -> SqlResult<&[u8]> {
        let before: &[u8] = self.buf;
        tarn_iproto::mp::skip_value(&mut self.buf).map_err(proto)?;
        let used = before.len() - self.buf.len();
        Ok(&before[..used])
    }
    pub fn string(&mut self) -> SqlResult<String> {
        match self.value()? {
            Value::Str(b) | Value::Bin(b) => Ok(String::from_utf8_lossy(&b).into_owned()),
            Value::Null => Ok(String::new()),
            other => Ok(other.to_string()),
        }
    }
    pub fn is_nil(&self) -> bool {
        self.buf.first() == Some(&0xc0)
    }
    /// The length of the map that comes next.
    pub fn map(&mut self) -> SqlResult<usize> {
        tarn_iproto::mp::read_map_len(&mut self.buf)
            .map(|n| n as usize)
            .map_err(proto)
    }
    /// Whether a map comes next.
    pub fn peek_map(&self) -> Option<()> {
        match self.buf.first() {
            Some(0x80..=0x8f) | Some(0xde) | Some(0xdf) => Some(()),
            _ => None,
        }
    }
    /// Decode a stored tuple of `t` into a row: each column from its field,
    /// or its fill value when the tuple ends before it (a row written before
    /// ALTER TABLE added the column).
    pub fn row_of(&mut self, t: &crate::catalog::TableDef) -> SqlResult<Vec<Value>> {
        let l = &t.layout;
        let mut row = if l.identity {
            self.row(t.columns.len())?
        } else {
            let n = self.array()?;
            let mut row = l.fills.clone();
            for f in 0..n {
                match l.field_col.get(f).copied().flatten() {
                    Some(c) => row[c] = self.value()?,
                    None => self.skip()?,
                }
            }
            row
        };
        // (a FLOAT or DOUBLE that holds an integer, stored as that integer)
        for &c in &l.floats {
            let f = match row[c] {
                Value::Int(i) => i as f64,
                Value::UInt(u) => u as f64,
                _ => continue,
            };
            row[c] = Value::Float(f);
        }
        Ok(row)
    }

    /// Decode a stored tuple: the table's columns followed by bucket_id,
    /// which is dropped.
    pub fn row(&mut self, ncols: usize) -> SqlResult<Vec<Value>> {
        let n = self.array()?;
        let mut row = Vec::with_capacity(ncols);
        for i in 0..n {
            if i < ncols {
                row.push(self.value()?);
            } else {
                self.skip()?;
            }
        }
        while row.len() < ncols {
            row.push(Value::Null);
        }
        Ok(row)
    }
}

pub fn proto(e: impl std::fmt::Display) -> SqlError {
    SqlError::internal(format!("storage protocol error: {}", e))
}

/// Tarantool's duplicate-key message without its dump of both tuples
/// (which shows weight fields as bytes): "Duplicate key exists in unique
/// index "ix" in space "db.t"".
fn duplicate_message(m: &str) -> String {
    match m.find(" with old tuple") {
        Some(i) => m[..i].to_string(),
        None => m.to_string(),
    }
}

/// Map a Tarantool server error to a MySQL error.
///
/// ER_READONLY means the request reached a node that is no longer the shard
/// leader and did nothing; the connection is then stale (see
/// `Client::is_stale`), so the transaction is aborted with a retryable 1213
/// and its retry goes to the new leader.
pub fn from_iproto(e: tarn_iproto::Error) -> SqlError {
    match e {
        tarn_iproto::Error::Admission { scope } => SqlError::new(
            1205,
            "HY000",
            format!("Storage RPC admission refused before enqueue ({scope} limit); this RPC took no effect"),
        ),
        tarn_iproto::Error::Server { code, message } => {
            if code == tarn_iproto::ER_TRANSACTION_CONFLICT {
                SqlError::conflict(message)
            } else if code == tarn_iproto::ER_TUPLE_FOUND {
                SqlError::new(1062, "23000", duplicate_message(&message))
            } else if code == tarn_iproto::errcode::ER_NO_SUCH_INDEX_ID {
                // an index an ALTER TABLE dropped: re-plan with the new catalog
                SqlError::new(1412, "HY000", "Table definition has changed, please retry transaction")
            } else if code == tarn_iproto::errcode::ER_NO_SUCH_SPACE {
                // a table dropped (or recreated) through another server
                SqlError::new(1146, "42S02", format!("Table does not exist ({})", message))
            } else if code == tarn_iproto::ER_READONLY {
                SqlError::conflict(format!("shard leader changed, retry the transaction ({})", message))
            } else {
                SqlError::internal(format!("storage error {}: {}", code, message))
            }
        }
        other => SqlError::new(2013, "HY000", format!("Lost connection to storage: {}", other)),
    }
}

/// A request that may have changed data was in flight when the connection
/// to its shard broke: it may or may not have taken effect. Deliberately not
/// 1213, which clients (and Tarn itself) retry.
pub fn ambiguous(shard: u32, e: &dyn std::fmt::Display) -> SqlError {
    SqlError::new(
        1180,
        "40003",
        format!(
            "Lost connection to shard {} during commit ({}); the statement may or may not have been applied",
            shard, e
        ),
    )
}

/// A read (or a read-only commit) failed because the connection to its
/// shard broke: nothing was written, so the transaction can simply be
/// retried.
pub fn from_iproto_read(e: tarn_iproto::Error) -> SqlError {
    match e {
        e @ (tarn_iproto::Error::Closed | tarn_iproto::Error::Io(_)) => {
            SqlError::conflict(format!("connection to shard lost, retry the transaction ({})", e))
        }
        other => from_iproto(other),
    }
}

/// True if `data` (a CALL reply) is a `{status, ...}` table with this status.
pub fn status_is(data: &[u8], status: &str) -> bool {
    let mut rd = Rd::new(data);
    matches!(rd.array(), Ok(n) if n > 0)
        && matches!(rd.array(), Ok(m) if m > 0)
        && rd.string().is_ok_and(|s| s == status)
}

/// A native mutation may hand back only with a complete, known no-effect
/// envelope. A plausible prefix in a malformed reply is not a certificate.
pub fn native_write_handback(data: &[u8]) -> Option<bool> {
    let mut rd = Rd::new(data);
    if rd.array().ok()? != 1 {
        return None;
    }
    let fields = rd.array().ok()?;
    if !matches!(fields, 1 | 2) || read_str(&mut rd.buf).ok()? != b"lua" {
        return None;
    }
    let all = fields == 2;
    if all && read_str(&mut rd.buf).ok()? != b"all" {
        return None;
    }
    rd.buf.is_empty().then_some(all)
}

/// Decode an autocommit/local/RC write acknowledgement before allowing
/// success, a known rollback, or retry. The transport has already sent the
/// mutation: missing, malformed and future replies remain outcome-unknown.
/// Admin and read procedures use their own envelopes and `lua_status`.
pub fn write_reply(data: &[u8], shard: u32) -> SqlResult<u64> {
    let parsed = (|| -> SqlResult<Option<u64>> {
        let mut rd = Rd::new(data);
        if rd.array()? != 1 {
            return Err(proto("write must return exactly one status"));
        }
        let fields = rd.array()?;
        let tag = read_str(&mut rd.buf).map_err(proto)?;
        let result = match tag {
            b"ok" if matches!(fields, 2 | 3) => {
                let n = read_uint(&mut rd.buf).map_err(proto)?;
                if fields == 3 {
                    if !matches!(rd.buf.first(), Some(0x90..=0x9f | 0xdc | 0xdd | 0xc0)) {
                        return Err(proto("invalid write last tuple"));
                    }
                    rd.skip()?;
                }
                Some(n)
            }
            b"conflict" | b"no" | b"duplicate" | b"error" | b"locked" | b"timeout" | b"deadlock" if fields == 3 => {
                write_error_fields(&mut rd)?;
                None
            }
            b"moved" if fields == 4 => {
                if write_error_fields(&mut rd)? != 1213 {
                    return Err(proto("invalid moved write code"));
                }
                if rd.is_nil() {
                    rd.skip()?;
                } else if !matches!(read_uint(&mut rd.buf).map_err(proto)?, 1..=4294967295) {
                    return Err(proto("invalid moved write owner"));
                }
                None
            }
            b"throttled" if fields == 4 => {
                if write_error_fields(&mut rd)? != 1205 || !matches!(read_uint(&mut rd.buf).map_err(proto)?, 1..=50) {
                    return Err(proto("invalid write admission refusal"));
                }
                None
            }
            _ => return Err(proto("unsupported or incomplete write status")),
        };
        if !rd.buf.is_empty() {
            return Err(proto("trailing write reply data"));
        }
        Ok(result)
    })()
    .map_err(|e| ambiguous(shard, &e))?;
    match parsed {
        Some(n) => Ok(n),
        None => {
            let mut e = lua_status(data).err().expect("validated error status");
            // Native outcome_unknown() uses this numeric code. It must
            // preserve the same SQLSTATE as a lost mutation reply.
            if e.code == 1180 {
                e.sqlstate = *b"40003";
            }
            Err(e)
        }
    }
}

fn write_error_fields(rd: &mut Rd<'_>) -> SqlResult<u16> {
    let code = match read_uint(&mut rd.buf).map_err(proto)? {
        n @ 0..=65535 => n as u16,
        _ => return Err(proto("invalid write error code")),
    };
    // Validate the MessagePack wire type, not Value's SQL coercions: it
    // represents booleans as integers and non-UTF8 strings as binary.
    // Tarantool duplicate messages can contain raw collation-weight bytes.
    read_str(&mut rd.buf).map_err(proto)?;
    Ok(code)
}

/// Whether field `i` (from 0) of the `{status, ...}` table a procedure
/// returned is the string `want`.
pub fn status_arg_is(data: &[u8], i: u32, want: &str) -> bool {
    let mut rd = Rd::new(data);
    if !matches!(rd.array(), Ok(n) if n > 0) {
        return false;
    }
    match rd.array() {
        Ok(m) if m > i as usize => {}
        _ => return false,
    }
    for _ in 0..i {
        if rd.skip().is_err() {
            return false;
        }
    }
    rd.string().is_ok_and(|s| s == want)
}

/// The number of fields of the `{status, ...}` table a Tarn Lua procedure
/// returned (0 if there is none).
pub fn status_len(data: &[u8]) -> usize {
    let mut rd = Rd::new(data);
    match rd.array() {
        Ok(n) if n > 0 => rd.array().unwrap_or(0),
        _ => 0,
    }
}

/// Decode the `{status, ...}` table returned by a Tarn Lua procedure:
/// returns a reader positioned after the status for `ok`/`yes`, or the error.
pub fn lua_status<'a>(data: &'a [u8]) -> SqlResult<(String, Rd<'a>)> {
    let mut rd = Rd::new(data);
    let n = rd.array()?; // CALL returns an array of return values
    if n == 0 {
        return Err(proto("empty procedure reply"));
    }
    let m = rd.array()?; // our procedures return one table
    if m == 0 {
        return Err(proto("empty status table"));
    }
    let status = rd.string()?;
    match status.as_str() {
        "ok" | "yes" | "exists" => Ok((status, rd)),
        "moved" => {
            // {'moved', 1213, message, shard it went to}: see SqlError::moved
            if m > 1 {
                rd.skip()?;
            }
            let msg = if m > 2 { rd.string()? } else { status.clone() };
            Err(SqlError::moved(msg))
        }
        "conflict" | "no" | "duplicate" | "error" | "locked" | "timeout" | "throttled" | "deadlock" => {
            let code = if m > 1 { rd.value()?.as_i64().unwrap_or(0) } else { 0 };
            let msg = if m > 2 { rd.string()? } else { status.clone() };
            Err(match code {
                1213 => SqlError::conflict(msg),
                1062 => SqlError::new(1062, "23000", duplicate_message(&msg)),
                1264 => SqlError::new(1264, "22003", msg),
                1690 => SqlError::new(1690, "22003", msg),
                1146 => SqlError::new(1146, "42S02", msg),
                1050 => SqlError::new(1050, "42S01", msg),
                1846 => SqlError::new(1846, "0A000", msg),
                // Tarantool's errors from a write to a table or an index
                // dropped meanwhile: the same as from_iproto's
                c if c == tarn_iproto::errcode::ER_NO_SUCH_SPACE as i64 => {
                    SqlError::new(1146, "42S02", format!("Table does not exist ({})", msg))
                }
                c if c == tarn_iproto::errcode::ER_NO_SUCH_INDEX_ID as i64 => {
                    SqlError::new(1412, "HY000", "Table definition has changed, please retry transaction")
                }
                0 => SqlError::internal(msg),
                c if c < 1000 => SqlError::internal(format!("storage error {}: {}", c, msg)),
                c => SqlError::new(c as u16, "HY000", msg),
            })
        }
        other => Err(proto(format!("unexpected procedure status '{}'", other))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_outcomes_keep_codes_and_sqlstates() {
        for (tag, code, state) in [
            ("conflict", 1213, *b"40001"),
            ("deadlock", 1213, *b"40001"),
            ("timeout", 1205, *b"HY000"),
            ("duplicate", 1062, *b"23000"),
            ("error", 1412, *b"HY000"),
            ("error", 1180, *b"40003"),
        ] {
            let mut m = Mp::new();
            m.arr(1).arr(3).str(tag).uint(code).str("message");
            let e = write_reply(&m.0, 1).unwrap_err();
            assert_eq!((e.code, e.sqlstate), (code as u16, state));
        }
    }

    #[test]
    fn duplicate_message_can_contain_non_utf8_collation_weights() {
        let mut m = Mp::new();
        m.arr(1).arr(3).str("duplicate").uint(1062);
        let message = b"Duplicate key in index ue with old tuple \xff";
        rmp::encode::write_str_len(&mut m.0, message.len() as u32).unwrap();
        m.raw(message);
        let e = write_reply(&m.0, 1).unwrap_err();
        assert_eq!((e.code, e.sqlstate), (1062, *b"23000"));
        assert_eq!(e.message, "Duplicate key in index ue");
    }

    #[test]
    fn write_envelopes_validate_wire_types_before_sql_value_coercion() {
        let mut messages = Vec::new();
        for b in [false, true] {
            let mut m = Mp::new();
            m.arr(1).arr(2).str("ok").bool(b);
            messages.push(m);
            let mut m = Mp::new();
            m.arr(1).arr(3).str("error").bool(b).str("message");
            messages.push(m);
            let mut m = Mp::new();
            m.arr(1).arr(4).str("moved").uint(1213).str("message").bool(b);
            messages.push(m);
            let mut m = Mp::new();
            m.arr(1).arr(4).str("throttled").uint(1205).str("message").bool(b);
            messages.push(m);
        }
        // SQL Value::Bin also represents MessagePack arrays/maps/extensions;
        // none may substitute for the string error message.
        for raw in [&[0xc0][..], &[0xc4, 1, b'x'], &[0x90], &[0x80], &[0x01]] {
            let mut m = Mp::new();
            m.arr(1).arr(3).str("error").uint(1213).raw(raw);
            messages.push(m);
        }
        for m in messages {
            let e = write_reply(&m.0, 1).unwrap_err();
            assert_eq!((e.code, e.sqlstate), (1180, *b"40003"), "{:x?}", m.0);
        }
    }

    #[test]
    fn incomplete_write_replies_cannot_certify_rollback_or_success() {
        for tag in ["error", "conflict", "no", "moved", "throttled", "lua", "ok", "future"] {
            for outer in [1, 2] {
                for fields in [1, 2, 3, 4] {
                    let mut m = Mp::new();
                    m.arr(outer).arr(fields).str(tag);
                    if fields > 1 {
                        m.uint(1213);
                    }
                    if fields > 2 {
                        m.str("message");
                    }
                    if fields > 3 {
                        m.str("extra");
                    }
                    if outer == 2 {
                        m.nil();
                    }
                    if outer == 1 && fields == 3 && matches!(tag, "error" | "conflict" | "no") {
                        continue;
                    }
                    if outer == 1 && fields == 2 && tag == "ok" {
                        assert_eq!(write_reply(&m.0, 1).unwrap(), 1213);
                        continue;
                    }
                    let e = write_reply(&m.0, 1).unwrap_err();
                    assert_eq!((e.code, e.sqlstate), (1180, *b"40003"), "{tag}/{outer}/{fields}");
                }
            }
        }
        for code in [
            Value::Int(-1),
            Value::UInt(65536 + 1213),
            Value::str("1213"),
            Value::Float(1213.0),
        ] {
            let mut m = Mp::new();
            m.arr(1).arr(3).str("error").val(&code).str("message");
            assert_eq!(write_reply(&m.0, 1).unwrap_err().sqlstate, *b"40003");
        }
    }

    #[test]
    fn only_complete_native_handbacks_authorize_fallback() {
        let mut m = Mp::new();
        m.arr(1).arr(1).str("lua");
        assert_eq!(native_write_handback(&m.0), Some(false));
        m.nil();
        assert_eq!(native_write_handback(&m.0), None);
        let mut m = Mp::new();
        m.arr(1).arr(2).str("lua").str("all");
        assert_eq!(native_write_handback(&m.0), Some(true));
        for second in [Value::str("future"), Value::Null, Value::UInt(0)] {
            let mut m = Mp::new();
            m.arr(1).arr(2).str("lua").val(&second);
            assert_eq!(native_write_handback(&m.0), None);
        }
    }

    #[test]
    fn status_and_its_fields() {
        // [["lua", "all"]], [["lua"]], [["ok", 1]], []
        let all = [0x91, 0x92, 0xa3, b'l', b'u', b'a', 0xa3, b'a', b'l', b'l'];
        let lua = [0x91, 0x91, 0xa3, b'l', b'u', b'a'];
        assert!(status_is(&all, "lua") && status_arg_is(&all, 1, "all"));
        assert!(status_is(&lua, "lua") && !status_arg_is(&lua, 1, "all"));
        assert!(!status_arg_is(&[0x91, 0x92, 0xa2, b'o', b'k', 0x01], 1, "all"));
        assert!(!status_arg_is(&[0x90], 1, "all"));
    }
}
