//! Small msgpack writer/reader helpers for building IPROTO bodies and Lua
//! call arguments, and for decoding the replies of the Tarn Lua procedures.

use bytes::Bytes;
use tarn_common::{SqlError, SqlResult, Value};

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
    /// Decode a stored tuple of `t` into a row: each column from its field,
    /// or its fill value when the tuple ends before it (a row written before
    /// ALTER TABLE added the column).
    pub fn row_of(&mut self, t: &crate::catalog::TableDef) -> SqlResult<Vec<Value>> {
        let l = &t.layout;
        if l.identity {
            return self.row(t.columns.len());
        }
        let n = self.array()?;
        let mut row = l.fills.clone();
        for f in 0..n {
            match l.field_col.get(f).copied().flatten() {
                Some(c) => row[c] = self.value()?,
                None => self.skip()?,
            }
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

/// Map a Tarantool server error to a MySQL error.
///
/// ER_READONLY means the request reached a node that is no longer the shard
/// leader and did nothing; the connection is then stale (see
/// `Client::is_stale`), so the transaction is aborted with a retryable 1213
/// and its retry goes to the new leader.
pub fn from_iproto(e: tarn_iproto::Error) -> SqlError {
    match e {
        tarn_iproto::Error::Server { code, message } => {
            if code == tarn_iproto::ER_TRANSACTION_CONFLICT {
                SqlError::conflict(message)
            } else if code == tarn_iproto::ER_TUPLE_FOUND {
                SqlError::new(1062, "23000", message)
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
        "conflict" | "no" | "duplicate" | "error" | "locked" | "timeout" | "deadlock" => {
            let code = if m > 1 { rd.value()?.as_i64().unwrap_or(0) } else { 0 };
            let msg = if m > 2 { rd.string()? } else { status.clone() };
            Err(match code {
                1213 => SqlError::conflict(msg),
                1062 => SqlError::new(1062, "23000", msg),
                1264 => SqlError::new(1264, "22003", msg),
                1690 => SqlError::new(1690, "22003", msg),
                1146 => SqlError::new(1146, "42S02", msg),
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
