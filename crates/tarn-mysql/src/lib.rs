//! Server side of the MySQL client/server protocol.
//!
//! One `serve_connection` future per client connection drives the
//! handshake, authentication and command loop, and calls a [`Backend`]
//! (one instance per connection) for everything SQL. Reads go through a
//! growable buffer; responses are built into a write buffer that is flushed
//! once per batch of pipelined commands (just before the next read would
//! block), so a typical request/response costs one read and one write
//! syscall. Length-encoded integers/strings and packet framing go through
//! [`codec`], whose implementations are formally verified with Verus.
//!
//! Supported: HandshakeV10 with mysql_native_password (auth switch from any
//! other client plugin), CLIENT_DEPRECATE_EOF negotiated per client, text
//! and binary result sets, COM_QUERY, COM_INIT_DB, COM_PING, COM_QUIT,
//! COM_FIELD_LIST, COM_STATISTICS, COM_STMT_PREPARE / EXECUTE /
//! SEND_LONG_DATA / CLOSE / RESET, COM_SET_OPTION, COM_RESET_CONNECTION,
//! multi-statements (CLIENT_MULTI_STATEMENTS or COM_SET_OPTION).
//! Not supported: SSL, compression, cursors (a cursor request is answered
//! with an ordinary result set), COM_CHANGE_USER.

pub mod codec;
pub mod packets;

use bytes::{Buf, Bytes, BytesMut};
use packets::{cap, Reader};
use rand::Rng;
use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::sync::Arc;
use tarn_common::{Column, SqlError, SqlResult, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub use packets::{SERVER_STATUS_AUTOCOMMIT, SERVER_STATUS_IN_TRANS};

/// Largest logical packet accepted from a client (MySQL's default
/// max_allowed_packet); also the cap on long data per prepared statement.
pub const MAX_ALLOWED_PACKET: usize = 64 << 20;
/// Largest packet accepted before authentication (a handshake response is
/// a few hundred bytes).
pub const MAX_PREAUTH_PACKET: usize = 64 << 10;
/// Prepared statements per connection (MySQL's max_prepared_stmt_count
/// default, which MySQL applies server-wide).
pub const MAX_PREPARED_STMTS: usize = 16382;

#[derive(Clone, Debug)]
pub struct ServerOpts {
    /// Shared admission for retained socket buffers and prepared long data.
    pub memory: Option<Arc<dyn tarn_common::ResourceMemory>>,
    /// Version string in the handshake, e.g. "8.0.40-tarn".
    pub server_version: String,
    pub auth: AuthConfig,
}

impl Default for ServerOpts {
    fn default() -> Self {
        ServerOpts {
            memory: None,
            server_version: "8.0.40-tarn".to_string(),
            auth: AuthConfig::AllowAll,
        }
    }
}

#[derive(Clone, Debug)]
pub enum AuthConfig {
    /// Accept any user and password.
    AllowAll,
    /// user -> plaintext password (verified with mysql_native_password).
    Users(HashMap<String, String>),
}

#[derive(Clone, Debug)]
pub enum QueryResult {
    Ok {
        affected_rows: u64,
        last_insert_id: u64,
        info: String,
        warnings: u16,
    },
    Rows {
        columns: Arc<[Column]>,
        rows: Vec<Vec<Value>>,
        resource_guard: Option<tarn_common::ResourceGuard>,
    },
}

#[derive(Clone, Debug)]
pub struct PrepareInfo {
    pub num_params: u16,
    /// Result columns; empty if the statement returns no rows.
    pub columns: Arc<[Column]>,
}

/// SQL engine behind one client connection.
pub trait Backend: Send {
    /// `found_rows`: the client set CLIENT_FOUND_ROWS (UPDATE reports the
    /// rows it matched, not those it changed). `collation`: the character
    /// set and collation id the client asked for in its handshake response.
    fn on_connect(
        &mut self,
        user: &str,
        database: Option<&str>,
        found_rows: bool,
        collation: u8,
    ) -> impl Future<Output = SqlResult<()>> + Send;
    fn on_query(&mut self, sql: &str) -> impl Future<Output = SqlResult<QueryResult>> + Send;
    fn on_prepare(&mut self, stmt_id: u32, sql: &str) -> impl Future<Output = SqlResult<PrepareInfo>> + Send;
    fn on_execute(&mut self, stmt_id: u32, params: &[Value]) -> impl Future<Output = SqlResult<QueryResult>> + Send;
    fn on_close_stmt(&mut self, stmt_id: u32) -> impl Future<Output = ()> + Send;
    fn on_init_db(&mut self, db: &str) -> impl Future<Output = SqlResult<()>> + Send;
    /// SERVER_STATUS_IN_TRANS / SERVER_STATUS_AUTOCOMMIT bits.
    fn status_flags(&self) -> u16;
    fn on_disconnect(&mut self) -> impl Future<Output = ()> + Send;
}

struct Stmt {
    num_params: u16,
    types: Vec<(u8, bool)>,
    long_data: HashMap<u16, Vec<u8>>,
    /// Bytes in `long_data`; past MAX_ALLOWED_PACKET the data is dropped and
    /// the next EXECUTE fails.
    long_bytes: usize,
    long_overflow: bool,
    long_error: Option<SqlError>,
    long_charge: u64,
    long_guards: Vec<tarn_common::ResourceGuard>,
}

struct Conn<B> {
    stream: TcpStream,
    rbuf: BytesMut,
    wbuf: Vec<u8>,
    result_guards: Vec<tarn_common::ResourceGuard>,
    buffer_charge: u64,
    buffer_guards: Vec<tarn_common::ResourceGuard>,
    scratch: Vec<u8>,
    seq: u8,
    caps: u32,
    id: u32,
    opts: Arc<ServerOpts>,
    backend: B,
    stmts: HashMap<u32, Stmt>,
    next_stmt: u32,
    user: String,
    db: Option<String>,
    /// Collation id from the handshake response.
    collation: u8,
    started: std::time::Instant,
    questions: u64,
    /// Current packet size limit: MAX_PREAUTH_PACKET until authenticated.
    max_packet: usize,
    /// More results of the current multi-statement follow this one.
    more: bool,
}

const COM_QUIT: u8 = 0x01;
const COM_INIT_DB: u8 = 0x02;
const COM_QUERY: u8 = 0x03;
const COM_FIELD_LIST: u8 = 0x04;
const COM_STATISTICS: u8 = 0x09;
const COM_PING: u8 = 0x0e;
const COM_CHANGE_USER: u8 = 0x11;
const COM_STMT_PREPARE: u8 = 0x16;
const COM_STMT_EXECUTE: u8 = 0x17;
const COM_STMT_SEND_LONG_DATA: u8 = 0x18;
const COM_STMT_CLOSE: u8 = 0x19;
const COM_STMT_RESET: u8 = 0x1a;
const COM_SET_OPTION: u8 = 0x1b;
const COM_STMT_FETCH: u8 = 0x1c;
const COM_RESET_CONNECTION: u8 = 0x1f;

fn malformed() -> SqlError {
    SqlError::new(1835, "HY000", "Malformed communication packet.")
}

fn unknown_stmt(id: u32, cmd: &str) -> SqlError {
    SqlError::new(
        1243,
        "HY000",
        format!("Unknown prepared statement handler ({}) given to {}", id, cmd),
    )
}

fn text(b: &[u8]) -> std::borrow::Cow<'_, str> {
    String::from_utf8_lossy(b)
}

fn too_large() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "packet larger than max_allowed_packet")
}

/// Split a COM_QUERY text into its statements at `;` outside quotes and
/// comments (multi-statement mode). Statements that are empty or only
/// whitespace and comments are dropped, so a trailing `;` adds nothing.
pub fn split_statements(sql: &str) -> Vec<&str> {
    let b = sql.as_bytes();
    let mut out = Vec::new();
    let (mut start, mut i) = (0usize, 0usize);
    let mut content = false;
    fn push<'a>(out: &mut Vec<&'a str>, s: &'a str, content: bool) {
        if content {
            out.push(s.trim());
        }
    }
    while i < b.len() {
        let c = b[i];
        match c {
            b'\'' | b'"' | b'`' => {
                content = true;
                let mut j = i + 1;
                while j < b.len() {
                    if b[j] == b'\\' && c != b'`' {
                        j += 2;
                        continue;
                    }
                    if b[j] == c {
                        if b.get(j + 1) == Some(&c) {
                            j += 2;
                            continue;
                        }
                        break;
                    }
                    j += 1;
                }
                i = j + 1;
            }
            b'#' => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'-' if b.get(i + 1) == Some(&b'-') && b.get(i + 2).is_none_or(|x| *x <= b' ') => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if b.get(i + 1) == Some(&b'*') => {
                // executable comments /*! ... */ are statement text
                if matches!(b.get(i + 2), Some(b'!') | Some(b'M')) {
                    content = true;
                }
                let mut j = i + 2;
                while j + 1 < b.len() && !(b[j] == b'*' && b[j + 1] == b'/') {
                    j += 1;
                }
                i = (j + 2).min(b.len());
            }
            b';' => {
                push(&mut out, &sql[start..i], content);
                content = false;
                i += 1;
                start = i;
            }
            _ => {
                if !c.is_ascii_whitespace() {
                    content = true;
                }
                i += 1;
            }
        }
    }
    push(&mut out, &sql[start.min(sql.len())..], content);
    out
}

/// Take one complete logical packet (at most `max` bytes) off the front of
/// `rbuf`.
fn take_packet(rbuf: &mut BytesMut, max: usize) -> io::Result<Option<(Bytes, u8)>> {
    let Some((len, seq)) = codec::packet_header(rbuf) else {
        return Ok(None);
    };
    if len > max {
        return Err(too_large());
    }
    if len < codec::MAX_PAYLOAD {
        if rbuf.len() < 4 + len {
            return Ok(None);
        }
        let mut p = rbuf.split_to(4 + len);
        p.advance(4);
        return Ok(Some((p.freeze(), seq)));
    }
    // Multi-packet payload: wait until every piece is here, then reassemble.
    let mut off = 0usize;
    let mut last_seq;
    loop {
        let Some((l, s)) = codec::packet_header(&rbuf[off..]) else {
            return Ok(None);
        };
        if off + l > max {
            return Err(too_large());
        }
        if rbuf.len() < off + 4 + l {
            return Ok(None);
        }
        off += 4 + l;
        last_seq = s;
        if l < codec::MAX_PAYLOAD {
            break;
        }
    }
    let (payload, used) = codec::deframe(&rbuf[..off]).expect("all packets present");
    debug_assert_eq!(used, off);
    rbuf.advance(off);
    Ok(Some((Bytes::from(payload), last_seq)))
}

impl<B: Backend> Conn<B> {
    /// Append one packet built by `f` to the write buffer.
    fn send(&mut self, f: impl FnOnce(&mut Vec<u8>)) {
        self.scratch.clear();
        f(&mut self.scratch);
        codec::frame_packets(&mut self.wbuf, &self.scratch, &mut self.seq);
        if self.scratch.capacity() > 1 << 20 {
            self.scratch = Vec::with_capacity(4096);
        }
    }

    fn deprecate_eof(&self) -> bool {
        self.caps & cap::DEPRECATE_EOF != 0
    }

    fn found_rows(&self) -> bool {
        self.caps & cap::FOUND_ROWS != 0
    }

    /// Status flags for an OK/EOF packet: the backend's, plus
    /// SERVER_MORE_RESULTS_EXISTS while a multi-statement has more results.
    fn status(&self) -> u16 {
        self.backend.status_flags()
            | if self.more {
                packets::SERVER_MORE_RESULTS_EXISTS
            } else {
                0
            }
    }

    fn send_ok(&mut self, affected: u64, last_id: u64, warnings: u16, info: &str) {
        let st = self.status();
        self.send(|p| packets::ok(p, 0x00, affected, last_id, st, warnings, info));
    }

    fn send_err(&mut self, e: &SqlError) {
        self.send(|p| packets::err(p, e));
    }

    /// End of a result set / metadata block: EOF, or OK with header 0xFE.
    fn send_eof_or_ok(&mut self) {
        let st = self.status();
        if self.deprecate_eof() {
            self.send(|p| packets::ok(p, 0xFE, 0, 0, st, 0, ""));
        } else {
            self.send(|p| packets::eof(p, st));
        }
    }

    fn send_eof_if_needed(&mut self) {
        if !self.deprecate_eof() {
            let st = self.backend.status_flags();
            self.send(|p| packets::eof(p, st));
        }
    }

    async fn flush(&mut self) -> io::Result<()> {
        if !self.wbuf.is_empty() {
            self.stream.write_all(&self.wbuf).await?;
            self.wbuf.clear();
            if self.wbuf.capacity() > 64 << 10 {
                self.wbuf = Vec::with_capacity(16 << 10);
            }
        }
        if self.scratch.capacity() > 64 << 10 {
            self.scratch = Vec::with_capacity(1024);
        }
        self.result_guards.clear();
        Ok(())
    }

    async fn read_packet(&mut self) -> io::Result<Option<(Bytes, u8)>> {
        loop {
            if let Some(p) = take_packet(&mut self.rbuf, self.max_packet)? {
                return Ok(Some(p));
            }
            // Nothing complete buffered: answer everything so far, then block.
            self.flush().await?;
            let need = (self.rbuf.len().saturating_add(16 << 10).next_power_of_two() as u64)
                .saturating_mul(4)
                .saturating_add(256 << 10);
            if need > self.buffer_charge {
                if let Some(pool) = &self.opts.memory {
                    let guard = pool
                        .clone()
                        .reserve(need - self.buffer_charge)
                        .map_err(io::Error::other)?;
                    self.buffer_guards.push(guard);
                }
                self.buffer_charge = need;
            }
            self.rbuf.reserve(16 << 10);
            if self.stream.read_buf(&mut self.rbuf).await? == 0 {
                return Ok(None);
            }
        }
    }

    async fn handshake(&mut self) -> io::Result<bool> {
        let mut scramble = [0u8; 20];
        {
            let mut rng = rand::thread_rng();
            for b in scramble.iter_mut() {
                *b = rng.gen_range(33u8..127);
            }
        }
        self.seq = 0;
        let ver = self.opts.server_version.clone();
        let id = self.id;
        self.send(|p| packets::handshake_v10(p, &ver, id, &scramble, SERVER_STATUS_AUTOCOMMIT));
        let Some((payload, seq)) = self.read_packet().await? else {
            return Ok(false);
        };
        self.seq = seq.wrapping_add(1);
        let Some(hs) = packets::parse_handshake_response(&payload) else {
            self.send_err(&SqlError::new(1043, "08S01", "Bad handshake"));
            self.flush().await?;
            return Ok(false);
        };
        self.caps = hs.caps & packets::SERVER_CAPS;
        let user = text(hs.user).into_owned();
        let db = hs.database.filter(|d| !d.is_empty()).map(|d| text(d).into_owned());
        let mut response = hs.auth.to_vec();
        let plugin = hs
            .plugin
            .map(|p| text(p).into_owned())
            .unwrap_or_else(|| packets::NATIVE_PASSWORD.into());
        let users = match &self.opts.auth {
            AuthConfig::AllowAll => None,
            AuthConfig::Users(u) => Some(u.clone()),
        };
        if let Some(users) = users {
            if plugin != packets::NATIVE_PASSWORD {
                self.send(|p| packets::auth_switch(p, &scramble));
                let Some((resp, seq)) = self.read_packet().await? else {
                    return Ok(false);
                };
                self.seq = seq.wrapping_add(1);
                response = resp.to_vec();
            }
            let ok = users
                .get(&user)
                .is_some_and(|pw| packets::native_password_ok(pw, &scramble, &response));
            if !ok {
                let host = self.stream.peer_addr().map(|a| a.ip().to_string()).unwrap_or_default();
                let using = if response.is_empty() { "NO" } else { "YES" };
                self.send_err(&SqlError::new(
                    1045,
                    "28000",
                    format!(
                        "Access denied for user '{}'@'{}' (using password: {})",
                        user, host, using
                    ),
                ));
                self.flush().await?;
                return Ok(false);
            }
        }
        self.collation = hs.collation;
        match self
            .backend
            .on_connect(&user, db.as_deref(), self.found_rows(), hs.collation)
            .await
        {
            Ok(()) => {
                self.user = user;
                self.db = db;
                self.max_packet = MAX_ALLOWED_PACKET;
                self.send_ok(0, 0, 0, "");
                Ok(true)
            }
            Err(e) => {
                self.send_err(&e);
                self.flush().await?;
                Ok(false)
            }
        }
    }

    async fn send_result(&mut self, r: SqlResult<QueryResult>, binary: bool) -> io::Result<()> {
        match r {
            Err(e) => self.send_err(&e),
            Ok(QueryResult::Ok {
                affected_rows,
                last_insert_id,
                info,
                warnings,
            }) => self.send_ok(affected_rows, last_insert_id, warnings, &info),
            Ok(QueryResult::Rows {
                columns,
                rows,
                resource_guard,
            }) => {
                if columns.is_empty() {
                    self.send_ok(0, 0, 0, "");
                    return Ok(());
                }
                let n = columns.len();
                self.send(|p| codec::write_lenenc_int(p, n as u64));
                for c in columns.iter() {
                    self.send(|p| packets::column_def(p, c));
                }
                self.send_eof_if_needed();
                for row in &rows {
                    if binary {
                        self.send(|p| packets::binary_row(p, &columns, row));
                    } else {
                        self.send(|p| packets::text_row(p, n, row));
                    }
                    if self.wbuf.len() > 1 << 20 {
                        self.flush().await?;
                    }
                }
                self.send_eof_or_ok();
                if let Some(guard) = resource_guard {
                    self.result_guards.push(guard);
                }
            }
        }
        Ok(())
    }

    async fn reset_session(&mut self) {
        let ids: Vec<u32> = self.stmts.keys().copied().collect();
        for id in ids {
            self.backend.on_close_stmt(id).await;
        }
        self.stmts.clear();
        self.backend.on_disconnect().await;
        let (user, db) = (self.user.clone(), self.db.clone());
        let (found_rows, collation) = (self.found_rows(), self.collation);
        if let Err(e) = self
            .backend
            .on_connect(&user, db.as_deref(), found_rows, collation)
            .await
        {
            tracing::debug!("reset connection: reconnect with db {:?} failed: {}", db, e);
            let _ = self.backend.on_connect(&user, None, found_rows, collation).await;
        }
    }

    /// Handle one command. Returns false when the connection should close.
    async fn dispatch(&mut self, p: Bytes) -> io::Result<bool> {
        let Some(&cmd) = p.first() else {
            self.send_err(&malformed());
            return Ok(true);
        };
        let body = p.slice(1..);
        self.questions += 1;
        match cmd {
            COM_QUIT => return Ok(false),
            COM_QUERY => {
                let sql = text(&body);
                if self.caps & cap::MULTI_STATEMENTS != 0 {
                    let parts = split_statements(&sql);
                    if parts.len() > 1 {
                        // One result per statement; the first error ends
                        // the batch (the rest is not executed).
                        let last = parts.len() - 1;
                        for (i, part) in parts.into_iter().enumerate() {
                            let r = self.backend.on_query(part).await;
                            let failed = r.is_err();
                            self.more = i < last && !failed;
                            self.send_result(r, false).await?;
                            self.more = false;
                            if failed {
                                break;
                            }
                        }
                        return Ok(true);
                    }
                }
                let r = self.backend.on_query(&sql).await;
                self.send_result(r, false).await?;
            }
            COM_STMT_EXECUTE => {
                let mut r = Reader::new(&body);
                let (Some(id), Some(_flags), Some(_iters)) = (r.u32(), r.u8(), r.u32()) else {
                    self.send_err(&malformed());
                    return Ok(true);
                };
                let Some(st) = self.stmts.get_mut(&id) else {
                    self.send_err(&unknown_stmt(id, "mysqld_stmt_execute"));
                    return Ok(true);
                };
                if st.long_overflow {
                    st.long_data.clear();
                    st.long_bytes = 0;
                    st.long_overflow = false;
                    self.send_err(&SqlError::new(
                        1153,
                        "08S01",
                        "Got a packet bigger than 'max_allowed_packet' bytes",
                    ));
                    return Ok(true);
                }
                if let Some(error) = st.long_error.take() {
                    st.long_data.clear();
                    st.long_bytes = 0;
                    st.long_charge = 0;
                    st.long_guards.clear();
                    self.send_err(&error);
                    return Ok(true);
                }
                let params = packets::decode_params(&mut r, st.num_params as usize, &mut st.types, &mut st.long_data);
                let _long_guards = std::mem::take(&mut st.long_guards);
                st.long_data.clear();
                st.long_bytes = 0;
                st.long_charge = 0;
                st.long_guards.clear();
                match params {
                    None => self.send_err(&malformed()),
                    Some(params) => {
                        let res = self.backend.on_execute(id, &params).await;
                        self.send_result(res, true).await?;
                    }
                }
            }
            COM_STMT_PREPARE => {
                if self.stmts.len() >= MAX_PREPARED_STMTS {
                    self.send_err(&SqlError::new(
                        1461,
                        "42000",
                        format!(
                            "Can't create more than max_prepared_stmt_count statements (current value: {})",
                            MAX_PREPARED_STMTS
                        ),
                    ));
                    return Ok(true);
                }
                let sql = text(&body).into_owned();
                let id = self.next_stmt;
                self.next_stmt = self.next_stmt.wrapping_add(1).max(1);
                match self.backend.on_prepare(id, &sql).await {
                    Err(e) => self.send_err(&e),
                    Ok(info) => {
                        self.stmts.insert(
                            id,
                            Stmt {
                                num_params: info.num_params,
                                types: Vec::new(),
                                long_data: HashMap::new(),
                                long_bytes: 0,
                                long_overflow: false,
                                long_error: None,
                                long_charge: 0,
                                long_guards: Vec::new(),
                            },
                        );
                        let ncols = info.columns.len() as u16;
                        let nparams = info.num_params;
                        self.send(|p| packets::stmt_prepare_ok(p, id, ncols, nparams));
                        if nparams > 0 {
                            for _ in 0..nparams {
                                self.send(packets::param_def);
                            }
                            self.send_eof_if_needed();
                        }
                        if ncols > 0 {
                            for c in info.columns.iter() {
                                self.send(|p| packets::column_def(p, c));
                            }
                            self.send_eof_if_needed();
                        }
                    }
                }
            }
            COM_STMT_SEND_LONG_DATA => {
                let mut r = Reader::new(&body);
                if let (Some(id), Some(param)) = (r.u32(), r.u16()) {
                    if let Some(st) = self.stmts.get_mut(&id) {
                        let chunk = r.rest();
                        if st.long_overflow || st.long_bytes + chunk.len() > MAX_ALLOWED_PACKET {
                            st.long_overflow = true;
                            st.long_data.clear();
                            st.long_bytes = 0;
                            st.long_charge = 0;
                            st.long_guards.clear();
                        } else {
                            if st.long_error.is_some() {
                                return Ok(true);
                            }
                            let need =
                                ((st.long_bytes + chunk.len()).max(1).next_power_of_two() as u64).saturating_mul(4);
                            if need > st.long_charge {
                                if let Some(pool) = &self.opts.memory {
                                    match pool.clone().reserve(need - st.long_charge) {
                                        Ok(guard) => st.long_guards.push(guard),
                                        Err(error) => {
                                            st.long_error = Some(error);
                                            st.long_data.clear();
                                            st.long_bytes = 0;
                                            st.long_charge = 0;
                                            st.long_guards.clear();
                                            return Ok(true);
                                        }
                                    }
                                }
                                st.long_charge = need;
                            }
                            st.long_bytes += chunk.len();
                            st.long_data.entry(param).or_default().extend_from_slice(chunk);
                        }
                    }
                }
                // no response
            }
            COM_STMT_CLOSE => {
                if let Some(id) = Reader::new(&body).u32() {
                    if self.stmts.remove(&id).is_some() {
                        self.backend.on_close_stmt(id).await;
                    }
                }
                // no response
            }
            COM_STMT_RESET => match Reader::new(&body).u32() {
                Some(id) => match self.stmts.get_mut(&id) {
                    Some(st) => {
                        st.long_data.clear();
                        st.long_bytes = 0;
                        st.long_overflow = false;
                        st.long_error = None;
                        st.long_charge = 0;
                        st.long_guards.clear();
                        self.send_ok(0, 0, 0, "");
                    }
                    None => self.send_err(&unknown_stmt(id, "mysqld_stmt_reset")),
                },
                None => self.send_err(&malformed()),
            },
            COM_PING => self.send_ok(0, 0, 0, ""),
            COM_INIT_DB => {
                let db = text(&body).into_owned();
                match self.backend.on_init_db(&db).await {
                    Ok(()) => {
                        self.db = Some(db);
                        self.send_ok(0, 0, 0, "");
                    }
                    Err(e) => self.send_err(&e),
                }
            }
            COM_FIELD_LIST => self.send_eof_or_ok(),
            COM_STATISTICS => {
                let s = format!(
                    "Uptime: {}  Threads: 1  Questions: {}  Slow queries: 0  Opens: 0  Flush tables: 0  Open tables: 0  Queries per second avg: 0.000",
                    self.started.elapsed().as_secs(),
                    self.questions
                );
                self.send(|p| p.extend_from_slice(s.as_bytes()));
            }
            COM_SET_OPTION => match Reader::new(&body).u16() {
                // MYSQL_OPTION_MULTI_STATEMENTS_ON / _OFF
                Some(0) => {
                    self.caps |= cap::MULTI_STATEMENTS;
                    self.send_eof_or_ok();
                }
                Some(1) => {
                    self.caps &= !cap::MULTI_STATEMENTS;
                    self.send_eof_or_ok();
                }
                _ => self.send_err(&SqlError::new(1047, "08S01", "Unknown command")),
            },
            COM_RESET_CONNECTION => {
                self.reset_session().await;
                self.send_ok(0, 0, 0, "");
            }
            COM_STMT_FETCH => self.send_err(&SqlError::new(1235, "42000", "Server-side cursors are not supported")),
            COM_CHANGE_USER => self.send_err(&SqlError::new(1047, "08S01", "COM_CHANGE_USER is not supported")),
            _ => self.send_err(&SqlError::new(1047, "08S01", "Unknown command")),
        }
        Ok(true)
    }

    async fn run(&mut self) -> io::Result<()> {
        if !self.handshake().await? {
            return Ok(());
        }
        loop {
            let Some((payload, seq)) = self.read_packet().await? else {
                return Ok(());
            };
            self.seq = seq.wrapping_add(1);
            if !self.dispatch(payload).await? {
                return self.flush().await;
            }
        }
    }
}

/// Serve one client connection until it quits or the socket fails.
/// `backend.on_disconnect()` runs on every exit path.
pub async fn serve_connection<B: Backend>(
    stream: TcpStream,
    connection_id: u32,
    opts: Arc<ServerOpts>,
    backend: B,
) -> io::Result<()> {
    let _ = stream.set_nodelay(true);
    let initial_charge = (256 + 4 * 16) << 10;
    let buffer_guard = opts
        .memory
        .as_ref()
        .map(|pool| pool.clone().reserve(initial_charge))
        .transpose()
        .map_err(io::Error::other)?;
    let mut c = Conn {
        stream,
        rbuf: BytesMut::with_capacity(16 << 10),
        wbuf: Vec::with_capacity(16 << 10),
        result_guards: Vec::new(),
        buffer_charge: initial_charge,
        buffer_guards: buffer_guard.into_iter().collect(),
        scratch: Vec::with_capacity(1024),
        seq: 0,
        caps: 0,
        id: connection_id,
        opts,
        backend,
        stmts: HashMap::new(),
        next_stmt: 1,
        user: String::new(),
        db: None,
        collation: 0,
        started: std::time::Instant::now(),
        questions: 0,
        max_packet: MAX_PREAUTH_PACKET,
        more: false,
    };
    let r = c.run().await;
    c.backend.on_disconnect().await;
    r
}

#[cfg(test)]
mod tests {
    use super::*;

    fn packet(len: usize, seq: u8) -> Vec<u8> {
        let mut v = vec![
            (len & 0xff) as u8,
            ((len >> 8) & 0xff) as u8,
            ((len >> 16) & 0xff) as u8,
            seq,
        ];
        v.resize(4 + len, b'x');
        v
    }

    #[test]
    fn splits_statements() {
        assert_eq!(split_statements("SELECT 1"), vec!["SELECT 1"]);
        assert_eq!(split_statements("SELECT 1;"), vec!["SELECT 1"]);
        assert_eq!(
            split_statements(" SELECT 1 ; SELECT 2;\n"),
            vec!["SELECT 1", "SELECT 2"]
        );
        assert_eq!(
            split_statements("INSERT INTO t VALUES ('a;b', \"c;\", `d;`); SELECT 'it''s;' -- x;y\n; # z;\n"),
            vec!["INSERT INTO t VALUES ('a;b', \"c;\", `d;`)", "SELECT 'it''s;' -- x;y"]
        );
        assert_eq!(
            split_statements("SELECT 'a\\';b'; SELECT 2"),
            vec!["SELECT 'a\\';b'", "SELECT 2"]
        );
        assert_eq!(
            split_statements("/* c; */ SELECT 1; /*!40101 SET x=1 */;"),
            vec!["/* c; */ SELECT 1", "/*!40101 SET x=1 */"]
        );
        assert!(split_statements(" ; -- only\n; ").is_empty());
    }

    #[test]
    fn packet_size_limits() {
        // A small packet passes under the pre-authentication limit.
        let mut b = BytesMut::from(&packet(100, 1)[..]);
        let (p, seq) = take_packet(&mut b, MAX_PREAUTH_PACKET).unwrap().unwrap();
        assert_eq!((p.len(), seq), (100, 1));
        // A single packet above the limit is refused from its header alone,
        // before its body is buffered.
        let mut b = BytesMut::from(&packet(MAX_PREAUTH_PACKET + 1, 1)[..8]);
        assert!(take_packet(&mut b, MAX_PREAUTH_PACKET).is_err());
        // ... and is accepted after authentication.
        let mut b = BytesMut::from(&packet(MAX_PREAUTH_PACKET + 1, 1)[..]);
        assert_eq!(
            take_packet(&mut b, MAX_ALLOWED_PACKET).unwrap().unwrap().0.len(),
            MAX_PREAUTH_PACKET + 1
        );
        // A multi-packet payload is refused once its pieces exceed the limit.
        let max = codec::MAX_PAYLOAD; // 0xFFFFFF
        let mut big = packet(max, 0);
        big.extend_from_slice(&packet(max, 1)[..4]);
        let mut b = BytesMut::from(&big[..]);
        assert!(take_packet(&mut b, max + 10).is_err());
        // An incomplete packet under the limit just waits for more bytes.
        let mut b = BytesMut::from(&packet(1000, 0)[..500]);
        assert!(take_packet(&mut b, MAX_PREAUTH_PACKET).unwrap().is_none());
    }
}
