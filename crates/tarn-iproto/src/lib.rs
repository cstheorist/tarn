//! Async, pipelined Tarantool IPROTO client.
//!
//! One [`Client`] owns one TCP connection that carries any number of
//! in-flight requests, matched to their replies by `IPROTO_SYNC`. Requests
//! are encoded straight into a shared write buffer that a writer task
//! flushes with as few syscalls as possible; a reader task splits replies
//! out of a growing read buffer as zero-copy [`bytes::Bytes`].
//!
//! Interactive transactions use IPROTO streams: take an id from
//! [`Client::next_stream_id`], then [`Client::begin`], issue requests with
//! that stream id, and [`Client::commit`] / [`Client::rollback`].
//!
//! The [`mp`] module converts between MessagePack and
//! [`tarn_common::Value`], including Tarantool's `MP_DECIMAL` and
//! `MP_DATETIME` extensions.

mod client;
pub mod iproto;
pub mod mp;

pub use client::{Client, Response, ServerInfo, Stats};
pub use errcode::*;

use std::time::Duration;

/// Connection options.
#[derive(Clone)]
pub struct Opts {
    /// `host:port`.
    pub addr: String,
    /// User to authenticate as (chap-sha1). `None` stays `guest`.
    pub user: Option<String>,
    /// Password for `user`; `None` is the empty password.
    pub password: Option<String>,
    /// Bound on TCP connect + greeting + `IPROTO_ID` + `IPROTO_AUTH`.
    /// `Duration::ZERO` means no limit.
    pub connect_timeout: Duration,
    /// Retire a connection before reserving an oversized response frame.
    pub max_response_bytes: usize,
}

impl Opts {
    /// Options for `addr` with no authentication and a 5 s connect timeout.
    pub fn new(addr: impl Into<String>) -> Opts {
        Opts {
            addr: addr.into(),
            user: None,
            password: None,
            connect_timeout: Duration::from_secs(5),
            max_response_bytes: iproto::BODY_LEN_MAX,
        }
    }
}

impl std::fmt::Debug for Opts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Opts")
            .field("addr", &self.addr)
            .field("user", &self.user)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("connect_timeout", &self.connect_timeout)
            .field("max_response_bytes", &self.max_response_bytes)
            .finish()
    }
}

/// Index iterator (`enum iterator_type` in `src/box/iterator_type.h`).
#[repr(u32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Iter {
    /// key == x, ascending.
    Eq = 0,
    /// key == x, descending.
    Req = 1,
    /// All tuples.
    All = 2,
    /// key < x, descending.
    Lt = 3,
    /// key <= x, descending.
    Le = 4,
    /// key >= x, ascending.
    Ge = 5,
    /// key > x, ascending.
    Gt = 6,
}

/// Transaction isolation level for `IPROTO_BEGIN`
/// (`enum txn_isolation_level` in `src/box/txn.h`).
#[repr(u32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TxnIsolation {
    /// The server's `box.cfg.txn_isolation`.
    Default = 0,
    ReadCommitted = 1,
    ReadConfirmed = 2,
    BestEffort = 3,
    Linearizable = 4,
}

/// Client errors.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("tarantool i/o error: {0}")]
    Io(#[from] std::io::Error),
    /// Malformed data from the server or from the caller.
    #[error("tarantool protocol error: {0}")]
    Protocol(String),
    /// An error reply. `code` is the `box.error` code (see the `ER_*`
    /// constants).
    #[error("tarantool error {code}: {message}")]
    Server { code: u32, message: String },
    /// The connection is gone (failed, closed, or the server went away).
    #[error("tarantool connection closed")]
    Closed,
    /// `connect_timeout` elapsed.
    #[error("tarantool operation timed out")]
    Timeout,
}

impl Error {
    /// The `box.error` code of a server error.
    pub fn server_code(&self) -> Option<u32> {
        match self {
            Error::Server { code, .. } => Some(*code),
            _ => None,
        }
    }

    /// True for errors that mean the connection is unusable.
    pub fn is_connection_error(&self) -> bool {
        matches!(self, Error::Io(_) | Error::Closed)
    }
}

/// Tarantool `box.error` codes (`src/box/errcode.h`) that callers act on.
pub mod errcode {
    pub const ER_UNKNOWN: u32 = 0;
    pub const ER_ILLEGAL_PARAMS: u32 = 1;
    pub const ER_MEMORY_ISSUE: u32 = 2;
    /// Duplicate key in a unique index.
    pub const ER_TUPLE_FOUND: u32 = 3;
    pub const ER_UNSUPPORTED: u32 = 5;
    /// Write on a read-only instance (e.g. not the leader).
    pub const ER_READONLY: u32 = 7;
    pub const ER_KEY_PART_TYPE: u32 = 18;
    pub const ER_EXACT_MATCH: u32 = 19;
    pub const ER_INVALID_MSGPACK: u32 = 20;
    pub const ER_TUPLE_NOT_ARRAY: u32 = 22;
    pub const ER_FIELD_TYPE: u32 = 23;
    pub const ER_UPDATE_ARG_TYPE: u32 = 26;
    pub const ER_UNKNOWN_UPDATE_OP: u32 = 28;
    pub const ER_UPDATE_FIELD: u32 = 29;
    pub const ER_FUNCTION_TX_ACTIVE: u32 = 30;
    pub const ER_KEY_PART_COUNT: u32 = 31;
    /// Lua error raised inside a called function (`error()` / `box.error`).
    pub const ER_PROC_LUA: u32 = 32;
    pub const ER_NO_SUCH_PROC: u32 = 33;
    pub const ER_NO_SUCH_INDEX_ID: u32 = 35;
    pub const ER_NO_SUCH_SPACE: u32 = 36;
    pub const ER_EXACT_FIELD_COUNT: u32 = 38;
    pub const ER_FIELD_MISSING: u32 = 39;
    pub const ER_ACCESS_DENIED: u32 = 42;
    pub const ER_NO_SUCH_USER: u32 = 45;
    /// Bad user name or password.
    pub const ER_CREDS_MISMATCH: u32 = 47;
    pub const ER_UNKNOWN_REQUEST_TYPE: u32 = 48;
    pub const ER_NO_SUCH_FUNCTION: u32 = 51;
    pub const ER_MISSING_REQUEST_FIELD: u32 = 69;
    pub const ER_NO_CONNECTION: u32 = 77;
    pub const ER_TIMEOUT: u32 = 78;
    /// BEGIN while a transaction is already active in the stream.
    pub const ER_ACTIVE_TRANSACTION: u32 = 79;
    pub const ER_CANT_UPDATE_PRIMARY_KEY: u32 = 94;
    pub const ER_UPDATE_INTEGER_OVERFLOW: u32 = 95;
    /// MVCC conflict: the transaction was aborted and must be retried.
    pub const ER_TRANSACTION_CONFLICT: u32 = 97;
    /// A C function's module has no such symbol.
    pub const ER_LOAD_FUNCTION: u32 = 99;
    pub const ER_NO_TRANSACTION: u32 = 114;
    pub const ER_LOADING: u32 = 116;
    /// A C function's module could not be loaded.
    pub const ER_LOAD_MODULE: u32 = 138;
    pub const ER_TRANSACTION_YIELD: u32 = 154;
    pub const ER_TXN_ROLLBACK: u32 = 225;
    /// A stream-only request (BEGIN/COMMIT/ROLLBACK) sent with stream id 0 is
    /// rejected with `ER_UNABLE_TO_PROCESS_OUT_OF_STREAM`; a request that
    /// cannot run in a stream (e.g. AUTH) with `..._IN_STREAM`. There is no
    /// "no such stream" error: streams are created on first use.
    pub const ER_UNABLE_TO_PROCESS_IN_STREAM: u32 = 229;
    pub const ER_UNABLE_TO_PROCESS_OUT_OF_STREAM: u32 = 230;
    /// The stream transaction outlived its `begin` timeout and was rolled back.
    pub const ER_TRANSACTION_TIMEOUT: u32 = 231;
    pub const ER_SHUTDOWN: u32 = 279;
}
