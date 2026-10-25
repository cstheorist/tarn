//! IPROTO protocol constants, transcribed from Tarantool's
//! `src/box/iproto_constants.h` and `src/box/iproto_features.h`.
//!
//! Request type codes are `u32` so they can be passed straight to
//! [`Client::request`](crate::Client::request).
#![allow(missing_docs)]

// ---- request / response types (`IPROTO_TYPES`) ----
pub const OK: u32 = 0;
pub const SELECT: u32 = 1;
pub const INSERT: u32 = 2;
pub const REPLACE: u32 = 3;
pub const UPDATE: u32 = 4;
pub const DELETE: u32 = 5;
/// Legacy call that wraps results into tuples. Prefer [`CALL`].
pub const CALL_16: u32 = 6;
pub const AUTH: u32 = 7;
pub const EVAL: u32 = 8;
pub const UPSERT: u32 = 9;
/// CALL that returns arbitrary MessagePack (what `Client::call` sends).
pub const CALL: u32 = 10;
pub const EXECUTE: u32 = 11;
pub const NOP: u32 = 12;
pub const PREPARE: u32 = 13;
pub const BEGIN: u32 = 14;
pub const COMMIT: u32 = 15;
pub const ROLLBACK: u32 = 16;
pub const INSERT_ARROW: u32 = 17;
pub const DELETE_RANGE: u32 = 18;
pub const PING: u32 = 64;
pub const ID: u32 = 73;
pub const WATCH: u32 = 74;
pub const UNWATCH: u32 = 75;
pub const EVENT: u32 = 76;
pub const WATCH_ONCE: u32 = 77;
/// Non-final response (`box.session.push`). Shares the sync of the request
/// that is still running; the client skips these.
pub const CHUNK: u32 = 128;
/// Bit set in the response type of error responses; the low 15 bits are the
/// `box.error` code.
pub const TYPE_ERROR: u32 = 0x8000;

/// Size of the greeting the server sends on accept.
pub const GREETING_SIZE: usize = 128;
/// Bytes of the greeting salt used by chap-sha1.
pub const SCRAMBLE_SIZE: usize = 20;
/// Largest packet the server accepts (`IPROTO_BODY_LEN_MAX`, 2 GiB).
pub const BODY_LEN_MAX: usize = 2_147_483_648;

/// IPROTO map keys (`IPROTO_KEYS`). All fit in a positive fixint, so they are
/// encoded as a single byte.
pub mod key {
    pub const REQUEST_TYPE: u8 = 0x00;
    pub const SYNC: u8 = 0x01;
    pub const REPLICA_ID: u8 = 0x02;
    pub const LSN: u8 = 0x03;
    pub const TIMESTAMP: u8 = 0x04;
    pub const SCHEMA_VERSION: u8 = 0x05;
    pub const SERVER_VERSION: u8 = 0x06;
    pub const GROUP_ID: u8 = 0x07;
    pub const TSN: u8 = 0x08;
    pub const FLAGS: u8 = 0x09;
    pub const STREAM_ID: u8 = 0x0a;
    pub const THREAD_ID: u8 = 0x0b;
    pub const AUDIT_CONTEXT: u8 = 0x0c;
    pub const SPACE_ID: u8 = 0x10;
    pub const INDEX_ID: u8 = 0x11;
    pub const LIMIT: u8 = 0x12;
    pub const OFFSET: u8 = 0x13;
    pub const ITERATOR: u8 = 0x14;
    pub const INDEX_BASE: u8 = 0x15;
    pub const FETCH_POSITION: u8 = 0x1f;
    pub const KEY: u8 = 0x20;
    pub const TUPLE: u8 = 0x21;
    pub const FUNCTION_NAME: u8 = 0x22;
    pub const USER_NAME: u8 = 0x23;
    pub const EXPR: u8 = 0x27;
    pub const OPS: u8 = 0x28;
    pub const OPTIONS: u8 = 0x2b;
    pub const AFTER_POSITION: u8 = 0x2e;
    pub const AFTER_TUPLE: u8 = 0x2f;
    pub const DATA: u8 = 0x30;
    pub const ERROR_24: u8 = 0x31;
    pub const METADATA: u8 = 0x32;
    pub const BIND_METADATA: u8 = 0x33;
    pub const BIND_COUNT: u8 = 0x34;
    pub const POSITION: u8 = 0x35;
    pub const SQL_TEXT: u8 = 0x40;
    pub const SQL_BIND: u8 = 0x41;
    pub const SQL_INFO: u8 = 0x42;
    pub const STMT_ID: u8 = 0x43;
    pub const ERROR: u8 = 0x52;
    pub const VERSION: u8 = 0x54;
    pub const FEATURES: u8 = 0x55;
    pub const TIMEOUT: u8 = 0x56;
    pub const EVENT_KEY: u8 = 0x57;
    pub const EVENT_DATA: u8 = 0x58;
    pub const TXN_ISOLATION: u8 = 0x59;
    pub const VCLOCK_SYNC: u8 = 0x5a;
    pub const AUTH_TYPE: u8 = 0x5b;
    pub const SPACE_NAME: u8 = 0x5e;
    pub const INDEX_NAME: u8 = 0x5f;
    pub const TUPLE_FORMATS: u8 = 0x60;
    pub const IS_SYNC: u8 = 0x61;
}

/// Keys of the `MP_ERROR` map carried under [`key::ERROR`] (`src/box/mp_error.cc`).
pub mod error_key {
    /// Array of error maps, outermost error first.
    pub const STACK: u8 = 0x00;
    pub const TYPE: u8 = 0x00;
    pub const FILE: u8 = 0x01;
    pub const LINE: u8 = 0x02;
    pub const MESSAGE: u8 = 0x03;
    pub const ERRNO: u8 = 0x04;
    pub const CODE: u8 = 0x05;
    pub const FIELDS: u8 = 0x06;
}

/// Protocol feature ids exchanged in `IPROTO_ID` (`IPROTO_FEATURES`).
pub mod feature {
    pub const STREAMS: u32 = 0;
    pub const TRANSACTIONS: u32 = 1;
    pub const ERROR_EXTENSION: u32 = 2;
    pub const WATCHERS: u32 = 3;
    pub const PAGINATION: u32 = 4;
    pub const SPACE_AND_INDEX_NAMES: u32 = 5;
    pub const WATCH_ONCE: u32 = 6;
    pub const DML_TUPLE_EXTENSION: u32 = 7;
    pub const CALL_RET_TUPLE_EXTENSION: u32 = 8;
    pub const CALL_ARG_TUPLE_EXTENSION: u32 = 9;
    pub const FETCH_SNAPSHOT_CURSOR: u32 = 10;
    pub const IS_SYNC: u32 = 11;
    pub const INSERT_ARROW: u32 = 12;
}

/// The protocol version this client announces in `IPROTO_ID`. Streams and
/// transactions (the only features we request) appeared in version 1; the
/// server does not gate anything on the client's version, so we announce the
/// lowest version consistent with the features we declare.
pub const CLIENT_PROTOCOL_VERSION: u64 = 1;

/// Features this client announces. Deliberately *not* included:
/// `ERROR_EXTENSION` (CALL/EVAL would return error objects as `MP_ERROR`
/// extensions instead of strings) and the `*_TUPLE_EXTENSION` features
/// (tuples in `IPROTO_DATA` would become `MP_TUPLE` extensions with separate
/// format maps instead of plain arrays).
pub const CLIENT_FEATURES: [u32; 2] = [feature::STREAMS, feature::TRANSACTIONS];
