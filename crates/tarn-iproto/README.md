# tarn-iproto

Async, pipelined [Tarantool](https://www.tarantool.io) IPROTO client for Tarn's
storage layer. One TCP connection carries any number of in-flight requests.
Writes are coalesced into few syscalls, and replies are handed out as zero-copy
`bytes::Bytes`. Developed and tested against Tarantool 3.9.0.

## API

```rust
use tarn_iproto::{Client, Opts, Iter, TxnIsolation, mp};

let client = Client::connect(&Opts {
    addr: "127.0.0.1:3301".into(),
    user: Some("tarn".into()),
    password: Some("tarn".into()),
    connect_timeout: std::time::Duration::from_secs(3),
}).await?;

// Primary-key lookup: key and tuples are pre-encoded MessagePack arrays.
let mut key = Vec::new();
mp::encode_tuple(&mut key, &[tarn_common::Value::Int(42)]);
let resp = client.select(0, 512, 0, Iter::Eq, 0, 1, &key).await?;
for raw in resp.tuples()? { /* raw MessagePack array per tuple */ }
let rows = resp.rows()?;            // Vec<Vec<Value>>, strings zero-copy

// Interactive transaction in a stream.
let sid = client.next_stream_id();
client.begin(sid, Some(60.0), Some(TxnIsolation::ReadConfirmed)).await?;
client.replace(sid, 512, &tuple).await?;
client.commit(sid).await?;          // Err(Server{code: ER_TRANSACTION_CONFLICT, ..}) on conflict
```

| item | notes |
|---|---|
| `Opts { addr, user, password, connect_timeout }` | `user: None` stays `guest`. `password: None` is the empty password. `Duration::ZERO` means no timeout. `Opts::new(addr)` gives no auth and a 5 s timeout. |
| `Client::connect` | Reads the 128-byte greeting, then sends `IPROTO_ID` (version 1, features `streams` + `transactions`) and a chap-sha1 `IPROTO_AUTH` pipelined in one write. The whole handshake is bounded by `connect_timeout` (`Error::Timeout`). |
| `request(type, stream_id, body)` | Any request type, with `body` as a pre-encoded MessagePack map (empty = no body). |
| `ping`, `select`, `insert`, `replace`, `update`, `delete`, `call`, `eval` | `key`, `tuple`, `ops` and `args` are MessagePack arrays; an empty slice means `[]`. `call` is `IPROTO_CALL` (10), not `CALL_16`. |
| `update` field numbers | **1-based**: the request carries `IPROTO_INDEX_BASE = 1`, as in Lua `space:update()` and net.box. So `[["+", 2, 1], ["=", 3, "x"]]` adds 1 to the 2nd field and sets the 3rd. Negative numbers count from the end. |
| `begin(stream, timeout_secs, isolation)`, `commit`, `rollback` | Must use a non-zero stream id; stream id 0 gets `ER_UNABLE_TO_PROCESS_OUT_OF_STREAM`. |
| `next_stream_id()` | 1, 2, 3, … per connection. 0 means "no stream". |
| `is_closed()` | True once the connection failed or was closed. Every later call fails at once with `Error::Closed`. |
| `is_stale()`, `mark_stale()` | True once the server answered any request with `ER_READONLY` (it was demoted or is a replica). The connection keeps working; Tarn's connection pools stop handing it out and look for the new leader. |
| `Response { schema_version, body }` | `data()` returns the raw `IPROTO_DATA` value; `tuples()` returns each element of `DATA` as a raw slice. Both are zero-copy slices of `body`. |
| `Error` | `Io`, `Protocol(String)`, `Server { code, message }`, `Closed`, `Timeout`. `code` is the error code from the reply type (`type & 0x7fff`). `message` comes from `IPROTO_ERROR_24` (0x31), falling back to the first entry of the `IPROTO_ERROR` (0x52) stack. |
| error codes | `ER_TUPLE_FOUND` (3), `ER_READONLY` (7), `ER_PROC_LUA` (32), `ER_NO_SUCH_PROC` (33), `ER_NO_SUCH_SPACE` (36), `ER_TRANSACTION_CONFLICT` (97), `ER_TRANSACTION_TIMEOUT` (231), `ER_UNABLE_TO_PROCESS_{IN,OUT_OF}_STREAM` (229/230), and others: see `errcode` (all re-exported at the crate root). There is no "no such stream" error; the server creates streams on first use. |
| `Iter`, `TxnIsolation` | `#[repr(u32)]` + `Copy`, so `iter as u64` gives the wire code. |

### More API

`Opts::new`, `Client::close`, `Client::server_info` (greeting version,
protocol version, features, auth type), `Client::stats` (requests, replies,
write/read batches, bytes), `Response::rows`, `Error::server_code`,
`Error::is_connection_error`, and the `iproto` module (request types, keys,
feature ids). In `mp`: `decode_value_bytes` / `decode_tuple_shared` /
`decode_tuple_bytes` (zero-copy), `encode_decimal` / `decode_decimal` /
`decode_datetime`, and the `write_*` / `read_*` / `read_raw` primitives.

## MessagePack ↔ `Value` (`mp`)

| MessagePack | decodes to | `Value` | encodes as |
|---|---|---|---|
| nil | `Null` | `Null` | nil |
| bool | `Int(0/1)` | `Int(i)` | smallest encoding: positive fixint/uint8..64 for `i >= 0`, negative fixint/int8..64 otherwise (Tarantool's `unsigned` fields reject `MP_INT`) |
| uint ≤ `i64::MAX` / larger | `Int` / `UInt` | `UInt(u)` | smallest uint |
| int, float32/64 | `Int`, `Float` | `Float(f)` | float64 |
| str | `Str` (`Bin` if not valid UTF-8) | `Str(b)` | str (fixstr/str8/16/32) |
| bin | `Bin` | `Bin(b)` | bin8/16/32 |
| ext 1 `MP_DECIMAL` | `Decimal(text)` | `Decimal(text)` | `MP_DECIMAL` |
| ext 4 `MP_DATETIME` | `Str("YYYY-MM-DD HH:MM:SS[.ffffff]")` (UTC, tz ignored) | | |
| array, map, other ext (UUID, interval, …) | `Bin(raw MessagePack of the value)` | | |

**MP_DECIMAL** (`decimal_pack` / `decPackedFromNumber`): the ext payload is
`scale` (MP_UINT, or MP_INT when negative, i.e. a positive exponent) followed
by packed BCD. Digits are right-aligned, with at most one leading zero
nibble, and the final low nibble is the sign (`0xC` plus, `0xD` minus;
`0xA/0xE/0xF` and `0xB` are also accepted when decoding).
`"-12.340"` → `d6 01 03 12 34 0d`. The encoder parses
`[+-]digits[.digits][e[+-]digits]`, keeps trailing fractional zeros
(scale), and allows at most 76 significant digits (Tarantool's
`DECIMAL_MAX_DIGITS`). Zero is always positive. The decoder prints plain
notation: `1E+3` → `"1000"`, `1E-7` → `"0.0000001"`, `-0` → `"0"`. It falls
back to `d.dddE±x` only when |scale| > 1000. `encode_value` writes an
unparsable `Decimal` as a string, so the server rejects it rather than
storing a wrong number; `mp::encode_decimal` returns the error instead.
Unit tests check byte equality with what Tarantool's own `msgpack.encode`
produces, and the integration tests check `decimal.new(..)` values written by
Tarantool and server-side equality of the values we write.

## Design

```
callers ──encode under lock──▶ State { wbuf: Vec<u8>, waiters: sync→oneshot, next_sync }
   ▲                                 │ notify (only when wbuf was empty)
   │ oneshot<Reply>                  ▼
reader task ◀── socket ◀── server ◀── writer task: swap wbuf out, one write_all
```

* **Requests** are encoded straight into a shared `Vec<u8>` while holding a
  short `parking_lot` mutex. The same critical section assigns the
  `IPROTO_SYNC` and registers a `oneshot` sender in a sync-keyed map
  (multiplicative hasher). There is no per-request channel message and no
  intermediate buffer. The writer is notified only when the buffer goes from
  empty to non-empty.
* **Writer task:** it swaps the buffer with an empty spare and writes it with
  one `write_all`, so everything queued during the previous write goes out
  together. It has one non-obvious trick. The wake-up usually comes from the
  first of many tasks the reader just completed, and tokio's LIFO slot would
  run the writer right after that one task, so each write carried about 1.7
  requests. When replies are still outstanding (`waiters > requests already
  buffered`), the writer now calls `yield_now()` once before taking the
  buffer. With a single request in flight there is nothing to wait for, so it
  flushes immediately and adds no latency at concurrency 1.
* **Reader task:** it reads into a 128 KiB `BytesMut`, splits each complete
  packet off as a frozen `Bytes` (no copy), validates the header and the body
  envelope, and completes all waiters from one read under a single lock
  acquisition. Error messages are decoded later by the waiting caller.
  Every frame-length encoding is checked against `BODY_LEN_MAX` before
  reserving payload memory. Type and SYNC must be present exactly once;
  only OK and the database-error family are terminal. A nonempty body is
  one complete MessagePack map without trailing bytes.
  `IPROTO_CHUNK` pushes are skipped only for an outstanding SYNC, including
  a cancelled caller's retained entry; they never complete or fence a request.
  The exact terminal reply drains that entry even when its receiver is gone.
  Unknown/duplicate SYNCs, unsupported response types and malformed frames
  retire the connection. No part of a batch with ambiguous correlation is
  published as a database result. The handshake also requires unique owned
  terminal replies.
* **Connection death:** the reader or writer hits EOF or an error,
  `close()` is called explicitly, or the last `Client` clone is dropped. In
  each case the state is marked closed under the same lock used to register
  waiters. Then the waiter map is dropped, so every pending caller gets
  `Error::Closed`, both tasks are aborted and the socket closes; the server
  then rolls back open stream transactions. Later calls fail before encoding
  anything.
  For a sent mutation, this failure is transport uncertainty, not a matched
  retryable database error: the SQL layer reports 1180/40003 and never
  replays it. These checks do not add admission limits or post-connect
  deadlines; that separate work is tracked in [#127](https://github.com/cstheorist/tarn/issues/127).
* **Ordering:** packets enter the buffer in call order. Tarantool runs the
  requests of one stream strictly in order, so `begin`, writes and `commit`
  may even be issued concurrently on the same stream.
* **Memory:** zero-copy `Bytes` (response bodies, `rows()` strings,
  `decode_*_bytes` values) keep their 128 KiB read chunk alive. Use the copying
  `decode_value` for values that are cached for a long time.

Why only `streams` + `transactions` in `IPROTO_ID`: `error_extension` would
turn CALL/EVAL errors into `MP_ERROR` objects, and the `*_tuple_extension`
features would return `MP_TUPLE` extensions with separate format maps instead
of plain arrays.

## Tests

`cargo test -p tarn-iproto` includes codec tests, nine in-memory async
transport tests and integration tests against Tarantool. The transport
controls cover fragmented/coalesced and reordered replies, header and body
corruption, oversized uint32/uint64 length prefixes before allocation,
cancelled ownership, pushes, duplicate/unknown SYNCs and reusable database
errors. Each integration test starts its own Tarantool
(`$TARANTOOL`, default `tarantool`) in a temp dir
with `tests/common/init.lua`: `listen = 127.0.0.1:0`,
`memtx_use_mvcc_engine = true`, user `tarn`/`tarn` with super grants, and test
spaces and functions. The server exits by itself if the test process dies.

* **Handshake:** greeting, ID features, auth as `tarn`; a bad password and an
  unknown user both give `ER_CREDS_MISMATCH`; guest access is denied; connect
  refused gives `Io`, a silent peer gives `Timeout`, and a non-Tarantool peer
  gives `Protocol`.
* **Select:** every iterator on a unique and a non-unique index, with limit
  and offset; `ER_NO_SUCH_SPACE`, `ER_NO_SUCH_INDEX_ID`, `ER_KEY_PART_TYPE`.
* **DML:** insert, replace, update (`+`, `=`, `-` with a negative field number,
  `!`, `#`, a missing key, `ER_UPDATE_ARG_TYPE`, `ER_CANT_UPDATE_PRIMARY_KEY`)
  and delete; duplicate key gives `ER_TUPLE_FOUND` with its message; also the
  raw `request()` path.
* **Call/eval:** multiple return values (scalar, table, tuple), a list of
  tuples, argument echo, dotted names, `ER_NO_SUCH_PROC`, `ER_PROC_LUA`, a
  custom `box.error` code, eval with arguments, syntax errors,
  `ER_FUNCTION_TX_ACTIVE`.
* **Streams:**
  - A write inside a stream is invisible to another connection, and to the
    same connection outside the stream, until commit.
  - Rollback discards the transaction, and a stream can be reused afterwards.
  - Begin/writes/commit issued concurrently on one stream apply in order.
  - Out-of-stream BEGIN/COMMIT/ROLLBACK and nested BEGIN give errors.
  - Every isolation level is accepted. `linearizable` is accepted, but reads
    then fail with `ER_UNSUPPORTED`, which proves the level reaches the
    server.
  - A `begin` timeout gives `ER_TRANSACTION_TIMEOUT`.
  - Conflicts give `ER_TRANSACTION_CONFLICT`: the stream reads a key, another
    connection writes it, and the stream's write + commit fail; also two
    streams writing the same key.
  - Dropping the client releases its stream and connection on the server
    (checked through `box.stat.net()`).
* **Pipelining:** 20,000 concurrent tasks on one connection (selects, calls
  and evals, each answer checked); 50 tasks doing write-then-read in private
  key ranges; 3,000 in-flight futures inside one task; 4 MB requests and a
  2.3 MB reply while small requests interleave.
* **Stale leaders:** an `ER_READONLY` reply marks the connection stale,
  other errors do not, and the connection keeps working.
* **Connection loss:**
  - SIGKILL the server while 50 calls are parked inside it and 8 tasks run
    selects in a loop: all 58 fail with `Error::Closed` within a few ms (no
    hangs). `is_closed()` is then true, later calls fail at once, and
    reconnecting gives `Io`.
  - `close()` fails pending requests the same way.
* **Values:**
  - Every `Value` variant (integer boundaries, `u64::MAX`, ±inf, NaN,
    decimals up to 76 digits, strings and binaries around every header-size
    boundary up to 70 KB) round-trips through a space. The server also sees
    our decimals as real `decimal` values equal to the text.
  - `decimal.new(...)` values written by Tarantool, including `-0`, `1E+3`
    and `1e-30`, decode correctly, as do decimal arithmetic results.
  - Datetimes (tz offset, named tz, pre-1970, microseconds), booleans, nested
    containers, UUID and interval extensions decode as documented.

## Benchmarks

`examples/bench.rs` starts its own Tarantool (or use `--addr`). N tasks do
random primary-key `select`s, decoding the row with `rows()`, then
`replace`s, over one connection. It reports ops/s, latency percentiles,
batching, and the CPU per op of both processes.

```
cargo run --release -p tarn-iproto --example bench -- --secs 4 --tasks 64
```

The machine has 2 vCPUs, shared with Tarantool's two threads and with other
jobs (load average about 3.4 during the run). Absolute ops/s are therefore
a floor; CPU per op is the more stable figure.

| workload (64 tasks, 1 conn) | ops/s | p50 | p99 | p99.9 | requests/write | replies/read | client CPU/op | Tarantool CPU/op |
|---|---|---|---|---|---|---|---|---|
| select by PK + decode | 650,343 | 92 µs | 177 µs | 295 µs | 17.3 | 39.3 | 0.68 µs | 0.84 µs |
| replace | 372,243 | 162 µs | 285 µs | 857 µs | 15.1 | 43.6 | 0.65 µs | 2.00 µs |

Effect of the writer's yield (selects, 2 s per run):

| tasks | no yield: ops/s, req/write | always yield | adaptive (shipped) |
|---|---|---|---|
| 1 | 77k, 1.0 | 66k, 1.0 | 81k, 1.0 |
| 4 | 155k, 1.0 | 174k, 2.1 | 160k, 1.8 |
| 16 | 286k, 1.2 | 434k, 7.9 | 416k, 6.4 |
| 256 | 661k (p99 745 µs), 2.3 | 888k (p99 441 µs), 45 | 861k (p99 445 µs), 49 |

Zero-copy vs copying decode (`examples/decode_bench.rs --sweep`, best of 7,
rows of 4 strings, ns per row): slicing wins at every size, including when
rows are dropped on another thread:

| string bytes | 4 | 16 | 32 | 64 | 256 |
|---|---|---|---|---|---|
| copy | 188 | 197 | 219 | 224 | 309 |
| zero-copy | 118 | 126 | 149 | 153 | 167 |

## Protocol notes / surprises

* Tarantool 3.9 speaks IPROTO protocol version 10. It ignores the
  client's declared version and only stores the declared feature set, which
  changes reply encodings (see above).
* A stream needs no setup: the server creates it on the first request with
  that id and frees it when no requests are queued and no transaction is
  open. COMMIT and ROLLBACK without a transaction succeed silently.
* Memtx MVCC aborts a reader's transaction as soon as a conflicting write
  commits. The stream's *next statement* already fails with
  `ER_TRANSACTION_CONFLICT`, not only the commit. A later commit then fails
  the same way, and a rollback succeeds.
* A BEGIN timeout aborts the transaction on the server. Every later request in
  the stream, including COMMIT, gets `ER_TRANSACTION_TIMEOUT`.
* `IPROTO_UPDATE` without `IPROTO_INDEX_BASE` uses 0-based field numbers.
  net.box always sends `INDEX_BASE = 1`, and so does this client.
* The default `memtx_max_tuple_size` is 1 MB; larger tuples fail with
  error 110.
* The server applies backpressure with `net_msg_max` (default 768 in-flight
  messages per iproto thread). It stops reading the socket, so 20k
  outstanding requests simply queue in TCP and the client's buffer.
