//! The pipelined IPROTO connection.
//!
//! Design (see README.md for the long version):
//!
//! * Callers encode their packet *directly* into a shared write buffer while
//!   holding a short `parking_lot` mutex, register a `oneshot` waiter under
//!   the request's `IPROTO_SYNC` in the same critical section, and wake the
//!   writer task only when the buffer goes from empty to non-empty.
//! * The writer task swaps the whole buffer out and writes it with one
//!   `write_all`, so every request queued while the previous write was in
//!   flight goes out in a single syscall.
//! * The reader task reads into a large `BytesMut`, splits off each complete
//!   packet as a frozen `Bytes` (no copy), validates its header and body envelope, and
//!   completes all waiters of one read under a single lock acquisition.
//!   Error bodies are decoded by the waiting caller, not by the reader.
//! * When either task fails, the connection is marked closed under the same
//!   lock that guards waiter registration, all waiters are dropped (their
//!   callers observe [`Error::Closed`]) and later calls fail immediately.

use crate::admission::{Class, Permit, Scopes};
use crate::{AdmissionStats, Budget};
use std::collections::{HashMap, VecDeque};
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::Engine as _;
use bytes::{Buf, Bytes, BytesMut};
use parking_lot::Mutex;
use sha1::{Digest, Sha1};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UnixStream};
use tokio::sync::{oneshot, Notify};
use tokio::task::AbortHandle;

use crate::iproto::{self, key};
use crate::mp;
use crate::{Error, Iter, Opts, TxnIsolation};

#[cfg(test)]
mod tests;

/// Read buffer growth step.
const READ_CHUNK: usize = 128 * 1024;
/// Minimum free space before a read; below this the buffer is grown.
const READ_MIN_FREE: usize = 16 * 1024;
/// Initial write buffer capacity.
const WRITE_INITIAL: usize = 64 * 1024;
/// A write buffer that grew beyond this is not kept for reuse.
const WRITE_RETAIN_MAX: usize = 4 * 1024 * 1024;

/// Multiplicative hasher for the sync → waiter map (keys are sequential).
#[derive(Default)]
struct SyncHasher(u64);

impl Hasher for SyncHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = (self.0.rotate_left(8) ^ b as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        }
    }
    #[inline]
    fn write_u64(&mut self, v: u64) {
        self.0 = v.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    }
}

type Waiters = HashMap<u64, Waiter, BuildHasherDefault<SyncHasher>>;

struct Waiter {
    tx: oneshot::Sender<Reply>,
    permit: Permit,
    born: Instant,
    wire_bytes: usize,
}

// The socket halves drop before their guards. Retiring requests keep their
// shared scope charges until BOTH actual I/O owners have been dropped.
struct IoOwner<T> {
    io: T,
    _guard: IoGuard,
}
struct IoGuard(Arc<Physical>);
struct Physical {
    state: Mutex<(usize, Vec<Permit>)>,
}
impl Physical {
    fn retire(&self, permits: impl Iterator<Item = Permit>) {
        let mut s = self.state.lock();
        if s.0 != 0 {
            s.1.extend(permits);
        }
        // Dropping an unconsumed owning iterator also releases its permits.
    }
}
impl Drop for IoGuard {
    fn drop(&mut self) {
        let retired = {
            let mut s = self.0.state.lock();
            s.0 -= 1;
            if s.0 == 0 {
                std::mem::take(&mut s.1)
            } else {
                Vec::new()
            }
        };
        drop(retired);
    }
}

/// A raw reply as handed from the reader task to the waiting caller.
struct Reply {
    rtype: u32,
    schema_version: u64,
    body: Bytes,
}

impl Reply {
    fn into_response(self) -> Result<Response, Error> {
        if !terminal_type(self.rtype) {
            Err(Error::Protocol(format!(
                "unsupported terminal response type {}",
                self.rtype
            )))
        } else if self.rtype & iproto::TYPE_ERROR != 0 {
            Err(Error::Server {
                code: self.rtype & 0x7fff,
                message: error_message(&self.body),
            })
        } else {
            Ok(Response {
                schema_version: self.schema_version,
                body: self.body,
            })
        }
    }
}

/// Extract the error message from an error response body: `IPROTO_ERROR_24`
/// if present, otherwise the first entry of the `IPROTO_ERROR` stack.
fn error_message(body: &[u8]) -> String {
    let mut msg24: Option<String> = None;
    let mut msg_stack: Option<String> = None;
    let mut p = body;
    let n = mp::read_map_len(&mut p).unwrap_or(0);
    for _ in 0..n {
        let Ok(k) = mp::read_uint(&mut p) else { break };
        if k == key::ERROR_24 as u64 {
            match mp::read_str(&mut p) {
                Ok(s) => msg24 = Some(String::from_utf8_lossy(s).into_owned()),
                Err(_) => break,
            }
        } else if k == key::ERROR as u64 {
            let Ok(raw) = mp::read_raw(&mut p) else { break };
            msg_stack = stack_message(raw);
        } else if mp::skip_value(&mut p).is_err() {
            break;
        }
    }
    match (msg24, msg_stack) {
        (Some(m), _) if !m.is_empty() => m,
        (_, Some(m)) => m,
        (Some(m), None) => m,
        (None, None) => "unknown Tarantool error".to_string(),
    }
}

/// `{MP_ERROR_STACK: [{..., MP_ERROR_MESSAGE: str, ...}, ...]}` → message of
/// the first (outermost) error.
fn stack_message(mut p: &[u8]) -> Option<String> {
    let n = mp::read_map_len(&mut p).ok()?;
    for _ in 0..n {
        let k = mp::read_uint(&mut p).ok()?;
        if k == iproto::error_key::STACK as u64 {
            let cnt = mp::read_array_len(&mut p).ok()?;
            if cnt == 0 {
                return None;
            }
            let m = mp::read_map_len(&mut p).ok()?;
            for _ in 0..m {
                let k = mp::read_uint(&mut p).ok()?;
                if k == iproto::error_key::MESSAGE as u64 {
                    return Some(String::from_utf8_lossy(mp::read_str(&mut p).ok()?).into_owned());
                }
                mp::skip_value(&mut p).ok()?;
            }
            return None;
        }
        mp::skip_value(&mut p).ok()?;
    }
    None
}

/// A successful response.
#[derive(Clone, Debug)]
pub struct Response {
    /// `IPROTO_SCHEMA_VERSION` from the response header.
    pub schema_version: u64,
    /// The raw response body (a MessagePack map; empty for some replies).
    /// Shares memory with the connection's read buffer.
    pub body: Bytes,
}

impl Response {
    /// The raw MessagePack value of `IPROTO_DATA` (0x30), as a zero-copy
    /// slice of [`Response::body`].
    pub fn data(&self) -> Option<Bytes> {
        body_field(&self.body, key::DATA).map(|s| self.body.slice_ref(s))
    }

    /// Each element of the `IPROTO_DATA` array as a raw MessagePack slice
    /// (zero-copy). Returns an empty vector when the body has no data.
    pub fn tuples(&self) -> Result<Vec<Bytes>, Error> {
        let Some(data) = body_field(&self.body, key::DATA) else {
            return Ok(Vec::new());
        };
        let mut p = data;
        let n = mp::read_array_len(&mut p)? as usize;
        let mut out = Vec::with_capacity(n.min(p.len()));
        for _ in 0..n {
            let raw = mp::read_raw(&mut p)?;
            out.push(self.body.slice_ref(raw));
        }
        Ok(out)
    }

    /// Decode every tuple of `IPROTO_DATA` into values. Strings are zero-copy
    /// slices of the response body.
    pub fn rows(&self) -> Result<Vec<Vec<tarn_common::Value>>, Error> {
        let Some(data) = body_field(&self.body, key::DATA) else {
            return Ok(Vec::new());
        };
        let mut p = data;
        let n = mp::read_array_len(&mut p)? as usize;
        let mut out = Vec::with_capacity(n.min(p.len()));
        for _ in 0..n {
            out.push(mp::decode_tuple_shared(&self.body, &mut p)?);
        }
        Ok(out)
    }
}

/// Find the raw value stored under integer key `k` in a body map.
fn body_field(body: &[u8], k: u8) -> Option<&[u8]> {
    let mut p = body;
    let n = mp::read_map_len(&mut p).ok()?;
    for _ in 0..n {
        match p.first() {
            Some(&b) if b < 0x80 => {
                p = &p[1..];
                if b == k {
                    return mp::read_raw(&mut p).ok();
                }
            }
            _ => {
                mp::skip_value(&mut p).ok()?;
            }
        }
        mp::skip_value(&mut p).ok()?;
    }
    None
}

/// What the server told us during the handshake.
#[derive(Clone, Debug, Default)]
pub struct ServerInfo {
    /// Version from the greeting, e.g. `"3.9.0"`.
    pub version: String,
    /// `IPROTO_VERSION` from the `IPROTO_ID` reply (0 if not supported).
    pub protocol_version: u64,
    /// `IPROTO_FEATURES` from the `IPROTO_ID` reply.
    pub features: Vec<u32>,
    /// `IPROTO_AUTH_TYPE` from the `IPROTO_ID` reply.
    pub auth_type: String,
}

struct State {
    /// Encoded packets not yet handed to the writer.
    wbuf: Vec<u8>,
    /// Number of packets in `wbuf`.
    wbuf_requests: usize,
    /// Requests waiting for a reply, by sync.
    waiters: Waiters,
    order: VecDeque<u64>,
    next_sync: u64,
    closed: bool,
}

impl State {
    fn oldest(&mut self) -> Option<Instant> {
        while let Some(&sync) = self.order.front() {
            if let Some(w) = self.waiters.get(&sync) {
                return Some(w.born);
            }
            self.order.pop_front();
        }
        None
    }
}

pub(crate) struct Shared {
    id: u64,
    owner: Option<u32>,
    state: Mutex<State>,
    /// Wakes the writer: data available or connection closed.
    write_ready: Notify,
    deadline_ready: Notify,
    request_timeout: Duration,
    budget: Budget,
    parents: Vec<Budget>,
    scopes: Arc<Scopes>,
    physical: Arc<Physical>,
    closed: AtomicBool,
    /// The server answered a request with ER_READONLY: it is no longer the
    /// leader. The connection still works, but callers that need the leader
    /// should stop using it (see [`Client::is_stale`]).
    stale: AtomicBool,
    /// Marks the connection's user set: what it learned of the server, such
    /// as a procedure it lacks (see [`Client::marked`]).
    marks: AtomicU32,
    tasks: Mutex<Vec<AbortHandle>>,
    counters: Counters,
}

/// Updated once per read/write batch, not per request.
#[derive(Default)]
struct Counters {
    replies: AtomicU64,
    write_batches: AtomicU64,
    read_batches: AtomicU64,
    bytes_written: AtomicU64,
    bytes_read: AtomicU64,
}

/// Connection counters, monotonic since connect (handshake excluded).
/// `requests / write_batches` is the average number of requests coalesced
/// into one `write`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    pub id: u64,
    pub owner: Option<u32>,
    pub closed: bool,
    /// Requests queued.
    pub requests: u64,
    /// Final replies received.
    pub replies: u64,
    /// Buffers flushed by the writer task (one `write_all` each).
    pub write_batches: u64,
    /// Socket reads that returned data.
    pub read_batches: u64,
    pub bytes_written: u64,
    pub bytes_read: u64,
    pub admission: AdmissionStats,
    /// Exact encoded bytes still waiting for a terminal reply.
    pub pending_wire_bytes: usize,
    pub abandoned: usize,
    pub oldest_ms: u64,
    /// Refusals at any scope, before this connection queued the request.
    pub refusals: u64,
}

impl Shared {
    pub(crate) fn quiet_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire) && self.budget.stats().pending == 0
    }

    pub(crate) fn snapshot(&self) -> Stats {
        let c = &self.counters;
        let mut st = self.state.lock();
        let oldest_ms = st
            .oldest()
            .map_or(0, |b| b.elapsed().as_millis().min(u64::MAX as u128) as u64);
        let admission = self.budget.stats();
        Stats {
            id: self.id,
            owner: self.owner,
            closed: st.closed,
            requests: st.next_sync - FIRST_SYNC,
            replies: c.replies.load(Ordering::Relaxed),
            write_batches: c.write_batches.load(Ordering::Relaxed),
            read_batches: c.read_batches.load(Ordering::Relaxed),
            bytes_written: c.bytes_written.load(Ordering::Relaxed),
            bytes_read: c.bytes_read.load(Ordering::Relaxed),
            pending_wire_bytes: st.waiters.values().map(|w| w.wire_bytes).sum(),
            abandoned: st.waiters.values().filter(|w| w.tx.is_closed()).count(),
            oldest_ms,
            refusals: admission.request_refusals,
            admission,
        }
    }

    /// Mark the connection dead, fail every waiter, stop both tasks.
    fn close(&self, why: &dyn std::fmt::Display) -> bool {
        self.retire(why, false)
    }

    fn retire(&self, why: &dyn std::fmt::Display, deadline: bool) -> bool {
        let waiters = {
            let mut st = self.state.lock();
            if st.closed {
                return false;
            }
            st.closed = true;
            self.closed.store(true, Ordering::Release);
            st.wbuf = Vec::new();
            std::mem::take(&mut st.waiters)
        };
        if deadline {
            self.budget.deadline_retired();
            for b in &self.parents {
                b.deadline_retired();
            }
        }
        tracing::debug!(pending = waiters.len(), "tarantool connection closed: {why}");
        // Fail callers now, but retain capacity until the socket halves
        // have actually dropped. A cancelled caller cannot free its charge.
        self.physical.retire(waiters.into_values().map(|w| {
            drop(w.tx);
            w.permit
        }));
        self.write_ready.notify_one();
        for t in self.tasks.lock().drain(..) {
            t.abort();
        }
        true
    }
}

struct Inner {
    shared: Arc<Shared>,
    next_stream: AtomicU64,
    info: ServerInfo,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.shared.close(&"client dropped");
    }
}

/// A pipelined connection to one Tarantool instance. Cheap to clone; all
/// clones share the connection, which is closed when the last clone drops.
#[derive(Clone)]
pub struct Client {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("server", &self.inner.info.version)
            .field("closed", &self.is_closed())
            .finish()
    }
}

/// Append one complete packet: `0xce <u32 len> {header} body`.
#[inline]
fn encode_packet(out: &mut Vec<u8>, req_type: u32, sync: u64, stream_id: u64, body: impl FnOnce(&mut Vec<u8>)) {
    let start = out.len();
    out.extend_from_slice(&[0xce, 0, 0, 0, 0]);
    out.push(if stream_id != 0 { 0x83 } else { 0x82 });
    out.push(key::REQUEST_TYPE);
    mp::write_uint(out, req_type as u64);
    out.push(key::SYNC);
    mp::write_uint(out, sync);
    if stream_id != 0 {
        out.push(key::STREAM_ID);
        mp::write_uint(out, stream_id);
    }
    body(out);
    let len = (out.len() - start - 5) as u32;
    out[start + 1..start + 5].copy_from_slice(&len.to_be_bytes());
}

/// A parsed packet header plus its body.
struct Frame {
    rtype: u32,
    sync: u64,
    schema_version: u64,
    body: Bytes,
}

/// Only OK and the 15-bit database-error family are terminal responses.
/// Request codes, events and future nonfinal statuses cannot certify a result.
fn terminal_type(rtype: u32) -> bool {
    rtype == iproto::OK || (iproto::TYPE_ERROR..=0xffff).contains(&rtype)
}

/// Split one complete packet off the front of `buf`, if there is one. When
/// the packet is incomplete, makes sure `buf` has room for all of it.
fn next_frame(buf: &mut BytesMut) -> Result<Option<Frame>, Error> {
    let b = &buf[..];
    let Some(&first) = b.first() else { return Ok(None) };
    let (plen, len) = match first {
        0x00..=0x7f => (1usize, first as u64),
        0xcc if b.len() >= 2 => (2, b[1] as u64),
        0xcd if b.len() >= 3 => (3, u16::from_be_bytes([b[1], b[2]]) as u64),
        0xce if b.len() >= 5 => (5, u32::from_be_bytes([b[1], b[2], b[3], b[4]]) as u64),
        0xcf if b.len() >= 9 => (9, u64::from_be_bytes(b[1..9].try_into().unwrap())),
        0xcc..=0xcf => return Ok(None),
        _ => return Err(Error::Protocol(format!("invalid packet length prefix 0x{first:02x}"))),
    };
    // Check every encoding before narrowing or reserving, including an
    // incomplete uint32-prefixed reply advertising more than BODY_LEN_MAX.
    if len > iproto::BODY_LEN_MAX as u64 {
        return Err(Error::Protocol(format!("packet too large: {len} bytes")));
    }
    let total = plen
        .checked_add(len as usize)
        .ok_or_else(|| Error::Protocol("packet length overflow".into()))?;
    if b.len() < total {
        if buf.capacity() < total {
            buf.reserve(total - buf.len());
        }
        return Ok(None);
    }
    let mut pkt = buf.split_to(total);
    pkt.advance(plen);
    let pkt = pkt.freeze();
    let mut p: &[u8] = &pkt;
    let mut rtype = None;
    let mut sync = None;
    let mut schema_version = None;
    let n = mp::read_map_len(&mut p)?;
    for _ in 0..n {
        let k = mp::read_uint(&mut p)?;
        match k {
            k if k == key::REQUEST_TYPE as u64 => {
                let v = mp::read_uint(&mut p)?;
                let v = u32::try_from(v).map_err(|_| Error::Protocol(format!("bad response type {v}")))?;
                if rtype.replace(v).is_some() {
                    return Err(Error::Protocol("duplicate response type".into()));
                }
            }
            k if k == key::SYNC as u64 => {
                if sync.replace(mp::read_uint(&mut p)?).is_some() {
                    return Err(Error::Protocol("duplicate response SYNC".into()));
                }
            }
            k if k == key::SCHEMA_VERSION as u64 => {
                if schema_version.replace(mp::read_uint(&mut p)?).is_some() {
                    return Err(Error::Protocol("duplicate schema version".into()));
                }
            }
            _ => mp::skip_value(&mut p)?,
        }
    }
    let rtype = rtype.ok_or_else(|| Error::Protocol("missing response type".into()))?;
    let sync = sync.ok_or_else(|| Error::Protocol("missing response SYNC".into()))?;
    if !terminal_type(rtype) && rtype != iproto::CHUNK {
        return Err(Error::Protocol(format!("unsupported response type {rtype}")));
    }
    // Empty bodies are valid for replies such as PING. Otherwise one complete
    // map must occupy the rest of the frame; a valid prefix is not a reply.
    if !p.is_empty() {
        let mut fields = p;
        mp::read_map_len(&mut fields)?;
        let mut tail = p;
        mp::skip_value(&mut tail)?;
        if !tail.is_empty() {
            return Err(Error::Protocol("trailing response body bytes".into()));
        }
    }
    let hdr_len = pkt.len() - p.len();
    let body = pkt.slice(hdr_len..);
    Ok(Some(Frame {
        rtype,
        sync,
        schema_version: schema_version.unwrap_or(0),
        body,
    }))
}

async fn reader_task<R: AsyncRead + Unpin>(shared: Arc<Shared>, mut rd: IoOwner<R>, mut buf: BytesMut) {
    let mut done: Vec<Frame> = Vec::with_capacity(64);
    let mut ready: Vec<(Waiter, Frame)> = Vec::with_capacity(64);
    let why: Error = 'outer: loop {
        loop {
            match next_frame(&mut buf) {
                Ok(Some(f)) => done.push(f),
                Ok(None) => break,
                Err(e) => break 'outer e,
            }
        }
        if !done.is_empty() {
            let bad_sync = {
                let mut st = shared.state.lock();
                let mut bad_sync = None;
                for f in done.drain(..) {
                    // A cancelled caller still owns its entry until a final
                    // reply or connection retirement. CHUNK never removes it.
                    if f.rtype == iproto::CHUNK && st.waiters.contains_key(&f.sync) {
                        continue;
                    }
                    let Some(tx) = st.waiters.remove(&f.sync) else {
                        bad_sync = Some(f.sync);
                        break;
                    };
                    ready.push((tx, f));
                }
                bad_sync
            };
            if let Some(sync) = bad_sync {
                // Do not publish any part of an ambiguously correlated batch.
                // Dropping these senders and retiring the rest is uncertainty,
                // never a matched database error that can authorize a retry.
                shared.close(&format!("unknown or duplicate response SYNC {sync}"));
                shared.physical.retire(ready.drain(..).map(|(w, _)| {
                    drop(w.tx);
                    w.permit
                }));
                return;
            }
            shared.counters.replies.fetch_add(ready.len() as u64, Ordering::Relaxed);
            for (w, f) in ready.drain(..) {
                drop(w.permit); // an exact terminal was drained
                let _ = w.tx.send(Reply {
                    rtype: f.rtype,
                    schema_version: f.schema_version,
                    body: f.body,
                });
            }
        }
        if buf.capacity() - buf.len() < READ_MIN_FREE {
            buf.reserve(READ_CHUNK);
        }
        match rd.io.read_buf(&mut buf).await {
            Ok(0) => break Error::Io(std::io::ErrorKind::UnexpectedEof.into()),
            Ok(n) => {
                shared.counters.read_batches.fetch_add(1, Ordering::Relaxed);
                shared.counters.bytes_read.fetch_add(n as u64, Ordering::Relaxed);
            }
            Err(e) => break Error::Io(e),
        }
    };
    shared.close(&why);
}

async fn writer_task<W: AsyncWrite + Unpin>(shared: Arc<Shared>, mut wr: IoOwner<W>) {
    let mut buf: Vec<u8> = Vec::with_capacity(WRITE_INITIAL);
    loop {
        shared.write_ready.notified().await;
        // The notification usually comes from the first of several tasks
        // that the reader just woke, and tokio's LIFO slot runs us right
        // after it. If replies are still outstanding (so more callers are
        // likely runnable or about to be), yield once so they can enqueue
        // first and everything goes out in one write. With a single request
        // in flight there is nobody to wait for, so flush immediately.
        let wait_for_more = {
            let st = shared.state.lock();
            if st.closed {
                break;
            }
            st.waiters.len() > st.wbuf_requests
        };
        if wait_for_more {
            tokio::task::yield_now().await;
        }
        {
            let mut st = shared.state.lock();
            if st.closed {
                break;
            }
            std::mem::swap(&mut st.wbuf, &mut buf);
            st.wbuf_requests = 0;
        }
        if buf.is_empty() {
            continue;
        }
        if let Err(e) = wr.io.write_all(&buf).await {
            shared.close(&e);
            break;
        }
        if let Err(e) = wr.io.flush().await {
            shared.close(&e);
            break;
        }
        shared.counters.write_batches.fetch_add(1, Ordering::Relaxed);
        shared
            .counters
            .bytes_written
            .fetch_add(buf.len() as u64, Ordering::Relaxed);
        buf.clear();
        if buf.capacity() > WRITE_RETAIN_MAX {
            buf = Vec::with_capacity(WRITE_INITIAL);
        }
    }
}

// A later request can never have an earlier deadline. Keep an existing
// timer when its head drains: it may wake early, then inspect the new head.
// This avoids waking/resetting a third task on every normal reply batch.
async fn deadline_task(shared: Arc<Shared>) {
    loop {
        let born = {
            let mut st = shared.state.lock();
            if st.closed {
                return;
            }
            st.oldest()
        };
        let Some(born) = born else {
            shared.deadline_ready.notified().await;
            continue;
        };
        let due = born + shared.request_timeout;
        tokio::time::sleep_until(due.into()).await;
        let expired = {
            let mut st = shared.state.lock();
            !st.closed && st.oldest().is_some_and(|b| b.elapsed() >= shared.request_timeout)
        };
        if expired {
            // Only this monitor reports deadline retirement. Other close
            // causes can race; no second deadline monitor exists.
            shared.retire(&"oldest pending RPC exceeded its deadline", true);
            return;
        }
    }
}

/// chap-sha1: `sha1(password) XOR sha1(salt[..20] ++ sha1(sha1(password)))`.
fn chap_sha1_scramble(salt: &[u8], password: &str) -> [u8; iproto::SCRAMBLE_SIZE] {
    let hash1: [u8; 20] = Sha1::digest(password.as_bytes()).into();
    let hash2: [u8; 20] = Sha1::digest(hash1).into();
    let mut h = Sha1::new();
    h.update(&salt[..iproto::SCRAMBLE_SIZE]);
    h.update(hash2);
    let salted: [u8; 20] = h.finalize().into();
    let mut out = [0u8; 20];
    for i in 0..20 {
        out[i] = salted[i] ^ hash1[i];
    }
    out
}

/// Parse the 128-byte greeting: `"Tarantool <version> (Binary) <uuid>"`,
/// newline, base64 salt, newline. Returns (version, salt).
fn parse_greeting(g: &[u8; iproto::GREETING_SIZE]) -> Result<(String, Vec<u8>), Error> {
    let h = iproto::GREETING_SIZE / 2;
    if !g.starts_with(b"Tarantool ") || g[h - 1] != b'\n' || g[2 * h - 1] != b'\n' {
        return Err(Error::Protocol("not a Tarantool greeting".into()));
    }
    let line1 = String::from_utf8_lossy(&g[..h - 1]);
    let mut words = line1.split_ascii_whitespace();
    words.next();
    let version = words.next().unwrap_or("").to_string();
    if !line1.contains("(Binary)") {
        return Err(Error::Protocol(format!("not a binary protocol port: {}", line1.trim())));
    }
    let salt_b64 = std::str::from_utf8(&g[h..2 * h - 1])
        .map_err(|_| Error::Protocol("bad greeting salt".into()))?
        .trim();
    let salt = base64::engine::general_purpose::STANDARD
        .decode(salt_b64)
        .map_err(|e| Error::Protocol(format!("bad greeting salt: {e}")))?;
    if salt.len() < iproto::SCRAMBLE_SIZE {
        return Err(Error::Protocol("greeting salt too short".into()));
    }
    Ok((version, salt))
}

/// Parse the body of the `IPROTO_ID` reply.
fn parse_id_body(body: &[u8], info: &mut ServerInfo) -> Result<(), Error> {
    let mut p = body;
    if p.is_empty() {
        return Ok(());
    }
    let n = mp::read_map_len(&mut p)?;
    for _ in 0..n {
        let k = mp::read_uint(&mut p)?;
        if k == key::VERSION as u64 {
            info.protocol_version = mp::read_uint(&mut p)?;
        } else if k == key::FEATURES as u64 {
            let cnt = mp::read_array_len(&mut p)?;
            for _ in 0..cnt {
                info.features.push(mp::read_uint(&mut p)? as u32);
            }
        } else if k == key::AUTH_TYPE as u64 {
            info.auth_type = String::from_utf8_lossy(mp::read_str(&mut p)?).into_owned();
        } else {
            mp::skip_value(&mut p)?;
        }
    }
    Ok(())
}

const SYNC_ID: u64 = 1;
const SYNC_AUTH: u64 = 2;
/// First sync used after the handshake.
const FIRST_SYNC: u64 = 3;
static NEXT_CONNECTION: AtomicU64 = AtomicU64::new(1);

impl Client {
    /// Connect, read the greeting, send `IPROTO_ID` (protocol version and the
    /// `streams` + `transactions` features) and, if `opts.user` is set, a
    /// chap-sha1 `IPROTO_AUTH` (ID and AUTH are pipelined in one write).
    ///
    /// The whole handshake is bounded by `opts.connect_timeout`
    /// ([`Error::Timeout`]); a zero timeout means no limit.
    pub async fn connect(opts: &Opts) -> Result<Client, Error> {
        if opts.connect_timeout.is_zero() {
            return Self::connect_inner(opts).await;
        }
        match tokio::time::timeout(opts.connect_timeout, Self::connect_inner(opts)).await {
            Ok(r) => r,
            Err(_) => Err(Error::Timeout),
        }
    }

    async fn connect_inner(opts: &Opts) -> Result<Client, Error> {
        if opts.addr.starts_with("tls://") {
            let tls = opts
                .tls
                .clone()
                .ok_or_else(|| Error::Protocol("TLS endpoint requires a trusted CA configuration".into()))?;
            let stream = tarn_transport::connect(&opts.addr, tls).await?;
            let (rd, wr) = tokio::io::split(stream);
            return Self::start(opts, rd, wr).await;
        }
        // "unix/:/path" (Tarantool's notation) or a bare absolute path
        // selects a Unix domain socket; anything else is host:port.
        let unix_path = opts
            .addr
            .strip_prefix("unix/:")
            .or_else(|| opts.addr.starts_with('/').then_some(opts.addr.as_str()));
        if let Some(path) = unix_path {
            let (rd, wr) = UnixStream::connect(path).await?.into_split();
            return Self::start(opts, rd, wr).await;
        }
        if opts.tls.is_some() {
            return Err(Error::Protocol(
                "TLS configuration cannot be used with a plaintext TCP endpoint".into(),
            ));
        }
        let stream = TcpStream::connect(opts.addr.as_str()).await?;
        stream.set_nodelay(true)?;
        let (rd, wr) = stream.into_split();
        Self::start(opts, rd, wr).await
    }

    async fn start<R, W>(opts: &Opts, mut rd: R, mut wr: W) -> Result<Client, Error>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let mut greeting = [0u8; iproto::GREETING_SIZE];
        rd.read_exact(&mut greeting).await?;
        let (version, salt) = parse_greeting(&greeting)?;

        let mut out = Vec::with_capacity(256);
        encode_packet(&mut out, iproto::ID, SYNC_ID, 0, |o| {
            o.push(0x82);
            o.push(key::VERSION);
            mp::write_uint(o, iproto::CLIENT_PROTOCOL_VERSION);
            o.push(key::FEATURES);
            mp::write_array_len(o, iproto::CLIENT_FEATURES.len() as u32);
            for f in iproto::CLIENT_FEATURES {
                mp::write_uint(o, f as u64);
            }
        });
        if let Some(user) = &opts.user {
            let scramble = chap_sha1_scramble(&salt, opts.password.as_deref().unwrap_or(""));
            encode_packet(&mut out, iproto::AUTH, SYNC_AUTH, 0, |o| {
                o.push(0x82);
                o.push(key::USER_NAME);
                mp::write_str(o, user.as_bytes());
                o.push(key::TUPLE);
                o.push(0x92);
                mp::write_str(o, b"chap-sha1");
                mp::write_str(o, &scramble);
            });
        }
        wr.write_all(&out).await?;
        wr.flush().await?;

        let mut buf = BytesMut::with_capacity(READ_CHUNK);
        let mut id_reply: Option<Frame> = None;
        let mut auth_reply: Option<Frame> = None;
        let want_auth = opts.user.is_some();
        while id_reply.is_none() || (want_auth && auth_reply.is_none()) {
            match next_frame(&mut buf)? {
                Some(f) if !terminal_type(f.rtype) => {
                    return Err(Error::Protocol("nonterminal handshake reply".into()));
                }
                Some(f) if f.sync == SYNC_ID && id_reply.is_none() => id_reply = Some(f),
                Some(f) if f.sync == SYNC_AUTH && want_auth && auth_reply.is_none() => auth_reply = Some(f),
                Some(f) => return Err(Error::Protocol(format!("unexpected handshake SYNC {}", f.sync))),
                None => {
                    if rd.read_buf(&mut buf).await? == 0 {
                        return Err(Error::Io(std::io::ErrorKind::UnexpectedEof.into()));
                    }
                }
            }
        }

        let mut info = ServerInfo {
            version,
            auth_type: "chap-sha1".into(),
            ..Default::default()
        };
        let id = id_reply.expect("loop exits only with an ID reply");
        let id = Reply {
            rtype: id.rtype,
            schema_version: id.schema_version,
            body: id.body,
        };
        match id.into_response() {
            Ok(r) => parse_id_body(&r.body, &mut info)?,
            // Pre-2.10 servers do not know IPROTO_ID; carry on without
            // features (streams will then be rejected by the server).
            Err(Error::Server { code, .. }) if code == crate::ER_UNKNOWN_REQUEST_TYPE => {}
            Err(e) => return Err(e),
        }
        if let Some(a) = auth_reply {
            Reply {
                rtype: a.rtype,
                schema_version: a.schema_version,
                body: a.body,
            }
            .into_response()?;
        }

        opts.request_limits.validate()?;
        if opts.request_timeout.is_zero()
            || opts.request_timeout > Duration::from_secs(86400)
            || opts.admission.len() > 2
        {
            return Err(Error::Protocol(
                "RPC timeout must be in (0,86400] seconds with at most two enclosing scopes".into(),
            ));
        }
        let physical = Arc::new(Physical {
            state: Mutex::new((2, Vec::new())),
        });
        let budget = Budget::new("connection", opts.request_limits)?;
        let scopes = Scopes::new(&budget, &opts.admission)?;
        let shared = Arc::new(Shared {
            id: NEXT_CONNECTION.fetch_add(1, Ordering::Relaxed),
            owner: opts.observation_scope,
            state: Mutex::new(State {
                wbuf: Vec::with_capacity(WRITE_INITIAL),
                wbuf_requests: 0,
                waiters: Waiters::default(),
                order: VecDeque::new(),
                next_sync: FIRST_SYNC,
                closed: false,
            }),
            write_ready: Notify::new(),
            deadline_ready: Notify::new(),
            request_timeout: opts.request_timeout,
            budget,
            scopes,
            parents: opts.admission.clone(),
            physical: physical.clone(),
            closed: AtomicBool::new(false),
            stale: AtomicBool::new(false),
            marks: AtomicU32::new(0),
            tasks: Mutex::new(Vec::with_capacity(3)),
            counters: Counters::default(),
        });
        for scope in &shared.parents {
            scope.observe(&shared);
        }
        {
            // An immediately failing task must see all abort handles. No
            // lock is held across an await, and close takes state first.
            let mut tasks = shared.tasks.lock();
            let r = tokio::spawn(reader_task(
                shared.clone(),
                IoOwner {
                    io: rd,
                    _guard: IoGuard(physical.clone()),
                },
                buf,
            ));
            let w = tokio::spawn(writer_task(
                shared.clone(),
                IoOwner {
                    io: wr,
                    _guard: IoGuard(physical),
                },
            ));
            let d = tokio::spawn(deadline_task(shared.clone()));
            tasks.extend([r.abort_handle(), w.abort_handle(), d.abort_handle()]);
        }
        Ok(Client {
            inner: Arc::new(Inner {
                shared,
                next_stream: AtomicU64::new(1),
                info,
            }),
        })
    }

    /// Handshake information (server version, protocol version, features).
    pub fn server_info(&self) -> &ServerInfo {
        &self.inner.info
    }

    /// Traffic counters of this connection.
    pub fn stats(&self) -> Stats {
        self.inner.shared.snapshot()
    }

    /// A fresh stream id for this connection: starts at 1; 0 means "no
    /// stream". Server-side stream state lives until its transaction ends
    /// and it has no queued requests, so ids are never reused here.
    pub fn next_stream_id(&self) -> u64 {
        self.inner.next_stream.fetch_add(1, Ordering::Relaxed)
    }

    /// True once the connection has failed or been closed; every call on a
    /// closed client fails immediately with [`Error::Closed`].
    pub fn is_closed(&self) -> bool {
        self.inner.shared.closed.load(Ordering::Acquire)
    }

    /// True once the server answered some request with ER_READONLY (it was
    /// demoted, or is a replica), or after [`Client::mark_stale`].
    pub fn is_stale(&self) -> bool {
        self.inner.shared.stale.load(Ordering::Acquire)
    }

    pub fn mark_stale(&self) {
        self.inner.shared.stale.store(true, Ordering::Release);
    }

    /// Whether mark `bit` (0 to 31) is set on this connection: what its user
    /// learned of the server and noted with [`Client::mark`], such as a
    /// procedure the server lacks. A new connection has none.
    pub fn marked(&self, bit: u32) -> bool {
        self.inner.shared.marks.load(Ordering::Relaxed) & (1 << bit) != 0
    }

    pub fn mark(&self, bit: u32) {
        self.inner.shared.marks.fetch_or(1 << bit, Ordering::Relaxed);
    }

    /// Close the connection now. Pending and later requests fail with
    /// [`Error::Closed`]; the server rolls back open stream transactions.
    pub fn close(&self) {
        self.inner.shared.close(&"closed by client");
    }

    /// Queue one request and register its waiter. Encoding happens under the
    /// connection lock straight into the shared write buffer.
    #[inline]
    fn enqueue(
        &self,
        req_type: u32,
        stream_id: u64,
        charge: usize,
        class: Class,
        body: impl FnOnce(&mut Vec<u8>),
    ) -> Result<oneshot::Receiver<Reply>, Error> {
        let (tx, rx) = oneshot::channel();
        let shared = &self.inner.shared;
        let (was_empty, first_pending) = {
            let mut st = shared.state.lock();
            if st.closed {
                return Err(Error::Closed);
            }
            let permit = shared.scopes.acquire(charge, class).inspect_err(|_| {
                shared.budget.refused();
                for b in &shared.parents {
                    b.refused();
                }
            })?;
            let sync = st.next_sync;
            st.next_sync = st
                .next_sync
                .checked_add(1)
                .ok_or_else(|| Error::Protocol("request SYNC exhausted".into()))?;
            let was_empty = st.wbuf.is_empty();
            let first_pending = st.waiters.is_empty();
            let start = st.wbuf.len();
            encode_packet(&mut st.wbuf, req_type, sync, stream_id, body);
            let wire_bytes = st.wbuf.len() - start;
            if wire_bytes > charge {
                st.wbuf.truncate(start);
                return Err(Error::Protocol(
                    "internal request byte charge underestimated encoding".into(),
                ));
            }
            st.wbuf_requests += 1;
            st.waiters.insert(
                sync,
                Waiter {
                    tx,
                    permit,
                    born: Instant::now(),
                    wire_bytes,
                },
            );
            // Out-of-order replies must not leave an unbounded queue of old
            // identities behind one stalled request.
            if st.order.len() >= shared.budget.limits().requests.saturating_mul(2) {
                let State { order, waiters, .. } = &mut *st;
                order.retain(|id| waiters.contains_key(id));
            }
            st.order.push_back(sync);
            (was_empty, first_pending)
        };
        if was_empty {
            shared.write_ready.notify_one();
        }
        if first_pending {
            shared.deadline_ready.notify_one();
        }
        Ok(rx)
    }

    #[inline]
    async fn roundtrip(
        &self,
        req_type: u32,
        stream_id: u64,
        charge: usize,
        class: Class,
        body: impl FnOnce(&mut Vec<u8>),
    ) -> Result<Response, Error> {
        let rx = self.enqueue(req_type, stream_id, charge, class, body)?;
        match rx.await {
            Ok(reply) => {
                let r = reply.into_response();
                if let Err(Error::Server {
                    code: crate::ER_READONLY,
                    ..
                }) = &r
                {
                    self.inner.shared.stale.store(true, Ordering::Release);
                }
                r
            }
            Err(_) => Err(Error::Closed),
        }
    }

    /// Send an arbitrary request. `body` is a pre-encoded MessagePack map
    /// (or empty for no body); `stream_id` 0 means no stream.
    pub async fn request(&self, req_type: u32, stream_id: u64, body: Bytes) -> Result<Response, Error> {
        self.roundtrip(
            req_type,
            stream_id,
            body.len().saturating_add(128),
            Class::Normal,
            |out| out.extend_from_slice(&body),
        )
        .await
    }

    pub async fn ping(&self) -> Result<(), Error> {
        self.roundtrip(iproto::PING, 0, 128, Class::Normal, |_| {})
            .await
            .map(drop)
    }

    /// `IPROTO_SELECT`. `key` is a MessagePack array (empty slice = `[]`).
    #[allow(clippy::too_many_arguments)]
    pub async fn select(
        &self,
        stream_id: u64,
        space_id: u32,
        index_id: u32,
        iterator: Iter,
        offset: u32,
        limit: u32,
        key: &[u8],
    ) -> Result<Response, Error> {
        self.roundtrip(
            iproto::SELECT,
            stream_id,
            key.len().saturating_add(128),
            Class::Normal,
            |out| {
                out.push(0x86);
                out.push(key::SPACE_ID);
                mp::write_uint(out, space_id as u64);
                out.push(key::INDEX_ID);
                mp::write_uint(out, index_id as u64);
                out.push(key::ITERATOR);
                mp::write_uint(out, iterator as u64);
                out.push(key::OFFSET);
                mp::write_uint(out, offset as u64);
                out.push(key::LIMIT);
                mp::write_uint(out, limit as u64);
                out.push(key::KEY);
                put_array(out, key);
            },
        )
        .await
    }

    /// `IPROTO_INSERT` of a MessagePack array; the reply data is `[tuple]`.
    pub async fn insert(&self, stream_id: u64, space_id: u32, tuple: &[u8]) -> Result<Response, Error> {
        self.store(iproto::INSERT, stream_id, space_id, tuple).await
    }

    /// `IPROTO_REPLACE` of a MessagePack array; the reply data is `[tuple]`.
    pub async fn replace(&self, stream_id: u64, space_id: u32, tuple: &[u8]) -> Result<Response, Error> {
        self.store(iproto::REPLACE, stream_id, space_id, tuple).await
    }

    async fn store(&self, req_type: u32, stream_id: u64, space_id: u32, tuple: &[u8]) -> Result<Response, Error> {
        self.roundtrip(
            req_type,
            stream_id,
            tuple.len().saturating_add(128),
            Class::Normal,
            |out| {
                out.push(0x82);
                out.push(key::SPACE_ID);
                mp::write_uint(out, space_id as u64);
                out.push(key::TUPLE);
                put_array(out, tuple);
            },
        )
        .await
    }

    /// `IPROTO_UPDATE`. `ops` is a MessagePack array of operations such as
    /// `[["+", 2, 1], ["=", 3, "x"]]`. Field numbers are **1-based** (the
    /// request carries `IPROTO_INDEX_BASE = 1`, like `space:update()` in
    /// Lua and net.box); negative numbers count from the end. The reply data
    /// is `[new_tuple]`, or `[]` if no tuple matched `key`.
    pub async fn update(
        &self,
        stream_id: u64,
        space_id: u32,
        index_id: u32,
        key: &[u8],
        ops: &[u8],
    ) -> Result<Response, Error> {
        self.roundtrip(
            iproto::UPDATE,
            stream_id,
            key.len().saturating_add(ops.len()).saturating_add(128),
            Class::Normal,
            |out| {
                out.push(0x85);
                out.push(key::SPACE_ID);
                mp::write_uint(out, space_id as u64);
                out.push(key::INDEX_ID);
                mp::write_uint(out, index_id as u64);
                out.push(key::INDEX_BASE);
                out.push(1);
                out.push(key::KEY);
                put_array(out, key);
                out.push(key::TUPLE);
                put_array(out, ops);
            },
        )
        .await
    }

    /// `IPROTO_DELETE`; the reply data is `[old_tuple]` or `[]`.
    pub async fn delete(&self, stream_id: u64, space_id: u32, index_id: u32, key: &[u8]) -> Result<Response, Error> {
        self.roundtrip(
            iproto::DELETE,
            stream_id,
            key.len().saturating_add(128),
            Class::Normal,
            |out| {
                out.push(0x83);
                out.push(key::SPACE_ID);
                mp::write_uint(out, space_id as u64);
                out.push(key::INDEX_ID);
                mp::write_uint(out, index_id as u64);
                out.push(key::KEY);
                put_array(out, key);
            },
        )
        .await
    }

    /// `IPROTO_CALL` (type 10, not the legacy `CALL_16`): the reply data is
    /// an array of the function's return values, each as plain MessagePack.
    pub async fn call(&self, stream_id: u64, function: &str, args: &[u8]) -> Result<Response, Error> {
        self.call_class(stream_id, function, args, Class::Normal).await
    }

    /// A completion/recovery CALL may consume reserved capacity. It still
    /// obeys every total limit and the same uncertainty/deadline rules.
    pub async fn call_control(&self, stream_id: u64, function: &str, args: &[u8]) -> Result<Response, Error> {
        self.call_class(stream_id, function, args, Class::Completion).await
    }

    async fn call_class(&self, stream_id: u64, function: &str, args: &[u8], class: Class) -> Result<Response, Error> {
        self.roundtrip(
            iproto::CALL,
            stream_id,
            function.len().saturating_add(args.len()).saturating_add(128),
            class,
            |out| {
                out.push(0x82);
                out.push(key::FUNCTION_NAME);
                mp::write_str(out, function.as_bytes());
                out.push(key::TUPLE);
                put_array(out, args);
            },
        )
        .await
    }

    /// `IPROTO_EVAL`: the reply data is an array of the chunk's return values.
    pub async fn eval(&self, stream_id: u64, expr: &str, args: &[u8]) -> Result<Response, Error> {
        self.eval_class(stream_id, expr, args, Class::Normal).await
    }

    /// A bounded bootstrap probe needed to route completion/recovery work.
    pub async fn eval_control(&self, stream_id: u64, expr: &str, args: &[u8]) -> Result<Response, Error> {
        self.eval_class(stream_id, expr, args, Class::Completion).await
    }

    async fn eval_class(&self, stream_id: u64, expr: &str, args: &[u8], class: Class) -> Result<Response, Error> {
        self.roundtrip(
            iproto::EVAL,
            stream_id,
            expr.len().saturating_add(args.len()).saturating_add(128),
            class,
            |out| {
                out.push(0x82);
                out.push(key::EXPR);
                mp::write_str(out, expr.as_bytes());
                out.push(key::TUPLE);
                put_array(out, args);
            },
        )
        .await
    }

    /// `IPROTO_BEGIN` in `stream_id` (must be non-zero). `timeout_secs`
    /// makes the server roll the transaction back after that many seconds
    /// (later requests fail with `ER_TRANSACTION_TIMEOUT`).
    pub async fn begin(
        &self,
        stream_id: u64,
        timeout_secs: Option<f64>,
        isolation: Option<TxnIsolation>,
    ) -> Result<(), Error> {
        self.roundtrip(iproto::BEGIN, stream_id, 128, Class::Normal, |out| {
            let n = timeout_secs.is_some() as u8 + isolation.is_some() as u8;
            if n == 0 {
                return;
            }
            out.push(0x80 | n);
            if let Some(t) = timeout_secs {
                out.push(key::TIMEOUT);
                mp::write_f64(out, t);
            }
            if let Some(i) = isolation {
                out.push(key::TXN_ISOLATION);
                mp::write_uint(out, i as u64);
            }
        })
        .await
        .map(drop)
    }

    /// `IPROTO_COMMIT` of the stream's transaction.
    pub async fn commit(&self, stream_id: u64) -> Result<(), Error> {
        self.roundtrip(iproto::COMMIT, stream_id, 128, Class::Completion, |_| {})
            .await
            .map(drop)
    }

    /// `IPROTO_ROLLBACK` of the stream's transaction.
    pub async fn rollback(&self, stream_id: u64) -> Result<(), Error> {
        self.roundtrip(iproto::ROLLBACK, stream_id, 128, Class::Completion, |_| {})
            .await
            .map(drop)
    }
}

/// Append a caller-provided MessagePack array; an empty slice means `[]`.
#[inline]
fn put_array(out: &mut Vec<u8>, arr: &[u8]) {
    if arr.is_empty() {
        out.push(0x90);
    } else {
        out.extend_from_slice(arr);
    }
}
