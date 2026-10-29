//! Conservative SQL resource accounting. Charges precede materialization;
//! query charges live through result delivery and transaction charges live
//! through rollback/commit. Completion RPCs never acquire a new query budget.
//! This accounts logical allocations, not allocator arenas or total RSS.

use serde::Deserialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tarn_common::{ResourceGuard, SqlError, SqlResult, Value};

const MIB: u64 = 1 << 20;
/// A frame may include one row beyond the storage pager's 4 MiB target.
pub const STORAGE_REPLY_BYTES: usize = 8 << 20;
pub const WIRE_BYTES: u64 = (STORAGE_REPLY_BYTES as u64) * 2;
fn query_bytes() -> u64 {
    64 * MIB
}
fn transaction_bytes() -> u64 {
    32 * MIB
}
fn server_bytes() -> u64 {
    512 * MIB
}
fn query_work() -> u64 {
    100_000_000
}
fn server_work() -> u64 {
    1_000_000_000
}
fn read_timeout_ms() -> u64 {
    10_000
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct Limits {
    pub topk: bool,
    pub query_bytes: u64,
    pub transaction_bytes: u64,
    pub server_bytes: u64,
    pub query_work: u64,
    pub server_work: u64,
    pub read_timeout_ms: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            topk: true,
            query_bytes: query_bytes(),
            transaction_bytes: transaction_bytes(),
            server_bytes: server_bytes(),
            query_work: query_work(),
            server_work: server_work(),
            read_timeout_ms: read_timeout_ms(),
        }
    }
}

pub fn exhausted(kind: &str) -> SqlError {
    SqlError::new(1041, "HY000", format!("Tarn resource limit: {kind}"))
}

pub fn is_exhausted(error: &SqlError) -> bool {
    error.code == 1041 && error.message.starts_with("Tarn resource limit:")
}

fn add(counter: &AtomicU64, cost: u64, limit: u64) -> bool {
    counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |old| {
            old.checked_add(cost).filter(|n| *n <= limit)
        })
        .is_ok()
}

#[derive(Debug)]
pub struct Pool {
    pub limits: Limits,
    bytes: AtomicU64,
    peak_bytes: AtomicU64,
    work: AtomicU64,
    peak_work: AtomicU64,
    queries: AtomicU64,
    refusals: AtomicU64,
    read_deadlines: AtomicU64,
}

impl Pool {
    pub fn new(limits: Limits) -> SqlResult<Arc<Self>> {
        if limits.query_bytes == 0
            || limits.transaction_bytes == 0
            || limits.server_bytes == 0
            || limits.query_work == 0
            || limits.server_work == 0
            || limits.read_timeout_ms == 0
        {
            return Err(SqlError::internal("resource limits must be positive"));
        }
        Ok(Arc::new(Self {
            limits,
            bytes: AtomicU64::new(0),
            peak_bytes: AtomicU64::new(0),
            work: AtomicU64::new(0),
            peak_work: AtomicU64::new(0),
            queries: AtomicU64::new(0),
            refusals: AtomicU64::new(0),
            read_deadlines: AtomicU64::new(0),
        }))
    }

    fn reserve(&self, bytes: u64) -> SqlResult<()> {
        if !add(&self.bytes, bytes, self.limits.server_bytes) {
            self.refusals.fetch_add(1, Ordering::Relaxed);
            return Err(exhausted("gateway memory budget"));
        }
        self.peak_bytes
            .fetch_max(self.bytes.load(Ordering::Relaxed), Ordering::Relaxed);
        Ok(())
    }

    pub fn query(self: &Arc<Self>) -> Arc<Query> {
        self.queries.fetch_add(1, Ordering::Relaxed);
        Arc::new(Query {
            pool: self.clone(),
            bytes: AtomicU64::new(0),
            work: AtomicU64::new(0),
            reads: Arc::new(tokio::sync::Semaphore::new(
                (self.limits.query_bytes / (WIRE_BYTES * 2)).clamp(1, 4) as usize,
            )),
        })
    }

    pub fn account(self: &Arc<Self>, limit: u64) -> Account {
        Account {
            pool: self.clone(),
            limit,
            bytes: 0,
        }
    }

    pub fn stats(&self) -> [u64; 7] {
        [
            self.bytes.load(Ordering::Relaxed),
            self.peak_bytes.load(Ordering::Relaxed),
            self.work.load(Ordering::Relaxed),
            self.peak_work.load(Ordering::Relaxed),
            self.queries.load(Ordering::Relaxed),
            self.refusals.load(Ordering::Relaxed),
            self.read_deadlines.load(Ordering::Relaxed),
        ]
    }
}

#[derive(Debug)]
pub struct Query {
    pool: Arc<Pool>,
    bytes: AtomicU64,
    work: AtomicU64,
    reads: Arc<tokio::sync::Semaphore>,
}

impl Query {
    fn charge(&self, bytes: u64) -> SqlResult<()> {
        if !add(&self.bytes, bytes, self.pool.limits.query_bytes) {
            self.pool.refusals.fetch_add(1, Ordering::Relaxed);
            return Err(exhausted(&format!(
                "query memory budget ({} + {} > {})",
                self.bytes.load(Ordering::Relaxed),
                bytes,
                self.pool.limits.query_bytes
            )));
        }
        if let Err(error) = self.pool.reserve(bytes) {
            self.bytes.fetch_sub(bytes, Ordering::AcqRel);
            return Err(error);
        }
        Ok(())
    }

    fn release(&self, bytes: u64) {
        self.bytes.fetch_sub(bytes, Ordering::AcqRel);
        self.pool.bytes.fetch_sub(bytes, Ordering::AcqRel);
    }

    fn step(&self, n: u64) -> SqlResult<()> {
        if !add(&self.work, n, self.pool.limits.query_work) {
            self.pool.refusals.fetch_add(1, Ordering::Relaxed);
            return Err(exhausted("query work budget"));
        }
        if !add(&self.pool.work, n, self.pool.limits.server_work) {
            self.work.fetch_sub(n, Ordering::AcqRel);
            self.pool.refusals.fetch_add(1, Ordering::Relaxed);
            return Err(exhausted("gateway work budget"));
        }
        self.pool
            .peak_work
            .fetch_max(self.pool.work.load(Ordering::Relaxed), Ordering::Relaxed);
        Ok(())
    }

    pub fn guard(self: &Arc<Self>) -> ResourceGuard {
        ResourceGuard::new(self.clone())
    }
}

impl Drop for Query {
    fn drop(&mut self) {
        self.pool
            .bytes
            .fetch_sub(self.bytes.load(Ordering::Relaxed), Ordering::AcqRel);
        self.pool
            .work
            .fetch_sub(self.work.load(Ordering::Relaxed), Ordering::AcqRel);
        self.pool.queries.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Debug)]
pub struct Account {
    pool: Arc<Pool>,
    limit: u64,
    bytes: u64,
}

impl Account {
    pub fn resize(&mut self, bytes: u64) -> SqlResult<()> {
        if bytes >= self.bytes {
            self.charge(bytes - self.bytes)
        } else {
            self.pool.bytes.fetch_sub(self.bytes - bytes, Ordering::AcqRel);
            self.bytes = bytes;
            Ok(())
        }
    }
    pub fn charge(&mut self, bytes: u64) -> SqlResult<()> {
        let next = self
            .bytes
            .checked_add(bytes)
            .filter(|n| *n <= self.limit)
            .ok_or_else(|| {
                self.pool.refusals.fetch_add(1, Ordering::Relaxed);
                exhausted("retained transaction/session memory budget")
            })?;
        self.pool.reserve(bytes)?;
        self.bytes = next;
        Ok(())
    }
    pub fn used(&self) -> u64 {
        self.bytes
    }
}

impl Drop for Account {
    fn drop(&mut self) {
        self.pool.bytes.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

tokio::task_local! { static CURRENT: Arc<Query>; }
tokio::task_local! { static FRAME: Arc<Frame>; }

pub fn current() -> Option<Arc<Query>> {
    CURRENT.try_with(Arc::clone).ok()
}

pub async fn scope<T>(query: Arc<Query>, future: impl std::future::Future<Output = T>) -> T {
    CURRENT.scope(query, future).await
}

pub fn scope_sync<T>(query: Arc<Query>, run: impl FnOnce() -> T) -> T {
    CURRENT.sync_scope(query, run)
}

pub fn charge(bytes: u64) -> SqlResult<()> {
    let Some(query) = current() else { return Ok(()) };
    query.charge(bytes)?;
    let _ = FRAME.try_with(|frame| frame.bytes.fetch_add(bytes, Ordering::Relaxed));
    Ok(())
}

pub fn persistent(bytes: u64) -> SqlResult<()> {
    current().map_or(Ok(()), |query| query.charge(bytes))
}

pub fn retain(bytes: u64) -> SqlResult<Option<ResourceGuard>> {
    let Some(query) = current() else { return Ok(None) };
    query.charge(bytes)?;
    Ok(Some(ResourceGuard::new(Arc::new(ReadReservation {
        query,
        bytes,
        _permit: None,
    }))))
}

pub fn work(n: u64) -> SqlResult<()> {
    current().map_or(Ok(()), |query| query.step(n))
}

pub fn value_bytes(value: &Value) -> u64 {
    std::mem::size_of::<Value>() as u64
        + match value {
            Value::Str(s) | Value::Bin(s) => s.len() as u64,
            Value::Decimal(s) => s.len() as u64,
            _ => 0,
        }
}

pub fn row_bytes(row: &[Value]) -> u64 {
    row.iter().fold(64u64, |n, value| n.saturating_add(value_bytes(value)))
}

/// Detach values that outlive their charged page/query. Ordinary query rows
/// can stay zero-copy because the complete frame stays charged to the query.
pub fn own_value(value: Value) -> Value {
    match value {
        Value::Str(bytes) => Value::Str(bytes::Bytes::copy_from_slice(&bytes)),
        Value::Bin(bytes) => Value::Bin(bytes::Bytes::copy_from_slice(&bytes)),
        other => other,
    }
}
pub fn own_row(row: Vec<Value>) -> Vec<Value> {
    row.into_iter().map(own_value).collect()
}

pub fn row(row: &[Value]) -> SqlResult<()> {
    charge(row_bytes(row).saturating_mul(2))
}

pub fn decoded(data: usize, count: usize, columns: usize) -> SqlResult<()> {
    work((data as u64).saturating_add((count as u64).saturating_mul(columns as u64 + 1)))?;
    let slots =
        (count as u64).saturating_mul(64 + (columns as u64).saturating_mul(std::mem::size_of::<Value>() as u64));
    charge(slots.saturating_add((data as u64).saturating_mul(2)).saturating_mul(2))
}

/// A streaming top-K page may release its temporary allocations only after
/// retained candidates have been copied and charged independently.
#[derive(Debug)]
pub struct Frame {
    query: Option<Arc<Query>>,
    bytes: AtomicU64,
}

impl Frame {
    /// Successful output transfers the complete frame to the query owner.
    /// Discarded attempts instead refund temporary memory, never work.
    pub fn retain_all(&self) {
        self.bytes.store(0, Ordering::Relaxed);
    }
    pub fn scope_sync<T>(self: &Arc<Self>, run: impl FnOnce() -> T) -> T {
        FRAME.sync_scope(self.clone(), run)
    }
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            query: current(),
            bytes: AtomicU64::new(0),
        })
    }
    pub async fn scope<T>(self: &Arc<Self>, future: impl std::future::Future<Output = T>) -> T {
        FRAME.scope(self.clone(), future).await
    }
}

impl Drop for Frame {
    fn drop(&mut self) {
        if let Some(query) = &self.query {
            query.release(self.bytes.load(Ordering::Relaxed));
        }
    }
}

/// Keep an in-flight read's wire reservation after caller cancellation.
/// A deadline retires the physical socket; uncertain writes on that socket
/// retain the transport's existing 40003 classification and are not replayed.
pub async fn call_read(
    client: &tarn_iproto::Client,
    stream: u64,
    function: &'static str,
    args: &[u8],
) -> SqlResult<(Result<tarn_iproto::Response, tarn_iproto::Error>, Option<ResourceGuard>)> {
    let Some(query) = current() else {
        return Ok((client.call(stream, function, args).await, None));
    };
    let bytes = WIRE_BYTES.saturating_add(args.len() as u64 * 2);
    let permit = query
        .reads
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| exhausted("read admission closed"))?;
    query.charge(bytes)?;
    let reservation = Arc::new(ReadReservation {
        query: query.clone(),
        bytes,
        _permit: Some(permit),
    });
    let client = client.clone();
    let args = args.to_vec();
    let task = tokio::spawn(crate::observe::inherit(async move {
        let reply = tokio::time::timeout(
            std::time::Duration::from_millis(query.pool.limits.read_timeout_ms),
            client.call(stream, function, &args),
        )
        .await;
        let response = match reply {
            Ok(r) if r.is_err() && client.response_limit_exceeded() => {
                return Err(exhausted("storage reply exceeds 8 MiB"))
            }
            Ok(r) => r,
            Err(_) => {
                query.pool.read_deadlines.fetch_add(1, Ordering::Relaxed);
                client.close();
                Err(tarn_iproto::Error::Closed)
            }
        };
        Ok((response, Some(ResourceGuard::new(reservation))))
    }));
    task.await
        .map_err(|_| SqlError::internal("storage read worker stopped"))?
}

#[allow(clippy::too_many_arguments)]
pub async fn select_read(
    client: &tarn_iproto::Client,
    stream: u64,
    space: u32,
    index: u32,
    iter: tarn_iproto::Iter,
    offset: u32,
    limit: u32,
    key: &[u8],
) -> SqlResult<(Result<tarn_iproto::Response, tarn_iproto::Error>, Option<ResourceGuard>)> {
    let Some(query) = current() else {
        return Ok((
            client.select(stream, space, index, iter, offset, limit, key).await,
            None,
        ));
    };
    let bytes = WIRE_BYTES.saturating_add(key.len() as u64 * 2);
    let permit = query
        .reads
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| exhausted("read admission closed"))?;
    query.charge(bytes)?;
    let reservation = Arc::new(ReadReservation {
        query: query.clone(),
        bytes,
        _permit: Some(permit),
    });
    let client = client.clone();
    let key = key.to_vec();
    tokio::spawn(crate::observe::inherit(async move {
        let reply = tokio::time::timeout(
            std::time::Duration::from_millis(query.pool.limits.read_timeout_ms),
            client.select(stream, space, index, iter, offset, limit, &key),
        )
        .await;
        let result = match reply {
            Ok(result) if result.is_err() && client.response_limit_exceeded() => {
                return Err(exhausted("storage reply exceeds 8 MiB"))
            }
            Ok(result) => result,
            Err(_) => {
                query.pool.read_deadlines.fetch_add(1, Ordering::Relaxed);
                client.close();
                Err(tarn_iproto::Error::Closed)
            }
        };
        Ok((result, Some(ResourceGuard::new(reservation))))
    }))
    .await
    .map_err(|_| SqlError::internal("storage read worker stopped"))?
}

pub async fn cluster_read(
    cluster: &Arc<crate::cluster::Cluster>,
    shard: usize,
    function: &'static str,
    args: &[u8],
) -> SqlResult<(bytes::Bytes, Option<ResourceGuard>)> {
    let Some(query) = current() else {
        return cluster.call(shard, function, args).await.map(|data| (data, None));
    };
    let bytes = WIRE_BYTES.saturating_add(args.len() as u64 * 2);
    let permit = query
        .reads
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| exhausted("read admission closed"))?;
    query.charge(bytes)?;
    let reservation = Arc::new(ReadReservation {
        query: query.clone(),
        bytes,
        _permit: Some(permit),
    });
    let cluster = cluster.clone();
    let args = args.to_vec();
    tokio::spawn(crate::observe::inherit(async move {
        let reply = tokio::time::timeout(
            std::time::Duration::from_millis(query.pool.limits.read_timeout_ms),
            cluster.call(shard, function, &args),
        )
        .await;
        let data = match reply {
            Ok(result) => result?,
            Err(_) => {
                query.pool.read_deadlines.fetch_add(1, Ordering::Relaxed);
                cluster.shards[shard].retire_connections();
                return Err(SqlError::conflict("Storage read deadline expired; connections retired"));
            }
        };
        Ok((data, Some(ResourceGuard::new(reservation))))
    }))
    .await
    .map_err(|_| SqlError::internal("storage read worker stopped"))?
}

#[derive(Debug)]
struct ReadReservation {
    query: Arc<Query>,
    bytes: u64,
    _permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

/// Admit before mutation, then keep ownership through native handback and
/// final reply, including caller cancellation. Only an explicit storage
/// admission response can authorize the caller's existing retry policy.
pub async fn autocommit(
    cluster: &Arc<crate::cluster::Cluster>,
    shard: usize,
    native: bool,
    args: &[u8],
    lock_timeout: f64,
) -> SqlResult<(bytes::Bytes, Option<ResourceGuard>)> {
    let guard = retain(WIRE_BYTES.saturating_add(args.len() as u64 * 2))?;
    let cluster = cluster.clone();
    let args = args.to_vec();
    let timeout = std::time::Duration::from_millis(cluster.cfg.resources.read_timeout_ms)
        .max(std::time::Duration::from_secs_f64(lock_timeout.max(0.0) + 1.0));
    tokio::spawn(crate::observe::inherit(async move {
        let result = tokio::time::timeout(timeout, async {
            if native {
                cluster
                    .call_native_or_lua_once(
                        shard,
                        "tarn_storage.ts_autocommit",
                        "tarn_autocommit",
                        &args,
                        crate::session::MARK_NO_NATIVE_WRITE,
                    )
                    .await
            } else {
                cluster.call_once(shard, "tarn_autocommit", &args).await
            }
        })
        .await;
        let result = match result {
            Ok(result) => result,
            Err(_) => {
                cluster.shards[shard].retire_connections();
                Err(crate::mpw::ambiguous(
                    cluster.shards[shard].id,
                    &"autocommit deadline expired",
                ))
            }
        };
        result.map(|data| (data, guard))
    }))
    .await
    .map_err(|_| SqlError::new(1180, "40003", "Autocommit worker stopped; outcome may be unknown"))?
}

#[derive(Debug)]
struct ExternalReservation {
    pool: Arc<Pool>,
    bytes: u64,
}
impl Drop for ExternalReservation {
    fn drop(&mut self) {
        self.pool.bytes.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}
impl tarn_common::ResourceMemory for Pool {
    fn reserve(self: Arc<Self>, bytes: u64) -> SqlResult<ResourceGuard> {
        Pool::reserve(&self, bytes)?;
        Ok(ResourceGuard::new(Arc::new(ExternalReservation { pool: self, bytes })))
    }
}
impl Drop for ReadReservation {
    fn drop(&mut self) {
        self.query.release(self.bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn frames_refund_only_their_memory_and_results_keep_ownership() {
        let pool = Pool::new(Limits {
            query_bytes: 100,
            server_bytes: 150,
            ..Limits::default()
        })
        .unwrap();
        let query = pool.query();
        scope(query.clone(), async {
            charge(20).unwrap();
            let frame = Frame::new();
            frame
                .scope(async {
                    charge(50).unwrap();
                    work(7).unwrap();
                    persistent(10).unwrap();
                })
                .await;
            assert_eq!(pool.stats()[0], 80);
            drop(frame);
            assert_eq!(pool.stats()[0], 30);
            assert_eq!(pool.stats()[2], 7);
            assert!(charge(71).is_err());
        })
        .await;
        let guard = query.guard();
        drop(query);
        assert_eq!(pool.stats()[0], 30);
        drop(guard);
        assert_eq!(pool.stats()[0], 0);
        assert_eq!(pool.stats()[2], 0);
    }

    #[test]
    fn concurrent_accounts_cannot_over_admit_or_leak_failed_charges() {
        let pool = Pool::new(Limits {
            server_bytes: 100,
            ..Limits::default()
        })
        .unwrap();
        let mut one = pool.account(80);
        let mut two = pool.account(80);
        one.charge(70).unwrap();
        assert!(two.charge(31).is_err());
        two.charge(30).unwrap();
        assert!(one.charge(u64::MAX).is_err());
        assert_eq!(pool.stats()[0], 100);
        drop(one);
        drop(two);
        assert_eq!(pool.stats()[0], 0);
    }

    #[tokio::test]
    async fn cancelled_read_keeps_its_reservation_until_socket_retirement() {
        use crate::mpw::{Mp, Rd};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::{TcpListener, TcpStream};
        async fn request(socket: &mut TcpStream) -> u64 {
            assert_eq!(socket.read_u8().await.unwrap(), 0xce);
            let size = socket.read_u32().await.unwrap();
            let mut packet = vec![0; size as usize];
            socket.read_exact(&mut packet).await.unwrap();
            let mut rd = Rd::new(&packet);
            let mut sync = None;
            for _ in 0..rd.map().unwrap() {
                let key = rd.value().unwrap().as_i64().unwrap();
                let value = rd.value().unwrap().as_i64().unwrap();
                if key == 1 {
                    sync = Some(value as u64);
                }
            }
            sync.unwrap()
        }
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (sent, received) = tokio::sync::oneshot::channel();
        let peer = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut greeting = [b' '; 128];
            let version = b"Tarantool 3.9.0 (Binary)";
            greeting[..version.len()].copy_from_slice(version);
            greeting[63] = b'\n';
            greeting[64..108].copy_from_slice(b"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=");
            greeting[127] = b'\n';
            socket.write_all(&greeting).await.unwrap();
            let sync = request(&mut socket).await;
            let mut packet = Mp::new();
            packet.map(2).uint(0).uint(0).uint(1).uint(sync).map(0);
            socket.write_u8(0xce).await.unwrap();
            socket.write_u32(packet.0.len() as u32).await.unwrap();
            socket.write_all(&packet.0).await.unwrap();
            request(&mut socket).await;
            sent.send(()).unwrap();
            // An established peer that never returns the read's reply.
            let mut byte = [0];
            assert_eq!(socket.read(&mut byte).await.unwrap(), 0);
        });
        let client = tarn_iproto::Client::connect(&tarn_iproto::Opts::new(addr.to_string()))
            .await
            .unwrap();
        let pool = Pool::new(Limits {
            read_timeout_ms: 150,
            ..Limits::default()
        })
        .unwrap();
        let query = pool.query();
        let worker = tokio::spawn(scope(query.clone(), async move {
            call_read(&client, 0, "silent", &[0x90]).await
        }));
        received.await.unwrap();
        worker.abort();
        let _ = worker.await;
        drop(query);
        assert!(pool.stats()[0] >= WIRE_BYTES);
        assert_eq!(pool.stats()[4], 1);
        tokio::time::timeout(std::time::Duration::from_secs(2), peer)
            .await
            .unwrap()
            .unwrap();
        // The timeout task may be dropping the guard after closing the socket.
        for _ in 0..100 {
            if pool.stats()[0] == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(pool.stats()[0], 0);
        assert_eq!(pool.stats()[4], 0);
        assert_eq!(pool.stats()[6], 1);
    }
}
