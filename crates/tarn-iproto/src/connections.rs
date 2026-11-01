//! Physical socket admission, including connecting and retiring sockets.
//! A permit is released only after both I/O owners drop, independently of
//! client handles and cancelled requests. Completion discovery has a reserve.

use crate::Error;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Default)]
pub struct TrafficStats {
    pub requests: u64,
    pub bytes_written: u64,
    pub bytes_read: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ConnectionStats {
    pub open: usize,
    pub peak: usize,
    pub opened: u64,
    pub refused: u64,
}

struct Inner {
    limit: usize,
    reserve: usize,
    stats: Mutex<ConnectionStats>,
    retired: tokio::sync::Notify,
    requests: AtomicU64,
    bytes_written: AtomicU64,
    bytes_read: AtomicU64,
}

#[derive(Clone)]
pub struct ConnectionBudget(Arc<Inner>);

impl std::fmt::Debug for ConnectionBudget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionBudget")
            .field("limit", &self.0.limit)
            .field("reserve", &self.0.reserve)
            .finish()
    }
}

impl ConnectionBudget {
    pub fn new(limit: usize, reserve: usize) -> Result<Self, Error> {
        if reserve == 0 || reserve >= limit {
            return Err(Error::Protocol(
                "socket limit requires normal and completion capacity".into(),
            ));
        }
        Ok(Self(Arc::new(Inner {
            limit,
            reserve,
            stats: Mutex::new(ConnectionStats::default()),
            retired: tokio::sync::Notify::new(),
            requests: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
        })))
    }

    pub fn stats(&self) -> ConnectionStats {
        *self.0.stats.lock()
    }

    /// Lifetime traffic, including retired connections, excluding handshakes.
    /// Written bytes count completed write_all batches; a failed partial write
    /// is not included. Requests count successful enqueue, not commit outcome.
    pub fn traffic(&self) -> TrafficStats {
        TrafficStats {
            requests: self.0.requests.load(Ordering::Relaxed),
            bytes_written: self.0.bytes_written.load(Ordering::Relaxed),
            bytes_read: self.0.bytes_read.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn enqueued(&self) {
        self.0.requests.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn read(&self, bytes: usize) {
        self.0.bytes_read.fetch_add(bytes as u64, Ordering::Relaxed);
    }
    pub(crate) fn written(&self, bytes: usize) {
        self.0.bytes_written.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub fn available(&self, completion: bool) -> bool {
        self.stats().open < self.0.limit - if completion { 0 } else { self.0.reserve }
    }

    /// Wait only for physical retirement after a pool owner was evicted.
    /// This does not reserve a slot: concurrent discovery must still acquire.
    pub async fn wait_available(&self, completion: bool, timeout: std::time::Duration) {
        let wait = async {
            loop {
                let retired = self.0.retired.notified();
                tokio::pin!(retired);
                retired.as_mut().enable();
                if self.available(completion) {
                    return;
                }
                retired.await;
            }
        };
        let _ = tokio::time::timeout(timeout, wait).await;
    }

    pub(crate) fn acquire(&self, completion: bool) -> Result<ConnectionPermit, Error> {
        let mut s = self.0.stats.lock();
        if s.open >= self.0.limit - if completion { 0 } else { self.0.reserve } {
            s.refused = s.refused.saturating_add(1);
            return Err(Error::Admission {
                scope: "gateway sockets",
            });
        }
        s.open += 1;
        s.peak = s.peak.max(s.open);
        s.opened = s.opened.saturating_add(1);
        Ok(ConnectionPermit(self.clone()))
    }
}

pub(crate) struct ConnectionPermit(ConnectionBudget);

impl Drop for ConnectionPermit {
    fn drop(&mut self) {
        self.0 .0.stats.lock().open -= 1;
        self.0 .0.retired.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connecting_sockets_and_completion_reserve_share_one_cap() {
        let b = ConnectionBudget::new(3, 1).unwrap();
        let a = b.acquire(false).unwrap();
        let c = b.acquire(false).unwrap();
        assert!(b.acquire(false).is_err());
        let finish = b.acquire(true).unwrap();
        assert!(b.acquire(true).is_err());
        drop(a);
        // A completion socket cannot donate the protected capacity to ordinary discovery.
        assert!(b.acquire(false).is_err());
        drop(finish);
        let replacement = b.acquire(false).unwrap();
        drop((c, replacement));
        assert_eq!(
            b.stats(),
            ConnectionStats {
                open: 0,
                peak: 3,
                opened: 4,
                refused: 3
            }
        );
    }

    #[tokio::test]
    async fn eviction_waits_for_physical_retirement_without_reserving_or_disabling_limits() {
        let b = ConnectionBudget::new(2, 1).unwrap();
        let held = b.acquire(false).unwrap();
        let wait = b.wait_available(false, std::time::Duration::from_secs(1));
        tokio::pin!(wait);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), wait.as_mut())
                .await
                .is_err()
        );
        drop(held);
        wait.await;
        let replacement = b.acquire(false).unwrap();
        assert!(b.acquire(false).is_err());
        drop(replacement);
    }
}
