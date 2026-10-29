//! Physical socket admission, including connecting and retiring sockets.
//! A permit is released only after both I/O owners drop, independently of
//! client handles and cancelled requests. Completion discovery has a reserve.

use crate::Error;
use parking_lot::Mutex;
use std::sync::Arc;

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
        })))
    }

    pub fn stats(&self) -> ConnectionStats {
        *self.0.stats.lock()
    }

    pub fn available(&self, completion: bool) -> bool {
        self.stats().open < self.0.limit - if completion { 0 } else { self.0.reserve }
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
}
