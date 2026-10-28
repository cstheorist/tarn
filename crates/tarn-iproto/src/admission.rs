//! Count/byte reservations retained by an enqueued request, independently
//! of its waiting caller. Admission never waits or holds two scope locks.
//! Completion traffic can use the reserved part of every enclosing scope.

use crate::client::{Shared, Stats};
use crate::Error;
use parking_lot::Mutex;
use serde::Deserialize;
use std::sync::{Arc, Weak};

/// A hard scope limit, including ordinary and completion requests together.
/// Ordinary traffic cannot consume the reserved count or bytes.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct Limits {
    pub requests: usize,
    pub bytes: usize,
    pub reserved_requests: usize,
    pub reserved_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            requests: 256,
            bytes: 64 << 20,
            reserved_requests: 16,
            reserved_bytes: 8 << 20,
        }
    }
}

impl Limits {
    pub fn validate(&self) -> Result<(), Error> {
        if self.reserved_requests == 0
            || self.reserved_requests >= self.requests
            || self.reserved_bytes < 128
            || self.reserved_bytes >= self.bytes
            || self.bytes - self.reserved_bytes < 128
        {
            return Err(Error::Protocol(
                "RPC limits require nonzero normal/completion counts and at least 128 bytes for each".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Class {
    Normal,
    Completion,
}

/// Snapshot of charged requests, including caller-abandoned or retiring
/// requests. Byte charges conservatively include encoding headroom.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AdmissionStats {
    pub pending: usize,
    pub bytes: usize,
    pub normal_pending: usize,
    pub normal_bytes: usize,
    pub peak_pending: usize,
    pub peak_bytes: usize,
    pub refusals: u64,
    pub deadline_retirements: u64,
    pub request_refusals: u64,
    pub observation_drops: u64,
}

struct Inner {
    scope: &'static str,
    limits: Limits,
    state: Mutex<AdmissionStats>,
    connections: Mutex<Vec<Weak<Shared>>>,
}

/// Shared admission scope, for example a shard or an entire gateway.
/// Clones keep one budget across connection replacements.
#[derive(Clone)]
pub struct Budget(Arc<Inner>);

impl std::fmt::Debug for Budget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Budget")
            .field("scope", &self.0.scope)
            .field("limits", &self.0.limits)
            .finish()
    }
}

impl Budget {
    pub fn new(scope: &'static str, limits: Limits) -> Result<Self, Error> {
        limits.validate()?;
        Ok(Self(Arc::new(Inner {
            scope,
            limits,
            state: Mutex::new(AdmissionStats::default()),
            connections: Mutex::new(Vec::new()),
        })))
    }

    pub fn limits(&self) -> Limits {
        self.0.limits
    }

    pub fn stats(&self) -> AdmissionStats {
        *self.0.state.lock()
    }

    pub(crate) fn refused(&self) {
        let mut s = self.0.state.lock();
        s.request_refusals = s.request_refusals.saturating_add(1);
    }

    pub(crate) fn observe(&self, connection: &Arc<Shared>) {
        let mut watches = self.0.connections.lock();
        watches.retain(|w| w.upgrade().is_some_and(|c| !c.quiet_closed()));
        if watches.len() < 4096 {
            watches.push(Arc::downgrade(connection));
        } else {
            let mut s = self.0.state.lock();
            s.observation_drops = s.observation_drops.saturating_add(1);
        }
    }

    /// Cached physical-connection observations, including old pools with
    /// outstanding/retiring requests. At most 4096 watches per scope;
    /// observation_drops reports incomplete coverage. Enforcement has no
    /// watch limit and includes every charge, even an unobserved connection.
    pub fn connections(&self) -> Vec<Stats> {
        let connections = {
            let mut watches = self.0.connections.lock();
            watches.retain(|w| w.upgrade().is_some_and(|c| !c.quiet_closed()));
            watches.iter().filter_map(Weak::upgrade).collect::<Vec<_>>()
        };
        connections.into_iter().map(|c| c.snapshot()).collect()
    }

    pub(crate) fn deadline_retired(&self) {
        let mut s = self.0.state.lock();
        s.deadline_retirements = s.deadline_retirements.saturating_add(1);
    }

    fn charge(&self, bytes: usize, class: Class) -> Result<(), Error> {
        let mut s = self.0.state.lock();
        let l = self.0.limits;
        let fits = s.pending < l.requests
            && bytes <= l.bytes.saturating_sub(s.bytes)
            && (class == Class::Completion
                || (s.normal_pending < l.requests - l.reserved_requests
                    && bytes <= (l.bytes - l.reserved_bytes).saturating_sub(s.normal_bytes)));
        if !fits {
            s.refusals = s.refusals.saturating_add(1);
            return Err(Error::Admission { scope: self.0.scope });
        }
        s.pending += 1;
        s.bytes += bytes;
        if class == Class::Normal {
            s.normal_pending += 1;
            s.normal_bytes += bytes;
        }
        s.peak_pending = s.peak_pending.max(s.pending);
        s.peak_bytes = s.peak_bytes.max(s.bytes);
        Ok(())
    }

    fn refund(&self, bytes: usize, class: Class) {
        let mut s = self.0.state.lock();
        s.pending -= 1;
        s.bytes -= bytes;
        if class == Class::Normal {
            s.normal_pending -= 1;
            s.normal_bytes -= bytes;
        }
    }
}

/// The immutable scope set belongs to one physical connection. A request
/// retains this one reference, rather than cloning global/shard Arcs on
/// every enqueue and reply (which would contend across connections).
pub(crate) struct Scopes {
    budgets: Vec<Budget>,
}

impl Scopes {
    pub(crate) fn new(connection: &Budget, parents: &[Budget]) -> Result<Arc<Self>, Error> {
        if parents.len() > 2 {
            return Err(Error::Protocol("at most two enclosing RPC scopes".into()));
        }
        Ok(Arc::new(Self {
            budgets: std::iter::once(connection).chain(parents).cloned().collect(),
        }))
    }

    pub(crate) fn acquire(self: &Arc<Self>, bytes: usize, class: Class) -> Result<Permit, Error> {
        let mut permit = Permit {
            scopes: self.clone(),
            bytes,
            class,
            charged: 0,
        };
        for budget in &self.budgets {
            budget.charge(bytes, class)?;
            permit.charged += 1;
        }
        Ok(permit)
    }
}

pub(crate) struct Permit {
    scopes: Arc<Scopes>,
    bytes: usize,
    class: Class,
    charged: u8,
}

impl Drop for Permit {
    fn drop(&mut self) {
        for budget in self.scopes.budgets.iter().take(self.charged as usize) {
            budget.refund(self.bytes, self.class);
        }
    }
}

#[cfg(test)]
impl Permit {
    pub(crate) fn acquire(connection: &Budget, parents: &[Budget], bytes: usize, class: Class) -> Result<Self, Error> {
        Scopes::new(connection, parents)?.acquire(bytes, class)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn gate(scope: &'static str, n: usize, bytes: usize) -> Budget {
        Budget::new(
            scope,
            Limits {
                requests: n,
                bytes,
                reserved_requests: 1,
                reserved_bytes: 128,
            },
        )
        .unwrap()
    }

    #[test]
    fn ordinary_admission_preserves_both_completion_reserves() {
        let c = gate("connection", 3, 512);
        let a = Permit::acquire(&c, &[], 192, Class::Normal).unwrap();
        let b = Permit::acquire(&c, &[], 192, Class::Normal).unwrap();
        assert!(matches!(
            Permit::acquire(&c, &[], 1, Class::Normal),
            Err(Error::Admission { .. })
        ));
        let done = Permit::acquire(&c, &[], 128, Class::Completion).unwrap();
        assert_eq!((c.stats().pending, c.stats().bytes), (3, 512));
        assert!(Permit::acquire(&c, &[], 1, Class::Completion).is_err());
        drop((a, b, done));
        assert_eq!((c.stats().pending, c.stats().bytes), (0, 0));
        assert_eq!(
            (c.stats().peak_pending, c.stats().peak_bytes, c.stats().refusals),
            (3, 512, 2)
        );
    }

    #[test]
    fn parent_refusal_refunds_each_earlier_scope() {
        let c1 = gate("c1", 8, 4096);
        let c2 = gate("c2", 8, 4096);
        let shard = gate("shard", 4, 1024);
        let global = gate("global", 2, 512);
        let parents = [shard.clone(), global.clone()];
        let a = Permit::acquire(&c1, &parents, 128, Class::Normal).unwrap();
        assert!(matches!(
            Permit::acquire(&c2, &parents, 128, Class::Normal),
            Err(Error::Admission { scope: "global" })
        ));
        assert_eq!(
            (c2.stats().pending, shard.stats().pending, global.stats().pending),
            (0, 1, 1)
        );
        let done = Permit::acquire(&c2, &parents, 128, Class::Completion).unwrap();
        assert_eq!(global.stats().pending, 2);
        drop((a, done));
        assert_eq!(
            (
                c1.stats().bytes,
                c2.stats().bytes,
                shard.stats().bytes,
                global.stats().bytes
            ),
            (0, 0, 0, 0)
        );
    }

    #[test]
    fn invalid_limits_and_oversized_requests_never_charge() {
        let bad = Limits {
            requests: 1,
            reserved_requests: 1,
            ..Limits::default()
        };
        assert!(Budget::new("bad", bad).is_err());
        let c = gate("c", 4, 512);
        assert!(Permit::acquire(&c, &[], usize::MAX, Class::Completion).is_err());
        assert_eq!(c.stats().pending, 0);
    }
}
