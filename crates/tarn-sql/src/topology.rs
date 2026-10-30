//! Gateway-wide resource policy independent of the number of known shards.
//! Socket permits live in IPROTO's physical owners. Fanout admission bounds
//! active branches and waiters; transport charges outlive a cancelled branch.

use futures::{stream, StreamExt};
use serde::Deserialize;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use tarn_common::{SqlError, SqlResult};
use tokio::sync::Semaphore;

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TopologyConfig {
    pub max_connections: usize,
    pub completion_connections: usize,
    pub idle_secs: u64,
    pub fanout: usize,
    pub active_branches: usize,
    pub queued_branches: usize,
    pub control_reply_bytes: usize,
    pub response_bytes: usize,
    pub response_frame_bytes: usize,
    pub stats_shards_per_tick: usize,
}

impl Default for TopologyConfig {
    fn default() -> Self {
        Self {
            max_connections: 256,
            completion_connections: 16,
            idle_secs: 30,
            fanout: 8,
            active_branches: 64,
            queued_branches: 256,
            control_reply_bytes: 64 << 20,
            response_bytes: 256 << 20,
            response_frame_bytes: 32 << 20,
            stats_shards_per_tick: 8,
        }
    }
}

impl TopologyConfig {
    pub(crate) fn validate(&self) -> SqlResult<()> {
        if !(2..=65536).contains(&self.max_connections)
            || self.completion_connections == 0
            || self.completion_connections >= self.max_connections
            || self.max_connections.saturating_sub(self.completion_connections) < 2
            || !(1..=86400).contains(&self.idle_secs)
            || self.fanout == 0
            || self.fanout >= self.active_branches
            || self.active_branches > 4096
            || self.queued_branches == 0
            || self.queued_branches > 65536
            || !(8192..=1 << 30).contains(&self.control_reply_bytes)
            || !(8192..=1 << 30).contains(&self.response_bytes)
            || !(8192..=self.response_bytes).contains(&self.response_frame_bytes)
            || !(1..=1024).contains(&self.stats_shards_per_tick)
        {
            return Err(SqlError::internal("invalid topology resource limits"));
        }
        Ok(())
    }
}

/// One serializable read cannot pin the entire ordinary socket budget before
/// its bounded branches are polled. Existing transaction streams count too.
/// RC reads do not retain streams and continue through the fanout window.
pub(crate) fn check_pinned_reads(cfg: &TopologyConfig, shards: impl Iterator<Item = usize>) -> SqlResult<()> {
    let limit = (cfg.max_connections - cfg.completion_connections) / 2;
    let distinct: std::collections::BTreeSet<_> = shards.collect();
    if distinct.len() > limit {
        return Err(SqlError::new(
            1205,
            "HY000",
            "serializable read would exceed the pinned-shard budget",
        ));
    }
    Ok(())
}

pub(crate) struct Fanout {
    active: Semaphore,
    queue: Semaphore,
    running: AtomicUsize,
    peak: AtomicUsize,
}

impl Fanout {
    pub(crate) fn new(cfg: &TopologyConfig) -> Self {
        Self {
            active: Semaphore::new(cfg.active_branches),
            queue: Semaphore::new(cfg.queued_branches),
            running: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        }
    }

    pub(crate) async fn run<T>(&self, future: impl Future<Output = SqlResult<T>>) -> SqlResult<T> {
        let waiting = self
            .queue
            .try_acquire()
            .map_err(|_| SqlError::new(1205, "HY000", "gateway fanout queue full before branch execution"))?;
        let _slot = self.active.acquire().await.expect("fanout semaphore is never closed");
        drop(waiting);
        let n = self.running.fetch_add(1, Ordering::Relaxed) + 1;
        self.peak.fetch_max(n, Ordering::Relaxed);
        struct Running<'a>(&'a AtomicUsize);
        impl Drop for Running<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::Relaxed);
            }
        }
        let _running = Running(&self.running);
        future.await
    }

    pub(crate) fn counters(&self) -> (usize, usize) {
        (self.running.load(Ordering::Relaxed), self.peak.load(Ordering::Relaxed))
    }
}

/// Poll at most `limit` futures at once and retain their original order.
/// Unlike try_join_all, every admitted effectful branch reaches its outcome
/// even when another branch fails. Callers retain their existing error rules.
pub(crate) fn collect<'a, I, F>(futures: I, limit: usize) -> futures::future::BoxFuture<'a, Vec<F::Output>>
where
    I: IntoIterator<Item = F>,
    F: Future + Send + 'a,
    F::Output: Send + 'a,
{
    let futures: Vec<_> = futures.into_iter().collect();
    Box::pin(async move { stream::iter(futures).buffered(limit).collect().await })
}

/// A fixed polling budget, rotating over all live shards. Samples retain
/// their actual ages; a large topology cannot label an old sample fresh.
pub(crate) fn poll_shards(cluster: &crate::Cluster, cursor: &mut usize) -> Vec<usize> {
    let live: Vec<_> = cluster.live_shards().collect();
    if live.is_empty() {
        return Vec::new();
    }
    let n = cluster.cfg.topology.stats_shards_per_tick.min(live.len());
    let out = (0..n).map(|i| live[(*cursor + i) % live.len()]).collect();
    *cursor = (*cursor + n) % live.len();
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;
    use std::sync::Arc;

    #[test]
    fn a_read_leaves_ordinary_socket_capacity_and_counts_existing_unique_streams() {
        let cfg = TopologyConfig {
            max_connections: 20,
            completion_connections: 4,
            ..Default::default()
        };
        assert!(check_pinned_reads(&cfg, (0..8).chain(0..8)).is_ok());
        let error = check_pinned_reads(&cfg, (0..8).chain([8])).unwrap_err();
        assert_eq!(error.code, 1205);
        assert!(check_pinned_reads(&cfg, 0..1024).is_err());
    }

    #[test]
    fn configuration_cannot_disable_bounds_or_consume_every_slot_per_query() {
        let cfg: TopologyConfig = serde_json::from_str("{}").unwrap();
        cfg.validate().unwrap();
        for bad in [
            TopologyConfig {
                max_connections: 1,
                ..cfg.clone()
            },
            TopologyConfig {
                completion_connections: 0,
                ..cfg.clone()
            },
            TopologyConfig {
                fanout: cfg.active_branches,
                ..cfg.clone()
            },
            TopologyConfig {
                response_frame_bytes: cfg.response_bytes + 1,
                ..cfg.clone()
            },
        ] {
            assert!(bad.validate().is_err());
        }
        assert!(serde_json::from_str::<TopologyConfig>(r#"{"fanot":4}"#).is_err());
    }

    #[tokio::test]
    async fn branch_window_preserves_order_completeness_and_error_outcomes() {
        let cfg = TopologyConfig {
            fanout: 2,
            active_branches: 3,
            queued_branches: 3,
            ..Default::default()
        };
        let f = Fanout::new(&cfg);
        let executed = AtomicUsize::new(0);
        let executed = &executed;
        let results = collect(
            (0..128).map(|i| {
                f.run(async move {
                    executed.fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                    if i == 7 {
                        Err(SqlError::new(1213, "40001", "known conflict"))
                    } else {
                        Ok(i)
                    }
                })
            }),
            cfg.fanout,
        )
        .await;
        assert_eq!(executed.load(Ordering::Relaxed), 128);
        assert_eq!(results.len(), 128);
        for (i, result) in results.into_iter().enumerate() {
            if i == 7 {
                assert_eq!(result.unwrap_err().sqlstate, *b"40001");
            } else {
                assert_eq!(result.unwrap(), i);
            }
        }
        assert_eq!(f.counters(), (0, 2));
    }

    #[tokio::test]
    async fn bounded_waiting_is_fifo_and_cancellation_releases_only_branch_slots() {
        let cfg = TopologyConfig {
            active_branches: 1,
            queued_branches: 1,
            ..Default::default()
        };
        let f = Arc::new(Fanout::new(&cfg));
        let release = Arc::new(tokio::sync::Notify::new());
        let first = f.run(async {
            release.notified().await;
            Ok(1)
        });
        let second = f.run(async { Ok(2) });
        futures::pin_mut!(first, second);
        assert!(first.as_mut().now_or_never().is_none());
        assert!(second.as_mut().now_or_never().is_none());
        let called = AtomicUsize::new(0);
        let e = f
            .run(async {
                called.fetch_add(1, Ordering::Relaxed);
                Ok(3)
            })
            .await
            .unwrap_err();
        assert_eq!(e.code, 1205);
        assert_eq!(called.load(Ordering::Relaxed), 0);
        release.notify_one();
        assert_eq!(first.await.unwrap(), 1);
        assert_eq!(second.await.unwrap(), 2);
        assert_eq!(f.counters(), (0, 1));
        let owner = f.clone();
        let task = tokio::spawn(async move { owner.run(std::future::pending::<SqlResult<()>>()).await });
        while f.counters().0 == 0 {
            tokio::task::yield_now().await;
        }
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(f.counters().0, 0);
        assert!(f.run(async { Ok(()) }).await.is_ok());
    }
}
