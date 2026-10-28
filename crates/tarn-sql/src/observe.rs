//! Bounded diagnostics for completed foreground statements. Observation
//! never changes a SQL result or makes a storage call. Nested execution
//! shares its caller's scope; detached background work is not attributed.

use parking_lot::Mutex;
use sha1::{Digest, Sha1};
use sqlparser::dialect::MySqlDialect;
use sqlparser::tokenizer::{Token, Tokenizer};
use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tarn_common::{SqlResult, Value};

const CAPACITY: usize = 256;
const BUCKET_CAPACITY: usize = 4096;
const SAMPLE_BYTES: usize = 512;

fn prefix(s: &str, limit: usize) -> String {
    let mut end = s.len().min(limit);
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    s[..end].into()
}

#[derive(Clone, Debug)]
pub(crate) struct Descriptor(Arc<DescriptorData>);

#[derive(Debug)]
pub(crate) struct DescriptorData {
    digest: String,
    db: String,
    sample: String,
}

impl Deref for Descriptor {
    type Target = DescriptorData;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Descriptor {
    /// `template` must already have its literals redacted by the normalizer.
    pub(crate) fn new(db: Option<&str>, template: &str) -> Self {
        let db = db.unwrap_or("");
        let mut hash = Sha1::new();
        hash.update(db.as_bytes());
        hash.update([0]);
        hash.update(template.as_bytes());
        Self(Arc::new(DescriptorData {
            digest: format!("{:x}", hash.finalize()),
            db: prefix(db, 64),
            sample: prefix(template, SAMPLE_BYTES),
        }))
    }

    /// Used only at plan-cache insertion or PREPARE, not on cache hits.
    /// Redact every string/number/placeholder, including prepared literals
    /// and 0x values that the execution normalizer must preserve as syntax.
    pub(crate) fn sql(db: Option<&str>, sql: &str) -> Option<Self> {
        let tokens = Tokenizer::new(&MySqlDialect {}, sql).tokenize().ok()?;
        let mut text = String::new();
        for token in tokens {
            let value = match token {
                Token::EOF | Token::Whitespace(_) => continue,
                Token::Number(_, _)
                | Token::Placeholder(_)
                | Token::SingleQuotedString(_)
                | Token::DoubleQuotedString(_)
                | Token::TripleSingleQuotedString(_)
                | Token::TripleDoubleQuotedString(_)
                | Token::DollarQuotedString(_)
                | Token::SingleQuotedByteStringLiteral(_)
                | Token::DoubleQuotedByteStringLiteral(_)
                | Token::TripleSingleQuotedByteStringLiteral(_)
                | Token::TripleDoubleQuotedByteStringLiteral(_)
                | Token::SingleQuotedRawStringLiteral(_)
                | Token::DoubleQuotedRawStringLiteral(_)
                | Token::TripleSingleQuotedRawStringLiteral(_)
                | Token::TripleDoubleQuotedRawStringLiteral(_)
                | Token::NationalStringLiteral(_)
                | Token::QuoteDelimitedStringLiteral(_)
                | Token::NationalQuoteDelimitedStringLiteral(_)
                | Token::EscapedStringLiteral(_)
                | Token::UnicodeStringLiteral(_)
                | Token::HexStringLiteral(_) => "?".into(),
                Token::Word(w)
                    if w.quote_style.is_none()
                        && ["TRUE", "FALSE", "NULL"]
                            .iter()
                            .any(|v| w.value.eq_ignore_ascii_case(v)) =>
                {
                    "?".into()
                }
                Token::Word(w) if w.quote_style.is_none() => w.value.to_ascii_uppercase(),
                other => other.to_string(),
            };
            if !text.is_empty() {
                text.push(' ');
            }
            text.push_str(&value);
        }
        Some(Self::new(db, &text))
    }
}

#[derive(Default)]
struct Trace {
    descriptor: Option<Descriptor>,
    disabled: bool,
    shards: BTreeSet<usize>,
    buckets: BTreeSet<u32>,
    unknown_buckets: bool,
    rpc_attempts: u64,
    grouped_reads: u64,
    commit_parts: Option<usize>,
}

tokio::task_local! {
    static CURRENT: RefCell<Trace>;
}

fn edit(f: impl FnOnce(&mut Trace)) {
    let _ = CURRENT.try_with(|cell| {
        // Telemetry must not turn an unexpected nested observation into a
        // SQL panic. No borrow is held across a storage call or an await.
        if let Ok(mut t) = cell.try_borrow_mut() {
            if !t.disabled {
                f(&mut t);
            }
        }
    });
}

pub(crate) fn describe(descriptor: Descriptor) {
    edit(|t| {
        if t.descriptor.is_none() {
            t.descriptor = Some(descriptor);
        }
    });
}

pub(crate) fn disable() {
    edit(|t| {
        t.disabled = true;
        t.descriptor = None;
    });
}

pub(crate) fn shard(id: usize) {
    edit(|t| {
        t.shards.insert(id);
    });
}

pub(crate) fn bucket(bucket: Option<u32>) {
    edit(|t| match bucket {
        Some(b) if t.buckets.len() < BUCKET_CAPACITY || t.buckets.contains(&b) => {
            t.buckets.insert(b);
        }
        _ => t.unknown_buckets = true,
    });
}

pub(crate) fn rpc() {
    edit(|t| t.rpc_attempts = t.rpc_attempts.saturating_add(1));
}

pub(crate) fn read(route: &crate::txn::Route) {
    rpc();
    match route {
        crate::txn::Route::Buckets(buckets) => {
            for &b in buckets {
                bucket(Some(b));
            }
        }
        _ => bucket(None),
    }
}

pub(crate) fn grouped() {
    edit(|t| t.grouped_reads = t.grouped_reads.saturating_add(1));
}

pub(crate) fn commit(parts: usize) {
    edit(|t| t.commit_parts = Some(parts));
}

#[derive(Clone)]
struct Entry {
    descriptor: Descriptor,
    seen: u64,
    calls: u64,
    errors: u64,
    local: u64,
    distributed: u64,
    no_data: u64,
    max_shards: u64,
    shard_targets: u64,
    known_buckets: u64,
    unknown_bucket_calls: u64,
    rpc_attempts: u64,
    grouped_reads: u64,
    local_commits: u64,
    distributed_commits: u64,
    commit_errors: u64,
    total_us: u64,
}

impl Entry {
    fn new(descriptor: Descriptor) -> Self {
        Self {
            descriptor,
            seen: 0,
            calls: 0,
            errors: 0,
            local: 0,
            distributed: 0,
            no_data: 0,
            max_shards: 0,
            shard_targets: 0,
            known_buckets: 0,
            unknown_bucket_calls: 0,
            rpc_attempts: 0,
            grouped_reads: 0,
            local_commits: 0,
            distributed_commits: 0,
            commit_errors: 0,
            total_us: 0,
        }
    }
}

#[derive(Default)]
struct Registry {
    entries: HashMap<String, Entry>,
    tick: u64,
    evictions: u64,
}

#[derive(Default)]
pub(crate) struct Metrics {
    registry: Mutex<Registry>,
    tracked: AtomicU64,
    untracked: AtomicU64,
    cancelled: AtomicU64,
}

impl Metrics {
    fn record(&self, trace: Trace, error: bool, micros: u64) {
        let Some(descriptor) = trace.descriptor.filter(|_| !trace.disabled) else {
            self.untracked.fetch_add(1, Ordering::Relaxed);
            return;
        };
        self.tracked.fetch_add(1, Ordering::Relaxed);
        let mut r = self.registry.lock();
        r.tick = r.tick.wrapping_add(1);
        let tick = r.tick;
        if !r.entries.contains_key(&descriptor.digest) {
            if r.entries.len() == CAPACITY {
                if let Some(old) = r
                    .entries
                    .values()
                    .min_by_key(|e| e.seen)
                    .map(|e| e.descriptor.digest.clone())
                {
                    r.entries.remove(&old);
                    r.evictions = r.evictions.saturating_add(1);
                }
            }
            r.entries
                .insert(descriptor.digest.clone(), Entry::new(descriptor.clone()));
        }
        let e = r
            .entries
            .get_mut(&descriptor.digest)
            .expect("inserted or existing digest");
        e.seen = tick;
        e.calls = e.calls.saturating_add(1);
        e.errors = e.errors.saturating_add(u64::from(error));
        let n = trace.shards.len() as u64;
        e.no_data = e.no_data.saturating_add(u64::from(n == 0));
        e.local = e.local.saturating_add(u64::from(n == 1));
        e.distributed = e.distributed.saturating_add(u64::from(n > 1));
        e.max_shards = e.max_shards.max(n);
        e.shard_targets = e.shard_targets.saturating_add(n);
        e.known_buckets = e.known_buckets.saturating_add(trace.buckets.len() as u64);
        e.unknown_bucket_calls = e.unknown_bucket_calls.saturating_add(u64::from(trace.unknown_buckets));
        e.rpc_attempts = e.rpc_attempts.saturating_add(trace.rpc_attempts);
        e.grouped_reads = e.grouped_reads.saturating_add(trace.grouped_reads);
        if let Some(parts) = trace.commit_parts {
            if error {
                e.commit_errors = e.commit_errors.saturating_add(1);
            } else {
                e.local_commits = e.local_commits.saturating_add(u64::from(parts == 1));
                e.distributed_commits = e.distributed_commits.saturating_add(u64::from(parts > 1));
            }
        }
        e.total_us = e.total_us.saturating_add(micros);
    }

    pub(crate) fn counters(&self) -> Vec<(&'static str, u64)> {
        let r = self.registry.lock();
        vec![
            ("Tarn_query_digest_capacity", CAPACITY as u64),
            ("Tarn_query_digest_entries", r.entries.len() as u64),
            ("Tarn_query_digest_evictions", r.evictions),
            ("Tarn_query_digest_tracked", self.tracked.load(Ordering::Relaxed)),
            ("Tarn_query_digest_untracked", self.untracked.load(Ordering::Relaxed)),
            ("Tarn_query_digest_cancelled", self.cancelled.load(Ordering::Relaxed)),
        ]
    }

    pub(crate) fn rows(&self) -> Vec<Vec<Value>> {
        let mut entries: Vec<_> = self.registry.lock().entries.values().cloned().collect();
        entries.sort_by(|a, b| {
            b.calls
                .cmp(&a.calls)
                .then(a.descriptor.digest.cmp(&b.descriptor.digest))
        });
        entries
            .into_iter()
            .map(|e| {
                let mut row = vec![
                    Value::str(&e.descriptor.digest),
                    Value::str(&e.descriptor.db),
                    Value::str(&e.descriptor.sample),
                ];
                row.extend(
                    [
                        e.calls,
                        e.errors,
                        e.no_data,
                        e.local,
                        e.distributed,
                        e.max_shards,
                        e.shard_targets,
                        e.known_buckets,
                        e.unknown_bucket_calls,
                        e.rpc_attempts,
                        e.grouped_reads,
                        e.local_commits,
                        e.distributed_commits,
                        e.commit_errors,
                        e.total_us,
                    ]
                    .map(Value::UInt),
                );
                row
            })
            .collect()
    }
}

struct Completion<'a> {
    metrics: &'a Metrics,
    done: bool,
}

impl Completion<'_> {
    fn finish(&mut self) {
        self.done = true;
    }
}

impl Drop for Completion<'_> {
    fn drop(&mut self) {
        if !self.done {
            self.metrics.cancelled.fetch_add(1, Ordering::Relaxed);
        }
    }
}

pub(crate) async fn track<T>(metrics: &Metrics, future: impl Future<Output = SqlResult<T>>) -> SqlResult<T> {
    if CURRENT.try_with(|_| ()).is_ok() {
        return future.await;
    }
    let mut completion = Completion { metrics, done: false };
    CURRENT
        .scope(RefCell::new(Trace::default()), async move {
            let start = Instant::now();
            let result = future.await;
            let trace = CURRENT.with(|cell| cell.take());
            metrics.record(
                trace,
                result.is_err(),
                start.elapsed().as_micros().min(u64::MAX as u128) as u64,
            );
            completion.finish();
            result
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prepared_and_text_literals_share_a_redacted_descriptor() {
        let a = Descriptor::sql(Some("d"), "select * from t where id=?1 and s='private'").unwrap();
        let b = Descriptor::sql(Some("d"), "SELECT * FROM t WHERE id=42 AND s=X'1234'").unwrap();
        assert_eq!(a.digest, b.digest);
        assert!(!a.sample.contains("private") && !b.sample.contains("1234"));
        let hex = Descriptor::sql(None, "SELECT 0x1234 /* secret */").unwrap();
        assert_eq!(hex.sample, "SELECT ?");
        assert_eq!(
            Descriptor::sql(None, "SELECT TRUE,FALSE,NULL").unwrap().sample,
            "SELECT ? , ? , ?"
        );
    }

    #[tokio::test]
    async fn isolates_concurrent_queries_and_preserves_errors() {
        let m = Metrics::default();
        let (a, b) = tokio::join!(
            track(&m, async {
                describe(Descriptor::new(Some("d"), "SELECT ?"));
                shard(1);
                bucket(Some(7));
                rpc();
                tokio::task::yield_now().await;
                shard(1);
                Ok(())
            }),
            track(&m, async {
                describe(Descriptor::new(Some("d"), "UPDATE t SET v = ?"));
                shard(1);
                shard(2);
                bucket(None);
                commit(2);
                Err::<(), _>(tarn_common::SqlError::new(1180, "40003", "unknown"))
            }),
        );
        assert!(a.is_ok());
        assert_eq!(b.unwrap_err().sqlstate, *b"40003");
        let r = m.registry.lock();
        assert_eq!(r.entries.len(), 2);
        let read = r
            .entries
            .values()
            .find(|e| e.descriptor.sample.starts_with("SELECT"))
            .unwrap();
        assert_eq!(
            (read.calls, read.local, read.max_shards, read.rpc_attempts),
            (1, 1, 1, 1)
        );
        let write = r
            .entries
            .values()
            .find(|e| e.descriptor.sample.starts_with("UPDATE"))
            .unwrap();
        assert_eq!((write.distributed, write.errors, write.commit_errors), (1, 1, 1));
        assert_eq!(m.cancelled.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn bounds_cardinality_and_sample_bytes() {
        let m = Metrics::default();
        for i in 0..CAPACITY + 20 {
            track(&m, async {
                describe(Descriptor::new(Some("d"), &format!("SELECT {}{}", "雪".repeat(300), i)));
                for b in 1..=BUCKET_CAPACITY + 10 {
                    bucket(Some(b as u32));
                }
                Ok(())
            })
            .await
            .unwrap();
        }
        let r = m.registry.lock();
        assert_eq!(r.entries.len(), CAPACITY);
        assert_eq!(r.evictions, 20);
        assert!(r.entries.values().all(|e| e.descriptor.sample.len() <= SAMPLE_BYTES
            && e.known_buckets == BUCKET_CAPACITY as u64
            && e.unknown_bucket_calls == 1));
    }

    #[tokio::test]
    async fn cancellation_and_nested_execution_are_explicit() {
        let m = Metrics::default();
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(1),
            track(&m, async {
                describe(Descriptor::new(None, "SELECT ?"));
                std::future::pending::<SqlResult<()>>().await
            }),
        )
        .await;
        assert!(result.is_err());
        assert_eq!(m.cancelled.load(Ordering::Relaxed), 1);
        track(&m, async {
            describe(Descriptor::new(None, "SELECT outer"));
            track(&m, async {
                describe(Descriptor::new(None, "SELECT inner"));
                shard(2);
                Ok(())
            })
            .await
        })
        .await
        .unwrap();
        assert_eq!(m.registry.lock().entries.len(), 1);
        assert_eq!(m.cancelled.load(Ordering::Relaxed), 1);
    }
}
