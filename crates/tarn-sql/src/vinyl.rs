//! Vinyl admission replies and cached pressure statistics. Admission is
//! authoritative on storage; samples never grant a write permit. Only an
//! explicit before-effect refusal may retry (VinylAdmission.tla).

use crate::cluster::Cluster;
use crate::mpw::{proto, Rd};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tarn_common::{SqlError, SqlResult, Value};

const FIELDS: &[&str] = &[
    "dump_bandwidth",
    "write_rate",
    "rate_limit",
    "blocked_writers",
    "memory_level0",
    "memory_tuple",
    "memory_tx",
    "memory_limit",
    "compaction_queue",
    "run_count",
    "range_count",
    "runs_per_range",
    "admitted",
    "rejected",
    "quota_timeouts",
];
const STALE_AFTER: Duration = Duration::from_secs(5);

/// Wait only after an unambiguous before-effect refusal. Ordinary 1205,
/// a lost reply, or an unknown commit outcome never enters this retry loop.
#[derive(Default)]
pub(crate) struct RetryBudget {
    deadline: Option<Instant>,
    retries: u8,
}

impl RetryBudget {
    pub(crate) async fn pause(&mut self, data: &[u8]) -> SqlResult<bool> {
        let Some(delay) = refusal_delay(data)? else {
            return Ok(false);
        };
        let now = Instant::now();
        let deadline = *self.deadline.get_or_insert(now + Duration::from_millis(250));
        if now >= deadline || self.retries >= 32 {
            return Ok(false);
        }
        self.retries += 1;
        tokio::time::sleep(delay.min(deadline - now)).await;
        Ok(Instant::now() < deadline)
    }
}

fn refusal_delay(data: &[u8]) -> SqlResult<Option<Duration>> {
    if !crate::mpw::status_is(data, "throttled") {
        return Ok(None);
    }
    let decode = || -> SqlResult<Duration> {
        let mut rd = Rd::new(data);
        if rd.array()? != 1 || rd.array()? != 4 || rd.string()? != "throttled" {
            return Err(proto("invalid admission refusal envelope"));
        }
        if !matches!(rd.value()?, Value::Int(1205) | Value::UInt(1205)) || !matches!(rd.value()?, Value::Str(_)) {
            return Err(proto("invalid admission refusal"));
        }
        let millis = match rd.value()? {
            Value::Int(n @ 1..=50) => n as u64,
            Value::UInt(n @ 1..=50) => n,
            _ => return Err(proto("invalid admission retry delay")),
        };
        if !rd.buf.is_empty() {
            return Err(proto("trailing admission refusal data"));
        }
        Ok(Duration::from_millis(millis))
    };
    decode().map(Some).map_err(|_| {
        SqlError::new(
            1180,
            "40003",
            "Invalid vinyl admission refusal; the write outcome cannot be certified",
        )
    })
}

pub(crate) struct StatsSample {
    at: Instant,
    values: BTreeMap<String, Value>,
}

fn decode_stats(data: &[u8]) -> SqlResult<StatsSample> {
    let mut rd = Rd::new(data);
    if rd.array()? != 1 || rd.array()? != 2 || rd.string()? != "ok" {
        return Err(proto("invalid vinyl statistics reply"));
    }
    let count = rd.map()?;
    if count > 64 {
        return Err(proto("too many vinyl statistics"));
    }
    let mut values = BTreeMap::new();
    for _ in 0..count {
        let key = match rd.value()? {
            Value::Str(s) => String::from_utf8(s.to_vec()).map_err(proto)?,
            _ => return Err(proto("invalid vinyl statistic name")),
        };
        let value = rd.value()?;
        if FIELDS.contains(&key.as_str())
            && !matches!(&value, Value::Null | Value::UInt(_) | Value::Int(0..))
            && !matches!(&value, Value::Float(n) if n.is_finite() && *n >= 0.0)
        {
            return Err(proto("invalid vinyl statistic value"));
        }
        if values.insert(key, value).is_some() {
            return Err(proto("duplicate vinyl statistic"));
        }
    }
    if !rd.buf.is_empty() {
        return Err(proto("trailing vinyl statistics data"));
    }
    Ok(StatsSample {
        at: Instant::now(),
        values,
    })
}

pub(crate) fn start_stats(cluster: &Arc<Cluster>) {
    let weak = Arc::downgrade(cluster);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let Some(c) = weak.upgrade() else { break };
            if !c
                .catalog()
                .all()
                .any(|table| matches!(table.engine, crate::catalog::Engine::Vinyl))
            {
                c.vinyl_samples.lock().clear();
                continue;
            }
            let polls = c.live_shards().map(|shard| {
                let c = &c;
                async move {
                    let id = c.shards[shard].id;
                    let sample =
                        tokio::time::timeout(Duration::from_secs(3), c.call(shard, "tarn_vinyl_stats", &[0x90]))
                            .await
                            .ok()
                            .and_then(Result::ok)
                            .and_then(|data| decode_stats(&data).ok());
                    (id, sample)
                }
            });
            let results = futures::future::join_all(polls).await;
            let live: BTreeSet<_> = c.live_shards().map(|s| c.shards[s].id).collect();
            let mut samples = c.vinyl_samples.lock();
            samples.retain(|id, _| live.contains(id));
            for (id, sample) in results {
                if live.contains(&id) {
                    if let Some(sample) = sample {
                        samples.insert(id, sample);
                    }
                }
            }
        }
    });
}

pub(crate) fn stats(cluster: &Cluster) -> Vec<(String, String)> {
    let samples = cluster.vinyl_samples.lock();
    let mut out = Vec::new();
    for shard in cluster.live_shards() {
        let id = cluster.shards[shard].id;
        let prefix = format!("Tarn_vinyl_shard_{id}");
        let sample = samples.get(&id);
        let age = sample.map(|s| s.at.elapsed());
        out.push((
            format!("{prefix}_sample"),
            match age {
                None => "unavailable",
                Some(age) if age > STALE_AFTER => "stale",
                _ => "current",
            }
            .into(),
        ));
        out.push((
            format!("{prefix}_sample_age_ms"),
            age.map_or_else(|| "NULL".into(), |age| age.as_millis().to_string()),
        ));
        for field in FIELDS {
            out.push((
                format!("{prefix}_{field}"),
                sample
                    .and_then(|s| s.values.get(*field))
                    .map_or_else(|| "NULL".into(), ToString::to_string),
            ));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mpw::Mp;

    #[test]
    fn only_explicit_before_effect_refusals_allow_retry() {
        for status in ["ok", "timeout", "error", "no"] {
            let mut m = Mp::new();
            m.arr(1).arr(3).str(status).uint(1205).str("timeout");
            assert_eq!(refusal_delay(&m.0).unwrap(), None);
        }
        let mut m = Mp::new();
        m.arr(1)
            .arr(4)
            .str("throttled")
            .uint(1205)
            .str("vinyl pressure")
            .uint(10);
        assert_eq!(refusal_delay(&m.0).unwrap(), Some(Duration::from_millis(10)));
    }

    #[test]
    fn malformed_admission_replies_never_certify_no_effect() {
        for (code, delay) in [(1180, 10), (1205, 0), (1205, 51)] {
            let mut m = Mp::new();
            m.arr(1)
                .arr(4)
                .str("throttled")
                .uint(code)
                .str("vinyl pressure")
                .uint(delay);
            let e = refusal_delay(&m.0).unwrap_err();
            assert_eq!(e.code, 1180);
            assert_eq!(&e.sqlstate, b"40003");
        }
        let mut m = Mp::new();
        m.arr(1)
            .arr(4)
            .str("throttled")
            .uint(1205)
            .str("vinyl pressure")
            .uint(10)
            .nil();
        assert!(refusal_delay(&m.0).is_err());
    }

    #[test]
    fn samples_keep_exact_counters_and_unknown_metrics() {
        let mut m = Mp::new();
        m.arr(1).arr(2).str("ok").map(3);
        m.str("rate_limit").uint(u64::MAX);
        m.str("runs_per_range").f64(1.5);
        m.str("memory_level0").nil();
        let sample = decode_stats(&m.0).unwrap();
        assert_eq!(sample.values["rate_limit"].to_string(), u64::MAX.to_string());
        assert_eq!(sample.values["runs_per_range"], Value::Float(1.5));
        assert_eq!(sample.values["memory_level0"], Value::Null);
        assert!(!sample.values.contains_key("blocked_writers"));
    }

    #[test]
    fn invalid_samples_cannot_appear_current() {
        for value in [
            Value::Float(f64::NAN),
            Value::Float(f64::INFINITY),
            Value::Int(-1),
            Value::str("0"),
        ] {
            let mut m = Mp::new();
            m.arr(1).arr(2).str("ok").map(1).str("rate_limit").val(&value);
            assert!(decode_stats(&m.0).is_err());
        }
        let mut duplicate = Mp::new();
        duplicate
            .arr(1)
            .arr(2)
            .str("ok")
            .map(2)
            .str("rate_limit")
            .uint(1)
            .str("rate_limit")
            .uint(2);
        assert!(decode_stats(&duplicate.0).is_err());
        let mut trailing = Mp::new();
        trailing.arr(1).arr(2).str("ok").map(0).nil();
        assert!(decode_stats(&trailing.0).is_err());
    }
}
