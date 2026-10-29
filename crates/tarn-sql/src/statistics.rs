//! Bounded, optional planning hints. They are neither transaction reads nor
//! correctness evidence: primary-page samples are biased, vinyl sizes are
//! approximate, and concurrent writes/moves can change every estimate.

use crate::catalog::TableDef;
use crate::cluster::Cluster;
use crate::mpw::{lua_status, proto, Mp};
use crate::plan::AccessPath;
use parking_lot::Mutex;
use serde_json::{json, Value as Json};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tarn_common::{SqlError, SqlResult, Value};

pub const TABLE_CAPACITY: usize = 32;
const SAMPLE_ROWS: usize = 512;
const SAMPLE_COLUMNS: usize = 16;
const INDEX_CAPACITY: usize = 8;
const PREFIX_PARTS: usize = 3;
const TTL: Duration = Duration::from_secs(300);

#[derive(Clone, Debug)]
pub struct Prefix {
    pub index: u32,
    pub parts: usize,
    pub observed_distinct: usize,
    pub distinct: f64,
}

#[derive(Clone, Debug)]
pub struct TableStats {
    pub space: u32,
    pub version: u64,
    pub rows: f64,
    pub sampled: usize,
    pub shards: usize,
    pub prefixes: Vec<Prefix>,
    pub collected: Instant,
}

impl TableStats {
    pub fn estimate(&self, t: &TableDef, path: &AccessPath) -> Option<f64> {
        if self.version != t.ver || self.space != t.space_id {
            return None;
        }
        let (index, parts, keys) = match path {
            AccessPath::Full => return Some(self.rows),
            AccessPath::Point { keys, .. } => return Some((keys.len() as f64).min(self.rows)),
            AccessPath::Range { index, eq, lo, hi, .. } if lo.is_none() && hi.is_none() => (*index, eq.len(), 1),
            AccessPath::Prefixes { index, keys } => (*index, keys.first()?.len(), keys.len()),
            _ => return None,
        };
        if parts == 0 {
            return Some(self.rows);
        }
        let p = self
            .prefixes
            .iter()
            .filter(|p| p.index == index && p.parts <= parts)
            .max_by_key(|p| p.parts)?;
        Some((self.rows / p.distinct.max(1.0) * keys as f64).min(self.rows))
    }

    pub fn diagnostic(&self, t: &TableDef, path: &AccessPath) -> Json {
        json!({"estimated_rows_per_probe":self.estimate(t,path), "table_rows":self.rows,
            "sample_rows":self.sampled,"age_ms":self.collected.elapsed().as_millis(),
            "source":"ANALYZE primary-page sample; advisory, biased, not a snapshot or an examined-row count"})
    }
}

#[derive(Clone, Default)]
pub struct Snapshot {
    pub generation: u64,
    pub tables: HashMap<u32, Arc<TableStats>>,
}

#[derive(Default)]
struct State {
    catalog: u64,
    snapshot: Arc<Snapshot>,
}

#[derive(Default)]
pub struct Statistics {
    state: Mutex<State>,
    // One collector per gateway; a queue of ANALYZE requests is not a bound.
    pub(crate) collecting: tokio::sync::Mutex<()>,
}

impl Statistics {
    pub fn snapshot(&self, catalog: u64) -> Arc<Snapshot> {
        self.snapshot_at(catalog, Instant::now())
    }

    fn snapshot_at(&self, catalog: u64, now: Instant) -> Arc<Snapshot> {
        let mut s = self.state.lock();
        let changed = s.catalog != catalog;
        if changed
            || s.snapshot
                .tables
                .values()
                .any(|t| now.saturating_duration_since(t.collected) >= TTL)
        {
            let snapshot = Arc::make_mut(&mut s.snapshot);
            if changed {
                snapshot.tables.clear();
            } else {
                snapshot
                    .tables
                    .retain(|_, t| now.saturating_duration_since(t.collected) < TTL);
            }
            snapshot.generation = snapshot.generation.wrapping_add(1);
        }
        s.catalog = catalog;
        s.snapshot.clone()
    }

    pub(crate) fn publish(&self, catalog: u64, tables: Vec<TableStats>) {
        let _ = self.snapshot(catalog);
        let mut s = self.state.lock();
        // The caller also checks the catalog. A concurrent reload/publish
        // can only discard these hints, never make them schema evidence.
        if s.catalog != catalog {
            return;
        }
        let snapshot = Arc::make_mut(&mut s.snapshot);
        for t in tables {
            if !snapshot.tables.contains_key(&t.space) && snapshot.tables.len() == TABLE_CAPACITY {
                if let Some(id) = snapshot.tables.values().min_by_key(|t| t.collected).map(|t| t.space) {
                    snapshot.tables.remove(&id);
                }
            }
            snapshot.tables.insert(t.space, Arc::new(t));
        }
        snapshot.generation = snapshot.generation.wrapping_add(1);
    }
}

/// Only integer index prefixes are sampled in this first version. This
/// keeps transport, retained key widths and cross-language comparisons
/// bounded without approximating text collations or DECIMAL semantics.
pub(crate) async fn collect(cluster: &Cluster, t: &TableDef) -> SqlResult<TableStats> {
    let shards: Vec<_> = cluster.live_shards().collect();
    if shards.is_empty() || shards.len() > SAMPLE_ROWS {
        return Err(SqlError::unsupported("ANALYZE supports 1..512 live shards"));
    }
    let mut cols = BTreeSet::new();
    let mut prefixes = Vec::new();
    for index in t
        .indexes
        .iter()
        .filter(|i| !i.invisible && i.lazy.is_none())
        .take(INDEX_CAPACITY)
    {
        for (part, &c) in index.cols.iter().take(PREFIX_PARTS).enumerate() {
            if !t.columns[c].ty().is_integer() || (!cols.contains(&c) && cols.len() == SAMPLE_COLUMNS) {
                break;
            }
            cols.insert(c);
            prefixes.push((index.id, index.cols[..=part].to_vec()));
        }
    }
    let cols: Vec<_> = cols.into_iter().collect();
    let mut rows = 0.0;
    let mut samples = Vec::new();
    let limit = (SAMPLE_ROWS / shards.len()).min(256);
    for &shard in &shards {
        let mut m = Mp::new();
        m.arr(4)
            .uint(t.space_id as u64)
            .uint(t.ver)
            .uint(limit as u64)
            .arr(cols.len());
        for &c in &cols {
            m.uint(t.field_of(c) as u64 + 1);
        }
        let data = cluster.call(shard, "tarn_analyze", &m.0).await?;
        // Bound even a mismatched/malformed optional procedure's reply.
        if data.len() > 128 * 1024 {
            return Err(proto("ANALYZE reply exceeded its byte budget"));
        }
        let (_, mut r) = lua_status(&data)?;
        rows += match r.value()? {
            Value::UInt(n) => n as f64,
            Value::Int(n) if n >= 0 => n as f64,
            _ => return Err(proto("invalid ANALYZE size")),
        };
        let n = r.array()?;
        if n > limit {
            return Err(proto("ANALYZE sample exceeded its row budget"));
        }
        for _ in 0..n {
            if r.array()? != cols.len() {
                return Err(proto("ANALYZE sample changed its column count"));
            }
            let mut row = Vec::with_capacity(cols.len());
            for _ in &cols {
                row.push(match r.value()? {
                    Value::Int(v) => Some(v as i128),
                    Value::UInt(v) => Some(v as i128),
                    Value::Null => None,
                    _ => return Err(proto("noninteger ANALYZE sample")),
                });
            }
            samples.push(row);
        }
        if !r.buf.is_empty() {
            return Err(proto("trailing ANALYZE data"));
        }
    }
    let mut out = TableStats {
        space: t.space_id,
        version: t.ver,
        rows,
        sampled: samples.len(),
        shards: shards.len(),
        prefixes: Vec::new(),
        collected: Instant::now(),
    };
    for (index, parts) in prefixes {
        let positions: Vec<_> = parts
            .iter()
            .map(|c| cols.iter().position(|v| v == c).unwrap())
            .collect();
        let mut counts = HashMap::<Vec<Option<i128>>, usize>::new();
        for row in &samples {
            *counts.entry(positions.iter().map(|&c| row[c]).collect()).or_default() += 1;
        }
        let observed = counts.len();
        let singletons = counts.values().filter(|&&n| n == 1).count();
        // A cheap extrapolation, not an unbiased estimator. Zero samples,
        // skew and the leading-page bias are visible through diagnostics.
        let distinct = (observed as f64
            + singletons as f64 * (rows - samples.len() as f64).max(0.0) / samples.len().max(1) as f64)
            .clamp(1.0, rows.max(1.0));
        out.prefixes.push(Prefix {
            index,
            parts: parts.len(),
            observed_distinct: observed,
            distinct,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(space: u32, collected: Instant) -> TableStats {
        TableStats {
            space,
            version: 1,
            rows: 1000.0,
            sampled: 256,
            shards: 2,
            prefixes: vec![],
            collected,
        }
    }

    #[test]
    fn bounds_cache_and_invalidates_generations_on_expiry_and_schema() {
        let stats = Statistics::default();
        let now = Instant::now();
        for n in 0..TABLE_CAPACITY + 5 {
            stats.publish(1, vec![table(n as u32, now + Duration::from_millis(n as u64))]);
        }
        let s = stats.snapshot_at(1, now);
        assert_eq!(s.tables.len(), TABLE_CAPACITY);
        assert!(!s.tables.contains_key(&0));
        assert!(s.tables.contains_key(&((TABLE_CAPACITY + 4) as u32)));
        let expired = stats.snapshot_at(1, now + TTL + Duration::from_secs(1));
        assert!(expired.tables.is_empty());
        assert!(expired.generation > s.generation);
        stats.publish(1, vec![table(99, Instant::now())]);
        let s = stats.snapshot(1);
        let changed = stats.snapshot(2);
        assert!(changed.tables.is_empty());
        assert!(changed.generation > s.generation);
    }
}
