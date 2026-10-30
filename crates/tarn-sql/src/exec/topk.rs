//! Bounded selection for an unindexed ORDER BY/LIMIT scan. Each shard
//! retains at most OFFSET+LIMIT candidates; ordinary final sorting merges
//! those candidates and preserves the original order of equal keys.
use super::*;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use tarn_common::ResourceGuard;

#[derive(Clone, Copy)]
pub(super) struct Spec<'a> {
    pub want: usize,
    pub order: &'a [(Expr, bool)],
}

struct Ranked {
    key: Vec<Value>,
    row: Row,
    seq: u64,
    desc: Arc<[bool]>,
    _guard: Option<ResourceGuard>,
}
impl PartialEq for Ranked {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}
impl Eq for Ranked {}
impl PartialOrd for Ranked {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Ranked {
    fn cmp(&self, other: &Self) -> Ordering {
        for ((a, b), desc) in self.key.iter().zip(&other.key).zip(self.desc.iter()) {
            let order = sort_cmp(a, b);
            let order = if *desc { order.reverse() } else { order };
            if order.is_ne() {
                return order;
            }
        }
        self.seq.cmp(&other.seq)
    }
}

pub(super) struct TopK<'a> {
    spec: Spec<'a>,
    desc: Arc<[bool]>,
    seq: u64,
    heap: BinaryHeap<Ranked>,
    _slots: Option<ResourceGuard>,
}

impl<'a> TopK<'a> {
    pub fn new(spec: Spec<'a>) -> SqlResult<Self> {
        let slots = crate::budget::retain((spec.want as u64).saturating_mul(192))?;
        Ok(Self {
            spec,
            desc: spec.order.iter().map(|(_, d)| *d).collect(),
            seq: 0,
            heap: BinaryHeap::with_capacity(spec.want),
            _slots: slots,
        })
    }

    pub fn push(&mut self, row: Row, cx: &EvalCtx<'_>) -> SqlResult<()> {
        crate::budget::work(
            (self.spec.want.max(1).ilog2() as u64 + 1)
                .saturating_mul(self.spec.order.len() as u64)
                .saturating_mul(4),
        )?;
        crate::budget::charge((self.spec.order.len() as u64).saturating_mul(128))?;
        let key = self
            .spec
            .order
            .iter()
            .map(|(e, _)| e.eval(&row, cx))
            .collect::<SqlResult<Vec<_>>>()?;
        let cost = (crate::budget::row_bytes(&row) + crate::budget::row_bytes(&key)).saturating_mul(4);
        let mut ranked = Ranked {
            key,
            row,
            seq: self.seq,
            desc: self.desc.clone(),
            _guard: None,
        };
        self.seq += 1;
        if self.heap.len() == self.spec.want && self.heap.peek().is_some_and(|worst| ranked >= *worst) {
            return Ok(());
        }
        // Dropping the evicted row releases its reservation before admitting
        // its replacement. No reference to a discarded page remains.
        if self.heap.len() == self.spec.want {
            self.heap.pop();
        }
        ranked._guard = crate::budget::retain(cost)?;
        ranked.row = crate::budget::own_row(ranked.row);
        ranked.key = crate::budget::own_row(ranked.key);
        self.heap.push(ranked);
        Ok(())
    }

    pub fn finish(self) -> SqlResult<Vec<Row>> {
        let cost = self
            .heap
            .iter()
            .fold(0u64, |n, r| n.saturating_add(crate::budget::row_bytes(&r.row) * 2));
        crate::budget::persistent(cost)?;
        Ok(self.heap.into_sorted_vec().into_iter().map(|r| r.row).collect())
    }
}

pub(super) async fn select(
    cluster: &Arc<Cluster>,
    mode: &mut ReadMode<'_>,
    p: &SelectPlan,
    cx: &EvalCtx<'_>,
) -> SqlResult<Option<Vec<Row>>> {
    if !cluster.cfg.resources.topk {
        return Ok(None);
    }
    let Source::Table(ta) = &p.source else { return Ok(None) };
    if p.agg.is_some()
        || p.having.is_some()
        || p.distinct
        || p.order.is_empty()
        || p.lock.is_some()
        || ta.table.is_virtual()
        || ta.limit.is_some()
        || !(matches!(&ta.path, AccessPath::Full | AccessPath::Range { .. })
            || matches!(&ta.path, AccessPath::Prefixes { keys, .. } if keys.len()==1))
    {
        return Ok(None);
    }
    fn column(expr: &Expr) -> Option<usize> {
        match expr {
            Expr::Col(i) => Some(*i),
            Expr::Key(e, _) | Expr::Collate(e, _) => column(e),
            _ => None,
        }
    }
    if p.order.iter().any(|(e, _)| {
        column(e).is_none_or(|i| {
            ta.table
                .columns
                .get(i)
                .is_none_or(|c| matches!(c.ty().base, BaseType::Float | BaseType::Double))
        })
    }) {
        return Ok(None);
    }
    if let ReadMode::Txn(t) | ReadMode::Committed(t) | ReadMode::Statement(t) = &*mode {
        if t.writes.keys().any(|(sid, _)| *sid == ta.table.space_id) {
            return Ok(None);
        }
    }
    let Some(limit) = eval_limit(&p.limit, cx)? else {
        return Ok(None);
    };
    let want = limit
        .checked_add(eval_limit(&p.offset, cx)?.unwrap_or(0))
        .and_then(|n| usize::try_from(n).ok())
        .ok_or_else(|| crate::budget::exhausted("ORDER BY offset/limit"))?;
    if want == 0 {
        return Ok(None);
    }
    // Admission precedes heap allocation, including large offsets with no
    // qualifying rows. The retained payload is admitted row by row.
    let admission = crate::budget::retain((want as u64).saturating_mul(192))?;
    drop(admission);
    let map = cluster.bucket_map();
    let shards = cluster.live_shards().collect::<Vec<_>>();
    let calls = range_calls_in(&map, &shards, &ta.table, &ta.path, &[], cx, None, None)?;
    let none = BTreeSet::new();
    let keep = Keep {
        filter: p.filter.as_ref(),
        cx,
        t: &ta.table,
        written: &none,
        project: None,
        topk: Some(Spec { want, order: &p.order }),
    };
    let scans = read_calls(cluster, &map, mode, &ta.table, path_index(&ta.path), calls, false, keep).await?;
    let rows = scans.into_iter().flat_map(|s| s.rows).collect();
    cluster.stats.topk_scans.fetch_add(1, AtomicOrdering::Relaxed);
    finish_select(rows, p, cx).map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heap_matches_stable_sort_with_nulls_ties_and_mixed_directions() {
        let cx = EvalCtx {
            params: &[],
            db: Some("d"),
            user: "u",
            conn_id: 1,
            last_insert_id: 0,
            found_rows: 0,
            row_count: 0,
            now: "2026-01-02 03:04:05",
        };
        let mut seed = 19u64;
        let rows: Vec<Row> = (0..257)
            .map(|i| {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                vec![
                    if seed.is_multiple_of(7) {
                        Value::Null
                    } else {
                        Value::Int((seed % 13) as i64)
                    },
                    Value::str(&format!("k{}", seed % 3)),
                    Value::Int(i),
                ]
            })
            .collect();
        for desc in [[false, false], [true, false], [false, true], [true, true]] {
            let order = vec![(Expr::Col(0), desc[0]), (Expr::Col(1), desc[1])];
            let mut expected = rows.clone();
            sort_rows(&mut expected, &order, &cx).unwrap();
            for want in [1, 2, 7, 64, 256, 300] {
                let mut heap = TopK::new(Spec { want, order: &order }).unwrap();
                for row in &rows {
                    heap.push(row.clone(), &cx).unwrap();
                }
                assert_eq!(heap.finish().unwrap(), expected[..want.min(rows.len())]);
            }
        }
    }
}
