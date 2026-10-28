//! Exact, deliberately narrow extensions of the existing range-read contract.
//! Full scan coverage is retained even when no row passes the predicate.

use super::*;
use crate::txn::{reduce_rows, Reduced, StorageAgg};

fn table(p: &SelectPlan) -> Option<&TableAccess> {
    let Source::Table(ta) = &p.source else { return None };
    if p.lock.is_some() || ta.limit.is_some() || ta.table.is_virtual() || !pushes_filter(&ta.path) {
        return None;
    }
    Some(ta)
}

fn aggregates(p: &SelectPlan) -> Option<Vec<StorageAgg>> {
    let ta = table(p)?;
    let ap = p.agg.as_ref()?;
    if !ap.group.is_empty() || ap.aggs.is_empty() || ap.aggs.len() > 256 {
        return None;
    }
    ap.aggs
        .iter()
        .map(|a| {
            if a.distinct || !a.order.is_empty() {
                return None;
            }
            let col = match (&a.arg, a.func) {
                (_, AggFunc::CountStar) => None,
                (Some(Expr::Col(c)), _) if *c < ta.table.columns.len() => Some(*c),
                _ => return None,
            };
            let op = match a.func {
                AggFunc::CountStar => "countstar",
                AggFunc::Count => "count",
                AggFunc::Sum | AggFunc::Min | AggFunc::Max => {
                    let ty = ta.table.columns[col?].ty();
                    if !ty.is_integer() || ty.base == BaseType::Bool {
                        return None;
                    }
                    match a.func {
                        AggFunc::Sum => "sum",
                        AggFunc::Min => "min",
                        _ => "max",
                    }
                }
                _ => return None,
            };
            Some(StorageAgg { col, op })
        })
        .collect()
}

/// Merge exact integer partials. SUM's wire representation is an integer
/// string, never a floating-point value or a rounded DECIMAL partial.
fn merge(states: &mut [AggState], aggs: &[AggCall], values: Vec<Value>) -> SqlResult<()> {
    for ((state, agg), value) in states.iter_mut().zip(aggs).zip(values) {
        match state {
            AggState::Count(n) => {
                let k = match value {
                    Value::Int(k) if k >= 0 => k as u64,
                    Value::UInt(k) => k,
                    _ => return Err(crate::mpw::proto("invalid partial count")),
                };
                *n = n
                    .checked_add(k)
                    .ok_or_else(|| crate::mpw::proto("partial count overflow"))?;
            }
            AggState::SumInt(sum, any) if !value.is_null() => {
                let Value::Str(s) = value else {
                    return Err(crate::mpw::proto("invalid partial sum"));
                };
                let k = std::str::from_utf8(&s)
                    .ok()
                    .and_then(|s| s.parse::<i128>().ok())
                    .ok_or_else(|| crate::mpw::proto("invalid integer partial sum"))?;
                *sum = sum
                    .checked_add(k)
                    .ok_or_else(|| crate::mpw::proto("partial sum overflow"))?;
                *any = true;
            }
            AggState::SumInt(_, _) => {}
            AggState::Min(_) | AggState::Max(_) if matches!(value, Value::Null | Value::Int(_) | Value::UInt(_)) => {
                state.add(agg.func, agg.coll, Some(value));
            }
            _ => return Err(crate::mpw::proto("invalid partial aggregate")),
        }
    }
    Ok(())
}

pub(super) async fn reduce(
    cluster: &Arc<Cluster>,
    mode: &mut ReadMode<'_>,
    p: &SelectPlan,
    cx: &EvalCtx<'_>,
) -> SqlResult<Option<Row>> {
    if matches!(mode, ReadMode::Committed(_)) {
        return Ok(None); // DML candidate reads retain their existing contract.
    }
    let Some(aggs) = aggregates(p) else { return Ok(None) };
    let Some(preds) = storage_preds(p, cx)? else {
        return Ok(None);
    };
    let ta = table(p).unwrap();
    let t = &ta.table;
    let ap = p.agg.as_ref().unwrap();
    let mut states: Vec<_> = ap.aggs.iter().map(AggState::new).collect();
    let mut written = Vec::new();
    if let ReadMode::Txn(txn) | ReadMode::Committed(txn) | ReadMode::Statement(txn) = &*mode {
        for (_, w) in txn
            .writes
            .range((t.space_id, Bytes::new())..)
            .take_while(|((s, _), _)| *s == t.space_id)
        {
            written.push((w.shard, w.pk.clone()));
            if let Some(r) = &w.row {
                if match &p.filter {
                    Some(f) => f.eval_bool(r, cx)?,
                    None => true,
                } {
                    for (state, a) in states.iter_mut().zip(&ap.aggs) {
                        state.add(a.func, a.coll, a.arg.as_ref().map(|e| e.eval(r, cx)).transpose()?);
                    }
                }
            }
        }
    }
    let map = cluster.bucket_map();
    let calls = range_calls_in(
        &map,
        &cluster.live_shards().collect::<Vec<_>>(),
        t,
        &ta.path,
        &[],
        cx,
        None,
        None,
    )?;
    // A reduction hides page boundaries. Keep multi-shard RC statements in
    // the same validated snapshot used by paginated ordinary aggregates,
    // rather than allowing independently captured partials to fracture it.
    if calls.len() > 1 && matches!(mode, ReadMode::Statement(_)) {
        return Err(need_snapshot());
    }
    let in_stream = matches!(mode, ReadMode::Txn(_));
    let routes = calls
        .iter()
        .map(|c| read_route(&map, in_stream, c.shard, &[c.bucket]))
        .collect::<SqlResult<Vec<_>>>()?;
    if let ReadMode::Auto { calls: made } = mode {
        if *made + calls.len() > 1 {
            return Err(need_txn());
        }
    }
    let mut requests = Vec::new();
    for c in &calls {
        let (client, id, begin) = match mode {
            ReadMode::Txn(txn) => {
                let st = txn.stream(cluster, c.shard).await?;
                (st.client, st.id, st.begin)
            }
            _ => (cluster.shards[c.shard].client(cluster).await?, 0, false),
        };
        requests.push((client, id, begin));
    }
    let futs = calls
        .iter()
        .zip(&routes)
        .zip(&requests)
        .map(|((c, route), (client, id, begin))| {
            let exclude: Vec<_> = written
                .iter()
                .filter(|(s, _)| *s == c.shard)
                .map(|(_, k)| k.as_slice())
                .collect();
            let preds = &preds;
            let aggs = &aggs;
            async move {
                reduce_rows(
                    client,
                    *id,
                    t,
                    path_index(&ta.path),
                    &c.key,
                    c.iter,
                    c.end.as_ref().map(|(k, i)| (k.as_slice(), *i)),
                    preds,
                    &exclude,
                    !in_stream,
                    *begin,
                    route,
                    aggs,
                )
                .await
            }
        });
    let mut fallback = false;
    for (c, result) in calls.iter().zip(futures::future::join_all(futs).await) {
        let result = match result {
            Ok(r) => r,
            Err(e) => {
                if let ReadMode::Txn(txn) = mode {
                    if e.code == 1213 {
                        txn.doomed = Some(e.clone());
                    }
                }
                return Err(e);
            }
        };
        if matches!(result, Reduced::Unavailable) {
            fallback = true;
            continue;
        }
        match mode {
            ReadMode::Txn(txn) => {
                txn.mark_begun(c.shard);
                if let Some(r) = &c.range {
                    txn.note_range(c.shard, r.clone());
                }
            }
            ReadMode::Auto { calls } => *calls += 1,
            _ => {}
        }
        cluster.stats.storage_reductions.fetch_add(1, AtomicOrdering::Relaxed);
        match result {
            Reduced::Complete(values) => merge(&mut states, &ap.aggs, values)?,
            Reduced::Incomplete => fallback = true,
            Reduced::Unavailable => unreachable!(),
        }
    }
    if fallback {
        // An incomplete read may have begun a stream. Keep its coverage;
        // only read-only work is repeated. Auto must restart in a stream.
        if matches!(mode, ReadMode::Auto { calls } if *calls > 0) {
            return Err(need_txn());
        }
        return Ok(None);
    }
    Ok(Some(
        states.into_iter().zip(&ap.aggs).map(|(s, a)| s.finish(a)).collect(),
    ))
}

pub(super) async fn project(
    cluster: &Arc<Cluster>,
    mode: &mut ReadMode<'_>,
    p: &SelectPlan,
    cx: &EvalCtx<'_>,
) -> SqlResult<Option<Vec<Row>>> {
    let Some(ta) = table(p) else { return Ok(None) };
    if p.agg.is_some()
        || !p.order.is_empty()
        || p.having.is_some()
        || p.distinct
        || p.limit.is_some()
        || p.offset.is_some()
        || !p.project.iter().all(|e| matches!(e, Expr::Col(_)))
    {
        return Ok(None);
    }
    let Some(preds) = storage_preds(p, cx)? else {
        return Ok(None);
    };
    let t = &ta.table;
    let cols = std::cell::RefCell::new(BTreeSet::new());
    for e in p.project.iter().chain(p.filter.iter()) {
        e.remap(&|c| {
            cols.borrow_mut().insert(c);
            Expr::Col(c)
        });
    }
    let mut cols = cols.into_inner();
    let mut boundary = BTreeSet::new();
    boundary.extend(&t.indexes[0].cols);
    boundary.extend(&index_def(t, path_index(&ta.path)).cols);
    cols.extend(&boundary);
    if cols.len() >= t.columns.len() || cols.len() > 256 {
        return Ok(None);
    }
    let project = Projection {
        cols: cols.into_iter().collect(),
        boundary: boundary.into_iter().collect(),
        preds,
    };
    let written = match &*mode {
        ReadMode::Txn(txn) | ReadMode::Committed(txn) | ReadMode::Statement(txn) => txn.written_keys(t),
        _ => BTreeSet::new(),
    };
    let keep = Keep {
        filter: p.filter.as_ref(),
        cx,
        t,
        written: &written,
        project: Some(&project),
    };
    let map = cluster.bucket_map();
    let calls = range_calls_in(
        &map,
        &cluster.live_shards().collect::<Vec<_>>(),
        t,
        &ta.path,
        &[],
        cx,
        None,
        None,
    )?;
    let scans = read_calls(cluster, &map, mode, t, path_index(&ta.path), calls, false, keep).await?;
    cluster.stats.storage_projections.fetch_add(1, AtomicOrdering::Relaxed);
    let mut rows: Vec<_> = scans.into_iter().flat_map(|s| s.rows).collect();
    if let ReadMode::Txn(txn) | ReadMode::Committed(txn) | ReadMode::Statement(txn) = mode {
        rows = txn.unwritten(t, rows);
        for r in txn.own_rows(t.space_id) {
            if match &p.filter {
                Some(f) => f.eval_bool(r, cx)?,
                None => true,
            } {
                rows.push(r.clone());
            }
        }
    }
    Ok(Some(rows))
}
