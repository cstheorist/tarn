//! Rebalancing (issue #25; lua/tarn.lua "Rebalance"): bucket moves toward
//! an even spread over the active shards -- of data (by size) or of
//! requests (by load) -- in the background, a limited number at a time and
//! at a limited rate of bytes.
//!
//! One rebalance runs at a time: its record on the first shard
//! (`tarn_rebalance_get` / `_put`) holds its state, limits and the moves of
//! the current round. The SQL server that started it plans a round of moves
//! from the shards' sizes or load, makes them (each a move job, as `TARN
//! MOVE BUCKET`), and plans the next, until no move evens the shards out
//! more. `TARN REBALANCE PAUSE`, `RESUME` and `CANCEL` change the record's
//! status, which the owner reads between moves; a server that finds the
//! record of an owner that stopped (not written for `ddl_job_stale`) takes
//! it over and goes on. Each of them reads the record, changes it and
//! writes it back if its version is still the one it read.

use crate::cluster::Cluster;
use crate::mpw::{lua_status, Mp, Rd};
use futures::stream::{FuturesUnordered, StreamExt};
use futures::FutureExt;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tarn_common::{SqlError, SqlResult};

/// The most moves a round plans: the shards' sizes are read again for the
/// next.
const ROUND: usize = 64;

/// The rebalance fails after this many moves in a row failed.
const MAX_FAILURES_IN_ROW: u32 = 10;

/// One move of a round.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PlannedMove {
    pub bucket: u32,
    /// Shard ids.
    pub from: u32,
    pub to: u32,
    /// The bucket's bytes when the round was planned.
    pub bytes: u64,
    /// 'planned', 'moving', 'done', 'failed' or 'cancelled' ('copying' and
    /// 'fenced' for a move in progress, in `rebalance_status`).
    pub state: String,
    pub error: String,
}

/// The rebalancer's record.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Rebalance {
    pub owner: String,
    /// 'running', 'paused', 'cancelled', 'done' or 'failed'.
    pub status: String,
    /// 'size' or 'load'.
    pub by: String,
    pub max_moves: u32,
    /// Bytes per second (0: no limit).
    pub max_rate: u64,
    /// Unix seconds.
    pub started: f64,
    pub done: u64,
    pub failed: u64,
    pub bytes: u64,
    pub message: String,
    pub moves: Vec<PlannedMove>,
    /// Seconds since it was last written (by the first shard's clock).
    pub age: f64,
    /// Its version, which each write raises (set by the first shard).
    pub ver: u64,
}

/// How a move of the rebalancer ended.
enum MoveEnd {
    Done,
    Failed(String),
    /// It waited for a job in its way (a schema change, a change of the
    /// cluster's shards) and gave up: planned again, for later.
    Retry(String),
}

fn now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

fn num(rd: &mut Rd) -> SqlResult<u64> {
    Ok(rd.value()?.as_i64().unwrap_or(0).max(0) as u64)
}

impl Rebalance {
    pub fn ended(&self) -> bool {
        matches!(self.status.as_str(), "done" | "cancelled" | "failed")
    }

    fn encode(&self, m: &mut Mp) {
        m.map(11);
        m.str("owner").str(&self.owner);
        m.str("status").str(&self.status);
        m.str("by").str(&self.by);
        m.str("max_moves").uint(self.max_moves as u64);
        m.str("max_rate").uint(self.max_rate);
        m.str("started").f64(self.started);
        m.str("done").uint(self.done);
        m.str("failed").uint(self.failed);
        m.str("bytes").uint(self.bytes);
        m.str("message").str(&self.message);
        m.str("moves").arr(self.moves.len());
        for x in &self.moves {
            m.arr(6)
                .uint(x.bucket as u64)
                .uint(x.from as u64)
                .uint(x.to as u64)
                .uint(x.bytes);
            m.str(&x.state).str(&x.error);
        }
    }

    fn decode(rd: &mut Rd) -> SqlResult<Rebalance> {
        let mut r = Rebalance::default();
        for _ in 0..rd.map()? {
            let k = rd.string()?;
            match k.as_str() {
                "owner" => r.owner = rd.string()?,
                "status" => r.status = rd.string()?,
                "by" => r.by = rd.string()?,
                "max_moves" => r.max_moves = num(rd)? as u32,
                "max_rate" => r.max_rate = num(rd)?,
                "started" => r.started = rd.value()?.as_f64().unwrap_or(0.0),
                "done" => r.done = num(rd)?,
                "failed" => r.failed = num(rd)?,
                "bytes" => r.bytes = num(rd)?,
                "message" => r.message = rd.string()?,
                "ver" => r.ver = num(rd)?,
                "moves" => {
                    let n = if rd.peek_map().is_some() {
                        // (an empty Lua table)
                        for _ in 0..rd.map()? {
                            rd.skip()?;
                            rd.skip()?;
                        }
                        0
                    } else {
                        rd.array()?
                    };
                    for _ in 0..n {
                        let f = rd.array()?;
                        r.moves.push(PlannedMove {
                            bucket: num(rd)? as u32,
                            from: num(rd)? as u32,
                            to: num(rd)? as u32,
                            bytes: num(rd)?,
                            state: rd.string()?,
                            error: rd.string()?,
                        });
                        for _ in 6..f {
                            rd.skip()?;
                        }
                    }
                }
                _ => rd.skip()?,
            }
        }
        Ok(r)
    }
}

/// The moves that bring the shards' weights toward their mean, at most
/// `limit`: each from the heaviest shard to the lightest, while the two
/// differ by more than `tolerance`, of a bucket that brings them closer:
/// the one whose weight is closest to half their difference -- or, with
/// `even_counts`, to the buckets' mean weight while that is less, so that
/// the shards also end with even shares of the buckets (taking the heaviest
/// first would leave the shards they go to with fewer, heavier buckets; by
/// load, though, the hot buckets are the ones to move). `shards`: each
/// shard's id and its buckets' weights. (bucket, from, to, weight), in
/// order.
pub fn plan(
    shards: &[(u32, Vec<(u32, u64)>)],
    limit: usize,
    tolerance: u64,
    even_counts: bool,
) -> Vec<(u32, u32, u32, u64)> {
    let mut out = Vec::new();
    if shards.len() < 2 {
        return out;
    }
    let mut total: Vec<u64> = shards.iter().map(|(_, bs)| bs.iter().map(|&(_, w)| w).sum()).collect();
    let n: usize = shards.iter().map(|(_, bs)| bs.len()).sum();
    let mean_w = total.iter().sum::<u64>() / n.max(1) as u64;
    let mut buckets: Vec<Vec<(u32, u64)>> = shards.iter().map(|(_, bs)| bs.clone()).collect();
    while out.len() < limit {
        let h = (0..shards.len())
            .max_by_key(|&i| (total[i], std::cmp::Reverse(i)))
            .unwrap();
        let l = (0..shards.len()).min_by_key(|&i| (total[i], i)).unwrap();
        let gap = total[h] - total[l];
        if gap <= tolerance {
            break;
        }
        // moving weight w turns the gap into |gap - 2w|: smaller iff w < gap
        let target = if even_counts { (gap / 2).min(mean_w) } else { gap / 2 } as i128;
        let best = buckets[h]
            .iter()
            .enumerate()
            .filter(|(_, &(_, w))| w > 0 && w < gap)
            .min_by_key(|(_, &(b, w))| ((w as i128 - target).unsigned_abs(), b))
            .map(|(k, &x)| (k, x));
        let Some((k, (b, w))) = best else { break };
        buckets[h].swap_remove(k);
        buckets[l].push((b, w));
        total[h] -= w;
        total[l] += w;
        out.push((b, shards[h].0, shards[l].0, w));
    }
    out
}

/// What a rebalance plans with, kept by the server that runs it: each
/// bucket's bytes and (by load) requests as the shards reported them. A
/// bucket's numbers move with it, and they are all read again together only
/// after a while, so that they are all of the same time: had the shards a
/// bucket moved to counted it afresh, they would look heavier than the
/// others. Its requests, which its new shard has counted only since it
/// arrived, keep what they were until that shard has counted them as long.
#[derive(Default)]
pub(crate) struct Weights {
    bytes: HashMap<u32, u64>,
    load: HashMap<u32, u64>,
    read_at: Option<Instant>,
    took: Duration,
    /// The buckets this rebalance moved: when, and their requests then.
    moved: HashMap<u32, (Instant, u64)>,
}

/// The weights are read again after this long, or ten times as long as
/// reading them took.
const REREAD: Duration = Duration::from_secs(30);

impl Weights {
    /// Bucket `b` moved.
    fn moved(&mut self, b: u32) {
        let load = self.load.get(&b).copied().unwrap_or(0);
        self.moved.insert(b, (Instant::now(), load));
    }
}

impl Cluster {
    async fn rebalance_get(&self) -> SqlResult<Option<Rebalance>> {
        let d = self.call(0, "tarn_rebalance_get", &[0x90]).await?;
        let n = crate::mpw::status_len(&d);
        let (_, mut rd) = lua_status(&d)?;
        if rd.is_nil() {
            return Ok(None);
        }
        let mut r = Rebalance::decode(&mut rd)?;
        if n > 2 {
            r.age = rd.value()?.as_f64().unwrap_or(0.0);
        }
        Ok(Some(r))
    }

    /// Write the record if it is still the one of `owner` at the version
    /// read (None: if no rebalance runs): its new version, or None if it is
    /// not. (A repeated call whose first one wrote it finds the version
    /// changed: its caller reads the record again.)
    async fn rebalance_put(&self, owner: Option<(&str, u64)>, r: &Rebalance) -> SqlResult<Option<u64>> {
        let mut m = Mp::new();
        m.arr(3);
        match owner {
            Some((o, ver)) => m.str(o).uint(ver),
            None => m.nil().nil(),
        };
        r.encode(&mut m);
        let d = self.call(0, "tarn_rebalance_put", &m.0).await?;
        if crate::mpw::status_is(&d, "busy") {
            return Ok(None);
        }
        let (_, mut rd) = lua_status(&d)?;
        Ok(Some(num(&mut rd)?))
    }

    /// `TARN REBALANCE`: start a rebalance, run by this server; what it
    /// planned first.
    pub async fn rebalance_start(self: &Arc<Self>, by: &str, max_moves: u32, max_rate: u64) -> SqlResult<String> {
        if let Some(cur) = self.rebalance_get().await? {
            if !cur.ended() {
                return Err(busy());
            }
        }
        let owner = self.job_owner();
        let mut r = Rebalance {
            owner: owner.clone(),
            status: "running".into(),
            by: by.into(),
            max_moves: max_moves.max(1),
            max_rate,
            started: now(),
            ..Rebalance::default()
        };
        let mut w = Weights::default();
        r.moves = self.rebalance_plan(by, &mut w).await?;
        let planned = r.moves.len();
        if planned == 0 {
            r.status = "done".into();
            r.message = "the shards are balanced".into();
        }
        if self.rebalance_put(None, &r).await?.is_none() {
            // (another one began meanwhile -- or this one, by a repeated
            // call)
            if !self.rebalance_get().await?.is_some_and(|cur| cur.owner == owner) {
                return Err(busy());
            }
        }
        if planned > 0 {
            let c = self.clone();
            tokio::spawn(async move { c.rebalance_run(owner, w).await });
        }
        Ok(if planned == 0 {
            "The shards are balanced: nothing to move".into()
        } else {
            format!("Rebalancing by {}: {} moves in the first round", by, planned)
        })
    }

    /// `TARN REBALANCE PAUSE | RESUME | CANCEL`.
    pub async fn rebalance_control(&self, action: &str) -> SqlResult<String> {
        let to = match action {
            "pause" => "paused",
            "resume" => "running",
            _ => "cancelled",
        };
        for _ in 0..20 {
            let Some(mut r) = self.rebalance_get().await? else {
                return Err(SqlError::new(
                    1210,
                    "HY000",
                    "no rebalance has run (TARN REBALANCE starts one)",
                ));
            };
            if r.ended() {
                return Err(SqlError::new(
                    1210,
                    "HY000",
                    format!("the rebalance has ended ({})", r.status),
                ));
            }
            if r.status == to {
                return Ok(format!("The rebalance is {} already", to));
            }
            r.status = to.into();
            if to == "cancelled" {
                r.message = "cancelled".into();
                for x in r.moves.iter_mut().filter(|x| x.state == "planned") {
                    x.state = "cancelled".into();
                }
            }
            let owner = r.owner.clone();
            if self.rebalance_put(Some((&owner, r.ver)), &r).await?.is_some() {
                return Ok(format!("The rebalance is {}", to));
            }
            // (its owner wrote it meanwhile, or another server took it over)
        }
        Err(SqlError::conflict("the rebalance record keeps changing; try again"))
    }

    /// The record as `SHOW TARN REBALANCE` and `SHOW TARN MOVES` show it:
    /// the moves in progress with what their bucket is doing ('copying',
    /// 'fenced').
    pub async fn rebalance_status(&self) -> SqlResult<Option<Rebalance>> {
        let Some(mut r) = self.rebalance_get().await? else {
            return Ok(None);
        };
        if r.moves.iter().any(|x| x.state == "moving") {
            let reports = self.bucket_reports().await.unwrap_or_default();
            let state_on = |id: u32, b: u32| {
                self.shard_position(id)
                    .and_then(|p| reports.get(p))
                    .and_then(|rep| rep.others.iter().find(|o| o.0 == b).map(|o| o.1.clone()))
            };
            for x in r.moves.iter_mut().filter(|x| x.state == "moving") {
                x.state = match (
                    state_on(x.from, x.bucket).as_deref(),
                    state_on(x.to, x.bucket).as_deref(),
                ) {
                    (Some("fenced"), _) | (_, Some("held")) => "fenced",
                    (Some("sending"), _) | (_, Some("receiving")) => "copying",
                    _ => "moving",
                }
                .into();
            }
        }
        Ok(Some(r))
    }

    /// Read the buckets' weights again if they are old (see `Weights`).
    async fn rebalance_weights(&self, by: &str, active: &[usize], w: &mut Weights) -> SqlResult<()> {
        if w.read_at.is_some_and(|t| t.elapsed() < REREAD.max(w.took * 10)) {
            return Ok(());
        }
        let t0 = Instant::now();
        let sizes = self
            .read_fanout(active.iter().map(|&p| self.call(p, "tarn_bucket_sizes", &[0x90])))
            .await;
        let mut bytes = HashMap::new();
        for d in sizes {
            let d = d?;
            let (_, mut rd) = lua_status(&d)?;
            for _ in 0..rd.array()? {
                rd.array()?;
                let b = num(&mut rd)? as u32;
                let _rows = num(&mut rd)?;
                bytes.insert(b, num(&mut rd)?);
            }
        }
        let mut load = HashMap::new();
        let mut load_secs = f64::MAX;
        if by == "load" {
            let stats = self
                .read_fanout(active.iter().map(|&p| self.call(p, "tarn_bucket_stats", &[0x90])))
                .await;
            for d in stats {
                let d = d?;
                let (_, mut rd) = lua_status(&d)?;
                load_secs = load_secs.min(rd.value()?.as_f64().unwrap_or(0.0));
                for _ in 0..rd.array()? {
                    rd.array()?;
                    let b = num(&mut rd)? as u32;
                    let reads = num(&mut rd)?;
                    let writes = num(&mut rd)?;
                    let _bytes = num(&mut rd)?;
                    load.insert(b, reads + writes);
                }
            }
            // (a bucket's new shard has counted its requests only since it
            // arrived)
            for (b, (at, before)) in &w.moved {
                if at.elapsed().as_secs_f64() < load_secs {
                    let x = load.entry(*b).or_insert(0);
                    *x = (*x).max(*before);
                }
            }
        }
        w.bytes = bytes;
        w.load = load;
        w.read_at = Some(Instant::now());
        w.took = t0.elapsed();
        Ok(())
    }

    /// A round of moves by `by` ('size' or 'load') over the active shards.
    async fn rebalance_plan(&self, by: &str, w: &mut Weights) -> SqlResult<Vec<PlannedMove>> {
        // (moves go to the active shards: a shard may have joined, or begun
        // to leave)
        self.load_members().await?;
        self.refresh_buckets().await?;
        let map = self.bucket_map();
        let active: Vec<usize> = self.live_shards().filter(|&p| self.shard_active(p)).collect();
        if active.len() < 2 {
            return Ok(Vec::new());
        }
        self.rebalance_weights(by, &active, w).await?;
        let raw = if by == "load" { &w.load } else { &w.bytes };
        // The buckets of the active shards (a leaving shard's are the
        // drain's).
        let at: HashMap<usize, usize> = active.iter().enumerate().map(|(i, &p)| (p, i)).collect();
        let mut shards: Vec<(u32, Vec<(u32, u64)>)> = active.iter().map(|&p| (self.shards[p].id, Vec::new())).collect();
        let mut sum = 0u64;
        let mut n = 0u64;
        for b in 1..=map.count {
            if let Some(&i) = at.get(&map.shard(b)) {
                let x = raw.get(&b).copied().unwrap_or(0);
                shards[i].1.push((b, x));
                sum += x;
                n += 1;
            }
        }
        // A bucket weighs its bytes (or requests) plus the mean of all
        // buckets': the shards also get even shares of the buckets, where
        // the rows written later land. (With no requests at all, by load,
        // the buckets weigh the same.)
        let mean = sum.div_ceil(n.max(1)).max(1);
        for (_, bs) in shards.iter_mut() {
            for (_, x) in bs.iter_mut() {
                *x += mean;
            }
        }
        // even within 2% of a shard's share, or a bucket of mean weight
        let total = sum + mean * n;
        let tolerance = (total / active.len() as u64 / 50).max(mean);
        Ok(plan(&shards, ROUND, tolerance, by != "load")
            .into_iter()
            .map(|(bucket, from, to, _)| PlannedMove {
                bucket,
                from,
                to,
                bytes: w.bytes.get(&bucket).copied().unwrap_or(0),
                state: "planned".into(),
                error: String::new(),
            })
            .collect())
    }

    /// Take over the rebalance of a server that stopped: one whose record
    /// was not written for `ddl_job_stale` (from the loop that finishes
    /// abandoned jobs).
    pub(crate) async fn rebalance_takeover(self: &Arc<Self>) -> SqlResult<()> {
        let Some(mut r) = self.rebalance_get().await? else {
            return Ok(());
        };
        if r.ended() || r.age <= self.cfg.ddl_job_stale.max(1.0) {
            return Ok(());
        }
        let old = r.owner.clone();
        let me = self.job_owner();
        r.owner = me.clone();
        r.message = format!("taken over from {}", old);
        if self.rebalance_put(Some((&old, r.ver)), &r).await?.is_some() {
            tracing::warn!("taking over the rebalance that {} left", old);
            let c = self.clone();
            tokio::spawn(async move { c.rebalance_run(me, Weights::default()).await });
        }
        Ok(())
    }

    /// The rebalance of `owner`, until it ends or another server owns it.
    pub(crate) async fn rebalance_run(self: Arc<Self>, owner: String, mut w: Weights) {
        if let Err(e) = self.rebalance_loop(&owner, &mut w).await {
            tracing::warn!("rebalance: {}", e.message);
            for _ in 0..10 {
                match self.rebalance_get().await {
                    Ok(Some(mut r)) if r.owner == owner && !r.ended() => {
                        r.status = "failed".into();
                        r.message = e.message.clone();
                        if let Ok(Some(_)) = self.rebalance_put(Some((&owner, r.ver)), &r).await {
                            return;
                        }
                    }
                    Ok(_) => return,
                    Err(_) => {}
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }

    async fn rebalance_loop(&self, owner: &str, w: &mut Weights) -> SqlResult<()> {
        let start = Instant::now();
        let mut started_bytes: u64 = 0;
        let touch = Duration::from_secs_f64((self.cfg.ddl_job_stale / 3.0).clamp(0.05, 5.0));
        let mut last_put = Instant::now();
        let mut in_flight = FuturesUnordered::new();
        // the buckets this server moves now
        let mut moving: HashSet<u32> = HashSet::new();
        // the moves that ended and are not in the record yet
        let mut ended: Vec<(u32, MoveEnd)> = Vec::new();
        let mut failures_in_row = 0;
        let mut plan_errors = 0;
        // (after a move had to wait for a job in its way: none starts before)
        let mut hold_until = Instant::now();
        let mut owned = true;
        loop {
            let mut r = match self.rebalance_get().await {
                Ok(Some(r)) => r,
                Ok(None) => return Ok(()),
                // (the first shard's leader may be changing)
                Err(e) if last_put.elapsed() < Duration::from_secs(30) => {
                    tracing::debug!("rebalance: reading its record: {}", e.message);
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                }
                Err(e) => return Err(e),
            };
            if r.owner != owner {
                owned = false; // taken over: let the moves in flight end
            }
            let mut changed = false;
            // (again, if the record was not written since)
            for (b, end) in &ended {
                let Some(x) = r.moves.iter_mut().find(|x| x.bucket == *b && x.state == "moving") else {
                    continue;
                };
                match end {
                    MoveEnd::Done => {
                        x.state = "done".into();
                        x.error.clear();
                        r.done += 1;
                        r.bytes += x.bytes;
                    }
                    MoveEnd::Failed(e) => {
                        x.state = "failed".into();
                        x.error = e.clone();
                        r.failed += 1;
                    }
                    MoveEnd::Retry(e) => {
                        x.state = "planned".into();
                        r.message = format!("moves wait: {}", e);
                    }
                }
                changed = true;
            }
            if owned && r.status == "running" && failures_in_row >= MAX_FAILURES_IN_ROW {
                r.status = "failed".into();
                r.message = format!("{} moves in a row failed", MAX_FAILURES_IN_ROW);
                changed = true;
            }
            let pending = r.moves.iter().any(|x| x.state == "planned" || x.state == "moving");
            if owned && r.status == "running" && !pending && in_flight.is_empty() {
                match self.rebalance_plan(&r.by, w).await {
                    Ok(moves) => {
                        plan_errors = 0;
                        if moves.is_empty() {
                            // (the last round's moves stay, to be seen)
                            r.status = "done".into();
                            r.message = "the shards are balanced".into();
                        } else {
                            r.moves = moves;
                        }
                    }
                    // (a shard's leader may be changing)
                    Err(e) if plan_errors < 10 => {
                        plan_errors += 1;
                        r.message = format!("planning the next round: {}", e.message);
                        hold_until = Instant::now() + Duration::from_secs(1);
                    }
                    Err(e) => return Err(e),
                }
                changed = true;
            }
            let mut start_now = Vec::new();
            if owned && r.status == "running" && Instant::now() >= hold_until {
                let mut bytes = started_bytes;
                for x in r.moves.iter_mut() {
                    if in_flight.len() + start_now.len() >= r.max_moves as usize {
                        break;
                    }
                    // ('moving' and not this server's: a move of the owner
                    // it took over from, or one it did not get to start;
                    // moving it again does nothing once the bucket is there,
                    // and waits while the old move's job is finished)
                    if !(x.state == "planned" || (x.state == "moving" && !moving.contains(&x.bucket))) {
                        continue;
                    }
                    if r.max_rate > 0 && bytes as f64 > r.max_rate as f64 * start.elapsed().as_secs_f64() {
                        break;
                    }
                    let Some(to) = self.shard_position(x.to).filter(|&p| self.shard_active(p)) else {
                        x.state = "cancelled".into();
                        x.error = format!("shard {} is not an active shard of the cluster", x.to);
                        changed = true;
                        continue;
                    };
                    x.state = "moving".into();
                    bytes += x.bytes;
                    start_now.push((x.bucket, to, x.bytes));
                    changed = true;
                }
            }
            if owned && (changed || last_put.elapsed() >= touch) {
                match self.rebalance_put(Some((owner, r.ver)), &r).await {
                    Ok(Some(_)) => {
                        last_put = Instant::now();
                        ended.clear();
                        // (the moves start once the record says so)
                        for (b, to, bytes) in start_now {
                            started_bytes += bytes;
                            moving.insert(b);
                            in_flight.push(async move { (b, self.rebalance_move(b, to).await) });
                        }
                    }
                    // A pause, a resume or a cancel came in between, or
                    // another server took the rebalance over: read it again.
                    Ok(None) => continue,
                    Err(e) => tracing::debug!("rebalance: writing its record: {}", e.message),
                }
            }
            if in_flight.is_empty() && (!owned || r.ended()) {
                return Ok(());
            }
            // until a move ends, or a while (a pause, a cancel, the rate)
            let wait = touch.min(Duration::from_millis(500));
            let mut got = Vec::new();
            if in_flight.is_empty() {
                tokio::time::sleep(wait).await;
            } else if let Ok(Some(x)) = tokio::time::timeout(wait, in_flight.next()).await {
                got.push(x);
            }
            while let Some(Some(x)) = in_flight.next().now_or_never() {
                got.push(x);
            }
            for (b, end) in got {
                moving.remove(&b);
                match &end {
                    MoveEnd::Done => {
                        failures_in_row = 0;
                        w.moved(b);
                    }
                    MoveEnd::Failed(e) => {
                        tracing::warn!("rebalance: moving bucket {}: {}", b, e);
                        failures_in_row += 1;
                        // (a shard may have begun to leave)
                        let _ = self.load_members().await;
                    }
                    MoveEnd::Retry(_) => hold_until = Instant::now() + Duration::from_secs(1),
                }
                ended.push((b, end));
            }
        }
    }

    /// Move bucket `b` to the shard at position `to`, again after a
    /// conflict (1213: the bucket's map changed meanwhile).
    async fn rebalance_move(&self, b: u32, to: usize) -> MoveEnd {
        let mut tries = 0;
        loop {
            match self.move_bucket(b, to, self.cfg.move_timeout).await {
                Ok(_) => return MoveEnd::Done,
                Err(e) if e.code == 1213 && tries < 5 => {
                    tries += 1;
                    tokio::time::sleep(Duration::from_millis(200 * tries)).await;
                }
                Err(e) if e.code == 1205 => return MoveEnd::Retry(e.message),
                Err(e) => return MoveEnd::Failed(e.message),
            }
        }
    }
}

fn busy() -> SqlError {
    SqlError::new(
        1205,
        "HY000",
        "a rebalance is in progress (SHOW TARN REBALANCE; TARN REBALANCE CANCEL ends it)",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_evens_the_shards_out() {
        // shard 1: buckets 1..=8 of weight 10; shard 2: none
        let shards = vec![(1, (1..=8).map(|b| (b, 10)).collect()), (2, Vec::new())];
        let p = plan(&shards, 64, 0, false);
        assert_eq!(p.len(), 4);
        assert!(p.iter().all(|&(_, from, to, w)| from == 1 && to == 2 && w == 10));
        // a heavy bucket alone does not move back and forth
        let shards = vec![(1, vec![(1, 100)]), (2, vec![(2, 1), (3, 1)])];
        let p = plan(&shards, 64, 0, false);
        assert!(p.iter().all(|&(b, ..)| b != 1), "{:?}", p);
        // within the tolerance: nothing
        let shards = vec![(1, vec![(1, 10), (2, 10)]), (2, vec![(3, 10)])];
        assert!(plan(&shards, 64, 10, true).is_empty());
        // three shards, sizes uneven: the totals end within the largest
        // bucket's weight of each other
        let shards = vec![
            (1, (1..=30).map(|b| (b, b as u64)).collect()),
            (2, vec![]),
            (3, vec![(31, 5)]),
        ];
        let p = plan(&shards, 1000, 0, false);
        let mut tot = HashMap::new();
        for (id, bs) in &shards {
            tot.insert(*id, bs.iter().map(|&(_, w)| w).sum::<u64>());
        }
        for &(_, from, to, w) in &p {
            *tot.get_mut(&from).unwrap() -= w;
            *tot.get_mut(&to).unwrap() += w;
        }
        let (mx, mn) = (tot.values().max().unwrap(), tot.values().min().unwrap());
        assert!(mx - mn <= 30, "{:?}", tot);
        // the limit
        assert_eq!(plan(&shards, 3, 0, true).len(), 3);
    }

    #[test]
    fn plan_evens_the_buckets_out_too() {
        // 480 buckets of uneven sizes (2..=14) on two shards, plus the
        // mean, as rebalance_plan weighs them; two empty shards join
        let size = |b: u32| 2 + (b as u64 * 7919) % 13;
        let mean = (1..=480).map(size).sum::<u64>() / 480;
        let w = |b: u32| size(b) + mean;
        let shards = vec![
            (1, (1..=240).map(|b| (b, w(b))).collect()),
            (2, (241..=480).map(|b| (b, w(b))).collect()),
            (3, vec![]),
            (4, vec![]),
        ];
        let total: u64 = (1..=480).map(w).sum();
        let tolerance = (total / 4 / 50).max(mean);
        let mut moves = Vec::new();
        let mut cur = shards.clone();
        // (round after round, as the rebalancer plans)
        loop {
            let p = plan(&cur, 64, tolerance, true);
            if p.is_empty() {
                break;
            }
            for &(b, from, to, x) in &p {
                let i = cur.iter().position(|s| s.0 == from).unwrap();
                let k = cur[i].1.iter().position(|&(c, _)| c == b).unwrap();
                cur[i].1.swap_remove(k);
                let j = cur.iter().position(|s| s.0 == to).unwrap();
                cur[j].1.push((b, x));
            }
            moves.extend(p);
            assert!(moves.len() < 480, "it keeps moving");
        }
        let tot: Vec<u64> = cur.iter().map(|(_, bs)| bs.iter().map(|&(_, x)| x).sum()).collect();
        let counts: Vec<usize> = cur.iter().map(|(_, bs)| bs.len()).collect();
        assert!(
            tot.iter().max().unwrap() - tot.iter().min().unwrap() <= tolerance,
            "{:?}",
            tot
        );
        assert!(counts.iter().all(|&n| (114..=126).contains(&n)), "{:?}", counts);
        // about the fewest moves: half the buckets
        assert!(moves.len() <= 250, "{}", moves.len());
    }

    #[test]
    fn plan_by_load_moves_the_hot_buckets() {
        // shard 1's buckets 1..=10 are hot; every bucket also weighs the
        // mean, as rebalance_plan weighs them
        let load = |b: u32| if b <= 10 { 100 } else { 0 };
        let mean = 1000 / 300 + 1;
        let shards: Vec<(u32, Vec<(u32, u64)>)> = (0..3)
            .map(|i| {
                (
                    i + 1,
                    (i * 100 + 1..=i * 100 + 100).map(|b| (b, load(b) + mean)).collect(),
                )
            })
            .collect();
        let total: u64 = shards.iter().flat_map(|(_, bs)| bs.iter().map(|&(_, w)| w)).sum();
        let p = plan(&shards, 64, (total / 3 / 50).max(mean), false);
        // the hot ones first, then a few others to even the rest out
        let hot = p.iter().take_while(|&&(b, ..)| b <= 10).count();
        assert!(hot >= 6 && p.len() <= hot + 10, "{:?}", p);
        assert!(p.iter().all(|&(_, from, ..)| from == 1), "{:?}", p);
        // by size, buckets of mean weight would go first
        let p = plan(&shards, 64, (total / 3 / 50).max(mean), true);
        assert!(p[0].0 > 10, "{:?}", p);
    }

    #[test]
    fn record_round_trip() {
        let r = Rebalance {
            owner: "1-ab-3".into(),
            status: "running".into(),
            by: "size".into(),
            max_moves: 4,
            max_rate: 1000,
            started: 12.5,
            done: 3,
            failed: 1,
            bytes: 300,
            message: String::new(),
            moves: vec![PlannedMove {
                bucket: 7,
                from: 1,
                to: 3,
                bytes: 100,
                state: "planned".into(),
                error: String::new(),
            }],
            age: 0.0,
            ver: 0,
        };
        let mut m = Mp::new();
        r.encode(&mut m);
        assert_eq!(Rebalance::decode(&mut Rd::new(&m.0)).unwrap(), r);
    }
}
