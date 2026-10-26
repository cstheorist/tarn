//! The bucket map and bucket moves (issue #24; lua/tarn.lua "Buckets" and
//! "Bucket moves", docs/design.md §2.12).
//!
//! A row's bucket is `crc32c(shard key) % count + 1` (route.rs) and never
//! changes; which shard owns a bucket does. The shards keep the map: each
//! knows the buckets it owns, and those it is moving. A SQL server loads it
//! from all of them (`BucketMap::from_reports`, whose core is the verified
//! `tarn_verified::route::build_owner`) and routes by it. Its requests name
//! the buckets they were routed by; a shard refuses ('moved') a bucket it
//! does not own, the server loads the map again and the statement runs
//! again (`StaleRetry` in session.rs).
//!
//! A move of bucket b from shard S to shard R is a job (`JobKind::Move`)
//! that one server drives (`Cluster::move_bucket`): R receives, S sends and
//! notes the writes to b, the rows are copied while S serves them, R holds
//! b, S fences b and drains it, the writes since the copy began are copied,
//! S sends b (deletes its rows) and R takes it over (inserts them). Before
//! the send the move can be given up (both sides go back); from the send on
//! it only goes forward. A server that finds the job abandoned finishes it
//! (`Cluster::finish_move`).

use crate::cluster::{Cluster, Job, JobKind};
use crate::mpw::{lua_status, Mp, Rd};
use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};
use tarn_common::{SqlError, SqlResult};
use tarn_verified::route::{build_owner, shard_of_bucket, NO_SHARD};

/// The number of buckets of a new cluster, unless `bucket_count` in the
/// configuration says otherwise. Buckets are the unit a move moves: a
/// shard owns whole buckets, so with n buckets per shard the shards' sizes
/// can differ by one part in n; but each bucket is moved separately and has
/// a record on its shard (docs/operations.md "Buckets").
pub const NEW_CLUSTER_BUCKETS: u32 = 30_000;

/// The number of buckets of a cluster made by a version from before the
/// bucket map, unless its configuration set `bucket_count`: its rows'
/// bucket ids were computed with it.
pub const LEGACY_BUCKETS: u32 = 3_000;

/// The most buckets a cluster may have.
pub const MAX_BUCKETS: u32 = 1_000_000;

/// One shard's map (`tarn_buckets`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Report {
    /// Buckets in the cluster; 0: the shard has no map yet.
    pub count: u32,
    /// Without a map: the Tarn tables on the shard.
    pub tables: u32,
    /// Bumped by every send and receive on the shard.
    pub epoch: u64,
    /// The buckets it owns, as ranges (lo, hi).
    pub owned: Vec<(u32, u32)>,
    /// Buckets in a move: (bucket, state, peer shard id, move id); the
    /// states are lua/tarn.lua's ('sending', 'fenced', 'sent', 'receiving',
    /// 'held').
    pub others: Vec<(u32, String, Option<u32>, Option<String>)>,
    /// The shard's membership version (lua/tarn.lua "Membership").
    pub mver: u64,
}

/// Decode a `tarn_buckets` reply: `{'ok', count, epoch, owned, others}`.
pub fn parse_report(d: &[u8]) -> SqlResult<Report> {
    let (_, mut rd) = lua_status(d)?;
    let mut r = Report::default();
    let n_fields = {
        // the status table's length was read by lua_status; what is left
        // starts at count
        let mut probe = Rd::new(d);
        probe.array()?;
        probe.array()?
    };
    if n_fields < 2 {
        return Ok(r);
    }
    r.count = rd.value()?.as_i64().unwrap_or(0) as u32;
    if r.count == 0 {
        if n_fields >= 3 {
            r.tables = rd.value()?.as_i64().unwrap_or(0) as u32;
        }
        return Ok(r);
    }
    if n_fields < 5 {
        return Ok(r);
    }
    r.epoch = rd.value()?.as_i64().unwrap_or(0) as u64;
    for _ in 0..rd.array()? {
        rd.array()?;
        let lo = rd.value()?.as_i64().unwrap_or(0) as u32;
        let hi = rd.value()?.as_i64().unwrap_or(0) as u32;
        r.owned.push((lo, hi));
    }
    for _ in 0..rd.array()? {
        let m = rd.array()?;
        let b = rd.value()?.as_i64().unwrap_or(0) as u32;
        let state = rd.string()?;
        let peer = if m > 2 {
            rd.value()?.as_i64().map(|p| p as u32)
        } else {
            None
        };
        let mv = if m > 3 && !rd.is_nil() {
            Some(rd.string()?)
        } else {
            if m > 3 {
                rd.skip()?;
            }
            None
        };
        for _ in 4..m {
            rd.skip()?;
        }
        r.others.push((b, state, peer, mv));
    }
    if n_fields >= 6 {
        r.mver = rd.value()?.as_i64().unwrap_or(0).max(0) as u64;
    }
    Ok(r)
}

/// The number of buckets of a map made now, from the shards' reports: that
/// of a shard that has a map; else `bucket_count` from the configuration;
/// else, when the shards have tables (made by a version from before the
/// map, which computed their rows' bucket ids with its default),
/// `LEGACY_BUCKETS`; else (a new cluster) `NEW_CLUSTER_BUCKETS`.
pub fn new_map_count(configured: Option<u32>, reports: &[Report]) -> u32 {
    if let Some(c) = reports.iter().map(|r| r.count).find(|&c| c > 0) {
        return c;
    }
    if let Some(c) = configured {
        return c;
    }
    if reports.iter().any(|r| r.tables > 0) {
        LEGACY_BUCKETS
    } else {
        NEW_CLUSTER_BUCKETS
    }
}

/// The shard (position) of each bucket of a new cluster: contiguous ranges,
/// one per shard (`shard_of_bucket`, verified monotone and onto).
pub fn initial_ranges(count: u32, nshards: usize) -> Vec<(u32, u32)> {
    let mut out: Vec<(u32, u32)> = vec![(0, 0); nshards];
    for b in 1..=count {
        let s = shard_of_bucket(b, count, nshards as u32) as usize;
        if out[s].0 == 0 {
            out[s].0 = b;
        }
        out[s].1 = b;
    }
    out
}

/// Which shard owns each bucket.
#[derive(Clone, Debug)]
pub struct BucketMap {
    pub count: u32,
    /// owner[b - 1]: the position in `Cluster::shards` of the shard that
    /// owns bucket b -- or, for a bucket between a move's send and its
    /// receive, of the shard it is going to (which refuses it until then).
    owner: Vec<u32>,
    /// Each shard's epoch when this map was read (0: unknown).
    pub epochs: Vec<u64>,
    /// The buckets no shard owns now (in a move, between send and receive).
    pub moving: Vec<u32>,
    /// What the shards reported (`SHOW TARN BUCKETS`).
    pub reports: Vec<Report>,
    /// The membership version the map was read with: reads of every shard
    /// routed by it name it.
    pub mver: u64,
}

impl BucketMap {
    /// The layout of a new cluster, before the shards' map is read.
    pub fn initial(count: u32, nshards: usize) -> BucketMap {
        let count = count.max(1);
        let owner = (1..=count).map(|b| shard_of_bucket(b, count, nshards as u32)).collect();
        BucketMap {
            count,
            owner,
            epochs: vec![0; nshards],
            moving: Vec::new(),
            reports: Vec::new(),
            mver: 0,
        }
    }

    /// The map the shards' reports give (`reports[i]` from the shard at
    /// position i; `pos` maps shard ids to positions). Fails when a shard
    /// has no map, the counts differ, or two shards claim a bucket.
    pub fn from_reports(reports: Vec<Report>, pos: &HashMap<u32, usize>) -> SqlResult<BucketMap> {
        let nshards = reports.len();
        let count = reports.first().map(|r| r.count).unwrap_or(0);
        for (i, r) in reports.iter().enumerate() {
            if r.count == 0 {
                return Err(SqlError::internal(format!("shard at position {} has no bucket map", i)));
            }
            if r.count != count {
                return Err(SqlError::internal(format!(
                    "the shards' bucket maps disagree on the number of buckets ({} and {})",
                    count, r.count
                )));
            }
        }
        if count == 0 || count == u32::MAX {
            return Err(SqlError::internal("no bucket map"));
        }
        let ranges: Vec<(u32, u32, u32)> = reports
            .iter()
            .enumerate()
            .flat_map(|(i, r)| r.owned.iter().map(move |&(lo, hi)| (i as u32, lo, hi)))
            .collect();
        let Some(mut owner) = build_owner(count, &ranges) else {
            return Err(SqlError::internal(format!(
                "the shards' bucket maps overlap or are out of range: {}",
                describe_overlap(count, &ranges)
            )));
        };
        // A bucket no shard owns is between a move's send and its receive:
        // requests for it go to the shard it is going to.
        // (The shard receiving it says so; 'sent' records stay after moves
        // and may be old ones.)
        let mut moving = Vec::new();
        let mut going: BTreeMap<u32, usize> = BTreeMap::new();
        for (i, r) in reports.iter().enumerate() {
            for (b, state, ..) in &r.others {
                if state == "held" || state == "receiving" {
                    going.insert(*b, i);
                }
            }
        }
        for r in &reports {
            for (b, state, peer, _) in &r.others {
                if state == "sent" {
                    if let Some(&p) = peer.and_then(|p| pos.get(&p)) {
                        going.entry(*b).or_insert(p);
                    }
                }
            }
        }
        for b in 1..=count {
            let o = &mut owner[(b - 1) as usize];
            if *o == NO_SHARD {
                moving.push(b);
                *o = match going.get(&b) {
                    Some(&p) => p as u32,
                    None => shard_of_bucket(b, count, nshards as u32),
                };
            }
        }
        Ok(BucketMap {
            count,
            owner,
            epochs: reports.iter().map(|r| r.epoch).collect(),
            moving,
            reports,
            mver: 0,
        })
    }

    /// The shard (position) that owns bucket b.
    pub fn shard(&self, b: u32) -> usize {
        self.owner[(b.clamp(1, self.count) - 1) as usize] as usize
    }

    /// Every bucket has an owner: no move is between its send and receive.
    pub fn settled(&self) -> bool {
        self.moving.is_empty()
    }

    /// The epoch of shard `shard` in this map, for a read of every shard
    /// outside a transaction (the shard refuses it if its buckets changed
    /// since). Such a read must not run on a map with a bucket between a
    /// move's send and its receive: it would find the bucket's rows on no
    /// shard.
    pub fn epoch_of(&self, shard: usize) -> SqlResult<u64> {
        if !self.settled() {
            return Err(SqlError::moved(format!(
                "bucket {} is being moved; try again",
                self.moving[0]
            )));
        }
        // (A shard that joined after the map was read: read it again.)
        self.epochs.get(shard).copied().ok_or_else(|| {
            SqlError::moved(format!(
                "shard position {} is not in this server's bucket map yet; try again",
                shard
            ))
        })
    }
}

/// The first bucket two ranges both claim (or the bad range), for an error.
fn describe_overlap(count: u32, ranges: &[(u32, u32, u32)]) -> String {
    for (i, &(s, lo, hi)) in ranges.iter().enumerate() {
        if lo < 1 || lo > hi || hi > count {
            return format!("shard position {} reports {}..{} of {}", s, lo, hi, count);
        }
        for &(t, lo2, hi2) in &ranges[i + 1..] {
            if lo <= hi2 && lo2 <= hi {
                return format!(
                    "bucket {} is reported by the shards at positions {} and {}",
                    lo.max(lo2),
                    s,
                    t
                );
            }
        }
    }
    "?".into()
}

/// Bucket ranges as text: "1-1500, 1502".
pub fn ranges_text(r: &[(u32, u32)]) -> String {
    r.iter()
        .map(|&(lo, hi)| {
            if lo == hi {
                lo.to_string()
            } else {
                format!("{}-{}", lo, hi)
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// How long a refresh waits for buckets in a move to land before it takes
/// the map as it is (reads of every shard outside a transaction refuse to
/// run on it: see `Cluster::epoch_of`).
const SETTLE_WAIT: Duration = Duration::from_secs(2);

/// The most times a move copies a bucket again after its notes of the
/// writes since the copy were lost (leader changes on the sending shard).
const MAX_RECOPIES: usize = 3;

/// How long a step of a move is tried again after a leader change or a lost
/// connection (every step can be repeated).
const STEP_RETRY: Duration = Duration::from_secs(30);

/// An error a step of a move may get past by trying again: the shard's
/// leader changed, or the connection to it broke.
fn transient(e: &SqlError) -> bool {
    matches!(e.code, 2003 | 2013) || (e.code == 1213 && !e.moved && e.message.contains("leader"))
}

/// The pieces of a move.
#[derive(Clone, Debug)]
pub(crate) struct Move {
    pub bucket: u32,
    /// positions in `Cluster::shards`
    pub from: usize,
    pub to: usize,
    /// the move's id (the shards check it on every step)
    pub id: String,
}

impl Move {
    /// The job's arguments: {bucket, from, to, move} (shard ids).
    fn args(&self, c: &Cluster) -> Vec<u8> {
        let mut m = Mp::new();
        m.map(4)
            .str("bucket")
            .uint(self.bucket as u64)
            .str("from")
            .uint(c.shards[self.from].id as u64)
            .str("to")
            .uint(c.shards[self.to].id as u64)
            .str("move")
            .str(&self.id);
        m.0
    }

    fn of_args(c: &Cluster, args: &[u8]) -> SqlResult<Move> {
        let mut rd = Rd::new(args);
        let n = tarn_iproto::mp::read_map_len(&mut rd.buf).map_err(crate::mpw::proto)?;
        let (mut bucket, mut from, mut to, mut id) = (0u32, None, None, String::new());
        for _ in 0..n {
            match rd.string()?.as_str() {
                "bucket" => bucket = rd.value()?.as_i64().unwrap_or(0) as u32,
                "from" => from = rd.value()?.as_i64().and_then(|s| c.shard_position(s as u32)),
                "to" => to = rd.value()?.as_i64().and_then(|s| c.shard_position(s as u32)),
                "move" => id = rd.string()?,
                _ => rd.skip()?,
            }
        }
        match (from, to) {
            (Some(from), Some(to)) if bucket > 0 && !id.is_empty() => Ok(Move { bucket, from, to, id }),
            _ => Err(SqlError::internal("a bucket move job without its bucket, shards or id")),
        }
    }
}

impl Cluster {
    /// Every shard's map, in shard order.
    /// Every shard's part of the map, by position: a removed shard's is
    /// empty (it owns no bucket and is not asked).
    pub(crate) async fn bucket_reports(&self) -> SqlResult<Vec<Report>> {
        let replies = self.call_all("tarn_buckets", &[0x90]).await?;
        let mut out: Vec<Option<Report>> = Vec::with_capacity(replies.len());
        for d in &replies {
            out.push(match d {
                Some(d) => Some(parse_report(d)?),
                None => None,
            });
        }
        let count = out.iter().flatten().map(|r| r.count).find(|&c| c > 0).unwrap_or(0);
        Ok(out
            .into_iter()
            .map(|r| {
                r.unwrap_or_else(|| Report {
                    count,
                    ..Report::default()
                })
            })
            .collect())
    }

    /// Read the map from the shards and route by it; make it first on a
    /// cluster whose shards have none (a new cluster, or one from before
    /// bucket moves): each shard without a map gets the buckets of the
    /// initial layout that no other shard owns or moves. A shard added to a
    /// cluster gets none: buckets move to it.
    pub(crate) async fn load_buckets(&self) -> SqlResult<()> {
        let mut reports = match self.bucket_reports().await {
            Ok(r) => r,
            // Storage nodes from before bucket moves: route by the layout
            // of a new cluster, as servers did then.
            Err(e) if e.message.contains("'tarn_buckets' is not defined") => {
                tracing::warn!("the shards keep no bucket map (an older version): buckets cannot move");
                let count = self.cfg.bucket_count.unwrap_or(LEGACY_BUCKETS);
                self.install_buckets(BucketMap::initial(count, self.shards.len()));
                return Ok(());
            }
            Err(e) => return Err(e),
        };
        if reports.iter().any(|r| r.count == 0) {
            // (again when another server made some shards' maps meanwhile
            // with another count: then with that one)
            let mut tries = 0;
            while !self.init_buckets(&reports).await? {
                tries += 1;
                if tries == 3 {
                    return Err(SqlError::internal(
                        "the shards' bucket maps are being made with different counts",
                    ));
                }
                reports = self.bucket_reports().await?;
            }
            reports = self.bucket_reports().await?;
        }
        // A shard that knows a newer membership: read it first (shards
        // may have joined), and the map from every shard then.
        if reports.iter().any(|r| r.mver > self.member_ver()) {
            self.load_members().await?;
            reports = self.bucket_reports().await?;
        }
        // (Again while a move is between its send and its receive, and when
        // the shards' parts disagree: read at different moments, they may
        // straddle a send and the receive after it.)
        let deadline = Instant::now() + SETTLE_WAIT;
        loop {
            let pos = self.shard_pos.read().clone();
            let mver = self.member_ver();
            match BucketMap::from_reports(reports, &pos) {
                Ok(mut map) if map.settled() || Instant::now() >= deadline => {
                    map.mver = mver;
                    // (a bucket in flight is sent to the shard it is going
                    // to, or a guess: never a shard that left)
                    if let Some(first) = self.live_shards().next() {
                        for &b in &map.moving {
                            let o = &mut map.owner[(b - 1) as usize];
                            if self.shards[*o as usize].removed() {
                                *o = first as u32;
                            }
                        }
                    }
                    self.install_buckets(map);
                    return Ok(());
                }
                Err(e) if Instant::now() >= deadline => return Err(e),
                _ => {}
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
            reports = self.bucket_reports().await?;
        }
    }

    /// Make the map of each shard without one. False: a shard had one
    /// already with another count (another server made it at the same
    /// moment): the caller reads the reports again.
    async fn init_buckets(&self, reports: &[Report]) -> SqlResult<bool> {
        let count = new_map_count(self.cfg.bucket_count, reports);
        if count == 0 || count > MAX_BUCKETS {
            return Err(SqlError::internal(format!(
                "bucket_count must be between 1 and {} (it is {})",
                MAX_BUCKETS, count
            )));
        }
        // Buckets that a shard with a map owns or moves.
        let mut taken = vec![false; count as usize + 1];
        for r in reports.iter().filter(|r| r.count > 0) {
            for &(lo, hi) in &r.owned {
                for b in lo..=hi.min(count) {
                    taken[b as usize] = true;
                }
            }
            for (b, ..) in &r.others {
                if *b <= count {
                    taken[*b as usize] = true;
                }
            }
        }
        let initial = initial_ranges(count, self.shards.len());
        for (i, r) in reports.iter().enumerate() {
            if r.count > 0 || self.shards[i].removed() {
                continue;
            }
            let (lo, hi) = initial[i];
            let mut mine: Vec<(u32, u32)> = Vec::new();
            if lo > 0 {
                for b in lo..=hi {
                    if taken[b as usize] {
                        continue;
                    }
                    match mine.last_mut() {
                        Some(last) if last.1 + 1 == b => last.1 = b,
                        _ => mine.push((b, b)),
                    }
                }
            }
            let mut m = Mp::new();
            m.arr(2).uint(count as u64).arr(mine.len());
            for (lo, hi) in &mine {
                m.arr(2).uint(*lo as u64).uint(*hi as u64);
            }
            let d = self.call(i, "tarn_bucket_init", &m.0).await?;
            if crate::mpw::status_is(&d, "exists") {
                // another server made it meanwhile
                let mut rd = Rd::new(&d);
                rd.array()?; // the return values
                rd.array()?; // {'exists', count}
                rd.skip()?;
                if rd.value()?.as_i64() != Some(count as i64) {
                    return Ok(false);
                }
                continue;
            }
            lua_status(&d)?;
            tracing::info!(
                shard = self.shards[i].id,
                "made the bucket map: {} of {} buckets ({})",
                mine.iter().map(|(lo, hi)| hi - lo + 1).sum::<u32>(),
                count,
                ranges_text(&mine)
            );
        }
        Ok(true)
    }

    /// Read the map again after a request was refused for its bucket;
    /// concurrent callers share one read.
    pub async fn refresh_buckets(&self) -> SqlResult<()> {
        let seen = self.bucket_loads();
        let _g = self.bucket_mu.lock().await;
        if self.bucket_loads() != seen {
            return Ok(());
        }
        self.load_buckets().await
    }

    // ---- moves ----------------------------------------------------------------

    /// Move bucket `b` to the shard at position `to` (false: it is there
    /// already). `timeout`: how long the sending shard may wait for the
    /// transactions that hold locks on the bucket's rows, each time.
    pub async fn move_bucket(&self, b: u32, to: usize, timeout: f64) -> SqlResult<bool> {
        self.refresh_buckets().await?;
        if self.bucket_map().reports.is_empty() {
            return Err(SqlError::unsupported(
                "bucket moves on storage nodes from before them (they keep no bucket map)",
            ));
        }
        let count = self.bucket_map().count;
        if b == 0 || b > count {
            return Err(SqlError::new(
                1210,
                "HY000",
                format!("bucket {} does not exist (there are {})", b, count),
            ));
        }
        let name = format!("bucket:{}", b);
        let id = self.job_owner();
        let deadline = Instant::now() + Duration::from_secs_f64(timeout.max(1.0));
        // (the arguments are only known once the job holds the bucket)
        let mut placeholder = Mp::new();
        placeholder.map(0);
        let placeholder = placeholder.0;
        let mut job = self
            .job_begin_as(
                &id,
                JobKind::Move,
                &name,
                std::slice::from_ref(&name),
                0,
                &placeholder,
                deadline,
            )
            .await?;
        // Under the job, nothing else moves the bucket: read where it is.
        let res: SqlResult<Option<Move>> = async {
            // (and no shard joins or leaves: buckets move to the cluster's
            // active shards, as this server knows them since the job began)
            if self.shards[to].removed() || !self.shard_active(to) {
                return Err(SqlError::new(
                    1210,
                    "HY000",
                    format!(
                        "shard {} is leaving the cluster: no bucket moves to it",
                        self.shards[to].id
                    ),
                ));
            }
            self.reload_buckets().await?;
            let map = self.bucket_map();
            if !map.settled() && map.moving.contains(&b) {
                return Err(SqlError::moved(format!("bucket {} is being moved; try again", b)));
            }
            let from = map.shard(b);
            if from == to {
                return Ok(None);
            }
            Ok(Some(Move {
                bucket: b,
                from,
                to,
                id: id.clone(),
            }))
        }
        .await;
        let mv = match res {
            Ok(Some(mv)) => mv,
            Ok(None) => {
                self.job_end(&job.owner).await?;
                return Ok(false);
            }
            Err(e) => {
                self.job_end(&job.owner).await?;
                return Err(e);
            }
        };
        job.args = mv.args(self);
        self.job_args(&job).await?;
        let mut flipped = false;
        let r = self
            .with_job_alive(&job.owner, self.move_steps(&mv, &job, timeout, &mut flipped))
            .await;
        match r {
            Ok(()) => {
                self.job_end(&job.owner).await?;
                self.refresh_buckets().await?;
                Ok(true)
            }
            Err(e) if !flipped => {
                if let Err(a) = self.abort_move(&mv).await {
                    // The job stays: whoever finishes it gives the move up.
                    tracing::warn!("giving the move of bucket {} up: {}", b, a);
                    return Err(e);
                }
                self.job_end(&job.owner).await?;
                Err(e)
            }
            Err(e) => Err(SqlError::new(
                1180,
                "40003",
                format!(
                    "the move of bucket {} did not finish ({}); it completes in the background",
                    b, e.message
                ),
            )),
        }
    }

    /// Call a step of a move on the shard at position `shard`; the reply
    /// past its status.
    async fn move_step(&self, shard: usize, proc: &str, args: &[u8]) -> SqlResult<bytes::Bytes> {
        let d = self.move_call(shard, proc, args).await?;
        lua_status(&d)?;
        Ok(d)
    }

    /// The reply of a step of a move, tried again for up to `STEP_RETRY`
    /// after transient errors.
    async fn move_call(&self, shard: usize, proc: &str, args: &[u8]) -> SqlResult<bytes::Bytes> {
        let deadline = Instant::now() + STEP_RETRY;
        let mut wait = Duration::from_millis(50);
        loop {
            let r = self.call(shard, proc, args).await;
            let e = match &r {
                Err(e) => e.clone(),
                Ok(d) => match lua_status(d) {
                    Err(e) => e,
                    Ok(_) => return r,
                },
            };
            if !transient(&e) || Instant::now() + wait > deadline {
                return r;
            }
            tracing::info!(shard = self.shards[shard].id, "{}: {}; trying again", proc, e.message);
            tokio::time::sleep(wait).await;
            wait = (wait * 2).min(Duration::from_secs(1));
        }
    }

    async fn move_steps(&self, mv: &Move, job: &Job, timeout: f64, flipped: &mut bool) -> SqlResult<()> {
        let b = mv.bucket as u64;
        let (s_id, r_id) = (self.shards[mv.from].id as u64, self.shards[mv.to].id as u64);
        let mut a = Mp::new();
        a.arr(3).uint(b).uint(s_id).str(&mv.id);
        self.move_step(mv.to, "tarn_bucket_recv_begin", &a.0).await?;
        let mut gen = self.send_begin(mv, r_id, timeout).await?;
        let mut rows = self.copy_bucket(mv).await?;
        let mut a = Mp::new();
        a.arr(2).uint(b).str(&mv.id);
        self.move_step(mv.to, "tarn_bucket_hold", &a.0).await?;
        let mut recopies = 0;
        loop {
            let mut a = Mp::new();
            a.arr(3).uint(b).str(&mv.id).f64(timeout);
            self.move_step(mv.from, "tarn_bucket_fence", &a.0).await?;
            match self.copy_delta(mv, &gen).await? {
                Some(n) => {
                    rows += n;
                    break;
                }
                None if recopies < MAX_RECOPIES => {
                    // The sending shard's leader changed: its notes of the
                    // writes since the copy are gone. Copy it all again.
                    recopies += 1;
                    let mut a = Mp::new();
                    a.arr(2).uint(b).str(&mv.id);
                    self.move_step(mv.to, "tarn_bucket_recv_clear", &a.0).await?;
                    gen = self.send_begin(mv, r_id, timeout).await?;
                    rows = self.copy_bucket(mv).await?;
                }
                None => {
                    return Err(SqlError::conflict(format!(
                        "bucket {}: the sending shard kept changing leaders",
                        b
                    )))
                }
            }
        }
        // From here on the move only goes forward: a server that takes it
        // over finishes it. When the write of the phase fails it may still
        // have been made (a lost reply): read the job again, and give the
        // move up only if it is ours and not flipped; when that cannot be
        // told, leave the job to whoever finishes it.
        if let Err(e) = self.job_phase(job, "flip").await {
            match self
                .jobs()
                .await
                .map(|js| js.into_iter().find(|j| j.owner == job.owner))
            {
                Ok(Some(j)) if j.phase == "flip" => {}
                Ok(Some(_)) => return Err(e),
                _ => {
                    *flipped = true;
                    return Err(e);
                }
            }
        }
        *flipped = true;
        self.flip(mv).await?;
        self.stats
            .bucket_moves
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tracing::info!(
            "moved bucket {} ({} rows) from shard {} to shard {}",
            b,
            rows,
            s_id,
            r_id
        );
        Ok(())
    }

    /// S starts noting the writes to the bucket: the generation of its notes.
    async fn send_begin(&self, mv: &Move, r_id: u64, timeout: f64) -> SqlResult<String> {
        let mut a = Mp::new();
        a.arr(4).uint(mv.bucket as u64).uint(r_id).str(&mv.id).f64(timeout);
        let d = self.move_step(mv.from, "tarn_bucket_send_begin", &a.0).await?;
        let (_, mut rd) = lua_status(&d)?;
        rd.string()
    }

    /// Copy the rows of the bucket from S to R, a page at a time.
    async fn copy_bucket(&self, mv: &Move) -> SqlResult<u64> {
        let mut after: Vec<u8> = vec![0xc0];
        let mut n = 0u64;
        loop {
            let mut a = Mp::new();
            a.arr(2).uint(mv.bucket as u64).raw(&after);
            let d = self.move_step(mv.from, "tarn_bucket_rows", &a.0).await?;
            let (_, mut rd) = lua_status(&d)?;
            let rows = rd.raw_value()?.to_vec();
            n += Rd::new(&rows).array()? as u64;
            let next = if rd.buf.is_empty() || rd.is_nil() {
                None
            } else {
                Some(rd.raw_value()?.to_vec())
            };
            self.recv_rows(mv, &rows).await?;
            match next {
                Some(p) => after = p,
                None => return Ok(n),
            }
        }
    }

    /// Copy the rows written since generation `gen`'s notes began; None
    /// when the notes were lost.
    async fn copy_delta(&self, mv: &Move, gen: &str) -> SqlResult<Option<u64>> {
        let mut from = 1u64;
        let mut n = 0u64;
        loop {
            let mut a = Mp::new();
            a.arr(4).uint(mv.bucket as u64).str(&mv.id).str(gen).uint(from);
            let d = self.move_call(mv.from, "tarn_bucket_delta", &a.0).await?;
            if crate::mpw::status_is(&d, "lost") {
                return Ok(None);
            }
            let (_, mut rd) = lua_status(&d)?;
            let rows = rd.raw_value()?.to_vec();
            n += Rd::new(&rows).array()? as u64;
            let next = if rd.buf.is_empty() || rd.is_nil() {
                None
            } else {
                rd.value()?.as_i64()
            };
            self.recv_rows(mv, &rows).await?;
            match next {
                Some(i) => from = i as u64,
                None => return Ok(Some(n)),
            }
        }
    }

    async fn recv_rows(&self, mv: &Move, rows: &[u8]) -> SqlResult<()> {
        let mut a = Mp::new();
        a.arr(3).uint(mv.bucket as u64).str(&mv.id).raw(rows);
        self.move_step(mv.to, "tarn_bucket_recv_rows", &a.0).await.map(|_| ())
    }

    /// S sends the bucket, R takes it over (each repeatable).
    async fn flip(&self, mv: &Move) -> SqlResult<()> {
        let mut a = Mp::new();
        a.arr(2).uint(mv.bucket as u64).str(&mv.id);
        let d = self.move_call(mv.from, "tarn_bucket_send", &a.0).await?;
        if crate::mpw::status_is(&d, "not_moving") {
            // S gave the move up although the job says 'flip' (its driver
            // gave it up after its write of the phase seemed lost, see
            // move_steps): S never sent the bucket, so R gives it up too.
            tracing::warn!(
                "bucket {}: shard {} had given the move up; giving it up on shard {}",
                mv.bucket,
                self.shards[mv.from].id,
                self.shards[mv.to].id
            );
            self.move_step(mv.to, "tarn_bucket_recv_abort", &a.0).await?;
            return Ok(());
        }
        lua_status(&d)?;
        self.move_step(mv.to, "tarn_bucket_recv", &a.0).await?;
        Ok(())
    }

    /// Give a move up: S first, which takes the bucket back unless it has
    /// sent it; then R drops what it received. A bucket S has sent goes
    /// forward instead: R takes it over.
    async fn abort_move(&self, mv: &Move) -> SqlResult<()> {
        let mut a = Mp::new();
        a.arr(2).uint(mv.bucket as u64).str(&mv.id);
        let d = self.move_call(mv.from, "tarn_bucket_send_abort", &a.0).await?;
        match lua_status(&d) {
            Err(e) if e.message.contains("was sent already") => {
                self.move_step(mv.to, "tarn_bucket_recv", &a.0).await?;
                return Ok(());
            }
            Err(e) => return Err(e),
            Ok(_) => {}
        }
        self.move_step(mv.to, "tarn_bucket_recv_abort", &a.0).await?;
        Ok(())
    }

    /// Finish a move whose server stopped (the job is ours now): forward
    /// from phase 'flip', else back.
    pub(crate) async fn finish_move(&self, job: &Job) -> SqlResult<()> {
        // The phase as it is now that the job is ours (its server can no
        // longer change it).
        let Some(job) = self.jobs().await?.into_iter().find(|j| j.owner == job.owner) else {
            return Ok(());
        };
        let Ok(mv) = Move::of_args(self, &job.args) else {
            // Stopped before it recorded what it moves: no shard took a step.
            return self.job_end(&job.owner).await;
        };
        if job.phase == "flip" {
            self.flip(&mv).await?;
        } else {
            self.abort_move(&mv).await?;
        }
        self.job_end(&job.owner).await?;
        self.refresh_buckets().await?;
        Ok(())
    }

    /// `SHOW TARN BUCKETS`: per shard, the buckets it owns and those in a
    /// move.
    pub async fn show_buckets(&self) -> SqlResult<Vec<(u32, u32, String, u64, String)>> {
        self.load_members().await?;
        let reports = self.bucket_reports().await?;
        let mut out = Vec::new();
        for (i, r) in reports.iter().enumerate() {
            if self.shards[i].removed() {
                continue;
            }
            let n: u32 = r.owned.iter().map(|(lo, hi)| hi - lo + 1).sum();
            let moving = r
                .others
                .iter()
                .map(|(b, st, peer, _)| match peer {
                    Some(p) => format!(
                        "{} {} {} {}",
                        b,
                        st,
                        if st == "sent" || st == "sending" || st == "fenced" {
                            "to"
                        } else {
                            "from"
                        },
                        p
                    ),
                    None => format!("{} {}", b, st),
                })
                .collect::<Vec<_>>()
                .join(", ");
            out.push((self.shards[i].id, n, ranges_text(&r.owned), r.epoch, moving));
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report(count: u32, epoch: u64, owned: &[(u32, u32)], others: &[(u32, &str, u32)]) -> Report {
        Report {
            count,
            tables: 0,
            epoch,
            owned: owned.to_vec(),
            others: others
                .iter()
                .map(|&(b, s, p)| (b, s.to_string(), Some(p), Some("m".to_string())))
                .collect(),
            mver: 0,
        }
    }

    fn pos() -> HashMap<u32, usize> {
        [(1, 0), (2, 1), (3, 2)].into_iter().collect()
    }

    #[test]
    fn initial_layout_is_contiguous_and_covers_every_bucket() {
        let r = initial_ranges(3000, 3);
        assert_eq!(r, vec![(1, 1000), (1001, 2000), (2001, 3000)]);
        let r = initial_ranges(10, 4);
        assert_eq!(r.first().unwrap().0, 1);
        assert_eq!(r.last().unwrap().1, 10);
        for w in r.windows(2) {
            assert_eq!(w[0].1 + 1, w[1].0);
        }
        let m = BucketMap::initial(10, 4);
        for b in 1..=10 {
            let s = m.shard(b);
            assert!(r[s].0 <= b && b <= r[s].1);
        }
    }

    #[test]
    fn map_from_reports() {
        // shard 1 sent bucket 5 to shard 2, which has not taken it over yet
        let m = BucketMap::from_reports(
            vec![
                report(8, 2, &[(1, 4)], &[(5, "sent", 2)]),
                report(8, 1, &[(6, 8)], &[(5, "held", 1)]),
            ],
            &pos(),
        )
        .unwrap();
        assert_eq!(
            (1..=8).map(|b| m.shard(b)).collect::<Vec<_>>(),
            vec![0, 0, 0, 0, 1, 1, 1, 1]
        );
        assert!(!m.settled());
        assert_eq!(m.moving, vec![5]);
        assert_eq!(m.epochs, vec![2, 1]);
        // received
        let m = BucketMap::from_reports(
            vec![report(8, 2, &[(1, 4)], &[(5, "sent", 2)]), report(8, 2, &[(5, 8)], &[])],
            &pos(),
        )
        .unwrap();
        assert!(m.settled());
        assert_eq!(m.shard(5), 1);
        // a shard that is sending a bucket still owns it
        let m = BucketMap::from_reports(
            vec![
                report(8, 1, &[(1, 5)], &[(5, "fenced", 2)]),
                report(8, 1, &[(6, 8)], &[(5, "held", 1)]),
            ],
            &pos(),
        )
        .unwrap();
        assert!(m.settled());
        assert_eq!(m.shard(5), 0);
    }

    #[test]
    fn a_bucket_in_flight_goes_where_it_is_received() {
        // bucket 5 went 1 -> 2 long ago (shard 1 still says so) and is now
        // going 2 -> 3
        let m = BucketMap::from_reports(
            vec![
                report(9, 3, &[(1, 4)], &[(5, "sent", 2)]),
                report(9, 4, &[(6, 6)], &[(5, "sent", 3)]),
                report(9, 1, &[(7, 9)], &[(5, "held", 2)]),
            ],
            &pos(),
        )
        .unwrap();
        assert_eq!(m.moving, vec![5]);
        assert_eq!(m.shard(5), 2);
        assert!(m.epoch_of(0).is_err());
    }

    #[test]
    fn a_shard_the_map_does_not_have_is_refused() {
        // (a shard that joined after the map was read: the read of every
        // shard reads the map again)
        let map =
            BucketMap::from_reports(vec![report(8, 4, &[(1, 4)], &[]), report(8, 2, &[(5, 8)], &[])], &pos()).unwrap();
        assert_eq!(map.epoch_of(1).unwrap(), 2);
        let e = map.epoch_of(2).unwrap_err();
        assert!(e.moved, "{}", e.message);
    }

    #[test]
    fn bad_maps_are_refused() {
        let e = BucketMap::from_reports(vec![report(8, 1, &[(1, 5)], &[]), report(8, 1, &[(5, 8)], &[])], &pos())
            .unwrap_err();
        assert!(e.message.contains("bucket 5"), "{}", e.message);
        assert!(BucketMap::from_reports(
            vec![report(8, 1, &[(1, 4)], &[]), report(16, 1, &[(5, 8)], &[])],
            &pos()
        )
        .is_err());
        assert!(BucketMap::from_reports(vec![report(8, 1, &[(1, 8)], &[]), report(0, 0, &[], &[])], &pos()).is_err());
    }

    #[test]
    fn report_parsing() {
        let mut m2 = Mp::new();
        m2.arr(1).arr(5).str("ok").uint(16).uint(3);
        m2.arr(2).arr(2).uint(1).uint(4).arr(2).uint(6).uint(8);
        m2.arr(1).arr(4).uint(5).str("sent").uint(2).str("mv");
        let r = parse_report(&m2.0).unwrap();
        assert_eq!(r.count, 16);
        assert_eq!(r.epoch, 3);
        assert_eq!(r.owned, vec![(1, 4), (6, 8)]);
        assert_eq!(r.others, vec![(5, "sent".to_string(), Some(2), Some("mv".to_string()))]);
        assert_eq!(r.mver, 0);
        // with the shard's membership version
        let mut m6 = Mp::new();
        m6.arr(1)
            .arr(6)
            .str("ok")
            .uint(16)
            .uint(3)
            .arr(1)
            .arr(2)
            .uint(1)
            .uint(16)
            .arr(0)
            .uint(7);
        let r6 = parse_report(&m6.0).unwrap();
        assert_eq!((r6.owned.clone(), r6.others.len(), r6.mver), (vec![(1, 16)], 0, 7));
        let mut m3 = Mp::new();
        m3.arr(1).arr(2).str("ok").uint(0);
        assert_eq!(parse_report(&m3.0).unwrap().count, 0);
        assert_eq!(ranges_text(&r.owned), "1-4, 6-8");
        // no map, two tables
        let mut m4 = Mp::new();
        m4.arr(1).arr(3).str("ok").uint(0).uint(2);
        let r4 = parse_report(&m4.0).unwrap();
        assert_eq!((r4.count, r4.tables), (0, 2));
    }

    #[test]
    fn the_bucket_count_of_a_new_map() {
        let none = Report::default();
        let tables = Report {
            tables: 3,
            ..Report::default()
        };
        let mapped = Report {
            count: 512,
            owned: vec![(1, 512)],
            ..Report::default()
        };
        // a new cluster
        assert_eq!(new_map_count(None, &[none.clone(), none.clone()]), NEW_CLUSTER_BUCKETS);
        assert_eq!(new_map_count(Some(64), &[none.clone(), none.clone()]), 64);
        // made by a version from before the map: the count its rows use
        assert_eq!(new_map_count(None, &[none.clone(), tables.clone()]), LEGACY_BUCKETS);
        assert_eq!(new_map_count(Some(4096), &[tables.clone(), tables.clone()]), 4096);
        // a shard with a map (another server made it, or a shard was added)
        assert_eq!(new_map_count(None, &[mapped.clone(), none.clone()]), 512);
        assert_eq!(new_map_count(Some(64), &[tables, none, mapped]), 512);
    }
}
