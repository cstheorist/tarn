//! The cluster's shards (issue #25; lua/tarn.lua "Membership",
//! docs/design.md §2.13): the membership the shards keep, and adding a
//! shard while the cluster serves.
//!
//! Every shard keeps the membership (`tarn_members`): a version and the
//! shards with their replicas. A SQL server starts from its configuration,
//! reads the membership from the shards, and reads it again when a shard
//! reports a newer version (in its part of the bucket map, or by refusing a
//! request routed with an older one). `TARN ADD SHARD` runs as a job that
//! every other job waits for: it copies every table to the new shard, gives
//! it a bucket map with no buckets, and raises the membership on every
//! shard. Buckets then move to the new shard as to any other.

use crate::cluster::{Cluster, Job, JobKind, Shard};
use crate::mpw::{lua_status, Mp, Rd};
use futures::StreamExt;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tarn_common::{SqlError, SqlResult, Value};

/// The native export, rather than this gateway's catalog cache, decides
/// whether JOIN requires lazy-capable replicas. Retired-only clusters must
/// retain their retirement fences too.
struct SchemaImport<'a> {
    export: &'a [u8],
    lazy: bool,
}

impl<'a> SchemaImport<'a> {
    fn parse(export: &'a [u8]) -> SqlResult<Self> {
        let mut rd = Rd::new(export);
        // Old Lua can encode an empty table as an empty map.
        if rd.peek_map().is_some() {
            if rd.map()? == 0 && rd.buf.is_empty() {
                return Ok(Self { export, lazy: false });
            }
            return Err(crate::mpw::proto("schema export must be an array"));
        }
        let mut lazy = false;
        for pos in 0..rd.array()? {
            let mut tag = false;
            let mut schema_version = false;
            let fields = rd.map()?;
            for _ in 0..fields {
                match rd.string()?.as_str() {
                    "lazy_v1" => {
                        if pos != 0 || tag {
                            return Err(crate::mpw::proto("lazy schema tag must occur first, exactly once"));
                        }
                        tag = true;
                        let (mut version, mut active, mut retired) = (false, false, false);
                        for _ in 0..rd.map()? {
                            match rd.string()?.as_str() {
                                "version" => {
                                    if version || !matches!(rd.value()?, Value::Int(1) | Value::UInt(1)) {
                                        return Err(SqlError::unsupported("unsupported lazy schema export version"));
                                    }
                                    version = true;
                                }
                                "active" => {
                                    if active {
                                        return Err(crate::mpw::proto("duplicate active lazy export"));
                                    }
                                    active = true;
                                    for _ in 0..rd.array()? {
                                        rd.skip()?;
                                    }
                                }
                                "retired" => {
                                    if retired {
                                        return Err(crate::mpw::proto("duplicate retired lazy export"));
                                    }
                                    retired = true;
                                    for _ in 0..rd.array()? {
                                        rd.skip()?;
                                    }
                                }
                                _ => rd.skip()?,
                            }
                        }
                        if !version || !active || !retired {
                            return Err(crate::mpw::proto("incomplete lazy schema export"));
                        }
                    }
                    "schema_version" => {
                        if schema_version || !matches!(rd.value()?, Value::UInt(_) | Value::Int(0..)) {
                            return Err(crate::mpw::proto("invalid exported native schema version"));
                        }
                        schema_version = true;
                    }
                    _ => rd.skip()?,
                }
            }
            if tag {
                if !schema_version || fields != 2 {
                    return Err(crate::mpw::proto("invalid lazy schema export header"));
                }
                lazy = true;
            }
        }
        if !rd.buf.is_empty() {
            return Err(crate::mpw::proto("trailing schema export data"));
        }
        Ok(Self { export, lazy })
    }

    fn args(&self) -> Vec<u8> {
        let mut m = Mp::new();
        m.arr(if self.lazy { 2 } else { 1 }).raw(self.export);
        if self.lazy {
            m.uint(1);
        }
        m.0
    }
}

/// One shard of the membership.
#[derive(Clone, Debug, PartialEq)]
pub struct Member {
    pub id: u32,
    pub replicas: Vec<String>,
    /// 'active' or 'removed'.
    pub state: String,
}

/// The membership: its version and its shards.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Members {
    pub ver: u64,
    pub shards: Vec<Member>,
}

impl Members {
    pub fn encode(&self, m: &mut Mp) {
        m.map(2).str("ver").uint(self.ver).str("shards").arr(self.shards.len());
        for s in &self.shards {
            m.map(3)
                .str("id")
                .uint(s.id as u64)
                .str("replicas")
                .arr(s.replicas.len());
            for r in &s.replicas {
                m.str(r);
            }
            m.str("state").str(&s.state);
        }
    }

    /// The arguments of a job that records it.
    fn args(&self) -> Vec<u8> {
        let mut m = Mp::new();
        self.encode(&mut m);
        m.0
    }

    /// Decode what `encode` wrote (a job's arguments).
    fn decode(rd: &mut Rd) -> SqlResult<Members> {
        let mut out = Members::default();
        for _ in 0..rd.map()? {
            match rd.string()?.as_str() {
                "ver" => out.ver = rd.value()?.as_i64().unwrap_or(0).max(0) as u64,
                "shards" => out.shards = decode_shards(rd)?,
                _ => rd.skip()?,
            }
        }
        Ok(out)
    }
}

/// A list of members: {{id, replicas, state}, ...} (Lua tables, as maps).
fn decode_shards(rd: &mut Rd) -> SqlResult<Vec<Member>> {
    let mut out = Vec::new();
    if rd.is_nil() {
        rd.skip()?;
        return Ok(out);
    }
    // (an empty Lua table may come as a map)
    let n = match rd.peek_map() {
        Some(_) => {
            let n = rd.map()?;
            for _ in 0..n {
                rd.skip()?;
                rd.skip()?;
            }
            return Ok(out);
        }
        None => rd.array()?,
    };
    for _ in 0..n {
        let mut m = Member {
            id: 0,
            replicas: Vec::new(),
            state: "active".into(),
        };
        for _ in 0..rd.map()? {
            match rd.string()?.as_str() {
                "id" => m.id = rd.value()?.as_i64().unwrap_or(0) as u32,
                "replicas" => {
                    for _ in 0..rd.array()? {
                        m.replicas.push(rd.string()?);
                    }
                }
                "state" => m.state = rd.string()?,
                _ => rd.skip()?,
            }
        }
        out.push(m);
    }
    Ok(out)
}

/// A shard's reply to `tarn_members`: {'ok', ver, shards}.
pub fn parse_members(d: &[u8]) -> SqlResult<Members> {
    let (_, mut rd) = lua_status(d)?;
    let ver = rd.value()?.as_i64().unwrap_or(0).max(0) as u64;
    let shards = decode_shards(&mut rd)?;
    Ok(Members { ver, shards })
}

/// The shard id in a `tarn_info` reply (a map).
fn info_shard_id(d: &[u8]) -> SqlResult<u32> {
    let mut rd = Rd::new(d);
    rd.array()?;
    for _ in 0..rd.map()? {
        if rd.string()? == "shard_id" {
            return Ok(rd.value()?.as_i64().unwrap_or(0) as u32);
        }
        rd.skip()?;
    }
    Err(crate::mpw::proto("tarn_info: no shard_id"))
}

/// How long a change of the cluster's shards waits for the jobs in
/// progress (it waits for all of them), and then for every shard to record
/// the new membership.
const ADD_WAIT: Duration = Duration::from_secs(60);

/// How many buckets a shard that leaves moves at once.
const DRAIN_PARALLEL: usize = 8;

/// A change of the cluster's shards (a job of kind `members`).
enum Change<'a> {
    /// Shard `id` joins, with these replicas.
    Add { id: u32, replicas: &'a [String] },
    /// Shard `id` leaves: 'draining', then 'removed'.
    State { id: u32, state: &'static str },
}

impl Cluster {
    /// The membership this server knows (version 0: its configuration's).
    pub fn members(&self) -> Members {
        let m = self.members.read().clone();
        if m.ver > 0 {
            return m;
        }
        Members {
            ver: 0,
            shards: self
                .cfg
                .shards
                .iter()
                .map(|s| Member {
                    id: s.id,
                    replicas: s.replicas.clone(),
                    state: "active".into(),
                })
                .collect(),
        }
    }

    /// Read the membership from the shards, and take in a newer one than
    /// this server knows: the shards that joined are added at the end of
    /// `shards`, those that left are marked. A shard that cannot be reached
    /// is passed over when another answers (it may be one that left, which
    /// the others' membership says).
    pub(crate) async fn load_members(&self) -> SqlResult<()> {
        use std::sync::atomic::Ordering;
        let seen = self.member_reads.load(Ordering::Acquire);
        let _g = self.members_mu.lock().await;
        if self.member_reads.load(Ordering::Acquire) != seen {
            return Ok(());
        }
        let live: Vec<usize> = self.live_shards().collect();
        let replies = self
            .read_fanout(live.iter().map(|&i| self.call(i, "tarn_members", &[0x90])))
            .await;
        let mut best: Option<Members> = None;
        let mut failed: Option<SqlError> = None;
        let mut answered = false;
        for r in replies {
            let d = match r {
                Ok(d) => d,
                // Storage nodes from before membership: the configuration's.
                Err(e) if e.message.contains("'tarn_members' is not defined") => {
                    answered = true;
                    continue;
                }
                Err(e) => {
                    failed.get_or_insert(e);
                    continue;
                }
            };
            answered = true;
            let m = parse_members(&d)?;
            if best.as_ref().is_none_or(|b| m.ver > b.ver) {
                best = Some(m);
            }
        }
        if !answered {
            if let Some(e) = failed {
                return Err(e);
            }
        }
        let Some(m) = best else {
            self.member_reads.fetch_add(1, Ordering::Release);
            return Ok(());
        };
        if m.ver <= self.member_ver() {
            self.member_reads.fetch_add(1, Ordering::Release);
            return Ok(());
        }
        for s in &m.shards {
            match self.shard_position(s.id) {
                None if s.state != "removed" => {
                    let p = self.add_known_shard(Shard::new(s.id, s.replicas.clone()))?;
                    tracing::info!("shard {} joined the cluster (position {})", s.id, p);
                }
                Some(p) if s.state == "removed" => self.shards[p].set_removed(),
                _ => {}
            }
        }
        self.set_members(m);
        self.member_reads.fetch_add(1, Ordering::Release);
        Ok(())
    }

    /// Whether the shard at position `p` is active (not leaving the
    /// cluster) in the membership this server knows.
    pub fn shard_active(&self, p: usize) -> bool {
        let id = self.shards[p].id;
        self.members
            .read()
            .shards
            .iter()
            .find(|s| s.id == id)
            .is_none_or(|s| s.state == "active")
    }

    /// `SHOW TARN SHARDS`: the membership, read from the shards.
    pub async fn show_members(&self) -> SqlResult<Members> {
        self.load_members().await?;
        Ok(self.members())
    }

    /// `TARN ADD SHARD id 'replica,...'`: add a shard with no buckets (they
    /// move to it afterwards).
    pub async fn add_shard(&self, id: u32, replicas: Vec<String>) -> SqlResult<()> {
        if id == 0 || replicas.is_empty() {
            return Err(SqlError::new(
                1210,
                "HY000",
                "TARN ADD SHARD needs a shard id and its replicas",
            ));
        }
        self.load_members().await?;
        self.change_members(&Change::Add {
            id,
            replicas: &replicas,
        })
        .await
    }

    /// `TARN REMOVE SHARD id`: shard `id` leaves the cluster. It is marked
    /// draining -- buckets no longer move to it, and it decides no
    /// transaction (participants refuse it as a decision shard) -- its
    /// buckets move to the other shards, and once no transaction needs it,
    /// it is marked removed: the shards and the SQL servers stop asking it,
    /// and its storage can be stopped. Run again, it goes on from where it
    /// stopped. The number of buckets it moved.
    pub async fn remove_shard(&self, id: u32) -> SqlResult<u64> {
        self.load_members().await?;
        let m = self.members();
        let state = m
            .shards
            .iter()
            .find(|s| s.id == id)
            .map(|s| s.state.clone())
            .ok_or_else(|| SqlError::new(1210, "HY000", format!("there is no shard {} in the cluster", id)))?;
        if state == "removed" {
            return Err(SqlError::new(
                1210,
                "HY000",
                format!("shard {} has left the cluster already", id),
            ));
        }
        let pos = self
            .shard_position(id)
            .ok_or_else(|| SqlError::new(1210, "HY000", format!("there is no shard {} in the cluster", id)))?;
        if pos == 0 {
            return Err(SqlError::new(
                1210,
                "HY000",
                format!(
                    "shard {} keeps the cluster's jobs and catalog (the first shard of the configuration): it cannot leave",
                    id
                ),
            ));
        }
        if !m.shards.iter().any(|s| s.id != id && s.state == "active") {
            return Err(SqlError::new(
                1210,
                "HY000",
                "no other shard would be left to take its buckets",
            ));
        }
        if state == "active" {
            self.change_members(&Change::State { id, state: "draining" }).await?;
        }
        let moved = self.drain(pos).await?;
        self.wait_unneeded(id).await?;
        self.change_members(&Change::State { id, state: "removed" }).await?;
        Ok(moved)
    }

    /// Move every bucket of the shard at position `pos` to the active
    /// shards, each to the one with the fewest, a few at a time: the number
    /// moved.
    async fn drain(&self, pos: usize) -> SqlResult<u64> {
        let mut moved = 0;
        let mut failed_passes = 0;
        let mut last_error: Option<SqlError> = None;
        let mut busy_until: Option<Instant> = None;
        loop {
            let rep = crate::buckets::parse_report(&self.call(pos, "tarn_buckets", &[0x90]).await?)?;
            let busy = rep.others.iter().any(|(_, st, ..)| st != "sent");
            let mine: Vec<u32> = rep.owned.iter().flat_map(|&(lo, hi)| lo..=hi).collect();
            if mine.is_empty() && !busy {
                return Ok(moved);
            }
            if failed_passes >= 3 {
                return Err(last_error
                    .unwrap_or_else(|| SqlError::new(1205, "HY000", "the shard's buckets did not move; try again")));
            }
            if mine.is_empty() {
                // a move in progress (one of another server, or one given
                // up that a takeover finishes): wait for it
                let until = *busy_until.get_or_insert_with(|| {
                    Instant::now() + Duration::from_secs_f64(self.cfg.move_timeout * 2.0 + 10.0)
                });
                if Instant::now() >= until {
                    return Err(SqlError::new(
                        1205,
                        "HY000",
                        format!("shard {} has a bucket move in progress; try again", self.shards[pos].id),
                    ));
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
            self.refresh_buckets().await?;
            let map = self.bucket_map();
            let targets: Vec<usize> = self
                .live_shards()
                .filter(|&p| p != pos && self.shard_active(p))
                .collect();
            if targets.is_empty() {
                return Err(SqlError::new(1210, "HY000", "no active shard to take the buckets"));
            }
            let mut have: HashMap<usize, u64> = targets.iter().map(|&p| (p, 0)).collect();
            for b in 1..=map.count {
                if let Some(n) = have.get_mut(&map.shard(b)) {
                    *n += 1;
                }
            }
            let mut plan = Vec::with_capacity(mine.len());
            for b in mine {
                let to = *targets.iter().min_by_key(|&&p| (have[&p], p)).unwrap();
                *have.get_mut(&to).unwrap() += 1;
                plan.push((b, to));
            }
            let results: Vec<(u32, SqlResult<bool>)> = futures::stream::iter(plan)
                .map(|(b, to)| async move { (b, self.move_bucket(b, to, self.cfg.move_timeout).await) })
                .buffer_unordered(DRAIN_PARALLEL)
                .collect()
                .await;
            let mut pass_moved = 0;
            for (b, r) in results {
                match r {
                    Ok(true) => pass_moved += 1,
                    Ok(false) => {}
                    Err(e) => {
                        tracing::warn!("draining shard {}: bucket {}: {}", self.shards[pos].id, b, e.message);
                        last_error = Some(e);
                    }
                }
            }
            moved += pass_moved;
            if pass_moved == 0 {
                failed_passes += 1;
            }
        }
    }

    /// Wait until no shard may still need shard `id` to finish a
    /// transaction (lua/tarn.lua `tarn_shard_records`); from when it began
    /// to leave, no new one can.
    async fn wait_unneeded(&self, id: u32) -> SqlResult<()> {
        let deadline = Instant::now() + Duration::from_secs_f64(self.cfg.max_txn_age.max(5.0) * 2.0);
        loop {
            let n = self.records_for(id).await?;
            if n == 0 {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(SqlError::new(
                    1205,
                    "HY000",
                    format!(
                        "shard {} is still needed to finish {} transaction(s); TARN REMOVE SHARD {} goes on when run again",
                        id, n, id
                    ),
                ));
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// The transactions on the shards in the cluster that may still need
    /// shard `id` to finish.
    async fn records_for(&self, id: u32) -> SqlResult<u64> {
        let mut a = Mp::new();
        a.arr(1).uint(id as u64);
        let live: Vec<usize> = self.live_shards().collect();
        let replies =
            futures::future::try_join_all(live.iter().map(|&p| self.call(p, "tarn_shard_records", &a.0))).await?;
        let mut n = 0;
        for d in replies {
            let (_, mut rd) = lua_status(&d)?;
            n += rd.value()?.as_i64().unwrap_or(0).max(0) as u64;
        }
        Ok(n)
    }

    /// The membership with change `ch` made (its version raised), or why it
    /// cannot be made.
    fn changed(&self, ch: &Change<'_>) -> SqlResult<Members> {
        let mut next = self.members();
        next.ver += 1;
        match *ch {
            Change::Add { id, replicas } => {
                if next.shards.iter().any(|s| s.id == id && s.state != "removed") {
                    return Err(SqlError::new(
                        1210,
                        "HY000",
                        format!("shard {} is in the cluster already", id),
                    ));
                }
                next.shards.retain(|s| s.id != id);
                next.shards.push(Member {
                    id,
                    replicas: replicas.to_vec(),
                    state: "active".into(),
                });
            }
            Change::State { id, state } => {
                let s =
                    next.shards.iter_mut().find(|s| s.id == id).ok_or_else(|| {
                        SqlError::new(1210, "HY000", format!("there is no shard {} in the cluster", id))
                    })?;
                if s.state == "removed" {
                    return Err(SqlError::new(
                        1210,
                        "HY000",
                        format!("shard {} has left the cluster already", id),
                    ));
                }
                s.state = state.into();
            }
        }
        Ok(next)
    }

    /// Make change `ch` as a job of kind `members`, which every other job
    /// waits for and which waits for them.
    async fn change_members(&self, ch: &Change<'_>) -> SqlResult<()> {
        let deadline = Instant::now() + ADD_WAIT;
        let name = "members".to_string();
        let args = self.changed(ch)?.args();
        let mut job = self
            .job_begin_as(
                &self.job_owner(),
                JobKind::Members,
                &name,
                std::slice::from_ref(&name),
                0,
                &args,
                deadline,
            )
            .await?;
        let owner = job.owner.clone();
        let mut applied = false;
        let r = self
            .with_job_alive(&owner, self.members_job(&mut job, ch, &mut applied))
            .await;
        match r {
            Ok(()) => {
                self.job_end(&owner).await?;
                self.refresh_buckets().await
            }
            // From phase 'apply' on, the job is finished (forward) by
            // whoever finds it abandoned: some shards may know the new
            // membership already, and moves must wait until all do.
            Err(e) if applied => Err(e),
            Err(e) => {
                // A shard that was to join may have the tables and a map,
                // but it is no member: the statement can run again.
                let _ = self.job_end(&owner).await;
                Err(e)
            }
        }
    }

    async fn members_job(&self, job: &mut Job, ch: &Change<'_>, applied: &mut bool) -> SqlResult<()> {
        // No other change of the shards runs while this job does: the
        // membership read now (one may have ended as this job began) is the
        // one to change.
        self.load_members().await?;
        let next = self.changed(ch)?;
        let args = next.args();
        if args != job.args {
            job.args = args;
            self.job_args(job).await?;
        }
        let new = match *ch {
            Change::Add { id, replicas } => Some(self.prepare_new_shard(id, replicas).await?),
            // (checked before the job too; a check here sees the shards as
            // the membership this job replaces left them)
            Change::State { id, state: "removed" } => {
                let n = self.records_for(id).await?;
                if n > 0 {
                    return Err(SqlError::new(
                        1205,
                        "HY000",
                        format!("shard {} is still needed to finish {} transaction(s)", id, n),
                    ));
                }
                None
            }
            Change::State { .. } => None,
        };
        // From here on the change is made.
        if let Err(e) = self.job_phase(job, "apply").await {
            // (Written or not? A job left in 'apply' is finished forward.)
            match self.jobs().await?.into_iter().find(|j| j.owner == job.owner) {
                Some(j) if j.phase == "apply" => {}
                Some(_) => return Err(e),
                None => {
                    *applied = true;
                    return Err(e);
                }
            }
        }
        *applied = true;
        if let Some(new) = new {
            self.add_known_shard(new)?;
        }
        // Every shard must learn it before the job ends: moves to a shard
        // that joins wait for the job (lua/tarn.lua "Membership").
        let until = Instant::now() + ADD_WAIT;
        loop {
            match self.publish_members(&next).await {
                Ok(()) => return Ok(()),
                Err(e) if Instant::now() >= until => return Err(e),
                Err(e) => {
                    tracing::warn!(
                        "changing the cluster's shards: recording the membership: {}; trying again",
                        e.message
                    );
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
    }

    /// Check the storage of shard `id` that is to join, and give it every
    /// table and a bucket map with no buckets: the shard, to add to
    /// `shards` once it joins.
    async fn prepare_new_shard(&self, id: u32, replicas: &[String]) -> SqlResult<Shard> {
        let new = Shard::new(id, replicas.to_vec());
        let client = new.client(self).await?;
        let call = |func: &'static str, args: Vec<u8>| {
            let client = client.clone();
            async move {
                let r = client.call(0, func, &args).await.map_err(crate::mpw::from_iproto)?;
                r.data().ok_or_else(|| crate::mpw::proto("no data"))
            }
        };
        // A Tarn storage node of shard `id`: one that has no buckets, and
        // no tables but the cluster's, which have no rows (a join that
        // stopped before the new shard joined leaves them).
        let d = call("tarn_info", vec![0x90]).await?;
        let got = info_shard_id(&d)?;
        if got != id {
            return Err(SqlError::new(
                1210,
                "HY000",
                format!("{} is a storage node of shard {}, not {}", replicas[0], got, id),
            ));
        }
        let count = self.bucket_map().count;
        let rep = crate::buckets::parse_report(&call("tarn_buckets", vec![0x90]).await?)?;
        if !rep.owned.is_empty() || !rep.others.is_empty() || (rep.count != 0 && rep.count != count) {
            return Err(SqlError::new(
                1210,
                "HY000",
                format!("shard {} has a bucket map of its own already", id),
            ));
        }
        // Every table, as the first shard has it (no schema change runs).
        let d = self.call(0, "tarn_schema_export", &[0x90]).await?;
        let (_, mut rd) = lua_status(&d)?;
        let start = d.len() - rd.buf.len();
        rd.skip()?;
        if !rd.buf.is_empty() {
            return Err(crate::mpw::proto("trailing schema export reply"));
        }
        let schema = SchemaImport::parse(&d[start..])?;
        if schema.lazy {
            // Check EVERY configured candidate replica before sending the
            // coordinator token. A capable leader alone is insufficient:
            // an older follower could later promote without row capture.
            self.lazy_capabilities_on(replicas).await?;
        }
        lua_status(&call("tarn_schema_import", schema.args()).await?)?;
        // A bucket map with no buckets.
        let mut m = Mp::new();
        m.arr(2).uint(count as u64).arr(0);
        let d = call("tarn_bucket_init", m.0).await?;
        if crate::mpw::status_is(&d, "exists") {
            let (_, mut rd) = lua_status(&d)?;
            let c = rd.value()?.as_i64().unwrap_or(0);
            if c != count as i64 {
                return Err(SqlError::new(
                    1210,
                    "HY000",
                    format!("shard {} has a bucket map of {} buckets, the cluster {}", id, c, count),
                ));
            }
        } else {
            lua_status(&d)?;
        }
        Ok(new)
    }

    /// Record membership `next` on every shard in it, and on a shard it
    /// removes (best effort: that one may be gone already).
    async fn publish_members(&self, next: &Members) -> SqlResult<()> {
        let mut m = Mp::new();
        m.arr(1);
        next.encode(&mut m);
        for s in &next.shards {
            let p = match self.shard_position(s.id) {
                Some(p) => p,
                None if s.state == "removed" => continue,
                None => self.add_known_shard(Shard::new(s.id, s.replicas.clone()))?,
            };
            if s.state == "removed" {
                if !self.shards[p].removed() {
                    if let Err(e) = self
                        .call(p, "tarn_members_set", &m.0)
                        .await
                        .and_then(|d| lua_status(&d).map(|_| ()))
                    {
                        tracing::info!("shard {} left the cluster; telling it so: {}", s.id, e.message);
                    }
                    self.shards[p].set_removed();
                }
                continue;
            }
            let d = self.call(p, "tarn_members_set", &m.0).await?;
            lua_status(&d)?;
        }
        self.set_members(next.clone());
        Ok(())
    }

    /// Finish an abandoned change of the cluster's shards (the job is ours
    /// now): forward from phase 'apply', else give it up.
    pub(crate) async fn finish_members(&self, job: &Job) -> SqlResult<()> {
        let Some(job) = self.jobs().await?.into_iter().find(|j| j.owner == job.owner) else {
            return Ok(());
        };
        if job.phase == "apply" {
            let next = Members::decode(&mut Rd::new(&job.args))?;
            self.publish_members(&next).await?;
        }
        self.job_end(&job.owner).await?;
        self.refresh_buckets().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lazy_tag(m: &mut Mp, active: bool, retired: bool, version: Value) {
        m.map(2)
            .str("schema_version")
            .uint(100)
            .str("lazy_v1")
            .map(3)
            .str("version")
            .val(&version)
            .str("active")
            .arr(active as usize);
        if active {
            m.map(1).str("gen").uint(10002);
        }
        m.str("retired").arr(retired as usize);
        if retired {
            m.arr(8)
                .uint(10001)
                .uint(10000)
                .uint(9)
                .str("retired")
                .uint(1)
                .map(0)
                .nil()
                .bool(true);
        }
    }

    #[test]
    fn schema_import_uses_authoritative_active_or_retired_tag() {
        for (active, retired) in [(true, false), (false, true), (true, true)] {
            let mut export = Mp::new();
            export.arr(2);
            lazy_tag(&mut export, active, retired, Value::UInt(1));
            export.map(1).str("name").str("d.t");
            // No catalog cache participates. Even a retired-only export
            // requires every candidate replica to support the token.
            let parsed = SchemaImport::parse(&export.0).unwrap();
            assert!(parsed.lazy);
            let args = parsed.args();
            let mut rd = Rd::new(&args);
            assert_eq!(rd.array().unwrap(), 2);
            assert_eq!(rd.raw_value().unwrap(), export.0);
            assert_eq!(rd.value().unwrap().as_i64(), Some(1));
            assert!(rd.buf.is_empty());
        }
    }

    #[test]
    fn ordinary_schema_import_keeps_one_argument_compatibility() {
        for export in [vec![0x90], vec![0x80]] {
            let parsed = SchemaImport::parse(&export).unwrap();
            assert!(!parsed.lazy);
            let args = parsed.args();
            let mut rd = Rd::new(&args);
            assert_eq!(rd.array().unwrap(), 1);
            assert_eq!(rd.raw_value().unwrap(), export);
            assert!(rd.buf.is_empty());
        }
        let mut export = Mp::new();
        export
            .arr(1)
            .map(2)
            .str("name")
            .str("d.t")
            .str("def")
            .map(1)
            .str("lazy")
            .arr(0);
        let parsed = SchemaImport::parse(&export.0).unwrap();
        assert!(!parsed.lazy);
        let args = parsed.args();
        let mut rd = Rd::new(&args);
        assert_eq!(rd.array().unwrap(), 1);
        assert_eq!(rd.raw_value().unwrap(), export.0);
        assert!(rd.buf.is_empty());
    }

    #[test]
    fn unsupported_or_malformed_lazy_join_tag_fails_before_import() {
        for version in [Value::UInt(0), Value::UInt(2), Value::Float(1.0), Value::str("1")] {
            let mut export = Mp::new();
            export.arr(1);
            lazy_tag(&mut export, true, false, version);
            assert!(SchemaImport::parse(&export.0).is_err());
        }
        let mut late = Mp::new();
        late.arr(2).map(1).str("name").str("d.t");
        lazy_tag(&mut late, true, false, Value::UInt(1));
        assert!(SchemaImport::parse(&late.0).is_err());
        let mut incomplete = Mp::new();
        incomplete
            .arr(1)
            .map(2)
            .str("schema_version")
            .uint(1)
            .str("lazy_v1")
            .map(2)
            .str("version")
            .uint(1)
            .str("active")
            .arr(0);
        assert!(SchemaImport::parse(&incomplete.0).is_err());
        let mut d = Mp::new();
        d.arr(1);
        lazy_tag(&mut d, true, false, Value::UInt(1));
        d.nil();
        assert!(SchemaImport::parse(&d.0).is_err());
        let mut ambiguous = Mp::new();
        ambiguous
            .arr(1)
            .map(3)
            .str("schema_version")
            .uint(1)
            .str("name")
            .str("d.t")
            .str("lazy_v1")
            .map(3)
            .str("version")
            .uint(1)
            .str("active")
            .arr(0)
            .str("retired")
            .arr(0);
        assert!(SchemaImport::parse(&ambiguous.0).is_err());
    }

    #[test]
    fn members_round_trip() {
        let m = Members {
            ver: 3,
            shards: vec![
                Member {
                    id: 1,
                    replicas: vec!["a:1".into(), "b:1".into()],
                    state: "active".into(),
                },
                Member {
                    id: 3,
                    replicas: vec!["c:1".into()],
                    state: "removed".into(),
                },
            ],
        };
        let mut mp = Mp::new();
        m.encode(&mut mp);
        assert_eq!(Members::decode(&mut Rd::new(&mp.0)).unwrap(), m);
        // as tarn_members replies
        let mut r = Mp::new();
        r.arr(1).arr(3).str("ok").uint(3);
        let mut sh = Mp::new();
        m.encode(&mut sh);
        let mut rd = Rd::new(&sh.0);
        rd.map().unwrap();
        rd.skip().unwrap();
        rd.skip().unwrap();
        rd.skip().unwrap();
        r.0.extend_from_slice(rd.buf);
        assert_eq!(parse_members(&r.0).unwrap(), m);
        // no record: version 0, no shards (an empty Lua table)
        let mut e = Mp::new();
        e.arr(1).arr(3).str("ok").uint(0).arr(0);
        assert_eq!(parse_members(&e.0).unwrap(), Members::default());
    }
}
