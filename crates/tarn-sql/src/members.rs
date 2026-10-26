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
use std::time::{Duration, Instant};
use tarn_common::{SqlError, SqlResult};

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

/// How long `TARN ADD SHARD` waits for the jobs in progress (it waits for
/// all of them), and then for every shard to record the new membership.
const ADD_WAIT: Duration = Duration::from_secs(60);

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
    /// `shards`, those that left are marked.
    pub(crate) async fn load_members(&self) -> SqlResult<()> {
        let _g = self.members_mu.lock().await;
        let live: Vec<usize> = (0..self.shards.len()).filter(|&i| !self.shards[i].removed()).collect();
        let replies = futures::future::join_all(live.iter().map(|&i| self.call(i, "tarn_members", &[0x90]))).await;
        let mut best: Option<Members> = None;
        for r in replies {
            let d = match r {
                Ok(d) => d,
                // Storage nodes from before membership: the configuration's.
                Err(e) if e.message.contains("'tarn_members' is not defined") => continue,
                Err(e) => return Err(e),
            };
            let m = parse_members(&d)?;
            if best.as_ref().is_none_or(|b| m.ver > b.ver) {
                best = Some(m);
            }
        }
        let Some(m) = best else { return Ok(()) };
        if m.ver <= self.member_ver() {
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
        Ok(())
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
        self.not_member(id)?;
        let deadline = Instant::now() + ADD_WAIT;
        let name = "members".to_string();
        let args = self.members_with(id, &replicas).args();
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
            .with_job_alive(&owner, self.add_shard_job(&mut job, id, &replicas, &mut applied))
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
                // The new shard may have the tables and a map, but it is no
                // member: the statement can run again.
                let _ = self.job_end(&owner).await;
                Err(e)
            }
        }
    }

    /// Fail if shard `id` is a member.
    fn not_member(&self, id: u32) -> SqlResult<()> {
        if self.members().shards.iter().any(|s| s.id == id && s.state != "removed") {
            return Err(SqlError::new(
                1210,
                "HY000",
                format!("shard {} is in the cluster already", id),
            ));
        }
        Ok(())
    }

    /// The membership with shard `id` added.
    fn members_with(&self, id: u32, replicas: &[String]) -> Members {
        let mut next = self.members();
        next.ver += 1;
        next.shards.retain(|s| s.id != id);
        next.shards.push(Member {
            id,
            replicas: replicas.to_vec(),
            state: "active".into(),
        });
        next
    }

    async fn add_shard_job(&self, job: &mut Job, id: u32, replicas: &[String], applied: &mut bool) -> SqlResult<()> {
        // No other change of the shards runs while this job does: the
        // membership read now (one may have ended as this job began) is the
        // one to extend.
        self.load_members().await?;
        self.not_member(id)?;
        let next = self.members_with(id, replicas);
        let args = next.args();
        if args != job.args {
            job.args = args;
            self.job_args(job).await?;
        }
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
        let mut m = Mp::new();
        m.arr(1).raw(rd.raw_value()?);
        lua_status(&call("tarn_schema_import", m.0).await?)?;
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
        // From here on the shard joins.
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
        self.add_known_shard(new)?;
        // Every shard must learn it before the job ends: moves to the new
        // shard wait for the job (lua/tarn.lua "Membership").
        let until = Instant::now() + ADD_WAIT;
        loop {
            match self.publish_members(&next).await {
                Ok(()) => return Ok(()),
                Err(e) if Instant::now() >= until => return Err(e),
                Err(e) => {
                    tracing::warn!(
                        "TARN ADD SHARD {}: recording the membership: {}; trying again",
                        id,
                        e.message
                    );
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
    }

    /// Record membership `next` on every shard (the new ones too).
    async fn publish_members(&self, next: &Members) -> SqlResult<()> {
        let mut m = Mp::new();
        m.arr(1);
        next.encode(&mut m);
        for s in &next.shards {
            if s.state == "removed" {
                continue;
            }
            let p = match self.shard_position(s.id) {
                Some(p) => p,
                None => self.add_known_shard(Shard::new(s.id, s.replicas.clone()))?,
            };
            let d = self.call(p, "tarn_members_set", &m.0).await?;
            lua_status(&d)?;
        }
        self.set_members(next.clone());
        Ok(())
    }

    /// Finish an abandoned `TARN ADD SHARD` (the job is ours now): forward
    /// from phase 'apply', else give it up.
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
