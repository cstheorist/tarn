//! ALTER TABLE adding a unique key without the shard key to a table: its
//! global index is built online (issue #292, tla/GlobalUniqueBuild.tla), in
//! one job (`JobKind::UniqueBuild`) of ALTER steps and a backfill
//! (`alter::online_steps`):
//! 1. `building`: the index is published building, its entries and
//!    conflicts spaces made, with no local index yet. From then on every
//!    write maintains its entries, and records a row whose key another
//!    row's entry holds as a conflict (`session::building`).
//! 2. `backfill`: the rows written before get their entries, or are
//!    recorded as conflicts, unless they already are: a page at a time,
//!    each shard's rows in primary-key order, through transactions whose
//!    commit validates the rows (`Session::building_backfill`). Once a
//!    page commits, the job stores the shard's cursor past it.
//! 3. `enforcing`: the table's local unique index is built; behind the
//!    drained fence each shard checks that no conflict of its is left
//!    (tarn_alter_prepare, `a.uix_check`); the index is published
//!    enforcing, its conflicts space dropped.
//! 4. `dropping`, if the third step fails (two rows share a key: 1062, or
//!    another error): the table is published as it was and the index's
//!    spaces dropped; the statement fails with the third step's error.
//!
//! Each step fences every shard, durably (`a.building`), and drains it
//! before any shard publishes. The job's phase names its stage and the
//! step's phase in it ("building:apply"); its arguments, the steps', the
//! cursors and the error to report. A server that takes the job over goes
//! on from there. The job holds the table, and bucket moves wait for it
//! throughout (tarn_job_begin), so that no row escapes the cursors.

use super::*;
use crate::session::Session;
use crate::txn::{Mode as TxnMode, Txn};
use std::collections::BTreeMap;

/// How long the backfill sends a page's transaction again while it fails
/// for a transient reason, such as a conflict with the table's writes.
const BACKFILL_RETRY: Duration = Duration::from_secs(300);
/// Rows per backfill transaction.
const BACKFILL_PAGE: u32 = 64;

/// The state of an online build (its job's arguments).
pub(crate) struct UixBuild {
    pub(crate) db: String,
    pub(crate) table: String,
    /// The `lua_args` of its three steps.
    pub(crate) steps: [Vec<u8>; 3],
    /// The building version (the first step's).
    pub(crate) ver: u64,
    /// By shard id: the backfill's cursor, the primary key of the last row
    /// it passed (None: none yet), and whether it is done there.
    pub(crate) cursors: BTreeMap<u32, (Option<Vec<Value>>, bool)>,
    /// The error the statement fails with, once the third step failed.
    pub(crate) error: Option<SqlError>,
}

impl UixBuild {
    fn encode(&self) -> Vec<u8> {
        let mut m = Mp::new();
        m.map(6 + self.error.is_some() as usize);
        m.str("db").str(&self.db);
        m.str("table").str(&self.table);
        m.str("ver").uint(self.ver);
        m.str("uix_build").arr(3);
        for s in &self.steps {
            m.raw(s);
        }
        m.str("cursors").arr(self.cursors.len());
        for (id, (after, done)) in &self.cursors {
            m.arr(3).uint(*id as u64).uint(*done as u64);
            m.opt_vals(after);
        }
        m.str("version").uint(1);
        if let Some(e) = &self.error {
            m.str("error")
                .arr(3)
                .uint(e.code as u64)
                .str(std::str::from_utf8(&e.sqlstate).unwrap_or("HY000"))
                .str(&e.message);
        }
        m.0
    }

    pub(crate) fn parse(args: &[u8]) -> SqlResult<UixBuild> {
        let mut rd = Rd::new(args);
        let (mut db, mut table, mut ver) = (String::new(), String::new(), 0);
        let mut steps: Vec<Vec<u8>> = Vec::new();
        let mut cursors = BTreeMap::new();
        let mut error = None;
        for _ in 0..rd.map()? {
            match rd.string()?.as_str() {
                "db" => db = rd.string()?,
                "table" => table = rd.string()?,
                "ver" => ver = rd.value()?.as_i64().unwrap_or(0) as u64,
                "uix_build" => {
                    for _ in 0..rd.array()? {
                        steps.push(rd.raw_value()?.to_vec());
                    }
                }
                "cursors" => {
                    for _ in 0..rd.array()? {
                        rd.array()?;
                        let id = rd.value()?.as_i64().unwrap_or(0) as u32;
                        let done = rd.value()?.as_i64() == Some(1);
                        let after = if rd.is_nil() {
                            rd.skip()?;
                            None
                        } else {
                            let n = rd.array()?;
                            Some((0..n).map(|_| rd.value()).collect::<SqlResult<Vec<Value>>>()?)
                        };
                        cursors.insert(id, (after, done));
                    }
                }
                "error" => {
                    rd.array()?;
                    let code = rd.value()?.as_i64().unwrap_or(0) as u16;
                    let state = rd.string()?;
                    let message = rd.string()?;
                    error = Some(SqlError::new(code, &state, message));
                }
                _ => rd.skip()?,
            }
        }
        let steps: [Vec<u8>; 3] = steps
            .try_into()
            .map_err(|_| SqlError::internal("an online index build without its three steps"))?;
        Ok(UixBuild {
            db,
            table,
            steps,
            ver,
            cursors,
            error,
        })
    }
}

impl Cluster {
    /// ALTER TABLE change `ch` of `def`, which only adds unique keys without
    /// the shard key (`alter::builds_globals_online`): their global indexes
    /// built online. Ok(false): the table changed before the job began, and
    /// the statement starts over.
    pub(crate) async fn alter_globals_online(
        &self,
        def: &TableDef,
        ch: &crate::alter::Change,
        deadline: Instant,
        lock_timeout: f64,
    ) -> SqlResult<bool> {
        let n = ch.def.indexes.len() - def.indexes.len();
        let first = self.alloc_space_ids(2 * n as u64).await?;
        let ids: Vec<(u32, u32)> = (0..n as u32).map(|k| (first + 2 * k, first + 2 * k + 1)).collect();
        let vers = [
            self.next_catalog_version().await?,
            self.next_catalog_version().await?,
            self.next_catalog_version().await?,
        ];
        let steps = crate::alter::online_steps(def, ch, vers, &ids)?;
        let st = UixBuild {
            db: def.db.clone(),
            table: def.name.clone(),
            steps: [
                crate::alter::lua_args(&steps[0], vers[0], lock_timeout),
                crate::alter::lua_args(&steps[1], vers[1], lock_timeout),
                crate::alter::lua_args(&steps[2], vers[2], lock_timeout),
            ],
            ver: vers[0],
            cursors: BTreeMap::new(),
            error: None,
        };
        let space = def.space_name();
        let job = self
            .job_begin(
                JobKind::UniqueBuild,
                &space,
                std::slice::from_ref(&space),
                vers[0],
                &st.encode(),
                deadline,
            )
            .await?;
        if !self.job_still(&job, def).await? {
            return Ok(false);
        }
        self.run_job(&job).await?;
        Ok(true)
    }

    /// Run (or go on with) an online build, keeping its job alive.
    pub(crate) async fn run_uix_build(&self, job: &Job) -> SqlResult<()> {
        let mut st = UixBuild::parse(&job.args)?;
        let mut phase = match job.phase.as_str() {
            "prepare" => "building:prepare".to_string(),
            p => p.to_string(),
        };
        loop {
            let (stage, step) = phase.split_once(':').unwrap_or((phase.as_str(), ""));
            let (stage, step) = (stage.to_string(), step.to_string());
            match stage.as_str() {
                "building" => match self.uix_step(job, "building", &st.steps[0], &step).await? {
                    None => phase = self.uix_phase(job, "backfill").await?,
                    Some(e) => {
                        self.job_end(&job.owner).await?;
                        return Err(e);
                    }
                },
                "backfill" => match self.uix_backfill(job, &mut st).await {
                    Ok(()) => phase = self.uix_phase(job, "enforcing:prepare").await?,
                    Err(e) if e.code == 1180 => return Err(e),
                    Err(e) => {
                        tracing::warn!(table = %job.space, error = %e.message, "an online index build's backfill failed");
                        st.error = Some(e);
                        self.uix_save(job, &st).await?;
                        phase = self.uix_phase(job, "dropping:prepare").await?;
                    }
                },
                "enforcing" => match self.uix_step(job, "enforcing", &st.steps[1], &step).await? {
                    None => {
                        self.job_end(&job.owner).await?;
                        return Ok(());
                    }
                    Some(e) => {
                        st.error = Some(e);
                        self.uix_save(job, &st).await?;
                        phase = self.uix_phase(job, "dropping:prepare").await?;
                    }
                },
                "dropping" => match self.uix_step(job, "dropping", &st.steps[2], &step).await? {
                    None => {
                        self.job_end(&job.owner).await?;
                        return Err(st.error.take().unwrap_or_else(|| {
                            SqlError::new(1213, "40001", format!("ALTER TABLE of {} was rolled back", job.space))
                        }));
                    }
                    Some(e) => {
                        // (Its drain may wait past its timeout: again.)
                        tracing::warn!(table = %job.space, error = %e.message,
                            "dropping a failed online index build was refused; trying again");
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        phase = self.uix_phase(job, "dropping:prepare").await?;
                    }
                },
                _ => return Err(SqlError::internal(format!("an online index build in phase {phase}"))),
            }
        }
    }

    async fn uix_phase(&self, job: &Job, phase: &str) -> SqlResult<String> {
        self.record_phase(job, phase).await?;
        Ok(phase.to_string())
    }

    /// Store the build's state in its job (sent again while the first shard
    /// refuses it for a transient reason).
    async fn uix_save(&self, job: &Job, st: &UixBuild) -> SqlResult<()> {
        let mut a = Mp::new();
        a.arr(2).str(&job.owner);
        a.0.extend_from_slice(&st.encode());
        let saved = retry_transient(JOB_STEP_RETRY, Duration::from_millis(20), || async {
            let d = self.call(0, "tarn_job_args", &a.0).await?;
            Ok(!crate::mpw::status_is(&d, "gone"))
        })
        .await?;
        if saved {
            return Ok(());
        }
        Err(SqlError::new(
            1180,
            "40003",
            format!(
                "ALTER TABLE of {}: another server took the schema change over; it finishes it",
                job.space
            ),
        ))
    }

    /// One ALTER step of an online build (`stage`), from phase `step`
    /// (prepare, apply or abort), as `run_job_steps` runs an ALTER TABLE's:
    /// None once applied everywhere, or the reason it was aborted.
    async fn uix_step(&self, job: &Job, stage: &str, args: &[u8], step: &str) -> SqlResult<Option<SqlError>> {
        let mut call = Mp::new();
        call.arr(3).str(&job.space);
        call.0.extend_from_slice(args);
        call.uint(job.gen);
        let live: Vec<usize> = self.live_shards().collect();
        let send = |i: usize, proc: &'static str| {
            let call = &call;
            retry_transient(JOB_STEP_RETRY, Duration::from_millis(20), move || async move {
                self.call(i, proc, &call.0)
                    .await
                    .and_then(|d| lua_status(&d).map(|_| d.clone()))
            })
        };
        let mut phase = step.to_string();
        let mut failed: Option<SqlError> = None;
        if phase == "prepare" {
            for r in crate::topology::collect(
                live.iter().map(|&i| send(i, "tarn_alter_build")),
                self.cfg.topology.fanout,
            )
            .await
            {
                if let Err(e) = r {
                    failed.get_or_insert(e);
                }
            }
            let mut applied = false;
            if failed.is_none() {
                for r in crate::topology::collect(
                    live.iter().map(|&i| send(i, "tarn_alter_prepare")),
                    self.cfg.topology.fanout,
                )
                .await
                {
                    match r {
                        Ok(d) => applied |= applied_reply(&d),
                        Err(e) => {
                            failed.get_or_insert(e);
                        }
                    }
                }
            }
            phase = if failed.is_some() && !applied { "abort" } else { "apply" }.to_string();
            self.record_phase(job, &format!("{stage}:{phase}")).await?;
        }
        if phase == "abort" {
            for r in crate::topology::collect(
                live.iter().map(|&i| send(i, "tarn_alter_abort")),
                self.cfg.topology.fanout,
            )
            .await
            {
                if let Err(e) = r {
                    tracing::warn!("ALTER TABLE of {}: {} abort: {}", job.space, stage, e);
                    return Err(SqlError::new(
                        1180,
                        "40003",
                        format!(
                            "ALTER TABLE did not finish on every shard ({}); it completes in the background",
                            e.message
                        ),
                    ));
                }
            }
            return Ok(Some(failed.unwrap_or_else(|| {
                SqlError::new(1213, "40001", format!("ALTER TABLE of {} was rolled back", job.space))
            })));
        }
        for r in crate::topology::collect(
            live.iter().map(|&i| send(i, "tarn_alter_apply")),
            self.cfg.topology.fanout,
        )
        .await
        {
            if let Err(e) = r {
                return Err(SqlError::new(
                    1180,
                    "40003",
                    format!(
                        "ALTER TABLE did not finish on every shard ({}); it completes in the background",
                        e.message
                    ),
                ));
            }
        }
        self.bump_catalog().await?;
        Ok(None)
    }

    /// The backfill: every shard's rows, a page at a time from its cursor.
    async fn uix_backfill(&self, job: &Job, st: &mut UixBuild) -> SqlResult<()> {
        self.reload_catalog().await?;
        let t = self
            .catalog()
            .get(&st.db, &st.table)
            .filter(|t| t.ver == st.ver && t.globals.iter().any(|g| g.building()))
            .ok_or_else(|| {
                SqlError::internal(format!(
                    "{}: its definition is not the building one ({}) the backfill needs",
                    job.space, st.ver
                ))
            })?;
        let mut shards: Vec<(u32, usize)> = self.live_shards().map(|i| (self.shards[i].id, i)).collect();
        shards.sort_unstable();
        for (id, shard) in shards {
            let (mut after, done): (Option<Vec<Value>>, bool) = st.cursors.get(&id).cloned().unwrap_or((None, false));
            if done {
                continue;
            }
            let client = self.shards[shard].client(self).await?;
            loop {
                let mut key = Mp::new();
                let iter = match &after {
                    Some(k) => {
                        key.vals(k);
                        tarn_iproto::Iter::Gt
                    }
                    None => {
                        key.arr(0);
                        tarn_iproto::Iter::All
                    }
                };
                let r = client
                    .select(0, t.space_id, 0, iter, 0, BACKFILL_PAGE, &key.0)
                    .await
                    .map_err(from_iproto)?;
                let mut pks = Vec::new();
                for tup in r.tuples().map_err(crate::mpw::proto)? {
                    pks.push(t.pk_key(&Rd::shared(&tup).row_of(&t)?));
                }
                let last = pks.last().cloned();
                let full = pks.len() == BACKFILL_PAGE as usize;
                if !pks.is_empty() {
                    self.uix_backfill_page(&t, pks).await?;
                }
                after = last.or(after);
                st.cursors.insert(id, (after.clone(), !full));
                self.uix_save(job, st).await?;
                if !full {
                    break;
                }
            }
        }
        Ok(())
    }

    /// A page of the backfill, in a transaction sent again while it fails
    /// for a transient reason.
    /// A page that fails for a transient reason goes on a row at a time, so
    /// that a row its writers keep busy holds up no other.
    async fn uix_backfill_page(&self, t: &Arc<TableDef>, pks: Vec<Vec<Value>>) -> SqlResult<()> {
        match self.uix_backfill_rows(t, pks.clone(), Duration::ZERO).await {
            Err(e) if pks.len() > 1 && backfill_transient(&e) => {
                for pk in pks {
                    self.uix_backfill_rows(t, vec![pk], BACKFILL_RETRY).await?;
                }
                Ok(())
            }
            r => r,
        }
    }

    /// A transaction of the backfill for rows `pks` of `t`, sent again for
    /// up to `retry` while it fails for a transient reason.
    async fn uix_backfill_rows(&self, t: &Arc<TableDef>, pks: Vec<Vec<Value>>, retry: Duration) -> SqlResult<()> {
        let c = self.arc();
        let lock_timeout = self.globals.lock().lock_wait_timeout;
        let (mut since, mut wait) = (None::<Instant>, Duration::from_millis(20));
        loop {
            let s = Session::new(c.clone(), 0);
            let mut txn = Txn::with_mode(&c, true, TxnMode::Serializable, lock_timeout);
            let r = match s.building_backfill(&mut txn, t, pks.clone()).await {
                Ok(()) => txn.commit(&c).await.map(|_| ()),
                Err(e) => {
                    txn.rollback(&c);
                    Err(e)
                }
            };
            match r {
                Ok(()) => return Ok(()),
                Err(e) if backfill_transient(&e) && since.get_or_insert_with(Instant::now).elapsed() < retry => {
                    tracing::debug!(table = %t.name, error = %e.message, "a backfill transaction failed; trying again");
                    tokio::time::sleep(wait).await;
                    wait = (wait * 2).min(Duration::from_secs(2));
                }
                Err(e) => return Err(e),
            }
        }
    }
}

/// A failure of a backfill page that another try may not meet: a conflict
/// with the table's writes, a lock wait, a definition fence raised for a
/// while, an outcome a lost reply left unknown (the next try reads again,
/// and passes rows that have their entries), a shard between leaders.
fn backfill_transient(e: &SqlError) -> bool {
    matches!(e.code, 1180 | 1205 | 1213 | 1412) || crate::lazy::online_transient(e)
}
