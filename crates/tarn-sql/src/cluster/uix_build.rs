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
//!    each shard's rows in primary-key order up to the last one it had when
//!    the backfill began there, through transactions whose commit validates
//!    the rows (`Session::building_backfill`). The job stores the shards'
//!    cursors as the pages commit, at most every `BACKFILL_SAVE`.
//! 3. `enforcing`: the table's local unique index is built; behind the
//!    drained fence each shard checks that no conflict of its is left
//!    (tarn_alter_prepare, `a.uix_check`); the index is published
//!    enforcing, its conflicts space dropped. A PREPARE refused for a
//!    transient reason (a lock wait, a shard between leaders) is sent again
//!    for up to `JOB_STEP_RETRY` before the step is aborted. On a cluster
//!    that copies its index builds (`ClusterConfig::index_build`: replicated
//!    shards), the step copies the table into a new space whose indexes
//!    exist before its rows instead (`online_copies`, lua/tarn_engine.lua),
//!    whose PREPARE checks the conflicts behind its fence: a follower that
//!    built the index in place would hold the shard's writes (#267). Its
//!    BUILD and PREPARE go in slices, with a catch-up round, as an ALTER's,
//!    and the job's phase is the copy's decision: `enforcing:apply`, or
//!    `enforcing:abort` and the phases after it (`tarn_engine_decision`).
//! 4. `dropping`, if the third step fails (two rows share a key: 1062, or
//!    another error): the table is published as it was and the index's
//!    spaces dropped; the statement fails with the third step's error. This
//!    step only goes forward: it is never aborted (an ABORT would refuse its
//!    PREPARE for good), and its PREPARE is sent again until every shard has
//!    prepared, by this server for up to `JOB_STEP_RETRY`, then by whichever
//!    server finds the job abandoned.
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

/// How long the backfill sends a page's transaction, or its read of a page,
/// again while it fails for a transient reason, such as a conflict with the
/// table's writes or a shard between leaders.
const BACKFILL_RETRY: Duration = Duration::from_secs(300);
/// Rows per backfill transaction.
const BACKFILL_PAGE: u32 = 64;
/// How often the backfill stores its cursors at most (and once a shard is
/// done): a server that takes the job over passes again the rows of at most
/// this long, which have their entries.
const BACKFILL_SAVE: Duration = Duration::from_secs(1);

/// The state of an online build (its job's arguments).
pub(crate) struct UixBuild {
    pub(crate) db: String,
    pub(crate) table: String,
    /// The `lua_args` of its three steps.
    pub(crate) steps: [Vec<u8>; 3],
    /// The building version (the first step's).
    pub(crate) ver: u64,
    /// The backfill's cursor on each shard, by shard id.
    pub(crate) cursors: BTreeMap<u32, Cursor>,
    /// The error the statement fails with, once the third step failed.
    pub(crate) error: Option<SqlError>,
}

/// Where the backfill is on a shard (tarn_uix_backfill_page). Keys are the
/// shard's primary-index keys as it encodes them, kept as they are.
#[derive(Clone, Default)]
pub(crate) struct Cursor {
    /// The key of the last row passed (None: none yet).
    after: Option<Vec<u8>>,
    /// The shard's last key when the backfill began there (None: not yet
    /// read). The rows after it were written since the index began
    /// building.
    bound: Option<Vec<u8>>,
    done: bool,
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
        for (id, c) in &self.cursors {
            m.arr(4).uint(*id as u64).uint(c.done as u64);
            for k in [&c.after, &c.bound] {
                match k {
                    Some(k) => m.raw(k),
                    None => m.nil(),
                };
            }
        }
        m.str("version").uint(2);
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
        let raw_or_nil = |rd: &mut Rd| -> SqlResult<Option<Vec<u8>>> {
            if rd.is_nil() {
                rd.skip()?;
                Ok(None)
            } else {
                Ok(Some(rd.raw_value()?.to_vec()))
            }
        };
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
                        let n = rd.array()?;
                        let id = rd.value()?.as_i64().unwrap_or(0) as u32;
                        let done = rd.value()?.as_i64() == Some(1);
                        let after = raw_or_nil(&mut rd)?;
                        let bound = if n > 3 { raw_or_nil(&mut rd)? } else { None };
                        for _ in 4..n {
                            rd.skip()?;
                        }
                        cursors.insert(id, Cursor { after, bound, done });
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

/// Sends a refused step again, with a growing pause, until `limit` has
/// passed since the first refusal. (Not since the first try: one that
/// copies the table may take far longer than `limit` before a shard
/// refuses its PREPARE.)
struct StepRetry {
    since: Option<Instant>,
    wait: Duration,
    limit: Duration,
}

impl StepRetry {
    fn new(limit: Duration) -> StepRetry {
        StepRetry {
            since: None,
            wait: Duration::from_millis(200),
            limit,
        }
    }

    /// After a refusal: wait before the next try, or false once `limit`
    /// has passed since the first one.
    async fn wait(&mut self) -> bool {
        if self.since.get_or_insert_with(Instant::now).elapsed() >= self.limit {
            return false;
        }
        tokio::time::sleep(self.wait).await;
        self.wait = (self.wait * 2).min(Duration::from_secs(5));
        true
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
        let copy = match self.online_copies(def).await {
            false => None,
            true => Some(crate::alter::OnlineCopy {
                space_id: self.alloc_space_ids(1).await?,
                // (as alter_table decides for a copy)
                durable: self
                    .replica_not_answering("tarn_engine_durable_capabilities")
                    .await
                    .is_none(),
            }),
        };
        let mut steps = crate::alter::online_steps(def, ch, vers, &ids, copy)?;
        // Without the copy, the second step builds the keys' local indexes
        // in place: on a replicated shard, as copy_index_builds would not,
        // within the limit of a fill behind the fence (#267).
        steps[0].building_limit = copy.is_none()
            && self.cfg.index_build != IndexBuild::InPlace
            && self.live_shards().any(|i| self.shards[i].replicas().len() > 1);
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

    /// Whether the step of an online build of `def`'s new global indexes
    /// that makes them enforcing copies the table into a new space whose
    /// indexes exist before its rows, rather than building their local
    /// indexes in place (`alter::online_steps`): as `copy_index_builds`
    /// decides for an ALTER (`ClusterConfig::index_build`), when the table
    /// can be copied (`alter::online_copy_eligible`) and every replica of
    /// every shard knows such a copy, which checks the indexes' conflicts
    /// behind its fence and keeps the table's other global and lazy indexes.
    async fn online_copies(&self, def: &TableDef) -> bool {
        let wanted = match self.cfg.index_build {
            IndexBuild::InPlace => false,
            IndexBuild::Copy => true,
            IndexBuild::Auto => self.live_shards().any(|i| self.shards[i].replicas().len() > 1),
        };
        if !wanted || !crate::alter::online_copy_eligible(def) {
            return false;
        }
        let mut needed = vec![
            "tarn_uix_building_copy_capabilities",
            "tarn_engine_rebuild_capabilities",
            "tarn_engine_rebuild_aux_capabilities",
            "tarn_engine_rebuild_drops_capabilities",
        ];
        if def.has_lazy() {
            needed.push("tarn_engine_rebuild_lazy_capabilities");
        }
        for func in needed {
            if let Some(addr) = self.replica_not_answering(func).await {
                tracing::info!(
                    table = %def.space_name(),
                    replica = %addr,
                    "the new global unique index's local index is built in place: a replica does not answer {}",
                    func
                );
                return false;
            }
        }
        true
    }

    /// Run (or go on with) an online build, keeping its job alive.
    pub(crate) async fn run_uix_build(&self, job: &Job) -> SqlResult<()> {
        let mut st = UixBuild::parse(&job.args)?;
        let mut phase = match job.phase.as_str() {
            "prepare" => "building:prepare".to_string(),
            p => p.to_string(),
        };
        let rolled_back = || SqlError::new(1213, "40001", format!("ALTER TABLE of {} was rolled back", job.space));
        loop {
            match phase.as_str() {
                "building:prepare" => match self.uix_prepare(job, &st.steps[0]).await? {
                    None => phase = self.uix_phase(job, "building:apply").await?,
                    Some(e) => {
                        self.uix_phase(job, "building:abort").await?;
                        self.uix_abort(job, "building", &st.steps[0]).await?;
                        self.job_end(&job.owner).await?;
                        return Err(e);
                    }
                },
                "building:apply" => {
                    self.uix_apply(job, &st.steps[0]).await?;
                    phase = self.uix_phase(job, "backfill").await?;
                }
                "building:abort" => {
                    self.uix_abort(job, "building", &st.steps[0]).await?;
                    self.job_end(&job.owner).await?;
                    return Err(st.error.take().unwrap_or_else(rolled_back));
                }
                "backfill" => match self.uix_backfill(job, &mut st).await {
                    Ok(()) => phase = self.uix_phase(job, "enforcing:prepare").await?,
                    Err(e) if e.code == 1180 => return Err(e),
                    Err(e) => {
                        tracing::warn!(table = %job.space, error = %e.message, "an online index build's backfill failed");
                        st.error.get_or_insert(e);
                        self.uix_save(job, &st).await?;
                        phase = self.uix_phase(job, "dropping:prepare").await?;
                    }
                },
                "enforcing:prepare" => {
                    // A refusal for a transient reason: PREPARE again (a
                    // shard that refused it undid it; one that prepared takes
                    // it again), as the backfill is done.
                    let mut retry = StepRetry::new(JOB_STEP_RETRY);
                    phase = loop {
                        match self.uix_prepare(job, &st.steps[1]).await? {
                            None => break self.uix_phase(job, "enforcing:apply").await?,
                            Some(e) if step_retried(&e) => {
                                // (still this server's job)
                                self.record_phase(job, "enforcing:prepare").await?;
                                if retry.wait().await {
                                    tracing::info!(table = %job.space, error = %e.message,
                                        "an online index build's enforcing step was refused; sending it again");
                                    continue;
                                }
                                st.error.get_or_insert(e);
                            }
                            Some(e) => {
                                st.error.get_or_insert(e);
                            }
                        }
                        self.uix_save(job, &st).await?;
                        break self.uix_phase(job, "enforcing:abort").await?;
                    };
                }
                "enforcing:apply" => {
                    self.uix_apply(job, &st.steps[1]).await?;
                    self.job_end(&job.owner).await?;
                    return Ok(());
                }
                "enforcing:abort" => {
                    self.uix_abort(job, "enforcing", &st.steps[1]).await?;
                    phase = self.uix_phase(job, "dropping:prepare").await?;
                }
                "dropping:prepare" => {
                    let mut retry = StepRetry::new(JOB_STEP_RETRY);
                    phase = loop {
                        match self.uix_prepare(job, &st.steps[2]).await? {
                            None => break self.uix_phase(job, "dropping:apply").await?,
                            Some(e) => {
                                self.record_phase(job, "dropping:prepare").await?;
                                if retry.wait().await {
                                    tracing::info!(table = %job.space, error = %e.message,
                                        "dropping a failed online index build was refused; sending it again");
                                    continue;
                                }
                                // The change failed; whichever server finds
                                // the job abandoned drops the index.
                                tracing::warn!(table = %job.space, error = %e.message,
                                    "dropping a failed online index build was refused; it is left to finish later");
                                return Err(st.error.take().unwrap_or(e));
                            }
                        }
                    };
                }
                "dropping:apply" => {
                    self.uix_apply(job, &st.steps[2]).await?;
                    self.job_end(&job.owner).await?;
                    return Err(st.error.take().unwrap_or_else(rolled_back));
                }
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

    /// The call of step `args` to `proc` on shard `i`, sent again while it
    /// fails for a transient reason. BUILD and PREPARE take a slice (issue
    /// #184, as in run_job_steps): a step that copies the table returns
    /// ["ok", "more"] once a call has copied that long, and is sent again
    /// until it is done; one in place ignores it.
    async fn uix_send(&self, job: &Job, i: usize, proc: &'static str, args: &[u8]) -> SqlResult<bytes::Bytes> {
        let sliced = matches!(proc, "tarn_alter_build" | "tarn_alter_prepare");
        let mut call = Mp::new();
        call.arr(3 + sliced as usize).str(&job.space);
        call.0.extend_from_slice(args);
        call.uint(job.gen);
        if sliced {
            call.map(1).str("slice").f64(self.cfg.step_slice());
        }
        let call = &call;
        loop {
            let d = retry_transient(JOB_STEP_RETRY, Duration::from_millis(20), move || async move {
                self.call(i, proc, &call.0)
                    .await
                    .and_then(|d| lua_status(&d).map(|_| d.clone()))
            })
            .await?;
            if !more_reply(&d) {
                return Ok(d);
            }
        }
    }

    /// BUILD, then PREPARE, of step `args` on every live shard: None once
    /// every shard has prepared, or one has applied it already (the step
    /// goes forward); else the first refusal.
    async fn uix_prepare(&self, job: &Job, args: &[u8]) -> SqlResult<Option<SqlError>> {
        let live: Vec<usize> = self.live_shards().collect();
        let build = || async {
            let mut first: Option<SqlError> = None;
            for r in crate::topology::collect(
                live.iter().map(|&i| self.uix_send(job, i, "tarn_alter_build", args)),
                self.cfg.topology.fanout,
            )
            .await
            {
                if let Err(e) = r {
                    first.get_or_insert(e);
                }
            }
            first
        };
        let mut failed = build().await;
        // A copy's catch-up round, as in run_job_steps: its BUILD sent again
        // before any shard's PREPARE runs its catch-up passes over the
        // writes since its BUILD ended, while the other shards copied, so
        // that no shard fences its writes for that.
        if failed.is_none() && crate::alter::job_copies(args)? {
            failed = build().await;
        }
        if failed.is_some() {
            return Ok(failed);
        }
        let mut applied = false;
        for r in crate::topology::collect(
            live.iter().map(|&i| self.uix_send(job, i, "tarn_alter_prepare", args)),
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
        Ok(if applied { None } else { failed })
    }

    /// ABORT of step `args` (of `stage`) on every live shard.
    async fn uix_abort(&self, job: &Job, stage: &str, args: &[u8]) -> SqlResult<()> {
        let live: Vec<usize> = self.live_shards().collect();
        for r in crate::topology::collect(
            live.iter().map(|&i| self.uix_send(job, i, "tarn_alter_abort", args)),
            self.cfg.topology.fanout,
        )
        .await
        {
            if let Err(e) = r {
                tracing::warn!("ALTER TABLE of {}: {} abort: {}", job.space, stage, e);
                return Err(unfinished(&e));
            }
        }
        Ok(())
    }

    /// APPLY of step `args` on every live shard, then the catalog's bump.
    async fn uix_apply(&self, job: &Job, args: &[u8]) -> SqlResult<()> {
        let live: Vec<usize> = self.live_shards().collect();
        for r in crate::topology::collect(
            live.iter().map(|&i| self.uix_send(job, i, "tarn_alter_apply", args)),
            self.cfg.topology.fanout,
        )
        .await
        {
            if let Err(e) = r {
                return Err(unfinished(&e));
            }
        }
        self.bump_catalog().await
    }

    /// The backfill: every shard's rows, a page at a time from its cursor.
    async fn uix_backfill(&self, job: &Job, st: &mut UixBuild) -> SqlResult<()> {
        let t = retry_transient(BACKFILL_RETRY, Duration::from_millis(20), || async {
            self.reload_catalog().await?;
            Ok(self
                .catalog()
                .get(&st.db, &st.table)
                .filter(|t| t.ver == st.ver && t.globals.iter().any(|g| g.building())))
        })
        .await?
        .ok_or_else(|| {
            SqlError::internal(format!(
                "{}: its definition is not the building one ({}) the backfill needs",
                job.space, st.ver
            ))
        })?;
        let mut shards: Vec<(u32, usize)> = self.live_shards().map(|i| (self.shards[i].id, i)).collect();
        shards.sort_unstable();
        let mut saved = Instant::now();
        for (id, shard) in shards {
            let mut cur = st.cursors.get(&id).cloned().unwrap_or_default();
            while !cur.done {
                let (rows, next) = self.uix_page(shard, &t, &cur).await?;
                if !rows.is_empty() {
                    self.uix_backfill_page(&t, rows).await?;
                }
                cur = next;
                st.cursors.insert(id, cur.clone());
                if cur.done || saved.elapsed() >= BACKFILL_SAVE {
                    self.uix_save(job, st).await?;
                    saved = Instant::now();
                }
            }
        }
        Ok(())
    }

    /// The next page of the backfill on `shard` (tarn_uix_backfill_page):
    /// its rows' primary keys, and the cursor past it. Sent again while it
    /// fails for a transient reason (it only reads).
    async fn uix_page(&self, shard: usize, t: &TableDef, cur: &Cursor) -> SqlResult<(Vec<Vec<Value>>, Cursor)> {
        let mut a = Mp::new();
        a.arr(4).uint(t.space_id as u64);
        for k in [&cur.after, &cur.bound] {
            match k {
                Some(k) => a.raw(k),
                None => a.nil(),
            };
        }
        a.uint(BACKFILL_PAGE as u64);
        let a = &a;
        let d = retry_transient(BACKFILL_RETRY, Duration::from_millis(20), move || async move {
            self.call(shard, "tarn_uix_backfill_page", &a.0)
                .await
                .and_then(|d| lua_status(&d).map(|_| d.clone()))
        })
        .await?;
        // {OK, rows, last key, done, bound}
        let (_, mut rd) = lua_status(&d)?;
        let mut pks = Vec::new();
        for _ in 0..rd.array()? {
            pks.push(t.pk_key(&Rd::new(rd.raw_value()?).row_of(t)?));
        }
        let raw = |rd: &mut Rd| -> SqlResult<Option<Vec<u8>>> {
            if rd.is_nil() {
                rd.skip()?;
                Ok(None)
            } else {
                Ok(Some(rd.raw_value()?.to_vec()))
            }
        };
        let after = raw(&mut rd)?;
        let done = rd.value()?.as_i64() == Some(1);
        let bound = raw(&mut rd)?;
        Ok((pks, Cursor { after, bound, done }))
    }

    /// A page of the backfill, in a transaction that fails for a transient
    /// reason goes on a row at a time, in transactions that lock their row
    /// and keys instead of validating them, so that a row its writers keep
    /// busy holds up no other, and is not starved by them.
    async fn uix_backfill_page(&self, t: &Arc<TableDef>, pks: Vec<Vec<Value>>) -> SqlResult<()> {
        match self
            .uix_backfill_rows(t, pks.clone(), TxnMode::Serializable, Duration::ZERO)
            .await
        {
            Err(e) if backfill_transient(&e) => {
                for pk in pks {
                    self.uix_backfill_rows(t, vec![pk], TxnMode::ReadCommitted, BACKFILL_RETRY)
                        .await?;
                }
                Ok(())
            }
            r => r,
        }
    }

    /// A transaction of the backfill for rows `pks` of `t`, sent again for
    /// up to `retry` while it fails for a transient reason.
    async fn uix_backfill_rows(
        &self,
        t: &Arc<TableDef>,
        pks: Vec<Vec<Value>>,
        mode: TxnMode,
        retry: Duration,
    ) -> SqlResult<()> {
        let c = self.arc();
        let lock_timeout = self.globals.lock().lock_wait_timeout;
        let (mut since, mut wait) = (None::<Instant>, Duration::from_millis(20));
        loop {
            let s = Session::new(c.clone(), 0);
            let mut txn = Txn::with_mode(&c, true, mode, lock_timeout);
            let page: futures::future::BoxFuture<'_, SqlResult<()>> =
                Box::pin(s.building_backfill(&mut txn, t, pks.clone()));
            let r = match page.await {
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

/// A step that did not finish on every shard: it completes in the
/// background (whichever server finds the job abandoned goes on with it).
fn unfinished(e: &SqlError) -> SqlError {
    SqlError::new(
        1180,
        "40003",
        format!(
            "ALTER TABLE did not finish on every shard ({}); it completes in the background",
            e.message
        ),
    )
}

/// A refusal of the enforcing step's PREPARE that another try may not meet:
/// a lock wait past the step's timeout (its drain), a shard between leaders
/// or down, a transaction conflict.
fn step_retried(e: &SqlError) -> bool {
    e.code == 1205 || crate::lazy::online_transient(e)
}

/// A failure of a backfill page that another try may not meet: a conflict
/// with the table's writes, a lock wait, a definition fence raised for a
/// while, an outcome a lost reply left unknown (the next try reads again,
/// and passes rows that have their entries), a shard between leaders.
fn backfill_transient(e: &SqlError) -> bool {
    matches!(e.code, 1180 | 1205 | 1213 | 1412) || crate::lazy::online_transient(e)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_step_is_sent_again_for_the_limit_after_its_first_refusal() {
        let limit = Duration::from_millis(300);
        let mut retry = StepRetry::new(limit);
        // (a first try that takes longer than the limit, as a copy may)
        tokio::time::sleep(limit * 2).await;
        let first = Instant::now();
        let mut tries = 0;
        while retry.wait().await {
            tries += 1;
        }
        // (pauses of 200 and 400 ms, then the limit has passed)
        assert_eq!(tries, 2);
        assert!(first.elapsed() >= limit, "{:?}", first.elapsed());
    }

    #[test]
    fn the_job_arguments_round_trip() {
        // (A shard's keys are kept as it encodes them.)
        let mut key = Mp::new();
        key.arr(2).uint(7).str("a");
        let st = UixBuild {
            db: "d".into(),
            table: "t".into(),
            steps: [vec![0x90], vec![0x91, 0x01], vec![0xc0]],
            ver: 5,
            cursors: BTreeMap::from([
                (
                    1,
                    Cursor {
                        after: Some(key.0.clone()),
                        bound: Some(key.0.clone()),
                        done: false,
                    },
                ),
                (
                    2,
                    Cursor {
                        after: None,
                        bound: None,
                        done: true,
                    },
                ),
            ]),
            error: Some(SqlError::new(1062, "23000", "Duplicate entry '7' for key 'u'")),
        };
        let back = UixBuild::parse(&st.encode()).unwrap();
        assert_eq!((back.db.as_str(), back.table.as_str(), back.ver), ("d", "t", 5));
        assert_eq!(back.steps, st.steps);
        let c = &back.cursors[&1];
        assert_eq!(
            (c.after.as_deref(), c.bound.as_deref(), c.done),
            (Some(&key.0[..]), Some(&key.0[..]), false)
        );
        let c = &back.cursors[&2];
        assert!(c.after.is_none() && c.bound.is_none() && c.done);
        let e = back.error.unwrap();
        assert_eq!((e.code, e.message.as_str()), (1062, "Duplicate entry '7' for key 'u'"));
    }
}
