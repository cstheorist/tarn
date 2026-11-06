//! Writes to a building global index (issue #292, the `catalog` module
//! comment). Each write decides, at the key its row leaves and at the one it
//! joins, what becomes of the key's entry and conflicts, by the rules of
//! tla/GlobalUniqueRules.tla (`uix::leave_key`, `uix::join_key`, proven in
//! crates/tarn-verified). A decision reads the key's entry and conflicts as
//! the transaction sees them: its own earlier writes over the stored ones
//! (tla/GlobalUniqueWrites.tla, `BugNoOverlay`). It holds at the commit only
//! if what it read is unchanged by then (`Serialized`, `BugKeyUnserialized`):
//! - Every decision that changes a key's conflicts writes the key's entry
//!   too (with the same holder, if it keeps one), and every decision reads
//!   the entry. So two decisions about a key always conflict: a
//!   serializable transaction's read of the entry is validated at its
//!   commit, which covers the key's conflicts too.
//! - READ COMMITTED reads both keys' entries under X locks, taken in key
//!   order before either decision and held to the commit, and the
//!   conflicts under the entry's lock.
//!
//! A decision reads no more conflicts than it needs: whether its own row is
//! recorded (a point read), or which conflict takes the entry over (the
//! first not deleted by the transaction), so that a value many rows share
//! costs each of their writes no more than another.
//!
//! The writes replace or delete: a decision refuses nothing, so no write of
//! the building version fails with 1062. A transaction whose validation
//! fails is retried.

use super::*;
use crate::catalog::GlobalIndex;
use bytes::Bytes;
use std::collections::HashSet;
use tarn_verified::uix::{join_key, leave_key, Join, Leave};

/// The entries the statement's READ COMMITTED locks read, by key.
type LockedEntries = HashMap<Bytes, Option<Row>>;

/// A building index's space dropped meanwhile (the build published or
/// failed on that shard): the catalog changed (1412), and the statement runs
/// again with the new one.
fn gone(e: SqlError) -> SqlError {
    if e.code == 1146 {
        crate::txn::definition_changed()
    } else {
        e
    }
}

impl Session {
    /// The writes a row write makes to building global index `g` of table
    /// `t`: `before` is the old row's entry, `after` the new row's (None:
    /// no row, or a key with a NULL part).
    pub(super) async fn stage_building(
        &self,
        txn: &mut Txn,
        g: &GlobalIndex,
        before: Option<Vec<Value>>,
        after: Option<Vec<Value>>,
    ) -> SqlResult<()> {
        self.stage_building_once(txn, g, before, after).await.map_err(gone)
    }

    async fn stage_building_once(
        &self,
        txn: &mut Txn,
        g: &GlobalIndex,
        before: Option<Vec<Value>>,
        after: Option<Vec<Value>>,
    ) -> SqlResult<()> {
        let k = g.key_len();
        let same = match (&before, &after) {
            (Some(b), Some(a)) => key_bytes(b) == key_bytes(a),
            (None, None) => true,
            _ => false,
        };
        // A write that keeps its row's key and primary key reads and writes
        // no entry.
        if same {
            return Ok(());
        }
        let mut locked = LockedEntries::new();
        if txn.mode == TxnMode::ReadCommitted {
            let mut keys: Vec<(usize, Bytes, Vec<Value>)> = before
                .iter()
                .chain(after.iter())
                .map(|e| {
                    let key = e[..k].to_vec();
                    (self.cluster.shard_of(&key), key_bytes(&key), key)
                })
                .collect();
            keys.sort_by(|a, b| (a.0, &a.1).cmp(&(b.0, &b.1)));
            keys.dedup_by(|a, b| a.1 == b.1);
            for (shard, kb, key) in keys {
                let entry = self.building_lock(txn, g, shard, key).await?;
                locked.insert(kb, entry);
            }
        }
        if let Some(e) = before {
            self.building_leave(txn, g, e, &locked).await?;
        }
        if let Some(e) = after {
            self.building_join(txn, g, e, &locked).await?;
        }
        Ok(())
    }

    /// READ COMMITTED: X-lock the entry of `key` and read it, the latest
    /// committed one (None: no entry).
    async fn building_lock(
        &self,
        txn: &mut Txn,
        g: &GlobalIndex,
        shard: usize,
        key: Vec<Value>,
    ) -> SqlResult<Option<Row>> {
        let x = LockOpts {
            exclusive: true,
            read: true,
            ..Default::default()
        };
        let lk = LockKey {
            table: g.table.clone(),
            index: 0,
            key,
        };
        Ok(match txn.lock(&self.cluster, shard, &[lk], x).await?.pop() {
            Some(Locked::Row(r)) => r,
            _ => None,
        })
    }

    /// The entry of `key`, as this transaction sees it: its own write, the
    /// one its READ COMMITTED lock read, or the one its stream reads.
    async fn building_entry(
        &self,
        txn: &mut Txn,
        g: &GlobalIndex,
        key: &[Value],
        locked: &LockedEntries,
    ) -> SqlResult<Option<Row>> {
        if let Some(b) = txn.buffered(g.table.space_id, key) {
            return Ok(b.cloned());
        }
        let shard = self.cluster.shard_of(key);
        if txn.mode == TxnMode::ReadCommitted {
            return match locked.get(&key_bytes(key)) {
                Some(r) => Ok(r.clone()),
                None => self.building_lock(txn, g, shard, key.to_vec()).await,
            };
        }
        let st = txn.stream(&self.cluster, shard).await?;
        let bucket = Some(self.cluster.bucket_of(key));
        let (rows, _) = select_rows(
            &st.client,
            st.id,
            &g.table,
            0,
            key,
            Iter::Eq,
            Some(1),
            None,
            false,
            st.begin,
            None,
            &Route::in_stream(bucket),
        )
        .await?;
        txn.mark_begun(shard);
        txn.note_point(shard, g.table.space_id, 0, key.to_vec(), bucket)?;
        Ok(rows.into_iter().next())
    }

    /// Stored conflicts (`ct`) at `keys`, whole primary keys (`unique`) or a
    /// key's (the first `limit` there, in the index's order), as this
    /// transaction reads them: through its streams, or (READ COMMITTED,
    /// under the keys' entries' locks) the latest committed ones.
    async fn building_stored(
        &self,
        txn: &mut Txn,
        ct: &TableDef,
        keys: Vec<Vec<Value>>,
        limit: Option<u32>,
        unique: bool,
    ) -> SqlResult<Vec<Row>> {
        let mut mode = if txn.mode == TxnMode::ReadCommitted {
            ReadMode::Committed(txn)
        } else {
            ReadMode::Txn(txn)
        };
        exec::fetch_keys(&self.cluster, &mut mode, ct, ct.pk(), keys, unique, limit).await
    }

    /// Whether row entry `e` is recorded as a conflict at its key, as this
    /// transaction sees it.
    async fn building_recorded(&self, txn: &mut Txn, g: &GlobalIndex, e: &[Value]) -> SqlResult<bool> {
        let ct = g.conflicts.clone().expect("building");
        if let Some(b) = txn.buffered(ct.space_id, e) {
            return Ok(b.is_some());
        }
        Ok(!self
            .building_stored(txn, &ct, vec![e.to_vec()], None, true)
            .await?
            .is_empty())
    }

    /// A conflict recorded at `key` that may take its entry over, as this
    /// transaction sees them (None: none): the first stored one that it has
    /// not written (it reads one more than it deleted there), else one it
    /// recorded.
    async fn building_first_conflict(&self, txn: &mut Txn, g: &GlobalIndex, key: &[Value]) -> SqlResult<Option<Row>> {
        let ct = g.conflicts.clone().expect("building");
        let kb = key_bytes(key);
        let (mut deleted, mut written, mut recorded) = (0u32, HashSet::new(), Vec::new());
        for w in txn
            .buffered_in(ct.space_id)
            .filter(|w| key_bytes(&w.pk[..key.len()]) == kb)
        {
            written.insert(key_bytes(&w.pk));
            match &w.row {
                Some(r) => recorded.push(r.clone()),
                None => deleted += 1,
            }
        }
        let stored = self
            .building_stored(txn, &ct, vec![key.to_vec()], Some(deleted + 1), false)
            .await?;
        if let Some(r) = stored
            .into_iter()
            .find(|r| !written.contains(&key_bytes(&ct.pk_key(r))))
        {
            return Ok(Some(r));
        }
        Ok(recorded.into_iter().min_by_key(|r| key_bytes(r)))
    }

    /// Row entry `e` leaves its key: its conflict there is dropped, or, if
    /// its entry holds the key, a conflicting row takes the entry over, or
    /// the entry goes.
    async fn building_leave(
        &self,
        txn: &mut Txn,
        g: &GlobalIndex,
        e: Vec<Value>,
        locked: &LockedEntries,
    ) -> SqlResult<()> {
        let key = e[..g.key_len()].to_vec();
        // (A key without an entry has no conflicts: the row was not indexed
        // there yet.)
        let Some(holder) = self.building_entry(txn, g, &key, locked).await? else {
            return Ok(());
        };
        let mine = key_bytes(&e);
        if key_bytes(&holder) == mine {
            let first = self.building_first_conflict(txn, g, &key).await?;
            match leave_key(true, first.is_some()) {
                Leave::Promote => {
                    let q = first.expect("a conflict to promote");
                    self.building_write(txn, g, &key, WOp::Delete, q.clone(), true)?;
                    self.building_write(txn, g, &key, WOp::Replace, q, false)?;
                }
                Leave::DropEntry => self.building_write(txn, g, &key, WOp::Delete, e, false)?,
                Leave::DropConflict => unreachable!("the holder leaves"),
            }
            return Ok(());
        }
        let recorded = self.building_recorded(txn, g, &e).await?;
        match leave_key(false, recorded) {
            Leave::DropConflict => {
                // (none recorded: the row was not indexed there yet)
                if recorded {
                    self.building_write(txn, g, &key, WOp::Delete, e, true)?;
                    self.building_write(txn, g, &key, WOp::Replace, holder, false)?;
                }
            }
            Leave::Promote | Leave::DropEntry => unreachable!("a row that does not hold the key"),
        }
        Ok(())
    }

    /// Row entry `e` joins its key: it takes a free key's entry, or keeps
    /// its own, or is recorded as a conflict there.
    async fn building_join(
        &self,
        txn: &mut Txn,
        g: &GlobalIndex,
        e: Vec<Value>,
        locked: &LockedEntries,
    ) -> SqlResult<()> {
        let key = e[..g.key_len()].to_vec();
        let holder = self.building_entry(txn, g, &key, locked).await?;
        let holds = holder.as_ref().is_some_and(|h| key_bytes(h) == key_bytes(&e));
        match join_key(holder.is_none(), holds) {
            Join::Take => self.building_write(txn, g, &key, WOp::Replace, e, false)?,
            Join::Keep => {}
            Join::Conflict => {
                self.building_write(txn, g, &key, WOp::Replace, e, true)?;
                let h = holder.expect("a holder");
                self.building_write(txn, g, &key, WOp::Replace, h, false)?;
            }
        }
        Ok(())
    }

    /// The backfill of `t`'s building global indexes (`cluster::uix_build`):
    /// rows `pks` of it get their entries, or are recorded as conflicts,
    /// unless they already are (a write since the indexes began building
    /// indexed them, or an earlier try of this page). The rows, entries and
    /// conflicts are read through `txn`, whose commit validates them: a row
    /// written since, or a decision about one of the keys, fails it, and the
    /// backfill reads the page again (tla/GlobalUniqueWrites.tla,
    /// BackfillWrite, BugBackfillUnvalidated). A READ COMMITTED `txn` locks
    /// the rows, then the keys' entries in key order, and reads them under
    /// the locks instead, so that the rows cannot change before it commits.
    pub(crate) async fn building_backfill(
        &self,
        txn: &mut Txn,
        t: &Arc<TableDef>,
        pks: Vec<Vec<Value>>,
    ) -> SqlResult<()> {
        self.building_backfill_once(txn, t, pks).await.map_err(gone)
    }

    async fn building_backfill_once(&self, txn: &mut Txn, t: &Arc<TableDef>, pks: Vec<Vec<Value>>) -> SqlResult<()> {
        txn.building.insert(t.space_id);
        let rc = txn.mode == TxnMode::ReadCommitted;
        let rows = if rc {
            let x = LockOpts {
                exclusive: true,
                read: true,
                ..Default::default()
            };
            let mut rows = Vec::with_capacity(pks.len());
            for pk in pks {
                let shard = exec::route_prefix(&self.cluster, t, &t.indexes[0], &pk)
                    .ok_or_else(|| SqlError::internal("a primary key that routes to no shard"))?;
                let lk = LockKey {
                    table: t.clone(),
                    index: 0,
                    key: pk,
                };
                if let Some(Locked::Row(Some(r))) = txn.lock(&self.cluster, shard, &[lk], x).await?.pop() {
                    rows.push(r);
                }
            }
            rows
        } else {
            exec::fetch_keys(&self.cluster, &mut ReadMode::Txn(txn), t, t.pk(), pks, true, None).await?
        };
        for g in t.globals.iter().filter(|g| g.building()) {
            let k = g.key_len();
            let entries: Vec<Vec<Value>> = rows.iter().filter_map(|r| g.entry(t, r)).collect();
            if entries.is_empty() {
                continue;
            }
            let mut keys: Vec<(usize, Bytes, Vec<Value>)> = entries
                .iter()
                .map(|e| {
                    let key = e[..k].to_vec();
                    (self.cluster.shard_of(&key), key_bytes(&key), key)
                })
                .collect();
            keys.sort_by(|a, b| (a.0, &a.1).cmp(&(b.0, &b.1)));
            keys.dedup_by(|a, b| a.1 == b.1);
            let stored = if rc {
                let mut stored = Vec::new();
                for (shard, _, key) in keys {
                    if let Some(h) = self.building_lock(txn, g, shard, key).await? {
                        stored.push(h);
                    }
                }
                stored
            } else {
                let keys = keys.into_iter().map(|(_, _, key)| key).collect();
                exec::fetch_keys(
                    &self.cluster,
                    &mut ReadMode::Txn(txn),
                    &g.table,
                    g.table.pk(),
                    keys,
                    true,
                    None,
                )
                .await?
            };
            let mut holders: HashMap<Bytes, Row> = stored.into_iter().map(|h| (key_bytes(&h[..k]), h)).collect();
            // Those another row's entry holds may be recorded as conflicts.
            let held: Vec<Vec<Value>> = entries
                .iter()
                .filter(|e| {
                    holders
                        .get(&key_bytes(&e[..k]))
                        .is_some_and(|h| key_bytes(h) != key_bytes(e))
                })
                .cloned()
                .collect();
            let recorded: HashSet<Bytes> = if held.is_empty() {
                Default::default()
            } else {
                let ct = g.conflicts.clone().expect("building");
                self.building_stored(txn, &ct, held, None, true)
                    .await?
                    .iter()
                    .map(|c| key_bytes(c))
                    .collect()
            };
            for e in entries {
                let (key, mine) = (e[..k].to_vec(), key_bytes(&e));
                match holders.get(&key_bytes(&key)) {
                    // (indexed already)
                    Some(h) if key_bytes(h) == mine => {}
                    Some(_) if recorded.contains(&mine) => {}
                    Some(h) => {
                        let h = h.clone();
                        match join_key(false, false) {
                            Join::Conflict => {
                                self.building_write(txn, g, &key, WOp::Replace, e, true)?;
                                self.building_write(txn, g, &key, WOp::Replace, h, false)?;
                            }
                            Join::Take | Join::Keep => unreachable!("a held key"),
                        }
                    }
                    None => {
                        self.building_write(txn, g, &key, WOp::Replace, e.clone(), false)?;
                        holders.insert(key_bytes(&key), e);
                    }
                }
            }
        }
        Ok(())
    }

    /// Buffer a write of an entry (or, with `conflict`, of a conflict) of
    /// key `key` of `g`: `row` is the entry, or the deleted one.
    fn building_write(
        &self,
        txn: &mut Txn,
        g: &GlobalIndex,
        key: &[Value],
        op: WOp,
        row: Row,
        conflict: bool,
    ) -> SqlResult<()> {
        let table = if conflict {
            g.conflicts.clone().expect("building")
        } else {
            g.table.clone()
        };
        let pk = if conflict { row.clone() } else { key.to_vec() };
        txn.buffer(BufWrite {
            shard: self.cluster.shard_of(key),
            table,
            op,
            row: (op != WOp::Delete).then_some(row),
            pk,
        })
    }
}
