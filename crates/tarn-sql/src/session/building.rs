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
//! The writes replace or delete: a decision refuses nothing, so no write of
//! the building version fails with 1062. A transaction whose validation
//! fails is retried.

use super::*;
use crate::catalog::GlobalIndex;
use bytes::Bytes;
use tarn_verified::uix::{join_key, leave_key, Join, Leave};

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
            for (shard, _, key) in keys {
                self.building_lock(txn, g, shard, key).await?;
            }
        }
        if let Some(e) = before {
            self.building_leave(txn, g, e).await?;
        }
        if let Some(e) = after {
            self.building_join(txn, g, e).await?;
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

    /// The entry of `key` and, with `conflicts`, the conflicts recorded
    /// there (in key order), as this transaction sees them.
    async fn building_key(
        &self,
        txn: &mut Txn,
        g: &GlobalIndex,
        key: &[Value],
        conflicts: bool,
    ) -> SqlResult<(Option<Row>, Vec<Row>)> {
        let shard = self.cluster.shard_of(key);
        let entry = match txn.buffered(g.table.space_id, key) {
            Some(b) => b.cloned(),
            None if txn.mode == TxnMode::ReadCommitted => self.building_lock(txn, g, shard, key.to_vec()).await?,
            None => {
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
                rows.into_iter().next()
            }
        };
        // (A key without an entry has no conflicts.)
        let Some(ct) = g.conflicts.as_ref().filter(|_| conflicts && entry.is_some()) else {
            return Ok((entry, Vec::new()));
        };
        let stored = {
            let mut mode = if txn.mode == TxnMode::ReadCommitted {
                ReadMode::Committed(txn)
            } else {
                ReadMode::Txn(txn)
            };
            exec::fetch_keys(&self.cluster, &mut mode, ct, ct.pk(), vec![key.to_vec()], false, None).await?
        };
        let kb = key_bytes(key);
        let mut found: Vec<(Bytes, Row)> = txn
            .overlay(ct, stored)
            .into_iter()
            .filter(|r| key_bytes(&r[..key.len()]) == kb)
            .map(|r| (key_bytes(&r), r))
            .collect();
        found.sort_by(|a, b| a.0.cmp(&b.0));
        found.dedup_by(|a, b| a.0 == b.0);
        Ok((entry, found.into_iter().map(|(_, r)| r).collect()))
    }

    /// Row entry `e` leaves its key: its conflict there is dropped, or, if
    /// its entry holds the key, a conflicting row takes the entry over, or
    /// the entry goes.
    async fn building_leave(&self, txn: &mut Txn, g: &GlobalIndex, e: Vec<Value>) -> SqlResult<()> {
        let key = e[..g.key_len()].to_vec();
        let (holder, conflicts) = self.building_key(txn, g, &key, true).await?;
        let mine = key_bytes(&e);
        let holds = holder.as_ref().is_some_and(|h| key_bytes(h) == mine);
        match leave_key(holds, !conflicts.is_empty()) {
            Leave::DropConflict => {
                // (none recorded: the row was not indexed there yet)
                if conflicts.iter().any(|c| key_bytes(c) == mine) {
                    self.building_write(txn, g, &key, WOp::Delete, e, true)?;
                    if let Some(h) = holder {
                        self.building_write(txn, g, &key, WOp::Replace, h, false)?;
                    }
                }
            }
            Leave::DropEntry => self.building_write(txn, g, &key, WOp::Delete, e, false)?,
            Leave::Promote => {
                let q = conflicts.into_iter().next().expect("a conflict to promote");
                self.building_write(txn, g, &key, WOp::Delete, q.clone(), true)?;
                self.building_write(txn, g, &key, WOp::Replace, q, false)?;
            }
        }
        Ok(())
    }

    /// Row entry `e` joins its key: it takes a free key's entry, or keeps
    /// its own, or is recorded as a conflict there.
    async fn building_join(&self, txn: &mut Txn, g: &GlobalIndex, e: Vec<Value>) -> SqlResult<()> {
        let key = e[..g.key_len()].to_vec();
        let (holder, _) = self.building_key(txn, g, &key, false).await?;
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
    /// BackfillWrite, BugBackfillUnvalidated).
    pub(crate) async fn building_backfill(
        &self,
        txn: &mut Txn,
        t: &Arc<TableDef>,
        pks: Vec<Vec<Value>>,
    ) -> SqlResult<()> {
        txn.building.insert(t.space_id);
        let rows = exec::fetch_keys(&self.cluster, &mut ReadMode::Txn(txn), t, t.pk(), pks, true, None).await?;
        for g in t.globals.iter().filter(|g| g.building()) {
            let k = g.key_len();
            let entries: Vec<Vec<Value>> = rows.iter().filter_map(|r| g.entry(t, r)).collect();
            if entries.is_empty() {
                continue;
            }
            let mut keys: Vec<Vec<Value>> = entries.iter().map(|e| e[..k].to_vec()).collect();
            keys.sort_by_cached_key(|key| key_bytes(key));
            keys.dedup_by(|a, b| key_bytes(a) == key_bytes(b));
            let stored = exec::fetch_keys(
                &self.cluster,
                &mut ReadMode::Txn(txn),
                &g.table,
                g.table.pk(),
                keys,
                true,
                None,
            )
            .await?;
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
            let recorded: std::collections::HashSet<Bytes> = if held.is_empty() {
                Default::default()
            } else {
                let ct = g.conflicts.as_ref().expect("building");
                exec::fetch_keys(&self.cluster, &mut ReadMode::Txn(txn), ct, ct.pk(), held, true, None)
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
