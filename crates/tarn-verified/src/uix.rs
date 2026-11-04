//! Maintenance of global unique indexes, verified with Verus.
//!
//! A unique key that does not contain the shard key has a global index: one
//! entry per row whose key has no NULL part, written in the same transaction
//! as the row (`tarn-sql`, catalog and `Session::stage`). A write replaces a
//! row's old version (or none) with a new version (or none, a delete);
//! `index_delta` decides which entry writes keep the index exact.
//!
//! Proven here, with the index modelled as a set of entries and the row's
//! old and new entries as `eo` and `en`:
//!   * applying the delta to any index that holds the old version's entry
//!     (if it has one) leaves the other rows' entries alone and holds the new
//!     version's entry (if it has one) instead -- so an index that matches its
//!     table keeps matching it;
//!   * it writes nothing when the entry does not change (an UPDATE of other
//!     columns touches no other shard), deletes only an entry that exists and
//!     inserts only one that should.
//! That the inserted entry's key is free (uniqueness) is not decided here:
//! the entry's primary key enforces it when the transaction commits.
//!
//! An index being built (issue #292, tla/GlobalUniqueBuild.tla) refuses no
//! write: a row whose key another row's entry holds is recorded as a
//! conflict at that key instead, and when the holder leaves the key, a
//! conflicting row takes the entry over. A key's rows are then its holder
//! and its conflicts. `leave_key` and `join_key` decide what a write does
//! at the key its row leaves and at the one it joins; proven here, with a
//! key's holder (none: no entry) and conflicts as sets of rows:
//!   * a well-formed key (no conflict without a holder, and the holder not
//!     among the conflicts) stays well formed;
//!   * the key's rows lose the leaving row, or gain the joining one, and no
//!     other row: the index holds every row it held, under its key.
//! The index can be published unique once every row joined it and no
//! conflict is left: then each key has one row.

use vstd::prelude::*;

verus! {

#[derive(Clone, Copy, Debug)]
pub struct Delta {
    /// Delete the old version's entry.
    pub delete_old: bool,
    /// Insert the new version's entry.
    pub insert_new: bool,
}

/// The entries one version of a row contributes: its entry if the row
/// exists and its key has no NULL part.
pub open spec fn entries(present: bool, null: bool, e: int) -> Set<int> {
    if present && !null { set![e] } else { Set::empty() }
}

pub open spec fn apply(s: Set<int>, d: Delta, eo: int, en: int) -> Set<int> {
    let s1 = if d.delete_old { s.remove(eo) } else { s };
    if d.insert_new { s1.insert(en) } else { s1 }
}

/// `had_old`/`has_new`: the old/new version exists; `old_null`/`new_null`:
/// its key has a NULL part; `same`: both have an entry and the entries are
/// equal.
pub fn index_delta(had_old: bool, old_null: bool, has_new: bool, new_null: bool, same: bool) -> (d: Delta)
    requires
        same ==> (had_old && !old_null && has_new && !new_null),
    ensures
        forall|rest: Set<int>, eo: int, en: int|
            #![trigger apply(rest.union(entries(had_old, old_null, eo)), d, eo, en)]
            (same <==> (had_old && !old_null && has_new && !new_null && eo == en))
                && !rest.contains(eo) && !rest.contains(en)
            ==> apply(rest.union(entries(had_old, old_null, eo)), d, eo, en)
                =~= rest.union(entries(has_new, new_null, en)),
        same ==> !d.delete_old && !d.insert_new,
        d.delete_old ==> had_old && !old_null,
        d.insert_new ==> has_new && !new_null,
{
    if same {
        Delta { delete_old: false, insert_new: false }
    } else {
        Delta { delete_old: had_old && !old_null, insert_new: has_new && !new_null }
    }
}

/// The rows of one key of a building index: its entry's holder (if it has
/// an entry) and the rows recorded as conflicts at the key.
pub open spec fn rows_of(holder: Option<int>, conflicts: Set<int>) -> Set<int> {
    match holder {
        Some(h) => conflicts.insert(h),
        None => conflicts,
    }
}

/// A key of a building index is well formed: a key without an entry has no
/// conflict, and the holder of its entry is not among its conflicts.
pub open spec fn wf(holder: Option<int>, conflicts: Set<int>) -> bool {
    match holder {
        Some(h) => !conflicts.contains(h),
        None => conflicts =~= Set::empty(),
    }
}

/// What a write does at the key its row leaves (or deletes).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Leave {
    /// Delete the row's conflict at the key, if it has one: another row's
    /// entry holds the key, or none does (the row had no entry or conflict
    /// there yet).
    DropConflict,
    /// Delete the key's entry, the row's: no other row has the key.
    DropEntry,
    /// Give the key's entry to a row recorded as a conflict there, and
    /// delete that row's conflict.
    Promote,
}

pub open spec fn after_leave(l: Leave, holder: Option<int>, conflicts: Set<int>, q: int, r: int) -> (Option<int>, Set<int>) {
    match l {
        Leave::DropConflict => (holder, conflicts.remove(r)),
        Leave::DropEntry => (None, conflicts),
        Leave::Promote => (Some(q), conflicts.remove(q)),
    }
}

/// Row r leaves a key of a building index. `holds`: r's entry holds the
/// key; `others`: a conflict is recorded at the key, and q is one (the row
/// a promotion gives the entry to).
pub fn leave_key(holds: bool, others: bool) -> (l: Leave)
    ensures
        forall|holder: Option<int>, conflicts: Set<int>, q: int, r: int|
            #![trigger after_leave(l, holder, conflicts, q, r)]
            wf(holder, conflicts) && holds == (holder == Some(r))
                && others == !(conflicts =~= Set::empty()) && (holds && others ==> conflicts.contains(q))
            ==> wf(after_leave(l, holder, conflicts, q, r).0, after_leave(l, holder, conflicts, q, r).1)
                && rows_of(after_leave(l, holder, conflicts, q, r).0, after_leave(l, holder, conflicts, q, r).1)
                    =~= rows_of(holder, conflicts).remove(r),
        l == Leave::Promote <==> holds && others,
        l == Leave::DropEntry <==> holds && !others,
{
    let l = if !holds {
        Leave::DropConflict
    } else if others {
        Leave::Promote
    } else {
        Leave::DropEntry
    };
    assert forall|holder: Option<int>, conflicts: Set<int>, q: int, r: int|
        #![trigger after_leave(l, holder, conflicts, q, r)]
        wf(holder, conflicts) && holds == (holder == Some(r))
            && others == !(conflicts =~= Set::empty()) && (holds && others ==> conflicts.contains(q))
        implies wf(after_leave(l, holder, conflicts, q, r).0, after_leave(l, holder, conflicts, q, r).1)
            && rows_of(after_leave(l, holder, conflicts, q, r).0, after_leave(l, holder, conflicts, q, r).1)
                =~= rows_of(holder, conflicts).remove(r) by {
        match l {
            Leave::DropConflict => {
                match holder {
                    Some(h) => {
                        assert(h != r);
                        assert(conflicts.remove(r).insert(h) =~= conflicts.insert(h).remove(r));
                    }
                    None => {
                        assert(conflicts.remove(r) =~= Set::<int>::empty());
                    }
                }
            }
            Leave::DropEntry => {
                assert(holder == Some(r));
                assert(conflicts =~= Set::<int>::empty());
                assert(conflicts.insert(r).remove(r) =~= Set::<int>::empty());
            }
            Leave::Promote => {
                assert(holder == Some(r));
                assert(!conflicts.contains(r));
                assert(conflicts.contains(q));
                assert(q != r);
                assert(conflicts.remove(q).insert(q) =~= conflicts);
                assert(conflicts.insert(r).remove(r) =~= conflicts);
            }
        }
    }
    l
}

/// What a write does at the key its row joins (inserts or moves to).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Join {
    /// Insert the key's entry, for the row: no entry holds it.
    Take,
    /// Nothing: the row's own entry holds the key.
    Keep,
    /// Record the row as a conflict at the key: another row's entry holds
    /// it. (Recorded again, it is the same conflict.)
    Conflict,
}

pub open spec fn after_join(j: Join, holder: Option<int>, conflicts: Set<int>, r: int) -> (Option<int>, Set<int>) {
    match j {
        Join::Take => (Some(r), conflicts),
        Join::Keep => (holder, conflicts),
        Join::Conflict => (holder, conflicts.insert(r)),
    }
}

/// Row r joins a key of a building index. `free`: no entry holds the key;
/// `holds`: r's entry does.
pub fn join_key(free: bool, holds: bool) -> (j: Join)
    requires
        !(free && holds),
    ensures
        forall|holder: Option<int>, conflicts: Set<int>, r: int|
            #![trigger after_join(j, holder, conflicts, r)]
            wf(holder, conflicts) && free == (holder is None) && holds == (holder == Some(r))
            ==> wf(after_join(j, holder, conflicts, r).0, after_join(j, holder, conflicts, r).1)
                && rows_of(after_join(j, holder, conflicts, r).0, after_join(j, holder, conflicts, r).1)
                    =~= rows_of(holder, conflicts).insert(r),
        j == Join::Take <==> free,
        j == Join::Keep <==> holds,
{
    let j = if free {
        Join::Take
    } else if holds {
        Join::Keep
    } else {
        Join::Conflict
    };
    assert forall|holder: Option<int>, conflicts: Set<int>, r: int|
        #![trigger after_join(j, holder, conflicts, r)]
        wf(holder, conflicts) && free == (holder is None) && holds == (holder == Some(r))
        implies wf(after_join(j, holder, conflicts, r).0, after_join(j, holder, conflicts, r).1)
            && rows_of(after_join(j, holder, conflicts, r).0, after_join(j, holder, conflicts, r).1)
                =~= rows_of(holder, conflicts).insert(r) by {
        match j {
            Join::Take => {
                assert(conflicts =~= Set::<int>::empty());
                assert(Set::<int>::empty().insert(r) =~= conflicts.insert(r));
            }
            Join::Keep => {
                assert(conflicts.insert(r).insert(r) =~= conflicts.insert(r));
            }
            Join::Conflict => {
                let h = holder.unwrap();
                assert(h != r);
                assert(conflicts.insert(r).insert(h) =~= conflicts.insert(h).insert(r));
            }
        }
    }
    j
}

} // verus!

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deltas() {
        let d = |a, b, c, e, s| {
            let d = index_delta(a, b, c, e, s);
            (d.delete_old, d.insert_new)
        };
        assert_eq!(d(false, false, true, false, false), (false, true)); // insert
        assert_eq!(d(false, false, true, true, false), (false, false)); // insert, NULL key
        assert_eq!(d(true, false, false, false, false), (true, false)); // delete
        assert_eq!(d(true, false, true, false, true), (false, false)); // key unchanged
        assert_eq!(d(true, false, true, false, false), (true, true)); // key changed
        assert_eq!(d(true, true, true, false, false), (false, true)); // NULL -> value
        assert_eq!(d(true, false, true, true, false), (true, false)); // value -> NULL
    }

    #[test]
    fn building_keys() {
        // the holder leaves: a conflicting row takes the entry, or none is left
        assert_eq!(leave_key(true, true), Leave::Promote);
        assert_eq!(leave_key(true, false), Leave::DropEntry);
        // another row's entry holds the key, or none does
        assert_eq!(leave_key(false, true), Leave::DropConflict);
        assert_eq!(leave_key(false, false), Leave::DropConflict);
        assert_eq!(join_key(true, false), Join::Take);
        assert_eq!(join_key(false, true), Join::Keep);
        assert_eq!(join_key(false, false), Join::Conflict);
    }
}
