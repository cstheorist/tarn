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
}
