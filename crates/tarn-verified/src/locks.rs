//! The lock checks of the native storage procedures (`tarn-storage`, issue
//! #28), verified with Verus.
//!
//! The Lua module's lock index holds, for each key, the transactions with a
//! lock on it and their modes (`LK[sid][iid][kbin] = {[txid] = 'R' | 'W'}`
//! in lua/tarn.lua), and for each key the transactions with a row lock on it
//! (`PL`). The native procedures check the keys they read and write against
//! a copy of it, which counts instead: for each key, the transactions that
//! hold it in each mode (`Counts`) and those with a row lock on it, and for
//! each index the W locks on its keys. The Lua module tells the copy of each
//! change it makes: a transaction's mode on a key went from `from` to `to`
//! (`NONE` when it holds no lock there), a row lock was taken or released.
//!
//! Modelled here: the holders of a key as a map from transactions to modes,
//! the locks of an index as a map from (key, transaction) to mode, the
//! holders of a row lock as a set; `census` counts those of a mode. Proven
//! here, for every change the Lua module makes:
//!   * `step` keeps a key's counts the census of its holders, `step_w` an
//!     index's W count the census of its W locks, and `row_step` a key's row
//!     locks the number of their holders;
//!   * the checks are sound: with counts that are the census, a key that
//!     `read_free` passes has no W lock, so none that `point_w_locked` finds,
//!     whoever reads; a primary key that `primary_free` passes, and the key
//!     of another index that `secondary_free` passes, no lock that
//!     `write_conflict` finds, whoever writes; an index that `index_free`
//!     passes, no W lock on any of its keys, so none that `range_w_locked`
//!     finds, whatever the range; a key that `row_free` passes, no row lock
//!     that `write_plocked` finds.
//! The checks count every holder, the reader's or writer's own locks too:
//! the native procedures serve transactions that hold none.
//! Not proven: that the Lua module tells the copy of every change (its test
//! switch, cfg.check_locks, compares the two after each), and that a key is
//! looked up by the bytes the Lua module's `kbin` gives it.

use vstd::prelude::*;

verus! {

/// The modes of a lock, as the Lua module tells them: none, R, W.
pub const NONE: u8 = 0;

pub const R: u8 = 1;

pub const W: u8 = 2;

/// The mode of `t` in `h` (NONE: no lock).
pub open spec fn mode_of<K>(h: Map<K, u8>, t: K) -> u8 {
    if h.contains_key(t) {
        h[t]
    } else {
        NONE
    }
}

/// `h` once the mode of `t` became `to`.
pub open spec fn update<K>(h: Map<K, u8>, t: K, to: u8) -> Map<K, u8> {
    if to == NONE {
        h.remove(t)
    } else {
        h.insert(t, to)
    }
}

/// Every lock in `h` is an R or a W lock.
pub open spec fn holds<K>(h: Map<K, u8>) -> bool {
    forall|t: K| #[trigger] h.contains_key(t) ==> h[t] == R || h[t] == W
}

/// The locks of mode `m` in `h`.
pub open spec fn of_mode<K>(h: Map<K, u8>, m: u8) -> Set<K> {
    h.dom().filter(|t: K| h[t] == m)
}

/// How many locks of mode `m` there are in `h`.
pub open spec fn census<K>(h: Map<K, u8>, m: u8) -> nat {
    of_mode(h, m).len()
}

/// A change of one lock's mode moves the census of a mode by one at most:
/// down if it had that mode, up if it has it now.
pub proof fn lemma_census_update<K>(h: Map<K, u8>, t: K, to: u8, m: u8)
    requires
        m != NONE,
    ensures
        census(update(h, t, to), m) + (if mode_of(h, t) == m { 1int } else { 0 }) == census(h, m) + (if to
            == m {
            1int
        } else {
            0
        }),
{
    let h2 = update(h, t, to);
    let (s, s2) = (of_mode(h, m), of_mode(h2, m));
    // (the locks of the others are the same)
    assert(s.remove(t) =~= s2.remove(t));
    assert(s.len() == s.remove(t).len() + (if s.contains(t) { 1int } else { 0 }));
    assert(s2.len() == s2.remove(t).len() + (if s2.contains(t) { 1int } else { 0 }));
    assert(s.contains(t) == (mode_of(h, t) == m));
    assert(s2.contains(t) == (to == m));
}

/// A lock of mode `m` makes the census of `m` positive.
pub proof fn lemma_census_positive<K>(h: Map<K, u8>, t: K, m: u8)
    requires
        h.contains_key(t),
        h[t] == m,
    ensures
        census(h, m) > 0,
{
    assert(of_mode(h, m).contains(t));
}

/// The counts of a key: the transactions that hold an R lock on it, and a W
/// lock.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Counts {
    pub r: u64,
    pub w: u64,
}

/// The counts are the census of the holders `h` of a key.
pub open spec fn census_of(c: Counts, h: Map<int, u8>) -> bool {
    holds(h) && c.r == census(h, R) && c.w == census(h, W)
}

/// The counts once a transaction's mode on the key went from `from` to `to`
/// (lk_point, lk_release): the census of the holders after the change, if
/// they were before it. (No count reaches u64::MAX: there are fewer
/// transactions.)
pub fn step(c: Counts, from: u8, to: u8) -> (out: Counts)
    ensures
        forall|h: Map<int, u8>, t: int|
            census_of(c, h) && mode_of(h, t) == from && (to == NONE || to == R || to == W) && c.r < u64::MAX
                && c.w < u64::MAX ==> #[trigger] census_of(out, update(h, t, to)),
{
    let r = moved(c.r, from == R, to == R);
    let w = moved(c.w, from == W, to == W);
    let out = Counts { r, w };
    proof {
        assert forall|h: Map<int, u8>, t: int|
            census_of(c, h) && mode_of(h, t) == from && (to == NONE || to == R || to == W) && c.r < u64::MAX
                && c.w < u64::MAX implies #[trigger] census_of(out, update(h, t, to)) by {
            lemma_census_update(h, t, to, R);
            lemma_census_update(h, t, to, W);
        }
    }
    out
}

/// A count of a mode, once a lock that had it (`before`) or not has it
/// (`after`) or not.
fn moved(n: u64, before: bool, after: bool) -> (out: u64)
    ensures
        before && !after ==> out == (if n > 0 { n - 1 } else { 0 }),
        !before && after ==> out == (if n < u64::MAX { (n + 1) as u64 } else { n }),
        before == after ==> out == n,
{
    if before && !after {
        if n > 0 {
            n - 1
        } else {
            0
        }
    } else if !before && after && n < u64::MAX {
        n + 1
    } else {
        n
    }
}

/// Another transaction than `txid` holds a W lock (point_w_locked).
pub open spec fn w_locked(h: Map<int, u8>, txid: int) -> bool {
    exists|t: int| t != txid && #[trigger] h.contains_key(t) && h[t] == W
}

/// Another transaction than `txid` holds a lock, of any mode when `any`,
/// else an R lock (write_conflict: on the primary key, `any`; on the key of
/// another index, `any` when the new row claims it).
pub open spec fn locked(h: Map<int, u8>, txid: int, any: bool) -> bool {
    exists|t: int| t != txid && #[trigger] h.contains_key(t) && (any || h[t] == R)
}

/// A read of a key may go on: no W lock on it (select_locked of a point
/// read, reads_w_locked of a read set's point).
pub fn read_free(c: Counts) -> (ok: bool)
    ensures
        forall|h: Map<int, u8>, txid: int| census_of(c, h) && ok ==> !#[trigger] w_locked(h, txid),
{
    let ok = c.w == 0;
    proof {
        assert forall|h: Map<int, u8>, txid: int| census_of(c, h) && ok implies !#[trigger] w_locked(
            h,
            txid,
        ) by {
            assert forall|t: int| t != txid && #[trigger] h.contains_key(t) implies h[t] != W by {
                if h[t] == W {
                    lemma_census_positive(h, t, W);
                }
            }
        }
    }
    ok
}

/// A write of the row with this primary key may go on: no lock on it.
pub fn primary_free(c: Counts) -> (ok: bool)
    ensures
        forall|h: Map<int, u8>, txid: int| census_of(c, h) && ok ==> !#[trigger] locked(h, txid, true),
{
    let ok = c.r == 0 && c.w == 0;
    proof {
        assert forall|h: Map<int, u8>, txid: int| census_of(c, h) && ok implies !#[trigger] locked(
            h,
            txid,
            true,
        ) by {
            assert forall|t: int| t != txid implies !#[trigger] h.contains_key(t) by {
                if h.contains_key(t) {
                    lemma_census_positive(h, t, h[t]);
                }
            }
        }
    }
    ok
}

/// A write of a row that had or takes this key of another index may go on:
/// no R lock on it, and no lock at all when the row `claims` it (a key of a
/// unique index, none of its parts NULL, that the new row takes).
pub fn secondary_free(c: Counts, claims: bool) -> (ok: bool)
    ensures
        forall|h: Map<int, u8>, txid: int| census_of(c, h) && ok ==> !#[trigger] locked(h, txid, claims),
{
    let ok = c.r == 0 && (!claims || c.w == 0);
    proof {
        assert forall|h: Map<int, u8>, txid: int| census_of(c, h) && ok implies !#[trigger] locked(
            h,
            txid,
            claims,
        ) by {
            assert forall|t: int| t != txid && #[trigger] h.contains_key(t) implies !(claims || h[t] == R) by {
                if claims || h[t] == R {
                    lemma_census_positive(h, t, h[t]);
                }
            }
        }
    }
    ok
}

/// The W locks of an index once a transaction's mode on one of its keys went
/// from `from` to `to`: the census of W in the index's locks, (key,
/// transaction) -> mode, after the change, if it was before it.
pub fn step_w(n: u64, from: u8, to: u8) -> (out: u64)
    ensures
        forall|h: Map<(int, int), u8>, p: (int, int)|
            holds(h) && n == census(h, W) && mode_of(h, p) == from && (to == NONE || to == R || to == W) && n
                < u64::MAX ==> #[trigger] census(update(h, p, to), W) == out,
{
    let out = moved(n, from == W, to == W);
    proof {
        assert forall|h: Map<(int, int), u8>, p: (int, int)|
            holds(h) && n == census(h, W) && mode_of(h, p) == from && (to == NONE || to == R || to == W) && n
                < u64::MAX implies #[trigger] census(update(h, p, to), W) == out by {
            lemma_census_update(h, p, to, W);
        }
    }
    out
}

/// Another transaction than `txid` holds a W lock on some key of the index
/// (range_w_locked, whatever the range).
pub open spec fn index_w_locked(h: Map<(int, int), u8>, txid: int) -> bool {
    exists|k: int, t: int| t != txid && #[trigger] h.contains_key((k, t)) && h[(k, t)] == W
}

/// A read of a range of an index may go on: no W lock on any of its keys.
pub fn index_free(n: u64) -> (ok: bool)
    ensures
        forall|h: Map<(int, int), u8>, txid: int|
            holds(h) && n == census(h, W) && ok ==> !#[trigger] index_w_locked(h, txid),
{
    let ok = n == 0;
    proof {
        assert forall|h: Map<(int, int), u8>, txid: int|
            holds(h) && n == census(h, W) && ok implies !#[trigger] index_w_locked(h, txid) by {
            assert forall|k: int, t: int| t != txid && #[trigger] h.contains_key((k, t)) implies h[(k, t)] != W by {
                if h[(k, t)] == W {
                    lemma_census_positive(h, (k, t), W);
                }
            }
        }
    }
    ok
}

/// The row locks on a key once a transaction took one (`take`: it held none
/// there, pl_hold) or released it (it held one, pl_release_key,
/// pl_release_all): the number of holders after, if it was before.
pub fn row_step(n: u64, take: bool) -> (out: u64)
    ensures
        take ==> forall|s: Set<int>, t: int|
            n == s.len() && !s.contains(t) && n < u64::MAX ==> #[trigger] s.insert(t).len() == out,
        !take ==> forall|s: Set<int>, t: int| n == s.len() && s.contains(t) ==> #[trigger] s.remove(t).len() == out,
{
    moved(n, !take, take)
}

/// Another transaction than `txid` holds a row lock (plock_holder).
pub open spec fn row_locked(s: Set<int>, txid: int) -> bool {
    exists|t: int| t != txid && #[trigger] s.contains(t)
}

/// A write that touches this key may go on: no row lock on it
/// (write_plocked).
pub fn row_free(n: u64) -> (ok: bool)
    ensures
        forall|s: Set<int>, txid: int| n == s.len() && ok ==> !#[trigger] row_locked(s, txid),
{
    let ok = n == 0;
    proof {
        assert forall|s: Set<int>, txid: int| n == s.len() && ok implies !#[trigger] row_locked(s, txid) by {
            assert forall|t: int| t != txid implies !#[trigger] s.contains(t) by {
                if s.contains(t) {
                    assert(s.len() != 0);
                }
            }
        }
    }
    ok
}

} // verus!

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn steps() {
        let c = Counts { r: 0, w: 0 };
        let c = step(c, NONE, R);
        assert_eq!(c, Counts { r: 1, w: 0 });
        let c = step(c, R, W);
        assert_eq!(c, Counts { r: 0, w: 1 });
        assert_eq!(step(c, W, W), c);
        assert_eq!(step(c, W, NONE), Counts { r: 0, w: 0 });
        assert_eq!(
            step(Counts { r: 0, w: 0 }, R, NONE),
            Counts { r: 0, w: 0 },
            "(never below 0)"
        );
        assert_eq!(step_w(3, R, W), 4);
        assert_eq!(step_w(3, W, NONE), 2);
        assert_eq!(step_w(3, NONE, R), 3);
        assert_eq!((row_step(1, true), row_step(1, false)), (2, 0));
    }

    #[test]
    fn checks() {
        let (none, r, w) = (Counts { r: 0, w: 0 }, Counts { r: 1, w: 0 }, Counts { r: 0, w: 1 });
        assert!(read_free(none) && read_free(r) && !read_free(w));
        assert!(primary_free(none) && !primary_free(r) && !primary_free(w));
        assert!(secondary_free(none, true) && !secondary_free(r, false) && secondary_free(w, false));
        assert!(!secondary_free(w, true));
        assert!(index_free(0) && !index_free(1) && row_free(0) && !row_free(2));
    }
}
