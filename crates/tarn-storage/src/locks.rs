//! A copy of the lock index of the Lua module, for the native procedures'
//! checks (issue #28, part 6): the point locks of prepared transactions
//! (LK), their range locks (RL) and the row locks (PL) of lua/tarn.lua
//! ("Volatile lock index", "Row locks").
//!
//! The Lua module owns the index. In the step that changes it, it calls the
//! functions below, through the pointers it finds in the state
//! (`State::lk_point` ...): between two steps the copy holds what the index
//! holds. A native procedure then checks the keys it reads and writes as the
//! Lua procedure would, instead of handing back every step on a table that
//! some lock is on.
//!
//! The copy counts, for each key, the transactions that hold a lock on it,
//! by mode; it does not know which transactions. The native procedures serve
//! only the transactions that hold no lock here (they never prepared here,
//! and those with row locks go to Lua), so every lock is another
//! transaction's: a count above zero hands the step back where the Lua
//! procedure would have found the lock. The counts change, and the checks
//! read them, through `tarn_verified::locks`, which proves both: the counts
//! stay those of the holders through each change, and a key a check passes
//! has no lock that the Lua check would find.
//!
//! A key is the Lua module's `kbin`: the MessagePack of the key, as
//! `msgpack.encode` writes it (`write::lua_array`), after its space and
//! index (4 bytes each, big-endian).

use crate::mp::{Rd, Wr};
use crate::write::{lua_array, lua_value};
use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashMap;
use tarn_verified::locks as v;
use tarn_verified::locks::Counts;

/// The modes of a point lock, as the Lua module passes them: none, R, W.
pub use tarn_verified::locks::{NONE, R, W};

const ZERO: Counts = Counts { r: 0, w: 0 };

/// A range lock of an index: its bounds as the Lua module encodes them
/// (`msgpack.encode` of a key; None: no bound), and how many transactions
/// hold one with these bounds. (Whether a bound is included is not kept:
/// the checks take every bound as included, which finds a lock wherever
/// the Lua checks do.)
struct RangeLock {
    lo: Option<Box<[u8]>>,
    hi: Option<Box<[u8]>>,
    n: u64,
}

/// Keys of an index (`kbin`), each with a count.
type Keys = HashMap<Box<[u8]>, u64>;

#[derive(Default)]
struct Copy {
    /// LK: for each key, the transactions with an R lock on it, and with a
    /// W lock.
    points: HashMap<Box<[u8]>, Counts>,
    /// The W point locks on each index, by (space, index).
    w_index: HashMap<(u32, u32), u64>,
    /// The keys of each index with W point locks on them, and how many.
    w_keys: HashMap<(u32, u32), Keys>,
    /// RL: the range locks on each space (on any of its indexes).
    ranges: HashMap<u32, u64>,
    /// RL: the range locks of each index.
    rlocks: HashMap<(u32, u32), Vec<RangeLock>>,
    /// PL: for each key, the transactions that hold a row lock on it.
    rows: HashMap<Box<[u8]>, u64>,
    /// (the key being looked up)
    buf: Vec<u8>,
}

thread_local! {
    static COPY: RefCell<Copy> = RefCell::new(Copy::default());
}

fn compose(buf: &mut Vec<u8>, sid: u32, iid: u32, kb: &[u8]) {
    buf.clear();
    buf.extend_from_slice(&sid.to_be_bytes());
    buf.extend_from_slice(&iid.to_be_bytes());
    buf.extend_from_slice(kb);
}

/// Set the count of `k` (a count of zero goes).
fn set<K: std::hash::Hash + Eq>(m: &mut HashMap<K, u64>, k: K, n: u64) {
    if n == 0 {
        m.remove(&k);
    } else {
        m.insert(k, n);
    }
}

/// The same, for a key of the copy (in `buf`; `zero`: the counts are),
/// copied only when the copy has no entry for it.
fn set_key<V>(m: &mut HashMap<Box<[u8]>, V>, buf: &[u8], n: V, zero: bool) {
    if zero {
        m.remove(buf);
    } else if let Some(e) = m.get_mut(buf) {
        *e = n;
    } else {
        m.insert(buf.into(), n);
    }
}

impl Copy {
    fn point(&mut self, sid: u32, iid: u32, kb: &[u8], from: u8, to: u8) {
        if from == to {
            return;
        }
        compose(&mut self.buf, sid, iid, kb);
        let c = v::step(self.points.get(&self.buf[..]).copied().unwrap_or(ZERO), from, to);
        set_key(&mut self.points, &self.buf, c, c == ZERO);
        let n = self.w_index.get(&(sid, iid)).copied().unwrap_or(0);
        set(&mut self.w_index, (sid, iid), v::step_w(n, from, to));
        if from == W || to == W {
            let keys = self.w_keys.entry((sid, iid)).or_default();
            set_key(keys, kb, c.w, c.w == 0);
            if keys.is_empty() {
                self.w_keys.remove(&(sid, iid));
            }
        }
    }

    fn range(&mut self, sid: u32, iid: u32, lo: Option<&[u8]>, hi: Option<&[u8]>, d: i32) {
        let n = self
            .ranges
            .get(&sid)
            .copied()
            .unwrap_or(0)
            .saturating_add_signed(d as i64);
        set(&mut self.ranges, sid, n);
        let locks = self.rlocks.entry((sid, iid)).or_default();
        match locks
            .iter()
            .position(|r| r.lo.as_deref() == lo && r.hi.as_deref() == hi)
        {
            Some(i) => {
                let r = &mut locks[i];
                r.n = r.n.saturating_add_signed(d as i64);
                if r.n == 0 {
                    locks.swap_remove(i);
                }
            }
            None if d > 0 => locks.push(RangeLock {
                lo: lo.map(Into::into),
                hi: hi.map(Into::into),
                n: d as u64,
            }),
            None => {}
        }
        if locks.is_empty() {
            self.rlocks.remove(&(sid, iid));
        }
    }

    fn row(&mut self, sid: u32, iid: u32, kb: &[u8], d: i32) {
        compose(&mut self.buf, sid, iid, kb);
        let mut n = self.rows.get(&self.buf[..]).copied().unwrap_or(0);
        for _ in 0..d.unsigned_abs() {
            n = v::row_step(n, d > 0);
        }
        set_key(&mut self.rows, &self.buf, n, n == 0);
    }

    fn get(&mut self, sid: u32, iid: u32, kb: &[u8]) -> (Counts, u64) {
        compose(&mut self.buf, sid, iid, kb);
        let p = self.points.get(&self.buf[..]).copied().unwrap_or(ZERO);
        (p, self.rows.get(&self.buf[..]).copied().unwrap_or(0))
    }
}

// --- called by the Lua module ----------------------------------------------

/// A transaction's point lock on key `kb` of index `iid` of space `sid` went
/// from mode `from` to mode `to` (NONE, R, W: lk_point, lk_release).
///
/// # Safety
/// `kb` points to `len` bytes.
pub unsafe extern "C" fn lk_point(sid: u32, iid: u32, kb: *const u8, len: usize, from: u8, to: u8) {
    let kb = std::slice::from_raw_parts(kb, len);
    COPY.with(|c| c.borrow_mut().point(sid, iid, kb, from, to));
}

/// A bound passed by the Lua module: `len` bytes at `b`, or none (null).
unsafe fn bound<'a>(b: *const u8, len: usize) -> Option<&'a [u8]> {
    (!b.is_null()).then(|| std::slice::from_raw_parts(b, len))
}

/// `d` more range locks on index `iid` of space `sid`, with these bounds
/// (lk_range, lk_release).
///
/// # Safety
/// `lo` (null: none) points to `lo_len` bytes, `hi` to `hi_len`.
pub unsafe extern "C" fn lk_range(
    sid: u32,
    iid: u32,
    lo: *const u8,
    lo_len: usize,
    hi: *const u8,
    hi_len: usize,
    d: i32,
) {
    let (lo, hi) = (bound(lo, lo_len), bound(hi, hi_len));
    COPY.with(|c| c.borrow_mut().range(sid, iid, lo, hi, d));
}

/// `d` more transactions hold a row lock on key `kb` of index `iid` of space
/// `sid` (pl_hold: 1, pl_release_key and pl_release_all: -1).
///
/// # Safety
/// `kb` points to `len` bytes.
pub unsafe extern "C" fn pl_key(sid: u32, iid: u32, kb: *const u8, len: usize, d: i32) {
    let kb = std::slice::from_raw_parts(kb, len);
    COPY.with(|c| c.borrow_mut().row(sid, iid, kb, d));
}

/// Forget the point and range locks (`which` & 1: the Lua module makes its
/// index again, lk_reload), or the row locks (& 2: pl_fresh).
pub extern "C" fn locks_clear(which: u8) {
    COPY.with(|c| {
        let mut c = c.borrow_mut();
        if which & 1 != 0 {
            c.points.clear();
            c.w_index.clear();
            c.w_keys.clear();
            c.ranges.clear();
            c.rlocks.clear();
        }
        if which & 2 != 0 {
            c.rows.clear();
        }
    });
}

/// (The Lua module's check of the copy, cfg.check_locks.) The R and W point
/// locks on key `kb` of index `iid` of space `sid`, and the transactions
/// with a row lock on it: `out[0..3]`.
///
/// # Safety
/// `kb` points to `len` bytes, `out` to 3 counts.
pub unsafe extern "C" fn locks_key(sid: u32, iid: u32, kb: *const u8, len: usize, out: *mut u32) {
    let kb = std::slice::from_raw_parts(kb, len);
    let (p, rows) = COPY.with(|c| c.borrow_mut().get(sid, iid, kb));
    *out = count(p.r);
    *out.add(1) = count(p.w);
    *out.add(2) = count(rows);
}

/// A count, for the Lua module's check (a count beyond u32 is not one there).
fn count(n: u64) -> u32 {
    u32::try_from(n).unwrap_or(u32::MAX)
}

/// (The same.) The W point locks on index `iid` of space `sid`, the range
/// locks on the space, the keys of the index with W locks, and the bounds of
/// the index's range locks: `out[0..4]`.
///
/// # Safety
/// `out` points to 4 counts.
pub unsafe extern "C" fn locks_index(sid: u32, iid: u32, out: *mut u32) {
    let n = COPY.with(|c| {
        let c = c.borrow();
        [
            c.w_index.get(&(sid, iid)).copied().unwrap_or(0),
            c.ranges.get(&sid).copied().unwrap_or(0),
            c.w_keys.get(&(sid, iid)).map_or(0, |k| k.len() as u64),
            c.rlocks.get(&(sid, iid)).map_or(0, |r| r.len() as u64),
        ]
    });
    for (i, n) in n.into_iter().enumerate() {
        *out.add(i) = count(n);
    }
}

/// (The same.) The range locks on index `iid` of space `sid` with these
/// bounds, and how many bounds the index's range locks have: `out[0..2]`.
///
/// # Safety
/// As for `lk_range`; `out` points to 2 counts.
pub unsafe extern "C" fn locks_range(
    sid: u32,
    iid: u32,
    lo: *const u8,
    lo_len: usize,
    hi: *const u8,
    hi_len: usize,
    out: *mut u32,
) {
    let (lo, hi) = (bound(lo, lo_len), bound(hi, hi_len));
    let (n, bounds) = COPY.with(|c| {
        let c = c.borrow();
        let locks = c.rlocks.get(&(sid, iid));
        let n = locks
            .and_then(|l| l.iter().find(|r| r.lo.as_deref() == lo && r.hi.as_deref() == hi))
            .map_or(0, |r| r.n);
        (n, locks.map_or(0, |l| l.len() as u64))
    });
    *out = count(n);
    *out.add(1) = count(bounds);
}

/// (The same.) How many keys have point locks, keys have row locks, indexes
/// have W point locks, spaces have range locks, indexes have W-locked keys
/// and indexes have range locks: `out[0..6]`.
///
/// # Safety
/// `out` points to 6 counts.
pub unsafe extern "C" fn locks_sizes(out: *mut u32) {
    COPY.with(|c| {
        let c = c.borrow();
        *out = count(c.points.len() as u64);
        *out.add(1) = count(c.rows.len() as u64);
        *out.add(2) = count(c.w_index.len() as u64);
        *out.add(3) = count(c.ranges.len() as u64);
        *out.add(4) = count(c.w_keys.len() as u64);
        *out.add(5) = count(c.rlocks.len() as u64);
    });
}

// --- lookups -------------------------------------------------------------------

/// The transactions with an R lock, and with a W lock, on key `kb` of index
/// `iid` of space `sid`.
pub fn point(sid: u32, iid: u32, kb: &[u8]) -> Counts {
    COPY.with(|c| c.borrow_mut().get(sid, iid, kb).0)
}

/// Whether a transaction holds a W point lock on some key of index `iid` of
/// space `sid`.
pub fn w_on_index(sid: u32, iid: u32) -> bool {
    !v::index_free(COPY.with(|c| c.borrow().w_index.get(&(sid, iid)).copied().unwrap_or(0)))
}

/// Whether a transaction holds a range lock on space `sid`.
pub fn ranges_on(sid: u32) -> bool {
    COPY.with(|c| c.borrow().ranges.contains_key(&sid))
}

/// Whether some key of index `iid` of space `sid` with a W lock on it is one
/// that `hit` finds (it says whether a key lies in the range read).
pub fn w_key_hit(sid: u32, iid: u32, mut hit: impl FnMut(&[u8]) -> bool) -> bool {
    COPY.with(|c| {
        let c = c.borrow();
        c.w_keys
            .get(&(sid, iid))
            .is_some_and(|keys| keys.keys().any(|k| hit(k)))
    })
}

/// Whether some range lock of index `iid` of space `sid` is one that `hit`
/// finds (it says whether a key written lies between the bounds).
pub fn range_hit(sid: u32, iid: u32, mut hit: impl FnMut(Option<&[u8]>, Option<&[u8]>) -> bool) -> bool {
    COPY.with(|c| {
        let c = c.borrow();
        c.rlocks
            .get(&(sid, iid))
            .is_some_and(|locks| locks.iter().any(|r| hit(r.lo.as_deref(), r.hi.as_deref())))
    })
}

/// Whether index `iid` of space `sid` has range locks.
pub fn ranges_on_index(sid: u32, iid: u32) -> bool {
    COPY.with(|c| c.borrow().rlocks.contains_key(&(sid, iid)))
}

/// Whether a transaction holds a row lock on key `kb` of index `iid` of
/// space `sid`.
pub fn row_locked(sid: u32, iid: u32, kb: &[u8]) -> bool {
    !v::row_free(COPY.with(|c| c.borrow_mut().get(sid, iid, kb).1))
}

// --- keys ------------------------------------------------------------------------

/// Whether `v` (a value as `lua_value` has it) is one that a lock's key is
/// looked up by here: not an extension. (A decimal's MessagePack is the Lua
/// serializer's to decide, which this module does not check: a key with one
/// goes to Lua.)
fn plain(v: &[u8]) -> bool {
    !matches!(v.first(), Some(0xc7..=0xc9 | 0xd4..=0xd8))
}

/// A key (a MessagePack array) as the lock index has it: `kbin`, the key as
/// the Lua procedure decodes it and `msgpack.encode` writes it again. None
/// for one that is not looked up here: not an array, or with a value Lua
/// would store otherwise (`lua_value`), or a decimal.
pub fn lock_key(v: &[u8]) -> Option<Cow<'_, [u8]>> {
    let k = lua_array(v)?;
    let mut r = Rd::new(&k);
    for _ in 0..r.array()? {
        if !plain(r.raw()?) {
            return None;
        }
    }
    Some(k)
}

/// The fields of a tuple (its MessagePack data), as MessagePack values.
pub fn fields_of(data: &[u8]) -> Option<Vec<&[u8]>> {
    let mut r = Rd::new(data);
    let n = r.array()?;
    let mut out = Vec::with_capacity(n as usize);
    for _ in 0..n {
        out.push(r.raw()?);
    }
    Some(out)
}

/// The key of an index whose parts are `parts` ((field from 0, nullable))
/// in a tuple whose fields are `t`, as the lock index has it (`kbin` of the
/// key that `key_def:extract_key` gives: NULL for a nullable field the tuple
/// lacks). None when a part that is not nullable lacks its field, or with a
/// value `lock_key` does not look up.
pub fn key_of(t: &[&[u8]], parts: &[(u32, bool)]) -> Option<Vec<u8>> {
    let mut w = Wr::with_capacity(16);
    w.array(u32::try_from(parts.len()).ok()?);
    for &(f, nullable) in parts {
        let v = match t.get(f as usize) {
            Some(v) => lua_value(v)?,
            None if nullable => Cow::Borrowed(&[0xc0][..]),
            None => return None,
        };
        if !plain(&v) {
            return None;
        }
        w.raw(&v);
    }
    Some(w.0)
}

/// Whether a key (a MessagePack array) has a NULL part (has_null: a unique
/// key with one claims nothing).
fn has_null(k: &[u8]) -> bool {
    let mut r = Rd::new(k);
    let n = r.array().unwrap_or(0);
    (0..n).any(|_| r.raw() == Some(&[0xc0][..]))
}

// --- the native procedures' checks -------------------------------------------------

/// An index of a table, as the checks of a write need it: its number,
/// whether it is unique, and its parts: (the field, from 0; nullable).
#[derive(Clone, Debug, PartialEq)]
pub struct KeyIndex {
    pub iid: u32,
    pub unique: bool,
    pub parts: Vec<(u32, bool)>,
}

/// Whether a point read (of a whole key of a unique index, by a read with
/// the lock check; or a point of a read set) meets no W lock of a prepared
/// transaction on its key (point_w_locked; TarnCommit!SnapshotRead,
/// readsOK).
pub fn point_read_free(sid: u32, iid: u32, key: &[u8]) -> bool {
    lock_key(key).is_some_and(|k| v::read_free(self::point(sid, iid, &k)))
}

/// Whether a read of a range of index `iid` of space `sid` meets no W lock
/// of a prepared transaction on a key in it (range_w_locked): none on the
/// index, or none on a key `in_range` places in the range (it says whether
/// a key may lie in it).
pub fn range_free(sid: u32, iid: u32, in_range: &mut dyn FnMut(&[u8]) -> bool) -> bool {
    !w_on_index(sid, iid) || !w_key_hit(sid, iid, |k| in_range(k))
}

/// How a key that a write gives an index compares with the bounds of the
/// index's range locks: `key` readies key `k` of index `iid` (false: it is
/// not compared, and may lie in any range), then `within` says whether it
/// may lie between two bounds (None: no bound; false only when it is known
/// to lie outside them).
pub trait Bounds {
    fn key(&mut self, iid: u32, k: &[u8]) -> bool;
    fn within(&mut self, lo: Option<&[u8]>, hi: Option<&[u8]>) -> bool;
}

/// Whether a write meets no lock, as write_conflict finds (TarnCommit:
/// writesOK, and the row locks of plocks): the write of space `sid` with
/// primary key `pk` (as `lock_key` has it), of a row whose versions before
/// and after are `old` and `new` (their fields), on a table whose indexes
/// are `indexes` (the primary one among them). `locked`: the table may have
/// locks of prepared transactions; `plocked`: row locks. None of another
/// transaction:
/// - no row lock on the primary key, nor on a key a unique index takes from
///   the new row (write_plocked);
/// - no point lock on the primary key, nor a range lock whose range holds it
///   (`bounds` compares);
/// - no R lock on the keys the row had or takes in the other indexes, nor any
///   lock on a key of a unique index that the new row claims (none of its
///   parts NULL), nor a range lock whose range holds one of those keys.
#[allow(clippy::too_many_arguments)]
pub fn write_free(
    sid: u32,
    pk: &[u8],
    old: Option<&[&[u8]]>,
    new: Option<&[&[u8]]>,
    indexes: &[KeyIndex],
    locked: bool,
    plocked: bool,
    bounds: &mut dyn Bounds,
) -> bool {
    let secondary = || indexes.iter().filter(|ix| ix.iid != 0);
    if plocked {
        if row_locked(sid, 0, pk) {
            return false;
        }
        if let Some(t) = new {
            for ix in secondary().filter(|ix| ix.unique) {
                match key_of(t, &ix.parts) {
                    Some(k) if !row_locked(sid, ix.iid, &k) => {}
                    _ => return false,
                }
            }
        }
    }
    if !locked {
        return true;
    }
    // (range_locked: a range lock of the index whose range holds the key)
    let mut ranged = |iid: u32, k: &[u8]| {
        ranges_on_index(sid, iid) && (!bounds.key(iid, k) || range_hit(sid, iid, |lo, hi| bounds.within(lo, hi)))
    };
    if !v::primary_free(self::point(sid, 0, pk)) || ranged(0, pk) {
        return false;
    }
    for (t, new) in [(old, false), (new, true)] {
        let Some(t) = t else { continue };
        for ix in secondary() {
            let Some(k) = key_of(t, &ix.parts) else {
                return false;
            };
            let claims = new && ix.unique && !has_null(&k);
            if !v::secondary_free(self::point(sid, ix.iid, &k), claims) || ranged(ix.iid, &k) {
                return false;
            }
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sizes() -> [u32; 6] {
        let mut out = [9u32; 6];
        unsafe { locks_sizes(out.as_mut_ptr()) };
        out
    }

    fn range(iid: u32, lo: Option<&[u8]>, hi: Option<&[u8]>, d: i32) {
        let (l, ll) = lo.map_or((std::ptr::null(), 0), |b| (b.as_ptr(), b.len()));
        let (h, hl) = hi.map_or((std::ptr::null(), 0), |b| (b.as_ptr(), b.len()));
        unsafe { lk_range(512, iid, l, ll, h, hl, d) };
    }

    /// (keys of one-byte integers, compared as their bytes)
    fn byte_order(k: &[u8], lo: Option<&[u8]>, hi: Option<&[u8]>) -> bool {
        lo.is_none_or(|b| k >= b) && hi.is_none_or(|b| k <= b)
    }

    /// Bounds that compare keys as `byte_order` does.
    #[derive(Default)]
    struct ByteOrder(Vec<u8>);

    impl Bounds for ByteOrder {
        fn key(&mut self, _: u32, k: &[u8]) -> bool {
            self.0 = k.to_vec();
            true
        }
        fn within(&mut self, lo: Option<&[u8]>, hi: Option<&[u8]>) -> bool {
            byte_order(&self.0, lo, hi)
        }
    }

    /// Bounds that cannot tell: every key may lie in every range.
    struct Unknown;

    impl Bounds for Unknown {
        fn key(&mut self, _: u32, _: &[u8]) -> bool {
            false
        }
        fn within(&mut self, _: Option<&[u8]>, _: Option<&[u8]>) -> bool {
            unreachable!("not readied")
        }
    }

    #[test]
    fn counts_the_holders_of_each_mode() {
        locks_clear(3);
        let k = b"\x91\x01";
        let p = |from, to| unsafe { lk_point(512, 1, k.as_ptr(), k.len(), from, to) };
        // a key read by one transaction, which then writes it; another's read
        p(NONE, R);
        assert_eq!(point(512, 1, k), Counts { r: 1, w: 0 });
        assert!(!w_on_index(512, 1));
        p(R, W);
        assert_eq!(point(512, 1, k), Counts { r: 0, w: 1 });
        assert!(w_on_index(512, 1) && !w_on_index(512, 0) && !w_on_index(513, 1));
        p(NONE, R);
        assert_eq!(point(512, 1, k), Counts { r: 1, w: 1 });
        // (no change)
        p(W, W);
        assert_eq!(point(512, 1, k), Counts { r: 1, w: 1 });
        p(W, NONE);
        assert_eq!(point(512, 1, k), Counts { r: 1, w: 0 });
        assert!(!w_on_index(512, 1));
        assert_eq!(sizes(), [1, 0, 0, 0, 0, 0]);
        p(R, NONE);
        assert_eq!(point(512, 1, k), Counts { r: 0, w: 0 });
        assert_eq!(point(512, 1, b"\x91\x02"), Counts { r: 0, w: 0 });
        assert_eq!(sizes(), [0, 0, 0, 0, 0, 0]);
        // the same key of another index, or space
        p(NONE, W);
        assert_eq!(point(512, 2, k), Counts { r: 0, w: 0 });
        assert_eq!(point(513, 1, k), Counts { r: 0, w: 0 });
        locks_clear(1);
        assert_eq!(point(512, 1, k), Counts { r: 0, w: 0 });
        assert_eq!(sizes(), [0, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn counts_ranges_and_row_locks() {
        locks_clear(3);
        let (a, b) = (&[0x91, 0x01][..], &[0x91, 0x05][..]);
        range(1, Some(a), Some(b), 2);
        range(1, None, Some(b), 1);
        assert!(ranges_on(512) && !ranges_on(513) && ranges_on_index(512, 1) && !ranges_on_index(512, 0));
        let mut out = [9u32; 4];
        unsafe { locks_index(512, 1, out.as_mut_ptr()) };
        assert_eq!(
            out,
            [0, 3, 0, 2],
            "3 range locks on the table, of 2 bounds on its index"
        );
        let mut out = [9u32; 2];
        unsafe { locks_range(512, 1, a.as_ptr(), a.len(), b.as_ptr(), b.len(), out.as_mut_ptr()) };
        assert_eq!(out, [2, 2]);
        assert_eq!(sizes(), [0, 0, 0, 1, 0, 1]);
        // the bounds of each, to compare a key with
        let mut seen = Vec::new();
        assert!(!range_hit(512, 1, |lo, hi| {
            seen.push((lo.map(|x| x.to_vec()), hi.map(|x| x.to_vec())));
            false
        }));
        seen.sort();
        assert_eq!(
            seen,
            vec![(None, Some(b.to_vec())), (Some(a.to_vec()), Some(b.to_vec()))]
        );
        assert!(range_hit(512, 1, |lo, hi| byte_order(&[0x91, 0x03], lo, hi)));
        assert!(!range_hit(512, 1, |lo, hi| byte_order(&[0x91, 0x07], lo, hi)));
        range(1, Some(a), Some(b), -2);
        range(1, None, Some(b), -1);
        assert!(!ranges_on(512) && !ranges_on_index(512, 1));
        let k = b"\x91\xa1x";
        let row = |d| unsafe { pl_key(512, 0, k.as_ptr(), k.len(), d) };
        row(1);
        row(1);
        assert!(row_locked(512, 0, k) && !row_locked(512, 1, k));
        let mut out = [9u32; 3];
        unsafe { locks_key(512, 0, k.as_ptr(), k.len(), out.as_mut_ptr()) };
        assert_eq!(out, [0, 0, 2]);
        row(-1);
        assert!(row_locked(512, 0, k));
        assert_eq!(sizes(), [0, 1, 0, 0, 0, 0]);
        row(-1);
        assert!(!row_locked(512, 0, k));
        row(1);
        range(0, None, None, 1);
        locks_clear(1);
        assert!(!ranges_on(512) && !ranges_on_index(512, 0));
        assert!(row_locked(512, 0, k), "the point and range locks alone");
        locks_clear(2);
        assert!(!row_locked(512, 0, k));
        assert_eq!(sizes(), [0, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn keys_as_the_lua_module_encodes_them() {
        // an integer in a longer form than needed, a double that holds an
        // integer, a string with a longer header than needed, NULL
        let k = [
            0x94, 0xcc, 0x05, 0xcb, 0x3f, 0xf0, 0, 0, 0, 0, 0, 0, 0xd9, 0x01, b'x', 0xc0,
        ];
        assert_eq!(&lock_key(&k).unwrap()[..], &[0x94, 0x05, 0x01, 0xa1, b'x', 0xc0][..]);
        assert!(matches!(lock_key(&[0x91, 0x05]), Some(Cow::Borrowed(_))));
        // not an array; a decimal (MP_EXT 1); a NaN; an array in a key
        let nan = [0x91, 0xcb, 0x7f, 0xf8, 0, 0, 0, 0, 0, 0];
        for bad in [&[0x05][..], &[0x91, 0xd5, 0x01, 0x00, 0x1c], &nan, &[0x91, 0x90]] {
            assert!(lock_key(bad).is_none(), "{:x?}", bad);
        }
        // the key of fields 2 and 0 of a tuple
        let t = [0x93, 0xcd, 0x00, 0x07, 0xa1, b'a', 0xca, 0x40, 0x00, 0x00, 0x00];
        let f = fields_of(&t).unwrap();
        assert_eq!(f.len(), 3);
        assert_eq!(key_of(&f, &[(2, false), (0, false)]).unwrap(), vec![0x92, 0x02, 0x07]);
        assert_eq!(key_of(&f, &[(1, true)]).unwrap(), vec![0x91, 0xa1, b'a']);
        assert_eq!(
            key_of(&f, &[(3, true)]).unwrap(),
            vec![0x91, 0xc0],
            "a nullable field the row lacks"
        );
        assert!(key_of(&f, &[(3, false)]).is_none(), "another");
        assert!(has_null(&[0x92, 0x01, 0xc0]) && !has_null(&[0x92, 0x01, 0x02]) && !has_null(&[0x90]));
    }

    fn ix(iid: u32, unique: bool, fields: &[u32]) -> KeyIndex {
        KeyIndex {
            iid,
            unique,
            parts: fields.iter().map(|&f| (f, f == 2)).collect(),
        }
    }

    #[test]
    fn checks_writes_as_write_conflict() {
        locks_clear(3);
        // t(id, v, u): primary key id, v non-unique, u unique (nullable)
        let indexes = [ix(0, true, &[0]), ix(1, false, &[1]), ix(2, true, &[2])];
        let (old, new) = ([0x93, 0x01, 0x05, 0x07], [0x93, 0x01, 0x06, 0x08]);
        let (old, new) = (fields_of(&old).unwrap(), fields_of(&new).unwrap());
        let pk = [0x91, 0x01];
        let write = || {
            write_free(
                512,
                &pk,
                Some(&old),
                Some(&new),
                &indexes,
                true,
                true,
                &mut ByteOrder::default(),
            )
        };
        let p = |iid, k: &[u8], from, to| unsafe { lk_point(512, iid, k.as_ptr(), k.len(), from, to) };
        assert!(write(), "no lock");
        // the primary key, locked in either mode
        for m in [R, W] {
            p(0, &pk, NONE, m);
            assert!(!write());
            p(0, &pk, m, NONE);
        }
        p(0, &[0x91, 0x02], NONE, W);
        assert!(write(), "another row's");
        // keys of the other indexes: R locks on the keys the row had or takes
        for k in [[0x91, 0x05], [0x91, 0x06]] {
            p(1, &k, NONE, R);
            assert!(!write());
            p(1, &k, R, W);
            assert!(write(), "a W lock of a row that has the same key");
            p(1, &k, W, NONE);
        }
        // and any lock on a key a unique index takes from the new row
        p(2, &[0x91, 0x07], NONE, W);
        assert!(write(), "a W lock on the key the row gives up");
        p(2, &[0x91, 0x08], NONE, W);
        assert!(!write());
        let new_null = fields_of(&[0x93, 0x01, 0x06, 0xc0]).unwrap();
        p(2, &[0x91, 0xc0], NONE, W);
        assert!(
            write_free(
                512,
                &pk,
                Some(&old),
                Some(&new_null),
                &indexes,
                true,
                true,
                &mut Unknown
            ),
            "a NULL claims nothing"
        );
        p(2, &[0x91, 0xc0], W, R);
        assert!(!write_free(
            512,
            &pk,
            Some(&old),
            Some(&new_null),
            &indexes,
            true,
            true,
            &mut Unknown
        ));
        locks_clear(1);
        assert!(write());
        // range locks: whose range holds the primary key, or a key the row had
        // or takes in another index
        let (k1, k5, k6, k9) = ([0x91, 0x01], [0x91, 0x05], [0x91, 0x06], [0x91, 0x09]);
        for (iid, lo, hi) in [(0, &k1, &k1), (1, &k5, &k5), (1, &k6, &k9), (2, &k6, &k9)] {
            range(iid, Some(lo), Some(hi), 1);
            assert!(!write(), "{iid} {lo:x?} {hi:x?}");
            assert!(
                write_free(512, &pk, Some(&old), Some(&new), &indexes, false, false, &mut Unknown),
                "(unless there is none)"
            );
            range(iid, Some(lo), Some(hi), -1);
        }
        for (iid, lo, hi) in [(0, &k5, &k9), (1, &k9, &k9), (2, &k1, &k5)] {
            range(iid, Some(lo), Some(hi), 1);
            assert!(write(), "{iid} {lo:x?} {hi:x?}: another range");
            range(iid, Some(lo), Some(hi), -1);
        }
        range(1, None, Some(&k1), 1);
        assert!(write(), "a range below the keys");
        assert!(
            !write_free(512, &pk, Some(&old), Some(&new), &indexes, true, true, &mut Unknown),
            "(which a comparison that cannot tell takes to hold them)"
        );
        range(1, None, Some(&k1), -1);
        // row locks: on the primary key, or a key a unique index takes
        let row = |iid, k: &[u8], d| unsafe { pl_key(512, iid, k.as_ptr(), k.len(), d) };
        for (iid, k) in [(0, [0x91, 0x01]), (2, [0x91, 0x08])] {
            row(iid, &k, 1);
            assert!(!write());
            assert!(write_free(
                512,
                &pk,
                Some(&old),
                Some(&new),
                &indexes,
                true,
                false,
                &mut Unknown
            ));
            row(iid, &k, -1);
        }
        for (iid, k) in [(1, [0x91, 0x06]), (2, [0x91, 0x07])] {
            row(iid, &k, 1);
            assert!(write());
            row(iid, &k, -1);
        }
        // a delete of a row that is not there: its primary key
        p(0, &pk, NONE, R);
        let any = &mut Unknown;
        assert!(!write_free(512, &pk, None, None, &indexes, true, false, any));
        assert!(write_free(512, &[0x91, 0x03], None, None, &indexes, true, false, any));
        // a row without the field of an index: NULL there if it is nullable
        let short = fields_of(&[0x92, 0x03, 0x05]).unwrap();
        assert!(write_free(
            512,
            &[0x91, 0x03],
            None,
            Some(&short),
            &indexes,
            true,
            false,
            any
        ));
        p(2, &[0x91, 0xc0], NONE, R);
        assert!(!write_free(
            512,
            &[0x91, 0x03],
            None,
            Some(&short),
            &indexes,
            true,
            false,
            any
        ));
        let shorter = fields_of(&[0x91, 0x03]).unwrap();
        assert!(
            !write_free(512, &[0x91, 0x03], None, Some(&shorter), &indexes, true, false, any),
            "else to Lua"
        );
        locks_clear(3);
    }

    #[test]
    fn checks_reads_as_reads_w_locked() {
        locks_clear(3);
        let k = [0x91, 0x07];
        let p = |iid, from, to| unsafe { lk_point(512, iid, k.as_ptr(), k.len(), from, to) };
        let in_range = |lo: &'static [u8], hi: &'static [u8]| move |key: &[u8]| byte_order(key, Some(lo), Some(hi));
        assert!(point_read_free(512, 0, &k) && range_free(512, 0, &mut |_| true));
        p(0, NONE, R);
        assert!(
            point_read_free(512, 0, &k) && range_free(512, 0, &mut |_| true),
            "an R lock"
        );
        p(0, R, W);
        assert!(!point_read_free(512, 0, &k) && !range_free(512, 0, &mut in_range(b"\x91\x01", b"\x91\x09")));
        assert!(point_read_free(512, 0, &[0x91, 0x08]), "another key");
        assert!(
            range_free(512, 0, &mut in_range(b"\x91\x08", b"\x91\x09")),
            "another range"
        );
        assert!(!range_free(512, 0, &mut |_| true), "(a comparison that cannot tell)");
        assert!(
            point_read_free(512, 1, &k) && range_free(512, 1, &mut |_| true),
            "another index"
        );
        // (a key in a longer form than needed)
        assert!(!point_read_free(512, 0, &[0x91, 0xcc, 0x07]));
        // a key that is not looked up here: to Lua
        assert!(!point_read_free(513, 0, &[0x07]));
        p(0, W, NONE);
        assert_eq!(sizes(), [0, 0, 0, 0, 0, 0], "no W-locked key left");
        locks_clear(3);
    }
}
