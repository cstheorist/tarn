//! Bounded native primary-key batches.
//!
//! Every requested key consumes work, including a miss. Row and byte
//! charges cover only present stored tuples (not the fixed outer reply
//! headers). Like Lua's PG.full, all three budgets must have room BEFORE
//! a lookup; the size discovered by that lookup must fit BEFORE append.
//! Failure means handing back the whole request, never returning a
//! certified-complete prefix. These functions prove accounting, not CAPI
//! lookup correctness, lock freshness or the no-yield snapshot premise.

use vstd::prelude::*;

verus! {

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Budget {
    pub work: u64,
    pub rows: u64,
    pub bytes: u64,
}

/// Check before the next C index lookup, even if that key may be missing.
#[inline]
pub fn can_step(b: Budget, max_work: u64, max_rows: u64, max_bytes: u64) -> (r: bool)
    ensures
        r == (b.work < max_work && b.rows < max_rows && b.bytes < max_bytes),
{
    b.work < max_work && b.rows < max_rows && b.bytes < max_bytes
}

/// Account for one lookup before appending its result. A miss ignores
/// `tuple_bytes`; callers normally pass zero for it. Arbitrary budgets and
/// sizes are accepted without overflow, including u64::MAX.
#[inline]
pub fn step(
    b: Budget,
    present: bool,
    tuple_bytes: u64,
    max_work: u64,
    max_rows: u64,
    max_bytes: u64,
) -> (r: Option<Budget>)
    ensures
        (r is Some) <==> (b.work < max_work && b.rows < max_rows && b.bytes < max_bytes
            && (!present || b.bytes as int + tuple_bytes as int <= max_bytes as int)),
        r matches Some(n) ==> n.work <= max_work && n.rows <= max_rows && n.bytes <= max_bytes
            && n.work as int == b.work as int + 1
            && n.rows as int == b.rows as int + (if present { 1int } else { 0int })
            && n.bytes as int == b.bytes as int + (if present { tuple_bytes as int } else { 0int }),
{
    if !can_step(b, max_work, max_rows, max_bytes) {
        return None;
    }
    if present {
        if tuple_bytes > max_bytes - b.bytes {
            return None;
        }
        Some(Budget { work: b.work + 1, rows: b.rows + 1, bytes: b.bytes + tuple_bytes })
    } else {
        Some(Budget { work: b.work + 1, rows: b.rows, bytes: b.bytes })
    }
}

} // verus!
