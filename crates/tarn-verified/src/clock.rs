//! Arithmetic at the native clock-health boundary.
//!
//! The monotonic clock cannot decrease, the observer gap is bounded, and
//! raw wall-clock displacement from the last LOGICAL wall observation must
//! agree with elapsed monotonic time within the drift allowance. A bounded
//! backward correction is therefore allowed; publication keeps the maximum
//! logical wall value. Inputs are nanoseconds. This proves the shared exact
//! arithmetic, not clock sources, peer bounds, Lua publication or GC safety.

use vstd::prelude::*;

verus! {

#[inline]
pub fn logical_wall(previous: u64, raw: u64) -> (r: u64)
    ensures
        r >= previous,
        r >= raw,
        r == previous || r == raw,
        r == (if previous >= raw { previous } else { raw }),
{
    if previous >= raw { previous } else { raw }
}

/// A single clock retains an absolute error band around a nondecreasing
/// reference. This does not establish a pairwise bound between clocks;
/// the configured peer/issuer bound remains a separate trusted premise.
pub proof fn lemma_logical_wall_preserves_band(
    previous: u64, raw: u64, previous_reference: int, reference: int, error: nat,
)
    requires
        previous_reference <= reference,
        previous as int <= previous_reference + error,
        reference - error <= raw as int <= reference + error,
    ensures
        reference - error <= (if previous >= raw { previous } else { raw }) as int <= reference + error,
{
}

#[inline]
pub fn observation_ok(
    last_wall: u64,
    last_mono: u64,
    wall: u64,
    mono: u64,
    max_gap: u64,
    max_drift: u64,
) -> (r: bool)
    ensures
        r == (mono >= last_mono
            && mono as int - last_mono as int <= max_gap as int
            && (if wall as int - last_wall as int >= mono as int - last_mono as int {
                (wall as int - last_wall as int) - (mono as int - last_mono as int) <= max_drift as int
            } else {
                (mono as int - last_mono as int) - (wall as int - last_wall as int) <= max_drift as int
            })),
{
    if mono < last_mono {
        return false;
    }
    let dm = mono - last_mono;
    if dm > max_gap {
        return false;
    }
    let logical = logical_wall(last_wall, wall);
    let forward = logical - last_wall;
    let backward = logical - wall;
    if wall < last_wall {
        // backward + dm may overflow even though each delta is valid.
        backward <= max_drift && dm <= max_drift - backward
    } else if forward >= dm {
        forward - dm <= max_drift
    } else {
        dm - forward <= max_drift
    }
}

} // verus!
