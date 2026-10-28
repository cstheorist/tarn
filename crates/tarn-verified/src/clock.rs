//! Arithmetic at the native clock-health boundary.
//!
//! A published observation is usable only while both clocks have advanced,
//! the observer gap is bounded and their elapsed times differ by no more
//! than the configured drift allowance. Inputs are nanoseconds. This proves
//! the exact overflow-free decision used by native procedures; it does not
//! certify clock sources, remote offsets, policy persistence or GC safety.

use vstd::prelude::*;

verus! {

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
        r == (wall >= last_wall && mono >= last_mono
            && mono as int - last_mono as int <= max_gap as int
            && (if wall as int - last_wall as int >= mono as int - last_mono as int {
                (wall as int - last_wall as int) - (mono as int - last_mono as int) <= max_drift as int
            } else {
                (mono as int - last_mono as int) - (wall as int - last_wall as int) <= max_drift as int
            })),
{
    if wall < last_wall || mono < last_mono {
        return false;
    }
    let dm = mono - last_mono;
    if dm > max_gap {
        return false;
    }
    let dw = wall - last_wall;
    if dw >= dm {
        dw - dm <= max_drift
    } else {
        dm - dw <= max_drift
    }
}

} // verus!
