//! Native use of the published clock-health observation.
//!
//! Quorum evidence is obtained afresh through Tarantool's public transaction
//! API. This check only bounds the separate clock-health cache. The Lua
//! producer's certification/persistence and the clock sources remain trusted;
//! the exact elapsed-time arithmetic is the shared Verus-verified body.

use crate::state::State;

pub fn clock_ready(s: &State, mono: u64, wall: u64) -> bool {
    s.fresh_clock_ok == 1
        && (s.fresh_mode == 1 || s.fresh_mode == 2)
        && s.clock_max_gap != 0
        && tarn_verified::clock::observation_ok(
            s.clock_wall,
            s.clock_mono,
            wall,
            mono,
            s.clock_max_gap,
            s.clock_max_drift,
        )
}
