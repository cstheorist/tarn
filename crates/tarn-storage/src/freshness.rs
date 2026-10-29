//! Native use of the published clock-health observation.
//!
//! Quorum evidence is obtained afresh through Tarantool's public transaction
//! API. This check only bounds the separate clock-health cache. The Lua
//! producer's certification/persistence and the clock sources remain trusted;
//! it publishes a nondecreasing logical wall baseline. The shared verified
//! body compares each current RAW wall sample with that logical baseline,
//! allowing only corrections within the same elapsed-time drift budget.
//! This native check neither changes the baseline nor certifies peer bounds.

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_raw_correction_uses_the_shared_logical_baseline_check() {
        let mut s = State {
            fresh_clock_ok: 1,
            fresh_mode: 1,
            clock_wall: 100,
            clock_mono: 20,
            clock_max_gap: 5,
            clock_max_drift: 6,
            ..State::default()
        };
        assert!(clock_ready(&s, 25, 99));
        s.clock_max_drift = 5;
        assert!(!clock_ready(&s, 25, 99));
        s.clock_max_drift = u64::MAX;
        assert!(!clock_ready(&s, 19, 99));
        s.fresh_clock_ok = 0;
        assert!(!clock_ready(&s, 25, 99));
    }
}
