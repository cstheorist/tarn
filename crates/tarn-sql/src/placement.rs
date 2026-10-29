//! Decision-record placement (TarnCommit!DecisionParticipant). Choose once
//! from writing participants, then send that durable shard ID everywhere;
//! recovery uses the recorded ID, never a fresh placement decision. The
//! membership/identity premises are checked in DecisionPlacement.tla.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Writer {
    pub id: u32,
    pub position: usize,
    pub active: bool,
}

/// The same transaction and eligible durable IDs select the same writer,
/// regardless of this gateway's shard positions or configuration order.
/// With no active writer, retain a writer as before: storage must refuse
/// its leaving-shard PREPARE, rather than inventing a read-only participant.
pub(crate) fn decision_writer(txid: &str, mut writers: Vec<Writer>) -> Option<Writer> {
    if writers.iter().any(|w| w.active) {
        writers.retain(|w| w.active);
    }
    if writers.is_empty() {
        return None;
    }
    writers.sort_unstable_by_key(|w| w.id);
    let hash = tarn_verified::route::crc32c_update(tarn_verified::route::CRC_INIT, txid.as_bytes());
    Some(writers[hash as usize % writers.len()])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn writer(id: u32, position: usize, active: bool) -> Writer {
        Writer { id, position, active }
    }

    #[test]
    fn empty_and_single_writer() {
        assert_eq!(decision_writer("1-abcd-1", vec![]), None);
        for active in [true, false] {
            let w = writer(17, 2, active);
            assert_eq!(decision_writer("1-abcd-1", vec![w]), Some(w));
        }
    }

    #[test]
    fn stable_durable_identity_across_gateway_positions() {
        let a = vec![writer(17, 0, true), writer(2, 1, true), writer(90, 2, true)];
        let b = vec![writer(90, 0, true), writer(17, 2, true), writer(2, 3, true)];
        for seq in 1..1024 {
            let id = format!("7-1234abcd-{seq}");
            let wa = decision_writer(&id, a.clone()).unwrap();
            let wb = decision_writer(&id, b.clone()).unwrap();
            assert_eq!(wa.id, wb.id);
            assert!(a.contains(&wa) && b.contains(&wb));
        }
    }

    #[test]
    fn prefers_active_writers_and_retains_the_inactive_fallback() {
        let candidates = vec![writer(2, 3, false), writer(17, 0, true), writer(90, 2, true)];
        let mut seen = std::collections::BTreeSet::new();
        for seq in 1..1024 {
            seen.insert(
                decision_writer(&format!("1-abcd-{seq}"), candidates.clone())
                    .unwrap()
                    .id,
            );
        }
        assert_eq!(seen, [17, 90].into());
        let inactive: Vec<_> = candidates.iter().map(|w| Writer { active: false, ..*w }).collect();
        for seq in 1..1024 {
            assert!(inactive.contains(&decision_writer(&format!("1-abcd-{seq}"), inactive.clone()).unwrap()));
        }
    }

    #[test]
    fn transaction_identities_spread_a_common_writer_set() {
        let candidates = vec![writer(2, 0, true), writer(17, 1, true), writer(90, 2, true)];
        let mut counts = [0; 3];
        for node in 1..=4 {
            for boot in [0x1234abcd_u32, 0xcafe5678] {
                for seq in 1..=3000 {
                    let id = format!("{node}-{boot:08x}-{seq}");
                    counts[decision_writer(&id, candidates.clone()).unwrap().position] += 1;
                }
            }
        }
        // A deterministic check of realistic IDs, not a probabilistic or
        // adversarial-load guarantee. The live benchmark reports its spread.
        for n in counts {
            assert!((7600..=8400).contains(&n), "{counts:?}");
        }
    }
}
