//! Tests of the erased (plain Rust) build of the verified modules: the
//! proofs cover the source; these check the compiled artifact on the
//! boundary cases the proofs are about.

use tarn_verified::coord::{self, Decision, PartState, RecStatus, Step, Vote};
use tarn_verified::route;
use tarn_verified::wire;

#[test]
fn lenenc_boundaries_round_trip() {
    for &n in &[0u64, 1, 250, 251, 252, 0xFFFF, 0x1_0000, 0xFF_FFFF, 0x100_0000, u32::MAX as u64, u64::MAX] {
        let mut out = Vec::new();
        wire::write_lenenc_int(&mut out, n);
        assert_eq!(out.len(), wire::lenenc_int_len(n));
        out.extend_from_slice(&[0xAB, 0xCD]);
        assert_eq!(wire::read_lenenc_int(&out), Some((n, wire::lenenc_int_len(n))), "n={}", n);
    }
    assert_eq!(wire::read_lenenc_int(&[0xFB]), None);
    assert_eq!(wire::read_lenenc_int(&[0xFF]), None);
    assert_eq!(wire::read_lenenc_int(&[0xFC, 1]), None);
    assert_eq!(wire::read_lenenc_int(&[]), None);
}

#[test]
fn lenenc_strings() {
    let mut out = Vec::new();
    wire::write_lenenc_str(&mut out, b"hello");
    assert_eq!(wire::read_lenenc_str_bounds(&out), Some((1, 5, 6)));
    assert_eq!(wire::read_lenenc_str_bounds(&out[..4]), None);
}

#[test]
fn framing_boundaries() {
    let m = wire::MAX_PAYLOAD;
    for &len in &[0usize, 1, m - 1, m, m + 1, 2 * m, 2 * m + 5] {
        let payload: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
        let mut out = Vec::new();
        let mut seq = 254u8;
        wire::frame_packets(&mut out, &payload, &mut seq);
        let packets = len / m + 1;
        assert_eq!(out.len(), len + 4 * packets, "len={}", len);
        assert_eq!(seq, 254u8.wrapping_add(packets as u8));
        if len % m == 0 && len > 0 {
            // terminated by an empty packet
            assert_eq!(&out[out.len() - 4..out.len() - 1], &[0, 0, 0]);
        }
        let mut wire_bytes = out.clone();
        wire_bytes.extend_from_slice(&[9, 9, 9]);
        let (back, used) = wire::deframe(&wire_bytes).expect("complete payload");
        assert_eq!(back, payload);
        assert_eq!(used, out.len());
        assert!(wire::deframe(&out[..out.len() - 1]).is_none() || len == 0);
    }
}

#[test]
fn crc_matches_tarantool() {
    assert_eq!(route::crc32c_update(route::CRC_INIT, b"123456789"), 486108540);
    let b = route::bucket_from_crc(486108540, 3000);
    assert!((1..=3000).contains(&b));
    for bucket in 1..=3000 {
        assert!(route::shard_of_bucket(bucket, 3000, 7) < 7);
    }
}

#[test]
fn coordinator_decisions() {
    assert!(matches!(coord::decide(&[Vote::Yes, Vote::Yes]), Decision::Commit));
    assert!(matches!(coord::decide(&[Vote::Yes, Vote::No]), Decision::Abort));
    assert!(matches!(coord::decide(&[Vote::Unknown, Vote::No]), Decision::Abort));
    assert!(matches!(coord::decide(&[Vote::Yes, Vote::Unknown]), Decision::Unknown));
    use PartState::*;
    assert!(matches!(coord::resolve_step(RecStatus::Staging, &[Prepared, Applied]), Step::DecideCommit));
    assert!(matches!(coord::resolve_step(RecStatus::Staging, &[Prepared, Aborted]), Step::DecideAbort));
    match coord::resolve_step(RecStatus::Staging, &[Prepared, None, None]) {
        Step::FenceParticipants(m) => assert_eq!(m, vec![1, 2]),
        other => panic!("{:?}", other),
    }
    assert!(matches!(coord::resolve_step(RecStatus::None, &[Prepared]), Step::FenceRecord));
}
