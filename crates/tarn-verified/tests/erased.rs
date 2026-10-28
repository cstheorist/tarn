//! Tests of the erased (plain Rust) build of the verified modules: the
//! proofs cover the source; these check the compiled artifact on the
//! boundary cases the proofs are about.

use tarn_verified::batch::{self, Budget};
use tarn_verified::coord::{self, Decision, PartState, RecStatus, Step, Vote};
use tarn_verified::mp;
use tarn_verified::route;
use tarn_verified::wire;

#[test]
fn batch_budget_preserves_misses_and_exact_last_tuple() {
    let mut b = Budget {
        work: 0,
        rows: 0,
        bytes: 0,
    };
    // The input represents A, missing B, A: count both duplicates and the
    // missing lookup, but no row or bytes for B.
    for (present, size) in [(true, 7), (false, u64::MAX), (true, 7)] {
        assert!(batch::can_step(b, 3, 2, 14));
        b = batch::step(b, present, size, 3, 2, 14).unwrap();
    }
    assert_eq!(
        b,
        Budget {
            work: 3,
            rows: 2,
            bytes: 14
        }
    );
    assert!(!batch::can_step(b, 3, 2, 14));
    assert_eq!(batch::step(b, false, 0, 3, 2, 14), None);
}

#[test]
fn batch_budget_refuses_each_exhausted_resource_before_a_miss() {
    for (work, rows, bytes) in [(1, 0, 0), (0, 1, 0), (0, 0, 1)] {
        let b = Budget { work, rows, bytes };
        assert!(!batch::can_step(b, 1, 1, 1));
        assert_eq!(batch::step(b, false, 0, 1, 1, 1), None);
    }
    let empty = Budget {
        work: 0,
        rows: 0,
        bytes: 0,
    };
    for (w, r, b) in [(0, 1, 1), (1, 0, 1), (1, 1, 0)] {
        assert!(!batch::can_step(empty, w, r, b));
        assert_eq!(batch::step(empty, true, 0, w, r, b), None);
    }
}

#[test]
fn batch_budget_checks_large_sizes_without_overflow_or_partial_charge() {
    let b = Budget {
        work: u64::MAX - 1,
        rows: u64::MAX - 1,
        bytes: u64::MAX - 1,
    };
    assert_eq!(batch::step(b, true, 2, u64::MAX, u64::MAX, u64::MAX), None);
    assert_eq!(batch::step(b, true, u64::MAX, u64::MAX, u64::MAX, u64::MAX), None);
    assert_eq!(
        batch::step(b, true, 1, u64::MAX, u64::MAX, u64::MAX),
        Some(Budget {
            work: u64::MAX,
            rows: u64::MAX,
            bytes: u64::MAX
        }),
    );
    assert_eq!(
        batch::step(b, false, u64::MAX, u64::MAX, u64::MAX, u64::MAX),
        Some(Budget {
            work: u64::MAX,
            rows: u64::MAX - 1,
            bytes: u64::MAX - 1
        }),
    );
}

#[test]
fn lenenc_boundaries_round_trip() {
    for &n in &[
        0u64,
        1,
        250,
        251,
        252,
        0xFFFF,
        0x1_0000,
        0xFF_FFFF,
        0x100_0000,
        u32::MAX as u64,
        u64::MAX,
    ] {
        let mut out = Vec::new();
        wire::write_lenenc_int(&mut out, n);
        assert_eq!(out.len(), wire::lenenc_int_len(n));
        out.extend_from_slice(&[0xAB, 0xCD]);
        assert_eq!(
            wire::read_lenenc_int(&out),
            Some((n, wire::lenenc_int_len(n))),
            "n={}",
            n
        );
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
fn bucket_owners_from_reports() {
    let n = route::NO_SHARD;
    // shard 0 owns 1..=3 and 6, shard 1 owns 4; bucket 5 is between a
    // move's send and its receive
    let o = route::build_owner(6, &[(0, 1, 3), (1, 4, 4), (0, 6, 6)]).unwrap();
    assert_eq!(o, vec![0, 0, 0, 1, n, 0]);
    // two reports claim bucket 3
    assert!(route::build_owner(6, &[(0, 1, 3), (1, 3, 4)]).is_none());
    // out of range, empty, or naming no shard
    assert!(route::build_owner(6, &[(0, 0, 3)]).is_none());
    assert!(route::build_owner(6, &[(0, 5, 7)]).is_none());
    assert!(route::build_owner(6, &[(0, 4, 3)]).is_none());
    assert!(route::build_owner(6, &[(n, 1, 1)]).is_none());
    // a new cluster's layout is a partition
    let ranges: Vec<(u32, u32, u32)> = (0..7u32)
        .map(|s| {
            let bs: Vec<u32> = (1..=3000)
                .filter(|&b| route::shard_of_bucket(b, 3000, 7) == s)
                .collect();
            (s, bs[0], *bs.last().unwrap())
        })
        .collect();
    let o = route::build_owner(3000, &ranges).unwrap();
    assert!(o.iter().all(|&s| s < 7));
}

#[test]
fn coordinator_decisions() {
    assert!(matches!(coord::decide(&[Vote::Yes, Vote::Yes]), Decision::Commit));
    assert!(matches!(coord::decide(&[Vote::Yes, Vote::No]), Decision::Abort));
    assert!(matches!(coord::decide(&[Vote::Unknown, Vote::No]), Decision::Abort));
    assert!(matches!(coord::decide(&[Vote::Yes, Vote::Unknown]), Decision::Unknown));
    use PartState::*;
    assert!(matches!(
        coord::resolve_step(RecStatus::Staging, &[Prepared, Applied], false),
        Step::DecideCommit
    ));
    assert!(matches!(
        coord::resolve_step(RecStatus::Staging, &[Prepared, Aborted], false),
        Step::DecideAbort
    ));
    match coord::resolve_step(RecStatus::Staging, &[Prepared, None, None], true) {
        Step::FenceParticipants(m) => assert_eq!(m, vec![1, 2]),
        other => panic!("{:?}", other),
    }
    assert!(matches!(
        coord::resolve_step(RecStatus::None, &[Prepared], false),
        Step::FenceRecord
    ));
    // expired: fence a missing record only while someone is still prepared
    assert!(matches!(
        coord::resolve_step(RecStatus::None, &[Prepared, None], true),
        Step::FenceRecord
    ));
    assert!(matches!(
        coord::resolve_step(RecStatus::None, &[Applied, None], true),
        Step::Gone
    ));
    assert!(matches!(
        coord::resolve_step(RecStatus::None, &[None, None], false),
        Step::FenceRecord
    ));
    assert!(matches!(
        coord::resolve_step(RecStatus::Committed, &[None, Applied], true),
        Step::Committed
    ));
}

#[test]
fn msgpack_integers_at_their_boundaries() {
    // (value, its encoding's length)
    for &(v, len) in &[
        (0i128, 1usize),
        (0x7f, 1),
        (0x80, 2),
        (0xff, 2),
        (0x100, 3),
        (0xffff, 3),
        (0x1_0000, 5),
        (0xffff_ffff, 5),
        (0x1_0000_0000, 9),
        (u64::MAX as i128, 9),
        (-1, 1),
        (-32, 1),
        (-33, 2),
        (-128, 2),
        (-129, 3),
        (-0x8000, 3),
        (-0x8001, 5),
        (i32::MIN as i128, 5),
        (i32::MIN as i128 - 1, 9),
        (i64::MIN as i128, 9),
    ] {
        let mut out = vec![0xc0];
        assert!(mp::write_int(&mut out, v));
        assert_eq!(out.len(), 1 + len, "v={}", v);
        out.extend_from_slice(&[0xc3, 0xc2]);
        assert_eq!(mp::read_int(&out, 1), Some((v, 1 + len)), "v={}", v);
        assert_eq!(mp::skip(&out, 1), Some(1 + len), "v={}", v);
        let u = mp::read_uint(&out, 1);
        assert_eq!(u, (v >= 0).then(|| (v as u64, 1 + len)), "v={}", v);
        if v >= 0 {
            let mut w = vec![0xc0];
            mp::write_uint(&mut w, v as u64);
            assert_eq!(w, out[..1 + len]);
        }
    }
    // Tarantool's bytes
    let enc = |v: i128| {
        let mut out = Vec::new();
        assert!(mp::write_int(&mut out, v));
        out
    };
    assert_eq!(enc(-33), [0xd0, 0xdf]);
    assert_eq!(enc(-129), [0xd1, 0xff, 0x7f]);
    assert_eq!(enc(300), [0xcd, 0x01, 0x2c]);
    assert_eq!(enc(i64::MIN as i128), [0xd3, 0x80, 0, 0, 0, 0, 0, 0, 0]);
    // neither int64 nor uint64: nothing written
    let mut out = vec![1];
    assert!(!mp::write_int(&mut out, u64::MAX as i128 + 1));
    assert!(!mp::write_int(&mut out, i64::MIN as i128 - 1));
    assert_eq!(out, [1]);
    // a signed encoding of a non-negative integer is an unsigned one too
    assert_eq!(mp::read_uint(&[0xd1, 0x01, 0x00], 0), Some((256, 3)));
    assert_eq!(mp::read_uint(&[0xd1, 0x81, 0x00], 0), None);
    assert_eq!(mp::read_int(&[0xd1, 0x81, 0x00], 0), Some((-0x7f00, 3)));
    // cut short, or not an integer
    assert_eq!(mp::read_int(&[0xcf, 0, 0, 0, 0, 0, 0, 0], 0), None);
    assert_eq!(mp::read_int(&[0xc0], 0), None);
    assert_eq!(mp::read_int(&[1], 1), None);
}

#[test]
fn msgpack_headers_at_their_boundaries() {
    for &(n, len) in &[
        (0u32, 1usize),
        (15, 1),
        (16, 3),
        (0xffff, 3),
        (0x1_0000, 5),
        (u32::MAX, 5),
    ] {
        let mut out = Vec::new();
        mp::write_array(&mut out, n);
        assert_eq!(out.len(), len, "n={}", n);
        assert_eq!(mp::read_array(&out, 0), Some((n, len)), "n={}", n);
        assert_eq!(mp::read_map(&out, 0), None);
        let mut out = Vec::new();
        mp::write_map(&mut out, n);
        assert_eq!(out.len(), len, "n={}", n);
        assert_eq!(mp::read_map(&out, 0), Some((n, len)), "n={}", n);
        assert_eq!(mp::read_array(&out, 0), None);
    }
    assert_eq!(mp::read_map(&[0x82], 0), Some((2, 1)));
    assert_eq!(mp::read_map(&[0xde, 0x01, 0x00], 0), Some((256, 3)));
    assert_eq!(mp::read_map(&[0xdf, 0, 0, 1], 0), None);
    for &(bin, n, len) in &[
        (false, 0usize, 1usize),
        (false, 31, 1),
        (false, 32, 2),
        (true, 0, 2),
        (true, 31, 2),
        (false, 0xff, 2),
        (true, 0x100, 3),
        (false, 0xffff, 3),
        (true, 0x1_0000, 5),
    ] {
        let mut out = vec![0x90];
        mp::write_bytes_header(&mut out, bin, n as u32);
        assert_eq!(out.len(), 1 + len, "bin={} n={}", bin, n);
        out.resize(1 + len + n, 7);
        assert_eq!(
            mp::read_bytes(&out, 1, bin),
            Some((1 + len, 1 + len + n)),
            "bin={} n={}",
            bin,
            n
        );
        assert_eq!(mp::read_bytes(&out, 1, !bin), None);
        assert_eq!(mp::skip(&out, 1), Some(out.len()));
        assert_eq!(mp::skip(&out[..out.len() - 1], 1), None, "cut short");
    }
    let mut out = Vec::new();
    mp::write_double_bits(&mut out, 1.5f64.to_bits());
    assert_eq!(mp::read_float_bits(&out, 0), Some((1.5f64.to_bits(), true, 9)));
    assert_eq!(
        mp::read_float_bits(&[0xca, 0x3f, 0xc0, 0, 0], 0),
        Some((1.5f32.to_bits() as u64, false, 5))
    );
    // an array whose length is filled in later
    let mut out = vec![0xc0];
    let at = mp::write_array_slot(&mut out);
    out.extend_from_slice(&[1, 2, 3]);
    assert!(mp::patch_array(&mut out, at, 3));
    assert_eq!(out, [0xc0, 0xdd, 0, 0, 0, 3, 1, 2, 3]);
    assert_eq!(mp::read_array(&out, 1), Some((3, 6)));
    assert_eq!(mp::skip(&out, 1), Some(9));
    assert!(!mp::patch_array(&mut out, 5, 3));
    assert_eq!(out, [0xc0, 0xdd, 0, 0, 0, 3, 1, 2, 3]);
}

#[test]
fn msgpack_values_skipped_however_nested() {
    // [{"e": 5, "m": [1, nil, true]}, -1, "x", 1.5, bin(2), fixext1, ext8(2)]
    let buf: &[u8] = &[
        0x97, 0x82, 0xa1, b'e', 0x05, 0xa1, b'm', 0x93, 0x01, 0xc0, 0xc3, 0xff, 0xa1, b'x', 0xcb, 0, 0, 0, 0, 0, 0, 0,
        0, 0xc4, 2, 1, 2, 0xd4, 1, 7, 0xc7, 2, 9, 1, 2,
    ];
    assert_eq!(mp::skip(buf, 0), Some(buf.len()));
    assert_eq!(mp::skip(buf, 1), Some(11));
    for end in 0..buf.len() {
        assert_eq!(mp::skip(&buf[..end], 0), None, "cut at {}", end);
    }
    // a byte no value starts with
    assert_eq!(mp::skip(&[0x91, 0xc1], 0), None);
    // more elements than bytes left
    assert_eq!(mp::skip(&[0xdd, 0xff, 0xff, 0xff, 0xff, 1, 2], 0), None);
    assert_eq!(mp::skip(&[0xdf, 0xff, 0xff, 0xff, 0xff], 0), None);
    assert_eq!(mp::skip(&[], 0), None);
    assert_eq!(mp::skip(&[1], 5), None);
}
