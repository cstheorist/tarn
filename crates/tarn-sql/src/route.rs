//! Row placement: shard key -> bucket -> shard.
//!
//! The bucket function is vshard's `mpcrc32`: Tarantool's `digest.crc32`
//! (CRC-32C, initial value 0xFFFFFFFF, no final xor) over the shard key
//! parts, where strings contribute their raw bytes and every other value its
//! msgpack encoding; bucket = crc % bucket_count + 1. Using the same function
//! means vshard/crud routers configured with `bucket_id_mpcrc32` find rows
//! written by Tarn. The arithmetic core lives in tarn-verified, where it is
//! proven in Verus (bucket in range, shard map total and a partition).

use tarn_common::Value;

/// Bytes a single shard-key value contributes to the hash.
pub fn key_part_bytes(v: &Value, out: &mut Vec<u8>) {
    match v {
        Value::Str(b) | Value::Bin(b) => out.extend_from_slice(b),
        other => tarn_iproto::mp::encode_value(out, other),
    }
}

/// Bucket id (1-based) of a shard key.
pub fn bucket_of(key: &[Value], bucket_count: u32) -> u32 {
    let mut crc = tarn_verified::route::CRC_INIT;
    let mut buf = Vec::with_capacity(32);
    for v in key {
        buf.clear();
        key_part_bytes(v, &mut buf);
        crc = tarn_verified::route::crc32c_update(crc, &buf);
    }
    tarn_verified::route::bucket_from_crc(crc, bucket_count)
}

/// Shard index (0-based) owning a bucket: contiguous, equal-sized ranges.
pub fn shard_of_bucket(bucket: u32, bucket_count: u32, nshards: u32) -> u32 {
    tarn_verified::route::shard_of_bucket(bucket, bucket_count, nshards)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_tarantool_digest_crc32() {
        // digest.crc32('123456789') in Tarantool 3.9
        assert_eq!(
            tarn_verified::route::crc32c_update(tarn_verified::route::CRC_INIT, b"123456789"),
            486108540
        );
        assert_eq!(
            tarn_verified::route::crc32c_update(tarn_verified::route::CRC_INIT, b""),
            0xFFFF_FFFF
        );
    }

    #[test]
    fn buckets_in_range_and_spread() {
        let mut seen = std::collections::HashSet::new();
        for i in 0..10_000i64 {
            let b = bucket_of(&[Value::Int(i)], 3000);
            assert!((1..=3000).contains(&b));
            seen.insert(b);
        }
        assert!(seen.len() > 2900);
        for b in 1..=3000 {
            assert!(shard_of_bucket(b, 3000, 3) < 3);
        }
    }

    #[test]
    fn sql_equal_supported_keys_share_routing_bytes() {
        use crate::types::{compare, SqlType};
        use std::cmp::Ordering;
        // These exercise encoding boundaries as well as signed/unsigned
        // and scaled-decimal probes of the same typed integer key.
        let ty = SqlType::parse("BIGINT UNSIGNED").unwrap();
        for n in (0..1024).chain([65535, 65536, 1 << 32, 9007199254740993, u64::MAX]) {
            let values = [Value::UInt(n), Value::Decimal(format!("{n}.000"))];
            assert_eq!(compare(&values[0], &values[1]), Some(Ordering::Equal));
            let stored = ty.coerce(values[0].clone(), "k").unwrap();
            let probe = crate::exec::coerce_key_part(&ty, &values[1]).unwrap();
            for count in [1, 256, 30000] {
                assert_eq!(
                    bucket_of(std::slice::from_ref(&stored), count),
                    bucket_of(std::slice::from_ref(&probe), count)
                );
            }
            if n <= i64::MAX as u64 {
                assert_eq!(bucket_of(&[Value::Int(n as i64)], 30000), bucket_of(&[stored], 30000));
            }
        }
        for value in ["a", "a ", "a\t", "é", "e\u{301}", "😀", "\0"] {
            let a = Value::str(value);
            let b = Value::Bin(bytes::Bytes::copy_from_slice(value.as_bytes()));
            assert_eq!(compare(&a, &b), Some(Ordering::Equal));
            assert_eq!(bucket_of(&[a], 30000), bucket_of(&[b], 30000));
        }
    }
}
