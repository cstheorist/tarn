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
        assert_eq!(tarn_verified::route::crc32c_update(tarn_verified::route::CRC_INIT, b"123456789"), 486108540);
        assert_eq!(tarn_verified::route::crc32c_update(tarn_verified::route::CRC_INIT, b""), 0xFFFF_FFFF);
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
}
