//! Shard routing, verified with Verus.
//!
//! Proven here:
//!   * `crc32c_update` computes exactly the bitwise CRC-32C fold `crc_bytes`
//!     (reflected polynomial 0x82F63B78, no final xor - Tarantool's
//!     `digest.crc32`), for every input, without overflow or panics;
//!   * `bucket_from_crc` returns a bucket in 1..=bucket_count;
//!   * `shard_of_bucket` returns a shard in 0..nshards, is monotone in the
//!     bucket (so shards own contiguous bucket ranges), and every shard owns
//!     at least one bucket - i.e. the map is a total function onto all shards,
//!     which is the partition property routing relies on: every row has
//!     exactly one owning shard, and no shard is left without buckets.

use vstd::prelude::*;

verus! {

pub const CRC_INIT: u32 = 0xFFFF_FFFF;
pub const POLY: u32 = 0x82F6_3B78;

/// One shift step of the reflected CRC.
pub open spec fn bit_step(c: u32) -> u32 {
    if c & 1 == 1 { (c >> 1) ^ POLY } else { c >> 1 }
}

/// Eight shift steps: the effect of one input byte already xor-ed into c.
pub open spec fn byte_step(c: u32) -> u32 {
    bit_step(bit_step(bit_step(bit_step(bit_step(bit_step(bit_step(bit_step(c))))))))
}

/// CRC of a byte sequence, folded left to right from state `c`.
pub open spec fn crc_bytes(c: u32, s: Seq<u8>) -> u32
    decreases s.len(),
{
    if s.len() == 0 {
        c
    } else {
        byte_step(crc_bytes(c, s.drop_last()) ^ (s.last() as u32))
    }
}

fn bit_step_exec(c: u32) -> (r: u32)
    ensures r == bit_step(c),
{
    if c & 1 == 1 { (c >> 1) ^ POLY } else { c >> 1 }
}

fn byte_step_exec(c: u32) -> (r: u32)
    ensures r == byte_step(c),
{
    let c1 = bit_step_exec(c);
    let c2 = bit_step_exec(c1);
    let c3 = bit_step_exec(c2);
    let c4 = bit_step_exec(c3);
    let c5 = bit_step_exec(c4);
    let c6 = bit_step_exec(c5);
    let c7 = bit_step_exec(c6);
    bit_step_exec(c7)
}

/// Continue a CRC-32C computation over `data`.
pub fn crc32c_update(crc: u32, data: &[u8]) -> (r: u32)
    ensures r == crc_bytes(crc, data@),
{
    let mut c = crc;
    let mut i: usize = 0;
    while i < data.len()
        invariant
            i <= data.len(),
            c == crc_bytes(crc, data@.subrange(0, i as int)),
        decreases data.len() - i,
    {
        proof {
            let pre = data@.subrange(0, i as int);
            let next = data@.subrange(0, i as int + 1);
            assert(next.drop_last() =~= pre);
            assert(next.last() == data@[i as int]);
        }
        c = byte_step_exec(c ^ (data[i] as u32));
        i = i + 1;
    }
    proof {
        assert(data@.subrange(0, data@.len() as int) =~= data@);
    }
    c
}

/// Bucket ids are 1-based, as in vshard.
pub fn bucket_from_crc(crc: u32, bucket_count: u32) -> (b: u32)
    requires bucket_count > 0,
    ensures
        1 <= b <= bucket_count,
        b == crc % bucket_count + 1,
{
    crc % bucket_count + 1
}

pub open spec fn shard_spec(bucket: int, bucket_count: int, nshards: int) -> int {
    ((bucket - 1) * nshards) / bucket_count
}

/// Owning shard (0-based) of a bucket: contiguous, near-equal ranges.
pub fn shard_of_bucket(bucket: u32, bucket_count: u32, nshards: u32) -> (s: u32)
    requires
        bucket_count > 0,
        nshards > 0,
        1 <= bucket <= bucket_count,
    ensures
        s < nshards,
        s as int == shard_spec(bucket as int, bucket_count as int, nshards as int),
{
    proof {
        assert(((bucket as int) - 1) * (nshards as int) <= 0xFFFF_FFFFint * 0xFFFF_FFFFint) by (nonlinear_arith)
            requires 1 <= bucket <= 0xFFFF_FFFF, nshards <= 0xFFFF_FFFF;
    }
    let num: u64 = (bucket as u64 - 1) * (nshards as u64);
    proof {
        assert((bucket as int - 1) * (nshards as int) < (bucket_count as int) * (nshards as int)) by (nonlinear_arith)
            requires 1 <= bucket <= bucket_count, nshards > 0;
        assert(((bucket as int - 1) * (nshards as int)) / (bucket_count as int) < nshards as int) by (nonlinear_arith)
            requires
                (bucket as int - 1) * (nshards as int) < (bucket_count as int) * (nshards as int),
                bucket_count > 0,
                (bucket as int - 1) * (nshards as int) >= 0;
        assert((bucket as int - 1) * (nshards as int) >= 0) by (nonlinear_arith)
            requires 1 <= bucket, nshards > 0;
    }
    let s = num / (bucket_count as u64);
    s as u32
}

/// Shards own contiguous bucket ranges: the map is monotone.
pub proof fn lemma_shard_monotone(b1: int, b2: int, bucket_count: int, nshards: int)
    requires
        bucket_count > 0,
        nshards > 0,
        1 <= b1 <= b2 <= bucket_count,
    ensures
        shard_spec(b1, bucket_count, nshards) <= shard_spec(b2, bucket_count, nshards),
{
    assert((b1 - 1) * nshards <= (b2 - 1) * nshards) by (nonlinear_arith)
        requires b1 <= b2, nshards > 0;
    vstd::arithmetic::div_mod::lemma_div_is_ordered((b1 - 1) * nshards, (b2 - 1) * nshards, bucket_count);
}

/// Every shard owns at least one bucket (when there are at least as many
/// buckets as shards): the first bucket of shard k is ceil(k*B/N)+1.
pub proof fn lemma_every_shard_owns_a_bucket(k: int, bucket_count: int, nshards: int)
    requires
        nshards > 0,
        bucket_count >= nshards,
        0 <= k < nshards,
    ensures
        exists|b: int| 1 <= b <= bucket_count && #[trigger] shard_spec(b, bucket_count, nshards) == k,
{
    // b - 1 = ceil(k * B / N)
    let q = (k * bucket_count + nshards - 1) / nshards;
    let b = q + 1;
    assert(k * bucket_count >= 0) by (nonlinear_arith) requires k >= 0, bucket_count >= 0;
    assert(q * nshards >= k * bucket_count) by {
        vstd::arithmetic::div_mod::lemma_fundamental_div_mod(k * bucket_count + nshards - 1, nshards);
        vstd::arithmetic::div_mod::lemma_mod_bound(k * bucket_count + nshards - 1, nshards);
    }
    assert(q * nshards < k * bucket_count + nshards) by {
        vstd::arithmetic::div_mod::lemma_fundamental_div_mod(k * bucket_count + nshards - 1, nshards);
        vstd::arithmetic::div_mod::lemma_mod_bound(k * bucket_count + nshards - 1, nshards);
    }
    // 0 <= q < B, so b is a valid bucket
    assert(q >= 0) by {
        vstd::arithmetic::div_mod::lemma_div_pos_is_pos(k * bucket_count + nshards - 1, nshards);
    }
    assert(q < bucket_count) by (nonlinear_arith)
        requires
            q * nshards < k * bucket_count + nshards,
            k < nshards,
            bucket_count >= nshards,
            nshards > 0,
            k >= 0;
    // floor(q * N / B) == k because k*B <= q*N < k*B + N <= (k+1)*B
    assert(q * nshards < (k + 1) * bucket_count) by (nonlinear_arith)
        requires q * nshards < k * bucket_count + nshards, bucket_count >= nshards;
    assert((q * nshards) / bucket_count == k) by {
        vstd::arithmetic::div_mod::lemma_div_is_ordered(k * bucket_count, q * nshards, bucket_count);
        vstd::arithmetic::div_mod::lemma_div_multiples_vanish(k, bucket_count);
        assert((k * bucket_count) / bucket_count == k) by {
            vstd::arithmetic::div_mod::lemma_div_by_multiple(k, bucket_count);
        }
        vstd::arithmetic::div_mod::lemma_div_is_ordered(q * nshards, (k + 1) * bucket_count - 1, bucket_count);
        assert(((k + 1) * bucket_count - 1) / bucket_count == k) by {
            vstd::arithmetic::div_mod::lemma_fundamental_div_mod((k + 1) * bucket_count - 1, bucket_count);
            vstd::arithmetic::div_mod::lemma_mod_bound((k + 1) * bucket_count - 1, bucket_count);
            assert(((k + 1) * bucket_count - 1) == k * bucket_count + (bucket_count - 1)) by (nonlinear_arith);
            vstd::arithmetic::div_mod::lemma_fundamental_div_mod_converse((k + 1) * bucket_count - 1, bucket_count, k, bucket_count - 1);
        }
    }
    assert(shard_spec(b, bucket_count, nshards) == k);
}

} // verus!
