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
//!     exactly one owning shard, and no shard is left without buckets. This
//!     is the layout of a new cluster; buckets move from there (issue #24);
//!   * `build_owner` turns the bucket ranges the shards report owning into
//!     the owner of each bucket, or refuses them when two reports claim one
//!     bucket: every bucket a report holds maps to that report's shard, and
//!     a bucket no report holds (one between the send and the receive of a
//!     move) to `NO_SHARD`.

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

/// The owner of a bucket that no shard owns: between the send and the
/// receive of a move (lua/tarn.lua "Bucket moves").
pub const NO_SHARD: u32 = 0xFFFF_FFFF;

/// A shard's report `r = (shard, lo, hi)` holds bucket `b`: lo <= b <= hi.
pub open spec fn holds(r: (u32, u32, u32), b: int) -> bool {
    r.1 as int <= b && b <= r.2 as int
}

/// The owner of each bucket, `owner[b - 1]`, from the ranges the shards
/// report owning: `ranges[i] = (shard, lo, hi)`, buckets lo..=hi. None when
/// a range is empty, outside 1..=count or names NO_SHARD, or when two
/// ranges share a bucket: the reports do not give each bucket one owner.
pub fn build_owner(count: u32, ranges: &[(u32, u32, u32)]) -> (r: Option<Vec<u32>>)
    requires
        count < 0xFFFF_FFFF,
    ensures
        match r {
            Some(o) => {
                &&& o@.len() == count
                // a bucket a report holds is that report's shard's ...
                &&& forall|b: int, i: int|
                    #![trigger o@[b], ranges@[i]]
                    0 <= b < count && 0 <= i < ranges@.len() && holds(ranges@[i], b + 1) ==> o@[b]
                        == ranges@[i].0
                // ... and no report holding it is NO_SHARD's
                &&& forall|b: int|
                    #![trigger o@[b]]
                    0 <= b < count ==> (o@[b] == NO_SHARD <==> forall|i: int|
                        0 <= i < ranges@.len() ==> !holds(#[trigger] ranges@[i], b + 1))
            },
            None => true,
        },
{
    let mut owner: Vec<u32> = Vec::new();
    let mut k: u32 = 0;
    while k < count
        invariant
            k <= count,
            owner@.len() == k,
            forall|x: int| 0 <= x < k ==> owner@[x] == NO_SHARD,
        decreases count - k,
    {
        owner.push(NO_SHARD);
        k = k + 1;
    }
    let mut i: usize = 0;
    while i < ranges.len()
        invariant
            i <= ranges@.len(),
            count < 0xFFFF_FFFF,
            owner@.len() == count,
            forall|x: int, j: int|
                #![trigger owner@[x], ranges@[j]]
                0 <= x < count && 0 <= j < i && holds(ranges@[j], x + 1) ==> owner@[x] == ranges@[j].0,
            forall|x: int|
                #![trigger owner@[x]]
                0 <= x < count ==> (owner@[x] == NO_SHARD <==> forall|j: int|
                    0 <= j < i ==> !holds(#[trigger] ranges@[j], x + 1)),
        decreases ranges@.len() - i,
    {
        let (s, lo, hi) = ranges[i];
        if s == NO_SHARD || lo < 1 || lo > hi || hi > count {
            return None;
        }
        let ghost before = owner@;
        let mut b: u32 = lo;
        while b <= hi
            invariant
                1 <= lo <= b,
                b <= hi + 1,
                hi <= count,
                count < 0xFFFF_FFFF,
                s != NO_SHARD,
                owner@.len() == count,
                before.len() == count,
                forall|x: int|
                    #![trigger owner@[x]]
                    0 <= x < count && !(lo - 1 <= x && x < b - 1) ==> owner@[x] == before[x],
                forall|x: int|
                    #![trigger owner@[x]]
                    lo - 1 <= x && x < b - 1 ==> owner@[x] == s && before[x] == NO_SHARD,
            decreases hi + 1 - b,
        {
            if owner[(b - 1) as usize] != NO_SHARD {
                return None;  // another report holds bucket b too
            }
            owner.set((b - 1) as usize, s);
            b = b + 1;
        }
        proof {
            let r = ranges@[i as int];
            assert(r == (s, lo, hi));
            assert forall|x: int, j: int|
                #![trigger owner@[x], ranges@[j]]
                0 <= x < count && 0 <= j < i + 1 && holds(ranges@[j], x + 1) implies owner@[x]
                    == ranges@[j].0 by {
                if j == i as int {
                    assert(lo - 1 <= x && x < hi);
                } else if lo - 1 <= x && x < hi {
                    // before[x] == NO_SHARD: no earlier report holds x
                    assert(owner@[x] == s);
                    assert(before[x] == NO_SHARD);
                    assert(!holds(ranges@[j], x + 1));
                }
            }
            assert forall|x: int|
                #![trigger owner@[x]]
                0 <= x < count implies (owner@[x] == NO_SHARD <==> forall|j: int|
                    0 <= j < i + 1 ==> !holds(#[trigger] ranges@[j], x + 1)) by {
                if lo - 1 <= x && x < hi {
                    assert(holds(ranges@[i as int], x + 1));
                } else {
                    assert(!holds(ranges@[i as int], x + 1));
                    assert(owner@[x] == before[x]);
                    if owner@[x] == NO_SHARD {
                        assert forall|j: int| 0 <= j < i + 1 implies !holds(#[trigger] ranges@[j], x + 1) by {
                            if j < i as int {
                                assert(!holds(ranges@[j], x + 1));
                            }
                        }
                    } else {
                        let j0 = choose|j: int| 0 <= j < i && holds(#[trigger] ranges@[j], x + 1);
                        assert(0 <= j0 < i + 1 && holds(ranges@[j0], x + 1));
                    }
                }
            }
        }
        i = i + 1;
    }
    Some(owner)
}

} // verus!
