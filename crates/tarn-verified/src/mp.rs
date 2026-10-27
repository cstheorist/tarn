//! The MessagePack of Tarn's native storage procedures (`tarn-storage`,
//! issue #28), verified with Verus: they write the tuples they store and
//! the replies they send with these functions, and read their arguments
//! with them.
//!
//! Proven here, for every input:
//!   * the writers emit exactly the specified encodings: `write_uint`
//!     `uint_spec(v)`, `write_int` `int_spec(v)` (an integer of int64 or
//!     uint64; it writes nothing for another), `write_array` `array_spec(n)`,
//!     `write_bytes_header` `bytes_header_spec(bin, n)`, `write_double_bits`
//!     `double_spec(bits)`; `patch_array` fills in the header that
//!     `write_array_slot` left;
//!   * those are Tarantool's encodings: the shortest -- no encoding of the
//!     same integer, and no header of an array, a string or a binary of the
//!     same length, is shorter (`lemma_*_shortest`) -- and an integer that is
//!     not negative unsigned;
//!   * the readers decode per `uint_dec`, `int_dec`, `array_dec`, `map_dec`,
//!     `bytes_dec` and `float_dec`, and read back what the writers emit,
//!     whatever follows (`lemma_*_roundtrip`);
//!   * `skip` ends where `values_end` says a value ends, however nested --
//!     an array or a map holds that many values; a string, a binary, a
//!     number or an extension that many bytes -- or returns None for a value
//!     cut short or a byte no value starts with. A value the writers emit
//!     ends where its encoding does, and the elements of an array follow its
//!     header (`lemma_skip_*`); `bytes_dec` agrees with it.
//! No function can panic, overflow or read past its buffer.

use vstd::prelude::*;

verus! {

// ---------------------------------------------------------------------------
// Big-endian numbers
// ---------------------------------------------------------------------------

pub open spec fn byte(v: u64, shift: u64) -> u8 {
    ((v >> shift) & 0xff) as u8
}

pub open spec fn be2(v: u64) -> Seq<u8> {
    seq![byte(v, 8), byte(v, 0)]
}

pub open spec fn be4(v: u64) -> Seq<u8> {
    seq![byte(v, 24), byte(v, 16), byte(v, 8), byte(v, 0)]
}

pub open spec fn be8(v: u64) -> Seq<u8> {
    seq![byte(v, 56), byte(v, 48), byte(v, 40), byte(v, 32), byte(v, 24), byte(v, 16), byte(v, 8), byte(v, 0)]
}

/// The big-endian number in `s[i..i + n]` (n: 1, 2, 4 or 8).
pub open spec fn be_val(s: Seq<u8>, i: int, n: int) -> u64 {
    if n == 1 {
        s[i] as u64
    } else if n == 2 {
        ((s[i] as u64) << 8u64) | (s[i + 1] as u64)
    } else if n == 4 {
        ((s[i] as u64) << 24u64) | ((s[i + 1] as u64) << 16u64) | ((s[i + 2] as u64) << 8u64) | (s[i + 3] as u64)
    } else {
        ((s[i] as u64) << 56u64) | ((s[i + 1] as u64) << 48u64) | ((s[i + 2] as u64) << 40u64)
            | ((s[i + 3] as u64) << 32u64) | ((s[i + 4] as u64) << 24u64) | ((s[i + 5] as u64) << 16u64)
            | ((s[i + 6] as u64) << 8u64) | (s[i + 7] as u64)
    }
}

/// A number of n bytes is less than 2^8n, and less than half of it when its
/// first byte is.
pub proof fn lemma_be_max(s: Seq<u8>, i: int)
    ensures
        be_val(s, i, 1) <= 0xff,
        be_val(s, i, 2) <= 0xffff,
        be_val(s, i, 4) <= 0xffff_ffff,
        s[i] < 0x80 ==> be_val(s, i, 1) < 0x80 && be_val(s, i, 2) < 0x8000 && be_val(s, i, 4) < 0x8000_0000
            && be_val(s, i, 8) < 0x8000_0000_0000_0000,
        s[i] >= 0x80 ==> be_val(s, i, 1) >= 0x80 && be_val(s, i, 2) >= 0x8000 && be_val(s, i, 4) >= 0x8000_0000
            && be_val(s, i, 8) >= 0x8000_0000_0000_0000,
{
    let (a, b, c, d) = (s[i], s[i + 1], s[i + 2], s[i + 3]);
    let (e, f, g, h) = (s[i + 4], s[i + 5], s[i + 6], s[i + 7]);
    assert(((a as u64) << 8u64) | (b as u64) <= 0xffff) by (bit_vector);
    assert(((a as u64) << 24u64) | ((b as u64) << 16u64) | ((c as u64) << 8u64) | (d as u64) <= 0xffff_ffff)
        by (bit_vector);
    assert(a < 0x80 ==> ((a as u64) << 8u64) | (b as u64) < 0x8000) by (bit_vector);
    assert(a >= 0x80 ==> ((a as u64) << 8u64) | (b as u64) >= 0x8000) by (bit_vector);
    assert(a < 0x80 ==> ((a as u64) << 24u64) | ((b as u64) << 16u64) | ((c as u64) << 8u64) | (d as u64)
        < 0x8000_0000) by (bit_vector);
    assert(a >= 0x80 ==> ((a as u64) << 24u64) | ((b as u64) << 16u64) | ((c as u64) << 8u64) | (d as u64)
        >= 0x8000_0000) by (bit_vector);
    assert(a < 0x80 ==> ((a as u64) << 56u64) | ((b as u64) << 48u64) | ((c as u64) << 40u64)
        | ((d as u64) << 32u64) | ((e as u64) << 24u64) | ((f as u64) << 16u64) | ((g as u64) << 8u64)
        | (h as u64) < 0x8000_0000_0000_0000) by (bit_vector);
    assert(a >= 0x80 ==> ((a as u64) << 56u64) | ((b as u64) << 48u64) | ((c as u64) << 40u64)
        | ((d as u64) << 32u64) | ((e as u64) << 24u64) | ((f as u64) << 16u64) | ((g as u64) << 8u64)
        | (h as u64) >= 0x8000_0000_0000_0000) by (bit_vector);
}

/// The big-endian number of `n` bytes (1, 2, 4 or 8) at `buf[i..]`.
#[inline(always)]
pub fn read_be(buf: &[u8], i: usize, n: usize) -> (r: Option<u64>)
    requires
        n == 1 || n == 2 || n == 4 || n == 8,
    ensures
        r matches Some(v) ==> i + n <= buf.len() && v == be_val(buf@, i as int, n as int),
        r is None ==> i + n > buf.len(),
{
    if i > buf.len() || n > buf.len() - i {
        return None;
    }
    if n == 1 {
        Some(buf[i] as u64)
    } else if n == 2 {
        Some(((buf[i] as u64) << 8u64) | (buf[i + 1] as u64))
    } else if n == 4 {
        Some(
            ((buf[i] as u64) << 24u64) | ((buf[i + 1] as u64) << 16u64) | ((buf[i + 2] as u64) << 8u64)
                | (buf[i + 3] as u64),
        )
    } else {
        Some(
            ((buf[i] as u64) << 56u64) | ((buf[i + 1] as u64) << 48u64) | ((buf[i + 2] as u64) << 40u64)
                | ((buf[i + 3] as u64) << 32u64) | ((buf[i + 4] as u64) << 24u64) | ((buf[i + 5] as u64) << 16u64)
                | ((buf[i + 6] as u64) << 8u64) | (buf[i + 7] as u64),
        )
    }
}

/// What `be2`, `be4` and `be8` write reads back.
pub proof fn lemma_be_roundtrip(v: u64, pre: Seq<u8>, rest: Seq<u8>)
    ensures
        v <= 0xffff ==> be_val(pre + be2(v) + rest, pre.len() as int, 2) == v,
        v <= 0xffff_ffff ==> be_val(pre + be4(v) + rest, pre.len() as int, 4) == v,
        be_val(pre + be8(v) + rest, pre.len() as int, 8) == v,
{
    let i = pre.len() as int;
    let s2 = pre + be2(v) + rest;
    let s4 = pre + be4(v) + rest;
    let s8 = pre + be8(v) + rest;
    assert(s2[i] == byte(v, 8) && s2[i + 1] == byte(v, 0));
    assert(s4[i] == byte(v, 24) && s4[i + 1] == byte(v, 16) && s4[i + 2] == byte(v, 8) && s4[i + 3] == byte(v, 0));
    assert(s8[i] == byte(v, 56) && s8[i + 1] == byte(v, 48) && s8[i + 2] == byte(v, 40) && s8[i + 3] == byte(v, 32));
    assert(s8[i + 4] == byte(v, 24) && s8[i + 5] == byte(v, 16) && s8[i + 6] == byte(v, 8) && s8[i + 7] == byte(v, 0));
    assert(v <= 0xffff ==> ((((v >> 8u64) & 0xff) as u8) as u64) << 8u64 | ((((v >> 0u64) & 0xff) as u8) as u64)
        == v) by (bit_vector);
    assert(v <= 0xffff_ffff ==> ((((v >> 24u64) & 0xff) as u8) as u64) << 24u64 | ((((v >> 16u64) & 0xff) as u8)
        as u64) << 16u64 | ((((v >> 8u64) & 0xff) as u8) as u64) << 8u64 | ((((v >> 0u64) & 0xff) as u8) as u64)
        == v) by (bit_vector);
    assert(((((v >> 56u64) & 0xff) as u8) as u64) << 56u64 | ((((v >> 48u64) & 0xff) as u8) as u64) << 48u64
        | ((((v >> 40u64) & 0xff) as u8) as u64) << 40u64 | ((((v >> 32u64) & 0xff) as u8) as u64) << 32u64
        | ((((v >> 24u64) & 0xff) as u8) as u64) << 24u64 | ((((v >> 16u64) & 0xff) as u8) as u64) << 16u64
        | ((((v >> 8u64) & 0xff) as u8) as u64) << 8u64 | ((((v >> 0u64) & 0xff) as u8) as u64) == v)
        by (bit_vector);
}

/// `b`, after what `out` holds.
#[inline(always)]
fn put(out: &mut Vec<u8>, b: &[u8])
    ensures
        final(out)@ == old(out)@ + b@,
{
    let ghost start = out@;
    out.extend_from_slice(b);
    proof {
        assert forall|i: int| 0 <= i < out@.len() implies out@[i] == (start + b@)[i] by {
            if i >= start.len() {
                assert(cloned::<u8>(b@[i - start.len()], out@[i]));
            }
        }
        assert(out@ =~= start + b@);
    }
}

/// A byte `h`, then `v` in 2, 4 or 8 bytes, big-endian.
#[inline(always)]
fn put_be2(out: &mut Vec<u8>, h: u8, v: u64)
    ensures
        final(out)@ == old(out)@ + seq![h] + be2(v),
{
    let a = [h, ((v >> 8u64) & 0xff) as u8, ((v >> 0u64) & 0xff) as u8];
    put(out, &a);
    proof {
        assert(a@ =~= seq![h] + be2(v));
    }
}

#[inline(always)]
fn put_be4(out: &mut Vec<u8>, h: u8, v: u64)
    ensures
        final(out)@ == old(out)@ + seq![h] + be4(v),
{
    let a = [
        h,
        ((v >> 24u64) & 0xff) as u8,
        ((v >> 16u64) & 0xff) as u8,
        ((v >> 8u64) & 0xff) as u8,
        ((v >> 0u64) & 0xff) as u8,
    ];
    put(out, &a);
    proof {
        assert(a@ =~= seq![h] + be4(v));
    }
}

#[inline(always)]
fn put_be8(out: &mut Vec<u8>, h: u8, v: u64)
    ensures
        final(out)@ == old(out)@ + seq![h] + be8(v),
{
    let a = [
        h,
        ((v >> 56u64) & 0xff) as u8,
        ((v >> 48u64) & 0xff) as u8,
        ((v >> 40u64) & 0xff) as u8,
        ((v >> 32u64) & 0xff) as u8,
        ((v >> 24u64) & 0xff) as u8,
        ((v >> 16u64) & 0xff) as u8,
        ((v >> 8u64) & 0xff) as u8,
        ((v >> 0u64) & 0xff) as u8,
    ];
    put(out, &a);
    proof {
        assert(a@ =~= seq![h] + be8(v));
    }
}

// ---------------------------------------------------------------------------
// Integers
// ---------------------------------------------------------------------------

/// The encoding of an unsigned integer: the shortest.
pub open spec fn uint_spec(v: u64) -> Seq<u8> {
    if v <= 0x7f {
        seq![v as u8]
    } else if v <= 0xff {
        seq![0xccu8, v as u8]
    } else if v <= 0xffff {
        seq![0xcdu8] + be2(v)
    } else if v <= 0xffff_ffff {
        seq![0xceu8] + be4(v)
    } else {
        seq![0xcfu8] + be8(v)
    }
}

/// An integer that has an encoding: of int64 or of uint64.
pub open spec fn int_in_range(v: int) -> bool {
    -0x8000_0000_0000_0000 <= v && v <= 0xffff_ffff_ffff_ffff
}

/// The encoding of an integer: unsigned when it is not negative; else its
/// two's complement in the fewest bytes.
pub open spec fn int_spec(v: int) -> Seq<u8>
    recommends
        int_in_range(v),
{
    if v >= 0 {
        uint_spec(v as u64)
    } else if v >= -32 {
        seq![(v + 0x100) as u8]
    } else if v >= -128 {
        seq![0xd0u8, (v + 0x100) as u8]
    } else if v >= -0x8000 {
        seq![0xd1u8] + be2((v + 0x1_0000) as u64)
    } else if v >= -0x8000_0000 {
        seq![0xd2u8] + be4((v + 0x1_0000_0000) as u64)
    } else {
        seq![0xd3u8] + be8((v + 0x1_0000_0000_0000_0000) as u64)
    }
}

#[inline]
pub fn write_uint(out: &mut Vec<u8>, v: u64)
    ensures
        final(out)@ == old(out)@ + uint_spec(v),
{
    let ghost start = out@;
    if v <= 0x7f {
        out.push(v as u8);
    } else if v <= 0xff {
        put(out, &[0xcc, v as u8]);
    } else if v <= 0xffff {
        put_be2(out, 0xcd, v);
    } else if v <= 0xffff_ffff {
        put_be4(out, 0xce, v);
    } else {
        put_be8(out, 0xcf, v);
    }
    proof {
        assert(out@ =~= start + uint_spec(v));
    }
}

/// An integer, as `int_spec` encodes it; false, having written nothing, for
/// one of neither int64 nor uint64.
#[inline]
pub fn write_int(out: &mut Vec<u8>, v: i128) -> (ok: bool)
    ensures
        ok == int_in_range(v as int),
        ok ==> final(out)@ == old(out)@ + int_spec(v as int),
        !ok ==> final(out)@ == old(out)@,
{
    let ghost start = out@;
    if v > 0xffff_ffff_ffff_ffff || v < -0x8000_0000_0000_0000 {
        return false;
    }
    if v >= 0 {
        write_uint(out, v as u64);
    } else if v >= -32 {
        out.push((v + 0x100) as u8);
    } else if v >= -128 {
        put(out, &[0xd0, (v + 0x100) as u8]);
    } else if v >= -0x8000 {
        put_be2(out, 0xd1, (v + 0x1_0000) as u64);
    } else if v >= -0x8000_0000 {
        put_be4(out, 0xd2, (v + 0x1_0000_0000) as u64);
    } else {
        put_be8(out, 0xd3, (v + 0x1_0000_0000_0000_0000) as u64);
    }
    proof {
        assert(out@ =~= start + int_spec(v as int));
    }
    true
}

/// The bytes after the header of an unsigned (0xcc..0xcf) or a signed
/// (0xd0..0xd3) integer, else 0.
pub open spec fn int_width(b: u8) -> int {
    if b == 0xcc || b == 0xd0 {
        1
    } else if b == 0xcd || b == 0xd1 {
        2
    } else if b == 0xce || b == 0xd2 {
        4
    } else if b == 0xcf || b == 0xd3 {
        8
    } else {
        0
    }
}

/// The integer whose two's complement in n bytes is `v`.
pub open spec fn signed(v: u64, n: int) -> int {
    if n == 1 {
        if v >= 0x80 { v - 0x100 } else { v as int }
    } else if n == 2 {
        if v >= 0x8000 { v - 0x1_0000 } else { v as int }
    } else if n == 4 {
        if v >= 0x8000_0000 { v - 0x1_0000_0000 } else { v as int }
    } else {
        if v >= 0x8000_0000_0000_0000 { v - 0x1_0000_0000_0000_0000 } else { v as int }
    }
}

/// An integer of any encoding at `s[i..]`: its value and where it ends.
pub open spec fn int_dec(s: Seq<u8>, i: int) -> Option<(int, int)> {
    if i < 0 || i >= s.len() {
        None
    } else if s[i] <= 0x7f {
        Some((s[i] as int, i + 1))
    } else if s[i] >= 0xe0 {
        Some((s[i] - 0x100, i + 1))
    } else {
        let n = int_width(s[i]);
        if n == 0 || i + 1 + n > s.len() {
            None
        } else if s[i] >= 0xd0 {
            Some((signed(be_val(s, i + 1, n), n), i + 1 + n))
        } else {
            Some((be_val(s, i + 1, n) as int, i + 1 + n))
        }
    }
}

/// An unsigned integer at `s[i..]`, or a signed one that is not negative:
/// its value and where it ends.
pub open spec fn uint_dec(s: Seq<u8>, i: int) -> Option<(u64, int)> {
    if i < 0 || i >= s.len() {
        None
    } else if s[i] <= 0x7f {
        Some((s[i] as u64, i + 1))
    } else {
        let n = int_width(s[i]);
        if n == 0 || i + 1 + n > s.len() || (s[i] >= 0xd0 && s[i + 1] >= 0x80) {
            None
        } else {
            Some((be_val(s, i + 1, n), i + 1 + n))
        }
    }
}

#[inline]
pub fn read_uint(buf: &[u8], i: usize) -> (r: Option<(u64, usize)>)
    ensures
        match (r, uint_dec(buf@, i as int)) {
            (Some((v, end)), Some((sv, send))) => v == sv && end as int == send && end <= buf.len(),
            (None, None) => true,
            _ => false,
        },
{
    if i >= buf.len() {
        return None;
    }
    let b = buf[i];
    proof {
        lemma_be_max(buf@, i as int + 1);
    }
    match b {
        0x00..=0x7f => Some((b as u64, i + 1)),
        0xcc => Some((read_be(buf, i + 1, 1)?, i + 2)),
        0xcd => Some((read_be(buf, i + 1, 2)?, i + 3)),
        0xce => Some((read_be(buf, i + 1, 4)?, i + 5)),
        0xcf => Some((read_be(buf, i + 1, 8)?, i + 9)),
        // (a signed integer: not negative)
        0xd0 => {
            let v = read_be(buf, i + 1, 1)?;
            if v >= 0x80 { None } else { Some((v, i + 2)) }
        },
        0xd1 => {
            let v = read_be(buf, i + 1, 2)?;
            if v >= 0x8000 { None } else { Some((v, i + 3)) }
        },
        0xd2 => {
            let v = read_be(buf, i + 1, 4)?;
            if v >= 0x8000_0000 { None } else { Some((v, i + 5)) }
        },
        0xd3 => {
            let v = read_be(buf, i + 1, 8)?;
            if v >= 0x8000_0000_0000_0000 { None } else { Some((v, i + 9)) }
        },
        _ => None,
    }
}

#[inline]
pub fn read_int(buf: &[u8], i: usize) -> (r: Option<(i128, usize)>)
    ensures
        match (r, int_dec(buf@, i as int)) {
            (Some((v, end)), Some((sv, send))) => v as int == sv && end as int == send && end <= buf.len(),
            (None, None) => true,
            _ => false,
        },
{
    if i >= buf.len() {
        return None;
    }
    let b = buf[i];
    proof {
        lemma_be_max(buf@, i as int + 1);
    }
    match b {
        0x00..=0x7f => Some((b as i128, i + 1)),
        0xe0..=0xff => Some((b as i128 - 0x100, i + 1)),
        0xcc => Some((read_be(buf, i + 1, 1)? as i128, i + 2)),
        0xcd => Some((read_be(buf, i + 1, 2)? as i128, i + 3)),
        0xce => Some((read_be(buf, i + 1, 4)? as i128, i + 5)),
        0xcf => Some((read_be(buf, i + 1, 8)? as i128, i + 9)),
        // two's complement
        0xd0 => {
            let v = read_be(buf, i + 1, 1)? as i128;
            Some((if v >= 0x80 { v - 0x100 } else { v }, i + 2))
        },
        0xd1 => {
            let v = read_be(buf, i + 1, 2)? as i128;
            Some((if v >= 0x8000 { v - 0x1_0000 } else { v }, i + 3))
        },
        0xd2 => {
            let v = read_be(buf, i + 1, 4)? as i128;
            Some((if v >= 0x8000_0000 { v - 0x1_0000_0000 } else { v }, i + 5))
        },
        0xd3 => {
            let v = read_be(buf, i + 1, 8)? as i128;
            Some((if v >= 0x8000_0000_0000_0000 { v - 0x1_0000_0000_0000_0000 } else { v }, i + 9))
        },
        _ => None,
    }
}

/// An unsigned integer, or a signed one that is not negative, is the
/// integer `int_dec` reads.
pub proof fn lemma_uint_is_int(s: Seq<u8>, i: int)
    ensures
        uint_dec(s, i) matches Some((v, e)) ==> int_dec(s, i) == Some((v as int, e)),
{
    if 0 <= i < s.len() && s[i] > 0x7f {
        lemma_be_max(s, i + 1);
    }
}

pub proof fn lemma_uint_roundtrip(v: u64, pre: Seq<u8>, rest: Seq<u8>)
    ensures
        uint_dec(pre + uint_spec(v) + rest, pre.len() as int) == Some((v, (pre.len() + uint_spec(v).len()) as int)),
{
    let i = pre.len() as int;
    let s = pre + uint_spec(v) + rest;
    if v <= 0x7f {
        assert(s[i] == v as u8);
    } else if v <= 0xff {
        assert(s[i] == 0xccu8 && s[i + 1] == v as u8);
    } else if v <= 0xffff {
        let p = pre + seq![0xcdu8];
        assert(s =~= p + be2(v) + rest);
        assert(s[i] == 0xcdu8);
        lemma_be_roundtrip(v, p, rest);
    } else if v <= 0xffff_ffff {
        let p = pre + seq![0xceu8];
        assert(s =~= p + be4(v) + rest);
        assert(s[i] == 0xceu8);
        lemma_be_roundtrip(v, p, rest);
    } else {
        let p = pre + seq![0xcfu8];
        assert(s =~= p + be8(v) + rest);
        assert(s[i] == 0xcfu8);
        lemma_be_roundtrip(v, p, rest);
    }
}

pub proof fn lemma_int_roundtrip(v: int, pre: Seq<u8>, rest: Seq<u8>)
    requires
        int_in_range(v),
    ensures
        int_dec(pre + int_spec(v) + rest, pre.len() as int) == Some((v, (pre.len() + int_spec(v).len()) as int)),
{
    let i = pre.len() as int;
    let s = pre + int_spec(v) + rest;
    if v >= 0 {
        lemma_uint_roundtrip(v as u64, pre, rest);
        lemma_uint_is_int(s, i);
    } else if v >= -32 {
        assert(s[i] == (v + 0x100) as u8);
    } else if v >= -128 {
        assert(s[i] == 0xd0u8 && s[i + 1] == (v + 0x100) as u8);
    } else if v >= -0x8000 {
        let p = pre + seq![0xd1u8];
        assert(s =~= p + be2((v + 0x1_0000) as u64) + rest);
        assert(s[i] == 0xd1u8);
        lemma_be_roundtrip((v + 0x1_0000) as u64, p, rest);
    } else if v >= -0x8000_0000 {
        let p = pre + seq![0xd2u8];
        assert(s =~= p + be4((v + 0x1_0000_0000) as u64) + rest);
        assert(s[i] == 0xd2u8);
        lemma_be_roundtrip((v + 0x1_0000_0000) as u64, p, rest);
    } else {
        let p = pre + seq![0xd3u8];
        assert(s =~= p + be8((v + 0x1_0000_0000_0000_0000) as u64) + rest);
        assert(s[i] == 0xd3u8);
        lemma_be_roundtrip((v + 0x1_0000_0000_0000_0000) as u64, p, rest);
    }
}

/// No encoding of an integer is shorter than `int_spec`'s.
pub proof fn lemma_int_shortest(s: Seq<u8>, i: int)
    ensures
        int_dec(s, i) matches Some((v, e)) ==> int_in_range(v) && int_spec(v).len() <= e - i,
{
    if 0 <= i < s.len() {
        lemma_be_max(s, i + 1);
    }
}

// ---------------------------------------------------------------------------
// Headers of arrays, maps, strings and binaries; doubles
// ---------------------------------------------------------------------------

/// The header of an array of `n` elements: the shortest.
pub open spec fn array_spec(n: u32) -> Seq<u8> {
    if n <= 15 {
        seq![(0x90 + n) as u8]
    } else if n <= 0xffff {
        seq![0xdcu8] + be2(n as u64)
    } else {
        seq![0xddu8] + be4(n as u64)
    }
}

#[inline]
pub fn write_array(out: &mut Vec<u8>, n: u32)
    ensures
        final(out)@ == old(out)@ + array_spec(n),
{
    let ghost start = out@;
    if n <= 15 {
        out.push((0x90 + n) as u8);
    } else if n <= 0xffff {
        put_be2(out, 0xdc, n as u64);
    } else {
        put_be4(out, 0xdd, n as u64);
    }
    proof {
        assert(out@ =~= start + array_spec(n));
    }
}

/// The header of an array at `s[i..]`: its elements, and where it ends.
pub open spec fn array_dec(s: Seq<u8>, i: int) -> Option<(u64, int)> {
    if i < 0 || i >= s.len() {
        None
    } else if 0x90 <= s[i] && s[i] <= 0x9f {
        Some(((s[i] - 0x90) as u64, i + 1))
    } else if s[i] == 0xdc && i + 3 <= s.len() {
        Some((be_val(s, i + 1, 2), i + 3))
    } else if s[i] == 0xdd && i + 5 <= s.len() {
        Some((be_val(s, i + 1, 4), i + 5))
    } else {
        None
    }
}

/// The header of a map at `s[i..]`: its pairs, and where it ends.
pub open spec fn map_dec(s: Seq<u8>, i: int) -> Option<(u64, int)> {
    if i < 0 || i >= s.len() {
        None
    } else if 0x80 <= s[i] && s[i] <= 0x8f {
        Some(((s[i] - 0x80) as u64, i + 1))
    } else if s[i] == 0xde && i + 3 <= s.len() {
        Some((be_val(s, i + 1, 2), i + 3))
    } else if s[i] == 0xdf && i + 5 <= s.len() {
        Some((be_val(s, i + 1, 4), i + 5))
    } else {
        None
    }
}

#[inline]
pub fn read_array(buf: &[u8], i: usize) -> (r: Option<(u32, usize)>)
    ensures
        match (r, array_dec(buf@, i as int)) {
            (Some((n, end)), Some((sn, send))) => n as u64 == sn && end as int == send && end <= buf.len(),
            (None, None) => true,
            _ => false,
        },
{
    if i >= buf.len() {
        return None;
    }
    let b = buf[i];
    proof {
        lemma_be_max(buf@, i as int + 1);
    }
    match b {
        0x90..=0x9f => Some(((b - 0x90) as u32, i + 1)),
        0xdc => Some((read_be(buf, i + 1, 2)? as u32, i + 3)),
        0xdd => Some((read_be(buf, i + 1, 4)? as u32, i + 5)),
        _ => None,
    }
}

#[inline]
pub fn read_map(buf: &[u8], i: usize) -> (r: Option<(u32, usize)>)
    ensures
        match (r, map_dec(buf@, i as int)) {
            (Some((n, end)), Some((sn, send))) => n as u64 == sn && end as int == send && end <= buf.len(),
            (None, None) => true,
            _ => false,
        },
{
    if i >= buf.len() {
        return None;
    }
    let b = buf[i];
    proof {
        lemma_be_max(buf@, i as int + 1);
    }
    match b {
        0x80..=0x8f => Some(((b - 0x80) as u32, i + 1)),
        0xde => Some((read_be(buf, i + 1, 2)? as u32, i + 3)),
        0xdf => Some((read_be(buf, i + 1, 4)? as u32, i + 5)),
        _ => None,
    }
}

pub proof fn lemma_array_roundtrip(n: u32, pre: Seq<u8>, rest: Seq<u8>)
    ensures
        array_dec(pre + array_spec(n) + rest, pre.len() as int) == Some(
            (n as u64, (pre.len() + array_spec(n).len()) as int),
        ),
{
    let i = pre.len() as int;
    let s = pre + array_spec(n) + rest;
    if n <= 15 {
        assert(s[i] == (0x90 + n) as u8);
    } else if n <= 0xffff {
        let p = pre + seq![0xdcu8];
        assert(s =~= p + be2(n as u64) + rest);
        assert(s[i] == 0xdcu8);
        lemma_be_roundtrip(n as u64, p, rest);
    } else {
        let p = pre + seq![0xddu8];
        assert(s =~= p + be4(n as u64) + rest);
        assert(s[i] == 0xddu8);
        lemma_be_roundtrip(n as u64, p, rest);
    }
}

/// No header of an array of as many elements is shorter than `array_spec`'s.
pub proof fn lemma_array_shortest(s: Seq<u8>, i: int)
    ensures
        array_dec(s, i) matches Some((n, e)) ==> n <= 0xffff_ffff && array_spec(n as u32).len() <= e - i,
{
    if 0 <= i < s.len() {
        lemma_be_max(s, i + 1);
    }
}

/// The header of a string (`bin`: false) or a binary of `n` bytes: the
/// shortest.
pub open spec fn bytes_header_spec(bin: bool, n: u32) -> Seq<u8> {
    if !bin && n <= 31 {
        seq![(0xa0 + n) as u8]
    } else if n <= 0xff {
        seq![if bin { 0xc4u8 } else { 0xd9u8 }, n as u8]
    } else if n <= 0xffff {
        seq![if bin { 0xc5u8 } else { 0xdau8 }] + be2(n as u64)
    } else {
        seq![if bin { 0xc6u8 } else { 0xdbu8 }] + be4(n as u64)
    }
}

#[inline]
pub fn write_bytes_header(out: &mut Vec<u8>, bin: bool, n: u32)
    ensures
        final(out)@ == old(out)@ + bytes_header_spec(bin, n),
{
    let ghost start = out@;
    if !bin && n <= 31 {
        out.push((0xa0 + n) as u8);
    } else if n <= 0xff {
        put(out, &[if bin { 0xc4u8 } else { 0xd9u8 }, n as u8]);
    } else if n <= 0xffff {
        put_be2(out, if bin { 0xc5u8 } else { 0xdau8 }, n as u64);
    } else {
        put_be4(out, if bin { 0xc6u8 } else { 0xdbu8 }, n as u64);
    }
    proof {
        assert(out@ =~= start + bytes_header_spec(bin, n));
    }
}

pub open spec fn is_str(b: u8) -> bool {
    (0xa0 <= b && b <= 0xbf) || (0xd9 <= b && b <= 0xdb)
}

pub open spec fn is_bin(b: u8) -> bool {
    0xc4 <= b && b <= 0xc6
}

/// The bytes of a string (`bin`: false) or a binary at `s[i..]`: where they
/// start and end -- as `head` has the value.
pub open spec fn bytes_dec(s: Seq<u8>, i: int, bin: bool) -> Option<(int, int)> {
    if 0 <= i && i < s.len() && (if bin { is_bin(s[i]) } else { is_str(s[i]) }) {
        match head(s, i) {
            Some((h, p, _)) => if i + h + p <= s.len() {
                Some((i + h, i + h + p))
            } else {
                None
            },
            None => None,
        }
    } else {
        None
    }
}

#[inline]
pub fn read_bytes(buf: &[u8], i: usize, bin: bool) -> (r: Option<(usize, usize)>)
    ensures
        match (r, bytes_dec(buf@, i as int, bin)) {
            (Some((start, end)), Some((ss, se))) => start as int == ss && end as int == se && start <= end
                && end <= buf.len(),
            (None, None) => true,
            _ => false,
        },
{
    if i >= buf.len() {
        return None;
    }
    let marker = match buf[i] {
        0xc4..=0xc6 => bin,
        0xa0..=0xbf | 0xd9..=0xdb => !bin,
        _ => false,
    };
    if !marker {
        return None;
    }
    let (h, p, _) = read_head(buf, i)?;
    if h > buf.len() - i || p > (buf.len() - i - h) as u64 {
        None
    } else {
        Some((i + h, i + h + p as usize))
    }
}

pub proof fn lemma_bytes_roundtrip(bin: bool, data: Seq<u8>, pre: Seq<u8>, rest: Seq<u8>)
    requires
        data.len() <= 0xffff_ffff,
    ensures
        ({
            let h = bytes_header_spec(bin, data.len() as u32);
            bytes_dec(pre + h + data + rest, pre.len() as int, bin) == Some(
                ((pre.len() + h.len()) as int, (pre.len() + h.len() + data.len()) as int),
            )
        }),
{
    let n = data.len() as u32;
    let i = pre.len() as int;
    let s = pre + bytes_header_spec(bin, n) + data + rest;
    if !bin && n <= 31 {
        assert(s[i] == (0xa0 + n) as u8);
    } else if n <= 0xff {
        assert(s[i] == (if bin { 0xc4u8 } else { 0xd9u8 }) && s[i + 1] == n as u8);
    } else if n <= 0xffff {
        let p = pre + seq![if bin { 0xc5u8 } else { 0xdau8 }];
        assert(s =~= p + be2(n as u64) + (data + rest));
        assert(s[i] == (if bin { 0xc5u8 } else { 0xdau8 }));
        lemma_be_roundtrip(n as u64, p, data + rest);
    } else {
        let p = pre + seq![if bin { 0xc6u8 } else { 0xdbu8 }];
        assert(s =~= p + be4(n as u64) + (data + rest));
        assert(s[i] == (if bin { 0xc6u8 } else { 0xdbu8 }));
        lemma_be_roundtrip(n as u64, p, data + rest);
    }
}

/// No header of a string or a binary of as many bytes is shorter than
/// `bytes_header_spec`'s.
pub proof fn lemma_bytes_shortest(s: Seq<u8>, i: int, bin: bool)
    ensures
        bytes_dec(s, i, bin) matches Some((start, end)) ==> end - start <= 0xffff_ffff
            && bytes_header_spec(bin, (end - start) as u32).len() <= start - i,
{
    if 0 <= i < s.len() {
        lemma_be_max(s, i + 1);
    }
}

/// A double: 0xcb, then its bits, big-endian.
pub open spec fn double_spec(bits: u64) -> Seq<u8> {
    seq![0xcbu8] + be8(bits)
}

#[inline]
pub fn write_double_bits(out: &mut Vec<u8>, bits: u64)
    ensures
        final(out)@ == old(out)@ + double_spec(bits),
{
    let ghost start = out@;
    put_be8(out, 0xcb, bits);
    proof {
        assert(out@ =~= start + double_spec(bits));
    }
}

/// A float (`double`: false) or a double at `s[i..]`: its bits, and where
/// it ends.
pub open spec fn float_dec(s: Seq<u8>, i: int) -> Option<(u64, bool, int)> {
    if i < 0 || i >= s.len() {
        None
    } else if s[i] == 0xca && i + 5 <= s.len() {
        Some((be_val(s, i + 1, 4), false, i + 5))
    } else if s[i] == 0xcb && i + 9 <= s.len() {
        Some((be_val(s, i + 1, 8), true, i + 9))
    } else {
        None
    }
}

#[inline]
pub fn read_float_bits(buf: &[u8], i: usize) -> (r: Option<(u64, bool, usize)>)
    ensures
        match (r, float_dec(buf@, i as int)) {
            (Some((v, d, end)), Some((sv, sd, send))) => v == sv && d == sd && end as int == send && end <= buf.len(),
            (None, None) => true,
            _ => false,
        },
{
    if i >= buf.len() {
        return None;
    }
    match buf[i] {
        0xca => Some((read_be(buf, i + 1, 4)?, false, i + 5)),
        0xcb => Some((read_be(buf, i + 1, 8)?, true, i + 9)),
        _ => None,
    }
}

pub proof fn lemma_double_roundtrip(bits: u64, pre: Seq<u8>, rest: Seq<u8>)
    ensures
        float_dec(pre + double_spec(bits) + rest, pre.len() as int) == Some((bits, true, (pre.len() + 9) as int)),
{
    let i = pre.len() as int;
    let s = pre + double_spec(bits) + rest;
    let p = pre + seq![0xcbu8];
    assert(s =~= p + be8(bits) + rest);
    assert(s[i] == 0xcbu8);
    lemma_be_roundtrip(bits, p, rest);
}

/// An array header to fill in later (`patch_array`): its longest form.
#[inline]
pub fn write_array_slot(out: &mut Vec<u8>) -> (at: usize)
    ensures
        at == old(out).len(),
        final(out)@ == old(out)@ + seq![0xddu8, 0u8, 0u8, 0u8, 0u8],
{
    let ghost start = out@;
    let at = out.len();
    put(out, &[0xdd, 0, 0, 0, 0]);
    proof {
        assert(out@ =~= start + seq![0xddu8, 0u8, 0u8, 0u8, 0u8]);
    }
    at
}

/// The header of an array of `n` elements at `out[at..at + 5]`, in its
/// longest form; false, having changed nothing, when `out` ends before.
#[inline]
pub fn patch_array(out: &mut Vec<u8>, at: usize, n: u32) -> (ok: bool)
    ensures
        ok == (at + 5 <= old(out).len()),
        ok ==> final(out)@ == old(out)@.subrange(0, at as int) + seq![0xddu8] + be4(n as u64) + old(
            out,
        )@.subrange(at + 5, old(out).len() as int),
        !ok ==> final(out)@ == old(out)@,
{
    if at > out.len() || out.len() - at < 5 {
        return false;
    }
    let ghost start = out@;
    let v = n as u64;
    out.set(at, 0xdd);
    out.set(at + 1, ((v >> 24u64) & 0xff) as u8);
    out.set(at + 2, ((v >> 16u64) & 0xff) as u8);
    out.set(at + 3, ((v >> 8u64) & 0xff) as u8);
    out.set(at + 4, ((v >> 0u64) & 0xff) as u8);
    proof {
        assert(out@ =~= start.subrange(0, at as int) + seq![0xddu8] + be4(v) + start.subrange(
            at + 5,
            start.len() as int,
        ));
    }
    true
}

// ---------------------------------------------------------------------------
// Skipping values
// ---------------------------------------------------------------------------

/// The value whose first byte is `s[i]`: the bytes of its header, the bytes
/// that follow it as its own (a number's, a string's, a binary's, an
/// extension's type and data), and the values nested in it (an array's
/// elements, a map's keys and values). None for a byte no value starts with
/// (0xc1) or a header cut short.
pub open spec fn head(s: Seq<u8>, i: int) -> Option<(int, int, int)> {
    if i < 0 || i >= s.len() {
        None
    } else {
        let b = s[i];
        match b {
            // integers that fit in it, nil, false, true
            0x00..=0x7f | 0xe0..=0xff | 0xc0 | 0xc2 | 0xc3 => Some((1, 0, 0)),
            // maps, arrays and strings of fewer than 16 or 32
            0x80..=0x8f => Some((1, 0, 2 * (b - 0x80))),
            0x90..=0x9f => Some((1, 0, b - 0x90)),
            0xa0..=0xbf => Some((1, b - 0xa0, 0)),
            // integers, floats and extensions of a fixed size
            0xcc | 0xd0 => Some((1, 1, 0)),
            0xcd | 0xd1 | 0xd4 => Some((1, 2, 0)),
            0xd5 => Some((1, 3, 0)),
            0xca | 0xce | 0xd2 => Some((1, 4, 0)),
            0xd6 => Some((1, 5, 0)),
            0xcb | 0xcf | 0xd3 => Some((1, 8, 0)),
            0xd7 => Some((1, 9, 0)),
            0xd8 => Some((1, 17, 0)),
            // binaries, extensions (their type, and their data) and strings
            // of a length of 1, 2 or 4 bytes
            0xc4 | 0xc7 | 0xd9 => if i + 2 <= s.len() {
                Some((2, be_val(s, i + 1, 1) + if b == 0xc7 { 1int } else { 0int }, 0))
            } else {
                None
            },
            0xc5 | 0xc8 | 0xda => if i + 3 <= s.len() {
                Some((3, be_val(s, i + 1, 2) + if b == 0xc8 { 1int } else { 0int }, 0))
            } else {
                None
            },
            0xc6 | 0xc9 | 0xdb => if i + 5 <= s.len() {
                Some((5, be_val(s, i + 1, 4) + if b == 0xc9 { 1int } else { 0int }, 0))
            } else {
                None
            },
            // arrays and maps of a length of 2 or 4 bytes
            0xdc | 0xde => if i + 3 <= s.len() {
                Some((3, 0, be_val(s, i + 1, 2) * if b == 0xde { 2int } else { 1int }))
            } else {
                None
            },
            0xdd | 0xdf => if i + 5 <= s.len() {
                Some((5, 0, be_val(s, i + 1, 4) * if b == 0xdf { 2int } else { 1int }))
            } else {
                None
            },
            // (0xc1)
            _ => None,
        }
    }
}

/// Where `n` values that start at `s[i]`, one after the other, end; None
/// if one of them is cut short or starts with no value's byte.
pub open spec fn values_end(s: Seq<u8>, i: int, n: int) -> Option<int>
    decreases s.len() - i,
{
    if n <= 0 {
        Some(i)
    } else if i < 0 || i >= s.len() {
        None
    } else {
        match head(s, i) {
            None => None,
            Some((h, p, k)) => {
                if h < 1 || p < 0 || k < 0 || i + h + p > s.len() {
                    None
                } else {
                    values_end(s, i + h + p, n - 1 + k)
                }
            },
        }
    }
}

/// A value takes a byte at least.
pub proof fn lemma_values_take_bytes(s: Seq<u8>, i: int, n: int)
    ensures
        values_end(s, i, n) matches Some(e) ==> (n <= 0 ==> e == i) && (n > 0 ==> i + n <= e && e <= s.len()),
    decreases s.len() - i,
{
    if n > 0 && 0 <= i && i < s.len() {
        match head(s, i) {
            Some((h, p, k)) => {
                if h >= 1 && p >= 0 && k >= 0 && i + h + p <= s.len() {
                    lemma_values_take_bytes(s, i + h + p, n - 1 + k);
                }
            },
            None => {},
        }
    }
}

/// `head`: the header's bytes, the value's own bytes, and the values nested
/// in it.
#[inline(always)]
fn read_head(buf: &[u8], i: usize) -> (r: Option<(usize, u64, u64)>)
    ensures
        match (r, head(buf@, i as int)) {
            (Some((h, p, k)), Some((sh, sp, sk))) => h as int == sh && p as int == sp && k as int == sk,
            (None, None) => true,
            _ => false,
        },
{
    if i >= buf.len() {
        return None;
    }
    let b = buf[i];
    proof {
        lemma_be_max(buf@, i as int + 1);
    }
    match b {
        0x00..=0x7f | 0xe0..=0xff | 0xc0 | 0xc2 | 0xc3 => Some((1, 0, 0)),
        0x80..=0x8f => Some((1, 0, 2 * (b - 0x80) as u64)),
        0x90..=0x9f => Some((1, 0, (b - 0x90) as u64)),
        0xa0..=0xbf => Some((1, (b - 0xa0) as u64, 0)),
        0xcc | 0xd0 => Some((1, 1, 0)),
        0xcd | 0xd1 | 0xd4 => Some((1, 2, 0)),
        0xd5 => Some((1, 3, 0)),
        0xca | 0xce | 0xd2 => Some((1, 4, 0)),
        0xd6 => Some((1, 5, 0)),
        0xcb | 0xcf | 0xd3 => Some((1, 8, 0)),
        0xd7 => Some((1, 9, 0)),
        0xd8 => Some((1, 17, 0)),
        0xc4 | 0xc7 | 0xd9 => {
            let v = read_be(buf, i + 1, 1)?;
            Some((2, if b == 0xc7 { v + 1 } else { v }, 0))
        },
        0xc5 | 0xc8 | 0xda => {
            let v = read_be(buf, i + 1, 2)?;
            Some((3, if b == 0xc8 { v + 1 } else { v }, 0))
        },
        0xc6 | 0xc9 | 0xdb => {
            let v = read_be(buf, i + 1, 4)?;
            Some((5, if b == 0xc9 { v + 1 } else { v }, 0))
        },
        0xdc | 0xde => {
            let v = read_be(buf, i + 1, 2)?;
            Some((3, 0, if b == 0xde { 2 * v } else { v }))
        },
        0xdd | 0xdf => {
            let v = read_be(buf, i + 1, 4)?;
            Some((5, 0, if b == 0xdf { 2 * v } else { v }))
        },
        _ => None,
    }
}

/// Where the value whose first byte is `buf[i]` ends: its bytes are
/// `buf[i..end]`.
#[inline]
pub fn skip(buf: &[u8], i: usize) -> (r: Option<usize>)
    ensures
        match r {
            Some(end) => values_end(buf@, i as int, 1) == Some(end as int),
            None => values_end(buf@, i as int, 1) is None,
        },
{
    if i >= buf.len() {
        return None;
    }
    let mut pos = i;
    let mut pending: u64 = 1;
    while pending > 0
        invariant
            i <= pos <= buf.len(),
            values_end(buf@, i as int, 1) == values_end(buf@, pos as int, pending as int),
        decreases buf.len() - pos,
    {
        let (h, p, k) = read_head(buf, pos)?;
        if h > buf.len() - pos || p > (buf.len() - pos - h) as u64 {
            return None;
        }
        let next = pos + h + p as usize;
        // the values left take a byte each at least
        if k > (buf.len() - next) as u64 || pending - 1 > (buf.len() - next) as u64 - k {
            proof {
                lemma_values_take_bytes(buf@, next as int, pending - 1 + k);
            }
            return None;
        }
        pos = next;
        pending = pending - 1 + k;
    }
    Some(pos)
}

/// An integer, a double, a string or a binary the writers emit ends where
/// its encoding does.
pub proof fn lemma_skip_int(v: int, pre: Seq<u8>, rest: Seq<u8>)
    requires
        int_in_range(v),
    ensures
        values_end(pre + int_spec(v) + rest, pre.len() as int, 1) == Some((pre.len() + int_spec(v).len()) as int),
{
    let e = (pre.len() + int_spec(v).len()) as int;
    lemma_int_roundtrip(v, pre, rest);
    assert(values_end(pre + int_spec(v) + rest, e, 0) == Some(e));
}

pub proof fn lemma_skip_double(bits: u64, pre: Seq<u8>, rest: Seq<u8>)
    ensures
        values_end(pre + double_spec(bits) + rest, pre.len() as int, 1) == Some((pre.len() + 9) as int),
{
    let s = pre + double_spec(bits) + rest;
    assert(s[pre.len() as int] == 0xcbu8);
    assert(values_end(s, pre.len() + 9int, 0) == Some(pre.len() + 9int));
}

pub proof fn lemma_skip_bytes(bin: bool, data: Seq<u8>, pre: Seq<u8>, rest: Seq<u8>)
    requires
        data.len() <= 0xffff_ffff,
    ensures
        ({
            let h = bytes_header_spec(bin, data.len() as u32);
            values_end(pre + h + data + rest, pre.len() as int, 1) == Some((pre.len() + h.len() + data.len()) as int)
        }),
{
    let h = bytes_header_spec(bin, data.len() as u32);
    let e = (pre.len() + h.len() + data.len()) as int;
    lemma_bytes_roundtrip(bin, data, pre, rest);
    assert(values_end(pre + h + data + rest, e, 0) == Some(e));
}

/// The elements of an array follow its header.
pub proof fn lemma_skip_array(n: u32, pre: Seq<u8>, rest: Seq<u8>)
    ensures
        ({
            let s = pre + array_spec(n) + rest;
            values_end(s, pre.len() as int, 1) == values_end(s, (pre.len() + array_spec(n).len()) as int, n as int)
        }),
{
    lemma_array_roundtrip(n, pre, rest);
}

} // verus!
