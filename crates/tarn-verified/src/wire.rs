//! MySQL wire-format primitives, verified with Verus.
//!
//! Proven here, for every input:
//!   * length-encoded integers: `write_lenenc_int` emits exactly
//!     `lenenc_spec(n)` (the minimal encoding), `lenenc_int_len` is its
//!     length, and `read_lenenc_int` decodes per `decode_spec`; decoding an
//!     encoding followed by arbitrary bytes yields the original value and the
//!     exact number of bytes consumed (round trip);
//!   * packet framing: `frame_packets` emits `frames_spec(payload, seq)`:
//!     chunks of 0xFFFFFF bytes, each with a 3-byte length and a wrapping
//!     sequence id, and a final short (possibly empty) packet - so a payload
//!     whose length is an exact multiple of 0xFFFFFF is terminated by an
//!     empty packet, the classic MySQL 16 MiB boundary bug;
//!     `deframe_spec(frames_spec(p, s) ++ rest) == p` (reassembly inverts
//!     framing), and the exec `deframe` implements `deframe_spec`.
//! None of the functions can panic or index out of bounds.

use vstd::prelude::*;

verus! {

// ---------------------------------------------------------------------------
// Length-encoded integers
// ---------------------------------------------------------------------------

pub open spec fn byte(n: u64, shift: u64) -> u8 {
    ((n >> shift) & 0xff) as u8
}

pub open spec fn lenenc_spec(n: u64) -> Seq<u8> {
    if n < 251 {
        seq![n as u8]
    } else if n < 0x1_0000 {
        seq![0xFCu8, byte(n, 0), byte(n, 8)]
    } else if n < 0x100_0000 {
        seq![0xFDu8, byte(n, 0), byte(n, 8), byte(n, 16)]
    } else {
        seq![0xFEu8, byte(n, 0), byte(n, 8), byte(n, 16), byte(n, 24), byte(n, 32), byte(n, 40), byte(n, 48), byte(n, 56)]
    }
}

pub open spec fn le2(a: u8, b: u8) -> u64 {
    (a as u64) | ((b as u64) << 8)
}

pub open spec fn le3(a: u8, b: u8, c: u8) -> u64 {
    (a as u64) | ((b as u64) << 8) | ((c as u64) << 16)
}

pub open spec fn le8(s: Seq<u8>) -> u64
    recommends s.len() >= 8,
{
    (s[0] as u64) | ((s[1] as u64) << 8) | ((s[2] as u64) << 16) | ((s[3] as u64) << 24)
        | ((s[4] as u64) << 32) | ((s[5] as u64) << 40) | ((s[6] as u64) << 48) | ((s[7] as u64) << 56)
}

/// Decoding of a length-encoded integer at the start of `s`: the value and
/// the number of bytes it occupies; None for NULL (0xFB), the error marker
/// (0xFF) or a truncated buffer.
pub open spec fn decode_spec(s: Seq<u8>) -> Option<(u64, nat)> {
    if s.len() == 0 {
        None
    } else if s[0] < 0xFB {
        Some((s[0] as u64, 1))
    } else if s[0] == 0xFC && s.len() >= 3 {
        Some((le2(s[1], s[2]), 3))
    } else if s[0] == 0xFD && s.len() >= 4 {
        Some((le3(s[1], s[2], s[3]), 4))
    } else if s[0] == 0xFE && s.len() >= 9 {
        Some((le8(s.subrange(1, 9)), 9))
    } else {
        None
    }
}

pub proof fn lemma_lenenc_roundtrip(n: u64, rest: Seq<u8>)
    ensures
        decode_spec(lenenc_spec(n) + rest) == Some((n, lenenc_spec(n).len())),
{
    let e = lenenc_spec(n);
    let s = e + rest;
    assert((n >> 0u64) == n) by (bit_vector);
    if n < 251 {
        assert(s[0] == n as u8);
        assert((n as u8) as u64 == n) by (bit_vector) requires n < 251;
    } else if n < 0x1_0000 {
        assert(s[0] == 0xFCu8 && s[1] == byte(n, 0) && s[2] == byte(n, 8));
        assert(((n & 0xff) as u8) as u64 | ((((n >> 8u64) & 0xff) as u8) as u64) << 8 == n) by (bit_vector)
            requires n < 0x1_0000;
    } else if n < 0x100_0000 {
        assert(s[0] == 0xFDu8 && s[1] == byte(n, 0) && s[2] == byte(n, 8) && s[3] == byte(n, 16));
        assert(((n & 0xff) as u8) as u64 | ((((n >> 8u64) & 0xff) as u8) as u64) << 8
            | ((((n >> 16u64) & 0xff) as u8) as u64) << 16 == n) by (bit_vector)
            requires n < 0x100_0000;
    } else {
        let t = s.subrange(1, 9);
        assert(s[0] == 0xFEu8);
        assert(t[0] == byte(n, 0) && t[1] == byte(n, 8) && t[2] == byte(n, 16) && t[3] == byte(n, 24));
        assert(t[4] == byte(n, 32) && t[5] == byte(n, 40) && t[6] == byte(n, 48) && t[7] == byte(n, 56));
        assert(((n & 0xff) as u8) as u64 | ((((n >> 8u64) & 0xff) as u8) as u64) << 8
            | ((((n >> 16u64) & 0xff) as u8) as u64) << 16 | ((((n >> 24u64) & 0xff) as u8) as u64) << 24
            | ((((n >> 32u64) & 0xff) as u8) as u64) << 32 | ((((n >> 40u64) & 0xff) as u8) as u64) << 40
            | ((((n >> 48u64) & 0xff) as u8) as u64) << 48 | ((((n >> 56u64) & 0xff) as u8) as u64) << 56 == n) by (bit_vector);
    }
}

pub fn lenenc_int_len(n: u64) -> (l: usize)
    ensures l == lenenc_spec(n).len(),
{
    if n < 251 { 1 } else if n < 0x1_0000 { 3 } else if n < 0x100_0000 { 4 } else { 9 }
}

pub fn write_lenenc_int(out: &mut Vec<u8>, n: u64)
    ensures final(out)@ == old(out)@ + lenenc_spec(n),
{
    let ghost start = out@;
    proof { assert((n >> 0u64) == n) by (bit_vector); }
    if n < 251 {
        out.push(n as u8);
    } else if n < 0x1_0000 {
        out.push(0xFC);
        out.push(((n >> 0) & 0xff) as u8);
        out.push(((n >> 8) & 0xff) as u8);
    } else if n < 0x100_0000 {
        out.push(0xFD);
        out.push(((n >> 0) & 0xff) as u8);
        out.push(((n >> 8) & 0xff) as u8);
        out.push(((n >> 16) & 0xff) as u8);
    } else {
        out.push(0xFE);
        out.push(((n >> 0) & 0xff) as u8);
        out.push(((n >> 8) & 0xff) as u8);
        out.push(((n >> 16) & 0xff) as u8);
        out.push(((n >> 24) & 0xff) as u8);
        out.push(((n >> 32) & 0xff) as u8);
        out.push(((n >> 40) & 0xff) as u8);
        out.push(((n >> 48) & 0xff) as u8);
        out.push(((n >> 56) & 0xff) as u8);
    }
    proof { assert(out@ =~= start + lenenc_spec(n)); }
}

pub fn read_lenenc_int(buf: &[u8]) -> (r: Option<(u64, usize)>)
    ensures
        match (r, decode_spec(buf@)) {
            (Some((v, k)), Some((sv, sk))) => v == sv && k as nat == sk && k <= buf.len(),
            (None, None) => true,
            _ => false,
        },
{
    if buf.len() == 0 {
        return None;
    }
    let b0 = buf[0];
    if b0 < 0xFB {
        Some((b0 as u64, 1))
    } else if b0 == 0xFC && buf.len() >= 3 {
        Some(((buf[1] as u64) | ((buf[2] as u64) << 8), 3))
    } else if b0 == 0xFD && buf.len() >= 4 {
        Some(((buf[1] as u64) | ((buf[2] as u64) << 8) | ((buf[3] as u64) << 16), 4))
    } else if b0 == 0xFE && buf.len() >= 9 {
        let v = (buf[1] as u64) | ((buf[2] as u64) << 8) | ((buf[3] as u64) << 16) | ((buf[4] as u64) << 24)
            | ((buf[5] as u64) << 32) | ((buf[6] as u64) << 40) | ((buf[7] as u64) << 48) | ((buf[8] as u64) << 56);
        proof {
            let t = buf@.subrange(1, 9);
            assert(t[0] == buf@[1] && t[1] == buf@[2] && t[2] == buf@[3] && t[3] == buf@[4]);
            assert(t[4] == buf@[5] && t[5] == buf@[6] && t[6] == buf@[7] && t[7] == buf@[8]);
        }
        Some((v, 9))
    } else {
        None
    }
}

/// A length-encoded string: lenenc length followed by the bytes.
pub fn write_lenenc_str(out: &mut Vec<u8>, s: &[u8])
    ensures final(out)@ == old(out)@ + lenenc_spec(s.len() as u64) + s@,
{
    write_lenenc_int(out, s.len() as u64);
    let ghost mid = out@;
    let mut i: usize = 0;
    while i < s.len()
        invariant
            i <= s.len(),
            out@ == mid + s@.subrange(0, i as int),
        decreases s.len() - i,
    {
        out.push(s[i]);
        proof { assert(out@ =~= mid + s@.subrange(0, i as int + 1)); }
        i = i + 1;
    }
    proof { assert(s@.subrange(0, s.len() as int) =~= s@); }
}

/// Decode a length-encoded string; returns (start, len) of its bytes inside
/// `buf` and the total bytes consumed, never reading out of bounds.
pub fn read_lenenc_str_bounds(buf: &[u8]) -> (r: Option<(usize, usize, usize)>)
    ensures
        r matches Some((start, len, consumed)) ==> {
            &&& decode_spec(buf@) matches Some((v, k))
            &&& start as nat == k && len as u64 == v
            &&& consumed == start + len && consumed <= buf.len()
        },
{
    match read_lenenc_int(buf) {
        None => None,
        Some((n, k)) => {
            if n > (buf.len() - k) as u64 {
                None
            } else {
                Some((k, n as usize, k + n as usize))
            }
        },
    }
}

// ---------------------------------------------------------------------------
// Packet framing
// ---------------------------------------------------------------------------

pub const MAX_PAYLOAD: usize = 0xFF_FFFF;

pub open spec fn header(len: nat, seq: u8) -> Seq<u8>
    recommends len <= MAX_PAYLOAD,
{
    seq![(len % 256) as u8, ((len / 256) % 256) as u8, ((len / 65536) % 256) as u8, seq]
}

pub open spec fn next_seq(s: u8) -> u8 {
    if s == 255 { 0 } else { (s + 1) as u8 }
}

/// Wire bytes for one logical payload starting at sequence id `seq`.
pub open spec fn frames_spec(p: Seq<u8>, seq: u8) -> Seq<u8>
    decreases p.len(),
{
    if p.len() < MAX_PAYLOAD {
        header(p.len(), seq) + p
    } else {
        header(MAX_PAYLOAD as nat, seq) + p.subrange(0, MAX_PAYLOAD as int)
            + frames_spec(p.subrange(MAX_PAYLOAD as int, p.len() as int), next_seq(seq))
    }
}

/// Number of packets used for a payload of length n.
pub open spec fn packet_count(n: nat) -> nat
    decreases n,
{
    if n < MAX_PAYLOAD { 1 } else { 1 + packet_count((n - MAX_PAYLOAD) as nat) }
}

pub open spec fn seq_after(seq: u8, k: nat) -> u8
    decreases k,
{
    if k == 0 { seq } else { seq_after(next_seq(seq), (k - 1) as nat) }
}

pub open spec fn header_len(w: Seq<u8>) -> nat
    recommends w.len() >= 4,
{
    (w[0] as nat) + (w[1] as nat) * 256 + (w[2] as nat) * 65536
}

/// Reassembly: read packets until one shorter than MAX_PAYLOAD; returns the
/// payload and the number of wire bytes consumed.
pub open spec fn deframe_spec(w: Seq<u8>) -> Option<(Seq<u8>, nat)>
    decreases w.len(),
{
    if w.len() < 4 {
        None
    } else {
        let len = header_len(w);
        if w.len() < 4 + len {
            None
        } else if len < MAX_PAYLOAD {
            Some((w.subrange(4, 4 + len as int), 4 + len))
        } else {
            match deframe_spec(w.subrange(4 + len as int, w.len() as int)) {
                Some((rest, used)) => Some((w.subrange(4, 4 + len as int) + rest, 4 + len + used)),
                None => None,
            }
        }
    }
}

proof fn lemma_header_len(len: nat, seq: u8)
    requires len <= MAX_PAYLOAD,
    ensures header(len, seq).len() == 4, header_len(header(len, seq)) == len,
{
    let h = header(len, seq);
    assert(h[0] as nat == len % 256);
    assert(h[1] as nat == (len / 256) % 256);
    assert(h[2] as nat == (len / 65536) % 256);
    assert(len % 256 + ((len / 256) % 256) * 256 + ((len / 65536) % 256) * 65536 == len) by (nonlinear_arith)
        requires len <= 0xFF_FFFF;
}

pub proof fn lemma_deframe_frames(p: Seq<u8>, seq: u8, rest: Seq<u8>)
    ensures deframe_spec(frames_spec(p, seq) + rest) == Some((p, frames_spec(p, seq).len())),
    decreases p.len(),
{
    let w = frames_spec(p, seq) + rest;
    if p.len() < MAX_PAYLOAD {
        lemma_header_len(p.len(), seq);
        assert(w.subrange(4, 4 + p.len() as int) =~= p);
    } else {
        let head = p.subrange(0, MAX_PAYLOAD as int);
        let tail = p.subrange(MAX_PAYLOAD as int, p.len() as int);
        lemma_header_len(MAX_PAYLOAD as nat, seq);
        lemma_deframe_frames(tail, next_seq(seq), rest);
        let ft = frames_spec(tail, next_seq(seq));
        assert(frames_spec(p, seq) == header(MAX_PAYLOAD as nat, seq) + head + ft);
        assert(w.subrange(4, 4 + MAX_PAYLOAD as int) =~= head);
        assert(w.subrange(4 + MAX_PAYLOAD as int, w.len() as int) =~= ft + rest);
        assert(head + tail =~= p);
        assert(header_len(w) == MAX_PAYLOAD) by {
            assert(w[0] == header(MAX_PAYLOAD as nat, seq)[0]);
            assert(w[1] == header(MAX_PAYLOAD as nat, seq)[1]);
            assert(w[2] == header(MAX_PAYLOAD as nat, seq)[2]);
        }
    }
}

fn header_exec(out: &mut Vec<u8>, len: usize, seq: u8)
    requires len <= MAX_PAYLOAD,
    ensures final(out)@ == old(out)@ + header(len as nat, seq),
{
    out.push((len % 256) as u8);
    out.push(((len / 256) % 256) as u8);
    out.push(((len / 65536) % 256) as u8);
    out.push(seq);
    proof { assert(out@ =~= old(out)@ + header(len as nat, seq)); }
}

proof fn lemma_seq_after_succ(s: u8, k: nat)
    ensures seq_after(s, k + 1) == seq_after(next_seq(s), k),
{
}

/// Append the packets carrying `payload`, advancing `seq` past them.
pub fn frame_packets(out: &mut Vec<u8>, payload: &[u8], seq: &mut u8)
    ensures
        final(out)@ == old(out)@ + frames_spec(payload@, *old(seq)),
        *final(seq) == seq_after(*old(seq), packet_count(payload@.len())),
{
    let ghost start = out@;
    let ghost s0 = *seq;
    let mut off: usize = 0;
    proof { assert(payload@.subrange(0, payload@.len() as int) =~= payload@); }
    loop
        invariant
            off <= payload.len(),
            start == old(out)@,
            s0 == *old(seq),
            out@ + frames_spec(payload@.subrange(off as int, payload.len() as int), *seq)
                == start + frames_spec(payload@, s0),
            seq_after(*seq, packet_count((payload.len() - off) as nat))
                == seq_after(s0, packet_count(payload@.len())),
        decreases payload.len() - off,
    {
        let remaining = payload.len() - off;
        let n = if remaining < MAX_PAYLOAD { remaining } else { MAX_PAYLOAD };
        let ghost before = out@;
        let ghost cur = payload@.subrange(off as int, payload.len() as int);
        let ghost old_seq = *seq;
        header_exec(out, n, *seq);
        let mut i: usize = 0;
        while i < n
            invariant
                i <= n,
                n <= MAX_PAYLOAD,
                off + n <= payload.len(),
                out@ == before + header(n as nat, old_seq) + payload@.subrange(off as int, (off + i) as int),
            decreases n - i,
        {
            out.push(payload[off + i]);
            proof {
                assert(out@ =~= before + header(n as nat, old_seq) + payload@.subrange(off as int, (off + i + 1) as int));
            }
            i = i + 1;
        }
        *seq = if *seq == 255 { 0 } else { *seq + 1 };
        proof {
            assert(*seq == next_seq(old_seq));
            assert(cur.len() == remaining);
        }
        if remaining < MAX_PAYLOAD {
            proof {
                assert(cur =~= payload@.subrange(off as int, (off + n) as int));
                assert(frames_spec(cur, old_seq) == header(cur.len(), old_seq) + cur);
                assert(out@ == before + frames_spec(cur, old_seq));
                assert(packet_count(remaining as nat) == 1);
                lemma_seq_after_succ(old_seq, 0);
                assert(seq_after(next_seq(old_seq), 0) == next_seq(old_seq));
                assert(seq_after(old_seq, 1) == *seq);
                assert(before + frames_spec(cur, old_seq) == start + frames_spec(payload@, s0));
                assert(out@ == start + frames_spec(payload@, s0));
                assert(*seq == seq_after(s0, packet_count(payload@.len())));
                assert(start == old(out)@);
                assert(s0 == *old(seq));
            }
            return;
        }
        proof {
            let head = cur.subrange(0, MAX_PAYLOAD as int);
            let tail = cur.subrange(MAX_PAYLOAD as int, cur.len() as int);
            assert(head =~= payload@.subrange(off as int, (off + n) as int));
            assert(tail =~= payload@.subrange((off + n) as int, payload.len() as int));
            assert(frames_spec(cur, old_seq) == header(MAX_PAYLOAD as nat, old_seq) + head + frames_spec(tail, next_seq(old_seq)));
            assert(out@ + frames_spec(tail, *seq) =~= before + frames_spec(cur, old_seq));
            assert(packet_count(remaining as nat) == 1 + packet_count((remaining - MAX_PAYLOAD) as nat));
            lemma_seq_after_succ(old_seq, packet_count((remaining - MAX_PAYLOAD) as nat));
        }
        off = off + n;
    }
}

/// Reassemble one logical payload from complete packets at the start of
/// `buf`; returns the payload and the number of bytes consumed, or None if
/// `buf` does not yet hold the whole payload.
pub fn deframe(buf: &[u8]) -> (r: Option<(Vec<u8>, usize)>)
    ensures
        match (r, deframe_spec(buf@)) {
            (Some((p, k)), Some((sp, sk))) => p@ == sp && k as nat == sk,
            (None, None) => true,
            _ => false,
        },
{
    let mut payload: Vec<u8> = Vec::new();
    let mut pos: usize = 0;
    proof {
        assert(buf@.subrange(0, buf@.len() as int) =~= buf@);
        if let Some((rest, used)) = deframe_spec(buf@) {
            assert(payload@ + rest =~= rest);
        }
    }
    loop
        invariant
            pos <= buf.len(),
            deframe_spec(buf@) == match deframe_spec(buf@.subrange(pos as int, buf.len() as int)) {
                Some((rest, used)) => Some((payload@ + rest, (pos + used) as nat)),
                None => None::<(Seq<u8>, nat)>,
            },
        decreases buf.len() - pos,
    {
        let ghost w = buf@.subrange(pos as int, buf.len() as int);
        if buf.len() - pos < 4 {
            proof { assert(w.len() < 4); }
            return None;
        }
        let len = (buf[pos] as usize) + (buf[pos + 1] as usize) * 256 + (buf[pos + 2] as usize) * 65536;
        proof {
            assert(w[0] == buf@[pos as int] && w[1] == buf@[pos + 1] && w[2] == buf@[pos + 2]);
            assert(len as nat == header_len(w));
        }
        if buf.len() - pos - 4 < len {
            return None;
        }
        let ghost before = payload@;
        let mut i: usize = 0;
        while i < len
            invariant
                i <= len,
                pos + 4 + len <= buf.len(),
                payload@ == before + buf@.subrange((pos + 4) as int, (pos + 4 + i) as int),
            decreases len - i,
        {
            payload.push(buf[pos + 4 + i]);
            proof {
                assert(payload@ =~= before + buf@.subrange((pos + 4) as int, (pos + 4 + i + 1) as int));
            }
            i = i + 1;
        }
        proof {
            assert(w.subrange(4, 4 + len as int) =~= buf@.subrange((pos + 4) as int, (pos + 4 + len) as int));
            assert(w.subrange(4 + len as int, w.len() as int) =~= buf@.subrange((pos + 4 + len) as int, buf.len() as int));
        }
        if len < MAX_PAYLOAD {
            return Some((payload, pos + 4 + len));
        }
        pos = pos + 4 + len;
        proof {
            assert(payload@ == before + w.subrange(4, 4 + len as int));
        }
    }
}

/// Decode the 4-byte packet header: (payload length, sequence id).
pub fn packet_header(buf: &[u8]) -> (r: Option<(usize, u8)>)
    ensures
        r matches Some((len, s)) ==> buf.len() >= 4 && len as nat == header_len(buf@) && s == buf@[3]
            && len <= MAX_PAYLOAD,
        r is None ==> buf.len() < 4,
{
    if buf.len() < 4 {
        return None;
    }
    let len = (buf[0] as usize) + (buf[1] as usize) * 256 + (buf[2] as usize) * 65536;
    Some((len, buf[3]))
}

} // verus!
