//! Byte-level MySQL wire helpers: length-encoded integers and strings, and
//! packet framing. Every function here is a thin wrapper over the formally
//! verified implementation in `tarn_verified::wire` (Verus proves functional
//! correctness against a byte-level spec, absence of panics/overflow, and
//! that `deframe` inverts `frame_packets`). The whole crate routes its
//! lenenc and framing work through this module.

use tarn_verified::wire;

/// Largest payload one packet carries; longer payloads continue in the next
/// packet, and a payload that is an exact multiple ends with an empty packet.
pub const MAX_PAYLOAD: usize = wire::MAX_PAYLOAD;

#[inline]
pub fn lenenc_int_len(n: u64) -> usize {
    wire::lenenc_int_len(n)
}

#[inline]
pub fn write_lenenc_int(out: &mut Vec<u8>, n: u64) {
    wire::write_lenenc_int(out, n)
}

/// (value, bytes consumed); None if truncated or the first byte is 0xFB
/// (the NULL marker) or 0xFF.
#[inline]
pub fn read_lenenc_int(buf: &[u8]) -> Option<(u64, usize)> {
    wire::read_lenenc_int(buf)
}

#[inline]
pub fn write_lenenc_str(out: &mut Vec<u8>, s: &[u8]) {
    wire::write_lenenc_str(out, s)
}

/// (string bytes, bytes consumed); None if truncated.
#[inline]
pub fn read_lenenc_str(buf: &[u8]) -> Option<(&[u8], usize)> {
    let (start, len, used) = wire::read_lenenc_str_bounds(buf)?;
    Some((&buf[start..start + len], used))
}

/// Append the wire packets for one logical payload, advancing `seq`.
#[inline]
pub fn frame_packets(out: &mut Vec<u8>, payload: &[u8], seq: &mut u8) {
    wire::frame_packets(out, payload, seq)
}

/// (payload length, sequence id) of the packet starting at `buf`.
#[inline]
pub fn packet_header(buf: &[u8]) -> Option<(usize, u8)> {
    wire::packet_header(buf)
}

/// Reassemble one logical payload from the complete packets at the start of
/// `buf`: (payload, bytes consumed), or None if more bytes are needed.
#[inline]
pub fn deframe(buf: &[u8]) -> Option<(Vec<u8>, usize)> {
    wire::deframe(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lenenc_boundaries() {
        for &n in &[0u64, 250, 251, 0xFFFF, 0x1_0000, 0xFF_FFFF, 0x100_0000, u64::MAX] {
            let mut v = Vec::new();
            write_lenenc_int(&mut v, n);
            assert_eq!(v.len(), lenenc_int_len(n));
            assert_eq!(read_lenenc_int(&v), Some((n, v.len())));
            assert_eq!(read_lenenc_int(&v[..v.len() - 1]), None);
        }
        assert_eq!(read_lenenc_int(&[0xFB]), None);
        let mut v = Vec::new();
        write_lenenc_str(&mut v, b"abc");
        assert_eq!(read_lenenc_str(&v), Some((&b"abc"[..], 4)));
    }

    #[test]
    fn framing_boundaries() {
        for &len in &[0usize, MAX_PAYLOAD - 1, MAX_PAYLOAD, MAX_PAYLOAD + 1, 2 * MAX_PAYLOAD] {
            let p: Vec<u8> = (0..len).map(|i| i as u8).collect();
            let mut out = Vec::new();
            let mut seq = 0u8;
            frame_packets(&mut out, &p, &mut seq);
            assert_eq!(seq as usize, len / MAX_PAYLOAD + 1);
            let (back, used) = deframe(&out).unwrap();
            assert_eq!(back, p);
            assert_eq!(used, out.len());
            assert_eq!(packet_header(&out).map(|h| h.0), Some(len.min(MAX_PAYLOAD)));
        }
    }
}
