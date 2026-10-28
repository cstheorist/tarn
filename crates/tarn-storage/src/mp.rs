//! The little MessagePack the procedures need: reading their arguments
//! (values are skipped or taken as raw bytes, which Tarantool's API takes as
//! they are) and writing their replies and the tuples they store. The
//! encodings and decodings are `tarn_verified::mp`'s, proven with Verus:
//! Tarantool's shortest encodings, read back as written, and values skipped
//! as the format nests them, without a panic on any input.

use tarn_verified::mp;

/// A reader over a MessagePack buffer. Every method returns None when the
/// buffer ends early or holds something else than asked for, and then
/// leaves the position where it was.
pub struct Rd<'a> {
    pub buf: &'a [u8],
    pub pos: usize,
}

impl<'a> Rd<'a> {
    pub fn new(buf: &'a [u8]) -> Rd<'a> {
        Rd { buf, pos: 0 }
    }

    /// A value a verified decoder read, and the position after it.
    fn read<T>(&mut self, r: Option<(T, usize)>) -> Option<T> {
        let (v, end) = r?;
        self.pos = end;
        Some(v)
    }

    pub fn peek(&self) -> Option<u8> {
        self.buf.get(self.pos).copied()
    }

    /// The next value is nil (and is read).
    pub fn nil(&mut self) -> bool {
        if self.peek() == Some(0xc0) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    pub fn array(&mut self) -> Option<u32> {
        self.read(mp::read_array(self.buf, self.pos))
    }

    pub fn map(&mut self) -> Option<u32> {
        self.read(mp::read_map(self.buf, self.pos))
    }

    /// An unsigned integer (or a non-negative signed one).
    pub fn uint(&mut self) -> Option<u64> {
        self.read(mp::read_uint(self.buf, self.pos))
    }

    /// An integer of any encoding.
    pub fn int(&mut self) -> Option<i128> {
        self.read(mp::read_int(self.buf, self.pos))
    }

    pub fn bool(&mut self) -> Option<bool> {
        let v = match self.peek()? {
            0xc2 => false,
            0xc3 => true,
            _ => return None,
        };
        self.pos += 1;
        Some(v)
    }

    /// The bytes of a string (`bin`: false) or a binary.
    fn bytes(&mut self, bin: bool) -> Option<&'a [u8]> {
        let (start, end) = mp::read_bytes(self.buf, self.pos, bin)?;
        let s = self.buf.get(start..end)?;
        self.pos = end;
        Some(s)
    }

    pub fn bin(&mut self) -> Option<&'a [u8]> {
        self.bytes(true)
    }

    pub fn str(&mut self) -> Option<&'a [u8]> {
        self.bytes(false)
    }

    /// A float or a double, as a double.
    pub fn float(&mut self) -> Option<f64> {
        let (bits, double, end) = mp::read_float_bits(self.buf, self.pos)?;
        self.pos = end;
        Some(if double {
            f64::from_bits(bits)
        } else {
            f32::from_bits(bits as u32) as f64
        })
    }

    /// Skip one value, however nested.
    pub fn skip(&mut self) -> Option<()> {
        self.pos = mp::skip(self.buf, self.pos)?;
        Some(())
    }

    /// The raw bytes of the next value.
    pub fn raw(&mut self) -> Option<&'a [u8]> {
        let end = mp::skip(self.buf, self.pos)?;
        let s = self.buf.get(self.pos..end)?;
        self.pos = end;
        Some(s)
    }
}

/// A MessagePack writer.
#[derive(Default)]
pub struct Wr(pub Vec<u8>);

impl Wr {
    pub fn with_capacity(n: usize) -> Wr {
        Wr(Vec::with_capacity(n))
    }

    pub fn array(&mut self, n: u32) -> &mut Wr {
        mp::write_array(&mut self.0, n);
        self
    }

    pub fn map(&mut self, n: u32) -> &mut Wr {
        mp::write_map(&mut self.0, n);
        self
    }

    pub fn str(&mut self, s: &str) -> &mut Wr {
        self.bytes_header(false, s.len()).raw(s.as_bytes())
    }

    pub fn uint(&mut self, v: u64) -> &mut Wr {
        mp::write_uint(&mut self.0, v);
        self
    }

    /// An integer in its shortest encoding (unsigned when not negative), as
    /// Tarantool encodes it. `v` must fit in an int64 or a uint64.
    pub fn int(&mut self, v: i128) -> &mut Wr {
        assert!(mp::write_int(&mut self.0, v), "{v} is neither an int64 nor a uint64");
        self
    }

    pub fn double(&mut self, v: f64) -> &mut Wr {
        mp::write_double_bits(&mut self.0, v.to_bits());
        self
    }

    /// The shortest header of a string (`bin`: false) or a binary of `n`
    /// bytes.
    pub fn bytes_header(&mut self, bin: bool, n: usize) -> &mut Wr {
        let n = u32::try_from(n).expect("a string or a binary under 4 GiB");
        mp::write_bytes_header(&mut self.0, bin, n);
        self
    }

    /// An array header whose length is filled in later (`patch_array`): 5
    /// bytes, the longest form.
    pub fn array_slot(&mut self) -> usize {
        mp::write_array_slot(&mut self.0)
    }

    pub fn patch_array(&mut self, at: usize, n: u32) {
        assert!(mp::patch_array(&mut self.0, at, n), "no array slot at {at}");
    }

    pub fn raw(&mut self, b: &[u8]) -> &mut Wr {
        self.0.extend_from_slice(b);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_what_it_writes() {
        let mut w = Wr::default();
        w.array(4).uint(300).str("ok").uint(1 << 40);
        let at = w.array_slot();
        w.uint(1).uint(2);
        w.patch_array(at, 2);
        let mut r = Rd::new(&w.0);
        assert_eq!(r.array(), Some(4));
        assert_eq!(r.uint(), Some(300));
        assert_eq!(r.str(), Some(&b"ok"[..]));
        assert_eq!(r.uint(), Some(1 << 40));
        assert_eq!(r.array(), Some(2));
        assert_eq!((r.uint(), r.uint()), (Some(1), Some(2)));
        assert_eq!(r.peek(), None);
    }

    #[test]
    fn skips_nested_values() {
        // [{"e": 5, "m": [1, nil, true]}, -1, "x", 1.5, bin(2), ext(1)]
        let buf: &[u8] = &[
            0x96, 0x82, 0xa1, b'e', 0x05, 0xa1, b'm', 0x93, 0x01, 0xc0, 0xc3, 0xff, 0xa1, b'x', 0xcb, 0, 0, 0, 0, 0, 0,
            0, 0, 0xc4, 2, 1, 2, 0xd4, 1, 7,
        ];
        let mut r = Rd::new(buf);
        assert_eq!(r.raw().map(|b| b.len()), Some(buf.len()));
        let mut r = Rd::new(buf);
        r.array().unwrap();
        assert_eq!(r.map(), Some(2));
        assert_eq!(r.str(), Some(&b"e"[..]));
        assert_eq!(r.uint(), Some(5));
        r.str().unwrap();
        r.skip().unwrap();
        assert_eq!(r.uint(), None); // -1
                                    // a truncated buffer
        assert_eq!(Rd::new(&buf[..10]).skip(), None);
    }

    #[test]
    fn integers_of_any_sign() {
        let mut w = Wr::default();
        w.uint(u64::MAX).uint(7);
        let mut r = Rd::new(&w.0);
        assert_eq!((r.int(), r.int()), (Some(u64::MAX as i128), Some(7)));
        for (buf, v) in [
            (&[0xff][..], -1i128),
            (&[0xe0], -32),
            (&[0xd0, 0x80], -128),
            (&[0xd1, 0xff, 0x00], -256),
            (&[0xd2, 0x80, 0, 0, 0], i32::MIN as i128),
            (&[0xd3, 0x80, 0, 0, 0, 0, 0, 0, 0], i64::MIN as i128),
            (&[0xd0, 0x05], 5),
        ] {
            assert_eq!(Rd::new(buf).int(), Some(v), "{:x?}", buf);
        }
        assert_eq!(Rd::new(&[0xc0]).int(), None);
        assert_eq!(Rd::new(&[0xcb, 0, 0, 0, 0, 0, 0, 0, 0]).int(), None);
    }

    #[test]
    fn signed_encodings_of_non_negative_integers() {
        assert_eq!(Rd::new(&[0xd0, 0x05]).uint(), Some(5));
        assert_eq!(Rd::new(&[0xd1, 0x01, 0x00]).uint(), Some(256));
        assert_eq!(Rd::new(&[0xd0, 0xff]).uint(), None);
        assert!(Rd::new(&[0xc0]).nil());
        assert_eq!(Rd::new(&[0xc3]).bool(), Some(true));
    }

    #[test]
    fn a_failed_read_moves_nothing() {
        // a uint16 cut short, a string, a negative integer, a float
        let buf: &[u8] = &[0xcd, 0x01];
        let mut r = Rd::new(buf);
        assert_eq!(
            (r.uint(), r.int(), r.skip(), r.raw(), r.pos),
            (None, None, None, None, 0)
        );
        let mut r = Rd::new(&[0xa1, b'x']);
        assert_eq!(
            (r.uint(), r.array(), r.map(), r.bin(), r.bool(), r.float(), r.pos),
            (None, None, None, None, None, None, 0)
        );
        assert_eq!((r.str(), r.pos), (Some(&b"x"[..]), 2));
        let mut r = Rd::new(&[0xff]);
        assert_eq!((r.uint(), r.pos), (None, 0));
        assert_eq!((r.int(), r.pos), (Some(-1), 1));
        let mut r = Rd::new(&[0xca, 0x3f, 0xc0, 0, 0]);
        assert_eq!((r.int(), r.float(), r.pos), (None, Some(1.5), 5));
    }

    #[test]
    #[should_panic(expected = "neither an int64 nor a uint64")]
    fn integers_out_of_range_are_not_written() {
        Wr::default().int(u64::MAX as i128 + 1);
    }
}
