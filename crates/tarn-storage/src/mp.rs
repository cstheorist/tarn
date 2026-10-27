//! The little MessagePack the procedures need: reading their arguments
//! (values are skipped or taken as raw bytes, which Tarantool's API takes as
//! they are) and writing their replies.

/// A reader over a MessagePack buffer. Every method returns None when the
/// buffer ends early or holds something else than asked for.
pub struct Rd<'a> {
    pub buf: &'a [u8],
    pub pos: usize,
}

impl<'a> Rd<'a> {
    pub fn new(buf: &'a [u8]) -> Rd<'a> {
        Rd { buf, pos: 0 }
    }

    fn byte(&mut self) -> Option<u8> {
        let b = *self.buf.get(self.pos)?;
        self.pos += 1;
        Some(b)
    }

    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(n)?;
        let s = self.buf.get(self.pos..end)?;
        self.pos = end;
        Some(s)
    }

    fn be(&mut self, n: usize) -> Option<u64> {
        Some(self.take(n)?.iter().fold(0u64, |a, &b| (a << 8) | b as u64))
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
        match self.byte()? {
            b @ 0x90..=0x9f => Some((b & 0x0f) as u32),
            0xdc => Some(self.be(2)? as u32),
            0xdd => Some(self.be(4)? as u32),
            _ => None,
        }
    }

    pub fn map(&mut self) -> Option<u32> {
        match self.byte()? {
            b @ 0x80..=0x8f => Some((b & 0x0f) as u32),
            0xde => Some(self.be(2)? as u32),
            0xdf => Some(self.be(4)? as u32),
            _ => None,
        }
    }

    /// An unsigned integer (or a non-negative signed one).
    pub fn uint(&mut self) -> Option<u64> {
        match self.byte()? {
            b @ 0x00..=0x7f => Some(b as u64),
            0xcc => self.be(1),
            0xcd => self.be(2),
            0xce => self.be(4),
            0xcf => self.be(8),
            0xd0 => {
                let v = self.be(1)? as u8 as i8;
                (v >= 0).then_some(v as u64)
            }
            0xd1 => {
                let v = self.be(2)? as u16 as i16;
                (v >= 0).then_some(v as u64)
            }
            0xd2 => {
                let v = self.be(4)? as u32 as i32;
                (v >= 0).then_some(v as u64)
            }
            0xd3 => {
                let v = self.be(8)? as i64;
                (v >= 0).then_some(v as u64)
            }
            _ => None,
        }
    }

    pub fn bool(&mut self) -> Option<bool> {
        match self.byte()? {
            0xc2 => Some(false),
            0xc3 => Some(true),
            _ => None,
        }
    }

    pub fn str(&mut self) -> Option<&'a [u8]> {
        let n = match self.byte()? {
            b @ 0xa0..=0xbf => (b & 0x1f) as usize,
            0xd9 => self.be(1)? as usize,
            0xda => self.be(2)? as usize,
            0xdb => self.be(4)? as usize,
            _ => return None,
        };
        self.take(n)
    }

    /// Skip one value, however nested.
    pub fn skip(&mut self) -> Option<()> {
        let mut pending: u64 = 1;
        while pending > 0 {
            pending -= 1;
            let b = self.byte()?;
            let (skip, more): (usize, u64) = match b {
                0x00..=0x7f | 0xe0..=0xff | 0xc0 | 0xc2 | 0xc3 => (0, 0),
                0x80..=0x8f => (0, 2 * (b & 0x0f) as u64),
                0x90..=0x9f => (0, (b & 0x0f) as u64),
                0xa0..=0xbf => ((b & 0x1f) as usize, 0),
                0xcc | 0xd0 => (1, 0),
                0xcd | 0xd1 => (2, 0),
                0xce | 0xd2 | 0xca => (4, 0),
                0xcf | 0xd3 | 0xcb => (8, 0),
                0xd9 | 0xc4 => (self.be(1)? as usize, 0),
                0xda | 0xc5 => (self.be(2)? as usize, 0),
                0xdb | 0xc6 => (self.be(4)? as usize, 0),
                0xdc => (0, self.be(2)?),
                0xdd => (0, self.be(4)?),
                0xde => (0, 2 * self.be(2)?),
                0xdf => (0, 2 * self.be(4)?),
                0xd4 => (2, 0),
                0xd5 => (3, 0),
                0xd6 => (5, 0),
                0xd7 => (9, 0),
                0xd8 => (17, 0),
                0xc7 => (self.be(1)? as usize + 1, 0),
                0xc8 => (self.be(2)? as usize + 1, 0),
                0xc9 => (self.be(4)? as usize + 1, 0),
                _ => return None,
            };
            self.take(skip)?;
            pending = pending.checked_add(more)?;
        }
        Some(())
    }

    /// The raw bytes of the next value.
    pub fn raw(&mut self) -> Option<&'a [u8]> {
        let start = self.pos;
        self.skip()?;
        Some(&self.buf[start..self.pos])
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
        match n {
            0..=15 => self.0.push(0x90 | n as u8),
            16..=0xffff => {
                self.0.push(0xdc);
                self.0.extend_from_slice(&(n as u16).to_be_bytes());
            }
            _ => {
                self.0.push(0xdd);
                self.0.extend_from_slice(&n.to_be_bytes());
            }
        }
        self
    }

    pub fn str(&mut self, s: &str) -> &mut Wr {
        let n = s.len();
        match n {
            0..=31 => self.0.push(0xa0 | n as u8),
            32..=0xff => {
                self.0.push(0xd9);
                self.0.push(n as u8);
            }
            0x100..=0xffff => {
                self.0.push(0xda);
                self.0.extend_from_slice(&(n as u16).to_be_bytes());
            }
            _ => {
                self.0.push(0xdb);
                self.0.extend_from_slice(&(n as u32).to_be_bytes());
            }
        }
        self.0.extend_from_slice(s.as_bytes());
        self
    }

    pub fn uint(&mut self, v: u64) -> &mut Wr {
        match v {
            0..=0x7f => self.0.push(v as u8),
            0x80..=0xff => {
                self.0.push(0xcc);
                self.0.push(v as u8);
            }
            0x100..=0xffff => {
                self.0.push(0xcd);
                self.0.extend_from_slice(&(v as u16).to_be_bytes());
            }
            0x1_0000..=0xffff_ffff => {
                self.0.push(0xce);
                self.0.extend_from_slice(&(v as u32).to_be_bytes());
            }
            _ => {
                self.0.push(0xcf);
                self.0.extend_from_slice(&v.to_be_bytes());
            }
        }
        self
    }

    /// An array header whose length is filled in later (`patch_array`): 5
    /// bytes, the longest form.
    pub fn array_slot(&mut self) -> usize {
        let at = self.0.len();
        self.0.extend_from_slice(&[0xdd, 0, 0, 0, 0]);
        at
    }

    pub fn patch_array(&mut self, at: usize, n: u32) {
        self.0[at + 1..at + 5].copy_from_slice(&n.to_be_bytes());
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
    fn signed_encodings_of_non_negative_integers() {
        assert_eq!(Rd::new(&[0xd0, 0x05]).uint(), Some(5));
        assert_eq!(Rd::new(&[0xd1, 0x01, 0x00]).uint(), Some(256));
        assert_eq!(Rd::new(&[0xd0, 0xff]).uint(), None);
        assert!(Rd::new(&[0xc0]).nil());
        assert_eq!(Rd::new(&[0xc3]).bool(), Some(true));
    }
}
