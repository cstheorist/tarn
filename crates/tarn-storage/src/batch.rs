//! A bounded subset of `tarn_select_keys`, through `ts_select`'s eight-argument
//! form. The twelve-argument ordinary read keeps its existing meaning. The
//! parser does no storage work; unsupported shapes are handed back whole.

use crate::fallback::{back, Why};
use crate::mp::Rd;
use crate::select::{parse_route, Route};
pub use tarn_verified::batch::{can_step, step, Budget};

/// Includes misses and duplicates: both cost an actual index lookup.
pub const MAX_KEYS: u64 = 128;

/// A whole native primary key, still encoded in the request buffer.
#[derive(Debug, PartialEq, Eq)]
pub struct Key<'a> {
    pub raw: &'a [u8],
    pub parts: u32,
}

#[derive(Debug, PartialEq)]
pub struct Args<'a> {
    pub space_id: u32,
    pub keys: Vec<Key<'a>>,
    pub route: Route<'a>,
}

/// The capability request has no state/data access and no read counters.
pub fn capability(buf: &[u8]) -> bool {
    let mut r = Rd::new(buf);
    r.array() == Some(0) && r.peek().is_none()
}

/// `(space, 0, keys, limit, true, false, nil, route)`.
/// A full unique key returns at most one row regardless of a positive
/// per-key limit, as Lua's `keys_scan` does. Zero and other shapes stay Lua.
pub fn parse(buf: &[u8]) -> Option<Args<'_>> {
    let mut r = Rd::new(buf);
    if r.array()? != 8 {
        return None;
    }
    let space_id = u32::try_from(r.uint()?).ok()?;
    if r.uint()? != 0 {
        return None;
    }
    let n = r.array()?;
    if n == 0 || u64::from(n) > MAX_KEYS {
        return None;
    }
    let mut keys = Vec::with_capacity(n as usize);
    for _ in 0..n {
        let raw = r.raw()?;
        let parts = Rd::new(raw).array()?;
        if parts == 0 {
            return None;
        }
        keys.push(Key { raw, parts });
    }
    if !r.nil() && r.uint()? == 0 {
        return None;
    }
    if !r.bool()? {
        return None;
    }
    if r.bool()? {
        return back(Why::Stream);
    }
    if !r.nil() {
        return back(Why::NextPage);
    }
    let route = parse_route(&mut r)?;
    if r.peek().is_some() {
        return None;
    }
    Some(Args { space_id, keys, route })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mp::Wr;

    fn request(keys: &[&[u8]], limit: Option<u64>, lock_check: bool, begin: bool, after: bool) -> Vec<u8> {
        let mut w = Wr::default();
        w.array(8).uint(10001).uint(0).array(keys.len() as u32);
        for key in keys {
            w.raw(key);
        }
        match limit {
            Some(n) => w.uint(n),
            None => w.raw(&[0xc0]),
        };
        w.raw(&[if lock_check { 0xc3 } else { 0xc2 }, if begin { 0xc3 } else { 0xc2 }]);
        w.raw(&[if after { 0x90 } else { 0xc0 }]);
        w.array(1).uint(7);
        w.0
    }

    #[test]
    fn retains_order_duplicates_and_misses_as_requested_keys() {
        let raw = request(&[&[0x91, 1], &[0x91, 9], &[0x91, 1]], Some(1), true, false, false);
        let a = parse(&raw).unwrap();
        assert_eq!(a.space_id, 10001);
        assert_eq!(
            a.keys.iter().map(|k| k.raw).collect::<Vec<_>>(),
            [&[0x91, 1][..], &[0x91, 9], &[0x91, 1]]
        );
        assert!(a.keys.iter().all(|k| k.parts == 1));
        assert_eq!(a.route, Route::Buckets(&[0x91, 7]));
    }

    #[test]
    fn accepts_composite_and_exact_work_bound() {
        let key = &[0x92, 1, 2][..];
        for limit in [None, Some(1), Some(9)] {
            let raw = request(&vec![key; MAX_KEYS as usize], limit, true, false, false);
            let a = parse(&raw).unwrap();
            assert_eq!(a.keys.len(), MAX_KEYS as usize);
            assert_eq!(a.keys[0].parts, 2);
        }
        assert!(parse(&request(&vec![key; MAX_KEYS as usize + 1], None, true, false, false)).is_none());
    }

    #[test]
    fn refuses_unsupported_or_incomplete_shapes() {
        for raw in [
            request(&[], None, true, false, false),
            request(&[&[0x90]], None, true, false, false),
            request(&[&[0x91, 1]], Some(0), true, false, false),
            request(&[&[0x91, 1]], None, false, false, false),
            request(&[&[0x91, 1]], None, true, true, false),
            request(&[&[0x91, 1]], None, true, false, true),
        ] {
            assert!(parse(&raw).is_none());
        }
        let raw = request(&[&[0x91, 1]], None, true, false, false);
        for end in 0..raw.len() {
            assert!(parse(&raw[..end]).is_none());
        }
        let mut trailing = raw.clone();
        trailing.push(0xc0);
        assert!(parse(&trailing).is_none());
        let mut wrong_index = raw;
        wrong_index[4] = 1;
        assert!(parse(&wrong_index).is_none());
    }

    #[test]
    fn capability_is_only_a_complete_empty_argument_array() {
        assert!(capability(&[0x90]));
        assert!(capability(&[0xdc, 0, 0]));
        for raw in [&[][..], &[0xc0], &[0x90, 0xc0], &[0x91, 1], &[0xdc, 0]] {
            assert!(!capability(raw));
        }
        assert!(parse(&[0x90]).is_none());
    }
}
