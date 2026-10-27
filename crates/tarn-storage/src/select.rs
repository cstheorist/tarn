//! `ts_select`: tarn_select (lua/tarn.lua) for the reads it serves without
//! any check that could fail or wait -- the autocommit reads of memtx
//! tables that SQL statements make most: a page of an index scan from a key,
//! routed by buckets this shard owns, with no prepared transaction's lock
//! and no held table on the shard (TarnCommit!SnapshotRead with nothing to
//! wait for). Anything else is handed back to tarn_select: the reply
//! ["lua"] tells the SQL server to call it. The decisions are the ones
//! tarn_select makes in that case, so either gives the same reply.

use crate::mp::Rd;
use crate::state::State;

/// Iterators (box.index.EQ ...: TarnSelect's `iterator` numbers, as ITER in
/// lua/tarn.lua).
pub const ITER_EQ: u32 = 0;
pub const ITER_GT: u32 = 6;

/// How a read was routed (tarn_select's `route`).
#[derive(Debug, PartialEq)]
pub enum Route<'a> {
    None,
    Bucket(u64),
    /// A MessagePack array of bucket numbers.
    Buckets(&'a [u8]),
    /// A read of every shard: the epoch and the membership version of the
    /// map it was routed with.
    Map {
        e: Option<u64>,
        m: Option<u64>,
    },
}

/// The arguments of a read ts_select serves: those of tarn_select, when
/// they are of that kind (None otherwise).
#[derive(Debug, PartialEq)]
pub struct Args<'a> {
    pub space_id: u32,
    pub index_id: u32,
    /// A MessagePack array.
    pub key: &'a [u8],
    pub iterator: u32,
    pub limit: Option<u64>,
    pub lock_check: bool,
    pub route: Route<'a>,
}

const EMPTY_KEY: &[u8] = &[0x90];

/// tarn_select's arguments: (space_id, index_id, key, iterator, limit,
/// end_key, end_incl, lock_check, offset, begin, after, route). None for a
/// read ts_select leaves to tarn_select: one that ends at a key, skips rows,
/// begins a stream transaction or continues a page; or arguments it does
/// not read.
pub fn parse(buf: &[u8]) -> Option<Args<'_>> {
    let mut r = Rd::new(buf);
    if r.array()? != 12 {
        return None;
    }
    let space_id = u32::try_from(r.uint()?).ok()?;
    let index_id = u32::try_from(r.uint()?).ok()?;
    let key = if r.nil() {
        EMPTY_KEY
    } else {
        let k = r.raw()?;
        Rd::new(k).array()?;
        k
    };
    let iterator = u32::try_from(r.uint()?).ok()?;
    if iterator > ITER_GT {
        return None;
    }
    let limit = if r.nil() { None } else { Some(r.uint()?) };
    if limit == Some(0) {
        return None;
    }
    if !r.nil() {
        return None; // end_key
    }
    r.skip()?; // end_incl
    let lock_check = r.bool()?;
    if !r.nil() && r.uint()? != 0 {
        return None; // offset
    }
    if !r.nil() && r.bool()? {
        return None; // begin
    }
    if !r.nil() {
        return None; // after
    }
    let route = match r.peek()? {
        0xc0 => {
            r.nil();
            Route::None
        }
        0x90..=0x9f | 0xdc | 0xdd => Route::Buckets(r.raw()?),
        0x80..=0x8f | 0xde | 0xdf => {
            let (mut e, mut m) = (None, None);
            for _ in 0..r.map()? {
                match r.str() {
                    Some(b"e") => e = Some(r.uint()?),
                    Some(b"m") => m = Some(r.uint()?),
                    _ => r.skip()?,
                }
            }
            Route::Map { e, m }
        }
        _ => Route::Bucket(r.uint()?),
    };
    Some(Args {
        space_id,
        index_id,
        key,
        iterator,
        limit,
        lock_check,
        route,
    })
}

/// Whether the shard serves a read routed by `route` as it is now, without
/// refusing it (BK.check_read: the map is current, every bucket of the
/// route is one this shard reads, the epoch and membership of a read of
/// every shard are current).
pub fn route_ok(s: &State, route: &Route) -> bool {
    if matches!(route, Route::None) {
        return true;
    }
    if s.bk_stale != 0 {
        return false;
    }
    match route {
        Route::None => true,
        Route::Map { e, m } => {
            if m.is_some_and(|m| m < s.mver) {
                return false;
            }
            s.count == 0 || e.is_none_or(|e| e == s.epoch)
        }
        Route::Bucket(b) => s.count == 0 || s.serves(*b),
        Route::Buckets(raw) => {
            if s.count == 0 {
                return true;
            }
            let mut r = Rd::new(raw);
            let Some(n) = r.array() else { return false };
            (0..n).all(|_| r.uint().is_some_and(|b| s.serves(b)))
        }
    }
}

/// Whether the read counts for the load of buckets (BK.count_reads): it was
/// routed by some.
pub fn counted(route: &Route) -> bool {
    matches!(route, Route::Bucket(_) | Route::Buckets(_))
}

/// Count the read for the buckets it was routed by (BK.count_reads).
pub fn count_reads(s: &mut State, route: &Route) {
    match route {
        Route::Bucket(b) => s.count_read(*b),
        Route::Buckets(raw) => {
            let mut r = Rd::new(raw);
            if let Some(n) = r.array() {
                for _ in 0..n {
                    match r.uint() {
                        Some(b) => s.count_read(b),
                        None => return,
                    }
                }
            }
        }
        _ => {}
    }
}

/// The rows a page holds, from the scan's rows (their sizes) one at a time:
/// tarn_select's page (PG.scan) when it does not end the page early -- the
/// rows of `limit`, or of `page_rows`, unless a row before the last reaches
/// `page_bytes` -- or None when it would (the reply would carry where the
/// next page starts: tarn_select's job).
pub struct Page {
    /// The most rows the scan reads.
    pub n: u64,
    limit: Option<u64>,
    page_bytes: u64,
    bytes: u64,
    rows: u64,
    cut: bool,
}

impl Page {
    pub fn new(limit: Option<u64>, page_rows: u32, page_bytes: u64) -> Page {
        let page_rows = (page_rows as u64).max(1);
        let n = match limit {
            Some(l) if l <= page_rows => l,
            _ => page_rows,
        };
        Page {
            n,
            limit,
            page_bytes,
            bytes: 0,
            rows: 0,
            cut: false,
        }
    }

    /// A row of `size` bytes, after the ones before it.
    pub fn row(&mut self, size: u64) {
        // (PG.scan cuts after a row that reaches page_bytes, unless it is
        // the last: known once the next one comes)
        if self.bytes >= self.page_bytes && self.rows > 0 {
            self.cut = true;
        }
        self.bytes += size;
        self.rows += 1;
    }

    /// Whether the page holds every row read: false when tarn_select would
    /// end it early.
    pub fn whole(&self) -> bool {
        !(self.cut || (self.rows == self.n && Some(self.n) != self.limit))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mp::Wr;

    fn args(f: impl Fn(&mut Wr, usize)) -> Vec<u8> {
        let mut w = Wr::default();
        w.array(12);
        for i in 0..12 {
            f(&mut w, i);
        }
        w.0
    }

    /// A point read routed by bucket 7, with `change` applied to argument i.
    fn point(change: Option<(usize, &[u8])>) -> Vec<u8> {
        args(|w, i| {
            if let Some((j, v)) = change {
                if i == j {
                    w.raw(v);
                    return;
                }
            }
            match i {
                0 => w.uint(512),
                1 => w.uint(0),
                2 => w.array(1).uint(17),
                3 => w.uint(0),
                4 => w.uint(1),
                5 | 8 | 10 => w.raw(&[0xc0]),
                6 | 7 => w.raw(&[0xc3]),
                9 => w.raw(&[0xc2]),
                _ => w.uint(7),
            };
        })
    }

    #[test]
    fn parses_the_reads_it_serves() {
        let buf = point(None);
        let a = parse(&buf).unwrap();
        assert_eq!(
            a,
            Args {
                space_id: 512,
                index_id: 0,
                key: &[0x91, 17],
                iterator: 0,
                limit: Some(1),
                lock_check: true,
                route: Route::Bucket(7)
            }
        );
        // no limit, no key, a list of buckets, a map
        assert_eq!(parse(&point(Some((4, &[0xc0])))).unwrap().limit, None);
        assert_eq!(parse(&point(Some((2, &[0xc0])))).unwrap().key, &[0x90]);
        assert_eq!(
            parse(&point(Some((11, &[0x92, 3, 4])))).unwrap().route,
            Route::Buckets(&[0x92, 3, 4])
        );
        assert_eq!(
            parse(&point(Some((11, &[0x82, 0xa1, b'e', 9, 0xa1, b'm', 2]))))
                .unwrap()
                .route,
            Route::Map { e: Some(9), m: Some(2) }
        );
        assert_eq!(parse(&point(Some((11, &[0xc0])))).unwrap().route, Route::None);
        // an offset of 0 and begin = false are no offset and no begin
        assert!(parse(&point(Some((8, &[0x00])))).is_some());
    }

    #[test]
    fn leaves_the_others_to_lua() {
        for (i, v) in [
            (5, &[0x91, 0x05][..]), // an end key
            (8, &[0x02]),           // an offset
            (9, &[0xc3]),           // begin
            (10, &[0xa1, b'x']),    // after
            (3, &[0x07]),           // BITS_ALL_SET
            (3, &[0xa2, b'E', b'Q']),
            (4, &[0x00]),        // limit 0
            (2, &[0x05]),        // a key that is not an array
            (11, &[0xa1, b'x']), // a route of another kind
        ] {
            assert!(parse(&point(Some((i, v)))).is_none(), "argument {} = {:?}", i, v);
        }
        assert!(parse(&[0x91, 0x01]).is_none());
        let buf = point(None);
        assert!(parse(&buf[..buf.len() - 1]).is_none());
    }

    #[test]
    fn routes() {
        let mut serves = vec![0u8; 9];
        serves[7] = 1;
        serves[8] = 1;
        let mut s = State::new();
        s.count = 8;
        s.serves = serves.as_mut_ptr();
        s.epoch = 5;
        s.mver = 3;
        assert!(!route_ok(&s, &Route::Bucket(7)), "the map is stale");
        assert!(route_ok(&s, &Route::None));
        s.bk_stale = 0;
        assert!(route_ok(&s, &Route::Bucket(7)) && !route_ok(&s, &Route::Bucket(6)));
        assert!(route_ok(&s, &Route::Buckets(&[0x92, 7, 8])));
        assert!(!route_ok(&s, &Route::Buckets(&[0x92, 7, 6])));
        assert!(!route_ok(&s, &Route::Buckets(&[0x91, 0xc0])));
        assert!(route_ok(&s, &Route::Map { e: Some(5), m: Some(3) }));
        assert!(route_ok(&s, &Route::Map { e: Some(5), m: None }));
        assert!(route_ok(&s, &Route::Map { e: None, m: Some(4) }));
        assert!(!route_ok(&s, &Route::Map { e: Some(4), m: Some(3) }));
        assert!(!route_ok(&s, &Route::Map { e: Some(5), m: Some(2) }));
        // a cluster with no bucket map: every bucket, every epoch
        s.count = 0;
        assert!(route_ok(&s, &Route::Bucket(6)) && route_ok(&s, &Route::Map { e: Some(1), m: None }));
        assert!(!route_ok(&s, &Route::Map { e: Some(1), m: Some(2) }));
    }

    #[test]
    fn pages() {
        // within the limit: whole
        let mut p = Page::new(Some(3), 4096, 1 << 20);
        assert_eq!(p.n, 3);
        for _ in 0..3 {
            p.row(100);
        }
        assert!(p.whole());
        // no limit, the page's rows reached: tarn_select ends the page
        let mut p = Page::new(None, 2, 1 << 20);
        p.row(1);
        p.row(1);
        assert!(!p.whole());
        let mut p = Page::new(None, 2, 1 << 20);
        p.row(1);
        assert!(p.whole());
        // a limit above the page's rows
        let mut p = Page::new(Some(10), 2, 1 << 20);
        assert_eq!(p.n, 2);
        p.row(1);
        p.row(1);
        assert!(!p.whole());
        // bytes: a row before the last reaches page_bytes
        let mut p = Page::new(Some(3), 4096, 100);
        p.row(100);
        p.row(1);
        assert!(!p.whole());
        // ... the last one may
        let mut p = Page::new(Some(3), 4096, 100);
        p.row(1);
        p.row(150);
        assert!(p.whole());
    }
}
