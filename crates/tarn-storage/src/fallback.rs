//! Why the native procedures hand requests to the Lua ones (issue #131):
//! counters by procedure and reason, which `tarn_info()` reports
//! (`native.fallbacks`). A procedure notes the reason where it hands a
//! request back (`back`); the function that runs it counts the reason
//! noted last (`counted`). A request handed back for a reason no check
//! names counts as `other`.

use crate::mp::Wr;
use std::cell::{Cell, RefCell};

/// The procedures whose hand-backs are counted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Proc {
    /// ts_select: reads.
    Read,
    /// ts_autocommit: autocommit writes.
    Write,
    /// ts_commit_local: commits of one shard.
    Commit,
    /// ts_validate: validations of read-only transactions.
    Validate,
}

const PROCS: [&str; 4] = ["read", "write", "commit", "validate"];

/// Why a procedure handed a request to the Lua one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Why {
    /// None that a check names: an error of the module API, a request
    /// whose transaction could not begin.
    Other,
    /// The procedure panicked (a bug; the Lua procedure served the request).
    Panic,
    /// The module is not in use yet, or lacks a setting.
    NotReady,
    /// Test hooks hold the requests (TARN_TEST_HOOKS).
    TestHooks,
    /// The node is read-only (a replica, or a demoted leader).
    ReadOnly,
    /// The bucket map is to be read again.
    StaleBuckets,
    /// The lock index is being made again (an election).
    StaleLocks,
    /// The definition fences are to be read again.
    StaleFences,
    /// A bucket of the request is not served or written here, is held by a
    /// move, or the request was routed with an older membership.
    Bucket,
    /// The window of the buckets' load counts ended.
    LoadWindow,
    /// A TRUNCATE or DROP TABLE holds a table.
    HeldTable,
    /// The stream began before a new election term, or before a TRUNCATE or
    /// DROP TABLE was applied.
    Term,
    /// A definition fence is above the one the writes were made with.
    Fence,
    /// A vinyl table.
    Vinyl,
    /// A table whose writes a lazy index captures.
    LazyCapture,
    /// A table or index definition this module does not read.
    Schema,
    /// Arguments of a kind the procedure leaves to Lua.
    Shape,
    /// A read that skips rows.
    Offset,
    /// A read that continues a page.
    NextPage,
    /// A read longer than one page.
    Page,
    /// An autocommit write inside a transaction, or a commit or a
    /// validation without the transaction of its stream.
    Stream,
    /// The transaction holds row locks here.
    OwnRowLocks,
    /// A prepared transaction's W lock on what the request read.
    WLock,
    /// A prepared transaction's lock on a key a write writes.
    Lock,
    /// A prepared transaction's range lock on a key a write writes.
    RangeLock,
    /// Another transaction's row lock on a key a write writes.
    RowLock,
    /// A key or bound the lock checks cannot look up or compare (a decimal,
    /// or a bound that is not a key of its index).
    LockKey,
    /// The storage refused a write (a duplicate key): the Lua procedure
    /// reports it.
    WriteError,
    /// Arithmetic out of its column's range (1264, 1690).
    Arith,
    /// An update of a row from before an ALTER TABLE added fields.
    Pad,
}

/// Every reason, in order.
pub const ALL: [Why; 30] = [
    Why::Other,
    Why::Panic,
    Why::NotReady,
    Why::TestHooks,
    Why::ReadOnly,
    Why::StaleBuckets,
    Why::StaleLocks,
    Why::StaleFences,
    Why::Bucket,
    Why::LoadWindow,
    Why::HeldTable,
    Why::Term,
    Why::Fence,
    Why::Vinyl,
    Why::LazyCapture,
    Why::Schema,
    Why::Shape,
    Why::Offset,
    Why::NextPage,
    Why::Page,
    Why::Stream,
    Why::OwnRowLocks,
    Why::WLock,
    Why::Lock,
    Why::RangeLock,
    Why::RowLock,
    Why::LockKey,
    Why::WriteError,
    Why::Arith,
    Why::Pad,
];

const N: usize = ALL.len();

impl Why {
    /// Its name in `tarn_info()`.
    pub fn name(self) -> &'static str {
        match self {
            Why::Other => "other",
            Why::Panic => "panic",
            Why::NotReady => "not_ready",
            Why::TestHooks => "test_hooks",
            Why::ReadOnly => "read_only",
            Why::StaleBuckets => "stale_buckets",
            Why::StaleLocks => "stale_locks",
            Why::StaleFences => "stale_fences",
            Why::Bucket => "bucket",
            Why::LoadWindow => "load_window",
            Why::HeldTable => "held_table",
            Why::Term => "term",
            Why::Fence => "fence",
            Why::Vinyl => "vinyl",
            Why::LazyCapture => "lazy_capture",
            Why::Schema => "schema",
            Why::Shape => "shape",
            Why::Offset => "offset",
            Why::NextPage => "next_page",
            Why::Page => "page",
            Why::Stream => "stream",
            Why::OwnRowLocks => "own_row_locks",
            Why::WLock => "w_lock",
            Why::Lock => "lock",
            Why::RangeLock => "range_lock",
            Why::RowLock => "row_lock",
            Why::LockKey => "lock_key",
            Why::WriteError => "write_error",
            Why::Arith => "arith",
            Why::Pad => "pad",
        }
    }
}

thread_local! {
    /// The reason noted last in the request being served.
    static NOTED: Cell<Why> = const { Cell::new(Why::Other) };
    /// The hand-backs of each procedure, by reason.
    static COUNTS: RefCell<[[u64; N]; 4]> = const { RefCell::new([[0; N]; 4]) };
}

/// Note why the request is handed back.
pub fn note(w: Why) {
    NOTED.with(|n| n.set(w));
}

/// Hand the request back for `w`: None, the reason noted.
pub fn back<T>(w: Why) -> Option<T> {
    note(w);
    None
}

/// A request begins: nothing noted yet.
pub fn begin() {
    note(Why::Other);
}

/// Count the hand-back of a request of `p` for the reason noted last (or
/// `w`, which overrides it: a panic).
pub fn counted(p: Proc, w: Option<Why>) {
    let w = w.unwrap_or_else(|| NOTED.with(|n| n.get()));
    COUNTS.with(|c| c.borrow_mut()[p as usize][w as usize] += 1);
}

/// The counters, as a MessagePack map of the procedures that handed some
/// requests back, each a map of its reasons with their counts (those
/// above zero).
pub fn encode() -> Vec<u8> {
    COUNTS.with(|c| {
        let c = c.borrow();
        let mut w = Wr::with_capacity(256);
        let procs: Vec<usize> = (0..4).filter(|&p| c[p].iter().any(|&n| n > 0)).collect();
        w.map(procs.len() as u32);
        for p in procs {
            w.str(PROCS[p]);
            let reasons: Vec<usize> = (0..N).filter(|&r| c[p][r] > 0).collect();
            w.map(reasons.len() as u32);
            for r in reasons {
                w.str(ALL[r].name()).uint(c[p][r]);
            }
        }
        w.0
    })
}

/// (For the Lua module, through the state: `tarn_info().native.fallbacks`.)
/// The counters (`encode`) in `buf`, which holds `cap` bytes; their length,
/// or 0 when they do not fit.
///
/// # Safety
/// `buf` points to `cap` writable bytes.
pub unsafe extern "C" fn fallbacks(buf: *mut u8, cap: usize) -> usize {
    let mp = encode();
    if mp.len() > cap {
        return 0;
    }
    std::ptr::copy_nonoverlapping(mp.as_ptr(), buf, mp.len());
    mp.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mp::Rd;

    #[test]
    fn every_reason_once_in_order() {
        for (i, w) in ALL.iter().enumerate() {
            assert_eq!(*w as usize, i, "{:?}", w);
            assert!(ALL[..i].iter().all(|v| v.name() != w.name()), "{}", w.name());
        }
    }

    #[test]
    fn counts_the_reason_noted_last() {
        COUNTS.with(|c| *c.borrow_mut() = [[0; N]; 4]);
        let mut buf = [0u8; 4096];
        assert_eq!(unsafe { fallbacks(buf.as_mut_ptr(), buf.len()) }, 1, "an empty map");
        assert_eq!(buf[0], 0x80);
        begin();
        counted(Proc::Read, None);
        begin();
        note(Why::WLock);
        let none: Option<()> = back(Why::Page);
        assert!(none.is_none());
        counted(Proc::Read, None);
        counted(Proc::Validate, Some(Why::Panic));
        let n = unsafe { fallbacks(buf.as_mut_ptr(), buf.len()) };
        let mut r = Rd::new(&buf[..n]);
        assert_eq!(r.map(), Some(2));
        let mut got = Vec::new();
        for _ in 0..2 {
            let p = String::from_utf8(r.str().unwrap().to_vec()).unwrap();
            for _ in 0..r.map().unwrap() {
                let w = String::from_utf8(r.str().unwrap().to_vec()).unwrap();
                got.push((p.clone(), w, r.uint().unwrap()));
            }
        }
        got.sort();
        let want = [("read", "other", 1), ("read", "page", 1), ("validate", "panic", 1)];
        assert_eq!(
            got,
            want.iter()
                .map(|&(p, w, n)| (p.to_string(), w.to_string(), n))
                .collect::<Vec<_>>()
        );
        assert_eq!(unsafe { fallbacks(buf.as_mut_ptr(), 3) }, 0, "too small a buffer");
    }

    #[test]
    fn a_map_of_more_than_15_reasons() {
        COUNTS.with(|c| *c.borrow_mut() = [[0; N]; 4]);
        for w in ALL {
            counted(Proc::Write, Some(w));
        }
        let mp = encode();
        let mut r = Rd::new(&mp);
        assert_eq!(r.map(), Some(1));
        assert_eq!(r.str(), Some(&b"write"[..]));
        assert_eq!(r.map(), Some(N as u32));
    }
}
