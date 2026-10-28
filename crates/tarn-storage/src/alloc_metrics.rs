//! Optional benchmark accounting for this module's Rust allocator requests.
//! This is neither RSS nor Tarantool tuple/C/Lua allocation accounting. Realloc
//! charges the new requested capacity as allocated and the old as freed, even
//! when System can resize in place. Failed requests do not change the counters.
//! Timing comparisons must use this same instrumented build for both modes.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

#[derive(Default)]
struct Counts {
    allocations: AtomicU64,
    reallocations: AtomicU64,
    deallocations: AtomicU64,
    allocated_bytes: AtomicU64,
    freed_bytes: AtomicU64,
    live_bytes: AtomicU64,
}

impl Counts {
    const fn new() -> Self {
        Self {
            allocations: AtomicU64::new(0),
            reallocations: AtomicU64::new(0),
            deallocations: AtomicU64::new(0),
            allocated_bytes: AtomicU64::new(0),
            freed_bytes: AtomicU64::new(0),
            live_bytes: AtomicU64::new(0),
        }
    }

    fn allocated(&self, size: usize) {
        self.allocations.fetch_add(1, Relaxed);
        self.allocated_bytes.fetch_add(size as u64, Relaxed);
        self.live_bytes.fetch_add(size as u64, Relaxed);
    }

    fn reallocated(&self, old: usize, new: usize) {
        self.reallocations.fetch_add(1, Relaxed);
        self.allocated_bytes.fetch_add(new as u64, Relaxed);
        self.freed_bytes.fetch_add(old as u64, Relaxed);
        if new >= old {
            self.live_bytes.fetch_add((new - old) as u64, Relaxed);
        } else {
            self.live_bytes.fetch_sub((old - new) as u64, Relaxed);
        }
    }

    fn deallocated(&self, size: usize) {
        self.deallocations.fetch_add(1, Relaxed);
        self.freed_bytes.fetch_add(size as u64, Relaxed);
        self.live_bytes.fetch_sub(size as u64, Relaxed);
    }

    fn snapshot(&self) -> [u64; 6] {
        [
            self.allocations.load(Relaxed),
            self.reallocations.load(Relaxed),
            self.deallocations.load(Relaxed),
            self.allocated_bytes.load(Relaxed),
            self.freed_bytes.load(Relaxed),
            self.live_bytes.load(Relaxed),
        ]
    }
}

struct MeasuredSystem(Counts);

// SAFETY: every allocation and release is delegated to System with the caller's
// unchanged layout/pointer. Counter operations cannot allocate, panic or unwind.
unsafe impl GlobalAlloc for MeasuredSystem {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller supplies GlobalAlloc's valid allocation layout.
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            self.0.allocated(layout.size());
        }
        p
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller supplies GlobalAlloc's valid allocation layout.
        let p = unsafe { System.alloc_zeroed(layout) };
        if !p.is_null() {
            self.0.allocated(layout.size());
        }
        p
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the caller supplies a live allocation and its original layout.
        unsafe { System.dealloc(ptr, layout) };
        self.0.deallocated(layout.size());
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: the caller supplies a live allocation, its original layout and
        // a valid nonzero new size. On failure System retains the old allocation.
        let p = unsafe { System.realloc(ptr, layout, new_size) };
        if !p.is_null() {
            self.0.reallocated(layout.size(), new_size);
        }
        p
    }
}

#[global_allocator]
static ALLOCATOR: MeasuredSystem = MeasuredSystem(Counts::new());

const KEYS: [&str; 6] = [
    "allocations",
    "reallocations",
    "deallocations",
    "allocated_bytes",
    "freed_bytes",
    "live_bytes",
];
const SCOPE: &str = "native Rust allocator requests; excludes Lua/C/tuples/RSS";

/// Read-only observer, with no Rust allocation of its own. Counters are module
/// wide, cumulative, wrapping u64, and independently sampled: observe between
/// workload windows, rather than expecting a multi-field atomic snapshot while
/// other threads allocate. Returning these bytes through Tarantool may allocate
/// in C/Lua, which is outside this counter's scope.
#[cfg(not(test))]
pub(crate) fn reply() -> ([u8; 512], usize) {
    encode(ALLOCATOR.0.snapshot())
}

fn encode(values: [u64; 6]) -> ([u8; 512], usize) {
    // The fixed schema fits even with six uint64 values. Strings use str8 to
    // avoid a Vec-backed writer changing the next measurement's counters.
    let mut out = [0u8; 512];
    out[0] = 0x92;
    let mut pos = 1;
    string(&mut out, &mut pos, "native_batch_allocs");
    out[pos] = 0x87;
    pos += 1;
    for (key, value) in KEYS.into_iter().zip(values) {
        string(&mut out, &mut pos, key);
        out[pos] = 0xcf;
        pos += 1;
        out[pos..pos + 8].copy_from_slice(&value.to_be_bytes());
        pos += 8;
    }
    string(&mut out, &mut pos, "scope");
    string(&mut out, &mut pos, SCOPE);
    (out, pos)
}

fn string(out: &mut [u8; 512], pos: &mut usize, value: &str) {
    out[*pos] = 0xd9;
    out[*pos + 1] = value.len() as u8;
    *pos += 2;
    out[*pos..*pos + value.len()].copy_from_slice(value.as_bytes());
    *pos += value.len();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mp::Rd;

    #[test]
    fn accounting_tracks_requested_capacity_across_grow_and_shrink() {
        let allocator = MeasuredSystem(Counts::new());
        // SAFETY: each successful System request is released once with its
        // current layout; realloc failures retain the previous allocation.
        unsafe {
            let layout = Layout::from_size_align(32, 8).unwrap();
            let p = allocator.alloc_zeroed(layout);
            assert!(!p.is_null());
            assert_eq!(std::slice::from_raw_parts(p, 32), &[0u8; 32]);
            assert_eq!(allocator.0.snapshot(), [1, 0, 0, 32, 0, 32]);
            let q = allocator.realloc(p, layout, 80);
            if q.is_null() {
                allocator.dealloc(p, layout);
                panic!("small test realloc failed");
            }
            assert_eq!(allocator.0.snapshot(), [1, 1, 0, 112, 32, 80]);
            let layout = Layout::from_size_align(80, 8).unwrap();
            let r = allocator.realloc(q, layout, 16);
            if r.is_null() {
                allocator.dealloc(q, layout);
                panic!("small test realloc failed");
            }
            assert_eq!(allocator.0.snapshot(), [1, 2, 0, 128, 112, 16]);
            allocator.dealloc(r, Layout::from_size_align(16, 8).unwrap());
            assert_eq!(allocator.0.snapshot(), [1, 2, 1, 128, 128, 0]);
        }
    }

    #[test]
    fn observer_encodes_full_width_counters_with_an_exact_scope() {
        let values = [0, 1, 255, 256, 1 << 40, u64::MAX];
        let (out, len) = encode(values);
        let mut r = Rd::new(&out[..len]);
        assert_eq!(r.array(), Some(2));
        assert_eq!(r.str(), Some(b"native_batch_allocs".as_slice()));
        assert_eq!(r.map(), Some(7));
        for (key, value) in KEYS.into_iter().zip(values) {
            assert_eq!(r.str(), Some(key.as_bytes()));
            assert_eq!(r.uint(), Some(value));
        }
        assert_eq!(r.str(), Some(b"scope".as_slice()));
        assert_eq!(r.str(), Some(SCOPE.as_bytes()));
        assert_eq!(r.peek(), None);
    }
}
