//! What the native procedures know of the shard's state: a struct the Lua
//! module (lua/tarn.lua, "Native procedures") keeps current through LuaJIT's
//! FFI. Both run in the transaction thread, one at a time, and a native
//! procedure does not yield, so it sees the state as of one moment, as a Lua
//! procedure's step that does not yield does.
//!
//! The layout is declared again in lua/tarn.lua (`struct tarn_ns`): change
//! both, and `LAYOUT` with them.

/// The version of this layout; the Lua module checks it.
pub const LAYOUT: u32 = 2;

/// The slots of `space_locks`.
pub const SPACES: u32 = 65536;

#[repr(C)]
pub struct State {
    pub layout: u32,
    /// The Lua module keeps the fields below current. Until it does, every
    /// request goes to Lua.
    pub ready: u8,
    /// BK.stale: the bucket map is read again before its next use.
    pub bk_stale: u8,
    /// LK_STALE: the lock index is rebuilt before its next use.
    pub lk_stale: u8,
    /// FENCE_STALE: the definition fences and the tables held by a TRUNCATE
    /// or DROP TABLE (DDL_N) are read again before their next use.
    pub fence_stale: u8,
    /// BK.count: the number of buckets (0: no bucket map).
    pub count: u32,
    /// DDL_N: the tables a TRUNCATE or DROP TABLE holds.
    pub ddl_n: u32,
    /// cfg.scan_page_rows: the most rows one read returns.
    pub page_rows: u32,
    /// SCAN_CHUNK: a read inside a transaction with no limit, or a limit
    /// above this, is a scan that may yield (PG.scan), whose pages end as a
    /// scan's do.
    pub scan_chunk: u32,
    /// cfg.scan_page_bytes: and their most bytes.
    pub page_bytes: u64,
    /// BK.epoch.
    pub epoch: u64,
    /// BK.mver: the membership version.
    pub mver: u64,
    /// When the current window of the load counts ends (BK.load: t0 +
    /// cfg.load_window), by the event loop's monotonic clock (fiber.clock()).
    /// A read counted after it goes to Lua, which starts the next window.
    pub load_until: f64,
    /// [count + 1]: 1 where this shard serves reads of the bucket (its
    /// state is active, sending or fenced: see BK.bad), else 0.
    pub serves: *mut u8,
    /// [count + 1]: the reads routed by each bucket in the current window of
    /// the load counts (BK.load).
    pub reads: *mut u32,
    /// [SPACES]: the locks of prepared transactions on the spaces of each
    /// slot, a space in slot id % SPACES (BK.ns_space_locks).
    pub space_locks: *mut u32,
    /// Reads served here, and handed to the Lua procedure.
    pub served: u64,
    pub to_lua: u64,
}

impl Default for State {
    fn default() -> State {
        State::new()
    }
}

impl State {
    pub const fn new() -> State {
        State {
            layout: LAYOUT,
            ready: 0,
            bk_stale: 1,
            lk_stale: 1,
            fence_stale: 1,
            count: 0,
            ddl_n: 0,
            page_rows: 0,
            scan_chunk: 0,
            page_bytes: 0,
            epoch: 0,
            mver: 0,
            load_until: 0.0,
            serves: std::ptr::null_mut(),
            reads: std::ptr::null_mut(),
            space_locks: std::ptr::null_mut(),
            served: 0,
            to_lua: 0,
        }
    }

    /// Whether this shard serves reads routed by bucket b.
    pub fn serves(&self, b: u64) -> bool {
        if self.serves.is_null() || b == 0 || b > self.count as u64 {
            return false;
        }
        // SAFETY: the Lua module keeps `serves` an array of count + 1 bytes
        // (BK.ns_arrays), alive while the pointer is set.
        unsafe { *self.serves.add(b as usize) != 0 }
    }

    /// Whether prepared transactions may hold locks on space `id` (unknown:
    /// yes).
    pub fn space_locked(&self, id: u32) -> bool {
        if self.space_locks.is_null() {
            return true;
        }
        // SAFETY: the Lua module keeps `space_locks` an array of SPACES
        // counters, alive while the pointer is set.
        unsafe { *self.space_locks.add((id % SPACES) as usize) != 0 }
    }

    /// Count a read routed by bucket b (BK.count_reads).
    pub fn count_read(&mut self, b: u64) {
        if self.reads.is_null() || b == 0 || b > self.count as u64 {
            return;
        }
        // SAFETY: as for `serves`, an array of count + 1 counters.
        unsafe {
            let p = self.reads.add(b as usize);
            *p = (*p).wrapping_add(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_matches_the_lua_declaration() {
        // (offsets as lua/tarn.lua's struct tarn_ns has them on x86_64 and
        // aarch64)
        use std::mem::offset_of;
        assert_eq!(offset_of!(State, ready), 4);
        assert_eq!(offset_of!(State, count), 8);
        assert_eq!(offset_of!(State, ddl_n), 12);
        assert_eq!(offset_of!(State, page_rows), 16);
        assert_eq!(offset_of!(State, scan_chunk), 20);
        assert_eq!(offset_of!(State, page_bytes), 24);
        assert_eq!(offset_of!(State, epoch), 32);
        assert_eq!(offset_of!(State, mver), 40);
        assert_eq!(offset_of!(State, load_until), 48);
        assert_eq!(offset_of!(State, serves), 56);
        assert_eq!(offset_of!(State, reads), 64);
        assert_eq!(offset_of!(State, space_locks), 72);
        assert_eq!(offset_of!(State, served), 80);
        assert_eq!(offset_of!(State, to_lua), 88);
        assert_eq!(std::mem::size_of::<State>(), 96);
    }

    #[test]
    fn buckets() {
        let mut serves = vec![0u8; 5];
        let mut reads = vec![0u32; 5];
        serves[3] = 1;
        let mut s = State::new();
        assert!(!s.serves(3)); // (no map yet)
        s.count = 4;
        s.serves = serves.as_mut_ptr();
        s.reads = reads.as_mut_ptr();
        assert!(s.serves(3) && !s.serves(2) && !s.serves(0) && !s.serves(5));
        s.count_read(3);
        s.count_read(3);
        s.count_read(9);
        assert_eq!(reads, vec![0, 0, 0, 2, 0]);
        // locks by space
        assert!(s.space_locked(512), "unknown: locked");
        let mut locks = vec![0u32; SPACES as usize];
        s.space_locks = locks.as_mut_ptr();
        locks[512] = 1;
        assert!(s.space_locked(512) && s.space_locked(512 + SPACES) && !s.space_locked(513));
    }
}
