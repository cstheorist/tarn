//! What the native procedures know of the shard's state: a struct the Lua
//! module (lua/tarn.lua, "Native procedures") keeps current through LuaJIT's
//! FFI. Both run in the transaction thread, one at a time. Native checks
//! see one moment while they do not yield; a commit can yield for WAL or
//! quorum and must reacquire the state afterward.
//!
//! The layout is declared again in lua/tarn.lua (`struct tarn_ns`): change
//! both, and `LAYOUT` with them.

use crate::locks;

/// The version of this layout; the Lua module checks it.
pub const LAYOUT: u32 = 14;

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
    /// [count + 1]: what this shard serves of each bucket (BK.bad): 2 reads
    /// and writes (active), 1 reads only (sending or fenced: a write to a
    /// bucket being sent is noted for its move, which the Lua procedures
    /// do), 0 nothing.
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
    /// PL_N: the row locks held (READ COMMITTED).
    pub pl_n: u32,
    /// The test hooks held (tarn_test_hook): while any is, every request
    /// goes to Lua, whose steps the hooks hold.
    pub hooks: u32,
    /// The native commits, numbered as they start (box_txn_commit()), and
    /// the oldest one in flight (u64::MAX: none): what waits for the commits
    /// in flight (wait_commits) waits for those that started before it.
    pub commit_seq: u64,
    pub oldest_inflight: u64,
    /// [count + 1]: the rows written to each bucket in the current window of
    /// the load counts, and their bytes (BK.load).
    pub writes: *mut u32,
    pub wbytes: *mut f64,
    /// [SPACES]: the highest definition fence (FENCE) of the spaces of each
    /// slot: a write made with an older definition is refused (1412).
    pub fences: *mut u64,
    /// Autocommit writes served here, and handed to the Lua procedure.
    pub w_served: u64,
    pub w_to_lua: u64,
    /// [SPACES]: the row locks held on the spaces of each slot (a space in
    /// slot id % SPACES): a write to a space without any needs none.
    pub pl_spaces: *mut u32,
    /// BK.held: the buckets held here (moving to this shard, their rows
    /// staged): a commit that read every shard may have to see them.
    pub held: u32,
    /// The space _tarn_scratch, target of tarn_validate's probe write (0:
    /// not made yet).
    pub scratch: u32,
    /// Commits of one shard (tarn_commit_local) served here, and handed to
    /// the Lua procedure.
    pub c_served: u64,
    pub c_to_lua: u64,
    /// TERM_TXN: the number (box_txn_id) of a transaction begun once this
    /// node saw its election term; a stream transaction below it may have
    /// begun in an earlier term.
    pub term_txn: u64,
    /// The local lazy-capture registry is unknown or stale. Native writes
    /// cannot capture an outbox, so every one must go through Lua until
    /// the registry is rebuilt. Reads do not depend on this flag.
    pub capture_stale: u8,
    /// [SPACES]: a source with active or prepared lazy capture excludes
    /// its slot from native writes. Hash collisions only overexclude.
    pub write_excluded: *mut u8,
    /// The number of a transaction begun once the last TRUNCATE or DROP
    /// TABLE was applied here (DDL_TXN): a stream transaction below it may
    /// have read a table one changed since.
    pub ddl_txn: u64,
    /// Validations (tarn_validate) served here, and handed to the Lua
    /// procedure.
    pub v_served: u64,
    pub v_to_lua: u64,
    /// The functions that keep this module's copy of the lock index
    /// (locks.rs), which the Lua module calls with each change of it.
    pub lk_point: unsafe extern "C" fn(u32, u32, *const u8, usize, u8, u8),
    pub lk_range: unsafe extern "C" fn(u32, u32, *const u8, usize, *const u8, usize, i32),
    pub pl_key: unsafe extern "C" fn(u32, u32, *const u8, usize, i32),
    pub locks_clear: extern "C" fn(u8),
    /// (and those of its check of the copy: cfg.check_locks)
    pub locks_key: unsafe extern "C" fn(u32, u32, *const u8, usize, *mut u32),
    pub locks_index: unsafe extern "C" fn(u32, u32, *mut u32),
    pub locks_sizes: unsafe extern "C" fn(*mut u32),
    pub locks_range: unsafe extern "C" fn(u32, u32, *const u8, usize, *const u8, usize, *mut u32),
    /// Why the procedures handed requests back (fallback.rs), for
    /// tarn_info().
    pub fallbacks: unsafe extern "C" fn(*mut u8, usize) -> usize,
    /// Freshness configuration: 0 unknown, 1 standalone, 2 synchronous
    /// replication, 3 unsupported. Revalidated through the Lua API guard at
    /// each native read; this cached value is not a quorum certificate.
    pub fresh_mode: u8,
    pub fresh_clock_ok: u8,
    pub fresh_reserved: [u8; 6],
    /// Final timeout of an owned empty autocommit barrier. Stream reads
    /// retain their existing sixty-second transaction timeout instead.
    pub read_timeout: f64,
    /// Nanosecond clock-health observation and its conservative lifetime.
    pub clock_mono: u64,
    pub clock_wall: u64,
    pub clock_max_gap: u64,
    pub clock_max_drift: u64,
    /// ABI13 stored LuaJIT FFI callback pointers here. ABI14 enters the
    /// public protected Lua API instead; never invoke those old pointers.
    pub fresh_guard_reserved: [u64; 4],
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
            pl_n: 0,
            hooks: 0,
            commit_seq: 0,
            oldest_inflight: u64::MAX,
            writes: std::ptr::null_mut(),
            wbytes: std::ptr::null_mut(),
            fences: std::ptr::null_mut(),
            w_served: 0,
            w_to_lua: 0,
            pl_spaces: std::ptr::null_mut(),
            held: 0,
            scratch: 0,
            c_served: 0,
            c_to_lua: 0,
            term_txn: 0,
            capture_stale: 1,
            write_excluded: std::ptr::null_mut(),
            ddl_txn: 0,
            v_served: 0,
            v_to_lua: 0,
            lk_point: locks::lk_point,
            lk_range: locks::lk_range,
            pl_key: locks::pl_key,
            locks_clear: locks::locks_clear,
            locks_key: locks::locks_key,
            locks_index: locks::locks_index,
            locks_sizes: locks::locks_sizes,
            locks_range: locks::locks_range,
            fallbacks: crate::fallback::fallbacks,
            fresh_mode: 0,
            fresh_clock_ok: 0,
            fresh_reserved: [0; 6],
            read_timeout: 0.0,
            clock_mono: 0,
            clock_wall: 0,
            clock_max_gap: 0,
            clock_max_drift: 0,
            fresh_guard_reserved: [0; 4],
        }
    }

    /// Whether this shard serves reads routed by bucket b.
    pub fn serves(&self, b: u64) -> bool {
        self.serving(b) != 0
    }

    /// Whether this shard takes writes to bucket b here (it is active).
    pub fn writable(&self, b: u64) -> bool {
        self.serving(b) == 2
    }

    fn serving(&self, b: u64) -> u8 {
        if self.serves.is_null() || b == 0 || b > self.count as u64 {
            return 0;
        }
        // SAFETY: the Lua module keeps `serves` an array of count + 1 bytes
        // (BK.ns_serves_all), alive while the pointer is set.
        unsafe { *self.serves.add(b as usize) }
    }

    /// The highest definition fence of space `id`'s slot (unknown: none
    /// lower than any).
    pub fn fence(&self, id: u32) -> u64 {
        if self.fences.is_null() {
            return u64::MAX;
        }
        // SAFETY: the Lua module keeps `fences` an array of SPACES
        // versions, alive while the pointer is set.
        unsafe { *self.fences.add((id % SPACES) as usize) }
    }

    /// Native writes cannot maintain a lazy source's commit-atomic outbox.
    /// Unknown state is excluded too, including after a leader change and
    /// while local DDL capture metadata is being activated. Check actual
    /// write space IDs, independently of the gateway's version/fence list.
    pub fn lazy_write_excluded(&self, id: u32) -> bool {
        if self.capture_stale != 0 || self.write_excluded.is_null() {
            return true;
        }
        // SAFETY: Lua keeps an array of SPACES bytes alive while its pointer
        // is set, on the same transaction thread as the native procedures.
        unsafe { *self.write_excluded.add((id % SPACES) as usize) != 0 }
    }

    /// Count a row of `bytes` written to bucket b (BK.count_writes).
    pub fn count_write(&mut self, b: u64, bytes: u64) {
        if self.writes.is_null() || self.wbytes.is_null() || b == 0 || b > self.count as u64 {
            return;
        }
        // SAFETY: as for `reads`, arrays of count + 1.
        unsafe {
            let p = self.writes.add(b as usize);
            *p = (*p).wrapping_add(1);
            *self.wbytes.add(b as usize) += bytes as f64;
        }
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

    /// Whether row locks may be held on space `id` (unknown: whether any
    /// is held).
    pub fn space_plocked(&self, id: u32) -> bool {
        if self.pl_n == 0 {
            return false;
        }
        if self.pl_spaces.is_null() {
            return true;
        }
        // SAFETY: the Lua module keeps `pl_spaces` an array of SPACES
        // counters, alive while the pointer is set.
        unsafe { *self.pl_spaces.add((id % SPACES) as usize) != 0 }
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
        assert_eq!(offset_of!(State, pl_n), 96);
        assert_eq!(offset_of!(State, hooks), 100);
        assert_eq!(offset_of!(State, commit_seq), 104);
        assert_eq!(offset_of!(State, oldest_inflight), 112);
        assert_eq!(offset_of!(State, writes), 120);
        assert_eq!(offset_of!(State, wbytes), 128);
        assert_eq!(offset_of!(State, fences), 136);
        assert_eq!(offset_of!(State, w_served), 144);
        assert_eq!(offset_of!(State, w_to_lua), 152);
        assert_eq!(offset_of!(State, pl_spaces), 160);
        assert_eq!(offset_of!(State, held), 168);
        assert_eq!(offset_of!(State, scratch), 172);
        assert_eq!(offset_of!(State, c_served), 176);
        assert_eq!(offset_of!(State, c_to_lua), 184);
        assert_eq!(offset_of!(State, term_txn), 192);
        assert_eq!(offset_of!(State, capture_stale), 200);
        assert_eq!(offset_of!(State, write_excluded), 208);
        assert_eq!(offset_of!(State, ddl_txn), 216);
        assert_eq!(offset_of!(State, v_served), 224);
        assert_eq!(offset_of!(State, v_to_lua), 232);
        assert_eq!(offset_of!(State, lk_point), 240);
        assert_eq!(offset_of!(State, lk_range), 248);
        assert_eq!(offset_of!(State, pl_key), 256);
        assert_eq!(offset_of!(State, locks_clear), 264);
        assert_eq!(offset_of!(State, locks_key), 272);
        assert_eq!(offset_of!(State, locks_index), 280);
        assert_eq!(offset_of!(State, locks_sizes), 288);
        assert_eq!(offset_of!(State, locks_range), 296);
        assert_eq!(offset_of!(State, fallbacks), 304);
        assert_eq!(offset_of!(State, fresh_mode), 312);
        assert_eq!(offset_of!(State, fresh_clock_ok), 313);
        assert_eq!(offset_of!(State, read_timeout), 320);
        assert_eq!(offset_of!(State, clock_mono), 328);
        assert_eq!(offset_of!(State, clock_wall), 336);
        assert_eq!(offset_of!(State, clock_max_gap), 344);
        assert_eq!(offset_of!(State, clock_max_drift), 352);
        assert_eq!(offset_of!(State, fresh_guard_reserved), 360);
        assert_eq!(std::mem::size_of::<State>(), 392);
    }

    #[test]
    fn lazy_capture_excludes_unknown_sources_and_slot_collisions() {
        let mut s = State::new();
        assert!(s.lazy_write_excluded(512));
        s.capture_stale = 0;
        assert!(
            s.lazy_write_excluded(512),
            "an absent registry is not an empty registry"
        );
        let mut excluded = vec![0u8; SPACES as usize];
        s.write_excluded = excluded.as_mut_ptr();
        assert!(!s.lazy_write_excluded(512));
        excluded[512] = 1;
        assert!(s.lazy_write_excluded(512));
        assert!(s.lazy_write_excluded(512 + SPACES), "collisions must fail closed");
        assert!(!s.lazy_write_excluded(513));
        s.capture_stale = 1;
        assert!(
            s.lazy_write_excluded(513),
            "leader/schema invalidation excludes every source"
        );
        // Capture invalidation is not a reason to abandon native reads.
        let mut serves = vec![0u8, 2];
        s.serves = serves.as_mut_ptr();
        s.count = 1;
        assert!(s.serves(1));
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
        // row locks by space
        assert!(!s.space_plocked(512), "none held");
        s.pl_n = 1;
        assert!(s.space_plocked(512), "unknown: any held");
        let mut pl = vec![0u32; SPACES as usize];
        s.pl_spaces = pl.as_mut_ptr();
        pl[512] = 1;
        assert!(s.space_plocked(512) && s.space_plocked(512 + SPACES) && !s.space_plocked(513));
    }
}
