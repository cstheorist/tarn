//! The procedures Tarantool calls (`box.schema.func.create('tarn_storage.<name>',
//! {language = 'C'})`), and the part of Tarantool's module API they use
//! (module.h).

use crate::mp::{Rd, Wr};
use crate::select::{self, Page, ITER_EQ};
use crate::state::{State, LAYOUT};
use std::cell::RefCell;
use std::collections::HashMap;
use std::os::raw::{c_char, c_int, c_uint};
use std::sync::atomic::{AtomicBool, Ordering};

#[repr(C)]
pub struct BoxFunctionCtx {
    _p: [u8; 0],
}
#[repr(C)]
pub struct BoxTuple {
    _p: [u8; 0],
}
#[repr(C)]
pub struct BoxIterator {
    _p: [u8; 0],
}

extern "C" {
    fn box_return_mp(ctx: *mut BoxFunctionCtx, mp: *const c_char, mp_end: *const c_char) -> c_int;
    fn box_index_get(
        space_id: u32,
        index_id: u32,
        key: *const c_char,
        key_end: *const c_char,
        result: *mut *mut BoxTuple,
    ) -> c_int;
    fn box_index_iterator(
        space_id: u32,
        index_id: u32,
        type_: c_int,
        key: *const c_char,
        key_end: *const c_char,
    ) -> *mut BoxIterator;
    fn box_iterator_next(it: *mut BoxIterator, result: *mut *mut BoxTuple) -> c_int;
    fn box_iterator_free(it: *mut BoxIterator);
    fn box_tuple_data(tuple: *mut BoxTuple, bsize: *mut usize) -> *const c_char;
    fn box_tuple_field(tuple: *mut BoxTuple, fieldno: u32) -> *const c_char;
    fn box_txn() -> bool;
    fn box_is_ro() -> bool;
    fn box_schema_version() -> u64;
    fn box_error_clear();
    fn box_error_set(file: *const c_char, line: c_uint, code: u32, format: *const c_char, ...) -> c_int;
    fn fiber_clock() -> f64;
}

/// System spaces (box.schema.SPACE_ID, INDEX_ID).
const SPACE_SPACE: u32 = 280;
const SPACE_INDEX: u32 = 288;

/// ER_LOAD_FUNCTION: "Failed to dynamically load function".
const ER_LOAD_FUNCTION: u32 = 99;

/// The Lua module expects another layout of the state (`ts_state`): the
/// procedures then fail as the functions of a module that does not load,
/// and the SQL servers stop calling them.
static OTHER_LAYOUT: AtomicBool = AtomicBool::new(false);

/// The state the Lua module keeps current: allocated by the first
/// `ts_state` call, never freed. (The procedures and the Lua module run in
/// the transaction thread only.)
struct Global(std::cell::UnsafeCell<*mut State>);
// SAFETY: only the transaction thread touches it.
unsafe impl Sync for Global {}
static STATE: Global = Global(std::cell::UnsafeCell::new(std::ptr::null_mut()));

fn state() -> Option<&'static mut State> {
    // SAFETY: see Global; the pointer, once set, stays valid.
    unsafe {
        let p = *STATE.0.get();
        if p.is_null() {
            None
        } else {
            Some(&mut *p)
        }
    }
}

/// What a space's indexes are, as far as reads need: per (space, index),
/// whether the space is memtx and whether the index is unique and how many
/// parts it has. Read from _space and _index, kept for one schema version.
#[derive(Clone, Copy)]
struct IndexInfo {
    memtx: bool,
    unique: bool,
    parts: u32,
}

/// IndexInfo by (space, index), of one schema version.
type IndexCache = (u64, HashMap<(u32, u32), Option<IndexInfo>>);

thread_local! {
    static INDEXES: RefCell<IndexCache> = RefCell::new((0, HashMap::new()));
}

/// A field of a tuple as a MessagePack slice (to the tuple's end: the
/// reader stops at the field's end).
unsafe fn field<'a>(t: *mut BoxTuple, i: u32) -> Option<Rd<'a>> {
    let mut size = 0usize;
    let data = box_tuple_data(t, &mut size);
    let f = box_tuple_field(t, i);
    if f.is_null() || data.is_null() {
        return None;
    }
    let start = (f as usize).checked_sub(data as usize)?;
    let all = std::slice::from_raw_parts(data as *const u8, size);
    Some(Rd::new(all.get(start..)?))
}

/// IndexInfo of index `index_id` of space `space_id`, or None when either
/// does not exist.
fn index_info(space_id: u32, index_id: u32) -> Option<IndexInfo> {
    // SAFETY: plain calls of the module API in the transaction thread.
    let ver = unsafe { box_schema_version() };
    if let Some(i) = INDEXES.with(|c| {
        let c = c.borrow();
        if c.0 == ver {
            c.1.get(&(space_id, index_id)).copied()
        } else {
            None
        }
    }) {
        return i;
    }
    let info = unsafe { read_index_info(space_id, index_id) };
    INDEXES.with(|c| {
        let mut c = c.borrow_mut();
        if c.0 != ver {
            c.0 = ver;
            c.1.clear();
        }
        c.1.insert((space_id, index_id), info);
    });
    info
}

unsafe fn read_index_info(space_id: u32, index_id: u32) -> Option<IndexInfo> {
    let mut key = Wr::with_capacity(16);
    key.array(1).uint(space_id as u64);
    let mut t: *mut BoxTuple = std::ptr::null_mut();
    if box_index_get(
        SPACE_SPACE,
        0,
        key.0.as_ptr().cast(),
        key.0.as_ptr().add(key.0.len()).cast(),
        &mut t,
    ) != 0
    {
        box_error_clear();
        return None;
    }
    if t.is_null() {
        return None;
    }
    // _space: [id, owner, name, engine, ...]
    let memtx = field(t, 3)?.str()? == b"memtx";
    let mut key = Wr::with_capacity(16);
    key.array(2).uint(space_id as u64).uint(index_id as u64);
    let mut t: *mut BoxTuple = std::ptr::null_mut();
    if box_index_get(
        SPACE_INDEX,
        0,
        key.0.as_ptr().cast(),
        key.0.as_ptr().add(key.0.len()).cast(),
        &mut t,
    ) != 0
    {
        box_error_clear();
        return None;
    }
    if t.is_null() {
        return None;
    }
    // _index: [space_id, index_id, name, type, opts, parts]
    let mut unique = true;
    let mut opts = field(t, 4)?;
    for _ in 0..opts.map()? {
        match opts.str() {
            Some(b"unique") => unique = opts.bool()?,
            _ => opts.skip()?,
        }
    }
    let parts = field(t, 5)?.array()?;
    Some(IndexInfo { memtx, unique, parts })
}

/// Reply with one MessagePack value.
unsafe fn reply(ctx: *mut BoxFunctionCtx, mp: &[u8]) -> c_int {
    box_return_mp(ctx, mp.as_ptr().cast(), mp.as_ptr().add(mp.len()).cast())
}

const TO_LUA: &[u8] = &[0x91, 0xa3, b'l', b'u', b'a'];

/// `ts_state([layout])`: the address of the state, which the Lua module
/// keeps current (lua/tarn.lua, "Native procedures"). `layout` is the one
/// the Lua module declares: when it is not this module's, the procedures
/// fail (until a call with this module's).
///
/// # Safety
/// Called by Tarantool with a valid context and argument buffer.
#[no_mangle]
pub unsafe extern "C" fn ts_state(ctx: *mut BoxFunctionCtx, args: *const c_char, args_end: *const c_char) -> c_int {
    let len = (args_end as usize).saturating_sub(args as usize);
    let mut r = Rd::new(std::slice::from_raw_parts(args as *const u8, len));
    if r.array().is_some_and(|n| n >= 1) {
        if let Some(l) = r.uint() {
            OTHER_LAYOUT.store(l != LAYOUT as u64, Ordering::Relaxed);
        }
    }
    let p = *STATE.0.get();
    let p = if p.is_null() {
        let p = Box::into_raw(Box::new(State::new()));
        *STATE.0.get() = p;
        p
    } else {
        p
    };
    let mut w = Wr::with_capacity(16);
    w.uint(p as u64);
    reply(ctx, &w.0)
}

/// `ts_select(...)`: tarn_select's arguments; its reply, or ["lua"] (see
/// select.rs).
///
/// # Safety
/// Called by Tarantool with a valid context and argument buffer.
#[no_mangle]
pub unsafe extern "C" fn ts_select(ctx: *mut BoxFunctionCtx, args: *const c_char, args_end: *const c_char) -> c_int {
    if OTHER_LAYOUT.load(Ordering::Relaxed) {
        return box_error_set(
            c"tarn-storage".as_ptr(),
            line!(),
            ER_LOAD_FUNCTION,
            c"tarn_storage: this build of the module does not match lua/tarn.lua (its state layout)".as_ptr(),
        );
    }
    let len = (args_end as usize).saturating_sub(args as usize);
    let buf = std::slice::from_raw_parts(args as *const u8, len);
    let out = std::panic::catch_unwind(|| select_page(buf)).unwrap_or(None);
    match out {
        Some(page) => {
            if let Some(s) = state() {
                s.served += 1;
            }
            reply(ctx, &page)
        }
        None => {
            if let Some(s) = state() {
                s.to_lua += 1;
            }
            reply(ctx, TO_LUA)
        }
    }
}

/// The reply to a read ts_select serves (["ok", rows]), or None for one it
/// leaves to tarn_select.
fn select_page(buf: &[u8]) -> Option<Vec<u8>> {
    let a = select::parse(buf)?;
    let s = state()?;
    if s.ready == 0 || !select::route_ok(s, &a.route) {
        return None;
    }
    // SAFETY: plain calls of the module API in the transaction thread.
    unsafe {
        // (the window of the load counts ended: tarn_select starts the next
        // one, then counts the read)
        if select::counted(&a.route) && fiber_clock() >= s.load_until {
            return None;
        }
        // (a read in a stream transaction, and a read with lock_check on a
        // replica, which tarn_select refuses: ER_READONLY)
        if box_txn() || (a.lock_check && (box_is_ro() || s.lk_stale != 0)) {
            return None;
        }
    }
    let info = index_info(a.space_id, a.index_id)?;
    // (vinyl reads may yield; prepared transactions' locks, or a held
    // table: the lock check may have to wait)
    if !info.memtx || (a.lock_check && (s.ddl_n != 0 || s.fence_stale != 0 || s.space_locked(a.space_id))) {
        return None;
    }
    let mut page = Page::new(a.limit, s.page_rows, s.page_bytes);
    let mut out = Wr::with_capacity(256);
    out.array(2).str("ok");
    let at = out.array_slot();
    let mut n = 0u32;
    let key_parts = Rd::new(a.key).array()?;
    // SAFETY: as above; the key is a MessagePack array; tuples are read
    // before anything else runs.
    unsafe {
        let key = a.key.as_ptr().cast::<c_char>();
        let key_end = key.add(a.key.len());
        if a.iterator == ITER_EQ && info.unique && key_parts == info.parts {
            let mut t: *mut BoxTuple = std::ptr::null_mut();
            if box_index_get(a.space_id, a.index_id, key, key_end, &mut t) != 0 {
                box_error_clear();
                return None;
            }
            if !t.is_null() {
                let mut size = 0usize;
                let data = box_tuple_data(t, &mut size);
                page.row(size as u64);
                out.raw(std::slice::from_raw_parts(data as *const u8, size));
                n = 1;
            }
        } else {
            let it = box_index_iterator(a.space_id, a.index_id, a.iterator as c_int, key, key_end);
            if it.is_null() {
                box_error_clear();
                return None;
            }
            while (n as u64) < page.n {
                let mut t: *mut BoxTuple = std::ptr::null_mut();
                if box_iterator_next(it, &mut t) != 0 {
                    box_iterator_free(it);
                    box_error_clear();
                    return None;
                }
                if t.is_null() {
                    break;
                }
                let mut size = 0usize;
                let data = box_tuple_data(t, &mut size);
                page.row(size as u64);
                out.raw(std::slice::from_raw_parts(data as *const u8, size));
                n += 1;
            }
            box_iterator_free(it);
        }
    }
    if !page.whole() {
        return None;
    }
    out.patch_array(at, n);
    select::count_reads(s, &a.route);
    Some(out.0)
}
