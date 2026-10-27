//! The procedures Tarantool calls (`box.schema.func.create('tarn_storage.<name>',
//! {language = 'C'})`), and the part of Tarantool's module API they use
//! (module.h).

use crate::mp::{Rd, Wr};
use crate::select::{self, Page, ITER_EQ};
use crate::state::{State, LAYOUT};
use crate::write::{self, Op};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::os::raw::{c_char, c_int, c_uint, c_void};
use std::panic::AssertUnwindSafe;
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
#[repr(C)]
pub struct BoxKeyDef {
    _p: [u8; 0],
}
#[repr(C)]
pub struct BoxError {
    _p: [u8; 0],
}

/// box_key_part_def_t: a union of these fields and 64 bytes.
#[repr(C)]
struct KeyPartDef {
    fieldno: u32,
    flags: u32,
    field_type: *const c_char,
    collation: *const c_char,
    path: *const c_char,
    _pad: [u8; 32],
}
const KEY_PART_IS_NULLABLE: u32 = 1;

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
    fn box_key_part_def_create(part: *mut KeyPartDef);
    fn box_key_def_new_v2(parts: *mut KeyPartDef, part_count: u32) -> *mut BoxKeyDef;
    fn box_key_def_delete(key_def: *mut BoxKeyDef);
    fn box_key_def_validate_key(key_def: *const BoxKeyDef, key: *const c_char, key_size: *mut u32) -> c_int;
    fn box_tuple_compare_with_key(tuple: *mut BoxTuple, key: *const c_char, key_def: *mut BoxKeyDef) -> c_int;
    fn box_error_set(file: *const c_char, line: c_uint, code: u32, format: *const c_char, ...) -> c_int;
    fn box_error_last() -> *mut BoxError;
    fn box_error_code(error: *const BoxError) -> u32;
    fn box_error_message(error: *const BoxError) -> *const c_char;
    fn fiber_clock() -> f64;
    fn box_txn_begin() -> c_int;
    fn box_txn_commit() -> c_int;
    fn box_txn_rollback() -> c_int;
    fn box_txn_savepoint() -> *mut c_void;
    fn box_txn_rollback_to_savepoint(savepoint: *mut c_void) -> c_int;
    fn box_txn_id() -> i64;
    fn box_txn_set_isolation(level: u32) -> c_int;
    fn box_txn_set_timeout(timeout: f64) -> c_int;
    fn box_insert(space_id: u32, tuple: *const c_char, tuple_end: *const c_char, result: *mut *mut BoxTuple) -> c_int;
    fn box_replace(space_id: u32, tuple: *const c_char, tuple_end: *const c_char, result: *mut *mut BoxTuple) -> c_int;
    fn box_delete(
        space_id: u32,
        index_id: u32,
        key: *const c_char,
        key_end: *const c_char,
        result: *mut *mut BoxTuple,
    ) -> c_int;
    #[allow(clippy::too_many_arguments)]
    fn box_update(
        space_id: u32,
        index_id: u32,
        key: *const c_char,
        key_end: *const c_char,
        ops: *const c_char,
        ops_end: *const c_char,
        index_base: c_int,
        result: *mut *mut BoxTuple,
    ) -> c_int;
}

/// System spaces (box.schema.SPACE_ID, INDEX_ID, COLLATION_ID).
const SPACE_SPACE: u32 = 280;
const SPACE_INDEX: u32 = 288;
const SPACE_COLLATION: u32 = 276;

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
/// whether the space is memtx, whether the index is unique, how many parts
/// it has, and its key definition (to compare rows with an end key, as
/// tarn_select's key_def of the index does: null when its parts are of a
/// kind this module does not build). Read from _space and _index, kept for
/// one schema version.
#[derive(Clone, Copy)]
struct IndexInfo {
    memtx: bool,
    unique: bool,
    parts: u32,
    key_def: *mut BoxKeyDef,
    /// The fields of its parts (from 0), when there are at most 8 and none
    /// has a path: what a key of the index is made of (else nfields = 0).
    fields: [u32; 8],
    nfields: u8,
}

/// What a space is, as far as writes need: memtx, and the field (from 0)
/// of its rows' bucket (BK.field: the first part of its index bucket_id).
#[derive(Clone, Copy)]
struct SpaceInfo {
    memtx: bool,
    bucket_field: Option<u32>,
}

/// IndexInfo by (space, index) and SpaceInfo by space, of one schema
/// version.
struct IndexCache {
    ver: u64,
    map: HashMap<(u32, u32), Option<IndexInfo>>,
    spaces: HashMap<u32, Option<SpaceInfo>>,
}

impl IndexCache {
    /// Forget every entry (another schema version).
    fn reset(&mut self, ver: u64) {
        for i in self.map.values().flatten() {
            if !i.key_def.is_null() {
                // SAFETY: built by box_key_def_new_v2, held only here.
                unsafe { box_key_def_delete(i.key_def) };
            }
        }
        self.map.clear();
        self.spaces.clear();
        self.ver = ver;
    }
}

thread_local! {
    static INDEXES: RefCell<IndexCache> = RefCell::new(IndexCache {
        ver: 0,
        map: HashMap::new(),
        spaces: HashMap::new(),
    });
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
        if c.ver == ver {
            c.map.get(&(space_id, index_id)).copied()
        } else {
            None
        }
    }) {
        return i;
    }
    let info = unsafe { read_index_info(space_id, index_id) };
    INDEXES.with(|c| {
        let mut c = c.borrow_mut();
        if c.ver != ver {
            c.reset(ver);
        }
        c.map.insert((space_id, index_id), info);
    });
    info
}

/// A key part of an _index tuple's parts: {field = n, type = s[, is_nullable
/// = b][, collation = id]...} or the older [n, s]. None for one this module
/// does not compare by: a path, a descending order, an excluded null, or
/// options it does not know.
struct Part {
    field: u32,
    ty: Vec<u8>,
    nullable: bool,
    collation: Option<u64>,
}

fn read_part(r: &mut Rd) -> Option<Part> {
    match r.peek()? {
        0x90..=0x9f | 0xdc | 0xdd => {
            if r.array()? != 2 {
                return None;
            }
            let field = u32::try_from(r.uint()?).ok()?;
            let ty = r.str()?.to_vec();
            Some(Part {
                field,
                ty,
                nullable: false,
                collation: None,
            })
        }
        _ => {
            let (mut field, mut ty, mut nullable, mut collation) = (None, None, false, None);
            for _ in 0..r.map()? {
                match r.str()? {
                    b"field" => field = Some(u32::try_from(r.uint()?).ok()?),
                    b"type" => ty = Some(r.str()?.to_vec()),
                    b"is_nullable" => nullable = r.bool()?,
                    b"collation" => collation = Some(r.uint()?),
                    b"sort_order" => {
                        if r.str()? != b"asc" {
                            return None;
                        }
                    }
                    b"exclude_null" => {
                        if r.bool()? {
                            return None;
                        }
                    }
                    _ => return None,
                }
            }
            Some(Part {
                field: field?,
                ty: ty?,
                nullable,
                collation,
            })
        }
    }
}

/// The name of collation `id` (_collation: [id, name, ...]).
unsafe fn collation_name(id: u64) -> Option<Vec<u8>> {
    let mut key = Wr::with_capacity(16);
    key.array(1).uint(id);
    let mut t: *mut BoxTuple = std::ptr::null_mut();
    if box_index_get(
        SPACE_COLLATION,
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
    Some(field(t, 1)?.str()?.to_vec())
}

/// An index's parts (_index field 5), or None for parts of a kind this
/// module does not read.
fn read_parts(parts: &mut Rd) -> Option<Vec<Part>> {
    let n = parts.array()?;
    let mut read = Vec::with_capacity(n as usize);
    for _ in 0..n {
        read.push(read_part(parts)?);
    }
    Some(read)
}

/// The key definition of an index's parts, or null.
unsafe fn key_def_of(read: &[Part]) -> *mut BoxKeyDef {
    // (NUL-terminated names, alive until the key definition is built)
    let mut names: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::with_capacity(read.len());
    for p in read {
        let mut ty = p.ty.clone();
        ty.push(0);
        let coll = match p.collation {
            Some(id) => match collation_name(id) {
                Some(mut c) => {
                    c.push(0);
                    Some(c)
                }
                None => return std::ptr::null_mut(),
            },
            None => None,
        };
        names.push((ty, coll));
    }
    let mut defs: Vec<KeyPartDef> = Vec::with_capacity(read.len());
    for (p, (ty, coll)) in read.iter().zip(&names) {
        let mut d: KeyPartDef = std::mem::zeroed();
        box_key_part_def_create(&mut d);
        d.fieldno = p.field;
        d.field_type = ty.as_ptr().cast();
        if p.nullable {
            d.flags |= KEY_PART_IS_NULLABLE;
        }
        if let Some(c) = coll {
            d.collation = c.as_ptr().cast();
        }
        defs.push(d);
    }
    let kd = box_key_def_new_v2(defs.as_mut_ptr(), defs.len() as u32);
    if kd.is_null() {
        box_error_clear();
    }
    kd
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
    let mut functional = false;
    let mut opts = field(t, 4)?;
    for _ in 0..opts.map()? {
        match opts.str() {
            Some(b"unique") => unique = opts.bool()?,
            Some(b"func") => {
                // (its parts are of the function's keys, not the tuple's)
                functional = true;
                opts.skip()?
            }
            _ => opts.skip()?,
        }
    }
    let parts = field(t, 5)?.array()?;
    let read = if functional {
        None
    } else {
        read_parts(&mut field(t, 5)?)
    };
    let key_def = match &read {
        Some(r) => key_def_of(r),
        None => std::ptr::null_mut(),
    };
    let mut fields = [0u32; 8];
    let mut nfields = 0u8;
    if let Some(r) = read.as_ref().filter(|r| r.len() <= 8) {
        for (i, p) in r.iter().enumerate() {
            fields[i] = p.field;
        }
        nfields = r.len() as u8;
    }
    Some(IndexInfo {
        memtx,
        unique,
        parts,
        key_def,
        fields,
        nfields,
    })
}

/// SpaceInfo of space `space_id`, or None when it does not exist.
fn space_info(space_id: u32) -> Option<SpaceInfo> {
    // SAFETY: plain calls of the module API in the transaction thread.
    let ver = unsafe { box_schema_version() };
    if let Some(i) = INDEXES.with(|c| {
        let c = c.borrow();
        if c.ver == ver {
            c.spaces.get(&space_id).copied()
        } else {
            None
        }
    }) {
        return i;
    }
    // (an error reading the system spaces: not cached)
    let info = unsafe { read_space_info(space_id) }.ok()?;
    INDEXES.with(|c| {
        let mut c = c.borrow_mut();
        if c.ver != ver {
            c.reset(ver);
        }
        c.spaces.insert(space_id, info);
    });
    info
}

/// SpaceInfo of a space: Ok(None) when there is no such space, or one whose
/// definition this module does not read; Err on an error.
unsafe fn read_space_info(space_id: u32) -> Result<Option<SpaceInfo>, ()> {
    let mut key = Wr::with_capacity(16);
    key.array(1).uint(space_id as u64);
    let (k, k_end) = (
        key.0.as_ptr().cast::<c_char>(),
        key.0.as_ptr().add(key.0.len()).cast::<c_char>(),
    );
    let mut t: *mut BoxTuple = std::ptr::null_mut();
    if box_index_get(SPACE_SPACE, 0, k, k_end, &mut t) != 0 {
        box_error_clear();
        return Err(());
    }
    if t.is_null() {
        return Ok(None);
    }
    // _space: [id, owner, name, engine, field_count, flags, format]
    let Some(memtx) = field(t, 3).and_then(|mut f| f.str()).map(|e| e == b"memtx") else {
        return Ok(None);
    };
    // _index: [space_id, index_id, name, type, opts, parts]: the index
    // bucket_id, among the space's: the first part of it is the field of
    // the rows' bucket (BK.field; a space without it has none)
    let it = box_index_iterator(SPACE_INDEX, 0, ITER_EQ as c_int, k, k_end);
    if it.is_null() {
        box_error_clear();
        return Err(());
    }
    let mut bucket_field = Ok(None);
    loop {
        let mut x: *mut BoxTuple = std::ptr::null_mut();
        if box_iterator_next(it, &mut x) != 0 {
            box_error_clear();
            box_iterator_free(it);
            return Err(());
        }
        if x.is_null() {
            break;
        }
        if field(x, 2).and_then(|mut f| f.str().map(|n| n == b"bucket_id")) == Some(true) {
            // (parts it does not read: the space is left to Lua)
            bucket_field = field(x, 5)
                .and_then(|mut f| read_parts(&mut f))
                .and_then(|p| p.first().map(|p| Some(p.field)))
                .ok_or(());
            break;
        }
    }
    box_iterator_free(it);
    let Ok(bucket_field) = bucket_field else {
        return Ok(None);
    };
    Ok(Some(SpaceInfo { memtx, bucket_field }))
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
    if s.ready == 0 || s.hooks != 0 || s.page_rows == 0 || !select::route_ok(s, &a.route) {
        return None;
    }
    // SAFETY: plain calls of the module API in the transaction thread.
    let in_txn = unsafe {
        // (a read with lock_check on a replica, which tarn_select refuses:
        // ER_READONLY)
        if a.lock_check && (box_is_ro() || s.lk_stale != 0) {
            return None;
        }
        // (the window of the load counts ended: tarn_select starts the next
        // one, then counts the read)
        if select::counted(&a.route) && fiber_clock() >= s.load_until {
            return None;
        }
        box_txn()
    };
    let info = index_info(a.space_id, a.index_id)?;
    // (vinyl reads may yield; prepared transactions' locks, or a held
    // table: the lock check may have to wait)
    if !info.memtx || (a.lock_check && (s.ddl_n != 0 || s.fence_stale != 0 || s.space_locked(a.space_id))) {
        return None;
    }
    // The end key, compared as tarn_select's key_def of the index compares
    // it, once it is a valid key of the index (else tarn_select fails).
    let end = match a.end_key {
        Some(k) => {
            if info.key_def.is_null() {
                return None;
            }
            let mut size = 0u32;
            // SAFETY: a MessagePack array in `buf`; the key definition is
            // alive (the cache keeps it for this schema version).
            if unsafe { box_key_def_validate_key(info.key_def, k.as_ptr().cast(), &mut size) } != 0 {
                unsafe { box_error_clear() };
                return None;
            }
            if size as usize != k.len() {
                return None;
            }
            Some(k.as_ptr().cast::<c_char>())
        }
        None => None,
    };
    let reverse = select::reverse(a.iterator);
    // The first read of a stream begins its transaction, as tarn_select
    // does; a read handed back takes it back.
    let mut begun = match a.begin && !in_txn {
        // SAFETY: a plain call of the module API in the transaction thread.
        true => Some(unsafe { Txn::begin_stream()? }),
        false => None,
    };
    let in_txn = in_txn || begun.is_some();
    let mut page = Page::new(&a, in_txn, s.page_rows, s.page_bytes, s.scan_chunk);
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
        if a.iterator == ITER_EQ && end.is_none() && info.unique && key_parts == info.parts {
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
            while page.more() {
                let mut t: *mut BoxTuple = std::ptr::null_mut();
                if box_iterator_next(it, &mut t) != 0 {
                    box_iterator_free(it);
                    box_error_clear();
                    return None;
                }
                if t.is_null() {
                    break;
                }
                if let Some(e) = end {
                    if select::past_end(box_tuple_compare_with_key(t, e, info.key_def), reverse, a.end_incl) {
                        break;
                    }
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
    // (the stream's transaction from now on)
    if let Some(t) = begun.as_mut() {
        t.open = false;
    }
    select::count_reads(s, &a.route);
    Some(out.0)
}

/// A tuple's data, copied (the tuple may go once the transaction moves on).
unsafe fn data_of(t: *mut BoxTuple) -> Option<Vec<u8>> {
    if t.is_null() {
        return None;
    }
    let mut size = 0usize;
    let data = box_tuple_data(t, &mut size);
    Some(std::slice::from_raw_parts(data as *const u8, size).to_vec())
}

/// Field `i` (from 0) of a tuple's data, as MessagePack (None: missing).
fn field_of(data: &[u8], i: u32) -> Option<&[u8]> {
    let mut r = Rd::new(data);
    let n = r.array()?;
    if i >= n {
        return None;
    }
    for _ in 0..i {
        r.skip()?;
    }
    r.raw()
}

/// The primary key of a tuple (index 0's `fields`), as a MessagePack array.
fn key_of(data: &[u8], pk: &IndexInfo) -> Option<Vec<u8>> {
    if pk.nfields == 0 {
        return None;
    }
    let mut w = Wr::with_capacity(32);
    w.array(pk.nfields as u32);
    for &f in &pk.fields[..pk.nfields as usize] {
        w.raw(field_of(data, f)?);
    }
    Some(w.0)
}

/// The row of space `space_id` with primary key `key`, copied.
unsafe fn get_row(space_id: u32, key: &[u8]) -> Result<Option<Vec<u8>>, ()> {
    let k = key.as_ptr().cast::<c_char>();
    let mut t: *mut BoxTuple = std::ptr::null_mut();
    if box_index_get(space_id, 0, k, k.add(key.len()), &mut t) != 0 {
        box_error_clear();
        return Err(());
    }
    Ok(data_of(t))
}

/// The local transaction of an autocommit write: rolled back unless it was
/// committed, also when a panic unwinds through it.
struct Txn {
    open: bool,
}

impl Txn {
    unsafe fn begin() -> Option<Txn> {
        if box_txn_begin() != 0 {
            box_error_clear();
            return None;
        }
        let txn = Txn { open: true };
        // (RW in lua/tarn.lua: a write reads the latest committed rows)
        if box_txn_set_isolation(TXN_ISOLATION_READ_COMMITTED) != 0 {
            box_error_clear();
            return None;
        }
        Some(txn)
    }

    /// A stream's transaction, as tarn_select begins it (BEGIN_STREAM in
    /// lua/tarn.lua: read-confirmed, rolled back after STREAM_TIMEOUT).
    unsafe fn begin_stream() -> Option<Txn> {
        if box_txn_begin() != 0 {
            box_error_clear();
            return None;
        }
        let txn = Txn { open: true };
        if box_txn_set_isolation(TXN_ISOLATION_READ_CONFIRMED) != 0 || box_txn_set_timeout(STREAM_TIMEOUT) != 0 {
            box_error_clear();
            return None;
        }
        Some(txn)
    }
}

/// The number of the request's transaction (box_txn_id: Tarantool numbers
/// transactions as they begin), or 0 without one.
unsafe fn txn_number() -> u64 {
    u64::try_from(box_txn_id()).unwrap_or(0)
}

impl Drop for Txn {
    fn drop(&mut self) {
        if self.open {
            // SAFETY: the transaction this guard began, in this thread.
            unsafe {
                box_txn_rollback();
                box_error_clear();
            }
        }
    }
}

thread_local! {
    /// The native commits in flight, by number (State::commit_seq).
    static INFLIGHT: RefCell<Vec<u64>> = const { RefCell::new(Vec::new()) };
}

/// A native commit starts: its number.
fn commit_begins(s: &mut State) -> u64 {
    s.commit_seq += 1;
    let seq = s.commit_seq;
    INFLIGHT.with(|f| f.borrow_mut().push(seq));
    s.oldest_inflight = s.oldest_inflight.min(seq);
    seq
}

/// Commit `seq` ended, committed or not.
fn commit_ends(s: &mut State, seq: u64) {
    INFLIGHT.with(|f| {
        let mut f = f.borrow_mut();
        f.retain(|&x| x != seq);
        s.oldest_inflight = f.iter().copied().min().unwrap_or(u64::MAX);
    });
}

/// enum txn_isolation_level: TXN_ISOLATION_READ_COMMITTED,
/// TXN_ISOLATION_READ_CONFIRMED.
const TXN_ISOLATION_READ_COMMITTED: u32 = 1;
const TXN_ISOLATION_READ_CONFIRMED: u32 = 2;
/// BEGIN_STREAM's timeout (lua/tarn.lua), seconds.
const STREAM_TIMEOUT: f64 = 60.0;
const ER_READONLY: u32 = 7;
const ER_TRANSACTION_CONFLICT: u32 = 97;
const ER_SYNC_ROLLBACK: u32 = 217;

/// One write done: the row's versions before and after it, what it
/// returned, and the bucket of its row (its new version's, else its old
/// one's: BK.of_plan).
struct Done {
    res: Option<Vec<u8>>,
    bucket: Option<u64>,
    bytes: u64,
}

/// The writes of a commit step, made under a savepoint of the stream's
/// transaction: undone unless the step commits, also when a panic unwinds
/// through it, and the transaction (with its reads) stays.
struct Savepoint {
    sp: *mut c_void,
    open: bool,
}

thread_local! {
    /// The request's transaction could not be kept: a rollback to its
    /// savepoint failed, and the whole transaction was rolled back. Its
    /// writes must not go to the Lua procedure then, nor may the SQL server
    /// think its stream's reads are still there: the step replies 1213.
    static TXN_LOST: Cell<bool> = const { Cell::new(false) };
}

impl Savepoint {
    unsafe fn take() -> Option<Savepoint> {
        let sp = box_txn_savepoint();
        if sp.is_null() {
            box_error_clear();
            return None;
        }
        Some(Savepoint { sp, open: true })
    }
}

impl Drop for Savepoint {
    fn drop(&mut self) {
        if !self.open {
            return;
        }
        // SAFETY: a savepoint of the transaction of this request, in its
        // thread. (The rollback runs the transaction's on_rollback
        // triggers: the Lua module sets none on stream transactions.)
        unsafe {
            let rc = box_txn_rollback_to_savepoint(self.sp);
            box_error_clear();
            if rc != 0 {
                // (not known to happen) the writes must not stay in a
                // transaction handed back
                box_txn_rollback();
                box_error_clear();
                TXN_LOST.with(|l| l.set(true));
            }
        }
    }
}

/// What undoes a commit step's writes until it commits: its own
/// transaction, or a savepoint of the stream's.
enum Undo {
    Txn(Txn),
    Savepoint(Savepoint),
}

impl Undo {
    /// From now on the writes are the step's to report (it commits).
    fn keep(&mut self) {
        match self {
            Undo::Txn(t) => t.open = false,
            Undo::Savepoint(p) => p.open = false,
        }
    }
}

/// What became of a write step served here.
enum Served {
    /// The reply.
    Reply(Vec<u8>),
    /// Its commit failed with ER_READONLY, left as the error of the call:
    /// the SQL server follows the leader change (reraise_readonly).
    Raise,
}

/// What the writes need of the schema, read before their transaction, which
/// must not read the system spaces: each table memtx, without a lock of a
/// prepared transaction or a row lock (READ COMMITTED: the Lua procedures
/// wait for one on what they write), and the primary key of the tables a
/// replace writes. None: the writes are left to Lua. A synchronous table's
/// commit waits for its quorum, as the Lua procedures' do, and is in flight
/// until then.
fn write_infos(s: &State, writes: &[write::Write]) -> Option<Vec<(SpaceInfo, Option<IndexInfo>)>> {
    // Both autocommit and stream commits must defer capture to Lua. Check
    // every actual source before schema reads or a transaction/savepoint,
    // independently of the gateway's possibly stale definition fences.
    if writes.iter().any(|w| s.lazy_write_excluded(w.space_id)) {
        return None;
    }
    let mut infos = Vec::with_capacity(writes.len());
    for w in writes {
        let sp = space_info(w.space_id)?;
        if !sp.memtx || s.space_locked(w.space_id) || s.space_plocked(w.space_id) {
            return None;
        }
        let pk = match w.op {
            Op::Replace => Some(index_info(w.space_id, 0)?),
            _ => None,
        };
        infos.push((sp, pk));
    }
    Some(infos)
}

/// Make the writes (write_step), in the transaction of the request: what
/// each did, and their affected rows (`affected`). None, having stopped at
/// the first that failed or that the Lua procedures would make otherwise:
/// the caller's Undo takes back what was written. Then the rows' buckets
/// take writes here (BK.rows_refused), and the writes count for them
/// (BK.count_writes).
///
/// # Safety
/// In the transaction thread, with the request's transaction open.
unsafe fn apply_writes(
    s: &mut State,
    writes: &[write::Write],
    infos: &[(SpaceInfo, Option<IndexInfo>)],
    found_rows: bool,
) -> Option<(Vec<Done>, u64)> {
    let mut done: Vec<Done> = Vec::with_capacity(writes.len());
    let mut n = 0u64;
    for (w, (sp, pk)) in writes.iter().zip(infos) {
        let p = w.payload.as_ptr().cast::<c_char>();
        let p_end = p.add(w.payload.len());
        let mut t: *mut BoxTuple = std::ptr::null_mut();
        let (old, new, res) = match w.op {
            Op::Insert => {
                if box_insert(w.space_id, p, p_end, &mut t) != 0 {
                    return None;
                }
                let r = data_of(t);
                (None, r.clone(), r)
            }
            Op::Replace => {
                let old = get_row(w.space_id, &key_of(&w.payload, pk.as_ref()?)?).ok()?;
                if box_replace(w.space_id, p, p_end, &mut t) != 0 {
                    return None;
                }
                let r = data_of(t);
                (old, r.clone(), r)
            }
            Op::Delete => {
                let old = get_row(w.space_id, &w.payload).ok()?;
                if box_delete(w.space_id, 0, p, p_end, &mut t) != 0 {
                    return None;
                }
                (old, None, data_of(t))
            }
            Op::Update => match get_row(w.space_id, &w.payload).ok()? {
                // (a row that is not there: nothing written)
                None => (None, None, None),
                Some(old) => {
                    // (a row from before an ALTER TABLE that added fields:
                    // the Lua procedure fills them in)
                    if let Some(width) = w.width {
                        if (Rd::new(&old).array()? as u64) < width {
                            return None;
                        }
                    }
                    let v = |f: u32| write::val(f.checked_sub(1).and_then(|f| field_of(&old, f)));
                    if !write::arith_ok(&w.ops, &w.ranges, v) {
                        return None;
                    }
                    let ops = write::update_ops(&w.ops, v);
                    let o = ops.as_ptr().cast::<c_char>();
                    if box_update(w.space_id, 0, p, p_end, o, o.add(ops.len()), 1, &mut t) != 0 {
                        return None;
                    }
                    let r = data_of(t);
                    (Some(old), r.clone(), r)
                }
            },
        };
        n += write::affected(w.op, old.as_deref(), new.as_deref(), found_rows);
        let row = new.as_deref().or(old.as_deref());
        let bucket = match (sp.bucket_field, row) {
            (Some(f), Some(row)) => Some(field_of(row, f).and_then(|b| Rd::new(b).uint())?),
            _ => None,
        };
        let bytes = res.as_ref().or(new.as_ref()).map_or(0, |t| t.len() as u64);
        done.push(Done { res, bucket, bytes });
    }
    if s.count != 0 {
        if done.iter().any(|d| d.bucket.is_some_and(|b| !s.writable(b))) {
            return None;
        }
        for d in &done {
            if let Some(b) = d.bucket {
                s.count_write(b, d.bytes);
            }
        }
    }
    Some((done, n))
}

/// Commit the request's transaction, whose writes `undo` takes back until
/// then: a commit in flight until it ends. Err: the reply of a commit that
/// failed, as commit_failure has it.
///
/// Other requests run while it waits for the WAL, and change the state: it
/// takes the state before and after the commit, and holds no reference to
/// it across (nor may its caller). Tarantool frees the call's arguments
/// then too: nothing borrowed from them may be used after it.
///
/// # Safety
/// In the transaction thread, with the request's transaction open.
unsafe fn commit_txn(mut undo: Undo, committed: &Cell<bool>) -> Result<(), Served> {
    let seq = state().map(commit_begins);
    // From here on the writes are the step's to report.
    undo.keep();
    committed.set(true);
    let rc = box_txn_commit();
    if let (Some(seq), Some(s)) = (seq, state()) {
        commit_ends(s, seq);
    }
    if rc == 0 {
        return Ok(());
    }
    let e = box_error_last();
    let (code, msg) = if e.is_null() {
        (0, String::new())
    } else {
        let m = std::ffi::CStr::from_ptr(box_error_message(e))
            .to_string_lossy()
            .into_owned();
        (box_error_code(e), m)
    };
    if code == ER_READONLY {
        return Err(Served::Raise);
    }
    box_error_clear();
    let mut out = Wr::with_capacity(64);
    // (commit_failure)
    match code {
        ER_SYNC_ROLLBACK => out.array(3).str("conflict").uint(1213).str(&format!(
            "shard leader changed during the commit, retry the transaction ({})",
            msg
        )),
        ER_TRANSACTION_CONFLICT => out.array(3).str("conflict").uint(1213).str(&msg),
        _ => out.array(3).str("error").uint(code as u64).str(&msg),
    };
    Err(Served::Reply(out.0))
}

/// What became of an autocommit write served here, or None for one left
/// to tarn_autocommit, with nothing written. `committed`: set once the
/// commit begins, from when the writes may have been made.
fn autocommit(buf: &[u8], committed: &Cell<bool>) -> Option<Served> {
    let (writes, opts) = write::parse(buf)?;
    let s = state()?;
    if s.ready == 0 || s.hooks != 0 || s.bk_stale != 0 || s.lk_stale != 0 || s.fence_stale != 0 {
        return None;
    }
    // Gateway metadata can lag behind local DDL activation. Check every
    // actual source before schema reads or Txn::begin, not just opts.fence;
    // otherwise a stale/partial fence could bypass commit-atomic capture.
    if writes.iter().any(|w| s.lazy_write_excluded(w.space_id)) {
        return None;
    }
    // SAFETY: plain calls of the module API in the transaction thread.
    unsafe {
        // (a replica: tarn_autocommit raises ER_READONLY; a transaction in
        // progress: not an autocommit write)
        if box_is_ro() || box_txn() {
            return None;
        }
        // (the writes count for their buckets' load, in a window that has
        // not ended: else tarn_autocommit starts the next one)
        if s.count != 0 && fiber_clock() >= s.load_until {
            return None;
        }
    }
    // The definitions the writes were made with are current (else 1412),
    // and their buckets take writes here (else moved).
    if opts.fence.iter().any(|&(sid, ver)| s.fence(sid) > ver) {
        return None;
    }
    if s.count != 0 && opts.wb.iter().any(|&b| !s.writable(b)) {
        return None;
    }
    let infos = write_infos(s, &writes)?;
    // SAFETY: the transaction is this call's; tuples are copied before
    // anything else runs.
    unsafe {
        let undo = Undo::Txn(Txn::begin()?);
        let (done, n) = apply_writes(s, &writes, &infos, opts.found_rows)?;
        if let Err(failed) = commit_txn(undo, committed) {
            return Some(failed);
        }
        // {OK, n, last}: the last write's result, if any
        let mut out = Wr::with_capacity(64);
        match done.iter().rev().find_map(|d| d.res.as_ref()) {
            Some(last) => out.array(3).str("ok").uint(n).raw(last),
            None => out.array(2).str("ok").uint(n),
        };
        Some(Served::Reply(out.0))
    }
}

/// What became of a commit of one shard served here (TarnCommit!CommitLocal),
/// or None for one left to tarn_commit_local, with nothing written: in a
/// stream, its transaction and reads are as they were. `committed`: as for
/// `autocommit`.
fn commit_local(buf: &[u8], committed: &Cell<bool>) -> Option<Served> {
    let c = write::parse_commit(buf)?;
    // (row locks: the Lua procedure checks that they survived, and releases
    // them with the commit)
    if c.locks {
        return None;
    }
    let s = state()?;
    if s.ready == 0 || s.hooks != 0 || s.bk_stale != 0 || s.lk_stale != 0 || s.fence_stale != 0 {
        return None;
    }
    // (a TRUNCATE or DROP TABLE holds a table: reads_ddl_conflict)
    if s.ddl_n != 0 {
        return None;
    }
    // SAFETY: plain calls of the module API in the transaction thread.
    unsafe {
        if box_is_ro() {
            return None;
        }
        if s.count != 0 && fiber_clock() >= s.load_until {
            return None;
        }
    }
    // BK.commit_check: the transaction read every shard with the current
    // membership (BK.member_refused); its writes' definitions are current;
    // the buckets it writes take writes here, and those it read are served
    // here, none of them held (BK.held_conflict)
    if c.mver.is_some_and(|v| v < s.mver) {
        return None;
    }
    if c.fence.iter().any(|&(sid, ver)| s.fence(sid) > ver) {
        return None;
    }
    if s.count != 0
        && (s.held != 0
            || c.wb.iter().any(|&b| !s.writable(b))
            || c.reads.iter().any(|r| r.bucket.is_some_and(|b| !s.serves(b))))
    {
        return None;
    }
    // reads_w_locked: no lock of a prepared transaction on a table it read
    if c.reads.iter().any(|r| s.space_locked(r.space_id)) {
        return None;
    }
    let infos = write_infos(s, &c.writes)?;
    // SAFETY: the transaction is the request's: the stream's, or this call's
    // without one; tuples are copied before anything else runs.
    unsafe {
        let undo = if box_txn() {
            // (a TRUNCATE or DROP TABLE applied since the stream began:
            // reads_ddl_conflict, with the tables read)
            if txn_number() < s.ddl_txn {
                return None;
            }
            Undo::Savepoint(Savepoint::take()?)
        } else if c.reads.is_empty() {
            // (a transaction that read nothing here: the coordinator sends
            // no stream)
            Undo::Txn(Txn::begin()?)
        } else {
            // (reads, and no transaction of their stream to commit them in)
            return None;
        };
        let (done, _) = apply_writes(s, &c.writes, &infos, false)?;
        if let Err(failed) = commit_txn(undo, committed) {
            return Some(failed);
        }
        // {OK, n}: the writes that wrote a row
        let n = done.iter().filter(|d| d.res.is_some()).count();
        let mut out = Wr::with_capacity(16);
        out.array(2).str("ok").uint(n as u64);
        Some(Served::Reply(out.0))
    }
}

/// What became of a validation served here (TarnCommit!ValidateRO, or the
/// commit of a read-only stream of one shard), or None for one left to
/// tarn_validate, with nothing changed. `committed`: as for `autocommit`.
fn validate(buf: &[u8], committed: &Cell<bool>) -> Option<Served> {
    let v = write::parse_validate(buf)?;
    // (row locks: the Lua procedure checks that they survived, and releases
    // them)
    if v.locks {
        return None;
    }
    let s = state()?;
    if s.ready == 0 || s.hooks != 0 || s.bk_stale != 0 || s.lk_stale != 0 || s.fence_stale != 0 {
        return None;
    }
    // (a TRUNCATE or DROP TABLE holds a table: reads_ddl_conflict)
    if s.ddl_n != 0 || s.scratch == 0 {
        return None;
    }
    // SAFETY: plain calls of the module API in the transaction thread.
    unsafe {
        // (a demoted node: tarn_validate raises ER_READONLY; no stream
        // transaction: it fails the reads, or only ends row locks)
        if box_is_ro() || !box_txn() {
            return None;
        }
        // The node has been the leader since the stream began (TERM_TXN),
        // and no TRUNCATE or DROP TABLE was applied since (DDL_TXN: the Lua
        // procedure checks the tables read). (A change of the election term
        // makes the node read-only at once, and stale before it is writable
        // again: term_txn is the term's here.)
        let id = txn_number();
        if id < s.term_txn || id < s.ddl_txn {
            return None;
        }
    }
    // BK.commit_check: the membership a read of every shard was routed
    // with; the buckets read are served here, none of them held
    if v.mver.is_some_and(|m| m < s.mver) {
        return None;
    }
    if s.count != 0 && (s.held != 0 || v.reads.iter().any(|r| r.bucket.is_some_and(|b| !s.serves(b)))) {
        return None;
    }
    // reads_w_locked: no lock of a prepared transaction on a table it read
    if v.reads.iter().any(|r| s.space_locked(r.space_id)) {
        return None;
    }
    // SAFETY: the stream's transaction; nothing yields until it commits
    // (the probe's space is data-temporary: no WAL write).
    unsafe {
        // The reads are unchanged: a transaction whose reads were
        // overwritten is in a read view, which refuses the probe write
        // (tarn_validate).
        let probe = [0x91u8, 0x01];
        let p = probe.as_ptr().cast::<c_char>();
        if box_replace(s.scratch, p, p.add(probe.len()), std::ptr::null_mut()) != 0 {
            let e = box_error_last();
            let msg = if e.is_null() {
                String::new()
            } else {
                std::ffi::CStr::from_ptr(box_error_message(e))
                    .to_string_lossy()
                    .into_owned()
            };
            box_txn_rollback();
            box_error_clear();
            let mut out = Wr::with_capacity(96);
            out.array(3)
                .str("conflict")
                .uint(1213)
                .str(&format!("read set changed before commit: {}", msg));
            return Some(Served::Reply(out.0));
        }
        committed.set(true);
        // (tarn_validate's box.commit() raises its error)
        if box_txn_commit() != 0 {
            return Some(Served::Raise);
        }
    }
    let mut out = Wr::with_capacity(8);
    out.array(1).str("ok");
    Some(Served::Reply(out.0))
}

/// The reply when a commit step could not take back its writes in a stream
/// (TXN_LOST): it rolled the stream's transaction back, nothing committed.
fn txn_lost() -> Vec<u8> {
    let mut out = Wr::with_capacity(96);
    out.array(3)
        .str("conflict")
        .uint(1213)
        .str("the shard could not take back the writes of a commit it left to Lua: retry the transaction");
    out.0
}

/// The reply when the procedure failed after its commit began: whether
/// the writes were made is not known, and they must not be made again.
fn outcome_unknown() -> Vec<u8> {
    let mut out = Wr::with_capacity(96);
    out.array(3)
        .str("error")
        .uint(1180)
        .str("the native procedure failed while committing: the writes may or may not have been made");
    out.0
}

/// Run a write step (`step`) for a request and reply: its reply, its
/// error (ER_READONLY), or ["lua"], counted in the state's counters
/// (`count`: served or not).
///
/// # Safety
/// Called by Tarantool with a valid context and argument buffer.
unsafe fn write_step(
    ctx: *mut BoxFunctionCtx,
    args: *const c_char,
    args_end: *const c_char,
    step: fn(&[u8], &Cell<bool>) -> Option<Served>,
    count: fn(&mut State, bool),
) -> c_int {
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
    let committed = Cell::new(false);
    TXN_LOST.with(|l| l.set(false));
    let out = match std::panic::catch_unwind(AssertUnwindSafe(|| step(buf, &committed))) {
        Ok(out) => out,
        // (once the commit began, the writes must not go to Lua as well)
        Err(_) if committed.get() => Some(Served::Reply(outcome_unknown())),
        Err(_) => None,
    };
    let out = match out {
        None if TXN_LOST.with(|l| l.get()) => Some(Served::Reply(txn_lost())),
        out => out,
    };
    if let Some(s) = state() {
        count(s, out.is_some());
    }
    match out {
        Some(Served::Reply(mp)) => reply(ctx, &mp),
        // (the diagnostics area holds the error)
        Some(Served::Raise) => -1,
        None => reply(ctx, TO_LUA),
    }
}

/// `ts_autocommit(writes, opts)`: tarn_autocommit's arguments; its reply,
/// or ["lua"], having written nothing (see write.rs).
///
/// # Safety
/// Called by Tarantool with a valid context and argument buffer.
#[no_mangle]
pub unsafe extern "C" fn ts_autocommit(
    ctx: *mut BoxFunctionCtx,
    args: *const c_char,
    args_end: *const c_char,
) -> c_int {
    write_step(ctx, args, args_end, autocommit, |s, served| {
        if served {
            s.w_served += 1;
        } else {
            s.w_to_lua += 1;
        }
    })
}

/// `ts_commit_local(txid, writes, rpoints, rranges, lock_gen, fence, wb,
/// mver)`: tarn_commit_local's arguments; its reply, or ["lua"], having
/// written nothing (see `commit_local`).
///
/// # Safety
/// Called by Tarantool with a valid context and argument buffer.
#[no_mangle]
pub unsafe extern "C" fn ts_commit_local(
    ctx: *mut BoxFunctionCtx,
    args: *const c_char,
    args_end: *const c_char,
) -> c_int {
    write_step(ctx, args, args_end, commit_local, |s, served| {
        if served {
            s.c_served += 1;
        } else {
            s.c_to_lua += 1;
        }
    })
}

/// `ts_validate(txid, rpoints, rranges, cross, lock_gen, mver)`:
/// tarn_validate's arguments; its reply, or ["lua"], having changed nothing
/// (see `validate`).
///
/// # Safety
/// Called by Tarantool with a valid context and argument buffer.
#[no_mangle]
pub unsafe extern "C" fn ts_validate(ctx: *mut BoxFunctionCtx, args: *const c_char, args_end: *const c_char) -> c_int {
    write_step(ctx, args, args_end, validate, |s, served| {
        if served {
            s.v_served += 1;
        } else {
            s.v_to_lua += 1;
        }
    })
}
