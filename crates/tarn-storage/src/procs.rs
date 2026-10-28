//! The procedures Tarantool calls (`box.schema.func.create('tarn_storage.<name>',
//! {language = 'C'})`), and the part of Tarantool's module API they use
//! (module.h).

use crate::batch;
use crate::fallback::{self, back, note, Proc, Why};
use crate::locks::{self, KeyIndex};
use crate::mp::{Rd, Wr};
use crate::select::{self, Page, ITER_EQ, ITER_REQ};
use crate::state::{State, LAYOUT};
use crate::write::{self, Op};
use std::borrow::Cow;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::os::raw::{c_char, c_int, c_uint, c_void};
use std::panic::AssertUnwindSafe;
use std::rc::Rc;
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
    fn box_key_def_validate_full_key(key_def: *const BoxKeyDef, key: *const c_char, key_size: *mut u32) -> c_int;
    fn box_tuple_compare_with_key(tuple: *mut BoxTuple, key: *const c_char, key_def: *mut BoxKeyDef) -> c_int;
    fn box_tuple_format_default() -> *mut c_void;
    fn box_tuple_new(format: *mut c_void, data: *const c_char, end: *const c_char) -> *mut BoxTuple;
    fn box_tuple_ref(tuple: *mut BoxTuple) -> c_int;
    fn box_tuple_unref(tuple: *mut BoxTuple);
    fn box_error_set(file: *const c_char, line: c_uint, code: u32, format: *const c_char, ...) -> c_int;
    fn box_error_last() -> *mut BoxError;
    fn box_error_code(error: *const BoxError) -> u32;
    fn box_error_message(error: *const BoxError) -> *const c_char;
    fn fiber_clock() -> f64;
    fn clock_monotonic64() -> i64;
    fn clock_realtime64() -> i64;
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
    /// The same parts at fields 0, 1, ...: a key of the index as a tuple,
    /// to compare keys with keys (`KeyTuple`; null as `key_def` is).
    key_def_pos: *mut BoxKeyDef,
    /// The fields of its parts (from 0), when there are at most 8 and none
    /// has a path: what a key of the index is made of (else nfields = 0).
    fields: [u32; 8],
    nfields: u8,
}

/// What a space is, as far as writes need: memtx, the field (from 0) of its
/// rows' bucket (BK.field: the first part of its index bucket_id), and its
/// indexes, for the lock checks (None: one of a kind they do not read, a
/// path, a function's).
#[derive(Clone)]
struct SpaceInfo {
    memtx: bool,
    bucket_field: Option<u32>,
    indexes: Option<Rc<[KeyIndex]>>,
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
            for kd in [i.key_def, i.key_def_pos] {
                if !kd.is_null() {
                    // SAFETY: built by box_key_def_new_v2, held only here.
                    unsafe { box_key_def_delete(kd) };
                }
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

/// The key definition of an index's parts (`positional`: the same parts at
/// fields 0, 1, ...), or null.
unsafe fn key_def_of(read: &[Part], positional: bool) -> *mut BoxKeyDef {
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
    for (i, (p, (ty, coll))) in read.iter().zip(&names).enumerate() {
        let mut d: KeyPartDef = std::mem::zeroed();
        box_key_part_def_create(&mut d);
        d.fieldno = if positional { i as u32 } else { p.field };
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
    let (key_def, key_def_pos) = match &read {
        Some(r) => (key_def_of(r, false), key_def_of(r, true)),
        None => (std::ptr::null_mut(), std::ptr::null_mut()),
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
        key_def_pos,
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
            c.spaces.get(&space_id).cloned()
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
        c.spaces.insert(space_id, info.clone());
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
    // _index: [space_id, index_id, name, type, opts, parts]: the space's
    // indexes; the first part of the index bucket_id is the field of the
    // rows' bucket (BK.field; a space without it has none)
    let it = box_index_iterator(SPACE_INDEX, 0, ITER_EQ as c_int, k, k_end);
    if it.is_null() {
        box_error_clear();
        return Err(());
    }
    let mut bucket_field = Ok(None);
    let mut indexes = Some(Vec::new());
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
        let parts = field(x, 5).and_then(|mut f| read_parts(&mut f));
        if field(x, 2).and_then(|mut f| f.str().map(|n| n == b"bucket_id")) == Some(true) {
            // (parts it does not read: the space is left to Lua)
            bucket_field = parts.as_ref().and_then(|p| p.first().map(|p| Some(p.field))).ok_or(());
        }
        indexes = indexes.and_then(|mut v| {
            v.push(key_index(x, parts?)?);
            Some(v)
        });
    }
    box_iterator_free(it);
    let Ok(bucket_field) = bucket_field else {
        return Ok(None);
    };
    Ok(Some(SpaceInfo {
        memtx,
        bucket_field,
        indexes: indexes.map(Rc::from),
    }))
}

/// An index (its _index tuple, and its parts) as the lock checks of a
/// write read it: None for one whose keys are not its parts' fields (a
/// function's).
unsafe fn key_index(x: *mut BoxTuple, parts: Vec<Part>) -> Option<KeyIndex> {
    let iid = u32::try_from(field(x, 1)?.uint()?).ok()?;
    let mut unique = true;
    let mut opts = field(x, 4)?;
    for _ in 0..opts.map()? {
        match opts.str() {
            Some(b"unique") => unique = opts.bool()?,
            Some(b"func") => return None,
            _ => opts.skip()?,
        }
    }
    Some(KeyIndex {
        iid,
        unique,
        parts: parts.iter().map(|p| (p.field, p.nullable)).collect(),
    })
}

/// Reply with one MessagePack value.
unsafe fn reply(ctx: *mut BoxFunctionCtx, mp: &[u8]) -> c_int {
    box_return_mp(ctx, mp.as_ptr().cast(), mp.as_ptr().add(mp.len()).cast())
}

const TO_LUA: &[u8] = &[0x91, 0xa3, b'l', b'u', b'a'];
const BATCH_CAPABILITY: &[u8] = b"\x92\xb1native_batch_keys\x01";

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
    // Pure discovery, including on an unready node: no state or read
    // counters are touched. Old modules leave this arity to Lua instead.
    if batch::capability(buf) {
        return reply(ctx, BATCH_CAPABILITY);
    }
    #[cfg(feature = "alloc-metrics")]
    if batch::allocation_query(buf) {
        let (data, len) = crate::alloc_metrics::reply();
        return reply(ctx, &data[..len]);
    }
    // Tarantool may release its CALL argument port when this fiber yields.
    // The quorum barrier now precedes use of parsed key/route slices, so
    // those slices must borrow our buffer rather than the argument port.
    let owned = buf.to_vec();
    let buf = owned.as_slice();
    fallback::begin();
    let raise = Cell::new(false);
    let out = std::panic::catch_unwind(AssertUnwindSafe(|| {
        if Rd::new(buf).array() == Some(8) {
            select_batch(buf, &raise)
        } else {
            select_page(buf, &raise)
        }
    }))
    .unwrap_or_else(|_| back(Why::Panic));
    if raise.get() {
        // A failed quorum barrier is an actual native error, not an
        // unsupported shape. Do not silently run it a second time in Lua.
        return -1;
    }
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
            fallback::counted(Proc::Read, None);
            reply(ctx, TO_LUA)
        }
    }
}

/// A clock check using actual clock calls, not the cached event-loop time.
/// In particular a stopped process must not serve before its watcher runs.
fn read_clock_ready() -> bool {
    // SAFETY: public clock API calls do not yield or inspect transactions.
    let (mono, wall) = unsafe { (clock_monotonic64(), clock_realtime64()) };
    let (Ok(mono), Ok(wall)) = (u64::try_from(mono), u64::try_from(wall)) else {
        return false;
    };
    state().is_some_and(|s| crate::freshness::clock_ready(s, mono, wall))
}

/// Copy callbacks out of State before invoking them. They are protected,
/// nonyielding Lua callbacks, but no Rust State reference may span them.
fn fresh_controls_ok() -> bool {
    let callback = state().and_then(|s| s.fresh_controls);
    callback.is_some_and(|f| matches!(unsafe { f() }, 1 | 2))
}

fn fresh_space_ok(space: u32) -> bool {
    let callback = state().and_then(|s| s.fresh_space);
    callback.is_some_and(|f| unsafe { f(space) } == 1)
}

fn fresh_stream_ok() -> bool {
    let callback = state().and_then(|s| s.fresh_ok);
    // SAFETY: a public transaction identity query, followed by the exact
    // current-ID certificate lookup. Neither step yields.
    let id = unsafe { txn_number() };
    id != 0 && callback.is_some_and(|f| unsafe { f(id) } == 1)
}

/// Prepare a supported read before acquiring schema/lock/native State.
/// None hands back; Some(None) owns no stream; Some(Some(txn)) owns a new
/// certified stream which its caller retains ONLY on complete success.
/// Barrier errors set `raise` and preserve the native diagnostic.
fn fresh_read(first: bool, checked: bool, space: u32, raise: &Cell<bool>) -> Option<Option<Txn>> {
    // A known demotion must reach Lua's ER_READONLY path even if clock
    // health is unknown, so the gateway can follow a replacement leader.
    if unsafe { box_is_ro() } {
        return back(Why::ReadOnly);
    }
    let timeout = {
        let s = ready_state()?;
        s.read_timeout
    };
    if !timeout.is_finite() || timeout <= 0.0 || !fresh_controls_ok() || !read_clock_ready() {
        return back(Why::Freshness);
    }
    let in_txn = unsafe { box_txn() };
    let owned = if in_txn {
        if !fresh_stream_ok() {
            return back(Why::Freshness);
        }
        None
    } else if first {
        // A new, empty transaction: no State reference crosses this wait.
        let t = unsafe { Txn::begin_stream(raise)? };
        let mark = state().and_then(|s| s.fresh_mark);
        let id = unsafe { txn_number() };
        if id == 0 || !mark.is_some_and(|f| unsafe { f(id) } == 1) {
            drop(t);
            return back(Why::Freshness);
        }
        Some(t)
    } else if checked {
        // Preserve the original no-TX memtx scan behavior. This rollback
        // closes only the empty barrier, never a caller's stream/read set.
        let t = unsafe { Txn::begin_fresh(timeout, raise)? };
        drop(t);
        None
    } else {
        return back(Why::Stream);
    };
    // The quorum wait may have applied a new term, schema or configuration.
    // Recheck actual synchronous flags; a cached fresh_mode is insufficient.
    if unsafe { box_is_ro() } {
        return back(Why::ReadOnly);
    }
    if !fresh_controls_ok() || !fresh_space_ok(space) || !read_clock_ready() {
        return back(Why::Freshness);
    }
    // Other native requests can run during the barrier; their diagnostic
    // counters must not determine this request's handback reason.
    fallback::begin();
    Some(owned)
}

/// Full primary-key autocommit batches (the eight-argument form of
/// tarn_select_keys). Every read is memtx and none yields: the state and
/// all requested keys' W claims describe the same snapshot, including
/// missing rows. Anything needing a continuation is handed back whole.
fn select_batch(buf: &[u8], raise: &Cell<bool>) -> Option<Vec<u8>> {
    note(Why::Shape);
    let a = batch::parse(buf)?;
    note(Why::Other);
    // Only an autocommit request is supported by this bounded kernel.
    // The empty freshness transaction is closed before its no-yield scan.
    if unsafe { box_txn() } {
        return back(Why::Stream);
    }
    let _owned = fresh_read(false, true, a.space_id, raise)?;
    let s = ready_state()?;
    if s.page_rows == 0 || s.page_bytes == 0 {
        return back(Why::NotReady);
    }
    // SAFETY: plain module API calls in the transaction thread. A batch
    // has closed its owned empty barrier and never borrows a stream, so
    // handback leaves no caller reads or callbacks to undo.
    unsafe {
        if box_is_ro() {
            return back(Why::ReadOnly);
        }
        if box_txn() {
            return back(Why::Stream);
        }
        if select::counted(&a.route) && fiber_clock() >= s.load_until {
            return back(Why::LoadWindow);
        }
    }
    if !select::route_ok(s, &a.route) {
        return back(Why::Bucket);
    }
    if s.ddl_n != 0 {
        return back(Why::HeldTable);
    }
    let Some(info) = index_info(a.space_id, 0) else {
        return back(Why::Schema);
    };
    if !info.memtx {
        return back(Why::Vinyl);
    }
    if !info.unique || info.parts == 0 || a.keys.iter().any(|k| k.parts != info.parts) {
        return back(Why::Shape);
    }
    if s.space_locked(a.space_id) {
        // Check the whole input, never just keys which return tuples: a
        // prepared insert can own a currently missing primary key.
        for key in &a.keys {
            if let Some(why) = locks::point_read_met(a.space_id, 0, key.raw) {
                return back(why);
            }
        }
    }
    let mut budget = batch::Budget {
        work: 0,
        rows: 0,
        bytes: 0,
    };
    let max_rows = u64::from(s.page_rows);
    let max_bytes = s.page_bytes;
    let mut out = Wr::with_capacity(256);
    out.array(2).str("ok");
    let at = out.array_slot();
    for key in &a.keys {
        if !batch::can_step(budget, batch::MAX_KEYS, max_rows, max_bytes) {
            return back(Why::Page);
        }
        // SAFETY: key.raw is a complete MessagePack key in this call's
        // buffer; the schema is stable and memtx lookups do not yield.
        unsafe {
            let p = key.raw.as_ptr().cast::<c_char>();
            let mut t = std::ptr::null_mut();
            if box_index_get(a.space_id, 0, p, p.add(key.raw.len()), &mut t) != 0 {
                box_error_clear();
                return None;
            }
            let (data, size) = if t.is_null() {
                (std::ptr::null(), 0)
            } else {
                let mut size = 0;
                let data = box_tuple_data(t, &mut size);
                if data.is_null() {
                    return None;
                }
                (data, size)
            };
            let Some(next) = batch::step(budget, !t.is_null(), size as u64, batch::MAX_KEYS, max_rows, max_bytes)
            else {
                return back(Why::Page);
            };
            budget = next;
            if !t.is_null() {
                // Copy the native tuple directly, in request order. There
                // is no deduplication; misses append no row.
                out.raw(std::slice::from_raw_parts(data.cast::<u8>(), size));
            }
        }
    }
    out.patch_array(at, budget.rows as u32); // no more than MAX_KEYS
    select::count_reads(s, &a.route);
    Some(out.0)
}

/// The reply to a read ts_select serves (["ok", rows]), or None for one it
/// leaves to tarn_select.
fn select_page(buf: &[u8], raise: &Cell<bool>) -> Option<Vec<u8>> {
    note(Why::Shape);
    let a = select::parse(buf)?;
    note(Why::Other);
    // The only yielding step precedes every State/schema/claim borrow.
    // If this first stream read hands back, dropping `begun` rolls back
    // only its newly owned transaction and its certificate.
    let mut begun = fresh_read(a.begin, a.lock_check, a.space_id, raise)?;
    let Some(s) = state() else {
        return back(Why::NotReady);
    };
    if s.ready == 0 || s.page_rows == 0 {
        return back(Why::NotReady);
    }
    if s.hooks != 0 {
        return back(Why::TestHooks);
    }
    if !select::route_ok(s, &a.route) {
        return back(if s.bk_stale != 0 {
            Why::StaleBuckets
        } else {
            Why::Bucket
        });
    }
    // SAFETY: plain calls of the module API in the transaction thread.
    let in_txn = unsafe {
        // (a read with lock_check on a replica, which tarn_select refuses:
        // ER_READONLY)
        if a.lock_check && box_is_ro() {
            return back(Why::ReadOnly);
        }
        if a.lock_check && s.lk_stale != 0 {
            return back(Why::StaleLocks);
        }
        // (the window of the load counts ended: tarn_select starts the next
        // one, then counts the read)
        if select::counted(&a.route) && fiber_clock() >= s.load_until {
            return back(Why::LoadWindow);
        }
        box_txn()
    };
    let Some(info) = index_info(a.space_id, a.index_id) else {
        return back(Why::Schema);
    };
    let Some(key_parts) = Rd::new(a.key).array() else {
        return back(Why::Shape);
    };
    // (vinyl reads may yield; a held table, or a prepared transaction's W
    // lock on what the read covers: the lock check may have to wait)
    if !info.memtx {
        return back(Why::Vinyl);
    }
    if a.lock_check && s.ddl_n != 0 {
        return back(Why::HeldTable);
    }
    if a.lock_check && s.fence_stale != 0 {
        return back(Why::StaleFences);
    }
    let range_check = match a.lock_check {
        true => lock_check(s, &a, &info, key_parts)?,
        false => false,
    };
    // The end key, compared as tarn_select's key_def of the index compares
    // it, once it is a valid key of the index (else tarn_select fails).
    let end = match a.end_key {
        Some(k) => {
            if info.key_def.is_null() {
                return back(Why::Schema);
            }
            let mut size = 0u32;
            // SAFETY: a MessagePack array in `buf`; the key definition is
            // alive (the cache keeps it for this schema version).
            if unsafe { box_key_def_validate_key(info.key_def, k.as_ptr().cast(), &mut size) } != 0 {
                unsafe { box_error_clear() };
                return back(Why::Shape);
            }
            if size as usize != k.len() {
                return back(Why::Shape);
            }
            Some(k.as_ptr().cast::<c_char>())
        }
        None => None,
    };
    let reverse = select::reverse(a.iterator);
    // The first read of a stream begins its transaction, as tarn_select
    // does; a read handed back takes it back.
    let in_txn = in_txn || begun.is_some();
    let mut page = Page::new(&a, in_txn, s.page_rows, s.page_bytes, s.scan_chunk);
    let mut out = Wr::with_capacity(256);
    out.array(2).str("ok");
    let at = out.array_slot();
    let mut n = 0u32;
    // (where the last row is in `out`)
    let mut last = 0..0;
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
                last = out.0.len()..out.0.len() + size;
                out.raw(std::slice::from_raw_parts(data as *const u8, size));
                n += 1;
            }
            box_iterator_free(it);
        }
    }
    if !page.whole() {
        return back(Why::Page);
    }
    // The range the read covers, from its key to its end key, or to its last
    // row when its limit cut the scan short (select_locked), against the
    // W-locked keys of its index.
    if range_check {
        let first = (key_parts > 0).then_some(a.key);
        let cut = n > 0 && a.limit.is_some_and(|l| n as u64 >= l);
        let last = match cut {
            true => {
                let parts: Vec<(u32, bool)> = info.fields[..info.nfields as usize]
                    .iter()
                    .map(|&f| (f, true))
                    .collect();
                let key = locks::fields_of(&out.0[last]).and_then(|row| locks::key_of(&row, &parts));
                match key.filter(|_| !parts.is_empty()) {
                    Some(k) => Some(Cow::Owned(k)),
                    None => return back(Why::LockKey),
                }
            }
            false => a.end_key.map(Cow::Borrowed),
        };
        let (lo, hi) = if matches!(a.iterator, ITER_EQ | ITER_REQ) {
            (first, first)
        } else if reverse {
            (last.as_deref(), first)
        } else {
            (first, last.as_deref())
        };
        if let Some(w) = range_read_met(a.space_id, a.index_id, lo, hi) {
            return back(w);
        }
    }
    out.patch_array(at, n);
    // (the stream's transaction from now on)
    if let Some(t) = begun.as_mut() {
        t.open = false;
    }
    select::count_reads(s, &a.route);
    Some(out.0)
}

/// The lock check of a read (tarn_select's select_locked; the Lua procedure
/// waits for a W lock of a prepared transaction on what the read covers):
/// Some(false) when it is done -- no lock on the table, or none on the key
/// of a point read -- Some(true) when the range the read covers is to be
/// compared with the W-locked keys of its index once its rows are read, and
/// None for the Lua procedure. (A nil key fails that check in the Lua
/// procedure, on a table with locks.)
fn lock_check(s: &State, a: &select::Args, info: &IndexInfo, key_parts: u32) -> Option<bool> {
    if !s.space_locked(a.space_id) {
        return Some(false);
    }
    if a.key_nil {
        return back(Why::Shape);
    }
    if matches!(a.iterator, ITER_EQ | ITER_REQ) && info.unique && key_parts == info.parts {
        return match locks::point_read_met(a.space_id, a.index_id, a.key) {
            Some(w) => back(w),
            None => Some(false),
        };
    }
    Some(locks::w_on_index(a.space_id, a.index_id))
}

/// A key of an index as a tuple of its parts, to compare it with keys (the
/// module API compares tuples with keys, not keys with keys).
struct KeyTuple(*mut BoxTuple);

impl KeyTuple {
    /// `key`, when it is a whole key of `kd` (an index's positional key
    /// definition: IndexInfo::key_def_pos).
    ///
    /// # Safety
    /// In the transaction thread; `kd` is alive.
    unsafe fn new(key: &[u8], kd: *mut BoxKeyDef) -> Option<KeyTuple> {
        if kd.is_null() || !valid_key(kd, key, true) {
            return None;
        }
        let k = key.as_ptr().cast::<c_char>();
        let t = box_tuple_new(box_tuple_format_default(), k, k.add(key.len()));
        if t.is_null() {
            box_error_clear();
            return None;
        }
        box_tuple_ref(t);
        Some(KeyTuple(t))
    }

    /// Whether the key lies between `lo` and `hi` (None: no bound), both
    /// included, as in_range compares them (compare_keys: on the parts the
    /// bound has); None when a bound is not a key of `kd`, or a prefix of
    /// one.
    ///
    /// # Safety
    /// As for `new`.
    unsafe fn within(&self, lo: Option<&[u8]>, hi: Option<&[u8]>, kd: *mut BoxKeyDef) -> Option<bool> {
        let mut c = [None, None];
        for (i, b) in [lo, hi].into_iter().enumerate() {
            if let Some(b) = b {
                if !valid_key(kd, b, false) {
                    return None;
                }
                c[i] = Some(box_tuple_compare_with_key(self.0, b.as_ptr().cast(), kd));
            }
        }
        Some(tarn_verified::locks::within(c[0], c[1]))
    }
}

impl Drop for KeyTuple {
    fn drop(&mut self) {
        // SAFETY: the reference `new` took.
        unsafe { box_tuple_unref(self.0) };
    }
}

/// Whether `key` is a key of `kd` (`whole`: of all its parts; else of the
/// first ones), every byte of it.
///
/// # Safety
/// In the transaction thread; `kd` is alive.
unsafe fn valid_key(kd: *mut BoxKeyDef, key: &[u8], whole: bool) -> bool {
    // (the module API reads the array's header unchecked)
    if Rd::new(key).array().is_none() {
        return false;
    }
    let mut size = 0u32;
    let k = key.as_ptr().cast();
    let rc = match whole {
        true => box_key_def_validate_full_key(kd, k, &mut size),
        false => box_key_def_validate_key(kd, k, &mut size),
    };
    if rc != 0 {
        box_error_clear();
        return false;
    }
    size as usize == key.len()
}

/// What a read of index `iid` of space `sid` between `lo` and `hi` meets
/// (range_w_locked; None: no bound): a prepared transaction's W lock on a
/// key between them, a key or a bound it cannot compare (which may lie
/// there), an index definition it does not read; None when it is free.
fn range_read_met(sid: u32, iid: u32, lo: Option<&[u8]>, hi: Option<&[u8]>) -> Option<Why> {
    if !locks::w_on_index(sid, iid) {
        return None;
    }
    let Some(info) = index_info(sid, iid) else {
        return Some(Why::Schema);
    };
    let kd = info.key_def_pos;
    if kd.is_null() {
        return Some(Why::Schema);
    }
    // SAFETY: in the transaction thread; the cache keeps `kd` for this
    // schema version, and nothing here changes the schema.
    unsafe {
        if [lo, hi].into_iter().flatten().any(|b| !valid_key(kd, b, false)) {
            return Some(Why::LockKey);
        }
        // (a W-locked key that is not compared may lie in the range)
        let mut uncompared = false;
        let free = locks::range_free(
            sid,
            iid,
            &mut |k| match KeyTuple::new(k, kd).and_then(|t| t.within(lo, hi, kd)) {
                Some(within) => within,
                None => {
                    uncompared = true;
                    true
                }
            },
        );
        match (free, uncompared) {
            (true, _) => None,
            (false, true) => Some(Why::LockKey),
            (false, false) => Some(Why::WLock),
        }
    }
}

/// The keys a write gives the indexes with range locks, compared with the
/// bounds of those locks (locks::Bounds).
struct WriteBounds<'a> {
    /// The indexes with range locks, and their positional key definitions.
    ranged: &'a [(u32, *mut BoxKeyDef)],
    /// The key readied, and its index's key definition.
    key: Option<(KeyTuple, *mut BoxKeyDef)>,
}

impl locks::Bounds for WriteBounds<'_> {
    fn key(&mut self, iid: u32, k: &[u8]) -> bool {
        self.key = None;
        let Some(&(_, kd)) = self.ranged.iter().find(|r| r.0 == iid) else {
            return false;
        };
        // SAFETY: in the transaction thread; the cache keeps `kd` for this
        // schema version, and nothing here changes the schema.
        self.key = unsafe { KeyTuple::new(k, kd) }.map(|t| (t, kd));
        self.key.is_some()
    }

    fn within(&mut self, lo: Option<&[u8]>, hi: Option<&[u8]>) -> bool {
        match &self.key {
            // SAFETY: as for `key`.
            Some((t, kd)) => unsafe { t.within(lo, hi, *kd) }.unwrap_or(true),
            None => true,
        }
    }
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
    unsafe fn begin_stream(raise: &Cell<bool>) -> Option<Txn> {
        Self::begin_fresh(STREAM_TIMEOUT, raise)
    }

    /// Public empty-transaction quorum barrier, followed by confirmed
    /// visibility before the first statement. Direct LINEARIZABLE Vinyl
    /// access remains unsupported; this is the pinned barrier composition.
    unsafe fn begin_fresh(timeout: f64, raise: &Cell<bool>) -> Option<Txn> {
        if box_txn_begin() != 0 {
            raise.set(true);
            return None;
        }
        let txn = Txn { open: true };
        // The final timeout MUST precede the first possible yield: it
        // cannot be changed after the transaction's timer has started.
        if box_txn_set_timeout(timeout) != 0
            || box_txn_set_isolation(TXN_ISOLATION_LINEARIZABLE) != 0
            || box_txn_set_isolation(TXN_ISOLATION_READ_CONFIRMED) != 0
        {
            let e = box_error_last();
            let (code, message) = if e.is_null() {
                (
                    ER_TRANSACTION_CONFLICT,
                    std::ffi::CString::new("strong read barrier failed").unwrap(),
                )
            } else {
                (
                    box_error_code(e),
                    std::ffi::CStr::from_ptr(box_error_message(e)).to_owned(),
                )
            };
            drop(txn);
            // Rollback and its callbacks may alter the current diagnostic.
            // Restore the original failure and propagate it without retry.
            box_error_set(
                c"tarn-storage".as_ptr(),
                line!(),
                code,
                c"%s".as_ptr(),
                message.as_ptr(),
            );
            raise.set(true);
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
const TXN_ISOLATION_LINEARIZABLE: u32 = 4;
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

/// What a write needs of the schema and of the locks on its table, read
/// before its transaction, which must not read the system spaces.
struct WriteInfo {
    sp: SpaceInfo,
    /// The primary key, for a replace (the row it replaces).
    pk: Option<IndexInfo>,
    /// The table may have locks of prepared transactions; row locks.
    locked: bool,
    plocked: bool,
    /// The indexes of the table with range locks, and their positional key
    /// definitions (to compare the keys written with the ranges).
    ranged: Vec<(u32, *mut BoxKeyDef)>,
}

/// What the writes need: each table memtx, with the primary key of the
/// tables a replace writes; and, where the table has locks of prepared
/// transactions or row locks (READ COMMITTED: the Lua procedures wait for
/// one on what they write), what their checks need (`write_allowed`): the
/// indexes, and how to compare keys with the ranges of those with range
/// locks. None: the writes are left to Lua. A synchronous table's commit
/// waits for its quorum, as the Lua procedures' do, and is in flight until
/// then.
fn write_infos(s: &State, writes: &[write::Write]) -> Option<Vec<WriteInfo>> {
    // Both autocommit and stream commits must defer capture to Lua. Check
    // every actual source before schema reads or a transaction/savepoint,
    // independently of the gateway's possibly stale definition fences.
    if writes.iter().any(|w| s.lazy_write_excluded(w.space_id)) {
        return back(Why::LazyCapture);
    }
    let mut infos = Vec::with_capacity(writes.len());
    for w in writes {
        let Some(sp) = space_info(w.space_id) else {
            return back(Why::Schema);
        };
        if !sp.memtx {
            return back(Why::Vinyl);
        }
        let (locked, plocked) = (s.space_locked(w.space_id), s.space_plocked(w.space_id));
        if (locked || plocked) && sp.indexes.is_none() {
            return back(Why::Schema);
        }
        let mut ranged = Vec::new();
        if locked && locks::ranges_on(w.space_id) {
            for ix in sp.indexes.as_deref().unwrap_or(&[]) {
                if locks::ranges_on_index(w.space_id, ix.iid) {
                    let kd = index_info(w.space_id, ix.iid).map_or(std::ptr::null_mut(), |i| i.key_def_pos);
                    if kd.is_null() {
                        return back(Why::Schema);
                    }
                    ranged.push((ix.iid, kd));
                }
            }
        }
        let pk = match w.op {
            Op::Replace => match index_info(w.space_id, 0) {
                Some(i) => Some(i),
                None => return back(Why::Schema),
            },
            _ => None,
        };
        infos.push(WriteInfo {
            sp,
            pk,
            locked,
            plocked,
            ranged,
        });
    }
    Some(infos)
}

/// The fields of a version of a row, if there is one (Err: not a tuple).
fn version_fields(t: Option<&[u8]>) -> Result<Option<Vec<&[u8]>>, ()> {
    match t {
        None => Ok(None),
        Some(t) => locks::fields_of(t).map(Some).ok_or(()),
    }
}

/// What a write that wrote `old` -> `new` (the row's versions, if any)
/// meets of the locks of prepared transactions and the row locks, as the
/// Lua procedures' writes_locked finds (locks::write_met), or of keys it
/// cannot look up; None when it meets none. Its primary key: for an insert
/// or a replace the row's, for an update or a delete the key it names
/// (write_target).
fn write_met(w: &write::Write, info: &WriteInfo, old: Option<&[u8]>, new: Option<&[u8]>) -> Option<Why> {
    if !info.locked && !info.plocked {
        return None;
    }
    let Some(indexes) = info.sp.indexes.as_deref() else {
        return Some(Why::Schema);
    };
    let pk = match w.op {
        Op::Insert | Op::Replace => {
            let parts = indexes.iter().find(|ix| ix.iid == 0).map(|ix| &ix.parts[..]);
            match (parts, locks::fields_of(&w.payload)) {
                (Some(p), Some(t)) if !p.is_empty() => locks::key_of(&t, p),
                _ => None,
            }
        }
        Op::Delete | Op::Update => locks::lock_key(&w.payload).map(|k| k.into_owned()),
    };
    let Some(pk) = pk else {
        return Some(Why::LockKey);
    };
    let (Ok(old), Ok(new)) = (version_fields(old), version_fields(new)) else {
        return Some(Why::Other);
    };
    let mut bounds = WriteBounds {
        ranged: &info.ranged,
        key: None,
    };
    locks::write_met(
        w.space_id,
        &pk,
        old.as_deref(),
        new.as_deref(),
        indexes,
        info.locked,
        info.plocked,
        &mut bounds,
    )
}

/// Make the writes (write_step), in the transaction of the request: what
/// each did, and their affected rows (`affected`). None, having stopped at
/// the first that failed, that the Lua procedures would make otherwise, or
/// that meets a lock (`write_allowed`: the Lua procedures wait for it, or
/// refuse the step): the caller's Undo takes back what was written. Then
/// the rows' buckets take writes here (BK.rows_refused), and the writes
/// count for them (BK.count_writes). (Nothing yields from the checks to the
/// commit: they see the state the commit is prepared in.)
///
/// # Safety
/// In the transaction thread, with the request's transaction open.
unsafe fn apply_writes(
    s: &mut State,
    writes: &[write::Write],
    infos: &[WriteInfo],
    found_rows: bool,
) -> Option<(Vec<Done>, u64)> {
    let mut done: Vec<Done> = Vec::with_capacity(writes.len());
    let mut n = 0u64;
    for (w, info) in writes.iter().zip(infos) {
        let (sp, pk) = (&info.sp, &info.pk);
        let p = w.payload.as_ptr().cast::<c_char>();
        let p_end = p.add(w.payload.len());
        let mut t: *mut BoxTuple = std::ptr::null_mut();
        let (old, new, res) = match w.op {
            Op::Insert => {
                if box_insert(w.space_id, p, p_end, &mut t) != 0 {
                    return back(Why::WriteError);
                }
                let r = data_of(t);
                (None, r.clone(), r)
            }
            Op::Replace => {
                let Some(key) = pk.as_ref().and_then(|pk| key_of(&w.payload, pk)) else {
                    return back(Why::Schema);
                };
                let old = get_row(w.space_id, &key).ok()?;
                if box_replace(w.space_id, p, p_end, &mut t) != 0 {
                    return back(Why::WriteError);
                }
                let r = data_of(t);
                (old, r.clone(), r)
            }
            Op::Delete => {
                let old = get_row(w.space_id, &w.payload).ok()?;
                if box_delete(w.space_id, 0, p, p_end, &mut t) != 0 {
                    return back(Why::WriteError);
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
                            return back(Why::Pad);
                        }
                    }
                    let v = |f: u32| write::val(f.checked_sub(1).and_then(|f| field_of(&old, f)));
                    if !write::arith_ok(&w.ops, &w.ranges, v) {
                        return back(Why::Arith);
                    }
                    let ops = write::update_ops(&w.ops, v);
                    let o = ops.as_ptr().cast::<c_char>();
                    if box_update(w.space_id, 0, p, p_end, o, o.add(ops.len()), 1, &mut t) != 0 {
                        return back(Why::WriteError);
                    }
                    let r = data_of(t);
                    (Some(old), r.clone(), r)
                }
            },
        };
        // (an update of a row that is not there writes nothing: the Lua
        // procedures check nothing of it)
        let skipped = w.op == Op::Update && old.is_none();
        if !skipped {
            if let Some(why) = write_met(w, info, old.as_deref(), new.as_deref()) {
                return back(why);
            }
        }
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
            return back(Why::Bucket);
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
    note(Why::Shape);
    let (writes, opts) = write::parse(buf)?;
    note(Why::Other);
    if unsafe { box_txn() } {
        return back(Why::Stream);
    }
    // Even a zero-row DELETE/UPDATE observes data and can return without
    // a synchronous WAL write. It needs the same freshness as a read.
    let raise = Cell::new(false);
    if fresh_read(false, true, writes.first()?.space_id, &raise).is_none() {
        return if raise.get() { Some(Served::Raise) } else { None };
    }
    if writes.iter().any(|w| !fresh_space_ok(w.space_id)) {
        return back(Why::Freshness);
    }
    let s = ready_state()?;
    // Gateway metadata can lag behind local DDL activation. Check every
    // actual source before schema reads or Txn::begin, not just opts.fence;
    // otherwise a stale/partial fence could bypass commit-atomic capture.
    if writes.iter().any(|w| s.lazy_write_excluded(w.space_id)) {
        return back(Why::LazyCapture);
    }
    // SAFETY: plain calls of the module API in the transaction thread.
    unsafe {
        // (a replica: tarn_autocommit raises ER_READONLY; a transaction in
        // progress: not an autocommit write)
        if box_is_ro() {
            return back(Why::ReadOnly);
        }
        if box_txn() {
            return back(Why::Stream);
        }
        // (the writes count for their buckets' load, in a window that has
        // not ended: else tarn_autocommit starts the next one)
        if s.count != 0 && fiber_clock() >= s.load_until {
            return back(Why::LoadWindow);
        }
    }
    // The definitions the writes were made with are current (else 1412),
    // and their buckets take writes here (else moved).
    if opts.fence.iter().any(|&(sid, ver)| s.fence(sid) > ver) {
        return back(Why::Fence);
    }
    if s.count != 0 && opts.wb.iter().any(|&b| !s.writable(b)) {
        return back(Why::Bucket);
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

/// What a read of a read set meets (reads_w_locked): a prepared
/// transaction's W lock on it -- for a point, on its key; for a range, on a
/// key of the index between its bounds -- or a key it cannot look up or
/// compare; None when it is free (also on a table without locks).
fn read_set_met(s: &State, r: &write::Read) -> Option<Why> {
    if !s.space_locked(r.space_id) {
        return None;
    }
    match r.key {
        Some(k) => locks::point_read_met(r.space_id, r.index_id, k),
        None => range_read_met(r.space_id, r.index_id, r.lo, r.hi),
    }
}

/// The state, when the write-side procedures may serve requests in it: the
/// module in use, no test hooks, and the bucket map, the lock index and the
/// fences current. Else None, the reason noted.
fn ready_state() -> Option<&'static mut State> {
    let Some(s) = state() else {
        return back(Why::NotReady);
    };
    let why = if s.ready == 0 {
        Why::NotReady
    } else if s.hooks != 0 {
        Why::TestHooks
    } else if s.bk_stale != 0 {
        Why::StaleBuckets
    } else if s.lk_stale != 0 {
        Why::StaleLocks
    } else if s.fence_stale != 0 {
        Why::StaleFences
    } else {
        return Some(s);
    };
    back(why)
}

/// What became of a commit of one shard served here (TarnCommit!CommitLocal),
/// or None for one left to tarn_commit_local, with nothing written: in a
/// stream, its transaction and reads are as they were. `committed`: as for
/// `autocommit`.
fn commit_local(buf: &[u8], committed: &Cell<bool>) -> Option<Served> {
    note(Why::Shape);
    let c = write::parse_commit(buf)?;
    note(Why::Other);
    // (row locks: the Lua procedure checks that they survived, and releases
    // them with the commit)
    if c.locks {
        return back(Why::OwnRowLocks);
    }
    if unsafe { box_txn() } {
        if !fresh_stream_ok() {
            return back(Why::Freshness);
        }
    } else if c.reads.is_empty() {
        let raise = Cell::new(false);
        if fresh_read(false, true, c.writes.first()?.space_id, &raise).is_none() {
            return if raise.get() { Some(Served::Raise) } else { None };
        }
    } else {
        return back(Why::Stream);
    }
    if !fresh_controls_ok()
        || !read_clock_ready()
        || c.writes.iter().any(|w| !fresh_space_ok(w.space_id))
        || c.reads.iter().any(|r| !fresh_space_ok(r.space_id))
    {
        return back(Why::Freshness);
    }
    let s = ready_state()?;
    // (a TRUNCATE or DROP TABLE holds a table: reads_ddl_conflict)
    if s.ddl_n != 0 {
        return back(Why::HeldTable);
    }
    // SAFETY: plain calls of the module API in the transaction thread.
    unsafe {
        if box_is_ro() {
            return back(Why::ReadOnly);
        }
        if s.count != 0 && fiber_clock() >= s.load_until {
            return back(Why::LoadWindow);
        }
    }
    // BK.commit_check: the transaction read every shard with the current
    // membership (BK.member_refused); its writes' definitions are current;
    // the buckets it writes take writes here, and those it read are served
    // here, none of them held (BK.held_conflict)
    if c.mver.is_some_and(|v| v < s.mver) {
        return back(Why::Bucket);
    }
    if c.fence.iter().any(|&(sid, ver)| s.fence(sid) > ver) {
        return back(Why::Fence);
    }
    if s.count != 0
        && (s.held != 0
            || c.wb.iter().any(|&b| !s.writable(b))
            || c.reads.iter().any(|r| r.bucket.is_some_and(|b| !s.serves(b))))
    {
        return back(Why::Bucket);
    }
    // reads_w_locked: no W lock of a prepared transaction on what it read
    if let Some(w) = c.reads.iter().find_map(|r| read_set_met(s, r)) {
        return back(w);
    }
    let infos = write_infos(s, &c.writes)?;
    // SAFETY: the transaction is the request's: the stream's, or this call's
    // without one; tuples are copied before anything else runs.
    unsafe {
        let undo = if box_txn() {
            // (a new election term since the stream began, which the Lua
            // procedure refuses: BK.stream_lost; a TRUNCATE or DROP TABLE
            // applied since, which it checks against the tables read:
            // reads_ddl_conflict)
            let id = txn_number();
            if id < s.term_txn || id < s.ddl_txn {
                return back(Why::Term);
            }
            Undo::Savepoint(Savepoint::take()?)
        } else if c.reads.is_empty() {
            // (a transaction that read nothing here: the coordinator sends
            // no stream)
            Undo::Txn(Txn::begin()?)
        } else {
            // (reads, and no transaction of their stream to commit them in)
            return back(Why::Stream);
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
    note(Why::Shape);
    let v = write::parse_validate(buf)?;
    note(Why::Other);
    // (row locks: the Lua procedure checks that they survived, and releases
    // them)
    if v.locks {
        return back(Why::OwnRowLocks);
    }
    // Never certify an arbitrary older ambient view with a late barrier.
    // The first read must have installed this exact transaction's token.
    if !unsafe { box_txn() } {
        return back(Why::Stream);
    }
    if !fresh_stream_ok() {
        return back(Why::Freshness);
    }
    if !fresh_controls_ok() || !read_clock_ready() || v.reads.iter().any(|r| !fresh_space_ok(r.space_id)) {
        return back(Why::Freshness);
    }
    let s = ready_state()?;
    // (a TRUNCATE or DROP TABLE holds a table: reads_ddl_conflict)
    if s.ddl_n != 0 {
        return back(Why::HeldTable);
    }
    if s.scratch == 0 {
        return back(Why::NotReady);
    }
    // SAFETY: plain calls of the module API in the transaction thread.
    unsafe {
        // (a demoted node: tarn_validate raises ER_READONLY; no stream
        // transaction: it fails the reads, or only ends row locks)
        if box_is_ro() {
            return back(Why::ReadOnly);
        }
        if !box_txn() {
            return back(Why::Stream);
        }
        // The node has been the leader since the stream began (TERM_TXN),
        // and no TRUNCATE or DROP TABLE was applied since (DDL_TXN: the Lua
        // procedure checks the tables read). (A change of the election term
        // makes the node read-only at once, and stale before it is writable
        // again: term_txn is the term's here.)
        let id = txn_number();
        if id < s.term_txn || id < s.ddl_txn {
            return back(Why::Term);
        }
    }
    // BK.commit_check: the membership a read of every shard was routed
    // with; the buckets read are served here, none of them held
    if v.mver.is_some_and(|m| m < s.mver) {
        return back(Why::Bucket);
    }
    if s.count != 0 && (s.held != 0 || v.reads.iter().any(|r| r.bucket.is_some_and(|b| !s.serves(b)))) {
        return back(Why::Bucket);
    }
    // reads_w_locked: no W lock of a prepared transaction on what it read
    if let Some(w) = v.reads.iter().find_map(|r| read_set_met(s, r)) {
        return back(w);
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
    proc_: Proc,
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
    // Write-only local commits and autocommit no-op results also require a
    // pre-data quorum barrier. Keep every parsed payload alive across it.
    let owned = std::slice::from_raw_parts(args as *const u8, len).to_vec();
    let buf = owned.as_slice();
    let committed = Cell::new(false);
    TXN_LOST.with(|l| l.set(false));
    fallback::begin();
    let out = match std::panic::catch_unwind(AssertUnwindSafe(|| step(buf, &committed))) {
        Ok(out) => out,
        // (once the commit began, the writes must not go to Lua as well)
        Err(_) if committed.get() => Some(Served::Reply(outcome_unknown())),
        Err(_) => back(Why::Panic),
    };
    let out = match out {
        None if TXN_LOST.with(|l| l.get()) => Some(Served::Reply(txn_lost())),
        out => out,
    };
    if let Some(s) = state() {
        count(s, out.is_some());
    }
    if out.is_none() {
        fallback::counted(proc_, None);
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
    write_step(ctx, args, args_end, autocommit, Proc::Write, |s, served| {
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
    write_step(ctx, args, args_end, commit_local, Proc::Commit, |s, served| {
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
    write_step(ctx, args, args_end, validate, Proc::Validate, |s, served| {
        if served {
            s.v_served += 1;
        } else {
            s.v_to_lua += 1;
        }
    })
}
