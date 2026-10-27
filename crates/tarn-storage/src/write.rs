//! `ts_autocommit`: tarn_autocommit (lua/tarn.lua) for the autocommit writes
//! it serves with nothing to wait for or refuse -- the single-shard DML of
//! SQL statements, to memtx tables: one local transaction that writes the
//! rows and commits (TarnCommit!CommitLocal of a transaction that read
//! nothing), when no prepared transaction holds a lock on the table, no row
//! is locked (READ COMMITTED), the buckets take writes here, and the
//! definition fence lets the writes through. Anything else goes to
//! tarn_autocommit, with nothing written: the reply ["lua"] tells the SQL
//! server to call it. This file reads the arguments and does the arithmetic
//! of a write; procs.rs runs it.
//! Sources that need lazy-index capture also go to Lua: a native write
//! cannot bypass the source row's atomic outbox transaction. The shard's
//! mirrored registry excludes them even when gateway metadata is stale.
//!
//! The values a write stores are the ones tarn_autocommit would store. The
//! Lua procedure decodes its arguments and Tarantool encodes them again, in
//! the forms its serializer gives Lua values: a double that holds an
//! integer becomes that integer (`lua_value`). The rows must be the same
//! bytes whichever procedure wrote them, as the affected rows of a later
//! write compare them.

use std::borrow::Cow;

use crate::mp::{Rd, Wr};

/// A write: tarn_autocommit's {op, space_id, payload[, ops, ranges, pad]},
/// its values as Lua stores them (`lua_value`).
#[derive(Debug, PartialEq)]
pub struct Write<'a> {
    pub op: Op,
    pub space_id: u32,
    /// insert, replace: the tuple; delete, update: the primary key (arrays).
    pub payload: Cow<'a, [u8]>,
    /// update: its operations.
    pub ops: Vec<UpOp<'a>>,
    /// update: the range of each integer column an operation adds to.
    pub ranges: Vec<Range>,
    /// update with a pad: the number of fields of the table's rows, which
    /// rows written before an ALTER TABLE added fields lack (the Lua
    /// procedure fills them in: such rows are left to it).
    pub width: Option<u64>,
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Op {
    Insert,
    Replace,
    Delete,
    Update,
}

/// An update operation {op, field, value}: '=' or '+', a field numbered
/// from 1.
#[derive(Debug, PartialEq)]
pub struct UpOp<'a> {
    pub add: bool,
    pub field: u32,
    /// The value, as MessagePack.
    pub value: Cow<'a, [u8]>,
}

/// {field, min, max, column name}: the values an integer column takes.
#[derive(Debug, PartialEq, Clone, Copy)]
pub struct Range {
    pub field: u32,
    pub lo: i128,
    pub hi: i128,
}

/// tarn_autocommit's options.
#[derive(Debug, PartialEq, Default)]
pub struct Opts {
    pub found_rows: bool,
    /// {space_id, version}: the definitions the writes were made with.
    pub fence: Vec<(u32, u64)>,
    /// The bucket of each write.
    pub wb: Vec<u64>,
}

/// tarn_autocommit's arguments (writes, opts): None for those it leaves to
/// the Lua procedure, or does not read.
pub fn parse(buf: &[u8]) -> Option<(Vec<Write<'_>>, Opts)> {
    let mut r = Rd::new(buf);
    if r.array()? != 2 {
        return None;
    }
    let n = r.array()?;
    let mut writes = Vec::with_capacity((n as usize).min(buf.len()));
    for _ in 0..n {
        writes.push(write(&mut r)?);
    }
    let mut opts = Opts::default();
    if !r.nil() {
        for _ in 0..r.map()? {
            match r.str()? {
                b"found_rows" => opts.found_rows = r.bool()?,
                b"fence" => {
                    let k = r.array()?;
                    if k % 2 != 0 {
                        return None;
                    }
                    for _ in 0..k / 2 {
                        opts.fence.push((u32::try_from(r.uint()?).ok()?, r.uint()?));
                    }
                }
                b"wb" => {
                    for _ in 0..r.array()? {
                        opts.wb.push(r.uint()?);
                    }
                }
                // (lock_timeout: no write served here waits for a lock)
                b"lock_timeout" => r.skip()?,
                _ => return None,
            }
        }
    }
    if r.peek().is_some() {
        return None;
    }
    Some((writes, opts))
}

/// A read of a commit, as far as its checks need: its table, and the bucket
/// it was routed by (None: every shard's).
#[derive(Debug, PartialEq)]
pub struct Read {
    pub space_id: u32,
    pub bucket: Option<u64>,
}

/// tarn_commit_local's arguments (txid, writes, rpoints, rranges, lock_gen,
/// fence, wb, mver), as far as its checks need.
#[derive(Debug, Default, PartialEq)]
pub struct Commit<'a> {
    pub writes: Vec<Write<'a>>,
    /// The points and ranges the transaction read here.
    pub reads: Vec<Read>,
    /// It holds row locks here (lock_gen): the Lua procedure checks them.
    pub locks: bool,
    pub fence: Vec<(u32, u64)>,
    pub wb: Vec<u64>,
    /// The membership version it was routed with, when it read every shard.
    pub mver: Option<u64>,
}

/// tarn_commit_local's arguments: None for those it leaves to the Lua
/// procedure, or does not read.
pub fn parse_commit(buf: &[u8]) -> Option<Commit<'_>> {
    let mut r = Rd::new(buf);
    if r.array()? != 8 {
        return None;
    }
    r.str()?;
    let mut c = Commit::default();
    let n = r.array()?;
    c.writes.reserve((n as usize).min(buf.len()));
    for _ in 0..n {
        let w = write(&mut r)?;
        // (a commit's writes are rows: inserts, replaces and deletes)
        if w.op == Op::Update {
            return None;
        }
        c.writes.push(w);
    }
    read_set(&mut r, &mut c.reads)?;
    c.locks = !r.nil();
    if c.locks {
        r.skip()?;
    }
    if !r.nil() {
        let k = r.array()?;
        if k % 2 != 0 {
            return None;
        }
        for _ in 0..k / 2 {
            c.fence.push((u32::try_from(r.uint()?).ok()?, r.uint()?));
        }
    }
    if !r.nil() {
        for _ in 0..r.array()? {
            c.wb.push(r.uint()?);
        }
    }
    c.mver = if r.nil() { None } else { Some(r.uint()?) };
    if r.peek().is_some() {
        return None;
    }
    Some(c)
}

/// A read set: points {space, index, key, bucket}, ranges {space, index,
/// lo, hi, lo_incl, hi_incl, bucket}.
fn read_set(r: &mut Rd, reads: &mut Vec<Read>) -> Option<()> {
    for (len, skip) in [(4, 2), (7, 5)] {
        for _ in 0..r.array()? {
            if r.array()? != len {
                return None;
            }
            let space_id = u32::try_from(r.uint()?).ok()?;
            for _ in 0..skip {
                r.skip()?;
            }
            let bucket = if r.nil() { None } else { Some(r.uint()?) };
            reads.push(Read { space_id, bucket });
        }
    }
    Some(())
}

/// tarn_validate's arguments (txid, rpoints, rranges, cross, lock_gen,
/// mver), as far as its checks need.
#[derive(Debug, Default, PartialEq)]
pub struct Validate {
    /// The points and ranges the transaction read here.
    pub reads: Vec<Read>,
    /// It holds row locks here (lock_gen): the Lua procedure checks them.
    pub locks: bool,
    /// The membership version it was routed with, when it read every shard.
    pub mver: Option<u64>,
}

/// tarn_validate's arguments: None for those it leaves to the Lua
/// procedure, or does not read.
pub fn parse_validate(buf: &[u8]) -> Option<Validate> {
    let mut r = Rd::new(buf);
    if r.array()? != 6 {
        return None;
    }
    r.str()?;
    let mut v = Validate::default();
    read_set(&mut r, &mut v.reads)?;
    r.skip()?; // (cross: the same checks)
    v.locks = !r.nil();
    if v.locks {
        r.skip()?;
    }
    v.mver = if r.nil() { None } else { Some(r.uint()?) };
    if r.peek().is_some() {
        return None;
    }
    Some(v)
}

fn write<'a>(r: &mut Rd<'a>) -> Option<Write<'a>> {
    let len = r.array()?;
    let op = match r.str()? {
        b"insert" => Op::Insert,
        b"replace" => Op::Replace,
        b"delete" => Op::Delete,
        b"update" => Op::Update,
        _ => return None,
    };
    let space_id = u32::try_from(r.uint()?).ok()?;
    let payload = lua_array(r.raw()?)?;
    let mut w = Write {
        op,
        space_id,
        payload,
        ops: Vec::new(),
        ranges: Vec::new(),
        width: None,
    };
    match (op, len) {
        (Op::Update, 4..=6) => {
            for _ in 0..r.array()? {
                if r.array()? != 3 {
                    return None;
                }
                let add = match r.str()? {
                    b"+" => true,
                    b"=" => false,
                    _ => return None,
                };
                let field = u32::try_from(r.uint()?).ok()?;
                w.ops.push(UpOp {
                    add,
                    field,
                    value: lua_value(r.raw()?)?,
                });
            }
            if len >= 5 && !r.nil() {
                for _ in 0..r.array()? {
                    if r.array()? != 4 {
                        return None;
                    }
                    let field = u32::try_from(r.uint()?).ok()?;
                    let (lo, hi) = (r.int()?, r.int()?);
                    r.skip()?; // (the column's name, for the error)
                    w.ranges.push(Range { field, lo, hi });
                }
            }
            // pad: {n, v1, v2, ...}: the rows end at field n + k, k values
            if len == 6 && !r.nil() {
                let k = r.array()?.checked_sub(1)?;
                let n = r.uint()?;
                for _ in 0..k {
                    r.skip()?;
                }
                w.width = Some(n.checked_add(k as u64)?);
            }
        }
        (Op::Update, _) => return None,
        (_, 3) => {}
        _ => return None,
    }
    Some(w)
}

/// 2^63 and 2^64, as doubles.
const P63: f64 = 9_223_372_036_854_775_808.0;
const P64: f64 = 18_446_744_073_709_551_616.0;

/// A value as Lua stores it once decoded (Tarantool's serializer, for a
/// Lua number: luaL_tofield): a double or a float that holds an integer in
/// [-2^63, 2^64) -- -0.0 too -- is that integer, and any other a double;
/// integers, strings and binaries take their shortest forms. Nil, booleans
/// and decimals (MP_EXT 1, which the SQL server encodes as Tarantool does)
/// are kept. None for what Lua may store otherwise -- an array, a map,
/// another extension, a NaN or an infinity: its write is left to Lua.
pub fn lua_value(v: &[u8]) -> Option<Cow<'_, [u8]>> {
    let mut r = Rd::new(v);
    let mut w = Wr::with_capacity(v.len());
    match r.peek()? {
        0x00..=0x7f | 0xe0..=0xff | 0xc0 | 0xc2 | 0xc3 => return Some(Cow::Borrowed(v)),
        0xcc..=0xd3 => {
            w.int(r.int()?);
        }
        0xca | 0xcb => {
            let f = r.float()?;
            if !f.is_finite() {
                return None;
            }
            if f.fract() == 0.0 && (0.0..P64).contains(&f) {
                w.uint(f as u64);
            } else if f.fract() == 0.0 && (-P63..P63).contains(&f) {
                w.int(f as i64 as i128);
            } else {
                w.double(f);
            }
        }
        b @ (0xa0..=0xbf | 0xd9..=0xdb | 0xc4..=0xc6) => {
            let bin = (0xc4..=0xc6).contains(&b);
            let body = if bin { r.bin()? } else { r.str()? };
            w.bytes_header(bin, body.len()).raw(body);
        }
        // a decimal, in its shortest header
        0xd4..=0xd8 | 0xc7..=0xc9 => {
            let n = v.len() - ext_header_len(v[0]);
            if v.get(ext_header_len(v[0]) - 1) != Some(&1) || ext_header_len(v[0]) != ext_header_size(n) {
                return None;
            }
            return Some(Cow::Borrowed(v));
        }
        _ => return None,
    }
    Some(if w.0 == v { Cow::Borrowed(v) } else { Cow::Owned(w.0) })
}

/// The length of an extension's header (its marker, length and type).
fn ext_header_len(marker: u8) -> usize {
    match marker {
        0xd4..=0xd8 => 2,
        0xc7 => 3,
        0xc8 => 4,
        _ => 6,
    }
}

/// The length of the shortest header of an extension of `n` bytes.
fn ext_header_size(n: usize) -> usize {
    match n {
        1 | 2 | 4 | 8 | 16 => 2,
        0..=0xff => 3,
        0x100..=0xffff => 4,
        _ => 6,
    }
}

/// An array of values (a tuple or a key) as Lua stores it: each value as
/// `lua_value` has it, in an array of the shortest header.
pub fn lua_array(v: &[u8]) -> Option<Cow<'_, [u8]>> {
    let mut r = Rd::new(v);
    let n = r.array()?;
    let mut w = Wr::with_capacity(v.len());
    w.array(n);
    let mut same = w.0.len() == r.pos;
    for _ in 0..n {
        let x = r.raw()?;
        let y = lua_value(x)?;
        same &= matches!(y, Cow::Borrowed(_));
        w.raw(&y);
    }
    if r.pos != v.len() {
        return None;
    }
    Some(if same { Cow::Borrowed(v) } else { Cow::Owned(w.0) })
}

/// A field's value, as an update's checks see it.
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Val {
    /// Missing, or NULL.
    Nil,
    Int(i128),
    /// Anything else (a double, a decimal, a string ...).
    Other,
}

pub fn val(mp: Option<&[u8]>) -> Val {
    let Some(mp) = mp else { return Val::Nil };
    let mut r = Rd::new(mp);
    if r.nil() {
        return Val::Nil;
    }
    match r.int() {
        Some(v) => Val::Int(v),
        None => Val::Other,
    }
}

const I64: (i128, i128) = (i64::MIN as i128, i64::MAX as i128);
const U64_MAX: i128 = u64::MAX as i128;

/// check_arith: whether the update's arithmetic on the integer columns of
/// `ranges` stays in BIGINT (BIGINT UNSIGNED for an unsigned column) and
/// ends in the column's range, as MySQL requires; false when it does not,
/// or when the values are not integers (the Lua procedure then decides, or
/// fails with 1690 or 1264). `old(field)`: the row's value.
pub fn arith_ok(ops: &[UpOp], ranges: &[Range], old: impl Fn(u32) -> Val) -> bool {
    for r in ranges {
        let mut v = old(r.field);
        for op in ops.iter().filter(|op| op.field == r.field) {
            // (once NULL, a value stays NULL to Lua's check)
            if v == Val::Nil {
                break;
            }
            let Val::Int(cur) = v else { return false };
            if op.add {
                let Val::Int(d) = val(Some(&op.value)) else {
                    return false;
                };
                let sum = cur + d;
                let bad = if r.lo >= 0 {
                    !(0..=U64_MAX).contains(&sum)
                } else {
                    !(I64.0..=I64.1).contains(&sum)
                };
                if bad {
                    return false;
                }
                v = Val::Int(sum);
            } else {
                v = val(Some(&op.value));
            }
        }
        match v {
            Val::Nil => {}
            Val::Int(x) => {
                if x < r.lo || x > r.hi {
                    return false;
                }
            }
            Val::Other => return false,
        }
    }
    true
}

/// mysql_update_ops: the operations to apply, a MessagePack array -- the
/// update's, except that '+' on a NULL field leaves it NULL (MySQL: NULL +
/// d = NULL; Tarantool would fail).
pub fn update_ops(ops: &[UpOp], old: impl Fn(u32) -> Val) -> Vec<u8> {
    let kept: Vec<&UpOp> = ops.iter().filter(|op| !(op.add && old(op.field) == Val::Nil)).collect();
    let mut w = crate::mp::Wr::with_capacity(16 * kept.len() + 8);
    w.array(kept.len() as u32);
    for op in kept {
        w.array(3)
            .str(if op.add { "+" } else { "=" })
            .uint(op.field as u64)
            .raw(&op.value);
    }
    w.0
}

/// MySQL's affected rows of one write (`affected`): `old`, `new` the row's
/// versions before and after it (MessagePack), if any.
pub fn affected(op: Op, old: Option<&[u8]>, new: Option<&[u8]>, found_rows: bool) -> u64 {
    let same = matches!((old, new), (Some(a), Some(b)) if a == b);
    match op {
        Op::Insert => 1,
        Op::Delete => old.is_some() as u64,
        Op::Replace => {
            if old.is_none() || same {
                1
            } else {
                2
            }
        }
        Op::Update => {
            if old.is_none() || new.is_none() || (same && !found_rows) {
                0
            } else {
                1
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mp::Wr;

    /// tarn_autocommit's arguments for an UPDATE of row {5} of space 512:
    /// SET field 2 = field 2 + 1, an INT column.
    fn update(pad: bool) -> Vec<u8> {
        let mut w = Wr::default();
        w.array(2).array(1).array(6).str("update").uint(512).array(1).uint(5);
        w.array(2);
        w.array(3).str("+").uint(2).uint(1);
        w.array(3).str("=").uint(3).str("x");
        w.array(1)
            .array(4)
            .uint(2)
            .raw(&[0xd2, 0x80, 0, 0, 0])
            .uint(i32::MAX as u64)
            .str("k");
        if pad {
            w.array(2).uint(4).uint(0);
        } else {
            w.raw(&[0xc0]);
        }
        w.raw(&[0x84]);
        w.str("lock_timeout").raw(&[0xcb, 0x40, 0x49, 0, 0, 0, 0, 0, 0]);
        w.str("found_rows").raw(&[0xc2]);
        w.str("fence").array(2).uint(512).uint(3);
        w.str("wb").array(1).uint(7);
        w.0
    }

    #[test]
    fn parses_an_update() {
        let buf = update(false);
        let (ws, o) = parse(&buf).unwrap();
        assert_eq!(ws.len(), 1);
        let w = &ws[0];
        assert_eq!((w.op, w.space_id, &w.payload[..]), (Op::Update, 512, &[0x91, 5][..]));
        assert_eq!(w.ops.len(), 2);
        assert!(w.ops[0].add && w.ops[0].field == 2 && w.ops[0].value[..] == [1]);
        assert!(!w.ops[1].add && w.ops[1].field == 3);
        assert_eq!(
            w.ranges,
            vec![Range {
                field: 2,
                lo: i32::MIN as i128,
                hi: i32::MAX as i128
            }]
        );
        assert_eq!(
            o,
            Opts {
                found_rows: false,
                fence: vec![(512, 3)],
                wb: vec![7]
            }
        );
        assert_eq!(w.width, None);
        // a pad {4, 0}: the rows have 5 fields
        let padded = update(true);
        let (ws, _) = parse(&padded).unwrap();
        assert_eq!(ws[0].width, Some(5));
    }

    #[test]
    fn parses_inserts_and_deletes() {
        let mut w = Wr::default();
        w.array(2).array(2);
        w.array(3).str("insert").uint(512).array(2).uint(1).str("a");
        w.array(3).str("delete").uint(513).array(1).uint(9);
        w.raw(&[0x80]);
        let (ws, o) = parse(&w.0).unwrap();
        assert_eq!((ws[0].op, ws[1].op, ws[1].space_id), (Op::Insert, Op::Delete, 513));
        assert_eq!(o, Opts::default());
        // other ops, other options, a payload that is not an array
        for bad in [
            &[
                0x92, 0x91, 0x93, 0xa6, b'u', b'p', b's', b'e', b'r', b't', 0x01, 0x90, 0x80,
            ][..],
            &[
                0x92, 0x91, 0x93, 0xa6, b'i', b'n', b's', b'e', b'r', b't', 0x01, 0x05, 0x80,
            ],
            &[0x92, 0x90, 0x81, 0xa1, b'x', 0x01],
        ] {
            assert!(parse(bad).is_none(), "{:x?}", bad);
        }
        assert!(parse(&[0x92, 0x90, 0x80]).is_some());
    }

    #[test]
    fn parses_a_commit() {
        // {txid, {insert, delete}, {point}, {range}, nil, {512, 3}, {7, 7}, nil}
        let mut w = Wr::default();
        w.array(8).str("T1");
        w.array(2);
        w.array(3).str("insert").uint(512).array(2).uint(1).double(5.0);
        w.array(3).str("delete").uint(512).array(1).uint(2);
        w.array(1).array(4).uint(512).uint(0).array(1).uint(9).uint(7);
        w.array(1)
            .array(7)
            .uint(513)
            .uint(1)
            .raw(&[0xc0, 0x91, 0x05, 0xc3, 0xc2, 0xc0]);
        w.raw(&[0xc0]);
        w.array(2).uint(512).uint(3);
        w.array(2).uint(7).uint(7);
        w.raw(&[0xc0]);
        let c = parse_commit(&w.0).unwrap();
        assert_eq!(c.writes.len(), 2);
        assert_eq!(
            &c.writes[0].payload[..],
            &[0x92, 0x01, 0x05][..],
            "values as Lua stores them"
        );
        assert_eq!(
            c.reads,
            vec![
                Read {
                    space_id: 512,
                    bucket: Some(7)
                },
                Read {
                    space_id: 513,
                    bucket: None
                }
            ]
        );
        assert!(!c.locks);
        assert_eq!((c.fence, c.wb, c.mver), (vec![(512, 3)], vec![7, 7], None));
        // row locks, a membership version
        let mut w = Wr::default();
        w.array(8)
            .str("T2")
            .array(0)
            .array(0)
            .array(0)
            .str("g/1")
            .array(0)
            .array(0)
            .uint(4);
        let c = parse_commit(&w.0).unwrap();
        assert!(c.locks && c.mver == Some(4));
        // an update is not a commit's
        let mut w = Wr::default();
        w.array(8).str("T3").array(1);
        w.array(4).str("update").uint(512).array(1).uint(1).array(0);
        w.array(0).array(0).raw(&[0xc0, 0xc0, 0xc0, 0xc0]);
        assert!(parse_commit(&w.0).is_none());
    }

    #[test]
    fn parses_a_validation() {
        // {txid, {point}, {range}, cross, nil, nil}
        let mut w = Wr::default();
        w.array(6).str("T1");
        w.array(1).array(4).uint(512).uint(0).array(1).uint(9).uint(7);
        w.array(1)
            .array(7)
            .uint(513)
            .uint(1)
            .raw(&[0xc0, 0x91, 0x05, 0xc3, 0xc2, 0xc0]);
        w.raw(&[0xc3, 0xc0, 0xc0]);
        let v = parse_validate(&w.0).unwrap();
        assert_eq!(
            v.reads,
            vec![
                Read {
                    space_id: 512,
                    bucket: Some(7)
                },
                Read {
                    space_id: 513,
                    bucket: None
                }
            ]
        );
        assert!(!v.locks && v.mver.is_none());
        // row locks, a membership version
        let mut w = Wr::default();
        w.array(6).str("T2").array(0).array(0).raw(&[0xc2]).str("g/1").uint(4);
        let v = parse_validate(&w.0).unwrap();
        assert!(v.locks && v.mver == Some(4));
        // arguments of another kind
        let mut w = Wr::default();
        w.array(5).str("T3").array(0).array(0).raw(&[0xc2, 0xc0]);
        assert!(parse_validate(&w.0).is_none());
    }

    fn op(add: bool, field: u32, value: &[u8]) -> UpOp<'_> {
        UpOp {
            add,
            field,
            value: Cow::Borrowed(value),
        }
    }

    fn double(v: f64) -> Vec<u8> {
        let mut w = Wr::default();
        w.double(v);
        w.0
    }

    #[test]
    fn values_as_lua_stores_them() {
        // doubles and floats that hold integers: those integers
        let as_lua = |v: &[u8]| lua_value(v).map(|c| c.into_owned());
        for (f, want) in [
            (0.0, &[0x00][..]),
            (-0.0, &[0x00]),
            (5.0, &[0x05]),
            (-3.0, &[0xfd]),
            (300.0, &[0xcd, 0x01, 0x2c]),
            (-200.0, &[0xd1, 0xff, 0x38]),
            (9007199254740992.0, &[0xcf, 0, 0x20, 0, 0, 0, 0, 0, 0]),
            (P63, &[0xcf, 0x80, 0, 0, 0, 0, 0, 0, 0]),
            (-P63, &[0xd3, 0x80, 0, 0, 0, 0, 0, 0, 0]),
        ] {
            assert_eq!(as_lua(&double(f)).as_deref(), Some(want), "{}", f);
        }
        // other doubles: as they are
        for f in [1.5, -0.25, P64, 1e300, -1e19, 1e-300] {
            assert_eq!(lua_value(&double(f)), Some(Cow::Borrowed(&double(f)[..])), "{}", f);
        }
        // floats: integers, or doubles
        assert_eq!(as_lua(&[0xca, 0x40, 0, 0, 0]).as_deref(), Some(&[0x02][..]));
        assert_eq!(as_lua(&[0xca, 0x3f, 0, 0, 0]), Some(double(0.5)));
        // not numbers: left to Lua
        for f in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert_eq!(lua_value(&double(f)), None);
        }
        // integers, strings and binaries: their shortest forms
        assert_eq!(as_lua(&[0xd0, 0x05]).as_deref(), Some(&[0x05][..]));
        assert_eq!(as_lua(&[0xcd, 0x00, 0x05]).as_deref(), Some(&[0x05][..]));
        assert_eq!(as_lua(&[0xd1, 0xff, 0x38]).as_deref(), Some(&[0xd1, 0xff, 0x38][..]));
        assert_eq!(as_lua(&[0xd9, 0x01, b'x']).as_deref(), Some(&[0xa1, b'x'][..]));
        assert_eq!(as_lua(&[0xc5, 0x00, 0x01, 7]).as_deref(), Some(&[0xc4, 0x01, 7][..]));
        for v in [
            &[0xa1, b'x'][..],
            &[0xc4, 0x01, 7],
            &[0xc0],
            &[0xc3],
            &[0xff],
            &[0xcc, 0x80],
        ] {
            assert_eq!(lua_value(v), Some(Cow::Borrowed(v)), "{:x?}", v);
        }
        // decimals in their shortest headers; arrays, maps, other extensions
        let dec = &[0xc7, 0x03, 0x01, 0x02, 0x15, 0x0c][..];
        assert_eq!(lua_value(dec), Some(Cow::Borrowed(dec)));
        let dec4 = &[0xd6, 0x01, 0x03, 0x12, 0x34, 0x5d][..];
        assert_eq!(lua_value(dec4), Some(Cow::Borrowed(dec4)));
        for v in [
            &[0xc7, 0x04, 0x01, 0x03, 0x12, 0x34, 0x5d][..],
            &[0xd8, 0x02, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            &[0x91, 0x01],
            &[0x80],
        ] {
            assert_eq!(lua_value(v), None, "{:x?}", v);
        }
        // arrays: as they are when every value is, else converted
        let t = &[0x93, 0x01, 0xa1, b'x', 0xc0][..];
        assert_eq!(lua_array(t), Some(Cow::Borrowed(t)));
        let mut w = Wr::default();
        w.array(3).uint(1).double(5.0).double(1.5);
        let mut want = Wr::default();
        want.array(3).uint(1).uint(5).double(1.5);
        assert_eq!(lua_array(&w.0).map(|c| c.into_owned()), Some(want.0));
        assert_eq!(lua_array(&[0xdc, 0x00, 0x01, 0x05]).as_deref(), Some(&[0x91, 0x05][..]));
        assert_eq!(lua_array(&[0x92, 0x01, 0x91, 0x02]), None);
        assert_eq!(lua_array(&[0x05]), None);
    }

    #[test]
    fn arithmetic() {
        let int = Range {
            field: 2,
            lo: i32::MIN as i128,
            hi: i32::MAX as i128,
        };
        let uns = Range {
            field: 2,
            lo: 0,
            hi: 255,
        };
        let big = Range {
            field: 2,
            lo: i64::MIN as i128,
            hi: i64::MAX as i128,
        };
        let one = [0x01u8];
        let minus = [0xffu8];
        let old = |v: Val| move |_f: u32| v;
        assert!(arith_ok(&[op(true, 2, &one)], &[int], old(Val::Int(5))));
        // out of the column's range: 1264
        assert!(!arith_ok(&[op(true, 2, &one)], &[uns], old(Val::Int(255))));
        assert!(!arith_ok(&[op(true, 2, &minus)], &[uns], old(Val::Int(0))));
        // out of BIGINT: 1690
        assert!(!arith_ok(&[op(true, 2, &one)], &[big], old(Val::Int(i64::MAX as i128))));
        let int64 = Range {
            field: 2,
            lo: i64::MIN as i128,
            hi: i64::MAX as i128,
        };
        assert!(arith_ok(
            &[op(true, 2, &minus)],
            &[int64],
            old(Val::Int(i64::MAX as i128))
        ));
        // NULL stays NULL, unchecked; an assignment to it too
        assert!(arith_ok(&[op(true, 2, &one)], &[uns], old(Val::Nil)));
        assert!(arith_ok(&[op(false, 2, &[0xcd, 0x10, 0x00])], &[uns], old(Val::Nil)));
        // an assignment, then an addition
        assert!(!arith_ok(
            &[op(false, 2, &[0xcc, 0xff]), op(true, 2, &one)],
            &[uns],
            old(Val::Int(1))
        ));
        assert!(arith_ok(
            &[op(false, 2, &[0xcc, 0xfe]), op(true, 2, &one)],
            &[uns],
            old(Val::Int(1))
        ));
        // values the Lua procedure decides
        assert!(!arith_ok(&[op(true, 2, &one)], &[int], old(Val::Other)));
        assert!(!arith_ok(&[op(true, 2, &[0xa1, b'x'])], &[int], old(Val::Int(1))));
        // operations on other fields do not count
        assert!(arith_ok(&[op(true, 3, &one)], &[uns], old(Val::Int(255))));
    }

    #[test]
    fn update_ops_skip_additions_to_null() {
        let one = [0x01u8];
        let ops = [op(true, 2, &one), op(false, 3, &[0xa1, b'x']), op(true, 4, &one)];
        let w = update_ops(&ops, |f| if f == 2 { Val::Nil } else { Val::Int(1) });
        let mut r = Rd::new(&w);
        assert_eq!(r.array(), Some(2));
        assert_eq!((r.array(), r.str(), r.uint()), (Some(3), Some(&b"="[..]), Some(3)));
        r.skip().unwrap();
        assert_eq!(
            (r.array(), r.str(), r.uint(), r.uint()),
            (Some(3), Some(&b"+"[..]), Some(4), Some(1))
        );
    }

    #[test]
    fn affected_rows() {
        let (a, b) = (&[0x91, 1][..], &[0x91, 2][..]);
        assert_eq!(affected(Op::Insert, None, Some(a), false), 1);
        assert_eq!(affected(Op::Replace, None, Some(a), false), 1);
        assert_eq!(affected(Op::Replace, Some(a), Some(a), false), 1);
        assert_eq!(affected(Op::Replace, Some(a), Some(b), false), 2);
        assert_eq!(affected(Op::Delete, Some(a), None, false), 1);
        assert_eq!(affected(Op::Delete, None, None, false), 0);
        assert_eq!(affected(Op::Update, None, None, true), 0);
        assert_eq!(affected(Op::Update, Some(a), Some(a), false), 0);
        assert_eq!(affected(Op::Update, Some(a), Some(a), true), 1);
        assert_eq!(affected(Op::Update, Some(a), Some(b), false), 1);
    }
}
