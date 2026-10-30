//! Optional, typed read RPCs. Only a matched absent-procedure response
//! permits fallback without an executed read; malformed replies fail closed.
use super::*;

pub struct Projection {
    pub cols: Vec<usize>,
    pub boundary: Vec<usize>,
    pub preds: Vec<StoragePred>,
}

pub struct StorageAgg {
    pub col: Option<usize>,
    pub op: &'static str,
}

pub enum Reduced {
    Unavailable,
    Incomplete,
    Complete(Vec<Value>),
}

fn predicates(m: &mut Mp, t: &TableDef, preds: &[StoragePred]) {
    m.arr(preds.len());
    for p in preds {
        m.arr(5).uint(t.field_of(p.col) as u64 + 1);
        match &p.op {
            PredOp::Cmp(op, v) => m.str(op).val(v),
            PredOp::In(vs) => m.str("in").vals(vs),
            PredOp::IsNull => m.str("null").nil(),
            PredOp::NotNull => m.str("notnull").nil(),
        };
        m.val(&t.layout.fills[p.col]).bool(p.int);
    }
}

async fn optional(
    client: &Client,
    stream: u64,
    name: &'static str,
    m: &Mp,
) -> SqlResult<Option<(Bytes, Option<tarn_common::ResourceGuard>)>> {
    let (reply, wire) = crate::budget::call_read(client, stream, name, &m.0).await?;
    match reply {
        Ok(r) => r
            .data()
            .map(|data| Some((data, wire)))
            .ok_or_else(|| crate::mpw::proto("no pushdown data")),
        Err(tarn_iproto::Error::Server { code, .. }) if code == tarn_iproto::errcode::ER_NO_SUCH_PROC => Ok(None),
        Err(e) => Err(from_iproto_read(e)),
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn reduce_rows(
    client: &Client,
    stream: u64,
    t: &TableDef,
    index: u32,
    key: &[Value],
    iter: Iter,
    end: Option<(&[Value], bool)>,
    preds: &[StoragePred],
    exclude: &[&[Value]],
    lock_check: bool,
    begin: bool,
    route: &Route,
    aggs: &[StorageAgg],
) -> SqlResult<Reduced> {
    crate::observe::read(route);
    let mut m = Mp::new();
    m.arr(12)
        .uint(t.space_id as u64)
        .uint(index as u64)
        .vals(key)
        .uint(iter as u64);
    match end {
        Some((k, incl)) => m.vals(k).bool(incl),
        None => m.nil().bool(true),
    };
    predicates(&mut m, t, preds);
    m.arr(exclude.len());
    for k in exclude {
        m.vals(k);
    }
    m.bool(lock_check).bool(begin);
    route.encode(&mut m);
    m.arr(aggs.len());
    for a in aggs {
        m.arr(3).str(a.op).uint(a.col.map_or(0, |c| t.field_of(c) as u64 + 1));
        m.val(&a.col.map_or(Value::Null, |c| t.layout.fills[c].clone()));
    }
    let Some((d, _wire)) = optional(client, stream, "tarn_reduce", &m).await? else {
        return Ok(Reduced::Unavailable);
    };
    decode_reduced(&d, aggs.len())
}

fn decode_reduced(d: &Bytes, width: usize) -> SqlResult<Reduced> {
    let (status, rd) = lua_status(d)?;
    let mut head = Rd::new(d);
    if status != "ok" || head.array()? != 1 || head.array()? != 3 {
        return Err(crate::mpw::proto("invalid reduction envelope"));
    }
    let mut rd = Rd::shared_at(d, rd);
    let n = rd.array()?;
    if n != 0 && n != width {
        return Err(crate::mpw::proto("partial aggregate width changed"));
    }
    crate::budget::decoded(d.len(), 1, n)?;
    let values = (0..n).map(|_| rd.value()).collect::<SqlResult<Vec<_>>>()?;
    let complete = match rd.buf.first() {
        Some(0xc3) => true,
        Some(0xc2) => false,
        _ => return Err(crate::mpw::proto("missing reduction completion")),
    };
    rd.skip()?;
    if !rd.buf.is_empty() || (complete && n != width) || (!complete && n != 0) {
        return Err(crate::mpw::proto("incomplete reduction payload"));
    }
    Ok(if complete {
        Reduced::Complete(values)
    } else {
        Reduced::Incomplete
    })
}

pub struct ProjectedPage {
    pub rows: Vec<Row>,
    pub next: Option<Vec<u8>>,
    pub read: usize,
    pub last: Option<Row>,
}

fn projected(rd: &mut Rd<'_>, t: &TableDef, cols: &[usize]) -> SqlResult<Row> {
    if rd.array()? != cols.len() {
        return Err(crate::mpw::proto("projected row width changed"));
    }
    let mut row = vec![Value::Null; t.columns.len()];
    for &c in cols {
        let v = rd.value()?;
        row[c] = if t.layout.floats.contains(&c) {
            match v {
                Value::Int(i) => Value::Float(i as f64),
                Value::UInt(u) => Value::Float(u as f64),
                other => other,
            }
        } else {
            v
        };
    }
    Ok(row)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn project_rows(
    client: &Client,
    stream: u64,
    t: &TableDef,
    index: u32,
    key: &[Value],
    iter: Iter,
    end: Option<(&[Value], bool)>,
    lock_check: bool,
    begin: bool,
    after: Option<&[u8]>,
    route: &Route,
    project: &Projection,
) -> SqlResult<Option<ProjectedPage>> {
    crate::observe::read(route);
    let mut m = Mp::new();
    m.arr(15)
        .uint(t.space_id as u64)
        .uint(index as u64)
        .vals(key)
        .uint(iter as u64)
        .nil();
    match end {
        Some((k, incl)) => m.vals(k).bool(incl),
        None => m.nil().bool(true),
    };
    m.bool(lock_check).nil().bool(begin);
    match after {
        Some(a) => m.raw(a),
        None => m.nil(),
    };
    route.encode(&mut m);
    m.arr(project.cols.len());
    for &c in &project.cols {
        m.arr(2).uint(t.field_of(c) as u64 + 1).val(&t.layout.fills[c]);
    }
    predicates(&mut m, t, &project.preds);
    m.arr(project.boundary.len());
    for &c in &project.boundary {
        m.arr(2).uint(t.field_of(c) as u64 + 1).val(&t.layout.fills[c]);
    }
    let Some((d, _wire)) = optional(client, stream, "tarn_select_projected", &m).await? else {
        return Ok(None);
    };
    decode_projected(&d, t, project).map(Some)
}

fn decode_projected(d: &Bytes, t: &TableDef, project: &Projection) -> SqlResult<ProjectedPage> {
    let (status, rd) = lua_status(d)?;
    let mut head = Rd::new(d);
    if status != "ok" || head.array()? != 1 || head.array()? != 5 {
        return Err(crate::mpw::proto("invalid projected envelope"));
    }
    let mut rd = Rd::shared_at(d, rd);
    let n = rd.array()?;
    // Sparse projected rows still allocate the full logical width. Include
    // the scan boundary row and charge before any row vector is allocated.
    crate::budget::decoded(d.len(), n.saturating_add(1), t.columns.len())?;
    let rows = (0..n)
        .map(|_| projected(&mut rd, t, &project.cols))
        .collect::<SqlResult<Vec<_>>>()?;
    let next = if rd.is_nil() {
        rd.skip()?;
        None
    } else {
        Some(rd.raw_value()?.to_vec())
    };
    let read = match rd.value()? {
        Value::Int(n) if n >= 0 => usize::try_from(n).ok(),
        Value::UInt(n) => usize::try_from(n).ok(),
        _ => None,
    }
    .ok_or_else(|| crate::mpw::proto("invalid projected scan coverage"))?;
    crate::budget::work(read as u64)?;
    let last = if rd.is_nil() {
        rd.skip()?;
        None
    } else {
        Some(projected(&mut rd, t, &project.boundary)?)
    };
    if !rd.buf.is_empty() || n > read || (read == 0) != last.is_none() || (read == 0 && next.is_some()) {
        return Err(crate::mpw::proto("inconsistent projected scan coverage"));
    }
    crate::observe::storage_rows(rows.len());
    Ok(ProjectedPage { rows, next, read, last })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::budget::{scope_sync, Limits, Pool};

    fn table() -> TableDef {
        use crate::catalog::Catalog;
        use crate::plan::Stmt;
        use crate::planner::{plan, PlanCtx};
        let catalog = Catalog::default();
        let cx = PlanCtx {
            statistics: None,
            catalog: &catalog,
            db: Some("d"),
            params: &[],
            conn_coll: "utf8mb4_bin",
            default_coll: "utf8mb4_bin",
        };
        let Stmt::CreateTable { def, .. } =
            plan("CREATE TABLE t (id INT PRIMARY KEY, v INT, payload VARCHAR(100))", &cx).unwrap()
        else {
            panic!()
        };
        *def
    }

    #[test]
    fn projected_rows_charge_logical_width_before_decoding() {
        let t = table();
        let project = Projection {
            cols: vec![0],
            boundary: vec![0],
            preds: vec![],
        };
        let mut m = Mp::new();
        m.arr(1)
            .arr(5)
            .str("ok")
            .arr(1)
            .arr(1)
            .uint(7)
            .nil()
            .uint(1)
            .arr(1)
            .uint(7);
        let data = m.bytes();
        let pool = Pool::new(Limits {
            query_bytes: 1024,
            ..Limits::default()
        })
        .unwrap();
        scope_sync(pool.query(), || {
            let page = decode_projected(&data, &t, &project).unwrap();
            assert_eq!(page.rows, vec![vec![Value::Int(7), Value::Null, Value::Null]]);
            assert_eq!(page.read, 1);
            assert!(pool.stats()[0] > data.len() as u64);
        });
        assert_eq!(pool.stats()[0], 0);
        // This header claims many rows but has no row payload. Admission must
        // fail before allocating or trying to decode the truncated rows.
        let mut oversized = Mp::new();
        oversized.arr(1).arr(5).str("ok").arr(1_000_000);
        scope_sync(pool.query(), || {
            let error = decode_projected(&oversized.bytes(), &t, &project).err().unwrap();
            assert!(crate::budget::is_exhausted(&error));
        });
        assert_eq!(pool.stats()[0], 0);
    }

    #[test]
    fn reduction_payload_is_charged_and_incomplete_stays_incomplete() {
        let pool = Pool::new(Limits {
            query_bytes: 1024,
            ..Limits::default()
        })
        .unwrap();
        let mut m = Mp::new();
        m.arr(1).arr(3).str("ok").arr(1).str(&"9".repeat(1024)).bool(true);
        scope_sync(pool.query(), || {
            let error = decode_reduced(&m.bytes(), 1).err().unwrap();
            assert!(crate::budget::is_exhausted(&error));
        });
        let mut m = Mp::new();
        m.arr(1).arr(3).str("ok").arr(0).bool(false);
        scope_sync(pool.query(), || {
            assert!(matches!(decode_reduced(&m.bytes(), 1).unwrap(), Reduced::Incomplete));
        });
        assert_eq!(pool.stats()[0], 0);
    }

    #[test]
    fn filtered_scan_coverage_still_consumes_work() {
        let t = table();
        let project = Projection {
            cols: vec![0],
            boundary: vec![0],
            preds: vec![],
        };
        let pool = Pool::new(Limits {
            query_work: 100,
            ..Limits::default()
        })
        .unwrap();
        let mut m = Mp::new();
        m.arr(1).arr(5).str("ok").arr(0).nil().uint(1000).arr(1).uint(1000);
        scope_sync(pool.query(), || {
            let error = decode_projected(&m.bytes(), &t, &project).err().unwrap();
            assert!(crate::budget::is_exhausted(&error));
        });
        assert_eq!(pool.stats()[0], 0);
    }
}
