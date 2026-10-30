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
) -> SqlResult<(Option<Bytes>, Option<tarn_common::ResourceGuard>)> {
    let (reply, wire) = crate::budget::call_read(client, stream, name, &m.0).await?;
    let data = match reply {
        Ok(r) => Some(r.data().ok_or_else(|| crate::mpw::proto("no pushdown data"))?),
        Err(tarn_iproto::Error::Server { code, .. }) if code == tarn_iproto::errcode::ER_NO_SUCH_PROC => None,
        Err(e) => return Err(from_iproto_read(e)),
    };
    Ok((data, wire))
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
    let (data, _wire) = optional(client, stream, "tarn_reduce", &m).await?;
    let Some(d) = data else {
        return Ok(Reduced::Unavailable);
    };
    let (status, rd) = lua_status(&d)?;
    let mut head = Rd::new(&d);
    if status != "ok" || head.array()? != 1 || head.array()? != 3 {
        return Err(crate::mpw::proto("invalid reduction envelope"));
    }
    let mut rd = Rd::shared_at(&d, rd);
    let n = rd.array()?;
    if n != 0 && n != aggs.len() {
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
    if !rd.buf.is_empty() || (complete && n != aggs.len()) || (!complete && n != 0) {
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
    let (data, _wire) = optional(client, stream, "tarn_select_projected", &m).await?;
    let Some(d) = data else {
        return Ok(None);
    };
    let (status, rd) = lua_status(&d)?;
    let mut head = Rd::new(&d);
    if status != "ok" || head.array()? != 1 || head.array()? != 5 {
        return Err(crate::mpw::proto("invalid projected envelope"));
    }
    let mut rd = Rd::shared_at(&d, rd);
    let n = rd.array()?;
    if n > rd.buf.len() {
        return Err(crate::mpw::proto("truncated projected rows"));
    }
    crate::budget::decoded(d.len(), n, t.columns.len())?;
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
    let last = if rd.is_nil() {
        rd.skip()?;
        None
    } else {
        crate::budget::decoded(0, 1, t.columns.len())?;
        Some(projected(&mut rd, t, &project.boundary)?)
    };
    if !rd.buf.is_empty() || n > read || (read == 0) != last.is_none() || (read == 0 && next.is_some()) {
        return Err(crate::mpw::proto("inconsistent projected scan coverage"));
    }
    Ok(Some(ProjectedPage { rows, next, read, last }))
}
