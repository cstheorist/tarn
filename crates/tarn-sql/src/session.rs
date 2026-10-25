//! One client connection: statement dispatch, prepared statements, explicit
//! and implicit transactions, DML fast paths, SHOW/SET handling and DDL.

use crate::admin::{self, Admin};
use crate::catalog::TableDef;
use crate::cluster::{CachedPlan, Cluster};
use crate::exec::{self, matching_rows, run_select, shard_of_row, ReadMode, NEED_TXN};
use crate::expr::{now_string, EvalCtx, Expr};
use crate::mpw::{lua_status, Mp, Rd};
use crate::normalize::{normalize, Mode};
use crate::plan::*;
use crate::planner::{self, PlanCtx};
use crate::txn::{key_bytes, select_rows, BufWrite, Row, Txn, WOp};
use crate::types::Arith;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tarn_common::{Column, ColumnType, SqlError, SqlResult, Value};
use tarn_iproto::Iter;

#[derive(Debug)]
pub enum Output {
    Ok { affected: u64, last_insert_id: u64, info: String },
    Rows { columns: Arc<[Column]>, rows: Vec<Vec<Value>> },
}

impl Output {
    fn ok(affected: u64) -> Output {
        Output::Ok { affected, last_insert_id: 0, info: String::new() }
    }
}

pub struct Prepared {
    template: String,
    pub n_params: usize,
    pub columns: Arc<[Column]>,
    plan: Option<(u64, Stmt)>,
    admin: Option<Admin>,
}

pub struct Session {
    pub cluster: Arc<Cluster>,
    pub db: Option<String>,
    pub user: String,
    pub conn_id: u32,
    pub autocommit: bool,
    txn: Option<Txn>,
    pub last_insert_id: u64,
    found_rows: u64,
    row_count: i64,
    prepared: HashMap<u32, Prepared>,
}

const MAX_RETRIES: usize = 20;

/// First keyword of a statement, skipping whitespace and comments.
fn first_keyword(sql: &str) -> &str {
    let mut s = sql;
    loop {
        s = s.trim_start();
        if let Some(r) = s.strip_prefix("/*") {
            if r.starts_with('!') {
                // MySQL versioned comment: its content is executable SQL
                let r = r[1..].trim_start_matches(|c: char| c.is_ascii_digit());
                s = r;
                continue;
            }
            match r.find("*/") {
                Some(i) => s = &r[i + 2..],
                None => return "",
            }
        } else if s.starts_with("-- ") || s.starts_with('#') {
            match s.find('\n') {
                Some(i) => s = &s[i + 1..],
                None => return "",
            }
        } else {
            break;
        }
    }
    let end = s.find(|c: char| !c.is_ascii_alphabetic()).unwrap_or(s.len());
    &s[..end]
}

fn is_ddl(sql: &str) -> bool {
    let k = first_keyword(sql);
    ["CREATE", "ALTER", "DROP", "TRUNCATE", "RENAME"].iter().any(|d| k.eq_ignore_ascii_case(d))
}

fn cols(names: &[(&str, ColumnType)]) -> Arc<[Column]> {
    names.iter().map(|(n, t)| Column::new(n, *t)).collect::<Vec<_>>().into()
}

fn like_ci(s: &str, pat: &Option<String>) -> bool {
    match pat {
        None => true,
        Some(p) => crate::expr::like_match(s.to_ascii_lowercase().as_bytes(), p.to_ascii_lowercase().as_bytes()),
    }
}

impl Session {
    pub fn new(cluster: Arc<Cluster>, conn_id: u32) -> Session {
        Session {
            cluster,
            db: None,
            user: "root".into(),
            conn_id,
            autocommit: true,
            txn: None,
            last_insert_id: 0,
            found_rows: 0,
            row_count: -1,
            prepared: HashMap::new(),
        }
    }

    pub fn in_transaction(&self) -> bool {
        self.txn.as_ref().map_or(false, |t| t.explicit)
    }

    pub fn use_db(&mut self, db: &str) -> SqlResult<()> {
        if !self.cluster.catalog().has_db(db) {
            return Err(SqlError::new(1049, "42000", format!("Unknown database '{}'", db)));
        }
        self.db = Some(db.to_string());
        Ok(())
    }

    // ---- entry points -------------------------------------------------------

    pub async fn query(&mut self, sql: &str) -> SqlResult<Output> {
        self.cluster.stats.queries.fetch_add(1, Ordering::Relaxed);
        if let Some(a) = admin::recognize(sql)? {
            return self.run_admin(a).await;
        }
        if is_ddl(sql) {
            // Literals in DDL are syntax (VARCHAR(20), DEFAULT '0'), not values.
            let stmt = self.plan_cached(sql, &[])?;
            return self.run(&stmt, &[]).await;
        }
        let n = normalize(sql, Mode::Literals)?;
        let stmt = self.plan_cached(&n.template, &n.params)?;
        self.run(&stmt, &n.params).await
    }

    pub fn prepare(&mut self, id: u32, sql: &str) -> SqlResult<(usize, Arc<[Column]>)> {
        if let Some(a) = admin::recognize(sql)? {
            self.prepared.insert(id, Prepared { template: sql.into(), n_params: 0, columns: Arc::from(Vec::new()), plan: None, admin: Some(a) });
            return Ok((0, Arc::from(Vec::new())));
        }
        let n = normalize(sql, Mode::Placeholders)?;
        let stmt = self.plan_cached(&n.template, &[])?;
        let columns = match &stmt {
            Stmt::Select(p) => p.columns.clone(),
            _ => Arc::from(Vec::new()),
        };
        let version = self.cluster.catalog().version;
        self.prepared.insert(
            id,
            Prepared { template: n.template, n_params: n.n_params, columns: columns.clone(), plan: Some((version, stmt)), admin: None },
        );
        Ok((n.n_params, columns))
    }

    pub async fn execute(&mut self, id: u32, params: &[Value]) -> SqlResult<Output> {
        self.cluster.stats.queries.fetch_add(1, Ordering::Relaxed);
        let (admin, stmt) = {
            let p = self.prepared.get(&id).ok_or_else(|| SqlError::new(1243, "HY000", format!("Unknown prepared statement handler ({})", id)))?;
            if params.len() != p.n_params {
                return Err(SqlError::new(1210, "HY000", "Incorrect arguments to mysqld_stmt_execute"));
            }
            let version = self.cluster.catalog().version;
            let stmt = match &p.plan {
                Some((v, s)) if *v == version => Some(s.clone()),
                _ => None,
            };
            (p.admin.clone(), stmt)
        };
        if let Some(a) = admin {
            return self.run_admin(a).await;
        }
        let stmt = match stmt {
            Some(s) => s,
            None => {
                // catalog changed since PREPARE: re-plan
                let template = self.prepared[&id].template.clone();
                let s = self.plan_cached(&template, &[])?;
                let version = self.cluster.catalog().version;
                self.prepared.get_mut(&id).unwrap().plan = Some((version, s.clone()));
                s
            }
        };
        self.run(&stmt, params).await
    }

    pub fn close(&mut self, id: u32) {
        self.prepared.remove(&id);
    }

    pub fn disconnect(&mut self) {
        if let Some(t) = self.txn.take() {
            t.rollback();
        }
    }

    pub fn status_flags(&self) -> u16 {
        let mut f = 0;
        if self.in_transaction() {
            f |= 1;
        }
        if self.autocommit {
            f |= 2;
        }
        f
    }

    fn plan_cached(&self, template: &str, params: &[Value]) -> SqlResult<Stmt> {
        let db = self.db.clone().unwrap_or_default();
        if let Some(p) = self.cluster.cached_plan(&db, template) {
            return Ok(p.stmt.clone());
        }
        let catalog = self.cluster.catalog();
        let ctx = PlanCtx { catalog: &catalog, db: self.db.as_deref(), params };
        let stmt = planner::plan(template, &ctx)?;
        if matches!(stmt, Stmt::Select(_) | Stmt::Insert(_) | Stmt::Update(_) | Stmt::Delete(_)) {
            self.cluster.cache_plan(&db, template, Arc::new(CachedPlan { version: catalog.version, stmt: stmt.clone() }));
        }
        Ok(stmt)
    }

    // ---- dispatch -------------------------------------------------------------

    async fn run(&mut self, stmt: &Stmt, params: &[Value]) -> SqlResult<Output> {
        match stmt {
            Stmt::Select(p) => self.run_select(p, params).await,
            Stmt::Insert(_) | Stmt::Update(_) | Stmt::Delete(_) => self.run_dml(stmt, params).await,
            Stmt::Admin(a) => self.run_admin(a.clone()).await,
            ddl => {
                self.implicit_commit().await?;
                self.run_ddl(ddl).await
            }
        }
    }

    async fn implicit_commit(&mut self) -> SqlResult<()> {
        if let Some(t) = self.txn.take() {
            t.commit(&self.cluster).await?;
        }
        Ok(())
    }

    fn txn_needed(&self) -> bool {
        self.txn.is_some() || !self.autocommit
    }

    fn ensure_txn(&mut self) {
        if self.txn.is_none() {
            self.txn = Some(Txn::new(&self.cluster, true));
        }
    }

    // ---- SELECT ----------------------------------------------------------------

    async fn run_select(&mut self, p: &Arc<SelectPlan>, params: &[Value]) -> SqlResult<Output> {
        let now = now_string();
        let columns = exec::render_columns(&p.columns, params);
        if self.txn_needed() {
            self.ensure_txn();
            let mut txn = self.txn.take().unwrap();
            let res = {
                let cx = self.cx(params, &now);
                let mut mode = ReadMode::Txn(&mut txn);
                run_select(&self.cluster, &mut mode, p, &cx).await
            };
            return self.finish_txn_stmt(txn, res.map(|rows| Output::Rows { columns, rows }));
        }
        // Autocommit: one lock-aware storage read if the plan allows it,
        // otherwise an implicit read-only transaction (validated at the end).
        {
            let cx = self.cx(params, &now);
            let mut mode = ReadMode::Auto { calls: 0 };
            match run_select(&self.cluster, &mut mode, p, &cx).await {
                Ok(rows) => {
                    self.found_rows = rows.len() as u64;
                    return Ok(Output::Rows { columns, rows });
                }
                Err(e) if e.code == NEED_TXN => {}
                Err(e) => return Err(e),
            }
        }
        let mut attempt = 0;
        loop {
            let mut txn = Txn::new(&self.cluster, false);
            let rows = {
                let cx = self.cx(params, &now);
                let mut mode = ReadMode::Txn(&mut txn);
                run_select(&self.cluster, &mut mode, p, &cx).await
            };
            let res = match rows {
                Ok(rows) => txn.commit(&self.cluster).await.map(|_| rows),
                Err(e) => {
                    txn.rollback();
                    Err(e)
                }
            };
            match res {
                Ok(rows) => {
                    self.found_rows = rows.len() as u64;
                    return Ok(Output::Rows { columns, rows });
                }
                Err(e) if e.code == 1213 && attempt < MAX_RETRIES => {
                    attempt += 1;
                    self.cluster.stats.retries.fetch_add(1, Ordering::Relaxed);
                    backoff(attempt).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn cx<'a>(&'a self, params: &'a [Value], now: &'a str) -> EvalCtx<'a> {
        EvalCtx {
            params,
            db: self.db.as_deref(),
            user: &self.user,
            conn_id: self.conn_id,
            last_insert_id: self.last_insert_id,
            found_rows: self.found_rows,
            row_count: self.row_count,
            now,
        }
    }

    /// After a statement inside an explicit/implicit-long transaction:
    /// a conflict ends the transaction (as a MySQL deadlock does).
    fn finish_txn_stmt(&mut self, txn: Txn, res: SqlResult<Output>) -> SqlResult<Output> {
        match res {
            Err(e) if e.code == 1213 || txn.doomed.is_some() => {
                txn.rollback();
                Err(e)
            }
            other => {
                self.txn = Some(txn);
                other
            }
        }
    }

    // ---- DML -------------------------------------------------------------------

    async fn run_dml(&mut self, stmt: &Stmt, params: &[Value]) -> SqlResult<Output> {
        let now = now_string();
        if self.txn_needed() {
            self.ensure_txn();
            let mut txn = self.txn.take().unwrap();
            let snapshot = txn.writes.clone();
            let res = self.dml_in_txn(&mut txn, stmt, params, &now).await;
            if res.is_err() {
                txn.writes = snapshot; // statement atomicity
            }
            let res = res.map(|(n, id)| self.dml_output(n, id));
            return self.finish_txn_stmt(txn, res);
        }
        if let Some(out) = self.dml_fast_path(stmt, params, &now).await? {
            return Ok(out);
        }
        let mut attempt = 0;
        loop {
            let mut txn = Txn::new(&self.cluster, false);
            let res = match self.dml_in_txn(&mut txn, stmt, params, &now).await {
                Ok(r) => txn.commit(&self.cluster).await.map(|_| r),
                Err(e) => {
                    txn.rollback();
                    Err(e)
                }
            };
            match res {
                Ok((n, id)) => return Ok(self.dml_output(n, id)),
                Err(e) if e.code == 1213 && attempt < MAX_RETRIES => {
                    attempt += 1;
                    self.cluster.stats.retries.fetch_add(1, Ordering::Relaxed);
                    backoff(attempt).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn dml_output(&mut self, affected: u64, insert_id: Option<u64>) -> Output {
        if let Some(id) = insert_id {
            self.last_insert_id = id;
        }
        self.row_count = affected as i64;
        Output::Ok { affected, last_insert_id: insert_id.unwrap_or(0), info: String::new() }
    }

    /// Single-shard autocommit DML that needs no read round trip: one call to
    /// `tarn_autocommit` (one local transaction on the owning shard).
    async fn dml_fast_path(&mut self, stmt: &Stmt, params: &[Value], now: &str) -> SqlResult<Option<Output>> {
        let cx = self.cx(params, now);
        let (shard, writes, insert_id) = match stmt {
            Stmt::Insert(p) if p.on_dup.is_empty() && !p.ignore => {
                let InsertSource::Values(_) = &p.source else { return Ok(None) };
                let (rows, id) = self.insert_rows(p, &cx, None).await?;
                let shard = shard_of_row(&self.cluster, &p.table, &rows[0]);
                if rows.iter().any(|r| shard_of_row(&self.cluster, &p.table, r) != shard) {
                    return Ok(None);
                }
                let mut m = Mp::new();
                m.arr(1).arr(rows.len());
                for r in &rows {
                    m.arr(3).str(if p.replace { "replace" } else { "insert" }).uint(p.table.space_id as u64);
                    self.encode_tuple(&p.table, r, &mut m);
                }
                (shard, m, id)
            }
            Stmt::Update(p) => {
                let Some((pk, ops)) = self.simple_update(p, &cx)? else { return Ok(None) };
                let idx = &p.access.table.indexes[0];
                let shard = exec::route_prefix(&self.cluster, &p.access.table, idx, &pk).unwrap_or(0);
                let mut m = Mp::new();
                m.arr(1).arr(1).arr(4).str("update").uint(p.access.table.space_id as u64).vals(&pk);
                m.arr(ops.len());
                for (op, field, v) in &ops {
                    m.arr(3).str(op).str(field).val(v);
                }
                (shard, m, None)
            }
            Stmt::Delete(p) => {
                let Some(pk) = self.simple_pk_target(&p.access, &p.filter, &p.order, &cx)? else { return Ok(None) };
                let idx = &p.access.table.indexes[0];
                let shard = exec::route_prefix(&self.cluster, &p.access.table, idx, &pk).unwrap_or(0);
                let mut m = Mp::new();
                m.arr(1).arr(1).arr(3).str("delete").uint(p.access.table.space_id as u64).vals(&pk);
                (shard, m, None)
            }
            _ => return Ok(None),
        };
        let mut attempt = 0;
        loop {
            let d = self.cluster.call(shard, "tarn_autocommit", &writes.0).await?;
            match lua_status(&d) {
                Ok((_, mut rd)) => {
                    let n = rd.value()?.as_i64().unwrap_or(0) as u64;
                    return Ok(Some(self.dml_output(n, insert_id)));
                }
                Err(e) if e.code == 1213 && attempt < MAX_RETRIES => {
                    attempt += 1;
                    self.cluster.stats.retries.fetch_add(1, Ordering::Relaxed);
                    backoff(attempt).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// The primary key targeted by an UPDATE/DELETE whose WHERE clause is
    /// exactly an equality on every primary-key column.
    fn simple_pk_target(&self, access: &TableAccess, filter: &Option<Expr>, order: &[(Expr, bool)], cx: &EvalCtx) -> SqlResult<Option<Vec<Value>>> {
        let AccessPath::Point { index: 0, keys } = &access.path else { return Ok(None) };
        if keys.len() != 1 || !order.is_empty() {
            return Ok(None);
        }
        let t = &access.table;
        let pk_cols = &t.indexes[0].cols;
        let mut conj = Vec::new();
        if let Some(f) = filter {
            split(f, &mut conj);
        }
        if conj.len() != pk_cols.len() {
            return Ok(None);
        }
        for c in &conj {
            match c {
                Expr::Cmp(a, crate::expr::CmpOp::Eq, b) => {
                    let col = match (&**a, &**b) {
                        (Expr::Col(x), o) | (o, Expr::Col(x)) if o.is_constant() => *x,
                        _ => return Ok(None),
                    };
                    if !pk_cols.contains(&col) {
                        return Ok(None);
                    }
                }
                _ => return Ok(None),
            }
        }
        let mut key = Vec::new();
        for (i, e) in keys[0].iter().enumerate() {
            let v = e.eval(&[], cx)?;
            match exec::coerce_key_part(t.columns[pk_cols[i]].ty(), &v) {
                Some(x) if !x.is_null() => key.push(x),
                _ => return Ok(None),
            }
        }
        Ok(Some(key))
    }

    /// UPDATE t SET c = const | c = c +/- const WHERE pk = ...: Tarantool
    /// update ops applied in one local transaction, no read round trip.
    fn simple_update(&self, p: &UpdatePlan, cx: &EvalCtx) -> SqlResult<Option<(Vec<Value>, Vec<(&'static str, String, Value)>)>> {
        let t = &p.access.table;
        if p.limit.is_some() {
            return Ok(None);
        }
        let Some(pk) = self.simple_pk_target(&p.access, &p.filter, &p.order, cx)? else { return Ok(None) };
        let mut ops = Vec::new();
        for (c, e) in &p.set {
            if t.is_pk_col(*c) || t.shard_key.contains(c) {
                return Ok(None);
            }
            let col = &t.columns[*c];
            let ty = col.ty();
            match e {
                Expr::Arith(a, op @ (Arith::Add | Arith::Sub), b) if matches!(&**a, Expr::Col(x) if x == c) && b.is_constant() && ty.is_integer() => {
                    let d = b.eval(&[], cx)?;
                    let Some(d) = d.as_i64() else { return Ok(None) };
                    let d = if *op == Arith::Sub { -d } else { d };
                    ops.push(("+", col.name.clone(), Value::Int(d)));
                }
                e if e.is_constant() => {
                    let v = ty.coerce(e.eval(&[], cx)?, &col.name)?;
                    if v.is_null() && !col.nullable {
                        return Err(SqlError::new(1048, "23000", format!("Column '{}' cannot be null", col.name)));
                    }
                    ops.push(("=", col.name.clone(), v));
                }
                _ => return Ok(None),
            }
        }
        Ok(Some((pk, ops)))
    }

    fn encode_tuple(&self, t: &TableDef, r: &Row, m: &mut Mp) {
        m.arr(r.len() + 1);
        for v in r {
            m.val(v);
        }
        let sk: Vec<Value> = t.shard_key.iter().map(|&c| r[c].clone()).collect();
        m.uint(self.cluster.bucket_of(&sk) as u64);
    }

    /// Materialize INSERT rows: defaults, AUTO_INCREMENT, type coercion.
    async fn insert_rows(&self, p: &InsertPlan, cx: &EvalCtx<'_>, from_select: Option<Vec<Row>>) -> SqlResult<(Vec<Row>, Option<u64>)> {
        let t = &p.table;
        let ncols = t.columns.len();
        let mut raw: Vec<Vec<Option<Value>>> = Vec::new();
        match (&p.source, from_select) {
            (InsertSource::Values(rows), _) => {
                for r in rows {
                    let mut out = Vec::with_capacity(ncols);
                    for e in r {
                        out.push(match e {
                            Some(e) => Some(e.eval(&[], cx)?),
                            None => None,
                        });
                    }
                    raw.push(out);
                }
            }
            (InsertSource::Select(_, targets), Some(rows)) => {
                for r in rows {
                    let mut out = vec![None; ncols];
                    for (i, &c) in targets.iter().enumerate() {
                        out[c] = Some(r[i].clone());
                    }
                    raw.push(out);
                }
            }
            _ => return Err(SqlError::internal("INSERT ... SELECT without rows")),
        }
        // AUTO_INCREMENT: missing, NULL or 0 get generated values
        let mut first_id = None;
        if let Some(ai) = t.auto_inc {
            let need = raw
                .iter()
                .filter(|r| match &r[ai] {
                    None | Some(Value::Null) => true,
                    Some(v) => v.as_i64() == Some(0),
                })
                .count() as u64;
            let mut next = if need > 0 { self.cluster.next_auto_inc(t, need).await? } else { 0 };
            let mut max_explicit = 0u64;
            for r in raw.iter_mut() {
                let gen = match &r[ai] {
                    None | Some(Value::Null) => true,
                    Some(v) => v.as_i64() == Some(0),
                };
                if gen {
                    first_id.get_or_insert(next);
                    r[ai] = Some(Value::UInt(next));
                    next += 1;
                } else if let Some(v) = r[ai].as_ref().and_then(|v| v.as_i64()) {
                    max_explicit = max_explicit.max(v.max(0) as u64);
                }
            }
            if max_explicit > 0 {
                self.cluster.note_auto_inc(t, max_explicit).await?;
            }
        }
        let mut rows = Vec::with_capacity(raw.len());
        for r in raw {
            let mut row = Vec::with_capacity(ncols);
            for (c, v) in r.into_iter().enumerate() {
                let col = &t.columns[c];
                let v = match v {
                    Some(v) => v,
                    None => t.default_value(c).ok_or_else(|| {
                        SqlError::new(1364, "HY000", format!("Field '{}' doesn't have a default value", col.name))
                    })?,
                };
                let v = col.ty().coerce(v, &col.name)?;
                if v.is_null() && !col.nullable {
                    return Err(SqlError::new(1048, "23000", format!("Column '{}' cannot be null", col.name)));
                }
                row.push(v);
            }
            rows.push(row);
        }
        Ok((rows, first_id))
    }

    async fn row_exists(&self, txn: &mut Txn, t: &Arc<TableDef>, pk: &[Value], shard: usize) -> SqlResult<Option<Row>> {
        if let Some(b) = txn.buffered(t.space_id, pk) {
            return Ok(b.cloned());
        }
        let (client, sid) = txn.stream(&self.cluster, shard).await?;
        let rows = select_rows(&client, sid, t, 0, pk, Iter::Eq, Some(1), None, false).await?;
        txn.note_point(shard, t.space_id, 0, pk.to_vec());
        Ok(rows.into_iter().next())
    }

    async fn dml_in_txn(&self, txn: &mut Txn, stmt: &Stmt, params: &[Value], now: &str) -> SqlResult<(u64, Option<u64>)> {
        let cx = self.cx(params, now);
        match stmt {
            Stmt::Insert(p) => {
                let t = &p.table;
                let selected = match &p.source {
                    InsertSource::Select(sp, _) => {
                        let mut mode = ReadMode::Txn(txn);
                        Some(run_select(&self.cluster, &mut mode, sp, &cx).await?)
                    }
                    _ => None,
                };
                let (rows, id) = self.insert_rows(p, &cx, selected).await?;
                let pk_cols = &t.indexes[0].cols;
                let mut affected = 0;
                for row in rows {
                    let pk: Vec<Value> = pk_cols.iter().map(|&c| row[c].clone()).collect();
                    let shard = shard_of_row(&self.cluster, t, &row);
                    let existing = self.row_exists(txn, t, &pk, shard).await?;
                    match existing {
                        Some(old) if !p.on_dup.is_empty() => {
                            // ON DUPLICATE KEY UPDATE: expressions see old row ++ proposed row
                            let mut both = old.clone();
                            both.extend(row.iter().cloned());
                            let mut new = old.clone();
                            for (c, e) in &p.on_dup {
                                let col = &t.columns[*c];
                                new[*c] = col.ty().coerce(e.eval(&both, &cx)?, &col.name)?;
                                both[*c] = new[*c].clone();
                            }
                            if new != old {
                                self.buffer_row_change(txn, t, &old, new, shard).await?;
                                affected += 2;
                            }
                        }
                        Some(_) if p.replace => {
                            txn.buffer(BufWrite { shard, table: t.clone(), op: WOp::Replace, row: Some(row), pk });
                            affected += 2;
                        }
                        Some(_) if p.ignore => {}
                        Some(_) => {
                            let shown: Vec<String> = pk.iter().map(|v| v.to_string()).collect();
                            return Err(SqlError::dup_key(&shown.join("-"), "PRIMARY"));
                        }
                        None => {
                            txn.buffer(BufWrite { shard, table: t.clone(), op: WOp::Insert, row: Some(row), pk });
                            affected += 1;
                        }
                    }
                }
                Ok((affected, id))
            }
            Stmt::Update(p) => {
                let t = &p.access.table;
                let rows = matching_rows(&self.cluster, txn, &p.access, &p.filter, &p.order, &p.limit, &cx).await?;
                let mut changed = 0;
                for old in rows {
                    let mut new = old.clone();
                    for (c, e) in &p.set {
                        let col = &t.columns[*c];
                        let v = col.ty().coerce(e.eval(&new, &cx)?, &col.name)?;
                        if v.is_null() && !col.nullable {
                            return Err(SqlError::new(1048, "23000", format!("Column '{}' cannot be null", col.name)));
                        }
                        new[*c] = v;
                    }
                    if new != old {
                        let shard = shard_of_row(&self.cluster, t, &old);
                        self.buffer_row_change(txn, t, &old, new, shard).await?;
                        changed += 1;
                    }
                }
                Ok((changed, None))
            }
            Stmt::Delete(p) => {
                let t = &p.access.table;
                let rows = matching_rows(&self.cluster, txn, &p.access, &p.filter, &p.order, &p.limit, &cx).await?;
                let n = rows.len() as u64;
                for old in rows {
                    let pk: Vec<Value> = t.indexes[0].cols.iter().map(|&c| old[c].clone()).collect();
                    let shard = shard_of_row(&self.cluster, t, &old);
                    txn.buffer(BufWrite { shard, table: t.clone(), op: WOp::Delete, row: None, pk });
                }
                Ok((n, None))
            }
            _ => Err(SqlError::internal("not a DML statement")),
        }
    }

    async fn buffer_row_change(&self, txn: &mut Txn, t: &Arc<TableDef>, old: &Row, new: Row, shard: usize) -> SqlResult<()> {
        let pk_cols = &t.indexes[0].cols;
        let old_pk: Vec<Value> = pk_cols.iter().map(|&c| old[c].clone()).collect();
        let new_pk: Vec<Value> = pk_cols.iter().map(|&c| new[c].clone()).collect();
        if key_bytes(&old_pk) == key_bytes(&new_pk) {
            txn.buffer(BufWrite { shard, table: t.clone(), op: WOp::Replace, row: Some(new), pk: new_pk });
            return Ok(());
        }
        // primary key (and possibly shard) changes: delete + insert
        let new_shard = shard_of_row(&self.cluster, t, &new);
        if self.row_exists(txn, t, &new_pk, new_shard).await?.is_some() {
            let shown: Vec<String> = new_pk.iter().map(|v| v.to_string()).collect();
            return Err(SqlError::dup_key(&shown.join("-"), "PRIMARY"));
        }
        txn.buffer(BufWrite { shard, table: t.clone(), op: WOp::Delete, row: None, pk: old_pk });
        txn.buffer(BufWrite { shard: new_shard, table: t.clone(), op: WOp::Insert, row: Some(new), pk: new_pk });
        Ok(())
    }

    // ---- DDL ---------------------------------------------------------------------

    async fn run_ddl(&mut self, stmt: &Stmt) -> SqlResult<Output> {
        let c = self.cluster.clone();
        match stmt {
            Stmt::CreateTable { def, if_not_exists } => c.create_table(def.clone(), *if_not_exists).await?,
            Stmt::CreateIndex { db, table, name, cols, unique, if_not_exists } => {
                c.create_index(db, table, name, cols, *unique, *if_not_exists).await?
            }
            Stmt::DropTable { tables, if_exists } => c.drop_tables(tables, *if_exists).await?,
            Stmt::Truncate { db, table } => c.truncate(db, table).await?,
            Stmt::CreateDatabase { name, if_not_exists } => c.create_database(name, *if_not_exists).await?,
            Stmt::DropDatabase { name, if_exists } => {
                let n = c.drop_database(name, *if_exists).await?;
                if self.db.as_deref().map_or(false, |d| d.eq_ignore_ascii_case(name)) {
                    self.db = None;
                }
                return Ok(Output::ok(n));
            }
            _ => return Err(SqlError::internal("not DDL")),
        }
        Ok(Output::ok(0))
    }

    // ---- session statements --------------------------------------------------

    async fn run_admin(&mut self, a: Admin) -> SqlResult<Output> {
        let catalog = self.cluster.catalog();
        let rows_out = |columns: Arc<[Column]>, rows: Vec<Vec<Value>>| Ok(Output::Rows { columns, rows });
        match a {
            Admin::Begin { .. } => {
                self.implicit_commit().await?;
                self.txn = Some(Txn::new(&self.cluster, true));
                Ok(Output::ok(0))
            }
            Admin::Commit => {
                if let Some(t) = self.txn.take() {
                    t.commit(&self.cluster).await?;
                }
                Ok(Output::ok(0))
            }
            Admin::Rollback => {
                if let Some(t) = self.txn.take() {
                    t.rollback();
                }
                Ok(Output::ok(0))
            }
            Admin::SetAutocommit(on) => {
                if on && !self.autocommit {
                    self.implicit_commit().await?;
                }
                self.autocommit = on;
                Ok(Output::ok(0))
            }
            Admin::SetIgnored | Admin::SetVar { .. } | Admin::Noop => Ok(Output::ok(0)),
            Admin::Use(db) => {
                self.use_db(&db)?;
                Ok(Output::ok(0))
            }
            Admin::SelectVars(vars) => {
                let mut columns = Vec::new();
                let mut row = Vec::new();
                for (alias, var) in &vars {
                    let v = admin::system_var(var, self.autocommit, self.db.as_deref()).unwrap_or(Value::Null);
                    let ty = match &v {
                        Value::Int(_) | Value::UInt(_) => ColumnType::LongLong,
                        _ => ColumnType::VarString,
                    };
                    columns.push(Column::new(alias, ty));
                    row.push(v);
                }
                rows_out(columns.into(), vec![row])
            }
            Admin::ShowDatabases { like } => {
                let mut rows: Vec<Vec<Value>> = vec![vec![Value::str("information_schema")]];
                for d in &catalog.databases {
                    if like_ci(d, &like) {
                        rows.push(vec![Value::str(d)]);
                    }
                }
                rows_out(cols(&[("Database", ColumnType::VarString)]), rows)
            }
            Admin::ShowTables { db, full, like } => {
                let db = self.resolve_db(db)?;
                let name = format!("Tables_in_{}", db);
                let mut rows = Vec::new();
                for t in catalog.tables_in(&db) {
                    if like_ci(&t.name, &like) {
                        let mut r = vec![Value::str(&t.name)];
                        if full {
                            r.push(Value::str("BASE TABLE"));
                        }
                        rows.push(r);
                    }
                }
                let c = if full {
                    cols(&[(&name, ColumnType::VarString), ("Table_type", ColumnType::VarString)])
                } else {
                    cols(&[(&name, ColumnType::VarString)])
                };
                rows_out(c, rows)
            }
            Admin::ShowColumns { db, table, .. } | Admin::Describe { db, table } => {
                let db = self.resolve_db(db)?;
                let t = catalog.get(&db, &table).ok_or_else(|| SqlError::no_table(&db, &table))?;
                let mut rows = Vec::new();
                for (i, c) in t.columns.iter().enumerate() {
                    let key = if t.is_pk_col(i) {
                        "PRI"
                    } else if t.indexes[1..].iter().any(|ix| ix.cols.first() == Some(&i) && ix.unique) {
                        "UNI"
                    } else if t.indexes[1..].iter().any(|ix| ix.cols.first() == Some(&i)) {
                        "MUL"
                    } else {
                        ""
                    };
                    rows.push(vec![
                        Value::str(&c.name),
                        Value::str(&c.ty().display()),
                        Value::str(if c.nullable { "YES" } else { "NO" }),
                        Value::str(key),
                        c.default.as_deref().map(Value::str).unwrap_or(Value::Null),
                        Value::str(if c.auto_increment { "auto_increment" } else { "" }),
                    ]);
                }
                rows_out(
                    cols(&[
                        ("Field", ColumnType::VarString),
                        ("Type", ColumnType::VarString),
                        ("Null", ColumnType::VarString),
                        ("Key", ColumnType::VarString),
                        ("Default", ColumnType::VarString),
                        ("Extra", ColumnType::VarString),
                    ]),
                    rows,
                )
            }
            Admin::ShowCreateTable { db, table } => {
                let db = self.resolve_db(db)?;
                let t = catalog.get(&db, &table).ok_or_else(|| SqlError::no_table(&db, &table))?;
                rows_out(
                    cols(&[("Table", ColumnType::VarString), ("Create Table", ColumnType::VarString)]),
                    vec![vec![Value::str(&t.name), Value::str(&show_create(&t))]],
                )
            }
            Admin::ShowIndex { db, table } => {
                let db = self.resolve_db(db)?;
                let t = catalog.get(&db, &table).ok_or_else(|| SqlError::no_table(&db, &table))?;
                let mut rows = Vec::new();
                for ix in &t.indexes {
                    for (seq, &c) in ix.cols.iter().enumerate() {
                        rows.push(vec![
                            Value::str(&t.name),
                            Value::Int(!ix.unique as i64),
                            Value::str(&ix.name),
                            Value::Int(seq as i64 + 1),
                            Value::str(&t.columns[c].name),
                            Value::str("A"),
                            Value::Null,
                            Value::Null,
                            Value::Null,
                            Value::str(if t.columns[c].nullable { "YES" } else { "" }),
                            Value::str("TREE"),
                            Value::str(""),
                            Value::str(""),
                            Value::str("YES"),
                            Value::Null,
                        ]);
                    }
                }
                let names = [
                    "Table", "Non_unique", "Key_name", "Seq_in_index", "Column_name", "Collation", "Cardinality", "Sub_part",
                    "Packed", "Null", "Index_type", "Comment", "Index_comment", "Visible", "Expression",
                ];
                let c: Vec<(&str, ColumnType)> = names.iter().map(|n| (*n, ColumnType::VarString)).collect();
                rows_out(cols(&c), rows)
            }
            Admin::ShowVariables { like } => {
                let mut rows = Vec::new();
                for name in admin::SHOWN_VARIABLES {
                    if like_ci(name, &like) {
                        let v = admin::system_var(name, self.autocommit, self.db.as_deref()).unwrap_or(Value::Null);
                        rows.push(vec![Value::str(name), Value::Str(v.to_text().unwrap_or_default())]);
                    }
                }
                rows_out(cols(&[("Variable_name", ColumnType::VarString), ("Value", ColumnType::VarString)]), rows)
            }
            Admin::ShowStatus { like } => Ok(self.show_status(&like)),
            Admin::ShowTarnStatus => Ok(self.show_status(&None)),
            Admin::ShowWarnings => rows_out(
                cols(&[("Level", ColumnType::VarString), ("Code", ColumnType::Long), ("Message", ColumnType::VarString)]),
                vec![],
            ),
            Admin::ShowEngines => rows_out(
                cols(&[
                    ("Engine", ColumnType::VarString),
                    ("Support", ColumnType::VarString),
                    ("Comment", ColumnType::VarString),
                    ("Transactions", ColumnType::VarString),
                    ("XA", ColumnType::VarString),
                    ("Savepoints", ColumnType::VarString),
                ]),
                vec![vec![
                    Value::str("memtx"),
                    Value::str("DEFAULT"),
                    Value::str("Tarantool in-memory engine, sharded, serializable"),
                    Value::str("YES"),
                    Value::str("NO"),
                    Value::str("NO"),
                ]],
            ),
            Admin::ShowCollation => rows_out(
                cols(&[("Collation", ColumnType::VarString), ("Charset", ColumnType::VarString), ("Id", ColumnType::LongLong), ("Default", ColumnType::VarString), ("Compiled", ColumnType::VarString), ("Sortlen", ColumnType::LongLong)]),
                vec![vec![Value::str("utf8mb4_bin"), Value::str("utf8mb4"), Value::Int(46), Value::str("Yes"), Value::str("Yes"), Value::Int(1)]],
            ),
            Admin::ShowCharset => rows_out(
                cols(&[("Charset", ColumnType::VarString), ("Description", ColumnType::VarString), ("Default collation", ColumnType::VarString), ("Maxlen", ColumnType::LongLong)]),
                vec![vec![Value::str("utf8mb4"), Value::str("UTF-8 Unicode"), Value::str("utf8mb4_bin"), Value::Int(4)]],
            ),
            Admin::ShowProcesslist => rows_out(
                cols(&[("Id", ColumnType::LongLong), ("User", ColumnType::VarString), ("Host", ColumnType::VarString), ("db", ColumnType::VarString), ("Command", ColumnType::VarString), ("Time", ColumnType::LongLong), ("State", ColumnType::VarString), ("Info", ColumnType::VarString)]),
                vec![vec![Value::UInt(self.conn_id as u64), Value::str(&self.user), Value::str("client"), self.db.as_deref().map(Value::str).unwrap_or(Value::Null), Value::str("Query"), Value::Int(0), Value::str(""), Value::str("SHOW PROCESSLIST")]],
            ),
            Admin::ShowTableStatus { db, like } => {
                let db = self.resolve_db(db)?;
                let mut rows = Vec::new();
                for t in catalog.tables_in(&db) {
                    if like_ci(&t.name, &like) {
                        rows.push(vec![Value::str(&t.name), Value::str("memtx"), Value::Int(10), Value::str("Fixed"), Value::Null, Value::Null]);
                    }
                }
                rows_out(
                    cols(&[("Name", ColumnType::VarString), ("Engine", ColumnType::VarString), ("Version", ColumnType::LongLong), ("Row_format", ColumnType::VarString), ("Rows", ColumnType::LongLong), ("Comment", ColumnType::VarString)]),
                    rows,
                )
            }
        }
    }

    fn show_status(&self, like: &Option<String>) -> Output {
        let s = &self.cluster.stats;
        let mut stats = vec![
            ("Tarn_queries", s.queries.load(Ordering::Relaxed)),
            ("Tarn_commits_local", s.commits_local.load(Ordering::Relaxed)),
            ("Tarn_commits_2pc", s.commits_2pc.load(Ordering::Relaxed)),
            ("Tarn_commits_read_only", s.commits_ro.load(Ordering::Relaxed)),
            ("Tarn_conflicts", s.conflicts.load(Ordering::Relaxed)),
            ("Tarn_retries", s.retries.load(Ordering::Relaxed)),
            ("Tarn_plan_cache_hits", s.plan_cache_hits.load(Ordering::Relaxed)),
            ("Tarn_plan_cache_misses", s.plan_cache_misses.load(Ordering::Relaxed)),
            ("Tarn_shards", self.cluster.nshards() as u64),
        ];
        stats.retain(|(n, _)| like_ci(n, like));
        let rows = stats.into_iter().map(|(n, v)| vec![Value::str(n), Value::str(&v.to_string())]).collect();
        Output::Rows { columns: cols(&[("Variable_name", ColumnType::VarString), ("Value", ColumnType::VarString)]), rows }
    }

    fn resolve_db(&self, db: Option<String>) -> SqlResult<String> {
        db.or_else(|| self.db.clone()).ok_or_else(|| SqlError::new(1046, "3D000", "No database selected"))
    }
}

fn split(e: &Expr, out: &mut Vec<Expr>) {
    match e {
        Expr::And(a, b) => {
            split(a, out);
            split(b, out);
        }
        other => out.push(other.clone()),
    }
}

async fn backoff(attempt: usize) {
    let base = 200u64 << attempt.min(6);
    let jitter = (std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.subsec_nanos()).unwrap_or(0) as u64) % base.max(1);
    tokio::time::sleep(std::time::Duration::from_micros(base + jitter)).await;
}

fn show_create(t: &TableDef) -> String {
    let mut s = format!("CREATE TABLE `{}` (\n", t.name);
    let mut lines = Vec::new();
    for c in &t.columns {
        let mut l = format!("  `{}` {}", c.name, c.ty().display());
        if !c.nullable {
            l.push_str(" NOT NULL");
        }
        if let Some(d) = &c.default {
            if d == "CURRENT_TIMESTAMP" {
                l.push_str(" DEFAULT CURRENT_TIMESTAMP");
            } else {
                l.push_str(&format!(" DEFAULT '{}'", d.replace('\'', "''")));
            }
        } else if c.nullable {
            l.push_str(" DEFAULT NULL");
        }
        if c.auto_increment {
            l.push_str(" AUTO_INCREMENT");
        }
        lines.push(l);
    }
    for ix in &t.indexes {
        let colnames: Vec<String> = ix.cols.iter().map(|&c| format!("`{}`", t.columns[c].name)).collect();
        if ix.id == 0 {
            lines.push(format!("  PRIMARY KEY ({})", colnames.join(",")));
        } else {
            lines.push(format!("  {}KEY `{}` ({})", if ix.unique { "UNIQUE " } else { "" }, ix.name, colnames.join(",")));
        }
    }
    s.push_str(&lines.join(",\n"));
    let sk: Vec<String> = t.shard_key.iter().map(|&c| format!("`{}`", t.columns[c].name)).collect();
    s.push_str(&format!("\n) ENGINE=memtx DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n/*!50100 PARTITION BY KEY ({}) */", sk.join(",")));
    s
}

/// Unused helper kept for API symmetry with Rd-based decoders.
#[allow(dead_code)]
fn first_value(d: &[u8]) -> SqlResult<Value> {
    let mut rd = Rd::new(d);
    rd.array()?;
    rd.value()
}
