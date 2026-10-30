//! Compile-time routing diagnostics. Uses the executor's key coercion,
//! bucket routing and colocation guards; never fetches rows, allocates IDs,
//! or starts a transaction. Unknown values remain unknown.

use super::*;
use serde_json::{json, Value as Json};

const MAX_KEYS: usize = 256;
const MAX_ACCESSES: usize = 64;

fn static_expr(e: &Expr, params: &[Value]) -> bool {
    match e {
        Expr::Lit(_) => true,
        Expr::Param(i) => *i < params.len(),
        Expr::Key(e, _) | Expr::Collate(e, _) | Expr::Cast(e, _) | Expr::Neg(e) => static_expr(e, params),
        _ => false,
    }
}

fn bound_path(path: &AccessPath, params: &[Value]) -> bool {
    match path {
        AccessPath::Point { keys, .. } | AccessPath::Prefixes { keys, .. } => {
            keys.iter().flatten().all(|e| static_expr(e, params))
        }
        AccessPath::Range { eq, .. } => eq.iter().all(|e| static_expr(e, params)),
        AccessPath::Full => true,
        AccessPath::Union(paths) => paths.iter().all(|p| bound_path(p, params)),
    }
}

struct Context<'a, 'c> {
    cluster: &'a Cluster,
    map: Arc<crate::buckets::BucketMap>,
    live: Vec<u32>,
    cx: &'a EvalCtx<'c>,
    committed: bool,
    buffered: &'a BTreeSet<u32>,
}

impl Context<'_, '_> {
    fn unknown(&self, reason: &str) -> Json {
        json!({"status":"unknown", "reason":reason, "shard_ids":null,
            "shard_count":null, "bucket_count":null, "max_shards":self.live.len()})
    }

    fn broadcast(&self) -> Json {
        json!({"status":"broadcast", "reason":"access prefix does not determine the complete shard key",
            "shard_ids":self.live, "shard_count":self.live.len(), "bucket_count":self.map.count,
            "max_shards":self.live.len()})
    }

    fn keys(&self, t: &TableDef, index: &IndexDef, keys: &[Vec<Value>]) -> Json {
        let mut shards = BTreeSet::new();
        let mut buckets = BTreeSet::new();
        for key in keys {
            let Some((position, bucket)) = route_bucket_in(&self.map, t, index, key) else {
                return self.broadcast();
            };
            if position >= self.cluster.shards.len() {
                return self.unknown("bucket ownership is not available in this map");
            }
            shards.insert(self.cluster.shards[position].id);
            buckets.insert(bucket);
        }
        json!({"status":"constrained", "shard_ids":shards, "shard_count":shards.len(),
            "bucket_count":buckets.len(), "max_shards":shards.len()})
    }

    fn path(&self, t: &TableDef, path: &AccessPath, depth: usize) -> Json {
        if depth > 16 {
            return self.unknown("diagnostic path-depth budget exceeded");
        }
        if !matches!(path, AccessPath::Union(_)) && !bound_path(path, self.cx.params) {
            return self.unknown("key depends on unbound parameters, outer rows, or an unevaluated expression");
        }
        match path {
            AccessPath::Full => self.broadcast(),
            AccessPath::Point { index, keys } | AccessPath::Prefixes { index, keys } => {
                if keys.len() > MAX_KEYS {
                    return self.unknown("diagnostic key budget exceeded");
                }
                let idx = index_def(t, *index);
                match eval_keys(t, idx, keys, &[], self.cx) {
                    Ok(keys) => self.keys(t, idx, &keys),
                    Err(_) => self.unknown("key cannot be evaluated safely during planning"),
                }
            }
            AccessPath::Range { index, eq, null_eq, .. } => {
                let idx = index_def(t, *index);
                match range_prefix(t, idx, eq, null_eq, &[], self.cx) {
                    Ok(prefix) => self.keys(t, idx, &prefix.into_iter().collect::<Vec<_>>()),
                    Err(_) => self.unknown("range prefix cannot be evaluated safely during planning"),
                }
            }
            AccessPath::Union(paths) => {
                if paths.len() > MAX_ACCESSES {
                    return self.unknown("diagnostic union budget exceeded");
                }
                let mut shards = BTreeSet::new();
                for p in paths {
                    let r = self.path(t, p, depth + 1);
                    let Some(ids) = r["shard_ids"].as_array() else {
                        return self.unknown("one index-union route is unknown");
                    };
                    for id in ids {
                        if let Some(id) = id.as_u64() {
                            shards.insert(id);
                        }
                    }
                }
                json!({"status":"union", "shard_ids":shards, "shard_count":shards.len(),
                    "bucket_count":null, "max_shards":shards.len()})
            }
        }
    }

    fn access(&self, a: &TableAccess, committed_candidates: bool) -> Json {
        let t = &a.table;
        let (kind, mut index) = match &a.path {
            AccessPath::Point { index, .. } => ("unique_point", Some(*index)),
            AccessPath::Range { index, .. } => ("range", Some(*index)),
            AccessPath::Prefixes { index, .. } => ("prefixes", Some(*index)),
            AccessPath::Union(_) => ("index_union", None),
            AccessPath::Full => ("full_scan", Some(0)),
        };
        if t.indexes.is_empty() {
            index = None;
        }
        let global = match (&a.path, index) {
            (AccessPath::Point { .. }, Some(i)) if self.live.len() > 1 && !committed_candidates => global_index(t, i),
            _ => None,
        };
        let routing = if t.is_virtual() {
            json!({"status":"gateway_metadata", "shard_ids":[], "shard_count":0, "bucket_count":0, "max_shards":0})
        } else if let Some(g) = global {
            // First read the global key; only its returned base PK tells
            // where the second read goes. Never call that one-shard access.
            let key_route = match &a.path {
                AccessPath::Point { keys, .. } if keys.len() <= MAX_KEYS && bound_path(&a.path, self.cx.params) => {
                    let idx = index_def(t, index.unwrap());
                    match eval_keys(t, idx, keys, &[], self.cx) {
                        Ok(keys) => self.keys(&g.table, g.table.pk(), &keys),
                        Err(_) => self.unknown("global lookup key cannot be evaluated safely"),
                    }
                }
                _ => self.unknown("global lookup key is unknown"),
            };
            json!({"status":"global_lookup_then_base_rows", "shard_ids":null, "shard_count":null,
                "bucket_count":null, "max_shards":self.live.len(), "global_key_route":key_route,
                "base_row_route":"depends on the base primary keys returned by the global index"})
        } else {
            self.path(t, &a.path, 0)
        };
        let catalog = self.cluster.catalog();
        let findings = crate::lint::check_table(&|db, name| catalog.get(db, name), t);
        json!({"table":format!("{}.{}",t.db,t.name), "alias":a.alias,
            "schema_findings":findings.iter().take(32).map(|f| json!({"level":f.level.name(),"message":f.message})).collect::<Vec<_>>(),
            "schema_findings_truncated":findings.len()>32,
            "definition_version":t.ver, "access_path":kind,
            "cardinality":self.cluster.statistics.snapshot(catalog.version).tables.get(&t.space_id)
                .filter(|s| s.version == t.ver).map(|s| s.diagnostic(t,&a.path)),
            "index":index.map(|i| index_def(t,i).name.clone()),
            "index_columns":index.map(|i| index_def(t,i).cols.iter().map(|&c| t.columns[c].name.clone()).collect::<Vec<_>>()),
            "shard_key":t.shard_key.iter().map(|&c| &t.columns[c].name).collect::<Vec<_>>(),
            "routing":routing})
    }

    fn colocation(&self, p: &SelectPlan, steps: &[(&TableAccess, JoinKind, Option<&Expr>)]) -> Json {
        if p.lock.is_some() {
            return json!({"eligible":false,"reason":"locking SELECT uses row-lock execution"});
        }
        if steps.iter().any(|(t, _, _)| self.buffered.contains(&t.table.space_id)) {
            return json!({"eligible":false,"reason":"this transaction has buffered writes affecting a joined table"});
        }
        if steps
            .iter()
            .any(|(t, _, _)| !bound_path(&t.path, self.cx.params) && has_unbound_path(&t.path, self.cx.params))
        {
            return json!({"eligible":null,"reason":"eligibility and root routing require bound parameters"});
        }
        match colocated::analyze(steps, self.cx) {
            Ok(Ok(plan)) => {
                let bucket = crate::route::bucket_of(&plan.shard_key, self.map.count);
                let position = self.map.shard(bucket);
                json!({"eligible":true,"reason":"identical ordered shard-key inputs proven by primary prefixes and ON equalities",
                    "bucket_id":bucket,"shard_id":(position < self.cluster.shards.len()).then(||self.cluster.shards[position].id),
                    "execution_condition":"no buffered writes affecting these tables; storage must support grouped reads"})
            }
            Ok(Err(reason)) => json!({"eligible":false,"reason":reason}),
            Err(_) => json!({"eligible":null,"reason":"key bindings cannot be evaluated during planning"}),
        }
    }

    fn select(&self, p: &SelectPlan) -> Json {
        let mut steps = Vec::new();
        let mut source = &p.source;
        loop {
            if steps.len() == MAX_ACCESSES {
                return json!({"accesses":[],"routing":self.unknown("diagnostic table budget exceeded"),"colocation":{"eligible":null,"reason":"diagnostic table budget exceeded"}});
            }
            match source {
                Source::Dual => break,
                Source::Table(a) => {
                    steps.push((a, JoinKind::Inner, None));
                    break;
                }
                Source::Join { left, right, kind, on } => {
                    steps.push((right, *kind, on.as_ref()));
                    source = left;
                }
            }
        }
        steps.reverse();
        let accesses: Vec<Json> = steps
            .iter()
            .map(|(a, _, _)| self.access(a, self.committed && p.lock.is_some()))
            .collect();
        let colocation = self.colocation(p, &steps);
        let routing = if steps.is_empty() {
            json!({"status":"gateway_only","shard_count":0,"bucket_count":0,"max_shards":0})
        } else {
            let mut ids = BTreeSet::new();
            let known = accesses.iter().all(|a| {
                if let Some(shards) = a["routing"]["shard_ids"].as_array() {
                    ids.extend(shards.iter().filter_map(Json::as_u64));
                    true
                } else {
                    false
                }
            });
            let fallback = if known {
                json!({"status":"access_union","shard_ids":ids,"shard_count":ids.len(),
                    "bucket_count":if accesses.len()==1 { accesses[0]["routing"]["bucket_count"].clone() } else { Json::Null },
                    "max_shards":ids.len()})
            } else {
                self.unknown("dependent or global-index base-row routes require runtime values")
            };
            if colocation["eligible"] == true {
                json!({"status":"conditional_grouped_read","shard_count":1,"bucket_count":1,
                    "shard_id":colocation["shard_id"],"bucket_id":colocation["bucket_id"],
                    "condition":colocation["execution_condition"],"fallback":fallback,
                    "max_shards":self.live.len()})
            } else {
                fallback
            }
        };
        json!({"accesses":accesses,"routing":routing,
            "join_estimate":p.join_estimate,
            "colocation":colocation,
            "runtime_alternative_access":p.ordered.as_ref().and_then(|path| steps.first().map(|(a,_,_)| json!({
                "routing":self.path(&a.table,path,0),
                "condition":"runtime row-count samples may choose the ordered index for LIMIT"}))),
            "lazy_index_alternative":crate::lazy::eligible(p).then(|| self.unknown(
                "an eligible nonlocking autocommit query may use lazy index routing and subsequent base-row fetches; this plan shows the strong path")),
            "runtime_participants":null,
            "runtime_observations":"SHOW TARN QUERIES reports completed foreground routing, separately from these estimates"})
    }

    fn globals(&self, t: &TableDef, values: Option<&[Vec<Option<Expr>>]>) -> Vec<Json> {
        t.globals
            .iter()
            .map(|g| {
                let route = if let Some(rows) = values.filter(|r| r.len() <= MAX_KEYS) {
                    let mut keys = Vec::new();
                    let mut known = true;
                    for row in rows {
                        let mut key = Vec::new();
                        for &column in &g.src[..g.key_len()] {
                            let value = row
                                .get(column)
                                .and_then(|e| e.as_ref())
                                .filter(|e| static_expr(e, self.cx.params))
                                .and_then(|e| e.eval(&[], self.cx).ok())
                                .and_then(|v| t.columns[column].ty().coerce(v, &t.columns[column].name).ok());
                            match value {
                                Some(v) => key.push(t.key_part(column, &v)),
                                None => {
                                    known = false;
                                    break;
                                }
                            }
                        }
                        if !known {
                            break;
                        }
                        if !key.iter().any(Value::is_null) {
                            keys.push(key);
                        }
                    }
                    if known {
                        self.keys(&g.table, g.table.pk(), &keys)
                    } else {
                        self.unknown("global key depends on row values, defaults, or unbound parameters")
                    }
                } else {
                    self.unknown("global key depends on affected rows or old/new values")
                };
                json!({"index":t.indexes[g.pos].name,"routing":route,
                "may_add_participants":true,"max_additional_shards":self.live.len().saturating_sub(1),
                "condition":"non-NULL old/new index entries change; base rows and global entries commit together"})
            })
            .collect()
    }

    fn insert_route(&self, t: &TableDef, values: Option<&[Vec<Option<Expr>>]>) -> Json {
        let Some(values) = values.filter(|rows| rows.len() <= MAX_KEYS) else {
            return self.unknown("inserted rows or diagnostic key budget are unknown");
        };
        let mut shards = BTreeSet::new();
        let mut buckets = BTreeSet::new();
        for values in values {
            let mut row = vec![Value::Null; t.columns.len()];
            for &column in &t.shard_key {
                let Some(value) = values
                    .get(column)
                    .and_then(|e| e.as_ref())
                    .filter(|e| static_expr(e, self.cx.params))
                    .and_then(|e| e.eval(&[], self.cx).ok())
                    .and_then(|v| t.columns[column].ty().coerce(v, &t.columns[column].name).ok())
                else {
                    return self
                        .unknown("shard key depends on inserted rows, defaults, AUTO_INCREMENT or unbound parameters");
                };
                row[column] = value;
            }
            let bucket = crate::route::bucket_of(&t.shard_key_of(&row), self.map.count);
            let position = self.map.shard(bucket);
            if position >= self.cluster.shards.len() {
                return self.unknown("bucket ownership is unknown");
            }
            buckets.insert(bucket);
            shards.insert(self.cluster.shards[position].id);
        }
        json!({"status":"constrained","shard_ids":shards,"shard_count":shards.len(),
            "bucket_count":buckets.len(),"max_shards":shards.len()})
    }
}

fn has_unbound(e: &Expr, params: &[Value]) -> bool {
    match e {
        Expr::Param(i) => *i >= params.len(),
        Expr::Key(e, _) | Expr::Collate(e, _) | Expr::Cast(e, _) | Expr::Neg(e) => has_unbound(e, params),
        _ => false,
    }
}

fn has_unbound_path(p: &AccessPath, params: &[Value]) -> bool {
    match p {
        AccessPath::Point { keys, .. } | AccessPath::Prefixes { keys, .. } => {
            keys.iter().flatten().any(|e| has_unbound(e, params))
        }
        AccessPath::Range { eq, .. } => eq.iter().any(|e| has_unbound(e, params)),
        AccessPath::Union(paths) => paths.iter().any(|p| has_unbound_path(p, params)),
        AccessPath::Full => false,
    }
}

pub(crate) fn render(
    cluster: &Cluster,
    stmt: &Stmt,
    version: u64,
    cx: &EvalCtx<'_>,
    committed: bool,
    buffered: &BTreeSet<u32>,
) -> SqlResult<Json> {
    let mut live: Vec<u32> = cluster.live_shards().map(|p| cluster.shards[p].id).collect();
    live.sort_unstable();
    let context = Context {
        cluster,
        map: cluster.bucket_map(),
        live,
        cx,
        committed,
        buffered,
    };
    let (kind, body) = match stmt {
        Stmt::Select(p) => ("select", context.select(p)),
        Stmt::Update(p) => (
            "update",
            json!({"accesses":[context.access(&p.access, context.committed)],
            "global_indexes":context.globals(&p.access.table,None),
            "base_write_routing":context.unknown("affected rows; assignments to a shard key can relocate rows")}),
        ),
        Stmt::Delete(p) => (
            "delete",
            json!({"accesses":[context.access(&p.access, context.committed)],
            "global_indexes":context.globals(&p.access.table,None)}),
        ),
        Stmt::Insert(p) => {
            let values = match &p.source {
                InsertSource::Values(v) => Some(v.as_slice()),
                _ => None,
            };
            (
                "insert",
                json!({"target":format!("{}.{}",p.table.db,p.table.name),
                "base_write_routing":context.insert_route(&p.table,values),
                "global_indexes":context.globals(&p.table,values),
                "conflict_write_routing":if p.replace || !p.on_dup.is_empty() {
                    Some(context.unknown("REPLACE or ON DUPLICATE KEY may also change existing rows and old global entries"))
                } else { None },
                "source":match &p.source {InsertSource::Select(p,_)=>Some(context.select(p)),_=>None}}),
            )
        }
        _ => {
            return Err(SqlError::unsupported(
                "EXPLAIN supports physical SELECT, INSERT, UPDATE and DELETE plans",
            ))
        }
    };
    Ok(json!({"format":"tarn-plan-v1","phase":"compile_time","executed":false,
        "statement":kind,"planned_catalog_version":version,"bucket_count":context.map.count,
        "live_shards":context.live,"plan":body,
        "limits":{"accesses":MAX_ACCESSES,"keys_per_access":MAX_KEYS},
        "routing_note":"constraints and upper bounds from this cached bucket map; not runtime row or participant counts"}))
}
