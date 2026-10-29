//! Endpoint-only read driver: actual strong KV SDK or weaker raw IPROTO SELECT.
//! No setup, EVAL, writes, service lifecycle or database credentials in argv.
//! READY/GO/DONE/SNAPSHOT_DONE bracket business requests and file output.

use serde::Deserialize;
use serde_json::{json, Value as Json};
use std::collections::{HashMap, HashSet};
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tarn_common::Value;
use tarn_iproto::{Client, Iter, Opts, Stats};
use tarn_sql::cluster::ShardConfig;
use tarn_sql::kv::{Credentials, KvOptions, StrongKvClient, TableHandle};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    version: u32,
    seeds: Vec<ShardConfig>,
    corpus: Corpus,
    routing: Routing,
    source: Json,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Corpus {
    db: String,
    table: String,
    tenant: i64,
    rows: u64,
    payload_hex: String,
    space_id: u32,
    physical_fields: Physical,
    tuple_fields: usize,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Physical {
    tenant: usize,
    id: usize,
    payload: usize,
    bucket: usize,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Routing {
    bucket_count: u32,
    ranges: Vec<[u32; 3]>,
    raw_endpoints: Vec<RawEndpoint>,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawEndpoint {
    shard_id: u32,
    address: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Auth {
    user: String,
    password: String,
}

struct Args {
    manifest: PathBuf,
    credentials: Option<PathBuf>,
    mode: String,
    batch: usize,
    workers: usize,
    seconds: f64,
    warmup: f64,
    seed: u64,
    output: PathBuf,
}
const HELP:&str="strong_kv_bench --manifest FILE --mode strong|raw --batch 1|32 --workers 1|16 --seconds N --warmup N --seed N --output PREFIX [--credentials-file FILE]\nCredentials: file {user,password} or TARN_KV_USER/TARN_KV_PASSWORD. No password argv.\nStdout JSON READY; stdin GO; stdout JSON DONE; stdin SNAPSHOT_DONE; stdout final JSON. Raw B32 is32pipelined native point SELECTs, a weaker-guarantee reference.";

fn args() -> Result<Option<Args>, String> {
    let mut all = HashMap::new();
    let mut input = std::env::args().skip(1);
    while let Some(flag) = input.next() {
        if flag == "--help" {
            println!("{HELP}");
            return Ok(None);
        }
        if ![
            "--manifest",
            "--mode",
            "--batch",
            "--workers",
            "--seconds",
            "--warmup",
            "--seed",
            "--output",
            "--credentials-file",
        ]
        .contains(&flag.as_str())
        {
            return Err("unknown driver argument".into());
        }
        let value = input.next().ok_or("missing argument value")?;
        if all.insert(flag, value).is_some() {
            return Err("duplicate argument".into());
        }
    }
    let get = |k: &str| all.get(k).cloned().ok_or_else(|| format!("missing {k}"));
    let a = Args {
        manifest: get("--manifest")?.into(),
        credentials: all.get("--credentials-file").map(PathBuf::from),
        mode: get("--mode")?,
        batch: get("--batch")?.parse().map_err(|_| "batch")?,
        workers: get("--workers")?.parse().map_err(|_| "workers")?,
        seconds: get("--seconds")?.parse().map_err(|_| "seconds")?,
        warmup: get("--warmup")?.parse().map_err(|_| "warmup")?,
        seed: get("--seed")?.parse().map_err(|_| "seed")?,
        output: get("--output")?.into(),
    };
    if !["strong", "raw"].contains(&a.mode.as_str())
        || ![1, 32].contains(&a.batch)
        || ![1, 16].contains(&a.workers)
        || !a.seconds.is_finite()
        || !(0.01..=30.0).contains(&a.seconds)
        || !a.warmup.is_finite()
        || !(0.01..=10.0).contains(&a.warmup)
    {
        return Err("unsupported cell/duration".into());
    }
    Ok(Some(a))
}

fn unhex(s: &str) -> Result<Vec<u8>, String> {
    if !s.len().is_multiple_of(2) || s.len() > 131072 || !s.is_ascii() {
        return Err("invalid payload_hex".into());
    }
    s.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).map_err(|_| "invalid hex".into()))
        .collect()
}

fn validate(m: &Manifest) -> Result<Vec<u32>, String> {
    let c = &m.corpus;
    let r = &m.routing;
    if m.version != 1
        || m.seeds.is_empty()
        || c.rows < 32
        || c.rows > i64::MAX as u64
        || c.space_id == 0
        || c.tuple_fields != 4
        || r.bucket_count == 0
        || r.bucket_count > 1_000_000
    {
        return Err("invalid corpus/routing manifest".into());
    }
    let fields = [
        c.physical_fields.tenant,
        c.physical_fields.id,
        c.physical_fields.payload,
        c.physical_fields.bucket,
    ];
    if fields.iter().copied().collect::<HashSet<_>>().len() != 4 || fields.iter().any(|&f| f >= 4) {
        return Err("raw benchmark requires four distinct physical fields".into());
    }
    let ids = r.raw_endpoints.iter().map(|n| n.shard_id).collect::<HashSet<_>>();
    if ids.len() != r.raw_endpoints.len()
        || ids.contains(&0)
        || r.raw_endpoints
            .iter()
            .any(|n| n.address.is_empty() || n.address.contains('@') || n.address.chars().any(char::is_whitespace))
    {
        return Err("invalid credential-free raw endpoints".into());
    }
    let mut owners = vec![0; r.bucket_count as usize];
    for &[lo, hi, sid] in &r.ranges {
        if lo == 0 || lo > hi || hi > r.bucket_count || !ids.contains(&sid) {
            return Err("invalid owner range".into());
        }
        for b in lo..=hi {
            let o = &mut owners[(b - 1) as usize];
            if *o != 0 {
                return Err("overlapping owner ranges".into());
            }
            *o = sid;
        }
    }
    if owners.contains(&0) {
        return Err("incomplete static raw routing map".into());
    }
    unhex(&c.payload_hex)?;
    Ok(owners)
}

#[derive(Clone)]
enum Backend {
    Strong(Arc<StrongKvClient>, TableHandle),
    Raw(Arc<HashMap<u32, Client>>, Arc<Vec<u32>>),
}
impl Backend {
    fn stats(&self) -> Vec<Stats> {
        let mut s = match self {
            Self::Strong(c, _) => c.connection_stats(),
            Self::Raw(cs, _) => cs.values().map(Client::stats).collect(),
        };
        s.sort_by_key(|s| s.id);
        s
    }
    async fn read(&self, c: &Corpus, ids: &[u64], payload: &[u8], count: u32) -> Result<usize, Json> {
        match self {
            Self::Strong(client, table) => {
                let keys = ids
                    .iter()
                    .map(|&id| vec![Value::Int(c.tenant), Value::Int(id as i64)])
                    .collect::<Vec<_>>();
                let rows=if keys.len()==1{client.get(table,keys[0].clone()).await.map(|r|vec![r.row])}else{client.get_many(table,&keys).await.map(|r|r.rows)}.map_err(|e|{
                    if let Some(raw)=e.server(){json!({"kind":raw.kind.to_string(),"domain":raw.domain.to_string(),"code":raw.code,"message":raw.message,"peer":raw.peer,"sqlstate":raw.sqlstate})}else{json!({"kind":"client","message":e.to_string()})}
                })?;
                if rows.len() != ids.len() {
                    return Err(json!({"kind":"validation","message":"slot count"}));
                }
                for (row, id) in rows.into_iter().zip(ids) {
                    let row = row.ok_or_else(|| json!({"kind":"validation","message":"missing hit","id":id}))?;
                    if row.len() != 3
                        || row[0].as_i64() != Some(c.tenant)
                        || row[1].as_i64() != Some(*id as i64)
                        || !payload_equal(&row[2], payload)
                    {
                        return Err(json!({"kind":"validation","message":"logical key/payload mismatch","id":id}));
                    }
                }
                Ok(ids.len())
            }
            Self::Raw(clients, owners) => {
                let bucket = tarn_sql::route::bucket_of(&[Value::Int(c.tenant)], count);
                let client = &clients[&owners[(bucket - 1) as usize]];
                // A raw batch is a bounded pipeline of full-PK IPROTO SELECTs,
                // never a range substitution or a hidden strong procedure.
                let replies = futures::future::join_all(ids.iter().map(|&id| async move {
                    let mut key = Vec::new();
                    tarn_iproto::mp::encode_tuple(&mut key, &[Value::Int(c.tenant), Value::Int(id as i64)]);
                    client
                        .select(0, c.space_id, 0, Iter::Eq, 0, 1, &key)
                        .await
                        .map_err(|e| json!({"id":id,"kind":"transport","message":e.to_string()}))
                }))
                .await;
                let mut errors = Vec::new();
                let mut seen = 0;
                for (reply, id) in replies.into_iter().zip(ids) {
                    match reply {
                        Err(e) => errors.push(e),
                        Ok(reply) => {
                            let rows = reply
                                .rows()
                                .map_err(|e| json!({"kind":"decode","message":e.to_string()}))?;
                            let f = &c.physical_fields;
                            if rows.len() != 1
                                || rows[0].len() != c.tuple_fields
                                || rows[0][f.tenant].as_i64() != Some(c.tenant)
                                || rows[0][f.id].as_i64() != Some(*id as i64)
                                || !payload_equal(&rows[0][f.payload], payload)
                                || rows[0][f.bucket].as_i64() != Some(bucket as i64)
                            {
                                errors.push(
                                    json!({"id":id,"kind":"validation","message":"raw key/payload/bucket mismatch"}),
                                );
                            } else {
                                seen += 1;
                            }
                        }
                    }
                }
                if errors.is_empty() {
                    Ok(seen)
                } else {
                    Err(json!({"kind":"raw_batch","errors":errors,"validated_rows_before_error":seen}))
                }
            }
        }
    }
}
fn payload_equal(v: &Value, want: &[u8]) -> bool {
    matches!(v,Value::Str(b)|Value::Bin(b)if b.as_ref()==want)
}

fn snapshots(stats: &[Stats]) -> Json {
    json!(stats.iter().map(|s|json!({"id":s.id,"closed":s.closed,"requests":s.requests,"replies":s.replies,"bytes_written":s.bytes_written,"bytes_read":s.bytes_read,"write_batches":s.write_batches,"read_batches":s.read_batches,"pending_wire_bytes":s.pending_wire_bytes,"abandoned":s.abandoned})).collect::<Vec<_>>())
}
fn delta(before: &[Stats], after: &[Stats]) -> Json {
    let same = !before.is_empty()
        && before.len() == after.len()
        && before
            .iter()
            .zip(after)
            .all(|(b, a)| b.id == a.id && !b.closed && !a.closed);
    let mut d = [0u64; 6];
    let mut complete = same;
    if same {
        for (b, a) in before.iter().zip(after) {
            for (slot, (x, y)) in d.iter_mut().zip([
                (a.requests, b.requests),
                (a.replies, b.replies),
                (a.bytes_written, b.bytes_written),
                (a.bytes_read, b.bytes_read),
                (a.write_batches, b.write_batches),
                (a.read_batches, b.read_batches),
            ]) {
                match x.checked_sub(y) {
                    Some(v) => match slot.checked_add(v) {
                        Some(sum) => *slot = sum,
                        None => complete = false,
                    },
                    None => complete = false,
                }
            }
        }
    }
    complete &= after.iter().all(|s| s.pending_wire_bytes == 0 && s.abandoned == 0);
    json!({"complete":complete,"requests":if complete{Some(d[0])}else{None},"replies":if complete{Some(d[1])}else{None},"bytes_written":if complete{Some(d[2])}else{None},"bytes_read":if complete{Some(d[3])}else{None},"write_batches":if complete{Some(d[4])}else{None},"read_batches":if complete{Some(d[5])}else{None}})
}

fn next(rng: &mut u64) -> u64 {
    *rng ^= *rng << 13;
    *rng ^= *rng >> 7;
    *rng ^= *rng << 17;
    *rng
}
fn keys(rng: &mut u64, rows: u64, batch: usize) -> Vec<u64> {
    let start = next(rng) % (rows - batch as u64 + 1) + 1;
    (start..start + batch as u64).collect()
}

struct Sample {
    ns: u64,
    ok: bool,
    rows: u32,
}
struct Worker {
    samples: Vec<Sample>,
    error: Option<Json>,
}
async fn phase(
    a: Arc<Args>,
    m: Arc<Manifest>,
    backend: Backend,
    payload: Arc<Vec<u8>>,
    capture: bool,
) -> Result<(Vec<Worker>, f64), String> {
    let start = Instant::now();
    let until = start + Duration::from_secs_f64(if capture { a.seconds } else { a.warmup });
    let mut tasks = Vec::new();
    for worker in 0..a.workers {
        let (a, m, b, p) = (a.clone(), m.clone(), backend.clone(), payload.clone());
        tasks.push(tokio::spawn(async move {
            let mut rng = (a.seed ^ ((worker as u64 + 1) << 32)).max(1);
            let mut result = Worker {
                samples: Vec::new(),
                error: None,
            };
            while Instant::now() < until {
                if result.samples.len() >= 250_000 {
                    result.error = Some(json!({"kind":"sample_cap","message":"bounded histogram cap reached"}));
                    break;
                }
                let ids = keys(&mut rng, m.corpus.rows, a.batch);
                let t = Instant::now();
                let r = b.read(&m.corpus, &ids, &p, m.routing.bucket_count).await;
                match r {
                    Ok(rows) => {
                        if capture {
                            result.samples.push(Sample {
                                ns: t.elapsed().as_nanos() as u64,
                                ok: true,
                                rows: rows as u32,
                            });
                        }
                    }
                    Err(e) => {
                        if capture {
                            result.samples.push(Sample {
                                ns: t.elapsed().as_nanos() as u64,
                                ok: false,
                                rows: 0,
                            });
                        }
                        result.error = Some(e);
                        break;
                    }
                }
            }
            result
        }));
    }
    let mut workers = Vec::new();
    for t in tasks {
        workers.push(t.await.map_err(|e| e.to_string())?);
    }
    Ok((workers, start.elapsed().as_secs_f64()))
}

fn gate(expected: &str) -> Result<(), String> {
    let mut line = String::new();
    std::io::stdin()
        .lock()
        .read_line(&mut line)
        .map_err(|e| e.to_string())?;
    if line.trim() != expected {
        return Err(format!("expected {expected}"));
    }
    Ok(())
}
fn event(v: Json) -> Result<(), String> {
    println!("{v}");
    std::io::stdout().flush().map_err(|e| e.to_string())
}
fn save(path: &Path, v: &Json) -> Result<(), String> {
    std::fs::write(path, serde_json::to_vec_pretty(v).map_err(|e| e.to_string())?).map_err(|e| e.to_string())
}

async fn run(a: Args) -> Result<bool, String> {
    if a.output.with_extension("json").exists()
        || (0..a.workers).any(|i| {
            ["latency.u64le", "outcome.u8", "rows.u32le"]
                .iter()
                .any(|suffix| a.output.with_extension(format!("worker{i}.{suffix}")).exists())
        })
    {
        return Err("output prefix already contains evidence; choose a new prefix".into());
    }
    let m: Manifest =
        serde_json::from_slice(&std::fs::read(&a.manifest).map_err(|e| e.to_string())?).map_err(|e| e.to_string())?;
    let owners = validate(&m)?;
    let payload = Arc::new(unhex(&m.corpus.payload_hex)?);
    let credentials = if let Some(path) = &a.credentials {
        let c: Auth = serde_json::from_slice(&std::fs::read(path).map_err(|e| e.to_string())?)
            .map_err(|_| "invalid credentials file")?;
        Credentials {
            user: c.user,
            password: c.password,
        }
    } else {
        Credentials {
            user: std::env::var("TARN_KV_USER").map_err(|_| "TARN_KV_USER required")?,
            password: std::env::var("TARN_KV_PASSWORD").map_err(|_| "TARN_KV_PASSWORD required")?,
        }
    };
    let limits = tarn_iproto::Limits {
        requests: 1024,
        ..Default::default()
    };
    let backend = if a.mode == "strong" {
        let mut opts = KvOptions::new(m.seeds.clone(), credentials);
        opts.request_limits = limits;
        let client = Arc::new(StrongKvClient::connect(opts).map_err(|e| e.to_string())?);
        let table = client
            .table(&m.corpus.db, &m.corpus.table)
            .await
            .map_err(|e| e.to_string())?;
        if table.schema().token.space_id != m.corpus.space_id
            || table
                .schema()
                .columns
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>()
                != ["tenant_id", "id", "p"]
        {
            return Err("SDK corpus schema differs from manifest".into());
        }
        Backend::Strong(client, table)
    } else {
        let mut clients = HashMap::new();
        for node in &m.routing.raw_endpoints {
            let mut o = Opts::new(node.address.clone());
            o.user = Some(credentials.user.clone());
            o.password = Some(credentials.password.clone());
            o.request_limits = limits;
            o.request_timeout = Duration::from_secs(5);
            clients.insert(node.shard_id, Client::connect(&o).await.map_err(|e| e.to_string())?);
        }
        Backend::Raw(Arc::new(clients), Arc::new(owners))
    };
    let (a, m) = (Arc::new(a), Arc::new(m));
    let discovery = backend.stats();
    let (warm, _) = phase(a.clone(), m.clone(), backend.clone(), payload.clone(), false).await?;
    if warm.iter().any(|w| w.error.is_some()) {
        return Err(format!(
            "warmup failed: {:?}",
            warm.iter().filter_map(|w| w.error.as_ref()).collect::<Vec<_>>()
        ));
    }
    let warmed = backend.stats();
    event(
        json!({"event":"READY","pid":std::process::id(),"mode":a.mode,"discovery_stats":snapshots(&discovery),"warmed_stats":snapshots(&warmed),"warmup_delta":delta(&discovery,&warmed),"source":m.source}),
    )?;
    gate("GO")?;
    let before = backend.stats();
    let (workers, elapsed) = phase(a.clone(), m.clone(), backend.clone(), payload, true).await?;
    let after = backend.stats();
    event(json!({"event":"DONE","pid":std::process::id()}))?;
    gate("SNAPSHOT_DONE")?;
    let mut raw = Vec::new();
    let mut errors = Vec::new();
    let mut latencies = Vec::new();
    let mut attempted = 0u64;
    let mut rows = 0u64;
    for (i, w) in workers.iter().enumerate() {
        let lp = a.output.with_extension(format!("worker{i}.latency.u64le"));
        let op = a.output.with_extension(format!("worker{i}.outcome.u8"));
        let rp = a.output.with_extension(format!("worker{i}.rows.u32le"));
        let (mut l, mut o, mut r) = (Vec::new(), Vec::new(), Vec::new());
        for s in &w.samples {
            l.extend_from_slice(&s.ns.to_le_bytes());
            o.push(u8::from(!s.ok));
            r.extend_from_slice(&s.rows.to_le_bytes());
            attempted += 1;
            rows += s.rows as u64;
            if s.ok {
                latencies.push(s.ns);
            }
        }
        std::fs::write(&lp, l).map_err(|e| e.to_string())?;
        std::fs::write(&op, o).map_err(|e| e.to_string())?;
        std::fs::write(&rp, r).map_err(|e| e.to_string())?;
        raw.push(json!({"worker":i,"samples":w.samples.len(),"latency":lp.file_name().unwrap(),"outcome":op.file_name().unwrap(),"rows":rp.file_name().unwrap()}));
        if let Some(e) = &w.error {
            errors.push(json!({"worker":i,"error":e}));
        }
        if w.samples.is_empty() {
            errors.push(json!({"worker":i,"error":{"kind":"empty_worker"}}));
        }
    }
    latencies.sort_unstable();
    let q = |p: f64| {
        latencies
            .get(((latencies.len().saturating_sub(1)) as f64 * p) as usize)
            .copied()
    };
    let counters = delta(&before, &after);
    let accepted = errors.is_empty() && !latencies.is_empty() && counters["complete"] == true;
    let result = json!({"event":"RESULT","accepted":accepted,"mode":a.mode,"batch":a.batch,"workers":a.workers,"attempted_operations":attempted,"successful_operations":latencies.len(),"attempted_keys":attempted*a.batch as u64,"successful_keys":latencies.len()*a.batch,"returned_rows":rows,"elapsed_seconds":elapsed,"operations_per_second":latencies.len()as f64/elapsed,"p50_ns":q(0.5),"p99_ns":q(0.99),"errors":errors,"raw":raw,"before_stats":snapshots(&before),"after_stats":snapshots(&after),"transport":counters,"client_cpu_seconds":null,"scope":if a.mode=="raw"{"raw local committed IPROTO;32pipelined SELECTs forB32; no Tarn freshness/claim/owner checks"}else{"actual SDK independent_points; no cross-shard snapshot"}});
    save(&a.output.with_extension("json"), &result)?;
    event(result)?;
    Ok(accepted)
}

#[tokio::main]
async fn main() {
    let result = match args() {
        Ok(Some(a)) => run(a).await,
        Ok(None) => return,
        Err(e) => Err(e),
    };
    match result {
        Ok(true) => {}
        Ok(false) => std::process::exit(1),
        Err(e) => {
            let _ = event(json!({"event":"ERROR","message":e}));
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn seeded_keys_are_bounded_unique_and_repeatable() {
        let mut cross_language = 181;
        assert_eq!(
            keys(&mut cross_language, 100000, 32),
            (74629..74661).collect::<Vec<_>>()
        );
        assert_eq!(cross_language, 193743195636);
        let (mut a, mut b) = (181, 181);
        for n in [1, 32] {
            for _ in 0..100 {
                let x = keys(&mut a, 100000, n);
                assert_eq!(x, keys(&mut b, 100000, n));
                assert_eq!(x.len(), n);
                assert!(x[0] >= 1 && x[n - 1] <= 100000);
                assert!(x.windows(2).all(|p| p[1] == p[0] + 1));
            }
        }
    }
    #[test]
    fn changed_connection_counters_are_not_reported_as_zero() {
        let b = Stats {
            id: 1,
            ..Default::default()
        };
        let a = Stats {
            id: 2,
            ..Default::default()
        };
        let d = delta(&[b], &[a]);
        assert_eq!(d["complete"], false);
        assert!(d["requests"].is_null());
        assert_eq!(delta(&[], &[])["complete"], false);
    }
}
