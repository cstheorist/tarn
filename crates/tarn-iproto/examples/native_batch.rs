//! Guarded full-primary-key batch reads (issue #141). No server is started.
//! `bench/native_batch.py` gates the measured interval and captures storage
//! metrics. Raw samples are little-endian u64 nanoseconds, with one u8
//! outcome per sample: 0 Lua, 1 native, 2 native hand-back followed by Lua,
//! 3 failed operation (first-RPC time unknown/zero; details in errors.txt).
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tarn_iproto::{mp, Client, Opts, Response};

#[derive(Clone)]
struct Args {
    addr: String,
    space: u32,
    epoch: u64,
    membership: u64,
    tenant: Option<i64>,
    route: String,
    bucket: u64,
    rows: u64,
    payload: usize,
    buckets: u64,
    batch: usize,
    workers: usize,
    conns: usize,
    seed: u64,
    seconds: f64,
    warmup: f64,
    timeout: f64,
    mode: String,
    shape: String,
    native_proc: String,
    output: PathBuf,
    eval: Option<String>,
    gate: bool,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            addr: String::new(),
            space: 0,
            epoch: 0,
            membership: 0,
            tenant: None,
            route: "epoch".into(),
            bucket: 0,
            rows: 100_000,
            payload: 64,
            buckets: 480,
            batch: 1,
            workers: 1,
            conns: 1,
            seed: 141,
            seconds: 3.0,
            warmup: 1.0,
            timeout: 5.0,
            mode: "lua".into(),
            shape: "hits".into(),
            native_proc: "tarn_storage.ts_select".into(),
            output: "batch".into(),
            eval: None,
            gate: false,
        }
    }
}

fn args() -> Args {
    let mut a = Args::default();
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        if flag == "--help" {
            println!("native_batch --addr HOST:PORT --space ID --epoch N --membership N [--tenant 42] [--route epoch|buckets --bucket N] [--batch 1..256] [--shape hits|duplicates|miss|mixed] [--mode lua|native] [--workers N] [--conns N] [--rows 100000] [--payload 64] [--warmup 1] [--seconds 3] [--seed 141] [--output PREFIX] [--gate]\nRaw files: latency.u64le, first.u64le, outcome.u8, route.msgpack. --eval CODE prints one JSON string returned by storage EVAL; starts no services.");
            std::process::exit(0);
        }
        if flag == "--gate" {
            a.gate = true;
            continue;
        }
        let v = it.next().unwrap_or_else(|| panic!("{flag} needs a value"));
        match flag.as_str() {
            "--addr" => a.addr = v,
            "--space" => a.space = v.parse().unwrap(),
            "--epoch" => a.epoch = v.parse().unwrap(),
            "--membership" => a.membership = v.parse().unwrap(),
            "--tenant" => a.tenant = Some(v.parse().unwrap()),
            "--route" => a.route = v,
            "--bucket" => a.bucket = v.parse().unwrap(),
            "--rows" => a.rows = v.parse().unwrap(),
            "--payload" => a.payload = v.parse().unwrap(),
            "--buckets" => a.buckets = v.parse().unwrap(),
            "--batch" => a.batch = v.parse().unwrap(),
            "--workers" => a.workers = v.parse().unwrap(),
            "--conns" => a.conns = v.parse().unwrap(),
            "--seed" => a.seed = v.parse().unwrap(),
            "--seconds" => a.seconds = v.parse().unwrap(),
            "--warmup" => a.warmup = v.parse().unwrap(),
            "--timeout" => a.timeout = v.parse().unwrap(),
            "--mode" => a.mode = v,
            "--shape" => a.shape = v,
            "--native-proc" => a.native_proc = v,
            "--output" => a.output = v.into(),
            "--eval" => a.eval = Some(v),
            _ => panic!("unknown option {flag}"),
        }
    }
    assert!(!a.addr.is_empty(), "--addr required");
    assert!(a.eval.is_some() || a.space > 0, "--space required");
    assert!((1..=256).contains(&a.batch) && (1..=64).contains(&a.workers));
    assert!((1..=64).contains(&a.conns) && (1..=10_000_000).contains(&a.rows));
    assert!((1..=4096).contains(&a.payload) && a.buckets > 0);
    assert!(a.seconds > 0.0 && a.seconds <= 120.0 && a.warmup >= 0.0 && a.warmup <= 30.0);
    assert!(a.timeout > 0.0 && a.timeout <= 15.0);
    assert!(["lua", "native"].contains(&a.mode.as_str()));
    assert!(["epoch", "buckets"].contains(&a.route.as_str()));
    assert!(a.route != "buckets" || (a.bucket > 0 && a.bucket <= a.buckets));
    assert!(["hits", "duplicates", "miss", "mixed"].contains(&a.shape.as_str()));
    a
}

fn next(rng: &mut u64) -> u64 {
    *rng ^= *rng << 13;
    *rng ^= *rng >> 7;
    *rng ^= *rng << 17;
    *rng
}

fn keys(a: &Args, rng: &mut u64, out: &mut Vec<u64>) {
    out.clear();
    for i in 0..a.batch {
        let hit = next(rng) % a.rows + 1;
        let k = match a.shape.as_str() {
            "miss" => hit + a.rows,
            "duplicates" if i % 2 == 1 => out[i - 1],
            "mixed" if i % 4 == 0 => hit + a.rows,
            "mixed" if i % 4 == 3 => out[i - 1],
            _ => hit,
        };
        out.push(k);
    }
}

fn encode(a: &Args, keys: &[u64], out: &mut Vec<u8>) {
    out.clear();
    mp::write_array_len(out, 8);
    mp::write_uint(out, a.space as u64);
    mp::write_uint(out, 0);
    mp::write_array_len(out, keys.len() as u32);
    for &key in keys {
        mp::write_array_len(out, if a.tenant.is_some() { 2 } else { 1 });
        if let Some(tenant) = a.tenant {
            mp::write_int(out, tenant);
        }
        mp::write_uint(out, key);
    }
    mp::write_nil(out);
    mp::write_bool(out, true);
    mp::write_bool(out, false);
    mp::write_nil(out);
    encode_route(a, keys.len(), out);
}

fn encode_route(a: &Args, count: usize, out: &mut Vec<u8>) {
    if a.route == "buckets" {
        mp::write_array_len(out, count as u32);
        for _ in 0..count {
            mp::write_uint(out, a.bucket);
        }
    } else {
        mp::write_map_len(out, 2);
        mp::write_str(out, b"e");
        mp::write_uint(out, a.epoch);
        mp::write_str(out, b"m");
        mp::write_uint(out, a.membership);
    }
}

#[derive(Debug, PartialEq)]
enum Reply {
    Lua,
    Rows(usize),
}

fn validate(data: &[u8], expected: &[u64], a: &Args) -> Result<Reply, String> {
    let check = || -> Result<Reply, tarn_iproto::Error> {
        let mut p = data;
        if mp::read_array_len(&mut p)? != 1 {
            return Err(tarn_iproto::Error::Protocol("CALL envelope".into()));
        }
        let reply_start = p;
        let n = mp::read_array_len(&mut p)?;
        let status = mp::read_str(&mut p)?;
        if n == 1 && status == b"lua" && p.is_empty() {
            return Ok(Reply::Lua);
        }
        if !(2..=3).contains(&n) || status != b"ok" {
            let mut raw = reply_start;
            return Err(tarn_iproto::Error::Protocol(format!(
                "guarded reply {:?}",
                mp::decode_tuple(&mut raw)
            )));
        }
        let want: Vec<_> = expected.iter().copied().filter(|&k| k <= a.rows).collect();
        let count = mp::read_array_len(&mut p)? as usize;
        if count != want.len() {
            return Err(tarn_iproto::Error::Protocol(format!(
                "rows {count}, expected {}",
                want.len()
            )));
        }
        for key in want {
            if mp::read_array_len(&mut p)? != if a.tenant.is_some() { 4 } else { 3 } {
                return Err(tarn_iproto::Error::Protocol("row arity".into()));
            }
            if let Some(tenant) = a.tenant {
                if mp::read_int(&mut p)? != tenant {
                    return Err(tarn_iproto::Error::Protocol("wrong tenant".into()));
                }
            }
            if mp::read_int(&mut p)? != key as i64 {
                return Err(tarn_iproto::Error::Protocol("row key/arity/order".into()));
            }
            let value = mp::read_str(&mut p)?;
            if value.len() != a.payload || value.iter().any(|&b| b != b'x') {
                return Err(tarn_iproto::Error::Protocol("payload bytes".into()));
            }
            let bucket = mp::read_uint(&mut p)?;
            if bucket == 0 || bucket > a.buckets || (a.bucket > 0 && bucket != a.bucket) {
                return Err(tarn_iproto::Error::Protocol("bucket".into()));
            }
        }
        if n == 3 {
            if p.first() != Some(&0xc0) {
                return Err(tarn_iproto::Error::Protocol("partial first page".into()));
            }
            p = &p[1..];
        }
        if !p.is_empty() {
            return Err(tarn_iproto::Error::Protocol("trailing reply bytes".into()));
        }
        Ok(Reply::Rows(count))
    };
    check().map_err(|e| e.to_string())
}

#[derive(Clone, Copy)]
struct Sample {
    ns: u64,
    first_ns: u64,
    outcome: u8,
    rows: usize,
}

async fn request(c: &Client, a: &Args, args: &[u8]) -> Result<Response, String> {
    let name = if a.mode == "native" {
        &a.native_proc
    } else {
        "tarn_select_keys"
    };
    tokio::time::timeout(Duration::from_secs_f64(a.timeout), c.call(0, name, args))
        .await
        .map_err(|_| "read RPC deadline; no retry".to_string())?
        .map_err(|e| e.to_string())
}

async fn one(c: &Client, a: &Args, args: &[u8], kk: &[u64]) -> Result<Sample, String> {
    let start = Instant::now();
    let r = request(c, a, args).await?;
    let first_ns = start.elapsed().as_nanos() as u64;
    let first = validate(&r.data().ok_or("missing CALL data")?, kk, a)?;
    let (outcome, rows) = match first {
        Reply::Lua if a.mode == "native" => {
            let r = tokio::time::timeout(Duration::from_secs_f64(a.timeout), c.call(0, "tarn_select_keys", args))
                .await
                .map_err(|_| "Lua fallback deadline; no retry")?
                .map_err(|e| e.to_string())?;
            match validate(&r.data().ok_or("missing fallback data")?, kk, a)? {
                Reply::Rows(n) => (2, n),
                Reply::Lua => return Err("Lua procedure handed back".into()),
            }
        }
        Reply::Rows(n) => (u8::from(a.mode == "native"), n),
        Reply::Lua => return Err("Lua procedure handed back".into()),
    };
    Ok(Sample {
        ns: start.elapsed().as_nanos() as u64,
        first_ns,
        outcome,
        rows,
    })
}

struct Phase {
    samples: Vec<Sample>,
    elapsed: f64,
    cpu: f64,
    errors: Vec<String>,
    // requests, replies, write batches, read batches, bytes written/read.
    transport: [u64; 6],
}

async fn phase(a: Arc<Args>, clients: &[Client], seconds: f64, capture: bool) -> Result<Phase, String> {
    let start = Instant::now();
    let until = start + Duration::from_secs_f64(seconds);
    let cpu0 = cpu();
    let before: Vec<_> = clients.iter().map(Client::stats).collect();
    let mut handles = Vec::new();
    for worker in 0..a.workers {
        let a = a.clone();
        let c = clients[worker % clients.len()].clone();
        handles.push(tokio::spawn(async move {
            let mut samples = Vec::new();
            let mut rng = (a.seed ^ ((worker as u64 + 1) << 32)).max(1);
            let mut kk = Vec::with_capacity(a.batch);
            let mut args = Vec::with_capacity(2048);
            let mut error = None;
            while Instant::now() < until {
                keys(&a, &mut rng, &mut kk);
                encode(&a, &kk, &mut args);
                let t0 = Instant::now();
                match one(&c, &a, &args, &kk).await {
                    Ok(sample) => {
                        if capture {
                            samples.push(sample);
                        }
                    }
                    Err(e) => {
                        if capture {
                            samples.push(Sample {
                                ns: t0.elapsed().as_nanos() as u64,
                                first_ns: 0,
                                outcome: 3,
                                rows: 0,
                            });
                        }
                        error = Some(format!("worker {worker}: {e}"));
                        break;
                    }
                }
            }
            (samples, error)
        }));
    }
    let mut all = Vec::new();
    let mut errors = Vec::new();
    for h in handles {
        let (samples, error) = h.await.map_err(|e| e.to_string())?;
        all.extend(samples);
        if let Some(e) = error {
            errors.push(e);
        }
    }
    let mut transport = [0; 6];
    for (c, b) in clients.iter().zip(before) {
        let end = c.stats();
        for (slot, value) in transport.iter_mut().zip([
            end.requests - b.requests,
            end.replies - b.replies,
            end.write_batches - b.write_batches,
            end.read_batches - b.read_batches,
            end.bytes_written - b.bytes_written,
            end.bytes_read - b.bytes_read,
        ]) {
            *slot += value;
        }
    }
    Ok(Phase {
        samples: all,
        elapsed: start.elapsed().as_secs_f64(),
        cpu: cpu() - cpu0,
        errors,
        transport,
    })
}

fn cpu() -> f64 {
    let mut r: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe {
        libc::getrusage(libc::RUSAGE_SELF, &mut r);
    }
    (r.ru_utime.tv_sec + r.ru_stime.tv_sec) as f64 + (r.ru_utime.tv_usec + r.ru_stime.tv_usec) as f64 / 1e6
}

async fn run(a: Args) -> Result<(), String> {
    let mut opts = Opts::new(a.addr.clone());
    opts.user = Some("tarn".into());
    opts.password = Some("tarn".into());
    let mut clients = Vec::new();
    for _ in 0..a.conns {
        clients.push(Client::connect(&opts).await.map_err(|e| e.to_string())?);
    }
    if let Some(code) = &a.eval {
        let r = tokio::time::timeout(Duration::from_secs_f64(a.timeout), clients[0].eval(0, code, &[0x90]))
            .await
            .map_err(|_| "observer deadline")?
            .map_err(|e| e.to_string())?;
        let data = r.data().ok_or("observer has no data")?;
        let mut p = &data[..];
        if mp::read_array_len(&mut p).map_err(|e| e.to_string())? != 1 {
            return Err("observer envelope".into());
        }
        let s = mp::read_str(&mut p).map_err(|e| e.to_string())?;
        println!("{}", std::str::from_utf8(s).map_err(|e| e.to_string())?);
        return Ok(());
    }
    let a = Arc::new(a);
    let mut route = Vec::new();
    encode_route(&a, a.batch, &mut route);
    std::fs::write(a.output.with_extension("route.msgpack"), route).map_err(|e| e.to_string())?;
    if a.warmup > 0.0 {
        let warm = phase(a.clone(), &clients, a.warmup, false).await?;
        if !warm.errors.is_empty() {
            return Err(warm.errors.join("\n"));
        }
    }
    println!("READY");
    std::io::stdout().flush().map_err(|e| e.to_string())?;
    if a.gate {
        let mut s = String::new();
        std::io::stdin().read_line(&mut s).map_err(|e| e.to_string())?;
        if s.trim() != "GO" {
            return Err("expected GO".into());
        }
    }
    let measured = phase(a.clone(), &clients, a.seconds, true).await?;
    let samples = &measured.samples;
    println!("DONE");
    std::io::stdout().flush().map_err(|e| e.to_string())?;
    if a.gate {
        // Keep summary output out of the pipe until the controller has read
        // DONE and sampled storage. This also excludes sample-file work from
        // the storage metric interval and avoids buffered-reader ambiguity.
        let mut s = String::new();
        std::io::stdin().read_line(&mut s).map_err(|e| e.to_string())?;
        if s.trim() != "FINISH" {
            return Err("expected FINISH".into());
        }
    }
    if samples.is_empty() {
        return Err("no measured operations".into());
    }
    let mut latency =
        BufWriter::new(File::create(a.output.with_extension("latency.u64le")).map_err(|e| e.to_string())?);
    let mut first = BufWriter::new(File::create(a.output.with_extension("first.u64le")).map_err(|e| e.to_string())?);
    let mut outcomes = BufWriter::new(File::create(a.output.with_extension("outcome.u8")).map_err(|e| e.to_string())?);
    for s in samples {
        latency.write_all(&s.ns.to_le_bytes()).map_err(|e| e.to_string())?;
        first.write_all(&s.first_ns.to_le_bytes()).map_err(|e| e.to_string())?;
        outcomes.write_all(&[s.outcome]).map_err(|e| e.to_string())?;
    }
    latency.flush().map_err(|e| e.to_string())?;
    first.flush().map_err(|e| e.to_string())?;
    outcomes.flush().map_err(|e| e.to_string())?;
    std::fs::write(a.output.with_extension("errors.txt"), measured.errors.join("\n")).map_err(|e| e.to_string())?;
    let mut sorted: Vec<_> = samples.iter().filter(|s| s.outcome != 3).map(|s| s.ns).collect();
    sorted.sort_unstable();
    let q = |p: f64| {
        if sorted.is_empty() {
            0
        } else {
            sorted[((sorted.len() - 1) as f64 * p) as usize]
        }
    };
    let native = samples.iter().filter(|s| s.outcome == 1).count();
    let fallback = samples.iter().filter(|s| s.outcome == 2).count();
    println!("{{\"operations\":{},\"successful_operations\":{},\"returned_rows\":{},\"elapsed_seconds\":{},\"client_cpu_seconds\":{},\"batch\":{},\"native_served\":{},\"fallbacks\":{},\"p50_ns\":{},\"p99_ns\":{},\"max_ns\":{},\"errors\":{},\"requests\":{},\"replies\":{},\"write_batches\":{},\"read_batches\":{},\"bytes_written\":{},\"bytes_read\":{}}}", samples.len(), sorted.len(), samples.iter().map(|s| s.rows).sum::<usize>(), measured.elapsed, measured.cpu, a.batch, native, fallback, q(0.50), q(0.99), sorted.last().copied().unwrap_or(0), measured.errors.len(),measured.transport[0],measured.transport[1],measured.transport[2],measured.transport[3],measured.transport[4],measured.transport[5]);
    if !measured.errors.is_empty() {
        return Err("measured operation failed; raw samples/errors preserved; no retry".into());
    }
    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    if let Err(e) = run(args()).await {
        eprintln!("native_batch: {e}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn reply(keys: &[u64], resume: bool) -> Vec<u8> {
        let mut m = Vec::new();
        mp::write_array_len(&mut m, 1);
        mp::write_array_len(&mut m, if resume { 3 } else { 2 });
        mp::write_str(&mut m, b"ok");
        mp::write_array_len(&mut m, keys.len() as u32);
        for &key in keys {
            mp::write_array_len(&mut m, 3);
            mp::write_uint(&mut m, key);
            mp::write_str(&mut m, b"xxxx");
            mp::write_uint(&mut m, 1);
        }
        if resume {
            mp::write_array_len(&mut m, 0);
        }
        m
    }
    #[test]
    fn duplicates_and_misses_preserve_request_order() {
        let a = Args {
            rows: 100,
            payload: 4,
            ..Args::default()
        };
        assert_eq!(
            validate(&reply(&[2, 2, 1], false), &[2, 2, 101, 1], &a),
            Ok(Reply::Rows(3))
        );
        assert!(validate(&reply(&[2, 1, 2], false), &[2, 2, 101, 1], &a).is_err());
        assert_eq!(validate(&reply(&[], false), &[101, 102], &a), Ok(Reply::Rows(0)));
    }
    #[test]
    fn a_partial_page_cannot_be_counted_as_a_complete_batch() {
        let a = Args {
            rows: 100,
            payload: 4,
            ..Args::default()
        };
        assert!(validate(&reply(&[1], true), &[1], &a).is_err());
        assert_eq!(
            validate(&[0x91, 0x91, 0xa3, b'l', b'u', b'a'], &[1], &a),
            Ok(Reply::Lua)
        );
    }
    #[test]
    fn composite_tenant_and_actual_route_vector_are_checked_independently() {
        let a = Args {
            tenant: Some(42),
            route: "buckets".into(),
            bucket: 37,
            payload: 4,
            ..Args::default()
        };
        let mut route = Vec::new();
        encode_route(&a, 3, &mut route);
        assert_eq!(route, vec![0x93, 37, 37, 37]);
        let mut data = vec![
            0x91, 0x92, 0xa2, b'o', b'k', 0x91, 0x94, 42, 7, 0xa4, b'x', b'x', b'x', b'x', 37,
        ];
        assert_eq!(validate(&data, &[7], &a), Ok(Reply::Rows(1)));
        data[7] = 43;
        assert!(
            validate(&data, &[7], &a).is_err(),
            "same id in another tenant is not this key"
        );
    }
}
