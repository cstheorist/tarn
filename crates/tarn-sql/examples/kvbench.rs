//! Direct Key/Value reads of SQL rows: N tasks read random primary keys of
//! a Tarn table straight from the Tarantool shards (IPROTO SELECT on the
//! table's space, index 0), routing each key with the same bucket function
//! the SQL layer uses. No SQL server is involved.
//!
//! ```text
//! cargo run --release -p tarn-sql --example kvbench -- \
//!     --shards 127.0.0.1:3311,127.0.0.1:3321 --space sbtest.sbtest1 \
//!     --keys 100000 --tasks 32 --secs 10 [--conns 1] [--check]
//! ```
//! `--check` verifies every read returned the row whose primary key was
//! asked for (and that the key exists).

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tarn_common::Value;
use tarn_iproto::{Client, Iter, Opts};

struct Args {
    shards: Vec<String>,
    space: String,
    keys: i64,
    tasks: usize,
    secs: f64,
    conns: usize,
    check: bool,
    buckets: u32,
}

fn parse() -> Args {
    let mut a = Args {
        shards: vec![],
        space: "sbtest.sbtest1".into(),
        keys: 10_000,
        tasks: 32,
        secs: 10.0,
        conns: 1,
        check: false,
        buckets: 3000,
    };
    let mut it = std::env::args().skip(1);
    while let Some(f) = it.next() {
        let mut v = || it.next().expect("missing value");
        match f.as_str() {
            "--shards" => a.shards = v().split(',').map(String::from).collect(),
            "--space" => a.space = v(),
            "--keys" => a.keys = v().parse().unwrap(),
            "--tasks" => a.tasks = v().parse().unwrap(),
            "--secs" => a.secs = v().parse().unwrap(),
            "--conns" => a.conns = v().parse().unwrap(),
            "--buckets" => a.buckets = v().parse().unwrap(),
            "--check" => a.check = true,
            other => panic!("unknown flag {other}"),
        }
    }
    assert!(!a.shards.is_empty(), "--shards required (in shard order)");
    a
}

#[tokio::main]
async fn main() {
    let a = parse();
    let mut pools: Vec<Vec<Client>> = Vec::new();
    for addr in &a.shards {
        let mut v = Vec::new();
        for _ in 0..a.conns {
            let mut o = Opts::new(addr.clone());
            o.user = Some("tarn".into());
            o.password = Some("tarn".into());
            v.push(Client::connect(&o).await.expect("connect"));
        }
        pools.push(v);
    }
    // Resolve the space id by name (the SQL catalog names spaces "db.table").
    let r = pools[0][0]
        .eval(0, "return box.space[...].id", &{
            let mut m = Vec::new();
            m.push(0x91);
            tarn_iproto::mp::encode_value(&mut m, &Value::str(&a.space));
            m
        })
        .await
        .expect("space lookup");
    let space_id = {
        let d = r.data().unwrap();
        let mut p: &[u8] = &d;
        tarn_iproto::mp::read_array_len(&mut p).unwrap();
        tarn_iproto::mp::decode_value(&mut p).unwrap().as_i64().expect("no such space") as u32
    };
    let pools = Arc::new(pools);
    let stop = Arc::new(AtomicBool::new(false));
    let ops = Arc::new(AtomicU64::new(0));
    let bad = Arc::new(AtomicU64::new(0));
    let nshards = a.shards.len() as u32;
    let mut handles = Vec::new();
    for t in 0..a.tasks {
        let (pools, stop, ops, bad) = (pools.clone(), stop.clone(), ops.clone(), bad.clone());
        let (keys, check, buckets) = (a.keys, a.check, a.buckets);
        handles.push(tokio::spawn(async move {
            let mut lat = Vec::with_capacity(1 << 16);
            let mut x: u64 = 0x9E37_79B9_7F4A_7C15 ^ (t as u64 + 1);
            let mut key = Vec::with_capacity(16);
            while !stop.load(Ordering::Relaxed) {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                let id = (x % keys as u64) as i64 + 1;
                let v = Value::Int(id);
                let bucket = tarn_sql::route::bucket_of(std::slice::from_ref(&v), buckets);
                let shard = tarn_sql::route::shard_of_bucket(bucket, buckets, nshards) as usize;
                let pool = &pools[shard];
                let c = &pool[(x as usize >> 3) % pool.len()];
                key.clear();
                tarn_iproto::mp::encode_tuple(&mut key, std::slice::from_ref(&v));
                let t0 = Instant::now();
                let r = c.select(0, space_id, 0, Iter::Eq, 0, 1, &key).await.expect("select");
                lat.push(t0.elapsed().as_nanos() as u32);
                if check {
                    let rows = r.rows().expect("decode");
                    if rows.len() != 1 || rows[0][0].as_i64() != Some(id) {
                        bad.fetch_add(1, Ordering::Relaxed);
                    }
                }
                ops.fetch_add(1, Ordering::Relaxed);
            }
            lat
        }));
    }
    let t0 = Instant::now();
    tokio::time::sleep(Duration::from_secs_f64(a.secs)).await;
    stop.store(true, Ordering::Relaxed);
    let mut all: Vec<u32> = Vec::new();
    for h in handles {
        all.extend(h.await.unwrap());
    }
    let el = t0.elapsed().as_secs_f64();
    all.sort_unstable();
    let pct = |p: f64| all.get(((all.len() as f64 * p) as usize).min(all.len().saturating_sub(1))).copied().unwrap_or(0) as f64 / 1000.0;
    println!(
        "kv point reads: {} ops in {:.1}s = {:.0} ops/s | latency us p50 {:.0} p95 {:.0} p99 {:.0} | wrong/missing {}",
        ops.load(Ordering::Relaxed),
        el,
        ops.load(Ordering::Relaxed) as f64 / el,
        pct(0.50),
        pct(0.95),
        pct(0.99),
        bad.load(Ordering::Relaxed)
    );
}
