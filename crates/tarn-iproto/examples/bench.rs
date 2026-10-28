//! Pipelined IPROTO benchmark: N tasks issue primary-key SELECTs (then, in a
//! separate phase, REPLACEs) over a few shared connections for a fixed time
//! and report throughput and latency percentiles.
//!
//! ```text
//! cargo run --release -p tarn-iproto --example bench -- \
//!     [--addr host:port] [--secs 10] [--tasks 64] [--conns 1] [--rows 100000] \
//!     [--mode select,replace,call]
//! ```
//!
//! Without `--addr` a local Tarantool is started (binary from `$TARANTOOL`,
//! default `tarantool`) with the same init script
//! as the integration tests; with `--addr` the target must have the `kv`
//! space (id 515) from `tests/common/init.lua` and a `tarn`/`tarn` user.

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use tarn_iproto::{mp, Client, Iter, Opts};

const KV: u32 = 515;
const INIT_LUA: &str = include_str!("../tests/common/init.lua");

struct Args {
    addr: Option<String>,
    secs: f64,
    tasks: usize,
    conns: usize,
    rows: u64,
    modes: Vec<String>,
}

fn parse_args() -> Args {
    let mut a = Args {
        addr: None,
        secs: 10.0,
        tasks: 64,
        conns: 1,
        rows: 100_000,
        modes: vec!["select".into(), "replace".into()],
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut val = || it.next().unwrap_or_else(|| panic!("{flag} needs a value"));
        match flag.as_str() {
            "--addr" => a.addr = Some(val()),
            "--secs" => a.secs = val().parse().expect("--secs"),
            "--tasks" => a.tasks = val().parse().expect("--tasks"),
            "--conns" => a.conns = val().parse().expect("--conns"),
            "--rows" => a.rows = val().parse().expect("--rows"),
            "--mode" => a.modes = val().split(',').map(str::to_string).collect(),
            other => panic!("unknown flag {other}"),
        }
    }
    a
}

struct LocalServer {
    child: Child,
    dir: PathBuf,
}

impl LocalServer {
    // The child is waited for in `Drop` (or before panicking).
    #[allow(clippy::zombie_processes)]
    fn start(rows: u64) -> (LocalServer, String) {
        let bin = std::env::var("TARANTOOL").unwrap_or_else(|_| "tarantool".into());
        let dir = std::env::temp_dir().join(format!("tarn-iproto-bench-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("init.lua"), INIT_LUA).unwrap();
        let mut child = Command::new(&bin)
            .arg("init.lua")
            .current_dir(&dir)
            .env("KV_ROWS", rows.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap_or_else(|e| panic!("cannot start {bin}: {e}"));
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if let Ok(addr) = std::fs::read_to_string(dir.join("listen.txt")) {
                return (LocalServer { child, dir }, addr.trim().to_string());
            }
            if Instant::now() > deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("tarantool did not start");
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for LocalServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// CPU time (user + system) used by this process so far.
fn self_cpu() -> Duration {
    // SAFETY: getrusage only writes into the zeroed struct we pass.
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    let tv = |t: libc::timeval| Duration::new(t.tv_sec as u64, t.tv_usec as u32 * 1000);
    tv(ru.ru_utime) + tv(ru.ru_stime)
}

/// CPU time (user + system) used by another process so far.
fn proc_cpu(pid: u32) -> Option<Duration> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let fields: Vec<&str> = stat[stat.rfind(')')? + 2..].split_whitespace().collect();
    let ticks: u64 = fields[11].parse::<u64>().ok()? + fields[12].parse::<u64>().ok()?;
    // SAFETY: sysconf has no preconditions.
    let hz = unsafe { libc::sysconf(libc::_SC_CLK_TCK) } as f64;
    Some(Duration::from_secs_f64(ticks as f64 / hz))
}

#[derive(Clone, Copy)]
enum Mode {
    Select,
    Replace,
    Call,
}

async fn phase(clients: &[Client], mode: Mode, args: &Args, server_pid: Option<u32>) {
    let cpu0 = self_cpu();
    let srv0 = server_pid.and_then(proc_cpu);
    let start = Instant::now();
    let deadline = start + Duration::from_secs_f64(args.secs);
    let mut handles = Vec::with_capacity(args.tasks);
    for t in 0..args.tasks {
        let c = clients[t % clients.len()].clone();
        let rows = args.rows;
        handles.push(tokio::spawn(async move {
            let mut lat: Vec<u32> = Vec::with_capacity(1 << 16);
            let mut rng = 0x9E37_79B9_7F4A_7C15u64 ^ ((t as u64 + 1) << 17);
            let mut buf = Vec::with_capacity(64);
            let mut checked = 0u64;
            while Instant::now() < deadline {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                let k = rng % rows + 1;
                buf.clear();
                let t0 = Instant::now();
                match mode {
                    Mode::Select => {
                        mp::write_array_len(&mut buf, 1);
                        mp::write_uint(&mut buf, k);
                        let r = c.select(0, KV, 0, Iter::Eq, 0, 1, &buf).await.expect("select");
                        let rows = r.rows().expect("decode");
                        assert_eq!(rows.len(), 1);
                        checked += rows[0].len() as u64;
                    }
                    Mode::Replace => {
                        mp::write_array_len(&mut buf, 3);
                        mp::write_uint(&mut buf, k);
                        mp::write_uint(&mut buf, 2 * k);
                        mp::write_str(&mut buf, b"replaced-value");
                        c.replace(0, KV, &buf).await.expect("replace");
                    }
                    Mode::Call => {
                        mp::write_array_len(&mut buf, 1);
                        mp::write_uint(&mut buf, k);
                        let r = c.call(0, "echo", &buf).await.expect("call");
                        checked += r.body.len() as u64;
                    }
                }
                lat.push(t0.elapsed().as_micros().min(u32::MAX as u128) as u32);
            }
            std::hint::black_box(checked);
            lat
        }));
    }
    let before: Vec<_> = clients.iter().map(Client::stats).collect();
    let mut all = Vec::new();
    for h in handles {
        all.extend(h.await.unwrap());
    }
    let (mut reqs, mut writes, mut reads) = (0, 0, 0);
    for (c, b) in clients.iter().zip(&before) {
        let a = c.stats();
        reqs += a.requests - b.requests;
        writes += a.write_batches - b.write_batches;
        reads += a.read_batches - b.read_batches;
    }
    let elapsed = start.elapsed().as_secs_f64();
    let client_cpu = (self_cpu() - cpu0).as_secs_f64();
    let server_cpu = match (srv0, server_pid.and_then(proc_cpu)) {
        (Some(a), Some(b)) => Some((b - a).as_secs_f64()),
        _ => None,
    };
    all.sort_unstable();
    let pct = |p: f64| {
        all.get(((all.len() as f64 * p) as usize).min(all.len().saturating_sub(1)))
            .copied()
            .unwrap_or(0)
    };
    let name = match mode {
        Mode::Select => "select",
        Mode::Replace => "replace",
        Mode::Call => "call",
    };
    println!(
        "{name:>8}: {:>9} ops in {elapsed:.1}s = {:>9.0} ops/s | latency us p50 {:>5} p99 {:>6} p99.9 {:>6} max {:>7}",
        all.len(),
        all.len() as f64 / elapsed,
        pct(0.50),
        pct(0.99),
        pct(0.999),
        all.last().copied().unwrap_or(0),
    );
    let n = all.len().max(1) as f64;
    println!(
        "          batching: {:.1} requests per write, {:.1} replies per read",
        reqs as f64 / writes.max(1) as f64,
        reqs as f64 / reads.max(1) as f64
    );
    print!(
        "          cpu: client {:.2} cores, {:.2} us/op",
        client_cpu / elapsed,
        client_cpu * 1e6 / n
    );
    match server_cpu {
        Some(s) => println!(" | tarantool {:.2} cores, {:.2} us/op", s / elapsed, s * 1e6 / n),
        None => println!(),
    }
}

#[tokio::main]
async fn main() {
    let args = parse_args();
    let (server, addr) = match &args.addr {
        Some(a) => (None, a.clone()),
        None => {
            let (s, a) = LocalServer::start(args.rows);
            (Some(s), a)
        }
    };
    let mut opts = Opts {
        addr: addr.clone(),
        user: Some("tarn".into()),
        password: Some("tarn".into()),
        connect_timeout: Duration::from_secs(5),
        ..Opts::new("")
    };
    // The environment flag lets the unchanged baseline binary ignore the
    // new hierarchy while the candidate measures all gateway-like scopes.
    if std::env::var_os("TARN_BENCH_SHARED_LIMITS").is_some() {
        let limits = tarn_iproto::Limits::default();
        let scale = |n| tarn_iproto::Limits {
            requests: limits.requests * n,
            bytes: limits.bytes * n,
            reserved_requests: limits.reserved_requests * n,
            reserved_bytes: limits.reserved_bytes * n,
        };
        opts.admission = vec![
            tarn_iproto::Budget::new("shard", scale(2)).unwrap(),
            tarn_iproto::Budget::new("global", scale(4)).unwrap(),
        ];
        println!(
            "admission profile: connection + shared shard + shared global; {:?}",
            opts.request_limits
        );
    }
    let mut clients = Vec::new();
    for _ in 0..args.conns {
        clients.push(Client::connect(&opts).await.expect("connect"));
    }
    println!(
        "tarantool {} at {addr}: {} tasks over {} connection(s), {} rows, {}s per phase",
        clients[0].server_info().version,
        args.tasks,
        args.conns,
        args.rows,
        args.secs
    );
    for m in &args.modes {
        let mode = match m.as_str() {
            "select" => Mode::Select,
            "replace" => Mode::Replace,
            "call" => Mode::Call,
            other => panic!("unknown mode {other}"),
        };
        phase(&clients, mode, &args, server.as_ref().map(|s| s.child.id())).await;
    }
    for c in &clients {
        assert_eq!(c.stats().refusals, 0, "benchmark saturation is a different workload");
        assert_eq!(c.stats().admission.deadline_retirements, 0);
    }
    drop(clients);
    drop(server);
}
