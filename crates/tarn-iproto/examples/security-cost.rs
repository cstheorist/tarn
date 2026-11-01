//! Matched encryption/durability cost on the real synchronous counter RPC.
//! No retries, including warm-up or a lost reply. Fixture credentials only.
use serde::Deserialize;
use std::{io, path::PathBuf, time::Instant};
use tarn_iproto::{mp, Client, Opts};
#[derive(Deserialize)]
struct Config {
    addr: String,
    password_file: PathBuf,
    tls: Option<tarn_transport::ClientTls>,
    operations: usize,
    repetitions: usize,
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args().nth(1).ok_or("usage: security-cost CONFIG")?;
    let cfg: Config = serde_json::from_slice(&std::fs::read(path)?)?;
    if cfg.operations == 0 || cfg.repetitions == 0 {
        return Err("positive cost sample sizes required".into());
    }
    let mut opts = Opts::new(&cfg.addr);
    opts.user = Some("tarn_service".into());
    opts.password = Some(tarn_transport::read_secret(&cfg.password_file)?);
    opts.tls = cfg.tls.map(|c| c.load()).transpose()?;
    let began = Instant::now();
    let c = Client::connect(&opts).await?;
    let connect_ms = began.elapsed().as_secs_f64() * 1000.;
    let mut args = Vec::new();
    mp::write_array_len(&mut args, 2);
    mp::write_str(&mut args, b"__security_cost");
    mp::write_uint(&mut args, 1);
    let mut previous = None;
    let mut samples = Vec::new();
    for _ in 0..10 {
        c.call(0, "tarn_counter_alloc", &args).await?;
    }
    for repetition in 0..cfg.repetitions {
        let mut latency_ms = Vec::with_capacity(cfg.operations);
        for _ in 0..cfg.operations {
            let t = Instant::now();
            let r = c.call(0, "tarn_counter_alloc", &args).await?;
            let ms = t.elapsed().as_secs_f64() * 1000.;
            let data = r.data().ok_or("missing counter reply")?;
            let mut b = data.as_ref();
            if mp::read_array_len(&mut b)? != 1 {
                return Err("bad counter reply".into());
            }
            let next = mp::read_uint(&mut b)?;
            if previous.is_some_and(|p| next != p + 1) {
                return Err(io::Error::other("counter duplicated or skipped a mutation").into());
            }
            previous = Some(next);
            latency_ms.push(ms);
        }
        samples.push(serde_json::json!({"repetition":repetition,"operations":cfg.operations,"latency_ms":latency_ms}));
    }
    c.close();
    println!(
        "{}",
        serde_json::json!({"connect_ms":connect_ms,"samples":samples,"last_counter":previous,"rpc":"tarn_counter_alloc","clients":1,"retries":0})
    );
    Ok(())
}
