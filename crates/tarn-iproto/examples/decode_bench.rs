//! Copying vs zero-copy tuple decoding.
//!
//! ```text
//! cargo run --release -p tarn-iproto --example decode_bench
//! ```
//!
//! Builds a select-like reply (an array of tuples) and decodes every tuple
//! with `mp::decode_tuple` (strings copied into fresh `Bytes`) and with
//! `mp::decode_tuple_shared` (strings are slices of the reply buffer),
//! including the cost of dropping the decoded rows. Also measures the
//! cross-thread case, where rows are dropped on another thread.

use std::hint::black_box;
use std::time::Instant;

use bytes::Bytes;
use tarn_common::Value;
use tarn_iproto::mp;

fn reply(rows: usize, str_len: usize) -> Bytes {
    let mut out = Vec::new();
    mp::write_array_len(&mut out, rows as u32);
    let text = "s".repeat(str_len);
    for i in 0..rows {
        let row = [
            Value::Int(i as i64),
            Value::Int(-(i as i64) * 7),
            Value::str(&text),
            Value::str(&text[..str_len / 2]),
            Value::Float(i as f64 * 0.5),
            Value::Null,
            Value::Decimal("1234.50".into()),
        ];
        mp::encode_tuple(&mut out, &row);
    }
    Bytes::from(out)
}

fn decode_all(src: &Bytes, zero_copy: bool) -> Vec<Vec<Value>> {
    let mut p: &[u8] = src;
    let n = mp::read_array_len(&mut p).unwrap() as usize;
    let mut rows = Vec::with_capacity(n);
    for _ in 0..n {
        rows.push(if zero_copy { mp::decode_tuple_shared(src, &mut p).unwrap() } else { mp::decode_tuple(&mut p).unwrap() });
    }
    rows
}

fn bench(label: &str, src: &Bytes, zero_copy: bool, iters: usize, drop_elsewhere: bool) -> f64 {
    let rows_per = {
        let mut p: &[u8] = src;
        mp::read_array_len(&mut p).unwrap() as usize
    };
    let (tx, rx) = std::sync::mpsc::sync_channel::<Vec<Vec<Value>>>(4);
    let dropper = std::thread::spawn(move || {
        for rows in rx {
            drop(black_box(rows));
        }
    });
    let t0 = Instant::now();
    for _ in 0..iters {
        let rows = decode_all(black_box(src), zero_copy);
        if drop_elsewhere {
            tx.send(rows).unwrap();
        } else {
            drop(black_box(rows));
        }
    }
    drop(tx);
    dropper.join().unwrap();
    let ns = t0.elapsed().as_nanos() as f64 / (iters * rows_per) as f64;
    println!("  {label:<34} {ns:>7.1} ns/row");
    ns
}

/// Per-string cost of copy vs slice for a range of sizes (4 strings/row).
/// Min of several interleaved trials, to filter out noise from other load.
fn sweep() {
    println!("per-string sweep (rows of 4 strings), min ns/row: size | copy | slice | copy x-thread | slice x-thread");
    for len in [4usize, 8, 16, 24, 32, 48, 64, 128, 256] {
        let mut out = Vec::new();
        mp::write_array_len(&mut out, 1000);
        let text = "s".repeat(len);
        for _ in 0..1000 {
            mp::encode_tuple(&mut out, &[Value::str(&text), Value::str(&text), Value::str(&text), Value::str(&text)]);
        }
        let src = Bytes::from(out);
        let trial = |zc: bool, x: bool| {
            let iters = 200;
            let (tx, rx) = std::sync::mpsc::sync_channel::<Vec<Vec<Value>>>(4);
            let dropper = std::thread::spawn(move || {
                for rows in rx {
                    drop(black_box(rows));
                }
            });
            let t0 = Instant::now();
            for _ in 0..iters {
                let rows = decode_all(black_box(&src), zc);
                if x {
                    tx.send(rows).unwrap();
                } else {
                    drop(black_box(rows));
                }
            }
            drop(tx);
            dropper.join().unwrap();
            t0.elapsed().as_nanos() as f64 / (iters * 1000) as f64
        };
        let mut best = [f64::MAX; 4];
        for _ in 0..7 {
            for (i, (zc, x)) in [(false, false), (true, false), (false, true), (true, true)].into_iter().enumerate() {
                best[i] = best[i].min(trial(zc, x));
            }
        }
        println!("  {len:>4} | {:>6.1} | {:>6.1} | {:>6.1} | {:>6.1}", best[0], best[1], best[2], best[3]);
    }
}

fn main() {
    if std::env::args().any(|a| a == "--sweep") {
        sweep();
        return;
    }
    for str_len in [8usize, 64, 512] {
        let src = reply(1000, str_len);
        let iters = if str_len >= 512 { 400 } else { 2000 };
        println!("rows of 7 fields incl. 2 strings ({str_len} and {} bytes), {} byte reply:", str_len / 2, src.len());
        for drop_elsewhere in [false, true] {
            let suffix = if drop_elsewhere { " (dropped on another thread)" } else { "" };
            let copy = bench(&format!("copy{suffix}"), &src, false, iters, drop_elsewhere);
            let zc = bench(&format!("zero-copy{suffix}"), &src, true, iters, drop_elsewhere);
            println!("  -> zero-copy is {:.2}x the speed of copy", copy / zc);
        }
    }
}
