//! Live fixture probe used by tests/rpcs.sh. Ordinary sleeping EVALs fill
//! the gateway budget; a completion must discover a fresh connection and
//! read transaction status while those ordinary requests remain pending.
use std::time::{Duration, Instant};
use tarn_iproto::Error;
use tarn_sql::{
    mpw::{Mp, Rd},
    Cluster, ClusterConfig,
};

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let path = std::env::args().nth(1).expect("rpc_reserve CLUSTER.json");
    let mut cfg: ClusterConfig = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    cfg.pool_size = 1;
    cfg.rpc.timeout_secs = 5.0;
    cfg.rpc.connection.requests = 4;
    cfg.rpc.connection.reserved_requests = 1;
    cfg.rpc.shard.requests = 4;
    cfg.rpc.shard.reserved_requests = 1;
    cfg.rpc.global.requests = 4;
    cfg.rpc.global.reserved_requests = 2;
    let cluster = Cluster::connect(cfg).await.unwrap();
    let client = cluster.shards[0].client(&cluster).await.unwrap();
    client.eval(0, "_G.__tarn_rpc_entered=0", &[0x90]).await.unwrap();
    let opts = cluster.opts(&cluster.cfg.shards[0].replicas[0]);
    let global = opts.admission.last().unwrap();
    let mut jobs = Vec::new();
    for _ in 0..2 {
        let c = client.clone();
        jobs.push(tokio::spawn(async move {
            c.eval(
                0,
                "_G.__tarn_rpc_entered=_G.__tarn_rpc_entered+1; require('fiber').sleep(3); return 1",
                &[0x90],
            )
            .await
        }));
    }
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if global.stats().normal_pending == 2 {
                let r = client
                    .eval_control(0, "return _G.__tarn_rpc_entered", &[0x90])
                    .await
                    .unwrap();
                let data = r.data().unwrap();
                let mut rd = Rd::new(&data);
                assert_eq!(rd.array().unwrap(), 1);
                if rd.value().unwrap().as_i64() == Some(2) {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("two admitted ordinary requests did not reach storage");
    assert!(matches!(client.ping().await, Err(Error::Admission { scope: "global" })));
    let old_id = client.stats().id;
    cluster.shards[0].invalidate();
    let mut args = Mp::new();
    args.arr(1).str(&cluster.new_txid());
    let start = Instant::now();
    tokio::time::timeout(Duration::from_secs(1), cluster.call(0, "tarn_txn_status", &args.0))
        .await
        .expect("completion could not discover a route under ordinary overload")
        .unwrap();
    assert_eq!(
        global.stats().normal_pending,
        2,
        "ordinary blockers finished before the completion witness"
    );
    let fresh = cluster.shards[0].client(&cluster).await.unwrap();
    assert_ne!(old_id, fresh.stats().id);
    println!(
        "completion: fresh connection discovered and status received in {:?} with both normal slots occupied",
        start.elapsed()
    );
    client.close();
    for job in jobs {
        assert!(job.await.unwrap().is_err());
    }
    tokio::time::timeout(Duration::from_secs(1), async {
        while global.stats().normal_pending != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    fresh.ping().await.unwrap();
    println!("COMPLETION RESERVE TEST PASSED");
}
