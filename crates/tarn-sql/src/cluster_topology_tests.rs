//! Metadata/control-plane simulation. One wire peer represents 1,024 logical
//! groups: this is explicitly not a 1,024-group storage deployment.
use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn simulated_1024_group_refresh_coalesces_two_gateways_under_physical_caps() {
    // One fake wire service with a loopback listener per logical identity.
    // Separate destination ports work on macOS as well as Linux, without
    // depending on noncanonical 127/8 addresses or bypassing compatibility.
    let mut listeners = Vec::with_capacity(MAX_SHARDS);
    let mut addresses = Vec::with_capacity(MAX_SHARDS);
    for _ in 0..MAX_SHARDS {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        addresses.push(listener.local_addr().unwrap().to_string());
        listeners.push(listener);
    }
    let address = |id: usize| addresses[id - 1].clone();
    let shards: Vec<_> = (1..=MAX_SHARDS)
        .map(|id| serde_json::json!({"id":id, "replicas":[address(id)]}))
        .collect();
    let cfg: ClusterConfig = serde_json::from_value(serde_json::json!({
        "shards":shards, "pool_size":4,
        "topology":{"max_connections":20,"completion_connections":4,"fanout":4}
    }))
    .unwrap();
    let mut data = Mp::new();
    data.map(1).uint(0x30).arr(1).arr(3).str("ok").uint(7).arr(MAX_SHARDS);
    for id in 1..=MAX_SHARDS {
        data.map(3)
            .str("id")
            .uint(id as u64)
            .str("replicas")
            .arr(1)
            .str(&address(id))
            .str("state")
            .str("active");
    }
    let payload = Arc::new(data.0);
    let payload_bytes = payload.len();
    let refreshes = Arc::new(AtomicUsize::new(0));
    let counted = refreshes.clone();
    let peer = tokio::spawn(async move {
        let mut groups = tokio::task::JoinSet::new();
        for (index, listener) in listeners.into_iter().enumerate() {
            let payload = payload.clone();
            let counted = counted.clone();
            groups.spawn(async move {
                let mut connections = tokio::task::JoinSet::new();
                loop {
                    let mut sock = wire_test::accept(&listener).await;
                    sock.set_nodelay(true).unwrap();
                    let id = index + 1;
                    let payload = payload.clone();
                    let counted = counted.clone();
                    connections.spawn(async move {
                        loop {
                            let Ok(prefix) = sock.read_u8().await else { return };
                            assert_eq!(prefix, 0xce);
                            let size = sock.read_u32().await.unwrap() as usize;
                            let mut packet = vec![0; size];
                            sock.read_exact(&mut packet).await.unwrap();
                            let mut rd = Rd::new(&packet);
                            let (mut kind, mut sync) = (0, 0);
                            for _ in 0..rd.map().unwrap() {
                                let k = rd.value().unwrap().as_i64().unwrap();
                                let v = rd.value().unwrap().as_i64().unwrap() as u64;
                                if k == 0 {
                                    kind = v;
                                }
                                if k == 1 {
                                    sync = v;
                                }
                            }
                            let mut body = Mp::new();
                            match kind {
                                7 => {
                                    body.map(0);
                                } // AUTH (test-only peer)
                                8 => {
                                    body.map(1).uint(0x30).arr(1).uint(0);
                                } // writable probe
                                10 => {
                                    let mut proc = String::new();
                                    for _ in 0..rd.map().unwrap() {
                                        if rd.value().unwrap().as_i64() == Some(0x22) {
                                            proc = rd.string().unwrap();
                                        } else {
                                            rd.skip().unwrap();
                                        }
                                    }
                                    match proc.as_str() {
                                        "tarn_compatibility" => {
                                            body.map(1)
                                                .uint(0x30)
                                                .arr(1)
                                                .arr(8)
                                                .str("ok")
                                                .uint(1)
                                                .uint(1)
                                                .uint(id as u64)
                                                .str("3.9.0")
                                                .uint(14)
                                                .str("ready")
                                                .f64(120.0);
                                        }
                                        "tarn_read_contract" => {
                                            body.map(1).uint(0x30).arr(1).arr(2).str("strong_reads").uint(1);
                                        }
                                        "tarn_members" => {
                                            counted.fetch_add(1, Ordering::Relaxed);
                                            body.0.extend_from_slice(&payload);
                                        }
                                        other => panic!("unexpected simulated control call {other}"),
                                    }
                                }
                                other => panic!("unexpected simulated request {other}"),
                            }
                            wire_test::reply(&mut sock, sync, 0, &body.0).await;
                            sock.flush().await.unwrap();
                        }
                    });
                    while let Some(done) = connections.try_join_next() {
                        done.unwrap();
                    }
                }
            });
        }
        while let Some(done) = groups.join_next().await {
            done.unwrap();
        }
    });
    let gateways = [
        Cluster::configured(cfg.clone()).unwrap(),
        Cluster::configured(cfg).unwrap(),
    ];
    let started = Instant::now();
    let callers = gateways.iter().flat_map(|c| (0..8).map(move |_| c.load_members()));
    let outcomes = tokio::time::timeout(Duration::from_secs(60), futures::future::join_all(callers))
        .await
        .unwrap();
    for outcome in outcomes {
        outcome.unwrap();
    }
    assert_eq!(refreshes.load(Ordering::Relaxed), MAX_SHARDS * gateways.len());
    for c in &gateways {
        assert_eq!(c.member_ver(), 7);
        assert_eq!(c.members.read().shards.len(), MAX_SHARDS);
        assert_eq!(c.member_reads.load(Ordering::Relaxed), 1);
        assert!(c.connections.stats().peak <= 20);
        assert!(c.responses.stats().peak <= c.cfg.topology.response_bytes);
        assert!(c.fanout.counters().1 <= 4);
        println!("metadata simulation: groups={MAX_SHARDS} gateways=2 payload={payload_bytes} sockets={:?} responses={:?} traffic={:?} elapsed={:?}",
            c.connections.stats(), c.responses.stats(), c.connections.traffic(), started.elapsed());
        assert_eq!(c.responses.stats().bytes, 0);
    }
    // The supported position limit is checked before indexing or opening I/O.
    let mut oversized = gateways[0].cfg.clone();
    oversized.shards.push(oversized.shards[0].clone());
    assert!(Cluster::configured(oversized).is_err());
    drop(gateways);
    peer.abort();
    let _ = peer.await;
}
