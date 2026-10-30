//! Observe real Session DML and IPROTO connection ownership, not just pool picks.
use super::*;
use crate::{Output, Session};

use super::test_support as common;

const MARK: u32 = 1; // Session's autocommit native-unavailable mark.

#[tokio::test]
async fn uncertain_native_reply_never_replays_a_committed_increment() {
    for mode in [
        "malformed_error",
        "malformed_lua",
        "malformed_returns",
        "malformed_missing",
        "unknown",
    ] {
        let (_t, _c, control, mut s) = setup(1).await;
        eval(
            &control,
            &format!("fallback_mode = '{mode}'; fallback_log = {{native = {{}}, lua = {{}}, active = {{}}}}"),
        )
        .await;
        let err = s.query("UPDATE t SET v = v + 1 WHERE id = 1").await.unwrap_err();
        assert_eq!((err.code, err.sqlstate), (1180, *b"40003"), "{mode}: {err}");
        assert_eq!(value(&mut s).await, 1, "{mode}: no replay after effects");
        let got = log(&control).await;
        assert_eq!(ids(&got, "native").len(), 1, "{mode}: {got}");
        assert!(ids(&got, "lua").is_empty(), "{mode}: no handback after effects: {got}");
    }
}

async fn setup(n: usize) -> (common::Tarantool, Arc<Cluster>, Client, Session) {
    let lua = concat!(env!("CARGO_MANIFEST_DIR"), "/../../lua");
    let t = common::Tarantool::start_with(include_str!("cluster_fallback_test.lua"), &[("TARN_TEST_LUA_DIR", lua)]);
    let control = t.client().await;
    let cfg = serde_json::from_value(serde_json::json!({
        "shards": [{"id": 1, "replicas": [t.addr]}],
        "pool_size": n, "bucket_count": 16, "deadlock_interval": 0
    }))
    .unwrap();
    let c = Cluster::connect(cfg).await.unwrap();
    // Exercise the complete configured pool despite lazy creation: external
    // owners keep each existing socket busy while the next one is requested.
    let mut pinned = Vec::new();
    for _ in 0..n {
        pinned.push(c.shards[0].client(&c).await.unwrap());
    }
    drop(pinned);
    let mut s = Session::new(c.clone(), 1);
    for sql in [
        "CREATE DATABASE f",
        "USE f",
        "CREATE TABLE t (id INT PRIMARY KEY, v INT)",
        "INSERT INTO t VALUES (1, 0)",
    ] {
        s.query(sql).await.unwrap();
    }
    (t, c, control, s)
}

fn pool(c: &Cluster) -> Arc<Pool> {
    c.shards[0].leader.read().clone().unwrap()
}

async fn eval(c: &Client, code: &str) {
    c.eval(0, code, &[0x90]).await.unwrap();
}

async fn log(c: &Client) -> serde_json::Value {
    let d = c
        .eval(0, "return require('json').encode(fallback_log)", &[0x90])
        .await
        .unwrap()
        .data()
        .unwrap();
    let mut r = Rd::new(&d);
    r.array().unwrap();
    serde_json::from_str(&r.string().unwrap()).unwrap()
}

fn ids(log: &serde_json::Value, kind: &str) -> Vec<u64> {
    log[kind]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_u64().unwrap())
        .collect()
}

fn affected(out: Output) {
    assert!(matches!(out, Output::Ok { affected: 1, .. }), "{out:?}");
}

async fn value(s: &mut Session) -> i64 {
    match s.query("SELECT v FROM t WHERE id = 1").await.unwrap() {
        Output::Rows { rows, .. } => rows[0][0].as_i64().unwrap(),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn warmed_unavailable_autocommit_uses_every_connection() {
    for n in [2, 4] {
        // A genuinely absent module, and the older module's blanket handback.
        for mode in ["missing", "all"] {
            let (_t, c, control, mut s) = setup(n).await;
            eval(
                &control,
                &format!(
                    "fallback_mode = '{mode}'; tarn_storage = {}",
                    if mode == "missing" {
                        "nil"
                    } else {
                        "{ts_autocommit = fallback_native}"
                    }
                ),
            )
            .await;
            let p = pool(&c);
            // Warm every physical connection through the actual unavailable/
            // blanket-handback branch, regardless of maintenance pool traffic.
            for i in 0..n {
                p.next.store(i, Ordering::Relaxed);
                affected(s.query("UPDATE t SET v = v + 1 WHERE id = 1").await.unwrap());
            }
            assert_eq!(p.clients.lock().len(), n);
            assert!(p.clients.lock().iter().all(|(c, _)| c.marked(MARK)));
            eval(
                &control,
                "fallback_log = {native = {}, lua = {}, active = {}}; fallback_hold = true",
            )
            .await;

            // Start each statement's owned RPC worker. Observe actual held
            // Lua calls below: the SQL future may first wait on its worker,
            // before that worker selects the physical connection.
            let mut sessions: Vec<_> = (0..n).map(|i| Session::new(c.clone(), i as u32 + 10)).collect();
            for s in &mut sessions {
                s.use_db("f").unwrap();
            }
            let mut calls: Vec<_> = sessions
                .iter_mut()
                .map(|s| Box::pin(s.query("UPDATE t SET v = v + 1 WHERE id = 1")))
                .collect();
            for call in &mut calls {
                assert!(futures::poll!(call.as_mut()).is_pending());
            }
            let got = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let got = log(&control).await;
                    if ids(&got, "lua").len() == n {
                        break got;
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .unwrap();
            let distinct: BTreeSet<_> = ids(&got, "lua").into_iter().collect();
            assert_eq!(distinct.len(), n, "pool={n}, mode={mode}: {got}");
            assert_eq!(got["active"].as_object().unwrap().len(), n);
            assert!(ids(&got, "native").is_empty(), "warmed native calls are skipped");
            eval(&control, "fallback_hold = false").await;
            for call in calls {
                affected(call.await.unwrap());
            }
            assert_eq!(value(&mut s).await, (2 * n) as i64);
        }
    }
}

#[tokio::test]
async fn matched_handback_keeps_its_physical_client() {
    for mode in ["lua", "all", "missing", "ok"] {
        let (_t, c, control, mut s) = setup(4).await;
        eval(
            &control,
            &format!(
                "fallback_mode = '{mode}'; fallback_log = {{native = {{}}, lua = {{}}, active = {{}}}}; {}",
                if mode == "missing" { "tarn_storage = nil" } else { "" }
            ),
        )
        .await;
        let p = pool(&c);
        // Identify the selected client by its server session, without a pick.
        let client = p.clients.lock()[0].0.clone();
        let r = client.eval(0, "return box.session.id()", &[0x90]).await.unwrap();
        let selected = common::decode_array(&r.data().unwrap())[0].as_i64().unwrap() as u64;
        p.next.store(0, Ordering::Relaxed);
        affected(s.query("UPDATE t SET v = v + 1 WHERE id = 1").await.unwrap());
        let got = log(&control).await;
        assert_eq!(
            ids(&got, "native"),
            if mode == "missing" { vec![] } else { vec![selected] }
        );
        assert_eq!(ids(&got, "lua"), if mode == "ok" { vec![] } else { vec![selected] });
        assert_eq!(p.clients.lock()[0].0.marked(MARK), matches!(mode, "all" | "missing"));
        assert_eq!(value(&mut s).await, 1);
    }
}

#[tokio::test]
async fn readonly_refusals_discover_a_new_client_before_lua() {
    for mode in ["readonly_native", "readonly_lua"] {
        let (_t, c, control, mut s) = setup(2).await;
        eval(
            &control,
            &format!("fallback_mode = '{mode}'; fallback_log = {{native = {{}}, lua = {{}}, active = {{}}}}"),
        )
        .await;
        let old = pool(&c);
        affected(s.query("UPDATE t SET v = v + 1 WHERE id = 1").await.unwrap());
        assert!(!Arc::ptr_eq(&old, &pool(&c)), "read-only pool must be invalidated");
        let got = log(&control).await;
        let native = ids(&got, "native");
        let lua = ids(&got, "lua");
        assert_eq!(native.len(), 1);
        if mode == "readonly_native" {
            assert_eq!(lua.len(), 1);
            assert_ne!(native[0], lua[0]);
        } else {
            assert_eq!(lua.len(), 2);
            assert_eq!(native[0], lua[0]);
            assert_ne!(lua[0], lua[1]);
        }
        assert_eq!(value(&mut s).await, 1);
    }
}

#[tokio::test]
async fn native_conflict_and_lost_reply_never_fall_back() {
    for mode in ["conflict", "hold_after_effect"] {
        let (mut t, _c, control, mut s) = setup(2).await;
        eval(
            &control,
            &format!(
                "fallback_mode = '{mode}'; fallback_hold = true; \
                 fallback_log = {{native = {{}}, lua = {{}}, active = {{}}}}"
            ),
        )
        .await;
        let mut call = Box::pin(s.query("UPDATE t SET v = v + 1 WHERE id = 1"));
        assert!(futures::poll!(call.as_mut()).is_pending());
        if mode == "hold_after_effect" {
            let got = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let got = log(&control).await;
                    if got["committed"] == true {
                        break got;
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .unwrap();
            assert_eq!(ids(&got, "native").len(), 1);
            assert!(ids(&got, "lua").is_empty());
            let r = control
                .eval(0, "return box.space['f.t']:get{1}[2]", &[0x90])
                .await
                .unwrap();
            assert_eq!(common::decode_array(&r.data().unwrap())[0].as_i64(), Some(1));
            // The mutation committed, but the native call has sent no reply.
            t.kill();
            let e = call.await.unwrap_err();
            assert_eq!((e.code, &e.sqlstate), (1180, b"40003"));
        } else {
            let e = call.await.unwrap_err();
            assert_eq!((e.code, &e.sqlstate), (1213, b"40001"));
            let got = log(&control).await;
            assert_eq!(ids(&got, "native").len(), 1);
            assert!(ids(&got, "lua").is_empty());
            assert_eq!(value(&mut s).await, 0);
        }
    }
}
