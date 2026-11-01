//! Catalog reloads. A DDL completion must not inherit a catalog read begun
//! before publication, and the poller must not reload a catalog that a
//! statement reloaded.
use super::*;

use super::test_support as common;

#[tokio::test]
async fn ddl_reload_observes_publication_after_an_inflight_refresh() {
    let lua = concat!(env!("CARGO_MANIFEST_DIR"), "/../../lua");
    let t = common::Tarantool::start_with(
        r#"
box.cfg{listen='127.0.0.1:0', memtx_use_mvcc_engine=true,
    memtx_memory=256*1024*1024, log='tarantool.log', checkpoint_interval=0}
package.path=os.getenv('TARN_TEST_LUA_DIR')..'/?.lua;'..package.path
require('tarn').init{shard_id=1, sync=false, native=false, check_yields=true}
local original=tarn_db_list
catalog_hold=true
function tarn_db_list()
    local result=original()
    catalog_read=true
    while catalog_hold do require('fiber').sleep(0.001) end
    return result
end
local f=assert(io.open('listen.tmp','w'))
f:write(tostring(box.info.listen)); f:close()
os.rename('listen.tmp','listen.txt')
"#,
        &[("TARN_TEST_LUA_DIR", lua)],
    );
    let control = t.client().await;
    let cfg = serde_json::from_value(serde_json::json!({
        "shards": [{"id":1, "replicas":[t.addr]}], "pool_size":2
    }))
    .unwrap();
    // No background poller: the test explicitly controls the overlapping read.
    let c = Arc::new(Cluster::configured(cfg).unwrap());
    tokio::time::timeout(Duration::from_secs(10), async {
        let reader = c.clone();
        let old = tokio::spawn(async move { reader.refresh_catalog().await });
        loop {
            let d = control
                .eval(0, "return catalog_read and 1 or 0", &[0x90])
                .await
                .unwrap()
                .data()
                .unwrap();
            let mut r = Rd::new(&d);
            r.array().unwrap();
            if r.value().unwrap().as_i64() == Some(1) {
                break;
            }
            tokio::task::yield_now().await;
        }
        // Publish a database after the first read captured its catalog. DDL
        // then bumps the version and asks for a mandatory reload before ACK.
        control
            .eval(
                0,
                "assert(tarn_db_create('published')[1]=='ok'); tarn_counter_alloc('catalog_version',1)",
                &[0x90],
            )
            .await
            .unwrap();
        let mut refresh = Box::pin(c.refresh_catalog());
        let mut ddl = Box::pin(c.reload_catalog());
        assert!(futures::poll!(refresh.as_mut()).is_pending());
        assert!(futures::poll!(ddl.as_mut()).is_pending());
        control.eval(0, "catalog_hold=false", &[0x90]).await.unwrap();
        old.await.unwrap().unwrap();
        refresh.await.unwrap();
        ddl.await.unwrap();
        assert!(
            c.catalog().has_db("published"),
            "DDL acknowledged with the pre-publication catalog"
        );
        // Overlapping stale-definition refreshes still share the first read.
        assert_eq!(c.reloads.load(Ordering::Acquire), 2);
    })
    .await
    .unwrap();
}

/// The poller reloads only a catalog newer than this server's: one that a
/// statement met reloaded already, and a reload discards every cached plan.
#[tokio::test]
async fn poller_skips_a_catalog_a_statement_reloaded() {
    let lua = concat!(env!("CARGO_MANIFEST_DIR"), "/../../lua");
    let t = common::Tarantool::start_with(
        r#"
box.cfg{listen='127.0.0.1:0', memtx_use_mvcc_engine=true,
    memtx_memory=256*1024*1024, log='tarantool.log', checkpoint_interval=0}
package.path=os.getenv('TARN_TEST_LUA_DIR')..'/?.lua;'..package.path
require('tarn').init{shard_id=1, sync=false, native=false, check_yields=true}
local f=assert(io.open('listen.tmp','w'))
f:write(tostring(box.info.listen)); f:close()
os.rename('listen.tmp','listen.txt')
"#,
        &[("TARN_TEST_LUA_DIR", lua)],
    );
    let control = t.client().await;
    let cfg = serde_json::from_value(serde_json::json!({
        "shards": [{"id":1, "replicas":[t.addr]}], "pool_size":2
    }))
    .unwrap();
    // No background poller: the test takes its polls.
    let c = Arc::new(Cluster::configured(cfg).unwrap());
    let publish =
        |db: &str| format!("assert(tarn_db_create('{db}')[1]=='ok'); tarn_counter_alloc('catalog_version',1)");
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut known = 0;
        control.eval(0, &publish("startup"), &[0x90]).await.unwrap();
        c.reload_catalog().await.unwrap();
        c.poll_catalog(&mut known).await.unwrap();
        assert_eq!(
            c.reloads.load(Ordering::Acquire),
            1,
            "the first poll reloaded the startup catalog"
        );
        // Another server publishes a database, and a statement here meets it
        // before the poller does.
        control.eval(0, &publish("first"), &[0x90]).await.unwrap();
        assert!(c.refresh_if_newer().await.unwrap());
        assert_eq!(c.reloads.load(Ordering::Acquire), 2);
        c.poll_catalog(&mut known).await.unwrap();
        assert_eq!(
            c.reloads.load(Ordering::Acquire),
            2,
            "the poller reloaded the catalog a statement had reloaded"
        );
        // One that no statement met: the poller reloads it.
        control.eval(0, &publish("second"), &[0x90]).await.unwrap();
        c.poll_catalog(&mut known).await.unwrap();
        assert_eq!(c.reloads.load(Ordering::Acquire), 3);
        assert!(c.catalog().has_db("second"));
        c.poll_catalog(&mut known).await.unwrap();
        assert_eq!(c.reloads.load(Ordering::Acquire), 3);
    })
    .await
    .unwrap();
}
