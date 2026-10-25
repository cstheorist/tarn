//! Integration tests against a real Tarantool (see tests/common).

mod common;

use std::time::{Duration, Instant};

use bytes::Bytes;
use common::{decode_array, int, s, tuple, Tarantool};
use tarn_common::Value;
use tarn_iproto::*;
use tokio::task::JoinSet;

const TEST: u32 = 512;
const ITER: u32 = 513;
const VALS: u32 = 514;
const KV: u32 = 515;

fn server_code<T: std::fmt::Debug>(r: Result<T, Error>) -> u32 {
    match r {
        Err(Error::Server { code, .. }) => code,
        other => panic!("expected a server error, got {other:?}"),
    }
}

fn key(k: i64) -> Vec<u8> {
    tuple(&[int(k)])
}

fn ids(r: &Response) -> Vec<i64> {
    r.rows()
        .unwrap()
        .iter()
        .map(|row| match row[0] {
            Value::Int(i) => i,
            ref v => panic!("unexpected key {v:?}"),
        })
        .collect()
}

/// Return values of a CALL/EVAL reply.
fn returned(r: &Response) -> Vec<Value> {
    decode_array(&r.data().expect("reply has IPROTO_DATA"))
}

fn update_ops(ops: &[Vec<Value>]) -> Vec<u8> {
    let mut out = Vec::new();
    mp::write_array_len(&mut out, ops.len() as u32);
    for op in ops {
        out.extend(tuple(op));
    }
    out
}

// ---------------------------------------------------------------------------
// Handshake
// ---------------------------------------------------------------------------

#[tokio::test]
async fn greeting_id_auth_and_ping() {
    let t = Tarantool::start();
    let c = t.client().await;
    c.ping().await.unwrap();
    let info = c.server_info();
    assert!(info.version.starts_with(|ch: char| ch.is_ascii_digit()), "{info:?}");
    assert!(info.protocol_version >= 1, "{info:?}");
    assert!(info.features.contains(&iproto::feature::STREAMS), "{info:?}");
    assert!(info.features.contains(&iproto::feature::TRANSACTIONS), "{info:?}");
    assert_eq!(info.auth_type, "chap-sha1");
    let r = c.eval(0, "return box.session.user()", &[]).await.unwrap();
    assert_eq!(returned(&r), vec![s("tarn")]);
    assert_eq!(c.next_stream_id(), 1);
    assert_eq!(c.next_stream_id(), 2);
    assert_ne!(t.client().await.next_stream_id(), 0);
    assert!(!c.is_closed());
    // Raw request API.
    c.request(iproto::PING, 0, Bytes::new()).await.unwrap();
}

/// A reply with ER_READONLY marks the connection stale (its server is no
/// longer the leader); other errors do not, and the connection keeps working.
#[tokio::test]
async fn readonly_reply_marks_connection_stale() {
    let t = Tarantool::start();
    let c = t.client().await;
    assert_eq!(server_code(c.eval(0, "box.error(box.error.NO_SUCH_SPACE, 'x')", &[]).await), 36);
    assert!(!c.is_stale());
    assert_eq!(server_code(c.eval(0, "box.error(box.error.READONLY)", &[]).await), ER_READONLY);
    assert!(c.is_stale() && !c.is_closed());
    c.ping().await.unwrap();
    let d = t.client().await;
    assert!(!d.is_stale());
    d.mark_stale();
    assert!(d.is_stale());
}

#[tokio::test]
async fn bad_password_and_unknown_user() {
    let t = Tarantool::start();
    let mut o = t.opts();
    o.password = Some("wrong".into());
    match Client::connect(&o).await {
        Err(Error::Server { code, message }) => {
            assert_eq!(code, ER_CREDS_MISMATCH);
            assert!(message.contains("credentials"), "{message}");
        }
        other => panic!("{other:?}"),
    }
    let mut o = t.opts();
    o.user = Some("nobody".into());
    assert_eq!(server_code(Client::connect(&o).await), ER_CREDS_MISMATCH);
    // The good password still works afterwards.
    t.client().await.ping().await.unwrap();
}

#[tokio::test]
async fn guest_session_without_user() {
    let t = Tarantool::start();
    let c = Client::connect(&Opts::new(t.addr.clone())).await.unwrap();
    c.ping().await.unwrap();
    let r = c.select(0, TEST, 0, Iter::All, 0, 10, &[]).await;
    assert_eq!(server_code(r), ER_ACCESS_DENIED);
}

#[tokio::test]
async fn connect_failures() {
    // Nothing listening.
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap().to_string();
    drop(l);
    assert!(matches!(Client::connect(&Opts::new(addr)).await, Err(Error::Io(_))));

    // A peer that never sends the greeting: connect_timeout applies.
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut o = Opts::new(l.local_addr().unwrap().to_string());
    o.connect_timeout = Duration::from_millis(200);
    let silent = tokio::spawn(async move {
        let (sock, _) = l.accept().await.unwrap();
        tokio::time::sleep(Duration::from_secs(30)).await;
        drop(sock);
    });
    let t0 = Instant::now();
    assert!(matches!(Client::connect(&o).await, Err(Error::Timeout)));
    assert!(t0.elapsed() < Duration::from_secs(3));
    silent.abort();

    // Something that is not Tarantool.
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let o = Opts::new(l.local_addr().unwrap().to_string());
    let http = tokio::spawn(async move {
        use tokio::io::AsyncWriteExt;
        let (mut sock, _) = l.accept().await.unwrap();
        let mut junk = b"HTTP/1.1 400 Bad Request\r\n".to_vec();
        junk.resize(128, b' ');
        sock.write_all(&junk).await.unwrap();
        tokio::time::sleep(Duration::from_secs(5)).await;
    });
    assert!(matches!(Client::connect(&o).await, Err(Error::Protocol(_))));
    http.abort();
}

// ---------------------------------------------------------------------------
// SELECT
// ---------------------------------------------------------------------------

#[tokio::test]
async fn select_every_iterator_with_limit_and_offset() {
    let t = Tarantool::start();
    let c = t.client().await;
    let sel = |index: u32, it: Iter, offset: u32, limit: u32, k: Vec<u8>| {
        let c = c.clone();
        async move { ids(&c.select(0, ITER, index, it, offset, limit, &k).await.unwrap()) }
    };
    let all = u32::MAX;
    assert_eq!(sel(0, Iter::Eq, 0, all, key(5)).await, vec![5]);
    assert_eq!(sel(0, Iter::Req, 0, all, key(5)).await, vec![5]);
    assert_eq!(sel(0, Iter::All, 0, all, vec![]).await, (1..=10).collect::<Vec<_>>());
    assert_eq!(sel(0, Iter::Lt, 0, all, key(5)).await, vec![4, 3, 2, 1]);
    assert_eq!(sel(0, Iter::Le, 0, all, key(5)).await, vec![5, 4, 3, 2, 1]);
    assert_eq!(sel(0, Iter::Ge, 0, all, key(5)).await, vec![5, 6, 7, 8, 9, 10]);
    assert_eq!(sel(0, Iter::Gt, 0, all, key(5)).await, vec![6, 7, 8, 9, 10]);
    assert_eq!(sel(0, Iter::Eq, 0, all, key(42)).await, Vec::<i64>::new());
    // Limit and offset.
    assert_eq!(sel(0, Iter::Ge, 2, 3, key(3)).await, vec![5, 6, 7]);
    assert_eq!(sel(0, Iter::All, 8, 100, vec![]).await, vec![9, 10]);
    assert_eq!(sel(0, Iter::All, 0, 0, vec![]).await, Vec::<i64>::new());
    assert_eq!(sel(0, Iter::Lt, 1, 2, key(5)).await, vec![3, 2]);
    assert_eq!(sel(0, Iter::Le, 0, 1, key(100)).await, vec![10]);
    // Non-unique secondary index on field 2 (= id % 3): Eq ascends, Req descends.
    assert_eq!(sel(1, Iter::Eq, 0, all, key(1)).await, vec![1, 4, 7, 10]);
    assert_eq!(sel(1, Iter::Req, 0, all, key(1)).await, vec![10, 7, 4, 1]);
    assert_eq!(sel(1, Iter::Eq, 1, 2, key(1)).await, vec![4, 7]);
    assert_eq!(sel(1, Iter::Gt, 0, all, key(1)).await, vec![2, 5, 8]);
    assert_eq!(sel(1, Iter::Lt, 0, all, key(1)).await, vec![9, 6, 3]);
    assert_eq!(sel(1, Iter::Ge, 0, all, key(2)).await, vec![2, 5, 8]);
    assert_eq!(sel(1, Iter::Le, 0, all, key(0)).await, vec![9, 6, 3]);

    // Whole tuples, zero-copy raw tuples, schema version.
    let r = c.select(0, ITER, 0, Iter::Ge, 0, 2, &key(9)).await.unwrap();
    assert_eq!(r.rows().unwrap(), vec![vec![int(9), int(0), s("v9")], vec![int(10), int(1), s("v10")]]);
    let raw = r.tuples().unwrap();
    assert_eq!(raw.len(), 2);
    assert_eq!(decode_array(&raw[1]), vec![int(10), int(1), s("v10")]);
    assert!(r.schema_version > 0);

    // Errors.
    assert_eq!(server_code(c.select(0, 9999, 0, Iter::All, 0, 1, &[]).await), ER_NO_SUCH_SPACE);
    assert_eq!(server_code(c.select(0, ITER, 7, Iter::All, 0, 1, &[]).await), ER_NO_SUCH_INDEX_ID);
    assert_eq!(server_code(c.select(0, ITER, 0, Iter::Eq, 0, 1, &tuple(&[s("x")])).await), ER_KEY_PART_TYPE);
}

// ---------------------------------------------------------------------------
// DML
// ---------------------------------------------------------------------------

#[tokio::test]
async fn insert_replace_update_delete() {
    let t = Tarantool::start();
    let c = t.client().await;
    let r = c.insert(0, TEST, &tuple(&[int(1), int(10), s("a")])).await.unwrap();
    assert_eq!(r.rows().unwrap(), vec![vec![int(1), int(10), s("a")]]);

    match c.insert(0, TEST, &tuple(&[int(1), int(0), s("dup")])).await {
        Err(Error::Server { code, message }) => {
            assert_eq!(code, ER_TUPLE_FOUND);
            assert!(message.contains("Duplicate key exists"), "{message}");
        }
        other => panic!("{other:?}"),
    }

    let r = c.replace(0, TEST, &tuple(&[int(1), int(20), s("b")])).await.unwrap();
    assert_eq!(r.rows().unwrap(), vec![vec![int(1), int(20), s("b")]]);

    // Field numbers are 1-based: {'+', 2, 1} bumps the second field.
    let ops = update_ops(&[vec![s("+"), int(2), int(1)], vec![s("="), int(3), s("x")]]);
    let r = c.update(0, TEST, 0, &key(1), &ops).await.unwrap();
    assert_eq!(r.rows().unwrap(), vec![vec![int(1), int(21), s("x")]]);
    // Negative field numbers count from the end; '!' inserts, '#' deletes.
    let ops = update_ops(&[vec![s("-"), int(-2), int(5)], vec![s("!"), int(4), s("tail")]]);
    let r = c.update(0, TEST, 0, &key(1), &ops).await.unwrap();
    assert_eq!(r.rows().unwrap(), vec![vec![int(1), int(16), s("x"), s("tail")]]);
    let r = c.update(0, TEST, 0, &key(1), &update_ops(&[vec![s("#"), int(4), int(1)]])).await.unwrap();
    assert_eq!(r.rows().unwrap(), vec![vec![int(1), int(16), s("x")]]);
    // No such key: empty data.
    let r = c.update(0, TEST, 0, &key(404), &update_ops(&[vec![s("+"), int(2), int(1)]])).await.unwrap();
    assert!(r.rows().unwrap().is_empty());
    // Arithmetic on a string field.
    let bad = update_ops(&[vec![s("+"), int(3), int(1)]]);
    assert_eq!(server_code(c.update(0, TEST, 0, &key(1), &bad).await), ER_UPDATE_ARG_TYPE);
    // Changing the primary key.
    let bad = update_ops(&[vec![s("="), int(1), int(2)]]);
    assert_eq!(server_code(c.update(0, TEST, 0, &key(1), &bad).await), ER_CANT_UPDATE_PRIMARY_KEY);

    let r = c.delete(0, TEST, 0, &key(1)).await.unwrap();
    assert_eq!(r.rows().unwrap(), vec![vec![int(1), int(16), s("x")]]);
    let r = c.delete(0, TEST, 0, &key(1)).await.unwrap();
    assert!(r.rows().unwrap().is_empty());
    assert!(c.select(0, TEST, 0, Iter::All, 0, 100, &[]).await.unwrap().rows().unwrap().is_empty());

    assert_eq!(server_code(c.insert(0, TEST, &tuple(&[s("str")])).await), ER_FIELD_TYPE);
    assert_eq!(server_code(c.insert(0, 9999, &tuple(&[int(1)])).await), ER_NO_SUCH_SPACE);

    // Without IPROTO_INDEX_BASE the server counts update fields from 0.
    c.replace(0, TEST, &tuple(&[int(3), int(30), s("z")])).await.unwrap();
    let mut body = vec![0x84, iproto::key::SPACE_ID];
    mp::write_uint(&mut body, TEST as u64);
    body.extend([iproto::key::INDEX_ID, 0, iproto::key::KEY]);
    body.extend(key(3));
    body.push(iproto::key::TUPLE);
    body.extend(update_ops(&[vec![s("+"), int(1), int(5)]]));
    let r = c.request(iproto::UPDATE, 0, Bytes::from(body)).await.unwrap();
    assert_eq!(r.rows().unwrap(), vec![vec![int(3), int(35), s("z")]]);
    c.delete(0, TEST, 0, &key(3)).await.unwrap();

    // Raw request with a hand-encoded body.
    let mut body = vec![0x82, iproto::key::SPACE_ID];
    mp::write_uint(&mut body, TEST as u64);
    body.push(iproto::key::TUPLE);
    body.extend(tuple(&[int(2), int(2), s("raw")]));
    let r = c.request(iproto::REPLACE, 0, Bytes::from(body)).await.unwrap();
    assert_eq!(r.rows().unwrap(), vec![vec![int(2), int(2), s("raw")]]);
}

// ---------------------------------------------------------------------------
// CALL / EVAL
// ---------------------------------------------------------------------------

#[tokio::test]
async fn call_and_eval() {
    let t = Tarantool::start();
    let c = t.client().await;

    // Several return values, including a table and a tuple.
    let r = c.call(0, "multi", &[]).await.unwrap();
    let vals = returned(&r);
    assert_eq!(vals.len(), 4);
    assert_eq!(vals[..2], [int(1), s("two")]);
    let Value::Bin(table) = &vals[2] else { panic!("{vals:?}") };
    assert_eq!(decode_array(table), vec![int(3), int(4)]);
    let Value::Bin(tup) = &vals[3] else { panic!("{vals:?}") };
    assert_eq!(decode_array(tup), vec![int(5), s("six")]);
    assert_eq!(r.tuples().unwrap().len(), 4);

    // A function returning a list of tuples: one return value.
    let r = c.call(0, "iter_rows", &[]).await.unwrap();
    let ret = r.tuples().unwrap();
    assert_eq!(ret.len(), 1);
    let mut p: &[u8] = &ret[0];
    let n = mp::read_array_len(&mut p).unwrap();
    assert_eq!(n, 10);
    assert_eq!(mp::decode_tuple(&mut p).unwrap(), vec![int(1), int(1), s("v1")]);

    // Arguments arrive intact.
    let args = vec![int(-5), s("x"), Value::Null, Value::Float(1.5), Value::Decimal("3.14".into()), Value::UInt(u64::MAX)];
    let r = c.call(0, "echo", &tuple(&args)).await.unwrap();
    assert_eq!(returned(&r), args);

    // Dotted names resolve through _G.
    let r = c.call(0, "box.session.user", &[]).await.unwrap();
    assert_eq!(returned(&r), vec![s("tarn")]);

    assert_eq!(server_code(c.call(0, "no_such_function", &[]).await), ER_NO_SUCH_PROC);
    match c.call(0, "fail", &tuple(&[s("boom")])).await {
        Err(Error::Server { code, message }) => {
            assert_eq!(code, ER_PROC_LUA);
            assert!(message.contains("boom"), "{message}");
        }
        other => panic!("{other:?}"),
    }
    match c.call(0, "fail_box", &[]).await {
        Err(Error::Server { code, message }) => {
            assert_eq!(code, 1234);
            assert_eq!(message, "custom failure");
        }
        other => panic!("{other:?}"),
    }

    let r = c.eval(0, "return ...", &tuple(&[int(1), s("a")])).await.unwrap();
    assert_eq!(returned(&r), vec![int(1), s("a")]);
    let r = c.eval(0, "local a, b = ... return a + b", &tuple(&[int(40), int(2)])).await.unwrap();
    assert_eq!(returned(&r), vec![int(42)]);
    let r = c.eval(0, "return box.info.ro", &[0x90]).await.unwrap();
    assert_eq!(returned(&r), vec![int(0)]);
    let r = c.eval(0, "box.space.test:replace{9, 9}", &[]).await.unwrap();
    assert_eq!(returned(&r), Vec::<Value>::new());
    assert_eq!(server_code(c.eval(0, "return +", &[]).await), ER_PROC_LUA);
    assert_eq!(server_code(c.eval(0, "box.begin()", &[]).await), ER_FUNCTION_TX_ACTIVE);
}

// ---------------------------------------------------------------------------
// Streams and transactions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn stream_writes_are_invisible_until_commit() {
    let t = Tarantool::start();
    let a = t.client().await;
    let b = t.client().await;
    let sid = a.next_stream_id();
    a.begin(sid, None, None).await.unwrap();
    a.insert(sid, TEST, &tuple(&[int(7), int(1), s("txn")])).await.unwrap();
    let inside = a.select(sid, TEST, 0, Iter::Eq, 0, 1, &key(7)).await.unwrap();
    assert_eq!(inside.rows().unwrap(), vec![vec![int(7), int(1), s("txn")]]);
    // Not visible to another connection, nor to this one outside the stream.
    assert!(b.select(0, TEST, 0, Iter::Eq, 0, 1, &key(7)).await.unwrap().rows().unwrap().is_empty());
    assert!(a.select(0, TEST, 0, Iter::Eq, 0, 1, &key(7)).await.unwrap().rows().unwrap().is_empty());
    a.commit(sid).await.unwrap();
    let after = b.select(0, TEST, 0, Iter::Eq, 0, 1, &key(7)).await.unwrap();
    assert_eq!(after.rows().unwrap(), vec![vec![int(7), int(1), s("txn")]]);

    // The same stream can run another transaction; rollback discards it.
    a.begin(sid, Some(5.0), Some(TxnIsolation::ReadCommitted)).await.unwrap();
    a.replace(sid, TEST, &tuple(&[int(7), int(2), s("second")])).await.unwrap();
    a.delete(sid, TEST, 0, &key(7)).await.unwrap();
    assert!(a.select(sid, TEST, 0, Iter::Eq, 0, 1, &key(7)).await.unwrap().rows().unwrap().is_empty());
    a.rollback(sid).await.unwrap();
    let after = b.select(0, TEST, 0, Iter::Eq, 0, 1, &key(7)).await.unwrap();
    assert_eq!(after.rows().unwrap(), vec![vec![int(7), int(1), s("txn")]]);

    // Pipelined BEGIN + writes + COMMIT without awaiting in between: the
    // server runs a stream's requests strictly in order.
    let sid2 = a.next_stream_id();
    let (t8, t9) = (tuple(&[int(8)]), tuple(&[int(9)]));
    let (r1, r2, r3, r4) = tokio::join!(
        a.begin(sid2, None, None),
        a.insert(sid2, TEST, &t8),
        a.insert(sid2, TEST, &t9),
        a.commit(sid2),
    );
    r1.unwrap();
    r2.unwrap();
    r3.unwrap();
    r4.unwrap();
    assert_eq!(ids(&b.select(0, TEST, 0, Iter::Ge, 0, 10, &key(8)).await.unwrap()), vec![8, 9]);
}

#[tokio::test]
async fn transaction_errors_isolation_and_timeout() {
    let t = Tarantool::start();
    let a = t.client().await;
    // Transaction control outside a stream is rejected.
    assert_eq!(server_code(a.begin(0, None, None).await), ER_UNABLE_TO_PROCESS_OUT_OF_STREAM);
    assert_eq!(server_code(a.commit(0).await), ER_UNABLE_TO_PROCESS_OUT_OF_STREAM);
    assert_eq!(server_code(a.rollback(0).await), ER_UNABLE_TO_PROCESS_OUT_OF_STREAM);
    // Nested BEGIN.
    let sid = a.next_stream_id();
    a.begin(sid, None, None).await.unwrap();
    assert_eq!(server_code(a.begin(sid, None, None).await), ER_ACTIVE_TRANSACTION);
    a.rollback(sid).await.unwrap();
    // COMMIT/ROLLBACK without a transaction are no-ops, like in net.box.
    a.commit(sid).await.unwrap();
    a.rollback(sid).await.unwrap();

    // Each isolation level reaches the server.
    for iso in [TxnIsolation::Default, TxnIsolation::ReadCommitted, TxnIsolation::ReadConfirmed, TxnIsolation::BestEffort] {
        let sid = a.next_stream_id();
        a.begin(sid, Some(10.0), Some(iso)).await.unwrap();
        a.select(sid, TEST, 0, Iter::All, 0, 1, &[]).await.unwrap();
        a.commit(sid).await.unwrap();
    }
    // Linearizable is accepted by BEGIN, but a standalone memtx space cannot
    // serve linearizable reads -- proof that the level was applied.
    let sid = a.next_stream_id();
    a.begin(sid, None, Some(TxnIsolation::Linearizable)).await.unwrap();
    assert_eq!(server_code(a.select(sid, TEST, 0, Iter::All, 0, 1, &[]).await), ER_UNSUPPORTED);
    a.rollback(sid).await.unwrap();

    // BEGIN timeout: the server aborts the transaction.
    let sid = a.next_stream_id();
    a.begin(sid, Some(0.1), None).await.unwrap();
    a.insert(sid, TEST, &tuple(&[int(1)])).await.unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(server_code(a.insert(sid, TEST, &tuple(&[int(2)])).await), ER_TRANSACTION_TIMEOUT);
    assert_eq!(server_code(a.commit(sid).await), ER_TRANSACTION_TIMEOUT);
    assert!(a.select(0, TEST, 0, Iter::All, 0, 10, &[]).await.unwrap().rows().unwrap().is_empty());
}

#[tokio::test]
async fn read_write_conflict_aborts_stream_transaction() {
    let t = Tarantool::start();
    let a = t.client().await;
    let b = t.client().await;
    b.replace(0, TEST, &tuple(&[int(1), int(0), s("base")])).await.unwrap();

    let sid = a.next_stream_id();
    a.begin(sid, None, None).await.unwrap();
    // The stream reads the key...
    let r = a.select(sid, TEST, 0, Iter::Eq, 0, 1, &key(1)).await.unwrap();
    assert_eq!(r.rows().unwrap(), vec![vec![int(1), int(0), s("base")]]);
    // ...another connection overwrites it...
    b.replace(0, TEST, &tuple(&[int(1), int(1), s("other")])).await.unwrap();
    // ...and the stream's write + commit must fail with a conflict. Memtx MVCC
    // already aborts the transaction when b commits, so the write itself
    // usually reports it; otherwise the commit does.
    let write = a.replace(sid, TEST, &tuple(&[int(1), int(2), s("mine")])).await;
    let write_failed = write.is_err();
    let res = match write {
        Err(e) => Err(e),
        Ok(_) => a.commit(sid).await,
    };
    assert_eq!(server_code(res), ER_TRANSACTION_CONFLICT);
    if write_failed {
        // The aborted transaction is still attached to the stream: COMMIT
        // reports the conflict again, ROLLBACK clears it.
        assert_eq!(server_code(a.commit(sid).await), ER_TRANSACTION_CONFLICT);
    }
    a.rollback(sid).await.unwrap();
    let r = b.select(0, TEST, 0, Iter::Eq, 0, 1, &key(1)).await.unwrap();
    assert_eq!(r.rows().unwrap(), vec![vec![int(1), int(1), s("other")]]);

    // Two streams writing the same key: the second committer conflicts.
    let (s1, s2) = (a.next_stream_id(), b.next_stream_id());
    a.begin(s1, None, None).await.unwrap();
    b.begin(s2, None, None).await.unwrap();
    a.select(s1, TEST, 0, Iter::Eq, 0, 1, &key(1)).await.unwrap();
    b.select(s2, TEST, 0, Iter::Eq, 0, 1, &key(1)).await.unwrap();
    a.replace(s1, TEST, &tuple(&[int(1), int(10)])).await.unwrap();
    b.replace(s2, TEST, &tuple(&[int(1), int(20)])).await.unwrap();
    a.commit(s1).await.unwrap();
    assert_eq!(server_code(b.commit(s2).await), ER_TRANSACTION_CONFLICT);
}

#[tokio::test]
async fn dropping_the_client_rolls_back_its_streams() {
    let t = Tarantool::start();
    let a = t.client().await;
    let b = t.client().await;
    let sid = a.next_stream_id();
    a.begin(sid, None, None).await.unwrap();
    a.insert(sid, TEST, &tuple(&[int(5)])).await.unwrap();
    let stat = "local n = box.stat.net() return n.STREAMS.current, n.CONNECTIONS.current";
    assert_eq!(returned(&b.eval(0, stat, &[]).await.unwrap()), vec![int(1), int(2)]);
    let a2 = a.clone();
    drop(a);
    a2.ping().await.unwrap(); // a clone keeps the connection open
    drop(a2);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let v = returned(&b.eval(0, stat, &[]).await.unwrap());
        if v == vec![int(0), int(1)] {
            break;
        }
        assert!(Instant::now() < deadline, "stream/connection not released: {v:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(b.select(0, TEST, 0, Iter::All, 0, 10, &[]).await.unwrap().rows().unwrap().is_empty());
}

// ---------------------------------------------------------------------------
// Pipelining
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pipelining_20k_concurrent_requests() {
    let t = Tarantool::start();
    let c = t.client().await;
    let t0 = Instant::now();
    let mut set = JoinSet::new();
    for i in 0..20_000i64 {
        let c = c.clone();
        set.spawn(async move {
            match i % 4 {
                0 | 1 => {
                    let k = i % 1000 + 1;
                    let r = c.select(0, KV, 0, Iter::Eq, 0, 1, &key(k)).await.unwrap();
                    assert_eq!(r.rows().unwrap(), vec![vec![int(k), int(2 * k), s(&format!("v{k}"))]]);
                }
                2 => {
                    let args = vec![int(i), s(&format!("x{i}"))];
                    let r = c.call(0, "echo", &tuple(&args)).await.unwrap();
                    assert_eq!(returned(&r), args);
                }
                _ => {
                    let r = c.eval(0, "return ... * 3", &key(i)).await.unwrap();
                    assert_eq!(returned(&r), vec![int(3 * i)]);
                }
            }
        });
    }
    let mut n = 0;
    while let Some(r) = set.join_next().await {
        r.unwrap();
        n += 1;
    }
    assert_eq!(n, 20_000);
    eprintln!("20k concurrent requests in {:?}", t0.elapsed());

    // Many tasks, each with a private key range, doing write-then-read.
    let mut set = JoinSet::new();
    for task in 0..50i64 {
        let c = c.clone();
        set.spawn(async move {
            for j in 0..200i64 {
                let k = 100_000 + task * 1000 + j;
                let r = c.replace(0, KV, &tuple(&[int(k), int(task), s("w")])).await.unwrap();
                assert_eq!(r.rows().unwrap()[0][0], int(k));
                let r = c.select(0, KV, 0, Iter::Eq, 0, 1, &key(k)).await.unwrap();
                assert_eq!(r.rows().unwrap(), vec![vec![int(k), int(task), s("w")]]);
            }
        });
    }
    while let Some(r) = set.join_next().await {
        r.unwrap();
    }
    let r = c.eval(0, "return box.space.kv:count()", &[]).await.unwrap();
    assert_eq!(returned(&r), vec![int(1000 + 50 * 200)]);

    // One task with thousands of in-flight futures (no spawning).
    let futs: Vec<_> = (1..=3000i64).map(|k| {
        let c = &c;
        async move { (k, c.select(0, KV, 0, Iter::Eq, 0, 1, &key(k % 1000 + 1)).await) }
    }).collect();
    for (k, r) in join_all(futs).await {
        let k = k % 1000 + 1;
        assert_eq!(r.unwrap().rows().unwrap()[0][..2], [int(k), int(2 * k)]);
    }
}

/// Minimal `join_all` so the tests need no extra dependency.
async fn join_all<F: std::future::Future>(futs: Vec<F>) -> Vec<F::Output> {
    let mut futs: Vec<_> = futs.into_iter().map(Box::pin).collect();
    let mut out: Vec<Option<F::Output>> = (0..futs.len()).map(|_| None).collect();
    std::future::poll_fn(|cx| {
        let mut pending = false;
        for (i, f) in futs.iter_mut().enumerate() {
            if out[i].is_none() {
                match f.as_mut().poll(cx) {
                    std::task::Poll::Ready(v) => out[i] = Some(v),
                    std::task::Poll::Pending => pending = true,
                }
            }
        }
        if pending {
            std::task::Poll::Pending
        } else {
            std::task::Poll::Ready(())
        }
    })
    .await;
    out.into_iter().map(Option::unwrap).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn large_requests_and_responses() {
    let t = Tarantool::start();
    let c = t.client().await;
    let big = "q".repeat(3 << 20);
    let blob = Value::Bin(Bytes::from(vec![0xabu8; 1 << 20]));
    c.replace(0, VALS, &tuple(&[int(1), s(&big), blob.clone()])).await.unwrap();
    let rows = c.select(0, VALS, 0, Iter::Eq, 0, 1, &key(1)).await.unwrap().rows().unwrap();
    assert_eq!(rows, vec![vec![int(1), s(&big), blob]]);

    c.eval(0, "for i = 2, 20001 do box.space.vals:replace{i, string.rep('x', 100)} end", &[]).await.unwrap();
    // A ~2.3 MB reply while small requests keep flowing on the same connection.
    let c2 = c.clone();
    let small = tokio::spawn(async move {
        for _ in 0..500 {
            c2.ping().await.unwrap();
        }
    });
    let r = c.select(0, VALS, 0, Iter::Gt, 0, u32::MAX, &key(1)).await.unwrap();
    assert!(r.body.len() > 2_000_000);
    let rows = r.rows().unwrap();
    assert_eq!(rows.len(), 20_000);
    assert!(rows.iter().all(|row| row[1] == s(&"x".repeat(100))));
    small.await.unwrap();
}

// ---------------------------------------------------------------------------
// Connection loss
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_death_fails_pending_and_new_requests() {
    let mut t = Tarantool::start();
    let c = t.client().await;
    let mut set = JoinSet::new();
    // Requests parked inside the server...
    for _ in 0..50 {
        let c = c.clone();
        set.spawn(async move { c.call(0, "nap", &tuple(&[int(60)])).await.map(drop) });
    }
    // ...and a steady stream of pipelined selects.
    for _ in 0..8 {
        let c = c.clone();
        set.spawn(async move {
            loop {
                c.select(0, KV, 0, Iter::Eq, 0, 1, &key(1)).await?;
            }
        });
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    let t0 = Instant::now();
    t.kill();
    let results = tokio::time::timeout(Duration::from_secs(10), async {
        let mut v = Vec::new();
        while let Some(r) = set.join_next().await {
            v.push(r.unwrap());
        }
        v
    })
    .await
    .expect("requests hung after the server died");
    eprintln!("all {} requests failed {:?} after the kill", results.len(), t0.elapsed());
    assert_eq!(results.len(), 58);
    for r in results {
        assert!(matches!(r, Err(Error::Closed)), "{r:?}");
    }
    assert!(c.is_closed());
    let t1 = Instant::now();
    assert!(matches!(c.ping().await, Err(Error::Closed)));
    assert!(matches!(c.select(0, KV, 0, Iter::All, 0, 1, &[]).await, Err(Error::Closed)));
    assert!(t1.elapsed() < Duration::from_millis(100));
    // Reconnecting to a dead server fails cleanly.
    assert!(matches!(Client::connect(&t.opts()).await, Err(Error::Io(_))));
}

#[tokio::test]
async fn explicit_close_fails_pending_requests() {
    let t = Tarantool::start();
    let c = t.client().await;
    let c2 = c.clone();
    let pending = tokio::spawn(async move { c2.call(0, "nap", &tuple(&[Value::Float(30.0)])).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    c.close();
    let r = tokio::time::timeout(Duration::from_secs(2), pending).await.expect("pending request hung").unwrap();
    assert!(matches!(r, Err(Error::Closed)), "{r:?}");
    assert!(c.is_closed());
    assert!(matches!(c.ping().await, Err(Error::Closed)));
    t.client().await.ping().await.unwrap();
}

// ---------------------------------------------------------------------------
// MessagePack values through the server
// ---------------------------------------------------------------------------

#[tokio::test]
async fn every_value_variant_round_trips_through_tarantool() {
    let t = Tarantool::start();
    let c = t.client().await;
    let mut cases = vec![
        Value::Null,
        int(0),
        int(127),
        int(128),
        int(-1),
        int(-33),
        int(-40_000),
        int(i64::MIN),
        int(i64::MAX),
        Value::UInt(u64::MAX),
        Value::UInt(1 << 63),
        Value::Float(0.0),
        Value::Float(-1.25),
        Value::Float(1e300),
        Value::Float(f64::MIN_POSITIVE),
        Value::Float(f64::NEG_INFINITY),
        Value::Decimal("-12.340".into()),
        Value::Decimal("0".into()),
        Value::Decimal("0.00".into()),
        Value::Decimal("1000".into()),
        Value::Decimal("0.0000001".into()),
        Value::Decimal("123456789012345678901234567890.123456".into()),
        Value::Decimal("-99999999999999999999999999999999999.999".into()),
        Value::Decimal("9".repeat(mp::DECIMAL_MAX_DIGITS)),
        s(""),
        s("héllo wörld ✓"),
        s(&"a".repeat(31)),
        s(&"b".repeat(32)),
        s(&"c".repeat(255)),
        s(&"d".repeat(256)),
        s(&"e".repeat(65_535)),
        s(&"f".repeat(65_536)),
        Value::Bin(Bytes::new()),
        Value::Bin(Bytes::from_static(&[0])),
        Value::Bin(Bytes::from(vec![0xffu8; 300])),
        Value::Bin(Bytes::from(vec![1u8; 70_000])),
    ];
    // Every case in one tuple too.
    let all_in_one = cases.clone();
    for (i, v) in cases.iter().enumerate() {
        let id = i as i64 + 1;
        let r = c.replace(0, VALS, &tuple(&[int(id), v.clone()])).await.unwrap();
        assert_eq!(r.rows().unwrap()[0][1], *v);
    }
    for (i, v) in cases.iter().enumerate() {
        let id = i as i64 + 1;
        let r = c.select(0, VALS, 0, Iter::Eq, 0, 1, &key(id)).await.unwrap();
        assert_eq!(r.rows().unwrap(), vec![vec![int(id), v.clone()]], "case {i}");
        // Copying decoder agrees with the zero-copy one.
        let raw = r.tuples().unwrap();
        let mut p: &[u8] = &raw[0];
        assert_eq!(mp::decode_tuple(&mut p).unwrap(), vec![int(id), v.clone()]);
        if let Value::Decimal(text) = v {
            // Tarantool sees a real decimal equal to our text.
            let ty = c.call(0, "vals_type", &key(id)).await.unwrap();
            assert_eq!(returned(&ty), vec![s("decimal")], "{text}");
            let eq = c.call(0, "vals_decimal_eq", &tuple(&[int(id), s(text)])).await.unwrap();
            assert_eq!(returned(&eq), vec![int(1)], "{text}");
        }
    }
    let mut row = vec![int(1000)];
    row.extend(all_in_one);
    c.replace(0, VALS, &tuple(&row)).await.unwrap();
    let r = c.select(0, VALS, 0, Iter::Eq, 0, 1, &key(1000)).await.unwrap();
    assert_eq!(r.rows().unwrap(), vec![row]);

    // NaN survives (compare bits).
    c.replace(0, VALS, &tuple(&[int(2000), Value::Float(f64::NAN)])).await.unwrap();
    let r = c.select(0, VALS, 0, Iter::Eq, 0, 1, &key(2000)).await.unwrap();
    let Value::Float(f) = r.rows().unwrap()[0][1] else { panic!() };
    assert!(f.is_nan());
    cases.clear();
}

#[tokio::test]
async fn decimals_and_datetimes_written_by_tarantool() {
    let t = Tarantool::start();
    let c = t.client().await;
    let nines = "9".repeat(mp::DECIMAL_MAX_DIGITS);
    let cases: Vec<(&str, String)> = vec![
        ("-12.340", "-12.340".into()),
        ("0", "0".into()),
        ("0.00", "0.00".into()),
        ("-0", "0".into()),
        ("1000", "1000".into()),
        ("1E+3", "1000".into()),
        ("1.5e3", "1500".into()),
        ("123456789012345678901234567890.123456", "123456789012345678901234567890.123456".into()),
        ("0.0000001", "0.0000001".into()),
        ("-1", "-1".into()),
        ("99", "99".into()),
        ("1234", "1234".into()),
        ("-0.5", "-0.5".into()),
        ("3.1415926535897932384626433832795028841", "3.1415926535897932384626433832795028841".into()),
        ("1e-30", "0.000000000000000000000000000001".into()),
        ("-4.2E+10", "-42000000000".into()),
        (nines.as_str(), nines.clone()),
    ];
    let put = "local id, s = ... box.space.vals:replace{id, require('decimal').new(s)}";
    for (i, (lit, canon)) in cases.iter().enumerate() {
        let id = i as i64 + 1;
        c.eval(0, put, &tuple(&[int(id), s(lit)])).await.unwrap();
        let r = c.select(0, VALS, 0, Iter::Eq, 0, 1, &key(id)).await.unwrap();
        assert_eq!(r.rows().unwrap()[0][1], Value::Decimal(canon.clone()), "decimal.new('{lit}')");
        // Our encoding of the canonical text equals Tarantool's value.
        let mine = id + 100;
        c.replace(0, VALS, &tuple(&[int(mine), Value::Decimal(canon.clone())])).await.unwrap();
        let eq = c
            .eval(0, "local a, b = ... return box.space.vals:get(a)[2] == box.space.vals:get(b)[2]", &tuple(&[int(id), int(mine)]))
            .await
            .unwrap();
        assert_eq!(returned(&eq), vec![int(1)], "{lit} vs {canon}");
    }
    // Arithmetic results.
    let r = c.eval(0, "local d = require('decimal') return d.new('1.10') * 3, d.new(1) / 8, -d.new('0.10')", &[]).await.unwrap();
    assert_eq!(
        returned(&r),
        vec![Value::Decimal("3.30".into()), Value::Decimal("0.125".into()), Value::Decimal("-0.10".into())]
    );

    // Datetimes: UTC, timezone ignored, microseconds when non-zero.
    let r = c
        .eval(
            0,
            "local dt = require('datetime') return \
             dt.new{year=2024, month=2, day=29, hour=13, min=14, sec=15, nsec=123456789, tzoffset=180}, \
             dt.new{year=1969, month=12, day=31, hour=23, min=59, sec=59}, \
             dt.new{year=2000, month=1, day=1, nsec=1000}, \
             dt.new{year=2026, month=10, day=24, hour=18, tz='Europe/Moscow'}",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        returned(&r),
        vec![
            s("2024-02-29 10:14:15.123456"),
            s("1969-12-31 23:59:59"),
            s("2000-01-01 00:00:00.000001"),
            s("2026-10-24 15:00:00"),
        ]
    );
    c.eval(0, "box.space.vals:replace{500, require('datetime').new{year=2001, month=9, day=9, hour=1, min=46, sec=40}}", &[])
        .await
        .unwrap();
    let r = c.select(0, VALS, 0, Iter::Eq, 0, 1, &key(500)).await.unwrap();
    assert_eq!(r.rows().unwrap()[0][1], s("2001-09-09 01:46:40"));

    // Booleans, nil, nested containers, UUID and interval extensions.
    let r = c
        .eval(
            0,
            "return true, false, nil, {1, {2}}, {a = 1}, \
             require('uuid').fromstr('f81d4fae-7dec-11d0-a765-00a0c91e6bf6'), \
             require('datetime').interval.new{year = 1}",
            &[],
        )
        .await
        .unwrap();
    let v = returned(&r);
    assert_eq!(v[..3], [int(1), int(0), Value::Null]);
    assert_eq!(v[3], Value::Bin(Bytes::from_static(&[0x92, 0x01, 0x91, 0x02])));
    assert_eq!(v[4], Value::Bin(Bytes::from_static(&[0x81, 0xa1, b'a', 0x01])));
    let mut uuid = vec![0xd8, 0x02];
    uuid.extend([0xf8, 0x1d, 0x4f, 0xae, 0x7d, 0xec, 0x11, 0xd0, 0xa7, 0x65, 0x00, 0xa0, 0xc9, 0x1e, 0x6b, 0xf6]);
    assert_eq!(v[5], Value::Bin(Bytes::from(uuid)));
    assert_eq!(v[6], Value::Bin(Bytes::from_static(&[0xc7, 0x05, 0x06, 0x02, 0x00, 0x01, 0x08, 0x01])));
}
