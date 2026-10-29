//! Exercise the actual handshake, writer and reader against an in-memory peer.
use super::*;
use std::time::Duration;
use tokio::io::DuplexStream;

async fn bounded<F: std::future::Future>(f: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(2), f)
        .await
        .expect("transport did not progress")
}

fn response(rtype: u32, sync: u64, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    encode_packet(&mut out, rtype, sync, 0, |o| o.extend_from_slice(body));
    out
}

fn raw_frame(payload: &[u8]) -> Vec<u8> {
    let mut out = vec![0xce];
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    out
}

async fn request_sync(peer: &mut DuplexStream) -> u64 {
    let mut prefix = [0u8; 5];
    bounded(peer.read_exact(&mut prefix)).await.unwrap();
    assert_eq!(prefix[0], 0xce);
    let mut packet = vec![0; u32::from_be_bytes(prefix[1..].try_into().unwrap()) as usize];
    bounded(peer.read_exact(&mut packet)).await.unwrap();
    let mut p = packet.as_slice();
    let n = mp::read_map_len(&mut p).unwrap();
    let mut sync = None;
    for _ in 0..n {
        if mp::read_uint(&mut p).unwrap() == key::SYNC as u64 {
            sync = Some(mp::read_uint(&mut p).unwrap());
        } else {
            mp::skip_value(&mut p).unwrap();
        }
    }
    sync.unwrap()
}

async fn connecting(auth: bool) -> (tokio::task::JoinHandle<Result<Client, Error>>, DuplexStream) {
    let mut opts = Opts::new("in-memory");
    if auth {
        opts.user = Some("tarn".into());
    }
    connecting_options(opts).await
}

async fn connecting_options(opts: Opts) -> (tokio::task::JoinHandle<Result<Client, Error>>, DuplexStream) {
    let (socket, mut peer) = tokio::io::duplex(4096);
    let (rd, wr) = tokio::io::split(socket);
    let auth = opts.user.is_some();
    let task = tokio::spawn(async move { Client::start(&opts, rd, wr).await });
    let mut greeting = [b' '; 128];
    let version = b"Tarantool 3.9.0 (Binary)";
    greeting[..version.len()].copy_from_slice(version);
    greeting[63] = b'\n';
    let salt = base64::engine::general_purpose::STANDARD.encode([7u8; 32]);
    greeting[64..64 + salt.len()].copy_from_slice(salt.as_bytes());
    greeting[127] = b'\n';
    peer.write_all(&greeting).await.unwrap();
    assert_eq!(request_sync(&mut peer).await, SYNC_ID);
    if auth {
        assert_eq!(request_sync(&mut peer).await, SYNC_AUTH);
    }
    (task, peer)
}

async fn connected() -> (Client, DuplexStream) {
    let (task, mut peer) = connecting(false).await;
    peer.write_all(&response(iproto::OK, SYNC_ID, &[0x80])).await.unwrap();
    (bounded(task).await.unwrap().unwrap(), peer)
}

async fn pending(c: &Client, peer: &mut DuplexStream) -> (u64, oneshot::Receiver<Reply>) {
    let rx = c
        .enqueue(iproto::PING, 0, 128, Class::Normal, |o| o.push(0x80))
        .unwrap();
    (request_sync(peer).await, rx)
}

async fn refused(packet: &[u8]) {
    let (c, mut peer) = connected().await;
    let (_, a) = pending(&c, &mut peer).await;
    let (_, b) = pending(&c, &mut peer).await;
    peer.write_all(packet).await.unwrap();
    assert!(bounded(a).await.is_err(), "invalid reply completed a request");
    assert!(bounded(b).await.is_err(), "unresolved request survived retirement");
    assert!(c.is_closed());
    assert!(matches!(c.ping().await, Err(Error::Closed)));
}

#[test]
fn every_length_encoding_and_fragmented_prefix() {
    let payload = [0x82, key::REQUEST_TYPE, 0, key::SYNC, 3, 0x80];
    let prefixes = [
        vec![6],
        vec![0xcc, 6],
        vec![0xcd, 0, 6],
        vec![0xce, 0, 0, 0, 6],
        vec![0xcf, 0, 0, 0, 0, 0, 0, 0, 6],
    ];
    for prefix in prefixes {
        for cut in 0..prefix.len() {
            assert!(next_frame(&mut BytesMut::from(&prefix[..cut])).unwrap().is_none());
        }
        let mut wire = prefix;
        wire.extend_from_slice(&payload);
        let f = next_frame(&mut BytesMut::from(wire.as_slice())).unwrap().unwrap();
        assert_eq!((f.rtype, f.sync), (iproto::OK, 3));
    }
}

#[test]
fn oversized_headers_refuse_before_reserving_payload_memory() {
    for len in [iproto::BODY_LEN_MAX as u64 + 1, u32::MAX as u64] {
        for width in [4, 8] {
            let mut header = vec![if width == 4 { 0xce } else { 0xcf }];
            header.extend_from_slice(&len.to_be_bytes()[8 - width..]);
            let mut buf = BytesMut::from(header.as_slice());
            let cap = buf.capacity();
            assert!(matches!(next_frame(&mut buf), Err(Error::Protocol(_))));
            assert_eq!(buf.capacity(), cap);
        }
    }
    let mut buf = BytesMut::from(&[0xcf, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff][..]);
    assert!(matches!(next_frame(&mut buf), Err(Error::Protocol(_))));
}

#[tokio::test]
async fn fragmented_coalesced_and_reordered_terminal_replies() {
    let (c, mut peer) = connected().await;
    let (sa, a) = pending(&c, &mut peer).await;
    let (sb, b) = pending(&c, &mut peer).await;
    let (sc, d) = pending(&c, &mut peer).await;
    let mut packets = response(iproto::OK, sc, &[0x81, key::DATA, 0x91, 33]);
    packets.extend(response(iproto::OK, sa, &[0x81, key::DATA, 0x91, 11]));
    packets.extend(response(iproto::OK, sb, &[0x81, key::DATA, 0x91, 22]));
    peer.write_all(&packets[..2]).await.unwrap();
    tokio::task::yield_now().await;
    peer.write_all(&packets[2..7]).await.unwrap();
    tokio::task::yield_now().await;
    peer.write_all(&packets[7..]).await.unwrap();
    for (rx, want) in [(a, 11), (b, 22), (d, 33)] {
        assert_eq!(
            bounded(rx)
                .await
                .unwrap()
                .into_response()
                .unwrap()
                .data()
                .unwrap()
                .as_ref(),
            &[0x91, want]
        );
    }
    assert!(!c.is_closed());
}

#[tokio::test]
async fn chunks_and_cancelled_callers_retain_exact_response_ownership() {
    let (c, mut peer) = connected().await;
    let (sa, mut a) = pending(&c, &mut peer).await;
    let push = response(iproto::CHUNK, sa, &[0x81, key::DATA, 0x91, 7]);
    peer.write_all(&push).await.unwrap();
    assert!(tokio::time::timeout(Duration::from_millis(20), &mut a).await.is_err());
    drop(a);
    let (sb, b) = pending(&c, &mut peer).await;
    assert_ne!(sa, sb);
    let mut packets = response(iproto::OK, sb, &[0x80]);
    packets.extend(push);
    packets.extend(response(iproto::OK, sa, &[0x80]));
    peer.write_all(&packets).await.unwrap();
    assert!(bounded(b).await.unwrap().into_response().is_ok());
    let (sc, d) = pending(&c, &mut peer).await;
    peer.write_all(&response(iproto::OK, sc, &[])).await.unwrap();
    assert!(bounded(d).await.unwrap().into_response().is_ok());
    assert!(!c.is_closed());
    assert!(c.inner.shared.state.lock().waiters.is_empty());
}

#[tokio::test]
async fn matched_terminal_database_error_keeps_connection_usable() {
    let (c, mut peer) = connected().await;
    let task = tokio::spawn({
        let c = c.clone();
        async move { c.ping().await }
    });
    let sync = request_sync(&mut peer).await;
    let body = [0x81, key::ERROR_24, 0xa3, b'b', b'a', b'd'];
    peer.write_all(&response(iproto::TYPE_ERROR | crate::ER_NO_SUCH_SPACE, sync, &body))
        .await
        .unwrap();
    assert!(matches!(
        bounded(task).await.unwrap(),
        Err(Error::Server {
            code: crate::ER_NO_SUCH_SPACE,
            ..
        })
    ));
    let task = tokio::spawn({
        let c = c.clone();
        async move { c.ping().await }
    });
    let sync = request_sync(&mut peer).await;
    peer.write_all(&response(iproto::OK, sync, &[])).await.unwrap();
    assert!(bounded(task).await.unwrap().is_ok());
    assert!(!c.is_closed());
}

#[tokio::test]
async fn invalid_types_and_unowned_fencing_errors_retire_pending_requests() {
    for rtype in [42, iproto::EVENT, 0x1_8000, u32::MAX] {
        refused(&response(rtype, FIRST_SYNC, &[0x80])).await;
    }
    for rtype in [iproto::OK, iproto::CHUNK, iproto::TYPE_ERROR | crate::ER_READONLY] {
        refused(&response(rtype, FIRST_SYNC + 100, &[0x80])).await;
    }
}

#[tokio::test]
async fn duplicate_final_or_push_after_final_retires_other_requests() {
    for second_type in [iproto::OK, iproto::CHUNK] {
        let (c, mut peer) = connected().await;
        let (sa, a) = pending(&c, &mut peer).await;
        let (_, b) = pending(&c, &mut peer).await;
        let mut packets = response(iproto::OK, sa, &[0x80]);
        packets.extend(response(second_type, sa, &[0x80]));
        peer.write_all(&packets).await.unwrap();
        // The first exact terminal may have completed in an earlier socket
        // read; it can never complete the still-pending second request.
        let _ = bounded(a).await;
        assert!(bounded(b).await.is_err());
        assert!(c.is_closed());
    }
}

#[tokio::test]
async fn missing_duplicate_mistyped_headers_and_malformed_bodies_are_fatal() {
    let packets: &[&[u8]] = &[
        &[0x81, key::SYNC, 3, 0x80],
        &[0x81, key::REQUEST_TYPE, 0, 0x80],
        &[0x83, key::REQUEST_TYPE, 0, key::SYNC, 3, key::SYNC, 4, 0x80],
        &[0x83, key::REQUEST_TYPE, 0, key::REQUEST_TYPE, 0, key::SYNC, 3, 0x80],
        &[0x82, key::REQUEST_TYPE, 0xa1, b'x', key::SYNC, 3, 0x80],
        &[0x82, key::REQUEST_TYPE, 0, key::SYNC, 3, 0x91, 1],
        &[0x82, key::REQUEST_TYPE, 0, key::SYNC, 3, 0x81, key::DATA, 0x91],
        &[0x82, key::REQUEST_TYPE, 0, key::SYNC, 3, 0x80, 0x80],
    ];
    for payload in packets {
        refused(&raw_frame(payload)).await;
    }
}

#[tokio::test]
async fn handshake_requires_unique_owned_terminal_replies() {
    for packet in [
        response(42, SYNC_ID, &[0x80]),
        response(iproto::CHUNK, SYNC_ID, &[0x80]),
        response(0, 77, &[0x80]),
    ] {
        let (task, mut peer) = connecting(false).await;
        peer.write_all(&packet).await.unwrap();
        assert!(matches!(bounded(task).await.unwrap(), Err(Error::Protocol(_))));
    }
    let (task, mut peer) = connecting(true).await;
    let mut packets = response(0, SYNC_ID, &[0x80]);
    packets.extend(response(0, SYNC_ID, &[0x80]));
    peer.write_all(&packets).await.unwrap();
    assert!(matches!(bounded(task).await.unwrap(), Err(Error::Protocol(_))));
}

async fn connected_options(opts: Opts) -> (Client, DuplexStream) {
    let (task, mut peer) = connecting_options(opts).await;
    peer.write_all(&response(iproto::OK, SYNC_ID, &[0x80])).await.unwrap();
    (bounded(task).await.unwrap().unwrap(), peer)
}

fn small_options() -> Opts {
    let mut opts = Opts::new("in-memory");
    opts.request_limits = crate::Limits {
        requests: 8,
        bytes: 4096,
        reserved_requests: 2,
        reserved_bytes: 512,
    };
    opts.request_timeout = Duration::from_millis(500);
    opts
}

async fn charges_gone(c: &Client) {
    bounded(async {
        while c.stats().admission.pending != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await;
}

#[tokio::test]
async fn cancelled_requests_keep_both_limits_and_completion_reserve() {
    let (c, mut peer) = connected_options(small_options()).await;
    let mut ids = Vec::new();
    for _ in 0..6 {
        let (id, rx) = pending(&c, &mut peer).await;
        ids.push(id);
        drop(rx);
    }
    for _ in 0..4097 {
        assert!(matches!(
            c.enqueue(iproto::PING, 0, 128, Class::Normal, |_| {}),
            Err(Error::Admission { scope: "connection" })
        ));
    }
    assert_eq!(
        (
            c.stats().admission.pending,
            c.stats().admission.bytes,
            c.stats().abandoned
        ),
        (6, 768, 6)
    );
    assert_eq!(c.stats().requests, 6); // refused requests own no identity
    for _ in 0..2 {
        drop(c.enqueue(iproto::PING, 0, 128, Class::Completion, |_| {}).unwrap());
        request_sync(&mut peer).await;
    }
    assert_eq!(c.stats().admission.pending, 8);
    assert!(c.enqueue(iproto::PING, 0, 128, Class::Completion, |_| {}).is_err());
    peer.write_all(&response(iproto::CHUNK, ids[0], &[0x80])).await.unwrap();
    tokio::task::yield_now().await;
    assert_eq!(c.stats().admission.pending, 8);
    peer.write_all(&response(iproto::OK, ids[0], &[0x80])).await.unwrap();
    bounded(async {
        while c.stats().admission.pending == 8 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(c.stats().admission.pending, 7);
    drop(c.enqueue(iproto::PING, 0, 128, Class::Normal, |_| {}).unwrap());
    c.close();
    charges_gone(&c).await;
}

#[tokio::test]
async fn old_cancelled_head_expires_despite_other_replies_and_compacts_order() {
    let (c, mut peer) = connected_options(small_options()).await;
    let (_, rx) = pending(&c, &mut peer).await;
    drop(rx);
    for _ in 0..100 {
        let (id, rx) = pending(&c, &mut peer).await;
        peer.write_all(&response(iproto::OK, id, &[0x80])).await.unwrap();
        bounded(rx).await.unwrap();
    }
    assert_eq!(c.stats().admission.pending, 1);
    assert!(c.inner.shared.state.lock().order.len() <= 16);
    assert_eq!(
        bounded(peer.read_u8()).await.unwrap_err().kind(),
        std::io::ErrorKind::UnexpectedEof
    );
    assert!(c.is_closed());
    charges_gone(&c).await;
    assert_eq!(c.stats().admission.deadline_retirements, 1);
    assert_eq!(c.stats().requests, 101);
}

#[tokio::test]
async fn slow_peer_bounds_encoded_bytes_and_retires_a_blocked_writer() {
    let mut opts = small_options();
    opts.request_limits.bytes = 64 << 10;
    opts.request_limits.reserved_bytes = 16 << 10;
    let (c, mut peer) = connected_options(opts).await;
    let body = vec![0x80; 16 << 10];
    for _ in 0..2 {
        drop(
            c.enqueue(iproto::EVAL, 0, body.len() + 128, Class::Normal, |out| {
                out.extend_from_slice(&body)
            })
            .unwrap(),
        );
    }
    for _ in 0..1000 {
        assert!(matches!(
            c.enqueue(iproto::EVAL, 0, body.len() + 128, Class::Normal, |_| panic!(
                "encoded refused request"
            )),
            Err(Error::Admission { .. })
        ));
    }
    let s = c.stats();
    assert_eq!((s.admission.pending, s.requests, s.abandoned), (2, 2, 2));
    assert!(s.pending_wire_bytes <= s.admission.bytes && s.admission.peak_bytes <= 64 << 10);
    // Do not read while waiting for the watchdog: the physical writer is
    // blocked by the duplex buffer, rather than just a silent response.
    bounded(async {
        while !c.is_closed() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    charges_gone(&c).await;
    let mut drained = Vec::new();
    bounded(peer.read_to_end(&mut drained)).await.unwrap();
    assert!(drained.len() <= 4096);
    assert_eq!(c.stats().admission.deadline_retirements, 1);
}

#[test]
fn retirement_does_not_refund_until_both_physical_owners_drop() {
    let budget = Budget::new("test", crate::Limits::default()).unwrap();
    let permit = Permit::acquire(&budget, &[], 128, Class::Normal).unwrap();
    let sockets = crate::ConnectionBudget::new(2, 1).unwrap();
    let physical = Arc::new(Physical {
        state: Mutex::new((2, Vec::new())),
        connection: Mutex::new(Some(sockets.acquire(false).unwrap())),
    });
    let a = IoGuard(physical.clone());
    let b = IoGuard(physical.clone());
    physical.retire(std::iter::once(permit));
    assert_eq!(budget.stats().pending, 1);
    drop(a);
    assert_eq!(budget.stats().pending, 1);
    assert_eq!(sockets.stats().open, 1);
    drop(b);
    assert_eq!(budget.stats().pending, 0);
    assert_eq!(sockets.stats().open, 0);
}

#[tokio::test]
async fn socket_budget_covers_handshake_cancellation_and_pinned_handles() {
    let sockets = crate::ConnectionBudget::new(2, 1).unwrap();
    let mut opts = Opts::new("in-memory");
    opts.connections = Some(sockets.clone());
    let (task, mut peer) = connecting_options(opts.clone()).await;
    assert_eq!(sockets.stats().open, 1);
    assert!(sockets.acquire(false).is_err());
    // No capability/ID response. Abandon the connecting caller.
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(sockets.stats().open, 0);
    let mut bytes = Vec::new();
    bounded(peer.read_to_end(&mut bytes)).await.unwrap();

    let (task, mut peer) = connecting_options(opts).await;
    peer.write_all(&response(iproto::OK, SYNC_ID, &[0x80])).await.unwrap();
    let client = bounded(task).await.unwrap().unwrap();
    assert!(client.exclusively_idle());
    let stream_owner = client.clone();
    assert!(!client.exclusively_idle());
    drop(stream_owner);
    let (_, reply) = pending(&client, &mut peer).await;
    drop(reply);
    assert!(!client.exclusively_idle(), "cancelled request pins its socket");
    client.close();
    // Closed handles can survive physical retirement; they consume no socket.
    bounded(async {
        while sockets.stats().open != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(sockets.stats().peak, 1);
    assert_eq!(sockets.traffic().requests, 1);
    assert!(sockets.traffic().bytes_written > 0);
}

#[tokio::test]
async fn response_budget_survives_response_slices_and_refuses_before_large_reserve() {
    let budget = crate::ResponseBudget::new(256);
    let mut opts = Opts::new("in-memory");
    opts.responses = Some(budget.clone());
    opts.response_frame_bytes = 256;
    let (task, mut peer) = connecting_options(opts).await;
    peer.write_all(&response(iproto::OK, SYNC_ID, &[0x80])).await.unwrap();
    let c = bounded(task).await.unwrap().unwrap();
    let (sync, rx) = pending(&c, &mut peer).await;
    let mut body = vec![0x81, key::DATA, 0x91];
    mp::write_str(&mut body, &[b'x'; 120]);
    peer.write_all(&response(iproto::OK, sync, &body)).await.unwrap();
    let reply = bounded(rx).await.unwrap().into_response().unwrap();
    let slice = reply.data().unwrap().slice(0..1);
    drop(reply);
    let charged = budget.stats().bytes;
    assert!(charged >= body.len());
    let (sync, rx) = pending(&c, &mut peer).await;
    peer.write_all(&response(iproto::OK, sync, &body)).await.unwrap();
    assert!(
        bounded(rx).await.is_err(),
        "post-enqueue exhaustion cannot report a successful prefix"
    );
    assert!(c.is_closed());
    assert_eq!(budget.stats().bytes, charged);
    drop(slice);
    assert_eq!(budget.stats().bytes, 0);
    assert_eq!(budget.stats().refusals, 1);

    let mut opts = Opts::new("in-memory");
    opts.response_frame_bytes = 256;
    let (task, mut peer) = connecting_options(opts).await;
    peer.write_all(&response(iproto::OK, SYNC_ID, &[0x80])).await.unwrap();
    let c = bounded(task).await.unwrap().unwrap();
    let (_, rx) = pending(&c, &mut peer).await;
    // A length alone must retire; no advertised payload is sent or allocated.
    peer.write_all(&[0xce, 0x01, 0x00, 0x00, 0x00]).await.unwrap();
    assert!(bounded(rx).await.is_err());
    assert!(c.is_closed());
}

#[tokio::test]
async fn shared_scopes_cover_multiple_connections_and_reserved_calls() {
    let mut opts = small_options();
    let shard = Budget::new(
        "shard",
        crate::Limits {
            requests: 4,
            bytes: 4096,
            reserved_requests: 1,
            reserved_bytes: 512,
        },
    )
    .unwrap();
    let global = Budget::new(
        "global",
        crate::Limits {
            requests: 3,
            bytes: 4096,
            reserved_requests: 1,
            reserved_bytes: 512,
        },
    )
    .unwrap();
    opts.admission = vec![shard.clone(), global.clone()];
    opts.observation_scope = Some(7);
    let (a, mut pa) = connected_options(opts.clone()).await;
    let (b, mut pb) = connected_options(opts).await;
    let (_, ra) = pending(&a, &mut pa).await;
    let (_, rb) = pending(&b, &mut pb).await;
    drop((ra, rb));
    assert_eq!(global.stats().pending, 2);
    assert!(matches!(b.ping().await, Err(Error::Admission { scope: "global" })));
    assert_eq!(
        (b.stats().requests, b.stats().admission.pending, shard.stats().pending),
        (1, 1, 2)
    );
    assert_eq!(global.stats().request_refusals, 1);
    let ac = a.clone();
    let done = tokio::spawn(async move { ac.call_control(0, "tarn_apply", &[0x90]).await });
    let id = request_sync(&mut pa).await;
    assert_eq!(global.stats().pending, 3);
    peer_reply(&mut pa, id).await;
    bounded(done).await.unwrap().unwrap();
    let watches = global.connections();
    assert_eq!(watches.len(), 2);
    assert!(watches.iter().all(|c| c.owner == Some(7)));
    assert_eq!(watches.iter().map(|c| c.abandoned).sum::<usize>(), 2);
    a.close();
    charges_gone(&a).await;
    assert_eq!(global.stats().pending, 1);
    assert_eq!(global.connections().len(), 1); // closed client refs are not a watch leak
    b.close();
    charges_gone(&b).await;
    assert_eq!((shard.stats().pending, global.stats().pending), (0, 0));
    assert!(global.connections().is_empty());
}

async fn peer_reply(peer: &mut DuplexStream, id: u64) {
    peer.write_all(&response(iproto::OK, id, &[0x80])).await.unwrap();
}
