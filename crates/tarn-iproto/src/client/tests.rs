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
    let (socket, mut peer) = tokio::io::duplex(4096);
    let (rd, wr) = tokio::io::split(socket);
    let mut opts = Opts::new("in-memory");
    if auth {
        opts.user = Some("tarn".into());
    }
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
    let rx = c.enqueue(iproto::PING, 0, |o| o.push(0x80)).unwrap();
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
    // The gateway's stricter envelope is checked before any allocation too.
    let mut limited = BytesMut::from(&[0xce, 0, 0x80, 0, 1][..]);
    let capacity = limited.capacity();
    assert!(next_frame_limited(&mut limited, 8 << 20).is_err());
    assert_eq!(limited.capacity(), capacity);
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
