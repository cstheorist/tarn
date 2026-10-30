//! Reconcile the real formatter/client with an independently encoded TCP peer.
use base64::Engine;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tarn_iproto::{Client, Opts};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[derive(Clone)]
struct Log(Arc<Mutex<Vec<u8>>>);
impl Write for Log {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let mut log = self.0.lock().unwrap();
        assert!(log.len() + bytes.len() <= 128 * 1024, "bounded probe log overflow");
        log.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

async fn packet(peer: &mut TcpStream) -> (u64, usize) {
    let mut prefix = [0; 5];
    peer.read_exact(&mut prefix).await.unwrap();
    assert_eq!(prefix[0], 0xce);
    let length = u32::from_be_bytes(prefix[1..].try_into().unwrap()) as usize;
    assert!(length <= 4096);
    let mut packet = vec![0; length];
    peer.read_exact(&mut packet).await.unwrap();
    let mut input = packet.as_slice();
    let mut sync = None;
    for _ in 0..tarn_iproto::mp::read_map_len(&mut input).unwrap() {
        if tarn_iproto::mp::read_uint(&mut input).unwrap() == 1 {
            sync = Some(tarn_iproto::mp::read_uint(&mut input).unwrap());
        } else {
            tarn_iproto::mp::skip_value(&mut input).unwrap();
        }
    }
    (sync.unwrap(), length + 5)
}

fn reply(sync: u64, chunk: bool) -> Vec<u8> {
    assert!(sync < 128);
    // Hand-encoded map {TYPE: OK/CHUNK, SYNC: sync}, then an empty body map.
    let mut payload = vec![0x82, 0x00];
    if chunk {
        payload.extend_from_slice(&[0xcc, 0x80]);
    } else {
        payload.push(0);
    }
    payload.extend_from_slice(&[1, sync as u8, 0x80]);
    assert!(payload.len() < 128);
    let mut packet = vec![payload.len() as u8];
    packet.extend_from_slice(&payload);
    packet
}

#[tokio::test]
async fn default_formatter_trace_matches_actual_peer_and_socket_counters() {
    let log = Log(Arc::new(Mutex::new(Vec::new())));
    let writer = log.clone();
    tracing_subscriber::fmt()
        .with_env_filter("tarn.commit_cost=trace")
        .with_ansi(true)
        .with_writer(move || writer.clone())
        .try_init()
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut peer, _) = listener.accept().await.unwrap();
        let mut greeting = [b' '; 128];
        let version = b"Tarantool 3.9.0 (Binary)";
        greeting[..version.len()].copy_from_slice(version);
        greeting[63] = b'\n';
        let salt = base64::engine::general_purpose::STANDARD.encode([7u8; 32]);
        greeting[64..64 + salt.len()].copy_from_slice(salt.as_bytes());
        greeting[127] = b'\n';
        peer.write_all(&greeting).await.unwrap();
        let (handshake, _) = packet(&mut peer).await;
        peer.write_all(&reply(handshake, false)).await.unwrap();
        let (sync, written) = packet(&mut peer).await;
        let chunk = reply(sync, true);
        let final_reply = reply(sync, false);
        peer.write_all(&chunk[..2]).await.unwrap();
        tokio::task::yield_now().await;
        peer.write_all(&chunk[2..]).await.unwrap();
        peer.write_all(&final_reply).await.unwrap();
        // Keep the physical connection alive until the client explicitly
        // closes it; this is not a transport-error test.
        let mut end = [0; 1];
        assert_eq!(peer.read(&mut end).await.unwrap(), 0);
        (written as u64, (chunk.len() + final_reply.len()) as u64)
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut options = Opts::new(address.to_string());
        options.observation_scope = Some(7);
        let client = Client::connect(&options).await.unwrap();
        // First argument is a private transaction identifier. The log must
        // contain only its hash and a fixed procedure phase label.
        let mut args = vec![0x91];
        tarn_iproto::mp::write_str(&mut args, b"private-probe-transaction");
        client.call(0, "tarn_prepare", &args).await.unwrap();
        let stats = client.stats();
        assert_eq!(stats.admission.pending, 0);
        client.close();
        let (peer_written, peer_read) = peer.await.unwrap();
        assert_eq!(stats.bytes_written, peer_written);
        assert_eq!(stats.bytes_read, peer_read);
        let bytes = log.0.lock().unwrap().clone();
        let text = std::str::from_utf8(&bytes).unwrap();
        assert!(!text.contains("private-probe-transaction"));
        assert!(!text.contains("tarn_prepare"));
        assert!(text.contains("request"));
        assert!(text.contains("terminal"));
        if let Some(directory) = std::env::var_os("TARN_COMMIT_COST_EVIDENCE_DIR") {
            let directory = std::path::PathBuf::from(directory);
            std::fs::create_dir(&directory).unwrap();
            std::fs::write(directory.join("trace.log"), &bytes).unwrap();
            let counters = format!(
                "[{{\"process\":{},\"connection\":{},\"bytes_written\":{},\"bytes_read\":{}}}]\n",
                std::process::id(),
                stats.id,
                peer_written,
                peer_read
            );
            std::fs::write(directory.join("peer-counters.json"), counters).unwrap();
        }
    })
    .await
    .expect("formatter/peer probe did not terminate");
}
