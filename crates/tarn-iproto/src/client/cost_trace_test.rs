//! Compare observations with bytes actually received/sent by the peer.
use super::tests::{bounded, connected_options, response};
use super::*;
use std::collections::BTreeMap;
use std::sync::OnceLock;
use tracing::{Event, Metadata, Subscriber};

#[derive(Clone, Default, Debug)]
struct Observed {
    text: BTreeMap<String, String>,
    numbers: BTreeMap<String, u64>,
    flags: BTreeMap<String, bool>,
}
impl tracing::field::Visit for Observed {
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.numbers.insert(field.name().into(), value);
    }
    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.flags.insert(field.name().into(), value);
    }
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.text.insert(field.name().into(), value.into());
    }
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.text.insert(field.name().into(), format!("{value:?}"));
    }
}

#[derive(Default)]
struct CaptureState {
    wanted: AtomicU64,
    events: Mutex<Vec<Observed>>,
    overflow: AtomicBool,
}
struct Capture(Arc<CaptureState>);
impl Subscriber for Capture {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.target() == "tarn.commit_cost"
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
    fn event(&self, event: &Event<'_>) {
        let mut value = Observed::default();
        event.record(&mut value);
        if value.numbers.get("connection") != Some(&self.0.wanted.load(Ordering::Relaxed)) {
            return;
        }
        let mut events = self.0.events.lock();
        if events.len() == 64 {
            self.0.overflow.store(true, Ordering::Relaxed);
        } else {
            events.push(value);
        }
    }
}

async fn packet(peer: &mut tokio::io::DuplexStream) -> (u64, usize) {
    let mut prefix = [0; 5];
    bounded(peer.read_exact(&mut prefix)).await.unwrap();
    assert_eq!(prefix[0], 0xce);
    let mut payload = vec![0; u32::from_be_bytes(prefix[1..].try_into().unwrap()) as usize];
    bounded(peer.read_exact(&mut payload)).await.unwrap();
    let mut input = payload.as_slice();
    let mut sync = None;
    for _ in 0..mp::read_map_len(&mut input).unwrap() {
        if mp::read_uint(&mut input).unwrap() == key::SYNC as u64 {
            sync = Some(mp::read_uint(&mut input).unwrap());
        } else {
            mp::skip_value(&mut input).unwrap();
        }
    }
    (sync.unwrap(), payload.len() + prefix.len())
}

fn events(capture: &CaptureState, kind: &str) -> Vec<Observed> {
    capture
        .events
        .lock()
        .iter()
        .filter(|event| event.text.get("cost_event").is_some_and(|k| k == kind))
        .cloned()
        .collect()
}

#[tokio::test]
async fn actual_wire_lengths_chunks_cancellation_and_content_exclusion() {
    static CAPTURE: OnceLock<Arc<CaptureState>> = OnceLock::new();
    let capture = CAPTURE.get_or_init(|| {
        let capture = Arc::new(CaptureState::default());
        tracing::subscriber::set_global_default(Capture(capture.clone())).unwrap();
        capture
    });
    let mut opts = Opts::new("in-memory");
    opts.observation_scope = Some(7);
    let (client, mut peer) = connected_options(opts).await;
    capture.wanted.store(client.stats().id, Ordering::Relaxed);
    let secret = "private-transaction-content";
    let mut args = vec![0x91];
    mp::write_str(&mut args, secret.as_bytes());
    let task = tokio::spawn({
        let client = client.clone();
        async move { client.call(257, "tarn_prepare", &args).await }
    });
    let (sync, request_bytes) = packet(&mut peer).await;
    let chunk = response(iproto::CHUNK, sync, &[0x80]);
    assert!(chunk.len() - 5 < 128);
    let chunk: Vec<u8> = [vec![(chunk.len() - 5) as u8], chunk[5..].to_vec()].concat();
    peer.write_all(&chunk[..1]).await.unwrap();
    tokio::task::yield_now().await;
    peer.write_all(&chunk[1..]).await.unwrap();
    bounded(async {
        while events(capture, "frame").is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(!task.is_finished(), "CHUNK was treated as completion");
    assert!(events(capture, "terminal").is_empty());
    let reply = response(iproto::OK, sync, &[0x80]);
    peer.write_all(&reply).await.unwrap();
    bounded(task).await.unwrap().unwrap();
    bounded(async {
        while events(capture, "terminal").is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let request = events(capture, "request");
    assert_eq!(request.len(), 1);
    assert_eq!(request[0].numbers["encoded_bytes"], request_bytes as u64);
    assert_eq!(request[0].numbers["stream"], 257);
    assert_eq!(request[0].numbers["owner"], 7);
    assert_eq!(request[0].text["phase"], "prepare");
    assert_eq!(
        request[0].text["transaction"],
        format!("{:x}", Sha1::digest(secret.as_bytes()))
    );
    let frames = events(capture, "frame");
    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0].numbers["encoded_bytes"], chunk.len() as u64);
    assert_eq!(frames[1].numbers["encoded_bytes"], reply.len() as u64);
    assert_eq!(client.stats().bytes_written, request_bytes as u64);
    assert_eq!(client.stats().bytes_read, (chunk.len() + reply.len()) as u64);
    let terminal = events(capture, "terminal");
    assert!(terminal[0].flags["delivered"]);
    let elapsed = terminal[0].numbers["at_ns"] - request[0].numbers["at_ns"];
    assert!(elapsed.abs_diff(terminal[0].numbers["elapsed_ns"]) <= 1);
    for event in capture.events.lock().iter() {
        assert!(event
            .text
            .values()
            .all(|value| !value.contains("private") && !value.contains("payload")));
    }

    // A cancelled caller cannot erase the encoded request, release its
    // admission early, or turn a drained reply into an acknowledged result.
    capture.events.lock().clear();
    let task = tokio::spawn({
        let client = client.clone();
        async move { client.call(0, "private-function-name", b"\x91\xa7payload").await }
    });
    let (sync, request_bytes) = packet(&mut peer).await;
    task.abort();
    assert!(bounded(task).await.unwrap_err().is_cancelled());
    assert_eq!(client.inner.shared.budget.stats().pending, 1);
    let reply = response(iproto::OK, sync, &[0x80]);
    peer.write_all(&reply).await.unwrap();
    bounded(async {
        while events(capture, "terminal").is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(client.inner.shared.budget.stats().pending, 0);
    assert!(!events(capture, "terminal")[0].flags["delivered"]);
    assert_eq!(
        events(capture, "request")[0].numbers["encoded_bytes"],
        request_bytes as u64
    );
    assert_eq!(events(capture, "request")[0].text["phase"], "other");
    assert_eq!(events(capture, "request")[0].text["transaction"], "unattributed");
    assert!(!capture.overflow.load(Ordering::Relaxed));
    for event in capture.events.lock().iter() {
        assert!(event
            .text
            .values()
            .all(|value| !value.contains("private") && !value.contains("payload")));
    }
    capture.wanted.store(0, Ordering::Relaxed);
    client.close();
}
