//! Opt-in transport observations for the distributed commit study (#187).
//! These events contain bounded labels/identities/lengths, never arguments,
//! function names supplied by callers, tuples or error-message contents.
//! Encoded requests are not a claim that all bytes reached the socket.
use sha1::{Digest, Sha1};
use std::sync::OnceLock;
use std::time::Instant;

const TARGET: &str = "tarn.commit_cost";
static EPOCH: OnceLock<Instant> = OnceLock::new();

#[derive(Clone, Copy, Default)]
pub(super) enum Phase {
    #[default]
    Other,
    Prepare,
    Apply,
    Abort,
    DecisionStatus,
    ParticipantStatus,
    Decision,
    Local,
    Autocommit,
    Validate,
    Read,
}

#[derive(Clone, Copy, Default)]
pub(super) struct Call {
    phase: Phase,
    transaction: Option<[u8; 20]>,
}

struct Transaction(Option<[u8; 20]>);
impl std::fmt::Display for Transaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(digest) = self.0 {
            for byte in digest {
                write!(f, "{byte:02x}")?;
            }
            Ok(())
        } else {
            f.write_str("unattributed")
        }
    }
}

impl Call {
    pub(super) fn capture(function: &str, args: &[u8]) -> Option<Self> {
        if !enabled() {
            return None;
        }
        let phase = Phase::call(function);
        let transaction = if matches!(
            phase,
            Phase::Prepare
                | Phase::Apply
                | Phase::Abort
                | Phase::DecisionStatus
                | Phase::ParticipantStatus
                | Phase::Decision
                | Phase::Local
        ) {
            let mut input = args;
            crate::mp::read_array_len(&mut input)
                .ok()
                .filter(|&count| count > 0)
                .and_then(|_| crate::mp::read_str(&mut input).ok())
                .filter(|id| !id.is_empty() && id.len() <= 256)
                .map(|id| Sha1::digest(id).into())
        } else {
            None
        };
        Some(Self { phase, transaction })
    }
}

impl Phase {
    pub(super) fn call(function: &str) -> Self {
        match function {
            "tarn_prepare" => Self::Prepare,
            "tarn_apply" => Self::Apply,
            "tarn_abort" => Self::Abort,
            "tarn_txn_status" => Self::DecisionStatus,
            "tarn_part_status" => Self::ParticipantStatus,
            "tarn_txn_cas" => Self::Decision,
            "tarn_commit_local" | "tarn_commit_rc" | "tarn_storage.ts_commit_local" => Self::Local,
            "tarn_autocommit" | "tarn_storage.ts_autocommit" => Self::Autocommit,
            "tarn_validate" | "tarn_storage.ts_validate" => Self::Validate,
            "tarn_select" | "tarn_select_keys" | "tarn_storage.ts_select" => Self::Read,
            _ => Self::Other,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Other => "other",
            Self::Prepare => "prepare",
            Self::Apply => "apply",
            Self::Abort => "abort",
            Self::DecisionStatus => "decision_status",
            Self::ParticipantStatus => "participant_status",
            Self::Decision => "decision",
            Self::Local => "local",
            Self::Autocommit => "autocommit",
            Self::Validate => "validate",
            Self::Read => "read",
        }
    }
}

pub(super) fn enabled() -> bool {
    let enabled = tracing::enabled!(target: TARGET, tracing::Level::TRACE);
    if enabled {
        EPOCH.get_or_init(Instant::now);
    }
    enabled
}

fn nanos(at: Instant) -> u64 {
    at.checked_duration_since(*EPOCH.get_or_init(Instant::now))
        .unwrap_or_default()
        .as_nanos()
        .min(u64::MAX as u128) as u64
}

pub(super) fn request(
    shared: &super::Shared,
    sync: u64,
    stream: u64,
    kind: u32,
    call: Call,
    bytes: usize,
    born: Instant,
) {
    tracing::trace!(target: TARGET, cost_event = "request", process = std::process::id() as u64,
        connection = shared.id, owner = shared.owner.unwrap_or(0) as u64,
        sync, stream, request_type = kind as u64, phase = call.phase.label(),
        transaction = %Transaction(call.transaction), encoded_bytes = bytes as u64,
        at_ns = nanos(born));
}

pub(super) fn frame(connection: u64, sync: u64, rtype: u32, bytes: usize) {
    tracing::trace!(target: TARGET, cost_event = "frame", process = std::process::id() as u64,
        connection, sync, response_type = rtype as u64, encoded_bytes = bytes as u64,
        at_ns = nanos(Instant::now()));
}

pub(super) fn terminal(connection: u64, sync: u64, born: Instant, delivered: bool) {
    if !enabled() {
        return;
    }
    let now = Instant::now();
    tracing::trace!(target: TARGET, cost_event = "terminal", process = std::process::id() as u64,
        connection, sync, delivered, at_ns = nanos(now),
        elapsed_ns = now.duration_since(born).as_nanos().min(u64::MAX as u128) as u64);
}

pub(super) fn retired(connection: u64, pending: usize, deadline: bool) {
    tracing::trace!(target: TARGET, cost_event = "retired", process = std::process::id() as u64,
        connection, pending = pending as u64, deadline, at_ns = nanos(Instant::now()));
}
