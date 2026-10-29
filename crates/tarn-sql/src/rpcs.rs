//! Gateway storage-RPC limits and cached diagnostics. Physical connections
//! enforce connection, stable-shard and gateway scopes together. Old pool
//! connections retain the same shard/global budgets until actual retirement.

use crate::cluster::Cluster;
use serde::Deserialize;
use tarn_common::{SqlError, SqlResult, Value};
use tarn_iproto::{AdmissionStats, Limits};

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RpcConfig {
    pub timeout_secs: f64,
    pub connection: Limits,
    pub shard: Limits,
    pub global: Limits,
}

impl Default for RpcConfig {
    fn default() -> Self {
        let connection = Limits::default();
        let scale = |n| Limits {
            requests: connection.requests * n,
            bytes: connection.bytes * n,
            reserved_requests: connection.reserved_requests * n,
            reserved_bytes: connection.reserved_bytes * n,
        };
        Self {
            timeout_secs: 120.0,
            connection,
            shard: scale(2),
            global: scale(4),
        }
    }
}

impl RpcConfig {
    pub(crate) fn validate(&self) -> SqlResult<()> {
        if !self.timeout_secs.is_finite() || !(0.001..=86400.0).contains(&self.timeout_secs) {
            return Err(SqlError::internal(
                "rpc.timeout_secs must be finite and in [0.001,86400]",
            ));
        }
        for limits in [&self.connection, &self.shard, &self.global] {
            // A terminal fence can name MAX_SHARDS u32 IDs plus a bounded
            // transaction ID. Preserve room for that entire control CALL.
            if limits.reserved_bytes < 8192 {
                return Err(SqlError::internal(
                    "RPC completion reserves require at least 8192 bytes",
                ));
            }
            limits.validate().map_err(|e| SqlError::internal(e.to_string()))?;
        }
        Ok(())
    }
}

fn scope_row(kind: &str, shard: Option<u32>, s: AdmissionStats) -> Vec<Value> {
    vec![
        Value::str(kind),
        shard.map_or(Value::Null, |s| Value::UInt(s as u64)),
        Value::Null,
        Value::Null,
        Value::UInt(s.pending as u64),
        Value::UInt(s.bytes as u64),
        Value::Null,
        Value::UInt(s.normal_pending as u64),
        Value::Null,
        Value::Null,
        Value::UInt(s.request_refusals),
        Value::UInt(s.deadline_retirements),
        Value::UInt(s.peak_pending as u64),
        Value::UInt(s.peak_bytes as u64),
        Value::UInt(s.observation_drops),
    ]
}

pub(crate) fn rows(cluster: &Cluster) -> Vec<Vec<Value>> {
    let mut rows = vec![scope_row("global", None, cluster.rpc_global.stats())];
    for s in &cluster.shards {
        rows.push(scope_row("shard", Some(s.id), s.rpc_budget(cluster).stats()));
    }
    for c in cluster.rpc_global.connections() {
        rows.push(vec![
            Value::str("connection"),
            c.owner.map_or(Value::Null, |s| Value::UInt(s as u64)),
            Value::UInt(c.id),
            Value::Int(i64::from(c.closed)),
            Value::UInt(c.admission.pending as u64),
            Value::UInt(c.admission.bytes as u64),
            Value::UInt(c.pending_wire_bytes as u64),
            Value::UInt(c.admission.normal_pending as u64),
            Value::UInt(c.abandoned as u64),
            Value::UInt(c.oldest_ms),
            Value::UInt(c.refusals),
            Value::UInt(c.admission.deadline_retirements),
            Value::UInt(c.admission.peak_pending as u64),
            Value::UInt(c.admission.peak_bytes as u64),
            Value::UInt(0),
        ]);
    }
    rows
}

pub(crate) fn counters(cluster: &Cluster) -> Vec<(&'static str, u64)> {
    let s = cluster.rpc_global.stats();
    let sockets = cluster.connections.stats();
    let responses = cluster.responses.stats();
    let (active, peak) = cluster.fanout.counters();
    vec![
        ("Tarn_storage_sockets", sockets.open as u64),
        ("Tarn_storage_sockets_peak", sockets.peak as u64),
        ("Tarn_storage_connect_attempts", sockets.opened),
        ("Tarn_storage_socket_refusals", sockets.refused),
        ("Tarn_fanout_active", active as u64),
        ("Tarn_fanout_peak", peak as u64),
        ("Tarn_response_bytes", responses.bytes as u64),
        ("Tarn_response_bytes_peak", responses.peak as u64),
        ("Tarn_response_refusals", responses.refusals),
        ("Tarn_rpc_pending", s.pending as u64),
        ("Tarn_rpc_charged_bytes", s.bytes as u64),
        ("Tarn_rpc_peak_pending", s.peak_pending as u64),
        ("Tarn_rpc_peak_bytes", s.peak_bytes as u64),
        ("Tarn_rpc_refusals", s.request_refusals),
        ("Tarn_rpc_deadline_retirements", s.deadline_retirements),
        ("Tarn_rpc_observation_drops", s.observation_drops),
        (
            "Tarn_rpc_oldest_ms",
            cluster
                .rpc_global
                .connections()
                .iter()
                .map(|c| c.oldest_ms)
                .max()
                .unwrap_or(0),
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_and_unsafe_configuration_are_explicit() {
        let default: RpcConfig = serde_json::from_str("{}").unwrap();
        default.validate().unwrap();
        assert_eq!(default.global.requests, 4 * default.connection.requests);
        assert_eq!(default.shard.bytes, 2 * default.connection.bytes);
        for timeout in [0.0, -1.0, f64::NAN, f64::INFINITY, 86401.0] {
            assert!(RpcConfig {
                timeout_secs: timeout,
                ..RpcConfig::default()
            }
            .validate()
            .is_err());
        }
        assert!(serde_json::from_str::<RpcConfig>(r#"{"unknown":1}"#).is_err());
        let bad: RpcConfig = serde_json::from_str(r#"{"connection":{"reserved_requests":0}}"#).unwrap();
        assert!(bad.validate().is_err());
    }

    #[test]
    fn before_enqueue_refusal_is_neither_abort_certification_nor_uncertainty() {
        let e = crate::mpw::from_iproto(tarn_iproto::Error::Admission { scope: "global" });
        assert_eq!((e.code, e.sqlstate), (1205, *b"HY000"));
        let read = crate::mpw::from_iproto_read(tarn_iproto::Error::Closed);
        assert_eq!((read.code, read.sqlstate), (1213, *b"40001"));
        let write = crate::mpw::ambiguous(7, &tarn_iproto::Error::Closed);
        assert_eq!((write.code, write.sqlstate), (1180, *b"40003"));
    }
}
