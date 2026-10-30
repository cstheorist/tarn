//! Connection admission for the supported storage protocol. This handshake
//! precedes user RPCs on every physical connection, including after failover.
//! It is not a read barrier or evidence about any transaction's outcome.

use crate::mpw::Rd;
use std::time::Duration;
use tarn_common::{SqlError, SqlResult, Value};
use tarn_iproto::Client;

pub const PROTOCOL: u64 = 1;
pub const FORMAT: u64 = 1;

fn incompatible(reason: impl std::fmt::Display) -> SqlError {
    SqlError::unsupported(format!("incompatible Tarn storage: {reason}; see docs/upgrades.md"))
}

fn uint(rd: &mut Rd<'_>) -> SqlResult<u64> {
    match rd.value()? {
        Value::UInt(n) => Ok(n),
        Value::Int(n) if n >= 0 => Ok(n as u64),
        _ => Err(incompatible("expected an unsigned compatibility field")),
    }
}

fn string(rd: &mut Rd<'_>) -> SqlResult<String> {
    match rd.value()? {
        Value::Str(s) => String::from_utf8(s.to_vec()).map_err(incompatible),
        _ => Err(incompatible("expected a string compatibility field")),
    }
}

fn validate(data: &[u8], shard: u32, max_txn_age: f64) -> SqlResult<()> {
    let mut rd = Rd::new(data);
    if rd.array()? != 1 || rd.array()? != 8 || string(&mut rd)? != "ok" {
        return Err(incompatible("invalid compatibility reply"));
    }
    let (protocol, format, id) = (uint(&mut rd)?, uint(&mut rd)?, uint(&mut rd)?);
    let runtime = string(&mut rd)?;
    let layout = uint(&mut rd)?;
    let native = string(&mut rd)?;
    let age = match rd.value()? {
        Value::Float(f) => f,
        Value::Int(n) => n as f64,
        Value::UInt(n) => n as f64,
        _ => return Err(incompatible("invalid transaction age")),
    };
    if !rd.buf.is_empty()
        || protocol != PROTOCOL
        || format != FORMAT
        || id != u64::from(shard)
        || runtime.split('-').next() != Some("3.9.0")
        || layout != 14
        || !matches!(native.as_str(), "ready" | "disabled" | "load_failed" | "abi_mismatch")
        || age != max_txn_age
    {
        return Err(incompatible(format!(
            "protocol={protocol}, format={format}, shard={id}, runtime={runtime}, native={native}/{layout}, max_txn_age={age}"
        )));
    }
    Ok(())
}

pub(crate) async fn admit(client: &Client, shard: u32, max_txn_age: f64, completion: bool) -> SqlResult<()> {
    let result = async {
        let call = async {
            if completion {
                client.call_control(0, "tarn_compatibility", &[0x90]).await
            } else {
                client.call(0, "tarn_compatibility", &[0x90]).await
            }
        };
        let reply = tokio::time::timeout(Duration::from_secs(3), call)
            .await
            .map_err(|_| incompatible("compatibility check timed out"))?
            .map_err(incompatible)?;
        let data = reply
            .data()
            .ok_or_else(|| incompatible("compatibility reply has no data"))?;
        validate(&data, shard, max_txn_age).map_err(incompatible)
    }
    .await;
    if result.is_err() {
        // Retire a cancelled/failed handshake's waiter with its socket.
        client.close();
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mpw::Mp;

    fn report(protocol: u64, format: u64, shard: u64, runtime: &str, native: &str, age: f64) -> Vec<u8> {
        let mut m = Mp::new();
        m.arr(1)
            .arr(8)
            .str("ok")
            .uint(protocol)
            .uint(format)
            .uint(shard)
            .str(runtime)
            .uint(14)
            .str(native)
            .f64(age);
        m.0
    }

    #[test]
    fn admits_supported_protocol_and_explicit_lua_fallbacks() {
        for native in ["ready", "disabled", "load_failed", "abi_mismatch"] {
            assert!(validate(&report(1, 1, 2, "3.9.0-0-g8b9a0f59a9", native, 120.0), 2, 120.0).is_ok());
        }
    }

    #[test]
    fn refuses_wrong_protocol_format_identity_runtime_or_expiry_contract() {
        for data in [
            report(2, 1, 2, "3.9.0", "ready", 120.0),
            report(1, 2, 2, "3.9.0", "ready", 120.0),
            report(1, 1, 1, "3.9.0", "ready", 120.0),
            report(1, 1, 2, "3.10.0", "ready", 120.0),
            report(1, 1, 2, "3.9.0", "pending", 120.0),
            report(1, 1, 2, "3.9.0", "ready", 60.0),
        ] {
            assert!(validate(&data, 2, 120.0).is_err());
        }
    }

    #[test]
    fn refuses_other_native_layout_contracts() {
        for layout in [0, 11, 13, 15] {
            let mut m = Mp::new();
            m.arr(1)
                .arr(8)
                .str("ok")
                .uint(1)
                .uint(1)
                .uint(2)
                .str("3.9.0")
                .uint(layout)
                .str("ready")
                .f64(120.0);
            assert!(validate(&m.0, 2, 120.0).is_err());
        }
    }

    #[test]
    fn refuses_truncated_extended_and_coerced_reports() {
        let data = report(1, 1, 2, "3.9.0", "ready", 120.0);
        for end in 0..data.len() {
            assert!(validate(&data[..end], 2, 120.0).is_err());
        }
        let mut trailing = data.clone();
        trailing.push(0xc0);
        assert!(validate(&trailing, 2, 120.0).is_err());
        let mut text_protocol = Mp::new();
        text_protocol
            .arr(1)
            .arr(8)
            .str("ok")
            .str("1")
            .uint(1)
            .uint(2)
            .str("3.9.0")
            .uint(14)
            .str("ready")
            .f64(120.0);
        assert!(validate(&text_protocol.0, 2, 120.0).is_err());
    }
}
