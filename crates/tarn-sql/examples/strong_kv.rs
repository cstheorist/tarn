//! Read a Tarn table through the versioned strong KV API, without a SQL server.
//!
//! Supply credentials with TARN_KV_USER/TARN_KV_PASSWORD or a local JSON file.
//! There is deliberately no command-line password option and no write mode.
//! Seed JSON: [{"id":1,"replicas":["127.0.0.1:3311"]}].
//! Keys JSON: [[7,1],[7,2],[7,1]] (complete logical primary keys).
//! SchemaChanged requires an explicit refresh and a caller decision to resubmit.
//! Stdout is one JSON result with schema, typed rows/errors and connection
//! counters; a failed read exits 1 after printing its error. --help is text.

use std::collections::HashMap;
use std::error::Error;
use tarn_common::Value;
use tarn_sql::kv::{Credentials, KvError, KvOptions, StrongKvClient, TableSchema};

const USAGE: &str = "strong_kv --seeds-file FILE --database DB --table TABLE --keys JSON [--credentials-file FILE]\nCredentials file: {\"user\":\"reader\",\"password\":\"...\"}; otherwise TARN_KV_USER and TARN_KV_PASSWORD are required.\nKeys: [[7,1],[7,2]], integer/string or {\"binary_hex\":\"00ff\"} values. JSON output is the default; read errors exit1 after JSON.\nIndependent strong points only; no cross-shard snapshot, transactions or writes. Authentication does not provide TLS.";

fn key_value(value: serde_json::Value) -> Result<Value, Box<dyn Error>> {
    Ok(match value {
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else if let Some(u) = n.as_u64() {
                Value::UInt(u)
            } else {
                return Err("the example accepts integer or string key values".into());
            }
        }
        serde_json::Value::String(s) => Value::str(&s),
        serde_json::Value::Object(mut object) if object.len() == 1 && object.contains_key("binary_hex") => {
            let s = object
                .remove("binary_hex")
                .and_then(|v| v.as_str().map(str::to_owned))
                .ok_or("binary_hex must be a string")?;
            if s.len() % 2 != 0 || !s.is_ascii() {
                return Err("invalid binary_hex".into());
            }
            let bytes = s
                .as_bytes()
                .as_chunks::<2>()
                .0
                .iter()
                .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16))
                .collect::<Result<Vec<_>, _>>()?;
            Value::Bin(bytes.into())
        }
        _ => return Err("the example accepts integer or string key values".into()),
    })
}

fn output_value(value: Value) -> serde_json::Value {
    use serde_json::json;
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Int(i) => json!(i),
        Value::UInt(u) => json!(u),
        Value::Float(f) if f.is_finite() => json!(f),
        Value::Float(f) => {
            json!({"float":if f.is_nan(){"nan"}else if f.is_sign_negative(){"-infinity"}else{"infinity"}})
        }
        Value::Decimal(d) => json!({"decimal":d}),
        Value::Str(s) => match std::str::from_utf8(&s) {
            Ok(s) => json!(s),
            Err(_) => json!({"string_bytes_hex":hex(&s)}),
        },
        Value::Bin(b) => json!({"binary_hex":hex(&b)}),
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|v| format!("{v:02x}")).collect()
}

fn schema(s: &TableSchema) -> serde_json::Value {
    serde_json::json!({"db":s.database,"name":s.table,"token":{"db":s.token.database,"name":s.token.table,"sid":s.token.space_id,"ver":s.token.definition_version,"definition":s.token.definition_hash,"layout":s.token.layout_hash},"columns":s.columns.iter().map(|c|serde_json::json!({"name":c.name,"type":c.sql_type,"nullable":c.nullable})).collect::<Vec<_>>()})
}

fn stats(client: &StrongKvClient) -> serde_json::Value {
    serde_json::json!({"scope":"current endpoint cache only; not a retired-connection lifetime census","connections":client.connection_stats().iter().map(|s|serde_json::json!({"id":s.id,"closed":s.closed,"requests":s.requests,"replies":s.replies,"bytes_written":s.bytes_written,"bytes_read":s.bytes_read,"read_batches":s.read_batches,"write_batches":s.write_batches,"pending_wire_bytes":s.pending_wire_bytes})).collect::<Vec<_>>()})
}

fn error(error: &KvError) -> serde_json::Value {
    if let Some(e) = error.server() {
        return serde_json::json!({"kind":e.kind.to_string(),"domain":e.domain.to_string(),"code":e.code,"message":e.message,"peer":e.peer,"sqlstate":e.sqlstate});
    }
    if let KvError::Transport(tarn_iproto::Error::Server { code, message }) = error {
        return serde_json::json!({"kind":"transport_server","domain":"tarantool","code":code,"message":message});
    }
    let kind = match error {
        KvError::Invalid(_) => "bad_request",
        KvError::Unsupported(_) => "unsupported",
        KvError::LimitExceeded(_) => "limit_exceeded",
        KvError::SchemaChanged(_) => "schema_changed",
        KvError::Protocol(_) => "protocol",
        KvError::Deadline => "deadline",
        KvError::Unavailable(_) => "unavailable",
        KvError::OwnerChanged(_) => "moved",
        KvError::Transport(_) => "transport",
        KvError::Server(_) => unreachable!(),
    };
    serde_json::json!({"kind":kind,"domain":"client","message":error.to_string()})
}

async fn run() -> Result<Option<serde_json::Value>, Box<dyn Error>> {
    let mut options = HashMap::new();
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        if flag == "--help" {
            println!("{USAGE}");
            return Ok(None);
        }
        if !["--seeds-file", "--database", "--table", "--keys", "--credentials-file"].contains(&flag.as_str()) {
            return Err(format!("unrecognized option; {USAGE}").into());
        }
        let value = args.next().ok_or("missing option value")?;
        if options.insert(flag, value).is_some() {
            return Err("duplicate option".into());
        }
    }
    let required = |flag: &str| options.get(flag).ok_or_else(|| format!("missing {flag}; {USAGE}"));
    let seeds = serde_json::from_slice(&std::fs::read(required("--seeds-file")?)?)?;
    let credentials = if let Some(path) = options.get("--credentials-file") {
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct FileCredentials {
            user: String,
            password: String,
        }
        let c: FileCredentials = serde_json::from_slice(&std::fs::read(path)?)
            .map_err(|_| "invalid credentials JSON (expected user/password strings)")?;
        Credentials {
            user: c.user,
            password: c.password,
        }
    } else {
        Credentials {
            user: std::env::var("TARN_KV_USER")?,
            password: std::env::var("TARN_KV_PASSWORD")?,
        }
    };
    let input: Vec<Vec<serde_json::Value>> = serde_json::from_str(required("--keys")?)?;
    let keys = input
        .into_iter()
        .map(|key| key.into_iter().map(key_value).collect())
        .collect::<Result<Vec<_>, _>>()?;
    let client = StrongKvClient::connect(KvOptions::new(seeds, credentials))?;
    let table = match client.table(required("--database")?, required("--table")?).await {
        Ok(t) => t,
        Err(e) => {
            return Ok(Some(
                serde_json::json!({"version":1,"status":"error","error":error(&e),"stats":stats(&client)}),
            ))
        }
    };
    let result = match client.get_many(&table, &keys).await {
        Ok(r) => r,
        Err(e) => {
            return Ok(Some(
                serde_json::json!({"version":1,"status":"error","schema":schema(table.schema()),"error":error(&e),"stats":stats(&client)}),
            ))
        }
    };
    let names = result.schema.columns.iter().map(|c| c.name.clone()).collect::<Vec<_>>();
    let rows = result
        .rows
        .into_iter()
        .map(|row| {
            row.map(|values| {
                names
                    .iter()
                    .cloned()
                    .zip(values.into_iter().map(output_value))
                    .collect::<serde_json::Map<_, _>>()
            })
        })
        .collect::<Vec<_>>();
    Ok(Some(
        serde_json::json!({"version":1,"status":"ok","consistency":"independent_points","schema":schema(&result.schema),"rows":rows,"stats":stats(&client)}),
    ))
}

#[tokio::main]
async fn main() {
    let result = match run().await {
        Ok(None) => return,
        Ok(Some(v)) => v,
        Err(e) => {
            serde_json::json!({"version":1,"status":"error","error":{"kind":"setup","domain":"client","message":e.to_string()}})
        }
    };
    let failed = result["status"] == "error";
    println!("{result}");
    if failed {
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn typed_json_does_not_round_large_keys_or_replace_bytes() {
        assert_eq!(key_value(serde_json::json!(u64::MAX)).unwrap(), Value::UInt(u64::MAX));
        assert_eq!(output_value(Value::UInt(u64::MAX)).as_u64(), Some(u64::MAX));
        let bytes = Value::Bin(vec![0, 255].into());
        assert_eq!(key_value(output_value(bytes.clone())).unwrap(), bytes);
        assert_eq!(
            output_value(Value::Str(vec![255].into())),
            serde_json::json!({"string_bytes_hex":"ff"})
        );
        assert_eq!(
            output_value(Value::Float(f64::NEG_INFINITY)),
            serde_json::json!({"float":"-infinity"})
        );
        assert_eq!(output_value(Value::Float(-0.0)).to_string(), "-0.0");
    }
}
