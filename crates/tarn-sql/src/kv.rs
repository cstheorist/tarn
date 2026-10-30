//! Versioned, read-only strong point access to Tarn SQL tables.
//!
//! A table handle freezes its schema. Ownership may be rediscovered, but a
//! definition change requires an explicit refresh by the caller. Independent
//! points do not promise a cross-shard snapshot or an execution order.

use crate::catalog::{self, TableDef};
use crate::cluster::ShardConfig;
use crate::mpw::{Mp, Rd};
use bytes::Bytes;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use tarn_common::Value;
use tarn_iproto::{Client, Opts};
use tokio::time::{timeout_at, Instant};

pub type KvResult<T> = Result<T, KvError>;
pub type Row = Vec<Value>;

/// No write API or ambient transaction is exposed by this client.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Consistency {
    IndependentPoints,
}

#[derive(Debug, thiserror::Error)]
pub enum KvError {
    #[error("invalid strong KV request: {0}")]
    Invalid(String),
    #[error("unsupported strong KV operation: {0}")]
    Unsupported(String),
    #[error("strong KV limit exceeded: {0}")]
    LimitExceeded(String),
    #[error("table schema changed; explicitly refresh the table handle: {0}")]
    SchemaChanged(#[source] KvCause),
    #[error("strong KV protocol error: {0}")]
    Protocol(String),
    #[error("strong KV read deadline elapsed")]
    Deadline,
    #[error("strong KV owner discovery exhausted: {0}")]
    Unavailable(String),
    #[error("strong KV owner changed: {0}")]
    OwnerChanged(#[source] KvCause),
    #[error(transparent)]
    Server(#[from] KvServerError),
    #[error(transparent)]
    Transport(#[from] tarn_iproto::Error),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KvErrorDomain {
    Contract,
    Mysql,
    Tarantool,
}

impl fmt::Display for KvErrorDomain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Contract => "contract",
            Self::Mysql => "mysql",
            Self::Tarantool => "tarantool",
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KvErrorKind {
    BadRequest,
    Unsupported,
    LimitExceeded,
    AmbientTransaction,
    SchemaChanged,
    Moved,
    AccessDenied,
    Unavailable,
    ReadError,
}

impl fmt::Display for KvErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::BadRequest => "bad_request",
            Self::Unsupported => "unsupported",
            Self::LimitExceeded => "limit_exceeded",
            Self::AmbientTransaction => "ambient_transaction",
            Self::SchemaChanged => "schema_changed",
            Self::Moved => "moved",
            Self::AccessDenied => "access_denied",
            Self::Unavailable => "unavailable",
            Self::ReadError => "read_error",
        })
    }
}

/// Codes retain their server namespace; SQLSTATE is absent unless supplied.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{domain} {kind} ({code}): {message}")]
pub struct KvServerError {
    pub kind: KvErrorKind,
    pub domain: KvErrorDomain,
    pub code: u32,
    pub message: String,
    pub peer: Option<u32>,
    pub sqlstate: Option<String>,
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum KvCause {
    #[error("{0}")]
    Local(String),
    #[error(transparent)]
    Server(#[from] KvServerError),
}
impl From<String> for KvCause {
    fn from(s: String) -> Self {
        Self::Local(s)
    }
}
impl From<&str> for KvCause {
    fn from(s: &str) -> Self {
        Self::Local(s.into())
    }
}

impl KvError {
    pub fn server(&self) -> Option<&KvServerError> {
        match self {
            Self::Server(e) | Self::SchemaChanged(KvCause::Server(e)) | Self::OwnerChanged(KvCause::Server(e)) => {
                Some(e)
            }
            _ => None,
        }
    }
}

/// Authentication is always supplied explicitly. Debug output omits the secret.
#[derive(Clone)]
pub struct Credentials {
    pub user: String,
    pub password: String,
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Credentials")
            .field("user", &self.user)
            .field("password", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, Debug)]
pub struct KvOptions {
    pub seeds: Vec<ShardConfig>,
    pub credentials: Credentials,
    /// Covers connection, greeting and authentication, also bounded by the read.
    pub connect_timeout: Duration,
    /// One absolute deadline includes every owner lookup and read attempt.
    pub read_timeout: Duration,
    pub max_read_attempts: usize,
    pub request_limits: tarn_iproto::Limits,
}

impl KvOptions {
    pub fn new(seeds: Vec<ShardConfig>, credentials: Credentials) -> Self {
        Self {
            seeds,
            credentials,
            connect_timeout: Duration::from_secs(3),
            read_timeout: Duration::from_secs(5),
            max_read_attempts: 3,
            request_limits: Default::default(),
        }
    }

    fn validate(&self) -> KvResult<()> {
        if self.credentials.user.is_empty()
            || self.seeds.is_empty()
            || self.seeds.len() > 1024
            || self.connect_timeout.is_zero()
            || self.read_timeout.is_zero()
            || self.read_timeout > Duration::from_secs(3600)
            || self.max_read_attempts == 0
            || self.max_read_attempts > 32
        {
            return Err(KvError::Invalid("empty credentials/seeds or invalid limits".into()));
        }
        let mut ids = HashSet::new();
        for seed in &self.seeds {
            if seed.id == 0
                || !ids.insert(seed.id)
                || seed.replicas.is_empty()
                || seed.replicas.len() > 32
                || seed
                    .replicas
                    .iter()
                    .any(|s| s.is_empty() || s.len() > 2048 || s.contains('@') || s.chars().any(char::is_whitespace))
            {
                return Err(KvError::Invalid("invalid or duplicate seed shard".into()));
            }
        }
        self.request_limits.validate()?;
        Ok(())
    }
}

/// Names and SQL types correspond to the logical row, not physical tuple slots.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnInfo {
    pub name: String,
    pub sql_type: String,
    pub nullable: bool,
}

#[derive(Clone, Debug)]
pub struct TableSchema {
    pub database: String,
    pub table: String,
    pub columns: Vec<ColumnInfo>,
    pub token: TableToken,
    definition: Arc<TableDef>,
}

/// Opaque server hashes are validated and echoed, not independently computed.
/// Authentication here does not add transport encryption or TLS verification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableToken {
    pub database: String,
    pub table: String,
    pub space_id: u32,
    pub definition_version: u64,
    pub definition_hash: String,
    pub layout_hash: String,
}

impl TableToken {
    fn encode(&self, m: &mut Mp) {
        m.map(6)
            .str("db")
            .str(&self.database)
            .str("name")
            .str(&self.table)
            .str("sid")
            .uint(self.space_id as u64)
            .str("ver")
            .uint(self.definition_version)
            .str("definition")
            .str(&self.definition_hash)
            .str("layout")
            .str(&self.layout_hash);
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Ownership {
    shard: u32,
    count: u32,
    epoch: u64,
    membership: u64,
}

impl Ownership {
    fn encode(&self, m: &mut Mp) {
        m.map(4)
            .str("shard_id")
            .uint(self.shard as u64)
            .str("bucket_count")
            .uint(self.count as u64)
            .str("bucket_epoch")
            .uint(self.epoch)
            .str("membership_version")
            .uint(self.membership);
    }
}

type WireMap<'a> = HashMap<&'a str, &'a [u8]>;

fn protocol(message: impl Into<String>) -> KvError {
    KvError::Protocol(message.into())
}

fn raw_value<'a>(p: &mut &'a [u8]) -> KvResult<&'a [u8]> {
    let before = *p;
    tarn_iproto::mp::skip_value(p)?;
    Ok(&before[..before.len() - p.len()])
}

fn fields<'a>(bytes: &'a [u8], allowed: &[&str], required: &[&str]) -> KvResult<WireMap<'a>> {
    let mut p = bytes;
    let n = tarn_iproto::mp::read_map_len(&mut p)? as usize;
    if n > allowed.len() {
        return Err(protocol("too many map fields"));
    }
    let mut result = HashMap::with_capacity(n);
    for _ in 0..n {
        let key =
            std::str::from_utf8(tarn_iproto::mp::read_str(&mut p)?).map_err(|_| protocol("non-UTF8 field name"))?;
        if !allowed.contains(&key) || result.contains_key(key) {
            return Err(protocol("unknown or duplicate map field"));
        }
        let value = raw_value(&mut p)?;
        result.insert(key, value);
    }
    if !p.is_empty() || required.iter().any(|key| !result.contains_key(key)) {
        return Err(protocol("incomplete map or trailing bytes"));
    }
    Ok(result)
}

fn text(bytes: &[u8]) -> KvResult<String> {
    let mut p = bytes;
    let value = tarn_iproto::mp::read_str(&mut p)?;
    if !p.is_empty() {
        return Err(protocol("trailing string bytes"));
    }
    std::str::from_utf8(value)
        .map(str::to_owned)
        .map_err(|_| protocol("non-UTF8 string"))
}

fn uint(bytes: &[u8]) -> KvResult<u64> {
    let mut p = bytes;
    let n = tarn_iproto::mp::read_uint(&mut p)?;
    if !p.is_empty() {
        return Err(protocol("trailing integer bytes"));
    }
    Ok(n)
}

fn u32_value(bytes: &[u8]) -> KvResult<u32> {
    u32::try_from(uint(bytes)?).map_err(|_| protocol("integer exceeds u32"))
}

fn token(bytes: &[u8]) -> KvResult<TableToken> {
    let f = fields(
        bytes,
        &["db", "name", "sid", "ver", "definition", "layout"],
        &["db", "name", "sid", "ver", "definition", "layout"],
    )?;
    let t = TableToken {
        database: text(f["db"])?,
        table: text(f["name"])?,
        space_id: u32_value(f["sid"])?,
        definition_version: uint(f["ver"])?,
        definition_hash: text(f["definition"])?,
        layout_hash: text(f["layout"])?,
    };
    if t.database.is_empty()
        || t.table.is_empty()
        || t.database.len() > 1024
        || t.table.len() > 1024
        || t.space_id == 0
        || [&t.definition_hash, &t.layout_hash]
            .iter()
            .any(|s| s.len() != 64 || !s.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)))
    {
        return Err(protocol("invalid table token"));
    }
    Ok(t)
}

fn ownership(bytes: &[u8]) -> KvResult<Ownership> {
    let f = fields(
        bytes,
        &["shard_id", "bucket_count", "bucket_epoch", "membership_version"],
        &["shard_id", "bucket_count", "bucket_epoch", "membership_version"],
    )?;
    let o = Ownership {
        shard: u32_value(f["shard_id"])?,
        count: u32_value(f["bucket_count"])?,
        epoch: uint(f["bucket_epoch"])?,
        membership: uint(f["membership_version"])?,
    };
    if o.shard == 0 || !(1..=crate::buckets::MAX_BUCKETS).contains(&o.count) {
        return Err(protocol("invalid ownership token"));
    }
    Ok(o)
}

fn envelope<'a>(data: &'a [u8], success: &[&str]) -> KvResult<WireMap<'a>> {
    if data.len() > MAX_SCHEMA_BYTES {
        return Err(protocol("oversized KV reply"));
    }
    let mut p = data;
    if tarn_iproto::mp::read_array_len(&mut p)? != 1 {
        return Err(protocol("CALL must return one value"));
    }
    let raw = raw_value(&mut p)?;
    if !p.is_empty() {
        return Err(protocol("trailing CALL reply bytes"));
    }
    // Inspect only the envelope tags before choosing its exact allowed fields.
    let mut allowed = success.to_vec();
    allowed.push("error");
    let f = fields(raw, &allowed, &["version", "status"])?;
    if uint(f["version"])? != 1 {
        return Err(KvError::Unsupported("unknown KV API version".into()));
    }
    match text(f["status"])?.as_str() {
        "ok" => {
            if f.contains_key("error") || success.iter().any(|k| !f.contains_key(k)) {
                return Err(protocol("incomplete success envelope"));
            }
            if text(f["consistency"])? != "independent_points" {
                return Err(protocol("unexpected consistency"));
            }
            Ok(f)
        }
        "error" => {
            if f.len() != 3 {
                return Err(protocol("partial data in error reply"));
            }
            let e = fields(
                f["error"],
                &["kind", "domain", "code", "message", "peer", "sqlstate"],
                &["kind", "domain", "code", "message"],
            )?;
            let kind = match text(e["kind"])?.as_str() {
                "bad_request" => KvErrorKind::BadRequest,
                "unsupported" => KvErrorKind::Unsupported,
                "limit_exceeded" => KvErrorKind::LimitExceeded,
                "ambient_transaction" => KvErrorKind::AmbientTransaction,
                "schema_changed" => KvErrorKind::SchemaChanged,
                "moved" => KvErrorKind::Moved,
                "access_denied" => KvErrorKind::AccessDenied,
                "unavailable" => KvErrorKind::Unavailable,
                "read_error" => KvErrorKind::ReadError,
                _ => return Err(protocol("unknown error kind")),
            };
            let domain = match text(e["domain"])?.as_str() {
                "contract" => KvErrorDomain::Contract,
                "mysql" => KvErrorDomain::Mysql,
                "tarantool" => KvErrorDomain::Tarantool,
                _ => return Err(protocol("unknown error domain")),
            };
            let code = u32_value(e["code"])?;
            let message = text(e["message"])?;
            let peer = e.get("peer").map(|p| u32_value(p)).transpose()?;
            if peer == Some(0) {
                return Err(protocol("invalid peer shard"));
            }
            let sqlstate = e.get("sqlstate").map(|s| text(s)).transpose()?;
            if let Some(value) = &sqlstate {
                if value.len() != 5 || !value.is_ascii() {
                    return Err(protocol("invalid SQLSTATE"));
                }
            }
            let cause = KvServerError {
                kind,
                domain,
                code,
                message,
                peer,
                sqlstate,
            };
            match kind {
                KvErrorKind::SchemaChanged => Err(KvError::SchemaChanged(cause.into())),
                KvErrorKind::Moved => Err(KvError::OwnerChanged(cause.into())),
                _ => Err(KvError::Server(cause)),
            }
        }
        _ => Err(protocol("unknown reply status")),
    }
}

fn array_items(bytes: &[u8], max: usize) -> KvResult<Vec<&[u8]>> {
    let mut p = bytes;
    let n = tarn_iproto::mp::read_array_len(&mut p)? as usize;
    if n > max {
        return Err(protocol("array exceeds KV bound"));
    }
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push(raw_value(&mut p)?);
    }
    if !p.is_empty() {
        return Err(protocol("trailing array bytes"));
    }
    Ok(out)
}

fn boolean(bytes: &[u8]) -> KvResult<bool> {
    let mut p = bytes;
    let b = tarn_iproto::mp::read_bool(&mut p)?;
    if !p.is_empty() {
        return Err(protocol("trailing boolean bytes"));
    }
    Ok(b)
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Member {
    id: u32,
    replicas: Vec<String>,
    state: String,
}

#[derive(Clone, Debug)]
struct Metadata {
    schema: Arc<TableSchema>,
    owner: Ownership,
    members: Vec<Member>,
    report: crate::buckets::Report,
    endpoint: String,
}

const META_FIELDS: &[&str] = &[
    "version",
    "status",
    "consistency",
    "table",
    "definition_json",
    "format",
    "primary",
    "shard_key_pk",
    "ownership",
    "members",
    "buckets",
    "limits",
];
const READ_FIELDS: &[&str] = &["version", "status", "consistency", "table", "slots"];

fn metadata(data: &[u8], db: &str, name: &str) -> KvResult<Metadata> {
    let f = envelope(data, META_FIELDS)?;
    let token = token(f["table"])?;
    if token.database != db || token.table != name {
        return Err(protocol("metadata names do not match request"));
    }
    let t = definition(&text(f["definition_json"])?)?;
    if t.db != db || t.name != name || t.space_id != token.space_id {
        return Err(protocol("descriptor does not match table token"));
    }
    let format = array_items(f["format"], MAX_FIELDS)?;
    let expected = t.lua_format();
    if format.len() != expected.len() {
        return Err(protocol("physical format width disagrees with SQL descriptor"));
    }
    for (entry, want) in format.iter().zip(&expected) {
        let e = array_items(entry, 3)?;
        if e.len() != 3 || text(e[0])? != want.0 || text(e[1])? != want.1 || boolean(e[2])? != want.2 {
            return Err(protocol("physical format disagrees with SQL descriptor"));
        }
    }
    let primary = array_items(f["primary"], MAX_COLUMNS)?;
    if primary.len() != t.pk().cols.len() {
        return Err(protocol("primary arity mismatch"));
    }
    for (part, &c) in primary.iter().zip(&t.pk().cols) {
        let e = array_items(part, 4)?;
        let physical = t.columns[c].weight.map(|n| n as usize).unwrap_or(t.field_of(c));
        if e.len() != 4 || uint(e[0])? != physical as u64 + 1 || boolean(e[2])? || !text(e[3])?.is_empty() {
            return Err(KvError::Unsupported(
                "functional, nullable or collated native primary index".into(),
            ));
        }
        let ty = text(e[1])?;
        if !["integer", "unsigned", "string", "varbinary"].contains(&ty.as_str()) || ty != expected[physical].1 {
            return Err(KvError::Unsupported(
                "native primary type is not supported by KV v1".into(),
            ));
        }
    }
    let routing = array_items(f["shard_key_pk"], MAX_COLUMNS)?;
    let want = t
        .shard_key
        .iter()
        .map(|c| t.pk().cols.iter().position(|p| p == c).map(|p| p + 1))
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| protocol("partition key is not in primary key"))?;
    if routing.len() != want.len() || routing.iter().zip(&want).any(|(p, w)| uint(p).ok() != Some(*w as u64)) {
        return Err(protocol("partition mapping disagrees with descriptor"));
    }
    let owner = ownership(f["ownership"])?;
    let mut members = Vec::new();
    let mut ids = HashSet::new();
    for raw in array_items(f["members"], 1024)? {
        let m = fields(raw, &["id", "replicas", "state"], &["id", "replicas", "state"])?;
        let id = u32_value(m["id"])?;
        let state = text(m["state"])?;
        if id == 0 || !ids.insert(id) || !["active", "draining", "removed"].contains(&state.as_str()) {
            return Err(protocol("invalid membership"));
        }
        let replicas = array_items(m["replicas"], 32)?
            .iter()
            .map(|v| text(v))
            .collect::<KvResult<Vec<_>>>()?;
        if replicas.is_empty()
            || replicas
                .iter()
                .any(|s| s.is_empty() || s.len() > 2048 || s.contains('@') || s.chars().any(char::is_whitespace))
        {
            return Err(protocol("invalid credential-free replica addresses"));
        }
        members.push(Member { id, replicas, state });
    }
    if !members.iter().any(|m| m.id == owner.shard && m.state != "removed") {
        return Err(protocol("local shard absent from membership"));
    }
    let b = fields(
        f["buckets"],
        &["count", "epoch", "owned", "others", "membership_version"],
        &["count", "epoch", "owned", "others", "membership_version"],
    )?;
    let mut report = crate::buckets::Report {
        count: u32_value(b["count"])?,
        epoch: uint(b["epoch"])?,
        mver: uint(b["membership_version"])?,
        ..Default::default()
    };
    if report.count != owner.count || report.epoch != owner.epoch || report.mver != owner.membership {
        return Err(protocol("ownership and bucket report disagree"));
    }
    for raw in array_items(b["owned"], owner.count as usize)? {
        let pair = array_items(raw, 2)?;
        if pair.len() != 2 {
            return Err(protocol("invalid owned range"));
        }
        let (lo, hi) = (u32_value(pair[0])?, u32_value(pair[1])?);
        if lo == 0 || lo > hi || hi > owner.count {
            return Err(protocol("out of range bucket ownership"));
        }
        report.owned.push((lo, hi));
    }
    for raw in array_items(b["others"], owner.count as usize)? {
        let e = array_items(raw, 4)?;
        if e.len() != 4 {
            return Err(protocol("invalid move record"));
        }
        let (bucket, state) = (u32_value(e[0])?, text(e[1])?);
        if bucket == 0
            || bucket > owner.count
            || !["sending", "fenced", "sent", "receiving", "held"].contains(&state.as_str())
        {
            return Err(protocol("invalid move state"));
        }
        let peer = if e[2] == [0xc0] { None } else { Some(u32_value(e[2])?) };
        let generation = if e[3] == [0xc0] { None } else { Some(text(e[3])?) };
        report.others.push((bucket, state, peer, generation));
    }
    let l = fields(
        f["limits"],
        &["max_keys", "max_key_bytes", "max_reply_bytes", "max_metadata_bytes"],
        &["max_keys", "max_key_bytes", "max_reply_bytes", "max_metadata_bytes"],
    )?;
    for (key, n) in [
        ("max_keys", 128),
        ("max_key_bytes", 65536),
        ("max_reply_bytes", 1048576),
        ("max_metadata_bytes", 1048576),
    ] {
        if uint(l[key])? != n {
            return Err(KvError::Unsupported("unrecognized KV v1 limits".into()));
        }
    }
    let schema = Arc::new(TableSchema {
        database: db.into(),
        table: name.into(),
        columns: t
            .columns
            .iter()
            .map(|c| ColumnInfo {
                name: c.name.clone(),
                sql_type: c.ty.clone(),
                nullable: c.nullable,
            })
            .collect(),
        token,
        definition: Arc::new(t),
    });
    Ok(Metadata {
        schema,
        owner,
        members,
        report,
        endpoint: String::new(),
    })
}

fn encoded_key(key: &[Value]) -> Vec<u8> {
    let mut m = Mp::new();
    m.vals(key);
    m.0
}

fn read_reply(data: &Bytes, handle: &TableHandle, keys: &[Vec<Value>]) -> KvResult<Vec<Option<Row>>> {
    let f = envelope(data, READ_FIELDS)?;
    if token(f["table"])? != handle.schema.token {
        return Err(KvError::SchemaChanged("reply did not echo frozen table token".into()));
    }
    let slots = array_items(f["slots"], 128)?;
    if slots.len() != keys.len() {
        return Err(protocol("incomplete point vector"));
    }
    let mut out = Vec::with_capacity(keys.len());
    for (i, (raw, key)) in slots.iter().zip(keys).enumerate() {
        let s = fields(
            raw,
            &["position", "key", "status", "tuple"],
            &["position", "key", "status"],
        )?;
        if uint(s["position"])? != i as u64 + 1 {
            return Err(protocol("slot position mismatch"));
        }
        let echoed = array_items(s["key"], MAX_COLUMNS)?
            .iter()
            .map(|r| {
                let mut p = *r;
                let v = tarn_iproto::mp::decode_value(&mut p)?;
                if !p.is_empty() {
                    return Err(protocol("trailing key bytes"));
                }
                Ok(v)
            })
            .collect::<KvResult<Vec<_>>>()?;
        if encoded_key(&echoed) != encoded_key(key) {
            return Err(protocol("slot key does not match requested native key"));
        }
        match text(s["status"])?.as_str() {
            "missing" if s.len() == 3 => out.push(None),
            "present" if s.len() == 4 => {
                let tuple = Bytes::copy_from_slice(s["tuple"]);
                let row = decode_row(&tuple, &handle.schema.definition)?;
                if encoded_key(&handle.schema.definition.pk_key(&row)) != encoded_key(key) {
                    return Err(protocol("returned tuple does not belong to requested key"));
                }
                out.push(Some(row));
            }
            _ => return Err(protocol("invalid present/missing slot")),
        }
    }
    Ok(out)
}

#[derive(Clone)]
struct Routing {
    map: crate::buckets::BucketMap,
    shards: Vec<Metadata>,
}

/// Read-only discovery and storage calls; this never constructs a SQL Cluster.
pub struct StrongKvClient {
    options: KvOptions,
    connections: parking_lot::Mutex<HashMap<String, Client>>,
    routes: parking_lot::Mutex<HashMap<(String, String), Arc<Routing>>>,
    budget: tarn_iproto::Budget,
}

impl StrongKvClient {
    /// Validates options; actual authenticated discovery is performed by table().
    pub fn connect(options: KvOptions) -> KvResult<Self> {
        options.validate()?;
        let budget = tarn_iproto::Budget::new("strong_kv", options.request_limits)?;
        Ok(Self {
            options,
            connections: parking_lot::Mutex::new(HashMap::new()),
            routes: parking_lot::Mutex::new(HashMap::new()),
            budget,
        })
    }

    /// Currently cached endpoints only. Retired/replaced connections are not a
    /// lifetime byte census; measurements must require stable connection IDs.
    pub fn connection_stats(&self) -> Vec<tarn_iproto::Stats> {
        self.connections.lock().values().map(Client::stats).collect()
    }

    async fn connection(&self, address: &str, deadline: Instant) -> KvResult<Client> {
        if Instant::now() >= deadline {
            return Err(KvError::Deadline);
        }
        let cached = { self.connections.lock().get(address).cloned() };
        if let Some(c) = cached {
            if !c.stats().closed {
                return Ok(c);
            }
        }
        let mut options = Opts::new(address);
        options.user = Some(self.options.credentials.user.clone());
        options.password = Some(self.options.credentials.password.clone());
        options.connect_timeout = self
            .options
            .connect_timeout
            .min(deadline.saturating_duration_since(Instant::now()));
        options.request_timeout = self.options.read_timeout;
        // Refuse an oversized advertised frame before reserving its body.
        // The V1 envelope is bounded to 1 MiB; allow 4 KiB for the IPROTO
        // header/body wrapper and authentication/capability replies.
        options.max_response_bytes = MAX_WIRE_RESPONSE_BYTES;
        options.request_limits = self.options.request_limits;
        options.admission = vec![self.budget.clone()];
        let client = timeout_at(deadline, Client::connect(&options))
            .await
            .map_err(|_| KvError::Deadline)??;
        let mut connections = self.connections.lock();
        // Bound cached endpoints across membership changes. Existing read-only
        // in-flight calls retain their physical connection independently.
        if connections.len() >= 1024 && !connections.contains_key(address) {
            connections.clear();
        }
        connections.insert(address.into(), client.clone());
        Ok(client)
    }

    async fn call(&self, address: &str, name: &str, args: &[u8], deadline: Instant) -> KvResult<Bytes> {
        if Instant::now() >= deadline {
            return Err(KvError::Deadline);
        }
        let c = self.connection(address, deadline).await?;
        let reply = timeout_at(deadline, c.call(0, name, args))
            .await
            .map_err(|_| KvError::Deadline)?;
        match reply {
            Ok(r) => r.data().ok_or_else(|| protocol("missing CALL data")),
            Err(e) => {
                self.connections.lock().remove(address);
                Err(e.into())
            }
        }
    }

    async fn describe(&self, replicas: &[String], db: &str, name: &str, deadline: Instant) -> KvResult<Metadata> {
        let mut m = Mp::new();
        m.arr(1)
            .map(3)
            .str("version")
            .uint(1)
            .str("db")
            .str(db)
            .str("name")
            .str(name);
        let mut last = None;
        for address in replicas {
            match self
                .call(address, "tarn_kv_v1.meta", &m.0, deadline)
                .await
                .and_then(|d| metadata(&d, db, name))
            {
                Ok(mut meta) => {
                    meta.endpoint = address.clone();
                    return Ok(meta);
                }
                Err(e) if retry_owner(&e) => last = Some(e),
                Err(e) => return Err(e),
            }
        }
        Err(last.unwrap_or_else(|| KvError::Unavailable("no configured replica".into())))
    }

    async fn routing(
        &self,
        db: &str,
        name: &str,
        handle: Option<&TableHandle>,
        deadline: Instant,
    ) -> KvResult<Routing> {
        let mut first = None;
        let mut last = None;
        for seed in &self.options.seeds {
            match self.describe(&seed.replicas, db, name, deadline).await {
                Ok(m) => {
                    if m.owner.shard != seed.id {
                        return Err(protocol("seed shard identity mismatch"));
                    }
                    first = Some(m);
                    break;
                }
                Err(e) if retry_owner(&e) => last = Some(e),
                Err(e) => return Err(e),
            }
        }
        let first = first.ok_or_else(|| last.unwrap_or_else(|| KvError::Unavailable("no seed leader".into())))?;
        if handle.is_some_and(|h| h.schema.token != first.schema.token) {
            return Err(KvError::SchemaChanged(
                "metadata no longer matches frozen handle".into(),
            ));
        }
        let mut members = first.members.clone();
        members.retain(|m| m.state != "removed");
        members.sort_by_key(|m| m.id);
        let mut shards = Vec::with_capacity(members.len());
        for member in &members {
            let m = if member.id == first.owner.shard {
                first.clone()
            } else {
                self.describe(&member.replicas, db, name, deadline).await?
            };
            if m.owner.shard != member.id {
                return Err(protocol("member shard identity mismatch"));
            }
            if m.schema.token != first.schema.token {
                return Err(KvError::SchemaChanged("shards disagree on frozen table token".into()));
            }
            if m.members != first.members || m.owner.membership != first.owner.membership {
                return Err(KvError::OwnerChanged("membership changed during discovery".into()));
            }
            shards.push(m);
        }
        let positions = shards.iter().enumerate().map(|(i, m)| (m.owner.shard, i)).collect();
        let mut map =
            crate::buckets::BucketMap::from_reports(shards.iter().map(|m| m.report.clone()).collect(), &positions)
                .map_err(|e| KvError::OwnerChanged(e.message.into()))?;
        if !map.settled() {
            return Err(KvError::OwnerChanged("bucket is in transfer".into()));
        }
        map.mver = first.owner.membership;
        Ok(Routing { map, shards })
    }

    pub async fn table(&self, db: &str, name: &str) -> KvResult<TableHandle> {
        identifier(db)?;
        identifier(name)?;
        let deadline = Instant::now() + self.options.read_timeout;
        let mut last = None;
        for _ in 0..self.options.max_read_attempts {
            match self.routing(db, name, None, deadline).await {
                Ok(r) => {
                    let handle = TableHandle {
                        schema: r.shards[0].schema.clone(),
                    };
                    let mut routes = self.routes.lock();
                    if routes.len() >= 16 {
                        routes.clear();
                    }
                    routes.insert((db.into(), name.into()), Arc::new(r));
                    return Ok(handle);
                }
                Err(e) if retry_owner(&e) => last = Some(e),
                Err(e) => return Err(e),
            }
        }
        Err(last.unwrap_or_else(|| KvError::Unavailable("owner discovery exhausted".into())))
    }

    /// Explicitly reacquire a schema; original logical keys are never reinterpreted.
    pub async fn refresh_table(&self, table: &TableHandle) -> KvResult<TableHandle> {
        self.table(&table.schema.database, &table.schema.table).await
    }

    pub async fn get(&self, table: &TableHandle, key: Vec<Value>) -> KvResult<KvPoint> {
        let batch = self.read_keys(table, &[key], true).await?;
        Ok(KvPoint {
            schema: batch.schema,
            row: batch.rows.into_iter().next().flatten(),
        })
    }

    pub async fn get_many(&self, table: &TableHandle, keys: &[Vec<Value>]) -> KvResult<KvBatch> {
        self.read_keys(table, keys, false).await
    }

    async fn read_keys(&self, table: &TableHandle, keys: &[Vec<Value>], single: bool) -> KvResult<KvBatch> {
        let deadline = Instant::now() + self.options.read_timeout;
        if !(1..=128).contains(&keys.len()) {
            return Err(KvError::Invalid("KV v1 requires 1..=128 keys".into()));
        }
        // Bound caller-supplied variable-length values before normalization
        // allocates weight strings or copies an oversized encoded request.
        // A lower bound on encoded input (one array/scalar header byte),
        // rather than a conservative upper bound that would reject valid
        // requests at the advertised exact 64 KiB boundary.
        let mut input_bytes = 1usize;
        for key in keys {
            if key.len() != table.schema.definition.pk().cols.len() {
                return Err(KvError::Invalid("a complete non-NULL primary key is required".into()));
            }
            input_bytes += 1;
            for value in key {
                let bytes = match value {
                    Value::Str(b) | Value::Bin(b) => b.len(),
                    Value::Decimal(s) => s.len(),
                    _ => 0,
                };
                input_bytes = input_bytes
                    .checked_add(bytes.saturating_add(1))
                    .ok_or_else(|| KvError::LimitExceeded("input keys".into()))?;
                if input_bytes > 65536 {
                    return Err(KvError::LimitExceeded("input keys exceed 64 KiB".into()));
                }
            }
        }
        let keys = keys.iter().map(|k| table.key(k)).collect::<KvResult<Vec<_>>>()?;
        let mut encoded = Mp::new();
        encoded.arr(keys.len());
        for key in &keys {
            encoded.vals(key);
        }
        if encoded.0.len() > 65536 {
            return Err(KvError::LimitExceeded("encoded keys exceed 64 KiB".into()));
        }
        let mut last = None;
        for _ in 0..self.options.max_read_attempts {
            let result = self.read_attempt(table, &keys, single, deadline).await;
            match result {
                Ok(rows) => {
                    return Ok(KvBatch {
                        schema: table.schema.clone(),
                        rows,
                    })
                }
                Err(e) if retry_owner(&e) => {
                    self.routes
                        .lock()
                        .remove(&(table.schema.database.clone(), table.schema.table.clone()));
                    last = Some(e);
                }
                Err(e) => return Err(e),
            }
        }
        Err(last.unwrap_or_else(|| KvError::Unavailable("owner discovery exhausted".into())))
    }

    async fn read_attempt(
        &self,
        table: &TableHandle,
        keys: &[Vec<Value>],
        single: bool,
        deadline: Instant,
    ) -> KvResult<Vec<Option<Row>>> {
        let cache_key = (table.schema.database.clone(), table.schema.table.clone());
        let cached = { self.routes.lock().get(&cache_key).cloned() };
        let routing = match cached {
            Some(r) if r.shards[0].schema.token == table.schema.token => r,
            _ => {
                let r = Arc::new(
                    self.routing(&table.schema.database, &table.schema.table, Some(table), deadline)
                        .await?,
                );
                let mut routes = self.routes.lock();
                if routes.len() >= 16 && !routes.contains_key(&cache_key) {
                    routes.clear();
                }
                routes.insert(cache_key, r.clone());
                r
            }
        };
        let mut groups: BTreeMap<usize, Vec<(usize, Vec<Value>)>> = BTreeMap::new();
        for (position, key) in keys.iter().enumerate() {
            let bucket = crate::route::bucket_of(&table.schema.definition.shard_key_of_pk(key), routing.map.count);
            groups
                .entry(routing.map.shard(bucket))
                .or_default()
                .push((position, key.clone()));
        }
        let mut result = vec![None; keys.len()];
        let mut reply_bytes = 0usize;
        for (shard, group) in groups {
            let meta = &routing.shards[shard];
            let mut m = Mp::new();
            m.arr(1)
                .map(5)
                .str("version")
                .uint(1)
                .str("consistency")
                .str("independent_points")
                .str("table");
            table.schema.token.encode(&mut m);
            m.str("ownership");
            meta.owner.encode(&mut m);
            if single {
                m.str("key").vals(&group[0].1);
            } else {
                m.str("keys").arr(group.len());
                for (_, key) in &group {
                    m.vals(key);
                }
            }
            let group_keys = group.iter().map(|(_, k)| k.clone()).collect::<Vec<_>>();
            // A changed leader restarts the complete vector, not only this group.
            let data = self
                .call(
                    &meta.endpoint,
                    if single {
                        "tarn_kv_v1.get"
                    } else {
                        "tarn_kv_v1.get_many"
                    },
                    &m.0,
                    deadline,
                )
                .await?;
            reply_bytes = reply_bytes
                .checked_add(data.len())
                .ok_or_else(|| KvError::LimitExceeded("aggregate data replies".into()))?;
            if reply_bytes > 1048576 {
                return Err(KvError::LimitExceeded("aggregate data replies exceed 1 MiB".into()));
            }
            let rows = read_reply(&data, table, &group_keys)?;
            for ((position, _), row) in group.into_iter().zip(rows) {
                result[position] = row;
            }
        }
        Ok(result)
    }
}

fn identifier(name: &str) -> KvResult<()> {
    if name.is_empty() || name.len() > 64 || name.contains('\0') {
        return Err(KvError::Invalid(
            "identifier must contain 1..=64 bytes without NUL".into(),
        ));
    }
    if name.contains('.') {
        return Err(KvError::Unsupported("KV v1 identifiers cannot contain dots".into()));
    }
    Ok(())
}

fn retry_owner(error: &KvError) -> bool {
    matches!(
        error,
        KvError::OwnerChanged(_) | KvError::Transport(tarn_iproto::Error::Closed | tarn_iproto::Error::Io(_))
    ) || matches!(error,KvError::Transport(tarn_iproto::Error::Server{code,..})if *code==tarn_iproto::errcode::ER_READONLY)
}

/// A frozen descriptor. Successful rows always carry this exact schema.
#[derive(Clone, Debug)]
pub struct TableHandle {
    schema: Arc<TableSchema>,
}

impl TableHandle {
    pub fn schema(&self) -> &Arc<TableSchema> {
        &self.schema
    }

    fn key(&self, key: &[Value]) -> KvResult<Vec<Value>> {
        let t = &self.schema.definition;
        if key.len() != t.pk().cols.len() || key.iter().any(Value::is_null) {
            return Err(KvError::Invalid("a complete non-NULL primary key is required".into()));
        }
        t.pk()
            .cols
            .iter()
            .zip(key)
            .map(|(&c, value)| {
                if matches!(value, Value::Float(f) if !f.is_finite()) {
                    return Err(KvError::Invalid("non-finite key value".into()));
                }
                // The SQL lookup codec owns type conversion and collation.
                // An unrepresentable key is not a successful fresh Missing.
                t.lookup_part(c, value)
                    .ok_or_else(|| KvError::Invalid(format!("invalid key value for {}", t.columns[c].name)))
            })
            .collect()
    }
}

#[derive(Clone, Debug)]
pub struct KvBatch {
    pub schema: Arc<TableSchema>,
    /// One slot per input, including duplicates and misses. Independent points
    /// may observe different committed values even when their keys are equal.
    pub rows: Vec<Option<Row>>,
}

#[derive(Clone, Debug)]
pub struct KvPoint {
    pub schema: Arc<TableSchema>,
    pub row: Option<Row>,
}

const MAX_SCHEMA_BYTES: usize = 1 << 20;
const MAX_WIRE_RESPONSE_BYTES: usize = MAX_SCHEMA_BYTES + 4096;
const MAX_COLUMNS: usize = 1024;
const MAX_FIELDS: usize = 8192;

/// Validate allocation/index bounds before invoking the shared SQL resolver.
fn definition(json: &str) -> KvResult<TableDef> {
    if json.len() > MAX_SCHEMA_BYTES {
        return Err(KvError::Protocol("oversized table descriptor".into()));
    }
    let raw: TableDef = serde_json::from_str(json).map_err(|e| KvError::Protocol(e.to_string()))?;
    if raw.columns.is_empty()
        || raw.columns.len() > MAX_COLUMNS
        || raw.indexes.is_empty()
        || raw.indexes.len() > MAX_COLUMNS
        || raw.space_id == 0
        || raw.bucket_field() >= MAX_FIELDS
        || raw
            .columns
            .iter()
            .enumerate()
            .any(|(c, column)| raw.field_of(c) >= MAX_FIELDS || column.weight.is_some_and(|n| n as usize >= MAX_FIELDS))
        || raw.dropped.iter().any(|&n| n as usize >= MAX_FIELDS)
        || raw
            .indexes
            .iter()
            .any(|i| i.cols.is_empty() || i.cols.iter().any(|&c| c >= raw.columns.len()))
    {
        return Err(KvError::Protocol("invalid descriptor bounds".into()));
    }
    if raw.pk().id != 0
        || !raw.pk().unique
        || !raw.declared_pk()
        || raw.columns.iter().any(|c| c.hidden)
        || raw.shard_key.is_empty()
        || raw.shard_key.iter().any(|c| !raw.pk().cols.contains(c))
    {
        return Err(KvError::Unsupported(
            "a declared full primary key containing the shard key is required".into(),
        ));
    }
    let mut columns = HashSet::new();
    let mut fields = HashSet::new();
    fields.insert(raw.bucket_field());
    for (c, column) in raw.columns.iter().enumerate() {
        if column.name.is_empty()
            || !columns.insert(column.name.to_ascii_lowercase())
            || !fields.insert(raw.field_of(c))
        {
            return Err(KvError::Protocol("duplicate column name or physical field".into()));
        }
        if let Some(f) = column.weight {
            if !fields.insert(f as usize) {
                return Err(KvError::Protocol("overlapping weight field".into()));
            }
        }
    }
    let t = catalog::parse_def_json(json).map_err(|e| KvError::Protocol(e.message))?;
    catalog::validate(&t).map_err(|e| KvError::Protocol(e.message))?;
    Ok(t)
}

fn decode_row(tuple: &Bytes, t: &TableDef) -> KvResult<Row> {
    let mut count = Rd::new(tuple);
    if count.array().map_err(|e| protocol(e.message))? > MAX_FIELDS {
        return Err(protocol("oversized physical tuple"));
    }
    let mut rd = Rd::shared(tuple);
    let row = rd.row_of(t).map_err(|e| KvError::Protocol(e.message))?;
    if !rd.buf.is_empty() {
        return Err(KvError::Protocol("trailing stored tuple bytes".into()));
    }
    Ok(row)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mpw::Mp;
    use serde_json::json;

    fn descriptor() -> serde_json::Value {
        json!({
            "db":"d", "name":"t", "space_id":10001, "ver":7,
            "columns":[
                {"name":"tenant", "ty":"BIGINT", "nullable":false,"default":null,"auto_increment":false},
                {"name":"id", "ty":"BIGINT", "nullable":false,"default":null,"auto_increment":false},
                {"name":"p", "ty":"VARBINARY(80)", "nullable":true,"default":null,"auto_increment":false}
            ],
            "indexes":[{"name":"PRIMARY","id":0,"cols":[0,1],"unique":true}],
            "shard_key":[0], "auto_inc":null
        })
    }

    fn handle(value: serde_json::Value) -> TableHandle {
        let t = definition(&value.to_string()).unwrap();
        TableHandle {
            schema: Arc::new(TableSchema {
                database: t.db.clone(),
                table: t.name.clone(),
                columns: t
                    .columns
                    .iter()
                    .map(|c| ColumnInfo {
                        name: c.name.clone(),
                        sql_type: c.ty.clone(),
                        nullable: c.nullable,
                    })
                    .collect(),
                token: TableToken {
                    database: t.db.clone(),
                    table: t.name.clone(),
                    space_id: t.space_id,
                    definition_version: t.ver,
                    definition_hash: "a".repeat(64),
                    layout_hash: "b".repeat(64),
                },
                definition: Arc::new(t),
            }),
        }
    }

    #[test]
    fn invalid_complete_keys_do_not_turn_into_fresh_misses() {
        let t = handle(descriptor());
        for key in [
            vec![],
            vec![Value::Int(1)],
            vec![Value::Int(1), Value::Null],
            vec![Value::Int(1), Value::Float(f64::INFINITY)],
        ] {
            assert!(matches!(t.key(&key), Err(KvError::Invalid(_))));
        }
        assert_eq!(
            t.key(&[Value::Int(-4), Value::UInt(7)]).unwrap(),
            vec![Value::Int(-4), Value::Int(7)]
        );
        let mut bad = descriptor();
        bad["shard_key"] = json!([2]);
        assert!(matches!(definition(&bad.to_string()), Err(KvError::Unsupported(_))));
    }

    #[test]
    fn key_and_row_codecs_handle_weights_and_instant_physical_layout() {
        let mut d = descriptor();
        d["columns"][1]["ty"] = json!("VARCHAR(40)");
        d["columns"][1]["collation"] = json!("utf8mb4_general_ci");
        d["columns"][1]["weight"] = json!(6);
        d["columns"][0]["field"] = json!(1);
        d["columns"][1]["field"] = json!(0);
        d["columns"][2]["field"] = json!(5);
        d["columns"][2]["fill"] = json!({"value":"older"});
        d["bucket"] = json!(2);
        d["dropped"] = json!([3, 4]);
        let h = handle(d);
        let t = &h.schema.definition;
        let canonical = h.key(&[Value::Int(7), Value::str("AbC")]).unwrap();
        assert_eq!(
            canonical,
            vec![Value::Int(7), t.lookup_part(1, &Value::str("abc")).unwrap()]
        );
        assert!(matches!(canonical[1], Value::Bin(_)));
        let mut tuple = Mp::new();
        tuple.vals(&[Value::str("AbC"), Value::Int(7), Value::UInt(23)]);
        let row = decode_row(&tuple.bytes(), t).unwrap();
        assert_eq!(
            row,
            vec![
                Value::Int(7),
                Value::str("AbC"),
                Value::Bin(Bytes::from_static(b"older"))
            ]
        );
        assert_eq!(h.schema.columns[0].name, "tenant");
        assert_eq!(h.schema.columns[2].name, "p");
    }

    #[test]
    fn descriptor_bounds_precede_sql_layout_resolution() {
        let mut d = descriptor();
        d["columns"][0]["field"] = json!(u32::MAX);
        assert!(matches!(definition(&d.to_string()), Err(KvError::Protocol(_))));
        let mut d = descriptor();
        d["indexes"][0]["cols"] = json!([9000]);
        assert!(matches!(definition(&d.to_string()), Err(KvError::Protocol(_))));
        let mut d = descriptor();
        d["columns"][1]["field"] = json!(0);
        assert!(matches!(definition(&d.to_string()), Err(KvError::Protocol(_))));
    }

    #[test]
    fn credentials_are_explicit_and_debug_redacted() {
        let options = KvOptions::new(
            vec![],
            Credentials {
                user: "reader".into(),
                password: "secret-181".into(),
            },
        );
        assert!(options.validate().is_err());
        assert!(!format!("{options:?}").contains("secret-181"));
    }

    fn meta_reply(h: &TableHandle, endpoint: &str, epoch: u64) -> Bytes {
        let mut m = Mp::new();
        m.arr(1)
            .map(12)
            .str("version")
            .uint(1)
            .str("status")
            .str("ok")
            .str("consistency")
            .str("independent_points")
            .str("table");
        h.schema.token.encode(&mut m);
        let t = &h.schema.definition;
        m.str("definition_json")
            .str(&serde_json::to_string(t.as_ref()).unwrap())
            .str("format")
            .arr(t.layout.width);
        for (name, ty, nullable) in t.lua_format() {
            m.arr(3).str(&name).str(&ty).bool(nullable);
        }
        m.str("primary").arr(t.pk().cols.len());
        for &c in &t.pk().cols {
            let field = t.columns[c].weight.map(|x| x as usize).unwrap_or(t.field_of(c));
            m.arr(4)
                .uint(field as u64 + 1)
                .str(&t.lua_format()[field].1)
                .bool(false)
                .str("");
        }
        m.str("shard_key_pk").arr(t.shard_key.len());
        for c in &t.shard_key {
            m.uint(t.pk().cols.iter().position(|p| p == c).unwrap() as u64 + 1);
        }
        let owner = Ownership {
            shard: 1,
            count: 4,
            epoch,
            membership: 7,
        };
        m.str("ownership");
        owner.encode(&mut m);
        m.str("members")
            .arr(1)
            .map(3)
            .str("id")
            .uint(1)
            .str("replicas")
            .arr(1)
            .str(endpoint)
            .str("state")
            .str("active");
        m.str("buckets")
            .map(5)
            .str("count")
            .uint(4)
            .str("epoch")
            .uint(epoch)
            .str("owned")
            .arr(1)
            .arr(2)
            .uint(1)
            .uint(4)
            .str("others")
            .arr(0)
            .str("membership_version")
            .uint(7);
        m.str("limits")
            .map(4)
            .str("max_keys")
            .uint(128)
            .str("max_key_bytes")
            .uint(65536)
            .str("max_reply_bytes")
            .uint(1048576)
            .str("max_metadata_bytes")
            .uint(1048576);
        m.bytes()
    }

    fn point_reply(h: &TableHandle, keys: &[Vec<Value>], payloads: &[Option<&str>]) -> Bytes {
        let mut m = Mp::new();
        m.arr(1)
            .map(5)
            .str("version")
            .uint(1)
            .str("status")
            .str("ok")
            .str("consistency")
            .str("independent_points")
            .str("table");
        h.schema.token.encode(&mut m);
        m.str("slots").arr(keys.len());
        for (i, (key, payload)) in keys.iter().zip(payloads).enumerate() {
            m.map(if payload.is_some() { 4 } else { 3 })
                .str("position")
                .uint(i as u64 + 1)
                .str("key")
                .vals(key)
                .str("status")
                .str(if payload.is_some() { "present" } else { "missing" });
            if let Some(p) = payload {
                m.str("tuple");
                let mut row = key.clone();
                row.push(Value::Bin(Bytes::copy_from_slice(p.as_bytes())));
                h.schema.definition.encode_tuple(&row, 1, &mut m);
            }
        }
        m.bytes()
    }

    fn error_reply(kind: &str, code: u64) -> Bytes {
        let mut m = Mp::new();
        m.arr(1)
            .map(3)
            .str("version")
            .uint(1)
            .str("status")
            .str("error")
            .str("error")
            .map(4)
            .str("kind")
            .str(kind)
            .str("domain")
            .str("contract")
            .str("code")
            .uint(code)
            .str("message")
            .str("test refusal");
        m.bytes()
    }

    #[test]
    fn slots_preserve_duplicates_and_misses_without_equal_value_promise() {
        let h = handle(descriptor());
        let keys = vec![
            vec![Value::Int(7), Value::Int(1)],
            vec![Value::Int(7), Value::Int(2)],
            vec![Value::Int(7), Value::Int(1)],
        ];
        let d = point_reply(&h, &keys, &[Some("first"), None, Some("later")]);
        let rows = read_reply(&d, &h, &keys).unwrap();
        assert_eq!(rows.len(), 3);
        assert!(rows[1].is_none());
        assert_ne!(rows[0], rows[2]);
        let mut wrong = keys.clone();
        wrong[2][1] = Value::Int(9);
        assert!(read_reply(&d, &h, &wrong).is_err());
        let mut changed = h.clone();
        Arc::make_mut(&mut changed.schema).token.layout_hash = "c".repeat(64);
        assert!(matches!(
            read_reply(&d, &changed, &keys),
            Err(KvError::SchemaChanged(_))
        ));
        for n in 0..d.len() {
            assert!(read_reply(&d.slice(..n), &h, &keys).is_err());
        }
        let mut trailing = d.to_vec();
        trailing.push(0xc0);
        assert!(read_reply(&Bytes::from(trailing), &h, &keys).is_err());
    }

    #[test]
    fn metadata_binds_layout_names_native_pk_and_outer_version() {
        let h = handle(descriptor());
        let d = meta_reply(&h, "127.0.0.1:3311", 2);
        let m = metadata(&d, "d", "t").unwrap();
        assert_eq!(m.owner.epoch, 2);
        assert_eq!(m.schema.columns[2].name, "p");
        assert!(metadata(&d, "d", "other").is_err());
        let mut changed = h.clone();
        Arc::make_mut(&mut changed.schema).token.definition_version = 900;
        let d = meta_reply(&changed, "127.0.0.1:3311", 2);
        // Outer catalog version is an independent fence, not JSON TableDef.ver.
        assert_eq!(metadata(&d, "d", "t").unwrap().schema.token.definition_version, 900);
        for n in 0..d.len() {
            assert!(metadata(&d.slice(..n), "d", "t").is_err());
        }
    }

    #[test]
    fn errors_are_typed_and_do_not_certify_partial_rows() {
        assert!(matches!(
            envelope(&error_reply("schema_changed", 1412), READ_FIELDS),
            Err(KvError::SchemaChanged(_))
        ));
        assert!(matches!(
            envelope(&error_reply("moved", 0), READ_FIELDS),
            Err(KvError::OwnerChanged(_))
        ));
        assert!(!retry_owner(&KvError::Server(KvServerError {
            kind: KvErrorKind::Unavailable,
            domain: KvErrorDomain::Mysql,
            code: 1213,
            message: "claim conflict".into(),
            peer: None,
            sqlstate: None
        })));
        let mut wrong = Mp::new();
        wrong
            .arr(1)
            .map(4)
            .str("version")
            .uint(1)
            .str("status")
            .str("error")
            .str("error")
            .map(4)
            .str("kind")
            .str("moved")
            .str("domain")
            .str("contract")
            .str("code")
            .uint(0)
            .str("message")
            .str("x")
            .str("slots")
            .arr(0);
        assert!(matches!(envelope(&wrong.0, READ_FIELDS), Err(KvError::Protocol(_))));
        assert!(fields(&[0x82, 0xa1, b'x', 1, 0xa1, b'x', 2], &["x"], &["x"]).is_err());
    }

    #[test]
    fn server_errors_retain_namespaces_and_schema_owner_causes() {
        let make = |kind: &str, domain: &str, state: bool| {
            let mut m = Mp::new();
            m.arr(1)
                .map(3)
                .str("version")
                .uint(1)
                .str("status")
                .str("error")
                .str("error")
                .map(if state { 6 } else { 4 })
                .str("kind")
                .str(kind)
                .str("domain")
                .str(domain)
                .str("code")
                .uint(7)
                .str("message")
                .str("unchanged server text");
            if state {
                m.str("sqlstate").str("HY000").str("peer").uint(2);
            }
            m.bytes()
        };
        for (kind, typed) in [
            ("access_denied", KvErrorKind::AccessDenied),
            ("unavailable", KvErrorKind::Unavailable),
            ("schema_changed", KvErrorKind::SchemaChanged),
            ("moved", KvErrorKind::Moved),
        ] {
            for (domain, typed_domain) in [
                ("mysql", KvErrorDomain::Mysql),
                ("tarantool", KvErrorDomain::Tarantool),
                ("contract", KvErrorDomain::Contract),
            ] {
                let error = envelope(&make(kind, domain, false), READ_FIELDS).unwrap_err();
                let raw = error.server().unwrap();
                assert_eq!(raw.kind, typed);
                assert_eq!(raw.domain, typed_domain);
                assert_eq!(raw.code, 7);
                assert_eq!(raw.message, "unchanged server text");
                assert!(raw.sqlstate.is_none());
                assert!(raw.peer.is_none());
                // Numeric7 never classifies these envelope errors as READONLY.
                assert_eq!(retry_owner(&error), typed == KvErrorKind::Moved);
            }
        }
        let error = envelope(&make("read_error", "mysql", true), READ_FIELDS).unwrap_err();
        assert_eq!(error.server().unwrap().sqlstate.as_deref(), Some("HY000"));
        assert_eq!(error.server().unwrap().peer, Some(2));
    }

    // In-process IPROTO mocks, not database services. Exercise actual auth,
    // public client dispatch, cached routing and whole-vector retry behavior.
    async fn mock_request(sock: &mut tokio::net::TcpStream) -> (u64, u64, u64, Vec<u8>) {
        use tokio::io::AsyncReadExt;
        assert_eq!(sock.read_u8().await.unwrap(), 0xce);
        let size = sock.read_u32().await.unwrap();
        // The 64 KiB key vector also carries table/ownership tokens. Keep
        // the fixture bounded without imposing the smaller generic mock cap.
        assert!(size < 2 * 1024 * 1024);
        let mut packet = vec![0; size as usize];
        sock.read_exact(&mut packet).await.unwrap();
        let mut rd = Rd::new(&packet);
        let (mut kind, mut sync, mut stream) = (0, 0, 0);
        for _ in 0..rd.map().unwrap() {
            let key = rd.value().unwrap().as_i64().unwrap();
            let value = rd.value().unwrap().as_i64().unwrap() as u64;
            match key {
                0 => kind = value,
                1 => sync = value,
                10 => stream = value,
                _ => {}
            }
        }
        (kind, sync, stream, rd.buf.to_vec())
    }

    async fn mock(
        make: impl FnOnce(&str) -> Vec<(&'static str, Bytes)>,
    ) -> (String, tokio::task::JoinHandle<Vec<(String, Vec<u8>)>>) {
        use crate::cluster::wire_test::{accept, reply};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let replies = make(&endpoint);
        let actor = tokio::spawn(async move {
            let mut sock = accept(&listener).await;
            let (kind, sync, stream, _) = mock_request(&mut sock).await;
            assert_eq!((kind, stream), (7, 0));
            reply(&mut sock, sync, 0, &[0x80]).await;
            let mut seen = Vec::new();
            for (name, data) in replies {
                let (kind, sync, stream, body) = mock_request(&mut sock).await;
                assert_eq!((kind, stream), (10, 0));
                let mut p = body.as_slice();
                let n = tarn_iproto::mp::read_map_len(&mut p).unwrap();
                let mut actual = None;
                let mut arguments = None;
                for _ in 0..n {
                    let k = tarn_iproto::mp::read_uint(&mut p).unwrap();
                    let v = raw_value(&mut p).unwrap();
                    if k == 0x22 {
                        actual = Some(text(v).unwrap());
                    } else if k == 0x21 {
                        assert_eq!(array_items(v, 1).unwrap().len(), 1);
                        arguments = Some(array_items(v, 1).unwrap()[0].to_vec());
                    }
                }
                assert_eq!(actual.as_deref(), Some(name));
                seen.push((name.to_owned(), arguments.unwrap()));
                let mut b = Mp::new();
                b.map(1).uint(0x30).raw(&data);
                reply(&mut sock, sync, 0, &b.0).await;
            }
            seen
        });
        (endpoint, actor)
    }

    #[tokio::test]
    async fn public_discovery_retires_an_oversized_advertised_frame_without_waiting_for_the_body() {
        use crate::cluster::wire_test::{accept, reply};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let actor = tokio::spawn(async move {
            let mut sock = accept(&listener).await;
            let (kind, sync, stream, _) = mock_request(&mut sock).await;
            assert_eq!((kind, stream), (7, 0));
            reply(&mut sock, sync, 0, &[0x80]).await;
            let (kind, _, stream, _) = mock_request(&mut sock).await;
            assert_eq!((kind, stream), (10, 0));
            let mut prefix = vec![0xce];
            prefix.extend_from_slice(&((MAX_WIRE_RESPONSE_BYTES + 1) as u32).to_be_bytes());
            sock.write_all(&prefix).await.unwrap();
            // No advertised payload is sent. The client must close from
            // this prefix rather than wait for its request deadline.
            let mut byte = [0];
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(2), sock.read(&mut byte))
                    .await
                    .unwrap()
                    .unwrap(),
                0
            );
        });
        let mut options = KvOptions::new(
            vec![ShardConfig {
                id: 1,
                replicas: vec![endpoint],
            }],
            Credentials {
                user: "reader".into(),
                password: "p".into(),
            },
        );
        // Isolate one physical retirement from permitted read-only
        // rediscovery. The transport reports retired waiters as Closed.
        options.max_read_attempts = 1;
        let client = StrongKvClient::connect(options).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(2), client.table("d", "t"))
            .await
            .unwrap();
        assert!(
            matches!(&result, Err(KvError::Transport(tarn_iproto::Error::Closed))),
            "{result:?}"
        );
        actor.await.unwrap();
        assert!(client.connection_stats().is_empty());
    }

    #[tokio::test]
    async fn public_batch_accepts_exact_key_byte_limit_and_rejects_one_over_before_wire() {
        let mut d = descriptor();
        d["columns"] = json!([{
            "name":"id", "ty":"VARBINARY(65535)", "nullable":false,
            "default":null, "auto_increment":false
        }]);
        d["indexes"][0]["cols"] = json!([0]);
        let h = handle(d);
        let keys = vec![vec![Value::Bin(Bytes::from(vec![b'k'; 65531]))]];
        let oversized = vec![vec![Value::Bin(Bytes::from(vec![b'k'; 65532]))]];
        for (input, length) in [(&keys, 65536), (&oversized, 65537)] {
            let mut encoded = Mp::new();
            encoded.arr(input.len()).vals(&input[0]);
            assert_eq!(encoded.0.len(), length);
        }
        let (endpoint, actor) = mock(|_| vec![("tarn_kv_v1.get_many", point_reply(&h, &keys, &[None]))]).await;
        let client = StrongKvClient::connect(KvOptions::new(
            vec![ShardConfig {
                id: 1,
                replicas: vec![endpoint.clone()],
            }],
            Credentials {
                user: "reader".into(),
                password: "test".into(),
            },
        ))
        .unwrap();
        let mut meta = metadata(&meta_reply(&h, &endpoint, 1), "d", "t").unwrap();
        meta.endpoint = endpoint;
        let map = crate::buckets::BucketMap::from_reports(vec![meta.report.clone()], &HashMap::from([(1, 0)])).unwrap();
        client.routes.lock().insert(
            ("d".into(), "t".into()),
            Arc::new(Routing {
                map,
                shards: vec![meta],
            }),
        );
        assert!(matches!(
            client.get_many(&h, &oversized).await,
            Err(KvError::LimitExceeded(_))
        ));
        let batch = client.get_many(&h, &keys).await.unwrap();
        assert_eq!(batch.rows, vec![None]);
        let seen = actor.await.unwrap();
        assert_eq!(seen.len(), 1, "the oversized request must never reach the wire");
        let fields = fields(
            &seen[0].1,
            &["version", "consistency", "table", "ownership", "keys"],
            &["version", "consistency", "table", "ownership", "keys"],
        )
        .unwrap();
        assert_eq!(fields["keys"].len(), 65536);
    }

    #[tokio::test]
    async fn public_client_never_retries_schema_changed_or_returns_a_prefix() {
        let h = handle(descriptor());
        let keys = vec![vec![Value::Int(7), Value::Int(1)]];
        // Cached routing isolates the actual read RPC and avoids any metadata
        // refresh being mistaken for a permitted schema retry.
        let (endpoint, actor) = mock(|_| vec![("tarn_kv_v1.get_many", error_reply("schema_changed", 1412))]).await;
        let client = StrongKvClient::connect(KvOptions::new(
            vec![ShardConfig {
                id: 1,
                replicas: vec![endpoint.clone()],
            }],
            Credentials {
                user: "reader".into(),
                password: "p".into(),
            },
        ))
        .unwrap();
        let mut meta = metadata(&meta_reply(&h, &endpoint, 1), "d", "t").unwrap();
        meta.endpoint = endpoint;
        let map = crate::buckets::BucketMap::from_reports(vec![meta.report.clone()], &HashMap::from([(1, 0)])).unwrap();
        client.routes.lock().insert(
            ("d".into(), "t".into()),
            Arc::new(Routing {
                map,
                shards: vec![meta],
            }),
        );
        assert!(matches!(
            client.get_many(&h, &keys).await,
            Err(KvError::SchemaChanged(_))
        ));
        actor.await.unwrap();
    }

    #[tokio::test]
    async fn owner_refresh_restarts_vector_with_identical_frozen_table_and_keys() {
        let h = handle(descriptor());
        let expected = h.schema.token.clone();
        let keys = vec![
            vec![Value::Int(7), Value::Int(1)],
            vec![Value::Int(7), Value::Int(2)],
            vec![Value::Int(7), Value::Int(1)],
        ];
        let (endpoint, actor) = mock(|address| {
            vec![
                ("tarn_kv_v1.meta", meta_reply(&h, address, 1)),
                ("tarn_kv_v1.get_many", error_reply("moved", 0)),
                ("tarn_kv_v1.meta", meta_reply(&h, address, 2)),
                (
                    "tarn_kv_v1.get_many",
                    point_reply(&h, &keys, &[Some("first"), None, Some("later")]),
                ),
            ]
        })
        .await;
        let client = StrongKvClient::connect(KvOptions::new(
            vec![ShardConfig {
                id: 1,
                replicas: vec![endpoint],
            }],
            Credentials {
                user: "reader".into(),
                password: "test".into(),
            },
        ))
        .unwrap();
        let table = client.table("d", "t").await.unwrap();
        let rows = client.get_many(&table, &keys).await.unwrap();
        assert_eq!(rows.schema.token, expected);
        assert!(rows.rows[1].is_none());
        assert_ne!(rows.rows[0], rows.rows[2]);
        let seen = actor.await.unwrap();
        let calls = seen
            .iter()
            .filter(|(name, _)| name.ends_with("get_many"))
            .collect::<Vec<_>>();
        assert_eq!(calls.len(), 2);
        for (i, (_, args)) in calls.iter().enumerate() {
            let f = fields(
                args,
                &["version", "consistency", "table", "ownership", "keys"],
                &["version", "consistency", "table", "ownership", "keys"],
            )
            .unwrap();
            assert_eq!(token(f["table"]).unwrap(), expected);
            assert_eq!(ownership(f["ownership"]).unwrap().epoch, i as u64 + 1);
            let mut m = Mp::new();
            m.arr(keys.len());
            for key in &keys {
                m.vals(key);
            }
            assert_eq!(f["keys"], m.0);
        }
    }

    #[tokio::test]
    async fn later_shard_schema_change_discards_earlier_successful_group() {
        let h = handle(descriptor());
        let tenant = |first: bool| {
            (0..100)
                .find(|&v| (crate::route::bucket_of(&[Value::Int(v)], 4) <= 2) == first)
                .unwrap()
        };
        let first = vec![Value::Int(tenant(true)), Value::Int(1)];
        let second = vec![Value::Int(tenant(false)), Value::Int(2)];
        let (a, first_actor) = mock(|_| {
            vec![(
                "tarn_kv_v1.get_many",
                point_reply(&h, std::slice::from_ref(&first), &[Some("already read")]),
            )]
        })
        .await;
        let (b, second_actor) = mock(|_| vec![("tarn_kv_v1.get_many", error_reply("schema_changed", 1412))]).await;
        let client = StrongKvClient::connect(KvOptions::new(
            vec![ShardConfig {
                id: 1,
                replicas: vec![a.clone()],
            }],
            Credentials {
                user: "reader".into(),
                password: "p".into(),
            },
        ))
        .unwrap();
        let mut m1 = metadata(&meta_reply(&h, &a, 1), "d", "t").unwrap();
        let mut m2 = m1.clone();
        m1.endpoint = a;
        m1.report.owned = vec![(1, 2)];
        m2.endpoint = b;
        m2.owner.shard = 2;
        m2.report.owned = vec![(3, 4)];
        let map = crate::buckets::BucketMap::from_reports(
            vec![m1.report.clone(), m2.report.clone()],
            &HashMap::from([(1, 0), (2, 1)]),
        )
        .unwrap();
        client.routes.lock().insert(
            ("d".into(), "t".into()),
            Arc::new(Routing {
                map,
                shards: vec![m1, m2],
            }),
        );
        assert!(matches!(
            client.get_many(&h, &[first, second]).await,
            Err(KvError::SchemaChanged(_))
        ));
        assert_eq!(first_actor.await.unwrap().len(), 1);
        assert_eq!(second_actor.await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn aggregate_reply_cap_discards_successful_first_shard() {
        let mut d = descriptor();
        d["columns"][2]["ty"] = json!("LONGBLOB");
        let h = handle(d);
        let tenant = |first: bool| {
            (0..100)
                .find(|&v| (crate::route::bucket_of(&[Value::Int(v)], 4) <= 2) == first)
                .unwrap()
        };
        let first = vec![Value::Int(tenant(true)), Value::Int(1)];
        let second = vec![Value::Int(tenant(false)), Value::Int(2)];
        let payload = "x".repeat(600_000);
        let data1 = point_reply(&h, std::slice::from_ref(&first), &[Some(&payload)]);
        let data2 = point_reply(&h, std::slice::from_ref(&second), &[Some(&payload)]);
        assert!(data1.len() < 1048576 && data2.len() < 1048576 && data1.len() + data2.len() > 1048576);
        let (a, one) = mock(|_| vec![("tarn_kv_v1.get_many", data1)]).await;
        let (b, two) = mock(|_| vec![("tarn_kv_v1.get_many", data2)]).await;
        let client = StrongKvClient::connect(KvOptions::new(
            vec![ShardConfig {
                id: 1,
                replicas: vec![a.clone()],
            }],
            Credentials {
                user: "reader".into(),
                password: "p".into(),
            },
        ))
        .unwrap();
        let mut m1 = metadata(&meta_reply(&h, &a, 1), "d", "t").unwrap();
        let mut m2 = m1.clone();
        m1.endpoint = a;
        m1.report.owned = vec![(1, 2)];
        m2.endpoint = b;
        m2.owner.shard = 2;
        m2.report.owned = vec![(3, 4)];
        let map = crate::buckets::BucketMap::from_reports(
            vec![m1.report.clone(), m2.report.clone()],
            &HashMap::from([(1, 0), (2, 1)]),
        )
        .unwrap();
        client.routes.lock().insert(
            ("d".into(), "t".into()),
            Arc::new(Routing {
                map,
                shards: vec![m1, m2],
            }),
        );
        assert!(matches!(
            client.get_many(&h, &[first, second]).await,
            Err(KvError::LimitExceeded(_))
        ));
        one.await.unwrap();
        two.await.unwrap();
    }
}
