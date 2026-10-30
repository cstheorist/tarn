use serde::Deserialize;
use std::{
    collections::{HashMap, HashSet},
    io,
    path::PathBuf,
};
use tarn_iproto::{mp, Client, Opts};
use tarn_sql::ClusterConfig;
use tarn_transport::{host::Requirements, ServerTls};
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Production {
    pub mysql_tls: ServerTls,
    pub users_file: PathBuf,
    pub failure_domains: HashMap<String, String>,
    pub max_connections: usize,
    pub host: Requirements,
}
impl Production {
    pub fn load(path: &str) -> io::Result<Self> {
        serde_json::from_slice(&tarn_transport::read_config(std::path::Path::new(path), 1_048_576)?)
            .map_err(|_| io::Error::other("invalid production profile"))
    }
    pub fn validate(&self, cfg: &ClusterConfig) -> io::Result<HashMap<String, String>> {
        if cfg.user != "tarn_service" || cfg.password_file.is_none() || cfg.password != "tarn" {
            return Err(io::Error::other(
                "production requires tarn_service and password_file, without an inline password",
            ));
        }
        if !cfg.storage_tls.as_ref().is_some_and(|c| c.has_identity()) {
            return Err(io::Error::other("production requires storage CA and client identity"));
        }
        if cfg.ack_on_prepare
            || cfg.pool_size == 0
            || self.max_connections == 0
            || self.max_connections > 100_000
            || cfg.shards.is_empty()
        {
            return Err(io::Error::other(
                "invalid production admission/durability configuration",
            ));
        }
        cfg.rpc.validate().map_err(|e| io::Error::other(e.message))?;
        tarn_sql::budget::Pool::new(cfg.resources.clone()).map_err(|e| io::Error::other(e.message))?;
        if u128::from(cfg.resources.server_bytes) * 4 > u128::from(self.host.min_memory_bytes) * 3 {
            return Err(io::Error::other(
                "SQL server budget requires at least 25% declared host memory headroom",
            ));
        }
        let mut seen = HashSet::new();
        let mut ids = HashSet::new();
        for shard in &cfg.shards {
            if !ids.insert(shard.id) || shard.id == 0 || shard.replicas.len() < 3 || shard.replicas.len() % 2 == 0 {
                return Err(io::Error::other(
                    "production requires distinct shard IDs and odd RF >= 3",
                ));
            }
            let mut domains = HashSet::new();
            for addr in &shard.replicas {
                tarn_transport::tls_endpoint(addr)?;
                let domain = self
                    .failure_domains
                    .get(addr)
                    .filter(|s| !s.trim().is_empty())
                    .ok_or_else(|| io::Error::other("replica missing a failure domain"))?;
                if !domains.insert(domain) || !seen.insert(addr) {
                    return Err(io::Error::other("duplicate replica/failure domain"));
                }
            }
        }
        if self.failure_domains.len() != seen.len() {
            return Err(io::Error::other(
                "failure domain inventory must exactly match configured replicas",
            ));
        }
        let users: HashMap<String, String> =
            serde_json::from_slice(&tarn_transport::read_private(&self.users_file, 1_048_576)?)
                .map_err(|_| io::Error::other("users_file must be a private JSON account map"))?;
        if users.is_empty()
            || users.iter().any(|(u, p)| {
                u.is_empty() || u.contains(['\0', '\n']) || p.len() < 20 || p.contains(['\0', '\n', '\r'])
            })
        {
            return Err(io::Error::other(
                "production requires named accounts with passwords of at least 20 bytes",
            ));
        }
        let report = self.host.check()?;
        tracing::info!("production host preflight: {}", serde_json::to_string(&report)?);
        Ok(users)
    }
    /// Raw identity/configuration check on every TLS-authenticated replica.
    /// Does not substitute for the per-operation clock/freshness guards.
    pub async fn health(&self, cfg: &ClusterConfig) -> Result<(), Box<dyn std::error::Error>> {
        let mut uuids = HashSet::new();
        for shard in &cfg.shards {
            let mut leaders = 0;
            let mut replicaset = None;
            for addr in &shard.replicas {
                let mut opts = Opts::new(addr);
                opts.user = Some(cfg.user.clone());
                opts.password = Some(cfg.password.clone());
                opts.tls = cfg.tls_config.clone();
                opts.request_timeout = std::time::Duration::from_secs(3);
                let c = Client::connect(&opts).await?;
                let r = c.call(0, "tarn_deployment_status", &[0x90]).await?;
                let bytes = r.data().ok_or("missing deployment status")?;
                let mut b = bytes.as_ref();
                if mp::read_array_len(&mut b)? != 1 || mp::read_array_len(&mut b)? != 11 {
                    return Err("invalid deployment status".into());
                }
                let production = mp::read_bool(&mut b)?;
                let wal = mp::read_str(&mut b)?;
                let rf = mp::read_uint(&mut b)? as usize;
                let quorum = mp::read_uint(&mut b)? as usize;
                let domain = std::str::from_utf8(mp::read_str(&mut b)?)?;
                let uuid = std::str::from_utf8(mp::read_str(&mut b)?)?;
                let id = mp::read_uint(&mut b)? as u32;
                let ready = mp::read_bool(&mut b)?;
                let strict = mp::read_bool(&mut b)?;
                let ro = mp::read_bool(&mut b)?;
                let set_uuid = std::str::from_utf8(mp::read_str(&mut b)?)?;
                if set_uuid.is_empty() || replicaset.as_ref().is_some_and(|s| s != set_uuid) {
                    return Err("replicas disagree on the replicaset identity".into());
                }
                replicaset = Some(set_uuid.to_owned());
                if !production
                    || wal != b"fsync"
                    || rf != shard.replicas.len()
                    || quorum != rf / 2 + 1
                    || self.failure_domains.get(addr).map(String::as_str) != Some(domain)
                    || uuid.is_empty()
                    || !uuids.insert(uuid.to_owned())
                    || id != shard.id
                    || !strict
                    || (!ro && !ready)
                    || !b.is_empty()
                {
                    return Err(
                        format!("production replica failed identity/durability/readiness checks: {addr} profile={production} wal={wal:?} rf={rf} quorum={quorum} domain={domain} shard={id} ready={ready} strict={strict} ro={ro}").into(),
                    );
                }
                for denied in [
                    c.eval(0, "return 1", &[0x90]).await,
                    c.call(0, "box.session.user", &[0x90]).await,
                    c.call(0, "tarn_clock_test_time", &[0x90]).await,
                    c.select(0, 304, 0, tarn_iproto::Iter::All, 0, 1, &[0x90]).await,
                ] {
                    if !matches!(denied, Err(tarn_iproto::Error::Server { code: 42, .. })) {
                        return Err(
                            format!("production service permissions exceed the RPC/data allowlist: {addr}").into(),
                        );
                    }
                }
                // Tarantool checks READONLY before access rights on writes;
                // the elected leader must independently prove raw-write denial.
                if !ro
                    && !matches!(
                        c.replace(0, 304, &[0x90]).await,
                        Err(tarn_iproto::Error::Server { code: 42, .. })
                    )
                {
                    return Err(format!("production service has raw-write authority: {addr}").into());
                }
                let mut guest_opts = opts.clone();
                guest_opts.user = None;
                guest_opts.password = None;
                if let Ok(guest) = Client::connect(&guest_opts).await {
                    let denied = guest.call(0, "tarn_role", &[0x90]).await;
                    guest.close();
                    if !matches!(denied, Err(tarn_iproto::Error::Server { code: 42, .. })) {
                        return Err("production guest can execute storage procedures".into());
                    }
                }
                leaders += usize::from(!ro);
                c.close();
            }
            if leaders != 1 {
                return Err("production requires exactly one ready leader per shard".into());
            }
        }
        Ok(())
    }
}
