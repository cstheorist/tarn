//! Community Edition TLS boundary: mutual TLS <-> private Unix sockets.
//! Each accepted stream gets exactly one upstream connection. Never replay.
use serde::Deserialize;
use std::{
    io,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tarn_transport::{ClientTls, ServerTls, TlsAcceptor};
use tokio::{
    io::copy_bidirectional,
    net::{TcpListener, UnixListener, UnixStream},
    sync::Semaphore,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    max_connections: usize,
    servers: Vec<Server>,
    clients: Vec<Client>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Server {
    listen: String,
    upstream_unix: PathBuf,
    tls: ServerTls,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Client {
    listen_unix: PathBuf,
    upstream: String,
    tls: ClientTls,
}
fn private_parent(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::MetadataExt;
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::other("socket requires an absolute private parent"))?;
    let m = std::fs::symlink_metadata(parent)?;
    // SAFETY: geteuid has no arguments or failure state.
    if !path.is_absolute() || !m.is_dir() || m.uid() != unsafe { libc::geteuid() } || m.mode() & 0o077 != 0 {
        return Err(io::Error::other(
            "socket directory must be absolute, private, and owned by service UID",
        ));
    }
    Ok(())
}
#[tokio::main]
async fn main() -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let path = std::env::args()
        .nth(1)
        .ok_or_else(|| io::Error::other("usage: tarn-tls CONFIG.json"))?;
    let c: Config = serde_json::from_slice(&tarn_transport::read_config(Path::new(&path), 1_048_576)?)
        .map_err(|_| io::Error::other("invalid bridge configuration"))?;
    if c.max_connections == 0 || c.max_connections > 100_000 || c.servers.is_empty() && c.clients.is_empty() {
        return Err(io::Error::other(
            "bridge requires 1..100000 connections and at least one listener",
        ));
    }
    let slots = Arc::new(Semaphore::new(c.max_connections));
    let mut tasks = tokio::task::JoinSet::new();
    for s in c.servers {
        private_parent(&s.upstream_unix)?;
        if s.tls.client_ca_file.is_none() {
            return Err(io::Error::other("storage TLS requires client certificates"));
        }
        let tls = TlsAcceptor::from(s.tls.load()?);
        let listener = TcpListener::bind(&s.listen).await?;
        let slots = slots.clone();
        tasks.spawn(async move {
            loop {
                let (tcp, _) = listener.accept().await?;
                tcp.set_nodelay(true)?;
                let Ok(permit) = slots.clone().try_acquire_owned() else {
                    drop(tcp);
                    continue;
                };
                let accept = tls.clone();
                let upstream = s.upstream_unix.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    // TLS authentication completes before connecting to storage.
                    let Ok(Ok(mut remote)) = tokio::time::timeout(Duration::from_secs(5), accept.accept(tcp)).await
                    else {
                        return;
                    };
                    if let Ok(mut local) = UnixStream::connect(upstream).await {
                        let _ = copy_bidirectional(&mut remote, &mut local).await;
                    }
                });
            }
            #[allow(unreachable_code)]
            Ok::<(), io::Error>(())
        });
    }
    for c in c.clients {
        private_parent(&c.listen_unix)?;
        tarn_transport::tls_endpoint(&c.upstream)?;
        if !c.tls.has_identity() {
            return Err(io::Error::other("storage TLS requires a client identity"));
        }
        let tls = c.tls.load()?;
        // Do not unlink a live or stale socket automatically. An operator must
        // establish ownership before cleaning one up after a crash.
        let listener = UnixListener::bind(&c.listen_unix)?;
        std::fs::set_permissions(&c.listen_unix, std::fs::Permissions::from_mode(0o600))?;
        let slots = slots.clone();
        tasks.spawn(async move {
            loop {
                let (mut local, _) = listener.accept().await?;
                let Ok(permit) = slots.clone().try_acquire_owned() else {
                    drop(local);
                    continue;
                };
                let tls = tls.clone();
                let upstream = c.upstream.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    if let Ok(Ok(mut remote)) =
                        tokio::time::timeout(Duration::from_secs(5), tarn_transport::connect(&upstream, tls)).await
                    {
                        let _ = copy_bidirectional(&mut local, &mut remote).await;
                    }
                });
            }
            #[allow(unreachable_code)]
            Ok::<(), io::Error>(())
        });
    }
    match tasks.join_next().await {
        Some(Ok(r)) => r,
        _ => Err(io::Error::other("bridge listener stopped")),
    }
}
