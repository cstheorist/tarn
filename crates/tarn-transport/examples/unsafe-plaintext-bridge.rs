//! Counterfactual cost control only. Loopback-only, same TCP/Unix hops and
//! copy buffers as tarn-tls. Never install this example in a deployment.
use serde::Deserialize;
use std::{io, path::PathBuf, sync::Arc};
use tokio::{
    io::copy_bidirectional,
    net::{TcpListener, TcpStream, UnixListener, UnixStream},
    sync::Semaphore,
};
#[derive(Deserialize)]
struct Config {
    max_connections: usize,
    servers: Vec<Server>,
    clients: Vec<Client>,
}
#[derive(Deserialize)]
struct Server {
    listen: String,
    upstream_unix: PathBuf,
}
#[derive(Deserialize)]
struct Client {
    listen_unix: PathBuf,
    upstream: String,
}
fn loopback(addr: &str) -> io::Result<std::net::SocketAddr> {
    let a: std::net::SocketAddr = addr.parse().map_err(io::Error::other)?;
    if !a.ip().is_loopback() {
        return Err(io::Error::other("unsafe cost control is loopback-only"));
    }
    Ok(a)
}
#[tokio::main]
async fn main() -> io::Result<()> {
    eprintln!("UNSAFE DEVELOPMENT COST CONTROL: NO ENCRYPTION OR PEER AUTHENTICATION");
    let p = std::env::args()
        .nth(1)
        .ok_or_else(|| io::Error::other("usage: unsafe-plaintext-bridge CONFIG"))?;
    let c: Config = serde_json::from_slice(&std::fs::read(p)?).map_err(io::Error::other)?;
    let slots = Arc::new(Semaphore::new(c.max_connections));
    let mut tasks = tokio::task::JoinSet::new();
    for s in c.servers {
        let l = TcpListener::bind(loopback(&s.listen)?).await?;
        let slots = slots.clone();
        tasks.spawn(async move {
            loop {
                let (mut remote, _) = l.accept().await?;
                let Ok(permit) = slots.clone().try_acquire_owned() else {
                    continue;
                };
                let path = s.upstream_unix.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    if let Ok(mut local) = UnixStream::connect(path).await {
                        let _ = copy_bidirectional(&mut remote, &mut local).await;
                    }
                });
            }
            #[allow(unreachable_code)]
            Ok::<(), io::Error>(())
        });
    }
    for c in c.clients {
        let (addr, _) = tarn_transport::tls_endpoint(&c.upstream)?;
        let addr = loopback(addr)?;
        let l = UnixListener::bind(c.listen_unix)?;
        let slots = slots.clone();
        tasks.spawn(async move {
            loop {
                let (mut local, _) = l.accept().await?;
                let Ok(permit) = slots.clone().try_acquire_owned() else {
                    continue;
                };
                tokio::spawn(async move {
                    let _permit = permit;
                    if let Ok(Ok(mut remote)) =
                        tokio::time::timeout(std::time::Duration::from_secs(5), TcpStream::connect(addr)).await
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
        _ => Err(io::Error::other("cost listener stopped")),
    }
}
