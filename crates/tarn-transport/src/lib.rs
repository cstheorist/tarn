//! TLS is established before an application stream is returned. There is
//! no plaintext fallback or application retry here. A failed write after
//! enqueue remains uncertain in the caller's existing protocol.

use bytes::{Buf, Bytes};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use serde::Deserialize;
use std::io::{self, BufReader, Read};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
pub use tokio_rustls::rustls;
pub use tokio_rustls::{TlsAcceptor, TlsConnector};

pub trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}
pub type BoxIo = Box<dyn Io>;

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

/// Sensitive files must be regular, owned by this process's UID, and
/// inaccessible to group/other. Check the opened descriptor, not a prior
/// pathname lookup. Rotation uses atomic rename of a new private file.
pub fn read_private(path: &Path, max: usize) -> io::Result<Vec<u8>> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
        let mut f = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)?;
        let meta = f.metadata()?;
        // SAFETY: geteuid has no arguments and no failure state.
        let uid = unsafe { libc::geteuid() };
        if !meta.is_file() || meta.uid() != uid || meta.mode() & 0o077 != 0 {
            return Err(invalid(
                "secret must be a private regular file owned by the service UID",
            ));
        }
        let mut bytes = Vec::new();
        (&mut f).take(max as u64 + 1).read_to_end(&mut bytes)?;
        if bytes.len() > max {
            return Err(invalid("secret file exceeds its size limit"));
        }
        Ok(bytes)
    }
    #[cfg(not(unix))]
    {
        let _ = (path, max);
        Err(invalid("private secret files require Unix ownership and mode checks"))
    }
}

pub fn read_secret(path: &Path) -> io::Result<String> {
    let mut bytes = read_private(path, 65_536)?;
    if bytes.last() == Some(&b'\n') {
        bytes.pop();
        if bytes.last() == Some(&b'\r') {
            bytes.pop();
        }
    }
    let text = String::from_utf8(bytes).map_err(|_| invalid("secret must be UTF-8"))?;
    if text.is_empty() || text.contains(['\0', '\n', '\r']) {
        return Err(invalid("secret must be one nonempty line"));
    }
    Ok(text)
}

/// Public configuration is not confidential, but its integrity is critical.
/// Accept service/root ownership, refuse other writers, and bound the read.
pub fn read_config(path: &Path, max: usize) -> io::Result<Vec<u8>> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    let f = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)?;
    let m = f.metadata()?;
    // SAFETY: geteuid has no arguments or failure state.
    if !m.is_file() || (m.uid() != unsafe { libc::geteuid() } && m.uid() != 0) || m.mode() & 0o022 != 0 {
        return Err(invalid(
            "configuration must be regular, service/root owned and not group/other writable",
        ));
    }
    let mut bytes = Vec::new();
    f.take(max as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > max {
        return Err(invalid("configuration exceeds its size limit"));
    }
    Ok(bytes)
}
fn certificates(path: &Path) -> io::Result<Vec<CertificateDer<'static>>> {
    let bytes = read_config(path, 1_048_576)?;
    let certs = rustls_pemfile::certs(&mut BufReader::new(bytes.as_slice())).collect::<Result<Vec<_>, _>>()?;
    if certs.is_empty() {
        return Err(invalid("certificate file contains no certificates"));
    }
    Ok(certs)
}

fn private_key(path: &Path) -> io::Result<PrivateKeyDer<'static>> {
    let bytes = read_private(path, 1_048_576)?;
    rustls_pemfile::private_key(&mut bytes.as_slice())?
        .ok_or_else(|| invalid("private key file contains no supported key"))
}

fn roots(path: &Path) -> io::Result<rustls::RootCertStore> {
    let mut roots = rustls::RootCertStore::empty();
    for cert in certificates(path)? {
        roots.add(cert).map_err(|_| invalid("invalid CA certificate"))?;
    }
    Ok(roots)
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientTls {
    pub ca_file: PathBuf,
    pub cert_file: Option<PathBuf>,
    pub key_file: Option<PathBuf>,
}

impl ClientTls {
    pub fn load(&self) -> io::Result<Arc<rustls::ClientConfig>> {
        let builder = rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .map_err(|_| invalid("no supported TLS protocol versions"))?
            .with_root_certificates(roots(&self.ca_file)?);
        let config = match (&self.cert_file, &self.key_file) {
            (Some(cert), Some(key)) => builder
                .with_client_auth_cert(certificates(cert)?, private_key(key)?)
                .map_err(|_| invalid("invalid TLS client identity"))?,
            (None, None) => builder.with_no_client_auth(),
            _ => return Err(invalid("TLS client certificate and key must be supplied together")),
        };
        Ok(Arc::new(config))
    }

    pub fn has_identity(&self) -> bool {
        self.cert_file.is_some() && self.key_file.is_some()
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerTls {
    pub cert_file: PathBuf,
    pub key_file: PathBuf,
    /// When present, every client must present a certificate from this CA.
    pub client_ca_file: Option<PathBuf>,
}

impl ServerTls {
    pub fn load(&self) -> io::Result<Arc<rustls::ServerConfig>> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let builder = rustls::ServerConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .map_err(|_| invalid("no supported TLS protocol versions"))?;
        let builder = match &self.client_ca_file {
            Some(path) => builder.with_client_cert_verifier(
                rustls::server::WebPkiClientVerifier::builder_with_provider(Arc::new(roots(path)?), provider)
                    .build()
                    .map_err(|_| invalid("invalid TLS client CA"))?,
            ),
            None => builder.with_no_client_auth(),
        };
        let config = builder
            .with_single_cert(certificates(&self.cert_file)?, private_key(&self.key_file)?)
            .map_err(|_| invalid("invalid TLS server identity"))?;
        Ok(Arc::new(config))
    }
}

/// Require a TLS URI and derive certificate verification/SNI from the
/// actual endpoint. No userinfo, query overrides, or alternate server name.
pub fn tls_endpoint(uri: &str) -> io::Result<(&str, ServerName<'static>)> {
    let authority = uri
        .strip_prefix("tls://")
        .ok_or_else(|| invalid("TLS endpoint requires tls://"))?;
    if authority.contains(['/', '@', '?', '#']) {
        return Err(invalid("invalid TLS endpoint authority"));
    }
    let (host, port) = if let Some(ip) = authority.strip_prefix('[') {
        let (host, rest) = ip.split_once(']').ok_or_else(|| invalid("invalid TLS IPv6 endpoint"))?;
        (
            host,
            rest.strip_prefix(':')
                .ok_or_else(|| invalid("TLS endpoint requires a port"))?,
        )
    } else {
        authority
            .rsplit_once(':')
            .ok_or_else(|| invalid("TLS endpoint requires a port"))?
    };
    if port.parse::<u16>().ok().filter(|n| *n > 0).is_none() {
        return Err(invalid("invalid TLS endpoint port"));
    }
    let name = ServerName::try_from(host.to_string()).map_err(|_| invalid("invalid TLS verification name"))?;
    Ok((authority, name))
}

pub async fn connect(
    uri: &str,
    config: Arc<rustls::ClientConfig>,
) -> io::Result<tokio_rustls::client::TlsStream<TcpStream>> {
    let (addr, name) = tls_endpoint(uri)?;
    let tcp = TcpStream::connect(addr).await?;
    tcp.set_nodelay(true)?;
    TlsConnector::from(config).connect(name, tcp).await
}

/// A MySQL SSLRequest and TLS ClientHello may share one TCP read. Preserve
/// every prefetched TLS byte when moving the socket into the TLS acceptor.
pub struct PrefixedIo {
    pub prefix: Bytes,
    pub io: BoxIo,
}

impl AsyncRead for PrefixedIo {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, out: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        if !self.prefix.is_empty() {
            let n = out.remaining().min(self.prefix.len());
            out.put_slice(&self.prefix[..n]);
            self.prefix.advance(n);
            Poll::Ready(Ok(()))
        } else {
            Pin::new(&mut self.io).poll_read(cx, out)
        }
    }
}
impl AsyncWrite for PrefixedIo {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, bytes: &[u8]) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}

pub mod host;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn secrets_refuse_other_readers_and_symlinks() {
        use std::os::unix::fs::{symlink, PermissionsExt};
        let dir = std::env::temp_dir().join(format!("tarn-secret-test-{}", std::process::id()));
        std::fs::create_dir(&dir).unwrap();
        let file = dir.join("secret");
        std::fs::write(&file, b"sensitive\n").unwrap();
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(read_secret(&file).is_err());
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(read_secret(&file).unwrap(), "sensitive");
        let link = dir.join("link");
        symlink(&file, &link).unwrap();
        assert!(read_secret(&link).is_err());
        assert!(read_config(&link, 100).is_err());
        assert!(read_private(&dir, 100).is_err());
        assert!(read_private(&file, 2).is_err());
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o666)).unwrap();
        assert!(read_config(&file, 100).is_err());
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)).unwrap();
        std::fs::write(&file, b"line1\nline2").unwrap();
        assert!(read_secret(&file).is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn tls_authorities_cannot_override_identity_verification() {
        for bad in [
            "localhost:3301",
            "tls://localhost",
            "tls://user@localhost:3301",
            "tls://localhost:0",
            "tls://localhost:3301?verify=false",
            "tls://localhost:3301/path",
        ] {
            assert!(tls_endpoint(bad).is_err(), "{bad}");
        }
        assert!(tls_endpoint("tls://localhost:3301").is_ok());
        assert!(tls_endpoint("tls://[::1]:3301").is_ok());
    }
}
