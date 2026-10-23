//! Authenticated HTTPS server for cached observations.
//!
//! Clients must present a certificate chaining to the dedicated monitoring CA
//! that is also valid for the configured collector identity; any other
//! certificate from that CA is refused after the handshake. Requests only read
//! the cache; they never collect or run hooks.
use anyhow::{anyhow, Context, Result};
use http_body_util::Full;
use hyper::{body::Bytes, Method, Request, Response, StatusCode};
use rustls::{
    pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer, ServerName},
    server::WebPkiClientVerifier,
    RootCertStore, ServerConfig,
};
use std::{path::Path, sync::Arc, time::Duration};
use tokio::{net::TcpListener, sync::watch};
use tokio_rustls::TlsAcceptor;

#[derive(Clone, Default)]
pub struct Cache {
    pub observations: Bytes,
    pub edge_metrics: Option<Bytes>,
}

pub struct Identity {
    pub chain: Vec<CertificateDer<'static>>,
    pub key: PrivateKeyDer<'static>,
}

pub fn load_identity(cert: &Path, key: &Path) -> Result<Identity> {
    let chain = CertificateDer::pem_file_iter(cert)
        .context("reading serving certificate")?
        .collect::<Result<Vec<_>, _>>()
        .context("parsing serving certificate")?;
    anyhow::ensure!(!chain.is_empty(), "serving certificate file is empty");
    let key = PrivateKeyDer::from_pem_file(key).context("reading serving key")?;
    Ok(Identity { chain, key })
}

/// Expiry of the leaf serving certificate, in Unix seconds.
pub fn not_after(cert: &CertificateDer<'_>) -> Option<i64> {
    let (_, parsed) = x509_parser::parse_x509_certificate(cert.as_ref()).ok()?;
    Some(parsed.validity().not_after.timestamp())
}

pub fn tls_config(identity: Identity, client_ca: &Path) -> Result<Arc<ServerConfig>> {
    let mut roots = RootCertStore::empty();
    for cert in CertificateDer::pem_file_iter(client_ca).context("reading monitoring CA")? {
        roots
            .add(cert.context("parsing monitoring CA")?)
            .context("adding monitoring CA")?;
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let verifier = WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider.clone())
        .build()
        .map_err(|e| anyhow!("client verifier: {e}"))?;
    let mut config = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .with_client_cert_verifier(verifier)
        .with_single_cert(identity.chain, identity.key)
        .context("serving certificate and key do not match")?;
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(Arc::new(config))
}

/// Whether the authenticated client leaf certificate names the collector.
fn is_collector(peer: Option<&[CertificateDer<'_>]>, identity: &str) -> bool {
    let Some(leaf) = peer.and_then(|p| p.first()) else {
        return false;
    };
    let Ok(name) = ServerName::try_from(identity) else {
        return false;
    };
    webpki::EndEntityCert::try_from(leaf)
        .and_then(|cert| cert.verify_is_valid_for_subject_name(&name))
        .is_ok()
}

fn respond(status: StatusCode, content_type: &str, body: Bytes) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header("content-type", content_type)
        .header("cache-control", "no-store")
        .body(Full::new(body))
        .expect("static response")
}

fn route(req: &Request<hyper::body::Incoming>, cache: &Cache) -> Response<Full<Bytes>> {
    if req.method() != Method::GET {
        return respond(StatusCode::METHOD_NOT_ALLOWED, "text/plain", Bytes::new());
    }
    match req.uri().path() {
        pir_host_observations::PATH if !cache.observations.is_empty() => respond(
            StatusCode::OK,
            "application/json",
            cache.observations.clone(),
        ),
        pir_host_observations::PATH => {
            respond(StatusCode::SERVICE_UNAVAILABLE, "text/plain", Bytes::new())
        }
        "/v1/edge-metrics" => match &cache.edge_metrics {
            Some(body) => respond(StatusCode::OK, "text/plain; version=0.0.4", body.clone()),
            None => respond(StatusCode::NOT_FOUND, "text/plain", Bytes::new()),
        },
        _ => respond(StatusCode::NOT_FOUND, "text/plain", Bytes::new()),
    }
}

pub async fn serve(
    listener: TcpListener,
    config: Arc<ServerConfig>,
    collector_identity: String,
    cache: watch::Receiver<Cache>,
) -> Result<()> {
    let acceptor = TlsAcceptor::from(config);
    let permits = Arc::new(tokio::sync::Semaphore::new(64));
    loop {
        let (stream, _) = listener.accept().await.context("accepting connection")?;
        let Ok(permit) = permits.clone().try_acquire_owned() else {
            continue;
        };
        let acceptor = acceptor.clone();
        let identity = collector_identity.clone();
        let cache = cache.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let Ok(Ok(tls)) =
                tokio::time::timeout(Duration::from_secs(5), acceptor.accept(stream)).await
            else {
                return;
            };
            if !is_collector(tls.get_ref().1.peer_certificates(), &identity) {
                // Same CA but not the collector: no response at all.
                return;
            }
            let service = hyper::service::service_fn(move |req| {
                let response = route(&req, &cache.borrow());
                async move { Ok::<_, std::convert::Infallible>(response) }
            });
            let connection = hyper::server::conn::http1::Builder::new()
                .serve_connection(hyper_util::rt::TokioIo::new(tls), service);
            let _ = tokio::time::timeout(Duration::from_secs(30), connection).await;
        });
    }
}
