//! Real mTLS boundary: only the collector identity from the monitoring CA reads.
use hyper::body::Bytes;
use pir_apm_sidecar::server;
use rcgen::{BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair};
use std::{path::Path, time::Duration};

struct Ca {
    cert: rcgen::Certificate,
    key: KeyPair,
}

fn ca() -> Ca {
    let key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(vec![]).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    Ca {
        cert: params.self_signed(&key).unwrap(),
        key,
    }
}

fn leaf(ca: &Ca, name: &str, usage: ExtendedKeyUsagePurpose) -> (String, String) {
    let key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(vec![name.into()]).unwrap();
    params.extended_key_usages = vec![usage];
    let cert = params.signed_by(&key, &ca.cert, &ca.key).unwrap();
    (cert.pem(), key.serialize_pem())
}

fn write(dir: &Path, name: &str, text: &str) -> std::path::PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, text).unwrap();
    path
}

async fn start(dir: &Path, monitoring: &Ca) -> std::net::SocketAddr {
    let (cert, key) = leaf(
        monitoring,
        "coordinator-01.pir-monitoring",
        ExtendedKeyUsagePurpose::ServerAuth,
    );
    let identity = server::load_identity(
        &write(dir, "server.pem", &cert),
        &write(dir, "server.key", &key),
    )
    .unwrap();
    assert!(server::not_after(&identity.chain[0]).is_some());
    let config =
        server::tls_config(identity, &write(dir, "ca.pem", &monitoring.cert.pem())).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (_tx, rx) = tokio::sync::watch::channel(server::Cache {
        observations: Bytes::from_static(b"{\"schema_version\":1}"),
        edge_metrics: None,
    });
    tokio::spawn(async move {
        let _tx = _tx;
        server::serve(listener, config, "pir-apm-collector".into(), rx).await
    });
    addr
}

fn client(
    trust: &Ca,
    identity: Option<(String, String)>,
    addr: std::net::SocketAddr,
) -> reqwest::Client {
    let mut builder = reqwest::Client::builder()
        .use_rustls_tls()
        .tls_built_in_root_certs(false)
        .add_root_certificate(reqwest::Certificate::from_pem(trust.cert.pem().as_bytes()).unwrap())
        .resolve("coordinator-01.pir-monitoring", addr)
        .timeout(Duration::from_secs(5));
    if let Some((cert, key)) = identity {
        builder = builder
            .identity(reqwest::Identity::from_pem(format!("{cert}{key}").as_bytes()).unwrap());
    }
    builder.build().unwrap()
}

async fn get(client: &reqwest::Client, addr: std::net::SocketAddr, path: &str) -> Result<u16, ()> {
    client
        .get(format!(
            "https://coordinator-01.pir-monitoring:{}{path}",
            addr.port()
        ))
        .send()
        .await
        .map(|r| r.status().as_u16())
        .map_err(|_| ())
}

#[tokio::test]
async fn only_the_collector_identity_from_the_monitoring_ca_is_served() {
    let dir = tempfile::tempdir().unwrap();
    let monitoring = ca();
    let other = ca();
    let addr = start(dir.path(), &monitoring).await;
    let collector = leaf(
        &monitoring,
        "pir-apm-collector",
        ExtendedKeyUsagePurpose::ClientAuth,
    );

    let authorized = client(&monitoring, Some(collector.clone()), addr);
    assert_eq!(get(&authorized, addr, "/v1/observations").await, Ok(200));
    assert_eq!(get(&authorized, addr, "/v1/edge-metrics").await, Ok(404));
    assert_eq!(get(&authorized, addr, "/").await, Ok(404));

    // No client certificate: the handshake requires one.
    assert!(
        get(&client(&monitoring, None, addr), addr, "/v1/observations")
            .await
            .is_err()
    );
    // Same CA, other identity: e.g. another host's server certificate used as a client.
    let peer = leaf(
        &monitoring,
        "worker-01.pir-monitoring",
        ExtendedKeyUsagePurpose::ClientAuth,
    );
    assert!(get(
        &client(&monitoring, Some(peer), addr),
        addr,
        "/v1/observations"
    )
    .await
    .is_err());
    // Collector name, foreign CA.
    let forged = leaf(
        &other,
        "pir-apm-collector",
        ExtendedKeyUsagePurpose::ClientAuth,
    );
    assert!(get(
        &client(&monitoring, Some(forged), addr),
        addr,
        "/v1/observations"
    )
    .await
    .is_err());
    // The collector refuses a server outside the monitoring CA.
    let wrong_trust = client(&other, Some(collector), addr);
    assert!(get(&wrong_trust, addr, "/v1/observations").await.is_err());
}

#[test]
fn check_mode_reports_the_file_digest_and_expected_checks() {
    use sha2::Digest;
    let dir = tempfile::tempdir().unwrap();
    let monitoring = ca();
    let (cert, key) = leaf(
        &monitoring,
        "h.pir-monitoring",
        ExtendedKeyUsagePurpose::ServerAuth,
    );
    write(dir.path(), "server.pem", &cert);
    write(dir.path(), "server.key", &key);
    write(dir.path(), "ca.pem", &monitoring.cert.pem());
    let config = serde_json::json!({
        "host_id": "h",
        "tls": {"cert": "server.pem", "key": "server.key", "client_ca": "ca.pem",
                "collector_identity": "pir-apm-collector"},
        "required_volumes": [{"path": "/srv/data", "uuid": "0a1b-2c3d"}],
        "units": ["pir-apm.service"],
        "state_path": dir.path().join("state.json"),
    });
    let path = write(dir.path(), "sidecar.json", &config.to_string());
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pir-apm-sidecar"))
        .args(["--config", path.to_str().unwrap(), "--check"])
        .env("CREDENTIALS_DIRECTORY", dir.path())
        .output()
        .unwrap();
    assert!(output.status.success());
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let expected = format!(
        "sha256:{}",
        hex::encode(sha2::Sha256::digest(std::fs::read(&path).unwrap()))
    );
    assert_eq!(report["config_digest"], expected.as_str());
    let checks: Vec<&str> = report["checks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c.as_str().unwrap())
        .collect();
    assert!(checks.contains(&"volume:/srv/data"));
    assert!(checks.contains(&"unit:pir-apm.service:oom"));
}
