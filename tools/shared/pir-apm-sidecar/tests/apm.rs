//! The central APM poller against a real sidecar server over mTLS.
use hyper::body::Bytes;
use pir_apm::observers::{self, Collector, Host, Poll, Settings};
use pir_apm_sidecar::{config, server};
use rcgen::{BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair};
use std::path::Path;

fn ca() -> (rcgen::Certificate, KeyPair) {
    let key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(vec![]).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    (params.self_signed(&key).unwrap(), key)
}

fn leaf(
    dir: &Path,
    file: &str,
    ca: &(rcgen::Certificate, KeyPair),
    name: &str,
    usage: ExtendedKeyUsagePurpose,
) {
    let key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(vec![name.into()]).unwrap();
    params.extended_key_usages = vec![usage];
    let cert = params.signed_by(&key, &ca.0, &ca.1).unwrap();
    std::fs::write(dir.join(format!("{file}.pem")), cert.pem()).unwrap();
    std::fs::write(dir.join(format!("{file}.key")), key.serialize_pem()).unwrap();
}

#[tokio::test]
async fn apm_accepts_only_authenticated_matching_documents() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let monitoring = ca();
    std::fs::write(d.join("ca.pem"), monitoring.0.pem()).unwrap();
    leaf(
        d,
        "server",
        &monitoring,
        "w1.pir-monitoring",
        ExtendedKeyUsagePurpose::ServerAuth,
    );
    leaf(
        d,
        "collector",
        &monitoring,
        "pir-apm-collector",
        ExtendedKeyUsagePurpose::ClientAuth,
    );
    leaf(
        d,
        "peer",
        &monitoring,
        "w2.pir-monitoring",
        ExtendedKeyUsagePurpose::ClientAuth,
    );

    let file = serde_json::json!({
        "host_id": "w1",
        "tls": {"cert": d.join("server.pem"), "key": d.join("server.key"),
                "client_ca": d.join("ca.pem"), "collector_identity": "pir-apm-collector"},
        "state_path": d.join("state.json"),
    })
    .to_string();
    std::fs::write(d.join("sidecar.json"), &file).unwrap();
    let loaded = config::Config::load(&d.join("sidecar.json")).unwrap();
    let identity = server::load_identity(&d.join("server.pem"), &d.join("server.key")).unwrap();
    let not_after = server::not_after(&identity.chain[0]);
    let tls = server::tls_config(identity, &d.join("ca.pem")).unwrap();
    let mut state = Default::default();
    let (checks, _) = pir_apm_sidecar::collect(&loaded.config, &mut state, not_after).await;
    let doc = pir_apm_sidecar::document(&loaded.config, &loaded.digest, "i1", checks);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (tx, rx) = tokio::sync::watch::channel(server::Cache {
        observations: Bytes::from(serde_json::to_vec(&doc).unwrap()),
        edge_metrics: None,
    });
    tokio::spawn(server::serve(listener, tls, "pir-apm-collector".into(), rx));

    let settings = |client: &str| Settings {
        mode: "shadow".into(),
        collector: Collector {
            cert: d.join(format!("{client}.pem")),
            key: d.join(format!("{client}.key")),
            ca: d.join("ca.pem"),
        },
        hosts: vec![],
        decommissioned: vec![],
        roster: None,
        host_snapshot: None,
    };
    let host = |id: &str, digest: &str, checks: &[&str]| Host {
        host_id: id.into(),
        address,
        tls_name: "w1.pir-monitoring".into(),
        config_digest: digest.into(),
        checks: checks.iter().map(|c| c.to_string()).collect(),
        edge_output: None,
        snapshot: None,
    };
    let poll = |s: Settings, h: Host| async move {
        observers::poll(&observers::client(&s, &h).unwrap(), &h).await
    };
    let expected = loaded.config.expected_checks();
    let expected: Vec<&str> = expected.iter().map(String::as_str).collect();
    let good = host("w1", &loaded.digest, &expected);
    match poll(settings("collector"), good.clone()).await {
        Poll::Valid(doc) => assert!(doc.checks.iter().any(|c| c.name == "certificate")),
        Poll::Failed(reason) => panic!("authorized poll failed: {reason}"),
    }
    let failed = |p: Poll| match p {
        Poll::Failed(reason) => reason,
        Poll::Valid(_) => panic!("accepted"),
    };
    let other = format!("sha256:{}", "f".repeat(64));
    assert_eq!(
        failed(poll(settings("collector"), host("w1", &other, &[])).await),
        "sidecar configuration digest does not match inventory"
    );
    assert_eq!(
        failed(poll(settings("collector"), host("w9", &loaded.digest, &[])).await),
        "reported host identity does not match enrollment"
    );
    assert_eq!(
        failed(
            poll(
                settings("collector"),
                host("w1", &loaded.digest, &["volume:/srv/x"])
            )
            .await
        ),
        "observation lacks configured checks"
    );
    assert_eq!(
        failed(poll(settings("peer"), good.clone()).await),
        "sidecar unreachable or authentication failed"
    );
    let mut wrong_name = good.clone();
    wrong_name.tls_name = "w2.pir-monitoring".into();
    assert_eq!(
        failed(poll(settings("collector"), wrong_name).await),
        "sidecar unreachable or authentication failed"
    );
    // An incompatible schema and an empty cache are rejected too.
    let mut v2 = serde_json::to_value(&doc).unwrap();
    v2["schema_version"] = 2.into();
    tx.send_replace(server::Cache {
        observations: Bytes::from(v2.to_string()),
        edge_metrics: None,
    });
    assert_eq!(
        failed(poll(settings("collector"), good.clone()).await),
        "incompatible observation schema"
    );
    tx.send_replace(server::Cache::default());
    assert_eq!(
        failed(poll(settings("collector"), good).await),
        "sidecar returned an error status"
    );
}
