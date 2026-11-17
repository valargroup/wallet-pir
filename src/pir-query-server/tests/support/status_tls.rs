//! Real TLS terminator over the real product HTTP routes, with transport faults.
use std::{path::PathBuf, process::Stdio, sync::OnceLock};
use tokio::io::{AsyncBufReadExt, BufReader};
pub struct Tls {
    pub origin: String,
    pub certificate: PathBuf,
    #[allow(dead_code)] // Fault injection is used by consumer tests, not every lifecycle caller.
    pub control: PathBuf,
    _root: tempfile::TempDir,
    _child: tokio::process::Child,
}
#[allow(dead_code)] // Consumers use either the product or root-host discovery helper.
impl Tls {
    pub async fn start(upstream: &str) -> Self {
        Self::with_discovery(upstream, upstream).await
    }
    /// A root publisher serves discovery on its management listener.
    pub async fn with_discovery(upstream: &str, management: &str) -> Self {
        let root = tempfile::tempdir().unwrap();
        let certificate = root.path().join("certificate.pem");
        let key = root.path().join("key.pem");
        let control = root.path().join("fault");
        std::fs::write(&control, "").unwrap();
        let (key_bytes, cert_bytes) = localhost_material().clone();
        std::fs::write(&key, key_bytes).unwrap();
        std::fs::write(&certificate, cert_bytes).unwrap();
        let support = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let workspace = support
            .ancestors()
            .find(|p| p.join("tools/ci/fast.py").is_file())
            .unwrap();
        let mut child = tokio::process::Command::new("python3")
            .env(
                "DISCOVERY_UPSTREAM",
                management.strip_prefix("http://").unwrap(),
            )
            .arg(workspace.join("src/pir-publisher/tests/support/distribution_tls.py"))
            .arg(upstream.strip_prefix("http://").unwrap())
            .arg(&certificate)
            .arg(&key)
            .arg(&control)
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut line = String::new();
        BufReader::new(child.stdout.take().unwrap())
            .read_line(&mut line)
            .await
            .unwrap();
        let port: u16 = line
            .trim()
            .parse()
            .expect("TLS terminator did not report its port");
        Self {
            origin: format!("https://localhost:{port}"),
            certificate,
            control,
            _root: root,
            _child: child,
        }
    }
}

/// One localhost certificate for every case in this process.
///
/// Each terminator still has its own files and listener. Generating a 2048-bit
/// key per case was repeated work with no bearing on the route under test.
fn localhost_material() -> &'static (Vec<u8>, Vec<u8>) {
    static MATERIAL: OnceLock<(Vec<u8>, Vec<u8>)> = OnceLock::new();
    MATERIAL.get_or_init(|| {
        let root = tempfile::tempdir().unwrap();
        let key = root.path().join("key.pem");
        let certificate = root.path().join("certificate.pem");
        let output = std::process::Command::new("openssl")
            .args([
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "1",
                "-subj",
                "/CN=localhost",
                "-addext",
                "subjectAltName=DNS:localhost",
                "-addext",
                "basicConstraints=critical,CA:FALSE",
                "-keyout",
            ])
            .arg(&key)
            .arg("-out")
            .arg(&certificate)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "TLS test certificate generation failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        (
            std::fs::read(&key).unwrap(),
            std::fs::read(&certificate).unwrap(),
        )
    })
}
