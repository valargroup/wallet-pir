//! Real TLS terminator over the real product HTTP routes, with transport faults.
use std::{path::PathBuf, process::Stdio};
use tokio::io::{AsyncBufReadExt, BufReader};
pub struct Tls {
    pub origin: String,
    pub certificate: PathBuf,
    #[allow(dead_code)] // Fault injection is used by consumer tests, not every lifecycle caller.
    pub control: PathBuf,
    _root: tempfile::TempDir,
    _child: tokio::process::Child,
}
impl Tls {
    pub async fn start(upstream: &str) -> Self {
        let root = tempfile::tempdir().unwrap();
        let certificate = root.path().join("certificate.pem");
        let key = root.path().join("key.pem");
        let control = root.path().join("fault");
        std::fs::write(&control, "").unwrap();
        let output = tokio::process::Command::new("openssl")
            .args([
                "req",
                "-x509",
                "-newkey",
                "ec",
                "-pkeyopt",
                "ec_paramgen_curve:prime256v1",
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
            .await
            .unwrap();
        assert!(
            output.status.success(),
            "TLS test certificate generation failed"
        );
        let support = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let workspace = support
            .ancestors()
            .find(|p| p.join("tools/ci/fast.py").is_file())
            .unwrap();
        let mut child = tokio::process::Command::new("python3")
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
