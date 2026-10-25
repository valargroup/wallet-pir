//! Spawned `pir-query-server` hosts for Enhance role regressions.

pub fn free() -> String {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .to_string()
}

/// One spawned `pir-query-server`, logging both streams to a file.
pub struct Host {
    child: tokio::process::Child,
    log: std::path::PathBuf,
    pub management: String,
}
#[allow(dead_code)]
impl Host {
    pub async fn start(dir: &std::path::Path, name: &str, document: serde_json::Value) -> Self {
        let config = dir.join(format!("{name}.json"));
        std::fs::write(&config, serde_json::to_vec_pretty(&document).unwrap()).unwrap();
        let log = dir.join(format!("{name}.log"));
        let file = std::fs::File::create(&log).unwrap();
        let child = tokio::process::Command::new(env!("CARGO_BIN_EXE_pir-query-server-test"))
            .arg("--config")
            .arg(&config)
            .env(
                "RUST_LOG",
                std::env::var("RUST_LOG").unwrap_or("info".into()),
            )
            .kill_on_drop(true)
            .stdout(file.try_clone().unwrap())
            .stderr(file)
            .spawn()
            .unwrap();
        Self {
            child,
            log,
            management: document["management_listen"].as_str().unwrap().into(),
        }
    }
    pub fn log(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }
    /// Waits for the host's typed readiness; a host that exits fails at once
    /// with its own output.
    pub async fn ready(&mut self) {
        let mut report = String::new();
        for _ in 0..1200 {
            if let Ok(response) = reqwest::get(format!("http://{}/ready", self.management)).await {
                let ready = response.status().is_success();
                report = response.text().await.unwrap_or_default();
                if ready {
                    return;
                }
            }
            self.assert_running();
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        panic!("host never became ready: {report}\n{}", self.log());
    }
    /// Fails with the host's own output if it has exited.
    pub fn assert_running(&mut self) {
        if let Some(status) = self.child.try_wait().unwrap() {
            panic!("host exited with {status}: {}", self.log());
        }
    }
    pub async fn stop(mut self) {
        self.child.kill().await.unwrap();
        self.child.wait().await.unwrap();
    }
    /// Requests the host's own shutdown, as systemd does, and waits for it to
    /// drain and exit.
    pub async fn terminate(mut self) -> std::process::ExitStatus {
        let pid = self.child.id().unwrap() as libc::pid_t;
        assert_eq!(unsafe { libc::kill(pid, libc::SIGTERM) }, 0);
        tokio::time::timeout(std::time::Duration::from_secs(90), self.child.wait())
            .await
            .unwrap_or_else(|_| panic!("host did not stop: {}", self.log()))
            .unwrap()
    }
}
