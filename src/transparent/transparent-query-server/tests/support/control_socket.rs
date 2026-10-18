//! A publication owner's real boundaries for tests: its control socket, its
//! HTTP listener on loopback, and a durable record write held open on disk.

use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// Sends one command line and waits up to `wait` for its reply line; `None`
/// when no reply arrived in time.
pub fn send(
    socket: &Path,
    command: &serde_json::Value,
    wait: Duration,
) -> Option<serde_json::Value> {
    let mut stream = UnixStream::connect(socket).unwrap();
    stream.set_read_timeout(Some(wait)).unwrap();
    writeln!(stream, "{command}").unwrap();
    let mut line = String::new();
    match BufReader::new(stream).read_line(&mut line) {
        Ok(n) if n > 0 => Some(serde_json::from_str(&line).unwrap()),
        _ => None,
    }
}

/// Sends one command line and closes the connection without reading a reply,
/// as a controller that disconnects mid-command does.
pub fn send_and_disconnect(socket: &Path, command: &serde_json::Value) {
    let mut stream = UnixStream::connect(socket).unwrap();
    writeln!(stream, "{command}").unwrap();
    stream.shutdown(std::net::Shutdown::Both).unwrap();
}

/// Waits until the control socket accepts connections.
pub fn connectable(socket: &Path) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while UnixStream::connect(socket).is_err() {
        assert!(Instant::now() < deadline, "control socket never listened");
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Polls status until `holding` says an activation holds the candidate,
/// which it keeps until its durable write returns.
pub fn until_holding(socket: &Path, holding: impl Fn(&serde_json::Value) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let status = send(
            socket,
            &serde_json::json!({"operation": "status"}),
            Duration::from_secs(5),
        )
        .expect("status waited for the activation");
        if holding(&status) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "activation never took the candidate"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Serves `router` on a loopback port; returns its base URL.
pub async fn serve(router: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(std::future::IntoFuture::into_future(axum::serve(
        listener, router,
    )));
    url
}

/// A FIFO holds the temporary write until release. A directory at the final
/// record path makes rename fail even on platforms that allow syncing a FIFO.
/// Keep that blocker until a subsequent serialized command proves persistence
/// has returned, then restore the original record before retrying activation.
pub struct BlockedWrite {
    path: PathBuf,
    restore: RecordRestore,
    release: std::sync::mpsc::Sender<()>,
    reader: std::thread::JoinHandle<()>,
}

pub struct RecordRestore {
    record: PathBuf,
    backup: Option<PathBuf>,
}

impl RecordRestore {
    pub fn restore(self) {
        std::fs::remove_dir(&self.record).unwrap();
        if let Some(backup) = self.backup {
            std::fs::rename(backup, self.record).unwrap();
        }
    }
}

impl BlockedWrite {
    pub fn at(record: PathBuf) -> Self {
        let backup = record.exists().then(|| {
            let backup = record.with_extension("before-block");
            std::fs::rename(&record, &backup).unwrap();
            backup
        });
        std::fs::create_dir(&record).unwrap();
        let path = record.with_extension("tmp");
        assert!(std::process::Command::new("mkfifo")
            .arg(&path)
            .status()
            .unwrap()
            .success());
        let (release, released) = std::sync::mpsc::channel::<()>();
        let reader = std::thread::spawn({
            let path = path.clone();
            move || {
                let _ = released.recv_timeout(Duration::from_secs(10));
                let mut bytes = Vec::new();
                std::fs::File::open(path)
                    .unwrap()
                    .read_to_end(&mut bytes)
                    .unwrap();
            }
        });
        Self {
            path,
            restore: RecordRestore { record, backup },
            release,
            reader,
        }
    }

    pub fn release(self) -> RecordRestore {
        let _ = self.release.send(());
        self.reader.join().unwrap();
        std::fs::remove_file(&self.path).unwrap();
        self.restore
    }
}
