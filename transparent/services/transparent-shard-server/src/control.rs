//! The root-only control socket both workers take publication commands on,
//! the client that sends one, and the durable records the commands write.
//!
//! A command is one JSON line and its reply is one JSON line. The command and
//! the reply's fields are each worker's own; the framing, the size bound and
//! the socket's permissions are shared.

use serde::de::DeserializeOwned;
use serde::Serialize;
use std::future::Future;
use std::io::{BufRead, Read, Write};
use std::path::Path;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

/// The longest command or reply line accepted, in bytes.
const LINE_BYTES: u64 = 1024 * 1024;

/// Writes `value` as JSON to `path` so that a crash leaves either the old
/// record or the new one: written beside it, synced, renamed over it, and the
/// directory synced so the rename itself survives.
pub(crate) fn persist(path: &Path, value: &impl Serialize) -> Result<(), String> {
    let parent = path.parent().ok_or("record needs a parent directory")?;
    std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    let temp = path.with_extension("tmp");
    let mut file = std::fs::File::create(&temp).map_err(|e| e.to_string())?;
    file.write_all(&serde_json::to_vec(value).map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())?;
    file.sync_all().map_err(|e| e.to_string())?;
    std::fs::rename(temp, path).map_err(|e| e.to_string())?;
    std::fs::File::open(parent)
        .and_then(|f| f.sync_all())
        .map_err(|e| e.to_string())
}

/// Listens on `path`, owner-only, answering each connection's one command
/// line with the reply line `handle` builds. A line that is oversized,
/// unterminated or not a command is answered `{"ok":false,"error":…}`.
///
/// Refuses to take over a socket another process is still listening on.
pub(crate) async fn listen<C, F, R>(
    path: &Path,
    handle: F,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    C: DeserializeOwned + Send + 'static,
    F: Fn(C) -> R + Clone + Send + Sync + 'static,
    R: Future<Output = serde_json::Value> + Send + 'static,
{
    use std::os::unix::fs::PermissionsExt;
    if path.exists() {
        if tokio::net::UnixStream::connect(path).await.is_ok() {
            return Err("control socket already in use".into());
        }
        std::fs::remove_file(path)?;
    }
    let listener = tokio::net::UnixListener::bind(path)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    loop {
        let (stream, _) = listener.accept().await?;
        let handle = handle.clone();
        tokio::spawn(async move {
            let (read, mut write) = stream.into_split();
            let mut reader = BufReader::new(read);
            let mut bytes = Vec::new();
            use tokio::io::AsyncReadExt;
            let line = match (&mut reader)
                .take(LINE_BYTES)
                .read_until(b'\n', &mut bytes)
                .await
            {
                Ok(_) if bytes.last() == Some(&b'\n') => match serde_json::from_slice(&bytes) {
                    Ok(command) => handle(command).await,
                    Err(error) => serde_json::json!({"ok": false, "error": error.to_string()}),
                },
                _ => {
                    serde_json::json!({"ok": false, "error": "invalid or oversized control command"})
                }
            };
            let _ = write.write_all(format!("{line}\n").as_bytes()).await;
        });
    }
}

/// The control client: reads one command of type `C` as JSON on stdin, sends
/// it to the socket named by the first argument, prints the reply line, and
/// exits 1 unless the reply says `"ok": true`. `name` is the binary's, for
/// its usage line.
pub fn run_client<C: Serialize + DeserializeOwned>(
    name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let socket = std::env::args()
        .nth(1)
        .ok_or_else(|| format!("usage: {name} SOCKET < command.json"))?;
    let mut input = String::new();
    std::io::stdin()
        .take(LINE_BYTES)
        .read_to_string(&mut input)?;
    let command: C = serde_json::from_str(&input)?;
    let mut stream = std::os::unix::net::UnixStream::connect(socket)?;
    stream.set_read_timeout(Some(std::time::Duration::from_secs(600)))?;
    writeln!(stream, "{}", serde_json::to_string(&command)?)?;
    let mut response = String::new();
    std::io::BufReader::new(stream)
        .take(LINE_BYTES)
        .read_line(&mut response)?;
    let body: serde_json::Value = serde_json::from_str(&response)?;
    print!("{response}");
    if body["ok"] != true {
        std::process::exit(1);
    }
    Ok(())
}
