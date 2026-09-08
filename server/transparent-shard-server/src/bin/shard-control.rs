//! Send one JSON command on stdin to the worker's root-only control socket.
use std::io::{BufRead, Read, Write};
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let socket = std::env::args()
        .nth(1)
        .ok_or("usage: shard-control SOCKET < command.json")?;
    let mut input = String::new();
    std::io::stdin()
        .take(1024 * 1024)
        .read_to_string(&mut input)?;
    let command: transparent_shard_server::live::Command = serde_json::from_str(&input)?;
    let mut stream = std::os::unix::net::UnixStream::connect(socket)?;
    stream.set_read_timeout(Some(std::time::Duration::from_secs(600)))?;
    writeln!(stream, "{}", serde_json::to_string(&command)?)?;
    let mut response = String::new();
    std::io::BufReader::new(stream)
        .take(1024 * 1024)
        .read_line(&mut response)?;
    let body: serde_json::Value = serde_json::from_str(&response)?;
    print!("{response}");
    if body["ok"] != true {
        std::process::exit(1);
    }
    Ok(())
}
