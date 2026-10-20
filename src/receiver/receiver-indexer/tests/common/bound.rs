//! The address a spawned `receiver-directory` actually listens on. Children bind port 0
//! and log where they bound, so no port is reserved, released and raced for.
use std::{net::SocketAddr, path::Path, time::Duration};

/// The `address` field of the first line after byte `from` of the child's log `log` that
/// contains `message`, waiting up to a minute for the child to write it.
pub async fn address(log: &Path, from: usize, message: &str) -> SocketAddr {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let text = std::fs::read(log).unwrap_or_default();
        let text = String::from_utf8_lossy(text.get(from..).unwrap_or_default());
        let found = plain(&text)
            .lines()
            .filter(|line| line.contains(message))
            .find_map(|line| {
                line.split_once("address=")?
                    .1
                    .split_whitespace()
                    .next()?
                    .parse()
                    .ok()
            });
        if let Some(address) = found {
            return address;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "no `{message}` address in {}:\n{text}",
            log.display()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// `text` without ANSI escape sequences, which colored log fields carry.
fn plain(text: &str) -> String {
    let mut plain = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            chars.by_ref().find(|c| c.is_ascii_alphabetic());
        } else {
            plain.push(c);
        }
    }
    plain
}
