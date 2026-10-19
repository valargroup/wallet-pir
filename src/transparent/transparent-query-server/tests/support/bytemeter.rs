//! A TCP relay that counts the bytes it forwards in each direction.
//!
//! Put between a client and a server, it measures what actually crosses the
//! wire: TLS records and HTTP/2 frames included when the client speaks them.
//! A client keeps its URL, and so its TLS server name, and only resolves the
//! host to the relay (`reqwest::ClientBuilder::resolve`). Bytes are counted
//! before they are forwarded, so once a client has read a whole response the
//! counters already include it.
#![allow(dead_code)]

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub struct ByteMeter {
    /// Where clients connect.
    pub addr: SocketAddr,
    up: Arc<AtomicU64>,
    down: Arc<AtomicU64>,
    connections: Arc<AtomicU64>,
    task: tokio::task::JoinHandle<()>,
}

impl ByteMeter {
    /// Relays every connection accepted on a loopback port to `upstream`.
    pub async fn start(upstream: SocketAddr) -> std::io::Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let up = Arc::new(AtomicU64::new(0));
        let down = Arc::new(AtomicU64::new(0));
        let connections = Arc::new(AtomicU64::new(0));
        let (up_total, down_total, accepted) = (up.clone(), down.clone(), connections.clone());
        let task = tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                accepted.fetch_add(1, Ordering::Relaxed);
                let (up, down) = (up_total.clone(), down_total.clone());
                tokio::spawn(async move {
                    let Ok(server) = tokio::net::TcpStream::connect(upstream).await else {
                        return;
                    };
                    let _ = client.set_nodelay(true);
                    let _ = server.set_nodelay(true);
                    let (client_read, client_write) = client.into_split();
                    let (server_read, server_write) = server.into_split();
                    tokio::join!(
                        relay(client_read, server_write, up),
                        relay(server_read, client_write, down)
                    );
                });
            }
        });
        Ok(Self {
            addr,
            up,
            down,
            connections,
            task,
        })
    }

    /// Bytes sent by clients so far.
    pub fn up(&self) -> u64 {
        self.up.load(Ordering::Relaxed)
    }

    /// Bytes sent to clients so far.
    pub fn down(&self) -> u64 {
        self.down.load(Ordering::Relaxed)
    }

    /// Connections accepted so far: each one is a TCP and, over HTTPS, a TLS
    /// handshake that a fresh client pays.
    pub fn connections(&self) -> u64 {
        self.connections.load(Ordering::Relaxed)
    }

    pub fn snapshot(&self) -> (u64, u64) {
        (self.up(), self.down())
    }
}

impl Drop for ByteMeter {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn relay(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    counter: Arc<AtomicU64>,
) {
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        let read = match from.read(&mut buffer).await {
            Ok(0) | Err(_) => break,
            Ok(read) => read,
        };
        counter.fetch_add(read as u64, Ordering::Relaxed);
        if to.write_all(&buffer[..read]).await.is_err() {
            break;
        }
    }
    let _ = to.shutdown().await;
}
