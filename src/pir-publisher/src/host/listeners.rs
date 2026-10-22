//! The host reserves sockets; products supply routers and cooperative shutdown.
use axum::Router;
use pir_pipeline::service::{Http, Shutdown};
use std::{
    collections::BTreeMap,
    io,
    net::SocketAddr,
    sync::{Arc, Mutex},
};
use tokio::net::TcpListener;

#[derive(Clone)]
pub(super) struct ReservedListeners(Arc<Mutex<BTreeMap<SocketAddr, TcpListener>>>);

impl ReservedListeners {
    /// Bind every declaration before any service starts. An error drops all
    /// reservations already made and identifies the failed owner and address.
    pub(super) async fn bind_all(
        declarations: &BTreeMap<SocketAddr, &'static str>,
    ) -> io::Result<Self> {
        let mut listeners = BTreeMap::new();
        for (address, owner) in declarations {
            let listener = TcpListener::bind(address).await.map_err(|source| {
                io::Error::new(
                    source.kind(),
                    ListenerBindError {
                        owner,
                        address: *address,
                        source,
                    },
                )
            })?;
            listeners.insert(*address, listener);
        }
        Ok(Self(Arc::new(Mutex::new(listeners))))
    }

    /// Each socket can be taken only once. The product stop request drains its
    /// HTTP service without changing the product's router or admission policy.
    pub(super) fn http_adapter(&self, shutdown: Shutdown) -> Http<Router> {
        let listeners = self.clone();
        Http::new(move |address, router| {
            let listener = listeners.0.lock().unwrap().remove(&address);
            let shutdown = shutdown.clone();
            async move {
                let listener = listener.ok_or_else(|| {
                    io::Error::other(format!(
                        "listener {address} was not reserved or is already in use"
                    ))
                })?;
                axum::serve(listener, router)
                    .with_graceful_shutdown(async move { shutdown.cancelled().await })
                    .await
            }
        })
    }
}

#[derive(Debug)]
struct ListenerBindError {
    owner: &'static str,
    address: SocketAddr,
    source: io::Error,
}
impl std::fmt::Display for ListenerBindError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "cannot bind {} listener {}: {}",
            self.owner, self.address, self.source
        )
    }
}
impl std::error::Error for ListenerBindError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

#[cfg(test)]
mod tests;
