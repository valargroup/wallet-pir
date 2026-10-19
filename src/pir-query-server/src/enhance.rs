//! Runs one Enhance serving role on the host's reserved listeners.
//!
//! On stop the listeners close, so a new connection is refused and the packing
//! router retries that evaluation once on another replica (a refused connection
//! precedes acceptance). Requests already accepted run to completion and
//! deliver their responses before the role returns, so a rolling restart of one
//! worker loses no admitted query.
use axum::Router;
use enhance_query_server::{
    packing_router::PackingRouter, placement::PlacementPolicy, query_ingress::QueryIngress,
    service::ServingRole, worker::Worker,
};
use std::{future::Future, net::SocketAddr};
use tokio::net::TcpListener;

type Error = Box<dyn std::error::Error + Send + Sync>;

pub async fn run_serving(
    role: ServingRole,
    listeners: Vec<TcpListener>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), Error> {
    // Bound in listen order.
    let mut listeners = listeners.into_iter();
    let mut take = || {
        listeners
            .next()
            .ok_or_else(|| "reserved listener is missing".to_string())
    };
    match role {
        ServingRole::Worker {
            listen: _,
            data_dir,
            sealed_shards,
            matvec,
        } => {
            let listener = take()?;
            let worker =
                Worker::open_with_backend(&data_dir, PlacementPolicy { sealed_shards }, matvec)?;
            serve_until(listener, worker.router(), shutdown).await?;
        }
        ServingRole::QueryIngress {
            data_dir,
            listen: _,
            control_listen: _,
            requests,
            uploads,
        } => {
            let public = take()?;
            let control = take()?;
            let ingress = QueryIngress::open_with_uploads(&data_dir, requests, uploads)?;
            let public_app = ingress
                .public_router()
                .into_make_service_with_connect_info::<SocketAddr>();
            let control_app = ingress.control_router();
            let (notify, public_stop, control_stop) = stop_pair(shutdown);
            let _notify = notify;
            tokio::try_join!(
                async move {
                    axum::serve(public, public_app)
                        .with_graceful_shutdown(public_stop)
                        .await
                },
                serve_until(control, control_app, control_stop),
            )?;
        }
        ServingRole::PackingRouter {
            data_dir,
            artifact_origin,
            listen: _,
            control_listen: _,
            max_objects,
            requests,
        } => {
            let public = take()?;
            let control = take()?;
            let router = PackingRouter::open(&data_dir, &artifact_origin, max_objects, requests)?;
            router.start_worker_health_monitor();
            let (notify, public_stop, control_stop) = stop_pair(shutdown);
            let _notify = notify;
            tokio::try_join!(
                serve_until(public, router.public_router(), public_stop),
                serve_until(control, router.control_router(), control_stop),
            )?;
        }
    }
    Ok(())
}

/// Serves `app` until `shutdown` resolves, then drains accepted requests.
async fn serve_until(
    listener: TcpListener,
    app: Router,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await
}

fn stop_pair(
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> (
    tokio::task::JoinHandle<()>,
    impl Future<Output = ()> + Send,
    impl Future<Output = ()> + Send,
) {
    let (sender, receiver) = tokio::sync::watch::channel(false);
    let notify = tokio::spawn(async move {
        shutdown.await;
        let _ = sender.send(true);
    });
    let mut first = receiver.clone();
    let mut second = receiver;
    (
        notify,
        async move {
            let _ = first.changed().await;
        },
        async move {
            let _ = second.changed().await;
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::get;
    use std::time::Duration;

    #[tokio::test]
    async fn stopping_refuses_new_connections_and_finishes_accepted_requests() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new().route(
            "/slow",
            get(|| async {
                tokio::time::sleep(Duration::from_millis(300)).await;
                "done"
            }),
        );
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(serve_until(listener, app, async {
            let _ = stopped.await;
        }));
        let client = reqwest::Client::new();
        let accepted = tokio::spawn({
            let client = client.clone();
            async move {
                client
                    .get(format!("http://{address}/slow"))
                    .send()
                    .await?
                    .text()
                    .await
            }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        stop.send(()).unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        // A fresh connection is refused, which the router may retry elsewhere.
        let refused = reqwest::Client::new()
            .get(format!("http://{address}/slow"))
            .send()
            .await
            .unwrap_err();
        assert!(refused.is_connect(), "{refused}");
        assert_eq!(accepted.await.unwrap().unwrap(), "done");
        server.await.unwrap().unwrap();
    }
}
