//! Graceful stop for replicated serving roles.
//!
//! On SIGTERM or Ctrl-C the listener is closed, so a new connection is refused
//! and the packing router retries that evaluation once on another replica
//! (a refused connection precedes acceptance). Requests already accepted run
//! to completion and deliver their responses before the process exits, so a
//! rolling restart of one worker loses no admitted query.

use axum::Router;
use std::future::Future;
use tokio::net::TcpListener;

/// Serves `app` until `shutdown` resolves, then drains accepted requests.
pub async fn serve_until(
    listener: TcpListener,
    app: Router,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await
}

/// Resolves on SIGTERM (systemd stop) or Ctrl-C.
pub async fn signal() {
    let interrupt = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut stream) => {
                stream.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = interrupt => {}
        _ = terminate => {}
    }
    tracing::info!("stop requested; draining accepted requests");
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
