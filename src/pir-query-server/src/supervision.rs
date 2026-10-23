//! Cooperative product lifecycle shared by the serving roles: serving beside
//! continuous synchronization until a stop, and stop-aware startup retries.
//! Nothing here drops a request or publication pass mid-operation.
type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Serve `app` on `listener` while `synchronization` runs, until `stop` or a
/// terminal synchronization error. A stop drains the requests the listener
/// already accepted, and lets a publication pass already started finish with
/// its durable writes; neither is dropped mid-operation. A terminal error
/// stops accepting requests, drains the accepted ones and is returned with its
/// original type.
pub async fn supervise(
    product: &'static str,
    listener: tokio::net::TcpListener,
    app: axum::Router,
    synchronization: impl std::future::Future<Output = Option<BoxError>> + Send + 'static,
    stop: pir_pipeline::service::Shutdown,
) -> Result<(), BoxError> {
    use std::future::IntoFuture;
    let draining = stop.clone();
    let serving = axum::serve(listener, app)
        .with_graceful_shutdown(async move { draining.cancelled().await })
        .into_future();
    tokio::pin!(serving);
    let mut synchronization = tokio::spawn(synchronization);
    // Only a caller that drops this future abandons the pass; the host never does.
    let mut owned = pir_pipeline::service::Tasks::default();
    owned.own(&synchronization);
    tokio::select! {
        result = &mut serving => {
            // Serving ended on its own or drained after a stop; either way
            // synchronization stops at its next pass boundary.
            stop.request();
            let stopped = (&mut synchronization).await;
            result?;
            match stopped {
                Ok(Some(error)) => Err(error),
                Ok(None) => Ok(()),
                Err(join) => Err(format!("{product} synchronization stopped: {join}").into()),
            }
        }
        result = &mut synchronization => {
            // Wallets stop being accepted; requests already accepted drain.
            stop.request();
            let drained = serving.await;
            match result {
                Ok(Some(error)) => Err(error),
                Ok(None) => Ok(drained?),
                Err(join) => Err(format!("{product} synchronization stopped: {join}").into()),
            }
        }
    }
}

#[cfg(any(feature = "transparent-history", feature = "transparent-detail"))]
/// Wait `delay` before another startup attempt; `false` once `stop` is requested.
pub async fn retry_after(
    delay: std::time::Duration,
    stop: &pir_pipeline::service::Shutdown,
) -> bool {
    tokio::select! {
        biased;
        _ = stop.cancelled() => false,
        _ = tokio::time::sleep(delay) => true,
    }
}
