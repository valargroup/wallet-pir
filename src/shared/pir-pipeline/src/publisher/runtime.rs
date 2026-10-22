use std::future::Future;

/// Preserve thread-affine state by constructing the workflow on its dedicated
/// runtime. Thread failure is returned to the service owner, never silently retried.
pub async fn dedicated_runtime<F, Fut>(name: &'static str, start: F) -> super::Result<()>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = super::Result<()>>,
{
    let thread = std::thread::Builder::new()
        .name(name.into())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .max_blocking_threads(2)
                .enable_all()
                .build()?;
            runtime.block_on(start())
        })?;
    tokio::task::spawn_blocking(move || {
        thread
            .join()
            .map_err(|_| -> super::Error { format!("{name} thread panicked").into() })?
    })
    .await?
}
