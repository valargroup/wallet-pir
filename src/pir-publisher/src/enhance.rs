//! Enhance coordinator lifecycle: the data-directory lock, listeners, router
//! heartbeat and poll scheduling. `enhance-publisher` opens and owns durable
//! state and publication decisions.
use enhance_publisher::{application::Ingestion, service::Config};
use pir_pipeline::service::{Http, Shutdown, Tasks};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

pub async fn run(
    config: Config,
    http: Http<axum::Router>,
    shutdown: Shutdown,
) -> crate::Result<()> {
    config.validate()?;
    let _lock = pir_pipeline::service::lock(&config.data_dir)?;
    let mut tasks = Tasks::default();
    let failure = Arc::new(AtomicBool::new(false));
    let opened = enhance_publisher::service::open(config)?;
    if let Some(listen) = opened.artifact_listen {
        let artifacts = opened.coordinator.artifact_router();
        let artifact = tokio::spawn(http.serve(listen, artifacts));
        tasks.own(&artifact);
        let failed = failure.clone();
        let stop = shutdown.clone();
        let watch = tokio::spawn(async move {
            let _ = artifact.await;
            failed.store(true, Ordering::SeqCst);
            stop.request();
        });
        tasks.own(&watch);
        let heartbeat = opened.coordinator.clone();
        let heartbeat_task = tokio::spawn(async move {
            loop {
                heartbeat.refresh_routers().await;
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
        tasks.own(&heartbeat_task);
    }
    let routes = opened.coordinator.clone().router();
    let task = tokio::spawn(http.serve(opened.listen, routes));
    tasks.own(&task);
    let (coordinator, config, journal, source) = opened.into_source()?;
    let serving = Arc::new(task);
    let stopped = serving.clone();
    let mut ingestion = Ingestion::new(
        config,
        coordinator,
        journal,
        source,
        Box::new(move || stopped.is_finished()),
    );
    let poll = ingestion.poll();
    pir_pipeline::supervise(&mut ingestion, poll, shutdown.cancelled()).await?;
    if failure.load(Ordering::SeqCst) {
        return Err("Enhance artifact listener stopped".into());
    }
    Ok(())
}
