//! Stable host-facing facade for independently configured Transparent publishers.
//! Controllers and response schemas remain private to the corresponding adapter.
pub use crate::history::config::Config as HistoryConfig;
pub use crate::history::filter_service::Config as FilterConfig;
pub use crate::tx_detail::service::Config as DetailConfig;
use pir_pipeline::ingestion::{ChainSettings, IngestionExecutor};
use pir_pipeline::publisher::{
    PublisherDefinition, PublisherResources, ReadinessProbe, ReadinessRule, Result,
};

fn serving(bytes: &[u8]) -> bool {
    serde_json::from_slice::<serde_json::Value>(bytes)
        .is_ok_and(|value| value["phase"] == "serving")
}
fn details_published(bytes: &[u8]) -> bool {
    serde_json::from_slice::<serde_json::Value>(bytes).is_ok_and(|value| {
        value["phase"] == "running"
            && value["halted"].is_null()
            && value["stalled"] == false
            && !value["published"].is_null()
    })
}
/// Static declaration only; the startup factory retains publication authority.
pub fn history<E>(
    config: HistoryConfig,
    chain: Option<&ChainSettings>,
    executor: E,
) -> Result<PublisherDefinition<axum::Router>>
where
    E: pir_pipeline::publisher::PublisherExecutor + IngestionExecutor,
{
    let chain = chain
        .cloned()
        .ok_or("enabled transparent_history publisher needs the root chain settings")?;
    let mut writable_paths = vec![config.publication_root.clone()];
    if !config.follow_journal {
        writable_paths.push(config.data_dir.clone());
    }
    if let Some(path) = &config.snapshot_dir {
        writable_paths.push(path.clone());
    }
    let probes = vec![ReadinessProbe {
        address: config.listen,
        path: "/v1/status",
        rule: ReadinessRule::Body(serving),
    }];
    let resources = PublisherResources {
        listeners: vec![config.listen],
        writable_paths,
        probes,
    };
    Ok(PublisherDefinition::new(
        "transparent_history",
        resources,
        move |context| async move {
            let feed = context
                .chain
                .ok_or("History publisher started without the root chain")?;
            let rpc = crate::ingest::zakura::connect(&chain.rpc_url, &chain.rpc_cookie)?;
            crate::history::controller::run_managed(
                config,
                rpc,
                feed,
                executor,
                context.http,
                context.shutdown,
            )
            .await
        },
    )
    .following_chain())
}
pub fn filters(config: FilterConfig) -> Result<PublisherDefinition<axum::Router>> {
    config.validate()?;
    let resources = PublisherResources {
        listeners: vec![config.listen],
        writable_paths: vec![config.data_dir.clone()],
        probes: vec![ReadinessProbe {
            address: config.listen,
            path: "/v1/health",
            rule: ReadinessRule::Body(serving),
        }],
    };
    Ok(PublisherDefinition::new(
        "transparent_filters",
        resources,
        move |context| crate::history::filter_service::run(config, context.http, context.shutdown),
    ))
}
pub fn details<E: pir_pipeline::publisher::PublisherExecutor>(
    config: DetailConfig,
    executor: E,
) -> Result<PublisherDefinition<axum::Router>> {
    config.validate()?;
    let resources = PublisherResources {
        listeners: vec![config.status_listen],
        writable_paths: vec![config.root.clone(), config.journal.clone()],
        probes: vec![ReadinessProbe {
            address: config.status_listen,
            path: "/v1/status",
            rule: ReadinessRule::Body(details_published),
        }],
    };
    Ok(PublisherDefinition::new(
        "transaction_details",
        resources,
        move |context| {
            crate::tx_detail::service::run(config, executor, context.http, context.shutdown)
        },
    ))
}
