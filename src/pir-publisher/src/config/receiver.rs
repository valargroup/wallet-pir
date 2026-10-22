//! Receiver retains its existing implementation; this bridge belongs only to
//! application composition and introduces no changes to its workflow.
use pir_pipeline::publisher::{
    PublisherDefinition, PublisherResources, ReadinessProbe, ReadinessRule, Result,
};
pub(super) fn configure(
    config: receiver_indexer::service::Config,
) -> Result<PublisherDefinition<axum::Router>> {
    config.validate()?;
    if !config.serve {
        return Err("unified Receiver publishing requires continuous serving".into());
    }
    let resources = PublisherResources {
        listeners: vec![config.bind],
        writable_paths: vec![config.data_dir.clone()],
        probes: vec![ReadinessProbe {
            address: config.bind,
            path: "/v1/receiver/init",
            rule: ReadinessRule::HttpSuccess,
        }],
    };
    Ok(PublisherDefinition::new(
        "receiver",
        resources,
        move |context| {
            receiver_indexer::service::run_isolated(config, context.http, context.shutdown)
        },
    ))
}
