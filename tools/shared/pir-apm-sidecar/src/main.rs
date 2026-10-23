use anyhow::{Context, Result};
use hyper::body::Bytes;
use pir_apm_sidecar::{config, resources, server};
use std::{path::PathBuf, time::Duration};

/// `pir-apm-sidecar --config FILE [--check]`; `--check` validates and prints
/// the host identity, configuration digest and expected check names.
fn parse_args() -> Result<(PathBuf, bool)> {
    let mut config = None;
    let mut check = false;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => config = args.next().map(PathBuf::from),
            "--check" => check = true,
            _ => anyhow::bail!("usage: pir-apm-sidecar --config FILE [--check]"),
        }
    }
    Ok((config.context("--config is required")?, check))
}

#[tokio::main]
async fn main() -> Result<()> {
    let (path, check) = parse_args()?;
    let loaded = config::Config::load(&path)?;
    let config = loaded.config;
    let tls = &config.tls;
    let identity = server::load_identity(
        &config::credential(&tls.cert),
        &config::credential(&tls.key),
    )?;
    let not_after = server::not_after(&identity.chain[0]);
    let tls_config = server::tls_config(identity, &config::credential(&tls.client_ca))?;
    if check {
        println!(
            "{}",
            serde_json::json!({"host_id": config.host_id, "config_digest": loaded.digest,
                "checks": config.expected_checks()})
        );
        return Ok(());
    }
    let listener = tokio::net::TcpListener::bind(config.listen)
        .await
        .with_context(|| format!("binding {}", config.listen))?;
    let (tx, rx) = tokio::sync::watch::channel(server::Cache::default());
    let state_path = PathBuf::from(&config.state_path);
    let incarnation = pir_apm_sidecar::incarnation();
    let collector = config.clone();
    let digest = loaded.digest.clone();
    tokio::spawn(async move {
        let mut state = resources::State::load(&state_path);
        let mut interval =
            tokio::time::interval(Duration::from_secs(pir_apm_sidecar::INTERVAL_SECONDS));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            let before = state.clone();
            let (checks, edge) = pir_apm_sidecar::collect(&collector, &mut state, not_after).await;
            let doc = pir_apm_sidecar::document(&collector, &digest, &incarnation, checks);
            if doc.validate().is_err() {
                eprintln!("collection produced an invalid document; serving none");
                tx.send_replace(server::Cache::default());
                continue;
            }
            if state != before && state.store(&state_path).is_err() {
                eprintln!("counter state could not be stored");
            }
            tx.send_replace(server::Cache {
                observations: Bytes::from(serde_json::to_vec(&doc).expect("serializable")),
                edge_metrics: edge.map(Bytes::from),
            });
        }
    });
    eprintln!(
        "pir-apm-sidecar {} serving {} on {}",
        config.host_id,
        pir_host_observations::PATH,
        config.listen
    );
    server::serve(listener, tls_config, config.tls.collector_identity.clone(), rx).await
}
