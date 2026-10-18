//! Experimental single-process live serving (`serve-live`) and read-only live
//! observation (`observe-live`) against a canonical node.
use super::{backend, require, synthetic::listeners, AnyError};
use crate::{
    status::{self, Controller, Generation},
    zakura::ZakuraClient,
};
use serde_json::json;
use status_query_server::protocol::*;
use std::{net::SocketAddr, path::PathBuf, time::Duration};

fn matches_manifest(snapshot: &status::index::Snapshot, manifest: &Manifest) -> bool {
    snapshot.network == manifest.network
        && snapshot.salt == manifest.salt
        && snapshot.start == manifest.coverage_start
        && snapshot.height == manifest.anchor_height
        && snapshot.anchor == manifest.anchor_hash
        && snapshot.entries == manifest.entries
        && snapshot.digest == manifest.rows_digest
}

const UNCHANGED_REAFFIRM_MS: u64 = 5_000;

async fn revalidate_candidate(
    rpc: &ZakuraClient,
    verified: &crate::source::Observation,
    candidate: &mut Generation,
) -> Result<bool, AnyError> {
    // A newer mempool observation does not invalidate a complete earlier
    // snapshot. Keep its original timestamp so freshness is never renewed by
    // rows that were not prepared from the newer observation.
    let canonical = rpc
        .block_hash(u64::from(candidate.manifest().anchor_height))
        .await?;
    let mut manifest = candidate.manifest().clone();
    let unchanged =
        accept_candidate_observation(&mut manifest, verified, canonical, status::now_ms())?;
    if unchanged {
        *candidate = candidate.reobserved(manifest.observed_ms);
    }
    Ok(unchanged)
}

fn accept_candidate_observation(
    manifest: &mut Manifest,
    verified: &crate::source::Observation,
    canonical: Hash,
    now_ms: u64,
) -> Result<bool, Error> {
    if canonical != manifest.anchor_hash || verified.snapshot.network != manifest.network {
        return Err(Error::Malformed);
    }
    let unchanged = matches_manifest(&verified.snapshot, manifest);
    if unchanged {
        manifest.observed_ms = verified.observed_ms;
    }
    manifest.fresh(now_ms)?;
    Ok(unchanged)
}

pub struct LiveConfig {
    pub rpc_url: String,
    pub cookie: PathBuf,
    pub salt_hex: String,
    pub window_blocks: u32,
    pub cuda: bool,
    pub listen: SocketAddr,
    pub worker: SocketAddr,
    pub router: SocketAddr,
    pub monitoring: SocketAddr,
    pub state_dir: PathBuf,
}

pub async fn serve_live(config: LiveConfig) -> Result<(), AnyError> {
    let LiveConfig {
        rpc_url,
        cookie,
        salt_hex,
        window_blocks,
        cuda,
        listen,
        worker,
        router,
        monitoring,
        state_dir,
    } = config;
    let salt: Hash = hex::decode(salt_hex)?
        .try_into()
        .map_err(|_| "salt must be exactly 32 bytes")?;
    let rpc = ZakuraClient::from_cookie_file(rpc_url, cookie)?;
    let mut source =
        crate::source::RollingWindow::open(state_dir.join("source"), salt, window_blocks)?;
    let first = source.observe(&rpc).await?;
    let mut authority = crate::authority::Authority::open(
        state_dir.join("authority"),
        first.snapshot.network,
        salt,
    )?;
    let controller = loop {
        let observed = source.observe(&rpc).await?;
        let (number, epoch) = authority.next_identity()?;
        let build = tokio::task::spawn_blocking(move || {
            Generation::prepare(
                &observed.snapshot,
                number,
                epoch,
                observed.observed_ms,
                None,
                backend(cuda),
            )
        })
        .await??;
        let (mut candidate, stats) = build;
        let verified = source.observe(&rpc).await?;
        let unchanged = match revalidate_candidate(&rpc, &verified, &mut candidate).await {
            Ok(value) => value,
            Err(error) => {
                println!(
                    "{}",
                    json!({"phase":"live_candidate_discarded","reason":error.to_string()})
                );
                continue;
            }
        };
        authority.commit(candidate.manifest())?;
        println!(
            "{}",
            json!({"phase":"live_initial_publication","generation":number,"recovery_epoch":epoch,"preparation":stats,"anchor_height":candidate.manifest().anchor_height,"source_unchanged":unchanged,"source_observed_ms":candidate.manifest().observed_ms,"queryable_ms":status::now_ms()})
        );
        break Controller::new(candidate);
    };
    let (_origin, jobs) = listeners(
        &controller,
        state_dir,
        listen,
        worker,
        router,
        monitoring,
        true,
    )
    .await?;
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            _ = interval.tick() => {}
        }
        let observed = match source.observe(&rpc).await {
            Ok(value) => value,
            Err(error) => {
                println!(
                    "{}",
                    json!({"phase":"live_observation_failed","error":error.to_string()})
                );
                continue;
            }
        };
        let previous = controller.current();
        if matches_manifest(&observed.snapshot, previous.manifest())
            && observed
                .observed_ms
                .saturating_sub(previous.manifest().observed_ms)
                < UNCHANGED_REAFFIRM_MS
        {
            continue;
        }
        let (number, epoch) = authority.next_identity()?;
        let candidate = if matches_manifest(&observed.snapshot, previous.manifest()) {
            previous.reidentified(number, epoch, observed.observed_ms)
        } else {
            let snapshot = observed.snapshot;
            let previous = previous.clone();
            let (mut candidate, stats) = match tokio::task::spawn_blocking(move || {
                Generation::prepare(
                    &snapshot,
                    number,
                    epoch,
                    observed.observed_ms,
                    Some(&previous),
                    backend(cuda),
                )
            })
            .await?
            {
                Ok(value) => value,
                Err(error) => {
                    println!(
                        "{}",
                        json!({"phase":"live_preparation_failed","error":error.to_string()})
                    );
                    continue;
                }
            };
            let verified = match source.observe(&rpc).await {
                Ok(value) => value,
                Err(error) => {
                    println!(
                        "{}",
                        json!({"phase":"live_revalidation_failed","error":error.to_string()})
                    );
                    continue;
                }
            };
            let unchanged = match revalidate_candidate(&rpc, &verified, &mut candidate).await {
                Ok(value) => value,
                Err(error) => {
                    println!(
                        "{}",
                        json!({"phase":"live_candidate_discarded","reason":error.to_string()})
                    );
                    continue;
                }
            };
            println!(
                "{}",
                json!({"phase":"live_prepared","stats":stats,"generation":number,"source_unchanged":unchanged})
            );
            candidate
        };
        if candidate.manifest().fresh(status::now_ms()).is_err() {
            continue;
        }
        authority.commit(candidate.manifest())?;
        controller.activate(candidate)?;
        println!(
            "{}",
            json!({"phase":"live_publication","generation":number,"recovery_epoch":epoch,"source_observed_ms":controller.current().manifest().observed_ms,"queryable_ms":status::now_ms()})
        );
    }
    for job in jobs {
        job.abort();
    }
    Ok(())
}

pub async fn observe_live(
    rpc_url: String,
    cookie: PathBuf,
    salt_hex: String,
    window_blocks: u32,
    samples: usize,
    interval_ms: u64,
    state_dir: Option<PathBuf>,
) -> Result<(), AnyError> {
    require(
        samples > 0 && interval_ms > 0,
        "invalid observation schedule",
    )?;
    let salt: Hash = hex::decode(salt_hex)?
        .try_into()
        .map_err(|_| "salt must be exactly 32 bytes")?;
    let rpc = ZakuraClient::from_cookie_file(rpc_url, cookie)?;
    let mut window = match state_dir {
        Some(path) => crate::source::RollingWindow::open(path, salt, window_blocks)?,
        None => crate::source::RollingWindow::new(salt, window_blocks)?,
    };
    let mut previous = None;
    for sample in 0..samples {
        if sample > 0 {
            tokio::time::sleep(Duration::from_millis(interval_ms)).await;
        }
        let observed = window.observe(&rpc).await?;
        let changed_rows = previous
            .as_ref()
            .map(|old| observed.snapshot.changed_rows(old));
        println!(
            "{}",
            json!({"phase":"live_observation","published":false,"sample":sample,
                "observed_ms":observed.observed_ms,"collect_ms":observed.collect_ms,
                "source_blocks":observed.blocks,"source_mempool":observed.mempool,
                "retained_forks":observed.retained_forks,"changed_rows":changed_rows,
                "anchor_height":observed.snapshot.height,
                "coverage_start":observed.snapshot.start,
                "indexed_entries":observed.snapshot.entries,
                "evicted_blocks":observed.snapshot.evicted_blocks,
                "rows_digest":hex::encode(observed.snapshot.digest)})
        );
        previous = Some(observed.snapshot);
    }
    Ok(())
}

#[cfg(test)]
mod live_candidate_tests {
    use super::*;

    fn observation(now_ms: u64) -> (Manifest, crate::source::Observation) {
        let snapshot = status::index::Snapshot {
            network: [1; 32],
            salt: [2; 32],
            start: 1,
            height: 1,
            anchor: [3; 32],
            entries: 0,
            rows: Vec::new(),
            digest: [4; 32],
            evicted_blocks: 0,
        };
        let manifest = Manifest {
            protocol: PROTOCOL.into(),
            network: snapshot.network,
            salt: snapshot.salt,
            generation: 1,
            recovery_epoch: 1,
            coverage_start: snapshot.start,
            anchor_height: snapshot.height,
            anchor_hash: snapshot.anchor,
            observed_ms: now_ms - 6_000,
            entries: snapshot.entries,
            rows_digest: snapshot.digest,
            public_digest: [5; 32],
        };
        (
            manifest,
            crate::source::Observation {
                snapshot,
                observed_ms: now_ms,
                collect_ms: 0.,
                blocks: 1,
                mempool: 0,
                retained_forks: 0,
            },
        )
    }

    #[test]
    fn changed_source_keeps_original_age_and_reorg_rejects_candidate() {
        let now = status::now_ms();
        let (mut manifest, mut verified) = observation(now);
        verified.snapshot.digest = [6; 32];
        assert_eq!(
            accept_candidate_observation(&mut manifest, &verified, [3; 32], now),
            Ok(false)
        );
        assert_eq!(manifest.observed_ms, now - 6_000);
        assert_eq!(
            accept_candidate_observation(&mut manifest, &verified, [7; 32], now),
            Err(Error::Malformed)
        );
        assert_eq!(manifest.observed_ms, now - 6_000);
    }

    #[test]
    fn identical_source_can_refresh_timestamp_but_stale_candidate_cannot_publish() {
        let now = status::now_ms();
        let (mut manifest, verified) = observation(now);
        assert_eq!(
            accept_candidate_observation(&mut manifest, &verified, [3; 32], now),
            Ok(true)
        );
        assert_eq!(manifest.observed_ms, now);
        let (mut stale, mut changed) = observation(now);
        changed.snapshot.digest = [6; 32];
        stale.observed_ms = now - MAX_AGE_MS - 1;
        assert_eq!(
            accept_candidate_observation(&mut stale, &changed, [3; 32], now),
            Err(Error::Stale)
        );
    }
}
