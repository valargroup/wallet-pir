//! Prepare source-verified runtimes using retained immutable evidence.
use super::*;

/// Prepares the runtimes of the candidate in `directory` beside its
/// revisions; returns its report for the cycle record.
///
/// The candidate is read as a worker reads it, verifying every table the
/// previous prepared candidate did not already hold as the same file. A
/// table that fails verification fails the publication. Each runtime is
/// linked from the active publication or a retained candidate that holds a
/// completed file of its identity, and built otherwise; a runtime that
/// cannot be built or written is reported and fails preparation. An
/// invalidation stops the preparation between segments, and the candidate
/// is then never offered. A candidate under the publication root stores its
/// runtimes; what that and the table verification cost is added to `storage`.
pub(super) async fn prepare_runtimes(
    config: &Config,
    authority: &Authority,
    directory: &Path,
    epoch: u64,
    storage: &mut serde_json::Value,
) -> Result<serde_json::Value, BoxError> {
    use transparent_shared::history::shardset::{LoadOptions, ShardSet};
    let mut retained = vec![authority
        .0
        .active
        .read()
        .unwrap()
        .publication
        .directory
        .clone()];
    let mut candidates: Vec<_> = std::fs::read_dir(&config.publication_root)?
        .filter_map(Result::ok)
        .filter(|e| {
            e.file_type().is_ok_and(|t| t.is_dir())
                && e.file_name()
                    .to_str()
                    .is_some_and(|s| s.starts_with("candidate-"))
                && e.path() != directory
        })
        .collect();
    candidates.sort_by_key(|e| std::cmp::Reverse(e.metadata().and_then(|m| m.modified()).ok()));
    retained.extend(candidates.into_iter().map(|e| e.path()));
    let previous = authority.0.prepared_set.lock().unwrap().take();
    let stop = {
        let authority = authority.clone();
        move || {
            authority.0.shutdown.requested() || authority.0.epoch.load(Ordering::Acquire) != epoch
        }
    };
    let directory = directory.to_path_buf();
    // Only the publisher's own candidates use its store; an operator-supplied
    // initial publication outside the publication root is never rewritten.
    let stored = directory.parent() == Some(config.publication_root.as_path());
    let (previous, prepared) = tokio::task::spawn_blocking(move || {
        // Every revision the publisher wrote; it retains them all, and so
        // may a worker.
        let options = LoadOptions {
            prune_excess: true,
            ..LoadOptions::whole(transparent_shared::history::shardset::DEFAULT_RETAIN_REVISIONS)
        };
        match ShardSet::open_reusing_until(&directory, &options, previous.as_ref(), &stop) {
            Ok(set) => {
                let prepared = transparent_shared::runtime::prebuild::prepare_publication(
                    &set, &directory, &retained, &stop,
                )
                .map_err(|e| e.to_string())
                .and_then(|prepared| {
                    let adopted = if stored {
                        let adopted = crate::history::store::adopt_runtimes(&directory, &stop)
                            .map_err(|e| format!("storing prepared runtimes: {e}"))?;
                        tracing::info!(?adopted, "stored candidate runtimes");
                        Some(adopted)
                    } else {
                        None
                    };
                    Ok((prepared, adopted, set.verification().clone()))
                });
                (Some(set), prepared)
            }
            // A stopped scan proved nothing; the earlier set keeps its evidence.
            Err(transparent_shared::history::shardset::LoadError::Stopped) => (
                previous,
                Err("candidate invalidated while preparing".to_string()),
            ),
            Err(error) => (
                previous,
                Err(format!("the candidate did not verify: {error}")),
            ),
        }
    })
    .await?;
    *authority.0.prepared_set.lock().unwrap() = previous;
    let (prepared, adopted, verification) = prepared?;
    storage["runtimes"] = serde_json::to_value(adopted)?;
    storage["verification"] = serde_json::to_value(verification)?;
    authority
        .0
        .runtime_preparations
        .fetch_add(1, Ordering::Relaxed);
    authority
        .0
        .runtime_preparation_failures
        .fetch_add(prepared.failures.len() as u64, Ordering::Relaxed);
    let report = prepared.report();
    authority.0.status.write().unwrap()["runtime_preparation"] = report.clone();
    if !prepared.failures.is_empty() {
        return Err(format!("history runtime preparation failed: {}", report).into());
    }
    Ok(report)
}
