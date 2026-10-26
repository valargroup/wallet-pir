//! Restore completed publications and apply background validation outcomes.
use super::*;

pub(super) fn integrity_failures(root: &Path) -> Result<Vec<IntegrityFailure>, BoxError> {
    match std::fs::read(root.join(INTEGRITY_FAILURES)) {
        Ok(bytes) => Ok(serde_json::from_slice(&bytes)?),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(e.into()),
    }
}

/// Whether `inventory` lists a failed artifact as it was, or bytes found corrupt.
pub(super) fn lists_failed(
    failures: &[IntegrityFailure],
    inventory: &transparent_shared::publication::Inventory,
) -> bool {
    inventory.artifacts.iter().any(|a| {
        failures.iter().any(|f| {
            (f.expected_sha256 == a.sha256) || f.found_sha256.as_deref() == Some(a.sha256.as_str())
        })
    })
}

pub(super) enum Restore {
    /// No completion evidence: the ordinary path prepares it in full.
    Absent,
    /// The saved inventory lists bytes recorded as failing integrity.
    Failed(IntegrityFailure),
    Restored(Box<transparent_shared::publication::Restored>),
}

/// The saved publication's completed distribution, without reading its bodies.
pub(super) fn restore_public(config: &Config, public: &Public) -> Result<Restore, BoxError> {
    use transparent_shared::publication::{Finalized, RestoreExpectation, RESTORE_FILE};
    // Recorded withdrawal or unfinished activation intent is resolved by the
    // ordinary path, which starts withdrawn; it is never bypassed.
    for intent in ["withdrawn.json", "activation.json"] {
        if config.publication_root.join(intent).exists() {
            tracing::warn!(
                intent,
                "unresolved publication intent; not restoring cheaply"
            );
            return Ok(Restore::Absent);
        }
    }
    let directory = &public.publication.directory;
    let record = match std::fs::read(directory.join(RESTORE_FILE)) {
        Ok(record) => record,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Restore::Absent),
        Err(e) => return Err(format!("unreadable completed-publication record: {e}").into()),
    };
    let map = public.filters.map();
    let restored = Finalized::restore_completed(
        directory,
        &record,
        &RestoreExpectation {
            network: map.network.clone(),
            genesis_hash: map.genesis_hash.clone(),
            map_sha256: public.publication.map_sha256.clone(),
            full: true,
            recent_from_height: config.recent_from,
            anchor_height: public.publication.height,
            anchor_hash: public.publication.hash.clone(),
        },
    )?;
    let failures = integrity_failures(&config.publication_root)?;
    if let Some(failure) = failures
        .iter()
        .find(|f| lists_failed(std::slice::from_ref(*f), restored.finalized.inventory()))
    {
        return Ok(Restore::Failed(failure.clone()));
    }
    Ok(Restore::Restored(Box::new(restored)))
}

/// Make a restored publication public after fresh authority checks: its exact
/// anchor is still the journal's and the node's, no invalidation intervened,
/// and no invalidation intervened.
pub(super) async fn serve_restored(
    config: &Config,
    rpc: &NodeClient,
    store: &Arc<tokio::sync::Mutex<EventStore>>,
    authority: &Authority,
    validation: transparent_shared::publication::Validation,
) -> Result<(), BoxError> {
    let epoch = authority.0.epoch.load(Ordering::Acquire);
    let current = authority.0.active.read().unwrap().clone();
    let publication = current.publication.clone();
    let distribution = current
        .distribution
        .clone()
        .ok_or("restored publication has no distribution")?;
    let anchored = || async {
        let journal = store.lock().await;
        journal
            .block_at(publication.height)
            .is_some_and(|b| b.block_hash.to_display_hex() == publication.hash)
    };
    if !anchored().await || rpc.block_hash_display(publication.height).await? != publication.hash {
        return Err("restored anchor is not canonical".into());
    }
    {
        let _gate = authority.0.publication_gate.lock().unwrap();
        if authority.0.epoch.load(Ordering::Acquire) != epoch
            || config.publication_root.join("withdrawn.json").exists()
            || authority.0.integrity_failed.load(Ordering::Acquire)
        {
            return Err("restored publication invalidated before serving".into());
        }
        // This process already offered the publication as withdrawn under
        // its startup revision, and query servers keep that withdrawal as
        // final for the revision. Serve it live under a newer one.
        authority.0.distribution_revision.store(
            advance_distribution_revision(&config.publication_root)?,
            Ordering::Release,
        );
        authority
            .0
            .invalidated_from
            .store(u64::MAX, Ordering::Release);
        authority.0.withdrawn.store(false, Ordering::Release);
        let pending = config.publication_root.join("activation.json");
        if pending.exists() {
            std::fs::remove_file(pending)?;
        }
    }
    {
        let mut status = authority.0.status.write().unwrap();
        status["phase"] = "serving".into();
        status["restored"] = true.into();
        status["public_height"] = publication.height.into();
        status["public_hash"] = publication.hash.clone().into();
        status["map_sha256"] = publication.map_sha256.clone().into();
    }
    authority
        .0
        .validation
        .submit(crate::history::validator::Job {
            identity: crate::history::validator::Identity {
                directory: publication.directory.clone(),
                map_sha256: publication.map_sha256.clone(),
                inventory_sha256: distribution.sha256().to_string(),
                height: publication.height,
                hash: publication.hash.clone(),
                epoch,
            },
            inventory: distribution.inventory().clone(),
            validation,
        });
    Ok(())
}

/// A validation status object for `identity`.
pub(super) fn validation_status(
    identity: &crate::history::validator::Identity,
    state: &str,
) -> serde_json::Value {
    serde_json::json!({
        "state": state,
        "publication": {"height": identity.height, "hash": identity.hash},
        "map_sha256": identity.map_sha256,
        "inventory_sha256": identity.inventory_sha256,
    })
}

/// Apply one validator result. A result changes status only while it is about
/// the active inventory; a confirmed mismatch is always recorded, and
/// withdraws the active publication whenever it lists the same bytes.
pub(super) fn validation_event(
    root: &Path,
    authority: &Authority,
    identity: &crate::history::validator::Identity,
    event: crate::history::validator::Event,
) {
    use crate::history::validator::Event;
    use transparent_shared::publication::{CorrectnessError, IntegrityError};
    // Callers hold the publication gate: an activation, reorg or withdrawal
    // advances the epoch or replaces the active publication, after which a
    // result about the old one changes nothing.
    let current = || {
        let active = authority.0.active.read().unwrap();
        authority.0.epoch.load(Ordering::Acquire) == identity.epoch
            && !authority.0.withdrawn.load(Ordering::Acquire)
            && active.publication.directory == identity.directory
            && active.publication.map_sha256 == identity.map_sha256
            && active.publication.height == identity.height
            && active.publication.hash == identity.hash
            && active
                .distribution
                .as_ref()
                .is_some_and(|d| d.sha256() == identity.inventory_sha256)
    };
    let set = |key: &str, value: serde_json::Value| {
        if current() {
            authority.0.status.write().unwrap()[key] = value;
        }
    };
    let key = |phase: &str| format!("{phase}_validation");
    let mismatch = matches!(
        event,
        Event::Integrity(Err(IntegrityError::Mismatch { .. }))
    );
    // The mismatch arm takes the gate itself.
    let _gate = (!mismatch).then(|| authority.0.publication_gate.lock().unwrap());
    let update = |phase: &str, f: &dyn Fn(&mut serde_json::Value)| {
        if current() {
            let mut status = authority.0.status.write().unwrap();
            f(&mut status[key(phase)]);
        }
    };
    match event {
        Event::Started(phase) => {
            let mut value = validation_status(identity, "pending");
            value["started_unix"] = unix_now().into();
            set(&key(phase), value);
        }
        Event::Progress(phase, bytes) => update(phase, &|v| v["bytes_hashed"] = bytes.into()),
        Event::Integrity(Ok(bytes)) => update("integrity", &|v| {
            let started = v["started_unix"].as_f64().unwrap_or_else(unix_now);
            v["state"] = "passed".into();
            v["bytes_hashed"] = bytes.into();
            v["seconds"] = (unix_now() - started).into();
            v.as_object_mut().map(|o| o.remove("reason"));
        }),
        Event::Integrity(Err(IntegrityError::Mismatch {
            path,
            expected_sha256,
            found_sha256,
            reason,
        })) => {
            tracing::error!(%path, %reason, inventory = %identity.inventory_sha256,
                "publication artifact failed integrity validation");
            let failure = IntegrityFailure {
                path,
                expected_sha256,
                found_sha256,
                inventory_sha256: identity.inventory_sha256.clone(),
                map_sha256: identity.map_sha256.clone(),
                reason: reason.clone(),
            };
            let _gate = authority.0.publication_gate.lock().unwrap();
            // An unreadable record is never replaced by one holding only this
            // failure; the publication is withdrawn below either way.
            let recorded = match integrity_failures(root) {
                Ok(mut failures) => {
                    if !failures.contains(&failure) {
                        failures.push(failure.clone());
                    }
                    match serde_json::to_vec(&failures)
                        .map_err(BoxError::from)
                        .and_then(|bytes| {
                            publication::write_durable(&root.join(INTEGRITY_FAILURES), &bytes)
                        }) {
                        Ok(()) => true,
                        Err(error) => {
                            tracing::error!(%error, "could not record the integrity failure");
                            false
                        }
                    }
                }
                Err(error) => {
                    tracing::error!(
                        %error,
                        "integrity failure record is unreadable; kept as is and the publication withdrawn"
                    );
                    false
                }
            };
            let active = authority.0.active.read().unwrap().clone();
            if let Some(distribution) = &active.distribution {
                if lists_failed(std::slice::from_ref(&failure), distribution.inventory()) {
                    authority.0.integrity_failed.store(true, Ordering::Release);
                    authority.0.withdrawn.store(true, Ordering::Release);
                    let mut status = authority.0.status.write().unwrap();
                    status["phase"] = "integrity_failed".into();
                    let mut value = validation_status(identity, "failed");
                    value["reason"] = format!("{}: {reason}", failure.path).into();
                    value["artifact"] = serde_json::to_value(&failure).unwrap_or_default();
                    // False when the failure is not durably recorded, so a
                    // restart would not by itself keep these bytes withdrawn.
                    value["recorded"] = recorded.into();
                    status["integrity_validation"] = value;
                }
            }
        }
        Event::Integrity(Err(
            error @ (IntegrityError::Unavailable(_) | IntegrityError::Stopped),
        )) => update("integrity", &|v| {
            v["state"] = "pending".into();
            v["reason"] = error.to_string().into();
        }),
        Event::Correctness(Ok(checked)) => {
            update("correctness", &|v| {
                let started = v["started_unix"].as_f64().unwrap_or_else(unix_now);
                v["state"] = "passed".into();
                v["runtimes_checked"] = checked.loaded.into();
                v["seconds"] = (unix_now() - started).into();
                v.as_object_mut().map(|o| o.remove("reason"));
            });
            if current() {
                authority.0.checked_runtimes.lock().unwrap().adopt(checked);
            }
        }
        Event::Correctness(Err(CorrectnessError::Failed(reason))) => {
            tracing::error!(%reason, inventory = %identity.inventory_sha256,
                "publication failed correctness validation; still serving");
            update("correctness", &|v| {
                v["state"] = "failed".into();
                v["reason"] = reason.clone().into();
            })
        }
        Event::Correctness(Err(error)) => update("correctness", &|v| {
            v["state"] = "pending".into();
            v["reason"] = error.to_string().into();
        }),
    }
}

/// Restore only producer-local preparation after the completion and authority
/// records passed their existing checks. Invalid sidecars never bootstrap.
pub(super) fn restore_preparation(
    config: &Config,
    public: &Public,
    restored: &transparent_shared::publication::Restored,
) -> Option<transparent_shared::publication::PreparationReuse> {
    use transparent_shared::publication::{RestoreRecord, PREPARATION_FILE, RESTORE_FILE};
    let root = &public.publication.directory;
    if root.parent() != Some(config.publication_root.as_path()) {
        return None;
    }
    let restore = || -> Result<_, BoxError> {
        let completion: RestoreRecord =
            serde_json::from_slice(&pir_publication::records::read_bounded(
                &root.join(RESTORE_FILE),
                transparent_shared::publication::MAX_METADATA_BYTES,
            )?)?;
        let saved = match pir_publication::records::read_bounded(
            &root.join(PREPARATION_FILE),
            transparent_shared::publication::MAX_METADATA_BYTES,
        ) {
            Ok(bytes) => Some(bytes),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        let preparation = restored.publisher_preparation(&completion, saved.as_deref())?;
        if saved.is_none() {
            pir_publication::records::persist(&root.join(PREPARATION_FILE), &preparation.record)?;
        }
        Ok(preparation)
    };
    match restore() {
        Ok(preparation) => Some(preparation),
        Err(error) => {
            tracing::warn!(%error, "publisher preparation cannot be reused; verifying in foreground");
            None
        }
    }
}
