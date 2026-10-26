//! Durably activate a source-bound history publication.
use super::*;

pub(super) async fn activate_history(
    config: &Config,
    rpc: &NodeClient,
    authority: &Authority,
    ready: HistoryCandidate,
) -> Result<(), BoxError> {
    let HistoryCandidate {
        candidate,
        epoch,
        height,
        hash,
        started,
        published_unix,
        prepared_unix,
        runtimes,
        storage,
    } = ready;
    // The host rechecked this exact source immediately before entering commit.
    if authority.0.epoch.load(Ordering::Acquire) != epoch {
        return Err("candidate invalidated before activation".into());
    }
    // The node request can outlive a stop request. Until activation intent is
    // written, abandoning the candidate still leaves the active publication alone.
    if authority.0.shutdown.requested() {
        tracing::info!(height, "candidate activation stopped by shutdown");
        return Ok(());
    }
    let directory = &candidate.publication.directory;
    // External initial publications are verified in full but are not adopted
    // into this publisher's immutable store and cannot supply restart reuse.
    let stored = directory.parent() == Some(config.publication_root.as_path());
    let background = candidate
        .distribution
        .as_ref()
        .filter(|d| d.requires_validation())
        .map(|d| d.validation(directory, config.recent_from, true))
        .transpose()?;

    {
        let distribution = candidate
            .distribution
            .as_ref()
            .ok_or("candidate has no prepared distribution")?;
        let failures = integrity_failures(&config.publication_root)?;
        if distribution.inventory().artifacts.iter().any(|a| {
            failures
                .iter()
                .any(|f| f.found_sha256.as_deref() == Some(a.sha256.as_str()))
        }) {
            return Err("candidate lists bytes that failed integrity validation".into());
        }
        // Completion evidence for a cheap restart, written only now that the
        // required preparation passed and before the active pointer moves.
        let record = distribution.restore_record(true, config.recent_from, height, &hash)?;
        if stored {
            let preparation = distribution.preparation_record(directory, &record)?;
            pir_publication::records::persist(
                &directory.join(transparent_shared::publication::PREPARATION_FILE),
                &preparation,
            )?;
        }
        pir_publication::records::persist(
            &directory.join(transparent_shared::publication::RESTORE_FILE),
            &record,
        )?;
    }
    publication::write_durable(
        &config.publication_root.join("activation.json"),
        &serde_json::to_vec(&candidate.publication)?,
    )?;
    #[cfg(test)]
    if INTERRUPT_AFTER_INTENT
        .lock()
        .unwrap()
        .remove(&config.publication_root)
    {
        return Err("interrupted after writing activation intent".into());
    }
    if authority.0.epoch.load(Ordering::Acquire) != epoch
        || rpc.block_hash_display(height).await? != hash
    {
        // The ingest loop invalidates the exact changed suffix.
        authority.0.withdrawn.store(true, Ordering::Release);
        return Err("candidate invalidated during activation".into());
    }
    let publication = candidate.publication.clone();
    let freshness = authority
        .0
        .observed
        .read()
        .unwrap()
        .range(..=height)
        .map(|(_, seen)| seen.elapsed().as_secs_f64())
        .fold(0f64, f64::max);
    {
        let _gate = authority.0.publication_gate.lock().unwrap();
        if authority.0.shutdown.requested() {
            return Err(transparent_shared::publication::Stopped.into());
        }
        if authority.0.epoch.load(Ordering::Acquire) != epoch {
            return Err("reorg raced the public pointer commit".into());
        }
        if let Some(distribution) = &candidate.distribution {
            // A failed failure-record write still withdraws this process.
            // Only ordinary preparation can recover; inherited evidence cannot.
            if distribution.requires_validation()
                && (authority.0.integrity_failed.load(Ordering::Acquire)
                    || lists_failed(
                        &integrity_failures(&config.publication_root)?,
                        distribution.inventory(),
                    ))
            {
                return Err("inherited preparation cannot clear an integrity failure".into());
            }
            // Every candidate must still name the files preparation checked,
            // including external initial publications that cannot supply receipts.
            distribution.check_prepared_files(&publication.directory)?;
        }
        crate::storage::layout::complete(&config.publication_root, "transparent_history")?;
        publication::write_durable(
            &config.publication_root.join("active.json"),
            &serde_json::to_vec(&candidate.publication)?,
        )?;
        authority.0.distribution_revision.store(
            advance_distribution_revision(&config.publication_root)?,
            Ordering::Release,
        );
        *authority.0.active.write().unwrap() = Arc::new(candidate);
        authority
            .0
            .invalidated_from
            .store(u64::MAX, Ordering::Release);
        authority.0.withdrawn.store(false, Ordering::Release);
        authority.0.integrity_failed.store(false, Ordering::Release);
        // The successor replaces the running job. Completed per-artifact
        // validation remains in the validator for the next inventory.
        authority.0.validation.cancel();
        // Freshly prepared bytes listed as a failed artifact was are verified
        // replacements; other recorded failures remain.
        if let Some(distribution) = &authority.0.active.read().unwrap().distribution {
            let failures = integrity_failures(&config.publication_root)?;
            let remaining: Vec<_> = failures
                .iter()
                .filter(|f| !lists_failed(std::slice::from_ref(*f), distribution.inventory()))
                .cloned()
                .collect();
            if remaining.len() != failures.len() {
                publication::write_durable(
                    &config.publication_root.join(INTEGRITY_FAILURES),
                    &serde_json::to_vec(&remaining)?,
                )?;
            }
        }
        for name in ["activation.json", "withdrawn.json"] {
            let p = config.publication_root.join(name);
            if p.exists() {
                std::fs::remove_file(p)?;
            }
        }
    }
    {
        let mut status = authority.0.status.write().unwrap();
        status["phase"] = "serving".into();
        status["public_height"] = height.into();
        status["public_hash"] = hash.clone().into();
        status["map_sha256"] = publication.map_sha256.clone().into();
        status["cycle_seconds"] = started.elapsed().as_secs_f64().into();
        authority
            .0
            .cycle_duration
            .observe(started.elapsed().as_secs_f64());
        authority.0.publication_freshness.observe(freshness);
        authority.0.last_publication.store(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            Ordering::Relaxed,
        );
        status["freshness_seconds"] = freshness.into();
        status.as_object_mut().unwrap().remove("publication_error");
        status.as_object_mut().unwrap().remove("restored");
        // Inherited producer preparation is not fresh validation.
        for phase in ["integrity_validation", "correctness_validation"] {
            status[phase] = serde_json::json!({"state":if background.is_some() { "pending" } else { "passed" },
                "source":if background.is_some() { "background" } else { "preparation" },
                "publication":{"height":height,"hash":hash},
                "map_sha256":publication.map_sha256,
                "inventory_sha256":authority.0.active.read().unwrap().distribution.as_ref().map(|d| d.sha256().to_string())});
        }
        // What this cycle did, for operators and measurements: when its
        // candidate was published, prepared and activated, what its runtimes
        // cost here, and what storing and verifying its objects hashed or
        // took over from earlier evidence.
        let cycle = serde_json::json!({
            "height": height,
            "hash": hash,
            "map_sha256": publication.map_sha256,
            "first_observed_seconds_ago": freshness,
            "published_unix": published_unix,
            "prepared_unix": prepared_unix,
            "activated_unix": unix_now(),
            "cycle_seconds": started.elapsed().as_secs_f64(),
            "runtime_preparation": runtimes,
            "storage": storage,
        });
        tracing::info!(%cycle, "publication cycle");
        status["last_cycle"] = cycle;
    }
    if let Some(validation) = background {
        let distribution = authority
            .0
            .active
            .read()
            .unwrap()
            .distribution
            .clone()
            .ok_or("activated publication has no distribution")?;
        authority
            .0
            .validation
            .submit(crate::history::validator::Job {
                identity: crate::history::validator::Identity {
                    directory: publication.directory.clone(),
                    map_sha256: publication.map_sha256.clone(),
                    inventory_sha256: distribution.sha256().into(),
                    height,
                    hash: hash.clone(),
                    epoch,
                },
                inventory: distribution.inventory().clone(),
                validation,
            });
    }
    authority
        .0
        .observed
        .write()
        .unwrap()
        .retain(|h, _| *h > height);
    tracing::info!(
        height,
        hash,
        freshness_seconds = freshness,
        seconds = started.elapsed().as_secs_f64(),
        "activated warm publication"
    );
    Ok(())
}
