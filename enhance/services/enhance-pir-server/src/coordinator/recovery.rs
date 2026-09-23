//! Durable recovery, fencing, and decision delivery.
use super::*;

impl Coordinator {
    /// Persist revocation before the caller truncates any canonical journal bytes.
    /// Retrying an unfinished rollback uses the same epoch and revocation set.
    pub async fn revoke_after(&self, records: u64) -> Result<(), String> {
        {
            let mut store = self.store.lock().unwrap();
            store.update(|state| {
                if state.recovery.rollback_to == Some(records) {
                    return Ok(());
                }
                let first = records / (32768 * enhance_pir::RECORDS_PER_ROW as u64);
                let deep = state.recovery.sealed.keys().any(|id| *id >= first);
                if deep {
                    state.recovery.epoch = state
                        .recovery
                        .epoch
                        .checked_add(1)
                        .ok_or("recovery epoch exhausted")?;
                    for id in first..enhance_pir::protocol::MAX_QUERY_SHARDS {
                        state
                            .recovery
                            .domain_epochs
                            .insert(id, state.recovery.epoch.to_string());
                    }
                }
                for manifest in &state.published {
                    for domain in &manifest.coverage.shards {
                        // A tail domain includes its predecessor's suffix.
                        if deep && domain.id >= first {
                            state
                                .recovery
                                .revoked
                                .insert(hex::encode(manifest.session_id(domain.id)?));
                        }
                    }
                }
                state.recovery.sealed.retain(|id, _| *id < first);
                state.recovery.rollback_to = Some(records);
                Ok(())
            })?;
        }
        self.deliver_revocations().await
    }

    pub(super) async fn deliver_revocations(&self) -> Result<(), String> {
        let (fence, groups) = {
            let store = self.store.lock().unwrap();
            (
                worker::Revocation {
                    recovery_epoch: store.state().recovery.epoch,
                    sessions: store.state().recovery.revoked.clone(),
                },
                store.state().groups.clone(),
            )
        };
        if fence.sessions.is_empty() {
            return Ok(());
        }
        if let Some(serving) = &self.serving {
            serving.revoke(&fence).await?;
        }
        // A missing peer stays fenced at the coordinator; publication waits for
        // its durable acknowledgment rather than trusting an old incarnation.
        for replica in groups.iter().flat_map(|g| &g.replicas) {
            let result = async {
                checked(
                    self.http
                        .post(format!("{}/internal/revoke", replica.url))
                        .timeout(std::time::Duration::from_secs(3))
                        .json(&fence)
                        .send()
                        .await
                        .map_err(|e| e.to_string())?,
                )
                .await?;
                Ok::<_, String>(())
            }
            .await;
            if let Err(error) = result {
                if self.serving.is_some() {
                    return Err(error);
                }
                tracing::warn!(replica = %replica.name, %error, "replica excluded until recovery fence acknowledged");
            }
        }
        Ok(())
    }

    pub(super) fn pending_replicas(&self) -> BTreeSet<String> {
        self.store.lock().unwrap().state().pending_replicas()
    }

    // Retry decisions independently; no disconnected participant authorizes
    // dropping its reservation or putting a second candidate on that worker.
    pub(super) async fn deliver_decisions(&self) -> Result<(), String> {
        self.deliver_commits().await?;
        let (pending, groups, epoch) = {
            let store = self.store.lock().unwrap();
            (
                store.state().pending_aborts.clone(),
                store.state().groups.clone(),
                store.state().epoch,
            )
        };
        for notification in pending {
            let replica = groups
                .iter()
                .flat_map(|g| &g.replicas)
                .find(|r| r.name == notification.replica)
                .ok_or("pending abort has no registered worker")?;
            let delivered: Result<(), String> = async {
                checked(
                    self.http
                        .post(format!("{}/internal/abort", replica.url))
                        .timeout(std::time::Duration::from_secs(3))
                        .json(&worker::Abort {
                            epoch,
                            operation: notification.operation.clone(),
                            attempt: notification.attempt,
                        })
                        .send()
                        .await
                        .map_err(|e| e.to_string())?,
                )
                .await?;
                Ok(())
            }
            .await;
            match delivered {
                Ok(()) => self.store.lock().unwrap().update(|s| {
                    s.pending_aborts.retain(|p| p != &notification);
                    Ok(())
                })?,
                Err(error) => {
                    tracing::warn!(replica = %replica.name, %error, "abort notification remains pending; worker excluded from new candidates")
                }
            }
        }
        Ok(())
    }

    // Caller owns the publication permit. Errors for one participant must not
    // prevent delivery to its peer; intent remains durable until an exact ack.
    pub(super) async fn deliver_commits(&self) -> Result<(), String> {
        let (pending, groups, epoch) = {
            let store = self.store.lock().unwrap();
            (
                store.state().pending_commits.clone(),
                store.state().groups.clone(),
                store.state().epoch,
            )
        };
        for notification in pending {
            let replica = groups
                .iter()
                .flat_map(|g| &g.replicas)
                .find(|r| r.name == notification.replica)
                .ok_or("pending commit has no registered worker")?;
            let delivered: Result<(), String> = async {
                let health: serde_json::Value = checked(
                    self.http
                        .get(format!("{}/internal/health", replica.url))
                        .timeout(std::time::Duration::from_secs(3))
                        .send()
                        .await
                        .map_err(|e| e.to_string())?,
                )
                .await?
                .json()
                .await
                .map_err(|e| e.to_string())?;
                let generation = notification.manifest.generation;
                // A pending replica receives no reserve, abort, retain or commit,
                // so its epoch and revision cannot move and its candidate cannot
                // clear. This exact fresh-state shape (the same one that admits a
                // new replica) is therefore only reachable from a wiped data
                // directory. The decision is durable here and on the peer; the
                // fresh worker holds nothing, so the notification is forfeited
                // and the next publication prepares it from scratch.
                if health["published"].as_array().is_some_and(|p| p.is_empty())
                    && health["candidate"].is_null()
                    && health["epoch"].as_u64() == Some(0)
                    && health["revision"].as_u64() == Some(0)
                {
                    tracing::warn!(replica = %replica.name, "worker restarted with fresh state; committed notification forfeited, next publication re-prepares it");
                    self.telemetry.lock().unwrap().forfeited_commits += 1;
                    return Ok(());
                }
                if health["published"]
                    .as_array()
                    .is_some_and(|gs| gs.iter().any(|g| g.as_u64() == Some(generation)))
                {
                    // Commit was durable but its response was lost. Match the
                    // exact decision before releasing this replica's quarantine.
                    if health["published_manifest_digests"][generation.to_string()]
                        != digest(&notification.manifest)
                    {
                        return Err(
                            "published worker manifest differs from committed decision".into()
                        );
                    }
                    return Ok(());
                }
                let candidate: worker::Candidate =
                    serde_json::from_value(health["candidate"].clone())
                        .map_err(|e| e.to_string())?;
                if candidate.operation != notification.operation
                    || candidate.attempt != notification.attempt
                    || candidate.generation != generation
                {
                    return Err("worker lost committed candidate; repair required".into());
                }
                checked(
                    preparation_request(&self.http, &replica.url)
                        .json(&candidate)
                        .send()
                        .await
                        .map_err(|e| e.to_string())?,
                )
                .await?;
                let revision = health["revision"]
                    .as_u64()
                    .ok_or("missing revision")?
                    .checked_add(1)
                    .ok_or("worker revision exhausted")?;
                checked(
                    self.http
                        .post(format!("{}/internal/commit", replica.url))
                        .json(&worker::Commit {
                            epoch,
                            revision,
                            generation,
                            manifest_digest: digest(&notification.manifest),
                            retained: notification.retained.clone(),
                        })
                        .send()
                        .await
                        .map_err(|e| e.to_string())?,
                )
                .await?;
                Ok(())
            }
            .await;
            match delivered {
                Ok(()) => self.store.lock().unwrap().update(|s| {
                    s.pending_commits.retain(|p| p != &notification);
                    Ok(())
                })?,
                Err(error) => {
                    tracing::warn!(replica = %replica.name, %error, "committed notification remains pending; worker excluded from new candidates")
                }
            }
        }
        Ok(())
    }

    /// Recover the durable decision. A committed placement is never rolled back by cancellation.
    pub async fn reconcile(&self) -> Result<(), String> {
        let _permit = self
            .publication
            .clone()
            .try_acquire_owned()
            .map_err(|_| "publication already in progress")?;
        self.reconcile_inner().await
    }

    // Caller owns the publication permit, including fallback within publication.
    pub(super) async fn reconcile_inner(&self) -> Result<(), String> {
        self.deliver_revocations().await?;
        let (op, published) = {
            let store = self.store.lock().unwrap();
            let s = store.state();
            (s.operation.clone(), s.published.clone())
        };
        let Some(op) = op else {
            self.deliver_decisions().await?;
            self.synchronize_retention().await;
            self.collect_retired_artifacts();
            self.synchronize_routers().await?;
            return Ok(());
        };
        let committed = matches!(
            op.phase,
            Phase::Committed | Phase::Draining | Phase::Complete
        );
        if committed {
            // Also migrate a pre-outbox committed journal before releasing its
            // candidate slot. Notification intent is durable before release.
            let manifest = published
                .first()
                .ok_or("committed operation missing manifest")?;
            self.store.lock().unwrap().update(|s| {
                for ack in &op.readiness {
                    if !s.pending_commits.iter().any(|p| p.replica == ack.replica) {
                        s.pending_commits.push(PendingCommit {
                            replica: ack.replica.clone(),
                            operation: op.id.clone(),
                            attempt: op.attempt,
                            manifest: manifest.clone(),
                            retained: published.iter().map(|m| m.generation).collect(),
                        });
                    }
                }
                s.cancel_participants(
                    &op,
                    &op.readiness.iter().map(|ack| ack.replica.clone()).collect(),
                );
                Ok(())
            })?;
        } else {
            self.store.lock().unwrap().abort_operation()?;
        }

        if committed {
            let manifest = published.first().ok_or("missing committed manifest")?;
            if self.manifest().await.as_ref() != Some(manifest) {
                let root = self.root.clone();
                let manifest = manifest.clone();
                let revoked = self.store.lock().unwrap().state().recovery.revoked.clone();
                let mut cache = BTreeMap::new();
                for snapshot in self.snapshots.read().await.iter() {
                    for (id, pack) in &snapshot.packing {
                        cache.insert(snapshot.saved.domain_keys[id].clone(), pack.clone());
                    }
                }
                let remote_packing = self.serving.is_some();
                let budget = self.packing_budget.clone();
                let restored = tokio::task::spawn_blocking(move || {
                    restore(
                        &root,
                        &manifest,
                        &revoked,
                        &mut cache,
                        remote_packing,
                        &budget,
                    )
                })
                .await
                .map_err(|e| e.to_string())??;
                let mut snapshots = self.snapshots.write().await;
                snapshots.insert(0, restored);
                snapshots.retain(|s| {
                    published
                        .iter()
                        .any(|m| m.generation == s.saved.manifest.generation)
                });
            }
        }
        self.store.lock().unwrap().update(|s| {
            if committed {
                if let Some(mut operation) = s.operation.take() {
                    operation.phase = Phase::Draining;
                    s.draining.push(operation);
                }
                s.draining.retain(|op| {
                    op.source_generations
                        .iter()
                        .any(|g| s.published.iter().any(|m| m.generation == *g))
                });
            }
            Ok(())
        })?;
        self.deliver_decisions().await?;
        self.synchronize_retention().await;
        self.collect_retired_artifacts();
        self.synchronize_routers().await?;
        Ok(())
    }
}
