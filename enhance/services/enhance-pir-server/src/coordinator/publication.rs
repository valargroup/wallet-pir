//! Candidate publication under the coordinator publication permit.
use super::*;
use sha2::Digest;

impl Coordinator {
    /// Serialize candidate preparation. Cancellation leaves a durable operation that must
    /// be reconciled before another candidate can be prepared.
    pub async fn publish(
        &self,
        journal: &crate::store::RecordJournal,
        height: u64,
        hash: String,
    ) -> Result<(), String> {
        self.publish_checked(journal, height, hash, || async { Ok(()) })
            .await
    }

    pub async fn publish_checked<F, Fut>(
        &self,
        journal: &crate::store::RecordJournal,
        height: u64,
        hash: String,
        validate: F,
    ) -> Result<(), String>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<(), String>>,
    {
        journal.ensure_healthy().map_err(|e| e.to_string())?;
        let _permit = self
            .publication
            .clone()
            .try_acquire_owned()
            .map_err(|_| "publication already in progress")?;
        let began = std::time::Instant::now();
        self.telemetry
            .lock()
            .unwrap()
            .observe(height, journal.tree_size(), &hash);
        let mut validate = Some(validate);
        let mut result = self
            .publish_inner(
                journal,
                height,
                hash.clone(),
                &mut validate,
                true,
                &BTreeSet::new(),
            )
            .await;
        let optional_retry = result
            .as_ref()
            .err()
            .is_some_and(|e| e.starts_with("optional worker "));
        let suppressed: BTreeSet<String> = result
            .as_ref()
            .err()
            .filter(|_| optional_retry)
            .and_then(|e| e.split_whitespace().nth(2))
            .map(str::to_owned)
            .into_iter()
            .collect();
        let retry = result.is_err()
            && validate.is_some()
            && (optional_retry || {
                let store = self.store.lock().unwrap();
                store.state().operation.as_ref().is_some_and(|op| {
                    op.phase == Phase::Planned
                        && op
                            .require_both
                            .iter()
                            .any(|group| store.state().assignments.values().any(|g| g == group))
                })
            });
        if retry {
            // No preparation or durable publication has occurred. Persist the
            // abort decision before retrying; unacknowledged workers stay excluded.
            result = match self.reconcile_inner().await {
                Ok(()) => {
                    self.publish_inner(journal, height, hash, &mut validate, false, &suppressed)
                        .await
                }
                Err(error) => Err(format!("publication retry awaits abort recovery: {error}")),
            };
        }
        *self.blocked.lock().unwrap() = result.as_ref().err().cloned();
        let mut telemetry = self.telemetry.lock().unwrap();
        telemetry.last_attempt_seconds = Some(began.elapsed().as_secs_f64());
        telemetry.last_attempt_succeeded = Some(result.is_ok());
        if result.is_ok() {
            telemetry.consecutive_failures = 0;
            let target = telemetry.target_identity();
            if target != telemetry.last_successful_target {
                telemetry.last_advancement_unix_seconds = Some(crate::control::notification_time());
                telemetry.last_successful_target = target;
            }
        } else {
            telemetry.consecutive_failures = telemetry.consecutive_failures.saturating_add(1);
        }
        result
    }

    pub(super) async fn publish_inner<F, Fut>(
        &self,
        journal: &crate::store::RecordJournal,
        height: u64,
        hash: String,
        validate: &mut Option<F>,
        allow_consolidation: bool,
        suppressed: &BTreeSet<String>,
    ) -> Result<(), String>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<(), String>>,
    {
        self.deliver_decisions().await?;
        let revoked = self.store.lock().unwrap().state().recovery.revoked.clone();
        if !revoked.is_empty() {
            let mut snapshots = self.snapshots.write().await;
            for snapshot in snapshots.iter_mut() {
                let packing = snapshot
                    .packing
                    .iter()
                    .filter(|(id, _)| {
                        snapshot
                            .saved
                            .manifest
                            .session_id(**id)
                            .is_ok_and(|hash| !revoked.contains(&hex::encode(hash)))
                    })
                    .map(|(id, pack)| (*id, pack.clone()))
                    .collect();
                *snapshot = Arc::new(Snapshot {
                    saved: snapshot.saved.clone(),
                    packing,
                });
            }
        }
        let (mut lifecycle, generation, attempt, epoch, revision, mut groups, previous) = {
            let store = self.store.lock().unwrap();
            let s = store.state();
            if s.operation.is_some() {
                return Err(
                    "unfinished operation requires reconciliation before publication".into(),
                );
            }
            (
                s.lifecycle.clone(),
                s.next_generation,
                s.next_attempt,
                s.epoch,
                s.revision,
                s.groups.clone(),
                s.assignments.clone(),
            )
        };
        let mut coverage = lifecycle.coverage(journal.tree_size(), Geometry::default())?;
        let mut recovery = self.store.lock().unwrap().state().recovery.clone();
        for shard in &mut coverage.shards {
            recovery
                .domain_epochs
                .entry(shard.id)
                .or_insert_with(|| "0".into());
            if shard.records == 32768 * enhance_pir::RECORDS_PER_ROW as u64 {
                let end = (shard.id + 1) * 32768 * enhance_pir::RECORDS_PER_ROW as u64;
                let crossing = journal
                    .blocks()
                    .iter()
                    .find(|b| b.first_position + b.action_count >= end)
                    .ok_or("missing completing block")?;
                if let Some((sealed_height, sealed_hash)) = recovery.sealed.get(&shard.id) {
                    if *sealed_height != crossing.height || *sealed_hash != crossing.hash {
                        return Err("sealed history changed without recovery fencing".into());
                    }
                    shard.state = enhance_pir::protocol::ShardState::Sealed;
                } else if height.saturating_sub(crossing.height) >= 1000 {
                    recovery
                        .sealed
                        .insert(shard.id, (crossing.height, crossing.hash.clone()));
                    shard.state = enhance_pir::protocol::ShardState::Sealed;
                }
            }
        }
        let pool_state = self.store.lock().unwrap().state().pool.clone();
        let base_assignments = if pool_state.is_some() {
            coverage
                .shards
                .iter()
                .map(|s| (s.id, groups[0].id.clone()))
                .collect()
        } else {
            control::assign(&coverage, &groups, &previous)?
        };
        let read = |start, count| {
            journal
                .read_records(start, count)
                .map_err(|e| e.to_string())
        };
        let plans: Vec<DomainPlan> = coverage
            .shards
            .iter()
            .map(|s| {
                let mut plan = runtime::plan(s.clone(), read)?;
                let epoch = recovery.domain_epochs[&s.id]
                    .parse::<u64>()
                    .map_err(|e| e.to_string())?;
                for unit in &mut plan.units {
                    unit.recovery_epoch = epoch;
                }
                Ok::<_, String>(plan)
            })
            .collect::<Result<_, _>>()?;
        let pool_assignments = if let Some(pool) = &pool_state {
            Some(
                self.plan_pool(
                    &coverage,
                    &groups,
                    &plans,
                    pool,
                    allow_consolidation,
                    suppressed,
                )
                .await?,
            )
        } else {
            None
        };
        let (assignments, consolidation_destinations, admission_destinations) =
            if let Some(placements) = &pool_assignments {
                let assignments = placements
                    .iter()
                    .map(|(id, workers)| {
                        let optional = pool_state
                            .as_ref()
                            .map(|p| p.optional_workers())
                            .unwrap_or_default();
                        let group = groups
                            .iter()
                            .find(|g| {
                                g.replicas.iter().any(|r| {
                                    workers.contains(&r.name) && !optional.contains(&r.name)
                                })
                            })
                            .expect("validated pool inventory");
                        (*id, group.id.clone())
                    })
                    .collect();
                (assignments, BTreeSet::new(), BTreeSet::new())
            } else {
                let (base_assignments, admission_destinations) = self
                    .place_new_if_admitted(&coverage, &groups, &plans, &previous, base_assignments)
                    .await?;
                let base_assignments = self
                    .relocate_if_memory_refused(
                        &coverage,
                        &groups,
                        &plans,
                        &previous,
                        base_assignments,
                    )
                    .await;
                let (assignments, consolidation_destinations) = if allow_consolidation {
                    self.consolidate_if_admitted(&coverage, &groups, &plans, base_assignments)
                        .await?
                } else {
                    (base_assignments, BTreeSet::new())
                };
                (
                    assignments,
                    consolidation_destinations,
                    admission_destinations,
                )
            };
        let operation_id = format!("generation-{generation}");
        let mut require_both = control::required_destination_pairs(&assignments, &previous);
        require_both.extend(consolidation_destinations);
        require_both.extend(admission_destinations);
        let strict_groups = require_both.clone();
        let operation = Operation {
            pool_assignments: pool_assignments.clone(),
            id: operation_id.clone(),
            attempt,
            epoch,
            expected_revision: revision,
            candidate_digest: digest(&plans),
            phase: Phase::Planned,
            assignments: assignments.clone(),
            affected_groups: if pool_assignments.is_some() {
                groups.iter().map(|g| g.id.clone()).collect()
            } else {
                assignments
                    .values()
                    .chain(previous.values())
                    .cloned()
                    .collect()
            },
            source_generations: self
                .store
                .lock()
                .unwrap()
                .state()
                .published
                .iter()
                .map(|m| m.generation)
                .collect(),
            require_both,
            readiness: Vec::new(),
            infrastructure_resources: BTreeMap::new(),
        };
        self.store.lock().unwrap().plan(operation)?;
        let mut candidates = BTreeMap::new();
        // Reserve complete per-worker assignments before any preparation.
        for group in &mut groups {
            for replica in &mut group.replicas {
                let own: Vec<_> = plans
                    .iter()
                    .filter(|p| {
                        pool_assignments.as_ref().map_or_else(
                            || assignments[&p.shard.id] == group.id,
                            |placements| placements[&p.shard.id].contains(&replica.name),
                        )
                    })
                    .cloned()
                    .collect();
                let previously_used = pool_state.as_ref().map_or_else(
                    || previous.values().any(|id| id == &group.id),
                    |pool| {
                        pool.placements
                            .values()
                            .any(|set| set.contains(&replica.name))
                    },
                );
                if own.is_empty() && !previously_used {
                    continue;
                }
                if self.pending_replicas().contains(&replica.name) {
                    if strict_groups.contains(&group.id)
                        || (pool_assignments.is_some() && !own.is_empty())
                    {
                        return Err("required replica awaits decision recovery".into());
                    }
                    continue;
                }
                let reservation: Result<_, String> = async {
                    // Every new incarnation must durably acknowledge the full
                    // fence before it can become a ready placement again.
                    let fence = worker::Revocation {
                        recovery_epoch: recovery.epoch,
                        sessions: recovery.revoked.clone(),
                    };
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
                    let health: serde_json::Value = self
                        .http
                        .get(format!("{}/internal/health", replica.url))
                        .timeout(std::time::Duration::from_secs(3))
                        .send()
                        .await
                        .map_err(|e| e.to_string())?
                        .error_for_status()
                        .map_err(|e| e.to_string())?
                        .json()
                        .await
                        .map_err(|e| e.to_string())?;
                    if health["protocol"].as_str() != Some(PROTOCOL_REVISION)
                        || health["placement_policy"]
                            != serde_json::to_value(group.placement_policy).unwrap()
                    {
                        return Err("worker protocol or placement policy mismatch".into());
                    }
                    replica.incarnation = health["incarnation"]
                        .as_str()
                        .ok_or("missing worker incarnation")?
                        .into();
                    let worker_revision = health["revision"]
                        .as_u64()
                        .ok_or("missing worker revision")?;
                    let candidate = worker::Candidate {
                        placement_policy: group.placement_policy,
                        operation: operation_id.clone(),
                        attempt,
                        epoch,
                        expected_revision: worker_revision,
                        generation,
                        plans: own.clone(),
                    };
                    let response = self
                        .http
                        .post(format!("{}/internal/reserve", replica.url))
                        .json(&candidate)
                        .send()
                        .await
                        .map_err(|e| e.to_string())?;
                    if response.status() == StatusCode::INSUFFICIENT_STORAGE {
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map_err(|e| e.to_string())?
                            .as_secs();
                        self.store.lock().unwrap().update(|state| {
                            if state.pool.is_some() {
                                state.capacity.request_worker(
                                    now,
                                    state.groups.iter().map(|g| g.replicas.len()).sum(),
                                )
                            } else {
                                state.capacity.memory_refused(
                                    journal.tree_size(),
                                    now,
                                    state.groups.len(),
                                )
                            }
                        })?;
                    }
                    let missing: Vec<String> = checked(response)
                        .await?
                        .json()
                        .await
                        .map_err(|e| e.to_string())?;
                    Ok((candidate, missing, worker_revision))
                }
                .await;
                match reservation {
                    Ok(reserved) => {
                        candidates.insert(replica.name.clone(), reserved);
                    }
                    Err(error)
                        if !strict_groups.contains(&group.id)
                            && (pool_assignments.is_none() || own.is_empty()) =>
                    {
                        tracing::warn!(replica = %replica.name, %error, "replica unavailable; ordinary publication may use its peer");
                    }
                    Err(error)
                        if pool_state
                            .as_ref()
                            .is_some_and(|p| p.optional_workers().contains(&replica.name)) =>
                    {
                        return Err(format!(
                            "optional worker {} reservation failed: {error}",
                            replica.name
                        ));
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        if let Some(placements) = &pool_assignments {
            for workers in placements.values() {
                if workers.iter().any(|name| !candidates.contains_key(name)) {
                    return Err("pool lacks required reservation quorum".into());
                }
            }
        } else {
            for group in &groups {
                if !assignments.values().any(|id| id == &group.id) {
                    continue;
                }
                let reserved = group
                    .replicas
                    .iter()
                    .filter(|r| candidates.contains_key(&r.name))
                    .count();
                if reserved
                    < if strict_groups.contains(&group.id) {
                        2
                    } else {
                        1
                    }
                {
                    return Err("group lacks its required reservation quorum".into());
                }
            }
        }
        self.store.lock().unwrap().update(|s| {
            s.groups = groups.clone();
            Ok(())
        })?;
        self.store
            .lock()
            .unwrap()
            .advance(epoch, &operation_id, attempt, Phase::Reserved)?;
        self.store
            .lock()
            .unwrap()
            .advance(epoch, &operation_id, attempt, Phase::Preparing)?;
        let mut packing = BTreeMap::new();
        let mut hints = BTreeMap::new();
        let domain_keys: BTreeMap<_, _> = plans
            .iter()
            .map(|p| (p.shard.id, digest(&(p.shard.logical_rows, &p.units))))
            .collect();
        {
            let previous = self.snapshots.read().await;
            for snapshot in previous.iter() {
                for (shard, key) in &domain_keys {
                    if !packing.contains_key(shard)
                        && snapshot.saved.domain_keys.get(shard) == Some(key)
                    {
                        packing.insert(*shard, snapshot.packing[shard].clone());
                        hints.insert(*shard, snapshot.saved.hints[shard].clone());
                    }
                }
            }
        }
        let reusable: BTreeSet<_> = packing.keys().copied().collect();
        let mut routes = BTreeMap::<u64, Vec<String>>::new();
        for group in &groups {
            for replica in &group.replicas {
                let Some((candidate, missing, _)) = candidates.get(&replica.name) else {
                    continue;
                };
                let prepared: Result<(), String> = async {
                    for plan in &candidate.plans {
                        for (spec, unit) in plan.shard.units.iter().zip(&plan.units) {
                            if !missing.contains(&unit.digest()) {
                                continue;
                            }
                            let rows = runtime::unit_rows(&plan.shard, spec, &mut { read })?;
                            checked(
                                self.http
                                    .put(format!("{}/internal/rows/{}", replica.url, unit.digest()))
                                    .header("x-enhance-epoch", candidate.epoch)
                                    .header("x-enhance-attempt", candidate.attempt)
                                    .header("x-enhance-operation", &candidate.operation)
                                    .body(rows)
                                    .send()
                                    .await
                                    .map_err(|e| e.to_string())?,
                            )
                            .await?;
                        }
                    }
                    checked(
                        preparation_request(&self.http, &replica.url)
                            .json(candidate)
                            .send()
                            .await
                            .map_err(|e| e.to_string())?,
                    )
                    .await?;
                    for plan in &candidate.plans {
                        if reusable.contains(&plan.shard.id) {
                            routes
                                .entry(plan.shard.id)
                                .or_default()
                                .push(replica.url.clone());
                            continue;
                        }
                        let response = checked(
                            self.http
                                .get(format!("{}/internal/hint/{}", replica.url, plan.shard.id))
                                .send()
                                .await
                                .map_err(|e| e.to_string())?,
                        )
                        .await?;
                        let bytes = bounded(response, 256 * 1024 * 1024).await?;
                        if let Some(name) = hints.get(&plan.shard.id) {
                            if crate::prepared_packing::hash_file(
                                &self.root.join("hints").join(name),
                            )? == hex::encode(sha2::Sha256::digest(&bytes))
                            {
                                routes
                                    .entry(plan.shard.id)
                                    .or_default()
                                    .push(replica.url.clone());
                                continue;
                            }
                        }

                        let params = parameters(plan.shard.logical_rows)?;
                        let hint = crate::wire::read_crs_blocks(
                            bytes.as_slice(),
                            params.db_cols / runtime::rlwe().d,
                            runtime::rlwe().d,
                        )
                        .map_err(|e| e.to_string())?;
                        let pack =
                            Packing::new(plan.shard.logical_rows, &hint, &self.packing_budget)?;
                        if self.serving.is_some() {
                            crate::prepared_packing::persist(&self.root, &pack)?;
                        }
                        let pack = Arc::new(PublishedPacking::new(
                            plan.shard.logical_rows,
                            pack,
                            self.serving.is_none(),
                        ));
                        let reference = pack.reference(plan.shard.id)?;
                        if let Some(existing) = packing.get(&plan.shard.id) {
                            let existing: &Arc<PublishedPacking> = existing;
                            if existing.reference(plan.shard.id)? != reference {
                                return Err("replica public material differs".into());
                            }
                        } else {
                            let name = format!("{}.bin", reference.public_params_sha256);
                            crate::artifact::write_atomic(&self.root.join("hints"), &name, |f| {
                                f.write_all(&bytes)
                            })
                            .map_err(|e| e.to_string())?;
                            crate::artifact::write_atomic(&self.root.join("public"), &name, |f| {
                                f.write_all(&pack.public)
                            })
                            .map_err(|e| e.to_string())?;
                            hints.insert(plan.shard.id, name);
                            packing.insert(plan.shard.id, pack);
                        }
                        routes
                            .entry(plan.shard.id)
                            .or_default()
                            .push(replica.url.clone());
                    }
                    Ok(())
                }
                .await;
                prepared.map_err(|error| {
                    if pool_state
                        .as_ref()
                        .is_some_and(|p| p.optional_workers().contains(&replica.name))
                    {
                        format!(
                            "optional worker {} preparation failed: {error}",
                            replica.name
                        )
                    } else {
                        error
                    }
                })?;
            }
        }
        let sessions = coverage
            .shards
            .iter()
            .map(|s| packing[&s.id].reference(s.id))
            .collect::<Result<_, _>>()?;
        let placement_revision = {
            let snapshots = self.snapshots.read().await;
            match snapshots.first() {
                Some(old) if old.saved.routes == routes => old.saved.manifest.placement_revision,
                Some(old) => old
                    .saved
                    .manifest
                    .placement_revision
                    .checked_add(1)
                    .ok_or("placement revision exhausted")?,
                None => 1,
            }
        };
        let manifest = Manifest {
            recovery_epoch: recovery.epoch,
            placement_revision,
            domain_recovery_epochs: coverage
                .shards
                .iter()
                .map(|s| (s.id, recovery.domain_epochs[&s.id].clone()))
                .collect(),
            schema_version: SCHEMA_VERSION,
            protocol_revision: PROTOCOL_REVISION.into(),
            network: "main".into(),
            pool: "ironwood".into(),
            generation,
            anchor_height: height,
            anchor_block_hash: hash,
            geometry: Geometry::default(),
            coverage,
            sessions,
            unit_identities: plans
                .iter()
                .map(|p| (p.shard.id, p.units.clone()))
                .collect(),
        };
        manifest.validate()?;
        let manifest_digest = digest(&manifest);
        let mut readiness = Vec::new();
        for group in &groups {
            for replica in &group.replicas {
                if !candidates.contains_key(&replica.name) {
                    continue;
                }
                let request = worker::Activation {
                    operation: operation_id.clone(),
                    attempt,
                    epoch,
                    manifest: manifest.clone(),
                };
                let ack: Result<serde_json::Value, String> = async {
                    checked(
                        self.http
                            .post(format!("{}/internal/activate", replica.url))
                            .json(&request)
                            .send()
                            .await
                            .map_err(|e| e.to_string())?,
                    )
                    .await?
                    .json()
                    .await
                    .map_err(|e| e.to_string())
                }
                .await;
                let ack = ack.map_err(|error| {
                    if pool_state
                        .as_ref()
                        .is_some_and(|p| p.optional_workers().contains(&replica.name))
                    {
                        format!(
                            "optional worker {} activation failed: {error}",
                            replica.name
                        )
                    } else {
                        error
                    }
                })?;
                if ack["incarnation"].as_str() != Some(&replica.incarnation)
                    || ack["candidate_digest"].as_str() != Some(&manifest_digest)
                {
                    return Err(
                        if pool_state
                            .as_ref()
                            .is_some_and(|p| p.optional_workers().contains(&replica.name))
                        {
                            format!("optional worker {} stale readiness", replica.name)
                        } else {
                            "stale worker readiness".into()
                        },
                    );
                }
                readiness.push(ReadyAck {
                    replica: replica.name.clone(),
                    incarnation: replica.incarnation.clone(),
                    candidate_digest: manifest_digest.clone(),
                });
            }
        }
        let saved = SavedSnapshot {
            manifest: manifest.clone(),
            routes,
            prepared: if self.serving.is_some() {
                hints
                    .iter()
                    .map(|(id, name)| {
                        Ok((
                            *id,
                            crate::prepared_packing::describe_hint(
                                &self.root,
                                name,
                                manifest
                                    .coverage
                                    .shards
                                    .iter()
                                    .find(|s| s.id == *id)
                                    .ok_or("unknown domain")?
                                    .logical_rows,
                            )?,
                        ))
                    })
                    .collect::<Result<_, String>>()?
            } else {
                BTreeMap::new()
            },
            hints,
            domain_keys,
        };
        let router_readiness = self.prepare_routers(Some(&saved)).await?;
        // Wait for admitted HTTP queries before retiring worker routes. Preparations did not block them.
        let mut snapshots = self.snapshots.write().await;
        validate
            .take()
            .ok_or("canonical validation already consumed")?()
        .await?;
        crate::artifact::write_atomic(
            &self.root.join("snapshots"),
            &format!("{generation}.json"),
            |f| serde_json::to_writer(f, &saved).map_err(std::io::Error::other),
        )
        .map_err(|e| e.to_string())?;
        File::open(self.root.join("snapshots"))
            .and_then(|f| f.sync_all())
            .map_err(|e| e.to_string())?;
        self.store.lock().unwrap().update(|s| {
            let op = s.operation.as_mut().ok_or("candidate disappeared")?;
            op.candidate_digest = manifest_digest.clone();
            op.readiness = readiness;
            Ok(())
        })?;
        self.store
            .lock()
            .unwrap()
            .advance(epoch, &operation_id, attempt, Phase::Ready)?;
        self.store
            .lock()
            .unwrap()
            .commit_with_recovery(manifest.clone(), lifecycle, recovery)?;
        snapshots.insert(0, Arc::new(Snapshot { saved, packing }));
        snapshots.truncate(RETAINED_GENERATIONS);
        drop(snapshots);
        if let Some(serving) = &self.serving {
            serving.activate(generation, router_readiness).await?;
        }
        self.reconcile_inner().await
    }
}
