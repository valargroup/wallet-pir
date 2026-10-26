//! Commit complete restart state before exposing successor authority.
use super::*;

impl Controller {
    pub(super) async fn activate_cycle(
        &mut self,
        candidate: DisplayCandidate,
        finalized: Option<DisplayActivation>,
    ) -> Result<(), CycleError> {
        let fence = self.fence.clone();
        // Commit authority and the map named by later rollback records before
        // another journal rollback can begin.
        let _journal = fence.journal().await;
        if let Some((ancestor, floor)) = fence.sealed() {
            return Err(CycleError::Halt(Halt::SealedReorg { ancestor, floor }));
        }
        let DisplayCandidate {
            material,
            candidate,
            sha,
            candidate_ms,
            runtime_preparation,
        } = candidate;
        let DisplayMaterial {
            started,
            tip,
            tip_hash,
            new_seals,
            dropped,
            floor,
            unsealed,
            recent_start,
            recent_id,
            recent,
            map,
        } = material;
        // A stop requested during preparation abandons the unactivated
        // candidate. Once finalization begins, it and its commit finish.
        if self.shutdown.requested() {
            tracing::info!(tip, "display candidate activation stopped by shutdown");
            return Ok(());
        }
        // The rollback record vouches only for what was published when it was
        // made. This candidate may be published without being committed, so
        // a restart from here on rewrites the recent range.
        self.fence.clear().map_err(retry)?;
        let mut next_active = self.active.clone();
        next_active.seals.extend(new_seals.iter().cloned());
        next_active.directory = candidate;
        next_active.map_sha256 = sha.clone();
        next_active.tip = tip;
        next_active.tip_hash = tip_hash.clone();
        next_active.cycle += 1;
        let finalize_started = Instant::now();
        if let Some(DisplayActivation {
            distribution,
            revision,
            finalized,
        }) = finalized
        {
            distribution
                .commit_publication(finalized, revision, &self.shutdown, &next_active)
                .map_err(|error| {
                    // Background validation can withdraw authority after the
                    // final phase check, including while the journal lock is
                    // awaited. No activation write began for this rejection.
                    if error.is::<super::super::distribution::PreparationChanged>() {
                        retry(error)
                    } else {
                        CycleError::Halt(Halt::Activation(error.to_string()))
                    }
                })?;
        } else {
            next_active
                .store(&self.root)
                .map_err(|error| CycleError::Halt(Halt::Activation(error.to_string())))?;
        }
        let activate_ms = ms(finalize_started);
        let activated_ms = now_ms();
        self.last_success_ms = activated_ms;

        // Committed: query servers download the new map.
        self.queue.drain(..new_seals.len());
        // Keep enough sealed blocks to fill the window from the sealed floor,
        // so a rollback above it can always move the window back.
        let keep_from = window_start(
            &self.layout.seal,
            floor,
            &self.cache.counts_from(floor)[..(unsealed - floor) as usize],
        );
        self.cache.drop_through(keep_from.min(recent_start) - 1);
        self.recent_previous = Some((recent_id, recent_start, recent.revision()));
        self.active = next_active;
        self.fence.activated(&self.active.map_sha256);
        self.fence.set_floor(self.sealed_floor());
        self.map = map;
        drop(_journal);
        #[cfg(feature = "fixtures")]
        if self.settings.record_maps {
            self.maps.push(self.map.clone());
        }
        let fresh: Vec<(u64, u64)> = self.observed.range(..=tip).map(|(h, o)| (*h, *o)).collect();
        self.observed = self.observed.split_off(&(tip + 1));
        for entry in &dropped {
            self.timeline.record(
                "drop",
                json!({"shard_id": entry.shard_id, "digest": entry.manifest_digest,
                    "start": entry.start_height, "end": entry.end_height,
                    "cycle": self.active.cycle}),
            );
        }
        let t = recent.timings;
        self.last_cycle = json!({
            "cycle": self.active.cycle, "tip": tip, "tip_hash": tip_hash,
            "first_observed_ms": fresh.iter().map(|f| f.1).min(),
            "build_s": t.build_s, "verify_s": t.verify_s, "digest_s": t.digest_s,
            "write_s": t.write_s, "candidate_ms": candidate_ms,
            "preparation_ms": runtime_preparation.as_ref().map(|p| p.0),
            "shipped_bytes": runtime_preparation.as_ref().map(|p| p.1.iter().map(|f| f.bytes).sum::<u64>()),
            "shipped": runtime_preparation.as_ref().map(|p| &p.1),
            "activate_ms": activate_ms,
            "activated_ms": activated_ms, "cycle_ms": ms(started),
            "freshness_ms": fresh.iter().map(|f| activated_ms.saturating_sub(f.1)).collect::<Vec<_>>(),
            "recent": recent.summary(), "archives": self.map.shards.len() - 1,
            "first_shard_id": self.map.first_shard_id,
            "archive_chain_sha256": self.active.archive_chain_sha256(),
            "map_sha256": sha,
            "sealed_published": new_seals.iter().map(|s| s.shard_id).collect::<Vec<_>>(),
            "dropped": dropped.iter().map(|s| s.shard_id).collect::<Vec<_>>(),
        });
        self.timeline.record("cycle", self.last_cycle.clone());
        let stages = [
            ("build", t.build_s),
            ("verify", t.verify_s),
            ("digest", t.digest_s),
            ("write", t.write_s),
        ]
        .into_iter()
        .chain(
            runtime_preparation
                .as_ref()
                .map(|p| ("prepare", p.0 as f64 / 1e3)),
        )
        .chain([("activate", activate_ms as f64 / 1e3)]);
        self.settings.metrics.observe_cycle(
            started.elapsed().as_secs_f64(),
            fresh
                .iter()
                .map(|f| activated_ms.saturating_sub(f.1) as f64 / 1e3),
            stages,
        );
        self.collect();
        Ok(())
    }
}
