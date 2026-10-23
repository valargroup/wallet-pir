//! Construct the recent material from stable cache handles.
use super::*;

impl Controller {
    /// Build the recent shard and map from the committed cache and ordered seals.
    pub(super) async fn build_cycle(&mut self) -> Result<DisplayMaterial, CycleError> {
        let started = Instant::now();
        let (tip, tip_hash) = self.cache.tip().ok_or_else(|| retry("empty cache"))?;
        let tip_hash = tip_hash.to_display_hex();
        let count = self.publishable();
        let built: Vec<&SealJob> = self.queue.iter().take(count).collect();
        let new_seals: Vec<SealRecord> = built.iter().map(|j| j.record()).collect();
        let mut archives: Vec<DisplayMapEntry> = self
            .map
            .shards
            .iter()
            .filter(|s| s.sealed)
            .cloned()
            .chain(
                built
                    .iter()
                    .map(|j| j.shard.as_ref().unwrap().entry.clone()),
            )
            .collect();
        let excess = archives
            .len()
            .saturating_sub(self.layout.max_archive_shards as usize);
        let dropped: Vec<DisplayMapEntry> = archives.drain(..excess).collect();

        let last = new_seals.last().or(self.active.seals.last());
        let recent_id = last.map_or(0, |s| s.shard_id + 1);
        let unsealed = last.map_or(self.layout.start_height, |s| s.end_height + 1);
        if unsealed
            != self.unsealed_start()
                + new_seals
                    .iter()
                    .map(|s| s.end_height + 1 - s.start_height)
                    .sum::<u64>()
        {
            return Err(retry("queued seals are not contiguous with the archives"));
        }
        // The recent window reaches back over the newest archives, but never
        // below the map's start or the cache's.
        let floor = archives
            .first()
            .map_or(self.layout.start_height, |a| a.start_height)
            .max(self.cache.start());
        let recent_start = recent_start(
            &self.layout.seal,
            floor,
            unsealed,
            self.cache.counts_from(floor),
        );
        // The block before the window lies at or below the sealed floor, so
        // the journal's hash for it never changes.
        let parent_block_hash = match last {
            Some(seal) if seal.end_height + 1 == recent_start => seal.terminal_block_hash.clone(),
            _ if recent_start == self.layout.start_height => self.layout.base_parent.clone(),
            _ => self
                .store
                .block_at(recent_start - 1)
                .ok_or_else(|| retry(format!("journal has no block at {}", recent_start - 1)))?
                .block_hash
                .to_display_hex(),
        };
        let spec = ShardSpec {
            shard_id: recent_id,
            start: recent_start,
            end: tip,
            parent_block_hash,
            terminal_block_hash: tip_hash.clone(),
            parent_manifest_digest: last.map_or(String::new(), |s| s.digest.clone()),
            sealed: false,
            // The recent shard keeps one revision lineage per shard id while
            // its window slides.
            previous: self
                .recent_previous
                .as_ref()
                .filter(|(id, _, _)| *id == recent_id)
                .map(|(_, _, p)| p.clone()),
        };
        // A stop abandons the unactivated candidate as a normal outcome.
        // Each blocking step is awaited; publishing the recent shard and
        // runtime_preparationing its runtimes each finish once started.
        let stopped = |controller: &Self| {
            let stopped = controller.shutdown.requested();
            if stopped {
                tracing::info!(tip, "display candidate preparation stopped by shutdown");
            }
            stopped
        };
        if stopped(self) {
            return Err(retry("display publication stopped"));
        }
        let parts = self.cache.records(recent_start..=tip);
        let (root, layout) = (self.root.clone(), self.layout.clone());
        let recent =
            tokio::task::spawn_blocking(move || publish_parts(&root, &layout, &spec, &parts))
                .await
                .map_err(retry)??;
        if stopped(self) {
            return Err(retry("display publication stopped"));
        }
        let map = self.layout.map(&archives, recent.entry.clone());
        map.check_shape()
            .map_err(|e| CycleError::Halt(Halt::SealedEntryChanged(e)))?;
        check_sealed_continuity(&self.map, &map, dropped.len())
            .map_err(|e| CycleError::Halt(Halt::SealedEntryChanged(e)))?;
        Ok(DisplayMaterial {
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
        })
    }
}
