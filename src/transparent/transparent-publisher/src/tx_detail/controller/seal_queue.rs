//! Plan ordered archive work; the root executes each preparation.
use super::*;

impl Controller {
    /// Queues every seal the rule makes over the unqueued part of the cache.
    pub(super) fn plan(&mut self) {
        let Some((tip, _)) = self.cache.tip() else {
            return;
        };
        let from = self
            .queue
            .back()
            .map_or_else(|| self.unsealed_start(), |j| j.end + 1);
        let next_id = self.queue.back().map_or_else(
            || self.active.seals.last().map_or(0, |s| s.shard_id + 1),
            |j| j.shard_id + 1,
        );
        let seals = plan_seals(&self.layout.seal, from, self.cache.counts_from(from));
        for (shard_id, range) in (next_id..).zip(seals) {
            self.queue.push_back(SealJob {
                shard_id,
                start: *range.start(),
                end: *range.end(),
                decided_tip: tip,
                bucket_counts: self.cache.archive_counts(range),
                state: JobState::Planned,
                shard: None,
                attempts: 0,
                retry_at: None,
            });
        }
        self.fence.set_floor(self.sealed_floor());
    }

    /// Starts the next seal build, one at a time.
    pub(super) fn schedule<E: pir_pipeline::publisher::PublisherExecutor>(&mut self, executor: E) {
        let now = Instant::now();
        if !self.building {
            if let Some(index) = self
                .queue
                .iter()
                .position(|j| j.state == JobState::Planned)
                .filter(|i| self.queue[*i].ready(now))
            {
                // Builds run in order, so every earlier job is built already.
                let parent = match index {
                    0 => self
                        .active
                        .seals
                        .last()
                        .map(|s| (s.digest.clone(), s.terminal_block_hash.clone())),
                    _ => self.queue[index - 1]
                        .shard
                        .as_ref()
                        .map(|s| (s.digest.clone(), s.manifest.terminal_block_hash.clone())),
                };
                let job = &self.queue[index];
                let Some(terminal) = self.cache.block(job.end).map(|b| b.hash.to_display_hex())
                else {
                    return;
                };
                let spec = ShardSpec {
                    shard_id: job.shard_id,
                    start: job.start,
                    end: job.end,
                    parent_block_hash: parent
                        .as_ref()
                        .map_or(self.layout.base_parent.clone(), |p| p.1.clone()),
                    terminal_block_hash: terminal,
                    parent_manifest_digest: parent.map_or(String::new(), |p| p.0),
                    sealed: true,
                    previous: None,
                };
                let parts = self.cache.records(job.start..=job.end);
                let (root, layout, sender) =
                    (self.root.clone(), self.layout.clone(), self.sender.clone());
                let shard_id = job.shard_id;
                self.queue[index].state = JobState::Building;
                self.building = true;
                let shutdown = self.shutdown.clone();
                tokio::spawn(async move {
                    let result = executor
                        .publish(preparation::ArchivePreparation {
                            root,
                            layout,
                            spec,
                            parts,
                            shutdown,
                        })
                        .await
                        .map(Box::new)
                        .map_err(PublishError::from);
                    let _ = sender.send(Background { shard_id, result });
                });
            }
        }
    }

    pub(super) fn background(&mut self, Background { shard_id, result }: Background) {
        self.building = false;
        let Some(job) = self.queue.iter_mut().find(|j| j.shard_id == shard_id) else {
            return;
        };
        match result {
            Ok(shard) => {
                job.state = JobState::Built;
                job.shard = Some(*shard);
                job.attempts = 0;
                job.retry_at = None;
                self.timeline.record("seal", seal_event(job));
                self.last_seal_ms = now_ms();
            }
            Err(PublishError::Other(error)) => {
                job.state = JobState::Planned;
                job.retry_at = Some(Instant::now() + backoff(job.attempts));
                job.attempts += 1;
                self.timeline.record(
                    "error",
                    json!({"stage": "seal_build", "shard_id": shard_id, "error": error}),
                );
            }
            Err(error) => {
                if let CycleError::Halt(halt) = CycleError::from(error) {
                    self.halt(halt);
                }
            }
        }
    }
}
