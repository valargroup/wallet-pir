//! Apply committed source updates and durable rollback fences.
use super::*;

impl Controller {
    pub(super) async fn poll_source(&mut self) -> Result<(), BoxError> {
        let fence = self.fence.clone();
        // Refresh, apply and archive planning must see one journal state. A
        // rollback either precedes this refresh or waits for its planned floor.
        let _journal = fence.journal().await;
        let floor = self.sealed_floor();
        let mut before_rollback =
            |ancestor: u64, hash: BlockHash| fence.before_rollback(ancestor, hash);
        let polled = pir_pipeline::acquire(
            async {
                self.source
                    .poll(
                        &mut self.store,
                        &self.cache,
                        floor,
                        &fence,
                        &mut before_rollback,
                    )
                    .await
                    .map(Input::Work)
            },
            &mut (),
        )
        .await;
        match polled {
            Ok(Input::Work(update)) => self.apply(update).await?,
            Ok(Input::Unchanged | Input::Deferred) => {}
            Err(SourceError::SealedReorg { ancestor, floor }) => {
                self.timeline.record(
                    "reorg",
                    json!({"ancestor": ancestor.to_string(), "floor": floor, "sealed": true}),
                );
                self.halt(Halt::SealedReorg { ancestor, floor });
            }
            Err(SourceError::Store(error)) => return Err(error.into()),
            Err(SourceError::Transient(error)) => {
                self.timeline
                    .record("error", json!({"stage": "source", "error": error}));
            }
        }
        Ok(())
    }

    pub(super) async fn apply(&mut self, update: SourceUpdate) -> Result<(), BoxError> {
        if let Some(ancestor) = update.rollback_to {
            let old_tip = self.cache.tip().map(|t| t.0);
            self.timeline.record(
                "reorg",
                json!({"ancestor": ancestor, "from_height": ancestor + 1, "old_tip": old_tip,
                    "depth": old_tip.map(|t| t.saturating_sub(ancestor))}),
            );
            // Sources that roll the journal back have recorded this already.
            if let Some(block) = self.store.block_at(ancestor) {
                self.fence.noted(ancestor, block.block_hash);
            }
            self.save_invalidations();
            self.cache.truncate_after(ancestor);
            if let Err(error) = self.index.truncate_after(ancestor) {
                tracing::error!(%error, "height index truncation failed");
            }
            self.observed.split_off(&(ancestor + 1));
        }
        for block in update.blocks {
            if let Err(error) = self.index.append(block.height, &block.records) {
                tracing::error!(%error, "height index append failed");
            }
            self.timeline.record(
                "block",
                json!({"height": block.height, "hash": block.hash.to_display_hex(),
                    "observed_ms": block.observed_ms, "ingested_ms": block.ingested_ms,
                    "records": block.records.len()}),
            );
            self.observed.insert(block.height, block.observed_ms);
            self.cache
                .push(block.height, block.hash, block.records, block.observed_ms)?;
        }
        if let Some(error) = update.error {
            self.timeline
                .record("error", json!({"stage": "source", "error": error}));
        }
        if let Some(lag) = update.lag {
            self.timeline.record("lag", lag);
        }
        self.plan();
        Ok(())
    }
}
