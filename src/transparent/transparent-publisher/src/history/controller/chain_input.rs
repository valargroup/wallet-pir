//! Observe chain changes and commit coherent journal input.
use super::*;

use pir_pipeline::ingestion::{BlockId, ChainObservation, Ingestion};

/// The History event journal as a root-scheduled writer, or a read-only
/// follower of a journal another process writes.
///
/// Independent from publication: backpressure or slow candidate preparation
/// cannot delay canonical observation, revocation or journal commits.
pub(super) struct HistoryIngestion {
    pub(super) config: Config,
    pub(super) rpc: NodeClient,
    pub(super) store: Arc<tokio::sync::Mutex<EventStore>>,
    /// The store's start and newest committed block, updated with each
    /// mutation; the store mutex is only taken for one operation at a time.
    pub(super) start: u64,
    pub(super) end: Option<BlockId>,
    pub(super) authority: Authority,
    pub(super) notify: tokio::sync::watch::Sender<u64>,
    pub(super) cache: OutputCache,
    pub(super) pending_invalidation: Option<u64>,
    /// Whether committed coverage has reached an observed tip; publication
    /// waits while the journal catches up.
    pub(super) current: Arc<AtomicBool>,
    /// The latest observation, reported as a followed journal's coverage.
    pub(super) followed_tip: Option<BlockId>,
}

/// The newest committed block of a store.
pub(super) fn newest(store: &EventStore) -> Option<BlockId> {
    let height = store.covered_through()?;
    Some(BlockId {
        height,
        hash: store.block_at(height)?.block_hash.to_display_hex(),
    })
}

impl HistoryIngestion {
    fn note_tip(&self, tip: &BlockId) {
        let public = self.authority.0.active.read().unwrap();
        if tip.height > public.publication.height
            || (tip.height == public.publication.height && tip.hash != public.publication.hash)
        {
            self.authority
                .0
                .observed
                .write()
                .unwrap()
                .entry(tip.height)
                .or_insert_with(std::time::Instant::now);
        }
        drop(public);
        let mut status = self.authority.0.status.write().unwrap();
        status["node_height"] = tip.height.into();
        status["node_hash"] = tip.hash.clone().into();
    }
    fn acknowledged(&self) -> bool {
        std::fs::read(self.config.publication_root.join("withdrawn.json"))
            .ok()
            .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok())
            .is_some_and(|v| v["acknowledged"] == true)
    }
}

impl Ingestion for HistoryIngestion {
    type Error = BoxError;
    fn start_height(&self, _tip: u64) -> u64 {
        self.start
    }
    fn committed(&self) -> Option<BlockId> {
        if self.config.follow_journal {
            return self.followed_tip.clone();
        }
        self.end.clone()
    }
    async fn checkpoint_at_or_below(&self, height: u64) -> Option<BlockId> {
        if self.config.follow_journal {
            return self.followed_tip.clone().filter(|tip| tip.height <= height);
        }
        let store = self.store.lock().await;
        let height = store.covered_through()?.min(height);
        Some(BlockId {
            height,
            hash: store.block_at(height)?.block_hash.to_display_hex(),
        })
    }
    /// Record the observed tip for freshness. A followed journal is refreshed
    /// and any rollback its writer committed is invalidated, exactly as one this
    /// process performs. An owned journal withdraws an orphaned advertised
    /// endpoint before any deep ancestor search.
    async fn observed(&mut self, observation: &ChainObservation) -> Result<(), BoxError> {
        let tip = &observation.tip;
        self.note_tip(tip);
        if self.config.follow_journal {
            self.followed_tip = Some(tip.clone());
            let (refreshed, end) = {
                let mut journal = self.store.lock().await;
                let end = journal.covered_through();
                (journal.refresh()?, end)
            };
            match refreshed {
                Refreshed::RolledBack { from } => {
                    self.pending_invalidation =
                        Some(self.pending_invalidation.map_or(from, |at| at.min(from)));
                    self.authority.0.status.write().unwrap()["reorg_depth"] =
                        end.map_or(0, |end| (end + 1).saturating_sub(from)).into();
                    self.notify.send_modify(|n| *n += 1);
                }
                Refreshed::Extended => self.notify.send_modify(|n| *n += 1),
                Refreshed::Unchanged => {}
            }
            if let Some(from) = self.pending_invalidation {
                invalidate(&self.config, &self.authority, from).await?;
                self.pending_invalidation = None;
            }
            if let Some(height) = self.store.lock().await.covered_through() {
                self.authority.0.status.write().unwrap()["journal_height"] = height.into();
            }
            self.current.store(true, Ordering::Release);
            return Ok(());
        }
        let public = self.authority.0.active.read().unwrap().clone();
        let public_changed = public.publication.height > tip.height
            || (public.publication.height == tip.height && public.publication.hash != tip.hash)
            || self
                .rpc
                .block_hash_display(public.publication.height)
                .await?
                != public.publication.hash;
        if public_changed
            && (!self.authority.0.withdrawn.load(Ordering::Acquire) || !self.acknowledged())
        {
            invalidate(&self.config, &self.authority, 0).await?;
        }
        if observation.genesis != self.store.lock().await.genesis_hash() {
            return Err("node genesis changed; publication withdrawn and journal retained".into());
        }
        Ok(())
    }
    async fn withdraw_above(&mut self, keep: Option<&BlockId>) -> Result<(), BoxError> {
        invalidate(
            &self.config,
            &self.authority,
            keep.map_or(0, |keep| keep.height + 1),
        )
        .await
    }
    async fn rewind(&mut self, keep: Option<&BlockId>) -> Result<(), BoxError> {
        if self.config.follow_journal {
            // Another process owns this journal; it commits its own rollback.
            return Ok(());
        }
        let end = self.committed().map(|block| block.height);
        let keep = keep.map(|keep| keep.height);
        {
            let mut store = self.store.lock().await;
            store.rollback_to(keep)?;
            self.end = newest(&store);
        }
        self.cache = OutputCache::new(crate::ingest::prevout::DEFAULT_CACHE_OUTPUTS);
        self.authority.0.status.write().unwrap()["reorg_depth"] =
            end.unwrap_or(0).saturating_sub(keep.unwrap_or(0)).into();
        self.notify.send_modify(|n| *n += 1);
        Ok(())
    }
    async fn ingest(&mut self, from: u64, through: u64) -> Result<(), BoxError> {
        if self.config.follow_journal {
            return Ok(());
        }
        self.current.store(false, Ordering::Release);
        for next in from..=through {
            let fetched = self.rpc.fetch_block(next).await?;
            let parent = BlockHash::from_internal_bytes(fetched.parent()).to_display_hex();
            let expected = if next == 0 {
                BlockHash::from_internal_bytes([0; 32]).to_display_hex()
            } else {
                self.end
                    .as_ref()
                    .filter(|end| end.height + 1 == next)
                    .ok_or("missing journal parent")?
                    .hash
                    .clone()
            };
            if parent != expected {
                return Err("node reorganized while fetching block; retry reconciliation".into());
            }
            let built =
                build_fetched_block_events(&self.rpc, &mut self.cache, next, fetched, false)
                    .await?;
            if self.rpc.block_hash_display(next).await? != built.block_hash.to_display_hex() {
                return Err("block became noncanonical during extraction".into());
            }
            {
                // History publications carry no display tables; the txid display
                // controller keeps its own journal with display sidecars.
                let mut journal = self.store.lock().await;
                journal.append_block(next, built.block_hash, &built.events)?;
                journal.commit()?;
            }
            self.end = Some(BlockId {
                height: next,
                hash: built.block_hash.to_display_hex(),
            });
            self.authority.0.status.write().unwrap()["journal_height"] = next.into();
            self.notify.send_modify(|n| *n += 1);
        }
        Ok(())
    }
    async fn caught_up(&mut self, _: &ChainObservation) -> Result<(), BoxError> {
        if !self.current.swap(true, Ordering::AcqRel) {
            self.notify.send_modify(|n| *n += 1);
        }
        Ok(())
    }
    fn failed(&mut self, error: &BoxError) -> pir_pipeline::FailurePolicy {
        if error.downcast_ref::<EventStoreError>().is_some() {
            return pir_pipeline::FailurePolicy::Stop;
        }
        tracing::error!(%error,"chain/journal follow failed; retrying without advancing coverage");
        self.authority.0.status.write().unwrap()["ingest_error"] = error.to_string().into();
        pir_pipeline::FailurePolicy::NextPoll
    }
}

pub(super) async fn invalidate(
    config: &Config,
    authority: &Authority,
    from: u64,
) -> Result<(), BoxError> {
    {
        let _gate = authority.0.publication_gate.lock().unwrap();
        let active = authority.0.active.read().unwrap();
        // A journal-only fork cancels candidates, but does not orphan this
        // older endpoint, which keeps serving.
        let retain =
            from > active.publication.height && !authority.0.withdrawn.load(Ordering::Acquire);
        if !retain {
            authority.0.withdrawn.store(true, Ordering::Release);
            authority.0.validation.cancel();
        }
        let epoch = authority.0.epoch.fetch_add(1, Ordering::AcqRel) + 1;
        if retain {
            authority.0.validation.retain(epoch);
        }
        authority.0.invalidated_from.store(from, Ordering::Release);
        authority.0.distribution_revision.store(
            advance_distribution_revision(&config.publication_root)?,
            Ordering::Release,
        );
        publication::write_atomic(
            &config.publication_root.join("withdrawn.json"),
            &serde_json::to_vec(&serde_json::json!({"from_height":from,"acknowledged":false}))?,
        )?;
    }
    publication::write_atomic(
        &config.publication_root.join("withdrawn.json"),
        &serde_json::to_vec(&serde_json::json!({"from_height":from,"acknowledged":true}))?,
    )?;
    Ok(())
}
