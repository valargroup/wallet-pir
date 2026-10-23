//! Observe chain changes and commit coherent journal input.
use super::*;

// Independent from publication: backpressure or slow candidate preparation cannot
// delay canonical observation, revocation or journal commits.
pub(super) struct HistoryInputTask {
    pub(super) config: Config,
    pub(super) rpc: NodeClient,
    pub(super) store: Arc<tokio::sync::Mutex<EventStore>>,
    pub(super) authority: Authority,
    pub(super) notify: tokio::sync::watch::Sender<u64>,
    pub(super) cache: OutputCache,
    pub(super) pending_invalidation: Option<u64>,
}
impl pir_pipeline::Task for HistoryInputTask {
    type Error = BoxError;
    async fn run(&mut self) -> Result<(), BoxError> {
        pir_pipeline::acquire(
            async {
                if self.config.follow_journal {
                    follow_once(
                        &self.config,
                        &self.rpc,
                        &self.store,
                        &self.authority,
                        &mut self.pending_invalidation,
                        &self.notify,
                    )
                    .await?;
                } else {
                    ingest_once(
                        &self.config,
                        &self.rpc,
                        &self.store,
                        &self.authority,
                        &mut self.cache,
                        &self.notify,
                    )
                    .await?;
                }
                Ok::<_, BoxError>(Input::Work(()))
            },
            &mut (),
        )
        .await?;
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

/// One pass of following a journal another process writes: observe the
/// node's tip for freshness, re-read the journal's checkpoint, and treat a
/// rollback its writer committed exactly as ingest treats one it performs.
///
/// A rollback whose invalidation fails is retried on the next pass, from the
/// lowest height seen, until it is recorded.
pub(super) async fn follow_once(
    config: &Config,
    rpc: &NodeClient,
    store: &Arc<tokio::sync::Mutex<EventStore>>,
    authority: &Authority,
    pending_invalidation: &mut Option<u64>,
    notify: &tokio::sync::watch::Sender<u64>,
) -> Result<(), BoxError> {
    let tip = rpc.tip_height().await?;
    let tip_hash = rpc.block_hash_display(tip).await?;
    {
        let public = authority.0.active.read().unwrap();
        if tip > public.publication.height
            || (tip == public.publication.height && tip_hash != public.publication.hash)
        {
            authority
                .0
                .observed
                .write()
                .unwrap()
                .entry(tip)
                .or_insert_with(std::time::Instant::now);
        }
    }
    authority.0.status.write().unwrap()["node_height"] = tip.into();
    authority.0.status.write().unwrap()["node_hash"] = tip_hash.into();
    let (refreshed, end) = {
        let mut journal = store.lock().await;
        let end = journal.covered_through();
        (journal.refresh()?, end)
    };
    match refreshed {
        Refreshed::RolledBack { from } => {
            *pending_invalidation = Some(pending_invalidation.map_or(from, |at| at.min(from)));
            authority.0.status.write().unwrap()["reorg_depth"] =
                end.map_or(0, |end| (end + 1).saturating_sub(from)).into();
            notify.send_modify(|n| *n += 1);
        }
        Refreshed::Extended => notify.send_modify(|n| *n += 1),
        Refreshed::Unchanged => {}
    }
    if let Some(from) = *pending_invalidation {
        invalidate(config, authority, from).await?;
        *pending_invalidation = None;
    }
    if let Some(height) = store.lock().await.covered_through() {
        authority.0.status.write().unwrap()["journal_height"] = height.into();
    }
    Ok(())
}

pub(super) async fn ingest_once(
    config: &Config,
    rpc: &NodeClient,
    store: &Arc<tokio::sync::Mutex<EventStore>>,
    authority: &Authority,
    cache: &mut OutputCache,
    notify: &tokio::sync::watch::Sender<u64>,
) -> Result<(), BoxError> {
    let tip = rpc.tip_height().await?;
    let tip_hash = rpc.block_hash_display(tip).await?;
    {
        let public = authority.0.active.read().unwrap();
        if tip > public.publication.height
            || (tip == public.publication.height && tip_hash != public.publication.hash)
        {
            authority
                .0
                .observed
                .write()
                .unwrap()
                .entry(tip)
                .or_insert_with(std::time::Instant::now);
        }
    }
    authority.0.status.write().unwrap()["node_height"] = tip.into();
    authority.0.status.write().unwrap()["node_hash"] = tip_hash.clone().into();
    let public = authority.0.active.read().unwrap().clone();
    let public_changed = public.publication.height > tip
        || rpc.block_hash_display(public.publication.height).await? != public.publication.hash;
    let acknowledged = || {
        std::fs::read(config.publication_root.join("withdrawn.json"))
            .ok()
            .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok())
            .is_some_and(|v| v["acknowledged"] == true)
    };
    // A deep ancestor search can take many RPC calls. Once the advertised
    // endpoint is known to be orphaned, withdraw before doing that search.
    let withdrew_early =
        public_changed && (!authority.0.withdrawn.load(Ordering::Acquire) || !acknowledged());
    if withdrew_early {
        invalidate(config, authority, 0).await?;
    }
    if public_changed && rpc.block_hash_display(0).await? != store.lock().await.genesis_hash() {
        return Err("node genesis changed; publication withdrawn and journal retained".into());
    }
    let mut ancestor = store.lock().await.covered_through();
    while let Some(height) = ancestor {
        let hash = store
            .lock()
            .await
            .block_at(height)
            .ok_or("missing journal endpoint")?
            .block_hash;
        if height <= tip && rpc.block_hash_display(height).await? == hash.to_display_hex() {
            break;
        }
        ancestor = height.checked_sub(1);
    }
    let end = store.lock().await.covered_through();
    let invalid_from = if ancestor != end {
        Some(ancestor.map_or(0, |h| h + 1))
    } else {
        None
    };
    if let Some(from) = invalid_from {
        // Repeated calls are necessary if a previous remote invalidation failed.
        // Avoid changing the epoch again after the same fork was reconciled.
        if !withdrew_early
            && (ancestor != end
                || !authority.0.withdrawn.load(Ordering::Acquire)
                || !acknowledged())
        {
            invalidate(config, authority, from).await?;
        }
    }
    if ancestor != end {
        store.lock().await.rollback_to(ancestor)?;
        *cache = OutputCache::new(crate::ingest::prevout::DEFAULT_CACHE_OUTPUTS);
        authority.0.status.write().unwrap()["reorg_depth"] = end
            .unwrap_or(0)
            .saturating_sub(ancestor.unwrap_or(0))
            .into();
        notify.send_modify(|n| *n += 1);
    }
    let mut next = store.lock().await.next_height();
    while next <= tip {
        let fetched = rpc.fetch_block(next).await?;
        let parent = BlockHash::from_internal_bytes(fetched.parent()).to_display_hex();
        let expected = if next == 0 {
            BlockHash::from_internal_bytes([0; 32]).to_display_hex()
        } else {
            store
                .lock()
                .await
                .block_at(next - 1)
                .ok_or("missing journal parent")?
                .block_hash
                .to_display_hex()
        };
        if parent != expected {
            return Err("node reorganized while fetching block; retry reconciliation".into());
        }
        let built = build_fetched_block_events(rpc, cache, next, fetched, false).await?;
        if rpc.block_hash_display(next).await? != built.block_hash.to_display_hex() {
            return Err("block became noncanonical during extraction".into());
        }
        {
            // History publications carry no display tables; the txid display
            // controller keeps its own journal with display sidecars.
            let mut journal = store.lock().await;
            journal.append_block(next, built.block_hash, &built.events)?;
            journal.commit()?;
        }
        authority.0.status.write().unwrap()["journal_height"] = next.into();
        notify.send_modify(|n| *n += 1);
        next += 1;
    }
    Ok(())
}
