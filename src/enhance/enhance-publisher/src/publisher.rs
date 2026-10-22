//! Publisher-owned preparation and recovery, independent of consumer placement.
mod http;
pub(crate) mod preparation;
mod recovery;
mod work;

use crate::{
    chain_source::ZakuraClient,
    distribution::PublicationDistribution,
    durable_state::PublisherState,
    journal::{CommittedJournalSnapshot, RecordJournal},
};
use enhance_shared::{
    protocol::{Manifest, MAX_QUERY_SHARDS},
    publication::Revocation,
};
use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use tokio::sync::Semaphore;

struct PublisherRuntimeState {
    durable: PublisherState,
    poisoned: bool,
    preparing: Option<u64>,
    error: Option<String>,
}

/// A bounded work token pins the committed journal prefix and the withdrawal
/// observed before preparation. Acquisition may append concurrently. A rewind
/// invalidates this token before changing any journal bytes.
#[derive(Clone)]
pub struct PublicationWork {
    owner: String,
    snapshot: Arc<CommittedJournalSnapshot>,
    manifest: Manifest,
    withdrawal_revision: u64,
}
/// Own durable publication identities and prepare local material independently
/// of consumers. Clones share locks, preparation capacity, and failure state.
#[derive(Clone)]
pub struct EnhancePublisher {
    root: PathBuf,
    state: Arc<Mutex<PublisherRuntimeState>>,
    distribution: PublicationDistribution,
    preparation: Arc<Semaphore>,
    incarnation: String,
    _owner: Arc<File>,
}
impl EnhancePublisher {
    /// Acquire the publisher's exclusive lifetime lock, validate saved floors,
    /// and reconcile retained publications. Restart durably withdraws authority;
    /// consumers need a newly checked publication before they can serve it.
    pub fn open(root: &Path, rpc: ZakuraClient) -> Result<Self, String> {
        Self::open_recovered(root, rpc)
    }

    /// Reserve a generation for the journal's committed prefix. Returns `None`
    /// when that prefix is already published. A later rewind invalidates the
    /// returned work before changing journal bytes.
    pub fn work(&self, journal: &RecordJournal) -> Result<Option<PublicationWork>, String> {
        self.reserve_work(journal)
    }

    pub fn distribution(&self) -> &PublicationDistribution {
        &self.distribution
    }

    /// Durable revocation precedes journal rewind. Repeated delivery of the
    /// same unfinished rewind preserves its epoch and revoked-session set.
    pub fn revoke_after(&self, records: u64) -> Result<(), String> {
        let mut state = self.state.lock().unwrap();
        let mut next = state.durable.clone();
        let first_affected_shard = records / (32768 * enhance_shared::RECORDS_PER_ROW as u64);
        let rewinds_sealed_history = next
            .recovery
            .sealed
            .keys()
            .any(|id| *id >= first_affected_shard);
        if rewinds_sealed_history {
            next.recovery.epoch = next
                .recovery
                .epoch
                .checked_add(1)
                .ok_or("recovery epoch exhausted")?;
            for id in first_affected_shard..MAX_QUERY_SHARDS {
                next.recovery
                    .domain_epochs
                    .insert(id, next.recovery.epoch.to_string());
            }
            for manifest in self.distribution.retained_manifests() {
                for shard in &manifest.coverage.shards {
                    if shard.id >= first_affected_shard {
                        next.recovery
                            .revoked
                            .insert(hex::encode(manifest.session_id(shard.id)?));
                    }
                }
            }
        }
        next.recovery
            .sealed
            .retain(|id, _| *id < first_affected_shard);
        self.save_state(&mut state, next)?;
        self.distribution.withdraw(&Revocation {
            recovery_epoch: state.durable.recovery.epoch,
            sessions: state.durable.recovery.revoked.clone(),
        })
    }
    /// Reserve preparation ownership for the host engine; no preparation runs here.
    pub(crate) fn publication(
        &self,
        work: PublicationWork,
    ) -> Result<preparation::EnhancePublication<'_>, String> {
        preparation::EnhancePublication::journal(self, work)
    }
    /// Execute through the supplied host engine, including offline integration callers.
    pub async fn publish<E: pir_pipeline::publisher::PublisherExecutor>(
        &self,
        work: PublicationWork,
        executor: E,
    ) -> Result<(), String> {
        if work.owner != self.incarnation {
            return Err("publication work belongs to another publisher".into());
        }
        let current = self.distribution.durable_current();
        if current.withdrawal_revision == work.withdrawal_revision
            && current.eligible
            && current.manifest.as_ref().is_some_and(|m| {
                m.anchor_height == work.manifest.anchor_height
                    && m.anchor_block_hash == work.manifest.anchor_block_hash
                    && m.coverage.records == work.manifest.coverage.records
            })
        {
            return Ok(());
        }
        let result = executor.publish(self.publication(work)?).await;
        self.publication_finished(result.as_ref().err().cloned());
        result
    }
    pub(crate) fn publication_finished(&self, error: Option<String>) {
        self.state.lock().unwrap().error = error;
    }
    pub fn router(&self) -> axum::Router {
        axum::Router::new()
            .route("/v1/health", axum::routing::get(http::health))
            .route("/ready", axum::routing::get(http::ready))
            .route("/metrics", axum::routing::get(http::metrics))
            .with_state(self.clone())
    }
}
#[cfg(test)]
mod tests;
