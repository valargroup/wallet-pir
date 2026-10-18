//! The durable activation transaction shared by history and display.
//!
//! Preparation, authority checks and retention policy belong to each product.
//! This boundary keeps command serialization and the reorg fence held until a
//! persisted candidate has been installed, even if its caller stops waiting.

use std::sync::Mutex;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

pub(crate) trait Adapter: Send + 'static {
    type Candidate: Send;
    type Validated;

    /// Current digest and whether the requested publication already satisfies
    /// this product's success rule. Release the serving lock before returning.
    fn active(&self, requested: &str) -> (String, bool);
    fn candidate(&self) -> &AsyncMutex<Option<Self::Candidate>>;
    fn publication_gate(&self) -> &Mutex<()>;
    fn epoch(&self) -> u64;
    fn candidate_epoch(candidate: &Self::Candidate) -> u64;
    fn digest(candidate: &Self::Candidate) -> &str;
    fn ready(candidate: &Self::Candidate) -> bool;

    /// Runs under the publication gate, before any durable state changes.
    fn validate(&self, candidate: &Self::Candidate) -> Result<Self::Validated, String>;
    fn persist(&self, candidate: &Self::Candidate) -> Result<(), String>;

    /// Installs an already-persisted candidate without further fallible work.
    /// Keep serving locks short; retirement and pin release remain product rules.
    fn install(&self, candidate: Self::Candidate, validated: Self::Validated);
}

pub(crate) async fn activate<A: Adapter>(
    expected: String,
    requested: String,
    adapter: A,
    operation: OwnedMutexGuard<()>,
) -> Result<(), String> {
    tokio::task::spawn_blocking(move || {
        // The blocking job, rather than its cancellable waiter, owns this guard.
        let _operation = operation;
        let (active, already_active) = adapter.active(&requested);
        if already_active {
            return Ok(());
        }
        if active != expected {
            return Err("active predecessor changed".into());
        }

        let mut candidate = adapter.candidate().blocking_lock();
        // Keep this order: candidate first, then the gate shared with revocation.
        let _gate = adapter.publication_gate().lock().unwrap();
        let next = candidate.as_ref().ok_or("no prepared candidate")?;
        if A::candidate_epoch(next) != adapter.epoch() {
            return Err("reorg invalidated prepared candidate".into());
        }
        if A::digest(next) != requested || !A::ready(next) {
            return Err("candidate identity/readiness mismatch".into());
        }
        let validated = adapter.validate(next)?;
        // A failure even after rename must leave the in-memory candidate intact.
        // Recovery reads the durable record; this transaction never retries it.
        adapter.persist(next)?;
        let next = candidate.take().expect("checked above");
        adapter.install(next, validated);
        Ok(())
    })
    .await
    .map_err(|e| e.to_string())?
}

#[cfg(test)]
mod tests;
