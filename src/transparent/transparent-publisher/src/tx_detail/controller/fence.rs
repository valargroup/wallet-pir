//! The rollback fence shared by the display journal's writer and its controller.
//!
//! The root-scheduled writer runs beside the controller, so the sealed floor,
//! the active map and the durable rollback record live here behind one lock.
//! A writer refuses a rollback below the floor, and otherwise withdraws HTTP
//! authority and records the rollback in `invalidate.json` before any
//! committed block leaves the journal.
use super::*;

#[derive(Default)]
struct FenceState {
    floor: u64,
    map_sha256: String,
    rollback: Option<RollbackRecord>,
    saved: Invalidations,
    /// A refused rollback: the controller halts on it.
    sealed: Option<(i128, u64)>,
}

pub struct RollbackFence {
    root: PathBuf,
    distribution: Option<super::super::distribution::Distribution>,
    state: std::sync::Mutex<FenceState>,
    journal: tokio::sync::Mutex<()>,
}

impl RollbackFence {
    pub(super) fn new(
        root: &Path,
        distribution: Option<super::super::distribution::Distribution>,
        saved: Invalidations,
        map_sha256: &str,
        floor: u64,
    ) -> Self {
        Self {
            root: root.to_path_buf(),
            distribution,
            journal: tokio::sync::Mutex::new(()),
            state: std::sync::Mutex::new(FenceState {
                floor,
                map_sha256: map_sha256.into(),
                rollback: saved.rollback.clone(),
                saved,
                sealed: None,
            }),
        }
    }

    /// A fence with no distribution or recorded rollback, for writer tests.
    #[cfg(test)]
    pub(crate) fn detached(root: &Path, floor: u64) -> Self {
        Self::new(root, None, Invalidations::default(), "", floor)
    }

    /// The highest height a rollback may not reach below.
    pub fn floor(&self) -> u64 {
        self.state.lock().unwrap().floor
    }

    /// Exclude journal rollback while the controller refreshes its committed
    /// view and plans archives, or commits publication and its rollback state.
    /// A writer takes this same lock through its final floor check and rewind.
    pub(crate) async fn journal(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.journal.lock().await
    }

    pub(super) fn set_floor(&self, floor: u64) {
        self.state.lock().unwrap().floor = floor;
    }

    /// Refuse a rollback to `ancestor` (`None` empties the journal) below the
    /// sealed floor, remembering it for the controller's durable halt.
    pub fn check(&self, ancestor: Option<u64>) -> Result<(), (i128, u64)> {
        let mut state = self.state.lock().unwrap();
        let ancestor = ancestor.map_or(-1, i128::from);
        if ancestor < i128::from(state.floor) {
            let refused = (ancestor, state.floor);
            state.sealed.get_or_insert(refused);
            return Err(refused);
        }
        Ok(())
    }

    /// A rollback a writer refused, if any.
    pub fn sealed(&self) -> Option<(i128, u64)> {
        self.state.lock().unwrap().sealed
    }

    /// Withdraw HTTP authority from `ancestor + 1`, then durably record the
    /// rollback under the active map. An error leaves the journal untouched.
    pub fn before_rollback(&self, ancestor: u64, hash: BlockHash) -> Result<(), String> {
        let mut state = self.state.lock().unwrap();
        if let Some(distribution) = &self.distribution {
            distribution
                .withdraw(ancestor + 1)
                .map_err(|e| e.to_string())?;
        }
        Self::note(&mut state, ancestor, hash);
        let next = Invalidations {
            rollback: state.rollback.clone(),
        };
        next.store(&self.root)
            .map_err(|e| format!("recording the invalidation from {}: {e}", ancestor + 1))?;
        state.saved = next;
        Ok(())
    }

    /// Note a rollback a source learned of after the journal changed.
    pub(super) fn noted(&self, ancestor: u64, hash: BlockHash) {
        Self::note(&mut self.state.lock().unwrap(), ancestor, hash);
    }

    fn note(state: &mut FenceState, ancestor: u64, hash: BlockHash) {
        let map_sha256 = state.map_sha256.clone();
        note_rollback(&mut state.rollback, &map_sha256, ancestor, hash);
    }

    /// Mirror the rollback record into `invalidate.json` when it changed.
    pub(super) fn save(&self) -> Result<(), BoxError> {
        let mut state = self.state.lock().unwrap();
        let next = Invalidations {
            rollback: state.rollback.clone(),
        };
        if next != state.saved {
            next.store(&self.root)?;
            state.saved = next;
        }
        Ok(())
    }

    /// Forget the rollback record before a candidate may be published.
    pub(super) fn clear(&self) -> Result<(), BoxError> {
        let mut state = self.state.lock().unwrap();
        if state.rollback.is_some() {
            let next = Invalidations { rollback: None };
            next.store(&self.root)?;
            state.saved = next;
            state.rollback = None;
        }
        Ok(())
    }

    /// Later rollbacks are recorded under the newly active map.
    pub(super) fn activated(&self, map_sha256: &str) {
        self.state.lock().unwrap().map_sha256 = map_sha256.into();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tx_detail::ingestion::{DetailIngestion, IngestError};
    use pir_pipeline::ingestion::{BlockId, Ingestion};
    use std::{future::Future, task::Poll};

    fn hash(height: u64) -> BlockHash {
        let mut bytes = [0; 32];
        bytes[..8].copy_from_slice(&height.to_le_bytes());
        BlockHash::from_internal_bytes(bytes)
    }

    fn writer(root: &Path) -> (DetailIngestion, Arc<RollbackFence>) {
        let cookie = root.join("cookie");
        std::fs::write(&cookie, "fixture:fixture").unwrap();
        let rpc = crate::ingest::zakura::connect("http://127.0.0.1:9", &cookie).unwrap();
        let mut store = EventStore::open(root.join("journal"), &"aa".repeat(32), 100).unwrap();
        for height in 100..=102 {
            store.append_block(height, hash(height), &[]).unwrap();
        }
        store.commit().unwrap();
        let fence = Arc::new(RollbackFence::detached(root, 100));
        let (notify, _) = tokio::sync::watch::channel(0);
        (
            DetailIngestion::new(rpc, store, fence.clone(), Arc::default(), notify),
            fence,
        )
    }

    fn committed_files(root: &Path) -> Vec<Vec<u8>> {
        ["events.bin", "blocks.bin", "checkpoint.bin"]
            .into_iter()
            .map(|name| std::fs::read(root.join("journal").join(name)).unwrap())
            .collect()
    }

    #[tokio::test]
    async fn an_archive_planned_after_withdrawal_prevents_journal_rewind() {
        let root = tempfile::tempdir().unwrap();
        let (mut writer, fence) = writer(root.path());
        let keep = BlockId {
            height: 101,
            hash: hash(101).to_display_hex(),
        };
        let before = committed_files(root.path());
        writer.withdraw_above(Some(&keep)).await.unwrap();
        {
            // The controller refreshes and plans an archive after withdrawal
            // but before the writer has changed any committed block.
            let _journal = fence.journal().await;
            fence.set_floor(102);
        }
        assert!(matches!(
            writer.rewind(Some(&keep)).await,
            Err(IngestError::SealedReorg(101, 102))
        ));
        assert_eq!(fence.sealed(), Some((101, 102)));
        assert_eq!(writer.committed().unwrap().height, 102);
        assert_eq!(committed_files(root.path()), before);
    }

    #[tokio::test]
    async fn journal_rewind_waits_until_archive_planning_releases_its_view() {
        let root = tempfile::tempdir().unwrap();
        let (mut writer, fence) = writer(root.path());
        let keep = BlockId {
            height: 101,
            hash: hash(101).to_display_hex(),
        };
        writer.withdraw_above(Some(&keep)).await.unwrap();
        let before = committed_files(root.path());
        let planning = fence.journal().await;
        {
            let rewind = writer.rewind(Some(&keep));
            tokio::pin!(rewind);
            assert!(
                std::future::poll_fn(|cx| Poll::Ready(rewind.as_mut().poll(cx).is_pending())).await,
                "rollback waits for the controller's committed view"
            );
            assert_eq!(committed_files(root.path()), before);
            drop(planning);
            rewind.await.unwrap();
        }
        assert_eq!(writer.committed().unwrap().height, 101);
        assert_eq!(fence.sealed(), None);
    }
}
