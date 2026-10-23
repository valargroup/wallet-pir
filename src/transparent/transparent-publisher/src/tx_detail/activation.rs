//! Crash-recoverable advancement of display restart state and HTTP authority.
//! Intent retains the complete successor until both records are durable. Recovery
//! runs under the root lock before collection; it grants no serving authority.
use super::publisher::{read_map, ActiveRecord};
use crate::{storage::records::write_durable, BoxError};
use serde::{Deserialize, Serialize};
use std::path::Path;
use transparent_shared::publication::Current;

const INTENT: &str = "activation.json";
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ActivationIntent {
    publication: ActiveRecord,
    current: Current,
}
impl ActivationIntent {
    fn validate(&self, root: &Path) -> Result<(), BoxError> {
        if self.publication.directory.parent() != Some(root)
            || self.publication.tip != self.current.anchor_height
            || self.publication.tip_hash != self.current.anchor_hash
        {
            return Err("display activation intent disagrees with its source binding".into());
        }
        let map = read_map(&self.publication.directory, &self.publication.map_sha256)?;
        let terminal = map.shards.last().ok_or("empty activation map")?;
        if map.genesis_hash != self.current.genesis_hash
            || map.network != self.current.network
            || terminal.end_height != self.publication.tip
            || terminal.terminal_block_hash != self.publication.tip_hash
        {
            return Err("display activation map disagrees with its source binding".into());
        }
        Ok(())
    }
    fn finish(&self, root: &Path) -> Result<(), BoxError> {
        self.validate(root)?;
        self.publication.store(root)?;
        write_durable(
            &root.join("http-current.json"),
            &serde_json::to_vec(&self.current)?,
        )?;
        std::fs::remove_file(root.join(INTENT))?;
        std::fs::File::open(root)?.sync_all()?;
        Ok(())
    }
}

/// Persist a successor completely before its in-memory serving view changes.
/// Failure leaves intent for recovery and must stop further activation attempts.
pub(super) fn commit(
    root: &Path,
    publication: &ActiveRecord,
    current: &Current,
) -> Result<(), BoxError> {
    let root = &std::fs::canonicalize(root)?;
    let intent = ActivationIntent {
        publication: publication.clone(),
        current: current.clone(),
    };
    intent.validate(root)?;
    write_durable(&root.join(INTENT), &serde_json::to_vec(&intent)?)?;
    intent.finish(root)
}

/// The candidate retained by durable intent, used by collection before recovery.
pub(super) fn pending(root: &Path) -> Result<Option<std::path::PathBuf>, BoxError> {
    let root = &std::fs::canonicalize(root)?;
    match std::fs::read(root.join(INTENT)) {
        Ok(raw) => {
            let intent: ActivationIntent = serde_json::from_slice(&raw)?;
            intent.validate(root)?;
            Ok(Some(intent.publication.directory))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

/// Complete an interrupted durable transaction before opening the saved view.
/// Fresh preparation and chain checks are still required before HTTP serving.
pub(super) fn recover(root: &Path) -> Result<(), BoxError> {
    let root = &std::fs::canonicalize(root)?;
    match std::fs::read(root.join(INTENT)) {
        Ok(raw) => serde_json::from_slice::<ActivationIntent>(&raw)?.finish(root),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[cfg(all(test, feature = "fixtures"))]
mod tests {
    use super::*;
    use crate::tx_detail::{fixture, publisher};
    use transparent_shared::{journal::events::EventStore, publication::Eligibility};

    fn root() -> (tempfile::TempDir, ActiveRecord, Current) {
        let temp = tempfile::tempdir().unwrap();
        let journal = temp.path().join("journal");
        fixture::write_journal(&journal, 100, 103, 0);
        let store = EventStore::open_existing(&journal).unwrap();
        let layout = fixture::layout(&store, 101, 4);
        publisher::bootstrap(&store, temp.path(), &layout, 103).unwrap();
        let mut publication = ActiveRecord::load(temp.path()).unwrap();
        publication.cycle += 1;
        let current = Current {
            schema: transparent_shared::tx_detail::distribution::SCHEMA.into(),
            revision: 2,
            recovery_epoch: 0,
            inventory_sha256: "ab".repeat(32),
            network: layout.network,
            genesis_hash: layout.genesis_hash,
            anchor_height: publication.tip,
            anchor_hash: publication.tip_hash.clone(),
            eligibility: Eligibility::Canonical,
            invalidated_from: None,
        };
        (temp, publication, current)
    }

    #[test]
    fn failed_restart_record_keeps_recoverable_intent() {
        let (temp, publication, current) = root();
        std::fs::remove_file(temp.path().join("active.json")).unwrap();
        std::fs::create_dir(temp.path().join("active.json")).unwrap();
        assert!(commit(temp.path(), &publication, &current).is_err());
        assert_eq!(
            pending(temp.path()).unwrap(),
            Some(publication.directory.clone())
        );
        std::fs::remove_dir(temp.path().join("active.json")).unwrap();
        recover(temp.path()).unwrap();
        assert_eq!(
            ActiveRecord::load(temp.path()).unwrap().cycle,
            publication.cycle
        );
        let saved: Current =
            serde_json::from_slice(&std::fs::read(temp.path().join("http-current.json")).unwrap())
                .unwrap();
        assert_eq!(saved, current);
        assert!(!temp.path().join(INTENT).exists());
    }

    #[test]
    fn failed_http_record_recovers_the_same_successor() {
        let (temp, publication, current) = root();
        std::fs::create_dir(temp.path().join("http-current.json")).unwrap();
        assert!(commit(temp.path(), &publication, &current).is_err());
        assert_eq!(
            ActiveRecord::load(temp.path()).unwrap().cycle,
            publication.cycle
        );
        std::fs::remove_dir(temp.path().join("http-current.json")).unwrap();
        recover(temp.path()).unwrap();
        assert_eq!(pending(temp.path()).unwrap(), None);
        assert_eq!(
            ActiveRecord::load(temp.path()).unwrap().cycle,
            publication.cycle
        );
    }

    #[test]
    fn intent_recovery_refuses_another_chain_binding_and_collection_refuses_bad_intent() {
        let (temp, publication, mut current) = root();
        current.anchor_hash = "ff".repeat(32);
        let intent = ActivationIntent {
            publication,
            current,
        };
        write_durable(
            &temp.path().join(INTENT),
            &serde_json::to_vec(&intent).unwrap(),
        )
        .unwrap();
        assert!(recover(temp.path()).is_err());
        assert!(publisher::collect(temp.path(), &Default::default(), 0).is_err());
    }
}
