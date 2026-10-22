//! Lock the stopped owners and make each state write durable before proceeding.
use crate::durable_state::DistributionState;
use enhance_shared::artifact;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    path::Path,
};

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct HandoffReceipt {
    pub(super) version: u16,
    pub(super) transaction: String,
    pub(super) completed: bool,
    pub(super) initialized: bool,
    pub(super) previous_sha256: String,
    pub(super) rollback_epoch: Option<u64>,
}

pub(super) fn save<T: Serialize>(root: &Path, name: &str, value: &T) -> Result<(), String> {
    save_with_sync(root, name, value, |directory| {
        File::open(directory)?.sync_all()
    })
}

// Keep the rename and its durability barrier together. A failed barrier must
// stop the handoff before the next state file or completion receipt is written.
pub(super) fn save_with_sync<T: Serialize>(
    root: &Path,
    name: &str,
    value: &T,
    sync: impl FnOnce(&Path) -> std::io::Result<()>,
) -> Result<(), String> {
    artifact::write_atomic(root, name, |file| {
        serde_json::to_writer(file, value).map_err(std::io::Error::other)
    })
    .and_then(|_| sync(root))
    .map_err(|e| e.to_string())
}
pub(super) fn lock(root: &Path, name: &str) -> Result<File, String> {
    fs::create_dir_all(root).map_err(|e| e.to_string())?;
    // The data directory already exists. Persist new publication/https entries
    // in their parent before any receipt can refer to files beneath them.
    File::open(root)
        .and_then(|directory| directory.sync_all())
        .and_then(|_| {
            File::open(
                root.parent()
                    .ok_or_else(|| std::io::Error::other("state directory has no parent"))?,
            )?
            .sync_all()
        })
        .map_err(|e| e.to_string())?;
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(root.join(name))
        .map_err(|e| e.to_string())?;
    file.try_lock()
        .map_err(|_| format!("state is still owned: {name}"))?;
    Ok(file)
}

/// Reserve or resume a transfer, marking it incomplete before any state write.
pub(super) fn reserve_transfer(
    root: &Path,
    https: &Path,
    rollback: bool,
    transaction: &str,
    previous_epoch: u64,
    previous_sha256: &str,
) -> Result<HandoffReceipt, String> {
    let receipt_path = root.join("handoff.json");
    let mut receipt = if receipt_path.exists() {
        let mut receipt: HandoffReceipt = crate::publication::read_metadata(&receipt_path)?;
        if receipt.version != 1 {
            return Err("unsupported or already rolled-back handoff".into());
        }
        if receipt.initialized
            && (!root.join("publisher.json").is_file() || !https.join("current.json").is_file())
        {
            return Err(
                "initialized handoff state is missing; refusing lower recovery floors".into(),
            );
        }
        if receipt.transaction != transaction {
            if rollback
                || !receipt.completed
                || receipt
                    .rollback_epoch
                    .is_none_or(|epoch| previous_epoch < epoch)
            {
                return Err("handoff belongs to another unfinished transaction".into());
            }
            let current: DistributionState =
                crate::publication::read_metadata(&https.join("current.json"))?;
            if current.current.eligible {
                return Err("previous HTTPS runtime has not withdrawn".into());
            }
            // A new reviewed deployment after completed restoration can reuse
            // the state. Its generation, recovery and revision floors remain.
            receipt.transaction = transaction.into();
            receipt.completed = false;
            receipt.previous_sha256 = previous_sha256.to_owned();
            receipt.rollback_epoch = None;
            save(root, "handoff.json", &receipt)?;
        } else if !rollback && receipt.rollback_epoch.is_some() {
            return Err("a rolled-back transaction cannot resume forward progress".into());
        }
        receipt
    } else {
        if rollback || root.join("publisher.json").exists() || https.join("current.json").exists() {
            return Err("state exists without a handoff receipt; refusing to overwrite it".into());
        }
        let receipt = HandoffReceipt {
            version: 1,
            transaction: transaction.into(),
            completed: false,
            initialized: false,
            previous_sha256: previous_sha256.to_owned(),
            rollback_epoch: None,
        };
        save(root, "handoff.json", &receipt)?;
        receipt
    };
    if !rollback && receipt.previous_sha256 != previous_sha256 {
        return Err("previous state changed since handoff began".into());
    }
    receipt.completed = false;
    save(root, "handoff.json", &receipt)?;
    Ok(receipt)
}
