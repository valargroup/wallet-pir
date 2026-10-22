//! Prepare complete, immutable publications without reaching a serving process.
//! Only the inventory is installed last; an interrupted build leaves reusable
//! objects but never a complete inventory referring to unfinished bytes.
//!
//! Every referenced object is hashed from disk once, when preparation finishes.
//! That pass records which file each hash was read from, so publication only
//! rehashes files that were changed, replaced or truncated since.
use enhance_shared::{
    protocol::{digest, Coverage, Geometry, MAX_QUERY_SHARDS, PROTOCOL_REVISION},
    publication::{Inventory, VERSION},
};
mod object_io;
mod preparation;
mod verification;

use object_io::persist_metadata;
pub use object_io::read_metadata;
use pir_publication::ObjectInstaller;
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    path::Path,
};
pub use verification::{verify_publication, PreparedPublication, VerificationStats};

/// Produce material for validated coverage, one domain at a time. The caller
/// owns chain authority and supplies the recovery epoch of every domain.
/// This operation neither activates consumers nor grants serving authority.
pub fn prepare_publication(
    root: &Path,
    coverage: Coverage,
    epochs: &BTreeMap<u64, String>,
    mut read: impl FnMut(u64, usize) -> Result<Vec<u8>, String>,
) -> Result<PreparedPublication, String> {
    validate_source(&coverage, epochs)?;
    fs::create_dir_all(root.join("objects")).map_err(|e| e.to_string())?;
    let owner = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(root.join("prepare.lock"))
        .map_err(|e| e.to_string())?;
    owner
        .try_lock()
        .map_err(|_| "publication preparation already owned")?;
    let inventory = construct_publication(root, coverage, epochs, &mut read)?;
    let prepared = verify_publication(root, inventory)?;
    persist_inventory(root, prepared.inventory())?;
    Ok(prepared)
}

/// Construct product domains incrementally through the shared object installer.
/// The caller holds the preparation lock until verification and commit end.
pub(crate) fn construct_publication(
    root: &Path,
    coverage: Coverage,
    epochs: &BTreeMap<u64, String>,
    mut read: impl FnMut(u64, usize) -> Result<Vec<u8>, String>,
) -> Result<Inventory, String> {
    validate_source(&coverage, epochs)?;
    fs::create_dir_all(root.join("objects")).map_err(|e| e.to_string())?;
    let objects = ObjectInstaller::open(root.join("objects")).map_err(|e| e.to_string())?;
    let domains = preparation::prepare_domains(root, &objects, &coverage, epochs, &mut read)?;
    let inventory = Inventory {
        version: VERSION,
        network: "main".into(),
        profile: PROTOCOL_REVISION.into(),
        coverage,
        domains,
    };
    Ok(inventory)
}

fn validate_source(coverage: &Coverage, epochs: &BTreeMap<u64, String>) -> Result<(), String> {
    coverage.validate(Geometry::default())?;
    if coverage.shards.len() > MAX_QUERY_SHARDS as usize || epochs.len() != coverage.shards.len() {
        return Err("incomplete publication recovery identities".into());
    }
    // Check every epoch before creating large files or preparing a runtime.
    for shard in &coverage.shards {
        let text = epochs.get(&shard.id).ok_or("missing domain epoch")?;
        let epoch: u64 = text.parse().map_err(|_| "invalid domain epoch")?;
        if epoch.to_string() != *text {
            return Err("noncanonical domain epoch".into());
        }
    }
    Ok(())
}

pub(crate) fn persist_inventory(root: &Path, inventory: &Inventory) -> Result<(), String> {
    File::open(root.join("objects"))
        .and_then(|f| f.sync_all())
        .map_err(|e| e.to_string())?;
    persist_metadata(
        root,
        "inventories",
        &format!("{}.json", digest(inventory)),
        inventory,
    )
}

#[cfg(test)]
mod tests;
