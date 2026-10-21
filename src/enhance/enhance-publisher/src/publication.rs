//! Prepare complete, immutable publications without reaching a serving process.
//! Only the inventory is installed last; an interrupted build leaves reusable
//! objects but never a complete inventory referring to unfinished bytes.
use crate::{artifact, prepared_packing, runtime, PackingBudget};
use enhance_query_server::{
    protocol::{digest, Coverage, Geometry, MAX_QUERY_SHARDS, PROTOCOL_REVISION},
    publication::{Domain, Inventory, Object, METADATA_LIMIT, VERSION},
};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{self, Read},
    path::Path,
};

/// Produce material for validated coverage, one domain at a time. The caller
/// owns chain authority and supplies the recovery epoch of every domain.
/// This operation neither activates consumers nor grants serving authority.
pub fn prepare(
    root: &Path,
    coverage: Coverage,
    epochs: &BTreeMap<u64, String>,
    mut read: impl FnMut(u64, usize) -> Result<Vec<u8>, String>,
) -> Result<Inventory, String> {
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
    let mut engine = runtime::Engine::new(&root.join("preparation"));
    let mut domains = Vec::new();
    for shard in &coverage.shards {
        let mut plan = runtime::plan(shard.clone(), &mut read)?;
        for unit in &mut plan.units {
            unit.recovery_epoch = epochs[&shard.id]
                .parse()
                .map_err(|_| "invalid domain epoch")?;
        }
        let mut rows = Vec::new();
        for (spec, unit) in plan.shard.units.iter().zip(&plan.units) {
            let bytes = runtime::unit_rows(&plan.shard, spec, &mut read)?;
            if hex::encode(Sha256::digest(&bytes)) != unit.content_sha256 {
                return Err("canonical rows changed during publication preparation".into());
            }
            rows.push(install(root, &bytes)?);
        }
        // A previously verified domain avoids hint and packing preparation.
        let key = digest(&plan);
        let retained = root.join("domains").join(format!("{key}.json"));
        if retained.exists() {
            let domain: Domain = read_metadata(&retained)?;
            if domain.plan != plan || domain.rows != rows {
                return Err("retained publication domain identity differs".into());
            }
            // Full structural validation happens on the complete inventory below.
            for object in domain.rows.iter().chain([&domain.hint, &domain.public]) {
                object.verify(root)?;
            }
            domain.packing_object().verify(root)?;
            domains.push(domain);
            continue;
        }
        let (evaluation, _) = engine
            .prepare(plan.clone(), &mut read)
            .map_err(|e| e.to_string())?;
        let hint = evaluation.hint()?;
        let packing =
            runtime::Packing::new(shard.logical_rows, &hint, &PackingBudget::coordinator())?;
        let prepared = prepared_packing::persist(root, &packing)?;
        install_file(
            root,
            &root.join(prepared_packing::DIRECTORY).join(&prepared.name),
            &prepared,
        )?;
        let mut encoded = Vec::new();
        crate::wire::write_crs_blocks(&mut encoded, &hint).map_err(|e| e.to_string())?;
        let domain = Domain {
            plan,
            rows,
            hint: install(root, &encoded)?,
            public: install(root, &packing.public)?,
            packing: prepared,
        };
        persist_metadata(root, "domains", &format!("{key}.json"), &domain)?;
        domains.push(domain);
    }
    let inventory = Inventory {
        version: VERSION,
        network: "main".into(),
        profile: PROTOCOL_REVISION.into(),
        coverage,
        domains,
    };
    inventory.verify(root)?;
    File::open(root.join("objects"))
        .and_then(|f| f.sync_all())
        .map_err(|e| e.to_string())?;
    persist_metadata(
        root,
        "inventories",
        &format!("{}.json", digest(&inventory)),
        &inventory,
    )?;
    Ok(inventory)
}

pub fn read_metadata<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, String> {
    let file = File::open(path).map_err(|e| e.to_string())?;
    if file.metadata().map_err(|e| e.to_string())?.len() > METADATA_LIMIT {
        return Err("publication metadata exceeds limit".into());
    }
    let mut bytes = Vec::new();
    file.take(METADATA_LIMIT + 1)
        .read_to_end(&mut bytes)
        .map_err(|e| e.to_string())?;
    if bytes.len() as u64 > METADATA_LIMIT {
        return Err("publication metadata exceeds limit".into());
    }
    serde_json::from_slice(&bytes).map_err(|e| e.to_string())
}

fn persist_metadata(
    root: &Path,
    directory: &str,
    name: &str,
    value: &impl serde::Serialize,
) -> Result<(), String> {
    let bytes = serde_json::to_vec(value).map_err(|e| e.to_string())?;
    if bytes.len() as u64 > METADATA_LIMIT {
        return Err("publication metadata exceeds limit".into());
    }
    let directory = root.join(directory);
    fs::create_dir_all(&directory).map_err(|e| e.to_string())?;
    artifact::write_atomic(&directory, name, |f| f.write_all(&bytes)).map_err(|e| e.to_string())?;
    File::open(directory)
        .and_then(|f| f.sync_all())
        .and_then(|_| File::open(root)?.sync_all())
        .map_err(|e| e.to_string())
}

fn install(root: &Path, bytes: &[u8]) -> Result<Object, String> {
    let object = Object {
        sha256: hex::encode(Sha256::digest(bytes)),
        bytes: bytes.len() as u64,
    };
    let path = object.path(root)?;
    if path.exists() {
        object.verify(root)?;
    } else {
        artifact::write_atomic_cold(&root.join("objects"), &object.sha256, |f| {
            f.write_all(bytes)
        })
        .map_err(|e| e.to_string())?;
    }
    Ok(object)
}

fn install_file(
    root: &Path,
    source: &Path,
    prepared: &prepared_packing::Artifact,
) -> Result<(), String> {
    let object = Object {
        sha256: prepared.sha256.clone(),
        bytes: prepared.bytes,
    };
    let path = object.path(root)?;
    if path.exists() {
        return object.verify(root);
    }
    artifact::write_atomic_cold(&root.join("objects"), &object.sha256, |f| {
        io::copy(&mut File::open(source)?, f)?;
        Ok(())
    })
    .map_err(|e| e.to_string())?;
    object.verify(root)
}

#[cfg(test)]
mod tests {
    use super::*;
    use enhance_query_server::protocol::Lifecycle;

    #[test]
    fn invalid_geometry_and_recovery_are_rejected_before_reading_records() {
        let scratch = tempfile::tempdir().unwrap();
        let root = scratch.path().join("publication");
        let coverage = Lifecycle::default()
            .coverage(1, Geometry::default())
            .unwrap();
        for epochs in [BTreeMap::new(), [(0, "01".into())].into()] {
            assert!(prepare(&root, coverage.clone(), &epochs, |_, _| {
                panic!("invalid metadata must not read records")
            })
            .is_err());
            assert!(!root.exists());
        }
        let mut malformed = coverage;
        malformed.shards[0].logical_rows = u64::MAX;
        assert!(
            prepare(&root, malformed, &[(0, "0".into())].into(), |_, _| {
                panic!("invalid geometry must not read records")
            })
            .is_err()
        );
        assert!(!root.exists());
    }

    #[test]
    fn preparation_has_one_writer() {
        let root = tempfile::tempdir().unwrap();
        let owner = File::create(root.path().join("prepare.lock")).unwrap();
        owner.lock().unwrap();
        let coverage = Lifecycle::default()
            .coverage(1, Geometry::default())
            .unwrap();
        assert!(
            prepare(root.path(), coverage, &[(0, "0".into())].into(), |_, _| {
                panic!("a second owner must not read records")
            })
            .is_err()
        );
    }
}
