//! Reuse a consumer's predecessor cache without trusting its old receipts.
//! Only verified immutable bytes are staged; serving pointers and authority
//! records remain the deployment transaction's responsibility.
use super::*;
use pir_artifact::catalog::Prepared;

pub fn prepare(preparation: &Path, root: &Path, stage: &Path) -> crate::Result<Prepared> {
    let publisher: Prepared = serde_json::from_slice(&std::fs::read(preparation)?)?;
    if publisher.schema != "artifact-checksum-preparation/1" {
        return Err("unsupported publisher preparation".into());
    }
    let old_bytes = std::fs::read(publisher.stage.join("predecessor-inventory.json"))?;
    publisher.previous_inventory.verify(&old_bytes)?;
    let new_bytes = std::fs::read(&publisher.inventory_file)?;
    publisher.inventory.checksum.require_current()?;
    publisher.inventory.checksum.verify(&new_bytes)?;
    if new_bytes.len() as u64 != publisher.inventory.bytes {
        return Err("prepared inventory length differs".into());
    }
    let old: Value = serde_json::from_slice(&old_bytes)?;
    let new: Value = serde_json::from_slice(&new_bytes)?;
    let mut catalog = Catalog::new(root, stage)?;
    let mut inputs = Vec::new();
    for (key, expected) in &publisher.objects {
        let digest = key
            .strip_prefix("sha256-")
            .ok_or("invalid predecessor cache key")?;
        let relative = Path::new("objects").join(digest);
        match std::fs::symlink_metadata(root.join(&relative)) {
            Ok(_) => inputs.push((relative, migration::sha256(digest.into())?, expected.bytes)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    catalog.pin_inputs(&inputs)?;
    for (key, expected) in &publisher.objects {
        let digest = key
            .strip_prefix("sha256-")
            .ok_or("invalid predecessor cache key")?;
        let previous = migration::sha256(digest.into())?;
        let relative = Path::new("objects").join(digest);
        let source = catalog.source_root().join(&relative);
        match std::fs::symlink_metadata(&source) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
            Ok(_) => {}
        }
        let target_relative =
            Path::new(&pir_artifact::namespace("objects")).join(&expected.checksum.digest);
        let checksum = serde_json::to_value(&expected.checksum)?;
        // Inventories list artifacts in path order, and a runtime's path
        // changes with its identity, so the two lists do not align by
        // position. Pair each runtime by its verified identity in each format.
        let runtime = match listed(&old).find(|a| a["sha256"].as_str() == Some(digest)) {
            Some(old) => Some((
                old,
                listed(&new)
                    .find(|a| a["checksum"] == checksum)
                    .ok_or("converted runtime is not in the prepared inventory")?,
            )),
            None => None,
        };
        if let Some((old, new)) = runtime {
            #[cfg(any(feature = "transparent-history", feature = "transparent-detail"))]
            {
                let target = stage.join(&target_relative);
                fn identity(value: &Value) -> crate::Result<[u8; 32]> {
                    let name = Path::new(text(value, "path")?)
                        .file_stem()
                        .and_then(|v| v.to_str())
                        .ok_or("invalid runtime filename")?;
                    let (_, identity) = name.rsplit_once('-').ok_or("missing runtime identity")?;
                    if identity.len() != 64 || !identity.is_ascii() {
                        return Err("invalid runtime identity length".into());
                    }
                    let mut bytes = [0; 32];
                    for (index, byte) in bytes.iter_mut().enumerate() {
                        *byte = u8::from_str_radix(&identity[index * 2..index * 2 + 2], 16)?;
                    }
                    Ok(bytes)
                }
                let (_, checksum) =
                    transparent_shared::runtime::disk::ShippedRuntimes::migrate_file(
                        &source,
                        &target,
                        identity(old)?,
                        identity(new)?,
                        &previous,
                        expected.bytes,
                        catalog.checkpoints(),
                    )?;
                if checksum != expected.checksum {
                    return Err("converted cache differs from publisher artifact".into());
                }
                catalog.transformed(&relative, &previous, expected.bytes, &target, checksum)?;
            }
            #[cfg(not(any(feature = "transparent-history", feature = "transparent-detail")))]
            {
                let _ = (old, new);
                return Err("Transparent cache migration is not compiled".into());
            }
        } else {
            let actual = catalog.object(&relative, &previous, expected.bytes)?;
            if actual.checksum != expected.checksum {
                return Err("cached artifact differs from publisher translation".into());
            }
        }
        if publisher.product.starts_with("transparent-") {
            let source_sha256 = new["artifacts"]
                .as_array()
                .and_then(|artifacts| artifacts.iter().find(|a| a["checksum"] == checksum))
                .and_then(|a| a.get("source_sha256"))
                .cloned();
            let mut receipt = serde_json::json!({"version":3,"checksum":expected.checksum,
                "bytes":expected.bytes,"object":root.join(&target_relative)});
            if let Some(source) = source_sha256 {
                receipt["source_sha256"] = source;
            }
            let marker = target_relative
                .with_file_name(format!("{}.received.json", expected.checksum.digest));
            catalog.metadata_at(&marker, &serde_json::to_vec(&receipt)?)?;
        }
    }
    catalog
        .finish(&publisher.product, publisher.previous_inventory, &new_bytes)
        .map_err(Into::into)
}

/// The runtime artifacts an inventory lists, by their header-bearing path.
fn listed(inventory: &Value) -> impl Iterator<Item = &Value> {
    inventory
        .get("artifacts")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|a| {
            a["path"]
                .as_str()
                .is_some_and(|path| path.ends_with(".runtime"))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cached_bytes_are_verified_against_both_inventories_before_installation() {
        let directory = tempfile::tempdir().unwrap();
        let publisher_root = directory.path().join("publisher");
        let publisher_stage = directory.path().join("publisher-stage");
        let cache_root = directory.path().join("consumer");
        let cache_stage = directory.path().join("consumer-stage");
        for root in [&publisher_root, &cache_root] {
            std::fs::create_dir_all(root.join("objects")).unwrap();
        }
        let bytes = b"unchanged artifact";
        let mut old = pir_artifact::Hasher::for_algorithm(pir_artifact::Algorithm::Sha256);
        old.update(bytes);
        let old = old.finish();
        let relative = Path::new("objects").join(&old.digest);
        std::fs::write(publisher_root.join(&relative), bytes).unwrap();
        std::fs::write(cache_root.join(&relative), bytes).unwrap();
        let mut catalog = Catalog::new(&publisher_root, &publisher_stage).unwrap();
        catalog.object(&relative, &old, bytes.len() as u64).unwrap();
        let predecessor = b"{}";
        let mut hash = pir_artifact::Hasher::for_algorithm(pir_artifact::Algorithm::Sha256);
        hash.update(predecessor);
        migration::persist(
            &publisher_stage.join("predecessor-inventory.json"),
            predecessor,
        )
        .unwrap();
        catalog.finish("status", hash.finish(), b"{}").unwrap();
        let input = publisher_stage.join("prepared.json");
        let mut prepared = prepare(&input, &cache_root, &cache_stage).unwrap();
        prepared.install(&cache_root).unwrap();
        let new = cache_root
            .join(pir_artifact::namespace("objects"))
            .join(pir_artifact::digest(bytes));
        assert_eq!(std::fs::read(new).unwrap(), bytes);
        std::fs::write(cache_root.join(relative), b"corrupted artifact").unwrap();
        assert!(prepare(&input, &cache_root, &cache_stage).is_err());
    }

    /// Runtime names derive from their identities, so the predecessor and
    /// prepared inventories can list the same runtimes in different orders.
    #[cfg(any(feature = "transparent-history", feature = "transparent-detail"))]
    #[test]
    fn cached_runtimes_pair_by_identity_when_renamed_paths_reorder() {
        use transparent_shared::runtime::disk::ShippedRuntimes;
        let directory = tempfile::tempdir().unwrap();
        let publisher_root = directory.path().join("publisher");
        let publisher_stage = directory.path().join("publisher-stage");
        let cache_root = directory.path().join("consumer");
        let cache_stage = directory.path().join("consumer-stage");
        for root in [&publisher_root, &cache_root] {
            std::fs::create_dir_all(root.join("objects")).unwrap();
            std::fs::create_dir_all(root.join("runtimes")).unwrap();
        }
        let sha256 = |bytes: &[u8]| {
            let mut hash = pir_artifact::Hasher::for_algorithm(pir_artifact::Algorithm::Sha256);
            hash.update(bytes);
            hash.finish()
        };
        let name = |identity: [u8; 32]| format!("runtimes/key-{}.runtime", hex(&identity));
        let mut catalog = Catalog::new(&publisher_root, &publisher_stage).unwrap();
        let (mut old_list, mut new_list) = (Vec::new(), Vec::new());
        // A sorts before B in the predecessor names and after it once renamed.
        for (old_identity, new_identity, body) in [
            ([0x01; 32], [0xf0; 32], b"runtime body A".as_slice()),
            ([0x02; 32], [0x0f; 32], b"runtime body B, longer".as_slice()),
        ] {
            let mut file = old_identity.to_vec();
            let mut header = pir_artifact::Hasher::for_algorithm(pir_artifact::Algorithm::Sha256);
            header.update(body);
            file.extend_from_slice(&header.finalize());
            file.extend_from_slice(body);
            let previous = sha256(&file);
            let relative = PathBuf::from(name(old_identity));
            std::fs::write(publisher_root.join(&relative), &file).unwrap();
            std::fs::write(cache_root.join("objects").join(&previous.digest), &file).unwrap();
            let target = publisher_stage.join(name(new_identity));
            let bytes = file.len() as u64;
            let (_, checksum) = ShippedRuntimes::migrate_file(
                &publisher_root.join(&relative),
                &target,
                old_identity,
                new_identity,
                &previous,
                bytes,
                catalog.checkpoints(),
            )
            .unwrap();
            catalog
                .transformed(&relative, &previous, bytes, &target, checksum.clone())
                .unwrap();
            old_list.push(serde_json::json!({"path":name(old_identity),
                "sha256":previous.digest,"bytes":bytes}));
            new_list.push(serde_json::json!({"path":name(new_identity),
                "checksum":checksum,"bytes":bytes}));
        }
        new_list.sort_by_key(|a| a["path"].as_str().unwrap().to_owned());
        assert_ne!(old_list[0]["bytes"], new_list[0]["bytes"]);
        let predecessor = serde_json::to_vec(&serde_json::json!({"artifacts":old_list})).unwrap();
        migration::persist(
            &publisher_stage.join("predecessor-inventory.json"),
            &predecessor,
        )
        .unwrap();
        let inventory = serde_json::to_vec(&serde_json::json!({"artifacts":new_list})).unwrap();
        catalog
            .finish("transparent-history", sha256(&predecessor), &inventory)
            .unwrap();
        let input = publisher_stage.join("prepared.json");
        let mut prepared = prepare(&input, &cache_root, &cache_stage).unwrap();
        prepared.verify_files().unwrap();
        prepared.install(&cache_root).unwrap();
        for artifact in &new_list {
            let checksum: pir_artifact::Checksum =
                serde_json::from_value(artifact["checksum"].clone()).unwrap();
            let path = cache_root
                .join(pir_artifact::namespace("objects"))
                .join(&checksum.digest);
            checksum.verify(&std::fs::read(path).unwrap()).unwrap();
        }
    }

    #[cfg(any(feature = "transparent-history", feature = "transparent-detail"))]
    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}
