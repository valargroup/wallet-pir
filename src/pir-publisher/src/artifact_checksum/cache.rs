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
    for (key, expected) in &publisher.objects {
        let digest = key
            .strip_prefix("sha256-")
            .ok_or("invalid predecessor cache key")?;
        let previous = migration::sha256(digest.into())?;
        let relative = Path::new("objects").join(digest);
        let source = root.join(&relative);
        match std::fs::symlink_metadata(&source) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
            Ok(_) => {}
        }
        let target_relative =
            Path::new(&pir_artifact::namespace("objects")).join(&expected.checksum.digest);
        let runtime = old
            .get("artifacts")
            .and_then(Value::as_array)
            .zip(new.get("artifacts").and_then(Value::as_array))
            .and_then(|(old, new)| {
                old.iter().zip(new).find(|(old, _)| {
                    old["sha256"].as_str() == Some(digest)
                        && old["path"]
                            .as_str()
                            .is_some_and(|path| path.ends_with(".runtime"))
                })
            });
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
                .and_then(|artifacts| {
                    artifacts.iter().find(|a| {
                        a["checksum"]
                            == serde_json::to_value(&expected.checksum).unwrap_or(Value::Null)
                    })
                })
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
}
