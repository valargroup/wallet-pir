//! One-time adapters for predecessor Transparent distribution inventories.
//! Source manifests remain unchanged; only the surrounding artifact contract
//! and the local prepared-runtime headers are converted.
use super::*;
use pir_artifact::{catalog::Catalog, migration::sha256};
use serde_json::Value;
use std::sync::Arc;

type Runtime = (crate::runtime::RuntimeKey, Arc<SharedParams>, String);

pub fn prepare(catalog: &mut Catalog, mut value: Value, display: bool) -> Result<Vec<u8>, Error> {
    let old_schema = if display {
        "transparent-display-prepared-distribution-v1"
    } else {
        "transparent-prepared-distribution-v1"
    };
    if value["schema"] != old_schema {
        return Err("expected predecessor Transparent inventory".into());
    }
    let root = catalog.root().to_path_buf();
    let source_runtimes = ShippedRuntimes::required(root.clone());
    let target_runtimes = ShippedRuntimes::required(catalog.stage().to_path_buf());
    let mut runtimes = BTreeMap::<String, Runtime>::new();
    let mut sources = BTreeMap::<String, String>::new();
    let relative = |path: &Path| -> Result<String, Error> {
        Ok(path
            .strip_prefix(&root)?
            .to_str()
            .ok_or("non-UTF8 publication path")?
            .to_owned())
    };
    if display {
        use crate::tx_detail::{kind, runtime_key, set::DisplaySet, DisplayRole};
        let set = DisplaySet::open_for_serving(
            &root,
            crate::history::shardset::DEFAULT_RETAIN_REVISIONS,
            DisplayRole::Combined,
        )?;
        if value["map_sha256"] != set.map_digest {
            return Err("display migration map differs".into());
        }
        for revision in set.current() {
            for (table, segment) in revision.targets() {
                let source = revision
                    .segment(table, segment)
                    .ok_or("missing display segment")?;
                sources.insert(relative(source.path())?, source.sha256().into());
                let shared = Arc::new(SharedParams::build_display(revision.geometry, kind(table))?);
                let key = runtime_key(&revision.digest, table, segment);
                let old_path = source_runtimes.previous_path(&key, &shared, source.sha256());
                runtimes.insert(relative(&old_path)?, (key, shared, source.sha256().into()));
            }
        }
    } else {
        use crate::history::shardset::{LoadOptions, DEFAULT_RETAIN_REVISIONS};
        let set = ShardSet::open_for_serving(&root, &LoadOptions::whole(DEFAULT_RETAIN_REVISIONS))?;
        if value["map_sha256"] != set.map_digest {
            return Err("history migration map differs".into());
        }
        let cutoff = value["recent_from_shard"]
            .as_u64()
            .ok_or("missing recent cutoff")?;
        let height = set
            .map
            .shards
            .get(usize::try_from(cutoff)?)
            .ok_or("recent cutoff outside map")?
            .start_height;
        let full = value["artifacts"]
            .as_array()
            .ok_or("missing artifacts")?
            .iter()
            .any(|a| a["kind"] == "archive_runtime");
        let plan = super::plan(&root, &set, height, full)?;
        for (path, expected) in plan.sources {
            sources.insert(relative(&path)?, expected.into());
        }
        for (_, key, shared, source) in plan.native {
            let path = source_runtimes.previous_path(&key, &shared, source.sha256());
            runtimes.insert(relative(&path)?, (key, shared, source.sha256().into()));
        }
    }
    let artifacts = value["artifacts"]
        .as_array_mut()
        .ok_or("missing predecessor artifact list")?;
    let mut seen = BTreeSet::new();
    for artifact in artifacts {
        let path = artifact["path"]
            .as_str()
            .ok_or("missing artifact path")?
            .to_owned();
        if !safe_path(&path) || !seen.insert(path.clone()) {
            return Err("unsafe or duplicate predecessor artifact".into());
        }
        let previous = sha256(
            artifact["sha256"]
                .as_str()
                .ok_or("missing predecessor checksum")?
                .into(),
        )?;
        let bytes = artifact["bytes"]
            .as_u64()
            .ok_or("missing predecessor artifact length")?;
        let checksum = if path.ends_with(".runtime") {
            let (key, shared, source) = runtimes
                .remove(&path)
                .ok_or("predecessor runtime is not in the publication")?;
            let (target, checksum) = source_runtimes.migrate_previous(
                &target_runtimes,
                &key,
                &shared,
                &source,
                &previous,
                bytes,
                catalog.checkpoints(),
            )?;
            catalog.transformed(
                Path::new(&path),
                &previous,
                bytes,
                &target,
                checksum.clone(),
            )?;
            artifact["path"] = target
                .strip_prefix(catalog.stage())?
                .to_str()
                .ok_or("non-UTF8 runtime path")?
                .into();
            checksum
        } else {
            let converted = catalog.object(Path::new(&path), &previous, bytes)?;
            if let Some(source) = sources.get(&path) {
                if *source != previous.digest {
                    return Err("predecessor table differs from source commitment".into());
                }
                artifact["source_sha256"] = source.clone().into();
            }
            catalog.install_at(&previous, Path::new(&path))?;
            converted.checksum
        };
        let object = artifact
            .as_object_mut()
            .ok_or("artifact must be an object")?;
        object.remove("sha256");
        object.insert("checksum".into(), serde_json::to_value(checksum)?);
    }
    value["runtime"] = RUNTIME.into();
    value["schema"] = if display {
        crate::tx_detail::distribution::SCHEMA.into()
    } else {
        SCHEMA.into()
    };
    if display {
        let inventory: crate::tx_detail::distribution::Inventory = serde_json::from_value(value)?;
        inventory.validate(&inventory.network, &inventory.genesis_hash)?;
        Ok(serde_json::to_vec(&inventory)?)
    } else {
        let inventory: Inventory = serde_json::from_value(value)?;
        inventory.validate(&inventory.network, &inventory.genesis_hash)?;
        Ok(serde_json::to_vec(&inventory)?)
    }
}
