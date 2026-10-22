//! Retained inventories own immutable files; collection runs after durable commit.
use super::PublicationDistribution;
use crate::publication::read_metadata;
use enhance_query_server::{
    protocol::canonical_hash,
    publication::{Inventory, Object},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    path::Path,
};

impl PublicationDistribution {
    pub(super) fn retained_objects(&self) -> Result<BTreeMap<String, Object>, String> {
        let retained = self.disk.lock().unwrap().retained.clone();
        let mut objects = BTreeMap::new();
        for retained in retained {
            let inventory: Inventory = read_metadata(
                &self
                    .root
                    .join(pir_artifact::namespace("inventories"))
                    .join(format!("{}.json", retained.inventory.checksum)),
            )?;
            if enhance_query_server::publication::inventory_digest(&inventory)
                != retained.inventory.checksum
                || !inventory.supports(&retained.manifest)?
            {
                return Err("retained Enhance inventory differs".into());
            }
            for domain in inventory.domains {
                let packing = domain.packing_object();
                for object in domain
                    .rows
                    .into_iter()
                    .chain([domain.hint, domain.public, packing])
                {
                    objects.insert(object.checksum.clone(), object);
                }
            }
        }
        Ok(objects)
    }

    /// Called with the preparation lock after the current pointer is durable.
    /// An open HTTP stream pins its inode across unlink. A bootstrap that has
    /// not opened a retired object reconciles current and retries instead.
    pub(super) fn collect_retired(&self) -> Result<(), String> {
        let retained = self.disk.lock().unwrap().retained.clone();
        let mut inventories = BTreeSet::new();
        let mut domains = BTreeSet::new();
        let mut units = BTreeSet::new();
        let mut packing = BTreeSet::new();
        let mut objects = BTreeSet::new();
        for retained in retained {
            let name = format!("{}.json", retained.inventory.checksum);
            let inventory: Inventory = read_metadata(
                &self
                    .root
                    .join(pir_artifact::namespace("inventories"))
                    .join(&name),
            )?;
            if enhance_query_server::publication::inventory_digest(&inventory)
                != retained.inventory.checksum
                || !inventory.supports(&retained.manifest)?
            {
                return Err("cannot collect: retained Enhance inventory differs".into());
            }
            inventories.insert(name);
            for domain in inventory.domains {
                domains.insert(format!(
                    "{}.json",
                    enhance_query_server::publication::inventory_digest(&domain.plan)
                ));
                units.extend(domain.plan.units.iter().map(|unit| unit.digest()));
                packing.insert(domain.packing.name.clone());
                packing.insert(format!("{}.json", domain.packing.name));
                objects.insert(domain.packing.checksum);
                objects.extend(
                    domain
                        .rows
                        .iter()
                        .chain([&domain.hint, &domain.public])
                        .map(|o| o.checksum.clone()),
                );
            }
        }
        for (directory, keep) in [
            (&*pir_artifact::namespace("objects"), objects),
            (&*pir_artifact::namespace("inventories"), inventories),
            ("domains", domains),
            (enhance_query_server::prepared_packing::DIRECTORY, packing),
        ] {
            collect_files(&self.root.join(directory), &keep)?;
        }
        let directory = self.root.join("preparation/artifacts-10");
        if directory.is_dir() {
            for entry in fs::read_dir(&directory).map_err(|e| e.to_string())? {
                let entry = entry.map_err(|e| e.to_string())?;
                if entry.file_type().map_err(|e| e.to_string())?.is_dir()
                    && entry
                        .file_name()
                        .to_str()
                        .is_some_and(|name| canonical_hash(name) && !units.contains(name))
                {
                    fs::remove_dir_all(entry.path()).map_err(|e| e.to_string())?;
                }
            }
            File::open(directory)
                .and_then(|f| f.sync_all())
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }
}
pub(super) fn collect_files(directory: &Path, keep: &BTreeSet<String>) -> Result<(), String> {
    if !directory.is_dir() {
        return Ok(());
    }
    for entry in fs::read_dir(directory).map_err(|e| e.to_string())? {
        let entry = entry.map_err(|e| e.to_string())?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let committed_name = name.strip_suffix(".tmp").unwrap_or(name);
        let owned = canonical_hash(committed_name)
            || committed_name
                .strip_suffix(".json")
                .is_some_and(canonical_hash)
            || committed_name
                .strip_suffix(".bin")
                .is_some_and(canonical_hash)
            || committed_name
                .strip_suffix(".bin.json")
                .is_some_and(canonical_hash);
        if owned && !keep.contains(name) && entry.file_type().map_err(|e| e.to_string())?.is_file()
        {
            fs::remove_file(entry.path()).map_err(|e| e.to_string())?;
        }
    }
    File::open(directory)
        .and_then(|f| f.sync_all())
        .map_err(|e| e.to_string())
}
