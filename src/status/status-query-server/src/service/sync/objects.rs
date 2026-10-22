//! Durable authority records and verified cached-object retention.
use crate::publication::{valid_digest, Failure, Inventory, Object};
use std::{collections::BTreeSet, fs, io::Read, path::Path as FsPath};

pub(super) fn persist(root: &FsPath, name: &str, bytes: &[u8]) -> std::io::Result<()> {
    pir_control::durable::write_atomic(root, name, |f| f.write_all(bytes))?;
    pir_control::durable::sync_dir(root)
}
pub(super) fn verify_file(path: &FsPath, object: &Object) -> Result<(), Failure> {
    let file = fs::File::open(path)?;
    if file.metadata()?.len() != object.bytes {
        return Err("Status cached object length differs".into());
    }
    let mut reader = std::io::BufReader::new(file);
    let mut buffer = [0u8; 65536];
    let mut hash = pir_artifact::Hasher::new();
    loop {
        let n = reader.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    if hex::encode(hash.finalize()) != object.checksum {
        return Err("Status cached object digest differs".into());
    }
    Ok(())
}

pub(super) fn collect(root: &FsPath, inventories: &[Object]) -> Result<(), Failure> {
    let mut keep = BTreeSet::new();
    for object in inventories {
        keep.insert(object.checksum.clone());
        let bytes = fs::read(root.join(&object.checksum))?;
        object.verify(&bytes)?;
        let inventory: Inventory = serde_json::from_slice(&bytes)?;
        inventory.validate_material()?;
        for item in inventory
            .units
            .iter()
            .chain(&inventory.packing)
            .chain(std::iter::once(&inventory.public))
        {
            keep.insert(item.checksum.clone());
        }
    }
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if valid_digest(&name) && entry.file_type()?.is_file() && !keep.contains(&name) {
            fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}
