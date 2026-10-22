//! Install immutable objects and durably replace bounded publication metadata.
use crate::{artifact, prepared_packing};
use enhance_shared::publication::{Object, METADATA_LIMIT};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File},
    io::{self, Read},
    path::Path,
};

/// Decode one JSON metadata file, refusing files or reads above the HTTPS
/// metadata bound. This does not validate the decoded value's domain rules.
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

pub(super) fn persist_metadata(
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

pub(super) fn install_object_bytes(root: &Path, bytes: &[u8]) -> Result<Object, String> {
    let object = Object {
        sha256: hex::encode(Sha256::digest(bytes)),
        bytes: bytes.len() as u64,
    };
    // An existing object is hashed with the complete inventory.
    if !object.path(root)?.exists() {
        artifact::write_atomic_cold(&root.join("objects"), &object.sha256, |f| {
            f.write_all(bytes)
        })
        .map_err(|e| e.to_string())?;
    }
    Ok(object)
}

pub(super) fn install_packing_object(
    root: &Path,
    source: &Path,
    prepared: &prepared_packing::Artifact,
) -> Result<(), String> {
    let object = Object {
        sha256: prepared.sha256.clone(),
        bytes: prepared.bytes,
    };
    // The copy is hashed with the complete inventory.
    if object.path(root)?.exists() {
        return Ok(());
    }
    artifact::write_atomic_cold(&root.join("objects"), &object.sha256, |f| {
        io::copy(&mut File::open(source)?, f)?;
        Ok(())
    })
    .map(drop)
    .map_err(|e| e.to_string())
}
