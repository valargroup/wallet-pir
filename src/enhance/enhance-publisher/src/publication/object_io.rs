//! Install immutable objects and durably replace bounded publication metadata.
use crate::{artifact, prepared_packing};
use enhance_shared::publication::{Object, METADATA_LIMIT};
use pir_publication::{open_object_file, ObjectFileError, ObjectInstaller};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File},
    io::{self, Read},
    path::Path,
};

/// Decode one JSON metadata file, refusing files or reads above the HTTPS
/// metadata bound. This does not validate the decoded value's domain rules.
pub fn read_metadata<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, String> {
    let file = open_object_file(path, None).map_err(|e| e.to_string())?;
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

/// Referencing an existing object does not alter its inode or hash it here.
/// The complete inventory verification must establish its contents before
/// preparation succeeds; this check establishes only its type and length.
fn object_exists(objects: &ObjectInstaller, object: &Object) -> Result<bool, String> {
    object.validate()?;
    let path = objects.directory().join(&object.sha256);
    match open_object_file(&path, Some(object.bytes)) {
        Ok(_) => Ok(true),
        Err(ObjectFileError::Io(error)) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.to_string()),
    }
}

fn release_object_cache(objects: &ObjectInstaller, object: &Object) -> Result<(), String> {
    let file = open_object_file(
        &objects.directory().join(&object.sha256),
        Some(object.bytes),
    )
    .map_err(|e| e.to_string())?;
    artifact::release_file_cache(&file);
    Ok(())
}

pub(super) fn install_object_bytes(
    objects: &ObjectInstaller,
    bytes: &[u8],
) -> Result<Object, String> {
    let object = Object {
        sha256: hex::encode(Sha256::digest(bytes)),
        bytes: bytes.len() as u64,
    };
    if !object_exists(objects, &object)? {
        objects
            .install_stream(bytes, &|| false)
            .map_err(|e| e.to_string())?;
        release_object_cache(objects, &object)?;
    }
    Ok(object)
}

pub(super) fn install_packing_object(
    objects: &ObjectInstaller,
    source: &Path,
    prepared: &prepared_packing::Artifact,
) -> Result<(), String> {
    let object = Object {
        sha256: prepared.sha256.clone(),
        bytes: prepared.bytes,
    };
    if object_exists(objects, &object)? {
        return Ok(());
    }
    let installed = objects
        .install_prepared_file(source, Some(&object.sha256), &|| false)
        .map_err(|e| e.to_string())?;
    if installed.reference.bytes != object.bytes {
        return Err("prepared packing object length differs from its descriptor".into());
    }
    release_object_cache(objects, &object)
}
