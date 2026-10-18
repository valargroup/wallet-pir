//! Local, checksummed preparation artifacts. Cache failures are misses, never
//! evidence of successful recovery. The caller validates wallet semantics.
use std::fs::{self, File, FileTimes, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

pub const LIMIT: u64 = 5 * 1024 * 1024 * 1024;
static SERIAL: AtomicU64 = AtomicU64::new(0);
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    #[default]
    Reuse,
    Refresh,
    Off,
}

pub fn default_dir() -> PathBuf {
    std::env::var_os("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|p| PathBuf::from(p).join(".cache")))
        .unwrap_or_else(std::env::temp_dir)
        .join("transparent-loadtest/preparation")
}
pub fn checksum(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
pub fn key(identity: &Value) -> String {
    checksum(&serde_json::to_vec(identity).expect("JSON identity"))
}

pub fn read(root: &Path, identity: &Value) -> Result<Option<Value>> {
    let path = root.join(format!("{}.json", key(identity)));
    let file = match File::open(&path) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    if file.metadata()?.len() > LIMIT {
        bail!("cache entry exceeds size limit");
    }
    let entry: Value = serde_json::from_reader(&file).context("invalid cache JSON")?;
    if entry["version"] != 1
        || entry["identity"] != *identity
        || entry["validation"]["outcome"] != "exact"
        || entry["validation"]["completion"] != "Complete"
        || entry["validation"]["unresolved_spends"] != 0
    {
        bail!("cache metadata or recovery validation mismatch");
    }
    if entry["seed_sha256"] != checksum(&serde_json::to_vec(&entry["seed"])?) {
        bail!("cache payload checksum mismatch");
    }
    let _ = file.set_times(FileTimes::new().set_modified(SystemTime::now()));
    Ok(Some(entry))
}

pub fn write(
    root: &Path,
    identity: &Value,
    seed: &Value,
    validation: &Value,
    provenance: &Value,
) -> Result<bool> {
    if validation["outcome"] != "exact"
        || validation["completion"] != "Complete"
        || validation["unresolved_spends"] != 0
    {
        bail!("only complete exact preparation may be cached");
    }
    let entry = json!({"version":1,"identity":identity,"seed":seed,"seed_sha256":checksum(&serde_json::to_vec(seed)?),"validation":validation,"provenance":provenance});
    let bytes = serde_json::to_vec(&entry)?;
    if bytes.len() as u64 > LIMIT {
        return Ok(false);
    }
    fs::create_dir_all(root)?;
    let target = root.join(format!("{}.json", key(identity)));
    let temporary = root.join(format!(
        ".{}-{}.tmp",
        std::process::id(),
        SERIAL.fetch_add(1, Ordering::Relaxed)
    ));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(&temporary, &target)?;
        prune(root, LIMIT)?;
        Ok(true)
    })();
    let _ = fs::remove_file(temporary);
    result
}

fn prune(root: &Path, limit: u64) -> Result<()> {
    let mut entries: Vec<_> = fs::read_dir(root)?
        .filter_map(|e| {
            let path = e.ok()?.path();
            if path.extension()?.to_str()? != "json" {
                return None;
            }
            let name = path.file_stem()?.to_str()?;
            if name.len() != 64 || !name.bytes().all(|b| b.is_ascii_hexdigit()) {
                return None;
            }
            let metadata = path.metadata().ok()?;
            Some((metadata.modified().ok()?, metadata.len(), path))
        })
        .collect();
    entries.sort_by_key(|e| e.0);
    let mut bytes: u64 = entries.iter().map(|e| e.1).sum();
    for (_, size, path) in entries {
        if bytes <= limit {
            break;
        }
        if fs::remove_file(path).is_ok() {
            bytes = bytes.saturating_sub(size);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn checksums_identity_atomic_writes_and_eviction() {
        let dir = tempfile::tempdir().unwrap();
        let identity = json!({"anchor":1});
        let valid = json!({"outcome":"exact","completion":"Complete","unresolved_spends":0});
        assert!(read(dir.path(), &identity).unwrap().is_none());
        assert!(write(
            dir.path(),
            &identity,
            &json!({}),
            &json!({"outcome":"failed"}),
            &json!({})
        )
        .is_err());
        std::thread::scope(|scope| {
            for _ in 0..4 {
                let identity = &identity;
                let valid = &valid;
                let dir = &dir;
                scope.spawn(move || {
                    write(
                        dir.path(),
                        identity,
                        &json!({"events":[]}),
                        valid,
                        &json!({}),
                    )
                    .unwrap()
                });
            }
        });
        assert!(read(dir.path(), &identity).unwrap().is_some());
        assert!(read(dir.path(), &json!({"anchor":2})).unwrap().is_none());
        let path = dir.path().join(format!("{}.json", key(&identity)));
        let mut entry: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        entry["seed"] = json!({"events":["corrupt"]});
        fs::write(&path, serde_json::to_vec(&entry).unwrap()).unwrap();
        assert!(read(dir.path(), &identity).is_err());
        write(dir.path(), &identity, &json!({}), &valid, &json!({})).unwrap();
        let unrelated = dir.path().join("notes.json");
        fs::write(&unrelated, "unrelated").unwrap();
        prune(dir.path(), 0).unwrap();
        assert!(unrelated.exists());
        assert!(read(dir.path(), &identity).unwrap().is_none());
    }
}
