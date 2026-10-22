//! Immutable predecessors for isolated runtime and recovery tests.
use crate::runtime::{DomainPlan, Engine};
use crate::RECORD_BYTES;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

pub fn records(start: u64, count: usize) -> Result<Vec<u8>, String> {
    let mut bytes = vec![0; count * RECORD_BYTES];
    for (i, row) in bytes.chunks_exact_mut(RECORD_BYTES).enumerate() {
        row[..8].copy_from_slice(&(start + i as u64).to_le_bytes());
    }
    Ok(bytes)
}

// Failure tests need a published predecessor, not another cold build of
// the same public rows. Copy its immutable bytes; each engine still loads
// and accounts for its own kernels under the test's fault instrumentation.
pub fn seed_published_fixture(root: &Path, domain: &DomainPlan) {
    if cfg!(feature = "qualification-tests") || cfg!(feature = "cuda") {
        return;
    }
    let Some(cache) = std::env::var_os("WALLET_PIR_TEST_RUNTIME_CACHE") else {
        return;
    };
    let cache = PathBuf::from(cache);
    let prepared = cache.join(format!(
        "enhance-unit-{}-{}",
        cfg!(feature = "native-reinspiring"),
        hex::encode(Sha256::digest(serde_json::to_vec(domain).unwrap()))
    ));
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(cache.join("enhance-unit.lock"))
        .unwrap();
    lock.lock().unwrap();
    if !prepared.exists() {
        let temporary = tempfile::tempdir_in(&cache).unwrap();
        drop(
            Engine::new(temporary.path())
                .prepare(domain.clone(), records)
                .unwrap(),
        );
        std::fs::rename(temporary.path(), &prepared).unwrap();
    }
    fn copy(source: &Path, target: &Path) {
        std::fs::create_dir_all(target).unwrap();
        for entry in std::fs::read_dir(source).unwrap() {
            let entry = entry.unwrap();
            let kind = entry.file_type().unwrap();
            let to = target.join(entry.file_name());
            if kind.is_dir() {
                copy(&entry.path(), &to);
            } else {
                assert!(
                    kind.is_file(),
                    "published fixtures contain only regular files"
                );
                std::fs::copy(entry.path(), to).unwrap();
            }
        }
    }
    let source = prepared.join("artifacts-9");
    assert!(
        source.is_dir(),
        "published fixture is missing its artifact tree"
    );
    copy(&source, &root.join("artifacts-9"));
}
