//! Coordinator-produced public packing artifacts and bounded verified loading.
use crate::runtime::{self, Packing};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File},
    io::{self, BufWriter, Read, Write},
    path::Path,
};
#[cfg(not(feature = "native-reinspiring"))]
pub const DIRECTORY: &str = "prepared-packing-v1";
#[cfg(feature = "native-reinspiring")]
pub const DIRECTORY: &str = "prepared-native-packing-v2";
#[cfg(not(feature = "native-reinspiring"))]
pub const FORMAT: u16 = 1;
#[cfg(feature = "native-reinspiring")]
pub const FORMAT: u16 = 3;
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Artifact {
    pub name: String,
    pub sha256: String,
    pub bytes: u64,
    pub format: u16,
}
impl Artifact {
    pub fn validate(&self, rows: u64, public: &str) -> Result<(), String> {
        if self.format != FORMAT
            || self.name != artifact_name(rows, public)?
            || !enhance_pir::protocol::canonical_hash(&self.sha256)
            || !valid_len(self.bytes, rows)?
        {
            return Err("invalid prepared artifact identity or size".into());
        }
        Ok(())
    }
}
pub fn artifact_name(rows: u64, public: &str) -> Result<String, String> {
    let params = enhance_pir::protocol::parameters(rows)?;
    let parameter = enhance_pir::protocol::parameter_id(params.db_rows as u64)?;
    Ok(format!(
        "{}.bin",
        enhance_pir::protocol::digest(&("prepared-packing-v1", parameter, public))
    ))
}
pub fn describe_hint(root: &Path, hint: &str, rows: u64) -> Result<Artifact, String> {
    let public = hint.strip_suffix(".bin").ok_or("invalid hint name")?;
    let artifact = describe(root, &artifact_name(rows, public)?)?;
    artifact.validate(rows, public)?;
    Ok(artifact)
}
/// Native preprocessing stores each matrix at 32 or 64 bits depending on its
/// content, so only a bound on the artifact length is fixed.
#[cfg(feature = "native-reinspiring")]
const NATIVE_BYTES: std::ops::RangeInclusive<u64> = 192 * 1024 * 1024..=400 * 1024 * 1024;
fn valid_len(bytes: u64, rows: u64) -> Result<bool, String> {
    #[cfg(feature = "native-reinspiring")]
    {
        enhance_pir::protocol::parameters(rows)?;
        Ok(NATIVE_BYTES.contains(&bytes))
    }
    #[cfg(not(feature = "native-reinspiring"))]
    {
        Ok(bytes == expected_len(rows)?)
    }
}
pub fn expected_len(rows: u64) -> Result<u64, String> {
    let params = enhance_pir::protocol::parameters(rows)?;
    inspiring::prepared::encoded_len(runtime::rlwe(), params.db_cols / runtime::rlwe().d)
        .map(|n| {
            n + 8
                + (params.db_cols * ipir_sp::modulus_switch::modulus_bits(runtime::rlwe().q))
                    .div_ceil(8) as u64
        })
        .map_err(|e| e.to_string())
}
pub fn describe(root: &Path, name: &str) -> Result<Artifact, String> {
    let path = root.join(DIRECTORY).join(format!("{name}.json"));
    serde_json::from_reader(File::open(path).map_err(|e| e.to_string())?).map_err(|e| e.to_string())
}
pub fn persist(root: &Path, pack: &Packing) -> Result<Artifact, String> {
    let name = artifact_name(
        pack.params.db_rows as u64,
        &hex::encode(Sha256::digest(&pack.public)),
    )?;
    let directory = root.join(DIRECTORY);
    fs::create_dir_all(&directory).map_err(|e| e.to_string())?;
    if let Ok(a) = describe(root, &name) {
        if a.validate(
            pack.params.db_rows as u64,
            &hex::encode(Sha256::digest(&pack.public)),
        )
        .is_ok()
            && verify(&directory.join(&name), &a).is_ok()
        {
            return Ok(a);
        }
    }
    crate::artifact::write_atomic_cold(&directory, &name, |file| {
        let mut out = BufWriter::with_capacity(65536, file);
        pack.write_prepared(&mut out).map_err(io::Error::other)?;
        out.flush()
    })
    .map_err(|e| e.to_string())?;
    let path = directory.join(&name);
    let bytes = fs::metadata(&path).map_err(|e| e.to_string())?.len();
    let sha256 = hash_file(&path)?;
    let a = Artifact {
        name: name.clone(),
        sha256,
        bytes,
        format: FORMAT,
    };
    crate::artifact::write_atomic(&directory, &format!("{name}.json"), |f| {
        serde_json::to_writer(f, &a).map_err(io::Error::other)
    })
    .map_err(|e| e.to_string())?;
    File::open(&directory)
        .and_then(|f| f.sync_all())
        .map_err(|e| e.to_string())?;
    Ok(a)
}
pub fn hash_file(path: &Path) -> Result<String, String> {
    let mut f = File::open(path).map_err(|e| e.to_string())?;
    let mut hash = Sha256::new();
    let mut buffer = [0; 65536];
    loop {
        let n = f.read(&mut buffer).map_err(|e| e.to_string())?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    crate::artifact::release_file_cache(&f);
    Ok(hex::encode(hash.finalize()))
}
pub fn verify(path: &Path, a: &Artifact) -> Result<(), String> {
    if fs::metadata(path).map_err(|e| e.to_string())?.len() != a.bytes
        || hash_file(path)? != a.sha256
    {
        return Err("prepared artifact checksum/length mismatch".into());
    }
    Ok(())
}
pub fn load(
    path: &Path,
    a: &Artifact,
    rows: u64,
    budget: &crate::PackingBudget,
) -> Result<Packing, String> {
    if a.format != FORMAT || !valid_len(a.bytes, rows)? {
        return Err("incompatible prepared artifact".into());
    }
    let mut file = File::open(path).map_err(|e| e.to_string())?;
    if file.metadata().map_err(|e| e.to_string())?.len() != a.bytes {
        return Err("prepared artifact length mismatch".into());
    }
    let mut hash = Sha256::new();
    let mut buffer = [0u8; 65536];
    loop {
        let n = file.read(&mut buffer).map_err(|e| e.to_string())?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    if hex::encode(hash.finalize()) != a.sha256 {
        return Err("prepared artifact checksum mismatch".into());
    }
    // Authenticate the same inode that is mapped, even across path replacement.
    let mut permissions = file.metadata().map_err(|e| e.to_string())?.permissions();
    permissions.set_readonly(true);
    file.set_permissions(permissions)
        .map_err(|e| e.to_string())?;
    let pack = Packing::map_prepared(&file, rows, budget)?;
    crate::artifact::release_file_cache(&file);
    Ok(pack)
}

static PREPARATIONS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static PREPARATION_MICROS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub(crate) fn observe_preparation(elapsed: std::time::Duration) {
    use std::sync::atomic::Ordering::Relaxed;
    PREPARATIONS.fetch_add(1, Relaxed);
    PREPARATION_MICROS.fetch_add(elapsed.as_micros() as u64, Relaxed);
}
pub(crate) fn metrics() -> String {
    use std::sync::atomic::Ordering::Relaxed;
    format!(
        "enhance_packing_preparations_total {}\nenhance_packing_preparation_seconds_total {}\n",
        PREPARATIONS.load(Relaxed),
        PREPARATION_MICROS.load(Relaxed) as f64 / 1e6
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn identities_bind_parameters_and_metadata_is_checked_before_loading() {
        let root = tempfile::tempdir().unwrap();
        let public = "ab".repeat(32);
        let name = artifact_name(4096, &public).unwrap();
        assert_ne!(name, artifact_name(8192, &public).unwrap());
        #[cfg(not(feature = "native-reinspiring"))]
        let (bytes, invalid) = {
            let n = expected_len(4096).unwrap();
            (n, [n - 1, n + 1])
        };
        #[cfg(feature = "native-reinspiring")]
        let (bytes, invalid) = (
            *NATIVE_BYTES.start(),
            [NATIVE_BYTES.start() - 1, NATIVE_BYTES.end() + 1],
        );
        let mut a = Artifact {
            name: name.clone(),
            sha256: "cd".repeat(32),
            bytes,
            format: FORMAT,
        };
        assert!(a.validate(4096, &public).is_ok());
        assert!(a.validate(8192, &public).is_err());
        for bytes in invalid {
            let resized = Artifact { bytes, ..a.clone() };
            assert!(resized.validate(4096, &public).is_err());
        }
        fs::create_dir_all(root.path().join(DIRECTORY)).unwrap();
        a.format = 0;
        fs::write(
            root.path().join(DIRECTORY).join(format!("{name}.json")),
            serde_json::to_vec(&a).unwrap(),
        )
        .unwrap();
        assert!(describe_hint(root.path(), &format!("{public}.bin"), 4096).is_err());
        let budget = crate::PackingBudget::router();
        assert!(load(&root.path().join("absent"), &a, 4096, &budget).is_err());
        assert_eq!(budget.charged_bytes(), 0);
    }
    #[test]
    fn corrupt_and_truncated_files_fail_verification() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test");
        fs::write(&path, b"prepared").unwrap();
        let a = Artifact {
            name: "test".into(),
            sha256: hash_file(&path).unwrap(),
            bytes: 8,
            format: FORMAT,
        };
        verify(&path, &a).unwrap();
        fs::write(&path, b"Prepared").unwrap();
        assert!(verify(&path, &a).is_err());
        fs::write(&path, b"prep").unwrap();
        assert!(verify(&path, &a).is_err());
    }
}
