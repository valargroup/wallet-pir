//! Digest-bound display distribution, separate from the wallet's display protocol.
use crate::publication::{digest, is_digest, safe_path, Artifact, Error, Kind, PROFILE, RUNTIME};
use crate::runtime::{disk::ShippedRuntimes, SharedParams};
use crate::tx_detail::{
    kind, runtime_key,
    set::{DisplaySet, MANIFEST_FILE, MAP_FILE},
    DisplayRole,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Read,
    path::{Path, PathBuf},
};

pub const SCHEMA: &str = "transparent-display-prepared-distribution-v1";
pub const PREFIX: &str = "/provider/txid/v1/";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Inventory {
    pub schema: String,
    pub network: String,
    pub genesis_hash: String,
    pub map_sha256: String,
    pub runtime: String,
    pub profile: String,
    pub artifacts: Vec<Artifact>,
}
impl Inventory {
    pub fn validate(&self, network: &str, genesis: &str) -> Result<(), Error> {
        if self.schema != SCHEMA
            || self.network != network
            || self.genesis_hash != genesis
            || self.runtime != RUNTIME
            || self.profile != PROFILE
            || !is_digest(&self.map_sha256)
            || self.artifacts.is_empty()
            || self.artifacts.len() > 100_000
        {
            return Err("invalid display distribution identity/profile".into());
        }
        let mut paths = BTreeSet::new();
        for file in &self.artifacts {
            if !safe_path(&file.path)
                || !is_digest(&file.sha256)
                || file.bytes == 0
                || !paths.insert(&file.path)
                || (file.kind == Kind::Metadata && file.bytes > 16 << 20)
            {
                return Err("unsafe, duplicate or incomplete display artifact".into());
            }
            if file.path.ends_with(".runtime") {
                let valid = file
                    .path
                    .strip_suffix(".runtime")
                    .and_then(|s| s.split_once('-'))
                    .is_some_and(|(a, b)| is_digest(a) && is_digest(b));
                if !valid || !matches!(file.kind, Kind::RecentRuntime | Kind::ArchiveRuntime) {
                    return Err("invalid display runtime artifact".into());
                }
            } else if file.path == MAP_FILE {
                if file.kind != Kind::Metadata {
                    return Err("display map is not metadata".into());
                }
            } else {
                let (revision, name) = file
                    .path
                    .split_once('/')
                    .ok_or("invalid display artifact path")?;
                if !is_digest(revision)
                    || name.contains('/')
                    || (name != MANIFEST_FILE && !name.ends_with(".bin"))
                {
                    return Err("invalid display revision artifact".into());
                }
            }
        }
        if !paths.contains(&MAP_FILE.to_string()) {
            return Err("display inventory omits map".into());
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct Finalized {
    pub inventory: Inventory,
    pub bytes: Vec<u8>,
    pub sha256: String,
    pub files: BTreeMap<String, PathBuf>,
    pub anchor_height: u64,
    pub anchor_hash: String,
}
impl Finalized {
    /// Verify source tables and every required native runtime before publication.
    pub fn prepare(root: &Path) -> Result<Self, Error> {
        let set = DisplaySet::open(root, 0, DisplayRole::Combined)?;
        let shipped = ShippedRuntimes::required(root.to_path_buf());
        let mut inventory = Inventory {
            schema: SCHEMA.into(),
            network: set.map.network.clone(),
            genesis_hash: set.map.genesis_hash.clone(),
            map_sha256: set.map_digest.clone(),
            runtime: RUNTIME.into(),
            profile: PROFILE.into(),
            artifacts: Vec::new(),
        };
        let mut files = BTreeMap::new();
        let mut paths = BTreeSet::new();
        let mut add = |path: PathBuf, kind| -> Result<(), Error> {
            let relative = path
                .strip_prefix(root)?
                .to_str()
                .ok_or("non-UTF8 display artifact")?
                .to_string();
            if !safe_path(&relative) {
                return Err("unsafe display source path".into());
            }
            let mut checked = root.to_path_buf();
            for component in relative.split('/') {
                checked.push(component);
                if std::fs::symlink_metadata(&checked)?
                    .file_type()
                    .is_symlink()
                {
                    return Err("symlink in display source path".into());
                }
            }
            if !paths.insert(relative.clone()) {
                return Ok(());
            }
            let mut file = std::fs::File::open(&path)?;
            if !file.metadata()?.is_file() {
                return Err("display artifact is not a regular file".into());
            }
            let bytes = file.metadata()?.len();
            use sha2::{Digest, Sha256};
            let mut hash = Sha256::new();
            let mut buffer = vec![0u8; 1 << 20];
            loop {
                let n = file.read(&mut buffer)?;
                if n == 0 {
                    break;
                }
                hash.update(&buffer[..n]);
            }
            let sha256 = hex::encode(hash.finalize());
            files.insert(sha256.clone(), path);
            inventory.artifacts.push(Artifact {
                path: relative,
                sha256,
                bytes,
                kind,
            });
            Ok(())
        };
        add(root.join(MAP_FILE), Kind::Metadata)?;
        for revision in set.current() {
            add(revision.dir.join(MANIFEST_FILE), Kind::Metadata)?;
            for (table, segment) in revision.targets() {
                let source = revision
                    .segment(table, segment)
                    .ok_or("missing display source")?;
                add(
                    source.path().to_path_buf(),
                    if revision.manifest.sealed {
                        Kind::ArchiveTable
                    } else {
                        Kind::RecentTable
                    },
                )?;
                let params = SharedParams::build_display(revision.geometry, kind(table))?;
                let key = runtime_key(&revision.digest, table, segment);
                let runtime = shipped.load(&key, &params, source.sha256())?;
                runtime.self_check(&params, &source.load()?)?;
                add(
                    shipped.path(&key, &params, source.sha256()),
                    if revision.manifest.sealed {
                        Kind::ArchiveRuntime
                    } else {
                        Kind::RecentRuntime
                    },
                )?;
            }
        }
        inventory.artifacts.sort_by(|a, b| a.path.cmp(&b.path));
        inventory.validate(&inventory.network, &inventory.genesis_hash)?;
        let bytes = serde_json::to_vec(&inventory)?;
        let terminal = set.map.shards.last().ok_or("empty display map")?;
        Ok(Self {
            sha256: digest(&bytes),
            bytes,
            inventory,
            files,
            anchor_height: terminal.end_height,
            anchor_hash: terminal.terminal_block_hash.clone(),
        })
    }
}
