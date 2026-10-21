//! Digest-bound display distribution, separate from the wallet's display protocol.
use crate::publication::{
    digest, hash_artifacts, is_digest, safe_path, Artifact, CheckedRuntimes, Error, Kind, PROFILE,
    RUNTIME,
};
use crate::runtime::{disk::ShippedRuntimes, SharedParams};
use crate::tx_detail::{
    kind, runtime_key,
    set::{DisplaySet, MANIFEST_FILE, MAP_FILE},
    DisplayRole,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
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
        Self::prepare_reusing(root, &set, &mut CheckedRuntimes::default())
    }
    /// Finalize the inventory of `set`, loaded from `root`, reusing only
    /// native runtimes whose exact bytes this process already loaded and
    /// self-checked. Every artifact is rehashed, and every source segment
    /// must still match its manifest whatever `set` took over.
    pub fn prepare_reusing(
        root: &Path,
        set: &DisplaySet,
        checked: &mut CheckedRuntimes,
    ) -> Result<Self, Error> {
        if set.role != DisplayRole::Combined {
            return Err("display distribution needs every revision held".into());
        }
        let previous = &checked.passed;
        checked.loaded = 0;
        checked.reused = 0;
        let mut passed = BTreeMap::new();
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
        let mut pending = Vec::new();
        let mut expected = BTreeMap::new();
        let mut native = Vec::new();
        let mut params = std::collections::HashMap::new();
        // Paths are checked again, symlinks included, while hashing.
        let mut add = |path: PathBuf, kind: Kind| -> bool {
            if !paths.insert(path.clone()) {
                return false;
            }
            pending.push((path, kind));
            true
        };
        let map = root.join(MAP_FILE);
        expected.insert(map.clone(), set.map_digest.clone());
        add(map, Kind::Metadata);
        for revision in set.current() {
            add(revision.dir.join(MANIFEST_FILE), Kind::Metadata);
            let (table_kind, runtime_kind) = if revision.manifest.sealed {
                (Kind::ArchiveTable, Kind::ArchiveRuntime)
            } else {
                (Kind::RecentTable, Kind::RecentRuntime)
            };
            for (table, segment) in revision.targets() {
                let source = revision
                    .segment(table, segment)
                    .ok_or("missing display source")?;
                expected.insert(source.path().to_path_buf(), source.sha256().to_owned());
                add(source.path().to_path_buf(), table_kind);
                let shared = match params.entry((revision.geometry.name, kind(table))) {
                    std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        entry.insert(std::sync::Arc::new(SharedParams::build_display(
                            revision.geometry,
                            kind(table),
                        )?))
                    }
                };
                let key = runtime_key(&revision.digest, table, segment);
                let path = shipped.path(&key, shared, source.sha256());
                if add(path.clone(), runtime_kind) {
                    native.push((path, key, shared.clone(), source));
                }
            }
        }
        // Hash immutable artifacts on the bounded CPU pool. Native handles
        // stay open so the later self-check reads the inode just hashed.
        let mut runtimes = BTreeMap::new();
        for (artifact, path, file) in hash_artifacts(root, pending)? {
            if expected
                .get(&path)
                .is_some_and(|sha256| *sha256 != artifact.sha256)
            {
                return Err("display source differs from its manifest".into());
            }
            if let Some(file) = file {
                runtimes.insert(path.clone(), (artifact.sha256.clone(), file));
            }
            files.insert(artifact.sha256.clone(), path);
            inventory.artifacts.push(artifact);
        }
        // Keep native loads sequential to bound resident runtime memory.
        for (path, key, shared, source) in native {
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or("non-UTF8 display runtime name")?
                .to_owned();
            let (sha256, file) = runtimes.remove(&path).ok_or("unhashed display runtime")?;
            // The derived name binds parameters and source; the whole-file
            // digest binds the exact bytes checked by a previous inventory.
            if previous.get(&name) == Some(&sha256) {
                checked.reused += 1;
            } else {
                // Header/length checks alone cannot finalize malformed native bytes.
                ShippedRuntimes::load_file(file, &key, &shared, source.sha256())?
                    .self_check(&shared, &source.load()?)?;
                checked.loaded += 1;
            }
            passed.insert(name, sha256);
        }
        inventory.artifacts.sort_by(|a, b| a.path.cmp(&b.path));
        inventory.validate(&inventory.network, &inventory.genesis_hash)?;
        let bytes = serde_json::to_vec(&inventory)?;
        let terminal = set.map.shards.last().ok_or("empty display map")?;
        checked.passed = passed;
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

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "qualification-tests")]
    use crate::tx_detail::prebuild::build_shipped;
    use crate::tx_detail::prebuild::build_shipped_verified;
    use crate::tx_detail::synth::{self, ShardSpec};
    use zakura_pir_protocol::transparent::shard::display::{DisplaySealParams, TXID_2K8};

    fn spec(shard_id: u64, start: u64, end: u64, sealed: bool) -> ShardSpec {
        ShardSpec {
            shard_id,
            start_height: start,
            end_height: end,
            sealed,
            revision: 0,
            supersedes: String::new(),
            parent_manifest_digest: String::new(),
            n_buckets: 1,
            archive_target: 1,
            geometry: &TXID_2K8,
        }
    }

    fn params() -> DisplaySealParams {
        DisplaySealParams {
            n_archive: 1,
            n_recent: 1,
            archive_target: 1,
            recent_window: 1,
            reorg_margin: 1,
        }
    }

    /// Replaces `path` with its bytes, one bit flipped at `at` (clamped).
    fn flip(path: &Path, at: usize) {
        let mut bytes = std::fs::read(path).unwrap();
        let at = at.min(bytes.len() - 1);
        bytes[at] ^= 1;
        std::fs::remove_file(path).unwrap();
        std::fs::write(path, bytes).unwrap();
    }

    fn replace(path: &Path, bytes: &[u8]) {
        std::fs::remove_file(path).unwrap();
        std::fs::write(path, bytes).unwrap();
    }

    /// One archive and one recent shard, linked into two candidates.
    fn publication(root: &Path) -> (PathBuf, PathBuf, Vec<synth::Published>) {
        let records = synth::records(40, 7);
        let archive = synth::write_shard(root, &spec(0, 10, 19, true), &records[..20]).unwrap();
        let mut recent = spec(1, 20, 29, false);
        recent.parent_manifest_digest = archive.digest.clone();
        let recent = synth::write_shard(root, &recent, &records[20..]).unwrap();
        let shards = vec![archive, recent];
        let (dir, _) = synth::write_candidate(root, &params(), &shards, "c0").unwrap();
        let (next, _) = synth::write_candidate(root, &params(), &shards, "c1").unwrap();
        (dir, next, shards)
    }

    /// Evidence of earlier checks spares only the native reload of the exact
    /// bytes it names. Building real runtimes is a qualification cost, so
    /// these stand-ins are never valid runtimes: every load refuses them.
    #[test]
    fn checked_evidence_never_replaces_rehashing() {
        let root = tempfile::tempdir().unwrap();
        let (dir, _, _) = publication(root.path());
        let set = DisplaySet::open(&dir, 0, DisplayRole::Combined).unwrap();
        let shipped = ShippedRuntimes::required(dir.clone());
        let mut evidence = BTreeMap::new();
        let mut runtimes = Vec::new();
        for revision in set.current() {
            for (table, segment) in revision.targets() {
                let source = revision.segment(table, segment).unwrap();
                let shared = SharedParams::build_display(revision.geometry, kind(table)).unwrap();
                let key = runtime_key(&revision.digest, table, segment);
                let path = shipped.path(&key, &shared, source.sha256());
                let bytes = format!("stand-in for {}", path.display()).into_bytes();
                std::fs::write(&path, &bytes).unwrap();
                let name = path.file_name().unwrap().to_str().unwrap().to_owned();
                evidence.insert(name, digest(&bytes));
                runtimes.push(path);
            }
        }
        assert_eq!(runtimes.len(), 2);
        // Without evidence the stand-ins are loaded and refused.
        assert!(Finalized::prepare(&dir).is_err());
        let mut checked = CheckedRuntimes::default();
        assert!(Finalized::prepare_reusing(&dir, &set, &mut checked).is_err());
        assert!(checked.passed.is_empty());

        let mut checked = CheckedRuntimes {
            passed: evidence.clone(),
            ..Default::default()
        };
        let reused = Finalized::prepare_reusing(&dir, &set, &mut checked).unwrap();
        assert_eq!((checked.loaded, checked.reused), (0, 2));
        assert_eq!(checked.passed, evidence);
        let hashed = |path: &Path| {
            let relative = path.strip_prefix(&dir).unwrap().to_str().unwrap();
            let artifact = reused
                .inventory
                .artifacts
                .iter()
                .find(|a| a.path == relative)
                .unwrap();
            assert_eq!(artifact.sha256, digest(&std::fs::read(path).unwrap()));
        };
        for revision in set.current() {
            hashed(&revision.dir.join(MANIFEST_FILE));
            for (table, segment) in revision.targets() {
                hashed(revision.segment(table, segment).unwrap().path());
            }
        }
        runtimes.iter().for_each(|path| hashed(path));
        hashed(&dir.join(MAP_FILE));

        // Changed bytes under a proven name are loaded and refused, and a
        // refused inventory neither adds evidence nor discards it.
        let original = std::fs::read(&runtimes[0]).unwrap();
        flip(&runtimes[0], usize::MAX);
        assert!(Finalized::prepare_reusing(&dir, &set, &mut checked).is_err());
        assert_eq!(checked.passed, evidence);
        // So are a missing file and another runtime's proven bytes.
        std::fs::remove_file(&runtimes[0]).unwrap();
        assert!(Finalized::prepare_reusing(&dir, &set, &mut checked).is_err());
        std::fs::copy(&runtimes[1], &runtimes[0]).unwrap();
        assert!(Finalized::prepare_reusing(&dir, &set, &mut checked).is_err());
        assert_eq!(checked.passed, evidence);
        replace(&runtimes[0], &original);
        Finalized::prepare_reusing(&dir, &set, &mut checked).unwrap();
        assert_eq!((checked.loaded, checked.reused), (0, 2));

        // Sources and the map the set already verified are hashed again: a
        // change after loading no longer matches its manifest or map digest.
        for revision in set.current() {
            let segment = revision.buckets[0][0].path().to_path_buf();
            let original = std::fs::read(&segment).unwrap();
            flip(&segment, 0);
            let error = Finalized::prepare_reusing(&dir, &set, &mut checked)
                .err()
                .unwrap()
                .to_string();
            assert!(error.contains("differs from its manifest"), "{error}");
            replace(&segment, &original);
        }
        let map = dir.join(MAP_FILE);
        let original = std::fs::read(&map).unwrap();
        flip(&map, 0);
        assert!(Finalized::prepare_reusing(&dir, &set, &mut checked).is_err());
        replace(&map, &original);
        Finalized::prepare_reusing(&dir, &set, &mut checked).unwrap();
        assert_eq!(checked.passed, evidence);
    }

    /// The same, end to end with real runtimes: a later inventory skips only
    /// the native reload of runtimes whose exact bytes this process already
    /// self-checked, and still rehashes every source a reused set took over.
    /// Two native builds exceed the ordinary per-case budget.
    #[cfg(feature = "qualification-tests")]
    #[test]
    fn distribution_reuses_only_unchanged_checked_runtimes() {
        let root = tempfile::tempdir().unwrap();
        let (dir, next, shards) = publication(root.path());
        for shard in &shards {
            build_shipped(&dir.join(&shard.digest), &dir).unwrap();
        }
        // Stamps vouch only for files settled before they were hashed.
        std::thread::sleep(std::time::Duration::from_millis(1100));

        let set = DisplaySet::open(&dir, 0, DisplayRole::Combined).unwrap();
        let mut checked = CheckedRuntimes::default();
        let first = Finalized::prepare_reusing(&dir, &set, &mut checked).unwrap();
        let runtimes: Vec<_> = first
            .inventory
            .artifacts
            .iter()
            .filter(|a| matches!(a.kind, Kind::RecentRuntime | Kind::ArchiveRuntime))
            .map(|a| dir.join(&a.path))
            .collect();
        assert_eq!(runtimes.len(), 2);
        assert_eq!((checked.loaded, checked.reused), (2, 0));
        assert_eq!(first.sha256, Finalized::prepare(&dir).unwrap().sha256);
        let second = Finalized::prepare_reusing(&dir, &set, &mut checked).unwrap();
        assert_eq!((checked.loaded, checked.reused), (0, 2));
        assert_eq!(second.sha256, first.sha256);
        assert_eq!(second.files, first.files);

        // A replaced runtime is reloaded and refused, however often its name
        // was proven before; a refused inventory keeps the previous proof.
        let original = std::fs::read(&runtimes[0]).unwrap();
        flip(&runtimes[0], usize::MAX);
        assert!(Finalized::prepare_reusing(&dir, &set, &mut checked).is_err());
        replace(&runtimes[0], &original);
        Finalized::prepare_reusing(&dir, &set, &mut checked).unwrap();
        assert_eq!((checked.loaded, checked.reused), (0, 2));

        // Names already proven cannot hide a missing file or another
        // runtime's valid bytes.
        std::fs::remove_file(&runtimes[0]).unwrap();
        assert!(Finalized::prepare_reusing(&dir, &set, &mut checked).is_err());
        std::fs::copy(&runtimes[1], &runtimes[0]).unwrap();
        assert!(Finalized::prepare_reusing(&dir, &set, &mut checked).is_err());
        replace(&runtimes[0], &original);

        // The next candidate links the same files: the set takes every
        // segment over without hashing, building links every runtime, and
        // the inventory still rehashes everything but reloads nothing.
        let reused =
            DisplaySet::open_reusing(&next, 0, Some(&set), &[], DisplayRole::Combined).unwrap();
        assert_eq!(reused.verification().segments_hashed, 0);
        assert_eq!(reused.verification().bytes_row_checked, 0);
        for revision in reused.current() {
            for shipped in build_shipped_verified(revision, &next, &[dir.clone()]).unwrap() {
                assert_eq!(shipped.build_seconds, 0.0);
            }
        }
        let linked = Finalized::prepare_reusing(&next, &reused, &mut checked).unwrap();
        assert_eq!((checked.loaded, checked.reused), (0, 2));
        assert_eq!(linked.sha256, first.sha256);
        assert!(linked.files.values().all(|path| path.starts_with(&next)));

        // A segment changed after the set took it over no longer matches its
        // manifest, though the runtime built from it is unchanged and proven.
        for sealed in [true, false] {
            let revision = reused
                .current()
                .iter()
                .find(|r| r.manifest.sealed == sealed)
                .unwrap();
            let segment = revision.buckets[0][0].path().to_path_buf();
            let original = std::fs::read(&segment).unwrap();
            flip(&segment, 0);
            let error = Finalized::prepare_reusing(&next, &reused, &mut checked)
                .err()
                .unwrap()
                .to_string();
            assert!(error.contains("differs from its manifest"), "{error}");
            replace(&segment, &original);
        }
        // So does a map changed after the set was loaded.
        let map = next.join(MAP_FILE);
        let original = std::fs::read(&map).unwrap();
        flip(&map, 0);
        assert!(Finalized::prepare_reusing(&next, &reused, &mut checked).is_err());
        replace(&map, &original);
        Finalized::prepare_reusing(&next, &reused, &mut checked).unwrap();
        assert_eq!((checked.loaded, checked.reused), (0, 2));
    }

    #[test]
    fn verified_building_needs_held_tables() {
        let root = tempfile::tempdir().unwrap();
        let records = synth::records(20, 3);
        let archive = synth::write_shard(root.path(), &spec(0, 10, 19, true), &records).unwrap();
        let revision = crate::tx_detail::set::DisplayRevision::read(&archive.dir).unwrap();
        assert!(build_shipped_verified(&revision, root.path(), &[]).is_err());
    }
}
