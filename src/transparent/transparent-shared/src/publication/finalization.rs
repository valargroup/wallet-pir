//! Prepare immutable history inventories for distribution.
use super::*;

/// Finalized immutable inventory and exact sources. HTTP never resolves a
/// caller-supplied filename against the filesystem.
#[derive(Clone)]
pub struct Finalized {
    pub(super) inventory: Inventory,
    pub(super) bytes: Vec<u8>,
    pub(super) sha256: String,
    pub(super) files: BTreeMap<String, (PathBuf, u64)>,
    pub(super) inherited_preparation: bool,
    pub(super) prepared_files: Option<BTreeMap<String, FileIdentity>>,
}
impl Finalized {
    /// Whether any bodies still owe independent validation after a restart.
    pub fn requires_validation(&self) -> bool {
        self.inherited_preparation
    }

    pub fn inventory(&self) -> &Inventory {
        &self.inventory
    }
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn sha256(&self) -> &str {
        &self.sha256
    }

    /// Acquire only a listed artifact, checking the size finalized for it.
    pub fn open_file(&self, digest: &str) -> Result<Option<std::fs::File>, Error> {
        self.files
            .get(digest)
            .map(|(path, bytes)| open_object_file(path, Some(*bytes)).map_err(Into::into))
            .transpose()
    }
    pub fn prepare(root: &Path, set: &ShardSet, recent_from_height: u64) -> Result<Self, Error> {
        Self::prepare_holdings(
            root,
            set,
            recent_from_height,
            false,
            &mut CheckedRuntimes::default(),
            &|| false,
        )
    }
    /// Finalize every table and runtime for autonomous full-coverage consumers.
    pub fn prepare_full(
        root: &Path,
        set: &ShardSet,
        recent_from_height: u64,
    ) -> Result<Self, Error> {
        Self::prepare_holdings(
            root,
            set,
            recent_from_height,
            true,
            &mut CheckedRuntimes::default(),
            &|| false,
        )
    }
    /// Finalize an inventory using unchanged process-local checks or explicitly
    /// inherited publisher preparation. Changed files are hashed and checked.
    pub fn prepare_reusing(
        root: &Path,
        set: &ShardSet,
        recent_from_height: u64,
        full: bool,
        checked: &mut CheckedRuntimes,
    ) -> Result<Self, Error> {
        Self::prepare_holdings(root, set, recent_from_height, full, checked, &|| false)
    }
    /// [`Self::prepare_reusing`], asking `stop` before each file it reads;
    /// once it answers true the preparation ends with [`Stopped`], nothing is
    /// finalized and `checked` keeps its earlier evidence.
    pub fn prepare_reusing_until(
        root: &Path,
        set: &ShardSet,
        recent_from_height: u64,
        full: bool,
        checked: &mut CheckedRuntimes,
        stop: &(dyn Fn() -> bool + Sync),
    ) -> Result<Self, Error> {
        Self::prepare_holdings(root, set, recent_from_height, full, checked, stop)
    }
    /// List unchanged tables from the set's process-local verification or local
    /// publisher preparation. Every other file follows ordinary verification.
    fn prepare_holdings(
        root: &Path,
        set: &ShardSet,
        recent_from_height: u64,
        full: bool,
        checked: &mut CheckedRuntimes,
        stop: &(dyn Fn() -> bool + Sync),
    ) -> Result<Self, Error> {
        let previous = &checked.passed;
        checked.loaded = 0;
        checked.reused = 0;
        checked.bytes_hashed = 0;
        checked.bytes_reused = 0;
        let mut passed = BTreeMap::new();
        let mut evidence = BTreeMap::new();
        let mut preparation = BTreeMap::new();
        let mut files = BTreeMap::new();
        let mut prepared_files = BTreeMap::new();
        let mut inherited_preparation = false;
        let Plan {
            mut inventory,
            mut pending,
            sources,
            native,
        } = plan(root, set, recent_from_height, full)?;
        // A runtime whose exact inode this process hashed and self-checked,
        // unchanged since, is listed with that digest instead of read again.
        for (path, kind, known) in &mut pending {
            if matches!(kind, Kind::RecentRuntime | Kind::ArchiveRuntime) {
                *known = path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .and_then(|name| {
                        checked
                            .evidence
                            .get(name)
                            .or_else(|| checked.preparation.get(name))
                    })
                    .map(|(sha256, bytes, stamp)| Known {
                        sha256: sha256.clone(),
                        bytes: *bytes,
                        stamp: *stamp,
                        prepared: checked
                            .preparation
                            .contains_key(path.file_name().and_then(|n| n.to_str()).unwrap_or("")),
                    });
            }
        }
        // Hash immutable artifacts on the existing bounded CPU pool. Native
        // handles stay open so the later self-check reads the inode just hashed.
        let mut runtimes = BTreeMap::new();
        for listed in hash_listed(root, pending, stop)? {
            let Listed {
                artifact,
                path,
                physical,
                opened,
                stamp,
                settled,
                read,
                prepared,
            } = listed;
            inherited_preparation |= prepared;
            if read {
                checked.bytes_hashed += artifact.bytes;
            } else {
                checked.bytes_reused += artifact.bytes;
            }
            if let Some(expected) = sources.get(&path) {
                if artifact.sha256 != *expected {
                    return Err("source segment differs from its manifest".into());
                }
            }
            if let Some(file) = opened {
                let runtime = (
                    artifact.sha256.clone(),
                    file,
                    physical.clone(),
                    stamp,
                    settled,
                    prepared,
                );
                runtimes.insert(path.clone(), runtime);
            }
            if full || artifact.kind.local() {
                files.insert(artifact.sha256.clone(), (physical, artifact.bytes));
            }
            prepared_files.insert(artifact.path.clone(), stamp);
            inventory.artifacts.push(artifact);
        }
        // Keep native loads sequential to bound resident runtime memory.
        for (path, key, shared, source) in native {
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or("non-UTF8 runtime name")?
                .to_owned();
            let (sha256, file, physical, stamp, settled, prepared) =
                runtimes.remove(&path).ok_or("unhashed runtime")?;
            let probe = file.try_clone()?;
            // The derived name binds parameters and source; the whole-file
            // digest binds the exact bytes checked by a previous inventory.
            if previous.get(&name) == Some(&sha256)
                || checked
                    .preparation
                    .get(&name)
                    .is_some_and(|(digest, _, identity)| digest == &sha256 && *identity == stamp)
            {
                checked.reused += 1;
            } else {
                if stop() {
                    return Err(Box::new(Stopped));
                }
                ShippedRuntimes::load_file(file, &key, &shared, source.sha256())?
                    .self_check(&shared, &source.load()?)?;
                checked.loaded += 1;
            }
            // Only the inode listed and checked, still unchanged and still
            // what its path names, is finalized or vouched for later.
            stamp.check(&physical, &probe)?;
            if settled {
                let target = if prepared {
                    &mut preparation
                } else {
                    &mut evidence
                };
                target.insert(
                    name.clone(),
                    (sha256.clone(), probe.metadata()?.len(), stamp),
                );
            }
            if !prepared {
                passed.insert(name, sha256);
            }
        }
        inventory.artifacts.sort_by(|a, b| a.path.cmp(&b.path));
        inventory.validate(&inventory.network, &inventory.genesis_hash)?;
        inventory.validate_map(&set.map)?;
        let bytes = serde_json::to_vec(&inventory)?;
        let sha256 = digest(&bytes);
        checked.passed = passed;
        checked.evidence = evidence;
        checked.preparation = preparation;
        Ok(Self {
            inventory,
            bytes,
            sha256,
            files,
            prepared_files: Some(prepared_files),
            inherited_preparation,
        })
    }
}
/// Every artifact a publication of `set` lists, in discovery order, with the
/// manifest digest of each source segment and the native runtimes to check.
/// Paths are checked to be safe, free of symbolic links and regular files;
/// nothing is hashed or loaded.
pub(super) struct Plan<'a> {
    pub(super) inventory: Inventory,
    pub(super) pending: Vec<(PathBuf, Kind, Option<Known>)>,
    pub(super) sources: BTreeMap<PathBuf, &'a str>,
    pub(super) native: Vec<NativeCheck<'a>>,
}
type NativeCheck<'a> = (
    PathBuf,
    crate::runtime::RuntimeKey,
    std::sync::Arc<SharedParams>,
    &'a crate::history::shardset::SegmentSource,
);
pub(super) fn plan<'a>(
    root: &Path,
    set: &'a ShardSet,
    recent_from_height: u64,
    full: bool,
) -> Result<Plan<'a>, Error> {
    let recent = set
        .map
        .shards
        .iter()
        .find(|s| s.start_height == recent_from_height)
        .ok_or("recent cutoff is not an existing shard boundary")?
        .shard_id;
    let inventory = Inventory {
        schema: SCHEMA.into(),
        network: set.map.network.clone(),
        genesis_hash: set.map.genesis_hash.clone(),
        map_sha256: set.map_digest.clone(),
        shard_schema: transparent_shard::manifest::SCHEMA.into(),
        runtime: RUNTIME.into(),
        profile: PROFILE.into(),
        recent_from_shard: recent,
        artifacts: Vec::new(),
    };
    let mut paths = BTreeSet::new();
    let mut pending = Vec::new();
    let mut sources = BTreeMap::new();
    let mut native = Vec::new();
    // `known`: a digest and length this process verified for the file
    // with this stamp, used only while the opened file still has it.
    let mut add = |path: PathBuf, kind: Kind, known: Option<Known>| -> Result<bool, Error> {
        let relative = path
            .strip_prefix(root)?
            .to_str()
            .ok_or("non-UTF8 artifact")?
            .to_string();
        resolve(root, &relative)?;
        if !paths.insert(relative.clone()) {
            return Ok(false);
        }
        pending.push((path, kind, known));
        Ok(true)
    };
    add(root.join("shards.json"), Kind::Metadata, None)?;
    let shipped = ShippedRuntimes::required(root.to_path_buf());
    // Parameters depend only on geometry and table kind; build each once.
    let mut built = BTreeMap::new();
    for entry in &set.map.shards {
        let dir = root.join(&entry.manifest_digest);
        add(dir.join("manifest.json"), Kind::Metadata, None)?;
        add(dir.join("filter.bin"), Kind::Metadata, None)?;
        let shard = set
            .revision(&entry.manifest_digest)
            .ok_or("producer needs verified complete set")?;
        let local = entry.shard_id >= recent;
        for table in shard.tables() {
            let params = if full || local {
                let id = (shard.geometry.name, table.kind().as_str());
                Some(match built.entry(id) {
                    std::collections::btree_map::Entry::Occupied(entry) => {
                        std::sync::Arc::clone(entry.get())
                    }
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        let shared = SharedParams::build(shard.geometry, table.kind())?;
                        std::sync::Arc::clone(entry.insert(std::sync::Arc::new(shared)))
                    }
                })
            } else {
                None
            };
            for segment in 0..shard.segments(table) {
                let source = shard
                    .segment(table, segment)
                    .ok_or("missing source segment")?;
                sources.insert(source.path().to_path_buf(), source.sha256());
                add(
                    source.path().to_path_buf(),
                    if local {
                        Kind::RecentTable
                    } else {
                        Kind::ArchiveTable
                    },
                    Known::of(source),
                )?;
                if let Some(shared) = &params {
                    let key = (
                        shard.runtime_identity(table).to_string(),
                        table.kind(),
                        segment,
                    );
                    let path = shipped.path(&key, shared, source.sha256());
                    if add(
                        path.clone(),
                        if local {
                            Kind::RecentRuntime
                        } else {
                            Kind::ArchiveRuntime
                        },
                        None,
                    )? {
                        native.push((path, key, shared.clone(), source));
                    }
                }
            }
        }
    }
    Ok(Plan {
        inventory,
        pending,
        sources,
        native,
    })
}
