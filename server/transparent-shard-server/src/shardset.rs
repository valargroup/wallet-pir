//! Loading a published shard set from disk.
//!
//! The publisher writes one directory per shard revision, named by the digest
//! of that revision's manifest, plus a `shards.json` holding the map. This
//! reads them back and refuses anything that does not verify.
//!
//! A shard's tables are stored one file per segment. Ordinarily there is one of
//! each; a shard whose content did not fit a single segment of its geometry has
//! more, and every segment shares that geometry, which is what keeps one
//! parameter set serving every shard that names it.
//!
//! # Manifests are read, tables are not
//!
//! Each segment is verified at load — length and digest — and then **released**.
//! What is retained is a [`SegmentSource`]: where the bytes are and what they
//! must hash to. A fleet-sized set is tens of gigabytes of tables, and holding
//! them resident to serve a handful of hot shards is the difference between a
//! worker that fits its host and one that does not. The bytes are re-read, and
//! re-verified, when a runtime is built from them.
//!
//! Verifying at startup anyway is deliberate. It is what makes an incomplete or
//! corrupt set a failed precondition rather than an outage: the deploy validates
//! the set before it stops the running service, and a check deferred to first
//! query would move that failure to after the cutover.
//!
//! Verification here is paranoid about *the operator's own files*, not only
//! about a hostile network. A shard whose table no longer matches its manifest
//! digest would be served as if it were the published shard, and every client
//! would reject its rows as a PIR fault rather than as a corrupt shard.
//!
//! # Revisions
//!
//! The map names one manifest digest per shard, and that is the current
//! revision. A growing tail is republished as a *new* digest beside the old one
//! rather than as changed bytes under the old, so a directory on disk that the
//! map does not name is ordinarily a superseded revision, not junk.
//!
//! Those are retained, up to a bound, because a wallet that fetched setup for a
//! revision and then queried it must be answered from *that* revision or told
//! plainly that it is gone. Answering from the current one instead would hand
//! back rows from a range the client did not ask about, and it would look like
//! a successful query.
//!
//! `filter.bin` is loaded, verified against the map, and — since the transparent
//! host was asked to serve a complete API — also served from here. It is around
//! a hundred kilobytes, so unlike the tables it is cheap to keep.
//!
//! That is a deliberate relaxation of an earlier rule, which kept public bytes
//! and private requests on separate origins so the two could not be correlated
//! by construction. What the rule actually bought was narrower than it looked:
//! the shard id of a private query is public in its own URL, and one operator
//! runs both services, so the correlation was available from logs regardless.
//! What it did preserve was a wallet's ability to reach the two over different
//! network paths, and that option is kept — the filter service still serves the
//! same bytes at the same paths on its own host, so a wallet that wants two
//! origins still has them.

use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use transparent_filter::{filter_hash, ShardMap};
use transparent_shard::layout::{by_name as geometry_by_name, Geometry};
use transparent_shard::manifest::{ShardManifest, SCHEMA};

/// Superseded revisions kept per shard, beyond the one the map names.
///
/// Bounded because each one is a directory whose manifest and filter stay
/// resident and whose runtimes may be built. One is enough for the ordinary
/// case — a wallet holding setup for the revision that was current when it
/// started — and the publisher only ever supersedes the tail.
pub const DEFAULT_RETAIN_REVISIONS: usize = 3;

#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    #[error("reading {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("{path} is not valid JSON: {source}")]
    Json {
        path: PathBuf,
        source: serde_json::Error,
    },
    #[error("{0}")]
    Invalid(String),
}

fn read(path: &Path) -> Result<Vec<u8>, LoadError> {
    std::fs::read(path).map_err(|source| LoadError::Io {
        path: path.to_path_buf(),
        source,
    })
}

/// Which of a shard's two private tables a query addresses.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Table {
    Directory,
    Pages,
}

impl Table {
    pub fn as_str(self) -> &'static str {
        match self {
            Table::Directory => "directory",
            Table::Pages => "pages",
        }
    }

    pub fn parse(text: &str) -> Option<Self> {
        match text {
            "directory" => Some(Table::Directory),
            "pages" => Some(Table::Pages),
            _ => None,
        }
    }

    /// Rows in one segment of this table, at `geometry`.
    pub fn rows(self, geometry: &Geometry) -> u64 {
        match self {
            Table::Directory => geometry.directory_rows,
            Table::Pages => geometry.page_rows,
        }
    }

    /// Bytes in one row of this table, at `geometry`.
    pub fn row_bytes(self, geometry: &Geometry) -> u32 {
        match self {
            Table::Directory => geometry.directory_row_bytes as u32,
            Table::Pages => geometry.page_row_bytes as u32,
        }
    }
}

/// The published seed a table's public query setup is derived from.
///
/// Distinct per geometry *and* per table. The two tables of one geometry have
/// different row counts, so their public parameters already differ; two
/// geometries have different row counts again. Deriving the seed from both
/// names means a client that mixed any of them up fails its own re-derivation
/// check rather than decoding a row against the wrong table's setup — which
/// would not error, it would return plausible nonsense.
///
/// Domain-separated by the schema string, so a future schema reusing these
/// names does not reuse their seeds.
pub fn setup_seed(geometry: &Geometry, table: Table) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(SCHEMA.as_bytes());
    hasher.update(b"/setup-seed\0");
    hasher.update(geometry.name.as_bytes());
    hasher.update(b"\0");
    hasher.update(table.as_str().as_bytes());
    let digest = hasher.finalize();
    u64::from_le_bytes(digest[..8].try_into().expect("eight bytes"))
}

/// Where one segment's bytes are, and what they must be.
///
/// Retained in place of the bytes themselves. `load` is the only way back to
/// them, and it re-checks both length and digest, so a file replaced or
/// truncated after startup fails the build rather than being packed into a
/// runtime and served.
#[derive(Clone, Debug)]
pub struct SegmentSource {
    pub path: PathBuf,
    pub rows: u64,
    pub row_bytes: u32,
    /// SHA-256 of the file, in hex, as the manifest publishes it.
    pub sha256: String,
}

impl SegmentSource {
    /// Bytes this segment occupies on disk, and transiently in memory while a
    /// runtime is built from it.
    ///
    /// Not `len`: a segment is a fixed rectangle of rows and is never empty, so
    /// the emptiness question `len` invites has no answer here.
    pub fn bytes(&self) -> usize {
        self.rows as usize * self.row_bytes as usize
    }

    /// Verifies length and digest without retaining the bytes.
    ///
    /// Streamed rather than read whole, because this runs for every segment of
    /// every shard at startup and a fleet-sized set would otherwise need its
    /// whole plaintext resident to prove it is intact.
    pub fn verify(&self) -> Result<(), LoadError> {
        let mut file = std::fs::File::open(&self.path).map_err(|source| LoadError::Io {
            path: self.path.clone(),
            source,
        })?;
        let mut hasher = Sha256::new();
        let mut buffer = vec![0u8; 1 << 20];
        let mut total = 0usize;
        loop {
            let read = file.read(&mut buffer).map_err(|source| LoadError::Io {
                path: self.path.clone(),
                source,
            })?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
            total += read;
        }
        self.check(total, hasher.finalize().as_slice())
    }

    /// Reads and verifies the segment, for building a runtime from.
    ///
    /// The caller drops the buffer as soon as the runtime is built. Nothing
    /// retains it: the runtime holds the *encoded* database, and keeping the
    /// plaintext beside it would be the accounting error this whole change
    /// exists to remove.
    pub fn load(&self) -> Result<Vec<u8>, LoadError> {
        let bytes = read(&self.path)?;
        self.check(bytes.len(), Sha256::digest(&bytes).as_slice())?;
        Ok(bytes)
    }

    fn check(&self, len: usize, digest: &[u8]) -> Result<(), LoadError> {
        if len != self.bytes() {
            return Err(LoadError::Invalid(format!(
                "{} is {len} bytes, the manifest says {}",
                self.path.display(),
                self.bytes()
            )));
        }
        if hex::encode(digest) != self.sha256 {
            return Err(LoadError::Invalid(format!(
                "{} does not match its manifest digest",
                self.path.display()
            )));
        }
        Ok(())
    }
}

/// One published shard revision: its manifest, its filter, and where its tables
/// are.
pub struct LoadedShard {
    pub manifest: ShardManifest,
    /// The manifest digest, which is also the directory name and the identity
    /// of this revision.
    pub digest: String,
    /// The geometry the manifest names, resolved against the registry.
    pub geometry: &'static Geometry,
    /// The shard's public range filter, as published.
    ///
    /// Retained rather than dropped after verification because this service
    /// serves it. A filter is around a hundred kilobytes, so a worker's whole
    /// assignment costs a few megabytes -- negligible beside the runtimes, and
    /// unlike the tables it is served on every sync rather than on a match.
    pub filter: Vec<u8>,
    /// One entry per segment, in segment order.
    pub directory: Vec<SegmentSource>,
    pub pages: Vec<SegmentSource>,
}

impl LoadedShard {
    /// One segment of a table, or `None` if this shard has no such segment.
    pub fn segment(&self, table: Table, segment: u32) -> Option<&SegmentSource> {
        self.sources(table).get(segment as usize)
    }

    /// How many segments this shard's table has. Ordinarily one.
    pub fn segments(&self, table: Table) -> u32 {
        self.sources(table).len() as u32
    }

    fn sources(&self, table: Table) -> &[SegmentSource] {
        match table {
            Table::Directory => &self.directory,
            Table::Pages => &self.pages,
        }
    }

    fn open(dir: &Path) -> Result<Self, LoadError> {
        let raw = read(&dir.join("manifest.json"))?;
        let manifest: ShardManifest =
            serde_json::from_slice(&raw).map_err(|source| LoadError::Json {
                path: dir.join("manifest.json"),
                source,
            })?;

        // The publisher names the directory by the manifest digest. Recomputing
        // it here means a manifest edited after publication is refused rather
        // than served under the identity of the shard it replaced.
        let digest = manifest.digest();
        let name = dir
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| LoadError::Invalid("shard path has no name".into()))?;
        if name != digest {
            return Err(LoadError::Invalid(format!(
                "shard directory is named {name} but its manifest digests to {digest}"
            )));
        }
        if manifest.schema != SCHEMA {
            return Err(LoadError::Invalid(format!(
                "shard declares schema {}, this server serves {SCHEMA}",
                manifest.schema
            )));
        }

        // A geometry is selected by name, not inferred from the dimensions.
        // An unknown name is refused: this build has no parameters for it, and
        // guessing from the row counts would mean serving a shape nobody
        // validated.
        let geometry = geometry_by_name(&manifest.geometry).ok_or_else(|| {
            LoadError::Invalid(format!(
                "shard {} names geometry {}, which this build does not serve",
                manifest.shard_id, manifest.geometry
            ))
        })?;

        // Every segment must match the named geometry, not just the first: a
        // segment with its own widths would silently need its own parameters,
        // and sharing one set per geometry is the whole point of naming it.
        let mut tables: Vec<Vec<SegmentSource>> = Vec::new();
        for (table, segments) in [
            (Table::Directory, &manifest.directory_segments),
            (Table::Pages, &manifest.page_segments),
        ] {
            if segments.is_empty() {
                return Err(LoadError::Invalid(format!(
                    "shard {} declares no {} segments",
                    manifest.shard_id,
                    table.as_str()
                )));
            }
            let mut sources = Vec::with_capacity(segments.len());
            for (index, published) in segments.iter().enumerate() {
                if published.rows != table.rows(geometry)
                    || published.row_bytes != table.row_bytes(geometry)
                {
                    return Err(LoadError::Invalid(format!(
                        "{} segment {index} of shard {} is {}x{} but geometry {} is {}x{}",
                        table.as_str(),
                        manifest.shard_id,
                        published.rows,
                        published.row_bytes,
                        geometry.name,
                        table.rows(geometry),
                        table.row_bytes(geometry),
                    )));
                }
                let source = SegmentSource {
                    path: dir.join(format!("{}.{index}.bin", table.as_str())),
                    rows: published.rows,
                    row_bytes: published.row_bytes,
                    sha256: published.sha256.clone(),
                };
                source.verify()?;
                sources.push(source);
            }
            tables.push(sources);
        }
        let mut tables = tables.into_iter();
        let directory = tables.next().expect("directory segments");
        let pages = tables.next().expect("page segments");

        let filter = read(&dir.join("filter.bin"))?;
        if filter_hash(&filter).to_display_hex() != manifest.filter_hash {
            return Err(LoadError::Invalid(format!(
                "filter of shard {} does not match its manifest digest",
                manifest.shard_id
            )));
        }

        Ok(Self {
            manifest,
            digest,
            geometry,
            filter,
            directory,
            pages,
        })
    }
}

/// A published shard set: the map, the revision it names for each shard, and
/// any superseded revisions still on disk.
pub struct ShardSet {
    pub map: ShardMap,
    /// The map exactly as it will be served.
    ///
    /// Serialized once rather than per request, so every consumer of this set
    /// sees the same bytes and `map_digest` describes what was actually sent.
    /// Re-serializing per request lets two origins publish byte-different JSON
    /// for one set, which a wallet comparing them would read as disagreement.
    pub map_json: Vec<u8>,
    pub map_digest: String,
    /// Every revision loaded, current and superseded.
    revisions: Vec<LoadedShard>,
    /// Index into `revisions` of the revision the map names, by shard id.
    current: BTreeMap<u64, usize>,
    /// Index into `revisions` by manifest digest, for revision-addressed
    /// requests.
    by_digest: BTreeMap<String, usize>,
}

impl ShardSet {
    /// Loads and verifies the set rooted at `dir`, retaining at most
    /// `retain_revisions` superseded revisions per shard.
    ///
    /// Every shard the map names must be present and verify. A partial set is
    /// refused rather than served with holes: a wallet syncing a range whose
    /// shards were silently missing would advance coverage over history it
    /// never retrieved.
    pub fn open(dir: &Path, retain_revisions: usize) -> Result<Self, LoadError> {
        let raw = read(&dir.join("shards.json"))?;
        let map: ShardMap = serde_json::from_slice(&raw).map_err(|source| LoadError::Json {
            path: dir.join("shards.json"),
            source,
        })?;
        map.check_shape()
            .map_err(|error| LoadError::Invalid(format!("shard map is malformed: {error}")))?;

        // Index by manifest digest, which is the identity of a *revision*.
        // Indexing by shard id would make a republished tail — old and new
        // revision directories side by side, which is exactly what the
        // publisher writes — look like two directories claiming one shard, and
        // refuse to start on a set the publisher considers well formed.
        let mut by_digest: BTreeMap<String, LoadedShard> = BTreeMap::new();
        for entry in std::fs::read_dir(dir).map_err(|source| LoadError::Io {
            path: dir.to_path_buf(),
            source,
        })? {
            let entry = entry.map_err(|source| LoadError::Io {
                path: dir.to_path_buf(),
                source,
            })?;
            if !entry.path().is_dir() {
                continue;
            }
            let shard = LoadedShard::open(&entry.path())?;
            // A digest names its own content, so two directories cannot hold
            // the same digest and different bytes. One filesystem path per
            // digest is still required, so the source of a revision is
            // unambiguous.
            if by_digest.insert(shard.digest.clone(), shard).is_some() {
                return Err(LoadError::Invalid(format!(
                    "two directories in {} hold the same manifest digest",
                    dir.display()
                )));
            }
        }

        let mut revisions: Vec<LoadedShard> = Vec::with_capacity(map.shards.len());
        let mut current: BTreeMap<u64, usize> = BTreeMap::new();
        let mut parent_digest = String::new();
        for entry in &map.shards {
            let shard = by_digest.remove(&entry.manifest_digest).ok_or_else(|| {
                LoadError::Invalid(format!(
                    "the map names revision {} of shard {} but it is absent",
                    entry.manifest_digest, entry.shard_id
                ))
            })?;
            let manifest = &shard.manifest;
            if manifest.shard_id != entry.shard_id
                || manifest.geometry != entry.geometry
                || manifest.start_height != entry.start_height
                || manifest.end_height != entry.end_height
                || manifest.terminal_block_hash != entry.terminal_block_hash
                || manifest.filter_hash != entry.filter_hash
                || manifest.sealed != entry.sealed
                || manifest.revision != entry.revision
                // A wallet reads the segment counts from the map and asks every
                // segment. A map that understated them would leave a segment
                // unqueried, and the wallet would advance coverage over history
                // it never read.
                || manifest.directory_segments.len() as u32 != entry.directory_segments
                || manifest.page_segments.len() as u32 != entry.page_segments
            {
                return Err(LoadError::Invalid(format!(
                    "shard {} disagrees with its map entry",
                    entry.shard_id
                )));
            }
            // The manifest chain is what makes the set a committed sequence
            // rather than a bag of ranges that happen to be adjacent.
            if manifest.parent_manifest_digest != parent_digest {
                return Err(LoadError::Invalid(format!(
                    "shard {} does not chain to its predecessor",
                    entry.shard_id
                )));
            }
            parent_digest = shard.digest.clone();
            current.insert(entry.shard_id, revisions.len());
            revisions.push(shard);
        }

        // What is left is either a superseded revision of a shard the map names
        // — which is retained so a wallet mid-sync can still be answered from
        // the revision it fetched setup for — or something that does not belong
        // in this set at all.
        let mut retained: BTreeMap<u64, usize> = BTreeMap::new();
        for (digest, shard) in by_digest {
            let shard_id = shard.manifest.shard_id;
            let Some(&index) = current.get(&shard_id) else {
                return Err(LoadError::Invalid(format!(
                    "revision {digest} is on disk but shard {shard_id} is not in the map"
                )));
            };
            let published = &revisions[index].manifest;
            // A revision at or past the current one is not a predecessor. It is
            // either the same content under a second digest or a publication
            // the map has not caught up with, and serving either would mean
            // answering for a revision no wallet was told about.
            if shard.manifest.revision >= published.revision {
                return Err(LoadError::Invalid(format!(
                    "revision {} of shard {shard_id} is on disk but the map names revision {}",
                    shard.manifest.revision, published.revision
                )));
            }
            // A sealed shard is final, so it has no superseded revisions to
            // retain; one on disk means the set was assembled from two
            // different partitions of the chain.
            if published.sealed && shard.manifest.sealed {
                return Err(LoadError::Invalid(format!(
                    "shard {shard_id} is sealed but has a second sealed revision on disk"
                )));
            }
            let held = retained.entry(shard_id).or_default();
            *held += 1;
            if *held > retain_revisions {
                return Err(LoadError::Invalid(format!(
                    "shard {shard_id} has more than {retain_revisions} superseded revisions on \
                     disk; prune the set or raise --retain-revisions"
                )));
            }
            revisions.push(shard);
        }

        let by_digest = revisions
            .iter()
            .enumerate()
            .map(|(index, shard)| (shard.digest.clone(), index))
            .collect();

        // Serialized here, once, so what is served and what is digested are the
        // same bytes.
        let map_json = serde_json::to_vec(&map).map_err(|source| LoadError::Json {
            path: dir.join("shards.json"),
            source,
        })?;
        let map_digest = hex::encode(Sha256::digest(&map_json));

        Ok(Self {
            map,
            map_json,
            map_digest,
            revisions,
            current,
            by_digest,
        })
    }

    /// The revision of `shard_id` the map currently names.
    pub fn get(&self, shard_id: u64) -> Option<&LoadedShard> {
        self.current
            .get(&shard_id)
            .map(|&index| &self.revisions[index])
    }

    /// One revision by its manifest digest, current or superseded.
    ///
    /// A request naming a digest this returns `None` for is not a bad request:
    /// it is a wallet holding a revision that has since been pruned, and the
    /// service tells it so explicitly rather than answering from whatever is
    /// current.
    pub fn revision(&self, digest: &str) -> Option<&LoadedShard> {
        self.by_digest
            .get(digest)
            .map(|&index| &self.revisions[index])
    }

    /// Shards the map names.
    pub fn len(&self) -> usize {
        self.current.len()
    }

    pub fn is_empty(&self) -> bool {
        self.current.is_empty()
    }

    /// Every revision held, current and superseded.
    pub fn revisions(&self) -> &[LoadedShard] {
        &self.revisions
    }

    /// Every geometry this set uses, in registry order.
    ///
    /// What the service builds parameter sets for. A set that mixes archive and
    /// recent shards needs both, and only both: preparing every registered
    /// geometry would leak parameters for shapes this worker does not hold.
    pub fn geometries(&self) -> Vec<&'static Geometry> {
        let mut seen: Vec<&'static Geometry> = Vec::new();
        for shard in &self.revisions {
            if !seen.iter().any(|held| held.name == shard.geometry.name) {
                seen.push(shard.geometry);
            }
        }
        seen.sort_by_key(|geometry| geometry.name);
        seen
    }
}
