//! Loading a display publication from disk.
//!
//! A publication directory holds `txid-shards.json` and one directory per
//! revision, named by its manifest digest: every archive the map names and
//! the recent shard, each hard-linked from the publisher's immutable store.
//! Every manifest is read and checked, whatever the worker's role, so any
//! worker can serve the map and every manifest. Tables are verified only for
//! the revisions the role serves: streamed SHA-256 per segment, then every
//! row through [`display::verify_rows`] with seek reads, so a corrupt shard is
//! refused at load rather than served as a PIR fault.
//!
//! A revision already verified by the previous snapshot, or staged ahead of
//! its seal, is taken over without re-reading only when the new directory
//! hard-links the same files (device, inode and length). Claiming the same
//! digest never skips verification.

use super::{kind, runtime_key, serves};
use crate::assignment::WorkerRole;
use crate::shardset::{LoadError, SegmentSource};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use transparent_shard::display::{self, DisplayManifest, DisplayMap, DisplayTable};
use transparent_shard::layout::Geometry;
use transparent_shard::txid::{self, ROW_BYTES};

/// The map file of a publication directory.
pub const MAP_FILE: &str = "txid-shards.json";
/// The manifest file of a revision directory.
pub const MANIFEST_FILE: &str = "manifest.json";

fn read(path: &Path) -> Result<Vec<u8>, LoadError> {
    std::fs::read(path).map_err(|source| LoadError::Io {
        path: path.to_path_buf(),
        source,
    })
}

fn invalid(message: impl Into<String>) -> LoadError {
    LoadError::Invalid(message.into())
}

/// One display shard revision: its manifest and, when this worker serves its
/// tier, where its verified tables are.
#[derive(Clone, Debug)]
pub struct DisplayRevision {
    pub manifest: DisplayManifest,
    /// The manifest digest: the directory name and the revision's identity.
    pub digest: String,
    /// The bytes `digest` is the hash of, served as-is.
    pub canonical: Vec<u8>,
    pub geometry: &'static Geometry,
    pub dir: PathBuf,
    /// Segments of each bucket directory, in bucket order. Empty unless held.
    pub buckets: Vec<Vec<SegmentSource>>,
    /// The shard's page segments. Empty unless held.
    pub pages: Vec<SegmentSource>,
}

impl DisplayRevision {
    /// Reads and checks a revision's manifest without touching its tables.
    pub fn read(dir: &Path) -> Result<Self, LoadError> {
        let path = dir.join(MANIFEST_FILE);
        let raw = read(&path)?;
        let manifest: DisplayManifest =
            serde_json::from_slice(&raw).map_err(|source| LoadError::Json { path, source })?;
        let digest = manifest.digest();
        let name = dir
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| invalid("display revision path has no name"))?;
        if name != digest {
            return Err(invalid(format!(
                "display revision directory is named {name} but its manifest digests to {digest}"
            )));
        }
        let geometry = manifest
            .validate()
            .map_err(|error| invalid(format!("display revision {digest}: {error}")))?;
        let canonical = manifest.canonical_bytes();
        Ok(Self {
            manifest,
            digest,
            canonical,
            geometry,
            dir: dir.to_path_buf(),
            buckets: Vec::new(),
            pages: Vec::new(),
        })
    }

    /// Whether this worker holds the revision's tables. Every display shard
    /// has at least one page segment, so a held revision has a nonempty list.
    pub fn held(&self) -> bool {
        !self.pages.is_empty()
    }

    /// Segments of `table`, or `None` for a bucket outside the shard.
    pub fn segments(&self, table: DisplayTable) -> Option<u32> {
        self.manifest
            .segments(table)
            .map(|segments| segments.len() as u32)
    }

    pub fn segment(&self, table: DisplayTable, segment: u32) -> Option<&SegmentSource> {
        match table {
            DisplayTable::Directory(bucket) => self.buckets.get(bucket as usize)?,
            DisplayTable::Pages => &self.pages,
        }
        .get(segment as usize)
    }

    /// Every table segment, bucket directories first.
    pub fn targets(&self) -> Vec<(DisplayTable, u32)> {
        let mut targets = Vec::new();
        for table in self.manifest.tables() {
            for segment in 0..self.segments(table).unwrap_or(0) {
                targets.push((table, segment));
            }
        }
        targets
    }

    /// Cache key strings of every table segment, for disk-cache keep sets.
    pub fn runtime_key_strings(&self) -> impl Iterator<Item = String> + '_ {
        self.targets()
            .into_iter()
            .map(|(table, segment)| runtime_key(&self.digest, table, segment).0)
    }

    /// Verifies every segment against the manifest, then every row, and
    /// returns the revision holding its sources.
    pub fn verify_tables(mut self) -> Result<Self, LoadError> {
        let geometry = self.geometry;
        let shard_id = self.manifest.shard_id;
        let mut buckets = Vec::new();
        let mut pages = Vec::new();
        for table in self.manifest.tables() {
            let published = self
                .manifest
                .segments(table)
                .expect("a listed table has segments");
            let rows = kind(table).rows(geometry);
            let row_bytes = kind(table).row_bytes(geometry);
            let mut sources = Vec::with_capacity(published.len());
            for (index, segment) in published.iter().enumerate() {
                if segment.rows != rows || segment.row_bytes != row_bytes {
                    return Err(invalid(format!(
                        "{} segment {index} of display shard {shard_id} is {}x{}, geometry {} is {rows}x{row_bytes}",
                        table.label(),
                        segment.rows,
                        segment.row_bytes,
                        geometry.name,
                    )));
                }
                let source = SegmentSource {
                    path: self.dir.join(table.file_name(index)),
                    rows,
                    row_bytes,
                    sha256: segment.sha256.clone(),
                };
                source.verify()?;
                sources.push(source);
            }
            match table {
                DisplayTable::Directory(_) => buckets.push(sources),
                DisplayTable::Pages => pages = sources,
            }
        }

        // Bucket membership, candidate rows, duplicates, page extents, orphan
        // fragments and counts, one row at a time.
        let open = |sources: &[SegmentSource]| {
            sources
                .iter()
                .map(|source| {
                    File::open(&source.path).map_err(|error| LoadError::Io {
                        path: source.path.clone(),
                        source: error,
                    })
                })
                .collect::<Result<Vec<_>, _>>()
        };
        let mut directory_files = buckets
            .iter()
            .map(|sources| open(sources))
            .collect::<Result<Vec<_>, _>>()?;
        let mut page_files = open(&pages)?;
        let directory_segments: Vec<usize> = buckets.iter().map(Vec::len).collect();
        let records: Vec<u64> = self.manifest.buckets.iter().map(|b| b.records).collect();
        let verified = display::verify_rows(
            shard_id,
            geometry,
            &directory_segments,
            pages.len(),
            &records,
            |table, segment, row| {
                let file = match table {
                    DisplayTable::Directory(bucket) => directory_files
                        .get_mut(bucket as usize)
                        .and_then(|files| files.get_mut(segment)),
                    DisplayTable::Pages => page_files.get_mut(segment),
                }
                .ok_or_else(|| txid::Error("display segment out of range".into()))?;
                file.seek(SeekFrom::Start((row * ROW_BYTES) as u64))
                    .map_err(|error| txid::Error(error.to_string()))?;
                let mut bytes = vec![0; ROW_BYTES];
                file.read_exact(&mut bytes)
                    .map_err(|error| txid::Error(error.to_string()))?;
                Ok(bytes)
            },
        )
        .map_err(|error| invalid(format!("display shard {shard_id}: {}", error.0)))?;
        let declared: Vec<_> = self
            .manifest
            .buckets
            .iter()
            .map(|bucket| bucket.page_histogram.clone())
            .collect();
        if verified.page_histograms != declared {
            return Err(invalid(format!(
                "display shard {shard_id}: page-count classes disagree with the rows"
            )));
        }
        self.buckets = buckets;
        self.pages = pages;
        Ok(self)
    }

    /// Takes over `old`'s verified sources when `dir` hard-links the same
    /// files; `None` when any file differs.
    fn relink(old: &Self, dir: &Path) -> Option<Self> {
        use std::os::unix::fs::MetadataExt;
        if !old.held() {
            return None;
        }
        let mut revision = old.clone();
        revision.dir = dir.to_path_buf();
        for source in revision
            .buckets
            .iter_mut()
            .flatten()
            .chain(revision.pages.iter_mut())
        {
            let next = dir.join(source.path.file_name()?);
            let a = std::fs::metadata(&source.path).ok()?;
            let b = std::fs::metadata(&next).ok()?;
            if a.dev() != b.dev() || a.ino() != b.ino() || a.len() != b.len() {
                return None;
            }
            source.path = next;
        }
        Some(revision)
    }
}

/// One loaded display publication.
pub struct DisplaySet {
    pub map: DisplayMap,
    /// The map exactly as served; `map_digest` is its SHA-256.
    pub map_json: Vec<u8>,
    pub map_digest: String,
    pub role: WorkerRole,
    /// The map's revisions in map order, then retained superseded recent
    /// revisions, newest first.
    revisions: Vec<DisplayRevision>,
    by_digest: BTreeMap<String, usize>,
    /// Superseded revisions past the retention bound: logged, not served.
    pub excess: Vec<PathBuf>,
}

impl DisplaySet {
    pub fn open(dir: &Path, retain: usize, role: WorkerRole) -> Result<Self, LoadError> {
        Self::open_reusing(dir, retain, None, &[], role)
    }

    /// Loads `dir`, taking over revisions of `previous` and `staged` that it
    /// hard-links, and verifying every other revision the role serves.
    pub fn open_reusing(
        dir: &Path,
        retain: usize,
        previous: Option<&Self>,
        staged: &[&DisplayRevision],
        role: WorkerRole,
    ) -> Result<Self, LoadError> {
        let map_path = dir.join(MAP_FILE);
        let raw = read(&map_path)?;
        let map: DisplayMap = serde_json::from_slice(&raw).map_err(|source| LoadError::Json {
            path: map_path,
            source,
        })?;
        map.check_shape()
            .map_err(|error| invalid(format!("display map is malformed: {error}")))?;
        // The map's identity is the digest of its canonical bytes, so a file
        // in any other serialization would be served under a digest the
        // controller never computed.
        let map_json = map.to_bytes();
        if map_json != raw {
            return Err(invalid("display map is not in its canonical serialization"));
        }
        let map_digest = hex::encode(Sha256::digest(&map_json));

        let mut found: BTreeMap<String, DisplayRevision> = BTreeMap::new();
        for entry in std::fs::read_dir(dir).map_err(|source| LoadError::Io {
            path: dir.to_path_buf(),
            source,
        })? {
            let entry = entry.map_err(|source| LoadError::Io {
                path: dir.to_path_buf(),
                source,
            })?;
            if entry.path().is_dir() {
                let revision = DisplayRevision::read(&entry.path())?;
                found.insert(revision.digest.clone(), revision);
            }
        }

        let hold = |revision: DisplayRevision| -> Result<DisplayRevision, LoadError> {
            let held = previous
                .and_then(|set| set.held(&revision.digest))
                .into_iter()
                .chain(staged.iter().copied())
                .filter(|old| old.digest == revision.digest)
                .find_map(|old| DisplayRevision::relink(old, &revision.dir));
            match held {
                Some(held) => Ok(held),
                None => revision.verify_tables(),
            }
        };

        let mut revisions = Vec::with_capacity(map.shards.len());
        for (index, entry) in map.shards.iter().enumerate() {
            let revision = found.remove(&entry.manifest_digest).ok_or_else(|| {
                invalid(format!(
                    "the display map names revision {} of shard {} but it is absent",
                    entry.manifest_digest, entry.shard_id
                ))
            })?;
            let manifest = &revision.manifest;
            if !entry.describes(manifest)
                || manifest.network != map.network
                || manifest.genesis_hash != map.genesis_hash
            {
                return Err(invalid(format!(
                    "display shard {} disagrees with its map entry",
                    entry.shard_id
                )));
            }
            // Manifests chain through the archives. The first listed shard
            // chains to an archive the window has dropped, unless it is the
            // very first shard, which chains to nothing.
            let chained = match index.checked_sub(1) {
                Some(before) => {
                    manifest.parent_manifest_digest == map.shards[before].manifest_digest
                }
                None => (entry.shard_id == 0) == manifest.parent_manifest_digest.is_empty(),
            };
            if !chained {
                return Err(invalid(format!(
                    "display shard {} does not chain to its predecessor",
                    entry.shard_id
                )));
            }
            revisions.push(if serves(role, entry.sealed) {
                hold(revision)?
            } else {
                revision
            });
        }

        // What is left must be a superseded recent revision, retained so a
        // wallet holding setup for it is still answered. A sealed revision is
        // final, so one the map does not name is not this publication's.
        let first = map.first_shard_id;
        let last = map
            .shards
            .last()
            .expect("a checked map is nonempty")
            .shard_id;
        let mut superseded = Vec::new();
        for (digest, revision) in found {
            let manifest = &revision.manifest;
            if manifest.sealed {
                return Err(invalid(format!(
                    "sealed display revision {digest} of shard {} is on disk but the map does not name it",
                    manifest.shard_id
                )));
            }
            if manifest.network != map.network
                || manifest.genesis_hash != map.genesis_hash
                || !(first..=last).contains(&manifest.shard_id)
            {
                return Err(invalid(format!(
                    "display revision {digest} does not belong to this publication"
                )));
            }
            superseded.push(revision);
        }
        superseded.sort_by_key(|revision| {
            std::cmp::Reverse((revision.manifest.end_height, revision.manifest.revision))
        });
        let mut excess = Vec::new();
        if serves(role, false) {
            for (index, revision) in superseded.into_iter().enumerate() {
                if index < retain {
                    revisions.push(hold(revision)?);
                } else {
                    tracing::warn!(
                        digest = %revision.digest,
                        path = %revision.dir.display(),
                        "superseded display revision past the retention bound; not served"
                    );
                    excess.push(revision.dir);
                }
            }
        }
        let by_digest = revisions
            .iter()
            .enumerate()
            .map(|(index, revision)| (revision.digest.clone(), index))
            .collect();
        Ok(Self {
            map,
            map_json,
            map_digest,
            role,
            revisions,
            by_digest,
            excess,
        })
    }

    /// Any revision this set knows, held or not.
    pub fn revision(&self, digest: &str) -> Option<&DisplayRevision> {
        self.by_digest
            .get(digest)
            .map(|&index| &self.revisions[index])
    }

    /// A revision whose tables this worker holds.
    pub fn held(&self, digest: &str) -> Option<&DisplayRevision> {
        self.revision(digest).filter(|revision| revision.held())
    }

    /// The revisions the map names, in map order.
    pub fn current(&self) -> &[DisplayRevision] {
        &self.revisions[..self.map.shards.len()]
    }

    /// Every revision known: current, then retained superseded.
    pub fn revisions(&self) -> &[DisplayRevision] {
        &self.revisions
    }

    /// Every runtime this worker keeps warm, in build order: the recent shard
    /// for a replica, archives oldest first for an owner. Retained superseded
    /// revisions are built on demand only.
    pub fn warm_targets(&self) -> Vec<(String, DisplayTable, u32)> {
        let mut held: Vec<&DisplayRevision> = self.current().iter().filter(|r| r.held()).collect();
        if self.role == WorkerRole::RecentReplica {
            held.reverse();
        }
        held.into_iter()
            .flat_map(|revision| {
                revision
                    .targets()
                    .into_iter()
                    .map(|(table, segment)| (revision.digest.clone(), table, segment))
            })
            .collect()
    }

    /// Cache key strings of every held revision.
    pub fn runtime_key_strings(&self) -> HashSet<String> {
        self.revisions
            .iter()
            .filter(|revision| revision.held())
            .flat_map(DisplayRevision::runtime_key_strings)
            .collect()
    }

    /// Every geometry the set names, in name order. Parameters are published
    /// for each whether or not this worker holds its tables.
    pub fn geometries(&self) -> Vec<&'static Geometry> {
        let mut seen: Vec<&'static Geometry> = Vec::new();
        for revision in &self.revisions {
            if !seen
                .iter()
                .any(|known| known.name == revision.geometry.name)
            {
                seen.push(revision.geometry);
            }
        }
        seen.sort_by_key(|geometry| geometry.name);
        seen
    }
}

#[cfg(test)]
mod tests {
    use super::super::synth::{self, ShardSpec};
    use super::*;
    use transparent_shard::display::{DisplaySealParams, TXID_2K};

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
            geometry: &TXID_2K,
        }
    }

    fn params() -> DisplaySealParams {
        DisplaySealParams {
            n_archive: 1,
            n_recent: 1,
            archive_target: 1,
            recent_floor: 1,
            reorg_margin: 1,
        }
    }

    /// One archive and one recent shard, written and linked into a candidate.
    fn publication(root: &Path) -> (PathBuf, Vec<synth::Published>) {
        let records = synth::records(40, 7);
        let archive = synth::write_shard(root, &spec(0, 10, 19, true), &records[..20]).unwrap();
        let mut recent = spec(1, 20, 29, false);
        recent.parent_manifest_digest = archive.digest.clone();
        let recent = synth::write_shard(root, &recent, &records[20..]).unwrap();
        let shards = vec![archive, recent];
        let (dir, _) = synth::write_candidate(root, &params(), &shards, "c0").unwrap();
        (dir, shards)
    }

    #[test]
    fn roles_hold_only_their_tier_and_relink_by_inode() {
        let root = tempfile::tempdir().unwrap();
        let (dir, shards) = publication(root.path());
        let owner = DisplaySet::open(&dir, 3, WorkerRole::ArchiveOwner).unwrap();
        assert!(owner.held(&shards[0].digest).is_some());
        assert!(owner.revision(&shards[1].digest).is_some());
        assert!(owner.held(&shards[1].digest).is_none());
        assert_eq!(owner.warm_targets().len(), 2);
        let replica = DisplaySet::open(&dir, 3, WorkerRole::RecentReplica).unwrap();
        assert!(replica.held(&shards[0].digest).is_none());
        assert!(replica.held(&shards[1].digest).is_some());
        assert_eq!(
            replica.runtime_key_strings(),
            HashSet::from([
                format!("{}/directory-0", shards[1].digest),
                shards[1].digest.clone()
            ])
        );

        // A second candidate linking the same files takes the sources over.
        let (next, _) = synth::write_candidate(root.path(), &params(), &shards, "c1").unwrap();
        let reused =
            DisplaySet::open_reusing(&next, 3, Some(&owner), &[], WorkerRole::ArchiveOwner)
                .unwrap();
        let held = reused.held(&shards[0].digest).unwrap();
        assert!(held.pages[0].path.starts_with(&next));
        assert!(held.buckets[0][0].path.starts_with(&next));
    }

    #[test]
    fn tampering_and_inconsistency_are_refused() {
        let root = tempfile::tempdir().unwrap();
        let (dir, shards) = publication(root.path());
        // The map's own bytes must be canonical.
        let map_path = dir.join(MAP_FILE);
        let canonical = std::fs::read(&map_path).unwrap();
        std::fs::write(&map_path, [canonical.as_slice(), b"\n"].concat()).unwrap();
        assert!(DisplaySet::open(&dir, 3, WorkerRole::RecentReplica).is_err());
        std::fs::write(&map_path, &canonical).unwrap();

        // A segment changed after publication. The candidate shares inodes
        // with the store, so write a fresh copy rather than editing in place.
        let segment = dir.join(&shards[1].digest).join("pages.0.bin");
        let mut bytes = std::fs::read(&segment).unwrap();
        bytes[100] ^= 1;
        std::fs::remove_file(&segment).unwrap();
        std::fs::write(&segment, &bytes).unwrap();
        assert!(DisplaySet::open(&dir, 3, WorkerRole::RecentReplica).is_err());
        // The owner never reads the recent tables, so it still loads.
        DisplaySet::open(&dir, 3, WorkerRole::ArchiveOwner).unwrap();

        // A sealed revision the map does not name.
        let root = tempfile::tempdir().unwrap();
        let (dir, shards) = publication(root.path());
        let stray = synth::write_shard(root.path(), &spec(5, 40, 49, true), &synth::records(3, 99))
            .unwrap();
        synth::link_revision(&stray.dir, &dir.join(&stray.digest)).unwrap();
        assert!(DisplaySet::open(&dir, 3, WorkerRole::ArchiveOwner).is_err());
        std::fs::remove_dir_all(dir.join(&stray.digest)).unwrap();

        // A broken manifest chain.
        let mut unchained = spec(1, 20, 29, false);
        unchained.parent_manifest_digest = "77".repeat(32);
        let records = synth::records(40, 7);
        let broken = synth::write_shard(root.path(), &unchained, &records[20..]).unwrap();
        let (dir, _) = synth::write_candidate(
            root.path(),
            &params(),
            &[shards[0].clone(), broken],
            "chain",
        )
        .unwrap();
        assert!(DisplaySet::open(&dir, 3, WorkerRole::ArchiveOwner).is_err());
    }

    #[test]
    fn superseded_recent_revisions_are_retained_up_to_the_bound() {
        let root = tempfile::tempdir().unwrap();
        let records = synth::records(60, 3);
        let archive =
            synth::write_shard(root.path(), &spec(0, 10, 19, true), &records[..20]).unwrap();
        let mut recents = Vec::new();
        for revision in 0..4u32 {
            let mut recent = spec(1, 20, 25 + revision as u64, false);
            recent.parent_manifest_digest = archive.digest.clone();
            recent.revision = revision;
            recent.supersedes = recents
                .last()
                .map(|r: &synth::Published| r.digest.clone())
                .unwrap_or_default();
            recents.push(
                synth::write_shard(
                    root.path(),
                    &recent,
                    &records[20..30 + 5 * revision as usize],
                )
                .unwrap(),
            );
        }
        let current = recents.last().unwrap().clone();
        let (dir, _) =
            synth::write_candidate(root.path(), &params(), &[archive, current], "r").unwrap();
        for old in &recents[..3] {
            synth::link_revision(&old.dir, &dir.join(&old.digest)).unwrap();
        }
        let set = DisplaySet::open(&dir, 2, WorkerRole::RecentReplica).unwrap();
        assert_eq!(set.revisions().len(), 4, "two current plus two retained");
        assert!(set.held(&recents[2].digest).is_some());
        assert!(set.held(&recents[1].digest).is_some());
        assert!(set.revision(&recents[0].digest).is_none());
        assert_eq!(set.excess.len(), 1);
        // An owner ignores superseded recent revisions altogether.
        let owner = DisplaySet::open(&dir, 2, WorkerRole::ArchiveOwner).unwrap();
        assert_eq!(owner.revisions().len(), 2);
    }
}
