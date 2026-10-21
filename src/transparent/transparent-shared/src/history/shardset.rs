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

use zakura_pir_protocol::transparent::filter as transparent_filter;
use zakura_pir_protocol::transparent::shard as transparent_shard;

use crate::history::assignment::{Assignment, WorkerRole, WorkerScope};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
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

/// Which shards of the set this process verifies tables for.
#[derive(Clone)]
pub enum LoadScope {
    /// Every shard the map names. The pilot and single-worker shape.
    Whole,
    /// Only the shards an assignment gives this worker. Every other shard's
    /// manifest and filter are still loaded, so the worker answers the public
    /// routes for the whole set; only its tables are a subset.
    Assigned {
        assignment: Arc<Assignment>,
        worker_id: String,
    },
}

/// How a set is loaded.
#[derive(Clone)]
pub struct LoadOptions {
    /// Superseded revisions kept per shard, beyond the one the map names.
    pub retain_revisions: usize,
    /// Bytes of superseded revisions kept per shard, on disk, beyond the
    /// current one. `None` bounds by count alone.
    pub retain_bytes: Option<u64>,
    /// Whether revisions past the bound are reported prunable rather than
    /// refused. The whole-set pilot refuses; a fleet worker reports, and the
    /// deploy prunes after activation.
    pub prune_excess: bool,
    pub scope: LoadScope,
}

impl LoadOptions {
    pub fn whole(retain_revisions: usize) -> Self {
        Self {
            retain_revisions,
            retain_bytes: None,
            prune_excess: false,
            scope: LoadScope::Whole,
        }
    }
}

/// A superseded revision on disk that the retention bound does not cover.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrunableRevision {
    pub path: PathBuf,
    pub shard_id: u64,
    pub revision: u32,
    pub digest: String,
    pub bytes: u64,
}

/// A shard revision's manifest and filter, without its tables.
///
/// Kept for every current shard of the set whatever the assignment, so any
/// worker can serve the map, the filters and the manifests. A few hundred
/// kilobytes per shard.
pub struct RevisionMeta {
    pub manifest: ShardManifest,
    pub digest: String,
    pub canonical_manifest: Vec<u8>,
    pub filter: Vec<u8>,
    pub geometry: &'static Geometry,
}

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
    #[error("{path} changed while it was verified")]
    ChangedDuringVerification { path: PathBuf },
    #[error("{0}")]
    Invalid(String),
}

fn read(path: &Path) -> Result<Vec<u8>, LoadError> {
    std::fs::read(path).map_err(|source| LoadError::Io {
        path: path.to_path_buf(),
        source,
    })
}

/// Which private table a query addresses: a history shard's directory or
/// pages, or a txid display bucket's table of fixed-size entries.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Table {
    Directory,
    Pages,
    TxDirectory,
}

impl Table {
    /// The tables a history shard has.
    pub const HISTORY: [Self; 2] = [Self::Directory, Self::Pages];
    pub fn as_str(self) -> &'static str {
        match self {
            Table::Directory => "directory",
            Table::TxDirectory => "txdirectory",
            Table::Pages => "pages",
        }
    }

    pub fn parse(text: &str) -> Option<Self> {
        match text {
            "directory" => Some(Table::Directory),
            "pages" => Some(Table::Pages),
            "txdirectory" => Some(Table::TxDirectory),
            _ => None,
        }
    }

    /// Rows in one segment of this table, at `geometry`.
    pub fn rows(self, geometry: &Geometry) -> u64 {
        match self {
            Table::Directory | Table::TxDirectory => geometry.directory_rows,
            Table::Pages => geometry.page_rows,
        }
    }

    /// Bytes in one row of this table, at `geometry`.
    pub fn row_bytes(self, geometry: &Geometry) -> u32 {
        match self {
            Table::Directory | Table::TxDirectory => geometry.directory_row_bytes as u32,
            Table::Pages => geometry.page_row_bytes as u32,
        }
    }
}

/// A table one history query addresses: a directory shard's directory, or
/// its page shard `j`, served as the table `pages-<j>` of the directory
/// shard's revision.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TableRef {
    Directory,
    Pages(u32),
}

impl TableRef {
    /// The table a request path names, or `None`. One table has one name.
    pub fn parse(name: &str) -> Option<Self> {
        if name == Table::Directory.as_str() {
            return Some(Self::Directory);
        }
        transparent_shard::manifest::parse_pages_table(name).map(Self::Pages)
    }

    /// The name on the wire and in the query binding.
    pub fn name(self) -> String {
        match self {
            Self::Directory => Table::Directory.as_str().to_string(),
            Self::Pages(child) => transparent_shard::manifest::pages_table(child),
        }
    }

    /// The kind, which decides the native parameters and setup seed: every
    /// page shard of a geometry shares one parameter set.
    pub fn kind(self) -> Table {
        match self {
            Self::Directory => Table::Directory,
            Self::Pages(_) => Table::Pages,
        }
    }
}

/// The published seed a table's public query setup is derived from; see
/// [`transparent_shard::display::setup_seed_named`], which clients share.
pub fn setup_seed(geometry: &Geometry, table: Table) -> u64 {
    transparent_shard::display::setup_seed_named(geometry.name, table.as_str())
}

/// Where one segment's bytes are, and what they must be.
///
/// Retained in place of the bytes themselves. `load` is the only way back to
/// them, and it re-checks both length and digest, so a file replaced or
/// truncated after startup fails the build rather than being packed into a
/// runtime and served.
///
/// A verified input record: only the loaders construct one, from a manifest
/// they have checked, and its fields are read-only. Naming a path and a digest
/// is not enough to obtain one, so nothing outside the loader can present an
/// arbitrary file as a verified segment.
#[derive(Clone, Debug)]
pub struct SegmentSource {
    path: PathBuf,
    rows: u64,
    row_bytes: u32,
    /// SHA-256 of the file, in hex, as the manifest publishes it.
    sha256: String,
    /// The first row a query may omit, from which every byte must be zero:
    /// a page shard's [`query_rows`], or `rows` when queries select every
    /// row. Checked wherever the digest is, so no runtime is built or
    /// restored from a segment whose omitted rows hold data.
    ///
    /// [`query_rows`]: transparent_shard::manifest::PageChild::query_rows
    zero_from_row: u64,
    /// What this process observed of the file the last time it hashed it,
    /// when that observation is certain enough to vouch for the file later;
    /// see [`FileStamp`]. In memory only, so a restart verifies everything.
    evidence: Option<FileStamp>,
}

/// The identity and change metadata of a file this process hashed.
///
/// Sampled from the open file before and after the bytes were read, and from
/// the path once they had been: all three must agree, or the file changed
/// while it was verified and is refused. A later revision whose segment
/// resolves to a file with the same stamp is the same inode, unmodified since
/// it was hashed, and inherits the verification without re-reading it.
///
/// Both modification and change time are part of the stamp: restoring the
/// modification time after writing cannot preserve the change time. Creating
/// or removing a hard link also changes it and conservatively requires another
/// hash. The modification time is only trusted when it predates the read
/// by more than any timestamp granularity, so a write in the same tick as the
/// file's last modification cannot leave the stamp unchanged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileStamp {
    dev: u64,
    ino: u64,
    len: u64,
    mtime_sec: i64,
    mtime_nsec: i64,
    ctime_sec: i64,
    ctime_nsec: i64,
}

impl FileStamp {
    fn of(meta: &std::fs::Metadata) -> Option<Self> {
        use std::os::unix::fs::MetadataExt;
        meta.is_file().then(|| Self {
            dev: meta.dev(),
            ino: meta.ino(),
            len: meta.len(),
            mtime_sec: meta.mtime(),
            mtime_nsec: meta.mtime_nsec(),
            ctime_sec: meta.ctime(),
            ctime_nsec: meta.ctime_nsec(),
        })
    }

    /// Whether the last modification predates `instant` by more than the
    /// coarsest timestamp granularity a host filesystem uses.
    fn settled_before(&self, instant: std::time::SystemTime) -> bool {
        let Ok(since) = instant.duration_since(std::time::UNIX_EPOCH) else {
            return false;
        };
        let modified = i128::from(self.mtime_sec) * 1_000_000_000 + i128::from(self.mtime_nsec);
        let changed = i128::from(self.ctime_sec) * 1_000_000_000 + i128::from(self.ctime_nsec);
        modified.max(changed) + i128::from(EVIDENCE_SETTLE_NANOS) < since.as_nanos() as i128
    }
}

/// How long before a read a file must last have been modified for its stamp
/// to vouch for it later: one second, the coarsest mtime granularity in use.
const EVIDENCE_SETTLE_NANOS: i64 = 1_000_000_000;

/// What one load spent proving its tables, and what it took over instead.
///
/// `bytes_hashed` counts table bytes streamed through SHA-256 while loading,
/// including the directory re-read that checks a choice table; runtime builds
/// and cache restores re-check their sources separately and are not counted.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct VerificationReport {
    pub segments_hashed: u64,
    pub segments_reused: u64,
    pub bytes_hashed: u64,
    /// The part of `bytes_hashed` that was page shard tables.
    pub page_bytes_hashed: u64,
    /// Table bytes inherited from earlier verification without re-reading.
    pub bytes_reused: u64,
    /// Rows read back for content checks beyond the digest (display rows).
    pub bytes_row_checked: u64,
    pub wall_micros: u64,
    /// CPU time of the loading thread, where the host reports it.
    pub cpu_micros: Option<u64>,
}

impl VerificationReport {
    fn hashed(&mut self, source: &SegmentSource, page: bool) {
        self.segments_hashed += 1;
        self.bytes_hashed += source.bytes() as u64;
        if page {
            self.page_bytes_hashed += source.bytes() as u64;
        }
    }

    fn reused(&mut self, source: &SegmentSource) {
        self.segments_reused += 1;
        self.bytes_reused += source.bytes() as u64;
    }

    /// Records the wall and thread CPU time since `started`.
    pub fn finish(&mut self, started: &VerificationClock) {
        self.wall_micros = started.wall.elapsed().as_micros() as u64;
        self.cpu_micros = started
            .cpu
            .zip(crate::runtime::procmem::thread_cpu_nanos())
            .map(|(before, after)| after.saturating_sub(before) / 1_000);
    }
}

/// When a load began, by the wall clock and the loading thread's CPU clock.
pub struct VerificationClock {
    wall: std::time::Instant,
    cpu: Option<u64>,
}

impl VerificationClock {
    pub fn start() -> Self {
        Self {
            wall: std::time::Instant::now(),
            cpu: crate::runtime::procmem::thread_cpu_nanos(),
        }
    }
}

impl SegmentSource {
    /// A segment as a checked manifest publishes it, not yet verified.
    /// Loader-only: the loaders construct one and then verify or reuse it.
    pub fn published(
        path: PathBuf,
        rows: u64,
        row_bytes: u32,
        sha256: String,
        zero_from_row: u64,
    ) -> Self {
        Self {
            path,
            rows,
            row_bytes,
            sha256,
            zero_from_row,
            evidence: None,
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn rows(&self) -> u64 {
        self.rows
    }

    pub fn row_bytes(&self) -> u32 {
        self.row_bytes
    }

    /// SHA-256 of the file, in hex, as the manifest publishes it.
    pub fn sha256(&self) -> &str {
        &self.sha256
    }

    pub fn zero_from_row(&self) -> u64 {
        self.zero_from_row
    }

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
        self.verify_stamped().map(drop)
    }

    /// Verifies the segment and returns what was observed of its file, when
    /// that is settled enough to serve as evidence later.
    fn verify_stamped(&self) -> Result<Option<FileStamp>, LoadError> {
        self.verify_stamped_observing(|| {})
    }

    fn verify_stamped_observing(
        &self,
        mut after_read: impl FnMut(),
    ) -> Result<Option<FileStamp>, LoadError> {
        // Collecting an old publication removes hard links and changes ctime
        // even when its shared table bytes are untouched. Retry the whole hash,
        // never the stamp alone. Content errors and persistent churn still fail.
        for attempt in 0..3 {
            match self.verify_stamped_once(&mut after_read) {
                Err(LoadError::ChangedDuringVerification { .. }) if attempt < 2 => continue,
                result => return result,
            }
        }
        unreachable!("the final verification attempt returns its result")
    }

    fn verify_stamped_once(
        &self,
        after_read: &mut impl FnMut(),
    ) -> Result<Option<FileStamp>, LoadError> {
        let io = |source| LoadError::Io {
            path: self.path.clone(),
            source,
        };
        let started = std::time::SystemTime::now();
        let file = std::fs::File::open(&self.path).map_err(io)?;
        let before = file.metadata().map_err(io)?;
        let context = self.verification_context();
        let receipt = crate::verification::Receipt::new(&self.path, &file, &context);
        if receipt.as_ref().is_some_and(|r| r.matches()) {
            return Ok(FileStamp::of(&before));
        }
        let received = crate::verification::matches_received(
            &self.path,
            &file,
            &self.sha256,
            self.bytes() as u64,
        );
        let mut hasher = Sha256::new();
        let mut buffer = vec![0u8; 1 << 20];
        let mut total = 0usize;
        let zero_from = self.zero_from();
        let mut tail_data = false;
        loop {
            let read = (&file).read(&mut buffer).map_err(io)?;
            if read == 0 {
                break;
            }
            if !received {
                hasher.update(&buffer[..read]);
            }
            if total + read > zero_from {
                let skip = zero_from.saturating_sub(total);
                tail_data |= buffer[skip..read].iter().any(|&byte| byte != 0);
            }
            total += read;
        }
        crate::runtime::filecache::consumed(&file);
        if received {
            if total != self.bytes() {
                return Err(LoadError::Invalid(
                    "received segment length mismatch".into(),
                ));
            }
        } else {
            self.check(total, hasher.finalize().as_slice())?;
        }
        self.check_zero_tail(tail_data)?;
        after_read();
        let after = file.metadata().map_err(io)?;
        let named = std::fs::metadata(&self.path).map_err(io)?;
        let stamp = FileStamp::of(&before);
        if stamp.is_none() || stamp != FileStamp::of(&after) || stamp != FileStamp::of(&named) {
            return Err(LoadError::ChangedDuringVerification {
                path: self.path.clone(),
            });
        }
        if let Some(receipt) = receipt {
            receipt.record();
        }
        Ok(stamp.filter(|stamp| stamp.settled_before(started)))
    }

    /// Verifies the segment and keeps the evidence for later revisions.
    fn verify_into(
        mut self,
        report: &mut VerificationReport,
        page: bool,
    ) -> Result<Self, LoadError> {
        let file = std::fs::File::open(&self.path).map_err(|source| LoadError::Io {
            path: self.path.clone(),
            source,
        })?;
        let received = crate::verification::matches_received(
            &self.path,
            &file,
            &self.sha256,
            self.bytes() as u64,
        );
        self.evidence = self.verify_stamped()?;
        if received {
            report.reused(&self);
        } else {
            report.hashed(&self, page);
        }
        Ok(self)
    }

    /// This segment's verification, carried to `candidate`: the same
    /// committed segment published at another path. Only when this process
    /// holds evidence for the file and `candidate` resolves to that very
    /// file, unmodified since; otherwise `None` and the caller verifies.
    fn rebind(&self, candidate: &SegmentSource) -> Option<Self> {
        let evidence = self.evidence?;
        if self.sha256 != candidate.sha256
            || self.rows != candidate.rows
            || self.row_bytes != candidate.row_bytes
            || self.zero_from_row != candidate.zero_from_row
        {
            return None;
        }
        let meta = std::fs::metadata(&candidate.path).ok()?;
        (FileStamp::of(&meta)? == evidence).then(|| Self {
            path: candidate.path.clone(),
            evidence: Some(evidence),
            ..self.clone()
        })
    }

    /// Takes over earlier verification of the same committed segment, or
    /// verifies it. Used by every loader so no table has a second policy.
    pub fn verify_or_reuse<'a>(
        self,
        earlier: impl IntoIterator<Item = &'a SegmentSource>,
        report: &mut VerificationReport,
        page: bool,
    ) -> Result<Self, LoadError> {
        // Local receipts preserve a successful source check across publication
        // changes and restart. Received files are trusted after their network
        // hash; each use still checks the current file's byte size.
        let file = std::fs::File::open(&self.path).map_err(|source| LoadError::Io {
            path: self.path.clone(),
            source,
        })?;
        if crate::verification::Receipt::new(&self.path, &file, &self.verification_context())
            .is_some_and(|r| r.matches())
        {
            report.reused(&self);
            return Ok(self);
        }
        match earlier
            .into_iter()
            .find_map(|earlier| earlier.rebind(&self))
        {
            Some(reused) => {
                report.reused(&reused);
                Ok(reused)
            }
            None => self.verify_into(report, page),
        }
    }

    /// Reads and verifies the segment, for building a runtime from.
    ///
    /// The caller drops the buffer as soon as the runtime is built. Nothing
    /// retains it: the runtime holds the *encoded* database, and keeping the
    /// plaintext beside it would be the accounting error this whole change
    /// exists to remove.
    pub fn load(&self) -> Result<Vec<u8>, LoadError> {
        let file = std::fs::File::open(&self.path).map_err(|source| LoadError::Io {
            path: self.path.clone(),
            source,
        })?;
        let mut bytes = Vec::with_capacity(self.bytes());
        // One extra byte detects an oversized/replaced source without allowing
        // an untrusted file length to drive an unbounded allocation.
        let context = self.verification_context();
        let receipt = crate::verification::Receipt::new(&self.path, &file, &context);
        let inherited = receipt.as_ref().is_some_and(|r| r.matches());
        (&file)
            .by_ref()
            .take(self.bytes() as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|source| LoadError::Io {
                path: self.path.clone(),
                source,
            })?;
        crate::runtime::filecache::consumed(&file);
        if !inherited {
            if !crate::verification::matches_received(
                &self.path,
                &file,
                &self.sha256,
                self.bytes() as u64,
            ) {
                self.check(bytes.len(), Sha256::digest(&bytes).as_slice())?;
            } else if bytes.len() != self.bytes() {
                return Err(LoadError::Invalid(
                    "received segment length mismatch".into(),
                ));
            }
            self.check_zero_tail(bytes[self.zero_from()..].iter().any(|&byte| byte != 0))?;
            if let Some(receipt) = receipt {
                receipt.record();
            }
        } else if bytes.len() != self.bytes() {
            return Err(LoadError::Invalid(
                "received segment length mismatch".into(),
            ));
        }
        Ok(bytes)
    }

    fn verification_context(&self) -> Vec<u8> {
        serde_json::to_vec(&(
            "segment-v1",
            &self.sha256,
            self.rows,
            self.row_bytes,
            self.zero_from_row,
        ))
        .expect("segment identity serializes")
    }

    /// Byte offset of [`Self::zero_from_row`].
    fn zero_from(&self) -> usize {
        self.zero_from_row.min(self.rows) as usize * self.row_bytes as usize
    }

    fn check_zero_tail(&self, tail_data: bool) -> Result<(), LoadError> {
        if tail_data {
            return Err(LoadError::Invalid(format!(
                "{} holds data at or past row {}, which its queries do not select",
                self.path.display(),
                self.zero_from_row
            )));
        }
        Ok(())
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
#[derive(Clone)]
pub struct LoadedShard {
    pub manifest: ShardManifest,
    /// The manifest digest, which is also the directory name and the identity
    /// of this revision.
    pub digest: String,
    /// The manifest in its canonical serialization: the bytes `digest` is the
    /// hash of. Served as-is, so a wallet recomputing the digest from what it
    /// received gets the digest the map names.
    pub canonical_manifest: Vec<u8>,
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
    /// The page shards, in order.
    pub children: Vec<LoadedChild>,
}

/// One page shard's tables.
#[derive(Clone, Debug)]
pub struct LoadedChild {
    /// The page shard's chained identity ([`ShardManifest::child_digests`]),
    /// in hex. Runtimes are keyed by it, so a sealed page shard's runtime is
    /// shared by every revision of its directory shard.
    pub identity: String,
    /// Rows a query of this page shard selects over.
    pub query_rows: u64,
    /// One entry per segment; empty when nothing was pushed into it.
    pub segments: Vec<SegmentSource>,
}

impl LoadedShard {
    /// One segment of a table, or `None` if this shard has no such segment.
    pub fn segment(&self, table: TableRef, segment: u32) -> Option<&SegmentSource> {
        self.sources(table).get(segment as usize)
    }

    /// How many segments this shard's table has. Ordinarily one for the
    /// directory, and zero for a page shard nothing was pushed into.
    pub fn segments(&self, table: TableRef) -> u32 {
        self.sources(table).len() as u32
    }

    fn sources(&self, table: TableRef) -> &[SegmentSource] {
        match table {
            TableRef::Directory => &self.directory,
            TableRef::Pages(child) => self
                .children
                .get(child as usize)
                .map_or(&[], |child| child.segments.as_slice()),
        }
    }

    /// Every table this revision holds rows in: the directory, then each page
    /// shard with at least one segment.
    pub fn tables(&self) -> impl Iterator<Item = TableRef> + '_ {
        std::iter::once(TableRef::Directory).chain(
            self.children
                .iter()
                .enumerate()
                .filter(|(_, child)| !child.segments.is_empty())
                .map(|(index, _)| TableRef::Pages(index as u32)),
        )
    }

    /// What a table's runtimes are keyed by: the revision digest for the
    /// directory, the page shard's identity for a page shard.
    pub fn runtime_identity(&self, table: TableRef) -> &str {
        match table {
            TableRef::Directory => &self.digest,
            TableRef::Pages(child) => self
                .children
                .get(child as usize)
                .map_or(self.digest.as_str(), |child| child.identity.as_str()),
        }
    }

    /// Everything this revision's runtimes are keyed by: its digest and the
    /// identity of each of its page shards. What a disk-cache collection
    /// must keep for the revision to stay warm.
    pub fn runtime_identities(&self) -> Vec<String> {
        std::iter::once(self.digest.clone())
            .chain(self.children.iter().map(|child| child.identity.clone()))
            .collect()
    }

    /// Rows a query of `table` selects over, given the table's full rows.
    pub fn query_rows(&self, table: TableRef, full: usize) -> usize {
        match table {
            TableRef::Directory => full,
            TableRef::Pages(child) => self
                .children
                .get(child as usize)
                .map_or(full, |child| child.query_rows as usize),
        }
    }

    /// Reads and checks a revision directory's manifest and filter, without
    /// touching its tables.
    ///
    /// Everything about the revision's identity is settled here: the
    /// directory is named by the manifest digest, the schema is this build's,
    /// the geometry is registered, and the filter digests to what the
    /// manifest says. What is *not* settled is whether the tables are present
    /// and intact, which `verify_tables` does for the shards this process
    /// serves.
    pub fn read_manifest(dir: &Path) -> Result<RevisionMeta, LoadError> {
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
        if manifest.layout != transparent_shard::ManifestLayout::current() {
            return Err(LoadError::Invalid(
                "manifest layout disagrees with shard schema".into(),
            ));
        }
        // The page shard list is checked by every worker for every
        // manifest, held or not: it is what a wallet walks.
        if let Some(geometry) = geometry_by_name(&manifest.geometry) {
            manifest
                .check_children(geometry.page_rows, geometry.page_row_bytes as u32)
                .map_err(|error| {
                    LoadError::Invalid(format!("shard {}: {error}", manifest.shard_id))
                })?;
        }
        if manifest.tag_salt_counter > transparent_shard::tag::MAX_TAG_SALT_COUNTER {
            return Err(LoadError::Invalid(format!(
                "shard declares tag salt counter {}, above {}",
                manifest.tag_salt_counter,
                transparent_shard::tag::MAX_TAG_SALT_COUNTER
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

        // A published choice table must at least decode and index the shard's
        // placed scripts. Checked here, where every worker reads every
        // manifest, so a malformed one is refused before its manifest is
        // served; whether it routes each script to its row needs the tables,
        // which `verify_tables` checks where they are held.
        manifest
            .directory_choice()
            .map_err(|error| LoadError::Invalid(format!("shard {}: {error}", manifest.shard_id)))?;

        let filter = read(&dir.join("filter.bin"))?;
        if filter_hash(&filter).to_display_hex() != manifest.filter_hash {
            return Err(LoadError::Invalid(format!(
                "filter of shard {} does not match its manifest digest",
                manifest.shard_id
            )));
        }
        let canonical_manifest = manifest.canonical_bytes();
        Ok(RevisionMeta {
            manifest,
            digest,
            canonical_manifest,
            filter,
            geometry,
        })
    }

    /// Bytes this revision's tables occupy on disk, from the manifest.
    pub fn table_bytes(meta: &RevisionMeta) -> u64 {
        let geometry = meta.geometry;
        let directory = meta.manifest.directory_segments.len() as u64
            * geometry.directory_rows
            * geometry.directory_row_bytes as u64;
        let pages = meta
            .manifest
            .page_children
            .iter()
            .map(|child| child.page_segments.len() as u64)
            .sum::<u64>()
            * geometry.page_rows
            * geometry.page_row_bytes as u64;
        directory + pages
    }

    /// Verifies every table segment of a revision and produces the loaded shard.
    pub fn verify_tables(dir: &Path, meta: RevisionMeta) -> Result<Self, LoadError> {
        Self::verify_tables_reusing(
            dir,
            meta,
            &Evidence::default(),
            &mut VerificationReport::default(),
        )
    }

    /// Verifies a revision's tables, taking over any segment `evidence`
    /// already proved: the same committed table segment — runtime identity,
    /// kind, index, digest and geometry — published as the very file this
    /// process hashed, unmodified since. Everything else is hashed.
    fn verify_tables_reusing(
        dir: &Path,
        meta: RevisionMeta,
        evidence: &Evidence<'_>,
        report: &mut VerificationReport,
    ) -> Result<Self, LoadError> {
        let RevisionMeta {
            manifest,
            digest,
            canonical_manifest,
            filter,
            geometry,
        } = meta;
        // Every segment must match the named geometry, not just the first: a
        // segment with its own widths would silently need its own parameters,
        // and sharing one set per geometry is the whole point of naming it.
        let source = |table: TableRef,
                      identity: &str,
                      index: usize,
                      published: &transparent_shard::manifest::TableGeometry,
                      zero_from_row: u64,
                      report: &mut VerificationReport|
         -> Result<SegmentSource, LoadError> {
            let kind = table.kind();
            if published.rows != kind.rows(geometry)
                || published.row_bytes != kind.row_bytes(geometry)
            {
                return Err(LoadError::Invalid(format!(
                    "{} segment {index} of shard {} is {}x{} but geometry {} is {}x{}",
                    table.name(),
                    manifest.shard_id,
                    published.rows,
                    published.row_bytes,
                    geometry.name,
                    kind.rows(geometry),
                    kind.row_bytes(geometry),
                )));
            }
            SegmentSource::published(
                dir.join(format!("{}.{index}.bin", table.name())),
                published.rows,
                published.row_bytes,
                published.sha256.clone(),
                zero_from_row,
            )
            .verify_or_reuse(
                evidence.get(identity, kind, index as u32).iter().copied(),
                report,
                kind == Table::Pages,
            )
        };
        if manifest.directory_segments.is_empty() {
            return Err(LoadError::Invalid(format!(
                "shard {} declares no directory segments",
                manifest.shard_id
            )));
        }
        let hashed_before = report.segments_hashed;
        let directory = manifest
            .directory_segments
            .iter()
            .enumerate()
            .map(|(index, published)| {
                source(
                    TableRef::Directory,
                    &digest,
                    index,
                    published,
                    published.rows,
                    report,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        // A directory taken over whole was proved under this very manifest,
        // choice table included, so its routes were checked then.
        let directory_reused = report.segments_hashed == hashed_before && evidence.holds(&digest);
        let identities = manifest.child_digests();
        let mut children = Vec::with_capacity(manifest.page_children.len());
        for (child, identity) in manifest.page_children.iter().zip(&identities) {
            let query_rows = child.query_rows();
            let identity = hex::encode(identity);
            let segments = child
                .page_segments
                .iter()
                .enumerate()
                .map(|(index, published)| {
                    source(
                        TableRef::Pages(child.index),
                        &identity,
                        index,
                        published,
                        query_rows,
                        report,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            children.push(LoadedChild {
                identity,
                query_rows,
                segments,
            });
        }
        if !directory_reused {
            verify_choice_routes(&manifest, geometry, &directory, report)?;
        }

        Ok(Self {
            manifest,
            digest,
            canonical_manifest,
            geometry,
            filter,
            directory,
            children,
        })
    }
}

/// Verification this process already holds, indexed by committed segment.
///
/// Borrowed from the publication the candidate replaces, so it lives exactly
/// as long as the held publication that proved it and is bounded by what
/// that publication retains. Nothing of it is persisted: a restarted process
/// has none and verifies every table.
#[derive(Default)]
pub(crate) struct Evidence<'a> {
    /// Every held source of each committed segment: a sealed page shard is
    /// held once per revision that publishes it, possibly as different files.
    segments: std::collections::HashMap<(String, Table, u32), Vec<&'a SegmentSource>>,
    revisions: std::collections::HashSet<&'a str>,
}

impl<'a> Evidence<'a> {
    fn of(set: Option<&'a ShardSet>) -> Self {
        let mut evidence = Self::default();
        for shard in set.into_iter().flat_map(|set| set.revisions.iter()) {
            evidence.revisions.insert(&shard.digest);
            for table in shard.tables() {
                let identity = shard.runtime_identity(table);
                let sources = shard.sources(table);
                for (index, source) in sources.iter().enumerate() {
                    evidence
                        .segments
                        .entry((identity.to_string(), table.kind(), index as u32))
                        .or_default()
                        .push(source);
                }
            }
        }
        evidence
    }

    fn get(&self, identity: &str, kind: Table, index: u32) -> &[&'a SegmentSource] {
        self.segments
            .get(&(identity.to_string(), kind, index))
            .map_or(&[], Vec::as_slice)
    }

    fn holds(&self, digest: &str) -> bool {
        self.revisions.contains(digest)
    }
}

/// Checks that a published choice table names the row holding every entry.
///
/// A wrong bit fails silently at the wallet: it queries the other candidate
/// row, finds no entry, and reads the script as absent. So a shard whose table
/// misroutes any entry, or indexes a different number of entries than the
/// directory holds, is refused before it is served.
fn verify_choice_routes(
    manifest: &ShardManifest,
    geometry: &Geometry,
    directory: &[SegmentSource],
    report: &mut VerificationReport,
) -> Result<(), LoadError> {
    let invalid = |why: String| LoadError::Invalid(format!("shard {}: {why}", manifest.shard_id));
    let Some(table) = manifest
        .directory_choice()
        .map_err(|error| invalid(error.to_string()))?
    else {
        return Ok(());
    };
    let mut entries = 0u64;
    let mut tags = std::collections::HashSet::new();
    for (segment, source) in directory.iter().enumerate() {
        let bytes = source.load()?;
        report.bytes_hashed += bytes.len() as u64;
        for (within, raw) in bytes.chunks(geometry.directory_row_bytes).enumerate() {
            let row = segment as u64 * geometry.directory_rows + within as u64;
            let decoded = transparent_shard::records::decode_directory_row(
                raw,
                manifest.page_children.len() as u32,
            )
            .map_err(|error| invalid(format!("directory row {row}: {error}")))?;
            // Rows carry script tags, not raw scripts, so the route each
            // script took cannot be recomputed here. The builder checks every
            // route against the encoded rows before publication, while it
            // still has the scripts. This pass decodes every row, refuses a
            // tag held twice anywhere in the shard, and counts entries
            // against the table.
            for entry in &decoded {
                if !tags.insert(entry.tag) {
                    return Err(invalid(format!(
                        "directory row {row} repeats script tag {}",
                        hex::encode(entry.tag)
                    )));
                }
            }
            entries += decoded.len() as u64;
        }
    }
    if entries != u64::from(table.keys()) {
        return Err(invalid(format!(
            "directory choice indexes {} scripts, the directory holds {entries}",
            table.keys()
        )));
    }
    Ok(())
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
    /// The current revision's manifest and filter for every shard the map
    /// names, tables or not. What the public routes are answered from.
    metadata: BTreeMap<String, RevisionMeta>,
    /// This worker's place in the assignment, when it has one.
    scope: Option<WorkerScope>,
    /// Superseded revisions past the retention bound, when pruning is allowed.
    prunable: Vec<PrunableRevision>,
    /// What loading this set spent on verification.
    verification: VerificationReport,
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
        Self::open_with(dir, &LoadOptions::whole(retain_revisions))
    }

    /// Loads the set under explicit options; see [`LoadOptions`].
    ///
    /// Under an assignment, every directory's manifest and filter are read
    /// and checked, the map is checked against the assignment, and tables are
    /// verified only for assigned shards. Unassigned shards' table files may
    /// be absent; their manifests and filters may not.
    pub fn open_with(dir: &Path, options: &LoadOptions) -> Result<Self, LoadError> {
        Self::open_reusing(dir, options, None)
    }

    /// Loads the set, taking over verification `previous` holds for any
    /// table segment the candidate publishes unchanged as the same file.
    ///
    /// Per segment, not per revision: a republished tail is a new revision
    /// whose sealed page shards are byte-identical and hard-linked from the
    /// one before, and those are not hashed again. Merely claiming the same
    /// digest never skips verification; see [`FileStamp`].
    pub fn open_reusing(
        dir: &Path,
        options: &LoadOptions,
        previous: Option<&Self>,
    ) -> Result<Self, LoadError> {
        let clock = VerificationClock::start();
        let evidence = Evidence::of(previous);
        let mut verification = VerificationReport::default();
        let raw = read(&dir.join("shards.json"))?;
        let map: ShardMap = serde_json::from_slice(&raw).map_err(|source| LoadError::Json {
            path: dir.join("shards.json"),
            source,
        })?;
        map.validate_supported()
            .map_err(|error| LoadError::Invalid(format!("shard map is malformed: {error}")))?;

        // Serialized here, once, so what is served and what is digested are the
        // same bytes.
        let map_json = serde_json::to_vec(&map).map_err(|source| LoadError::Json {
            path: dir.join("shards.json"),
            source,
        })?;
        let map_digest = hex::encode(Sha256::digest(&map_json));

        // The assignment is checked against the map before any directory is
        // read: a worker given the wrong assignment must not spend minutes
        // verifying tables it will then refuse to serve.
        let scope = match &options.scope {
            LoadScope::Whole => None,
            LoadScope::Assigned {
                assignment,
                worker_id,
            } => {
                assignment
                    .check_against(&map, &map_digest)
                    .map_err(|error| LoadError::Invalid(error.to_string()))?;
                Some(
                    assignment
                        .scope_for(worker_id)
                        .map_err(|error| LoadError::Invalid(error.to_string()))?,
                )
            }
        };
        let assigned = |shard_id: u64| match &scope {
            None => true,
            Some(scope) => scope.assigned.contains(&shard_id),
        };

        // Index by manifest digest, which is the identity of a *revision*.
        // Indexing by shard id would make a republished tail — old and new
        // revision directories side by side, which is exactly what the
        // publisher writes — look like two directories claiming one shard, and
        // refuse to start on a set the publisher considers well formed.
        let mut by_digest: BTreeMap<String, (PathBuf, RevisionMeta)> = BTreeMap::new();
        for entry in std::fs::read_dir(dir).map_err(|source| LoadError::Io {
            path: dir.to_path_buf(),
            source,
        })? {
            let entry = entry.map_err(|source| LoadError::Io {
                path: dir.to_path_buf(),
                source,
            })?;
            let path = entry.path();
            if path.is_symlink() {
                // Fleet publications may reference a revision stored once in
                // this worker's sibling catalog. Do not accept redirects out
                // of that catalog or through another symlink.
                let name = entry.file_name();
                let name = name.to_string_lossy();
                let catalog = dir.parent().unwrap_or(dir).join(".sources");
                let target = catalog.join(name.as_ref());
                let expected = Path::new("../.sources").join(name.as_ref());
                if name.len() != 64
                    || !name
                        .bytes()
                        .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
                    || std::fs::read_link(&path).ok().as_ref() != Some(&expected)
                    || !std::fs::symlink_metadata(&catalog).is_ok_and(|m| m.is_dir())
                    || !std::fs::symlink_metadata(&target).is_ok_and(|m| m.is_dir())
                {
                    return Err(LoadError::Invalid(format!(
                        "invalid revision catalog link: {}",
                        path.display()
                    )));
                }
            } else if !path.is_dir() {
                continue;
            }
            let meta = LoadedShard::read_manifest(&path)?;
            // A digest names its own content, so two directories cannot hold
            // the same digest and different bytes. One filesystem path per
            // digest is still required, so the source of a revision is
            // unambiguous.
            if by_digest
                .insert(meta.digest.clone(), (entry.path(), meta))
                .is_some()
            {
                return Err(LoadError::Invalid(format!(
                    "two directories in {} hold the same manifest digest",
                    dir.display()
                )));
            }
        }

        let mut revisions: Vec<LoadedShard> = Vec::with_capacity(map.shards.len());
        let mut current: BTreeMap<u64, usize> = BTreeMap::new();
        let mut metadata: BTreeMap<String, RevisionMeta> = BTreeMap::new();
        let mut current_revision: BTreeMap<u64, (u32, bool)> = BTreeMap::new();
        let mut parent_digest = String::new();
        for entry in &map.shards {
            let (path, meta) = by_digest.remove(&entry.manifest_digest).ok_or_else(|| {
                LoadError::Invalid(format!(
                    "the map names revision {} of shard {} but it is absent",
                    entry.manifest_digest, entry.shard_id
                ))
            })?;
            let manifest = &meta.manifest;
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
                // The map counts a directory shard's page shards where v11
                // counted page segments.
                || manifest.page_children.len() as u32 != entry.page_segments
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
            parent_digest = meta.digest.clone();
            current_revision.insert(entry.shard_id, (manifest.revision, manifest.sealed));
            if assigned(entry.shard_id) {
                let shard =
                    LoadedShard::verify_tables_reusing(&path, meta, &evidence, &mut verification)?;
                metadata.insert(
                    shard.digest.clone(),
                    RevisionMeta {
                        manifest: shard.manifest.clone(),
                        digest: shard.digest.clone(),
                        canonical_manifest: shard.canonical_manifest.clone(),
                        filter: shard.filter.clone(),
                        geometry: shard.geometry,
                    },
                );
                current.insert(entry.shard_id, revisions.len());
                revisions.push(shard);
            } else {
                metadata.insert(meta.digest.clone(), meta);
            }
        }

        // What is left is either a superseded revision of a shard the map names
        // — which is retained so a wallet mid-sync can still be answered from
        // the revision it fetched setup for — or something that does not belong
        // in this set at all. Under an assignment, superseded revisions of
        // unassigned shards are simply not this worker's concern.
        struct Superseded {
            revision: u32,
            bytes: u64,
            digest: String,
            path: PathBuf,
            meta: RevisionMeta,
        }
        let mut retained: BTreeMap<u64, Vec<Superseded>> = BTreeMap::new();
        for (digest, (path, meta)) in by_digest {
            let shard_id = meta.manifest.shard_id;
            let Some(&(published_revision, published_sealed)) = current_revision.get(&shard_id)
            else {
                return Err(LoadError::Invalid(format!(
                    "revision {digest} is on disk but shard {shard_id} is not in the map"
                )));
            };
            // A revision at or past the current one is not a predecessor. It is
            // either the same content under a second digest or a publication
            // the map has not caught up with, and serving either would mean
            // answering for a revision no wallet was told about.
            if meta.manifest.revision >= published_revision {
                return Err(LoadError::Invalid(format!(
                    "revision {} of shard {shard_id} is on disk but the map names revision {}",
                    meta.manifest.revision, published_revision
                )));
            }
            // A sealed shard is final, so it has no superseded revisions to
            // retain; one on disk means the set was assembled from two
            // different partitions of the chain.
            if published_sealed && meta.manifest.sealed {
                return Err(LoadError::Invalid(format!(
                    "shard {shard_id} is sealed but has a second sealed revision on disk"
                )));
            }
            if !assigned(shard_id) {
                continue;
            }
            let bytes = LoadedShard::table_bytes(&meta) + meta.filter.len() as u64;
            retained.entry(shard_id).or_default().push(Superseded {
                revision: meta.manifest.revision,
                bytes,
                digest,
                path,
                meta,
            });
        }

        // Newest first; the bound keeps what a lagging wallet is likeliest to
        // still hold and lets the oldest go.
        let mut prunable = Vec::new();
        for (shard_id, mut held) in retained {
            held.sort_by_key(|entry| std::cmp::Reverse(entry.revision));
            let mut kept = 0usize;
            let mut kept_bytes = 0u64;
            for Superseded {
                revision,
                bytes,
                digest,
                path,
                meta,
            } in held
            {
                let within = kept < options.retain_revisions
                    && options
                        .retain_bytes
                        .is_none_or(|bound| kept_bytes + bytes <= bound);
                if within {
                    kept += 1;
                    kept_bytes += bytes;
                    let shard = LoadedShard::verify_tables_reusing(
                        &path,
                        meta,
                        &evidence,
                        &mut verification,
                    )?;
                    revisions.push(shard);
                } else if options.prune_excess {
                    prunable.push(PrunableRevision {
                        path,
                        shard_id,
                        revision,
                        digest,
                        bytes,
                    });
                } else {
                    return Err(LoadError::Invalid(format!(
                        "shard {shard_id} has more than {} superseded revisions on disk; \
                         prune the set or raise --retain-revisions",
                        options.retain_revisions
                    )));
                }
            }
        }

        let by_digest = revisions
            .iter()
            .enumerate()
            .map(|(index, shard)| (shard.digest.clone(), index))
            .collect();

        verification.finish(&clock);
        Ok(Self {
            map,
            map_json,
            map_digest,
            revisions,
            current,
            by_digest,
            metadata,
            scope,
            prunable,
            verification,
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
        self.map.shards.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.shards.is_empty()
    }

    /// Shards this process holds tables for.
    pub fn assigned_len(&self) -> usize {
        self.current.len()
    }

    /// The current revision of every shard this process holds tables for.
    pub fn current(&self) -> impl Iterator<Item = &LoadedShard> {
        self.current
            .values()
            .map(move |&index| &self.revisions[index])
    }

    /// Whether this process holds tables for `shard_id`.
    pub fn is_assigned(&self, shard_id: u64) -> bool {
        self.current.contains_key(&shard_id)
    }

    /// Whether the map names `shard_id` at all.
    pub fn names(&self, shard_id: u64) -> bool {
        (shard_id as usize) < self.map.shards.len()
    }

    /// The current revision's manifest and filter for any shard the map
    /// names, assigned or not.
    pub fn meta(&self, shard_id: u64) -> Option<&RevisionMeta> {
        self.map
            .shards
            .get(shard_id as usize)
            .and_then(|entry| self.metadata.get(&entry.manifest_digest))
    }

    /// The manifest and filter of a revision by digest: the current revision
    /// of any shard, or a superseded revision this process holds.
    pub fn meta_by_digest(&self, digest: &str) -> Option<(&ShardManifest, &[u8], &[u8])> {
        if let Some(meta) = self.metadata.get(digest) {
            return Some((&meta.manifest, &meta.canonical_manifest, &meta.filter));
        }
        self.revision(digest).map(|shard| {
            (
                &shard.manifest,
                shard.canonical_manifest.as_slice(),
                shard.filter.as_slice(),
            )
        })
    }

    /// This worker's place in the assignment, if it loaded under one.
    pub fn scope(&self) -> Option<&WorkerScope> {
        self.scope.as_ref()
    }

    /// What loading this set spent proving its tables, and what it took
    /// over from the publication it replaced.
    pub fn verification(&self) -> &VerificationReport {
        &self.verification
    }

    /// Superseded revisions the retention bound does not cover.
    pub fn prunable(&self) -> &[PrunableRevision] {
        &self.prunable
    }

    /// Every runtime this worker should hold warm, in the order to build
    /// them: current revisions of assigned shards, newest shard first for a
    /// recent replica so the tail wallets hit hardest is ready first, then
    /// retained superseded revisions.
    pub fn warm_targets(&self) -> Vec<(String, TableRef, u32)> {
        let mut current: Vec<&LoadedShard> =
            self.current.values().map(|&i| &self.revisions[i]).collect();
        match self.scope.as_ref().map(|scope| scope.role) {
            Some(WorkerRole::RecentReplica) => {
                current.sort_by_key(|entry| std::cmp::Reverse(entry.manifest.shard_id))
            }
            _ => current.sort_by_key(|shard| shard.manifest.shard_id),
        }
        let current_digests: BTreeSet<&str> = current.iter().map(|s| s.digest.as_str()).collect();
        let mut superseded: Vec<&LoadedShard> = self
            .revisions
            .iter()
            .filter(|shard| !current_digests.contains(shard.digest.as_str()))
            .collect();
        superseded.sort_by(|a, b| {
            b.manifest
                .revision
                .cmp(&a.manifest.revision)
                .then(b.manifest.shard_id.cmp(&a.manifest.shard_id))
        });
        let mut targets = Vec::new();
        // A sealed page shard is shared by every revision of its directory
        // shard; its runtimes are warmed once.
        let mut seen = std::collections::HashSet::new();
        for shard in current.into_iter().chain(superseded) {
            for table in shard.tables() {
                if !seen.insert((shard.runtime_identity(table).to_string(), table.kind())) {
                    continue;
                }
                for segment in 0..shard.segments(table) {
                    targets.push((shard.digest.clone(), table, segment));
                }
            }
        }
        targets
    }

    /// Every revision held, current and superseded.
    pub fn revisions(&self) -> &[LoadedShard] {
        &self.revisions
    }

    /// Every geometry this set uses, in registry order.
    ///
    /// What the service builds parameter sets for: every geometry the map
    /// names, whether or not this worker holds tables for it. The init
    /// document is set-wide — a wallet fetches it once, from whichever worker
    /// the edge picks, and refuses a map naming a geometry the document does
    /// not declare — so a replica assigned only the recent tier still declares
    /// the archive's parameters. They are a function of the geometry alone
    /// and say nothing about which shards this process serves; the
    /// assignment says that. Not every registered geometry, though: a shape
    /// no shard in the set uses stays undeclared.
    pub fn geometries(&self) -> Vec<&'static Geometry> {
        let mut seen: Vec<&'static Geometry> = Vec::new();
        let held = self.revisions.iter().map(|shard| shard.geometry);
        let named = self.metadata.values().map(|meta| meta.geometry);
        for geometry in held.chain(named) {
            if !seen.iter().any(|known| known.name == geometry.name) {
                seen.push(geometry);
            }
        }
        seen.sort_by_key(|geometry| geometry.name);
        seen
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn small_source(dir: &Path) -> SegmentSource {
        let path = dir.join("source.bin");
        let bytes = vec![0u8; 32];
        std::fs::write(&path, &bytes).unwrap();
        SegmentSource::published(path, 2, 16, hex::encode(Sha256::digest(&bytes)), 2)
    }

    /// Waits until a change made now gets a later ctime than `path` has.
    ///
    /// Filesystems take ctime from a coarse clock, so an unlink or write in
    /// the tick of the previous change leaves the stamp unchanged.
    fn next_timestamp_tick(path: &Path) {
        use std::os::unix::fs::MetadataExt;
        let ctime = |path: &Path| {
            let meta = std::fs::metadata(path).unwrap();
            (meta.ctime(), meta.ctime_nsec())
        };
        let last = ctime(path);
        let probe = path.with_extension("tick");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let _ = std::fs::remove_file(&probe);
            std::fs::write(&probe, []).unwrap();
            if ctime(&probe) > last {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "filesystem timestamp did not advance within five seconds"
            );
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    #[test]
    fn collecting_a_hard_link_rechecks_the_complete_source() {
        let dir = tempfile::tempdir().unwrap();
        let source = small_source(dir.path());
        let link = dir.path().join("retired.bin");
        std::fs::hard_link(source.path(), &link).unwrap();
        let mut reads = 0;
        let evidence = source
            .verify_stamped_observing(|| {
                reads += 1;
                if reads == 1 {
                    next_timestamp_tick(source.path());
                    std::fs::remove_file(&link).unwrap();
                }
            })
            .unwrap();
        assert_eq!(
            reads, 2,
            "a changed identity requires another complete hash"
        );
        assert!(
            evidence.is_none(),
            "a freshly changed file cannot supply reusable evidence"
        );
    }

    #[test]
    fn a_changed_row_is_rejected_after_a_verification_race() {
        let dir = tempfile::tempdir().unwrap();
        let source = small_source(dir.path());
        let mut reads = 0;
        let result = source.verify_stamped_observing(|| {
            reads += 1;
            let mut changed = [0u8; 32];
            changed[0] = 1;
            next_timestamp_tick(source.path());
            std::fs::write(source.path(), changed).unwrap();
        });
        assert!(
            matches!(result, Err(LoadError::Invalid(_))),
            "the second hash must reject changed bytes"
        );
        assert_eq!(
            reads, 1,
            "a digest mismatch fails before another successful read"
        );
    }

    #[test]
    fn persistent_file_identity_changes_exhaust_the_verification_bound() {
        let dir = tempfile::tempdir().unwrap();
        let source = small_source(dir.path());
        let link = dir.path().join("retired.bin");
        let mut reads = 0;
        let result = source.verify_stamped_observing(|| {
            reads += 1;
            next_timestamp_tick(source.path());
            if link.exists() {
                std::fs::remove_file(&link).unwrap();
            } else {
                std::fs::hard_link(source.path(), &link).unwrap();
            }
        });
        assert!(matches!(
            result,
            Err(LoadError::ChangedDuringVerification { .. })
        ));
        assert_eq!(reads, 3, "verification must not retry indefinitely");
    }

    #[test]
    fn restoring_modification_time_does_not_hide_a_changed_source() {
        use std::fs::File;
        use std::os::unix::fs::FileExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.bin");
        let bytes = vec![0u8; 32];
        std::fs::write(&path, &bytes).unwrap();
        let file = File::options().write(true).open(&path).unwrap();
        let modified = std::time::SystemTime::now() - std::time::Duration::from_secs(60);
        file.set_modified(modified).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(1100));
        let source =
            SegmentSource::published(path.clone(), 2, 16, hex::encode(Sha256::digest(&bytes)), 2)
                .verify_into(&mut VerificationReport::default(), true)
                .unwrap();
        assert!(source.evidence.is_some());
        file.write_all_at(&[1], 0).unwrap();
        file.set_modified(modified).unwrap();
        let candidate = SegmentSource::published(path, 2, 16, source.sha256.clone(), 2);
        assert!(candidate.verify_or_reuse(
            Some(&source), &mut VerificationReport::default(), true,
        ).is_err(), "a changed source must be hashed and rejected even if its modification time was restored");
    }

    /// A segment whose rows past the ones its queries select hold data is refused
    /// on verification and on load, so no runtime is built or restored from it.
    #[test]
    fn a_segment_with_data_past_its_selected_rows_is_refused() {
        use sha2::Digest;
        use zakura_pir_query_server::transparent::native as transparent_native;
        let dir = tempfile::tempdir().unwrap();
        let (rows, row_bytes) = (2 * transparent_native::D as u64, 16u32);
        let mut bytes = vec![0u8; rows as usize * row_bytes as usize];
        bytes[7] = 1;
        let source = |bytes: &[u8], zero_from_row| {
            let path = dir.path().join("pages.0.bin");
            std::fs::write(&path, bytes).unwrap();
            SegmentSource::published(
                path,
                rows,
                row_bytes,
                hex::encode(sha2::Sha256::digest(bytes)),
                zero_from_row,
            )
        };
        let half = transparent_native::D as u64;
        assert!(source(&bytes, half).verify().is_ok());
        assert!(source(&bytes, half).load().is_ok());
        bytes[half as usize * row_bytes as usize] = 1;
        assert!(source(&bytes, half).verify().is_err());
        assert!(source(&bytes, half).load().is_err());
        assert!(source(&bytes, rows).verify().is_ok());
    }
}
