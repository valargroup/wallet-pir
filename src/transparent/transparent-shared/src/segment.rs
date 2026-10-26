//! Immutable table segments and reusable verification evidence.
use pir_publication::FileIdentity;
use sha2::{Digest, Sha256};
use std::{
    io::Read,
    path::{Path, PathBuf},
};

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
    /// The caller asked verification to stop. Nothing was proved and no
    /// evidence or receipt was recorded for the file being read.
    #[error("verification stopped")]
    Stopped,
}

/// Asked between bounded reads and before each unit of verification work;
/// once it answers true the work ends with [`LoadError::Stopped`].
pub type Stop<'a> = &'a (dyn Fn() -> bool + Sync);

/// A [`Stop`] that never stops, for callers without a stop request.
pub const NEVER: Stop<'static> = &|| false;

/// Where one segment's bytes are, and what they must be.
///
/// Publisher loads verify source contents; serving loads check only length.
/// The manifest digest remains the runtime identity in both cases.
#[derive(Clone, Debug)]
pub struct SegmentSource {
    pub(crate) path: PathBuf,
    pub(crate) rows: u64,
    pub(crate) row_bytes: u32,
    /// SHA-256 of the file, in hex, as the manifest publishes it.
    pub(crate) sha256: String,
    /// The first row a query may omit, from which every byte must be zero:
    /// a page shard's [`query_rows`], or `rows` when queries select every
    /// row. Checked wherever the digest is, so no runtime is built or
    /// restored from a segment whose omitted rows hold data.
    ///
    /// [`query_rows`]: zakura_pir_protocol::transparent::shard::manifest::PageChild::query_rows
    pub(crate) zero_from_row: u64,
    /// What this process observed of the file the last time it hashed it,
    /// when that observation is certain enough to vouch for the file later;
    /// see [`FileIdentity`]. This content evidence is process-local.
    pub(crate) evidence: Option<FileIdentity>,
    /// Local publisher preparation, never proof of a hash by this process.
    pub(crate) preparation: Option<FileIdentity>,
    pub(crate) size_only: bool,
}

#[cfg(test)]
thread_local! {
    /// Source loads on this thread that streamed the bytes through SHA-256.
    pub(crate) static LOADS_HASHED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// The directory beside a publisher's publications that stores each table
/// segment and prepared runtime once, named by the SHA-256 of its bytes.
pub const OBJECT_STORE: &str = ".objects";

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
    pub(crate) fn hashed(&mut self, source: &SegmentSource, page: bool) {
        self.segments_hashed += 1;
        self.bytes_hashed += source.bytes() as u64;
        if page {
            self.page_bytes_hashed += source.bytes() as u64;
        }
    }

    pub(crate) fn reused(&mut self, source: &SegmentSource) {
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
            preparation: None,
            size_only: false,
        }
    }

    /// Trust publisher preparation and check only the local file's size.
    pub fn for_serving(mut self) -> Result<Self, LoadError> {
        let opened = if self.check_object_link()? {
            std::fs::File::open(&self.path)
        } else {
            crate::verification::open(&self.path)
        };
        let file = opened.map_err(|source| LoadError::Io {
            path: self.path.clone(),
            source,
        })?;
        if file
            .metadata()
            .map_err(|source| LoadError::Io {
                path: self.path.clone(),
                source,
            })?
            .len()
            != self.bytes() as u64
        {
            return Err(LoadError::Invalid("source segment length mismatch".into()));
        }
        self.size_only = true;
        Ok(self)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Whether this segment's file is a link into the publisher's segment
    /// [`OBJECT_STORE`], refusing any link but the exact one: from
    /// `<publication>/<digest>/<file>` to `../../.objects/<sha256>` for this
    /// segment's own digest, with a real store and a regular object. A
    /// publication stores each segment once and links it from every revision
    /// that publishes it, so republishing never changes the object's inode.
    pub(crate) fn check_object_link(&self) -> Result<bool, LoadError> {
        let io = |source| LoadError::Io {
            path: self.path.clone(),
            source,
        };
        if !std::fs::symlink_metadata(&self.path)
            .map_err(io)?
            .file_type()
            .is_symlink()
        {
            return Ok(false);
        }
        let refused = || {
            LoadError::Invalid(format!(
                "invalid segment object link: {}",
                self.path.display()
            ))
        };
        let digest = self.sha256.len() == 64
            && self
                .sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));
        let expected = Path::new("../..").join(OBJECT_STORE).join(&self.sha256);
        if !digest || std::fs::read_link(&self.path).map_err(io)? != expected {
            return Err(refused());
        }
        let store = self
            .path
            .parent()
            .and_then(Path::parent)
            .and_then(Path::parent)
            .ok_or_else(refused)?
            .join(OBJECT_STORE);
        let real = |path: &Path, dir: bool| {
            std::fs::symlink_metadata(path).is_ok_and(|m| {
                !m.file_type().is_symlink() && if dir { m.is_dir() } else { m.is_file() }
            })
        };
        if !real(&store, true) || !real(&store.join(&self.sha256), false) {
            return Err(refused());
        }
        Ok(true)
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
        self.verify_stamped(NEVER).map(drop)
    }

    /// Verifies the segment and returns what was observed of its file, when
    /// that is settled enough to serve as evidence later.
    fn verify_stamped(&self, stop: Stop<'_>) -> Result<Option<FileIdentity>, LoadError> {
        self.verify_stamped_observing(|| {}, stop)
    }

    fn verify_stamped_observing(
        &self,
        mut after_read: impl FnMut(),
        stop: Stop<'_>,
    ) -> Result<Option<FileIdentity>, LoadError> {
        // Collecting an old publication removes hard links and changes ctime
        // even when its shared table bytes are untouched. Retry the whole hash,
        // never the stamp alone. Content errors and persistent churn still fail.
        for attempt in 0..3 {
            match self.verify_stamped_once(&mut after_read, stop) {
                Err(LoadError::ChangedDuringVerification { .. }) if attempt < 2 => continue,
                result => return result,
            }
        }
        unreachable!("the final verification attempt returns its result")
    }

    fn verify_stamped_once(
        &self,
        after_read: &mut impl FnMut(),
        stop: Stop<'_>,
    ) -> Result<Option<FileIdentity>, LoadError> {
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
            return Ok(FileIdentity::from_metadata(&before));
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
            // Checked before each bounded read: a stop leaves the file
            // unproved, with no receipt or evidence recorded for it.
            if stop() {
                return Err(LoadError::Stopped);
            }
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
        let stamp = FileIdentity::from_metadata(&before);
        if stamp.is_none()
            || stamp != FileIdentity::from_metadata(&after)
            || stamp != FileIdentity::from_metadata(&named)
        {
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
        stop: Stop<'_>,
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
        self.evidence = self.verify_stamped(stop)?;
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
        let evidence = self.evidence.or(self.preparation)?;
        if self.sha256 != candidate.sha256
            || self.rows != candidate.rows
            || self.row_bytes != candidate.row_bytes
            || self.zero_from_row != candidate.zero_from_row
        {
            return None;
        }
        candidate.check_object_link().ok()?;
        let physical = std::fs::canonicalize(&candidate.path).ok()?;
        let file = std::fs::File::open(&physical).ok()?;
        evidence.check(&physical, &file).ok()?;
        Some(Self {
            path: candidate.path.clone(),
            ..self.clone()
        })
    }

    /// Takes over earlier verification of the same committed segment, or
    /// verifies it. Used by every loader so no table has a second policy.
    /// `stop` is asked before each bounded read of a hash.
    pub fn verify_or_reuse<'a>(
        self,
        earlier: impl IntoIterator<Item = &'a SegmentSource>,
        report: &mut VerificationReport,
        page: bool,
        stop: Stop<'_>,
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
            None => self.verify_into(report, page, stop),
        }
    }

    /// Reads and verifies the segment, for building a runtime from.
    ///
    /// The caller drops the buffer as soon as the runtime is built. Nothing
    /// retains it: the runtime holds the *encoded* database, and keeping the
    /// plaintext beside it would be the accounting error this whole change
    /// exists to remove.
    pub fn load(&self) -> Result<Vec<u8>, LoadError> {
        let io = |source| LoadError::Io {
            path: self.path.clone(),
            source,
        };
        let file = std::fs::File::open(&self.path).map_err(io)?;
        let before = file.metadata().map_err(io)?;
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
            .map_err(io)?;
        crate::runtime::filecache::consumed(&file);
        // Publisher evidence can avoid a second hash; serving loads trust the
        // publisher and check only the length, even when evidence is present.
        let vouched = !self.size_only && !inherited && self.vouches_for(&file, &before);
        if self.size_only {
            if bytes.len() != self.bytes() {
                return Err(LoadError::Invalid("source segment length mismatch".into()));
            }
        } else if vouched {
            if bytes.len() != self.bytes() {
                return Err(LoadError::Invalid("verified segment length changed".into()));
            }
            self.check_zero_tail(bytes[self.zero_from()..].iter().any(|&byte| byte != 0))?;
        } else if !inherited {
            #[cfg(test)]
            LOADS_HASHED.with(|n| n.set(n.get() + 1));
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

    /// Check `bytes`, read from this segment's file, against its manifest
    /// length and digest and its zero tail, whatever evidence it carries.
    pub(crate) fn verify_bytes(&self, bytes: &[u8]) -> Result<(), LoadError> {
        self.check(bytes.len(), Sha256::digest(bytes).as_slice())?;
        self.check_zero_tail(bytes[self.zero_from()..].iter().any(|&byte| byte != 0))
    }

    /// What this process observed of the file when it verified it, if that
    /// is settled enough to vouch for the same unchanged file later.
    pub(crate) fn evidence(&self) -> Option<FileIdentity> {
        self.evidence
    }

    pub(crate) fn preparation_identity(&self) -> Option<FileIdentity> {
        self.evidence.or(self.preparation)
    }

    /// Whether this process's evidence vouches for `file`, opened at this
    /// segment's path and observed as `before` when it was opened: the same
    /// inode, unmodified since it was hashed, both before and now, and still
    /// what the path names. Without settled evidence the answer is `false`.
    pub(crate) fn vouches_for(&self, file: &std::fs::File, before: &std::fs::Metadata) -> bool {
        let Some(evidence) = self.evidence else {
            return false;
        };
        FileIdentity::from_metadata(before) == Some(evidence)
            && before.len() == self.bytes() as u64
            && evidence.check(&self.path, file).is_ok()
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

    /// Byte offset of the first row whose bytes must be zero.
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

#[cfg(test)]
mod tests;
