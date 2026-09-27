//! Immutable prepared publications and atomic, reorg-fenced serving state.
use axum::{body::Bytes, http::StatusCode};
use receiver_directory::{
    snapshot::{Manifest, Snapshot, ROW_BYTES},
    Hash,
};
use receiver_pir::{server::Server, ROWS};
use std::{
    collections::VecDeque,
    fs::File,
    io::Read,
    path::Path,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

pub struct Publication {
    pub(crate) server: Server,
    pub(crate) witnesses: Option<Bytes>,
    pub(crate) id: Hash,
}

impl Publication {
    /// Prepare a complete immutable revision. The caller must separately accept its chain anchor.
    pub fn new(server: Server, witnesses: Option<Vec<u8>>) -> Result<Self, receiver_pir::Error> {
        if let Some(proof) = &witnesses {
            receiver_directory::witness::WitnessSnapshot::decode(
                proof,
                &server.manifest().directory,
            )?;
        }
        Ok(Self {
            id: server.manifest().id()?,
            server,
            witnesses: witnesses.map(Into::into),
        })
    }

    /// Load only files named by the validated content-addressed directory revision.
    pub fn load(path: &Path) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let bytes = bounded(path, 16384)?;
        let manifest: Manifest = serde_json::from_slice(&bytes)?;
        manifest.validate()?;
        if manifest.rows != ROWS as u32 {
            return Err("publication needs 8192 rows".into());
        }
        let revision = hex::encode(manifest.revision()?);
        let data = bounded(
            &path.with_file_name(format!("{revision}.rows")),
            ROWS * ROW_BYTES,
        )?;
        let proof_path = path.with_file_name(format!("{revision}.witness"));
        let proof = if proof_path.exists() {
            Some(bounded(
                &proof_path,
                receiver_directory::witness::MAX_WITNESS_BYTES,
            )?)
        } else {
            None
        };
        Self::new(Server::new(Snapshot { manifest, data })?, proof).map_err(Into::into)
    }

    pub fn manifest(&self) -> &Manifest {
        &self.server.manifest().directory
    }
}

fn bounded(path: &Path, limit: usize) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let mut bytes = Vec::new();
    File::open(path)?
        .take((limit + 1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > limit {
        return Err("publication file exceeds limit".into());
    }
    Ok(bytes)
}

struct State {
    epoch: u64,
    current: Option<Arc<Publication>>,
    previous: Option<(Arc<Publication>, Instant)>,
    revoked: VecDeque<Hash>,
}

/// One current and one briefly retained canonical revision. Revocation also fences in-flight work.
#[derive(Clone)]
pub struct Publications(Arc<RwLock<State>>);

impl Default for Publications {
    fn default() -> Self {
        Self(Arc::new(RwLock::new(State {
            epoch: 0,
            current: None,
            previous: None,
            revoked: VecDeque::new(),
        })))
    }
}

impl Publications {
    pub fn epoch(&self) -> u64 {
        self.0.read().unwrap().epoch
    }

    /// Drop obsolete revision files after a successful publication. Use only with one index writer.
    /// Active query tasks own their data in memory; the canonical SQLite journal remains durable.
    pub fn prune_files(&self, root: &Path) -> std::io::Result<()> {
        let state = self.0.read().unwrap();
        let keep: Vec<_> = state
            .current
            .iter()
            .chain(state.previous.iter().map(|(p, _)| p))
            .map(|p| hex::encode(p.manifest().revision().expect("validated manifest")))
            .collect();
        drop(state);
        if keep.is_empty() {
            return Ok(());
        }
        for entry in std::fs::read_dir(root)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some((id, extension)) = name.to_str().and_then(|s| s.rsplit_once('.')) else {
                continue;
            };
            if id.len() == 64
                && id.bytes().all(|c| c.is_ascii_hexdigit())
                && matches!(extension, "json" | "rows" | "witness")
                && !keep.iter().any(|k| k == id)
                && entry.file_type()?.is_file()
            {
                std::fs::remove_file(entry.path())?;
            }
        }
        Ok(())
    }

    /// Anchors that still accept new requests. Validate independently of expensive preparation.
    pub fn anchors(&self) -> Vec<(u32, Hash)> {
        let state = self.0.read().unwrap();
        state
            .current
            .iter()
            .chain(
                state
                    .previous
                    .iter()
                    .filter(|(_, until)| *until > Instant::now())
                    .map(|(p, _)| p),
            )
            .map(|p| (p.manifest().end_height, p.manifest().end_hash))
            .collect()
    }

    /// Install only if no revocation occurred since the caller began preparing and validating.
    /// The caller must verify the new canonical anchor and revoke before replacing forked coverage.
    pub fn publish(&self, publication: Publication, expected_epoch: u64) -> bool {
        let mut state = self.0.write().unwrap();
        if state.epoch != expected_epoch {
            return false;
        }
        state.revoked.retain(|id| *id != publication.id);
        state.previous = state
            .current
            .take()
            .map(|p| (p, Instant::now() + Duration::from_secs(60)));
        state.current = Some(Arc::new(publication));
        true
    }

    /// Conservatively invalidate all sessions, including CPU work that started before the reorg.
    pub fn revoke(&self) {
        let mut state = self.0.write().unwrap();
        state.epoch = state
            .epoch
            .checked_add(1)
            .expect("recovery epoch exhausted");
        if let Some(p) = state.current.take() {
            state.revoked.push_back(p.id);
        }
        if let Some((p, _)) = state.previous.take() {
            state.revoked.push_back(p.id);
        }
        while state.revoked.len() > 8 {
            state.revoked.pop_front();
        }
    }

    pub(crate) fn select(&self, id: Option<Hash>) -> Result<(Arc<Publication>, u64), StatusCode> {
        let state = self.0.read().unwrap();
        if let Some(id) = id {
            if state.revoked.contains(&id) {
                return Err(StatusCode::GONE);
            }
            for p in state.current.iter().chain(
                state
                    .previous
                    .iter()
                    .filter(|(_, until)| *until > Instant::now())
                    .map(|(p, _)| p),
            ) {
                if p.id == id {
                    return Ok((p.clone(), state.epoch));
                }
            }
            return Err(StatusCode::CONFLICT);
        }
        state
            .current
            .clone()
            .map(|p| (p, state.epoch))
            .ok_or(StatusCode::SERVICE_UNAVAILABLE)
    }
}
