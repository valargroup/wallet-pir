//! Read-only HTTPS distribution backed by immutable material and fresh node checks.
//! Publication and revocation changes are durable before any HTTP observer can
//! see them. Restart never restores authority from cached metadata alone.
use crate::{
    publication::{self, read_metadata, Prepared},
    zakura::ZakuraClient,
};
use axum::{
    body::Body,
    extract::{DefaultBodyLimit, Path as HttpPath, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use enhance_query_server::{
    artifact,
    protocol::{canonical_hash, digest, Manifest, RETAINED_GENERATIONS},
    publication::{
        AuthorityRequest, AuthorityResponse, Current, Inventory, Object, METADATA_LIMIT, PREFIX,
        VERSION,
    },
    worker::Revocation,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Retained {
    manifest: Manifest,
    inventory: Object,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Disk {
    current: Current,
    retained: Vec<Retained>,
}

#[derive(Clone)]
pub struct Distribution {
    root: PathBuf,
    rpc: ZakuraClient,
    disk: Arc<Mutex<Disk>>,
    poisoned: Arc<AtomicBool>,
    _owner: Arc<File>,
}

type ApiResult<T> = Result<T, (StatusCode, String)>;
fn refused(error: impl ToString) -> (StatusCode, String) {
    (StatusCode::SERVICE_UNAVAILABLE, error.to_string())
}

impl Distribution {
    pub fn open(root: &Path, rpc: ZakuraClient) -> Result<Self, String> {
        fs::create_dir_all(root).map_err(|e| e.to_string())?;
        let owner = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(root.join("distribution.lock"))
            .map_err(|e| e.to_string())?;
        owner
            .try_lock()
            .map_err(|_| "Enhance distribution already owned")?;
        let mut disk: Disk = if root.join("current.json").exists() {
            read_metadata(&root.join("current.json"))?
        } else {
            Disk {
                current: Current {
                    version: VERSION,
                    revision: 1,
                    withdrawal_revision: 1,
                    recovery_epoch: 0,
                    revoked: BTreeSet::new(),
                    eligible: false,
                    manifest: None,
                    inventory: None,
                },
                retained: Vec::new(),
            }
        };
        disk.current.validate()?;
        if disk.retained.len() > RETAINED_GENERATIONS {
            return Err("too many retained Enhance publications".into());
        }
        for retained in &disk.retained {
            retained.manifest.validate()?;
            retained.inventory.validate()?;
            if retained.inventory.bytes > METADATA_LIMIT {
                return Err("retained inventory exceeds metadata limit".into());
            }
        }
        // A successor must revalidate material and chain authority before the
        // old current can become eligible again. Keep every durable fence.
        disk.current.revision = disk
            .current
            .revision
            .checked_add(1)
            .ok_or("distribution revision exhausted")?;
        disk.current.withdrawal_revision = disk.current.revision;
        disk.current.eligible = false;
        persist(root, &disk)?;
        Ok(Self {
            root: root.into(),
            rpc,
            disk: Arc::new(Mutex::new(disk)),
            poisoned: Arc::new(AtomicBool::new(false)),
            _owner: Arc::new(owner),
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub(crate) fn durable_current(&self) -> Current {
        self.disk.lock().unwrap().current.clone()
    }
    /// Last durably installed manifest, for operator tooling. This does not
    /// authorize serving; consumers must obtain fresh source authority.
    pub fn published_manifest(&self) -> Option<Manifest> {
        self.disk.lock().unwrap().current.manifest.clone()
    }
    pub(crate) fn retained_manifests(&self) -> Vec<Manifest> {
        self.disk
            .lock()
            .unwrap()
            .retained
            .iter()
            .map(|r| r.manifest.clone())
            .collect()
    }
    pub(crate) fn poison(&self) {
        self.poisoned.store(true, Ordering::Release);
    }

    /// Install a completed publication after hashing every object it names.
    /// No consumer address or acknowledgment participates; the only remote
    /// dependency is the canonical chain source.
    pub async fn publish(&self, manifest: Manifest, inventory: Inventory) -> Result<(), String> {
        if !inventory.supports(&manifest)? {
            return Err("inventory differs from wallet manifest".into());
        }
        self.install(
            manifest,
            self.durable_current().withdrawal_revision,
            move |root| publication::verify(root, inventory),
        )
        .await
    }

    /// Install material this process prepared, rehashing only objects whose
    /// file changed since preparation hashed it.
    pub async fn publish_prepared(
        &self,
        manifest: Manifest,
        prepared: Prepared,
    ) -> Result<(), String> {
        self.publish_if_unwithdrawn(
            manifest,
            prepared,
            self.durable_current().withdrawal_revision,
        )
        .await
    }

    pub(crate) async fn publish_if_unwithdrawn(
        &self,
        manifest: Manifest,
        prepared: Prepared,
        withdrawal_revision: u64,
    ) -> Result<(), String> {
        if !prepared.inventory().supports(&manifest)? {
            return Err("inventory differs from wallet manifest".into());
        }
        self.install(manifest, withdrawal_revision, move |root| {
            prepared.recheck(root)
        })
        .await
    }

    async fn install(
        &self,
        manifest: Manifest,
        withdrawal_revision: u64,
        verify: impl FnOnce(&Path) -> Result<Prepared, String> + Send + 'static,
    ) -> Result<(), String> {
        // Use the producer's lock too: collection must not race a candidate
        // writer, another commit, or verification of a not-yet-current object.
        let preparation = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(self.root.join("prepare.lock"))
            .map_err(|e| e.to_string())?;
        preparation
            .try_lock()
            .map_err(|_| "Enhance publication preparation already owned")?;
        let predecessor = self.disk.lock().unwrap().current.clone();
        if predecessor.withdrawal_revision != withdrawal_revision {
            return Err("Enhance publication withdrawn during preparation".into());
        }
        let root = self.root.clone();
        let prepared = tokio::task::spawn_blocking(move || verify(&root))
            .await
            .map_err(|e| e.to_string())??;
        let inventory = prepared.inventory();
        self.canonical(&manifest).await?;
        let bytes = serde_json::to_vec(inventory).map_err(|e| e.to_string())?;
        let object = Object {
            sha256: digest(inventory),
            bytes: bytes.len() as u64,
        };
        if object.bytes > METADATA_LIMIT {
            return Err("inventory exceeds metadata limit".into());
        }
        let directory = self.root.join("inventories");
        fs::create_dir_all(&directory).map_err(|e| e.to_string())?;
        artifact::write_atomic(&directory, &format!("{}.json", object.sha256), |f| {
            f.write_all(&bytes)
        })
        .map_err(|e| e.to_string())?;
        File::open(&directory)
            .and_then(|f| f.sync_all())
            .map_err(|e| e.to_string())?;
        let mut disk = self.disk.lock().unwrap();
        if disk.current != predecessor {
            return Err("Enhance publication authority changed during preparation".into());
        }
        for shard in &manifest.coverage.shards {
            if disk
                .current
                .revoked
                .contains(&hex::encode(manifest.session_id(shard.id)?))
            {
                return Err("cannot publish invalidated Enhance material".into());
            }
        }
        let mut next = disk.clone();
        next.current.revision = next
            .current
            .revision
            .checked_add(1)
            .ok_or("distribution revision exhausted")?;
        next.current.recovery_epoch = next.current.recovery_epoch.max(manifest.recovery_epoch);
        next.current.eligible = true;
        next.current.manifest = Some(manifest.clone());
        next.current.inventory = Some(object.clone());
        next.retained
            .retain(|r| digest(&r.manifest) != digest(&manifest));
        next.retained.insert(
            0,
            Retained {
                manifest,
                inventory: object,
            },
        );
        next.retained.truncate(RETAINED_GENERATIONS);
        self.commit(&mut disk, next)?;
        drop(disk);
        self.collect_retired()?;
        Ok(())
    }

    /// Withdraw before journal rewind, even when the rewind needs no new domain
    /// epoch. Retained material is never deleted by an authority change.
    pub fn withdraw(&self, revocation: &Revocation) -> Result<(), String> {
        let mut disk = self.disk.lock().unwrap();
        let mut next = disk.clone();
        if revocation.recovery_epoch < next.current.recovery_epoch
            || !next.current.revoked.is_subset(&revocation.sessions)
        {
            return Err("nonmonotonic Enhance distribution fence".into());
        }
        next.current.recovery_epoch = revocation.recovery_epoch;
        next.current.revoked = revocation.sessions.clone();
        next.current.eligible = false;
        next.current.revision = next
            .current
            .revision
            .checked_add(1)
            .ok_or("distribution revision exhausted")?;
        next.current.withdrawal_revision = next.current.revision;
        self.commit(&mut disk, next)
    }

    fn commit(&self, disk: &mut Disk, next: Disk) -> Result<(), String> {
        if self.poisoned.load(Ordering::Acquire) {
            return Err("Enhance distribution persistence failed; reopen required".into());
        }
        next.current.validate()?;
        if !next.current.succeeds(&disk.current) {
            return Err("Enhance distribution would replay authority".into());
        }
        if let Err(error) = persist(&self.root, &next) {
            self.poisoned.store(true, Ordering::Release);
            return Err(error);
        }
        *disk = next;
        Ok(())
    }

    async fn canonical(&self, manifest: &Manifest) -> Result<u64, String> {
        let started = now_ms()?;
        let elapsed = std::time::Instant::now();
        if self
            .rpc
            .block_hash_display(manifest.anchor_height)
            .await
            .map_err(|e| e.to_string())?
            != manifest.anchor_block_hash
        {
            return Err("Enhance publication anchor is no longer canonical".into());
        }
        if elapsed.elapsed() > pir_control::CONTROL_WATCHDOG {
            return Err("Enhance source check exceeded authority lifetime".into());
        }
        Ok(started)
    }

    pub(crate) async fn authority(
        &self,
        requested: Option<&str>,
    ) -> Result<AuthorityResponse, String> {
        if self.poisoned.load(Ordering::Acquire) {
            return Err("Enhance distribution persistence failed".into());
        }
        let (current, target) = {
            let disk = self.disk.lock().unwrap();
            let current = disk.current.clone();
            let target = match requested {
                Some(id) if canonical_hash(id) => disk
                    .retained
                    .iter()
                    .find(|r| digest(&r.manifest) == id)
                    .map(|r| r.manifest.clone()),
                Some(_) => return Err("invalid requested manifest identity".into()),
                None => current.manifest.clone(),
            };
            (current, target)
        };
        // Return durable withdrawal metadata without granting fresh authority.
        if !current.eligible && requested.is_none() {
            return Ok(AuthorityResponse {
                current,
                authorized_manifest_sha256: String::new(),
                observed_unix_ms: 0,
            });
        }
        let manifest = target.ok_or("Enhance publication is no longer retained")?;
        if !current.eligible
            || manifest.coverage.shards.iter().any(|s| {
                manifest
                    .session_id(s.id)
                    .is_ok_and(|id| current.revoked.contains(&hex::encode(id)))
            })
        {
            return Err("Enhance publication is withdrawn".into());
        }
        let observed_unix_ms = self.canonical(&manifest).await?;
        // Recheck after the RPC: a withdrawal concurrent with the node check
        // must be visible before the response grants any authority.
        let disk = self.disk.lock().unwrap();
        let latest = &disk.current;
        if self.poisoned.load(Ordering::Acquire)
            || !latest.eligible
            || (requested.is_none() && latest.manifest != current.manifest)
            || latest.withdrawal_revision != current.withdrawal_revision
            || !disk
                .retained
                .iter()
                .any(|retained| retained.manifest == manifest)
            || manifest.coverage.shards.iter().any(|shard| {
                manifest
                    .session_id(shard.id)
                    .is_ok_and(|id| latest.revoked.contains(&hex::encode(id)))
            })
        {
            return Err("Enhance authority changed during source validation".into());
        }
        // Ordinary publication advancement does not revoke a retained binding.
        // Return the latest floors together with this real canonical observation.
        Ok(AuthorityResponse {
            current: latest.clone(),
            authorized_manifest_sha256: digest(&manifest),
            observed_unix_ms,
        })
    }

    pub fn router(&self) -> Router {
        Router::new()
            .route(&format!("{PREFIX}current"), get(current))
            .route(&format!("{PREFIX}authority"), post(authority))
            .route(&format!("{PREFIX}inventories/:digest"), get(inventory))
            .route(&format!("{PREFIX}objects/:digest"), get(object))
            .layer(DefaultBodyLimit::max(1024))
            .with_state(self.clone())
    }

    fn objects(&self) -> Result<BTreeMap<String, Object>, String> {
        let retained = self.disk.lock().unwrap().retained.clone();
        let mut objects = BTreeMap::new();
        for retained in retained {
            let inventory: Inventory = read_metadata(
                &self
                    .root
                    .join("inventories")
                    .join(format!("{}.json", retained.inventory.sha256)),
            )?;
            if digest(&inventory) != retained.inventory.sha256
                || !inventory.supports(&retained.manifest)?
            {
                return Err("retained Enhance inventory differs".into());
            }
            for domain in inventory.domains {
                let packing = domain.packing_object();
                for object in domain
                    .rows
                    .into_iter()
                    .chain([domain.hint, domain.public, packing])
                {
                    objects.insert(object.sha256.clone(), object);
                }
            }
        }
        Ok(objects)
    }

    /// Called with the preparation lock after the current pointer is durable.
    /// An open HTTP stream pins its inode across unlink. A bootstrap that has
    /// not opened a retired object reconciles current and retries instead.
    fn collect_retired(&self) -> Result<(), String> {
        let retained = self.disk.lock().unwrap().retained.clone();
        let mut inventories = BTreeSet::new();
        let mut domains = BTreeSet::new();
        let mut units = BTreeSet::new();
        let mut packing = BTreeSet::new();
        let mut objects = BTreeSet::new();
        for retained in retained {
            let name = format!("{}.json", retained.inventory.sha256);
            let inventory: Inventory = read_metadata(&self.root.join("inventories").join(&name))?;
            if digest(&inventory) != retained.inventory.sha256
                || !inventory.supports(&retained.manifest)?
            {
                return Err("cannot collect: retained Enhance inventory differs".into());
            }
            inventories.insert(name);
            for domain in inventory.domains {
                domains.insert(format!("{}.json", digest(&domain.plan)));
                units.extend(domain.plan.units.iter().map(|unit| unit.digest()));
                packing.insert(domain.packing.name.clone());
                packing.insert(format!("{}.json", domain.packing.name));
                objects.insert(domain.packing.sha256);
                objects.extend(
                    domain
                        .rows
                        .iter()
                        .chain([&domain.hint, &domain.public])
                        .map(|o| o.sha256.clone()),
                );
            }
        }
        for (directory, keep) in [
            ("objects", objects),
            ("inventories", inventories),
            ("domains", domains),
            (enhance_query_server::prepared_packing::DIRECTORY, packing),
        ] {
            collect_files(&self.root.join(directory), &keep)?;
        }
        let directory = self.root.join("preparation/artifacts-9");
        if directory.is_dir() {
            for entry in fs::read_dir(&directory).map_err(|e| e.to_string())? {
                let entry = entry.map_err(|e| e.to_string())?;
                if entry.file_type().map_err(|e| e.to_string())?.is_dir()
                    && entry
                        .file_name()
                        .to_str()
                        .is_some_and(|name| canonical_hash(name) && !units.contains(name))
                {
                    fs::remove_dir_all(entry.path()).map_err(|e| e.to_string())?;
                }
            }
            File::open(directory)
                .and_then(|f| f.sync_all())
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }
}

fn collect_files(directory: &Path, keep: &BTreeSet<String>) -> Result<(), String> {
    if !directory.is_dir() {
        return Ok(());
    }
    for entry in fs::read_dir(directory).map_err(|e| e.to_string())? {
        let entry = entry.map_err(|e| e.to_string())?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let committed_name = name.strip_suffix(".tmp").unwrap_or(name);
        let owned = canonical_hash(committed_name)
            || committed_name
                .strip_suffix(".json")
                .is_some_and(canonical_hash)
            || committed_name
                .strip_suffix(".bin")
                .is_some_and(canonical_hash)
            || committed_name
                .strip_suffix(".bin.json")
                .is_some_and(canonical_hash);
        if owned && !keep.contains(name) && entry.file_type().map_err(|e| e.to_string())?.is_file()
        {
            fs::remove_file(entry.path()).map_err(|e| e.to_string())?;
        }
    }
    File::open(directory)
        .and_then(|f| f.sync_all())
        .map_err(|e| e.to_string())
}

fn now_ms() -> Result<u64, String> {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| e.to_string())?
            .as_millis(),
    )
    .map_err(|e| e.to_string())
}

fn persist(root: &Path, disk: &Disk) -> Result<(), String> {
    let bytes = serde_json::to_vec(disk).map_err(|e| e.to_string())?;
    if bytes.len() as u64 > METADATA_LIMIT {
        return Err("distribution state exceeds metadata limit".into());
    }
    artifact::write_atomic(root, "current.json", |f| f.write_all(&bytes))
        .map_err(|e| e.to_string())?;
    File::open(root)
        .and_then(|f| f.sync_all())
        .map_err(|e| e.to_string())
}

async fn current(State(distribution): State<Distribution>) -> ApiResult<Response> {
    distribution
        .authority(None)
        .await
        .map(metadata)
        .map_err(refused)
}

async fn authority(
    State(distribution): State<Distribution>,
    Json(request): Json<AuthorityRequest>,
) -> ApiResult<Response> {
    distribution
        .authority(Some(&request.manifest_sha256))
        .await
        .map(metadata)
        .map_err(refused)
}

fn metadata(value: AuthorityResponse) -> Response {
    ([(header::CACHE_CONTROL, "no-store")], Json(value)).into_response()
}

async fn inventory(
    State(distribution): State<Distribution>,
    HttpPath(id): HttpPath<String>,
) -> ApiResult<Response> {
    if !canonical_hash(&id) {
        return Err((StatusCode::BAD_REQUEST, "invalid inventory digest".into()));
    }
    let descriptor = distribution
        .disk
        .lock()
        .unwrap()
        .retained
        .iter()
        .find(|r| r.inventory.sha256 == id)
        .map(|r| r.inventory.clone())
        .ok_or((
            StatusCode::NOT_FOUND,
            "inventory is no longer retained".into(),
        ))?;
    stream(
        &distribution
            .root
            .join("inventories")
            .join(format!("{id}.json")),
        &descriptor,
    )
}

async fn object(
    State(distribution): State<Distribution>,
    HttpPath(id): HttpPath<String>,
) -> ApiResult<Response> {
    if !canonical_hash(&id) {
        return Err((StatusCode::BAD_REQUEST, "invalid object digest".into()));
    }
    let distribution_for_lookup = distribution.clone();
    let objects = tokio::task::spawn_blocking(move || distribution_for_lookup.objects())
        .await
        .map_err(refused)?
        .map_err(refused)?;
    let descriptor = objects.get(&id).ok_or((
        StatusCode::NOT_FOUND,
        "object is not referenced by a retained inventory".into(),
    ))?;
    stream(
        &descriptor.path(&distribution.root).map_err(refused)?,
        descriptor,
    )
}

fn stream(path: &Path, descriptor: &Object) -> ApiResult<Response> {
    let artifact =
        artifact::PublicationArtifact::open(path, descriptor.bytes, descriptor.sha256.clone())
            .map_err(refused)?;
    Ok((
        [
            (header::CONTENT_TYPE, "application/octet-stream"),
            (header::CACHE_CONTROL, "public, max-age=31536000, immutable"),
        ],
        Body::from_stream(artifact.stream()),
    )
        .into_response())
}
