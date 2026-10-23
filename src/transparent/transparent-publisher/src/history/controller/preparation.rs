//! History source-bound material operations; the host owns their execution order.
use super::*;
use pir_pipeline::publisher::Publication;

pub(super) struct HistoryPreparation<'a> {
    config: &'a Config,
    rpc: &'a NodeClient,
    authority: &'a Authority,
    work: Option<HistoryWork>,
    epoch: u64,
    height: u64,
    hash: String,
    seal: HistorySeal,
    archive_geometry: Option<String>,
    recent_from: Option<u64>,
}
pub(super) struct HistoryMaterial {
    directory: PathBuf,
    started: std::time::Instant,
    published_unix: f64,
    runtimes: serde_json::Value,
    storage: serde_json::Value,
}
impl<'a> HistoryPreparation<'a> {
    pub(super) fn new(
        config: &'a Config,
        rpc: &'a NodeClient,
        authority: &'a Authority,
        work: HistoryWork,
        seal: HistorySeal,
        archive_geometry: Option<String>,
        recent_from: Option<u64>,
    ) -> Self {
        authority.0.foreground.fetch_add(1, Ordering::AcqRel);
        Self {
            config,
            rpc,
            authority,
            epoch: work.epoch,
            height: work.height,
            hash: work.hash.clone(),
            work: Some(work),
            seal,
            archive_geometry,
            recent_from,
        }
    }
    fn stop(&self) -> impl Fn() -> bool + Send + Sync + 'static {
        let authority = self.authority.clone();
        let epoch = self.epoch;
        move || {
            authority.0.shutdown.requested() || authority.0.epoch.load(Ordering::Acquire) != epoch
        }
    }
    fn continuing(&self) -> Result<(), BoxError> {
        if self.authority.0.shutdown.requested() {
            return Err(transparent_shared::publication::Stopped.into());
        }
        if self.authority.0.epoch.load(Ordering::Acquire) != self.epoch {
            return Err("candidate invalidated before activation".into());
        }
        Ok(())
    }
}
impl Drop for HistoryPreparation<'_> {
    fn drop(&mut self) {
        self.authority.0.foreground.fetch_sub(1, Ordering::AcqRel);
    }
}
impl Publication for HistoryPreparation<'_> {
    type Error = BoxError;
    type Material = HistoryMaterial;
    type Stored = HistoryMaterial;
    type Verified = HistoryMaterial;
    type Candidate = HistoryCandidate;
    type Report = ();
    fn check(&self) -> Result<(), BoxError> {
        self.continuing()
    }
    async fn build(&mut self) -> Result<HistoryMaterial, BoxError> {
        self.continuing()?;
        let HistoryWork {
            old,
            snapshot,
            height,
            hash,
            ..
        } = self.work.take().expect("snapshot consumed once");
        let started = std::time::Instant::now();
        let same = height == old.publication.height && hash == old.publication.hash;
        let directory = if same {
            old.publication.directory.clone()
        } else {
            self.config.publication_root.join(format!(
                "candidate-{height}-{hash}-{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)?
                    .as_nanos()
            ))
        };
        if !same {
            std::fs::create_dir(&directory)?;
            let config = self.config;
            let options = PublishOptions {
                data_dir: config.data_dir.clone(),
                output: directory.clone(),
                previous: Some(old.publication.directory.clone()),
                recent_geometry: config.recent_geometry.clone(),
                archive_geometry: self.archive_geometry.clone(),
                recent_from: self.recent_from,
                zakura_rpc_url: config.rpc_url.clone(),
                zakura_cookie: config.rpc_cookie.clone(),
                through: Some(height),
                record: Some(directory.join("publication.json")),
                source_sha: Some(config.source_sha.clone()),
                directory_choice: config.directory_choice,
                range_profile: config.range_profile.clone(),
            };
            let stop = self.stop();
            let seal = self.seal;
            tokio::task::spawn_blocking(move || {
                publication::publish_until(
                    &options,
                    &snapshot,
                    BlockHash::from_internal_bytes([0; 32]),
                    &seal,
                    &stop,
                )
            })
            .await??;
        }
        self.continuing()?;
        Ok(HistoryMaterial {
            directory,
            started,
            published_unix: unix_now(),
            runtimes: serde_json::Value::Null,
            storage: serde_json::json!({}),
        })
    }
    async fn store(&mut self, mut material: HistoryMaterial) -> Result<HistoryMaterial, BoxError> {
        self.continuing()?;
        if material.directory.parent() == Some(self.config.publication_root.as_path()) {
            let directory = material.directory.clone();
            let stop = self.stop();
            let adopted = tokio::task::spawn_blocking(move || {
                crate::history::store::adopt_segments(&directory, &stop)
            })
            .await??;
            material.storage["segments"] = serde_json::to_value(adopted)?;
        }
        Ok(material)
    }
    async fn verify(&mut self, mut material: HistoryMaterial) -> Result<HistoryMaterial, BoxError> {
        self.continuing()?;
        self.authority.0.status.write().unwrap()["candidate_height"] = self.height.into();
        material.runtimes = prepare_runtimes(
            self.config,
            self.authority,
            &material.directory,
            self.epoch,
            &mut material.storage,
        )
        .await?;
        Ok(material)
    }
    async fn assemble(&mut self, material: HistoryMaterial) -> Result<HistoryCandidate, BoxError> {
        self.continuing()?;
        let HistoryMaterial {
            directory,
            started,
            published_unix,
            runtimes,
            mut storage,
        } = material;
        let config = self.config;
        let authority = self.authority;
        let mut candidate = read_public(directory.clone())?;
        let stop = self.stop();
        {
            // Reuse the exact verified set just used to prepare the candidate.
            let set = authority
                .0
                .prepared_set
                .lock()
                .unwrap()
                .take()
                .ok_or("prepared distribution has no verified candidate")?;
            let root = directory.clone();
            let cutoff = config.recent_from;
            let mut checked = std::mem::take(&mut *authority.0.checked_runtimes.lock().unwrap());
            let started = std::time::Instant::now();
            let (set, checked, distribution) = tokio::task::spawn_blocking(move || {
                let result = transparent_shared::publication::Finalized::prepare_reusing_until(
                    &root,
                    &set,
                    cutoff,
                    true,
                    &mut checked,
                    &stop,
                );
                (set, checked, result)
            })
            .await?;
            tracing::info!(
                loaded = checked.loaded,
                reused = checked.reused,
                bytes_hashed = checked.bytes_hashed,
                bytes_reused = checked.bytes_reused,
                seconds = started.elapsed().as_secs_f64(),
                "prepared distribution inventory"
            );
            storage["inventory"] = serde_json::json!({
                "runtimes_loaded": checked.loaded,
                "runtimes_reused": checked.reused,
                "bytes_hashed": checked.bytes_hashed,
                "bytes_reused": checked.bytes_reused,
            });
            *authority.0.checked_runtimes.lock().unwrap() = checked;
            *authority.0.prepared_set.lock().unwrap() = Some(set);
            candidate.distribution = Some(match distribution {
                Err(_) if authority.0.shutdown.requested() => {
                    return Err(transparent_shared::publication::Stopped.into())
                }
                Err(error) if error.is::<transparent_shared::publication::Stopped>() => {
                    return Err("candidate invalidated while preparing".into())
                }
                result => result?,
            });
        }

        Ok(HistoryCandidate {
            candidate,
            epoch: self.epoch,
            height: self.height,
            hash: self.hash.clone(),
            started,
            published_unix,
            prepared_unix: unix_now(),
            runtimes,
            storage,
        })
    }
    async fn recheck(&mut self, _: &mut HistoryCandidate) -> Result<(), BoxError> {
        self.continuing()?;
        if self.rpc.block_hash_display(self.height).await? != self.hash {
            return Err("candidate invalidated while preparing".into());
        }
        self.continuing()
    }
    async fn commit(self, candidate: HistoryCandidate) -> Result<(), BoxError> {
        activate_history(self.config, self.rpc, self.authority, candidate).await
    }
}
