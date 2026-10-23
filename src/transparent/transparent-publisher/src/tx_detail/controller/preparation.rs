//! Display material operations; the host sequences preparation and activation.
use super::*;
use pir_pipeline::publisher::Publication;
use transparent_shared::tx_detail::distribution::Finalized;

pub(super) struct DisplayPreparation<'a> {
    controller: &'a mut Controller,
    distribution: Option<super::super::distribution::Distribution>,
    revision: Option<u64>,
}
impl<'a> DisplayPreparation<'a> {
    pub(super) fn new(controller: &'a mut Controller) -> Self {
        let distribution = controller.settings.distribution.clone();
        let revision = distribution.as_ref().and_then(|d| d.revision());
        Self {
            controller,
            distribution,
            revision,
        }
    }
}
impl Publication for DisplayPreparation<'_> {
    type Error = BoxError;
    type Material = DisplayMaterial;
    type Stored = DisplayCandidate;
    type Verified = (DisplayCandidate, Option<Finalized>);
    type Candidate = Self::Verified;
    type Report = ();

    fn check(&self) -> Result<(), BoxError> {
        if self.controller.shutdown.requested() {
            return Err(transparent_shared::publication::Stopped.into());
        }
        if self.distribution.as_ref().and_then(|d| d.revision()) != self.revision {
            return Err(retry("display authority changed during preparation").into());
        }
        Ok(())
    }
    async fn build(&mut self) -> Result<DisplayMaterial, BoxError> {
        self.controller.build_cycle().await.map_err(Into::into)
    }
    async fn store(&mut self, material: DisplayMaterial) -> Result<DisplayCandidate, BoxError> {
        let started = Instant::now();
        let (candidate, sha) =
            write_candidate(&self.controller.root, &material.map).map_err(retry)?;
        Ok(DisplayCandidate {
            material,
            candidate,
            sha,
            candidate_ms: ms(started),
            runtime_preparation: None,
        })
    }
    async fn verify(
        &mut self,
        mut candidate: DisplayCandidate,
    ) -> Result<Self::Verified, BoxError> {
        let started = Instant::now();
        let finalized = if let Some(distribution) = &self.distribution {
            let stop = self.controller.shutdown.clone();
            let (finalized, prepared) = distribution
                .prepare_with(
                    candidate.candidate.clone(),
                    vec![self.controller.active.directory.clone()],
                    self.controller.shutdown.clone(),
                    move || stop.requested(),
                    self.controller.settings.runtime_preparation.clone(),
                )
                .await
                .map_err(retry)?;
            candidate.runtime_preparation = Some((ms(started), prepared));
            Some(finalized)
        } else {
            // Finite fixture publishers exercise the same verified-source
            // dependency without opening a node or granting HTTP authority.
            let revision = candidate.candidate.join(&candidate.material.recent.digest);
            let output = candidate.candidate.clone();
            let preparation = self.controller.settings.runtime_preparation.clone();
            let shutdown = self.controller.shutdown.clone();
            let prepared = tokio::task::spawn_blocking(move || {
                let revision =
                    transparent_shared::tx_detail::set::DisplayRevision::read(&revision)?
                        .verify_tables()?;
                preparation.prepare(&revision, &output, &[], &|| shutdown.requested())
            })
            .await?
            .map_err(retry)?;
            candidate.runtime_preparation = Some((ms(started), prepared));
            None
        };
        self.check()?;
        Ok((candidate, finalized))
    }
    async fn assemble(&mut self, verified: Self::Verified) -> Result<Self::Candidate, BoxError> {
        Ok(verified)
    }
    async fn recheck(&mut self, (_, finalized): &mut Self::Candidate) -> Result<(), BoxError> {
        if let Some((distribution, finalized)) = self.distribution.as_ref().zip(finalized.as_ref())
        {
            if !distribution
                .canonical(finalized, &self.controller.shutdown)
                .await
                .map_err(retry)?
            {
                return Err(retry("display publication anchor is not canonical").into());
            }
        }
        Ok(())
    }
    async fn commit(self, (candidate, finalized): Self::Candidate) -> Result<(), BoxError> {
        self.check()?;
        self.controller
            .activate_cycle(
                candidate,
                self.distribution
                    .zip(finalized)
                    .map(|(distribution, finalized)| DisplayActivation {
                        distribution,
                        finalized,
                        revision: self.revision,
                    }),
            )
            .await
            .map_err(Into::into)
    }
}

/// An ordered archive build has immutable cache handles and no publication
/// authority. The root runs the same preparation phases as foreground work.
pub(super) struct ArchivePreparation {
    pub(super) root: PathBuf,
    pub(super) layout: DisplayRoot,
    pub(super) spec: ShardSpec,
    pub(super) parts: Vec<Arc<[transparent_shard::txid::DisplayRecord]>>,
    pub(super) shutdown: pir_pipeline::service::Shutdown,
}
impl Publication for ArchivePreparation {
    type Error = BoxError;
    type Material = PublishedShard;
    type Stored = PublishedShard;
    type Verified = PublishedShard;
    type Candidate = PublishedShard;
    type Report = PublishedShard;
    fn check(&self) -> Result<(), BoxError> {
        if self.shutdown.requested() {
            return Err(transparent_shared::publication::Stopped.into());
        }
        Ok(())
    }
    async fn build(&mut self) -> Result<PublishedShard, BoxError> {
        let (root, layout, spec, parts) = (
            self.root.clone(),
            self.layout.clone(),
            self.spec.clone(),
            self.parts.clone(),
        );
        tokio::task::spawn_blocking(move || {
            publish_parts(&root, &layout, &spec, &parts).map_err(BoxError::from)
        })
        .await?
    }
    async fn store(&mut self, shard: PublishedShard) -> Result<PublishedShard, BoxError> {
        std::fs::File::open(shard.directory.join(MANIFEST_FILE))?.sync_all()?;
        std::fs::File::open(&shard.directory)?.sync_all()?;
        Ok(shard)
    }
    async fn verify(&mut self, shard: PublishedShard) -> Result<PublishedShard, BoxError> {
        // The builder already checked decoded rows. The disk reader verifies
        // the immutable references before this result can enter the seal queue.
        let directory = shard.directory.clone();
        tokio::task::spawn_blocking(move || {
            transparent_shared::tx_detail::set::DisplayRevision::read(&directory)?.verify_tables()
        })
        .await??;
        Ok(shard)
    }
    async fn assemble(&mut self, shard: PublishedShard) -> Result<PublishedShard, BoxError> {
        Ok(shard)
    }
    async fn recheck(&mut self, _: &mut PublishedShard) -> Result<(), BoxError> {
        self.check()
    }
    async fn commit(self, shard: PublishedShard) -> Result<PublishedShard, BoxError> {
        self.check()?;
        Ok(shard)
    }
}
