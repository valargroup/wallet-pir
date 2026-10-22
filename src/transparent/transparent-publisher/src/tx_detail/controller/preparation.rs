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
            prebuild: None,
        })
    }
    async fn verify(
        &mut self,
        mut candidate: DisplayCandidate,
    ) -> Result<Self::Verified, BoxError> {
        candidate.prebuild = self
            .controller
            .prebuild(&candidate.candidate, &candidate.material.recent.digest)
            .await;
        self.check()?;
        let finalized = if let Some(distribution) = &self.distribution {
            let stop = self.controller.shutdown.clone();
            Some(
                distribution
                    .prepare(
                        candidate.candidate.clone(),
                        vec![self.controller.active.directory.clone()],
                        self.controller.shutdown.clone(),
                        move || stop.requested(),
                    )
                    .await
                    .map_err(retry)?,
            )
        } else {
            None
        };
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
