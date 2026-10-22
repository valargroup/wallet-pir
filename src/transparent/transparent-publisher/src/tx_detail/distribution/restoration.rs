//! Saved display publications use the host's preparation and commit boundary.
use super::*;
use pir_pipeline::publisher::Publication;

pub(super) struct DisplayRestoration<'a, S> {
    pub(super) distribution: &'a Distribution,
    pub(super) directory: PathBuf,
    pub(super) retained: Vec<PathBuf>,
    pub(super) shutdown: pir_pipeline::service::Shutdown,
    pub(super) stop: Option<S>,
    pub(super) revision: Option<u64>,
    pub(super) canonical: bool,
}
impl<S: Fn() -> bool + Send + Sync + 'static> Publication for DisplayRestoration<'_, S> {
    type Error = BoxError;
    type Material = PathBuf;
    type Stored = PathBuf;
    type Verified = Finalized;
    type Candidate = Finalized;
    type Report = bool;
    fn check(&self) -> Result<(), BoxError> {
        if self.shutdown.requested() {
            return Err(Stopped.into());
        }
        if self.distribution.revision() != self.revision {
            return Err("display authority changed during restoration".into());
        }
        Ok(())
    }
    async fn build(&mut self) -> Result<PathBuf, BoxError> {
        Ok(self.directory.clone())
    }
    async fn store(&mut self, directory: PathBuf) -> Result<PathBuf, BoxError> {
        Ok(directory)
    }
    async fn verify(&mut self, directory: PathBuf) -> Result<Finalized, BoxError> {
        self.distribution
            .prepare(
                directory,
                self.retained.clone(),
                self.shutdown.clone(),
                self.stop.take().expect("one preparation"),
            )
            .await
    }
    async fn assemble(&mut self, finalized: Finalized) -> Result<Finalized, BoxError> {
        Ok(finalized)
    }
    async fn recheck(&mut self, finalized: &mut Finalized) -> Result<(), BoxError> {
        self.canonical = self
            .distribution
            .canonical(finalized, &self.shutdown)
            .await?;
        // A noncanonical saved publication withdraws authority and is rebuilt.
        if !self.canonical {
            self.revision = self.distribution.revision();
        }
        Ok(())
    }
    async fn commit(self, finalized: Finalized) -> Result<bool, BoxError> {
        self.check()?;
        if self.canonical {
            self.distribution
                .commit(finalized, self.revision, &self.shutdown)?;
        }
        Ok(self.canonical)
    }
}
