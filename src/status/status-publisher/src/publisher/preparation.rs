//! Status material and guarded authority operations supplied to the host engine.
use super::*;
use pir_publication::ObjectReference;

pub(super) struct StatusPublication<'a> {
    shutdown: pir_pipeline::service::Shutdown,
    publisher: &'a Publisher,
    observed: Arc<source::Observation>,
    authority: &'a mut authority::Authority,
    previous: Option<Generation>,
    rpc: &'a ZakuraClient,
    expected: u64,
    generation: u64,
    epoch: u64,
}

impl<'a> StatusPublication<'a> {
    pub(super) fn new(
        publisher: &'a Publisher,
        observed: Arc<source::Observation>,
        authority: &'a mut authority::Authority,
        previous: Option<Generation>,
        rpc: &'a ZakuraClient,
        shutdown: pir_pipeline::service::Shutdown,
    ) -> Result<Self, Failure> {
        let (expected, epoch) = publisher.published.read().unwrap().preparation_fence()?;
        let generation = authority.next_generation()?;
        Ok(Self {
            publisher,
            observed,
            authority,
            previous,
            rpc,
            expected,
            generation,
            epoch,
            shutdown,
        })
    }
}

impl pir_pipeline::publisher::Publication for StatusPublication<'_> {
    type Error = Failure;
    type Material = Generation;
    type Stored = (Generation, Inventory);
    type Verified = Self::Stored;
    type Candidate = (Generation, Inventory, ObjectReference);
    type Report = Generation;

    fn check(&self) -> Result<(), Failure> {
        if self.shutdown.requested() {
            return Err("Status publication stopped".into());
        }
        self.publisher
            .published
            .read()
            .unwrap()
            .check_fence(self.expected)?;
        Ok(())
    }

    async fn build(&mut self) -> Result<Generation, Failure> {
        let (observed, previous, generation, epoch) = (
            self.observed.clone(),
            self.previous.clone(),
            self.generation,
            self.epoch,
        );
        tokio::task::spawn_blocking(move || -> Result<Generation, Failure> {
            Ok(Generation::prepare(
                &observed.snapshot,
                generation,
                epoch,
                observed.observed_ms,
                previous.as_ref(),
            )?
            .0)
        })
        .await?
    }
    async fn store(&mut self, prepared: Generation) -> Result<Self::Stored, Failure> {
        let publisher = self.publisher.clone();
        let candidate = prepared.clone();
        let inventory = tokio::task::spawn_blocking(move || -> Result<Inventory, Failure> {
            let cache = publisher.exported.lock().unwrap();
            if let Some(old) = cache.as_ref().filter(|i| {
                i.manifest.network == candidate.manifest().network
                    && i.manifest.salt == candidate.manifest().salt
                    && i.manifest.rows_digest == candidate.manifest().rows_digest
            }) {
                let mut inventory = old.clone();
                inventory.manifest = candidate.manifest().clone();
                Ok(inventory)
            } else {
                candidate.export_prepared(|bytes| Ok(publisher.objects.store(bytes)?))
            }
        })
        .await??;
        Ok((prepared, inventory))
    }
    async fn verify(&mut self, stored: Self::Stored) -> Result<Self::Verified, Failure> {
        stored.1.validate_material()?;
        Ok(stored)
    }
    async fn assemble(
        &mut self,
        (prepared, inventory): Self::Verified,
    ) -> Result<Self::Candidate, Failure> {
        let object = self
            .publisher
            .objects
            .store(&serde_json::to_vec(&inventory)?)?;
        Ok((prepared, inventory, object))
    }
    async fn recheck(&mut self, candidate: &mut Self::Candidate) -> Result<(), Failure> {
        let manifest = &candidate.1.manifest;
        if self
            .rpc
            .block_hash(u64::from(manifest.anchor_height))
            .await?
            != manifest.anchor_hash
        {
            self.publisher.withdraw();
            return Err("Status candidate anchor disconnected".into());
        }
        Ok(())
    }
    async fn commit(
        self,
        (prepared, inventory, object): Self::Candidate,
    ) -> Result<Generation, Failure> {
        self.publisher.commit_inventory(
            self.authority,
            &inventory,
            object,
            self.expected,
            &self.shutdown,
        )?;
        tracing::info!(
            generation = self.generation,
            recovery_epoch = self.epoch,
            source_observed_ms = inventory.manifest.observed_ms,
            published_ms = now_ms(),
            "Status prepared publication"
        );
        Ok(prepared)
    }
}
