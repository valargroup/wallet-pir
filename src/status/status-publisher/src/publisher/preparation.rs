//! Status material and guarded authority operations supplied to the host engine.
use super::*;
use pir_publication::ObjectReference;
use std::sync::OnceLock;

/// Threads Status material is prepared on unless `STATUS_PREPARATION_THREADS`
/// sets another positive count.
const PREPARATION_THREADS: usize = 2;

/// The pool Status material is prepared on. The other products in the unified
/// publisher share the process's global rayon pool, so Status keeps its own and
/// neither waits behind their preparation nor delays it.
fn preparation_pool() -> &'static rayon::ThreadPool {
    static POOL: OnceLock<rayon::ThreadPool> = OnceLock::new();
    POOL.get_or_init(|| {
        let threads = match std::env::var("STATUS_PREPARATION_THREADS") {
            Ok(value) => value.parse().ok().filter(|n| *n > 0).unwrap_or_else(|| {
                tracing::warn!(%value, "ignoring invalid STATUS_PREPARATION_THREADS");
                PREPARATION_THREADS
            }),
            Err(_) => PREPARATION_THREADS,
        };
        rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .thread_name(|i| format!("status-prep-{i}"))
            .build()
            .expect("Status preparation threads start")
    })
}

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
        tokio::task::spawn_blocking(move || {
            preparation_pool().install(|| -> Result<Generation, Failure> {
                Ok(Generation::prepare(
                    &observed.snapshot,
                    generation,
                    epoch,
                    observed.observed_ms,
                    previous.as_ref(),
                )?
                .0)
            })
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

#[cfg(test)]
mod pool_tests {
    use super::preparation_pool;
    use rayon::prelude::*;
    use std::{sync::mpsc, time::Duration};

    #[test]
    fn preparation_runs_while_the_global_pool_is_busy() {
        // Hold every global worker until the Status work has finished.
        let (release, held) = mpsc::channel::<()>();
        let held = std::sync::Arc::new(std::sync::Mutex::new(held));
        let (started, busy) = mpsc::channel();
        for _ in 0..rayon::current_num_threads() {
            let (held, started) = (held.clone(), started.clone());
            rayon::spawn(move || {
                started.send(()).unwrap();
                let _ = held.lock().unwrap().recv_timeout(Duration::from_secs(30));
            });
        }
        for _ in 0..rayon::current_num_threads() {
            busy.recv_timeout(Duration::from_secs(10)).unwrap();
        }
        let (done, finished) = mpsc::channel();
        std::thread::spawn(move || {
            let sum: u64 = preparation_pool().install(|| (0..10_000u64).into_par_iter().sum());
            done.send(sum).unwrap();
        });
        let sum = finished.recv_timeout(Duration::from_secs(10));
        drop(release);
        assert_eq!(sum, Ok(49_995_000));
    }
}
