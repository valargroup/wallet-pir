//! Enhance domain construction and guarded distribution storage for the host.
use super::*;
use crate::publication::{self, PreparedPublication};
use enhance_shared::publication::{Current, Inventory, Object};
use std::fs::OpenOptions;

enum Source {
    Journal(Arc<CommittedJournalSnapshot>),
    Inventory(Inventory),
    Prepared(PreparedPublication),
}
pub enum Material {
    Inventory(Inventory),
    Prepared(PreparedPublication),
}
pub struct EnhancePublication<'a> {
    shutdown: pir_pipeline::service::Shutdown,
    distribution: &'a PublicationDistribution,
    owner: Option<&'a EnhancePublisher>,
    source: Option<Source>,
    manifest: Manifest,
    predecessor: Current,
    bind_material: bool,
    _permit: Option<tokio::sync::OwnedSemaphorePermit>,
    _preparation: File,
}
impl<'a> EnhancePublication<'a> {
    fn open(
        distribution: &'a PublicationDistribution,
        manifest: Manifest,
        source: Source,
    ) -> Result<Self, String> {
        let preparation = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(distribution.root().join("prepare.lock"))
            .map_err(|e| e.to_string())?;
        preparation
            .try_lock()
            .map_err(|_| "Enhance publication preparation already owned")?;
        Ok(Self {
            shutdown: Default::default(),
            distribution,
            owner: None,
            source: Some(source),
            manifest,
            predecessor: distribution.durable_current(),
            bind_material: false,
            _permit: None,
            _preparation: preparation,
        })
    }
    pub(crate) fn stopping(mut self, shutdown: pir_pipeline::service::Shutdown) -> Self {
        self.shutdown = shutdown;
        self
    }
    pub(crate) fn inventory(
        distribution: &'a PublicationDistribution,
        manifest: Manifest,
        inventory: Inventory,
    ) -> Result<Self, String> {
        Self::open(distribution, manifest, Source::Inventory(inventory))
    }
    pub(crate) fn prepared(
        distribution: &'a PublicationDistribution,
        manifest: Manifest,
        prepared: PreparedPublication,
    ) -> Result<Self, String> {
        Self::open(distribution, manifest, Source::Prepared(prepared))
    }
    pub(super) fn journal(
        owner: &'a EnhancePublisher,
        work: PublicationWork,
    ) -> Result<Self, String> {
        if work.owner != owner.incarnation {
            return Err("publication work belongs to another publisher".into());
        }
        let permit = owner
            .preparation
            .clone()
            .try_acquire_owned()
            .map_err(|_| "publication preparation already owned")?;
        if owner.state.lock().unwrap().poisoned {
            return Err("Enhance publisher persistence failed; reopen required".into());
        }
        let mut operation = Self::open(
            &owner.distribution,
            work.manifest,
            Source::Journal(work.snapshot),
        )?;
        if operation.predecessor.withdrawal_revision != work.withdrawal_revision {
            return Err("publication withdrawn before preparation".into());
        }
        operation.owner = Some(owner);
        operation._permit = Some(permit);
        operation.bind_material = true;
        owner.state.lock().unwrap().preparing = Some(operation.manifest.anchor_height);
        Ok(operation)
    }
}
impl Drop for EnhancePublication<'_> {
    fn drop(&mut self) {
        if let Some(owner) = self.owner {
            owner.state.lock().unwrap().preparing = None;
        }
    }
}
impl pir_pipeline::publisher::Publication for EnhancePublication<'_> {
    type Error = String;
    type Material = Material;
    type Stored = Material;
    type Verified = PreparedPublication;
    type Candidate = Object;
    type Report = ();
    fn check(&self) -> Result<(), String> {
        if self.shutdown.requested() {
            return Err("Enhance publication stopped".into());
        }
        if self.distribution.durable_current() != self.predecessor {
            return Err("publication changed during preparation".into());
        }
        Ok(())
    }
    async fn build(&mut self) -> Result<Material, String> {
        match self.source.take().expect("source consumed once") {
            Source::Journal(snapshot) => {
                let root = self.distribution.root().to_path_buf();
                let coverage = self.manifest.coverage.clone();
                let epochs = self.manifest.domain_recovery_epochs.clone();
                tokio::task::spawn_blocking(move || {
                    publication::construct_publication(&root, coverage, &epochs, |start, count| {
                        snapshot
                            .read_records(start, count)
                            .map_err(|e| e.to_string())
                    })
                    .map(Material::Inventory)
                })
                .await
                .map_err(|e| e.to_string())?
            }
            Source::Inventory(inventory) => Ok(Material::Inventory(inventory)),
            Source::Prepared(prepared) => Ok(Material::Prepared(prepared)),
        }
    }
    async fn store(&mut self, material: Material) -> Result<Material, String> {
        File::open(self.distribution.root().join("objects"))
            .and_then(|f| f.sync_all())
            .map_err(|e| e.to_string())?;
        Ok(material)
    }
    async fn verify(&mut self, material: Material) -> Result<PreparedPublication, String> {
        let root = self.distribution.root().to_path_buf();
        tokio::task::spawn_blocking(move || match material {
            Material::Inventory(inventory) => publication::verify_publication(&root, inventory),
            Material::Prepared(prepared) => prepared.recheck(&root),
        })
        .await
        .map_err(|e| e.to_string())?
    }
    async fn assemble(&mut self, prepared: PreparedPublication) -> Result<Object, String> {
        let inventory = prepared.inventory();
        if self.bind_material {
            self.manifest.sessions = inventory
                .domains
                .iter()
                .map(|d| d.reference())
                .collect::<Result<_, _>>()?;
            self.manifest.unit_identities = inventory
                .domains
                .iter()
                .map(|d| (d.plan.shard.id, d.plan.units.clone()))
                .collect();
        }
        self.manifest.validate()?;
        if !inventory.supports(&self.manifest)? {
            return Err("inventory differs from wallet manifest".into());
        }
        publication::persist_inventory(self.distribution.root(), inventory)?;
        let bytes = serde_json::to_vec(inventory).map_err(|e| e.to_string())?;
        Ok(Object {
            sha256: enhance_shared::protocol::digest(inventory),
            bytes: bytes.len() as u64,
        })
    }
    async fn recheck(&mut self, _: &mut Object) -> Result<(), String> {
        self.distribution
            .check_canonical_anchor(&self.manifest)
            .await?;
        Ok(())
    }
    async fn commit(self, object: Object) -> Result<(), String> {
        self.distribution
            .commit_candidate(&self.predecessor, self.manifest.clone(), object)?;
        // Only a durable successor counts as progress; retries and preparation
        // cannot reset the alert's clock.
        if let Some(owner) = self.owner {
            owner.state.lock().unwrap().last_advancement = Some(std::time::SystemTime::now());
        }
        Ok(())
    }
}
