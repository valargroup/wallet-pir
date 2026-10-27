//! Serve the last publication again after a restart while the source still holds it.
use super::*;
use pir_publication::ObjectReference;
use std::io::Read;

impl Publisher {
    /// Publish again the inventory that was current when this publisher last
    /// stopped, under a new generation, the current recovery epoch and
    /// `observed`'s time, when `observed` holds exactly its rows and anchor and
    /// that anchor is still canonical. Prepared material depends only on the
    /// rows, network and salt, so this publishes what preparing `observed` would
    /// publish, without rebuilding the database. Returns whether it published;
    /// a missing object or changed source leaves preparation to the caller.
    pub(super) async fn restore(
        &self,
        observed: &source::Observation,
        authority: &mut authority::Authority,
        rpc: &ZakuraClient,
        shutdown: &pir_pipeline::service::Shutdown,
    ) -> Result<bool, Failure> {
        let (reference, expected, epoch) = {
            let published = self.published.read().unwrap();
            let current = published
                .authority
                .as_ref()
                .ok_or("Status publication authority is not open")?
                .current();
            let Some(reference) = current.publication.clone() else {
                return Ok(false);
            };
            (reference, current.sequence, current.fence.recovery_epoch)
        };
        let Some(mut inventory) = self.retained_inventory(&reference)? else {
            return Ok(false);
        };
        let manifest = &inventory.manifest;
        if !workflow::same_content(manifest, observed)
            || rpc.block_hash(u64::from(manifest.anchor_height)).await? != manifest.anchor_hash
        {
            return Ok(false);
        }
        inventory.manifest.generation = authority.next_generation()?;
        inventory.manifest.recovery_epoch = epoch;
        inventory.manifest.observed_ms = observed.observed_ms;
        let object = self.objects.store(&serde_json::to_vec(&inventory)?)?;
        self.commit_inventory(authority, &inventory, object, expected, shutdown)?;
        Ok(true)
    }

    /// The inventory `reference` names, when it and every object it references
    /// are still retained and it is the material `reference` recorded.
    fn retained_inventory(
        &self,
        reference: &PublicationReference<StatusDescriptor>,
    ) -> Result<Option<Inventory>, Failure> {
        let open = |object: &ObjectReference| match self.objects.open_retained(&object.sha256) {
            Ok(file) => Ok(Some(file)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        };
        let Some(file) = open(&reference.inventory)? else {
            return Ok(None);
        };
        let mut bytes = Vec::new();
        file.take(METADATA_LIMIT as u64 + 1)
            .read_to_end(&mut bytes)?;
        if ObjectReference::for_bytes(&bytes) != reference.inventory {
            return Err("Status retained inventory differs from its reference".into());
        }
        let inventory: Inventory = serde_json::from_slice(&bytes)?;
        inventory.validate_material()?;
        if inventory.material_identity() != reference.material {
            return Err("Status retained inventory names other material".into());
        }
        for object in inventory.referenced_objects() {
            match open(object)? {
                Some(file) if file.metadata()?.len() == object.bytes => {}
                _ => return Ok(None),
            }
        }
        Ok(Some(inventory))
    }
}

#[cfg(all(test, feature = "fixtures"))]
mod tests {
    use super::*;

    fn publisher(root: &std::path::Path) -> Publisher {
        Publisher::new(Config {
            state_dir: root.into(),
            salt_hex: "01".repeat(32),
            window_blocks: 1,
            private_listen: "127.0.0.1:0".parse().unwrap(),
        })
        .unwrap()
    }

    #[tokio::test]
    async fn a_restart_publishes_the_last_inventory_again_only_while_the_source_holds_it() {
        let root = tempfile::tempdir().unwrap();
        let (rpc, source, mempool) = source::tests::controlled_fixture(false, false).await;
        let observed = source::observe(&rpc, [1; 32], 1).await.unwrap();
        let (network, salt) = (observed.snapshot.network, observed.snapshot.salt);
        let shutdown = pir_pipeline::service::Shutdown::default();

        // The previous process prepares, publishes and withdraws as it stops.
        let first = {
            let publisher = publisher(root.path());
            publisher.open_authority(network, salt).unwrap();
            let mut authority =
                authority::Authority::open(root.path().join("authority"), network, salt).unwrap();
            let (expected, epoch) = publisher
                .published
                .read()
                .unwrap()
                .preparation_fence()
                .unwrap();
            let generation = Generation::prepare(
                &observed.snapshot,
                authority.next_generation().unwrap(),
                epoch,
                observed.observed_ms,
                None,
            )
            .unwrap()
            .0;
            let inventory = generation
                .export_prepared(|bytes| Ok(publisher.objects.store(bytes)?))
                .unwrap();
            let object = publisher
                .objects
                .store(&serde_json::to_vec(&inventory).unwrap())
                .unwrap();
            publisher
                .commit_inventory(&mut authority, &inventory, object, expected, &shutdown)
                .unwrap();
            publisher.withdraw();
            inventory
        };

        let restarted = || async {
            let publisher = publisher(root.path());
            publisher.open_authority(network, salt).unwrap();
            let authority =
                authority::Authority::open(root.path().join("authority"), network, salt).unwrap();
            (publisher, authority)
        };
        // A source whose rows changed meanwhile needs new material.
        let original = mempool.read().unwrap().clone();
        *mempool.write().unwrap() = "88".repeat(32);
        let changed = source::observe(&rpc, [1; 32], 1).await.unwrap();
        assert_ne!(changed.snapshot.digest, observed.snapshot.digest);
        let (publisher, mut authority) = restarted().await;
        assert!(!publisher
            .restore(&changed, &mut authority, &rpc, &shutdown)
            .await
            .unwrap());
        assert!(
            !publisher
                .published
                .read()
                .unwrap()
                .authority
                .as_ref()
                .unwrap()
                .current()
                .eligible
        );
        drop((publisher, authority));

        // The same rows serve again at once under the new epoch.
        let (publisher, mut authority) = restarted().await;
        *mempool.write().unwrap() = original;
        let again = source::observe(&rpc, [1; 32], 1).await.unwrap();
        assert_eq!(again.snapshot.digest, observed.snapshot.digest);
        assert!(publisher
            .restore(&again, &mut authority, &rpc, &shutdown)
            .await
            .unwrap());
        let published = publisher.published.read().unwrap();
        let current = published.authority.as_ref().unwrap().current();
        let restored = published.manifest.clone().unwrap();
        assert!(current.eligible);
        assert!(restored.recovery_epoch > first.manifest.recovery_epoch);
        assert_eq!(restored.recovery_epoch, current.fence.recovery_epoch);
        assert_eq!(restored.generation, first.manifest.generation + 1);
        assert_eq!(restored.observed_ms, again.observed_ms);
        assert_eq!(
            current.publication.as_ref().unwrap().material,
            first.material_identity()
        );
        source.abort();
    }
}
