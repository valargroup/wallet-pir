//! Status's typed adapter of the shared publication contract: its pointer,
//! inventory and authority subject. Hashes bind bytes, not chain truth.
use crate::{profile, protocol::*};
use pir_publication::{
    AuthorityCheckRequest, MaterialIdentity, ObjectReference, PublicationEnvelope, PublicationScope,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const SCHEMA: &str = "status-prepared-distribution-v1";
pub const PREFIX: &str = "/provider/status/v1/";
pub const METADATA_LIMIT: usize = 64 << 10;
pub type Failure = Box<dyn std::error::Error + Send + Sync>;

/// A serving view's anchor: the publisher confirms the height still has this hash.
pub type AnchorSubject = (u32, Hash);
/// Status's authority check: anchors of candidate and serving views.
pub type AnchorCheckRequest = AuthorityCheckRequest<AnchorSubject>;

/// Status's publication envelope. Everything except the descriptor is the
/// shared publication contract.
pub type Current = PublicationEnvelope<StatusDescriptor>;

/// The Status-specific part of a publication reference.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct StatusDescriptor {
    /// The manifest generation the referenced inventory publishes.
    pub generation: u64,
}

/// The publication scope of Status on `network` with dataset `salt`.
pub fn scope(network: Hash, salt: Hash) -> PublicationScope {
    PublicationScope {
        product: SCHEMA.into(),
        chain: network,
        source: salt,
    }
}

/// Status checks of an envelope's scope and descriptor; the shared fields
/// are checked by [`PublicationEnvelope::validate`].
pub fn validate_current(current: &Current, network: Hash) -> Result<(), Failure> {
    current.validate()?;
    if current.scope.product != SCHEMA
        || current.scope.chain != network
        || current.scope.source == [0; 32]
    {
        return Err("invalid Status publisher identity".into());
    }
    if let Some(publication) = &current.publication {
        if publication.descriptor.generation == 0 {
            return Err("invalid Status publication generation".into());
        }
        publication.inventory.validate(METADATA_LIMIT as u64)?;
    }
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Inventory {
    pub schema: String,
    pub manifest: Manifest,
    pub units: Vec<ObjectReference>,
    pub packing: Vec<ObjectReference>,
    pub public: ObjectReference,
}
impl Inventory {
    /// Every object this inventory references, for retention and collection.
    pub fn referenced_objects(&self) -> impl Iterator<Item = &ObjectReference> {
        self.units
            .iter()
            .chain(&self.packing)
            .chain(std::iter::once(&self.public))
    }
    /// Identity of the exact distributed objects and commitments. A
    /// metadata-only refresh (new generation, epoch or observation time)
    /// keeps it, so the bytes stay reusable under a new publication identity.
    pub fn material_identity(&self) -> MaterialIdentity {
        let material = (
            &self.schema,
            self.manifest.network,
            self.manifest.salt,
            self.manifest.rows_digest,
            self.manifest.public_digest,
            &self.units,
            &self.packing,
            &self.public,
        );
        Sha256::digest(serde_json::to_vec(&material).expect("Status material serializes")).into()
    }

    /// Check that this inventory is exactly what eligible `current` publishes.
    pub fn validate(&self, current: &Current) -> Result<(), Failure> {
        self.validate_material()?;
        let publication = current
            .publication
            .as_ref()
            .ok_or("Status current names no publication")?;
        if self.schema != SCHEMA
            || self.manifest.network != current.scope.chain
            || self.manifest.recovery_epoch != current.fence.recovery_epoch
            || self.manifest.salt != current.scope.source
            || self.manifest.generation > publication.descriptor.generation
            || publication.material != self.material_identity()
            || self.units.len() != ROWS / profile().rlwe().d
        {
            return Err("Status inventory identity differs".into());
        }
        Ok(())
    }
    pub fn validate_material(&self) -> Result<(), Failure> {
        self.manifest.validate()?;
        if self.schema != SCHEMA || self.units.len() != ROWS / profile().rlwe().d {
            return Err("Status material geometry differs".into());
        }
        let unit_bytes = (profile().rlwe().d * ROW_BYTES) as u64;
        for unit in &self.units {
            unit.validate(unit_bytes)?;
            if unit.bytes != unit_bytes {
                return Err("Status database unit length differs".into());
            }
        }
        let expected = if cfg!(feature = "native-reinspiring") {
            profile().ypir().db_cols.div_ceil(profile().rlwe().d)
        } else {
            1
        };
        if self.packing.len() != expected {
            return Err("Status packing block count differs".into());
        }
        let mut total = 0u64;
        for block in &self.packing {
            block.validate(packing_limit())?;
            total = total
                .checked_add(block.bytes)
                .ok_or("Status packing size overflow")?;
        }
        if total > packing_limit() {
            return Err("Status packing exceeds geometry bound".into());
        }
        self.public.validate(2 << 20)?;
        if self.public.sha256 != hex::encode(self.manifest.public_digest) {
            return Err("Status public parameters differ".into());
        }
        Ok(())
    }
}
/// Conservative bound derived from the fixed hint geometry; loaders independently
/// validate the upstream prepared layout before allocating or mapping it.
pub fn packing_limit() -> u64 {
    (crate::Generation::hint_len() as u64).saturating_mul(64)
}
pub fn attempt_limit() -> u64 {
    packing_limit() + (ROWS * ROW_BYTES) as u64 + (4 << 20)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderStatus {
    pub inventory: Option<ObjectReference>,
    pub manifest: Option<Manifest>,
    pub synchronization: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn status_scope_changes_are_replays() {
        use pir_publication::{AuthorityFence, EnvelopeSuccession, PublicationReference};
        let old = Current {
            scope: scope([1; 32], [2; 32]),
            sequence: 10,
            eligible: true,
            fence: AuthorityFence {
                recovery_epoch: 2,
                latest_withdrawal: 0,
            },
            publication: Some(PublicationReference::new(
                [3; 32],
                ObjectReference::for_bytes(b"inventory"),
                StatusDescriptor { generation: 7 },
            )),
        };
        assert!(validate_current(&old, [1; 32]).is_ok());
        assert!(validate_current(&old, [9; 32]).is_err());
        for next_scope in [scope([9; 32], [2; 32]), scope([1; 32], [9; 32])] {
            let next = Current {
                scope: next_scope,
                sequence: 11,
                ..old.clone()
            };
            assert!(EnvelopeSuccession::classify(&next, Some(&old)).is_err());
        }
    }
    #[test]
    fn malformed_material_is_refused_before_loading_files() {
        let manifest = Manifest {
            protocol: PROTOCOL.into(),
            network: [1; 32],
            salt: [2; 32],
            generation: 7,
            recovery_epoch: 2,
            coverage_start: 1,
            anchor_height: 1,
            anchor_hash: [3; 32],
            observed_ms: 1,
            entries: 0,
            rows_digest: [4; 32],
            public_digest: [5; 32],
        };
        let inventory = Inventory {
            schema: SCHEMA.into(),
            manifest,
            units: vec![],
            packing: vec![],
            public: ObjectReference::for_bytes(b"public"),
        };
        let mut next = inventory.clone();
        next.manifest.generation += 1;
        next.manifest.observed_ms += 1;
        assert_eq!(inventory.material_identity(), next.material_identity());
        next.public = ObjectReference::for_bytes(b"other");
        assert_ne!(inventory.material_identity(), next.material_identity());
        next = inventory.clone();
        next.manifest.salt[0] ^= 1;
        assert_ne!(inventory.material_identity(), next.material_identity());
        next = inventory.clone();
        next.manifest.rows_digest[0] ^= 1;
        assert_ne!(inventory.material_identity(), next.material_identity());
        let current = Current {
            scope: scope([1; 32], [2; 32]),
            sequence: 10,
            eligible: true,
            fence: pir_publication::AuthorityFence {
                recovery_epoch: 2,
                latest_withdrawal: 0,
            },
            publication: Some(pir_publication::PublicationReference::new(
                inventory.material_identity(),
                ObjectReference::for_bytes(b"inventory"),
                StatusDescriptor { generation: 7 },
            )),
        };
        assert!(inventory.validate(&current).is_err());
        let result = crate::Generation::load_prepared(
            &inventory,
            true,
            |_| panic!("malformed geometry must not reach the filesystem"),
            ipir_sp::server::MatvecBackend::Cpu,
            None,
        );
        assert!(result.is_err());
    }
}
