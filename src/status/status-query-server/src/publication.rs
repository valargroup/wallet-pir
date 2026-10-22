//! Status's typed adapter of the shared publication contract: its pointer,
//! inventory and authority subject. Hashes bind bytes, not chain truth.
use crate::{profile, protocol::*};
use pir_publication::{AuthorityCheckRequest, ObjectReference, PublicationPointer};
use serde::{Deserialize, Serialize};

pub const SCHEMA: &str = "status-prepared-distribution-v1";
pub const PREFIX: &str = "/provider/status/v1/";
pub const METADATA_LIMIT: usize = 64 << 10;
pub type Failure = Box<dyn std::error::Error + Send + Sync>;

/// A serving view's anchor: the publisher confirms the height still has this hash.
pub type AnchorSubject = (u32, Hash);
/// Status's authority check: anchors of candidate and serving views.
pub type AnchorCheckRequest = AuthorityCheckRequest<Current, AnchorSubject>;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Current {
    pub schema: String,
    pub revision: u64,
    pub recovery_epoch: u64,
    pub network: Hash,
    pub salt: Hash,
    pub generation: u64,
    pub inventory: Option<ObjectReference>,
    pub eligible: bool,
}
impl Current {
    pub fn validate(&self, network: Hash) -> Result<(), Failure> {
        if self.schema != SCHEMA
            || self.revision == 0
            || self.generation == 0
            || self.salt == [0; 32]
            || self.network != network
            || self.recovery_epoch == 0
            || (self.eligible && self.inventory.is_none())
        {
            return Err("invalid Status publisher identity".into());
        }
        if let Some(i) = &self.inventory {
            i.validate(METADATA_LIMIT as u64)?;
        }
        Ok(())
    }
}
impl PublicationPointer for Current {
    fn revision(&self) -> u64 {
        self.revision
    }
    fn recovery_epoch(&self) -> u64 {
        self.recovery_epoch
    }
    fn eligible(&self) -> bool {
        self.eligible
    }
    fn inventory(&self) -> Option<&ObjectReference> {
        self.inventory.as_ref()
    }
    fn with_eligibility(&self, eligible: bool) -> Self {
        Self {
            eligible,
            ..self.clone()
        }
    }
    /// The network and dataset salt never change; generations never go back.
    fn may_follow(&self, previous: &Self) -> bool {
        self.network == previous.network
            && self.salt == previous.salt
            && self.generation >= previous.generation
    }
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
    /// Exact distributed objects and commitments; publication identity may advance.
    pub fn same_prepared_material(&self, next: &Self) -> bool {
        self.schema == next.schema
            && self.manifest.network == next.manifest.network
            && self.manifest.salt == next.manifest.salt
            && self.manifest.rows_digest == next.manifest.rows_digest
            && self.manifest.public_digest == next.manifest.public_digest
            && self.units == next.units
            && self.packing == next.packing
            && self.public == next.public
    }

    pub fn validate(&self, current: &Current) -> Result<(), Failure> {
        self.validate_material()?;
        if self.schema != SCHEMA
            || self.manifest.network != current.network
            || self.manifest.recovery_epoch != current.recovery_epoch
            || self.manifest.salt != current.salt
            || self.manifest.generation > current.generation
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
    fn current() -> Current {
        Current {
            schema: SCHEMA.into(),
            revision: 10,
            recovery_epoch: 2,
            network: [1; 32],
            salt: [2; 32],
            generation: 7,
            inventory: Some(ObjectReference::for_bytes(b"inventory")),
            eligible: true,
        }
    }
    #[test]
    fn status_identity_changes_are_replays() {
        use pir_publication::PointerSuccession;
        let old = current();
        for field in ["generation", "network", "salt"] {
            let mut next = old.clone();
            next.revision += 1;
            match field {
                "generation" => next.generation = 6,
                "network" => next.network = [9; 32],
                _ => next.salt = [9; 32],
            }
            assert!(
                PointerSuccession::classify(&next, Some(&old)).is_err(),
                "publisher changed {field}"
            );
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
        assert!(inventory.same_prepared_material(&next));
        next.public = ObjectReference::for_bytes(b"other");
        assert!(!inventory.same_prepared_material(&next));
        next = inventory.clone();
        next.manifest.salt[0] ^= 1;
        assert!(!inventory.same_prepared_material(&next));
        next = inventory.clone();
        next.manifest.rows_digest[0] ^= 1;
        assert!(!inventory.same_prepared_material(&next));
        assert!(inventory.validate(&current()).is_err());
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
