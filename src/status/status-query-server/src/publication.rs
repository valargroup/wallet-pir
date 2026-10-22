//! Status-owned HTTPS distribution contract; hashes bind bytes, not chain truth.
use crate::{profile, protocol::*};
use serde::{Deserialize, Serialize};

pub const SCHEMA: &str = "status-prepared-distribution-v2";
pub const PREFIX: &str = "/provider/status/v1/";
pub const METADATA_LIMIT: usize = 64 << 10;
pub const MAX_ANCHORS: usize = 256;
pub type Failure = Box<dyn std::error::Error + Send + Sync>;

pub fn digest(bytes: &[u8]) -> String {
    pir_artifact::digest(bytes)
}
pub fn valid_digest(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Object {
    #[serde(with = "pir_artifact::current")]
    pub checksum: String,
    pub bytes: u64,
}
impl Object {
    pub fn new(bytes: &[u8]) -> Self {
        Self {
            checksum: digest(bytes),
            bytes: bytes.len() as u64,
        }
    }
    pub fn validate(&self, limit: u64) -> Result<(), Failure> {
        if !valid_digest(&self.checksum) || self.bytes == 0 || self.bytes > limit {
            return Err("invalid Status object identity or size".into());
        }
        Ok(())
    }
    pub fn verify(&self, bytes: &[u8]) -> Result<(), Failure> {
        if bytes.len() as u64 != self.bytes || digest(bytes) != self.checksum {
            return Err("Status object digest or length differs".into());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Current {
    pub schema: String,
    pub revision: u64,
    pub recovery_epoch: u64,
    pub network: Hash,
    pub salt: Hash,
    pub generation: u64,
    pub inventory: Option<Object>,
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
    pub fn succeeds(&self, old: &Self) -> bool {
        if self.network != old.network
            || self.salt != old.salt
            || self.generation < old.generation
            || self.recovery_epoch < old.recovery_epoch
            || self.revision < old.revision
        {
            return false;
        }
        if self.revision > old.revision {
            return true;
        }
        let mut equal = old.clone();
        equal.eligible = self.eligible;
        self == &equal && (!self.eligible || old.eligible)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Inventory {
    pub schema: String,
    pub manifest: Manifest,
    pub units: Vec<Object>,
    pub packing: Vec<Object>,
    pub public: Object,
}
impl Inventory {
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
pub struct AnchorRequest {
    pub current: Current,
    pub anchors: Vec<(u32, Hash)>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AnchorResponse {
    pub current: Current,
    pub canonical: Vec<bool>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderStatus {
    pub inventory: Option<Object>,
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
            inventory: Some(Object::new(b"inventory")),
            eligible: true,
        }
    }
    #[test]
    fn withdrawal_cannot_be_undone_at_the_same_revision() {
        let old = current();
        let mut withdrawn = old.clone();
        withdrawn.eligible = false;
        assert!(withdrawn.succeeds(&old));
        assert!(!old.succeeds(&withdrawn));
        let mut newer = old.clone();
        newer.revision += 1;
        assert!(newer.succeeds(&withdrawn));
        for field in ["revision", "epoch", "generation", "network", "salt"] {
            let mut replay = newer.clone();
            match field {
                "revision" => replay.revision = 9,
                "epoch" => replay.recovery_epoch = 1,
                "generation" => replay.generation = 6,
                "network" => replay.network = [9; 32],
                _ => replay.salt = [9; 32],
            }
            assert!(!replay.succeeds(&old), "publisher changed {field}");
        }
    }
    #[test]
    fn content_addresses_and_lengths_are_checked_independently() {
        let object = Object::new(b"material");
        assert!(object.verify(b"material").is_ok());
        assert!(object.verify(b"materiaL").is_err());
        assert!(object.verify(b"materialx").is_err());
        assert!(object.validate(7).is_err());
        assert!(!valid_digest("../objects"));
        let mut invalid = object;
        invalid.checksum = invalid.checksum.to_uppercase();
        assert!(invalid.validate(100).is_err());
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
            public: Object::new(b"public"),
        };
        let mut next = inventory.clone();
        next.manifest.generation += 1;
        next.manifest.observed_ms += 1;
        assert!(inventory.same_prepared_material(&next));
        next.public = Object::new(b"other");
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
