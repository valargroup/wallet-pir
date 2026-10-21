//! Complete prepared Enhance material, independent of placement and publication time.
//! The publisher writes this inventory; serving roles validate it before loading.
use crate::{
    prepared_packing::{self, Artifact},
    protocol::{self, Coverage, Manifest, SessionRef, MAX_QUERY_SHARDS},
    runtime::DomainPlan,
    ROW_BYTES,
};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

pub const VERSION: u16 = 1;
pub const PREFIX: &str = "/provider/enhance/v1/";
pub const METADATA_LIMIT: u64 = 2 * 1024 * 1024;
pub const MAX_OBJECT_BYTES: u64 = 32768 * ROW_BYTES as u64;

/// Durable distribution identity. Source observation time is deliberately kept
/// out of it: a conditional metadata response cannot grant fresh authority.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Current {
    pub version: u16,
    pub revision: u64,
    /// Most recent explicit withdrawal, preserved across ordinary publication.
    pub withdrawal_revision: u64,
    pub recovery_epoch: u64,
    pub revoked: std::collections::BTreeSet<String>,
    pub eligible: bool,
    pub manifest: Option<Manifest>,
    pub inventory: Option<Object>,
}

impl Current {
    pub fn validate(&self) -> Result<(), String> {
        if self.version != VERSION
            || self.revision == 0
            || self.withdrawal_revision > self.revision
            || self.revoked.iter().any(|id| !protocol::canonical_hash(id))
            || self.manifest.is_some() != self.inventory.is_some()
            || (self.eligible && self.manifest.is_none())
        {
            return Err("invalid Enhance distribution authority".into());
        }
        if let (Some(manifest), Some(inventory)) = (&self.manifest, &self.inventory) {
            manifest.validate()?;
            if manifest.recovery_epoch > self.recovery_epoch || inventory.bytes > METADATA_LIMIT {
                return Err("publication exceeds its authority or metadata limit".into());
            }
            inventory.validate()?;
        }
        Ok(())
    }

    pub fn succeeds(&self, previous: &Self) -> bool {
        self.validate().is_ok()
            && previous.validate().is_ok()
            && self.revision >= previous.revision
            && self.withdrawal_revision >= previous.withdrawal_revision
            && self.recovery_epoch >= previous.recovery_epoch
            && previous.revoked.is_subset(&self.revoked)
            && (self.revision > previous.revision || self == previous)
            && self.manifest.as_ref().map(|m| m.generation)
                >= previous.manifest.as_ref().map(|m| m.generation)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorityRequest {
    pub manifest_sha256: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorityResponse {
    pub current: Current,
    pub authorized_manifest_sha256: String,
    /// Time of an actual successful node check, not an HTTP heartbeat.
    pub observed_unix_ms: u64,
}

impl AuthorityResponse {
    /// The existing five-second serving-authority window, measured from a
    /// source observation. Receiving or caching this response cannot renew it.
    pub fn fresh(&self, now_unix_ms: u64) -> Result<(), String> {
        self.current.validate()?;
        if !self.current.eligible
            || !protocol::canonical_hash(&self.authorized_manifest_sha256)
            || self.observed_unix_ms == 0
            || now_unix_ms
                .checked_sub(self.observed_unix_ms)
                .is_none_or(|age| age > pir_control::CONTROL_WATCHDOG.as_millis() as u64)
        {
            return Err("Enhance source authority is absent or expired".into());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Object {
    pub sha256: String,
    pub bytes: u64,
}

impl Object {
    pub fn validate(&self) -> Result<(), String> {
        // Reference packing can be larger than the source rows. Native and
        // reference formats retain their own stricter shape checks below.
        let limit = if cfg!(feature = "native-reinspiring") {
            MAX_OBJECT_BYTES
        } else {
            MAX_OBJECT_BYTES.max(prepared_packing::expected_len(32768)?)
        };
        if !protocol::canonical_hash(&self.sha256) || self.bytes == 0 || self.bytes > limit {
            return Err("invalid publication object".into());
        }
        Ok(())
    }

    pub fn path(&self, root: &Path) -> Result<PathBuf, String> {
        self.validate()?;
        Ok(root.join("objects").join(&self.sha256))
    }

    pub fn verify(&self, root: &Path) -> Result<(), String> {
        let path = self.path(root)?;
        if std::fs::metadata(&path).map_err(|e| e.to_string())?.len() != self.bytes
            || prepared_packing::hash_file(&path)? != self.sha256
        {
            return Err("publication object checksum or length mismatch".into());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Domain {
    pub plan: DomainPlan,
    pub rows: Vec<Object>,
    pub hint: Object,
    pub public: Object,
    pub packing: Artifact,
}

impl Domain {
    pub fn reference(&self) -> Result<SessionRef, String> {
        Ok(SessionRef {
            shard_id: self.plan.shard.id,
            public_params_sha256: self.public.sha256.clone(),
            parameter_id: protocol::parameter_id(self.plan.shard.logical_rows)?,
        })
    }

    pub fn packing_object(&self) -> Object {
        Object {
            sha256: self.packing.sha256.clone(),
            bytes: self.packing.bytes,
        }
    }

    /// Publication counters and placement do not change the required material.
    pub fn supports(&self, manifest: &Manifest) -> Result<bool, String> {
        manifest.validate()?;
        Ok(manifest.coverage.shards.iter().any(|shard| {
            shard.id == self.plan.shard.id
                && shard.logical_rows == self.plan.shard.logical_rows
                && shard.records == self.plan.shard.records
                && manifest.unit_identities.get(&shard.id) == Some(&self.plan.units)
        }) && manifest
            .sessions
            .iter()
            .any(|s| self.reference().is_ok_and(|reference| *s == reference)))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Inventory {
    pub version: u16,
    pub network: String,
    pub profile: String,
    pub coverage: Coverage,
    pub domains: Vec<Domain>,
}

impl Inventory {
    /// Validate all identities and declared sizes before loading any large object.
    pub fn validate(&self) -> Result<(), String> {
        if self.version != VERSION
            || self.network != "main"
            || self.profile != protocol::PROTOCOL_REVISION
            || self.domains.len() > MAX_QUERY_SHARDS as usize
            || self.domains.len() != self.coverage.shards.len()
        {
            return Err("incompatible Enhance publication inventory".into());
        }
        self.coverage.validate(protocol::Geometry::default())?;
        for (domain, shard) in self.domains.iter().zip(&self.coverage.shards) {
            if domain.plan.shard != *shard || domain.rows.len() != domain.plan.units.len() {
                return Err("publication domain or ordered units differ".into());
            }
            domain.plan.validate()?;
            for (rows, unit) in domain.rows.iter().zip(&domain.plan.units) {
                rows.validate()?;
                if rows.sha256 != unit.content_sha256
                    || rows.bytes != unit.allocated_rows * ROW_BYTES as u64
                {
                    return Err("publication rows differ from unit identity".into());
                }
            }
            domain.hint.validate()?;
            domain.public.validate()?;
            domain.packing_object().validate()?;
            let parameters = protocol::parameters(shard.logical_rows)?;
            #[cfg(feature = "native-reinspiring")]
            let public_bytes = crate::native::public_len(crate::native::COLS);
            #[cfg(not(feature = "native-reinspiring"))]
            let public_bytes = (parameters.db_cols
                * ipir_sp::modulus_switch::modulus_bits(crate::runtime::rlwe().q))
            .div_ceil(8);
            let hint_bytes = crate::wire::crs_encoded_len(
                parameters.db_cols / crate::runtime::rlwe().d,
                crate::runtime::rlwe().d,
            )
            .map_err(|e| e.to_string())?;
            if domain.public.bytes != public_bytes as u64 || domain.hint.bytes != hint_bytes {
                return Err("publication hint or public parameter size differs".into());
            }
            domain
                .packing
                .validate(shard.logical_rows, &domain.public.sha256)?;
        }
        Ok(())
    }

    pub fn supports(&self, manifest: &Manifest) -> Result<bool, String> {
        self.validate()?;
        manifest.validate()?;
        if self.coverage != manifest.coverage {
            return Ok(false);
        }
        for domain in &self.domains {
            if !domain.supports(manifest)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Verify every referenced file before a publisher makes the inventory current.
    pub fn verify(&self, root: &Path) -> Result<(), String> {
        self.validate()?;
        for domain in &self.domains {
            for object in domain.rows.iter().chain([&domain.hint, &domain.public]) {
                object.verify(root)?;
            }
            domain.packing_object().verify(root)?;
        }
        Ok(())
    }
}
