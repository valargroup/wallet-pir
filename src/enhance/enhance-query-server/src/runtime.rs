//! Unequal-unit PIR execution. Immutable, content-addressed units survive generation changes.
#[cfg(not(feature = "native-reinspiring"))]
use crate::ipir::deserialize_first_dim_query;
use crate::ipir::{
    add_crs_blocks_assign_mod, add_intermediate_assign_mod, CachedShard, PreparedShard,
    ShardRuntime,
};
use crate::protocol::*;
use crate::types::{DatabaseId, DatabaseLayout, ENHANCE_LAYOUT};
use crate::wire::read_crs_blocks;
use crate::{ipir::LoadError, matvec::MatvecConfig};
use crate::{ITEM_SIZE_BITS, RECORDS_PER_ROW, RECORD_BYTES};
#[cfg(not(feature = "native-reinspiring"))]
use base64::{engine::general_purpose::STANDARD, Engine as _};
use inspiring::RlweParams;
use ipir_sp::server::CrsBlock;
#[cfg(not(feature = "native-reinspiring"))]
use ipir_sp::YpirSchemeParams;
#[cfg(not(feature = "native-reinspiring"))]
use pir_runtime::packing::{PackingKeys, PackingSetup, PreparedPacking};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, Weak};

#[cfg(feature = "native-reinspiring")]
#[path = "native_runtime.rs"]
pub mod native_runtime;
#[cfg(feature = "native-reinspiring")]
pub use native_runtime::NativePacking as Packing;
pub fn modulus() -> u64 {
    #[cfg(feature = "native-reinspiring")]
    {
        pir_serving::modulus(32768, ITEM_SIZE_BITS)
    }
    #[cfg(not(feature = "native-reinspiring"))]
    {
        rlwe().q
    }
}

pub fn rlwe() -> &'static RlweParams {
    static RLWE: OnceLock<RlweParams> = OnceLock::new();
    RLWE.get_or_init(|| {
        pir_runtime::params::p16q48(32768, ITEM_SIZE_BITS)
            .expect("pinned profile")
            .rlwe()
            .clone()
    })
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DomainPlan {
    pub shard: QueryShard,
    pub units: Vec<UnitIdentity>,
}

impl DomainPlan {
    pub fn validate(&self) -> Result<(), String> {
        let geometry = Geometry::default();
        // The engine supports independently prepared plain units as well as
        // composed domains. Public manifests enforce canonical composition.
        let plain = self.shard.logical_rows == geometry.logical_rows(self.shard.records)?
            && self.shard.units == geometry.units(self.shard.records)?;
        let composed = self.shard.logical_rows == self.shard.expected_logical_rows(geometry)?
            && self.shard.units == self.shard.expected_units(geometry)?;
        if (!plain && !composed) || self.units.len() != self.shard.units.len() {
            return Err("invalid domain plan".into());
        }
        for (spec, unit) in self.shard.units.iter().zip(&self.units) {
            if unit.table != "enhance"
                || unit.shard_id != self.shard.id
                || unit.local_row_start != spec.local_row_start
                || unit.allocated_rows != spec.allocated_rows
                || unit.setup_sha256 != hex::encode(Sha256::digest(setup_seed(self.shard.id)))
                || unit.parameter_id != unit_parameter_id(unit.allocated_rows)?
                || unit.content_sha256.len() != 64
                || hex::decode(&unit.content_sha256).is_err()
            {
                return Err("incompatible mutable-unit identity".into());
            }
        }
        Ok(())
    }
}

pub fn plan(
    shard: QueryShard,
    mut read: impl FnMut(u64, usize) -> Result<Vec<u8>, String>,
) -> Result<DomainPlan, String> {
    let mut identities = Vec::new();
    for spec in &shard.units {
        let rows = unit_rows(&shard, spec, &mut read)?;
        identities.push(UnitIdentity {
            recovery_epoch: 0,
            table: "enhance".into(),
            shard_id: shard.id,
            local_row_start: spec.local_row_start,
            allocated_rows: spec.allocated_rows,
            setup_sha256: hex::encode(Sha256::digest(setup_seed(shard.id))),
            parameter_id: unit_parameter_id(spec.allocated_rows)?,
            content_sha256: hex::encode(Sha256::digest(&rows)),
        });
    }
    let plan = DomainPlan {
        shard,
        units: identities,
    };
    plan.validate()?;
    Ok(plan)
}

pub fn unit_rows(
    shard: &QueryShard,
    spec: &MutableUnit,
    read: &mut impl FnMut(u64, usize) -> Result<Vec<u8>, String>,
) -> Result<Vec<u8>, String> {
    let tail = shard.composed() && spec.local_row_start == 4096;
    let local = spec
        .local_row_start
        .checked_mul(RECORDS_PER_ROW as u64)
        .ok_or("unit range overflow")?;
    let (start, count) = if tail {
        (
            (shard.global_row_start - 4096) * RECORDS_PER_ROW as u64,
            4096 * RECORDS_PER_ROW as u64,
        )
    } else {
        (
            shard
                .global_row_start
                .checked_mul(RECORDS_PER_ROW as u64)
                .and_then(|s| s.checked_add(local))
                .ok_or("unit range overflow")?,
            shard
                .records
                .checked_sub(local)
                .ok_or("unit outside shard")?
                .min(spec.used_rows * RECORDS_PER_ROW as u64),
        )
    };
    let mut rows = read(start, count as usize)?;
    if rows.len() != count as usize * RECORD_BYTES {
        return Err("source returned wrong record count".into());
    }
    // Native queries omit selection coefficients past the domain's used rows,
    // which is exact only while every byte from there on is zero padding.
    if rows.len() > spec.used_rows as usize * ENHANCE_LAYOUT.row_bytes() {
        return Err("unit data past its used rows".into());
    }
    let allocated = spec.allocated_rows as usize * ENHANCE_LAYOUT.row_bytes();
    rows.reserve_exact(allocated.saturating_sub(rows.len()));
    rows.resize(allocated, 0);
    Ok(rows)
}

/// A plan's verified, prepared runtimes: the input a worker evaluates.
///
/// Only [`Engine::prepare`] constructs one, after the plan validates and every
/// unit's rows are bound to its committed digest, so a plan and a digest alone
/// are never enough to obtain one.
pub struct Evaluation {
    plan: DomainPlan,
    units: Vec<Arc<CachedShard>>,
}

/// Why [`Engine::prepare`] produced no evaluation, by what the caller must do.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PrepareError {
    /// The plan or its staged rows do not verify; the same input cannot succeed.
    InvalidSource(String),
    /// Restoring or building a runtime failed; the input verified.
    Runtime(String),
    /// The worker's resident budget cannot hold another unit.
    Admission(String),
}

impl std::fmt::Display for PrepareError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (Self::InvalidSource(message) | Self::Runtime(message) | Self::Admission(message)) =
            self;
        f.write_str(message)
    }
}

impl From<PrepareError> for String {
    fn from(error: PrepareError) -> Self {
        error.to_string()
    }
}

/// How [`Engine::prepare`] produced each unit's runtime.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct PrepareOutcome {
    /// Held live by another evaluation, so shared without any work.
    pub reused: usize,
    /// Restored from the verified on-disk artifact.
    pub restored: usize,
    /// Rebuilt from staged rows bound to the committed digest.
    pub built: usize,
}

impl PrepareOutcome {
    pub fn add(&mut self, other: Self) {
        self.reused += other.reused;
        self.restored += other.restored;
        self.built += other.built;
    }
}

impl Evaluation {
    pub fn plan(&self) -> &DomainPlan {
        &self.plan
    }

    pub fn units(&self) -> &[Arc<CachedShard>] {
        &self.units
    }

    pub fn evaluate(&self, coefficients: &[u64]) -> Result<Vec<u64>, EvaluationError> {
        let params =
            parameters(self.plan.shard.logical_rows).map_err(EvaluationError::Unavailable)?;
        if coefficients.len() != params.db_rows || coefficients.iter().any(|v| *v >= modulus()) {
            return Err(EvaluationError::InvalidQuery);
        }
        let mut answer = vec![0; params.db_cols];
        for (unit, spec) in self.units.iter().zip(&self.plan.units) {
            let start = spec.local_row_start as usize;
            let end = start + spec.allocated_rows as usize;
            let partial = unit
                .runtime()
                .evaluate(rlwe(), &coefficients[start..end])
                .map_err(|e| EvaluationError::Unavailable(e.to_string()))?;
            add_intermediate_assign_mod(&mut answer, &partial, modulus())
                .map_err(|e| EvaluationError::Unavailable(e.to_string()))?;
        }
        Ok(answer)
    }

    pub fn hint(&self) -> Result<Vec<CrsBlock>, String> {
        let params = parameters(self.plan.shard.logical_rows)?;
        let mut combined: Option<Vec<CrsBlock>> = None;
        for unit in &self.units {
            let blocks = read_crs_blocks(
                unit.publication().reader(),
                params.db_cols / rlwe().d,
                rlwe().d,
            )
            .map_err(|e| e.to_string())?;
            if let Some(sum) = &mut combined {
                add_crs_blocks_assign_mod(sum, &blocks, rlwe()).map_err(|e| e.to_string())?;
            } else {
                combined = Some(blocks);
            }
        }
        combined.ok_or_else(|| "empty query domain".into())
    }

    /// Borrow the existing MPH1 publication only when the generic combiner is
    /// the identity operation. Construction by Engine binds the layout, setup
    /// slice and padded rows; this check also excludes partial/composed domains
    /// and incompatible output parameters. Unallocated domain rows are zero.
    pub(crate) fn verified_hint_artifact(
        &self,
    ) -> Result<Option<crate::artifact::PublicationArtifact>, String> {
        let ([unit], [identity], [spec]) = (
            self.units.as_slice(),
            self.plan.units.as_slice(),
            self.plan.shard.units.as_slice(),
        ) else {
            return Ok(None);
        };
        let params = parameters(self.plan.shard.logical_rows)?;
        let runtime = unit.runtime();
        let local = runtime.server.params();
        let layout = DatabaseLayout {
            shard_rows: identity.allocated_rows as usize,
            ..ENHANCE_LAYOUT
        };
        let (ring, expected) = crate::ipir::shard_parameters(&layout).map_err(|e| e.to_string())?;
        if self.plan.validate().is_err()
            || self.plan.shard.composed()
            || spec.local_row_start != 0
            || identity.allocated_rows > self.plan.shard.logical_rows
            || !layout.shard_rows.is_multiple_of(rlwe().d)
            || runtime.query_row_start != 0
            || runtime.shard_id != self.plan.shard.id
            || runtime.rows_sha256 != identity.content_sha256
            || ring.d != rlwe().d
            || ring.q != rlwe().q
            || local.db_rows != expected.db_rows
            || local.db_cols != expected.db_cols
            || local.p != expected.p
            || local.item_size_bits != expected.item_size_bits
            || local.query_bits != expected.query_bits
            || local.q_prime_1 != expected.q_prime_1
            || local.db_cols != params.db_cols
            || local.p != params.p
        {
            return Ok(None);
        }
        // The same scanner as hint(), without allocating decoded matrices.
        // Includes the pinned reader's digest, exact length and clean EOF.
        unit.publication()
            .validate(params.db_cols / rlwe().d, rlwe().d)
            .map_err(|e| e.to_string())?;
        Ok(Some(unit.publication().clone()))
    }
}

#[cfg(not(feature = "native-reinspiring"))]
pub struct Packing {
    _charge: super::packing_budget::Charge,
    pub params: YpirSchemeParams,
    pub public: Vec<u8>,
    material: Option<PreparedPacking<'static>>,
    mapped: Option<inspiring::prepared::MappedPrepared<'static>>,
}

#[cfg(not(feature = "native-reinspiring"))]
impl Packing {
    pub(crate) fn write_prepared(&self, out: &mut impl std::io::Write) -> Result<(), String> {
        out.write_all(&(self.params.db_rows as u64).to_le_bytes())
            .map_err(|e| e.to_string())?;
        out.write_all(&self.public).map_err(|e| e.to_string())?;
        self.material
            .as_ref()
            .ok_or("cannot rewrite mapped artifact")?
            .write(out, rlwe())
            .map_err(|e| e.to_string())
    }
    /// Read-only mappings own their file pages through all in-flight query pins.
    /// Router artifacts are immutable: downloads replace inodes atomically and
    /// cache GC only unlinks. No path truncates a published artifact in place.
    pub(crate) fn map_prepared(
        file: &std::fs::File,
        rows: u64,
        budget: &crate::PackingBudget,
    ) -> Result<Self, String> {
        let mut charge = super::packing_budget::Charge::mapping(budget)?;
        let params = parameters(rows)?;
        let public_len =
            (params.db_cols * ipir_sp::modulus_switch::modulus_bits(rlwe().q)).div_ceil(8);
        // SAFETY: private router cache files are immutable once renamed into
        // place; the read-only mmap owns the inode across replacement/unlink.
        // Filesystem administrators must preserve that immutable-inode contract.
        let map = unsafe { memmap2::MmapOptions::new().map(file) }.map_err(|e| e.to_string())?;
        let mapped = inspiring::prepared::MappedPrepared::new(
            map,
            rlwe(),
            params.db_cols / rlwe().d,
            8 + public_len,
        )
        .map_err(|e| e.to_string())?;
        let prefix = mapped.prefix();
        if u64::from_le_bytes(prefix[..8].try_into().unwrap()) != params.db_rows as u64 {
            return Err("prepared row count mismatch".into());
        }
        let public = prefix[8..].to_vec();
        charge.resident();
        Ok(Self {
            _charge: charge,
            params,
            public,
            material: None,
            mapped: Some(mapped),
        })
    }

    pub fn new(
        logical_rows: u64,
        hint: &[CrsBlock],
        budget: &crate::PackingBudget,
    ) -> Result<Self, String> {
        let began = std::time::Instant::now();
        let mut charge = super::packing_budget::Charge::prepare(budget)?;
        let params = parameters(logical_rows)?;
        // These fixed public masks depend only on the pinned parameter set,
        // so generations share them while retaining their own hint material.
        static SETUP: OnceLock<PackingSetup<'static>> = OnceLock::new();
        let setup = SETUP.get_or_init(|| PackingSetup::new(rlwe())).clone();
        let material = PreparedPacking::prepare(rlwe(), hint, setup, None, |_| false)
            .map_err(|e| e.to_string())?;
        let public = material.public_rows(rlwe().q);
        charge.resident();
        crate::prepared_packing::observe_preparation(began.elapsed());
        Ok(Self {
            _charge: charge,
            params,
            public,
            material: Some(material),
            mapped: None,
        })
    }
    pub fn reference(&self, shard_id: u64) -> Result<SessionRef, String> {
        Ok(SessionRef {
            shard_id,
            public_params_sha256: hex::encode(Sha256::digest(&self.public)),
            parameter_id: parameter_id(self.params.db_rows as u64)?,
        })
    }
    pub fn session(&self, manifest: &Manifest, shard_id: u64) -> ShardSession {
        ShardSession {
            session_id: hex::encode(
                manifest
                    .session_id(shard_id)
                    .expect("validated published domain"),
            ),
            generation: manifest.generation,
            shard_id,
            params: self.params.clone(),
            public_params_base64: STANDARD.encode(&self.public),
        }
    }
    /// q48 queries always select over every logical row; `_query_rows` is
    /// only for the native profile.
    pub fn query_coefficients(
        &self,
        body: &[u8],
        binding: QueryBinding,
        _query_rows: usize,
    ) -> Result<Vec<u64>, String> {
        if QueryBinding::decode(body)? != binding
            || binding.epoch != Sha256::digest(&self.public)[..8]
        {
            return Err("query session mismatch".into());
        }
        let packing_len = PackingKeys::encoded_len(rlwe());
        let switched_len = (self.params.db_rows * self.params.query_bits).div_ceil(8);
        if body.len() != HEADER_BYTES + packing_len + switched_len {
            return Err("query framing length mismatch".into());
        }
        // Validate packing keys before sending any evaluation work.
        PackingKeys::decode(rlwe(), &body[HEADER_BYTES..HEADER_BYTES + packing_len])
            .map_err(|e| e.to_string())?;
        deserialize_first_dim_query(rlwe(), &self.params, &body[HEADER_BYTES + packing_len..])
            .map_err(|e| e.to_string())
    }
    pub fn pack(
        &self,
        body: &[u8],
        intermediate: &[u64],
        query_rows: usize,
    ) -> Result<Vec<u8>, String> {
        let binding = QueryBinding::decode(body)?;
        self.query_coefficients(body, binding, query_rows)?;
        if intermediate.len() != self.params.db_cols || intermediate.iter().any(|v| *v >= modulus())
        {
            return Err("invalid worker intermediate".into());
        }
        let len = PackingKeys::encoded_len(rlwe());
        let keys = PackingKeys::decode(rlwe(), &body[HEADER_BYTES..HEADER_BYTES + len])
            .map_err(|e| e.to_string())?;
        let q_prime_1 = self.params.q_prime_1;
        let packed = if let Some(mapped) = &self.mapped {
            mapped
                .pack(intermediate, keys.upstream())
                .map(|packed| pir_runtime::packing::response_bodies(&packed, q_prime_1))
        } else {
            self.material
                .as_ref()
                .expect("owned packing material")
                .pack(intermediate, &keys, q_prime_1)
        }
        .map_err(|e| e.to_string())?;
        let mut response = binding.encode();
        response.extend(packed);
        Ok(response)
    }
}

/// A published shard's public session material. Packing routers own the
/// serving representation.
pub struct PublishedPacking {
    pub logical_rows: u64,
    pub public: Vec<u8>,
}
impl PublishedPacking {
    pub fn metadata(logical_rows: u64, public: Vec<u8>) -> Self {
        Self {
            logical_rows,
            public,
        }
    }
    pub fn reference(&self, shard_id: u64) -> Result<SessionRef, String> {
        Ok(SessionRef {
            shard_id,
            public_params_sha256: hex::encode(Sha256::digest(&self.public)),
            parameter_id: parameter_id(parameters(self.logical_rows)?.db_rows as u64)?,
        })
    }
}

pub struct Engine {
    backend: MatvecConfig,
    root: PathBuf,
    units: BTreeMap<String, Weak<CachedShard>>,
}

impl Engine {
    pub fn new(root: &Path) -> Self {
        Self::with_backend(root, MatvecConfig::default())
    }

    pub fn with_backend(root: &Path, backend: MatvecConfig) -> Self {
        Self {
            backend,
            root: root.join("artifacts-9"),
            units: BTreeMap::new(),
        }
    }

    pub fn live_bytes(&self) -> u64 {
        self.live_sizes().values().sum()
    }

    pub fn live_sizes(&self) -> BTreeMap<String, u64> {
        self.units
            .iter()
            .filter_map(|(id, unit)| {
                unit.upgrade()
                    .map(|u| (id.clone(), u.runtime().server.db().len() as u64 * 2))
            })
            .collect()
    }

    /// Remove only content-addressed directories after strong runtime references drain.
    pub fn collect_unused(&mut self) -> Result<(), String> {
        self.units.retain(|_, unit| unit.strong_count() > 0);
        if !self.root.exists() {
            return Ok(());
        }
        for entry in std::fs::read_dir(&self.root).map_err(|e| e.to_string())? {
            let entry = entry.map_err(|e| e.to_string())?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.len() == 64
                && hex::decode(&name).is_ok()
                && !self.units.contains_key(&name)
                && entry.file_type().map_err(|e| e.to_string())?.is_dir()
            {
                std::fs::remove_dir_all(entry.path()).map_err(|e| e.to_string())?;
            }
        }
        Ok(())
    }

    /// Restores, reuses or builds every unit of `plan` from verified input and
    /// reports which it did. `read` supplies staged source rows, which are bound
    /// to the unit's committed digest before anything is built from them.
    pub fn prepare(
        &mut self,
        plan: DomainPlan,
        mut read: impl FnMut(u64, usize) -> Result<Vec<u8>, String>,
    ) -> Result<(Arc<Evaluation>, PrepareOutcome), PrepareError> {
        plan.validate().map_err(PrepareError::InvalidSource)?;
        let mut outcome = PrepareOutcome::default();
        let mut units = Vec::new();
        // A full-size setup keeps unchanged unit setup slices stable through logical growth.
        #[cfg(feature = "native-reinspiring")]
        let masks = crate::native::query_masks(plan.shard.id);
        #[cfg(not(feature = "native-reinspiring"))]
        let masks = {
            let params = parameters(32768).map_err(PrepareError::Runtime)?;
            let client = ipir_sp::IPIRClient::from_profile(
                params.num_items,
                params.item_size_bits,
                ipir_sp::SimplePirProfile::P16Q48,
            )
            .map_err(|e| PrepareError::Runtime(e.to_string()))?;
            let setup =
                client.generate_public_query_setup_simplepir_from_seed(setup_seed(plan.shard.id));
            setup.polys().to_vec()
        };
        for (spec, identity) in plan.shard.units.iter().zip(&plan.units) {
            let id = identity.digest();
            if let Some(unit) = self.units.get(&id).and_then(Weak::upgrade) {
                units.push(unit);
                outcome.reused += 1;
                continue;
            }
            let bytes = identity.allocated_rows * 12288 * 2;
            if self
                .live_bytes()
                .checked_add(bytes)
                .and_then(|n| n.checked_add(crate::placement::OVERHEAD))
                .is_none_or(|n| n > crate::placement::RESIDENT_LIMIT)
            {
                return Err(PrepareError::Admission(
                    "worker resident admission refused".into(),
                ));
            }
            let layout = DatabaseLayout {
                shard_rows: identity.allocated_rows as usize,
                ..ENHANCE_LAYOUT
            };
            let root = self.root.join(&id);
            let unit = match ShardRuntime::load_cached_with_backend(
                &root,
                DatabaseId::Enhance,
                &layout,
                plan.shard.id,
                identity.local_row_start as usize,
                &identity.content_sha256,
                rlwe(),
                self.backend,
            ) {
                Ok(unit) => {
                    outcome.restored += 1;
                    unit
                }
                Err(LoadError::Backend(error)) => {
                    return Err(PrepareError::Runtime(error.to_string()))
                }
                Err(LoadError::Artifact(_)) => {
                    let rows = unit_rows(&plan.shard, spec, &mut read)
                        .map_err(PrepareError::InvalidSource)?;
                    if hex::encode(Sha256::digest(&rows)) != identity.content_sha256 {
                        return Err(PrepareError::InvalidSource(
                            "canonical rows changed during preparation".into(),
                        ));
                    }
                    outcome.built += 1;
                    let unit = PreparedShard::build_with_backend(
                        &layout,
                        plan.shard.id,
                        identity.local_row_start as usize,
                        identity.content_sha256.clone(),
                        &rows,
                        rlwe(),
                        &masks,
                        self.backend,
                    )
                    .map_err(|e| PrepareError::Runtime(e.to_string()))?;
                    drop(rows);
                    unit.persist(
                        &crate::ipir::shard_artifact_dir(&root, DatabaseId::Enhance, plan.shard.id),
                        DatabaseId::Enhance,
                        &layout,
                        rlwe(),
                    )
                    .map_err(|e| PrepareError::Runtime(e.to_string()))?
                }
            };
            let unit = Arc::new(unit);
            self.units.insert(id, Arc::downgrade(&unit));
            units.push(unit);
        }
        self.units.retain(|_, w| w.strong_count() > 0);
        Ok((Arc::new(Evaluation { plan, units }), outcome))
    }
}

#[cfg(test)]
#[path = "runtime/tests.rs"]
mod tests;

#[derive(Debug, thiserror::Error)]
pub enum EvaluationError {
    #[error("invalid query coefficients")]
    InvalidQuery,
    #[error("{0}")]
    Unavailable(String),
}
