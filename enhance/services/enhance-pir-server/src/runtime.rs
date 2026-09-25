//! Unequal-unit PIR execution. Immutable, content-addressed units survive generation changes.
use crate::ipir::{
    add_crs_blocks_assign_mod, add_intermediate_assign_mod, deserialize_first_dim_query,
    CachedShard, PreparedShard, ShardRuntime,
};
use crate::types::{DatabaseId, DatabaseLayout, ENHANCE_LAYOUT};
use crate::wire::read_crs_blocks;
use crate::{ipir::LoadError, matvec::MatvecConfig};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use enhance_pir::protocol::*;
use enhance_pir::types::{ITEM_SIZE_BITS, RECORDS_PER_ROW, RECORD_BYTES};
use inspiring::{QueryPackPreprocessed, RlweParams, TopKeyImages};
use ipir_sp::serialize::{deserialize_packing_keys, serialized_packing_keys_len};
use ipir_sp::server::{
    build_pack_preprocessed_blocks, pack_intermediate_blocks, published_c1_rows, CrsBlock,
};
use ipir_sp::YpirSchemeParams;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, Weak};

#[cfg(feature = "native-reinspiring")]
#[path = "native_runtime.rs"]
pub(crate) mod native_runtime;
#[cfg(feature = "native-reinspiring")]
pub use native_runtime::NativePacking as Packing;
pub fn modulus() -> u64 {
    #[cfg(feature = "native-reinspiring")]
    {
        enhance_pir::native::Q
    }
    #[cfg(not(feature = "native-reinspiring"))]
    {
        rlwe().q
    }
}

pub fn rlwe() -> &'static RlweParams {
    static RLWE: OnceLock<RlweParams> = OnceLock::new();
    RLWE.get_or_init(|| {
        ipir_sp::params_for_simplepir_profile(
            32768,
            ITEM_SIZE_BITS,
            ipir_sp::SimplePirProfile::P16Q48,
        )
        .expect("pinned profile")
        .0
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
    let allocated = spec.allocated_rows as usize * ENHANCE_LAYOUT.row_bytes();
    rows.reserve_exact(allocated.saturating_sub(rows.len()));
    rows.resize(allocated, 0);
    Ok(rows)
}

pub struct Evaluation {
    pub plan: DomainPlan,
    pub units: Vec<Arc<CachedShard>>,
}

impl Evaluation {
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
                .runtime
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
                unit.publication.reader(),
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
}

#[cfg(not(feature = "native-reinspiring"))]
pub struct Packing {
    _charge: super::packing_budget::Charge,
    pub params: YpirSchemeParams,
    pub public: Vec<u8>,
    preprocessed: Vec<QueryPackPreprocessed<'static>>,
    top: Option<TopKeyImages<'static>>,
    mapped: Option<inspiring::prepared::MappedPrepared<'static>>,
}

#[cfg(not(feature = "native-reinspiring"))]
impl Packing {
    pub(crate) fn write_prepared(&self, out: &mut impl std::io::Write) -> Result<(), String> {
        out.write_all(&(self.params.db_rows as u64).to_le_bytes())
            .map_err(|e| e.to_string())?;
        out.write_all(&self.public).map_err(|e| e.to_string())?;
        inspiring::prepared::write(
            out,
            rlwe(),
            &self.preprocessed,
            self.top.as_ref().ok_or("cannot rewrite mapped artifact")?,
        )
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
            preprocessed: Vec::new(),
            top: None,
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
        let preprocessed =
            build_pack_preprocessed_blocks(rlwe(), hint).map_err(|e| e.to_string())?;
        let public = published_c1_rows(&preprocessed, rlwe().q);
        let top = TopKeyImages::build(rlwe());
        charge.resident();
        crate::prepared_packing::observe_preparation(began.elapsed());
        Ok(Self {
            _charge: charge,
            params,
            public,
            preprocessed,
            top: Some(top),
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
    pub fn query_coefficients(
        &self,
        body: &[u8],
        binding: QueryBinding,
    ) -> Result<Vec<u64>, String> {
        if QueryBinding::decode(body)? != binding
            || binding.epoch != Sha256::digest(&self.public)[..8]
        {
            return Err("query session mismatch".into());
        }
        let packing_len = serialized_packing_keys_len(rlwe());
        let switched_len = (self.params.db_rows * self.params.query_bits).div_ceil(8);
        if body.len() != HEADER_BYTES + packing_len + switched_len {
            return Err("query framing length mismatch".into());
        }
        // Validate packing keys before sending any evaluation work.
        deserialize_packing_keys(rlwe(), &body[HEADER_BYTES..HEADER_BYTES + packing_len])
            .map_err(|e| e.to_string())?;
        deserialize_first_dim_query(rlwe(), &self.params, &body[HEADER_BYTES + packing_len..])
            .map_err(|e| e.to_string())
    }
    pub fn pack(&self, body: &[u8], intermediate: &[u64]) -> Result<Vec<u8>, String> {
        let binding = QueryBinding::decode(body)?;
        self.query_coefficients(body, binding)?;
        if intermediate.len() != self.params.db_cols || intermediate.iter().any(|v| *v >= modulus())
        {
            return Err("invalid worker intermediate".into());
        }
        let len = serialized_packing_keys_len(rlwe());
        let keys = deserialize_packing_keys(rlwe(), &body[HEADER_BYTES..HEADER_BYTES + len])
            .map_err(|e| e.to_string())?;
        let packed = if let Some(mapped) = &self.mapped {
            mapped.pack(intermediate, &keys)
        } else {
            pack_intermediate_blocks(
                intermediate,
                &keys,
                self.top.as_ref().expect("owned packing top images"),
                &self.preprocessed,
            )
        }
        .map_err(|e| e.to_string())?;
        let mut response = binding.encode();
        response.extend(ipir_sp::modulus_switch::serialize_rlwe_response_bodies(
            &packed,
            self.params.q_prime_1,
        ));
        Ok(response)
    }
}

/// Public session material survives independently of the serving representation.
pub struct PublishedPacking {
    pub logical_rows: u64,
    pub public: Vec<u8>,
    serving: Option<Packing>,
}
impl PublishedPacking {
    pub fn is_serving(&self) -> bool {
        self.serving.is_some()
    }
    pub fn new(logical_rows: u64, pack: Packing, retain_serving: bool) -> Self {
        Self {
            logical_rows,
            public: pack.public.clone(),
            serving: retain_serving.then_some(pack),
        }
    }
    pub fn metadata(logical_rows: u64, public: Vec<u8>) -> Self {
        Self {
            logical_rows,
            public,
            serving: None,
        }
    }
    pub fn reference(&self, shard_id: u64) -> Result<SessionRef, String> {
        Ok(SessionRef {
            shard_id,
            public_params_sha256: hex::encode(Sha256::digest(&self.public)),
            parameter_id: parameter_id(parameters(self.logical_rows)?.db_rows as u64)?,
        })
    }
    pub fn session(&self, manifest: &Manifest, shard_id: u64) -> ShardSession {
        ShardSession {
            session_id: hex::encode(manifest.session_id(shard_id).expect("validated session")),
            generation: manifest.generation,
            shard_id,
            params: parameters(self.logical_rows).expect("validated geometry"),
            public_params_base64: STANDARD.encode(&self.public),
        }
    }
    pub fn query_coefficients(
        &self,
        body: &[u8],
        binding: QueryBinding,
    ) -> Result<Vec<u64>, String> {
        self.serving
            .as_ref()
            .ok_or("packing moved to router")?
            .query_coefficients(body, binding)
    }
    pub fn pack(&self, body: &[u8], intermediate: &[u64]) -> Result<Vec<u8>, String> {
        self.serving
            .as_ref()
            .ok_or("packing moved to router")?
            .pack(body, intermediate)
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
                    .map(|u| (id.clone(), u.runtime.server.db().len() as u64 * 2))
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

    pub fn prepare(
        &mut self,
        plan: DomainPlan,
        mut read: impl FnMut(u64, usize) -> Result<Vec<u8>, String>,
    ) -> Result<Arc<Evaluation>, String> {
        plan.validate()?;
        let mut units = Vec::new();
        // A full-size setup keeps unchanged unit setup slices stable through logical growth.
        #[cfg(feature = "native-reinspiring")]
        let masks = enhance_pir::native::query_masks(plan.shard.id);
        #[cfg(not(feature = "native-reinspiring"))]
        let masks = {
            let params = parameters(32768)?;
            let client = ipir_sp::IPIRClient::from_profile(
                params.num_items,
                params.item_size_bits,
                ipir_sp::SimplePirProfile::P16Q48,
            )
            .map_err(|e| e.to_string())?;
            let setup =
                client.generate_public_query_setup_simplepir_from_seed(setup_seed(plan.shard.id));
            setup.polys().to_vec()
        };
        for (spec, identity) in plan.shard.units.iter().zip(&plan.units) {
            let id = identity.digest();
            if let Some(unit) = self.units.get(&id).and_then(Weak::upgrade) {
                units.push(unit);
                continue;
            }
            let bytes = identity.allocated_rows * 12288 * 2;
            if self
                .live_bytes()
                .checked_add(bytes)
                .and_then(|n| n.checked_add(super::control::OVERHEAD))
                .is_none_or(|n| n > super::control::RESIDENT_LIMIT)
            {
                return Err("worker resident admission refused".into());
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
                Ok(unit) => unit,
                Err(LoadError::Backend(error)) => return Err(error.to_string()),
                Err(LoadError::Artifact(_)) => {
                    let rows = unit_rows(&plan.shard, spec, &mut read)?;
                    if hex::encode(Sha256::digest(&rows)) != identity.content_sha256 {
                        return Err("canonical rows changed during preparation".into());
                    }
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
                    .map_err(|e| e.to_string())?;
                    drop(rows);
                    unit.persist(
                        &crate::ipir::shard_artifact_dir(&root, DatabaseId::Enhance, plan.shard.id),
                        DatabaseId::Enhance,
                        &layout,
                        rlwe(),
                    )
                    .map_err(|e| e.to_string())?
                }
            };
            let unit = Arc::new(unit);
            self.units.insert(id, Arc::downgrade(&unit));
            units.push(unit);
        }
        self.units.retain(|_, w| w.strong_count() > 0);
        Ok(Arc::new(Evaluation { plan, units }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use enhance_pir::ROW_BYTES;
    fn records(start: u64, count: usize) -> Result<Vec<u8>, String> {
        let mut bytes = vec![0; count * RECORD_BYTES];
        for (i, row) in bytes.chunks_exact_mut(RECORD_BYTES).enumerate() {
            row[..8].copy_from_slice(&(start + i as u64).to_le_bytes());
        }
        Ok(bytes)
    }

    #[test]
    fn encrypted_bootstrap_round_trip_and_cached_restart() {
        round_trip_and_restart(MatvecConfig::default());
    }

    #[cfg(feature = "cuda")]
    #[test]
    #[ignore = "requires NVIDIA GPU and NVRTC"]
    fn cuda_encrypted_bootstrap_round_trip_and_cached_restart() {
        let backend = MatvecConfig {
            matvec_backend: crate::matvec::Backend::Cuda,
            cuda_device: None,
        };
        backend.validate().unwrap();
        round_trip_and_restart(backend);
    }

    fn round_trip_and_restart(backend: MatvecConfig) {
        let dir = tempfile::tempdir().unwrap();
        let coverage = Lifecycle::default()
            .coverage(67, Geometry::default())
            .unwrap();
        let plan = plan(coverage.shards[0].clone(), records).unwrap();
        let mut engine = Engine::with_backend(dir.path(), backend);
        let eval = engine.prepare(plan.clone(), records).unwrap();
        let pack = Packing::new(
            4096,
            &eval.hint().unwrap(),
            &crate::PackingBudget::coordinator(),
        )
        .unwrap();
        let manifest = Manifest {
            recovery_epoch: 0,
            placement_revision: 0,
            domain_recovery_epochs: [(0, "0".into())].into(),
            schema_version: SCHEMA_VERSION,
            protocol_revision: PROTOCOL_REVISION.into(),
            network: "main".into(),
            pool: "ironwood".into(),
            generation: 1,
            anchor_height: 3428143,
            anchor_block_hash: "01".repeat(32),
            geometry: Geometry::default(),
            coverage,
            sessions: vec![pack.reference(0).unwrap()],
            unit_identities: [(0, plan.units.clone())].into(),
        };
        let mut old_manifest = manifest.clone();
        old_manifest.protocol_revision = "ironwood-enhance-pir-v5".into();
        assert!(
            enhance_pir::client::QuerySession::new(&old_manifest, pack.session(&manifest, 0))
                .is_err()
        );
        let mut old_session = pack.session(&manifest, 0);
        old_session.params.query_bits = 46;
        assert!(enhance_pir::client::QuerySession::new(&manifest, old_session).is_err());
        let mut old_plan = plan.clone();
        old_plan.units[0].parameter_id = old_plan.units[0].parameter_id.replace("-v7/", "-v6/");
        assert!(old_plan.validate().is_err());
        let client =
            enhance_pir::client::QuerySession::new(&manifest, pack.session(&manifest, 0)).unwrap();
        for position in [0, 32, 33, 66] {
            let (query, slot) = client.prepare_position(position).unwrap();
            let binding = QueryBinding::decode(query.body()).unwrap();
            let old_len = query.body().len() - 4096 * 2 / 8;
            assert!(pack
                .query_coefficients(&query.body()[..old_len], binding)
                .is_err());
            let coefficients = pack.query_coefficients(query.body(), binding).unwrap();
            let response = pack
                .pack(query.body(), &eval.evaluate(&coefficients).unwrap())
                .unwrap();
            let row = client.decode(query, &response).unwrap();
            assert_eq!(
                &row[slot * RECORD_BYTES..(slot + 1) * RECORD_BYTES],
                records(position, 1).unwrap()
            );
        }
        let again = engine
            .prepare(plan.clone(), |_, _| {
                panic!("cached data must not be reread")
            })
            .unwrap();
        assert!(Arc::ptr_eq(&eval.units[0], &again.units[0]));
        drop(again);
        drop(eval);
        let mut restarted = Engine::with_backend(dir.path(), backend);
        restarted
            .prepare(plan.clone(), |_, _| {
                Err("canonical history unavailable".into())
            })
            .unwrap();
        // CPU can reopen a GPU-created artifact, and CUDA can open the same CPU-readable bytes.
        let mut cpu = Engine::new(dir.path());
        let cpu_eval = cpu
            .prepare(plan.clone(), |_, _| panic!("artifact must be reusable"))
            .unwrap();
        let mut selected = Engine::with_backend(dir.path(), backend);
        let selected_eval = selected
            .prepare(plan, |_, _| panic!("artifact must be reusable"))
            .unwrap();
        let query = vec![rlwe().q - 1; 4096];
        assert_eq!(
            cpu_eval.evaluate(&query).unwrap(),
            selected_eval.evaluate(&query).unwrap()
        );
        std::thread::scope(|scope| {
            for _ in 0..4 {
                let eval = &selected_eval;
                let query = &query;
                let expected = cpu_eval.evaluate(query).unwrap();
                scope.spawn(move || assert_eq!(eval.evaluate(query).unwrap(), expected));
            }
        });
        drop(cpu_eval);
        drop(selected_eval);
        selected.collect_unused().unwrap();
        assert_eq!(selected.live_bytes(), 0);
    }

    #[cfg(feature = "cuda")]
    #[test]
    fn backend_load_failure_does_not_rebuild_or_replace_published_units() {
        let root = tempfile::tempdir().unwrap();
        let coverage = Lifecycle::default()
            .coverage(67, Geometry::default())
            .unwrap();
        let plan = plan(coverage.shards[0].clone(), records).unwrap();
        let mut cpu = Engine::new(root.path());
        let published = cpu.prepare(plan.clone(), records).unwrap();
        let query = vec![1; 4096];
        let before = published.evaluate(&query).unwrap();
        let mut failed = Engine::with_backend(
            root.path(),
            MatvecConfig {
                matvec_backend: crate::matvec::Backend::Cuda,
                cuda_device: Some(usize::MAX),
            },
        );
        assert!(failed
            .prepare(plan, |_, _| panic!("device failure must not rebuild"))
            .is_err());
        assert_eq!(failed.live_bytes(), 0);
        assert_eq!(published.evaluate(&query).unwrap(), before);
    }

    #[test]
    fn partial_preparation_failure_preserves_published_and_retries() {
        partial_preparation_failure(MatvecConfig::default());
    }

    #[cfg(feature = "cuda")]
    #[test]
    #[ignore = "requires NVIDIA GPU and NVRTC"]
    fn cuda_partial_preparation_failure_preserves_published_and_retries() {
        partial_preparation_failure(MatvecConfig {
            matvec_backend: crate::matvec::Backend::Cuda,
            cuda_device: None,
        });
    }

    fn partial_preparation_failure(backend: MatvecConfig) {
        use crate::matvec::testing::{scoped, Faults};
        use std::sync::atomic::Ordering::SeqCst;
        for cached in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let old_plan = plan(
                Lifecycle::default()
                    .coverage(67, Geometry::default())
                    .unwrap()
                    .shards[0]
                    .clone(),
                records,
            )
            .unwrap();
            let candidate = plan(
                Lifecycle::default()
                    .coverage(8192 * 33 + 17, Geometry::default())
                    .unwrap()
                    .shards[0]
                    .clone(),
                records,
            )
            .unwrap();
            assert_eq!(candidate.units.len(), 2);
            assert!(candidate.units.iter().all(|u| !old_plan.units.contains(u)));
            let mut engine = Engine::with_backend(root.path(), backend);
            let faults = Arc::new(Faults::default());
            let published = scoped(&faults, || engine.prepare(old_plan, records)).unwrap();
            let query = vec![1; published.plan.shard.logical_rows as usize];
            let before = published.evaluate(&query).unwrap();
            let published_sizes = engine.live_sizes();
            let published_bytes = faults.live_bytes.load(SeqCst);
            if cached {
                // Populate candidate artifacts through an independent CPU engine.
                let mut cache = Engine::new(root.path());
                drop(cache.prepare(candidate.clone(), records).unwrap());
            }
            faults
                .fail_prepare_at
                .store(faults.prepare_calls.load(SeqCst) + 2, SeqCst);
            let mut reads = 0;
            let failed = scoped(&faults, || {
                engine.prepare(candidate.clone(), |start, count| {
                    assert!(
                        !cached,
                        "backend failure must not rebuild a cached artifact"
                    );
                    reads += 1;
                    records(start, count)
                })
            });
            assert!(failed.is_err());
            assert_eq!(
                faults.prepare_calls.load(SeqCst),
                3,
                "must fail after preparing two candidate units"
            );
            assert_eq!(reads, if cached { 0 } else { 2 });
            assert_eq!(
                faults.live_bytes.load(SeqCst),
                published_bytes,
                "partial candidate kernels must be dropped"
            );
            assert_eq!(engine.live_sizes(), published_sizes);
            assert_eq!(published.evaluate(&query).unwrap(), before);
            faults.fail_prepare_at.store(0, SeqCst);
            let retry = scoped(&faults, || {
                engine.prepare(candidate.clone(), |start, count| {
                    assert!(!cached, "cached retry must not require canonical history");
                    records(start, count)
                })
            })
            .unwrap();
            let oracle_root = tempfile::tempdir().unwrap();
            let oracle = Engine::new(oracle_root.path())
                .prepare(candidate, records)
                .unwrap();
            let candidate_query = vec![1; retry.plan.shard.logical_rows as usize];
            assert_eq!(
                retry.evaluate(&candidate_query).unwrap(),
                oracle.evaluate(&candidate_query).unwrap()
            );
            assert_eq!(published.evaluate(&query).unwrap(), before);
            drop(retry);
            engine.collect_unused().unwrap();
            assert_eq!(engine.live_sizes(), published_sizes);
            assert_eq!(faults.live_bytes.load(SeqCst), published_bytes);
            drop(published);
            engine.collect_unused().unwrap();
            assert_eq!(faults.live_bytes.load(SeqCst), 0);
        }
    }

    #[test]
    fn unequal_units_match_monolithic_preprocessing_and_evaluation() {
        let directory = tempfile::tempdir().unwrap();
        let coverage = Lifecycle::default()
            .coverage(8192 * 33 + 17, Geometry::default())
            .unwrap();
        let shard = coverage.shards[0].clone();
        let domain = plan(shard.clone(), records).unwrap();
        assert_eq!(
            domain
                .units
                .iter()
                .map(|u| u.allocated_rows)
                .collect::<Vec<_>>(),
            [8192, 2048]
        );
        let mut engine = Engine::new(directory.path());
        let evaluation = engine.prepare(domain, records).unwrap();
        let mut rows = records(0, shard.records as usize).unwrap();
        rows.resize(16384 * ENHANCE_LAYOUT.row_bytes(), 0);
        let params = parameters(16384).unwrap();
        let client = ipir_sp::IPIRClient::from_profile(
            params.num_items,
            params.item_size_bits,
            ipir_sp::SimplePirProfile::P16Q48,
        )
        .unwrap();
        let setup = client.generate_public_query_setup_simplepir_from_seed(setup_seed(shard.id));
        let monolithic = PreparedShard::build(
            &DatabaseLayout {
                shard_rows: 16384,
                ..ENHANCE_LAYOUT
            },
            shard.id,
            0,
            "test".into(),
            &rows,
            rlwe(),
            setup.polys(),
        )
        .unwrap();
        assert_eq!(evaluation.hint().unwrap(), monolithic.crs_blocks);
        for target in [0, 8191, 8192, 10239, 16383] {
            let mut query = vec![0; 16384];
            query[target] = rlwe().q - 1;
            assert_eq!(
                evaluation.evaluate(&query).unwrap(),
                monolithic.runtime.evaluate(rlwe(), &query).unwrap()
            );
        }
    }

    #[test]
    fn tail_domain_reuses_b_units_and_decodes_b_at_unchanged_coordinates() {
        use ipir_sp::modulus_switch::recover_published_c1;
        use ipir_sp::serialize::serialize_packing_keys;

        const B_START: u64 = 32768;
        const B_ROWS: u64 = 2049;
        const SUFFIX_ROWS: u64 = 4096;
        let directory = tempfile::tempdir().unwrap();
        let shard = QueryShard {
            id: 7,
            global_row_start: B_START,
            records: B_ROWS * RECORDS_PER_ROW as u64,
            logical_rows: 4096,
            state: ShardState::Growing,
            units: Geometry::default()
                .units(B_ROWS * RECORDS_PER_ROW as u64)
                .unwrap(),
        };
        let b_plan = plan(shard, records).unwrap();
        assert_eq!(
            b_plan
                .units
                .iter()
                .map(|unit| (unit.local_row_start, unit.allocated_rows))
                .collect::<Vec<_>>(),
            [(0, 4096)]
        );

        // Prepare B independently in both contexts; sharing an Arc here would
        // make the artifact comparison circular.
        let plain_root = directory.path().join("plain");
        let tail_root = directory.path().join("tail");
        let plain = Engine::new(&plain_root)
            .prepare(b_plan.clone(), records)
            .unwrap();
        let b_in_tail = Engine::new(&tail_root)
            .prepare(b_plan.clone(), records)
            .unwrap();
        assert_eq!(plain.plan.units, b_in_tail.plan.units);
        for identity in &b_plan.units {
            let relative = PathBuf::from("artifacts-9")
                .join(identity.digest())
                .join("enhance")
                .join("shard-00000007");
            for artifact in ["metadata.json", "database.u16le", "partial-crs.bin"] {
                assert_eq!(
                    std::fs::read(plain_root.join(&relative).join(artifact)).unwrap(),
                    std::fs::read(tail_root.join(&relative).join(artifact)).unwrap(),
                    "B artifact {artifact} differs"
                );
            }
        }
        for (left, right) in plain.units.iter().zip(&b_in_tail.units) {
            let mut left_hint = Vec::new();
            let mut right_hint = Vec::new();
            std::io::Read::read_to_end(&mut left.publication.reader(), &mut left_hint).unwrap();
            std::io::Read::read_to_end(&mut right.publication.reader(), &mut right_hint).unwrap();
            assert_eq!(left_hint, right_hint, "B partial hints differ");
        }

        // A's suffix occupies a new B-seeded unit after B's entire 4K slot.
        let layout = DatabaseLayout {
            shard_rows: SUFFIX_ROWS as usize,
            ..ENHANCE_LAYOUT
        };
        let suffix = records(9_000_000, (SUFFIX_ROWS * RECORDS_PER_ROW as u64) as usize).unwrap();
        let params = parameters(32768).unwrap();
        let setup_client = ipir_sp::IPIRClient::from_profile(
            params.num_items,
            params.item_size_bits,
            ipir_sp::SimplePirProfile::P16Q48,
        )
        .unwrap();
        let setup = setup_client.generate_public_query_setup_simplepir_from_seed(setup_seed(7));
        let suffix_hash = hex::encode(Sha256::digest(&suffix));
        let extra = PreparedShard::build(
            &layout,
            7,
            4096,
            suffix_hash.clone(),
            &suffix,
            rlwe(),
            setup.polys(),
        )
        .unwrap()
        .persist(
            &directory.path().join("suffix"),
            DatabaseId::Enhance,
            &layout,
            rlwe(),
        )
        .unwrap();
        let mut tail_plan = b_plan.clone();
        tail_plan.shard.logical_rows = 8192;
        tail_plan.units.push(UnitIdentity {
            recovery_epoch: 0,
            table: "enhance".into(),
            shard_id: 7,
            local_row_start: 4096,
            allocated_rows: SUFFIX_ROWS,
            setup_sha256: hex::encode(Sha256::digest(setup_seed(7))),
            parameter_id: unit_parameter_id(SUFFIX_ROWS).unwrap(),
            content_sha256: suffix_hash,
        });
        let mut tail_units = b_in_tail.units.clone();
        tail_units.push(Arc::new(extra));
        let tail = Evaluation {
            plan: tail_plan,
            units: tail_units,
        };
        assert_eq!(&tail.plan.units[..b_plan.units.len()], b_plan.units);

        let plain_hint = plain.hint().unwrap();
        let tail_hint = tail.hint().unwrap();
        assert_ne!(
            plain_hint, tail_hint,
            "the whole-domain hint includes A's suffix"
        );
        for (evaluation, hint, logical_rows) in
            [(&*plain, &plain_hint, 4096), (&tail, &tail_hint, 8192)]
        {
            let packing =
                Packing::new(logical_rows, hint, &crate::PackingBudget::coordinator()).unwrap();
            let params = parameters(logical_rows).unwrap();
            let wallet = ipir_sp::IPIRClient::from_profile(
                params.num_items,
                params.item_size_bits,
                ipir_sp::SimplePirProfile::P16Q48,
            )
            .unwrap();
            let setup = wallet.generate_public_query_setup_simplepir_from_seed(setup_seed(7));
            let public = recover_published_c1(
                &packing.public,
                rlwe().d,
                params.db_cols / rlwe().d,
                rlwe().q,
            );
            let binding = QueryBinding {
                recovery_epoch: 0,
                session_id: [7; 32],
                request_id: [8; 16],
                anchor_hash: [9; 32],
                generation: 1,
                shard_id: 7,
                epoch: Sha256::digest(&packing.public)[..8].try_into().unwrap(),
            };
            let targets: &[usize] = if logical_rows == 8192 {
                &[0, 2047, 2048, 4096]
            } else {
                &[0, 2047, 2048]
            };
            for &row in targets {
                let (query, keys, seed) = wallet.generate_fresh_query_simplepir(&setup, row);
                let mut body = binding.encode();
                body.extend(serialize_packing_keys(wallet.rlwe_params(), &keys).unwrap());
                body.extend(query.to_switched_bytes(rlwe().q, params.query_bits));
                let coefficients = packing.query_coefficients(&body, binding).unwrap();
                let response = packing
                    .pack(&body, &evaluation.evaluate(&coefficients).unwrap())
                    .unwrap();
                assert_eq!(QueryBinding::decode(&response).unwrap(), binding);
                let decoded =
                    wallet.decode_response_simplepir(seed, &public, &response[HEADER_BYTES..]);
                let start = if row == 4096 {
                    9_000_000
                } else {
                    (B_START + row as u64) * RECORDS_PER_ROW as u64
                };
                assert_eq!(
                    &decoded[..ROW_BYTES],
                    records(start, RECORDS_PER_ROW).unwrap(),
                    "row {row} decoded at the wrong local coordinate in {logical_rows} rows"
                );
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum EvaluationError {
    #[error("invalid query coefficients")]
    InvalidQuery,
    #[error("{0}")]
    Unavailable(String),
}
