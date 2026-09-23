//! Unequal-unit PIR execution. Immutable, content-addressed units survive generation changes.
use crate::ipir::{
    add_crs_blocks_assign_mod, add_intermediate_assign_mod, deserialize_first_dim_query,
    CachedShard, PreparedShard, ShardRuntime,
};
use crate::types::{DatabaseId, DatabaseLayout, ENHANCE_LAYOUT};
use crate::wire::read_crs_blocks;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use enhance_pir::types::{ITEM_SIZE_BITS, RECORDS_PER_ROW, RECORD_BYTES};
use enhance_pir::v4::*;
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

pub fn rlwe() -> &'static RlweParams {
    static RLWE: OnceLock<RlweParams> = OnceLock::new();
    RLWE.get_or_init(|| {
        ipir_sp::params_for_simplepir_profile(
            32768,
            ITEM_SIZE_BITS,
            ipir_sp::SimplePirProfile::P16Q49,
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
        if self.shard.logical_rows != geometry.logical_rows(self.shard.records)?
            || self.shard.units != geometry.units(self.shard.records)?
            || self.units.len() != self.shard.units.len()
        {
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
    let local = spec
        .local_row_start
        .checked_mul(RECORDS_PER_ROW as u64)
        .ok_or("unit range overflow")?;
    let start = shard
        .global_row_start
        .checked_mul(RECORDS_PER_ROW as u64)
        .and_then(|s| s.checked_add(local))
        .ok_or("unit range overflow")?;
    let count = shard
        .records
        .checked_sub(local)
        .ok_or("unit outside shard")?
        .min(spec.used_rows * RECORDS_PER_ROW as u64);
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
    pub fn evaluate(&self, coefficients: &[u64]) -> Result<Vec<u64>, String> {
        let params = parameters(self.plan.shard.logical_rows)?;
        if coefficients.len() != params.db_rows || coefficients.iter().any(|v| *v >= rlwe().q) {
            return Err("invalid query coefficients".into());
        }
        let mut answer = vec![0; params.db_cols];
        for (unit, spec) in self.units.iter().zip(&self.plan.units) {
            let start = spec.local_row_start as usize;
            let end = start + spec.allocated_rows as usize;
            let partial = unit
                .runtime
                .evaluate(rlwe(), &coefficients[start..end])
                .map_err(|e| e.to_string())?;
            add_intermediate_assign_mod(&mut answer, &partial, rlwe().q)
                .map_err(|e| e.to_string())?;
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

pub struct Packing {
    pub params: YpirSchemeParams,
    pub public: Vec<u8>,
    preprocessed: Vec<QueryPackPreprocessed<'static>>,
    top: TopKeyImages<'static>,
}

impl Packing {
    pub fn new(logical_rows: u64, hint: &[CrsBlock]) -> Result<Self, String> {
        let params = parameters(logical_rows)?;
        let preprocessed =
            build_pack_preprocessed_blocks(rlwe(), hint).map_err(|e| e.to_string())?;
        let public = published_c1_rows(&preprocessed, rlwe().q);
        Ok(Self {
            params,
            public,
            preprocessed,
            top: TopKeyImages::build(rlwe()),
        })
    }
    pub fn reference(&self, shard_id: u64) -> Result<SessionRef, String> {
        Ok(SessionRef {
            shard_id,
            public_params_sha256: hex::encode(Sha256::digest(&self.public)),
            parameter_id: parameter_id(self.params.db_rows as u64)?,
        })
    }
    pub fn session(&self, generation: u64, shard_id: u64) -> ShardSession {
        ShardSession {
            generation,
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
        if intermediate.len() != self.params.db_cols || intermediate.iter().any(|v| *v >= rlwe().q)
        {
            return Err("invalid worker intermediate".into());
        }
        let len = serialized_packing_keys_len(rlwe());
        let keys = deserialize_packing_keys(rlwe(), &body[HEADER_BYTES..HEADER_BYTES + len])
            .map_err(|e| e.to_string())?;
        let packed = pack_intermediate_blocks(intermediate, &keys, &self.top, &self.preprocessed)
            .map_err(|e| e.to_string())?;
        let mut response = binding.encode();
        response.extend(ipir_sp::modulus_switch::serialize_rlwe_response_bodies(
            &packed,
            self.params.q_prime_1,
        ));
        Ok(response)
    }
}

pub struct Engine {
    root: PathBuf,
    units: BTreeMap<String, Weak<CachedShard>>,
}

impl Engine {
    pub fn new(root: &Path) -> Self {
        Self {
            root: root.join("v4-artifacts-9"),
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
        let params = parameters(32768)?;
        let client = ipir_sp::IPIRClient::from_profile(
            params.num_items,
            params.item_size_bits,
            ipir_sp::SimplePirProfile::P16Q49,
        )
        .map_err(|e| e.to_string())?;
        let setup =
            client.generate_public_query_setup_simplepir_from_seed(setup_seed(plan.shard.id));
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
            let unit = match ShardRuntime::load_cached(
                &root,
                DatabaseId::Enhance,
                &layout,
                plan.shard.id,
                identity.local_row_start as usize,
                &identity.content_sha256,
                rlwe(),
            ) {
                Ok(unit) => unit,
                Err(_) => {
                    let rows = unit_rows(&plan.shard, spec, &mut read)?;
                    if hex::encode(Sha256::digest(&rows)) != identity.content_sha256 {
                        return Err("canonical rows changed during preparation".into());
                    }
                    let unit = PreparedShard::build(
                        &layout,
                        plan.shard.id,
                        identity.local_row_start as usize,
                        identity.content_sha256.clone(),
                        &rows,
                        rlwe(),
                        setup.polys(),
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
    fn records(start: u64, count: usize) -> Result<Vec<u8>, String> {
        let mut bytes = vec![0; count * RECORD_BYTES];
        for (i, row) in bytes.chunks_exact_mut(RECORD_BYTES).enumerate() {
            row[..8].copy_from_slice(&(start + i as u64).to_le_bytes());
        }
        Ok(bytes)
    }

    #[test]
    fn encrypted_bootstrap_round_trip_and_cached_restart() {
        let dir = tempfile::tempdir().unwrap();
        let coverage = Lifecycle::default()
            .coverage(67, Geometry::default())
            .unwrap();
        let plan = plan(coverage.shards[0].clone(), records).unwrap();
        let mut engine = Engine::new(dir.path());
        let eval = engine.prepare(plan.clone(), records).unwrap();
        let pack = Packing::new(4096, &eval.hint().unwrap()).unwrap();
        let manifest = Manifest {
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
        let client =
            enhance_pir::v4_client::QuerySession::new(&manifest, pack.session(1, 0)).unwrap();
        for position in [0, 32, 33, 66] {
            let (query, slot) = client.prepare_position(position).unwrap();
            let binding = QueryBinding::decode(query.body()).unwrap();
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
        let mut restarted = Engine::new(dir.path());
        restarted
            .prepare(plan, |_, _| Err("canonical history unavailable".into()))
            .unwrap();
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
            ipir_sp::SimplePirProfile::P16Q49,
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
}
