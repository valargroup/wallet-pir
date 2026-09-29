//! Native packing behind the existing coordinator/router publication protocol.
use crate::packing_budget::{Charge, PackingBudget};
use base64::{engine::general_purpose::STANDARD, Engine};
use enhance_pir::{native as n, protocol::*};
use ipir_sp::{server::CrsBlock, YpirSchemeParams};
use reinspiring::native::NativePreprocessed;
use sha2::{Digest, Sha256};

pub struct NativePacking {
    _charge: Charge,
    pub params: YpirSchemeParams,
    pub public: Vec<u8>,
    pre: Vec<NativePreprocessed>,
}
impl NativePacking {
    pub fn new(rows: u64, hint: &[CrsBlock], budget: &PackingBudget) -> Result<Self, String> {
        let began = std::time::Instant::now();
        let mut charge = Charge::prepare(budget)?;
        let params = parameters(rows)?;
        if hint.len() != n::COLS / n::D {
            return Err("native hint block count".into());
        }
        let setup = n::packing_setup();
        let pre = hint
            .iter()
            .map(|b| NativePreprocessed::build_two_mask(&setup, &b.rows))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())?;
        let public = n::publish(&pre)?;
        charge.resident();
        crate::prepared_packing::observe_preparation(began.elapsed());
        Ok(Self {
            _charge: charge,
            params,
            public,
            pre,
        })
    }
    pub(crate) fn write_prepared(&self, out: &mut impl std::io::Write) -> Result<(), String> {
        out.write_all(&(self.params.db_rows as u64).to_le_bytes())
            .map_err(|e| e.to_string())?;
        out.write_all(&self.public).map_err(|e| e.to_string())?;
        reinspiring::prepared_native::write(out, &self.pre).map_err(|e| e.to_string())
    }
    pub(crate) fn map_prepared(
        file: &std::fs::File,
        rows: u64,
        budget: &PackingBudget,
    ) -> Result<Self, String> {
        let mut charge = Charge::mapping(budget)?;
        let params = parameters(rows)?;
        let prefix = 8 + n::public_len(n::COLS);
        // SAFETY: the authenticated private cache inode is immutable; writers
        // replace paths atomically, and GC only unlinks retired files.
        let map = unsafe { memmap2::MmapOptions::new().map(file) }.map_err(|e| e.to_string())?;
        if map.len() < prefix || u64::from_le_bytes(map[..8].try_into().unwrap()) != rows {
            return Err("native prepared row header".into());
        }
        let public = map[8..prefix].to_vec();
        let pre =
            reinspiring::prepared_native::read(map, prefix, &n::packing_setup(), n::COLS / n::D)
                .map_err(|e| e.to_string())?;
        if public != n::publish(&pre)? {
            return Err("native public mask binding".into());
        }
        charge.resident();
        Ok(Self {
            _charge: charge,
            params,
            public,
            pre,
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
                    .expect("validated manifest session"),
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
            return Err("native query session mismatch".into());
        }
        n::parse(&body[HEADER_BYTES..], self.params.db_rows).map(|(_, q)| q)
    }
    pub fn pack(&self, body: &[u8], intermediate: &[u64]) -> Result<Vec<u8>, String> {
        let binding = QueryBinding::decode(body)?;
        self.query_coefficients(body, binding)?;
        if intermediate.len() != n::COLS || intermediate.iter().any(|&x| x >= n::Q) {
            return Err("native intermediate shape".into());
        }
        let (keys, _) = n::parse(&body[HEADER_BYTES..], self.params.db_rows)?;
        let mut out = binding.encode();
        out.extend(n::pack(&self.pre, &keys, intermediate)?);
        Ok(out)
    }
}

/// Build public unit hint contributions using exact lifted native-ring products.
pub fn hint(
    server: &ipir_sp::IPIRServer<u16>,
    setup: &[Vec<u64>],
) -> Result<Vec<CrsBlock>, String> {
    let rows = server.params().db_rows;
    let cols = server.params().db_cols;
    let db = server.db();
    Ok(
        pir_native::hint(setup, rows, cols, |col| &db[col * rows..(col + 1) * rows])?
            .into_iter()
            .map(|rows| CrsBlock { rows })
            .collect(),
    )
}
