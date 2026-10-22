use crate::artifact::{
    release_file_cache, write_atomic, write_atomic_cold, PublicationArtifact, VerifiedReader,
    IO_BUFFER_BYTES,
};
use crate::matvec::MatvecConfig;
use crate::types::{DatabaseId, DatabaseLayout};
use crate::wire::{crs_encoded_len, write_crs_blocks};
use inspiring::{InspiringError, RlweParams};
use ipir_sp::server::CrsBlock;
use ipir_sp::{IPIRSimpleQuery, YpirSchemeParams};
use pir_runtime::evaluation::PreparedDatabase;
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::{self, BufReader, Read};
use std::path::{Path, PathBuf};

// Shard composition helpers retained from ipir-sp e875404. The upstream
// main API exposes the query codec and CRS blocks; aggregation lives here.
/// Decode the global first-dimension query before slicing it across row shards.
pub fn deserialize_first_dim_query(
    rlwe: &RlweParams,
    ypir: &YpirSchemeParams,
    query: &[u8],
) -> Result<Vec<u64>, InspiringError> {
    let first_dim_query =
        IPIRSimpleQuery::from_switched_bytes(query, ypir.db_rows, rlwe.q, ypir.query_bits)?
            .into_first_dim();

    if first_dim_query.len() != ypir.db_rows {
        return Err(InspiringError::LweShape(format!(
            "expected {} first-dimension query values, got {}",
            ypir.db_rows,
            first_dim_query.len()
        )));
    }

    Ok(first_dim_query)
}

/// Add a worker's SimplePIR intermediate into a coordinator accumulator.
///
/// The first-dimension operation is linear over `Z_q`: if row shards partition
/// a database, summing their fixed-width outputs produces the exact monolithic
/// matrix-vector product. Inputs are required to be canonical residues so a
/// corrupt worker cannot smuggle an overflow or a second representation of the
/// same value across the protocol boundary.
pub use pir_runtime::evaluation::add_intermediate_assign_mod;

/// Add one shard's partial CRS/hint contribution into the global CRS.
///
/// Each contribution must have the complete output-block shape. This function
/// is deliberately shape-strict because the resulting public `c1` is bound to
/// the snapshot and a missing coefficient would silently make clients decode
/// garbage.
pub fn add_crs_blocks_assign_mod(
    accumulator: &mut [CrsBlock],
    contribution: &[CrsBlock],
    params: &RlweParams,
) -> Result<(), InspiringError> {
    if accumulator.len() != contribution.len() {
        return Err(InspiringError::PreprocessMismatch(format!(
            "CRS block counts differ: {} and {}",
            accumulator.len(),
            contribution.len()
        )));
    }

    for (left_block, right_block) in accumulator.iter_mut().zip(contribution) {
        for block in [&*left_block, right_block] {
            if block.rows.len() != params.d || block.rows.iter().any(|row| row.len() != params.d) {
                return Err(InspiringError::PreprocessMismatch(format!(
                    "CRS block must be {}x{}",
                    params.d, params.d
                )));
            }
        }
        for (left_row, right_row) in left_block.rows.iter_mut().zip(&right_block.rows) {
            #[cfg(feature = "native-reinspiring")]
            let modulus = crate::native::Q;
            #[cfg(not(feature = "native-reinspiring"))]
            let modulus = params.q;
            add_intermediate_assign_mod(left_row, right_row, modulus)?;
        }
    }

    Ok(())
}

// Version 2 bound cached shard preprocessing to the domain-separated enhance setup seed.
// Version 3 used the former wider action record. Version 4 namespaced table
// artifacts. Version 5 is the 724-byte Enhance record. Version 6 adds the
// authenticated transaction-shape byte and invalidates the old preprocessing.
// Version 7 is the twenty-nine-record row: db_cols goes from 4,096 to 12,288,
// so every persisted database and partial CRS from version 6 is the wrong shape.
// Version 8 binds the plaintext profile and changes Enhance to p=2^16 with a
// 46-bit minimum query width.
// Version 9 invalidates full-ciphertext artifacts for schema 11 suffix records.
// Version 10 tags artifact checksums independently of the protocol row binding.
const ARTIFACT_VERSION: u16 = 10;

#[derive(Serialize, Deserialize)]
struct ArtifactMetadata {
    version: u16,
    table: String,
    pir_profile: String,
    rlwe_degree: usize,
    rlwe_modulus: u64,
    db_rows: usize,
    db_cols: usize,
    plaintext_modulus: u64,
    shard_id: u64,
    query_row_start: usize,
    rows_sha256: String,
    #[serde(with = "pir_artifact::current")]
    database_checksum: String,
    #[serde(with = "pir_artifact::current")]
    crs_checksum: String,
}

impl ArtifactMetadata {
    fn matches_identity(
        &self,
        table: DatabaseId,
        layout: &DatabaseLayout,
        shard_id: u64,
        query_row_start: usize,
        rows_sha256: &str,
        rlwe: &RlweParams,
    ) -> bool {
        self.version == ARTIFACT_VERSION
            && self.table == table.as_str()
            && self.pir_profile == layout.pir_profile.id()
            && self.rlwe_degree == rlwe.d
            && self.rlwe_modulus == crate::runtime::modulus()
            && self.shard_id == shard_id
            && self.query_row_start == query_row_start
            && self.rows_sha256 == rows_sha256
    }
}

pub struct ShardRuntime {
    pub shard_id: u64,
    pub query_row_start: usize,
    pub rows_sha256: String,
    pub server: PreparedDatabase,
}

/// Offline preparation owns CRS only until it has been durably persisted.
///
/// A prepared shard is a candidate: building it checks only the shard's
/// structure (layout, row width). [`PreparedShard::persist`] turns it into a
/// [`CachedShard`] that a worker may serve; fields stay private so the CRS and
/// runtime cannot be swapped between those steps.
pub struct PreparedShard {
    runtime: ShardRuntime,
    crs_blocks: Vec<CrsBlock>,
}

/// Retained worker state: query database plus a small pinned publication handle.
///
/// Built only by persisting or loading a verified artifact; readers borrow its
/// parts immutably.
pub struct CachedShard {
    runtime: ShardRuntime,
    publication: PublicationArtifact,
}

impl CachedShard {
    pub fn runtime(&self) -> &ShardRuntime {
        &self.runtime
    }

    pub fn publication(&self) -> &PublicationArtifact {
        &self.publication
    }
}

/// Directory holding one shard's artifacts, namespaced by table so several
/// tables can share a worker's artifact root.
pub fn shard_artifact_dir(artifact_root: &Path, table: DatabaseId, shard_id: u64) -> PathBuf {
    artifact_root
        .join(table.as_str())
        .join(format!("shard-{shard_id:08}"))
}

impl ShardRuntime {
    /// Loads a persisted shard for serving.
    ///
    /// Checks the artifact's identity (table, layout, shard, rows digest and
    /// parameters) and sizes, and verifies the database and CRS files against
    /// the SHA-256 digests recorded at persist time. It does not decide whether
    /// the shard belongs to a current publication; the publication owner does.
    #[allow(clippy::too_many_arguments)]
    pub fn load_cached_with_backend(
        artifact_root: &Path,
        table: DatabaseId,
        layout: &DatabaseLayout,
        shard_id: u64,
        query_row_start: usize,
        rows_sha256: &str,
        rlwe: &RlweParams,
        backend: MatvecConfig,
    ) -> Result<CachedShard, LoadError> {
        Self::load_with_backend(
            &shard_artifact_dir(artifact_root, table, shard_id),
            table,
            layout,
            shard_id,
            query_row_start,
            rows_sha256,
            rlwe,
            backend,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn load_or_build(
        artifact_root: &Path,
        table: DatabaseId,
        layout: &DatabaseLayout,
        shard_id: u64,
        query_row_start: usize,
        rows_sha256: String,
        rows: &[u8],
        rlwe: &RlweParams,
        global_setup: &[Vec<u64>],
    ) -> Result<(CachedShard, bool), String> {
        let directory = shard_artifact_dir(artifact_root, table, shard_id);
        if let Ok(runtime) = Self::load(
            &directory,
            table,
            layout,
            shard_id,
            query_row_start,
            &rows_sha256,
            rlwe,
        ) {
            return Ok((runtime, false));
        }
        let prepared = PreparedShard::build(
            layout,
            shard_id,
            query_row_start,
            rows_sha256,
            rows,
            rlwe,
            global_setup,
        )
        .map_err(|error| error.to_string())?;
        let runtime = prepared
            .persist(&directory, table, layout, rlwe)
            .map_err(|error| error.to_string())?;
        Ok((runtime, true))
    }

    pub fn evaluate(&self, rlwe: &RlweParams, query: &[u64]) -> Result<Vec<u64>, InspiringError> {
        #[cfg(feature = "native-reinspiring")]
        let _ = rlwe;
        let shard_rows = self.server.params().db_rows;
        if query.len() != shard_rows {
            return Err(InspiringError::LweShape(format!(
                "shard query must contain {shard_rows} coefficients, got {}",
                query.len()
            )));
        }
        if query
            .iter()
            .any(|coefficient| *coefficient >= crate::runtime::modulus())
        {
            return Err(InspiringError::PreprocessMismatch(
                "query coefficient is not reduced modulo q".to_string(),
            ));
        }
        #[cfg(feature = "native-reinspiring")]
        let result = self
            .server
            .try_multiply_power_of_two(crate::native::Q, query);
        #[cfg(not(feature = "native-reinspiring"))]
        let result = self.server.try_multiply_query(rlwe, query);
        result.map_err(|error| {
            tracing::error!(%error, "matrix-vector evaluation failed");
            InspiringError::Internal("matrix-vector evaluation failed")
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn load(
        directory: &Path,
        table: DatabaseId,
        layout: &DatabaseLayout,
        shard_id: u64,
        query_row_start: usize,
        rows_sha256: &str,
        rlwe: &RlweParams,
    ) -> Result<CachedShard, String> {
        Self::load_with_backend(
            directory,
            table,
            layout,
            shard_id,
            query_row_start,
            rows_sha256,
            rlwe,
            MatvecConfig::default(),
        )
        .map_err(|e| e.to_string())
    }

    #[allow(clippy::too_many_arguments)]
    fn load_with_backend(
        directory: &Path,
        table: DatabaseId,
        layout: &DatabaseLayout,
        shard_id: u64,
        query_row_start: usize,
        rows_sha256: &str,
        rlwe: &RlweParams,
        backend: MatvecConfig,
    ) -> Result<CachedShard, LoadError> {
        let metadata: ArtifactMetadata = serde_json::from_reader(BufReader::new(
            File::open(directory.join("metadata.json")).map_err(|e| e.to_string())?,
        ))
        .map_err(|e| e.to_string())?;
        if !metadata.matches_identity(table, layout, shard_id, query_row_start, rows_sha256, rlwe) {
            return Err("artifact metadata mismatch".into());
        }
        let (_, local_params) = shard_parameters(layout).map_err(|e| e.to_string())?;
        if metadata.db_rows != local_params.db_rows
            || metadata.db_cols != local_params.db_cols
            || metadata.plaintext_modulus != local_params.p
        {
            return Err("persisted artifact parameter mismatch".into());
        }
        let coefficients = local_params
            .db_rows
            .checked_mul(local_params.db_cols)
            .ok_or("persisted database size overflow")?;
        let expected_db_bytes = coefficients
            .checked_mul(2)
            .ok_or("persisted database size overflow")? as u64;
        let file = File::open(directory.join("database.u16le")).map_err(|e| e.to_string())?;
        if file.metadata().map_err(|e| e.to_string())?.len() != expected_db_bytes {
            return Err("persisted database has the wrong size".into());
        }
        let cache_handle = file.try_clone().map_err(|e| e.to_string())?;
        let reader = BufReader::with_capacity(
            IO_BUFFER_BYTES,
            VerifiedReader::new(file, expected_db_bytes, metadata.database_checksum),
        );
        let server = read_database_with_backend(reader, local_params.clone(), backend)?;
        release_file_cache(&cache_handle);
        let blocks = local_params.db_cols / rlwe.d;
        let publication = PublicationArtifact::open(
            &directory.join("partial-crs.bin"),
            crs_encoded_len(blocks, rlwe.d).map_err(|e| e.to_string())?,
            metadata.crs_checksum,
        )
        .map_err(|e| e.to_string())?;
        publication
            .validate(blocks, rlwe.d)
            .map_err(|e| e.to_string())?;
        Ok(CachedShard {
            runtime: Self {
                shard_id,
                query_row_start,
                rows_sha256: rows_sha256.to_string(),
                server,
            },
            publication,
        })
    }
}

impl PreparedShard {
    pub fn runtime(&self) -> &ShardRuntime {
        &self.runtime
    }

    pub fn crs_blocks(&self) -> &[CrsBlock] {
        &self.crs_blocks
    }

    pub fn build(
        layout: &DatabaseLayout,
        shard_id: u64,
        query_row_start: usize,
        rows_sha256: String,
        rows: &[u8],
        rlwe: &RlweParams,
        global_setup: &[Vec<u64>],
    ) -> Result<Self, InspiringError> {
        Self::build_with_backend(
            layout,
            shard_id,
            query_row_start,
            rows_sha256,
            rows,
            rlwe,
            global_setup,
            MatvecConfig::default(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn build_with_backend(
        layout: &DatabaseLayout,
        shard_id: u64,
        query_row_start: usize,
        rows_sha256: String,
        rows: &[u8],
        rlwe: &RlweParams,
        global_setup: &[Vec<u64>],
        backend: MatvecConfig,
    ) -> Result<Self, InspiringError> {
        if rows.len() != layout.shard_bytes() {
            return Err(InspiringError::PreprocessMismatch(format!(
                "shard must be {} bytes, got {}",
                layout.shard_bytes(),
                rows.len()
            )));
        }
        if !query_row_start.is_multiple_of(rlwe.d) {
            return Err(InspiringError::PreprocessMismatch(
                "shard query row is not polynomial aligned".to_string(),
            ));
        }

        let (_, local_params) = shard_parameters(layout)?;
        let coefficients = RowPlaintextIter::new(
            rows,
            layout.row_bytes(),
            local_params.db_rows,
            local_params.db_cols,
            local_params.p.trailing_zeros() as usize,
        );
        let server = backend
            .server(local_params, coefficients, false)
            .map_err(|error| {
                tracing::error!(%error, "matrix-vector preparation failed");
                InspiringError::Internal("matrix-vector preparation failed")
            })
            .map(PreparedDatabase::new)?;
        let first_poly = query_row_start / rlwe.d;
        let poly_count = layout.shard_rows / rlwe.d;
        let setup = global_setup
            .get(first_poly..first_poly + poly_count)
            .ok_or_else(|| {
                InspiringError::PreprocessMismatch("global setup does not cover shard".to_string())
            })?;
        #[cfg(feature = "native-reinspiring")]
        let crs_blocks =
            pir_serving::hint(&server, setup).map_err(InspiringError::PreprocessMismatch)?;
        #[cfg(not(feature = "native-reinspiring"))]
        let crs_blocks = server.hint(rlwe, setup);

        Ok(Self {
            runtime: ShardRuntime {
                shard_id,
                query_row_start,
                rows_sha256,
                server,
            },
            crs_blocks,
        })
    }

    pub fn persist(
        self,
        directory: &Path,
        table: DatabaseId,
        layout: &DatabaseLayout,
        rlwe: &RlweParams,
    ) -> io::Result<CachedShard> {
        fs::create_dir_all(directory)?;
        let runtime = self.runtime;
        let database_checksum = write_atomic_cold(directory, "database.u16le", |writer| {
            for coefficient in runtime.server.db() {
                writer.write_all(&coefficient.to_le_bytes())?;
            }
            Ok(())
        })?;
        let crs_checksum = write_atomic_cold(directory, "partial-crs.bin", |writer| {
            write_crs_blocks(writer, &self.crs_blocks)
        })?;
        let publication = PublicationArtifact::open(
            &directory.join("partial-crs.bin"),
            crs_encoded_len(runtime.server.params().db_cols / rlwe.d, rlwe.d)?,
            crs_checksum.clone(),
        )?;
        drop(self.crs_blocks);
        let metadata = ArtifactMetadata {
            version: ARTIFACT_VERSION,
            table: table.as_str().to_string(),
            pir_profile: layout.pir_profile.id().to_string(),
            rlwe_degree: rlwe.d,
            rlwe_modulus: crate::runtime::modulus(),
            db_rows: runtime.server.params().db_rows,
            db_cols: runtime.server.params().db_cols,
            plaintext_modulus: runtime.server.params().p,
            shard_id: runtime.shard_id,
            query_row_start: runtime.query_row_start,
            rows_sha256: runtime.rows_sha256.clone(),
            database_checksum,
            crs_checksum,
        };
        write_atomic(directory, "metadata.json", |writer| {
            serde_json::to_writer_pretty(writer, &metadata).map_err(io::Error::other)
        })?;
        File::open(directory)?.sync_all()?;
        Ok(CachedShard {
            runtime,
            publication,
        })
    }
}

/// No partially read database is ever returned to a caller; see
/// [`pir_runtime::evaluation::build_from_le_u16`].
#[cfg(test)]
fn read_database(reader: impl Read, params: YpirSchemeParams) -> io::Result<PreparedDatabase> {
    read_database_with_backend(reader, params, MatvecConfig::default()).map_err(io::Error::other)
}

fn read_database_with_backend(
    mut reader: impl Read,
    params: YpirSchemeParams,
    backend: MatvecConfig,
) -> Result<PreparedDatabase, LoadError> {
    let count = params
        .db_rows
        .checked_mul(params.db_cols)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "database size overflow"))?;
    let server = pir_runtime::evaluation::build_from_le_u16(&mut reader, count, |coefficients| {
        backend
            .server(params, coefficients, true)
            .map(PreparedDatabase::new)
    })?;
    let server = server.map_err(LoadError::Backend)?;
    let mut trailing = [0];
    if reader.read(&mut trailing)? != 0 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "trailing database bytes").into());
    }
    Ok(server)
}

/// iPIR parameters for the global (coordinator-facing) database of a table
/// with `logical_rows` rows.
pub fn global_parameters(
    logical_rows: u64,
    layout: &DatabaseLayout,
) -> Result<(RlweParams, YpirSchemeParams), InspiringError> {
    ipir_sp::params_for_simplepir_profile(logical_rows, layout.item_size_bits(), layout.pir_profile)
}

/// iPIR parameters for one shard of a table. Row sharding is sound because the
/// column count derives from the row size, not the row count, so shard and
/// global intermediates have the same width.
pub fn shard_parameters(
    layout: &DatabaseLayout,
) -> Result<(RlweParams, YpirSchemeParams), InspiringError> {
    ipir_sp::params_for_simplepir_profile(
        layout.shard_rows as u64,
        layout.item_size_bits(),
        layout.pir_profile,
    )
}

pub struct RowPlaintextIter<'a> {
    data: &'a [u8],
    row_bytes: usize,
    db_cols: usize,
    plaintext_bits: usize,
    position: usize,
    total: usize,
}

impl<'a> RowPlaintextIter<'a> {
    pub fn new(
        data: &'a [u8],
        row_bytes: usize,
        db_rows: usize,
        db_cols: usize,
        plaintext_bits: usize,
    ) -> Self {
        Self {
            data,
            row_bytes,
            db_cols,
            plaintext_bits,
            position: 0,
            total: db_rows * db_cols,
        }
    }
}

impl Iterator for RowPlaintextIter<'_> {
    type Item = u16;

    fn next(&mut self) -> Option<Self::Item> {
        if self.position >= self.total {
            return None;
        }
        let row = self.position / self.db_cols;
        let column = self.position % self.db_cols;
        self.position += 1;
        let start = row * self.row_bytes;
        let end = start.saturating_add(self.row_bytes).min(self.data.len());
        let bytes = self.data.get(start..end).unwrap_or_default();
        Some(
            ipir_sp::bits::read_bits(bytes, column * self.plaintext_bits, self.plaintext_bits)
                as u16,
        )
    }
}

#[cfg(test)]
#[path = "ipir/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "ipir/persistence_tests.rs"]
mod persistence_tests;

/// Cache corruption may be rebuilt; a device failure must propagate unchanged.
#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    #[error("artifact: {0}")]
    Artifact(String),
    #[error("matrix-vector backend: {0}")]
    Backend(#[from] ipir_sp::server::KernelError),
}
impl From<String> for LoadError {
    fn from(e: String) -> Self {
        Self::Artifact(e)
    }
}
impl From<&str> for LoadError {
    fn from(e: &str) -> Self {
        Self::Artifact(e.into())
    }
}
impl From<io::Error> for LoadError {
    fn from(e: io::Error) -> Self {
        Self::Artifact(e.to_string())
    }
}
