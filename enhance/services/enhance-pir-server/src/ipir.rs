use crate::artifact::{write_atomic, PublicationArtifact, VerifiedReader, IO_BUFFER_BYTES};
use crate::types::{DatabaseId, DatabaseLayout};
use crate::wire::{crs_encoded_len, write_crs_blocks};
use inspiring::{InspiringError, RlweParams};
use ipir_sp::server::{CrsBlock, IPIRServer};
use ipir_sp::{IPIRSimpleQuery, YpirSchemeParams};
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
pub fn add_intermediate_assign_mod(
    accumulator: &mut [u64],
    contribution: &[u64],
    modulus: u64,
) -> Result<(), InspiringError> {
    if modulus < 2 {
        return Err(InspiringError::PreprocessMismatch(
            "intermediate modulus must be at least two".to_string(),
        ));
    }
    if accumulator.len() != contribution.len() {
        return Err(InspiringError::LweShape(format!(
            "intermediate widths differ: {} and {}",
            accumulator.len(),
            contribution.len()
        )));
    }

    for (left, right) in accumulator.iter_mut().zip(contribution) {
        if *left >= modulus || *right >= modulus {
            return Err(InspiringError::PreprocessMismatch(
                "intermediate coefficient is not reduced modulo q".to_string(),
            ));
        }
        let sum = u128::from(*left) + u128::from(*right);
        *left = (sum % u128::from(modulus)) as u64;
    }

    Ok(())
}

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
            add_intermediate_assign_mod(left_row, right_row, params.q)?;
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
const ARTIFACT_VERSION: u16 = 8;

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
    database_sha256: String,
    crs_sha256: String,
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
            && self.rlwe_modulus == rlwe.q
            && self.shard_id == shard_id
            && self.query_row_start == query_row_start
            && self.rows_sha256 == rows_sha256
    }
}

pub struct ShardRuntime {
    pub shard_id: u64,
    pub query_row_start: usize,
    pub rows_sha256: String,
    pub server: IPIRServer<u16>,
}

/// Offline preparation owns CRS only until it has been durably persisted.
pub struct PreparedShard {
    pub runtime: ShardRuntime,
    pub crs_blocks: Vec<CrsBlock>,
}

/// Retained worker state: query database plus a small pinned publication handle.
pub struct CachedShard {
    pub runtime: ShardRuntime,
    pub publication: PublicationArtifact,
    /// Monotonic load order, so a worker can pick the newest runtime of a shard.
    pub prepared_at: u64,
}

fn next_prepared_at() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// Directory holding one shard's artifacts, namespaced by table so several
/// tables can share a worker's artifact root.
pub fn shard_artifact_dir(artifact_root: &Path, table: DatabaseId, shard_id: u64) -> PathBuf {
    artifact_root
        .join(table.as_str())
        .join(format!("shard-{shard_id:08}"))
}

impl ShardRuntime {
    #[allow(clippy::too_many_arguments)]
    pub fn load_cached(
        artifact_root: &Path,
        table: DatabaseId,
        layout: &DatabaseLayout,
        shard_id: u64,
        query_row_start: usize,
        rows_sha256: &str,
        rlwe: &RlweParams,
    ) -> Result<CachedShard, String> {
        Self::load(
            &shard_artifact_dir(artifact_root, table, shard_id),
            table,
            layout,
            shard_id,
            query_row_start,
            rows_sha256,
            rlwe,
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
        let shard_rows = self.server.params().db_rows;
        if query.len() != shard_rows {
            return Err(InspiringError::LweShape(format!(
                "shard query must contain {shard_rows} coefficients, got {}",
                query.len()
            )));
        }
        if query.iter().any(|coefficient| *coefficient >= rlwe.q) {
            return Err(InspiringError::PreprocessMismatch(
                "query coefficient is not reduced modulo q".to_string(),
            ));
        }
        Ok(self.server.multiply_query(rlwe, query))
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
        let metadata: ArtifactMetadata = serde_json::from_reader(BufReader::new(
            File::open(directory.join("metadata.json")).map_err(|e| e.to_string())?,
        ))
        .map_err(|e| e.to_string())?;
        if !metadata.matches_identity(table, layout, shard_id, query_row_start, rows_sha256, rlwe) {
            return Err("artifact metadata mismatch".to_string());
        }
        let (_, local_params) = shard_parameters(layout).map_err(|e| e.to_string())?;
        if metadata.db_rows != local_params.db_rows
            || metadata.db_cols != local_params.db_cols
            || metadata.plaintext_modulus != local_params.p
        {
            return Err("persisted artifact parameter mismatch".to_string());
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
        let reader = BufReader::with_capacity(
            IO_BUFFER_BYTES,
            VerifiedReader::new(file, expected_db_bytes, metadata.database_sha256),
        );
        let server = read_database(reader, local_params.clone()).map_err(|e| e.to_string())?;
        let blocks = local_params.db_cols / rlwe.d;
        let publication = PublicationArtifact::open(
            &directory.join("partial-crs.bin"),
            crs_encoded_len(blocks, rlwe.d).map_err(|e| e.to_string())?,
            metadata.crs_sha256,
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
            prepared_at: next_prepared_at(),
        })
    }
}

impl PreparedShard {
    pub fn build(
        layout: &DatabaseLayout,
        shard_id: u64,
        query_row_start: usize,
        rows_sha256: String,
        rows: &[u8],
        rlwe: &RlweParams,
        global_setup: &[Vec<u64>],
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
        let server = IPIRServer::<u16>::new_auto_kernel(local_params, coefficients, false, true);
        let first_poly = query_row_start / rlwe.d;
        let poly_count = layout.shard_rows / rlwe.d;
        let setup = global_setup
            .get(first_poly..first_poly + poly_count)
            .ok_or_else(|| {
                InspiringError::PreprocessMismatch("global setup does not cover shard".to_string())
            })?;
        let crs_blocks = server
            .perform_offline_precomputation_simplepir(rlwe, setup)
            .crs_blocks;

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
        let database_sha256 = write_atomic(directory, "database.u16le", |writer| {
            for coefficient in runtime.server.db() {
                writer.write_all(&coefficient.to_le_bytes())?;
            }
            Ok(())
        })?;
        let crs_sha256 = write_atomic(directory, "partial-crs.bin", |writer| {
            write_crs_blocks(writer, &self.crs_blocks)
        })?;
        let publication = PublicationArtifact::open(
            &directory.join("partial-crs.bin"),
            crs_encoded_len(runtime.server.params().db_cols / rlwe.d, rlwe.d)?,
            crs_sha256.clone(),
        )?;
        drop(self.crs_blocks);
        let metadata = ArtifactMetadata {
            version: ARTIFACT_VERSION,
            table: table.as_str().to_string(),
            pir_profile: layout.pir_profile.id().to_string(),
            rlwe_degree: rlwe.d,
            rlwe_modulus: rlwe.q,
            db_rows: runtime.server.params().db_rows,
            db_cols: runtime.server.params().db_cols,
            plaintext_modulus: runtime.server.params().p,
            shard_id: runtime.shard_id,
            query_row_start: runtime.query_row_start,
            rows_sha256: runtime.rows_sha256.clone(),
            database_sha256,
            crs_sha256,
        };
        write_atomic(directory, "metadata.json", |writer| {
            serde_json::to_writer_pretty(writer, &metadata).map_err(io::Error::other)
        })?;
        File::open(directory)?.sync_all()?;
        Ok(CachedShard {
            runtime,
            publication,
            prepared_at: next_prepared_at(),
        })
    }
}

/// The upstream constructor requires an infallible iterator of exactly the
/// declared size. On read failure, finish with placeholders, then discard the
/// server. No partially read database is ever returned to a caller.
fn read_database(mut reader: impl Read, params: YpirSchemeParams) -> io::Result<IPIRServer<u16>> {
    let count = params
        .db_rows
        .checked_mul(params.db_cols)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "database size overflow"))?;
    let mut failure = None;
    let coefficients = (0..count).map(|_| {
        if failure.is_some() {
            return 0;
        }
        let mut bytes = [0; 2];
        match reader.read_exact(&mut bytes) {
            Ok(()) => u16::from_le_bytes(bytes),
            Err(e) => {
                failure = Some(e);
                0
            }
        }
    });
    let server = IPIRServer::<u16>::new_auto_kernel(params, coefficients, true, true);
    if let Some(e) = failure {
        return Err(e);
    }
    let mut trailing = [0];
    if reader.read(&mut trailing)? != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "trailing database bytes",
        ));
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
mod tests {
    use super::*;

    #[test]
    fn shard_sum_wraps_without_u64_overflow() {
        let q = u64::MAX;
        let mut sum = [q - 1, 0, 1];
        add_intermediate_assign_mod(&mut sum, &[q - 1, q - 1, q - 1], q).unwrap();
        assert_eq!(sum, [q - 2, q - 1, 0]);
    }

    #[test]
    fn shard_combiners_reject_malformed_contributions() {
        let (rlwe, _) = shard_parameters(&crate::types::ENHANCE_LAYOUT).unwrap();
        assert!(add_intermediate_assign_mod(&mut [0; 8], &[0; 7], rlwe.q).is_err());
        assert!(add_intermediate_assign_mod(&mut [0], &[rlwe.q], rlwe.q).is_err());
        assert!(add_intermediate_assign_mod(&mut [rlwe.q], &[0], rlwe.q).is_err());
        assert!(add_intermediate_assign_mod(&mut [0], &[0], 1).is_err());
        let mut malformed = [CrsBlock { rows: vec![] }];
        assert!(add_crs_blocks_assign_mod(&mut malformed, &[], &rlwe).is_err());
        assert!(
            add_crs_blocks_assign_mod(&mut malformed, &[CrsBlock { rows: vec![] }], &rlwe).is_err()
        );
        let malformed_row = CrsBlock {
            rows: vec![vec![]; rlwe.d],
        };
        assert!(
            add_crs_blocks_assign_mod(&mut [malformed_row.clone()], &[malformed_row], &rlwe)
                .is_err()
        );
    }

    /// Every served layout, with the instance count its rows need. One instance
    /// carries d * log2(p) plaintext bits.
    const LAYOUTS: &[(&str, DatabaseLayout, usize)] =
        &[("enhance", crate::types::ENHANCE_LAYOUT, 6)];

    #[test]
    fn every_layout_has_the_expected_instance_count() {
        for (name, layout, instances) in LAYOUTS {
            let (rlwe, shard) = shard_parameters(layout).expect("shard params");
            let (global_rlwe, global) = global_parameters(
                layout.logical_rows_for(layout.shard_rows as u64 * 4),
                layout,
            )
            .expect("global params");
            assert_eq!(rlwe.d, 2_048, "{name}");
            assert_eq!(shard.p, 1 << 16, "{name}");
            assert_eq!(shard.query_bits, 46, "{name}");
            assert_eq!(shard.instances, *instances, "{name}");
            assert_eq!(shard.db_cols, instances * rlwe.d, "{name}");
            // Shard and global parameters must agree on everything but row count,
            // or partials from shards could not be summed into the global answer.
            assert_eq!((global_rlwe.d, global_rlwe.q), (rlwe.d, rlwe.q), "{name}");
            assert_eq!(global.db_cols, shard.db_cols, "{name}");
            assert!(layout.shard_rows.is_multiple_of(rlwe.d), "{name}");
        }
    }

    #[test]
    fn enhance_rows_use_six_ipir_instances() {
        // A 24,321-byte row is 194,568 bits and fits six 32,768-bit instances
        // with 2,040 bits to spare. Thirty-four records need 200,464 bits and
        // would spill into a seventh instance.
        assert_eq!(crate::types::ENHANCE_LAYOUT.row_bytes(), 24_321);
        let (_, params) = shard_parameters(&crate::types::ENHANCE_LAYOUT).expect("params");
        assert_eq!(params.instances, 6);
        assert_eq!(params.db_cols, 12_288);
        assert_eq!(params.p, 1 << 16);
        assert_eq!(params.query_bits, 46);
    }

    #[test]
    fn sixteen_bit_row_encoding_preserves_the_odd_tail_byte_and_zero_padding() {
        let layout = crate::types::ENHANCE_LAYOUT;
        let (_, params) = shard_parameters(&layout).expect("params");
        let mut row = vec![0_u8; layout.row_bytes()];
        row[0] = 0x34;
        row[1] = 0x12;
        *row.last_mut().expect("nonempty row") = 0xab;

        let mut coefficients = RowPlaintextIter::new(
            &row,
            layout.row_bytes(),
            2,
            params.db_cols,
            layout.pir_profile.plaintext_bits(),
        );
        assert_eq!(coefficients.next(), Some(0x1234));
        assert_eq!(coefficients.nth(12_159), Some(0x00ab));
        assert_eq!(coefficients.next(), Some(0));
        assert_eq!(
            coefficients.nth(126),
            Some(0),
            "the padded second row is zero"
        );
    }
}

#[cfg(test)]
mod persistence_tests {
    use super::*;
    use crate::wire::{encode_crs_blocks, read_crs_blocks};
    use sha2::{Digest, Sha256};
    use std::io::{Cursor, Write};

    fn layout() -> DatabaseLayout {
        DatabaseLayout {
            record_bytes: 3584,
            records_per_row: 1,
            shard_rows: 2048,
            pir_profile: ipir_sp::SimplePirProfile::P14,
        }
    }

    #[test]
    fn streamed_database_rejects_short_corrupt_and_failed_reads_without_panicking() {
        let (_, params) = shard_parameters(&layout()).unwrap();
        let count = params.db_rows * params.db_cols;
        let bytes = vec![0u8; count * 2];
        let digest = hex::encode(Sha256::digest(&bytes));
        let input = BufReader::with_capacity(
            IO_BUFFER_BYTES,
            VerifiedReader::new(Cursor::new(&bytes), bytes.len() as u64, digest.clone()),
        );
        assert!(read_database(input, params.clone()).is_ok());
        for data in [&bytes[..bytes.len() - 1], &bytes[..1]] {
            assert!(read_database(data, params.clone()).is_err());
        }
        let mut corrupt = bytes;
        corrupt[0] = 1;
        let input = BufReader::with_capacity(
            IO_BUFFER_BYTES,
            VerifiedReader::new(Cursor::new(&corrupt), corrupt.len() as u64, digest),
        );
        assert!(read_database(input, params.clone()).is_err());
        struct Failed;
        impl Read for Failed {
            fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
                Err(io::Error::other("injected read failure"))
            }
        }
        assert!(read_database(Failed, params).is_err());
    }

    #[test]
    fn legacy_v7_artifacts_are_rejected_and_streamed_persistence_preserves_queries_and_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let layout = layout();
        let (rlwe, params) = shard_parameters(&layout).unwrap();
        let setup = ipir_sp::IPIRClient::from_profile(
            params.num_items,
            params.item_size_bits,
            layout.pir_profile,
        )
        .unwrap()
        .generate_public_query_setup_simplepir_from_seed(DatabaseId::Enhance.setup_seed_bytes());
        let rows = vec![3; layout.shard_bytes()];
        let prepared =
            PreparedShard::build(&layout, 0, 0, "fixture".into(), &rows, &rlwe, setup.polys())
                .unwrap();
        let query = vec![1; params.db_rows];
        let expected_query = prepared.runtime.evaluate(&rlwe, &query).unwrap();
        // Independently reproduce the pre-refactor artifact writer: whole byte
        // arrays and hashes, including the exact metadata schema and version.
        let db: Vec<u8> = prepared
            .runtime
            .server
            .db()
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect();
        let hint = encode_crs_blocks(&prepared.crs_blocks);
        fs::write(dir.path().join("database.u16le"), &db).unwrap();
        fs::write(dir.path().join("partial-crs.bin"), &hint).unwrap();
        let metadata = serde_json::json!({
            "version": 7, "table": "enhance", "rlwe_degree": rlwe.d, "rlwe_modulus": rlwe.q,
            "db_rows": params.db_rows, "db_cols": params.db_cols, "plaintext_modulus": params.p,
            "shard_id": 0, "query_row_start": 0, "rows_sha256": "fixture",
            "database_sha256": hex::encode(Sha256::digest(&db)),
            "crs_sha256": hex::encode(Sha256::digest(&hint)),
        });
        fs::write(
            dir.path().join("metadata.json"),
            serde_json::to_vec_pretty(&metadata).unwrap(),
        )
        .unwrap();
        let load = || {
            ShardRuntime::load(
                dir.path(),
                DatabaseId::Enhance,
                &layout,
                0,
                0,
                "fixture",
                &rlwe,
            )
        };
        assert!(load().is_err(), "profile-less v7 artifacts must be rebuilt");
        let cached = prepared
            .persist(dir.path(), DatabaseId::Enhance, &layout, &rlwe)
            .unwrap();
        assert_eq!(fs::read(dir.path().join("database.u16le")).unwrap(), db);
        assert_eq!(fs::read(dir.path().join("partial-crs.bin")).unwrap(), hint);
        assert_eq!(
            cached.runtime.evaluate(&rlwe, &query).unwrap(),
            expected_query
        );
        assert_eq!(
            load().unwrap().runtime.evaluate(&rlwe, &query).unwrap(),
            expected_query
        );
        assert_eq!(
            read_crs_blocks(Cursor::new(&hint), 1, rlwe.d).unwrap()[0].rows,
            read_crs_blocks(cached.publication.reader(), 1, rlwe.d).unwrap()[0].rows
        );
        let file = File::options()
            .write(true)
            .open(dir.path().join("partial-crs.bin"))
            .unwrap();
        file.set_len(hint.len() as u64 - 1).unwrap();
        assert!(load().is_err());
        // Restore CRS and independently corrupt the database and metadata.
        fs::write(dir.path().join("partial-crs.bin"), &hint).unwrap();
        File::options()
            .write(true)
            .open(dir.path().join("database.u16le"))
            .unwrap()
            .write_all(&[0])
            .unwrap();
        assert!(load().is_err());
        fs::write(dir.path().join("database.u16le"), &db).unwrap();
        let mut bad_metadata: serde_json::Value =
            serde_json::from_slice(&fs::read(dir.path().join("metadata.json")).unwrap()).unwrap();
        bad_metadata["pir_profile"] = serde_json::json!("simplepir-p16-q46-v1");
        fs::write(
            dir.path().join("metadata.json"),
            serde_json::to_vec(&bad_metadata).unwrap(),
        )
        .unwrap();
        assert!(load().is_err());
    }
}
