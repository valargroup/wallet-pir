//! Status PIR material for one generation: database units, their hints and the
//! router packing material, prepared from an index snapshot.
use super::{index, profile};
use crate::protocol::*;
#[cfg(not(feature = "native-reinspiring"))]
use ipir_sp::IPIRClient;
use ipir_sp::{
    server::{CrsBlock, MatvecBackend},
    IPIRServer, ProductionSimplePirParams, SimplePirProfile,
};
use pir_runtime::evaluation::{build_from_le_u16, PreparedDatabase};
#[cfg(not(feature = "native-reinspiring"))]
use pir_runtime::packing::{PackingKeys, PackingSetup, PreparedPacking};
use rayon::prelude::*;
use sha2::{Digest, Sha256};
#[cfg(not(feature = "native-reinspiring"))]
use std::sync::OnceLock;
use std::{sync::Arc, time::Instant};

/// Ciphertext modulus of the selected Status profile.
fn modulus() -> u64 {
    static Q: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *Q.get_or_init(|| pir_serving::modulus(ROWS as u64, ITEM_BITS))
}

/// Router-side packing material for one d-column hint block.
#[cfg(feature = "native-reinspiring")]
type PackingBlock = Arc<reinspiring::native::NativePreprocessed>;

#[cfg(not(feature = "native-reinspiring"))]
fn packing_setup() -> PackingSetup<'static> {
    static TOP: OnceLock<PackingSetup<'static>> = OnceLock::new();
    TOP.get_or_init(|| PackingSetup::new(profile().rlwe()))
        .clone()
}

pub(super) struct Unit {
    bytes_digest: Hash,
    pub(super) rows: Vec<u8>,
    db: PreparedDatabase,
    hint: Vec<CrsBlock>,
}
/// Update H = A * D in Z_q[X]/(X^d+1) for at most 4096 changed
/// plaintext coefficients per unit. A coefficient delta at row r contributes
/// delta * X^r * A to its column; wrapping X^d negates the coefficient.
/// The public setup and profile are unchanged. Dense changes use upstream
/// reconstruction. The candidate owns its hint; admitted views are immutable.
/// The native profile uses the same exact negacyclic product modulo 2^54.
fn incremental_hint(old: &Unit, bytes: &[u8], setup: &[u64]) -> Option<Vec<CrsBlock>> {
    sparse_hint_update(
        &old.rows,
        &old.hint,
        bytes,
        setup,
        HintShape {
            degree: profile().rlwe().d,
            columns: ROW_BYTES / 2,
            modulus: modulus(),
        },
    )
}

/// Dimensions of the existing negacyclic hint calculation. Production always
/// uses the pinned profile; small rings let its arithmetic oracle stay cheap.
#[derive(Clone, Copy)]
struct HintShape {
    degree: usize,
    columns: usize,
    modulus: u64,
}

fn sparse_hint_update(
    old_rows: &[u8],
    old_hint: &[CrsBlock],
    bytes: &[u8],
    setup: &[u64],
    shape: HintShape,
) -> Option<Vec<CrsBlock>> {
    let HintShape {
        degree: d,
        columns,
        modulus: q,
    } = shape;
    let mut changes = Vec::new();
    if bytes.len() != old_rows.len() {
        return None;
    }
    for (i, (value, prior)) in bytes
        .chunks_exact(2)
        .zip(old_rows.chunks_exact(2))
        .enumerate()
    {
        let value = u16::from_le_bytes(value.try_into().unwrap());
        let row = i / columns;
        let col = i % columns;
        let prior = u16::from_le_bytes(prior.try_into().unwrap());
        if value != prior {
            if changes.len() == 4096 {
                return None;
            }
            changes.push((row, col, i64::from(value) - i64::from(prior)));
        }
    }
    let mut hint = old_hint.to_vec();
    for (row, col, delta) in changes {
        let out = &mut hint[col / d].rows[col % d];
        for (j, a) in setup.iter().enumerate() {
            let position = row + j;
            let subtract = (delta < 0) != (position >= d);
            let change =
                ((u128::from(*a) * u128::from(delta.unsigned_abs())) % u128::from(q)) as u64;
            let target = &mut out[position % d];
            *target = if subtract {
                if *target >= change {
                    *target - change
                } else {
                    q - (change - *target)
                }
            } else {
                let sum = *target + change;
                if sum >= q {
                    sum - q
                } else {
                    sum
                }
            };
        }
    }
    Some(hint)
}

/// One prepared Status generation: its manifest and public parameters, and on
/// workers the serving units and packing material.
///
/// A `Generation` is a candidate until a publication owner (the `Controller`
/// or a distributed role) activates it; from then on request handlers hold it
/// only through an immutable `Arc`. Its fields are private so the manifest
/// always describes the material beside it: content changes only by preparing
/// a new generation, and identity (generation number, recovery epoch,
/// observation time) changes only through [`Generation::reidentified`] and
/// [`Generation::reobserved`], which copy the material unchanged.
#[derive(Clone)]
pub struct Generation {
    manifest: Manifest,
    public: Arc<Vec<u8>>,
    pub(super) units: Vec<Arc<Unit>>,
    #[cfg(feature = "native-reinspiring")]
    pub(super) packing: Arc<Vec<PackingBlock>>,
    #[cfg(not(feature = "native-reinspiring"))]
    packing: Option<Arc<PreparedPacking<'static>>>,
    pub(super) packing_digests: Vec<Hash>,
}

#[derive(Debug, serde::Serialize)]
pub struct Preparation {
    pub unit_rows: usize,
    pub rebuilt_units: usize,
    pub reused_units: usize,
    pub incremental_units: usize,
    pub database_hint_ms: f64,
    pub packing_ms: f64,
    pub total_ms: f64,
}

impl Generation {
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// The serialized public parameters wallets download for this generation.
    pub fn public(&self) -> &Arc<Vec<u8>> {
        &self.public
    }

    /// The same prepared material under a new identity from the authority.
    ///
    /// Used when an observation matches the active content: the publication
    /// owner issues a new generation number and recovery epoch rather than
    /// preparing identical material again. Checks nothing; the caller obtains
    /// the identity from its authority and observation from a verified source.
    pub fn reidentified(&self, generation: u64, recovery_epoch: u64, observed_ms: u64) -> Self {
        let mut next = self.clone();
        next.manifest.generation = generation;
        next.manifest.recovery_epoch = recovery_epoch;
        next.manifest.observed_ms = observed_ms;
        next
    }

    /// The same generation, observed again at `observed_ms`.
    ///
    /// Only the freshness time changes; the identity and material stay. The
    /// caller must have re-verified that the source still serves this content.
    pub fn reobserved(&self, observed_ms: u64) -> Self {
        let mut next = self.clone();
        next.manifest.observed_ms = observed_ms;
        next
    }

    /// Adopts the manifest a distributed controller assigned to this material.
    ///
    /// Crate-private: only the distributed roles call it, after checking the
    /// assigned manifest against their own (same content; only the identity,
    /// observation time and, on workers, the router's public digest differ).
    pub(crate) fn assign_manifest(&mut self, manifest: Manifest) {
        self.manifest = manifest;
    }

    /// A coordinator's generation: the manifest and public parameters only, with
    /// no serving units; the workers hold the database.
    pub fn public_only(manifest: Manifest, public: Arc<Vec<u8>>) -> Self {
        Self {
            manifest,
            public,
            units: Vec::new(),
            #[cfg(feature = "native-reinspiring")]
            packing: Arc::new(Vec::new()),
            #[cfg(not(feature = "native-reinspiring"))]
            packing: None,
            packing_digests: Vec::new(),
        }
    }

    pub fn prepare(
        snapshot: &index::Snapshot,
        generation: u64,
        recovery_epoch: u64,
        observed_ms: u64,
        previous: Option<&Self>,
        backend: MatvecBackend,
    ) -> Result<(Self, Preparation), Error> {
        let (mut worker, mut stats) = Self::prepare_worker(
            snapshot,
            generation,
            recovery_epoch,
            observed_ms,
            previous,
            backend,
        )?;
        let started = Instant::now();
        let router = Self::prepare_router_with_previous(
            worker.manifest.clone(),
            &worker.hint_bytes()?,
            previous,
        )?;
        worker.manifest = router.manifest;
        worker.public = router.public;
        worker.packing = router.packing;
        worker.packing_digests = router.packing_digests;
        stats.packing_ms = started.elapsed().as_secs_f64() * 1000.;
        stats.total_ms += stats.packing_ms;
        Ok((worker, stats))
    }

    /// Fixed-shape, little-endian hint artifact. Never includes wallet queries.
    pub fn hint_bytes(&self) -> Result<Vec<u8>, Error> {
        let mut hint = self.units.first().ok_or(Error::Unavailable)?.hint.clone();
        let q = modulus();
        if q > u64::MAX / 2 {
            return Err(Error::Malformed);
        }
        hint.par_iter_mut()
            .enumerate()
            .try_for_each(|(i, block)| -> Result<(), Error> {
                for unit in &self.units[1..] {
                    for (out, input) in block.rows.iter_mut().zip(&unit.hint[i].rows) {
                        for (a, b) in out.iter_mut().zip(input) {
                            if *a >= q || *b >= q {
                                return Err(Error::Malformed);
                            }
                            let sum = *a + *b;
                            *a = if sum >= q { sum - q } else { sum };
                        }
                    }
                }
                Ok(())
            })?;
        let mut out = vec![0; Self::hint_len()];
        let block_bytes = profile().rlwe().d * profile().rlwe().d * 8;
        out.par_chunks_mut(block_bytes)
            .zip(hint.par_iter())
            .for_each(|(out, block)| {
                for (out, value) in out.chunks_exact_mut(8).zip(block.rows.iter().flatten()) {
                    out.copy_from_slice(&value.to_le_bytes());
                }
            });
        Ok(out)
    }

    pub fn hint_len() -> usize {
        let p = profile();
        p.ypir().db_cols.div_ceil(p.rlwe().d) * p.rlwe().d * p.rlwe().d * 8
    }

    /// Router material owns no database or GPU allocations.
    pub fn prepare_router(manifest: Manifest, bytes: &[u8]) -> Result<Self, Error> {
        Self::prepare_router_with_previous(manifest, bytes, None)
    }
    pub fn prepare_router_with_previous(
        mut manifest: Manifest,
        bytes: &[u8],
        previous: Option<&Self>,
    ) -> Result<Self, Error> {
        manifest.validate()?;
        let p = profile();
        let d = p.rlwe().d;
        if bytes.len() != Self::hint_len() {
            return Err(Error::Malformed);
        }
        let mut blocks = Vec::new();
        for block in bytes.chunks_exact(d * d * 8) {
            let mut rows = Vec::new();
            for row in block.chunks_exact(d * 8) {
                let values: Vec<_> = row
                    .chunks_exact(8)
                    .map(|v| u64::from_le_bytes(v.try_into().unwrap()))
                    .collect();
                if values.iter().any(|v| *v >= modulus()) {
                    return Err(Error::Malformed);
                }
                rows.push(values);
            }
            blocks.push(CrsBlock { rows });
        }
        let packing_digests: Vec<Hash> = bytes
            .par_chunks(d * d * 8)
            .map(|block| Sha256::digest(block).into())
            .collect();
        let previous = previous
            .filter(|g| g.manifest.network == manifest.network && g.manifest.salt == manifest.salt);
        #[cfg(feature = "native-reinspiring")]
        {
            let setup = native_packing_setup(&manifest.network, &manifest.salt);
            let packing: Vec<PackingBlock> = blocks
                .par_iter()
                .enumerate()
                .map(|(i, block)| {
                    if let Some(old) = previous
                        .filter(|g| g.packing_digests.get(i) == packing_digests.get(i))
                        .and_then(|g| g.packing.get(i))
                    {
                        return Ok(old.clone());
                    }
                    reinspiring::native::NativePreprocessed::build_two_mask(&setup, &block.rows)
                        .map(Arc::new)
                        .map_err(|_| Error::Pir)
                })
                .collect::<Result<_, _>>()?;
            let packing_matrix_bytes: usize = packing.iter().map(|b| b.coefficient_bytes()).sum();
            tracing::info!(
                hint_bytes = bytes.len(),
                packing_matrix_bytes,
                preparation_threads = rayon::current_num_threads(),
                "Status native packing material geometry"
            );
            let public =
                zakura_pir_query_server::native::publish(&packing).map_err(|_| Error::Pir)?;
            manifest.public_digest = Sha256::digest(&public).into();
            Ok(Self {
                manifest,
                public: Arc::new(public),
                units: Vec::new(),
                packing: Arc::new(packing),
                packing_digests,
            })
        }
        #[cfg(not(feature = "native-reinspiring"))]
        {
            let packing = PreparedPacking::prepare(
                p.rlwe(),
                &blocks,
                packing_setup(),
                previous.and_then(|g| g.packing.as_deref()),
                |i| previous.is_some_and(|g| g.packing_digests.get(i) == packing_digests.get(i)),
            )
            .map_err(|_| Error::Pir)?;
            let packing_matrix_bytes = packing.coefficient_bytes();
            tracing::info!(
                hint_bytes = bytes.len(),
                packing_matrix_bytes,
                preparation_threads = rayon::current_num_threads(),
                "Status packing material geometry"
            );
            let public = packing.public_rows(p.rlwe().q);
            manifest.public_digest = Sha256::digest(&public).into();
            Ok(Self {
                manifest,
                public: Arc::new(public),
                units: Vec::new(),
                packing: Some(Arc::new(packing)),
                packing_digests,
            })
        }
    }

    /// Reuse unchanged polynomial units. Changed units are rebuilt; this baseline
    /// deliberately does not claim row-level incremental preprocessing.
    pub fn prepare_worker(
        snapshot: &index::Snapshot,
        generation: u64,
        recovery_epoch: u64,
        observed_ms: u64,
        previous: Option<&Self>,
        backend: MatvecBackend,
    ) -> Result<(Self, Preparation), Error> {
        let start = Instant::now();
        let p = profile();
        let d = p.rlwe().d;
        let masks = query_masks(&snapshot.network, &snapshot.salt)?;
        let local = ProductionSimplePirParams::new(d as u64, ITEM_BITS, SimplePirProfile::P16Q48)
            .map_err(|_| Error::Pir)?;
        let mut units = Vec::new();
        let mut rebuilt = 0;
        let mut reused = 0;
        let mut incremental_units = 0;
        for (i, bytes) in snapshot.rows.chunks_exact(d * ROW_BYTES).enumerate() {
            let digest: Hash = Sha256::digest(bytes).into();
            let old = previous
                .filter(|prev| {
                    prev.manifest.network == snapshot.network && prev.manifest.salt == snapshot.salt
                })
                .and_then(|prev| prev.units.get(i))
                .filter(|u| u.bytes_digest == digest);
            if let Some(old) = old {
                units.push(old.clone());
                reused += 1;
                continue;
            }
            // Snapshot rows are already verified, row-major little-endian u16.
            // The shared loader enforces full consumption without keeping a
            // second database copy and preserves a kernel-construction error.
            let mut reader = bytes;
            let db = build_from_le_u16(&mut reader, bytes.len() / 2, |values| {
                IPIRServer::try_from_profile_with_backend(&local, values, false, true, backend)
                    .map(PreparedDatabase::new)
            })
            .map_err(|_| Error::Unavailable)?
            .map_err(|_| Error::Unavailable)?;
            let base = previous
                .filter(|prev| {
                    prev.manifest.network == snapshot.network && prev.manifest.salt == snapshot.salt
                })
                .and_then(|prev| prev.units.get(i));
            let hint =
                if let Some(hint) = base.and_then(|old| incremental_hint(old, bytes, &masks[i])) {
                    incremental_units += 1;
                    hint
                } else {
                    unit_hint(&db, &masks[i..i + 1])?
                };
            units.push(Arc::new(Unit {
                bytes_digest: digest,
                rows: bytes.to_vec(),
                db,
                hint,
            }));
            rebuilt += 1;
        }
        let database_hint_ms = start.elapsed().as_secs_f64() * 1000.;
        let packing_ms = 0.;
        let public = Vec::new();
        #[cfg(feature = "native-reinspiring")]
        let packing = Arc::new(Vec::new());
        #[cfg(not(feature = "native-reinspiring"))]
        let packing = None;
        let manifest = Manifest {
            protocol: PROTOCOL.into(),
            network: snapshot.network,
            salt: snapshot.salt,
            generation,
            recovery_epoch,
            coverage_start: snapshot.start,
            anchor_height: snapshot.height,
            anchor_hash: snapshot.anchor,
            observed_ms,
            entries: snapshot.entries,
            rows_digest: snapshot.digest,
            public_digest: Sha256::digest(&public).into(),
        };
        manifest.validate()?;
        let stats = Preparation {
            unit_rows: d,
            rebuilt_units: rebuilt,
            reused_units: reused,
            incremental_units,
            database_hint_ms,
            packing_ms,
            total_ms: start.elapsed().as_secs_f64() * 1000.,
        };
        Ok((
            Self {
                manifest,
                public: Arc::new(public),
                units,
                packing,
                packing_digests: Vec::new(),
            },
            stats,
        ))
    }
    #[cfg(not(feature = "native-reinspiring"))]
    pub fn coefficients(&self, bytes: &[u8]) -> Result<Vec<u64>, Error> {
        let p = profile();
        let keys_len = PackingKeys::encoded_len(p.rlwe());
        if bytes.len() != HEADER_BYTES + keys_len + ROWS * 48 / 8
            || &bytes[..4] != QUERY_MAGIC
            || bytes[4..36] != self.manifest.id()
        {
            return Err(Error::Malformed);
        }
        PackingKeys::decode(p.rlwe(), &bytes[HEADER_BYTES..HEADER_BYTES + keys_len])
            .map_err(|_| Error::Malformed)?;
        Ok(ipir_sp::IPIRSimpleQuery::from_switched_bytes(
            &bytes[HEADER_BYTES + keys_len..],
            ROWS,
            p.rlwe().q,
            48,
        )
        .map_err(|_| Error::Malformed)?
        .into_first_dim())
    }
    pub fn evaluate(&self, coefficients: &[u64]) -> Result<Vec<u64>, Error> {
        let p = profile();
        if self.units.len() != ROWS / p.rlwe().d
            || coefficients.len() != ROWS
            || coefficients.iter().any(|v| *v >= modulus())
        {
            return Err(Error::Malformed);
        }
        let mut out = vec![0u64; p.ypir().db_cols];
        for (unit, query) in self.units.iter().zip(coefficients.chunks_exact(p.rlwe().d)) {
            #[cfg(feature = "native-reinspiring")]
            let partial = unit.db.try_multiply_power_of_two(modulus(), query);
            #[cfg(not(feature = "native-reinspiring"))]
            let partial = unit.db.try_multiply_query(p.rlwe(), query);
            let partial = partial.map_err(|_| Error::Unavailable)?;
            pir_runtime::evaluation::add_intermediate_assign_mod(&mut out, &partial, modulus())
                .map_err(|_| Error::Pir)?;
        }
        Ok(out)
    }
    #[cfg(feature = "native-reinspiring")]
    fn native_request(
        &self,
        bytes: &[u8],
    ) -> Result<(reinspiring::native::NativeKeys, Vec<u64>), Error> {
        // The selection is 49 bits, as the wallet-pir client sends it, or 44
        // dithered bits; the exact length says which. The envelope is the same.
        if bytes.len() < HEADER_BYTES
            || zakura_pir_query_server::native::accepted_query_bits(
                ROWS,
                bytes.len() - HEADER_BYTES,
            )
            .is_none()
            || &bytes[..4] != QUERY_MAGIC
            || bytes[4..36] != self.manifest.id()
        {
            return Err(Error::Malformed);
        }
        zakura_pir_query_server::native::parse_accepted(
            &native_packing_setup(&self.manifest.network, &self.manifest.salt),
            &bytes[HEADER_BYTES..],
            ROWS,
        )
        .map_err(|_| Error::Malformed)
    }
    #[cfg(feature = "native-reinspiring")]
    pub fn coefficients(&self, bytes: &[u8]) -> Result<Vec<u64>, Error> {
        self.native_request(bytes).map(|(_, query)| query)
    }
    #[cfg(feature = "native-reinspiring")]
    pub fn pack(&self, body: &[u8], values: &[u64]) -> Result<Vec<u8>, Error> {
        let (keys, _) = self.native_request(body)?;
        if self.packing.is_empty() {
            return Err(Error::Unavailable);
        }
        if values.len() != profile().ypir().db_cols {
            return Err(Error::Malformed);
        }
        let mut response = body[..HEADER_BYTES].to_vec();
        response.extend(
            zakura_pir_query_server::native::pack(&self.packing, &keys, values)
                .map_err(|_| Error::Pir)?,
        );
        Ok(response)
    }
    #[cfg(not(feature = "native-reinspiring"))]
    pub fn pack(&self, body: &[u8], values: &[u64]) -> Result<Vec<u8>, Error> {
        self.coefficients(body)?;
        let packing = self.packing.as_ref().ok_or(Error::Unavailable)?;
        let p = profile();
        if values.len() != p.ypir().db_cols || values.iter().any(|v| *v >= p.rlwe().q) {
            return Err(Error::Malformed);
        }
        let len = PackingKeys::encoded_len(p.rlwe());
        let keys = PackingKeys::decode(p.rlwe(), &body[HEADER_BYTES..HEADER_BYTES + len])
            .map_err(|_| Error::Malformed)?;
        let mut response = body[..HEADER_BYTES].to_vec();
        response.extend(
            packing
                .pack(values, &keys, p.ypir().q_prime_1)
                .map_err(|_| Error::Pir)?,
        );
        Ok(response)
    }
    /// Differential oracle: compare composed hints with upstream full-database preprocessing.
    pub fn verify_full_hint(&self, snapshot: &index::Snapshot) -> Result<(), Error> {
        let p = profile();
        let db = PreparedDatabase::new(IPIRServer::from_profile(
            p,
            snapshot
                .rows
                .chunks_exact(2)
                .map(|v| u16::from_le_bytes([v[0], v[1]])),
            false,
            true,
        ));
        let masks = query_masks(&snapshot.network, &snapshot.salt)?;
        let expected = unit_hint(&db, &masks)?;
        for (block_i, block) in expected.iter().enumerate() {
            for (r, row) in block.rows.iter().enumerate() {
                for (c, value) in row.iter().enumerate() {
                    let sum = self
                        .units
                        .iter()
                        .fold(0u128, |a, u| a + u128::from(u.hint[block_i].rows[r][c]))
                        % u128::from(modulus());
                    if sum != u128::from(*value) {
                        return Err(Error::Pir);
                    }
                }
            }
        }
        Ok(())
    }
}

/// Public first-dimension query masks of the selected profile, one per d rows.
fn query_masks(network: &Hash, salt: &Hash) -> Result<Vec<Vec<u64>>, Error> {
    #[cfg(feature = "native-reinspiring")]
    {
        Ok(native_query_masks(network, salt))
    }
    #[cfg(not(feature = "native-reinspiring"))]
    {
        let client = IPIRClient::from_profile(ROWS as u64, ITEM_BITS, SimplePirProfile::P16Q48)
            .map_err(|_| Error::Pir)?;
        Ok(client
            .generate_public_query_setup_simplepir_from_seed(setup_seed(network, salt))
            .polys()
            .to_vec())
    }
}

/// Hint contribution H = A * D for the rows covered by `masks`.
fn unit_hint(db: &PreparedDatabase, masks: &[Vec<u64>]) -> Result<Vec<CrsBlock>, Error> {
    #[cfg(feature = "native-reinspiring")]
    {
        pir_serving::hint(db, masks).map_err(|_| Error::Pir)
    }
    #[cfg(not(feature = "native-reinspiring"))]
    {
        Ok(db.hint(profile().rlwe(), masks))
    }
}

#[cfg(test)]
#[path = "generation/incremental_tests.rs"]
mod incremental_tests;

#[cfg(all(test, not(feature = "native-reinspiring")))]
#[path = "generation/fixed_input_tests.rs"]
mod fixed_input_tests;
