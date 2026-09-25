//! Experimental isolated HTTP service. Production controller recovery is not wired here.
pub mod artifact;
pub mod authority;
pub mod distributed;
pub mod fixture;
pub mod index;
pub mod publisher;
pub mod source;
pub mod telemetry;
use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use enhance_pir::status::*;
use inspiring::{QueryPackPreprocessed, TopKeyImages};
use ipir_sp::{
    server::{pack_intermediate_blocks, published_c1_rows, CrsBlock, MatvecBackend},
    IPIRClient, IPIRServer, ProductionSimplePirParams, SimplePirProfile,
};
use rayon::prelude::*;
use sha2::{Digest, Sha256};
use std::{
    sync::atomic::{AtomicBool, Ordering},
    sync::{Arc, OnceLock, RwLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Semaphore;

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
pub fn profile() -> &'static ProductionSimplePirParams {
    static P: OnceLock<ProductionSimplePirParams> = OnceLock::new();
    P.get_or_init(|| {
        ProductionSimplePirParams::new(ROWS as u64, ITEM_BITS, SimplePirProfile::P16Q48)
            .expect("fixed status profile")
    })
}

fn top_images() -> Arc<TopKeyImages<'static>> {
    static TOP: OnceLock<Arc<TopKeyImages<'static>>> = OnceLock::new();
    TOP.get_or_init(|| Arc::new(TopKeyImages::build(profile().rlwe())))
        .clone()
}

struct Unit {
    bytes_digest: Hash,
    rows: Vec<u8>,
    db: IPIRServer<u16>,
    hint: Vec<CrsBlock>,
}
/// Update H = A * D in Z_q[X]/(X^d+1) for at most 4096 changed
/// plaintext coefficients per unit. A coefficient delta at row r contributes
/// delta * X^r * A to its column; wrapping X^d negates the coefficient.
/// The public setup and q48 profile are unchanged. Dense changes use upstream
/// NTT reconstruction. The candidate owns its hint; admitted views are immutable.
fn incremental_hint(old: &Unit, bytes: &[u8], setup: &[u64]) -> Option<Vec<CrsBlock>> {
    let p = profile();
    let d = p.rlwe().d;
    let q = p.rlwe().q;
    let columns = ROW_BYTES / 2;
    let mut changes = Vec::new();
    if bytes.len() != old.rows.len() {
        return None;
    }
    for (i, (value, prior)) in bytes
        .chunks_exact(2)
        .zip(old.rows.chunks_exact(2))
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
    let mut hint = old.hint.clone();
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

#[derive(Clone)]
pub struct Generation {
    pub manifest: Manifest,
    pub public: Arc<Vec<u8>>,
    units: Vec<Arc<Unit>>,
    packing: Arc<Vec<QueryPackPreprocessed<'static>>>,
    top: Option<Arc<TopKeyImages<'static>>>,
    packing_digests: Vec<Hash>,
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
        worker.top = router.top;
        worker.packing_digests = router.packing_digests;
        stats.packing_ms = started.elapsed().as_secs_f64() * 1000.;
        stats.total_ms += stats.packing_ms;
        Ok((worker, stats))
    }

    /// Fixed-shape, little-endian hint artifact. Never includes wallet queries.
    pub fn hint_bytes(&self) -> Result<Vec<u8>, Error> {
        let mut hint = self.units.first().ok_or(Error::Unavailable)?.hint.clone();
        let q = profile().rlwe().q;
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
                if values.iter().any(|v| *v >= p.rlwe().q) {
                    return Err(Error::Malformed);
                }
                rows.push(values);
            }
            blocks.push(CrsBlock { rows });
        }
        let top = top_images();
        let packing_digests: Vec<Hash> = bytes
            .par_chunks(d * d * 8)
            .map(|block| Sha256::digest(block).into())
            .collect();
        let previous = previous
            .filter(|g| g.manifest.network == manifest.network && g.manifest.salt == manifest.salt);
        let packing: Vec<_> = blocks
            .par_iter()
            .enumerate()
            .map(|(i, block)| {
                if let Some(old) = previous
                    .filter(|g| g.packing_digests.get(i) == packing_digests.get(i))
                    .and_then(|g| g.packing.get(i))
                {
                    // Upstream's precomputation owns immutable matrices. Copy the
                    // exact values rather than rebuilding unchanged CRS blocks.
                    return Ok(QueryPackPreprocessed {
                        params: old.params,
                        collapse_a_final_ntt: old.collapse_a_final_ntt.clone(),
                        digits_ntt: old.digits_ntt.clone(),
                    });
                }
                QueryPackPreprocessed::build_with_top(p.rlwe(), &block.to_ntt(p.rlwe()), &top)
                    .map_err(|_| Error::Pir)
            })
            .collect::<Result<_, _>>()?;
        let public = published_c1_rows(&packing, p.rlwe().q);
        manifest.public_digest = Sha256::digest(&public).into();
        Ok(Self {
            manifest,
            public: Arc::new(public),
            units: Vec::new(),
            packing: Arc::new(packing),
            top: Some(top),
            packing_digests,
        })
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
        let client = IPIRClient::from_profile(ROWS as u64, ITEM_BITS, SimplePirProfile::P16Q48)
            .map_err(|_| Error::Pir)?;
        let setup = client.generate_public_query_setup_simplepir_from_seed(setup_seed(
            &snapshot.network,
            &snapshot.salt,
        ));
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
            let values = bytes
                .chunks_exact(2)
                .map(|x| u16::from_le_bytes([x[0], x[1]]));
            let db =
                IPIRServer::try_from_profile_with_backend(&local, values, false, true, backend)
                    .map_err(|_| Error::Unavailable)?;
            let base = previous
                .filter(|prev| {
                    prev.manifest.network == snapshot.network && prev.manifest.salt == snapshot.salt
                })
                .and_then(|prev| prev.units.get(i));
            let hint = if let Some(hint) =
                base.and_then(|old| incremental_hint(old, bytes, &setup.polys()[i]))
            {
                incremental_units += 1;
                hint
            } else {
                db.perform_offline_precomputation_simplepir(p.rlwe(), &setup.polys()[i..i + 1])
                    .crs_blocks
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
        let packing = Vec::new();
        let top = None;
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
                packing: Arc::new(packing),
                top,
                packing_digests: Vec::new(),
            },
            stats,
        ))
    }
    pub fn coefficients(&self, bytes: &[u8]) -> Result<Vec<u64>, Error> {
        let p = profile();
        let keys_len = ipir_sp::serialize::serialized_packing_keys_len(p.rlwe());
        if bytes.len() != HEADER_BYTES + keys_len + ROWS * 48 / 8
            || &bytes[..4] != b"SPQ1"
            || bytes[4..36] != self.manifest.id()
        {
            return Err(Error::Malformed);
        }
        ipir_sp::serialize::deserialize_packing_keys(
            p.rlwe(),
            &bytes[HEADER_BYTES..HEADER_BYTES + keys_len],
        )
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
            || coefficients.iter().any(|v| *v >= p.rlwe().q)
        {
            return Err(Error::Malformed);
        }
        let mut out = vec![0u64; p.ypir().db_cols];
        for (unit, query) in self.units.iter().zip(coefficients.chunks_exact(p.rlwe().d)) {
            let partial = unit
                .db
                .try_multiply_query(p.rlwe(), query)
                .map_err(|_| Error::Unavailable)?;
            for (a, b) in out.iter_mut().zip(partial) {
                *a = ((u128::from(*a) + u128::from(b)) % u128::from(p.rlwe().q)) as u64;
            }
        }
        Ok(out)
    }
    pub fn pack(&self, body: &[u8], values: &[u64]) -> Result<Vec<u8>, Error> {
        self.coefficients(body)?;
        if self.packing.is_empty() {
            return Err(Error::Unavailable);
        }
        let p = profile();
        if values.len() != p.ypir().db_cols || values.iter().any(|v| *v >= p.rlwe().q) {
            return Err(Error::Malformed);
        }
        let len = ipir_sp::serialize::serialized_packing_keys_len(p.rlwe());
        let keys = ipir_sp::serialize::deserialize_packing_keys(
            p.rlwe(),
            &body[HEADER_BYTES..HEADER_BYTES + len],
        )
        .map_err(|_| Error::Malformed)?;
        let ciphertexts = pack_intermediate_blocks(
            values,
            &keys,
            self.top.as_deref().ok_or(Error::Unavailable)?,
            &self.packing,
        )
        .map_err(|_| Error::Pir)?;
        let mut response = body[..HEADER_BYTES].to_vec();
        response.extend(ipir_sp::modulus_switch::serialize_rlwe_response_bodies(
            &ciphertexts,
            p.ypir().q_prime_1,
        ));
        Ok(response)
    }
    /// Differential oracle: compare composed hints with upstream full-database preprocessing.
    pub fn verify_full_hint(&self, snapshot: &index::Snapshot) -> Result<(), Error> {
        let p = profile();
        let db = IPIRServer::from_profile(
            p,
            snapshot
                .rows
                .chunks_exact(2)
                .map(|v| u16::from_le_bytes([v[0], v[1]])),
            false,
            true,
        );
        let client = IPIRClient::from_profile(ROWS as u64, ITEM_BITS, SimplePirProfile::P16Q48)
            .map_err(|_| Error::Pir)?;
        let setup = client.generate_public_query_setup_simplepir_from_seed(setup_seed(
            &snapshot.network,
            &snapshot.salt,
        ));
        let expected = db
            .perform_offline_precomputation_simplepir(p.rlwe(), setup.polys())
            .crs_blocks;
        for (block_i, block) in expected.iter().enumerate() {
            for (r, row) in block.rows.iter().enumerate() {
                for (c, value) in row.iter().enumerate() {
                    let sum = self
                        .units
                        .iter()
                        .fold(0u128, |a, u| a + u128::from(u.hint[block_i].rows[r][c]))
                        % u128::from(p.rlwe().q);
                    if sum != u128::from(*value) {
                        return Err(Error::Pir);
                    }
                }
            }
        }
        Ok(())
    }
}

struct Views {
    active: Arc<Generation>,
    retained: Vec<Arc<Generation>>,
}
#[derive(Clone)]
pub struct Controller {
    views: Arc<RwLock<Views>>,
    revoked: Arc<AtomicBool>,
}
impl Controller {
    pub fn new(g: Generation) -> Self {
        Self {
            revoked: Arc::new(AtomicBool::new(false)),
            views: Arc::new(RwLock::new(Views {
                active: Arc::new(g),
                retained: Vec::new(),
            })),
        }
    }
    pub fn can_prepare(&self) -> bool {
        let views = self.views.read().unwrap();
        views.retained.iter().all(|g| Arc::strong_count(g) == 1)
    }
    pub fn revoke(&self) {
        self.revoked.store(true, Ordering::SeqCst);
    }
    pub fn current(&self) -> Arc<Generation> {
        self.views.read().unwrap().active.clone()
    }
    fn resolve(&self, id: Hash) -> Result<Arc<Generation>, StatusCode> {
        let views = self.views.read().unwrap();
        std::iter::once(&views.active)
            .chain(views.retained.iter())
            .find(|g| g.manifest.id() == id)
            .cloned()
            .ok_or(StatusCode::CONFLICT)
    }
    fn for_body(&self, body: &[u8]) -> Result<Arc<Generation>, StatusCode> {
        if body.len() < HEADER_BYTES || &body[..4] != b"SPQ1" {
            return Err(StatusCode::BAD_REQUEST);
        }
        self.resolve(body[4..36].try_into().unwrap())
    }
    pub fn activate(&self, g: Generation) -> Result<(), Error> {
        g.manifest.fresh(now_ms())?;
        let mut views = self.views.write().unwrap();
        if g.manifest.generation <= views.active.manifest.generation
            || g.manifest.recovery_epoch < views.active.manifest.recovery_epoch
            || g.manifest.network != views.active.manifest.network
        {
            return Err(Error::Malformed);
        }
        // Keep only the immediately preceding material generation. In-flight
        // requests pin their own Arc; older sessions receive 409 and must
        // reinitialize. This bounds material retention without serializing
        // publications behind the entire freshness window.
        let old_active = views.active.clone();
        // Preserve pins across metadata-only refreshes as well as material
        // changes. `can_prepare` applies backpressure until older pins drain;
        // losing their bookkeeping here would allow unbounded hidden material.
        views.retained.retain(|old| Arc::strong_count(old) > 1);
        // Retain one revoked view as a tombstone so clients receive 410 on
        // epoch changes. `check` rejects it before its material is used.
        views.retained.push(old_active);
        views.active = Arc::new(g);
        Ok(())
    }
    fn check(&self, g: &Generation) -> Result<(), StatusCode> {
        if self.revoked.load(Ordering::SeqCst)
            || self.current().manifest.recovery_epoch != g.manifest.recovery_epoch
        {
            return Err(StatusCode::GONE);
        }
        g.manifest
            .fresh(now_ms())
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)
    }
    /// Synthetic fixture only: reaffirm an unchanged synthetic source, without rebuilding hints.
    /// Requires an explicit oracle digest so a failed source poll cannot renew freshness.
    pub fn reaffirm_fixture(&self, expected: Hash) -> Result<(), Error> {
        let mut views = self.views.write().unwrap();
        if views.active.manifest.rows_digest != expected {
            return Err(Error::Malformed);
        }
        if now_ms() / 1000 == views.active.manifest.observed_ms / 1000 {
            return Ok(());
        }
        views
            .retained
            .retain(|old| old.manifest.fresh(now_ms()).is_ok());
        // A one-second observation cadence needs enough retained identities to
        // cover the full freshness window while admitted requests finish.
        if views.retained.len() >= (MAX_AGE_MS / 1000 + 2) as usize {
            return Err(Error::Unavailable);
        }
        let mut next = (*views.active).clone();
        next.manifest.observed_ms = now_ms();
        let old = views.active.clone();
        views.retained.push(old);
        views.active = Arc::new(next);
        Ok(())
    }
    pub fn coordinator_routes(&self, router_origin: String) -> Router {
        let state = HttpState::new(self.clone(), router_origin);
        Router::new()
            .route("/v1/status/init", get(init))
            .route("/v1/status/session/:id", get(session))
            .route("/v1/status/query", post(forward))
            .layer(DefaultBodyLimit::max(512 * 1024))
            .with_state(state)
            .layer(axum::middleware::from_fn(|request, next| {
                telemetry::observe("coordinator", request, next)
            }))
    }
    pub fn router_routes(&self, worker_origin: String) -> Router {
        Router::new()
            .route("/v1/status/query", post(query))
            .layer(DefaultBodyLimit::max(512 * 1024))
            .with_state(HttpState::new(self.clone(), worker_origin))
            .layer(axum::middleware::from_fn(|request, next| {
                telemetry::observe("router", request, next)
            }))
    }
    pub fn worker_routes(&self) -> Router {
        Router::new()
            .route("/evaluate", post(evaluate))
            .layer(DefaultBodyLimit::max(HEADER_BYTES + ROWS * 8))
            .with_state(HttpState::new(self.clone(), String::new()))
            .layer(axum::middleware::from_fn(|request, next| {
                telemetry::observe("worker", request, next)
            }))
    }
}

#[derive(Clone)]
struct HttpState {
    controller: Controller,
    origin: String,
    http: reqwest::Client,
    permits: Arc<Semaphore>,
}
impl HttpState {
    fn new(controller: Controller, origin: String) -> Self {
        Self {
            controller,
            origin,
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                // Keep Status listener shutdown observable: no surviving idle connection
                // may mask a stopped role in the end-to-end failure scenario.
                .pool_max_idle_per_host(0)
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap(),
            permits: Arc::new(Semaphore::new(4)),
        }
    }
}
async fn init(State(s): State<HttpState>) -> Result<Json<Manifest>, StatusCode> {
    let g = s.controller.current();
    s.controller.check(&g)?;
    Ok(Json(g.manifest.clone()))
}
async fn session(
    State(s): State<HttpState>,
    Path(id): Path<String>,
) -> Result<Vec<u8>, StatusCode> {
    let id: Hash = hex::decode(id)
        .map_err(|_| StatusCode::BAD_REQUEST)?
        .try_into()
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let g = s.controller.resolve(id)?;
    s.controller.check(&g)?;
    Ok(g.public.as_ref().clone())
}
async fn bounded(mut response: reqwest::Response, limit: usize) -> Result<Vec<u8>, StatusCode> {
    if !response.status().is_success() {
        return Err(
            StatusCode::from_u16(response.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY)
        );
    }
    let mut out = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?
    {
        if chunk.len() > limit.saturating_sub(out.len()) {
            return Err(StatusCode::BAD_GATEWAY);
        }
        out.extend(&chunk);
    }
    Ok(out)
}
async fn forward(State(s): State<HttpState>, body: Bytes) -> Result<Vec<u8>, StatusCode> {
    let _permit = s
        .permits
        .clone()
        .try_acquire_owned()
        .map_err(|_| StatusCode::TOO_MANY_REQUESTS)?;
    let g = s.controller.for_body(&body)?;
    s.controller.check(&g)?;
    let response = s
        .http
        .post(format!("{}/v1/status/query", s.origin))
        .body(body)
        .send()
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    let bytes = bounded(response, 512 * 1024).await?;
    s.controller.check(&g)?;
    Ok(bytes)
}
async fn evaluate(State(s): State<HttpState>, body: Bytes) -> Result<Vec<u8>, StatusCode> {
    let permit = s
        .permits
        .clone()
        .try_acquire_owned()
        .map_err(|_| StatusCode::TOO_MANY_REQUESTS)?;
    let g = s.controller.for_body(&body)?;
    s.controller.check(&g)?;
    if body.len() != HEADER_BYTES + ROWS * 8 {
        return Err(StatusCode::BAD_REQUEST);
    }
    let controller = s.controller.clone();
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let coefficients = body[HEADER_BYTES..]
            .chunks_exact(8)
            .map(|b| u64::from_le_bytes(b.try_into().unwrap()))
            .collect::<Vec<_>>();
        let values = g
            .evaluate(&coefficients)
            .map_err(|_| StatusCode::BAD_REQUEST)?;
        let mut response = body[..HEADER_BYTES].to_vec();
        for v in values {
            response.extend(v.to_le_bytes());
        }
        controller.check(&g)?;
        Ok(response)
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
}
async fn query(State(s): State<HttpState>, body: Bytes) -> Result<Vec<u8>, StatusCode> {
    let permit = s
        .permits
        .clone()
        .try_acquire_owned()
        .map_err(|_| StatusCode::TOO_MANY_REQUESTS)?;
    let g = s.controller.for_body(&body)?;
    s.controller.check(&g)?;
    let coefficients = g.coefficients(&body).map_err(|_| StatusCode::BAD_REQUEST)?;
    let mut request = body[..HEADER_BYTES].to_vec();
    for value in coefficients {
        request.extend(value.to_le_bytes());
    }
    let response = s
        .http
        .post(format!("{}/evaluate", s.origin))
        .body(request)
        .send()
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    let response = bounded(response, HEADER_BYTES + profile().ypir().db_cols * 8).await?;
    if response.len() != HEADER_BYTES + profile().ypir().db_cols * 8
        || response[..HEADER_BYTES] != body[..HEADER_BYTES]
    {
        return Err(StatusCode::BAD_GATEWAY);
    }
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let values = response[HEADER_BYTES..]
            .chunks_exact(8)
            .map(|b| u64::from_le_bytes(b.try_into().unwrap()))
            .collect::<Vec<_>>();
        let pack_started = Instant::now();
        let packed = g.pack(&body, &values);
        telemetry::stage("router_pack", pack_started.elapsed(), packed.is_ok());
        let packed = packed.map_err(|_| StatusCode::BAD_GATEWAY)?;
        s.controller.check(&g)?;
        Ok(packed)
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
}

#[cfg(test)]
mod controller_tests {
    use super::*;

    fn generation() -> Generation {
        Generation {
            manifest: Manifest {
                protocol: PROTOCOL.into(),
                network: [1; 32],
                salt: [2; 32],
                generation: 1,
                recovery_epoch: 0,
                coverage_start: 1,
                anchor_height: 1,
                anchor_hash: [3; 32],
                observed_ms: now_ms(),
                entries: 0,
                rows_digest: [4; 32],
                public_digest: [5; 32],
            },
            public: Arc::new(Vec::new()),
            units: Vec::new(),
            packing: Arc::new(Vec::new()),
            top: None,
            packing_digests: Vec::new(),
        }
    }

    #[test]
    fn rapid_activation_bounds_retention_and_recovery_fences_old_sessions() {
        let base = generation();
        let controller = Controller::new(base.clone());
        let first = base.manifest.id();
        let mut second = base.clone();
        second.manifest.generation = 2;
        let second_id = second.manifest.id();
        controller.activate(second).unwrap();
        assert!(controller.resolve(first).is_ok());
        let mut third = base.clone();
        third.manifest.generation = 3;
        let third_id = third.manifest.id();
        controller.activate(third).unwrap();
        assert_eq!(controller.resolve(first).err(), Some(StatusCode::CONFLICT));
        assert!(controller.resolve(second_id).is_ok());
        let mut recovered = base;
        recovered.manifest.generation = 4;
        recovered.manifest.recovery_epoch = 1;
        controller.activate(recovered).unwrap();
        let revoked = controller.resolve(third_id).unwrap();
        assert_eq!(controller.check(&revoked), Err(StatusCode::GONE));
    }

    #[test]
    fn stale_candidate_cannot_be_activated() {
        let base = generation();
        let controller = Controller::new(base.clone());
        let mut stale = base;
        stale.manifest.generation = 2;
        stale.manifest.observed_ms = now_ms() - MAX_AGE_MS - 1;
        assert_eq!(controller.activate(stale), Err(Error::Stale));
        assert_eq!(controller.current().manifest.generation, 1);
    }
}

#[cfg(test)]
mod incremental_tests {
    use super::*;
    #[test]
    fn sparse_hint_matches_full_reconstruction_for_signed_negacyclic_boundaries() {
        let mut snapshot = fixture::snapshot(32, false).unwrap();
        let (base, _) =
            Generation::prepare_worker(&snapshot, 1, 1, now_ms(), None, MatvecBackend::Cpu)
                .unwrap();
        let locations = [
            0,
            ROW_BYTES - 2,
            (profile().rlwe().d - 1) * ROW_BYTES,
            ROWS * ROW_BYTES - 2,
        ];
        let original: Vec<_> = locations
            .iter()
            .map(|i| [snapshot.rows[*i], snapshot.rows[*i + 1]])
            .collect();
        for i in locations {
            snapshot.rows[i..i + 2].copy_from_slice(&u16::MAX.to_le_bytes());
        }
        snapshot.digest = Sha256::digest(&snapshot.rows).into();
        let (changed, stats) =
            Generation::prepare_worker(&snapshot, 2, 1, now_ms(), Some(&base), MatvecBackend::Cpu)
                .unwrap();
        assert_eq!(stats.incremental_units, 2);
        changed.verify_full_hint(&snapshot).unwrap();
        for (i, bytes) in locations.into_iter().zip(original) {
            snapshot.rows[i..i + 2].copy_from_slice(&bytes);
        }
        snapshot.digest = Sha256::digest(&snapshot.rows).into();
        let (restored, stats) = Generation::prepare_worker(
            &snapshot,
            3,
            1,
            now_ms(),
            Some(&changed),
            MatvecBackend::Cpu,
        )
        .unwrap();
        assert_eq!(stats.incremental_units, 2);
        restored.verify_full_hint(&snapshot).unwrap();
        assert_eq!(base.hint_bytes().unwrap(), restored.hint_bytes().unwrap());
    }
}
