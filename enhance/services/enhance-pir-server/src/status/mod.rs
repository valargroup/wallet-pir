//! Experimental isolated HTTP service. Production controller recovery is not wired here.
pub mod fixture;
pub mod index;
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
    server::{
        build_pack_preprocessed_blocks, pack_intermediate_blocks, published_c1_rows, CrsBlock,
        MatvecBackend,
    },
    IPIRClient, IPIRServer, ProductionSimplePirParams, SimplePirProfile,
};
use sha2::{Digest, Sha256};
use std::{
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

struct Unit {
    bytes_digest: Hash,
    db: IPIRServer<u16>,
    hint: Vec<CrsBlock>,
}
#[derive(Clone)]
pub struct Generation {
    pub manifest: Manifest,
    pub public: Vec<u8>,
    units: Vec<Arc<Unit>>,
    packing: Arc<Vec<QueryPackPreprocessed<'static>>>,
    top: Arc<TopKeyImages<'static>>,
}

#[derive(Debug, serde::Serialize)]
pub struct Preparation {
    pub unit_rows: usize,
    pub rebuilt_units: usize,
    pub reused_units: usize,
    pub database_hint_ms: f64,
    pub packing_ms: f64,
    pub total_ms: f64,
}

impl Generation {
    /// Reuse unchanged polynomial units. Changed units are rebuilt; this baseline
    /// deliberately does not claim row-level incremental preprocessing.
    pub fn prepare(
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
            let hint = db
                .perform_offline_precomputation_simplepir(p.rlwe(), &setup.polys()[i..i + 1])
                .crs_blocks;
            units.push(Arc::new(Unit {
                bytes_digest: digest,
                db,
                hint,
            }));
            rebuilt += 1;
        }
        let mut hint = units[0].hint.clone();
        for unit in &units[1..] {
            for (out, input) in hint.iter_mut().zip(&unit.hint) {
                for (out, input) in out.rows.iter_mut().zip(&input.rows) {
                    for (a, b) in out.iter_mut().zip(input) {
                        *a = ((u128::from(*a) + u128::from(*b)) % u128::from(p.rlwe().q)) as u64;
                    }
                }
            }
        }
        let database_hint_ms = start.elapsed().as_secs_f64() * 1000.;
        let packing_start = Instant::now();
        let packing = build_pack_preprocessed_blocks(p.rlwe(), &hint).map_err(|_| Error::Pir)?;
        let public = published_c1_rows(&packing, p.rlwe().q);
        let top = TopKeyImages::build(p.rlwe());
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
            database_hint_ms,
            packing_ms: packing_start.elapsed().as_secs_f64() * 1000.,
            total_ms: start.elapsed().as_secs_f64() * 1000.,
        };
        Ok((
            Self {
                manifest,
                public,
                units,
                packing: Arc::new(packing),
                top: Arc::new(top),
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
        if coefficients.len() != ROWS || coefficients.iter().any(|v| *v >= p.rlwe().q) {
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
        let ciphertexts = pack_intermediate_blocks(values, &keys, &self.top, &self.packing)
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
}
impl Controller {
    pub fn new(g: Generation) -> Self {
        Self {
            views: Arc::new(RwLock::new(Views {
                active: Arc::new(g),
                retained: Vec::new(),
            })),
        }
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
        let mut views = self.views.write().unwrap();
        if g.manifest.generation <= views.active.manifest.generation
            || g.manifest.recovery_epoch < views.active.manifest.recovery_epoch
            || g.manifest.network != views.active.manifest.network
        {
            return Err(Error::Malformed);
        }
        // Only two distinct material generations may be retained by the Status controller.
        views
            .retained
            .retain(|old| old.manifest.fresh(now_ms()).is_ok());
        if views
            .retained
            .iter()
            .any(|old| old.manifest.generation != views.active.manifest.generation)
        {
            return Err(Error::Unavailable);
        }
        let old = views.active.clone();
        views.retained.push(old);
        views.active = Arc::new(g);
        Ok(())
    }
    fn check(&self, g: &Generation) -> Result<(), StatusCode> {
        if self.current().manifest.recovery_epoch != g.manifest.recovery_epoch {
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
        if views.retained.len() >= 8 {
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
    Ok(g.public.clone())
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
