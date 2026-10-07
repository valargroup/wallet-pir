//! The lookup itself, ported from the shard server's reference tooling client
//! (`examples/support/txdisplay.rs`) onto a synchronous transport.

use crate::{
    Placement, ProtocolKind, Provenance, Route, Tier, TxidError, TxidLookup, TxidReply,
    TxidRequest, TxidTransport,
};
use base64::{engine::general_purpose::STANDARD as B64, Engine};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use transparent_events::Txid;
use transparent_native::{NativeScheme, TableProfile};
use transparent_shard::display::{
    self, display_by_name, DisplayKind, DisplayManifest, DisplayMap, DisplayMapEntry, DisplayTable,
};
use transparent_shard::layout::Geometry;
use transparent_shard::txid::{self, TransparentDisplayRecord};

/// Native profiles are a pure function of geometry and table kind and costly
/// to derive, so clients may share them; nothing network-facing is shared.
pub type ProfileCache = Arc<Mutex<HashMap<(&'static str, DisplayKind), Arc<TableProfile>>>>;

/// A cached map older than this is fetched again before a height above its
/// coverage is reported as [`Placement::Above`].
const ABOVE_REFRESH_AGE: Duration = Duration::from_secs(30);

#[derive(Deserialize)]
struct InitTable {
    rows: u64,
    row_bytes: u32,
    scheme: NativeScheme,
    setup_seed: u64,
}

#[derive(Deserialize)]
struct InitGeometry {
    name: String,
    txdirectory: InitTable,
    txpages: InitTable,
}

#[derive(Deserialize)]
struct InitDocument {
    schema: String,
    codec: String,
    bucket_domain: String,
    native_schema: String,
    geometries: Vec<InitGeometry>,
}

struct Init {
    supported: bool,
    geometries: HashMap<String, InitGeometry>,
}

struct CachedMap {
    map: DisplayMap,
    sha256: String,
    fetched: Instant,
}

struct Setup {
    public_params: Vec<u8>,
    epoch: [u8; 8],
}

/// What a lookup knows before its first query.
struct Located {
    init: Arc<Init>,
    entry: DisplayMapEntry,
    map_sha256: String,
    geometry: &'static Geometry,
    bucket: u32,
    manifest: Arc<DisplayManifest>,
}

/// One table of one revision, ready to be queried.
struct Target {
    table: DisplayTable,
    profile: Arc<TableProfile>,
    setups: Vec<Arc<Setup>>,
}

/// One attempt's result before the 409 retry policy is applied.
enum Attempt {
    Done(TxidLookup),
    /// The cached map was older than [`ABOVE_REFRESH_AGE`] and the height lies
    /// above it.
    RefreshForAbove,
}

/// The txid display client. Holds caches only; every request goes through
/// the transport passed to [`TxidDisplayClient::lookup`].
pub struct TxidDisplayClient {
    profiles: ProfileCache,
    init: Option<Arc<Init>>,
    map: Option<Arc<CachedMap>>,
    manifests: HashMap<String, Arc<DisplayManifest>>,
    setups: HashMap<(String, DisplayTable, u32), Arc<Setup>>,
}

impl Default for TxidDisplayClient {
    fn default() -> Self {
        Self::new()
    }
}

/// Sends one request unless cancelled, and maps every status but 200 to an
/// error. 409 comes back as [`TxidError::Stale`]; the lookup retries it once.
fn send(
    transport: &mut impl TxidTransport,
    cancel: &dyn Fn() -> bool,
    request: TxidRequest,
) -> Result<TxidReply, TxidError> {
    if cancel() {
        return Err(TxidError::Cancelled);
    }
    let reply = transport.send(request)?;
    match reply.status {
        200 => Ok(reply),
        409 => Err(TxidError::Stale),
        429 | 500..=599 => Err(TxidError::Unavailable {
            retry_after: reply
                .retry_after
                .as_deref()
                .and_then(|value| value.trim().parse::<u64>().ok())
                .map(Duration::from_secs),
        }),
        status @ 400..=499 => Err(TxidError::Refused(status)),
        status => Err(TxidError::Protocol(ProtocolKind::Status(status))),
    }
}

fn protocol<E>(kind: ProtocolKind) -> impl FnOnce(E) -> TxidError {
    move |_| TxidError::Protocol(kind)
}

fn tier(entry: &DisplayMapEntry) -> Tier {
    if entry.sealed {
        Tier::Archive
    } else {
        Tier::Recent
    }
}

impl TxidDisplayClient {
    pub fn new() -> Self {
        Self::with_profiles(ProfileCache::default())
    }

    /// A client sharing derived native profiles with others.
    pub fn with_profiles(profiles: ProfileCache) -> Self {
        Self {
            profiles,
            init: None,
            map: None,
            manifests: HashMap::new(),
            setups: HashMap::new(),
        }
    }

    pub fn profiles(&self) -> ProfileCache {
        self.profiles.clone()
    }

    /// The SHA-256 of the map this client holds, if any.
    pub fn map_sha256(&self) -> Option<&str> {
        self.map.as_ref().map(|cached| cached.sha256.as_str())
    }

    /// Forgets the map so the next lookup fetches it again.
    pub fn invalidate_map(&mut self) {
        self.map = None;
    }

    /// Looks up `txid` (internal byte order) mined at `mined_height`, which
    /// the caller's accepted chain supplies. `cancel` is polled before every
    /// request.
    ///
    /// A 409 refreshes the map and retries the whole lookup once; a second
    /// 409 is [`TxidError::Stale`]. Nothing else is retried.
    pub fn lookup(
        &mut self,
        transport: &mut impl TxidTransport,
        txid: [u8; 32],
        mined_height: u64,
        cancel: &dyn Fn() -> bool,
    ) -> Result<TxidLookup, TxidError> {
        let txid = Txid(txid);
        // Every later attempt fetches the map afresh; an attempt made with a
        // fresh map never asks for another placement refresh.
        let mut refresh = false;
        let mut stale_retried = false;
        loop {
            match self.attempt(transport, cancel, txid, mined_height, refresh) {
                Ok(Attempt::Done(lookup)) => return Ok(lookup),
                Ok(Attempt::RefreshForAbove) => refresh = true,
                Err(TxidError::Stale) if !stale_retried => {
                    stale_retried = true;
                    refresh = true;
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn attempt(
        &mut self,
        transport: &mut impl TxidTransport,
        cancel: &dyn Fn() -> bool,
        txid: Txid,
        height: u64,
        refresh: bool,
    ) -> Result<Attempt, TxidError> {
        let located = match self.locate(transport, cancel, txid, height, refresh)? {
            Ok(located) => located,
            Err(attempt) => return Ok(attempt),
        };
        let geometry = located.geometry;
        let provenance = Provenance {
            map_sha256: located.map_sha256.clone(),
            shard_id: located.entry.shard_id,
            revision: located.entry.revision,
            manifest_digest: located.entry.manifest_digest.clone(),
            tier: tier(&located.entry),
        };

        // Directory phase: exactly two queries, whether or not they coincide.
        let rows = display::candidate_rows(
            &txid,
            located.entry.shard_id,
            located.bucket,
            geometry.directory_rows,
        );
        let segments = located.entry.directory_segments[located.bucket as usize];
        let table = DisplayTable::Directory(located.bucket);
        let target = self.target(transport, cancel, &located, table, segments)?;
        let decoded = self.queries(transport, cancel, &located, &target, &rows)?;
        let rows: Vec<Vec<u8>> = decoded.into_values().collect();
        let Some(found) =
            txid::find_directory(&rows, txid).map_err(protocol(ProtocolKind::Decode))?
        else {
            return Ok(Attempt::Done(TxidLookup::Absent));
        };
        if found.pages == 0 {
            let record = txid::assemble(&found, &[]).map_err(protocol(ProtocolKind::Decode))?;
            return Ok(Attempt::Done(TxidLookup::Found { record, provenance }));
        }

        // Page phase: exactly `pages` queries, one per page of the extent.
        let page_rows = geometry.page_rows;
        let segments = located.manifest.page_segments.len() as u32;
        let first = u64::from(found.first_page)
            .checked_sub(1)
            .ok_or(TxidError::Protocol(ProtocolKind::Pages))?;
        let end = first + u64::from(found.pages);
        if end > u64::from(segments) * page_rows {
            return Err(TxidError::Protocol(ProtocolKind::Pages));
        }
        let target = self.target(transport, cancel, &located, DisplayTable::Pages, segments)?;
        let pages: Vec<u64> = (first..end).collect();
        let selected: Vec<u64> = pages.iter().map(|page| page % page_rows).collect();
        let decoded = self.queries(transport, cancel, &located, &target, &selected)?;
        let mut rows = Vec::with_capacity(pages.len());
        for page in pages {
            let key = ((page / page_rows) as usize, page % page_rows);
            rows.push(
                decoded
                    .get(&key)
                    .cloned()
                    .ok_or(TxidError::Protocol(ProtocolKind::Pages))?,
            );
        }
        let record: TransparentDisplayRecord =
            txid::assemble(&found, &rows).map_err(protocol(ProtocolKind::Decode))?;
        if record.txid != txid {
            return Err(TxidError::Protocol(ProtocolKind::Decode));
        }
        Ok(Attempt::Done(TxidLookup::Found { record, provenance }))
    }

    /// Init, map, placement and manifest: everything before a query.
    fn locate(
        &mut self,
        transport: &mut impl TxidTransport,
        cancel: &dyn Fn() -> bool,
        txid: Txid,
        height: u64,
        refresh: bool,
    ) -> Result<Result<Located, Attempt>, TxidError> {
        let init = self.init(transport, cancel)?;
        if !init.supported {
            return Ok(Err(Attempt::Done(TxidLookup::Unsupported)));
        }
        let cached = self.map(transport, cancel, refresh)?;
        if cached.map.schema != display::DISPLAY_SCHEMA {
            return Ok(Err(Attempt::Done(TxidLookup::Unsupported)));
        }
        cached
            .map
            .check_shape()
            .map_err(protocol(ProtocolKind::Map))?;
        if height < cached.map.start_height {
            return Ok(Err(Attempt::Done(TxidLookup::PlacementUnknown(
                Placement::Below,
            ))));
        }
        let Some(entry) = cached.map.shard_for_height(height) else {
            // The shape check makes the shards contiguous, so only the tip
            // is left; a map that is not fresh may not have reached it yet.
            return Ok(Err(
                if !refresh && cached.fetched.elapsed() >= ABOVE_REFRESH_AGE {
                    Attempt::RefreshForAbove
                } else {
                    Attempt::Done(TxidLookup::PlacementUnknown(Placement::Above))
                },
            ));
        };
        let entry = entry.clone();
        let Some(geometry) = display_by_name(&entry.geometry) else {
            return Ok(Err(Attempt::Done(TxidLookup::Unsupported)));
        };
        if !init.geometries.contains_key(geometry.name) {
            return Ok(Err(Attempt::Done(TxidLookup::Unsupported)));
        }
        let bucket = display::bucket(&txid, entry.n_buckets);
        let manifest = self.manifest(transport, cancel, &entry)?;
        Ok(Ok(Located {
            init,
            entry,
            map_sha256: cached.sha256.clone(),
            geometry,
            bucket,
            manifest,
        }))
    }

    fn init(
        &mut self,
        transport: &mut impl TxidTransport,
        cancel: &dyn Fn() -> bool,
    ) -> Result<Arc<Init>, TxidError> {
        if let Some(init) = &self.init {
            return Ok(init.clone());
        }
        let reply = send(
            transport,
            cancel,
            TxidRequest::new(Route::Init, Route::Init.template().into(), Vec::new()),
        )?;
        let document: InitDocument =
            serde_json::from_slice(&reply.body).map_err(protocol(ProtocolKind::Init))?;
        let supported = document.schema == display::DISPLAY_SCHEMA
            && document.codec == txid::CODEC
            && document.bucket_domain.as_bytes() == display::BUCKET_DOMAIN
            && document.native_schema == transparent_shard::manifest::SCHEMA;
        let init = Arc::new(Init {
            supported,
            geometries: document
                .geometries
                .into_iter()
                .map(|g| (g.name.clone(), g))
                .collect(),
        });
        self.init = Some(init.clone());
        Ok(init)
    }

    fn map(
        &mut self,
        transport: &mut impl TxidTransport,
        cancel: &dyn Fn() -> bool,
        refresh: bool,
    ) -> Result<Arc<CachedMap>, TxidError> {
        if let (false, Some(cached)) = (refresh, &self.map) {
            return Ok(cached.clone());
        }
        let reply = send(
            transport,
            cancel,
            TxidRequest::new(Route::Map, Route::Map.template().into(), Vec::new()),
        )?;
        let sha256 = hex::encode(Sha256::digest(&reply.body));
        if reply.map_sha256.as_deref().map(str::trim) != Some(sha256.as_str()) {
            return Err(TxidError::Protocol(ProtocolKind::MapHeader));
        }
        let map: DisplayMap =
            serde_json::from_slice(&reply.body).map_err(protocol(ProtocolKind::Map))?;
        if map.to_bytes() != reply.body {
            return Err(TxidError::Protocol(ProtocolKind::MapCanonical));
        }
        // Revisions the new map no longer lists are never asked for again.
        let listed: HashSet<&str> = map
            .shards
            .iter()
            .map(|entry| entry.manifest_digest.as_str())
            .collect();
        self.manifests
            .retain(|digest, _| listed.contains(digest.as_str()));
        self.setups
            .retain(|(digest, _, _), _| listed.contains(digest.as_str()));
        let cached = Arc::new(CachedMap {
            map,
            sha256,
            fetched: Instant::now(),
        });
        self.map = Some(cached.clone());
        Ok(cached)
    }

    fn manifest(
        &mut self,
        transport: &mut impl TxidTransport,
        cancel: &dyn Fn() -> bool,
        entry: &DisplayMapEntry,
    ) -> Result<Arc<DisplayManifest>, TxidError> {
        if let Some(manifest) = self.manifests.get(&entry.manifest_digest) {
            return Ok(manifest.clone());
        }
        let path = format!(
            "/v1/txid/shards/{}/revisions/{}/manifest",
            entry.shard_id, entry.manifest_digest
        );
        let reply = send(
            transport,
            cancel,
            TxidRequest::new(Route::Manifest, path, Vec::new()),
        )?;
        let manifest: DisplayManifest =
            serde_json::from_slice(&reply.body).map_err(protocol(ProtocolKind::Manifest))?;
        if manifest.canonical_bytes() != reply.body || !entry.describes(&manifest) {
            return Err(TxidError::Protocol(ProtocolKind::Manifest));
        }
        manifest
            .validate()
            .map_err(protocol(ProtocolKind::Manifest))?;
        let manifest = Arc::new(manifest);
        self.manifests
            .insert(entry.manifest_digest.clone(), manifest.clone());
        Ok(manifest)
    }

    /// The locally derived profile of `kind` at `geometry`, checked against
    /// what init publishes.
    fn profile(
        &self,
        init: &Init,
        geometry: &'static Geometry,
        kind: DisplayKind,
    ) -> Result<Arc<TableProfile>, TxidError> {
        let cached = self
            .profiles
            .lock()
            .unwrap()
            .get(&(geometry.name, kind))
            .cloned();
        let profile = match cached {
            Some(profile) => profile,
            None => {
                // The native profile is the history kind's: history schema
                // and table name. Only the binding is display-specific.
                let profile = Arc::new(
                    TableProfile::new(
                        transparent_shard::manifest::SCHEMA,
                        geometry.name,
                        kind.as_str(),
                        kind.rows(geometry),
                        kind.row_bytes(geometry),
                    )
                    .map_err(protocol(ProtocolKind::Profile))?,
                );
                self.profiles
                    .lock()
                    .unwrap()
                    .insert((geometry.name, kind), profile.clone());
                profile
            }
        };
        let served = init
            .geometries
            .get(geometry.name)
            .map(|g| match kind {
                DisplayKind::TxPages => &g.txpages,
                DisplayKind::TxDirectory => &g.txdirectory,
            })
            .ok_or(TxidError::Protocol(ProtocolKind::Profile))?;
        if served.scheme != profile.scheme
            || served.rows != kind.rows(geometry)
            || served.row_bytes != kind.row_bytes(geometry)
            || served.setup_seed != display::setup_seed(geometry, kind)
        {
            return Err(TxidError::Protocol(ProtocolKind::Profile));
        }
        Ok(profile)
    }

    #[allow(clippy::too_many_arguments)]
    fn setup(
        &mut self,
        transport: &mut impl TxidTransport,
        cancel: &dyn Fn() -> bool,
        entry: &DisplayMapEntry,
        table: DisplayTable,
        segment: u32,
        segments: u32,
        profile: &TableProfile,
    ) -> Result<Arc<Setup>, TxidError> {
        let digest = &entry.manifest_digest;
        let key = (digest.clone(), table, segment);
        if let Some(setup) = self.setups.get(&key) {
            return Ok(setup.clone());
        }
        let path = format!(
            "/v1/txid/{}/shards/{}/revisions/{digest}/setup/{}/{segment}",
            entry.tier(),
            entry.shard_id,
            table.label()
        );
        let reply = send(
            transport,
            cancel,
            TxidRequest::new(Route::Setup, path, Vec::new()),
        )?;
        let bad = TxidError::Protocol(ProtocolKind::Setup);
        let s: serde_json::Value = serde_json::from_slice(&reply.body).map_err(|_| bad.clone())?;
        let bucket = match table {
            DisplayTable::Directory(b) => serde_json::json!(b),
            DisplayTable::Pages => serde_json::Value::Null,
        };
        if s["manifest_digest"] != *digest
            || s["shard_id"] != entry.shard_id
            || s["table"] != table.label()
            || s["bucket"] != bucket
            || s["segment"] != segment
            || s["segments"] != segments
            || s["geometry"] != entry.geometry
        {
            return Err(bad);
        }
        let public_params = B64
            .decode(s["public_params"].as_str().ok_or_else(|| bad.clone())?)
            .map_err(|_| bad.clone())?;
        if public_params.len() != profile.scheme.public_bytes
            || s["public_params_sha256"] != hex::encode(Sha256::digest(&public_params))
        {
            return Err(bad);
        }
        let epoch: [u8; 8] = hex::decode(
            s["public_params_epoch"]
                .as_str()
                .ok_or_else(|| bad.clone())?,
        )
        .map_err(|_| bad.clone())?
        .try_into()
        .map_err(|_| bad.clone())?;
        let setup = Arc::new(Setup {
            public_params,
            epoch,
        });
        self.setups.insert(key, setup.clone());
        Ok(setup)
    }

    /// The profile and every segment's setup of one table of a located shard.
    fn target(
        &mut self,
        transport: &mut impl TxidTransport,
        cancel: &dyn Fn() -> bool,
        located: &Located,
        table: DisplayTable,
        segments: u32,
    ) -> Result<Target, TxidError> {
        let profile = self.profile(&located.init, located.geometry, table.kind())?;
        let mut setups = Vec::with_capacity(segments as usize);
        for segment in 0..segments {
            setups.push(self.setup(
                transport,
                cancel,
                &located.entry,
                table,
                segment,
                segments,
                &profile,
            )?);
        }
        Ok(Target {
            table,
            profile,
            setups,
        })
    }

    /// One query per entry of `rows`, in order, each against every segment
    /// of the target table. Returns the decoded rows by (segment, row);
    /// coincident rows come back once per query and are kept once.
    fn queries(
        &mut self,
        transport: &mut impl TxidTransport,
        cancel: &dyn Fn() -> bool,
        located: &Located,
        target: &Target,
        rows: &[u64],
    ) -> Result<BTreeMap<(usize, u64), Vec<u8>>, TxidError> {
        let entry = &located.entry;
        let binding = display::query_binding(&entry.manifest_digest, target.table);
        let stride = 16 + target.profile.scheme.response_bytes;
        let path = format!(
            "/v1/txid/{}/shards/{}/revisions/{}/query/{}",
            entry.tier(),
            entry.shard_id,
            entry.manifest_digest,
            target.table.label()
        );
        let mut unique = BTreeMap::new();
        for &row in rows {
            if cancel() {
                return Err(TxidError::Cancelled);
            }
            let (secret, upload) = target
                .profile
                .prepare(row as usize)
                .map_err(protocol(ProtocolKind::Profile))?;
            let mut body = binding.to_vec();
            body.extend(upload);
            let reply = send(
                transport,
                cancel,
                TxidRequest::new(Route::Query, path.clone(), body),
            )?;
            if reply.body.len() != target.setups.len() * stride {
                return Err(TxidError::Protocol(ProtocolKind::Length));
            }
            for (segment, (frame, setup)) in reply
                .body
                .chunks_exact(stride)
                .zip(target.setups.iter())
                .enumerate()
            {
                if frame[..8] != binding {
                    return Err(TxidError::Protocol(ProtocolKind::Binding));
                }
                if frame[8..16] != setup.epoch {
                    return Err(TxidError::Protocol(ProtocolKind::Epoch));
                }
                let decoded = target
                    .profile
                    .decode(&secret, &setup.public_params, &frame[16..])
                    .map_err(protocol(ProtocolKind::Decode))?;
                unique.insert((segment, row), decoded);
            }
        }
        Ok(unique)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Method, TransportError};
    use std::collections::VecDeque;

    /// Replies from a script, recording every request.
    #[derive(Default)]
    struct Scripted {
        replies: VecDeque<Result<TxidReply, TransportError>>,
        sent: Vec<TxidRequest>,
    }

    impl TxidTransport for Scripted {
        fn send(&mut self, request: TxidRequest) -> Result<TxidReply, TransportError> {
            self.sent.push(request);
            self.replies.pop_front().expect("an unscripted request")
        }
    }

    fn reply(status: u16) -> TxidReply {
        TxidReply {
            status,
            ..TxidReply::default()
        }
    }

    fn init(codec: &str) -> TxidReply {
        TxidReply {
            status: 200,
            body: serde_json::json!({
                "schema": display::DISPLAY_SCHEMA,
                "codec": codec,
                "bucket_domain": std::str::from_utf8(display::BUCKET_DOMAIN).unwrap(),
                "native_schema": transparent_shard::manifest::SCHEMA,
                "geometries": [],
            })
            .to_string()
            .into_bytes(),
            ..TxidReply::default()
        }
    }

    fn never() -> bool {
        false
    }

    fn status_of(status: u16, retry_after: Option<&str>) -> TxidError {
        let mut transport = Scripted::default();
        transport.replies.push_back(Ok(TxidReply {
            retry_after: retry_after.map(str::to_string),
            ..reply(status)
        }));
        TxidDisplayClient::new()
            .lookup(&mut transport, [1; 32], 10, &never)
            .unwrap_err()
    }

    #[test]
    fn statuses_map_to_errors_without_retry() {
        assert_eq!(
            status_of(503, Some(" 3 ")),
            TxidError::Unavailable {
                retry_after: Some(Duration::from_secs(3))
            }
        );
        assert_eq!(
            status_of(503, Some("soon")),
            TxidError::Unavailable { retry_after: None }
        );
        assert_eq!(
            status_of(502, None),
            TxidError::Unavailable { retry_after: None }
        );
        for status in [400, 404, 408, 411, 421] {
            assert_eq!(status_of(status, None), TxidError::Refused(status));
        }
        for status in [204, 301] {
            assert_eq!(
                status_of(status, None),
                TxidError::Protocol(ProtocolKind::Status(status))
            );
        }
    }

    #[test]
    fn unsupported_init_sends_nothing_else() {
        let mut transport = Scripted::default();
        transport
            .replies
            .push_back(Ok(init("transparent-txid-display-v9")));
        let mut client = TxidDisplayClient::new();
        assert_eq!(
            client.lookup(&mut transport, [1; 32], 10, &never),
            Ok(TxidLookup::Unsupported)
        );
        // Cached: a second lookup sends nothing at all.
        assert_eq!(
            client.lookup(&mut transport, [2; 32], 10, &never),
            Ok(TxidLookup::Unsupported)
        );
        assert_eq!(transport.sent.len(), 1);
        assert_eq!(transport.sent[0].route, Route::Init);
        assert_eq!(transport.sent[0].method, Method::Get);
        assert_eq!(transport.sent[0].path(), "/v1/txid/init");
        assert!(transport.sent[0].body.is_empty());
    }

    #[test]
    fn malformed_metadata_is_protocol_and_transport_errors_pass_through() {
        let mut transport = Scripted::default();
        transport.replies.push_back(Ok(TxidReply {
            body: b"{".to_vec(),
            ..reply(200)
        }));
        assert_eq!(
            TxidDisplayClient::new().lookup(&mut transport, [1; 32], 10, &never),
            Err(TxidError::Protocol(ProtocolKind::Init))
        );
        let mut transport = Scripted::default();
        transport.replies.push_back(Ok(init(txid::CODEC)));
        let body = b"{}".to_vec();
        transport.replies.push_back(Ok(TxidReply {
            map_sha256: Some(hex::encode(Sha256::digest(&body))),
            body,
            ..reply(200)
        }));
        assert_eq!(
            TxidDisplayClient::new().lookup(&mut transport, [1; 32], 10, &never),
            Err(TxidError::Protocol(ProtocolKind::Map))
        );
        let mut transport = Scripted::default();
        transport
            .replies
            .push_back(Err(TransportError("connection refused".into())));
        assert_eq!(
            TxidDisplayClient::new().lookup(&mut transport, [1; 32], 10, &never),
            Err(TxidError::Transport(TransportError(
                "connection refused".into()
            )))
        );
    }

    #[test]
    fn cancel_is_checked_before_the_first_request() {
        let mut transport = Scripted::default();
        assert_eq!(
            TxidDisplayClient::new().lookup(&mut transport, [1; 32], 10, &|| true),
            Err(TxidError::Cancelled)
        );
        assert!(transport.sent.is_empty());
    }

    #[test]
    fn templates_carry_no_identifiers() {
        for route in Route::ALL {
            let template = route.template();
            assert!(template.starts_with("/v1/txid/"));
            assert!(!template["/v1".len()..].contains(|c: char| c.is_ascii_digit()));
            assert_eq!(route.method() == Method::Post, route == Route::Query);
        }
        let request = TxidRequest::new(Route::Query, "/x".into(), vec![1]);
        assert_eq!(request.content_type(), Some("application/octet-stream"));
        assert_eq!(request.template(), Route::Query.template());
    }
}
