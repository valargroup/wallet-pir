//! Wallet client for the tiered txid display publication
//! (`/v1/txid/`, see `transparent/docs/txid-display.md`).
//!
//! The client is synchronous and has no HTTP stack: the wallet supplies a
//! [`TxidTransport`] that sends one [`TxidRequest`] and returns the raw
//! [`TxidReply`]. The client interprets every status code itself, validates
//! everything the server publishes against its own derivation, and caches
//! native profiles, the init document, the map, manifests and setups.
//!
//! A lookup sends, in order and one at a time: init, map, the shard's
//! manifest and the bucket's directory setups when they are not cached, then
//! exactly two directory queries (even when both candidate rows coincide).
//! A paged record then fetches the pages setups when not cached and sends
//! exactly `pages` page queries. An absent txid sends the same two directory
//! queries. Placement comes from the caller's mined height; nothing is asked
//! to discover it, so placement and support results send no query.
//!
//! Any validation failure is [`TxidError::Protocol`], never
//! [`TxidLookup::Absent`].

mod client;

pub use client::{ProfileCache, TxidDisplayClient};
pub use transparent_shard::txid::{DisplayOutput, TransparentDisplayRecord};

use std::time::Duration;

/// HTTP method of a request.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Method {
    Get,
    Post,
}

impl Method {
    pub fn as_str(self) -> &'static str {
        match self {
            Method::Get => "GET",
            Method::Post => "POST",
        }
    }
}

/// Which route a request addresses.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Route {
    Init,
    Map,
    Manifest,
    Setup,
    Query,
}

impl Route {
    pub const ALL: [Route; 5] = [
        Route::Init,
        Route::Map,
        Route::Manifest,
        Route::Setup,
        Route::Query,
    ];

    /// The route's path template: no shard id, digest, table, segment or
    /// txid, so it can be logged.
    pub fn template(self) -> &'static str {
        match self {
            Route::Init => "/v1/txid/init",
            Route::Map => "/v1/txid/shards",
            Route::Manifest => "/v1/txid/shards/{shard}/revisions/{digest}/manifest",
            Route::Setup => {
                "/v1/txid/{tier}/shards/{shard}/revisions/{digest}/setup/{table}/{segment}"
            }
            Route::Query => "/v1/txid/{tier}/shards/{shard}/revisions/{digest}/query/{table}",
        }
    }

    pub fn method(self) -> Method {
        match self {
            Route::Query => Method::Post,
            _ => Method::Get,
        }
    }
}

/// One request the client asks the transport to send.
///
/// The path is origin-relative (it starts with `/v1/txid/`); the transport
/// prefixes its base URL. A `POST` body is `application/octet-stream` and its
/// length must be declared (`Content-Length`). Neither path nor body carries
/// the txid; the path carries shard id, revision digest, tier and table,
/// which the map already makes public.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TxidRequest {
    pub method: Method,
    pub route: Route,
    path: String,
    pub body: Vec<u8>,
}

impl TxidRequest {
    fn new(route: Route, path: String, body: Vec<u8>) -> Self {
        Self {
            method: route.method(),
            route,
            path,
            body,
        }
    }

    /// The origin-relative path to send.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// The route template, safe to log.
    pub fn template(&self) -> &'static str {
        self.route.template()
    }

    /// The `Content-Type` a body must be sent with, if the request has one.
    pub fn content_type(&self) -> Option<&'static str> {
        (self.method == Method::Post).then_some("application/octet-stream")
    }
}

/// What came back, uninterpreted: the client, not the transport, decides
/// what a status means.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TxidReply {
    pub status: u16,
    /// The raw `Retry-After` header, if any.
    pub retry_after: Option<String>,
    /// The raw `X-Txid-Map-Sha256` header, if any.
    pub map_sha256: Option<String>,
    pub body: Vec<u8>,
}

/// The transport could not complete the exchange (connect, TLS, timeout,
/// truncated body). The message must not contain the txid; it never sees one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransportError(pub String);

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "transport: {}", self.0)
    }
}

impl std::error::Error for TransportError {}

/// Sends one request and returns the reply, whatever its status.
pub trait TxidTransport {
    fn send(&mut self, request: TxidRequest) -> Result<TxidReply, TransportError>;
}

impl<T: TxidTransport + ?Sized> TxidTransport for &mut T {
    fn send(&mut self, request: TxidRequest) -> Result<TxidReply, TransportError> {
        (**self).send(request)
    }
}

impl<T: TxidTransport + ?Sized> TxidTransport for Box<T> {
    fn send(&mut self, request: TxidRequest) -> Result<TxidReply, TransportError> {
        (**self).send(request)
    }
}

/// The publication tier of the shard a lookup was answered from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Tier {
    /// A sealed, content-pure archive shard.
    Archive,
    /// The unsealed recent shard, rebuilt as blocks arrive.
    Recent,
}

impl Tier {
    pub fn as_str(self) -> &'static str {
        match self {
            Tier::Archive => "archive",
            Tier::Recent => "recent",
        }
    }
}

/// Which publication answered a lookup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Provenance {
    /// SHA-256 of the map the lookup used, hex.
    pub map_sha256: String,
    pub shard_id: u64,
    pub revision: u32,
    /// Digest of the shard revision's manifest, hex.
    pub manifest_digest: String,
    pub tier: Tier,
}

/// Where a mined height lies relative to the published shards.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Placement {
    /// Before the first height the publication serves.
    Below,
    /// After the last height the publication covers, even after a map
    /// refresh.
    Above,
}

/// The outcome of a lookup that completed.
#[derive(Clone, Debug, PartialEq)]
pub enum TxidLookup {
    Found {
        record: TransparentDisplayRecord,
        provenance: Provenance,
    },
    /// The shard covering the height was queried and does not hold the txid.
    Absent,
    /// No shard covers the height; nothing was queried.
    PlacementUnknown(Placement),
    /// The server publishes a schema, codec or geometry this client does not
    /// know; nothing was queried.
    Unsupported,
}

/// What failed validation. Never a reason to treat a txid as absent.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ProtocolKind {
    /// The init document does not parse.
    Init,
    /// The map does not parse or is malformed.
    Map,
    /// The map is not in its canonical encoding.
    MapCanonical,
    /// The `X-Txid-Map-Sha256` header is missing or disagrees with the map.
    MapHeader,
    /// The manifest is not canonical, not the map entry's, or invalid.
    Manifest,
    /// The served native parameters differ from the local derivation.
    Profile,
    /// A setup's identity, length or digest is wrong.
    Setup,
    /// A response frame's query binding is not the request's.
    Binding,
    /// A response frame's epoch is not its setup's.
    Epoch,
    /// A response body has the wrong length.
    Length,
    /// A row, entry or record does not decode, or decodes to another txid.
    Decode,
    /// A directory entry names pages outside the shard.
    Pages,
    /// An unexpected status code (not 200 nor one with defined semantics).
    Status(u16),
}

/// Why a lookup did not complete.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TxidError {
    /// The service is overloaded or unavailable (503, and 429/5xx). Not
    /// retried internally; `retry_after` is the server's hint.
    Unavailable {
        retry_after: Option<Duration>,
    },
    /// The revision stayed unserved after one map refresh and retry (409).
    Stale,
    /// The server refused the request (400, 408, 411, 421 or another 4xx).
    /// Not retried.
    Refused(u16),
    /// Something the server published or returned failed validation.
    Protocol(ProtocolKind),
    Transport(TransportError),
    /// The caller's cancel predicate returned true before a request.
    Cancelled,
}

impl std::fmt::Display for TxidError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TxidError::Unavailable { retry_after } => {
                write!(f, "txid service unavailable (retry after {retry_after:?})")
            }
            TxidError::Stale => write!(f, "txid revision stale after a map refresh"),
            TxidError::Refused(status) => write!(f, "txid request refused with {status}"),
            TxidError::Protocol(kind) => write!(f, "txid protocol violation: {kind:?}"),
            TxidError::Transport(error) => write!(f, "{error}"),
            TxidError::Cancelled => write!(f, "txid lookup cancelled"),
        }
    }
}

impl std::error::Error for TxidError {}

impl From<TransportError> for TxidError {
    fn from(error: TransportError) -> Self {
        TxidError::Transport(error)
    }
}
