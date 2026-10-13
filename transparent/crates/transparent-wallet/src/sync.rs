//! Birthday to balance: the join between public filters and private retrieval.
//!
//! This is the path the whole design exists to make cheap:
//!
//! 1. fetch the published shard map and binary-search it for the birthday;
//! 2. download **every** filter from that shard forward and match locally;
//! 3. for each matched script and directory shard, privately retrieve its
//!    directory row, then walk the chain of runs its entry points to, newest
//!    first, through the shard's page shards (see `walk_chains`);
//! 4. replay the events into a UTXO set.
//!
//! Step 2 downloads every filter in range, not only the ones that will match.
//! Which filters a wallet asks for must not be a function of its scripts.
//!
//! Step 3 addresses a row over the shard's whole logical row space, which is
//! its segments concatenated. Every segment answers, and the row is identified
//! by its exact script bytes rather than by which segment returned it: asking
//! only the segment that holds the script would disclose part of the script.
//! Coverage advances for a shard only once every segment has been processed.
//!
//! # Settled and provisional coverage
//!
//! The last shard in the map may be a provisional tail revision, which will be
//! superseded as the chain advances. Coverage taken from one is reported
//! separately from settled coverage, with the revision's digest, so a caller
//! can re-derive that range instead of merging a later revision into it.
//!
//! # What the checks here establish
//!
//! The first profile trusts the publisher and the retrieval service to provide
//! correct, complete, mutually consistent data for the declared anchor. What
//! this code checks — filter digests against the map, exact script bytes
//! against the row, a page header against the entry that located it, the event
//! total against what the pages yielded — detects corruption, stale data and
//! accidentally mixed shards. None of it establishes that the index matches the
//! chain: a service that returns a consistent lie passes every one of them.
//!
//! # What this leaks, stated plainly
//!
//! Step 3 queries only the shards that matched, so the service learns which
//! chain ranges the wallet had probable activity in. That is the leak the
//! design accepts, and it is real: with content-sealed shards a busy period is
//! covered by a narrower shard, so the localization is sharper during busy
//! periods than quiet ones. This code does not hide it and must not be
//! described as if it did.

use crate::adapters::{Acceptance, ChainView, ScriptProvider, StaticScripts};
use crate::client::{classify_transport, ClientError, Table, TableClient};
use crate::ledger::{Ledger, LedgerError};
use crate::memory_store::MemoryStore;
use crate::store::{
    uncovered, Anchor, CachedRun, CoverageKind, CoverageRange, PendingPages, RunCursor,
    ScriptEntry, ScriptOrigin, SetIdentity, SetupBlob, SetupKey, ShardCommit, StoreError,
    StoredEvent, WalletStore,
};
use crate::transport::ShardRequest;
use crate::transport::{ByteCharges, FilterSource, ShardTransport, StaleRevision};
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::time::Duration;
use transparent_events::TransparentEvent;
use transparent_filter::{BlockHash, FilterLimits, ScriptBytes, ShardKey, ShardMap};
use transparent_shard::build::candidate_rows;
use transparent_shard::manifest::{PageChild, ShardManifest};
use transparent_shard::page_row::PageEntry;
use transparent_shard::records::DirectoryEntry;

#[path = "sync_ahead.rs"]
mod ahead;

#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    #[error("client: {0}")]
    Client(#[from] ClientError),
    #[error("ledger: {0}")]
    Ledger(#[from] LedgerError),
    #[error("filter: {0}")]
    Filter(#[from] transparent_filter::FilterError),
    #[error("record: {0}")]
    Record(#[from] transparent_shard::records::RecordError),
    #[error("transport: {0}")]
    Transport(String),
    #[error("store: {0}")]
    Store(#[from] StoreError),
    /// The store's accepted anchor lies above the offered map's end, and the
    /// wallet's chain still accepts it. A lower anchor is never accepted
    /// silently.
    #[error("the store's anchor {stored} is above the map's end {offered}, which the chain still accepts")]
    AnchorRegressed { stored: u64, offered: u64 },
    #[error("{0}")]
    Invalid(String),
    /// A row in a private reply failed on content: it did not decode, a
    /// record or link in it was not what the shard declares, or it
    /// contradicts what the store holds. A reply failing as a whole (its
    /// length, revision binding or parameter epoch) is a request failure
    /// instead, [`SyncError::Client`]: the service makes those without
    /// knowing the row.
    ///
    /// A sync does not stop for one: it fails the directory entry the reply
    /// was for, finishes everything else, counts the failure in the store
    /// and reports [`IncompleteReason::ContentFailed`]; see
    /// [`retrieve_shard_into`] and [`walk_chains`]. It reaches a caller from
    /// the one-shot [`sync`], which has no store to count it in, and when a
    /// contradiction cannot be laid at one entry's door.
    #[error("content: {0}")]
    Content(String),
    #[error("service serves schema {served}, this build reads {expected}")]
    Schema {
        served: String,
        expected: &'static str,
    },
    /// A shard names a geometry this build has no parameters for.
    ///
    /// Fatal for the whole sync rather than a reason to skip the shard. Skipping
    /// would advance coverage past a range whose history was never retrieved,
    /// and the wallet would report a synchronised balance it has not earned.
    #[error(
        "shard set uses geometry {0}, which this build does not know; upgrade before syncing this \
         range"
    )]
    UnknownGeometry(String),
    /// The map names a range-filter profile this build does not know.
    ///
    /// Its filters would be decoded under the wrong Golomb-Rice parameters,
    /// which neither fails reliably nor matches correctly, so the sync stops
    /// before matching anything.
    #[error(
        "shard set uses range-filter profile {0}, which this build does not know; upgrade \
         before syncing this range"
    )]
    UnknownProfile(String),
    /// A revision this sync needed was withdrawn, and refreshing the map did
    /// not offer one that is served.
    ///
    /// Terminal, and coverage stopped where it was. Distinct from a transport
    /// failure so a caller can re-validate a map against its accepted chain and
    /// come back, rather than treating a routine republication as a broken set.
    #[error(
        "shard {shard_id}: {stale}; the map was refreshed {refreshes} time(s) without offering a \
         live revision"
    )]
    StaleRevision {
        shard_id: u64,
        stale: StaleRevision,
        refreshes: u32,
    },
    /// The service stayed at its capacity limit for the whole retry budget.
    #[error("shard {shard_id}: the service had no free cache capacity after {attempts} attempts")]
    Overloaded { shard_id: u64, attempts: u32 },
    /// A shard's manifest does not describe the shard the map named.
    ///
    /// Terminal for the sync, and coverage stops where it was. Every field a
    /// retrieval relies on — geometry, range, segment shape, chain link — is
    /// checked against the map before a single private request, so a
    /// publication that is internally inconsistent is refused rather than read
    /// under whichever of its two stories the wallet happened to see first.
    #[error("shard {shard_id}: manifest {field} does not agree with the map")]
    ManifestMismatch { shard_id: u64, field: &'static str },
    /// A refreshed map is not the same shard set, continued.
    ///
    /// The caller validated the map this sync started from against the wallet's
    /// accepted chain. A map fetched mid-sync inherits that validation only over
    /// the range the two agree on, so a disagreement about already-covered
    /// history is a different set rather than a longer one, and the caller has
    /// to re-validate before retrying.
    #[error(
        "the refreshed shard map is not a continuation of the one this sync started from: {0}"
    )]
    MapDiverged(String),
    /// The map contradicts sealed history the store holds on the wallet's own
    /// chain, and declares no re-cut of it.
    ///
    /// Sealed content is immutable. A sealed range described differently is a
    /// reorg when the chain has moved, and is rolled back as one. This is the
    /// other case: the chain accepts the block the stored range rests on and
    /// also the block the map's sealed shard now covering its start ends on,
    /// at or below the target. Both are on the chain the wallet follows, so
    /// nothing explains the change but the publisher, and retrying does not
    /// resolve it while the publisher serves this history. When the chain
    /// does not yet accept the replacement's end block, as when the publisher
    /// followed a shallow reorg through a shard it had just sealed before the
    /// wallet's chain did, the sync stops with
    /// [`IncompleteReason::ChainUnknown`] instead and becomes an ordinary reorg
    /// once the chain moves. Refused before anything is read, and after
    /// rolling back only a reorg the wallet's chain itself shows, which runs
    /// first so that no map can hold it back. Distinct from
    /// [`MapDiverged`](Self::MapDiverged), which a re-cut published mid-sync
    /// also produces and which the next sync resolves.
    ///
    /// A replica still serving a map from before a re-cut the store has
    /// already followed looks the same from here, since the store keeps no
    /// re-cut epoch: an embedding that records the epoch it last synced at
    /// can tell the two apart before calling the sync.
    #[error(
        "the map replaces the sealed shard the store holds from height {start_height} (revision \
         {revision_digest}) with another the wallet's chain also accepts, and declares no re-cut \
         of it"
    )]
    SealedRewrite {
        start_height: u64,
        revision_digest: String,
    },
}

/// Map refreshes one sync will spend recovering a withdrawn revision.
///
/// A tail is republished on a block cadence, so a wallet lapped this many times
/// while reading is not converging on the tip, and saying so beats re-reading a
/// moving target indefinitely. Every refresh must also *change* the refused
/// revision, so this bounds genuine republication rather than a loop.
const MAX_MAP_REFRESHES: u32 = 4;

/// Attempts one shard gets against a service that is out of cache capacity.
const MAX_OVERLOAD_ATTEMPTS: u32 = 4;

/// Wait before a second attempt when the service names no delay, doubled per
/// attempt after that.
const OVERLOAD_BACKOFF: Duration = Duration::from_millis(250);

/// Ceiling on any single wait before jitter, so a service asking for an
/// implausible delay cannot park a sync inside it.
const OVERLOAD_BACKOFF_CAP: Duration = Duration::from_secs(2);

/// Passes a sync makes while the wallet's rules keep adding scripts. A
/// gap-limit advance that never converges is a wallet fault; this stops it
/// from becoming an unbounded sync.
const MAX_DISCOVERY_PASSES: u32 = 16;

/// Content failures after which a directory entry is no longer queried.
///
/// A service can aim a bad reply at chosen rows and watch what the wallet
/// does next. Each failure changes later traffic once (the entry is asked
/// again, then not), so this is how many such observations an entry gives
/// away; a service could deny the entry service anyway by corrupting it.
pub const MAX_CONTENT_FAILURES: u32 = 3;

/// One geometry's parameters, as a service declares them.
///
/// The dimensions come with the schemes so a wallet can check the pair rather
/// than adopt either. A service naming a registry geometry but publishing other
/// row counts under it is refused; so is one publishing the registry's row
/// counts with a scheme they do not derive.
pub struct GeometryParams {
    pub name: String,
    pub directory_rows: u64,
    pub directory_row_bytes: u32,
    pub directory_scheme: transparent_native::NativeScheme,
    /// The 44-bit dithered directory scheme, when the service advertises one.
    pub directory_scheme_dq44: Option<transparent_native::NativeScheme>,
    pub directory_setup_seed: u64,
    pub page_rows: u64,
    pub page_row_bytes: u32,
    pub pages_scheme: transparent_native::NativeScheme,
    /// The 44-bit dithered page scheme, when the service advertises one.
    pub pages_scheme_dq44: Option<transparent_native::NativeScheme>,
    pub pages_setup_seed: u64,
}

/// The geometries a service declares, checked against this build's registry.
pub struct ServiceGeometry {
    /// The schema the service serves, from its init response.
    ///
    /// A row's meaning is entirely a function of its schema, and the bytes
    /// carry no version of their own — a fixed-width row has nowhere to put
    /// one without spending space on every row. So a wallet that decoded
    /// whatever it was handed would read a newer layout as the one it knows
    /// and reconstruct a plausible, wrong history. This is where that stops.
    pub schema: String,
    /// One entry per geometry the service holds. A set spanning an archive tier
    /// and a recent tier declares two.
    pub geometries: Vec<GeometryParams>,
}

/// The prepared clients for one geometry: one per table.
struct GeometryClients {
    directory: TableClient,
    pages: TableClient,
}

/// Every geometry a sync has prepared, by registry name.
///
/// Prepared on first use rather than up front, so a wallet syncing only recent
/// history never pays to derive archive parameters. Once prepared, a geometry's
/// clients are reused for every shard naming it — which is the saving that
/// naming geometries from a closed registry exists to buy.
struct Clients {
    schema: String,
    prepared: HashMap<String, GeometryClients>,
}

impl Clients {
    /// Prepares the clients for `name`, checking the service's declaration
    /// against this build's registry and re-deriving the scheme.
    ///
    /// Three things have to agree: the name must be one this build knows, the
    /// dimensions the service published under it must be the registry's, and
    /// the scheme must be what those dimensions derive. A service that could
    /// move any one of them independently could choose a geometry for the
    /// wallet, and choosing the geometry is choosing what the response reveals.
    fn prepare(
        &mut self,
        name: &str,
        declared: &[GeometryParams],
    ) -> Result<&mut GeometryClients, SyncError> {
        if !self.prepared.contains_key(name) {
            let geometry = transparent_shard::layout::by_name(name)
                .ok_or_else(|| SyncError::UnknownGeometry(name.to_string()))?;
            let params = declared
                .iter()
                .find(|params| params.name == name)
                .ok_or_else(|| {
                    SyncError::Invalid(format!(
                        "the map uses geometry {name} but the service declares no parameters for it"
                    ))
                })?;
            if params.directory_rows != geometry.directory_rows
                || params.directory_row_bytes != geometry.directory_row_bytes as u32
                || params.page_rows != geometry.page_rows
                || params.page_row_bytes != geometry.page_row_bytes as u32
            {
                return Err(SyncError::Invalid(format!(
                    "the service declares geometry {name} with dimensions this build does not \
                     know it by"
                )));
            }
            let clients = GeometryClients {
                directory: TableClient::new_with_schema(
                    &self.schema,
                    Table::Directory,
                    name,
                    geometry.directory_rows,
                    geometry.directory_row_bytes as u32,
                    &params.directory_scheme,
                    params.directory_scheme_dq44.as_ref(),
                )?,
                pages: TableClient::new_with_schema(
                    &self.schema,
                    Table::Pages(0),
                    name,
                    geometry.page_rows,
                    geometry.page_row_bytes as u32,
                    &params.pages_scheme,
                    params.pages_scheme_dq44.as_ref(),
                )?,
            };
            self.prepared.insert(name.to_string(), clients);
        }
        Ok(self.prepared.get_mut(name).expect("just prepared"))
    }
}

/// What one sync recovered.
pub struct SyncOutcome {
    pub ledger: Ledger,
    pub charges: ByteCharges,
    /// Shards where at least one script matched. Ascending.
    pub matched_shards: Vec<u64>,
    /// Matches that turned out to hold no directory entry: the filter's false
    /// positives, plus any script outside the private tables' coverage.
    pub unproductive_matches: u64,
    /// Height through which coverage is complete, including any provisional
    /// tail revision this sync used.
    pub covered_through: u64,
    /// Height through which coverage came from sealed shards alone.
    ///
    /// Equal to `covered_through` when the sync used no provisional revision.
    /// A balance beyond this height is current but not settled.
    pub settled_through: u64,
    /// The provisional revisions this sync took coverage from: shard id,
    /// revision number and the manifest digest that identifies the revision.
    ///
    /// A caller records these with the coverage. When a later revision or the
    /// sealed shard appears, the range they covered is re-derived rather than
    /// extended, because a revision replaces its predecessor.
    pub provisional: Vec<ProvisionalCoverage>,
    /// Times this sync refetched the map to recover a withdrawn revision.
    ///
    /// Non-zero means part of the range was re-derived under a later revision.
    /// The abandoned attempt's bytes are still counted in `charges`, so a
    /// recovered sync legitimately reports more filter and setup bytes than a
    /// clean one over the same range.
    pub map_refreshes: u32,
}

/// One provisional revision a sync took coverage from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProvisionalCoverage {
    pub shard_id: u64,
    pub revision: u32,
    pub manifest_digest: String,
    pub end_height: u64,
}

/// Bounds on one call to [`sync_into`]. `None` is unbounded.
///
/// Reaching a bound is not an error: the sync commits what it has, records
/// the page work still owed, and reports `Incomplete`. The next call resumes
/// from the durable pending work.
#[derive(Clone, Copy, Debug, Default)]
pub struct WorkLimits {
    /// Private queries, directory and pages together.
    pub max_queries: Option<u64>,
    /// Private bytes: setup, query uploads and responses.
    pub max_private_bytes: Option<u64>,
}

impl WorkLimits {
    pub const UNLIMITED: Self = Self {
        max_queries: None,
        max_private_bytes: None,
    };
}

/// Why a sync stopped short.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IncompleteReason {
    QueryBudget,
    ByteBudget,
    /// The store would not hold more pending page work.
    PendingLimit,
    /// The service stayed at capacity for the whole retry budget on this shard.
    Overloaded {
        shard_id: u64,
    },
    /// The wallet's chain view could not confirm a block its coverage rests
    /// on, so neither a reorg nor its absence could be established.
    ChainUnknown {
        height: u64,
    },
    /// The wallet's rules kept adding scripts past the pass bound.
    DiscoveryUnbounded,
    /// Coverage stops short at directory entries whose replies failed on
    /// content; [`SyncReport::failed_entries`] lists them. Everything else
    /// was read and committed.
    ContentFailed,
    PublicationBehind {
        height: u64,
    },
    UnresolvedSpends,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Completion {
    Complete,
    Incomplete {
        reason: IncompleteReason,
        /// Pending page retrievals left in the store.
        pending: usize,
    },
}

/// What one [`sync_into`] call did.
pub struct SyncReport {
    /// The store's whole ledger after this sync, not only what it added.
    pub ledger: Ledger,
    pub charges: ByteCharges,
    pub matched_shards: Vec<u64>,
    pub unproductive_matches: u64,
    /// Height through which every script in scope is covered from its
    /// required height, provisional coverage included.
    pub covered_through: u64,
    /// The same counting settled coverage only.
    pub settled_through: u64,
    pub provisional: Vec<ProvisionalCoverage>,
    pub map_refreshes: u32,
    pub completion: Completion,
    /// The height everything above was rolled back to, if this sync found a
    /// reorg or a replaced provisional tail.
    pub rolled_back_to: Option<u64>,
    /// Provisional revision digests whose coverage was truncated and
    /// re-derived.
    pub replaced_revisions: Vec<String>,
    /// Scripts the wallet's rules added during this sync.
    pub scripts_added: usize,
    /// Store commits this sync made.
    pub commits: u64,
    /// Directory entries this sync failed on content or held back because
    /// they had failed [`MAX_CONTENT_FAILURES`] times, by shard and script.
    pub failed_entries: Vec<FailedEntry>,
}

/// One script's directory entry in one shard that failed on content.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FailedEntry {
    pub shard_id: u64,
    pub script: Vec<u8>,
    /// Content failures the store holds for the entry, this sync's included.
    /// At [`MAX_CONTENT_FAILURES`] the entry is no longer queried.
    pub failures: u32,
}

/// Runs one sync from `birthday` to the explicit wallet-accepted target.
///
/// `scripts` is the wallet's derived script set. It never leaves the machine:
/// matching is local, and a private query names a row, not a script.
///
/// One-shot: a fresh in-memory store, every script required from `birthday`,
/// no work limits, using the caller's accepted chain view. A returning
/// wallet uses [`sync_into`] with its own store.
#[allow(clippy::too_many_arguments)]
pub fn sync(
    map: &ShardMap,
    map_bytes: u64,
    geometry: &ServiceGeometry,
    filters: &mut impl FilterSource,
    transport: &mut impl ShardTransport,
    scripts: &[ScriptBytes],
    birthday: u64,
    chain: &impl ChainView,
    target_anchor: &Anchor,
) -> Result<SyncOutcome, SyncError> {
    let mut store = MemoryStore::new();
    let mut provider = StaticScripts(
        scripts
            .iter()
            .map(|script| ScriptEntry {
                script: script.as_slice().to_vec(),
                origin: ScriptOrigin::Derived,
                required_from: birthday,
            })
            .collect(),
    );
    let report = sync_into(
        &mut store,
        map,
        map_bytes,
        geometry,
        chain,
        &mut provider,
        filters,
        transport,
        &WorkLimits::UNLIMITED,
        target_anchor,
    )?;
    match report.completion {
        Completion::Complete => Ok(SyncOutcome {
            ledger: report.ledger,
            charges: report.charges,
            matched_shards: report.matched_shards,
            unproductive_matches: report.unproductive_matches,
            covered_through: report.covered_through,
            settled_through: report.settled_through,
            provisional: report.provisional,
            map_refreshes: report.map_refreshes,
        }),
        // Without a store to keep it, incomplete work is a failure.
        Completion::Incomplete {
            reason: IncompleteReason::Overloaded { shard_id },
            ..
        } => Err(SyncError::Overloaded {
            shard_id,
            attempts: MAX_OVERLOAD_ATTEMPTS,
        }),
        Completion::Incomplete {
            reason: IncompleteReason::ContentFailed,
            ..
        } => Err(SyncError::Content(format!(
            "replies failed for {} directory entries",
            report.failed_entries.len()
        ))),
        Completion::Incomplete { reason, .. } => Err(SyncError::Invalid(format!(
            "the sync stopped short: {reason:?}"
        ))),
    }
}

/// Syncs a wallet's store to its accepted target, independently of publication tip.
///
/// See the module documentation for the shape of a sync. The store is the
/// wallet's memory: what it holds decides what is fetched, and what is
/// fetched is committed to it shard by shard. On return the store is
/// consistent whether the sync completed or not.
#[allow(clippy::too_many_arguments)]
pub fn sync_into<S: WalletStore>(
    store: &mut S,
    map: &ShardMap,
    map_bytes: u64,
    geometry: &ServiceGeometry,
    chain: &impl ChainView,
    scripts: &mut impl ScriptProvider,
    filters: &mut impl FilterSource,
    transport: &mut impl ShardTransport,
    limits: &WorkLimits,
    target_anchor: &Anchor,
) -> Result<SyncReport, SyncError> {
    if !transparent_shard::manifest::supported_schema(&geometry.schema) {
        return Err(SyncError::Schema {
            served: geometry.schema.clone(),
            expected: transparent_shard::SCHEMA,
        });
    }
    map.check_shape()
        .map_err(|error| SyncError::Invalid(format!("shard map is malformed: {error}")))?;
    if transparent_filter::range_profile(&map.profile).is_none() {
        return Err(SyncError::UnknownProfile(map.profile.clone()));
    }
    let genesis = BlockHash::from_display_hex(&map.genesis_hash)?;
    BlockHash::from_display_hex(&target_anchor.hash)?;
    if target_anchor.height < map.start_height {
        return Err(SyncError::Invalid(
            "target precedes publication start".into(),
        ));
    }
    match chain.is_accepted(target_anchor.height, &target_anchor.hash) {
        Acceptance::Rejected => {
            return Err(SyncError::Invalid(
                "target anchor rejected by wallet chain".into(),
            ))
        }
        Acceptance::Unknown => {
            return incomplete_at(
                store,
                IncompleteReason::ChainUnknown {
                    height: target_anchor.height,
                },
            )
        }
        Acceptance::Accepted => {}
    }
    if map
        .shards
        .last()
        .is_none_or(|s| s.end_height < target_anchor.height)
    {
        return incomplete_at(
            store,
            IncompleteReason::PublicationBehind {
                height: target_anchor.height,
            },
        );
    }
    check_target_hash(map, target_anchor)?;
    let retained = store
        .events()?
        .iter()
        .map(|e| event_height(&e.event))
        .max()
        .unwrap_or(0);
    if let Some(a) = store.anchor()? {
        if a.height > target_anchor.height
            && chain.is_accepted(a.height, &a.hash) == Acceptance::Accepted
        {
            return Err(SyncError::AnchorRegressed {
                stored: a.height,
                offered: target_anchor.height,
            });
        }
    }
    if retained > target_anchor.height {
        return Err(SyncError::Invalid(
            "target below retained events; explicitly roll back to an accepted anchor first".into(),
        ));
    }

    // The store is bound to a publication lineage. A map from another
    // lineage is refused before anything is read from it.
    let identity = SetIdentity::of_schema(map, &geometry.schema);
    store.bind_set(&identity).map_err(|error| match error {
        StoreError::SetMismatch { stored, offered } => SyncError::MapDiverged(format!(
            "the store is bound to set {stored}, this map is {offered}"
        )),
        other => SyncError::Store(other),
    })?;
    let first_commit = store.last_commit()?;

    let Some(tip_entry) = map.shards.last() else {
        return Err(SyncError::Invalid("the map names no shards".into()));
    };
    let through = target_anchor.height;
    let _ = tip_entry;

    // A map that ends below the anchor the wallet accepted is not a newer
    // publication; if the wallet's chain still accepts that anchor, the
    // map is refused rather than silently rewound.
    if let Some(anchor) = store.anchor()? {
        if anchor.height > through
            && chain.is_accepted(anchor.height, &anchor.hash) == Acceptance::Accepted
        {
            return Err(SyncError::AnchorRegressed {
                stored: anchor.height,
                offered: through,
            });
        }
    }

    let mut charges = ByteCharges {
        map_bytes,
        ..Default::default()
    };
    let mut rolled_back_to: Option<u64> = None;
    let mut replaced_revisions: Vec<String> = Vec::new();

    // Every revision the map's re-cuts declare, by digest, looked up once.
    let declared = map.superseded_index();

    // Reorg detection over what the store holds. Every distinct block the
    // coverage rests on is asked of the wallet's chain, newest first; the
    // first rejected one rolls everything above the highest accepted block
    // below it back.
    let mut rests_on: BTreeMap<u64, String> = BTreeMap::new();
    // Settled ranges the map neither still publishes nor declares superseded.
    let mut rewritten: Vec<CoverageRange> = Vec::new();
    for entry in store.scripts()? {
        for range in store.coverage(&entry.script)? {
            rests_on
                .entry(range.end_height)
                .or_insert(range.terminal_block_hash.clone());
            if range.kind == CoverageKind::Settled && !vouches_for(map, &declared, &range) {
                rewritten.push(range);
            }
        }
    }
    let mut ancestor: Option<u64> = None;
    let mut unknown: Option<u64> = None;
    for (height, hash) in rests_on.iter().rev() {
        match chain.is_accepted(*height, hash) {
            Acceptance::Rejected => {
                // Roll back to the highest accepted block below this one.
                let mut below = map.start_height.saturating_sub(1);
                for (candidate, candidate_hash) in rests_on.range(..height).rev() {
                    if chain.is_accepted(*candidate, candidate_hash) == Acceptance::Accepted {
                        below = *candidate;
                        break;
                    }
                }
                ancestor = Some(below);
                break;
            }
            Acceptance::Accepted => break,
            Acceptance::Unknown => {
                unknown = Some(*height);
            }
        }
    }
    if let Some(height) = ancestor {
        store.rollback_above(&accepted_at(chain, map, height)?, "reorg")?;
        rolled_back_to = Some(height);
    } else if let Some(height) = unknown {
        if !rests_on.is_empty() {
            // Nothing could be confirmed. Reading on would either accept a
            // branch the wallet has not seen or roll back on a guess.
            return chain_unknown(
                store,
                map,
                target_anchor,
                charges,
                height,
                rolled_back_to,
                replaced_revisions,
                first_commit,
            );
        }
    }

    // Sealed content is immutable, so a sealed range the map now describes
    // differently, without declaring a re-cut of it, is judged by what the
    // wallet's chain says; see `judge_rewrite`. Only after the scan above,
    // which depends on the chain alone, has rolled back any reorg the chain
    // shows, and only for the ranges that rollback kept: a map cannot hold
    // back a reorg by changing history below it. A contradiction on the
    // wallet's own chain is refused before anything else is rolled back or
    // read; anything the chain cannot settle yet stops the sync as an
    // unknown block does.
    let mut contradiction: Option<&CoverageRange> = None;
    let mut unplaced: Option<u64> = None;
    for range in rewritten
        .iter()
        .filter(|range| ancestor.is_none_or(|kept| range.end_height <= kept))
    {
        match judge_rewrite(map, range, target_anchor, chain) {
            Rewrite::Reorg => {}
            Rewrite::Contradiction => {
                if contradiction.is_none_or(|held| held.start_height > range.start_height) {
                    contradiction = Some(range);
                }
            }
            Rewrite::Unsettled(height) => {
                unplaced = Some(unplaced.map_or(height, |held| held.max(height)));
            }
        }
    }
    if let Some(range) = contradiction {
        return Err(SyncError::SealedRewrite {
            start_height: range.start_height,
            revision_digest: range.revision_digest.clone(),
        });
    }
    if let Some(height) = unplaced {
        return chain_unknown(
            store,
            map,
            target_anchor,
            charges,
            height,
            rolled_back_to,
            replaced_revisions,
            first_commit,
        );
    }

    // Provisional reconciliation. A tail revision the map has moved past is
    // truncated and re-derived; one the map has sealed is promoted.
    let mut truncate_from: Option<u64> = None;
    let mut promote: Vec<(u64, String)> = Vec::new();
    // A partially retrieved revision may have events and pending pages but no
    // completed coverage yet. Unfinished work under a revision the map no
    // longer publishes is dropped, and what it saved is rolled back from
    // where the revision began, never from wherever the map now puts its
    // shard id: a re-cut gives that id to another range.
    let mut dropped: Vec<u64> = Vec::new();
    let mut saved: Option<Vec<StoredEvent>> = None;
    for pending in store.pending()? {
        if map
            .shards
            .get(pending.shard_id as usize)
            .is_some_and(|published| published.manifest_digest == pending.revision_digest)
        {
            continue;
        }
        if saved.is_none() {
            saved = Some(store.events()?);
        }
        let lowest_saved = saved
            .iter()
            .flatten()
            .filter(|stored| stored.revision_digest == pending.revision_digest)
            .map(|stored| event_height(&stored.event))
            .min();
        let declared = declared.get(pending.revision_digest.as_str()).copied();
        match unfinished(&pending, declared, lowest_saved, chain) {
            Unfinished::Rewind(height) => {
                truncate_from = Some(truncate_from.map_or(height, |held| held.min(height)));
            }
            Unfinished::Drop => dropped.extend(pending.id),
        }
        if !replaced_revisions.contains(&pending.revision_digest) {
            replaced_revisions.push(pending.revision_digest);
        }
    }
    for range in store.provisional()? {
        match map.shards.get(range.shard_id as usize) {
            Some(published) if published.manifest_digest == range.revision_digest => {
                if published.sealed {
                    promote.push((range.shard_id, range.revision_digest.clone()));
                }
            }
            _ => {
                truncate_from = Some(
                    truncate_from
                        .map_or(range.start_height, |held: u64| held.min(range.start_height)),
                );
                if !replaced_revisions.contains(&range.revision_digest) {
                    replaced_revisions.push(range.revision_digest.clone());
                }
            }
        }
    }
    if let Some(start) = truncate_from {
        // From the start of the shard now covering that height: at or below
        // everything the replaced revision saved, and a boundary the map names
        // a block for, so the rollback never depends on the wallet's chain
        // holding a hash inside a shard.
        let start = map
            .shard_for_height(start)
            .map_or(start, |entry| entry.start_height.min(start));
        let below = start.saturating_sub(1);
        if rolled_back_to.is_none_or(|held| held > below) {
            store.rollback_above(&accepted_at(chain, map, below)?, "replaced revision")?;
            rolled_back_to = Some(below);
        }
    }
    for (shard_id, digest) in promote {
        store.promote_provisional(shard_id, &digest)?;
    }

    let discard: Vec<u64> = store
        .pending()?
        .iter()
        .filter(|p| {
            p.target_anchor.as_ref() != Some(target_anchor)
                || p.id.is_some_and(|id| dropped.contains(&id))
        })
        .filter_map(|p| p.id)
        .collect();
    if !discard.is_empty() {
        store.commit_shard(ShardCommit {
            pending_complete: discard,
            ..Default::default()
        })?;
    }

    // Replies a lookahead sends early are held here until the walk asks for
    // them; see `ahead`. With nothing stashed both pass straight through.
    let transport = &mut ahead::Staged::new(transport);
    let filters = &mut ahead::StagedFilters::new(filters);
    // A budgeted sync walks in sequence: whether a later shard's queries are
    // sent at all depends on page work the walk discovers on the way.
    let look_ahead = transport.concurrency() > 1
        && limits.max_queries.is_none()
        && limits.max_private_bytes.is_none();

    // The wallet's script set, with its own required heights.
    store.add_scripts(&scripts.scripts())?;
    let mut scripts_added = 0usize;
    let mut clients = Clients {
        schema: geometry.schema.clone(),
        prepared: HashMap::new(),
    };
    let mut matched_shards: Vec<u64> = Vec::new();
    let mut unproductive = 0u64;
    let mut refreshes = 0u32;
    let mut held_map_digest: Option<String> = None;
    let mut active: Cow<'_, ShardMap> = Cow::Borrowed(map);
    let mut completion = Completion::Complete;
    let mut failures = Failures::default();

    'passes: for pass in 0..=MAX_DISCOVERY_PASSES {
        if pass == MAX_DISCOVERY_PASSES {
            completion = Completion::Incomplete {
                reason: IncompleteReason::DiscoveryUnbounded,
                pending: store.pending()?.len(),
            };
            break;
        }
        let entries = store.scripts()?;
        let mut active_scripts: Vec<Vec<u8>> = Vec::new();

        // Pending page work first: it is already located, and finishing it
        // is the cheapest coverage available.
        let pending = store.pending()?;
        let mut by_revision: BTreeMap<(u64, String), Vec<PendingPages>> = BTreeMap::new();
        for item in pending {
            by_revision
                .entry((item.shard_id, item.revision_digest.clone()))
                .or_default()
                .push(item);
        }
        for ((shard_id, digest), mut items) in by_revision {
            // An entry that failed earlier in this sync, or too often, is
            // not walked again; one still owing dummies sends them.
            let mut kept = Vec::with_capacity(items.len());
            for item in items {
                if !hold(store, &mut failures.entries, shard_id, &item.script)?
                    || failures.owes(shard_id, &item.script).is_some()
                {
                    kept.push(item);
                }
            }
            items = kept;
            if items.is_empty() {
                continue;
            }
            let Some(entry) = active.shards.get(shard_id as usize) else {
                continue;
            };
            if entry.manifest_digest != digest {
                // Its revision is gone, here only after a refresh mid-sync;
                // the ordinary planning re-derives the range.
                continue;
            }
            let entry = entry.clone();
            let previous_digest = previous_digest_of(&active, shard_id);
            let outcome = with_refusals(
                store,
                &active,
                &entry,
                &previous_digest,
                geometry,
                &mut clients,
                transport,
                &mut charges,
                limits,
                |store, prepared, transport, charges, manifest| {
                    finish_pages(
                        store,
                        &entry,
                        manifest,
                        &items,
                        prepared,
                        transport,
                        charges,
                        limits,
                        target_anchor,
                        chain,
                        &mut failures,
                    )
                },
            );
            match outcome {
                Ok(Some(stopped)) => {
                    completion = stopped;
                    break 'passes;
                }
                Ok(None) => {
                    if !matched_shards.contains(&shard_id) {
                        matched_shards.push(shard_id);
                    }
                    let events = store.events()?;
                    for item in &items {
                        if holds_history(&events, &item.script, &entry, target_anchor.height)
                            && !active_scripts.contains(&item.script)
                        {
                            active_scripts.push(item.script.clone());
                        }
                    }
                }
                Err(SyncError::StaleRevision { .. }) => {
                    // Handled by the ordinary planning below on the next map.
                }
                Err(other) => return Err(other),
            }
        }

        // Plan: which shards each script still needs.
        let through = target_anchor.height;
        let mut work: BTreeMap<usize, Vec<Vec<u8>>> = BTreeMap::new();
        for entry in &entries {
            let from = entry.required_from.max(active.start_height);
            if from > through {
                continue;
            }
            let coverage = store.coverage(&entry.script)?;
            for (gap_start, gap_end) in uncovered(&coverage, from, through) {
                for (index, shard) in active.shards.iter().enumerate() {
                    if shard.end_height < gap_start {
                        continue;
                    }
                    if shard.start_height > gap_end {
                        break;
                    }
                    let scripts = work.entry(index).or_default();
                    if !scripts.contains(&entry.script) {
                        scripts.push(entry.script.clone());
                    }
                }
            }
        }

        let mut uncached = Vec::new();
        for index in work.keys() {
            let entry = &active.shards[*index];
            if store
                .filter(&entry.manifest_digest, &entry.filter_hash)?
                .is_none()
            {
                uncached.push(entry.shard_id);
            }
        }
        if filters.uses_parents() {
            charges.filter_bytes += filters
                .prepare_parents(&active, &uncached, store)
                .map_err(|e| SyncError::Transport(e.to_string()))?;
        } else {
            // Every uncached filter in the walk is downloaded anyway; overlap
            // them rather than paying one round trip each.
            filters.prefetch(&uncached);
        }
        let mut index_iter: Vec<usize> = work.keys().copied().collect();
        if look_ahead && !filters.uses_parents() {
            ahead::look_ahead(
                store,
                &active,
                genesis,
                &index_iter,
                &work,
                geometry,
                &mut clients,
                filters,
                transport,
                target_anchor,
                chain,
                &failures.entries,
            )?;
        }
        let mut position = 0usize;
        while position < index_iter.len() {
            let index = index_iter[position];
            let Some(entry) = active.shards.get(index).cloned() else {
                break;
            };
            let scripts_here = work.get(&index).cloned().unwrap_or_default();
            let previous_digest = previous_digest_of(&active, entry.shard_id);
            let result = read_shard_into(
                store,
                &active,
                &entry,
                &previous_digest,
                genesis,
                &scripts_here,
                geometry,
                &mut clients,
                filters,
                transport,
                &mut charges,
                limits,
                target_anchor,
                chain,
                &mut failures,
            );
            match result {
                Ok(ShardRead::Done { matched }) => {
                    if !matched.is_empty() {
                        if !matched_shards.contains(&entry.shard_id) {
                            matched_shards.push(entry.shard_id);
                        }
                        for script in matched {
                            if !active_scripts.contains(&script) {
                                active_scripts.push(script);
                            }
                        }
                    }
                    position += 1;
                }
                Ok(ShardRead::Unproductive { count }) => {
                    unproductive += count;
                    position += 1;
                }
                Ok(ShardRead::Stopped(stopped)) => {
                    completion = stopped;
                    break 'passes;
                }
                Err(SyncError::StaleRevision { stale, .. }) => {
                    // The service withdrew this revision while the sync was
                    // reading it. Refresh the map, check it is the same set
                    // continued, roll this shard's provisional coverage back,
                    // and re-derive from whatever replaced it.
                    //
                    // Nothing read ahead survives a refresh: the rest of this
                    // pass is walked in sequence.
                    ahead::clear(transport, filters, &mut clients);
                    if refreshes >= MAX_MAP_REFRESHES
                        || (stale.map_sha256.is_some()
                            && stale.map_sha256.as_deref() == held_map_digest.as_deref())
                    {
                        return Err(SyncError::StaleRevision {
                            shard_id: entry.shard_id,
                            stale,
                            refreshes,
                        });
                    }
                    let (covered, _) = coverage_summary(store, &active, target_anchor.height)?;
                    let (fresh, digest) = refresh_map(&active, covered, filters, &mut charges)?;
                    check_target_hash(&fresh, target_anchor)?;
                    if fresh
                        .shards
                        .last()
                        .is_none_or(|s| s.end_height < target_anchor.height)
                    {
                        completion = Completion::Incomplete {
                            reason: IncompleteReason::PublicationBehind {
                                height: target_anchor.height,
                            },
                            pending: store.pending()?.len(),
                        };
                        active = Cow::Owned(fresh);
                        break 'passes;
                    }
                    refreshes += 1;
                    held_map_digest = Some(digest);
                    let Some(resume) = fresh
                        .shards
                        .iter()
                        .position(|candidate| candidate.shard_id == entry.shard_id)
                    else {
                        // The refreshed map no longer advertises this range.
                        active = Cow::Owned(fresh);
                        break;
                    };
                    if fresh.shards[resume].manifest_digest == stale.revision {
                        return Err(SyncError::StaleRevision {
                            shard_id: entry.shard_id,
                            stale,
                            refreshes,
                        });
                    }
                    check_resume(&fresh.shards[resume], &entry).map_err(SyncError::MapDiverged)?;
                    let resume_start = fresh.shards[resume].start_height;
                    let below = resume_start.saturating_sub(1);
                    store.rollback_above(
                        &accepted_at(chain, &active, below)?,
                        "revision withdrawn mid-sync",
                    )?;
                    if rolled_back_to.is_none_or(|held| held > below) {
                        rolled_back_to = Some(below);
                    }
                    if !replaced_revisions.contains(&stale.revision) {
                        replaced_revisions.push(stale.revision.clone());
                    }
                    active = Cow::Owned(fresh);
                    // Re-plan against the new map from this shard on.
                    let mut rest: Vec<usize> = (resume..active.shards.len())
                        .take_while(|i| active.shards[*i].start_height <= through)
                        .collect();
                    index_iter.truncate(position);
                    index_iter.append(&mut rest);
                    for later in resume..active.shards.len() {
                        work.entry(later).or_insert_with(|| scripts_here.clone());
                    }
                }
                Err(other) => return Err(other),
            }
        }
        // Anything read ahead and not consumed belongs to this pass only.
        ahead::clear(transport, filters, &mut clients);

        let added = scripts.on_activity(&active_scripts);
        if added.is_empty() {
            break;
        }
        scripts_added += store.add_scripts(&added)?;
    }

    let (covered_through, settled_through) =
        coverage_summary(store, &active, target_anchor.height)?;
    if completion == Completion::Complete {
        if covered_through < target_anchor.height {
            // A failed entry leaves its script uncovered over its shard.
            completion = Completion::Incomplete {
                reason: if failures.entries.is_empty() {
                    IncompleteReason::DiscoveryUnbounded
                } else {
                    IncompleteReason::ContentFailed
                },
                pending: store.pending()?.len(),
            };
        } else if !store.pending()?.is_empty() {
            completion = Completion::Incomplete {
                reason: IncompleteReason::PendingLimit,
                pending: store.pending()?.len(),
            };
        } else if !store.ledger()?.unresolved().is_empty() {
            completion = Completion::Incomplete {
                reason: IncompleteReason::UnresolvedSpends,
                pending: 0,
            };
        } else {
            store.commit_anchor(target_anchor, settled_through, covered_through)?;
        }
    }
    matched_shards.sort_unstable();
    let mut failed = failures.entries;
    failed.sort_by(|a, b| (a.shard_id, &a.script).cmp(&(b.shard_id, &b.script)));
    Ok(SyncReport {
        ledger: store.ledger()?,
        charges,
        matched_shards,
        unproductive_matches: unproductive,
        covered_through,
        settled_through,
        provisional: provisional_of(store, &active)?,
        map_refreshes: refreshes,
        completion,
        rolled_back_to,
        replaced_revisions,
        scripts_added,
        commits: store.last_commit()? - first_commit,
        failed_entries: failed,
    })
}

/// Whether the map still vouches for a settled range the store holds.
///
/// The range is looked up by the height it starts at, not by its shard id: a
/// re-cut renumbers shards, so the id it was read under may now name another
/// range. The map vouches for it when it still publishes that revision there,
/// or when one of its re-cuts declares exactly that revision superseded: the
/// same digest, shard id and start height, sealed, ending on the same block.
/// That keeps a wallet's history across the re-cut without re-reading it.
///
/// The block a range ends on is its source anchor, the published endpoint,
/// when the store kept one, which every store since schema 2 does. A range
/// without one matches a declaration when it ends inside the declared range,
/// or at its end on the declared block. A range with an endpoint but cut
/// short of its shard's end by a target or a rollback is left to the chain
/// checks, as is anything the map's sealed shards do not cover.
fn vouches_for(
    map: &ShardMap,
    declared: &BTreeMap<&str, &transparent_filter::SupersededShard>,
    range: &CoverageRange,
) -> bool {
    if range
        .source_anchor
        .as_ref()
        .is_some_and(|source| source.height != range.end_height)
    {
        return true;
    }
    let Some(entry) = map.shard_for_height(range.start_height) else {
        return true;
    };
    if !entry.sealed {
        return true;
    }
    if entry.start_height == range.start_height
        && entry.manifest_digest == range.revision_digest
        && range.source_anchor.as_ref().is_none_or(|source| {
            source.height == entry.end_height && source.hash == entry.terminal_block_hash
        })
    {
        return true;
    }
    declared
        .get(range.revision_digest.as_str())
        .is_some_and(|declared| {
            let ends_as_declared = match &range.source_anchor {
                Some(source) => {
                    declared.end_height == source.height
                        && declared.terminal_block_hash == source.hash
                }
                // Without a recorded endpoint a range may have been cut short
                // of its shard by a target or a rollback, as one with an
                // endpoint is above: inside the declared range it matches,
                // and at the declared end it must rest on the declared block.
                None => {
                    range.end_height < declared.end_height
                        || (range.end_height == declared.end_height
                            && range.terminal_block_hash == declared.terminal_block_hash)
                }
            };
            declared.sealed
                && declared.shard_id == range.shard_id
                && declared.start_height == range.start_height
                && ends_as_declared
        })
}

/// What a settled range the map no longer vouches for means.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Rewrite {
    /// The wallet's chain rejects the block the range rests on: a reorg,
    /// rolled back like any other.
    Reorg,
    /// The wallet's chain accepts both the block the range rests on and the
    /// one the shard now covering its start ends on: the publisher
    /// contradicts itself on the chain the wallet follows. Retrying does not
    /// change that while it serves this history.
    Contradiction,
    /// The chain cannot settle it yet; the height is the block it would need
    /// to confirm. A publisher that followed a shallow reorg across a shard it
    /// had just sealed, before the wallet's chain did, looks like this, and
    /// becomes an ordinary reorg once the chain moves.
    Unsettled(u64),
}

/// Judges a settled range `vouches_for` refused, by what the wallet's chain
/// says about the block it rests on and about the block the map's shard now
/// covering its start ends on.
///
/// Sealing is not delayed for finality, so a publisher can follow a reorg
/// through a just-sealed shard before the wallet's chain does. Only when the
/// chain accepts the replacement's end too, at or below the target, is the
/// change the publisher's alone.
fn judge_rewrite(
    map: &ShardMap,
    range: &CoverageRange,
    target: &Anchor,
    chain: &impl ChainView,
) -> Rewrite {
    match chain.is_accepted(range.end_height, &range.terminal_block_hash) {
        Acceptance::Rejected => Rewrite::Reorg,
        Acceptance::Unknown => Rewrite::Unsettled(range.end_height),
        Acceptance::Accepted => match map.shard_for_height(range.start_height) {
            Some(entry)
                if entry.sealed
                    && entry.end_height <= target.height
                    && chain.is_accepted(entry.end_height, &entry.terminal_block_hash)
                        == Acceptance::Accepted =>
            {
                Rewrite::Contradiction
            }
            Some(entry) => Rewrite::Unsettled(entry.end_height),
            // `vouches_for` refuses only ranges a sealed shard covers, so
            // this and the unsealed case above are not reached; both would
            // be the publisher ahead of or apart from the chain, not settled.
            None => Rewrite::Unsettled(range.end_height),
        },
    }
}

/// What becomes of unfinished page work under a revision the map no longer
/// publishes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Unfinished {
    /// Dropped, and what the revision saved stays.
    Drop,
    /// Dropped, and the store rolls back from this height.
    Rewind(u64),
}

/// Decides [`Unfinished`] for one item, from what the wallet itself stored.
///
/// Every event an item saved lies at or below the target it was read for,
/// which the store keeps with it. So when a declared re-cut replaced the
/// sealed revision and the wallet's chain still accepts that target block,
/// what was saved is history on the chain the wallet follows, which a re-cut
/// does not change: it stays, the script's gap is read again under the shard
/// now covering it, and events are kept once per outpoint. The declaration
/// only says this is a re-cut; nothing it claims about heights or blocks is
/// relied on.
///
/// Otherwise what the revision saved is rolled back from the lowest height it
/// saved anything at, or the declared start if that is lower; never from the
/// declared start alone, which the wallet has not checked. A revision that
/// saved nothing leaves nothing to roll back.
fn unfinished(
    pending: &PendingPages,
    declared: Option<&transparent_filter::SupersededShard>,
    lowest_saved: Option<u64>,
    chain: &impl ChainView,
) -> Unfinished {
    let declared = declared.filter(|declared| declared.shard_id == pending.shard_id);
    let target_held = pending.target_anchor.as_ref().is_some_and(|target| {
        chain.is_accepted(target.height, &target.hash) == Acceptance::Accepted
    });
    if declared.is_some_and(|declared| declared.sealed) && target_held {
        return Unfinished::Drop;
    }
    match lowest_saved {
        Some(lowest) => Unfinished::Rewind(
            declared.map_or(lowest, |declared| declared.start_height.min(lowest)),
        ),
        None => Unfinished::Drop,
    }
}

/// Whether `script` has stored history inside `entry`'s range, up to the
/// target.
///
/// By height rather than by the revision that recorded it: an event is kept
/// once per outpoint, so history first read under a revision a re-cut has
/// since replaced keeps that revision's name when the new shard is read.
fn holds_history(
    events: &[StoredEvent],
    script: &[u8],
    entry: &transparent_filter::ShardMapEntry,
    through: u64,
) -> bool {
    let range = entry.start_height..=entry.end_height.min(through);
    events
        .iter()
        .any(|stored| stored.script == script && range.contains(&event_height(&stored.event)))
}

fn previous_digest_of(map: &ShardMap, shard_id: u64) -> String {
    shard_id
        .checked_sub(1)
        .and_then(|before| map.shards.get(before as usize))
        .map(|entry| entry.manifest_digest.clone())
        .unwrap_or_default()
}

/// Heights through which every script is covered from its required height:
/// provisional coverage included, then settled only. The minimum over
/// scripts, so one script's gap holds the whole wallet's figure down.
fn coverage_summary<S: WalletStore>(
    store: &S,
    map: &ShardMap,
    through: u64,
) -> Result<(u64, u64), SyncError> {
    let floor = map.start_height.saturating_sub(1);
    let mut covered = through;
    let mut settled = through;
    let entries = store.scripts()?;
    if entries.is_empty() {
        return Ok((floor, floor));
    }
    for entry in entries {
        let from = entry.required_from.max(map.start_height);
        let ranges = store.coverage(&entry.script)?;
        let all = uncovered(&ranges, from, through);
        let mine = all
            .first()
            .map_or(through, |(start, _)| start.saturating_sub(1));
        covered = covered.min(mine);
        let settled_ranges: Vec<CoverageRange> = ranges
            .iter()
            .filter(|range| range.kind == CoverageKind::Settled)
            .cloned()
            .collect();
        let settled_gaps = uncovered(&settled_ranges, from, through);
        let mine_settled = settled_gaps
            .first()
            .map_or(through, |(start, _)| start.saturating_sub(1));
        settled = settled.min(mine_settled);
    }
    Ok((covered.max(floor), settled.max(floor)))
}

fn provisional_of<S: WalletStore>(
    store: &S,
    map: &ShardMap,
) -> Result<Vec<ProvisionalCoverage>, SyncError> {
    let mut seen: BTreeMap<(u64, String), u64> = BTreeMap::new();
    for range in store.provisional()? {
        let end = seen
            .entry((range.shard_id, range.revision_digest.clone()))
            .or_insert(range.end_height);
        *end = (*end).max(range.end_height);
    }
    Ok(seen
        .into_iter()
        .map(
            |((shard_id, manifest_digest), end_height)| ProvisionalCoverage {
                shard_id,
                // The digest identifies the revision; its number is the map's.
                revision: map
                    .shards
                    .get(shard_id as usize)
                    .filter(|entry| entry.manifest_digest == manifest_digest)
                    .map(|entry| entry.revision)
                    .unwrap_or(0),
                manifest_digest,
                end_height,
            },
        )
        .collect())
}

enum ShardRead {
    /// Every script in scope was covered; `matched` names those with history.
    Done { matched: Vec<Vec<u8>> },
    /// The filter matched but nothing was found; coverage advanced anyway.
    Unproductive { count: u64 },
    /// A budget or refusal stopped the sync with the store consistent.
    Stopped(Completion),
}

/// Reads one shard for the scripts that need it, committing to the store.
#[allow(clippy::too_many_arguments)]
fn read_shard_into<S: WalletStore>(
    store: &mut S,
    map: &ShardMap,
    entry: &transparent_filter::ShardMapEntry,
    previous_digest: &str,
    genesis: BlockHash,
    scripts: &[Vec<u8>],
    geometry: &ServiceGeometry,
    clients: &mut Clients,
    filters: &mut impl FilterSource,
    transport: &mut impl ShardTransport,
    charges: &mut ByteCharges,
    limits: &WorkLimits,
    target_anchor: &Anchor,
    chain: &impl ChainView,
    failures: &mut Failures,
) -> Result<ShardRead, SyncError> {
    let endpoint = endpoint_of(entry, target_anchor);
    match chain.is_accepted(endpoint.height, &endpoint.hash) {
        Acceptance::Accepted => {}
        Acceptance::Unknown => {
            return Ok(ShardRead::Stopped(Completion::Incomplete {
                reason: IncompleteReason::ChainUnknown {
                    height: endpoint.height,
                },
                pending: store.pending()?.len(),
            }))
        }
        Acceptance::Rejected => {
            return Err(SyncError::Invalid(
                "published coverage endpoint rejected by wallet chain".into(),
            ))
        }
    }
    // Ordinary sources download every filter in range. The explicit experiment
    // may establish a parent negative; both paths use the same bounded commit.
    let cached = store.filter(&entry.manifest_digest, &entry.filter_hash)?;
    let (parent_negative, parent_cost) = if cached.is_none() {
        filters
            .parent_negative(map, entry.shard_id, scripts, store)
            .map_err(|e| SyncError::Transport(e.to_string()))?
    } else {
        (false, 0)
    };
    charges.filter_bytes += parent_cost;
    let matches = if parent_negative {
        Vec::new()
    } else {
        let bytes = match cached {
            Some(bytes) => bytes,
            None => {
                let (bytes, cost) = filters.filter(entry.shard_id).map_err(|error| {
                    SyncError::Transport(crate::transport::describe_error(error.as_ref()))
                })?;
                charges.filter_bytes += cost;
                // Bind the bytes to the map before believing anything they say.
                if transparent_filter::filter_hash(&bytes).to_display_hex() != entry.filter_hash {
                    // The public filter URL is shard-addressed: activation may
                    // have replaced it since this map was fetched. Never cache or
                    // match these bytes. The bounded stale-revision path requires
                    // a changed, validated map before retrying; an unchanged map
                    // still fails, including when the filter was simply corrupt.
                    return Err(SyncError::StaleRevision {
                        shard_id: entry.shard_id,
                        stale: StaleRevision {
                            shard_id: entry.shard_id,
                            revision: entry.manifest_digest.clone(),
                            map_sha256: None,
                        },
                        refreshes: 0,
                    });
                }
                store.put_filter(
                    &entry.manifest_digest,
                    &entry.filter_hash,
                    entry.sealed,
                    &bytes,
                )?;
                bytes
            }
        };
        charges.filters_checked += 1;
        match_filter(map, entry, genesis, scripts, &bytes)?
    };
    if matches.is_empty() {
        // A genuinely empty range for every script here: coverage advances
        // with no private request.
        commit_bounded(
            store,
            target_anchor,
            ShardCommit {
                source_anchor: None,
                shard_id: entry.shard_id,
                revision_digest: entry.manifest_digest.clone(),
                sealed: entry.sealed,
                start_height: entry.start_height,
                end_height: entry.end_height,
                terminal_block_hash: entry.terminal_block_hash.clone(),
                events: Vec::new(),
                covered_scripts: scripts.to_vec(),
                pending_upsert: Vec::new(),
                pending_complete: Vec::new(),
                cached_runs: Vec::new(),
                failed_entries: Vec::new(),
            },
        )?;
        return Ok(ShardRead::Done {
            matched: Vec::new(),
        });
    }
    // A geometry this build does not know is fatal the moment there is
    // something to fetch, whatever the manifest would say about it.
    if transparent_shard::layout::by_name(&entry.geometry).is_none() {
        return Err(SyncError::UnknownGeometry(entry.geometry.clone()));
    }
    let matched_all: Vec<Vec<u8>> = matches.iter().map(|&i| scripts[i].clone()).collect();
    let unmatched: Vec<Vec<u8>> = scripts
        .iter()
        .filter(|script| !matched_all.contains(script))
        .cloned()
        .collect();
    // An entry held out of this sync is not queried at all, and stays
    // uncovered over this shard.
    let mut matched_scripts = Vec::with_capacity(matched_all.len());
    for script in matched_all {
        if !hold(store, &mut failures.entries, entry.shard_id, &script)? {
            matched_scripts.push(script);
        }
    }
    if matched_scripts.is_empty() {
        if !unmatched.is_empty() {
            commit_bounded(
                store,
                target_anchor,
                ShardCommit {
                    source_anchor: None,
                    shard_id: entry.shard_id,
                    revision_digest: entry.manifest_digest.clone(),
                    sealed: entry.sealed,
                    start_height: entry.start_height,
                    end_height: entry.end_height,
                    terminal_block_hash: entry.terminal_block_hash.clone(),
                    covered_scripts: unmatched,
                    ..Default::default()
                },
            )?;
        }
        return Ok(ShardRead::Done {
            matched: Vec::new(),
        });
    }

    let outcome = with_refusals(
        store,
        map,
        entry,
        previous_digest,
        geometry,
        clients,
        transport,
        charges,
        limits,
        |store, prepared, transport, charges, manifest| {
            // Validated when the manifest was verified; decoded again here
            // rather than carried, since it is a few kilobytes.
            let choice = directory_choice(manifest)?;
            let salt = tag_salt_of(manifest)?;
            retrieve_shard_into(
                store,
                entry,
                manifest,
                &matched_scripts,
                &unmatched,
                choice.as_ref(),
                &salt,
                prepared,
                transport,
                charges,
                limits,
                target_anchor,
                chain,
                failures,
            )
        },
    )?;
    match outcome {
        Some(stopped) => Ok(ShardRead::Stopped(stopped)),
        None => {
            // Which matched scripts actually held history is what the
            // caller's gap-limit rule wants; the store knows.
            let mut found = Vec::new();
            let mut unproductive = 0u64;
            let events = store.events()?;
            for script in &matched_scripts {
                if failures
                    .entries
                    .iter()
                    .any(|held| held.shard_id == entry.shard_id && held.script == *script)
                {
                    continue;
                }
                if holds_history(&events, script, entry, target_anchor.height) {
                    found.push(script.clone());
                } else {
                    unproductive += 1;
                }
            }
            if found.is_empty() {
                Ok(ShardRead::Unproductive {
                    count: unproductive,
                })
            } else {
                Ok(ShardRead::Done { matched: found })
            }
        }
    }
}

/// The block a shard's coverage ends on for this sync: its own terminal
/// block, or the target when the shard runs past it.
fn endpoint_of(entry: &transparent_filter::ShardMapEntry, target_anchor: &Anchor) -> Anchor {
    if entry.end_height > target_anchor.height {
        target_anchor.clone()
    } else {
        Anchor {
            height: entry.end_height,
            hash: entry.terminal_block_hash.clone(),
        }
    }
}

/// Which of `scripts` a shard's filter matches, by index.
fn match_filter(
    map: &ShardMap,
    entry: &transparent_filter::ShardMapEntry,
    genesis: BlockHash,
    scripts: &[Vec<u8>],
    bytes: &[u8],
) -> Result<Vec<usize>, SyncError> {
    let profile = transparent_filter::range_profile(&map.profile)
        .ok_or_else(|| SyncError::UnknownProfile(map.profile.clone()))?;
    let validated =
        transparent_filter::validate_range_filter(bytes, FilterLimits::default(), profile)?;
    let terminal = BlockHash::from_display_hex(&entry.terminal_block_hash)?;
    let key = ShardKey::derive(
        &map.profile,
        genesis,
        entry.shard_id,
        entry.start_height,
        entry.end_height,
        terminal,
    );
    let owned: Vec<ScriptBytes> = scripts
        .iter()
        .map(|script| ScriptBytes::new(script.clone()))
        .collect();
    Ok(transparent_filter::match_range_scripts(
        &validated, key, &owned,
    )?)
}

/// Runs `work` against a shard, verifying the manifest first and lifting the
/// service's two refusals: a withdrawn revision propagates for the map
/// refresh, an overload is retried with backoff and then reported as an
/// incomplete sync with the store consistent.
#[allow(clippy::too_many_arguments)]
fn with_refusals<S: WalletStore, T: ShardTransport, F>(
    store: &mut S,
    map: &ShardMap,
    entry: &transparent_filter::ShardMapEntry,
    previous_digest: &str,
    geometry: &ServiceGeometry,
    clients: &mut Clients,
    transport: &mut T,
    charges: &mut ByteCharges,
    limits: &WorkLimits,
    mut work: F,
) -> Result<Option<Completion>, SyncError>
where
    F: FnMut(
        &mut S,
        &mut GeometryClients,
        &mut T,
        &mut ByteCharges,
        &ShardManifest,
    ) -> Result<Option<Completion>, SyncError>,
{
    let _ = limits;
    let mut attempt = 1u32;
    let mut manifest: Option<ShardManifest> = None;
    loop {
        let verified = match &manifest {
            Some(verified) => verified,
            None => match fetch_manifest(entry, previous_digest, map, transport, charges) {
                Ok(verified) => {
                    if verified.schema != geometry.schema {
                        return Err(SyncError::ManifestMismatch {
                            shard_id: entry.shard_id,
                            field: "schema",
                        });
                    }
                    manifest.insert(verified)
                }
                Err(SyncError::Client(ClientError::Stale(stale))) => {
                    return Err(SyncError::StaleRevision {
                        shard_id: entry.shard_id,
                        stale,
                        refreshes: 0,
                    })
                }
                Err(SyncError::Client(ClientError::Overloaded(overloaded))) => {
                    if attempt >= MAX_OVERLOAD_ATTEMPTS {
                        return Ok(Some(Completion::Incomplete {
                            reason: IncompleteReason::Overloaded {
                                shard_id: entry.shard_id,
                            },
                            pending: store.pending()?.len(),
                        }));
                    }
                    std::thread::sleep(overload_backoff(attempt, overloaded.retry_after));
                    attempt += 1;
                    continue;
                }
                Err(other) => return Err(other),
            },
        };
        let prepared = clients.prepare(&verified.geometry, &geometry.geometries)?;
        match work(store, prepared, transport, charges, verified) {
            Ok(stopped) => return Ok(stopped),
            Err(SyncError::Client(ClientError::Stale(stale))) => {
                return Err(SyncError::StaleRevision {
                    shard_id: entry.shard_id,
                    stale,
                    refreshes: 0,
                })
            }
            Err(SyncError::Client(ClientError::Overloaded(overloaded))) => {
                if attempt >= MAX_OVERLOAD_ATTEMPTS {
                    return Ok(Some(Completion::Incomplete {
                        reason: IncompleteReason::Overloaded {
                            shard_id: entry.shard_id,
                        },
                        pending: store.pending()?.len(),
                    }));
                }
                // Nothing partial is lost: every commit so far stands, and
                // the work resumes from the store on the next attempt.
                std::thread::sleep(overload_backoff(attempt, overloaded.retry_after));
                attempt += 1;
            }
            Err(SyncError::Client(ClientError::SetupMismatch {
                revision,
                table,
                segment,
            })) => {
                // The setup held for that segment is not the one the service
                // answers under. It is forgotten, here and in the store, so
                // the next attempt fetches it again; this read still fails,
                // as any failed request does, and counts against no entry.
                let prepared = clients.prepare(&verified.geometry, &geometry.geometries)?;
                match table {
                    Table::Directory => prepared.directory.forget(&revision, table, segment),
                    Table::Pages(_) => prepared.pages.forget(&revision, table, segment),
                }
                let set_digest = store
                    .set_identity()?
                    .map(|identity| identity.digest())
                    .unwrap_or_default();
                store.drop_setup(&SetupKey {
                    set_digest,
                    revision_digest: revision.clone(),
                    table,
                    segment,
                })?;
                return Err(SyncError::Client(ClientError::SetupMismatch {
                    revision,
                    table,
                    segment,
                }));
            }
            Err(other) => return Err(other),
        }
    }
}

/// Whether a budget has been reached.
fn budget_stopped(charges: &ByteCharges, limits: &WorkLimits) -> Option<IncompleteReason> {
    if let Some(max) = limits.max_queries {
        if charges.queries() >= max {
            return Some(IncompleteReason::QueryBudget);
        }
    }
    if let Some(max) = limits.max_private_bytes {
        let private = charges.setup_bytes() + charges.query_upload() + charges.query_download();
        if private >= max {
            return Some(IncompleteReason::ByteBudget);
        }
    }
    None
}

/// Fetches a matched shard's manifest and verifies it against the map.
fn fetch_manifest(
    entry: &transparent_filter::ShardMapEntry,
    previous_digest: &str,
    map: &ShardMap,
    transport: &mut impl ShardTransport,
    charges: &mut ByteCharges,
) -> Result<ShardManifest, SyncError> {
    let (raw, cost) = transport
        .manifest(entry.shard_id, &entry.manifest_digest)
        .map_err(|error| SyncError::from(classify_transport(error)))?;
    charges.add_manifest(cost);
    verify_manifest(&raw, entry, previous_digest, map)
}

/// Checks a manifest against the map entry that named it, the entry before
/// it, and this build's registry.
///
/// The digest first: the bytes must hash to the digest the map names, or
/// nothing else in them is worth reading. Then every field a retrieval relies
/// on. The parent link ties the manifest to the map's previous entry, so a
/// manifest that is individually well formed but belongs to a different
/// sequence of shards is refused too. Segment shapes are checked against the
/// registry entry the manifest names, which is what makes the geometry a
/// property of the verified manifest rather than of the map alone.
///
/// None of this is an independence proof. The map, the manifest and the
/// tables come from one publisher; agreement among them rules out corruption,
/// stale data and mixed publications, not omission.
fn verify_manifest(
    raw: &[u8],
    entry: &transparent_filter::ShardMapEntry,
    previous_digest: &str,
    map: &ShardMap,
) -> Result<ShardManifest, SyncError> {
    let shard_id = entry.shard_id;
    let mismatch = |field: &'static str| SyncError::ManifestMismatch { shard_id, field };
    let manifest: ShardManifest = serde_json::from_slice(raw)
        .map_err(|error| SyncError::Invalid(format!("shard {shard_id} manifest: {error}")))?;
    if manifest.digest() != entry.manifest_digest {
        return Err(mismatch("digest"));
    }
    if !transparent_shard::manifest::supported_schema(&manifest.schema) {
        return Err(SyncError::Schema {
            served: manifest.schema.clone(),
            expected: transparent_shard::SCHEMA,
        });
    }
    if manifest.layout != transparent_shard::ManifestLayout::current() {
        return Err(mismatch("layout"));
    }
    if manifest.tag_salt_counter > transparent_shard::tag::MAX_TAG_SALT_COUNTER {
        return Err(mismatch("tag_salt_counter"));
    }
    if manifest.network != map.network {
        return Err(mismatch("network"));
    }
    if manifest.genesis_hash != map.genesis_hash {
        return Err(mismatch("genesis_hash"));
    }
    if manifest.profile != map.profile {
        return Err(mismatch("profile"));
    }
    if manifest.shard_id != entry.shard_id {
        return Err(mismatch("shard_id"));
    }
    if manifest.geometry != entry.geometry {
        return Err(mismatch("geometry"));
    }
    if manifest.start_height != entry.start_height || manifest.end_height != entry.end_height {
        return Err(mismatch("range"));
    }
    if manifest.parent_block_hash != entry.parent_block_hash {
        return Err(mismatch("parent_block_hash"));
    }
    if manifest.terminal_block_hash != entry.terminal_block_hash {
        return Err(mismatch("terminal_block_hash"));
    }
    if manifest.filter_hash != entry.filter_hash {
        return Err(mismatch("filter_hash"));
    }
    if manifest.sealed != entry.sealed {
        return Err(mismatch("sealed"));
    }
    if manifest.revision != entry.revision {
        return Err(mismatch("revision"));
    }
    if manifest.parent_manifest_digest != previous_digest {
        return Err(mismatch("parent_manifest_digest"));
    }
    // The map's `page_segments` counts the directory shard's page shards.
    if manifest.directory_segments.len() as u32 != entry.directory_segments
        || manifest.page_children.len() as u32 != entry.page_segments
    {
        return Err(mismatch("segment_count"));
    }
    let geometry = transparent_shard::layout::by_name(&manifest.geometry)
        .ok_or_else(|| SyncError::UnknownGeometry(manifest.geometry.clone()))?;
    for segment in &manifest.directory_segments {
        if segment.rows != geometry.directory_rows
            || segment.row_bytes != geometry.directory_row_bytes as u32
        {
            return Err(mismatch("directory_segment_shape"));
        }
    }
    // Every page shard: contiguous, in order, sealed but for an open last one
    // of a tail, ending on the shard's terminal, segments of the registry's
    // shape. A walk relies on each of these.
    manifest
        .check_children(geometry.page_rows, geometry.page_row_bytes as u32)
        .map_err(|_| mismatch("page_children"))?;
    for child in &manifest.page_children {
        BlockHash::from_display_hex(&child.terminal_block_hash)
            .map_err(|_| mismatch("page_children"))?;
    }
    if let Some(seal) = map.seal.get(&manifest.geometry) {
        if manifest.seal.scripts_target != seal.max_scripts
            || manifest.seal.page_rows_target != seal.max_page_rows
        {
            return Err(mismatch("seal"));
        }
    }
    directory_choice(&manifest)?;
    Ok(manifest)
}

/// The manifest's directory choice table, if it publishes one.
///
/// A table that does not decode, or that indexes a different number of
/// scripts than the shard places, stops the sync. Falling back to two queries
/// would be safe for this shard, but it would hide a malformed publication
/// behind a working one.
fn directory_choice(
    manifest: &ShardManifest,
) -> Result<Option<transparent_shard::ChoiceTable>, SyncError> {
    manifest.directory_choice().map_err(|error| {
        SyncError::Invalid(format!("shard {} manifest: {error}", manifest.shard_id))
    })
}

/// How long to wait before re-asking an overloaded service.
///
/// The service's own figure is preferred, because it knows why it refused, but
/// it is capped: a delay a wallet cannot sanity-check is a delay a
/// misconfigured or hostile service could use to park a sync inside one call.
/// Either way the wait is jittered by up to half again, so wallets refused
/// together do not return together.
fn overload_backoff(attempt: u32, asked: Option<Duration>) -> Duration {
    let wait = asked.unwrap_or_else(|| OVERLOAD_BACKOFF * 2u32.saturating_pow(attempt - 1));
    crate::backoff::jittered(wait.min(OVERLOAD_BACKOFF_CAP))
}

/// Refetches the map after a withdrawn revision, and checks it continues the
/// one in hand.
///
/// Returns the map and the digest of the bytes it came from, so a later refusal
/// naming that same digest can be recognised as unresolvable rather than
/// spending another refresh on it.
fn refresh_map(
    started_from: &ShardMap,
    covered_through: u64,
    filters: &mut impl FilterSource,
    charges: &mut ByteCharges,
) -> Result<(ShardMap, String), SyncError> {
    let (bytes, cost) = filters
        .shard_map()
        .map_err(|error| SyncError::Transport(crate::transport::describe_error(error.as_ref())))?;
    charges.map_bytes += cost;
    let fresh: ShardMap = serde_json::from_slice(&bytes).map_err(|error| {
        SyncError::Invalid(format!("refreshed shard map is malformed: {error}"))
    })?;
    fresh.check_shape().map_err(|error| {
        SyncError::Invalid(format!("refreshed shard map is malformed: {error}"))
    })?;
    check_continuation(started_from, &fresh, covered_through).map_err(SyncError::MapDiverged)?;
    let digest = hex::encode(<sha2::Sha256 as sha2::Digest>::digest(&bytes));
    Ok((fresh, digest))
}

/// Checks a refreshed map is the same shard set, continued.
///
/// A map fetched mid-sync is not independently trusted. The caller validated
/// the map this sync started from against the wallet's accepted chain, and a
/// refresh inherits that validation only over the range the two agree on. So
/// the set's identity must be unchanged, and every shard whose range is already
/// covered must be the *same entry* — sealed content is immutable, so a
/// disagreement there means a different set rather than a longer one.
///
/// Everything this function rules out is what makes the recovery sound: once it
/// passes, the shard the sync resumes at begins exactly where coverage stopped,
/// so no range can be skipped and none can need retracting.
fn check_continuation(
    started_from: &ShardMap,
    fresh: &ShardMap,
    covered_through: u64,
) -> Result<(), String> {
    if fresh.genesis_hash != started_from.genesis_hash {
        return Err("it names a different genesis block".into());
    }
    if fresh.network != started_from.network {
        return Err("it names a different network".into());
    }
    if fresh.profile != started_from.profile {
        return Err("it names a different filter profile".into());
    }
    if fresh.range_envelope_version != started_from.range_envelope_version {
        return Err("it names a different range envelope version".into());
    }
    if fresh.start_height != started_from.start_height {
        return Err("its coverage begins at a different height".into());
    }
    // A re-cut redraws the boundaries this walk was planned over and gives
    // shard ids to other ranges, even where it lies above everything covered
    // so far. This sync ends; the next starts from the re-cut map, whose
    // declaration lets it keep what the store holds.
    if fresh.recut_epoch() != started_from.recut_epoch() {
        return Err(format!(
            "it is re-cut epoch {}, this sync started from epoch {}",
            fresh.recut_epoch(),
            started_from.recut_epoch()
        ));
    }
    // New geometries may appear as a set grows into another tier; the seal
    // parameters of a geometry already in use may not change, because they are
    // what the boundaries were derived from.
    for (name, seal) in &started_from.seal {
        if let Some(refreshed) = fresh.seal.get(name) {
            if refreshed != seal {
                return Err(format!("it reseals geometry {name}"));
            }
        }
    }
    for (index, before) in started_from.shards.iter().enumerate() {
        if before.end_height > covered_through {
            break;
        }
        match fresh.shards.get(index) {
            Some(after) if after == before => {}
            Some(_) => {
                return Err(format!(
                    "shard {} covers height {} but is not the shard this sync read",
                    before.shard_id, before.end_height
                ))
            }
            None => {
                return Err(format!(
                    "it withdraws shard {}, whose range this sync already covered",
                    before.shard_id
                ))
            }
        }
    }
    // A map that ends below what is already covered withdraws history the
    // wallet has read, which is the same disagreement as a re-cut shard.
    if let Some(last) = fresh.shards.last() {
        if last.end_height < covered_through {
            return Err(format!(
                "its coverage ends at {}, below the {covered_through} this sync already read",
                last.end_height
            ));
        }
    } else if covered_through >= started_from.start_height {
        return Err("it publishes no shards at all".into());
    }
    Ok(())
}

/// Checks the shard the walk resumes at after a refresh.
///
/// The walk resumes by shard id, which is sound only while the id still names
/// a range starting where the withdrawn one did. `check_continuation` already
/// refuses a refreshed map that changes covered history or its re-cut epoch,
/// so with a well-formed map this holds; it is checked again here because
/// resuming anywhere else would skip or repeat heights.
fn check_resume(
    resumed: &transparent_filter::ShardMapEntry,
    withdrawn: &transparent_filter::ShardMapEntry,
) -> Result<(), String> {
    if resumed.start_height != withdrawn.start_height {
        return Err(format!(
            "it moves shard {} from height {} to {}",
            withdrawn.shard_id, withdrawn.start_height, resumed.start_height
        ));
    }
    Ok(())
}

/// Salt the wallet derives from a verified manifest. Records are accepted
/// only under this salt.
fn tag_salt_of(manifest: &ShardManifest) -> Result<[u8; 32], SyncError> {
    let terminal = BlockHash::from_display_hex(&manifest.terminal_block_hash).map_err(|error| {
        SyncError::Invalid(format!(
            "shard {} terminal block hash: {error}",
            manifest.shard_id
        ))
    })?;
    Ok(transparent_shard::tag_salt(
        manifest.shard_id,
        &terminal,
        manifest.tag_salt_counter,
    ))
}

/// The directory phase for one shard: the candidate rows for every matched
/// script, inline events committed, chain walks recorded; then the walks.
///
/// A script whose replies fail on content fails its entry alone: it gets no
/// coverage and no walk, as an entry with inline history or a false-positive
/// match sends no pages either, and the shard's one commit still lands for
/// every other script, counting the failure. Every directory query planned for
/// a script is sent before any of its replies is decoded, so the queries sent
/// are the same whatever came back, and in the order a lookahead stashed them.
#[allow(clippy::too_many_arguments)]
fn retrieve_shard_into<S: WalletStore, T: ShardTransport>(
    store: &mut S,
    entry: &transparent_filter::ShardMapEntry,
    manifest: &ShardManifest,
    matched: &[Vec<u8>],
    unmatched: &[Vec<u8>],
    choice: Option<&transparent_shard::ChoiceTable>,
    salt: &[u8; 32],
    clients: &mut GeometryClients,
    transport: &mut T,
    charges: &mut ByteCharges,
    limits: &WorkLimits,
    target_anchor: &Anchor,
    chain: &impl ChainView,
    failures: &mut Failures,
) -> Result<Option<Completion>, SyncError> {
    let shard_id = entry.shard_id;
    let geometry = transparent_shard::layout::by_name(&entry.geometry)
        .ok_or_else(|| SyncError::UnknownGeometry(entry.geometry.clone()))?;
    let revision = entry.manifest_digest.as_str();
    // An overload can occur after the directory commit but before the walks
    // finish. with_refusals calls this function again on retry. Resume those
    // durable walks first: rediscovering their directories would enqueue
    // duplicate work and leave the original walks pending after the retry.
    let pending: Vec<_> = store
        .pending()?
        .into_iter()
        .filter(|p| p.shard_id == shard_id && p.revision_digest == revision)
        .collect();
    let mut resumed = Vec::with_capacity(pending.len());
    for item in &pending {
        if !held(store, &failures.entries, shard_id, &item.script)?
            || failures.owes(shard_id, &item.script).is_some()
        {
            resumed.push(item.clone());
        }
    }
    if !resumed.is_empty() {
        if let Some(stopped) = finish_pages(
            store,
            entry,
            manifest,
            &resumed,
            clients,
            transport,
            charges,
            limits,
            target_anchor,
            chain,
            failures,
        )? {
            return Ok(Some(stopped));
        }
    }
    // A retry after an overload does not ask again for an entry that already
    // failed in this sync, nor count it twice.
    let mut unowed = Vec::with_capacity(matched.len());
    for script in matched {
        if !pending.iter().any(|p| &p.script == script)
            && !held(store, &failures.entries, shard_id, script)?
        {
            unowed.push(script);
        }
    }
    let matched = unowed;
    if matched.is_empty() {
        // The earlier directory commit also covered all unmatched scripts.
        return Ok(None);
    }
    let set_digest = store
        .set_identity()?
        .map(|identity| identity.digest())
        .unwrap_or_default();
    open_table(
        store,
        &set_digest,
        shard_id,
        revision,
        Table::Directory,
        entry.directory_segments,
        &mut clients.directory,
        transport,
        charges,
        None,
    )?;

    if let Some(reason) = budget_stopped(charges, limits) {
        return Ok(Some(Completion::Incomplete {
            reason,
            pending: store.pending()?.len(),
        }));
    }

    let mut events: Vec<StoredEvent> = Vec::new();
    let mut covered: Vec<Vec<u8>> = unmatched.to_vec();
    let mut pending_upsert: Vec<PendingPages> = Vec::new();
    let mut failed_here: Vec<Vec<u8>> = Vec::new();

    for script in matched {
        // With a published choice table, exactly the one candidate row it
        // names is queried. Without one, both candidate rows are. Either way
        // the query count per matched script is fixed for the shard: querying
        // the first and stopping on a hit would make it depend on where the
        // script landed, which is a function of the script. Candidates are
        // taken over the shard's whole logical row space; each names one row
        // within a segment, and every segment answers it.
        let wanted = transparent_shard::script_tag(salt, script);
        let rows = geometry.directory_rows * entry.directory_segments as u64;
        let candidates = candidate_rows(shard_id, script, rows);
        let queried: &[u64] = match choice {
            Some(table) => std::slice::from_ref(&candidates[table.choice(shard_id, script)]),
            None => &candidates,
        };
        let mut replies = Vec::with_capacity(queried.len());
        for &row in queried {
            let (_, within) = transparent_shard::layout::split_row(row, geometry.directory_rows);
            let directory_rows = clients.directory.rows();
            let answer = clients.directory.fetch_row(
                transport,
                shard_id,
                revision,
                Table::Directory,
                entry.directory_segments,
                directory_rows,
                within as usize,
                charges,
            );
            replies.push((row, answer?));
        }
        let found = match entry_in(entry, manifest, geometry, &wanted, &replies) {
            Ok(found) => found,
            Err(SyncError::Content(_)) => {
                failed_here.push(script.clone());
                continue;
            }
            Err(other) => return Err(other),
        };
        match found {
            // A filter match with no directory entry is either a false positive
            // or a script outside the private tables' coverage. Both are
            // wasted work rather than errors; coverage still advances.
            None => covered.push(script.clone()),
            Some(found) => {
                for event in &found.inline {
                    events.push(StoredEvent {
                        script: script.clone(),
                        event: *event,
                        shard_id,
                        revision_digest: revision.to_string(),
                    });
                }
                match found.head() {
                    Some((child, row)) => pending_upsert.push(PendingPages {
                        id: None,
                        shard_id,
                        revision_digest: revision.to_string(),
                        script: script.clone(),
                        cursor: RunCursor {
                            child,
                            row,
                            older_runs: None,
                        },
                        fragments: Vec::new(),
                        carried: found.inline.clone(),
                        attempts: 0,
                        target_anchor: Some(target_anchor.clone()),
                    }),
                    None => covered.push(script.clone()),
                }
            }
        }
    }

    // The directory phase is one commit: inline events, coverage for every
    // script that needs no pages, and the walk owed for the rest. The walks
    // that follow resume from the store even if nothing else of this call
    // survives.
    let pending_ids_before: std::collections::BTreeSet<u64> =
        store.pending()?.iter().filter_map(|p| p.id).collect();
    commit_bounded(
        store,
        target_anchor,
        ShardCommit {
            source_anchor: None,
            shard_id,
            revision_digest: revision.to_string(),
            sealed: entry.sealed,
            start_height: entry.start_height,
            end_height: entry.end_height,
            terminal_block_hash: entry.terminal_block_hash.clone(),
            events,
            covered_scripts: covered,
            pending_upsert,
            pending_complete: Vec::new(),
            cached_runs: Vec::new(),
            failed_entries: failed_here.clone(),
        },
    )?;
    for script in &failed_here {
        record_failure(store, &mut failures.entries, shard_id, script)?;
    }
    let owed: Vec<PendingPages> = store
        .pending()?
        .into_iter()
        .filter(|p| {
            p.shard_id == shard_id
                && p.revision_digest == revision
                && p.id.is_some_and(|id| !pending_ids_before.contains(&id))
        })
        .collect();
    if owed.is_empty() {
        return Ok(None);
    }
    finish_pages(
        store,
        entry,
        manifest,
        &owed,
        clients,
        transport,
        charges,
        limits,
        target_anchor,
        chain,
        failures,
    )
}

/// The entry tagged `wanted` among the replies to one script's candidate
/// rows, if there is one, checked as far as it can be before a page is read:
/// every reply decodes, the tag is in at most one row, the inline events lie
/// in the shard, and the run it points to is a row a page query can select.
/// Any of these failing is a content failure of the entry.
fn entry_in(
    entry: &transparent_filter::ShardMapEntry,
    manifest: &ShardManifest,
    geometry: &transparent_shard::layout::Geometry,
    wanted: &[u8],
    replies: &[(u64, Reply)],
) -> Result<Option<DirectoryEntry>, SyncError> {
    let shard_id = entry.shard_id;
    let children = manifest.page_children.len() as u32;
    let mut found: Option<(u64, DirectoryEntry)> = None;
    for (row, reply) in replies {
        for raw in reply {
            let decoded = transparent_shard::records::decode_directory_row(raw, children)
                .map_err(|error| SyncError::Content(format!("shard {shard_id}: {error}")))?;
            for candidate in decoded {
                // The row is selected by a hash of the raw script. The tag
                // derived from this manifest's salt is what settles a hit.
                // A tag from any other salt does not match.
                if candidate.tag[..] != *wanted {
                    continue;
                }
                match &found {
                    // The two candidate hashes can name the same row. The row
                    // is still fetched twice, so the query count does not
                    // depend on the script, and the second sighting is the
                    // same entry, not a second.
                    Some((seen_row, _)) if seen_row == row => {}
                    Some(_) => {
                        return Err(SyncError::Content(format!(
                            "shard {shard_id} holds a script tag twice"
                        )));
                    }
                    None => found = Some((*row, candidate)),
                }
            }
        }
    }
    let Some((_, found)) = found else {
        return Ok(None);
    };
    if found.inline.iter().any(|event| {
        let height = event_height(event);
        height < entry.start_height || height > entry.end_height
    }) {
        return Err(SyncError::Content(format!(
            "shard {shard_id} holds an inline event outside its range"
        )));
    }
    if let Some((child, row)) = found.head() {
        locate(manifest, geometry, shard_id, child, u64::from(row))?;
    }
    Ok(Some(found))
}

/// Walks the chains still owed for one directory shard revision, privately.
///
/// Every query of `pages-<j>` selects among that page shard's first
/// [`PageChild::query_rows`]. See [`walk_chains`] for the walk itself.
#[allow(clippy::too_many_arguments)]
fn finish_pages<S: WalletStore, T: ShardTransport>(
    store: &mut S,
    entry: &transparent_filter::ShardMapEntry,
    manifest: &ShardManifest,
    owed: &[PendingPages],
    clients: &mut GeometryClients,
    transport: &mut T,
    charges: &mut ByteCharges,
    limits: &WorkLimits,
    target_anchor: &Anchor,
    chain: &impl ChainView,
    failures: &mut Failures,
) -> Result<Option<Completion>, SyncError> {
    if entry.end_height < target_anchor.height {
        match chain.is_accepted(entry.end_height, &entry.terminal_block_hash) {
            Acceptance::Accepted => {}
            Acceptance::Unknown => {
                return Ok(Some(Completion::Incomplete {
                    reason: IncompleteReason::ChainUnknown {
                        height: entry.end_height,
                    },
                    pending: store.pending()?.len(),
                }))
            }
            Acceptance::Rejected => {
                return Err(SyncError::Invalid(
                    "pending shard endpoint rejected by wallet chain".into(),
                ))
            }
        }
    }
    let mut rows = PirRows {
        entry,
        manifest,
        client: &mut clients.pages,
        transport,
        charges,
        limits,
    };
    walk_chains(
        store,
        entry,
        manifest,
        owed,
        &mut rows,
        target_anchor,
        failures,
    )
}

/// One page row a walk asks for: page shard `child`, and a row over its
/// logical row space, which is its segments concatenated.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RowRequest {
    child: u32,
    row: u64,
}

/// One private reply: the row from every table segment.
///
/// A reply that does not verify as a whole (its length, its revision binding,
/// the parameter epoch it was made under) fails its request, not an entry:
/// the service produces those without knowing which row the query selects,
/// so they cannot be aimed at one. Only the rows' own content, decoded and
/// checked, can fail an entry.
type Reply = Vec<Vec<u8>>;

/// Where a walk's page rows come from: privately, or in tests, directly.
trait RowSource {
    /// Why the walk must stop before another round, if it must.
    fn stopped(&self) -> Option<IncompleteReason>;
    /// Replies for a prefix of the requests, in order. A budget may stop
    /// dispatch between requests; each reply contains every table segment.
    fn fetch<S: WalletStore>(
        &mut self,
        store: &mut S,
        requests: &[RowRequest],
    ) -> Result<Vec<Reply>, SyncError>;
    /// A query whose reply is not read: row 0 of page shard `child`. Every
    /// query to a table selects among the same rows, so it is the length of
    /// any other query to that table, and the service cannot tell it apart.
    fn dummy(&self, child: u32) -> RowRequest {
        RowRequest { child, row: 0 }
    }
}

/// Page rows by private query against one directory shard revision.
struct PirRows<'a, T: ShardTransport> {
    entry: &'a transparent_filter::ShardMapEntry,
    manifest: &'a ShardManifest,
    client: &'a mut TableClient,
    transport: &'a mut T,
    charges: &'a mut ByteCharges,
    limits: &'a WorkLimits,
}

impl<T: ShardTransport> RowSource for PirRows<'_, T> {
    fn stopped(&self) -> Option<IncompleteReason> {
        budget_stopped(self.charges, self.limits)
    }

    /// Sends a round's queries together when the transport overlaps
    /// requests, one at a time otherwise. Which queries are sent is the
    /// walk's alone; a batch changes only when.
    fn fetch<S: WalletStore>(
        &mut self,
        store: &mut S,
        requests: &[RowRequest],
    ) -> Result<Vec<Reply>, SyncError> {
        let shard_id = self.entry.shard_id;
        let revision = self.entry.manifest_digest.as_str();
        let geometry = transparent_shard::layout::by_name(&self.entry.geometry)
            .ok_or_else(|| SyncError::UnknownGeometry(self.entry.geometry.clone()))?;
        let set_digest = store
            .set_identity()?
            .map(|identity| identity.digest())
            .unwrap_or_default();
        if self.limits.max_private_bytes.is_none() && self.transport.concurrency() > 1 {
            let remaining = self
                .limits
                .max_queries
                .map(|limit| limit.saturating_sub(self.charges.queries()) as usize)
                .unwrap_or(requests.len());
            let requests = &requests[..requests.len().min(remaining)];
            // Each request: its table, segment count, query rows and row within.
            let mut planned = Vec::with_capacity(requests.len());
            for request in requests {
                let child = self
                    .manifest
                    .child(request.child)
                    .ok_or_else(|| SyncError::Invalid("a walk left the page shards".into()))?;
                let table = Table::Pages(request.child);
                let segments = child.page_segments.len() as u32;
                open_table(
                    store,
                    &set_digest,
                    shard_id,
                    revision,
                    table,
                    segments,
                    self.client,
                    self.transport,
                    self.charges,
                    None,
                )?;
                let (_, within) =
                    transparent_shard::layout::split_row(request.row, geometry.page_rows);
                planned.push((
                    table,
                    segments,
                    child.query_rows() as usize,
                    within as usize,
                ));
            }
            let mut answers = Vec::with_capacity(planned.len());
            if self.transport.concurrency() <= 1 || planned.len() <= 1 {
                for (table, segments, query_rows, within) in planned {
                    answers.push(self.client.fetch_row(
                        self.transport,
                        shard_id,
                        revision,
                        table,
                        segments,
                        query_rows,
                        within,
                        self.charges,
                    )?);
                }
                return Ok(answers);
            }
            let mut queries = Vec::with_capacity(planned.len());
            for (table, _, query_rows, within) in &planned {
                queries.push(
                    self.client
                        .prepare_prefix(revision, *table, *query_rows, *within)?,
                );
            }
            let batch: Vec<ShardRequest<'_>> = planned
                .iter()
                .zip(&queries)
                .map(|((table, ..), query)| ShardRequest::Query {
                    shard_id,
                    revision,
                    table: *table,
                    body: &query.body,
                })
                .collect();
            let replies = self.transport.batch(&batch);
            drop(batch);
            for (((table, segments, query_rows, within), query), reply) in
                planned.into_iter().zip(queries).zip(replies)
            {
                match reply {
                    Some(Ok((response, _))) => {
                        self.charges.add_query(
                            table,
                            query.body.len() as u64,
                            response.len() as u64,
                        );
                        answers.push(
                            self.client
                                .decode(revision, table, segments, query, &response)?,
                        );
                    }
                    Some(Err(error)) => return Err(SyncError::Client(classify_transport(error))),
                    None => answers.push(self.client.fetch_row(
                        self.transport,
                        shard_id,
                        revision,
                        table,
                        segments,
                        query_rows,
                        within,
                        self.charges,
                    )?),
                }
            }
            return Ok(answers);
        }
        let mut answers = Vec::with_capacity(requests.len());
        // A finite byte allowance is checked after each atomic response. Opening
        // all setups or dispatching a whole batch first would exceed that contract.
        for request in requests {
            if self.stopped().is_some() {
                break;
            }
            let child = self
                .manifest
                .child(request.child)
                .ok_or_else(|| SyncError::Invalid("a walk left the page shards".into()))?;
            let table = Table::Pages(request.child);
            let segments = child.page_segments.len() as u32;
            if !open_table(
                store,
                &set_digest,
                shard_id,
                revision,
                table,
                segments,
                self.client,
                self.transport,
                self.charges,
                Some(self.limits),
            )? {
                break;
            }
            if self.stopped().is_some() {
                break;
            }
            let (_, within) = transparent_shard::layout::split_row(request.row, geometry.page_rows);
            answers.push(self.client.fetch_row(
                self.transport,
                shard_id,
                revision,
                table,
                segments,
                child.query_rows() as usize,
                within as usize,
                self.charges,
            )?);
        }
        Ok(answers)
    }
}

/// The history cached below a run, and the page shard of the cached run it
/// starts from.
type CachedBelow = (u8, Vec<TransparentEvent>);

/// One script's chain walk in progress.
struct Walk {
    item: PendingPages,
    /// The first fragment of the run at the cursor, once read.
    head: Option<PageEntry>,
    /// Runs read and committed in this call, newest first, with their page
    /// shard: what the run cache is filled from once the walk ends.
    walked: Vec<(u8, Vec<TransparentEvent>)>,
    done: bool,
    /// After a content failure: the page shard and number of queries the
    /// walk had committed to and not sent, sent as dummies in the next round,
    /// after which the walk ends.
    dummies: Option<(u8, u32)>,
}

/// A walk's failure, with the queries valid replies had already committed it
/// to: `dummies` of them, to page shard `child`.
struct Failure {
    error: SyncError,
    child: u8,
    dummies: u32,
}

impl From<SyncError> for Failure {
    fn from(error: SyncError) -> Self {
        Self {
            error,
            child: 0,
            dummies: 0,
        }
    }
}

/// Walks the chains of runs `owed` describes, all scripts' rounds together,
/// newest run first, committing each run once it is read and checked.
///
/// A round reads each run's first fragment or a prefix of its remaining
/// fragments. Checked fragments are saved between passes, so a one-query
/// budget can advance even a long run. A complete run is committed before
/// checking the next request's budget. A walk reaching a cached sealed run
/// stops there and takes that history.
///
/// Refused, as content failures: a link to the same or a later page shard;
/// an `older_runs` that does not drop by exactly one per hop, which is how a
/// chain cut short or extended shows; fragments out of ordinal, count or
/// greedy boundary; events not strictly increasing from the oldest run into
/// the inline events; and an event in a page shard other than the one open
/// when it was pushed out — the event two places after it must lie in that
/// page shard's range.
///
/// A content failure fails its walk alone; the other walks finish. The failed
/// walk keeps its pending item, so a later sync retries it, and the failure is
/// counted against the entry. It still sends what valid replies had already
/// committed it to, the fragments of its current run not yet asked for, as
/// dummy queries to that run's page shard, and then nothing. A run whose link
/// names a page shard the manifest does not declare, or a row outside that
/// table, sends one dummy to its own page shard in the link's place. Nothing
/// that depends on the bad reply is sent.
fn walk_chains<S: WalletStore, R: RowSource>(
    store: &mut S,
    entry: &transparent_filter::ShardMapEntry,
    manifest: &ShardManifest,
    owed: &[PendingPages],
    rows: &mut R,
    target_anchor: &Anchor,
    failures: &mut Failures,
) -> Result<Option<Completion>, SyncError> {
    let geometry = transparent_shard::layout::by_name(&entry.geometry)
        .ok_or_else(|| SyncError::UnknownGeometry(entry.geometry.clone()))?;
    let identities: Vec<String> = manifest.child_digests().iter().map(hex::encode).collect();
    let salts = manifest
        .page_children
        .iter()
        .map(|child| {
            let terminal = BlockHash::from_display_hex(&child.terminal_block_hash)?;
            Ok(transparent_shard::page_tag_salt(
                manifest.shard_id,
                child.index,
                child.end_height,
                &terminal,
                child.tag_salt_counter,
            ))
        })
        .collect::<Result<Vec<[u8; 32]>, SyncError>>()?;
    let walker = Walker {
        entry,
        manifest,
        geometry,
        identities: &identities,
        salts: &salts,
        target_anchor,
    };
    let mut walks: Vec<Walk> = Vec::with_capacity(owed.len());
    for item in owed {
        let mut walk = Walk {
            item: item.clone(),
            head: None,
            walked: Vec::new(),
            done: false,
            dummies: None,
        };
        if let Some(owed) = failures.owes(entry.shard_id, &item.script) {
            // Failed earlier in this sync, before a retry: it still sends
            // what it owed.
            walk.dummies = Some(owed);
        } else if let Some(raw) = item.fragments.first() {
            let head = walker
                .fragment(
                    &item.script,
                    item.cursor.child,
                    std::slice::from_ref(raw),
                    0,
                    None,
                )
                .map_err(Failure::from)
                .and_then(|head| {
                    walker.check_located(&head, &item.cursor, item.fragments.len())?;
                    Ok(head)
                });
            match head {
                Ok(head) => walk.head = Some(head),
                Err(failure) => walker.fail(store, &mut walk, failures, failure)?,
            }
        }
        walks.push(walk);
    }
    loop {
        // A walk at a run the store holds history for stops there.
        for walk in walks
            .iter_mut()
            .filter(|w| !w.done && w.dummies.is_none() && w.head.is_none())
        {
            let child = walk.item.cursor.child;
            let stopped = match walker.cached(store, &walk.item.script, child) {
                Ok(Some(older)) => walker.finish(store, walk, Vec::new(), Some((child, older))),
                Ok(None) => Ok(()),
                Err(error) => Err(error),
            };
            if let Err(error) = stopped {
                walker.fail(store, walk, failures, error.into())?;
            }
        }
        if walks.iter().all(|walk| walk.done) {
            return Ok(None);
        }
        if rows.stopped().is_some() {
            for walk in walks.iter_mut().filter(|w| !w.done && w.dummies.is_none()) {
                if walk
                    .head
                    .as_ref()
                    .is_some_and(|head| walk.item.fragments.len() == head.fragment_count as usize)
                {
                    if let Err(failure) = walker.complete(store, walk) {
                        walker.fail(store, walk, failures, failure)?;
                    }
                }
            }
            if walks.iter().all(|walk| walk.done) {
                return Ok(None);
            }
        }
        if let Some(reason) = rows.stopped() {
            return Ok(Some(Completion::Incomplete {
                reason,
                pending: store.pending()?.len(),
            }));
        }

        // This round's rows, every walk's together.
        let mut requests: Vec<RowRequest> = Vec::new();
        let mut spans: Vec<(usize, usize, Option<CachedBelow>)> = Vec::new();
        for walk in walks.iter_mut() {
            let start = requests.len();
            let mut older = None;
            if walk.dummies.is_none() && !walk.done {
                match walker.plan(store, walk) {
                    Ok((planned, below)) => {
                        requests.extend(planned);
                        older = below;
                    }
                    Err(error) => walker.fail(store, walk, failures, error.into())?,
                }
            }
            if let Some((child, count)) = walk.dummies {
                requests.extend((0..count).map(|_| rows.dummy(u32::from(child))));
            }
            spans.push((start, requests.len(), older));
        }
        let answers = rows.fetch(store, &requests)?;
        if answers.len() > requests.len()
            || answers.is_empty() && !requests.is_empty() && rows.stopped().is_none()
        {
            return Err(SyncError::Invalid("a round lost an answer".into()));
        }
        for (walk, (start, end, older)) in walks.iter_mut().zip(spans) {
            if walk.done {
                continue;
            }
            // A failed walk's dummies were this round's; their replies are
            // not read, and the walk ends.
            if walk.dummies.take().is_some() {
                failures.settle(entry.shard_id, &walk.item.script);
                walk.done = true;
                continue;
            }
            if start >= answers.len() && start != end {
                continue;
            }
            let available = if start == end {
                &[][..]
            } else {
                &answers[start..end.min(answers.len())]
            };
            if let Err(failure) = walker.advance(store, walk, available, older) {
                walker.fail(store, walk, failures, failure)?;
            }
        }
    }
}

/// What a walk checks against, for one directory shard revision.
struct Walker<'a> {
    entry: &'a transparent_filter::ShardMapEntry,
    manifest: &'a ShardManifest,
    geometry: &'static transparent_shard::layout::Geometry,
    /// Each page shard's chained identity, in hex: the run cache's key.
    identities: &'a [String],
    /// Each page shard's tag salt.
    salts: &'a [[u8; 32]],
    target_anchor: &'a Anchor,
}

/// Page shard `child` of a directory shard, which its manifest must declare.
fn page_child(manifest: &ShardManifest, shard_id: u64, child: u8) -> Result<&PageChild, SyncError> {
    manifest.child(u32::from(child)).ok_or_else(|| {
        SyncError::Content(format!(
            "shard {shard_id} names page shard {child}, which it does not declare"
        ))
    })
}

/// The request for `row` of page shard `child`, if the row is one a query of
/// that table can select. A pointer is a claim like any other.
fn locate(
    manifest: &ShardManifest,
    geometry: &transparent_shard::layout::Geometry,
    shard_id: u64,
    child: u8,
    row: u64,
) -> Result<RowRequest, SyncError> {
    let page = page_child(manifest, shard_id, child)?;
    if page.page_segments.is_empty() {
        return Err(SyncError::Content(format!(
            "shard {shard_id} points into page shard {child}, which holds no rows"
        )));
    }
    let space = geometry.page_rows * page.page_segments.len() as u64;
    let (_, within) = transparent_shard::layout::split_row(row, geometry.page_rows);
    if row >= space || within >= page.query_rows() {
        return Err(SyncError::Content(format!(
            "shard {shard_id} page {row} is outside page shard {child}'s table"
        )));
    }
    Ok(RowRequest {
        child: u32::from(child),
        row,
    })
}

impl Walker<'_> {
    fn child(&self, child: u8) -> Result<&PageChild, SyncError> {
        page_child(self.manifest, self.entry.shard_id, child)
    }

    /// The request for `row` of page shard `child`; see [`locate`].
    fn locate(&self, child: u8, row: u64) -> Result<RowRequest, SyncError> {
        locate(
            self.manifest,
            self.geometry,
            self.entry.shard_id,
            child,
            row,
        )
    }

    /// Checks a run's first fragment, read at `cursor`, before the walk
    /// relies on it: every fragment it declares must be a row a page query
    /// can select, or the run is not located at all. Then its link: the
    /// older-run count its chain promised (see [`check_head`]) and a run to
    /// link to that starts at a row of an earlier, declared page shard. A
    /// link that fails any of these still leaves this run located, so the
    /// walk owes the queries for its fragments not yet held, of `held`, and
    /// one in the link's place, all as dummies: the round the honest walk
    /// would send.
    fn check_located(
        &self,
        head: &PageEntry,
        cursor: &RunCursor,
        held: usize,
    ) -> Result<(), Failure> {
        let last = u64::from(cursor.row) + u64::from(head.fragment_count.max(1)) - 1;
        self.locate(cursor.child, last)?;
        let link = check_head(head, cursor).and_then(|()| match head.link.previous() {
            Some((child, row)) => self.locate(child, u64::from(row)).map(|_| ()),
            None => Ok(()),
        });
        link.map_err(|error| Failure {
            error,
            child: cursor.child,
            dummies: head.fragment_count.saturating_sub(held.max(1) as u32) + 1,
        })
    }

    /// One round's requests for a walk, and the history cached below its
    /// run when the walk will stop there.
    fn plan<S: WalletStore>(
        &self,
        store: &S,
        walk: &Walk,
    ) -> Result<(Vec<RowRequest>, Option<CachedBelow>), SyncError> {
        let cursor = walk.item.cursor;
        let Some(head) = &walk.head else {
            return Ok((
                vec![self.locate(cursor.child, u64::from(cursor.row))?],
                None,
            ));
        };
        let mut requests = Vec::new();
        for ordinal in walk.item.fragments.len() as u32..head.fragment_count {
            let row = u64::from(cursor.row) + u64::from(ordinal);
            requests.push(self.locate(cursor.child, row)?);
        }
        let mut older = None;
        if let Some((child, row)) = head.link.previous() {
            older = self
                .cached(store, &walk.item.script, child)?
                .map(|events| (child, events));
            if older.is_none() {
                requests.push(self.locate(child, u64::from(row))?);
            }
        }
        Ok((requests, older))
    }

    /// The history cached for `script` from its run in page shard `child`
    /// back to the start of the directory shard, when that page shard is
    /// sealed. Each cached run holds only its own events and names the cached
    /// run below it, so the history is put back together by following those
    /// names; a missing run anywhere along the way is a miss.
    fn cached<S: WalletStore>(
        &self,
        store: &S,
        script: &[u8],
        child: u8,
    ) -> Result<Option<Vec<TransparentEvent>>, SyncError> {
        if !self.child(child)?.sealed {
            return Ok(None);
        }
        let corrupt = |why: &str| SyncError::Store(StoreError::Corrupt(why.into()));
        let mut index = usize::from(child);
        let mut runs = Vec::new();
        loop {
            let Some(run) = store.cached_run(&self.identities[index], script)? else {
                return Ok(None);
            };
            if run.events.is_empty() {
                return Err(corrupt("a cached run holds no events"));
            }
            runs.push(run.events);
            let Some(older) = run.older else { break };
            // The identity of a page shard covers every older one, so the run
            // below must be in an earlier page shard of this directory shard.
            index = self.identities[..index]
                .iter()
                .position(|identity| *identity == older)
                .ok_or_else(|| {
                    corrupt("a cached run names a run outside the page shards below it")
                })?;
        }
        let mut history: Vec<TransparentEvent> = Vec::new();
        for run in runs.into_iter().rev() {
            if let (Some(last), Some(first)) = (history.last(), run.first()) {
                if last.sort_key() >= first.sort_key() {
                    return Err(corrupt("cached runs are not in order"));
                }
            }
            history.extend(run);
        }
        Ok(Some(history))
    }

    /// The one fragment of `script`'s run in page shard `child` with this
    /// ordinal, among every segment's row. Exactly one may claim it.
    fn fragment(
        &self,
        script: &[u8],
        child: u8,
        answers: &[Vec<u8>],
        ordinal: u32,
        count: Option<u32>,
    ) -> Result<PageEntry, SyncError> {
        let shard_id = self.entry.shard_id;
        let wanted = transparent_shard::script_tag(&self.salts[usize::from(child)], script);
        // A packed row also carries other scripts' histories. Those are
        // discarded, and deliberately not used: two of a wallet's own scripts
        // sharing a row still cost two fetches, so the request transcript does
        // not depend on which scripts share a row.
        let mut found = None;
        for raw in answers {
            let decoded = transparent_shard::page_row::decode_page_row(raw).map_err(|error| {
                SyncError::Content(format!("shard {shard_id} page shard {child}: {error}"))
            })?;
            for candidate in decoded {
                if candidate.tag != wanted || candidate.ordinal != ordinal {
                    continue;
                }
                if count.is_some_and(|count| candidate.fragment_count != count) {
                    return Err(SyncError::Content(format!(
                        "shard {shard_id} page shard {child}: fragment {ordinal} disagrees with \
                         its run's fragment count"
                    )));
                }
                if found.is_some() {
                    return Err(SyncError::Content(format!(
                        "shard {shard_id} page shard {child} holds fragment {ordinal} twice"
                    )));
                }
                found = Some(candidate);
            }
        }
        found.ok_or_else(|| {
            SyncError::Content(format!(
                "shard {shard_id} page shard {child}: the row a link located does not hold \
                 fragment {ordinal} of the run"
            ))
        })
    }

    /// Takes one walk through one round's replies.
    fn advance<S: WalletStore>(
        &self,
        store: &mut S,
        walk: &mut Walk,
        answers: &[Reply],
        older: Option<CachedBelow>,
    ) -> Result<(), Failure> {
        let cursor = walk.item.cursor;
        let script = walk.item.script.clone();
        if walk.head.is_none() {
            let [answer] = answers else {
                return Err(SyncError::Invalid("a round lost an answer".into()).into());
            };
            let head = self.fragment(&script, cursor.child, answer, 0, None)?;
            self.check_located(&head, &cursor, 1)?;
            walk.item.fragments.push(encode_fragment(&head)?);
            walk.head = Some(head);
            return Ok(self.commit(
                store,
                &script,
                Vec::new(),
                Some(walk.item.clone()),
                None,
                Vec::new(),
            )?);
        }
        let head = walk.head.as_ref().expect("head decoded above").clone();
        let count = head.fragment_count as usize;
        if walk.item.fragments.len() > count {
            return Err(SyncError::Invalid("a run has too many fragments".into()).into());
        }
        let remaining = count - walk.item.fragments.len();
        let (answers, next) = answers.split_at(remaining.min(answers.len()));
        if next.len() > 1 {
            return Err(SyncError::Invalid("a round has extra answers".into()).into());
        }
        // The run's fragments this round asked for and a budget left unsent:
        // what the walk still owes if this run fails.
        let unsent = (remaining - answers.len()) as u32;
        let failing = |error: SyncError| Failure {
            error,
            child: cursor.child,
            dummies: unsent,
        };
        let mut boundary = validate_page_boundary(None, &head.events).map_err(failing)?;
        let mut run = head.events.clone();
        // Recheck saved boundaries before using them after a restart.
        for (ordinal, raw) in walk.item.fragments.iter().enumerate().skip(1) {
            let fragment = self
                .fragment(
                    &script,
                    cursor.child,
                    std::slice::from_ref(raw),
                    ordinal as u32,
                    Some(head.fragment_count),
                )
                .map_err(failing)?;
            boundary =
                validate_page_boundary(Some(&boundary), &fragment.events).map_err(failing)?;
            run.extend(fragment.events);
        }
        for answer in answers {
            let ordinal = walk.item.fragments.len() as u32;
            let fragment = self
                .fragment(
                    &script,
                    cursor.child,
                    answer,
                    ordinal,
                    Some(head.fragment_count),
                )
                .map_err(failing)?;
            boundary =
                validate_page_boundary(Some(&boundary), &fragment.events).map_err(failing)?;
            run.extend(fragment.events.iter().copied());
            walk.item.fragments.push(encode_fragment(&fragment)?);
        }
        if walk.item.fragments.len() < count {
            return Ok(self.commit(
                store,
                &script,
                Vec::new(),
                Some(walk.item.clone()),
                None,
                Vec::new(),
            )?);
        }
        check_run(self.child(cursor.child)?, &run, &walk.item.carried)?;
        walk.walked.push((cursor.child, run.clone()));
        match head.link.previous() {
            None => Ok(self.finish(store, walk, run, None)?),
            Some(_) if older.is_some() => Ok(self.finish(store, walk, run, older)?),
            Some((child, row)) => {
                let mut following = run.clone();
                following.extend(walk.item.carried.iter().copied());
                following.truncate(2);
                let mut progressed = walk.item.clone();
                progressed.cursor = RunCursor {
                    child,
                    row,
                    older_runs: Some(head.link.older_runs - 1),
                };
                progressed.carried = following;
                progressed.fragments.clear();
                // A bad head for the next run fails the walk, but not the run
                // already checked: that is committed, and the walk resumes at
                // the next run's head.
                let mut failure = None;
                let mut next_head = None;
                if let [answer] = next {
                    let checked = self
                        .fragment(&script, child, answer, 0, None)
                        .map_err(Failure::from)
                        .and_then(|head| {
                            self.check_located(&head, &progressed.cursor, 1)?;
                            Ok(head)
                        });
                    match checked {
                        Ok(head) => {
                            progressed.fragments.push(encode_fragment(&head)?);
                            next_head = Some(head);
                        }
                        Err(error) => failure = Some(error),
                    }
                }
                progressed.attempts += 1;
                self.commit(
                    store,
                    &script,
                    run,
                    Some(progressed.clone()),
                    None,
                    Vec::new(),
                )?;
                walk.item = progressed;
                walk.head = next_head;
                failure.map_or(Ok(()), Err)
            }
        }
    }

    /// Takes a walk whose run is read in full through to its end, with no
    /// replies: what a stopped round still finishes.
    fn complete<S: WalletStore>(&self, store: &mut S, walk: &mut Walk) -> Result<(), Failure> {
        let older = match walk.head.as_ref().and_then(|head| head.link.previous()) {
            Some((child, _)) => self
                .cached(store, &walk.item.script, child)?
                .map(|events| (child, events)),
            None => None,
        };
        self.advance(store, walk, &[], older)
    }

    /// Ends a walk after `failure`. A content failure is counted against the
    /// walk's entry, which keeps its pending item for a later sync, and the
    /// walk sends the dummies it owes in the next round, if any; any other
    /// failure is the call's.
    fn fail<S: WalletStore>(
        &self,
        store: &mut S,
        walk: &mut Walk,
        failures: &mut Failures,
        failure: Failure,
    ) -> Result<(), SyncError> {
        let Failure {
            error,
            child,
            dummies,
        } = failure;
        let SyncError::Content(_) = error else {
            return Err(error);
        };
        let entry = self.entry;
        commit_bounded(
            store,
            self.target_anchor,
            ShardCommit {
                source_anchor: None,
                shard_id: entry.shard_id,
                revision_digest: entry.manifest_digest.clone(),
                sealed: entry.sealed,
                start_height: entry.start_height,
                end_height: entry.end_height,
                terminal_block_hash: entry.terminal_block_hash.clone(),
                failed_entries: vec![walk.item.script.clone()],
                ..Default::default()
            },
        )?;
        record_failure(
            store,
            &mut failures.entries,
            entry.shard_id,
            &walk.item.script,
        )?;
        if dummies > 0 {
            walk.dummies = Some((child, dummies));
            failures
                .owed
                .push((entry.shard_id, walk.item.script.clone(), child, dummies));
        } else {
            walk.done = true;
        }
        Ok(())
    }

    /// Ends a walk: `run` is the last run read, `older` the page shard and
    /// history cached below it, if the walk stopped at a cached run. Fills the
    /// run cache for every run this call read in a sealed page shard: each
    /// with its own events and the name of the cached run below it.
    fn finish<S: WalletStore>(
        &self,
        store: &mut S,
        walk: &mut Walk,
        run: Vec<TransparentEvent>,
        older: Option<CachedBelow>,
    ) -> Result<(), SyncError> {
        let (mut below, older) = match older {
            Some((child, events)) => (Some(self.identities[usize::from(child)].clone()), events),
            None => (None, Vec::new()),
        };
        let newer = run.first().or(walk.item.carried.first());
        if let (Some(last), Some(first)) = (older.last(), newer) {
            if last.sort_key() >= first.sort_key() {
                return Err(SyncError::Content(
                    "cached history is not older than the run that links to it".into(),
                ));
            }
        }
        let mut cached_runs = Vec::new();
        for (child, events) in walk.walked.iter().rev() {
            // Only the newest run can be in the open page shard, so every run
            // below a cached one is cached too.
            if !self.child(*child)?.sealed {
                break;
            }
            let identity = self.identities[usize::from(*child)].clone();
            cached_runs.push(CachedRun {
                child_digest: identity.clone(),
                script: walk.item.script.clone(),
                shard_id: self.entry.shard_id,
                events: events.clone(),
                older: below.replace(identity),
            });
        }
        let mut events = older;
        events.extend(run);
        let script = walk.item.script.clone();
        self.commit(store, &script, events, None, walk.item.id, cached_runs)?;
        walk.done = true;
        Ok(())
    }

    /// Commits a step of one walk: the events it read, and either the walk's
    /// next state or its end, with the script's coverage.
    fn commit<S: WalletStore>(
        &self,
        store: &mut S,
        script: &[u8],
        events: Vec<TransparentEvent>,
        progressed: Option<PendingPages>,
        finished: Option<u64>,
        cached_runs: Vec<CachedRun>,
    ) -> Result<(), SyncError> {
        let entry = self.entry;
        let done = progressed.is_none();
        commit_bounded(
            store,
            self.target_anchor,
            ShardCommit {
                source_anchor: None,
                shard_id: entry.shard_id,
                revision_digest: entry.manifest_digest.clone(),
                sealed: entry.sealed,
                start_height: entry.start_height,
                end_height: entry.end_height,
                terminal_block_hash: entry.terminal_block_hash.clone(),
                events: events
                    .into_iter()
                    .map(|event| StoredEvent {
                        script: script.to_vec(),
                        event,
                        shard_id: entry.shard_id,
                        revision_digest: entry.manifest_digest.clone(),
                    })
                    .collect(),
                covered_scripts: if done {
                    vec![script.to_vec()]
                } else {
                    Vec::new()
                },
                pending_upsert: progressed.into_iter().collect(),
                pending_complete: finished.into_iter().collect(),
                cached_runs,
                failed_entries: Vec::new(),
            },
        )?;
        Ok(())
    }
}

/// Checks a run's first fragment against the cursor that located it.
///
/// Its `older_runs` must be the one fewer the newer run promised — which is
/// what makes a chain cut short, or one that carries on past where it should
/// end, a refusal rather than a shorter or longer history. It may not claim
/// more older runs than there are earlier page shards, and its link must
/// point to an earlier page shard: never this one, never a later one.
fn check_head(head: &PageEntry, cursor: &RunCursor) -> Result<(), SyncError> {
    let link = head.link;
    match cursor.older_runs {
        Some(expected) if link.older_runs != expected => {
            return Err(SyncError::Content(format!(
                "a run in page shard {} reports {} older runs where its chain promised {expected}",
                cursor.child, link.older_runs
            )))
        }
        _ if link.older_runs > cursor.child => {
            return Err(SyncError::Content(format!(
                "a run in page shard {} reports {} older runs",
                cursor.child, link.older_runs
            )))
        }
        _ => {}
    }
    if let Some((child, _)) = link.previous() {
        if child >= cursor.child {
            return Err(SyncError::Content(format!(
                "a run in page shard {} links to page shard {child}, which is not earlier",
                cursor.child
            )));
        }
    }
    Ok(())
}

/// Checks a run against the two events after it and its page shard.
///
/// Events strictly increase from the run into `carried`. Each of the run's
/// events was pushed out when the event two places after it arrived, into the
/// page shard open then, so that event's height lies in this page shard's
/// range. A run placed in any other page shard fails here.
fn check_run(
    child: &PageChild,
    run: &[TransparentEvent],
    carried: &[TransparentEvent],
) -> Result<(), SyncError> {
    if run.is_empty() || carried.len() != transparent_shard::INLINE_EVENTS as usize {
        return Err(SyncError::Content(format!(
            "a run in page shard {} cannot be checked",
            child.index
        )));
    }
    let following: Vec<&TransparentEvent> = run.iter().chain(carried).collect();
    if following
        .windows(2)
        .any(|pair| pair[0].sort_key() >= pair[1].sort_key())
    {
        return Err(SyncError::Content(format!(
            "a run in page shard {} is not older than the history after it",
            child.index
        )));
    }
    for (index, _) in run.iter().enumerate() {
        let height = event_height(following[index + 2]);
        if height < child.start_height || height > child.end_height {
            return Err(SyncError::Content(format!(
                "an event in page shard {} was pushed out at height {height}, when that page \
                 shard was not open",
                child.index
            )));
        }
    }
    Ok(())
}

fn validate_page_boundary(
    previous: Option<&crate::store::PageBoundary>,
    events: &[transparent_events::TransparentEvent],
) -> Result<crate::store::PageBoundary, SyncError> {
    let first = events
        .first()
        .ok_or_else(|| SyncError::Content("empty fragment".into()))?;
    if let Some(previous) = previous {
        if previous.last_event.sort_key() > first.sort_key()
            || previous.event_bytes as usize
                + transparent_shard::compact::event_len(first, Some(&previous.last_event))
                <= transparent_shard::packing::FRAGMENT_PAYLOAD
        {
            return Err(SyncError::Content("noncanonical fragment boundary".into()));
        }
    }
    Ok(crate::store::PageBoundary {
        event_bytes: transparent_shard::compact::encoded_len(events) as u32,
        last_event: *events.last().expect("nonempty fragment"),
    })
}

/// Fetches the published setup for every segment of a shard's table, once,
/// reusing what the store holds under the exact set, revision and segment.
///
/// Each segment publishes its own `c1`, so each is opened separately; a shard
/// with one segment — the ordinary case — costs exactly one setup per table.
#[allow(clippy::too_many_arguments)]
fn open_table<S: WalletStore>(
    store: &mut S,
    set_digest: &str,
    shard_id: u64,
    revision: &str,
    table: Table,
    segments: u32,
    client: &mut TableClient,
    transport: &mut impl ShardTransport,
    charges: &mut ByteCharges,
    limits: Option<&WorkLimits>,
) -> Result<bool, SyncError> {
    if segments == 0 {
        return Err(SyncError::Invalid(format!(
            "shard {shard_id} declares no {} segments",
            table.name()
        )));
    }
    for segment in 0..segments {
        if client.is_open(revision, table, segment) {
            continue;
        }
        let key = SetupKey {
            set_digest: set_digest.to_string(),
            revision_digest: revision.to_string(),
            table,
            segment,
        };
        let (params, digest) = match store.setup(&key)? {
            Some(blob) => (blob.public_params_base64, blob.public_params_sha256),
            None => {
                if limits
                    .and_then(|limits| budget_stopped(charges, limits))
                    .is_some()
                {
                    return Ok(false);
                }
                let (raw, cost) = transport
                    .setup(shard_id, revision, table, segment)
                    .map_err(|error| SyncError::from(classify_transport(error)))?;
                charges.add_setup(table, cost);
                let parsed: serde_json::Value = serde_json::from_slice(&raw)
                    .map_err(|error| SyncError::Transport(error.to_string()))?;
                let params = parsed["public_params"]
                    .as_str()
                    .ok_or_else(|| SyncError::Invalid("setup has no public_params".into()))?
                    .to_string();
                let digest = parsed["public_params_sha256"]
                    .as_str()
                    .ok_or_else(|| SyncError::Invalid("setup has no digest".into()))?
                    .to_string();
                (params, digest)
            }
        };
        client.open_segment(revision, table, segment, &params, &digest)?;
        // Kept only after the client verified it against its digest.
        store.put_setup(
            &key,
            &SetupBlob {
                public_params_base64: params,
                public_params_sha256: digest,
            },
        )?;
    }
    Ok(true)
}

fn check_target_hash(map: &ShardMap, target: &Anchor) -> Result<(), SyncError> {
    if map
        .shard_for_height(target.height)
        .is_some_and(|s| s.end_height == target.height && s.terminal_block_hash != target.hash)
    {
        return Err(SyncError::Invalid(
            "publication endpoint disagrees with wallet-accepted target hash".into(),
        ));
    }
    Ok(())
}

/// Commits only the wallet-accepted prefix while retaining the publication identity.
fn commit_bounded<S: WalletStore>(
    store: &mut S,
    target: &Anchor,
    mut commit: ShardCommit,
) -> Result<u64, SyncError> {
    let source = Anchor {
        height: commit.end_height,
        hash: commit.terminal_block_hash.clone(),
    };
    for event in commit
        .events
        .iter()
        .map(|e| &e.event)
        .chain(commit.pending_upsert.iter().flat_map(|p| p.carried.iter()))
    {
        let height = event_height(event);
        if height < commit.start_height || height > source.height {
            return Err(SyncError::Content(
                "event lies outside its published shard".into(),
            ));
        }
    }
    commit.source_anchor = Some(source);
    if commit.end_height >= target.height {
        commit.end_height = target.height;
        commit.terminal_block_hash = target.hash.clone();
    }
    // Events above the target are not kept. A walk's carried events are not
    // history but what it checks the next run against, so they stay whole.
    commit
        .events
        .retain(|e| event_height(&e.event) <= target.height);
    // What a reply carried contradicting what the store holds is the reply's
    // content failure, not the store's.
    store.commit_shard(commit).map_err(|error| match error {
        StoreError::Contradiction(why) => SyncError::Content(format!("contradiction: {why}")),
        other => SyncError::Store(other),
    })
}

/// A checked fragment, saved as a page row holding only it.
fn encode_fragment(fragment: &PageEntry) -> Result<Vec<u8>, SyncError> {
    Ok(transparent_shard::page_row::encode_page_row(
        std::slice::from_ref(fragment),
    )?)
}

/// A sync's content failures so far: the entries, and the dummy queries
/// failed walks still owe. A shard read retried after an overload sends what
/// is still owed before the walk ends.
#[derive(Default)]
struct Failures {
    entries: Vec<FailedEntry>,
    /// Shard id, script, page shard and the number of dummies owed.
    owed: Vec<(u64, Vec<u8>, u8, u32)>,
}

impl Failures {
    /// The dummies the walk of `script`'s entry in `shard_id` still owes.
    fn owes(&self, shard_id: u64, script: &[u8]) -> Option<(u8, u32)> {
        self.owed
            .iter()
            .find(|(shard, owner, ..)| *shard == shard_id && owner == script)
            .map(|&(_, _, child, count)| (child, count))
    }

    /// Records that the dummies `owes` names were sent.
    fn settle(&mut self, shard_id: u64, script: &[u8]) {
        self.owed
            .retain(|(shard, owner, ..)| !(*shard == shard_id && owner == script));
    }
}

/// Whether `script`'s entry in shard `shard_id` is held out of this sync:
/// it failed on content earlier in the sync, or has failed
/// [`MAX_CONTENT_FAILURES`] times. A held entry is not queried at all.
fn held<S: WalletStore>(
    store: &S,
    failed: &[FailedEntry],
    shard_id: u64,
    script: &[u8],
) -> Result<bool, SyncError> {
    Ok(failed
        .iter()
        .any(|entry| entry.shard_id == shard_id && entry.script == script)
        || store.entry_failures(shard_id, script)? >= MAX_CONTENT_FAILURES)
}

/// [`held`], listing an entry held for its failure count in the report the
/// first time this sync meets it.
fn hold<S: WalletStore>(
    store: &S,
    failed: &mut Vec<FailedEntry>,
    shard_id: u64,
    script: &[u8],
) -> Result<bool, SyncError> {
    if failed
        .iter()
        .any(|entry| entry.shard_id == shard_id && entry.script == script)
    {
        return Ok(true);
    }
    let failures = store.entry_failures(shard_id, script)?;
    if failures < MAX_CONTENT_FAILURES {
        return Ok(false);
    }
    failed.push(FailedEntry {
        shard_id,
        script: script.to_vec(),
        failures,
    });
    Ok(true)
}

/// Lists an entry whose failure a commit just counted, with the store's count.
fn record_failure<S: WalletStore>(
    store: &S,
    failed: &mut Vec<FailedEntry>,
    shard_id: u64,
    script: &[u8],
) -> Result<(), SyncError> {
    let failures = store.entry_failures(shard_id, script)?;
    match failed
        .iter_mut()
        .find(|entry| entry.shard_id == shard_id && entry.script == script)
    {
        Some(entry) => entry.failures = failures,
        None => failed.push(FailedEntry {
            shard_id,
            script: script.to_vec(),
            failures,
        }),
    }
    Ok(())
}

pub(crate) fn event_height(event: &transparent_events::TransparentEvent) -> u64 {
    match event {
        transparent_events::TransparentEvent::Receive(e) => u64::from(e.height),
        transparent_events::TransparentEvent::Spend(e) => u64::from(e.height),
    }
}

fn accepted_at(chain: &impl ChainView, map: &ShardMap, height: u64) -> Result<Anchor, SyncError> {
    let hash = chain.hash_at(height).or_else(|| {
        map.shards.iter().find_map(|s| {
            if s.end_height == height {
                Some(s.terminal_block_hash.clone())
            } else if s.start_height.checked_sub(1) == Some(height) {
                Some(s.parent_block_hash.clone())
            } else {
                None
            }
        })
    });
    match hash {
        Some(hash) if chain.is_accepted(height, &hash) == Acceptance::Accepted => {
            Ok(Anchor { height, hash })
        }
        _ => Err(SyncError::Invalid(format!(
            "wallet has no accepted rollback hash at {height}"
        ))),
    }
}

/// The report of a sync stopped because the wallet's chain cannot confirm a
/// block at `height`: whatever was already rolled back, and nothing read.
#[allow(clippy::too_many_arguments)]
fn chain_unknown<S: WalletStore>(
    store: &S,
    map: &ShardMap,
    target_anchor: &Anchor,
    charges: ByteCharges,
    height: u64,
    rolled_back_to: Option<u64>,
    replaced_revisions: Vec<String>,
    first_commit: u64,
) -> Result<SyncReport, SyncError> {
    let (covered, settled) = coverage_summary(store, map, target_anchor.height)?;
    Ok(SyncReport {
        ledger: store.ledger()?,
        charges,
        matched_shards: Vec::new(),
        unproductive_matches: 0,
        covered_through: covered,
        settled_through: settled,
        provisional: provisional_of(store, map)?,
        map_refreshes: 0,
        completion: Completion::Incomplete {
            reason: IncompleteReason::ChainUnknown { height },
            pending: store.pending()?.len(),
        },
        rolled_back_to,
        replaced_revisions,
        scripts_added: 0,
        commits: store.last_commit()? - first_commit,
        failed_entries: Vec::new(),
    })
}

fn incomplete_at<S: WalletStore>(
    store: &S,
    reason: IncompleteReason,
) -> Result<SyncReport, SyncError> {
    Ok(SyncReport {
        ledger: store.ledger()?,
        charges: ByteCharges::default(),
        matched_shards: vec![],
        unproductive_matches: 0,
        covered_through: 0,
        settled_through: 0,
        provisional: vec![],
        map_refreshes: 0,
        completion: Completion::Incomplete {
            reason,
            pending: store.pending()?.len(),
        },
        rolled_back_to: None,
        replaced_revisions: vec![],
        scripts_added: 0,
        commits: 0,
        failed_entries: Vec::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::StaticChain;
    use transparent_filter::{SealParameters, ShardMapEntry};

    fn entry(shard_id: u64, start: u64, end: u64, sealed: bool) -> ShardMapEntry {
        ShardMapEntry {
            shard_id,
            geometry: "decoupled-2k-8k".into(),
            start_height: start,
            end_height: end,
            parent_block_hash: format!("{:064x}", shard_id),
            terminal_block_hash: format!("{:064x}", shard_id + 100),
            filter_hash: format!("{:064x}", shard_id + 200),
            scripts: 1,
            page_rows: 1,
            txids: 1,
            directory_segments: 1,
            page_segments: 1,
            manifest_digest: format!("{:064x}", shard_id + 300),
            revision: 0,
            sealed,
        }
    }

    fn map(shards: Vec<ShardMapEntry>) -> ShardMap {
        let mut seal = std::collections::BTreeMap::new();
        seal.insert(
            "decoupled-2k-8k".to_string(),
            SealParameters {
                max_scripts: 98_304,
                max_page_rows: 7_936,
                max_txids: 0,
            },
        );
        ShardMap {
            genesis_hash: format!("{:064x}", 1),
            network: "main".into(),
            profile: "zcash-transparent-range-v1".into(),
            range_envelope_version: 1,
            start_height: 100,
            seal,
            shards,
            recuts: Vec::new(),
        }
    }

    /// The ordinary case the recovery exists for: the sealed prefix agrees and
    /// the unsealed tail has been replaced by a longer revision.
    #[test]
    fn a_replaced_tail_over_an_agreeing_prefix_continues_the_set() {
        let before = map(vec![entry(0, 100, 199, true), entry(1, 200, 249, false)]);
        let mut after = before.clone();
        after.shards[1].end_height = 299;
        after.shards[1].revision = 1;
        after.shards[1].manifest_digest = "ff".repeat(32);
        assert_eq!(check_continuation(&before, &after, 199), Ok(()));
    }

    /// Shards appended past the tail is the same case, and is what a wallet
    /// several revisions behind meets.
    #[test]
    fn appended_shards_continue_the_set() {
        let before = map(vec![entry(0, 100, 199, true), entry(1, 200, 249, false)]);
        let mut after = before.clone();
        after.shards[1].sealed = true;
        after.shards.push(entry(2, 250, 299, false));
        assert!(check_continuation(&before, &after, 199).is_ok());
    }

    #[test]
    fn a_different_set_identity_is_not_a_continuation() {
        let before = map(vec![entry(0, 100, 199, true)]);
        for mutate in [
            (|m: &mut ShardMap| m.genesis_hash = "ff".repeat(32)) as fn(&mut ShardMap),
            |m: &mut ShardMap| m.network = "test".into(),
            |m: &mut ShardMap| m.profile = "other".into(),
            |m: &mut ShardMap| m.range_envelope_version = 2,
            |m: &mut ShardMap| m.start_height = 1,
        ] {
            let mut after = before.clone();
            mutate(&mut after);
            assert!(
                check_continuation(&before, &after, 199).is_err(),
                "a changed set identity must be refused"
            );
        }
    }

    /// Sealed content is immutable, so a shard whose covered range now reads
    /// differently means a different set — not a longer one. This is the check
    /// that makes the resume point provably equal to where coverage stopped.
    ///
    /// A declared re-cut is no continuation either, even when it lies wholly above
    /// what the sync has covered: it gives the ids the walk resumes by to
    /// other ranges. That sync ends, and the next starts from the re-cut map.
    #[test]
    fn a_re_cut_covered_shard_is_not_a_continuation() {
        let before = map(vec![entry(0, 100, 199, true), entry(1, 200, 249, false)]);
        let mut after = before.clone();
        after.shards[0].end_height = 149;
        assert!(check_continuation(&before, &after, 199).is_err());

        // Shards 1 and 2 re-cut into one, the tail renumbered above it.
        let before = map(vec![
            entry(0, 100, 199, true),
            entry(1, 200, 249, true),
            entry(2, 250, 299, true),
            entry(3, 300, 320, false),
        ]);
        let mut wide = entry(1, 200, 299, true);
        wide.revision = 1;
        wide.manifest_digest = "aa".repeat(32);
        let mut tail = entry(2, 300, 321, false);
        tail.revision = 1;
        tail.manifest_digest = "bb".repeat(32);
        let undeclared = map(vec![before.shards[0].clone(), wide, tail]);
        let mut declared = undeclared.clone();
        declared.recuts.push(transparent_filter::Recut {
            epoch: 1,
            from_height: 200,
            superseded: before.shards[1..]
                .iter()
                .map(|entry| transparent_filter::SupersededShard {
                    shard_id: entry.shard_id,
                    geometry: entry.geometry.clone(),
                    start_height: entry.start_height,
                    end_height: entry.end_height,
                    terminal_block_hash: entry.terminal_block_hash.clone(),
                    manifest_digest: entry.manifest_digest.clone(),
                    revision: entry.revision,
                    sealed: entry.sealed,
                })
                .collect(),
        });
        for covered in [199, 249] {
            assert!(
                check_continuation(&before, &declared, covered).is_err(),
                "a declared re-cut ends the sync with {covered} covered"
            );
        }
        assert!(
            check_continuation(&before, &undeclared, 249).is_err(),
            "an undeclared re-cut of covered history"
        );
        // Wholly above coverage and undeclared, it is the walk's resume
        // point, not this check, that refuses a shard id moved elsewhere.
        assert!(check_continuation(&before, &undeclared, 199).is_ok());
        // The same map refreshed again continues itself.
        assert!(check_continuation(&declared, &declared, 321).is_ok());
    }

    /// Stored sealed coverage is found in the map by the height it starts at.
    /// A re-cut that declares the revision it replaced keeps that coverage
    /// good; the same re-cut undeclared, or a declaration that disagrees with
    /// what the store holds, does not.
    #[test]
    fn sealed_coverage_is_vouched_for_by_height_or_by_declaration() {
        let before = map(vec![
            entry(0, 100, 199, true),
            entry(1, 200, 249, true),
            entry(2, 250, 299, true),
            entry(3, 300, 320, false),
        ]);
        let held = |shard: &ShardMapEntry| CoverageRange {
            script: vec![1],
            start_height: shard.start_height,
            end_height: shard.end_height,
            kind: CoverageKind::Settled,
            shard_id: shard.shard_id,
            revision_digest: shard.manifest_digest.clone(),
            terminal_block_hash: shard.terminal_block_hash.clone(),
            source_anchor: Some(Anchor {
                height: shard.end_height,
                hash: shard.terminal_block_hash.clone(),
            }),
        };
        let mut wide = entry(1, 200, 299, true);
        wide.revision = 1;
        wide.manifest_digest = "aa".repeat(32);
        let mut tail = entry(2, 300, 321, false);
        tail.revision = 1;
        tail.manifest_digest = "bb".repeat(32);
        let undeclared = map(vec![before.shards[0].clone(), wide, tail]);
        let mut declared = undeclared.clone();
        declared.recuts.push(transparent_filter::Recut {
            epoch: 1,
            from_height: 200,
            superseded: before.shards[1..]
                .iter()
                .map(|entry| transparent_filter::SupersededShard {
                    shard_id: entry.shard_id,
                    geometry: entry.geometry.clone(),
                    start_height: entry.start_height,
                    end_height: entry.end_height,
                    terminal_block_hash: entry.terminal_block_hash.clone(),
                    manifest_digest: entry.manifest_digest.clone(),
                    revision: entry.revision,
                    sealed: entry.sealed,
                })
                .collect(),
        });
        let vouches = |map: &ShardMap, range: &CoverageRange| {
            vouches_for(map, &map.superseded_index(), range)
        };
        for shard in &before.shards[..3] {
            assert!(vouches(&before, &held(shard)));
            assert!(vouches(&declared, &held(shard)), "{}", shard.shard_id);
        }
        assert!(vouches(&undeclared, &held(&before.shards[0])));
        assert!(!vouches(&undeclared, &held(&before.shards[1])));
        assert!(
            !vouches(&undeclared, &held(&before.shards[2])),
            "a sealed range whose start the map no longer cuts at"
        );
        // A declaration must name what the store holds.
        let mut moved = held(&before.shards[1]);
        moved.shard_id = 7;
        assert!(!vouches(&declared, &moved));
        let mut longer = held(&before.shards[1]);
        longer.end_height += 1;
        longer.source_anchor.as_mut().unwrap().height += 1;
        assert!(!vouches(&declared, &longer));
        // A range cut short of its shard is left to the chain checks.
        let mut clipped = held(&before.shards[1]);
        clipped.end_height -= 10;
        assert!(vouches(&undeclared, &clipped));
        // Without a source anchor, the covered endpoint must be the declared
        // one; the published revision itself is still matched by digest.
        let mut legacy = held(&before.shards[1]);
        legacy.source_anchor = None;
        assert!(vouches(&declared, &legacy));
        legacy.terminal_block_hash = "ee".repeat(32);
        assert!(
            !vouches(&declared, &legacy),
            "at the declared end, another block"
        );
        // Cut short by a target, it rests on a block inside the range.
        legacy.end_height -= 10;
        assert!(vouches(&declared, &legacy));
        legacy.end_height += 11;
        assert!(!vouches(&declared, &legacy), "past the declared end");
        let mut legacy_published = held(&before.shards[0]);
        legacy_published.source_anchor = None;
        legacy_published.terminal_block_hash = "ee".repeat(32);
        assert!(vouches(&declared, &legacy_published));
    }

    /// A sealed range the map no longer vouches for is a contradiction only
    /// when the wallet's chain accepts both the block it rests on and the
    /// block the replacement ends on, at or below the target. A publisher
    /// that followed a shallow reorg through a just-sealed shard before the
    /// wallet's chain did is unsettled, not refused, and becomes a reorg once
    /// the chain moves.
    #[test]
    fn a_rewrite_is_refused_only_when_both_blocks_are_on_the_wallets_chain() {
        let stored = entry(1, 200, 249, true);
        let held = CoverageRange {
            script: vec![1],
            start_height: 200,
            end_height: 249,
            kind: CoverageKind::Settled,
            shard_id: 1,
            revision_digest: stored.manifest_digest.clone(),
            terminal_block_hash: stored.terminal_block_hash.clone(),
            source_anchor: Some(Anchor {
                height: 249,
                hash: stored.terminal_block_hash.clone(),
            }),
        };
        let mut replacement = stored.clone();
        replacement.manifest_digest = "aa".repeat(32);
        replacement.terminal_block_hash = "bb".repeat(32);
        let republished = map(vec![entry(0, 100, 199, true), replacement.clone()]);
        let target = Anchor {
            height: 260,
            hash: "cc".repeat(32),
        };
        let chain = |at_249: &str| StaticChain {
            hashes: [(249, at_249.to_string()), (260, target.hash.clone())].into(),
        };
        // The wallet's chain still holds the old block: the publisher is
        // ahead of it through a reorg, or wrong; either way, not yet.
        assert_eq!(
            judge_rewrite(
                &republished,
                &held,
                &target,
                &chain(&stored.terminal_block_hash)
            ),
            Rewrite::Unsettled(249)
        );
        // Once the chain moves to the publisher's branch, it is a reorg.
        assert_eq!(
            judge_rewrite(&republished, &held, &target, &chain(&"bb".repeat(32))),
            Rewrite::Reorg
        );
        assert_eq!(
            judge_rewrite(&republished, &held, &target, &StaticChain::default()),
            Rewrite::Unsettled(249)
        );
        // Both blocks on the wallet's chain, at different heights: an
        // undeclared re-cut or a rewrite, and the publisher's alone.
        let mut wider = replacement.clone();
        wider.end_height = 259;
        wider.terminal_block_hash = "dd".repeat(32);
        let recut = map(vec![entry(0, 100, 199, true), wider.clone()]);
        let both = StaticChain {
            hashes: [
                (249, stored.terminal_block_hash.clone()),
                (259, "dd".repeat(32)),
                (260, target.hash.clone()),
            ]
            .into(),
        };
        assert_eq!(
            judge_rewrite(&recut, &held, &target, &both),
            Rewrite::Contradiction
        );
        // Not when the replacement ends past the target or is unsealed.
        let early = Anchor {
            height: 255,
            hash: "ee".repeat(32),
        };
        assert_eq!(
            judge_rewrite(&recut, &held, &early, &both),
            Rewrite::Unsettled(259)
        );
        let mut unsealed = recut.clone();
        unsealed.shards[1].sealed = false;
        assert_eq!(
            judge_rewrite(&unsealed, &held, &target, &both),
            Rewrite::Unsettled(259)
        );
    }

    /// Unfinished pages under a revision the map no longer publishes keep
    /// what they saved only for a declared sealed re-cut whose item's own
    /// target the chain still accepts; nothing else the declaration says is
    /// relied on.
    #[test]
    fn unfinished_work_keeps_its_events_only_over_an_accepted_target() {
        let target = Anchor {
            height: 260,
            hash: "aa".repeat(32),
        };
        let item = PendingPages {
            id: Some(1),
            shard_id: 2,
            revision_digest: "11".repeat(32),
            script: vec![1],
            cursor: RunCursor {
                child: 0,
                row: 0,
                older_runs: None,
            },
            fragments: Vec::new(),
            carried: Vec::new(),
            attempts: 0,
            target_anchor: Some(target.clone()),
        };
        let declaration = |shard_id: u64, sealed: bool| transparent_filter::SupersededShard {
            shard_id,
            geometry: "decoupled-2k-8k".into(),
            start_height: 250,
            end_height: 299,
            terminal_block_hash: "bb".repeat(32),
            manifest_digest: item.revision_digest.clone(),
            revision: 0,
            sealed,
        };
        let accepting = StaticChain {
            hashes: [(260, target.hash.clone()), (299, "bb".repeat(32))].into(),
        };
        // The target since reorganised away; the declared end still accepted.
        let reorganised = StaticChain {
            hashes: [(260, "cc".repeat(32)), (299, "bb".repeat(32))].into(),
        };
        let sealed = declaration(2, true);
        assert_eq!(
            unfinished(&item, Some(&sealed), Some(255), &accepting),
            Unfinished::Drop,
            "declared, sealed, target accepted: the saved events stay"
        );
        assert_eq!(
            unfinished(&item, Some(&sealed), Some(255), &reorganised),
            Unfinished::Rewind(250),
            "a declaration whose end the chain accepts cannot keep events read for a target \
             it no longer accepts"
        );
        let mut forged = sealed.clone();
        forged.start_height = 258;
        assert_eq!(
            unfinished(&item, Some(&forged), Some(255), &reorganised),
            Unfinished::Rewind(255),
            "never from the declared start alone"
        );
        assert_eq!(
            unfinished(&item, Some(&declaration(2, false)), Some(255), &accepting),
            Unfinished::Rewind(250),
            "a declared tail is rolled back"
        );
        assert_eq!(
            unfinished(&item, Some(&declaration(3, true)), Some(255), &accepting),
            Unfinished::Rewind(255),
            "a declaration under another id is no declaration of this item"
        );
        assert_eq!(
            unfinished(&item, None, Some(255), &accepting),
            Unfinished::Rewind(255),
            "undeclared: from the lowest saved height"
        );
        assert_eq!(unfinished(&item, None, None, &accepting), Unfinished::Drop);
        let unknown = StaticChain::default();
        assert_eq!(
            unfinished(&item, Some(&sealed), Some(255), &unknown),
            Unfinished::Rewind(250),
            "a target the chain cannot place is not held"
        );
        let mut untargeted = item.clone();
        untargeted.target_anchor = None;
        assert_eq!(
            unfinished(&untargeted, Some(&sealed), Some(255), &accepting),
            Unfinished::Rewind(250)
        );
    }

    /// The walk resumes after a refresh only at a shard starting where the
    /// withdrawn one did; anything else would skip or repeat heights.
    #[test]
    fn the_walk_resumes_only_where_the_withdrawn_shard_started() {
        let withdrawn = entry(2, 250, 299, true);
        let mut republished = withdrawn.clone();
        republished.manifest_digest = "aa".repeat(32);
        republished.end_height = 320;
        assert_eq!(check_resume(&republished, &withdrawn), Ok(()));
        let moved = entry(2, 300, 349, true);
        let error = check_resume(&moved, &withdrawn).unwrap_err();
        assert!(
            error.contains("moves shard 2 from height 250 to 300"),
            "{error}"
        );
    }

    #[test]
    fn withdrawing_a_covered_shard_is_not_a_continuation() {
        let before = map(vec![entry(0, 100, 199, true), entry(1, 200, 249, false)]);
        let after = map(vec![]);
        assert!(check_continuation(&before, &after, 199).is_err());
    }

    #[test]
    fn coverage_ending_below_what_was_read_is_not_a_continuation() {
        let before = map(vec![entry(0, 100, 199, true), entry(1, 200, 249, false)]);
        let after = map(vec![entry(0, 100, 199, true)]);
        assert!(
            check_continuation(&before, &after, 249).is_err(),
            "a map ending below covered history withdraws what the wallet read"
        );
        assert!(
            check_continuation(&before, &after, 199).is_ok(),
            "the same map is fine when only the uncovered tail is gone"
        );
    }

    /// Seal parameters are what the boundaries were derived from, so a geometry
    /// already in use may not be resealed — but a set growing into another tier
    /// may declare a geometry the wallet had not seen.
    #[test]
    fn reseal_is_refused_but_a_new_geometry_is_not() {
        let before = map(vec![entry(0, 100, 199, true)]);
        let mut resealed = before.clone();
        resealed
            .seal
            .get_mut("decoupled-2k-8k")
            .unwrap()
            .max_page_rows = 1;
        assert!(check_continuation(&before, &resealed, 199).is_err());

        let mut widened = before.clone();
        widened.seal.insert(
            "decoupled-32k-32k".to_string(),
            SealParameters {
                max_scripts: 393_216,
                max_page_rows: 63_488,
                max_txids: 0,
            },
        );
        assert!(check_continuation(&before, &widened, 199).is_ok());
    }

    fn manifest_for(entry: &ShardMapEntry, parent: &str, map: &ShardMap) -> ShardManifest {
        use transparent_shard::manifest::{
            ManifestLayout, ManifestOccupancy, ManifestSeal, TableGeometry,
        };
        let geometry = transparent_shard::layout::by_name(&entry.geometry).unwrap();
        let seal = &map.seal[&entry.geometry];
        ShardManifest {
            schema: transparent_shard::SCHEMA.to_string(),
            profile: map.profile.clone(),
            geometry: entry.geometry.clone(),
            network: map.network.clone(),
            genesis_hash: map.genesis_hash.clone(),
            shard_id: entry.shard_id,
            start_height: entry.start_height,
            end_height: entry.end_height,
            parent_block_hash: entry.parent_block_hash.clone(),
            terminal_block_hash: entry.terminal_block_hash.clone(),
            tag_salt_counter: 0,
            parent_manifest_digest: parent.to_string(),
            sealed: entry.sealed,
            revision: entry.revision,
            supersedes: String::new(),
            seal: ManifestSeal {
                scripts_target: seal.max_scripts,
                scripts_capacity: seal.max_scripts * 2,
                directory_bytes_target: 1,
                directory_bytes_capacity: 2,
                page_rows_target: seal.max_page_rows,
                page_rows_capacity: geometry.page_rows,
                max_page_children: transparent_shard::MAX_PAGE_CHILDREN,
            },
            layout: ManifestLayout {
                max_script_bytes: transparent_shard::MAX_SCRIPT_BYTES as u32,
                inline_events: transparent_shard::INLINE_EVENTS,
                events_per_page: transparent_shard::EVENTS_PER_PAGE,
                page_row_header_bytes: transparent_shard::PAGE_ROW_HEADER_BYTES as u32,
                page_entry_header_bytes: transparent_shard::PAGE_ENTRY_HEADER_BYTES as u32,
                directory_choices: transparent_shard::build::DIRECTORY_CHOICES as u32,
            },
            filter_hash: entry.filter_hash.clone(),
            directory_segments: (0..entry.directory_segments)
                .map(|i| TableGeometry {
                    rows: geometry.directory_rows,
                    row_bytes: geometry.directory_row_bytes as u32,
                    sha256: format!("{:064x}", 1000 + i),
                })
                .collect(),
            // One page shard per map-declared page shard: all but the last a
            // single block, the last the rest of the range.
            page_children: (0..u64::from(entry.page_segments))
                .map(|i| {
                    let last = i + 1 == u64::from(entry.page_segments);
                    PageChild {
                        index: i as u32,
                        start_height: entry.start_height + i,
                        end_height: if last {
                            entry.end_height
                        } else {
                            entry.start_height + i
                        },
                        terminal_block_hash: if last {
                            entry.terminal_block_hash.clone()
                        } else {
                            format!("{:064x}", 3000 + i)
                        },
                        tag_salt_counter: 0,
                        sealed: entry.sealed || !last,
                        page_segments: vec![TableGeometry {
                            rows: geometry.page_rows,
                            row_bytes: geometry.page_row_bytes as u32,
                            sha256: format!("{:064x}", 2000 + i),
                        }],
                        occupancy: transparent_shard::manifest::PageChildOccupancy {
                            page_rows: 1,
                            fragments: 1,
                            runs: 1,
                            events: 1,
                        },
                    }
                })
                .collect(),
            occupancy: ManifestOccupancy {
                scripts: 1,
                page_rows: u64::from(entry.page_segments),
                fragments: u64::from(entry.page_segments),
                events: 1,
                blocks: 100,
                txids: 1,
                excluded_scripts: 0,
            },
            directory_choice: None,
        }
    }

    /// A published choice table is checked with the manifest: one that does
    /// not decode, or indexes a different number of scripts than the shard
    /// places, stops the sync rather than falling back to two queries.
    #[test]
    fn a_malformed_directory_choice_is_refused_with_the_manifest() {
        let script: &[u8] = &[0x51, 0x01];
        let valid = transparent_shard::manifest::encode_directory_choice(
            &transparent_shard::ChoiceTable::build(0, &[(script, 1)]).unwrap(),
        );
        let two = transparent_shard::manifest::encode_directory_choice(
            &transparent_shard::ChoiceTable::build(0, &[(script, 1), (&[0x51, 0x02], 0)]).unwrap(),
        );
        for (choice, accepted) in [
            (valid, true),
            (two, false),
            ("not base64!".to_string(), false),
            (String::new(), false),
        ] {
            let mut entry = entry(0, 100, 199, true);
            let map = map(vec![entry.clone()]);
            let mut manifest = manifest_for(&entry, "", &map);
            manifest.directory_choice = Some(choice.clone());
            entry.manifest_digest = manifest.digest();
            let map = self::map(vec![entry.clone()]);
            let verified = verify_manifest(&manifest.canonical_bytes(), &entry, "", &map);
            match (accepted, verified) {
                (true, Ok(verified)) => {
                    assert!(directory_choice(&verified).unwrap().is_some())
                }
                (false, Err(SyncError::Invalid(_))) => {}
                (_, other) => panic!("{choice:?}: {other:?}"),
            }
        }
    }

    /// The manifest that digests to what the map names, and agrees with it in
    /// every field, is accepted; every single-field departure is refused with
    /// the field named, and a changed body without a changed digest is caught
    /// by the digest first.
    #[test]
    fn a_manifest_is_accepted_only_when_it_agrees_with_the_map_in_every_field() {
        let mut first = entry(0, 100, 199, true);
        let mut second = entry(1, 200, 249, false);
        let map = map(vec![first.clone(), second.clone()]);
        let genuine_first = manifest_for(&first, "", &map);
        first.manifest_digest = genuine_first.digest();
        let genuine_second = manifest_for(&second, &first.manifest_digest, &map);
        second.manifest_digest = genuine_second.digest();
        let map = self::map(vec![first.clone(), second.clone()]);

        verify_manifest(&genuine_first.canonical_bytes(), &first, "", &map).expect("genuine");
        verify_manifest(
            &genuine_second.canonical_bytes(),
            &second,
            &first.manifest_digest,
            &map,
        )
        .expect("genuine, chained");
        let mut bad_layout = genuine_second.clone();
        bad_layout.layout.events_per_page = 46;
        let mut matching_entry = second.clone();
        matching_entry.manifest_digest = bad_layout.digest();
        assert!(matches!(
            verify_manifest(
                &bad_layout.canonical_bytes(),
                &matching_entry,
                &first.manifest_digest,
                &map
            ),
            Err(SyncError::ManifestMismatch {
                field: "layout",
                ..
            })
        ));

        // Any change to the bytes changes the digest, so the first refusal is
        // always the digest's.
        let mut altered = genuine_second.clone();
        altered.end_height += 1;
        let error = verify_manifest(
            &altered.canonical_bytes(),
            &second,
            &first.manifest_digest,
            &map,
        )
        .unwrap_err();
        assert!(
            matches!(
                error,
                SyncError::ManifestMismatch {
                    field: "digest",
                    ..
                }
            ),
            "{error}"
        );

        // A map entry that disagrees with a genuine manifest names the field.
        type Mutation = Box<dyn Fn(&mut ShardMapEntry)>;
        let cases: Vec<(&str, Mutation)> = vec![
            (
                "geometry",
                Box::new(|e| e.geometry = "decoupled-32k-32k".into()),
            ),
            ("range", Box::new(|e| e.end_height = 250)),
            (
                "parent_block_hash",
                Box::new(|e| e.parent_block_hash = "ff".repeat(32)),
            ),
            (
                "terminal_block_hash",
                Box::new(|e| e.terminal_block_hash = "ee".repeat(32)),
            ),
            ("filter_hash", Box::new(|e| e.filter_hash = "dd".repeat(32))),
            ("sealed", Box::new(|e| e.sealed = true)),
            ("revision", Box::new(|e| e.revision = 1)),
            ("segment_count", Box::new(|e| e.page_segments = 2)),
        ];
        for (field, mutate) in cases {
            let mut disagreeing = second.clone();
            mutate(&mut disagreeing);
            // The digest still matches the bytes; only the map entry changed.
            disagreeing.manifest_digest = genuine_second.digest();
            let error = verify_manifest(
                &genuine_second.canonical_bytes(),
                &disagreeing,
                &first.manifest_digest,
                &map,
            )
            .unwrap_err();
            match error {
                SyncError::ManifestMismatch { field: found, .. } => assert_eq!(found, field),
                other => panic!("{field}: {other}"),
            }
        }
        // A broken chain is a different sequence of shards.
        let error =
            verify_manifest(&genuine_second.canonical_bytes(), &second, "", &map).unwrap_err();
        assert!(matches!(
            error,
            SyncError::ManifestMismatch {
                field: "parent_manifest_digest",
                ..
            }
        ));
        // A map naming another set is refused before any field comparison.
        let mut other_set = map.clone();
        other_set.genesis_hash = "cc".repeat(32);
        let error = verify_manifest(
            &genuine_second.canonical_bytes(),
            &second,
            &first.manifest_digest,
            &other_set,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            SyncError::ManifestMismatch {
                field: "genesis_hash",
                ..
            }
        ));
    }

    #[test]
    fn backoff_prefers_the_services_figure_and_caps_it() {
        // Each wait is its base jittered by up to half again, never less.
        let within = |wait: Duration, base: Duration| wait >= base && wait <= base * 3 / 2;
        assert!(within(overload_backoff(1, None), OVERLOAD_BACKOFF));
        assert!(within(overload_backoff(2, None), OVERLOAD_BACKOFF * 2));
        assert!(
            within(
                overload_backoff(1, Some(Duration::from_millis(10))),
                Duration::from_millis(10)
            ),
            "the service knows why it refused"
        );
        assert!(
            within(
                overload_backoff(1, Some(Duration::from_secs(3600))),
                OVERLOAD_BACKOFF_CAP
            ),
            "a delay a wallet cannot sanity-check must not park a sync"
        );
        assert!(
            within(overload_backoff(20, None), OVERLOAD_BACKOFF_CAP),
            "doubling is capped too"
        );
    }
    #[test]
    fn compact_boundaries_reject_underfilled_reordered_and_cross_row_reference_shortcuts() {
        use transparent_events::{ReceiveEvent, SpendEvent, TransparentEvent, Txid};
        let receive = TransparentEvent::Receive(ReceiveEvent {
            metadata: None,
            height: 100,
            transaction_index: 0,
            txid: Txid([7; 32]),
            output_index: 3,
            value: 9,
            coinbase: false,
        });
        let spend = TransparentEvent::Spend(SpendEvent {
            metadata: None,
            height: 101,
            transaction_index: 0,
            spending_txid: Txid([8; 32]),
            input_index: 0,
            spent_txid: Txid([7; 32]),
            spent_output_index: 3,
        });
        let mut boundary = crate::store::PageBoundary {
            event_bytes: 4000,
            last_event: receive,
        };
        // 58 spare bytes would hold the local 43-byte form. Treating this
        // spend as 79 bytes would incorrectly accept the premature boundary.
        assert!(validate_page_boundary(Some(&boundary), &[spend]).is_err());
        boundary.event_bytes = 4029;
        let next = validate_page_boundary(Some(&boundary), &[spend]).unwrap();
        assert_eq!(next.event_bytes, 79); // References reset at the new fragment.
        assert!(validate_page_boundary(Some(&next), &[receive]).is_err());
        assert!(validate_page_boundary(None, &[]).is_err());
    }
}

/// The chain walk over real built tables, read directly: every check a walk
/// makes, and the run cache stopping it.
#[cfg(test)]
mod walk_tests {
    use super::*;
    use transparent_events::{ReceiveEvent, SpendEvent, Txid};
    use transparent_filter::ShardMapEntry;
    use transparent_shard::build::{Block, Built, BuiltChild, BuiltDirectory, ShardStream};
    use transparent_shard::layout::DECOUPLED_2K_8K;
    use transparent_shard::manifest::{
        ManifestLayout, ManifestOccupancy, ManifestSeal, PageChildOccupancy, TableGeometry,
    };
    use transparent_shard::page_row::RunLink;
    use transparent_shard::seal::{DecoupledPolicy, Limit};

    fn hash_at(height: u64) -> BlockHash {
        let mut bytes = [3u8; 32];
        bytes[..8].copy_from_slice(&height.to_le_bytes());
        BlockHash::from_internal_bytes(bytes)
    }

    fn script(tag: u32) -> Vec<u8> {
        let mut bytes = vec![0x76, 0xa9, 0x14];
        bytes.extend_from_slice(&tag.to_le_bytes());
        bytes.extend_from_slice(&[0u8; 16]);
        bytes.extend_from_slice(&[0x88, 0xac]);
        bytes
    }

    fn receive(height: u32, nonce: u32) -> TransparentEvent {
        let mut txid = [0u8; 32];
        txid[..4].copy_from_slice(&nonce.to_le_bytes());
        txid[4..8].copy_from_slice(&height.to_le_bytes());
        TransparentEvent::Receive(ReceiveEvent {
            metadata: None,
            height,
            txid: Txid(txid),
            transaction_index: 0,
            output_index: nonce,
            value: 1_000,
            coinbase: false,
        })
    }

    fn spend(height: u32, nonce: u32) -> TransparentEvent {
        let mut txid = [0u8; 32];
        txid[..4].copy_from_slice(&nonce.to_le_bytes());
        txid[4..8].copy_from_slice(&height.to_le_bytes());
        TransparentEvent::Spend(SpendEvent {
            metadata: None,
            height,
            spending_txid: Txid(txid),
            transaction_index: 1,
            input_index: nonce,
            spent_txid: Txid([7; 32]),
            spent_output_index: nonce,
        })
    }

    /// `count` blocks from `first`: four busy scripts, which build long
    /// chains, and many quiet ones whose pushed-out events close page shards.
    fn chain(first: u64, count: u64) -> Vec<Block> {
        let mut state = 0x2545_f491_4f6c_dd1du64;
        let mut next = move |bound: u64| {
            state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            (z ^ (z >> 31)) % bound
        };
        let mut nonce = 0u32;
        (first..first + count)
            .map(|height| {
                let mut events = Vec::new();
                for _ in 0..1 + next(5) {
                    let tag = if next(2) == 0 {
                        next(4) as u32
                    } else {
                        100 + next(60) as u32
                    };
                    nonce += 1;
                    let at = height as u32;
                    let event = if next(3) == 0 {
                        spend(at, nonce)
                    } else {
                        receive(at, nonce)
                    };
                    events.push((ScriptBytes::new(script(tag)), event));
                }
                (height, events)
            })
            .collect()
    }

    /// The chain every walk test reads: busy and quiet scripts, then two
    /// blocks in which only the busiest script is active, so the open page
    /// shard of the tail holds a run of a script with older runs.
    fn blocks() -> Vec<Block> {
        let mut blocks = chain(1_000, 80);
        let last = blocks.last().unwrap().0;
        for (offset, nonce) in [(1u64, 900_001u32), (2, 900_002)] {
            let height = last + offset;
            blocks.push((
                height,
                vec![(ScriptBytes::new(script(0)), receive(height as u32, nonce))],
            ));
        }
        blocks
    }

    struct Directory {
        built: BuiltDirectory,
        children: Vec<BuiltChild>,
    }

    /// [`blocks`] and what they build, once per test binary: building is
    /// the slow part of every walk test.
    fn fixture() -> &'static (Vec<Block>, Vec<Directory>) {
        static FIXTURE: std::sync::OnceLock<(Vec<Block>, Vec<Directory>)> =
            std::sync::OnceLock::new();
        FIXTURE.get_or_init(|| {
            let blocks = blocks();
            let directories = build(&blocks);
            (blocks, directories)
        })
    }

    /// Directory shards over `blocks` under a policy that closes a page shard
    /// at two rows and a directory shard at 32 page shards. The last is a
    /// tail whose last page shard is open.
    fn build(blocks: &[Block]) -> Vec<Directory> {
        let policy = DecoupledPolicy {
            scripts: Limit::new(10_000, 20_000).unwrap(),
            directory_bytes: Limit::new(1_000_000, 2_000_000).unwrap(),
            page_rows: Limit::new(2, 4).unwrap(),
            max_children: 32,
            ..DecoupledPolicy::for_geometry(&DECOUPLED_2K_8K)
        };
        let genesis =
            BlockHash::from_display_hex(transparent_filter::MAINNET_GENESIS_DISPLAY).unwrap();
        let mut stream = ShardStream::with_policy(
            &DECOUPLED_2K_8K,
            policy,
            blocks[0].0,
            0,
            genesis,
            transparent_filter::RANGE_PROFILE,
        )
        .unwrap();
        let terminal = |height: u64| Some(hash_at(height));
        let mut out = Vec::new();
        for block in blocks {
            out.extend(stream.push_block(block.clone(), &terminal).unwrap());
        }
        out.extend(stream.finish(&terminal).unwrap());
        let mut directories = Vec::new();
        let mut children = Vec::new();
        for built in out {
            match built {
                Built::Child(child) => children.push(child),
                Built::Directory { built, .. } => directories.push(Directory {
                    built,
                    children: std::mem::take(&mut children),
                }),
            }
        }
        directories
    }

    fn manifest_of(directory: &Directory) -> ShardManifest {
        let built = &directory.built;
        let geometry = built.geometry;
        let page_children: Vec<PageChild> = directory
            .children
            .iter()
            .map(|child| PageChild {
                index: child.index,
                start_height: child.start_height,
                end_height: child.end_height,
                terminal_block_hash: child.terminal_block_hash.to_display_hex(),
                tag_salt_counter: child.tag_salt_counter,
                sealed: child.sealed,
                page_segments: child
                    .segment_digests(geometry)
                    .iter()
                    .map(|digest| TableGeometry {
                        rows: geometry.page_rows,
                        row_bytes: geometry.page_row_bytes as u32,
                        sha256: hex::encode(digest),
                    })
                    .collect(),
                occupancy: PageChildOccupancy {
                    page_rows: child.page_rows,
                    fragments: child.fragments,
                    runs: child.runs,
                    events: child.events,
                },
            })
            .collect();
        ShardManifest {
            schema: transparent_shard::SCHEMA.to_string(),
            profile: transparent_filter::RANGE_PROFILE.to_string(),
            geometry: geometry.name.to_string(),
            network: "main".into(),
            genesis_hash: transparent_filter::MAINNET_GENESIS_DISPLAY.into(),
            shard_id: built.shard_id,
            start_height: built.start_height,
            end_height: built.end_height,
            parent_block_hash: hash_at(built.start_height - 1).to_display_hex(),
            terminal_block_hash: hash_at(built.end_height).to_display_hex(),
            tag_salt_counter: built.tag_salt_counter,
            parent_manifest_digest: String::new(),
            sealed: built.sealed,
            revision: 0,
            supersedes: String::new(),
            seal: ManifestSeal {
                scripts_target: 10_000,
                scripts_capacity: 20_000,
                directory_bytes_target: 1_000_000,
                directory_bytes_capacity: 2_000_000,
                page_rows_target: 2,
                page_rows_capacity: 4,
                max_page_children: 32,
            },
            layout: ManifestLayout::current(),
            filter_hash: "00".repeat(32),
            directory_segments: vec![TableGeometry {
                rows: geometry.directory_rows,
                row_bytes: geometry.directory_row_bytes as u32,
                sha256: "00".repeat(32),
            }],
            occupancy: ManifestOccupancy {
                scripts: built.scripts,
                page_rows: page_children.iter().map(|c| c.occupancy.page_rows).sum(),
                fragments: page_children.iter().map(|c| c.occupancy.fragments).sum(),
                events: built.events,
                blocks: built.end_height - built.start_height + 1,
                txids: built.txids,
                excluded_scripts: built.excluded_scripts,
            },
            page_children,
            directory_choice: None,
        }
    }

    fn entry_of(manifest: &ShardManifest) -> ShardMapEntry {
        ShardMapEntry {
            shard_id: manifest.shard_id,
            geometry: manifest.geometry.clone(),
            start_height: manifest.start_height,
            end_height: manifest.end_height,
            parent_block_hash: manifest.parent_block_hash.clone(),
            terminal_block_hash: manifest.terminal_block_hash.clone(),
            filter_hash: manifest.filter_hash.clone(),
            scripts: manifest.occupancy.scripts,
            page_rows: manifest.occupancy.page_rows,
            txids: 0,
            directory_segments: 1,
            page_segments: manifest.page_children.len() as u32,
            manifest_digest: manifest.digest(),
            revision: 0,
            sealed: manifest.sealed,
        }
    }

    fn target_of(manifest: &ShardManifest) -> Anchor {
        Anchor {
            height: manifest.end_height,
            hash: manifest.terminal_block_hash.clone(),
        }
    }

    /// The directory entry for `script`, read straight from the table.
    fn directory_entry(directory: &Directory, script: &[u8]) -> Option<DirectoryEntry> {
        let built = &directory.built;
        let salt = transparent_shard::tag_salt(
            built.shard_id,
            &hash_at(built.end_height),
            built.tag_salt_counter,
        );
        let wanted = transparent_shard::script_tag(&salt, script);
        let rows = built.geometry.directory_rows;
        candidate_rows(built.shard_id, script, rows)
            .into_iter()
            .flat_map(|row| {
                transparent_shard::records::decode_directory_row(
                    built.directory_row(row),
                    built.children,
                )
                .unwrap()
            })
            .find(|entry| entry.tag == wanted)
    }

    /// Every event `script` has in `directory`'s range, oldest first.
    fn history(blocks: &[Block], directory: &Directory, script: &[u8]) -> Vec<TransparentEvent> {
        let range = directory.built.start_height..=directory.built.end_height;
        let mut events: Vec<TransparentEvent> = blocks
            .iter()
            .filter(|(height, _)| range.contains(height))
            .flat_map(|(_, events)| events.iter())
            .filter(|(owner, _)| owner.as_slice() == script)
            .map(|(_, event)| *event)
            .collect();
        events.sort_by_key(TransparentEvent::sort_key);
        events
    }

    /// Page rows read straight from the built tables, recording every
    /// request and round. Rows in `corrupt` reply with bytes that do not
    /// decode; rows in `rewritten` reply with the segments given, which do.
    /// Round `overloaded`, counted from zero, is refused as an overloaded
    /// service refuses it.
    struct Direct<'a> {
        children: &'a [BuiltChild],
        requests: Vec<RowRequest>,
        rounds: usize,
        /// Requests per round, in order.
        round_sizes: Vec<usize>,
        corrupt: std::collections::BTreeSet<(u32, u64)>,
        rewritten: BTreeMap<(u32, u64), Vec<Vec<u8>>>,
        overloaded: Option<usize>,
    }

    impl<'a> Direct<'a> {
        fn new(directory: &'a Directory) -> Self {
            Self {
                children: &directory.children,
                requests: Vec::new(),
                rounds: 0,
                round_sizes: Vec::new(),
                corrupt: Default::default(),
                rewritten: BTreeMap::new(),
                overloaded: None,
            }
        }

        /// The requests of round `round`, counted from zero.
        fn round(&self, round: usize) -> &[RowRequest] {
            let start: usize = self.round_sizes[..round].iter().sum();
            &self.requests[start..start + self.round_sizes[round]]
        }
    }

    impl RowSource for Direct<'_> {
        fn stopped(&self) -> Option<IncompleteReason> {
            None
        }

        fn fetch<S: WalletStore>(
            &mut self,
            _: &mut S,
            requests: &[RowRequest],
        ) -> Result<Vec<Reply>, SyncError> {
            if self.overloaded == Some(self.rounds) {
                self.overloaded = None;
                return Err(SyncError::Client(ClientError::Overloaded(
                    crate::transport::Overloaded { retry_after: None },
                )));
            }
            self.rounds += 1;
            self.round_sizes.push(requests.len());
            self.requests.extend_from_slice(requests);
            Ok(requests
                .iter()
                .map(|request| {
                    let key = (request.child, request.row);
                    let mut row = self.children[request.child as usize]
                        .page_row(&DECOUPLED_2K_8K, request.row)
                        .to_vec();
                    if self.corrupt.contains(&key) {
                        row.iter_mut().for_each(|byte| *byte ^= 0xa5);
                    }
                    self.rewritten.get(&key).cloned().unwrap_or(vec![row])
                })
                .collect())
        }
    }

    /// The directory phase, without the directory query: the entry's inline
    /// events committed and its walk recorded, as `retrieve_shard_into`
    /// leaves them. Returns the walks owed.
    fn directory_phase(
        store: &mut MemoryStore,
        directory: &Directory,
        manifest: &ShardManifest,
        scripts: &[Vec<u8>],
    ) -> Vec<PendingPages> {
        let entry = entry_of(manifest);
        let mut commit = ShardCommit {
            shard_id: entry.shard_id,
            revision_digest: entry.manifest_digest.clone(),
            sealed: entry.sealed,
            start_height: entry.start_height,
            end_height: entry.end_height,
            terminal_block_hash: entry.terminal_block_hash.clone(),
            ..Default::default()
        };
        for script in scripts {
            let found = directory_entry(directory, script).expect("an entry");
            let (child, row) = found.head().expect("a paged script");
            commit
                .events
                .extend(found.inline.iter().map(|event| StoredEvent {
                    script: script.clone(),
                    event: *event,
                    shard_id: entry.shard_id,
                    revision_digest: entry.manifest_digest.clone(),
                }));
            commit.pending_upsert.push(PendingPages {
                id: None,
                shard_id: entry.shard_id,
                revision_digest: entry.manifest_digest.clone(),
                script: script.clone(),
                cursor: RunCursor {
                    child,
                    row,
                    older_runs: None,
                },
                fragments: Vec::new(),
                carried: found.inline.clone(),
                attempts: 0,
                target_anchor: Some(target_of(manifest)),
            });
        }
        let before: Vec<u64> = store
            .pending()
            .unwrap()
            .iter()
            .filter_map(|p| p.id)
            .collect();
        commit_bounded(store, &target_of(manifest), commit).unwrap();
        store
            .pending()
            .unwrap()
            .into_iter()
            .filter(|p| p.id.is_some_and(|id| !before.contains(&id)))
            .collect()
    }

    fn walk(
        store: &mut MemoryStore,
        manifest: &ShardManifest,
        owed: &[PendingPages],
        rows: &mut Direct<'_>,
    ) -> Result<Option<Completion>, SyncError> {
        let mut failures = Failures::default();
        let walked = walk_chains(
            store,
            &entry_of(manifest),
            manifest,
            owed,
            rows,
            &target_of(manifest),
            &mut failures,
        );
        assert!(
            failures.entries.is_empty(),
            "an honest walk fails nothing: {:?}",
            failures.entries
        );
        walked
    }

    /// [`walk`] where entries may fail: the entries that did.
    fn walk_failing(
        store: &mut MemoryStore,
        manifest: &ShardManifest,
        owed: &[PendingPages],
        rows: &mut Direct<'_>,
    ) -> Vec<FailedEntry> {
        let mut failures = Failures::default();
        assert_eq!(
            walk_chains(
                store,
                &entry_of(manifest),
                manifest,
                owed,
                rows,
                &target_of(manifest),
                &mut failures,
            )
            .unwrap(),
            None
        );
        assert!(failures.owed.is_empty(), "every owed dummy was sent");
        failures.entries
    }

    fn stored(store: &MemoryStore, script: &[u8]) -> Vec<TransparentEvent> {
        store
            .events()
            .unwrap()
            .into_iter()
            .filter(|stored| stored.script == script)
            .map(|stored| stored.event)
            .collect()
    }

    /// The runs a script's chain has, newest first, read straight from the
    /// tables: page shard and first row.
    fn runs_of(directory: &Directory, script: &[u8]) -> Vec<(u8, u32)> {
        let manifest = manifest_of(directory);
        let mut runs = Vec::new();
        let mut next = directory_entry(directory, script).and_then(|e| e.head());
        while let Some((child, row)) = next {
            runs.push((child, row));
            let page = &manifest.page_children[child as usize];
            let salt = transparent_shard::page_tag_salt(
                manifest.shard_id,
                page.index,
                page.end_height,
                &BlockHash::from_display_hex(&page.terminal_block_hash).unwrap(),
                page.tag_salt_counter,
            );
            let tag = transparent_shard::script_tag(&salt, script);
            next = transparent_shard::decode_page_row(
                directory.children[child as usize].page_row(&DECOUPLED_2K_8K, u64::from(row)),
            )
            .unwrap()
            .into_iter()
            .find(|e| e.tag == tag && e.ordinal == 0)
            .unwrap()
            .link
            .previous();
        }
        runs
    }

    /// Paged scripts of `directory`, busiest chains first.
    fn paged(directory: &Directory) -> Vec<Vec<u8>> {
        let mut paged: Vec<(usize, Vec<u8>)> = (0..4)
            .chain(100..160)
            .map(script)
            .map(|script| (runs_of(directory, &script).len(), script))
            .filter(|(runs, _)| *runs > 0)
            .collect();
        paged.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
        paged.into_iter().map(|(_, script)| script).collect()
    }

    /// Every paged script's history comes back exactly, all walks sharing
    /// rounds, in one round per run plus one; and the run cache is filled
    /// for every run in a sealed page shard with that run's own events and
    /// the run below it, which together rebuild the history back to the
    /// directory shard's start.
    #[test]
    fn walking_every_chain_recovers_each_history_in_one_round_per_run() {
        let (blocks, directories) = fixture();
        let directory = &directories[0];
        let manifest = manifest_of(directory);
        manifest
            .check_children(DECOUPLED_2K_8K.page_rows, 4_096)
            .unwrap();
        let scripts = paged(directory);
        let longest = runs_of(directory, &scripts[0]).len();
        assert!(longest >= 3, "a chain long enough to walk: {longest}");

        let mut store = MemoryStore::new();
        let owed = directory_phase(&mut store, directory, &manifest, &scripts);
        let mut rows = Direct::new(directory);
        assert_eq!(walk(&mut store, &manifest, &owed, &mut rows).unwrap(), None);
        assert_eq!(rows.rounds, longest + 1);
        assert!(store.pending().unwrap().is_empty());
        let identities: Vec<String> = manifest.child_digests().iter().map(hex::encode).collect();
        for script in &scripts {
            let expected = history(blocks, directory, script);
            assert_eq!(stored(&store, script), expected);
            assert_eq!(store.coverage(script).unwrap().len(), 1);
            // Oldest run first: each cached run holds its own events and
            // names the run below it, and together they rebuild the history.
            let runs = runs_of(directory, script);
            let mut rebuilt = Vec::new();
            for (index, (child, _)) in runs.iter().enumerate().rev() {
                let cached = store
                    .cached_run(&identities[usize::from(*child)], script)
                    .unwrap();
                if manifest.page_children[usize::from(*child)].sealed {
                    let cached = cached.expect("a sealed run is cached");
                    assert_eq!(cached.shard_id, manifest.shard_id);
                    assert_eq!(
                        cached.older,
                        runs.get(index + 1)
                            .map(|(older, _)| identities[usize::from(*older)].clone())
                    );
                    rebuilt.extend(cached.events);
                    assert_eq!(rebuilt[..], expected[..rebuilt.len()]);
                } else {
                    assert_eq!(cached, None);
                }
            }
        }
    }

    /// Tip catch-up: a walk whose newest run is in the open page shard reads
    /// that run and stops at the cached run it links to; one whose newest run
    /// is in a sealed page shard reads nothing at all.
    #[test]
    fn a_cached_run_stops_the_walk() {
        let (blocks, directories) = fixture();
        let tail = directories.last().unwrap();
        let manifest = manifest_of(tail);
        assert!(!manifest.sealed);
        let open = manifest.page_children.len() as u8 - 1;
        assert!(!manifest.page_children[open as usize].sealed);
        let scripts = paged(tail);
        let at_tip: Vec<Vec<u8>> = scripts
            .iter()
            .filter(|s| {
                let runs = runs_of(tail, s);
                runs.len() >= 2 && runs[0].0 == open
            })
            .cloned()
            .collect();
        let sealed_head: Vec<Vec<u8>> = scripts
            .iter()
            .filter(|s| runs_of(tail, s)[0].0 != open)
            .cloned()
            .collect();
        assert!(!at_tip.is_empty() && !sealed_head.is_empty());

        let mut store = MemoryStore::new();
        let owed = directory_phase(&mut store, tail, &manifest, &scripts);
        let mut first = Direct::new(tail);
        walk(&mut store, &manifest, &owed, &mut first).unwrap();

        // The same revision walked again from its directory entries.
        let owed = directory_phase(&mut store, tail, &manifest, &scripts);
        let mut again = Direct::new(tail);
        assert_eq!(
            walk(&mut store, &manifest, &owed, &mut again).unwrap(),
            None
        );
        assert!(again
            .requests
            .iter()
            .all(|request| request.child == u32::from(open)));
        // Every script whose newest run is in the open page shard reads that
        // run again, and nothing older.
        let head_fragments: usize = scripts
            .iter()
            .filter(|script| runs_of(tail, script)[0].0 == open)
            .map(|script| {
                let (child, row) = runs_of(tail, script)[0];
                let salt = transparent_shard::page_tag_salt(
                    manifest.shard_id,
                    u32::from(child),
                    manifest.page_children[usize::from(child)].end_height,
                    &hash_at(manifest.page_children[usize::from(child)].end_height),
                    manifest.page_children[usize::from(child)].tag_salt_counter,
                );
                let tag = transparent_shard::script_tag(&salt, script);
                transparent_shard::decode_page_row(
                    tail.children[usize::from(child)].page_row(&DECOUPLED_2K_8K, u64::from(row)),
                )
                .unwrap()
                .into_iter()
                .find(|e| e.tag == tag)
                .unwrap()
                .fragment_count as usize
            })
            .sum();
        assert_eq!(again.requests.len(), head_fragments);
        assert_eq!(again.rounds, 2, "the head run, then its rest");
        for script in &scripts {
            assert_eq!(stored(&store, script), history(blocks, tail, script));
        }

        // A head in a sealed page shard that is cached costs no page query.
        let mut fresh = MemoryStore::new();
        let owed = directory_phase(&mut fresh, tail, &manifest, &sealed_head);
        walk(&mut fresh, &manifest, &owed, &mut Direct::new(tail)).unwrap();
        let owed = directory_phase(&mut fresh, tail, &manifest, &sealed_head);
        let mut cached = Direct::new(tail);
        walk(&mut fresh, &manifest, &owed, &mut cached).unwrap();
        assert_eq!((cached.rounds, cached.requests.len()), (0, 0));
        assert!(fresh.pending().unwrap().is_empty());
    }

    /// A walk over a manifest that places a page shard's range elsewhere
    /// finds the events of its runs pushed out while it was not open, and
    /// refuses them: a content failure of the entry, counted, with the walk
    /// still owed for a later sync.
    #[test]
    fn a_run_in_the_wrong_page_shard_is_refused() {
        let (_, directories) = fixture();
        let directory = &directories[0];
        let mut manifest = manifest_of(directory);
        let script = paged(directory).remove(0);
        let runs = runs_of(directory, &script);
        let (child, _) = runs[runs.len() - 1];
        // The oldest run's page shard, claimed to open only after its last
        // block: nothing could have been pushed into it. Its end, which its
        // tag salt is keyed on, stays.
        let page = &mut manifest.page_children[usize::from(child)];
        page.start_height = page.end_height + 1;
        let mut store = MemoryStore::new();
        let owed = directory_phase(
            &mut store,
            directory,
            &manifest,
            std::slice::from_ref(&script),
        );
        let failed = walk_failing(&mut store, &manifest, &owed, &mut Direct::new(directory));
        assert_eq!(
            failed,
            vec![FailedEntry {
                shard_id: manifest.shard_id,
                script: script.clone(),
                failures: 1,
            }]
        );
        assert_eq!(store.entry_failures(manifest.shard_id, &script).unwrap(), 1);
        assert_eq!(store.pending().unwrap().len(), 1, "the walk stays owed");
        assert_eq!(store.coverage(&script).unwrap(), vec![]);
        // Refused again, and counted again, on the next attempt.
        let owed = store.pending().unwrap();
        let failed = walk_failing(&mut store, &manifest, &owed, &mut Direct::new(directory));
        assert_eq!(failed[0].failures, 2);
    }

    /// The first fragment of `script`'s run at `(child, row)`, read straight
    /// from the table.
    fn run_head(directory: &Directory, script: &[u8], (child, row): (u8, u32)) -> PageEntry {
        let manifest = manifest_of(directory);
        let page = &manifest.page_children[usize::from(child)];
        let salt = transparent_shard::page_tag_salt(
            manifest.shard_id,
            page.index,
            page.end_height,
            &BlockHash::from_display_hex(&page.terminal_block_hash).unwrap(),
            page.tag_salt_counter,
        );
        let tag = transparent_shard::script_tag(&salt, script);
        transparent_shard::decode_page_row(
            directory.children[usize::from(child)].page_row(&DECOUPLED_2K_8K, u64::from(row)),
        )
        .unwrap()
        .into_iter()
        .find(|e| e.tag == tag && e.ordinal == 0)
        .unwrap()
    }

    fn sorted(requests: &[RowRequest]) -> Vec<(u32, u64)> {
        let mut requests: Vec<(u32, u64)> = requests.iter().map(|r| (r.child, r.row)).collect();
        requests.sort_unstable();
        requests
    }

    /// A fragment that does not decode, in the middle of a run, fails that
    /// script's walk alone. Every round until that run ends sends what the
    /// honest walk sends; the failed walk sends nothing after, and every
    /// other script's history comes back exactly. The failure is counted,
    /// and the walk stays owed from the run it failed in.
    #[test]
    fn a_corrupt_fragment_fails_only_its_walk() {
        // Script 0's one run spans several rows; script 1's is shorter.
        let blocks = vec![(
            1_000,
            (0..200)
                .map(|nonce| (ScriptBytes::new(script(0)), receive(1_000, nonce)))
                .chain((200..230).map(|nonce| (ScriptBytes::new(script(1)), receive(1_000, nonce))))
                .collect(),
        )];
        let directories = build(&blocks);
        let directory = &directories[0];
        let manifest = manifest_of(directory);
        let scripts = [script(0), script(1)];
        let victim = scripts[0].clone();
        let runs = runs_of(directory, &victim);
        let head = run_head(directory, &victim, runs[0]);
        assert!(head.fragment_count >= 3, "{}", head.fragment_count);
        let (child, row) = runs[0];

        let mut honest_store = MemoryStore::new();
        let owed = directory_phase(&mut honest_store, directory, &manifest, &scripts);
        let mut honest = Direct::new(directory);
        walk(&mut honest_store, &manifest, &owed, &mut honest).unwrap();

        let mut store = MemoryStore::new();
        let owed = directory_phase(&mut store, directory, &manifest, &scripts);
        let mut corrupt = Direct::new(directory);
        corrupt
            .corrupt
            .insert((u32::from(child), u64::from(row) + 1));
        let failed = walk_failing(&mut store, &manifest, &owed, &mut corrupt);
        assert_eq!(
            failed,
            vec![FailedEntry {
                shard_id: manifest.shard_id,
                script: victim.clone(),
                failures: 1,
            }]
        );
        // The run's head comes in the first round, the rest of it in the
        // second, where it ends: the transcript is the honest one.
        assert_eq!(corrupt.round_sizes, honest.round_sizes);
        assert_eq!(corrupt.requests, honest.requests);
        assert_eq!(
            stored(&store, &scripts[1]),
            history(&blocks, directory, &scripts[1])
        );
        assert_eq!(store.coverage(&scripts[1]).unwrap().len(), 1);
        let pending = store.pending().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].script, victim);
        assert_eq!(
            (pending[0].cursor.child, pending[0].cursor.row),
            (child, row)
        );
        assert_eq!(pending[0].fragments.len(), 1, "the checked head is kept");
        assert_eq!(store.coverage(&victim).unwrap(), vec![]);
        assert_eq!(store.entry_failures(manifest.shard_id, &victim).unwrap(), 1);

        // The retry resumes after the kept head and, answered honestly,
        // finishes.
        walk(&mut store, &manifest, &pending, &mut Direct::new(directory)).unwrap();
        assert!(store.pending().unwrap().is_empty());
        assert_eq!(
            stored(&store, &victim),
            history(&blocks, directory, &victim)
        );
    }

    /// `victim`'s newest run in `directory`, its head and its row with the
    /// victim's head entry replaced by what `rewrite` makes of it.
    fn rewritten_head(
        directory: &Directory,
        victim: &[u8],
        rewrite: impl Fn(PageEntry) -> PageEntry,
    ) -> ((u8, u32), PageEntry, Vec<u8>) {
        let (child, row) = runs_of(directory, victim)[0];
        let head = run_head(directory, victim, (child, row));
        let entries: Vec<PageEntry> = transparent_shard::decode_page_row(
            directory.children[usize::from(child)].page_row(&DECOUPLED_2K_8K, u64::from(row)),
        )
        .unwrap()
        .into_iter()
        .map(|entry| {
            if entry.tag == head.tag && entry.ordinal == 0 {
                rewrite(entry)
            } else {
                entry
            }
        })
        .collect();
        let bytes = transparent_shard::page_row::encode_page_row(&entries).unwrap();
        ((child, row), head, bytes)
    }

    /// Walks every paged script of the fixture's first directory shard
    /// honestly, and again with `victim`'s newest run's head row replaced by
    /// `segments`: both walks' requests, and the store the second leaves.
    fn honest_and_rewritten(
        victim: &[u8],
        at: (u8, u32),
        segments: Vec<Vec<u8>>,
    ) -> (
        Direct<'static>,
        Direct<'static>,
        MemoryStore,
        Vec<FailedEntry>,
    ) {
        let (_, directories) = fixture();
        let directory = &directories[0];
        let manifest = manifest_of(directory);
        let scripts = paged(directory);
        let mut honest_store = MemoryStore::new();
        let owed = directory_phase(&mut honest_store, directory, &manifest, &scripts);
        let mut honest = Direct::new(directory);
        walk(&mut honest_store, &manifest, &owed, &mut honest).unwrap();

        let mut store = MemoryStore::new();
        let owed = directory_phase(&mut store, directory, &manifest, &scripts);
        let mut bad = Direct::new(directory);
        bad.rewritten
            .insert((u32::from(at.0), u64::from(at.1)), segments);
        let failed = walk_failing(&mut store, &manifest, &owed, &mut bad);
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0].script, victim);
        for script in scripts.iter().filter(|script| **script != victim) {
            assert_eq!(
                stored(&store, script),
                history(&fixture().0, directory, script)
            );
        }
        // The head was not kept: the next attempt asks for it again.
        let pending = store.pending().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!((pending[0].cursor.child, pending[0].cursor.row), at);
        assert!(pending[0].fragments.is_empty());
        (honest, bad, store, failed)
    }

    /// A run whose head is otherwise valid but whose link is not fails its
    /// walk when the head arrives. In the next round the walk still sends the
    /// rest of that run and one query in the link's place, as dummies to the
    /// run's own page shard, so every round is the honest round's size until
    /// that run ends; then it stops, and every other walk finishes.
    fn a_bad_link_is_padded(rewrite: impl Fn(RunLink) -> RunLink) {
        let (_, directories) = fixture();
        let directory = &directories[0];
        let victim = paged(directory).remove(0);
        let ((child, row), head, bytes) = rewritten_head(directory, &victim, |entry| {
            let link = rewrite(entry.link);
            entry.with_link(link).unwrap()
        });
        let (honest, bad, ..) = honest_and_rewritten(&victim, (child, row), vec![bytes]);
        assert_eq!(bad.round_sizes[..2], honest.round_sizes[..2]);
        assert_eq!(sorted(bad.round(0)), sorted(honest.round(0)));
        // The victim's honest second round: the rest of its newest run, and
        // the head of the run it links to.
        let mut theirs: Vec<(u32, u64)> = (1..head.fragment_count)
            .map(|ordinal| (u32::from(child), u64::from(row + ordinal)))
            .collect();
        let (older, older_row) = head.link.previous().expect("a linked run");
        theirs.push((u32::from(older), u64::from(older_row)));
        let mut expected = sorted(honest.round(1));
        for request in &theirs {
            let at = expected.iter().position(|held| held == request).unwrap();
            expected.remove(at);
        }
        expected.extend((0..head.fragment_count).map(|_| (u32::from(child), 0)));
        expected.sort_unstable();
        assert_eq!(sorted(bad.round(1)), expected);
        for (failing, honest) in bad.round_sizes[2..].iter().zip(&honest.round_sizes[2..]) {
            assert!(failing <= honest);
        }
    }

    /// A link to a row outside the page shard's table.
    #[test]
    fn a_link_outside_the_tables_fails_only_its_walk() {
        a_bad_link_is_padded(|link| RunLink {
            prev_row: 60_000,
            ..link
        });
    }

    /// A link to page table 7 of a shard that declares fewer: it is not an
    /// earlier page shard, and is refused as one, but the walk still sends
    /// what the honest walk would.
    #[test]
    fn a_link_to_an_undeclared_page_table_fails_only_its_walk() {
        let (_, directories) = fixture();
        let declared = manifest_of(&directories[0]).page_children.len();
        assert!((3..7).contains(&declared), "{declared} page shards");
        a_bad_link_is_padded(|link| RunLink {
            prev_child: 7,
            ..link
        });
    }

    /// An `older_runs` the chain did not promise is the link's defect too.
    #[test]
    fn an_older_runs_mismatch_fails_only_its_walk() {
        a_bad_link_is_padded(|link| RunLink {
            older_runs: link.older_runs + 1,
            ..link
        });
    }

    /// A head that cannot be believed at all, here held twice across the
    /// reply's segments, fails its walk with nothing more sent for it: what
    /// it would have committed the walk to is unknown.
    #[test]
    fn a_fragment_held_twice_fails_only_its_walk() {
        let (_, directories) = fixture();
        let directory = &directories[0];
        let victim = paged(directory).remove(0);
        let (at, head, bytes) = rewritten_head(directory, &victim, |entry| entry);
        // A second segment holding the victim's first fragment alone.
        let again = transparent_shard::page_row::encode_page_row(&[head]).unwrap();
        let (honest, bad, ..) = honest_and_rewritten(&victim, at, vec![bytes, again]);
        assert_eq!(bad.round(0), honest.round(0));
        assert!(bad.requests.len() < honest.requests.len());
    }

    /// The row a link located holds the fragment under another entry's tag,
    /// not the victim's: the walk fails, and only it.
    #[test]
    fn a_fragment_owned_by_another_entry_fails_only_its_walk() {
        let (_, directories) = fixture();
        let directory = &directories[0];
        let victim = paged(directory).remove(0);
        let (at, _, bytes) = rewritten_head(directory, &victim, |mut entry| {
            entry.tag = [0xee; transparent_shard::SCRIPT_TAG_BYTES];
            entry
        });
        let (honest, bad, ..) = honest_and_rewritten(&victim, at, vec![bytes]);
        assert_eq!(bad.round(0), honest.round(0));
        assert!(bad.requests.len() < honest.requests.len());
    }

    /// An overload refusing the round a failed walk owed its dummies in does
    /// not lose them: the retried read sends them first, and the failure is
    /// counted once.
    #[test]
    fn dummies_owed_before_an_overload_are_sent_on_the_retry() {
        let (_, directories) = fixture();
        let directory = &directories[0];
        let manifest = manifest_of(directory);
        let scripts = paged(directory);
        let victim = scripts[0].clone();
        let ((child, row), head, bytes) = rewritten_head(directory, &victim, |entry| {
            let link = RunLink {
                prev_row: 60_000,
                ..entry.link
            };
            entry.with_link(link).unwrap()
        });
        let mut store = MemoryStore::new();
        let owed = directory_phase(&mut store, directory, &manifest, &scripts);
        let mut failures = Failures::default();
        let mut refused = Direct::new(directory);
        refused
            .rewritten
            .insert((u32::from(child), u64::from(row)), vec![bytes]);
        refused.overloaded = Some(1);
        let error = walk_chains(
            &mut store,
            &entry_of(&manifest),
            &manifest,
            &owed,
            &mut refused,
            &target_of(&manifest),
            &mut failures,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            SyncError::Client(ClientError::Overloaded(_))
        ));
        assert_eq!(
            failures.owes(manifest.shard_id, &victim),
            Some((child, head.fragment_count))
        );

        let mut retry = Direct::new(directory);
        let owed = store.pending().unwrap();
        walk_chains(
            &mut store,
            &entry_of(&manifest),
            &manifest,
            &owed,
            &mut retry,
            &target_of(&manifest),
            &mut failures,
        )
        .unwrap();
        let dummies = retry
            .round(0)
            .iter()
            .filter(|request| (request.child, request.row) == (u32::from(child), 0))
            .count();
        assert!(dummies >= head.fragment_count as usize, "{dummies}");
        assert!(failures.owed.is_empty());
        assert_eq!(failures.entries.len(), 1);
        assert_eq!(store.entry_failures(manifest.shard_id, &victim).unwrap(), 1);
    }

    /// A directory row whose content does not decode fails its entry, on
    /// content, whichever candidate row it was; the honest replies find the
    /// entry.
    #[test]
    fn a_corrupt_directory_reply_fails_only_its_entry() {
        let (_, directories) = fixture();
        let directory = &directories[0];
        let manifest = manifest_of(directory);
        let entry = entry_of(&manifest);
        let built = &directory.built;
        let script = paged(directory).remove(0);
        let salt = transparent_shard::tag_salt(
            built.shard_id,
            &hash_at(built.end_height),
            built.tag_salt_counter,
        );
        let wanted = transparent_shard::script_tag(&salt, &script);
        let replies = |corrupt: Option<usize>| -> Vec<(u64, Reply)> {
            candidate_rows(built.shard_id, &script, built.geometry.directory_rows)
                .into_iter()
                .enumerate()
                .map(|(index, row)| {
                    let mut bytes = built.directory_row(row).to_vec();
                    if corrupt == Some(index) {
                        bytes.iter_mut().for_each(|byte| *byte ^= 0xa5);
                    }
                    (row, vec![bytes])
                })
                .collect()
        };
        let found = entry_in(&entry, &manifest, built.geometry, &wanted, &replies(None))
            .unwrap()
            .expect("the entry");
        assert_eq!(
            found.head(),
            directory_entry(directory, &script).unwrap().head()
        );
        for corrupt in 0..2 {
            assert!(matches!(
                entry_in(
                    &entry,
                    &manifest,
                    built.geometry,
                    &wanted,
                    &replies(Some(corrupt))
                ),
                Err(SyncError::Content(_))
            ));
        }
    }

    /// A walk resumes from its durable cursor after any round, and the
    /// history it ends with is the same.
    #[test]
    fn a_walk_resumes_from_its_cursor() {
        let (blocks, directories) = fixture();
        let directory = &directories[0];
        let manifest = manifest_of(directory);
        let script = paged(directory).remove(0);
        let mut store = MemoryStore::new();
        let owed = directory_phase(
            &mut store,
            directory,
            &manifest,
            std::slice::from_ref(&script),
        );
        struct Limited<'a> {
            inner: Direct<'a>,
            rounds: usize,
        }
        impl RowSource for Limited<'_> {
            fn stopped(&self) -> Option<IncompleteReason> {
                (self.inner.rounds >= self.rounds).then_some(IncompleteReason::QueryBudget)
            }
            fn fetch<S: WalletStore>(
                &mut self,
                store: &mut S,
                requests: &[RowRequest],
            ) -> Result<Vec<Reply>, SyncError> {
                self.inner.fetch(store, requests)
            }
        }
        let mut limited = Limited {
            inner: Direct::new(directory),
            rounds: 2,
        };
        let stopped = walk_chains(
            &mut store,
            &entry_of(&manifest),
            &manifest,
            &owed,
            &mut limited,
            &target_of(&manifest),
            &mut Failures::default(),
        )
        .unwrap();
        assert!(matches!(
            stopped,
            Some(Completion::Incomplete {
                reason: IncompleteReason::QueryBudget,
                pending: 1
            })
        ));
        let resumed = store.pending().unwrap();
        assert!(resumed[0].cursor.older_runs.is_some());
        walk(&mut store, &manifest, &resumed, &mut Direct::new(directory)).unwrap();
        assert!(store.pending().unwrap().is_empty());
        assert_eq!(stored(&store, &script), history(blocks, directory, &script));
    }

    /// Every pass may read only one row, including when a run spans many rows.
    /// Saved progress must eventually recover the same history without rereading rows.
    #[test]
    fn one_query_passes_finish_a_history_without_repeating_rows() {
        let (blocks, directories) = fixture();
        let directory = &directories[0];
        let manifest = manifest_of(directory);
        let script = paged(directory).remove(0);
        let mut store = MemoryStore::new();
        directory_phase(
            &mut store,
            directory,
            &manifest,
            std::slice::from_ref(&script),
        );
        struct OneQuery<'a> {
            inner: Direct<'a>,
        }
        impl RowSource for OneQuery<'_> {
            fn stopped(&self) -> Option<IncompleteReason> {
                (!self.inner.requests.is_empty()).then_some(IncompleteReason::QueryBudget)
            }
            fn fetch<S: WalletStore>(
                &mut self,
                store: &mut S,
                requests: &[RowRequest],
            ) -> Result<Vec<Reply>, SyncError> {
                let remaining = usize::from(self.inner.requests.is_empty());
                self.inner
                    .fetch(store, &requests[..requests.len().min(remaining)])
            }
        }
        let mut queried = std::collections::BTreeSet::new();
        for _ in 0..500 {
            let owed = store.pending().unwrap();
            if owed.is_empty() {
                break;
            }
            let mut rows = OneQuery {
                inner: Direct::new(directory),
            };
            walk_chains(
                &mut store,
                &entry_of(&manifest),
                &manifest,
                &owed,
                &mut rows,
                &target_of(&manifest),
                &mut Failures::default(),
            )
            .unwrap();
            assert_eq!(rows.inner.requests.len(), 1);
            for request in rows.inner.requests {
                assert!(
                    queried.insert((request.child, request.row)),
                    "a pass repeated a saved fragment"
                );
            }
        }
        assert!(
            store.pending().unwrap().is_empty(),
            "one-query passes must finish"
        );
        assert_eq!(stored(&store, &script), history(blocks, directory, &script));
    }

    /// A short run with no remaining requests can finish after an earlier
    /// script uses the last query of a truncated round.
    #[test]
    fn a_partial_round_finishes_a_later_run_without_requests() {
        let blocks = vec![(
            1_000,
            (0..200)
                .map(|nonce| (ScriptBytes::new(script(0)), receive(1_000, nonce)))
                .chain((200..230).map(|nonce| (ScriptBytes::new(script(1)), receive(1_000, nonce))))
                .collect(),
        )];
        let directories = build(&blocks);
        let directory = &directories[0];
        let manifest = manifest_of(directory);
        let scripts = [script(0), script(1)];
        for (script, multiple) in scripts.iter().zip([true, false]) {
            let runs = runs_of(directory, script);
            assert_eq!(runs.len(), 1);
            let (child, row) = runs[0];
            let entries = transparent_shard::decode_page_row(
                directory.children[child as usize].page_row(&DECOUPLED_2K_8K, row.into()),
            )
            .unwrap();
            assert!(entries
                .iter()
                .any(|entry| entry.ordinal == 0 && (entry.fragment_count > 1) == multiple));
        }
        let mut store = MemoryStore::new();
        let owed = directory_phase(&mut store, directory, &manifest, &scripts);
        struct ThreeQueries<'a> {
            inner: Direct<'a>,
        }
        impl RowSource for ThreeQueries<'_> {
            fn stopped(&self) -> Option<IncompleteReason> {
                (self.inner.requests.len() >= 3).then_some(IncompleteReason::QueryBudget)
            }
            fn fetch<S: WalletStore>(
                &mut self,
                store: &mut S,
                requests: &[RowRequest],
            ) -> Result<Vec<Reply>, SyncError> {
                let remaining = 3usize.saturating_sub(self.inner.requests.len());
                self.inner
                    .fetch(store, &requests[..requests.len().min(remaining)])
            }
        }
        let mut rows = ThreeQueries {
            inner: Direct::new(directory),
        };
        walk_chains(
            &mut store,
            &entry_of(&manifest),
            &manifest,
            &owed,
            &mut rows,
            &target_of(&manifest),
            &mut Failures::default(),
        )
        .unwrap();
        assert_eq!(rows.inner.requests.len(), 3);
        assert_eq!(
            stored(&store, &scripts[1]),
            history(&blocks, directory, &scripts[1])
        );
        let pending = store.pending().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].script, scripts[0]);
        walk(&mut store, &manifest, &pending, &mut Direct::new(directory)).unwrap();
        assert!(store.pending().unwrap().is_empty());
        for script in &scripts {
            assert_eq!(stored(&store, script), history(&blocks, directory, script));
        }
    }

    fn head(_child: u8, link: RunLink) -> PageEntry {
        PageEntry::new([1; 14], 0, 1, vec![receive(10, 1)])
            .unwrap()
            .with_link(link)
            .unwrap()
    }

    fn cursor(child: u8, older_runs: Option<u8>) -> RunCursor {
        RunCursor {
            child,
            row: 0,
            older_runs,
        }
    }

    fn link(prev_child: u8, prev_row: u32, older_runs: u8) -> RunLink {
        RunLink {
            prev_child,
            prev_row,
            older_runs,
        }
    }

    /// A chain that ends where its count says more runs remain is cut short.
    #[test]
    fn a_truncated_chain_is_refused() {
        let end = head(3, RunLink::default());
        assert!(check_head(&end, &cursor(3, Some(0))).is_ok());
        assert!(check_head(&end, &cursor(3, Some(1))).is_err());
        assert!(check_head(&end, &cursor(3, Some(2))).is_err());
    }

    /// `older_runs` drops by exactly one per hop, and never claims more runs
    /// than there are earlier page shards.
    #[test]
    fn an_older_runs_mismatch_is_refused() {
        let linked = head(5, link(2, 9, 2));
        assert!(check_head(&linked, &cursor(5, Some(2))).is_ok());
        assert!(check_head(&linked, &cursor(5, None)).is_ok());
        assert!(check_head(&linked, &cursor(5, Some(1))).is_err());
        assert!(check_head(&linked, &cursor(5, Some(3))).is_err());
        // A chain that carries on where it should have ended.
        assert!(check_head(&linked, &cursor(5, Some(0))).is_err());
        // Two older runs need two earlier page shards.
        assert!(check_head(&head(1, link(0, 9, 2)), &cursor(1, None)).is_err());
    }

    /// A link points only to an earlier page shard.
    #[test]
    fn a_forward_or_self_pointer_is_refused() {
        assert!(check_head(&head(4, link(3, 1, 1)), &cursor(4, None)).is_ok());
        assert!(check_head(&head(4, link(4, 1, 1)), &cursor(4, None)).is_err());
        assert!(check_head(&head(4, link(5, 1, 1)), &cursor(4, None)).is_err());
    }

    fn page_child(start: u64, end: u64) -> PageChild {
        PageChild {
            index: 1,
            start_height: start,
            end_height: end,
            terminal_block_hash: "00".repeat(32),
            tag_salt_counter: 0,
            sealed: true,
            page_segments: Vec::new(),
            occupancy: PageChildOccupancy {
                page_rows: 0,
                fragments: 0,
                runs: 0,
                events: 0,
            },
        }
    }

    /// The design's worked example: P1 = 1300-1649 holds e2@1020, pushed by
    /// e4@1400; P2 = 1650-1999 holds e3@1100 and e4@1400, pushed by e5@1700
    /// and e6@1710. Anywhere else, or out of order, is refused.
    #[test]
    fn push_out_placement_and_order_are_checked() {
        let e = |height: u32| receive(height, height);
        let carried = [e(1700), e(1710)];
        assert!(check_run(&page_child(1650, 1999), &[e(1100), e(1400)], &carried).is_ok());
        assert!(check_run(&page_child(1300, 1649), &[e(1020)], &[e(1100), e(1400)]).is_ok());
        // e2 placed in P0 = 1000-1299 would have been pushed by e4@1400.
        assert!(check_run(&page_child(1000, 1299), &[e(1020)], &[e(1100), e(1400)]).is_err());
        assert!(check_run(&page_child(1705, 1999), &[e(1100), e(1400)], &carried).is_err());
        // Out of order, or not older than what follows.
        assert!(check_run(&page_child(1650, 1999), &[e(1400), e(1100)], &carried).is_err());
        assert!(check_run(&page_child(1650, 1999), &[e(1100), e(1705)], &carried).is_err());
        assert!(check_run(&page_child(1650, 1999), &[e(1100)], &carried[..1]).is_err());
    }
}
