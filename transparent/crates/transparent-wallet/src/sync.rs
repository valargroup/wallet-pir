//! Birthday to balance: the join between public filters and private retrieval.
//!
//! This is the path the whole design exists to make cheap:
//!
//! 1. fetch the published shard map and binary-search it for the birthday;
//! 2. download **every** filter from that shard forward and match locally;
//! 3. for each matched script and shard, privately retrieve both candidate
//!    directory rows, then the pages the decoded extent locates;
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
    uncovered, Anchor, CoverageKind, CoverageRange, PendingPages, ScriptEntry, ScriptOrigin,
    SetIdentity, SetupBlob, SetupKey, ShardCommit, StoreError, StoredEvent, WalletStore,
};
use crate::transport::{ByteCharges, FilterSource, ShardTransport, StaleRevision};
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::time::Duration;
use transparent_filter::{BlockHash, FilterLimits, ScriptBytes, ShardKey, ShardMap};
use transparent_shard::build::candidate_rows;
use transparent_shard::manifest::ShardManifest;
use transparent_shard::page_row::decode_page_row;
use transparent_shard::records::{decode_directory_row, DirectoryEntry};

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
    pub directory_scheme: ipir_sp::YpirSchemeParams,
    pub directory_setup_seed: u64,
    pub page_rows: u64,
    pub page_row_bytes: u32,
    pub pages_scheme: ipir_sp::YpirSchemeParams,
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
                directory: TableClient::new(
                    Table::Directory,
                    geometry.directory_rows,
                    geometry.directory_row_bytes as u32,
                    params.directory_setup_seed,
                    &params.directory_scheme,
                )?,
                pages: TableClient::new(
                    Table::Pages,
                    geometry.page_rows,
                    geometry.page_row_bytes as u32,
                    params.pages_setup_seed,
                    &params.pages_scheme,
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
    if geometry.schema != transparent_shard::SCHEMA {
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
    let identity = SetIdentity::of(map);
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

    // Reorg detection over what the store holds. Every distinct block the
    // coverage rests on is asked of the wallet's chain, newest first; the
    // first rejected one rolls everything above the highest accepted block
    // below it back. Sealed coverage the map now describes differently is
    // the same finding from the publication's side.
    let mut rests_on: BTreeMap<u64, String> = BTreeMap::new();
    for entry in store.scripts()? {
        for range in store.coverage(&entry.script)? {
            rests_on
                .entry(range.end_height)
                .or_insert(range.terminal_block_hash.clone());
            if range.kind == CoverageKind::Settled {
                if let Some(published) = map.shards.get(range.shard_id as usize) {
                    if published.sealed
                        && range
                            .source_anchor
                            .as_ref()
                            .is_none_or(|source| range.end_height == source.height)
                        && published.start_height == range.start_height
                        && (published.manifest_digest != range.revision_digest
                            || range.source_anchor.as_ref().is_some_and(|source| {
                                published.end_height != source.height
                                    || published.terminal_block_hash != source.hash
                            }))
                    {
                        // Sealed content is immutable; a different sealed
                        // shard at the same place is another chain.
                        rests_on.insert(range.end_height, "reorganised".into());
                    }
                }
            }
        }
    }
    let mut ancestor: Option<u64> = None;
    let mut unknown: Option<u64> = None;
    for (height, hash) in rests_on.iter().rev() {
        let acceptance = if hash == "reorganised" {
            Acceptance::Rejected
        } else {
            chain.is_accepted(*height, hash)
        };
        match acceptance {
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
        if ancestor.is_none() && !rests_on.is_empty() {
            // Nothing could be confirmed. Reading on would either accept a
            // branch the wallet has not seen or roll back on a guess.
            let ledger = store.ledger()?;
            let (covered, settled) = coverage_summary(store, map, target_anchor.height)?;
            return Ok(SyncReport {
                ledger,
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
            });
        }
    }

    // Provisional reconciliation. A tail revision the map has moved past is
    // truncated and re-derived; one the map has sealed is promoted.
    let mut truncate_from: Option<u64> = None;
    let mut promote: Vec<(u64, String)> = Vec::new();
    // A partially retrieved revision may have events and pending pages but no
    // completed coverage yet. Replacing it must rewind those events too.
    for pending in store.pending()? {
        let published = map.shards.get(pending.shard_id as usize).ok_or_else(|| {
            SyncError::MapDiverged(format!(
                "publication withdraws pending shard {}",
                pending.shard_id
            ))
        })?;
        if published.manifest_digest != pending.revision_digest {
            truncate_from = Some(
                truncate_from.map_or(published.start_height, |h| h.min(published.start_height)),
            );
            if !replaced_revisions.contains(&pending.revision_digest) {
                replaced_revisions.push(pending.revision_digest);
            }
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
        let below = start.saturating_sub(1);
        if rolled_back_to.is_none_or(|held| held > below) {
            store.rollback_above(
                &accepted_at(chain, map, below)?,
                "provisional tail replaced",
            )?;
            rolled_back_to = Some(below);
        }
    }
    for (shard_id, digest) in promote {
        store.promote_provisional(shard_id, &digest)?;
    }

    let discard: Vec<u64> = store
        .pending()?
        .iter()
        .filter(|p| p.target_anchor.as_ref() != Some(target_anchor))
        .filter_map(|p| p.id)
        .collect();
    if !discard.is_empty() {
        store.commit_shard(ShardCommit {
            pending_complete: discard,
            ..Default::default()
        })?;
    }

    // The wallet's script set, with its own required heights.
    store.add_scripts(&scripts.scripts())?;
    let mut scripts_added = 0usize;
    let mut clients = Clients {
        prepared: HashMap::new(),
    };
    let mut matched_shards: Vec<u64> = Vec::new();
    let mut unproductive = 0u64;
    let mut refreshes = 0u32;
    let mut held_map_digest: Option<String> = None;
    let mut active: Cow<'_, ShardMap> = Cow::Borrowed(map);
    let mut completion = Completion::Complete;

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
        for ((shard_id, digest), items) in by_revision {
            let Some(entry) = active.shards.get(shard_id as usize) else {
                continue;
            };
            if entry.manifest_digest != digest {
                // Its revision is gone; the reconciliation above rolled its
                // coverage back and the ordinary planning will re-derive it.
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
                |store, prepared, transport, charges, _| {
                    finish_pages(
                        store,
                        &entry,
                        &items,
                        prepared,
                        transport,
                        charges,
                        limits,
                        target_anchor,
                        chain,
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
                    for item in &items {
                        if store.events()?.iter().any(|e| {
                            e.script == item.script
                                && e.shard_id == shard_id
                                && event_height(&e.event) <= target_anchor.height
                        }) && !active_scripts.contains(&item.script)
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
            completion = Completion::Incomplete {
                reason: IncompleteReason::DiscoveryUnbounded,
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
    })
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
) -> Result<ShardRead, SyncError> {
    let endpoint = if entry.end_height > target_anchor.height {
        target_anchor.clone()
    } else {
        Anchor {
            height: entry.end_height,
            hash: entry.terminal_block_hash.clone(),
        }
    };
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
        let profile = transparent_filter::range_profile(&map.profile)
            .ok_or_else(|| SyncError::UnknownProfile(map.profile.clone()))?;
        let validated =
            transparent_filter::validate_range_filter(&bytes, FilterLimits::default(), profile)?;
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
        transparent_filter::match_range_scripts(&validated, key, &owned)?
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
    let matched_scripts: Vec<Vec<u8>> = matches.iter().map(|&i| scripts[i].clone()).collect();
    let unmatched: Vec<Vec<u8>> = scripts
        .iter()
        .filter(|script| !matched_scripts.contains(script))
        .cloned()
        .collect();

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
            retrieve_shard_into(
                store,
                entry,
                &matched_scripts,
                &unmatched,
                choice.as_ref(),
                prepared,
                transport,
                charges,
                limits,
                target_anchor,
                chain,
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
                let has_history = events
                    .iter()
                    .any(|stored| stored.script == *script && stored.shard_id == entry.shard_id);
                if has_history {
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
                Ok(verified) => manifest.insert(verified),
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
    if manifest.schema != transparent_shard::SCHEMA {
        return Err(SyncError::Schema {
            served: manifest.schema.clone(),
            expected: transparent_shard::SCHEMA,
        });
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
    if manifest.directory_segments.len() as u32 != entry.directory_segments
        || manifest.page_segments.len() as u32 != entry.page_segments
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
    for segment in &manifest.page_segments {
        if segment.rows != geometry.page_rows || segment.row_bytes != geometry.page_row_bytes as u32
        {
            return Err(mismatch("page_segment_shape"));
        }
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

/// The directory phase for one shard: both candidate rows for every matched
/// script, inline events committed, page work recorded; then the pages.
#[allow(clippy::too_many_arguments)]
fn retrieve_shard_into<S: WalletStore>(
    store: &mut S,
    entry: &transparent_filter::ShardMapEntry,
    matched: &[Vec<u8>],
    unmatched: &[Vec<u8>],
    choice: Option<&transparent_shard::ChoiceTable>,
    clients: &mut GeometryClients,
    transport: &mut impl ShardTransport,
    charges: &mut ByteCharges,
    limits: &WorkLimits,
    target_anchor: &Anchor,
    chain: &impl ChainView,
) -> Result<Option<Completion>, SyncError> {
    let shard_id = entry.shard_id;
    let geometry = transparent_shard::layout::by_name(&entry.geometry)
        .ok_or_else(|| SyncError::UnknownGeometry(entry.geometry.clone()))?;
    let revision = entry.manifest_digest.as_str();
    // An overload can occur after the directory commit but before pages finish.
    // with_refusals calls this function again on retry. Resume those durable
    // entries first: rediscovering their directories would enqueue duplicate
    // work and leave the original entries pending after the retry succeeds.
    let pending: Vec<_> = store
        .pending()?
        .into_iter()
        .filter(|p| p.shard_id == shard_id && p.revision_digest == revision)
        .collect();
    if !pending.is_empty() {
        if let Some(stopped) = finish_pages(
            store,
            entry,
            &pending,
            clients,
            transport,
            charges,
            limits,
            target_anchor,
            chain,
        )? {
            return Ok(Some(stopped));
        }
    }
    let matched: Vec<_> = matched
        .iter()
        .filter(|script| !pending.iter().any(|p| &p.script == *script))
        .collect();
    if matched.is_empty() {
        // The earlier directory commit also covered all unmatched scripts.
        return Ok(None);
    }
    let set_digest = store
        .set_identity()?
        .map(|identity| identity.digest())
        .unwrap_or_default();
    let GeometryClients { directory, pages } = clients;
    open_table(
        store,
        &set_digest,
        shard_id,
        revision,
        Table::Directory,
        entry.directory_segments,
        directory,
        transport,
        charges,
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

    for script in matched {
        // With a published choice table, exactly the one candidate row it
        // names is queried. Without one, both candidate rows are. Either way
        // the query count per matched script is fixed for the shard: querying
        // the first and stopping on a hit would make it depend on where the
        // script landed, which is a function of the script. Candidates are
        // taken over the shard's whole logical row space; each names one row
        // within a segment, and every segment answers it.
        let mut found: Option<(u64, DirectoryEntry)> = None;
        let rows = geometry.directory_rows * entry.directory_segments as u64;
        let candidates = candidate_rows(shard_id, script, rows);
        let queried: &[u64] = match choice {
            Some(table) => std::slice::from_ref(&candidates[table.choice(shard_id, script)]),
            None => &candidates,
        };
        for &row in queried {
            let (_, within) = transparent_shard::layout::split_row(row, geometry.directory_rows);
            let answers = directory.fetch_row(
                transport,
                shard_id,
                revision,
                entry.directory_segments,
                within as usize,
                charges,
            )?;
            for raw in answers {
                for candidate in decode_directory_row(&raw)? {
                    // The row is selected by a hash, and a hash can collide or
                    // be misplaced; the segment is not named at all. The exact
                    // script bytes are what settle both.
                    if candidate.script == *script {
                        match &found {
                            // The two candidate hashes can name the same row
                            // (one script in 8,192 at recent-8k). The row is
                            // still fetched twice, so the query count does not
                            // depend on the script, and the second sighting is
                            // the same entry, not a second one.
                            Some((seen_row, _)) if *seen_row == row => {}
                            Some(_) => {
                                return Err(SyncError::Invalid(format!(
                                    "shard {shard_id} holds a script twice"
                                )));
                            }
                            None => found = Some((row, candidate)),
                        }
                    }
                }
            }
        }
        let found = found.map(|(_, entry)| entry);
        match found {
            // A filter match with no directory entry is either a false positive
            // or a script outside the private tables' coverage. Both are
            // wasted work rather than errors; coverage still advances.
            None => covered.push(script.clone()),
            Some(found) => {
                if found.total_events < found.inline.len() as u32
                    || (found.page_count == 0 && found.total_events != found.inline.len() as u32)
                {
                    return Err(SyncError::Invalid(
                        "inline directory total disagrees with records".into(),
                    ));
                }
                for event in &found.inline {
                    events.push(StoredEvent {
                        script: found.script.clone(),
                        event: *event,
                        shard_id,
                        revision_digest: revision.to_string(),
                    });
                }
                if found.page_count > 0 {
                    pending_upsert.push(PendingPages {
                        validated_events: found.inline.len() as u32,
                        target_anchor: Some(target_anchor.clone()),
                        id: None,
                        shard_id,
                        revision_digest: revision.to_string(),
                        script: found.script.clone(),
                        first_page: found.first_page,
                        page_count: found.page_count,
                        total_events: found.total_events,
                        inline: found.inline.clone(),
                        next_ordinal: 0,
                        attempts: 0,
                    });
                } else {
                    covered.push(script.clone());
                }
            }
        }
    }

    // The directory phase is one commit: inline events, coverage for every
    // script that needs no pages, and the page work owed for the rest. Page
    // retrieval that follows resumes from the store even if nothing else of
    // this call survives.
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
        },
    )?;
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
    let _ = pages;
    finish_pages(
        store,
        entry,
        &owed,
        clients,
        transport,
        charges,
        limits,
        target_anchor,
        chain,
    )
}

/// Fetches the pages still owed for one shard revision, committing each
/// page's events with the pending item's progress, and closing each item
/// with its script's coverage when its total is accounted for.
#[allow(clippy::too_many_arguments)]
fn finish_pages<S: WalletStore>(
    store: &mut S,
    entry: &transparent_filter::ShardMapEntry,
    owed: &[PendingPages],
    clients: &mut GeometryClients,
    transport: &mut impl ShardTransport,
    charges: &mut ByteCharges,
    limits: &WorkLimits,
    target_anchor: &Anchor,
    chain: &impl ChainView,
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
    let shard_id = entry.shard_id;
    let geometry = transparent_shard::layout::by_name(&entry.geometry)
        .ok_or_else(|| SyncError::UnknownGeometry(entry.geometry.clone()))?;
    let revision = entry.manifest_digest.as_str();
    let set_digest = store
        .set_identity()?
        .map(|identity| identity.digest())
        .unwrap_or_default();
    let pages = &mut clients.pages;
    open_table(
        store,
        &set_digest,
        shard_id,
        revision,
        Table::Pages,
        entry.page_segments,
        pages,
        transport,
        charges,
    )?;
    for item in owed {
        let mut recovered = item.validated_events;
        if recovered > item.total_events {
            return Err(SyncError::Invalid(
                "pending validation count exceeds directory total".into(),
            ));
        }
        for ordinal in item.next_ordinal..item.page_count {
            if let Some(reason) = budget_stopped(charges, limits) {
                return Ok(Some(Completion::Incomplete {
                    reason,
                    pending: store.pending()?.len(),
                }));
            }
            // The extent indexes the shard's page space, which is its segments
            // concatenated. It must still land inside that space: a directory
            // entry is a claim like any other.
            let row = item.first_page.checked_add(ordinal).ok_or_else(|| {
                SyncError::Invalid(format!("shard {shard_id} page extent overflows"))
            })?;
            let space = geometry.page_rows * u64::from(entry.page_segments);
            if u64::from(row) >= space {
                return Err(SyncError::Invalid(format!(
                    "shard {shard_id} page {row} is outside its {space}-row table"
                )));
            }
            let (_, within) = transparent_shard::layout::split_row(row as u64, geometry.page_rows);
            let answers = pages.fetch_row(
                transport,
                shard_id,
                revision,
                entry.page_segments,
                within as usize,
                charges,
            )?;
            // A directory entry could point anywhere, and every segment
            // answered; an entry's own header is what says whose history it
            // holds, and exactly one may claim this fragment.
            //
            // A packed row also carries other scripts' histories. Those are
            // discarded, and deliberately not used: two of a wallet's own
            // scripts sharing a row still cost two fetches. Satisfying the
            // second from the first response would make the request
            // transcript depend on which scripts share a row, which is a
            // fact about other people's history and not this wallet's.
            let mut fragment = None;
            for raw in answers {
                for candidate in decode_page_row(&raw)? {
                    if candidate.script == item.script
                        && candidate.ordinal == ordinal
                        && candidate.fragment_count == item.page_count
                    {
                        if fragment.is_some() {
                            return Err(SyncError::Invalid(format!(
                                "shard {shard_id} holds page {row} twice"
                            )));
                        }
                        fragment = Some(candidate);
                    }
                }
            }
            let fragment = fragment.ok_or_else(|| {
                SyncError::Invalid(format!(
                    "shard {shard_id} page {row} does not belong to the entry that located it"
                ))
            })?;
            recovered = recovered
                .checked_add(fragment.events.len() as u32)
                .ok_or_else(|| SyncError::Invalid("page event count overflow".into()))?;
            if recovered > item.total_events {
                return Err(SyncError::Invalid(
                    "pages exceed the directory event total".into(),
                ));
            }
            let events: Vec<StoredEvent> = fragment
                .events
                .iter()
                .map(|event| StoredEvent {
                    script: item.script.clone(),
                    event: *event,
                    shard_id,
                    revision_digest: revision.to_string(),
                })
                .collect();
            let last = ordinal + 1 == item.page_count;
            // The directory promised a total; the pages must account for it,
            // or the wallet cannot tell a complete history from a truncated one.
            if last && recovered != item.total_events {
                return Err(SyncError::Invalid(format!(
                    "shard {shard_id} yielded {recovered} events where the directory promised {}",
                    item.total_events
                )));
            }
            let mut progressed = item.clone();
            progressed.next_ordinal = ordinal + 1;
            progressed.validated_events = recovered;
            progressed.attempts += 1;
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
                    covered_scripts: if last {
                        vec![item.script.clone()]
                    } else {
                        Vec::new()
                    },
                    pending_upsert: if last { Vec::new() } else { vec![progressed] },
                    pending_complete: if last {
                        item.id.into_iter().collect()
                    } else {
                        Vec::new()
                    },
                },
            )?;
        }
    }
    Ok(None)
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
) -> Result<(), SyncError> {
    if segments == 0 {
        return Err(SyncError::Invalid(format!(
            "shard {shard_id} declares no {} segments",
            table.as_str()
        )));
    }
    for segment in 0..segments {
        if client.is_open(revision, segment) {
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
        client.open_segment(revision, segment, &params, &digest)?;
        // Kept only after the client verified it against its digest.
        store.put_setup(
            &key,
            &SetupBlob {
                public_params_base64: params,
                public_params_sha256: digest,
            },
        )?;
    }
    Ok(())
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
        .chain(commit.pending_upsert.iter().flat_map(|p| p.inline.iter()))
    {
        let height = event_height(event);
        if height < commit.start_height || height > source.height {
            return Err(SyncError::Invalid(
                "event lies outside its published shard".into(),
            ));
        }
    }
    commit.source_anchor = Some(source);
    if commit.end_height >= target.height {
        commit.end_height = target.height;
        commit.terminal_block_hash = target.hash.clone();
    }
    commit
        .events
        .retain(|e| event_height(&e.event) <= target.height);
    for pending in &mut commit.pending_upsert {
        pending.inline.retain(|e| event_height(e) <= target.height);
    }
    Ok(store.commit_shard(commit)?)
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
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use transparent_filter::{SealParameters, ShardMapEntry};

    fn entry(shard_id: u64, start: u64, end: u64, sealed: bool) -> ShardMapEntry {
        ShardMapEntry {
            shard_id,
            geometry: "recent-8k".into(),
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
            "recent-8k".to_string(),
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
    #[test]
    fn a_re_cut_covered_shard_is_not_a_continuation() {
        let before = map(vec![entry(0, 100, 199, true), entry(1, 200, 249, false)]);
        let mut after = before.clone();
        after.shards[0].end_height = 149;
        assert!(check_continuation(&before, &after, 199).is_err());
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
        resealed.seal.get_mut("recent-8k").unwrap().max_page_rows = 1;
        assert!(check_continuation(&before, &resealed, 199).is_err());

        let mut widened = before.clone();
        widened.seal.insert(
            "archive-wide".to_string(),
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
            parent_manifest_digest: parent.to_string(),
            sealed: entry.sealed,
            revision: entry.revision,
            supersedes: String::new(),
            seal: ManifestSeal {
                scripts_target: seal.max_scripts,
                scripts_capacity: seal.max_scripts * 2,
                page_rows_target: seal.max_page_rows,
                page_rows_capacity: geometry.page_rows,
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
            page_segments: (0..entry.page_segments)
                .map(|i| TableGeometry {
                    rows: geometry.page_rows,
                    row_bytes: geometry.page_row_bytes as u32,
                    sha256: format!("{:064x}", 2000 + i),
                })
                .collect(),
            occupancy: ManifestOccupancy {
                scripts: 1,
                page_rows: 1,
                fragments: 1,
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
            ("geometry", Box::new(|e| e.geometry = "recent-4k".into())),
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
}
