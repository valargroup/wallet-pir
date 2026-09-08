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

use crate::client::{classify_transport, ClientError, Table, TableClient};
use crate::ledger::{Ledger, LedgerError};
use crate::transport::{ByteCharges, FilterSource, ShardTransport, StaleRevision};
use std::borrow::Cow;
use std::collections::HashMap;
use std::time::Duration;
use transparent_events::TransparentEvent;
use transparent_filter::{
    validate_filter, BlockHash, FilterLimits, ScriptBytes, ShardKey, ShardMap,
};
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

/// Ceiling on any single wait, so a service asking for an implausible delay
/// cannot park a sync inside it.
const OVERLOAD_BACKOFF_CAP: Duration = Duration::from_secs(2);

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
                    "the service declares geometry {name} with dimensions this build does not                      know it by"
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

/// Runs one sync from `birthday` to the end of the published map.
///
/// `scripts` is the wallet's derived script set. It never leaves the machine:
/// matching is local, and a private query names a row, not a script.
pub fn sync(
    map: &ShardMap,
    map_bytes: u64,
    geometry: &ServiceGeometry,
    filters: &mut impl FilterSource,
    transport: &mut impl ShardTransport,
    scripts: &[ScriptBytes],
    birthday: u64,
) -> Result<SyncOutcome, SyncError> {
    if geometry.schema != transparent_shard::SCHEMA {
        return Err(SyncError::Schema {
            served: geometry.schema.clone(),
            expected: transparent_shard::SCHEMA,
        });
    }
    map.check_shape()
        .map_err(|error| SyncError::Invalid(format!("shard map is malformed: {error}")))?;
    let genesis = BlockHash::from_display_hex(&map.genesis_hash)?;

    // With content-sealed shards there is no width to divide by, so the map is
    // the only way to turn a height into a shard. A birthday before coverage
    // begins starts at shard zero; one past the tip covers nothing.
    let first_shard = if birthday <= map.start_height {
        0
    } else {
        map.shard_for_height(birthday)
            .map(|shard| shard.shard_id)
            .ok_or_else(|| {
                SyncError::Invalid(format!("birthday {birthday} is beyond published coverage"))
            })?
    };

    let mut clients = Clients {
        prepared: HashMap::new(),
    };

    let mut charges = ByteCharges {
        map_bytes,
        ..Default::default()
    };
    let mut matched_shards = Vec::new();
    let mut unproductive = 0u64;
    let mut events: Vec<(Vec<u8>, TransparentEvent)> = Vec::new();
    let mut covered_through = birthday.max(map.start_height).saturating_sub(1);
    let mut settled_through = covered_through;
    let mut provisional = Vec::new();

    let mut active: Cow<'_, ShardMap> = Cow::Borrowed(map);
    let mut index = first_shard as usize;
    let mut refreshes = 0u32;
    // Only a map this sync fetched itself can be compared with the digest the
    // service reports. The caller passed a parsed map and a byte count, not the
    // bytes, so there is nothing to hash until a refresh produces some.
    let mut held_map_digest: Option<String> = None;

    while index < active.shards.len() {
        // Cloned so the borrow of `active` ends before a refresh may replace
        // it. An entry is a handful of short strings; the filter download it
        // gates dwarfs the copy.
        let entry = active.shards[index].clone();
        let previous_digest = index
            .checked_sub(1)
            .map(|before| active.shards[before].manifest_digest.clone())
            .unwrap_or_default();
        let read = read_shard(
            &entry,
            &previous_digest,
            &active,
            genesis,
            scripts,
            geometry,
            &mut clients,
            filters,
            transport,
            &mut charges,
        );
        match read {
            Ok(recovered) => {
                if let Some(recovered) = recovered {
                    // Pushed after the retrieval, not before it: a shard that
                    // was re-derived under a later revision would otherwise
                    // appear twice, and "matched" can only observably mean
                    // matched and read.
                    matched_shards.push(entry.shard_id);
                    unproductive += recovered.unproductive;
                    events.extend(recovered.events);
                }
                // Reached only after every segment of this shard was retrieved
                // and validated: an error above leaves coverage where it was.
                covered_through = entry.end_height;
                if entry.sealed {
                    settled_through = entry.end_height;
                } else {
                    provisional.push(ProvisionalCoverage {
                        shard_id: entry.shard_id,
                        revision: entry.revision,
                        manifest_digest: entry.manifest_digest.clone(),
                        end_height: entry.end_height,
                    });
                }
                index += 1;
            }
            Err(SyncError::StaleRevision { stale, .. }) => {
                // The service withdrew this revision while the sync was reading
                // it. Refresh the map, check it is the same set continued, and
                // re-derive this range from whatever replaced it. Everything
                // read from earlier shards stands; everything this attempt read
                // is discarded, and the bytes it cost are still charged.
                if refreshes >= MAX_MAP_REFRESHES
                    || (stale.map_sha256.is_some()
                        && stale.map_sha256.as_deref() == held_map_digest.as_deref())
                {
                    // Either the budget is gone, or the service says the map
                    // this sync already holds is the current one — so refetching
                    // it again cannot produce a live revision.
                    return Err(SyncError::StaleRevision {
                        shard_id: entry.shard_id,
                        stale,
                        refreshes,
                    });
                }
                let (fresh, digest) = refresh_map(&active, covered_through, filters, &mut charges)?;
                refreshes += 1;
                held_map_digest = Some(digest);
                let Some(resume) = fresh.shard_for_height(covered_through + 1).map(|shard| {
                    fresh
                        .shards
                        .iter()
                        .position(|candidate| candidate.shard_id == shard.shard_id)
                        .expect("shard_for_height returns an entry of this map")
                }) else {
                    // The refreshed map no longer advertises anything past what
                    // this sync already covered. `check_continuation` has
                    // established that it does not end *below* that, so nothing
                    // was skipped: coverage is exactly what it would have been
                    // had the withdrawn tail never been advertised at all.
                    break;
                };
                if fresh.shards[resume].manifest_digest == stale.revision {
                    // The map still names the revision the service refused.
                    // They disagree with each other, and asking again loops.
                    return Err(SyncError::StaleRevision {
                        shard_id: entry.shard_id,
                        stale,
                        refreshes,
                    });
                }
                // `check_continuation` has established that the refreshed map
                // agrees about every range already covered, so the shard
                // resumed at begins exactly where coverage left off. Retracting
                // provisional records past that point is therefore a no-op, but
                // it is the rule `ProvisionalCoverage` documents — a re-derived
                // range replaces rather than extends — and it belongs written
                // where the re-derivation happens.
                let resume_start = fresh.shards[resume].start_height;
                provisional.retain(|covered| covered.end_height < resume_start);
                covered_through = resume_start.saturating_sub(1);
                active = Cow::Owned(fresh);
                index = resume;
            }
            Err(other) => return Err(other),
        }
    }

    let mut ledger = Ledger::new();
    ledger.replay(&mut events)?;

    Ok(SyncOutcome {
        ledger,
        charges,
        matched_shards,
        unproductive_matches: unproductive,
        covered_through,
        settled_through,
        provisional,
        map_refreshes: refreshes,
    })
}

/// Reads one shard: its filter always, its history only if a script matched.
///
/// `Ok(None)` means the filter matched nothing, which is a genuinely empty
/// range rather than one that went unretrieved — the filter is built over the
/// shard's own scripts and is independent of the table shape, so passing over
/// it is safe even for a geometry this build cannot decode. That is what lets
/// an old wallet sync the recent window across an archive tier it does not
/// know. It stops the moment there is something to fetch: an unknown geometry
/// on a *matched* shard is fatal, because the alternative is reporting a
/// synchronised balance over history that was never read.
///
/// The filter is fetched here rather than by the caller because a republished
/// shard publishes a new filter with its new content. Re-deriving a shard
/// therefore means re-matching it, not merely re-querying it.
#[allow(clippy::too_many_arguments)]
fn read_shard(
    entry: &transparent_filter::ShardMapEntry,
    previous_digest: &str,
    map: &ShardMap,
    genesis: BlockHash,
    scripts: &[ScriptBytes],
    geometry: &ServiceGeometry,
    clients: &mut Clients,
    filters: &mut impl FilterSource,
    transport: &mut impl ShardTransport,
    charges: &mut ByteCharges,
) -> Result<Option<Recovered>, SyncError> {
    let profile = map.profile.as_str();
    // Every filter in range is downloaded, matched or not.
    let (bytes, cost) = filters
        .filter(entry.shard_id)
        .map_err(|error| SyncError::Transport(error.to_string()))?;
    charges.filter_bytes += cost;
    charges.filters_checked += 1;

    // Bind the bytes to the map before believing anything they say.
    if transparent_filter::filter_hash(&bytes).to_display_hex() != entry.filter_hash {
        return Err(SyncError::Invalid(format!(
            "shard {} filter does not match its published digest",
            entry.shard_id
        )));
    }
    let validated = validate_filter(&bytes, FilterLimits::default())?;
    let terminal = BlockHash::from_display_hex(&entry.terminal_block_hash)?;
    let key = ShardKey::derive(
        profile,
        genesis,
        entry.shard_id,
        entry.start_height,
        entry.end_height,
        terminal,
    );
    let matches = transparent_filter::match_range_scripts(&validated, key, scripts)?;
    if matches.is_empty() {
        return Ok(None);
    }

    // A geometry this build does not know is fatal the moment there is
    // something to fetch, whatever the manifest would say about it.
    if transparent_shard::layout::by_name(&entry.geometry).is_none() {
        return Err(SyncError::UnknownGeometry(entry.geometry.clone()));
    }

    // Retried here, one level inside the map-refresh loop, because an overload
    // says nothing about the map: refetching it would be pure cost, and the
    // same revision is still the right one to ask for.
    //
    // The manifest is fetched and checked before any private request, once
    // per revision this sync touches. What it establishes is that the map,
    // the manifest and the registry tell one story about this shard; it does
    // not establish that the story is complete, because the same publisher
    // told all of it. Its fetch meets the same two refusals a private request
    // can, and they are lifted the same way.
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
                        return Err(SyncError::Overloaded {
                            shard_id: entry.shard_id,
                            attempts: attempt,
                        });
                    }
                    std::thread::sleep(overload_backoff(attempt, overloaded.retry_after));
                    attempt += 1;
                    continue;
                }
                Err(other) => return Err(other),
            },
        };
        // The geometry is the verified manifest's, which `verify_manifest`
        // has shown to be the map's as well.
        let prepared = clients.prepare(&verified.geometry, &geometry.geometries)?;
        match retrieve_shard(entry, &matches, scripts, prepared, transport, charges) {
            Ok(recovered) => return Ok(Some(recovered)),
            // Both refusals reach here wrapped as client errors, because setup
            // and query both report through `ClientError`. Lifting them to
            // their own `SyncError` variants at this one boundary is what lets
            // the caller's loop match on a withdrawn revision without knowing
            // which of the two requests met it.
            Err(SyncError::Client(ClientError::Stale(stale))) => {
                return Err(SyncError::StaleRevision {
                    shard_id: entry.shard_id,
                    stale,
                    refreshes: 0,
                })
            }
            Err(SyncError::Client(ClientError::Overloaded(overloaded))) => {
                if attempt >= MAX_OVERLOAD_ATTEMPTS {
                    return Err(SyncError::Overloaded {
                        shard_id: entry.shard_id,
                        attempts: attempt,
                    });
                }
                // Nothing partial is kept: `retrieve_shard` returns its events
                // by value, so a refused attempt leaves no state to unwind.
                std::thread::sleep(overload_backoff(attempt, overloaded.retry_after));
                attempt += 1;
            }
            Err(other) => return Err(other),
        }
    }
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
    Ok(manifest)
}

/// How long to wait before re-asking an overloaded service.
///
/// The service's own figure is preferred, because it knows why it refused, but
/// it is capped: a delay a wallet cannot sanity-check is a delay a
/// misconfigured or hostile service could use to park a sync inside one call.
fn overload_backoff(attempt: u32, asked: Option<Duration>) -> Duration {
    let wait = asked.unwrap_or_else(|| OVERLOAD_BACKOFF * 2u32.saturating_pow(attempt - 1));
    wait.min(OVERLOAD_BACKOFF_CAP)
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
        .map_err(|error| SyncError::Transport(error.to_string()))?;
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

struct Recovered {
    events: Vec<(Vec<u8>, TransparentEvent)>,
    unproductive: u64,
}

/// Retrieves every matched script's history from one shard, across all of its
/// segments.
fn retrieve_shard(
    entry: &transparent_filter::ShardMapEntry,
    matches: &[usize],
    scripts: &[ScriptBytes],
    clients: &mut GeometryClients,
    transport: &mut impl ShardTransport,
    charges: &mut ByteCharges,
) -> Result<Recovered, SyncError> {
    let shard_id = entry.shard_id;
    // The geometry is resolved once per shard rather than per row: the map
    // named it, `Clients::prepare` checked it against the registry, and every
    // offset below is taken against the same entry.
    let geometry = transparent_shard::layout::by_name(&entry.geometry)
        .ok_or_else(|| SyncError::UnknownGeometry(entry.geometry.clone()))?;
    let revision = entry.manifest_digest.as_str();
    let GeometryClients { directory, pages } = clients;
    open_table(
        shard_id,
        revision,
        Table::Directory,
        entry.directory_segments,
        directory,
        transport,
        charges,
    )?;

    let mut events = Vec::new();
    let mut unproductive = 0u64;
    let mut needs_pages = Vec::new();

    for index in matches {
        let script = scripts
            .get(*index)
            .ok_or_else(|| SyncError::Invalid("match index outside the script set".into()))?;

        // Both candidate rows are queried. Querying only the first and stopping
        // on a hit would make the number of queries depend on where the script
        // landed, which is a function of the script. Candidates are taken over
        // the shard's whole logical row space; each names one row within a
        // segment, and every segment answers it.
        let mut found: Option<DirectoryEntry> = None;
        let rows = geometry.directory_rows * entry.directory_segments as u64;
        for row in candidate_rows(shard_id, script.as_slice(), rows) {
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
                    if candidate.script == script.as_slice() {
                        if found.is_some() {
                            return Err(SyncError::Invalid(format!(
                                "shard {shard_id} holds a script twice"
                            )));
                        }
                        found = Some(candidate);
                    }
                }
            }
        }

        match found {
            // A filter match with no directory entry is either a false positive
            // or a script outside the private tables' coverage. Both are
            // wasted work rather than errors, and both are counted.
            None => unproductive += 1,
            Some(entry) => {
                for event in &entry.inline {
                    events.push((entry.script.clone(), *event));
                }
                if entry.page_count > 0 {
                    needs_pages.push(entry);
                }
            }
        }
    }

    if !needs_pages.is_empty() {
        open_table(
            shard_id,
            revision,
            Table::Pages,
            entry.page_segments,
            pages,
            transport,
            charges,
        )?;
        for found in needs_pages {
            let mut recovered = found.inline.len() as u32;
            for ordinal in 0..found.page_count {
                // The extent indexes the shard's page space, which is its
                // segments concatenated, so an extent that runs past a segment
                // boundary needs no special handling here. It must still land
                // inside that space: a directory entry is a claim like any
                // other, and one pointing past the table would otherwise be
                // answered by whatever the arithmetic wrapped onto.
                let row = found.first_page.checked_add(ordinal).ok_or_else(|| {
                    SyncError::Invalid(format!("shard {shard_id} page extent overflows"))
                })?;
                let space = geometry.page_rows * u64::from(entry.page_segments);
                if u64::from(row) >= space {
                    return Err(SyncError::Invalid(format!(
                        "shard {shard_id} page {row} is outside its {space}-row table"
                    )));
                }
                let (_, within) =
                    transparent_shard::layout::split_row(row as u64, geometry.page_rows);
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
                        if candidate.script == found.script
                            && candidate.ordinal == ordinal
                            && candidate.fragment_count == found.page_count
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
                recovered += fragment.events.len() as u32;
                for event in fragment.events {
                    events.push((found.script.clone(), event));
                }
            }
            // The directory promised a total; the pages must account for it, or
            // the wallet cannot tell a complete history from a truncated one.
            if recovered != found.total_events {
                return Err(SyncError::Invalid(format!(
                    "shard {shard_id} yielded {recovered} events where the directory promised {}",
                    found.total_events
                )));
            }
        }
    }

    Ok(Recovered {
        events,
        unproductive,
    })
}

/// Fetches the published setup for every segment of a shard's table, once.
///
/// Each segment publishes its own `c1`, so each is opened separately; a shard
/// with one segment — the ordinary case — costs exactly one setup per table.
fn open_table(
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
        let (raw, cost) = transport
            .setup(shard_id, revision, table, segment)
            .map_err(|error| SyncError::from(classify_transport(error)))?;
        charges.add_setup(table, cost);
        let parsed: serde_json::Value = serde_json::from_slice(&raw)
            .map_err(|error| SyncError::Transport(error.to_string()))?;
        let params = parsed["public_params"]
            .as_str()
            .ok_or_else(|| SyncError::Invalid("setup has no public_params".into()))?;
        let digest = parsed["public_params_sha256"]
            .as_str()
            .ok_or_else(|| SyncError::Invalid("setup has no digest".into()))?;
        client.open_segment(revision, segment, params, digest)?;
    }
    Ok(())
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
        assert_eq!(overload_backoff(1, None), OVERLOAD_BACKOFF);
        assert_eq!(overload_backoff(2, None), OVERLOAD_BACKOFF * 2);
        assert_eq!(
            overload_backoff(1, Some(Duration::from_millis(10))),
            Duration::from_millis(10),
            "the service knows why it refused"
        );
        assert_eq!(
            overload_backoff(1, Some(Duration::from_secs(3600))),
            OVERLOAD_BACKOFF_CAP,
            "a delay a wallet cannot sanity-check must not park a sync"
        );
        assert_eq!(
            overload_backoff(20, None),
            OVERLOAD_BACKOFF_CAP,
            "doubling is capped too"
        );
    }
}
