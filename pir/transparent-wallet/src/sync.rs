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

use crate::client::{ClientError, Table, TableClient};
use crate::ledger::{Ledger, LedgerError};
use crate::transport::{ByteCharges, FilterSource, ShardTransport};
use transparent_events::TransparentEvent;
use transparent_filter::{
    validate_filter, BlockHash, FilterLimits, ScriptBytes, ShardKey, ShardMap,
};
use transparent_shard::build::candidate_rows;
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
}

/// The geometry a service declares, checked against this build's constants.
pub struct ServiceGeometry {
    /// The schema the service serves, from its init response.
    ///
    /// A row's meaning is entirely a function of its schema, and the bytes
    /// carry no version of their own — a fixed-width row has nowhere to put
    /// one without spending space on every row. So a wallet that decoded
    /// whatever it was handed would read a newer layout as the one it knows
    /// and reconstruct a plausible, wrong history. This is where that stops.
    pub schema: String,
    pub directory_scheme: ipir_sp::YpirSchemeParams,
    pub directory_setup_seed: u64,
    pub pages_scheme: ipir_sp::YpirSchemeParams,
    pub pages_setup_seed: u64,
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

    let mut directory = TableClient::new(
        Table::Directory,
        transparent_shard::DIRECTORY_ROWS as u64,
        transparent_shard::DIRECTORY_ROW_BYTES as u32,
        geometry.directory_setup_seed,
        &geometry.directory_scheme,
    )?;
    let mut pages = TableClient::new(
        Table::Pages,
        transparent_shard::PAGE_ROWS as u64,
        transparent_shard::PAGE_ROW_BYTES as u32,
        geometry.pages_setup_seed,
        &geometry.pages_scheme,
    )?;

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

    for entry in map.shards.iter().skip(first_shard as usize) {
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
            &map.profile,
            genesis,
            entry.shard_id,
            entry.start_height,
            entry.end_height,
            terminal,
        );
        let matches = transparent_filter::match_range_scripts(&validated, key, scripts)?;

        if !matches.is_empty() {
            matched_shards.push(entry.shard_id);
            let recovered = retrieve_shard(
                entry,
                &matches,
                scripts,
                &mut directory,
                &mut pages,
                transport,
                &mut charges,
            )?;
            unproductive += recovered.unproductive;
            events.extend(recovered.events);
        }
        // Reached only after every segment of this shard was retrieved and
        // validated: an error above leaves coverage where it was.
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
    })
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
    directory: &mut TableClient,
    pages: &mut TableClient,
    transport: &mut impl ShardTransport,
    charges: &mut ByteCharges,
) -> Result<Recovered, SyncError> {
    let shard_id = entry.shard_id;
    open_table(
        shard_id,
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
        let rows = transparent_shard::DIRECTORY_ROWS as u64 * entry.directory_segments as u64;
        for row in candidate_rows(shard_id, script.as_slice(), rows) {
            let (_, within) =
                transparent_shard::layout::split_row(row, transparent_shard::DIRECTORY_ROWS as u64);
            let answers = directory.fetch_row(
                transport,
                shard_id,
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
                let space = transparent_shard::PAGE_ROWS as u64 * u64::from(entry.page_segments);
                if u64::from(row) >= space {
                    return Err(SyncError::Invalid(format!(
                        "shard {shard_id} page {row} is outside its {space}-row table"
                    )));
                }
                let (_, within) = transparent_shard::layout::split_row(
                    row as u64,
                    transparent_shard::PAGE_ROWS as u64,
                );
                let answers = pages.fetch_row(
                    transport,
                    shard_id,
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
        if client.is_open(shard_id, segment) {
            continue;
        }
        let (raw, cost) = transport
            .setup(shard_id, table, segment)
            .map_err(|error| SyncError::Transport(error.to_string()))?;
        charges.setup_bytes += cost;
        charges.shards_opened += 1;
        let parsed: serde_json::Value = serde_json::from_slice(&raw)
            .map_err(|error| SyncError::Transport(error.to_string()))?;
        let params = parsed["public_params"]
            .as_str()
            .ok_or_else(|| SyncError::Invalid("setup has no public_params".into()))?;
        let digest = parsed["public_params_sha256"]
            .as_str()
            .ok_or_else(|| SyncError::Invalid("setup has no digest".into()))?;
        client.open_segment(shard_id, segment, params, digest)?;
    }
    Ok(())
}
