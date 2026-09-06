//! Birthday to balance: the join between public filters and private retrieval.
//!
//! This is the path the whole design exists to make cheap:
//!
//! 1. fetch the published shard map and binary-search it for the birthday;
//! 2. download **every** filter from that shard forward and match locally;
//! 3. for each matched script and shard, privately retrieve both candidate
//!    directory rows, then the pages the decoded record locates;
//! 4. replay the events into a UTXO set.
//!
//! Step 2 downloads every filter in range, not only the ones that will match.
//! Which filters a wallet asks for must not be a function of its scripts.
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
use transparent_shard::records::{decode_directory_row, DirectoryEntry, Page};

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
}

/// The geometry a service declares, checked against this build's constants.
pub struct ServiceGeometry {
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
    /// Height through which coverage is complete.
    pub covered_through: u64,
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
                entry.shard_id,
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
        covered_through = entry.end_height;
    }

    let mut ledger = Ledger::new();
    ledger.replay(&mut events)?;

    Ok(SyncOutcome {
        ledger,
        charges,
        matched_shards,
        unproductive_matches: unproductive,
        covered_through,
    })
}

struct Recovered {
    events: Vec<(Vec<u8>, TransparentEvent)>,
    unproductive: u64,
}

/// Retrieves every matched script's history from one shard.
fn retrieve_shard(
    shard_id: u64,
    matches: &[usize],
    scripts: &[ScriptBytes],
    directory: &mut TableClient,
    pages: &mut TableClient,
    transport: &mut impl ShardTransport,
    charges: &mut ByteCharges,
) -> Result<Recovered, SyncError> {
    open_table(shard_id, Table::Directory, directory, transport, charges)?;

    let mut events = Vec::new();
    let mut unproductive = 0u64;
    let mut needs_pages = Vec::new();

    for index in matches {
        let script = scripts
            .get(*index)
            .ok_or_else(|| SyncError::Invalid("match index outside the script set".into()))?;

        // Both candidate rows are queried. Querying only the first and stopping
        // on a hit would make the number of queries depend on where the script
        // landed, which is a function of the script.
        let mut found: Option<DirectoryEntry> = None;
        for row in candidate_rows(
            shard_id,
            script.as_slice(),
            transparent_shard::DIRECTORY_ROWS as u64,
        ) {
            let raw = directory.fetch_row(transport, shard_id, row as usize, charges)?;
            for entry in decode_directory_row(&raw)? {
                // The row is selected by a hash, and a hash can collide or be
                // misplaced. The exact script bytes are what settle it.
                if entry.script == script.as_slice() {
                    if found.is_some() {
                        return Err(SyncError::Invalid(format!(
                            "shard {shard_id} holds a script twice"
                        )));
                    }
                    found = Some(entry);
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
        open_table(shard_id, Table::Pages, pages, transport, charges)?;
        for entry in needs_pages {
            let mut recovered = entry.inline.len() as u32;
            for ordinal in 0..entry.page_count {
                let row = entry.first_page + ordinal;
                let raw = pages.fetch_row(transport, shard_id, row as usize, charges)?;
                let page = Page::decode(&raw)?.ok_or_else(|| {
                    SyncError::Invalid(format!(
                        "shard {shard_id} directory locates an empty page row"
                    ))
                })?;
                // A directory entry could point anywhere; the page's own header
                // is what says whose history it holds.
                if page.script != entry.script
                    || page.ordinal != ordinal
                    || page.page_count != entry.page_count
                {
                    return Err(SyncError::Invalid(format!(
                        "shard {shard_id} page {row} does not belong to the entry that located it"
                    )));
                }
                recovered += page.events.len() as u32;
                for event in page.events {
                    events.push((entry.script.clone(), event));
                }
            }
            // The directory promised a total; the pages must account for it, or
            // the wallet cannot tell a complete history from a truncated one.
            if recovered != entry.total_events {
                return Err(SyncError::Invalid(format!(
                    "shard {shard_id} yielded {recovered} events where the directory promised {}",
                    entry.total_events
                )));
            }
        }
    }

    Ok(Recovered {
        events,
        unproductive,
    })
}

/// Fetches a shard's published setup for one table, once.
fn open_table(
    shard_id: u64,
    table: Table,
    client: &mut TableClient,
    transport: &mut impl ShardTransport,
    charges: &mut ByteCharges,
) -> Result<(), SyncError> {
    if client.is_open(shard_id) {
        return Ok(());
    }
    let (raw, cost) = transport
        .setup(shard_id, table)
        .map_err(|error| SyncError::Transport(error.to_string()))?;
    charges.setup_bytes += cost;
    charges.shards_opened += 1;
    let parsed: serde_json::Value =
        serde_json::from_slice(&raw).map_err(|error| SyncError::Transport(error.to_string()))?;
    let params = parsed["public_params"]
        .as_str()
        .ok_or_else(|| SyncError::Invalid("setup has no public_params".into()))?;
    let digest = parsed["public_params_sha256"]
        .as_str()
        .ok_or_else(|| SyncError::Invalid("setup has no digest".into()))?;
    client.open_shard(shard_id, params, digest)?;
    Ok(())
}
