//! Synthetic display journals for the controller's tests.

use super::publisher::DisplayRoot;
use crate::events::EventStore;
use crate::publication::Journal;
use sha2::{Digest, Sha256};
use std::path::Path;
use transparent_events::{FeeState, TransactionMetadata, Txid};
use transparent_filter::BlockHash;
use transparent_shard::display::{DisplaySealParams, TXID_2K};
use transparent_shard::txid::{DisplayOutput, TransparentDisplayRecord};

/// A non-coinbase record with one output of `script` bytes.
pub(crate) fn record(id: u32, script: usize) -> TransparentDisplayRecord {
    record_tagged(0, id, script)
}

pub(crate) fn record_tagged(tag: u8, id: u32, script: usize) -> TransparentDisplayRecord {
    let mut seed = id.to_le_bytes().to_vec();
    seed.push(tag);
    TransparentDisplayRecord {
        txid: Txid(Sha256::digest(&seed).into()),
        coinbase: false,
        metadata: TransactionMetadata {
            fee: FeeState::Exact(1_000),
            transparent_input_count: 1,
            has_shielded_components: false,
        },
        outputs: vec![DisplayOutput {
            value: 5,
            script: vec![0x51; script],
        }],
    }
}

pub(crate) fn block_hash(tag: u8, height: u64) -> BlockHash {
    let mut seed = height.to_le_bytes().to_vec();
    seed.push(tag);
    BlockHash::from_internal_bytes(Sha256::digest(&seed).into())
}

/// Two to four records a block, one in about fifteen paged.
pub(crate) fn chain_records(tag: u8, height: u64) -> Vec<TransparentDisplayRecord> {
    let count = 2 + (height % 3) as u32;
    (0..count)
        .map(|i| {
            let id = height as u32 * 8 + i;
            let script = if id % 15 == 7 {
                300
            } else {
                20 + (id % 5) as usize
            };
            record_tagged(tag, id, script)
        })
        .collect()
}

pub(crate) fn params() -> DisplaySealParams {
    DisplaySealParams {
        n_archive: 1,
        n_recent: 1,
        archive_target: 10,
        recent_floor: 5,
        reorg_margin: 2,
    }
}

pub(crate) const GENESIS: &str = "00000000000000000000000000000000000000000000000000000000000000aa";

/// Writes blocks `[start, through]` of chain `tag` and closes the journal.
pub(crate) fn write_journal(dir: &Path, start: u64, through: u64, tag: u8) {
    let mut store = EventStore::open(dir, GENESIS, start).unwrap();
    for height in start..=through {
        store
            .append_block_with_display(
                height,
                block_hash(tag, height),
                &[],
                &chain_records(tag, height),
            )
            .unwrap();
    }
    store.commit().unwrap();
}

pub(crate) fn layout(store: &impl Journal, start: u64, max_archive_shards: u64) -> DisplayRoot {
    DisplayRoot {
        geometry: TXID_2K.name.to_string(),
        seal: params(),
        max_archive_shards,
        network: transparent_filter::NETWORK.to_string(),
        genesis_hash: store.genesis_hash().to_string(),
        start_height: start,
        base_parent: store
            .block_at(start - 1)
            .unwrap()
            .block_hash
            .to_display_hex(),
    }
}
