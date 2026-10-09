//! Synthetic display journals for the controller's tests.

use super::publisher::DisplayRoot;
use crate::events::EventStore;
use crate::publication::Journal;
use sha2::{Digest, Sha256};
use std::path::Path;
use transparent_events::{FeeState, TransactionMetadata, Txid};
use transparent_filter::BlockHash;
use transparent_shard::display::{DisplaySealParams, TXID_2K};
use transparent_shard::txid::{DisplayOutput, DisplayRecord};
use transparent_shard::txid_v1::TransparentDisplayRecord;
use transparent_shard::txid_v2x::{DisplayInput, TransparentDisplayRecordV2x};

/// The published entry of a non-coinbase transaction spending one P2PKH
/// output into `outputs` P2PKH outputs; more than two set the entry's
/// omission flag.
pub(crate) fn record(id: u32, outputs: usize) -> DisplayRecord {
    record_tagged(0, id, outputs)
}

pub(crate) fn record_tagged(tag: u8, id: u32, outputs: usize) -> DisplayRecord {
    source_tagged(tag, id, outputs)
        .display_record()
        .expect("a fixture entry is valid")
}

/// The journal's source record of [`record_tagged`]: the transaction with
/// its one input's spent output.
pub(crate) fn source_tagged(tag: u8, id: u32, outputs: usize) -> TransparentDisplayRecordV2x {
    let mut seed = id.to_le_bytes().to_vec();
    seed.push(tag);
    let digest = |i: u8| Sha256::digest([&seed[..], &[i]].concat());
    let p2pkh = |i: u8| [&[0x76, 0xa9, 0x14][..], &digest(i)[..20], &[0x88, 0xac]].concat();
    let outputs: Vec<DisplayOutput> = (0..outputs)
        .map(|i| DisplayOutput {
            value: 5 + i as u64,
            script: p2pkh(i as u8),
        })
        .collect();
    let fee = 1_000;
    let input = DisplayInput {
        prevout_txid: Txid(digest(u8::MAX - 1).into()),
        prevout_index: 0,
        value: fee + outputs.iter().map(|o| o.value).sum::<u64>(),
        script: p2pkh(u8::MAX),
    };
    TransparentDisplayRecordV2x {
        record: TransparentDisplayRecord {
            txid: Txid(Sha256::digest(&seed).into()),
            coinbase: false,
            metadata: TransactionMetadata {
                fee: FeeState::Exact(fee),
                transparent_input_count: 1,
                has_shielded_components: false,
            },
            outputs,
        },
        inputs: vec![input],
    }
}

pub(crate) fn block_hash(tag: u8, height: u64) -> BlockHash {
    let mut seed = height.to_le_bytes().to_vec();
    seed.push(tag);
    BlockHash::from_internal_bytes(Sha256::digest(&seed).into())
}

/// Two to four source records a block of one or two outputs, one in about
/// fifteen with more than two.
pub(crate) fn chain_sources(tag: u8, height: u64) -> Vec<TransparentDisplayRecordV2x> {
    let count = 2 + (height % 3) as u32;
    (0..count)
        .map(|i| {
            let id = height as u32 * 8 + i;
            let outputs = if id % 15 == 7 {
                3
            } else {
                1 + (id % 2) as usize
            };
            source_tagged(tag, id, outputs)
        })
        .collect()
}

/// The published entries of [`chain_sources`].
pub(crate) fn chain_records(tag: u8, height: u64) -> Vec<DisplayRecord> {
    chain_sources(tag, height)
        .iter()
        .map(|source| source.display_record().expect("a fixture entry is valid"))
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
        let sources = chain_sources(tag, height);
        let events = crate::display_journal::implied_events(height, &sources).unwrap();
        store
            .append_block_with_display(height, block_hash(tag, height), &events, &sources)
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
