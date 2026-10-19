//! Offline tools around the display controller: verifying a root against its
//! journal, planning a bootstrap start, benching recent shards and writing
//! synthetic journals. None of them serve or publish to workers.

use zakura_pir_protocol::transparent::events as transparent_events;
use zakura_pir_publisher::transparent as transparent_shard;
use zakura_pir_publisher::transparent::filter as transparent_filter;

use super::publisher::{read_map, verify_dir, ActiveRecord, DisplayRoot, SealRecord, ShardSpec};
use crate::history::publication::{BoxError, Journal};
use serde_json::{json, Value};
use std::path::Path;
use transparent_shard::display::plan_seals;
use transparent_shard::display::split::{index_file, RECENT_MAP_FILE};
use transparent_shared::journal::events::EventStore;

/// Re-bootstraps into a scratch root at the journal end and checks every
/// recorded seal against it; verifies every revision of the active candidate.
///
/// A seal's range depends only on the chain below it, but whether the rule
/// has made it yet depends on blocks above it. Seals the journal makes beyond
/// the recorded ones are queued, not wrong. Recorded seals beyond the
/// reproduced ones were made on blocks a later reorg replaced: each is
/// rebuilt from its recorded parent and must match its range and digest.
pub fn verify(root: &Path, journal: &Path, scratch: Option<&Path>) -> Result<Value, BoxError> {
    let layout = DisplayRoot::load(root)?;
    let active = ActiveRecord::load(root)?;
    let map = read_map(&active.directory, &active.map_sha256)?;
    let mut problems = Vec::new();
    for entry in &map.shards {
        match verify_dir(
            &active.directory.join(&entry.manifest_digest),
            &entry.manifest_digest,
        ) {
            Ok(manifest) if entry.describes(&manifest) => {}
            Ok(_) => problems.push(format!(
                "shard {} manifest differs from the map",
                entry.shard_id
            )),
            Err(error) => problems.push(format!("shard {}: {error}", entry.shard_id)),
        }
    }
    // The split map, when the candidate has one, must be the full map's.
    let split = map.split()?;
    let split_written = active.directory.join(RECENT_MAP_FILE).exists();
    if split_written {
        let files = std::iter::once((RECENT_MAP_FILE.to_string(), &split.recent_bytes)).chain(
            split
                .chunks
                .iter()
                .map(|chunk| (index_file(&chunk.sha256), &chunk.bytes)),
        );
        for (name, expected) in files {
            if std::fs::read(active.directory.join(&name)).ok().as_ref() != Some(expected) {
                problems.push(format!("{name} differs from the split of the map"));
            }
        }
    }
    let store = EventStore::open_existing(journal)?;
    let through = store.covered_through().ok_or("the journal is empty")?;
    let mut report = json!({"map_sha256": active.map_sha256, "revisions_verified": map.shards.len(),
        "recent_map_sha256": split.recent_sha256, "index_chunks": split.chunks.len(),
        "split_written": split_written,
        "seals": active.seals.len(), "archive_chain_sha256": active.archive_chain_sha256(),
        "through": through});
    if !active.seals.is_empty() {
        let scratch = tempfile::Builder::new()
            .prefix(".verify-")
            .tempdir_in(scratch.unwrap_or(root))?;
        let rebuilt = super::publisher::bootstrap(&store, scratch.path(), &layout, through)?.seals;
        let identity = |s: &SealRecord| {
            (
                s.shard_id,
                s.start_height,
                s.end_height,
                s.digest.clone(),
                s.terminal_block_hash.clone(),
            )
        };
        let differs = active
            .seals
            .iter()
            .zip(&rebuilt)
            .position(|(a, b)| identity(a) != identity(b));
        let mut unreproduced = 0;
        if let Some(index) = differs {
            problems.push(format!(
                "seal {index} differs from the journal's: recorded {:?}, reproduced {:?}",
                identity(&active.seals[index]),
                identity(&rebuilt[index])
            ));
        } else {
            for index in rebuilt.len()..active.seals.len() {
                let parent = index.checked_sub(1).map(|i| &active.seals[i]);
                match rebuild_seal(
                    &store,
                    scratch.path(),
                    &layout,
                    parent,
                    &active.seals[index],
                ) {
                    Ok(()) => unreproduced += 1,
                    Err(error) => problems.push(format!("seal {index}: {error}")),
                }
            }
        }
        report["reproduced_seals"] = rebuilt.len().into();
        report["queued"] = rebuilt.len().saturating_sub(active.seals.len()).into();
        report["unreproduced_verified"] = unreproduced.into();
    }
    report["ok"] = problems.is_empty().into();
    report["problems"] = problems.into();
    Ok(report)
}

/// Rebuilds one recorded seal from the journal, after its recorded parent,
/// without the gates that decide when the rule makes it.
fn rebuild_seal(
    store: &EventStore,
    scratch: &Path,
    layout: &DisplayRoot,
    parent: Option<&SealRecord>,
    seal: &SealRecord,
) -> Result<(), BoxError> {
    use transparent_shard::display::{archive_boundary, height_counts};
    let (id, start) = parent.map_or((0, layout.start_height), |p| {
        (p.shard_id + 1, p.end_height + 1)
    });
    let through = store.covered_through().unwrap_or(0).min(seal.end_height);
    let mut counts = Vec::new();
    let mut records = Vec::new();
    for height in start..=through {
        let block = store.display_at(height)?;
        counts.push(height_counts(&layout.seal, &block));
        records.extend(block);
    }
    let end = archive_boundary(&layout.seal, start, &counts);
    if (seal.shard_id, seal.start_height, Some(seal.end_height)) != (id, start, end) {
        return Err(format!(
            "recorded shard {} over {}..={}, the rule makes shard {id} over {start}..={end:?}",
            seal.shard_id, seal.start_height, seal.end_height
        )
        .into());
    }
    let terminal = store
        .block_at(seal.end_height)
        .ok_or("the journal lost a sealed terminal")?
        .block_hash
        .to_display_hex();
    let spec = ShardSpec {
        shard_id: id,
        start,
        end: seal.end_height,
        parent_block_hash: parent.map_or(layout.base_parent.clone(), |p| {
            p.terminal_block_hash.clone()
        }),
        terminal_block_hash: terminal,
        parent_manifest_digest: parent.map_or(String::new(), |p| p.digest.clone()),
        sealed: true,
        previous: None,
    };
    let shard = super::publisher::publish_shard(scratch, layout, &spec, &records)?;
    if (&shard.digest, &spec.terminal_block_hash) != (&seal.digest, &seal.terminal_block_hash) {
        return Err(format!(
            "rebuilt as {} ending at {}, recorded {} ending at {}",
            shard.digest, spec.terminal_block_hash, seal.digest, seal.terminal_block_hash
        )
        .into());
    }
    Ok(())
}

/// Picks a display start and bootstrap end from the journal's counts: the
/// start leaves `archives + replay_seals` seals decided by the journal end,
/// and the end is the last tip before seal `archives + 1` is decided, so the
/// recent shard starts near its largest and replay seals right away.
pub fn plan_start(
    store: &impl Journal,
    seal: &transparent_shard::display::DisplaySealParams,
    archives: u64,
    replay_seals: u64,
    max_archive_shards: u64,
) -> Result<Value, BoxError> {
    use transparent_shard::display::height_counts;
    let end = store.covered_through().ok_or("the journal is empty")?;
    let first = store.start_height() + 1;
    let wanted = (archives + replay_seals) as usize;
    // Read backwards until the rule makes enough seals over [start, end].
    let per_seal = seal.archive_target * u64::from(seal.n_archive);
    let mut reversed = Vec::new();
    let mut records = 0u64;
    let mut target = per_seal * (wanted as u64 + 1) + seal.recent_window * u64::from(seal.n_recent);
    let (start, seals) = loop {
        while records < target && end + 1 - reversed.len() as u64 > first {
            let height = end - reversed.len() as u64;
            let block = store.display_at(height)?;
            records += block.len() as u64;
            reversed.push(height_counts(seal, &block));
        }
        let start = end + 1 - reversed.len() as u64;
        let counts: Vec<_> = reversed.iter().rev().cloned().collect();
        let seals = plan_seals(seal, start, &counts);
        if seals.len() >= wanted || start <= first {
            break (start, seals);
        }
        target += target / 4 + per_seal;
    };
    if seals.len() < wanted || archives == 0 {
        return Err(format!(
            "the journal holds {} seals from {start}; {wanted} wanted",
            seals.len()
        )
        .into());
    }
    // The first tip at which seal j (0-based) is decided: the reorg margin
    // above its end.
    let decided = |j: usize| -> u64 { (*seals[j].end() + seal.reorg_margin).min(end) };
    let through = if (archives as usize) < seals.len() {
        decided(archives as usize) - 1
    } else {
        end
    };
    let at_through = (0..seals.len()).filter(|j| decided(*j) <= through).count();
    let replayed = (0..seals.len()).filter(|j| decided(*j) > through).count();
    let total = at_through + replayed;
    Ok(json!({
        "start": start, "through": through, "journal_start": store.start_height(),
        "journal_end": end, "archives_at_through": at_through, "replay_seals": replayed,
        "expected_drops": (total as u64).saturating_sub(max_archive_shards),
        "records": records,
    }))
}

/// Builds recent shards of synthetic records at each size and reports their
/// table shape and timings, for choosing the archive target.
pub fn bench_recent(
    sizes: &[u64],
    geometry: &str,
    scratch: &Path,
    seed: u64,
) -> Result<Vec<Value>, BoxError> {
    let mut out = Vec::new();
    for &size in sizes {
        let records = synthetic_records(size, seed);
        let temp = tempfile::Builder::new()
            .prefix(".bench-")
            .tempdir_in(scratch)?;
        let layout = DisplayRoot {
            geometry: geometry.to_string(),
            seal: Default::default(),
            max_archive_shards: 1,
            network: transparent_filter::NETWORK.to_string(),
            genesis_hash: "00".repeat(32),
            start_height: 1,
            base_parent: "00".repeat(32),
        };
        let g = layout.validate()?;
        let spec = ShardSpec {
            shard_id: 0,
            start: 1,
            end: 1,
            parent_block_hash: "00".repeat(32),
            terminal_block_hash: "11".repeat(32),
            parent_manifest_digest: String::new(),
            sealed: false,
            previous: None,
        };
        let shard = super::publisher::publish_shard(temp.path(), &layout, &spec, &records)?;
        let segments = shard.entry.directory_segments.iter().sum::<u32>() as u64;
        let slots = segments
            * g.directory_rows
            * transparent_shard::txid::slots_per_row(g.directory_row_bytes) as u64;
        out.push(json!({
            "geometry": geometry, "records": size,
            "directory_segments": shard.entry.directory_segments,
            "used_bytes": shard.used_bytes, "fill": size as f64 / slots as f64,
            "build_s": shard.timings.build_s, "verify_s": shard.timings.verify_s,
            "digest_s": shard.timings.digest_s, "write_s": shard.timings.write_s,
        }));
    }
    Ok(out)
}

/// Genesis identity of journals written by [`synth_journal`]; never a real chain.
pub const SYNTHETIC_GENESIS: &str =
    "5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e";

/// Writes a v3 journal of synthetic blocks `[start, start + blocks)` with
/// display sidecars and the events their sources imply, for local replay and
/// benches.
///
/// Each block holds between half and one and a half times `mean_records`
/// records of [`synthetic_records`]' size mix. Deterministic in `seed`.
pub fn synth_journal(
    out: &std::path::Path,
    start: u64,
    blocks: u64,
    mean_records: u64,
    seed: u64,
) -> Result<u64, BoxError> {
    use sha2::{Digest, Sha256};
    let mut store =
        transparent_shared::journal::events::EventStore::open(out, SYNTHETIC_GENESIS, start)?;
    if store.next_height() != start {
        return Err("synthetic journal directory is not empty".into());
    }
    let mut total = 0;
    for height in start..start + blocks {
        let block_seed = u64::from_le_bytes(
            Sha256::digest([seed.to_le_bytes(), height.to_le_bytes()].concat())[..8]
                .try_into()
                .unwrap(),
        );
        let count = mean_records / 2 + block_seed % (mean_records + 1);
        let sources = synthetic_sources(count, block_seed);
        let hash = transparent_filter::BlockHash::from_internal_bytes(
            Sha256::digest([b"synthetic".as_slice(), &block_seed.to_le_bytes()].concat()).into(),
        );
        let events =
            transparent_shared::journal::display_journal::implied_events(height, &sources)?;
        store.append_block_with_display(height, hash, &events, &sources)?;
        total += count;
        if (height - start + 1).is_multiple_of(1_000) {
            store.commit()?;
        }
    }
    store.commit()?;
    Ok(total)
}

/// Source records with a mainnet-like case mix: mostly one recipient with
/// shielded change or zcashd-style two outputs, then unshielding, several
/// source scripts, more than two outputs and outputs without an address.
/// Every input spends a distinct, synthetic outpoint.
pub fn synthetic_sources(
    count: u64,
    seed: u64,
) -> Vec<transparent_shard::txid_v2x::TransparentDisplayRecordV2x> {
    use sha2::{Digest, Sha256};
    use transparent_events::{FeeState, TransactionMetadata, Txid};
    use transparent_shard::txid::DisplayOutput;
    use transparent_shard::txid_v1::TransparentDisplayRecord;
    use transparent_shard::txid_v2x::{DisplayInput, TransparentDisplayRecordV2x};
    let mut x = seed ^ 0x9e37_79b9_7f4a_7c15;
    let mut next = move || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    };
    let p2pkh = |tag: u64| {
        let mut script = vec![0x76, 0xa9, 0x14];
        script.extend_from_slice(&Sha256::digest(tag.to_le_bytes())[..20]);
        script.extend_from_slice(&[0x88, 0xac]);
        script
    };
    (0..count)
        .map(|i| {
            let txid = Txid(Sha256::digest([seed.to_le_bytes(), i.to_le_bytes()].concat()).into());
            let class = next() % 1_000;
            let (inputs, outputs, distinct, shielded) = match class {
                0..=599 => (1 + next() % 3, 1, false, true),
                600..=849 => (1 + next() % 3, 2, false, false),
                850..=909 => (0, 1, false, true),
                910..=959 => (2 + next() % 4, 1, true, false),
                _ => (1, 3 + next() % 5, false, false),
            };
            let outputs: Vec<_> = (0..outputs)
                .map(|o| DisplayOutput {
                    value: 1_000 + next() % 1_000_000,
                    script: if class >= 990 && o == 0 {
                        vec![0x6a; 1 + (next() % 40) as usize]
                    } else {
                        p2pkh(next())
                    },
                })
                .collect();
            // Extra inputs of one zatoshi each raise the fee by as much;
            // shielded change takes what the first input leaves over.
            let fee = 10_000 + inputs.saturating_sub(1);
            let paid = outputs.iter().map(|o| o.value).sum::<u64>() + 10_000;
            let inputs: Vec<_> = (0..inputs)
                .map(|n| DisplayInput {
                    prevout_txid: Txid(
                        Sha256::digest([txid.0.as_slice(), &n.to_le_bytes()].concat()).into(),
                    ),
                    prevout_index: n as u32,
                    value: if n == 0 {
                        paid + 50_000 * u64::from(shielded)
                    } else {
                        1
                    },
                    script: p2pkh(if distinct {
                        seed ^ i << 8 ^ n
                    } else {
                        seed ^ i << 8
                    }),
                })
                .collect();
            TransparentDisplayRecordV2x {
                record: TransparentDisplayRecord {
                    txid,
                    coinbase: false,
                    metadata: TransactionMetadata {
                        fee: FeeState::Exact(if inputs.is_empty() { 10_000 } else { fee }),
                        transparent_input_count: inputs.len() as u32,
                        has_shielded_components: shielded,
                    },
                    outputs,
                },
                inputs,
            }
        })
        .collect()
}

/// The published entries of [`synthetic_sources`].
pub fn synthetic_records(count: u64, seed: u64) -> Vec<transparent_shard::txid::DisplayRecord> {
    synthetic_sources(count, seed)
        .iter()
        .map(|source| {
            source
                .display_record()
                .expect("a synthetic source is valid")
        })
        .collect()
}
