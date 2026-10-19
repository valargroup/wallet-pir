//! A bounded copy of a committed journal, including hash-addressed sidecars.
//!
//! The source remains read-only, even with a live writer. Copy event bytes in
//! bulk, adjust only block offsets, and compare decoded history before exposing
//! the result. A changed committed endpoint refuses the copy. A failed export
//! keeps its private scratch directory for diagnosis and cannot be resumed as
//! a successful export.
use super::BoxError;
use serde::Serialize;
use std::{
    fs::{self, File},
    io::{Seek, SeekFrom, Write},
    path::Path,
};
use transparent_shared::journal::events::EventStore;
use zakura_pir_protocol::transparent::events::TransparentEvent;

#[derive(Debug, Serialize)]
pub struct Report {
    pub version: u16,
    pub genesis_hash: String,
    pub start_height: u64,
    pub end_height: u64,
    pub end_hash: String,
    pub blocks: u64,
    pub events: u64,
    pub event_bytes: u64,
    pub sidecar_bytes: u64,
    pub blocks_without_sidecars: u64,
}

/// Export inclusive heights into a new directory. Preserve both sidecar
/// families where present. `require_sidecars` refuses absent sidecars, useful
/// when the source contract requires them. Otherwise report that absence rather
/// than inventing history the source never recorded. No chain RPC or writer
/// lock is needed. The destination metadata appears only after verification.
pub fn export(
    source: &Path,
    destination: &Path,
    first: u64,
    last: u64,
    require_sidecars: bool,
) -> Result<Report, BoxError> {
    let journal = EventStore::open_existing(source)?;
    if first > last || first < journal.start_height() {
        return Err("export range is empty or precedes the journal".into());
    }
    let endpoint = journal
        .block_at(last)
        .ok_or("export end is not committed")?;
    let initial = journal
        .block_at(first)
        .ok_or("export start is not committed")?;
    if !journal.still_committed(last, endpoint)? {
        return Err("journal changed before export".into());
    }
    let parent = destination
        .parent()
        .ok_or("destination needs a parent directory")?
        .canonicalize()?;
    if parent.starts_with(source.canonicalize()?) {
        return Err("export destination must be outside the source journal".into());
    }
    // create_dir refuses an existing destination, including a symlink. It also
    // prevents accidentally putting the copy directly over the source journal.
    fs::create_dir(destination)?;
    let scratch = destination.join("incomplete");
    fs::create_dir(&scratch)?;
    let (mut snapshot, offset) = journal.snapshot_suffix(first, &scratch)?;
    let bytes = journal
        .block_at(last + 1)
        .map_or(snapshot.metadata()?.len(), |next| next.offset - offset);
    snapshot.set_len(bytes)?;
    snapshot.seek(SeekFrom::Start(0))?;
    let mut events_file = File::create(scratch.join("events.bin"))?;
    if std::io::copy(&mut snapshot, &mut events_file)? != bytes {
        return Err("journal event copy was shortened".into());
    }
    events_file.sync_all()?;
    let mut block_file = std::io::BufWriter::new(File::create(scratch.join("blocks.bin"))?);
    let mut sidecar_bytes = 0;
    let mut blocks_without_sidecars = 0;
    for height in first..=last {
        let block = journal.block_at(height).ok_or("missing committed block")?;
        block_file.write_all(block.block_hash.internal_bytes())?;
        block_file.write_all(&(block.offset - initial.offset).to_le_bytes())?;
        block_file.write_all(&block.event_count.to_le_bytes())?;
        let name = format!("{}.bin", block.block_hash.to_display_hex());
        let mut copied = false;
        for family in ["display-v1", "display-v2x"] {
            if source.join(family).is_symlink() {
                return Err("journal sidecar directory is a symlink".into());
            }
            let path = source.join(family).join(&name);
            let metadata = match fs::symlink_metadata(&path) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
            if !metadata.is_file() || metadata.file_type().is_symlink() {
                return Err("journal sidecar is not a regular file".into());
            }
            let directory = scratch.join(family);
            fs::create_dir_all(&directory)?;
            let target = directory.join(&name);
            sidecar_bytes += fs::copy(&path, &target)?;
            File::open(&target)?.sync_all()?;
            copied = true;
        }
        if !copied {
            blocks_without_sidecars += 1;
            if require_sidecars {
                return Err(format!("block {height} has no committed display sidecar").into());
            }
        }
    }
    block_file.flush()?;
    block_file.get_ref().sync_all()?;
    fs::write(
        scratch.join("meta.json"),
        serde_json::to_vec(&serde_json::json!({
            "version":journal.version(), "genesis_hash":journal.genesis_hash(), "start_height":first
        }))?,
    )?;
    let mut checkpoint = bytes.to_le_bytes().to_vec();
    checkpoint.extend_from_slice(&((last - first + 1) * 48).to_le_bytes());
    fs::write(scratch.join("checkpoint.bin"), checkpoint)?;
    let copied = EventStore::open_existing(&scratch)?;
    let mut events = 0;
    let mut candidates = std::collections::BTreeMap::<Vec<u8>, usize>::new();
    let mut per_zone = [0; 3];
    for height in first..=last {
        let expected = journal.events_at(height)?.ok_or("source block missing")?;
        let actual = copied.events_at(height)?.ok_or("copied block missing")?;
        if expected != actual
            || copied.block_at(height).map(|b| b.block_hash)
                != journal.block_at(height).map(|b| b.block_hash)
        {
            return Err(format!("export differs from source at height {height}").into());
        }
        events += actual.len() as u64;
        let zone = (((height - first) * 3 / (last - first + 1)) as usize).min(2);
        for (script, event) in &actual {
            if per_zone[zone] < 64
                && matches!(event, TransparentEvent::Receive(event) if event.value > 0)
                && matches!(script.as_slice().len(), 23 | 25)
                && !candidates.contains_key(script.as_slice())
            {
                candidates.insert(script.as_slice().to_vec(), zone);
                per_zone[zone] += 1;
            }
        }
    }
    // Resolve spent values only from receives inside this copy. Candidates
    // with earlier unknown prevouts are not usable interval-history oracles.
    let mut histories: std::collections::BTreeMap<Vec<u8>, Vec<TransparentEvent>> = candidates
        .keys()
        .cloned()
        .map(|script| (script, Vec::new()))
        .collect();
    if !histories.is_empty() {
        for height in first..=last {
            for (script, event) in copied.events_at(height)?.ok_or("copied block missing")? {
                if let Some(history) = histories.get_mut(script.as_slice()) {
                    if history.len() < 1025 {
                        history.push(event);
                    }
                }
            }
        }
    }
    let mut samples = Vec::new();
    let mut selected = [false; 3];
    for (script, history) in histories {
        let zone = candidates[&script];
        if selected[zone] {
            continue;
        }
        if history.len() > 1024 {
            continue;
        }
        let Some(expected) = interval_ledger(&script, &history, last) else {
            continue;
        };
        let script_hex = hex::encode(&script);
        samples.push(serde_json::json!({"zone":zone,"script_hex":script_hex,"birthday":first,"expected":expected}));
        selected[zone] = true;
    }
    fs::write(
        scratch.join("oracle.json"),
        serde_json::to_vec_pretty(&samples)?,
    )?;
    if !journal.still_committed(last, endpoint)? {
        return Err("journal changed while exporting; copy refused".into());
    }
    let report = Report {
        version: journal.version(),
        genesis_hash: journal.genesis_hash().into(),
        start_height: first,
        end_height: last,
        end_hash: endpoint.block_hash.to_display_hex(),
        blocks: last - first + 1,
        events,
        event_bytes: bytes,
        sidecar_bytes,
        blocks_without_sidecars,
    };
    fs::write(
        scratch.join("export.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    for entry in fs::read_dir(&scratch)? {
        let entry = entry?;
        if entry.file_name() == "meta.json" {
            continue;
        }
        fs::rename(entry.path(), destination.join(entry.file_name()))?;
    }
    // Metadata is the reader's entry point; install it last.
    File::open(scratch.join("meta.json"))?.sync_all()?;
    fs::rename(scratch.join("meta.json"), destination.join("meta.json"))?;
    File::open(destination)?.sync_all()?;
    fs::remove_dir(&scratch)?;
    Ok(report)
}

fn interval_ledger(
    script: &[u8],
    events: &[TransparentEvent],
    through: u64,
) -> Option<serde_json::Value> {
    use std::collections::BTreeMap;
    let script = hex::encode(script);
    let mut utxos = BTreeMap::<(String, u32), serde_json::Value>::new();
    let mut transactions = BTreeMap::<String, (u32, u64, u64)>::new();
    let mut spends = Vec::new();
    for event in events {
        match event {
            TransparentEvent::Receive(e) => {
                let txid = e.txid.to_display_hex();
                transactions
                    .entry(txid.clone())
                    .or_insert((e.height, 0, 0))
                    .1 += e.value;
                utxos.insert(
                    (txid.clone(), e.output_index),
                    serde_json::json!({"txid":txid,"index":e.output_index,
                    "value":e.value,"script":script,"height":e.height,"coinbase":e.coinbase}),
                );
            }
            TransparentEvent::Spend(e) => {
                let spent_txid = e.spent_txid.to_display_hex();
                let previous = utxos.remove(&(spent_txid.clone(), e.spent_output_index))?;
                let value = previous["value"].as_u64()?;
                let txid = e.spending_txid.to_display_hex();
                transactions
                    .entry(txid.clone())
                    .or_insert((e.height, 0, 0))
                    .2 += value;
                spends.push(serde_json::json!({"spent_txid":spent_txid,"spent_index":e.spent_output_index,
                    "spending_txid":txid,"input_index":e.input_index,"height":e.height,"value":value,"script":script}));
            }
        }
    }
    if transactions.is_empty() {
        return None;
    }
    let history: Vec<_> = transactions.into_iter().map(|(txid,(height,received,spent))|
        serde_json::json!({"txid":txid,"height":height,"received":received,"spent":spent})).collect();
    Some(
        serde_json::json!({"covered_through":through,"unresolved":0,"utxos":utxos.into_values().collect::<Vec<_>>(),"spends":spends,"history":history}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use transparent_shared::journal::display_journal;
    use zakura_pir_protocol::transparent::{
        events::{ReceiveEvent, TransparentEvent, Txid},
        filter::{BlockHash, ScriptBytes},
    };

    fn fixture(root: &Path) -> EventStore {
        let mut journal = EventStore::open(root, &"01".repeat(32), 100).unwrap();
        for height in 100..104 {
            let hash = BlockHash::from_internal_bytes([height as u8; 32]);
            let event = TransparentEvent::Receive(ReceiveEvent {
                metadata: Some(
                    zakura_pir_protocol::transparent::events::TransactionMetadata {
                        fee: zakura_pir_protocol::transparent::events::FeeState::Exact(1),
                        transparent_input_count: 1,
                        has_shielded_components: false,
                    },
                ),
                height: height as u32,
                txid: Txid([height as u8; 32]),
                transaction_index: 0,
                output_index: 0,
                value: 50,
                coinbase: false,
            });
            let events = vec![(ScriptBytes::new(vec![0x51]), event)];
            display_journal::write(root, hash, &[]).unwrap();
            journal.append_block(height, hash, &events).unwrap();
        }
        journal.commit().unwrap();
        journal
    }

    #[test]
    fn copies_only_requested_heights_without_changing_a_live_writer() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        let journal = fixture(&source);
        let names = [
            "meta.json",
            "checkpoint.bin",
            "events.bin",
            "blocks.bin",
            "writer.lock",
        ];
        let before: Vec<_> = names
            .iter()
            .map(|p| fs::read(source.join(p)).unwrap())
            .collect();
        let target = root.path().join("copy");
        let result = export(&source, &target, 101, 102, true).unwrap();
        assert_eq!(result.blocks, 2);
        assert_eq!(result.events, 2);
        let copy = EventStore::open_existing(&target).unwrap();
        assert_eq!(copy.start_height(), 101);
        assert_eq!(copy.covered_through(), Some(102));
        for h in 101..=102 {
            assert_eq!(copy.events_at(h).unwrap(), journal.events_at(h).unwrap());
        }
        for (name, expected) in names.iter().zip(before) {
            assert_eq!(fs::read(source.join(name)).unwrap(), expected);
        }
        assert!(!target.join("writer.lock").exists());
        assert!(export(&source, &target, 101, 102, true).is_err());
    }

    #[test]
    fn refuses_uncommitted_heights_and_missing_sidecars_without_exposing_a_journal() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        let _writer = fixture(&source);
        let target = root.path().join("copy");
        assert!(export(&source, &target, 101, 104, true).is_err());
        assert!(!target.exists());
        fs::remove_file(source.join("display-v2x").join(format!(
            "{}.bin",
            BlockHash::from_internal_bytes([102; 32]).to_display_hex()
        )))
        .unwrap();
        assert!(export(&source, &target, 101, 102, true).is_err());
        assert!(!target.join("meta.json").exists());
    }

    #[test]
    fn reports_source_sidecar_absence_instead_of_claiming_complete_display_inputs() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        let _writer = fixture(&source);
        fs::remove_dir_all(source.join("display-v2x")).unwrap();
        let target = root.path().join("copy");
        let result = export(&source, &target, 101, 102, false).unwrap();
        assert_eq!(result.blocks_without_sidecars, 2);
        assert_eq!(result.sidecar_bytes, 0);
        assert_eq!(result.events, 2);
    }

    #[test]
    fn preserves_events_carried_only_in_a_sidecar() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        let journal = fixture(&source);
        let hash = journal.block_at(101).unwrap().block_hash;
        let ordinary = journal.events_at(101).unwrap().unwrap();
        let mut event = ordinary[0].1.clone();
        let TransparentEvent::Receive(receive) = &mut event else {
            unreachable!()
        };
        receive.txid = Txid([250; 32]);
        let oversized = vec![(ScriptBytes::new(vec![0x51; 10_001]), event)];
        fs::remove_file(
            source
                .join("display-v2x")
                .join(format!("{}.bin", hash.to_display_hex())),
        )
        .unwrap();
        display_journal::write_with_events(&source, hash, &[], &oversized).unwrap();
        let target = root.path().join("copy");
        let report = export(&source, &target, 101, 102, true).unwrap();
        assert_eq!(report.events, 3);
        assert_eq!(
            EventStore::open_existing(target)
                .unwrap()
                .events_at(101)
                .unwrap(),
            journal.events_at(101).unwrap()
        );
    }

    #[test]
    fn interval_oracle_resolves_spends_and_refuses_unknown_earlier_outputs() {
        use zakura_pir_protocol::transparent::events::SpendEvent;
        let receive = TransparentEvent::Receive(ReceiveEvent {
            metadata: None,
            height: 100,
            txid: Txid([1; 32]),
            transaction_index: 0,
            output_index: 0,
            value: 50,
            coinbase: false,
        });
        let spend = TransparentEvent::Spend(SpendEvent {
            metadata: None,
            height: 101,
            spending_txid: Txid([2; 32]),
            transaction_index: 0,
            input_index: 0,
            spent_txid: Txid([1; 32]),
            spent_output_index: 0,
        });
        assert!(interval_ledger(&[0x51], std::slice::from_ref(&spend), 102).is_none());
        let ledger = interval_ledger(&[0x51], &[receive, spend], 102).unwrap();
        assert_eq!(ledger["utxos"], serde_json::json!([]));
        assert_eq!(ledger["spends"][0]["value"], 50);
        assert_eq!(ledger["history"].as_array().unwrap().len(), 2);
        assert_eq!(ledger["unresolved"], 0);
    }

    #[test]
    fn retains_the_legacy_journal_codec_without_conversion() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        drop(EventStore::open(&source, &"01".repeat(32), 100).unwrap());
        fs::write(
            source.join("meta.json"),
            serde_json::to_vec(&serde_json::json!({
                "version":2,"genesis_hash":"01".repeat(32),"start_height":100
            }))
            .unwrap(),
        )
        .unwrap();
        let writer = fixture(&source);
        let destination = root.path().join("copy");
        let report = export(&source, &destination, 101, 102, false).unwrap();
        assert_eq!(report.version, 2);
        let copy = EventStore::open_existing(destination).unwrap();
        assert_eq!(copy.version(), 2);
        assert_eq!(copy.events_at(101).unwrap(), writer.events_at(101).unwrap());
    }
}
