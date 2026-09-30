use crate::{
    dataset::{verify_blocks, Batch, Manifest},
    proto::CompactBlock,
};
use anyhow::{ensure, Result};
use std::collections::{BTreeMap, HashMap, HashSet};
use transparent_events::{ReceiveEvent, SpendEvent, TransparentEvent, Txid};
use transparent_wallet::store::{
    Anchor, ScriptEntry, SetIdentity, ShardCommit, StoredEvent, WalletStore,
};

/// One independently persisted block-scan wallet. No expected events enter this scanner.
pub struct Scanner {
    scripts: HashSet<Vec<u8>>,
    owned: HashMap<(Txid, u32), Vec<u8>>,
    pub next: u64,
    pub through: u64,
    dataset_id: String,
    genesis_hash: String,
    anchor_hash: String,
}
impl Scanner {
    pub fn new(
        store: &mut impl WalletStore,
        manifest: &Manifest,
        scripts: &[ScriptEntry],
        from: u64,
        through: u64,
    ) -> Result<Self> {
        manifest.validate(true)?;
        ensure!(
            !scripts.is_empty()
                && from >= manifest.start
                && from <= through
                && through == manifest.anchor_height,
            "invalid scanner range"
        );
        store.bind_set(&SetIdentity {
            shard_schema: "compact-blocks-v1".to_string(),
            network: "benchmark".into(),
            genesis_hash: manifest.genesis_hash.clone(),
            profile: format!("compact-blocks:{}", manifest.id()?),
            range_envelope_version: 1,
            start_height: manifest.start,
            seal: BTreeMap::new(),
        })?;
        store.add_scripts(scripts)?;
        let dataset_id = manifest.id()?;
        let mut next = through + 1;
        for script in scripts {
            let mut height = from;
            let coverage = store.coverage(&script.script)?;
            let mut ranges: Vec<_> = coverage
                .iter()
                .filter(|r| r.revision_digest == dataset_id)
                .collect();
            ranges.sort_by_key(|r| r.start_height);
            for r in ranges {
                if r.start_height <= height && r.end_height >= height {
                    height = r.end_height + 1;
                }
            }
            next = next.min(height);
        }
        let owned = store
            .events()?
            .into_iter()
            .filter_map(|e| match e.event {
                TransparentEvent::Receive(r) => Some(((r.txid, r.output_index), e.script)),
                _ => None,
            })
            .collect();
        Ok(Self {
            scripts: scripts.iter().map(|s| s.script.clone()).collect(),
            owned,
            next,
            through,
            dataset_id,
            genesis_hash: manifest.genesis_hash.clone(),
            anchor_hash: manifest.anchor_hash.clone(),
        })
    }
    pub fn apply(
        &mut self,
        store: &mut impl WalletStore,
        batch: &Batch,
        blocks: &[CompactBlock],
    ) -> Result<()> {
        verify_blocks(blocks, batch)?;
        if let Some(block) = blocks.first().filter(|b| b.height == 0) {
            ensure!(
                crate::dataset::display_hash(&block.hash)? == self.genesis_hash
                    && block.prev_hash == vec![0; 32],
                "genesis block mismatch"
            );
        }
        if batch.end < self.next {
            return Ok(());
        }
        ensure!(
            batch.start <= self.next && batch.end >= self.next,
            "missing scan range"
        );
        let start = self.next;
        let end = batch.end.min(self.through);
        let mut events = Vec::new();
        // Keep in-memory changes separate until the durable commit succeeds.
        let mut additions = HashMap::new();
        for block in blocks
            .iter()
            .filter(|b| b.height >= start && b.height <= end)
        {
            for tx in &block.vtx {
                let txid = Txid(tx.txid.as_slice().try_into()?);
                let transaction_index = u16::try_from(tx.index)?;
                for (index, input) in tx.vin.iter().enumerate() {
                    let previous = Txid(input.prevout_txid.as_slice().try_into()?);
                    if let Some(script) = additions
                        .get(&(previous, input.prevout_index))
                        .or_else(|| self.owned.get(&(previous, input.prevout_index)))
                    {
                        events.push(StoredEvent {
                            script: script.clone(),
                            shard_id: start,
                            revision_digest: self.dataset_id.clone(),
                            event: TransparentEvent::Spend(SpendEvent {
                                metadata: None,
                                height: u32::try_from(block.height)?,
                                spending_txid: txid,
                                transaction_index,
                                input_index: u32::try_from(index)?,
                                spent_txid: previous,
                                spent_output_index: input.prevout_index,
                            }),
                        });
                    }
                }
                for (index, output) in tx.vout.iter().enumerate() {
                    if self.scripts.contains(&output.script_pub_key) {
                        let output_index = u32::try_from(index)?;
                        additions.insert((txid, output_index), output.script_pub_key.clone());
                        events.push(StoredEvent {
                            script: output.script_pub_key.clone(),
                            shard_id: start,
                            revision_digest: self.dataset_id.clone(),
                            event: TransparentEvent::Receive(ReceiveEvent {
                                metadata: None,
                                height: u32::try_from(block.height)?,
                                txid,
                                transaction_index,
                                output_index,
                                value: output.value,
                                coinbase: tx.index == 0,
                            }),
                        });
                    }
                }
            }
        }
        store.commit_shard(ShardCommit {
            shard_id: start,
            revision_digest: self.dataset_id.clone(),
            sealed: true,
            start_height: start,
            end_height: end,
            terminal_block_hash: batch.hash.clone(),
            events,
            covered_scripts: self.scripts.iter().cloned().collect(),
            ..Default::default()
        })?;
        self.owned.extend(additions);
        self.next = end + 1;
        Ok(())
    }
    pub fn finish(&self, store: &mut impl WalletStore, anchor: &Anchor) -> Result<()> {
        ensure!(
            self.next == self.through + 1
                && anchor.height == self.through
                && anchor.hash == self.anchor_hash,
            "incomplete scan"
        );
        ensure!(store.ledger()?.unresolved().is_empty(), "unresolved spends");
        store.commit_anchor(anchor, anchor.height, anchor.height)?;
        Ok(())
    }
}
