//! Reserve a committed prefix and its generation before preparing any bytes.
use super::{EnhancePublisher, PublicationWork};
use crate::journal::RecordJournal;
use enhance_material::protocol::{self, Geometry, Manifest, ShardState};
use std::{collections::BTreeMap, sync::Arc};

impl EnhancePublisher {
    /// Reserve only publisher identities. No worker or router address, capacity
    /// acknowledgment, or activation time participates in this work token.
    pub(super) fn reserve_work(
        &self,
        journal: &RecordJournal,
    ) -> Result<Option<PublicationWork>, String> {
        let snapshot = journal.snapshot().map_err(|e| e.to_string())?;
        let anchor = snapshot.blocks().last().ok_or("empty Enhance journal")?;
        if self.state.lock().unwrap().poisoned {
            return Err("Enhance publisher persistence failed; reopen required".into());
        }
        let current = self.distribution.durable_current();
        if current.eligible
            && current.manifest.as_ref().is_some_and(|m| {
                m.anchor_height == anchor.height
                    && m.anchor_block_hash == anchor.hash
                    && m.coverage.records == snapshot.tree_size()
            })
        {
            return Ok(None);
        }
        let mut state = self.state.lock().unwrap();
        let mut next = state.durable.clone();
        let mut coverage = next
            .lifecycle
            .coverage(snapshot.tree_size(), Geometry::default())?;
        for shard in &mut coverage.shards {
            next.recovery
                .domain_epochs
                .entry(shard.id)
                .or_insert_with(|| "0".into());
            if shard.records == 32768 * enhance_material::RECORDS_PER_ROW as u64 {
                let end = (shard.id + 1) * 32768 * enhance_material::RECORDS_PER_ROW as u64;
                let crossing = snapshot
                    .blocks()
                    .iter()
                    .find(|b| b.first_position + b.action_count >= end)
                    .ok_or("missing completing block")?;
                if let Some((height, hash)) = next.recovery.sealed.get(&shard.id) {
                    if *height != crossing.height || *hash != crossing.hash {
                        return Err("sealed history changed without recovery fencing".into());
                    }
                    shard.state = ShardState::Sealed;
                } else if anchor.height.saturating_sub(crossing.height) >= 1000 {
                    next.recovery
                        .sealed
                        .insert(shard.id, (crossing.height, crossing.hash.clone()));
                    shard.state = ShardState::Sealed;
                }
            }
        }
        let generation = next.next_generation;
        next.next_generation = generation.checked_add(1).ok_or("generation exhausted")?;
        let epochs = coverage
            .shards
            .iter()
            .map(|s| (s.id, next.recovery.domain_epochs[&s.id].clone()))
            .collect();
        let manifest = Manifest {
            schema_version: protocol::SCHEMA_VERSION,
            protocol_revision: protocol::PROTOCOL_REVISION.into(),
            network: "main".into(),
            pool: "ironwood".into(),
            generation,
            placement_revision: 0,
            recovery_epoch: next.recovery.epoch,
            anchor_height: anchor.height,
            anchor_block_hash: anchor.hash.clone(),
            geometry: Geometry::default(),
            coverage,
            domain_recovery_epochs: epochs,
            sessions: Vec::new(),
            unit_identities: BTreeMap::new(),
        };
        self.save_state(&mut state, next)?;
        Ok(Some(PublicationWork {
            owner: self.incarnation.clone(),
            snapshot: Arc::new(snapshot),
            manifest,
            withdrawal_revision: current.withdrawal_revision,
        }))
    }
}
