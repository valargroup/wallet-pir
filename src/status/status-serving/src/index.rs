//! Complete-block rolling index. Failed candidates cannot mutate active snapshots.
use enhance_pir::status::*;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

#[derive(Clone, PartialEq, Eq)]
pub struct Block {
    pub height: u32,
    pub hash: Hash,
    pub parent: Hash,
    pub txids: Vec<Hash>,
}

/// Source metadata never crosses the compact wire boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ForkRecord {
    pub txid: Hash,
    pub height: u32,
    pub block: Hash,
}
impl ForkRecord {
    pub fn validate(&self) -> Result<(), Error> {
        if self.height == 0 || self.block == [0; 32] {
            return Err(Error::Malformed);
        }
        Ok(())
    }
}
#[derive(Clone)]
struct SourceRecord {
    txid: Hash,
    tag: u8,
    height: u32,
    block: Hash,
}
impl SourceRecord {
    fn encode(&self) -> Result<[u8; SLOT_BYTES], Error> {
        Record {
            txid: self.txid,
            tag: self.tag,
            height: self.height,
        }
        .encode()
    }
}

pub struct Snapshot {
    pub network: Hash,
    pub salt: Hash,
    pub start: u32,
    pub height: u32,
    pub anchor: Hash,
    pub entries: usize,
    pub rows: Vec<u8>,
    pub digest: Hash,
    pub evicted_blocks: usize,
}

impl Snapshot {
    pub fn build(
        network: Hash,
        salt: Hash,
        blocks: &[Block],
        mempool: &[Hash],
        forks: &[ForkRecord],
    ) -> Result<Self, Error> {
        let last = blocks.last().ok_or(Error::CoverageIncomplete)?;
        for (i, b) in blocks.iter().enumerate() {
            if b.height == 0
                || b.hash == [0; 32]
                || (i > 0
                    && (blocks[i - 1].height.checked_add(1) != Some(b.height)
                        || blocks[i - 1].hash != b.parent))
            {
                return Err(Error::Malformed);
            }
        }
        let mut records = BTreeMap::<Hash, SourceRecord>::new();
        for r in forks {
            r.validate()?;
            if r.height < blocks[0].height || r.height > last.height {
                continue;
            }
            let old = records.get(&r.txid);
            if old.is_none_or(|o| (r.height, r.block) > (o.height, o.block)) {
                records.insert(
                    r.txid,
                    SourceRecord {
                        txid: r.txid,
                        tag: 3,
                        height: r.height,
                        block: r.block,
                    },
                );
            }
        }
        for txid in mempool {
            records.insert(
                *txid,
                SourceRecord {
                    txid: *txid,
                    tag: 1,
                    height: 0,
                    block: [0; 32],
                },
            );
        }
        for b in blocks {
            for txid in &b.txids {
                if records.get(txid).is_some_and(|r| r.tag == 2) {
                    return Err(Error::Malformed);
                }
                records.insert(
                    *txid,
                    SourceRecord {
                        txid: *txid,
                        tag: 2,
                        height: b.height,
                        block: b.hash,
                    },
                );
            }
        }
        let mut counts = vec![0usize; ROWS];
        for txid in records.keys() {
            counts[bucket(&network, &salt, txid)] += 1;
        }
        let mut first = 0;
        while records.len() > MAX_ENTRIES || counts.iter().any(|c| *c > SLOTS) {
            if first + 1 == blocks.len() {
                return Err(Error::Capacity);
            }
            let cutoff = blocks[first].height;
            records.retain(|txid, r| {
                if r.tag != 1 && r.height <= cutoff {
                    counts[bucket(&network, &salt, txid)] -= 1;
                    false
                } else {
                    true
                }
            });
            first += 1;
        }
        let mut rows = vec![0; ROWS * ROW_BYTES];
        let mut offsets = vec![0; ROWS];
        for r in records.values() {
            let b = bucket(&network, &salt, &r.txid);
            let off = b * ROW_BYTES + offsets[b] * SLOT_BYTES;
            rows[off..off + SLOT_BYTES].copy_from_slice(&r.encode()?);
            offsets[b] += 1;
        }
        let digest = Sha256::digest(&rows).into();
        Ok(Self {
            network,
            salt,
            start: blocks[first].height,
            height: last.height,
            anchor: last.hash,
            entries: records.len(),
            rows,
            digest,
            evicted_blocks: first,
        })
    }

    pub fn changed_rows(&self, previous: &Self) -> usize {
        self.rows
            .chunks_exact(ROW_BYTES)
            .zip(previous.rows.chunks_exact(ROW_BYTES))
            .filter(|(a, b)| a != b)
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn block(height: u32, txids: Vec<Hash>) -> Block {
        Block {
            height,
            hash: [height as u8; 32],
            parent: [height.saturating_sub(1) as u8; 32],
            txids,
        }
    }
    #[test]
    fn fork_selection_is_deterministic_and_hashes_stay_internal() {
        let blocks = [block(1, vec![]), block(2, vec![])];
        let forks = vec![
            ForkRecord {
                txid: [8; 32],
                height: 1,
                block: [9; 32],
            },
            ForkRecord {
                txid: [8; 32],
                height: 2,
                block: [4; 32],
            },
            ForkRecord {
                txid: [8; 32],
                height: 2,
                block: [5; 32],
            },
        ];
        let a = Snapshot::build([1; 32], [2; 32], &blocks, &[], &forks).unwrap();
        let mut reversed = forks.clone();
        reversed.reverse();
        let b = Snapshot::build([1; 32], [2; 32], &blocks, &[], &reversed).unwrap();
        assert_eq!(a.digest, b.digest);
        let offset = bucket(&a.network, &a.salt, &[8; 32]) * ROW_BYTES;
        let record = Record::decode(&a.rows[offset..offset + SLOT_BYTES])
            .unwrap()
            .unwrap();
        assert_eq!((record.tag, record.height), (3, 2));
        assert!(Snapshot::build(
            [1; 32],
            [2; 32],
            &blocks,
            &[],
            &[ForkRecord {
                txid: [8; 32],
                height: 2,
                block: [0; 32]
            }]
        )
        .is_err());
    }
    #[test]
    fn precedence_and_missing_coverage() {
        let snapshot = Snapshot::build(
            [1; 32],
            [2; 32],
            &[block(1, vec![[3; 32]])],
            &[[3; 32], [4; 32]],
            &[ForkRecord {
                txid: [4; 32],
                height: 1,
                block: [5; 32],
            }],
        )
        .unwrap();
        let m = Manifest {
            protocol: PROTOCOL.into(),
            network: [1; 32],
            salt: [2; 32],
            generation: 1,
            recovery_epoch: 0,
            coverage_start: 1,
            anchor_height: 1,
            anchor_hash: [1; 32],
            observed_ms: 100,
            entries: snapshot.entries,
            rows_digest: snapshot.digest,
            public_digest: [0; 32],
        };
        for (txid, expected) in [
            ([3; 32], Observation::Mined(1)),
            ([4; 32], Observation::Mempool),
        ] {
            let row = bucket(&m.network, &m.salt, &txid);
            assert_eq!(
                decode_row(
                    &m,
                    &txid,
                    None,
                    &snapshot.rows[row * ROW_BYTES..(row + 1) * ROW_BYTES]
                ),
                Ok(expected)
            );
        }
        let missing = [8; 32];
        let row = bucket(&m.network, &m.salt, &missing);
        let bytes = &snapshot.rows[row * ROW_BYTES..(row + 1) * ROW_BYTES];
        assert_eq!(
            decode_row(&m, &missing, None, bytes),
            Err(Error::CoverageIncomplete)
        );
        assert_eq!(
            decode_row(&m, &missing, Some(1), bytes),
            Ok(Observation::NotFound)
        );
    }
    #[test]
    fn overflow_evicts_whole_blocks_and_mandatory_overflow_fails() {
        let network = [1; 32];
        let salt = [2; 32];
        let mut ids = Vec::new();
        for i in 0u64.. {
            let txid = Sha256::digest(i.to_le_bytes()).into();
            if bucket(&network, &salt, &txid) == 0 {
                ids.push(txid);
            }
            if ids.len() == SLOTS + 1 {
                break;
            }
        }
        let blocks = [block(1, vec![ids[0]]), block(2, ids[1..].to_vec())];
        let snapshot = Snapshot::build(network, salt, &blocks, &[], &[]).unwrap();
        assert_eq!(
            (snapshot.start, snapshot.entries, snapshot.evicted_blocks),
            (2, 256, 1)
        );
        assert!(matches!(
            Snapshot::build(network, salt, &[block(1, vec![])], &ids, &[]),
            Err(Error::Capacity)
        ));
    }
    #[test]
    fn broken_chain_and_duplicate_canonical_txid_rejected() {
        assert!(matches!(
            Snapshot::build(
                [1; 32],
                [2; 32],
                &[block(1, vec![]), block(3, vec![])],
                &[],
                &[]
            ),
            Err(Error::Malformed)
        ));
        assert!(matches!(
            Snapshot::build(
                [1; 32],
                [2; 32],
                &[block(1, vec![[3; 32]]), block(2, vec![[3; 32]])],
                &[],
                &[]
            ),
            Err(Error::Malformed)
        ));
    }
}
