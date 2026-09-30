//! Resolving the locking script of a spent output.
//!
//! Most spends are recent, so a bounded cache of the outputs created by the
//! blocks just ingested answers the large majority of lookups without any RPC.
//! The rest go to `getrawtransaction`, which an archive node answers from its
//! transaction index.
//!
//! A lookup that cannot be answered is an error. It is never an empty script:
//! a filter missing a real spend would tell a wallet it had no activity.

use crate::extract::{outpoint_label, PreviousOutputs};
use crate::zakura::ZakuraClient;
use std::collections::{HashMap, VecDeque};
use zakura_chain::transaction::Transaction;
use zakura_chain::transparent::{Input, OutPoint, Output};

/// Previous transactions requested per JSON-RPC batch.
///
/// The backfill's critical path is round trips, not node work. Measured over
/// the genesis range at height 339,000, a block needed 907 previous-output
/// lookups against 250 cache hits, and at a batch of 16 that was 57 sequential
/// round trips for one block of a 1.34-second budget. The node does the same
/// work either way; what a larger batch removes is the waiting.
///
/// The earlier value matched the batch size a Python collector used against
/// this node, which aligned the two tools without either of them measuring it.
pub const PREVOUT_BATCH: usize = 256;

/// Number of recently seen *outputs* whose scripts are retained.
///
/// Bounded by outputs rather than by transactions, because transactions are a
/// poor proxy for memory: an early-chain mining-pool coinbase can carry
/// thousands of outputs, so a transaction-bounded cache grew without limit over
/// that range and was killed by the OOM killer partway through a genesis
/// backfill. Outputs are what the map actually stores.
///
/// An entry is an outpoint, a script and their map overhead — near 110 bytes
/// once the allocator and the load factor are counted — so this is around a
/// gigabyte. It bounds memory rather than guaranteeing a hit rate: a miss is
/// correct, just slower.
///
/// Sized from the miss rate rather than from a round number. Over the genesis
/// range the cache held two million outputs and answered 22% of lookups: at
/// roughly 230 outputs a block that is a window of about 8,700 blocks, and a
/// spend reaching further back than that paid an RPC round trip. Eight million
/// widens the window to about 35,000 blocks. A host with less memory than that
/// deserves should pass `--cache-outputs`; the cost of guessing low is speed,
/// and the cost of guessing high is the OOM killer, which is why this is a
/// default and not a floor.
pub const DEFAULT_CACHE_OUTPUTS: usize = 8_000_000;

/// Outputs of transactions seen recently, with insertion-order eviction.
///
/// Eviction is still whole transactions — a partially evicted transaction would
/// leave outputs that can never be found again — but the budget it enforces is
/// the output count.
pub struct OutputCache {
    scripts: HashMap<OutPoint, Output>,
    /// Transaction ids in insertion order, for eviction.
    order: VecDeque<(zakura_chain::transaction::Hash, u32)>,
    /// Outputs currently held, kept alongside so eviction needs no walk.
    outputs: usize,
    capacity: usize,
}

impl OutputCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            scripts: HashMap::new(),
            order: VecDeque::new(),
            outputs: 0,
            capacity: capacity.max(1),
        }
    }

    /// Records every output of a transaction.
    pub fn insert_transaction(&mut self, transaction: &Transaction) {
        let txid = transaction.hash();
        let outputs = transaction.outputs();
        for (index, output) in outputs.iter().enumerate() {
            self.scripts.insert(
                OutPoint {
                    hash: txid,
                    index: index as u32,
                },
                output.clone(),
            );
        }
        if !outputs.is_empty() {
            self.order.push_back((txid, outputs.len() as u32));
            self.outputs += outputs.len();
            // Evict whole transactions until the output budget holds. A single
            // transaction larger than the budget is kept rather than evicted
            // immediately, since evicting it would leave the block being
            // extracted unable to resolve its own outputs.
            while self.outputs > self.capacity && self.order.len() > 1 {
                if let Some((evicted, count)) = self.order.pop_front() {
                    for index in 0..count {
                        self.scripts.remove(&OutPoint {
                            hash: evicted,
                            index,
                        });
                    }
                    self.outputs = self.outputs.saturating_sub(count as usize);
                }
            }
        }
    }

    pub fn get(&self, outpoint: &OutPoint) -> Option<&Output> {
        self.scripts.get(outpoint)
    }

    pub fn len(&self) -> usize {
        self.scripts.len()
    }
    pub fn is_empty(&self) -> bool {
        self.scripts.is_empty()
    }
}

/// Resolver backed by the cache first and Zakura second.
///
/// Holds a Tokio runtime handle because `extract_elements` is synchronous: the
/// element set for a block is built in one pass, and threading async through it
/// would buy nothing when the node is on loopback.
pub struct ZakuraPreviousOutputs<'a> {
    client: &'a ZakuraClient,
    runtime: tokio::runtime::Handle,
    cache: &'a mut OutputCache,
    /// Lookups that had to go to the node.
    pub rpc_lookups: u64,
    /// Lookups answered from the cache.
    pub cache_hits: u64,
}

impl<'a> ZakuraPreviousOutputs<'a> {
    pub fn new(
        client: &'a ZakuraClient,
        runtime: tokio::runtime::Handle,
        cache: &'a mut OutputCache,
    ) -> Self {
        Self {
            client,
            runtime,
            cache,
            rpc_lookups: 0,
            cache_hits: 0,
        }
    }
}

impl PreviousOutputs for ZakuraPreviousOutputs<'_> {
    fn previous_output(
        &mut self,
        outpoint: &OutPoint,
    ) -> Result<Option<Output>, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(script) = self.cache.get(outpoint) {
            self.cache_hits += 1;
            return Ok(Some(script.clone()));
        }
        self.rpc_lookups += 1;
        let mut txid = outpoint.hash.0;
        txid.reverse();
        let txid = hex::encode(txid);
        let transaction = self
            .runtime
            .block_on(self.client.transaction(&txid))
            .map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>)?;
        let Some(transaction) = transaction else {
            return Ok(None);
        };
        if transaction.hash() != outpoint.hash {
            return Err("previous transaction identity mismatch".into());
        }
        // Cache the whole transaction: a block that spends one of its outputs
        // often spends several.
        self.cache.insert_transaction(&transaction);
        let script = transaction.outputs().get(outpoint.index as usize).cloned();
        if script.is_none() {
            tracing::warn!(
                outpoint = %outpoint_label(outpoint),
                outputs = transaction.outputs().len(),
                "previous transaction has no output at this index"
            );
        }
        Ok(script)
    }
}

/// Resolves, in batches, every previous output this block will need.
///
/// A pre-pass rather than lazy lookups: resolving during extraction costs one
/// round trip per missing transaction, which dominates ingest as soon as the
/// node is not on loopback. After this returns, extraction finds everything in
/// the cache and issues no further requests.
///
/// Outputs created earlier in this same block are already in the cache and are
/// not requested. A transaction the node cannot supply is left absent, so
/// extraction still fails closed rather than treating it as an empty script.
pub async fn prefetch_previous_outputs(
    zakura: &ZakuraClient,
    cache: &mut OutputCache,
    transactions: &[std::sync::Arc<Transaction>],
) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    let mut wanted: Vec<String> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for transaction in transactions {
        for input in transaction.inputs() {
            let Input::PrevOut { outpoint, .. } = input else {
                continue;
            };
            if cache.get(outpoint).is_some() {
                continue;
            }
            if !seen.insert(outpoint.hash) {
                continue;
            }
            let mut txid = outpoint.hash.0;
            txid.reverse();
            wanted.push(hex::encode(txid));
        }
    }
    let mut fetched = 0u64;
    for chunk in wanted.chunks(PREVOUT_BATCH) {
        for transaction in zakura.transactions(chunk).await?.into_iter().flatten() {
            cache.insert_transaction(&transaction);
            fetched += 1;
        }
    }
    Ok(fetched)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn outpoint(seed: u8, index: u32) -> OutPoint {
        OutPoint {
            hash: zakura_chain::transaction::Hash([seed; 32]),
            index,
        }
    }

    /// A transaction with `outputs` outputs, standing in for the early-chain
    /// mining-pool coinbases that made a transaction-bounded cache unbounded.
    fn wide_transaction(tag: u8, outputs: usize) -> std::sync::Arc<Transaction> {
        std::sync::Arc::new(Transaction::V1 {
            inputs: Vec::new(),
            outputs: (0..outputs)
                .map(|index| zakura_chain::transparent::Output {
                    value: zakura_chain::amount::Amount::try_from(1).unwrap(),
                    lock_script: zakura_chain::transparent::Script::new(&[
                        0x76,
                        0xa9,
                        0x14,
                        tag,
                        index as u8,
                    ]),
                })
                .collect(),
            lock_time: zakura_chain::transaction::LockTime::unlocked(),
        })
    }

    fn outpoint_of(transaction: &Transaction, index: u32) -> OutPoint {
        OutPoint {
            hash: transaction.hash(),
            index,
        }
    }

    /// The budget is outputs, not transactions. A transaction-bounded cache
    /// grew without limit over early-chain blocks, whose mining-pool coinbases
    /// carry thousands of outputs each, and a genesis backfill was killed by
    /// the OOM killer partway through because of it.
    #[test]
    fn one_huge_transaction_counts_for_all_its_outputs() {
        let mut cache = OutputCache::new(100);
        let wide = wide_transaction(0, 250);
        cache.insert_transaction(&wide);
        // A transaction larger than the whole budget is kept: evicting it would
        // leave the block being extracted unable to resolve its own outputs.
        assert!(cache.get(&outpoint_of(&wide, 0)).is_some());

        // The next insert evicts it, because the budget is now exceeded by a
        // transaction that is not the only one held.
        let next = wide_transaction(1, 10);
        cache.insert_transaction(&next);
        assert!(
            cache.get(&outpoint_of(&wide, 0)).is_none(),
            "the oversized transaction should be evicted once another arrives"
        );
        assert!(cache.get(&outpoint_of(&next, 0)).is_some());
    }

    /// Many small transactions evict by output count rather than by count of
    /// transactions, so a fixed budget holds a predictable amount of memory
    /// whatever the shape of the blocks.
    #[test]
    fn eviction_is_by_output_count_not_transaction_count() {
        let mut cache = OutputCache::new(20);
        let first = wide_transaction(0, 8);
        cache.insert_transaction(&first);
        for tag in 1..4u8 {
            cache.insert_transaction(&wide_transaction(tag, 8));
        }
        // Four transactions of eight outputs is 32, past a 20-output budget, so
        // the earliest are gone even though only four transactions were seen.
        assert!(cache.get(&outpoint_of(&first, 0)).is_none());
        assert!(cache.len() <= 24, "held {} outputs", cache.len());
    }

    #[test]
    fn an_empty_cache_answers_nothing() {
        let cache = OutputCache::new(4);
        assert!(cache.is_empty());
        assert!(cache.get(&outpoint(1, 0)).is_none());
    }

    #[test]
    fn eviction_bounds_the_cache_without_losing_recent_entries() {
        use zakura_chain::amount::Amount;
        use zakura_chain::transparent::{Output, Script};

        let make = |tag: u8| {
            std::sync::Arc::new(Transaction::V1 {
                inputs: vec![],
                outputs: vec![Output {
                    value: Amount::try_from(i64::from(tag) + 1).unwrap(),
                    lock_script: Script::new(&[0x76, 0xa9, tag]),
                }],
                lock_time: zakura_chain::transaction::LockTime::unlocked(),
            })
        };

        let mut cache = OutputCache::new(2);
        let first = make(1);
        let second = make(2);
        let third = make(3);
        cache.insert_transaction(&first);
        cache.insert_transaction(&second);
        cache.insert_transaction(&third);

        // Capacity is in transactions, and the oldest was evicted.
        assert_eq!(cache.len(), 2);
        let gone = OutPoint {
            hash: first.hash(),
            index: 0,
        };
        let kept = OutPoint {
            hash: third.hash(),
            index: 0,
        };
        assert!(cache.get(&gone).is_none(), "oldest entry should be evicted");
        assert_eq!(cache.get(&kept), Some(&vec![0x76, 0xa9, 3]));
    }
}
