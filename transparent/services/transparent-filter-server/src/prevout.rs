//! Resolving the locking script of a spent output.
//!
//! Most spends are recent, so a bounded cache of the outputs created by the
//! blocks just ingested answers the large majority of lookups without any RPC.
//! The rest go to `getrawtransaction`, which an archive node answers from its
//! transaction index.
//!
//! A lookup that cannot be answered is an error. It is never an empty script:
//! a filter missing a real spend would tell a wallet it had no activity.
//!
//! The cache, the batched pre-pass and the straggler lookups are
//! `zakura_server_chain::transparent`'s; this module keeps the budgets the
//! service runs them with.

pub use zakura_server_chain::transparent::OutputCache;

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
pub const PREVOUT_BATCH: usize = zakura_server_chain::transparent::PREVOUT_BATCH;

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
pub const DEFAULT_CACHE_OUTPUTS: usize = zakura_server_chain::transparent::DEFAULT_CACHE_OUTPUTS;

const _: () = assert!(PREVOUT_BATCH == 256 && DEFAULT_CACHE_OUTPUTS == 8_000_000);
