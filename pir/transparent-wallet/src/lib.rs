//! A wallet's transparent recovery: birthday to balance.
//!
//! Given a birthday height, a script set and a published shard set, this
//! downloads every public filter in range, matches locally, privately
//! retrieves history only from the shards that matched, and replays the result
//! into a UTXO set and a history.
//!
//! It is deliberately split so the arithmetic can be checked without the
//! network: [`ledger`] takes events and produces state, and knows nothing about
//! where they came from. That is what lets the same replay be driven by private
//! retrieval and by an independent traversal of the chain, and their results
//! compared exactly.
//!
//! What this does not hide is which chain ranges the wallet queried. See
//! [`sync`] for the statement of that leak.

pub mod client;
pub mod ledger;
pub mod sync;
pub mod transport;

pub use ledger::{ConfirmedSpend, Ledger, LedgerError, TransactionSummary, UnresolvedSpend, Utxo};
pub use sync::{sync, ServiceGeometry, SyncError, SyncOutcome};
pub use transport::{ByteCharges, FilterSource, ShardTransport, TableCharges};
