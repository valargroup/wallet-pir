//! Receiver discovery records. A match is a candidate, never proof of ownership or spendability.
pub mod extract;
pub mod filter;
pub mod record;
pub mod snapshot;
#[cfg(feature = "store")]
pub mod store;
pub mod witness;

pub use record::{Payment, Receiver, Record, RECORD_BYTES};
/// A 32-byte hash in protocol byte order.
pub type Hash = [u8; 32];

/// Why directory data or coverage was rejected.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("malformed receiver directory data")]
    Malformed,
    #[error("receiver directory coverage is incomplete or uses a different anchor")]
    Coverage,
    #[error("receiver directory bucket capacity exceeded")]
    Capacity,
    #[error("missing continuation page")]
    MissingPage,
    #[cfg(feature = "store")]
    #[error(transparent)]
    Sql(#[from] rusqlite::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}
