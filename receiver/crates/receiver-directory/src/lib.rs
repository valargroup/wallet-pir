//! Receiver discovery records. A match is a candidate, never proof of ownership or spendability.
pub mod extract;
pub mod record;
pub mod snapshot;

pub use record::{Payment, Receiver, Record, RECORD_BYTES};
pub type Hash = [u8; 32];

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
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}
