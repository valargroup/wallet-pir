//! Enhance private evaluation messages and the publisher's durable invalidations.
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Evaluate {
    /// Full wallet binding; evaluation refuses requests without it.
    #[serde(default)]
    pub binding: Option<Vec<u8>>,
    pub session_id: String,
    pub generation: u64,
    pub shard_id: u64,
    pub epoch: String,
    pub coefficients: Vec<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Intermediate {
    #[serde(default)]
    pub binding: Option<Vec<u8>>,
    pub generation: u64,
    pub shard_id: u64,
    pub epoch: String,
    pub coefficients: Vec<u64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Revocation {
    #[serde(with = "crate::protocol::decimal_u64")]
    pub recovery_epoch: u64,
    pub sessions: BTreeSet<String>,
}
