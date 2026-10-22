//! Enhance private evaluation messages between ingress, routers and workers.
use serde::{Deserialize, Serialize};

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
