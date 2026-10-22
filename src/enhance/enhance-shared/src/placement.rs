//! Pure worker placement policy and memory constants. Publication planning and
//! durable ledger state belong to the publisher.
use serde::{Deserialize, Serialize};

pub const MIB: u64 = 1024 * 1024;
pub const RESIDENT_LIMIT: u64 = 7 * 1024 * MIB - 512 * MIB;
pub const OVERHEAD: u64 = 728 * MIB;

/// Versioned deployment policy. Seven sealed shards require hardware qualification.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PlacementPolicy {
    pub sealed_shards: usize,
}
impl Default for PlacementPolicy {
    fn default() -> Self {
        Self { sealed_shards: 6 }
    }
}
impl PlacementPolicy {
    pub fn validate(self) -> Result<(), String> {
        if !matches!(self.sealed_shards, 6 | 7) {
            return Err("sealed shard limit must be six or seven".into());
        }
        Ok(())
    }
    pub fn limit(self, active: bool) -> usize {
        if active {
            5
        } else {
            self.sealed_shards
        }
    }
}
