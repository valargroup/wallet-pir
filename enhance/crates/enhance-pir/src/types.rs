use ipir_sp::YpirSchemeParams;
use serde::{Deserialize, Serialize};

pub const SCHEMA_VERSION: u16 = 7;
pub const PROTOCOL_REVISION: &str = "ironwood-enhance-pir-v2";
pub const NETWORK: &str = "main";
pub const POOL: &str = "ironwood";
pub const ACTIVATION_HEIGHT: u64 = 3_428_143;

pub const RECORDS_PER_ROW: usize = 9;
pub const ROW_BYTES: usize = RECORD_BYTES * RECORDS_PER_ROW;
pub const SHARD_ROWS: usize = 8_192;
pub const SHARD_POSITIONS: usize = SHARD_ROWS * RECORDS_PER_ROW;
/// Shards assigned to one logical worker group. Every replica in the group
/// holds the complete assignment; replicas are alternatives, not additive
/// contributors to a query.
pub const SHARDS_PER_GROUP: u64 = 16;
/// Backwards-compatible alias for callers compiled against the former
/// single-owner placement terminology.
pub const SHARDS_PER_WORKER: u64 = SHARDS_PER_GROUP;
pub const ITEM_SIZE_BITS: u64 = (ROW_BYTES * 8) as u64;

/// Pinned deterministic setup seed for the Enhance PIR protocol.
pub const ENHANCE_SETUP_SEED: u64 = 0xa4d6_9bc2_317e_085f;

pub fn setup_seed_bytes() -> [u8; 32] {
    let mut bytes = [0; 32];
    bytes[..8].copy_from_slice(&ENHANCE_SETUP_SEED.to_le_bytes());
    bytes
}

pub use crate::record::*;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardDescriptor {
    pub shard_id: u64,
    pub global_row_start: u64,
    pub populated_positions: u64,
    pub rows_sha256: String,
    pub sealed: bool,
    pub worker: String,
}

/// The complete public description of one answerable Enhance PIR generation.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct EnhanceGeneration {
    pub schema_version: u16,
    pub protocol_revision: String,
    pub network: String,
    pub pool: String,
    pub anchor_height: u64,
    pub anchor_block_hash: String,
    pub ironwood_tree_size: u64,
    pub generation: u64,
    pub record_bytes: u32,
    pub records_per_row: u32,
    pub row_bytes: u32,
    pub shard_rows: u32,
    pub used_rows: u64,
    pub logical_rows: u64,
    pub parameter_id: String,
    pub setup_seed: u64,
    pub public_params_epoch: String,
    pub public_params_sha256: String,
    pub shards: Vec<ShardDescriptor>,
}

/// An atomic description of one answerable Enhance PIR generation.
///
/// The published parameters are base64-encoded so all material needed to
/// construct a query session is captured by one JSON response.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct EnhanceSession {
    pub generation: EnhanceGeneration,
    pub params: YpirSchemeParams,
    pub public_params_base64: String,
}

impl EnhanceGeneration {
    pub fn row_for_position(&self, position: u64) -> Option<(usize, usize)> {
        if position >= self.ironwood_tree_size {
            return None;
        }
        let row = position / RECORDS_PER_ROW as u64;
        (row < self.logical_rows).then_some((row as usize, position as usize % RECORDS_PER_ROW))
    }
}

pub const fn used_rows_for(positions: u64) -> u64 {
    positions.div_ceil(RECORDS_PER_ROW as u64)
}

pub fn logical_rows_for(used_rows: u64) -> u64 {
    used_rows.max(SHARD_ROWS as u64).next_power_of_two()
}

pub fn group_index_for_shard(shard_id: u64, group_count: usize) -> Option<usize> {
    if group_count == 0 {
        return None;
    }
    let index = usize::try_from(shard_id / SHARDS_PER_GROUP).ok()?;
    (index < group_count).then_some(index)
}

/// Backwards-compatible alias for the former single-owner placement helper.
pub fn worker_index_for_shard(shard_id: u64, worker_count: usize) -> Option<usize> {
    group_index_for_shard(shard_id, worker_count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_layout_contains_only_enhancement_fields() {
        let record = EnhanceRecord::from_parts(EnhanceRecordParts {
            ephemeral_key: [1; 32],
            enc_ciphertext: [2; 580],
            cv_net: [3; 32],
            out_ciphertext: [4; 80],
            has_transparent_inputs: true,
            has_transparent_outputs: false,
            metadata: crate::EnhanceTransactionMetadata::new(0, Some(0)).unwrap(),
        });
        assert_eq!(RECORD_BYTES, 737);
        assert_eq!(ROW_BYTES, 6_633);
        assert_eq!(record.ephemeral_key(), &[1; 32]);
        assert_eq!(record.enc_ciphertext(), &[2; 580]);
        assert_eq!(record.cv_net(), &[3; 32]);
        assert_eq!(record.out_ciphertext(), &[4; 80]);
        assert!(record.has_transparent_inputs());
        assert!(!record.has_transparent_outputs());
    }

    #[test]
    fn geometry_is_fixed_and_aligned() {
        assert_eq!(SHARD_POSITIONS, 73_728);
        assert_eq!(SHARD_ROWS % 2_048, 0);
        assert_eq!(used_rows_for(9), 1);
        assert_eq!(used_rows_for(10), 2);
        assert_eq!(logical_rows_for(0), 8_192);
    }
}
