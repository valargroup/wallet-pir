use ipir_sp::YpirSchemeParams;
use serde::{Deserialize, Serialize};

// Schema 8 widens the PIR row from nine to twenty-nine 737-byte records. The
// record encoding is unchanged; the row geometry, PIR parameters and every
// derived artifact are not. Schema-7 clients reject schema 8 on sight, which is
// the intended behaviour: interpreting a 21,373-byte row with 6,633-byte offsets
// would silently return the wrong record. The protocol revision stays at v2
// because the wire framing and endpoints are unchanged -- the same convention
// the schema 6 -> 7 record-width transition used.
pub const SCHEMA_VERSION: u16 = 8;
pub const PROTOCOL_REVISION: &str = "ironwood-enhance-pir-v2";
pub const NETWORK: &str = "main";
pub const POOL: &str = "ironwood";
pub const ACTIVATION_HEIGHT: u64 = 3_428_143;

pub const RECORDS_PER_ROW: usize = 29;
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
        assert_eq!(ROW_BYTES, 21_373);
        assert_eq!(record.ephemeral_key(), &[1; 32]);
        assert_eq!(record.enc_ciphertext(), &[2; 580]);
        assert_eq!(record.cv_net(), &[3; 32]);
        assert_eq!(record.out_ciphertext(), &[4; 80]);
        assert!(record.has_transparent_inputs());
        assert!(!record.has_transparent_outputs());
    }

    /// Slot arithmetic across a row boundary, and the row boundary the schema-8
    /// layout actually has. A nine-record build passes the first two of these
    /// and fails the rest, which is the point: the record-to-row mapping is the
    /// part of this change a client cannot detect by reading the manifest.
    #[test]
    fn positions_map_to_the_expected_row_and_slot() {
        let generation = |tree_size: u64, logical_rows: u64| EnhanceGeneration {
            schema_version: SCHEMA_VERSION,
            protocol_revision: PROTOCOL_REVISION.to_string(),
            network: NETWORK.to_string(),
            pool: POOL.to_string(),
            anchor_height: ACTIVATION_HEIGHT,
            anchor_block_hash: "00".repeat(32),
            ironwood_tree_size: tree_size,
            generation: 1,
            record_bytes: RECORD_BYTES as u32,
            records_per_row: RECORDS_PER_ROW as u32,
            row_bytes: ROW_BYTES as u32,
            shard_rows: SHARD_ROWS as u32,
            used_rows: used_rows_for(tree_size),
            logical_rows,
            parameter_id: "test".to_string(),
            setup_seed: ENHANCE_SETUP_SEED,
            public_params_epoch: String::new(),
            public_params_sha256: String::new(),
            shards: vec![],
        };

        let wide = generation(1_000_000, 65_536);
        // First slot, last slot of row zero, and the crossing into row one.
        assert_eq!(wide.row_for_position(0), Some((0, 0)));
        assert_eq!(wide.row_for_position(28), Some((0, 28)));
        assert_eq!(wide.row_for_position(29), Some((1, 0)));
        assert_eq!(wide.row_for_position(57), Some((1, 28)));
        assert_eq!(wide.row_for_position(58), Some((2, 0)));
        // First and last position of physical shard one.
        assert_eq!(
            wide.row_for_position(SHARD_POSITIONS as u64),
            Some((SHARD_ROWS, 0))
        );
        assert_eq!(
            wide.row_for_position(2 * SHARD_POSITIONS as u64 - 1),
            Some((2 * SHARD_ROWS - 1, RECORDS_PER_ROW - 1))
        );

        // A position past the published tree size is outside coverage even
        // though its row exists, and so is one past the logical database.
        let partial = generation(30, 8_192);
        assert_eq!(partial.row_for_position(29), Some((1, 0)));
        assert_eq!(partial.row_for_position(30), None);
        let narrow = generation(u64::MAX, 8_192);
        assert_eq!(
            narrow.row_for_position(8_192 * RECORDS_PER_ROW as u64 - 1),
            Some((8_191, RECORDS_PER_ROW - 1))
        );
        assert_eq!(
            narrow.row_for_position(8_192 * RECORDS_PER_ROW as u64),
            None
        );
    }

    /// Where the upload doubles. `logical_rows_for` rounds to a power of two, so
    /// crossing 29 x 16,384 positions moves the query from 16,384 rows to
    /// 32,768 -- about 88 KiB more upload per query, at no other cost. Pin the
    /// exact position so the jump is a reviewed number rather than a surprise.
    #[test]
    fn logical_rows_double_at_the_published_boundary() {
        const LAST_AT_16K: u64 = 29 * 16_384;
        assert_eq!(LAST_AT_16K, 475_136);
        assert_eq!(logical_rows_for(used_rows_for(LAST_AT_16K)), 16_384);
        assert_eq!(logical_rows_for(used_rows_for(LAST_AT_16K + 1)), 32_768);
        // And the floor: a nearly empty database still publishes 8,192 rows.
        assert_eq!(logical_rows_for(used_rows_for(1)), 8_192);
    }

    #[test]
    fn geometry_is_fixed_and_aligned() {
        assert_eq!(SHARD_POSITIONS, 237_568);
        assert_eq!(SHARD_ROWS % 2_048, 0);
        assert_eq!(used_rows_for(29), 1);
        assert_eq!(used_rows_for(30), 2);
        assert_eq!(logical_rows_for(0), 8_192);
    }
}
