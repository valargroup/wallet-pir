//! Storage layout for the Enhance database.
pub use enhance_pir::types::{
    EnhanceRecord, ACTIVATION_HEIGHT, ENHANCE_SETUP_SEED, RECORDS_PER_ROW, RECORD_BYTES, ROW_BYTES,
    SHARD_ROWS,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DatabaseId {
    Enhance,
}

impl DatabaseId {
    pub const fn as_str(self) -> &'static str {
        "enhance"
    }
    pub const fn layout(self) -> DatabaseLayout {
        ENHANCE_LAYOUT
    }
    pub const fn setup_seed(self) -> u64 {
        ENHANCE_SETUP_SEED
    }
    pub fn setup_seed_bytes(self) -> [u8; 32] {
        let mut bytes = [0; 32];
        bytes[..8].copy_from_slice(&self.setup_seed().to_le_bytes());
        bytes
    }
}

impl std::fmt::Display for DatabaseId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::str::FromStr for DatabaseId {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "enhance" => Ok(Self::Enhance),
            _ => Err(format!("unknown PIR database: {value:?}")),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DatabaseLayout {
    pub record_bytes: usize,
    pub records_per_row: usize,
    pub shard_rows: usize,
    pub pir_profile: ipir_sp::SimplePirProfile,
}

impl DatabaseLayout {
    pub const fn row_bytes(self) -> usize {
        self.record_bytes * self.records_per_row
    }

    pub const fn shard_positions(self) -> usize {
        self.shard_rows * self.records_per_row
    }

    pub const fn item_size_bits(self) -> u64 {
        (self.row_bytes() * 8) as u64
    }

    pub const fn shard_bytes(self) -> usize {
        self.shard_rows * self.row_bytes()
    }

    pub const fn used_rows_for(self, positions: u64) -> u64 {
        positions.div_ceil(self.records_per_row as u64)
    }

    pub fn logical_rows_for(self, used_rows: u64) -> u64 {
        used_rows.max(self.shard_rows as u64).next_power_of_two()
    }
}

pub const ENHANCE_LAYOUT: DatabaseLayout = DatabaseLayout {
    record_bytes: RECORD_BYTES,
    records_per_row: RECORDS_PER_ROW,
    shard_rows: SHARD_ROWS,
    pir_profile: ipir_sp::SimplePirProfile::P16Q48,
};
