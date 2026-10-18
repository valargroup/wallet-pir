//! Shared worker admission limits used by execution and publication decisions.
pub const MIB: u64 = 1024 * 1024;
pub const RESIDENT_LIMIT: u64 = 7 * 1024 * MIB - 512 * MIB;
pub const OVERHEAD: u64 = 728 * MIB;
