//! The `zcash-transparent-basic-v1` application profile.
//!
//! This is a proposed application profile that reuses BIP 158's basic encoding
//! and analogous transparent-script inclusion rules. It is not an assertion of
//! an existing Zcash network standard, and it implies no support for Bitcoin
//! peer-service messages or signalling.

/// Profile identifier carried in application metadata and cache keys.
pub const PROFILE: &str = "zcash-transparent-basic-v1";

/// Profile identifier for filters covering a shard rather than a single block.
///
/// A separate profile, not a version bump of [`PROFILE`]: the two use different
/// keyings, cover different things, and a wallet must never accept one where it
/// asked for the other. The string is part of the SipHash key derivation (see
/// `ShardKey`), so the separation is enforced by the bytes and not only by
/// metadata a server could misreport.
///
/// `P` and `M` are unchanged. In BIP 158 an element is mapped into `[0, N*M)`,
/// so the per-tested-element false-positive rate is `1/M` regardless of how
/// many elements the filter holds: a shard filter over twelve thousand scripts
/// is exactly as precise per query as a block filter over twelve. What grows
/// with the element count is the filter's *size*, which is what deduplicating
/// scripts across a shard's blocks is meant to pay for.
pub const RANGE_PROFILE: &str = "zcash-transparent-range-v1";

/// Golomb-Rice parameter. Fixed by the profile; not configurable.
pub const P: u8 = 19;

/// False-positive range parameter. Fixed by the profile; not configurable.
pub const M: u64 = 784_931;

/// Network identifier carried alongside the genesis hash.
pub const NETWORK: &str = "main";

/// Zcash mainnet genesis block hash in RPC display order.
///
/// Chain identity is the genesis hash, not the network name: the name alone
/// would let a filter built on one chain be accepted on another that happens to
/// share it.
pub const MAINNET_GENESIS_DISPLAY: &str =
    "00040fe8ec8471911baa1db1266ea15dd06b4a8a5c453883c000b031973dce08";

/// First height this profile publishes filters for.
///
/// Coverage begins at Ironwood activation rather than genesis. A wallet whose
/// birthday precedes this height is not served by this deployment; that is a
/// deployment limitation, not a property of the encoding.
pub const START_HEIGHT: u64 = 3_428_143;
