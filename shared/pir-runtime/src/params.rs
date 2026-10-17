//! Profile parameters.

use inspiring::RlweParams;
use ipir_sp::{SimplePirProfile, YpirSchemeParams};

/// The RLWE ring and SimplePIR scan shape of the 16-bit-plaintext,
/// 48-bit-modulus profile for `rows` logical rows of `item_bits` each.
pub fn p16q48(rows: u64, item_bits: u64) -> Result<(RlweParams, YpirSchemeParams), String> {
    ipir_sp::params_for_simplepir_profile(rows, item_bits, SimplePirProfile::P16Q48)
        .map_err(|error| error.to_string())
}
