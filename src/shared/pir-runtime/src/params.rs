//! Profile parameters.

use ipir_sp::{ProductionSimplePirParams, SimplePirProfile};

/// Validated upstream 16-bit-plaintext, 48-bit-modulus profile for `rows`
/// logical rows of `item_bits` each. Products retain their shape selection.
pub fn p16q48(rows: u64, item_bits: u64) -> Result<ProductionSimplePirParams, String> {
    ProductionSimplePirParams::new(rows, item_bits, SimplePirProfile::P16Q48)
        .map_err(|error| error.to_string())
}
