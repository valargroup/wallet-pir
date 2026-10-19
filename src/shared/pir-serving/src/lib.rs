//! Serving helpers shared by the Enhance and Status serving libraries: the
//! selected profile's ciphertext modulus and native public hint contributions.
//! Products keep their own shapes and pass them in.

#[cfg(feature = "native-reinspiring")]
use ipir_sp::server::CrsBlock;

/// Ciphertext modulus of the selected profile: the native ring with
/// `native-reinspiring`, otherwise the P16Q48 profile for `rows` rows of
/// `item_bits` each.
pub fn modulus(rows: u64, item_bits: u64) -> u64 {
    #[cfg(feature = "native-reinspiring")]
    {
        let _ = (rows, item_bits);
        zakura_pir_server_core::native::Q
    }
    #[cfg(not(feature = "native-reinspiring"))]
    {
        pir_runtime::params::p16q48(rows, item_bits)
            .expect("pinned profile")
            .rlwe()
            .q
    }
}

/// Build public unit hint contributions using exact lifted native-ring products.
#[cfg(feature = "native-reinspiring")]
pub fn hint(
    server: &pir_runtime::evaluation::PreparedDatabase,
    setup: &[Vec<u64>],
) -> Result<Vec<CrsBlock>, String> {
    let rows = server.params().db_rows;
    let cols = server.params().db_cols;
    let db = server.db();
    Ok(
        zakura_pir_server_core::native::hint(setup, rows, cols, |col| {
            &db[col * rows..(col + 1) * rows]
        })?
        .into_iter()
        .map(|rows| CrsBlock { rows })
        .collect(),
    )
}
