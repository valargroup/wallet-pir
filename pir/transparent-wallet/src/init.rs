//! The service's init document, parsed into what the wallet checks.
//!
//! Not behind the HTTP feature: a wallet with its own transport still has to
//! read the same document, and a second parser would be a second place for the
//! two to disagree about a field.

use crate::sync::{GeometryParams, ServiceGeometry};
use crate::transport::BoxError;

/// Parses the service's init document into what the wallet checks it against.
///
/// The service publishes one entry per geometry it holds. The wallet re-derives
/// each scheme from the dimensions published beside it and refuses any that
/// does not reproduce.
pub fn parse_init(raw: &[u8]) -> Result<ServiceGeometry, BoxError> {
    let init: serde_json::Value = serde_json::from_slice(raw)?;
    let schema = init["schema"]
        .as_str()
        .ok_or("init carries no schema")?
        .to_string();
    let mut geometries = Vec::new();
    for entry in init["geometries"]
        .as_array()
        .ok_or("init publishes no geometries")?
    {
        let u64_at = |key: &str| {
            entry[key]
                .as_u64()
                .ok_or_else(|| format!("init geometry lacks {key}"))
        };
        geometries.push(GeometryParams {
            name: entry["name"]
                .as_str()
                .ok_or("init geometry lacks a name")?
                .to_string(),
            directory_rows: u64_at("directory_rows")?,
            directory_row_bytes: u64_at("directory_row_bytes")? as u32,
            directory_scheme: serde_json::from_value(entry["directory_scheme"].clone())?,
            directory_setup_seed: u64_at("directory_setup_seed")?,
            page_rows: u64_at("page_rows")?,
            page_row_bytes: u64_at("page_row_bytes")? as u32,
            pages_scheme: serde_json::from_value(entry["pages_scheme"].clone())?,
            pages_setup_seed: u64_at("pages_setup_seed")?,
        });
    }
    Ok(ServiceGeometry { schema, geometries })
}
