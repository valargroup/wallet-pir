//! A completed layout identifies storage, never permission to serve its bytes.
use crate::{storage::records::write_durable, BoxError};
use serde::{Deserialize, Serialize};
use std::path::Path;

pub const FILE: &str = "storage-layout.json";
pub const SCHEMA: &str = "transparent-object-storage/1";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Layout {
    schema: String,
    product: String,
}

/// Finish bootstrap or offline preparation only after material is complete.
pub fn complete(root: &Path, product: &str) -> Result<(), BoxError> {
    write_durable(
        &root.join(FILE),
        &serde_json::to_vec(&Layout {
            schema: SCHEMA.into(),
            product: product.into(),
        })?,
    )
}

/// Existing state must be prepared explicitly. Refusal changes no source files.
pub fn require(root: &Path, product: &str) -> Result<(), BoxError> {
    let raw = std::fs::read(root.join(FILE)).map_err(|error| {
        format!(
            "{} has no completed object-storage layout; use a current publication root: {error}",
            root.display()
        )
    })?;
    let layout: Layout = serde_json::from_slice(&raw)?;
    if layout.schema != SCHEMA || layout.product != product {
        return Err("prepared storage layout belongs to another product or version".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn existing_storage_needs_its_product_completion_record() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("active.json"), b"legacy pointer").unwrap();
        assert!(require(root.path(), "transparent_history").is_err());
        complete(root.path(), "transparent_history").unwrap();
        require(root.path(), "transparent_history").unwrap();
        assert!(require(root.path(), "transaction_details").is_err());
        assert_eq!(
            std::fs::read(root.path().join("active.json")).unwrap(),
            b"legacy pointer"
        );
    }
}
