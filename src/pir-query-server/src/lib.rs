//! Unified query-server host. Product libraries retain serving authority.
pub mod config;
mod host;
pub use host::{run, verify};
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Hash exact local artifact bytes for operations that seed an immutable cache.
/// This supplies a content identity; it grants no publication authority.
pub fn hash_artifact(input: &std::path::Path) -> Result<pir_artifact::Checksum> {
    Ok(pir_artifact::hash_reader(std::fs::File::open(input)?)?.0)
}
