//! Identity of this compiled reader, separately from any journal's ingest.
use crate::AnyError;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

pub fn compiled() -> Value {
    serde_json::from_str(include_str!(concat!(
        env!("OUT_DIR"),
        "/source_identity.json"
    )))
    .expect("build script embeds valid source identity JSON")
}

pub fn executing() -> Result<Value, AnyError> {
    let mut identity = compiled();
    identity["compiled_source_file_sha256"] = json!({
        "extractor":hex::encode(Sha256::digest(include_bytes!("../../../../services/transparent-filter-server/src/extract.rs"))),
        "census":hex::encode(Sha256::digest(include_bytes!("census.rs"))),
    });
    identity["binary_sha256"] = json!(hex::encode(Sha256::digest(std::fs::read(
        std::env::current_exe()?
    )?)));
    Ok(identity)
}
