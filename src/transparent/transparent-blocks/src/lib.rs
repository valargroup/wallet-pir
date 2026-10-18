//! Pinned compact-block benchmark format, immutable datasets and wallet scanning.
//! This is a benchmark transport, not a lightwalletd compatibility implementation.
pub mod dataset;
pub mod scan;
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/cash.z.wallet.sdk.rpc.rs"));
}
pub fn digest(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(bytes))
}
pub fn schema_digest() -> String {
    digest(include_bytes!("../compact_formats.proto"))
}
