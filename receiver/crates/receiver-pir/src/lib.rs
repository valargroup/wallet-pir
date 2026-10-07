//! Encrypted row retrieval. Results are candidates for wallet validation, not chain proofs.
mod client;
#[cfg(feature = "server")]
pub mod server;
pub mod transport;

pub use client::{Client, Query};
use ipir_sp::{ProductionSimplePirParams, SimplePirProfile};
use receiver_directory::Record;
use receiver_directory::{snapshot, Hash};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::OnceLock;

/// The session protocol, including its fixed 48-bit query encoding.
pub const PROTOCOL: &str = "ironwood-receiver-pir-v1-q48";
/// Smallest served directory. Publications grow by powers of two up to `MAX_ROWS`.
pub const MIN_ROWS: u32 = 8192;
pub use receiver_directory::snapshot::MAX_ROWS;
/// Request header length: [`MAGIC`], the session ID and a fresh nonce.
pub const HEADER_BYTES: usize = 52;
/// Leading bytes of every request header, which responses echo.
pub const MAGIC: &[u8; 4] = b"RPQ1";
/// Bound on a serialized directory or session manifest, read before parsing.
pub const MAX_MANIFEST_BYTES: usize = 16384;

/// Why a connection, lookup or evaluation failed.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Directory(#[from] receiver_directory::Error),
    #[error("unsupported receiver PIR protocol or geometry")]
    Unsupported,
    #[error("malformed receiver PIR message")]
    Malformed,
    #[error("receiver PIR operation failed")]
    Pir,
    #[error("receiver publication changed")]
    Revision,
    #[error("receiver transport failed: {0}")]
    Transport(String),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

/// Both the directory and its PIR setup belong to one immutable session.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Manifest {
    pub protocol: String,
    pub directory: snapshot::Manifest,
    pub public_digest: Hash,
}

impl Manifest {
    /// Check the directory manifest, the protocol and the supported geometry.
    pub fn validate(&self) -> Result<(), Error> {
        self.directory.validate()?;
        if self.protocol != PROTOCOL {
            return Err(Error::Unsupported);
        }
        validate_rows(self.directory.rows)
    }

    /// The session ID that pins every route and request to this manifest.
    pub fn id(&self) -> Result<Hash, Error> {
        self.validate()?;
        let mut h = Sha256::new();
        h.update(b"ironwood-receiver-pir/v1/session\0");
        h.update(serde_json::to_vec(self)?);
        Ok(h.finalize().into())
    }
}

/// Supply this from wallet chain validation, never by copying the directory manifest.
#[derive(Clone, Copy, Debug)]
pub struct AcceptedCoverage {
    pub genesis: Hash,
    pub required_start: u32,
    pub height: u32,
    pub hash: Hash,
}

impl AcceptedCoverage {
    /// Fail unless `m` ends at this anchor and covers the required history.
    pub fn check(&self, m: &snapshot::Manifest) -> Result<(), Error> {
        Ok(m.accept(self.genesis, self.required_start, self.height, self.hash)?)
    }
}

/// Validate the common publisher, server and client geometry contract.
pub fn validate_rows(rows: u32) -> Result<(), Error> {
    if !rows.is_power_of_two() || !(MIN_ROWS..=MAX_ROWS).contains(&rows) {
        return Err(Error::Unsupported);
    }
    Ok(())
}

/// The PIR parameters for a directory of `rows` rows, built once per size.
fn profile(rows: u32) -> Result<&'static ProductionSimplePirParams, Error> {
    validate_rows(rows)?;
    const COUNT: usize = (MAX_ROWS / MIN_ROWS).ilog2() as usize + 1;
    static PROFILES: [OnceLock<ProductionSimplePirParams>; COUNT] =
        [const { OnceLock::new() }; COUNT];
    let p = PROFILES[(rows / MIN_ROWS).ilog2() as usize].get_or_init(|| {
        ProductionSimplePirParams::new(
            u64::from(rows),
            (snapshot::ROW_BYTES * 8) as u64,
            SimplePirProfile::P16Q48,
        )
        .expect("supported receiver PIR profile")
    });
    // The protocol fixes the query encoding. Never silently use fewer bits than
    // the underlying profile requires when adding another directory size.
    if p.ypir().query_bits != 48 {
        return Err(Error::Unsupported);
    }
    Ok(p)
}

/// The public setup seed, bound to the manifest's revision.
fn setup_seed(m: &snapshot::Manifest) -> Result<Hash, Error> {
    let mut h = Sha256::new();
    h.update(b"ironwood-receiver-pir/v1/q48/setup\0");
    h.update(m.revision()?);
    Ok(h.finalize().into())
}

/// Exact public setup length for a supported publication.
pub fn public_bytes(rows: u32) -> Result<usize, Error> {
    let p = profile(rows)?;
    Ok(p.ypir().db_cols / p.rlwe().d
        * ipir_sp::modulus_switch::published_c1_len(p.rlwe().d, p.rlwe().q))
}

/// Exact request length, including the session header and packing keys.
pub fn query_bytes(rows: u32) -> Result<usize, Error> {
    let p = profile(rows)?;
    Ok(HEADER_BYTES
        + ipir_sp::serialize::serialized_packing_keys_len(p.rlwe())
        + (p.ypir().db_rows * p.ypir().query_bits).div_ceil(8))
}

/// Exact response length for a supported publication.
pub fn response_bytes(rows: u32) -> Result<usize, Error> {
    let p = profile(rows)?;
    Ok(HEADER_BYTES
        + p.ypir().db_cols / p.rlwe().d
            * ipir_sp::modulus_switch::response_body_len(p.rlwe().d, p.ypir().q_prime_1))
}

/// Reject inconsistent continuation pages before exposing a complete history to a wallet.
fn check_next(previous: &Record, next: &Record) -> Result<(), Error> {
    let a = &previous.payment;
    let b = &next.payment;
    if next.receiver != previous.receiver
        || next.page != previous.page + 1
        || next.total != previous.total
        || b.position <= a.position
        || (b.height, b.tx_index, b.action_index) <= (a.height, a.tx_index, a.action_index)
        || (b.height == a.height && b.block_hash != a.block_hash)
        || (b.height == a.height && b.tx_index == a.tx_index && b.txid != a.txid)
    {
        return Err(Error::Malformed);
    }
    Ok(())
}
