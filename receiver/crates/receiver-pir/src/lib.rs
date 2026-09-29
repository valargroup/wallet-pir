//! Encrypted row retrieval. Results are candidates for wallet validation, not chain proofs.
mod client;
#[cfg(feature = "http")]
pub mod http;
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

pub const PROTOCOL: &str = "ironwood-receiver-pir-v1-q48";
pub const ROWS: usize = 8192;
pub const HEADER_BYTES: usize = 52;
const MAGIC: &[u8; 4] = b"RPQ1";

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
    #[error("payment history exceeds the caller's page budget")]
    PageBudget,
    #[error("receiver transport failed: {0}")]
    Transport(String),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[cfg(feature = "http")]
    #[error(transparent)]
    Http(#[from] reqwest::Error),
}

/// Both the directory and its PIR setup belong to one immutable session.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Manifest {
    pub protocol: String,
    pub directory: snapshot::Manifest,
    pub public_digest: Hash,
}

impl Manifest {
    pub fn validate(&self) -> Result<(), Error> {
        self.directory.validate()?;
        if self.protocol != PROTOCOL || self.directory.rows != ROWS as u32 {
            return Err(Error::Unsupported);
        }
        Ok(())
    }

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
    pub fn check(&self, m: &snapshot::Manifest) -> Result<(), Error> {
        Ok(m.accept(self.genesis, self.required_start, self.height, self.hash)?)
    }
}

fn profile() -> &'static ProductionSimplePirParams {
    static PROFILE: OnceLock<ProductionSimplePirParams> = OnceLock::new();
    PROFILE.get_or_init(|| {
        ProductionSimplePirParams::new(
            ROWS as u64,
            (snapshot::ROW_BYTES * 8) as u64,
            SimplePirProfile::P16Q48,
        )
        .expect("fixed receiver PIR profile")
    })
}

fn setup_seed(m: &snapshot::Manifest) -> Result<Hash, Error> {
    let mut h = Sha256::new();
    h.update(b"ironwood-receiver-pir/v1/q48/setup\0");
    h.update(m.revision()?);
    Ok(h.finalize().into())
}

pub fn public_bytes() -> usize {
    let p = profile();
    p.ypir().db_cols / p.rlwe().d
        * ipir_sp::modulus_switch::published_c1_len(p.rlwe().d, p.rlwe().q)
}

pub fn query_bytes() -> usize {
    HEADER_BYTES + ipir_sp::serialize::serialized_packing_keys_len(profile().rlwe()) + ROWS * 6
}

pub fn response_bytes() -> usize {
    let p = profile();
    HEADER_BYTES
        + p.ypir().db_cols / p.rlwe().d
            * ipir_sp::modulus_switch::response_body_len(p.rlwe().d, p.ypir().q_prime_1)
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
