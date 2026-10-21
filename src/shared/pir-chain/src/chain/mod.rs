//! Minimal decoded chain data for RPC ingestion. Transactions are read with the
//! borrowed wire reader `zcash_transaction_wire`, which checks the wire grammar and
//! computes transaction IDs without the shielded proving stack. It does not check that
//! shielded fields are valid curve points; consensus, cryptographic and proof
//! verification remain the node's responsibility. The optional RocksDB adapter
//! converts through canonical bytes.
// Product features use different parts of the same private decoded wire data.
// Keep the complete decoder available without requiring every product at once.
#![allow(dead_code)]
mod transaction;
#[cfg(feature = "transparent")]
pub(crate) use transaction::Action;
pub(crate) use transaction::Transaction;

use sha2::{Digest, Sha256};
use std::{
    fmt,
    io::{self, Read},
    sync::Arc,
};
use zcash_protocol::{encoding::CompactSize, value::Zatoshis};

pub(crate) type Amount = Zatoshis;
pub(crate) const MAX_BLOCK_BYTES: u64 = 2_000_000;
pub(crate) const MIN_TRANSPARENT_TX_SIZE: u64 = 54;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Hash(pub [u8; 32]);
impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&crate::hash::display(&self.0))
    }
}
impl std::str::FromStr for Hash {
    type Err = crate::NodeError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        crate::hash::parse_display(s).map(Self)
    }
}
#[derive(Clone, Copy, Debug)]
pub(crate) struct Height(pub u32);
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Script(Vec<u8>);
impl Script {
    pub(crate) fn new(bytes: &[u8]) -> Self {
        Self(bytes.to_vec())
    }
    pub(crate) fn as_raw_bytes(&self) -> &[u8] {
        &self.0
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct OutPoint {
    pub hash: Hash,
    pub index: u32,
}
#[derive(Clone, Debug)]
pub(crate) enum Input {
    Coinbase { height: Option<Height> },
    PrevOut { outpoint: OutPoint },
}
impl Input {
    pub(crate) fn outpoint(&self) -> Option<OutPoint> {
        match self {
            Self::PrevOut { outpoint } => Some(*outpoint),
            _ => None,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Output {
    pub value: Amount,
    pub lock_script: Script,
}
impl Output {
    /// Reads a value in the money range, then a length-prefixed script.
    pub(crate) fn zcash_deserialize(mut raw: impl Read) -> io::Result<Self> {
        let mut value = [0; 8];
        raw.read_exact(&mut value)?;
        let value = Zatoshis::from_nonnegative_i64_le_bytes(value)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "value out of range"))?;
        let len = CompactSize::read(&mut raw)?;
        let mut script = Vec::new();
        raw.take(len).read_to_end(&mut script)?;
        if script.len() as u64 != len {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        Ok(Self {
            value,
            lock_script: Script(script),
        })
    }
    pub(crate) fn zcash_serialize_to_vec(&self) -> io::Result<Vec<u8>> {
        let mut bytes = self.value.to_u64_le_bytes().to_vec();
        CompactSize::write(&mut bytes, self.lock_script.0.len())?;
        bytes.extend_from_slice(&self.lock_script.0);
        Ok(bytes)
    }
}
#[derive(Clone, Debug)]
pub(crate) struct Utxo {
    pub output: Output,
}

#[derive(Debug)]
pub(crate) struct Header {
    pub previous_block_hash: Hash,
    pub merkle_root: [u8; 32],
    pub time: i64,
}
#[derive(Debug)]
pub(crate) struct Block {
    pub header: Header,
    pub transactions: Vec<Arc<Transaction>>,
    hash: Hash,
}
/// The serialized length of a header before its Equihash solution.
const HEADER_FIXED_BYTES: usize = 4 + 32 + 32 + 32 + 4 + 4 + 32;
impl Header {
    /// Reads a header and returns it with its hash, the SHA-256d of its encoding.
    fn read(input: &mut &[u8]) -> io::Result<(Self, Hash)> {
        let start = *input;
        let (fixed, mut rest) = input
            .split_at_checked(HEADER_FIXED_BYTES)
            .ok_or(io::ErrorKind::UnexpectedEof)?;
        let version = i32::from_le_bytes(fixed[..4].try_into().expect("four bytes"));
        if version < 4 {
            return Err(io::Error::other("invalid block version"));
        }
        let solution = CompactSize::read(&mut rest)?;
        if !matches!(solution, 36 | 1344) {
            return Err(io::Error::other("invalid Equihash solution length"));
        }
        let rest = rest
            .get(solution as usize..)
            .ok_or(io::ErrorKind::UnexpectedEof)?;
        let encoded = &start[..start.len() - rest.len()];
        let hash = Hash(Sha256::digest(Sha256::digest(encoded)).into());
        let field = |at: usize| -> [u8; 32] { fixed[at..at + 32].try_into().expect("32 bytes") };
        let header = Self {
            previous_block_hash: Hash(field(4)),
            merkle_root: field(36),
            time: u32::from_le_bytes(fixed[100..104].try_into().expect("four bytes")).into(),
        };
        *input = rest;
        Ok((header, hash))
    }
}
impl Block {
    /// Reads one block from the start of `input` and advances it past the block. At
    /// most [`MAX_BLOCK_BYTES`] are read.
    pub(crate) fn zcash_deserialize(input: &mut &[u8]) -> io::Result<Self> {
        let mut reader = &input[..input.len().min(MAX_BLOCK_BYTES as usize)];
        let (header, hash) = Header::read(&mut reader)?;
        let count = CompactSize::read(&mut reader)?;
        if count == 0 || count > MAX_BLOCK_BYTES / MIN_TRANSPARENT_TX_SIZE {
            return Err(io::Error::other("invalid block transaction count"));
        }
        let mut transactions = Vec::new();
        for _ in 0..count {
            transactions.push(Arc::new(Transaction::zcash_deserialize(&mut reader)?));
        }
        *input = &input[input.len().min(MAX_BLOCK_BYTES as usize) - reader.len()..];
        Ok(Self {
            hash,
            header,
            transactions,
        })
    }
    pub(crate) fn hash(&self) -> Hash {
        self.hash
    }
    pub(crate) fn coinbase_height(&self) -> Option<Height> {
        match self.transactions.first()?.inputs().first()? {
            Input::Coinbase { height } => *height,
            _ => None,
        }
    }
    pub(crate) fn merkle_root(&self) -> [u8; 32] {
        let mut hashes: Vec<_> = self.transactions.iter().map(|tx| tx.hash().0).collect();
        while hashes.len() > 1 {
            if hashes.len() % 2 != 0 {
                hashes.push(*hashes.last().expect("nonempty level"));
            }
            hashes = hashes
                .as_chunks::<2>()
                .0
                .iter()
                .map(|pair| {
                    let mut sha = Sha256::new();
                    sha.update(pair[0]);
                    sha.update(pair[1]);
                    Sha256::digest(sha.finalize()).into()
                })
                .collect();
        }
        hashes.first().copied().unwrap_or([0; 32])
    }
}

#[cfg(test)]
mod tests;
