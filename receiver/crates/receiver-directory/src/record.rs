//! Payment records and their fixed-width row encoding.
use crate::{Error, Hash};

/// Full canonical receiver. The same encoding is shared by refund and incoming keys.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct Receiver([u8; 43]);

impl Receiver {
    /// Accept only bytes that decode to a valid Ironwood address.
    pub fn from_bytes(bytes: [u8; 43]) -> Result<Self, Error> {
        Option::<orchard::Address>::from(orchard::Address::from_raw_address_bytes(&bytes))
            .map(|_| Self(bytes))
            .ok_or(Error::Malformed)
    }
    /// The raw 43-byte address.
    pub fn as_bytes(&self) -> &[u8; 43] {
        &self.0
    }
}

/// Compact context plus location. All hashes use protocol byte order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Payment {
    pub height: u32,
    pub block_hash: Hash,
    pub txid: Hash,
    pub tx_index: u32,
    pub action_index: u32,
    pub position: u64,
    /// The Action's input nullifier, not the received note's spend nullifier.
    pub action_nullifier: Hash,
    pub cmx: Hash,
    pub ephemeral_key: Hash,
    pub ciphertext_prefix: [u8; 52],
}

/// One payment per page; every page repeats the count for the same immutable revision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Record {
    pub receiver: Receiver,
    pub page: u32,
    pub total: u32,
    pub payment: Payment,
}

/// Encoded size of one [`Record`].
pub const RECORD_BYTES: usize = 285;

impl Record {
    /// Encode one row slot. A page outside its total is malformed.
    pub fn encode(&self) -> Result<[u8; RECORD_BYTES], Error> {
        if self.total == 0 || self.page >= self.total {
            return Err(Error::Malformed);
        }
        let p = &self.payment;
        let mut out = [0; RECORD_BYTES];
        out[0] = 1;
        out[1..44].copy_from_slice(self.receiver.as_bytes());
        out[44..48].copy_from_slice(&self.page.to_le_bytes());
        out[48..52].copy_from_slice(&self.total.to_le_bytes());
        out[52] = 1;
        out[53..57].copy_from_slice(&p.height.to_le_bytes());
        out[57..89].copy_from_slice(&p.block_hash);
        out[89..121].copy_from_slice(&p.txid);
        out[121..125].copy_from_slice(&p.tx_index.to_le_bytes());
        out[125..129].copy_from_slice(&p.action_index.to_le_bytes());
        out[129..137].copy_from_slice(&p.position.to_le_bytes());
        out[137..169].copy_from_slice(&p.action_nullifier);
        out[169..201].copy_from_slice(&p.cmx);
        out[201..233].copy_from_slice(&p.ephemeral_key);
        out[233..285].copy_from_slice(&p.ciphertext_prefix);
        Ok(out)
    }

    /// Only an entirely zero slot is empty. Corruption must never advance a gap limit.
    pub fn decode(b: &[u8]) -> Result<Option<Self>, Error> {
        if b.len() != RECORD_BYTES {
            return Err(Error::Malformed);
        }
        if b.iter().all(|x| *x == 0) {
            return Ok(None);
        }
        if b[0] != 1 || b[52] != 1 {
            return Err(Error::Malformed);
        }
        let r = Self {
            receiver: Receiver::from_bytes(b[1..44].try_into().unwrap())?,
            page: u32::from_le_bytes(b[44..48].try_into().unwrap()),
            total: u32::from_le_bytes(b[48..52].try_into().unwrap()),
            payment: Payment {
                height: u32::from_le_bytes(b[53..57].try_into().unwrap()),
                block_hash: b[57..89].try_into().unwrap(),
                txid: b[89..121].try_into().unwrap(),
                tx_index: u32::from_le_bytes(b[121..125].try_into().unwrap()),
                action_index: u32::from_le_bytes(b[125..129].try_into().unwrap()),
                position: u64::from_le_bytes(b[129..137].try_into().unwrap()),
                action_nullifier: b[137..169].try_into().unwrap(),
                cmx: b[169..201].try_into().unwrap(),
                ephemeral_key: b[201..233].try_into().unwrap(),
                ciphertext_prefix: b[233..285].try_into().unwrap(),
            },
        };
        if r.total == 0 || r.page >= r.total {
            return Err(Error::Malformed);
        }
        Ok(Some(r))
    }
}
