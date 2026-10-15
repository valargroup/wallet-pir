//! Payment records and their fixed-width row encoding.
use crate::Error;
use orchard::{
    note::{ExtractedNoteCommitment, Nullifier},
    note_encryption::IronwoodDomain,
};
use receiver_wire::RawRecord;
pub use receiver_wire::{Payment, RECORD_BYTES};
use zcash_note_encryption::{Domain, EphemeralKeyBytes};

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

/// One payment per page; every page repeats the count for the same immutable revision.
/// Encoding and decoding validate the receiver, note commitment, Action nullifier,
/// and ephemeral key in addition to the wire structure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Record {
    pub receiver: Receiver,
    pub page: u32,
    pub total: u32,
    pub payment: Payment,
}

impl Record {
    /// Encode one row slot, rejecting invalid page bounds, noncanonical field
    /// elements and invalid ephemeral keys.
    pub fn encode(&self) -> Result<[u8; RECORD_BYTES], Error> {
        self.check()?;
        RawRecord {
            receiver: *self.receiver.as_bytes(),
            page: self.page,
            total: self.total,
            payment: self.payment.clone(),
        }
        .encode()
        .map_err(|_| Error::Malformed)
    }

    /// Only an entirely zero slot is empty. Corruption must never advance a gap limit.
    /// Receiver and action fields are cryptographically checked before returning.
    pub fn decode(bytes: &[u8]) -> Result<Option<Self>, Error> {
        let Some(raw) = RawRecord::decode(bytes).map_err(|_| Error::Malformed)? else {
            return Ok(None);
        };
        let record = Self {
            receiver: Receiver::from_bytes(raw.receiver)?,
            page: raw.page,
            total: raw.total,
            payment: raw.payment,
        };
        record.check()?;
        Ok(Some(record))
    }

    /// Checks the page against its total and the fields [`Payment`] requires.
    fn check(&self) -> Result<(), Error> {
        let p = &self.payment;
        if self.total == 0
            || self.page >= self.total
            || ExtractedNoteCommitment::from_bytes(&p.cmx).is_none().into()
            || Nullifier::from_bytes(&p.action_nullifier).is_none().into()
            || IronwoodDomain::epk(&EphemeralKeyBytes(p.ephemeral_key)).is_none()
        {
            return Err(Error::Malformed);
        }
        Ok(())
    }
}
