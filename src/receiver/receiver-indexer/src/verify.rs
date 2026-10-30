//! The checks the receiver protocol crates leave to their caller: every payment's
//! receiver and Action fields, once, before it is stored. Wallets check every served
//! record and witness path against their own key and tree, so publications are not
//! checked again. The checks are `zakura-pir-protocol`'s `receiver::crypto`; this
//! module maps the local protocol types onto its types and its canonical record bytes.
use receiver_store::{Error, IndexedBlock, Store};
use zakura_pir_publisher::receiver::crypto::Record;
use zakura_pir_publisher::receiver::{Payment, RawRecord, ReceiverBytes};

/// The library's refusal, as the directory error a caller already handles.
pub fn directory_error(error: zakura_pir_publisher::receiver::crypto::Error) -> Error {
    match error {
        zakura_pir_publisher::receiver::crypto::Error::Malformed => Error::Malformed,
        zakura_pir_publisher::receiver::crypto::Error::Coverage => Error::Coverage,
        zakura_pir_publisher::receiver::crypto::Error::Capacity => Error::Capacity,
    }
}

/// Checks a payment to `receiver` before it is stored.
pub fn check_payment(receiver: &ReceiverBytes, payment: &Payment) -> Result<(), Error> {
    let slot = RawRecord {
        receiver: *receiver,
        page: 0,
        total: 1,
        payment: payment.clone(),
        ordinal: 0,
    }
    .encode()
    .map_err(|_| Error::Malformed)?;
    // A well-formed record has a valid receiver, note commitment, Action nullifier and
    // ephemeral key.
    Record::decode(&slot).map_err(directory_error).map(|_| ())
}

/// Checks every payment of `block`, then appends it to `store`.
pub fn append(store: &mut Store, block: &IndexedBlock) -> Result<(), Error> {
    for (receiver, payment) in &block.payments {
        check_payment(receiver, payment)?;
    }
    store.append(block)
}

#[cfg(test)]
mod tests;
