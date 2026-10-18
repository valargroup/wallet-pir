//! Synthetic mixed transactions and a trusted-RPC test double, from wallet-libraries'
//! server chain fixtures, and the frozen Enhance records of the public fixture.
//!
//! Mixed transactions keep the public fixture's two Ironwood actions and
//! replace its value balance and transparent parts. Values are hand-set, so
//! expected fees are independent of the producer. The node double serves
//! single JSON-RPC 1.0 calls and JSON-RPC 2.0 batches. Proofs and signatures
//! are not valid; the producer neither checks nor needs them.
#![allow(dead_code, unused_imports)]

use enhance_pir::EnhanceRecord;
pub use zakura_server_chain::testing::{
    block, cookie, funding, ironwood_coinbase, serve, serve_chain, spend, BatchFault, Chain, Mixed,
    SharedChain, Tx, IRONWOOD_FIXTURE_HEX as FIXTURE_HEX,
};

/// The public Ironwood fixture transaction.
pub fn fixture() -> Tx {
    zakura_server_chain::testing::ironwood_fixture()
}

/// `transaction` as `getrawtransaction` answers.
pub fn raw(transaction: &Tx) -> String {
    transaction.raw_hex()
}

/// The fixture's frozen record for action `index`, without epk and compact prefix.
pub fn fixture_record(index: usize) -> EnhanceRecord {
    let hex = [
        include_str!("../fixtures/ironwood-canonical-record-0.hex"),
        include_str!("../fixtures/ironwood-canonical-record-1.hex"),
    ][index];
    let bytes = hex::decode(hex.trim()).unwrap()[84..].to_vec();
    EnhanceRecord::from_bytes(bytes.try_into().unwrap()).unwrap()
}

/// Asserts the record keeps the fixture action's encrypted fields.
pub fn assert_fixture_action(record: &EnhanceRecord, index: usize) {
    let fixture = fixture_record(index);
    assert_eq!(
        record.enc_ciphertext_suffix(),
        fixture.enc_ciphertext_suffix()
    );
    assert_eq!(record.cv_net(), fixture.cv_net());
    assert_eq!(record.out_ciphertext(), fixture.out_ciphertext());
    assert_eq!(record.metadata().expiry_height(), 3483371);
}
