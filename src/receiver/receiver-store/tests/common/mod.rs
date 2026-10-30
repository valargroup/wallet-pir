//! Fixtures shared by the receiver services' tests, included with `#[path]`.
#![allow(dead_code)]
use zakura_pir_publisher::receiver::{Payment, RawRecord, ReceiverBytes};

/// The Orchard receiver that the Action in `fixtures/zero-ovk-action.json` pays,
/// decoded independently from the swap's `refundTo` address; see
/// `fixtures/zero-ovk-action-oracle/README.md`. The fixture's `receiver` field holds the
/// same bytes.
pub const RECEIVER_HEX: &str =
    "42cd776949bba6f2164b347ebdbf9c225028ed0f5b6eaa3a402f3322c6aadfbde6288d167dbcaed0fdf01e";

/// The receiver from [`RECEIVER_HEX`], a valid Ironwood address.
pub fn receiver() -> ReceiverBytes {
    hex::decode(RECEIVER_HEX).unwrap().try_into().unwrap()
}

/// Page `page` of `total` payments to [`receiver`] in block 101 (`[3; 32]`), at
/// position `200 + page`.
pub fn record(page: u32, total: u32) -> RawRecord {
    RawRecord {
        receiver: receiver(),
        page,
        total,
        payment: Payment {
            height: 101,
            block_hash: [3; 32],
            txid: [page as u8; 32],
            tx_index: page + 1,
            action_index: 0,
            position: 200 + u64::from(page),
            action_nullifier: [5; 32],
            cmx: [6; 32],
            ephemeral_key: [7; 32],
            ciphertext_prefix: [8; 52],
        },
        ordinal: page,
    }
}

/// The Ironwood receiver at external index zero of the spending key whose bytes are all
/// `seed`, for seeds 1 to 4, pinned in `fixtures/receivers.json`.
pub fn seeded_receiver(seed: u8) -> ReceiverBytes {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/receivers.json")).unwrap();
    hex::decode(fixture["seeds"][seed.to_string()].as_str().unwrap())
        .unwrap()
        .try_into()
        .unwrap()
}

/// [`record`]'s payment to [`receiver`] with an all-zero witness payload, as a
/// segment build takes it.
pub fn paid(page: u32) -> (ReceiverBytes, Payment, [u8; 512]) {
    (receiver(), record(page, 1).payment, [0; 512])
}
