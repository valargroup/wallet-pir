//! The canonical transparent event model.
//!
//! One fixed-width record shape is shared by everything that touches an event:
//! the indexer that appends it to its journal, the builder that packs it into a
//! shard's directory and pages, and the wallet that decodes it out of a private
//! response. A single encoding is what makes those three agree; three
//! hand-written ones would agree only until they didn't.
//!
//! Two events exist, and between them they are the whole recovery scope: every
//! transparent output controlled by a supported script, and every later input
//! that consumes one of those outputs. Both are indexed under the *exact raw
//! locking script* of the output in question — for a spend, that is the script
//! of the output being consumed, not anything in the spending input.
//!
//! # What is deliberately absent
//!
//! There is no block hash in a record. It would cost 32 bytes of every 96, and
//! it would be worse than useless: a wallet that took an event's block hash
//! from the server could be handed an event placed on a branch it has not
//! accepted. The height is in the record, and the wallet resolves height to
//! hash against its own accepted chain, which is the only source that can
//! answer that question safely.
//!
//! There is no script in a record either. The script is the *key* a record is
//! stored under, and it is carried once in the enclosing directory entry or
//! page header rather than once per event. A decoder must therefore always
//! learn the script from the container and check it, never infer it.

use std::fmt;

/// Serialized size of one event. Fixed, and the same for both kinds.
///
/// Uniform width is not an aesthetic choice: a shard's pages are padded to a
/// fixed row size and a PIR response has fixed geometry, so a variable-width
/// event would leak the shape of a history through the number of rows needed to
/// hold it.
pub const EVENT_BYTES: usize = 96;

const KIND_RECEIVE: u8 = 0;
const KIND_SPEND: u8 = 1;

const FLAG_COINBASE: u8 = 1 << 0;
const KNOWN_FLAGS: u8 = FLAG_COINBASE;

// Field offsets within a record.
const OFF_KIND: usize = 0;
const OFF_FLAGS: usize = 1;
const OFF_TX_INDEX: usize = 2;
const OFF_HEIGHT: usize = 4;
const OFF_VALUE: usize = 8;
const OFF_TXID: usize = 16;
const OFF_INDEX: usize = 48;
const OFF_SPENT_TXID: usize = 52;
const OFF_SPENT_INDEX: usize = 84;
const OFF_RESERVED: usize = 88;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum EventError {
    #[error("event record is {0} bytes, expected {EVENT_BYTES}")]
    Length(usize),
    #[error("unknown event kind {0}")]
    Kind(u8),
    #[error("unknown flag bits set: {0:#04x}")]
    Flags(u8),
    #[error("reserved bytes are not zero")]
    Reserved,
    #[error("a {kind} event must not set {field}")]
    UnusedField {
        kind: &'static str,
        field: &'static str,
    },
}

/// A transaction id in internal (serialized) byte order.
///
/// The same discipline as block hashes: display order is the reverse, and is
/// only ever produced at a presentation boundary.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Txid(pub [u8; 32]);

impl Txid {
    pub fn to_display_hex(self) -> String {
        let mut bytes = self.0;
        bytes.reverse();
        hex::encode(bytes)
    }
}

impl fmt::Debug for Txid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Txid({})", self.to_display_hex())
    }
}

/// An output created under a wallet-supported script.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReceiveEvent {
    pub height: u32,
    pub txid: Txid,
    pub transaction_index: u16,
    pub output_index: u32,
    pub value: u64,
    /// Retained for the wallet's maturity policy. Recovering a coinbase receive
    /// does not make it spendable; that decision stays with the wallet.
    pub coinbase: bool,
}

/// An input consuming an output that was created under a supported script.
///
/// `spent_txid` and `spent_output_index` identify the consumed outpoint, which
/// is what lets a wallet remove the right entry from its UTXO map. The script
/// this event is indexed under is the *consumed output's* script.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SpendEvent {
    pub height: u32,
    pub spending_txid: Txid,
    pub transaction_index: u16,
    pub input_index: u32,
    pub spent_txid: Txid,
    pub spent_output_index: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransparentEvent {
    Receive(ReceiveEvent),
    Spend(SpendEvent),
}

/// A receive's stable identity: the outpoint it creates.
pub type ReceiveIdentity = (Txid, u32);

/// A spend's stable identity.
///
/// The spending input *and* the consumed outpoint, together. The spender alone
/// would not be unique — one transaction can consume several of a wallet's
/// outputs — and the consumed outpoint alone would collide across a reorg that
/// re-spends it in a different transaction.
pub type SpendIdentity = (Txid, u32, Txid, u32);

impl TransparentEvent {
    pub fn height(&self) -> u32 {
        match self {
            TransparentEvent::Receive(event) => event.height,
            TransparentEvent::Spend(event) => event.height,
        }
    }

    pub fn transaction_index(&self) -> u16 {
        match self {
            TransparentEvent::Receive(event) => event.transaction_index,
            TransparentEvent::Spend(event) => event.transaction_index,
        }
    }

    /// The transaction this event belongs to.
    ///
    /// For a receive that is the creating transaction; for a spend, the
    /// spending one. This is the key a wallet uses to group events for history
    /// presentation, and the key into the optional transaction-detail table.
    pub fn txid(&self) -> Txid {
        match self {
            TransparentEvent::Receive(event) => event.txid,
            TransparentEvent::Spend(event) => event.spending_txid,
        }
    }

    /// A total order that is stable across rebuilds.
    ///
    /// Pages are ordered by this, and a wallet checks the ordering it receives,
    /// so it must be a function of the event alone — never of iteration order,
    /// hash-map layout, or the order the indexer happened to see transactions
    /// in. Height first, then position within the block, then the event's own
    /// identity to break ties within one transaction.
    pub fn sort_key(&self) -> (u32, u16, u8, Txid, u32, Txid, u32) {
        match self {
            TransparentEvent::Receive(event) => (
                event.height,
                event.transaction_index,
                KIND_RECEIVE,
                event.txid,
                event.output_index,
                Txid([0; 32]),
                0,
            ),
            TransparentEvent::Spend(event) => (
                event.height,
                event.transaction_index,
                KIND_SPEND,
                event.spending_txid,
                event.input_index,
                event.spent_txid,
                event.spent_output_index,
            ),
        }
    }

    pub fn to_bytes(self) -> [u8; EVENT_BYTES] {
        let mut bytes = [0u8; EVENT_BYTES];
        match self {
            TransparentEvent::Receive(event) => {
                bytes[OFF_KIND] = KIND_RECEIVE;
                if event.coinbase {
                    bytes[OFF_FLAGS] = FLAG_COINBASE;
                }
                bytes[OFF_TX_INDEX..OFF_HEIGHT]
                    .copy_from_slice(&event.transaction_index.to_le_bytes());
                bytes[OFF_HEIGHT..OFF_VALUE].copy_from_slice(&event.height.to_le_bytes());
                bytes[OFF_VALUE..OFF_TXID].copy_from_slice(&event.value.to_le_bytes());
                bytes[OFF_TXID..OFF_INDEX].copy_from_slice(&event.txid.0);
                bytes[OFF_INDEX..OFF_SPENT_TXID].copy_from_slice(&event.output_index.to_le_bytes());
            }
            TransparentEvent::Spend(event) => {
                bytes[OFF_KIND] = KIND_SPEND;
                bytes[OFF_TX_INDEX..OFF_HEIGHT]
                    .copy_from_slice(&event.transaction_index.to_le_bytes());
                bytes[OFF_HEIGHT..OFF_VALUE].copy_from_slice(&event.height.to_le_bytes());
                bytes[OFF_TXID..OFF_INDEX].copy_from_slice(&event.spending_txid.0);
                bytes[OFF_INDEX..OFF_SPENT_TXID].copy_from_slice(&event.input_index.to_le_bytes());
                bytes[OFF_SPENT_TXID..OFF_SPENT_INDEX].copy_from_slice(&event.spent_txid.0);
                bytes[OFF_SPENT_INDEX..OFF_RESERVED]
                    .copy_from_slice(&event.spent_output_index.to_le_bytes());
            }
        }
        bytes
    }

    /// Decodes a record, rejecting anything it does not fully understand.
    ///
    /// Strictness is the point. These bytes arrive from a PIR response, and a
    /// record with an unknown kind, an unknown flag, dirty reserved bytes, or a
    /// field set that its kind does not use is evidence that the wallet and the
    /// service disagree about the encoding. Decoding it leniently would turn
    /// that disagreement into a wrong balance instead of an error.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, EventError> {
        if bytes.len() != EVENT_BYTES {
            return Err(EventError::Length(bytes.len()));
        }
        if bytes[OFF_RESERVED..].iter().any(|byte| *byte != 0) {
            return Err(EventError::Reserved);
        }
        let flags = bytes[OFF_FLAGS];
        if flags & !KNOWN_FLAGS != 0 {
            return Err(EventError::Flags(flags));
        }
        let transaction_index =
            u16::from_le_bytes(bytes[OFF_TX_INDEX..OFF_HEIGHT].try_into().expect("2 bytes"));
        let height = u32::from_le_bytes(bytes[OFF_HEIGHT..OFF_VALUE].try_into().expect("4 bytes"));
        let value = u64::from_le_bytes(bytes[OFF_VALUE..OFF_TXID].try_into().expect("8 bytes"));
        let txid = Txid(bytes[OFF_TXID..OFF_INDEX].try_into().expect("32 bytes"));
        let index = u32::from_le_bytes(
            bytes[OFF_INDEX..OFF_SPENT_TXID]
                .try_into()
                .expect("4 bytes"),
        );
        let spent_txid = Txid(
            bytes[OFF_SPENT_TXID..OFF_SPENT_INDEX]
                .try_into()
                .expect("32 bytes"),
        );
        let spent_output_index = u32::from_le_bytes(
            bytes[OFF_SPENT_INDEX..OFF_RESERVED]
                .try_into()
                .expect("4 bytes"),
        );

        match bytes[OFF_KIND] {
            KIND_RECEIVE => {
                if spent_txid != Txid([0; 32]) || spent_output_index != 0 {
                    return Err(EventError::UnusedField {
                        kind: "receive",
                        field: "the consumed outpoint",
                    });
                }
                Ok(TransparentEvent::Receive(ReceiveEvent {
                    height,
                    txid,
                    transaction_index,
                    output_index: index,
                    value,
                    coinbase: flags & FLAG_COINBASE != 0,
                }))
            }
            KIND_SPEND => {
                if value != 0 {
                    return Err(EventError::UnusedField {
                        kind: "spend",
                        field: "a value",
                    });
                }
                if flags != 0 {
                    return Err(EventError::UnusedField {
                        kind: "spend",
                        field: "the coinbase flag",
                    });
                }
                Ok(TransparentEvent::Spend(SpendEvent {
                    height,
                    spending_txid: txid,
                    transaction_index,
                    input_index: index,
                    spent_txid,
                    spent_output_index,
                }))
            }
            kind => Err(EventError::Kind(kind)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn receive() -> TransparentEvent {
        TransparentEvent::Receive(ReceiveEvent {
            height: 3_428_143,
            txid: Txid([7; 32]),
            transaction_index: 3,
            output_index: 1,
            value: 100_000,
            coinbase: false,
        })
    }

    fn spend() -> TransparentEvent {
        TransparentEvent::Spend(SpendEvent {
            height: 3_428_200,
            spending_txid: Txid([9; 32]),
            transaction_index: 2,
            input_index: 0,
            spent_txid: Txid([7; 32]),
            spent_output_index: 1,
        })
    }

    #[test]
    fn both_kinds_round_trip() {
        for event in [receive(), spend()] {
            assert_eq!(TransparentEvent::from_bytes(&event.to_bytes()), Ok(event));
        }
    }

    #[test]
    fn a_coinbase_receive_round_trips_its_flag() {
        let event = TransparentEvent::Receive(ReceiveEvent {
            coinbase: true,
            ..match receive() {
                TransparentEvent::Receive(event) => event,
                _ => unreachable!(),
            }
        });
        assert_eq!(TransparentEvent::from_bytes(&event.to_bytes()), Ok(event));
    }

    #[test]
    fn every_record_is_the_same_width_regardless_of_kind() {
        assert_eq!(receive().to_bytes().len(), EVENT_BYTES);
        assert_eq!(spend().to_bytes().len(), EVENT_BYTES);
    }

    /// A shorter or longer slice is a framing disagreement, not a value to
    /// interpret. Accepting one would silently reinterpret every later record
    /// in a page.
    #[test]
    fn a_wrong_length_record_is_refused() {
        let bytes = receive().to_bytes();
        assert_eq!(
            TransparentEvent::from_bytes(&bytes[..EVENT_BYTES - 1]),
            Err(EventError::Length(EVENT_BYTES - 1))
        );
    }

    #[test]
    fn unknown_kinds_flags_and_dirty_reserved_bytes_are_refused() {
        let mut bytes = receive().to_bytes();
        bytes[OFF_KIND] = 7;
        assert_eq!(
            TransparentEvent::from_bytes(&bytes),
            Err(EventError::Kind(7))
        );

        let mut bytes = receive().to_bytes();
        bytes[OFF_FLAGS] = 0x80;
        assert_eq!(
            TransparentEvent::from_bytes(&bytes),
            Err(EventError::Flags(0x80))
        );

        let mut bytes = receive().to_bytes();
        bytes[EVENT_BYTES - 1] = 1;
        assert_eq!(
            TransparentEvent::from_bytes(&bytes),
            Err(EventError::Reserved)
        );
    }

    /// Fields a kind does not use must be zero. Otherwise a service could smuggle
    /// data past a wallet that ignores those bytes, and two implementations
    /// could disagree about a record while both calling it valid.
    #[test]
    fn fields_a_kind_does_not_use_must_be_zero() {
        let mut bytes = receive().to_bytes();
        bytes[OFF_SPENT_INDEX] = 1;
        assert!(matches!(
            TransparentEvent::from_bytes(&bytes),
            Err(EventError::UnusedField {
                kind: "receive",
                ..
            })
        ));

        let mut bytes = spend().to_bytes();
        bytes[OFF_VALUE] = 1;
        assert!(matches!(
            TransparentEvent::from_bytes(&bytes),
            Err(EventError::UnusedField { kind: "spend", .. })
        ));

        let mut bytes = spend().to_bytes();
        bytes[OFF_FLAGS] = FLAG_COINBASE;
        assert!(matches!(
            TransparentEvent::from_bytes(&bytes),
            Err(EventError::UnusedField { kind: "spend", .. })
        ));
    }

    /// Ordering must not depend on which kind an event is at the same position,
    /// but it must be total: two events from one transaction still order.
    #[test]
    fn the_sort_key_orders_by_chain_position_then_identity() {
        let mut events = vec![spend(), receive()];
        events.sort_by_key(|event| event.sort_key());
        assert_eq!(events, vec![receive(), spend()]);

        let a = TransparentEvent::Receive(ReceiveEvent {
            output_index: 0,
            ..match receive() {
                TransparentEvent::Receive(event) => event,
                _ => unreachable!(),
            }
        });
        assert!(a.sort_key() < receive().sort_key());
    }

    #[test]
    fn txids_display_in_reversed_order() {
        let mut bytes = [0u8; 32];
        bytes[0] = 0xab;
        assert!(Txid(bytes).to_display_hex().ends_with("ab"));
    }
}
