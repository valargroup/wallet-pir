//! Turning an accepted Zcash block into its filter element set.
//!
//! The rules, from the profile:
//!
//! 1. every nonempty transparent output script, including coinbase outputs,
//!    except a script whose *first* opcode byte is `OP_RETURN`;
//! 2. the nonempty previous-output **locking** script of every transparent
//!    input, excluding the coinbase input — not the unlocking script, not the
//!    transaction id, not the outpoint, and not any address text;
//! 3. deduplication by raw script bytes across the whole block;
//! 4. an unresolvable previous output is a construction error.
//!
//! Elements come from the raw parsed block, not from any helper that filters to
//! the script types some other component happens to support. A shared public
//! filter that silently omitted unusual scripts would report "no activity" to a
//! wallet that does have activity.

use std::sync::Arc;
use transparent_events::{
    FeeState, ReceiveEvent, SpendEvent, TransactionMetadata, TransparentEvent, Txid,
};
use transparent_filter::ScriptBytes;
use transparent_shard::txid::DisplayOutput;
use transparent_shard::txid_v1::TransparentDisplayRecord;
use transparent_shard::txid_v2x::{DisplayInput, TransparentDisplayRecordV2x};
use zakura_chain::transaction::Transaction;
use zakura_chain::transparent::{Input, OutPoint, Output, Utxo};

#[derive(Debug, thiserror::Error)]
pub enum ExtractError {
    #[error("transaction metadata: {0}")]
    Metadata(String),
    #[error("previous output {0} is unavailable; refusing to build a partial filter")]
    MissingPreviousOutput(String),
    #[error("block has {0} transactions, more than the event encoding can index")]
    BlockTooLarge(usize),
    #[error("previous output lookup failed for {outpoint}: {source}")]
    Lookup {
        outpoint: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

/// Renders an outpoint for diagnostics. Display order, as a person would look
/// it up.
pub fn outpoint_label(outpoint: &OutPoint) -> String {
    let mut bytes = outpoint.hash.0;
    bytes.reverse();
    format!("{}:{}", hex::encode(bytes), outpoint.index)
}

/// Supplies the full value and locking script of an output created before this block.
pub trait PreviousOutputs {
    /// Returns the complete `outpoint` output, or `None` if it is not known.
    ///
    /// `None` aborts the block. It must never be treated as an empty script:
    /// that would publish a filter missing a real spend, and a wallet checking
    /// it would be told it had no activity.
    fn previous_output(
        &mut self,
        outpoint: &OutPoint,
    ) -> Result<Option<Output>, Box<dyn std::error::Error + Send + Sync>>;
}

/// One event, paired with the exact raw script it is indexed under.
///
/// The pairing is the whole point: a spend is indexed under the script of the
/// output it *consumes*, which is nowhere in the spending input and has to be
/// resolved. Keeping the two together means no later stage can re-derive the
/// key and get it wrong.
pub type IndexedEvent = (ScriptBytes, TransparentEvent);

/// Every indexed event in one block.
///
/// This is the primary extraction; [`extract_elements`] is a projection of it.
/// They were once separate, and separate is how a filter comes to disagree with
/// the events it gates: a script present in the events but missing from the
/// filter is a permanent, silent coverage hole, because the wallet would be
/// told it had no activity and would never look.
///
/// Same-block spends resolve from this block's own outputs before the resolver
/// is consulted, so a transaction spending an output created earlier in the
/// same block needs no lookup.
///
/// Takes the transaction list rather than the whole block: the block hash is
/// not part of an event. A wallet resolves height to hash against its own
/// accepted chain, which is the only source that can safely answer whether an
/// event sits on a branch it accepts.
pub fn extract_block(
    transactions: &[Arc<Transaction>],
    previous: &mut impl PreviousOutputs,
    height: u32,
) -> Result<ExtractedBlock, ExtractError> {
    extract(transactions, previous, height, true)
}

/// [`extract_block`], building display source records only when `display`
/// is set. History and filter ingest never need them, so a record the display
/// codec refuses (a v2x record is bounded; the block's events are not) can
/// only stop the display ingest, never the history.
pub fn extract(
    transactions: &[Arc<Transaction>],
    previous: &mut impl PreviousOutputs,
    height: u32,
    display: bool,
) -> Result<ExtractedBlock, ExtractError> {
    // Every output this block creates, keyed by outpoint, for same-block spends.
    let mut created: std::collections::HashMap<OutPoint, Output> = std::collections::HashMap::new();
    for transaction in transactions {
        let txid = transaction.hash();
        for (index, output) in transaction.outputs().iter().enumerate() {
            created.insert(
                OutPoint {
                    hash: txid,
                    index: index as u32,
                },
                output.clone(),
            );
        }
    }

    let mut events: Vec<IndexedEvent> = Vec::new();
    let mut records = Vec::new();

    for (transaction_index, transaction) in transactions.iter().enumerate() {
        let txid = Txid(transaction.hash().0);
        // A transaction index beyond u16 would silently alias another
        // transaction's events under this one's ordering. No Zcash block comes
        // close, which is exactly why it must be checked rather than assumed.
        let transaction_index = u16::try_from(transaction_index)
            .map_err(|_| ExtractError::BlockTooLarge(transactions.len()))?;
        let coinbase = transaction
            .inputs()
            .iter()
            .any(|input| matches!(input, Input::Coinbase { .. }));

        let mut prevouts = std::collections::HashMap::new();
        for input in transaction.inputs() {
            let Input::PrevOut { outpoint, .. } = input else {
                continue;
            };
            let output = match created.get(outpoint) {
                Some(output) => output.clone(),
                None => previous
                    .previous_output(outpoint)
                    .map_err(|source| ExtractError::Lookup {
                        outpoint: outpoint_label(outpoint),
                        source,
                    })?
                    .ok_or_else(|| ExtractError::MissingPreviousOutput(outpoint_label(outpoint)))?,
            };
            if prevouts
                .insert(
                    *outpoint,
                    Utxo {
                        output,
                        height: zakura_chain::block::Height(height),
                        from_coinbase: false,
                    },
                )
                .is_some()
            {
                return Err(ExtractError::Metadata("duplicate transaction input".into()));
            }
        }
        let transparent_input_count = u32::try_from(prevouts.len())
            .map_err(|_| ExtractError::Metadata("input count exceeds u32".into()))?;
        let fee = if coinbase {
            FeeState::NotApplicable
        } else {
            let balance = transaction
                .value_balance(&prevouts)
                .map_err(|e| ExtractError::Metadata(e.to_string()))?;
            let fee = balance
                .remaining_transaction_value()
                .map_err(|e| ExtractError::Metadata(e.to_string()))?;
            FeeState::Exact(u64::from(fee))
        };
        let metadata = TransactionMetadata {
            fee,
            transparent_input_count,
            has_shielded_components: transaction.has_shielded_data(),
        };
        metadata
            .validate(coinbase)
            .map_err(|e| ExtractError::Metadata(e.to_string()))?;

        if display && (!transaction.inputs().is_empty() || !transaction.outputs().is_empty()) {
            let output = |o: &Output| DisplayOutput {
                value: u64::from(o.value),
                script: o.lock_script.as_raw_bytes().to_vec(),
            };
            // Transaction input order, which the published source is chosen
            // in; the coinbase input spends nothing. No extra lookup: these
            // are the previous outputs the fee was computed from.
            let inputs = transaction
                .inputs()
                .iter()
                .filter_map(|input| match input {
                    Input::PrevOut { outpoint, .. } => Some(outpoint),
                    Input::Coinbase { .. } => None,
                })
                .map(|outpoint| {
                    let spent = &prevouts
                        .get(outpoint)
                        .ok_or_else(|| {
                            ExtractError::MissingPreviousOutput(outpoint_label(outpoint))
                        })?
                        .output;
                    Ok(DisplayInput {
                        prevout_txid: Txid(outpoint.hash.0),
                        prevout_index: outpoint.index,
                        value: u64::from(spent.value),
                        script: spent.lock_script.as_raw_bytes().to_vec(),
                    })
                })
                .collect::<Result<Vec<_>, ExtractError>>()?;
            let record = TransparentDisplayRecordV2x {
                record: TransparentDisplayRecord {
                    txid,
                    coinbase,
                    metadata,
                    outputs: transaction.outputs().iter().map(output).collect(),
                },
                inputs,
            };
            record
                .encode()
                .map_err(|e| ExtractError::Metadata(e.to_string()))?;
            // A source no entry can be derived from stops the ingest here,
            // not at publication.
            record
                .display_record()
                .map_err(|e| ExtractError::Metadata(e.to_string()))?;
            records.push(record);
        }
        // Outputs. Coinbase outputs are included; a leading OP_RETURN is not.
        for (output_index, output) in transaction.outputs().iter().enumerate() {
            let script = ScriptBytes::new(output.lock_script.as_raw_bytes().to_vec());
            if !script.is_filter_element() {
                continue;
            }
            events.push((
                script,
                TransparentEvent::Receive(ReceiveEvent {
                    metadata: Some(metadata),
                    height,
                    txid,
                    transaction_index,
                    output_index: output_index as u32,
                    value: u64::from(output.value),
                    coinbase,
                }),
            ));
        }

        // Inputs. The coinbase input spends nothing and is skipped explicitly.
        for (input_index, input) in transaction.inputs().iter().enumerate() {
            let outpoint = match input {
                Input::Coinbase { .. } => continue,
                Input::PrevOut { outpoint, .. } => outpoint,
            };
            let bytes = prevouts
                .get(outpoint)
                .ok_or_else(|| ExtractError::MissingPreviousOutput(outpoint_label(outpoint)))?
                .output
                .lock_script
                .as_raw_bytes()
                .to_vec();
            // A previous output can legitimately have an empty script; it is
            // then not an element. It cannot legitimately be OP_RETURN, since
            // such an output is unspendable, but the same rule is applied
            // rather than assuming well-formed history.
            let script = ScriptBytes::new(bytes);
            if !script.is_filter_element() {
                continue;
            }
            events.push((
                script,
                TransparentEvent::Spend(SpendEvent {
                    metadata: Some(metadata),
                    height,
                    spending_txid: txid,
                    transaction_index,
                    input_index: input_index as u32,
                    spent_txid: Txid(outpoint.hash.0),
                    spent_output_index: outpoint.index,
                }),
            ));
        }
    }

    Ok(ExtractedBlock {
        events,
        display: records,
    })
}

pub struct ExtractedBlock {
    pub events: Vec<IndexedEvent>,
    /// The display source record of every transaction with a transparent
    /// input or output, in block order; empty unless display was requested.
    pub display: Vec<TransparentDisplayRecordV2x>,
}

pub fn extract_events(
    transactions: &[Arc<Transaction>],
    previous: &mut impl PreviousOutputs,
    height: u32,
) -> Result<Vec<IndexedEvent>, ExtractError> {
    Ok(extract(transactions, previous, height, false)?.events)
}

/// The element set for one block.
///
/// The scripts of [`extract_events`], deduplicated. Deriving it here rather
/// than walking the block a second time is what guarantees the filter covers
/// exactly the scripts the events are keyed by.
///
/// Deduplication is on the raw bytes only: two distinct scripts whose hashes
/// collide are two elements, and collapsing them would drop coverage.
pub fn extract_elements(
    transactions: &[Arc<Transaction>],
    previous: &mut impl PreviousOutputs,
) -> Result<Vec<ScriptBytes>, ExtractError> {
    // The height does not affect which scripts appear, and this projection
    // discards the events anyway, so any value serves.
    let mut elements: Vec<ScriptBytes> = extract_events(transactions, previous, 0)?
        .into_iter()
        .map(|(script, _)| script)
        .collect();
    elements.sort();
    elements.dedup();
    Ok(elements)
}

#[cfg(test)]
pub(crate) mod testing {
    use super::*;

    /// A resolver backed by a fixed map, for tests.
    #[derive(Clone, Default)]
    pub struct MapPreviousOutputs {
        pub scripts: std::collections::HashMap<OutPoint, Vec<u8>>,
        pub values: std::collections::HashMap<OutPoint, u64>,
        pub lookups: usize,
    }

    impl PreviousOutputs for MapPreviousOutputs {
        fn previous_output(
            &mut self,
            outpoint: &OutPoint,
        ) -> Result<Option<Output>, Box<dyn std::error::Error + Send + Sync>> {
            self.lookups += 1;
            Ok(self.scripts.get(outpoint).map(|bytes| Output {
                value: zakura_chain::amount::Amount::try_from(
                    *self.values.get(outpoint).unwrap_or(&100_000_000),
                )
                .unwrap(),
                lock_script: zakura_chain::transparent::Script::new(bytes),
            }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::testing::MapPreviousOutputs;
    use super::*;
    use zakura_chain::amount::Amount;
    use zakura_chain::serialization::ZcashDeserialize;
    use zakura_chain::transparent::{Output, Script};

    fn script(bytes: Vec<u8>) -> Script {
        Script::new(&bytes)
    }

    fn p2pkh(tag: u8) -> Vec<u8> {
        let mut bytes = vec![0x76, 0xa9, 0x14];
        bytes.extend_from_slice(&[tag; 20]);
        bytes.extend_from_slice(&[0x88, 0xac]);
        bytes
    }

    fn output(bytes: Vec<u8>) -> Output {
        Output {
            value: Amount::try_from(1000).unwrap(),
            lock_script: script(bytes),
        }
    }

    fn transaction(inputs: Vec<Input>, outputs: Vec<Output>) -> Arc<Transaction> {
        Arc::new(Transaction::V1 {
            inputs,
            outputs,
            lock_time: zakura_chain::transaction::LockTime::unlocked(),
        })
    }

    /// A coinbase input, built by deserializing its consensus encoding.
    ///
    /// `CoinbaseData`'s constructor is test-gated inside zebra-chain, so the
    /// variant cannot be built directly from here. Going through the real
    /// parser is better anyway: it is the same path production takes, so a
    /// change in how coinbase inputs are recognised would show up in these
    /// tests rather than only in production.
    fn coinbase_input() -> Input {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&[0u8; 32]);
        bytes.extend_from_slice(&0xffff_ffffu32.to_le_bytes());
        // BIP34-style height for 3,428,143: opcode 0x03 then three LE bytes,
        // followed by arbitrary miner data.
        let data = [0x03u8, 0x2f, 0x4f, 0x34, 0xaa, 0xbb];
        bytes.push(data.len() as u8);
        bytes.extend_from_slice(&data);
        bytes.extend_from_slice(&0u32.to_le_bytes());
        let input = Input::zcash_deserialize(bytes.as_slice()).expect("coinbase input");
        assert!(
            matches!(input, Input::Coinbase { .. }),
            "not parsed as coinbase"
        );
        input
    }

    fn prevout_input(outpoint: OutPoint) -> Input {
        Input::PrevOut {
            outpoint,
            unlock_script: script(vec![0x47, 0x30]),
            sequence: 0,
        }
    }

    fn elements_of(
        transactions: &[Arc<Transaction>],
        previous: &mut MapPreviousOutputs,
    ) -> Vec<Vec<u8>> {
        extract_elements(transactions, previous)
            .expect("extract")
            .into_iter()
            .map(|script| script.0)
            .collect()
    }

    fn contains(elements: &[Vec<u8>], script: &[u8]) -> bool {
        elements.iter().any(|element| element == script)
    }

    #[test]
    fn exact_fee_counts_all_inputs_and_is_attributed_to_the_spender() {
        let first = OutPoint {
            hash: zakura_chain::transaction::Hash([10; 32]),
            index: 0,
        };
        let second = OutPoint {
            hash: zakura_chain::transaction::Hash([11; 32]),
            index: 1,
        };
        let mut previous = MapPreviousOutputs::default();
        previous.scripts.insert(first, p2pkh(1));
        previous.scripts.insert(second, vec![]);
        previous.values.insert(first, 1000);
        previous.values.insert(second, 2000);
        let mut recipient = output(p2pkh(2));
        recipient.value = Amount::try_from(2500).unwrap();
        let tx = transaction(
            vec![prevout_input(first), prevout_input(second)],
            vec![recipient],
        );
        let events = extract_events(std::slice::from_ref(&tx), &mut previous, 100).unwrap();
        assert_eq!(
            events.len(),
            2,
            "the empty consumed script does not create an event"
        );
        for (_, event) in events {
            assert_eq!(event.txid(), Txid(tx.hash().0));
            assert_eq!(
                event.metadata(),
                Some(TransactionMetadata {
                    fee: FeeState::Exact(500),
                    transparent_input_count: 2,
                    has_shielded_components: false,
                })
            );
        }
        assert_eq!(
            previous.lookups, 2,
            "unindexed inputs still contribute value and count"
        );
        previous.values.insert(first, 0);
        assert!(
            extract_events(&[tx], &mut previous, 100).is_err(),
            "negative fees abort extraction"
        );
    }

    #[test]
    fn display_entries_take_the_first_address_shaped_source_in_input_order() {
        use transparent_shard::txid::{Address, AddressKind};
        let outpoint = |byte: u8| OutPoint {
            hash: zakura_chain::transaction::Hash([byte; 32]),
            index: 0,
        };
        let p2pk = [&[0x21][..], &[2; 33], &[0xac]].concat();
        let mut previous = MapPreviousOutputs::default();
        for (byte, script) in [(20, p2pk.clone()), (21, p2pkh(5)), (22, p2pkh(6))] {
            previous.scripts.insert(outpoint(byte), script);
            previous.values.insert(outpoint(byte), 1_000);
        }
        let outputs = |n: usize| {
            (0..n)
                .map(|i| {
                    let mut o = output(p2pkh(30 + i as u8));
                    o.value = Amount::try_from(100).unwrap();
                    o
                })
                .collect::<Vec<_>>()
        };
        // A P2PK input first: the source is the next, address-shaped one,
        // and the distinct scripts are named as an omission.
        let mixed = transaction(
            vec![prevout_input(outpoint(20)), prevout_input(outpoint(21))],
            outputs(3),
        );
        let block = extract_block(std::slice::from_ref(&mixed), &mut previous, 9).unwrap();
        let record = block.display[0].display_record().unwrap();
        let entry = record.entry;
        assert_eq!(record.txid, Txid(mixed.hash().0));
        assert_eq!(entry.source, Address::from_script(&p2pkh(5)));
        assert!(entry.multiple_source_scripts && entry.more_than_two_outputs());
        assert_eq!(
            (entry.input_count, entry.output_count, entry.fee),
            (2, 3, 1_700)
        );
        assert_eq!(
            entry.outputs[1].unwrap().address,
            Address::from_script(&p2pkh(31))
        );
        // One script, two outputs: a complete regular entry.
        let mut again = MapPreviousOutputs::default();
        for byte in [23, 24] {
            again.scripts.insert(outpoint(byte), p2pkh(7));
            again.values.insert(outpoint(byte), 1_000);
        }
        let regular = transaction(
            vec![prevout_input(outpoint(23)), prevout_input(outpoint(24))],
            outputs(2),
        );
        let entry = extract_block(&[regular], &mut again, 9).unwrap().display[0]
            .display_record()
            .unwrap()
            .entry;
        assert!(entry.is_complete());
        assert_eq!(entry.source.kind, AddressKind::P2pkh);
        assert_eq!(entry.source, Address::from_script(&p2pkh(7)));
    }

    #[test]
    fn an_empty_block_has_no_elements() {
        let mut previous = MapPreviousOutputs::default();
        assert!(elements_of(&[], &mut previous).is_empty());
    }

    #[test]
    fn a_receive_only_block_yields_its_output_scripts() {
        let transactions = vec![transaction(
            vec![coinbase_input()],
            vec![output(p2pkh(1)), output(p2pkh(2))],
        )];
        let mut previous = MapPreviousOutputs::default();
        let elements = elements_of(&transactions, &mut previous);
        assert_eq!(elements.len(), 2);
        assert!(contains(&elements, &p2pkh(1)));
        // A coinbase receive is included like any other output.
        assert!(contains(&elements, &p2pkh(2)));
        assert_eq!(previous.lookups, 0, "coinbase input must not be looked up");
    }

    #[test]
    fn a_spend_with_no_matching_new_output_still_yields_the_spent_script() {
        let spent = OutPoint {
            hash: zakura_chain::transaction::Hash([9; 32]),
            index: 0,
        };
        let transactions = vec![transaction(
            vec![prevout_input(spent)],
            // Change goes to an unrelated script.
            vec![output(p2pkh(50))],
        )];
        let mut previous = MapPreviousOutputs::default();
        previous.scripts.insert(spent, p2pkh(7));
        let elements = elements_of(&transactions, &mut previous);
        assert!(contains(&elements, &p2pkh(7)), "spent script missing");
        assert!(contains(&elements, &p2pkh(50)));
        assert_eq!(previous.lookups, 1);
    }

    #[test]
    fn a_same_block_spend_resolves_without_a_lookup() {
        let funding = transaction(vec![coinbase_input()], vec![output(p2pkh(11))]);
        let funding_id = funding.hash();
        let spending = transaction(
            vec![prevout_input(OutPoint {
                hash: funding_id,
                index: 0,
            })],
            vec![output(p2pkh(12))],
        );
        let mut previous = MapPreviousOutputs::default();
        let elements = elements_of(&[funding, spending], &mut previous);
        assert_eq!(
            previous.lookups, 0,
            "same-block spend must not need a lookup"
        );
        assert!(contains(&elements, &p2pkh(11)));
        assert!(contains(&elements, &p2pkh(12)));
    }

    #[test]
    fn op_return_outputs_are_excluded_but_a_later_0x6a_byte_is_not() {
        // A script whose payload happens to contain 0x6a is a normal script.
        let mut embedded = vec![0x76, 0xa9, 0x14];
        embedded.extend_from_slice(&[0x6a; 20]);
        embedded.extend_from_slice(&[0x88, 0xac]);

        let transactions = vec![transaction(
            vec![coinbase_input()],
            vec![
                output(vec![0x6a, 0x04, 0xde, 0xad, 0xbe, 0xef]),
                output(embedded.clone()),
                output(p2pkh(3)),
            ],
        )];
        let mut previous = MapPreviousOutputs::default();
        let elements = elements_of(&transactions, &mut previous);
        assert!(!elements
            .iter()
            .any(|element| element.first() == Some(&0x6a)));
        assert!(
            contains(&elements, &embedded),
            "embedded 0x6a wrongly excluded"
        );
        assert!(contains(&elements, &p2pkh(3)));
        assert_eq!(elements.len(), 2);
    }

    #[test]
    fn empty_scripts_are_not_elements() {
        let spent = OutPoint {
            hash: zakura_chain::transaction::Hash([4; 32]),
            index: 1,
        };
        let transactions = vec![transaction(
            vec![prevout_input(spent)],
            vec![output(vec![]), output(p2pkh(6))],
        )];
        let mut previous = MapPreviousOutputs::default();
        // An empty previous-output script is resolvable and simply not an element.
        previous.scripts.insert(spent, vec![]);
        let elements = elements_of(&transactions, &mut previous);
        assert_eq!(elements, vec![p2pkh(6)]);
    }

    #[test]
    fn a_repeated_script_appears_once() {
        let transactions = vec![transaction(
            vec![coinbase_input()],
            vec![output(p2pkh(8)), output(p2pkh(8)), output(p2pkh(8))],
        )];
        let mut previous = MapPreviousOutputs::default();
        assert_eq!(elements_of(&transactions, &mut previous), vec![p2pkh(8)]);
    }

    #[test]
    fn a_nonstandard_script_is_still_an_element() {
        // Not any recognised template, and not address-decodable.
        let odd = vec![0x51, 0x52, 0x53, 0xae, 0xff, 0x00, 0x01];
        let transactions = vec![transaction(
            vec![coinbase_input()],
            vec![output(odd.clone())],
        )];
        let mut previous = MapPreviousOutputs::default();
        assert_eq!(elements_of(&transactions, &mut previous), vec![odd]);
    }

    #[test]
    fn a_missing_previous_output_blocks_publication() {
        let spent = OutPoint {
            hash: zakura_chain::transaction::Hash([5; 32]),
            index: 2,
        };
        let transactions = vec![transaction(
            vec![prevout_input(spent)],
            vec![output(p2pkh(9))],
        )];
        let mut previous = MapPreviousOutputs::default();
        let error = extract_elements(&transactions, &mut previous).unwrap_err();
        assert!(
            matches!(error, ExtractError::MissingPreviousOutput(_)),
            "an unresolved previous output must abort the block, not be treated as empty"
        );
    }

    #[test]
    fn the_element_set_is_order_independent() {
        let a = transaction(vec![coinbase_input()], vec![output(p2pkh(1))]);
        let b = transaction(vec![coinbase_input()], vec![output(p2pkh(2))]);
        let mut previous = MapPreviousOutputs::default();
        let forward = elements_of(&[a.clone(), b.clone()], &mut previous);
        let backward = elements_of(&[b, a], &mut previous);
        assert_eq!(forward, backward);
    }

    // --- Events -----------------------------------------------------------

    fn events_of(
        transactions: &[Arc<Transaction>],
        previous: &mut MapPreviousOutputs,
    ) -> Vec<IndexedEvent> {
        extract_events(transactions, previous, 4_242).expect("events")
    }

    /// The invariant the whole arrangement exists to guarantee. If the filter
    /// ever covered a different set of scripts than the events are keyed by, a
    /// wallet would be told it had no activity in a shard that holds its money,
    /// and it would never look again.
    #[test]
    fn the_element_set_is_exactly_the_scripts_the_events_are_keyed_by() {
        let spent = OutPoint {
            hash: zakura_chain::transaction::Hash([9; 32]),
            index: 3,
        };
        let transactions = vec![
            transaction(vec![coinbase_input()], vec![output(p2pkh(1))]),
            transaction(
                vec![prevout_input(spent)],
                vec![output(p2pkh(2)), output(vec![0x6a, 0xff]), output(vec![])],
            ),
        ];
        let mut previous = MapPreviousOutputs::default();
        previous.scripts.insert(spent, p2pkh(7));

        let events = events_of(&transactions, &mut previous.clone());
        let mut from_events: Vec<Vec<u8>> = events
            .iter()
            .map(|(script, _)| script.as_slice().to_vec())
            .collect();
        from_events.sort();
        from_events.dedup();

        let mut elements = elements_of(&transactions, &mut previous);
        elements.sort();

        assert_eq!(from_events, elements);
        // And the excluded scripts really were present in the block, so the
        // test would notice if the exclusion rule silently stopped applying.
        assert!(!contains(&elements, &[0x6a, 0xff]));
    }

    /// A spend must be filed under the script of the output it consumes. Filing
    /// it under the spending transaction's own outputs would leave the paying
    /// wallet unable to discover that its money had moved.
    #[test]
    fn a_spend_is_indexed_under_the_consumed_outputs_script() {
        let spent = OutPoint {
            hash: zakura_chain::transaction::Hash([9; 32]),
            index: 3,
        };
        let transactions = vec![transaction(
            vec![prevout_input(spent)],
            vec![output(p2pkh(50))],
        )];
        let mut previous = MapPreviousOutputs::default();
        previous.scripts.insert(spent, p2pkh(7));

        let events = events_of(&transactions, &mut previous);
        let spends: Vec<&IndexedEvent> = events
            .iter()
            .filter(|(_, event)| matches!(event, TransparentEvent::Spend(_)))
            .collect();
        assert_eq!(spends.len(), 1);
        let (script, event) = spends[0];
        assert_eq!(script.as_slice(), p2pkh(7).as_slice());
        match event {
            TransparentEvent::Spend(spend) => {
                assert_eq!(spend.spent_txid, Txid([9; 32]));
                assert_eq!(spend.spent_output_index, 3);
                assert_eq!(spend.height, 4_242);
                assert_eq!(spend.input_index, 0);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn a_receive_carries_its_value_outpoint_and_coinbase_status() {
        let funding = transaction(vec![coinbase_input()], vec![output(p2pkh(1))]);
        let transactions = vec![
            funding.clone(),
            transaction(
                vec![prevout_input(OutPoint {
                    hash: funding.hash(),
                    index: 0,
                })],
                vec![output(p2pkh(2))],
            ),
        ];
        let mut previous = MapPreviousOutputs::default();
        let events = events_of(&transactions, &mut previous);

        let coinbase = match events[0].1 {
            TransparentEvent::Receive(event) => event,
            _ => unreachable!("first event is the coinbase output"),
        };
        assert!(coinbase.coinbase);
        assert_eq!(coinbase.transaction_index, 0);
        assert_eq!(coinbase.output_index, 0);
        // The fixture mints every output at 1000 zatoshi.
        assert_eq!(coinbase.value, 1000);
        assert_eq!(coinbase.height, 4_242);

        let ordinary = match events[1].1 {
            TransparentEvent::Receive(event) => event,
            _ => unreachable!("second event is the ordinary output"),
        };
        assert!(
            !ordinary.coinbase,
            "only a coinbase transaction is coinbase"
        );
        assert_eq!(ordinary.transaction_index, 1);
    }

    /// Every event a wallet decodes must survive the wire encoding unchanged;
    /// otherwise the indexer and the wallet disagree about what was indexed.
    #[test]
    fn extracted_events_round_trip_through_their_encoding() {
        let spent = OutPoint {
            hash: zakura_chain::transaction::Hash([9; 32]),
            index: 3,
        };
        let transactions = vec![
            transaction(vec![coinbase_input()], vec![output(p2pkh(1))]),
            transaction(vec![prevout_input(spent)], vec![output(p2pkh(2))]),
        ];
        let mut previous = MapPreviousOutputs::default();
        previous.scripts.insert(spent, p2pkh(7));
        for (_, event) in events_of(&transactions, &mut previous) {
            assert_eq!(TransparentEvent::from_bytes(&event.to_bytes()), Ok(event));
        }
    }

    /// Every display source lists its inputs in input order, from the
    /// previous outputs the fee already needed: no extra lookup, and a
    /// same-block spend needs none at all. The sidecar round-trips.
    #[test]
    fn display_sources_list_inputs_without_extra_lookups() {
        let funding = transaction(vec![coinbase_input()], vec![output(p2pkh(11))]);
        let outside = OutPoint {
            hash: zakura_chain::transaction::Hash([9; 32]),
            index: 3,
        };
        let same_block = OutPoint {
            hash: funding.hash(),
            index: 0,
        };
        let mut big = output(vec![0x51; 10_001]);
        big.value = Amount::try_from(500).unwrap();
        let transactions = vec![
            funding.clone(),
            transaction(
                vec![prevout_input(outside), prevout_input(same_block)],
                vec![output(p2pkh(12)), big],
            ),
        ];
        let mut previous = MapPreviousOutputs::default();
        previous.scripts.insert(outside, vec![0x52; 12_000]);
        previous.values.insert(outside, 600);
        let listed = extract_block(&transactions, &mut previous, 7).unwrap();
        assert_eq!(previous.lookups, 1, "only the outside prevout is looked up");
        assert_eq!(listed.display.len(), 2);
        // History needs no display source, so builds none, and its events
        // are the same.
        let mut again = previous.clone();
        let history = extract(&transactions, &mut again, 7, false).unwrap();
        assert!(history.display.is_empty());
        assert_eq!(history.events, listed.events);
        assert!(
            listed.display[0].inputs.is_empty(),
            "coinbase lists no inputs"
        );
        let inputs = &listed.display[1].inputs;
        assert_eq!(inputs.len(), 2);
        assert_eq!(
            (
                inputs[0].prevout_txid,
                inputs[0].prevout_index,
                inputs[0].value
            ),
            (Txid([9; 32]), 3, 600)
        );
        assert_eq!(inputs[0].script, vec![0x52; 12_000]);
        assert_eq!(inputs[1].prevout_txid, Txid(funding.hash().0));
        assert_eq!(
            (inputs[1].value, inputs[1].script.clone()),
            (1000, p2pkh(11))
        );

        let journal = tempfile::tempdir().unwrap();
        let hash = transparent_filter::BlockHash::from_internal_bytes([5; 32]);
        let mut store = crate::events::EventStore::open(
            journal.path(),
            transparent_filter::MAINNET_GENESIS_DISPLAY,
            7,
        )
        .unwrap();
        store
            .append_block_with_display(7, hash, &listed.events, &listed.display)
            .unwrap();
        store.commit().unwrap();
        let name = format!("{}.bin", hash.to_display_hex());
        assert!(journal.path().join("display-v2x").join(&name).exists());
        assert!(!journal.path().join("display-v1").exists());
        assert_eq!(
            crate::display_journal::read_sources(journal.path(), hash).unwrap(),
            listed.display
        );
        let entries: Vec<_> = listed
            .display
            .iter()
            .map(|source| source.display_record().unwrap())
            .collect();
        assert_eq!(store.display_at(7).unwrap(), entries);
    }

    #[test]
    fn an_unresolvable_previous_output_blocks_event_extraction_too() {
        let spent = OutPoint {
            hash: zakura_chain::transaction::Hash([9; 32]),
            index: 0,
        };
        let transactions = vec![transaction(vec![prevout_input(spent)], vec![])];
        let mut previous = MapPreviousOutputs::default();
        assert!(matches!(
            extract_events(&transactions, &mut previous, 1),
            Err(ExtractError::MissingPreviousOutput(_))
        ));
    }
}
