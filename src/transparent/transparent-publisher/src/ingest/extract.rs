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
//!
//! Reading the block and resolving every previous output is
//! `zakura_server_chain::transparent`'s, which returns each transaction's raw
//! output and spent-output scripts with its exact fee, and fails the block on an
//! output it cannot resolve. This module applies the rules above to what it read.

use transparent_events::{
    FeeState, ReceiveEvent, SpendEvent, TransactionMetadata, TransparentEvent, Txid,
};
use transparent_filter::ScriptBytes;
use transparent_shard::txid::DisplayOutput;
use transparent_shard::txid_v1::TransparentDisplayRecord;
use transparent_shard::txid_v2x::{DisplayInput, TransparentDisplayRecordV2x};
use zakura_server_chain::transparent::Fee;
pub use zakura_server_chain::transparent::{
    read_transaction, BoxError, ExtractError, FetchedBlock, Output, PreviousOutputs,
    TransparentTransaction,
};

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
/// The block hash is not part of an event. A wallet resolves height to hash
/// against its own accepted chain, which is the only source that can safely
/// answer whether an event sits on a branch it accepts.
pub fn extract_block(
    block: &FetchedBlock,
    previous: &mut impl PreviousOutputs,
    height: u32,
) -> Result<ExtractedBlock, ExtractError> {
    extract(block, previous, height, true)
}

/// [`extract_block`], building display source records only when `display`
/// is set. History and filter ingest never need them, so a record the display
/// codec refuses (a v2x record is bounded; the block's events are not) can
/// only stop the display ingest, never the history.
pub fn extract(
    block: &FetchedBlock,
    previous: &mut impl PreviousOutputs,
    height: u32,
    display: bool,
) -> Result<ExtractedBlock, ExtractError> {
    index(&block.transparent(height, previous)?, height, display)
}

/// The events, and when `display` is set the display source records, of a
/// block's transparent transactions as read at `height`.
pub fn index(
    transactions: &[TransparentTransaction],
    height: u32,
    display: bool,
) -> Result<ExtractedBlock, ExtractError> {
    let mut events: Vec<IndexedEvent> = Vec::new();
    let mut records = Vec::new();

    for transaction in transactions {
        let txid = Txid(transaction.txid);
        let transaction_index = transaction.transaction_index;
        let coinbase = transaction.coinbase;
        let fee = match transaction.fee {
            Fee::NotApplicable => FeeState::NotApplicable,
            Fee::Exact(fee) => FeeState::Exact(fee),
        };
        let metadata = TransactionMetadata {
            fee,
            transparent_input_count: transaction.transparent_input_count,
            has_shielded_components: transaction.has_shielded_components,
        };
        metadata
            .validate(coinbase)
            .map_err(|e| ExtractError::Metadata(e.to_string()))?;

        // A coinbase transaction always has its coinbase input; any other input
        // spends an earlier output and is listed.
        let has_transparent_parts =
            coinbase || !transaction.inputs.is_empty() || !transaction.outputs.is_empty();
        if display && has_transparent_parts {
            // Transaction input order, which the published source is chosen
            // in; the coinbase input spends nothing. No extra lookup: these
            // are the previous outputs the fee was computed from.
            let inputs = transaction
                .inputs
                .iter()
                .map(|input| DisplayInput {
                    prevout_txid: Txid(input.prevout_txid),
                    prevout_index: input.prevout_index,
                    value: input.spent.value,
                    script: input.spent.script.clone(),
                })
                .collect();
            let record = TransparentDisplayRecordV2x {
                record: TransparentDisplayRecord {
                    txid,
                    coinbase,
                    metadata,
                    outputs: transaction
                        .outputs
                        .iter()
                        .map(|o| DisplayOutput {
                            value: o.value,
                            script: o.script.clone(),
                        })
                        .collect(),
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
        for (output_index, output) in transaction.outputs.iter().enumerate() {
            let script = ScriptBytes::new(output.script.clone());
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
                    value: output.value,
                    coinbase,
                }),
            ));
        }

        // Inputs. The coinbase input spends nothing and is not listed.
        for input in &transaction.inputs {
            // A previous output can legitimately have an empty script; it is
            // then not an element. It cannot legitimately be OP_RETURN, since
            // such an output is unspendable, but the same rule is applied
            // rather than assuming well-formed history.
            let script = ScriptBytes::new(input.spent.script.clone());
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
                    input_index: input.input_index,
                    spent_txid: Txid(input.prevout_txid),
                    spent_output_index: input.prevout_index,
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
    block: &FetchedBlock,
    previous: &mut impl PreviousOutputs,
    height: u32,
) -> Result<Vec<IndexedEvent>, ExtractError> {
    Ok(extract(block, previous, height, false)?.events)
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
    block: &FetchedBlock,
    previous: &mut impl PreviousOutputs,
) -> Result<Vec<ScriptBytes>, ExtractError> {
    // The height does not affect which scripts appear, and this projection
    // discards the events anyway, so any value serves.
    let mut elements: Vec<ScriptBytes> = extract_events(block, previous, 0)?
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
    use std::collections::HashMap;
    use zakura_server_chain::transparent::{BoxError, Output};

    /// A resolver backed by a fixed map, for tests. Outpoints are a txid in
    /// protocol byte order and an output index.
    #[derive(Clone, Default)]
    pub struct MapPreviousOutputs {
        pub scripts: HashMap<([u8; 32], u32), Vec<u8>>,
        pub values: HashMap<([u8; 32], u32), u64>,
        pub lookups: usize,
    }

    impl PreviousOutputs for MapPreviousOutputs {
        fn previous_output(
            &mut self,
            txid: [u8; 32],
            index: u32,
        ) -> Result<Option<Output>, BoxError> {
            self.lookups += 1;
            Ok(self.scripts.get(&(txid, index)).map(|bytes| Output {
                value: *self.values.get(&(txid, index)).unwrap_or(&100_000_000),
                script: bytes.clone(),
            }))
        }
    }

    /// `transactions` as one raw block, parsed as the node's would be.
    pub fn block_of(transactions: &[zakura_server_chain::testing::Tx]) -> FetchedBlock {
        let (raw, _) = zakura_server_chain::testing::block(transactions, 0);
        FetchedBlock::from_bytes(&hex::decode(raw).unwrap()).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::testing::{block_of, MapPreviousOutputs};
    use super::*;
    use zakura_server_chain::testing::{self as chain, Spend, Tx};

    type Outpoint = ([u8; 32], u32);

    fn p2pkh(tag: u8) -> Vec<u8> {
        let mut bytes = vec![0x76, 0xa9, 0x14];
        bytes.extend_from_slice(&[tag; 20]);
        bytes.extend_from_slice(&[0x88, 0xac]);
        bytes
    }

    /// An output paying the fixture's 1000 zatoshis to `bytes`.
    fn output(bytes: Vec<u8>) -> (i64, Vec<u8>) {
        (1000, bytes)
    }

    fn transaction(inputs: Vec<Spend>, outputs: Vec<(i64, Vec<u8>)>) -> Tx {
        let outputs: Vec<(i64, &[u8])> = outputs
            .iter()
            .map(|(value, script)| (*value, script.as_slice()))
            .collect();
        chain::transfer(inputs, &outputs)
    }

    /// A coinbase transaction, its input encoding a BIP34 height.
    fn coinbase(outputs: Vec<(i64, Vec<u8>)>) -> Tx {
        let outputs: Vec<(i64, &[u8])> = outputs
            .iter()
            .map(|(value, script)| (*value, script.as_slice()))
            .collect();
        chain::coinbase(3_428_143, &outputs)
    }

    fn prevout_input(outpoint: Outpoint) -> Spend {
        chain::spend_outpoint(outpoint.0, outpoint.1)
    }

    fn elements_of(transactions: &[Tx], previous: &mut MapPreviousOutputs) -> Vec<Vec<u8>> {
        extract_elements(&block_of(transactions), previous)
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
        let first = ([10; 32], 0);
        let second = ([11; 32], 1);
        let mut previous = MapPreviousOutputs::default();
        previous.scripts.insert(first, p2pkh(1));
        previous.scripts.insert(second, vec![]);
        previous.values.insert(first, 1000);
        previous.values.insert(second, 2000);
        let tx = transaction(
            vec![prevout_input(first), prevout_input(second)],
            vec![(2500, p2pkh(2))],
        );
        let block = block_of(std::slice::from_ref(&tx));
        let events = extract_events(&block, &mut previous, 100).unwrap();
        assert_eq!(
            events.len(),
            2,
            "the empty consumed script does not create an event"
        );
        for (_, event) in events {
            assert_eq!(event.txid(), Txid(tx.txid()));
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
            extract_events(&block, &mut previous, 100).is_err(),
            "negative fees abort extraction"
        );
    }

    #[test]
    fn display_entries_take_the_first_address_shaped_source_in_input_order() {
        use transparent_shard::txid::{Address, AddressKind};
        let outpoint = |byte: u8| ([byte; 32], 0);
        let p2pk = [&[0x21][..], &[2; 33], &[0xac]].concat();
        let mut previous = MapPreviousOutputs::default();
        for (byte, script) in [(20, p2pk.clone()), (21, p2pkh(5)), (22, p2pkh(6))] {
            previous.scripts.insert(outpoint(byte), script);
            previous.values.insert(outpoint(byte), 1_000);
        }
        let outputs = |n: usize| {
            (0..n)
                .map(|i| (100, p2pkh(30 + i as u8)))
                .collect::<Vec<_>>()
        };
        // A P2PK input first: the source is the next, address-shaped one,
        // and the distinct scripts are named as an omission.
        let mixed = transaction(
            vec![prevout_input(outpoint(20)), prevout_input(outpoint(21))],
            outputs(3),
        );
        let block =
            extract_block(&block_of(std::slice::from_ref(&mixed)), &mut previous, 9).unwrap();
        let record = block.display[0].display_record().unwrap();
        let entry = record.entry;
        assert_eq!(record.txid, Txid(mixed.txid()));
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
        let entry = extract_block(&block_of(&[regular]), &mut again, 9)
            .unwrap()
            .display[0]
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
        let transactions = vec![coinbase(vec![output(p2pkh(1)), output(p2pkh(2))])];
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
        let spent = ([9; 32], 0);
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
        let funding = coinbase(vec![output(p2pkh(11))]);
        let spending = transaction(vec![chain::spend(&funding, 0)], vec![output(p2pkh(12))]);
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

        let transactions = vec![coinbase(vec![
            output(vec![0x6a, 0x04, 0xde, 0xad, 0xbe, 0xef]),
            output(embedded.clone()),
            output(p2pkh(3)),
        ])];
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
        let spent = ([4; 32], 1);
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
        let transactions = vec![coinbase(vec![
            output(p2pkh(8)),
            output(p2pkh(8)),
            output(p2pkh(8)),
        ])];
        let mut previous = MapPreviousOutputs::default();
        assert_eq!(elements_of(&transactions, &mut previous), vec![p2pkh(8)]);
    }

    #[test]
    fn a_nonstandard_script_is_still_an_element() {
        // Not any recognised template, and not address-decodable.
        let odd = vec![0x51, 0x52, 0x53, 0xae, 0xff, 0x00, 0x01];
        let transactions = vec![coinbase(vec![output(odd.clone())])];
        let mut previous = MapPreviousOutputs::default();
        assert_eq!(elements_of(&transactions, &mut previous), vec![odd]);
    }

    #[test]
    fn a_missing_previous_output_blocks_publication() {
        let spent = ([5; 32], 2);
        let transactions = vec![transaction(
            vec![prevout_input(spent)],
            vec![output(p2pkh(9))],
        )];
        let mut previous = MapPreviousOutputs::default();
        let error = extract_elements(&block_of(&transactions), &mut previous).unwrap_err();
        assert!(
            matches!(error, ExtractError::MissingPreviousOutput(_)),
            "an unresolved previous output must abort the block, not be treated as empty"
        );
    }

    #[test]
    fn the_element_set_is_order_independent() {
        let a = coinbase(vec![output(p2pkh(1))]);
        let b = coinbase(vec![output(p2pkh(2))]);
        let mut previous = MapPreviousOutputs::default();
        let forward = elements_of(&[a.clone(), b.clone()], &mut previous);
        let backward = elements_of(&[b, a], &mut previous);
        assert_eq!(forward, backward);
    }

    // --- Events -----------------------------------------------------------

    fn events_of(transactions: &[Tx], previous: &mut MapPreviousOutputs) -> Vec<IndexedEvent> {
        extract_events(&block_of(transactions), previous, 4_242).expect("events")
    }

    /// The invariant the whole arrangement exists to guarantee. If the filter
    /// ever covered a different set of scripts than the events are keyed by, a
    /// wallet would be told it had no activity in a shard that holds its money,
    /// and it would never look again.
    #[test]
    fn the_element_set_is_exactly_the_scripts_the_events_are_keyed_by() {
        let spent = ([9; 32], 3);
        let transactions = vec![
            coinbase(vec![output(p2pkh(1))]),
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
        let spent = ([9; 32], 3);
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
        let funding = coinbase(vec![output(p2pkh(1))]);
        let transactions = vec![
            funding.clone(),
            transaction(vec![chain::spend(&funding, 0)], vec![output(p2pkh(2))]),
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
        let spent = ([9; 32], 3);
        let transactions = vec![
            coinbase(vec![output(p2pkh(1))]),
            transaction(vec![prevout_input(spent)], vec![output(p2pkh(2))]),
        ];
        let mut previous = MapPreviousOutputs::default();
        previous.scripts.insert(spent, p2pkh(7));
        for (_, event) in events_of(&transactions, &mut previous) {
            assert_eq!(
                TransparentEvent::from_bytes(&event.to_bytes().unwrap()),
                Ok(event)
            );
        }
    }

    /// Every display source lists its inputs in input order, from the
    /// previous outputs the fee already needed: no extra lookup, and a
    /// same-block spend needs none at all. The sidecar round-trips.
    #[test]
    fn display_sources_list_inputs_without_extra_lookups() {
        let funding = coinbase(vec![output(p2pkh(11))]);
        let outside = ([9; 32], 3);
        let transactions = vec![
            funding.clone(),
            transaction(
                vec![prevout_input(outside), chain::spend(&funding, 0)],
                vec![output(p2pkh(12)), (500, vec![0x51; 10_001])],
            ),
        ];
        let mut previous = MapPreviousOutputs::default();
        previous.scripts.insert(outside, vec![0x52; 12_000]);
        previous.values.insert(outside, 600);
        let block = block_of(&transactions);
        let listed = extract_block(&block, &mut previous, 7).unwrap();
        assert_eq!(previous.lookups, 1, "only the outside prevout is looked up");
        assert_eq!(listed.display.len(), 2);
        // History needs no display source, so builds none, and its events
        // are the same.
        let mut again = previous.clone();
        let history = extract(&block, &mut again, 7, false).unwrap();
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
        assert_eq!(inputs[1].prevout_txid, Txid(funding.txid()));
        assert_eq!(
            (inputs[1].value, inputs[1].script.clone()),
            (1000, p2pkh(11))
        );

        let journal = tempfile::tempdir().unwrap();
        let hash = transparent_filter::BlockHash::from_internal_bytes([5; 32]);
        let mut store = transparent_shared::journal::events::EventStore::open(
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
            transparent_shared::journal::display_journal::read_sources(journal.path(), hash)
                .unwrap(),
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
        let spent = ([9; 32], 0);
        let transactions = vec![transaction(vec![prevout_input(spent)], vec![])];
        let mut previous = MapPreviousOutputs::default();
        assert!(matches!(
            extract_events(&block_of(&transactions), &mut previous, 1),
            Err(ExtractError::MissingPreviousOutput(_))
        ));
    }
}
