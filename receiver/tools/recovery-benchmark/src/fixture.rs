use enhance_pir::{EnhanceRecord, EnhanceRecordParts, EnhanceTransactionMetadata};
use incrementalmerkletree::frontier::Frontier;
use orchard::tree::MerkleHashOrchard;
use orchard::{
    keys::{FullViewingKey, OutgoingViewingKey, Scope, SpendingKey},
    note_encryption::{CompactAction, IronwoodDomain, IronwoodNoteEncryption},
    value::{NoteValue, ValueCommitTrapdoor, ValueCommitment},
};
use prost::Message;
use receiver_directory::{
    Payment, Receiver, Record,
    snapshot::{Manifest, PROFILE, Snapshot},
    witness::WitnessSnapshot,
};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use zakura_swap_receiving::{KeyId, Purpose};
use zcash_client_backend::{
    data_api::{
        chain::{BlockSource, ChainState},
        testing::{AddressType, FakeCompactOutput, IronwoodFvk, TestBuilder, TestCache, TestState},
    },
    proto::compact_formats::CompactBlock,
};
use zcash_client_sqlite::testing::{
    BlockCache,
    db::{TestDb, TestDbFactory},
};
use zcash_keys::address::{Address, UnifiedAddress};
use zcash_note_encryption::try_compact_note_decryption;
use zcash_primitives::block::BlockHash;
use zcash_protocol::{consensus::BlockHeight, local_consensus::LocalNetwork, value::Zatoshis};

pub const START: u32 = 3428143;
pub const KEYS: u32 = 25;
pub const BLOCKS: usize = 256;
pub const BATCH: usize = 1000;
pub type Wallet = TestState<BlockCache, TestDb, LocalNetwork>;
pub fn wallet(blocks: &[CompactBlock]) -> Wallet {
    let mut cache = BlockCache::new();
    for b in blocks {
        cache.insert(b);
    }
    let h = Some(BlockHeight::from_u32(START));
    let network = LocalNetwork {
        overwinter: h,
        sapling: h,
        blossom: h,
        heartwood: h,
        canopy: h,
        nu5: h,
        nu6: h,
        nu6_1: h,
        nu6_2: h,
        nu6_3: h,
        ..TestBuilder::<(), ()>::DEFAULT_NETWORK
    };
    TestBuilder::new()
        .with_network(network)
        .with_block_cache(cache)
        .with_data_store_factory(TestDbFactory::file_backed())
        .with_account_from_sapling_activation(BlockHash([0; 32]))
        .build()
}
pub struct Fixture {
    pub blocks: Vec<CompactBlock>,
    pub batch_states: Vec<ChainState>,
    pub parent: FullViewingKey,
    pub keys: Vec<FullViewingKey>,
    pub records: Vec<Record>,
    pub snapshot: Snapshot,
    pub proof: Vec<u8>,
    pub enhance: serde_json::Value,
    pub hash: String,
    pub expected: Vec<serde_json::Value>,
}
pub fn generate(shape: Option<&serde_json::Value>, cache: &std::path::Path) -> Fixture {
    let mut st = wallet(&[]);
    let parent = FullViewingKey::from(st.test_account().unwrap().usk().orchard());
    let keys: Vec<_> = (0..KEYS)
        .map(|i| {
            KeyId::new(Purpose::Receive, u64::from(i))
                .derive(&parent)
                .unwrap()
        })
        .collect();
    let noise = FullViewingKey::from(&SpendingKey::from_bytes([7; 32]).unwrap());
    let counts: Vec<usize> = shape.map_or_else(
        || vec![16; BLOCKS],
        |s| {
            assert_eq!(s["start_height"].as_u64().unwrap(), u64::from(START));
            s["action_counts"]
                .as_array()
                .unwrap()
                .iter()
                .map(|n| usize::try_from(n.as_u64().unwrap()).unwrap())
                .collect()
        },
    );
    let indexed: BTreeSet<u64> = shape.map_or_else(BTreeSet::new, |s| {
        s["indexed_positions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n.as_u64().unwrap())
            .collect()
    });
    let nonempty: Vec<_> = counts
        .iter()
        .enumerate()
        .filter_map(|(i, n)| (*n > 0).then_some(i))
        .collect();
    assert!(nonempty.len() >= 5);
    let owners: Vec<_> = [0usize, 3, 7, 12, 20]
        .into_iter()
        .enumerate()
        .map(|(i, key)| (nonempty[i * nonempty.len() / 5], key))
        .collect();
    let blocks = if cache.exists() {
        let bytes = std::fs::read(cache).unwrap();
        let mut input = bytes.as_slice();
        let mut blocks = Vec::new();
        while !input.is_empty() {
            blocks.push(CompactBlock::decode_length_delimited(&mut input).unwrap());
        }
        assert_eq!(blocks.len(), counts.len() + 1);
        blocks
    } else {
        let mut spent = None;
        for (block, count) in counts.iter().copied().enumerate() {
            if block % 5000 == 0 {
                eprintln!("fixture blocks={block}/{}", counts.len());
            }
            if count == 0 {
                st.generate_empty_block();
                continue;
            }
            let owner = owners.iter().find(|(b, _)| *b == block).map(|(_, k)| *k);
            let fk = IronwoodFvk(owner.map_or_else(|| noise.clone(), |i| keys[i].clone()));
            let noise_fk = IronwoodFvk(noise.clone());
            let mut outputs = vec![FakeCompactOutput::new(
                &fk,
                AddressType::DefaultExternal,
                Zatoshis::const_from_u64(100_000),
            )];
            for _ in 1..count {
                outputs.push(FakeCompactOutput::new(
                    &noise_fk,
                    AddressType::DefaultExternal,
                    Zatoshis::const_from_u64(10_000),
                ));
            }
            let (_, _, nfs) = st.generate_next_block_multi(&outputs);
            if owner == Some(0) {
                spent = Some(nfs[0]);
            }
        }
        let recipient = Address::Unified(
            UnifiedAddress::from_receivers(
                Some(noise.address_at(0u32, Scope::External)),
                None,
                None,
            )
            .unwrap(),
        );
        st.generate_next_block_spending(
            &IronwoodFvk(keys[0].clone()),
            (spent.unwrap(), Zatoshis::const_from_u64(100_000)),
            recipient,
            Zatoshis::const_from_u64(100_000),
        );
        let mut blocks = Vec::new();
        st.cache()
            .with_blocks::<_, zcash_client_sqlite::error::SqliteClientError>(None, None, |b| {
                blocks.push(b);
                Ok(())
            })
            .unwrap();
        let mut bytes = Vec::new();
        for block in &blocks {
            block.encode_length_delimited(&mut bytes).unwrap();
        }
        std::fs::write(cache, bytes).unwrap();
        blocks
    };
    let mut batch_states = Vec::new();
    let mut ironwood_tree = Frontier::<MerkleHashOrchard, 32>::empty();
    let mut orchard_tree = Frontier::<MerkleHashOrchard, 32>::empty();
    let mut commitments = Vec::new();
    let mut records = Vec::new();
    let mut expected = Vec::new();
    let mut enhancement = Vec::new();
    let prepared: Vec<_> = keys
        .iter()
        .map(|k| k.to_ivk(Scope::External).prepare())
        .collect();
    let noise_ivk = noise.to_ivk(Scope::External).prepare();
    for (block_index, block) in blocks.iter().enumerate() {
        assert_eq!(u32::from(block.height()), START + block_index as u32);
        let previous_hash = if block_index == 0 {
            BlockHash([0; 32])
        } else {
            blocks[block_index - 1].hash()
        };
        assert_eq!(block.prev_hash, previous_hash.0);
        if block_index % BATCH == 0 {
            batch_states.push(ChainState::new(
                block.height() - 1,
                previous_hash,
                Frontier::empty(),
                orchard_tree.clone(),
                ironwood_tree.clone(),
            ));
        }
        let owner = owners
            .iter()
            .find(|(b, _)| *b == block_index)
            .map(|(_, k)| *k);
        let mut slots = Vec::new();
        let mut slot_count = 0;
        let mut block_action_index = 0;
        for tx in &block.vtx {
            assert!(tx.outputs.is_empty());
            for action in &tx.actions {
                assert!(orchard_tree.append(
                    MerkleHashOrchard::from_bytes(&action.cmx().unwrap().to_bytes()).unwrap()
                ));
            }
            for (action_index, compact) in tx.ironwood_actions.iter().enumerate() {
                let position = commitments.len() as u64;
                let action = CompactAction::try_from(compact).unwrap();
                let cmx = compact.cmx().unwrap().to_bytes();
                assert!(ironwood_tree.append(MerkleHashOrchard::from_bytes(&cmx).unwrap()));
                commitments.push(cmx);
                let known = if block_action_index == 0 { owner } else { None };
                block_action_index += 1;
                // The fixture knows its recipients. Avoid an untimed 25-key scan of noise.
                let selected = known.map(|i| (i, &prepared[i], &keys[i]));
                let selected = selected.or_else(|| {
                    indexed
                        .contains(&position)
                        .then_some((usize::MAX, &noise_ivk, &noise))
                });
                if let Some((i, ivk, fvk)) = selected {
                    if let Some((note, address)) = try_compact_note_decryption(
                        &IronwoodDomain::for_compact_action(&action),
                        ivk,
                        &action,
                    ) {
                        assert_eq!(address, fvk.address_at(0u32, Scope::External));
                        if known.is_some() {
                            expected.push(serde_json::json!({
                                "txid": hex::encode(tx.txid().as_ref()),
                                "action_index": action_index,
                                "position": position,
                                "value": note.value().inner(),
                                "nullifier": hex::encode(note.nullifier(&keys[i]).to_bytes()),
                                "spent": i == 0,
                            }));
                            let cv = ValueCommitment::derive(
                                note.value() - NoteValue::ZERO,
                                ValueCommitTrapdoor::from_bytes([0; 32]).unwrap(),
                            );
                            let cmx =
                                orchard::note::ExtractedNoteCommitment::from(note.commitment());
                            let enc = IronwoodNoteEncryption::new(
                                Some(OutgoingViewingKey::from([0; 32])),
                                note,
                                [0; 512],
                            );
                            let full = enc.encrypt_note_plaintext();
                            assert_eq!(&full[..52], compact.ciphertext.as_slice());
                            let record = EnhanceRecord::from_parts(EnhanceRecordParts {
                                enc_ciphertext_suffix: full[52..].try_into().unwrap(),
                                cv_net: cv.to_bytes(),
                                out_ciphertext: enc.encrypt_outgoing_plaintext(
                                    &cv,
                                    &cmx,
                                    &mut rand::thread_rng(),
                                ),
                                has_transparent_inputs: false,
                                has_transparent_outputs: false,
                                metadata: EnhanceTransactionMetadata::new(
                                    u32::from(block.height()),
                                    None,
                                )
                                .unwrap(),
                            })
                            .as_bytes()
                            .to_vec();
                            slots.push(serde_json::json!({"offset": slot_count, "hex": hex::encode(record)}));
                        }
                        records.push(Record {
                            receiver: Receiver::from_bytes(address.to_raw_address_bytes()).unwrap(),
                            page: 0,
                            total: 1,
                            payment: Payment {
                                height: block.height().into(),
                                block_hash: block.hash().0,
                                txid: *tx.txid().as_ref(),
                                tx_index: tx.index as u32,
                                action_index: action_index as u32,
                                position,
                                action_nullifier: compact.nullifier.as_slice().try_into().unwrap(),
                                cmx: compact.cmx.as_slice().try_into().unwrap(),
                                ephemeral_key: compact.ephemeral_key.as_slice().try_into().unwrap(),
                                ciphertext_prefix: compact
                                    .ciphertext
                                    .as_slice()
                                    .try_into()
                                    .unwrap(),
                            },
                        });
                    }
                }
                slot_count += 1;
            }
        }
        let mut hash = block.hash().0;
        hash.reverse();
        enhancement.push(serde_json::json!({"height":u32::from(block.height()),"hash":hex::encode(hash),"count":slot_count,"records":slots}));
    }
    assert_eq!(expected.len(), 5);
    // Noise payments share one receiver. Give them valid contiguous directory pages.
    let noise_receiver = noise
        .address_at(0u32, Scope::External)
        .to_raw_address_bytes();
    let noise_count = records
        .iter()
        .filter(|r| *r.receiver.as_bytes() == noise_receiver)
        .count();
    for (page, r) in records
        .iter_mut()
        .filter(|r| *r.receiver.as_bytes() == noise_receiver)
        .enumerate()
    {
        r.page = page as u32;
        r.total = noise_count as u32;
    }
    let last = blocks.last().unwrap();
    let snapshot = Snapshot::build(
        Manifest {
            profile: PROFILE.into(),
            genesis: [1; 32],
            start_height: START,
            start_parent: [0; 32],
            start_position: 0,
            end_height: last.height().into(),
            end_hash: last.hash().0,
            end_position: commitments.len() as u64,
            rows: 8192,
            salt: [4; 32],
            records: 0,
            data_sha256: [0; 32],
        },
        &records,
    )
    .unwrap();
    let positions: BTreeSet<_> = records.iter().map(|r| r.payment.position as u32).collect();
    let proof = WitnessSnapshot::build(&snapshot.manifest, &commitments, &positions)
        .unwrap()
        .encode();
    let mut digest = Sha256::new();
    for b in &blocks {
        let bytes = b.encode_to_vec();
        digest.update((bytes.len() as u64).to_le_bytes());
        digest.update(bytes);
    }
    let hash = hex::encode(digest.finalize());
    Fixture {
        blocks,
        batch_states,
        parent,
        keys,
        records,
        snapshot,
        proof,
        enhance: serde_json::json!({"blocks":enhancement}),
        hash,
        expected,
    }
}
