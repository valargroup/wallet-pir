use enhance_pir::{EnhanceRecord, EnhanceRecordParts, EnhanceTransactionMetadata, RECORD_BYTES};
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
        chain::BlockSource,
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
    pub parent: FullViewingKey,
    pub keys: Vec<FullViewingKey>,
    pub records: Vec<Record>,
    pub snapshot: Snapshot,
    pub proof: Vec<u8>,
    pub enhance: serde_json::Value,
    pub hash: String,
    pub expected: Vec<serde_json::Value>,
}
pub fn generate() -> Fixture {
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
    let owners = [(0usize, 0usize), (40, 3), (80, 7), (120, 12), (160, 20)];
    let mut spent = None;
    for block in 0..BLOCKS {
        let owner = owners.iter().find(|(b, _)| *b == block).map(|(_, k)| *k);
        let fk = IronwoodFvk(owner.map_or_else(|| noise.clone(), |i| keys[i].clone()));
        let noise_fk = IronwoodFvk(noise.clone());
        let mut outputs = vec![FakeCompactOutput::new(
            &fk,
            AddressType::DefaultExternal,
            Zatoshis::const_from_u64(100_000),
        )];
        for _ in 1..16 {
            outputs.push(FakeCompactOutput::new(
                &noise_fk,
                AddressType::DefaultExternal,
                Zatoshis::const_from_u64(10_000),
            ));
        }
        let (_, _, nfs) = st.generate_next_block_multi(&outputs);
        if block == 0 {
            spent = Some(nfs[0]);
        }
    }
    let recipient = Address::Unified(
        UnifiedAddress::from_receivers(Some(noise.address_at(0u32, Scope::External)), None, None)
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
    let mut commitments = Vec::new();
    let mut records = Vec::new();
    let mut expected = Vec::new();
    let mut enhancement = Vec::new();
    let prepared: Vec<_> = keys
        .iter()
        .map(|k| k.to_ivk(Scope::External).prepare())
        .collect();
    for block in &blocks {
        let mut slots = Vec::new();
        for tx in &block.vtx {
            for (action_index, compact) in tx.ironwood_actions.iter().enumerate() {
                let position = commitments.len() as u64;
                let action = CompactAction::try_from(compact).unwrap();
                commitments.push(compact.cmx().unwrap().to_bytes());
                let mut record = vec![0; RECORD_BYTES];
                for (i, ivk) in prepared.iter().enumerate() {
                    if let Some((note, address)) = try_compact_note_decryption(
                        &IronwoodDomain::for_compact_action(&action),
                        ivk,
                        &action,
                    ) {
                        assert_eq!(address, keys[i].address_at(0u32, Scope::External));
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
                        let cmx = orchard::note::ExtractedNoteCommitment::from(note.commitment());
                        let enc = IronwoodNoteEncryption::new(
                            Some(OutgoingViewingKey::from([0; 32])),
                            note,
                            [0; 512],
                        );
                        let full = enc.encrypt_note_plaintext();
                        assert_eq!(&full[..52], compact.ciphertext.as_slice());
                        record = EnhanceRecord::from_parts(EnhanceRecordParts {
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
                                ciphertext_prefix: full[..52].try_into().unwrap(),
                            },
                        });
                    }
                }
                slots.push(hex::encode(record));
            }
        }
        let mut hash = block.hash().0;
        hash.reverse();
        enhancement.push(serde_json::json!({"height":u32::from(block.height()),"hash":hex::encode(hash),"records":slots}));
    }
    assert_eq!(records.len(), 5);
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
