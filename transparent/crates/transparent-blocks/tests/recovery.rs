use transparent_blocks::{dataset::*, proto::*, scan::Scanner};
use transparent_wallet::store::{Anchor, ScriptEntry, ScriptOrigin};
use transparent_wallet::{MemoryStore, WalletStore};
fn blocks() -> Vec<CompactBlock> {
    let script = vec![0x76, 0xa9, 0x14, 1];
    vec![
        CompactBlock {
            height: 0,
            hash: vec![1; 32],
            prev_hash: vec![0; 32],
            vtx: vec![CompactTx {
                index: 0,
                txid: vec![2; 32],
                vout: vec![TxOut {
                    value: 10,
                    script_pub_key: script.clone(),
                }],
                ..Default::default()
            }],
            chain_metadata: Some(ChainMetadata::default()),
            ..Default::default()
        },
        CompactBlock {
            height: 1,
            hash: vec![3; 32],
            prev_hash: vec![1; 32],
            vtx: vec![
                CompactTx {
                    index: 1,
                    txid: vec![4; 32],
                    vin: vec![CompactTxIn {
                        prevout_txid: vec![2; 32],
                        prevout_index: 0,
                    }],
                    vout: vec![TxOut {
                        value: 9,
                        script_pub_key: script,
                    }],
                    actions: vec![CompactOrchardAction {
                        nullifier: vec![9; 32],
                        cmx: vec![10; 32],
                        ephemeral_key: vec![11; 32],
                        ciphertext: vec![12; 52],
                    }],
                    ..Default::default()
                },
                CompactTx {
                    index: 2,
                    txid: vec![5; 32],
                    vin: vec![CompactTxIn {
                        prevout_txid: vec![4; 32],
                        prevout_index: 0,
                    }],
                    ..Default::default()
                },
            ],
            chain_metadata: Some(ChainMetadata {
                orchard_commitment_tree_size: 1,
                ..Default::default()
            }),
            ..Default::default()
        },
    ]
}
#[test]
fn representations_scan_and_resume_without_duplicates() {
    let dir = tempfile::tempdir().unwrap();
    let blocks = blocks();
    let batch = write_batch(dir.path(), 0, &blocks).unwrap();
    let mut manifest = Manifest::new("01".repeat(32), 0, 1, "03".repeat(32), "fixture".into());
    manifest.batches.push(batch.clone());
    manifest.complete = true;
    manifest.save(dir.path()).unwrap();
    for variant in VARIANTS {
        for encoding in ["identity", "gzip"] {
            let encoded = read_batch(dir.path(), &manifest, 0, variant, encoding).unwrap();
            let decoded = decode(&encoded, encoding).unwrap();
            verify_blocks(&decoded, &batch).unwrap();
            if variant == "shielded" {
                assert!(decoded[0].vtx.is_empty());
                assert!(decoded[1].vtx[0].vin.is_empty());
                assert_eq!(decoded[1].vtx[0].actions[0].ciphertext, vec![12; 52]);
            }
        }
    }
    let scripts = vec![ScriptEntry {
        script: vec![0x76, 0xa9, 0x14, 1],
        required_from: 0,
        origin: ScriptOrigin::Derived,
    }];
    let mut store = MemoryStore::new();
    let mut scanner = Scanner::new(&mut store, &manifest, &scripts, 0, 1).unwrap();
    scanner.apply(&mut store, &batch, &blocks).unwrap();
    assert_eq!(store.events().unwrap().len(), 4);
    let mut resumed = Scanner::new(&mut store, &manifest, &scripts, 0, 1).unwrap();
    assert_eq!(resumed.next, 2);
    resumed.apply(&mut store, &batch, &blocks).unwrap();
    resumed
        .finish(
            &mut store,
            &Anchor {
                height: 1,
                hash: "03".repeat(32),
            },
        )
        .unwrap();
    assert!(resumed
        .finish(
            &mut store,
            &Anchor {
                height: 1,
                hash: "ff".repeat(32)
            }
        )
        .is_err());
    assert_eq!(store.events().unwrap().len(), 4);
    assert!(store.ledger().unwrap().unresolved().is_empty());
    let mut broken = blocks.clone();
    broken[1].prev_hash = vec![0; 32];
    assert!(verify_blocks(&broken, &batch).is_err());
    assert!(verify_blocks(&blocks[..1], &batch).is_err());
    let path = dir
        .path()
        .join(file_name(0, "transparent", "gzip").unwrap());
    std::fs::write(path, b"broken").unwrap();
    assert!(read_batch(dir.path(), &manifest, 0, "transparent", "gzip").is_err());
}
