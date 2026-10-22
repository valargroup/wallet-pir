//! Frozen public transaction byte slices checked through parser and HTTPS-fed PIR.
use enhance_shared::{client::EnhancePirClient, RECORD_BYTES};
use pir_e2e_tests::enhance_fixture::System;
use sha2::{Digest, Sha256};
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn canonical_transaction_records_match_frozen_oracle_across_publication() {
    let raw = hex::decode(include_str!("fixtures/ironwood-fee-expiry.hex").trim()).unwrap();
    let oracle: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/ironwood-record-oracle.json")).unwrap();
    assert_eq!(
        hex::encode(Sha256::digest(&raw)),
        oracle["transaction_sha256"]
    );
    let tx = zakura_server_chain::testing::Tx::from_bytes(&raw);
    assert_eq!(tx.txid_display(), oracle["transaction_id"]);
    assert!(!tx.has_sapling_outputs());
    // The coordinator's producer, with no spent outputs to resolve.
    let actions = zakura_server_chain::enhance::raw_transaction_actions(
        &raw,
        &std::collections::HashMap::new(),
    )
    .unwrap();
    assert!(actions
        .iter()
        .all(|a| !a.has_transparent_inputs && !a.has_transparent_outputs));
    let records = enhance_publisher::chain_source::records(actions).unwrap();
    let expected: Vec<_> = [
        include_str!("fixtures/ironwood-canonical-record-0.hex"),
        include_str!("fixtures/ironwood-canonical-record-1.hex"),
    ]
    .iter()
    .map(|s| hex::decode(s.trim()).unwrap())
    .collect();
    assert_eq!(records.len(), 2);
    for (i, record) in records.iter().enumerate() {
        let offsets = &oracle["records"][i]["raw_offsets"];
        let mut sliced = Vec::new();
        for (field, length) in [
            ("ephemeral_key", 32),
            ("enc_ciphertext", 580),
            ("cv_net", 32),
            ("out_ciphertext", 80),
        ] {
            let start = offsets[field].as_u64().unwrap() as usize;
            sliced.extend_from_slice(&raw[start..start + length]);
        }
        sliced.push(4); // Fee present; this fixture has no transparent inputs/outputs.
        for (field, length) in [("expiry_height", 4), ("fee_value_balance", 8)] {
            let start = offsets[field].as_u64().unwrap() as usize;
            sliced.extend_from_slice(&raw[start..start + length]);
        }
        assert_eq!(
            sliced, expected[i],
            "frozen oracle must retain its raw-byte provenance"
        );
        assert_eq!(record.as_bytes().as_slice(), &expected[i][84..]);
        let ciphertext_start = offsets["enc_ciphertext"].as_u64().unwrap() as usize;
        assert_eq!(
            record.enc_ciphertext_suffix().as_slice(),
            &raw[ciphertext_start + 52..ciphertext_start + 580]
        );
        assert_eq!(
            hex::encode(Sha256::digest(&expected[i])),
            oracle["records"][i]["record_sha256"]
        );
        assert_eq!(record.metadata().expiry_height(), 3483371);
        assert_eq!(record.metadata().fee_zatoshis(), Some(10000));
    }
    // Synthetic positions straddle a PIR row; they are not the actions' chain positions.
    let mut rows = vec![vec![0; RECORD_BYTES]; 67];
    for i in 0..2 {
        rows[32 + i] = records[i].as_bytes().to_vec();
    }
    let mut system = System::start(rows).await;
    let origin = system.origin.clone();
    let mut retained = EnhancePirClient::connect(&origin).await.unwrap();
    let old = retained.manifest().generation;
    system.append(vec![vec![0; RECORD_BYTES]]).await;
    let mut current = EnhancePirClient::connect(&origin).await.unwrap();
    for (i, bytes) in expected.iter().enumerate() {
        assert_eq!(
            retained
                .query_position_with_timing(32 + i as u64)
                .await
                .unwrap()
                .0
                .as_ref(),
            &bytes[84..]
        );
        assert_eq!(
            current
                .query_position_with_timing(32 + i as u64)
                .await
                .unwrap()
                .0
                .as_ref(),
            &bytes[84..]
        );
    }
    assert!(retained.manifest().generation > old);
    assert_eq!(
        retained.manifest().generation,
        current.manifest().generation
    );
    assert!(current.manifest().generation > old);
}
