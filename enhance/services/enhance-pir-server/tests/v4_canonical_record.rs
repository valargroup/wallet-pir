//! Frozen public transaction byte slices checked through parser and v4 HTTP PIR.
use enhance_pir::{
    v4_client::EnhancePirClient, EnhanceRecord, EnhanceRecordParts, EnhanceTransactionMetadata,
    RECORD_BYTES,
};
use enhance_pir_server::{
    store::RecordJournal,
    types::{DatabaseId, ENHANCE_LAYOUT},
    v4::{
        control::{Group, Ledger, Replica},
        coordinator::Coordinator,
        worker::Worker,
    },
};
use sha2::{Digest, Sha256};
use zakura_chain::serialization::ZcashDeserialize;
async fn serve(router: axum::Router) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    (
        url,
        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        }),
    )
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn canonical_transaction_records_match_frozen_oracle_across_publication() {
    let raw = hex::decode(include_str!("fixtures/ironwood-fee-expiry.hex").trim()).unwrap();
    let oracle: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/ironwood-record-oracle.json")).unwrap();
    assert_eq!(
        hex::encode(Sha256::digest(&raw)),
        oracle["transaction_sha256"]
    );
    let tx = zakura_chain::transaction::Transaction::zcash_deserialize(raw.as_slice()).unwrap();
    assert_eq!(tx.hash().to_string(), oracle["transaction_id"]);
    assert!(!tx.has_transparent_inputs() && !tx.has_transparent_outputs());
    assert!(
        tx.sapling_spends_per_anchor().next().is_none()
            && tx.sapling_outputs().next().is_none()
            && tx.orchard_actions().next().is_none()
    );
    let fee: i64 = tx.ironwood_value_balance().ironwood_amount().into();
    let metadata = EnhanceTransactionMetadata::new(
        tx.expiry_height().unwrap().0,
        Some(fee.try_into().unwrap()),
    )
    .unwrap();
    let records: Vec<_> = tx
        .ironwood_actions()
        .map(|a| {
            EnhanceRecord::from_parts(EnhanceRecordParts {
                enc_ciphertext_suffix: <[u8; 580]>::from(a.enc_ciphertext)[52..]
                    .try_into()
                    .unwrap(),
                cv_net: a.cv.into(),
                out_ciphertext: a.out_ciphertext.into(),
                has_transparent_inputs: false,
                has_transparent_outputs: false,
                metadata,
            })
        })
        .collect();
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
    let root = tempfile::tempdir().unwrap();
    let mut tasks = Vec::new();
    let mut replicas = Vec::new();
    for i in 0..2 {
        let (url, task) = serve(
            Worker::open(&root.path().join(format!("worker-{i}")))
                .unwrap()
                .router(),
        )
        .await;
        tasks.push(task);
        replicas.push(Replica {
            name: format!("r{i}"),
            url,
            incarnation: String::new(),
            ledger: Ledger::default(),
        });
    }
    let coordinator = Coordinator::open(
        &root.path().join("control"),
        vec![Group {
            placement_policy: Default::default(),
            id: "g0".into(),
            sequence: 0,
            replicas,
            settling: false,
        }],
    )
    .unwrap();
    let (origin, server) = serve(coordinator.clone().router()).await;
    let mut journal = RecordJournal::open(
        root.path().join("journal"),
        DatabaseId::Enhance,
        ENHANCE_LAYOUT,
    )
    .unwrap();
    // Synthetic positions straddle a PIR row; they are not the actions' chain positions.
    let mut rows = vec![vec![0; RECORD_BYTES]; 67];
    for i in 0..2 {
        rows[32 + i] = records[i].as_bytes().to_vec();
    }
    journal
        .append_block(3483367, "01".repeat(32), &rows)
        .unwrap();
    coordinator
        .publish(&journal, 3483367, "01".repeat(32))
        .await
        .unwrap();
    let mut retained = EnhancePirClient::connect(&origin).await.unwrap();
    let old = retained.manifest().generation;
    journal
        .append_block(3483368, "02".repeat(32), &[vec![0; RECORD_BYTES]])
        .unwrap();
    coordinator
        .publish(&journal, 3483368, "02".repeat(32))
        .await
        .unwrap();
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
    assert_eq!(retained.manifest().generation, old);
    assert!(current.manifest().generation > old);
    server.abort();
    for task in tasks {
        task.abort();
    }
}
