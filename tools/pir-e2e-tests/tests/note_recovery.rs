//! Authenticated synthetic Ironwood records through real HTTP PIR.
//! Fixed inputs are public test material, never wallet credentials; the
//! library-owned fixture holds the Orchard and note-encryption cryptography.
use enhance_publisher::local_serving as serving;
use enhance_publisher::{
    control::{Group, Ledger, Replica},
    store::RecordJournal,
};
use enhance_query_server::{
    client::EnhancePirClient, EnhanceRecord, EnhanceRecordParts, EnhanceTransactionMetadata,
    RECORD_BYTES,
};
use enhance_query_server::{
    types::{DatabaseId, ENHANCE_LAYOUT},
    worker::Worker,
};
use zakura_pir_client::enhance::testing::{
    decrypt_incoming, recover_outgoing, FixtureKey, IronwoodOutputFixture, NoteDomain, OutputBytes,
    RecoveredNote, FIXTURE_MEMO, FIXTURE_VALUE,
};

/// Incoming decryption with the recipient's own key in the Ironwood domain.
fn own_ivk(output: &OutputBytes) -> Option<RecoveredNote> {
    decrypt_incoming(NoteDomain::Ironwood, FixtureKey::Own, output)
}

/// Outgoing recovery with `key`, using the returned `cv_net` and the
/// `out_ciphertext` of `record`.
fn ovk(
    key: FixtureKey,
    output: &OutputBytes,
    cv_net: &[u8; 32],
    record: &EnhanceRecord,
) -> Option<RecoveredNote> {
    recover_outgoing(key, output, cv_net, record.out_ciphertext())
}

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
async fn incoming_and_outgoing_recovery_authenticate_pir_returned_record() {
    let fixture = IronwoodOutputFixture::new();
    let expected = Some(RecoveredNote {
        value: FIXTURE_VALUE,
        recipient_matches: true,
        note_matches: true,
        memo: FIXTURE_MEMO,
    });
    let record = EnhanceRecord::from_parts(EnhanceRecordParts {
        enc_ciphertext_suffix: fixture.output.enc_ciphertext_suffix(),
        cv_net: fixture.cv_net,
        out_ciphertext: fixture.out_ciphertext,
        has_transparent_inputs: false,
        has_transparent_outputs: false,
        metadata: EnhanceTransactionMetadata::new(3428144, None).unwrap(),
    });
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
    let serving = serving::Serving::start(
        root.path(),
        &root.path().join("control"),
        vec![Group {
            placement_policy: Default::default(),
            id: "g0".into(),
            sequence: 0,
            replicas,
        }],
    )
    .await
    .unwrap();
    let (coordinator, origin) = (serving.coordinator(), serving.origin.clone());
    let mut journal = RecordJournal::open(
        root.path().join("journal"),
        DatabaseId::Enhance,
        ENHANCE_LAYOUT,
    )
    .unwrap();
    let mut records = vec![vec![0; RECORD_BYTES]; 67];
    records[32] = record.as_bytes().to_vec();
    journal
        .append_block(3428143, "01".repeat(32), &records)
        .unwrap();
    coordinator
        .publish(&journal, 3428143, "01".repeat(32))
        .await
        .unwrap();
    let mut client = EnhancePirClient::connect(&origin).await.unwrap();
    let bytes = client.query_position_with_timing(32).await.unwrap().0;
    assert_eq!(bytes.as_ref(), record.as_bytes());
    let returned = EnhanceRecord::from_bytes(*bytes.as_bytes()).unwrap();
    // The compact block supplies the nullifier, cmx, ephemeral key and the
    // ciphertext prefix; the PIR-returned record supplies the rest.
    let compact = &fixture.output;
    let output = compact.with_enc_ciphertext_suffix(returned.enc_ciphertext_suffix());
    let returned_cv = *returned.cv_net();
    assert_eq!(own_ivk(&output), expected);
    assert_eq!(
        ovk(FixtureKey::Own, &output, &returned_cv, &returned),
        expected
    );
    assert!(decrypt_incoming(NoteDomain::Orchard, FixtureKey::Own, &output).is_none());
    assert!(decrypt_incoming(NoteDomain::Ironwood, FixtureKey::Other, &output).is_none());
    assert!(ovk(FixtureKey::Other, &output, &returned_cv, &returned).is_none());
    let mut out_corrupt = *record.as_bytes();
    out_corrupt[enhance_query_server::RECORD_OUT_CIPHERTEXT_OFFSET] ^= 1;
    let out_corrupt = EnhanceRecord::from_bytes(out_corrupt).unwrap();
    let out_corrupt_output =
        compact.with_enc_ciphertext_suffix(out_corrupt.enc_ciphertext_suffix());
    assert!(ovk(
        FixtureKey::Own,
        &out_corrupt_output,
        &returned_cv,
        &out_corrupt
    )
    .is_none());
    assert_eq!(own_ivk(&out_corrupt_output), expected);
    // Expiry/fee and transaction-shape flags are indexer metadata, outside note
    // authentication. Do not claim that decrypting the note authenticates them.
    let mut metadata_changed = *record.as_bytes();
    metadata_changed[enhance_query_server::RECORD_EXPIRY_HEIGHT_OFFSET] ^= 1;
    let metadata_changed = EnhanceRecord::from_bytes(metadata_changed).unwrap();
    assert_eq!(
        own_ivk(&compact.with_enc_ciphertext_suffix(metadata_changed.enc_ciphertext_suffix())),
        expected
    );
    let mut cmx_tampered = output.clone();
    cmx_tampered.cmx[0] ^= 1;
    assert!(own_ivk(&cmx_tampered).is_none());
    let mut corrupted = *record.as_bytes();
    corrupted[100] ^= 1;
    let corrupted = EnhanceRecord::from_bytes(corrupted).unwrap();
    let corrupted_output = compact.with_enc_ciphertext_suffix(corrupted.enc_ciphertext_suffix());
    assert!(own_ivk(&corrupted_output).is_none());
    assert!(ovk(FixtureKey::Own, &corrupted_output, &returned_cv, &corrupted).is_none());
    let mut prefix_corrupt = output.clone();
    prefix_corrupt.enc_ciphertext[0] ^= 1;
    assert!(own_ivk(&prefix_corrupt).is_none());
    let mut epk_tampered = output.clone();
    epk_tampered.ephemeral_key[0] ^= 1;
    assert!(own_ivk(&epk_tampered).is_none());
    for task in tasks {
        task.abort();
    }
}
