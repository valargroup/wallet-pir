//! Authenticated synthetic Ironwood records through real HTTP PIR.
//! Fixed inputs are public test material, never wallet credentials.
use enhance_pir::{
    client::EnhancePirClient, EnhanceRecord, EnhanceRecordParts, EnhanceTransactionMetadata,
    RECORD_BYTES,
};
use enhance_pir_server::{
    control::{Group, Ledger, Replica},
    coordinator::Coordinator,
    store::RecordJournal,
    types::{DatabaseId, ENHANCE_LAYOUT},
    worker::Worker,
};
use orchard::{
    keys::{FullViewingKey, PreparedIncomingViewingKey, Scope, SpendingKey},
    note::{ExtractedNoteCommitment, NoteVersion, Nullifier, RandomSeed, Rho},
    note_encryption::{CompactAction, IronwoodDomain, IronwoodNoteEncryption, OrchardDomain},
    value::{NoteValue, ValueCommitTrapdoor, ValueCommitment},
    Note,
};
use zcash_note_encryption::{
    try_note_decryption, try_output_recovery_with_ovk, Domain, EphemeralKeyBytes, ShieldedOutput,
};

struct Output {
    record: EnhanceRecord,
    cmx: [u8; 32],
    ephemeral_key: [u8; 32],
    enc_ciphertext: [u8; 580],
}
impl<D: Domain<ExtractedCommitmentBytes = [u8; 32]>> ShieldedOutput<D, 580> for Output {
    fn ephemeral_key(&self) -> EphemeralKeyBytes {
        EphemeralKeyBytes(self.ephemeral_key)
    }
    fn cmstar_bytes(&self) -> [u8; 32] {
        self.cmx
    }
    fn enc_ciphertext(&self) -> &[u8; 580] {
        &self.enc_ciphertext
    }
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
    let sk = SpendingKey::from_bytes([7; 32]).unwrap();
    let fvk = FullViewingKey::from(&sk);
    let recipient = fvk.address_at(0u32, Scope::External);
    let ivk = PreparedIncomingViewingKey::new(&fvk.to_ivk(Scope::External));
    let ovk = fvk.to_ovk(Scope::External);
    let rho = Rho::from_bytes(&[0; 32]).unwrap();
    let note = Note::from_parts(
        recipient,
        NoteValue::from_raw(5),
        rho,
        RandomSeed::from_bytes([9; 32], &rho).unwrap(),
        NoteVersion::V3,
    )
    .unwrap();
    let memo = [42; 512];
    let cv = ValueCommitment::derive(
        NoteValue::from_raw(5) - NoteValue::from_raw(0),
        ValueCommitTrapdoor::from_bytes([0; 32]).unwrap(),
    );
    let cmx = ExtractedNoteCommitment::from(note.commitment());
    let encryptor = IronwoodNoteEncryption::new(Some(ovk.clone()), note, memo);
    let ephemeral_key = IronwoodDomain::epk_bytes(encryptor.epk()).0;
    let enc_ciphertext = encryptor.encrypt_note_plaintext();
    let record = EnhanceRecord::from_parts(EnhanceRecordParts {
        enc_ciphertext_suffix: enc_ciphertext[52..].try_into().unwrap(),
        cv_net: cv.to_bytes(),
        out_ciphertext: encryptor.encrypt_outgoing_plaintext(&cv, &cmx, &mut rand::thread_rng()),
        has_transparent_inputs: false,
        has_transparent_outputs: false,
        metadata: EnhanceTransactionMetadata::new(3428144, None).unwrap(),
    });
    let compact = CompactAction::from_parts(
        Nullifier::from_bytes(&[0; 32]).unwrap(),
        cmx,
        EphemeralKeyBytes(ephemeral_key),
        enc_ciphertext[..52].try_into().unwrap(),
    );
    let domain = IronwoodDomain::for_compact_action(&compact);
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
    let mut reconstructed = [0; 580];
    reconstructed[..52].copy_from_slice(&enc_ciphertext[..52]);
    reconstructed[52..].copy_from_slice(returned.enc_ciphertext_suffix());
    let mut output = Output {
        record: EnhanceRecord::from_bytes(bytes.as_ref().try_into().unwrap()).unwrap(),
        cmx: cmx.to_bytes(),
        ephemeral_key,
        enc_ciphertext: reconstructed,
    };
    assert_eq!(
        try_note_decryption(&domain, &ivk, &output),
        Some((note, recipient, memo))
    );
    let returned_cv = ValueCommitment::from_bytes(output.record.cv_net()).unwrap();
    assert_eq!(
        try_output_recovery_with_ovk(
            &domain,
            &ovk,
            &output,
            &returned_cv,
            output.record.out_ciphertext()
        ),
        Some((note, recipient, memo))
    );
    assert!(
        try_note_decryption(&OrchardDomain::for_compact_action(&compact), &ivk, &output).is_none()
    );
    let other_fvk = FullViewingKey::from(&SpendingKey::from_bytes([8; 32]).unwrap());
    let other_ivk = PreparedIncomingViewingKey::new(&other_fvk.to_ivk(Scope::External));
    assert!(try_note_decryption(&domain, &other_ivk, &output).is_none());
    assert!(try_output_recovery_with_ovk(
        &domain,
        &other_fvk.to_ovk(Scope::External),
        &output,
        &returned_cv,
        output.record.out_ciphertext()
    )
    .is_none());
    let mut out_corrupt = *record.as_bytes();
    out_corrupt[enhance_pir::types::RECORD_OUT_CIPHERTEXT_OFFSET] ^= 1;
    output.record = EnhanceRecord::from_bytes(out_corrupt).unwrap();
    assert!(try_output_recovery_with_ovk(
        &domain,
        &ovk,
        &output,
        &returned_cv,
        output.record.out_ciphertext()
    )
    .is_none());
    assert_eq!(
        try_note_decryption(&domain, &ivk, &output),
        Some((note, recipient, memo))
    );
    // Expiry/fee and transaction-shape flags are indexer metadata, outside note
    // authentication. Do not claim that decrypting the note authenticates them.
    let mut metadata_changed = *record.as_bytes();
    metadata_changed[enhance_pir::types::RECORD_EXPIRY_HEIGHT_OFFSET] ^= 1;
    output.record = EnhanceRecord::from_bytes(metadata_changed).unwrap();
    assert_eq!(
        try_note_decryption(&domain, &ivk, &output),
        Some((note, recipient, memo))
    );
    output.record = record.clone();
    output.cmx[0] ^= 1;
    assert!(try_note_decryption(&domain, &ivk, &output).is_none());
    output.cmx = cmx.to_bytes();
    let mut corrupted = *record.as_bytes();
    corrupted[100] ^= 1;
    output.record = EnhanceRecord::from_bytes(corrupted).unwrap();
    output.enc_ciphertext[52..].copy_from_slice(output.record.enc_ciphertext_suffix());
    assert!(try_note_decryption(&domain, &ivk, &output).is_none());
    assert!(try_output_recovery_with_ovk(
        &domain,
        &ovk,
        &output,
        &returned_cv,
        output.record.out_ciphertext()
    )
    .is_none());
    output.enc_ciphertext = reconstructed;
    output.enc_ciphertext[0] ^= 1;
    assert!(try_note_decryption(&domain, &ivk, &output).is_none());
    output.enc_ciphertext = reconstructed;
    output.ephemeral_key[0] ^= 1;
    assert!(try_note_decryption(&domain, &ivk, &output).is_none());
    server.abort();
    for task in tasks {
        task.abort();
    }
}
