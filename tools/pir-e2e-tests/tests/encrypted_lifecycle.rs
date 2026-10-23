//! Encrypted publication and restart through real listeners.
#[path = "encrypted_lifecycle/discovery.rs"]
mod discovery;
#[path = "support/encrypted.rs"]
mod encrypted;
#[path = "encrypted_lifecycle/publication.rs"]
mod publication;
#[path = "support/publication.rs"]
#[allow(dead_code)]
mod publication_source;
use encrypted::record;

/// No HTTP listeners or consumer processes participate in publication preparation.
#[test]
fn offline_publication_loads_and_answers_without_consumers() {
    use enhance_publisher::publication::VerificationStats;
    use enhance_shared::{
        client::QuerySession,
        prepared_packing,
        protocol::{self, Geometry, Lifecycle, Manifest, QueryBinding},
        runtime::Engine,
        PackingBudget, RECORD_BYTES,
    };
    let started = std::time::Instant::now();
    let root = tempfile::tempdir().unwrap();
    let coverage = Lifecycle::default()
        .coverage(67, Geometry::default())
        .unwrap();
    let epochs = [(0, "0".into())].into();
    let source = |start, count| Ok((start..start + count as u64).flat_map(record).collect());
    let prepared = enhance_publisher::publication::prepare_publication(
        root.path(),
        coverage.clone(),
        &epochs,
        source,
    )
    .unwrap();
    let cold = started.elapsed();
    let inventory = prepared.inventory().clone();
    enhance_publisher::publication::verify_publication(root.path(), inventory.clone()).unwrap();
    let domain = &inventory.domains[0];
    let object_path = domain.packing_object().path(root.path()).unwrap();
    let installed = std::fs::metadata(&object_path).unwrap().modified().unwrap();
    let packing = prepared_packing::load(
        &object_path,
        &domain.packing,
        domain.plan.shard.logical_rows,
        &PackingBudget::coordinator(),
    )
    .unwrap();
    let manifest = Manifest {
        recovery_epoch: 0,
        placement_revision: 1,
        domain_recovery_epochs: epochs.clone(),
        schema_version: protocol::SCHEMA_VERSION,
        protocol_revision: protocol::PROTOCOL_REVISION.into(),
        network: "main".into(),
        pool: "ironwood".into(),
        generation: 1,
        anchor_height: 3428143,
        anchor_block_hash: "01".repeat(32),
        geometry: Geometry::default(),
        coverage: coverage.clone(),
        sessions: vec![domain.reference().unwrap()],
        unit_identities: [(0, domain.plan.units.clone())].into(),
    };
    assert!(inventory.supports(&manifest).unwrap());
    let mut later = manifest.clone();
    later.generation += 1;
    later.placement_revision += 1;
    later.anchor_height += 1;
    later.anchor_block_hash = "02".repeat(32);
    assert!(
        domain.supports(&later).unwrap(),
        "unchanged material survives publication advancement"
    );
    later.sessions[0].public_params_sha256 = "03".repeat(32);
    assert!(
        !domain.supports(&later).unwrap(),
        "different setup is incompatible"
    );
    let consumer = tempfile::tempdir().unwrap();
    let (evaluation, _) = Engine::new(consumer.path())
        .prepare(domain.plan.clone(), |start, count| {
            let bytes =
                std::fs::read(domain.rows[0].path(root.path())?).map_err(|e| e.to_string())?;
            let begin = start as usize * RECORD_BYTES;
            Ok(bytes[begin..begin + count * RECORD_BYTES].to_vec())
        })
        .unwrap();
    let session = QuerySession::new(&manifest, packing.session(&manifest, 0)).unwrap();
    for position in [0, 32, 33, 66] {
        let (query, slot) = session.prepare_position(position).unwrap();
        let binding = QueryBinding::decode(query.body()).unwrap();
        let coefficients = packing
            .query_coefficients(query.body(), binding, session.query_rows())
            .unwrap();
        let answer = packing
            .pack(
                query.body(),
                &evaluation.evaluate(&coefficients).unwrap(),
                session.query_rows(),
            )
            .unwrap();
        let row = session.decode(query, &answer).unwrap();
        assert_eq!(
            &row[slot * RECORD_BYTES..(slot + 1) * RECORD_BYTES],
            record(position)
        );
    }
    // Let the cold build's objects settle so their hashes may be reused.
    std::thread::sleep(std::time::Duration::from_millis(1100));
    let began_reuse = std::time::Instant::now();
    let reused =
        enhance_publisher::publication::prepare_publication(root.path(), coverage, &epochs, source)
            .unwrap();
    assert_eq!(
        protocol::digest(&inventory),
        protocol::digest(reused.inventory())
    );
    assert_eq!(
        installed,
        std::fs::metadata(&object_path).unwrap().modified().unwrap()
    );
    let mut invalid = inventory.clone();
    invalid.domains[0].packing.format += 1;
    assert!(invalid.validate().is_err());
    invalid = inventory.clone();
    invalid.domains[0].rows[0].bytes -= 1;
    assert!(invalid.validate().is_err());
    invalid = inventory.clone();
    invalid.domains[0].plan.units[0].setup_sha256 = "04".repeat(32);
    assert!(invalid.validate().is_err());
    invalid = inventory.clone();
    invalid.domains[0].rows[0].sha256 = "../escape".into();
    assert!(invalid.validate().is_err());
    eprintln!(
        "offline preparation cold={cold:?} reuse={:?}",
        began_reuse.elapsed()
    );
    let mut objects = std::collections::BTreeMap::new();
    for domain in &inventory.domains {
        let packing = domain.packing_object();
        for object in domain
            .rows
            .iter()
            .chain([&domain.hint, &domain.public, &packing])
        {
            objects.insert(object.sha256.clone(), object.bytes);
        }
    }
    let every_object = VerificationStats {
        files: objects.len() as u64,
        bytes: objects.values().sum(),
        reused: 0,
    };
    assert_eq!(
        reused.verification_stats(),
        every_object,
        "preparation reads each referenced object once"
    );
    assert_eq!(
        enhance_publisher::publication::verify_publication(root.path(), inventory.clone())
            .unwrap()
            .verification_stats(),
        every_object,
        "an inventory without evidence is hashed in full"
    );
    let published = reused.recheck(root.path()).unwrap();
    assert_eq!(
        published.verification_stats(),
        VerificationStats {
            files: 0,
            bytes: 0,
            reused: objects.len() as u64,
        },
        "publication rereads no unchanged object"
    );
    // A hint naming the settled rows object keeps its role-required length, so
    // only the object check can see that those bytes have another length.
    let mut aliased = inventory.clone();
    let rows = aliased.domains[0].rows[0].clone();
    assert_ne!(rows.bytes, aliased.domains[0].hint.bytes);
    aliased.domains[0].hint.sha256 = rows.sha256;
    aliased.validate().unwrap();
    assert!(
        enhance_publisher::publication::verify_publication(root.path(), aliased).is_err(),
        "a reference is checked against its own length, not an earlier one"
    );
    let rows_path = domain.rows[0].path(root.path()).unwrap();
    // Published objects are installed read-only; the tampering writer must
    // lift that first, as only the file's owner or root could.
    std::fs::set_permissions(
        &rows_path,
        std::os::unix::fs::PermissionsExt::from_mode(0o644),
    )
    .unwrap();
    std::fs::OpenOptions::new()
        .write(true)
        .open(rows_path)
        .unwrap()
        .set_len(1)
        .unwrap();
    assert!(enhance_publisher::publication::verify_publication(root.path(), inventory).is_err());
    assert!(
        published.recheck(root.path()).is_err(),
        "a truncated object is rehashed despite preparation evidence"
    );
}
