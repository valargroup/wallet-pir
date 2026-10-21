//! Encrypted publication and restart through real listeners.
#[path = "encrypted_lifecycle/discovery.rs"]
mod discovery;
#[path = "support/encrypted.rs"]
mod encrypted;
#[path = "encrypted_lifecycle/publication.rs"]
mod publication;
use encrypted::{record, workers};
use enhance_publisher::local_serving as serving;
use enhance_publisher::store::RecordJournal;
use enhance_query_server::{
    client::EnhancePirClient,
    types::{DatabaseId, ENHANCE_LAYOUT},
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

/// No HTTP listeners or consumer processes participate in publication preparation.
#[test]
fn offline_publication_loads_and_answers_without_consumers() {
    use enhance_publisher::publication::Hashed;
    use enhance_query_server::{
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
    let prepared =
        enhance_publisher::publication::prepare(root.path(), coverage.clone(), &epochs, source)
            .unwrap();
    let cold = started.elapsed();
    let inventory = prepared.inventory().clone();
    enhance_publisher::publication::verify(root.path(), inventory.clone()).unwrap();
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
        enhance_publisher::publication::prepare(root.path(), coverage, &epochs, source).unwrap();
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
    let every_object = Hashed {
        files: objects.len() as u64,
        bytes: objects.values().sum(),
        reused: 0,
    };
    assert_eq!(
        reused.hashed(),
        every_object,
        "preparation reads each referenced object once"
    );
    assert_eq!(
        enhance_publisher::publication::verify(root.path(), inventory.clone())
            .unwrap()
            .hashed(),
        every_object,
        "an inventory without evidence is hashed in full"
    );
    let published = reused.recheck(root.path()).unwrap();
    assert_eq!(
        published.hashed(),
        Hashed {
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
        enhance_publisher::publication::verify(root.path(), aliased).is_err(),
        "a reference is checked against its own length, not an earlier one"
    );
    let rows_path = domain.rows[0].path(root.path()).unwrap();
    std::fs::OpenOptions::new()
        .write(true)
        .open(rows_path)
        .unwrap()
        .set_len(1)
        .unwrap();
    assert!(enhance_publisher::publication::verify(root.path(), inventory).is_err());
    assert!(
        published.recheck(root.path()).is_err(),
        "a truncated object is rehashed despite preparation evidence"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn encrypted_publication_advances_with_a_pending_commit_and_survives_restart() {
    // Ordinary correctness uses the small reference profile. The explicit native
    // qualification builds and transfers hundreds of MiB of public matrices;
    // its cold publication cost is separate from wallet request latency.
    let native_qualification = cfg!(all(
        feature = "native-reinspiring",
        feature = "qualification-tests"
    ));
    assert!(
        !cfg!(feature = "native-reinspiring") || native_qualification,
        "native publication recovery requires the qualification-tests feature"
    );
    let deadline = Duration::from_secs(if native_qualification { 180 } else { 30 });
    let phase = std::cell::Cell::new("starting listeners");
    let started = std::time::Instant::now();
    tokio::time::timeout(deadline, async {
        let root = tempfile::tempdir().unwrap();
        let reject_commit = Arc::new(AtomicBool::new(false));
        let (_listeners, groups) = workers(root.path(), 2, reject_commit.clone()).await;
        let state_dir = root.path().join("coordinator");
        let mut serving = serving::Serving::start(root.path(), &state_dir, groups.clone())
            .await
            .unwrap();
        let coordinator = serving.coordinator();
        let mut journal = RecordJournal::open(
            root.path().join("journal"),
            DatabaseId::Enhance,
            ENHANCE_LAYOUT,
        )
        .unwrap();
        journal
            .append_block(
                3428143,
                "01".repeat(32),
                &(0..67).map(record).collect::<Vec<_>>(),
            )
            .unwrap();
        // Both workers prepare the first assignment, but one loses its commit
        // acknowledgement. Its healthy peer must still publish the next block.
        phase.set("publishing the first block");
        reject_commit.store(true, Ordering::SeqCst);
        coordinator
            .publish(&journal, 3428143, "01".repeat(32))
            .await
            .unwrap();
        eprintln!("initial encrypted publication: {:?}", started.elapsed());
        // Keep one real commit unacknowledged while its healthy peer publishes
        // again. This bridges the small decision-delivery tests to real worker
        // preparation, publication selection and encrypted wallet answers.
        let read_state = || -> enhance_publisher::control::State {
            serde_json::from_slice(&std::fs::read(state_dir.join("controller.json")).unwrap())
                .unwrap()
        };
        let pending = read_state().pending_commits;
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].replica, "worker-0");
        journal
            .append_block(3428144, "02".repeat(32), &[record(67)])
            .unwrap();
        phase.set("publishing while a commit is pending");
        coordinator
            .publish(&journal, 3428144, "02".repeat(32))
            .await
            .unwrap();
        assert_eq!(read_state().pending_commits, pending);
        eprintln!("appended encrypted publication: {:?}", started.elapsed());
        phase.set("querying the new publication");
        let mut current = EnhancePirClient::connect(&serving.origin).await.unwrap();
        assert_eq!(
            current
                .query_position_with_timing(67)
                .await
                .unwrap()
                .0
                .as_ref(),
            record(67)
        );
        phase.set("retrying the pending commit");
        reject_commit.store(false, Ordering::SeqCst);
        coordinator.reconcile().await.unwrap();
        assert!(read_state().pending_commits.is_empty());
        let malformed = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap()
            .post(format!("{}/v1/enhance/query", serving.origin))
            .body(vec![0; 8])
            .send()
            .await
            .unwrap();
        assert!(malformed.status().is_client_error());
        let _ = malformed.bytes().await.unwrap();
        drop(current);
        drop(coordinator);
        phase.set("restoring the coordinator from disk");
        serving.restart(groups).await.unwrap();
        phase.set("querying after restart with a fresh wallet");
        let mut client = EnhancePirClient::connect(&serving.origin).await.unwrap();
        // These positions share a row. One encrypted request checks that the
        // update preserved the old slot beside the appended record, and that
        // a fresh wallet can recover both after reopening durable state.
        let recovered = client.query_positions(&[66, 67]).await.unwrap();
        assert_eq!(recovered[0].as_ref(), record(66));
        assert_eq!(recovered[1].as_ref(), record(67));
        drop(client);
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "encrypted HTTP smoke exceeded its deadline while {}",
            phase.get()
        )
    });
}
