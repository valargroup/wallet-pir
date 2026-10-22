use super::*;
use crate::test_support::{records, seed_published_fixture};

/// Deterministic bytes for fixed-input regression vectors.
#[cfg(not(feature = "native-reinspiring"))]
fn stream(label: &[u8], len: usize) -> Vec<u8> {
    (0u64..)
        .flat_map(|i| {
            Sha256::new()
                .chain_update(label)
                .chain_update(i.to_le_bytes())
                .finalize()
        })
        .take(len)
        .collect()
}

/// Pins public material and the packed answer for fixed records, setup
/// seed and query bytes. The digests were captured on the base commit
/// before the shared runtime extraction; they must never change.
#[cfg(not(feature = "native-reinspiring"))]
#[test]
fn fixed_inputs_pin_public_material_and_answer() {
    let dir = tempfile::tempdir().unwrap();
    let coverage = Lifecycle::default()
        .coverage(67, Geometry::default())
        .unwrap();
    let plan = plan(coverage.shards[0].clone(), records).unwrap();
    // The 67-record publication is the same immutable predecessor other
    // failure tests build. Reuse those bytes; this case still checks the
    // pinned digests and packed answer.
    seed_published_fixture(dir.path(), &plan);
    let mut engine = Engine::new(dir.path());
    let (eval, _) = engine.prepare(plan, records).unwrap();
    let hint = eval.hint().unwrap();
    let pack = Packing::new(4096, &hint, &crate::PackingBudget::coordinator()).unwrap();
    let mut hint_digest = Sha256::new();
    for value in hint.iter().flat_map(|b| b.rows.iter().flatten()) {
        hint_digest.update(value.to_le_bytes());
    }
    let binding = QueryBinding {
        generation: 1,
        shard_id: 0,
        epoch: Sha256::digest(&pack.public)[..8].try_into().unwrap(),
        recovery_epoch: 0,
        session_id: [3; 32],
        request_id: [4; 16],
        anchor_hash: [5; 32],
    };
    let q = rlwe().q;
    let keys: Vec<u64> = stream(b"keys", PackingKeys::encoded_len(rlwe()) * 2)
        .chunks_exact(8)
        .map(|c| u64::from_le_bytes(c.try_into().unwrap()) % q)
        .collect();
    let mut body = binding.encode();
    let keys =
        ipir_sp::bits::u64s_to_contiguous_bytes(&keys, ipir_sp::modulus_switch::modulus_bits(q));
    body.extend(&keys[..PackingKeys::encoded_len(rlwe())]);
    body.extend(stream(
        b"query",
        (pack.params.db_rows * pack.params.query_bits).div_ceil(8),
    ));
    let rows = pack.params.db_rows;
    let coefficients = pack.query_coefficients(&body, binding, rows).unwrap();
    let answer = pack
        .pack(&body, &eval.evaluate(&coefficients).unwrap(), rows)
        .unwrap();
    assert_eq!(
        [
            hex::encode(hint_digest.finalize()),
            hex::encode(Sha256::digest(&pack.public)),
            hex::encode(Sha256::digest(&answer)),
        ],
        [
            "03c4b6b973673c506b1f12f18605daddbab033ec3c40a4a055fac538a833e361",
            "89b1ad472659862a987130688c0e967189695e66d1eba518c2b950938f9f84df",
            "045515ff10f14273c2a4118093407e9563add14410c8ad2392d5e277ae60db20",
        ]
    );
}

#[test]
fn encrypted_bootstrap_round_trip_and_cached_restart() {
    round_trip_and_restart(MatvecConfig::default());
}

#[cfg(feature = "cuda")]
#[test]
#[ignore = "requires NVIDIA GPU and NVRTC"]
fn cuda_encrypted_bootstrap_round_trip_and_cached_restart() {
    let backend = MatvecConfig {
        matvec_backend: crate::matvec::Backend::Cuda,
        cuda_device: None,
    };
    backend.validate().unwrap();
    round_trip_and_restart(backend);
}

/// A unit spec whose data would run past its used rows is refused, since
/// native queries omit the selection over every later row.
#[test]
fn unit_data_must_end_at_its_used_rows() {
    let g = Geometry::default();
    let coverage = Lifecycle::default()
        .coverage(32768 * RECORDS_PER_ROW as u64 + 100, g)
        .unwrap();
    let shard = &coverage.shards[1];
    assert!(shard.composed());
    let tail = MutableUnit {
        local_row_start: 4096,
        used_rows: 4096,
        allocated_rows: 4096,
    };
    assert!(unit_rows(shard, &tail, &mut records).is_ok());
    let short = MutableUnit {
        used_rows: 100,
        ..tail
    };
    assert!(unit_rows(shard, &short, &mut records).is_err());
}

/// Partial, misaligned and Status layouts are extra properties of the
/// same 67-record runtime the bootstrap round trip already prepares.
#[cfg(feature = "qualification-tests")]
#[test]
fn hint_artifact_excludes_partial_misaligned_and_status_layouts() {
    let dir = tempfile::tempdir().unwrap();
    let coverage = Lifecycle::default()
        .coverage(67, Geometry::default())
        .unwrap();
    let domain = plan(coverage.shards[0].clone(), records).unwrap();
    seed_published_fixture(dir.path(), &domain);
    let eval = Engine::new(dir.path())
        .prepare(domain.clone(), records)
        .unwrap()
        .0;
    assert!(eval.verified_hint_artifact().unwrap().is_some());
    // These layouts must keep this runtime's hint. An independent build of
    // the same rows is the 67-record oracle.
    let expected = eval.hint().unwrap();
    let mut partial = domain.clone();
    partial.shard = Lifecycle::default()
        .coverage(8192 * RECORDS_PER_ROW as u64 + 17, Geometry::default())
        .unwrap()
        .shards[0]
        .clone();
    for plan in [partial, {
        let mut misaligned = domain.clone();
        misaligned.units[0].local_row_start = 1;
        misaligned.shard.units[0].local_row_start = 1;
        misaligned
    }] {
        let fallback = Evaluation {
            plan,
            units: eval.units.clone(),
        };
        assert!(fallback.verified_hint_artifact().unwrap().is_none());
        // Preserve the generic partial contribution, with no new rewrite.
        assert_eq!(fallback.hint().unwrap(), expected);
    }

    // A real persisted unit with the Status row layout (12288-byte rows,
    // `status_query_server::protocol::ROW_BYTES`) has a different output width, even though
    // its degree and plaintext profile agree.
    let layout = DatabaseLayout {
        record_bytes: 12288,
        records_per_row: 1,
        shard_rows: domain.units[0].allocated_rows as usize,
        pir_profile: ipir_sp::SimplePirProfile::P16Q48,
    };
    let (_, params) = crate::ipir::shard_parameters(&layout).unwrap();
    #[cfg(not(feature = "native-reinspiring"))]
    let setup = ipir_sp::IPIRClient::from_profile(
        params.num_items,
        params.item_size_bits,
        layout.pir_profile,
    )
    .unwrap()
    .generate_public_query_setup_simplepir_from_seed(setup_seed(domain.shard.id))
    .polys()
    .to_vec();
    #[cfg(feature = "native-reinspiring")]
    let setup = crate::native::public_query_masks(
        setup_seed(domain.shard.id),
        params.db_rows,
        params.db_cols,
    );
    let status_rows = vec![0; layout.shard_bytes()];
    let mut status_domain = domain;
    status_domain.units[0].content_sha256 = hex::encode(Sha256::digest(&status_rows));
    let status = PreparedShard::build(
        &layout,
        status_domain.shard.id,
        0,
        status_domain.units[0].content_sha256.clone(),
        &status_rows,
        rlwe(),
        &setup,
    )
    .unwrap()
    .persist(
        &dir.path().join("status-layout"),
        DatabaseId::Enhance,
        &layout,
        rlwe(),
    )
    .unwrap();
    let unsupported = Evaluation {
        plan: status_domain,
        units: vec![Arc::new(status)],
    };
    assert!(unsupported.verified_hint_artifact().unwrap().is_none());
    assert!(
        unsupported.hint().is_err(),
        "generic shape refusal is unchanged"
    );
}

fn round_trip_and_restart(backend: MatvecConfig) {
    let dir = tempfile::tempdir().unwrap();
    let coverage = Lifecycle::default()
        .coverage(67, Geometry::default())
        .unwrap();
    let plan = plan(coverage.shards[0].clone(), records).unwrap();
    let mut engine = Engine::with_backend(dir.path(), backend);
    let (eval, outcome) = engine.prepare(plan.clone(), records).unwrap();
    assert_eq!(outcome.built, eval.units.len());
    assert_eq!((outcome.reused, outcome.restored), (0, 0));
    let pack = Packing::new(
        4096,
        &eval.hint().unwrap(),
        &crate::PackingBudget::coordinator(),
    )
    .unwrap();
    let manifest = Manifest {
        recovery_epoch: 0,
        placement_revision: 0,
        domain_recovery_epochs: [(0, "0".into())].into(),
        schema_version: SCHEMA_VERSION,
        protocol_revision: PROTOCOL_REVISION.into(),
        network: "main".into(),
        pool: "ironwood".into(),
        generation: 1,
        anchor_height: 3428143,
        anchor_block_hash: "01".repeat(32),
        geometry: Geometry::default(),
        coverage,
        sessions: vec![pack.reference(0).unwrap()],
        unit_identities: [(0, plan.units.clone())].into(),
    };
    let mut old_manifest = manifest.clone();
    old_manifest.protocol_revision = "ironwood-enhance-pir-v5".into();
    assert!(crate::client::QuerySession::new(&old_manifest, pack.session(&manifest, 0)).is_err());
    let mut old_session = pack.session(&manifest, 0);
    old_session.params.query_bits = 46;
    assert!(crate::client::QuerySession::new(&manifest, old_session).is_err());
    let mut old_plan = plan.clone();
    old_plan.units[0].parameter_id = old_plan.units[0]
        .parameter_id
        .replace(PROTOCOL_REVISION, "ironwood-enhance-pir-v6");
    assert_ne!(old_plan, plan);
    assert!(old_plan.validate().is_err());
    let client = crate::client::QuerySession::new(&manifest, pack.session(&manifest, 0)).unwrap();
    for position in [0, 32, 33, 66] {
        let (query, slot) = client.prepare_position(position).unwrap();
        let binding = QueryBinding::decode(query.body()).unwrap();
        let rows = client.query_rows();
        let old_len = query.body().len() - 4096 * 2 / 8;
        assert!(pack
            .query_coefficients(&query.body()[..old_len], binding, rows)
            .is_err());
        let coefficients = pack
            .query_coefficients(query.body(), binding, rows)
            .unwrap();
        let response = pack
            .pack(query.body(), &eval.evaluate(&coefficients).unwrap(), rows)
            .unwrap();
        // The three-row domain selects over one block. Its shorter upload
        // is the full one's prefix and is answered byte for byte the same;
        // the server takes only the prefix its manifest prescribes.
        #[cfg(feature = "native-reinspiring")]
        {
            assert_eq!(rows, 2048);
            let short = &query.body()[..HEADER_BYTES + crate::native::request_len(rows)];
            // The omitted rows are zero-filled, not the full upload's
            // ciphertexts; they select only zero padding, so the scan is equal.
            let selection = pack.query_coefficients(short, binding, rows).unwrap();
            assert_eq!(selection[..rows], coefficients[..rows]);
            assert!(selection[rows..].iter().all(|&c| c == 0));
            let scan = eval.evaluate(&selection).unwrap();
            assert_eq!(scan, eval.evaluate(&coefficients).unwrap());
            assert_eq!(pack.pack(short, &scan, rows).unwrap(), response);
            assert!(pack.query_coefficients(short, binding, 4096).is_err());
        }
        let row = client.decode(query, &response).unwrap();
        assert_eq!(
            &row[slot * RECORD_BYTES..(slot + 1) * RECORD_BYTES],
            records(position, 1).unwrap()
        );
    }
    let (again, outcome) = engine
        .prepare(plan.clone(), |_, _| {
            panic!("cached data must not be reread")
        })
        .unwrap();
    assert!(Arc::ptr_eq(&eval.units[0], &again.units[0]));
    assert_eq!(outcome.reused, again.units.len());
    drop(again);
    drop(eval);
    let mut restarted = Engine::with_backend(dir.path(), backend);
    let (restored, outcome) = restarted
        .prepare(plan.clone(), |_, _| {
            Err("canonical history unavailable".into())
        })
        .unwrap();
    assert_eq!(outcome.restored, restored.units.len());
    // CPU can reopen a GPU-created artifact, and CUDA can open the same CPU-readable bytes.
    let selected_eval = restored;
    let cpu_eval = if backend.matvec_backend == crate::matvec::Backend::Cpu {
        selected_eval.clone()
    } else {
        Engine::new(dir.path())
            .prepare(plan, |_, _| panic!("artifact must be reusable"))
            .unwrap()
            .0
    };
    let query = vec![modulus() - 1; 4096];
    if backend.matvec_backend != crate::matvec::Backend::Cpu {
        assert_eq!(
            cpu_eval.evaluate(&query).unwrap(),
            selected_eval.evaluate(&query).unwrap()
        );
    }
    std::thread::scope(|scope| {
        for _ in 0..4 {
            let eval = &selected_eval;
            let query = &query;
            let expected = cpu_eval.evaluate(query).unwrap();
            scope.spawn(move || assert_eq!(eval.evaluate(query).unwrap(), expected));
        }
    });
    drop(cpu_eval);
    drop(selected_eval);
    restarted.collect_unused().unwrap();
    assert_eq!(restarted.live_bytes(), 0);
}

#[test]
fn cached_backend_failure_does_not_read_rows_or_replace_published_units() {
    use crate::matvec::testing::{scoped, Faults};
    use std::sync::atomic::Ordering::SeqCst;
    let root = tempfile::tempdir().unwrap();
    let domain = plan(
        Lifecycle::default()
            .coverage(67, Geometry::default())
            .unwrap()
            .shards[0]
            .clone(),
        records,
    )
    .unwrap();
    seed_published_fixture(root.path(), &domain);
    let published = Engine::new(root.path())
        .prepare(domain.clone(), records)
        .unwrap()
        .0;
    let query = vec![1; 4096];
    let before = published.evaluate(&query).unwrap();
    let faults = Arc::new(Faults::default());
    faults.fail_prepare_at.store(1, SeqCst);
    let mut failed = Engine::new(root.path());
    let error = scoped(&faults, || {
        failed.prepare(domain, |_, _| {
            panic!("a backend failure must not reread canonical rows")
        })
    });
    assert!(matches!(error, Err(PrepareError::Runtime(_))));
    assert_eq!(faults.prepare_calls.load(SeqCst), 1);
    assert_eq!(faults.live_bytes.load(SeqCst), 0);
    assert_eq!(failed.live_bytes(), 0);
    assert_eq!(published.evaluate(&query).unwrap(), before);
}

#[cfg(feature = "cuda")]
#[test]
fn backend_load_failure_does_not_rebuild_or_replace_published_units() {
    let root = tempfile::tempdir().unwrap();
    let coverage = Lifecycle::default()
        .coverage(67, Geometry::default())
        .unwrap();
    let plan = plan(coverage.shards[0].clone(), records).unwrap();
    let mut cpu = Engine::new(root.path());
    let (published, _) = cpu.prepare(plan.clone(), records).unwrap();
    let query = vec![1; 4096];
    let before = published.evaluate(&query).unwrap();
    let mut failed = Engine::with_backend(
        root.path(),
        MatvecConfig {
            matvec_backend: crate::matvec::Backend::Cuda,
            cuda_device: Some(usize::MAX),
        },
    );
    assert!(matches!(
        failed.prepare(plan, |_, _| panic!("device failure must not rebuild")),
        Err(PrepareError::Runtime(_))
    ));
    assert_eq!(failed.live_bytes(), 0);
    assert_eq!(published.evaluate(&query).unwrap(), before);
}

#[test]
fn partial_preparation_failure_preserves_published_and_retries() {
    partial_preparation_failure(MatvecConfig::default(), false);
}

// Fast checks cover partial cleanup on a cold candidate and refusal to
// rebuild a cached unit after a backend error independently. Qualification
// retains their combination at the second cached unit.
#[cfg(feature = "qualification-tests")]
#[test]
fn cached_partial_preparation_failure_preserves_published_and_retries() {
    partial_preparation_failure(MatvecConfig::default(), true);
}

#[cfg(feature = "cuda")]
#[test]
#[ignore = "requires NVIDIA GPU and NVRTC"]
fn cuda_partial_preparation_failure_preserves_published_and_retries() {
    for cached in [false, true] {
        partial_preparation_failure(
            MatvecConfig {
                matvec_backend: crate::matvec::Backend::Cuda,
                cuda_device: None,
            },
            cached,
        );
    }
}

fn partial_preparation_failure(backend: MatvecConfig, cached: bool) {
    use crate::matvec::testing::{scoped, Faults};
    use std::sync::atomic::Ordering::SeqCst;
    let root = tempfile::tempdir().unwrap();
    let old_plan = plan(
        Lifecycle::default()
            .coverage(67, Geometry::default())
            .unwrap()
            .shards[0]
            .clone(),
        records,
    )
    .unwrap();
    // Both fixtures prepare two new units and fail on the second.
    // The valid composed tail uses smaller 4K/2K units; qualification
    // also exercises the original ordinary 8K/2K domain.
    let geometry = Geometry::default();
    let shard = if cfg!(feature = "qualification-tests") || cfg!(feature = "cuda") {
        Lifecycle::default()
            .coverage(8192 * 33 + 17, geometry)
            .unwrap()
            .shards[0]
            .clone()
    } else {
        Lifecycle::default()
            .coverage(
                geometry.max_shard_rows * RECORDS_PER_ROW as u64 + 17,
                geometry,
            )
            .unwrap()
            .shards[1]
            .clone()
    };
    let candidate = plan(shard, records).unwrap();
    assert_eq!(candidate.units.len(), 2);
    assert!(candidate.units.iter().all(|u| !old_plan.units.contains(u)));
    seed_published_fixture(root.path(), &old_plan);
    let mut engine = Engine::with_backend(root.path(), backend);
    let faults = Arc::new(Faults::default());
    let published = scoped(&faults, || engine.prepare(old_plan, records))
        .unwrap()
        .0;
    let query = vec![1; published.plan.shard.logical_rows as usize];
    let before = published.evaluate(&query).unwrap();
    let published_sizes = engine.live_sizes();
    let published_bytes = faults.live_bytes.load(SeqCst);
    if cached {
        // Populate candidate artifacts through an independent CPU engine.
        let mut cache = Engine::new(root.path());
        drop(cache.prepare(candidate.clone(), records).unwrap());
    }
    faults
        .fail_prepare_at
        .store(faults.prepare_calls.load(SeqCst) + 2, SeqCst);
    let mut reads = 0;
    let failed = scoped(&faults, || {
        engine.prepare(candidate.clone(), |start, count| {
            assert!(
                !cached,
                "backend failure must not rebuild a cached artifact"
            );
            reads += 1;
            records(start, count)
        })
    });
    assert!(failed.is_err());
    assert_eq!(
        faults.prepare_calls.load(SeqCst),
        3,
        "must fail after preparing two candidate units"
    );
    assert_eq!(reads, if cached { 0 } else { 2 });
    assert_eq!(
        faults.live_bytes.load(SeqCst),
        published_bytes,
        "partial candidate kernels must be dropped"
    );
    assert_eq!(engine.live_sizes(), published_sizes);
    assert_eq!(published.evaluate(&query).unwrap(), before);
    faults.fail_prepare_at.store(0, SeqCst);
    let retry = scoped(&faults, || {
        engine.prepare(candidate.clone(), |start, count| {
            assert!(!cached, "cached retry must not require canonical history");
            records(start, count)
        })
    })
    .unwrap()
    .0;
    // The all-one query is the column sum of the committed raw rows.
    // Use plain integer addition as an independent oracle rather than
    // preparing a second copy through the same native implementation.
    let columns = parameters(candidate.shard.logical_rows).unwrap().db_cols;
    let mut expected = vec![0u64; columns];
    for spec in &candidate.shard.units {
        let rows = unit_rows(&candidate.shard, spec, &mut records).unwrap();
        for row in rows.chunks_exact(ENHANCE_LAYOUT.row_bytes()) {
            for (sum, bytes) in expected.iter_mut().zip(row.chunks_exact(2)) {
                *sum += u64::from(u16::from_le_bytes(bytes.try_into().unwrap()));
            }
        }
    }
    for value in &mut expected {
        *value %= modulus();
    }
    let candidate_query = vec![1; retry.plan.shard.logical_rows as usize];
    assert_eq!(retry.evaluate(&candidate_query).unwrap(), expected);
    assert_eq!(published.evaluate(&query).unwrap(), before);
    drop(retry);
    engine.collect_unused().unwrap();
    assert_eq!(engine.live_sizes(), published_sizes);
    assert_eq!(faults.live_bytes.load(SeqCst), published_bytes);
    drop(published);
    engine.collect_unused().unwrap();
    assert_eq!(faults.live_bytes.load(SeqCst), 0);
}

impl Evaluation {
    /// Borrow the existing publication only when the generic combiner is the
    /// identity. Partial and composed domains, including the 8K tail, return
    /// none and still evaluate through the summed units.
    fn verified_hint_artifact(
        &self,
    ) -> Result<Option<crate::artifact::PublicationArtifact>, String> {
        let ([unit], [identity], [spec]) = (
            self.units.as_slice(),
            self.plan.units.as_slice(),
            self.plan.shard.units.as_slice(),
        ) else {
            return Ok(None);
        };
        let params = parameters(self.plan.shard.logical_rows)?;
        let runtime = unit.runtime();
        let local = runtime.server.params();
        let layout = DatabaseLayout {
            shard_rows: identity.allocated_rows as usize,
            ..ENHANCE_LAYOUT
        };
        let (ring, expected) = crate::ipir::shard_parameters(&layout).map_err(|e| e.to_string())?;
        if self.plan.validate().is_err()
            || self.plan.shard.composed()
            || spec.local_row_start != 0
            || identity.allocated_rows > self.plan.shard.logical_rows
            || !layout.shard_rows.is_multiple_of(rlwe().d)
            || runtime.query_row_start != 0
            || runtime.shard_id != self.plan.shard.id
            || runtime.rows_sha256 != identity.content_sha256
            || ring.d != rlwe().d
            || ring.q != rlwe().q
            || local.db_rows != expected.db_rows
            || local.db_cols != expected.db_cols
            || local.p != expected.p
            || local.item_size_bits != expected.item_size_bits
            || local.query_bits != expected.query_bits
            || local.q_prime_1 != expected.q_prime_1
            || local.db_cols != params.db_cols
            || local.p != params.p
        {
            return Ok(None);
        }
        unit.publication()
            .validate(params.db_cols / rlwe().d, rlwe().d)
            .map_err(|e| e.to_string())?;
        Ok(Some(unit.publication().clone()))
    }
}

// One supported 8K composed tail, including the zero-filled gap, against
// monolithic preprocessing. Larger tail-growth campaigns stay in qualification.
#[test]
fn unequal_units_match_monolithic_preprocessing_and_evaluation() {
    let directory = tempfile::tempdir().unwrap();
    let geometry = Geometry::default();
    let coverage = Lifecycle::default()
        .coverage(
            geometry.max_shard_rows * RECORDS_PER_ROW as u64 + 17,
            geometry,
        )
        .unwrap();
    // One composed tail exercises unequal units, A's suffix and a padded gap.
    let shard = coverage.shards[1].clone();
    let monolithic_rows = 8192;
    let expected_units = [2048, 4096];
    let targets = [0, 2047, 2048, 4095, 4096, 8191];
    let domain = plan(shard.clone(), records).unwrap();
    assert_eq!(
        domain
            .units
            .iter()
            .map(|u| u.allocated_rows)
            .collect::<Vec<_>>(),
        expected_units
    );
    let mut engine = Engine::new(directory.path());
    let evaluation = engine.prepare(domain, records).unwrap().0;
    assert!(evaluation.verified_hint_artifact().unwrap().is_none());
    // Assemble the whole table independently of unit_rows: B's own records
    // start at local zero, and a composed tail appends A's final 4K rows.
    let row_bytes = ENHANCE_LAYOUT.row_bytes();
    let mut rows = vec![0; monolithic_rows * row_bytes];
    let own = records(
        shard.global_row_start * RECORDS_PER_ROW as u64,
        shard.records as usize,
    )
    .unwrap();
    rows[..own.len()].copy_from_slice(&own);
    if shard.composed() {
        let suffix = records(
            (shard.global_row_start - geometry.min_shard_rows) * RECORDS_PER_ROW as u64,
            (geometry.min_shard_rows * RECORDS_PER_ROW as u64) as usize,
        )
        .unwrap();
        let at = geometry.min_shard_rows as usize * row_bytes;
        rows[at..at + suffix.len()].copy_from_slice(&suffix);
    }
    // Native masks are expanded once at full size for every logical size.
    #[cfg(feature = "native-reinspiring")]
    let setup = crate::native::query_masks(shard.id);
    #[cfg(not(feature = "native-reinspiring"))]
    let setup = {
        let params = parameters(monolithic_rows as u64).unwrap();
        let client = ipir_sp::IPIRClient::from_profile(
            params.num_items,
            params.item_size_bits,
            ipir_sp::SimplePirProfile::P16Q48,
        )
        .unwrap();
        client
            .generate_public_query_setup_simplepir_from_seed(setup_seed(shard.id))
            .polys()
            .to_vec()
    };
    let monolithic = PreparedShard::build(
        &DatabaseLayout {
            shard_rows: monolithic_rows,
            ..ENHANCE_LAYOUT
        },
        shard.id,
        0,
        "test".into(),
        &rows,
        rlwe(),
        &setup,
    )
    .unwrap();
    assert_eq!(evaluation.hint().unwrap(), monolithic.crs_blocks().to_vec());
    for target in targets {
        let mut query = vec![0; monolithic_rows];
        query[target] = modulus() - 1;
        assert_eq!(
            evaluation.evaluate(&query).unwrap(),
            monolithic.runtime().evaluate(rlwe(), &query).unwrap()
        );
    }
}

#[cfg(feature = "qualification-tests")]
#[test]
fn tail_domain_reuses_b_units_and_decodes_b_at_unchanged_coordinates() {
    use crate::ROW_BYTES;
    #[cfg(not(feature = "native-reinspiring"))]
    use ipir_sp::modulus_switch::recover_published_c1;
    #[cfg(not(feature = "native-reinspiring"))]
    use ipir_sp::serialize::serialize_packing_keys;

    const B_START: u64 = 32768;
    const B_ROWS: u64 = 2049;
    const B_SLOT: u64 = 4096;
    const SUFFIX_ROWS: u64 = B_SLOT;
    let directory = tempfile::tempdir().unwrap();
    let shard = QueryShard {
        id: 7,
        global_row_start: B_START,
        records: B_ROWS * RECORDS_PER_ROW as u64,
        logical_rows: 4096,
        state: ShardState::Growing,
        units: Geometry::default()
            .units(B_ROWS * RECORDS_PER_ROW as u64)
            .unwrap(),
    };
    let b_plan = plan(shard, records).unwrap();
    assert_eq!(
        b_plan
            .units
            .iter()
            .map(|unit| (unit.local_row_start, unit.allocated_rows))
            .collect::<Vec<_>>(),
        [(0, B_SLOT)]
    );

    // Prepare B independently in both contexts; sharing an Arc here would
    // make the artifact comparison circular.
    let plain_root = directory.path().join("plain");
    let tail_root = directory.path().join("tail");
    let plain = Engine::new(&plain_root)
        .prepare(b_plan.clone(), records)
        .unwrap()
        .0;
    let b_in_tail = Engine::new(&tail_root)
        .prepare(b_plan.clone(), records)
        .unwrap()
        .0;
    assert_eq!(plain.plan.units, b_in_tail.plan.units);
    for identity in &b_plan.units {
        let relative = PathBuf::from("artifacts-9")
            .join(identity.digest())
            .join("enhance")
            .join("shard-00000007");
        for artifact in ["metadata.json", "database.u16le", "partial-crs.bin"] {
            assert_eq!(
                std::fs::read(plain_root.join(&relative).join(artifact)).unwrap(),
                std::fs::read(tail_root.join(&relative).join(artifact)).unwrap(),
                "B artifact {artifact} differs"
            );
        }
    }
    for (left, right) in plain.units.iter().zip(&b_in_tail.units) {
        let mut left_hint = Vec::new();
        let mut right_hint = Vec::new();
        std::io::Read::read_to_end(&mut left.publication().reader(), &mut left_hint).unwrap();
        std::io::Read::read_to_end(&mut right.publication().reader(), &mut right_hint).unwrap();
        assert_eq!(left_hint, right_hint, "B partial hints differ");
    }

    // A's suffix occupies a new B-seeded unit after B's entire slot.
    let layout = DatabaseLayout {
        shard_rows: SUFFIX_ROWS as usize,
        ..ENHANCE_LAYOUT
    };
    let suffix = records(9_000_000, (SUFFIX_ROWS * RECORDS_PER_ROW as u64) as usize).unwrap();
    #[cfg(feature = "native-reinspiring")]
    let setup = crate::native::query_masks(7);
    #[cfg(not(feature = "native-reinspiring"))]
    let setup = {
        let params = parameters(32768).unwrap();
        let setup_client = ipir_sp::IPIRClient::from_profile(
            params.num_items,
            params.item_size_bits,
            ipir_sp::SimplePirProfile::P16Q48,
        )
        .unwrap();
        setup_client
            .generate_public_query_setup_simplepir_from_seed(setup_seed(7))
            .polys()
            .to_vec()
    };
    let suffix_hash = hex::encode(Sha256::digest(&suffix));
    let extra = PreparedShard::build(
        &layout,
        7,
        B_SLOT as usize,
        suffix_hash.clone(),
        &suffix,
        rlwe(),
        &setup,
    )
    .unwrap()
    .persist(
        &directory.path().join("suffix"),
        DatabaseId::Enhance,
        &layout,
        rlwe(),
    )
    .unwrap();
    let mut tail_plan = b_plan.clone();
    tail_plan.shard.logical_rows = 8192;
    tail_plan.units.push(UnitIdentity {
        recovery_epoch: 0,
        table: "enhance".into(),
        shard_id: 7,
        local_row_start: B_SLOT,
        allocated_rows: SUFFIX_ROWS,
        setup_sha256: hex::encode(Sha256::digest(setup_seed(7))),
        parameter_id: unit_parameter_id(SUFFIX_ROWS).unwrap(),
        content_sha256: suffix_hash,
    });
    let mut tail_units = b_in_tail.units.clone();
    tail_units.push(Arc::new(extra));
    let tail = Evaluation {
        plan: tail_plan,
        units: tail_units,
    };
    assert_eq!(&tail.plan.units[..b_plan.units.len()], b_plan.units);

    let plain_hint = plain.hint().unwrap();
    let tail_hint = tail.hint().unwrap();
    assert_ne!(
        plain_hint, tail_hint,
        "the whole-domain hint includes A's suffix"
    );
    for (evaluation, hint, logical_rows) in
        [(&*plain, &plain_hint, 4096), (&tail, &tail_hint, 8192)]
    {
        let packing =
            Packing::new(logical_rows, hint, &crate::PackingBudget::coordinator()).unwrap();
        let params = parameters(logical_rows).unwrap();
        #[cfg(not(feature = "native-reinspiring"))]
        let (wallet, setup, public) = {
            let wallet = ipir_sp::IPIRClient::from_profile(
                params.num_items,
                params.item_size_bits,
                ipir_sp::SimplePirProfile::P16Q48,
            )
            .unwrap();
            let setup = wallet.generate_public_query_setup_simplepir_from_seed(setup_seed(7));
            let public = recover_published_c1(
                &packing.public,
                rlwe().d,
                params.db_cols / rlwe().d,
                rlwe().q,
            );
            (wallet, setup, public)
        };
        let binding = QueryBinding {
            recovery_epoch: 0,
            session_id: [7; 32],
            request_id: [8; 16],
            anchor_hash: [9; 32],
            generation: 1,
            shard_id: 7,
            epoch: Sha256::digest(&packing.public)[..8].try_into().unwrap(),
        };
        let mut targets = vec![0, B_ROWS as usize - 2, B_ROWS as usize - 1];
        if logical_rows == 8192 {
            targets.push(B_SLOT as usize);
        }
        for row in targets {
            let mut body = binding.encode();
            #[cfg(feature = "native-reinspiring")]
            let secret = {
                let (secret, upload) = crate::native::prepare(7, params.db_rows, row).unwrap();
                body.extend(upload);
                secret
            };
            #[cfg(not(feature = "native-reinspiring"))]
            let seed = {
                let (query, keys, seed) = wallet.generate_fresh_query_simplepir(&setup, row);
                body.extend(serialize_packing_keys(wallet.rlwe_params(), &keys).unwrap());
                body.extend(query.to_switched_bytes(rlwe().q, params.query_bits));
                seed
            };
            let coefficients = packing
                .query_coefficients(&body, binding, params.db_rows)
                .unwrap();
            let response = packing
                .pack(
                    &body,
                    &evaluation.evaluate(&coefficients).unwrap(),
                    params.db_rows,
                )
                .unwrap();
            assert_eq!(QueryBinding::decode(&response).unwrap(), binding);
            #[cfg(feature = "native-reinspiring")]
            let decoded =
                crate::native::decode(&secret, &packing.public, &response[HEADER_BYTES..]).unwrap();
            #[cfg(not(feature = "native-reinspiring"))]
            let decoded =
                wallet.decode_response_simplepir(&seed, &public, &response[HEADER_BYTES..]);
            let start = if row == B_SLOT as usize {
                9_000_000
            } else {
                (B_START + row as u64) * RECORDS_PER_ROW as u64
            };
            assert_eq!(
                &decoded[..ROW_BYTES],
                records(start, RECORDS_PER_ROW).unwrap(),
                "row {row} decoded at the wrong local coordinate in {logical_rows} rows"
            );
        }
    }
}
