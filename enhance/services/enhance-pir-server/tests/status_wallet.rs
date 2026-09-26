//! Independent wallet implementation must consume real server q48 material.
use enhance_pir::status as server;
use enhance_pir_server::status::{fixture, now_ms, Generation};
use ipir_sp::server::MatvecBackend;
#[cfg(not(feature = "native-reinspiring"))]
use zakura_pir_status as wallet;

#[cfg(not(feature = "native-reinspiring"))]
#[test]
fn wallet_and_server_share_wire_identity_and_encrypted_observations() {
    assert_eq!(server::PROTOCOL, "status-pir-v2-q48");
    assert_eq!(server::PROTOCOL, wallet::PROTOCOL);
    assert_eq!(
        (
            wallet::ROWS,
            wallet::SLOTS,
            wallet::SLOT_BYTES,
            wallet::ROW_BYTES
        ),
        (8192, 256, 40, 12288)
    );
    assert_eq!(enhance_pir_server::status::profile().ypir().db_cols, 6144);
    assert_eq!(server::MAX_AGE_MS, wallet::MAX_AGE_MS);
    let entries = std::env::var("STATUS_TEST_ENTRIES")
        .map(|n| n.parse::<usize>().unwrap())
        .unwrap_or(32);
    let backend = if std::env::var_os("STATUS_TEST_CUDA").is_some() {
        MatvecBackend::Cuda { device: 0 }
    } else {
        MatvecBackend::Cpu
    };
    let snapshot = fixture::snapshot(entries, false).unwrap();
    let (g, _) = Generation::prepare(&snapshot, 1, 1, now_ms(), None, backend).unwrap();
    let manifest: wallet::Manifest =
        serde_json::from_value(serde_json::to_value(&g.manifest).unwrap()).unwrap();
    assert_eq!(manifest.id(), g.manifest.id());
    assert_eq!(
        wallet::setup_seed(&snapshot.network, &snapshot.salt),
        server::setup_seed(&snapshot.network, &snapshot.salt)
    );
    let anchor = wallet::AcceptedAnchor {
        network: snapshot.network,
        height: snapshot.height,
        hash: snapshot.anchor,
    };
    let client = wallet::Client::new(manifest, &g.public, &anchor).unwrap();
    let coverage = wallet::LocalCoverageContext {
        earliest_possible_inclusion: Some(snapshot.start),
        required_through: Some(snapshot.height),
    };
    for (txid, expected) in fixture::cases(entries, false) {
        let query = client.prepare(&txid, coverage, now_ms()).unwrap();
        let coefficients = g.coefficients(&query.body).unwrap();
        let values = g.evaluate(&coefficients).unwrap();
        let response = g.pack(&query.body, &values).unwrap();
        let actual = client.decode(query, &response, now_ms()).unwrap();
        let expected = match expected {
            server::Observation::Mined(h) => wallet::Observation::Mined(h),
            server::Observation::Mempool => wallet::Observation::Mempool,
            server::Observation::Forked => wallet::Observation::Forked,
            server::Observation::NotFound => wallet::Observation::NotFound,
        };
        assert_eq!(actual, expected);
    }
    assert!(client
        .manifest
        .fresh(client.manifest.observed_ms + 20_000)
        .is_ok());
    assert_eq!(
        client.manifest.fresh(client.manifest.observed_ms + 20_001),
        Err(wallet::Error::Stale)
    );
    let a = client
        .prepare(&fixture::txid(0), coverage, now_ms())
        .unwrap();
    let b = client
        .prepare(&fixture::txid(0), coverage, now_ms())
        .unwrap();
    assert_ne!(a.body, b.body);
    let mut old_envelope = a.body.clone();
    old_envelope[..4].copy_from_slice(b"SPQ1");
    assert!(g.coefficients(&old_envelope).is_err());
    let mut other_session = a.body.clone();
    other_session[4] ^= 1;
    assert!(g.coefficients(&other_session).is_err());
}

/// The independent wallet implements only q48. The native profile is checked
/// with the in-repo client over the same fixture cases and envelope rejections.
#[cfg(feature = "native-reinspiring")]
#[test]
fn native_client_decodes_real_server_two_mask_material() {
    assert_eq!(server::PROTOCOL, "status-pir-v3-native-two-mask-m29");
    let entries = std::env::var("STATUS_TEST_ENTRIES")
        .map(|n| n.parse::<usize>().unwrap())
        .unwrap_or(32);
    let backend = if std::env::var_os("STATUS_TEST_CUDA").is_some() {
        MatvecBackend::Cuda { device: 0 }
    } else {
        MatvecBackend::Cpu
    };
    let snapshot = fixture::snapshot(entries, false).unwrap();
    let (g, _) = Generation::prepare(&snapshot, 1, 1, now_ms(), None, backend).unwrap();
    assert_eq!(
        g.public.len(),
        enhance_pir::native::public_len(server::COLS)
    );
    let anchor = server::AcceptedAnchor {
        network: snapshot.network,
        height: snapshot.height,
        hash: snapshot.anchor,
    };
    let client = server::Client::new(g.manifest.clone(), &g.public, &anchor).unwrap();
    for (txid, expected) in fixture::cases(entries, false) {
        let query = client
            .prepare(&txid, Some(snapshot.start), now_ms())
            .unwrap();
        assert_eq!(
            query.body.len(),
            server::HEADER_BYTES + enhance_pir::native::request_len(server::ROWS)
        );
        let coefficients = g.coefficients(&query.body).unwrap();
        let values = g.evaluate(&coefficients).unwrap();
        let response = g.pack(&query.body, &values).unwrap();
        assert_eq!(client.decode(query, &response, now_ms()), Ok(expected));
    }
    let a = client
        .prepare(&fixture::txid(0), Some(snapshot.start), now_ms())
        .unwrap();
    let mut q48_envelope = a.body.clone();
    q48_envelope[..4].copy_from_slice(b"SPQ2");
    assert!(g.coefficients(&q48_envelope).is_err());
    let mut other_session = a.body.clone();
    other_session[4] ^= 1;
    assert!(g.coefficients(&other_session).is_err());
    let mut tampered = g.public.to_vec();
    tampered[0] ^= 1;
    assert!(server::Client::new(g.manifest.clone(), &tampered, &anchor).is_err());
}
