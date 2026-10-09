//! Independent wallet implementation must consume real server q48 material.
use enhance_pir::status as server;
use enhance_pir_server::status::{fixture, now_ms, Generation};
use ipir_sp::server::MatvecBackend;
#[cfg(not(feature = "native-reinspiring"))]
use zakura_pir_status as wallet;

/// In-memory stand-in for the HTTPS origin: the wallet library only speaks
/// through its typed transport, so the server generation answers behind it.
#[cfg(not(feature = "native-reinspiring"))]
struct InProcess {
    g: std::sync::Arc<Generation>,
    manifest: Vec<u8>,
    bodies: std::sync::Mutex<Vec<Vec<u8>>>,
}

#[cfg(not(feature = "native-reinspiring"))]
impl wallet::transport::Transport for InProcess {
    async fn get(&self, url: &str, max_bytes: usize) -> Result<Vec<u8>, wallet::Error> {
        let bytes = if url.ends_with("/v1/status/init") {
            self.manifest.clone()
        } else if url.ends_with(&format!(
            "/v1/status/session/{}",
            hex::encode(self.g.manifest.id())
        )) {
            self.g.public.to_vec()
        } else {
            return Err(wallet::Error::Unavailable);
        };
        assert!(bytes.len() <= max_bytes, "wallet limit below served length");
        Ok(bytes)
    }
    async fn post(
        &self,
        url: &str,
        body: Vec<u8>,
        max_bytes: usize,
    ) -> Result<Vec<u8>, wallet::Error> {
        assert!(url.ends_with("/v1/status/query"));
        let coefficients = self
            .g
            .coefficients(&body)
            .map_err(|_| wallet::Error::Unavailable)?;
        let values = self
            .g
            .evaluate(&coefficients)
            .map_err(|_| wallet::Error::Unavailable)?;
        let response = self
            .g
            .pack(&body, &values)
            .map_err(|_| wallet::Error::Unavailable)?;
        self.bodies.lock().unwrap().push(body);
        assert!(
            response.len() <= max_bytes,
            "wallet limit below served length"
        );
        Ok(response)
    }
}

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
    assert_eq!(server::SKEW_MS, wallet::MAX_FUTURE_SKEW_MS);
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
    let origin = InProcess {
        manifest: serde_json::to_vec(&g.manifest).unwrap(),
        g: std::sync::Arc::new(g),
        bodies: Default::default(),
    };
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let base = "https://status.invalid";
    // Wallet-owned anchors: an exact anchor and a recent-window verifier both
    // accept the served anchor; a foreign anchor is refused before session setup.
    let exact = wallet::AcceptedAnchor {
        network: snapshot.network,
        height: snapshot.height,
        hash: snapshot.anchor,
    };
    let window = wallet::AcceptedAnchors {
        network: snapshot.network,
        known: vec![
            (snapshot.height.saturating_sub(1), [9; 32]),
            (snapshot.height, snapshot.anchor),
        ],
    };
    let foreign = wallet::AcceptedAnchor {
        network: snapshot.network,
        height: snapshot.height,
        hash: [7; 32],
    };
    let client = runtime.block_on(async {
        let pending = wallet::transport::PendingClient::fetch(&origin, base, now_ms)
            .await
            .unwrap();
        assert!(pending.accept(&origin, &foreign).await.is_err());
        let pending = wallet::transport::PendingClient::fetch(&origin, base, now_ms)
            .await
            .unwrap();
        pending.accept(&origin, &window).await.unwrap();
        let pending = wallet::transport::PendingClient::fetch(&origin, base, now_ms)
            .await
            .unwrap();
        pending.accept(&origin, &exact).await.unwrap()
    });
    let coverage = wallet::LocalCoverageContext {
        earliest_possible_inclusion: Some(snapshot.start),
        required_through: Some(snapshot.height),
    };
    for (txid, expected) in fixture::cases(entries, false) {
        let actual = runtime
            .block_on(client.observe(&origin, &txid, coverage))
            .unwrap();
        let expected = match expected {
            server::Observation::Mined(h) => wallet::Observation::Mined(h),
            server::Observation::Mempool => wallet::Observation::Mempool,
            server::Observation::Forked => wallet::Observation::Forked,
            server::Observation::NotFound => wallet::Observation::NotFound,
        };
        assert_eq!(actual, expected);
    }
    let observed = client.manifest().observed_ms;
    assert!(client.manifest().fresh(observed + 20_000).is_ok());
    assert_eq!(
        client.manifest().fresh(observed + 20_001),
        Err(wallet::Error::Stale)
    );
    assert!(client
        .manifest()
        .fresh(observed.saturating_sub(4_000))
        .is_ok());
    assert_eq!(
        client.manifest().fresh(observed.saturating_sub(6_000)),
        Err(wallet::Error::Malformed)
    );
    // Fresh randomness per request; stale-envelope and cross-session replays are refused.
    let txid = fixture::txid(0);
    runtime
        .block_on(client.observe(&origin, &txid, coverage))
        .unwrap();
    runtime
        .block_on(client.observe(&origin, &txid, coverage))
        .unwrap();
    let bodies = origin.bodies.lock().unwrap();
    let (a, b) = (&bodies[bodies.len() - 2], &bodies[bodies.len() - 1]);
    assert_ne!(a, b);
    let mut old_envelope = a.clone();
    old_envelope[..4].copy_from_slice(b"SPQ1");
    assert!(origin.g.coefficients(&old_envelope).is_err());
    let mut other_session = a.clone();
    other_session[4] ^= 1;
    assert!(origin.g.coefficients(&other_session).is_err());
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

/// Status servers accept a 44-bit dithered selection in the same envelope as
/// the 49-bit one the in-repo client sends, by its exact length, and answer it
/// with the published rows. Every other length is refused.
#[cfg(feature = "native-reinspiring")]
#[test]
fn native_server_answers_dithered_queries_in_the_same_envelope() {
    use enhance_pir::native as n;
    let snapshot = fixture::snapshot(32, false).unwrap();
    let (g, _) = Generation::prepare(&snapshot, 1, 1, now_ms(), None, MatvecBackend::Cpu).unwrap();
    let setup = server::native_packing_setup(&g.manifest.network, &g.manifest.salt);
    let masks = server::native_query_masks(&g.manifest.network, &g.manifest.salt);
    let mut header = server::QUERY_MAGIC.to_vec();
    header.extend(g.manifest.id());
    header.extend([7u8; 16]);
    assert_eq!(header.len(), server::HEADER_BYTES);
    for row in [0, 1_234, server::ROWS - 1] {
        let (secret, payload) =
            pir_native::prepare_dithered(&setup, &masks, server::ROWS, row).unwrap();
        let mut body = header.clone();
        body.extend(payload);
        assert_eq!(
            body.len(),
            server::HEADER_BYTES + n::request_len_bits(server::ROWS, n::DITHERED_QUERY_BITS)
        );
        assert_eq!(
            body.len() + server::ROWS * 5 / 8,
            server::HEADER_BYTES + n::request_len(server::ROWS)
        );
        let coefficients = g.coefficients(&body).unwrap();
        let values = g.evaluate(&coefficients).unwrap();
        let response = g.pack(&body, &values).unwrap();
        assert_eq!(
            response[..server::HEADER_BYTES],
            body[..server::HEADER_BYTES]
        );
        let decoded = n::decode_cols(
            &secret,
            &g.public,
            &response[server::HEADER_BYTES..],
            server::COLS,
        )
        .unwrap();
        assert_eq!(
            decoded,
            snapshot.rows[row * server::ROW_BYTES..(row + 1) * server::ROW_BYTES],
            "row {row}"
        );
        for len in [body.len() - 1, body.len() + 1] {
            let mut wrong = body.clone();
            wrong.resize(len, 0);
            assert!(g.coefficients(&wrong).is_err(), "{len} bytes");
            assert!(g.pack(&wrong, &values).is_err(), "{len} bytes");
        }
    }
    // A 47-bit selection, neither accepted width, is refused too.
    let mut other = header.clone();
    other.resize(
        server::HEADER_BYTES + n::request_len_bits(server::ROWS, 47),
        0,
    );
    assert!(g.coefficients(&other).is_err());
}
