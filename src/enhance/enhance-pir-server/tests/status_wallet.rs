//! The native Status profile, checked with the in-repo client over real server material.
#![cfg(feature = "native-reinspiring")]
use enhance_pir::status as server;
use enhance_pir_server::status::{fixture, now_ms, Generation};
use ipir_sp::server::MatvecBackend;

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
        g.public().len(),
        enhance_pir::native::public_len(server::COLS)
    );
    let anchor = server::AcceptedAnchor {
        network: snapshot.network,
        height: snapshot.height,
        hash: snapshot.anchor,
    };
    let client = server::Client::new(g.manifest().clone(), g.public(), &anchor).unwrap();
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
    let mut tampered = g.public().to_vec();
    tampered[0] ^= 1;
    assert!(server::Client::new(g.manifest().clone(), &tampered, &anchor).is_err());
}

/// Status servers accept a 44-bit dithered selection in the same envelope as
/// the 49-bit one the in-repo client sends, by its exact length, and answer it
/// with the published rows. Every other length is refused.
#[test]
fn native_server_answers_dithered_queries_in_the_same_envelope() {
    use enhance_pir::native as n;
    let snapshot = fixture::snapshot(32, false).unwrap();
    let (g, _) = Generation::prepare(&snapshot, 1, 1, now_ms(), None, MatvecBackend::Cpu).unwrap();
    let setup = server::native_packing_setup(&g.manifest().network, &g.manifest().salt);
    let masks = server::native_query_masks(&g.manifest().network, &g.manifest().salt);
    let mut header = server::QUERY_MAGIC.to_vec();
    header.extend(g.manifest().id());
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
            g.public(),
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
