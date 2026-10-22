//! Independent hint oracles and damaged-publication checks for local production.
#[path = "boundary_recovery_model.rs"]
mod boundary_recovery_model;
#[path = "record_layout.rs"]
mod record_layout;
#[path = "status_routes.rs"]
mod status_routes;
use enhance_query_server::protocol::{Geometry, Lifecycle};
use enhance_query_server::runtime::{plan, Engine};
use enhance_query_server::wire::encode_crs_blocks;
use pir_e2e_tests::enhance_fixture::record;
fn rows(start: u64, count: usize) -> Result<Vec<u8>, String> {
    Ok((start..start + count as u64).flat_map(record).collect())
}
/// One reference runtime for the pinned 67-record hint. The reference packing
/// artifact is larger than 512 MiB even for this domain, so this oracle does
/// not build it; qualification publications do.
async fn hint_matches_oracle(count: u64) {
    let root = tempfile::tempdir().unwrap();
    let shard = Lifecycle::default()
        .coverage(count, Geometry::default())
        .unwrap()
        .shards[0]
        .clone();
    let domain = plan(shard, rows).unwrap();
    #[cfg(feature = "native-reinspiring")]
    let again = domain.clone();
    let evaluation = Engine::new(&root.path().join("oracle"))
        .prepare(domain, rows)
        .unwrap()
        .0;
    let expected = encode_crs_blocks(&evaluation.hint().unwrap());
    #[cfg(feature = "native-reinspiring")]
    {
        let other = Engine::new(&root.path().join("other"))
            .prepare(again, rows)
            .unwrap()
            .0;
        assert_eq!(expected, encode_crs_blocks(&other.hint().unwrap()));
    }
    let path = root.path().join("hint.bin");
    std::fs::write(&path, &expected).unwrap();
    let served = path.clone();
    let listener = pir_e2e_tests::enhance_fixture::serve(axum::Router::new().route(
        "/hint",
        axum::routing::get(move || {
            let served = served.clone();
            async move { std::fs::read(served).unwrap() }
        }),
    ))
    .await;
    let actual = reqwest::get(format!("{}/hint", listener.url))
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(actual.as_ref(), expected.as_slice());
    #[cfg(not(feature = "native-reinspiring"))]
    {
        // These existing fixed vectors pin the independently verified reference
        // bytes. Native preparation uses the second engine above.
        use sha2::{Digest, Sha256};
        assert_eq!(
            hex::encode(Sha256::digest(&actual)),
            match count {
                67 => "c6020b03b132c41dbb3d7f85b97c1dcc6381e8f2ec2db8c98ac89f8db7e5b70a",
                68 => "12f1a4de5fa69d4f37d86c98c8e7e2b7f37ec3d2613b638d90075437e7825fc0",
                _ => unreachable!(),
            }
        );
    }
    listener.stop().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn published_hint_matches_independent_oracle_67() {
    hint_matches_oracle(67).await;
}
#[cfg(feature = "qualification-tests")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn published_hint_matches_independent_oracle_68() {
    hint_matches_oracle(68).await;
}
#[test]
fn published_hint_rejects_damage_before_loading() {
    use enhance_query_server::artifact::PublicationArtifact;
    let root = tempfile::tempdir().unwrap();
    // Byte validation needs a valid encoding, not an evaluator. The TLS oracle
    // cases above independently check the publisher's real prepared hint.
    let degree = 4usize;
    let blocks = 2usize;
    let mut original = b"MPH1".to_vec();
    original.extend_from_slice(&(blocks as u32).to_le_bytes());
    for block in 0..blocks {
        original.extend_from_slice(&(degree as u32).to_le_bytes());
        for row in 0..degree {
            original.extend_from_slice(&(degree as u32).to_le_bytes());
            for column in 0..degree {
                original.extend_from_slice(&((block + row + column) as u64).to_le_bytes());
            }
        }
    }
    let digest = pir_artifact::digest(&original);
    let path = root.path().join("damaged-hint");
    for fault in ["corrupt", "truncated", "shape", "trailing", "format"] {
        let mut bytes = original.clone();
        match fault {
            "truncated" => {
                bytes.pop();
            }
            "trailing" => bytes.push(0),
            "shape" => bytes[8] ^= 1,
            "format" => bytes[0] ^= 1,
            _ => {
                let end = bytes.len() - 1;
                bytes[end] ^= 1;
            }
        }
        std::fs::write(&path, bytes).unwrap();
        assert!(
            PublicationArtifact::revalidate_hint_file(
                &path,
                original.len() as u64,
                &digest,
                blocks,
                degree
            )
            .is_err(),
            "{fault}"
        );
    }
    std::fs::write(&path, &original).unwrap();
    let pinned = PublicationArtifact::open(&path, original.len() as u64, digest).unwrap();
    std::fs::remove_file(&path).unwrap();
    enhance_query_server::wire::validate_crs_blocks(pinned.reader(), blocks, degree).unwrap();
}
