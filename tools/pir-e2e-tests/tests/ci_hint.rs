//! Independent hint oracles and damaged-publication checks for local production.
#[path = "boundary_recovery_model.rs"]
mod boundary_recovery_model;
#[path = "record_layout.rs"]
mod record_layout;
#[path = "status_routes.rs"]
mod status_routes;
use enhance_query_server::publication::{AuthorityResponse, Inventory, PREFIX};
#[cfg(feature = "native-reinspiring")]
use enhance_query_server::{runtime::Engine, wire::encode_crs_blocks};
#[cfg(feature = "native-reinspiring")]
use pir_e2e_tests::enhance_fixture::record;
#[path = "support/publication.rs"]
#[allow(dead_code)]
mod publication_source;
async fn hint_matches_oracle(count: u64) {
    let fixture = publication_source::Published::with_records(count).await;
    let source = fixture.source();
    let certificate = reqwest::Certificate::from_pem(
        &std::fs::read(source.trusted_ca.as_ref().unwrap()).unwrap(),
    )
    .unwrap();
    let http = reqwest::Client::builder()
        .add_root_certificate(certificate)
        .build()
        .unwrap();
    let current: AuthorityResponse = http
        .get(format!("{}{PREFIX}current", source.publisher_origin))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let object = current.current.inventory.unwrap();
    let inventory: Inventory = http
        .get(format!(
            "{}{PREFIX}inventories/{}",
            source.publisher_origin, object.sha256
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let domain = &inventory.domains[0];
    let actual = http
        .get(format!(
            "{}{PREFIX}objects/{}",
            source.publisher_origin, domain.hint.sha256
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .bytes()
        .await
        .unwrap();
    #[cfg(feature = "native-reinspiring")]
    {
        let root = tempfile::tempdir().unwrap();
        let oracle = Engine::new(root.path())
            .prepare(domain.plan.clone(), |start, n| {
                Ok((start..start + n as u64).flat_map(record).collect())
            })
            .unwrap()
            .0;
        assert_eq!(actual.as_ref(), encode_crs_blocks(&oracle.hint().unwrap()));
    }
    #[cfg(not(feature = "native-reinspiring"))]
    {
        // These existing fixed vectors pin the independently verified reference
        // bytes. Native preparation uses the generic oracle above.
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
    use sha2::{Digest, Sha256};
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
    let digest = hex::encode(Sha256::digest(&original));
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
