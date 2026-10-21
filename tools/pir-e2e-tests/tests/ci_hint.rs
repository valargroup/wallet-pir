//! Independent hint oracles and damaged-publication checks for local production.
#[path = "boundary_recovery_model.rs"]
mod boundary_recovery_model;
#[path = "record_layout.rs"]
mod record_layout;
#[path = "status_routes.rs"]
mod status_routes;
use enhance_query_server::{
    protocol::{Geometry, Lifecycle},
    publication::{AuthorityResponse, Inventory, PREFIX},
};
#[cfg(feature = "native-reinspiring")]
use enhance_query_server::{runtime::Engine, wire::encode_crs_blocks};
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
    let coverage = Lifecycle::default()
        .coverage(67, Geometry::default())
        .unwrap();
    let inventory = enhance_publisher::publication::prepare(
        root.path(),
        coverage,
        &[(0, "0".into())].into(),
        |start, n| Ok((start..start + n as u64).flat_map(record).collect()),
    )
    .unwrap();
    let object = &inventory.domains[0].hint;
    let original = std::fs::read(object.path(root.path()).unwrap()).unwrap();
    let degree = enhance_query_server::runtime::rlwe().d;
    let per_block = (degree * 8 + 4) * degree + 4;
    assert_eq!((original.len() - 8) % per_block, 0);
    let blocks = (original.len() - 8) / per_block;
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
