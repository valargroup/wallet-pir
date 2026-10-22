use super::*;
use enhance_query_server::protocol::{Coverage, Geometry};
use serde_json::{json, Value};

// Handwritten examples of the saved-state formats. Keep field order and
// numeric-versus-string encoding visible, independently of the Rust type names.
const PUBLISHER: &str = r#"{"version":1,"next_generation":23,"lifecycle":{"identities":[0],"next_id":1},"recovery":{"epoch":4,"domain_epochs":{"0":"4"},"revoked":["abababababababababababababababababababababababababababababababab"],"sealed":{"0":[3428143,"1212121212121212121212121212121212121212121212121212121212121212"]}}}"#;
const DISTRIBUTION: &str = r#"{"current":{"version":2,"revision":9,"withdrawal_revision":7,"recovery_epoch":4,"revoked":["abababababababababababababababababababababababababababababababab"],"eligible":false,"manifest":null,"inventory":null},"retained":[]}"#;

#[test]
fn publisher_and_distribution_keep_the_existing_disk_encoding() {
    let publisher: PublisherState = serde_json::from_str(PUBLISHER).unwrap();
    assert_eq!(publisher.next_generation, 23);
    assert_eq!(publisher.recovery.domain_epochs[&0], "4");
    assert_eq!(publisher.recovery.sealed[&0].0, 3_428_143);
    assert_eq!(serde_json::to_string(&publisher).unwrap(), PUBLISHER);

    let distribution: DistributionState = serde_json::from_str(DISTRIBUTION).unwrap();
    assert_eq!(distribution.current.withdrawal_revision, 7);
    assert!(!distribution.current.eligible);
    assert_eq!(serde_json::to_string(&distribution).unwrap(), DISTRIBUTION);
}

#[test]
fn a_fresh_publisher_keeps_the_existing_initial_floors() {
    assert_eq!(
        serde_json::to_value(PublisherState::default()).unwrap(),
        json!({
            "version": 1, "next_generation": 1,
            "lifecycle": {"identities": [], "next_id": 0},
            "recovery": {"epoch": 0, "domain_epochs": {}, "revoked": [], "sealed": {}}
        })
    );
}

fn retained_publication() -> Value {
    json!({
        "manifest": {
            "recovery_epoch": "4", "placement_revision": 0,
            "domain_recovery_epochs": {}, "schema_version": 11,
            "protocol_revision": "stored-profile", "network": "main", "pool": "ironwood",
            "generation": 22, "anchor_height": 3_428_143, "anchor_block_hash": "12".repeat(32),
            "geometry": Geometry::default(),
            "coverage": Coverage { records: 0, shards: vec![], routes: vec![] },
            "sessions": [], "unit_identities": {}
        },
        "inventory": {"checksum": {"algorithm": pir_artifact::CURRENT, "digest": "34".repeat(32)}, "bytes": 128}
    })
}

#[test]
fn retained_publications_preserve_manifest_and_inventory_encodings() {
    // Decoding disk shapes does not grant authority or replace domain validation.
    let retained = retained_publication();
    let mut disk: Value = serde_json::from_str(DISTRIBUTION).unwrap();
    disk["retained"] = json!([retained]);
    let decoded: DistributionState = serde_json::from_value(disk.clone()).unwrap();
    assert_eq!(serde_json::to_value(decoded).unwrap(), disk);
}

#[test]
fn shared_disk_types_still_reject_unknown_and_missing_required_fields() {
    let publisher: Value = serde_json::from_str(PUBLISHER).unwrap();
    for pointer in ["", "/recovery", "/lifecycle"] {
        let mut extra = publisher.clone();
        extra.pointer_mut(pointer).unwrap()["unexpected"] = json!(true);
        assert!(serde_json::from_value::<PublisherState>(extra).is_err());
    }
    let mut missing = publisher.clone();
    missing["recovery"]
        .as_object_mut()
        .unwrap()
        .remove("sealed");
    assert!(serde_json::from_value::<PublisherState>(missing).is_err());

    let mut disk: Value = serde_json::from_str(DISTRIBUTION).unwrap();
    disk["retained"] = json!([retained_publication()]);
    for pointer in ["", "/current", "/retained/0", "/retained/0/inventory"] {
        let mut extra = disk.clone();
        extra.pointer_mut(pointer).unwrap()["unexpected"] = json!(true);
        assert!(serde_json::from_value::<DistributionState>(extra).is_err());
    }
    let mut missing = disk;
    missing.as_object_mut().unwrap().remove("retained");
    assert!(serde_json::from_value::<DistributionState>(missing).is_err());
}
