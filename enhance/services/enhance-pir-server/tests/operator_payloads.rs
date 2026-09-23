//! Retain historical deployment fixtures for rollback tooling and rejection tests.
//! Current schema-11 payloads are exercised through the real v4 HTTP tests.
use enhance_pir::{client::QuerySession, EnhanceSession};
use std::path::Path;

#[test]
fn historical_operator_payloads_are_rejected_by_current_client() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../ops/fixtures/enhance");
    for (name, schema, width) in [
        ("init.json", 9, 24_321),
        ("init-schema7-nine-record.json", 7, 6_633),
    ] {
        let bytes = std::fs::read(root.join(name)).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["generation"]["schema_version"], schema);
        assert_eq!(value["generation"]["row_bytes"], width);
        // Schema 7 also predates the required profile field; rejection during
        // deserialization is as valid as rejection during session validation.
        if let Ok(session) = serde_json::from_value::<EnhanceSession>(value) {
            assert!(QuerySession::from_session(session).is_err());
        }
    }
}
