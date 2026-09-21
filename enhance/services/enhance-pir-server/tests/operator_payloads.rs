//! The Enhance init payload the deploy script parses, pinned as a golden file.
//!
//! `JQ_ENHANCE_INIT_COMPLETE` in `enhance/ops/scripts/deploy-enhance-pir.sh` is
//! the gate that decides whether a rolled-out fleet is serving the layout the
//! release intended. Until now it was compile-checked and never evaluated, so
//! it asserted `row_bytes == 6633` for as long as nobody read it -- through a
//! change that made 6,633 the wrong answer.
//!
//! This writes the document the server actually serializes, and
//! `ops/scripts/check-jq-contracts.sh` runs the script's own jq program against
//! it. Neither side transcribes the other, so neither can go stale alone. A
//! change to the operator contract shows up as a diff in `enhance/ops/fixtures/`.
//!
//! Accept a deliberate change with:
//!
//! ```text
//! UPDATE_OPS_FIXTURES=1 cargo test -p enhance-pir-server --test operator_payloads
//! ```
//!
//! Cheap on purpose: `EnhanceSession` is built from `GenerationManifest`, which
//! is plain data. Nothing here builds a `ShardRuntime`, so the fixture costs
//! milliseconds rather than the gigabyte-scale preprocessing a real publish does.

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use enhance_pir::types::EnhanceSession;
use enhance_pir_server::ipir::global_parameters;
use enhance_pir_server::types::{
    DatabaseId, GenerationManifest, ShardDescriptor, TableManifest, ENHANCE_LAYOUT,
    ENHANCE_SETUP_SEED, PROTOCOL_REVISION, RECORDS_PER_ROW, RECORD_BYTES, SHARD_POSITIONS,
    SHARD_ROWS,
};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// A tree size that spans two shards, so `shards` is a list rather than a
/// single element and the sealed/frontier distinction is visible in the fixture.
const TREE_SIZE: u64 = SHARD_POSITIONS as u64 + 300;
const ANCHOR_HEIGHT: u64 = 3_489_681;

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../ops/fixtures/enhance")
}

fn session() -> EnhanceSession {
    let layout = ENHANCE_LAYOUT;
    let used_rows = layout.used_rows_for(TREE_SIZE);
    let logical_rows = layout.logical_rows_for(used_rows);
    let (rlwe, params) = global_parameters(logical_rows, &layout).expect("global parameters");

    // Stand-in public material of exactly the length the client recomputes from
    // the parameters. The client checks the length and the digest, so a wrong
    // instance count here would fail the same way a wrong server would.
    let public_params = vec![
        0u8;
        (params.db_cols / rlwe.d)
            * ipir_sp::modulus_switch::published_c1_len(rlwe.d, rlwe.q)
    ];
    let digest = Sha256::digest(&public_params);

    let shards = (0..TREE_SIZE.div_ceil(SHARD_POSITIONS as u64))
        .map(|shard_id| {
            let populated = TREE_SIZE
                .saturating_sub(shard_id * SHARD_POSITIONS as u64)
                .min(SHARD_POSITIONS as u64);
            ShardDescriptor {
                shard_id,
                global_row_start: shard_id * SHARD_ROWS as u64,
                populated_positions: populated,
                rows_sha256: format!("{shard_id:064x}"),
                sealed: populated == SHARD_POSITIONS as u64,
                worker: "enhance-pir-worker-01".to_string(),
            }
        })
        .collect();

    let mut tables = BTreeMap::new();
    tables.insert(
        DatabaseId::Enhance,
        TableManifest {
            record_bytes: RECORD_BYTES as u32,
            records_per_row: RECORDS_PER_ROW as u32,
            row_bytes: layout.row_bytes() as u32,
            shard_rows: SHARD_ROWS as u32,
            positions: TREE_SIZE,
            used_rows,
            logical_rows,
            parameter_id: format!(
                "{PROTOCOL_REVISION}-enhance-d{}-p{}-rows{}-cols{}",
                rlwe.d, params.p, logical_rows, params.db_cols
            ),
            setup_seed: ENHANCE_SETUP_SEED,
            public_params_epoch: hex::encode(&digest[..8]),
            public_params_sha256: hex::encode(digest),
            shards,
        },
    );

    let generation = GenerationManifest {
        anchor_height: ANCHOR_HEIGHT,
        anchor_block_hash: format!("{ANCHOR_HEIGHT:064x}"),
        ironwood_tree_size: TREE_SIZE,
        generation: ANCHOR_HEIGHT,
        tables,
    }
    .public()
    .expect("enhance table is present");

    EnhanceSession {
        generation,
        params,
        public_params_base64: BASE64_STANDARD.encode(&public_params),
    }
}

/// Writes `name` if `UPDATE_OPS_FIXTURES` is set, and otherwise compares.
fn golden(name: &str, actual: &serde_json::Value) {
    let path = fixture_dir().join(name);
    let rendered = format!("{}\n", serde_json::to_string_pretty(actual).unwrap());
    if std::env::var_os("UPDATE_OPS_FIXTURES").is_some() {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, &rendered).unwrap();
        return;
    }
    let expected = std::fs::read_to_string(&path).unwrap_or_else(|error| {
        panic!(
            "{}: {error}\nrun: UPDATE_OPS_FIXTURES=1 cargo test -p enhance-pir-server \
             --test operator_payloads",
            path.display()
        )
    });
    assert_eq!(
        rendered,
        expected,
        "{} is stale; run: UPDATE_OPS_FIXTURES=1 cargo test -p enhance-pir-server \
         --test operator_payloads",
        path.display()
    );
}

#[test]
fn init_payload_matches_the_operator_fixture() {
    let value = serde_json::to_value(session()).expect("serialize session");
    // The fields the deploy gate reads, asserted here as well so a fixture
    // refresh that quietly changed the layout is not self-approving.
    assert_eq!(value["generation"]["schema_version"], 8);
    assert_eq!(value["generation"]["record_bytes"], 737);
    assert_eq!(value["generation"]["records_per_row"], 29);
    assert_eq!(value["generation"]["row_bytes"], 21_373);
    assert_eq!(value["generation"]["shard_rows"], 8_192);
    golden("init.json", &value);
}

/// The nine-record document the fleet served before this migration, kept as the
/// negative case for the deploy gate. `check-jq-contracts.sh` asserts that
/// `JQ_ENHANCE_INIT_COMPLETE` *rejects* it: a gate that accepts the layout it is
/// replacing would have let the rollout report success against the old fleet.
#[test]
fn superseded_nine_record_payload_is_still_a_valid_document() {
    let path = fixture_dir().join("init-schema7-nine-record.json");
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("{}: {error}", path.display()));
    let value: serde_json::Value = serde_json::from_str(&text).expect("valid JSON");
    assert_eq!(value["generation"]["schema_version"], 7);
    assert_eq!(value["generation"]["records_per_row"], 9);
    assert_eq!(value["generation"]["row_bytes"], 6_633);
}
