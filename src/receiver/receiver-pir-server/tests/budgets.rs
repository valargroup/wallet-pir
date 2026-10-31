//! The inventory's capture and setup budgets at full sealed shards.
use zakura_pir_query_server::receiver::{
    choice_bytes,
    inventory::{FilterRef, Inventory, Shard, CHUNK_SHARDS, MAX_CAPTURE_BYTES, MAX_CHUNK_BYTES},
    native::public_bytes,
    query_rows, shards, ChoiceRef, Companion, Coverage, Directory, Publication, Session,
    Table as _, Tree, SCHEMA, SEAL_TARGET,
};

/// Sealed shard `index` of a chain of shards one block each, its one segment's
/// manifests at the seal target with `receivers` distinct receivers, and a filter.
fn shard(index: u32, receivers: u32) -> Shard {
    let hash = |i: u32| {
        let mut h = [0; 32];
        h[..4].copy_from_slice(&(i + 1).to_le_bytes());
        h
    };
    let position = |i: u32| u64::from(i) * u64::from(SEAL_TARGET);
    let coverage = Coverage {
        genesis: [1; 32],
        history_revision: 0,
        start_height: 100 + index,
        start_parent: hash(index),
        start_position: position(index),
        end_height: 100 + index,
        end_hash: hash(index + 1),
        end_position: position(index + 1),
    };
    let directory = Directory {
        schema: SCHEMA.into(),
        coverage: coverage.clone(),
        salt: [7; 32],
        entries: receivers,
        payments: SEAL_TARGET,
        choice: ChoiceRef {
            digest: [8; 32],
            bytes: choice_bytes(receivers) as u32,
            keys: receivers,
        },
        data_sha256: [9; 32],
    };
    let companion = Companion {
        schema: SCHEMA.into(),
        coverage,
        directory: directory.revision().unwrap(),
        rows: query_rows(SEAL_TARGET),
        query_rows: query_rows(SEAL_TARGET),
        payments: SEAL_TARGET,
        tree: Tree {
            height: 100 + index,
            hash: hash(index + 1),
            size: position(index + 1),
        },
        data_sha256: [10; 32],
    };
    Shard {
        index,
        sealed: true,
        start_height: 100 + index,
        start_parent: hash(index),
        start_position: position(index),
        end_height: 100 + index,
        end_hash: hash(index + 1),
        end_position: position(index + 1),
        segments: vec![Publication {
            directory: Session {
                manifest: directory,
                public_digest: [11; 32],
            },
            companion: Session {
                manifest: companion,
                public_digest: [12; 32],
            },
        }],
        filter: Some(FilterRef {
            digest: [13; 32],
            bytes: shards::MAX_FILTER_BYTES as u32,
            receivers: receivers.min(shards::MAX_RECEIVERS),
        }),
    }
}

/// Many full sealed shards stay within the capture and setup budgets: every chunk of
/// 16 shards is under its 1 MiB bound, the 128 MiB capture budget holds far more
/// shards than the archive tier's 64-shard trigger, and the setups and choice
/// objects a catch-up reads across 64 sealed directories fit a 16 MiB wallet cache.
#[test]
fn many_sealed_shards_fit_the_capture_and_setup_budgets() {
    let shards: Vec<_> = (0..256).map(|i| shard(i, SEAL_TARGET)).collect();
    let inventory = Inventory::build([1; 32], 0, shards).unwrap();
    let chunks = inventory.chunks().unwrap();
    let bytes: usize = chunks
        .iter()
        .map(|c| serde_json::to_vec(c).unwrap().len())
        .sum();
    let per_shard = bytes / 256;
    assert!(per_shard < MAX_CHUNK_BYTES / CHUNK_SHARDS);
    let capture = MAX_CAPTURE_BYTES / per_shard;
    assert!(
        capture >= 1 << 15,
        "{per_shard} bytes a shard, {capture} shards"
    );
    let setup = public_bytes() + choice_bytes(SEAL_TARGET);
    assert!(64 * setup <= 16 << 20, "{setup} bytes a sealed directory");
    println!(
        "shard_json_bytes={per_shard} capture_shards={capture} setup_and_choice_bytes={setup}"
    );
}
