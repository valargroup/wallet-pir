use super::retention::collect_files;
use super::*;

#[test]
fn collect_files_removes_only_unretained_owned_files() {
    let scratch = tempfile::tempdir().unwrap();
    let directory = scratch.path().join("objects");
    assert!(collect_files(&directory, &BTreeSet::new()).is_ok());
    fs::create_dir(&directory).unwrap();
    let (kept, stale) = ("ab".repeat(32), "cd".repeat(32));
    let retained = [
        kept.clone(),
        format!("{kept}.json"),
        format!("{kept}.bin"),
        format!("{kept}.bin.json"),
    ];
    let removed = [
        stale.clone(),
        format!("{stale}.json"),
        format!("{stale}.bin"),
        format!("{stale}.bin.json"),
        format!("{kept}.tmp"),
        format!("{kept}.json.tmp"),
        format!("{stale}.bin.tmp"),
    ];
    // Names the publisher does not own, including near-hashes, stay.
    let foreign = [
        "current.json".to_string(),
        "distribution.lock".to_string(),
        // A distinct digest avoids aliasing `kept` on case-insensitive filesystems.
        "AC".repeat(32),
        format!("{}.json", "a".repeat(63)),
        format!("{stale}.txt"),
    ];
    for name in retained.iter().chain(&removed).chain(&foreign) {
        fs::write(directory.join(name), name).unwrap();
    }
    // A directory under an owned name is not a collected file.
    fs::create_dir(directory.join("ef".repeat(32))).unwrap();
    collect_files(&directory, &retained.iter().cloned().collect()).unwrap();
    let mut remaining: Vec<_> = fs::read_dir(&directory)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    remaining.sort();
    let mut expected: Vec<_> = retained
        .iter()
        .chain(&foreign)
        .cloned()
        .chain(["ef".repeat(32)])
        .collect();
    expected.sort();
    assert_eq!(remaining, expected);
}

/// A valid single-shard manifest at `generation`, anchored at height 1 with the
/// hash the test node returns for that height.
fn manifest(generation: u64) -> Manifest {
    let geometry = enhance_shared::protocol::Geometry::default();
    let coverage = enhance_shared::protocol::Lifecycle::default()
        .coverage(1, geometry)
        .unwrap();
    let plan = enhance_shared::runtime::plan(coverage.shards[0].clone(), |_, count| {
        Ok(vec![0; count * enhance_shared::RECORD_BYTES])
    })
    .unwrap();
    Manifest {
        recovery_epoch: 0,
        placement_revision: 1,
        domain_recovery_epochs: [(0, "0".into())].into(),
        schema_version: enhance_shared::protocol::SCHEMA_VERSION,
        protocol_revision: enhance_shared::protocol::PROTOCOL_REVISION.into(),
        network: "main".into(),
        pool: "ironwood".into(),
        generation,
        anchor_height: 1,
        anchor_block_hash: format!("{:064x}", 1),
        geometry,
        coverage,
        sessions: vec![enhance_shared::protocol::SessionRef {
            shard_id: 0,
            public_params_sha256: "02".repeat(32),
            parameter_id: enhance_shared::protocol::parameter_id(plan.shard.logical_rows).unwrap(),
        }],
        unit_identities: [(0, plan.units)].into(),
    }
}

/// A durable state whose current publication is the newest of `count`
/// retained ones, newest first.
fn state(count: u64) -> DistributionState {
    let inventory = |generation: u64| Object {
        sha256: format!("{generation:064x}"),
        bytes: 1,
    };
    DistributionState {
        current: Current {
            version: VERSION,
            revision: 1,
            withdrawal_revision: 1,
            recovery_epoch: 0,
            revoked: BTreeSet::new(),
            eligible: true,
            manifest: Some(manifest(count)),
            inventory: Some(inventory(count)),
        },
        retained: (1..=count)
            .rev()
            .map(|generation| RetainedPublication {
                manifest: manifest(generation),
                inventory: inventory(generation),
            })
            .collect(),
    }
}

fn rpc(root: &Path, origin: &str) -> ZakuraClient {
    let cookie = root.join("cookie");
    fs::write(&cookie, "fixture:fixture").unwrap();
    ZakuraClient::from_cookie_file(origin, cookie).unwrap()
}

#[test]
fn a_longer_retained_list_still_opens() {
    let scratch = tempfile::tempdir().unwrap();
    let rpc = rpc(scratch.path(), "http://127.0.0.1:1");
    let root = scratch.path().join("https");
    fs::create_dir(&root).unwrap();
    persist(&root, &state(RETAINED_PUBLICATIONS as u64)).unwrap();
    let distribution = PublicationDistribution::open(&root, rpc.clone()).unwrap();
    assert_eq!(
        distribution.retained_manifests().len(),
        RETAINED_PUBLICATIONS
    );
    drop(distribution);
    persist(&root, &state(RETAINED_PUBLICATIONS as u64 + 1)).unwrap();
    assert!(PublicationDistribution::open(&root, rpc).is_err());
}
