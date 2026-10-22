use super::verification::{check_object, FileIdentity, VerificationPass, SETTLED};
use super::*;
use crate::{prepared_packing, runtime};
use enhance_query_server::protocol::Lifecycle;
use enhance_query_server::publication::Domain;
use enhance_query_server::publication::Object;
use sha2::{Digest, Sha256};
use std::time::{Duration, UNIX_EPOCH};

fn object(root: &Path, bytes: &[u8]) -> Object {
    let object = Object {
        sha256: hex::encode(Sha256::digest(bytes)),
        bytes: bytes.len() as u64,
    };
    fs::create_dir_all(root.join("objects")).unwrap();
    fs::write(object.path(root).unwrap(), bytes).unwrap();
    object
}

/// Let every file written so far settle, so its hash can be reused.
fn settle() {
    std::thread::sleep(SETTLED + Duration::from_millis(100));
}

fn verified(root: &Path, object: &Object) -> FileIdentity {
    let mut hashed = VerificationStats::default();
    let identity = check_object(root, object, None, &mut hashed)
        .unwrap()
        .unwrap();
    assert_eq!(hashed.files, 1);
    identity
}

#[test]
fn changed_replaced_and_truncated_objects_are_rehashed() {
    let root = tempfile::tempdir().unwrap();
    let root = root.path();
    let unchanged = object(root, b"unchanged object");
    let rewritten = object(root, b"rewritten object");
    let restored = object(root, b"restored mtime!!");
    let replaced = object(root, b"replaced object!");
    let truncated = object(root, b"truncated object");
    settle();
    let all = [&unchanged, &rewritten, &restored, &replaced, &truncated];
    let evidence: Vec<_> = all.iter().map(|o| verified(root, o)).collect();
    let mut hashed = VerificationStats::default();
    for (object, identity) in all.iter().zip(&evidence) {
        check_object(root, object, Some(identity), &mut hashed).unwrap();
    }
    assert_eq!(
        hashed,
        VerificationStats {
            files: 0,
            bytes: 0,
            reused: 5,
        }
    );

    fs::write(rewritten.path(root).unwrap(), b"Rewritten object").unwrap();
    let path = restored.path(root).unwrap();
    let modified = fs::metadata(&path).unwrap().modified().unwrap();
    fs::write(&path, b"Restored mtime!!").unwrap();
    File::options()
        .write(true)
        .open(&path)
        .unwrap()
        .set_modified(modified)
        .unwrap();
    assert_eq!(fs::metadata(&path).unwrap().modified().unwrap(), modified);
    let path = replaced.path(root).unwrap();
    let replacement = root.join("replacement");
    fs::write(&replacement, b"Replaced object!").unwrap();
    File::options()
        .write(true)
        .open(&replacement)
        .unwrap()
        .set_modified(fs::metadata(&path).unwrap().modified().unwrap())
        .unwrap();
    fs::rename(&replacement, &path).unwrap();
    File::options()
        .write(true)
        .open(truncated.path(root).unwrap())
        .unwrap()
        .set_len(1)
        .unwrap();

    let mut hashed = VerificationStats::default();
    check_object(root, &unchanged, Some(&evidence[0]), &mut hashed).unwrap();
    assert_eq!(hashed.reused, 1);
    for (object, identity) in all.iter().zip(&evidence).skip(1) {
        let mut hashed = VerificationStats::default();
        assert!(
            check_object(root, object, Some(identity), &mut hashed).is_err(),
            "{object:?} must not be accepted from stale evidence"
        );
        assert_eq!(hashed.reused, 0);
    }
}

#[test]
fn a_replaced_object_with_the_same_bytes_is_hashed_again() {
    let root = tempfile::tempdir().unwrap();
    let root = root.path();
    let object = object(root, b"same bytes");
    settle();
    let identity = verified(root, &object);
    let replacement = root.join("replacement");
    fs::write(&replacement, b"same bytes").unwrap();
    fs::rename(&replacement, object.path(root).unwrap()).unwrap();
    let mut hashed = VerificationStats::default();
    check_object(root, &object, Some(&identity), &mut hashed).unwrap();
    assert_eq!((hashed.files, hashed.reused), (1, 0));
}

#[test]
fn a_recently_changed_file_is_not_reused() {
    let root = tempfile::tempdir().unwrap();
    let object = object(root.path(), b"fresh object");
    let identity = FileIdentity::of(&fs::metadata(object.path(root.path()).unwrap()).unwrap());
    let ctime = UNIX_EPOCH + Duration::new(identity.ctime.0 as u64, identity.ctime.1 as u32);
    assert!(!identity.settled_before(ctime));
    assert!(!identity.settled_before(ctime + SETTLED - Duration::from_nanos(1)));
    assert!(identity.settled_before(ctime + SETTLED));
}

#[test]
fn a_fresh_object_is_hashed_once_per_pass() {
    let root = tempfile::tempdir().unwrap();
    let object = object(root.path(), b"fresh object");
    let mut first = VerificationPass::default();
    first.check_object(root.path(), &object, None).unwrap();
    assert!(first.evidence.is_empty());
    first.check_object(root.path(), &object, None).unwrap();
    let once = VerificationStats {
        files: 1,
        bytes: object.bytes,
        reused: 0,
    };
    assert_eq!(first.hashed, once);

    let mut second = VerificationPass::default();
    second
        .check_object(root.path(), &object, first.evidence.get(&object.sha256))
        .unwrap();
    assert_eq!(
        second.hashed, once,
        "a later pass must hash fresh files again"
    );

    let contradictory = Object {
        bytes: object.bytes + 1,
        ..object
    };
    assert!(first
        .check_object(root.path(), &contradictory, None)
        .is_err());
    assert_eq!(first.hashed, once);
}

#[test]
fn repeated_digest_with_a_different_length_is_rejected() {
    use enhance_query_server::{protocol, RECORD_BYTES};

    let root = tempfile::tempdir().unwrap();
    let coverage = Lifecycle::default()
        .coverage(1, Geometry::default())
        .unwrap();
    let mut read = |_, count| Ok(vec![0; count * RECORD_BYTES]);
    let plan = runtime::plan(coverage.shards[0].clone(), &mut read).unwrap();
    let bytes = runtime::unit_rows(&plan.shard, &plan.shard.units[0], &mut read).unwrap();
    let rows = object(root.path(), &bytes);
    let parameters = protocol::parameters(plan.shard.logical_rows).unwrap();
    #[cfg(feature = "native-reinspiring")]
    let public_bytes = enhance_query_server::native::public_len(enhance_query_server::native::COLS);
    #[cfg(not(feature = "native-reinspiring"))]
    let public_bytes =
        (parameters.db_cols * ipir_sp::modulus_switch::modulus_bits(runtime::rlwe().q)).div_ceil(8);
    let public = Object {
        sha256: "11".repeat(32),
        bytes: public_bytes as u64,
    };
    let hint = Object {
        sha256: rows.sha256.clone(),
        bytes: crate::wire::crs_encoded_len(
            parameters.db_cols / runtime::rlwe().d,
            runtime::rlwe().d,
        )
        .unwrap(),
    };
    assert_ne!(rows.bytes, hint.bytes);
    let packing = prepared_packing::Artifact {
        name: prepared_packing::artifact_name(plan.shard.logical_rows, &public.sha256).unwrap(),
        sha256: "22".repeat(32),
        #[cfg(feature = "native-reinspiring")]
        bytes: 192 * 1024 * 1024,
        #[cfg(not(feature = "native-reinspiring"))]
        bytes: prepared_packing::expected_len(plan.shard.logical_rows).unwrap(),
        format: prepared_packing::FORMAT,
    };
    let inventory = Inventory {
        version: VERSION,
        network: "main".into(),
        profile: PROTOCOL_REVISION.into(),
        coverage,
        domains: vec![Domain {
            plan,
            rows: vec![rows],
            hint,
            public,
            packing,
        }],
    };
    inventory.validate().unwrap();
    settle();
    // Only the row file exists: verification must reject the contradictory
    // hint length before attempting to open the later public or packing files.
    assert_eq!(
        verify_publication(root.path(), inventory).unwrap_err(),
        "publication object checksum or length mismatch"
    );
}

#[test]
fn invalid_geometry_and_recovery_are_rejected_before_reading_records() {
    let scratch = tempfile::tempdir().unwrap();
    let root = scratch.path().join("publication");
    let coverage = Lifecycle::default()
        .coverage(1, Geometry::default())
        .unwrap();
    for epochs in [BTreeMap::new(), [(0, "01".into())].into()] {
        assert!(
            prepare_publication(&root, coverage.clone(), &epochs, |_, _| {
                panic!("invalid metadata must not read records")
            })
            .is_err()
        );
        assert!(!root.exists());
    }
    let mut malformed = coverage;
    malformed.shards[0].logical_rows = u64::MAX;
    assert!(
        prepare_publication(&root, malformed, &[(0, "0".into())].into(), |_, _| {
            panic!("invalid geometry must not read records")
        })
        .is_err()
    );
    assert!(!root.exists());
}

#[test]
fn preparation_has_one_writer() {
    let root = tempfile::tempdir().unwrap();
    let owner = File::create(root.path().join("prepare.lock")).unwrap();
    owner.lock().unwrap();
    let coverage = Lifecycle::default()
        .coverage(1, Geometry::default())
        .unwrap();
    assert!(
        prepare_publication(root.path(), coverage, &[(0, "0".into())].into(), |_, _| {
            panic!("a second owner must not read records")
        })
        .is_err()
    );
}
