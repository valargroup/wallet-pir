//! File integrity is established against both the open inode and its name.
use pir_publication::{
    hash_object_file, open_object_file, verify_object_file, FileIdentity, ObjectFileError,
    ObjectReference,
};
use std::{
    cell::Cell,
    fs::{self, File},
    io::{Read, Seek, SeekFrom, Write},
    os::unix::fs::{FileExt, MetadataExt},
    time::{Duration, UNIX_EPOCH},
};

#[test]
fn whole_file_hashing_checks_digest_length_and_named_identity() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("object");
    fs::write(&path, b"abc").unwrap();
    let mut file = File::open(&path).unwrap();
    file.seek(SeekFrom::End(0)).unwrap();
    let expected = ObjectReference {
        sha256: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad".into(),
        bytes: 3,
    };
    let (actual, identity) =
        hash_object_file(&path, &mut file, Some(&expected), &|| false).unwrap();
    assert_eq!(actual, expected);
    identity.check(&path, &file).unwrap();
    let before = file.metadata().unwrap();
    let (_, repeated) = hash_object_file(&path, &mut file, None, &|| false).unwrap();
    assert_eq!(identity, repeated);
    let after = file.metadata().unwrap();
    assert_eq!(
        (
            before.ino(),
            before.nlink(),
            before.ctime(),
            before.ctime_nsec()
        ),
        (
            after.ino(),
            after.nlink(),
            after.ctime(),
            after.ctime_nsec()
        )
    );
    let wrong_length = ObjectReference {
        bytes: 4,
        ..expected.clone()
    };
    assert!(matches!(
        hash_object_file(&path, &mut file, Some(&wrong_length), &|| false),
        Err(ObjectFileError::LengthMismatch { .. })
    ));
    let wrong_digest = ObjectReference::for_bytes(b"def");
    assert!(
        matches!(hash_object_file(&path, &mut file, Some(&wrong_digest), &|| false), Err(ObjectFileError::DigestMismatch { actual, .. }) if actual == expected.sha256)
    );
    let replacement = root.path().join("replacement");
    fs::write(&replacement, b"abc").unwrap();
    fs::rename(&replacement, &path).unwrap();
    assert!(identity.check(&path, &file).is_err());
    let (actual, changed) = hash_object_file(
        &path,
        &mut File::open(&path).unwrap(),
        Some(&expected),
        &|| false,
    )
    .unwrap();
    assert_eq!(actual, expected);
    assert_ne!(identity, changed);
    fs::remove_file(&path).unwrap();
    fs::write(&replacement, b"abc").unwrap();
    std::os::unix::fs::symlink(&replacement, &path).unwrap();
    // Even a symlink to the opened inode cannot stand in for its physical name.
    let mut target = File::open(&replacement).unwrap();
    assert!(hash_object_file(&path, &mut target, None, &|| false).is_err());
}

#[test]
fn reusable_verification_binds_expected_bytes_and_the_exact_file() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("object");
    fs::write(&path, b"abc").unwrap();
    let expected = ObjectReference::for_bytes(b"abc");
    let check = |prior| {
        verify_object_file(
            &path,
            &mut open_object_file(&path, None).unwrap(),
            Some(&expected),
            prior,
            &|| false,
        )
    };
    let fresh = check(None).unwrap();
    assert!(!fresh.reused && !fresh.reusable);
    std::thread::sleep(Duration::from_millis(1100));
    let settled = check(None).unwrap();
    assert!(settled.reusable && !settled.reused);
    let prior = Some((&expected, settled.identity));
    assert!(check(prior).unwrap().reused);
    let replacement = root.path().join("replacement");
    fs::write(&replacement, b"abc").unwrap();
    fs::rename(&replacement, &path).unwrap();
    assert!(
        !check(prior).unwrap().reused,
        "another inode requires a full read"
    );
    fs::write(&path, b"def").unwrap();
    assert!(matches!(
        check(prior),
        Err(ObjectFileError::DigestMismatch { .. })
    ));
}

#[test]
fn changes_during_bounded_reads_and_cancellation_return_no_success() {
    for change in ["unchanged", "write", "replace", "truncate", "cancel"] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("object");
        let mut writer = File::create(&path).unwrap();
        std::io::copy(&mut std::io::repeat(5).take((1 << 20) + 1), &mut writer).unwrap();
        writer.flush().unwrap();
        let modified = writer.metadata().unwrap().modified().unwrap();
        let mut file = File::open(&path).unwrap();
        let polls = Cell::new(0);
        let result = hash_object_file(&path, &mut file, None, &|| {
            polls.set(polls.get() + 1);
            if polls.get() == 2 {
                match change {
                    "write" => {
                        // Restoring mtime leaves ctime as the change signal. Cross
                        // the supported one-second timestamp tick before writing.
                        std::thread::sleep(Duration::from_millis(1100));
                        writer.write_all_at(&[9], 0).unwrap();
                        writer.set_modified(modified).unwrap();
                    }
                    "truncate" => writer.set_len(3).unwrap(),
                    "replace" => {
                        let next = root.path().join("next");
                        fs::copy(&path, &next).unwrap();
                        fs::rename(&next, &path).unwrap();
                    }
                    "cancel" => return true,
                    _ => {}
                }
            }
            false
        });
        if change == "unchanged" {
            let (reference, _) = result.unwrap();
            assert_eq!(reference.bytes, (1 << 20) + 1);
            assert_eq!(
                reference.sha256,
                "08246d5b9766f570a410e410874c14d389eb941cfdfc5c6fead40c1564fae906"
            );
        } else if change == "cancel" {
            assert!(matches!(result, Err(ObjectFileError::Cancelled)));
        } else {
            assert!(
                matches!(result, Err(ObjectFileError::FileChanged(_))),
                "{change}: {result:?}"
            );
        }
    }
}

#[test]
fn settlement_requires_both_timestamps_to_precede_the_safety_window() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("object");
    fs::write(&path, b"abc").unwrap();
    let file = File::open(&path).unwrap();
    let metadata = file.metadata().unwrap();
    let latest =
        (metadata.mtime(), metadata.mtime_nsec()).max((metadata.ctime(), metadata.ctime_nsec()));
    let time = UNIX_EPOCH + Duration::new(latest.0 as u64, latest.1 as u32);
    let identity = FileIdentity::capture(&path, &file).unwrap();
    assert!(!identity.settled_before(time));
    assert!(!identity.settled_before(time + Duration::from_secs(1)));
    assert!(identity.settled_before(time + Duration::from_secs(1) + Duration::from_nanos(1)));
    file.set_modified(time + Duration::from_secs(3600)).unwrap();
    let future = FileIdentity::capture(&path, &file).unwrap();
    assert!(!future.settled_before(time + Duration::from_secs(2)));
}

#[test]
fn checked_access_preserves_metadata_and_pins_the_acquired_file() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("object");
    fs::write(&path, b"first").unwrap();
    let mut opened = open_object_file(&path, Some(5)).unwrap();
    let identity = FileIdentity::capture(&path, &opened).unwrap();
    let links = opened.metadata().unwrap().nlink();
    for _ in 0..3 {
        let repeated = open_object_file(&path, Some(5)).unwrap();
        assert_eq!(FileIdentity::capture(&path, &repeated).unwrap(), identity);
        assert_eq!(repeated.metadata().unwrap().nlink(), links);
    }
    let next = root.path().join("next");
    fs::write(&next, b"other").unwrap();
    fs::rename(&next, &path).unwrap();
    let mut bytes = String::new();
    opened.read_to_string(&mut bytes).unwrap();
    assert_eq!(
        bytes, "first",
        "an acquired stream keeps the original inode"
    );
    assert!(identity.check(&path, &opened).is_err());
    assert!(hash_object_file(&path, &mut opened, None, &|| false).is_err());
    let mut replacement = open_object_file(&path, Some(5)).unwrap();
    bytes.clear();
    replacement.read_to_string(&mut bytes).unwrap();
    assert_eq!(bytes, "other");
}

#[test]
fn checked_access_refuses_wrong_lengths_types_and_redirected_storage() {
    let root = tempfile::tempdir().unwrap();
    let store = root.path().join("objects");
    fs::create_dir(&store).unwrap();
    let path = store.join("object");
    fs::write(&path, b"bytes").unwrap();
    assert!(matches!(
        open_object_file(&path, Some(4)),
        Err(ObjectFileError::LengthMismatch { .. })
    ));
    assert!(
        matches!(open_object_file(&store.join("missing"), None), Err(ObjectFileError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound)
    );
    assert!(matches!(
        open_object_file(&store, None),
        Err(ObjectFileError::NotRegularFile(_))
    ));
    let link = store.join("link");
    std::os::unix::fs::symlink(&path, &link).unwrap();
    assert!(matches!(
        open_object_file(&link, Some(5)),
        Err(ObjectFileError::NotRegularFile(_))
    ));
    let linked_store = root.path().join("linked-store");
    std::os::unix::fs::symlink(&store, &linked_store).unwrap();
    assert!(matches!(
        open_object_file(&linked_store.join("object"), Some(5)),
        Err(ObjectFileError::NotDirectory(_))
    ));
}
