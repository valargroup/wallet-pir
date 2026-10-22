//! File integrity is established against both the open inode and its name.
use pir_publication::{hash_object_file, FileIdentity, ObjectFileError, ObjectReference};
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
    std::os::unix::fs::symlink(&replacement, &path).unwrap();
    assert!(hash_object_file(&path, &mut file, None, &|| false).is_err());
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
