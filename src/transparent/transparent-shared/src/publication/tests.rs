use super::*;

#[test]
fn artifact_hashes_match_the_complete_files_in_input_order() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let inputs = [
        ("large.bin", vec![7u8; 128 << 10]),
        ("small.bin", vec![3u8; 17]),
    ];
    let mut pending = Vec::new();
    for (name, bytes) in &inputs {
        let path = root.join(name);
        std::fs::write(&path, bytes).unwrap();
        pending.push((path, Kind::RecentTable));
    }
    let actual = hash_artifacts(root, pending).unwrap();
    for ((artifact, path, file), (name, bytes)) in actual.iter().zip(&inputs) {
        assert!(file.is_none());
        assert_eq!(artifact.path, *name);
        assert_eq!(*path, root.join(name));
        assert_eq!(artifact.bytes, bytes.len() as u64);
        assert_eq!(artifact.sha256, hex::encode(Sha256::digest(bytes)));
        assert_eq!(artifact.kind, Kind::RecentTable);
    }
}

#[test]
fn publisher_inventory_hashes_received_artifacts_in_full() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let path = root.join("source.bin");
    let original = b"network bytes";
    std::fs::write(&path, original).unwrap();
    let expected = Artifact {
        path: "source.bin".into(),
        sha256: digest(original),
        bytes: original.len() as u64,
        kind: Kind::RecentTable,
    };
    record_received_file(&path, &expected, &path).unwrap();
    let changed = b"same size!!!!";
    std::fs::write(&path, changed).unwrap();
    let hashed = hash_artifacts(root, vec![(path, Kind::RecentTable)]).unwrap();
    assert_eq!(hashed[0].0.sha256, digest(changed));
    assert_ne!(hashed[0].0.sha256, expected.sha256);
}

#[test]
fn native_hash_keeps_the_opened_inode_for_verification() {
    use std::io::{Read, Seek, SeekFrom};
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let path = root.join("native.runtime");
    let original = b"the bytes the inventory hashed";
    std::fs::write(&path, original).unwrap();
    let mut hashed = hash_artifacts(root, vec![(path.clone(), Kind::RecentRuntime)]).unwrap();
    let (artifact, _, opened) = hashed.pop().unwrap();
    let replacement = root.join("replacement");
    std::fs::write(&replacement, b"a later path replacement").unwrap();
    std::fs::rename(replacement, &path).unwrap();
    let mut file = opened.expect("native verification needs the hashed handle");
    file.seek(SeekFrom::Start(0)).unwrap();
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes, original);
    assert_eq!(artifact.sha256, digest(&bytes));
    assert_ne!(artifact.sha256, digest(&std::fs::read(path).unwrap()));
}

#[test]
fn an_unreadable_artifact_prevents_finalization() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let valid = root.join("valid.bin");
    std::fs::write(&valid, b"complete file").unwrap();
    assert!(hash_artifacts(
        root,
        vec![
            (valid, Kind::RecentTable),
            (root.join("missing.bin"), Kind::RecentTable),
        ]
    )
    .is_err());
}

#[test]
#[cfg(unix)]
fn artifact_hashing_refuses_a_symbolic_link() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    std::fs::write(root.join("real.bin"), b"complete file").unwrap();
    std::os::unix::fs::symlink(root.join("real.bin"), root.join("link.bin")).unwrap();
    assert!(hash_artifacts(root, vec![(root.join("link.bin"), Kind::RecentTable)]).is_err());
}
/// A stop between bounded reads ends the hash of a file mid-stream, and
/// one before a file ends the listing before it is opened.
#[test]
fn a_stop_ends_listing_between_bounded_reads() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("table.bin");
    std::fs::write(&path, vec![7u8; (2 << 20) + 1]).unwrap();
    let asked = std::sync::atomic::AtomicU32::new(0);
    let stop = || asked.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 1;
    let mut file = std::fs::File::open(&path).unwrap();
    let error = listed_digest(&path, &mut file, None, &stop).unwrap_err();
    assert!(error.is::<Stopped>());
    assert_eq!(
        asked.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "one chunk was read"
    );
    let pending = vec![(path.clone(), Kind::RecentTable, None)];
    let error = hash_listed(dir.path(), pending, &|| true).unwrap_err();
    assert!(error.is::<Stopped>());
    // Resumed without a stop, the same file is hashed whole.
    let pending = vec![(path.clone(), Kind::RecentTable, None)];
    let listed = hash_listed(dir.path(), pending, &|| false).unwrap();
    assert_eq!(listed[0].artifact.sha256, digest(&vec![7u8; (2 << 20) + 1]));
}

#[test]
fn verified_evidence_lists_an_unchanged_file_without_hashing_it() {
    use std::os::unix::fs::FileExt;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("table.bin");
    std::fs::write(&path, b"verified table").unwrap();
    let stamp = FileIdentity::from_metadata(&std::fs::metadata(&path).unwrap()).unwrap();
    let sha = digest(b"verified table");
    let open = || std::fs::File::open(&path).unwrap();
    let before = listed_hashes(dir.path());
    let listed = listed_digest(&path, &mut open(), Some((&sha, 14, stamp)), &|| false).unwrap();
    assert_eq!((listed.sha256, listed.bytes), (sha.clone(), 14));
    assert_eq!(listed_hashes(dir.path()), before);
    // Same size, different bytes: the stamp no longer matches, so the
    // listed digest is that of the bytes now present.
    std::thread::sleep(std::time::Duration::from_millis(20));
    std::fs::File::options()
        .write(true)
        .open(&path)
        .unwrap()
        .write_all_at(b"V", 0)
        .unwrap();
    let listed = listed_digest(&path, &mut open(), Some((&sha, 14, stamp)), &|| false).unwrap();
    assert_eq!(
        (listed.sha256, listed.bytes),
        (digest(b"Verified table"), 14)
    );
    assert_eq!(listed_hashes(dir.path()), before + 1);
    // A replaced path is another inode.
    std::fs::write(dir.path().join("next"), b"verified table").unwrap();
    std::fs::rename(dir.path().join("next"), &path).unwrap();
    listed_digest(&path, &mut open(), Some((&sha, 14, stamp)), &|| false).unwrap();
    assert_eq!(listed_hashes(dir.path()), before + 2);
}

#[test]
fn local_files_without_receipts_are_checked_only_for_size() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("object");
    let bytes = b"correct bytes";
    std::fs::write(&path, bytes).unwrap();
    let expected = Artifact {
        path: "object".into(),
        sha256: digest(bytes),
        bytes: bytes.len() as u64,
        kind: Kind::Metadata,
    };
    verify_file(&path, &expected).unwrap();
    std::fs::write(&path, b"corrupt bytes").unwrap();
    verify_file(&path, &expected).unwrap();
    std::fs::write(&path, b"short").unwrap();
    assert!(verify_file(&path, &expected).is_err());
}

#[test]
fn downloaded_artifacts_use_size_checks_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("object");
    let bytes = b"network bytes";
    std::fs::write(&path, bytes).unwrap();
    let expected = Artifact {
        path: "object".into(),
        sha256: digest(bytes),
        bytes: bytes.len() as u64,
        kind: Kind::Metadata,
    };
    record_received_file(&path, &expected, &path).unwrap();
    verify_file(&path, &expected).unwrap();
    std::fs::write(&path, b"same size!!!!").unwrap();
    verify_file(&path, &expected).unwrap();
    std::fs::write(&path, b"short").unwrap();
    assert!(verify_file(&path, &expected).is_err());
}

/// A publication beside both stores, holding one revision whose segment
/// and one runtime are stored once in them and linked exactly.
fn stored_publication() -> (tempfile::TempDir, PathBuf, String, String, String) {
    let base = tempfile::tempdir().unwrap();
    let root = base.path().join("candidate-1");
    let revision = "ab".repeat(32);
    let object = digest(b"rows");
    let runtime = format!("{}-{}.runtime", "cd".repeat(32), "ef".repeat(32));
    let objects = base.path().join(OBJECT_STORE);
    for dir in [&root.join(&revision), &objects] {
        std::fs::create_dir_all(dir).unwrap();
    }
    std::fs::write(objects.join(&object), b"rows").unwrap();
    std::fs::write(objects.join(digest(b"runtime")), b"runtime").unwrap();
    std::fs::write(root.join(&revision).join("filter.bin"), b"filter").unwrap();
    let link = |target: PathBuf, at: PathBuf| std::os::unix::fs::symlink(target, at).unwrap();
    link(
        Path::new("../..").join(OBJECT_STORE).join(&object),
        root.join(&revision).join("pages.0.bin"),
    );
    link(
        Path::new("..").join(OBJECT_STORE).join(digest(b"runtime")),
        root.join(&runtime),
    );
    (base, root, revision, object, runtime)
}

#[test]
fn exact_store_links_resolve_to_their_stored_file() {
    let (base, root, revision, object, runtime) = stored_publication();
    assert_eq!(
        resolve(&root, &format!("{revision}/pages.0.bin"))
            .unwrap()
            .0,
        base.path().join(OBJECT_STORE).join(&object)
    );
    assert_eq!(
        resolve(&root, &runtime).unwrap().0,
        base.path().join(OBJECT_STORE).join(digest(b"runtime"))
    );
    // Plain files still resolve to themselves.
    assert_eq!(
        resolve(&root, &format!("{revision}/filter.bin")).unwrap().0,
        root.join(&revision).join("filter.bin")
    );
}

#[test]
fn store_redirects_traversal_and_missing_targets_fail_closed() {
    let (base, root, revision, object, runtime) = stored_publication();
    let refused = |relative: &str| resolve(&root, relative).is_err();
    let symlink = |target: &Path, at: PathBuf| std::os::unix::fs::symlink(target, at).unwrap();
    assert!(refused(&format!("../{OBJECT_STORE}/{object}")));
    assert!(refused(&format!("{revision}/../{revision}/pages.0.bin")));
    assert!(refused(&format!("{revision}/absent.bin")));
    // Metadata is never a link, nor is a revision directory.
    let dir = root.join(&revision);
    std::fs::remove_file(dir.join("filter.bin")).unwrap();
    symlink(
        &Path::new("../..").join(OBJECT_STORE).join(&object),
        dir.join("filter.bin"),
    );
    assert!(refused(&format!("{revision}/filter.bin")));
    let linked = "12".repeat(32);
    symlink(Path::new(&revision), root.join(&linked));
    assert!(refused(&format!("{linked}/pages.0.bin")));
    // A link anywhere but its store, or under another name, is refused.
    let outside = base.path().join("outside");
    std::fs::write(&outside, b"rows").unwrap();
    symlink(&outside, dir.join("pages.1.bin"));
    assert!(refused(&format!("{revision}/pages.1.bin")));
    symlink(Path::new("../../outside"), dir.join("pages.2.bin"));
    assert!(refused(&format!("{revision}/pages.2.bin")));
    symlink(
        &Path::new("../..").join(OBJECT_STORE).join("not-a-digest"),
        dir.join("pages.3.bin"),
    );
    assert!(refused(&format!("{revision}/pages.3.bin")));
    // A runtime may not use a segment's depth, nor a segment a runtime's.
    let other = format!("{}-{}.runtime", "11".repeat(32), "22".repeat(32));
    symlink(
        &Path::new("../..")
            .join(OBJECT_STORE)
            .join(digest(b"runtime")),
        root.join(&other),
    );
    assert!(refused(&other));
    symlink(
        &Path::new("..").join(OBJECT_STORE).join(&object),
        dir.join("pages.5.bin"),
    );
    assert!(refused(&format!("{revision}/pages.5.bin")));
    // A missing object is refused, as is an object that is itself a link.
    symlink(
        &Path::new("../..").join(OBJECT_STORE).join("33".repeat(32)),
        dir.join("pages.4.bin"),
    );
    assert!(refused(&format!("{revision}/pages.4.bin")));
    let objects = base.path().join(OBJECT_STORE);
    std::fs::remove_file(objects.join(digest(b"runtime"))).unwrap();
    symlink(&outside, objects.join(digest(b"runtime")));
    assert!(refused(&runtime));
    // A store that is itself a link is refused.
    let moved = base.path().join("moved");
    std::fs::rename(base.path().join(OBJECT_STORE), &moved).unwrap();
    symlink(&moved, base.path().join(OBJECT_STORE));
    assert!(refused(&format!("{revision}/pages.0.bin")));
}

#[test]
fn listed_evidence_is_settled_stable_and_never_survives_a_change() {
    use std::os::unix::fs::FileExt;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("x.runtime");
    std::fs::write(&path, b"runtime bytes").unwrap();
    let open = || std::fs::File::open(&path).unwrap();
    // Just written: hashed, but too recent to vouch for the file later.
    let listed = listed_digest(&path, &mut open(), None, &|| false).unwrap();
    assert_eq!(
        (listed.sha256.as_str(), listed.bytes),
        (digest(b"runtime bytes").as_str(), 13)
    );
    assert!(!listed.settled);
    // Settled: the stamp of the hashed inode becomes evidence.
    let old = std::time::SystemTime::now() - std::time::Duration::from_secs(10);
    open().set_modified(old).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(1100));
    let listed = listed_digest(&path, &mut open(), None, &|| false).unwrap();
    assert!(
        listed.settled,
        "a settled unchanged file vouches for itself"
    );
    let stamp = listed.stamp;
    let sha = listed.sha256;
    let hashed = listed_hashes(dir.path());
    let known = Some((sha.as_str(), 13, stamp));
    let again = listed_digest(&path, &mut open(), known, &|| false).unwrap();
    assert_eq!((again.sha256.as_str(), again.stamp), (sha.as_str(), stamp));
    assert_eq!(
        listed_hashes(dir.path()),
        hashed,
        "no bytes were read again"
    );
    // A same-size write with the modification time restored still
    // changes the stamp, so the new bytes are hashed and listed.
    open_write(&path).write_all_at(b"R", 0).unwrap();
    open().set_modified(old).unwrap();
    let changed = listed_digest(&path, &mut open(), known, &|| false).unwrap();
    assert_eq!(changed.sha256, digest(b"Runtime bytes"));
    assert!(
        !changed.settled,
        "a change this recent proves nothing later"
    );
    assert_eq!(listed_hashes(dir.path()), hashed + 1);
    // A replaced inode with the original bytes is hashed again too.
    std::fs::write(dir.path().join("next"), b"runtime bytes").unwrap();
    std::fs::rename(dir.path().join("next"), &path).unwrap();
    listed_digest(&path, &mut open(), known, &|| false).unwrap();
    assert_eq!(listed_hashes(dir.path()), hashed + 2);
}

fn open_write(path: &Path) -> std::fs::File {
    std::fs::File::options().write(true).open(path).unwrap()
}

#[test]
fn rejects_unsafe_paths_and_noncanonical_digests() {
    for p in [
        "../x",
        "data.bin.received.json",
        "data.bin.checked-receipt",
        "/x",
        "a//b",
        "a/./b",
        "a/../b",
        "a%2fb",
        "https://host/x",
        "x?y",
        "x#y",
        "C:x",
        r"a\b",
        "",
    ] {
        assert!(!safe_path(p), "{p}");
    }
    assert!(safe_path("abc-123/directory.0.bin"));
    assert!(is_digest(&"ab".repeat(32)));
    assert!(!is_digest(&"AB".repeat(32)));
}

#[test]
fn anchor_batches_bind_one_snapshot_and_answer_every_anchor_once() {
    let current = Current {
        schema: SCHEMA.into(),
        revision: 7,
        recovery_epoch: 2,
        inventory_sha256: "11".repeat(32),
        network: "main".into(),
        genesis_hash: "00".repeat(32),
        anchor_height: 9,
        anchor_hash: "22".repeat(32),
        eligibility: Eligibility::Canonical,
        invalidated_from: None,
    };
    let pair = |height, byte: &str| AnchorPair {
        height,
        hash: byte.repeat(32),
    };
    assert!(AnchorBatchRequest::new(&current, vec![]).is_err());
    assert!(AnchorBatchRequest::new(&current, vec![pair(1, "aa"), pair(1, "aa")]).is_err());
    assert!(AnchorBatchRequest::new(&current, vec![pair(1, "AA")]).is_err());
    let many = (0..=MAX_ANCHOR_BATCH as u64)
        .map(|h| pair(h, "aa"))
        .collect();
    assert!(AnchorBatchRequest::new(&current, many).is_err());
    let full: Vec<_> = (0..MAX_ANCHOR_BATCH as u64)
        .map(|h| pair(h, "aa"))
        .collect();
    let largest = AnchorBatchRequest::new(&current, full).unwrap();
    assert!(serde_json::to_vec(&largest).unwrap().len() < MAX_ANCHOR_BATCH_BYTES);
    let response = AnchorBatch {
        schema: SCHEMA.into(),
        revision: 7,
        recovery_epoch: 2,
        inventory_sha256: "11".repeat(32),
        genesis_hash: "00".repeat(32),
        results: (0..MAX_ANCHOR_BATCH as u64)
            .map(|height| AnchorResult {
                height,
                hash: "aa".repeat(32),
                canonical: true,
            })
            .collect(),
    };
    assert!(serde_json::to_vec(&response).unwrap().len() < MAX_ANCHOR_BATCH_BYTES);

    let request = AnchorBatchRequest::new(&current, vec![pair(1, "aa"), pair(2, "bb")]).unwrap();
    assert!(request.names(&current));
    let mut other = current.clone();
    other.inventory_sha256 = "33".repeat(32);
    assert!(!request.names(&other));
    let result = |height, byte: &str, canonical| AnchorResult {
        height,
        hash: byte.repeat(32),
        canonical,
    };
    let batch = AnchorBatch {
        schema: SCHEMA.into(),
        revision: 7,
        recovery_epoch: 2,
        inventory_sha256: "11".repeat(32),
        genesis_hash: "00".repeat(32),
        results: vec![result(2, "bb", false), result(1, "aa", true)],
    };
    let answers = batch.answers(&request, &current.genesis_hash).unwrap();
    assert!(answers[&pair(1, "aa")]);
    assert!(!answers[&pair(2, "bb")]);
    assert!(batch.answers(&request, &"44".repeat(32)).is_err());
    for broken in [
        vec![result(1, "aa", true)],
        vec![result(1, "aa", true), result(1, "aa", true)],
        vec![
            result(1, "aa", true),
            result(2, "bb", false),
            result(1, "aa", true),
        ],
        vec![result(1, "aa", true), result(3, "bb", false)],
    ] {
        let mut wrong = batch.clone();
        wrong.results = broken;
        assert!(wrong.answers(&request, &current.genesis_hash).is_err());
    }
    for mutate in [
        |b: &mut AnchorBatch| b.revision += 1,
        |b: &mut AnchorBatch| b.recovery_epoch += 1,
        |b: &mut AnchorBatch| b.inventory_sha256 = "33".repeat(32),
        |b: &mut AnchorBatch| b.schema = "other".into(),
    ] {
        let mut wrong = batch.clone();
        mutate(&mut wrong);
        assert!(wrong.answers(&request, &current.genesis_hash).is_err());
    }
}

#[test]
fn opening_an_artifact_whose_link_count_changed_is_not_a_confirmed_mismatch() {
    let artifact = Artifact {
        path: "shard/filter.bin".into(),
        sha256: digest(b"filter"),
        bytes: 6,
        kind: Kind::RecentTable,
    };
    let changed: Error = Box::new(ObjectFileError::FileChanged("filter.bin".into()));
    assert!(matches!(
        validation::open_failure(&artifact, &changed),
        IntegrityError::Unavailable(_)
    ));
    let unreadable: Error = Box::new(ObjectFileError::Io(io::ErrorKind::NotFound.into()));
    assert!(matches!(
        validation::open_failure(&artifact, &unreadable),
        IntegrityError::Unavailable(_)
    ));
    let not_file: Error = Box::new(ObjectFileError::NotRegularFile("filter.bin".into()));
    assert!(matches!(
        validation::open_failure(&artifact, &not_file),
        IntegrityError::Mismatch {
            found_sha256: None,
            ..
        }
    ));
    let escaping: Error = "publisher artifact link leaves the object store".into();
    assert!(matches!(
        validation::open_failure(&artifact, &escaping),
        IntegrityError::Mismatch { .. }
    ));
}
