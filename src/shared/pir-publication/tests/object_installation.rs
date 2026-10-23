//! Filesystem contracts shared by byte-producing and file-producing publishers.
use pir_publication::{ObjectFileError, ObjectInstaller, ObjectReference, PublishedObjectStore};
use std::{
    cell::Cell,
    fs::{self, File},
    io::{self, Cursor, Read},
    os::unix::fs::{FileExt, MetadataExt},
    path::Path,
    sync::mpsc,
    time::Duration,
};

fn identity(path: &Path) -> (u64, u64, i64, i64, u64) {
    let metadata = fs::metadata(path).unwrap();
    (
        metadata.dev(),
        metadata.ino(),
        metadata.ctime(),
        metadata.ctime_nsec(),
        metadata.nlink(),
    )
}

#[test]
fn byte_and_prepared_file_publishers_reuse_the_same_completed_object() {
    let root = tempfile::tempdir().unwrap();
    let directory = root.path().join("objects");
    let store = PublishedObjectStore::open(directory.clone()).unwrap();
    let reference = store.store(b"prepared object").unwrap();
    let object = directory.join(&reference.sha256);
    let before = identity(&object);
    let prepared = root.path().join("prepared");
    fs::write(&prepared, b"prepared object").unwrap();
    let installed = ObjectInstaller::open(directory.clone())
        .unwrap()
        .install_prepared_file(&prepared, Some(&reference.sha256), &|| false)
        .unwrap();
    assert!(!installed.installed);
    assert_eq!(installed.reference, reference);
    assert_eq!(store.store(b"prepared object").unwrap(), reference);
    assert_eq!(identity(&object), before);
    assert_eq!(fs::read(&object).unwrap(), b"prepared object");
    assert!(fs::metadata(&object).unwrap().permissions().readonly());
    assert!(
        store.open_retained(&reference.sha256).is_err(),
        "installation grants no serving authority"
    );

    let fresh = root.path().join("fresh-prepared");
    fs::write(&fresh, b"another prepared object").unwrap();
    let adopted = ObjectInstaller::open(directory.clone())
        .unwrap()
        .install_prepared_file(&fresh, None, &|| false)
        .unwrap();
    assert!(adopted.installed);
    let completed = directory.join(&adopted.reference.sha256);
    assert_eq!(
        fs::metadata(&completed).unwrap().ino(),
        fs::metadata(&fresh).unwrap().ino(),
        "adoption does not copy the prepared body"
    );
    assert_eq!(
        store.store(b"another prepared object").unwrap(),
        adopted.reference
    );
}

#[test]
fn conflicting_existing_objects_are_refused_without_replacement() {
    let root = tempfile::tempdir().unwrap();
    let installer = ObjectInstaller::open(root.path().join("objects")).unwrap();
    let reference = ObjectReference::for_bytes(b"expected");
    let target = installer.directory().join(&reference.sha256);
    for bytes in [b"conflict".as_slice(), b"short"] {
        fs::write(&target, bytes).unwrap();
        let before = identity(&target);
        assert!(installer
            .install_stream(b"expected".as_slice(), &|| false)
            .is_err());
        assert_eq!(fs::read(&target).unwrap(), bytes);
        assert_eq!(identity(&target), before);
    }
    fs::remove_file(&target).unwrap();
    let outside = root.path().join("outside");
    fs::write(&outside, b"expected").unwrap();
    std::os::unix::fs::symlink(&outside, &target).unwrap();
    assert!(installer
        .install_stream(b"expected".as_slice(), &|| false)
        .is_err());
    assert!(fs::symlink_metadata(&target)
        .unwrap()
        .file_type()
        .is_symlink());
    assert_eq!(fs::read(&outside).unwrap(), b"expected");
}

struct FailingInput(bool);
impl Read for FailingInput {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if self.0 {
            return Err(io::Error::other("input interrupted"));
        }
        self.0 = true;
        buffer[..4].copy_from_slice(b"part");
        Ok(4)
    }
}

#[test]
fn interrupted_input_and_cancelled_verification_expose_no_completed_object() {
    let root = tempfile::tempdir().unwrap();
    let installer = ObjectInstaller::open(root.path().join("objects")).unwrap();
    assert!(installer
        .install_stream(FailingInput(false), &|| false)
        .is_err());
    assert_eq!(fs::read_dir(installer.directory()).unwrap().count(), 0);
    let source = root.path().join("prepared");
    fs::write(&source, vec![7; (1 << 20) + 1]).unwrap();
    let polls = Cell::new(0);
    let error = installer
        .install_prepared_file(&source, None, &|| {
            polls.set(polls.get() + 1);
            polls.get() == 3
        })
        .unwrap_err();
    assert!(matches!(error, ObjectFileError::Cancelled), "{error}");
    assert_eq!(fs::read_dir(installer.directory()).unwrap().count(), 0);
}

struct PausedInput {
    input: Cursor<Vec<u8>>,
    started: Option<mpsc::SyncSender<()>>,
    resume: mpsc::Receiver<()>,
}
impl Read for PausedInput {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if self.input.position() == 0 {
            return self.input.read(&mut buffer[..4]);
        }
        if let Some(started) = self.started.take() {
            started.send(()).unwrap();
            self.resume.recv_timeout(Duration::from_secs(5)).unwrap();
        }
        self.input.read(buffer)
    }
}

#[test]
fn concurrent_installation_never_exposes_partial_bytes_or_replaces_the_winner() {
    let root = tempfile::tempdir().unwrap();
    let directory = root.path().join("objects");
    let installer = ObjectInstaller::open(directory.clone()).unwrap();
    let bytes = b"one complete immutable object";
    let reference = ObjectReference::for_bytes(bytes);
    let target = directory.join(&reference.sha256);
    let (started, ready) = mpsc::sync_channel(0);
    let (resume, paused) = mpsc::sync_channel(0);
    let pending = std::thread::spawn(move || {
        ObjectInstaller::open(directory)
            .unwrap()
            .install_stream(
                PausedInput {
                    input: Cursor::new(bytes.to_vec()),
                    started: Some(started),
                    resume: paused,
                },
                &|| false,
            )
            .unwrap()
    });
    ready.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(
        !target.exists(),
        "the first writer has only staged a prefix"
    );
    let prepared = root.path().join("prepared");
    fs::write(&prepared, bytes).unwrap();
    let winner = installer
        .install_prepared_file(&prepared, Some(&reference.sha256), &|| false)
        .unwrap();
    assert!(winner.installed);
    let before = identity(&target);
    resume.send(()).unwrap();
    let second = pending.join().unwrap();
    assert!(!second.installed);
    assert_eq!(second.reference, reference);
    assert_eq!(identity(&target), before);
    assert_eq!(fs::read(&target).unwrap(), bytes);
    assert_eq!(fs::read_dir(installer.directory()).unwrap().count(), 1);
}

#[test]
fn replacement_and_mutation_during_verification_or_before_installation_fail() {
    for (replace, change_on_poll) in [(false, 3), (true, 3), (false, 6), (true, 6)] {
        let root = tempfile::tempdir().unwrap();
        let installer = ObjectInstaller::open(root.path().join("objects")).unwrap();
        let source = root.path().join("prepared");
        let original = vec![5; (1 << 20) + 1];
        fs::write(&source, &original).unwrap();
        // Keep an already-open writer to exercise mutation despite read-only
        // installation permissions. The restored mtime must not hide it.
        let writer = File::options().write(true).open(&source).unwrap();
        let modified = writer.metadata().unwrap().modified().unwrap();
        let replacement = root.path().join("replacement");
        fs::write(&replacement, &original).unwrap();
        let polls = Cell::new(0);
        let result = installer.install_prepared_file(&source, None, &|| {
            polls.set(polls.get() + 1);
            if polls.get() == change_on_poll {
                if replace {
                    fs::rename(&replacement, &source).unwrap();
                } else {
                    // Restoring mtime leaves ctime as the change signal. Cross
                    // the supported one-second timestamp tick before writing.
                    std::thread::sleep(Duration::from_millis(1100));
                    writer.write_all_at(&[9], 0).unwrap();
                    writer.set_modified(modified).unwrap();
                }
            }
            false
        });
        assert!(
            matches!(result, Err(ObjectFileError::FileChanged(_))),
            "replace={replace}, poll={change_on_poll}: {result:?}"
        );
        assert_eq!(fs::read_dir(installer.directory()).unwrap().count(), 0);
    }
}

#[test]
fn a_prepared_file_must_match_its_declared_digest() {
    let root = tempfile::tempdir().unwrap();
    let installer = ObjectInstaller::open(root.path().join("objects")).unwrap();
    let source = root.path().join("prepared");
    fs::write(&source, b"wrong prepared file").unwrap();
    let expected = ObjectReference::for_bytes(b"expected");
    assert!(matches!(
        installer.install_prepared_file(&source, Some(&expected.sha256), &|| false),
        Err(ObjectFileError::DigestMismatch { .. })
    ));
    assert_eq!(fs::read_dir(installer.directory()).unwrap().count(), 0);
}
