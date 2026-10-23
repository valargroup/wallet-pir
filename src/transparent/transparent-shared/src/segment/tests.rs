use super::*;

fn small_source(dir: &Path) -> SegmentSource {
    let path = dir.join("source.bin");
    let bytes = vec![0u8; 32];
    std::fs::write(&path, &bytes).unwrap();
    SegmentSource::published(path, 2, 16, hex::encode(Sha256::digest(&bytes)), 2)
}

/// Waits until a change made now gets a later ctime than `path` has.
///
/// Filesystems take ctime from a coarse clock, so an unlink or write in
/// the tick of the previous change leaves the stamp unchanged.
fn next_timestamp_tick(path: &Path) {
    use std::os::unix::fs::MetadataExt;
    let ctime = |path: &Path| {
        let meta = std::fs::metadata(path).unwrap();
        (meta.ctime(), meta.ctime_nsec())
    };
    let last = ctime(path);
    let probe = path.with_extension("tick");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let _ = std::fs::remove_file(&probe);
        std::fs::write(&probe, []).unwrap();
        if ctime(&probe) > last {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "filesystem timestamp did not advance within five seconds"
        );
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}

#[test]
fn serving_sources_need_no_receipt_and_never_rehash_local_contents() {
    let dir = tempfile::tempdir().unwrap();
    let source = small_source(dir.path());
    let expected = std::fs::read(source.path()).unwrap();
    let serving = source.clone().for_serving().unwrap();
    let mut changed = expected.clone();
    changed[0] ^= 1;
    std::fs::write(source.path(), &changed).unwrap();
    assert_eq!(serving.load().unwrap(), changed);
    assert!(
        source.verify().is_err(),
        "publisher still checks source contents"
    );
    std::fs::write(source.path(), &expected[..expected.len() - 1]).unwrap();
    assert!(serving.load().is_err());
    assert!(source.for_serving().is_err());
}

#[test]
fn collecting_a_hard_link_rechecks_the_complete_source() {
    let dir = tempfile::tempdir().unwrap();
    let source = small_source(dir.path());
    let link = dir.path().join("retired.bin");
    std::fs::hard_link(source.path(), &link).unwrap();
    let mut reads = 0;
    let evidence = source
        .verify_stamped_observing(
            || {
                reads += 1;
                if reads == 1 {
                    next_timestamp_tick(source.path());
                    std::fs::remove_file(&link).unwrap();
                }
            },
            NEVER,
        )
        .unwrap();
    assert_eq!(
        reads, 2,
        "a changed identity requires another complete hash"
    );
    assert!(
        evidence.is_none(),
        "a freshly changed file cannot supply reusable evidence"
    );
}

#[test]
fn a_changed_row_is_rejected_after_a_verification_race() {
    let dir = tempfile::tempdir().unwrap();
    let source = small_source(dir.path());
    let mut reads = 0;
    let result = source.verify_stamped_observing(
        || {
            reads += 1;
            let mut changed = [0u8; 32];
            changed[0] = 1;
            next_timestamp_tick(source.path());
            std::fs::write(source.path(), changed).unwrap();
        },
        NEVER,
    );
    assert!(
        matches!(result, Err(LoadError::Invalid(_))),
        "the second hash must reject changed bytes"
    );
    assert_eq!(
        reads, 1,
        "a digest mismatch fails before another successful read"
    );
}

#[test]
fn persistent_file_identity_changes_exhaust_the_verification_bound() {
    let dir = tempfile::tempdir().unwrap();
    let source = small_source(dir.path());
    let link = dir.path().join("retired.bin");
    let mut reads = 0;
    let result = source.verify_stamped_observing(
        || {
            reads += 1;
            next_timestamp_tick(source.path());
            if link.exists() {
                std::fs::remove_file(&link).unwrap();
            } else {
                std::fs::hard_link(source.path(), &link).unwrap();
            }
        },
        NEVER,
    );
    assert!(matches!(
        result,
        Err(LoadError::ChangedDuringVerification { .. })
    ));
    assert_eq!(reads, 3, "verification must not retry indefinitely");
}

#[test]
fn restoring_modification_time_does_not_hide_a_changed_source() {
    use std::fs::File;
    use std::os::unix::fs::FileExt;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("source.bin");
    let bytes = vec![0u8; 32];
    std::fs::write(&path, &bytes).unwrap();
    let file = File::options().write(true).open(&path).unwrap();
    let modified = std::time::SystemTime::now() - std::time::Duration::from_secs(60);
    file.set_modified(modified).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(1100));
    let source =
        SegmentSource::published(path.clone(), 2, 16, hex::encode(Sha256::digest(&bytes)), 2)
            .verify_into(&mut VerificationReport::default(), true, NEVER)
            .unwrap();
    assert!(source.evidence.is_some());
    file.write_all_at(&[1], 0).unwrap();
    file.set_modified(modified).unwrap();
    let candidate = SegmentSource::published(path, 2, 16, source.sha256.clone(), 2);
    assert!(
        candidate
            .verify_or_reuse(
                Some(&source),
                &mut VerificationReport::default(),
                true,
                NEVER,
            )
            .is_err(),
        "a changed source must be hashed and rejected even if its modification time was restored"
    );
}

/// A stop between bounded reads ends verification with nothing proved:
/// no evidence, no receipt; verification resumed later hashes it whole.
#[test]
fn a_stop_during_a_source_scan_proves_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("source.bin");
    let bytes = vec![0u8; (2 << 20) + 32];
    std::fs::write(&path, &bytes).unwrap();
    let rows = bytes.len() as u64 / 32;
    let source = SegmentSource::published(
        path.clone(),
        rows,
        32,
        hex::encode(Sha256::digest(&bytes)),
        rows,
    );
    let asked = std::sync::atomic::AtomicU32::new(0);
    let stop = || asked.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 1;
    let mut report = VerificationReport::default();
    let result = source
        .clone()
        .verify_or_reuse(None, &mut report, true, &stop);
    assert!(matches!(result, Err(LoadError::Stopped)));
    assert_eq!(
        asked.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "stopped after one chunk"
    );
    assert_eq!(report.segments_hashed, 0);
    assert!(crate::verification::Receipt::new(
        &path,
        &std::fs::File::open(&path).unwrap(),
        &source.verification_context()
    )
    .is_none_or(|receipt| !receipt.matches()));
    source
        .verify_or_reuse(None, &mut report, true, NEVER)
        .unwrap();
    assert_eq!(report.segments_hashed, 1);
}

fn hashed_loads() -> u64 {
    LOADS_HASHED.with(std::cell::Cell::get)
}

/// A file verified with settled evidence by this process.
fn settled_source(dir: &Path) -> (SegmentSource, std::fs::File, std::time::SystemTime) {
    let path = dir.join("source.bin");
    let bytes = vec![0u8; 32];
    std::fs::write(&path, &bytes).unwrap();
    let file = std::fs::File::options().write(true).open(&path).unwrap();
    let modified = std::time::SystemTime::now() - std::time::Duration::from_secs(60);
    file.set_modified(modified).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(1100));
    let source = SegmentSource::published(path, 2, 16, hex::encode(Sha256::digest(&bytes)), 2)
        .verify_into(&mut VerificationReport::default(), true, NEVER)
        .unwrap();
    assert!(source.evidence.is_some());
    (source, file, modified)
}

#[test]
fn an_unchanged_verified_source_loads_without_another_hash() {
    let dir = tempfile::tempdir().unwrap();
    let (source, _file, _) = settled_source(dir.path());
    let before = hashed_loads();
    assert_eq!(source.load().unwrap(), vec![0u8; 32]);
    assert_eq!(hashed_loads(), before, "the evidence vouches for the file");
    let unverified = SegmentSource::published(
        source.path().to_path_buf(),
        2,
        16,
        source.sha256().to_string(),
        2,
    );
    unverified.load().unwrap();
    assert_eq!(hashed_loads(), before + 1, "a first load is hashed");
}

#[test]
fn a_same_size_change_with_restored_mtime_fails_an_evidence_backed_load() {
    use std::os::unix::fs::FileExt;
    let dir = tempfile::tempdir().unwrap();
    let (source, file, modified) = settled_source(dir.path());
    next_timestamp_tick(source.path());
    file.write_all_at(&[1], 0).unwrap();
    file.set_modified(modified).unwrap();
    let before = hashed_loads();
    assert!(source.load().is_err());
    assert_eq!(hashed_loads(), before + 1, "changed ctime forces a hash");
}

#[test]
fn a_replaced_path_is_hashed_again_even_with_identical_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let (source, _file, _) = settled_source(dir.path());
    let replacement = dir.path().join("replacement.bin");
    std::fs::write(&replacement, vec![0u8; 32]).unwrap();
    std::fs::rename(&replacement, source.path()).unwrap();
    let before = hashed_loads();
    source.load().unwrap();
    assert_eq!(hashed_loads(), before + 1, "another inode has no evidence");
    std::fs::write(source.path(), vec![1u8; 32]).unwrap();
    assert!(source.load().is_err());
}

/// A segment whose rows past the ones its queries select hold data is refused
/// on verification and on load, so no runtime is built or restored from it.
#[test]
fn a_segment_with_data_past_its_selected_rows_is_refused() {
    use sha2::Digest;
    use zakura_pir_query_server::transparent::native as transparent_native;
    let dir = tempfile::tempdir().unwrap();
    let (rows, row_bytes) = (2 * transparent_native::D as u64, 16u32);
    let mut bytes = vec![0u8; rows as usize * row_bytes as usize];
    bytes[7] = 1;
    let source = |bytes: &[u8], zero_from_row| {
        let path = dir.path().join("pages.0.bin");
        std::fs::write(&path, bytes).unwrap();
        SegmentSource::published(
            path,
            rows,
            row_bytes,
            hex::encode(sha2::Sha256::digest(bytes)),
            zero_from_row,
        )
    };
    let half = transparent_native::D as u64;
    assert!(source(&bytes, half).verify().is_ok());
    assert!(source(&bytes, half).load().is_ok());
    bytes[half as usize * row_bytes as usize] = 1;
    assert!(source(&bytes, half).verify().is_err());
    assert!(source(&bytes, half).load().is_err());
    assert!(source(&bytes, rows).verify().is_ok());
}
