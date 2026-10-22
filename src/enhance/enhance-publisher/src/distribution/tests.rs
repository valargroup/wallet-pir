use super::retention::{collect_files, collect_units};
use super::*;

#[test]
fn collect_files_removes_only_unretained_owned_files() {
    let scratch = tempfile::tempdir().unwrap();
    let directory = scratch.path().join(pir_artifact::namespace("objects"));
    assert!(collect_files(&directory, &BTreeSet::new()).is_ok());
    fs::create_dir_all(&directory).unwrap();
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

#[test]
fn collect_units_retires_only_unretained_units_the_preparation_engine_wrote() {
    let scratch = tempfile::tempdir().unwrap();
    let engine_root = crate::publication::engine_root(scratch.path());
    // The directory the preparation engine writes its units to.
    let units = engine_root.join(enhance_query_server::runtime::UNIT_DIRECTORY);
    let (active, obsolete) = ("ab".repeat(32), "cd".repeat(32));
    for name in [&active, &obsolete, &"not-a-unit".to_string()] {
        fs::create_dir_all(units.join(name).join("enhance")).unwrap();
    }
    collect_units(&engine_root, &BTreeSet::from([active.clone()])).unwrap();
    assert!(units.join(&active).join("enhance").is_dir());
    assert!(!units.join(&obsolete).exists());
    assert!(units.join("not-a-unit").is_dir());
}
