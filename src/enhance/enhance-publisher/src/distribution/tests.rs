use super::retention::collect_files;
use super::*;

#[test]
fn collect_files_removes_only_unretained_owned_files() {
    let scratch = tempfile::tempdir().unwrap();
    let directory = scratch.path().join(pir_artifact::namespace("objects"));
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
