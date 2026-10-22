//! Real History publications keep reusable verification across object references.
use std::{
    collections::BTreeMap,
    fs,
    os::unix::fs::{FileExt, MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    time::Duration,
};
use transparent_publisher::history::{
    publication::{publish, DirectoryChoice, PublishOptions},
    store::{adopt_runtimes, adopt_segments},
};
use transparent_shared::{
    history::shardset::{LoadOptions, ShardSet, DEFAULT_RETAIN_REVISIONS},
    journal::events::EventStore,
    publication::{CheckedRuntimes, Finalized, Kind},
    runtime::prebuild::prepare_publication,
};
use zakura_pir_protocol::transparent::events::{
    FeeState, ReceiveEvent, TransactionMetadata, TransparentEvent, Txid,
};
use zakura_pir_publisher::transparent::filter::{BlockHash, ScriptBytes, RANGE_PROFILE};

fn journal(path: &Path) -> EventStore {
    let mut journal = EventStore::open(path, &"00".repeat(32), 0).unwrap();
    for height in 0..2 {
        let event = TransparentEvent::Receive(ReceiveEvent {
            metadata: Some(TransactionMetadata {
                fee: FeeState::Exact(0),
                transparent_input_count: 1,
                has_shielded_components: false,
            }),
            height,
            txid: Txid([height as u8 + 1; 32]),
            transaction_index: 0,
            output_index: 0,
            value: 100,
            coinbase: false,
        });
        journal
            .append_block(
                u64::from(height),
                BlockHash::from_internal_bytes([height as u8 + 1; 32]),
                &[(ScriptBytes::new(vec![0x51, height as u8]), event)],
            )
            .unwrap();
    }
    journal.commit().unwrap();
    journal
}

fn publish_candidate(journal: &EventStore, output: &Path, previous: Option<&Path>) {
    publish(
        &PublishOptions {
            data_dir: PathBuf::new(),
            output: output.into(),
            previous: previous.map(Path::to_path_buf),
            recent_geometry: "decoupled-2k-8k".into(),
            archive_geometry: None,
            recent_from: None,
            zakura_rpc_url: String::new(),
            zakura_cookie: PathBuf::new(),
            through: None,
            record: None,
            source_sha: None,
            directory_choice: DirectoryChoice::Off,
            range_profile: RANGE_PROFILE.into(),
        },
        journal,
        BlockHash::from_internal_bytes([0; 32]),
    )
    .unwrap();
}

#[derive(Debug, PartialEq, Eq)]
struct StoredObjectIdentity {
    path: PathBuf,
    inode: u64,
    changed: (i64, i64),
    links: u64,
}

fn objects(root: &Path, publication: &Finalized) -> BTreeMap<String, StoredObjectIdentity> {
    publication
        .inventory()
        .artifacts
        .iter()
        .filter(|artifact| artifact.kind != Kind::Metadata)
        .map(|artifact| {
            let logical = root.join(&artifact.path);
            assert!(fs::symlink_metadata(&logical)
                .unwrap()
                .file_type()
                .is_symlink());
            let physical = fs::canonicalize(&logical).unwrap();
            let metadata = fs::metadata(&physical).unwrap();
            (
                artifact.sha256.clone(),
                StoredObjectIdentity {
                    path: physical,
                    inode: metadata.ino(),
                    changed: (metadata.ctime(), metadata.ctime_nsec()),
                    links: metadata.nlink(),
                },
            )
        })
        .collect()
}

#[test]
fn successive_publications_reuse_table_and_runtime_bodies_without_changing_objects() {
    let root = tempfile::tempdir().unwrap();
    let journal = journal(&root.path().join("journal"));
    let publications = root.path().join("publications");
    fs::create_dir(&publications).unwrap();
    let options = LoadOptions {
        prune_excess: true,
        ..LoadOptions::whole(DEFAULT_RETAIN_REVISIONS)
    };
    let mut checked = CheckedRuntimes::default();
    let mut previous: Option<(PathBuf, ShardSet)> = None;
    let mut original = None;
    for cycle in 0..3 {
        let directory = publications.join(format!("candidate-{cycle}"));
        publish_candidate(
            &journal,
            &directory,
            previous.as_ref().map(|(path, _)| path.as_path()),
        );
        let tables = adopt_segments(&directory, &|| false).unwrap();
        let set =
            ShardSet::open_reusing(&directory, &options, previous.as_ref().map(|(_, set)| set))
                .unwrap();
        let retained: Vec<_> = previous
            .as_ref()
            .map(|(path, _)| path.clone())
            .into_iter()
            .collect();
        prepare_publication(&set, &directory, &retained, &|| false).unwrap();
        let runtimes = adopt_runtimes(&directory, &|| false).unwrap();
        let finalized =
            Finalized::prepare_reusing(&directory, &set, 0, true, &mut checked).unwrap();
        let current = objects(&directory, &finalized);
        assert!(!current.is_empty());
        if cycle == 0 {
            original = Some(current);
        } else {
            assert_eq!(
                tables.bytes_hashed, 0,
                "carrying a table reference must not reinstall it"
            );
            assert_eq!(
                runtimes.bytes_hashed, 0,
                "carrying a runtime reference must not reinstall it"
            );
            assert_eq!(
                Some(&current),
                original.as_ref(),
                "publication references changed stored inode metadata"
            );
        }
        if cycle == 2 {
            assert_eq!(
                set.verification().bytes_hashed,
                0,
                "settled table evidence must be reusable"
            );
            assert_eq!(
                checked.loaded, 0,
                "unchanged runtimes must not be loaded again"
            );
            let metadata: u64 = finalized
                .inventory()
                .artifacts
                .iter()
                .filter(|artifact| artifact.kind == Kind::Metadata)
                .map(|artifact| artifact.bytes)
                .sum();
            assert_eq!(
                checked.bytes_hashed, metadata,
                "only local metadata may be hashed again"
            );
            let runtime = finalized
                .inventory()
                .artifacts
                .iter()
                .find(|artifact| artifact.kind == Kind::RecentRuntime)
                .unwrap();
            let object = fs::canonicalize(directory.join(&runtime.path)).unwrap();
            // Corruption with restored mtime must still invalidate publisher
            // evidence through the inode's change time.
            fs::set_permissions(&object, fs::Permissions::from_mode(0o644)).unwrap();
            let file = fs::File::options()
                .read(true)
                .write(true)
                .open(&object)
                .unwrap();
            let modified = file.metadata().unwrap().modified().unwrap();
            let mut byte = [0];
            file.read_exact_at(&mut byte, 100).unwrap();
            file.write_all_at(&[byte[0] ^ 1], 100).unwrap();
            file.set_modified(modified).unwrap();
            assert!(Finalized::prepare_reusing(&directory, &set, 0, true, &mut checked).is_err());
        }
        previous = Some((directory, set));
        if cycle < 2 {
            // Real files must cross the filesystem timestamp-granularity
            // boundary before their verification can vouch for a later pass.
            std::thread::sleep(Duration::from_millis(1100));
        }
    }
}
