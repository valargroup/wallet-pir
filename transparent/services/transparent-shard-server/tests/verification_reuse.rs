//! Verification carried across directory revisions.
//!
//! A republished tail is a new revision whose sealed page shards are the same
//! committed tables, hard-linked from the revision before. A worker that has
//! already hashed those files does not hash them again: it takes the
//! verification over when the candidate's segment is the same committed
//! segment (identity, kind, digest, geometry) and resolves to the very file
//! it hashed, unmodified since. Anything else — a replaced file, an in-place
//! write, a missing file, a restart — is verified in full.

use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use zakura_transparent_private_shared::events::{ReceiveEvent, TransparentEvent, Txid};
use zakura_transparent_private_shared::filter::{
    BlockHash, ScriptBytes, SealParameters, ShardMap, ShardMapEntry,
};
#[path = "support/revision.rs"]
mod revision;
use transparent_shard_server::shardset::{
    LoadOptions, ShardSet, TableRef, VerificationReport, DEFAULT_RETAIN_REVISIONS,
};
use zakura_transparent_private_server::shard::build::build_directory_shard;
use zakura_transparent_private_shared::shard::layout::{Geometry, DECOUPLED_2K_8K};
use zakura_transparent_private_shared::shard::manifest::pages_table;

const GEOMETRY: &Geometry = &DECOUPLED_2K_8K;
const GENESIS: &str = zakura_transparent_private_shared::filter::MAINNET_GENESIS_DISPLAY;
const FIRST: u64 = 3_428_143;

fn genesis() -> BlockHash {
    BlockHash::from_display_hex(GENESIS).unwrap()
}

fn hash_at(height: u64) -> BlockHash {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&height.to_le_bytes());
    BlockHash::from_internal_bytes(bytes)
}

fn script(tag: u32) -> ScriptBytes {
    let mut bytes = vec![0x76, 0xa9, 0x14];
    bytes.extend_from_slice(&tag.to_le_bytes());
    bytes.extend_from_slice(&[0u8; 16]);
    bytes.extend_from_slice(&[0x88, 0xac]);
    ScriptBytes::new(bytes)
}

/// Every event through `last`: a few light scripts at every height, and in
/// the first page shard's range enough long runs — each two fragments, so two
/// whole page rows — that the sealed page shard spans two segments.
fn events_through(last: u64) -> Vec<(ScriptBytes, TransparentEvent)> {
    let event = |tag: u32, height: u64, n: u64| {
        let mut txid = [0u8; 32];
        txid[..8].copy_from_slice(&height.to_le_bytes());
        txid[8..16].copy_from_slice(&n.to_le_bytes());
        (
            script(tag),
            TransparentEvent::Receive(ReceiveEvent {
                metadata: None,
                height: height as u32,
                txid: Txid(txid),
                transaction_index: n as u16,
                output_index: 0,
                value: 1_000 + n,
                coinbase: false,
            }),
        )
    };
    let mut events = Vec::new();
    for height in FIRST..=last {
        for n in 0..LIGHT_SCRIPTS {
            events.push(event(n as u32, height, n));
        }
        if height < FIRST + RUN_EVENTS {
            for k in 0..LONG_RUNS {
                events.push(event(1_000 + k as u32, height, LIGHT_SCRIPTS + k));
            }
        }
    }
    events
}

const LIGHT_SCRIPTS: u64 = 8;
/// One more event than a page fragment holds, so each run is two rows.
const RUN_EVENTS: u64 =
    zakura_transparent_private_shared::shard::layout::EVENTS_PER_PAGE as u64 + 1;
/// Enough two-row runs to pass one 8,192-row segment.
const LONG_RUNS: u64 = 4_160;
/// Where the first page shard ends: sealed in every revision.
const FIRST_CHILD_END: u64 = FIRST + 99;

/// Builds revision `revision` of shard zero, ending at `end`, with page
/// shards ending at `ends`, into `<dir>/<digest>`. Page shards the previous
/// revision `link_from` published with the same identity are hard-linked from
/// it rather than written, as the publisher does.
fn write_revision(
    dir: &Path,
    end: u64,
    ends: &[u64],
    revision: u32,
    supersedes: &str,
    link_from: Option<&Path>,
) -> ShardMapEntry {
    let spec = revision::Revision {
        shard_id: 0,
        start: FIRST,
        end,
        geometry: GEOMETRY,
        sealed: false,
        revision,
        supersedes,
        parent_digest: "",
        children: ends.len() as u32,
        genesis: genesis(),
        hash: &hash_at,
    };
    let events = events_through(end);
    let blocks = (FIRST..=end)
        .map(|height| {
            (
                height,
                events
                    .iter()
                    .filter(|(_, event)| u64::from(event.height()) == height)
                    .cloned()
                    .collect(),
            )
        })
        .collect();
    let (built, children) = build_directory_shard(
        0,
        GEOMETRY,
        genesis(),
        zakura_transparent_private_shared::filter::RANGE_PROFILE,
        blocks,
        ends,
        &hash_at,
        false,
    )
    .unwrap();
    let (manifest, entry) = revision::write_built(dir, &spec, &built, &children);
    if let Some(from) = link_from {
        let old: zakura_transparent_private_shared::shard::manifest::ShardManifest =
            serde_json::from_slice(&std::fs::read(from.join("manifest.json")).unwrap()).unwrap();
        for (child, published) in manifest.page_children.iter().enumerate() {
            if old.page_children.get(child) != Some(published) {
                continue;
            }
            for segment in 0..published.page_segments.len() {
                let name = format!("{}.{segment}.bin", pages_table(child as u32));
                let target = dir.join(&entry.manifest_digest).join(&name);
                std::fs::remove_file(&target).unwrap();
                std::fs::hard_link(from.join(&name), &target).unwrap();
            }
        }
    }
    entry
}

fn write_map(dir: &Path, entry: &ShardMapEntry) {
    let map = ShardMap {
        genesis_hash: GENESIS.to_string(),
        network: zakura_transparent_private_shared::filter::NETWORK.to_string(),
        profile: zakura_transparent_private_shared::filter::RANGE_PROFILE.to_string(),
        range_envelope_version: zakura_transparent_private_shared::filter::RANGE_ENVELOPE_VERSION,
        start_height: FIRST,
        seal: std::collections::BTreeMap::from([(
            GEOMETRY.name.to_string(),
            SealParameters {
                max_scripts: 8_192,
                max_page_rows: 2_048,
                max_txids: 0,
            },
        )]),
        shards: vec![entry.clone()],
        recuts: Vec::new(),
    };
    std::fs::write(
        dir.join("shards.json"),
        serde_json::to_vec_pretty(&map).unwrap(),
    )
    .unwrap();
}

/// Moves every file's modification time back a minute, as for files
/// published well before a worker loads them. Evidence is only kept for a
/// file whose last write predates its hashing by more than a timestamp tick.
fn age(dir: &Path) {
    let past = SystemTime::now() - Duration::from_secs(60);
    for entry in walk(dir) {
        File::options()
            .write(true)
            .open(&entry)
            .unwrap()
            .set_modified(past)
            .unwrap();
    }
}

fn walk(dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            files.extend(walk(&path));
        } else {
            files.push(path);
        }
    }
    files
}

/// Links a revision directory into another publication directory.
fn link_revision(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        std::fs::hard_link(entry.path(), to.join(entry.file_name())).unwrap();
    }
}

fn open(dir: &Path, previous: Option<&ShardSet>) -> Result<ShardSet, String> {
    ShardSet::open_reusing(dir, &LoadOptions::whole(DEFAULT_RETAIN_REVISIONS), previous)
        .map_err(|error| error.to_string())
}

/// Bytes of every page segment of a revision's children past `from`.
fn page_bytes(set: &ShardSet, digest: &str, from: usize) -> u64 {
    let shard = set.revision(digest).unwrap();
    (from..shard.children.len())
        .flat_map(|child| {
            (0..shard.segments(TableRef::Pages(child as u32))).map(move |segment| (child, segment))
        })
        .map(|(child, segment)| {
            shard
                .segment(TableRef::Pages(child as u32), segment)
                .unwrap()
                .bytes() as u64
        })
        .sum()
}

/// The world every case starts from: publication one holding revision zero,
/// loaded and verified; publication two holding revision one beside it, its
/// first page shard hard-linked from revision zero.
struct World {
    _root: tempfile::TempDir,
    root: PathBuf,
    p1: PathBuf,
    p2: PathBuf,
    r0: ShardMapEntry,
    r1: ShardMapEntry,
    first: ShardSet,
}

impl World {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().to_path_buf();
        let store = path.join("store");
        let r0 = write_revision(
            &store,
            FIRST + 149,
            &[FIRST_CHILD_END, FIRST + 149],
            0,
            "",
            None,
        );
        let r1 = write_revision(
            &store,
            FIRST + 199,
            &[FIRST_CHILD_END, FIRST + 149, FIRST + 199],
            1,
            &r0.manifest_digest,
            Some(&store.join(&r0.manifest_digest)),
        );
        age(&store);
        let p1 = path.join("p1");
        link_revision(
            &store.join(&r0.manifest_digest),
            &p1.join(&r0.manifest_digest),
        );
        write_map(&p1, &r0);
        let p2 = path.join("p2");
        for entry in [&r0, &r1] {
            link_revision(
                &store.join(&entry.manifest_digest),
                &p2.join(&entry.manifest_digest),
            );
        }
        write_map(&p2, &r1);
        // Link operations also change ctime; evidence is retained only once
        // both filesystem clocks predate verification by a full timestamp tick.
        std::thread::sleep(Duration::from_millis(1100));
        let first = open(&p1, None).unwrap();
        Self {
            _root: root,
            root: path,
            p1,
            p2,
            r0,
            r1,
            first,
        }
    }

    /// The first page shard's segment files in revision one of `dir`.
    fn sealed_pages(&self, dir: &Path) -> Vec<PathBuf> {
        let shard = self.first.revision(&self.r0.manifest_digest).unwrap();
        (0..shard.segments(TableRef::Pages(0)))
            .map(|segment| {
                dir.join(&self.r1.manifest_digest)
                    .join(format!("{}.{segment}.bin", pages_table(0)))
            })
            .collect()
    }

    /// A third publication linking everything publication two holds.
    fn p3(&self) -> PathBuf {
        let p3 = self.root.join("p3");
        for entry in [&self.r0, &self.r1] {
            link_revision(
                &self.p2.join(&entry.manifest_digest),
                &p3.join(&entry.manifest_digest),
            );
        }
        write_map(&p3, &self.r1);
        p3
    }
}

#[test]
fn the_first_load_hashes_every_table_and_a_restart_hashes_them_again() {
    let world = World::new();
    let report = world.first.verification().clone();
    let shard = world.first.revision(&world.r0.manifest_digest).unwrap();
    assert!(
        shard.segments(TableRef::Pages(0)) > 1,
        "the sealed page shard spans several segments"
    );
    assert_eq!(report.segments_reused, 0);
    assert_eq!(
        report.page_bytes_hashed,
        page_bytes(&world.first, &world.r0.manifest_digest, 0)
    );
    assert!(report.wall_micros > 0);

    // A restart holds no evidence: every table of both revisions is hashed.
    let restarted = open(&world.p2, None).unwrap();
    let report = restarted.verification();
    assert_eq!(report.segments_reused, 0);
    assert_eq!(
        report.page_bytes_hashed,
        page_bytes(&restarted, &world.r0.manifest_digest, 0)
            + page_bytes(&restarted, &world.r1.manifest_digest, 0)
    );
}

#[test]
fn unchanged_sealed_pages_are_not_hashed_again_in_a_new_revision_and_directory() {
    let world = World::new();
    let second = open(&world.p2, Some(&world.first)).unwrap();
    let report: &VerificationReport = second.verification();
    // Revision zero is retained whole; of revision one, only the page shards
    // revision zero did not publish are hashed. The sealed first page shard,
    // every segment of it, costs no table bytes.
    assert_eq!(
        report.page_bytes_hashed,
        page_bytes(&second, &world.r1.manifest_digest, 1),
        "{report:?}"
    );
    let sealed = second.revision(&world.r1.manifest_digest).unwrap();
    for segment in 0..sealed.segments(TableRef::Pages(0)) {
        let source = sealed.segment(TableRef::Pages(0), segment).unwrap();
        assert!(
            source.path().starts_with(&world.p2),
            "rebound into the candidate"
        );
    }

    // Collecting the publication the evidence was first taken in breaks
    // nothing: every source names the candidate's links.
    std::fs::remove_dir_all(&world.p1).unwrap();
    for shard in second.revisions() {
        for table in shard.tables() {
            for segment in 0..shard.segments(table) {
                let source = shard.segment(table, segment).unwrap();
                assert!(source.path().starts_with(&world.p2));
                source.load().unwrap();
            }
        }
    }

    // Adding hard links changes ctime. The next load conservatively hashes
    // them again, then unchanged files carry their new verification forward.
    let p3 = world.p3();
    std::thread::sleep(Duration::from_millis(1100));
    let third = open(&p3, Some(&second)).unwrap();
    assert!(third.verification().bytes_hashed > 0);
    let fourth = open(&p3, Some(&third)).unwrap();
    assert_eq!(fourth.verification().bytes_hashed, 0);
}

#[test]
fn a_replaced_file_is_hashed_and_a_corrupt_one_refused() {
    let world = World::new();
    let pages = world.sealed_pages(&world.p2);
    // Same bytes, new inode: the evidence does not cover it, so it is hashed.
    let bytes = std::fs::read(&pages[0]).unwrap();
    std::fs::remove_file(&pages[0]).unwrap();
    std::fs::write(&pages[0], &bytes).unwrap();
    let second = open(&world.p2, Some(&world.first)).unwrap();
    assert_eq!(
        second.verification().page_bytes_hashed,
        // Removing the old link also changes the retained inode's ctime.
        page_bytes(&second, &world.r1.manifest_digest, 1) + 2 * bytes.len() as u64
    );

    // Same length, one bit off, new inode: refused.
    let mut corrupt = bytes.clone();
    corrupt[0] ^= 1;
    std::fs::remove_file(&pages[0]).unwrap();
    std::fs::write(&pages[0], &corrupt).unwrap();
    age(&world.p2);
    let error = open(&world.p2, Some(&world.first)).err().unwrap();
    assert!(
        error.contains("does not match its manifest digest"),
        "{error}"
    );
}

#[test]
fn an_in_place_write_voids_the_evidence() {
    let world = World::new();
    let p3 = world.p3();
    std::thread::sleep(Duration::from_millis(1100));
    let second = open(&world.p2, Some(&world.first)).unwrap();
    let pages = world.sealed_pages(&p3);
    // Rewrite the same bytes in place: same inode and length, new mtime.
    // Every link sees it — both revisions publish this page shard as the
    // one file — so it is hashed again under each, and passes.
    let file = OpenOptions::new().write(true).open(&pages[0]).unwrap();
    let original = std::fs::read(&pages[0]).unwrap();
    file.write_all_at(&original[..64], 0).unwrap();
    drop(file);
    let third = open(&p3, Some(&second)).unwrap();
    assert_eq!(
        third.verification().page_bytes_hashed,
        2 * original.len() as u64
    );

    // A changed byte in place is refused, though inode and length agree.
    let file = OpenOptions::new().write(true).open(&pages[0]).unwrap();
    file.write_all_at(&[original[0] ^ 1], 0).unwrap();
    drop(file);
    let error = open(&p3, Some(&second)).err().unwrap();
    assert!(
        error.contains("does not match its manifest digest"),
        "{error}"
    );
}

#[test]
fn a_missing_file_is_refused_whatever_the_evidence() {
    let world = World::new();
    let pages = world.sealed_pages(&world.p2);
    std::fs::remove_file(&pages[1]).unwrap();
    let error = open(&world.p2, Some(&world.first)).err().unwrap();
    assert!(error.contains(&pages[1].display().to_string()), "{error}");
}

#[test]
fn a_file_with_unsettled_timestamps_is_not_vouched_for() {
    let world = World::new();
    let p3 = world.p3();
    std::thread::sleep(Duration::from_millis(1100));
    // Keep the same inode under every publication. A future modification time
    // makes its stamp uncertain regardless of how long fixture hashing takes.
    let pages = world.sealed_pages(&world.p2);
    let bytes = std::fs::read(&pages[0]).unwrap();
    let file = OpenOptions::new().write(true).open(&pages[0]).unwrap();
    file.write_all_at(&bytes[..64], 0).unwrap();
    file.set_modified(SystemTime::now() + Duration::from_secs(3600))
        .unwrap();
    let second = open(&world.p2, Some(&world.first)).unwrap();
    let third = open(&p3, Some(&second)).unwrap();
    // Both revisions publish this inode. Neither may inherit its verification
    // while its timestamp is uncertain.
    assert_eq!(
        third.verification().page_bytes_hashed,
        2 * bytes.len() as u64
    );
}

#[test]
fn catalog_publications_preserve_verification_across_changing_maps_and_collection() {
    let world = World::new();
    let catalog = world.root.join(".sources");
    std::fs::create_dir(&catalog).unwrap();
    for entry in [&world.r0, &world.r1] {
        std::fs::rename(
            world.p2.join(&entry.manifest_digest),
            catalog.join(&entry.manifest_digest),
        )
        .unwrap();
    }
    let publish = |round: usize, current: &ShardMapEntry| {
        let path = world.root.join(format!("{round:064x}"));
        std::fs::create_dir(&path).unwrap();
        for entry in [&world.r0, &world.r1]
            .into_iter()
            .filter(|entry| entry.revision <= current.revision)
        {
            std::os::unix::fs::symlink(
                format!("../.sources/{}", entry.manifest_digest),
                path.join(&entry.manifest_digest),
            )
            .unwrap();
        }
        write_map(&path, current);
        path
    };
    let first_dir = publish(0, &world.r0);
    let first = open(&first_dir, None).unwrap();
    let second_dir = publish(1, &world.r1);
    let mut previous = open(&second_dir, Some(&first)).unwrap();
    assert_eq!(
        previous.verification().page_bytes_hashed,
        page_bytes(&previous, &world.r1.manifest_digest, 1)
    );
    // Three ordinary publications and collection must never mutate source
    // identity. Restart still hashes every assigned source independently.
    for round in 2..5 {
        let next_dir = publish(round, &world.r1);
        let next = open(&next_dir, Some(&previous)).unwrap();
        assert_eq!(
            next.verification().bytes_hashed,
            0,
            "{:?}",
            next.verification()
        );
        assert!(next.verification().segments_reused > 0);
        eprintln!("catalog round {round}: {:?}", next.verification());
        previous = next;
    }
    std::fs::remove_dir_all(first_dir).unwrap();
    std::fs::remove_dir_all(second_dir).unwrap();
    let final_dir = world.root.join(format!("{:064x}", 4));
    let restarted = open(&final_dir, None).unwrap();
    assert_eq!(restarted.verification().segments_reused, 0);
    assert!(restarted.verification().bytes_hashed > 0);
    // A one-row source change must fail even while prior evidence exists.
    let pages = world.sealed_pages(&final_dir);
    let file = OpenOptions::new().write(true).open(&pages[0]).unwrap();
    file.write_at(&[0xff], 8).unwrap();
    assert!(open(&final_dir, Some(&previous)).is_err());
}

#[test]
fn catalog_links_reject_redirects_and_missing_sources() {
    let world = World::new();
    let catalog = world.root.join(".sources");
    std::fs::create_dir(&catalog).unwrap();
    let publication = world.root.join("linked");
    std::fs::create_dir(&publication).unwrap();
    write_map(&publication, &world.r1);
    let link = publication.join(&world.r1.manifest_digest);
    for target in [
        world.p2.join(&world.r1.manifest_digest),
        PathBuf::from(format!("../.sources/{}", world.r1.manifest_digest)),
    ] {
        std::os::unix::fs::symlink(target, &link).unwrap();
        assert!(open(&publication, Some(&world.first))
            .err()
            .unwrap()
            .contains("invalid revision catalog link"));
        std::fs::remove_file(&link).unwrap();
    }
    let stored = catalog.join(&world.r1.manifest_digest);
    std::os::unix::fs::symlink(world.p2.join(&world.r1.manifest_digest), &stored).unwrap();
    std::os::unix::fs::symlink(format!("../.sources/{}", world.r1.manifest_digest), &link).unwrap();
    assert!(open(&publication, Some(&world.first))
        .err()
        .unwrap()
        .contains("invalid revision catalog link"));
}
