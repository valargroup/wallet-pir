use zakura_pir_publisher::transparent as transparent_shard;
#[cfg(feature = "qualification-tests")]
use zakura_pir_publisher::transparent::filter as transparent_filter;

use super::*;
use crate::tx_detail::fixture::{self, chain_records};
#[cfg(feature = "qualification-tests")]
use crate::tx_detail::fixture::{block_hash, chain_sources};
#[cfg(feature = "qualification-tests")]
use crate::tx_detail::offline::{bench_recent, synthetic_records};
use crate::tx_detail::offline::{plan_start, verify};
#[cfg(feature = "qualification-tests")]
use crate::tx_detail::publisher::CANDIDATE_PREFIX;
use crate::tx_detail::publisher::{bootstrap, SEALED_DIR};
use crate::tx_detail::source::{open_writer, Script, ScriptStep};
use crate::tx_detail::timeline::read_timeline;
use std::collections::BTreeSet;
use std::os::unix::fs::{FileExt, MetadataExt};
#[cfg(feature = "qualification-tests")]
use transparent_filter::BlockHash;
#[cfg(feature = "qualification-tests")]
use transparent_shard::txid::{DisplayEntry, DisplayRecord, ENTRY_BYTES};
#[cfg(feature = "qualification-tests")]
use transparent_shard::txid_v2x::TransparentDisplayRecordV2x;

/// Leaves runtimes to finalization; only the tests that check runtimes
/// build them.
fn no_prebuild(_revision: &Path, _candidate: &Path) -> Result<Vec<Shipped>, String> {
    Ok(Vec::new())
}

fn settings() -> Settings {
    Settings {
        exit_when_idle: true,
        trust_sealed: true,
        record_maps: true,
        prebuild: no_prebuild,
        ..Settings::default()
    }
}

/// Settings that re-verify every sealed revision when a controller opens.
fn verifying() -> Settings {
    Settings {
        trust_sealed: false,
        ..settings()
    }
}

fn advance(from: u64, to: u64, step: u64) -> Vec<ScriptStep> {
    let mut steps = Vec::new();
    let mut height = from;
    while height < to {
        height = (height + step).min(to);
        steps.push(ScriptStep::Advance(height));
    }
    steps
}

struct Run {
    outcome: Outcome,
    maps: Vec<DisplayMap>,
    active: ActiveRecord,
}

async fn run_script(
    root: &Path,
    journal: &Path,
    steps: Vec<ScriptStep>,
    settings: Settings,
) -> Run {
    let store = open_writer(journal).unwrap();
    let status = settings.status.clone();
    let mut controller =
        Controller::open(root, store, Source::Script(Script::new(steps)), settings).unwrap();
    let outcome = tokio::time::timeout(
        Duration::from_secs(120),
        controller.run(crate::host_engine::PublisherEngine),
    )
    .await
    .unwrap_or_else(|_| panic!("the controller settles: {}", status.read().unwrap()))
    .unwrap();
    Run {
        outcome,
        maps: controller.maps().to_vec(),
        active: controller.active().clone(),
    }
}

type SealIdentity = (u64, u64, u64, String, String);

/// Seal identity without the tip it was decided at, which depends on pacing.
fn identities(seals: &[SealRecord]) -> Vec<SealIdentity> {
    seals
        .iter()
        .map(|s| {
            (
                s.shard_id,
                s.start_height,
                s.end_height,
                s.digest.clone(),
                s.terminal_block_hash.clone(),
            )
        })
        .collect()
}

fn listing(dir: &Path) -> BTreeSet<String> {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect()
}

fn bootstrap_at(
    journal: &Path,
    root: &Path,
    layout: &DisplayRoot,
    through: u64,
) -> Vec<SealRecord> {
    let store = EventStore::open_existing(journal).unwrap();
    bootstrap(&store, root, layout, through).unwrap().seals
}

fn layout(journal: &Path, max_archive_shards: u64) -> DisplayRoot {
    fixture::layout(
        &EventStore::open_existing(journal).unwrap(),
        101,
        max_archive_shards,
    )
}

#[cfg(feature = "qualification-tests")]
fn active_map(root: &Path) -> DisplayMap {
    let active = ActiveRecord::load(root).unwrap();
    read_map(&active.directory, &active.map_sha256).unwrap()
}

#[cfg(feature = "qualification-tests")]
fn sealed_count(map: &DisplayMap) -> usize {
    map.shards.iter().filter(|s| s.sealed).count()
}

/// Every map is well formed and keeps its predecessor's archives, apart from
/// the ones its window dropped.
fn check_maps(maps: &[DisplayMap], first: &DisplayMap) {
    let mut previous = first;
    for map in maps {
        map.check_shape().unwrap();
        let dropped = (map.first_shard_id - previous.first_shard_id) as usize;
        check_sealed_continuity(previous, map, dropped).unwrap();
        previous = map;
    }
}

/// Checks each map's recent shard is the window ending at its tip: the
/// latest start whose blocks hold `window` txids (`held` counts a height's),
/// capped at the first unsealed height and floored at the map's start.
/// Returns how many maps' recent shards overlap an archive.
#[cfg(feature = "qualification-tests")]
fn check_windows(maps: &[DisplayMap], window: u64, held: impl Fn(u64) -> u64) -> usize {
    let mut overlapping = 0;
    for map in maps {
        let recent = map.shards.last().unwrap();
        let (mut start, mut sum) = (recent.end_height + 1, 0u64);
        while sum < window && start > map.start_height {
            start -= 1;
            sum += held(start);
        }
        assert_eq!(
            recent.start_height,
            start.min(map.unsealed_start()),
            "{recent:?}"
        );
        overlapping += usize::from(recent.start_height < map.unsealed_start());
    }
    overlapping
}

/// A window of about eight blocks, against archives of three or four: the
/// recent shard reaches over several archives, moves back when a rollback
/// empties the tip, and is the same after a restart, whose cache must reach
/// back as far.
#[cfg(feature = "qualification-tests")]
#[tokio::test(flavor = "multi_thread")]
async fn the_recent_window_overlaps_archives_through_reorgs_and_restarts() {
    let temp = tempfile::tempdir().unwrap();
    let journal = temp.path().join("journal");
    fixture::write_journal(&journal, 100, 130, 0);
    let mut layout = layout(&journal, 100);
    layout.seal.recent_window = 25;
    let root = temp.path().join("root");
    bootstrap_at(&journal, &root, &layout, 112);
    let first = active_map(&root);
    let run = run_script(&root, &journal, advance(112, 130, 2), settings()).await;
    assert_eq!(run.outcome, Outcome::Idle);
    check_maps(&run.maps, &first);
    let original = |h: u64| chain_records(0, h).len() as u64;
    // A cycle that publishes while a seal is still staging leaves a wider
    // unsealed range, which can hold the window alone; once idle, every seal
    // is published and the window reaches back over several archives.
    assert!(check_windows(&run.maps, 25, original) > 0);
    let last = run.maps.last().unwrap().clone();
    assert!(last.unsealed_start() - last.shards.last().unwrap().start_height > 3);

    // The tip block is replaced by an empty one: the window reaches back.
    let tip = 130;
    let forked = move |h: u64| match h {
        h if h < tip => original(h),
        h if h == tip => 0,
        h => chain_records(9, h).len() as u64,
    };
    let steps = vec![ScriptStep::Reorg {
        ancestor: tip - 1,
        blocks: vec![(block_hash(9, tip), vec![])],
    }];
    let run = run_script(&root, &journal, steps, settings()).await;
    assert_eq!(run.outcome, Outcome::Idle);
    check_maps(&run.maps, &last);
    check_windows(&run.maps, 25, forked);
    let moved = run.maps.last().unwrap().clone();
    assert!(moved.shards.last().unwrap().start_height < last.shards.last().unwrap().start_height);

    // A restart reloads the cache from the window's floor and goes on.
    let steps = vec![ScriptStep::Extend(fork(9, tip + 1, tip + 12))];
    let run = run_script(&root, &journal, steps, settings()).await;
    assert_eq!(run.outcome, Outcome::Idle);
    check_maps(&run.maps, &moved);
    check_windows(&run.maps, 25, forked);
    assert!(sealed_count(run.maps.last().unwrap()) > sealed_count(&moved));
}

#[cfg(feature = "qualification-tests")]
#[tokio::test(flavor = "multi_thread")]
async fn incremental_stepping_reproduces_bootstrap() {
    let through = if cfg!(feature = "qualification-tests") {
        125
    } else {
        118
    };
    let steps = if cfg!(feature = "qualification-tests") {
        vec![1, 7]
    } else {
        vec![1]
    };
    let temp = tempfile::tempdir().unwrap();
    let journal = temp.path().join("journal");
    fixture::write_journal(&journal, 100, through, 0);
    let layout = layout(&journal, 100);
    let whole_root = temp.path().join("whole");
    let whole = bootstrap_at(&journal, &whole_root, &layout, through);
    for step in steps {
        let root = temp.path().join(format!("step-{step}"));
        let started = bootstrap_at(&journal, &root, &layout, 112);
        assert!(
            started.len() >= 2
                && whole.len()
                    >= started.len()
                        + if cfg!(feature = "qualification-tests") {
                            3
                        } else {
                            1
                        },
            "{} then {} seals",
            started.len(),
            whole.len()
        );
        let first = active_map(&root);
        let run = run_script(&root, &journal, advance(112, through, step), settings()).await;
        assert_eq!(run.outcome, Outcome::Idle);
        assert_eq!(
            identities(&run.active.seals),
            identities(&whole),
            "step {step}"
        );
        // The same sealed revisions, byte for byte: digests name contents,
        // and each verifies from disk.
        assert_eq!(
            listing(&root.join(SEALED_DIR)),
            listing(&whole_root.join(SEALED_DIR))
        );
        for seal in &run.active.seals[started.len()..] {
            verify_dir(&root.join(SEALED_DIR).join(&seal.digest), &seal.digest).unwrap();
        }
        assert!(run.maps.len() >= ((through - 112) / step) as usize);
        check_maps(&run.maps, &first);
        assert_eq!(run.maps.last().unwrap().covered_through(), Some(through));
        check_windows(&run.maps, fixture::params().recent_window, |h| {
            chain_records(0, h).len() as u64
        });
        // Retention: the active candidate and three others at most.
        let candidates = listing(&root)
            .into_iter()
            .filter(|n| n.starts_with(CANDIDATE_PREFIX))
            .count();
        assert!(candidates <= 4, "{candidates} candidates");
        // Every seal made while running is recorded with the tip that decided it.
        let events = read_timeline(&root).unwrap();
        let seals: Vec<_> = events
            .iter()
            .filter(|e| e["kind"] == "seal" && e["bootstrap"].is_null())
            .collect();
        assert_eq!(seals.len(), whole.len() - started.len());
        for seal in seals {
            let (decided, end) = (seal["decided_tip"].as_u64(), seal["end"].as_u64());
            assert!(decided.unwrap() >= end.unwrap() + fixture::params().reorg_margin);
        }
        assert!(events
            .iter()
            .any(|e| e["kind"] == "cycle" && e["sealed_published"] != json!([])));
    }
}

/// The first replaced height of every rollback the timeline recorded, in order.
#[cfg(feature = "qualification-tests")]
fn reorgs_from(root: &Path) -> Vec<u64> {
    read_timeline(root)
        .unwrap()
        .iter()
        .filter(|e| e["kind"] == "reorg")
        .filter_map(|e| e["from_height"].as_u64())
        .collect()
}

/// The rollback record `invalidate.json` holds.
#[cfg(feature = "qualification-tests")]
fn saved_rollback(root: &Path) -> Value {
    serde_json::from_slice::<Value>(&std::fs::read(root.join(INVALIDATE_FILE)).unwrap()).unwrap()
        ["rollback"]
        .clone()
}

#[cfg(feature = "qualification-tests")]
fn fork(tag: u8, from: u64, through: u64) -> Vec<(BlockHash, Vec<TransparentDisplayRecordV2x>)> {
    (from..=through)
        .map(|h| (block_hash(tag, h), chain_sources(tag, h)))
        .collect()
}

#[cfg(feature = "qualification-tests")]
#[tokio::test(flavor = "multi_thread")]
async fn reorgs_rebuild_recent_and_a_sealed_fork_halts() {
    let temp = tempfile::tempdir().unwrap();
    let journal = temp.path().join("journal");
    fixture::write_journal(&journal, 100, 125, 0);
    let layout = layout(&journal, 100);
    let root = temp.path().join("root");
    let sealed = bootstrap_at(&journal, &root, &layout, 118);

    // A fork two blocks below the tip, inside the margin.
    let steps = vec![
        ScriptStep::Advance(121),
        ScriptStep::Reorg {
            ancestor: 119,
            blocks: fork(1, 120, 122),
        },
    ];
    let run = run_script(&root, &journal, steps, settings()).await;
    assert_eq!(run.outcome, Outcome::Idle);
    let (before, after) = (&run.maps[0], run.maps.last().unwrap());
    check_maps(&run.maps, before);
    let recent = after.shards.last().unwrap();
    assert_eq!(recent.end_height, 122);
    assert_eq!(
        recent.terminal_block_hash,
        block_hash(1, 122).to_display_hex()
    );
    assert!(identities(&run.active.seals).starts_with(&identities(&sealed)));
    if before.shards.last().unwrap().shard_id == recent.shard_id {
        // The rebuilt revision supersedes the one the fork orphaned.
        let manifest: DisplayManifest = serde_json::from_slice(
            &std::fs::read(
                run.active
                    .directory
                    .join(&recent.manifest_digest)
                    .join(MANIFEST_FILE),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(
            manifest.supersedes,
            before.shards.last().unwrap().manifest_digest
        );
    }
    // The rollback replaced published coverage from the first forked height.
    assert_eq!(reorgs_from(&root), vec![120]);
    // The tooling index followed the fork.
    let index = HeightIndex::open(&root).unwrap();
    let orphaned: BTreeSet<_> = chain_records(0, 120).iter().map(|r| r.txid.0).collect();
    assert!(index
        .read_all()
        .unwrap()
        .iter()
        .all(|(txid, _)| !orphaned.contains(txid)));
    assert_eq!(index.last_height().unwrap(), Some(122));
    drop(index);

    // A fork reaching into a sealed range halts without changing anything.
    let last_seal = run.active.seals.last().unwrap().clone();
    let floor = last_seal.end_height;
    let active = std::fs::read(root.join("active.json")).unwrap();
    let steps = vec![ScriptStep::Reorg {
        ancestor: floor - 1,
        blocks: fork(2, floor, floor + 8),
    }];
    let run = run_script(&root, &journal, steps, settings()).await;
    assert!(
        matches!(run.outcome, Outcome::Halted(ref r) if r.contains("sealed floor")),
        "{:?}",
        run.outcome
    );
    assert!(root.join(HALTED_FILE).exists());
    assert_eq!(std::fs::read(root.join("active.json")).unwrap(), active);
    let store = EventStore::open_existing(&journal).unwrap();
    assert_eq!(store.covered_through(), Some(122));
    assert_eq!(
        store.block_at(floor).unwrap().block_hash.to_display_hex(),
        last_seal.terminal_block_hash
    );
    drop(store);
    // A halted root stays halted across restarts.
    let run = run_script(&root, &journal, advance(122, 123, 1), settings()).await;
    assert!(matches!(run.outcome, Outcome::Halted(_)));
}

type FileIdentity = BTreeMap<PathBuf, (u64, i64)>;

fn sealed_files(root: &Path) -> FileIdentity {
    let mut out = BTreeMap::new();
    for dir in std::fs::read_dir(root.join(SEALED_DIR)).unwrap() {
        for file in std::fs::read_dir(dir.unwrap().path()).unwrap() {
            let file = file.unwrap();
            let meta = file.metadata().unwrap();
            out.insert(
                file.path(),
                (meta.ino(), meta.mtime() * 1_000_000_000 + meta.mtime_nsec()),
            );
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn restart_resumes_without_touching_sealed_files() {
    let middle = if cfg!(feature = "qualification-tests") {
        121
    } else {
        118
    };
    let through = if cfg!(feature = "qualification-tests") {
        130
    } else {
        124
    };
    let temp = tempfile::tempdir().unwrap();
    let journal = temp.path().join("journal");
    fixture::write_journal(&journal, 100, through, 0);
    let layout = layout(&journal, 100);
    let whole = bootstrap_at(&journal, &temp.path().join("whole"), &layout, through);
    let root = temp.path().join("root");
    bootstrap_at(&journal, &root, &layout, 112);
    let first = run_script(&root, &journal, advance(112, middle, 9), verifying()).await;
    assert_eq!(first.outcome, Outcome::Idle);
    let sealed_before = sealed_files(&root);
    // Recovery verifies sealed bytes and continues from the activated tip.
    let second = run_script(&root, &journal, advance(middle, through, 9), verifying()).await;
    assert_eq!(second.outcome, Outcome::Idle);
    assert_eq!(identities(&second.active.seals), identities(&whole));
    let sealed_after = sealed_files(&root);
    for (path, identity) in &sealed_before {
        assert_eq!(sealed_after.get(path), Some(identity), "{}", path.display());
    }
    check_maps(&second.maps, first.maps.last().unwrap());
    // No seal is decided twice across the restart.
    let decided: Vec<_> = read_timeline(&root)
        .unwrap()
        .iter()
        .filter(|e| e["kind"] == "seal")
        .map(|e| e["shard_id"].as_u64().unwrap())
        .collect();
    assert_eq!(decided.iter().collect::<BTreeSet<_>>().len(), decided.len());

    // Damaged sealed bytes halt recovery, unless sealed revisions are trusted.
    let victim = second.active.seals.last().unwrap().digest.clone();
    let segment = root
        .join(SEALED_DIR)
        .join(&victim)
        .join("directory-0.0.bin");
    let first_byte = std::fs::read(&segment).unwrap()[0];
    std::fs::OpenOptions::new()
        .write(true)
        .open(&segment)
        .unwrap()
        .write_all_at(&[first_byte ^ 1], 0)
        .unwrap();
    let run = run_script(&root, &journal, vec![], settings()).await;
    assert_eq!(run.outcome, Outcome::Idle);
    let run = run_script(&root, &journal, vec![], verifying()).await;
    assert!(
        matches!(run.outcome, Outcome::Halted(ref r) if r.contains(&victim)),
        "{:?}",
        run.outcome
    );
}

#[cfg(feature = "qualification-tests")]
#[tokio::test(flavor = "multi_thread")]
async fn the_window_drops_oldest_archives_and_keeps_the_rest() {
    let through = if cfg!(feature = "qualification-tests") {
        130
    } else {
        124
    };
    let temp = tempfile::tempdir().unwrap();
    let journal = temp.path().join("journal");
    fixture::write_journal(&journal, 100, through, 0);
    let layout = layout(&journal, 2);
    let whole = bootstrap_at(&journal, &temp.path().join("whole"), &layout, through);
    let root = temp.path().join("root");
    let started = bootstrap_at(&journal, &root, &layout, 112);
    let first = active_map(&root);
    assert_eq!(first.first_shard_id, started.len() as u64 - 2);
    let sealed_before = sealed_files(&root);
    let run = run_script(&root, &journal, advance(112, through, 6), settings()).await;
    assert_eq!(run.outcome, Outcome::Idle);
    check_maps(&run.maps, &first);
    assert!(run.maps.iter().all(|m| sealed_count(m) <= 2));
    let last = run.maps.last().unwrap();
    assert_eq!(last.first_shard_id, whole.len() as u64 - 2);
    assert_eq!(last.start_height, whole[whole.len() - 2].start_height);
    let kept: Vec<_> = last
        .shards
        .iter()
        .filter(|s| s.sealed)
        .map(|s| s.manifest_digest.clone())
        .collect();
    let expected: Vec<_> = whole[whole.len() - 2..]
        .iter()
        .map(|s| s.digest.clone())
        .collect();
    assert_eq!(kept, expected);
    // Every seal is still recorded; dropped archives stay on disk untouched.
    assert_eq!(identities(&run.active.seals), identities(&whole));
    let sealed_after = sealed_files(&root);
    for (path, identity) in &sealed_before {
        assert_eq!(sealed_after.get(path), Some(identity));
    }
    let drops = read_timeline(&root)
        .unwrap()
        .into_iter()
        .filter(|e| e["kind"] == "drop")
        .count();
    assert_eq!(drops, whole.len() - 2);
}

#[cfg(feature = "qualification-tests")]
#[tokio::test(flavor = "multi_thread")]
async fn a_restart_after_a_rollback_rewrites_what_it_replaced() {
    let temp = tempfile::tempdir().unwrap();
    let journal = temp.path().join("journal");
    fixture::write_journal(&journal, 100, 125, 0);
    let layout = layout(&journal, 100);
    let root = temp.path().join("root");
    bootstrap_at(&journal, &root, &layout, 118);

    // Tip 121 is published; the journal then rolls back to 119 and the next
    // journal write fails, before the controller hears of the rollback.
    let steps = [ScriptStep::Advance(121), ScriptStep::FailAfterRollback(119)];
    let mut controller = Controller::open(
        &root,
        open_writer(&journal).unwrap(),
        Source::Script(Script::new(steps)),
        settings(),
    )
    .unwrap();
    let error = tokio::time::timeout(
        Duration::from_secs(120),
        controller.run(crate::host_engine::PublisherEngine),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(error.to_string().contains("scripted journal failure"));
    assert_eq!(controller.active().tip, 121);
    let activated = controller.active().map_sha256.clone();
    drop(controller);
    assert!(reorgs_from(&root).is_empty());
    let record = saved_rollback(&root);
    assert_eq!(record["ancestor"], 119);
    assert_eq!(record["map_sha256"], json!(activated));

    // The restart rewrites exactly what the rollback replaced.
    let steps = vec![ScriptStep::Extend(fork(1, 120, 122))];
    let run = run_script(&root, &journal, steps, settings()).await;
    assert_eq!(run.outcome, Outcome::Idle);
    assert_eq!(reorgs_from(&root), vec![120]);
    assert_eq!(run.active.tip_hash, block_hash(1, 122).to_display_hex());
    assert!(
        saved_rollback(&root).is_null(),
        "a new publication clears it"
    );

    // A journal moved without a record (another writer, while the controller
    // was down) rewrites the whole recent range, and the tooling index is
    // rewritten over it.
    let recent_start = active_map(&root).shards.last().unwrap().start_height;
    {
        let mut store = open_writer(&journal).unwrap();
        store.rollback_to(Some(120)).unwrap();
        for (h, (hash, sources)) in (121..).zip(fork(2, 121, 122)) {
            let events =
                transparent_shared::journal::display_journal::implied_events(h, &sources).unwrap();
            store
                .append_block_with_display(h, hash, &events, &sources)
                .unwrap();
        }
        store.commit().unwrap();
    }
    let run = run_script(&root, &journal, vec![], settings()).await;
    assert_eq!(run.outcome, Outcome::Idle);
    assert_eq!(reorgs_from(&root), vec![120, recent_start]);
    assert_eq!(run.active.tip_hash, block_hash(2, 122).to_display_hex());
    let index: BTreeSet<_> = HeightIndex::open(&root)
        .unwrap()
        .read_all()
        .unwrap()
        .into_iter()
        .collect();
    let txids = |tag: u8, h: u64| -> Vec<([u8; 32], u64)> {
        chain_records(tag, h)
            .iter()
            .map(|r| (r.txid.0, h))
            .collect()
    };
    for (tag, h) in [(0, 120), (0, 121), (1, 121), (1, 122)] {
        assert!(
            txids(tag, h).iter().all(|t| !index.contains(t)),
            "{tag}@{h}"
        );
    }
    for (tag, h) in [(1, 120), (2, 121), (2, 122)] {
        assert!(txids(tag, h).iter().all(|t| index.contains(t)), "{tag}@{h}");
    }
}

#[cfg(feature = "qualification-tests")]
#[tokio::test(flavor = "multi_thread")]
async fn a_seal_a_rollback_undoes_waits_for_the_reorg_margin() {
    use transparent_shard::display::height_counts;
    let temp = tempfile::tempdir().unwrap();
    let journal = temp.path().join("journal");
    fixture::write_journal(&journal, 100, 130, 0);
    let layout = layout(&journal, 100);
    let root = temp.path().join("root");
    let started = bootstrap_at(&journal, &root, &layout, 112).len();
    let first = active_map(&root);
    // The first tip that decides more seals; the last of them is undone when
    // that tip's block is rolled back, which puts it inside the margin again.
    let params = fixture::params();
    let mut counts: Vec<_> = (101..=130)
        .map(|h| height_counts(&params, &chain_records(0, h)))
        .collect();
    let decided =
        |counts: &[_], tip: u64| plan_seals(&params, 101, &counts[..=(tip - 101) as usize]).len();
    let tip = (113..=130)
        .find(|t| decided(&counts, *t) > started)
        .unwrap();
    let before = decided(&counts, tip);
    counts.truncate((tip - 101) as usize);
    let after = decided(&counts, tip - 1);
    assert_eq!(after, before - 1);
    // The block that put the seal a margin below the tip is rolled back.
    let steps = vec![
        ScriptStep::Advance(tip),
        ScriptStep::Reorg {
            ancestor: tip - 1,
            blocks: vec![],
        },
    ];
    let mut controller = Controller::open(
        &root,
        open_writer(&journal).unwrap(),
        Source::Script(Script::new(steps)),
        settings(),
    )
    .unwrap();
    while controller.cache.tip().map(|t| t.0) < Some(tip) {
        controller.poll_source().await.unwrap();
    }
    assert_eq!(controller.queue.len(), before - started);
    // Every decided seal is built before the rollback arrives.
    while controller.queue.iter().any(|j| j.state != JobState::Built) {
        controller.schedule();
        let event = controller.receiver.recv().await.unwrap();
        controller.background(event);
    }
    controller.poll_source().await.unwrap();
    assert_eq!(controller.cache.tip().map(|t| t.0), Some(tip - 1));
    assert_eq!(
        controller.queue.len(),
        before - started,
        "the undone seal is kept"
    );
    assert_eq!(controller.publishable(), after - started, "and waits");
    assert_eq!(
        controller
            .run(crate::host_engine::PublisherEngine)
            .await
            .unwrap(),
        Outcome::Idle
    );
    assert_eq!(controller.active().seals.len(), after);
    let mut maps = controller.maps().to_vec();
    drop(controller);
    // The same seal publishes once the margin holds again.
    let steps = vec![ScriptStep::Extend(fork(3, tip, tip + 3))];
    let run = run_script(&root, &journal, steps, settings()).await;
    assert_eq!(run.outcome, Outcome::Idle);
    assert!(run.active.seals.len() >= before);
    maps.extend(run.maps);
    check_maps(&maps, &first);
    // Every published archive ends at least the margin below its map's tip,
    // and every recent shard fills the window.
    for map in &maps {
        let recent = map.shards.last().unwrap();
        assert!(map.unsealed_start() + params.reorg_margin <= recent.end_height + 1);
        assert!(
            recent.min_bucket_records >= params.recent_window,
            "{recent:?}"
        );
    }
}

#[cfg(feature = "qualification-tests")]
#[tokio::test(flavor = "multi_thread")]
async fn an_exceeded_recent_bound_alerts_once_and_seals_publish_once() {
    let temp = tempfile::tempdir().unwrap();
    let journal = temp.path().join("journal");
    fixture::write_journal(&journal, 100, 124, 0);
    let layout = layout(&journal, 100);
    let root = temp.path().join("root");
    let started = bootstrap_at(&journal, &root, &layout, 112).len();
    let settings = Settings {
        max_recent_records: Some(1),
        ..settings()
    };
    let run = run_script(&root, &journal, advance(112, 124, 4), settings).await;
    assert_eq!(run.outcome, Outcome::Idle);
    assert!(run.active.seals.len() > started);
    let events = read_timeline(&root).unwrap();
    let seals: Vec<_> = events
        .iter()
        .filter(|e| e["kind"] == "seal" && e["bootstrap"].is_null())
        .map(|e| e["shard_id"].as_u64().unwrap())
        .collect();
    assert_eq!(seals.iter().collect::<BTreeSet<_>>().len(), seals.len());
    // The unsealed range exceeded its bound: one alert, not one per loop.
    let lag = events
        .iter()
        .filter(|e| e["kind"] == "lag" && e["reason"] == "recent_records")
        .count();
    assert_eq!(lag, 1);
}

#[cfg(feature = "qualification-tests")]
#[tokio::test(flavor = "multi_thread")]
async fn replay_paces_coalesces_and_stops_at_its_end() {
    use crate::tx_detail::source::ReplaySource;
    let temp = tempfile::tempdir().unwrap();
    let journal = temp.path().join("journal");
    fixture::write_journal(&journal, 100, 130, 0);
    let layout = layout(&journal, 100);
    let root = temp.path().join("root");
    bootstrap_at(&journal, &root, &layout, 112);
    // A 1 ms step is always overrun by a cycle, so steps coalesce; the
    // replay end stops short of the journal end.
    let source = Source::Replay(ReplaySource::new(3, Duration::from_millis(1), 124), None);
    let mut controller =
        Controller::open(&root, open_writer(&journal).unwrap(), source, settings()).unwrap();
    let outcome = tokio::time::timeout(
        Duration::from_secs(120),
        controller.run(crate::host_engine::PublisherEngine),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(outcome, Outcome::Idle);
    assert_eq!(controller.map().covered_through(), Some(124));
    let events = read_timeline(&root).unwrap();
    let blocks: Vec<_> = events.iter().filter(|e| e["kind"] == "block").collect();
    let heights: Vec<_> = blocks
        .iter()
        .map(|e| e["height"].as_u64().unwrap())
        .collect();
    assert_eq!(heights, (113..=124).collect::<Vec<_>>());
    // Each block is observed at its own scheduled step, in order.
    let observed: Vec<_> = blocks
        .iter()
        .map(|e| e["observed_ms"].as_u64().unwrap())
        .collect();
    assert!(observed.windows(2).all(|w| w[0] <= w[1]));
    let lag: Vec<_> = events.iter().filter(|e| e["kind"] == "lag").collect();
    assert!(!lag.is_empty());
    assert!(lag.iter().all(|e| e["behind_steps"].as_u64().unwrap() >= 1));
}

#[test]
fn plan_start_leaves_room_for_bootstrap_and_replay_seals() {
    use transparent_shard::display::height_counts;
    let temp = tempfile::tempdir().unwrap();
    let journal = temp.path().join("journal");
    fixture::write_journal(&journal, 100, 220, 0);
    let store = EventStore::open_existing(&journal).unwrap();
    let params = fixture::params();
    let plan = plan_start(&store, &params, 4, 3, 6).unwrap();
    let start = plan["start"].as_u64().unwrap();
    let through = plan["through"].as_u64().unwrap();
    assert_eq!(plan["archives_at_through"], 4);
    let replayed = plan["replay_seals"].as_u64().unwrap();
    assert!(replayed >= 3);
    assert_eq!(plan["expected_drops"], replayed + 4 - 6);
    // Bootstrap at `through` makes exactly the archives asked for, and the
    // next block decides the next seal.
    let counts: Vec<_> = (start..=220)
        .map(|h| height_counts(&params, &chain_records(0, h)))
        .collect();
    let at = |tip: u64| plan_seals(&params, start, &counts[..=(tip - start) as usize]).len();
    assert_eq!(at(through), 4);
    assert!(at(through + 1) > 4);
    assert_eq!(at(220) as u64, 4 + replayed);
    assert!(plan_start(&store, &params, 50, 50, 6).is_err());
}

#[test]
fn verify_reproduces_a_running_root() {
    let through = if cfg!(feature = "qualification-tests") {
        125
    } else {
        118
    };
    let temp = tempfile::tempdir().unwrap();
    let journal = temp.path().join("journal");
    fixture::write_journal(&journal, 100, through, 0);
    let layout = layout(&journal, 3);
    let root = temp.path().join("root");
    let started = bootstrap_at(&journal, &root, &layout, 112).len();
    // Seals the journal makes past the recorded ones are queued, not wrong.
    let report = verify(&root, &journal, None).unwrap();
    assert_eq!(report["ok"], true, "{report}");
    assert!(report["queued"].as_u64().unwrap() > 0, "{report}");
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let run = runtime.block_on(run_script(
        &root,
        &journal,
        advance(112, through, 7),
        settings(),
    ));
    assert_eq!(run.outcome, Outcome::Idle);
    assert!(run.active.seals.len() > started);
    let report = verify(&root, &journal, None).unwrap();
    assert_eq!(report["ok"], true, "{report}");
    assert_eq!(report["reproduced_seals"], run.active.seals.len());
    assert_eq!(report["queued"], 0);
    assert!(!listing(&root).iter().any(|n| n.starts_with(".verify-")));

    // A rollback after the last seal leaves its end inside the reorg margin:
    // the journal no longer makes it, so it is rebuilt on its own.
    let last = run.active.seals.last().unwrap().clone();
    {
        let mut store = open_writer(&journal).unwrap();
        store.rollback_to(Some(last.end_height + 1)).unwrap();
        store.commit().unwrap();
    }
    let report = verify(&root, &journal, None).unwrap();
    assert_eq!(report["ok"], true, "{report}");
    assert_eq!(report["reproduced_seals"], run.active.seals.len() - 1);
    assert_eq!(report["unreproduced_verified"], 1);
    let tamper = |index: usize| {
        let mut active = ActiveRecord::load(&root).unwrap();
        let original = active.clone();
        active.seals[index].digest = "00".repeat(32);
        active.store(&root).unwrap();
        let report = verify(&root, &journal, None).unwrap();
        original.store(&root).unwrap();
        report
    };
    // A recorded seal that differs from the journal's is reported, whether
    // the journal reproduces it or it is rebuilt on its own.
    for index in [0, run.active.seals.len() - 1] {
        let report = tamper(index);
        assert_eq!(report["ok"], false, "{report}");
    }
}

#[cfg(feature = "qualification-tests")]
#[test]
fn bench_records_follow_the_requested_case_mix() {
    // Large enough that the 1% of outputs without an address is countable.
    let records = synthetic_records(20_000, 7);
    let count = |f: &dyn Fn(&DisplayEntry) -> bool| records.iter().filter(|r| f(&r.entry)).count();
    let complete = count(&|e| e.is_complete());
    let unshield = count(&|e| e.input_count == 0);
    let multiple = count(&|e| e.multiple_source_scripts);
    let more = count(&|e| e.more_than_two_outputs());
    let no_address = count(&|e| e.outputs[0].is_some_and(|o| !o.address.is_address()));
    assert!((17_800..=18_600).contains(&complete), "{complete} complete");
    assert!(
        (1_000..=1_400).contains(&unshield),
        "{unshield} unshielding"
    );
    assert!(
        (850..=1_150).contains(&multiple),
        "{multiple} multiple sources"
    );
    assert!((650..=950).contains(&more), "{more} more than two outputs");
    assert!(
        (130..=270).contains(&no_address),
        "{no_address} without an address"
    );
    // Shielded change is a fact, never mixed funding, and every record is
    // its txid's entry at the fixed width.
    assert_eq!(count(&|e| e.shielded_and_transparent_funding), 0);
    assert!(count(&|e| e.shielded_components) > 10_000);
    for record in &records {
        assert_eq!(record.entry.encode().unwrap().len(), ENTRY_BYTES);
        DisplayRecord::new(record.txid, record.entry).unwrap();
    }
    let temp = tempfile::tempdir().unwrap();
    let lines = bench_recent(&[500], "txid-2k8", temp.path(), 7).unwrap();
    assert_eq!(lines[0]["records"], 500);
    assert_eq!(lines[0]["directory_segments"], json!([1]));
    assert!(lines[0]["fill"].as_f64().unwrap() > 0.0);
}

#[cfg(feature = "qualification-tests")]
mod live {
    //! Live follow against a mock node, as `controller.rs`'s RPC fixture.
    use super::*;
    use crate::tx_detail::source::{LiveSource, ReplaySource};
    use axum::{extract::State, routing::post, Json, Router};

    type Chain = Arc<RwLock<Vec<Vec<u8>>>>;

    fn raw_block(parent: [u8; 32], height: u32, nonce: u8) -> Vec<u8> {
        // A syntactically valid header and a coinbase with no outputs; consensus
        // is the node's job, not the controller's. The chain parser rejects
        // blocks without transactions.
        let mut raw = 4u32.to_le_bytes().to_vec();
        raw.extend(parent);
        raw.extend([0; 64]);
        raw.extend(1_700_000_000u32.to_le_bytes());
        raw.extend(0x1f07ffffu32.to_le_bytes());
        raw.extend([nonce; 32]);
        raw.extend([0xfd, 0x40, 0x05]);
        raw.extend([0; 1344]);
        raw.push(1);
        // A height-zero coinbase must carry the real genesis script, and nothing
        // here reads coinbase heights, so genesis encodes height one.
        raw.extend(zakura_server_chain::testing::coinbase(height.max(1), &[]).bytes());
        raw
    }

    fn hash(raw: &[u8]) -> BlockHash {
        let block = zakura_server_chain::transparent::FetchedBlock::from_bytes(raw).unwrap();
        BlockHash::from_internal_bytes(block.hash())
    }

    fn extend(chain: &mut Vec<Vec<u8>>, count: usize, tag: u8) {
        for i in 0..count {
            let parent = chain
                .last()
                .map(|b| *hash(b).internal_bytes())
                .unwrap_or([0; 32]);
            chain.push(raw_block(parent, chain.len() as u32, tag + i as u8));
        }
    }

    async fn rpc(State(chain): State<Chain>, Json(request): Json<Value>) -> Json<Value> {
        let height = || {
            request["params"][0]
                .as_u64()
                .or_else(|| request["params"][0].as_str().and_then(|s| s.parse().ok()))
                .unwrap() as usize
        };
        let chain = chain.read().unwrap();
        let result = match request["method"].as_str().unwrap() {
            "getblockcount" => json!(chain.len() - 1),
            "getblockhash" => json!(hash(&chain[height()]).to_display_hex()),
            "getblock" => json!(hex::encode(&chain[height()])),
            method => panic!("unexpected RPC {method}"),
        };
        Json(json!({"id": request["id"], "result": result, "error": null}))
    }

    async fn published(status: &Arc<RwLock<Value>>, tip: u64, hash: &BlockHash) {
        let wanted = hash.to_display_hex();
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                {
                    let status = status.read().unwrap();
                    if status["published"]["tip"] == tip
                        && status["published"]["tip_hash"] == wanted
                    {
                        return;
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("tip {tip} was not published"));
    }

    #[cfg(feature = "qualification-tests")]
    #[tokio::test(flavor = "multi_thread")]
    async fn live_follow_publishes_rolls_back_and_halts_on_a_sealed_fork() {
        let temp = tempfile::tempdir().unwrap();
        let chain: Chain = Arc::new(RwLock::new(Vec::new()));
        extend(&mut chain.write().unwrap(), 6, 1);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new()
            .route("/", post(rpc))
            .with_state(chain.clone());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let cookie = temp.path().join("cookie");
        std::fs::write(&cookie, "fixture:fixture").unwrap();
        let client = crate::ingest::zakura::connect(format!("http://{address}"), &cookie).unwrap();

        // The journal starts at genesis; display starts one block later.
        let journal = temp.path().join("journal");
        {
            let blocks = chain.read().unwrap();
            let mut store =
                EventStore::open(&journal, &hash(&blocks[0]).to_display_hex(), 0).unwrap();
            for (h, raw) in blocks.iter().enumerate() {
                store
                    .append_block_with_display(h as u64, hash(raw), &[], &[])
                    .unwrap();
            }
            store.commit().unwrap();
        }
        let root = temp.path().join("root");
        let store = EventStore::open_existing(&journal).unwrap();
        let layout = fixture::layout(&store, 1, 4);
        // Bootstrap short of the journal end: replay shows the rest, then the
        // controller follows the node without operator action.
        bootstrap(&store, &root, &layout, 3).unwrap();
        drop(store);

        let settings = Settings {
            prebuild: super::no_prebuild,
            ..Settings::default()
        };
        let status = settings.status.clone();
        let live = LiveSource::new(client, Duration::from_millis(20));
        let replay = ReplaySource::new(1, Duration::from_millis(20), 5);
        let source = Source::Replay(replay, Some(live));
        let mut controller =
            Controller::open(&root, open_writer(&journal).unwrap(), source, settings).unwrap();

        let driver = tokio::spawn({
            let chain = chain.clone();
            async move {
                // Replay reaches the journal end first.
                let tip = hash(&chain.read().unwrap()[5]);
                published(&status, 5, &tip).await;
                // New blocks are ingested and published.
                extend(&mut chain.write().unwrap(), 3, 10);
                let tip = hash(&chain.read().unwrap()[8]);
                published(&status, 8, &tip).await;
                // A fork above the display start rolls back and republishes.
                {
                    let mut blocks = chain.write().unwrap();
                    blocks.truncate(7);
                    extend(&mut blocks, 3, 20);
                }
                let tip = hash(&chain.read().unwrap()[9]);
                published(&status, 9, &tip).await;
                // A fork replacing the display start's parent cannot be followed.
                {
                    let mut blocks = chain.write().unwrap();
                    blocks.clear();
                    extend(&mut blocks, 12, 40);
                }
            }
        });
        let outcome = tokio::time::timeout(
            Duration::from_secs(60),
            controller.run(crate::host_engine::PublisherEngine),
        )
        .await
        .expect("the controller halts")
        .unwrap();
        driver.await.unwrap();
        assert!(
            matches!(outcome, Outcome::Halted(ref r) if r.contains("sealed floor")),
            "{outcome:?}"
        );
        assert_eq!(reorgs_from(&root), vec![7]);
        // The halted controller left the journal on the last followed chain.
        assert_eq!(controller.store().covered_through(), Some(9));
        assert_eq!(controller.map().covered_through(), Some(9));
        assert_eq!(controller.active().tip, 9);
        server.abort();
    }
}

/// Shipped runtime files at the root of `candidate`, by name.
fn shipped_files(candidate: &Path) -> BTreeSet<String> {
    listing(candidate)
        .into_iter()
        .filter(|name| name.ends_with(".runtime") || name.ends_with(".partial"))
        .collect()
}

/// Every candidate carries one runtime per table segment of its recent
/// revision, as plain files at its root; the cycle
/// records the prebuild; revision directories still hold exactly their
/// manifests' files; and collection removes a candidate with its files.
#[cfg(feature = "qualification-tests")]
#[tokio::test(flavor = "multi_thread")]
async fn shipped_runtimes_go_out_with_each_candidate_and_go_with_it() {
    use transparent_shared::tx_detail::prebuild::build_shipped;
    use transparent_shared::tx_detail::set::DisplayRevision;
    let through = if cfg!(feature = "qualification-tests") {
        125
    } else {
        121
    };
    let temp = tempfile::tempdir().unwrap();
    let journal = temp.path().join("journal");
    fixture::write_journal(&journal, 100, through, 0);
    let layout = layout(&journal, 100);
    let root = temp.path().join("root");
    bootstrap_at(&journal, &root, &layout, 112);
    let settings = Settings {
        prebuild: build_shipped,
        retain_candidates: 1,
        ..settings()
    };
    let run = run_script(&root, &journal, advance(112, through, 3), settings).await;
    assert_eq!(run.outcome, Outcome::Idle);
    let cycles: Vec<Value> = read_timeline(&root)
        .unwrap()
        .into_iter()
        .filter(|e| e["kind"] == "cycle")
        .collect();
    let minimum_cycles = if cfg!(feature = "qualification-tests") {
        4
    } else {
        3
    };
    assert!(cycles.len() >= minimum_cycles, "{} cycles", cycles.len());
    let map = run.maps.last().unwrap();
    let recent = map.shards.last().unwrap();
    let candidate = &run.active.directory;
    let targets = DisplayRevision::read(&candidate.join(&recent.manifest_digest))
        .unwrap()
        .targets();
    let files = shipped_files(candidate);
    assert_eq!(files.len(), targets.len());
    assert!(files.iter().all(|name| name.ends_with(".runtime")));
    for cycle in &cycles {
        assert!(cycle["prebuild_ms"].is_u64(), "{cycle}");
        assert_eq!(cycle["prebuild_failures"], 0);
        let shipped = cycle["shipped"].as_array().unwrap();
        assert_eq!(shipped.len(), targets.len());
        assert_eq!(
            cycle["shipped_bytes"].as_u64().unwrap(),
            shipped
                .iter()
                .map(|f| f["bytes"].as_u64().unwrap())
                .sum::<u64>()
        );
    }
    for entry in &map.shards {
        verify_dir(
            &candidate.join(&entry.manifest_digest),
            &entry.manifest_digest,
        )
        .unwrap();
    }
    // Collection kept the active candidate and one other; nothing shipped
    // is left anywhere else under the root.
    let candidates: Vec<_> = listing(&root)
        .into_iter()
        .filter(|n| n.starts_with(CANDIDATE_PREFIX))
        .collect();
    assert!(candidates.len() <= 2, "{candidates:?}");
    assert!(shipped_files(&root).is_empty());
    for tier in ["recent", "sealed"] {
        for revision in listing(&root.join(tier)) {
            assert!(shipped_files(&root.join(tier).join(revision)).is_empty());
        }
    }
}

/// A prebuild failure is counted, leaves no file behind and never stops the
/// cycle: the candidate is published, and finalization builds the runtimes.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_prebuild_still_publishes() {
    fn fails(revision: &Path, candidate: &Path) -> Result<Vec<Shipped>, String> {
        assert!(revision.starts_with(candidate));
        std::fs::write(candidate.join("ab-cd.partial"), b"half").unwrap();
        std::fs::write(candidate.join("ab-cd.runtime"), b"half").unwrap();
        Err("no room to build".into())
    }
    let temp = tempfile::tempdir().unwrap();
    let journal = temp.path().join("journal");
    fixture::write_journal(&journal, 100, 118, 0);
    let layout = layout(&journal, 100);
    let root = temp.path().join("root");
    bootstrap_at(&journal, &root, &layout, 112);
    let settings = Settings {
        prebuild: fails,
        ..settings()
    };
    let run = run_script(&root, &journal, advance(112, 118, 3), settings).await;
    assert_eq!(run.outcome, Outcome::Idle);
    assert_eq!(run.active.tip, 118);
    let events = read_timeline(&root).unwrap();
    let cycles: Vec<_> = events.iter().filter(|e| e["kind"] == "cycle").collect();
    assert!(cycles.len() >= 2, "{} cycles", cycles.len());
    for (index, cycle) in cycles.iter().enumerate() {
        assert!(cycle["prebuild_ms"].is_null());
        assert_eq!(cycle["prebuild_failures"], index as u64 + 1);
    }
    assert_eq!(
        events
            .iter()
            .filter(|e| e["kind"] == "error" && e["stage"] == "prebuild")
            .count(),
        cycles.len()
    );
    assert!(shipped_files(&run.active.directory).is_empty());
}

/// The value of one series in a Prometheus text rendering.
#[cfg(feature = "qualification-tests")]
fn series(text: &str, name: &str) -> f64 {
    text.lines()
        .find_map(|line| line.strip_prefix(name)?.strip_prefix(' ')?.parse().ok())
        .unwrap_or_else(|| panic!("{name} is missing:\n{text}"))
}

/// Checks that one histogram's buckets are cumulative and end at its count,
/// and returns `(count, sum)`.
#[cfg(feature = "qualification-tests")]
fn histogram(text: &str, name: &str, labels: &str) -> (f64, f64) {
    let comma = if labels.is_empty() { "" } else { "," };
    let prefix = format!("{name}_bucket{{{labels}{comma}le=");
    let buckets: Vec<f64> = text
        .lines()
        .filter(|l| l.starts_with(&prefix))
        .map(|l| l.rsplit(' ').next().unwrap().parse().unwrap())
        .collect();
    assert!(buckets.len() > 1, "{name} {labels}:\n{text}");
    assert!(buckets.windows(2).all(|w| w[0] <= w[1]), "{buckets:?}");
    let braces = if labels.is_empty() {
        String::new()
    } else {
        format!("{{{labels}}}")
    };
    let count = series(text, &format!("{name}_count{braces}"));
    let sum = series(text, &format!("{name}_sum{braces}"));
    assert_eq!(*buckets.last().unwrap(), count, "{name} {labels}");
    assert!(sum >= 0.);
    (count, sum)
}

/// `/metrics` after scripted cycles: every family is present, each stage that
/// ran has one observation per cycle, and each published block one freshness
/// observation.
#[cfg(feature = "qualification-tests")]
#[tokio::test(flavor = "multi_thread")]
async fn metrics_report_cycles_stages_and_freshness() {
    let temp = tempfile::tempdir().unwrap();
    let journal = temp.path().join("journal");
    fixture::write_journal(&journal, 100, 118, 0);
    let layout = layout(&journal, 100);
    let root = temp.path().join("root");
    bootstrap_at(&journal, &root, &layout, 112);
    let settings = settings();
    let metrics = settings.metrics.clone();
    let run = run_script(&root, &journal, advance(112, 118, 3), settings).await;
    assert_eq!(run.outcome, Outcome::Idle);
    let cycles = read_timeline(&root)
        .unwrap()
        .into_iter()
        .filter(|e| e["kind"] == "cycle")
        .count() as f64;
    assert!(cycles >= 2., "{cycles} cycles");

    let text = metrics.render();
    for gauge in [
        "node_height",
        "published_height",
        "lag_blocks",
        "oldest_unpublished_seconds",
        "stalled",
        "halted",
        "consecutive_failures",
        "seal_queue",
        "last_success_timestamp_seconds",
        "last_seal_timestamp_seconds",
    ] {
        let declared = format!("# TYPE txid_display_{gauge} gauge\n");
        assert!(text.contains(&declared), "{gauge}");
    }
    assert_eq!(series(&text, "txid_display_node_height"), 118.);
    assert_eq!(series(&text, "txid_display_published_height"), 118.);
    assert_eq!(series(&text, "txid_display_lag_blocks"), 0.);
    assert_eq!(series(&text, "txid_display_oldest_unpublished_seconds"), 0.);
    assert_eq!(series(&text, "txid_display_stalled"), 0.);
    assert_eq!(series(&text, "txid_display_halted"), 0.);
    assert!(series(&text, "txid_display_process_start_time_seconds") > 0.);
    assert_eq!(series(&text, "txid_display_consecutive_failures"), 0.);
    assert!(series(&text, "txid_display_last_success_timestamp_seconds") > 1e9);
    // A cycle that only waited for a seal records no cycle event.
    assert!(series(&text, "txid_display_cycles_total") >= cycles);
    assert_eq!(series(&text, "txid_display_cycle_failures_total"), 0.);

    let (count, sum) = histogram(&text, "txid_display_cycle_seconds", "");
    assert_eq!(count, cycles);
    assert!(sum > 0.);
    let (count, _) = histogram(&text, "txid_display_freshness_seconds", "");
    assert_eq!(count, 6., "one observation per block 113..=118");
    assert!(text.contains("# TYPE txid_display_cycle_stage_seconds histogram\n"));
    for stage in crate::tx_detail::metrics::STAGES {
        let labels = format!("stage=\"{stage}\"");
        let (count, _) = histogram(&text, "txid_display_cycle_stage_seconds", &labels);
        assert_eq!(count, cycles, "{stage}");
    }
}

/// A stop requested before activation abandons the candidate as a normal
/// outcome: nothing activates and the active record is unchanged. A later
/// run without a stop prepares and activates from the same state.
#[tokio::test(flavor = "multi_thread")]
async fn a_stopped_candidate_is_not_activated_and_preparation_resumes() {
    let temp = tempfile::tempdir().unwrap();
    let journal = temp.path().join("journal");
    fixture::write_journal(&journal, 100, 118, 0);
    let layout = layout(&journal, 100);
    let root = temp.path().join("root");
    bootstrap_at(&journal, &root, &layout, 112);
    let mut controller = Controller::open(
        &root,
        open_writer(&journal).unwrap(),
        Source::Script(Script::new(advance(112, 118, 6))),
        settings(),
    )
    .unwrap();
    let before = controller.active().clone();
    let candidates = || {
        listing(&root)
            .into_iter()
            .filter(|name| name.starts_with("candidate"))
            .count()
    };
    let existing = candidates();
    controller.shutdown.request();
    // The journal holds blocks past the active tip, so a cycle is due.
    while controller.cache.tip().map(|t| t.0) <= Some(before.tip) {
        controller.poll_source().await.unwrap();
    }
    let prepared = crate::host_engine::PublisherEngine
        .prepare(preparation::DisplayPreparation::new(&mut controller))
        .await;
    assert!(
        matches!(prepared, Err(ref error) if error.is::<transparent_shared::publication::Stopped>()),
        "stopped before candidate installation"
    );
    assert_eq!(controller.active().tip, before.tip, "nothing activated");
    assert_eq!(ActiveRecord::load(&root).unwrap().tip, before.tip);
    assert_eq!(candidates(), existing, "no candidate was written");
    // A requested stop also ends the loop before another cycle starts.
    let shutdown = pir_pipeline::service::Shutdown::default();
    shutdown.request();
    assert_eq!(
        controller
            .run_until(crate::host_engine::PublisherEngine, shutdown)
            .await
            .unwrap(),
        Outcome::Idle
    );
    assert_eq!(ActiveRecord::load(&root).unwrap().tip, before.tip);
    drop(controller);

    let resumed = run_script(&root, &journal, advance(112, 118, 6), settings()).await;
    assert_eq!(resumed.outcome, Outcome::Idle);
    assert!(
        resumed.active.tip > before.tip,
        "preparation resumed and activated"
    );
}

/// A stop requested while a candidate's runtime prebuild runs is observed
/// only after that blocking build returns: until then the controller still
/// holds its root lock and journal, and `run_until` has not returned. The
/// unactivated candidate is then abandoned.
#[tokio::test(flavor = "multi_thread")]
async fn a_stop_keeps_the_root_owned_until_a_running_prebuild_returns() {
    static ENTERED: std::sync::LazyLock<std::sync::Barrier> =
        std::sync::LazyLock::new(|| std::sync::Barrier::new(2));
    static RELEASE: std::sync::LazyLock<std::sync::Barrier> =
        std::sync::LazyLock::new(|| std::sync::Barrier::new(2));
    static CALLS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    fn held(_revision: &Path, _candidate: &Path) -> Result<Vec<Shipped>, String> {
        if CALLS.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
            ENTERED.wait();
            RELEASE.wait();
        }
        Ok(Vec::new())
    }
    let temp = tempfile::tempdir().unwrap();
    let journal = temp.path().join("journal");
    fixture::write_journal(&journal, 100, 118, 0);
    let layout = layout(&journal, 100);
    let root = temp.path().join("root");
    bootstrap_at(&journal, &root, &layout, 112);
    let mut controller = Controller::open(
        &root,
        open_writer(&journal).unwrap(),
        Source::Script(Script::new(advance(112, 118, 6))),
        Settings {
            prebuild: held,
            ..settings()
        },
    )
    .unwrap();
    let before = controller.active().clone();
    let shutdown = pir_pipeline::service::Shutdown::default();
    let running = tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            let outcome = controller
                .run_until(crate::host_engine::PublisherEngine, shutdown)
                .await;
            (controller, outcome)
        }
    });
    tokio::task::spawn_blocking(|| ENTERED.wait())
        .await
        .unwrap();
    shutdown.request();
    let lock = std::fs::File::options()
        .read(true)
        .write(true)
        .open(root.join("controller.lock"))
        .unwrap();
    assert!(lock.try_lock().is_err(), "the root is still owned");
    assert!(open_writer(&journal).is_err(), "the journal is still owned");
    assert!(!running.is_finished(), "run_until waits for the prebuild");
    tokio::task::spawn_blocking(|| RELEASE.wait())
        .await
        .unwrap();
    let (controller, outcome) = running.await.unwrap();
    assert_eq!(outcome.unwrap(), Outcome::Idle);
    assert_eq!(CALLS.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(controller.active().tip, before.tip, "nothing activated");
    assert_eq!(ActiveRecord::load(&root).unwrap().tip, before.tip);
    drop(controller);
    lock.try_lock()
        .expect("dropping the controller releases the root");
    drop(lock);
    let resumed = run_script(&root, &journal, advance(112, 118, 6), settings()).await;
    assert!(resumed.active.tip > before.tip, "a later run activates");
}

/// A stop requested after a candidate is prepared abandons it: nothing
/// activates, and the active record and saved rollback record are
/// unchanged. A later run resumes.
#[cfg(feature = "qualification-tests")]
#[tokio::test(flavor = "multi_thread")]
async fn a_stop_before_activation_keeps_the_rollback_record() {
    let temp = tempfile::tempdir().unwrap();
    let journal = temp.path().join("journal");
    fixture::write_journal(&journal, 100, 125, 0);
    let layout = layout(&journal, 100);
    let root = temp.path().join("root");
    bootstrap_at(&journal, &root, &layout, 118);

    // Tip 121 is published; the journal then rolls back to 119 and the next
    // journal write fails, leaving a rollback record.
    let steps = [ScriptStep::Advance(121), ScriptStep::FailAfterRollback(119)];
    let mut controller = Controller::open(
        &root,
        open_writer(&journal).unwrap(),
        Source::Script(Script::new(steps)),
        settings(),
    )
    .unwrap();
    let error = tokio::time::timeout(
        Duration::from_secs(120),
        controller.run(crate::host_engine::PublisherEngine),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(error.to_string().contains("scripted journal failure"));
    drop(controller);
    let before_record = saved_rollback(&root);
    assert!(!before_record.is_null(), "a rollback is recorded");

    let mut controller = Controller::open(
        &root,
        open_writer(&journal).unwrap(),
        Source::Script(Script::new(vec![ScriptStep::Extend(fork(1, 120, 122))])),
        settings(),
    )
    .unwrap();
    let before = controller.active().clone();
    while controller.cache.tip().map(|t| t.0) < Some(122) {
        controller.poll_source().await.unwrap();
    }
    let Ok(Input::Work(candidate)) = controller.prepare_cycle().await else {
        panic!("a candidate is due");
    };
    controller.shutdown.request();
    assert!(
        controller.activate_cycle(candidate).await.is_ok(),
        "stopped before candidate installation"
    );
    assert_eq!(controller.active().map_sha256, before.map_sha256);
    let stored = ActiveRecord::load(&root).unwrap();
    assert_eq!(
        (stored.tip, stored.map_sha256),
        (before.tip, before.map_sha256.clone())
    );
    assert_eq!(
        saved_rollback(&root),
        before_record,
        "the rollback record is kept"
    );
    // The stop also ends the loop before another cycle starts.
    let shutdown = pir_pipeline::service::Shutdown::default();
    shutdown.request();
    assert_eq!(
        controller
            .run_until(crate::host_engine::PublisherEngine, shutdown)
            .await
            .unwrap(),
        Outcome::Idle
    );
    assert_eq!(ActiveRecord::load(&root).unwrap().tip, before.tip);
    drop(controller);

    let run = run_script(&root, &journal, vec![ScriptStep::Advance(122)], settings()).await;
    assert_eq!(run.outcome, Outcome::Idle);
    assert_eq!(run.active.tip, 122, "a later run activates");
    assert!(saved_rollback(&root).is_null());
}
