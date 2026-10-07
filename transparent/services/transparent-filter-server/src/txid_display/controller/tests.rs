use super::*;
use crate::txid_display::fixture::{self, block_hash, chain_records};
use crate::txid_display::publisher::{bootstrap, CANDIDATE_PREFIX, SEALED_DIR};
use crate::txid_display::serving::{Transport, WorkerRole, WorkersFile};
use crate::txid_display::source::{open_writer, Script, ScriptStep};
use crate::txid_display::timeline::read_timeline;
use std::collections::BTreeSet;
use std::os::unix::fs::{FileExt, MetadataExt};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use transparent_filter::BlockHash;
use transparent_shard::txid::TransparentDisplayRecord;

fn settings() -> Settings {
    Settings {
        exit_when_idle: true,
        trust_sealed: true,
        record_maps: true,
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
    workers: Vec<WorkerConfig>,
    settings: Settings,
) -> Run {
    let store = open_writer(journal).unwrap();
    let status = settings.status.clone();
    let mut controller = Controller::open(
        root,
        store,
        Source::Script(Script::new(steps)),
        workers,
        settings,
    )
    .unwrap();
    let outcome = tokio::time::timeout(Duration::from_secs(120), controller.run())
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

fn active_map(root: &Path) -> DisplayMap {
    let active = ActiveRecord::load(root).unwrap();
    read_map(&active.directory, &active.map_sha256).unwrap()
}

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

#[tokio::test(flavor = "multi_thread")]
async fn incremental_stepping_reproduces_bootstrap() {
    let temp = tempfile::tempdir().unwrap();
    let journal = temp.path().join("journal");
    fixture::write_journal(&journal, 100, 125, 0);
    let layout = layout(&journal, 100);
    let whole_root = temp.path().join("whole");
    let whole = bootstrap_at(&journal, &whole_root, &layout, 125);
    for step in [1, 7] {
        let root = temp.path().join(format!("step-{step}"));
        let started = bootstrap_at(&journal, &root, &layout, 112);
        assert!(
            started.len() >= 2 && whole.len() >= started.len() + 3,
            "{} then {} seals",
            started.len(),
            whole.len()
        );
        let first = active_map(&root);
        let run = run_script(&root, &journal, advance(112, 125, step), vec![], settings()).await;
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
        assert!(run.maps.len() >= (13 / step) as usize);
        check_maps(&run.maps, &first);
        assert_eq!(run.maps.last().unwrap().covered_through(), Some(125));
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

fn fork(tag: u8, from: u64, through: u64) -> Vec<(BlockHash, Vec<TransparentDisplayRecord>)> {
    (from..=through)
        .map(|h| (block_hash(tag, h), chain_records(tag, h)))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn reorgs_rebuild_recent_and_a_sealed_fork_halts() {
    let temp = tempfile::tempdir().unwrap();
    let journal = temp.path().join("journal");
    fixture::write_journal(&journal, 100, 125, 0);
    let layout = layout(&journal, 100);
    let root = temp.path().join("root");
    let sealed = bootstrap_at(&journal, &root, &layout, 118);
    let (log, _states) = spawn_fakes(temp.path(), &[("recent", "recent-replica")]);
    let workers = vec![socket_worker(
        temp.path(),
        "recent",
        WorkerRole::RecentReplica,
    )];

    // A fork two blocks below the tip, inside the margin.
    let steps = vec![
        ScriptStep::Advance(121),
        ScriptStep::Reorg {
            ancestor: 119,
            blocks: fork(1, 120, 122),
        },
    ];
    let run = run_script(&root, &journal, steps, workers.clone(), settings()).await;
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
    // Published coverage is withdrawn from the first replaced height.
    let invalidations: Vec<_> = log
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, c)| c["operation"] == "invalidate")
        .map(|(_, c)| c["from_height"].as_u64().unwrap())
        .collect();
    assert_eq!(invalidations, vec![120]);
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
    let run = run_script(&root, &journal, steps, workers.clone(), settings()).await;
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
    let run = run_script(&root, &journal, advance(122, 123, 1), workers, settings()).await;
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
    let temp = tempfile::tempdir().unwrap();
    let journal = temp.path().join("journal");
    fixture::write_journal(&journal, 100, 130, 0);
    let layout = layout(&journal, 100);
    let whole = bootstrap_at(&journal, &temp.path().join("whole"), &layout, 130);
    let root = temp.path().join("root");
    bootstrap_at(&journal, &root, &layout, 112);
    let first = run_script(&root, &journal, advance(112, 121, 9), vec![], verifying()).await;
    assert_eq!(first.outcome, Outcome::Idle);
    let sealed_before = sealed_files(&root);
    // Recovery verifies sealed bytes and continues from the activated tip.
    let second = run_script(&root, &journal, advance(121, 130, 9), vec![], verifying()).await;
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
    let run = run_script(&root, &journal, vec![], vec![], settings()).await;
    assert_eq!(run.outcome, Outcome::Idle);
    let run = run_script(&root, &journal, vec![], vec![], verifying()).await;
    assert!(
        matches!(run.outcome, Outcome::Halted(ref r) if r.contains(&victim)),
        "{:?}",
        run.outcome
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_window_drops_oldest_archives_and_keeps_the_rest() {
    let temp = tempfile::tempdir().unwrap();
    let journal = temp.path().join("journal");
    fixture::write_journal(&journal, 100, 130, 0);
    let layout = layout(&journal, 2);
    let whole = bootstrap_at(&journal, &temp.path().join("whole"), &layout, 130);
    let root = temp.path().join("root");
    let started = bootstrap_at(&journal, &root, &layout, 112);
    let first = active_map(&root);
    assert_eq!(first.first_shard_id, started.len() as u64 - 2);
    let sealed_before = sealed_files(&root);
    let run = run_script(&root, &journal, advance(112, 130, 6), vec![], settings()).await;
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

type Log = Arc<std::sync::Mutex<Vec<(String, Value)>>>;
type States = BTreeMap<String, Arc<std::sync::Mutex<Fake>>>;

/// A display worker that keeps only control state. After its first
/// activation it refuses a prepare naming an archive it was not staged with.
#[derive(Default)]
struct Fake {
    active: Option<(String, String, Vec<String>)>,
    prepared: Option<(String, String, Vec<String>)>,
    staged: BTreeSet<String>,
    /// Refuse this many invalidations.
    fail_invalidations: u32,
    /// Apply this activation (counting from 1) but reply that it failed.
    lose_activation: Option<usize>,
    activations: usize,
    /// Hold every stage reply this long.
    stage_delay: Duration,
    /// After this many stages, the next prepare finds the worker restarted:
    /// staging is lost and the prepare fails.
    restart_after_stages: Option<usize>,
    stages: usize,
}

fn fake_reply(state: &mut Fake, role: &str, command: &mut Value) -> Value {
    let expected = state
        .active
        .as_ref()
        .map_or("", |a| a.0.as_str())
        .to_string();
    let fail = |e: String| json!({"ok": false, "error": e});
    match command["operation"].as_str().unwrap_or_default() {
        "status" => json!({"ok": true, "role": role,
            "active": state.active.as_ref().map(|(sha, dir, _)| json!({"directory": dir, "map_sha256": sha})),
            "staged": state.staged, "warm": true, "binary_sha256": "fake"}),
        "stage" => {
            let dir = PathBuf::from(command["directory"].as_str().unwrap());
            let digest = dir.file_name().unwrap().to_string_lossy().into_owned();
            match verify_dir(&dir, &digest) {
                Ok(manifest) if manifest.sealed => {
                    state.stages += 1;
                    state.staged.insert(digest.clone());
                    json!({"ok": true, "digest": digest, "built": 2, "seconds": 0.0})
                }
                other => fail(format!("bad staged revision: {:?}", other.err())),
            }
        }
        "prepare" => {
            if command["expected"] != expected {
                return fail("active predecessor changed".into());
            }
            let dir = command["publication"]["directory"]
                .as_str()
                .unwrap()
                .to_string();
            let sha = command["publication"]["map_sha256"]
                .as_str()
                .unwrap()
                .to_string();
            let map = match read_map(Path::new(&dir), &sha) {
                Ok(map) => map,
                Err(e) => return fail(e.to_string()),
            };
            let sealed: Vec<String> = map
                .shards
                .iter()
                .filter(|s| s.sealed)
                .map(|s| s.manifest_digest.clone())
                .collect();
            command["sealed"] = json!(sealed);
            if state
                .restart_after_stages
                .is_some_and(|n| state.stages >= n)
            {
                state.restart_after_stages = None;
                state.staged.clear();
                return fail("worker restarted".into());
            }
            if let (true, Some((_, _, served))) = (role == "archive-owner", &state.active) {
                if let Some(new) = sealed
                    .iter()
                    .find(|d| !served.contains(d) && !state.staged.contains(*d))
                {
                    return fail(format!("unstaged archive {new}"));
                }
            }
            state.prepared = Some((sha, dir, sealed));
            json!({"ok": true, "warm": true, "built": 1, "reused": 0, "seconds": 0.0})
        }
        "activate" => {
            if command["expected"] != expected {
                return fail("active predecessor changed".into());
            }
            match state.prepared.take() {
                Some(prepared) if command["map_sha256"] == prepared.0 => {
                    state.staged.retain(|d| !prepared.2.contains(d));
                    state.active = Some(prepared);
                    state.activations += 1;
                    if state.lose_activation == Some(state.activations) {
                        return fail("reply lost".into());
                    }
                    json!({"ok": true})
                }
                other => {
                    state.prepared = other;
                    fail("not prepared".into())
                }
            }
        }
        "invalidate" => {
            if command["expected"] != expected {
                return fail("active predecessor changed".into());
            }
            if state.fail_invalidations > 0 {
                state.fail_invalidations -= 1;
                return fail("invalidation refused".into());
            }
            json!({"ok": true})
        }
        "collect" => json!({"ok": true, "removed": []}),
        "discard" => {
            state.prepared = None;
            json!({"ok": true})
        }
        other => fail(format!("unknown operation {other}")),
    }
}

fn spawn_fakes(dir: &Path, workers: &[(&str, &'static str)]) -> (Log, States) {
    let log: Log = Default::default();
    let mut states = BTreeMap::new();
    for (name, role) in workers {
        let listener = tokio::net::UnixListener::bind(dir.join(format!("{name}.sock"))).unwrap();
        let state: Arc<std::sync::Mutex<Fake>> = Default::default();
        states.insert(name.to_string(), state.clone());
        let (log, name, role) = (log.clone(), name.to_string(), *role);
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let (read, mut write) = stream.into_split();
                let mut line = String::new();
                tokio::io::BufReader::new(read)
                    .read_line(&mut line)
                    .await
                    .unwrap();
                let mut command: Value = serde_json::from_str(&line).unwrap();
                let (reply, delay) = {
                    let mut state = state.lock().unwrap();
                    let reply = fake_reply(&mut state, role, &mut command);
                    let staged = command["operation"] == "stage";
                    (reply, state.stage_delay * u32::from(staged))
                };
                tokio::time::sleep(delay).await;
                log.lock().unwrap().push((name.clone(), command));
                write
                    .write_all(format!("{reply}\n").as_bytes())
                    .await
                    .unwrap();
            }
        });
    }
    (log, states)
}

fn socket_worker(dir: &Path, name: &str, role: WorkerRole) -> WorkerConfig {
    WorkerConfig {
        name: name.into(),
        role,
        transport: Transport::Socket {
            socket: dir.join(format!("{name}.sock")),
        },
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn archive_owners_stage_before_any_map_names_a_seal() {
    let temp = tempfile::tempdir().unwrap();
    let journal = temp.path().join("journal");
    fixture::write_journal(&journal, 100, 128, 0);
    let layout = layout(&journal, 100);
    let root = temp.path().join("root");
    let bootstrapped = bootstrap_at(&journal, &root, &layout, 112);
    let (log, states) = spawn_fakes(
        temp.path(),
        &[("archive", "archive-owner"), ("recent", "recent-replica")],
    );
    let workers = vec![
        socket_worker(temp.path(), "archive", WorkerRole::ArchiveOwner),
        socket_worker(temp.path(), "recent", WorkerRole::RecentReplica),
    ];
    let run = run_script(&root, &journal, advance(112, 128, 4), workers, settings()).await;
    assert_eq!(run.outcome, Outcome::Idle);
    let log = log.lock().unwrap().clone();
    let ops = |worker: &str, op: &str| -> Vec<(usize, Value)> {
        log.iter()
            .enumerate()
            .filter(|(_, (w, c))| w == worker && c["operation"] == op)
            .map(|(i, (_, c))| (i, c.clone()))
            .collect()
    };
    let new_seals = &run.active.seals[bootstrapped.len()..];
    assert!(new_seals.len() >= 2, "{} seals", new_seals.len());
    let archive_prepares = ops("archive", "prepare");
    let archive_activations = ops("archive", "activate");
    let recent_prepares = ops("recent", "prepare");
    let stages = ops("archive", "stage");
    for seal in new_seals {
        let named = |prepares: &[(usize, Value)]| {
            prepares
                .iter()
                .find(|(_, c)| {
                    c["sealed"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|d| d == &seal.digest)
                })
                .map(|(i, _)| *i)
                .unwrap()
        };
        let staged = stages
            .iter()
            .find(|(_, c)| c["directory"].as_str().unwrap().ends_with(&seal.digest))
            .map(|(i, _)| *i)
            .expect("every seal is staged");
        let archive = named(&archive_prepares);
        let activated = archive_activations
            .iter()
            .find(|(i, _)| *i > archive)
            .unwrap()
            .0;
        let recent = named(&recent_prepares);
        assert!(
            staged < archive && archive < activated && activated < recent,
            "{seal:?}"
        );
    }
    // Archive owners are prepared only when the sealed set changes: once for
    // bootstrap's archives, then once per publication of new seals.
    let sets: Vec<_> = archive_prepares
        .iter()
        .map(|(_, c)| c["sealed"].clone())
        .collect();
    assert!(sets.windows(2).all(|pair| pair[0] != pair[1]));
    let mut seal_cycles = 0;
    let mut previous = bootstrapped.len();
    for map in &run.maps {
        if sealed_count(map) != previous {
            seal_cycles += 1;
            previous = sealed_count(map);
        }
    }
    assert_eq!(archive_prepares.len(), seal_cycles + 1);
    // Recent replicas take every map, starting from bootstrap's.
    assert_eq!(recent_prepares.len(), run.maps.len() + 1);
    let recent = states["recent"].lock().unwrap();
    assert_eq!(recent.active.as_ref().unwrap().0, run.active.map_sha256);
    // A socket worker serves straight from the root: the directory it holds
    // survives collection while it is active.
    let archive = states["archive"].lock().unwrap();
    assert!(Path::new(&archive.active.as_ref().unwrap().1)
        .join("txid-shards.json")
        .exists());
    assert!(archive.staged.is_empty());
}

fn commands(log: &Log, worker: &str) -> Vec<Value> {
    log.lock()
        .unwrap()
        .iter()
        .filter(|(w, _)| w == worker)
        .map(|(_, c)| c.clone())
        .collect()
}

fn invalidated(log: &Log) -> Vec<u64> {
    commands(log, "recent")
        .iter()
        .filter(|c| c["operation"] == "invalidate")
        .map(|c| c["from_height"].as_u64().unwrap())
        .collect()
}

fn pending_invalidations(root: &Path) -> Value {
    serde_json::from_slice::<Value>(&std::fs::read(root.join(INVALIDATE_FILE)).unwrap()).unwrap()
        ["pending"]
        .clone()
}

#[tokio::test(flavor = "multi_thread")]
async fn invalidations_reach_every_replica_before_it_moves_on() {
    let temp = tempfile::tempdir().unwrap();
    let journal = temp.path().join("journal");
    fixture::write_journal(&journal, 100, 125, 0);
    let layout = layout(&journal, 100);
    let root = temp.path().join("root");
    bootstrap_at(&journal, &root, &layout, 118);
    let (log, states) = spawn_fakes(temp.path(), &[("recent", "recent-replica")]);
    {
        let mut recent = states["recent"].lock().unwrap();
        // Tip 119 takes effect on the replica, but the reply is lost, so the
        // controller never commits it; then the first invalidation fails.
        recent.lose_activation = Some(2);
        recent.fail_invalidations = 1;
    }
    let workers = vec![socket_worker(
        temp.path(),
        "recent",
        WorkerRole::RecentReplica,
    )];
    let steps = vec![
        ScriptStep::Advance(119),
        ScriptStep::Reorg {
            ancestor: 118,
            blocks: fork(1, 119, 121),
        },
    ];
    let run = run_script(&root, &journal, steps, workers, settings()).await;
    assert_eq!(run.outcome, Outcome::Idle);
    let ops = commands(&log, "recent");
    let lost = ops
        .iter()
        .filter(|c| c["operation"] == "activate")
        .nth(1)
        .unwrap()["map_sha256"]
        .clone();
    // Above the committed tip but below what the replica serves: withdrawn
    // anyway, naming the map the replica actually serves, and retried.
    let invalidations: Vec<usize> = (0..ops.len())
        .filter(|i| ops[*i]["operation"] == "invalidate")
        .collect();
    assert_eq!(invalidated(&log), vec![119, 119]);
    assert!(invalidations.iter().all(|i| ops[*i]["expected"] == lost));
    // Nothing newer reaches the replica until it acknowledges.
    assert!(ops[invalidations[0]..invalidations[1]]
        .iter()
        .all(|c| c["operation"] != "prepare" && c["operation"] != "activate"));
    assert_eq!(run.active.tip, 121);
    assert_eq!(
        states["recent"].lock().unwrap().active.as_ref().unwrap().0,
        run.active.map_sha256
    );
    assert_eq!(pending_invalidations(&root), json!({}));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_restart_after_a_rollback_still_invalidates() {
    let temp = tempfile::tempdir().unwrap();
    let journal = temp.path().join("journal");
    fixture::write_journal(&journal, 100, 125, 0);
    let layout = layout(&journal, 100);
    let root = temp.path().join("root");
    bootstrap_at(&journal, &root, &layout, 118);
    let (log, _states) = spawn_fakes(temp.path(), &[("recent", "recent-replica")]);
    let workers = vec![socket_worker(
        temp.path(),
        "recent",
        WorkerRole::RecentReplica,
    )];

    // Tip 121 is published; the journal then rolls back to 119 and the next
    // journal write fails, before the controller hears of the rollback.
    let steps = [ScriptStep::Advance(121), ScriptStep::FailAfterRollback(119)];
    let mut controller = Controller::open(
        &root,
        open_writer(&journal).unwrap(),
        Source::Script(Script::new(steps)),
        workers.clone(),
        settings(),
    )
    .unwrap();
    let error = tokio::time::timeout(Duration::from_secs(120), controller.run())
        .await
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("scripted journal failure"));
    assert_eq!(controller.active().tip, 121);
    drop(controller);
    assert!(invalidated(&log).is_empty());
    assert_eq!(pending_invalidations(&root), json!({"recent": 120}));

    // The restart withdraws exactly what the rollback replaced.
    let steps = vec![ScriptStep::Extend(fork(1, 120, 122))];
    let run = run_script(&root, &journal, steps, workers.clone(), settings()).await;
    assert_eq!(run.outcome, Outcome::Idle);
    assert_eq!(invalidated(&log), vec![120]);
    assert_eq!(run.active.tip_hash, block_hash(1, 122).to_display_hex());
    assert_eq!(pending_invalidations(&root), json!({}));

    // A journal moved without a record (another writer, while the controller
    // was down) withdraws the whole recent range, and the tooling index is
    // rewritten over it.
    let recent_start = active_map(&root).shards.last().unwrap().start_height;
    {
        let mut store = open_writer(&journal).unwrap();
        store.rollback_to(Some(120)).unwrap();
        for (h, (hash, records)) in (121..).zip(fork(2, 121, 122)) {
            store
                .append_block_with_display(h, hash, &[], &records)
                .unwrap();
        }
        store.commit().unwrap();
    }
    let run = run_script(&root, &journal, vec![], workers, settings()).await;
    assert_eq!(run.outcome, Outcome::Idle);
    assert_eq!(invalidated(&log), vec![120, recent_start]);
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

#[tokio::test(flavor = "multi_thread")]
async fn a_seal_a_rollback_undoes_waits_for_the_recent_floor() {
    use transparent_shard::display::height_counts;
    let temp = tempfile::tempdir().unwrap();
    let journal = temp.path().join("journal");
    fixture::write_journal(&journal, 100, 130, 0);
    let layout = layout(&journal, 100);
    let root = temp.path().join("root");
    let started = bootstrap_at(&journal, &root, &layout, 112).len();
    let first = active_map(&root);
    // The first tip that decides more seals; the last of them is undone when
    // that tip's block is replaced by an empty one.
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
    counts.push(height_counts(&params, &[]));
    let after = decided(&counts, tip);
    assert_eq!(after, before - 1);
    let (log, states) = spawn_fakes(
        temp.path(),
        &[("archive", "archive-owner"), ("recent", "recent-replica")],
    );
    // Staging outlasts the rollback that follows the decision.
    states["archive"].lock().unwrap().stage_delay = Duration::from_millis(500);
    let workers = vec![
        socket_worker(temp.path(), "archive", WorkerRole::ArchiveOwner),
        socket_worker(temp.path(), "recent", WorkerRole::RecentReplica),
    ];
    // The block that met the recent floor is replaced by an empty one.
    let steps = vec![
        ScriptStep::Advance(tip),
        ScriptStep::Reorg {
            ancestor: tip - 1,
            blocks: vec![(block_hash(9, tip), vec![])],
        },
    ];
    let run = run_script(&root, &journal, steps, workers.clone(), settings()).await;
    assert_eq!(run.outcome, Outcome::Idle);
    assert_eq!(run.active.seals.len(), after);
    let staged = commands(&log, "archive")
        .iter()
        .filter(|c| c["operation"] == "stage")
        .count();
    assert_eq!(staged, before - started);
    let mut maps = run.maps;
    states["archive"].lock().unwrap().stage_delay = Duration::ZERO;
    // The same seal publishes once the floor holds again.
    let steps = vec![ScriptStep::Extend(fork(3, tip + 1, tip + 4))];
    let run = run_script(&root, &journal, steps, workers, settings()).await;
    assert_eq!(run.outcome, Outcome::Idle);
    assert!(run.active.seals.len() >= before);
    maps.extend(run.maps);
    check_maps(&maps, &first);
    let mut sealed = sealed_count(&first);
    for map in &maps {
        if sealed_count(map) > sealed {
            let recent = map.shards.last().unwrap();
            assert!(recent.records >= params.recent_floor, "{recent:?}");
        }
        sealed = sealed_count(map);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_archive_owner_that_lost_staging_is_staged_again() {
    let temp = tempfile::tempdir().unwrap();
    let journal = temp.path().join("journal");
    fixture::write_journal(&journal, 100, 124, 0);
    let layout = layout(&journal, 100);
    let root = temp.path().join("root");
    let started = bootstrap_at(&journal, &root, &layout, 112).len();
    let (log, states) = spawn_fakes(
        temp.path(),
        &[("archive", "archive-owner"), ("recent", "recent-replica")],
    );
    states["archive"].lock().unwrap().restart_after_stages = Some(1);
    let workers = vec![
        socket_worker(temp.path(), "archive", WorkerRole::ArchiveOwner),
        socket_worker(temp.path(), "recent", WorkerRole::RecentReplica),
    ];
    let settings = Settings {
        max_recent_records: Some(1),
        ..settings()
    };
    let run = run_script(&root, &journal, advance(112, 124, 4), workers, settings).await;
    assert_eq!(run.outcome, Outcome::Idle);
    assert!(run.active.seals.len() > started);
    let digest = &run.active.seals[started].digest;
    let log = log.lock().unwrap().clone();
    let position = |pred: &dyn Fn(&(String, Value)) -> bool| -> Vec<usize> {
        (0..log.len()).filter(|i| pred(&log[*i])).collect()
    };
    let stages = position(&|(w, c)| {
        w == "archive"
            && c["operation"] == "stage"
            && c["directory"].as_str().unwrap().ends_with(digest.as_str())
    });
    assert_eq!(stages.len(), 2, "staged again after the restart");
    // The archive owner refused the seal once, then took it once staged
    // again; without that, every cycle would repeat the refusal.
    let naming = position(&|(w, c)| {
        w == "archive" && c["operation"] == "prepare" && c["sealed"].to_string().contains(digest)
    });
    assert!(naming[0] < stages[1] && stages[1] < naming[1]);
    assert_eq!(
        states["recent"].lock().unwrap().active.as_ref().unwrap().0,
        run.active.map_sha256
    );
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

#[tokio::test(flavor = "multi_thread")]
async fn command_adapters_ship_then_relay_control() {
    use std::os::unix::fs::PermissionsExt;
    let temp = tempfile::tempdir().unwrap();
    let journal = temp.path().join("journal");
    fixture::write_journal(&journal, 100, 124, 0);
    let layout = layout(&journal, 100);
    let root = temp.path().join("root");
    bootstrap_at(&journal, &root, &layout, 112);
    let (_log, states) = spawn_fakes(
        temp.path(),
        &[("archive", "archive-owner"), ("recent", "recent-replica")],
    );
    let adapter = temp.path().join("adapter.py");
    std::fs::write(
        &adapter,
        r#"#!/usr/bin/env python3
import json, socket, sys
config = json.load(open(sys.argv[1]))
request = json.load(sys.stdin)
with open(config["log"], "a") as log:
    log.write(json.dumps(request) + "\n")
if request["operation"] == "ship":
    print(json.dumps({"ok": True, "directory": request["source"], "seconds": 0.0}))
else:
    s = socket.socket(socket.AF_UNIX)
    s.connect(config["sockets"][request["worker"]])
    s.sendall((json.dumps(request["command"]) + "\n").encode())
    print(json.dumps({"ok": True, "reply": json.loads(s.makefile().readline()), "seconds": 0.0}))
"#,
    )
    .unwrap();
    std::fs::set_permissions(&adapter, std::fs::Permissions::from_mode(0o700)).unwrap();
    let adapter_log = temp.path().join("adapter.log");
    let config = temp.path().join("fleet.json");
    let sockets = json!({"archive": temp.path().join("archive.sock"),
        "recent": temp.path().join("recent.sock")});
    std::fs::write(
        &config,
        serde_json::to_vec(&json!({"log": adapter_log, "sockets": sockets})).unwrap(),
    )
    .unwrap();
    let workers: WorkersFile = serde_json::from_value(json!({"workers": [
        {"name": "archive", "role": "archive-owner",
            "transport": {"command": adapter, "config": config}},
        {"name": "recent", "role": "recent-replica",
            "transport": {"command": adapter, "config": config}},
    ]}))
    .unwrap();
    let run = run_script(
        &root,
        &journal,
        advance(112, 124, 4),
        workers.workers,
        settings(),
    )
    .await;
    assert_eq!(run.outcome, Outcome::Idle);
    let requests: Vec<Value> = std::fs::read_to_string(&adapter_log)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    let ships: Vec<_> = requests
        .iter()
        .filter(|r| r["operation"] == "ship")
        .collect();
    let staged: Vec<_> = ships.iter().filter(|r| r["kind"] == "staged").collect();
    assert!(!staged.is_empty());
    for ship in staged {
        assert!(ship["link_dest"].is_null());
        assert!(ship["source"]
            .as_str()
            .unwrap()
            .ends_with(ship["name"].as_str().unwrap()));
    }
    let candidates: Vec<_> = ships
        .iter()
        .filter(|r| r["kind"] == "candidate" && r["worker"] == "recent")
        .collect();
    assert!(candidates.len() >= 3);
    // Each candidate after the first links against the worker's previous one.
    assert!(candidates[0]["link_dest"].is_null());
    for pair in candidates.windows(2) {
        assert_eq!(pair[1]["link_dest"], pair[0]["source"]);
    }
    assert_eq!(
        candidates.last().unwrap()["name"],
        json!(run.active.map_sha256)
    );
    assert_eq!(
        states["recent"].lock().unwrap().active.as_ref().unwrap().0,
        run.active.map_sha256
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn replay_paces_coalesces_and_stops_at_its_end() {
    use crate::txid_display::source::ReplaySource;
    let temp = tempfile::tempdir().unwrap();
    let journal = temp.path().join("journal");
    fixture::write_journal(&journal, 100, 130, 0);
    let layout = layout(&journal, 100);
    let root = temp.path().join("root");
    bootstrap_at(&journal, &root, &layout, 112);
    // A 1 ms step is always overrun by a cycle, so steps coalesce; the
    // replay end stops short of the journal end.
    let source = Source::Replay(ReplaySource::new(3, Duration::from_millis(1), 124), None);
    let mut controller = Controller::open(
        &root,
        open_writer(&journal).unwrap(),
        source,
        vec![],
        settings(),
    )
    .unwrap();
    let outcome = tokio::time::timeout(Duration::from_secs(120), controller.run())
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
    let temp = tempfile::tempdir().unwrap();
    let journal = temp.path().join("journal");
    fixture::write_journal(&journal, 100, 125, 0);
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
        advance(112, 125, 7),
        vec![],
        settings(),
    ));
    assert_eq!(run.outcome, Outcome::Idle);
    assert!(run.active.seals.len() > started);
    let report = verify(&root, &journal, None).unwrap();
    assert_eq!(report["ok"], true, "{report}");
    assert_eq!(report["reproduced_seals"], run.active.seals.len());
    assert_eq!(report["queued"], 0);
    assert!(!listing(&root).iter().any(|n| n.starts_with(".verify-")));

    // A reorg after the last seal replaces the blocks that met its recent
    // floor: the journal no longer makes it, so it is rebuilt on its own.
    let last = run.active.seals.last().unwrap().clone();
    {
        let mut store = open_writer(&journal).unwrap();
        store.rollback_to(Some(last.end_height + 1)).unwrap();
        store
            .append_block_with_display(last.end_height + 2, block_hash(9, 0), &[], &[])
            .unwrap();
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

#[test]
fn bench_records_follow_the_requested_size_mix() {
    // Large enough that the 0.1% multi-page tail is countable.
    let records = synthetic_records(20_000, 7);
    let sizes: Vec<usize> = records.iter().map(|r| r.encode().unwrap().len()).collect();
    let inline = sizes.iter().filter(|s| (50..=110).contains(*s)).count();
    let one_page = sizes.iter().filter(|s| (129..=4_050).contains(*s)).count();
    let near_cutoff = sizes.iter().filter(|s| (129..=528).contains(*s)).count();
    let multi_page = sizes.iter().filter(|s| **s > 4_050).count();
    assert!((17_600..=18_400).contains(&inline), "{inline} inline");
    assert!((1_750..=2_250).contains(&one_page), "{one_page} one page");
    assert!(
        near_cutoff * 4 >= one_page * 3,
        "{near_cutoff} of {one_page} near the cutoff"
    );
    assert!((5..=50).contains(&multi_page), "{multi_page} multi-page");
    let temp = tempfile::tempdir().unwrap();
    let lines = bench_recent(&[500], "txid-2k", temp.path(), 7).unwrap();
    assert_eq!(lines[0]["records"], 500);
    assert_eq!(lines[0]["directory_segments"], json!([1]));
    assert!(lines[0]["fill"].as_f64().unwrap() > 0.0);
}

mod live {
    //! Live follow against a mock node, as `controller.rs`'s RPC fixture.
    use super::*;
    use crate::txid_display::source::{LiveSource, ReplaySource};
    use crate::zakura::ZakuraClient;
    use axum::{extract::State, routing::post, Json, Router};
    use zakura_chain::serialization::ZcashDeserialize;

    type Chain = Arc<RwLock<Vec<Vec<u8>>>>;

    fn raw_block(parent: [u8; 32], nonce: u8) -> Vec<u8> {
        // A syntactically valid header and no transactions; consensus is the
        // node's job, not the controller's.
        let mut raw = 4u32.to_le_bytes().to_vec();
        raw.extend(parent);
        raw.extend([0; 64]);
        raw.extend(1_700_000_000u32.to_le_bytes());
        raw.extend(0x1f07ffffu32.to_le_bytes());
        raw.extend([nonce; 32]);
        raw.extend([0xfd, 0x40, 0x05]);
        raw.extend([0; 1344]);
        raw.push(0);
        raw
    }

    fn hash(raw: &[u8]) -> BlockHash {
        let block = zakura_chain::block::Block::zcash_deserialize(raw).unwrap();
        BlockHash::from_display_hex(&block.hash().to_string()).unwrap()
    }

    fn extend(chain: &mut Vec<Vec<u8>>, count: usize, tag: u8) {
        for i in 0..count {
            let parent = chain
                .last()
                .map(|b| *hash(b).internal_bytes())
                .unwrap_or([0; 32]);
            chain.push(raw_block(parent, tag + i as u8));
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
        let client = ZakuraClient::from_cookie_file(format!("http://{address}"), &cookie).unwrap();

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

        let (log, _states) = spawn_fakes(temp.path(), &[("recent", "recent-replica")]);
        let workers = vec![socket_worker(
            temp.path(),
            "recent",
            WorkerRole::RecentReplica,
        )];
        let settings = Settings::default();
        let status = settings.status.clone();
        let live = LiveSource::new(client, Duration::from_millis(20));
        let replay = ReplaySource::new(1, Duration::from_millis(20), 5);
        let source = Source::Replay(replay, Some(live));
        let mut controller = Controller::open(
            &root,
            open_writer(&journal).unwrap(),
            source,
            workers,
            settings,
        )
        .unwrap();

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
        let outcome = tokio::time::timeout(Duration::from_secs(60), controller.run())
            .await
            .expect("the controller halts")
            .unwrap();
        driver.await.unwrap();
        assert!(
            matches!(outcome, Outcome::Halted(ref r) if r.contains("sealed floor")),
            "{outcome:?}"
        );
        let invalidations: Vec<_> = log
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, c)| c["operation"] == "invalidate")
            .map(|(_, c)| c["from_height"].clone())
            .collect();
        assert_eq!(invalidations, vec![json!(7)]);
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

/// With `--ship-runtimes` every candidate carries one runtime per table
/// segment of its recent revision, as plain files at its root; the cycle
/// records the prebuild; revision directories still hold exactly their
/// manifests' files; and collection removes a candidate with its files.
#[tokio::test(flavor = "multi_thread")]
async fn shipped_runtimes_go_out_with_each_candidate_and_go_with_it() {
    use transparent_shard_server::display::prebuild::build_shipped;
    use transparent_shard_server::display::set::DisplayRevision;
    let temp = tempfile::tempdir().unwrap();
    let journal = temp.path().join("journal");
    fixture::write_journal(&journal, 100, 125, 0);
    let layout = layout(&journal, 100);
    let root = temp.path().join("root");
    bootstrap_at(&journal, &root, &layout, 112);
    let settings = Settings {
        ship_runtimes: Some(build_shipped),
        retain_candidates: 1,
        ..settings()
    };
    let run = run_script(&root, &journal, advance(112, 125, 3), vec![], settings).await;
    assert_eq!(run.outcome, Outcome::Idle);
    let cycles: Vec<Value> = read_timeline(&root)
        .unwrap()
        .into_iter()
        .filter(|e| e["kind"] == "cycle")
        .collect();
    assert!(cycles.len() >= 4, "{} cycles", cycles.len());
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
/// cycle: the candidate is published without runtimes.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_prebuild_still_publishes_without_runtimes() {
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
        ship_runtimes: Some(fails),
        ..settings()
    };
    let run = run_script(&root, &journal, advance(112, 118, 3), vec![], settings).await;
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
