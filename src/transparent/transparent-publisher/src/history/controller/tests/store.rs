//! The object store through the real controller path: what successive
//! publications hash, what a restart after an interrupted cycle keeps, and
//! what candidate collection may remove.
use super::*;
use std::os::unix::fs::MetadataExt;
use transparent_shared::history::shardset::OBJECT_STORE;
use transparent_shared::publication::Kind;

impl Follow {
    /// Mines `blocks` more blocks and brings the journal to the new tip.
    async fn advance(&self, authority: &Authority, blocks: usize) {
        let tip = {
            let mut chain = self.chain.write().unwrap();
            let tag = 100 + chain.len() as u8;
            extend(&mut chain, blocks, tag);
            chain.len() as u64 - 1
        };
        let (notify, _) = tokio::sync::watch::channel(0);
        ingest_once(
            &self.config,
            &self.client,
            &self.store,
            authority,
            &mut OutputCache::new(100),
            &notify,
        )
        .await
        .unwrap();
        assert_eq!(self.store.lock().await.covered_through(), Some(tip));
    }

    /// One publication cycle of the real controller, then candidate
    /// collection as the service loop runs it. Returns the cycle record.
    async fn cycle(&self, authority: &Authority) -> serde_json::Value {
        publish_follow(&self.config, &self.client, &self.store, authority)
            .await
            .unwrap();
        collect_candidates(&self.config, authority).unwrap();
        authority.0.status.read().unwrap()["last_cycle"].clone()
    }

    /// A process started now, as startup leaves it before its first cycle:
    /// the saved active publication (or the initial one), served from its
    /// completion evidence and held for background validation when that
    /// restores, and no verification evidence in memory. Returns whether it
    /// restored.
    fn restart(&self) -> (Authority, bool) {
        let authority = self.authority(Default::default());
        if !self.config.publication_root.join("active.json").exists() {
            return (authority, false);
        }
        let restore = restore_public(&self.config, &authority.0.active.read().unwrap()).unwrap();
        let Restore::Restored(found) = restore else {
            return (authority, false);
        };
        let transparent_shared::publication::Restored {
            finalized,
            validation,
        } = *found;
        let publication = authority.0.active.read().unwrap().publication.clone();
        let mut public = read_public(publication.directory.clone(), Vec::new()).unwrap();
        public.distribution = Some(finalized.clone());
        *authority.0.active.write().unwrap() = Arc::new(public);
        // No validator runs here: the job stays pending, holding its input.
        authority
            .0
            .validation
            .submit(crate::history::validator::Job {
                identity: crate::history::validator::Identity {
                    directory: publication.directory,
                    map_sha256: publication.map_sha256,
                    inventory_sha256: finalized.sha256().to_string(),
                    height: publication.height,
                    hash: publication.hash,
                    epoch: 0,
                },
                inventory: finalized.inventory().clone(),
                validation,
            });
        (authority, true)
    }
}

/// The stored file a candidate entry links, and what identifies its inode.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Object {
    path: PathBuf,
    stamp: (u64, i64, i64),
    bytes: u64,
}

fn object(link: &Path) -> Object {
    assert!(
        std::fs::symlink_metadata(link)
            .unwrap()
            .file_type()
            .is_symlink(),
        "{} is a store link",
        link.display()
    );
    let path = std::fs::canonicalize(link).unwrap();
    assert_eq!(
        path.parent().unwrap().file_name().unwrap(),
        OBJECT_STORE,
        "{} links into the store",
        link.display()
    );
    let meta = std::fs::metadata(&path).unwrap();
    Object {
        path,
        stamp: (meta.ino(), meta.ctime(), meta.ctime_nsec()),
        bytes: meta.len(),
    }
}

/// Every table segment link of every revision of `directory`, which the
/// controller's load verifies, by revision and file, and every runtime link
/// beside them.
fn links(directory: &Path) -> (BTreeMap<String, Object>, BTreeMap<String, Object>) {
    let mut tables = BTreeMap::new();
    let mut runtimes = BTreeMap::new();
    for entry in std::fs::read_dir(directory).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().into_string().unwrap();
        if entry.file_type().unwrap().is_dir() {
            for file in std::fs::read_dir(entry.path()).unwrap() {
                let file = file.unwrap();
                if file.file_type().unwrap().is_symlink() {
                    let file_name = file.file_name().into_string().unwrap();
                    tables.insert(format!("{name}/{file_name}"), object(&file.path()));
                }
            }
        } else if name.ends_with(".runtime") {
            runtimes.insert(name, object(&entry.path()));
        }
    }
    (tables, runtimes)
}

/// Every link of `directory` resolves to a stored object holding exactly
/// the bytes its name, the SHA-256 of its bytes, promises.
fn assert_complete(directory: &Path) {
    let (tables, runtimes) = links(directory);
    assert!(!tables.is_empty() && !runtimes.is_empty());
    for object in tables.values().chain(runtimes.values()) {
        assert_eq!(
            transparent_shared::publication::digest(&std::fs::read(&object.path).unwrap()),
            object.path.file_name().unwrap().to_str().unwrap(),
            "{} holds its named bytes",
            object.path.display()
        );
    }
}

fn counter(cycle: &serde_json::Value, path: &[&str]) -> u64 {
    let mut value = &cycle["storage"];
    for key in path {
        value = &value[key];
    }
    value
        .as_u64()
        .unwrap_or_else(|| panic!("{path:?} in {cycle}"))
}

/// What one cycle left linked: every revision's tables by path, every
/// runtime beside them, and the runtimes its inventory lists.
struct Linked {
    tables: BTreeMap<String, Object>,
    runtimes: BTreeMap<String, Object>,
    listed: BTreeMap<String, Object>,
}

impl Linked {
    /// Whether `object` is one of these tables or runtimes, unchanged.
    fn holds(&self, object: &Object) -> bool {
        self.tables
            .values()
            .chain(self.runtimes.values())
            .any(|o| o == object)
    }
}

/// Successive real publications read each stored table and runtime only
/// while it is new. The counters are exact. A file's evidence vouches for
/// it only once it has settled, so the load verifies again a revision's
/// table first seen, or stored, in the last cycle, and takes over every
/// other one; the inventory hashes only tables stored in this cycle and
/// runtimes new to it or stored in the last one, and lists the rest from
/// that evidence; a runtime the last inventory listed is not loaded and
/// self-checked again; and storing hashes only objects new to the store.
#[tokio::test(flavor = "multi_thread")]
async fn successive_publications_hash_only_new_objects() {
    let follow = follow_fixture().await;
    let authority = follow.authority(Default::default());
    let mut history: Vec<Linked> = Vec::new();
    let mut store_before = BTreeMap::new();
    for cycle in 0..3usize {
        if cycle == 0 {
            follow.ingest(&authority).await;
        } else {
            // Let every object written so far settle before the next read.
            std::thread::sleep(std::time::Duration::from_millis(1100));
            follow.advance(&authority, 1).await;
        }
        let record = follow.cycle(&authority).await;
        let active = authority.0.active.read().unwrap().clone();
        assert_eq!(active.publication.height, 4 + cycle as u64);
        let directory = &active.publication.directory;
        assert_complete(directory);
        let (tables, runtimes) = links(directory);
        let inventory = active.distribution.as_ref().unwrap().inventory().clone();
        // Retained revisions keep their runtimes beside the candidate; the
        // inventory lists those of the map's current revisions.
        let listed: BTreeMap<_, _> = inventory
            .artifacts
            .iter()
            .filter(|a| matches!(a.kind, Kind::RecentRuntime | Kind::ArchiveRuntime))
            .map(|a| (a.path.clone(), runtimes[&a.path].clone()))
            .collect();
        assert!(!listed.is_empty());
        let now = Linked {
            tables,
            runtimes,
            listed,
        };
        let last = cycle.checked_sub(1).map(|k| &history[k]);
        let before_last = cycle.checked_sub(2).map(|k| &history[k]);

        // Storing hashed exactly the objects new to the store, once each.
        let store = config_store(&follow.config);
        let new = |objects: &BTreeMap<String, Object>| -> BTreeMap<String, u64> {
            objects
                .values()
                .filter(|o| !store_before.contains_key(&name_of(o)))
                .map(|o| (name_of(o), o.bytes))
                .collect()
        };
        let (new_tables, new_runtimes) = (new(&now.tables), new(&now.runtimes));
        assert_eq!(
            counter(&record, &["segments", "installed"]),
            new_tables.len() as u64,
            "cycle {cycle}: {record}"
        );
        assert_eq!(
            counter(&record, &["segments", "bytes_hashed"]),
            new_tables.values().sum::<u64>(),
            "cycle {cycle}: {record}"
        );
        assert_eq!(
            counter(&record, &["runtimes", "installed"]),
            new_runtimes.len() as u64,
            "cycle {cycle}: {record}"
        );

        // The load took over every table its revision held last cycle as
        // the same object, settled before that cycle read it.
        let reused_tables: u64 = now
            .tables
            .iter()
            .filter(|(path, o)| {
                last.is_some_and(|l| l.tables.get(*path) == Some(o))
                    && before_last.is_some_and(|b| b.holds(o))
            })
            .map(|(_, o)| o.bytes)
            .sum();
        let all_tables: u64 = now.tables.values().map(|o| o.bytes).sum();
        assert_eq!(
            counter(&record, &["verification", "bytes_reused"]),
            reused_tables,
            "cycle {cycle}: {record}"
        );
        assert_eq!(
            counter(&record, &["verification", "bytes_hashed"]),
            all_tables - reused_tables,
            "cycle {cycle}: {record}"
        );

        // A runtime the last inventory listed as the same bytes is not
        // loaded and self-checked again.
        let checked_again = now
            .listed
            .iter()
            .filter(|(name, o)| {
                last.and_then(|l| l.listed.get(*name)).map(name_of) != Some(name_of(o))
            })
            .count() as u64;
        assert_eq!(
            counter(&record, &["inventory", "runtimes_loaded"]),
            checked_again,
            "cycle {cycle}: {record}"
        );
        assert_eq!(
            counter(&record, &["inventory", "runtimes_reused"]),
            now.listed.len() as u64 - checked_again,
            "cycle {cycle}: {record}"
        );

        // The inventory listed from evidence every table already stored
        // before this cycle, and every runtime the last inventory listed as
        // the same object, settled before it was read; it hashed the rest
        // and the local metadata.
        let mut reused = 0;
        for artifact in &inventory.artifacts {
            let path = directory.join(&artifact.path);
            let link = std::fs::symlink_metadata(&path)
                .unwrap()
                .file_type()
                .is_symlink();
            assert_eq!(link, artifact.kind != Kind::Metadata, "{}", artifact.path);
            if !link {
                continue;
            }
            let o = object(&path);
            assert_eq!(o.bytes, artifact.bytes);
            let evidence = match artifact.kind {
                Kind::RecentRuntime | Kind::ArchiveRuntime => {
                    last.and_then(|l| l.listed.get(&artifact.path)) == Some(&o)
                        && before_last.is_some_and(|b| b.holds(&o))
                }
                _ => last.is_some_and(|l| l.holds(&o)),
            };
            if evidence {
                reused += artifact.bytes;
            }
        }
        let listed_bytes: u64 = inventory.artifacts.iter().map(|a| a.bytes).sum();
        assert_eq!(
            counter(&record, &["inventory", "bytes_reused"]),
            reused,
            "cycle {cycle}: {record}"
        );
        assert_eq!(
            counter(&record, &["inventory", "bytes_hashed"]),
            listed_bytes - reused,
            "cycle {cycle}: {record}"
        );
        if cycle == 2 {
            // By now unchanged tables are neither verified nor listed by
            // reading them again.
            assert!(reused_tables > 0 && reused > 0, "{record}");
        }
        history.push(now);
        store_before = store;
    }
    follow.server.abort();
}

fn name_of(object: &Object) -> String {
    object
        .path
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned()
}

/// The objects in the store beside `config`'s publications.
fn config_store(config: &Config) -> BTreeMap<String, u64> {
    std::fs::read_dir(config.publication_root.join(OBJECT_STORE))
        .unwrap()
        .map(|e| {
            let e = e.unwrap();
            (
                e.file_name().into_string().unwrap(),
                e.metadata().unwrap().len(),
            )
        })
        .collect()
}

/// The candidate directories under the publication root.
fn candidates(config: &Config) -> std::collections::BTreeSet<PathBuf> {
    std::fs::read_dir(&config.publication_root)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| {
            p.is_dir()
                && p.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with("candidate-")
        })
        .collect()
}

/// Where a test interrupts the publication of the next height, standing in
/// for a process that dies there.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Interrupt {
    /// While storing the candidate, just before its first new object is
    /// installed.
    BeforeInstall,
    /// Once the candidate is stored and its inventory prepared, before
    /// activation writes its completion evidence.
    BeforeCompletion,
    /// Once activation wrote the candidate's completion evidence and
    /// `activation.json`, before the active pointer moves.
    AfterActivationIntent,
}

/// A publication interrupted at `interrupt` leaves complete durable state: the
/// saved active publication still has every object it links, and so does an
/// activation's pending candidate. Collection after the restart removes none
/// of them. The restarted process publishes the same height again, finding
/// the interrupted cycle's objects already stored, and still reads every
/// table and runtime it lists: stored objects, links and saved records are
/// never taken as verified bytes.
async fn interrupted_publication_restarts_from_complete_state(interrupt: Interrupt) {
    let follow = follow_fixture().await;
    let config = &follow.config;
    let first = follow.authority(Default::default());
    follow.ingest(&first).await;
    follow.cycle(&first).await;
    let active = first.0.active.read().unwrap().publication.clone();
    assert_eq!(active.height, 4);
    // Settled: only the absence of this process's evidence makes the
    // restarted one read them again.
    std::thread::sleep(std::time::Duration::from_millis(1100));
    follow.advance(&first, 1).await;
    let before = candidates(config);
    match interrupt {
        Interrupt::BeforeInstall => {
            crate::history::store::INTERRUPT_INSTALL
                .lock()
                .unwrap()
                .insert(config.publication_root.clone());
            let error = publish_follow(config, &follow.client, &follow.store, &first)
                .await
                .unwrap_err()
                .to_string();
            assert!(error.contains("interrupted before installing"), "{error}");
        }
        Interrupt::BeforeCompletion => {
            let Input::Work(work) = acquire_history(config, &follow.client, &follow.store, &first)
                .await
                .unwrap()
            else {
                panic!("new durable work expected");
            };
            let Input::Work(ready) = prepare_follow(config, &follow.client, &first, work)
                .await
                .unwrap()
            else {
                panic!("a prepared candidate expected");
            };
            drop(ready);
        }
        Interrupt::AfterActivationIntent => {
            super::super::INTERRUPT_AFTER_INTENT
                .lock()
                .unwrap()
                .insert(config.publication_root.clone());
            let error = publish_follow(config, &follow.client, &follow.store, &first)
                .await
                .unwrap_err()
                .to_string();
            assert!(error.contains("interrupted after writing"), "{error}");
        }
    }
    drop(first);
    let interrupted: Vec<_> = candidates(config).difference(&before).cloned().collect();
    assert_eq!(interrupted.len(), 1, "{interrupted:?}");
    let interrupted = &interrupted[0];

    // The durable state a restart finds.
    let saved: ActivePublication = serde_json::from_slice(
        &std::fs::read(config.publication_root.join("active.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(saved.directory, active.directory);
    let intent = config.publication_root.join("activation.json");
    let completion = interrupted.join(transparent_shared::publication::RESTORE_FILE);
    if interrupt == Interrupt::AfterActivationIntent {
        let pending: ActivePublication =
            serde_json::from_slice(&std::fs::read(&intent).unwrap()).unwrap();
        assert_eq!(&pending.directory, interrupted);
        assert!(completion.exists());
    } else {
        assert!(!intent.exists() && !completion.exists());
    }
    let (restarted, restored) = follow.restart();
    // Unresolved activation intent is never restored cheaply.
    assert_eq!(restored, interrupt != Interrupt::AfterActivationIntent);
    let pending = intent.exists().then_some(interrupted);
    collect_candidates(config, &restarted).unwrap();
    for directory in std::iter::once(&active.directory).chain(pending) {
        assert_complete(directory);
    }

    let record = follow.cycle(&restarted).await;
    let now = restarted.0.active.read().unwrap().publication.clone();
    assert_eq!(now.height, 5);
    assert_ne!(&now.directory, interrupted);
    assert_complete(&now.directory);
    assert!(!intent.exists(), "the restart resolved the intent");
    // It found the interrupted cycle's work stored...
    let found = counter(&record, &["segments", "shared"])
        + counter(&record, &["segments", "linked"])
        + counter(&record, &["runtimes", "shared"])
        + counter(&record, &["runtimes", "linked"]);
    assert!(found > 0, "{record}");
    if interrupt == Interrupt::BeforeInstall {
        // ...but not the object it never installed.
        assert!(
            counter(&record, &["segments", "installed"])
                + counter(&record, &["runtimes", "installed"])
                > 0,
            "{record}"
        );
    }
    // ...and still read every byte it lists in this cycle: it verified
    // every table, hashed and self-checked every runtime, and listed from
    // evidence only tables its own verification had just hashed.
    assert_eq!(
        counter(&record, &["verification", "bytes_reused"]),
        0,
        "{record}"
    );
    assert_eq!(
        counter(&record, &["inventory", "runtimes_reused"]),
        0,
        "{record}"
    );
    let inventory = restarted
        .0
        .active
        .read()
        .unwrap()
        .distribution
        .clone()
        .unwrap();
    let runtime_bytes: u64 = inventory
        .inventory()
        .artifacts
        .iter()
        .filter(|a| matches!(a.kind, Kind::RecentRuntime | Kind::ArchiveRuntime))
        .map(|a| a.bytes)
        .sum();
    assert_eq!(
        counter(&record, &["inventory", "runtimes_loaded"]),
        inventory
            .inventory()
            .artifacts
            .iter()
            .filter(|a| matches!(a.kind, Kind::RecentRuntime | Kind::ArchiveRuntime))
            .count() as u64,
        "{record}"
    );
    assert!(
        counter(&record, &["inventory", "bytes_hashed"]) >= runtime_bytes,
        "{record}"
    );
    assert!(
        counter(&record, &["inventory", "bytes_reused"])
            <= counter(&record, &["verification", "bytes_hashed"]),
        "{record}"
    );
    follow.server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_publication_interrupted_before_an_object_install_restarts_complete() {
    interrupted_publication_restarts_from_complete_state(Interrupt::BeforeInstall).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_publication_interrupted_before_completion_restarts_complete() {
    interrupted_publication_restarts_from_complete_state(Interrupt::BeforeCompletion).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_publication_interrupted_after_its_activation_intent_restarts_complete() {
    interrupted_publication_restarts_from_complete_state(Interrupt::AfterActivationIntent).await;
}

/// Copies a candidate as links: symbolic links keep their targets and
/// files are hard-linked.
fn copy_links(source: &Path, target: &Path) {
    std::fs::create_dir(target).unwrap();
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let kind = entry.file_type().unwrap();
        let to = target.join(entry.file_name());
        if kind.is_symlink() {
            std::os::unix::fs::symlink(std::fs::read_link(entry.path()).unwrap(), to).unwrap();
        } else if kind.is_dir() {
            copy_links(&entry.path(), &to);
        } else {
            std::fs::hard_link(entry.path(), to).unwrap();
        }
    }
}

/// Candidate collection never removes an object that the active
/// publication, an interrupted activation's pending candidate or a
/// publication held for validation links, even when newer unused candidates
/// push all three past the few it retains. It removes exactly what no
/// remaining publication links, and a reader holding a removed object open
/// still reads all of its bytes.
#[tokio::test(flavor = "multi_thread")]
async fn collection_keeps_every_object_an_active_pending_or_validating_publication_links() {
    use transparent_shared::publication::RESTORE_FILE;
    let follow = follow_fixture().await;
    let config = &follow.config;
    let authority = follow.authority(Default::default());
    follow.ingest(&authority).await;
    follow.cycle(&authority).await;
    let validating = authority.0.active.read().unwrap().clone();
    follow.advance(&authority, 1).await;
    follow.cycle(&authority).await;
    let active = authority
        .0
        .active
        .read()
        .unwrap()
        .publication
        .directory
        .clone();
    // The replaced publication is still validated, as after a restart.
    let Restore::Restored(found) = restore_public(config, &validating).unwrap() else {
        panic!("a completed publication restores");
    };
    assert!(validating.publication.directory.join(RESTORE_FILE).exists());
    authority
        .0
        .validation
        .submit(crate::history::validator::Job {
            identity: crate::history::validator::Identity {
                directory: validating.publication.directory.clone(),
                map_sha256: validating.publication.map_sha256.clone(),
                inventory_sha256: found.finalized.sha256().to_string(),
                height: validating.publication.height,
                hash: validating.publication.hash.clone(),
                epoch: 0,
            },
            inventory: found.finalized.inventory().clone(),
            validation: found.validation,
        });
    // The next candidate, prepared and named as activation intent by a
    // process that then died.
    follow.advance(&authority, 1).await;
    let Input::Work(work) = acquire_history(config, &follow.client, &follow.store, &authority)
        .await
        .unwrap()
    else {
        panic!("new durable work expected");
    };
    let Input::Work(ready) = prepare_follow(config, &follow.client, &authority, work)
        .await
        .unwrap()
    else {
        panic!("a prepared candidate expected");
    };
    let pending = ready.candidate.publication.clone();
    publication::write_atomic(
        &config.publication_root.join("activation.json"),
        &serde_json::to_vec(&pending).unwrap(),
    )
    .unwrap();
    // Four newer unused candidates. The oldest of them alone links an
    // object, which a reader holds open.
    let objects = config.publication_root.join(OBJECT_STORE);
    let held = b"bytes a response is still sending";
    let digest = transparent_shared::publication::digest(held);
    std::fs::write(objects.join(&digest), held).unwrap();
    let newer: Vec<_> = (0..4)
        .map(|i| config.publication_root.join(format!("candidate-newer-{i}")))
        .collect();
    for (i, directory) in newer.iter().enumerate() {
        copy_links(&active, directory);
        if i == 0 {
            std::os::unix::fs::symlink(
                Path::new("..").join(OBJECT_STORE).join(&digest),
                directory.join(format!("{digest}-{digest}.runtime")),
            )
            .unwrap();
        }
        std::fs::File::open(directory)
            .unwrap()
            .set_modified(
                std::time::SystemTime::now() + std::time::Duration::from_secs(10 * (i as u64 + 1)),
            )
            .unwrap();
    }
    let mut reader = std::fs::File::open(objects.join(&digest)).unwrap();

    collect_candidates(config, &authority).unwrap();
    assert!(!newer[0].exists(), "the oldest unused candidate is removed");
    assert!(newer[1..].iter().all(|d| d.exists()));
    assert!(!objects.join(&digest).exists(), "its object is removed");
    let mut read = Vec::new();
    std::io::Read::read_to_end(&mut reader, &mut read).unwrap();
    assert_eq!(read, held, "an open object keeps its bytes");
    for directory in [
        &validating.publication.directory,
        &active,
        &pending.directory,
    ] {
        assert!(directory.exists());
        assert_complete(directory);
    }
    // Exactly the objects a remaining publication links are kept.
    let mut linked = std::collections::BTreeSet::new();
    for directory in candidates(config) {
        let (tables, runtimes) = links(&directory);
        linked.extend(tables.values().chain(runtimes.values()).map(name_of));
    }
    assert_eq!(
        config_store(config)
            .into_keys()
            .collect::<std::collections::BTreeSet<_>>(),
        linked
    );
    follow.server.abort();
}
