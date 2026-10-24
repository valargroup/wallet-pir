//! Open publisher state and hand product operations to the root executor.
use super::*;

pub async fn run_managed<E>(
    config: Config,
    rpc: NodeClient,
    feed: pir_pipeline::ingestion::ChainFeed,
    executor: E,
    http: pir_pipeline::service::Http<Router>,
    shutdown: pir_pipeline::service::Shutdown,
) -> Result<(), BoxError>
where
    E: pir_pipeline::publisher::PublisherExecutor + pir_pipeline::ingestion::IngestionExecutor,
{
    if config.publication_root.join("active.json").exists() {
        crate::storage::layout::require(&config.publication_root, "transparent_history")?;
    }
    std::fs::create_dir_all(&config.publication_root)?;
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(config.publication_root.join("controller.lock"))?;
    lock.try_lock()
        .map_err(|e| format!("another publication controller owns this root: {e}"))?;
    // Reserve a revision before exposing any state. A restarted controller
    // cannot replay a former current pointer, even if it reuses its map.
    let distribution_revision = advance_distribution_revision(&config.publication_root)?;
    let existing = EventStore::open_existing(&config.data_dir)?;
    if existing.start_height() != 0 || existing.genesis_hash() != rpc.block_hash_display(0).await? {
        return Err("journal identity does not match genesis RPC history".into());
    }
    std::fs::create_dir_all(config.snapshot_dir())?;
    // A followed journal stays its writer's: read-only, unlocked, untruncated.
    let store = if config.follow_journal {
        existing
    } else {
        let genesis = existing.genesis_hash().to_string();
        let start = existing.start_height();
        drop(existing);
        EventStore::open(&config.data_dir, &genesis, start)?
    };
    let store = Arc::new(tokio::sync::Mutex::new(store));
    let active_path = config.publication_root.join("active.json");
    let public = if active_path.exists() {
        let saved: ActivePublication = serde_json::from_slice(&std::fs::read(&active_path)?)?;
        let loaded = read_public(saved.directory)?;
        if loaded.publication.map_sha256 != saved.map_sha256 {
            return Err("active record disagrees with publication".into());
        }
        loaded
    } else {
        read_public(config.initial_publication.clone())?
    };
    if public.filters.map().genesis_hash != store.lock().await.genesis_hash() {
        return Err("publication genesis does not match the journal".into());
    }
    let started = std::time::Instant::now();
    let mut public = public;
    let mut restored = None;
    let mut failed = None;
    if active_path.exists() {
        match restore_public(&config, &public) {
            Ok(Restore::Restored(found)) => {
                public.distribution = Some(found.finalized);
                restored = Some(found.validation);
            }
            // Only an installation without completion evidence (a fresh one,
            // or one written before this record existed) is prepared in full.
            Ok(Restore::Absent) => tracing::warn!(
                "completed publication has no restore record; it is prepared again in full before serving"
            ),
            // Recorded corrupt bytes stay withdrawn in this process too; only
            // a candidate with verified replacement bytes serves again.
            Ok(Restore::Failed(failure)) => {
                tracing::error!(path = %failure.path, reason = %failure.reason,
                    "the saved publication lists bytes that failed integrity validation; it stays withdrawn");
                failed = Some(failure);
            }
            // Present but unusable evidence, unsafe or incomplete files, an
            // unreadable integrity record or unresolved intent refuse to
            // start rather than hiding a full scan after a deploy stop.
            Err(error) => {
                return Err(format!(
                    "completed publication {} cannot be restored: {error}; deleting its completion record does not clear recorded integrity failures or publication intent, so deploy verified replacement bytes",
                    public.publication.directory.display()
                )
                .into())
            }
        }
    }
    let restore_seconds = started.elapsed().as_secs_f64();
    let authority = Authority(Arc::new(Inner {
        cycle_duration: Default::default(),
        publication_freshness: Default::default(),
        telemetry: Default::default(),
        publication_attempts: AtomicU64::new(0),
        publication_failures: AtomicU64::new(0),
        last_publication: AtomicU64::new(0),
        active: RwLock::new(Arc::new(public)),
        withdrawn: AtomicBool::new(true),
        epoch: AtomicU64::new(0),
        distribution_revision: AtomicU64::new(distribution_revision),
        distribution_slots: Arc::new(tokio::sync::Semaphore::new(DISTRIBUTION_SLOTS)),
        canonicality: Some(rpc.clone()),
        invalidated_from: AtomicU64::new(u64::MAX),
        publication_gate: std::sync::Mutex::new(()),
        status: RwLock::new(serde_json::json!({"phase":"starting",
            "integrity_validation":{"state":"pending"},
            "correctness_validation":{"state":"pending"}})),
        observed: RwLock::new(BTreeMap::new()),
        prepared_set: std::sync::Mutex::new(None),
        shutdown: shutdown.clone(),
        checked_runtimes: Default::default(),
        runtime_preparations: AtomicU64::new(0),
        runtime_preparation_failures: AtomicU64::new(0),
        validation: Default::default(),
        integrity_failed: AtomicBool::new(failed.is_some()),
        foreground: AtomicU64::new(0),
    }));
    if let Some(failure) = failed {
        let mut status = authority.0.status.write().unwrap();
        status["phase"] = "integrity_failed".into();
        status["integrity_validation"] = serde_json::json!({"state":"failed",
            "publication":{"height":authority.0.active.read().unwrap().publication.height,
                "hash":authority.0.active.read().unwrap().publication.hash},
            "map_sha256":failure.map_sha256,"inventory_sha256":failure.inventory_sha256,
            "reason":format!("{}: {}", failure.path, failure.reason),
            "artifact":serde_json::to_value(&failure).unwrap_or_default()});
    }
    struct Fence(Authority);
    impl Drop for Fence {
        fn drop(&mut self) {
            self.0 .0.withdrawn.store(true, Ordering::SeqCst);
        }
    }
    let _fence = Fence(authority.clone());
    // Joined when this function returns, before the controller lock is released.
    let _validator = crate::history::validator::Validator::spawn(
        authority.0.validation.clone(),
        Arc::new(transparent_shared::runtime::memory::WorkMemory::default()),
        {
            let authority = authority.clone();
            let root = config.publication_root.clone();
            Box::new(move || {
                authority.0.distribution_slots.available_permits() < DISTRIBUTION_SLOTS
                    || authority.0.foreground.load(Ordering::Acquire) > 0
                    || crate::history::validator::held_by_test(&root)
            })
        },
        {
            let authority = authority.clone();
            let root = config.publication_root.clone();
            Box::new(move |identity, event| validation_event(&root, &authority, identity, event))
        },
    )?;
    let mut tasks = pir_pipeline::service::Tasks::default();
    let server = tokio::spawn(http.serve(config.listen, authority.router()));
    tasks.own(&server);
    let (notify, updates) = tokio::sync::watch::channel(0u64);
    let current = Arc::new(AtomicBool::new(false));
    let (start, end) = {
        let journal = store.lock().await;
        (journal.start_height(), chain_input::newest(&journal))
    };
    let writer = HistoryIngestion {
        config: config.clone(),
        rpc: rpc.clone(),
        store: store.clone(),
        start,
        end,
        authority: authority.clone(),
        notify,
        cache: OutputCache::new(crate::ingest::prevout::DEFAULT_CACHE_OUTPUTS),
        pending_invalidation: None,
        current: current.clone(),
        followed_tip: None,
    };
    let input_stop = shutdown.clone();
    let failed_authority = authority.clone();
    let scheduler = executor.clone();
    let ingest = tokio::spawn(async move {
        let result = scheduler.ingest(writer, feed, input_stop.clone()).await;
        if result.is_err() {
            let _gate = failed_authority.0.publication_gate.lock().unwrap();
            failed_authority.0.withdrawn.store(true, Ordering::Release);
            failed_authority.0.epoch.fetch_add(1, Ordering::AcqRel);
            input_stop.request();
        }
        result
    });
    tasks.own(&ingest);
    if let Some(validation) = restored {
        // Serve the restored publication as soon as authority allows; its
        // bodies are verified again in the background.
        match serve_restored(&config, &rpc, &store, &authority, validation).await {
            Ok(()) => {
                let mut status = authority.0.status.write().unwrap();
                status["restore"] = serde_json::json!({
                    "metadata_seconds": restore_seconds,
                    "serving_seconds": started.elapsed().as_secs_f64(),
                });
                tracing::info!(
                    metadata_seconds = restore_seconds,
                    serving_seconds = started.elapsed().as_secs_f64(),
                    "restored completed publication is serving"
                );
            }
            Err(error) => tracing::warn!(
                %error,
                "restored publication is not served; preparing it again in full"
            ),
        }
    }
    executor
        .run(
            HistoryPublication {
                config,
                rpc,
                store,
                authority,
                shutdown: shutdown.clone(),
                server,
                ingest,
                updates,
                current,
            },
            shutdown,
        )
        .await
}
