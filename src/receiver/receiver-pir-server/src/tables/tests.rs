use super::*;
use std::sync::mpsc;
use zakura_pir_query_server::receiver::{Coverage, Tree, SCHEMA};

/// A prepared minimum directory whose end hash is `variant`, shared with other tests.
fn directory(variant: u8) -> Server<Directory> {
    crate::prepared::directory(crate::prepared::empty(variant).directory)
        .unwrap()
        .0
}

/// A directory source over `server` whose every load runs `hook` first.
fn source(server: Server<Directory>, hook: impl Fn() + Send + Sync + 'static) -> Arc<Source> {
    let (session, public) = (server.session().clone(), server.public().to_vec());
    Arc::new(
        Source::directory(session, public, move || {
            hook();
            Ok(server.clone())
        })
        .unwrap(),
    )
}

/// A reservation for preparing a `kind` table of the directory's rows, waiting for none.
fn reserve(tables: &Tables, kind: Kind) -> Reservation {
    tables
        .reserve_peak(kind, DIRECTORY_ROWS, std::time::Duration::ZERO)
        .unwrap()
}

/// A companion source declaring `rows` rows, with no table behind it: its load runs
/// `hook` and fails. Only its charges are under test.
fn declared(rows: u32, hook: impl Fn() + Send + Sync + 'static) -> Arc<Source> {
    use sha2::Digest;
    let coverage = Coverage {
        genesis: [1; 32],
        history_revision: 0,
        start_height: 100,
        start_parent: [2; 32],
        start_position: 0,
        end_height: 100,
        end_hash: [3; 32],
        end_position: 0,
    };
    let manifest = Companion {
        schema: SCHEMA.into(),
        coverage,
        directory: [0; 32],
        rows,
        query_rows: receiver::QUERY_BLOCK_ROWS,
        payments: 0,
        tree: Tree {
            height: 100,
            hash: [3; 32],
            size: 0,
        },
        data_sha256: [0; 32],
    };
    let public = vec![0; receiver::native::public_bytes()];
    let session = Session {
        manifest,
        public_digest: sha2::Sha256::digest(&public).into(),
    };
    Arc::new(
        Source::companion(session, public, move || {
            hook();
            Err(Error::Malformed)
        })
        .unwrap(),
    )
}

/// The charges are profile-aware: a companion's preparation covers both row copies,
/// the scratch and the directory it reads, and the minimum budget is the largest
/// directory resident beside the largest companion's preparation.
#[test]
fn charges_cover_the_preparation_peak() {
    let (d, c) = (DIRECTORY_ROWS, MAX_COMPANION_ROWS);
    assert_eq!(
        peak_bytes(Kind::Companion, c),
        2 * (256 << 20) + (8 << 20) + (320 << 20)
    );
    assert_eq!(peak_bytes(Kind::Directory, d), 2 * (8 << 20) + (320 << 20));
    assert_eq!(resident_bytes(d), (8 << 20) + (72 << 20));
    assert_eq!(MIN_BUDGET, 920 << 20);
    assert!(peak_bytes(Kind::Companion, d) > peak_bytes(Kind::Directory, d));
}

/// At the minimum budget, a cold load of the largest companion is charged its whole
/// preparation peak beside the largest directory in use; any further table in use
/// leaves no room, which is `Busy` without running the load.
#[test]
fn the_minimum_budget_prepares_the_largest_companion_beside_a_directory() {
    let tables = Arc::new(Tables::new(MIN_BUDGET));
    let server = directory(1);
    let held = tables.insert(
        &Source::memory(Loaded::Directory(server)),
        Loaded::Directory(directory(1)),
        reserve(&tables, Kind::Directory),
    );
    let charged = Arc::new(AtomicUsize::new(0));
    let companion = declared(MAX_COMPANION_ROWS, {
        let (tables, charged) = (tables.clone(), charged.clone());
        move || charged.store(tables.resident(), Ordering::SeqCst)
    });
    assert!(matches!(
        tables.get(&companion),
        Err(LoadError::Load(Error::Malformed))
    ));
    assert_eq!(
        charged.load(Ordering::SeqCst),
        resident_bytes(DIRECTORY_ROWS) + peak_bytes(Kind::Companion, MAX_COMPANION_ROWS)
    );
    // The failed load released its charge; the directory keeps its own.
    assert_eq!(tables.resident(), resident_bytes(DIRECTORY_ROWS));
    let other = tables.insert(
        &source(directory(2), || ()),
        Loaded::Directory(directory(2)),
        reserve(&tables, Kind::Directory),
    );
    charged.store(0, Ordering::SeqCst);
    assert!(matches!(tables.get(&companion), Err(LoadError::Busy)));
    assert_eq!(charged.load(Ordering::SeqCst), 0);
    drop((held, other));
}

/// A publisher preparation and query cold loads share one budget: a reserved peak
/// makes a cold load that cannot fit beside it `Busy` until the preparation is cached,
/// and a reservation that tables in use leave no room for waits for them to finish.
#[test]
fn a_publisher_preparation_and_cold_loads_share_the_budget() {
    let budget = resident_bytes(DIRECTORY_ROWS) + peak_bytes(Kind::Directory, DIRECTORY_ROWS);
    let tables = Arc::new(Tables::new(budget));
    let (one, two) = (source(directory(1), || ()), source(directory(2), || ()));
    let first = tables.get(&one).unwrap();
    let reserved = reserve(&tables, Kind::Directory);
    assert_eq!(tables.resident(), budget);
    assert!(matches!(tables.get(&two), Err(LoadError::Busy)));
    let prepared = tables.insert(&two, Loaded::Directory(directory(2)), reserved);
    assert_eq!(tables.resident(), 2 * resident_bytes(DIRECTORY_ROWS));
    // Both tables in use leave no room for another preparation's peak.
    let now = std::time::Duration::ZERO;
    assert!(matches!(
        tables.reserve_peak(Kind::Directory, DIRECTORY_ROWS, now),
        Err(LoadError::Busy)
    ));
    let finished = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(100));
        drop(first);
    });
    let wait = std::time::Duration::from_secs(10);
    let reserved = tables
        .reserve_peak(Kind::Directory, DIRECTORY_ROWS, wait)
        .unwrap();
    finished.join().unwrap();
    assert_eq!(tables.resident(), budget);
    drop((reserved, prepared));
}

/// A load that idle tables cannot make room for is `Busy` without evicting the tables
/// in use, so they stay cached for their next query; an idle one is evicted instead.
#[test]
fn a_full_budget_never_evicts_tables_in_use() {
    let budget = resident_bytes(DIRECTORY_ROWS) + peak_bytes(Kind::Directory, DIRECTORY_ROWS);
    let tables = Tables::new(budget);
    let (one, two, three) = (
        source(directory(1), || ()),
        source(directory(2), || ()),
        source(directory(3), || ()),
    );
    let (first, second) = (tables.get(&one).unwrap(), tables.get(&two).unwrap());
    let loads = tables.loads();
    assert!(matches!(tables.get(&three), Err(LoadError::Busy)));
    assert!(Arc::ptr_eq(&tables.get(&one).unwrap(), &first));
    assert!(Arc::ptr_eq(&tables.get(&two).unwrap(), &second));
    assert_eq!(tables.loads(), loads);
    drop(second);
    assert!(tables.get(&three).is_ok());
    assert!(Arc::ptr_eq(&tables.get(&one).unwrap(), &first));
    assert_eq!(tables.loads(), loads + 1);
    assert!(tables.resident() <= tables.budget());
}

/// Cold builds racing: concurrent misses for one table prepare it once and share it,
/// and a different table whose preparation does not fit beside the running one is
/// `Busy` while that one runs, then loads once it finished.
#[test]
fn cold_builds_racing_prepare_once_within_the_budget() {
    let budget = peak_bytes(Kind::Directory, DIRECTORY_ROWS) + resident_bytes(DIRECTORY_ROWS);
    let tables = Arc::new(Tables::new(budget));
    let (started, release) = (
        Arc::new(std::sync::Barrier::new(2)),
        Arc::new(Mutex::new(())),
    );
    let gate = release.lock().unwrap();
    let loads = Arc::new(AtomicUsize::new(0));
    let racing = source(directory(1), {
        let (started, release, loads) = (started.clone(), release.clone(), loads.clone());
        move || {
            if loads.fetch_add(1, Ordering::SeqCst) == 0 {
                started.wait();
            }
            drop(release.lock().unwrap());
        }
    });
    let workers: Vec<_> = (0..4)
        .map(|_| {
            let (tables, racing) = (tables.clone(), racing.clone());
            std::thread::spawn(move || tables.get(&racing).unwrap())
        })
        .collect();
    started.wait();
    // The running preparation holds its peak; another one does not fit beside it.
    assert_eq!(
        tables.resident(),
        peak_bytes(Kind::Directory, DIRECTORY_ROWS)
    );
    let other = source(directory(2), || ());
    assert!(matches!(tables.get(&other), Err(LoadError::Busy)));
    drop(gate);
    let loaded: Vec<_> = workers.into_iter().map(|w| w.join().unwrap()).collect();
    assert_eq!(loads.load(Ordering::SeqCst), 1);
    assert!(loaded.iter().all(|l| Arc::ptr_eq(l, &loaded[0])));
    drop(loaded);
    assert!(tables.get(&other).is_ok());
    assert!(tables.loading.lock().unwrap().is_empty());
}

/// A request cancelled while its blocking worker prepares frees nothing: the charge
/// stays at the preparation's peak until the worker finishes, then shrinks to the
/// prepared table's resident bytes.
#[tokio::test]
async fn a_cancelled_request_keeps_its_preparation_charged() {
    let tables = Arc::new(Tables::new(1 << 40));
    let (entered, wait) = mpsc::channel();
    let (finish, finished) = mpsc::channel::<()>();
    let finished = Mutex::new(finished);
    let blocked = source(directory(1), move || {
        entered.send(()).unwrap();
        finished.lock().unwrap().recv().unwrap();
    });
    let request = tokio::spawn({
        let tables = tables.clone();
        async move {
            tokio::task::spawn_blocking(move || tables.get(&blocked).map(drop))
                .await
                .unwrap()
        }
    });
    tokio::task::spawn_blocking(move || wait.recv().unwrap())
        .await
        .unwrap();
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    assert_eq!(
        tables.resident(),
        peak_bytes(Kind::Directory, DIRECTORY_ROWS)
    );
    finish.send(()).unwrap();
    let settled = async {
        while tables.resident() != resident_bytes(DIRECTORY_ROWS) {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    };
    tokio::time::timeout(std::time::Duration::from_secs(10), settled)
        .await
        .unwrap();
    assert_eq!(tables.loads(), 1);
}

/// Concurrent misses for one filter or choice object read it once and share the cached
/// copy; one whose bytes are not the named object fails.
#[test]
fn concurrent_object_misses_load_once() {
    use zakura_pir_query_server::receiver::shards::{Coverage, ShardFilter};
    let coverage = Coverage {
        start_height: 100,
        end_height: 101,
        end_hash: [3; 32],
    };
    let filter = ShardFilter::build([1; 32], 0, coverage, &[crate::common::receiver()]).unwrap();
    let reads = Arc::new(AtomicUsize::new(0));
    let source = Arc::new(ObjectSource::filter(
        filter.digest(),
        filter.bytes().len() as u32,
        filter.receivers(),
        {
            let (reads, encoding) = (reads.clone(), filter.bytes().to_vec());
            move || {
                reads.fetch_add(1, Ordering::SeqCst);
                std::thread::sleep(std::time::Duration::from_millis(100));
                Ok(encoding.clone())
            }
        },
    ));
    let tables = Arc::new(Tables::new(1 << 30));
    let loads: Vec<_> = (0..8)
        .map(|_| {
            let (tables, source) = (tables.clone(), source.clone());
            std::thread::spawn(move || tables.object(&source).unwrap())
        })
        .collect();
    let loaded: Vec<_> = loads.into_iter().map(|l| l.join().unwrap()).collect();
    assert_eq!(reads.load(Ordering::SeqCst), 1);
    assert!(loaded.iter().all(|l| Arc::ptr_eq(l, &loaded[0])));
    assert_eq!(tables.resident(), filter.bytes().len());
    let segment = crate::prepared::empty(1).directory;
    let reference = segment.manifest.choice;
    let choice = ObjectSource::choice(&reference, move || Ok(segment.choice.clone()));
    assert_eq!(
        tables.object(&choice).unwrap().object().unwrap().len(),
        reference.bytes as usize
    );
    let wrong = ObjectSource::choice(&reference, move || Ok(vec![0; reference.bytes as usize]));
    let fresh = Tables::new(1 << 30);
    assert!(matches!(
        fresh.object(&wrong),
        Err(LoadError::Load(Error::Malformed))
    ));
    assert_eq!(fresh.resident(), 0);
}
