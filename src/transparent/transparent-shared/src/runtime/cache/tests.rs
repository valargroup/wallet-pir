#[cfg(target_os = "linux")]
use crate::runtime::scheduling::BUILD_NICE;
use crate::runtime::table_runtime::{first_differing_column, SELF_CHECK_COLUMNS};
#[test]
fn eviction_keeps_selected_idle_entries_and_pinned_ones() {
    let cache = RuntimeCache::new(1024, 1, Arc::new(Metrics::default()));
    let entry = |reserved| Entry {
        slot: Arc::new(Slot {
            runtime: tokio::sync::OnceCell::new(),
        }),
        reserved,
        last_used: 0,
    };
    let current = ("current".to_string(), Table::Directory, 0);
    let retired = ("retired".to_string(), Table::Directory, 0);
    let in_use = ("in-use".to_string(), Table::Directory, 0);
    let held = entry(40);
    let pin = held.slot.clone();
    {
        let mut inner = cache.inner.lock().unwrap();
        inner.entries.insert(current.clone(), entry(10));
        inner.entries.insert(retired.clone(), entry(20));
        inner.entries.insert(in_use.clone(), held);
        inner.resident = 70;
    }
    cache.evict_unpinned_except(|key| key.0 == "current");
    let inner = cache.inner.lock().unwrap();
    assert!(inner.entries.contains_key(&current));
    assert!(inner.entries.contains_key(&in_use), "a query holds it");
    assert!(!inner.entries.contains_key(&retired));
    assert_eq!(inner.resident, 50);
    drop(pin);
}

#[test]
fn repair_evicts_only_affected_idle_runtime_entries() {
    let root = tempfile::tempdir().unwrap();
    let disk = disk::DiskCache::new(root.path().to_path_buf(), 1024).unwrap();
    let cache = RuntimeCache::new(1024, 1, Arc::new(Metrics::default())).with_disk(Some(disk));
    let affected = ("affected".to_string(), Table::Directory, 0);
    let healthy = ("healthy".to_string(), Table::Directory, 0);
    use sha2::{Digest, Sha256};
    let affected_disk = root.path().join(format!(
        "{}-cached.runtime",
        hex::encode(Sha256::digest(affected.0.as_bytes()))
    ));
    let healthy_disk = root.path().join(format!(
        "{}-cached.runtime",
        hex::encode(Sha256::digest(healthy.0.as_bytes()))
    ));
    std::fs::write(&affected_disk, b"derived from corrupt source").unwrap();
    std::fs::write(&healthy_disk, b"unrelated runtime").unwrap();
    let pinned = Arc::new(Slot {
        runtime: tokio::sync::OnceCell::new(),
    });
    {
        let mut inner = cache.inner.lock().unwrap();
        inner.entries.insert(
            affected.clone(),
            Entry {
                slot: pinned.clone(),
                reserved: 10,
                last_used: 0,
            },
        );
        inner.entries.insert(
            healthy.clone(),
            Entry {
                slot: Arc::new(Slot {
                    runtime: tokio::sync::OnceCell::new(),
                }),
                reserved: 20,
                last_used: 0,
            },
        );
        inner.resident = 30;
    }
    assert!(!cache.evict_keys(std::slice::from_ref(&affected)).unwrap());
    assert!(
        affected_disk.exists(),
        "an active snapshot writer keeps its file"
    );
    assert_eq!(
        cache.resident_bytes(),
        30,
        "an in-flight request retains its budget"
    );
    drop(pinned);
    let writer = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(root.path().join(".lock"))
        .unwrap();
    writer.lock().unwrap();
    assert!(
        !cache.evict_keys(std::slice::from_ref(&affected)).unwrap(),
        "an unrelated disk writer must defer repair without blocking the cache"
    );
    assert!(affected_disk.exists());
    drop(writer);
    assert!(cache.evict_keys(std::slice::from_ref(&affected)).unwrap());
    assert!(
        !affected_disk.exists(),
        "a repaired source cannot restore its old derivative"
    );
    assert!(healthy_disk.exists());
    assert_eq!(cache.resident_bytes(), 20);
    assert!(cache.inner.lock().unwrap().entries.contains_key(&healthy));
}

/// The tiled walk visits every cell and names the lowest differing
/// column, as the column-order walk it replaced did, at tile edges and in
/// a partial last tile.
#[test]
fn self_check_tiles_find_the_lowest_differing_column() {
    let (cols, rows) = (2 * SELF_CHECK_COLUMNS + 5, 9);
    assert_eq!(first_differing_column(cols, rows, |_, _| false), None);
    for col in 0..cols {
        for row in [0, rows / 2, rows - 1] {
            assert_eq!(
                first_differing_column(cols, rows, |r, c| (r, c) == (row, col)),
                Some(col)
            );
        }
        let other = (col + SELF_CHECK_COLUMNS) % cols;
        assert_eq!(
            first_differing_column(cols, rows, |r, c| (r == 0 && c == other)
                || (r == rows - 1 && c == col)),
            Some(col.min(other))
        );
    }
}

#[test]
fn runtime_builds_run_in_their_own_smaller_pool() {
    let cores = std::thread::available_parallelism().map_or(1, |n| n.get());
    let (index, threads) =
        build_pool().install(|| (rayon::current_thread_index(), rayon::current_num_threads()));
    assert!(index.is_some(), "the closure runs on a build pool thread");
    assert!(threads <= cores.max(1));
    assert!(
        rayon::current_thread_index().is_none(),
        "the caller is not a pool thread"
    );
    #[cfg(target_os = "linux")]
    assert_eq!(
        build_pool()
            .install(
                || rustix::process::getpriority_process(Some(rustix::thread::gettid())).unwrap()
            ),
        BUILD_NICE
    );
}

use super::*;
use transparent_shard::display::{TXID_2K8, TXID_4K8};
use transparent_shard::layout::{DECOUPLED_2K_8K, DECOUPLED_32K_32K};

// Cache units exercise the generic native API with one supported column
// instance. These are test domains, never advertised publication geometries.
// Production HTTP checks and qualification retain the registered layouts.
pub(crate) const NATIVE_INSTANCE: transparent_shard::layout::Geometry =
    transparent_shard::layout::Geometry {
        name: "test-native-instance",
        directory_row_bytes: 2_048,
        page_row_bytes: 2_048,
        ..TXID_2K8
    };

/// The masks a table's rows publish, from the whole-table hint over the
/// columns the profile encodes (single bytes for an 8-bit table, u16
/// pairs for a 16-bit one), without the scan server: what the certificate
/// tool rebuilds.
/// Ordinary cache/identity checks need valid native arithmetic, not large
/// history rows. Full qualification retains the caller's deployed shape.
pub(crate) fn fixture_shape(
    geometry: &'static transparent_shard::layout::Geometry,
    table: Table,
) -> (&'static transparent_shard::layout::Geometry, Table) {
    if cfg!(feature = "qualification-tests") {
        (geometry, table)
    } else {
        (&NATIVE_INSTANCE, Table::TxDirectory)
    }
}

/// Share only parameters of the same schema. History and display derive
/// different masks even when their test dimensions match.
pub(crate) fn fixture_params(
    geometry: &'static transparent_shard::layout::Geometry,
    table: Table,
    display: bool,
) -> Arc<SharedParams> {
    if cfg!(feature = "qualification-tests") {
        return Arc::new(if display {
            SharedParams::build_display(geometry, table).unwrap()
        } else {
            SharedParams::build(geometry, table).unwrap()
        });
    }
    assert_eq!(geometry, &NATIVE_INSTANCE);
    assert_eq!(table, Table::TxDirectory);
    static HISTORY: std::sync::LazyLock<Arc<SharedParams>> = std::sync::LazyLock::new(|| {
        Arc::new(SharedParams::build(&NATIVE_INSTANCE, Table::TxDirectory).unwrap())
    });
    static DISPLAY: std::sync::LazyLock<Arc<SharedParams>> = std::sync::LazyLock::new(|| {
        Arc::new(SharedParams::build_display(&NATIVE_INSTANCE, Table::TxDirectory).unwrap())
    });
    if display {
        DISPLAY.clone()
    } else {
        HISTORY.clone()
    }
}

/// Two fully occupied deterministic inputs for recovery tests. All rows
/// differ between inputs, so a random self-check cannot miss the forgery.
pub(crate) fn recovery_rows(shared: &SharedParams, different: bool) -> Vec<u8> {
    let seed = if different { 5 } else { 4 };
    (0..shared.profile.rows * shared.profile.row_bytes)
        .map(|i| ((i + seed * 7_919).wrapping_mul(2_654_435_761) >> 9) as u8)
        .collect()
}

/// Prepare immutable native bytes once; every consumer gets a private file
/// before testing corruption, replacement or retirement. Qualification
/// deliberately keeps independent cold builds.
pub(crate) fn prepared_fixture(shared: &SharedParams, rows: &[u8]) -> Result<TableRuntime, String> {
    use disk::ShippedRuntimes;
    if cfg!(feature = "qualification-tests") {
        return TableRuntime::build(shared, rows);
    }
    static LOCAL: std::sync::OnceLock<tempfile::TempDir> = std::sync::OnceLock::new();
    let root = std::env::var_os("WALLET_PIR_TEST_RUNTIME_CACHE")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            LOCAL
                .get_or_init(|| tempfile::tempdir().unwrap())
                .path()
                .to_path_buf()
        })
        .join("transparent-predecessors");
    std::fs::create_dir_all(&root).unwrap();
    let shared_files = ShippedRuntimes::new(root.clone());
    let key = ("ab".repeat(32), shared.table, 0);
    let digest = hex::encode(Sha256::digest(rows));
    let path = shared_files.path(&key, shared, &digest);
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(root.join("fixture.lock"))
        .unwrap();
    lock.lock().unwrap();
    if !path.exists() {
        let runtime = TableRuntime::build(shared, rows)?;
        shared_files.write(&key, shared, &digest, &runtime).unwrap();
    }
    let private = tempfile::tempdir().unwrap();
    let files = ShippedRuntimes::new(private.path().to_path_buf());
    std::fs::copy(&path, files.path(&key, shared, &digest)).unwrap();
    drop(lock);
    // Loaded runtimes own their mappings after the private pathname is
    // removed; no consumer can mutate the retained fixture's inode.
    files
        .load(&key, shared, &digest)
        .map_err(|error| error.to_string())
}

fn reference_masks(profile: &native::TableProfile, rows: &[u8]) -> Vec<u8> {
    let hint = if profile.native.entry_bytes() == 1 {
        let columns: Vec<Vec<u8>> = (0..profile.cols)
            .map(|col| {
                (0..profile.rows)
                    .map(|row| rows[row * profile.row_bytes + col])
                    .collect()
            })
            .collect();
        zakura_pir_server_core::native::hint(&profile.masks, profile.rows, profile.cols, |c| {
            &columns[c]
        })
    } else {
        let columns: Vec<Vec<u16>> = (0..profile.cols)
            .map(|col| {
                (0..profile.rows)
                    .map(|row| native::row_coefficient(rows, profile.row_bytes, row, col))
                    .collect()
            })
            .collect();
        zakura_pir_server_core::native::hint(&profile.masks, profile.rows, profile.cols, |c| {
            &columns[c]
        })
    }
    .unwrap();
    profile
        .native
        .publish(&zakura_pir_server_core::native::preprocess(&profile.setup, &hint).unwrap())
        .unwrap()
}

fn reservation(table: Table, rows: u64, row_bytes: usize) -> u64 {
    reserved_bytes(table, rows, row_bytes as u32)
}

#[test]
fn failed_waiter_cannot_forget_a_replacement_slot() {
    let cache = RuntimeCache::new(1024, 1, Arc::new(Metrics::default()));
    let key = ("revision".to_string(), Table::Directory, 0);
    let (failed, _) = cache.slot_for(key.clone(), 100).unwrap();
    cache.forget(&key, &failed);
    let (replacement, _) = cache.slot_for(key.clone(), 100).unwrap();
    cache.forget(&key, &failed);
    assert_eq!(cache.resident_bytes(), 100);
    assert!(Arc::ptr_eq(
        &cache.slot_for(key, 100).unwrap().0,
        &replacement
    ));
}

fn mib(bytes: u64) -> f64 {
    bytes as f64 / f64::from(1 << 20)
}

/// A built runtime, history or display, holds no more than the planned
/// four-byte size and no less than the 27-bit packed matrix a CPU with
/// AVX-512 VBMI may choose, so its charge never exceeds its reservation.
#[test]
fn a_built_runtime_holds_its_planned_size_or_less() {
    let matrix_narrow = (native::D * native::D * native::ELL * 4) as u64;
    let matrix_packed = (native::D * (native::D * native::ELL / 8) * 27 + 8) as u64;
    for (geometry, table) in [
        (&DECOUPLED_2K_8K, Table::Directory),
        fixture_shape(&TXID_2K8, Table::TxDirectory),
    ] {
        let shared = SharedParams::build(geometry, table).unwrap();
        let profile = &shared.profile;
        let blocks = profile.blocks() as u64;
        let rows: Vec<u8> = (0..profile.rows * profile.row_bytes)
            .map(|i| (i.wrapping_mul(2_654_435_761) >> 9) as u8)
            .collect();
        let held = TableRuntime::build(&shared, &rows).unwrap().held_bytes();
        eprintln!(
            "{} {}: reserved {:.2} MiB, planned {:.2} MiB, built {:.2} MiB",
            geometry.name,
            table.as_str(),
            mib(shared.reserved_bytes()),
            mib(shared.held_bytes()),
            mib(held)
        );
        assert!(held <= shared.held_bytes(), "{}", geometry.name);
        if profile.native.ell == 1 {
            // A one-digit matrix needs eight-byte words on a table with
            // data: the 8-bit history tables hold their reservation.
            assert_eq!(held, shared.reserved_bytes(), "{}", geometry.name);
            continue;
        }
        assert!(
            held >= shared.held_bytes() - blocks * (matrix_narrow - matrix_packed),
            "{}",
            geometry.name
        );
        assert!(held < shared.reserved_bytes());
    }
}

/// Admission is unchanged: a new entry is admitted against its full
/// bound, and while it is building that bound stays charged, so builds in
/// flight together can never be admitted past the budget on the strength
/// of sizes they have not reached yet. Only once a runtime exists does its
/// charge fall, never rise, and eviction returns exactly what is charged.
#[test]
fn a_reservation_is_released_only_after_its_build_and_only_downward() {
    let shared = SharedParams::build_display(&TXID_2K8, Table::TxDirectory).unwrap();
    // A runtime built below its bound, as an empty table's is.
    let (bound, held) = (shared.reserved_bytes(), shared.reserved_bytes() / 2);
    let metrics = Arc::new(Metrics::default());
    let cache = RuntimeCache::new(bound + held, 2, metrics.clone());
    let key = |segment| ("revision".to_string(), Table::TxDirectory, segment);
    let (a, _) = cache.slot_for(key(0), bound).unwrap();
    assert_eq!(cache.resident_bytes(), bound);
    // A second build may not start while the first still holds its bound,
    // although both would fit at their built size.
    assert!(matches!(
        cache.slot_for(key(1), bound),
        Err(CacheError::Overloaded)
    ));
    cache.settle(&key(0), &a, held);
    assert_eq!(cache.resident_bytes(), held);
    assert_eq!(Metrics::get(&metrics.resident_bytes), held);
    let (b, _) = cache.slot_for(key(1), bound).unwrap();
    assert_eq!(cache.resident_bytes(), held + bound);
    assert_eq!(cache.resident_bytes(), cache.budget());
    // A runtime that came out larger than planned keeps its bound, and a
    // second settle cannot raise a charge already lowered.
    cache.settle(&key(1), &b, bound + 1);
    cache.settle(&key(0), &a, bound);
    assert_eq!(cache.resident_bytes(), held + bound);
    // A stale slot cannot rewrite a replacement's charge.
    let stale = Arc::new(Slot {
        runtime: tokio::sync::OnceCell::new(),
    });
    cache.settle(&key(1), &stale, 0);
    assert_eq!(cache.resident_bytes(), held + bound);
    assert!(matches!(
        cache.slot_for(key(2), bound),
        Err(CacheError::Overloaded)
    ));
    drop((a, b));
    cache.evict_unpinned();
    assert_eq!(cache.resident_bytes(), 0);
    assert_eq!(cache.entries(), 0);
    assert_eq!(Metrics::get(&metrics.resident_bytes), 0);
    assert_eq!(Metrics::get(&metrics.cache_entries), 0);
    assert_eq!(Metrics::get(&metrics.evictions), 2);
}

/// The same through real builds: a build stalled on its source keeps its
/// whole bound charged, so a second build that would fit only at the
/// first's built size is refused until the first runtime exists. Then the
/// first is charged what it holds, and the second is admitted beside it
/// without evicting anything.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn a_build_in_flight_keeps_its_bound_until_the_runtime_exists() {
    use std::io::Write;
    // An empty 8-bit table: its one-digit matrix fits four-byte words, so
    // it is built narrower than its bound, which a table with data is not.
    let (geometry, table) = fixture_shape(&TXID_2K8, Table::TxDirectory);
    let dir = tempfile::tempdir().unwrap();
    let shared = Arc::new(SharedParams::build(geometry, table).unwrap());
    let rows = vec![0u8; shared.profile.rows * shared.profile.row_bytes];
    let built = TableRuntime::build(&shared, &rows).unwrap().held_bytes();
    let bound = shared.reserved_bytes();
    assert!(built < bound, "an empty table is built below its bound");
    let source = SegmentSource::published(
        dir.path().join("table.bin"),
        table.rows(geometry),
        table.row_bytes(geometry),
        hex::encode(Sha256::digest(&rows)),
        table.rows(geometry),
    );
    assert!(std::process::Command::new("mkfifo")
        .arg(source.path())
        .status()
        .unwrap()
        .success());
    let (opened_tx, opened_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let writer = {
        let (path, rows) = (source.path().to_path_buf(), rows.clone());
        std::thread::spawn(move || {
            let mut file = std::fs::File::create(path).unwrap();
            let _ = opened_tx.send(());
            release_rx.recv().unwrap();
            file.write_all(&rows).unwrap();
        })
    };
    let metrics = Arc::new(Metrics::default());
    let cache = Arc::new(RuntimeCache::new(bound + built, 2, metrics.clone()));
    let key = move |segment| ("ab".repeat(32), table, segment);
    let first = {
        let (cache, shared, source) = (cache.clone(), shared.clone(), source.clone());
        tokio::spawn(async move { cache.get(key(0), shared, source).await })
    };
    tokio::time::timeout(std::time::Duration::from_secs(30), opened_rx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cache.resident_bytes(), bound);
    assert!(matches!(
        cache.get(key(1), shared.clone(), source.clone()).await,
        Err(CacheError::Overloaded)
    ));
    assert_eq!(Metrics::get(&metrics.overloads), 1);
    assert_eq!(cache.resident_bytes(), bound);
    release_tx.send(()).unwrap();
    writer.join().unwrap();
    let first = first.await.unwrap().unwrap();
    let held = first.get().held_bytes();
    assert_eq!(held, built);
    assert_eq!(cache.resident_bytes(), held);
    assert_eq!(Metrics::get(&metrics.resident_bytes), held);
    std::fs::remove_file(source.path()).unwrap();
    std::fs::write(source.path(), &rows).unwrap();
    let second = cache.get(key(1), shared.clone(), source).await.unwrap();
    assert_eq!(cache.resident_bytes(), held + second.get().held_bytes());
    assert!(cache.resident_bytes() <= cache.budget());
    assert_eq!(Metrics::get(&metrics.builds), 2);
    assert_eq!(Metrics::get(&metrics.evictions), 0);
    assert_eq!(Metrics::get(&metrics.cache_entries), 2);
}

/// The warm-fit check is never stricter than the sum of reservations it
/// replaces, for every history and display geometry: equal while every
/// runtime can be in flight at once, smaller beyond that by the bound's
/// excess of each runtime that cannot.
#[test]
fn the_warm_fit_never_exceeds_the_reservations_it_replaces() {
    use transparent_shard::display::DISPLAY_PROFILES;
    let mut every = Vec::new();
    for geometry in transparent_shard::layout::PROFILES {
        for table in [Table::Directory, Table::Pages] {
            every.push(SharedParams::build(geometry, table).unwrap());
        }
    }
    for geometry in DISPLAY_PROFILES {
        every.push(SharedParams::build_display(geometry, Table::TxDirectory).unwrap());
    }
    for shared in &every {
        let excess = shared.reserved_bytes() - shared.held_bytes();
        for count in 0..8u64 {
            for in_flight in 0..6u64 {
                let set = std::iter::repeat_n(shared, count as usize);
                let needed = warm_bytes(set, in_flight as usize);
                let charged = count * shared.reserved_bytes();
                assert!(needed <= charged);
                assert_eq!(charged - needed, count.saturating_sub(in_flight) * excess);
            }
        }
    }
    // Every table is 8-bit and holds its bound, so there is no excess.
    let all = warm_bytes(&every, 4);
    assert!(all <= every.iter().map(SharedParams::reserved_bytes).sum::<u64>());
    assert_eq!(
        warm_bytes(&every, every.len()),
        every.iter().map(SharedParams::reserved_bytes).sum::<u64>()
    );
}

/// Runtimes a combined display worker holds at genesis coverage: 189
/// archives of 90,000 entries (17.05M entries at the 2026-10-11 tip), the
/// recent shard and one retired recent revision.
pub(crate) const GENESIS_RUNTIMES: usize = 191;

/// A `txid-2k8` display runtime is 8-bit: its 8,192-byte rows are four
/// 2,048-byte blocks, each with a one-digit compiled matrix at eight-byte
/// words, so the planned built size is the reservation, 144.17 MiB. The
/// genesis plan's 191 runtimes hold 26.9 GiB, their built size being their
/// bound, which fits a 32 GiB cache and the display worker's 48G `MemoryMax`.
#[test]
fn genesis_display_runtimes_fit_at_their_built_size() {
    let shared = SharedParams::build_display(&TXID_2K8, Table::TxDirectory).unwrap();
    assert_eq!(shared.profile.blocks(), 4);
    let (reserved, held) = (shared.reserved_bytes(), shared.held_bytes());
    assert_eq!((reserved, held), (151_171_248, 151_171_248));
    assert!((mib(held) - 144.17).abs() < 0.01);
    let gib = |bytes: u64| bytes as f64 / f64::from(1 << 30);
    let runtimes = std::iter::repeat_n(&shared, GENESIS_RUNTIMES);
    let needed = warm_bytes(runtimes, 4);
    assert!((gib(needed) - 26.89).abs() < 0.1, "{}", gib(needed));
    assert_eq!(needed, GENESIS_RUNTIMES as u64 * shared.reserved_bytes());
    assert!(needed < 32 << 30, "a 32 GiB cache");
}

/// A `txid-4k8` runtime has twice the rows at the same four blocks: the
/// database doubles, the compiled matrices do not, so 160.17 MiB. With
/// 180,000-entry archives, genesis coverage is about 95 archives, so about
/// 97 runtimes: 15.2 GiB.
#[test]
fn genesis_txid_4k8_runtimes_fit_at_their_built_size() {
    let shared = SharedParams::build_display(&TXID_4K8, Table::TxDirectory).unwrap();
    assert_eq!(shared.profile.blocks(), 4);
    let (reserved, held) = (shared.reserved_bytes(), shared.held_bytes());
    assert_eq!(reserved, held);
    assert!((mib(held) - 160.17).abs() < 0.01, "{reserved}");
    let gib = |bytes: u64| bytes as f64 / f64::from(1 << 30);
    let needed = warm_bytes(std::iter::repeat_n(&shared, 97), 4);
    assert!((gib(needed) - 15.17).abs() < 0.1, "{}", gib(needed));
    assert!(needed < 32 << 30, "a 32 GiB cache");
}

/// The native reservation at the pinned geometry. A recent page table
/// (8-bit): a 32 MiB database, two blocks of published masks, and the
/// bound on two blocks of one-digit preprocessing (32 MiB of compiled
/// matrix plus both masks each). A txid display table (8-bit) the same
/// per block, at four 2,048-byte blocks per 8,192-byte row.
///
/// If this fails, the cache budget is being computed against a different
/// scheme than the one deployed, and every sizing decision downstream of it
/// is wrong. Re-measure with `shard-residency` rather than adjusting the
/// formula to match.
#[test]
fn the_reservation_is_the_native_upper_bound() {
    assert_eq!(
        reservation(
            Table::Pages,
            DECOUPLED_2K_8K.page_rows,
            DECOUPLED_2K_8K.page_row_bytes
        ),
        (32 << 20) + 2 * 11_264 + 16 + 2 * ((32 + 2 * 2_048 * 8 + 8) + (32 << 20))
    );
    let (rows, row_bytes) = (
        Table::TxDirectory.rows(&TXID_2K8),
        Table::TxDirectory.row_bytes(&TXID_2K8),
    );
    let blocks = u64::from(row_bytes) / 2_048;
    assert_eq!(
        reservation(Table::TxDirectory, rows, row_bytes as usize),
        rows * u64::from(row_bytes)
            + blocks * 11_264
            + 16
            + blocks * ((32 + 2 * 2_048 * 8 + 8) + (32 << 20))
    );
}

/// Only the database term follows the row count: the published masks and
/// preprocessing follow the row width, which the registry holds at one
/// instance. That is why a taller table is cheap in memory while it divides
/// the shard count, and it is the whole basis of the archive tier.
#[test]
fn only_the_database_term_follows_the_row_count() {
    let narrow = reservation(
        Table::Pages,
        DECOUPLED_2K_8K.page_rows,
        DECOUPLED_2K_8K.page_row_bytes,
    );
    let wide = reservation(
        Table::Pages,
        DECOUPLED_32K_32K.page_rows,
        DECOUPLED_32K_32K.page_row_bytes,
    );
    let fixed = narrow - DECOUPLED_2K_8K.page_rows * DECOUPLED_2K_8K.page_row_bytes as u64;
    let ratio = DECOUPLED_32K_32K.page_rows / DECOUPLED_2K_8K.page_rows;
    assert_eq!(wide - fixed, (narrow - fixed) * ratio);
}

/// The certificate tool rebuilds a segment's masks from its row bytes
/// without the scan server. It binds its report to a snapshot by requiring
/// those masks to equal the served ones, so the two derivations must agree.
#[test]
fn masks_rebuilt_from_row_bytes_equal_the_served_masks() {
    // Fast uses one native column instance. Qualification keeps the
    // deployed history page geometry this oracle was first pinned on.
    let (geometry, table) = fixture_shape(&DECOUPLED_2K_8K, Table::Pages);
    let shared = SharedParams::build(geometry, table).unwrap();
    let profile = &shared.profile;
    let rows: Vec<u8> = (0..profile.rows * profile.row_bytes)
        .map(|i| (i.wrapping_mul(2_654_435_761) >> 7) as u8)
        .collect();
    let rebuilt = reference_masks(profile, &rows);
    assert_eq!(
        rebuilt,
        TableRuntime::build(&shared, &rows).unwrap().public_params
    );
}

/// Every registry geometry must have parameters and a reservation that
/// fits a plausible budget. A geometry whose runtime did not fit one worker
/// could never be served at all, and finding that out at first query would
/// be finding it out in production.
#[test]
fn every_registry_geometry_reserves_something_servable() {
    for geometry in transparent_shard::layout::PROFILES {
        for table in [Table::Directory, Table::Pages] {
            let shared = SharedParams::build(geometry, table).unwrap();
            let bytes = shared.reserved_bytes();
            assert!(bytes > 0, "{}", geometry.name);
            assert!(
                bytes < (2 << 30),
                "{} reserves {bytes} bytes for one segment",
                geometry.name
            );
            // One byte per coefficient: a 4,096-byte row is two blocks.
            assert_eq!(shared.profile.blocks(), 2, "{}", geometry.name);
        }
    }
}
