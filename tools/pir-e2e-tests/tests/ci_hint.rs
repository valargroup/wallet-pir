//! Independent hint oracles and damaged-artifact checks over real HTTP.
//!
//! The other fast Enhance contracts live in this executable so a publisher or
//! query-server change links them once. Each contract still runs.
#[path = "boundary_recovery_model.rs"]
mod boundary_recovery_model;
#[path = "support/encrypted.rs"]
mod encrypted;
#[path = "record_layout.rs"]
mod record_layout;
#[path = "status_routes.rs"]
mod status_routes;
use encrypted::{record, workers, Listening};
use enhance_query_server::types::DatabaseId;
use std::sync::{atomic::AtomicBool, Arc};

async fn prepare_hint_fixture(
    root: &std::path::Path,
    count: u64,
) -> (Listening, enhance_query_server::worker::Candidate) {
    use enhance_query_server::protocol::{Geometry, Lifecycle};
    use enhance_query_server::{runtime, worker::Candidate};
    let (mut listeners, _) = workers(root, 1, Arc::new(AtomicBool::new(false))).await;
    let listening = listeners.pop().unwrap();
    let shard = Lifecycle::default()
        .coverage(count, Geometry::default())
        .unwrap()
        .shards[0]
        .clone();
    let source = |start, n| Ok((start..start + n as u64).flat_map(record).collect());
    let plan = runtime::plan(shard, source).unwrap();
    let candidate = Candidate {
        placement_policy: Default::default(),
        operation: "hint-proof".into(),
        attempt: 1,
        epoch: 1,
        expected_revision: 0,
        generation: 1,
        plans: vec![plan.clone()],
    };
    let http = reqwest::Client::new();
    hint_command(&http, &listening.url, "reserve", &candidate).await;
    for (spec, identity) in plan.shard.units.iter().zip(&plan.units) {
        let rows = runtime::unit_rows(&plan.shard, spec, &mut { source }).unwrap();
        http.put(format!(
            "{}/internal/rows/{}",
            listening.url,
            identity.digest()
        ))
        .header("x-enhance-epoch", "1")
        .header("x-enhance-attempt", "1")
        .header("x-enhance-operation", &candidate.operation)
        .body(rows)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    }
    hint_command(&http, &listening.url, "prepare", &candidate).await;
    (listening, candidate)
}

async fn hint_command(
    http: &reqwest::Client,
    url: &str,
    command: &str,
    body: &impl serde::Serialize,
) {
    http.post(format!("{url}/internal/{command}"))
        .json(body)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
}

async fn hint_matches_generic(count: u64) {
    use enhance_query_server::{runtime::Engine, wire::encode_crs_blocks};
    #[cfg(not(feature = "native-reinspiring"))]
    use sha2::{Digest, Sha256};
    let root = tempfile::tempdir().unwrap();
    let (worker, candidate) = prepare_hint_fixture(root.path(), count).await;
    let oracle_root = tempfile::tempdir().unwrap();
    let oracle = Engine::new(oracle_root.path())
        .prepare(candidate.plans[0].clone(), |start, n| {
            Ok((start..start + n as u64).flat_map(record).collect())
        })
        .unwrap()
        .0;
    // Exercise the unchanged decoder/combiner/writer on independently built
    // material, rather than comparing a publication with its own serialization.
    let expected = encode_crs_blocks(&oracle.hint().unwrap());
    let http = reqwest::Client::new();
    let response = http
        .get(format!("{}/internal/hint/0", worker.url))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert!(response.headers().get("content-length").is_none());
    let actual = response.bytes().await.unwrap();
    assert_eq!(actual.as_ref(), expected);
    #[cfg(not(feature = "native-reinspiring"))]
    assert_eq!(
        hex::encode(Sha256::digest(&actual)),
        match count {
            // Immutable original 67/68 smoke material retained by the timing
            // diagnosis, before the transport optimization.
            67 => "c6020b03b132c41dbb3d7f85b97c1dcc6381e8f2ec2db8c98ac89f8db7e5b70a",
            68 => "12f1a4de5fa69d4f37d86c98c8e7e2b7f37ec3d2613b638d90075437e7825fc0",
            _ => unreachable!(),
        }
    );
    assert_eq!(
        std::fs::read_dir(root.path().join("worker-0/hints"))
            .unwrap()
            .count(),
        0
    );
    drop(http);
    worker.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_unit_hint_matches_independent_generic_67() {
    hint_matches_generic(67).await;
}

#[cfg(feature = "qualification-tests")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_unit_hint_matches_independent_generic_68() {
    hint_matches_generic(68).await;
}

fn sha256_file(path: &std::path::Path) -> String {
    use sha2::Digest;
    use std::io::Read;
    let mut file = std::fs::File::open(path).unwrap();
    let mut hasher = sha2::Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf).unwrap();
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    hex::encode(hasher.finalize())
}

async fn sha256_response(mut response: reqwest::Response) -> String {
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    while let Some(chunk) = response.chunk().await.unwrap() {
        hasher.update(&chunk);
    }
    hex::encode(hasher.finalize())
}

/// One-byte or one-length edit of a hint file, remembered so it can be undone
/// on the same inode. Replacing the file would leave the worker's open handle
/// on the damaged inode and force another preparation.
enum HintDamage {
    Byte { offset: u64, saved: u8 },
    Truncated { saved: u8 },
    Trailing,
}

fn harm_hint(path: &std::path::Path, fault: &str, len: u64) -> HintDamage {
    use std::io::{Read, Seek, SeekFrom, Write};
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    let damage = match fault {
        "truncated" => {
            file.seek(SeekFrom::End(-1)).unwrap();
            let mut saved = [0u8; 1];
            file.read_exact(&mut saved).unwrap();
            file.set_len(len - 1).unwrap();
            HintDamage::Truncated { saved: saved[0] }
        }
        "trailing" => {
            file.seek(SeekFrom::End(0)).unwrap();
            file.write_all(&[0]).unwrap();
            HintDamage::Trailing
        }
        other => {
            let offset = match other {
                "shape" => 8,
                "format" => 0,
                _ => len - 1,
            };
            file.seek(SeekFrom::Start(offset)).unwrap();
            let mut saved = [0u8; 1];
            file.read_exact(&mut saved).unwrap();
            file.seek(SeekFrom::Start(offset)).unwrap();
            file.write_all(&[saved[0] ^ 1]).unwrap();
            HintDamage::Byte {
                offset,
                saved: saved[0],
            }
        }
    };
    file.flush().unwrap();
    damage
}

fn heal_hint(path: &std::path::Path, len: u64, damage: HintDamage) {
    use std::io::{Seek, SeekFrom, Write};
    let mut file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    match damage {
        HintDamage::Truncated { saved } => {
            file.set_len(len).unwrap();
            file.seek(SeekFrom::End(-1)).unwrap();
            file.write_all(&[saved]).unwrap();
        }
        HintDamage::Trailing => file.set_len(len).unwrap(),
        HintDamage::Byte { offset, saved } => {
            file.seek(SeekFrom::Start(offset)).unwrap();
            file.write_all(&[saved]).unwrap();
        }
    }
    file.flush().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_unit_hint_rejects_damage_and_pins_retired_file() {
    use enhance_query_server::{
        ipir::shard_artifact_dir,
        worker::{Abort, Retain},
    };
    let root = tempfile::tempdir().unwrap();
    let (worker, mut candidate) = prepare_hint_fixture(root.path(), 67).await;
    let worker_root = root.path().join("worker-0");
    let artifact_root = worker_root
        .join("artifacts-9")
        .join(candidate.plans[0].units[0].digest());
    let path = shard_artifact_dir(&artifact_root, DatabaseId::Enhance, 0).join("partial-crs.bin");
    let len = std::fs::metadata(&path).unwrap().len();
    let http = reqwest::Client::new();
    let url = format!("{}/internal/hint/0", worker.url);
    // Each distinct fault is refused on these bytes. The fast path edits the
    // open file in place and keeps that runtime for the pin and retirement
    // checks. Qualification still refuses every fault over HTTP, which retires
    // the open file and prepares the same candidate again.
    let faults = ["corrupt", "truncated", "shape", "trailing", "format"];
    let degree = enhance_query_server::runtime::rlwe().d;
    let per_block = (degree * 8 + 4) * degree + 4;
    assert_eq!((len - 8) % per_block as u64, 0, "hint encoding length");
    let blocks = ((len - 8) / per_block as u64) as usize;
    let digest = serde_json::from_reader::<_, serde_json::Value>(
        std::fs::File::open(path.with_file_name("metadata.json")).unwrap(),
    )
    .unwrap()["crs_sha256"]
        .as_str()
        .unwrap()
        .to_string();
    let hint_ok = |path: &std::path::Path| {
        enhance_query_server::artifact::PublicationArtifact::revalidate_hint_file(
            path, len, &digest, blocks, degree,
        )
    };
    assert!(
        hint_ok(&path).is_ok(),
        "prepared hint must pass the worker checks"
    );
    if cfg!(feature = "qualification-tests") {
        for fault in faults {
            let damage = harm_hint(&path, fault, len);
            let response = http.get(&url).send().await.unwrap();
            assert_eq!(
                response.status(),
                reqwest::StatusCode::SERVICE_UNAVAILABLE,
                "{fault}"
            );
            let _ = response.bytes().await.unwrap();
            heal_hint(&path, len, damage);
            hint_command(
                &http,
                &worker.url,
                "abort",
                &Abort {
                    epoch: candidate.epoch,
                    operation: candidate.operation.clone(),
                    attempt: candidate.attempt,
                },
            )
            .await;
            candidate.attempt += 1;
            hint_command(&http, &worker.url, "reserve", &candidate).await;
            hint_command(&http, &worker.url, "prepare", &candidate).await;
        }
    } else {
        for fault in faults {
            let damage = harm_hint(&path, fault, len);
            assert!(hint_ok(&path).is_err(), "{fault}");
            heal_hint(&path, len, damage);
        }
    }
    // Disconnect does not poison healthy shared validation state.
    drop(http.get(&url).send().await.unwrap());
    // A pinned, verified unit remains readable without its original pathname.
    std::fs::remove_file(&path).unwrap();
    let response = http.get(&url).send().await.unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    hint_command(
        &http,
        &worker.url,
        "abort",
        &Abort {
            epoch: candidate.epoch,
            operation: candidate.operation.clone(),
            attempt: candidate.attempt,
        },
    )
    .await;
    // Abort drops the candidate runtime; the response owns only its pinned file.
    // The manual unlink above must not stand in for retention's directory GC.
    assert!(
        artifact_root.is_dir(),
        "unit directory must exist before GC"
    );
    hint_command(
        &http,
        &worker.url,
        "retain",
        &Retain {
            epoch: 1,
            generations: vec![1],
        },
    )
    .await;
    assert!(
        !artifact_root.exists(),
        "retirement must reclaim the unit directory"
    );
    assert_eq!(sha256_response(response).await, digest);
    // A saved path alone cannot authorize serving, even when it holds the
    // prepared bytes. Copying keeps the shared fixture's inode untouched.
    let relative = path.strip_prefix(&worker_root).unwrap();
    let source = encrypted::shared_hint_fixture(&candidate.plans[0]);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::copy(source.join(relative), &path).unwrap();
    let response = http.get(&url).send().await.unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    let _ = response.bytes().await.unwrap();
    // Corruption, unlinking and retirement touched only this worker's copy.
    {
        let mut next_epoch = candidate.plans[0].clone();
        next_epoch.units[0].recovery_epoch += 1;
        assert_eq!(encrypted::shared_hint_fixture(&next_epoch), source);
        next_epoch.units[0].content_sha256 = "00".repeat(32);
        assert_ne!(encrypted::shared_hint_fixture(&next_epoch), source);
        assert_eq!(sha256_file(&source.join(relative)), digest);
    }
    drop(http);
    worker.stop().await;
}
