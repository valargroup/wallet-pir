//! Independent hint oracles and damaged-artifact checks over real HTTP.
#[path = "support/encrypted.rs"]
mod encrypted;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_unit_hint_rejects_damage_and_pins_retired_file() {
    use enhance_query_server::{
        ipir::shard_artifact_dir,
        worker::{Abort, Retain},
    };
    use std::io::{Seek, SeekFrom, Write};
    let root = tempfile::tempdir().unwrap();
    let (worker, mut candidate) = prepare_hint_fixture(root.path(), 67).await;
    let worker_root = root.path().join("worker-0");
    let artifact_root = worker_root
        .join("artifacts-9")
        .join(candidate.plans[0].units[0].digest());
    let path = shard_artifact_dir(&artifact_root, DatabaseId::Enhance, 0).join("partial-crs.bin");
    let original = std::fs::read(&path).unwrap();
    let http = reqwest::Client::new();
    let url = format!("{}/internal/hint/0", worker.url);
    // Damage the real owning worker's pinned material after successful prepare.
    // Each retry restores immutable material and obtains a fresh verified handle.
    for fault in ["corrupt", "truncated", "shape", "trailing", "format"] {
        let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        match fault {
            "truncated" => file.set_len(original.len() as u64 - 1).unwrap(),
            "trailing" => {
                file.seek(SeekFrom::End(0)).unwrap();
                file.write_all(&[0]).unwrap();
            }
            _ => {
                let offset = match fault {
                    "shape" => 8,
                    "format" => 0,
                    _ => original.len() - 1,
                };
                file.seek(SeekFrom::Start(offset as u64)).unwrap();
                file.write_all(&[original[offset] ^ 1]).unwrap();
            }
        }
        drop(file);
        let response = http.get(&url).send().await.unwrap();
        assert_eq!(
            response.status(),
            reqwest::StatusCode::SERVICE_UNAVAILABLE,
            "{fault}"
        );
        let _ = response.bytes().await.unwrap();
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
        // Atomic replacement, never mutate the diagnostic author's raw inputs.
        let replacement = path.with_extension("replacement");
        std::fs::write(&replacement, &original).unwrap();
        std::fs::rename(replacement, &path).unwrap();
        candidate.attempt += 1;
        hint_command(&http, &worker.url, "reserve", &candidate).await;
        hint_command(&http, &worker.url, "prepare", &candidate).await;
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
    assert_eq!(response.bytes().await.unwrap().as_ref(), original);
    // A saved path alone cannot authorize serving, even if it contains valid bytes.
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, &original).unwrap();
    let response = http.get(&url).send().await.unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    let _ = response.bytes().await.unwrap();
    // Corruption, unlinking and retirement touched only this worker's copy.
    {
        let relative = path.strip_prefix(&worker_root).unwrap();
        let retained = if let Some(source) = encrypted::shared_hint_fixture(&candidate.plans[0]) {
            std::fs::read(source.join(relative)).unwrap()
        } else {
            let fixtures = encrypted::HINT_FIXTURES.lock().unwrap();
            let source = &fixtures.iter().find(|(count, _)| *count == 67).unwrap().1;
            std::fs::read(source.path().join(relative)).unwrap()
        };
        assert_eq!(retained, original);
    }
    drop(http);
    worker.stop().await;
}
