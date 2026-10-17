//! Exercise durable decision delivery without constructing publication material.
use super::*;
use axum::{extract::State as HttpState, response::IntoResponse, Router};

#[derive(Clone, Copy, Default, PartialEq)]
enum Fault {
    #[default]
    None,
    Offline,
    RejectCommit,
    LoseCommitReply,
    WrongDigest,
    RejectAbort,
    LoseAbortReply,
}

#[derive(Default)]
struct PeerState {
    fault: Fault,
    candidate: Option<worker::Candidate>,
    published: BTreeMap<u64, String>,
    prepared: Vec<worker::Candidate>,
    commits: Vec<worker::Commit>,
    aborts: Vec<worker::Abort>,
    retention: Vec<worker::Retain>,
}

struct Peer {
    url: String,
    state: Arc<Mutex<PeerState>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Peer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Peer {
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(Mutex::new(PeerState::default()));
        let router = Router::new()
            .fallback(peer_request)
            .with_state(state.clone());
        let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        Self { url, state, task }
    }

    fn fault(&self, fault: Fault) {
        self.state.lock().unwrap().fault = fault;
    }

    fn candidate(&self, generation: u64) {
        self.state.lock().unwrap().candidate = Some(worker::Candidate {
            placement_policy: Default::default(),
            epoch: 1,
            operation: format!("publication-{generation}"),
            attempt: generation,
            expected_revision: 0,
            generation,
            plans: vec![],
        });
    }
}

async fn peer_request(
    HttpState(state): HttpState<Arc<Mutex<PeerState>>>,
    request: axum::extract::Request,
) -> axum::response::Response {
    use axum::{http::StatusCode, Json};
    let path = request.uri().path().to_owned();
    let body = axum::body::to_bytes(request.into_body(), 65536)
        .await
        .unwrap();
    let mut peer = state.lock().unwrap();
    if peer.fault == Fault::Offline {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    match path.as_str() {
        "/internal/health" => {
            let mut digests = peer.published.clone();
            if peer.fault == Fault::WrongDigest {
                for value in digests.values_mut() {
                    *value = "incorrect digest".into();
                }
            }
            Json(serde_json::json!({
                "published": peer.published.keys().collect::<Vec<_>>(),
                "published_manifest_digests": digests,
                "candidate": peer.candidate,
                "revision": 0,
            }))
            .into_response()
        }
        "/internal/prepare" => {
            let candidate: worker::Candidate = serde_json::from_slice(&body).unwrap();
            assert_eq!(peer.candidate.as_ref(), Some(&candidate));
            peer.prepared.push(candidate);
            StatusCode::OK.into_response()
        }
        "/internal/commit" => {
            if peer.fault == Fault::RejectCommit {
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            }
            let commit: worker::Commit = serde_json::from_slice(&body).unwrap();
            let candidate = peer.candidate.take().unwrap();
            assert_eq!(commit.generation, candidate.generation);
            assert_eq!(commit.revision, 1);
            assert!(commit.retained.contains(&commit.generation));
            peer.published
                .insert(commit.generation, commit.manifest_digest.clone());
            peer.commits.push(commit);
            if peer.fault == Fault::LoseCommitReply {
                StatusCode::SERVICE_UNAVAILABLE.into_response()
            } else {
                StatusCode::OK.into_response()
            }
        }
        "/internal/abort" => {
            if peer.fault == Fault::RejectAbort {
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            }
            let abort: worker::Abort = serde_json::from_slice(&body).unwrap();
            peer.candidate = None;
            peer.aborts.push(abort);
            if peer.fault == Fault::LoseAbortReply {
                StatusCode::SERVICE_UNAVAILABLE.into_response()
            } else {
                StatusCode::OK.into_response()
            }
        }
        "/internal/retain" => {
            let retained: worker::Retain = serde_json::from_slice(&body).unwrap();
            peer.published
                .retain(|generation, _| retained.generations.contains(generation));
            peer.retention.push(retained);
            StatusCode::OK.into_response()
        }
        _ => panic!("unexpected recovery request: {path}"),
    }
}

fn groups(peers: &[Peer; 2]) -> Vec<Group> {
    vec![Group {
        placement_policy: Default::default(),
        id: "group".into(),
        sequence: 0,
        replicas: peers
            .iter()
            .enumerate()
            .map(|(index, peer)| crate::control::Replica {
                name: format!("peer-{index}"),
                url: peer.url.clone(),
                incarnation: String::new(),
                ledger: Default::default(),
            })
            .collect(),
    }]
}

fn notification(replica: usize, generation: u64) -> PendingCommit {
    let vector: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../enhance-pir/tests/fixtures/v7-session.json"
    )))
    .unwrap();
    let mut manifest: Manifest = serde_json::from_value(vector["manifest"].clone()).unwrap();
    manifest.generation = generation;
    manifest.protocol_revision = enhance_pir::protocol::PROTOCOL_REVISION.into();
    for (session, shard) in manifest.sessions.iter_mut().zip(&manifest.coverage.shards) {
        session.parameter_id = enhance_pir::protocol::parameter_id(shard.logical_rows).unwrap();
    }
    for unit in manifest.unit_identities.values_mut().flatten() {
        unit.parameter_id = enhance_pir::protocol::unit_parameter_id(unit.allocated_rows).unwrap();
    }
    manifest.validate().unwrap();
    PendingCommit {
        enqueued_at: crate::control::notification_time(),
        replica: format!("peer-{replica}"),
        operation: format!("publication-{generation}"),
        attempt: generation,
        manifest,
        retained: vec![generation],
    }
}

fn enqueue(coordinator: &Coordinator, pending: PendingCommit) {
    coordinator
        .store
        .lock()
        .unwrap()
        .update(|state| {
            state.pending_commits.push(pending);
            Ok(())
        })
        .unwrap();
}

#[tokio::test]
async fn committed_notification_survives_reopening_and_later_peer_decisions() {
    let root = tempfile::tempdir().unwrap();
    let peers = [Peer::start().await, Peer::start().await];
    let inventory = groups(&peers);
    let coordinator = Coordinator::open(root.path(), inventory.clone()).unwrap();
    let delayed = notification(0, 1);
    for (index, peer) in peers.iter().enumerate() {
        peer.candidate(1);
        enqueue(&coordinator, notification(index, 1));
    }
    peers[0].fault(Fault::RejectCommit);
    coordinator.deliver_decisions().await.unwrap();
    assert_eq!(
        coordinator
            .store
            .lock()
            .unwrap()
            .state()
            .pending_commits
            .as_slice(),
        std::slice::from_ref(&delayed)
    );
    assert_eq!(peers[1].state.lock().unwrap().commits.len(), 1);
    drop(coordinator);
    let coordinator = Coordinator::open(root.path(), inventory).unwrap();
    peers[0].fault(Fault::Offline);
    // The pending decision remains deliverable even after its generation is no
    // longer in the serving retention set. Only that worker remains excluded.
    assert!(coordinator
        .store
        .lock()
        .unwrap()
        .state()
        .published
        .is_empty());
    for generation in 2..=7 {
        peers[1].candidate(generation);
        enqueue(&coordinator, notification(1, generation));
        coordinator.deliver_decisions().await.unwrap();
        coordinator
            .store
            .lock()
            .unwrap()
            .update(|state| {
                // The recovery component needs the retained manifest identities,
                // not their publication artifacts, to synchronize worker retention.
                state.published = vec![notification(1, generation).manifest];
                Ok(())
            })
            .unwrap();
        coordinator.synchronize_retention().await;
        assert!(
            peers[0].state.lock().unwrap().retention.is_empty(),
            "pending decisions keep their old retention"
        );
        assert_eq!(
            coordinator
                .store
                .lock()
                .unwrap()
                .state()
                .pending_commits
                .as_slice(),
            std::slice::from_ref(&delayed)
        );
        assert_eq!(coordinator.pending_replicas(), ["peer-0".into()].into());
    }
    peers[0].fault(Fault::None);
    coordinator.deliver_decisions().await.unwrap();
    assert!(coordinator.pending_replicas().is_empty());
    {
        let state = peers[0].state.lock().unwrap();
        assert_eq!(state.commits.len(), 1);
        assert_eq!(state.commits[0].manifest_digest, digest(&delayed.manifest));
        assert_eq!(state.commits[0].retained, delayed.retained);
        assert_eq!(
            state.commits[0].epoch,
            coordinator.store.lock().unwrap().state().epoch
        );
    }
    coordinator.synchronize_retention().await;
    assert!(
        peers[0].state.lock().unwrap().published.is_empty(),
        "the old decision applies before its generation expires"
    );
    assert_eq!(peers[0].state.lock().unwrap().retention[0].generations, [7]);
}

#[tokio::test]
async fn a_lost_commit_reply_requires_the_exact_published_manifest() {
    let root = tempfile::tempdir().unwrap();
    let peers = [Peer::start().await, Peer::start().await];
    let inventory = groups(&peers);
    let coordinator = Coordinator::open(root.path(), inventory.clone()).unwrap();
    peers[0].candidate(1);
    enqueue(&coordinator, notification(0, 1));
    peers[0].fault(Fault::LoseCommitReply);
    coordinator.deliver_decisions().await.unwrap();
    assert_eq!(coordinator.pending_replicas(), ["peer-0".into()].into());
    drop(coordinator);
    let coordinator = Coordinator::open(root.path(), inventory).unwrap();
    peers[0].fault(Fault::WrongDigest);
    coordinator.deliver_decisions().await.unwrap();
    assert!(!coordinator.pending_replicas().is_empty());
    peers[0].fault(Fault::None);
    coordinator.deliver_decisions().await.unwrap();
    assert!(coordinator.pending_replicas().is_empty());
    assert_eq!(
        peers[0].state.lock().unwrap().commits.len(),
        1,
        "acknowledged commits are not replayed"
    );
}

#[tokio::test]
async fn a_missing_or_conflicting_candidate_never_receives_a_commit() {
    let root = tempfile::tempdir().unwrap();
    let peers = [Peer::start().await, Peer::start().await];
    let coordinator = Coordinator::open(root.path(), groups(&peers)).unwrap();
    enqueue(&coordinator, notification(0, 1));
    for mismatch in 0..4 {
        peers[0].candidate(1);
        {
            let mut state = peers[0].state.lock().unwrap();
            match mismatch {
                0 => state.candidate = None,
                1 => state.candidate.as_mut().unwrap().operation = "other".into(),
                2 => state.candidate.as_mut().unwrap().attempt += 1,
                _ => state.candidate.as_mut().unwrap().generation += 1,
            }
        }
        coordinator.deliver_decisions().await.unwrap();
        assert!(!coordinator.pending_replicas().is_empty());
        assert!(peers[0].state.lock().unwrap().prepared.is_empty());
        assert!(peers[0].state.lock().unwrap().commits.is_empty());
    }
    peers[0].candidate(1);
    coordinator.deliver_decisions().await.unwrap();
    assert!(coordinator.pending_replicas().is_empty());
}

fn cancel_attempt(coordinator: &Coordinator, id: &str) {
    let mut store = coordinator.store.lock().unwrap();
    let operation = crate::control::Operation {
        pool_assignments: None,
        id: id.into(),
        attempt: store.state().next_attempt,
        epoch: store.state().epoch,
        expected_revision: store.state().revision,
        candidate_digest: "00".repeat(32),
        phase: Phase::Planned,
        assignments: BTreeMap::new(),
        affected_groups: ["group".into()].into(),
        source_generations: vec![],
        require_both: BTreeSet::new(),
        // No readiness reply: a reservation may have applied before the reply
        // was lost, so cancellation must still notify every participant.
        readiness: vec![],
        infrastructure_resources: BTreeMap::new(),
    };
    store.plan(operation).unwrap();
    store.abort_operation().unwrap();
}

#[tokio::test]
async fn ambiguous_aborts_survive_reopening_and_cannot_be_replaced_by_later_cancellation() {
    let root = tempfile::tempdir().unwrap();
    let peers = [Peer::start().await, Peer::start().await];
    let inventory = groups(&peers);
    let coordinator = Coordinator::open(root.path(), inventory.clone()).unwrap();
    peers[0].fault(Fault::RejectAbort);
    cancel_attempt(&coordinator, "first");
    assert_eq!(coordinator.pending_replicas().len(), 2);
    coordinator.deliver_decisions().await.unwrap();
    let pending = coordinator
        .store
        .lock()
        .unwrap()
        .state()
        .pending_aborts
        .clone();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].replica, "peer-0");
    drop(coordinator);
    let coordinator = Coordinator::open(root.path(), inventory).unwrap();
    peers[0].fault(Fault::Offline);
    cancel_attempt(&coordinator, "later");
    coordinator.deliver_decisions().await.unwrap();
    assert_eq!(
        coordinator.store.lock().unwrap().state().pending_aborts,
        pending
    );
    assert_eq!(
        coordinator.store.lock().unwrap().state().next_generation,
        1,
        "aborts do not publish"
    );
    peers[0].fault(Fault::LoseAbortReply);
    coordinator.deliver_decisions().await.unwrap();
    assert_eq!(
        coordinator.store.lock().unwrap().state().pending_aborts,
        pending
    );
    peers[0].fault(Fault::None);
    coordinator.deliver_decisions().await.unwrap();
    assert!(coordinator.pending_replicas().is_empty());
    let state = peers[0].state.lock().unwrap();
    assert_eq!(state.aborts.len(), 2);
    for abort in &state.aborts {
        assert_eq!(abort.operation, pending[0].operation);
        assert_eq!(abort.attempt, pending[0].attempt);
    }
}
