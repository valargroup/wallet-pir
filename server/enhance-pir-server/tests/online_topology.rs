use enhance_pir::client::{record_in_row, QuerySession};
use enhance_pir_server::{
    coordinator::{Anchor, CoordinatorState, TableJournal, TableSetup},
    store::RecordJournal,
    topology::{Append, Group, Replica, TopologyStore},
    types::{
        DatabaseId, EnhanceRecord, EnhanceRecordParts, ENHANCE_LAYOUT, SHARDS_PER_GROUP,
        SHARD_POSITIONS,
    },
    worker::{router, WorkerState},
};
use std::sync::Arc;

fn record(tag: u8) -> EnhanceRecord {
    EnhanceRecord::from_parts(EnhanceRecordParts {
        ephemeral_key: [tag; 32],
        enc_ciphertext: [tag; 580],
        cv_net: [tag; 32],
        out_ciphertext: [tag; 80],
        has_transparent_inputs: false,
        has_transparent_outputs: false,
    })
}

async fn fetch(state: &CoordinatorState, session: &QuerySession, position: u64) -> EnhanceRecord {
    let (query, slot) = session.prepare_position(position).unwrap();
    let answer = state
        .answer_query(DatabaseId::Enhance, query.body())
        .await
        .unwrap();
    record_in_row(&session.decode(query, &answer).unwrap(), slot)
}

#[tokio::test]
async fn online_append_and_aborted_candidates_preserve_retained_queries() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_test_writer()
        .try_init();
    let directory = tempfile::tempdir().unwrap();
    let mut servers = Vec::new();
    let mut groups = Vec::new();
    for group in 0..2 {
        let mut replicas = Vec::new();
        for replica in 0..2 {
            let name = format!("worker-{group}-{replica}");
            let worker = WorkerState::new(directory.path().join(&name)).unwrap();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            replicas.push(Replica {
                name,
                url: format!("http://{}", listener.local_addr().unwrap()),
            });
            servers.push(tokio::spawn(async move {
                axum::serve(listener, router(worker)).await.unwrap();
            }));
        }
        groups.push(Group {
            name: format!("group-{group}"),
            replicas,
        });
    }
    let topology = Arc::new(
        TopologyStore::open(
            directory.path().join("topology.json"),
            vec![groups[0].clone()],
        )
        .unwrap(),
    );
    let state = CoordinatorState::new(vec![TableSetup {
        table: DatabaseId::Enhance,
        groups: vec![groups[0].target()],
    }])
    .unwrap()
    .with_topology(topology.clone());
    let mut journal = RecordJournal::open(
        directory.path().join("journal"),
        DatabaseId::Enhance,
        ENHANCE_LAYOUT,
    )
    .unwrap();
    journal
        .append_block(1, format!("{:064x}", 1), &[record(1)])
        .unwrap();
    state
        .publish_from_store(&journal, 1, format!("{:064x}", 1))
        .await
        .unwrap();
    let old = QuerySession::from_session(state.session().unwrap()).unwrap();
    topology
        .append(Append {
            operation_id: "append".into(),
            expected_revision: 0,
            groups: groups.clone(),
        })
        .unwrap();
    state
        .publish_from_store(&journal, 1, format!("{:064x}", 1))
        .await
        .unwrap();
    assert_eq!(topology.status().revision, 1);
    assert_eq!(fetch(&state, &old, 0).await, record(1));
    let observations = state.observe().await;
    assert_eq!(observations.worker_groups.len(), 2);
    assert!(observations
        .worker_groups
        .iter()
        .all(|g| g.ready_replicas == 2));
    // Fill the coordinator's retention window, then repeatedly reject fully
    // prepared candidates. None may evict the oldest published worker snapshot.
    for height in 2..=7 {
        journal
            .append_block(height, format!("{height:064x}"), &[record(height as u8)])
            .unwrap();
        state
            .publish_from_store(&journal, height, format!("{height:064x}"))
            .await
            .unwrap();
    }
    for height in 8..=17 {
        journal
            .append_block(height, format!("{height:064x}"), &[record(height as u8)])
            .unwrap();
        let source = TableJournal::new(DatabaseId::Enhance, &journal).unwrap();
        let error = state
            .publish_checked(
                &[&source],
                Anchor {
                    height,
                    hash: format!("{height:064x}"),
                },
                || async { Err("reorg".into()) },
            )
            .await
            .unwrap_err();
        assert_eq!(error, "reorg");
        assert_eq!(fetch(&state, &old, 0).await, record(1));
    }
    // Losing one original replica must not affect the generation's query route.
    servers[0].abort();
    assert_eq!(fetch(&state, &old, 0).await, record(1));
    // The rejection case above has its own retained window. Release it before
    // the independent full-range case so CI need not host both working sets.
    // Full-capacity retention under load belongs to the c-4 qualification run.
    drop(old);
    drop(state);
    for server in servers.drain(..) {
        server.abort();
        let _ = server.await;
    }
    for (group_index, group) in groups.iter().enumerate() {
        for (replica_index, replica) in group.replicas.iter().enumerate() {
            if group_index == 0 && replica_index == 0 {
                continue; // Keep the original peer offline for this case too.
            }
            let worker = WorkerState::new(directory.path().join(&replica.name)).unwrap();
            let listener =
                tokio::net::TcpListener::bind(replica.url.strip_prefix("http://").unwrap())
                    .await
                    .unwrap();
            servers.push(tokio::spawn(async move {
                axum::serve(listener, router(worker)).await.unwrap();
            }));
        }
    }
    let state = CoordinatorState::new(vec![TableSetup {
        table: DatabaseId::Enhance,
        groups: groups.iter().map(Group::target).collect(),
    }])
    .unwrap()
    .with_topology(topology);
    // Cross the first group boundary and the power-of-two query-domain
    // boundary together. The newly used group must contribute its partial,
    // while a session opened before the transition retains its original domain.
    let boundary = SHARDS_PER_GROUP as usize * SHARD_POSITIONS;
    journal
        .append_block(18, format!("{:064x}", 18), &vec![record(18); boundary - 17])
        .unwrap();
    state
        .publish_from_store(&journal, 18, format!("{:064x}", 18))
        .await
        .unwrap();
    let before_boundary = QuerySession::from_session(state.session().unwrap()).unwrap();
    assert_eq!(
        fetch(&state, &before_boundary, (boundary - 1) as u64).await,
        record(18)
    );
    journal
        .append_block(19, format!("{:064x}", 19), &[record(19)])
        .unwrap();
    state
        .publish_from_store(&journal, 19, format!("{:064x}", 19))
        .await
        .unwrap();
    let after_boundary = QuerySession::from_session(state.session().unwrap()).unwrap();
    assert_eq!(
        fetch(&state, &before_boundary, (boundary - 1) as u64).await,
        record(18)
    );
    assert_eq!(
        fetch(&state, &after_boundary, (boundary - 1) as u64).await,
        record(18)
    );
    assert_eq!(
        fetch(&state, &after_boundary, boundary as u64).await,
        record(19)
    );
    for server in servers {
        server.abort();
    }
}
