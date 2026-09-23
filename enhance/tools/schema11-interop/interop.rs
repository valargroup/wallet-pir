//! Build as a separate test crate against the pinned wallet-pir checkout.
//! Queries traverse the real coordinator and two worker HTTP listeners.
use enhance_pir_server::{
    store::RecordJournal,
    types::{DatabaseId, ENHANCE_LAYOUT},
    v4::{
        control::{Group, Ledger, Replica},
        coordinator::Coordinator,
        worker::Worker,
    },
};
use futures_util::StreamExt;
use zakura_pir_enhance::{
    transport::{Client, Method, PendingClient, Request, ResponseBody, Transport},
    AcceptedAnchor, ClientError, ClientResourceLimits, EnhanceRecord, GenerationAcceptance,
    RECORD_BYTES,
};

struct LoopbackTransport(reqwest::Client, std::sync::atomic::AtomicUsize);

impl LoopbackTransport {
    fn new() -> Self {
        Self(
            reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(60))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap(),
            std::sync::atomic::AtomicUsize::new(0),
        )
    }
}

impl Transport for LoopbackTransport {
    async fn execute(&self, request: Request) -> Result<ResponseBody, ClientError> {
        if matches!(request.method, Method::Post) {
            self.1.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        let mut body = request.response_body();
        let method = match request.method {
            Method::Get => reqwest::Method::GET,
            Method::Post => reqwest::Method::POST,
        };
        let mut response = self
            .0
            .request(method, request.url)
            .body(request.body)
            .send()
            .await
            .map_err(|e| ClientError::Transport(e.to_string()))?;
        if !response.status().is_success() {
            return Err(ClientError::HttpStatus(response.status().as_u16()));
        }
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|e| ClientError::Transport(e.to_string()))?
        {
            body.extend(&chunk)?;
        }
        Ok(body.finish())
    }
}

async fn query_position(
    client: &mut Client,
    transport: &LoopbackTransport,
    position: u64,
) -> Result<EnhanceRecord, ClientError> {
    let stream = client.query_batch(transport, [position])?;
    futures_util::pin_mut!(stream);
    stream.next().await.expect("one result").record
}

async fn serve(router: axum::Router) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (url, task)
}

fn record(position: u64) -> Vec<u8> {
    let mut bytes = vec![0; RECORD_BYTES];
    bytes[..8].copy_from_slice(&position.to_le_bytes());
    bytes
}

fn acceptance(height: u64, hash: u8, records: u64) -> GenerationAcceptance {
    GenerationAcceptance::new(
        "main",
        3_428_143,
        AcceptedAnchor::new(height, [hash; 32], records),
        ClientResourceLimits::with_cache(4_096, 1),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wallet_client_round_trips_real_v4_http_and_requires_fresh_acceptance() {
    let root = tempfile::tempdir().unwrap();
    let mut tasks = Vec::new();
    let mut replicas = Vec::new();
    for index in 0..2 {
        let worker = Worker::open(&root.path().join(format!("worker-{index}"))).unwrap();
        let (url, task) = serve(worker.router()).await;
        tasks.push(task);
        replicas.push(Replica {
            name: format!("replica-{index}"),
            url,
            incarnation: String::new(),
            ledger: Ledger::default(),
        });
    }
    let coordinator = Coordinator::open(
        &root.path().join("control"),
        vec![Group {
            id: "group-1".into(),
            sequence: 0,
            replicas,
            settling: false,
        }],
    )
    .unwrap();
    let (origin, task) = serve(coordinator.clone().router()).await;
    tasks.push(task);
    let mut journal = RecordJournal::open(
        root.path().join("journal"),
        DatabaseId::Enhance,
        ENHANCE_LAYOUT,
    )
    .unwrap();
    journal
        .append_block(
            3_428_143,
            "01".repeat(32),
            &(0..67).map(record).collect::<Vec<_>>(),
        )
        .unwrap();
    coordinator
        .publish(&journal, 3_428_143, "01".repeat(32))
        .await
        .unwrap();

    let transport = LoopbackTransport::new();
    let pending = PendingClient::fetch(&transport, &origin).await.unwrap();
    assert_eq!(pending.manifest().coverage.records, 67);
    assert!(pending.accept(&acceptance(3_428_143, 2, 67)).is_err());
    let mut client = PendingClient::fetch(&transport, &origin)
        .await
        .unwrap()
        .accept(&acceptance(3_428_143, 1, 67))
        .unwrap();
    for position in [0, 32, 33, 65, 66, 33] {
        let answer = query_position(&mut client, &transport, position)
            .await
            .unwrap();
        assert_eq!(
            answer.as_bytes(),
            record(position).as_slice(),
            "position {position}"
        );
    }
    assert!(query_position(&mut client, &transport, 67).await.is_err());

    // The sixth publication expires the first generation. A newly fetched
    // manifest still needs a new locally scanned anchor before its setup is used.
    for offset in 1..=5u64 {
        let position = journal.tree_size();
        let height = 3_428_143 + offset;
        let hash = format!("{offset:02x}").repeat(32);
        journal
            .append_block(height, hash.clone(), &[record(position)])
            .unwrap();
        coordinator.publish(&journal, height, hash).await.unwrap();
    }
    let stale = query_position(&mut client, &transport, 0)
        .await
        .unwrap_err();
    assert_eq!(stale.http_status(), Some(410));
    let pending = PendingClient::fetch(&transport, &origin).await.unwrap();
    assert!(pending.accept(&acceptance(3_428_143, 1, 67)).is_err());
    let mut refreshed = PendingClient::fetch(&transport, &origin)
        .await
        .unwrap()
        .accept(&acceptance(3_428_148, 5, 72))
        .unwrap();
    assert_eq!(
        query_position(&mut refreshed, &transport, 71)
            .await
            .unwrap()
            .as_bytes(),
        record(71).as_slice()
    );
    for task in tasks {
        task.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scanned_wallet_applies_one_real_pir_row_atomically() {
    use orchard::{
        keys::{PreparedIncomingViewingKey, Scope},
        note_encryption::{CompactAction, IronwoodDomain, IronwoodNoteEncryption},
    };
    use zcash_client_backend::data_api::{
        chain::BlockSource,
        enhance_pir::{
            EnhancePirBatchResult, EnhancePirRead, EnhancePirStoreResult, EnhancePirWork,
            EnhancePirWrite, EnhancementMode,
        },
        testing::{AddressType, FakeCompactOutput, IronwoodFvk, TestBuilder},
    };
    use zcash_client_sqlite::testing::{db::TestDbFactory, BlockCache};
    use zcash_note_encryption::try_compact_note_decryption;
    use zcash_primitives::block::BlockHash;
    use zcash_protocol::{consensus::BlockHeight, local_consensus::LocalNetwork, value::Zatoshis};

    let activation = BlockHeight::from_u32(100_000);
    let mut wallet = TestBuilder::new()
        .with_network(LocalNetwork {
            nu6: Some(activation),
            nu6_1: Some(activation),
            nu6_2: Some(activation),
            nu6_3: Some(activation),
            ..TestBuilder::<(), ()>::DEFAULT_NETWORK
        })
        .with_data_store_factory(TestDbFactory::default())
        .with_block_cache(BlockCache::new())
        .with_account_from_sapling_activation(BlockHash([0; 32]))
        .build();
    let (height, _) = wallet.generate_empty_block();
    wallet.scan_cached_blocks(height, 1);
    let fvk = IronwoodFvk(wallet.test_account_orchard().unwrap().clone());
    let (height, _, _) = wallet.generate_next_block_multi(&[
        FakeCompactOutput::new(
            &fvk,
            AddressType::DefaultExternal,
            Zatoshis::const_from_u64(10_000),
        ),
        FakeCompactOutput::new(
            &fvk,
            AddressType::DefaultExternal,
            Zatoshis::const_from_u64(20_000),
        ),
    ]);
    wallet.scan_cached_blocks(height, 1);
    wallet
        .wallet_mut()
        .db_mut()
        .set_enhancement_mode(EnhancementMode::PrivateIronwood);
    let requests: Vec<_> = wallet
        .wallet()
        .db()
        .enhance_pir_work()
        .unwrap()
        .into_iter()
        .filter_map(|w| match w {
            EnhancePirWork::Query(r) => Some(r),
            _ => None,
        })
        .collect();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[0].request_id().txid(),
        requests[1].request_id().txid()
    );
    let mut compact = None;
    wallet
        .cache()
        .with_blocks::<_, ()>(Some(height), Some(1), |block| {
            compact = Some(block);
            Ok(())
        })
        .unwrap();
    let compact = compact.unwrap();
    let ivk = PreparedIncomingViewingKey::new(&fvk.0.to_ivk(Scope::External));
    let records: Vec<_> = compact.vtx[0]
        .ironwood_actions
        .iter()
        .map(|action| {
            let action = CompactAction::try_from(action).unwrap();
            let domain = IronwoodDomain::for_compact_action(&action);
            let (note, _) = try_compact_note_decryption(&domain, &ivk, &action).unwrap();
            let ciphertext =
                IronwoodNoteEncryption::new(None, note, [7; 512]).encrypt_note_plaintext();
            enhance_pir::EnhanceRecord::from_parts(enhance_pir::EnhanceRecordParts {
                enc_ciphertext_suffix: ciphertext[52..].try_into().unwrap(),
                cv_net: [0; 32],
                out_ciphertext: [0; 80],
                has_transparent_inputs: false,
                has_transparent_outputs: false,
                metadata: enhance_pir::EnhanceTransactionMetadata::new(0, Some(0)).unwrap(),
            })
        })
        .collect();
    let root = tempfile::tempdir().unwrap();
    let mut tasks = Vec::new();
    let mut replicas = Vec::new();
    for i in 0..2 {
        let (url, task) = serve(
            Worker::open(&root.path().join(format!("worker-{i}")))
                .unwrap()
                .router(),
        )
        .await;
        tasks.push(task);
        replicas.push(Replica {
            name: format!("r{i}"),
            url,
            incarnation: String::new(),
            ledger: Ledger::default(),
        });
    }
    let coordinator = Coordinator::open(
        &root.path().join("control"),
        vec![Group {
            id: "g0".into(),
            sequence: 0,
            replicas,
            settling: false,
        }],
    )
    .unwrap();
    let (origin, task) = serve(coordinator.clone().router()).await;
    tasks.push(task);
    let mut journal = RecordJournal::open(
        root.path().join("journal"),
        DatabaseId::Enhance,
        ENHANCE_LAYOUT,
    )
    .unwrap();
    let mut rows = vec![vec![0; RECORD_BYTES]; 2];
    for (request, record) in requests.iter().zip(&records) {
        rows[u64::from(request.position()) as usize] = record.as_bytes().to_vec();
    }
    let mut display_hash = compact.hash.clone();
    display_hash.reverse();
    let hash = hex::encode(&display_hash);
    journal
        .append_block(u32::from(height) as u64, hash.clone(), &rows)
        .unwrap();
    coordinator
        .publish(&journal, u32::from(height) as u64, hash)
        .await
        .unwrap();
    let transport = LoopbackTransport::new();
    let pending = PendingClient::fetch(&transport, &origin).await.unwrap();
    // This synthetic local chain uses the same mainnet wire contract. Bind to
    // the actual wallet-scanned anchor, rather than trusting the server's claim.
    let anchor = zakura_pir_enhance::wallet::snapshot_anchor(pending.manifest()).unwrap();
    assert_eq!(
        wallet
            .wallet()
            .db()
            .enhance_pir_snapshot_status(anchor)
            .unwrap(),
        zcash_client_backend::data_api::enhance_pir::EnhancePirSnapshotStatus::Accepted
    );
    let accepted = GenerationAcceptance::new(
        "main",
        u32::from(activation) as u64,
        AcceptedAnchor::new(
            u32::from(height) as u64,
            display_hash.try_into().unwrap(),
            2,
        ),
        ClientResourceLimits::new(4096),
    );
    let mut client = pending.accept(&accepted).unwrap();
    let batch = [requests[1], requests[0], requests[1]];
    let result = client.query_row_requests(&transport, &batch).await.unwrap();
    assert_eq!(transport.1.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(
        result.slots.iter().map(|(r, _)| *r).collect::<Vec<_>>(),
        batch
    );
    for (request, record) in &result.slots {
        assert_eq!(
            record.as_bytes(),
            records[u64::from(request.position()) as usize].as_bytes()
        );
    }
    // A corrupted second live record must not commit the first note's memo.
    let mut bad = result.slots.clone();
    let mut corrupted = *bad[1].1.as_bytes();
    corrupted[100] ^= 1;
    bad[1].1 = EnhanceRecord::from_bytes(corrupted).unwrap();
    assert!(matches!(
        wallet
            .wallet_mut()
            .db_mut()
            .apply_ironwood_enhance_records(&bad)
            .unwrap(),
        EnhancePirBatchResult::Rejected { .. }
    ));
    let remaining = wallet.wallet().db().enhance_pir_work().unwrap();
    assert_eq!(remaining.len(), 2);
    assert_eq!(
        wallet
            .wallet_mut()
            .db_mut()
            .apply_ironwood_enhance_records(&result.slots)
            .unwrap(),
        EnhancePirBatchResult::Committed(vec![EnhancePirStoreResult::Stored; 3])
    );
    assert!(wallet.wallet().db().enhance_pir_work().unwrap().is_empty());
    for task in tasks {
        task.abort();
    }
}
