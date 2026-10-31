use super::*;
use axum::{extract::State as HttpState, routing::post, Json, Router};
#[cfg(feature = "qualification-tests")]
use receiver_pir_server::publication::DEFAULT_RETENTION;
#[cfg(feature = "qualification-tests")]
use receiver_pir_server::Publication;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::sync::{Notify, Semaphore};
use zakura_pir_publisher::receiver::{self, Segment as Tables};

/// A node's tip, block hashes by height and the time in every block header. Each
/// block hash request runs `hook` if there is one, signals `asked`, then waits for a
/// permit from `gate` if there is one. A genesis read then applies `repoint`, if
/// set: the next genesis, or none, so later reads fail.
#[derive(Clone, Default)]
struct Node {
    tip: Arc<Mutex<u64>>,
    hashes: Arc<Mutex<HashMap<u64, [u8; 32]>>>,
    time: Arc<Mutex<i64>>,
    repoint: Arc<Mutex<Option<Option<[u8; 32]>>>>,
    hook: Option<Arc<dyn Fn() + Send + Sync>>,
    gate: Option<Arc<Semaphore>>,
    asked: Arc<Notify>,
}

/// Serves `node` over JSON-RPC and returns a client for it.
async fn serve_node(node: Node) -> NodeClient {
    async fn handler(HttpState(node): HttpState<Node>, Json(r): Json<Value>) -> Json<Value> {
        let result = match r["method"].as_str().unwrap() {
            "getblockcount" => json!(*node.tip.lock().unwrap()),
            "getblockhash" => {
                if let Some(hook) = &node.hook {
                    hook();
                }
                node.asked.notify_one();
                if let Some(gate) = &node.gate {
                    gate.acquire().await.unwrap().forget();
                }
                let height = r["params"][0].as_u64().unwrap();
                let mut hashes = node.hashes.lock().unwrap();
                let Some(hash) = hashes.get(&height).copied() else {
                    return Json(
                        json!({"result": null, "error": {"code": -8, "message": "no block"}}),
                    );
                };
                match node.repoint.lock().unwrap().take_if(|_| height == 0) {
                    Some(Some(genesis)) => hashes.insert(0, genesis),
                    Some(None) => hashes.remove(&0),
                    None => None,
                };
                json!(zakura_server_chain::hash::display(&hash))
            }
            "getblockheader" => match *node.time.lock().unwrap() {
                // A node that cannot give the header.
                i64::MIN => {
                    return Json(json!({"result": null,
                    "error": {"code": -8, "message": "header unavailable"}}))
                }
                time => json!({"time": time}),
            },
            "getblock" => {
                return Json(json!({"result": null,
                "error": {"code": -8, "message": "temporarily unavailable"}}))
            }
            method => panic!("unexpected {method}"),
        };
        Json(json!({"result": result, "error": null}))
    }
    let app = Router::new().route("/", post(handler)).with_state(node);
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", socket.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
    NodeClient::new(url, None).unwrap()
}

/// An empty one-shard inventory ending at block `[hash; 32]`, height 101.
#[cfg(feature = "qualification-tests")]
fn publication(hash: u8) -> Publication {
    let segment = crate::prepared::segment(crate::prepared::empty(hash)).unwrap();
    Publication::assemble([1; 32], 0, vec![(false, vec![segment], None)]).unwrap()
}

/// A check that finds an anchor off the chain after a revocation and a new
/// publication replaced it leaves the new one serving; a served anchor off the
/// chain is revoked. Fast already checks that refusal on the serving runtime;
/// this repeats it with two prepared publications.
#[cfg(feature = "qualification-tests")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stale_failure_does_not_revoke_a_newer_publication() {
    let gate = Arc::new(Semaphore::new(0));
    let node = Node {
        gate: Some(gate.clone()),
        ..Node::default()
    };
    let mainnet = Network::Mainnet.genesis_hash();
    *node.tip.lock().unwrap() = 200;
    node.hashes
        .lock()
        .unwrap()
        .extend([(0, mainnet), (101, [2; 32])]);
    let rpc = serve_node(node.clone()).await;
    let publications = Publications::default();
    assert!(publications.publish(publication(1), 0));
    let check = tokio::spawn({
        let (publications, rpc) = (publications.clone(), rpc.clone());
        async move {
            check_serving(&publications, &rpc, mainnet, 200)
                .await
                .unwrap()
        }
    });
    // While the check of A waits for the node, a rewind revokes A and B is published.
    node.asked.notified().await;
    publications.revoke();
    assert!(publications.publish(publication(2), 1));
    // A's hash, then the genesis that confirms the node's network.
    gate.add_permits(2);
    check.await.unwrap();
    assert_eq!(publications.anchors(), [(101, [2; 32])]);
    gate.add_permits(10);
    check_serving(&publications, &rpc, mainnet, 200)
        .await
        .unwrap();
    assert_eq!(publications.anchors(), [(101, [2; 32])]);
    node.hashes.lock().unwrap().insert(101, [3; 32]);
    check_serving(&publications, &rpc, mainnet, 200)
        .await
        .unwrap();
    assert!(publications.anchors().is_empty());
}

/// The report time the serving tests start from, in Unix seconds.
const NOW: i64 = 1_000_000_000;

/// A serving indexer whose index and node both stop at Ironwood activation.
struct Paused {
    dir: tempfile::TempDir,
    /// The seal target every pass's state uses; see [`Paused::fresh_state`].
    target: u32,
    args: Config,
    rpc: NodeClient,
    /// The node `rpc` serves, its block headers dated [`NOW`] at first.
    node: Node,
    #[cfg(feature = "qualification-tests")]
    publications: Publications,
    #[cfg(feature = "qualification-tests")]
    origin: String,
    #[cfg(feature = "qualification-tests")]
    state: State,
}

impl Paused {
    /// Indexes the activation block and serves it. Each block hash request first
    /// runs `on_hash` on the provider store, as a concurrent feed read would.
    async fn new(on_hash: impl Fn(&mut PayoutStore) + Send + Sync + 'static) -> Self {
        Self::serving(on_hash, Publications::default()).await
    }

    /// [`Self::new`] serving through `publications`.
    async fn serving(
        on_hash: impl Fn(&mut PayoutStore) + Send + Sync + 'static,
        publications: Publications,
    ) -> Self {
        let height = u64::from(crate::blocks::ironwood_activation());
        let dir = tempfile::tempdir().unwrap();
        let provider = dir.path().join(PROVIDERS);
        let node = Node {
            hook: Some(Arc::new(move || {
                on_hash(&mut PayoutStore::open(&provider).unwrap())
            })),
            ..Node::default()
        };
        *node.tip.lock().unwrap() = height;
        node.hashes.lock().unwrap().extend([
            (0, Network::Mainnet.genesis_hash()),
            (height - 1, [2; 32]),
            (height, [3; 32]),
        ]);
        *node.time.lock().unwrap() = NOW;
        let rpc = serve_node(node.clone()).await;
        let mut store = Store::open(
            dir.path().join(receiver_store::STORE_FILE),
            StoreConfig {
                genesis: Network::Mainnet.genesis_hash(),
                start_height: height as u32,
                start_parent: [2; 32],
                start_position: 0,
            },
        )
        .unwrap();
        store
            .append(&receiver_store::IndexedBlock {
                height: height as u32,
                hash: [3; 32],
                parent: [2; 32],
                start_position: 0,
                end_position: 0,
                coinbase_actions: 0,
                payments: Vec::new(),
                commitments: Vec::new(),
            })
            .unwrap();
        let args = Config {
            data_dir: dir.path().to_owned(),
            start_height: height as u32,
            end_height: None,
            depth: 0,
            batch_size: crate::blocks::MAX_RECEIVER_BATCH_BLOCKS,
            concurrency: 8,
            audit_enabled: true,
            filter_blocks: None,
            history_revision: 0,
        };
        #[cfg(feature = "qualification-tests")]
        let origin = {
            let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let origin = format!("http://{}", socket.local_addr().unwrap());
            let app = crate::router::router_with_publications(publications.clone());
            tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
            origin
        };
        #[cfg(not(feature = "qualification-tests"))]
        drop(publications);
        Self {
            dir,
            target: crate::seal::Policy::default().target,
            args,
            rpc,
            node,
            #[cfg(feature = "qualification-tests")]
            publications,
            #[cfg(feature = "qualification-tests")]
            origin,
            #[cfg(feature = "qualification-tests")]
            state: State::default(),
        }
    }

    /// Restarts the service as a new process would: fresh serving state, table cache
    /// and listener, with the same budget, over the same data directory and node.
    #[cfg(feature = "qualification-tests")]
    async fn restart(&mut self) {
        self.publications =
            Publications::new(DEFAULT_RETENTION, self.publications.tables().budget());
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        self.origin = format!("http://{}", socket.local_addr().unwrap());
        let app = crate::router::router_with_publications(self.publications.clone());
        tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
        self.state = self.fresh_state();
    }

    /// Refreshes, as the serving loop does.
    #[cfg(feature = "qualification-tests")]
    async fn refresh(&mut self) -> Result<()> {
        let target = self.target;
        self.state.with_policy(|p| p.target = target);
        refresh(
            &self.args,
            std::slice::from_ref(&self.rpc),
            Network::Mainnet.genesis_hash(),
            Some(&self.publications),
            &mut self.state,
        )
        .await
    }

    /// The service's `/v1/receiver/{path}` response.
    #[cfg(feature = "qualification-tests")]
    async fn get(&self, path: &str) -> Value {
        let url = format!("{}/v1/receiver/{path}", self.origin);
        reqwest::get(url).await.unwrap().json().await.unwrap()
    }

    /// The current inventory's last shard's first segment, from its first chunk.
    #[cfg(feature = "qualification-tests")]
    async fn segment(&self) -> receiver::Publication {
        let head: zakura_pir_query_server::receiver::inventory::Head =
            serde_json::from_value(self.get("inventory").await).unwrap();
        let chunk = self
            .get(&format!("inventory/{}/0", hex::encode(head.inventory)))
            .await;
        let chunk: zakura_pir_query_server::receiver::inventory::Chunk =
            serde_json::from_value(chunk).unwrap();
        chunk.shards.last().unwrap().segments[0].clone()
    }

    /// A fresh pass state sealing at [`Self::target`].
    fn fresh_state(&self) -> State {
        let mut state = State::default();
        state.with_policy(|p| p.target = self.target);
        state
    }

    fn store(&self) -> Store {
        Store::open(
            self.dir.path().join(receiver_store::STORE_FILE),
            StoreConfig {
                genesis: Network::Mainnet.genesis_hash(),
                start_height: crate::blocks::ironwood_activation(),
                start_parent: [2; 32],
                start_position: 0,
            },
        )
        .unwrap()
    }

    /// The provider store.
    #[cfg(feature = "qualification-tests")]
    fn provider(&self) -> PayoutStore {
        PayoutStore::open(self.dir.path().join(PROVIDERS)).unwrap()
    }
}

impl Paused {
    /// Appends a validated block after the store's tip paying each of `paid` once,
    /// with `extra` further commitments, every commitment `variant`, and makes the
    /// node agree. Returns the block.
    fn extend(
        &mut self,
        paid: &[[u8; 43]],
        extra: usize,
        variant: u8,
    ) -> receiver_store::IndexedBlock {
        let mut store = self.store();
        let tip = store.tip().unwrap();
        let height = tip.height + 1;
        let mut hash = [variant; 32];
        hash[..4].copy_from_slice(&height.to_le_bytes());
        let cmx = [6; 32];
        let payments = paid
            .iter()
            .enumerate()
            .map(|(i, receiver)| {
                let mut payment = super::common::record(0, 1).payment;
                (payment.height, payment.block_hash, payment.cmx) = (height, hash, cmx);
                payment.position = tip.position + i as u64;
                payment.tx_index = i as u32 + 1;
                payment.txid = [variant; 32];
                payment.txid[..8].copy_from_slice(&payment.position.to_le_bytes());
                (*receiver, payment)
            })
            .collect();
        let count = paid.len() + extra;
        let mut commitments = vec![cmx; count];
        if let Some(first) = commitments.get_mut(paid.len()) {
            first[31] = variant & 0x3f;
        }
        let block = receiver_store::IndexedBlock {
            height,
            hash,
            parent: tip.hash,
            start_position: tip.position,
            end_position: tip.position + count as u64,
            coinbase_actions: 0,
            payments,
            commitments,
        };
        verify::append(&mut store, &block).unwrap();
        *self.node.tip.lock().unwrap() = u64::from(height);
        self.node
            .hashes
            .lock()
            .unwrap()
            .insert(u64::from(height), hash);
        block
    }

    /// Captures the served inventory as a wallet does: its head and shards, with the
    /// wallet that reads it, whose query count follows. With `filters` a lookup skips a
    /// sealed shard whose filter excludes its receiver.
    #[cfg(feature = "qualification-tests")]
    async fn capture(&self, filters: bool) -> (Captured, Arc<std::sync::atomic::AtomicUsize>) {
        let wallet = crate::lookup::Wallet::at(&self.origin, "/v1/receiver");
        let (head, shards) = wallet.inventory().await.unwrap();
        let tip = self.store().tip().unwrap();
        assert_eq!((head.end_height, head.end_hash), (tip.height, tip.hash));
        let queries = wallet.queries.clone();
        (
            Captured {
                wallet,
                head,
                shards,
                filters,
            },
            queries,
        )
    }
}

/// A captured inventory and the wallet that reads it.
#[cfg(feature = "qualification-tests")]
struct Captured {
    wallet: crate::lookup::Wallet,
    head: zakura_pir_query_server::receiver::inventory::Head,
    shards: Vec<zakura_pir_query_server::receiver::inventory::Shard>,
    filters: bool,
}

/// Every record of `receiver` in `client`'s inventory, with the shard and segment
/// holding it and its payload, skipping sealed shards whose filter excludes it as a
/// wallet may. A refusal is its status.
#[cfg(feature = "qualification-tests")]
async fn try_lookup(
    client: &Captured,
    receiver: [u8; 43],
) -> std::result::Result<
    Vec<(
        u32,
        u32,
        zakura_pir_publisher::receiver::RawRecord,
        [u8; 512],
    )>,
    reqwest::StatusCode,
> {
    let mut found = Vec::new();
    for shard in &client.shards {
        if let (true, Some(filter)) = (client.filters, &shard.filter) {
            let bytes = client
                .wallet
                .get(&format!("/shard-filters/{}", hex::encode(filter.digest)))
                .await?;
            let filter =
                zakura_pir_publisher::receiver::shards::ShardFilter::decode(&bytes).unwrap();
            if !filter.matches(&[receiver])[0] {
                continue;
            }
        }
        for (index, segment) in shard.segments.iter().enumerate() {
            for (record, payload) in client.wallet.lookup(segment, &receiver).await? {
                found.push((shard.index, index as u32, record, payload));
            }
        }
    }
    Ok(found)
}

/// [`try_lookup`], which must succeed.
#[cfg(feature = "qualification-tests")]
async fn lookup(
    client: &Captured,
    receiver: [u8; 43],
) -> Vec<(
    u32,
    u32,
    zakura_pir_publisher::receiver::RawRecord,
    [u8; 512],
)> {
    try_lookup(client, receiver).await.unwrap()
}

/// One segment of a layout a one-shot pass wrote: its directory and companion
/// manifests and rows.
struct Written {
    directory: receiver::Directory,
    companion: receiver::Companion,
    rows: Vec<u8>,
}

impl Written {
    /// Every record the companion holds, in slot order, with its payload.
    fn slots(&self) -> Vec<(zakura_pir_publisher::receiver::RawRecord, [u8; 512])> {
        (0..self.companion.payments)
            .map(|slot| {
                let at = (slot / receiver::ROW_SLOTS) as usize * receiver::ROW_BYTES
                    + (slot % receiver::ROW_SLOTS) as usize * receiver::SLOT_BYTES;
                receiver::decode_slot(&self.rows[at..at + receiver::SLOT_BYTES])
                    .unwrap()
                    .unwrap()
            })
            .collect()
    }

    /// The payment of `receiver`, which must be the segment's only one for it, with
    /// its companion payload.
    fn payment(&self, receiver: [u8; 43]) -> (zakura_pir_publisher::receiver::Payment, [u8; 512]) {
        let mut found: Vec<_> = self
            .slots()
            .into_iter()
            .filter(|s| s.0.receiver == receiver)
            .collect();
        assert_eq!(found.len(), 1);
        let (record, payload) = found.remove(0);
        (record.payment, payload)
    }

    /// Whether this segment wrote a payment to `receiver`.
    fn contains(&self, receiver: [u8; 43]) -> bool {
        self.slots().iter().any(|s| s.0.receiver == receiver)
    }
}

/// Runs a one-shot pass with `state` into an emptied publications directory, as an
/// indexer that writes files does, and returns every segment it wrote in layout
/// order. One-shot passes prepare no PIR tables.
async fn one_shot(paused: &Paused, state: &mut State) -> (Layout, Vec<Vec<Written>>) {
    let root = paused.dir.path().join("publications");
    let _ = std::fs::remove_dir_all(&root);
    let rpc = std::slice::from_ref(&paused.rpc);
    refresh(
        &paused.args,
        rpc,
        Network::Mainnet.genesis_hash(),
        None,
        state,
    )
    .await
    .unwrap();
    let read = |name: String| std::fs::read(root.join(name)).unwrap();
    let name = std::fs::read_dir(&root)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .find(|n| n.ends_with(".inventory.json"))
        .unwrap();
    let layout: Layout = serde_json::from_slice(&read(name)).unwrap();
    let shards = layout
        .shards
        .iter()
        .map(|shard| {
            shard
                .segments
                .iter()
                .map(|s| {
                    let (d, c) = (hex::encode(s.directory), hex::encode(s.companion));
                    Written {
                        directory: serde_json::from_slice(&read(format!("d-{d}.json"))).unwrap(),
                        companion: serde_json::from_slice(&read(format!("c-{c}.json"))).unwrap(),
                        rows: read(format!("c-{c}.rows")),
                    }
                })
                .collect()
        })
        .collect();
    (layout, shards)
}

/// The lower path of every position in a fresh rebuild from all `store`'s
/// commitments, the independent oracle for served payloads.
fn fresh(store: &Store, positions: &[u64]) -> Vec<[u8; 512]> {
    let size = store.tip().unwrap().position;
    let tree: Vec<_> = (0..size.div_ceil(1 << 16))
        .flat_map(|s| store.commitments(s << 16, size.min((s + 1) << 16)).unwrap())
        .collect();
    zakura_pir_publisher::receiver::witness::payloads(&tree, positions).unwrap()
}

/// Blocks that each reach the target seal into shards. A wallet's library client
/// skips a sealed shard its filter excludes and reads the rest over real encrypted
/// PIR, with each payment's companion payload equal to a fresh rebuild; the filters
/// serve by digest; a restart rebuilds the identical inventory; and a displaced
/// inventory keeps serving its chunks and sessions while sealed sessions carry over.
#[cfg(feature = "qualification-tests")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sealed_shards_serve_through_the_library_client() {
    use super::common::{receiver, seeded_receiver};
    let mut paused = Paused::new(|_| {}).await;
    paused.target = 1;
    let (one, two) = (receiver(), seeded_receiver(1));
    paused.extend(&[one], 2, 0);
    paused.extend(&[two], 0, 0);
    paused.extend(&[one], 1, 0);
    paused.extend(&[], 3, 0);
    paused.refresh().await.unwrap();
    let (client, queries) = paused.capture(true).await;
    let sealed: Vec<_> = client
        .shards
        .iter()
        .map(|s| (s.sealed, s.filter.is_some()))
        .collect();
    assert_eq!(
        sealed,
        [(true, true), (true, true), (true, true), (false, false)]
    );
    let found = lookup(&client, one).await;
    // Shard 1 holds only the other receiver, so its filter spares a query.
    assert_eq!(
        queries.load(std::sync::atomic::Ordering::SeqCst),
        5,
        "a directory and a companion query in shards 0 and 2, and the open shard's directory"
    );
    let positions: Vec<u64> = found.iter().map(|f| f.2.payment.position).collect();
    assert_eq!(
        found.iter().map(|f| f.0).collect::<Vec<_>>(),
        [0, 2],
        "{positions:?}"
    );
    let store = paused.store();
    let expected = fresh(&store, &positions);
    for ((_, _, _, payload), expected) in found.iter().zip(expected) {
        assert_eq!(*payload, expected);
    }
    let head = client.head.clone();
    // A restart rebuilds the identical inventory from the store.
    paused.state = paused.fresh_state();
    paused.refresh().await.unwrap();
    let (again, _) = paused.capture(true).await;
    assert_eq!(&again.head, &head);
    // A new tip replaces the recent tier; the sealed shards keep their sessions and
    // the displaced inventory still serves its chunk.
    paused.extend(&[two], 0, 0);
    paused.refresh().await.unwrap();
    let (next, _) = paused.capture(true).await;
    assert_ne!(next.head.inventory, head.inventory);
    let sessions = |c: &Captured, i: usize| c.shards[i].segments[0].directory.id().unwrap();
    assert_eq!(sessions(&next, 0), sessions(&client, 0));
    // Both inventories and the kept shards share each sealed filter's one source.
    let live = paused.publications.live();
    assert_eq!(live.len(), 2);
    let mut shared = 0;
    for kept in &paused.state.kept {
        for publication in &live {
            if let Some(source) = publication.filter(&kept.filter.digest()) {
                assert!(Arc::ptr_eq(source, &kept.filter));
                shared += 1;
            }
        }
    }
    assert_eq!(
        shared,
        3 * 2 + 1,
        "three shards in both, the new seal in one"
    );
    let chunk = format!(
        "{}/v1/receiver/inventory/{}/0",
        paused.origin,
        hex::encode(head.inventory)
    );
    assert!(reqwest::get(chunk).await.unwrap().status().is_success());
    assert_eq!(lookup(&client, two).await.len(), 1);
}

#[cfg(feature = "qualification-tests")]
/// A seal whose choice objects cannot be built is never recorded: the pass fails, the
/// previous inventory keeps serving and the recorded seals stay, and the next pass that
/// can build the shard records its seal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_seal_build_records_nothing_and_retries() {
    let mut paused = Paused::new(|_| {}).await;
    paused.target = 1;
    paused.extend(&[super::common::receiver()], 2, 0);
    paused.refresh().await.unwrap();
    let first = paused.get("inventory").await;
    assert_eq!(paused.store().sealed_shards().unwrap().len(), 1);
    paused.extend(&[super::common::seeded_receiver(1)], 0, 0);
    paused.state.with_policy(|p| p.seal_seeds = 0);
    let error = paused.refresh().await.unwrap_err();
    assert!(error.to_string().contains("choice"), "{error}");
    assert_eq!(paused.store().sealed_shards().unwrap().len(), 1);
    assert_eq!(paused.get("inventory").await, first);
    let seeds = crate::seal::Policy::default().seal_seeds;
    paused.state.with_policy(|p| p.seal_seeds = seeds);
    paused.refresh().await.unwrap();
    assert_eq!(paused.store().sealed_shards().unwrap().len(), 2);
    assert_ne!(paused.get("inventory").await, first);
}

#[cfg(feature = "qualification-tests")]
/// A publisher stop ends shard building at the next shard instead of waiting for
/// every shard, and writes no publication.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_requested_stop_ends_the_pass_before_the_next_shard() {
    let mut paused = Paused::new(|_| {}).await;
    paused.target = 1;
    paused.extend(&[super::common::receiver()], 2, 0);
    let stop = pir_pipeline::service::Shutdown::default();
    stop.request();
    let mut state = State::stopping_with(stop);
    state.with_policy(|p| p.target = paused.target);
    let rpc = std::slice::from_ref(&paused.rpc);
    let error = refresh(
        &paused.args,
        rpc,
        Network::Mainnet.genesis_hash(),
        None,
        &mut state,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("stopped"), "{error}");
    let root = paused.dir.path().join("publications");
    let written = std::fs::read_dir(&root).map_or(0, |entries| {
        entries
            .filter(|e| {
                let path = e.as_ref().unwrap().path();
                path.to_str().unwrap().ends_with(".inventory.json")
            })
            .count()
    });
    assert_eq!(written, 0);
}

#[cfg(feature = "qualification-tests")]
/// Two one-shot runs a block apart keep a sealed payment table whose provisional
/// companion changed: each table is written under its own revision, so the second run
/// writes the new companion beside the old instead of colliding with it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_shot_runs_name_a_changed_companion_by_its_own_revision() {
    let mut paused = Paused::new(|_| {}).await;
    paused.target = 1;
    paused.extend(&[super::common::receiver()], 2, 0);
    paused.extend(&[], 1, 0);
    let genesis = Network::Mainnet.genesis_hash();
    let root = paused.dir.path().join("publications");
    let layouts = || -> Vec<Layout> {
        std::fs::read_dir(&root)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.to_str().unwrap().ends_with(".inventory.json"))
            .map(|p| serde_json::from_slice(&std::fs::read(p).unwrap()).unwrap())
            .collect()
    };
    let mut state = paused.fresh_state();
    let rpc = std::slice::from_ref(&paused.rpc);
    refresh(&paused.args, rpc, genesis, None, &mut state)
        .await
        .unwrap();
    let first = layouts().remove(0);
    paused.extend(&[], 1, 0);
    let rpc = std::slice::from_ref(&paused.rpc);
    refresh(&paused.args, rpc, genesis, None, &mut state)
        .await
        .unwrap();
    let all = layouts();
    assert_eq!(all.len(), 2);
    let second = all.into_iter().find(|l| *l != first).unwrap();
    let sealed = |l: &Layout| {
        let segment = &l.shards[0].segments[0];
        (segment.directory, segment.companion)
    };
    let ((payments, old), (same, new)) = (sealed(&first), sealed(&second));
    assert_eq!(payments, same);
    assert_ne!(old, new);
    for companion in [old, new] {
        let name = format!("c-{}.json", hex::encode(companion));
        assert!(root.join(name).exists());
    }
}

/// More sealed shards than the table budget holds still serve every payment over
/// real encrypted PIR: prepared tables are cached within the budget and reloaded from
/// the data directory when evicted, and a restart prepares none of them again. A
/// sealed shard and the recent tier are four tables against a budget of two, the
/// most one lookup needs at once.
#[cfg(feature = "qualification-tests")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sealed_tables_beyond_the_budget_load_on_demand() {
    use super::common::receiver;
    // Room for one table's preparation beside one prepared table.
    let budget = receiver_pir_server::tables::resident_bytes(2048)
        + receiver_pir_server::tables::peak_bytes(receiver::Kind::Companion, 2048);
    let publications = Publications::new(std::time::Duration::from_secs(600), budget);
    let mut paused = Paused::serving(|_| {}, publications.clone()).await;
    paused.target = 1;
    let payees = [receiver()];
    for payee in payees {
        paused.extend(&[payee], 0, 0);
    }
    paused.extend(&[], 1, 0);
    paused.refresh().await.unwrap();
    let tables = publications.tables();
    assert!(tables.resident() <= budget);
    let (client, _) = paused.capture(false).await;
    assert_eq!(client.shards.len(), 2);
    let loads = tables.loads();
    for payee in payees {
        let found = lookup(&client, payee).await;
        assert_eq!(found.len(), 1);
        let (_, _, record, _) = &found[0];
        let payload = found[0].3;
        assert_eq!(
            payload,
            fresh(&paused.store(), &[record.payment.position])[0]
        );
        assert!(tables.resident() <= budget);
    }
    assert!(tables.loads() > loads, "evicted tables were loaded again");
    // A new process finds every table in the data directory and prepares none:
    // nothing it serves yet names them, so they must outlive its first pass.
    let head = client.head.clone();
    let stored = std::fs::read_dir(paused.dir.path().join(crate::durable::TABLES_DIR))
        .unwrap()
        .count();
    paused.restart().await;
    paused.refresh().await.unwrap();
    let (again, _) = paused.capture(false).await;
    assert_eq!(&again.head, &head);
    assert_eq!(paused.state.durable.as_ref().unwrap().prepared(), 0);
    let tables = paused.publications.tables();
    assert_eq!(tables.loads(), 0);
    assert!(tables.resident() <= budget);
    let found = lookup(&again, payees[0]).await;
    assert_eq!(found.len(), 1);
    assert_eq!(
        std::fs::read_dir(paused.dir.path().join(crate::durable::TABLES_DIR))
            .unwrap()
            .count(),
        stored
    );
}

/// The checked records kept shards hold in memory.
#[cfg(feature = "qualification-tests")]
fn kept_leaves(state: &State) -> usize {
    state
        .kept
        .iter()
        .flat_map(|k| &k.segments)
        .map(|s| s.records.as_ref().map_or(0, |r| r.len()))
        .sum()
}

/// Memory a serving indexer holds for its inventories: the table budget's resident
/// bytes (prepared tables and filter encodings), and bytes held outside it, which are
/// kept shards' checked records.
#[cfg(feature = "qualification-tests")]
fn footprint(paused: &Paused) -> (usize, usize) {
    let leaves = kept_leaves(&paused.state)
        * std::mem::size_of::<zakura_pir_publisher::receiver::RawRecord>();
    (paused.publications.tables().resident(), leaves)
}

/// Publishing more sealed shards, and restarting as a new process, keeps the accounted
/// footprint within the table budget, with nothing held outside it once companions
/// freeze: memory does not grow with sealed history. Every payment still serves.
#[cfg(feature = "qualification-tests")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resident_memory_stays_bounded_as_sealed_shards_accumulate() {
    use super::common::{receiver, seeded_receiver};
    // Four sealed shards per batch, the second batch all paying one receiver.
    let batches = [
        [
            receiver(),
            seeded_receiver(1),
            seeded_receiver(2),
            seeded_receiver(3),
        ],
        [seeded_receiver(4); 4],
    ];
    // Room for one table's preparation beside one prepared table.
    let budget = receiver_pir_server::tables::resident_bytes(2048)
        + receiver_pir_server::tables::peak_bytes(receiver::Kind::Companion, 2048);
    let publications = Publications::new(DEFAULT_RETENTION, budget);
    let mut paused = Paused::serving(|_| {}, publications).await;
    paused.target = 1;
    let mut footprints = Vec::new();
    for batch in batches {
        for payee in batch {
            paused.extend(&[payee], 0, 0);
        }
        // Completing the Ironwood shard freezes every companion so far.
        paused.extend(&[], 1 << 16, 0);
        paused.refresh().await.unwrap();
        footprints.push(footprint(&paused));
    }
    let sealed = paused.state.kept.len();
    assert_eq!(sealed, 8);
    paused.restart().await;
    paused.refresh().await.unwrap();
    assert_eq!(paused.state.kept.len(), sealed);
    assert_eq!(paused.state.durable.as_ref().unwrap().prepared(), 0);
    footprints.push(footprint(&paused));
    for (resident, held) in &footprints {
        assert!(*resident <= budget, "{footprints:?}");
        assert_eq!(*held, 0, "{footprints:?}");
    }
    // The oldest and newest shards serve through cold loads within the budget.
    let (client, _) = paused.capture(true).await;
    for (payee, payments) in [(receiver(), 1), (seeded_receiver(4), 4)] {
        assert_eq!(lookup(&client, payee).await.len(), payments);
        let (resident, held) = footprint(&paused);
        assert!(resident <= budget && held == 0);
    }
}

/// A displaced inventory that a request still holds keeps its stored tables through a
/// pass after its retention ends, so the request can still load them; the first pass
/// after it is released deletes them. The serving crate checks the same reference
/// rule without preparing another inventory.
#[cfg(feature = "qualification-tests")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_held_inventory_keeps_its_stored_tables_until_released() {
    let publications = Publications::new(std::time::Duration::from_millis(1), 1 << 30);
    let mut paused = Paused::serving(|_| {}, publications.clone()).await;
    paused.extend(&[super::common::receiver()], 0, 0);
    paused.refresh().await.unwrap();
    // A request selected the recent tier and waits to load it.
    let held = publications.live().remove(0);
    let recent = held.segments().last().unwrap().directory.clone();
    let revision = recent
        .directory_session()
        .unwrap()
        .manifest
        .revision()
        .unwrap();
    let rows = paused
        .dir
        .path()
        .join(crate::durable::TABLES_DIR)
        .join(format!("d-{}.rows", hex::encode(revision)));
    paused.extend(&[], 1, 0);
    paused.refresh().await.unwrap();
    assert!(rows.exists());
    drop((held, recent));
    paused.extend(&[], 1, 0);
    paused.refresh().await.unwrap();
    assert!(!rows.exists());
}

#[cfg(feature = "qualification-tests")]
/// A stored manifest over the manifest size bound does not stop a restart: its table
/// is discarded as a leftover and prepared again, and a wallet reads it as usual.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_oversized_stored_manifest_is_prepared_again_after_a_restart() {
    let payee = super::common::receiver();
    let mut paused = Paused::new(|_| {}).await;
    paused.extend(&[payee], 0, 0);
    paused.refresh().await.unwrap();
    let tables = paused.dir.path().join(crate::durable::TABLES_DIR);
    let manifest = std::fs::read_dir(&tables)
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| {
            let name = p.file_name().unwrap().to_str().unwrap();
            name.starts_with("d-") && name.ends_with(".json")
        })
        .unwrap();
    let mut bytes = std::fs::read(&manifest).unwrap();
    bytes.resize(
        zakura_pir_publisher::receiver::session::MAX_MANIFEST_BYTES + 1,
        b' ',
    );
    std::fs::write(&manifest, bytes).unwrap();
    paused.restart().await;
    paused.refresh().await.unwrap();
    assert!(paused.state.durable.as_ref().unwrap().prepared() >= 1);
    let (client, _) = paused.capture(false).await;
    assert_eq!(lookup(&client, payee).await.len(), 1);
}

#[cfg(feature = "qualification-tests")]
/// A stored table whose public setup is not the one its manifest binds is not
/// served after a restart: it is prepared again, and a wallet reads it as usual.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_corrupt_stored_setup_is_prepared_again_after_a_restart() {
    let payee = super::common::receiver();
    let mut paused = Paused::new(|_| {}).await;
    paused.extend(&[payee], 0, 0);
    paused.refresh().await.unwrap();
    let tables = paused.dir.path().join(crate::durable::TABLES_DIR);
    let publics: Vec<_> = std::fs::read_dir(&tables)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "public"))
        .collect();
    assert_eq!(publics.len(), 2, "the directory and companion");
    for public in &publics {
        let mut bytes = std::fs::read(public).unwrap();
        bytes[0] ^= 1;
        std::fs::write(public, bytes).unwrap();
    }
    paused.restart().await;
    paused.refresh().await.unwrap();
    assert_eq!(paused.state.durable.as_ref().unwrap().prepared(), 2);
    let (client, _) = paused.capture(false).await;
    let found = lookup(&client, payee).await;
    assert_eq!(found.len(), 1);
    let (_, _, record, _) = &found[0];
    let payload = found[0].3;
    assert_eq!(
        payload,
        fresh(&paused.store(), &[record.payment.position])[0]
    );
}

#[cfg(feature = "qualification-tests")]
/// After a restart a stored table or filter is reused only if it is exactly what the
/// index rebuilds: corrupted or missing rows are prepared again and a corrupted filter
/// is written again, so wallets read every payment, payload and filter as before.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stored_copies_are_rebuilt_unless_they_match_the_index() {
    let payee = super::common::receiver();
    let mut paused = Paused::new(|_| {}).await;
    paused.target = 1;
    paused.extend(&[payee], 0, 0);
    paused.extend(&[], 1, 0);
    paused.refresh().await.unwrap();
    let tables = paused.dir.path().join(crate::durable::TABLES_DIR);
    let files = |extension: &str, prefix: &str| -> Vec<std::path::PathBuf> {
        std::fs::read_dir(&tables)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| {
                p.extension().is_some_and(|e| e == extension)
                    && p.file_name().unwrap().to_str().unwrap().starts_with(prefix)
            })
            .collect()
    };
    let flip = |path: &std::path::Path| {
        let mut bytes = std::fs::read(path).unwrap();
        bytes[0] ^= 1;
        std::fs::write(path, bytes).unwrap();
    };
    let (payments, witnesses, filters) = (
        files("rows", "d-"),
        files("rows", "c-"),
        files("filter", "f-"),
    );
    assert_eq!((payments.len(), witnesses.len(), filters.len()), (2, 2, 1));
    let filter = std::fs::read(&filters[0]).unwrap();
    payments.iter().for_each(|p| flip(p));
    witnesses
        .iter()
        .for_each(|w| std::fs::remove_file(w).unwrap());
    flip(&filters[0]);
    paused.restart().await;
    paused.refresh().await.unwrap();
    assert_eq!(paused.state.durable.as_ref().unwrap().prepared(), 4);
    assert_eq!(std::fs::read(&filters[0]).unwrap(), filter);
    let (client, _) = paused.capture(true).await;
    let found = lookup(&client, payee).await;
    assert_eq!(found.len(), 1);
    let (_, _, record, _) = &found[0];
    let payload = found[0].3;
    assert_eq!(
        payload,
        fresh(&paused.store(), &[record.payment.position])[0]
    );
}

#[cfg(feature = "qualification-tests")]
/// After a restart a directory whose stored choice object is corrupted or missing is
/// prepared and written again, so wallets keep finding their payments.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bad_stored_choice_is_prepared_again_after_a_restart() {
    let payee = super::common::receiver();
    let mut paused = Paused::new(|_| {}).await;
    paused.target = 1;
    paused.extend(&[payee], 0, 0);
    paused.extend(&[], 1, 0);
    paused.refresh().await.unwrap();
    let tables = paused.dir.path().join(crate::durable::TABLES_DIR);
    let mut choices: Vec<_> = std::fs::read_dir(&tables)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "choice"))
        .collect();
    choices.sort();
    assert_eq!(choices.len(), 2);
    let stored: Vec<_> = choices.iter().map(|c| std::fs::read(c).unwrap()).collect();
    let mut flipped = stored[0].clone();
    flipped[0] ^= 1;
    std::fs::write(&choices[0], flipped).unwrap();
    std::fs::remove_file(&choices[1]).unwrap();
    paused.restart().await;
    paused.refresh().await.unwrap();
    assert_eq!(paused.state.durable.as_ref().unwrap().prepared(), 2);
    for (choice, bytes) in choices.iter().zip(&stored) {
        assert_eq!(&std::fs::read(choice).unwrap(), bytes);
    }
    let (client, _) = paused.capture(false).await;
    assert_eq!(lookup(&client, payee).await.len(), 1);
}

#[cfg(feature = "qualification-tests")]
/// A cold reload, staged or stored, reads only the rows of the session it was
/// verified under: a longer replacement is refused before it is read, and a
/// replaced session is never used.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cold_reloads_read_only_the_verified_geometry() {
    let mut paused = Paused::new(|_| {}).await;
    paused.extend(&[super::common::receiver()], 0, 0);
    paused.refresh().await.unwrap();
    let stored = paused.publications.live()[0].segments()[0]
        .directory
        .clone();
    let revision = stored
        .directory_session()
        .unwrap()
        .manifest
        .revision()
        .unwrap();
    let file = |extension: &str| {
        let name = format!("d-{}.{extension}", hex::encode(revision));
        paused
            .dir
            .path()
            .join(crate::durable::TABLES_DIR)
            .join(name)
    };
    let rows = std::fs::read(file("rows")).unwrap();
    let mut session: receiver::Session<receiver::Directory> =
        serde_json::from_slice(&std::fs::read(file("json")).unwrap()).unwrap();
    session.manifest.salt[0] ^= 1;
    std::fs::write(file("json"), serde_json::to_vec(&session).unwrap()).unwrap();
    let cold = || receiver_pir_server::tables::Tables::new(1 << 30);
    assert!(cold().get(&stored).is_ok());
    std::fs::write(file("rows"), [rows.as_slice(), &rows].concat()).unwrap();
    let tables = cold();
    assert!(matches!(
        tables.get(&stored),
        Err(receiver_pir_server::tables::LoadError::Load(_))
    ));
    assert_eq!(tables.resident(), 0);
}

/// `receiver`'s first payment's companion payload in the first shard, checked against
/// a fresh rebuild from `store`.
#[cfg(feature = "qualification-tests")]
async fn served_payload(client: &Captured, store: &Store, receiver: [u8; 43]) -> [u8; 512] {
    let (record, payload) = client
        .wallet
        .lookup(&client.shards[0].segments[0], &receiver)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(payload, fresh(store, &[record.payment.position])[0]);
    payload
}

/// The first shard's directory and companion session IDs and companion tree height.
#[cfg(feature = "qualification-tests")]
fn first_shard(client: &Captured) -> ([u8; 32], [u8; 32], u32) {
    let segment = &client.shards[0].segments[0];
    (
        segment.directory.id().unwrap(),
        segment.companion.id().unwrap(),
        segment.companion.manifest.tree.height,
    )
}

/// A sealed shard's directory stays while its companion follows its witness
/// dependencies: provisional at each tip until its payment's Ironwood shard
/// completes, rebuilt when a reorg replaces the block after the seal, frozen at the
/// completing block and reused, and rebuilt again when a reorg replaces that block
/// even though every payment block is unchanged. Served payloads equal a fresh
/// rebuild throughout.
#[cfg(feature = "qualification-tests")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn witness_dependencies_follow_reorgs_after_a_seal() {
    let one = super::common::receiver();
    let mut paused = Paused::new(|_| {}).await;
    paused.target = 1;
    let paid = paused.extend(&[one], 4, 0);
    paused.refresh().await.unwrap();
    let (sealed, _) = paused.capture(false).await;
    let (payments, w0, _) = first_shard(&sealed);
    // Short of completing Ironwood shard 0, after the seal.
    let filler = (1 << 16) - 15;
    paused.extend(&[], filler, 0);
    paused.refresh().await.unwrap();
    let (client, _) = paused.capture(false).await;
    let (p1, w1, _) = first_shard(&client);
    assert_eq!(p1, payments);
    assert_ne!(w1, w0);
    let provisional = served_payload(&client, &paused.store(), one).await;
    // A provisional companion keeps its records to rebuild at the next tip.
    assert_eq!(kept_leaves(&paused.state), 1);
    // A reorg after the seal, before witness finality.
    paused.store().rewind(paid.height, paid.hash).unwrap();
    paused.extend(&[], filler, 1);
    paused.refresh().await.unwrap();
    let (client, _) = paused.capture(false).await;
    let (p2, w2, _) = first_shard(&client);
    assert_eq!(p2, payments);
    assert_ne!(w2, w1);
    assert_ne!(
        served_payload(&client, &paused.store(), one).await,
        provisional
    );
    // Completing the Ironwood shard freezes the companion at the completing block.
    let completing = paused.extend(&[], 20, 0);
    paused.refresh().await.unwrap();
    let (client, _) = paused.capture(false).await;
    let (_, w3, frozen_at) = first_shard(&client);
    assert_eq!(frozen_at, completing.height);
    let final_payload = served_payload(&client, &paused.store(), one).await;
    // A frozen one keeps none: only its record count stays in memory.
    assert_eq!(kept_leaves(&paused.state), 0);
    assert_eq!(paused.state.kept[0].segments[0].payments, 1);
    paused.extend(&[], 1, 0);
    paused.refresh().await.unwrap();
    assert_eq!(first_shard(&paused.capture(false).await.0).1, w3);
    // A reorg of the completing block rebuilds the shard: the directory is unchanged,
    // so its stored copy is reused, and the companion is built again.
    let before = paused.store().checkpoint(completing.height - 1).unwrap();
    paused.store().rewind(before.height, before.hash).unwrap();
    paused.extend(&[], 20, 2);
    paused.refresh().await.unwrap();
    let (client, _) = paused.capture(false).await;
    let (p5, w5, _) = first_shard(&client);
    assert_eq!(p5, payments);
    assert_ne!(w5, w3);
    assert_ne!(
        served_payload(&client, &paused.store(), one).await,
        final_payload
    );
}

/// The first sealed shard's payment table revision and companion manifest in
/// `shards`, as a one-shot pass wrote them.
#[cfg(feature = "qualification-tests")]
fn first_written(shards: &[Vec<Written>]) -> ([u8; 32], receiver::Companion) {
    use receiver::Table as _;
    let segment = &shards[0][0];
    (
        segment.directory.revision().unwrap(),
        segment.companion.clone(),
    )
}

/// One history crosses witness finality and both reorg boundaries. A provisional
/// companion follows the tip, a completed one freezes, and replacing either
/// block rebuilds its witness without changing the payment table. Each payload
/// is checked against an independent rebuild from the stored commitments.
#[cfg(feature = "qualification-tests")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn written_companions_follow_reorgs_before_and_after_finality() {
    let one = super::common::receiver();
    let mut paused = Paused::new(|_| {}).await;
    paused.target = 1;
    let mut state = paused.fresh_state();
    let paid = paused.extend(&[one], 4, 0);
    let (payments, w0) = first_written(&one_shot(&paused, &mut state).await.1);
    // Short of completing Ironwood shard 0, after the seal.
    let filler = (1 << 16) - 15;
    paused.extend(&[], filler, 0);
    let (_, shards) = one_shot(&paused, &mut state).await;
    let (p1, w1) = first_written(&shards);
    assert_eq!(p1, payments);
    assert_ne!(w1, w0);
    assert_eq!(w1.tree.height, paused.store().tip().unwrap().height);
    let (payment, provisional) = shards[0][0].payment(one);
    assert_eq!(provisional, fresh(&paused.store(), &[payment.position])[0]);
    // A reorg after the seal, before witness finality.
    paused.store().rewind(paid.height, paid.hash).unwrap();
    paused.extend(&[], filler, 1);
    let (_, shards) = one_shot(&paused, &mut state).await;
    let (p2, w2) = first_written(&shards);
    assert_eq!(p2, payments);
    assert_ne!(w2, w1);
    let (_, reorged) = shards[0][0].payment(one);
    assert_eq!(reorged, fresh(&paused.store(), &[payment.position])[0]);
    assert_ne!(reorged, provisional);
    // Continue the same history through the block that completes the shard.
    let completing = paused.extend(&[], 20, 0);
    let (_, shards) = one_shot(&paused, &mut state).await;
    let (p3, w3) = first_written(&shards);
    assert_eq!(p3, payments);
    assert_eq!(w3.tree.height, completing.height);
    let (payment, final_payload) = shards[0][0].payment(one);
    assert_eq!(
        final_payload,
        fresh(&paused.store(), &[payment.position])[0]
    );
    paused.extend(&[], 1, 0);
    assert_eq!(first_written(&one_shot(&paused, &mut state).await.1).1, w3);
    // A reorg of the completing block rebuilds the companion of the unchanged table.
    let before = paused.store().checkpoint(completing.height - 1).unwrap();
    paused.store().rewind(before.height, before.hash).unwrap();
    paused.extend(&[], 20, 2);
    let (_, shards) = one_shot(&paused, &mut state).await;
    let (p5, w5) = first_written(&shards);
    assert_eq!(p5, payments);
    assert_ne!(w5, w3);
    let (_, rebuilt) = shards[0][0].payment(one);
    assert_eq!(rebuilt, fresh(&paused.store(), &[payment.position])[0]);
    assert_ne!(rebuilt, final_payload);
}

/// A correction adding a payment to a sealed shard older than the rolling filters
/// unseals from the corrected block and writes the shards again under the new
/// generation with the same boundaries, the payment and its witness, without any PIR
/// preparation. `a_correction_older_than_retention_rebuilds_its_shard` also checks
/// the served sessions' revocation under `qualification-tests`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_written_correction_older_than_retention_reseals_its_shard() {
    use super::common::{receiver, seeded_receiver};
    let mut paused = Paused::new(|_| {}).await;
    (paused.target, paused.args.filter_blocks) = (1, Some(1));
    let block = paused.extend(&[receiver()], 1, 0);
    paused.extend(&[seeded_receiver(1)], 0, 0);
    paused.extend(&[], 1, 0);
    let bounds = |shards: &[Vec<Written>]| {
        shards
            .iter()
            .map(|s| {
                (
                    s[0].directory.coverage.start_height,
                    s[0].directory.coverage.end_height,
                )
            })
            .collect::<Vec<_>>()
    };
    let (old, shards) = one_shot(&paused, &mut paused.fresh_state()).await;
    let old_bounds = bounds(&shards);
    let mut corrected = block.clone();
    let mut omitted = corrected.payments[0].1.clone();
    omitted.position += 1;
    omitted.cmx = corrected.commitments[1];
    (omitted.tx_index, omitted.txid) = (2, [9; 32]);
    let three = seeded_receiver(2);
    corrected.payments.push((three, omitted.clone()));
    paused.store().correct(1, &[corrected]).unwrap();
    assert!(paused.store().sealed_shards().unwrap().is_empty());
    paused.args.history_revision = 1;
    let (layout, shards) = one_shot(&paused, &mut paused.fresh_state()).await;
    assert_eq!(layout.history_revision, 1);
    assert_eq!(bounds(&shards), old_bounds);
    let sealed = |l: &Layout| l.shards.iter().map(|s| s.sealed).collect::<Vec<_>>();
    assert_eq!(sealed(&layout), sealed(&old));
    let (payment, payload) = shards[0][0].payment(three);
    let fields =
        |p: &zakura_pir_publisher::receiver::Payment| (p.position, p.cmx, p.tx_index, p.txid);
    assert_eq!(
        fields(&payment),
        (
            omitted.position,
            omitted.cmx,
            omitted.tx_index,
            omitted.txid
        )
    );
    assert_eq!(payload, fresh(&paused.store(), &[omitted.position])[0]);
}

/// A reorg across a seal drops the sealed shard, and the replacement chain seals
/// again with its own payment and a different end. Written passes prepare no PIR
/// tables. `a_reorg_across_a_seal_reseals_the_replacement` also revokes the served
/// session under `qualification-tests`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_written_reorg_across_a_seal_reseals_the_replacement() {
    use super::common::{receiver, seeded_receiver};
    let mut paused = Paused::new(|_| {}).await;
    paused.target = 1;
    paused.extend(&[receiver()], 0, 0);
    paused.extend(&[], 1, 0);
    let (old, old_shards) = one_shot(&paused, &mut paused.fresh_state()).await;
    assert!(old.shards[0].sealed);
    let old_end = old_shards[0][0].directory.coverage.end_hash;
    let activation = paused
        .store()
        .checkpoint(crate::blocks::ironwood_activation())
        .unwrap();
    paused
        .store()
        .rewind(activation.height, activation.hash)
        .unwrap();
    assert!(paused.store().sealed_shards().unwrap().is_empty());
    let replaced = seeded_receiver(1);
    paused.extend(&[replaced], 0, 7);
    paused.extend(&[], 1, 7);
    let (layout, shards) = one_shot(&paused, &mut paused.fresh_state()).await;
    assert!(layout.shards[0].sealed);
    assert_ne!(shards[0][0].directory.coverage.end_hash, old_end);
    assert!(!shards
        .iter()
        .flatten()
        .any(|segment| segment.contains(receiver())));
    assert!(shards
        .iter()
        .flatten()
        .any(|segment| segment.contains(replaced)));
    shards[0][0].payment(replaced);
}

/// A reorg across a seal removes the sealed shard and revokes its sessions, and the
/// replacement chain is sealed afresh. Fast already reseals that replacement through
/// a written pass.
#[cfg(feature = "qualification-tests")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reorg_across_a_seal_reseals_the_replacement() {
    use super::common::{receiver, seeded_receiver};
    let mut paused = Paused::new(|_| {}).await;
    paused.target = 1;
    paused.extend(&[receiver()], 0, 0);
    paused.extend(&[], 1, 0);
    paused.refresh().await.unwrap();
    let (old, _) = paused.capture(true).await;
    let old_head = old.head.clone();
    let activation = paused
        .store()
        .checkpoint(crate::blocks::ironwood_activation())
        .unwrap();
    paused
        .store()
        .rewind(activation.height, activation.hash)
        .unwrap();
    assert!(paused.store().sealed_shards().unwrap().is_empty());
    paused.extend(&[seeded_receiver(1)], 0, 7);
    paused.extend(&[], 1, 7);
    paused.refresh().await.unwrap();
    let chunk = format!(
        "{}/v1/receiver/inventory/{}/0",
        paused.origin,
        hex::encode(old_head.inventory)
    );
    assert_eq!(
        reqwest::get(chunk).await.unwrap().status(),
        reqwest::StatusCode::GONE
    );
    let (client, _) = paused.capture(true).await;
    let shard = &client.shards[0];
    assert!(shard.sealed);
    assert_ne!(shard.end_hash, old.shards[0].end_hash);
    assert!(lookup(&client, receiver()).await.is_empty());
    assert_eq!(lookup(&client, seeded_receiver(1)).await.len(), 1);
}

/// A correction adding a payment to a sealed shard older than the rolling filters
/// revokes every session at once, unseals from the corrected block and republishes
/// under the new generation with the same boundaries, the payment and its witness.
#[cfg(feature = "qualification-tests")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_correction_older_than_retention_rebuilds_its_shard() {
    use super::common::{receiver, seeded_receiver};
    let mut paused = Paused::new(|_| {}).await;
    (paused.target, paused.args.filter_blocks) = (1, Some(1));
    let block = paused.extend(&[receiver()], 1, 0);
    paused.extend(&[seeded_receiver(1)], 0, 0);
    paused.extend(&[], 1, 0);
    paused.refresh().await.unwrap();
    let (old, _) = paused.capture(true).await;
    let bounds = |c: &Captured| {
        c.shards
            .iter()
            .map(|s| (s.start_height, s.end_height))
            .collect::<Vec<_>>()
    };
    let mut corrected = block.clone();
    let mut omitted = corrected.payments[0].1.clone();
    omitted.position += 1;
    omitted.cmx = corrected.commitments[1];
    (omitted.tx_index, omitted.txid) = (2, [9; 32]);
    let three = seeded_receiver(2);
    corrected.payments.push((three, omitted.clone()));
    paused.store().correct(1, &[corrected]).unwrap();
    assert!(paused.store().sealed_shards().unwrap().is_empty());
    paused.args.history_revision = 1;
    paused.refresh().await.unwrap();
    let chunk = format!(
        "{}/v1/receiver/inventory/{}/0",
        paused.origin,
        hex::encode(old.head.inventory)
    );
    assert_eq!(
        reqwest::get(chunk).await.unwrap().status(),
        reqwest::StatusCode::GONE
    );
    let (client, _) = paused.capture(true).await;
    assert_eq!(client.head.history_revision, 1);
    assert_eq!(bounds(&client), bounds(&old));
    let found = lookup(&client, three).await;
    assert_eq!(found.len(), 1);
    let (shard, _, record, payload) = &found[0];
    assert_eq!((*shard, record.payment.position), (0, omitted.position));
    assert_eq!(*payload, fresh(&paused.store(), &[omitted.position])[0]);
}

/// An offline correction edits an existing index and never starts an empty one,
/// which would hide a version 2 index from its conversion.
#[test]
fn a_correction_never_starts_an_empty_index() {
    let dir = tempfile::tempdir().unwrap();
    let config = StoreConfig {
        genesis: Network::Mainnet.genesis_hash(),
        start_height: crate::blocks::ironwood_activation(),
        start_parent: [2; 32],
        start_position: 0,
    };
    assert!(correct(dir.path(), config, 1, &[]).is_err());
    assert!(!dir.path().join(receiver_store::STORE_FILE).exists());
}

#[cfg(feature = "qualification-tests")]
/// A correction is refused while the service holds the data directory, and once
/// applied offline the restarted service publishes the store's higher revision under
/// its unchanged configuration, revoking older sessions before serving.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_offline_correction_is_published_at_the_next_start() {
    let mut paused = Paused::new(|_| {}).await;
    paused.refresh().await.unwrap();
    let old = paused.segment().await.directory;
    let height = crate::blocks::ironwood_activation();
    let block = receiver_store::IndexedBlock {
        height,
        hash: [3; 32],
        parent: [2; 32],
        start_position: 0,
        end_position: 0,
        coinbase_actions: 0,
        payments: Vec::new(),
        commitments: Vec::new(),
    };
    let config = StoreConfig {
        genesis: Network::Mainnet.genesis_hash(),
        start_height: height,
        start_parent: [2; 32],
        start_position: 0,
    };
    let held = pir_pipeline::service::lock(paused.dir.path()).unwrap();
    let refused = correct(
        paused.dir.path(),
        config.clone(),
        1,
        std::slice::from_ref(&block),
    );
    assert!(refused
        .unwrap_err()
        .to_string()
        .contains("service is running"));
    drop(held);
    correct(paused.dir.path(), config, 1, std::slice::from_ref(&block)).unwrap();
    paused.state = paused.fresh_state();
    paused.refresh().await.unwrap();
    assert_eq!(paused.args.history_revision, 0);
    assert_eq!(paused.get("inventory").await["history_revision"], 1);
    let id = hex::encode(old.id().unwrap());
    let gone = reqwest::get(format!("{}/v1/receiver/public/{id}", paused.origin))
        .await
        .unwrap();
    assert_eq!(gone.status(), reqwest::StatusCode::GONE);
}

#[cfg(feature = "qualification-tests")]
/// An outage proves nothing about canonicality; a restarted application recovers
/// its committed index and serves the same session through the real HTTP router.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_failure_keeps_serving_and_restart_recovers() {
    let mut paused = Paused::new(|_| {}).await;
    paused.refresh().await.unwrap();
    let first = paused.get("inventory").await;
    paused.node.hashes.lock().unwrap().remove(&0);
    assert!(paused.refresh().await.is_err());
    assert_eq!(paused.get("inventory").await, first);
    paused
        .node
        .hashes
        .lock()
        .unwrap()
        .insert(0, Network::Mainnet.genesis_hash());
    paused.state = paused.fresh_state();
    paused.refresh().await.unwrap();
    assert_eq!(paused.get("inventory").await, first);
}

#[cfg(feature = "qualification-tests")]
/// A raised correction revision is durable before the backfill it needs: when that
/// backfill fails, a restart that rolls the configured revision back still uses it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_raised_revision_is_durable_before_backfill() {
    let mut paused = Paused::new(|_| {}).await;
    paused.refresh().await.unwrap();
    let next = u64::from(crate::blocks::ironwood_activation()) + 1;
    *paused.node.tip.lock().unwrap() = next;
    paused.node.hashes.lock().unwrap().insert(next, [4; 32]);
    paused.args.history_revision = 1;
    // The node serves no block bodies, so the backfill fails.
    assert!(paused.refresh().await.is_err());
    assert_eq!(paused.store().history_revision().unwrap(), 1);
    assert_eq!(
        paused.publications.epoch(),
        1,
        "older sessions were revoked"
    );
    // A restart configured with the old revision still collects under the store's.
    paused.args.history_revision = 0;
    paused.state = paused.fresh_state();
    let epoch = paused.publications.epoch();
    assert!(paused.refresh().await.is_err());
    assert_eq!(paused.store().history_revision().unwrap(), 1);
    assert_eq!(paused.publications.epoch(), epoch, "nothing moved back");
}

#[cfg(feature = "qualification-tests")]
/// A one-shot run writes no rolling filters, so it neither builds them nor fails on
/// them: a filter window that cannot be built leaves its publication unaffected,
/// while a serving run, which does build it, refuses.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_one_shot_run_does_not_build_rolling_filters() {
    let mut paused = Paused::new(|_| {}).await;
    paused.args.filter_blocks = Some(0);
    let rpc = std::slice::from_ref(&paused.rpc);
    let genesis = Network::Mainnet.genesis_hash();
    refresh(&paused.args, rpc, genesis, None, &mut paused.fresh_state())
        .await
        .unwrap();
    let written = std::fs::read_dir(paused.dir.path().join("publications")).unwrap();
    assert!(written.count() > 0);
    assert!(paused.refresh().await.is_err());
}

#[cfg(feature = "qualification-tests")]
/// A caught-up service keeps the rolling window it serves while the tip and
/// correction revision stay put, rather than rebuilding it every poll, and builds a
/// new one once the tip moves.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unchanged_tip_keeps_the_rolling_window() {
    let mut paused = Paused::new(|_| {}).await;
    paused.refresh().await.unwrap();
    let window = paused.publications.filter_window().unwrap();
    paused.refresh().await.unwrap();
    assert!(Arc::ptr_eq(
        &window,
        &paused.publications.filter_window().unwrap()
    ));
    // An empty block after the tip, indexed and on the node's chain.
    let next = crate::blocks::ironwood_activation() + 1;
    paused
        .store()
        .append(&receiver_store::IndexedBlock {
            height: next,
            hash: [4; 32],
            parent: [3; 32],
            start_position: 0,
            end_position: 0,
            coinbase_actions: 0,
            payments: Vec::new(),
            commitments: Vec::new(),
        })
        .unwrap();
    *paused.node.tip.lock().unwrap() = u64::from(next);
    paused
        .node
        .hashes
        .lock()
        .unwrap()
        .insert(u64::from(next), [4; 32]);
    paused.refresh().await.unwrap();
    assert!(!Arc::ptr_eq(
        &window,
        &paused.publications.filter_window().unwrap()
    ));
}

#[cfg(feature = "qualification-tests")]
/// A window the byte bound shortened starts after the block count's first block, and
/// is still kept while the tip and correction revision stay put.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unchanged_tip_keeps_a_byte_shortened_window() {
    let mut paused = Paused::new(|_| {}).await;
    let next = crate::blocks::ironwood_activation() + 1;
    paused
        .store()
        .append(&receiver_store::IndexedBlock {
            height: next,
            hash: [4; 32],
            parent: [3; 32],
            start_position: 0,
            end_position: 0,
            coinbase_actions: 0,
            payments: Vec::new(),
            commitments: Vec::new(),
        })
        .unwrap();
    *paused.node.tip.lock().unwrap() = u64::from(next);
    paused
        .node
        .hashes
        .lock()
        .unwrap()
        .insert(u64::from(next), [4; 32]);
    paused.refresh().await.unwrap();
    assert_eq!(
        paused.publications.filter_window().unwrap().head().first,
        next - 1
    );
    // The same tip's window as the byte bound would leave it, without its first block.
    let hints = paused.store().hints(next, next, usize::MAX).unwrap();
    let genesis = Network::Mainnet.genesis_hash();
    let shortened = receiver_pir_server::blocks::BlockWindow::new(
        genesis,
        0,
        hints.first,
        hints.parent,
        hints.hashes,
        hints.sets,
    )
    .unwrap();
    let epoch = paused.publications.epoch();
    assert!(paused.publications.publish_filters(shortened, epoch));
    let window = paused.publications.filter_window().unwrap();
    assert_eq!(window.head().first, next);
    paused.refresh().await.unwrap();
    assert!(Arc::ptr_eq(
        &window,
        &paused.publications.filter_window().unwrap()
    ));
}

#[cfg(feature = "qualification-tests")]
/// The terminal block's time only dates the optional payout audit: a node that
/// cannot give it degrades the audit report, and with the audit off it is never
/// asked for, so publication goes ahead either way.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unavailable_block_time_never_blocks_publication() {
    let mut paused = Paused::new(|_| {}).await;
    *paused.node.time.lock().unwrap() = i64::MIN;
    paused.refresh().await.unwrap();
    let health = paused.get("health").await;
    assert!(health["serving"].is_string());
    assert!(health["indexer"].is_null(), "{health}");
    paused.args.audit_enabled = false;
    paused.publications.revoke();
    paused.state = paused.fresh_state();
    paused.refresh().await.unwrap();
    let health = paused.get("health").await;
    assert!(health["serving"].is_string());
    assert!(health["indexer"].is_object(), "{health}");
}

#[cfg(feature = "qualification-tests")]
/// Restoring hints after revocation cannot masquerade as a live PIR publication.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unchanged_anchor_rebuilds_revoked_pir() {
    let mut paused = Paused::new(|_| {}).await;
    paused.refresh().await.unwrap();
    let first = paused.get("inventory").await;
    paused.publications.revoke();
    assert!(paused.publications.current_anchor().is_none());
    paused.refresh().await.unwrap();
    assert_eq!(paused.get("inventory").await, first);
    assert!(paused.publications.current_anchor().is_some());
}

#[cfg(feature = "qualification-tests")]
/// Revocation and durable rewind precede collection: a source that cannot supply
/// replacement blocks must not leave the orphaned session available over HTTP.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reorg_revokes_before_replacement_source_failure() {
    let mut paused = Paused::new(|_| {}).await;
    paused.refresh().await.unwrap();
    let height = crate::blocks::ironwood_activation();
    paused
        .node
        .hashes
        .lock()
        .unwrap()
        .insert(u64::from(height), [4; 32]);
    assert!(paused.refresh().await.is_err());
    assert!(paused.publications.anchors().is_empty());
    let response = reqwest::get(format!("{}/v1/receiver/inventory", paused.origin))
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    let store = paused.store();
    assert_eq!(store.tip().unwrap().height, height - 1);
    assert_eq!(store.counts().unwrap(), (0, 0));
}

#[cfg(feature = "qualification-tests")]
/// Invalidation after real preparation must fence activation without discarding
/// the independently committed input. A new pass recovers through the same API.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_pipeline_invalidation_between_preparation_and_activation() {
    let mut paused = Paused::new(|_| {}).await;
    let mut store = paused.store();
    let mut provider = paused.provider();
    let height = crate::blocks::ironwood_activation();
    let work = collect(
        &paused.args,
        &paused.rpc,
        u64::from(height),
        &mut store,
        Some(&paused.publications),
        Network::Mainnet.genesis_hash(),
        std::time::Instant::now(),
    )
    .await
    .unwrap()
    .unwrap();
    let input = work.select(&mut provider, &mut paused.state).await.unwrap();
    let PublicationInput::NewMaterial(publication) = input else {
        panic!("new material expected")
    };
    let prepared = pir_pipeline::engine::PublisherEngine
        .prepare(publication)
        .await
        .unwrap();
    paused.publications.revoke();
    let result = prepared.commit().await;
    assert!(result.is_err());
    assert!(paused.publications.anchors().is_empty());
    assert!(paused.state.active.is_none());
    assert_eq!(store.tip().unwrap().height, height);
    drop(store);
    paused.refresh().await.unwrap();
    assert_eq!(paused.get("inventory").await["end_height"], height);
}

#[cfg(feature = "qualification-tests")]
/// A response admitted under the old revision retains its immutable bytes
/// while the shared lifecycle activates different rows at the next anchor.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_pipeline_handover_keeps_an_admitted_old_publication_response() {
    use sha2::{Digest, Sha256};
    let mut paused = Paused::new(|_| {}).await;
    paused.refresh().await.unwrap();
    let old = paused.segment().await;
    let wallet = crate::lookup::Wallet::at(&paused.origin, "/v1/receiver");
    let (secret, query) = crate::lookup::query(&old.directory, 0, 2048);
    let response = reqwest::Client::new()
        .post(format!("{}/v1/receiver/query", paused.origin))
        .body(query)
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    // Reuse the same valid public record as the preparation/restart case;
    // writable store and publication ownership remain independent.
    let height = crate::blocks::ironwood_activation() + 1;
    let mut record = super::common::record(0, 1);
    record.payment.height = height;
    record.payment.block_hash = [4; 32];
    record.payment.position = 0;
    let expected = record.clone();
    verify::append(
        &mut paused.store(),
        &receiver_store::IndexedBlock {
            height,
            hash: [4; 32],
            parent: [3; 32],
            start_position: 0,
            end_position: 1,
            coinbase_actions: 0,
            commitments: vec![record.payment.cmx],
            payments: vec![(record.receiver, record.payment)],
        },
    )
    .unwrap();
    *paused.node.tip.lock().unwrap() = u64::from(height);
    paused
        .node
        .hashes
        .lock()
        .unwrap()
        .insert(u64::from(height), [4; 32]);
    paused.refresh().await.unwrap();
    let next = paused.segment().await;
    assert_ne!(
        next.directory.manifest.data_sha256,
        old.directory.manifest.data_sha256
    );
    // The old empty directory has an independently known all-zero row oracle.
    let bytes = response.bytes().await.unwrap();
    let mut zero_rows = Sha256::new();
    for _ in 0..receiver::DIRECTORY_ROWS {
        zero_rows.update([0; receiver::ROW_BYTES]);
    }
    assert_eq!(
        <[u8; 32]>::from(zero_rows.finalize()),
        old.directory.manifest.data_sha256
    );
    let public = wallet
        .get(&format!(
            "/public/{}",
            hex::encode(old.directory.id().unwrap())
        ))
        .await;
    // The displaced session still serves its setup within its retention.
    let public = public.unwrap();
    use zakura_pir_client::native::ProfileClientExt as _;
    let row = zakura_pir_client::native::P8
        .decode_cols(&secret, &public, &bytes[52..], receiver::native::COLS)
        .unwrap();
    assert!(row.iter().all(|b| *b == 0));
    // The new publication independently answers the now-present record.
    let found = wallet.lookup(&next, &expected.receiver).await.unwrap();
    assert_eq!(found[0].0.payment, expected.payment);
}

#[cfg(feature = "qualification-tests")]
/// A refresh with nothing new keeps the publication, and a changed audit report
/// replaces only its health without rebuilding it: a completed payout stays pending
/// until the terminal block's time passes its grace, however long the chain pauses.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_paused_chain_refreshes_its_report_without_rebuilding() {
    let mut paused = Paused::new(|_| {}).await;
    // A completed read of the payout feed that saw a payout complete.
    let payout = super::common::receiver();
    paused
        .provider()
        .record(
            "near-payout-audit",
            &[(payout, [7; 32], NOW - 10)],
            NOW - 50,
            NOW - 10,
        )
        .unwrap();
    paused.refresh().await.unwrap();
    let first = paused.get("health").await;
    assert_eq!(first["indexer"]["payouts_missing"], 0);
    paused.refresh().await.unwrap();
    assert_eq!(paused.get("health").await, first);
    assert!(paused.publications.ready_at().is_none());
    // A terminal block past the payout's grace. No table is prepared again, so the
    // same session took the new report in place.
    *paused.node.time.lock().unwrap() = NOW - 10 + 3600;
    let prepared = paused.state.durable.as_ref().unwrap().prepared();
    paused.refresh().await.unwrap();
    assert_eq!(paused.state.durable.as_ref().unwrap().prepared(), prepared);
    let reported = paused.get("health").await;
    assert_eq!(reported["indexer"]["payouts_missing"], 1);
    assert_eq!(reported["serving"], first["serving"]);
    assert!(paused.publications.ready_at().is_none());
}

#[cfg(feature = "qualification-tests")]
/// A node on another network with a higher tip is left out of a refresh, so its
/// chain cannot revoke the served publication.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_on_another_network_cannot_revoke_sessions() {
    let mut paused = Paused::new(|_| {}).await;
    paused.refresh().await.unwrap();
    let anchors = paused.publications.anchors();
    let height = u64::from(crate::blocks::ironwood_activation());
    let other = Node::default();
    *other.tip.lock().unwrap() = height + 10;
    other
        .hashes
        .lock()
        .unwrap()
        .extend((0..=height + 10).map(|h| (h, [9; 32])));
    let nodes = [serve_node(other).await, paused.rpc.clone()];
    let genesis = Network::Mainnet.genesis_hash();
    let publications = Some(&paused.publications);
    refresh(
        &paused.args,
        &nodes,
        genesis,
        publications,
        &mut paused.state,
    )
    .await
    .unwrap();
    assert_eq!(paused.publications.anchors(), anchors);
}

#[cfg(feature = "qualification-tests")]
/// A node that leaves mainnet, or stops giving its genesis, after ranking publishes
/// nothing, one-shot or serving, though its chain still matches the index; one
/// still on mainnet publishes both ways.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_repointed_during_a_pass_publishes_nothing() {
    let mainnet = Network::Mainnet.genesis_hash();
    for repoint in [Some(Some([9; 32])), Some(None), None] {
        let mut paused = Paused::new(|_| {}).await;
        *paused.node.repoint.lock().unwrap() = repoint;
        let rpc = std::slice::from_ref(&paused.rpc);
        let one_shot = refresh(&paused.args, rpc, mainnet, None, &mut paused.state).await;
        assert_eq!(one_shot.is_ok(), repoint.is_none());
        let written = paused.dir.path().join("publications").exists();
        assert_eq!(written, repoint.is_none());
        *paused.node.repoint.lock().unwrap() = repoint;
        paused.node.hashes.lock().unwrap().insert(0, mainnet);
        assert_eq!(paused.refresh().await.is_ok(), repoint.is_none());
        let serving = !paused.publications.anchors().is_empty();
        assert_eq!(serving, repoint.is_none());
        assert_eq!(paused.state.active.is_some(), repoint.is_none());
    }
}

#[cfg(feature = "qualification-tests")]
/// A node that leaves mainnet, or stops giving its genesis, after ranking cannot
/// revoke the served publication with its other chain; one still on mainnet does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_repointed_after_ranking_cannot_revoke_sessions() {
    let mainnet = Network::Mainnet.genesis_hash();
    let height = u64::from(crate::blocks::ironwood_activation());
    for repoint in [Some(Some([9; 32])), Some(None), None] {
        let mut paused = Paused::new(|_| {}).await;
        paused.refresh().await.unwrap();
        // The node now answers from a chain without the served anchor.
        paused.node.hashes.lock().unwrap().insert(height, [8; 32]);
        *paused.node.repoint.lock().unwrap() = repoint;
        let nodes = std::slice::from_ref(&paused.rpc);
        let (tip, rpc) = freshest(nodes, mainnet).await.unwrap();
        let checked = check_serving(&paused.publications, &rpc, mainnet, tip).await;
        assert_eq!(checked.is_ok(), repoint.is_none());
        let serving = !paused.publications.anchors().is_empty();
        assert_eq!(serving, repoint.is_some());
    }
}

#[cfg(feature = "qualification-tests")]
/// A terminal block dated ahead of the wall clock leaves a fresh payout pending.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_future_block_time_reports_no_payout_missing_early() {
    let mut paused = Paused::new(|_| {}).await;
    let (payout, now) = (super::common::receiver(), unix_now());
    paused
        .provider()
        .record(
            "near-payout-audit",
            &[(payout, [7; 32], now - 10)],
            now - 50,
            now - 10,
        )
        .unwrap();
    *paused.node.time.lock().unwrap() = now + 2 * 3600;
    paused.refresh().await.unwrap();
    assert_eq!(paused.get("health").await["indexer"]["payouts_missing"], 0);
}

#[cfg(feature = "qualification-tests")]
/// A stored tip above the requested depth, as after a restart with a larger
/// `--depth`, is published rather than waited past.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_larger_depth_publishes_the_stored_tip() {
    let mut paused = Paused::new(|_| {}).await;
    paused.args.depth = 5;
    paused.refresh().await.unwrap();
    let height = crate::blocks::ironwood_activation();
    assert_eq!(paused.get("inventory").await["end_height"], height);
}

/// One-page segment tables over the fixture coverage, paying `record` with `payload`.
fn tables(record: &zakura_pir_publisher::receiver::RawRecord, payload: [u8; 512]) -> Tables {
    let paid = (record.receiver, record.payment.clone(), payload);
    crate::prepared::tables(crate::prepared::coverage(3), &[paid], 2048)
}

/// A payload supplying a sibling at level 15, which no position below 32,768 has in a
/// tree of fewer leaves, written into `tables`' first slot and rehashed.
fn invalid_proof(mut tables: Tables) -> Tables {
    use sha2::Digest;
    tables.companion.data[zakura_pir_publisher::receiver::RECORD_BYTES + 15 * 32] = 1;
    tables.companion.manifest.data_sha256 = sha2::Sha256::digest(&tables.companion.data).into();
    tables
}

/// Fixture preparation refuses a structurally invalid payload before reusing a cached
/// server, as production preparation does.
#[test]
fn fixture_preparation_returns_protocol_errors_before_reusing_a_server() {
    use zakura_pir_publisher::receiver::session::Error::Directory;
    use zakura_pir_publisher::receiver::Error::Malformed;
    let valid = super::common::record(0, 1);
    assert!(matches!(
        crate::prepared::segment(invalid_proof(tables(&valid, [0; 512]))),
        Err(Directory(Malformed))
    ));
}
