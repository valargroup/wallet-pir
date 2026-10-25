//! Receiver qualification using the root publisher, the shared TLS terminator,
//! and unchanged wallet clients. The node only supplies synthetic block envelopes.
#![allow(dead_code)] // Shared by the lifecycle and all-products composition cases.
use axum::{extract::State, routing::post, Json, Router};
use serde_json::{json, Value};
use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};
use zakura_server_chain::{testing::receiver::RefundBlock, Network};
#[path = "../../../../src/receiver/receiver-store/tests/common/mod.rs"]
mod common;
#[path = "../../../../src/receiver/receiver-pir-server/tests/common/history.rs"]
mod history;
use super::{
    query_host::{free, Host},
    tls::Tls,
};

pub fn activation() -> u32 {
    Network::Mainnet.ironwood_activation()
}

#[derive(Clone)]
struct Chain {
    blocks: Arc<Mutex<Vec<RefundBlock>>>,
    unavailable: Arc<AtomicBool>,
}
async fn node(State(chain): State<Chain>, Json(request): Json<Value>) -> Json<Value> {
    if chain.unavailable.load(Ordering::SeqCst) {
        return Json(json!({"result":null,"error":{"code":-8,"message":"fixture unavailable"}}));
    }
    let blocks = chain.blocks.lock().unwrap();
    let parameter = &request["params"][0];
    let height = parameter
        .as_u64()
        .or_else(|| parameter.as_str().and_then(|s| s.parse().ok()));
    let block = blocks.iter().enumerate().find(|(index, block)| {
        height == Some(u64::from(activation()) + *index as u64)
            || parameter.as_str() == Some(&block.display_hash())
    });
    let result = match request["method"].as_str().unwrap() {
        "getblockcount" => json!(activation() as usize + blocks.len() - 1),
        "getblockhash" if height == Some(0) => json!(zakura_server_chain::hash::display(
            &Network::Mainnet.genesis_hash()
        )),
        "getblockhash" if height == Some(u64::from(activation() - 1)) => json!("00".repeat(32)),
        "getblockhash" => json!(block.expect("fixture block height").1.display_hash()),
        "getblock" if request["params"][1] == 1 => {
            let through = block.expect("fixture block hash").0;
            let size: usize = blocks[..=through]
                .iter()
                .map(|block| block.commitments().len())
                .sum();
            json!({"trees":{"ironwood":{"size":size}}})
        }
        "getblock" => json!(block.expect("fixture raw block").1.raw_hex()),
        "getblockheader" => json!({"time":1_800_000_000}),
        method => panic!("unexpected Receiver node method {method}"),
    };
    Json(json!({"result":result,"error":null,"id":request["id"]}))
}

pub struct Receiver {
    root: PathBuf,
    _scratch: Option<tempfile::TempDir>,
    chain: Chain,
    node_task: tokio::task::JoinHandle<()>,
    pub tls: Tls,
    publisher: tokio::process::Child,
    config: PathBuf,
    pub management: String,
}
impl Receiver {
    pub async fn start() -> Self {
        let scratch = tempfile::tempdir().unwrap();
        let (root, scratch) = match std::env::var_os("WALLET_PIR_CAMPAIGN_OUT") {
            Some(output) => {
                let path = tempfile::Builder::new()
                    .prefix("receiver-publisher-")
                    .tempdir_in(output)
                    .unwrap()
                    .keep();
                (path, None)
            }
            None => (scratch.path().to_owned(), Some(scratch)),
        };
        let first = RefundBlock::new(activation());
        let second = first
            .clone()
            .coinbase_only()
            .at_height(activation() + 1)
            .after(first.hash());
        let third = second
            .clone()
            .at_height(activation() + 2)
            .after(second.hash());
        let chain = Chain {
            blocks: Arc::new(Mutex::new(vec![first, second, third])),
            unavailable: Arc::new(AtomicBool::new(false)),
        };
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let rpc = format!("http://{}", socket.local_addr().unwrap());
        let app = Router::new()
            .route("/", post(node))
            .with_state(chain.clone());
        let node_task = tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
        let management = free();
        let listen = free();
        let config = root.as_path().join("publisher.json");
        std::fs::write(
            &config,
            serde_json::to_vec(&json!({
                "management_listen": management,
                "chain": {"rpc_url":rpc,"no_auth":true,"poll_ms":100},
                "receiver":{"enabled":true,"settings":{
                    "data_dir":root.as_path().join("state"),"bind":listen,"min_rows":8192,
                    "shard_rows":8192,"filter_blocks":32
                }}
            }))
            .unwrap(),
        )
        .unwrap();
        let publisher = Self::spawn(&config, root.as_path());
        let tls =
            Tls::with_discovery(&format!("http://{listen}"), &format!("http://{management}")).await;
        let mut result = Self {
            root,
            _scratch: scratch,
            chain,
            node_task,
            tls,
            publisher,
            config,
            management,
        };
        result.ready().await;
        result
    }
    fn spawn(config: &Path, root: &Path) -> tokio::process::Child {
        let log = std::fs::File::create(root.join("publisher.log")).unwrap();
        tokio::process::Command::new(
            std::env::var_os("WALLET_PIR_PUBLISHER_BIN")
                .unwrap_or_else(|| env!("CARGO_BIN_EXE_pir-publisher-test").into()),
        )
        .arg("--config")
        .arg(config)
        .env("RUST_LOG", "info")
        .stdout(log.try_clone().unwrap())
        .stderr(log)
        .kill_on_drop(true)
        .spawn()
        .unwrap()
    }
    pub async fn ready(&mut self) {
        for _ in 0..1200 {
            if reqwest::get(format!("http://{}/ready", self.management))
                .await
                .is_ok_and(|r| r.status().is_success())
            {
                return;
            }
            assert!(
                self.publisher.try_wait().unwrap().is_none(),
                "{}",
                std::fs::read_to_string(self.root.as_path().join("publisher.log")).unwrap()
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        panic!(
            "Receiver publisher did not become ready: {}",
            std::fs::read_to_string(self.root.as_path().join("publisher.log")).unwrap()
        );
    }
    pub fn section(&self, root: &Path) -> Value {
        json!({"enabled":true,"settings":{
            "publisher":self.tls.origin,"publisher_ca":self.tls.certificate,
            "state_dir":root.join("receiver"),"genesis":Network::Mainnet.genesis_hash(),
            "activation_height":activation(),"poll_seconds":1,"table_memory_mib":1536,
            "bootstrap_max_bytes":256<<20,"retention_seconds":600
        }})
    }
    pub fn query_document(&self, root: &Path) -> Value {
        json!({"public_listen":free(),"management_listen":free(),"receiver":self.section(root)})
    }
    pub async fn query(&self, root: &Path, name: &str) -> (Host, Value) {
        std::fs::create_dir_all(root).unwrap();
        let document = self.query_document(root);
        let mut host = Host::start(root, name, document.clone()).await;
        host.ready().await;
        (host, document)
    }
    pub fn anchor(&self) -> (u32, [u8; 32]) {
        let blocks = self.chain.blocks.lock().unwrap();
        let index = blocks.len() - 3;
        (activation() + index as u32, blocks[index].hash())
    }
    pub fn append(&self) {
        let mut blocks = self.chain.blocks.lock().unwrap();
        let last = blocks.last().unwrap();
        let next = last
            .clone()
            .at_height(activation() + blocks.len() as u32)
            .after(last.hash());
        blocks.push(next);
    }
    pub fn reorg(&self) {
        let mut blocks = self.chain.blocks.lock().unwrap();
        blocks[0] = blocks[0].clone().renonced();
        for i in 1..blocks.len() {
            blocks[i] = blocks[i].clone().after(blocks[i - 1].hash());
        }
    }
    pub async fn restart(&mut self, revision: u64) {
        self.publisher.kill().await.unwrap();
        self.publisher.wait().await.unwrap();
        let mut config: Value =
            serde_json::from_slice(&std::fs::read(&self.config).unwrap()).unwrap();
        config["receiver"]["settings"]["history_revision"] = revision.into();
        std::fs::write(&self.config, serde_json::to_vec(&config).unwrap()).unwrap();
        self.publisher = Self::spawn(&self.config, self.root.as_path());
        self.ready().await;
    }
    /// Obstruct only preparation writes; completed immutable inputs stay readable.
    pub fn block_preparation(&self, blocked: bool) {
        let tables = self.root.as_path().join("state/tables");
        if blocked {
            // Existing table files keep their paths. Denying directory creation
            // for each new output uses filesystem permissions, not a service hook.
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&tables, std::fs::Permissions::from_mode(0o500)).unwrap();
        } else {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&tables, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
    }
    /// Fail the publisher's next durable authority replacement without changing
    /// the node or any immutable object. Restoring writes must permit recovery.
    pub fn block_authority_write(&self, blocked: bool) {
        let path = self.root.join("state/distribution/authority.json");
        let saved = path.with_extension("saved");
        if blocked {
            std::fs::rename(&path, &saved).unwrap();
            std::fs::create_dir(&path).unwrap();
        } else {
            std::fs::remove_dir(&path).unwrap();
            std::fs::rename(&saved, &path).unwrap();
        }
    }
    pub fn transport_outage(&self, unavailable: bool) {
        std::fs::write(
            &self.tls.control,
            if unavailable { "refused-body" } else { "" },
        )
        .unwrap();
    }
    pub async fn wallet(&self, origin: &str) {
        let (height, hash) = self.anchor();
        let http = Http(reqwest::Client::new());
        let session = history::connect(
            origin,
            &http,
            history::Accepted {
                genesis: Network::Mainnet.genesis_hash(),
                required_start: activation(),
                height,
                hash,
            },
        )
        .await
        .unwrap();
        let record = session.page(common::receiver(), 0).await.unwrap().unwrap();
        let first = self.chain.blocks.lock().unwrap()[0].clone();
        assert_eq!(record.payment.height, activation());
        assert_eq!(record.payment.block_hash, first.hash());
        assert_eq!(record.payment.txid, first.txids()[1]);
        assert_eq!(record.payment.position, 1);
        assert_eq!(record.total, 1, "fixture contains one non-coinbase payment");
        let mut absent = common::receiver();
        absent[0] ^= 1;
        assert!(session.page(absent, 0).await.unwrap().is_none());
        let blocks = self.chain.blocks.lock().unwrap();
        let leaves: Vec<_> = blocks[..=(height - activation()) as usize]
            .iter()
            .flat_map(|b| b.commitments())
            .collect();
        drop(blocks);
        let expected = zakura_pir_publisher::receiver::witness::payloads(&leaves, &[1]).unwrap();
        assert_eq!(
            session.witness(record.ordinal).await.unwrap(),
            expected[0],
            "wallet witness matches a fresh independent full-tree build"
        );
    }
}
impl Drop for Receiver {
    fn drop(&mut self) {
        self.node_task.abort();
    }
}
pub struct Http(pub reqwest::Client);
impl zakura_pir_client::receiver::transport::Transport for Http {
    async fn get(
        &self,
        url: &str,
        limit: usize,
    ) -> Result<Vec<u8>, zakura_pir_query_server::receiver::session::Error> {
        read(self.0.get(url), limit).await
    }
    async fn post(
        &self,
        url: &str,
        body: Vec<u8>,
        limit: usize,
    ) -> Result<Vec<u8>, zakura_pir_query_server::receiver::session::Error> {
        read(self.0.post(url).body(body), limit).await
    }
}
async fn read(
    request: reqwest::RequestBuilder,
    limit: usize,
) -> Result<Vec<u8>, zakura_pir_query_server::receiver::session::Error> {
    use zakura_pir_query_server::receiver::session::Error;
    let mut response = request
        .send()
        .await
        .map_err(|e| Error::Transport(e.to_string()))?;
    if !response.status().is_success() {
        return Err(Error::Transport(response.status().to_string()));
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| Error::Transport(e.to_string()))?
    {
        if bytes.len() + chunk.len() > limit {
            return Err(Error::Malformed);
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}
