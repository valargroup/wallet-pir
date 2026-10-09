//! A minimal JSON-RPC client for a Zakura node: what the indexer reads.
//!
//! It copies the parts of `enhance-pir-server`'s client the indexer needs, so the two
//! services stay independent until a shared node client exists.
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::json;
use std::path::{Path, PathBuf};
use zakura_chain::block::Hash;

/// A failed node request.
#[derive(Debug, thiserror::Error)]
pub enum ZakuraError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("cookie error: {0}")]
    Cookie(#[from] std::io::Error),
    #[error("invalid RPC cookie")]
    InvalidCookie,
    #[error("RPC returned {0}: {1}")]
    Rpc(i64, String),
    #[error("RPC response is missing a result")]
    MissingResult,
    #[error("invalid block hex: {0}")]
    Hex(#[from] hex::FromHexError),
    #[error("invalid canonical block: {0}")]
    Block(String),
    #[error("Ironwood tree size is unavailable at height {0}")]
    MissingTreeSize(u64),
    #[error("Ironwood tree root is unavailable at height {0}")]
    MissingTreeRoot(u64),
    #[error("RPC response: {0}")]
    Body(#[from] crate::BodyError),
    #[error("invalid RPC response: {0}")]
    Decode(serde_json::Error),
    #[error("node is on another network")]
    OtherNetwork,
}

/// Bound on every node RPC response. The largest the indexer reads is a verbose
/// (verbosity 1) `getblock`, which lists one quoted transaction ID per transaction,
/// allowed 128 bytes with separators and whitespace, for as many of the smallest
/// transactions as fit in a maximum-size block (about 4.7 MB), plus 64 KiB for the
/// envelope and the block's other fields. That exceeds a maximum-size raw block's hex
/// (4 MB), as the assertion below keeps.
const RPC_RESPONSE_BYTES: usize = (zakura_chain::block::MAX_BLOCK_BYTES
    / zakura_chain::transaction::MIN_TRANSPARENT_TX_SIZE)
    as usize
    * 128
    + 64 * 1024;
const _: () =
    assert!(RPC_RESPONSE_BYTES > 2 * zakura_chain::block::MAX_BLOCK_BYTES as usize + 64 * 1024);

/// One node's JSON-RPC endpoint and the path of its `user:password` cookie file, if
/// it requires one.
#[derive(Clone)]
pub struct ZakuraClient {
    http: reqwest::Client,
    url: String,
    cookie: Option<PathBuf>,
}

/// The `user` and `password` of a cookie file.
fn read_cookie(path: &Path) -> Result<(String, String), ZakuraError> {
    let cookie = std::fs::read_to_string(path)?;
    match cookie.trim().split_once(':') {
        Some((username, password)) if !username.is_empty() && !password.is_empty() => {
            Ok((username.to_owned(), password.to_owned()))
        }
        _ => Err(ZakuraError::InvalidCookie),
    }
}

#[derive(Deserialize)]
struct RpcResponse<T> {
    result: Option<T>,
    error: Option<RpcError>,
}

#[derive(Deserialize)]
struct RpcError {
    code: i64,
    message: String,
}

/// The tree sizes from a verbose `getblock` response.
#[derive(Deserialize)]
pub(crate) struct VerboseBlock {
    pub(crate) trees: BlockTrees,
}

/// The note commitment tree sizes after a block.
#[derive(Deserialize)]
pub(crate) struct BlockTrees {
    pub(crate) ironwood: Option<TreeSize>,
}

/// One note commitment tree's size.
#[derive(Deserialize)]
pub(crate) struct TreeSize {
    pub(crate) size: u64,
}

/// The block and Ironwood tree state from a `z_gettreestate` response.
#[derive(Deserialize)]
pub(crate) struct Treestate {
    /// Displayed (reversed) hex, as `getblockhash` gives it.
    pub(crate) hash: String,
    pub(crate) height: u64,
    /// Older nodes omit it.
    #[serde(default)]
    pub(crate) ironwood: Option<PoolTreestate>,
}

/// One pool's tree state.
#[derive(Deserialize)]
pub(crate) struct PoolTreestate {
    pub(crate) commitments: Commitments,
}

/// A tree's root, absent when the node has no state for it.
#[derive(Deserialize)]
pub(crate) struct Commitments {
    #[serde(rename = "finalRoot")]
    pub(crate) final_root: Option<String>,
}

impl ZakuraClient {
    /// A client for the node at `url` that authenticates with the cookie file at
    /// `cookie`, read on every request since the node rotates it when it restarts, or
    /// sends no credentials for `None`, for a node whose RPC disables authentication.
    pub fn new(url: String, cookie: Option<&Path>) -> Result<Self, ZakuraError> {
        Ok(Self {
            http: reqwest::Client::builder()
                .connect_timeout(std::time::Duration::from_secs(5))
                .timeout(std::time::Duration::from_secs(120))
                .build()?,
            url,
            cookie: cookie.map(Path::to_owned),
        })
    }

    /// The `nodes` whose genesis block is `genesis` (see [`Self::check_network`]), each
    /// with its tip, highest first and in `nodes` order among equal tips. A caller runs
    /// a whole pass on the first, so every read in it comes from one node, and the next
    /// pass ranks again, rereading each genesis, since an endpoint can be repointed.
    /// Fails, with the last node's error, only when no node qualifies. Every read is
    /// concurrent, so a stalled node delays ranking by one timeout, not one per node.
    pub async fn ranked(
        nodes: &[ZakuraClient],
        genesis: Hash,
    ) -> Result<Vec<(u64, ZakuraClient)>, ZakuraError> {
        let tips: Vec<_> = nodes
            .iter()
            .map(|node| {
                let node = node.clone();
                tokio::spawn(async move {
                    let (_, tip) =
                        tokio::try_join!(node.check_network(genesis), node.tip_height())?;
                    Ok(tip)
                })
            })
            .collect();
        let (mut ranked, mut last) = (Vec::new(), None);
        for (node, tip) in nodes.iter().zip(tips) {
            match tip.await.expect("tip read task") {
                Ok(tip) => ranked.push((tip, node.clone())),
                Err(error) => last = Some(error),
            }
        }
        if ranked.is_empty() {
            return Err(last.unwrap_or_else(|| ZakuraError::Block("no node RPC endpoint".into())));
        }
        // A stable sort keeps the configured order among equal tips.
        ranked.sort_by_key(|(tip, _)| std::cmp::Reverse(*tip));
        Ok(ranked)
    }

    /// Fails with [`ZakuraError::OtherNetwork`] unless the node's genesis block is
    /// `genesis`, so a node on another network is never taken as evidence about this
    /// one's chain.
    pub async fn check_network(&self, genesis: Hash) -> Result<(), ZakuraError> {
        let node = (self.block_hash(0).await?)
            .parse::<Hash>()
            .map_err(|e| ZakuraError::Block(e.to_string()))?;
        match node == genesis {
            true => Ok(()),
            false => Err(ZakuraError::OtherNetwork),
        }
    }

    /// The node's chain tip height.
    pub async fn tip_height(&self) -> Result<u64, ZakuraError> {
        self.call("getblockcount", json!([])).await
    }

    /// The hash of the node's block at `height`, in RPC display order.
    pub async fn block_hash(&self, height: u64) -> Result<String, ZakuraError> {
        self.call("getblockhash", json!([height])).await
    }

    /// One JSON-RPC call to the node, with the cookie file's current credentials if it
    /// has one. Its response is read up to [`RPC_RESPONSE_BYTES`] within the client's
    /// timeout.
    pub(crate) async fn call<T: DeserializeOwned>(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<T, ZakuraError> {
        let body =
            json!({"jsonrpc": "1.0", "id": "receiver-indexer", "method": method, "params": params});
        let mut request = self.http.post(&self.url);
        if let Some(path) = &self.cookie {
            let (username, password) = read_cookie(path)?;
            request = request.basic_auth(username, Some(password));
        }
        let response = request.json(&body).send().await?.error_for_status()?;
        let body = crate::read_limited(response, RPC_RESPONSE_BYTES).await?;
        let response: RpcResponse<T> =
            serde_json::from_slice(&body).map_err(ZakuraError::Decode)?;
        if let Some(error) = response.error {
            return Err(ZakuraError::Rpc(error.code, error.message));
        }
        response.result.ok_or(ZakuraError::MissingResult)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{extract::State, routing::post, Json, Router};
    use std::sync::{
        atomic::{AtomicU8, Ordering},
        Arc, Mutex,
    };

    /// A node at tip `tip` whose genesis block is `[genesis; 32]`, read on each
    /// request. Returns its URL.
    async fn node(tip: u64, genesis: Arc<AtomicU8>) -> String {
        let app = Router::new().route(
            "/",
            post(move |Json(request): Json<serde_json::Value>| {
                let genesis = genesis.load(Ordering::SeqCst);
                async move {
                    let result = match request["method"].as_str() {
                        Some("getblockhash") => json!(Hash([genesis; 32]).to_string()),
                        _ => json!(tip),
                    };
                    Json(json!({"result": result, "error": null}))
                }
            }),
        );
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", socket.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
        url
    }

    /// A node at tip 7 that accepts only the cookie `accepted` holds. Returns its URL.
    async fn cookie_node(accepted: Arc<Mutex<String>>) -> String {
        async fn handler(
            State(accepted): State<Arc<Mutex<String>>>,
            headers: axum::http::HeaderMap,
        ) -> axum::response::Response {
            use axum::response::IntoResponse;
            let cookie = accepted.lock().unwrap().clone();
            let (username, password) = cookie.split_once(':').unwrap();
            // The header reqwest sends for these credentials.
            let expected = reqwest::Client::new()
                .post("http://node")
                .basic_auth(username, Some(password))
                .build()
                .unwrap()
                .headers()[axum::http::header::AUTHORIZATION]
                .clone();
            if headers.get(axum::http::header::AUTHORIZATION) != Some(&expected) {
                return axum::http::StatusCode::UNAUTHORIZED.into_response();
            }
            Json(json!({"result": 7, "error": null})).into_response()
        }
        let app = Router::new().route("/", post(handler)).with_state(accepted);
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", socket.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
        url
    }

    /// Each request reads the cookie file, so a client follows a rotated cookie and
    /// recovers once a missing one is replaced.
    #[tokio::test]
    async fn a_rotated_cookie_is_reread() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".cookie");
        let accepted = Arc::new(Mutex::new(String::new()));
        let rpc = ZakuraClient::new(cookie_node(accepted.clone()).await, Some(&path)).unwrap();
        let rotate = |cookie: &str| {
            *accepted.lock().unwrap() = cookie.to_owned();
            std::fs::write(&path, cookie).unwrap();
        };
        rotate("user:a");
        assert_eq!(rpc.tip_height().await.unwrap(), 7);
        rotate("user:b");
        assert_eq!(rpc.tip_height().await.unwrap(), 7);
        // A node restarting removes its cookie before writing the next one.
        std::fs::remove_file(&path).unwrap();
        assert!(matches!(
            rpc.tip_height().await,
            Err(ZakuraError::Cookie(_))
        ));
        rotate("user:c");
        assert_eq!(rpc.tip_height().await.unwrap(), 7);
    }

    /// Nodes rank by tip, highest first and in configured order among equal tips,
    /// leaving out a node that does not answer or, checked again on every ranking, is
    /// on another network, however high its tip; with none left ranking fails.
    #[tokio::test]
    async fn nodes_rank_by_tip() {
        let ours = Hash([1; 32]);
        let client = |url: String| ZakuraClient::new(url, None).unwrap();
        let on = |genesis: u8| Arc::new(AtomicU8::new(genesis));
        let down = client("http://127.0.0.1:1".into());
        let other = client(node(200, on(2)).await);
        let repointed = on(1);
        let nodes = [
            down.clone(),
            other.clone(),
            client(node(100, on(1)).await),
            client(node(105, on(1)).await),
            client(node(105, on(1)).await),
            client(node(150, repointed.clone()).await),
        ];
        let order = |ranked: Vec<(u64, ZakuraClient)>| -> Vec<_> {
            ranked
                .into_iter()
                .map(|(tip, node)| (tip, node.url))
                .collect()
        };
        let urls: Vec<_> = nodes.iter().map(|node| node.url.clone()).collect();
        let ranked = ZakuraClient::ranked(&nodes, ours).await.unwrap();
        let mut expected = vec![
            (150, urls[5].clone()),
            (105, urls[3].clone()),
            (105, urls[4].clone()),
            (100, urls[2].clone()),
        ];
        assert_eq!(order(ranked), expected);
        repointed.store(2, Ordering::SeqCst);
        expected.remove(0);
        let ranked = ZakuraClient::ranked(&nodes, ours).await.unwrap();
        assert_eq!(order(ranked), expected);
        assert!(ZakuraClient::ranked(&[down, other], ours).await.is_err());
    }
}
