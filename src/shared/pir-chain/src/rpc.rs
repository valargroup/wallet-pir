//! A JSON-RPC client for a Zakura node.
//!
//! Two configurations keep the services' existing behaviour: [`NodeClient::new`]
//! (the receiver indexer's) rereads a cookie file on every request, since the node
//! rotates it when it restarts, and bounds every response; and
//! [`NodeClient::from_cookie_file`] (Enhance's and Status's) reads the cookie once
//! and reports a refused credential as [`NodeError::InvalidCookie`]. Transport,
//! authentication and framing live in `pir-node-rpc`; this facade adds chain
//! interpretation and maps transport errors onto [`NodeError`].
use pir_node_rpc::{Credentials, HttpPolicy, RpcClient, RpcError};
use serde::de::DeserializeOwned;
use serde_json::json;
use std::path::Path;

use crate::NodeError;

/// Bound on every response of a [`NodeClient::new`] client. The largest the receiver
/// indexer reads is a verbose (verbosity 1) `getblock`, which lists one quoted
/// transaction ID per transaction, allowed 128 bytes with separators and whitespace,
/// for as many of the smallest transactions as fit in a maximum-size block (about
/// 4.7 MB), plus 64 KiB for the envelope and the block's other fields. That exceeds a
/// maximum-size raw block's hex (4 MB), as the assertion below keeps.
pub const RPC_RESPONSE_BYTES: usize =
    (crate::chain::MAX_BLOCK_BYTES / crate::chain::MIN_TRANSPARENT_TX_SIZE) as usize * 128
        + 64 * 1024;
const _: () = assert!(RPC_RESPONSE_BYTES > 2 * crate::chain::MAX_BLOCK_BYTES as usize + 64 * 1024);

const RECEIVER_POLICY: HttpPolicy = HttpPolicy {
    connection_timeout: Some(std::time::Duration::from_secs(20)),
    request_timeout: std::time::Duration::from_secs(120),
    response_limit: RPC_RESPONSE_BYTES,
};
/// Enhance's, Status's and Transparent's policy, shared with the root follower.
const FIXED_COOKIE_POLICY: HttpPolicy = HttpPolicy::SHARED;

impl From<RpcError> for NodeError {
    fn from(error: RpcError) -> Self {
        match error {
            RpcError::Http(error) => NodeError::Http(error),
            RpcError::Cookie(error) => NodeError::Cookie(error),
            RpcError::InvalidCookie => NodeError::InvalidCookie,
            RpcError::Rpc(code, message) => NodeError::Rpc(code, message),
            RpcError::MissingResult => NodeError::MissingResult,
            RpcError::Oversized(limit) => NodeError::Oversized(limit),
            RpcError::Decode(error) => NodeError::Decode(error),
            RpcError::Batch(message) => NodeError::Batch(message),
        }
    }
}

/// One node's JSON-RPC endpoint and how to authenticate to it.
#[derive(Clone)]
pub struct NodeClient {
    rpc: RpcClient,
}

impl NodeClient {
    /// A client for the node at `url` that authenticates with the cookie file at
    /// `cookie`, read on every request since the node rotates it when it restarts, or
    /// sends no credentials for `None`, for a node whose RPC disables authentication.
    /// Every response is read up to [`RPC_RESPONSE_BYTES`].
    pub fn new(url: String, cookie: Option<&Path>) -> Result<Self, NodeError> {
        Self::configured(
            url,
            cookie.map_or(Credentials::None, |path| {
                Credentials::CookieFile(path.to_owned())
            }),
            "receiver-indexer",
            RECEIVER_POLICY,
        )
    }

    /// A client for the node at `rpc_url` with the credentials the cookie file at
    /// `cookie_path` holds now. A refused credential is [`NodeError::InvalidCookie`].
    /// Singles and whole batches are limited to 64 MiB per response.
    pub fn from_cookie_file(
        rpc_url: impl Into<String>,
        cookie_path: impl AsRef<Path>,
    ) -> Result<Self, NodeError> {
        let credentials = Credentials::read_once(cookie_path.as_ref())?;
        Self::configured(
            rpc_url.into(),
            credentials,
            "enhance-pir",
            FIXED_COOKIE_POLICY,
        )
    }

    fn configured(
        url: String,
        credentials: Credentials,
        id: &'static str,
        policy: HttpPolicy,
    ) -> Result<Self, NodeError> {
        Ok(Self {
            rpc: RpcClient::new(url, credentials, id, policy)?,
        })
    }

    /// This client, naming itself `id` in its JSON-RPC requests, as the node logs.
    pub fn with_request_id(self, id: &'static str) -> Self {
        Self {
            rpc: self.rpc.with_request_id(id),
        }
    }

    /// The endpoint this client calls.
    pub fn url(&self) -> &str {
        self.rpc.url()
    }

    /// The `nodes` whose genesis block is `genesis` (see [`Self::check_network`]), each
    /// with its tip, highest first and in `nodes` order among equal tips. A caller runs
    /// a whole pass on the first, so every read in it comes from one node, and the next
    /// pass ranks again, rereading each genesis, since an endpoint can be repointed.
    /// Fails, with the last node's error, only when no node qualifies. Every read is
    /// concurrent, so a stalled node delays ranking by one timeout, not one per node.
    pub async fn ranked(
        nodes: &[NodeClient],
        genesis: [u8; 32],
    ) -> Result<Vec<(u64, NodeClient)>, NodeError> {
        let tips: Vec<_> = nodes
            .iter()
            .map(|node| {
                let node = node.clone();
                tokio::spawn(async move {
                    let (_, tip) =
                        tokio::try_join!(node.check_network(genesis), node.tip_height())?;
                    Ok::<_, NodeError>(tip)
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
            return Err(last.unwrap_or_else(|| NodeError::Block("no node RPC endpoint".into())));
        }
        // A stable sort keeps the configured order among equal tips.
        ranked.sort_by_key(|(tip, _)| std::cmp::Reverse(*tip));
        Ok(ranked)
    }

    /// Fails with [`NodeError::OtherNetwork`] unless the node's genesis block is
    /// `genesis` (protocol byte order), so a node on another network is never taken as
    /// evidence about this one's chain.
    pub async fn check_network(&self, genesis: [u8; 32]) -> Result<(), NodeError> {
        match self.block_hash(0).await? == genesis {
            true => Ok(()),
            false => Err(NodeError::OtherNetwork),
        }
    }

    /// The node's chain tip height.
    pub async fn tip_height(&self) -> Result<u64, NodeError> {
        Ok(self.rpc.tip_height().await?)
    }

    /// The hash, in protocol byte order, of the node's block at `height`.
    pub async fn block_hash(&self, height: u64) -> Result<[u8; 32], NodeError> {
        crate::hash::parse_display(&self.block_hash_display(height).await?)
    }

    /// The hash of the node's block at `height`, in RPC display order.
    pub async fn block_hash_display(&self, height: u64) -> Result<String, NodeError> {
        Ok(self.rpc.block_hash_display(height).await?)
    }

    /// The node's raw `getblock` bytes for `block`, a height or a display-order hash.
    pub(crate) async fn raw_block(&self, block: String) -> Result<Vec<u8>, NodeError> {
        let raw: String = self.call("getblock", json!([block, 0])).await?;
        Ok(hex::decode(raw)?)
    }

    /// One JSON-RPC 1.0 call to the node.
    pub(crate) async fn call<T: DeserializeOwned>(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<T, NodeError> {
        Ok(self.rpc.call(method, params).await?)
    }

    /// One JSON-RPC 2.0 batch, results placed by their echoed id; see
    /// [`RpcClient::batch_mixed`].
    #[cfg(any(test, feature = "enhance", feature = "transparent"))]
    pub(crate) async fn batch<T: DeserializeOwned>(
        &self,
        method: &str,
        params: Vec<serde_json::Value>,
    ) -> Result<Vec<Result<T, (i64, String)>>, NodeError> {
        Ok(self.rpc.batch(method, params).await?)
    }

    /// [`Self::batch`] for calls of any methods, each given as
    /// `{"method": ..., "params": ...}`.
    #[cfg(any(test, feature = "enhance", feature = "transparent"))]
    pub(crate) async fn batch_mixed<T: DeserializeOwned>(
        &self,
        calls: Vec<serde_json::Value>,
    ) -> Result<Vec<Result<T, (i64, String)>>, NodeError> {
        Ok(self.rpc.batch_mixed(calls).await?)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use axum::{Json, Router, extract::State, routing::post};
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicU8, Ordering},
    };

    /// Serves `app` on a local port. Returns its URL.
    pub(crate) async fn serve(app: Router) -> String {
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", socket.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
        url
    }

    /// A node at tip `tip` whose genesis block is `[genesis; 32]`, read on each
    /// request. Returns its URL.
    async fn node(tip: u64, genesis: Arc<AtomicU8>) -> String {
        serve(Router::new().route(
            "/",
            post(move |Json(request): Json<serde_json::Value>| {
                let genesis = genesis.load(Ordering::SeqCst);
                async move {
                    let result = match request["method"].as_str() {
                        Some("getblockhash") => json!(crate::hash::display(&[genesis; 32])),
                        _ => json!(tip),
                    };
                    Json(json!({"result": result, "error": null}))
                }
            }),
        ))
        .await
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
        serve(Router::new().route("/", post(handler)).with_state(accepted)).await
    }

    /// Each request reads the cookie file, so a client follows a rotated cookie and
    /// recovers once a missing one is replaced.
    #[tokio::test]
    async fn a_rotated_cookie_is_reread() {
        let dir = std::env::temp_dir().join(format!("cookie-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(".cookie");
        let accepted = Arc::new(Mutex::new(String::new()));
        let rpc = NodeClient::new(cookie_node(accepted.clone()).await, Some(&path)).unwrap();
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
        assert!(matches!(rpc.tip_height().await, Err(NodeError::Cookie(_))));
        rotate("user:c");
        assert_eq!(rpc.tip_height().await.unwrap(), 7);
        // A client that read its cookie once reports the node's refusal as a bad
        // cookie once the node rotates it.
        let fixed = NodeClient::from_cookie_file(rpc.url(), &path).unwrap();
        assert_eq!(fixed.tip_height().await.unwrap(), 7);
        rotate("user:d");
        assert!(matches!(
            fixed.tip_height().await,
            Err(NodeError::InvalidCookie)
        ));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// Nodes rank by tip, highest first and in configured order among equal tips,
    /// leaving out a node that does not answer or, checked again on every ranking, is
    /// on another network, however high its tip; with none left ranking fails.
    #[tokio::test]
    async fn nodes_rank_by_tip() {
        let ours = [1; 32];
        let client = |url: String| NodeClient::new(url, None).unwrap();
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
        let order = |ranked: Vec<(u64, NodeClient)>| -> Vec<_> {
            ranked
                .into_iter()
                .map(|(tip, node)| (tip, node.url().to_owned()))
                .collect()
        };
        let urls: Vec<_> = nodes.iter().map(|node| node.url().to_owned()).collect();
        let ranked = NodeClient::ranked(&nodes, ours).await.unwrap();
        let mut expected = vec![
            (150, urls[5].clone()),
            (105, urls[3].clone()),
            (105, urls[4].clone()),
            (100, urls[2].clone()),
        ];
        assert_eq!(order(ranked), expected);
        repointed.store(2, Ordering::SeqCst);
        expected.remove(0);
        let ranked = NodeClient::ranked(&nodes, ours).await.unwrap();
        assert_eq!(order(ranked), expected);
        assert!(NodeClient::ranked(&[down, other], ours).await.is_err());
    }

    /// A bounded client refuses a body over its bound, declared or not.
    #[tokio::test]
    async fn bounded_clients_refuse_oversized_answers() {
        let huge = "0".repeat(RPC_RESPONSE_BYTES);
        let url = serve(Router::new().route(
            "/",
            post(move || {
                let huge = huge.clone();
                async move { Json(json!({"result": huge, "error": null})) }
            }),
        ))
        .await;
        let rpc = NodeClient::new(url, None).unwrap();
        assert!(matches!(
            rpc.block_hash_display(1).await,
            Err(NodeError::Oversized(RPC_RESPONSE_BYTES))
        ));
    }
    /// Transport, framing, deadlines and batches are covered by `pir-node-rpc`; the
    /// facade keeps each client's policy.
    #[test]
    fn production_deadlines_and_response_bounds_are_explicit() {
        use std::time::Duration;
        assert_eq!(
            RECEIVER_POLICY.connection_timeout,
            Some(Duration::from_secs(20))
        );
        assert_eq!(RECEIVER_POLICY.request_timeout, Duration::from_secs(120));
        assert_eq!(RECEIVER_POLICY.response_limit, RPC_RESPONSE_BYTES);
        assert_eq!(FIXED_COOKIE_POLICY.connection_timeout, None);
        assert_eq!(
            FIXED_COOKIE_POLICY.request_timeout,
            Duration::from_secs(120)
        );
        assert_eq!(FIXED_COOKIE_POLICY.response_limit, 64 * 1024 * 1024);
    }
}
