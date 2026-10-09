//! A minimal JSON-RPC client for a Zakura node: what the indexer reads.
//!
//! It copies the parts of `enhance-pir-server`'s client the indexer needs, so the two
//! services stay independent until a shared node client exists.
use reqwest::StatusCode;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::json;
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

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
    #[error("no node has reached height {0}")]
    Behind(u64),
}

/// Nodes' JSON-RPC endpoints, tried in order, and their cookie, if they require one.
#[derive(Clone)]
pub struct ZakuraClient {
    http: reqwest::Client,
    rpc_urls: Vec<String>,
    cookie: Option<Arc<Cookie>>,
}

/// A node's `user:password` cookie file and the credentials last read from it, shared
/// by a client's clones. A node rotates its cookie when it restarts, so a rejected
/// request rereads the file (see [`ZakuraClient::call_at`]).
struct Cookie {
    path: PathBuf,
    /// `None` after a failed read, so the next request reads the file again.
    credentials: Mutex<Option<(String, String)>>,
}

impl Cookie {
    /// The credentials in memory, or the file's if a read failed since.
    fn current(&self) -> Result<(String, String), ZakuraError> {
        let credentials = self.credentials.lock().unwrap().clone();
        credentials.map_or_else(|| self.reload(), Ok)
    }

    /// Rereads the file, replacing the credentials in memory.
    fn reload(&self) -> Result<(String, String), ZakuraError> {
        let read = read_cookie(&self.path);
        *self.credentials.lock().unwrap() = read.as_ref().ok().cloned();
        read
    }
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

impl ZakuraClient {
    /// A client that authenticates with the node's `user:password` cookie file, which
    /// must be readable now and is reread whenever a node rejects it.
    pub fn from_cookie_file(
        rpc_urls: Vec<String>,
        cookie_path: impl AsRef<Path>,
    ) -> Result<Self, ZakuraError> {
        let path = cookie_path.as_ref().to_owned();
        let credentials = Mutex::new(Some(read_cookie(&path)?));
        Self::with_cookie(rpc_urls, Some(Arc::new(Cookie { path, credentials })))
    }

    /// A client for explicitly selected nodes whose RPC disables authentication.
    pub fn unauthenticated(rpc_urls: Vec<String>) -> Result<Self, ZakuraError> {
        Self::with_cookie(rpc_urls, None)
    }

    /// See [`Self::from_cookie_file`] and [`Self::unauthenticated`].
    fn with_cookie(
        rpc_urls: Vec<String>,
        cookie: Option<Arc<Cookie>>,
    ) -> Result<Self, ZakuraError> {
        if rpc_urls.is_empty() {
            return Err(ZakuraError::Block("no node RPC endpoint".into()));
        }
        Ok(Self {
            http: reqwest::Client::builder()
                .connect_timeout(std::time::Duration::from_secs(5))
                .timeout(std::time::Duration::from_secs(120))
                .build()?,
            rpc_urls,
            cookie,
        })
    }

    /// The highest chain tip among the nodes that answer, so a lagging first node
    /// cannot hide blocks a later one has.
    pub async fn tip_height(&self) -> Result<u64, ZakuraError> {
        let (mut best, mut last) = (None, None);
        for url in &self.rpc_urls {
            match self.call_at::<u64>(url, "getblockcount", &json!([])).await {
                Ok(tip) => best = best.max(Some(tip)),
                Err(error) => last = Some(error),
            }
        }
        best.ok_or_else(|| last.expect("at least one node RPC endpoint"))
    }

    /// The hash of the block at `height`, in RPC display order, from the node with the
    /// highest tip that has reached it (the next highest if that call fails), so a node
    /// on a stale fork just behind the tip cannot pin the chain the directory follows.
    pub async fn block_hash(&self, height: u64) -> Result<String, ZakuraError> {
        let mut last = ZakuraError::Behind(height);
        let mut reached = Vec::new();
        for (order, url) in self.rpc_urls.iter().enumerate() {
            match self.call_at::<u64>(url, "getblockcount", &json!([])).await {
                Ok(tip) if tip >= height => reached.push((std::cmp::Reverse(tip), order, url)),
                Ok(_) => last = ZakuraError::Behind(height),
                Err(error) => last = error,
            }
        }
        reached.sort();
        for (_, _, url) in reached {
            match self.call_at(url, "getblockhash", &json!([height])).await {
                Ok(hash) => return Ok(hash),
                Err(error) => last = error,
            }
        }
        Err(last)
    }

    /// One JSON-RPC call to the first node that answers it. Callers validate answers
    /// against each other (batch links and anchors), so nodes may differ between calls.
    pub(crate) async fn call<T: DeserializeOwned>(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<T, ZakuraError> {
        let mut last = None;
        for url in &self.rpc_urls {
            match self.call_at(url, method, &params).await {
                Ok(result) => return Ok(result),
                Err(error) => last = Some(error),
            }
        }
        Err(last.expect("at least one node RPC endpoint"))
    }

    /// One JSON-RPC call to `url`, authenticated unless the node disables
    /// authentication. A rejected request rereads the cookie and, if it changed, is
    /// retried once with the new one.
    async fn call_at<T: DeserializeOwned>(
        &self,
        url: &str,
        method: &str,
        params: &serde_json::Value,
    ) -> Result<T, ZakuraError> {
        let body =
            json!({"jsonrpc": "1.0", "id": "receiver-indexer", "method": method, "params": params});
        let send = |credentials: Option<&(String, String)>| {
            let mut request = self.http.post(url);
            if let Some((username, password)) = credentials {
                request = request.basic_auth(username, Some(password));
            }
            request.json(&body).send()
        };
        let credentials = self.cookie.as_ref().map(|c| c.current()).transpose()?;
        let mut response = send(credentials.as_ref()).await?;
        if response.status() == StatusCode::UNAUTHORIZED {
            let Some(cookie) = &self.cookie else {
                return Err(ZakuraError::InvalidCookie);
            };
            let fresh = cookie.reload()?;
            if Some(&fresh) == credentials.as_ref() {
                return Err(ZakuraError::InvalidCookie);
            }
            response = send(Some(&fresh)).await?;
            if response.status() == StatusCode::UNAUTHORIZED {
                return Err(ZakuraError::InvalidCookie);
            }
        }
        let response: RpcResponse<T> = response.error_for_status()?.json().await?;
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
    use serde_json::Value;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A node at tip `tip` whose block hashes are `fill` repeated, refusing heights
    /// above its tip as nodes do. Returns its URL.
    async fn node(tip: u64, fill: &'static str) -> String {
        async fn handler(
            State((tip, fill)): State<(u64, &'static str)>,
            Json(request): Json<Value>,
        ) -> Json<Value> {
            Json(match request["method"].as_str().unwrap() {
                "getblockcount" => json!({"result": tip, "error": null}),
                _ if request["params"][0].as_u64().unwrap() > tip => {
                    json!({"result": null, "error": {"code": -8, "message": "out of range"}})
                }
                _ => json!({"result": fill.repeat(64), "error": null}),
            })
        }
        let app = Router::new()
            .route("/", post(handler))
            .with_state((tip, fill));
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", socket.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
        url
    }

    /// A node at tip 7 that accepts only the cookie `accepted` holds, counting its
    /// requests. Returns its URL.
    async fn cookie_node(accepted: Arc<Mutex<String>>, requests: Arc<AtomicUsize>) -> String {
        type Node = (Arc<Mutex<String>>, Arc<AtomicUsize>);
        async fn handler(
            State((accepted, requests)): State<Node>,
            headers: axum::http::HeaderMap,
        ) -> axum::response::Response {
            use axum::response::IntoResponse;
            requests.fetch_add(1, Ordering::SeqCst);
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
        let app = Router::new()
            .route("/", post(handler))
            .with_state((accepted, requests));
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", socket.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
        url
    }

    /// A client and its clone follow a rotated cookie, recover once a missing cookie
    /// is replaced, and retry a rejected request at most once.
    #[tokio::test]
    async fn a_rotated_cookie_is_reread() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".cookie");
        let accepted = Arc::new(Mutex::new("user:a".to_owned()));
        let requests = Arc::new(AtomicUsize::new(0));
        let url = cookie_node(accepted.clone(), requests.clone()).await;
        let rotate = |cookie: &str| {
            *accepted.lock().unwrap() = cookie.to_owned();
            std::fs::write(&path, cookie).unwrap();
        };
        rotate("user:a");
        let rpc = ZakuraClient::from_cookie_file(vec![url], &path).unwrap();
        let clone = rpc.clone();
        assert_eq!(rpc.tip_height().await.unwrap(), 7);
        rotate("user:b");
        assert_eq!(clone.tip_height().await.unwrap(), 7);
        assert_eq!(rpc.tip_height().await.unwrap(), 7);
        // A node restarting removes its cookie before writing the next one.
        std::fs::remove_file(&path).unwrap();
        *accepted.lock().unwrap() = "user:c".to_owned();
        assert!(matches!(
            rpc.tip_height().await,
            Err(ZakuraError::Cookie(_))
        ));
        rotate("user:c");
        assert_eq!(rpc.tip_height().await.unwrap(), 7);
        // A node that rejects every cookie gets one retry with a changed cookie, and
        // none with the same one.
        *accepted.lock().unwrap() = "nobody:x".to_owned();
        std::fs::write(&path, "user:d").unwrap();
        for retried in [true, false] {
            requests.store(0, Ordering::SeqCst);
            assert!(matches!(
                rpc.tip_height().await,
                Err(ZakuraError::InvalidCookie)
            ));
            assert_eq!(requests.load(Ordering::SeqCst), 1 + usize::from(retried));
        }
    }

    /// A node that rejects the cookie does not stop a call reaching the next node.
    #[tokio::test]
    async fn a_rejected_cookie_falls_back_to_the_next_node() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".cookie");
        std::fs::write(&path, "user:a").unwrap();
        let requests = Arc::new(AtomicUsize::new(0));
        let rejecting = cookie_node(Arc::new(Mutex::new("user:b".into())), requests.clone());
        let accepting = cookie_node(Arc::new(Mutex::new("user:a".into())), Arc::default());
        let rpc =
            ZakuraClient::from_cookie_file(vec![rejecting.await, accepting.await], &path).unwrap();
        assert_eq!(
            rpc.call::<u64>("getblockcount", json!([])).await.unwrap(),
            7
        );
        assert_eq!(requests.load(Ordering::SeqCst), 1);
    }

    /// A lagging first node hides neither the freshest node's tip nor its blocks.
    #[tokio::test]
    async fn a_lagging_first_node_defers_to_the_freshest_node() {
        let rpc = ZakuraClient::unauthenticated(vec![
            "http://127.0.0.1:1".into(),
            node(100, "a").await,
            node(105, "b").await,
        ])
        .unwrap();
        assert_eq!(rpc.tip_height().await.unwrap(), 105);
        assert_eq!(rpc.block_hash(103).await.unwrap(), "b".repeat(64));
        // A node that reached the height but trails the freshest one, as a node on a
        // stale fork would, does not answer for it.
        assert_eq!(rpc.block_hash(50).await.unwrap(), "b".repeat(64));
        assert!(matches!(
            rpc.block_hash(106).await,
            Err(ZakuraError::Behind(106))
        ));
    }
}
