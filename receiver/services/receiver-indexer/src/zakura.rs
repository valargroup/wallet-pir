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
    #[error("RPC response is larger than {0} bytes")]
    Oversized(usize),
    #[error("invalid RPC response: {0}")]
    Decode(serde_json::Error),
}

/// Bytes allowed for the JSON-RPC envelope and a block's fixed fields around its
/// variable part.
const RPC_OVERHEAD_BYTES: usize = 64 * 1024;
/// The largest `getblockcount` or `getblockhash` response: a number or one hash.
const SCALAR_RESPONSE_BYTES: usize = 4096;
/// The largest raw (verbosity 0) `getblock` response: a maximum-size block in hex.
pub(crate) const RAW_BLOCK_RESPONSE_BYTES: usize =
    2 * zakura_chain::block::MAX_BLOCK_BYTES as usize + RPC_OVERHEAD_BYTES;
/// The largest verbose (verbosity 1) `getblock` response. It lists one quoted
/// transaction ID per transaction, allowed 128 bytes with separators and whitespace,
/// for as many of the smallest transactions as fit in a maximum-size block.
pub(crate) const VERBOSE_BLOCK_RESPONSE_BYTES: usize = (zakura_chain::block::MAX_BLOCK_BYTES
    / zakura_chain::transaction::MIN_TRANSPARENT_TX_SIZE)
    as usize
    * 128
    + RPC_OVERHEAD_BYTES;

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
            match self
                .call_at::<u64>(url, "getblockcount", &json!([]), SCALAR_RESPONSE_BYTES)
                .await
            {
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
            match self
                .call_at::<u64>(url, "getblockcount", &json!([]), SCALAR_RESPONSE_BYTES)
                .await
            {
                Ok(tip) if tip >= height => reached.push((std::cmp::Reverse(tip), order, url)),
                Ok(_) => last = ZakuraError::Behind(height),
                Err(error) => last = error,
            }
        }
        reached.sort();
        for (_, _, url) in reached {
            match self
                .call_at(url, "getblockhash", &json!([height]), SCALAR_RESPONSE_BYTES)
                .await
            {
                Ok(hash) => return Ok(hash),
                Err(error) => last = error,
            }
        }
        Err(last)
    }

    /// One JSON-RPC call to the first node that answers it with a response of at most
    /// `limit` bytes. Callers validate answers against each other (batch links and
    /// anchors), so nodes may differ between calls.
    pub(crate) async fn call<T: DeserializeOwned>(
        &self,
        method: &str,
        params: serde_json::Value,
        limit: usize,
    ) -> Result<T, ZakuraError> {
        let mut last = None;
        for url in &self.rpc_urls {
            match self.call_at(url, method, &params, limit).await {
                Ok(result) => return Ok(result),
                Err(error) => last = Some(error),
            }
        }
        Err(last.expect("at least one node RPC endpoint"))
    }

    /// One JSON-RPC call to `url`, authenticated unless the node disables
    /// authentication. A rejected request rereads the cookie and, if it changed, is
    /// retried once with the new one. A response body over `limit` bytes is
    /// [`ZakuraError::Oversized`], refused before it is buffered past the limit; the
    /// client's timeout covers reading it.
    async fn call_at<T: DeserializeOwned>(
        &self,
        url: &str,
        method: &str,
        params: &serde_json::Value,
        limit: usize,
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
        let mut response = response.error_for_status()?;
        if response
            .content_length()
            .is_some_and(|length| length > limit as u64)
        {
            return Err(ZakuraError::Oversized(limit));
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            if body.len() + chunk.len() > limit {
                return Err(ZakuraError::Oversized(limit));
            }
            body.extend_from_slice(&chunk);
        }
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
            rpc.call::<u64>("getblockcount", json!([]), SCALAR_RESPONSE_BYTES)
                .await
                .unwrap(),
            7
        );
        assert_eq!(requests.load(Ordering::SeqCst), 1);
    }

    /// A node that answers every request with `chunks`, declaring `declared` as the
    /// body's length, or sending it chunked without a length for `None`. Returns its
    /// URL.
    async fn raw_node(chunks: Vec<Vec<u8>>, declared: Option<usize>) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", socket.local_addr().unwrap());
        tokio::spawn(async move {
            loop {
                let (mut stream, _) = socket.accept().await.unwrap();
                // Read the whole request, so closing the connection cannot reset it.
                let mut request = Vec::new();
                let mut buffer = [0; 4096];
                let end = loop {
                    let n = stream.read(&mut buffer).await.unwrap();
                    request.extend_from_slice(&buffer[..n]);
                    if let Some(end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                        break end + 4;
                    }
                };
                let head = String::from_utf8_lossy(&request[..end]).to_lowercase();
                let length: usize = head
                    .lines()
                    .find_map(|line| line.strip_prefix("content-length:"))
                    .unwrap()
                    .trim()
                    .parse()
                    .unwrap();
                while request.len() < end + length {
                    let n = stream.read(&mut buffer).await.unwrap();
                    request.extend_from_slice(&buffer[..n]);
                }
                let framing = match declared {
                    Some(length) => format!("Content-Length: {length}"),
                    None => "Transfer-Encoding: chunked".into(),
                };
                let mut response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n{framing}\r\n\
                     Connection: close\r\n\r\n"
                )
                .into_bytes();
                for chunk in &chunks {
                    if declared.is_none() {
                        response.extend(format!("{:x}\r\n", chunk.len()).into_bytes());
                    }
                    response.extend(chunk);
                    if declared.is_none() {
                        response.extend(b"\r\n");
                    }
                }
                if declared.is_none() {
                    response.extend(b"0\r\n\r\n");
                }
                // The client may close first once it has seen enough.
                let _ = stream.write_all(&response).await;
            }
        });
        url
    }

    /// `json` padded with trailing whitespace to `length` bytes.
    fn padded(json: &str, length: usize) -> Vec<u8> {
        let mut body = json.as_bytes().to_vec();
        body.resize(length, b' ');
        body
    }

    /// A response over its method's limit is refused whether it declares its length or
    /// not, one at the limit is accepted, and a bounded body that is not a response is
    /// a decoding error. A node sending too much does not stop a call reaching the next.
    #[tokio::test]
    async fn responses_are_bounded_per_call() {
        let limit = SCALAR_RESPONSE_BYTES;
        let tip = r#"{"result": 7, "error": null}"#;
        let call = |url: String| async move {
            ZakuraClient::unauthenticated(vec![url])
                .unwrap()
                .call::<u64>("getblockcount", json!([]), limit)
                .await
        };
        let exact = padded(tip, limit);
        assert_eq!(
            call(raw_node(vec![exact.clone()], Some(limit)).await)
                .await
                .unwrap(),
            7
        );
        let half = exact[..limit / 2].to_vec();
        let rest = exact[limit / 2..].to_vec();
        assert_eq!(
            call(raw_node(vec![half.clone(), rest], None).await)
                .await
                .unwrap(),
            7
        );
        // A declared length over the limit is refused before the body is read.
        let declared = raw_node(vec![], Some(1 << 30)).await;
        assert!(matches!(
            call(declared.clone()).await,
            Err(ZakuraError::Oversized(l)) if l == limit
        ));
        let chunked = raw_node(vec![half.clone(), half.clone(), b" ".to_vec()], None).await;
        assert!(matches!(
            call(chunked).await,
            Err(ZakuraError::Oversized(l)) if l == limit
        ));
        let malformed = raw_node(vec![b"{\"result\": 7".to_vec()], Some(12)).await;
        assert!(matches!(call(malformed).await, Err(ZakuraError::Decode(_))));
        let rpc = ZakuraClient::unauthenticated(vec![declared, node(9, "a").await]).unwrap();
        assert_eq!(
            rpc.call::<u64>("getblockcount", json!([]), limit)
                .await
                .unwrap(),
            9
        );
        assert_eq!(rpc.tip_height().await.unwrap(), 9);
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
