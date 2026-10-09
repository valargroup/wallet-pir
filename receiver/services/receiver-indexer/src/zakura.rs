//! A minimal JSON-RPC client for a Zakura node: what the indexer reads.
//!
//! It copies the parts of `enhance-pir-server`'s client the indexer needs, so the two
//! services stay independent until a shared node client exists.
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::json;
use std::path::{Path, PathBuf};

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
/// The largest `z_gettreestate` response: three serialized frontiers of at most 33
/// nodes each, hex-encoded, are a few kilobytes.
pub(crate) const TREESTATE_RESPONSE_BYTES: usize = RPC_OVERHEAD_BYTES;
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

    /// The `nodes` that report a tip, each with its tip, highest first and in `nodes`
    /// order among equal tips. A caller runs a whole pass on the first, so every read
    /// in it comes from one node, and the next pass ranks again. Fails, with the last
    /// node's error, only when no node answers.
    pub async fn ranked(nodes: &[ZakuraClient]) -> Result<Vec<(u64, ZakuraClient)>, ZakuraError> {
        let (mut ranked, mut last) = (Vec::new(), None);
        for node in nodes {
            match node.tip_height().await {
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

    /// The node's chain tip height.
    pub async fn tip_height(&self) -> Result<u64, ZakuraError> {
        self.call("getblockcount", json!([]), SCALAR_RESPONSE_BYTES)
            .await
    }

    /// The hash of the node's block at `height`, in RPC display order.
    pub async fn block_hash(&self, height: u64) -> Result<String, ZakuraError> {
        self.call("getblockhash", json!([height]), SCALAR_RESPONSE_BYTES)
            .await
    }

    /// One JSON-RPC call to the node, with the cookie file's current credentials if it
    /// has one. A response body over `limit` bytes is
    /// [`ZakuraError::Oversized`], refused before it is buffered past the limit; the
    /// client's timeout covers reading it.
    pub(crate) async fn call<T: DeserializeOwned>(
        &self,
        method: &str,
        params: serde_json::Value,
        limit: usize,
    ) -> Result<T, ZakuraError> {
        let body =
            json!({"jsonrpc": "1.0", "id": "receiver-indexer", "method": method, "params": params});
        let mut request = self.http.post(&self.url);
        if let Some(path) = &self.cookie {
            let (username, password) = read_cookie(path)?;
            request = request.basic_auth(username, Some(password));
        }
        let mut response = request.json(&body).send().await?.error_for_status()?;
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
    use std::sync::{Arc, Mutex};

    /// A node at tip `tip`. Returns its URL.
    async fn node(tip: u64) -> String {
        let app = Router::new().route(
            "/",
            post(move || async move { Json(json!({"result": tip, "error": null})) }),
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
    /// a decoding error.
    #[tokio::test]
    async fn responses_are_bounded_per_call() {
        let limit = SCALAR_RESPONSE_BYTES;
        let tip = r#"{"result": 7, "error": null}"#;
        let call = |url: String| async move {
            ZakuraClient::new(url, None)
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
            call(declared).await,
            Err(ZakuraError::Oversized(l)) if l == limit
        ));
        let chunked = raw_node(vec![half.clone(), half.clone(), b" ".to_vec()], None).await;
        assert!(matches!(
            call(chunked).await,
            Err(ZakuraError::Oversized(l)) if l == limit
        ));
        let malformed = raw_node(vec![b"{\"result\": 7".to_vec()], Some(12)).await;
        assert!(matches!(call(malformed).await, Err(ZakuraError::Decode(_))));
    }

    /// Nodes rank by tip, highest first and in configured order among equal tips,
    /// leaving out a node that does not answer; with none answering ranking fails.
    #[tokio::test]
    async fn nodes_rank_by_tip() {
        let client = |url: String| ZakuraClient::new(url, None).unwrap();
        let down = client("http://127.0.0.1:1".into());
        let nodes = [
            down.clone(),
            client(node(100).await),
            client(node(105).await),
            client(node(105).await),
        ];
        let ranked = ZakuraClient::ranked(&nodes).await.unwrap();
        let order: Vec<_> = ranked
            .iter()
            .map(|(tip, node)| (*tip, node.url.clone()))
            .collect();
        let urls: Vec<_> = nodes[1..].iter().map(|node| node.url.clone()).collect();
        assert_eq!(
            order,
            [
                (105, urls[1].clone()),
                (105, urls[2].clone()),
                (100, urls[0].clone())
            ]
        );
        assert!(ZakuraClient::ranked(&[down]).await.is_err());
    }
}
