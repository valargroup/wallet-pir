use super::*;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const POLICY: HttpPolicy = HttpPolicy {
    connection_timeout: Some(Duration::from_secs(20)),
    request_timeout: Duration::from_secs(120),
    response_limit: 1024 * 1024,
};

fn client(url: String, credentials: Credentials) -> RpcClient {
    RpcClient::new(url, credentials, "test", POLICY).unwrap()
}

/// One received request: its lowercase-named headers and its JSON body.
struct Request {
    headers: Vec<(String, String)>,
    body: serde_json::Value,
}
impl Request {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }
}

/// A minimal HTTP/1.1 node double: every connection carries one request, answered
/// with `handler`'s status and JSON body. Returns its URL.
async fn serve(
    handler: impl Fn(Request) -> (u16, serde_json::Value) + Send + Sync + 'static,
) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let handler = Arc::new(handler);
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let handler = handler.clone();
            tokio::spawn(async move {
                let mut request = Vec::new();
                let mut buffer = [0u8; 4096];
                let head = loop {
                    let n = socket.read(&mut buffer).await.unwrap();
                    if n == 0 {
                        return;
                    }
                    request.extend_from_slice(&buffer[..n]);
                    if let Some(at) = request.windows(4).position(|b| b == b"\r\n\r\n") {
                        break at;
                    }
                };
                let text = String::from_utf8_lossy(&request[..head]).to_string();
                let headers: Vec<_> = text
                    .lines()
                    .skip(1)
                    .filter_map(|line| line.split_once(':'))
                    .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_owned()))
                    .collect();
                let length: usize = headers
                    .iter()
                    .find(|(k, _)| k == "content-length")
                    .map_or(0, |(_, v)| v.parse().unwrap());
                let mut body = request[head + 4..].to_vec();
                while body.len() < length {
                    let n = socket.read(&mut buffer).await.unwrap();
                    body.extend_from_slice(&buffer[..n]);
                }
                let body = serde_json::from_slice(&body).unwrap();
                let (status, answer) = handler(Request { headers, body });
                let answer = answer.to_string();
                let response = format!(
                    "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{answer}",
                    answer.len()
                );
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            });
        }
    });
    url
}

/// A peer writing exact `headers` then, after `delay`, `body`.
async fn raw_node(headers: String, body: Vec<u8>, delay: Duration) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        let mut buffer = [0u8; 4096];
        loop {
            let n = socket.read(&mut buffer).await.unwrap();
            if n == 0 {
                return;
            }
            request.extend_from_slice(&buffer[..n]);
            if request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                break;
            }
        }
        if socket.write_all(headers.as_bytes()).await.is_err() {
            return;
        }
        tokio::time::sleep(delay).await;
        let _ = socket.write_all(&body).await;
        let _ = socket.shutdown().await;
    });
    url
}

fn basic(cookie: &str) -> String {
    let (username, password) = cookie.split_once(':').unwrap();
    reqwest::Client::new()
        .post("http://node")
        .basic_auth(username, Some(password))
        .build()
        .unwrap()
        .headers()[reqwest::header::AUTHORIZATION]
        .to_str()
        .unwrap()
        .to_owned()
}

/// A rereading client follows a rotated cookie and recovers once a missing one
/// is replaced; a read-once client reports the node's refusal as a bad cookie.
#[tokio::test]
async fn cookies_are_reread_or_refused_after_rotation() {
    let dir = std::env::temp_dir().join(format!("pir-node-rpc-cookie-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(".cookie");
    let accepted = Arc::new(Mutex::new(String::new()));
    let url = serve({
        let accepted = accepted.clone();
        move |request| {
            let expected = basic(&accepted.lock().unwrap());
            if request.header("authorization") != Some(expected.as_str()) {
                return (401, json!(null));
            }
            (200, json!({"result": 7, "error": null}))
        }
    })
    .await;
    let rotate = |cookie: &str| {
        *accepted.lock().unwrap() = cookie.to_owned();
        std::fs::write(&path, cookie).unwrap();
    };
    let rereading = client(url.clone(), Credentials::CookieFile(path.clone()));
    rotate("user:a");
    assert_eq!(rereading.tip_height().await.unwrap(), 7);
    rotate("user:b");
    assert_eq!(rereading.tip_height().await.unwrap(), 7);
    std::fs::remove_file(&path).unwrap();
    assert!(matches!(
        rereading.tip_height().await,
        Err(RpcError::Cookie(_))
    ));
    rotate("user:c");
    assert_eq!(rereading.tip_height().await.unwrap(), 7);
    let fixed = client(url, Credentials::read_once(&path).unwrap());
    assert_eq!(fixed.tip_height().await.unwrap(), 7);
    rotate("user:d");
    assert!(matches!(
        fixed.tip_height().await,
        Err(RpcError::InvalidCookie)
    ));
    std::fs::write(&path, "nopassword").unwrap();
    assert!(matches!(
        Credentials::read_once(&path),
        Err(RpcError::InvalidCookie)
    ));
    std::fs::remove_dir_all(&dir).unwrap();
}

/// Requests name the caller and carry the JSON-RPC 1.0 envelope; RPC errors keep
/// their code and a missing result is typed.
#[tokio::test]
async fn single_calls_keep_their_envelope_and_typed_errors() {
    let url = serve(|request| {
        assert_eq!(request.body["jsonrpc"], "1.0");
        assert_eq!(request.body["id"], "follower");
        match request.body["method"].as_str().unwrap() {
            "getblockhash" => {
                assert_eq!(request.body["params"], json!([5]));
                (200, json!({"result": "00ab", "error": null}))
            }
            "getblockcount" => (200, json!({"result": null, "error": null})),
            _ => (
                200,
                json!({"result": null, "error": {"code": -8, "message": "out of range"}}),
            ),
        }
    })
    .await;
    let rpc = client(url, Credentials::None).with_request_id("follower");
    assert_eq!(rpc.block_hash_display(5).await.unwrap(), "00ab");
    assert!(matches!(
        rpc.tip_height().await,
        Err(RpcError::MissingResult)
    ));
    assert!(matches!(
        rpc.call::<u64>("other", json!([])).await,
        Err(RpcError::Rpc(-8, message)) if message == "out of range"
    ));
}

#[tokio::test]
async fn exact_limit_and_unannounced_bodies_obey_the_same_bound() {
    let body = br#"{"result":7,"error":null}"#.to_vec();
    for declared in [false, true] {
        for limit in [body.len(), body.len() - 1] {
            let length = if declared {
                format!("Content-Length: {}\r\n", body.len())
            } else {
                String::new()
            };
            let url = raw_node(
                format!("HTTP/1.1 200 OK\r\n{length}Connection: close\r\n\r\n"),
                body.clone(),
                Duration::ZERO,
            )
            .await;
            let policy = HttpPolicy {
                response_limit: limit,
                ..POLICY
            };
            let result = RpcClient::new(url, Credentials::None, "test", policy)
                .unwrap()
                .tip_height()
                .await;
            if limit == body.len() {
                assert_eq!(result.unwrap(), 7);
            } else {
                assert!(matches!(result, Err(RpcError::Oversized(bound)) if bound == limit));
            }
        }
    }
}

#[tokio::test]
async fn malformed_and_truncated_bodies_are_refused() {
    for (body, length) in [
        (b"{".to_vec(), 1),
        (br#"{"result":7,"error":null}"#.to_vec(), 100),
    ] {
        let url = raw_node(
            format!("HTTP/1.1 200 OK\r\nContent-Length: {length}\r\nConnection: close\r\n\r\n"),
            body,
            Duration::ZERO,
        )
        .await;
        assert!(matches!(
            client(url, Credentials::None).tip_height().await,
            Err(RpcError::Decode(_)) | Err(RpcError::Http(_))
        ));
    }
}

#[tokio::test]
async fn the_total_deadline_covers_a_body_after_headers_arrive() {
    let body = br#"{"result":7,"error":null}"#.to_vec();
    let url = raw_node(
        format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len()),
        body,
        Duration::from_millis(300),
    )
    .await;
    let policy = HttpPolicy {
        request_timeout: Duration::from_millis(30),
        ..POLICY
    };
    let rpc = RpcClient::new(url, Credentials::None, "test", policy).unwrap();
    assert!(matches!(rpc.tip_height().await, Err(RpcError::Http(error)) if error.is_timeout()));
}

#[tokio::test]
async fn the_connection_deadline_covers_a_stalled_tls_handshake() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("https://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (_socket, _) = listener.accept().await.unwrap();
        tokio::time::sleep(Duration::from_secs(10)).await;
    });
    let policy = HttpPolicy {
        connection_timeout: Some(Duration::from_millis(30)),
        request_timeout: Duration::from_secs(5),
        ..POLICY
    };
    let rpc = RpcClient::new(url, Credentials::None, "test", policy).unwrap();
    let start = std::time::Instant::now();
    assert!(matches!(rpc.tip_height().await, Err(RpcError::Http(error)) if error.is_timeout()));
    assert!(start.elapsed() < Duration::from_secs(3));
    server.abort();
}

#[tokio::test]
async fn batch_answers_are_ordered_and_bad_ids_fail_the_whole_batch() {
    let valid = json!([{"id":1,"error":{"code":-5,"message":"missing"}}, {"id":0,"result":7}]);
    let cases = vec![
        (valid, true),
        (json!([{"id":0,"result":7},{"id":0,"result":8}]), false),
        (json!([{"id":0,"result":7}]), false),
        (json!([{"id":0,"result":7},{"id":2,"result":8}]), false),
        (json!([{"id":0,"result":7},{"id":-1,"result":8}]), false),
        (json!([{"id":0,"result":7},{"id":"1","result":8}]), false),
        (json!([{"id":0,"result":7},{"id":1.0,"result":8}]), false),
        (json!([{"id":0,"result":7},{"id":null,"result":8}]), false),
        (json!([{"id":0,"result":7},{"result":8}]), false),
        (json!([{"id":0,"result":7},{"id":1}]), false),
    ];
    for (answer, valid) in cases {
        let url = serve(move |request| {
            assert_eq!(request.body[0]["jsonrpc"], "2.0");
            assert_eq!(request.body[0]["id"], 0);
            assert_eq!(request.body[1]["id"], 1);
            (200, answer.clone())
        })
        .await;
        let result = client(url, Credentials::None)
            .batch::<u64>("getblockcount", vec![json!([]), json!([])])
            .await;
        if valid {
            assert_eq!(result.unwrap(), vec![Ok(7), Err((-5, "missing".into()))]);
        } else {
            assert!(matches!(result, Err(RpcError::Batch(_))));
        }
    }
}

#[tokio::test]
async fn a_whole_batch_rpc_error_keeps_its_code_and_empty_batches_send_nothing() {
    let url = serve(|_| (200, json!({"error":{"code":-32600,"message":"bad batch"}}))).await;
    let rpc = client(url, Credentials::None);
    assert!(matches!(
        rpc.batch::<u64>("getblockcount", vec![json!([])]).await,
        Err(RpcError::Rpc(-32600, message)) if message == "bad batch"
    ));
    let unreachable = client("http://127.0.0.1:1".into(), Credentials::None);
    assert!(
        unreachable
            .batch::<u64>("getblockcount", vec![])
            .await
            .unwrap()
            .is_empty()
    );
}
