//! The transport against real HTTP servers whose every byte the test writes:
//! batch validation, authentication and cookie rotation, deadlines, the
//! response bound with and without a declared length, truncation and
//! malformed bodies.

use node_rpc::{Client, CookieReading, Error, Policy, RpcError, Unauthorized};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// What a scripted server does with one request.
#[derive(Clone)]
enum Reply {
    /// A complete response with this status, body and declared length.
    Body {
        status: u16,
        body: Vec<u8>,
        length: bool,
    },
    /// The headers declare `declared` bytes; `sent` are sent, then the
    /// connection closes.
    Truncated { declared: usize, sent: Vec<u8> },
    /// Headers, then the body never arrives.
    Stall,
}

fn ok(body: Value) -> Reply {
    Reply::Body {
        status: 200,
        body: serde_json::to_vec(&body).unwrap(),
        length: true,
    }
}

/// One received request: its Authorization header and JSON body.
#[derive(Clone, Debug)]
struct Seen {
    authorization: Option<String>,
    body: Value,
}

async fn read_request(stream: &mut TcpStream) -> Option<Seen> {
    let mut raw = Vec::new();
    let mut buffer = [0u8; 4096];
    let head_end = loop {
        let n = stream.read(&mut buffer).await.ok()?;
        if n == 0 {
            return None;
        }
        raw.extend_from_slice(&buffer[..n]);
        if let Some(at) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
            break at + 4;
        }
    };
    let head = String::from_utf8_lossy(&raw[..head_end]).to_string();
    let header = |name: &str| {
        head.lines()
            .find(|line| line.to_ascii_lowercase().starts_with(&format!("{name}:")))
            .map(|line| line.split_once(':').unwrap().1.trim().to_string())
    };
    let length: usize = header("content-length")?.parse().ok()?;
    while raw.len() < head_end + length {
        let n = stream.read(&mut buffer).await.ok()?;
        if n == 0 {
            return None;
        }
        raw.extend_from_slice(&buffer[..n]);
    }
    Some(Seen {
        authorization: header("authorization"),
        body: serde_json::from_slice(&raw[head_end..head_end + length]).ok()?,
    })
}

/// A server answering each request with `reply(request)`; returns its URL and
/// the requests it saw.
async fn server(
    reply: impl Fn(&Seen) -> Reply + Send + Sync + 'static,
) -> (String, Arc<Mutex<Vec<Seen>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let reply = Arc::new(reply);
    let log = seen.clone();
    tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (reply, log) = (reply.clone(), log.clone());
            tokio::spawn(async move {
                let Some(request) = read_request(&mut stream).await else {
                    return;
                };
                log.lock().unwrap().push(request.clone());
                match reply(&request) {
                    Reply::Body {
                        status,
                        body,
                        length,
                    } => {
                        let mut head = format!("HTTP/1.1 {status} X\r\ncontent-type: application/json\r\nconnection: close\r\n");
                        if length {
                            head.push_str(&format!("content-length: {}\r\n", body.len()));
                        }
                        head.push_str("\r\n");
                        let _ = stream.write_all(head.as_bytes()).await;
                        let _ = stream.write_all(&body).await;
                    }
                    Reply::Truncated { declared, sent } => {
                        let head = format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {declared}\r\nconnection: close\r\n\r\n"
                        );
                        let _ = stream.write_all(head.as_bytes()).await;
                        let _ = stream.write_all(&sent).await;
                    }
                    Reply::Stall => {
                        let _ = stream
                            .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 10\r\n\r\n")
                            .await;
                        tokio::time::sleep(Duration::from_secs(600)).await;
                    }
                }
                let _ = stream.shutdown().await;
            });
        }
    });
    (url, seen)
}

fn policy(response_bytes: usize) -> Policy {
    Policy {
        response_bytes,
        ..Policy::cookie_at_construction()
    }
}

fn client(url: &str, policy: Policy) -> Client {
    Client::new(url, None, "product-id", policy).unwrap()
}

/// A single call is JSON-RPC 1.0 under the product's fixed id; its result is
/// decoded, an RPC error and a missing result are reported as such.
#[tokio::test]
async fn single_calls_keep_their_shape_and_errors() {
    let (url, seen) = server(|request| match request.body["method"].as_str() {
        Some("getblockcount") => ok(json!({"result": 7, "error": null, "id": "product-id"})),
        Some("getrawtransaction") => {
            ok(json!({"result": null, "error": {"code": -5, "message": "No information"}}))
        }
        _ => ok(json!({"result": null, "error": null})),
    })
    .await;
    let rpc = client(&url, Policy::cookie_at_construction());
    assert_eq!(
        rpc.call::<u64>("getblockcount", json!([])).await.unwrap(),
        7
    );
    let request = seen.lock().unwrap()[0].body.clone();
    assert_eq!(
        request,
        json!({"jsonrpc": "1.0", "id": "product-id", "method": "getblockcount", "params": []})
    );
    assert!(matches!(
        rpc.call::<String>("getrawtransaction", json!(["ab", 0])).await,
        Err(Error::Rpc(-5, message)) if message == "No information"
    ));
    assert!(matches!(
        rpc.call::<u64>("other", json!([])).await,
        Err(Error::MissingResult)
    ));
    // A result of the wrong type is a decoding failure, not a value.
    let (url, _) = server(|_| ok(json!({"result": "seven", "error": null}))).await;
    assert!(matches!(
        client(&url, Policy::cookie_at_construction())
            .call::<u64>("getblockcount", json!([]))
            .await,
        Err(Error::Decode(_))
    ));
}

/// A batch is JSON-RPC 2.0 with ids 0..n; responses arriving in any order are
/// returned in request order, each with its own result or RPC error.
#[tokio::test]
async fn batches_are_returned_in_request_order() {
    let (url, seen) = server(|request| {
        let mut entries: Vec<Value> = request
            .body
            .as_array()
            .unwrap()
            .iter()
            .map(|call| {
                let id = call["id"].as_u64().unwrap();
                if id == 2 {
                    json!({"jsonrpc": "2.0", "id": id, "result": null, "error": {"code": -5, "message": "missing"}})
                } else {
                    json!({"jsonrpc": "2.0", "id": id, "result": format!("r{id}")})
                }
            })
            .collect();
        entries.reverse();
        ok(Value::Array(entries))
    })
    .await;
    let rpc = client(&url, Policy::cookie_at_construction());
    let calls: Vec<(&str, Value)> = (0..4).map(|i| ("getblockhash", json!([i]))).collect();
    let results = rpc.batch::<String>(&calls).await.unwrap();
    assert_eq!(
        results,
        vec![
            Ok("r0".to_string()),
            Ok("r1".to_string()),
            Err(RpcError {
                code: -5,
                message: "missing".into()
            }),
            Ok("r3".to_string()),
        ]
    );
    let request = seen.lock().unwrap()[0].body.clone();
    assert_eq!(
        request[3],
        json!({"jsonrpc": "2.0", "id": 3, "method": "getblockhash", "params": [3]})
    );
    // An empty batch sends nothing.
    assert!(rpc.batch::<String>(&[]).await.unwrap().is_empty());
    assert_eq!(seen.lock().unwrap().len(), 1);
}

/// Exactly one response per issued integer id: anything else fails the whole
/// batch, as does a whole-batch refusal, reported as the node's error.
#[tokio::test]
async fn malformed_batches_are_refused_whole() {
    let entry = |id: Value| json!({"jsonrpc": "2.0", "id": id, "result": "x"});
    let cases: Vec<(&str, Value)> = vec![
        (
            "a missing id",
            json!([entry(json!(0)), {"jsonrpc": "2.0", "result": "x"}]),
        ),
        ("a duplicate id", json!([entry(json!(0)), entry(json!(0))])),
        (
            "an unexpected id",
            json!([entry(json!(0)), entry(json!(2))]),
        ),
        ("a negative id", json!([entry(json!(0)), entry(json!(-1))])),
        ("a string id", json!([entry(json!(0)), entry(json!("1"))])),
        (
            "a fractional id",
            json!([entry(json!(0)), entry(json!(1.5))]),
        ),
        ("too few entries", json!([entry(json!(0))])),
        (
            "too many entries",
            json!([entry(json!(0)), entry(json!(1)), entry(json!(2))]),
        ),
        (
            "an entry without a result",
            json!([entry(json!(0)), {"jsonrpc": "2.0", "id": 1}]),
        ),
        ("an object without an error", json!({"result": []})),
    ];
    for (name, body) in cases {
        let (url, _) = server(move |_| ok(body.clone())).await;
        let result = client(&url, Policy::cookie_at_construction())
            .batch::<String>(&[("a", json!([])), ("b", json!([]))])
            .await;
        assert!(matches!(result, Err(Error::Batch(_))), "{name}: {result:?}");
    }
    let (url, _) = server(|_| {
        ok(json!({"result": null, "error": {"code": -32600, "message": "batch refused"}, "id": null}))
    })
    .await;
    assert!(matches!(
        client(&url, Policy::cookie_at_construction())
            .batch::<String>(&[("a", json!([]))])
            .await,
        Err(Error::Rpc(-32600, message)) if message == "batch refused"
    ));
}

/// A 401 is an invalid cookie under the shared policy and the HTTP status error
/// under Receiver's.
#[tokio::test]
async fn unauthorized_follows_the_policy() {
    let (url, _) = server(|_| Reply::Body {
        status: 401,
        body: Vec::new(),
        length: true,
    })
    .await;
    assert!(matches!(
        client(&url, Policy::cookie_at_construction())
            .call::<u64>("getblockcount", json!([]))
            .await,
        Err(Error::InvalidCookie)
    ));
    let receiver = client(&url, Policy::cookie_every_request(1 << 20));
    match receiver.call::<u64>("getblockcount", json!([])).await {
        Err(Error::Http(error)) => assert_eq!(error.status().map(|s| s.as_u16()), Some(401)),
        other => panic!("{other:?}"),
    }
}

/// Basic auth from the cookie file: read once under the shared policy, before
/// every request under Receiver's, which follows a rotation and recovers once a
/// removed cookie is replaced.
#[tokio::test]
async fn cookies_are_read_as_the_policy_says() {
    let (url, seen) = server(|_| ok(json!({"result": 1, "error": null}))).await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(".cookie");
    std::fs::write(&path, "user:first\n").unwrap();
    let once = Client::new(&url, Some(&path), "id", Policy::cookie_at_construction()).unwrap();
    let every = Client::new(
        &url,
        Some(&path),
        "id",
        Policy::cookie_every_request(1 << 20),
    )
    .unwrap();
    let basic = |password: &str| {
        reqwest::Client::new()
            .post("http://node")
            .basic_auth("user", Some(password))
            .build()
            .unwrap()
            .headers()[reqwest::header::AUTHORIZATION]
            .to_str()
            .unwrap()
            .to_string()
    };
    std::fs::write(&path, "user:second\n").unwrap();
    once.call::<u64>("m", json!([])).await.unwrap();
    every.call::<u64>("m", json!([])).await.unwrap();
    let auth: Vec<_> = seen
        .lock()
        .unwrap()
        .iter()
        .map(|s| s.authorization.clone().unwrap())
        .collect();
    assert_eq!(auth, vec![basic("first"), basic("second")]);
    // A node restarting removes its cookie before writing the next one.
    std::fs::remove_file(&path).unwrap();
    assert!(matches!(
        every.call::<u64>("m", json!([])).await,
        Err(Error::Cookie(_))
    ));
    std::fs::write(&path, "user:third").unwrap();
    every.call::<u64>("m", json!([])).await.unwrap();
    assert_eq!(
        seen.lock().unwrap().last().unwrap().authorization,
        Some(basic("third"))
    );
    // A malformed cookie is refused, at construction or per request.
    std::fs::write(&path, "no-separator").unwrap();
    assert!(matches!(
        Client::new(&url, Some(&path), "id", Policy::cookie_at_construction()),
        Err(Error::InvalidCookie)
    ));
    assert!(matches!(
        every.call::<u64>("m", json!([])).await,
        Err(Error::InvalidCookie)
    ));
    // Without a cookie no credentials are sent.
    client(&url, Policy::cookie_at_construction())
        .call::<u64>("m", json!([]))
        .await
        .unwrap();
    assert_eq!(seen.lock().unwrap().last().unwrap().authorization, None);
}

/// The bound holds at exactly its size and refuses one byte more, whether the
/// response declares its length or not.
#[tokio::test]
async fn responses_are_bounded_with_or_without_a_length() {
    let body = serde_json::to_vec(&json!({"result": "x".repeat(1000), "error": null})).unwrap();
    for length in [true, false] {
        let reply = Reply::Body {
            status: 200,
            body: body.clone(),
            length,
        };
        let (url, _) = server(move |_| reply.clone()).await;
        let exact = client(&url, policy(body.len()));
        assert_eq!(
            exact.call::<String>("m", json!([])).await.unwrap().len(),
            1000,
            "declared {length}"
        );
        let short = client(&url, policy(body.len() - 1));
        assert!(
            matches!(short.call::<String>("m", json!([])).await, Err(Error::Oversized(n)) if n == body.len() - 1),
            "declared {length}"
        );
    }
}

/// A body cut short of its declared length, and one that is not JSON, are
/// refused.
#[tokio::test]
async fn truncated_and_malformed_bodies_are_refused() {
    let (url, _) = server(|_| Reply::Truncated {
        declared: 100,
        sent: b"{\"result\": 1".to_vec(),
    })
    .await;
    assert!(matches!(
        client(&url, Policy::cookie_at_construction())
            .call::<u64>("m", json!([]))
            .await,
        Err(Error::Http(_))
    ));
    let (url, _) = server(|_| Reply::Body {
        status: 200,
        body: b"{\"result\": 1,".to_vec(),
        length: true,
    })
    .await;
    assert!(matches!(
        client(&url, Policy::cookie_at_construction())
            .call::<u64>("m", json!([]))
            .await,
        Err(Error::Decode(_))
    ));
    assert!(matches!(
        client(&url, Policy::cookie_at_construction())
            .batch::<u64>(&[("m", json!([]))])
            .await,
        Err(Error::Decode(_))
    ));
}

/// The total deadline covers a body that never arrives; the connect deadline,
/// a connection the server never completes. Both are injected short here.
#[tokio::test]
async fn deadlines_bound_the_body_and_the_connection() {
    let (url, _) = server(|_| Reply::Stall).await;
    let short = Policy {
        total: Duration::from_millis(300),
        ..Policy::cookie_at_construction()
    };
    let started = Instant::now();
    match client(&url, short).call::<u64>("m", json!([])).await {
        Err(Error::Http(error)) => assert!(error.is_timeout(), "{error}"),
        other => panic!("{other:?}"),
    }
    assert!(started.elapsed() < Duration::from_secs(5));

    // A listener whose accept queue is full: the kernel stops completing
    // handshakes, so a further connection never establishes.
    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let address = socket.local_addr().unwrap();
    let listener = socket.listen(1).unwrap();
    let mut held = Vec::new();
    for _ in 0..8 {
        if let Ok(Ok(stream)) =
            tokio::time::timeout(Duration::from_millis(200), TcpStream::connect(address)).await
        {
            held.push(stream);
        }
    }
    let connect = Policy {
        connect: Some(Duration::from_millis(300)),
        ..Policy::cookie_every_request(1 << 20)
    };
    let started = Instant::now();
    match client(&format!("http://{address}"), connect)
        .call::<u64>("m", json!([]))
        .await
    {
        Err(Error::Http(error)) => assert!(error.is_connect() || error.is_timeout(), "{error}"),
        other => panic!("{other:?}"),
    }
    assert!(started.elapsed() < Duration::from_secs(5));
    drop((listener, held));
    assert_eq!(
        Policy::cookie_every_request(1).connect,
        Some(Duration::from_secs(20))
    );
    assert_eq!(
        Policy::cookie_at_construction().cookie,
        CookieReading::AtConstruction
    );
    assert_eq!(
        Policy::cookie_every_request(1).unauthorized,
        Unauthorized::HttpStatus
    );
}
