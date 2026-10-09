//! A `pir-monitor` service probe for the receiver directory. It checks the served
//! publication against independent nodes, looks up a pinned historical payment over live
//! encrypted PIR, as Transparent's canary checks one query against a pinned row, checks
//! the filter file against the manifest, then checks the NEAR feed's freshness and the indexer's payout check, which health reports
//! on the private network. It prints one JSON line: `passed`, on failure a `category` and
//! `detail`, and the lookup as `phase: "live_encrypted_probe"` with `queries` and
//! `correct`. `answer_mismatch` marks served data that is wrong, which the monitor treats
//! as a correctness incident; `oracle_invalid` a fixture that fails its pin; anything
//! else, such as `oracle_unavailable` when no node has reached the publication, is an
//! availability failure. Every response body is bounded before it is buffered.
use clap::Parser;
use receiver_directory::{
    extract::Action,
    filter::{Filters, MAX_FILTERS_BYTES},
    Hash, Payment,
};
use receiver_indexer::zakura::ZakuraClient;
use receiver_pir::{
    public_bytes, response_bytes, transport::MAX_PIR_PAGES, AcceptedCoverage, Client, Manifest,
    MAX_MANIFEST_BYTES,
};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::path::PathBuf;

/// The recent set's largest age that wallets still trust (`zakura-pir-receiver`).
const MAX_RECENT_AGE_SECS: i64 = 15 * 60;
/// Bound on the health report, a few hundred bytes.
const MAX_HEALTH_BYTES: usize = 64 * 1024;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Parser)]
struct Args {
    /// The receiver directory's public origin, which serves the wallet routes.
    #[arg(long)]
    origin: String,
    /// The service's health route on the private network, such as
    /// `http://10.70.0.11:18380/v1/receiver/health`; the public edge does not serve it.
    #[arg(long)]
    health_url: String,
    /// The pinned payment: a public zero-OVK Action with its transaction, height and
    /// note position (see [`Fixture`]).
    #[arg(long)]
    fixture: PathBuf,
    /// The fixture file's SHA-256, hex.
    #[arg(long)]
    fixture_sha256: String,
    /// An independent node's RPC endpoint. Repeat it for fallbacks, tried in order;
    /// the first that has reached the publication checks it.
    #[arg(long, required = true)]
    rpc_url: Vec<String>,
    #[arg(long, required_unless_present = "no_auth", conflicts_with = "no_auth")]
    cookie: Option<PathBuf>,
    #[arg(long)]
    no_auth: bool,
    /// Blocks the publication may trail the freshest node's tip. It must cover the
    /// indexer's `--depth` plus its publication delay (a poll and PIR preparation) and
    /// a rotation's 60-second grace: the default 12 suits the default depth of 2.
    #[arg(long, default_value_t = 12)]
    max_lag: u64,
}

/// A failed check's category and detail.
type Failure = (&'static str, Value);

/// A historical payment that every publication holds, independently extracted from a
/// node's verbose block.
#[derive(Deserialize)]
struct Fixture {
    /// Displayed (reversed) hex, as RPC shows it.
    txid: String,
    height: u32,
    action_index: u32,
    position: u64,
    action: FixtureAction,
}

/// The Action's fields as RPC names them, hex.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FixtureAction {
    cv: String,
    nullifier: String,
    cmx: String,
    ephemeral_key: String,
    enc_ciphertext: String,
    out_ciphertext: String,
}

/// Decodes a fixed-length hex field.
fn bytes<const N: usize>(hex: &str) -> Option<[u8; N]> {
    hex::decode(hex).ok()?.try_into().ok()
}

#[tokio::main]
async fn main() {
    let mut lookup = None;
    let (passed, category, detail) = match probe(Args::parse(), &mut lookup).await {
        Ok(None) => (true, None, Value::Null),
        Ok(Some((category, detail))) => (false, Some(category), detail),
        Err(error) => (false, Some("request_failed"), error.to_string().into()),
    };
    let mut output = json!({"passed": passed, "category": category, "detail": detail});
    if let Some((queries, correct)) = lookup {
        output["phase"] = "live_encrypted_probe".into();
        output["queries"] = queries.into();
        output["correct"] = (if correct { queries } else { 0 }).into();
    }
    println!("{output}");
    std::process::exit(if passed { 0 } else { 1 });
}

/// The failed check, or `None` when every check passes. `lookup` records the encrypted
/// queries made and whether they found the fixture's payment.
async fn probe(args: Args, lookup: &mut Option<(u32, bool)>) -> Result<Option<Failure>> {
    let raw = std::fs::read(&args.fixture)?;
    if hex::encode(Sha256::digest(&raw)) != args.fixture_sha256.to_ascii_lowercase() {
        return Ok(Some(("oracle_invalid", json!({"fixture": "sha256"}))));
    }
    let fixture: Fixture = serde_json::from_slice(&raw)?;
    let a = &fixture.action;
    let action = (|| {
        Some(Action {
            cv: bytes(&a.cv)?,
            nullifier: bytes(&a.nullifier)?,
            cmx: bytes(&a.cmx)?,
            ephemeral_key: bytes(&a.ephemeral_key)?,
            enc_ciphertext: bytes(&a.enc_ciphertext)?,
            out_ciphertext: bytes(&a.out_ciphertext)?,
        })
    })();
    let txid: Option<Hash> = fixture
        .txid
        .parse::<zakura_chain::transaction::Hash>()
        .ok()
        .map(|t| t.0);
    let (Some(action), Some(txid)) = (action, txid) else {
        return Ok(Some(("oracle_invalid", json!({"fixture": "malformed"}))));
    };
    let Ok(Some(receiver)) = action.recover_receiver() else {
        return Ok(Some(("oracle_invalid", json!({"fixture": "not zero-OVK"}))));
    };
    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(20))
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let origin = args.origin.trim_end_matches('/');
    let manifest = fetch_manifest(&http, origin).await?;
    let directory = &manifest.directory;
    let height = u64::from(directory.end_height);
    let (rpc, tip) = match oracle(&args.rpc_url, args.cookie.as_deref(), height).await? {
        Ok(found) => found,
        Err(tips) => {
            return Ok(Some((
                "oracle_unavailable",
                json!({"end_height": height, "nodes": tips}),
            )))
        }
    };
    if let Some(failure) = check_anchor(&rpc, tip, directory, args.max_lag).await? {
        return Ok(Some(failure));
    }
    if directory.start_height > fixture.height || directory.end_height < fixture.height {
        return Ok(Some((
            "oracle_invalid",
            json!({"fixture": "outside coverage"}),
        )));
    }
    // The pinned receiver's pages over live encrypted PIR, in position order, until the
    // fixture's payment or a later one.
    let id = hex::encode(manifest.id()?);
    let rows = directory.rows;
    let public = get(
        &http,
        &format!("{origin}/v1/receiver/public/{id}"),
        public_bytes(rows)?,
    )
    .await?;
    let accepted = AcceptedCoverage {
        genesis: directory.genesis,
        required_start: directory.start_height,
        height: directory.end_height,
        hash: directory.end_hash,
    };
    let client = Client::new(manifest.clone(), &public, accepted)?;
    let mut queries = 0;
    let found = loop {
        let query = client.prepare(receiver, queries)?;
        let response = http
            .post(format!("{origin}/v1/receiver/query"))
            .body(query.body().to_vec())
            .send()
            .await?;
        let response = read_limited(response, response_bytes(rows)?).await?;
        queries += 1;
        let record = match client.decode(query, &response) {
            Ok(Some(record)) => record,
            Ok(None) => break None,
            Err(error) => {
                *lookup = Some((queries, false));
                return Ok(Some((
                    "answer_mismatch",
                    json!({"lookup": error.to_string()}),
                )));
            }
        };
        if record.payment.position >= fixture.position || queries == record.total {
            break Some(record.payment);
        }
        if queries == MAX_PIR_PAGES {
            return Err("pinned receiver's history exceeds the probe's page bound".into());
        }
    };
    let expected = |p: &Payment| {
        p.position == fixture.position
            && p.height == fixture.height
            && p.txid == txid
            && p.action_index == fixture.action_index
            && p.action_nullifier == action.nullifier
            && p.cmx == action.cmx
            && p.ephemeral_key == action.ephemeral_key
            && p.ciphertext_prefix[..] == action.enc_ciphertext[..52]
    };
    let correct = match &found {
        Some(payment) if expected(payment) => {
            let node: zakura_chain::block::Hash =
                rpc.block_hash(u64::from(fixture.height)).await?.parse()?;
            if node.0 == payment.block_hash {
                let block = rpc.receiver_block(node.0).await?;
                match check_tx_index(&block, &txid, payment) {
                    Ok(correct) => correct,
                    Err(failure) => {
                        *lookup = Some((queries, false));
                        return Ok(Some(failure));
                    }
                }
            } else {
                false
            }
        }
        _ => false,
    };
    *lookup = Some((queries, correct));
    if !correct {
        return Ok(Some((
            "answer_mismatch",
            json!({"lookup": found.map(|p| (p.height, p.position)), "queries": queries}),
        )));
    }
    if let Some(failure) = check_filters(&http, origin, &id, directory).await? {
        return Ok(Some(failure));
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs() as i64;
    let recent = directory
        .filters
        .iter()
        .find(|set| set.label == "near-intents/recent")
        .and_then(|set| set.until_unix);
    if recent.is_none_or(|until| until < now - MAX_RECENT_AGE_SECS) {
        return Ok(Some(("stale_feed", json!({"recent_until": recent}))));
    }
    indexer_report(&http, origin, &args.health_url, &id).await
}

/// Checks the publication's anchor against `rpc`, the oracle node at tip `tip`: its
/// hash and genesis, the tree size after it (read by that hash, see
/// [`ZakuraClient::receiver_boundary`]), which is the coverage the manifest claims, and
/// that it trails `tip` by at most `max_lag` blocks. A boundary the node cannot give is
/// `oracle_unavailable`, not evidence against the publication.
async fn check_anchor(
    rpc: &ZakuraClient,
    tip: u64,
    directory: &receiver_directory::snapshot::Manifest,
    max_lag: u64,
) -> Result<Option<Failure>> {
    let height = directory.end_height;
    let boundary = match rpc.receiver_boundary(height).await {
        Ok(boundary) => boundary,
        Err(error) => {
            return Ok(Some((
                "oracle_unavailable",
                json!({"end_height": height, "boundary": error.to_string()}),
            )))
        }
    };
    let genesis: zakura_chain::block::Hash = rpc.block_hash(0).await?.parse()?;
    if boundary.hash != directory.end_hash || genesis.0 != directory.genesis {
        return Ok(Some((
            "answer_mismatch",
            json!({"anchor_off_chain": height}),
        )));
    }
    if boundary.position != directory.end_position {
        return Ok(Some((
            "answer_mismatch",
            json!({"end_position": directory.end_position, "node_tree_size": boundary.position}),
        )));
    }
    if tip - u64::from(height) > max_lag {
        return Ok(Some((
            "stale_publication",
            json!({"end_height": height, "node_tip": tip}),
        )));
    }
    Ok(None)
}

/// Whether `payment`, the fixture's as served, has the transaction index of the
/// fixture's transaction `txid` in `block`, the oracle's canonical block at the
/// fixture's height. A fixture whose transaction is not in that block is invalid.
fn check_tx_index(
    block: &zakura_chain::block::Block,
    txid: &Hash,
    payment: &Payment,
) -> std::result::Result<bool, Failure> {
    let index = block
        .transactions
        .iter()
        .position(|tx| tx.hash().0 == *txid)
        .ok_or(("oracle_invalid", json!({"fixture": "not in its block"})))?;
    Ok(u32::try_from(index).is_ok_and(|index| index == payment.tx_index))
}

/// Checks the session's filter file, which wallets test before any lookup: its digest
/// must be the manifest's and it must decode to exactly the declared sets.
async fn check_filters(
    http: &reqwest::Client,
    origin: &str,
    id: &str,
    directory: &receiver_directory::snapshot::Manifest,
) -> Result<Option<Failure>> {
    let url = format!("{origin}/v1/receiver/filters/{id}");
    let bytes = get(http, &url, MAX_FILTERS_BYTES).await?;
    let valid = Hash::from(Sha256::digest(&bytes)) == directory.filters_sha256
        && Filters::decode(&bytes).is_ok_and(|filters| directory.check_filters(&filters).is_ok());
    Ok((!valid).then(|| ("answer_mismatch", json!({"filters_bytes": bytes.len()}))))
}

/// The indexer's payout check from health, which must report serving the probed
/// publication `id`, or the one the origin serves now if it rotated since: otherwise
/// the private health URL and the public origin are not the same service.
async fn indexer_report(
    http: &reqwest::Client,
    origin: &str,
    health_url: &str,
    id: &str,
) -> Result<Option<Failure>> {
    let health: Value = serde_json::from_slice(&get(http, health_url, MAX_HEALTH_BYTES).await?)?;
    let serving = health["serving"].as_str();
    if serving != Some(id) {
        let current = hex::encode(fetch_manifest(http, origin).await?.id()?);
        if serving != Some(current.as_str()) {
            return Ok(Some((
                "report_unavailable",
                json!({"probed": id, "health_serving": serving}),
            )));
        }
    }
    match health["indexer"]["payouts_missing"].as_u64() {
        None => Ok(Some(("report_unavailable", Value::Null))),
        Some(0) => Ok(None),
        Some(missing) => Ok(Some((
            "answer_mismatch",
            json!({"payouts_missing": missing}),
        ))),
    }
}

/// The reachable node with the highest tip, with that tip, if it has reached `height`.
/// The highest tip bounds the lag, and a node behind the publication can check neither
/// its anchor nor its lag, so otherwise the error lists each node's tip or failure.
async fn oracle(
    urls: &[String],
    cookie: Option<&std::path::Path>,
    height: u64,
) -> Result<std::result::Result<(ZakuraClient, u64), Vec<Value>>> {
    let mut tips = Vec::new();
    let mut best: Option<(ZakuraClient, u64)> = None;
    for url in urls {
        let node = match cookie {
            Some(path) => ZakuraClient::from_cookie_file(vec![url.clone()], path)?,
            None => ZakuraClient::unauthenticated(vec![url.clone()])?,
        };
        match node.tip_height().await {
            Ok(tip) => {
                tips.push(tip.into());
                if best.as_ref().is_none_or(|(_, top)| tip > *top) {
                    best = Some((node, tip));
                }
            }
            Err(error) => tips.push(error.to_string().into()),
        }
    }
    Ok(match best {
        Some((node, tip)) if tip >= height => Ok((node, tip)),
        _ => Err(tips),
    })
}

/// The validated session manifest the origin serves now.
async fn fetch_manifest(http: &reqwest::Client, origin: &str) -> Result<Manifest> {
    let bytes = get(
        http,
        &format!("{origin}/v1/receiver/init"),
        MAX_MANIFEST_BYTES,
    )
    .await?;
    let manifest: Manifest = serde_json::from_slice(&bytes)?;
    manifest.validate()?;
    Ok(manifest)
}

/// GETs `url`, reading at most `limit` bytes of its body.
async fn get(http: &reqwest::Client, url: &str, limit: usize) -> Result<Vec<u8>> {
    read_limited(http.get(url).send().await?, limit).await
}

/// A successful response's body, refused as soon as it exceeds `limit` bytes.
async fn read_limited(response: reqwest::Response, limit: usize) -> Result<Vec<u8>> {
    let mut response = response.error_for_status()?;
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err("response exceeds its bound".into());
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if body.len() + chunk.len() > limit {
            return Err("response exceeds its bound".into());
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

#[cfg(test)]
#[path = "../../../crates/receiver-directory/tests/common/mod.rs"]
mod common;

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{routing, Json, Router};

    /// A node at tip `tip` whose block hashes are `fill` repeated. Returns its URL.
    async fn node(tip: u64, fill: &'static str) -> String {
        let app = Router::new().route(
            "/",
            routing::post(move |Json(request): Json<Value>| async move {
                Json(match request["method"].as_str() {
                    Some("getblockcount") => json!({"result": tip, "error": null}),
                    _ => json!({"result": fill.repeat(64), "error": null}),
                })
            }),
        );
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", socket.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
        url
    }

    /// A node that merely reached the publication does not hide a later, fresher one,
    /// so the lag is measured from the highest tip.
    #[tokio::test]
    async fn the_oracle_is_the_freshest_node_that_reached_the_publication() {
        let urls = vec![
            node(105, "a").await,
            node(150, "b").await,
            node(90, "c").await,
        ];
        let Ok((rpc, tip)) = oracle(&urls, None, 100).await.unwrap() else {
            panic!("a node reached the publication");
        };
        assert_eq!(tip, 150);
        assert_eq!(rpc.block_hash(100).await.unwrap(), "b".repeat(64));
        let behind = vec![node(90, "c").await, "http://127.0.0.1:1".into()];
        let Err(tips) = oracle(&behind, None, 100).await.unwrap() else {
            panic!("no node reached the publication");
        };
        assert_eq!(tips.len(), 2);
    }

    /// A node at tip `tip` on a chain whose block at every positive height is
    /// `[hash; 32]`, reporting Ironwood tree size `size`, and genesis `[1; 32]` as
    /// [`super::common::manifest`] declares. With `reorg`, every second hash it gives
    /// is `[9; 32]`, as when the chain changes between two reads. Returns a client.
    async fn chain(tip: u64, hash: u8, size: Option<u64>, reorg: bool) -> ZakuraClient {
        let asked = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let app = Router::new().route(
            "/",
            routing::post(move |Json(request): Json<Value>| async move {
                let display = |byte| zakura_chain::block::Hash([byte; 32]).to_string();
                let result = match request["method"].as_str().unwrap() {
                    "getblockcount" => json!(tip),
                    "getblockhash" if request["params"][0] == 0 => json!(display(1)),
                    "getblockhash" => {
                        let second = asked.fetch_add(1, std::sync::atomic::Ordering::SeqCst) % 2;
                        json!(display(if reorg && second == 1 { 9 } else { hash }))
                    }
                    "getblock" => match size {
                        Some(size) => json!({"trees": {"ironwood": {"size": size}}}),
                        None => json!({"trees": {}}),
                    },
                    method => panic!("unexpected {method}"),
                };
                Json(json!({"result": result, "error": null}))
            }),
        );
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", socket.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
        ZakuraClient::unauthenticated(vec![url]).unwrap()
    }

    /// [`super::common::manifest`] ending at Ironwood activation, where a node reports
    /// tree sizes.
    fn anchored() -> receiver_directory::snapshot::Manifest {
        let mut directory = super::common::manifest(receiver_pir::MIN_ROWS);
        directory.end_height = receiver_indexer::blocks::ironwood_activation();
        directory
    }

    /// The anchor must have the node's hash and the tree size after that block. A node
    /// that cannot give the size, or whose chain changes during the read, proves
    /// nothing against the publication.
    #[tokio::test]
    async fn the_anchor_must_match_the_nodes_hash_and_tree_size() {
        let directory = anchored();
        let tip = u64::from(directory.end_height);
        let check = |rpc: ZakuraClient| {
            let directory = directory.clone();
            async move {
                check_anchor(&rpc, tip, &directory, 12)
                    .await
                    .unwrap()
                    .map(|failure| failure.0)
            }
        };
        assert_eq!(check(chain(tip, 3, Some(300), false).await).await, None);
        for (hash, size, reorg, expected) in [
            (3, Some(301), false, "answer_mismatch"),
            (4, Some(300), false, "answer_mismatch"),
            (3, None, false, "oracle_unavailable"),
            (3, Some(300), true, "oracle_unavailable"),
        ] {
            let rpc = chain(tip, hash, size, reorg).await;
            assert_eq!(check(rpc).await, Some(expected), "{hash} {size:?} {reorg}");
        }
    }

    /// `--max-lag` defaults to 12 and bounds the publication's lag inclusively.
    #[tokio::test]
    async fn the_lag_bound_is_configurable() {
        let args = |extra: &[&str]| {
            let required = [
                "receiver-probe",
                "--origin",
                "https://receiver",
                "--health-url",
                "http://10.0.0.1/health",
                "--fixture",
                "fixture.json",
                "--fixture-sha256",
                "00",
                "--rpc-url",
                "http://node",
                "--no-auth",
            ];
            Args::try_parse_from(required.iter().chain(extra)).unwrap()
        };
        assert_eq!(args(&[]).max_lag, 12);
        // A depth of 50 needs about ten more blocks for the poll, preparation and grace.
        let max_lag = args(&["--max-lag", "60"]).max_lag;
        assert_eq!(max_lag, 60);
        let directory = anchored();
        let height = u64::from(directory.end_height);
        for (lag, stale) in [(60, false), (61, true)] {
            let rpc = chain(height + lag, 3, Some(300), false).await;
            let failure = check_anchor(&rpc, height + lag, &directory, max_lag);
            assert_eq!(
                failure.await.unwrap().map(|f| f.0),
                stale.then_some("stale_publication")
            );
        }
    }

    /// The served payment must carry its transaction's index in the oracle's block,
    /// and a fixture transaction missing from that block is invalid.
    #[test]
    fn the_transaction_index_comes_from_the_oracles_block() {
        use zakura_chain::{
            block::Block,
            serialization::{ZcashDeserialize, ZcashSerialize},
            transaction::Transaction,
        };
        let raw = hex::decode(include_str!("../tests/fixtures/receiver-refund.hex").trim());
        let refund = Transaction::zcash_deserialize(raw.unwrap().as_slice()).unwrap();
        let mut first = refund.clone();
        if let Transaction::V6 { expiry_height, .. } = &mut first {
            expiry_height.0 += 1;
        }
        // A header and empty solution, then the two transactions.
        let mut bytes = vec![0u8; 140];
        bytes[..4].copy_from_slice(&4u32.to_le_bytes());
        bytes.extend_from_slice(&[0xfd, 0x40, 0x05]);
        bytes.extend_from_slice(&[0; 1344]);
        bytes.push(2);
        first.zcash_serialize(&mut bytes).unwrap();
        refund.zcash_serialize(&mut bytes).unwrap();
        let block = Block::zcash_deserialize(bytes.as_slice()).unwrap();
        let txid = refund.hash().0;
        let mut payment = super::common::record(0, 1).payment;
        payment.tx_index = 1;
        assert_eq!(check_tx_index(&block, &txid, &payment), Ok(true));
        payment.tx_index = 0;
        assert_eq!(check_tx_index(&block, &txid, &payment), Ok(false));
        let missing = check_tx_index(&block, &[0; 32], &payment);
        assert_eq!(missing.unwrap_err().0, "oracle_invalid");
    }

    /// A session manifest whose directory salt starts with `salt`.
    fn manifest(salt: u8) -> Manifest {
        let mut directory = super::common::manifest(receiver_pir::MIN_ROWS);
        directory.salt[0] = salt;
        Manifest {
            protocol: receiver_pir::PROTOCOL.into(),
            directory,
            public_digest: [0; 32],
        }
    }

    /// A service whose origin serves `current` and whose health reports `serving`, and
    /// a route with a body over the health bound. Returns its origin.
    async fn serve(current: Manifest, serving: String) -> String {
        let app = Router::new()
            .route(
                "/v1/receiver/init",
                routing::get(move || async move { Json(current) }),
            )
            .route(
                "/health",
                routing::get(move || async move {
                    Json(json!({"serving": serving, "indexer": {"payouts_missing": 0}}))
                }),
            )
            .route(
                "/large",
                routing::get(|| async { vec![b' '; MAX_HEALTH_BYTES + 1] }),
            );
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", socket.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
        origin
    }

    /// A filter file is accepted only with the manifest's digest.
    #[tokio::test]
    async fn the_filter_file_must_match_its_manifest() {
        let snapshot = receiver_directory::snapshot::Snapshot::build(
            super::common::manifest(receiver_pir::MIN_ROWS),
            &[],
            &[],
        )
        .unwrap();
        let serve = |bytes: Vec<u8>| async move {
            let app = Router::new().route(
                "/v1/receiver/filters/x",
                routing::get(move || async move { bytes }),
            );
            let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let origin = format!("http://{}", socket.local_addr().unwrap());
            tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
            origin
        };
        let http = reqwest::Client::new();
        let origin = serve(snapshot.filters.clone()).await;
        let check = check_filters(&http, &origin, "x", &snapshot.manifest);
        assert!(check.await.unwrap().is_none());
        let mut altered = snapshot.filters.clone();
        *altered.last_mut().unwrap() ^= 1;
        let origin = serve(altered).await;
        let check = check_filters(&http, &origin, "x", &snapshot.manifest);
        assert_eq!(check.await.unwrap().unwrap().0, "answer_mismatch");
    }

    /// Health's report counts only for the probed publication, or the one the origin
    /// serves after a rotation, and bodies are bounded.
    #[tokio::test]
    async fn the_report_is_bound_to_the_probed_publication() {
        let (probed, rotated) = (manifest(1), manifest(2));
        let id = |m: &Manifest| hex::encode(m.id().unwrap());
        let http = reqwest::Client::new();
        let report = |origin: String| {
            let (http, id) = (http.clone(), id(&probed));
            async move {
                indexer_report(&http, &origin, &format!("{origin}/health"), &id)
                    .await
                    .unwrap()
            }
        };
        let origin = serve(probed.clone(), id(&probed)).await;
        assert!(report(origin.clone()).await.is_none());
        assert!(get(&http, &format!("{origin}/large"), MAX_HEALTH_BYTES)
            .await
            .is_err());
        // The service rotated after the lookup.
        let origin = serve(rotated.clone(), id(&rotated)).await;
        assert!(report(origin).await.is_none());
        // Health answers for a publication the origin does not serve.
        let origin = serve(probed.clone(), id(&rotated)).await;
        assert_eq!(report(origin).await.unwrap().0, "report_unavailable");
    }
}
