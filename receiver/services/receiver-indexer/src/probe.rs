//! A `pir-monitor` service probe for the receiver directory, also run as a deploy's
//! exact check. With `--await-feed-reads` it first waits for the running process to
//! read both NEAR feeds (`feeds_not_read` otherwise). It then checks the served
//! publication against independent nodes, one at a time and freshest first (see
//! [`oracle`]), looks up a pinned historical payment over live encrypted PIR, as
//! Transparent's canary checks one query against a pinned row, checks the witness file
//! (with `--witnesses`) and the filter file against the manifest, then checks the NEAR
//! feed's freshness and the indexer's payout check, which health reports on the private
//! network. It prints one JSON line: `passed`, on failure a `category` and `detail`,
//! and the lookup as `phase: "live_encrypted_probe"` with `queries` and `correct`.
//! `answer_mismatch` marks served data that is wrong, which the monitor treats as a
//! correctness incident; `oracle_invalid` a fixture that fails its pin, including an
//! Action whose recovered receiver differs from the fixture's pinned one; anything else,
//! such as `oracle_unavailable` when no node that reached the publication can complete
//! the chain checks, is an availability failure. Every response body is bounded before
//! it is buffered.
use clap::Parser;
use receiver_directory::{
    extract::Action,
    filter::{Filters, MAX_FILTERS_BYTES},
    witness::{WitnessSnapshot, MAX_WITNESS_BYTES},
    Hash, Payment, Receiver,
};
use receiver_indexer::{
    near::Feed,
    zakura::{ZakuraClient, ZakuraError},
};
use receiver_pir::{
    public_bytes, response_bytes, transport::MAX_PIR_PAGES, AcceptedCoverage, Client, Manifest,
    MAX_MANIFEST_BYTES,
};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{path::PathBuf, time::Duration};

/// The recent set's largest age that wallets still trust (`zakura-pir-receiver`).
const MAX_RECENT_AGE_SECS: i64 = 15 * 60;
/// Bound on the health report, a few hundred bytes.
const MAX_HEALTH_BYTES: usize = 64 * 1024;
/// How often `--await-feed-reads` polls health.
const FEED_READS_POLL: Duration = Duration::from_secs(2);

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
    /// The pinned payment: a public zero-OVK Action with its transaction, height, note
    /// position and independently decoded receiver (see [`Fixture`]).
    #[arg(long)]
    fixture: PathBuf,
    /// The fixture file's SHA-256, hex.
    #[arg(long)]
    fixture_sha256: String,
    /// An independent node's RPC endpoint. Repeat it for fallbacks: the nodes that
    /// reached the publication check it one at a time, highest tip first and in this
    /// order among equal tips, until one completes the checks.
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
    /// Before any network check, wait up to this many seconds for health's `near.reads`
    /// to show that the running process completed a read of both NEAR feeds, as a
    /// deploy that sets the partner key must. Feeds are read in turn at the explorer's
    /// 5.5-second request pacing (about 40 seconds a poll at October 2026 volume), and
    /// the 60-second poll interval starts after both, so the deploy's 300 allows about
    /// three polls. That is a budget, not a worst-case bound: a first read of a new
    /// provider database can take far longer. Without this flag, as in the monitor's
    /// periodic probe, nothing waits.
    #[arg(long, value_name = "SECS", value_parser = clap::value_parser!(u64).range(1..))]
    await_feed_reads: Option<u64>,
    /// The service serves witness files, as the indexer does with `--witnesses`: each
    /// run then checks the session's file against the nodes' Ironwood root. The probe
    /// cannot infer this from the service, which would answer a lost file as one never
    /// configured, so the deployment states it.
    #[arg(long)]
    witnesses: bool,
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
    /// The Orchard receiver the Action pays, hex, decoded independently of zero-OVK
    /// recovery (from the swap's refund address). Recovery must reproduce it, so a
    /// recovery regression shared by the indexer and the probe cannot pass.
    receiver: String,
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
    let Ok(fixture) = serde_json::from_slice::<Fixture>(&raw) else {
        return Ok(Some(("oracle_invalid", json!({"fixture": "malformed"}))));
    };
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
    let Some(receiver) = bytes(&fixture.receiver).and_then(|b| Receiver::from_bytes(b).ok()) else {
        return Ok(Some(("oracle_invalid", json!({"fixture": "receiver"}))));
    };
    match action.recover_receiver() {
        Ok(Some(recovered)) if recovered == receiver => {}
        Ok(Some(_)) => {
            return Ok(Some((
                "oracle_invalid",
                json!({"fixture": "recovered receiver differs from the pin"}),
            )))
        }
        _ => return Ok(Some(("oracle_invalid", json!({"fixture": "not zero-OVK"})))),
    }
    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(20))
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    if let Some(secs) = args.await_feed_reads {
        let wait = Duration::from_secs(secs);
        let gate = await_feed_reads(&http, &args.health_url, wait, FEED_READS_POLL);
        if let Some(failure) = gate.await {
            return Ok(Some(failure));
        }
    }
    let origin = args.origin.trim_end_matches('/');
    let manifest = fetch_manifest(&http, origin).await?;
    let directory = &manifest.directory;
    if directory.start_height > fixture.height || directory.end_height < fixture.height {
        return Ok(Some((
            "oracle_invalid",
            json!({"fixture": "outside coverage"}),
        )));
    }
    let oracle = oracle(
        &args.rpc_url,
        args.cookie.as_deref(),
        directory,
        fixture.height,
        args.witnesses,
    );
    let (verified, tip) = match oracle.await? {
        Ok(found) => found,
        Err(failure) => return Ok(Some(failure)),
    };
    if let Some(failure) = check_lag(tip, directory, args.max_lag) {
        return Ok(Some(failure));
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
        Some(payment) if expected(payment) && verified.fixture_hash == payment.block_hash => {
            match check_tx_index(&verified.fixture_block, &txid, payment) {
                Ok(correct) => correct,
                Err(failure) => {
                    *lookup = Some((queries, false));
                    return Ok(Some(failure));
                }
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
    if let (Some(root), Some(payment)) = (verified.root, &found) {
        let check = check_witnesses(&http, origin, &id, directory, payment, root);
        if let Some(failure) = check.await? {
            return Ok(Some(failure));
        }
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

/// Polls `health_url` every `poll` until its `near.reads` holds a read time for both
/// feeds, each request bounded by the time left of `wait`. At the deadline it fails as
/// `feeds_not_read` with the last reads seen and the last request error.
async fn await_feed_reads(
    http: &reqwest::Client,
    health_url: &str,
    wait: Duration,
    poll: Duration,
) -> Option<Failure> {
    let deadline = tokio::time::Instant::now() + wait;
    let (mut reads, mut error) = (Value::Null, None);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Some((
                "feeds_not_read",
                json!({"waited_secs": wait.as_secs(), "reads": reads, "last_error": error}),
            ));
        }
        let health = tokio::time::timeout(remaining, get(http, health_url, MAX_HEALTH_BYTES));
        match health.await {
            Ok(Ok(body)) => match serde_json::from_slice::<Value>(&body) {
                Ok(health) => {
                    reads = health["near"]["reads"].clone();
                    if [Feed::Payouts, Feed::Refunds]
                        .iter()
                        .all(|feed| reads[feed.name()].as_i64().is_some())
                    {
                        return None;
                    }
                }
                Err(e) => error = Some(e.to_string()),
            },
            Ok(Err(e)) => error = Some(e.to_string()),
            Err(_) => error = Some("health did not answer before the deadline".to_owned()),
        }
        tokio::time::sleep_until((tokio::time::Instant::now() + poll).min(deadline)).await;
    }
}

/// Checks the publication's anchor against `rpc`: its hash and genesis, and the tree
/// size after it (read by that hash, see [`ZakuraClient::receiver_boundary`]), which is
/// the coverage the manifest claims. A read that fails or a chain that changes during
/// it is an error, which proves nothing against the publication.
async fn check_anchor(
    rpc: &ZakuraClient,
    directory: &receiver_directory::snapshot::Manifest,
) -> std::result::Result<Option<Failure>, ZakuraError> {
    let height = directory.end_height;
    let boundary = rpc.receiver_boundary(height).await?;
    let genesis = parse_hash(&rpc.block_hash(0).await?)?;
    if boundary.hash != directory.end_hash || genesis != directory.genesis {
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
    Ok(None)
}

/// A displayed block hash in protocol byte order.
fn parse_hash(displayed: &str) -> std::result::Result<Hash, ZakuraError> {
    displayed
        .parse::<zakura_chain::block::Hash>()
        .map(|hash| hash.0)
        .map_err(|e| ZakuraError::Block(e.to_string()))
}

/// Whether the publication trails `tip`, the highest any node reported, by more than
/// `max_lag` blocks.
fn check_lag(
    tip: u64,
    directory: &receiver_directory::snapshot::Manifest,
    max_lag: u64,
) -> Option<Failure> {
    let height = u64::from(directory.end_height);
    (tip.saturating_sub(height) > max_lag).then(|| {
        (
            "stale_publication",
            json!({"end_height": height, "node_tip": tip}),
        )
    })
}

/// What one node gave for checking the served data against.
struct Verified {
    /// The node's block hash at the fixture's height.
    fixture_hash: Hash,
    /// That block, read by its hash and checked against its merkle root.
    fixture_block: zakura_chain::block::Block,
    /// The Ironwood root after the publication's terminal block, when witness files
    /// are checked.
    root: Option<Hash>,
}

/// The outcome of [`verify`] on one node.
enum Verdict {
    /// Every check passed.
    Verified(Verified),
    /// Valid evidence against the publication or the fixture. It is final.
    Failed(Failure),
    /// The node could not complete the checks, so the next one tries.
    Unavailable(ZakuraError),
}

/// Checks the publication against one node: the anchor ([`check_anchor`]), then the
/// fixture's canonical block at `fixture_height` and, with `witnesses`, the Ironwood
/// root after the terminal block, read by its hash. Its reads come from one chain: the
/// terminal block must still be the node's after the last of them.
async fn verify(
    rpc: &ZakuraClient,
    directory: &receiver_directory::snapshot::Manifest,
    fixture_height: u32,
    witnesses: bool,
) -> Verdict {
    let checks = async {
        if let Some(failure) = check_anchor(rpc, directory).await? {
            return Ok(Err(failure));
        }
        let fixture_hash = parse_hash(&rpc.block_hash(u64::from(fixture_height)).await?)?;
        let fixture_block = rpc.receiver_block(fixture_hash).await?;
        let root = if witnesses {
            Some(
                rpc.ironwood_root(directory.end_hash, directory.end_height)
                    .await?,
            )
        } else {
            None
        };
        let end = parse_hash(&rpc.block_hash(u64::from(directory.end_height)).await?)?;
        if end != directory.end_hash {
            return Err(ZakuraError::Block("chain changed during the checks".into()));
        }
        Ok(Ok(Verified {
            fixture_hash,
            fixture_block,
            root,
        }))
    };
    match checks.await {
        Ok(Ok(verified)) => Verdict::Verified(verified),
        Ok(Err(failure)) => Verdict::Failed(failure),
        Err(error) => Verdict::Unavailable(error),
    }
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

/// Checks the session's common witness file, which wallets prove their payments with:
/// it must bind to the publication, prove `payment`, the fixture's as served, at its
/// position, and have `root`, the node's Ironwood root after the terminal block, so a
/// self-consistent file over another tree fails too. A file the service cannot send
/// is an error.
async fn check_witnesses(
    http: &reqwest::Client,
    origin: &str,
    id: &str,
    directory: &receiver_directory::snapshot::Manifest,
    payment: &Payment,
    root: Hash,
) -> Result<Option<Failure>> {
    let url = format!("{origin}/v1/receiver/witness/{id}");
    let bytes = get(http, &url, MAX_WITNESS_BYTES).await?;
    let proof = match WitnessSnapshot::decode(&bytes, directory) {
        Ok(proof) => proof,
        Err(error) => {
            return Ok(Some((
                "answer_mismatch",
                json!({"witnesses": error.to_string(), "witness_bytes": bytes.len()}),
            )))
        }
    };
    let proved = u32::try_from(payment.position)
        .is_ok_and(|position| proof.path(position, payment.cmx).is_ok());
    if !proved {
        return Ok(Some((
            "answer_mismatch",
            json!({"witness_path": payment.position}),
        )));
    }
    if proof.root() != root {
        return Ok(Some((
            "answer_mismatch",
            json!({"witness_root": hex::encode(proof.root()), "node_root": hex::encode(root)}),
        )));
    }
    Ok(None)
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

/// Checks the publication with [`verify`] on the nodes that have reached it, one at a
/// time, highest tip first and in `urls` order among equal tips. The first node to
/// complete the checks decides, and the first valid evidence against the publication
/// is final rather than retried on a friendlier node; only a node that could not
/// complete them hands over to the next. Returns the result with the highest tip any
/// node reported, from which lag is measured whichever node verified. With no node
/// completing the checks it is `oracle_unavailable`, with each node's tip or failure
/// and each attempt's error.
async fn oracle(
    urls: &[String],
    cookie: Option<&std::path::Path>,
    directory: &receiver_directory::snapshot::Manifest,
    fixture_height: u32,
    witnesses: bool,
) -> Result<std::result::Result<(Verified, u64), Failure>> {
    let height = u64::from(directory.end_height);
    let (mut tips, mut reached, mut top) = (Vec::<Value>::new(), Vec::new(), 0);
    for (node, url) in urls.iter().enumerate() {
        let rpc = match cookie {
            Some(path) => ZakuraClient::from_cookie_file(vec![url.clone()], path)?,
            None => ZakuraClient::unauthenticated(vec![url.clone()])?,
        };
        match rpc.tip_height().await {
            Ok(tip) => {
                tips.push(tip.into());
                top = top.max(tip);
                if tip >= height {
                    reached.push((node, rpc, tip));
                }
            }
            Err(error) => tips.push(error.to_string().into()),
        }
    }
    // A stable sort keeps the configured order among equal tips.
    reached.sort_by_key(|(_, _, tip)| std::cmp::Reverse(*tip));
    let mut attempts = Vec::new();
    for (node, rpc, _) in &reached {
        match verify(rpc, directory, fixture_height, witnesses).await {
            Verdict::Verified(verified) => return Ok(Ok((verified, top))),
            Verdict::Failed(failure) => return Ok(Err(failure)),
            Verdict::Unavailable(error) => {
                attempts.push(json!({"node": node, "error": error.to_string()}))
            }
        }
    }
    Ok(Err((
        "oracle_unavailable",
        json!({"end_height": height, "nodes": tips, "attempts": attempts}),
    )))
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
                match check_anchor(&rpc, &directory).await {
                    Ok(failure) => failure.map(|failure| failure.0),
                    Err(_) => Some("unverified"),
                }
            }
        };
        assert_eq!(check(chain(tip, 3, Some(300), false).await).await, None);
        for (hash, size, reorg, expected) in [
            (3, Some(301), false, "answer_mismatch"),
            (4, Some(300), false, "answer_mismatch"),
            (3, None, false, "unverified"),
            (3, Some(300), true, "unverified"),
        ] {
            let rpc = chain(tip, hash, size, reorg).await;
            assert_eq!(check(rpc).await, Some(expected), "{hash} {size:?} {reorg}");
        }
    }

    /// `--max-lag` defaults to 12 and bounds the publication's lag inclusively.
    #[test]
    fn the_lag_bound_is_configurable() {
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
            assert_eq!(
                check_lag(height + lag, &directory, max_lag).map(|f| f.0),
                stale.then_some("stale_publication")
            );
        }
    }

    /// A block holding a transaction before the fixture's refund, with a valid merkle
    /// root, and the refund's ID.
    fn fixture_block() -> (zakura_chain::block::Block, Hash) {
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
        let mut block = Block::zcash_deserialize(bytes.as_slice()).unwrap();
        let root = block.transactions.iter().collect();
        std::sync::Arc::make_mut(&mut block.header).merkle_root = root;
        (block, refund.hash().0)
    }

    /// A node for [`oracle`] over [`anchored`]: its tip and the hash it gives at the
    /// anchor's height, `[anchor; 32]`, with the tree size after it. Genesis is
    /// `[1; 32]`, as [`super::common::manifest`] declares, and the block below the
    /// anchor is [`fixture_block`]. `fails` names the reads it refuses: every
    /// `getblockhash` but genesis, or the raw `getblock`. With `moved`, the anchor's
    /// height holds `[9; 32]` after the boundary's two reads, as when the chain changes
    /// during the checks. Its `z_gettreestate` gives `root`, or no root for `None`, and
    /// names another block when `fails` is `"treestate block"`.
    #[derive(Clone, Copy)]
    struct Node {
        tip: u64,
        anchor: u8,
        size: Option<u64>,
        fails: &'static str,
        moved: bool,
        root: Option<Hash>,
    }

    /// A node at tip `tip` that agrees with [`anchored`].
    fn good(tip: u64) -> Node {
        Node {
            tip,
            anchor: 3,
            size: Some(300),
            fails: "",
            moved: false,
            root: Some([7; 32]),
        }
    }

    /// Serves `node`. Returns its URL.
    async fn serve_node(node: Node) -> String {
        use zakura_chain::serialization::ZcashSerialize;
        let end = u64::from(anchored().end_height);
        let (block, _) = fixture_block();
        let fixture = block.hash().to_string();
        let raw = hex::encode(block.zcash_serialize_to_vec().unwrap());
        let reads = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let app = Router::new().route(
            "/",
            routing::post(move |Json(request): Json<Value>| {
                let (fixture, raw, reads) = (fixture.clone(), raw.clone(), reads.clone());
                async move {
                    let display = |byte| zakura_chain::block::Hash([byte; 32]).to_string();
                    let (method, params) =
                        (request["method"].as_str().unwrap(), &request["params"]);
                    let refused = match method {
                        "getblockhash" => node.fails == method && params[0] != 0,
                        "getblock" => node.fails == "raw getblock" && params[1] == 0,
                        _ => false,
                    };
                    if refused {
                        let error = json!({"code": -1, "message": "refused"});
                        return Json(json!({"result": null, "error": error}));
                    }
                    let result = match (method, params[0].as_u64()) {
                        ("getblockcount", _) => json!(node.tip),
                        ("getblockhash", Some(0)) => json!(display(1)),
                        ("getblockhash", Some(height)) if height == end => {
                            let read = reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            json!(display(if node.moved && read >= 2 {
                                9
                            } else {
                                node.anchor
                            }))
                        }
                        ("getblockhash", Some(height)) if height == end - 1 => json!(fixture),
                        ("getblock", _) if params[1] == 1 => match node.size {
                            Some(size) => json!({"trees": {"ironwood": {"size": size}}}),
                            None => json!({"trees": {}}),
                        },
                        ("getblock", _) => json!(raw),
                        ("z_gettreestate", _) => {
                            let hash = match node.fails {
                                "treestate block" => json!(display(9)),
                                _ => params[0].clone(),
                            };
                            let commitments = match node.root {
                                Some(root) => json!({"finalRoot": hex::encode(root)}),
                                None => json!({}),
                            };
                            json!({"hash": hash, "height": end, "ironwood": {"commitments": commitments}})
                        }
                        _ => panic!("unexpected {request}"),
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

    /// What [`oracle`] decides over `nodes`, listed in this order, reading roots with
    /// `witnesses`: the highest tip and the root once a node verifies the fixture's
    /// block, or the failure.
    async fn decide(
        nodes: &[Node],
        witnesses: bool,
    ) -> std::result::Result<(u64, Option<Hash>), Failure> {
        let mut urls = Vec::new();
        for node in nodes {
            urls.push(serve_node(*node).await);
        }
        let directory = anchored();
        let fixture_height = directory.end_height - 1;
        let (verified, top) = oracle(&urls, None, &directory, fixture_height, witnesses)
            .await
            .unwrap()?;
        assert_eq!(verified.fixture_hash, fixture_block().0.hash().0);
        Ok((top, verified.root))
    }

    /// A node that cannot complete the checks hands over to the next that reached the
    /// publication, highest tip first, and a node behind the publication is never
    /// asked. Lag is measured from the highest tip whichever node verified, so falling
    /// back to a lower node cannot hide it.
    #[tokio::test]
    async fn nodes_that_cannot_verify_hand_over_to_the_next() {
        let end = u64::from(anchored().end_height);
        // A node behind on another chain would contradict the anchor if asked.
        let behind = Node {
            anchor: 4,
            ..good(end - 1)
        };
        let nodes = [
            behind,
            good(end),
            Node {
                fails: "getblockhash",
                ..good(end + 50)
            },
            Node {
                fails: "raw getblock",
                ..good(end + 40)
            },
            Node {
                moved: true,
                ..good(end + 30)
            },
            Node {
                size: None,
                ..good(end + 20)
            },
        ];
        let (top, _) = decide(&nodes, false).await.unwrap();
        assert_eq!(top, end + 50);
        assert_eq!(
            check_lag(top, &anchored(), 12).unwrap().0,
            "stale_publication"
        );
        // With every node that reached the publication failing, each attempt is listed.
        let (category, detail) = decide(&nodes[2..], false).await.unwrap_err();
        assert_eq!(category, "oracle_unavailable");
        assert_eq!(detail["attempts"].as_array().unwrap().len(), 4);
        assert_eq!(detail["nodes"].as_array().unwrap().len(), 4);
        let (category, detail) = decide(&[behind], false).await.unwrap_err();
        assert_eq!(category, "oracle_unavailable");
        assert!(detail["attempts"].as_array().unwrap().is_empty());
    }

    /// Valid evidence against the publication from the first node to complete the
    /// checks is final, even when a later node would agree with it, and equal tips
    /// keep their configured order.
    #[tokio::test]
    async fn valid_evidence_against_the_publication_is_final() {
        let end = u64::from(anchored().end_height);
        let off_chain = Node {
            anchor: 4,
            ..good(end + 2)
        };
        let short = Node {
            size: Some(299),
            ..good(end + 2)
        };
        for first in [off_chain, short] {
            let (category, _) = decide(&[good(end + 1), first], false).await.unwrap_err();
            assert_eq!(category, "answer_mismatch");
            let (category, _) = decide(&[first, good(end + 2)], false).await.unwrap_err();
            assert_eq!(category, "answer_mismatch");
            assert_eq!(
                decide(&[good(end + 2), first], false).await,
                Ok((end + 2, None))
            );
        }
    }

    /// With witness files checked, the root comes from the first node that gives one for
    /// the terminal block: a node without root data, or whose tree state names another
    /// block as when the chain moves during the read, hands over to the next, and with
    /// none giving it the probe is unavailable, never passing. Without witness files,
    /// no root is read.
    #[tokio::test]
    async fn the_root_comes_from_a_node_that_has_it() {
        let end = u64::from(anchored().end_height);
        let rootless = Node {
            root: None,
            ..good(end + 3)
        };
        let moved = Node {
            fails: "treestate block",
            ..good(end + 2)
        };
        let other = Node {
            root: Some([8; 32]),
            ..good(end + 1)
        };
        assert_eq!(
            decide(&[rootless, moved, other], true).await,
            Ok((end + 3, Some([8; 32])))
        );
        let (category, detail) = decide(&[rootless, moved], true).await.unwrap_err();
        assert_eq!(category, "oracle_unavailable");
        assert_eq!(detail["attempts"].as_array().unwrap().len(), 2);
        assert_eq!(decide(&[rootless, moved], false).await, Ok((end + 3, None)));
    }

    /// The node's Ironwood root and the witness file's agree byte for byte: the pinned
    /// node's tree over the fixture refund's commitment gives, through
    /// `Root::bytes_in_display_order`, the root `WitnessSnapshot` builds, not its reverse.
    #[test]
    fn the_node_and_the_witness_file_encode_roots_alike() {
        use zakura_chain::serialization::ZcashDeserialize;
        let raw = hex::decode(include_str!("../tests/fixtures/receiver-refund.hex").trim());
        let refund =
            zakura_chain::transaction::Transaction::zcash_deserialize(raw.unwrap().as_slice())
                .unwrap();
        let cmx = refund.ironwood_actions().next().unwrap().cm_x;
        let mut tree = zakura_chain::orchard::tree::NoteCommitmentTree::default();
        tree.append(cmx).unwrap();
        let node_root = tree.root().bytes_in_display_order();
        let mut manifest = super::common::manifest(receiver_pir::MIN_ROWS);
        (manifest.start_position, manifest.end_position) = (0, 1);
        let positions = Default::default();
        let file = WitnessSnapshot::build(&manifest, &[cmx.into()], &positions).unwrap();
        assert_eq!(file.root(), node_root);
        let mut reversed = node_root;
        reversed.reverse();
        assert_ne!(file.root(), reversed);
    }

    /// A publication with records at positions 0 and 5 of `tree`, the witness file for
    /// it built from `served`, and the payment at position 5.
    fn witnessed(
        tree: &[Hash],
        served: &[Hash],
    ) -> (receiver_directory::snapshot::Manifest, Vec<u8>, Payment) {
        let mut manifest = super::common::manifest(receiver_pir::MIN_ROWS);
        manifest.start_position = 0;
        manifest.end_position = tree.len() as u64;
        let records: Vec<_> = [0u8, 5]
            .into_iter()
            .enumerate()
            .map(|(page, position)| {
                let mut record = super::common::record(page as u32, 2);
                record.payment.position = position.into();
                record.payment.cmx = tree[usize::from(position)];
                record
            })
            .collect();
        let snapshot =
            receiver_directory::snapshot::Snapshot::build(manifest, &records, &[]).unwrap();
        let positions = [0, 5].into_iter().collect();
        let proof = WitnessSnapshot::build(&snapshot.manifest, served, &positions).unwrap();
        (
            snapshot.manifest,
            proof.encode(),
            records[1].payment.clone(),
        )
    }

    /// The witness file must bind to the probed publication, prove the fixture's
    /// commitment at its position and have the node's root; a file the service cannot
    /// send, or one over its bound, fails the probe too.
    #[tokio::test]
    async fn the_witness_file_must_prove_the_fixture_under_the_nodes_root() {
        let tree: Vec<Hash> = (1..=8).map(|i| [i; 32]).collect();
        let (directory, proof, payment) = witnessed(&tree, &tree);
        let root = WitnessSnapshot::decode(&proof, &directory).unwrap().root();
        let serve = |status: u16, bytes: Vec<u8>| async move {
            let status = axum::http::StatusCode::from_u16(status).unwrap();
            let app = Router::new().route(
                "/v1/receiver/witness/x",
                routing::get(move || async move { (status, bytes) }),
            );
            let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let origin = format!("http://{}", socket.local_addr().unwrap());
            tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
            origin
        };
        let http = reqwest::Client::new();
        let check = |origin: String, payment: Payment| {
            let (http, directory) = (http.clone(), directory.clone());
            async move {
                check_witnesses(&http, &origin, "x", &directory, &payment, root)
                    .await
                    .map(|failure| failure.map(|f| f.1))
                    .map_err(|e| e.to_string())
            }
        };
        assert_eq!(
            check(serve(200, proof.clone()).await, payment.clone()).await,
            Ok(None)
        );
        for (status, bytes) in [(503, Vec::new()), (200, vec![0; MAX_WITNESS_BYTES + 1])] {
            assert!(check(serve(status, bytes).await, payment.clone())
                .await
                .is_err());
        }
        let truncated = proof[..proof.len() - 1].to_vec();
        let detail = check(serve(200, truncated).await, payment.clone()).await;
        assert!(detail.unwrap().unwrap()["witnesses"].is_string());
        // Another revision's file, for the same tree under another salt.
        let mut other = directory.clone();
        other.salt[0] ^= 1;
        let stale = WitnessSnapshot::build(&other, &tree, &[0, 5].into_iter().collect());
        let detail = check(serve(200, stale.unwrap().encode()).await, payment.clone()).await;
        assert!(detail.unwrap().unwrap()["witnesses"].is_string());
        let mut wrong = payment.clone();
        wrong.cmx = [9; 32];
        let detail = check(serve(200, proof.clone()).await, wrong).await;
        assert_eq!(detail.unwrap().unwrap()["witness_path"], 5);
        // A file over another tree that still holds the payment proves it, but under a
        // root the node does not have.
        let mut forged = tree.clone();
        forged[2] = [20; 32];
        let (_, forged, _) = witnessed(&tree, &forged);
        let detail = check(serve(200, forged).await, payment).await;
        assert_eq!(detail.unwrap().unwrap()["node_root"], hex::encode(root));
    }

    /// The served payment must carry its transaction's index in the oracle's block,
    /// and a fixture transaction missing from that block is invalid.
    #[test]
    fn the_transaction_index_comes_from_the_oracles_block() {
        let (block, txid) = fixture_block();
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

    /// A health route that answers its `n`th request, from 0, with `answer(n)`. Returns
    /// its URL and the request count.
    async fn health_route(
        answer: impl Fn(usize) -> (axum::http::StatusCode, String) + Clone + Send + Sync + 'static,
    ) -> (String, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
        use std::sync::{atomic::Ordering, Arc};
        let asked = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count = asked.clone();
        let app = Router::new().route(
            "/health",
            routing::get(move || {
                let answer = answer.clone();
                let n = count.fetch_add(1, Ordering::SeqCst);
                async move { answer(n) }
            }),
        );
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/health", socket.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
        (url, asked)
    }

    /// A health body whose `near.reads` are `payouts` and `refunds`.
    fn reads(payouts: Value, refunds: Value) -> (axum::http::StatusCode, String) {
        let health = json!({"near": {"reads": {"near-payouts": payouts, "near-refunds": refunds}}});
        (axum::http::StatusCode::OK, health.to_string())
    }

    /// The feed gate passes once health shows both feeds read, however late within its
    /// wait, and otherwise fails at its deadline with the last reads and request error.
    #[tokio::test]
    async fn the_feed_gate_waits_for_both_reads() {
        use std::sync::atomic::Ordering;
        let http = reqwest::Client::new();
        let poll = Duration::from_millis(50);
        let gate = |url: String, secs| {
            let http = http.clone();
            async move { await_feed_reads(&http, &url, Duration::from_secs(secs), poll).await }
        };
        let (url, asked) = health_route(|_| reads(json!(5), json!(6))).await;
        assert_eq!(gate(url, 30).await, None);
        assert_eq!(asked.load(Ordering::SeqCst), 1);
        // The refund feed's read appears on the fourth request.
        let (url, asked) =
            health_route(|n| reads(json!(5), if n < 3 { Value::Null } else { json!(6) })).await;
        assert_eq!(gate(url, 30).await, None);
        assert_eq!(asked.load(Ordering::SeqCst), 4);
        let (url, _) = health_route(|_| reads(json!(5), Value::Null)).await;
        let (category, detail) = gate(url, 1).await.unwrap();
        assert_eq!(category, "feeds_not_read");
        assert_eq!(
            detail,
            json!({"waited_secs": 1, "reads": {"near-payouts": 5, "near-refunds": null},
                "last_error": null})
        );
        // A failed request is reported after later answers, and a body without reads
        // (an older server) never passes.
        let (url, _) = health_route(|n| match n {
            0 => (axum::http::StatusCode::SERVICE_UNAVAILABLE, String::new()),
            _ => (
                axum::http::StatusCode::OK,
                json!({"serving": "x"}).to_string(),
            ),
        })
        .await;
        let (category, detail) = gate(url, 1).await.unwrap();
        assert_eq!(
            (category, &detail["reads"]),
            ("feeds_not_read", &Value::Null)
        );
        assert!(
            detail["last_error"].as_str().unwrap().contains("503"),
            "{detail}"
        );
    }

    /// With `--await-feed-reads`, a gate that fails ends the probe before it asks the
    /// origin for anything.
    #[tokio::test]
    async fn the_feed_gate_runs_before_the_origin_is_asked() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let asked = std::sync::Arc::new(AtomicUsize::new(0));
        let count = asked.clone();
        let app = Router::new().route(
            "/v1/receiver/init",
            routing::get(move || {
                count.fetch_add(1, Ordering::SeqCst);
                async { axum::http::StatusCode::SERVICE_UNAVAILABLE }
            }),
        );
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", socket.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
        let (health, _) = health_route(|_| reads(Value::Null, Value::Null)).await;
        let fixture = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../ops/digitalocean/probe-fixture.json"
        );
        let digest = hex::encode(Sha256::digest(std::fs::read(fixture).unwrap()));
        let argv = |extra: &[&str]| {
            let required = [
                "receiver-probe",
                "--origin",
                &origin,
                "--health-url",
                &health,
                "--fixture",
                fixture,
                "--fixture-sha256",
                &digest,
                "--rpc-url",
                "http://127.0.0.1:1",
                "--no-auth",
            ];
            Args::try_parse_from(required.iter().chain(extra))
        };
        let gated = argv(&["--await-feed-reads", "1"]).unwrap();
        let failure = probe(gated, &mut None).await;
        assert_eq!(failure.unwrap().unwrap().0, "feeds_not_read");
        assert_eq!(asked.load(Ordering::SeqCst), 0);
        // Without the flag the probe goes straight to the origin.
        assert!(probe(argv(&[]).unwrap(), &mut None).await.is_err());
        assert_eq!(asked.load(Ordering::SeqCst), 1);
        assert!(argv(&["--await-feed-reads", "0"]).is_err());
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

    /// The fixture's pinned receiver is checked against recovery before any request:
    /// a missing, malformed or invalid pin, or another valid receiver, is
    /// `oracle_invalid` and reaches no server, while the independent pin goes on.
    #[tokio::test]
    async fn the_fixture_receiver_pin_is_checked_before_any_request() {
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };
        let requests = Arc::new(AtomicUsize::new(0));
        let counted = requests.clone();
        let app = Router::new().fallback(move || {
            counted.fetch_add(1, Ordering::SeqCst);
            async { axum::http::StatusCode::NOT_FOUND }
        });
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", socket.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fixture.json");
        let run = |fixture: Value| {
            let bytes = serde_json::to_vec(&fixture).unwrap();
            std::fs::write(&path, &bytes).unwrap();
            let args = Args::try_parse_from([
                "receiver-probe",
                "--origin",
                &url,
                "--health-url",
                &format!("{url}/health"),
                "--fixture",
                path.to_str().unwrap(),
                "--fixture-sha256",
                &hex::encode(Sha256::digest(&bytes)),
                "--rpc-url",
                &url,
                "--no-auth",
            ])
            .unwrap();
            async move { probe(args, &mut None).await }
        };
        let mut fixture: Value = serde_json::from_str(include_str!(
            "../../../crates/receiver-directory/tests/fixtures/zero-ovk-action.json"
        ))
        .unwrap();
        fixture["position"] = 0.into();
        assert_eq!(fixture["receiver"], super::common::RECEIVER_HEX);
        let other = {
            use orchard::keys::{FullViewingKey, Scope, SpendingKey};
            let fvk = FullViewingKey::from(&SpendingKey::from_bytes([3; 32]).unwrap());
            hex::encode(fvk.address_at(0u32, Scope::External).to_raw_address_bytes())
        };
        for (pin, detail) in [
            (None, "malformed"),
            (Some("zz".to_owned()), "receiver"),
            (
                Some(super::common::RECEIVER_HEX[2..].to_owned()),
                "receiver",
            ),
            // The identity point is not a transmission key.
            (Some("00".repeat(43)), "receiver"),
            (Some(other), "recovered receiver differs from the pin"),
        ] {
            let mut altered = fixture.clone();
            match pin {
                Some(pin) => altered["receiver"] = pin.into(),
                None => {
                    altered.as_object_mut().unwrap().remove("receiver");
                }
            }
            let failure = run(altered).await.unwrap();
            assert_eq!(
                failure,
                Some(("oracle_invalid", json!({"fixture": detail})))
            );
            assert_eq!(requests.load(Ordering::SeqCst), 0, "{detail}");
        }
        // The independent pin passes, so the probe asks the origin for its manifest.
        assert!(run(fixture).await.is_err());
        assert!(requests.load(Ordering::SeqCst) > 0);
    }
}
