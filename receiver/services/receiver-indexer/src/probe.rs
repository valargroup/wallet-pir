//! A `pir-monitor` service probe for the receiver directory, run as `receiver-directory
//! probe` or `receiver-probe`, and a deploy's exact check. With `--await-feed-reads` it
//! first waits for the running process to read both NEAR feeds (`feeds_not_read`
//! otherwise). It then checks the served publication against independent nodes, one at
//! a time and freshest first, looks up a pinned historical payment over live encrypted
//! PIR, as Transparent's canary checks one query against a pinned row, checks the
//! witness file (with `--witnesses`) and the filter file against the manifest, then
//! checks the NEAR feed's freshness and the indexer's payout check, which health
//! reports on the private network. It prints one JSON line: `passed`, on failure a
//! `category` and `detail`, and the lookup as `phase: "live_encrypted_probe"` with
//! `queries` and `correct`. `answer_mismatch` marks served data that is wrong, which
//! the monitor treats as a correctness incident; `oracle_invalid` a fixture that fails
//! its pin, including an Action whose recovered receiver differs from the fixture's
//! pinned one; anything else, such as `oracle_unavailable` when no node that reached
//! the publication can complete the chain checks, or `payouts_uncheckable` when the
//! indexer's report counts a recent completed payout that NEAR gave no usable
//! transaction for, is an availability failure. Every response body is bounded before
//! it is buffered.
use crate::{
    near::Feed,
    read_limited,
    zakura::{ZakuraClient, ZakuraError},
};
use clap::Parser;
use receiver_directory::{
    extract::Action,
    filter::{Filters, MAX_FILTERS_BYTES, PAID},
    witness::{WitnessSnapshot, MAX_WITNESS_BYTES},
    Hash, Payment, Receiver,
};
use receiver_pir::{
    public_bytes, response_bytes, transport::MAX_PIR_PAGES, AcceptedCoverage, Client, Manifest,
    MAX_MANIFEST_BYTES,
};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{path::PathBuf, time::Duration};

/// The production fixture: a mainnet refund to a zero-OVK receiver (see `Fixture`).
const MAINNET_FIXTURE: &[u8] = include_bytes!("../fixtures/mainnet-probe.json");

/// The recent set's largest age that wallets still trust (`zakura-pir-receiver`).
const MAX_RECENT_AGE_SECS: i64 = 15 * 60;
/// Bound on the health report, a few hundred bytes.
const MAX_HEALTH_BYTES: usize = 64 * 1024;
/// How often `--await-feed-reads` polls health.
const FEED_READS_POLL: Duration = Duration::from_secs(2);

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// The probe's command line.
#[derive(Parser)]
pub struct Args {
    /// The receiver directory's public origin, which serves the wallet routes.
    #[arg(long)]
    origin: String,
    /// The service's health route on the private network, such as
    /// `http://10.70.0.11:18380/v1/receiver/health`; the public edge does not serve it.
    #[arg(long)]
    health_url: String,
    /// A fixture file to look up instead of the embedded mainnet one, for tests.
    #[arg(long)]
    fixture: Option<PathBuf>,
    /// An independent node's RPC endpoint. Repeat it for fallbacks: the nodes that
    /// reached the publication check it one at a time, ranked as
    /// [`ZakuraClient::ranked`] does, until one completes the checks.
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
    /// The hash of the block at `height`, displayed hex.
    block_hash: String,
    /// The transaction's index in that block.
    tx_index: u32,
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

/// Runs the probe, prints its JSON line and returns whether every check passed.
pub async fn run(args: Args) -> bool {
    let mut lookup = None;
    let (passed, category, detail) = match probe(args, &mut lookup).await {
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
    passed
}

/// The failed check, or `None` when every check passes. `lookup` records the encrypted
/// queries made and whether they found the fixture's payment.
async fn probe(args: Args, lookup: &mut Option<(u32, bool)>) -> Result<Option<Failure>> {
    let raw = match &args.fixture {
        Some(path) => std::fs::read(path)?,
        None => MAINNET_FIXTURE.to_vec(),
    };
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
    let block_hash = parse_hash(&fixture.block_hash).ok();
    let (Some(action), Some(txid), Some(block_hash)) = (action, txid, block_hash) else {
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
    let nodes = args
        .rpc_url
        .iter()
        .map(|url| ZakuraClient::new(url.clone(), args.cookie.as_deref()))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let pinned = (fixture.height, block_hash);
    let (rpc, root, tip) = match oracle(&nodes, directory, pinned, args.witnesses).await {
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
    // Wallets require history from Ironwood activation, independently of what the
    // manifest claims.
    let accepted = AcceptedCoverage {
        genesis: directory.genesis,
        required_start: crate::blocks::ironwood_activation(),
        height: directory.end_height,
        hash: directory.end_hash,
    };
    let client = match Client::new(manifest.clone(), &public, accepted) {
        Ok(client) => client,
        // Served history that starts after activation is wrong for every wallet.
        Err(receiver_pir::Error::Directory(receiver_directory::Error::Coverage)) => {
            return Ok(Some((
                "answer_mismatch",
                json!({"coverage": "starts after Ironwood activation"}),
            )))
        }
        Err(error) => return Err(error.into()),
    };
    let mut queries = 0;
    let found = loop {
        let query = client.prepare(receiver, queries)?;
        let response = http
            .post(format!("{origin}/v1/receiver/query"))
            .body(query.body().to_vec())
            .send()
            .await?;
        let response = read_limited(response.error_for_status()?, response_bytes(rows)?).await?;
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
            && p.block_hash == block_hash
            && p.txid == txid
            && p.tx_index == fixture.tx_index
            && p.action_index == fixture.action_index
            && p.action_nullifier == action.nullifier
            && p.cmx == action.cmx
            && p.ephemeral_key == action.ephemeral_key
            && p.ciphertext_prefix[..] == action.enc_ciphertext[..52]
    };
    let correct = found.as_ref().is_some_and(expected);
    *lookup = Some((queries, correct));
    if !correct {
        return Ok(Some((
            "answer_mismatch",
            json!({"lookup": found.map(|p| (p.height, p.position)), "queries": queries}),
        )));
    }
    if let (Some(root), Some(payment)) = (root, &found) {
        let check = check_witnesses(&http, origin, &id, directory, payment, root);
        if let Some(failure) = check.await? {
            return Ok(Some(failure));
        }
    }
    if let Some(failure) = check_filters(&http, origin, &id, directory, &receiver).await? {
        return Ok(Some(failure));
    }
    // The node's checks hold for the whole lookup only if the terminal block is still
    // its own.
    if parse_hash(&rpc.block_hash(u64::from(directory.end_height)).await?)? != directory.end_hash {
        return Err("chain changed during the probe".into());
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
    indexer_report(&http, &args.health_url, &id).await
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

/// The outcome of [`verify`] on one node.
enum Verdict {
    /// Every check passed, with the Ironwood root after the publication's terminal
    /// block when witness files are checked.
    Verified(Option<Hash>),
    /// Valid evidence against the publication or the fixture. It is final.
    Failed(Failure),
    /// The node could not complete the checks, so the next one tries.
    Unavailable(ZakuraError),
}

/// Checks the publication against one node: the anchor ([`check_anchor`]), then the
/// fixture's `pinned` height and block hash, which must be the node's (a fixture off
/// the chain is invalid), and, with `witnesses`, the Ironwood root after the terminal
/// block, read by its hash. Reads by hash or of deep history need no recheck; the
/// probe rechecks the terminal block once its lookup ends.
async fn verify(
    rpc: &ZakuraClient,
    directory: &receiver_directory::snapshot::Manifest,
    pinned: (u32, Hash),
    witnesses: bool,
) -> Verdict {
    let checks = async {
        if let Some(failure) = check_anchor(rpc, directory).await? {
            return Ok(Err(failure));
        }
        if parse_hash(&rpc.block_hash(u64::from(pinned.0)).await?)? != pinned.1 {
            return Ok(Err(("oracle_invalid", json!({"fixture": "block hash"}))));
        }
        let root = if witnesses {
            Some(
                rpc.ironwood_root(directory.end_hash, directory.end_height)
                    .await?,
            )
        } else {
            None
        };
        Ok(Ok(root))
    };
    match checks.await {
        Ok(Ok(root)) => Verdict::Verified(root),
        Ok(Err(failure)) => Verdict::Failed(failure),
        Err(error) => Verdict::Unavailable(error),
    }
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
/// must be the manifest's, it must decode to exactly the declared sets, and its paid
/// set must hold `receiver`, the fixture's, or wallets would skip that lookup.
async fn check_filters(
    http: &reqwest::Client,
    origin: &str,
    id: &str,
    directory: &receiver_directory::snapshot::Manifest,
    receiver: &Receiver,
) -> Result<Option<Failure>> {
    let url = format!("{origin}/v1/receiver/filters/{id}");
    let bytes = get(http, &url, MAX_FILTERS_BYTES).await?;
    let filters = Filters::decode(&bytes).ok().filter(|filters| {
        Hash::from(Sha256::digest(&bytes)) == directory.filters_sha256
            && directory.check_filters(filters).is_ok()
    });
    let Some(filters) = filters else {
        return Ok(Some((
            "answer_mismatch",
            json!({"filters_bytes": bytes.len()}),
        )));
    };
    let paid = filters
        .get(PAID)
        .is_some_and(|paid| paid.matches(&directory.salt, &[*receiver])[0]);
    Ok((!paid).then(|| {
        (
            "answer_mismatch",
            json!({"paid_filter": "omits the fixture"}),
        )
    }))
}

/// The indexer's payout check from health, which must report serving the probed
/// publication `id`. Otherwise the report may describe another publication, such as
/// one rotated in since the lookup, or another service. A missing payout outranks an
/// uncheckable one.
async fn indexer_report(
    http: &reqwest::Client,
    health_url: &str,
    id: &str,
) -> Result<Option<Failure>> {
    let health: Value = serde_json::from_slice(&get(http, health_url, MAX_HEALTH_BYTES).await?)?;
    let serving = health["serving"].as_str();
    if serving != Some(id) {
        return Ok(Some((
            "report_unavailable",
            json!({"probed": id, "health_serving": serving}),
        )));
    }
    let count = |field: &str| health["indexer"][field].as_u64();
    match (count("payouts_missing"), count("payouts_uncheckable")) {
        (None, _) | (_, None) => Ok(Some(("report_unavailable", Value::Null))),
        (Some(0), Some(0)) => Ok(None),
        (Some(0), Some(uncheckable)) => Ok(Some((
            "payouts_uncheckable",
            json!({"payouts_uncheckable": uncheckable}),
        ))),
        (Some(missing), _) => Ok(Some((
            "answer_mismatch",
            json!({"payouts_missing": missing}),
        ))),
    }
}

/// Checks the publication with [`verify`] on the nodes that have reached it, one at a
/// time in [`ZakuraClient::ranked`] order. The first node to complete the checks
/// decides, and the first valid evidence against the publication is final rather than
/// retried on a friendlier node; only a node that could not complete them hands over
/// to the next. Returns the node that verified and its result, with the highest tip
/// any node reported, from which lag is measured whichever node verified. With no node
/// completing the checks it is `oracle_unavailable`, with each attempt's tip and error.
async fn oracle(
    nodes: &[ZakuraClient],
    directory: &receiver_directory::snapshot::Manifest,
    pinned: (u32, Hash),
    witnesses: bool,
) -> std::result::Result<(ZakuraClient, Option<Hash>, u64), Failure> {
    let height = u64::from(directory.end_height);
    let unavailable = |detail: Value| ("oracle_unavailable", detail);
    let ranked = ZakuraClient::ranked(nodes)
        .await
        .map_err(|error| unavailable(json!({"end_height": height, "error": error.to_string()})))?;
    let top = ranked[0].0;
    let mut attempts = Vec::new();
    for (tip, rpc) in ranked.iter().filter(|(tip, _)| *tip >= height) {
        match verify(rpc, directory, pinned, witnesses).await {
            Verdict::Verified(root) => return Ok((rpc.clone(), root, top)),
            Verdict::Failed(failure) => return Err(failure),
            Verdict::Unavailable(error) => {
                attempts.push(json!({"node_tip": tip, "error": error.to_string()}))
            }
        }
    }
    Err(unavailable(
        json!({"end_height": height, "node_tip": top, "attempts": attempts}),
    ))
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

/// GETs `url`, reading at most `limit` bytes of a successful response's body.
async fn get(http: &reqwest::Client, url: &str, limit: usize) -> Result<Vec<u8>> {
    let response = http.get(url).send().await?.error_for_status()?;
    Ok(read_limited(response, limit).await?)
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
        ZakuraClient::new(url, None).unwrap()
    }

    /// [`super::common::manifest`] ending at Ironwood activation, where a node reports
    /// tree sizes.
    fn anchored() -> receiver_directory::snapshot::Manifest {
        let mut directory = super::common::manifest(receiver_pir::MIN_ROWS);
        directory.end_height = crate::blocks::ironwood_activation();
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

    /// A node for [`oracle`] over [`anchored`]: its tip and the hash it gives at the
    /// anchor's height `end`, `[anchor; 32]`, with the tree size after it. Genesis is
    /// `[1; 32]`, as [`super::common::manifest`] declares, and the block below the
    /// anchor, the fixture's, is `[fixture; 32]`. With `fails` set to `"getblockhash"`
    /// it refuses every `getblockhash` but genesis. With `moved`, the anchor's
    /// height holds `[9; 32]` after the boundary's first read, as when the chain changes
    /// during the checks. Its `z_gettreestate` gives `root`, or no root for `None`, and
    /// names another block when `fails` is `"treestate block"`.
    #[derive(Clone, Copy)]
    struct Node {
        tip: u64,
        end: u64,
        anchor: u8,
        fixture: u8,
        size: Option<u64>,
        fails: &'static str,
        moved: bool,
        root: Option<Hash>,
    }

    /// A node at tip `tip` that agrees with [`anchored`].
    fn good(tip: u64) -> Node {
        Node {
            tip,
            end: u64::from(anchored().end_height),
            anchor: 3,
            fixture: 5,
            size: Some(300),
            fails: "",
            moved: false,
            root: Some([7; 32]),
        }
    }

    /// Serves `node`. Returns its URL.
    async fn serve_node(node: Node) -> String {
        let end = node.end;
        let reads = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let app = Router::new().route(
            "/",
            routing::post(move |Json(request): Json<Value>| {
                let reads = reads.clone();
                async move {
                    let display = |byte| zakura_chain::block::Hash([byte; 32]).to_string();
                    let (method, params) =
                        (request["method"].as_str().unwrap(), &request["params"]);
                    if node.fails == method && params[0] != 0 {
                        let error = json!({"code": -1, "message": "refused"});
                        return Json(json!({"result": null, "error": error}));
                    }
                    let result = match (method, params[0].as_u64()) {
                        ("getblockcount", _) => json!(node.tip),
                        ("getblockhash", Some(0)) => json!(display(1)),
                        ("getblockhash", Some(height)) if height == end => {
                            let read = reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            json!(display(if node.moved && read >= 1 {
                                9
                            } else {
                                node.anchor
                            }))
                        }
                        ("getblockhash", Some(height)) if height == end - 1 => {
                            json!(display(node.fixture))
                        }
                        ("getblock", _) => match node.size {
                            Some(size) => json!({"trees": {"ironwood": {"size": size}}}),
                            None => json!({"trees": {}}),
                        },
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

    /// What [`oracle`] decides over `nodes`, listed in this order, for a fixture pinned
    /// to `[5; 32]` below the anchor, reading roots with `witnesses`: the highest tip
    /// and the root once a node verifies, or the failure.
    async fn decide(
        nodes: &[Node],
        witnesses: bool,
    ) -> std::result::Result<(u64, Option<Hash>), Failure> {
        let mut clients = Vec::new();
        for node in nodes {
            clients.push(ZakuraClient::new(serve_node(*node).await, None).unwrap());
        }
        let directory = anchored();
        let pinned = (directory.end_height - 1, [5; 32]);
        let (_, root, top) = oracle(&clients, &directory, pinned, witnesses).await?;
        Ok((top, root))
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
        assert_eq!(detail["attempts"].as_array().unwrap().len(), 3);
        assert_eq!(detail["node_tip"], end + 50);
        let (category, detail) = decide(&[behind], false).await.unwrap_err();
        assert_eq!(category, "oracle_unavailable");
        assert!(detail["attempts"].as_array().unwrap().is_empty());
    }

    /// Valid evidence against the publication, or against the fixture's pinned block,
    /// from the first node to complete the checks is final, even when a later node
    /// would agree with it, and equal tips keep their configured order.
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
        // A node whose block at the fixture's height is not the pinned one.
        let unpinned = Node {
            fixture: 6,
            ..good(end + 2)
        };
        for (first, expected) in [
            (off_chain, "answer_mismatch"),
            (short, "answer_mismatch"),
            (unpinned, "oracle_invalid"),
        ] {
            let (category, _) = decide(&[good(end + 1), first], false).await.unwrap_err();
            assert_eq!(category, expected);
            let (category, _) = decide(&[first, good(end + 2)], false).await.unwrap_err();
            assert_eq!(category, expected);
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

    /// Wallets require history from Ironwood activation, so a publication starting
    /// after it is an answer mismatch before any lookup, though it holds the fixture and
    /// agrees with the node; one starting at activation reaches the lookup.
    #[tokio::test]
    async fn a_publication_must_start_at_ironwood_activation() {
        let activation = crate::blocks::ironwood_activation();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fixture.json");
        let mut fixture: Value = serde_json::from_slice(MAINNET_FIXTURE).unwrap();
        fixture["height"] = (activation + 1).into();
        fixture["block_hash"] = zakura_chain::block::Hash([5; 32]).to_string().into();
        std::fs::write(&path, fixture.to_string()).unwrap();
        let node = Node {
            end: u64::from(activation) + 2,
            ..good(u64::from(activation) + 2)
        };
        let rpc = serve_node(node).await;
        for (start, reaches_lookup) in [(activation + 1, false), (activation, true)] {
            let mut directory = anchored();
            (directory.start_height, directory.end_height) = (start, activation + 2);
            let snapshot =
                receiver_directory::snapshot::Snapshot::build(directory, &[], &[]).unwrap();
            let server = receiver_pir::server::Server::new(snapshot).unwrap();
            let publications = receiver_pir_server::Publications::default();
            let publication = receiver_pir_server::Publication::new(server, None).unwrap();
            assert!(publications.publish(publication, 0));
            let app = receiver_pir_server::router_with_publications(publications);
            let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let origin = format!("http://{}", socket.local_addr().unwrap());
            tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
            let args = Args::parse_from([
                "probe",
                "--origin",
                &origin,
                "--health-url",
                &format!("{origin}/v1/receiver/health"),
                "--fixture",
                path.to_str().unwrap(),
                "--rpc-url",
                &rpc,
                "--no-auth",
            ]);
            let mut lookup = None;
            let (category, detail) = probe(args, &mut lookup).await.unwrap().unwrap();
            // The empty publication lacks the fixture's payment once looked up.
            assert_eq!(category, "answer_mismatch", "{start}");
            assert_eq!(lookup.is_some(), reaches_lookup, "{start}");
            assert_eq!(detail.get("coverage").is_none(), reaches_lookup, "{start}");
        }
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

    /// The hex ID of `m`, as health reports it.
    fn id(m: &Manifest) -> String {
        hex::encode(m.id().unwrap())
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

    /// A service whose health answers `health`, and a route with a body over the
    /// health bound. Returns its origin.
    async fn serve(health: Value) -> String {
        let app = Router::new()
            .route("/health", routing::get(move || async move { Json(health) }))
            .route(
                "/large",
                routing::get(|| async { vec![b' '; MAX_HEALTH_BYTES + 1] }),
            );
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", socket.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
        origin
    }

    /// A filter file is accepted only with the manifest's digest and with the fixture's
    /// receiver in its paid set.
    #[tokio::test]
    async fn the_filter_file_must_match_its_manifest() {
        let build = |records: &[receiver_directory::Record]| {
            let manifest = super::common::manifest(receiver_pir::MIN_ROWS);
            receiver_directory::snapshot::Snapshot::build(manifest, records, &[]).unwrap()
        };
        let snapshot = build(&[super::common::record(0, 1)]);
        let receiver = super::common::receiver();
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
        let check = check_filters(&http, &origin, "x", &snapshot.manifest, &receiver);
        assert!(check.await.unwrap().is_none());
        let mut altered = snapshot.filters.clone();
        *altered.last_mut().unwrap() ^= 1;
        let origin = serve(altered).await;
        let check = check_filters(&http, &origin, "x", &snapshot.manifest, &receiver);
        assert_eq!(check.await.unwrap().unwrap().0, "answer_mismatch");
        // A self-consistent publication whose paid set omits the fixture's receiver.
        let empty = build(&[]);
        let origin = serve(empty.filters.clone()).await;
        let check = check_filters(&http, &origin, "x", &empty.manifest, &receiver);
        assert_eq!(
            check.await.unwrap().unwrap().1["paid_filter"],
            "omits the fixture"
        );
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
        let argv = |extra: &[&str]| {
            let required = [
                "receiver-probe",
                "--origin",
                &origin,
                "--health-url",
                &health,
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

    /// Health's report counts only for the probed publication, even after a rotation,
    /// and bodies are bounded.
    #[tokio::test]
    async fn the_report_is_bound_to_the_probed_publication() {
        let (probed, rotated) = (id(&manifest(1)), id(&manifest(2)));
        let http = reqwest::Client::new();
        let report = |health: Value| {
            let (http, probed) = (http.clone(), probed.clone());
            async move {
                let origin = serve(health).await;
                indexer_report(&http, &format!("{origin}/health"), &probed)
                    .await
                    .unwrap()
            }
        };
        let indexer = |missing: u64| json!({"payouts_missing": missing, "payouts_uncheckable": 0});
        let current = report(json!({"serving": probed, "indexer": indexer(0)}));
        assert!(current.await.is_none());
        let origin = serve(Value::Null).await;
        assert!(get(&http, &format!("{origin}/large"), MAX_HEALTH_BYTES)
            .await
            .is_err());
        // The service rotated after the lookup, or health answers for another one.
        let failure = report(json!({"serving": rotated, "indexer": indexer(0)}))
            .await
            .unwrap();
        assert_eq!(
            failure,
            (
                "report_unavailable",
                json!({"probed": probed, "health_serving": rotated})
            )
        );
        // The probed publication's report is missing, or counts a missing payout.
        let failure = report(json!({"serving": probed})).await.unwrap();
        assert_eq!(failure, ("report_unavailable", Value::Null));
        let failure = report(json!({"serving": probed, "indexer": indexer(2)}))
            .await
            .unwrap();
        assert_eq!(failure, ("answer_mismatch", json!({"payouts_missing": 2})));
        // A recent payout the report could not check is an availability failure.
        let uncheckable = json!({"payouts_missing": 0, "payouts_uncheckable": 1});
        let failure = report(json!({"serving": probed, "indexer": uncheckable}))
            .await
            .unwrap();
        assert_eq!(
            failure,
            ("payouts_uncheckable", json!({"payouts_uncheckable": 1}))
        );
    }

    /// The fixture's pinned receiver is checked against recovery before any request:
    /// a missing, malformed or invalid pin, or another valid receiver, is
    /// `oracle_invalid` and reaches no server, while the embedded fixture's pin passes
    /// and the probe goes on.
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
        // Runs the probe on `fixture`, or on the embedded one for `None`.
        let run = |fixture: Option<Value>| {
            let mut args = vec![
                "receiver-probe".to_owned(),
                "--origin".into(),
                url.clone(),
                "--health-url".into(),
                format!("{url}/health"),
                "--rpc-url".into(),
                url.clone(),
                "--no-auth".into(),
            ];
            if let Some(fixture) = fixture {
                std::fs::write(&path, serde_json::to_vec(&fixture).unwrap()).unwrap();
                args.extend(["--fixture".into(), path.to_str().unwrap().into()]);
            }
            let args = Args::try_parse_from(args).unwrap();
            async move { probe(args, &mut None).await }
        };
        let fixture: Value = serde_json::from_slice(MAINNET_FIXTURE).unwrap();
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
            let failure = run(Some(altered)).await.unwrap();
            assert_eq!(
                failure,
                Some(("oracle_invalid", json!({"fixture": detail})))
            );
            assert_eq!(requests.load(Ordering::SeqCst), 0, "{detail}");
        }
        // The independent pin passes, so the probe asks the origin for its manifest.
        assert!(run(None).await.is_err());
        assert!(requests.load(Ordering::SeqCst) > 0);
    }
}
