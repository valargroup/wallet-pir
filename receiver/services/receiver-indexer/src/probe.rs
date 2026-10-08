//! A `pir-monitor` service probe for the receiver directory. It checks the served
//! publication against independent nodes, looks up a pinned historical payment over live
//! encrypted PIR, as Transparent's canary checks one query against a pinned row, then
//! checks the NEAR feed's freshness and the indexer's payout check, which health reports
//! on the private network. It prints one JSON line: `passed`, on failure a `category` and
//! `detail`, and the lookup as `phase: "live_encrypted_probe"` with `queries` and
//! `correct`. `answer_mismatch` marks served data that is wrong, which the monitor treats
//! as a correctness incident; `oracle_invalid` a fixture that fails its pin; anything
//! else is an availability failure.
use clap::Parser;
use receiver_directory::{extract::Action, Hash, Payment};
use receiver_indexer::zakura::ZakuraClient;
use receiver_pir::{transport::MAX_PIR_PAGES, AcceptedCoverage, Client, Manifest};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::path::PathBuf;

/// Blocks a publication may trail the node's tip: the indexer's depth, its poll and a
/// rotation's grace, with margin.
const MAX_LAG_BLOCKS: u64 = 12;
/// The recent set's largest age that wallets still trust (`zakura-pir-receiver`).
const MAX_RECENT_AGE_SECS: i64 = 15 * 60;

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
    /// A node's RPC endpoint. Repeat it for fallbacks, tried in order.
    #[arg(long, required = true)]
    rpc_url: Vec<String>,
    #[arg(long, required_unless_present = "no_auth", conflicts_with = "no_auth")]
    cookie: Option<PathBuf>,
    #[arg(long)]
    no_auth: bool,
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
    let rpc = match &args.cookie {
        Some(path) => ZakuraClient::from_cookie_file(args.rpc_url.clone(), path)?,
        None => ZakuraClient::unauthenticated(args.rpc_url.clone())?,
    };
    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(20))
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let origin = args.origin.trim_end_matches('/');
    let get = |url: String| {
        let request = http.get(url);
        async move {
            let response = request.send().await?.error_for_status()?;
            Ok::<_, reqwest::Error>(response.bytes().await?.to_vec())
        }
    };
    let manifest: Manifest =
        serde_json::from_slice(&get(format!("{origin}/v1/receiver/init")).await?)?;
    manifest.validate()?;
    let directory = &manifest.directory;
    let height = u64::from(directory.end_height);
    let tip = rpc.tip_height().await?;
    if height <= tip {
        let node: zakura_chain::block::Hash = rpc.block_hash(height).await?.parse()?;
        let genesis: zakura_chain::block::Hash = rpc.block_hash(0).await?.parse()?;
        if node.0 != directory.end_hash || genesis.0 != directory.genesis {
            return Ok(Some((
                "answer_mismatch",
                json!({"anchor_off_chain": height}),
            )));
        }
    }
    if tip.saturating_sub(height) > MAX_LAG_BLOCKS {
        return Ok(Some((
            "stale_publication",
            json!({"end_height": height, "node_tip": tip}),
        )));
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
    let public = get(format!("{origin}/v1/receiver/public/{id}")).await?;
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
            .await?
            .error_for_status()?
            .bytes()
            .await?;
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
            node.0 == payment.block_hash
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
    let health: Value = serde_json::from_slice(&get(args.health_url).await?)?;
    match health["indexer"]["payouts_missing"].as_u64() {
        None => Ok(Some(("report_unavailable", Value::Null))),
        Some(0) => Ok(None),
        Some(missing) => Ok(Some((
            "answer_mismatch",
            json!({"payouts_missing": missing}),
        ))),
    }
}
