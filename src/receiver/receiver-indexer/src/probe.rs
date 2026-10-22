//! Bounded real PIR, witness and rolling-filter checks against an independent node.
use crate::read_limited;
use clap::Parser;
use receiver_directory::{
    blocks::{Batch, Head, MAX_BATCH_BYTES},
    Hash, Payment, ReceiverBytes,
};
use receiver_pir::{
    public_bytes, response_bytes, transport::MAX_WITNESS_BYTES, AcceptedCoverage, Client, Manifest,
    MAX_MANIFEST_BYTES,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::path::PathBuf;
use zakura_receiver_crypto::{recovery::Action, witness::WitnessSnapshot, Receiver};
use zakura_server_chain::{hash, Network, NodeClient, NodeError};

/// The production fixture: a mainnet refund to a zero-OVK receiver (see `Fixture`).
const MAINNET_FIXTURE: &[u8] = include_bytes!("../fixtures/mainnet-probe.json");

/// Bound on the health report, a few hundred bytes.
const MAX_HEALTH_BYTES: usize = 64 * 1024;
/// Pages the probe reads looking for the fixture's payment before it gives up.
const MAX_PROBE_PAGES: u32 = 16;

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
    /// An independent node's RPC endpoint. Repeat it for fallbacks: the nodes that
    /// reached the publication check it one at a time, ranked as
    /// [`NodeClient::ranked`] does, until one completes the checks.
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
    /// The service serves witness files, as the indexer does with `--witnesses`: each
    /// run then checks the session's file against the nodes' Ironwood root. The probe
    /// cannot infer this from the service, which would answer a lost file as one never
    /// configured, so the deployment states it.
    #[arg(long)]
    witnesses: bool,
    /// Check a staged PIR-only service that intentionally publishes no rolling hints.
    /// Indexing deployments must retain the default filter checks.
    #[arg(long)]
    without_block_filters: bool,
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

/// Runs the probe with the embedded mainnet fixture, prints its JSON line and returns
/// whether every check passed.
pub async fn run(args: Args) -> bool {
    let mut lookup = None;
    let (passed, category, detail) = match probe(args, MAINNET_FIXTURE, &mut lookup).await {
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

/// The failed check for the JSON `fixture`, or `None` when every check passes. `lookup`
/// records the encrypted queries made and whether they found the fixture's payment.
async fn probe(
    args: Args,
    fixture: &[u8],
    lookup: &mut Option<(u32, bool)>,
) -> Result<Option<Failure>> {
    let Ok(fixture) = serde_json::from_slice::<Fixture>(fixture) else {
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
    let txid: Option<Hash> = hash::parse_display(&fixture.txid).ok();
    let block_hash = hash::parse_display(&fixture.block_hash).ok();
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
    let receiver = *receiver.as_bytes();
    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(20))
        .redirect(reqwest::redirect::Policy::none())
        .build()?;

    let origin = args.origin.trim_end_matches('/');
    let init = http.get(format!("{origin}/v1/receiver/init"));
    let manifest = fetch(init, MAX_MANIFEST_BYTES, "manifest", |bytes| {
        let manifest: Manifest = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
        manifest.validate().map_err(|e| e.to_string())?;
        Ok::<_, String>(manifest)
    });
    let manifest = match manifest.await? {
        Ok(manifest) => manifest,
        Err(failure) => return Ok(Some(failure)),
    };
    let directory = &manifest.directory;
    // Another network's publication is wrong whatever the nodes or the fixture say.
    let genesis = Network::Mainnet.genesis_hash();
    if directory.genesis != genesis {
        let served = hash::display(&directory.genesis);
        return Ok(Some((
            "answer_mismatch",
            json!({"expected_genesis": hash::display(&genesis), "served_genesis": served}),
        )));
    }
    // A start after the fixture is truncated history, which the activation check below
    // classifies. An end before it leaves the fixture uncheckable: oracle_invalid by design.
    if directory.end_height < fixture.height {
        return Ok(Some((
            "oracle_invalid",
            json!({"fixture": "after coverage"}),
        )));
    }
    let nodes = args
        .rpc_url
        .iter()
        .map(|url| NodeClient::new(url.clone(), args.cookie.as_deref()))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let pinned = (fixture.height, block_hash);
    // A session displaced during a slow ranking fails this run; the next starts from /init.
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
    // Wallets require history from Ironwood activation, independently of what the
    // manifest claims.
    let accepted = AcceptedCoverage {
        genesis,
        required_start: crate::blocks::ironwood_activation(),
        height: directory.end_height,
        hash: directory.end_hash,
    };
    // Served history that starts after activation is wrong for every wallet.
    if let Err(receiver_pir::Error::Directory(receiver_directory::Error::Coverage)) =
        accepted.check(directory)
    {
        return Ok(Some((
            "answer_mismatch",
            json!({"coverage": "starts after Ironwood activation"}),
        )));
    }
    let public = http.get(format!("{origin}/v1/receiver/public/{id}"));
    let client = fetch(public, public_bytes(rows)?, "public", |bytes| {
        Client::new(manifest.clone(), bytes, accepted)
    });
    let client = match client.await? {
        Ok(client) => client,
        Err(failure) => return Ok(Some(failure)),
    };
    let mut queries = 0;
    let found = loop {
        let query = client.prepare(receiver, queries)?;
        let request = http
            .post(format!("{origin}/v1/receiver/query"))
            .body(query.body().to_vec());
        let answer = fetch(request, response_bytes(rows)?, "lookup", |bytes| {
            client.decode(query, bytes)
        });
        let answer = answer.await?;
        queries += 1;
        let record = match answer {
            Ok(Some(record)) => record,
            Ok(None) => break None,
            Err(failure) => {
                *lookup = Some((queries, false));
                return Ok(Some(failure));
            }
        };
        if record.payment.position >= fixture.position || queries == record.total {
            break Some(record.payment);
        }
        if queries == MAX_PROBE_PAGES {
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
    if let Some(failure) = check_filters(
        &http,
        origin,
        &rpc,
        directory,
        &receiver,
        found.as_ref(),
        args.without_block_filters,
    )
    .await?
    {
        return Ok(Some(failure));
    }
    // The node's checks hold for the whole lookup only if the terminal block is still
    // its own.
    if rpc.block_hash(u64::from(directory.end_height)).await? != directory.end_hash {
        return Err("chain changed during the probe".into());
    }
    indexer_report(&http, &args.health_url, &id).await
}

/// Checks the publication's anchor against `rpc`: its hash and the tree size after it
/// (read by that hash, see [`NodeClient::receiver_boundary`]), which is the coverage
/// the manifest claims. A read that fails, a chain that changes during it, or a node
/// not on mainnet ([`NodeError::OtherNetwork`]) is an error, which proves nothing
/// against the publication.
async fn check_anchor(
    rpc: &NodeClient,
    directory: &receiver_directory::snapshot::Manifest,
) -> std::result::Result<Option<Failure>, NodeError> {
    rpc.check_network(Network::Mainnet.genesis_hash()).await?;
    let height = directory.end_height;
    let boundary = crate::blocks::boundary(rpc, height).await?;
    if boundary.hash != directory.end_hash {
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
    /// The node could not complete the checks, or is on another network, so the next
    /// one tries.
    Unavailable(NodeError),
}

/// Checks the publication against one node: the anchor ([`check_anchor`]), then the
/// fixture's `pinned` height and block hash, which must be the node's (a fixture off
/// the chain is invalid), and, with `witnesses`, the Ironwood root after the terminal
/// block, read by its hash. Reads by hash or of deep history need no recheck; the
/// probe rechecks the terminal block once its lookup ends.
async fn verify(
    rpc: &NodeClient,
    directory: &receiver_directory::snapshot::Manifest,
    pinned: (u32, Hash),
    witnesses: bool,
) -> Verdict {
    let checks = async {
        if let Some(failure) = check_anchor(rpc, directory).await? {
            return Ok(Err(failure));
        }
        if rpc.block_hash(u64::from(pinned.0)).await? != pinned.1 {
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
    let request = http.get(format!("{origin}/v1/receiver/witness/{id}"));
    let proof = fetch(request, MAX_WITNESS_BYTES, "witnesses", |bytes| {
        let publication = crate::verify::publication(directory).map_err(|e| e.to_string())?;
        WitnessSnapshot::decode(bytes, &publication).map_err(|e| e.to_string())
    });
    let proof = match proof.await? {
        Ok(proof) => proof,
        Err(failure) => return Ok(Some(failure)),
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

/// Checks current rolling hints independently of PIR readiness. A pruned fixture is
/// covered by the PIR check; a retained fixture must also appear in its block filter.
async fn check_filters(
    http: &reqwest::Client,
    origin: &str,
    rpc: &NodeClient,
    directory: &receiver_directory::snapshot::Manifest,
    receiver: &ReceiverBytes,
    payment: Option<&Payment>,
    without_block_filters: bool,
) -> Result<Option<Failure>> {
    if without_block_filters {
        return Ok(None);
    }
    let head = fetch(
        http.get(format!("{origin}/v1/receiver/block-filters/head")),
        16384,
        "block_head",
        |bytes| {
            let head: Head = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
            head.validate(&directory.genesis)
                .map_err(|e| e.to_string())?;
            Ok::<_, String>(head)
        },
    )
    .await?;
    let head = match head {
        Ok(head) => head,
        Err(failure) => return Ok(Some(failure)),
    };
    let retained = payment.filter(|p| p.height >= head.first && p.height <= head.tip.height);
    let from = retained.map_or(head.tip.height, |p| p.height);
    let batch = fetch(
        http.get(format!(
            "{origin}/v1/receiver/block-filters/{}/{from}/1",
            head.revision
        )),
        MAX_BATCH_BYTES,
        "block_batch",
        |bytes| {
            let batch = Batch::decode(bytes).map_err(|e| e.to_string())?;
            batch.check(&head, from, 1).map_err(|e| e.to_string())?;
            Ok::<_, String>(batch)
        },
    )
    .await?;
    let batch = match batch {
        Ok(batch) => batch,
        Err(failure) => return Ok(Some(failure)),
    };
    if batch.filters()[0].block().hash != rpc.block_hash(u64::from(from)).await?
        || (retained.is_some() && !batch.matches(0, &[*receiver])?[0])
    {
        return Ok(Some(("answer_mismatch", json!({"block_filter": from}))));
    }
    Ok(None)
}

/// The indexer's payout check from health, which must report serving the probed
/// publication `id`. Otherwise the report may describe another publication, such as
/// one rotated in since the lookup, or another service. Confirmed missing payouts
/// fail the probe. Unavailable audit data is diagnostic only.
async fn indexer_report(
    http: &reqwest::Client,
    health_url: &str,
    id: &str,
) -> Result<Option<Failure>> {
    // The indexer's monitoring channel, not wallet data: a bad body is an availability
    // failure, not an answer mismatch (see `fetch`).
    let response = http.get(health_url).send().await?.error_for_status()?;
    let health: Value = serde_json::from_slice(&read_limited(response, MAX_HEALTH_BYTES).await?)?;
    let serving = health["serving"].as_str();
    if serving != Some(id) {
        return Ok(Some((
            "report_unavailable",
            json!({"probed": id, "health_serving": serving}),
        )));
    }
    let count = |field: &str| health["indexer"][field].as_u64();
    match (count("payouts_missing"), count("payouts_uncheckable")) {
        (Some(missing), _) if missing > 0 => Ok(Some((
            "answer_mismatch",
            json!({"payouts_missing": missing}),
        ))),
        (Some(0), Some(0)) => Ok(None),
        counts => {
            tracing::warn!(?counts, "optional payout audit unavailable or incomplete");
            Ok(None)
        }
    }
}

/// Checks the publication with [`verify`] on the nodes that have reached it, one at a
/// time in [`NodeClient::ranked`] order. The first node to complete the checks
/// decides, and the first valid evidence against the publication is final rather than
/// retried on a friendlier node; only a node that could not complete them hands over
/// to the next. Nodes not on mainnet are left out of the ranking. Returns the node
/// that verified and its result, with the highest tip any ranked node reported, from
/// which lag is measured whichever node verified. With no node completing the checks
/// it is `oracle_unavailable`, with each attempt's tip and error.
async fn oracle(
    nodes: &[NodeClient],
    directory: &receiver_directory::snapshot::Manifest,
    pinned: (u32, Hash),
    witnesses: bool,
) -> std::result::Result<(NodeClient, Option<Hash>, u64), Failure> {
    let height = u64::from(directory.end_height);
    let unavailable = |detail: Value| ("oracle_unavailable", detail);
    let ranked = NodeClient::ranked(nodes, Network::Mainnet.genesis_hash())
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

/// Sends `request` for a wallet-facing artifact and decodes its body. A failed request
/// or any status other than 2xx (redirects are not followed) is an error; a successful
/// body over `limit` bytes, or one `decode` refuses, is wrong served data:
/// `answer_mismatch` naming `artifact`.
async fn fetch<T, E: std::fmt::Display>(
    request: reqwest::RequestBuilder,
    limit: usize,
    artifact: &str,
    decode: impl FnOnce(&[u8]) -> std::result::Result<T, E>,
) -> Result<std::result::Result<T, Failure>> {
    let response = request.send().await?;
    if !response.status().is_success() {
        return Err(format!("{artifact}: status {}", response.status()).into());
    }
    let error = match read_limited(response, limit).await {
        Ok(body) => match decode(&body) {
            Ok(value) => return Ok(Ok(value)),
            Err(error) => error.to_string(),
        },
        Err(error @ crate::BodyError::Oversized(_)) => error.to_string(),
        Err(error) => return Err(error.into()),
    };
    let mut detail = serde_json::Map::new();
    detail.insert(artifact.into(), error.into());
    Ok(Err(("answer_mismatch", detail.into())))
}

#[cfg(test)]
#[path = "../../receiver-store/tests/common/mod.rs"]
pub(crate) mod common;

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{routing, Json, Router};

    /// A node at tip `tip` on a chain whose block at every positive height is
    /// `[hash; 32]`, reporting Ironwood tree size `size`, and mainnet's genesis. With
    /// `reorg`, every second hash it gives is `[9; 32]`, as when the chain changes
    /// between two reads. Returns a client.
    async fn chain(tip: u64, hash: u8, size: Option<u64>, reorg: bool) -> NodeClient {
        let asked = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let app = Router::new().route(
            "/",
            routing::post(move |Json(request): Json<Value>| async move {
                let display = |byte| hash::display(&[byte; 32]);
                let result = match request["method"].as_str().unwrap() {
                    "getblockcount" => json!(tip),
                    "getblockhash" if request["params"][0] == 0 => {
                        json!(hash::display(&Network::Mainnet.genesis_hash()))
                    }
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
        NodeClient::new(url, None).unwrap()
    }

    /// [`super::common::manifest`] on mainnet, ending at Ironwood activation, where a
    /// node reports tree sizes.
    fn anchored() -> receiver_directory::snapshot::Manifest {
        let mut directory = super::common::manifest(receiver_pir::MIN_ROWS);
        directory.genesis = Network::Mainnet.genesis_hash();
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
        let check = |rpc: NodeClient| {
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
    /// `genesis`, and every other block below the anchor, the fixture's among them, is
    /// `[fixture; 32]`. With `fails` set to `"getblockhash"` it refuses every
    /// `getblockhash` but genesis. With `moved`, the anchor's height holds `[9; 32]`
    /// after the boundary's first read, as when the chain changes during the checks. Its
    /// `z_gettreestate` gives `root`, or no root for `None`, and names another block
    /// when `fails` is `"treestate block"`.
    #[derive(Clone, Copy)]
    struct Node {
        tip: u64,
        genesis: Hash,
        end: u64,
        anchor: u8,
        fixture: u8,
        size: Option<u64>,
        fails: &'static str,
        moved: bool,
        root: Option<Hash>,
    }

    /// A mainnet node at tip `tip` that agrees with [`anchored`].
    fn good(tip: u64) -> Node {
        Node {
            tip,
            genesis: Network::Mainnet.genesis_hash(),
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
                    let display = |byte| hash::display(&[byte; 32]);
                    let (method, params) =
                        (request["method"].as_str().unwrap(), &request["params"]);
                    if node.fails == method && params[0] != 0 {
                        let error = json!({"code": -1, "message": "refused"});
                        return Json(json!({"result": null, "error": error}));
                    }
                    let result = match (method, params[0].as_u64()) {
                        ("getblockcount", _) => json!(node.tip),
                        ("getblockhash", Some(0)) => {
                            json!(hash::display(&node.genesis))
                        }
                        ("getblockhash", Some(height)) if height == end => {
                            let read = reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            json!(display(if node.moved && read >= 1 {
                                9
                            } else {
                                node.anchor
                            }))
                        }
                        ("getblockhash", Some(height)) if height < end => {
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
            clients.push(NodeClient::new(serve_node(*node).await, None).unwrap());
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

    /// A node on another network, however high its tip, is left out without counting
    /// against the publication or setting the lag.
    #[tokio::test]
    async fn a_node_on_another_network_is_skipped() {
        let end = u64::from(anchored().end_height);
        let other = Node {
            genesis: [2; 32],
            anchor: 4,
            ..good(end + 100)
        };
        assert_eq!(decide(&[other, good(end)], false).await, Ok((end, None)));
        let (category, detail) = decide(&[other], false).await.unwrap_err();
        assert_eq!(category, "oracle_unavailable");
        assert_eq!(detail["error"], "node is on another network");
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

    /// Serves an empty publication of `directory`. Returns the probe's arguments for it,
    /// with the node at `rpc`.
    async fn publish(directory: receiver_directory::snapshot::Manifest, rpc: &str) -> Args {
        let snapshot = receiver_directory::snapshot::Snapshot::build(directory, &[]).unwrap();
        let publications = receiver_pir_server::Publications::default();
        let publication = crate::verify::prepare(snapshot, None).unwrap();
        assert!(publications.publish(publication, 0));
        let app = crate::router::router_with_publications(publications);
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", socket.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
        Args::parse_from([
            "probe",
            "--origin",
            &origin,
            "--health-url",
            &format!("{origin}/v1/receiver/health"),
            "--rpc-url",
            rpc,
            "--no-auth",
        ])
    }

    /// Staged service probing skips hints only with an explicit operator flag.
    #[tokio::test]
    async fn staged_pir_only_probe_does_not_hide_an_indexer_filter_outage() {
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };
        let reads = Arc::new(AtomicUsize::new(0));
        let count = reads.clone();
        let app = Router::new().route(
            "/v1/receiver/block-filters/head",
            routing::get(move || {
                count.fetch_add(1, Ordering::SeqCst);
                async { axum::http::StatusCode::SERVICE_UNAVAILABLE }
            }),
        );
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", socket.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
        let base = [
            "probe",
            "--origin",
            &origin,
            "--health-url",
            &origin,
            "--rpc-url",
            "http://127.0.0.1:1",
            "--no-auth",
        ];
        let required = Args::parse_from(base);
        let staged = Args::parse_from(base.into_iter().chain(["--without-block-filters"]));
        let node = NodeClient::new(required.rpc_url[0].clone(), None).unwrap();
        let http = reqwest::Client::new();
        let receiver = super::common::receiver();
        assert!(check_filters(
            &http,
            &origin,
            &node,
            &anchored(),
            &receiver,
            None,
            staged.without_block_filters
        )
        .await
        .unwrap()
        .is_none());
        assert_eq!(reads.load(Ordering::SeqCst), 0);
        let result = check_filters(
            &http,
            &origin,
            &node,
            &anchored(),
            &receiver,
            None,
            required.without_block_filters,
        )
        .await;
        assert!(result.is_err() || result.unwrap().is_some());
        assert_eq!(reads.load(Ordering::SeqCst), 1);
    }

    /// A publication on another network is an answer mismatch naming both genesis
    /// hashes, whether the nodes are on mainnet, on the publication's network (where
    /// the fixture's block differs), or unreachable.
    #[tokio::test]
    async fn a_publication_on_another_network_is_an_answer_mismatch() {
        let mut directory = anchored();
        directory.genesis = [2; 32];
        let end = u64::from(directory.end_height);
        let mut fixture: Value = serde_json::from_slice(MAINNET_FIXTURE).unwrap();
        fixture["height"] = (end - 1).into();
        fixture["block_hash"] = hash::display(&[5; 32]).into();
        let fixture = fixture.to_string();
        let other = Node {
            genesis: [2; 32],
            fixture: 6,
            ..good(end)
        };
        let closed = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let unreachable = format!("http://{}", closed.local_addr().unwrap());
        drop(closed);
        let expected = json!({
            "expected_genesis": hash::display(&Network::Mainnet.genesis_hash()),
            "served_genesis": hash::display(&[2; 32]),
        });
        for rpc in [
            serve_node(good(end)).await,
            serve_node(other).await,
            unreachable,
        ] {
            let args = publish(directory.clone(), &rpc).await;
            let failure = probe(args, fixture.as_bytes(), &mut None).await.unwrap();
            assert_eq!(
                failure,
                Some(("answer_mismatch", expected.clone())),
                "{rpc}"
            );
        }
    }

    /// Wallets require history from Ironwood activation, so a publication starting
    /// after it is an answer mismatch before any lookup, though it agrees with the node,
    /// whether or not it still holds the fixture; one starting at activation reaches the
    /// lookup.
    #[tokio::test]
    async fn a_publication_must_start_at_ironwood_activation() {
        let activation = crate::blocks::ironwood_activation();
        let mut fixture: Value = serde_json::from_slice(MAINNET_FIXTURE).unwrap();
        fixture["height"] = (activation + 1).into();
        fixture["block_hash"] = hash::display(&[5; 32]).into();
        let fixture = fixture.to_string();
        for (start, end, reaches_lookup) in [
            (activation + 1, activation + 2, false),
            (activation + 2, activation + 3, false),
            (activation, activation + 2, true),
        ] {
            let end_node = u64::from(end);
            let rpc = serve_node(Node {
                end: end_node,
                ..good(end_node)
            })
            .await;
            let mut directory = anchored();
            (directory.start_height, directory.end_height) = (start, end);
            let mut lookup = None;
            let args = publish(directory, &rpc).await;
            let (category, detail) = probe(args, fixture.as_bytes(), &mut lookup)
                .await
                .unwrap()
                .unwrap();
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
        use zakura_server_chain::testing::{ironwood_root_of, Tx};
        let raw = hex::decode(include_str!("../tests/fixtures/receiver-refund.hex").trim());
        let refund = Tx::from_bytes(&raw.unwrap());
        let cmx = refund.ironwood_commitments()[0];
        let node_root = ironwood_root_of(&[refund]);
        let mut manifest = super::common::manifest(receiver_pir::MIN_ROWS);
        (manifest.start_position, manifest.end_position) = (0, 1);
        let positions = Default::default();
        let file = WitnessSnapshot::build(
            &crate::verify::publication(&manifest).unwrap(),
            &[cmx],
            &positions,
        )
        .unwrap();
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
        let snapshot = receiver_directory::snapshot::Snapshot::build(manifest, &records).unwrap();
        let positions = [0, 5].into_iter().collect();
        let proof = WitnessSnapshot::build(
            &crate::verify::publication(&snapshot.manifest).unwrap(),
            served,
            &positions,
        )
        .unwrap();
        (
            snapshot.manifest,
            proof.encode(),
            records[1].payment.clone(),
        )
    }

    /// The witness file must bind to the probed publication, prove the fixture's
    /// commitment at its position and have the node's root.
    #[tokio::test]
    async fn the_witness_file_must_prove_the_fixture_under_the_nodes_root() {
        let tree: Vec<Hash> = (1..=8).map(|i| [i; 32]).collect();
        let (directory, proof, payment) = witnessed(&tree, &tree);
        let root =
            WitnessSnapshot::decode(&proof, &crate::verify::publication(&directory).unwrap())
                .unwrap()
                .root();
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
        let truncated = proof[..proof.len() - 1].to_vec();
        let detail = check(serve(200, truncated).await, payment.clone()).await;
        assert!(detail.unwrap().unwrap()["witnesses"].is_string());
        // Another revision's file, for the same tree under another salt.
        let mut other = directory.clone();
        other.salt[0] ^= 1;
        let stale = WitnessSnapshot::build(
            &crate::verify::publication(&other).unwrap(),
            &tree,
            &[0, 5].into_iter().collect(),
        );
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

    /// [`fetch`] against a real server: a successful body within its bound that decodes
    /// is the value; a successful body over its bound or one the decoder refuses is an
    /// `answer_mismatch` naming the artifact; a redirect or a failure status is an
    /// error, whatever its body.
    #[tokio::test]
    async fn fetch_classifies_responses() {
        use axum::http::StatusCode;
        const LIMIT: usize = 64;
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();
        let oversized = " ".repeat(LIMIT + 1);
        for (status, body, expected) in [
            (StatusCode::OK, "{}".to_owned(), "value"),
            (StatusCode::OK, oversized.clone(), "answer_mismatch"),
            (StatusCode::OK, "{".to_owned(), "answer_mismatch"),
            (StatusCode::FOUND, "<html>moved</html>".to_owned(), "error"),
            (StatusCode::FOUND, oversized.clone(), "error"),
            (StatusCode::INTERNAL_SERVER_ERROR, "{}".to_owned(), "error"),
        ] {
            let app = Router::new().route("/", routing::get(move || async move { (status, body) }));
            let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}/", socket.local_addr().unwrap());
            tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
            let fetched = fetch(http.get(url), LIMIT, "artifact", |bytes| {
                serde_json::from_slice::<Value>(bytes)
            });
            let outcome = match fetched.await {
                Ok(Ok(_)) => "value",
                Ok(Err((category, detail))) if detail["artifact"].is_string() => category,
                Ok(Err(failure)) => panic!("{status}: {failure:?}"),
                Err(_) => "error",
            };
            assert_eq!(outcome, expected, "{status}");
        }
    }

    /// A successful `init` response that is malformed JSON or a manifest failing
    /// validation (protocol or geometry) reaches the probe's result as an answer
    /// mismatch.
    #[tokio::test]
    async fn an_invalid_served_manifest_is_an_answer_mismatch() {
        let mut protocol = manifest(0);
        protocol.protocol = "other".into();
        let mut geometry = manifest(0);
        geometry.directory.rows = receiver_pir::MIN_ROWS + 1;
        let json = |m: &Manifest| serde_json::to_string(m).unwrap();
        for body in ["{".to_owned(), json(&protocol), json(&geometry)] {
            let app = Router::new().route(
                "/v1/receiver/init",
                routing::get(move || async move { body }),
            );
            let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let origin = format!("http://{}", socket.local_addr().unwrap());
            tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
            let args = Args::parse_from([
                "probe",
                "--origin",
                &origin,
                "--health-url",
                &format!("{origin}/health"),
                "--rpc-url",
                &origin,
                "--no-auth",
            ]);
            let (category, detail) = probe(args, MAINNET_FIXTURE, &mut None)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(category, "answer_mismatch");
            assert!(detail["manifest"].is_string());
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

    /// Health's report counts only for the probed publication, even after a rotation,
    /// and a body over its bound is an error, not an answer mismatch.
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
        let failure = indexer_report(&http, &format!("{origin}/large"), &probed).await;
        assert!(failure.is_err());
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
        // An unavailable optional audit cannot reject otherwise valid chain evidence.
        assert!(report(json!({"serving": probed})).await.is_none());
        assert!(report(json!({"serving": probed, "indexer": null}))
            .await
            .is_none());
        let failure = report(json!({"serving": probed, "indexer": indexer(2)}))
            .await
            .unwrap();
        assert_eq!(failure, ("answer_mismatch", json!({"payouts_missing": 2})));
        let uncheckable = json!({"payouts_missing": 0, "payouts_uncheckable": 1});
        assert!(report(json!({"serving": probed, "indexer": uncheckable}))
            .await
            .is_none());
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
        // Runs the probe on `fixture`, or on the embedded one for `None`.
        let run = |fixture: Option<Value>| {
            let args = vec![
                "receiver-probe".to_owned(),
                "--origin".into(),
                url.clone(),
                "--health-url".into(),
                format!("{url}/health"),
                "--rpc-url".into(),
                url.clone(),
                "--no-auth".into(),
            ];
            let fixture = fixture.map_or(MAINNET_FIXTURE.to_vec(), |f| f.to_string().into());
            let args = Args::try_parse_from(args).unwrap();
            async move { probe(args, &fixture, &mut None).await }
        };
        let fixture: Value = serde_json::from_slice(MAINNET_FIXTURE).unwrap();
        assert_eq!(fixture["receiver"], super::common::RECEIVER_HEX);
        let other = hex::encode(super::common::seeded_receiver(3));
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

    /// Against an indexer serving a synthetic chain that holds the public refund, a
    /// fixture pinned where the refund was indexed passes with one encrypted query and
    /// its witness, even when the first node is unreachable. A misplaced pin is an
    /// answer mismatch, and nodes behind the publication leave it unchecked.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_live_lookup_finds_the_indexed_refund() {
        use axum::extract::State;
        use std::sync::{
            atomic::{AtomicU64, Ordering},
            Arc,
        };
        use zakura_server_chain::testing::receiver::{ironwood_root, RefundBlock};
        #[derive(Clone)]
        struct Chain {
            blocks: Arc<[RefundBlock; 2]>,
            tip: Arc<AtomicU64>,
        }
        /// The refund's block at activation and a coinbase-only block after it.
        async fn handler(State(chain): State<Chain>, Json(r): Json<Value>) -> Json<Value> {
            let height = u64::from(crate::blocks::ironwood_activation());
            let [first, second] = &*chain.blocks;
            let requested = r["params"][0]
                .as_u64()
                .or_else(|| r["params"][0].as_str().and_then(|s| s.parse().ok()));
            let block = if requested == Some(height + 1) {
                second
            } else {
                first
            };
            let extended = r["params"][0] == second.display_hash();
            let result = match r["method"].as_str().unwrap() {
                "getblockcount" => json!(chain.tip.load(Ordering::SeqCst)),
                "getblockhash" if r["params"][0] == 0 => {
                    json!(hash::display(&Network::Mainnet.genesis_hash()))
                }
                "getblockhash" if r["params"][0] == height - 1 => json!("00".repeat(32)),
                "getblockhash" => json!(block.display_hash()),
                "getblock" if r["params"][1] == 1 => {
                    json!({"trees": {"ironwood": {"size": if extended { 3 } else { 2 }}}})
                }
                "getblockheader" => json!({"time": 1_800_000_000}),
                "getblock" => json!(block.raw_hex()),
                "z_gettreestate" => {
                    let blocks: &[&RefundBlock] =
                        if extended { &[first, second] } else { &[first] };
                    json!({"hash": r["params"][0], "height": height + u64::from(extended),
                        "ironwood": {"commitments": {"finalRoot": hex::encode(ironwood_root(blocks))}}})
                }
                method => panic!("unexpected {method}"),
            };
            Json(json!({"result": result, "error": null}))
        }
        let activation = crate::blocks::ironwood_activation();
        let first = RefundBlock::new(activation);
        let second = first
            .clone()
            .coinbase_only()
            .at_height(activation + 1)
            .after(first.hash());
        let tip = Arc::new(AtomicU64::new(u64::from(activation) + 1));
        let chain = Chain {
            blocks: Arc::new([first, second]),
            tip: tip.clone(),
        };
        let app = Router::new()
            .route("/", routing::post(handler))
            .with_state(chain);
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", socket.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });

        // A one-shot run writes the rows this test reads the refund's indexed place from,
        // then the serving run publishes the same chain from memory.
        let dir = tempfile::tempdir().unwrap();
        let config = crate::application::Config {
            data_dir: dir.path().to_owned(),
            min_rows: receiver_pir::MIN_ROWS,
            witnesses: true,
            max_witness_commitments: 1 << 24,
            start_height: activation,
            end_height: None,
            depth: 0,
            batch_size: crate::blocks::MAX_RECEIVER_BATCH_BLOCKS,
            concurrency: 8,
            audit_enabled: false,
            filter_blocks: None,
            history_revision: 0,
        };
        let nodes = [NodeClient::new(url.clone(), None).unwrap()];
        let genesis = Network::Mainnet.genesis_hash();
        let mut state = crate::application::State::default();
        crate::application::refresh(&config, &nodes, genesis, None, &mut state)
            .await
            .unwrap();
        let publications = receiver_pir_server::Publications::default();
        crate::application::refresh(&config, &nodes, genesis, Some(&publications), &mut state)
            .await
            .unwrap();
        let rows = std::fs::read_dir(dir.path().join("publications"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| path.extension().is_some_and(|e| e == "rows"))
            .unwrap();
        let payment = std::fs::read(rows)
            .unwrap()
            .chunks(receiver_directory::snapshot::ROW_BYTES)
            .flat_map(|row| {
                row[..receiver_directory::snapshot::SLOTS * receiver_directory::RECORD_BYTES]
                    .chunks(receiver_directory::RECORD_BYTES)
                    .map(|slot| zakura_receiver_crypto::Record::decode(slot).unwrap())
                    .collect::<Vec<_>>()
            })
            .flatten()
            .next()
            .unwrap()
            .payment;
        let app = crate::router::router_with_publications(publications);
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", socket.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });

        let mut fixture: Value = serde_json::from_str(include_str!(
            "../../receiver-store/tests/fixtures/zero-ovk-action.json"
        ))
        .unwrap();
        fixture["height"] = payment.height.into();
        fixture["block_hash"] = hash::display(&payment.block_hash).into();
        fixture["tx_index"] = payment.tx_index.into();
        fixture["position"] = payment.position.into();
        let health = format!("{origin}/v1/receiver/health");
        let run = |fixture: &Value, rpc: &[&str]| {
            let mut args = vec!["probe", "--origin", &origin, "--health-url", &health];
            args.extend(rpc.iter().flat_map(|node| ["--rpc-url", node]));
            args.extend(["--no-auth", "--witnesses"]);
            let args = Args::parse_from(args);
            let fixture = fixture.to_string();
            async move {
                let mut lookup = None;
                let failure = probe(args, fixture.as_bytes(), &mut lookup).await;
                (failure.unwrap(), lookup)
            }
        };
        assert_eq!(run(&fixture, &[&url]).await, (None, Some((1, true))));
        let fallback = run(&fixture, &["http://127.0.0.1:1", &url]).await;
        assert_eq!(fallback, (None, Some((1, true))));
        let mut moved = fixture.clone();
        moved["position"] = (payment.position + 1).into();
        let (failure, lookup) = run(&moved, &[&url]).await;
        assert_eq!(failure.unwrap().0, "answer_mismatch");
        assert_eq!(lookup, Some((1, false)));
        tip.store(u64::from(activation), Ordering::SeqCst);
        let (failure, lookup) = run(&fixture, &[&url]).await;
        assert_eq!(failure.unwrap().0, "oracle_unavailable");
        assert_eq!(lookup, None);
    }
}
