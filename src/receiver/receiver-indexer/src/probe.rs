//! Bounded real PIR, companion witness and rolling-filter checks against an independent
//! node.
use crate::canonical::{tree_mismatch, TreeAnchor, TreeMismatch};
use crate::read_limited;
use clap::Parser;
use serde::Deserialize;
use serde_json::{json, Value};
use std::path::PathBuf;
use zakura_pir_client::receiver::Client;
use zakura_pir_client::receiver::WitnessClient;
use zakura_pir_publisher::receiver::crypto::{recovery::Action, Receiver};
use zakura_pir_publisher::receiver::inventory::{
    Capture, Head, Inventory, Shard, MAX_CAPTURE_BYTES, MAX_CHUNK_BYTES,
};
use zakura_pir_publisher::receiver::native::public_bytes;
use zakura_pir_publisher::receiver::native::response_bytes;
use zakura_pir_publisher::receiver::session::Publication;
use zakura_pir_publisher::receiver::session::MAX_MANIFEST_BYTES;
use zakura_pir_publisher::receiver::{
    blocks::{Batch, MAX_BATCH_BYTES},
    shards::{ShardFilter, MAX_FILTER_BYTES},
    Hash, Payment, ReceiverBytes,
};
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
    health_url: Option<String>,
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
    /// a full retained set's wait: the default 12 suits the default depth of 2.
    #[arg(long, default_value_t = 12)]
    max_lag: u64,
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
    let genesis = Network::Mainnet.genesis_hash();
    let request = http.get(format!("{origin}/v1/receiver/inventory"));
    let head = fetch(request, MAX_MANIFEST_BYTES, "manifest", |bytes| {
        serde_json::from_slice::<Head>(bytes)
    });
    let head = match head.await? {
        Ok(head) => head,
        Err(failure) => return Ok(Some(failure)),
    };
    // Another network's publication is wrong whatever the nodes or the fixture say.
    if head.genesis != genesis {
        let served = hash::display(&head.genesis);
        return Ok(Some((
            "answer_mismatch",
            json!({"expected_genesis": hash::display(&genesis), "served_genesis": served}),
        )));
    }
    if let Err(error) = head.validate(&genesis) {
        return Ok(Some((
            "answer_mismatch",
            json!({"manifest": error.to_string()}),
        )));
    }
    // A start after the fixture is truncated history, which the activation check below
    // classifies. An end before it leaves the fixture uncheckable: oracle_invalid by design.
    if head.end_height < fixture.height {
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
    // An inventory displaced during a slow ranking fails this run; the next starts from
    // its head.
    let shard = u16::try_from(fixture.position >> 16)?;
    let (rpc, root, tip) = match oracle(&nodes, &head, pinned, shard).await {
        Ok(found) => found,
        Err(failure) => return Ok(Some(failure)),
    };
    if let Some(failure) = check_lag(tip, &head, args.max_lag) {
        return Ok(Some(failure));
    }
    let inventory = match capture(&http, origin, head.clone()).await? {
        Ok(inventory) => inventory,
        Err(failure) => return Ok(Some(failure)),
    };
    // Wallets require history from Ironwood activation, independently of what the
    // inventory claims; served history that starts later is wrong for every wallet.
    let activation = crate::blocks::ironwood_activation();
    if inventory
        .accept(&genesis, activation, head.end_height, &head.end_hash)
        .is_err()
    {
        return Ok(Some((
            "answer_mismatch",
            json!({"coverage": "starts after Ironwood activation"}),
        )));
    }
    let Some(descriptor) = inventory
        .shards()
        .iter()
        .find(|s| (s.start_height..=s.end_height).contains(&fixture.height))
    else {
        return Ok(Some((
            "answer_mismatch",
            json!({"coverage": "fixture height"}),
        )));
    };
    if let Some(failure) =
        check_shard_filter(&http, origin, &inventory, descriptor, &receiver).await?
    {
        return Ok(Some(failure));
    }
    // The pinned receiver's pages over live encrypted PIR, segment by segment and in
    // position order, until the fixture's payment or a later one.
    let mut queries = 0;
    let mut found = None;
    for segment in &descriptor.segments {
        let manifest = &segment.payments;
        let rows = manifest.directory.rows;
        let id = hex::encode(manifest.id()?);
        let public = http.get(format!("{origin}/v1/receiver/public/{id}"));
        let client = fetch(public, public_bytes(rows)?, "public", |bytes| {
            Client::new(manifest.clone(), bytes)
        });
        let client = match client.await? {
            Ok(client) => client,
            Err(failure) => return Ok(Some(failure)),
        };
        let mut page = 0;
        let last = loop {
            let query = client.prepare(receiver, page)?;
            let request = http
                .post(format!("{origin}/v1/receiver/query"))
                .body(query.body().to_vec());
            let answer = fetch(request, response_bytes(rows)?, "lookup", |bytes| {
                client.decode(query, bytes)
            });
            let answer = answer.await?;
            queries += 1;
            page += 1;
            let record = match answer {
                Ok(Some(record)) => record,
                Ok(None) => break None,
                Err(failure) => {
                    *lookup = Some((queries, false));
                    return Ok(Some(failure));
                }
            };
            if record.payment.position >= fixture.position || page == record.total {
                break Some(record);
            }
            if page == MAX_PROBE_PAGES {
                return Err("pinned receiver's history exceeds the probe's page bound".into());
            }
        };
        if let Some(record) = last.filter(|r| r.payment.position >= fixture.position) {
            found = Some((record, segment));
            break;
        }
    }
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
    let correct = found.as_ref().is_some_and(|(r, _)| expected(&r.payment));
    *lookup = Some((queries, correct));
    let (record, segment) = match found {
        Some(found) if correct => found,
        found => {
            let found = found.map(|(r, _)| (r.payment.height, r.payment.position));
            return Ok(Some((
                "answer_mismatch",
                json!({"lookup": found, "queries": queries}),
            )));
        }
    };
    // The segment's own end and its companion's construction block must be the node's,
    // with the tree sizes their manifests claim, before its payload is trusted.
    if let Some(failure) = check_anchor(&rpc, &segment_anchors(segment)).await? {
        return Ok(Some(failure));
    }
    let check = check_witness(&http, origin, segment, &record, root);
    if let Some(failure) = check.await? {
        return Ok(Some(failure));
    }
    if let Some(failure) = check_filters(
        &http,
        origin,
        &rpc,
        &head,
        &receiver,
        Some(&record.payment),
        args.without_block_filters,
    )
    .await?
    {
        return Ok(Some(failure));
    }
    let anchors = head_anchor(&head)
        .into_iter()
        .chain(segment_anchors(segment));
    still_anchored(&rpc, anchors.map(|(anchor, _, _)| anchor)).await?;
    match &args.health_url {
        Some(url) => indexer_report(&http, url, &hex::encode(head.inventory)).await,
        None => Ok(None),
    }
}

/// Downloads `head`'s inventory chunk by chunk through the library's bounded
/// [`Capture`], which checks each chunk before the next is requested and retains at most
/// [`MAX_CAPTURE_BYTES`]. A chunk the service cannot send is an error; one that does not
/// fit the head, or chunks that are not exactly its inventory, are wrong served data.
async fn capture(
    http: &reqwest::Client,
    origin: &str,
    head: Head,
) -> Result<std::result::Result<Inventory, Failure>> {
    let mismatch = |error: zakura_pir_publisher::receiver::session::Error| {
        ("answer_mismatch", json!({"inventory": error.to_string()}))
    };
    let id = hex::encode(head.inventory);
    let mut capture = match Capture::new(head, MAX_CAPTURE_BYTES) {
        Ok(capture) => capture,
        Err(error) => return Ok(Err(mismatch(error))),
    };
    while let Some(index) = capture.next_chunk() {
        let request = http.get(format!("{origin}/v1/receiver/inventory/{id}/{index}"));
        let pushed = fetch(request, MAX_CHUNK_BYTES, "inventory", |bytes| {
            capture.push(bytes)
        });
        if let Err(failure) = pushed.await? {
            return Ok(Err(failure));
        }
    }
    Ok(capture.finish().map_err(mismatch))
}

/// Checks the fixture shard's filter, when it is sealed with one: it must decode, hold
/// its digest, size and count, cover exactly the shard under the inventory's network
/// and correction, and match the fixture's receiver, whose indexed payment it holds.
async fn check_shard_filter(
    http: &reqwest::Client,
    origin: &str,
    inventory: &Inventory,
    shard: &Shard,
    receiver: &ReceiverBytes,
) -> Result<Option<Failure>> {
    let Some(reference) = &shard.filter else {
        return Ok(None);
    };
    let request = http.get(format!(
        "{origin}/v1/receiver/shard-filters/{}",
        hex::encode(reference.digest)
    ));
    let head = inventory.head();
    let filter = fetch(request, MAX_FILTER_BYTES, "shard_filter", |bytes| {
        let filter = ShardFilter::decode(bytes).map_err(|e| e.to_string())?;
        filter
            .check(&head.genesis, head.history_revision, &shard.coverage())
            .map_err(|e| e.to_string())?;
        if filter.digest() != reference.digest
            || filter.bytes().len() != reference.bytes as usize
            || filter.receivers() != u64::from(reference.receivers)
        {
            return Err("filter differs from its descriptor".to_owned());
        }
        Ok(filter)
    });
    let filter = match filter.await? {
        Ok(filter) => filter,
        Err(failure) => return Ok(Some(failure)),
    };
    if !filter.matches(&[*receiver])[0] {
        return Ok(Some((
            "answer_mismatch",
            json!({"shard_filter": shard.index}),
        )));
    }
    Ok(None)
}

/// The inventory's end, with the tree size it claims, and the keys a failure names it by.
fn head_anchor(head: &Head) -> [(TreeAnchor, &'static str, &'static str); 1] {
    let anchor = TreeAnchor {
        height: head.end_height,
        hash: head.end_hash,
        size: head.end_position,
    };
    [(anchor, "anchor_off_chain", "end_position")]
}

/// A segment's payment table end and companion construction block, with the tree sizes
/// their manifests claim, and the keys a failure names each by.
fn segment_anchors(segment: &Publication) -> [(TreeAnchor, &'static str, &'static str); 2] {
    [
        (
            TreeAnchor::payments(&segment.payments.directory),
            "segment_anchor_off_chain",
            "segment_end_position",
        ),
        (
            TreeAnchor::witnesses(&segment.witnesses.table),
            "witness_anchor_off_chain",
            "witness_tree_size",
        ),
    ]
}

/// Checks `anchors` against `rpc`, each a block and the tree size a served table claims
/// after it (see [`tree_mismatch`]), with the keys a failure names it by; the first off
/// the chain or at another size fails. A read that fails, a chain that changes during
/// it, or a node not on mainnet ([`NodeError::OtherNetwork`]) is an error, which proves
/// nothing against the publication.
async fn check_anchor(
    rpc: &NodeClient,
    anchors: &[(TreeAnchor, &str, &str)],
) -> std::result::Result<Option<Failure>, NodeError> {
    rpc.check_network(Network::Mainnet.genesis_hash()).await?;
    for &(anchor, off_chain, claimed) in anchors {
        match tree_mismatch(rpc, anchor).await? {
            None => {}
            Some(TreeMismatch::OffChain) => {
                return Ok(Some(("answer_mismatch", json!({off_chain: anchor.height}))))
            }
            Some(TreeMismatch::Size(node)) => {
                return Ok(Some((
                    "answer_mismatch",
                    json!({claimed: anchor.size, "node_tree_size": node}),
                )))
            }
        }
    }
    Ok(None)
}

/// Errs unless `rpc` still has every block in `anchors`, the blocks [`check_anchor`]
/// checked, by hash. The node's checks hold for the whole lookup only if none changed
/// during it.
async fn still_anchored(
    rpc: &NodeClient,
    anchors: impl IntoIterator<Item = TreeAnchor>,
) -> Result<()> {
    for anchor in anchors {
        if rpc.block_hash(u64::from(anchor.height)).await? != anchor.hash {
            return Err("chain changed during the probe".into());
        }
    }
    Ok(())
}

/// Whether the publication trails `tip`, the highest any node reported, by more than
/// `max_lag` blocks.
fn check_lag(tip: u64, directory: &Head, max_lag: u64) -> Option<Failure> {
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
    /// Every check passed, with the root of the fixture's Ironwood shard once the node
    /// has completed it.
    Verified(Option<Hash>),
    /// Valid evidence against the publication or the fixture. It is final.
    Failed(Failure),
    /// The node could not complete the checks, or is on another network, so the next
    /// one tries.
    Unavailable(NodeError),
}

/// Checks the publication against one node: the anchor ([`check_anchor`]), then the
/// fixture's `pinned` height and block hash, which must be the node's (a fixture off
/// the chain is invalid), and reads the root of the fixture's Ironwood `shard`, `None`
/// while the node has not completed it. Reads by hash or of deep history need no
/// recheck; the probe rechecks every anchor once its lookup ends ([`still_anchored`]).
async fn verify(rpc: &NodeClient, directory: &Head, pinned: (u32, Hash), shard: u16) -> Verdict {
    let checks = async {
        if let Some(failure) = check_anchor(rpc, &head_anchor(directory)).await? {
            return Ok(Err(failure));
        }
        if rpc.block_hash(u64::from(pinned.0)).await? != pinned.1 {
            return Ok(Err(("oracle_invalid", json!({"fixture": "block hash"}))));
        }
        let root = rpc.ironwood_subtree(shard).await?.map(|(root, _)| root);
        Ok(Ok(root))
    };
    match checks.await {
        Ok(Ok(root)) => Verdict::Verified(root),
        Ok(Err(failure)) => Verdict::Failed(failure),
        Err(error) => Verdict::Unavailable(error),
    }
}

/// Reads `record`'s companion witness payload over live encrypted PIR, as wallets do,
/// by the ordinal the record carries. It must supply exactly the siblings its position
/// has at the witness table's tree size, each a canonical node, and once final it must
/// fold from the record's commitment to `root`, the node's root for that Ironwood shard,
/// so a self-consistent path over another tree fails too. A final payload for a shard
/// the node has not completed is wrong. A provisional payload has no independent oracle
/// here, so the check is unavailable rather than passed. A session the service cannot
/// answer is an error.
async fn check_witness(
    http: &reqwest::Client,
    origin: &str,
    publication: &Publication,
    record: &zakura_pir_publisher::receiver::RawRecord,
    root: Option<Hash>,
) -> Result<Option<Failure>> {
    let witnesses = &publication.witnesses;
    let rows = witnesses.table.rows;
    let id = hex::encode(witnesses.id()?);
    let public = http.get(format!("{origin}/v1/receiver/public/{id}"));
    let client = fetch(public, public_bytes(rows)?, "witness_public", |bytes| {
        WitnessClient::new(witnesses.clone(), bytes)
    });
    let client = match client.await? {
        Ok(client) => client,
        Err(failure) => return Ok(Some(failure)),
    };
    let query = client.prepare(record.ordinal)?;
    let request = http
        .post(format!("{origin}/v1/receiver/query"))
        .body(query.body().to_vec());
    let payload = fetch(request, response_bytes(rows)?, "witness", |bytes| {
        client.decode(query, bytes)
    });
    let payload = match payload.await? {
        Ok(payload) => payload,
        Err(failure) => return Ok(Some(failure)),
    };
    let position = record.payment.position;
    let folded = crate::witness::shard_root(
        &payload,
        position,
        &record.payment.cmx,
        witnesses.table.tree_size,
    );
    match (folded, root) {
        // No independent oracle covers a filling shard's siblings, so this run cannot
        // vouch for the payload: unavailable, never passed.
        (Ok(None), _) => Ok(Some((
            "oracle_unavailable",
            json!({"witness": "provisional", "witness_path": position}),
        ))),
        (Ok(Some(folded)), Some(root)) if folded == root => Ok(None),
        (Ok(Some(folded)), root) => Ok(Some((
            "answer_mismatch",
            json!({"witness_root": hex::encode(folded), "node_root": root.map(hex::encode)}),
        ))),
        (Err(_), _) => Ok(Some(("answer_mismatch", json!({"witness_path": position})))),
    }
}

/// Checks current rolling hints independently of PIR readiness. A pruned fixture is
/// covered by the PIR check; a retained fixture must also appear in its block filter.
async fn check_filters(
    http: &reqwest::Client,
    origin: &str,
    rpc: &NodeClient,
    directory: &Head,
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
            let head: zakura_pir_publisher::receiver::blocks::Head =
                serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
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
    directory: &Head,
    pinned: (u32, Hash),
    shard: u16,
) -> std::result::Result<(NodeClient, Option<Hash>, u64), Failure> {
    let height = u64::from(directory.end_height);
    let unavailable = |detail: Value| ("oracle_unavailable", detail);
    let ranked = NodeClient::ranked(nodes, Network::Mainnet.genesis_hash())
        .await
        .map_err(|error| unavailable(json!({"end_height": height, "error": error.to_string()})))?;
    let top = ranked[0].0;
    let mut attempts = Vec::new();
    for (tip, rpc) in ranked.iter().filter(|(tip, _)| *tip >= height) {
        match verify(rpc, directory, pinned, shard).await {
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
    use zakura_pir_publisher::receiver::snapshot::SnapshotPublisherExt as _;

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
    fn anchored_directory() -> zakura_pir_publisher::receiver::snapshot::Manifest {
        let mut directory =
            super::common::manifest(zakura_pir_publisher::receiver::session::MIN_ROWS);
        directory.genesis = Network::Mainnet.genesis_hash();
        directory.end_height = crate::blocks::ironwood_activation();
        directory
    }

    /// An inventory head covering [`anchored_directory`].
    fn anchored() -> Head {
        let d = anchored_directory();
        Head {
            protocol: zakura_pir_publisher::receiver::inventory::PROTOCOL.into(),
            genesis: d.genesis,
            history_revision: 0,
            inventory: [0; 32],
            shards: 1,
            start_height: d.start_height,
            start_position: d.start_position,
            end_height: d.end_height,
            end_hash: d.end_hash,
            end_position: d.end_position,
        }
    }

    /// The companion witness manifest of `directory`, built at its end.
    fn companion(
        directory: &zakura_pir_publisher::receiver::snapshot::Manifest,
    ) -> zakura_pir_publisher::receiver::witness::Manifest {
        let snapshot =
            zakura_pir_publisher::receiver::snapshot::Snapshot::build(directory.clone(), &[])
                .unwrap();
        crate::witness::table(&snapshot, &[]).unwrap().manifest
    }

    /// The anchor must have the node's hash and the tree size after that block. A node
    /// that cannot give the size, or whose chain changes during the read, proves
    /// nothing against the publication.
    #[tokio::test]
    async fn the_anchor_must_match_the_nodes_hash_and_tree_size() {
        let head = anchored();
        let tip = u64::from(head.end_height);
        let check = |rpc: NodeClient| {
            let head = head.clone();
            async move {
                match check_anchor(&rpc, &head_anchor(&head)).await {
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
        // A segment's companion construction block is checked the same way, even with
        // its payment table's end on the chain.
        let directory = anchored_directory();
        let segment = |table: zakura_pir_publisher::receiver::witness::Manifest| {
            let snapshot =
                zakura_pir_publisher::receiver::snapshot::Snapshot::build(directory.clone(), &[])
                    .unwrap();
            let server = crate::prepared::payments(snapshot).unwrap();
            Publication {
                payments: server.manifest().clone(),
                witnesses: zakura_pir_publisher::receiver::session::WitnessManifest {
                    protocol: zakura_pir_publisher::receiver::session::PROTOCOL.into(),
                    table,
                    public_digest: [0; 32],
                },
            }
        };
        let rpc = chain(tip, 3, Some(300), false).await;
        let table = companion(&directory);
        assert_eq!(
            check_anchor(&rpc, &segment_anchors(&segment(table.clone())))
                .await
                .unwrap(),
            None
        );
        let mut wrong = table.clone();
        wrong.tree_size = 299;
        let failure = check_anchor(&rpc, &segment_anchors(&segment(wrong)))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            failure.1,
            json!({"witness_tree_size": 299, "node_tree_size": 300})
        );
        wrong = table.clone();
        wrong.tree_hash = [4; 32];
        let failure = check_anchor(&rpc, &segment_anchors(&segment(wrong)))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(failure.1, json!({"witness_anchor_off_chain": tip}));
    }

    /// A node for [`oracle`] over [`anchored`]: its tip and the hash it gives at the
    /// anchor's height `end`, `[anchor; 32]`, with the tree size after it. Genesis is
    /// `genesis`, and every other block below the anchor, the fixture's among them, is
    /// `[fixture; 32]`. With `fails` set to `"getblockhash"` it refuses every
    /// `getblockhash` but genesis. With `moved`, the anchor's height holds `[9; 32]`
    /// after the boundary's first read, as when the chain changes during the checks. Its
    /// `z_getsubtreesbyindex` gives `root` as the requested completed subtree, or none
    /// for `None`, and answers for another index when `fails` is `"subtree index"`.
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
                        ("z_getsubtreesbyindex", _) => {
                            let index = params[1].as_u64().unwrap()
                                + u64::from(node.fails == "subtree index");
                            let subtrees = match node.root {
                                Some(root) => {
                                    json!([{"root": hex::encode(root), "end_height": end}])
                                }
                                None => json!([]),
                            };
                            json!({"pool": "ironwood", "start_index": index, "subtrees": subtrees})
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
    /// to `[5; 32]` below the anchor in Ironwood shard zero: the highest tip and the
    /// shard's root once a node verifies, or the failure.
    async fn decide(nodes: &[Node]) -> std::result::Result<(u64, Option<Hash>), Failure> {
        let mut clients = Vec::new();
        for node in nodes {
            clients.push(NodeClient::new(serve_node(*node).await, None).unwrap());
        }
        let directory = anchored();
        let pinned = (directory.end_height - 1, [5; 32]);
        let (_, root, top) = oracle(&clients, &directory, pinned, 0).await?;
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
        let (top, _) = decide(&nodes).await.unwrap();
        assert_eq!(top, end + 50);
        assert_eq!(
            check_lag(top, &anchored(), 12).unwrap().0,
            "stale_publication"
        );
        // With every node that reached the publication failing, each attempt is listed.
        let (category, detail) = decide(&nodes[2..]).await.unwrap_err();
        assert_eq!(category, "oracle_unavailable");
        assert_eq!(detail["attempts"].as_array().unwrap().len(), 3);
        assert_eq!(detail["node_tip"], end + 50);
        let (category, detail) = decide(&[behind]).await.unwrap_err();
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
        assert_eq!(decide(&[other, good(end)]).await, Ok((end, Some([7; 32]))));
        let (category, detail) = decide(&[other]).await.unwrap_err();
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
            let (category, _) = decide(&[good(end + 1), first]).await.unwrap_err();
            assert_eq!(category, expected);
            let (category, _) = decide(&[first, good(end + 2)]).await.unwrap_err();
            assert_eq!(category, expected);
            assert_eq!(
                decide(&[good(end + 2), first]).await,
                Ok((end + 2, Some([7; 32])))
            );
        }
    }

    /// The fixture shard's root comes from the node that decides: a node whose subtree
    /// answer is for another index hands over to the next, and one that has not
    /// completed the shard decides with no root, which a final payload then fails.
    #[tokio::test]
    async fn the_shard_root_comes_from_the_deciding_node() {
        let end = u64::from(anchored().end_height);
        let wrong = Node {
            fails: "subtree index",
            ..good(end + 3)
        };
        let other = Node {
            root: Some([8; 32]),
            ..good(end + 1)
        };
        assert_eq!(decide(&[wrong, other]).await, Ok((end + 3, Some([8; 32]))));
        let (category, detail) = decide(&[wrong]).await.unwrap_err();
        assert_eq!(category, "oracle_unavailable");
        assert_eq!(detail["attempts"].as_array().unwrap().len(), 1);
        let incomplete = Node {
            root: None,
            ..good(end + 2)
        };
        assert_eq!(decide(&[incomplete, other]).await, Ok((end + 2, None)));
    }

    /// After the lookup, a reorg of any anchor's block fails the probe, including a
    /// segment's companion construction block below an unchanged payment end.
    #[tokio::test]
    async fn a_reorg_of_either_anchor_during_the_lookup_fails_the_probe() {
        let directory = anchored_directory();
        let end = u64::from(directory.end_height);
        let mut table = companion(&directory);
        table.tree_height = directory.end_height - 1;
        table.tree_hash = [5; 32];
        let recheck = |node: Node| {
            let (directory, table) = (directory.clone(), table.clone());
            async move {
                let rpc = NodeClient::new(serve_node(node).await, None).unwrap();
                let anchors = [
                    TreeAnchor::payments(&directory),
                    TreeAnchor::witnesses(&table),
                ];
                still_anchored(&rpc, anchors).await.is_ok()
            }
        };
        assert!(recheck(good(end)).await);
        let witness_reorged = Node {
            fixture: 6,
            ..good(end)
        };
        let end_reorged = Node {
            anchor: 4,
            ..good(end)
        };
        assert!(!recheck(witness_reorged).await);
        assert!(!recheck(end_reorged).await);
    }

    /// Serves an empty publication of `directory`. Returns the probe's arguments for it,
    /// with the node at `rpc`.
    async fn publish(
        directory: zakura_pir_publisher::receiver::snapshot::Manifest,
        rpc: &str,
    ) -> Args {
        let genesis = directory.genesis;
        let snapshot =
            zakura_pir_publisher::receiver::snapshot::Snapshot::build(directory, &[]).unwrap();
        let publications = receiver_pir_server::Publications::default();
        let table = crate::witness::table(&snapshot, &[]).unwrap();
        let segment = crate::verify::prepare(snapshot, table).unwrap();
        let publication = receiver_pir_server::Publication::assemble(
            genesis,
            0,
            vec![(false, vec![segment], None)],
        )
        .unwrap();
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

    #[cfg(feature = "qualification-tests")]
    /// A publication on another network is an answer mismatch naming both genesis
    /// hashes, whether the nodes are on mainnet, on the publication's network (where
    /// the fixture's block differs), or unreachable.
    #[tokio::test]
    async fn a_publication_on_another_network_is_an_answer_mismatch() {
        let mut directory = anchored_directory();
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
        // Port 0 accepts no connection, unlike a released port another test may take.
        let unreachable = "http://127.0.0.1:0".to_owned();
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

    /// Probes an empty publication covering `start..=end`, against a node agreeing with
    /// it, for the mainnet fixture moved to the block after Ironwood activation. Asserts
    /// the answer mismatch, and that it reaches the lookup only when `reaches_lookup`.
    async fn probe_coverage(start: u32, end: u32, reaches_lookup: bool) {
        let activation = crate::blocks::ironwood_activation();
        let mut fixture: Value = serde_json::from_slice(MAINNET_FIXTURE).unwrap();
        fixture["height"] = (activation + 1).into();
        fixture["block_hash"] = hash::display(&[5; 32]).into();
        let fixture = fixture.to_string();
        let end_node = u64::from(end);
        let rpc = serve_node(Node {
            end: end_node,
            ..good(end_node)
        })
        .await;
        let mut directory = anchored_directory();
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

    /// Wallets require history from Ironwood activation, so a publication starting
    /// after it is an answer mismatch before any lookup, though it agrees with the node.
    #[tokio::test]
    async fn a_publication_starting_after_ironwood_activation_is_refused() {
        let activation = crate::blocks::ironwood_activation();
        probe_coverage(activation + 1, activation + 2, false).await;
    }

    /// A publication starting at Ironwood activation reaches the lookup.
    #[tokio::test]
    async fn a_publication_starting_at_ironwood_activation_reaches_the_lookup() {
        let activation = crate::blocks::ironwood_activation();
        probe_coverage(activation, activation + 2, true).await;
    }

    /// The node and witness payloads encode tree nodes alike: folding the fixture
    /// refund's commitment with empty subtrees through `combine` gives the root the pinned
    /// node's tree reports through `Root::bytes_in_display_order`, not its reverse. The
    /// node encodes subtree roots the same way (`Node::bytes_in_display_order`).
    #[test]
    fn the_node_and_witness_payloads_encode_nodes_alike() {
        use zakura_pir_publisher::receiver::crypto::witness::{combine, empty_root};
        use zakura_server_chain::testing::{ironwood_root_of, Tx};
        let raw = hex::decode(include_str!("../tests/fixtures/receiver-refund.hex").trim());
        let refund = Tx::from_bytes(&raw.unwrap());
        let mut node = refund.ironwood_commitments()[0];
        let node_root = ironwood_root_of(&[refund]);
        for level in 0..32 {
            node = combine(level, &node, &empty_root(level).unwrap()).unwrap();
        }
        assert_eq!(node, node_root);
        let mut reversed = node_root;
        reversed.reverse();
        assert_ne!(node, reversed);
    }

    #[cfg(feature = "qualification-tests")]
    /// Serves a publication whose one record, at position 5, has a final payload of
    /// siblings `[level + 1; 32]` in a tree of `tree_size` leaves. Returns the origin,
    /// the publication, the record and its shard's root.
    async fn witnessed(
        tree_size: u64,
        corrupt: bool,
    ) -> (
        String,
        Publication,
        zakura_pir_publisher::receiver::RawRecord,
        Hash,
    ) {
        use zakura_pir_publisher::receiver::{crypto::witness::combine, wire::encode_payload};
        let mut manifest =
            super::common::manifest(zakura_pir_publisher::receiver::session::MIN_ROWS);
        (manifest.start_position, manifest.end_position) = (0, tree_size);
        let mut record = super::common::record(0, 1);
        record.payment.position = 5;
        let siblings: [Option<Hash>; 16] = std::array::from_fn(|level| {
            zakura_pir_publisher::receiver::wire::supplied(5, tree_size, level as u8)
                .then_some([level as u8 + 1; 32])
        });
        let mut root = record.payment.cmx;
        for (level, sibling) in siblings.iter().enumerate() {
            let sibling = sibling.unwrap_or([0; 32]);
            root = match (5u64 >> level) & 1 {
                0 => combine(level as u8, &root, &sibling),
                _ => combine(level as u8, &sibling, &root),
            }
            .unwrap();
        }
        let mut siblings = siblings;
        if corrupt {
            siblings[0] = Some([0xff; 32]);
        }
        let payload = encode_payload(&siblings, 5, tree_size).unwrap();
        let snapshot =
            zakura_pir_publisher::receiver::snapshot::Snapshot::build(manifest, &[record.clone()])
                .unwrap();
        let genesis = snapshot.manifest.genesis;
        let table = crate::witness::table(&snapshot, &[payload]).unwrap();
        // A corrupt payload would be refused by the publisher's checks; serve it anyway.
        let segment = match corrupt {
            false => crate::verify::prepare(snapshot, table).unwrap(),
            true => crate::prepared::segment(snapshot, table).unwrap(),
        };
        let session = segment.publication();
        let publication = receiver_pir_server::Publication::assemble(
            genesis,
            0,
            vec![(false, vec![segment], None)],
        )
        .unwrap();
        let publications = receiver_pir_server::Publications::default();
        assert!(publications.publish(publication, 0));
        let app = crate::router::router_with_publications(publications);
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", socket.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
        (origin, session, record, root)
    }

    #[cfg(feature = "qualification-tests")]
    /// The fixture's companion payload, read over live PIR by its record's ordinal, must
    /// fold to the node's shard root once final.
    #[tokio::test]
    async fn the_witness_payload_must_reach_the_nodes_shard_root() {
        let http = reqwest::Client::new();
        let (origin, publication, record, root) = witnessed(1 << 17, false).await;
        let check = |record: zakura_pir_publisher::receiver::RawRecord, root: Option<Hash>| {
            let (http, origin, publication) = (http.clone(), origin.clone(), publication.clone());
            async move {
                check_witness(&http, &origin, &publication, &record, root)
                    .await
                    .unwrap()
                    .map(|failure| failure.1)
            }
        };
        assert_eq!(check(record.clone(), Some(root)).await, None);
        let detail = check(record.clone(), Some([9; 32])).await.unwrap();
        assert_eq!(detail["node_root"], hex::encode([9; 32]));
        // A final payload for a shard the node has not completed.
        assert!(check(record.clone(), None).await.is_some());
        // Another commitment at the position folds elsewhere.
        let mut wrong = record.clone();
        wrong.payment.cmx = [9; 32];
        assert!(check(wrong, Some(root)).await.is_some());
    }

    #[cfg(feature = "qualification-tests")]
    /// A provisional companion payload is unavailable to check, and one supplying a
    /// noncanonical node is wrong.
    #[tokio::test]
    async fn a_provisional_witness_payload_is_unchecked_unless_malformed() {
        let http = reqwest::Client::new();
        let (origin, publication, record, _) = witnessed(64, false).await;
        let provisional = check_witness(&http, &origin, &publication, &record, None).await;
        assert_eq!(provisional.unwrap().unwrap().0, "oracle_unavailable");
        let (origin, publication, record, _) = witnessed(64, true).await;
        let corrupt = check_witness(&http, &origin, &publication, &record, None).await;
        let (category, detail) = corrupt.unwrap().unwrap();
        assert_eq!(
            (category, &detail["witness_path"]),
            ("answer_mismatch", &json!(5))
        );
    }

    /// The hex inventory ID `[byte; 32]`, as health reports one.
    fn id(byte: u8) -> String {
        hex::encode([byte; 32])
    }

    /// A head promising many chunks whose first chunk does not fit it is refused after
    /// that one request, before any other chunk is fetched.
    #[tokio::test]
    async fn the_inventory_capture_stops_at_the_first_bad_chunk() {
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };
        let requests = Arc::new(AtomicUsize::new(0));
        let counted = requests.clone();
        let app = Router::new().fallback(move || {
            counted.fetch_add(1, Ordering::SeqCst);
            async { Json(json!({"inventory": vec![0u8; 32], "index": 0, "shards": []})) }
        });
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", socket.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
        let mut head = anchored();
        head.shards = zakura_pir_publisher::receiver::inventory::MAX_SHARDS;
        let failure = capture(&reqwest::Client::new(), &origin, head)
            .await
            .unwrap();
        let (category, detail) = failure.unwrap_err();
        assert_eq!(category, "answer_mismatch");
        assert!(detail["inventory"].is_string(), "{detail}");
        assert_eq!(requests.load(Ordering::SeqCst), 1);
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

    /// A successful inventory head response that is malformed JSON or a head failing
    /// validation (protocol or shard count) reaches the probe's result as an answer
    /// mismatch.
    #[tokio::test]
    async fn an_invalid_served_manifest_is_an_answer_mismatch() {
        let mut protocol = anchored();
        protocol.protocol = "other".into();
        let mut empty = anchored();
        empty.shards = 0;
        let json = |h: &Head| serde_json::to_string(h).unwrap();
        for body in ["{".to_owned(), json(&protocol), json(&empty)] {
            let app = Router::new().route(
                "/v1/receiver/inventory",
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
        let (probed, rotated) = (id(1), id(2));
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

    #[cfg(feature = "qualification-tests")]
    /// Against an indexer serving a synthetic chain that holds the public refund, a
    /// fixture pinned where the refund was indexed is found with one encrypted query, and
    /// its provisional companion payload is reported unchecked, even when the first node
    /// is unreachable. A misplaced pin is an
    /// answer mismatch, and nodes behind the publication leave it unchecked.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_live_lookup_finds_the_indexed_refund() {
        use axum::extract::State;
        use std::sync::{
            atomic::{AtomicU64, Ordering},
            Arc,
        };
        use zakura_server_chain::testing::receiver::RefundBlock;
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
                // The synthetic chain never completes an Ironwood shard.
                "z_getsubtreesbyindex" => {
                    json!({"pool": "ironwood", "start_index": r["params"][1], "subtrees": []})
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
            min_rows: zakura_pir_publisher::receiver::session::MIN_ROWS,
            shard_rows: zakura_pir_publisher::receiver::session::MIN_ROWS,
            shard_target: None,
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
            .chunks(zakura_pir_publisher::receiver::snapshot::ROW_BYTES)
            .flat_map(|row| {
                row[..zakura_pir_publisher::receiver::snapshot::SLOTS
                    * zakura_pir_publisher::receiver::RECORD_BYTES]
                    .chunks(zakura_pir_publisher::receiver::RECORD_BYTES)
                    .map(|slot| {
                        zakura_pir_publisher::receiver::crypto::Record::decode(slot).unwrap()
                    })
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
            args.push("--no-auth");
            let args = Args::parse_from(args);
            let fixture = fixture.to_string();
            async move {
                let mut lookup = None;
                let failure = probe(args, fixture.as_bytes(), &mut lookup).await;
                (failure.unwrap(), lookup)
            }
        };
        // The synthetic chain never completes the refund's Ironwood shard, so the lookup
        // passes and the provisional witness is reported unchecked.
        let unchecked = |(failure, lookup): (Option<Failure>, _)| {
            (failure.map(|f| f.1["witness"].clone()), lookup)
        };
        let provisional = (Some(json!("provisional")), Some((1, true)));
        assert_eq!(unchecked(run(&fixture, &[&url]).await), provisional);
        let fallback = run(&fixture, &["http://127.0.0.1:1", &url]).await;
        assert_eq!(unchecked(fallback), provisional);
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
