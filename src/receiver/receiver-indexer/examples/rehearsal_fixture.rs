//! Operator-rehearsal fixture for `serve-publication`; never a production tool.
//!
//! `stage DIR NAME OFFSET HASH_BYTE` writes a complete publication directory, in the
//! indexer's file layout, ending `OFFSET` blocks after Ironwood activation at a block
//! hash of `HASH_BYTE` repeated, with two payments to the shared fixture receiver,
//! and prints its record and the blocks it depends on, each with the Ironwood tree size
//! a node must report after it. `lookup ORIGIN OFFSET HASH_BYTE` retrieves the
//! fixture receiver's payments over encrypted PIR and exits zero only if they are
//! exactly the two this fixture staged: the oracle is the construction, not the server.
#[path = "../../receiver-store/tests/common/mod.rs"]
mod common;
#[path = "../../receiver-pir-server/tests/common/history.rs"]
mod history;
use zakura_pir_client::receiver::transport::Transport;
use zakura_pir_publisher::receiver::session::Error;
use zakura_pir_publisher::receiver::session::MIN_ROWS;
use zakura_pir_publisher::receiver::snapshot::SnapshotPublisherExt as _;
use zakura_pir_publisher::receiver::witness;
use zakura_pir_publisher::receiver::{snapshot::Snapshot, Hash, Payment};

fn activation() -> u32 {
    receiver_indexer::blocks::ironwood_activation()
}

fn payments(offset: u32, hash: Hash) -> Vec<Payment> {
    [0u8, 5]
        .into_iter()
        .enumerate()
        .map(|(page, position)| {
            let mut payment = common::record(page as u32, 2).payment;
            payment.height = activation() + offset;
            payment.block_hash = hash;
            payment.position = position.into();
            payment.cmx = [position + 1; 32];
            payment
        })
        .collect()
}

fn publication(offset: u32, hash: Hash) -> (Snapshot, witness::Table) {
    let mut manifest = common::manifest(MIN_ROWS);
    manifest.genesis = zakura_server_chain::Network::Mainnet.genesis_hash();
    manifest.start_height = activation();
    manifest.end_height = activation() + offset;
    manifest.end_hash = hash;
    manifest.start_position = 0;
    manifest.end_position = 8;
    let records: Vec<_> = payments(offset, hash)
        .into_iter()
        .enumerate()
        .map(|(page, payment)| {
            let mut record = common::record(page as u32, 2);
            record.payment = payment;
            record
        })
        .collect();
    let snapshot = Snapshot::build(manifest, &records).unwrap();
    let tree: Vec<Hash> = (1..=8).map(|i| [i; 32]).collect();
    let payloads = zakura_pir_publisher::receiver::witness::payloads(&tree, &[0, 5]).unwrap();
    let table = receiver_indexer::witness::table(&snapshot, &payloads).unwrap();
    (snapshot, table)
}

struct Http(reqwest::Client);
impl Transport for Http {
    async fn get(&self, url: &str, limit: usize) -> Result<Vec<u8>, Error> {
        read(self.0.get(url), limit).await
    }
    async fn post(&self, url: &str, body: Vec<u8>, limit: usize) -> Result<Vec<u8>, Error> {
        read(self.0.post(url).body(body), limit).await
    }
}
async fn read(request: reqwest::RequestBuilder, limit: usize) -> Result<Vec<u8>, Error> {
    let response = request
        .send()
        .await
        .map_err(|e| Error::Transport(e.to_string()))?;
    if !response.status().is_success() {
        return Err(Error::Transport(response.status().to_string()));
    }
    let body = response
        .bytes()
        .await
        .map_err(|e| Error::Transport(e.to_string()))?;
    if body.len() > limit {
        return Err(Error::Malformed);
    }
    Ok(body.to_vec())
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let byte = |s: &str| [s.parse::<u8>().expect("HASH_BYTE"); 32];
    match args.get(1).map(String::as_str) {
        Some("stage") => {
            let (dir, name) = (std::path::Path::new(&args[2]), &args[3]);
            let (offset, hash) = (args[4].parse().expect("OFFSET"), byte(&args[5]));
            let (snapshot, witness) = publication(offset, hash);
            let dir = dir.join(name);
            std::fs::create_dir_all(&dir).unwrap();
            let segment = hex::encode(snapshot.manifest.revision().unwrap());
            let file = |ext: &str| dir.join(format!("{segment}.{ext}"));
            let layout = receiver_indexer::application::Layout {
                genesis: snapshot.manifest.genesis,
                history_revision: snapshot.manifest.history_revision,
                shards: vec![receiver_indexer::application::LayoutShard {
                    sealed: false,
                    segments: vec![receiver_indexer::application::LayoutSegment {
                        payments: snapshot.manifest.revision().unwrap(),
                        witnesses: witness.manifest.revision().unwrap(),
                    }],
                    filter: None,
                }],
            };
            let (id, bytes) = layout.encode().unwrap();
            let revision = hex::encode(id);
            std::fs::write(dir.join(format!("{revision}.inventory.json")), bytes).unwrap();
            std::fs::write(
                file("json"),
                serde_json::to_vec_pretty(&snapshot.manifest).unwrap(),
            )
            .unwrap();
            std::fs::write(file("rows"), &snapshot.data).unwrap();
            let companion = hex::encode(witness.manifest.revision().unwrap());
            let companion = |ext: &str| dir.join(format!("{companion}.{ext}"));
            std::fs::write(companion("witness"), &witness.data).unwrap();
            std::fs::write(
                companion("witness.json"),
                serde_json::to_vec_pretty(&witness.manifest).unwrap(),
            )
            .unwrap();
            use receiver_indexer::canonical::TreeAnchor;
            let anchors: Vec<_> = [
                TreeAnchor::payments(&snapshot.manifest),
                TreeAnchor::witnesses(&witness.manifest),
            ]
            .iter()
            .map(|anchor| {
                serde_json::json!({"height": anchor.height,
                    "hash": zakura_server_chain::hash::display(&anchor.hash),
                    "size": anchor.size})
            })
            .collect();
            let genesis = snapshot.manifest.genesis;
            let segment = receiver_indexer::verify::prepare(snapshot, witness).unwrap();
            let inventory = receiver_pir_server::Publication::assemble(
                genesis,
                0,
                vec![(false, vec![segment], None)],
            )
            .unwrap();
            let session = inventory.id();
            println!(
                "{}",
                serde_json::json!({"directory": dir, "revision": revision,
                    "end_height": activation() + offset,
                    "end_hash": zakura_server_chain::hash::display(&hash),
                    "anchors": anchors, "session": hex::encode(session)})
            );
        }
        Some("lookup") => {
            let origin = &args[2];
            let (offset, hash) = (args[3].parse().expect("OFFSET"), byte(&args[4]));
            let accepted = history::Accepted {
                genesis: zakura_server_chain::Network::Mainnet.genesis_hash(),
                required_start: activation(),
                height: activation() + offset,
                hash,
            };
            let http = Http(reqwest::Client::new());
            let session = history::connect(origin, http, accepted).await.unwrap();
            let mut found = history::inventory_history(&session.client, common::receiver())
                .await
                .unwrap();
            found.sort_by_key(|payment| payment.position);
            let exact = found == payments(offset, hash);
            println!(
                "{}",
                serde_json::json!({"payments": found.len(), "exact": exact,
                    "positions": found.iter().map(|p| p.position).collect::<Vec<_>>()})
            );
            std::process::exit(if exact { 0 } else { 1 });
        }
        _ => {
            eprintln!("usage: rehearsal_fixture stage DIR NAME OFFSET HASH_BYTE | lookup ORIGIN OFFSET HASH_BYTE");
            std::process::exit(2);
        }
    }
}
