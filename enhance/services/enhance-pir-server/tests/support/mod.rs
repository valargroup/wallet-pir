//! Synthetic mixed transactions and a trusted-RPC test double.
//!
//! Mixed transactions keep the public fixture's two Ironwood actions and
//! replace its value balance and transparent parts. Values are hand-set, so
//! expected fees are independent of the producer. The node double serves
//! single JSON-RPC 1.0 calls and JSON-RPC 2.0 batches. Proofs and signatures
//! are not valid; the producer neither checks nor needs them.
#![allow(dead_code)]

use axum::{extract::State, http::HeaderMap, http::StatusCode, routing::post, Json, Router};
use enhance_pir::EnhanceRecord;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use zakura_chain::amount::Amount;
use zakura_chain::block::Height;
use zakura_chain::serialization::{ZcashDeserialize, ZcashSerialize};
use zakura_chain::transaction::{self, LockTime, Transaction};
use zakura_chain::transparent::{Input, OutPoint, Output, Script};

pub const FIXTURE_HEX: &str = include_str!("../fixtures/ironwood-fee-expiry.hex");

pub fn fixture() -> Transaction {
    reparse_bytes(&hex::decode(FIXTURE_HEX.trim()).unwrap())
}

fn reparse_bytes(bytes: &[u8]) -> Transaction {
    Transaction::zcash_deserialize(bytes).unwrap()
}

/// Serializes and parses back, as the node and producer see it.
pub fn reparse(transaction: Transaction) -> Transaction {
    reparse_bytes(&transaction.zcash_serialize_to_vec().unwrap())
}

pub fn raw(transaction: &Transaction) -> String {
    hex::encode(transaction.zcash_serialize_to_vec().unwrap())
}

fn output(value: i64) -> Output {
    Output {
        value: Amount::try_from(value).unwrap(),
        // A non-standard test script; no real address is encoded.
        lock_script: Script::new(&[0x51]),
    }
}

/// Spends output `index` of `previous`.
pub fn spend(previous: &Transaction, index: u32) -> Input {
    spend_outpoint(OutPoint {
        hash: previous.hash(),
        index,
    })
}

pub fn spend_outpoint(outpoint: OutPoint) -> Input {
    Input::PrevOut {
        outpoint,
        unlock_script: Script::new(&[0x51]),
        sequence: u32::MAX,
    }
}

/// A transparent V4 transaction paying `values`. Its own input refers to an
/// arbitrary `seed` outpoint that is never resolved.
pub fn funding(seed: u8, values: &[i64]) -> Transaction {
    reparse(Transaction::V4 {
        inputs: vec![spend_outpoint(OutPoint {
            hash: transaction::Hash([seed; 32]),
            index: 0,
        })],
        outputs: values.iter().copied().map(output).collect(),
        lock_time: LockTime::unlocked(),
        expiry_height: Height(0),
        joinsplit_data: None,
        sapling_shielded_data: None,
    })
}

pub struct Mixed {
    pub inputs: Vec<Input>,
    pub outputs: Vec<i64>,
    pub ironwood: i64,
    /// Orchard value balance; the fixture's Ironwood bundle is reused as the
    /// Orchard bundle since both pools share one encoding.
    pub orchard: Option<i64>,
    /// Sapling bundle and its value balance.
    pub sapling: Option<(zakura_chain::sapling::ShieldedData<zakura_chain::sapling::SharedAnchor>, i64)>,
}

impl Mixed {
    pub fn new(inputs: Vec<Input>, outputs: &[i64], ironwood: i64) -> Self {
        Self {
            inputs,
            outputs: outputs.to_vec(),
            ironwood,
            orchard: None,
            sapling: None,
        }
    }

    pub fn build(self) -> Transaction {
        let Transaction::V6 {
            network_upgrade,
            lock_time,
            expiry_height,
            mut ironwood_shielded_data,
            ..
        } = fixture()
        else {
            panic!("fixture is V6");
        };
        let ironwood = ironwood_shielded_data.as_mut().unwrap();
        ironwood.value_balance = Amount::try_from(self.ironwood).unwrap();
        let orchard_shielded_data = self.orchard.map(|value| {
            let mut orchard = ironwood.clone();
            // Ironwood's cross-address flag is reserved in the Orchard pool.
            orchard.flags = zakura_chain::orchard::Flags::ENABLE_SPENDS
                | zakura_chain::orchard::Flags::ENABLE_OUTPUTS;
            orchard.value_balance = Amount::try_from(value).unwrap();
            orchard
        });
        let sapling_shielded_data = self.sapling.map(|(mut sapling, value)| {
            sapling.value_balance = Amount::try_from(value).unwrap();
            sapling
        });
        reparse(Transaction::V6 {
            network_upgrade,
            lock_time,
            expiry_height,
            inputs: self.inputs,
            outputs: self.outputs.into_iter().map(output).collect(),
            sapling_shielded_data,
            orchard_shielded_data,
            ironwood_shielded_data,
        })
    }
}

/// A coinbase transaction carrying the fixture's Ironwood actions.
pub fn ironwood_coinbase(height: u32) -> Transaction {
    let Transaction::V6 {
        network_upgrade,
        lock_time,
        expiry_height,
        mut ironwood_shielded_data,
        ..
    } = fixture()
    else {
        panic!("fixture is V6");
    };
    ironwood_shielded_data.as_mut().unwrap().value_balance = Amount::try_from(-50_000).unwrap();
    reparse(Transaction::V6 {
        network_upgrade,
        lock_time,
        expiry_height,
        inputs: vec![Input::Coinbase {
            height: Height(height),
            data: vec![0x51; 4],
            sequence: u32::MAX,
        }],
        outputs: vec![output(100_000)],
        sapling_shielded_data: None,
        orchard_shielded_data: None,
        ironwood_shielded_data,
    })
}

/// A synthetic block envelope: zero roots, `nonce` distinguishes hashes.
/// The parser accepts it; consensus validity is trusted to the RPC.
pub fn block(transactions: &[Transaction], nonce: u8) -> (String, String) {
    let mut raw = vec![0u8; 140];
    raw[..4].copy_from_slice(&4u32.to_le_bytes());
    raw[108] = nonce;
    raw.extend_from_slice(&[0xfd, 0x40, 0x05]);
    raw.extend_from_slice(&[0; 1344]);
    assert!(transactions.len() < 0xfd);
    raw.push(transactions.len() as u8);
    for transaction in transactions {
        raw.extend_from_slice(&transaction.zcash_serialize_to_vec().unwrap());
    }
    let parsed = zakura_chain::block::Block::zcash_deserialize(raw.as_slice()).unwrap();
    (hex::encode(raw), parsed.hash().to_string())
}

/// The fixture's frozen record for action `index`, without epk and compact prefix.
pub fn fixture_record(index: usize) -> EnhanceRecord {
    let hex = [
        include_str!("../fixtures/ironwood-canonical-record-0.hex"),
        include_str!("../fixtures/ironwood-canonical-record-1.hex"),
    ][index];
    let bytes = hex::decode(hex.trim()).unwrap()[84..].to_vec();
    EnhanceRecord::from_bytes(bytes.try_into().unwrap()).unwrap()
}

/// Asserts the record keeps the fixture action's encrypted fields.
pub fn assert_fixture_action(record: &EnhanceRecord, index: usize) {
    let fixture = fixture_record(index);
    assert_eq!(record.enc_ciphertext_suffix(), fixture.enc_ciphertext_suffix());
    assert_eq!(record.cv_net(), fixture.cv_net());
    assert_eq!(record.out_ciphertext(), fixture.out_ciphertext());
    assert_eq!(record.metadata().expiry_height(), 3483371);
}

/// Fault injected into JSON-RPC 2.0 batch responses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BatchFault {
    None,
    /// Answer the batch with one error object.
    WholeBatchError,
    /// Drop the last entry.
    Short,
    /// Repeat the first id.
    DuplicateId,
    /// Echo an id past the batch.
    OutOfRangeId,
    /// Answer every transaction with this RPC error code.
    EntryError(i64),
    /// Return a different transaction than requested.
    WrongTransaction,
}

#[derive(Default)]
pub struct Chain {
    /// Height to (raw block hex, hash, Ironwood tree size after it).
    pub blocks: HashMap<u64, (String, String, u64)>,
    pub tip: u64,
    pub transactions: HashMap<String, String>,
    pub fault: Option<BatchFault>,
    /// Heights whose `getblock` is held until released.
    pub hold: Option<u64>,
    /// Method names in request order; a batch records each entry.
    pub calls: Vec<String>,
    /// Count of HTTP requests that were batches.
    pub batches: usize,
}

impl Chain {
    pub fn add_transaction(&mut self, transaction: &Transaction) {
        self.transactions
            .insert(transaction.hash().to_string(), raw(transaction));
    }

    /// Appends a block at `height` holding `transactions`.
    pub fn push_block(&mut self, height: u64, transactions: &[Transaction], nonce: u8) -> String {
        let previous = self
            .blocks
            .get(&(height - 1))
            .map_or(0, |block| block.2);
        let actions: u64 = transactions
            .iter()
            .map(|tx| tx.ironwood_actions().count() as u64)
            .sum();
        let (raw, hash) = block(transactions, nonce);
        for transaction in transactions {
            self.add_transaction(transaction);
        }
        self.blocks
            .insert(height, (raw, hash.clone(), previous + actions));
        self.tip = self.tip.max(height);
        hash
    }

    pub fn count(&self, method: &str) -> usize {
        self.calls.iter().filter(|call| *call == method).count()
    }
}

pub type SharedChain = Arc<Mutex<Chain>>;

fn answer(chain: &mut Chain, request: &Value) -> Value {
    let method = request["method"].as_str().unwrap().to_string();
    chain.calls.push(method.clone());
    let id = request["id"].clone();
    let error = |code: i64, message: &str| json!({"result": null, "error": {"code": code, "message": message}, "id": id});
    let height = |value: &Value| {
        value
            .as_u64()
            .or_else(|| value.as_str().and_then(|s| s.parse().ok()))
            .unwrap()
    };
    let result = match method.as_str() {
        "getblockcount" => json!(chain.tip),
        "getblockhash" => match chain.blocks.get(&height(&request["params"][0])) {
            Some(block) => json!(block.1),
            None => return error(-8, "Block height out of range"),
        },
        "getblock" => {
            let Some(block) = chain.blocks.get(&height(&request["params"][0])) else {
                return error(-8, "Block height out of range");
            };
            match request["params"][1].as_u64().unwrap() {
                0 => json!(block.0),
                2 => json!({"trees": {"ironwood": {"size": block.2}}}),
                _ => panic!("unexpected verbosity"),
            }
        }
        "getrawtransaction" => {
            assert_eq!(request["params"][1], 0);
            if let Some(BatchFault::EntryError(code)) = chain.fault {
                return error(code, "injected");
            }
            if chain.fault == Some(BatchFault::WrongTransaction) {
                json!(chain.transactions.values().next().unwrap())
            } else {
                match chain.transactions.get(request["params"][0].as_str().unwrap()) {
                    Some(raw) => json!(raw),
                    None => {
                        return error(
                            -5,
                            "No such mempool or main chain transaction",
                        )
                    }
                }
            }
        }
        other => panic!("unexpected RPC method {other}"),
    };
    json!({"result": result, "error": null, "id": id})
}

async fn handle(
    State(chain): State<SharedChain>,
    headers: HeaderMap,
    Json(request): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    // Public, test-only credential. No developer or service credentials are used.
    if headers.get("authorization").and_then(|v| v.to_str().ok()) != Some("Basic dGVzdDp0ZXN0") {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let hold = chain.lock().unwrap().hold;
    if let (Some(hold), Some("getblock")) = (hold, request["method"].as_str()) {
        if request["params"][0] == hold.to_string() {
            while chain.lock().unwrap().hold == Some(hold) {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        }
    }
    let mut chain = chain.lock().unwrap();
    let Value::Array(requests) = request else {
        assert_eq!(request["jsonrpc"], "1.0", "single calls stay on JSON-RPC 1.0");
        return Ok(Json(answer(&mut chain, &request)));
    };
    chain.batches += 1;
    let mut answers = Vec::new();
    for request in &requests {
        // The node rejects JSON-RPC 1.0 batches.
        assert_eq!(request["jsonrpc"], "2.0");
        answers.push(answer(&mut chain, request));
    }
    // Responses may arrive in any order; the client places them by id.
    answers.reverse();
    match chain.fault.unwrap_or(BatchFault::None) {
        BatchFault::WholeBatchError => {
            return Ok(Json(
                json!({"result": null, "error": {"code": -32600, "message": "batch refused"}, "id": null}),
            ))
        }
        BatchFault::Short => {
            answers.pop();
        }
        BatchFault::DuplicateId => {
            let id = answers[0]["id"].clone();
            answers.last_mut().unwrap()["id"] = id;
        }
        BatchFault::OutOfRangeId => answers[0]["id"] = json!(requests.len()),
        _ => {}
    }
    Ok(Json(Value::Array(answers)))
}

/// Serves `chain` as a JSON-RPC node at the returned URL.
pub async fn serve_chain(chain: SharedChain) -> (String, tokio::task::JoinHandle<()>) {
    serve(Router::new().route("/", post(handle)).with_state(chain)).await
}

pub async fn serve(router: Router) -> (String, tokio::task::JoinHandle<()>) {
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", socket.local_addr().unwrap());
    (
        origin,
        tokio::spawn(async move {
            axum::serve(socket, router).await.unwrap();
        }),
    )
}

/// A cookie file holding the public test credential.
pub fn cookie(dir: &std::path::Path) -> std::path::PathBuf {
    let path = dir.join("cookie");
    std::fs::write(&path, "test:test").unwrap();
    path
}
