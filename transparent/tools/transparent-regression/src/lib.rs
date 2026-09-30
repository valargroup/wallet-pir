//! Frozen journal expectations and a reference reducer independent of wallet replay.
pub mod journal;
use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use transparent_events::TransparentEvent;
use transparent_filter::ShardMap;
use transparent_wallet::{Anchor, WalletStore};

pub const SCHEMA: &str = "transparent-regression-v1";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct EventRecord {
    pub script: String,
    pub event: String,
}
impl EventRecord {
    pub fn decode(&self) -> Result<TransparentEvent> {
        Ok(TransparentEvent::from_bytes(&hex::decode(&self.event)?)?)
    }
    pub fn new(script: &[u8], event: &TransparentEvent) -> Self {
        Self {
            script: hex::encode(script),
            event: hex::encode(event.to_bytes()),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    pub events: Vec<EventRecord>,
    pub utxos: BTreeMap<String, Value>,
    pub spends: BTreeMap<String, Value>,
    pub history: BTreeMap<String, Value>,
    pub confirmed_balance: u64,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Checkpoint {
    pub anchor: Anchor,
    pub expected: Snapshot,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Case {
    pub id: String,
    pub profile: String,
    pub scripts: Vec<String>,
    pub required_from: u64,
    pub checkpoints: Vec<Checkpoint>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Fixture {
    pub schema: String,
    pub source_sha: String,
    pub provenance: String,
    /// Canonical compact map bytes served by the public origins.
    pub map_sha256: String,
    pub publication_file_sha256: String,
    pub map: ShardMap,
    pub cutoff_height: u64,
    /// Journal-derived hashes, never obtained by trusting the retrieval map.
    pub accepted_headers: BTreeMap<u64, String>,
    pub cases: Vec<Case>,
}

pub fn height(event: &TransparentEvent) -> u64 {
    match event {
        TransparentEvent::Receive(e) => e.height.into(),
        TransparentEvent::Spend(e) => e.height.into(),
    }
}
fn key(txid: transparent_events::Txid, index: u32) -> String {
    format!("{}:{index}", txid.to_display_hex())
}

/// Independent outpoint bookkeeping. This does not call Ledger::replay/apply/history.
pub fn reference(records: &[EventRecord], through: u64) -> Result<Snapshot> {
    let mut decoded = records
        .iter()
        .map(|r| Ok((r.clone(), r.decode()?)))
        .collect::<Result<Vec<_>>>()?;
    decoded.retain(|(_, e)| height(e) <= through);
    decoded.sort_by_key(|(_, e)| e.sort_key());
    let mut seen = BTreeSet::new();
    let mut receives = BTreeMap::new();
    let mut spent = BTreeSet::new();
    let mut result = Snapshot {
        events: vec![],
        utxos: BTreeMap::new(),
        spends: BTreeMap::new(),
        history: BTreeMap::new(),
        confirmed_balance: 0,
    };
    for (record, event) in decoded {
        if !seen.insert(record.clone()) {
            continue;
        }
        result.events.push(record.clone());
        let (txid, h, received, spent_value) = match event {
            TransparentEvent::Receive(e) => {
                let k = key(e.txid, e.output_index);
                let value = json!({"script": record.script, "value": e.value, "height": e.height, "coinbase": e.coinbase});
                if receives.insert(k.clone(), value.clone()).is_some() {
                    bail!("duplicate receive identity {k}");
                }
                result.utxos.insert(k, value);
                (e.txid, e.height, e.value, 0)
            }
            TransparentEvent::Spend(e) => {
                let k = key(e.spent_txid, e.spent_output_index);
                let receive = receives
                    .get(&k)
                    .with_context(|| format!("unresolved expected spend {k}"))?;
                if receive["script"] != record.script {
                    bail!("spent script disagrees for {k}");
                }
                if !spent.insert(k.clone()) {
                    bail!("double spend in reference {k}");
                }
                let value = receive["value"].as_u64().context("receive value")?;
                result.utxos.remove(&k);
                result.spends.insert(k, json!({"script": record.script, "value": value, "height": e.height, "spending_txid": e.spending_txid.to_display_hex()}));
                (e.spending_txid, e.height, 0, value)
            }
        };
        let summary = result
            .history
            .entry(txid.to_display_hex())
            .or_insert(json!({"height": h, "received": 0u64, "spent": 0u64}));
        summary["received"] = json!(summary["received"]
            .as_u64()
            .unwrap()
            .checked_add(received)
            .context("received overflow")?);
        summary["spent"] = json!(summary["spent"]
            .as_u64()
            .unwrap()
            .checked_add(spent_value)
            .context("spent overflow")?);
    }
    result.events.sort();
    result.confirmed_balance = result.utxos.values().try_fold(0u64, |sum, v| {
        sum.checked_add(v["value"].as_u64().unwrap())
            .context("balance overflow")
    })?;
    Ok(result)
}

pub fn actual(store: &impl WalletStore) -> Result<Snapshot> {
    let ledger = store.ledger()?;
    if !ledger.unresolved().is_empty() {
        bail!("unresolved wallet spends");
    }
    let mut events = store
        .events()?
        .iter()
        .map(|e| EventRecord::new(&e.script, &e.event))
        .collect::<Vec<_>>();
    events.sort();
    Ok(Snapshot {
        events,
        utxos: ledger.utxos().map(|u| (key(u.txid, u.output_index), json!({"script": hex::encode(&u.script), "value": u.value, "height": u.creation_height, "coinbase": u.coinbase}))).collect(),
        spends: ledger.spends().iter().map(|s| (key(s.spent_txid, s.spent_output_index), json!({"script": hex::encode(&s.script), "value": s.value, "height": s.height, "spending_txid": s.spending_txid.to_display_hex()}))).collect(),
        history: ledger.history().iter().map(|t| (t.txid.to_display_hex(), json!({"height": t.height, "received": t.received, "spent": t.spent}))).collect(),
        confirmed_balance: ledger.confirmed_balance(),
    })
}

pub fn validate(f: &Fixture) -> Result<()> {
    if f.schema != SCHEMA || f.cases.is_empty() {
        bail!("unsupported or empty fixture");
    }
    f.map.check_shape().map_err(anyhow::Error::msg)?;
    let mut ids = BTreeSet::new();
    for case in &f.cases {
        if case.id.is_empty()
            || !case
                .id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            || !ids.insert(&case.id)
        {
            bail!("invalid/duplicate case id");
        }
        if case.scripts.is_empty() || case.checkpoints.is_empty() {
            bail!("empty case {}", case.id);
        }
        let scripts = case
            .scripts
            .iter()
            .map(|s| {
                let b = hex::decode(s)?;
                let supported =
                    (b.len() == 25 && b[..3] == [0x76, 0xa9, 0x14] && b[23..] == [0x88, 0xac])
                        || (b.len() == 23 && b[..2] == [0xa9, 0x14] && b[22] == 0x87);
                if !supported || hex::encode(&b) != *s {
                    bail!("noncanonical/unsupported script");
                }
                Ok(s.clone())
            })
            .collect::<Result<BTreeSet<_>>>()?;
        let final_state = &case.checkpoints.last().unwrap().expected;
        let records = final_state
            .events
            .iter()
            .map(EventRecord::decode)
            .collect::<Result<Vec<_>>>()?;
        match case.profile.as_str() {
            "unused" | "unused-p2sh" if !records.is_empty() => bail!("unused profile has activity"),
            "coinbase"
                if !records
                    .iter()
                    .any(|e| matches!(e, TransparentEvent::Receive(r) if r.coinbase)) =>
            {
                bail!("coinbase profile has no coinbase")
            }
            "zero-balance"
                if records.is_empty()
                    || !final_state.utxos.is_empty()
                    || final_state.spends.is_empty() =>
            {
                bail!("zero-balance profile must retain spent history")
            }
            "multi-script"
                if case.scripts.len() < 2
                    || !final_state.history.values().any(|t| {
                        t["received"].as_u64().unwrap_or(0) > 0
                            && t["spent"].as_u64().unwrap_or(0) > 0
                    }) =>
            {
                bail!("multi-script profile lacks a self-transfer")
            }
            "p2sh" if records.is_empty() || !case.scripts.iter().any(|s| s.len() == 46) => {
                bail!("active P2SH profile lacks activity")
            }
            _ => {}
        }
        let mut previous = None;
        for c in &case.checkpoints {
            if previous.is_some_and(|h| c.anchor.height <= h)
                || c.anchor.height < case.required_from
                || f.map.shard_for_height(c.anchor.height).is_none()
            {
                bail!("invalid checkpoint order/range");
            }
            previous = Some(c.anchor.height);
            if f.accepted_headers.get(&c.anchor.height) != Some(&c.anchor.hash) {
                bail!("checkpoint lacks journal hash");
            }
            for e in &c.expected.events {
                if !scripts.contains(&e.script) || height(&e.decode()?) > c.anchor.height {
                    bail!("expected event outside wallet/anchor");
                }
            }
            if reference(&final_state.events, c.anchor.height)? != c.expected {
                bail!("inconsistent frozen expectation");
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use transparent_events::{ReceiveEvent, SpendEvent, Txid};
    #[test]
    fn reference_keeps_zero_balance_history_and_bounds_spends_by_anchor() {
        let script = hex::decode(format!("a914{}87", "11".repeat(20))).unwrap();
        let receive = TransparentEvent::Receive(ReceiveEvent {
            metadata: None,
            height: 10,
            txid: Txid([1; 32]),
            transaction_index: 0,
            output_index: 0,
            value: 123,
            coinbase: true,
        });
        let spend = TransparentEvent::Spend(SpendEvent {
            metadata: None,
            height: 20,
            spending_txid: Txid([2; 32]),
            transaction_index: 0,
            input_index: 0,
            spent_txid: Txid([1; 32]),
            spent_output_index: 0,
        });
        let records = vec![
            EventRecord::new(&script, &spend),
            EventRecord::new(&script, &receive),
        ];
        let before = reference(&records, 9).unwrap();
        assert!(before.events.is_empty());
        let received = reference(&records, 10).unwrap();
        assert_eq!(received.confirmed_balance, 123);
        assert_eq!(received.utxos.values().next().unwrap()["coinbase"], true);
        let spent = reference(&records, 20).unwrap();
        assert_eq!(spent.confirmed_balance, 0);
        assert_eq!(spent.history.len(), 2);
        assert_eq!(spent.events.len(), 2);
        assert_eq!(spent.spends.len(), 1);
        assert!(
            reference(&records[..1], 20).is_err(),
            "missing receives must never become fabricated outputs"
        );
    }
}
