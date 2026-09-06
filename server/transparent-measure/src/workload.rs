//! Choosing the wallet script sets to measure, from the real journal.
//!
//! The shapes are the mainnet study's, so the results are comparable with the
//! evidence already recorded: unused scripts, small and median histories, and
//! the largest history in the range. They are drawn from the journal rather
//! than invented, because a synthetic distribution would measure the fixture
//! instead of the chain.
//!
//! These are *script groupings*, not real wallets. Nobody's wallet held exactly
//! these scripts, and the numbers they produce are workload measurements rather
//! than user traces. That distinction matters when the results are quoted.

use std::collections::BTreeMap;
use transparent_filter::ScriptBytes;
use transparent_shard::MAX_SCRIPT_BYTES;

/// One named script set to measure.
pub struct Workload {
    pub name: String,
    pub scripts: Vec<ScriptBytes>,
    /// What the journal says these scripts hold, for the report.
    pub journal_events: u64,
}

/// Per-script event counts over the whole journal.
pub struct ScriptCensus {
    /// Only scripts the private tables can index. A script outside coverage is
    /// deliberately excluded: it would be in the filters but never in a
    /// directory, so measuring it would measure the coverage gap rather than
    /// the retrieval.
    pub counts: BTreeMap<Vec<u8>, u64>,
    pub excluded: u64,
}

impl ScriptCensus {
    pub fn new() -> Self {
        Self {
            counts: BTreeMap::new(),
            excluded: 0,
        }
    }

    pub fn observe(&mut self, script: &[u8]) {
        if script.len() > MAX_SCRIPT_BYTES {
            self.excluded += 1;
            return;
        }
        *self.counts.entry(script.to_vec()).or_insert(0) += 1;
    }

    /// Scripts ordered by history length, shortest first.
    fn by_length(&self) -> Vec<(&Vec<u8>, u64)> {
        let mut ordered: Vec<_> = self.counts.iter().map(|(s, n)| (s, *n)).collect();
        // Ties broken by the script itself, so selection is reproducible rather
        // than dependent on map iteration order.
        ordered.sort_by(|a, b| a.1.cmp(&b.1).then(a.0.cmp(b.0)));
        ordered
    }

    fn take(&self, at: usize, count: usize) -> Vec<ScriptBytes> {
        let ordered = self.by_length();
        ordered
            .iter()
            .skip(at.min(ordered.len()))
            .take(count)
            .map(|(script, _)| ScriptBytes::new((*script).clone()))
            .collect()
    }

    fn events_of(&self, scripts: &[ScriptBytes]) -> u64 {
        scripts
            .iter()
            .filter_map(|script| self.counts.get(script.as_slice()))
            .sum()
    }
}

/// Scripts that are certainly not on chain.
///
/// Derived from a counter rather than randomness so a run is reproducible, and
/// checked against the census so a coincidental hit cannot silently turn an
/// "unused" workload into a used one.
fn unused(census: &ScriptCensus, count: usize) -> Vec<ScriptBytes> {
    let mut scripts = Vec::with_capacity(count);
    let mut nonce = 0u64;
    while scripts.len() < count {
        let mut bytes = vec![0x76, 0xa9, 0x14];
        bytes.extend_from_slice(&nonce.to_le_bytes());
        bytes.extend_from_slice(&[0xee; 12]);
        bytes.extend_from_slice(&[0x88, 0xac]);
        nonce += 1;
        if census.counts.contains_key(&bytes) {
            continue;
        }
        scripts.push(ScriptBytes::new(bytes));
    }
    scripts
}

/// The workload set, in the study's shapes.
pub fn workloads(census: &ScriptCensus) -> Vec<Workload> {
    let ordered = census.by_length();
    let total = ordered.len();
    let mut out = Vec::new();

    for count in [20usize, 100] {
        let scripts = unused(census, count);
        out.push(Workload {
            name: format!("{count} unused scripts"),
            journal_events: 0,
            scripts,
        });
    }

    // "Small" is the short end of the distribution; "median" is its middle.
    // Both are ten scripts, as the study used, so a wallet's cost is measured
    // against a plausible number of active addresses rather than one.
    for (name, at) in [
        ("10 small histories", 0),
        ("10 median histories", total / 2),
    ] {
        let scripts = census.take(at, 10);
        out.push(Workload {
            name: name.to_string(),
            journal_events: census.events_of(&scripts),
            scripts,
        });
    }

    // The longest history in the range. This is the case the study found most
    // expensive, and the one most likely to break the comparison.
    if total > 0 {
        let scripts = census.take(total - 1, 1);
        out.push(Workload {
            name: "largest history".to_string(),
            journal_events: census.events_of(&scripts),
            scripts,
        });
    }

    // A plausible restoring wallet: a gap-limit window of derived scripts, most
    // of which are unused. This is the shape a real restoration actually has,
    // and none of the study's six shapes covers it.
    if total > 0 {
        let mut scripts = census.take(total / 2, 5);
        scripts.extend(unused(census, 95));
        out.push(Workload {
            name: "restoring wallet: 5 active, 95 unused".to_string(),
            journal_events: census.events_of(&scripts),
            scripts,
        });
    }

    out
}
