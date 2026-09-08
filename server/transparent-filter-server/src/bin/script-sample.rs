//! Draws a workload sample from the journal, with the answer each client
//! should reach.
//!
//! A load test that cannot say whether its clients recovered the right
//! history measures throughput of something. This tool reads the journal at
//! the pinned anchor and emits, per workload class, synthetic wallets made of
//! real on-chain scripts, each with the height its sync starts from and a
//! digest of exactly the events it should recover over that range. The load
//! harness recomputes the digest from what a client's store holds and counts
//! a sync as correct only when they agree.
//!
//! # Classes
//!
//! - `unused`: scripts that appear nowhere in the journal.
//! - `small-active`: scripts with a few events anywhere.
//! - `catch-up-1d`, `catch-up-7d`, `catch-up-30d`: wallets already synced to
//!   1,152, 8,064 or 34,560 blocks below the anchor that resume from there.
//! - `restore-6m`: wallets whose activity lies entirely in the recent tier,
//!   restored from the cutoff.
//! - `restore-old`: wallets with activity below the cutoff, restored from
//!   the set's start.
//! - `multi-script`: wallets of many active scripts.
//! - `reused-tail`: the scripts with the most events in the last shards, one
//!   per wallet.
//!
//! # Privacy
//!
//! Every script here is a public on-chain script; the groupings are synthetic
//! and describe no real wallet. The file must not be published beside a claim
//! that it is a user population.

use clap::Parser;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use transparent_events::TransparentEvent;
use transparent_filter_server::events::EventStore;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

const DAY_BLOCKS: u64 = 1_152;

#[derive(Parser)]
#[command(
    name = "script-sample",
    about = "Draw a load-test workload with expected results from the journal"
)]
struct Cli {
    #[arg(long, default_value = "./transparent-event-data")]
    data_dir: PathBuf,
    /// The pinned anchor; the journal must be committed through it.
    #[arg(long)]
    anchor_height: u64,
    /// The recorded cutoff height.
    #[arg(long)]
    cutoff_height: u64,
    /// The set's first height.
    #[arg(long, default_value_t = 0)]
    start_height: u64,
    #[arg(long, default_value_t = 1)]
    seed: u64,
    /// Wallets per class.
    #[arg(long, default_value_t = 100)]
    per_class: usize,
    /// Scripts per multi-script wallet.
    #[arg(long, default_value_t = 40)]
    multi_scripts: usize,
    #[arg(long)]
    out: PathBuf,
    #[arg(long)]
    source_sha: Option<String>,
}

struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 11
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

/// A synthetic P2PKH script that no chain activity should name.
fn unused_script(lcg: &mut Lcg) -> Vec<u8> {
    let mut bytes = vec![0x76, 0xa9, 0x14];
    for _ in 0..20 {
        bytes.push((lcg.next() & 0xff) as u8);
    }
    bytes.extend_from_slice(&[0x88, 0xac]);
    bytes
}

/// Bounded reservoir of scripts seen in a height window.
fn reservoir(
    store: &EventStore,
    from: u64,
    through: u64,
    lcg: &mut Lcg,
    want: usize,
) -> Result<Vec<Vec<u8>>, BoxError> {
    let mut seen: HashSet<Vec<u8>> = HashSet::new();
    let mut picked: Vec<Vec<u8>> = Vec::new();
    let mut count = 0u64;
    for height in from..=through {
        for (script, _) in store.events_at(height)?.unwrap_or_default() {
            if script.as_slice().len() > 40 || !seen.insert(script.as_slice().to_vec()) {
                continue;
            }
            count += 1;
            if picked.len() < want {
                picked.push(script.as_slice().to_vec());
            } else {
                let slot = lcg.below(count);
                if (slot as usize) < want {
                    picked[slot as usize] = script.as_slice().to_vec();
                }
            }
        }
    }
    Ok(picked)
}

/// Event counts per script over a window, for the reused tail.
fn counts(store: &EventStore, from: u64, through: u64) -> Result<HashMap<Vec<u8>, u64>, BoxError> {
    let mut counts: HashMap<Vec<u8>, u64> = HashMap::new();
    for height in from..=through {
        for (script, _) in store.events_at(height)?.unwrap_or_default() {
            if script.as_slice().len() > 40 {
                continue;
            }
            *counts.entry(script.as_slice().to_vec()).or_default() += 1;
        }
    }
    Ok(counts)
}

/// What one candidate script did over the whole published range.
#[derive(Default)]
struct Activity {
    first: Option<u64>,
    last: Option<u64>,
    events: Vec<TransparentEvent>,
}

/// Digest of the events a wallet of `scripts` should recover over
/// `[from, through]`: the canonical bytes of those events in canonical order.
fn expected_digest(
    activity: &HashMap<Vec<u8>, Activity>,
    scripts: &[Vec<u8>],
    from: u64,
    through: u64,
) -> (String, u64) {
    let mut events: Vec<TransparentEvent> = Vec::new();
    for script in scripts {
        if let Some(seen) = activity.get(script) {
            for event in &seen.events {
                let height = match event {
                    TransparentEvent::Receive(r) => u64::from(r.height),
                    TransparentEvent::Spend(s) => u64::from(s.height),
                };
                if height >= from && height <= through {
                    events.push(*event);
                }
            }
        }
    }
    events.sort_by_key(|event| event.sort_key());
    events.dedup();
    let mut hasher = Sha256::new();
    for event in &events {
        hasher.update(event.to_bytes());
    }
    (hex::encode(hasher.finalize()), events.len() as u64)
}

fn main() -> Result<(), BoxError> {
    let cli = Cli::parse();
    let store = EventStore::open_existing(&cli.data_dir)?;
    let covered = store.covered_through().ok_or("the journal is empty")?;
    if cli.anchor_height > covered || cli.cutoff_height >= cli.anchor_height {
        return Err(format!(
            "anchor {} and cutoff {} must lie within the journal's {}-{covered} and in order",
            cli.anchor_height, cli.cutoff_height, cli.start_height
        )
        .into());
    }
    let anchor = cli.anchor_height;
    let cutoff = cli.cutoff_height;
    let start = cli.start_height.max(store.start_height());
    let mut lcg = Lcg(cli.seed ^ 0x9E37_79B9_7F4A_7C15);
    let n = cli.per_class;

    // Candidate draws from windows, before the one full pass that
    // characterises each candidate.
    eprintln!("drawing candidates");
    let day = reservoir(&store, anchor - DAY_BLOCKS + 1, anchor, &mut lcg, n * 2)?;
    let week = reservoir(&store, anchor - 7 * DAY_BLOCKS + 1, anchor, &mut lcg, n * 2)?;
    let month = reservoir(
        &store,
        anchor - 30 * DAY_BLOCKS + 1,
        anchor,
        &mut lcg,
        n * 2,
    )?;
    let recent = reservoir(
        &store,
        cutoff,
        anchor,
        &mut lcg,
        n * 6 + cli.multi_scripts * n,
    )?;
    let archive_lo = reservoir(
        &store,
        start,
        start + 200_000.min(cutoff - start),
        &mut lcg,
        n,
    )?;
    let archive_mid = reservoir(
        &store,
        cutoff.saturating_sub(400_000).max(start),
        cutoff - 1,
        &mut lcg,
        n * 2,
    )?;
    let tail_counts = counts(&store, anchor.saturating_sub(20_000).max(cutoff), anchor)?;
    let mut reused: Vec<(Vec<u8>, u64)> = tail_counts.into_iter().collect();
    reused.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    reused.truncate(n);
    let unused: Vec<Vec<u8>> = (0..n * 4).map(|_| unused_script(&mut lcg)).collect();

    let mut candidates: HashSet<Vec<u8>> = HashSet::new();
    for group in [
        &day,
        &week,
        &month,
        &recent,
        &archive_lo,
        &archive_mid,
        &unused,
    ] {
        candidates.extend(group.iter().cloned());
    }
    candidates.extend(reused.iter().map(|(s, _)| s.clone()));
    eprintln!(
        "{} candidates; one pass over {start}-{anchor}",
        candidates.len()
    );

    let mut activity: HashMap<Vec<u8>, Activity> = HashMap::new();
    let started = std::time::Instant::now();
    for height in start..=anchor {
        for (script, event) in store.events_at(height)?.unwrap_or_default() {
            let key = script.as_slice();
            if !candidates.contains(key) {
                continue;
            }
            let entry = activity.entry(key.to_vec()).or_default();
            entry.first.get_or_insert(height);
            entry.last = Some(height);
            entry.events.push(event);
        }
        if height % 500_000 == 0 && height > start {
            eprintln!("  {height} ({:.0}s)", started.elapsed().as_secs_f64());
        }
    }
    eprintln!("pass done in {:.0}s", started.elapsed().as_secs_f64());

    let active = |s: &Vec<u8>| activity.get(s).is_some_and(|a| !a.events.is_empty());
    let first_of = |s: &Vec<u8>| activity.get(s).and_then(|a| a.first);

    let mut clients: Vec<serde_json::Value> = Vec::new();
    let mut push = |class: &str, scripts: Vec<Vec<u8>>, from: u64| {
        if scripts.is_empty() {
            return;
        }
        let (digest, events) = expected_digest(&activity, &scripts, from, anchor);
        clients.push(serde_json::json!({
            "class": class,
            "scripts": scripts.iter().map(hex::encode).collect::<Vec<_>>(),
            "required_from": from,
            "expected_range": [from, anchor],
            "expected_digest": digest,
            "journal_events": events,
        }));
    };

    // Unused: verified absent by the pass.
    let confirmed_unused: Vec<Vec<u8>> = unused.iter().filter(|s| !active(s)).cloned().collect();
    for chunk in confirmed_unused.chunks(4).take(n) {
        push("unused", chunk.to_vec(), start);
    }
    // Small active: a few events, drawn from the recent reservoir.
    let small: Vec<Vec<u8>> = recent
        .iter()
        .filter(|s| {
            activity
                .get(*s)
                .is_some_and(|a| (1..=4).contains(&a.events.len()))
        })
        .cloned()
        .collect();
    for script in small.iter().take(n) {
        push(
            "small-active",
            vec![script.clone()],
            first_of(script).unwrap_or(start),
        );
    }
    // Catch-ups: a synced wallet resuming k blocks below the anchor. Two
    // active scripts from the window plus two unused, as a wallet's gap
    // limit leaves it.
    let mut unused_iter = confirmed_unused.iter().cycle();
    for (class, pool, k) in [
        ("catch-up-1d", &day, DAY_BLOCKS),
        ("catch-up-7d", &week, 7 * DAY_BLOCKS),
        ("catch-up-30d", &month, 30 * DAY_BLOCKS),
    ] {
        for pair in pool.chunks(2).take(n) {
            let mut scripts = pair.to_vec();
            scripts.push(unused_iter.next().cloned().unwrap_or_default());
            scripts.push(unused_iter.next().cloned().unwrap_or_default());
            scripts.retain(|s| !s.is_empty());
            push(class, scripts, anchor - k + 1);
        }
    }
    // Six-month restore: activity entirely at or above the cutoff.
    let recent_only: Vec<Vec<u8>> = recent
        .iter()
        .filter(|s| first_of(s).is_some_and(|h| h >= cutoff))
        .cloned()
        .collect();
    for group in recent_only.chunks(5).take(n) {
        let mut scripts = group.to_vec();
        for _ in 0..5 {
            if let Some(u) = unused_iter.next() {
                scripts.push(u.clone());
            }
        }
        push("restore-6m", scripts, cutoff);
    }
    // Old birthday: activity below the cutoff, restored from the start.
    let old: Vec<Vec<u8>> = archive_lo
        .iter()
        .chain(archive_mid.iter())
        .filter(|s| first_of(s).is_some_and(|h| h < cutoff))
        .cloned()
        .collect();
    for group in old.chunks(3).take(n) {
        push("restore-old", group.to_vec(), start);
    }
    // Multi-script: many active recent scripts in one wallet.
    let multi_pool: Vec<Vec<u8>> = recent.iter().filter(|s| active(s)).cloned().collect();
    for group in multi_pool.chunks(cli.multi_scripts).take(n) {
        if group.len() < cli.multi_scripts / 2 {
            break;
        }
        push("multi-script", group.to_vec(), cutoff);
    }
    // Reused tail: the busiest scripts of the last shards, one each.
    for (script, _) in reused.iter().take(n) {
        push(
            "reused-tail",
            vec![script.clone()],
            first_of(script).unwrap_or(cutoff),
        );
    }

    let mut by_class: BTreeMap<&str, usize> = BTreeMap::new();
    for client in &clients {
        *by_class
            .entry(client["class"].as_str().unwrap_or(""))
            .or_default() += 1;
    }
    let record = serde_json::json!({
        "schema": "transparent-script-sample-v1",
        "generated_at": chrono::Utc::now().to_rfc3339(),
        "tool_sha": cli.source_sha,
        "data_dir": cli.data_dir,
        "genesis_hash": store.genesis_hash(),
        "start_height": start,
        "cutoff_height": cutoff,
        "anchor_height": anchor,
        "anchor_hash": store.block_at(anchor).map(|e| e.block_hash.to_display_hex()),
        "seed": cli.seed,
        "per_class": n,
        "provenance": "public on-chain scripts grouped synthetically; describes no real wallet",
        "expected_digest": "sha256 over the canonical 96-byte records of the wallet's events in [required_from, anchor], canonical order, deduplicated",
        "classes": by_class,
        "clients": clients,
    });
    std::fs::write(&cli.out, serde_json::to_vec_pretty(&record)?)?;
    eprintln!(
        "{} clients written to {}",
        record["clients"].as_array().map_or(0, |c| c.len()),
        cli.out.display()
    );
    Ok(())
}
