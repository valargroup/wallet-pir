//! Frozen, read-only-journal evaluation of experimental parent filters.
use anyhow::{ensure, Context, Result};
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::Write,
    path::{Path, PathBuf},
    time::Instant,
};
use transparent_filter::{
    experimental_parent::{self as parent, Manifest, Parent},
    *,
};
use transparent_regression::journal::EventStore;

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    /// Export experiment-only prior ledgers, checked against journal expectations.
    Seeds {
        #[arg(long)]
        data_dir: PathBuf,
        #[arg(long)]
        map: PathBuf,
        #[arg(long)]
        sample: PathBuf,
        #[arg(long)]
        out: PathBuf,
    },

    /// Extract complete script sets and require byte-identical child filters.
    Extract {
        #[arg(long)]
        data_dir: PathBuf,
        #[arg(long)]
        publication: PathBuf,
        #[arg(long)]
        sample: PathBuf,
        #[arg(long)]
        out: PathBuf,
    },
    /// Build the predeclared matrix; never reads a running chain/service.
    Sweep {
        #[arg(long)]
        dataset: PathBuf,
        #[arg(long)]
        out: PathBuf,
        #[arg(long, default_value_t = 100_000)]
        absent_count: usize,
    },
}
#[derive(Clone, Serialize, Deserialize)]
struct Wallet {
    class: String,
    scripts: Vec<String>,
    required_from: u64,
}
#[derive(Clone, Serialize, Deserialize)]
struct Sample {
    genesis_hash: String,
    anchor_height: u64,
    anchor_hash: String,
    clients: Vec<Wallet>,
}
#[derive(Clone, Serialize, Deserialize)]
struct Row {
    index: usize,
    profile: String,
    split: String,
    child_bytes: u64,
    parent_bytes: u64,
    metadata_bytes: u64,
    requests: u64,
    true_descents: u64,
    false_descents: u64,
    skipped_children: u64,
}
fn write_json(path: impl AsRef<Path>, v: &impl Serialize) -> Result<()> {
    fs::write(path, serde_json::to_vec_pretty(v)?)?;
    Ok(())
}
fn read_json<T: serde::de::DeserializeOwned>(path: impl AsRef<Path>) -> Result<T> {
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}
fn scripts(path: impl AsRef<Path>) -> Result<BTreeSet<Vec<u8>>> {
    let raw = fs::read(path)?;
    let mut rest = raw.as_slice();
    let mut result = BTreeSet::new();
    while !rest.is_empty() {
        ensure!(rest.len() >= 4, "truncated scripts");
        let n = u32::from_le_bytes(rest[..4].try_into()?) as usize;
        rest = &rest[4..];
        ensure!(rest.len() >= n, "truncated script");
        result.insert(rest[..n].to_vec());
        rest = &rest[n..];
    }
    Ok(result)
}
fn extract(data_dir: &Path, publication: &Path, sample_path: &Path, out: &Path) -> Result<()> {
    ensure!(!out.exists(), "output already exists");
    let map: ShardMap = read_json(publication.join("shards.json"))?;
    map.check_shape().map_err(anyhow::Error::msg)?;
    let raw_sample = fs::read(sample_path)?;
    let sample: Sample = serde_json::from_slice(&raw_sample)?;
    let journal = EventStore::open_existing(data_dir)?;
    ensure!(
        journal.genesis_hash() == map.genesis_hash && sample.genesis_hash == map.genesis_hash,
        "chain mismatch"
    );
    ensure!(
        journal
            .block_at(sample.anchor_height)
            .context("sample anchor unavailable")?
            .block_hash
            .to_display_hex()
            == sample.anchor_hash,
        "sample anchor mismatch"
    );
    ensure!(
        sample.anchor_height <= map.shards.last().context("empty map")?.end_height,
        "publication too short"
    );
    fs::create_dir_all(out.join("scripts"))?;
    fs::create_dir(out.join("children"))?;
    fs::write(out.join("sample.json"), &raw_sample)?;
    write_json(out.join("map.json"), &map)?;
    let genesis = BlockHash::from_display_hex(&map.genesis_hash)?;
    let mut sizes = Vec::new();
    let start = Instant::now();
    for c in &map.shards {
        ensure!(
            journal
                .block_at(c.end_height)
                .context("missing child endpoint")?
                .block_hash
                .to_display_hex()
                == c.terminal_block_hash,
            "child endpoint mismatch"
        );
        let mut set = BTreeSet::new();
        for h in c.start_height..=c.end_height {
            for (s, e) in journal.events_at(h)?.context("missing journal block")? {
                ensure!(u64::from(e.height()) == h, "event height mismatch");
                if s.is_filter_element() {
                    set.insert(s.as_slice().to_vec());
                }
            }
        }
        let owned: Vec<_> = set.iter().cloned().map(ScriptBytes::new).collect();
        let key = ShardKey::derive(
            &map.profile,
            genesis,
            c.shard_id,
            c.start_height,
            c.end_height,
            BlockHash::from_display_hex(&c.terminal_block_hash)?,
        );
        let bytes = build_range_filter(key, &owned)?.0;
        ensure!(
            filter_hash(&bytes).to_display_hex() == c.filter_hash,
            "reconstructed filter mismatch at {}",
            c.shard_id
        );
        let published = fs::read(publication.join(&c.manifest_digest).join("filter.bin"))?;
        ensure!(published == bytes, "publication bytes differ");
        fs::write(
            out.join("children").join(format!("{}.bin", c.shard_id)),
            &bytes,
        )?;
        let mut f = fs::File::create(out.join("scripts").join(format!("{}.bin", c.shard_id)))?;
        for s in &set {
            f.write_all(&(s.len() as u32).to_le_bytes())?;
            f.write_all(s)?;
        }
        sizes.push((c.manifest_digest.clone(), bytes.len() as u64));
        eprintln!(
            "verified shard {}: {} elements, {} bytes",
            c.shard_id,
            set.len(),
            bytes.len()
        );
    }
    write_json(out.join("child-bytes.json"), &sizes)?;
    write_json(
        out.join("provenance.json"),
        &json!({"schema":parent::SCHEMA,"map_sha256":parent::sha(&serde_json::to_vec(&map)?),"sample_sha256":parent::sha(&raw_sample),"journal":data_dir,"publication":publication,"journal_committed_through":journal.covered_through(),"seconds":start.elapsed().as_secs_f64(),"complete":true,"binary_sha256":parent::sha(&fs::read(std::env::current_exe()?)?),"command":std::env::args().collect::<Vec<_>>()}),
    )
}
fn split(sample: &Sample) -> Vec<String> {
    let mut groups: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
    for (i, w) in sample.clients.iter().enumerate() {
        groups.entry(&w.class).or_default().push(i);
    }
    let mut result = vec!["held-out".to_string(); sample.clients.len()];
    for ids in groups.values_mut() {
        ids.sort_by_key(|i| parent::sha(format!("parent-eval-split-v1:1:{i}").as_bytes()));
        for i in ids.iter().take(80) {
            result[*i] = "tuning".into();
        }
    }
    result
}
fn score(rows: &[Row]) -> Value {
    let weights: BTreeMap<&str, f64> = [
        ("catch-up-1d", 2.),
        ("catch-up-7d", 2.),
        ("catch-up-30d", 2.),
        ("unused", 2.),
        ("small-active", 4.),
        ("restore-6m", 3.),
        ("restore-old", 2.),
        ("multi-script", 2.),
        ("reused-tail", 1.),
    ]
    .into();
    let mut profiles = BTreeMap::new();
    for profile in weights.keys() {
        let r: Vec<_> = rows.iter().filter(|r| r.profile == *profile).collect();
        if r.is_empty() {
            continue;
        }
        let mean = r
            .iter()
            .map(|r| (r.child_bytes + r.parent_bytes + r.metadata_bytes) as f64)
            .sum::<f64>()
            / r.len() as f64;
        profiles.insert(*profile, mean);
    }
    json!({"profiles":profiles,"recent_mean": (["catch-up-1d","catch-up-7d","catch-up-30d"].iter().map(|p|profiles.get(p).copied().unwrap_or(0.)).sum::<f64>()/3.),"mixed_mean":profiles.iter().map(|(p,v)|v*weights[p]/20.).sum::<f64>()})
}
fn sweep(dataset: &Path, out: &Path, absent_count: usize) -> Result<()> {
    ensure!(!out.exists(), "output already exists");
    ensure!(absent_count > 0, "absent count must be positive");
    let provenance: Value = read_json(dataset.join("provenance.json"))?;
    ensure!(provenance["complete"] == true, "incomplete extraction");
    let map: ShardMap = read_json(dataset.join("map.json"))?;
    let mut sample: Sample = read_json(dataset.join("sample.json"))?;
    ensure!(
        parent::sha(&serde_json::to_vec(&map)?) == provenance["map_sha256"],
        "changed map"
    );
    ensure!(
        parent::sha(&fs::read(dataset.join("sample.json"))?) == provenance["sample_sha256"],
        "changed sample"
    );
    let sizes: Vec<(String, u64)> = read_json(dataset.join("child-bytes.json"))?;
    let size: BTreeMap<_, _> = sizes.iter().cloned().collect();
    let mut splits = split(&sample);
    // Sensitivity cases are never assigned product weights or used for tuning.
    let absent: Vec<String> = (0..1000)
        .map(|i| {
            let digest = parent::sha(format!("parent-eval-stress-v1:{i}").as_bytes());
            format!("76a914{}88ac", &digest[..40])
        })
        .collect();
    let mut stress = Vec::new();
    for n in [100, 1000] {
        for (label, from) in [
            ("recent", sample.anchor_height.saturating_sub(8063)),
            ("archive", 0),
        ] {
            stress.push(Wallet {
                class: format!("stress-{label}-{n}-absent"),
                scripts: absent[..n].to_vec(),
                required_from: from,
            });
        }
    }
    for blocks in [1, 48] {
        for class in ["unused", "catch-up-1d"] {
            let w = sample
                .clients
                .iter()
                .find(|w| w.class == class)
                .context("missing stress source")?;
            stress.push(Wallet {
                class: format!("stress-{blocks}-blocks-{class}"),
                scripts: w.scripts.clone(),
                required_from: sample.anchor_height.saturating_sub(blocks - 1),
            });
        }
    }
    let old: Vec<_> = sample
        .clients
        .iter()
        .filter(|w| w.class == "restore-old")
        .collect();
    stress.push(Wallet {
        class: "stress-archive-dispersed".into(),
        scripts: old
            .iter()
            .take(40)
            .flat_map(|w| w.scripts.iter().take(1).cloned())
            .collect(),
        required_from: 0,
    });
    stress.push(Wallet {
        class: "stress-archive-concentrated".into(),
        scripts: old.first().context("missing old wallet")?.scripts.clone(),
        required_from: 0,
    });
    splits.extend(stress.iter().map(|_| "stress".to_string()));
    sample.clients.extend(stress);

    let wallet_scripts: Vec<Vec<Vec<u8>>> = sample
        .clients
        .iter()
        .map(|w| {
            w.scripts
                .iter()
                .map(|s| hex::decode(s).map_err(anyhow::Error::from))
                .collect::<Result<_>>()
        })
        .collect::<Result<_>>()?;
    fs::create_dir_all(out.join("artifacts"))?;
    write_json(out.join("split.json"), &splits)?;
    write_json(out.join("evaluation-workload.json"), &sample)?;
    let mut baseline: Vec<Row> = sample
        .clients
        .iter()
        .enumerate()
        .map(|(i, w)| Row {
            index: i,
            profile: w.class.clone(),
            split: splits[i].clone(),
            child_bytes: 0,
            parent_bytes: 0,
            metadata_bytes: 0,
            requests: 0,
            true_descents: 0,
            false_descents: 0,
            skipped_children: 0,
        })
        .collect();
    for c in &map.shards {
        let bytes = fs::read(dataset.join("children").join(format!("{}.bin", c.shard_id)))?;
        ensure!(
            filter_hash(&bytes).to_display_hex() == c.filter_hash,
            "child changed"
        );
        ensure!(
            size[&c.manifest_digest] == bytes.len() as u64,
            "child length changed"
        );
        for (i, w) in sample.clients.iter().enumerate() {
            if c.end_height >= w.required_from && c.start_height <= sample.anchor_height {
                baseline[i].child_bytes += bytes.len() as u64;
                baseline[i].requests += 1;
            }
        }
    }
    write_json(out.join("baseline-wallets.json"), &baseline)?;
    let mut candidates = Vec::new();
    for (tier, ks) in [
        ("recent-8k", vec![1, 2, 4, 8, 16]),
        ("archive-wide", vec![1, 4, 8, 16, 32, 50, 80, 160]),
    ] {
        let children: Vec<_> = map
            .shards
            .iter()
            .filter(|c| c.geometry == tier && c.sealed)
            .cloned()
            .collect();
        for k in ks {
            let mut manifests: Vec<_> = [100, 1000, 10000]
                .iter()
                .map(|_| Manifest {
                    schema: parent::SCHEMA.into(),
                    map_sha256: parent::sha(&serde_json::to_vec(&map).unwrap()),
                    parents: Vec::new(),
                    child_bytes: sizes.clone(),
                })
                .collect();
            let mut rows = vec![baseline.clone(); 3];
            let mut group_metrics = vec![Vec::new(); 3];
            for group in children.chunks(k) {
                let mut union = BTreeSet::new();
                let mut entries = 0;
                for c in group {
                    let set = scripts(dataset.join("scripts").join(format!("{}.bin", c.shard_id)))?;
                    entries += set.len();
                    union.extend(set);
                }
                // Absence verified against the exact union. Fixed seed, no workload tuning.
                let absent: Vec<Vec<u8>> = (0..)
                    .map(|i| {
                        let h = parent::sha(format!("parent-eval-absent-v1:{i}").as_bytes());
                        let mut s = vec![0x76, 0xa9, 0x14];
                        s.extend(hex::decode(&h[..40]).unwrap());
                        s.extend([0x88, 0xac]);
                        s
                    })
                    .filter(|s| !union.contains(s))
                    .take(absent_count)
                    .collect();
                for (j, m) in [100, 1000, 10000].into_iter().enumerate() {
                    let started = Instant::now();
                    let mut p = Parent {
                        genesis_hash: map.genesis_hash.clone(),
                        profile: map.profile.clone(),
                        m,
                        p: parent::optimal_p(m),
                        children: group.to_vec(),
                        filter_hash: String::new(),
                        bytes: 0,
                        elements: 0,
                    };
                    let bytes = p.build(&union)?;
                    let validated = p.validate(&bytes)?;
                    // Full set check with sorted mapped values, including legitimate collisions.
                    let owned: Vec<_> = union.iter().cloned().map(ScriptBytes::new).collect();
                    let mapped = matching::map_wallet_scripts_keyed(&validated, p.keys()?, &owned);
                    ensure!(
                        mapped
                            .iter()
                            .map(|x| x.0)
                            .eq(validated.values().iter().copied()),
                        "parent omitted an element"
                    );
                    drop(mapped);
                    drop(owned);
                    fs::write(
                        out.join("artifacts").join(format!("{}.bin", p.filter_hash)),
                        &bytes,
                    )?;
                    let absent_owned: Vec<_> =
                        absent.iter().cloned().map(ScriptBytes::new).collect();
                    let false_hits =
                        matching::match_keyed(&validated, p.keys()?, &absent_owned)?.len();
                    for (i, w) in sample.clients.iter().enumerate() {
                        let relevant: Vec<_> = group
                            .iter()
                            .filter(|c| {
                                c.end_height >= w.required_from
                                    && c.start_height <= sample.anchor_height
                            })
                            .collect();
                        let child_cost: u64 =
                            relevant.iter().map(|c| size[&c.manifest_digest]).sum();
                        if p.bytes >= child_cost {
                            continue;
                        }
                        rows[j][i].parent_bytes += p.bytes;
                        rows[j][i].requests += 1;
                        let hit = p.matches(&validated, &wallet_scripts[i])?;
                        if hit {
                            if wallet_scripts[i].iter().any(|s| union.contains(s)) {
                                rows[j][i].true_descents += 1;
                            } else {
                                rows[j][i].false_descents += 1;
                            }
                        } else {
                            rows[j][i].child_bytes -= child_cost;
                            rows[j][i].requests -= relevant.len() as u64;
                            rows[j][i].skipped_children += relevant.len() as u64;
                        }
                    }
                    group_metrics[j].push(json!({"first":group[0].shard_id,"children":group.len(),"union_entries":union.len(),"child_entries":entries,"bytes":p.bytes,"absent_tests":absent_count,"false_hits":false_hits,"false_rate":false_hits as f64/absent_count as f64,"seconds":started.elapsed().as_secs_f64()}));
                    manifests[j].parents.push(p);
                }
            }
            for (j, m) in [100, 1000, 10000].into_iter().enumerate() {
                let id = format!("{tier}-k{k}-m{m}");
                let manifest = serde_json::to_vec(&manifests[j])?;
                // Manifest discovery is a real cold-client download, counted in full.
                for r in &mut rows[j] {
                    let w = &sample.clients[r.index];
                    if children.iter().any(|c| {
                        c.end_height >= w.required_from && c.start_height <= sample.anchor_height
                    }) {
                        r.metadata_bytes = manifest.len() as u64;
                        r.requests += 1;
                    }
                }
                fs::write(out.join(format!("{id}.json")), &manifest)?;
                write_json(out.join(format!("{id}-wallets.json")), &rows[j])?;
                let tuning: Vec<_> = rows[j]
                    .iter()
                    .filter(|r| r.split == "tuning")
                    .cloned()
                    .collect();
                let held: Vec<_> = rows[j]
                    .iter()
                    .filter(|r| r.split == "held-out")
                    .cloned()
                    .collect();
                candidates.push(json!({"id":id,"tier":tier,"k":k,"m":m,"p":parent::optimal_p(m),"manifest_bytes":manifest.len(),"tuning":score(&tuning),"held_out":score(&held),"groups":group_metrics[j]}));
                eprintln!("completed {id}");
            }
            write_json(out.join("progress.json"), &candidates)?;
        }
    }
    write_json(
        out.join("report.json"),
        &json!({"schema":parent::SCHEMA,"provenance":provenance,"baseline":score(&baseline),"candidates":candidates,"measurement":"exact encoded filter and metadata payloads; excludes PIR and transport overhead; HTTP validation required"}),
    )?;
    Ok(())
}
fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Seeds {
            data_dir,
            map,
            sample,
            out,
        } => export_seeds(&data_dir, &map, &sample, &out),
        Command::Extract {
            data_dir,
            publication,
            sample,
            out,
        } => extract(&data_dir, &publication, &sample, &out),
        Command::Sweep {
            dataset,
            out,
            absent_count,
        } => sweep(&dataset, &out, absent_count),
    }
}

fn export_seeds(data_dir: &Path, map_path: &Path, sample_path: &Path, out: &Path) -> Result<()> {
    use sha2::{Digest, Sha256};
    use transparent_events::TransparentEvent;
    use transparent_regression::{reference, EventRecord};
    #[derive(Deserialize)]
    struct Client {
        scripts: Vec<String>,
        required_from: u64,
        expected_digest: String,
        journal_events: u64,
    }
    ensure!(!out.exists(), "seed output exists");
    let raw = fs::read(sample_path)?;
    let value: Value = serde_json::from_slice(&raw)?;
    let clients: Vec<Client> = serde_json::from_value(value["clients"].clone())?;
    let map: ShardMap = read_json(map_path)?;
    map.check_shape().map_err(anyhow::Error::msg)?;
    let through = value["anchor_height"].as_u64().context("missing anchor")?;
    let anchor = json!({"height":through,"hash":value["anchor_hash"]});
    let journal = EventStore::open_existing(data_dir)?;
    ensure!(
        map.genesis_hash == journal.genesis_hash() && value["genesis_hash"] == map.genesis_hash,
        "seed chain mismatch"
    );
    ensure!(
        journal
            .block_at(through)
            .context("seed anchor missing")?
            .block_hash
            .to_display_hex()
            == value["anchor_hash"],
        "seed anchor mismatch"
    );
    for c in &map.shards {
        ensure!(
            journal
                .block_at(c.end_height)
                .context("missing shard endpoint")?
                .block_hash
                .to_display_hex()
                == c.terminal_block_hash,
            "seed map endpoint mismatch"
        );
    }
    let mut wanted: std::collections::HashMap<Vec<u8>, Vec<TransparentEvent>> =
        std::collections::HashMap::new();
    for w in &clients {
        for s in &w.scripts {
            wanted.entry(hex::decode(s)?).or_default();
        }
    }
    for height in map.start_height..=through {
        for (s, e) in journal.events_at(height)?.context("missing seed block")? {
            ensure!(
                u64::from(e.height()) == height,
                "seed event height mismatch"
            );
            if let Some(events) = wanted.get_mut(s.as_slice()) {
                events.push(e);
            }
        }
        if height % 100_000 == 0 {
            eprintln!("seed journal height {height}");
        }
    }
    fs::create_dir(out)?;
    let map_sha = parent::sha(&serde_json::to_vec(&map)?);
    let mut hashes = BTreeMap::new();
    for (i, w) in clients.iter().enumerate() {
        let mut window = Vec::new();
        let mut prior = Vec::new();
        for script in &w.scripts {
            for e in &wanted[&hex::decode(script)?] {
                if u64::from(e.height()) < w.required_from {
                    prior.push(EventRecord {
                        script: script.clone(),
                        event: hex::encode(e.to_bytes()),
                    });
                } else {
                    window.push(*e);
                }
            }
        }
        window.sort_by_key(TransparentEvent::sort_key);
        window.dedup();
        let mut digest = Sha256::new();
        for e in &window {
            digest.update(e.to_bytes());
        }
        ensure!(
            hex::encode(digest.finalize()) == w.expected_digest
                && window.len() as u64 == w.journal_events,
            "journal oracle mismatch for seed {i}"
        );
        prior.sort();
        prior.dedup();
        reference(&prior, w.required_from.saturating_sub(1))?;
        let seed = json!({"map_sha256":map_sha,"genesis_hash":map.genesis_hash,"anchor":anchor,"events":prior});
        let envelope = json!({"seed":seed,"scripts":w.scripts,"required_from":w.required_from,"actual_digest":w.expected_digest,"events":w.journal_events,"seed_sha256":parent::sha(&serde_json::to_vec(&seed)?)});
        let bytes = serde_json::to_vec(&envelope)?;
        hashes.insert(i.to_string(), parent::sha(&bytes));
        fs::write(out.join(format!("{i}.json")), bytes)?;
    }
    write_json(
        out.join("manifest.json"),
        &json!({"schema":"transparent-parent-journal-seeds-v1","sample_sha256":parent::sha(&raw),"map_sha256":map_sha,"anchor":anchor,"files":hashes,"source":"read-only journal; sample digests and independent prior-ledger oracle verified","binary_sha256":parent::sha(&fs::read(std::env::current_exe()?)?)}),
    )
}

#[cfg(test)]
mod seed_tests {
    use super::*;
    use transparent_events::{ReceiveEvent, SpendEvent, TransparentEvent, Txid};
    #[test]
    fn journal_seeds_verify_window_and_preserve_only_prior_events() {
        let dir = tempfile::tempdir().unwrap();
        let journal = dir.path().join("journal");
        fs::create_dir(&journal).unwrap();
        let script = vec![0x51];
        let genesis = BlockHash([1; 32]).to_display_hex();
        let end = BlockHash([2; 32]).to_display_hex();
        let receive = TransparentEvent::Receive(ReceiveEvent {
            height: 0,
            txid: Txid([3; 32]),
            transaction_index: 0,
            output_index: 0,
            value: 7,
            coinbase: false,
        });
        let spend = TransparentEvent::Spend(SpendEvent {
            height: 1,
            spending_txid: Txid([4; 32]),
            transaction_index: 0,
            input_index: 0,
            spent_txid: Txid([3; 32]),
            spent_output_index: 0,
        });
        let mut events = Vec::new();
        let mut blocks = Vec::new();
        for (i, event) in [receive, spend].iter().enumerate() {
            blocks.extend([i as u8 + 1; 32]);
            blocks.extend((events.len() as u64).to_le_bytes());
            blocks.extend(1u64.to_le_bytes());
            events.extend(1u16.to_le_bytes());
            events.extend(&script);
            events.extend(event.to_bytes());
        }
        fs::write(journal.join("events.bin"), &events).unwrap();
        fs::write(journal.join("blocks.bin"), &blocks).unwrap();
        let mut checkpoint = (events.len() as u64).to_le_bytes().to_vec();
        checkpoint.extend((blocks.len() as u64).to_le_bytes());
        fs::write(journal.join("checkpoint.bin"), checkpoint).unwrap();
        write_json(
            journal.join("meta.json"),
            &json!({"version":2,"genesis_hash":genesis,"start_height":0}),
        )
        .unwrap();
        let child = ShardMapEntry {
            shard_id: 0,
            geometry: "recent-8k".into(),
            start_height: 0,
            end_height: 1,
            parent_block_hash: "00".repeat(32),
            terminal_block_hash: end.clone(),
            filter_hash: "11".repeat(32),
            scripts: 1,
            page_rows: 1,
            txids: 2,
            directory_segments: 1,
            page_segments: 1,
            manifest_digest: "22".repeat(32),
            revision: 0,
            sealed: false,
        };
        let map = ShardMap {
            genesis_hash: genesis.clone(),
            network: "main".into(),
            profile: RANGE_PROFILE.into(),
            range_envelope_version: 1,
            start_height: 0,
            seal: [(
                "recent-8k".into(),
                SealParameters {
                    max_scripts: 10,
                    max_page_rows: 10,
                    max_txids: 0,
                },
            )]
            .into(),
            shards: vec![child],
        };
        let map_path = dir.path().join("map.json");
        write_json(&map_path, &map).unwrap();
        let sample = dir.path().join("sample.json");
        let mut value = json!({"genesis_hash":genesis,"anchor_height":1,"anchor_hash":end,"clients":[{"scripts":["51"],"required_from":1,"expected_digest":parent::sha(&spend.to_bytes()),"journal_events":1}]});
        write_json(&sample, &value).unwrap();
        let out = dir.path().join("seeds");
        export_seeds(&journal, &map_path, &sample, &out).unwrap();
        let seed: Value = read_json(out.join("0.json")).unwrap();
        assert_eq!(seed["seed"]["events"].as_array().unwrap().len(), 1);
        assert_eq!(
            seed["seed"]["events"][0]["event"],
            hex::encode(receive.to_bytes())
        );
        value["clients"][0]["expected_digest"] = json!("bad");
        write_json(&sample, &value).unwrap();
        let bad = dir.path().join("bad");
        assert!(export_seeds(&journal, &map_path, &sample, &bad).is_err());
        assert!(!bad.join("manifest.json").exists());
    }
}
