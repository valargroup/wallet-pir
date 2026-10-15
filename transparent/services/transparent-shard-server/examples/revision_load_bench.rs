//! Paired before/after measurement of loading a republished tail.
//!
//! `revision_load_bench DIR LONG_RUNS REPEATS` builds one directory shard whose
//! first page shard is sealed and spans several segments, publishes revision zero,
//! then revision one beside it with the sealed page shard hard-linked, as the
//! publisher does, into a second publication directory. It loads the first cold and
//! then the second reusing the first, as a worker's prepare does, `REPEATS` times,
//! and prints, per load: wall time, this process's CPU time, bytes read (rchar) and
//! peak RSS, all from /proc so the same file measures any revision of the loader.
use std::path::Path;
use std::time::{Duration, Instant, SystemTime};
use transparent_events::{ReceiveEvent, TransparentEvent, Txid};
use transparent_filter::{BlockHash, ScriptBytes, SealParameters, ShardMap, ShardMapEntry};
#[path = "../tests/support/revision.rs"]
mod revision;
use transparent_shard::build::build_directory_shard;
use transparent_shard::layout::DECOUPLED_2K_8K;
use transparent_shard::manifest::pages_table;
use transparent_shard_server::shardset::{LoadOptions, ShardSet, DEFAULT_RETAIN_REVISIONS};

const FIRST: u64 = 3_428_143;
const RUN: u64 = transparent_shard::layout::EVENTS_PER_PAGE as u64 + 1;

fn hash_at(height: u64) -> BlockHash {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&height.to_le_bytes());
    BlockHash::from_internal_bytes(bytes)
}

fn script(tag: u32) -> ScriptBytes {
    let mut bytes = vec![0x76, 0xa9, 0x14];
    bytes.extend_from_slice(&tag.to_le_bytes());
    bytes.extend_from_slice(&[0u8; 16]);
    bytes.extend_from_slice(&[0x88, 0xac]);
    ScriptBytes::new(bytes)
}

fn events(last: u64, long_runs: u64) -> Vec<(ScriptBytes, TransparentEvent)> {
    let mut events = Vec::new();
    for height in FIRST..=last {
        let mut push = |tag: u32, n: u64| {
            let mut txid = [0u8; 32];
            txid[..8].copy_from_slice(&height.to_le_bytes());
            txid[8..16].copy_from_slice(&n.to_le_bytes());
            events.push((
                script(tag),
                TransparentEvent::Receive(ReceiveEvent {
                    metadata: None,
                    height: height as u32,
                    txid: Txid(txid),
                    transaction_index: n as u16,
                    output_index: 0,
                    value: 1_000 + n,
                    coinbase: false,
                }),
            ));
        };
        for n in 0..8 {
            push(n as u32, n);
        }
        if height < FIRST + RUN {
            for k in 0..long_runs {
                push(1_000 + k as u32, 8 + k);
            }
        }
    }
    events
}

fn write(
    dir: &Path,
    end: u64,
    ends: &[u64],
    revision: u32,
    supersedes: &str,
    long_runs: u64,
    link: Option<&Path>,
) -> ShardMapEntry {
    let genesis = BlockHash::from_display_hex(transparent_filter::MAINNET_GENESIS_DISPLAY).unwrap();
    let spec = revision::Revision {
        shard_id: 0,
        start: FIRST,
        end,
        geometry: &DECOUPLED_2K_8K,
        sealed: false,
        revision,
        supersedes,
        parent_digest: "",
        children: ends.len() as u32,
        genesis,
        hash: &hash_at,
    };
    let all = events(end, long_runs);
    let blocks = (FIRST..=end)
        .map(|h| {
            (
                h,
                all.iter()
                    .filter(|(_, e)| u64::from(e.height()) == h)
                    .cloned()
                    .collect(),
            )
        })
        .collect();
    let (built, children) = build_directory_shard(
        0,
        &DECOUPLED_2K_8K,
        genesis,
        transparent_filter::RANGE_PROFILE,
        blocks,
        ends,
        &hash_at,
        false,
    )
    .unwrap();
    let (manifest, entry) = revision::write_built(dir, &spec, &built, &children);
    if let Some(from) = link {
        let old: transparent_shard::manifest::ShardManifest =
            serde_json::from_slice(&std::fs::read(from.join("manifest.json")).unwrap()).unwrap();
        for (child, published) in manifest.page_children.iter().enumerate() {
            if old.page_children.get(child) != Some(published) {
                continue;
            }
            for segment in 0..published.page_segments.len() {
                let name = format!("{}.{segment}.bin", pages_table(child as u32));
                let target = dir.join(&entry.manifest_digest).join(&name);
                std::fs::remove_file(&target).unwrap();
                std::fs::hard_link(from.join(&name), &target).unwrap();
            }
        }
    }
    entry
}

fn map(dir: &Path, entry: &ShardMapEntry) {
    let map = ShardMap {
        genesis_hash: transparent_filter::MAINNET_GENESIS_DISPLAY.to_string(),
        network: transparent_filter::NETWORK.to_string(),
        profile: transparent_filter::RANGE_PROFILE.to_string(),
        range_envelope_version: transparent_filter::RANGE_ENVELOPE_VERSION,
        start_height: FIRST,
        seal: std::collections::BTreeMap::from([(
            DECOUPLED_2K_8K.name.to_string(),
            SealParameters {
                max_scripts: 8_192,
                max_page_rows: 2_048,
                max_txids: 0,
            },
        )]),
        shards: vec![entry.clone()],
        recuts: Vec::new(),
    };
    std::fs::write(
        dir.join("shards.json"),
        serde_json::to_vec_pretty(&map).unwrap(),
    )
    .unwrap();
}

fn link_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        std::fs::hard_link(entry.path(), to.join(entry.file_name())).unwrap();
    }
}

fn age(dir: &Path) {
    let past = SystemTime::now() - Duration::from_secs(120);
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            age(&path);
        } else {
            std::fs::File::options()
                .write(true)
                .open(&path)
                .unwrap()
                .set_modified(past)
                .unwrap();
        }
    }
}

/// (user+system CPU in clock ticks, rchar bytes, peak RSS kB) of this process.
fn sample() -> (u64, u64, u64) {
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap();
    let fields: Vec<&str> = stat
        .rsplit(')')
        .next()
        .unwrap()
        .split_whitespace()
        .collect();
    let cpu = fields[11].parse::<u64>().unwrap() + fields[12].parse::<u64>().unwrap();
    let io = std::fs::read_to_string("/proc/self/io").unwrap();
    let rchar = io
        .lines()
        .find_map(|l| l.strip_prefix("rchar: "))
        .unwrap()
        .parse()
        .unwrap();
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    let hwm = status
        .lines()
        .find_map(|l| l.strip_prefix("VmHWM:"))
        .unwrap()
        .trim()
        .trim_end_matches(" kB")
        .parse()
        .unwrap();
    (cpu, rchar, hwm)
}

fn measure<T>(f: impl FnOnce() -> T) -> (T, serde_json::Value) {
    // Reset the peak RSS mark, so it reports this load rather than the fixture.
    let _ = std::fs::write("/proc/self/clear_refs", "5");
    let (cpu, rchar, _) = sample();
    let started = Instant::now();
    let value = f();
    let wall = started.elapsed();
    let (cpu2, rchar2, hwm) = sample();
    let ticks = 100.0; // USER_HZ on Linux
    (
        value,
        serde_json::json!({"wall_ms": wall.as_secs_f64() * 1e3,
        "cpu_ms": (cpu2 - cpu) as f64 * 1e3 / ticks, "read_bytes": rchar2 - rchar, "peak_rss_kb": hwm}),
    )
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let root = Path::new(&args[1]);
    let long_runs: u64 = args[2].parse().unwrap();
    let repeats: usize = args[3].parse().unwrap();
    let store = root.join("store");
    let r0 = write(
        &store,
        FIRST + 149,
        &[FIRST + 99, FIRST + 149],
        0,
        "",
        long_runs,
        None,
    );
    let r1 = write(
        &store,
        FIRST + 199,
        &[FIRST + 99, FIRST + 149, FIRST + 199],
        1,
        &r0.manifest_digest,
        long_runs,
        Some(&store.join(&r0.manifest_digest)),
    );
    age(&store);
    let (p1, p2) = (root.join("p1"), root.join("p2"));
    link_dir(
        &store.join(&r0.manifest_digest),
        &p1.join(&r0.manifest_digest),
    );
    map(&p1, &r0);
    for entry in [&r0, &r1] {
        link_dir(
            &store.join(&entry.manifest_digest),
            &p2.join(&entry.manifest_digest),
        );
    }
    map(&p2, &r1);
    let table_bytes: u64 = walk_bytes(&store.join(&r1.manifest_digest));
    let options = LoadOptions::whole(DEFAULT_RETAIN_REVISIONS);
    for repeat in 0..repeats {
        let (first, cold) = measure(|| ShardSet::open_reusing(&p1, &options, None).unwrap());
        let (_second, next) =
            measure(|| ShardSet::open_reusing(&p2, &options, Some(&first)).unwrap());
        println!(
            "{}",
            serde_json::json!({"repeat": repeat, "long_runs": long_runs,
            "revision_one_table_bytes": table_bytes, "cold_first_publication": cold,
            "republished_tail_reusing_previous": next})
        );
    }
}

fn walk_bytes(dir: &Path) -> u64 {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().metadata().unwrap().len())
        .sum()
}
