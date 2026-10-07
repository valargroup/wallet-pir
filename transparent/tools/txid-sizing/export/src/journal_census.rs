//! Offline census of immutable, checkpoint-referenced display sidecars.
//! No RPC, parents, UTXO reconstruction, service dependency or journal writes.
use super::{display_journal, events::EventStore, AnyError};
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::Path,
    time::Instant,
};
use transparent_events::{encode_varint, FeeState, MAX_MONEY};
use transparent_shard::txid::{candidate_rows, TransparentDisplayRecord, CODEC};

const THRESHOLDS: [usize; 7] = [128, 192, 256, 384, 512, 768, 1024];
const ARCHIVE: usize = 40_000;
const ROW: usize = 4096;
const FRAGMENT: usize = ROW - 4 - 2 - 40;
const ERAS: [(u64, &str); 11] = [
    (0, "Sprout"),
    (347500, "Overwinter"),
    (419200, "Sapling"),
    (653600, "Blossom"),
    (903000, "Heartwood"),
    (1046400, "Canopy"),
    (1687104, "NU5"),
    (2726400, "NU6"),
    (3146400, "NU6.1"),
    (3364600, "NU6.2"),
    (3428143, "NU6.3"),
];
type Hist = BTreeMap<usize, u64>;
fn era(height: u64) -> &'static str {
    ERAS.iter().rev().find(|(h, _)| height >= *h).unwrap().1
}
fn varlen(value: u64) -> usize {
    let mut b = Vec::new();
    encode_varint(value, &mut b);
    b.len()
}
fn size_bounds(record: &TransparentDisplayRecord) -> Result<[usize; 3], AnyError> {
    let actual = record.encode()?.len();
    Ok(match record.metadata.fee {
        // These are a hypothetical exact-fee population, not the stored bytes.
        FeeState::Unknown => [actual, actual + varlen(0), actual + varlen(MAX_MONEY)],
        _ => [actual, actual, actual],
    })
}
fn quantile(h: &Hist, numerator: u64, denominator: u64) -> Option<usize> {
    let n: u64 = h.values().sum();
    if n == 0 {
        return None;
    }
    let target = (n * numerator).div_ceil(denominator);
    let mut at = 0;
    h.iter().find_map(|(&s, &v)| {
        at += v;
        (at >= target).then_some(s)
    })
}
fn histogram_report(h: &Hist) -> Value {
    json!({"count":h.values().sum::<u64>(), "min":h.first_key_value().map(|(k,_)|k),
        "p50":quantile(h,50,100), "p99":quantile(h,99,100),
        "max":h.last_key_value().map(|(k,_)|k), "histogram":h.iter().map(|(&s,&n)|[s as u64,n]).collect::<Vec<_>>()})
}

#[derive(Default)]
struct Domain {
    records: u64,
    outputs: u64,
    inputs: u64,
    shielded: u64,
    input_only: u64,
    fees: [u64; 4], // unknown, exact zero, exact nonzero, nonapplicable
    hist: [Hist; 3],
}
impl Domain {
    fn add(&mut self, r: &TransparentDisplayRecord, sizes: [usize; 3]) {
        self.records += 1;
        self.outputs += r.outputs.len() as u64;
        self.inputs += r.metadata.transparent_input_count as u64;
        self.shielded += u64::from(r.metadata.has_shielded_components);
        self.input_only +=
            u64::from(r.outputs.is_empty() && r.metadata.transparent_input_count > 0);
        self.fees[match r.metadata.fee {
            FeeState::Unknown => 0,
            FeeState::Exact(0) => 1,
            FeeState::Exact(_) => 2,
            FeeState::NotApplicable => 3,
        }] += 1;
        for (i, size) in sizes.into_iter().enumerate() {
            *self.hist[i].entry(size).or_default() += 1;
        }
    }
    fn report(&self) -> Value {
        let mut thresholds = BTreeMap::new();
        for t in THRESHOLDS {
            let mut rows = Vec::new();
            for h in &self.hist {
                let inline: u64 = h.range(..=t).map(|(_, n)| n).sum();
                let fragments: u64 = h
                    .range(t + 1..)
                    .map(|(s, n)| s.div_ceil(FRAGMENT) as u64 * n)
                    .sum();
                let inline_bytes: u64 = h.range(..=t).map(|(s, n)| *s as u64 * n).sum();
                let overflow_bytes: u64 = h
                    .range(t + 1..)
                    .map(|(s, n)| (*s as u64 + 42 * s.div_ceil(FRAGMENT) as u64) * n)
                    .sum();
                rows.push(json!({"inline":inline,"overflow":self.records-inline,
                    "coverage":(self.records>0).then(||inline as f64/self.records as f64),
                    "fragments":fragments,"directory_entry_bytes":48*self.records+inline_bytes,
                    "overflow_entry_bytes":overflow_bytes}));
            }
            thresholds.insert(t.to_string(), json!({"stored":rows[0],"exact_fee_lower_size":rows[1],"exact_fee_upper_size":rows[2]}));
        }
        let frontiers: BTreeMap<_,_> = [85,90,95,99].into_iter().map(|p| (p.to_string(),
            json!({"stored":quantile(&self.hist[0],p,100),"exact_fee_lower_size":quantile(&self.hist[1],p,100),
                "exact_fee_upper_size":quantile(&self.hist[2],p,100)}))).collect();
        json!({"records":self.records,"outputs":self.outputs,"transparent_inputs":self.inputs,
            "shielded_component_records":self.shielded,"input_only":self.input_only,
            "fees":{"unknown":self.fees[0],"exact_zero":self.fees[1],"exact_nonzero":self.fees[2],"not_applicable":self.fees[3]},
            "sizes":{"stored":histogram_report(&self.hist[0]),"exact_fee_lower_size":histogram_report(&self.hist[1]),"exact_fee_upper_size":histogram_report(&self.hist[2])},
            "thresholds":thresholds,"empirical_frontiers_bytes":frontiers})
    }
}

#[derive(Clone)]
struct Shape {
    id: [u8; 32],
    sizes: [usize; 3],
    height: u64,
    lookup_hash: u64,
    overflow_hash: u64,
    coincidence: u64,
    coarse_coincidence: bool,
    coarse_choices: [u64; 2],
}
impl Shape {
    fn new(id: [u8; 32], sizes: [usize; 3], height: u64, archive: usize) -> Self {
        let lookup_hash = hash_value(id, b"txid-sizing/lookup/");
        let overflow_hash = hash_value(id, b"txid-sizing/overflow/");
        let mut coincidence = 0;
        for buckets in [1, 4, 16, 64] {
            let b = (lookup_hash % buckets) as usize;
            let [a, c] = candidate_rows(transparent_events::Txid(id), b as u64, 4096);
            if a == c {
                coincidence |= 1u64 << b;
            }
        }
        let [a, b] = candidate_rows(transparent_events::Txid(id), archive as u64, 4096);
        Self {
            id,
            sizes,
            height,
            lookup_hash,
            overflow_hash,
            coincidence,
            coarse_coincidence: a == b,
            coarse_choices: [a, b],
        }
    }
}
// Row packing uses the existing v1 fragment envelope and txid ordering, but
// changing the threshold is a layout proposal, not production builder output.
fn packed_rows(records: &[Shape], threshold: usize, side: usize) -> (usize, u64) {
    let mut rows = 0;
    let mut used = 4;
    let mut queries = 0;
    for r in records {
        let size = r.sizes[side];
        if size <= threshold {
            continue;
        }
        let mut left = size;
        let mut first = None;
        while left > 0 {
            let n = left.min(FRAGMENT);
            if rows == 0 {
                rows = 1;
            }
            if used + 42 + n > ROW {
                rows += 1;
                used = 4;
            }
            first.get_or_insert(rows);
            used += 42 + n;
            left -= n;
        }
        queries += (rows - first.unwrap() + 1) as u64;
    }
    (rows, queries)
}

fn directory_segments(records: &[Shape], threshold: usize, side: usize) -> usize {
    let mut segments = vec![vec![4usize; 4096]];
    for r in records {
        let size = r.sizes[side];
        let extent = 48 + if size <= threshold { size } else { 0 };
        let [a, b] = r.coarse_choices.map(|v| v as usize);
        let mut placed = false;
        for rows in &mut segments {
            let c = if rows[a] <= rows[b] { a } else { b };
            for choice in [c, a, b] {
                if rows[choice] + extent <= ROW {
                    rows[choice] += extent;
                    placed = true;
                    break;
                }
            }
            if placed {
                break;
            }
        }
        if !placed {
            let mut rows = vec![4usize; 4096];
            rows[a] += extent;
            segments.push(rows);
        }
    }
    segments.len()
}
fn safe_page_bounds(records: &[Shape], t: usize) -> [usize; 2] {
    let mut min_bytes = 0usize;
    let mut max_bytes = 0usize;
    let mut max_fragments = 0usize;
    for r in records {
        let [_, lo, hi] = r.sizes;
        if lo > t {
            min_bytes += lo + 42 * lo.div_ceil(FRAGMENT);
        }
        if hi > t {
            let f = hi.div_ceil(FRAGMENT);
            max_fragments += f;
            max_bytes += hi + 42 * f;
        }
    }
    [
        min_bytes.div_ceil(ROW - 4),
        max_fragments.min((2 * max_bytes).div_ceil(ROW - 4)),
    ]
}

#[derive(Default)]
struct Pages {
    // (threshold, stored/lower/upper, era): archive demand distribution
    demand: BTreeMap<(usize, usize, String), Hist>,
    // Exact total allocated rows for shared adjacent k-archive page tables.
    allocations: BTreeMap<(usize, usize, usize, usize), u64>,
    // Pending page demand, rounded only at a k-archive group boundary.
    pending: BTreeMap<(usize, usize, usize), usize>,
    archives: u64,
    directories: BTreeMap<(usize, usize), u64>,
    queries: BTreeMap<(usize, usize), u64>,
}
impl Pages {
    fn flush_group(&mut self, t: usize, side: usize, k: usize) {
        let demand = self.pending.remove(&(t, side, k)).unwrap_or(0);
        for geometry in [256, 512, 1024, 4096] {
            let rows = demand.div_ceil(geometry).max(1) * geometry;
            *self.allocations.entry((t, side, k, geometry)).or_default() += rows as u64;
        }
    }
    fn add(&mut self, records: &mut [Shape]) {
        records.sort_unstable_by_key(|r| r.id);
        self.archives += 1;
        let eras: BTreeSet<_> = records.iter().map(|r| era(r.height)).collect();
        let label = if eras.len() == 1 {
            eras.first().unwrap().to_string()
        } else {
            "mixed-era".into()
        };
        for t in THRESHOLDS {
            for side in 0..5 {
                let rows = if side < 3 {
                    let (rows, queries) = packed_rows(records, t, side);
                    *self.queries.entry((t, side)).or_default() += queries;
                    *self.directories.entry((t, side)).or_default() +=
                        directory_segments(records, t, side) as u64;
                    rows
                } else {
                    safe_page_bounds(records, t)[side - 3]
                };
                for name in ["all", label.as_str()] {
                    *self
                        .demand
                        .entry((t, side, name.into()))
                        .or_default()
                        .entry(rows)
                        .or_default() += 1;
                }
                for k in [1, 4, 16, 64] {
                    *self.pending.entry((t, side, k)).or_default() += rows;
                    if self.archives.is_multiple_of(k as u64) {
                        self.flush_group(t, side, k);
                    }
                }
            }
        }
    }
    fn report(mut self) -> Value {
        for (t, side, k) in self.pending.keys().copied().collect::<Vec<_>>() {
            self.flush_group(t, side, k);
        }
        let demands: Vec<_> = self.demand.iter().map(|((t,side,era),h)| json!({"threshold":t,"size_population":side_name(*side),"era":era,"page_rows":histogram_report(h)})).collect();
        let fixed: u64 = 14848 + 16 + 32 + 2 * 2048 * 8 + 8 + 2048 * 2048 * 2 * 8;
        let layouts: Vec<_> = self
            .allocations
            .iter()
            .map(|(&(t, side, k, geometry), &rows)| {
                let segments = rows / geometry as u64;
                json!({"threshold":t,"size_population":side_name(side),"archives_per_pages_table":k,
                "page_geometry_rows":geometry,"table_groups":self.archives.div_ceil(k as u64),
                "allocated_segments":segments,"encoded_database_bytes":rows*ROW as u64,
                "native_preprocessing_reservation_bytes":rows*ROW as u64+segments*fixed,
                "directory_geometry_rows":4096,
                "directory_encoded_database_bytes":self.directories.get(&(t,side)).map(|s|s*4096*ROW as u64),
                "directory_native_preprocessing_reservation_bytes":self.directories.get(&(t,side)).map(|s|s*(4096*ROW as u64+fixed)),
                "total_encoded_database_bytes":self.directories.get(&(t,side)).map(|s|s*4096*ROW as u64+rows*ROW as u64),
                "total_native_preprocessing_reservation_bytes":self.directories.get(&(t,side)).map(|s|s*(4096*ROW as u64+fixed)+rows*ROW as u64+segments*fixed),
                "native_rss_bytes":Value::Null,"latency_seconds":Value::Null})
            })
            .collect();
        json!({"archive_records":ARCHIVE,"archives":self.archives,
            "uncapped_page_query_sum":self.queries.iter().map(|((t,side),n)|json!({"threshold":t,"size_population":side_name(*side),"queries":n})).collect::<Vec<_>>(),"demand":demands,"layout_options":layouts,
            "packing":"txid-sorted per archive, exact v1 fragment envelope; shared tables concatenate independently packed archives (conservative extra partial rows)",
            "qualification":"Stored sizes replay the v1 packer. Exact-fee endpoint scenarios can have row-order discontinuities; separate safe page bounds use minimum entry bytes /4092 and maximum next-fit demand <= min(max fragments,ceil(2*max entry bytes/4092)). These safe bounds cover every interior fee width. Directory endpoint allocations are scenarios, not interval bounds.",
            "memory":"Encoded allocations and source reservation formula only; includes independent per-archive 4096-row directories for the three size scenarios; excludes journal reader, identity SQLite, compiler and runtime overhead; not native RSS or latency."})
    }
}
fn side_name(side: usize) -> &'static str {
    [
        "stored",
        "exact_fee_lower_size",
        "exact_fee_upper_size",
        "exact_fee_safe_rows_lower",
        "exact_fee_safe_rows_upper",
    ][side]
}

// Observable route proposals. Neither hash namespace is an implemented route.
#[derive(Clone, Copy)]
struct RouteModel {
    coarse: bool,
    lookups: usize,
    overflows: usize,
    era_visible: bool,
    requests_visible: bool,
}
fn models() -> Vec<RouteModel> {
    let mut out = Vec::new();
    for coarse in [true, false] {
        for lookups in if coarse { vec![1] } else { vec![1, 4, 16, 64] } {
            for overflows in [1, 4, 16] {
                for full_observables in [false, true] {
                    let era_visible = full_observables;
                    let requests_visible = full_observables;
                    out.push(RouteModel {
                        coarse,
                        lookups,
                        overflows,
                        era_visible,
                        requests_visible,
                    });
                }
            }
        }
    }
    out
}
fn hash_value(id: [u8; 32], domain: &[u8]) -> u64 {
    let mut h = Sha256::new();
    h.update(domain);
    h.update(id);
    u64::from_le_bytes(h.finalize()[..8].try_into().unwrap())
}
type RouteKey = (usize, usize, usize, usize, usize); // lookup, overflow-or-inline, era, pages, initial-query count
fn route_key(r: &Shape, size: usize, t: usize, m: RouteModel, archive: usize) -> RouteKey {
    let lookup = if m.coarse {
        archive
    } else {
        (r.lookup_hash % m.lookups as u64) as usize
    };
    let overflow = if size <= t {
        usize::MAX
    } else {
        (r.overflow_hash % m.overflows as u64) as usize
    };
    let e = if m.era_visible {
        ERAS.iter().rposition(|(h, _)| r.height >= *h).unwrap()
    } else {
        0
    };
    let pages = if m.requests_visible && size > t {
        size.div_ceil(FRAGMENT)
    } else {
        0
    };
    let initial = if m.requests_visible {
        let coincident = if m.coarse {
            r.coarse_coincidence
        } else {
            r.coincidence & (1u64 << lookup) != 0
        };
        if coincident {
            1
        } else {
            2
        }
    } else {
        0
    };
    (lookup, overflow, e, pages, initial)
}
#[derive(Default)]
struct Candidates {
    classes: u64,
    definite: Hist,
    possible: Hist,
    uncertain_records: u64,
}
impl Candidates {
    fn add(&mut self, groups: BTreeMap<RouteKey, [u64; 2]>) {
        for (_, [lo, hi]) in groups {
            self.classes += 1;
            *self.definite.entry(lo as usize).or_default() += 1;
            *self.possible.entry(hi as usize).or_default() += 1;
        }
    }
    fn report(&self) -> Value {
        let controls: Vec<_> = [5,1000,10000].into_iter().map(|k|json!({"K":k,
            "classes_with_fewer_than_K_definite_candidates":self.definite.range(..k).map(|(_,n)|n).sum::<u64>(),
            "classes_with_fewer_than_K_possible_candidates":self.possible.range(..k).map(|(_,n)|n).sum::<u64>()})).collect();
        json!({"possible_classes":self.classes,"uncertain_records":self.uncertain_records,
            "definite_candidates_per_possible_class":histogram_report(&self.definite),
            "possible_candidates_per_possible_class":histogram_report(&self.possible),"controls":controls,
            "qualified_privacy_guarantee":false})
    }
}
struct Routes {
    models: Vec<RouteModel>,
    // Hash models accumulate across archives; coarse models finalize each archive.
    pending: Vec<Vec<BTreeMap<RouteKey, [u64; 2]>>>,
    result: Vec<Vec<Candidates>>,
}
impl Routes {
    fn new() -> Self {
        let models = models();
        Self {
            pending: (0..models.len())
                .map(|_| (0..7).map(|_| BTreeMap::new()).collect())
                .collect(),
            result: (0..models.len())
                .map(|_| (0..7).map(|_| Candidates::default()).collect())
                .collect(),
            models,
        }
    }
    fn add(&mut self, records: &[Shape], archive: usize) {
        for (i, &m) in self.models.iter().enumerate() {
            for (j, t) in THRESHOLDS.into_iter().enumerate() {
                let groups = &mut self.pending[i][j];
                for r in records {
                    // Unknown-fee width span is < FRAGMENT, and all tested inline
                    // cutoffs are < FRAGMENT-width. At most two route keys exist;
                    // endpoints cover every interior width without per-width hashes.
                    let lower = route_key(r, r.sizes[1], t, m, archive);
                    let upper = route_key(r, r.sizes[2], t, m, archive);
                    let definite = lower == upper;
                    self.result[i][j].uncertain_records += u64::from(!definite);
                    let n = groups.entry(lower).or_default();
                    n[0] += u64::from(definite);
                    n[1] += 1;
                    if !definite {
                        groups.entry(upper).or_default()[1] += 1;
                    }
                }
                if m.coarse {
                    self.result[i][j].add(std::mem::take(groups));
                }
            }
        }
    }
    fn report(mut self) -> Value {
        let mut out = Vec::new();
        for (i, m) in self.models.iter().enumerate() {
            for (j, t) in THRESHOLDS.into_iter().enumerate() {
                self.result[i][j].add(std::mem::take(&mut self.pending[i][j]));
                out.push(json!({"threshold":t,"lookup":if m.coarse {"chronological-40000-record-archive"} else {"independent-hash"},
                "lookup_hash_buckets":(!m.coarse).then_some(m.lookups),"overflow_hash_buckets":m.overflows,
                "era_visible":m.era_visible,"logical_fragment_count_and_initial_query_count_visible":m.requests_visible,
                "candidates":self.result[i][j].report()}));
            }
        }
        json!({"models":out,"candidate_unit":"Distinct real txids; identity uniqueness checked in disk SQLite. Never padding, outputs or fragments.",
            "bounds":"Every possible unknown fee width is covered by the endpoint route keys (width span < fragment capacity). Definite members have exactly one possible route key; possible members can have several. Histograms count classes, not weighted transaction probabilities. A class with zero definite members need not exist in the realized exact-fee population.",
            "observables":"Initial lookup bucket, inline/overflow branch, broad overflow hash bucket; optionally era and logical fragment count plus directory-choice coincidence. One fixed snapshot/revision. Timing, retries, exact shared-page query counts, page segment counts and differing refresh revisions are NOT modeled and can further narrow classes.",
            "policy":"K=5 is a diagnostic control; K=1000/10000 are engineering policies, not formal anonymity guarantees. Global overflow cannot widen an already exposed lookup bucket. Cover traffic and padding are not counted as real candidates."})
    }
}

pub fn collect(args: &[String]) -> Result<Value, AnyError> {
    if !(3..=4).contains(&args.len()) {
        return Err(
            "usage: --journal-census JOURNAL ANCHOR_HEIGHT ANCHOR_HASH_OR_--anchor-from-journal [SCRATCH_DIR]".into(),
        );
    }
    let dir = Path::new(&args[0]);
    let end: u64 = args[1].parse()?;
    let derive_anchor = args[2] == "--anchor-from-journal";
    // Hold a shared lock throughout. Never race an ingest, truncate or create a
    // source file; the existing writer holds its exclusive lock until stopped.
    let lock = File::open(dir.join("writer.lock"))?;
    lock.try_lock_shared()?;
    let store = EventStore::open_existing(dir)?;
    if store.start_height() != 0
        || store.genesis_hash() != transparent_filter::MAINNET_GENESIS_DISPLAY
    {
        return Err("census requires mainnet genesis start".into());
    }
    if store
        .block_at(0)
        .ok_or("genesis missing")?
        .block_hash
        .to_display_hex()
        != store.genesis_hash()
    {
        return Err("genesis block hash mismatch".into());
    }
    let observed = store
        .block_at(end)
        .ok_or("anchor outside committed journal")?
        .block_hash
        .to_display_hex();
    let expected = if derive_anchor {
        observed.clone()
    } else {
        args[2].clone()
    };
    if observed != expected {
        return Err("anchor hash mismatch".into());
    }
    let source_path = dir.canonicalize()?;
    let scratch_parent = args
        .get(3)
        .map(std::path::PathBuf::from)
        .unwrap_or_else(std::env::temp_dir)
        .canonicalize()?;
    if scratch_parent.starts_with(&source_path) {
        return Err("scratch must be outside source journal".into());
    }
    let scratch = tempfile::tempdir_in(scratch_parent)?;
    let mut identities = Connection::open(scratch.path().join("identities.sqlite"))?;
    identities.execute_batch("PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF; PRAGMA cache_size=-32768; CREATE TABLE ids(txid BLOB PRIMARY KEY) WITHOUT ROWID;")?;
    let mut domains: BTreeMap<String, Domain> = BTreeMap::new();
    let mut archives = Vec::with_capacity(ARCHIVE);
    let mut pages = Pages::default();
    let mut routes = Routes::new();
    let mut digest = Sha256::new();
    let mut source_bytes = 0u64;
    let mut sidecar_digest = Sha256::new();
    let start = Instant::now();
    let mut n = 0u64;
    for height in 0..=end {
        let block = store.block_at(height).ok_or("committed height missing")?;
        digest.update(height.to_le_bytes());
        digest.update(block.block_hash.internal_bytes());
        let path = dir
            .join("display-v1")
            .join(format!("{}.bin", block.block_hash.to_display_hex()));
        source_bytes += std::fs::metadata(&path)?.len();
        // Use the existing checksum/extent/duplicate/canonical-codec reader.
        // Do not load events.bin; event replay adds no display sizing facts.
        let records = display_journal::read(dir, block.block_hash)?;
        if records.iter().filter(|r| r.coinbase).count() != 1 {
            return Err("display block must contain exactly one eligible coinbase".into());
        }
        let mut sidecar = File::open(&path)?;
        sidecar.seek(SeekFrom::End(-32))?;
        let mut sum = [0u8; 32];
        sidecar.read_exact(&mut sum)?;
        sidecar_digest.update(height.to_le_bytes());
        sidecar_digest.update(block.block_hash.internal_bytes());
        sidecar_digest.update(sum);
        let txn = identities.transaction()?;
        {
            let mut insert = txn.prepare_cached("INSERT INTO ids VALUES (?1)")?;
            for record in &records {
                if !record.coinbase
                    && record.metadata.transparent_input_count == 0
                    && record.outputs.is_empty()
                {
                    return Err("ineligible transaction in display sidecar".into());
                }
                insert
                    .execute(params![record.txid.0.as_slice()])
                    .map_err(|_| "duplicate txid or identity scratch failure")?;
                let sizes = size_bounds(record)?;
                let category = if record.coinbase {
                    "coinbase"
                } else {
                    "non_coinbase"
                };
                for domain in [
                    "all".to_string(),
                    category.to_string(),
                    era(height).to_string(),
                    format!("{}/{}", era(height), category),
                ] {
                    domains.entry(domain).or_default().add(record, sizes);
                }
                archives.push(Shape::new(
                    record.txid.0,
                    sizes,
                    height,
                    pages.archives as usize,
                ));
                n += 1;
                if archives.len() == ARCHIVE {
                    routes.add(&archives, pages.archives as usize);
                    pages.add(&mut archives);
                    archives.clear();
                }
            }
        }
        txn.commit()?;
        if height.is_multiple_of(10_000) {
            eprintln!(
                "journal census: height={height}/{end} records={n} elapsed_seconds={:.1}",
                start.elapsed().as_secs_f64()
            );
        }
    }
    let partial_archive_records = archives.len();
    if !archives.is_empty() {
        routes.add(&archives, pages.archives as usize);
        pages.add(&mut archives);
    }
    let identity_count: u64 = identities.query_row("SELECT count(*) FROM ids", [], |r| r.get(0))?;
    if identity_count != n {
        return Err("distinct identity inventory mismatch".into());
    }
    let checkpoint = std::fs::read(dir.join("checkpoint.bin"))?;
    let pins: BTreeMap<_, _> = [
        (
            "reader_events",
            include_bytes!("../../../../services/transparent-filter-server/src/events.rs")
                .as_slice(),
        ),
        (
            "reader_sidecar",
            include_bytes!("../../../../services/transparent-filter-server/src/display_journal.rs")
                .as_slice(),
        ),
        (
            "codec",
            include_bytes!("../../../../crates/transparent-shard/src/txid.rs").as_slice(),
        ),
        (
            "metadata",
            include_bytes!("../../../../crates/transparent-events/src/lib.rs").as_slice(),
        ),
        ("census", include_bytes!("journal_census.rs").as_slice()),
        ("lock", include_bytes!("../Cargo.lock").as_slice()),
    ]
    .into_iter()
    .map(|(name, bytes)| (name, hex::encode(Sha256::digest(bytes))))
    .collect();
    let output = json!({"schema":"txid-display-journal-census-v1","codec":CODEC,
        "source":{"start_height":0,"anchor_height":end,"anchor_hash_display":expected,"anchor_hash_external_expectation":!derive_anchor,"journal_version":store.version(),
            "journal_covered_through":store.covered_through(),"era_boundaries":ERAS,"checkpoint_hex":hex::encode(checkpoint),
            "block_membership_sha256":hex::encode(digest.finalize()),
            "sidecar_content_digest_chain_sha256":hex::encode(sidecar_digest.finalize()),"sidecar_bytes_read":source_bytes,
            "sidecar_validation":"existing reader checks SHA256, hash envelope, unique block txids and canonical display encoding",
            "chain_consensus_independently_validated":false,"parser_git_pin":"af944f5194ef2e9921bc96af017629450375013c",
            "compiled_source_file_sha256":pins,
            "binary_sha256":hex::encode(Sha256::digest(std::fs::read(std::env::current_exe()?)?)),"ingest_executable_and_source_pin":Value::Null},
        "measurement":{"elapsed_seconds":start.elapsed().as_secs_f64(),"partial_archive_records":partial_archive_records,"distinct_display_records":n,"committed_blocks":end+1,
            "total_chain_transactions":Value::Null,"shielded_only_exclusions":Value::Null,
            "inventory_limit":"Display sidecars cannot reveal excluded shielded-only transactions. Attach the ingest's independent canonical transaction/exclusion receipt; no exclusion count is inferred from events or empty sidecars."},
        "fee_policy":{"unknown_exact_fee_encoded_bytes":[varlen(0),varlen(MAX_MONEY)],"MAX_MONEY":MAX_MONEY,
            "unknown_is_measured_exact_fee":false,"exact_zero_and_nonapplicable_distinct":true},
        "domains":domains.iter().map(|(k,d)|(k.clone(),d.report())).collect::<BTreeMap<_,_>>(),
        "pages":pages.report(),"joint_routes":routes.report(),
        "limitations":"Full committed eligible-sidecar census conditional on ingest completeness, not independent full-chain transaction/exclusion audit. Current codec only; thresholds above 128, independent/shared routing and small page geometries are proposals. No native benchmark, service change or production qualification."});
    Ok(output)
}

pub fn run(args: &[String]) -> Result<(), AnyError> {
    let output = collect(args)?;
    serde_json::to_writer(std::io::stdout().lock(), &output)?;
    println!();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use transparent_events::{TransactionMetadata, Txid};
    use transparent_shard::txid::DisplayOutput;
    fn record(id: u8) -> TransparentDisplayRecord {
        TransparentDisplayRecord {
            txid: Txid([id; 32]),
            coinbase: false,
            metadata: TransactionMetadata {
                fee: FeeState::Unknown,
                transparent_input_count: 1,
                has_shielded_components: false,
            },
            outputs: vec![DisplayOutput {
                value: 0,
                script: vec![0x51],
            }],
        }
    }
    #[test]
    #[ignore = "explicit bounded analysis profiler; not native PIR or chain evidence"]
    fn profile_synthetic_40000_records() {
        let start = Instant::now();
        let mut records = Vec::with_capacity(ARCHIVE);
        for i in 0..ARCHIVE {
            let id: [u8; 32] = Sha256::digest((i as u64).to_le_bytes()).into();
            let size = if i % 100 < 92 {
                110
            } else {
                [250, 8000, 16384][i % 3]
            };
            records.push(Shape::new(
                id,
                [size, size + 1, size + varlen(MAX_MONEY)],
                i as u64,
                0,
            ));
        }
        let mut routes = Routes::new();
        routes.add(&records, 0);
        let mut pages = Pages::default();
        pages.add(&mut records);
        let routes = routes.report();
        let pages = pages.report();
        eprintln!(
            "{}",
            json!({"schema":"txid-sizing-synthetic-analysis-profile-v1",
            "records":records.len(),"elapsed_seconds":start.elapsed().as_secs_f64(),
            "route_models":routes["models"].as_array().unwrap().len(),
            "layout_options":pages["layout_options"].as_array().unwrap().len(),
            "qualification":"Synthetic algorithm time only; excludes disk identity checks, journal reads and native PIR. No chain throughput, coverage, RSS or latency claim."})
        );
    }
    #[test]
    fn complete_synthetic_journal_census_and_cross_block_duplicate_refusal() {
        let dir = tempfile::tempdir().unwrap();
        let hash = transparent_filter::BlockHash::from_display_hex(
            transparent_filter::MAINNET_GENESIS_DISPLAY,
        )
        .unwrap();
        let mut store =
            EventStore::open(dir.path(), transparent_filter::MAINNET_GENESIS_DISPLAY, 0).unwrap();
        let mut coinbase = record(1);
        coinbase.coinbase = true;
        coinbase.metadata.transparent_input_count = 0;
        coinbase.metadata.fee = FeeState::NotApplicable;
        store
            .append_block_with_display(0, hash, &[], &[coinbase.clone(), record(2)])
            .unwrap();
        store.commit().unwrap();
        drop(store);
        let args = vec![
            dir.path().to_str().unwrap().to_string(),
            "0".into(),
            hash.to_display_hex(),
        ];
        let output = collect(&args).unwrap();
        assert_eq!(output["measurement"]["distinct_display_records"], 2);
        assert!(output["measurement"]["shielded_only_exclusions"].is_null());
        assert_eq!(output["domains"]["all"]["fees"]["unknown"], 1);
        assert_eq!(output["domains"]["coinbase"]["records"], 1);
        assert_eq!(
            output["domains"]["all"]["thresholds"]["128"]["stored"]["inline"],
            2
        );
        assert_eq!(output["pages"]["archives"], 1);
        let mut wrong = args.clone();
        wrong[2] = "00".repeat(32);
        assert!(collect(&wrong).is_err());
        let mut store =
            EventStore::open(dir.path(), transparent_filter::MAINNET_GENESIS_DISPLAY, 0).unwrap();
        let hash2 = transparent_filter::BlockHash::from_internal_bytes([3; 32]);
        store
            .append_block_with_display(1, hash2, &[], &[coinbase])
            .unwrap();
        store.commit().unwrap();
        drop(store);
        let args = vec![
            dir.path().to_str().unwrap().to_string(),
            "1".into(),
            hash2.to_display_hex(),
        ];
        assert!(collect(&args)
            .unwrap_err()
            .to_string()
            .contains("duplicate txid"));
    }
    #[test]
    fn incomplete_and_ineligible_sidecar_records_are_refused() {
        let hash = transparent_filter::BlockHash::from_display_hex(
            transparent_filter::MAINNET_GENESIS_DISPLAY,
        )
        .unwrap();
        for missing_coinbase in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let mut store =
                EventStore::open(dir.path(), transparent_filter::MAINNET_GENESIS_DISPLAY, 0)
                    .unwrap();
            let mut excluded = record(2);
            excluded.metadata.transparent_input_count = 0;
            excluded.metadata.has_shielded_components = true;
            excluded.outputs.clear();
            let mut records = vec![excluded];
            if !missing_coinbase {
                let mut cb = record(1);
                cb.coinbase = true;
                cb.metadata.transparent_input_count = 0;
                cb.metadata.fee = FeeState::NotApplicable;
                records.push(cb);
            }
            store
                .append_block_with_display(0, hash, &[], &records)
                .unwrap();
            store.commit().unwrap();
            drop(store);
            let args = vec![
                dir.path().to_str().unwrap().to_string(),
                "0".into(),
                "--anchor-from-journal".into(),
            ];
            let err = collect(&args).unwrap_err().to_string();
            assert!(err.contains(if missing_coinbase {
                "exactly one eligible coinbase"
            } else {
                "ineligible transaction"
            }));
        }
    }
    #[test]
    fn current_codec_packing_matches_independent_existing_builder() {
        let mut records = vec![record(1), record(2), record(3)];
        records[0].outputs[0].script = vec![0x51; 4051];
        records[1].outputs[0].script = vec![0x51; 170];
        records[2].outputs[0].script = vec![0x51; 1];
        let mut shapes: Vec<_> = records
            .iter()
            .map(|r| Shape::new(r.txid.0, size_bounds(r).unwrap(), 0, 0))
            .collect();
        shapes.sort_unstable_by_key(|s| s.id);
        let tables =
            transparent_shard::txid::build(0, &transparent_shard::RECENT_4K, &records).unwrap();
        transparent_shard::txid::verify(
            0,
            &transparent_shard::RECENT_4K,
            &tables.directory,
            &tables.pages,
            tables.records,
        )
        .unwrap();
        let occupied = tables
            .pages
            .iter()
            .flat_map(|s| s.chunks_exact(4096))
            .filter(|r| u32::from_le_bytes(r[..4].try_into().unwrap()) > 0)
            .count();
        assert_eq!(packed_rows(&shapes, 128, 0).0, occupied);
        assert_eq!(directory_segments(&shapes, 128, 0), tables.directory.len());
    }
    #[test]
    fn every_interior_fee_width_has_conservative_page_and_route_bounds() {
        for t in THRESHOLDS {
            for lo in [t.saturating_sub(3), t + 1, 4048, 4051, 8100] {
                let shapes: Vec<_> = (1..=9)
                    .map(|id| Shape::new([id; 32], [lo, lo, lo + 7], 1, 0))
                    .collect();
                let bounds = safe_page_bounds(&shapes, t);
                for width in 0..=7 {
                    let mut interior = shapes.clone();
                    for r in &mut interior {
                        r.sizes[0] = lo + width;
                    }
                    let rows = packed_rows(&interior, t, 0).0;
                    assert!(bounds[0] <= rows && rows <= bounds[1]);
                    for m in models() {
                        let low = route_key(&shapes[0], lo, t, m, 0);
                        let high = route_key(&shapes[0], lo + 7, t, m, 0);
                        let key = route_key(&shapes[0], lo + width, t, m, 0);
                        assert!(key == low || key == high);
                    }
                }
            }
        }
    }
    #[test]
    fn unknown_exact_zero_nonapplicable_and_frontiers() {
        let mut r = record(1);
        let b = size_bounds(&r).unwrap();
        assert_eq!(b[1], b[0] + 1);
        assert_eq!(b[2], b[0] + varlen(MAX_MONEY));
        r.metadata.fee = FeeState::Exact(0);
        let exact = size_bounds(&r).unwrap();
        assert_eq!(exact, [b[1]; 3]);
        r.coinbase = true;
        r.metadata.transparent_input_count = 0;
        r.metadata.fee = FeeState::NotApplicable;
        assert_eq!(size_bounds(&r).unwrap()[0], size_bounds(&r).unwrap()[2]);
        assert_eq!(
            quantile(&BTreeMap::from([(128, 85), (192, 15)]), 85, 100),
            Some(128)
        );
        assert_eq!(quantile(&BTreeMap::new(), 99, 100), None);
    }
    #[test]
    fn exact_fragment_packing_and_shared_rounding() {
        let r = |id, size| Shape::new([id; 32], [size; 3], 1, 0);
        assert_eq!(packed_rows(&[r(1, 128)], 128, 0), (0, 0));
        assert_eq!(packed_rows(&[r(1, 4050)], 128, 0), (1, 1));
        assert_eq!(packed_rows(&[r(1, 4051)], 128, 0), (2, 2));
        assert_eq!(packed_rows(&[r(1, 129), r(2, 129)], 128, 0), (1, 2));
        let mut p = Pages::default();
        p.add(&mut [r(1, 4051)]);
        p.add(&mut [r(2, 4051)]);
        let out = p.report();
        let layouts = out["layout_options"].as_array().unwrap();
        let find = |k| {
            layouts
                .iter()
                .find(|v| {
                    v["threshold"] == 128
                        && v["size_population"] == "stored"
                        && v["archives_per_pages_table"] == k
                        && v["page_geometry_rows"] == 256
                })
                .unwrap()
        };
        assert_eq!(find(1)["allocated_segments"], 2);
        assert_eq!(find(4)["allocated_segments"], 1);
    }
    #[test]
    fn five_real_candidates_and_ambiguous_membership() {
        let mut rs = Routes::new();
        let records: Vec<_> = (1..=5)
            .map(|i| Shape::new([i; 32], [129; 3], 1, 0))
            .collect();
        rs.add(&records, 0);
        let out = rs.report();
        let models = out["models"].as_array().unwrap();
        let v = models
            .iter()
            .find(|v| {
                v["threshold"] == 128
                    && v["lookup"] == "independent-hash"
                    && v["lookup_hash_buckets"] == 1
                    && v["overflow_hash_buckets"] == 1
                    && v["era_visible"] == false
                    && v["logical_fragment_count_and_initial_query_count_visible"] == false
            })
            .unwrap();
        assert_eq!(
            v["candidates"]["definite_candidates_per_possible_class"]["min"],
            5
        );
        let mut rs = Routes::new();
        rs.add(&[Shape::new([1; 32], [127, 128, 135], 1, 0)], 0);
        let out = rs.report();
        let v = &out["models"][0];
        assert_eq!(v["candidates"]["uncertain_records"], 1);
        assert_eq!(
            v["candidates"]["definite_candidates_per_possible_class"]["min"],
            0
        );
    }
    #[test]
    fn journal_reader_fixture_orphans_missing_checksum_and_writer_lock() {
        let dir = tempfile::tempdir().unwrap();
        let hash = transparent_filter::BlockHash::from_internal_bytes([2; 32]);
        let mut store =
            EventStore::open(dir.path(), transparent_filter::MAINNET_GENESIS_DISPLAY, 0).unwrap();
        store
            .append_block_with_display(0, hash, &[], &[record(1)])
            .unwrap();
        store.commit().unwrap();
        let read_lock = File::open(dir.path().join("writer.lock")).unwrap();
        assert!(read_lock.try_lock_shared().is_err());
        drop(store);
        read_lock.try_lock_shared().unwrap();
        let reader = EventStore::open_existing(dir.path()).unwrap();
        assert_eq!(
            display_journal::read(dir.path(), reader.block_at(0).unwrap().block_hash)
                .unwrap()
                .len(),
            1
        );
        display_journal::write(
            dir.path(),
            transparent_filter::BlockHash::from_internal_bytes([3; 32]),
            &[record(4)],
        )
        .unwrap();
        assert!(reader.block_at(1).is_none());
        let path = dir
            .path()
            .join("display-v1")
            .join(format!("{}.bin", hash.to_display_hex()));
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[50] ^= 1;
        std::fs::write(&path, bytes).unwrap();
        assert!(display_journal::read(dir.path(), hash).is_err());
        std::fs::remove_file(path).unwrap();
        assert!(display_journal::read(dir.path(), hash).is_err());
    }
}
