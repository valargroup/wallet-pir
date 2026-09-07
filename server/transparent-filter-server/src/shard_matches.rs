//! How many shards each exact script appears in.
//!
//! This is the number that turns a boundary policy into a wallet's bill. A
//! script active in `g` shards costs `2 * g` directory queries to restore — two
//! because the directory gives every script two candidate rows and both are
//! always queried — so a policy that halves the shard count halves that term.
//! Nothing else in the census reports it: shard occupancy says how much a shard
//! holds, not how many shards one script has to be looked for in.
//!
//! # Why it is not a hash map
//!
//! Counting distinct scripts is the one measurement here that does not fit in
//! memory. The Ironwood sample has 93,281 indexable scripts and would, but a
//! genesis-to-tip journal has orders of magnitude more, and each occurrence must
//! be kept until every shard has been seen. So occurrences are spilled to sorted
//! runs and merged, which costs disk proportional to script-shard occurrences
//! and memory proportional to one run. The exact bytes are kept rather than a
//! digest: a digest would make the group counts probabilistic, and the tail of
//! this distribution is what the geometry decision rests on.

use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Longest script recorded, matching the directory's own coverage limit.
const SCRIPT_BYTES: usize = 40;

/// One occurrence: padded script, then length, then the fragments that script's
/// history costs in this shard.
///
/// The sort key is the leading script and length, so a plain byte comparison
/// still groups a script's occurrences and still keeps two scripts with a
/// common padded prefix apart. The fragment count rides along in the tail,
/// where it does not disturb that order.
const RECORD: usize = SCRIPT_BYTES + 2 + 4;

/// Bytes of a record that participate in the sort, which is the script key.
const KEY: usize = SCRIPT_BYTES + 2;

/// Occurrences held before a sorted run is spilled. About 42 MB.
const RUN_RECORDS: usize = 1_000_000;

/// Accumulates one policy's script-shard occurrences.
pub struct MatchCounter {
    dir: PathBuf,
    tag: String,
    buffered: Vec<[u8; RECORD]>,
    run_records: usize,
    runs: Vec<PathBuf>,
    occurrences: u64,
}

impl MatchCounter {
    pub fn new(dir: &Path, tag: &str) -> Self {
        Self::with_run_records(dir, tag, RUN_RECORDS)
    }

    /// A counter that spills every `run_records` occurrences.
    ///
    /// Exists so a test can reach the merge with a handful of scripts. A single
    /// run never merges anything, so a counter that only ever buffered would
    /// test the sort and nothing else.
    pub fn with_run_records(dir: &Path, tag: &str, run_records: usize) -> Self {
        assert!(run_records > 0, "a run holds at least one record");
        Self {
            dir: dir.to_path_buf(),
            // Policy names come from a command line and become file names.
            tag: tag
                .chars()
                .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
                .collect(),
            buffered: Vec::with_capacity(run_records.min(RUN_RECORDS)),
            run_records,
            runs: Vec::new(),
            occurrences: 0,
        }
    }

    /// Records that `script` appeared in one more shard.
    ///
    /// Scripts too long for a directory entry are dropped, because they are
    /// never placed and so never cost a directory query.
    pub fn record(&mut self, script: &[u8], fragments: u32) -> Result<(), BoxError> {
        if script.len() > SCRIPT_BYTES {
            return Ok(());
        }
        let mut record = [0u8; RECORD];
        record[..script.len()].copy_from_slice(script);
        record[SCRIPT_BYTES..KEY].copy_from_slice(&(script.len() as u16).to_le_bytes());
        record[KEY..].copy_from_slice(&fragments.to_le_bytes());
        self.buffered.push(record);
        self.occurrences += 1;
        if self.buffered.len() >= self.run_records {
            self.spill()?;
        }
        Ok(())
    }

    fn spill(&mut self) -> Result<(), BoxError> {
        if self.buffered.is_empty() {
            return Ok(());
        }
        self.buffered.sort_unstable();
        let path = self
            .dir
            .join(format!("{}-{}.run", self.tag, self.runs.len()));
        let mut out = BufWriter::new(File::create(&path)?);
        for record in &self.buffered {
            out.write_all(record)?;
        }
        out.flush()?;
        self.buffered.clear();
        self.runs.push(path);
        Ok(())
    }

    /// Merges the runs and counts each script's shards.
    ///
    /// Consumes the counter and removes its spill files, so a census leaves
    /// nothing behind whatever it was asked to measure.
    pub fn finish(mut self) -> Result<Distribution, BoxError> {
        self.spill()?;
        let mut readers: Vec<RunReader> = self
            .runs
            .iter()
            .map(|path| RunReader::open(path))
            .collect::<Result<_, _>>()?;

        // A min-heap over the runs' fronts. Every occurrence of one script is
        // adjacent in the merged order, so a group ends at the first record
        // that differs and no script has to be held.
        let mut heap: BinaryHeap<std::cmp::Reverse<([u8; RECORD], usize)>> = BinaryHeap::new();
        for (index, reader) in readers.iter_mut().enumerate() {
            if let Some(record) = reader.next()? {
                heap.push(std::cmp::Reverse((record, index)));
            }
        }

        let mut counts: std::collections::BTreeMap<u64, u64> = Default::default();
        let mut scripts: Vec<Script> = Vec::new();
        // Grouping is on the key alone. Two occurrences of one script differ in
        // their fragment tails, so comparing whole records would split a script
        // into one group per distinct history length it ever had.
        let mut current: Option<([u8; KEY], Script)> = None;
        while let Some(std::cmp::Reverse((record, index))) = heap.pop() {
            let key: [u8; KEY] = record[..KEY].try_into().expect("the key is a prefix");
            let fragments =
                u32::from_le_bytes(record[KEY..].try_into().expect("four trailing bytes")) as u64;
            match &mut current {
                Some((held, run)) if *held == key => {
                    run.shards += 1;
                    run.fragments += fragments;
                }
                other => {
                    if let Some((_, run)) = other.take() {
                        *counts.entry(run.shards).or_default() += 1;
                        scripts.push(run);
                    }
                    *other = Some((
                        key,
                        Script {
                            shards: 1,
                            fragments,
                        },
                    ));
                }
            }
            if let Some(next) = readers[index].next()? {
                heap.push(std::cmp::Reverse((next, index)));
            }
        }
        if let Some((_, run)) = current {
            *counts.entry(run.shards).or_default() += 1;
            scripts.push(run);
        }

        for path in &self.runs {
            let _ = std::fs::remove_file(path);
        }
        Ok(Distribution {
            counts,
            scripts,
            occurrences: self.occurrences,
        })
    }
}

struct RunReader {
    reader: BufReader<File>,
}

impl RunReader {
    fn open(path: &Path) -> Result<Self, BoxError> {
        Ok(Self {
            reader: BufReader::new(File::open(path)?),
        })
    }

    fn next(&mut self) -> Result<Option<[u8; RECORD]>, BoxError> {
        let mut record = [0u8; RECORD];
        match self.reader.read_exact(&mut record) {
            Ok(()) => Ok(Some(record)),
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
            Err(error) => Err(error.into()),
        }
    }
}

/// What one script costs a wallet to restore.
#[derive(Clone, Copy, Debug)]
pub struct Script {
    /// Shards it appears in. Two directory queries each.
    pub shards: u64,
    /// Page fragments across those shards. One query each.
    pub fragments: u64,
}

/// How many shards scripts appear in, and what retrieving them costs.
pub struct Distribution {
    /// Shards appeared in, to scripts that appeared in that many.
    counts: std::collections::BTreeMap<u64, u64>,
    /// One entry per distinct script. Held because the cost question is about
    /// the tail — a mean over a distribution whose maximum is a thousandfold
    /// its median describes nobody.
    scripts: Vec<Script>,
    occurrences: u64,
}

impl Distribution {
    pub fn scripts(&self) -> u64 {
        self.counts.values().sum()
    }

    fn percentile(&self, p: f64) -> u64 {
        let scripts = self.scripts();
        if scripts == 0 {
            return 0;
        }
        let target = ((p / 100.0) * scripts as f64).ceil().max(1.0) as u64;
        let mut seen = 0u64;
        for (g, count) in &self.counts {
            seen += count;
            if seen >= target {
                return *g;
            }
        }
        self.counts.keys().next_back().copied().unwrap_or(0)
    }

    /// Scripts that appeared in exactly `g` shards, for tests and reporting.
    pub fn scripts_matching(&self, g: u64) -> u64 {
        self.counts.get(&g).copied().unwrap_or(0)
    }

    /// What restoring each script costs in private query bytes, as a
    /// distribution.
    ///
    /// Two directory queries per shard the script appears in, one page query
    /// per fragment, priced at the geometry's own query sizes. This is the term
    /// a geometry decision moves: filters and the map are paid whatever the
    /// shard boundaries are, and setup is a per-shard constant an order of
    /// magnitude below a query.
    ///
    /// It prices a wallet that restores the whole journal for one script. A
    /// real wallet holds many scripts and most of them are never used, so this
    /// is the cost of an *active* script rather than of a wallet — which is the
    /// comparison a geometry has to win, because the inactive ones cost the
    /// same under every geometry.
    pub fn cost_bytes(&self, directory_query: u64, page_query: u64) -> Vec<u64> {
        let mut costs: Vec<u64> = self
            .scripts
            .iter()
            .map(|script| script.shards * 2 * directory_query + script.fragments * page_query)
            .collect();
        costs.sort_unstable();
        costs
    }

    /// Fragments across every script, which is the page-query total.
    pub fn fragments(&self) -> u64 {
        self.scripts.iter().map(|script| script.fragments).sum()
    }

    /// Prints the distribution and the directory queries it implies.
    ///
    /// The mean is reported alongside the tail rather than instead of it: a
    /// reused address that appears in every shard is one script in the count and
    /// a restoration cost nothing like the mean.
    pub fn report(&self) {
        let scripts = self.scripts();
        if scripts == 0 {
            return;
        }
        let mean = self.occurrences as f64 / scripts as f64;
        let max = self.counts.keys().next_back().copied().unwrap_or(0);
        println!("  shard matches per script (over {scripts} distinct indexable scripts)");
        println!(
            "    g            mean {mean:>7.2}  p50 {:>5}  p90 {:>5}  p95 {:>5}  p99 {:>5}  max {max:>6}",
            self.percentile(50.0),
            self.percentile(90.0),
            self.percentile(95.0),
            self.percentile(99.0),
        );
        // Two candidate rows, both always queried, once per matched shard. This
        // is the term a wider shard buys down, and the reason shard count and
        // not shard size is what a restoring wallet pays for.
        println!(
            "    2g queries   mean {:>7.2}  p50 {:>5}  p90 {:>5}  p95 {:>5}  p99 {:>5}  max {:>6}",
            mean * 2.0,
            self.percentile(50.0) * 2,
            self.percentile(90.0) * 2,
            self.percentile(95.0) * 2,
            self.percentile(99.0) * 2,
            max * 2,
        );
        println!(
            "    occurrences  {} script-shard pairs across the journal",
            self.occurrences
        );
    }

    /// Prints what an active script pays to restore, at these query sizes.
    pub fn report_cost(&self, directory_query: u64, page_query: u64) {
        let costs = self.cost_bytes(directory_query, page_query);
        if costs.is_empty() {
            return;
        }
        let mb = |bytes: u64| bytes as f64 / 1e6;
        let at = |p: f64| {
            let index = ((p / 100.0) * costs.len() as f64).ceil().max(1.0) as usize - 1;
            costs[index.min(costs.len() - 1)]
        };
        let total: u128 = costs.iter().map(|c| *c as u128).sum();
        println!(
            "  restoration cost per active script (queries only, {directory_query} B directory, \
{page_query} B page)"
        );
        println!(
            "    MB           mean {:7.2}  p50 {:7.2}  p90 {:7.2}  p95 {:7.2}  p99 {:8.2}  max {:9.2}",
            total as f64 / costs.len() as f64 / 1e6,
            mb(at(50.0)),
            mb(at(90.0)),
            mb(at(95.0)),
            mb(at(99.0)),
            mb(*costs.last().expect("nonempty")),
        );
        println!(
            "    fragments    {} across all scripts, against {} script-shard pairs",
            self.fragments(),
            self.occurrences,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn script(tag: u8, len: usize) -> Vec<u8> {
        vec![tag; len]
    }

    fn count(occurrences: &[Vec<u8>], run_records: usize) -> Distribution {
        count_with(
            &occurrences
                .iter()
                .map(|s| (s.clone(), 0u32))
                .collect::<Vec<_>>(),
            run_records,
        )
    }

    fn count_with(occurrences: &[(Vec<u8>, u32)], run_records: usize) -> Distribution {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut counter = MatchCounter::with_run_records(dir.path(), "test", run_records);
        for (script, fragments) in occurrences {
            counter.record(script, *fragments).expect("recorded");
        }
        let distribution = counter.finish().expect("merged");
        // Nothing is left behind: the merge removes what it read.
        assert_eq!(
            std::fs::read_dir(dir.path()).expect("readable").count(),
            0,
            "spill files should be removed"
        );
        distribution
    }

    /// The merge must give the same answer however many runs it came from. One
    /// run exercises the sort alone; a run per record exercises the heap with
    /// every group split across every file, which is where an off-by-one in the
    /// grouping would show.
    #[test]
    fn a_script_is_counted_once_per_shard_it_appears_in() {
        // a in three shards, b in two, c in one.
        let occurrences: Vec<Vec<u8>> = vec![
            script(0xa, 25),
            script(0xb, 25),
            script(0xa, 25),
            script(0xc, 25),
            script(0xb, 25),
            script(0xa, 25),
        ];
        for run_records in [1usize, 2, 5, 1_000] {
            let d = count(&occurrences, run_records);
            assert_eq!(d.scripts(), 3, "run_records {run_records}");
            assert_eq!(d.scripts_matching(3), 1, "run_records {run_records}");
            assert_eq!(d.scripts_matching(2), 1, "run_records {run_records}");
            assert_eq!(d.scripts_matching(1), 1, "run_records {run_records}");
            // The median of {1, 2, 3} shards, read off the histogram rather
            // than a materialised list.
            assert_eq!(d.percentile(50.0), 2);
            assert_eq!(d.percentile(100.0), 3);
        }
    }

    /// Two scripts that pad to the same bytes are two scripts. Without the
    /// length in the record they would merge, and a script would be counted
    /// together with anything that happened to be a prefix of it.
    #[test]
    fn padding_does_not_merge_two_different_scripts() {
        let short = vec![0x11, 0x22];
        let mut long = short.clone();
        long.push(0x00);
        let d = count(&[short.clone(), long, short], 1);
        assert_eq!(d.scripts(), 2);
        assert_eq!(d.scripts_matching(2), 1);
        assert_eq!(d.scripts_matching(1), 1);
    }

    /// A script too long for a directory entry is never placed, so it never
    /// costs a directory query and must not appear in this distribution.
    #[test]
    fn a_script_outside_directory_coverage_is_not_counted() {
        let d = count(
            &[script(0xd, SCRIPT_BYTES + 1), script(0xe, SCRIPT_BYTES)],
            1,
        );
        assert_eq!(d.scripts(), 1, "only the indexable script is counted");
    }

    /// A script's fragments accumulate across its shards while its identity
    /// stays one group. Grouping on the whole record instead would split a
    /// script into one group per history length it happened to have, which
    /// reads as several scripts each appearing once.
    #[test]
    fn fragments_accumulate_without_splitting_the_script() {
        let a = script(0xa, 25);
        let b = script(0xb, 25);
        let d = count_with(
            &[
                (a.clone(), 3),
                (b.clone(), 0),
                (a.clone(), 0),
                (a.clone(), 7),
                (b.clone(), 2),
            ],
            1,
        );
        assert_eq!(d.scripts(), 2, "two scripts, whatever their histories");
        assert_eq!(d.fragments(), 12);

        // a: 3 shards, 10 fragments. b: 2 shards, 2 fragments.
        let costs = d.cost_bytes(100, 10);
        assert_eq!(costs, vec![2 * 2 * 100 + 2 * 10, 3 * 2 * 100 + 10 * 10]);
    }

    /// A script inside the inline allowance everywhere costs page queries
    /// nowhere, and is still counted.
    #[test]
    fn a_wholly_inline_script_costs_only_directory_queries() {
        let d = count_with(&[(script(0xc, 25), 0), (script(0xc, 25), 0)], 1);
        assert_eq!(d.fragments(), 0);
        assert_eq!(d.cost_bytes(100, 10), vec![2 * 2 * 100]);
    }

    #[test]
    fn an_empty_journal_has_no_distribution() {
        let d = count(&[], 1);
        assert_eq!(d.scripts(), 0);
        assert_eq!(d.percentile(50.0), 0);
    }
}
