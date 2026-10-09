//! Derives the tier boundary from the pinned anchor's chain time.
//!
//! The deployment target divides the chain into an archive tier and a recent
//! tier at "six calendar months before the anchor". A height standing in for
//! that interval would drift with block rate, and re-deriving it at every
//! publish would re-shard the chain, so the boundary is computed once, from
//! the anchor block's header time, and the resulting height is recorded. A
//! later publish passes the recorded height back through `--expect-cutoff`,
//! which re-derives and refuses a disagreement rather than trusting a typed
//! number.
//!
//! # The rule
//!
//! `cutoff_time` is the anchor's header time minus `--months` calendar months,
//! with the day of month clamped to the target month's last day (so 31 August
//! minus six months is 28 or 29 February) and the time of day kept.
//!
//! `cutoff_height` is one more than the highest height whose header time is
//! before `cutoff_time`. Block times are not monotone — consensus only
//! requires a block's time to exceed the median of the eleven before it — so
//! the first crossing found by binary search is only a first estimate; the
//! tool scans a window past it for later blocks that still fall before the
//! cutoff time. The definition makes the boundary unambiguous: every block at
//! or after `cutoff_height` has a header time at or after `cutoff_time`, and
//! the block just before it does not.
//!
//! That "every block after" claim is proved, not assumed. Once eleven
//! consecutive blocks at or after the cutoff all carry times at or after
//! `cutoff_time`, their median does too, and every later block must exceed
//! that median. The tool checks those eleven inside the scanned window.
//!
//! The recorded boundary is not that calendar height but the start of the
//! archive shard still open there. The tool replays the journal from its first
//! height through the archive sealer under `--archive-geometry`'s thresholds,
//! exactly as the publisher does, and stops before the calendar height. If a
//! shard is still accumulating, the boundary moves back to its first height;
//! if the last block sealed one, the calendar height already is a boundary.
//! Either way every archive shard is sealed by a threshold, never cut short by
//! the geometry change, and the recent tier starts at or before the calendar
//! height, so it still covers at least `--months`. A boundary that would leave
//! the archive tier empty is refused.
//!
//! # Sources
//!
//! Header times come from the node, since the journal stores block hashes but
//! not times: from its RocksDB when `--state-dir` is given, otherwise from
//! `getblockheader` over RPC. Hashes at the boundary and the anchor are
//! checked against the journal, so the record names the chain the journal
//! actually holds.

use chrono::{DateTime, Months, Utc};
use clap::Parser;
use std::collections::BTreeMap;
use std::path::PathBuf;
use transparent_events::TransparentEvent;
use transparent_filter::ScriptBytes;
use transparent_filter_server::events::EventStore;
use transparent_filter_server::state::StateReader;
use transparent_filter_server::zakura::ZakuraClient;
use transparent_shard::layout::{by_name as geometry_by_name, Geometry};
use transparent_shard::seal::{PageBasis, SealPolicy, Sealer};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Parser)]
#[command(
    name = "shard-cutoff",
    about = "Derive and record the archive/recent tier boundary from the anchor's chain time"
)]
struct Cli {
    #[arg(long, default_value = "./transparent-event-data")]
    data_dir: PathBuf,
    /// Anchor height. Defaults to the journal's committed end.
    #[arg(long)]
    anchor_height: Option<u64>,
    /// Calendar months between the cutoff and the anchor.
    #[arg(long, default_value_t = 6)]
    months: u32,
    /// Archive geometry the publication will use; its seal thresholds decide
    /// where the archive's last shard ends.
    #[arg(long, default_value = "archive-wide")]
    archive_geometry: String,
    /// The node's `[state] cache_dir`; reads header times from its RocksDB.
    #[arg(long)]
    state_dir: Option<PathBuf>,
    #[arg(long, default_value = "http://127.0.0.1:8232")]
    zakura_rpc_url: String,
    /// RPC cookie, used when `--state-dir` is not given.
    #[arg(long)]
    zakura_cookie: Option<PathBuf>,
    /// A previously recorded cutoff height. The derivation must reproduce it.
    #[arg(long)]
    expect_cutoff: Option<u64>,
    /// Heights scanned past the first crossing for non-monotone times.
    #[arg(long, default_value_t = 2_000)]
    window: u64,
    /// Where to write the JSON record; stdout when omitted.
    #[arg(long)]
    out: Option<PathBuf>,
    /// Source commit of this tool, recorded verbatim.
    #[arg(long)]
    source_sha: Option<String>,
}

/// Header times and hashes by height, whichever node interface answers.
trait Headers {
    fn time(&mut self, height: u64) -> Result<DateTime<Utc>, BoxError>;
    fn hash(&mut self, height: u64) -> Result<String, BoxError>;
    fn label(&self) -> &'static str;
}

struct StateHeaders(StateReader);

impl Headers for StateHeaders {
    fn time(&mut self, height: u64) -> Result<DateTime<Utc>, BoxError> {
        Ok(self.0.block_time(height)?)
    }
    fn hash(&mut self, height: u64) -> Result<String, BoxError> {
        Ok(self.0.block_hash(height)?)
    }
    fn label(&self) -> &'static str {
        "state"
    }
}

struct RpcHeaders {
    runtime: tokio::runtime::Runtime,
    client: ZakuraClient,
}

impl Headers for RpcHeaders {
    fn time(&mut self, height: u64) -> Result<DateTime<Utc>, BoxError> {
        let hash = self.hash(height)?;
        let header = self.runtime.block_on(
            self.client
                .call_value("getblockheader", serde_json::json!([hash, true])),
        )?;
        let seconds = header
            .get("time")
            .and_then(serde_json::Value::as_i64)
            .ok_or_else(|| format!("getblockheader at {height} carries no integer time"))?;
        DateTime::<Utc>::from_timestamp(seconds, 0)
            .ok_or_else(|| format!("header time {seconds} at {height} is out of range").into())
    }
    fn hash(&mut self, height: u64) -> Result<String, BoxError> {
        Ok(self.runtime.block_on(self.client.block_hash(height))?)
    }
    fn label(&self) -> &'static str {
        "rpc"
    }
}

/// Memoises header times, so the window scan and the proof reuse the search's reads.
struct Cached<'a> {
    inner: &'a mut dyn Headers,
    times: BTreeMap<u64, DateTime<Utc>>,
}

impl Cached<'_> {
    fn time(&mut self, height: u64) -> Result<DateTime<Utc>, BoxError> {
        if let Some(time) = self.times.get(&height) {
            return Ok(*time);
        }
        let time = self.inner.time(height)?;
        self.times.insert(height, time);
        Ok(time)
    }
}

/// What the derivation found, before hashes are attached.
#[derive(Debug, PartialEq, Eq)]
struct Derived {
    /// First height whose time is at or after the cutoff time, by binary search.
    first_at_or_after: u64,
    /// Highest height whose time is before the cutoff time.
    last_before: u64,
    /// `last_before + 1`.
    cutoff_height: u64,
    /// Inclusive range whose times were read one by one.
    scanned: (u64, u64),
}

/// The rule, over any time source.
///
/// `times` must answer for every height in `start..=anchor`. Errors name what
/// the caller should change: a journal that begins after the cutoff, an anchor
/// before it, or a window too small to contain the non-monotone tail.
fn derive(
    times: &mut impl FnMut(u64) -> Result<DateTime<Utc>, BoxError>,
    start: u64,
    anchor: u64,
    cutoff_time: DateTime<Utc>,
    window: u64,
) -> Result<Derived, BoxError> {
    if anchor <= start {
        return Err(format!("anchor {anchor} does not lie after the journal start {start}").into());
    }
    if times(anchor)? < cutoff_time {
        return Err(
            format!("the anchor's own time is before the cutoff time {cutoff_time}").into(),
        );
    }
    if times(start)? >= cutoff_time {
        return Err(format!(
            "the journal's first block {start} is already at or after {cutoff_time}; \
             the archive tier would be empty"
        )
        .into());
    }
    // Invariant: times(lo) < cutoff_time <= times(hi).
    let (mut lo, mut hi) = (start, anchor);
    while hi - lo > 1 {
        let mid = lo + (hi - lo) / 2;
        if times(mid)? >= cutoff_time {
            hi = mid;
        } else {
            lo = mid;
        }
    }
    let first_at_or_after = hi;
    let scan_end = anchor.min(first_at_or_after.saturating_add(window));
    let mut last_before = lo;
    for height in first_at_or_after..=scan_end {
        if times(height)? < cutoff_time {
            last_before = height;
        }
    }
    let cutoff_height = last_before + 1;
    // Proof that nothing later can fall before the cutoff time: eleven
    // consecutive blocks at or after it, all inside the scanned range, all at
    // or after the cutoff time. Their median is then at or after it, and
    // consensus makes every later block exceed that median.
    let proof_end = cutoff_height + 10;
    if proof_end > anchor {
        return Err(format!(
            "only {} blocks lie at or after the cutoff {cutoff_height}; the recent tier \
             needs at least eleven for the boundary to be provable",
            anchor - cutoff_height + 1
        )
        .into());
    }
    if proof_end > scan_end {
        return Err(format!(
            "times before the cutoff continue to {last_before}, within {} of the window's end; \
             widen --window beyond {window}",
            scan_end - last_before
        )
        .into());
    }
    Ok(Derived {
        first_at_or_after,
        last_before,
        cutoff_height,
        scanned: (first_at_or_after, scan_end),
    })
}

/// Moves `calendar` back to the first height of the archive shard open there.
///
/// Offers `start..calendar` to an archive sealer, as the publisher would, and
/// returns the boundary with the number of archive shards before it.
fn seal_back(
    events: &mut impl FnMut(u64) -> Result<Vec<(ScriptBytes, TransparentEvent)>, BoxError>,
    archive: &Geometry,
    start: u64,
    calendar: u64,
) -> Result<(u64, u64), BoxError> {
    let mut sealer = Sealer::resume(
        SealPolicy::for_geometry(archive),
        start,
        PageBasis::default(),
        *archive,
        0,
    );
    for height in start..calendar {
        sealer.push_block(height, &events(height)?)?;
    }
    let boundary = sealer.open_start().unwrap_or(calendar);
    if boundary == start {
        return Err(format!(
            "the first {} shard is still open at the calendar cutoff {calendar}; \
             the archive tier would be empty",
            archive.name
        )
        .into());
    }
    Ok((boundary, sealer.next_shard_id()))
}

/// The anchor time minus `months` calendar months, day clamped to the month.
fn cutoff_time(anchor_time: DateTime<Utc>, months: u32) -> Result<DateTime<Utc>, BoxError> {
    anchor_time
        .checked_sub_months(Months::new(months))
        .ok_or_else(|| format!("{anchor_time} minus {months} months is out of range").into())
}

fn main() -> Result<(), BoxError> {
    let cli = Cli::parse();
    let store = EventStore::open_existing(&cli.data_dir)?;
    let start = store.start_height();
    let covered = store.covered_through().ok_or("the journal is empty")?;
    let anchor = cli.anchor_height.unwrap_or(covered);
    if anchor > covered || anchor < start {
        return Err(format!("anchor {anchor} is outside the journal's {start}-{covered}").into());
    }
    if cli.months == 0 {
        return Err("--months must be at least one".into());
    }
    let archive = geometry_by_name(&cli.archive_geometry)
        .ok_or_else(|| format!("unknown geometry {:?}", cli.archive_geometry))?;

    let mut headers: Box<dyn Headers> = match (&cli.state_dir, &cli.zakura_cookie) {
        (Some(dir), _) => Box::new(StateHeaders(StateReader::open(
            dir,
            zakura_chain::parameters::Network::Mainnet,
        )?)),
        (None, Some(cookie)) => Box::new(RpcHeaders {
            runtime: tokio::runtime::Runtime::new()?,
            client: ZakuraClient::from_cookie_file(&cli.zakura_rpc_url, cookie)?,
        }),
        (None, None) => return Err("give --state-dir or --zakura-cookie".into()),
    };

    // The chain the source describes must be the chain the journal holds.
    let source_genesis = headers.hash(0)?;
    if source_genesis != store.genesis_hash() {
        return Err(format!(
            "the {} source is on genesis {source_genesis}, the journal on {}",
            headers.label(),
            store.genesis_hash()
        )
        .into());
    }
    let journal_hash = |height: u64| -> Result<String, BoxError> {
        store
            .block_at(height)
            .map(|entry| entry.block_hash.to_display_hex())
            .ok_or_else(|| format!("height {height} is missing from the journal").into())
    };
    let anchor_hash = headers.hash(anchor)?;
    if anchor_hash != journal_hash(anchor)? {
        return Err(format!(
            "anchor {anchor} is {anchor_hash} at the node but {} in the journal",
            journal_hash(anchor)?
        )
        .into());
    }

    let mut cached = Cached {
        inner: headers.as_mut(),
        times: BTreeMap::new(),
    };
    let anchor_time = cached.time(anchor)?;
    let cutoff_at = cutoff_time(anchor_time, cli.months)?;
    let derived = derive(
        &mut |height| cached.time(height),
        start,
        anchor,
        cutoff_at,
        cli.window,
    )?;
    let calendar = derived.cutoff_height;
    eprintln!(
        "calendar cutoff {calendar}; replaying {start}-{} through the {} sealer",
        calendar - 1,
        archive.name
    );
    let (cutoff, archive_shards) = seal_back(
        &mut |height| {
            store
                .events_at(height)?
                .ok_or_else(|| format!("height {height} is missing from the journal").into())
        },
        archive,
        start,
        calendar,
    )?;
    if let Some(expected) = cli.expect_cutoff {
        if expected != cutoff {
            return Err(format!(
                "the recorded cutoff {expected} is not what the rule derives ({cutoff}); \
                 do not publish under a boundary the anchor no longer justifies"
            )
            .into());
        }
    }

    let cutoff_hash = cached.inner.hash(cutoff)?;
    let previous_hash = cached.inner.hash(cutoff - 1)?;
    for (height, hash) in [(cutoff, &cutoff_hash), (cutoff - 1, &previous_hash)] {
        let in_journal = journal_hash(height)?;
        if *hash != in_journal {
            return Err(format!(
                "height {height} is {hash} at the node but {in_journal} in the journal"
            )
            .into());
        }
    }
    let cutoff_block_time = cached.time(cutoff)?;
    let previous_block_time = cached.time(cutoff - 1)?;

    let record = serde_json::json!({
        "schema": "transparent-cutoff-v2",
        "generated_at": Utc::now().to_rfc3339(),
        "tool_sha": cli.source_sha,
        "source": cached.inner.label(),
        "network": transparent_filter::NETWORK,
        "genesis_hash": store.genesis_hash(),
        "journal": {
            "data_dir": cli.data_dir,
            "start_height": start,
            "covered_through": covered,
        },
        "anchor": {
            "height": anchor,
            "hash": anchor_hash,
            "time": anchor_time.to_rfc3339(),
        },
        "rule": {
            "months": cli.months,
            "description": "cutoff_time = anchor header time minus the calendar months, \
                            day clamped to the target month's last day, time of day kept; \
                            cutoff_height = 1 + the highest height whose header time is \
                            before cutoff_time; proved final by eleven consecutive blocks \
                            at or after cutoff_time from cutoff_height; the boundary is the \
                            first height of the archive shard still open at cutoff_height \
                            under the archive geometry's seal thresholds, or cutoff_height \
                            when none is",
            "window": cli.window,
            "archive_geometry": archive.name,
        },
        "cutoff": {
            "time": cutoff_at.to_rfc3339(),
            "height": cutoff,
            "hash": cutoff_hash,
            "block_time": cutoff_block_time.to_rfc3339(),
            "previous_height": cutoff - 1,
            "previous_hash": previous_hash,
            "previous_block_time": previous_block_time.to_rfc3339(),
            "calendar_height": calendar,
            "archive_shards": archive_shards,
            "first_height_at_or_after_cutoff_time": derived.first_at_or_after,
            "last_height_before_cutoff_time": derived.last_before,
            "scanned": [derived.scanned.0, derived.scanned.1],
        },
        "tiers": {
            "archive": [start, cutoff - 1],
            "recent": [cutoff, anchor],
        },
    });
    let bytes = serde_json::to_vec_pretty(&record)?;
    match &cli.out {
        Some(path) => {
            std::fs::write(path, &bytes)?;
            eprintln!(
                "anchor {anchor} at {anchor_time}; cutoff time {cutoff_at}; calendar height \
                 {calendar}; cutoff height {cutoff} (archive {start}-{} in {archive_shards} \
                 shards, recent {cutoff}-{anchor}); written to {}",
                cutoff - 1,
                path.display()
            );
        }
        None => println!("{}", String::from_utf8(bytes)?),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn at(seconds: i64) -> DateTime<Utc> {
        DateTime::<Utc>::from_timestamp(seconds, 0).unwrap()
    }

    /// Times 100 s apart, with one block after the crossing stamped early.
    fn non_monotone(height: u64) -> Result<DateTime<Utc>, BoxError> {
        let base = 100 * height as i64;
        Ok(at(match height {
            // The first crossing is at 50 (time 5,000). Block 53 is stamped
            // back before it, which consensus permits.
            53 => 4_900,
            _ => base,
        }))
    }

    #[test]
    fn the_cutoff_is_past_the_last_early_stamped_block_not_the_first_crossing() {
        let derived = derive(&mut non_monotone, 0, 200, at(5_000), 20).unwrap();
        assert_eq!(derived.first_at_or_after, 50);
        assert_eq!(derived.last_before, 53);
        assert_eq!(derived.cutoff_height, 54);
    }

    #[test]
    fn monotone_times_put_the_cutoff_at_the_first_crossing() {
        let mut times = |height: u64| Ok(at(100 * height as i64));
        let derived = derive(&mut times, 0, 200, at(5_000), 20).unwrap();
        assert_eq!(derived.cutoff_height, 50);
        // A cutoff time between two blocks lands on the later one.
        let derived = derive(&mut times, 0, 200, at(5_001), 20).unwrap();
        assert_eq!(derived.cutoff_height, 51);
    }

    #[test]
    fn a_window_too_small_to_prove_the_boundary_is_refused() {
        // The early-stamped block is at the window's end, so the eleven-block
        // proof would run past what was scanned.
        let error = derive(&mut non_monotone, 0, 200, at(5_000), 3).unwrap_err();
        assert!(error.to_string().contains("widen --window"), "{error}");
    }

    #[test]
    fn a_journal_that_begins_after_the_cutoff_time_is_refused() {
        let mut times = |height: u64| Ok(at(100 * height as i64));
        let error = derive(&mut times, 60, 200, at(5_000), 20).unwrap_err();
        assert!(
            error.to_string().contains("archive tier would be empty"),
            "{error}"
        );
    }

    #[test]
    fn an_archive_shard_still_open_from_the_start_is_refused() {
        // Empty blocks reach no threshold, so the first archive shard is still
        // open at the calendar height and moving back would empty the tier.
        let archive = geometry_by_name("archive-wide").unwrap();
        let error = seal_back(&mut |_| Ok(Vec::new()), archive, 0, 100).unwrap_err();
        assert!(
            error.to_string().contains("archive tier would be empty"),
            "{error}"
        );
    }

    #[test]
    fn the_calendar_rule_clamps_to_the_end_of_the_month() {
        let anchor = Utc.with_ymd_and_hms(2026, 8, 31, 13, 45, 7).unwrap();
        let cutoff = cutoff_time(anchor, 6).unwrap();
        assert_eq!(
            cutoff,
            Utc.with_ymd_and_hms(2026, 2, 28, 13, 45, 7).unwrap()
        );
        let anchor = Utc.with_ymd_and_hms(2028, 8, 31, 0, 0, 0).unwrap();
        assert_eq!(
            cutoff_time(anchor, 6).unwrap(),
            Utc.with_ymd_and_hms(2028, 2, 29, 0, 0, 0).unwrap()
        );
        let anchor = Utc.with_ymd_and_hms(2026, 9, 6, 23, 59, 59).unwrap();
        assert_eq!(
            cutoff_time(anchor, 6).unwrap(),
            Utc.with_ymd_and_hms(2026, 3, 6, 23, 59, 59).unwrap()
        );
    }
}
