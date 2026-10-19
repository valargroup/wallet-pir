//! NEAR Intents explorer feed for the receiver directory's recent and seen filters
//! (`receiver_directory::filter`). It reads every swap into or out of ZEC, newest first,
//! whichever app created it, and records the Orchard receiver of each payout
//! (`recipient`) and refund (`refundTo`) address with the swap's creation time. Unfunded
//! quotes count: they hold an address until their deadline. The explorer allows one
//! request every five seconds per partner key, which this module never logs.
//!
//! Each feed is read two ways. Live reads ([`Explorer::sync`]) follow the newest swaps
//! from a cursor and keep the recent set fresh, reaching back at most a day. History
//! sweeps ([`Explorer::sweep`]) walk the feed from its newest swap down to a floor a few
//! pages a poll, recording each page, so they resume after a failure or restart however
//! long the history is. A full pass backfills to `--near-since` and rereads the whole
//! history daily, and an hourly recent pass rereads the last week, so swaps listed late,
//! corrected or skipped by a read are recorded. The seen set is published only once that
//! history reaches the live reads without a gap.
use receiver_directory::{
    filter::{RECENT, SEEN},
    snapshot::ProviderSet,
    ReceiverBytes,
};
use receiver_store::{Pass, ProviderStore, Store, Token};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    path::Path,
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
    time::Duration,
};
use zakura_receiver_crypto::{Network, Receiver};

const ENDPOINT: &str = "https://explorer.near-intents.org/api/v0/transactions";
/// Every status the explorer's `statuses` filter accepts (its OpenAPI spec). The 1Click
/// status API's `KNOWN_DEPOSIT_TX` is not one: the explorer rejects the request.
const STATUSES: &str = "FAILED,INCOMPLETE_DEPOSIT,PENDING_DEPOSIT,PROCESSING,REFUNDED,SUCCESS";
/// The status of a completed swap, whose payout transactions the explorer reports.
const SUCCESS: &str = "SUCCESS";
const PAGE: usize = 1000;
/// Bound on one page's body: a record is about 1 KB, so eight times a full page.
const MAX_PAGE_BYTES: usize = 8 * 1024 * 1024;
/// The explorer's per-partner rate limit, with margin.
const REQUEST_INTERVAL: Duration = Duration::from_millis(5_500);
/// How far back a live read starts before the cursor, for swaps the explorer lists late.
/// A payout read also rescans successful swaps back [`RECENT_SECS`] before the cursor,
/// so it sees a payout complete up to a day after its swap was created. Swaps are
/// refunded at their deadline, so later completions are out of scope.
const OVERLAP_SECS: i64 = 3600;
/// Bound on one live read, so an explorer that ignores paging cannot loop forever. A
/// live read reaches back at most [`RECENT_SECS`], far inside it. History comes from
/// sweeps, which are not bounded by it.
const MAX_PAGES: usize = 500;
/// Pages one poll reads of each feed's sweeps by default (`--near-sweep-pages`), about
/// three minutes at the request interval, so live reads keep the recent set within
/// wallets' 15-minute freshness.
pub const SWEEP_PAGES: usize = 30;
/// How long after the history's end the next recent pass begins.
const RECONCILE_INTERVAL_SECS: i64 = 60 * 60;
/// How far a recent pass reaches back before the history's end. A swap the explorer
/// lists or corrects later than this after its creation waits for the full pass.
const RECONCILE_LOOKBACK_SECS: i64 = 7 * 24 * 60 * 60;
/// How long after the last full pass began the next one begins.
const FULL_INTERVAL_SECS: i64 = 24 * 60 * 60;
/// The full pass duration health's capacity measures against (`full_pass_secs`). A
/// correction older than a week waits up to about twice the duration, and the
/// explorer's request limit caps how many pages any budget reads a day.
pub const MAX_FULL_PASS_SECS: u64 = 2 * 24 * 60 * 60;
/// The provider name in the directory's filter set labels.
pub const PROVIDER: &str = "near-intents";
/// How far back the recent set reaches from the feeds' last complete read.
pub const RECENT_SECS: i64 = 24 * 60 * 60;
/// How long before a publication's terminal block's time a read must first have seen
/// a payout complete for the report to check its payment. The chain's clock, capped at
/// the wall clock, measures it, so payouts stay pending, not missing, while the chain
/// pauses or behind `--depth`.
const COMPLETION_GRACE_SECS: i64 = 60 * 60;
/// A completed payout to an Orchard receiver that NEAR reported without a parsable
/// transaction is recorded under [`uncheckable_receiver`] and this many zero bytes,
/// which no transaction hash starts with, then a digest of its swap's length-prefixed
/// deposit address and optional memo, the explorer's identity for a swap, so one row
/// per swap. [`Capture::report`] counts these (`payouts_uncheckable`) while first seen
/// within [`RECENT_SECS`].
const UNCHECKABLE_ZEROS: usize = 16;

/// Whether `txid` is the record of an uncheckable payout; see [`UNCHECKABLE_ZEROS`].
fn uncheckable(txid: &receiver_directory::Hash) -> bool {
    txid[..UNCHECKABLE_ZEROS] == [0; UNCHECKABLE_ZEROS]
}

/// The receiver of every uncheckable payout record: a fixed valid address (zero
/// diversifier, transmission key with x-coordinate 1) that never enters a filter set.
fn uncheckable_receiver() -> ReceiverBytes {
    let mut bytes = [0; 43];
    bytes[11] = 1;
    *Receiver::from_bytes(bytes)
        .expect("a valid address")
        .as_bytes()
}

/// The NEAR filter sets, read from one state of `store`. Both hold payout and refund
/// receivers, so a feed that lacks what a set needs holds it back, and wallets do not
/// mistake its absence for an empty set. `near-intents/recent` needs both feeds' live
/// coverage: it declares when that began and when their last complete live reads began,
/// the point from which the recent window reaches back. `near-intents/seen` also needs
/// both feeds' history (see [`ProviderStore::history`]) to reach that live coverage
/// without a gap: it declares where the history begins and the same last reads.
pub fn provider_sets(store: &ProviderStore) -> Result<Vec<ProviderSet>> {
    store.view(sets_in)
}

/// See [`provider_sets`].
fn sets_in(store: &ProviderStore) -> Result<Vec<ProviderSet>> {
    let (payouts, refunds) = (Feed::Payouts.name(), Feed::Refunds.name());
    // A feed's live coverage: its start and the beginning of its last complete read.
    let live = |feed| -> Result<_> { Ok(store.started(feed)?.zip(store.read(feed)?)) };
    let Some((since_unix, until_unix)) = common(live(payouts)?, live(refunds)?) else {
        return Ok(Vec::new());
    };
    let (recent, seen) = store.sets(until_unix - RECENT_SECS)?;
    let mut sets = vec![ProviderSet {
        label: format!("{PROVIDER}/{RECENT}"),
        window_secs: Some(RECENT_SECS as u64),
        since_unix,
        until_unix,
        receivers: recent,
    }];
    let history = |feed| -> Result<_> { Ok(store.history(feed)?.map(|h| (h.since, h.through))) };
    let history = common(history(payouts)?, history(refunds)?);
    if let Some((history_since, _)) =
        history.filter(|&(since, through)| through >= since_unix && since <= until_unix)
    {
        sets.push(ProviderSet {
            label: format!("{PROVIDER}/{SEEN}"),
            window_secs: None,
            since_unix: history_since,
            until_unix,
            receivers: seen,
        });
    }
    Ok(sets)
}

/// The span two feeds share, the later start to the earlier end, if both have one and
/// it is not empty.
fn common(a: Option<(i64, i64)>, b: Option<(i64, i64)>) -> Option<(i64, i64)> {
    a.zip(b)
        .map(|(a, b)| (a.0.max(b.0), a.1.min(b.1)))
        .filter(|(since, until)| since <= until)
}

/// A digest of every field of `sets` and their receivers in any order, so a
/// publication built from them can tell whether newer sets differ.
pub fn digest(sets: &[ProviderSet]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(b"receiver-indexer/provider-sets\0");
    h.update((sets.len() as u64).to_le_bytes());
    for set in sets {
        h.update((set.label.len() as u64).to_le_bytes());
        h.update(set.label.as_bytes());
        h.update(set.window_secs.map_or([0; 9], |w| {
            let mut field = [1; 9];
            field[1..].copy_from_slice(&w.to_le_bytes());
            field
        }));
        h.update(set.since_unix.to_le_bytes());
        h.update(set.until_unix.to_le_bytes());
        let mut receivers: Vec<_> = set.receivers.iter().collect();
        receivers.sort_unstable();
        h.update((receivers.len() as u64).to_le_bytes());
        receivers.into_iter().for_each(|r| h.update(r));
    }
    h.finalize().into()
}

/// The provider sets and report inputs read from one state of a [`ProviderStore`], so
/// a publication's filters and its [`Capture::report`] describe the same feed state.
pub struct Capture {
    /// The NEAR filter sets; see [`provider_sets`].
    pub sets: Vec<ProviderSet>,
    /// When each feed's last complete live read began.
    feeds: serde_json::Map<String, serde_json::Value>,
    /// Each feed's history and sweep in progress; see [`Capture::report`].
    history: serde_json::Map<String, serde_json::Value>,
    /// Checkable payouts first seen complete more than [`COMPLETION_GRACE_SECS`]
    /// before the terminal block's time.
    payouts: Vec<(ReceiverBytes, receiver_directory::Hash)>,
    /// Uncheckable payouts first seen in that span but within [`RECENT_SECS`].
    uncheckable: usize,
    /// The longer feed's full pass duration: the last completed one's, or the running
    /// one's so far when that is longer.
    pub full_pass_secs: Option<u64>,
}

/// Reads [`provider_sets`] and the inputs of a report on a publication whose terminal
/// block has time `anchor_time`, in Unix seconds, in one read transaction of `store`.
/// Running sweep duration uses wall-clock `now`, independently of chain progress.
pub fn capture(store: &ProviderStore, anchor_time: i64, now: i64) -> Result<Capture> {
    store.view(|store| {
        let (mut feeds, mut history) = (serde_json::Map::new(), serde_json::Map::new());
        let mut full_pass_secs = None;
        for feed in [Feed::Payouts, Feed::Refunds].map(Feed::name) {
            feeds.insert(feed.into(), store.read(feed)?.into());
            let mut sweeps = serde_json::Map::new();
            for pass in [Pass::Recent, Pass::Full] {
                let sweep = store.sweep(feed, pass)?.map(|s| {
                    serde_json::json!({"floor": s.floor, "began": s.began,
                        "reached": s.reached, "pages": s.pages})
                });
                sweeps.insert(pass.name().into(), sweep.into());
            }
            let span = store.history(feed)?;
            let running = store
                .sweep(feed, Pass::Full)?
                .map(|s| now.saturating_sub(s.began));
            let secs = span.map(|h| h.full_secs).max(running);
            full_pass_secs = full_pass_secs.max(secs.map(|s| s.max(0) as u64));
            history.insert(
                feed.into(),
                serde_json::json!({"since": span.map(|h| h.since),
                    "through": span.map(|h| h.through), "full": span.map(|h| h.full),
                    "full_secs": span.map(|h| h.full_secs), "sweeps": sweeps}),
            );
        }
        // Those first seen by the earlier bound are a subset of those seen by the later.
        let count = |until| -> Result<usize> {
            let payouts = store.payouts(until)?;
            Ok(payouts.iter().filter(|p| uncheckable(&p.1)).count())
        };
        let mut payouts = store.payouts(anchor_time - COMPLETION_GRACE_SECS)?;
        payouts.retain(|p| !uncheckable(&p.1));
        Ok(Capture {
            sets: sets_in(store)?,
            feeds,
            history,
            payouts,
            uncheckable: count(anchor_time - COMPLETION_GRACE_SECS)?
                - count(anchor_time - RECENT_SECS)?,
            full_pass_secs,
        })
    })
}

impl Capture {
    /// The feed's health for monitoring. `feeds` gives when each feed's last complete live
    /// read began, its freshness. `history` gives each feed's completeness apart from
    /// that: the span its completed sweeps cover, `since` to `through`, when the last
    /// full pass began (`full`) and how long it took (`full_secs`), all null until the
    /// first sweep completes, and each pass
    /// in progress (`sweeps`) with its floor, start, the oldest creation time it reached
    /// and its pages. The rest counts how many payouts first
    /// seen complete more than an hour before the terminal block's time, however long
    /// ago, have no payment to their receiver in the transaction NEAR reported in
    /// `index` (`payouts_missing`), out of all of them (`payouts_checked`). A missing
    /// payout means the indexer missed it, or NEAR paid it without the zero OVK, which a
    /// seed restore cannot find. Payouts to an Orchard receiver that NEAR reported
    /// complete without a parsable transaction cannot be looked up;
    /// `payouts_uncheckable` counts those first seen in the same span but within the
    /// last day, so it clears on its own. Payouts to other recipients are not checked.
    pub fn report(&self, index: &Store) -> Result<serde_json::Value> {
        let mut missing = 0;
        for (receiver, txid) in &self.payouts {
            if !index.paid_in(receiver, txid)? {
                missing += 1;
            }
        }
        Ok(serde_json::json!({
            "feeds": self.feeds,
            "history": self.history,
            "payouts_checked": self.payouts.len(),
            "payouts_missing": missing,
            "payouts_uncheckable": self.uncheckable,
        }))
    }
}

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// One direction of the feed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Feed {
    /// Swaps into ZEC, whose `recipient` is a payout address.
    Payouts,
    /// Swaps out of ZEC, whose `refundTo` is a refund address.
    Refunds,
}

impl Feed {
    /// The feed's cursor name in the provider store.
    pub fn name(self) -> &'static str {
        match self {
            Self::Payouts => "near-payouts",
            Self::Refunds => "near-refunds",
        }
    }

    /// The explorer parameter that selects this direction's ZEC swaps.
    fn chain_filter(self) -> &'static str {
        match self {
            Self::Payouts => "toChainId",
            Self::Refunds => "fromChainId",
        }
    }
}

/// This process's evidence that the explorer served its key: for each feed, when the
/// last read this process completed began, in Unix seconds. Health reports it as
/// `near.reads`, null for a feed this process has not read, so without a key both stay
/// null. The report's `feeds` cannot serve as this evidence: they come from the
/// published capture of `providers.sqlite`, so they lag publication and may hold an
/// earlier process's reads.
#[derive(Clone, Default)]
pub struct Reads(Arc<[AtomicI64; 2]>);

impl Reads {
    /// Records that a read of `feed` that began at `began` completed.
    fn record(&self, feed: Feed, began: i64) {
        self.0[feed as usize].store(began, Ordering::SeqCst);
    }

    /// Health's `near` field: `{"reads": {"near-payouts": <began>|null, "near-refunds": ...}}`.
    pub fn health(&self) -> serde_json::Map<String, serde_json::Value> {
        let reads: serde_json::Map<_, _> = [Feed::Payouts, Feed::Refunds]
            .into_iter()
            .map(|feed| {
                let began = self.0[feed as usize].load(Ordering::SeqCst);
                (feed.name().into(), (began != 0).then_some(began).into())
            })
            .collect();
        serde_json::Map::from_iter([("near".into(), serde_json::json!({ "reads": reads }))])
    }
}

/// Reads both feeds once, in turn, into the provider store at `path` (see
/// [`Explorer::sync`]), recording each completed read in `reads`, then advances each
/// feed's history sweep toward `start` (see [`Explorer::sweep`]). A failure is logged
/// and leaves its feed's entry unchanged.
pub async fn poll(explorer: &mut Explorer, path: &Path, start: i64, reads: &Reads) {
    let mut store = match ProviderStore::open(path) {
        Ok(store) => store,
        Err(error) => return tracing::warn!(%error, "near provider store unavailable"),
    };
    for feed in [Feed::Payouts, Feed::Refunds] {
        let began = unix_now();
        match explorer.sync(&mut store, feed).await {
            Ok(receivers) => {
                reads.record(feed, began);
                tracing::info!(feed = feed.name(), receivers, "near feed read")
            }
            Err(error) => tracing::warn!(feed = feed.name(), %error, "near feed deferred"),
        }
    }
    for feed in [Feed::Payouts, Feed::Refunds] {
        match explorer.sweep(&mut store, feed, start).await {
            Ok(0) => {}
            Ok(pages) => tracing::info!(feed = feed.name(), pages, "near history swept"),
            Err(error) => tracing::warn!(feed = feed.name(), %error, "near history deferred"),
        }
    }
}

/// Seconds since the Unix epoch.
pub(crate) fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after 1970")
        .as_secs() as i64
}

/// One explorer record, reduced to what the feed reads. Only the paging fields are
/// required, so a record missing an address or status does not fail the read.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Swap {
    created_at_timestamp: i64,
    deposit_address: String,
    deposit_memo: Option<String>,
    recipient: Option<String>,
    refund_to: Option<String>,
    status: Option<String>,
    /// The payout transactions, displayed (reversed) hex as on ZEC explorers.
    destination_chain_tx_hashes: Option<Vec<String>>,
}

/// Explorer access with a partner key.
pub struct Explorer {
    http: reqwest::Client,
    endpoint: String,
    key: String,
    next_request: tokio::time::Instant,
    /// [`REQUEST_INTERVAL`], except in tests.
    interval: Duration,
    /// [`MAX_PAGES`], except in tests.
    max_pages: usize,
    /// `--near-sweep-pages`.
    sweep_pages: usize,
}

impl Explorer {
    /// An explorer client that authenticates with `key` and reads up to `sweep_pages`
    /// history pages of each feed a poll (see [`Self::sweep`]).
    pub fn new(key: String, sweep_pages: usize) -> Result<Self> {
        Ok(Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(60))
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            endpoint: ENDPOINT.to_owned(),
            key,
            next_request: tokio::time::Instant::now(),
            interval: REQUEST_INTERVAL,
            max_pages: MAX_PAGES,
            sweep_pages,
        })
    }

    /// An explorer client that reads `endpoint` instead of NEAR's, without waiting
    /// between requests.
    #[cfg(test)]
    fn at(endpoint: String) -> Self {
        Self {
            endpoint,
            interval: Duration::ZERO,
            ..Self::new(String::new(), SWEEP_PAGES).unwrap()
        }
    }

    /// Reads `feed` back to an hour before its cursor (`OVERLAP_SECS`), but never more
    /// than [`RECENT_SECS`] before the read began, and records each swap's Orchard
    /// receiver in `store`, with when the read began, and each completed payout to one
    /// with its reported transactions, dated when both scans finished so a long read
    /// cannot use up its grace. A payout read that found a cursor also rescans
    /// successful swaps created up to [`RECENT_SECS`] before it, but again never more
    /// than that before the read began, for their completions only. Older completions
    /// are the sweeps'. Both scans are recorded in one transaction once both finish (see
    /// [`ProviderStore::record`]), so a failed or over-long scan records nothing. A read
    /// that cannot reach back to its cursor's overlap, such as the feed's first or one
    /// after a long outage, starts the feed's live coverage where it began. Older swaps
    /// are the sweeps' (see [`Self::sweep`]). Times are capped at the read's start, so a
    /// record dated in the future cannot hide later swaps. Returns how many receivers it
    /// recorded.
    pub async fn sync(&mut self, store: &mut ProviderStore, feed: Feed) -> Result<usize> {
        let read_at = unix_now();
        let cursor = store.cursor(feed.name())?;
        // Decided before fetching: a read that found a cursor reads back only to it.
        let reach = read_at - RECENT_SECS;
        let floor = cursor.map_or(reach, |c| (c - OVERLAP_SECS).max(reach));
        let started = cursor
            .is_none_or(|c| floor > c - OVERLAP_SECS)
            .then_some(floor);
        let mut newest = cursor.unwrap_or(floor).min(read_at);
        let mut found = Vec::new();
        let mut completed = Vec::new();
        self.scan(feed, STATUSES, floor, read_at, |swap, created| {
            newest = newest.max(created);
            observe(feed, swap, created, &mut found, &mut completed);
        })
        .await?;
        if let (Feed::Payouts, Some(cursor)) = (feed, cursor) {
            let floor = (cursor - RECENT_SECS).max(reach);
            self.scan(feed, SUCCESS, floor, read_at, |swap, _| {
                if let Some(receiver) = swap.recipient.as_deref().and_then(orchard_receiver) {
                    completions(swap, receiver, &mut completed);
                }
            })
            .await?;
        }
        let seen_at = unix_now();
        let completed: Vec<_> = completed
            .into_iter()
            .map(|(r, t)| (r, t, seen_at))
            .collect();
        store.record(feed.name(), started, &found, &completed, newest, read_at)?;
        Ok(found.len())
    }

    /// Advances `feed`'s history sweeps by up to `--near-sweep-pages` pages of swaps in
    /// every status between them, each pass in progress or due getting at least half
    /// the pages and any the other leaves, so neither starves the other however long it
    /// runs. Each page is recorded as it is read (see [`ProviderStore::sweep_page`]),
    /// so a failure or restart resumes after the last recorded page and no bound on one read limits the history. A pass not in
    /// progress begins from the explorer's newest swap when it is due. The full pass
    /// reads down to the history's start, or to `start` while the history does not
    /// reach back to it, as on the first backfill or after `--near-since` moves
    /// earlier, and is due then and `FULL_INTERVAL_SECS` after the last full pass
    /// began, so a swap corrected however long after its creation is recorded. The
    /// recent pass reads down to `RECONCILE_LOOKBACK_SECS` before the history's end and
    /// is due `RECONCILE_INTERVAL_SECS` after the history's end, so swaps listed late
    /// or skipped by a read are recorded soon. Returns the pages it read.
    pub async fn sweep(
        &mut self,
        store: &mut ProviderStore,
        feed: Feed,
        start: i64,
    ) -> Result<usize> {
        let mut pages = 0;
        // The recent pass takes up to half, the full pass what remains, and the recent
        // pass any the full pass left.
        let half = self.sweep_pages.div_ceil(2);
        for (pass, share) in [
            (Pass::Recent, half),
            (Pass::Full, usize::MAX),
            (Pass::Recent, usize::MAX),
        ] {
            let budget = share.min(self.sweep_pages - pages);
            pages += self.pass(store, feed, pass, start, budget).await?;
        }
        Ok(pages)
    }

    /// Advances `feed`'s `pass` by up to `budget` pages, beginning it if it is due; see
    /// [`Self::sweep`]. Returns the pages it read.
    async fn pass(
        &mut self,
        store: &mut ProviderStore,
        feed: Feed,
        pass: Pass,
        start: i64,
        budget: usize,
    ) -> Result<usize> {
        let name = feed.name();
        let sweep = match store.sweep(name, pass)? {
            Some(sweep) => sweep,
            None if budget == 0 => return Ok(0),
            None => {
                let now = unix_now();
                let floor = match (pass, store.history(name)?) {
                    (Pass::Full, None) => start,
                    (Pass::Full, Some(h)) if start < h.since => start,
                    (Pass::Full, Some(h)) if now >= h.full + FULL_INTERVAL_SECS => h.since,
                    (Pass::Recent, Some(h)) if now >= h.through + RECONCILE_INTERVAL_SECS => {
                        (h.through - RECONCILE_LOOKBACK_SECS).max(h.since)
                    }
                    _ => return Ok(0),
                };
                store.begin_sweep(name, pass, floor, now)?
            }
        };
        let mut after = sweep.after;
        for pages in 1..=budget {
            let swaps = self.page(feed, STATUSES, after.as_ref()).await?;
            let seen_at = unix_now();
            let (mut found, mut completed, mut reached) = (Vec::new(), Vec::new(), None);
            for swap in &swaps {
                let created = swap.created_at_timestamp.min(seen_at);
                reached = Some(reached.map_or(created, |r: i64| r.min(created)));
                if created >= sweep.floor {
                    observe(feed, swap, created, &mut found, &mut completed);
                }
            }
            let next = next_page(&swaps, sweep.floor, after.as_ref())?;
            let completed: Vec<_> = completed
                .into_iter()
                .map(|(r, t)| (r, t, seen_at))
                .collect();
            store.sweep_page(
                name,
                pass,
                sweep.began,
                &found,
                &completed,
                reached,
                next.as_ref(),
                seen_at,
            )?;
            match next {
                Some(next) => after = Some(next),
                None => return Ok(pages),
            }
        }
        Ok(budget)
    }

    /// Pages `feed`'s swaps in `statuses`, newest first, passing `each` every swap with
    /// its creation time, capped at `read_at`, from `floor` on. Past [`MAX_PAGES`] it
    /// fails.
    async fn scan(
        &mut self,
        feed: Feed,
        statuses: &str,
        floor: i64,
        read_at: i64,
        mut each: impl FnMut(&Swap, i64),
    ) -> Result<()> {
        let mut after = None;
        for _ in 0..self.max_pages {
            let swaps = self.page(feed, statuses, after.as_ref()).await?;
            for swap in &swaps {
                let created = swap.created_at_timestamp.min(read_at);
                if created >= floor {
                    each(swap, created);
                }
            }
            match next_page(&swaps, floor, after.as_ref())? {
                Some(next) => after = Some(next),
                None => return Ok(()),
            }
        }
        Err("NEAR explorer read exceeded its page bound".into())
    }

    /// One page of `feed`'s swaps in `statuses`, newest first, older than `after` when
    /// given.
    async fn page(
        &mut self,
        feed: Feed,
        statuses: &str,
        after: Option<&Token>,
    ) -> Result<Vec<Swap>> {
        tokio::time::sleep_until(self.next_request).await;
        let mut query = vec![
            (feed.chain_filter(), "zec".to_owned()),
            ("statuses", statuses.to_owned()),
            ("numberOfTransactions", PAGE.to_string()),
        ];
        if let Some((address, memo)) = after {
            query.push(("lastDepositAddress", address.clone()));
            if let Some(memo) = memo {
                query.push(("lastDepositMemo", memo.clone()));
            }
        }
        let response = self
            .http
            .get(&self.endpoint)
            .bearer_auth(&self.key)
            .query(&query)
            .send()
            .await;
        self.next_request = tokio::time::Instant::now() + self.interval;
        let response = response?;
        let status = response.status();
        if !status.is_success() {
            return Err(format!("NEAR explorer returned HTTP {status}").into());
        }
        let body = crate::read_limited(response, MAX_PAGE_BYTES).await?;
        let swaps: Vec<Swap> = serde_json::from_slice(&body)?;
        // A longer page is not one this read asked for, so it cannot end the read.
        if swaps.len() > PAGE {
            return Err("NEAR explorer page exceeds the requested length".into());
        }
        Ok(swaps)
    }
}

/// Where the page after `swaps`, which began after `after`, starts if a read down to
/// `floor` continues: only after a full page whose last swap is not older than `floor`.
/// A start that repeats `after` is an error, so an explorer that ignores paging cannot
/// hold a read in place.
fn next_page(swaps: &[Swap], floor: i64, after: Option<&Token>) -> Result<Option<Token>> {
    match swaps.last() {
        Some(last) if swaps.len() == PAGE && last.created_at_timestamp >= floor => {
            let next = (last.deposit_address.clone(), last.deposit_memo.clone());
            if after == Some(&next) {
                return Err("NEAR explorer paging made no progress".into());
            }
            Ok(Some(next))
        }
        _ => Ok(None),
    }
}

/// Notes `swap`, created at `created`: the Orchard receiver of its `feed` address,
/// `true` marking a payout, and for a payout its completions (see [`completions`]).
fn observe(
    feed: Feed,
    swap: &Swap,
    created: i64,
    found: &mut Vec<(ReceiverBytes, bool, i64)>,
    completed: &mut Vec<(ReceiverBytes, receiver_directory::Hash)>,
) {
    let address = match feed {
        Feed::Payouts => &swap.recipient,
        Feed::Refunds => &swap.refund_to,
    };
    if let Some(receiver) = address.as_deref().and_then(orchard_receiver) {
        found.push((receiver, feed == Feed::Payouts, created));
        if feed == Feed::Payouts {
            completions(swap, receiver, completed);
        }
    }
}

/// The Orchard receiver of a mainnet unified address, the only kind a per-swap key has.
fn orchard_receiver(address: &str) -> Option<ReceiverBytes> {
    Receiver::from_unified_address(address, Network::Main)
        .ok()
        .map(|receiver| *receiver.as_bytes())
}

/// Adds the payouts of `swap` to `receiver` if it succeeded: one per parsable reported
/// transaction, or else one uncheckable record (see [`UNCHECKABLE_ZEROS`]).
fn completions(
    swap: &Swap,
    receiver: ReceiverBytes,
    out: &mut Vec<(ReceiverBytes, receiver_directory::Hash)>,
) {
    if swap.status.as_deref() != Some(SUCCESS) {
        return;
    }
    let before = out.len();
    out.extend(
        swap.destination_chain_tx_hashes
            .iter()
            .flatten()
            .filter_map(|hash| zakura_server_chain::hash::parse_display(hash).ok())
            .map(|txid| (receiver, txid)),
    );
    if out.len() == before {
        let address = swap.deposit_address.as_bytes();
        let memo = swap.deposit_memo.as_deref();
        let digest = Sha256::new()
            .chain_update((address.len() as u64).to_le_bytes())
            .chain_update(address)
            .chain_update([u8::from(memo.is_some())])
            .chain_update(memo.unwrap_or_default())
            .finalize();
        let mut txid = [0; 32];
        txid[UNCHECKABLE_ZEROS..].copy_from_slice(&digest[..32 - UNCHECKABLE_ZEROS]);
        out.push((uncheckable_receiver(), txid));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    /// A mainnet payout transaction from 2026-10-07, displayed hex.
    const PAYOUT_TXID: &str = "7f79a9262c301e3da38907fdbfca663c1fcb04b6a890ea737c281fc2ff707e7f";

    #[test]
    fn only_mainnet_unified_orchard_receivers_count() {
        // A Vizor swap address from 2026-10-07.
        let swap = "u14nnj43rj7dpf7qh6gu24fuyu8vld9fgatxd32xre27yqgu6p0yq0sf0t3uxnwts4968hf7d8nvyh4wfzqtmcdt6xzk7el6pn0ufx6pdg";
        assert_eq!(
            &orchard_receiver(swap).unwrap()[..4],
            &[0x15, 0x5f, 0x51, 0xfe]
        );
        assert!(orchard_receiver("t1KfwsnwJeNRVjQGBDZhwKskpQbih2qx5Ua").is_none());
        assert!(orchard_receiver("check-alice.near").is_none());
    }

    /// Records a completed `pass` of `feed` from `floor` to `began` that found nothing.
    fn swept(store: &mut ProviderStore, feed: &str, pass: Pass, floor: i64, began: i64) {
        store.begin_sweep(feed, pass, floor, began).unwrap();
        store
            .sweep_page(feed, pass, began, &[], &[], None, None, began)
            .unwrap();
    }

    /// `feed`'s history span, `since` to `through`, if any.
    fn span(store: &ProviderStore, feed: &str) -> Option<(i64, i64)> {
        store.history(feed).unwrap().map(|h| (h.since, h.through))
    }

    /// The recent set needs both feeds' live coverage and reaches back from the older
    /// read. The seen set also needs both feeds' history to reach that coverage without
    /// a gap, and declares where the history begins.
    #[test]
    fn provider_sets_reach_back_from_the_last_complete_read() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = ProviderStore::open(dir.path().join("providers.sqlite")).unwrap();
        let (payouts, refunds) = (Feed::Payouts.name(), Feed::Refunds.name());
        // A feed that never finished a read publishes nothing.
        assert!(provider_sets(&store).unwrap().is_empty());
        let until = 2_000 + RECENT_SECS;
        let old = (receiver(1), true, 1_500);
        let recent = (receiver(2), true, until - 60);
        store
            .record(payouts, Some(1_000), &[old, recent], &[], until, until + 30)
            .unwrap();
        // Both sets hold refunds too, so neither is published before that feed reads.
        assert!(provider_sets(&store).unwrap().is_empty());
        let old_refund = (receiver(3), false, 1_500);
        let refund = (receiver(4), false, until - 120);
        // A receiver given as both a payout and a refund address counts once.
        let both = (receiver(2), false, until - 90);
        store
            .record(
                refunds,
                Some(2_000),
                &[old_refund, refund, both],
                &[],
                until,
                until,
            )
            .unwrap();
        let labels = |store: &ProviderStore| -> Vec<String> {
            let sets = provider_sets(store).unwrap();
            sets.into_iter().map(|set| set.label).collect()
        };
        // Without both feeds' history, and with a refund history that ends before the
        // live coverage begins, only the recent set is published.
        assert_eq!(labels(&store), ["near-intents/recent"]);
        swept(&mut store, payouts, Pass::Full, 500, 2_500);
        assert_eq!(labels(&store), ["near-intents/recent"]);
        swept(&mut store, refunds, Pass::Full, 800, 1_500);
        assert_eq!(labels(&store), ["near-intents/recent"]);
        // A sweep that reaches into the refund history closes the gap.
        swept(&mut store, refunds, Pass::Recent, 1_400, 3_000);
        let sets = provider_sets(&store).unwrap();
        let labels: Vec<_> = sets.iter().map(|set| set.label.as_str()).collect();
        assert_eq!(labels, ["near-intents/recent", "near-intents/seen"]);
        assert_eq!((sets[0].since_unix, sets[0].until_unix), (2_000, until));
        assert_eq!((sets[1].since_unix, sets[1].until_unix), (800, until));
        let sorted = |mut receivers: Vec<ReceiverBytes>| {
            receivers.sort();
            receivers
        };
        assert_eq!(
            sorted(sets[0].receivers.clone()),
            sorted(vec![receiver(2), receiver(4)])
        );
        assert_eq!(
            sorted(sets[1].receivers.clone()),
            sorted(vec![receiver(1), receiver(2), receiver(3), receiver(4)])
        );
    }

    /// A failed first read, including one over the page bound, records no start, so a
    /// restart cannot leave the feed claiming coverage it never read. A completed one
    /// starts the live coverage a day before it began.
    #[tokio::test]
    async fn a_feed_starts_only_once_a_read_completes() {
        use axum::{http::StatusCode, routing::get, Router};
        let app = Router::new()
            .route("/fail", get(|| async { (StatusCode::BAD_GATEWAY, "") }))
            // Valid JSON, but over the page bound.
            .route(
                "/large",
                get(|| async { format!("[{}]", " ".repeat(MAX_PAGE_BYTES)) }),
            )
            .route("/ok", get(|| async { "[]" }));
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", socket.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
        let dir = tempfile::tempdir().unwrap();
        let mut store = ProviderStore::open(dir.path().join("providers.sqlite")).unwrap();
        let feed = Feed::Payouts;
        for route in ["fail", "large"] {
            let mut failing = Explorer::at(format!("{origin}/{route}"));
            assert!(failing.sync(&mut store, feed).await.is_err());
            assert_eq!(store.started(feed.name()).unwrap(), None);
        }
        let mut working = Explorer::at(format!("{origin}/ok"));
        let before = unix_now();
        working.sync(&mut store, feed).await.unwrap();
        let started = store.started(feed.name()).unwrap().unwrap();
        assert!((before - RECENT_SECS..=unix_now() - RECENT_SECS).contains(&started));
    }

    /// Health's `near.reads` shows only this process's completed reads: null at start
    /// although `providers.sqlite` holds an earlier run's read times, still null after
    /// reads the explorer refuses, and each feed's begin time once a read of it completes.
    #[tokio::test]
    async fn health_reports_only_this_processs_completed_reads() {
        use axum::{extract::RawQuery, http::StatusCode, routing::get, Router};
        use serde_json::{json, Value};
        let app = Router::new()
            .route("/denied", get(|| async { (StatusCode::UNAUTHORIZED, "") }))
            // Accepts the payout feed (`toChainId`) only.
            .route(
                "/payouts",
                get(|RawQuery(query): RawQuery| async move {
                    match query.unwrap_or_default().contains("fromChainId") {
                        true => (StatusCode::BAD_REQUEST, ""),
                        false => (StatusCode::OK, "[]"),
                    }
                }),
            )
            .route("/ok", get(|| async { "[]" }));
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let explorer = format!("http://{}", socket.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("providers.sqlite");
        let mut store = ProviderStore::open(&path).unwrap();
        for feed in [Feed::Payouts, Feed::Refunds] {
            store
                .record(feed.name(), Some(1_000), &[], &[], 2_000, 2_000)
                .unwrap();
        }
        drop(store);
        let reads = Reads::default();
        let app = receiver_pir_server::router_with_health(
            receiver_pir_server::Publications::default(),
            {
                let reads = reads.clone();
                Arc::new(move || reads.health())
            },
        );
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let health = format!("http://{}/v1/receiver/health", socket.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
        let near_reads = || async {
            let health: Value = reqwest::get(&health).await.unwrap().json().await.unwrap();
            health["near"]["reads"].clone()
        };
        let unread = json!({"near-payouts": null, "near-refunds": null});
        assert_eq!(near_reads().await, unread);
        let mut denied = Explorer::at(format!("{explorer}/denied"));
        poll(&mut denied, &path, 1_000, &reads).await;
        assert_eq!(near_reads().await, unread);
        let store = ProviderStore::open(&path).unwrap();
        assert_eq!(store.read(Feed::Payouts.name()).unwrap(), Some(2_000));
        let before = unix_now();
        let mut payouts = Explorer::at(format!("{explorer}/payouts"));
        poll(&mut payouts, &path, 1_000, &reads).await;
        let read = near_reads().await;
        assert!(read["near-payouts"].as_i64().unwrap() >= before);
        assert!(read["near-refunds"].is_null());
        poll(
            &mut Explorer::at(format!("{explorer}/ok")),
            &path,
            1_000,
            &reads,
        )
        .await;
        let read = near_reads().await;
        assert!(read["near-refunds"].as_i64().unwrap() >= before);
    }

    /// A read that reaches its page bound, first read included, or receives a page
    /// longer than it asked for fails and records nothing; one that ends on its last
    /// allowed page succeeds.
    #[tokio::test]
    async fn every_read_is_bounded_and_records_nothing_when_it_fails() {
        use axum::{extract::State, routing::get, Router};
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };
        const SWAP: &str = "u14nnj43rj7dpf7qh6gu24fuyu8vld9fgatxd32xre27yqgu6p0yq0sf0t3uxnwts4968hf7d8nvyh4wfzqtmcdt6xzk7el6pn0ufx6pdg";
        // Serves `full` pages of `len` records, each record with a fresh paging token
        // and a completed payout, created `page` seconds before `top`, then an empty page.
        #[derive(Clone)]
        struct Pages {
            served: Arc<AtomicUsize>,
            full: usize,
            len: usize,
            top: i64,
        }
        async fn page(State(feed): State<Pages>) -> String {
            let page = feed.served.fetch_add(1, Ordering::SeqCst);
            let len = if page < feed.full { feed.len } else { 0 };
            let created = feed.top - page as i64;
            let records: Vec<_> = (0..len)
                .map(|i| {
                    serde_json::json!({"recipient": SWAP, "createdAtTimestamp": created,
                        "depositAddress": format!("{page}-{i}"), "status": "SUCCESS",
                        "destinationChainTxHashes": [PAYOUT_TXID]})
                })
                .collect();
            serde_json::Value::from(records).to_string()
        }
        let serve = |full, len| async move {
            let app = Router::new().route("/", get(page)).with_state(Pages {
                served: Arc::default(),
                full,
                len,
                top: unix_now(),
            });
            let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let origin = format!("http://{}/", socket.local_addr().unwrap());
            tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
            origin
        };
        let dir = tempfile::tempdir().unwrap();
        let mut store = ProviderStore::open(dir.path().join("providers.sqlite")).unwrap();
        let feed = Feed::Payouts.name();
        let nothing_recorded = |store: &ProviderStore| {
            store.cursor(feed).unwrap().is_none()
                && store.read(feed).unwrap().is_none()
                && store.started(feed).unwrap().is_none()
                && store.payouts(i64::MAX).unwrap().is_empty()
                && store.sets(0).unwrap() == (vec![], vec![])
        };
        // Full pages that never end, and a page one record too long.
        for (full, len) in [(usize::MAX, PAGE), (1, PAGE + 1)] {
            let mut explorer = Explorer::at(serve(full, len).await);
            explorer.max_pages = 3;
            assert!(explorer.sync(&mut store, Feed::Payouts).await.is_err());
            assert!(nothing_recorded(&store));
        }
        // The last allowed page ends the read.
        let mut explorer = Explorer::at(serve(2, PAGE).await);
        explorer.max_pages = 3;
        assert_eq!(
            explorer.sync(&mut store, Feed::Payouts).await.unwrap(),
            2 * PAGE
        );
        assert!(store.started(feed).unwrap().is_some());
        assert_eq!(store.payouts(i64::MAX).unwrap().len(), 1);
    }

    /// A payout read that follows a cursor also rescans successful swaps back a day
    /// before that cursor, but never more than a day before the read, for their
    /// completions only: swaps older than the overlap that have since succeeded are
    /// recorded, without their receivers, and one older than the day is not. If the
    /// rescan fails, neither scan records anything.
    #[tokio::test]
    async fn a_payout_read_rescans_successes_a_day_before_its_cursor() {
        use axum::{
            extract::{Query, State},
            http::StatusCode,
            routing::get,
            Router,
        };
        use serde_json::{json, Value};
        use std::{
            collections::HashMap,
            sync::{Arc, Mutex},
        };
        /// The `statuses` of every request, and the pages served for all statuses and
        /// for successes, `None` failing.
        #[derive(Clone, Default)]
        struct Served {
            asked: Arc<Mutex<Vec<String>>>,
            pages: Arc<Mutex<(Value, Option<Value>)>>,
        }
        async fn page(
            State(served): State<Served>,
            Query(query): Query<HashMap<String, String>>,
        ) -> std::result::Result<String, StatusCode> {
            let statuses = query["statuses"].clone();
            served.asked.lock().unwrap().push(statuses.clone());
            let (all, successes) = served.pages.lock().unwrap().clone();
            match (statuses == SUCCESS, successes) {
                (false, _) => Ok(all.to_string()),
                (true, Some(page)) => Ok(page.to_string()),
                (true, None) => Err(StatusCode::BAD_GATEWAY),
            }
        }
        let served = Served::default();
        let app = Router::new()
            .route("/", get(page))
            .with_state(served.clone());
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut explorer = Explorer::at(format!("http://{}/", socket.local_addr().unwrap()));
        tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
        // A swap to `receiver(seed)` paid in transaction `[seed; 32]` when it succeeded.
        let swap = |seed: u8, created: i64, status: &str| {
            let address = Receiver::from_bytes(receiver(seed))
                .unwrap()
                .to_unified_address(Network::Main);
            json!({"recipient": address, "createdAtTimestamp": created,
                "depositAddress": seed.to_string(), "status": status,
                "destinationChainTxHashes": [format!("{seed:02x}").repeat(32)]})
        };
        let serve = |all: Value, successes: Option<Value>| {
            *served.pages.lock().unwrap() = (all, successes);
        };
        let asked = || served.asked.lock().unwrap().clone();
        let dir = tempfile::tempdir().unwrap();
        let mut store = ProviderStore::open(dir.path().join("providers.sqlite")).unwrap();
        let feed = Feed::Payouts;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        // Two hours pass after the first read.
        let cursor = now - 2 * 3600;
        let since = cursor - 3600;
        let first = [(receiver(1), true, cursor)];
        store
            .record(feed.name(), Some(since), &first, &[], cursor, cursor)
            .unwrap();
        let read = store.read(feed.name()).unwrap();

        serve(json!([swap(2, now - 600, SUCCESS)]), None);
        assert!(explorer.sync(&mut store, feed).await.is_err());
        assert_eq!(asked(), [STATUSES, SUCCESS]);
        assert_eq!(store.cursor(feed.name()).unwrap(), Some(cursor));
        assert_eq!(store.read(feed.name()).unwrap(), read);
        assert!(store.payouts(i64::MAX).unwrap().is_empty());
        assert_eq!(store.sets(0).unwrap().1, [receiver(1)]);

        let successes = json!([
            swap(1, cursor, SUCCESS),
            swap(3, cursor - 2 * OVERLAP_SECS, SUCCESS),
            swap(4, now - RECENT_SECS - 60, SUCCESS),
        ]);
        serve(json!([swap(2, now - 600, SUCCESS)]), Some(successes));
        assert_eq!(explorer.sync(&mut store, feed).await.unwrap(), 1);
        assert_eq!(asked()[2..], [STATUSES, SUCCESS]);
        let mut payouts = store.payouts(i64::MAX).unwrap();
        payouts.sort();
        let mut expected: Vec<_> = [1, 2, 3].map(|seed| (receiver(seed), [seed; 32])).into();
        expected.sort();
        assert_eq!(payouts, expected);
        let mut seen = store.sets(0).unwrap().1;
        seen.sort();
        let mut expected = vec![receiver(1), receiver(2)];
        expected.sort();
        assert_eq!(seen, expected);
        assert_eq!(store.cursor(feed.name()).unwrap(), Some(now - 600));
        assert_eq!(store.started(feed.name()).unwrap(), Some(since));
    }

    /// A record missing its address is skipped, a future date is capped at the read's
    /// start, and a completed payout is recorded, without a parsable transaction as
    /// uncheckable, one row per swap, dated when the read finished, not began.
    #[tokio::test]
    async fn a_read_skips_bad_records_caps_future_dates_and_notes_completions() {
        use axum::{routing::get, Router};
        let swap = "u14nnj43rj7dpf7qh6gu24fuyu8vld9fgatxd32xre27yqgu6p0yq0sf0t3uxnwts4968hf7d8nvyh4wfzqtmcdt6xzk7el6pn0ufx6pdg";
        let past = unix_now() - 3_000;
        let page = serde_json::json!([
            {"recipient": swap, "refundTo": null, "createdAtTimestamp": 9_999_999_999i64,
             "depositAddress": "a", "depositMemo": null, "status": "SUCCESS",
             "destinationChainTxHashes": [PAYOUT_TXID, "not hex"]},
            {"recipient": null, "createdAtTimestamp": past + 1_000, "depositAddress": "b"},
            {"recipient": swap, "createdAtTimestamp": past, "depositAddress": "c",
             "status": "SUCCESS", "destinationChainTxHashes": ["not hex"]},
            {"recipient": swap, "createdAtTimestamp": past, "depositAddress": "d",
             "status": "SUCCESS"},
        ])
        .to_string();
        // A page that takes over a second, so the read ends in a later second.
        let app = Router::new().route(
            "/",
            get(move || {
                let page = page.clone();
                async move {
                    tokio::time::sleep(Duration::from_millis(1100)).await;
                    page
                }
            }),
        );
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}/", socket.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
        let dir = tempfile::tempdir().unwrap();
        let mut store = ProviderStore::open(dir.path().join("providers.sqlite")).unwrap();
        let read = Explorer::at(origin)
            .sync(&mut store, Feed::Payouts)
            .await
            .unwrap();
        assert_eq!(read, 3);
        let began = store.read(Feed::Payouts.name()).unwrap().unwrap();
        assert!(store.payouts(began).unwrap().is_empty());
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        assert!(store.cursor(Feed::Payouts.name()).unwrap().unwrap() <= now);
        let mut completed = store.payouts(now + 60).unwrap();
        completed.retain(|p| !uncheckable(&p.1));
        assert_eq!(completed.len(), 1);
        assert_eq!(store.payouts(now + 60).unwrap().len(), 3);
        // The explorer's displayed hex is reversed into protocol byte order.
        assert_eq!(completed[0].1[0], 0x7f);
        assert_eq!(completed[0].1[31], 0x7f);
        assert_eq!(completed[0].1[1], 0x7e);
    }

    /// A completed payout to an Orchard receiver without a parsable transaction is one
    /// uncheckable record per swap, told apart by memo, under a receiver that enters no
    /// filter set; one whose recipient is missing, malformed, for another network or
    /// without an Orchard receiver is not recorded, with or without a transaction. A
    /// reread after reopening derives the same records, keeping when each was first
    /// seen, and the report counts them for a day after their grace.
    #[tokio::test]
    async fn uncheckable_payouts_are_one_record_per_swap() {
        use axum::{routing::get, Router};
        let swap = "u14nnj43rj7dpf7qh6gu24fuyu8vld9fgatxd32xre27yqgu6p0yq0sf0t3uxnwts4968hf7d8nvyh4wfzqtmcdt6xzk7el6pn0ufx6pdg";
        let orchard = orchard_receiver(swap).unwrap();
        let excluded = [
            None,
            Some("not an address".to_owned()),
            Some(
                Receiver::from_bytes(orchard)
                    .unwrap()
                    .to_unified_address(Network::Test),
            ),
            Some("t1KfwsnwJeNRVjQGBDZhwKskpQbih2qx5Ua".to_owned()),
            // Mainnet P2SH and Sapling addresses of all-one bytes, and a unified address of
            // those Sapling and P2PKH receivers, encoded with zakura-address 3.0.0.
            Some("t3JevopkKaLy85HNJG8bqunxoPsQ5o8GRya".to_owned()),
            Some("zs1qyqszqgpqyqszqgpqyqszqgpqyqszqgpqyqszqgpqyqszqgpqyqszqgpqyqszqgpqyqszvsj7ke".to_owned()),
            Some("u1cux9lm2ndc6034mtnny5m0ussphqtgc3ept4979nq82mqsrjgda3r7s6gcvl7jy0056nkcmxr7r50x7kvcw5he63rs8mf0t0963j2r7ygcnunx9nxpnvcwzn3ng5gcwdg87gy9yyq9s".to_owned()),
        ];
        let record =
            |recipient: Option<&str>, address: &str, memo: Option<&str>, txids: &[&str]| {
                serde_json::json!({"recipient": recipient, "createdAtTimestamp": 9_999_999_999i64,
                "depositAddress": address, "depositMemo": memo, "status": "SUCCESS",
                "destinationChainTxHashes": txids})
            };
        let mut page = vec![
            record(Some(swap), "a", None, &[]),
            record(Some(swap), "a", Some("m"), &["not hex"]),
        ];
        for (i, recipient) in excluded.iter().enumerate() {
            for txids in [&[][..], &[PAYOUT_TXID]] {
                page.push(record(
                    recipient.as_deref(),
                    &format!("{i}-{}", txids.len()),
                    None,
                    txids,
                ));
            }
        }
        let page = serde_json::Value::from(page).to_string();
        let app = Router::new().route(
            "/",
            get(move || {
                let page = page.clone();
                async move { page }
            }),
        );
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut explorer = Explorer::at(format!("http://{}/", socket.local_addr().unwrap()));
        tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("providers.sqlite");
        let mut store = ProviderStore::open(&path).unwrap();
        explorer.sync(&mut store, Feed::Payouts).await.unwrap();
        let first_seen = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let first = store.payouts(first_seen).unwrap();
        assert_eq!(first.len(), 2);
        assert!(first
            .iter()
            .all(|p| p.0 == uncheckable_receiver() && uncheckable(&p.1)));
        assert_eq!(store.sets(0).unwrap(), (vec![orchard], vec![orchard]));
        tokio::time::sleep(Duration::from_millis(1100)).await;
        drop(store);
        let mut store = ProviderStore::open(&path).unwrap();
        explorer.sync(&mut store, Feed::Payouts).await.unwrap();
        assert_eq!(store.payouts(i64::MAX).unwrap(), first);
        assert_eq!(store.payouts(first_seen).unwrap(), first);
        let counted = |anchor_time| {
            capture(&store, anchor_time, anchor_time)
                .unwrap()
                .uncheckable
        };
        assert_eq!(counted(first_seen + COMPLETION_GRACE_SECS), 2);
        assert_eq!(counted(first_seen + RECENT_SECS + 1), 0);
    }

    /// A test explorer whose `swaps`, newest first, page as NEAR's do: a request gets
    /// up to a page after the swap whose deposit address is its `lastDepositAddress`,
    /// whatever statuses or direction it asks for. Each request's `lastDepositAddress`
    /// is kept in `asked`, and every request fails while `failing` is set.
    #[derive(Clone, Default)]
    struct Listing {
        swaps: Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
        asked: Arc<std::sync::Mutex<Vec<Option<String>>>>,
        failing: Arc<std::sync::atomic::AtomicBool>,
    }

    impl Listing {
        /// Serves the listing and returns a client for it.
        async fn explorer(&self) -> Explorer {
            use axum::{
                extract::{Query, State},
                http::StatusCode,
                routing::get,
                Router,
            };
            use std::collections::HashMap;
            /// Answers one request as [`Listing`] describes.
            async fn page(
                State(listing): State<Listing>,
                Query(query): Query<HashMap<String, String>>,
            ) -> std::result::Result<String, StatusCode> {
                let after = query.get("lastDepositAddress").cloned();
                listing.asked.lock().unwrap().push(after.clone());
                if listing.failing.load(Ordering::SeqCst) {
                    return Err(StatusCode::BAD_GATEWAY);
                }
                let swaps = listing.swaps.lock().unwrap();
                let from = after.map_or(0, |after| {
                    1 + swaps
                        .iter()
                        .position(|swap| swap["depositAddress"] == after.as_str())
                        .unwrap()
                });
                let page = &swaps[from.min(swaps.len())..(from + PAGE).min(swaps.len())];
                Ok(serde_json::Value::from(page.to_vec()).to_string())
            }
            let app = Router::new().route("/", get(page)).with_state(self.clone());
            let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let origin = format!("http://{}/", socket.local_addr().unwrap());
            tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
            Explorer::at(origin)
        }

        /// Lists `swap`, created at its `createdAtTimestamp`, in creation order.
        fn list(&self, swap: serde_json::Value) {
            let mut swaps = self.swaps.lock().unwrap();
            let created = swap["createdAtTimestamp"].as_i64().unwrap();
            let at = swaps
                .iter()
                .position(|s| s["createdAtTimestamp"].as_i64().unwrap() < created)
                .unwrap_or(swaps.len());
            swaps.insert(at, swap);
        }
    }

    /// An unfunded swap with deposit address `address`, created at `created`, whose
    /// payout and refund addresses both hold `receiver(seed)`, or neither when `seed`
    /// is `None`.
    fn listed(seed: Option<u8>, created: i64, address: &str) -> serde_json::Value {
        let unified = seed.map(|seed| {
            Receiver::from_bytes(receiver(seed))
                .unwrap()
                .to_unified_address(Network::Main)
        });
        serde_json::json!({"recipient": unified, "refundTo": unified,
            "createdAtTimestamp": created, "depositAddress": address,
            "status": "PENDING_DEPOSIT"})
    }

    /// `receivers`, sorted.
    fn sorted(mut receivers: Vec<ReceiverBytes>) -> Vec<ReceiverBytes> {
        receivers.sort();
        receivers
    }

    /// A backfill longer than one poll's sweep records each page as it reads it: a
    /// failed page records nothing, and after a restart the sweep resumes after its last
    /// recorded page and completes the history from `--near-since`. Once complete, a
    /// later `--near-since` leaves the history alone and an earlier one backfills it.
    #[tokio::test]
    async fn a_backfill_resumes_after_a_failure_and_a_restart() {
        let (feed, now) = (Feed::Refunds, unix_now());
        let name = feed.name();
        let listing = Listing::default();
        // 2,500 swaps a minute apart, a page each to receivers 1, 2 and 3.
        let (one, two, three) = (receiver(1), receiver(2), receiver(3));
        let addresses: Vec<_> = [one, two, three]
            .map(|r| {
                Receiver::from_bytes(r)
                    .unwrap()
                    .to_unified_address(Network::Main)
            })
            .into();
        for i in 0..2_500 {
            let address = &addresses[i / PAGE];
            let created = now - 60 * i as i64;
            listing
                .swaps
                .lock()
                .unwrap()
                .push(serde_json::json!({"refundTo": address,
                "createdAtTimestamp": created, "depositAddress": format!("d{i}")}));
        }
        let start = now - 60 * 3_000;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("providers.sqlite");
        let mut store = ProviderStore::open(&path).unwrap();
        let mut explorer = listing.explorer().await;
        explorer.sweep_pages = 1;
        assert_eq!(explorer.sweep(&mut store, feed, start).await.unwrap(), 1);
        let sweep = store.sweep(name, Pass::Full).unwrap().unwrap();
        assert_eq!(
            (sweep.floor, sweep.pages, sweep.after.clone()),
            (start, 1, Some(("d999".to_owned(), None)))
        );
        assert_eq!(sweep.reached, Some(now - 60 * 999));
        assert_eq!(store.history(name).unwrap(), None);
        assert_eq!(store.sweep(name, Pass::Recent).unwrap(), None);
        assert_eq!(store.sets(0).unwrap().1, [one]);
        listing.failing.store(true, Ordering::SeqCst);
        assert!(explorer.sweep(&mut store, feed, start).await.is_err());
        assert_eq!(store.sweep(name, Pass::Full).unwrap(), Some(sweep.clone()));
        listing.failing.store(false, Ordering::SeqCst);
        drop((store, explorer));

        let mut store = ProviderStore::open(&path).unwrap();
        let mut explorer = listing.explorer().await;
        assert_eq!(explorer.sweep(&mut store, feed, start).await.unwrap(), 2);
        let asked = listing.asked.lock().unwrap().clone();
        let d = |i: usize| Some(format!("d{i}"));
        assert_eq!(asked, [None, d(999), d(999), d(1999)]);
        assert_eq!(store.sweep(name, Pass::Full).unwrap(), None);
        let history = (start, sweep.began);
        assert_eq!(span(&store, name), Some(history));
        assert_eq!(
            sorted(store.sets(0).unwrap().1),
            sorted(vec![one, two, three])
        );
        // Complete history waits for the next pass, whatever the start.
        assert_eq!(explorer.sweep(&mut store, feed, start).await.unwrap(), 0);
        assert_eq!(
            explorer.sweep(&mut store, feed, start + 600).await.unwrap(),
            0
        );
        assert_eq!(span(&store, name), Some(history));
        let earlier = start - 3_600;
        assert_eq!(explorer.sweep(&mut store, feed, earlier).await.unwrap(), 3);
        let (since, through) = span(&store, name).unwrap();
        assert_eq!(since, earlier);
        assert!(through >= now);
    }

    /// A swap the explorer lists only after the live reads passed it, beyond their
    /// hour of overlap, and one a live read skipped because its address was missing
    /// then, are recorded by the next recent pass, which reaches back a week before the
    /// history's end. A swap corrected ten days after its creation is beyond that, and
    /// the next daily full pass records it.
    #[tokio::test]
    async fn reconciliation_records_swaps_listed_late_or_corrected() {
        let (feed, now) = (Feed::Refunds, unix_now());
        let name = feed.name();
        let listing = Listing::default();
        listing.list(listed(Some(1), now - 600, "a"));
        listing.list(listed(None, now - 5 * 3600, "skipped"));
        let old = now - 10 * RECENT_SECS;
        listing.list(listed(None, old, "old"));
        let dir = tempfile::tempdir().unwrap();
        let mut store = ProviderStore::open(dir.path().join("providers.sqlite")).unwrap();
        let mut explorer = listing.explorer().await;
        explorer.sweep_pages = 1;
        explorer.sync(&mut store, feed).await.unwrap();
        // A full pass over a day ago and a recent pass two hours ago, so both are due.
        let (since, full) = (now - 30 * RECENT_SECS, now - RECENT_SECS - 600);
        swept(&mut store, name, Pass::Full, since, full);
        swept(&mut store, name, Pass::Recent, full - 3600, now - 2 * 3600);
        // The two skipped swaps' addresses filled in, and a swap listed three days late.
        {
            let mut swaps = listing.swaps.lock().unwrap();
            swaps[1] = listed(Some(3), now - 5 * 3600, "skipped");
            swaps[2] = listed(Some(4), old, "old");
        }
        listing.list(listed(Some(2), now - 3 * RECENT_SECS, "late"));
        explorer.sync(&mut store, feed).await.unwrap();
        assert_eq!(store.sets(0).unwrap().1, [receiver(1)]);
        assert_eq!(explorer.sweep(&mut store, feed, since).await.unwrap(), 1);
        let recent = sorted(vec![receiver(1), receiver(2), receiver(3)]);
        assert_eq!(sorted(store.sets(0).unwrap().1), recent);
        let history = store.history(name).unwrap().unwrap();
        assert_eq!((history.since, history.full), (since, full));
        assert!(history.through >= now);
        // The full pass was due too, but the recent pass used this poll's page.
        assert_eq!(explorer.sweep(&mut store, feed, since).await.unwrap(), 1);
        let all = sorted(vec![receiver(1), receiver(2), receiver(3), receiver(4)]);
        assert_eq!(sorted(store.sets(0).unwrap().1), all);
        let history = store.history(name).unwrap().unwrap();
        assert_eq!(history.since, since);
        assert!(history.full >= now);
        assert_eq!(explorer.sweep(&mut store, feed, since).await.unwrap(), 0);
    }

    /// While a recent and a full pass both need many polls, each advances every poll:
    /// the recent pass, which would otherwise take every page and be due again by the
    /// time it finished, leaves the full pass its share.
    #[tokio::test]
    async fn the_recent_pass_cannot_starve_the_full_pass() {
        let (feed, now) = (Feed::Refunds, unix_now());
        let name = feed.name();
        let listing = Listing::default();
        let address = Receiver::from_bytes(receiver(1))
            .unwrap()
            .to_unified_address(Network::Main);
        for i in 0..6 * PAGE {
            let swap = serde_json::json!({"refundTo": address,
                "createdAtTimestamp": now - 60 * i as i64, "depositAddress": format!("d{i}")});
            listing.swaps.lock().unwrap().push(swap);
        }
        let dir = tempfile::tempdir().unwrap();
        let mut store = ProviderStore::open(dir.path().join("providers.sqlite")).unwrap();
        // Both passes are due.
        let (since, full) = (now - 30 * RECENT_SECS, now - RECENT_SECS - 600);
        swept(&mut store, name, Pass::Full, since, full);
        swept(&mut store, name, Pass::Recent, full - 3600, now - 2 * 3600);
        let mut explorer = listing.explorer().await;
        explorer.sweep_pages = 2;
        for poll in 1..=3 {
            assert_eq!(explorer.sweep(&mut store, feed, since).await.unwrap(), 2);
            for pass in [Pass::Recent, Pass::Full] {
                let pages = store.sweep(name, pass).unwrap().unwrap().pages;
                assert_eq!(pages, poll, "{}", pass.name());
            }
        }
    }

    /// After a long outage, a live read of either feed reaches back only a day, its
    /// payout completion rescan included, so it succeeds however many pages the outage
    /// holds, beyond its page bound, and restarts the feed's live coverage, which the
    /// history then no longer reaches. The recent pass reads those pages over several
    /// polls and closes the gap.
    #[tokio::test]
    async fn the_sweep_reads_an_outage_beyond_the_live_page_bound() {
        let now = unix_now();
        let listing = Listing::default();
        listing.list(listed(Some(1), now - 60, "recent"));
        let old = Receiver::from_bytes(receiver(2))
            .unwrap()
            .to_unified_address(Network::Main);
        for i in 0..3 * PAGE {
            let created = now - 2 * RECENT_SECS - 100 * i as i64;
            let swap = serde_json::json!({"recipient": old, "refundTo": old,
                "createdAtTimestamp": created, "depositAddress": format!("d{i}")});
            listing.swaps.lock().unwrap().push(swap);
        }
        let mut explorer = listing.explorer().await;
        (explorer.max_pages, explorer.sweep_pages) = (2, 1);
        for feed in [Feed::Payouts, Feed::Refunds] {
            let name = feed.name();
            let dir = tempfile::tempdir().unwrap();
            let mut store = ProviderStore::open(dir.path().join("providers.sqlite")).unwrap();
            let (start, outage) = (now - 40 * RECENT_SECS, now - 10 * RECENT_SECS);
            store
                .record(name, Some(start), &[], &[], outage, outage)
                .unwrap();
            swept(&mut store, name, Pass::Full, start, outage);
            explorer.sync(&mut store, feed).await.unwrap();
            let started = store.started(name).unwrap().unwrap();
            assert!(started >= now - RECENT_SECS);
            assert!(span(&store, name).unwrap().1 < started);
            assert_eq!(store.sets(0).unwrap().1, [receiver(1)]);
            let mut polls = 0;
            while span(&store, name).unwrap().1 < started {
                assert_eq!(explorer.sweep(&mut store, feed, start).await.unwrap(), 1);
                polls += 1;
            }
            assert_eq!(polls, 4, "{name}");
            let expected = sorted(vec![receiver(1), receiver(2)]);
            assert_eq!(sorted(store.sets(0).unwrap().1), expected);
        }
    }

    /// The report gives each feed's freshness and its history apart: a live read with
    /// no history, a sweep in progress, then completed history, which a reopened store
    /// still reports.
    #[test]
    fn the_report_tells_history_from_freshness() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("providers.sqlite");
        let (index, _) = paid_index(&dir.path().join("directory.sqlite"));
        let mut store = ProviderStore::open(&path).unwrap();
        let (payouts, refunds) = (Feed::Payouts.name(), Feed::Refunds.name());
        let now = 1_000_000;
        for feed in [payouts, refunds] {
            store
                .record(feed, Some(now - 100), &[], &[], now - 10, now - 10)
                .unwrap();
        }
        let history = |store: &ProviderStore| report(store, &index, now)["history"].clone();
        let idle = serde_json::json!({"recent": null, "full": null});
        let none = serde_json::json!({"since": null, "through": null, "full": null,
            "full_secs": null, "sweeps": idle});
        assert_eq!(report(&store, &index, now)["feeds"][payouts], now - 10);
        assert_eq!(history(&store)[payouts], none);
        store
            .begin_sweep(payouts, Pass::Full, 500, now - 5)
            .unwrap();
        let next = ("d".to_owned(), Some("m".to_owned()));
        let (pass, reached) = (Pass::Full, Some(9_000));
        store
            .sweep_page(
                payouts,
                pass,
                now - 5,
                &[],
                &[],
                reached,
                Some(&next),
                now - 4,
            )
            .unwrap();
        let sweep = serde_json::json!({"floor": 500, "began": now - 5, "reached": 9_000,
            "pages": 1});
        let progress = serde_json::json!({"since": null, "through": null, "full": null,
            "full_secs": null, "sweeps": {"recent": null, "full": sweep}});
        assert_eq!(history(&store)[payouts], progress);
        assert_eq!(capture(&store, now, now).unwrap().full_pass_secs, Some(5));
        store
            .sweep_page(payouts, pass, now - 5, &[], &[], Some(400), None, now)
            .unwrap();
        let complete = serde_json::json!({"since": 500, "through": now - 5, "full": now - 5,
            "full_secs": 5, "sweeps": idle});
        assert_eq!(history(&store)[payouts], complete);
        assert_eq!(history(&store)[refunds], none);
        drop(store);
        let store = ProviderStore::open(&path).unwrap();
        assert_eq!(history(&store)[payouts], complete);
        assert_eq!(report(&store, &index, now)["feeds"][payouts], now - 10);
    }

    /// On a long history the full pass duration grows: one that took 49 hours is
    /// reported, and a running pass that has taken longer than that counts instead, for
    /// health's capacity.
    #[test]
    fn a_long_full_pass_reports_its_duration() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = ProviderStore::open(dir.path().join("providers.sqlite")).unwrap();
        let (feed, now, hour) = (Feed::Refunds.name(), 1_000_000_000, 3600);
        let began = now - 60 * hour;
        store.begin_sweep(feed, Pass::Full, 0, began).unwrap();
        let end = now - 11 * hour;
        store
            .sweep_page(feed, Pass::Full, began, &[], &[], None, None, end)
            .unwrap();
        let history = store.history(feed).unwrap().unwrap();
        assert_eq!((history.full, history.full_secs), (began, 49 * hour));
        let secs = |store: &ProviderStore, now| capture(store, now, now).unwrap().full_pass_secs;
        assert_eq!(secs(&store, now), Some(49 * hour as u64));
        store
            .begin_sweep(feed, Pass::Full, 0, now - 10 * hour)
            .unwrap();
        assert_eq!(secs(&store, now), Some(49 * hour as u64));
        assert_eq!(secs(&store, now + 40 * hour), Some(50 * hour as u64));
        // The chain stays behind the sweep's start while wall time continues.
        assert_eq!(
            capture(&store, now - 20 * hour, now + 40 * hour)
                .unwrap()
                .full_pass_secs,
            Some(50 * hour as u64)
        );
    }

    /// [`Capture::report`] for the current state of `provider` on a terminal block with
    /// time `now`.
    fn report(provider: &ProviderStore, index: &Store, now: i64) -> serde_json::Value {
        capture(provider, now, now).unwrap().report(index).unwrap()
    }

    /// A receiver from the spending key with every byte `seed`.
    fn receiver(seed: u8) -> ReceiverBytes {
        crate::probe::common::seeded_receiver(seed)
    }

    /// An index from height 100 whose first block pays `receiver(1)` in transaction
    /// `[7; 32]`, and that block.
    fn paid_index(path: &std::path::Path) -> (Store, receiver_store::IndexedBlock) {
        use receiver_directory::Payment;
        use receiver_store::{Config, IndexedBlock};
        let index = Store::open(
            path,
            Config {
                genesis: [1; 32],
                start_height: 100,
                start_parent: [2; 32],
                start_position: 0,
            },
        )
        .unwrap();
        let payment = Payment {
            height: 100,
            block_hash: [3; 32],
            txid: [7; 32],
            tx_index: 1,
            action_index: 0,
            position: 0,
            action_nullifier: [0; 32],
            cmx: [5; 32],
            // The identity encoding is not a valid ephemeral key.
            ephemeral_key: [7; 32],
            ciphertext_prefix: [0; 52],
        };
        let block = IndexedBlock {
            height: 100,
            hash: [3; 32],
            parent: [2; 32],
            start_position: 0,
            end_position: 1,
            coinbase_actions: 0,
            payments: vec![(receiver(1), payment)],
            commitments: vec![[5; 32]],
        };
        (index, block)
    }

    /// A payout found before a rewind removed its payment is reported missing, after
    /// both databases reopen, until reindexing restores the payment, with no write to
    /// the provider store after the payout is recorded.
    #[test]
    fn a_rewind_reports_a_found_payout_missing() {
        let dir = tempfile::tempdir().unwrap();
        let (provider_path, index_path) = (
            dir.path().join("providers.sqlite"),
            dir.path().join("directory.sqlite"),
        );
        let (mut index, block) = paid_index(&index_path);
        index.append(&block).unwrap();
        let now = 1_000_000;
        ProviderStore::open(&provider_path)
            .unwrap()
            .record(
                "near-payouts",
                Some(0),
                &[],
                &[(receiver(1), [7; 32], now - 2 * 60 * 60)],
                now,
                now - 2 * 60 * 60,
            )
            .unwrap();
        let provider = ProviderStore::open(&provider_path).unwrap();
        assert_eq!(report(&provider, &index, now)["payouts_missing"], 0);
        index.rewind(99, [2; 32]).unwrap();
        drop((provider, index));
        let provider = ProviderStore::open(&provider_path).unwrap();
        let (mut index, block) = paid_index(&index_path);
        assert_eq!(report(&provider, &index, now)["payouts_missing"], 1);
        index.append(&block).unwrap();
        assert_eq!(report(&provider, &index, now)["payouts_missing"], 0);
    }

    /// A payout first seen complete more than an hour before the terminal block's time
    /// with no indexed payment in its transaction is reported missing, even to a
    /// receiver paid before; a newer one is pending, not checked yet. An uncheckable
    /// payout is counted apart while first seen within the last day.
    #[test]
    fn report_counts_completed_payouts_missing_from_the_index() {
        let dir = tempfile::tempdir().unwrap();
        let mut provider = ProviderStore::open(dir.path().join("providers.sqlite")).unwrap();
        let (mut index, block) = paid_index(&dir.path().join("directory.sqlite"));
        index.append(&block).unwrap();
        let now = 1_000_000;
        // The indexed payout, then a later one to the same receiver that is not indexed.
        let early = now - 2 * 60 * 60;
        let payouts = [
            (receiver(1), [7; 32], early),
            (receiver(1), [8; 32], early),
            (receiver(2), [0; 32], early),
        ];
        provider
            .record("near-payouts", Some(0), &[], &payouts, now, early)
            .unwrap();
        let payouts = [(receiver(2), [9; 32], now - 60)];
        provider
            .record("near-payouts", None, &[], &payouts, now, now - 60)
            .unwrap();
        provider
            .record("near-payouts", None, &[], &[], now, now - 30)
            .unwrap();
        let first = report(&provider, &index, now);
        assert_eq!(first["payouts_checked"], 2);
        assert_eq!(first["payouts_missing"], 1);
        assert_eq!(first["payouts_uncheckable"], 1);
        // A day later every payout is checked: the found one again, and the missing
        // one, though old, still reported beside the newer unindexed payout.
        let report = report(&provider, &index, now + 24 * 60 * 60);
        assert_eq!(report["payouts_checked"], 3);
        assert_eq!(report["payouts_missing"], 2);
        assert_eq!(report["payouts_uncheckable"], 0);
        assert_eq!(report["feeds"]["near-payouts"], now - 30);
        assert!(report["feeds"]["near-refunds"].is_null());
    }

    /// A completion ages from when a read saw it, not when the read began: after a read
    /// lasting two hours it stays pending, checkable or not, until its grace from then
    /// ends, while the feed reports the read's start. A later read, after a reopen,
    /// keeps the earliest observation.
    #[test]
    fn completions_age_from_when_a_read_saw_them() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("providers.sqlite");
        let (index, _) = paid_index(&dir.path().join("directory.sqlite"));
        let (began, seen) = (1_000_000, 1_000_000 + 2 * 60 * 60);
        let record = |read_at, seen_at| {
            let payouts = [
                (receiver(1), [8; 32], seen_at),
                (uncheckable_receiver(), [0; 32], seen_at),
            ];
            ProviderStore::open(&path)
                .unwrap()
                .record("near-payouts", Some(0), &[], &payouts, read_at, read_at)
                .unwrap();
        };
        record(began, seen);
        let due = seen + COMPLETION_GRACE_SECS;
        let provider = ProviderStore::open(&path).unwrap();
        let pending = report(&provider, &index, due - 1);
        assert_eq!(pending["feeds"]["near-payouts"], began);
        assert_eq!(pending["payouts_checked"], 0);
        assert_eq!(pending["payouts_uncheckable"], 0);
        drop(provider);
        record(seen, seen + 60 * 60);
        let provider = ProviderStore::open(&path).unwrap();
        let checked = report(&provider, &index, due);
        assert_eq!(checked["feeds"]["near-payouts"], seen);
        assert_eq!(checked["payouts_missing"], 1);
        assert_eq!(checked["payouts_uncheckable"], 1);
    }

    #[test]
    fn explorer_records_parse() {
        let page: Vec<Swap> = serde_json::from_str(
            r#"[{"recipient":"r","refundTo":"t","createdAtTimestamp":5,"depositAddress":"d",
                "depositMemo":null,"status":"PENDING_DEPOSIT","amountIn":"0"}]"#,
        )
        .unwrap();
        assert_eq!(page[0].created_at_timestamp, 5);
        assert_eq!(page[0].deposit_memo, None);
    }

    /// Reads the last hour of the live feed. Needs a partner key in
    /// `NEAR_INTENTS_EXPLORER`.
    #[tokio::test]
    #[ignore]
    async fn live_feed_records_recent_receivers() {
        let key = std::env::var("NEAR_INTENTS_EXPLORER").expect("partner key");
        let dir = tempfile::tempdir().unwrap();
        let mut store = ProviderStore::open(dir.path().join("providers.sqlite")).unwrap();
        let mut explorer = Explorer::new(key, SWEEP_PAGES).unwrap();
        let since = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
            - 3600;
        let payouts = explorer.sync(&mut store, Feed::Payouts).await.unwrap();
        let refunds = explorer.sync(&mut store, Feed::Refunds).await.unwrap();
        let (recent, seen) = store.sets(since).unwrap();
        eprintln!(
            "payouts={payouts} refunds={refunds} recent={} seen={}",
            recent.len(),
            seen.len()
        );
        assert!(payouts > 0 && refunds > 0);
        assert!(store.cursor(Feed::Payouts.name()).unwrap().unwrap() >= since);
        // A second read starts from the cursor and finds nothing new to lose.
        explorer.sync(&mut store, Feed::Payouts).await.unwrap();
        assert!(store.sets(since).unwrap().0.len() >= recent.len());
    }
}
