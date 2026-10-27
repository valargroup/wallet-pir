//! Optional NEAR payout and refund audit. Provider data never governs issuance or
//! publication.
//!
//! It answers one question independently of NEAR's Zcash addresses: did NEAR send each
//! reported payout and refund so a wallet can find it (zero OVK, Ironwood)? Payouts name
//! their transaction. Refunds do not, so a refund counts as found when any of its swap's
//! reported origin transactions pays the refund receiver. Index completeness is the
//! canary's, not this audit's; see [`receiver_store::audit`] for when items retire.
use receiver_store::{
    Confirmation, OpenRefund, PaymentIndex, PayoutChange, PayoutStore, RefundChange,
    ReportedRefund, Store, RETIRE_AFTER_SECS,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
    time::Duration,
};
use zakura_pir_publisher::receiver::crypto::{Network, Receiver};
use zakura_pir_publisher::receiver::{Hash, ReceiverBytes};

const ENDPOINT: &str = "https://explorer.near-intents.org/api/v0/transactions";
/// 1Click's per-swap status, which needs no key.
const STATUS_ENDPOINT: &str = "https://1click.chaindefuser.com/v0/status";
/// The status of a completed swap, whose payout transactions the explorer reports.
const SUCCESS: &str = "SUCCESS";
/// The status of a failed swap whose deposit NEAR refunded.
const REFUNDED: &str = "REFUNDED";
/// Refunds sent back on the origin chain, the only kind that reaches Zcash.
const ORIGIN_CHAIN: &str = "ORIGIN_CHAIN";
/// Bound on one page's body: a record is about 1 KB, so eight times a full page.
const MAX_PAGE_BYTES: usize = 8 * 1024 * 1024;
/// Bound on one status body.
const MAX_STATUS_BYTES: usize = 256 * 1024;
/// The explorer's per-partner rate limit, with margin.
const REQUEST_INTERVAL: Duration = Duration::from_millis(5_500);
/// Bound on one read, about a month of swaps, so an explorer that ignores paging cannot
/// loop forever. It bounds a first read too: one from a `since` too far back fails
/// rather than record a feed that skipped unread history.
const MAX_PAGES: usize = 500;
/// Overlap for late completions, and retention for uncheckable audit records.
pub const AUDIT_LOOKBACK_SECS: i64 = 24 * 60 * 60;
/// The refund feed's overlap: refunds are few, so it reads back further.
const REFUND_LOOKBACK_SECS: i64 = 7 * 24 * 60 * 60;
/// How long before a publication's terminal block's time a read must first have seen
/// a payout complete, or a refund's newest candidate, for the report to check it. The
/// chain's clock, capped at the wall clock, measures it, so items stay pending, not
/// missing, while the chain pauses or behind `--depth`.
const COMPLETION_GRACE_SECS: i64 = 60 * 60;
/// The refund feeds read on every this many payout polls.
const REFUND_POLLS: u64 = 5;
/// Refund swaps first seen this long ago leave the report and the audit task.
const COHORT_SECS: i64 = 7 * 24 * 60 * 60;
/// The report's window for new discrepancies, short so one clears and the next
/// notifies again; monitoring holds warnings, which never re-notify while active.
const NEW_SECS: i64 = 3 * 60 * 60;
/// The report's window for a pattern of discrepancies.
const DAY_SECS: i64 = 24 * 60 * 60;
/// How long a due, unmatched excess refund may go unconfirmed before it is reported.
const UNCONFIRMED_SECS: i64 = 60 * 60;
/// Bounds on the audit task's extra requests per poll.
const CONFIRMS_PER_POLL: usize = 10;
const REFRESHES_PER_POLL: usize = 5;
/// How often one unresolved swap may be re-read from the explorer.
const REFRESH_SECS: i64 = 60 * 60;
/// The most deposit addresses the report lists for the day's missing refunds.
const MISSING_LISTED: usize = 10;
/// A completed payout to an Orchard receiver that NEAR reported without a parsable
/// transaction is recorded under [`uncheckable_receiver`] and this many zero bytes,
/// which no transaction hash starts with, then a digest of its swap's length-prefixed
/// deposit address and optional memo, the explorer's identity for a swap, so one row
/// per swap. [`Capture::report`] counts these (`payouts_uncheckable`) while first seen
/// within [`AUDIT_LOOKBACK_SECS`].
const UNCHECKABLE_ZEROS: usize = 16;

/// Whether `txid` is the record of an uncheckable payout; see [`UNCHECKABLE_ZEROS`].
fn uncheckable(txid: &Hash) -> bool {
    txid[..UNCHECKABLE_ZEROS] == [0; UNCHECKABLE_ZEROS]
}

/// The receiver of every uncheckable payout record: a fixed valid address (zero
/// diversifier, transmission key with x-coordinate 1) that the audit uses only as a
/// sentinel and never checks against the index.
fn uncheckable_receiver() -> ReceiverBytes {
    let mut bytes = [0; 43];
    bytes[11] = 1;
    *Receiver::from_bytes(bytes)
        .expect("a valid address")
        .as_bytes()
}

/// A swap's identity: a digest of its length-prefixed deposit address and optional
/// memo, as the explorer pages by.
fn identity(address: &str, memo: Option<&str>) -> Hash {
    Sha256::new()
        .chain_update((address.len() as u64).to_le_bytes())
        .chain_update(address.as_bytes())
        .chain_update([u8::from(memo.is_some())])
        .chain_update(memo.unwrap_or_default())
        .finalize()
        .into()
}

/// Audit inputs read from one consistent database snapshot.
pub struct Capture {
    /// When each feed's last complete read began.
    feeds: serde_json::Map<String, serde_json::Value>,
    /// Unretired checkable payouts first seen complete more than
    /// [`COMPLETION_GRACE_SECS`] before the terminal block's time.
    payouts: Vec<(ReceiverBytes, Hash)>,
    /// Checkable payouts retired after being found, which stay checked.
    retired: u64,
    /// Uncheckable payouts first seen in that span but within [`AUDIT_LOOKBACK_SECS`].
    uncheckable: usize,
    /// Unretired refund swaps first seen within [`COHORT_SECS`] of the anchor.
    refunds: Vec<OpenRefund>,
    /// Each refund feed's progress watermark; see [`PayoutStore::checked_through`].
    through: [Option<i64>; 2],
    anchor: i64,
}

/// Captures the audit for chain time `anchor_time` in one read transaction.
pub fn capture(store: &PayoutStore, anchor_time: i64) -> Result<Capture> {
    store.view(|store| {
        let mut feeds = serde_json::Map::new();
        for feed in Feed::ALL {
            feeds.insert(feed.name().into(), store.read(feed.name())?.into());
        }
        let due = anchor_time - COMPLETION_GRACE_SECS;
        let mut payouts = store.due_payouts(due)?;
        payouts.retain(|p| !uncheckable(&p.1));
        Ok(Capture {
            feeds,
            payouts,
            retired: store.retired_payouts()?,
            uncheckable: store.uncheckable_payouts(
                anchor_time - AUDIT_LOOKBACK_SECS,
                due,
                UNCHECKABLE_ZEROS,
            )?,
            refunds: store.open_refunds(anchor_time - COHORT_SECS)?,
            through: [
                store.checked_through(Feed::Refunds.name())?,
                store.checked_through(Feed::ExcessRefunds.name())?,
            ],
            anchor: anchor_time,
        })
    })
}

/// Where a refund swap stands in one report.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Standing {
    /// Not due, or due and awaiting a 1Click answer that is not overdue.
    Pending,
    /// No candidate yet, so nothing to check.
    Uncheckable,
    /// A candidate pays the refund receiver.
    Found,
    /// A refund with no candidate paying the refund receiver.
    Missing,
    /// Due and unmatched, but 1Click could not confirm the refund for an hour.
    Unconfirmed,
}

/// `refund`'s standing at chain time `anchor`, given whether a candidate pays it.
fn standing(refund: &OpenRefund, matched: bool, anchor: i64) -> Standing {
    let Some(due) = refund
        .candidates_changed_at
        .map(|t| t + COMPLETION_GRACE_SECS)
    else {
        return Standing::Uncheckable;
    };
    match () {
        _ if due > anchor => Standing::Pending,
        _ if matched => Standing::Found,
        _ if refund.feed == Feed::Refunds.name() || refund.refund_confirmed_at.is_some() => {
            Standing::Missing
        }
        _ if refund.confirm_last == Some(Confirmation::Zero) => Standing::Pending,
        _ if anchor - due >= UNCONFIRMED_SECS => Standing::Unconfirmed,
        _ => Standing::Pending,
    }
}

/// Whether any of `refund`'s candidates pays its receiver, by `paid_in`.
fn matched<E>(
    refund: &OpenRefund,
    mut paid_in: impl FnMut(&ReceiverBytes, &Hash) -> std::result::Result<bool, E>,
) -> std::result::Result<bool, E> {
    for txid in &refund.candidates {
        if paid_in(&refund.receiver, txid)? {
            return Ok(true);
        }
    }
    Ok(false)
}

impl Capture {
    /// The audit's health for monitoring.
    ///
    /// Payouts: how many payouts first seen complete more than an hour before the
    /// terminal block's time and not yet retired have no payment to their receiver in
    /// the transaction NEAR reported in `index` (`payouts_missing`), and how many have
    /// been checked in all (`payouts_checked`, retired ones included). A missing payout
    /// means the indexer missed it, or NEAR paid it without the zero OVK, which a seed
    /// restore cannot find. Payouts to an Orchard receiver that NEAR reported complete
    /// without a parsable transaction cannot be looked up; `payouts_uncheckable` counts
    /// those first seen in the same span but within the last day, so it clears on its
    /// own. Payouts to other recipients are not checked.
    ///
    /// Refunds, over unretired swaps first seen within a week: a swap is due an hour
    /// after its newest candidate, and missing when none of its candidates pays its
    /// refund receiver (for an excess refund, once 1Click confirms one was made).
    /// `refunds_missing_new` and `refunds_missing_day` count those that became due in
    /// the last 3 h and 24 h, `refunds_unconfirmed_new` the excess refunds 1Click could
    /// not confirm, and `refunds_uncheckable_new` the refunded swaps reported without a
    /// transaction. Each feed's `*_checked_through` only moves forward.
    pub fn report(&self, index: &Store) -> Result<serde_json::Value> {
        self.report_with(|receiver, txid| index.paid_in(receiver, txid))
    }

    /// [`Self::report`] against any index's `paid_in`.
    fn report_with(
        &self,
        mut paid_in: impl FnMut(
            &ReceiverBytes,
            &Hash,
        ) -> std::result::Result<bool, receiver_store::Error>,
    ) -> Result<serde_json::Value> {
        let mut missing = 0;
        for (receiver, txid) in &self.payouts {
            if !paid_in(receiver, txid)? {
                missing += 1;
            }
        }
        let anchor = self.anchor;
        let recent = |since: i64, refund: &OpenRefund| {
            refund
                .candidates_changed_at
                .is_some_and(|t| t + COMPLETION_GRACE_SECS > anchor - since)
        };
        let (mut checked, mut pending, mut lost, mut new, mut day, mut unconfirmed) =
            (0, 0, 0, 0, 0, 0);
        let mut uncheckable_new = 0;
        let mut deposits = Vec::new();
        for refund in &self.refunds {
            match standing(refund, matched(refund, &mut paid_in)?, anchor) {
                Standing::Pending => pending += 1,
                Standing::Uncheckable => {
                    let at = refund.seen_at + COMPLETION_GRACE_SECS;
                    uncheckable_new += usize::from(at <= anchor && at > anchor - NEW_SECS);
                }
                Standing::Found => checked += 1,
                Standing::Missing => {
                    checked += 1;
                    lost += 1;
                    new += usize::from(recent(NEW_SECS, refund));
                    if recent(DAY_SECS, refund) {
                        day += 1;
                        if deposits.len() < MISSING_LISTED {
                            deposits.push(refund.deposit_address.clone());
                        }
                    }
                }
                Standing::Unconfirmed => {
                    pending += 1;
                    unconfirmed += usize::from(recent(NEW_SECS, refund));
                }
            }
        }
        let through = self.through.map(|t| t.map(|t| t + COMPLETION_GRACE_SECS));
        Ok(serde_json::json!({
            "feeds": self.feeds,
            "payouts_checked": self.retired + self.payouts.len() as u64,
            "payouts_missing": missing,
            "payouts_uncheckable": self.uncheckable,
            "refunds_checked": checked,
            "refunds_pending": pending,
            "refunds_missing": lost,
            "refunds_missing_new": new,
            "refunds_missing_day": day,
            "refunds_missing_deposits": deposits,
            "refunds_uncheckable_new": uncheckable_new,
            "refunds_unconfirmed_new": unconfirmed,
            "refunds_checked_through": through[0],
            "excess_refunds_checked_through": through[1],
        }))
    }
}

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// One explorer feed of ZEC swaps.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Feed {
    /// Swaps into ZEC, whose `recipient` is a payout address.
    Payouts,
    /// Failed swaps out of ZEC, refunded to `refundTo`.
    Refunds,
    /// Successful swaps out of ZEC whose unused deposit was refunded to `refundTo`,
    /// recognised by a second origin transaction.
    ExcessRefunds,
}

impl Feed {
    /// Every feed, in [`Reads`] order.
    const ALL: [Self; 3] = [Self::Payouts, Self::Refunds, Self::ExcessRefunds];

    /// The feed's cursor name in the provider store. The previous release's
    /// `near-payouts` cursor also bounds its receiver set, so this audit keeps its own
    /// and leaves that one for a rollback to resume from.
    pub fn name(self) -> &'static str {
        match self {
            Self::Payouts => "near-payout-audit",
            Self::Refunds => "near-refund-audit",
            Self::ExcessRefunds => "near-excess-refund-audit",
        }
    }

    /// The explorer parameter that selects this feed's ZEC swaps.
    fn chain_filter(self) -> &'static str {
        match self {
            Self::Payouts => "toChainId",
            Self::Refunds | Self::ExcessRefunds => "fromChainId",
        }
    }

    /// The swap status the feed reads.
    fn status(self) -> &'static str {
        match self {
            Self::Payouts | Self::ExcessRefunds => SUCCESS,
            Self::Refunds => REFUNDED,
        }
    }

    /// Records per page: refunds are few, so a short page holds weeks of them.
    fn page(self) -> usize {
        match self {
            Self::Payouts | Self::ExcessRefunds => 1000,
            Self::Refunds => 100,
        }
    }

    /// How far each read reaches back from its cursor, which is also how far its
    /// first read reaches.
    fn lookback(self) -> i64 {
        match self {
            Self::Payouts | Self::ExcessRefunds => AUDIT_LOOKBACK_SECS,
            Self::Refunds => REFUND_LOOKBACK_SECS,
        }
    }
}

/// This process's evidence that the explorer served its key: when the last read of
/// each feed this process completed began, in Unix seconds. Health reports it as
/// `near.reads`, null until this process completes a read, so it stays null without a
/// key. The report's `feeds` cannot serve as this evidence: they come from
/// `providers.sqlite`, which may hold an earlier process's reads.
#[derive(Clone, Default)]
pub struct Reads(Arc<[AtomicI64; 3]>);

impl Reads {
    /// Records that a read of `feed` that began at `began` completed.
    fn record(&self, feed: Feed, began: i64) {
        self.0[feed as usize].store(began, Ordering::SeqCst);
    }

    /// Health's `near` field: `{"reads": {<feed>: <began>|null, ...}}`. Every feed is
    /// listed, so monitoring can tell a release that reads refunds from one that does
    /// not.
    pub fn health(&self) -> serde_json::Map<String, serde_json::Value> {
        let reads: serde_json::Map<_, _> = Feed::ALL
            .into_iter()
            .map(|feed| {
                let began = self.0[feed as usize].load(Ordering::SeqCst);
                (feed.name().into(), (began != 0).then_some(began).into())
            })
            .collect();
        serde_json::Map::from_iter([("near".into(), serde_json::json!({ "reads": reads }))])
    }
}

/// The audit task's clients, stores and schedule.
pub struct Audit {
    explorer: Explorer,
    status: Status,
    providers: PathBuf,
    index: PathBuf,
    since: i64,
    polls: u64,
    reconciled: bool,
}

impl Audit {
    /// An audit reading with explorer `key` into the provider store at `providers`,
    /// checking against the directory store at `index`, with the payout feed's first
    /// read from `since`.
    pub fn new(key: String, providers: PathBuf, index: PathBuf, since: i64) -> Result<Self> {
        Ok(Self {
            explorer: Explorer::new(key)?,
            status: Status::new(STATUS_ENDPOINT.to_owned())?,
            providers,
            index,
            since,
            polls: 0,
            reconciled: false,
        })
    }
}

/// One audit pass: reads the payout feed, and the refund feeds every
/// [`REFUND_POLLS`]th pass, recording each completed read in `reads`, then settles
/// open items against the index (see [`Audit::settle`]). A failure is logged and
/// leaves its feed's entry unchanged.
pub async fn poll(audit: &mut Audit, reads: &Reads) {
    let mut store = match PayoutStore::open(&audit.providers) {
        Ok(store) => store,
        Err(error) => return tracing::warn!(%error, "near provider store unavailable"),
    };
    if !audit.reconciled {
        match store.reconcile() {
            Ok(opened) => {
                audit.reconciled = true;
                tracing::info!(opened, "near audit reconciled open payouts")
            }
            Err(error) => tracing::warn!(%error, "near audit reconciliation deferred"),
        }
    }
    let refunds = audit.polls.is_multiple_of(REFUND_POLLS);
    audit.polls += 1;
    for feed in Feed::ALL {
        if feed != Feed::Payouts && !refunds {
            continue;
        }
        let began = unix_now();
        let read = match feed {
            Feed::Payouts => audit.explorer.sync(&mut store, feed, audit.since).await,
            _ => audit.explorer.sync_refunds(&mut store, feed).await,
        };
        match read {
            Ok(records) => {
                reads.record(feed, began);
                tracing::info!(feed = feed.name(), records, "near feed read")
            }
            Err(error) => tracing::warn!(feed = feed.name(), %error, "near feed deferred"),
        }
    }
    if let Err(error) = audit.settle(&mut store).await {
        tracing::warn!(%error, "near audit settlement deferred");
    }
}

/// How an open item's found state moves, given whether it is found now; see
/// [`receiver_store::audit`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Step {
    Found,
    Reopen,
    Retire,
}

/// See [`Step`]: found once, reopened by a miss, retired by a later find at least
/// [`RETIRE_AFTER_SECS`] after the first.
fn step(found_at: Option<i64>, matched: bool, now: i64) -> Option<Step> {
    match (matched, found_at) {
        (true, None) => Some(Step::Found),
        (true, Some(at)) if now - at >= RETIRE_AFTER_SECS => Some(Step::Retire),
        (false, Some(_)) => Some(Step::Reopen),
        _ => None,
    }
}

/// When to ask 1Click again after `attempts` earlier answers, `answer` being the
/// latest: zeros back off from 30 min to 6 h, failures from 5 min to 2 h.
fn retry_at(answer: Confirmation, attempts: u32, now: i64) -> Option<i64> {
    let (base, cap) = match answer {
        Confirmation::Refunded => return None,
        Confirmation::Zero => (30 * 60, 6 * 60 * 60),
        Confirmation::Failed => (5 * 60, 2 * 60 * 60),
    };
    Some(now + (base << attempts.min(8)).min(cap))
}

/// The work [`Audit::settle`] found that needs the network.
#[derive(Default)]
struct Followups {
    confirm: Vec<OpenRefund>,
    refresh: Vec<OpenRefund>,
}

impl Audit {
    /// Settles open payouts and refunds against the directory index, opened read-only:
    /// marks found, reopened and retired items and missing refunds, all from one read
    /// finished before any request. Then it re-reads unresolved swaps older than their
    /// feed's overlap and asks 1Click about unmatched excess refunds, both bounded per
    /// poll.
    async fn settle(&mut self, store: &mut PayoutStore) -> Result<()> {
        let now = unix_now();
        let (payout_changes, refund_changes, mut followups) = self.review(store, now)?;
        store.settle_payouts(&payout_changes, now)?;
        store.settle_refunds(&refund_changes, now)?;
        followups.refresh.truncate(REFRESHES_PER_POLL);
        self.follow_up(store, followups).await
    }

    /// Reviews every open item against the index in one read, returning the verdicts
    /// and the follow-ups that need the network.
    #[allow(clippy::type_complexity)]
    fn review(
        &self,
        store: &PayoutStore,
        now: i64,
    ) -> Result<(
        Vec<(ReceiverBytes, Hash, PayoutChange)>,
        Vec<(Hash, RefundChange)>,
        Followups,
    )> {
        let payouts = store.open_payouts()?;
        let refunds = store.open_refunds(now - COHORT_SECS)?;
        let overlap = |feed: Feed| -> Result<Option<i64>> {
            Ok(store.cursor(feed.name())?.map(|c| c - feed.lookback()))
        };
        let overlaps = [overlap(Feed::Refunds)?, overlap(Feed::ExcessRefunds)?];
        let index = PaymentIndex::open(&self.index)?;
        Ok(index.view(|index| {
            let mut payout_changes = Vec::new();
            for p in &payouts {
                let change = if uncheckable(&p.txid) {
                    (p.seen_at < now - AUDIT_LOOKBACK_SECS).then_some(PayoutChange::Expire)
                } else {
                    step(p.found_at, index.paid_in(&p.receiver, &p.txid)?, now).map(|s| match s {
                        Step::Found => PayoutChange::Found,
                        Step::Reopen => PayoutChange::Reopen,
                        Step::Retire => PayoutChange::Retire,
                    })
                };
                payout_changes.extend(change.map(|c| (p.receiver, p.txid, c)));
            }
            let mut refund_changes = Vec::new();
            let mut followups = Followups::default();
            for r in &refunds {
                let found = matched(r, |receiver, txid| index.paid_in(receiver, txid))?;
                refund_changes.extend(step(r.found_at, found, now).map(|s| {
                    let change = match s {
                        Step::Found => RefundChange::Found,
                        Step::Reopen => RefundChange::Reopen,
                        Step::Retire => RefundChange::Retire,
                    };
                    (r.swap, change)
                }));
                let due = r
                    .candidates_changed_at
                    .is_some_and(|t| t + COMPLETION_GRACE_SECS <= now);
                if found || !due {
                    continue;
                }
                let excess = r.feed == Feed::ExcessRefunds.name();
                if !excess && r.checked_at.is_none() {
                    refund_changes.push((r.swap, RefundChange::Missing));
                }
                if excess
                    && r.refund_confirmed_at.is_none()
                    && r.confirm_after.is_none_or(|at| at <= now)
                {
                    followups.confirm.push(r.clone());
                }
                let overlap = overlaps[usize::from(excess)];
                if overlap.is_some_and(|o| r.created_at < o)
                    && r.refreshed_at.is_none_or(|at| at + REFRESH_SECS <= now)
                {
                    followups.refresh.push(r.clone());
                }
            }
            Ok((payout_changes, refund_changes, followups))
        })?)
    }

    /// The rest of [`Self::settle`]: the network follow-ups.
    async fn follow_up(&mut self, store: &mut PayoutStore, mut followups: Followups) -> Result<()> {
        for refund in followups.refresh {
            match self.explorer.search(&refund).await {
                Ok(candidates) => {
                    store.merge_candidates(&refund.swap, &candidates, unix_now())?;
                    store.settle_refunds(&[(refund.swap, RefundChange::Refreshed)], unix_now())?;
                }
                Err(error) => {
                    tracing::warn!(%error, "near refund refresh deferred");
                    break;
                }
            }
        }
        // Oldest retry first, so swaps that keep failing cannot starve newer ones.
        followups
            .confirm
            .sort_by_key(|r| r.confirm_after.unwrap_or(i64::MIN));
        followups.confirm.truncate(CONFIRMS_PER_POLL);
        for refund in followups.confirm {
            let now = unix_now();
            let (answer, origin, stop) = match self.status.refunded(&refund).await {
                Ok((Some(amount), origin)) if amount > 0 => (Confirmation::Refunded, origin, false),
                Ok((Some(_), origin)) => (Confirmation::Zero, origin, false),
                Ok((None, _)) => (Confirmation::Failed, Vec::new(), false),
                Err(StatusError::Swap(error)) => {
                    tracing::warn!(%error, "near refund confirmation failed");
                    (Confirmation::Failed, Vec::new(), false)
                }
                Err(StatusError::Service(error)) => {
                    tracing::warn!(%error, "near refund confirmation deferred");
                    (Confirmation::Failed, Vec::new(), true)
                }
            };
            let retry = retry_at(answer, refund.confirm_attempts, now);
            store.settle_refunds(
                &[(refund.swap, RefundChange::Confirmed(answer, retry))],
                now,
            )?;
            // After the answer, so a new candidate's restarted grace clears a zero.
            store.merge_candidates(&refund.swap, &origin, now)?;
            if stop {
                break;
            }
        }
        Ok(())
    }
}

/// Seconds since the Unix epoch.
pub(crate) fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after 1970")
        .as_secs() as i64
}

/// One explorer record, reduced to what the feeds read. Only the paging fields are
/// required, so a record missing an address or status does not fail the read.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Swap {
    created_at_timestamp: i64,
    deposit_address: String,
    deposit_memo: Option<String>,
    recipient: Option<String>,
    status: Option<String>,
    refund_to: Option<String>,
    refund_type: Option<String>,
    /// The payout transactions, displayed (reversed) hex as on ZEC explorers.
    destination_chain_tx_hashes: Option<Vec<String>>,
    /// The deposit and refund transactions on the origin chain, in no reliable order.
    origin_chain_tx_hashes: Option<Vec<String>>,
}

impl Swap {
    /// The swap's refund candidates: the origin and destination hashes that parse.
    /// NEAR sometimes lists a ZEC refund under the destination and the other chain's
    /// payout under the origin, and a hash from another chain never pays a Zcash
    /// receiver, so both lists are searched.
    fn refund_candidates(&self) -> Vec<Hash> {
        let mut candidates = parse_hashes(self.origin_chain_tx_hashes.iter().flatten());
        for hash in parse_hashes(self.destination_chain_tx_hashes.iter().flatten()) {
            if !candidates.contains(&hash) {
                candidates.push(hash);
            }
        }
        candidates
    }
}

/// Every parsable displayed hash in `hashes`.
fn parse_hashes<'a>(hashes: impl IntoIterator<Item = &'a String>) -> Vec<Hash> {
    hashes
        .into_iter()
        .filter_map(|hash| zakura_server_chain::hash::parse_display(hash).ok())
        .collect()
}

/// The refund `swap` reports to an Orchard receiver on `feed`, if it is one: an
/// origin-chain refund in the feed's status, with any candidates for a refunded swap,
/// or at least two origin hashes for an excess refund (its deposit and its refund, or
/// the other chain's payout listed there; see [`Swap::refund_candidates`]).
fn reported_refund(feed: Feed, swap: &Swap, created_at: i64) -> Option<ReportedRefund> {
    if swap.status.as_deref() != Some(feed.status())
        || swap.refund_type.as_deref() != Some(ORIGIN_CHAIN)
    {
        return None;
    }
    let receiver = swap.refund_to.as_deref().and_then(orchard_receiver)?;
    if feed == Feed::ExcessRefunds
        && parse_hashes(swap.origin_chain_tx_hashes.iter().flatten()).len() < 2
    {
        return None;
    }
    let candidates = swap.refund_candidates();
    Some(ReportedRefund {
        swap: identity(&swap.deposit_address, swap.deposit_memo.as_deref()),
        receiver,
        deposit_address: swap.deposit_address.clone(),
        deposit_memo: swap.deposit_memo.clone(),
        created_at,
        candidates,
    })
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
}

impl Explorer {
    /// An explorer client that authenticates with `key`.
    pub fn new(key: String) -> Result<Self> {
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
        })
    }

    /// Audits completed ZEC payouts with a bounded overlap for late completion, returning
    /// how many payout records the read found. A failed or oversized read commits
    /// neither payouts nor its cursor.
    pub async fn sync(&mut self, store: &mut PayoutStore, feed: Feed, since: i64) -> Result<usize> {
        let read_at = unix_now();
        let cursor = store.cursor(feed.name())?;
        let floor = cursor.map_or(since, |c| c - feed.lookback());
        let mut newest = cursor.unwrap_or(since).min(read_at);
        let mut completed = Vec::new();
        self.scan(feed, floor, read_at, cursor.is_none(), |swap, created| {
            newest = newest.max(created);
            if let Some(receiver) = swap.recipient.as_deref().and_then(orchard_receiver) {
                completions(swap, receiver, &mut completed);
            }
        })
        .await?;
        let seen_at = unix_now();
        let completed: Vec<_> = completed
            .into_iter()
            .map(|(r, t)| (r, t, seen_at))
            .collect();
        store.record(feed.name(), &completed, newest, read_at)?;
        Ok(completed.len())
    }

    /// Reads one refund feed from `max(floor, cursor - lookback)`, the feed's durable
    /// floor being set on its first read, and returns how many refunds it found. A
    /// failed or oversized read commits neither refunds nor its cursor.
    pub async fn sync_refunds(&mut self, store: &mut PayoutStore, feed: Feed) -> Result<usize> {
        let read_at = unix_now();
        let floor = store.floor(feed.name(), read_at - feed.lookback())?;
        let cursor = store.cursor(feed.name())?;
        let from = cursor.map_or(floor, |c| (c - feed.lookback()).max(floor));
        let mut newest = cursor.unwrap_or(floor).min(read_at);
        let mut refunds = Vec::new();
        self.scan(feed, from, read_at, cursor.is_none(), |swap, created| {
            newest = newest.max(created);
            refunds.extend(reported_refund(feed, swap, created));
        })
        .await?;
        store.record_refunds(feed.name(), &refunds, newest, read_at, unix_now())?;
        Ok(refunds.len())
    }

    /// The parsable origin transactions the explorer now reports for `refund`'s swap,
    /// found by deposit address and kept only for the same address and memo.
    async fn search(&mut self, refund: &OpenRefund) -> Result<Vec<Hash>> {
        let query = vec![
            ("search", refund.deposit_address.clone()),
            ("numberOfTransactions", "10".to_owned()),
        ];
        let swaps = self.request(query, 10).await?;
        Ok(swaps
            .iter()
            .filter(|s| {
                s.deposit_address == refund.deposit_address && s.deposit_memo == refund.deposit_memo
            })
            .flat_map(Swap::refund_candidates)
            .collect())
    }

    /// An explorer client that reads `endpoint` instead of NEAR's, without waiting
    /// between requests.
    #[cfg(test)]
    fn at(endpoint: String) -> Self {
        Self {
            endpoint,
            interval: Duration::ZERO,
            ..Self::new(String::new()).unwrap()
        }
    }

    /// Pages `feed`'s swaps in its status, newest first, passing `each` every swap with
    /// its creation time, capped at `read_at`, from `floor` on. Past [`MAX_PAGES`] it
    /// fails, suggesting a later `--near-since` when the read is the feed's `first`.
    async fn scan(
        &mut self,
        feed: Feed,
        floor: i64,
        read_at: i64,
        first: bool,
        mut each: impl FnMut(&Swap, i64),
    ) -> Result<()> {
        let mut after: Option<(String, Option<String>)> = None;
        for page in 0.. {
            if page == self.max_pages {
                return Err(if first {
                    "NEAR explorer first read exceeded its page bound; \
                        start it later with --near-since"
                        .into()
                } else {
                    "NEAR explorer read exceeded its page bound".into()
                });
            }
            let swaps = self.page(feed, after.as_ref()).await?;
            for swap in &swaps {
                let created = swap.created_at_timestamp.min(read_at);
                if created >= floor {
                    each(swap, created);
                }
            }
            match swaps.last() {
                Some(last) if swaps.len() == feed.page() && last.created_at_timestamp >= floor => {
                    let next = Some((last.deposit_address.clone(), last.deposit_memo.clone()));
                    if next == after {
                        return Err("NEAR explorer paging made no progress".into());
                    }
                    after = next;
                }
                _ => break,
            }
        }
        Ok(())
    }

    /// One page of `feed`'s swaps, newest first, older than `after` when given.
    async fn page(
        &mut self,
        feed: Feed,
        after: Option<&(String, Option<String>)>,
    ) -> Result<Vec<Swap>> {
        let mut query = vec![
            (feed.chain_filter(), "zec".to_owned()),
            ("statuses", feed.status().to_owned()),
            ("numberOfTransactions", feed.page().to_string()),
        ];
        if let Some((address, memo)) = after {
            query.push(("lastDepositAddress", address.clone()));
            if let Some(memo) = memo {
                query.push(("lastDepositMemo", memo.clone()));
            }
        }
        self.request(query, feed.page()).await
    }

    /// One explorer request with `query`, at the partner rate, refusing more than
    /// `length` records.
    async fn request(&mut self, query: Vec<(&str, String)>, length: usize) -> Result<Vec<Swap>> {
        tokio::time::sleep_until(self.next_request).await;
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
        if swaps.len() > length {
            return Err("NEAR explorer page exceeds the requested length".into());
        }
        Ok(swaps)
    }
}

/// Why a 1Click status read failed: for this swap only, or for the service, which ends
/// the poll's confirmations.
#[derive(Debug)]
enum StatusError {
    Swap(Box<dyn std::error::Error + Send + Sync>),
    Service(Box<dyn std::error::Error + Send + Sync>),
}

/// 1Click's swap status, which needs no key; used only to confirm unmatched excess
/// refunds.
struct Status {
    http: reqwest::Client,
    endpoint: String,
}

impl Status {
    /// A status client for `endpoint`.
    fn new(endpoint: String) -> Result<Self> {
        Ok(Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .redirect(reqwest::redirect::Policy::none())
                .user_agent("receiver-indexer")
                .build()?,
            endpoint,
        })
    }

    /// `refund`'s swap's refunded amount in base units, `None` when absent or
    /// malformed, with the origin transactions 1Click reports.
    async fn refunded(
        &self,
        refund: &OpenRefund,
    ) -> std::result::Result<(Option<u128>, Vec<Hash>), StatusError> {
        let mut query = vec![("depositAddress", refund.deposit_address.as_str())];
        if let Some(memo) = &refund.deposit_memo {
            query.push(("depositMemo", memo));
        }
        let response = self
            .http
            .get(&self.endpoint)
            .query(&query)
            .send()
            .await
            .map_err(|e| StatusError::Service(e.into()))?;
        let status = response.status();
        if status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err(StatusError::Service(
                format!("1Click returned HTTP {status}").into(),
            ));
        }
        if !status.is_success() {
            return Err(StatusError::Swap(
                format!("1Click returned HTTP {status}").into(),
            ));
        }
        let body = crate::read_limited(response, MAX_STATUS_BYTES)
            .await
            .map_err(|e| StatusError::Service(e.into()))?;
        let body: serde_json::Value =
            serde_json::from_slice(&body).map_err(|e| StatusError::Swap(e.into()))?;
        let details = &body["swapDetails"];
        let amount = details["refundedAmount"]
            .as_str()
            .and_then(|a| a.parse().ok());
        // Both lists, as for explorer records; see `Swap::refund_candidates`.
        let origin = ["originChainTxHashes", "destinationChainTxHashes"]
            .into_iter()
            .flat_map(|field| details[field].as_array().into_iter().flatten())
            .filter_map(|h| h["hash"].as_str())
            .filter_map(|h| zakura_server_chain::hash::parse_display(h).ok())
            .collect();
        Ok((amount, origin))
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
fn completions(swap: &Swap, receiver: ReceiverBytes, out: &mut Vec<(ReceiverBytes, Hash)>) {
    if swap.status.as_deref() != Some(SUCCESS) {
        return;
    }
    let before = out.len();
    out.extend(
        parse_hashes(swap.destination_chain_tx_hashes.iter().flatten())
            .into_iter()
            .map(|txid| (receiver, txid)),
    );
    if out.len() == before {
        let digest = identity(&swap.deposit_address, swap.deposit_memo.as_deref());
        let mut txid = [0; 32];
        txid[UNCHECKABLE_ZEROS..].copy_from_slice(&digest[..32 - UNCHECKABLE_ZEROS]);
        out.push((uncheckable_receiver(), txid));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    /// [`Capture::report`] for the current state of `provider` on a terminal block with
    /// time `now`.
    fn report(provider: &PayoutStore, index: &Store, now: i64) -> serde_json::Value {
        capture(provider, now).unwrap().report(index).unwrap()
    }

    /// A receiver from the spending key with every byte `seed`.
    fn receiver(seed: u8) -> ReceiverBytes {
        crate::probe::common::seeded_receiver(seed)
    }

    /// An index from height 100 whose first block pays `receiver(1)` in transaction
    /// `[7; 32]`, and that block.
    fn paid_index(path: &std::path::Path) -> (Store, receiver_store::IndexedBlock) {
        use receiver_store::{Config, IndexedBlock};
        use zakura_pir_publisher::receiver::Payment;
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
        PayoutStore::open(&provider_path)
            .unwrap()
            .record(
                "near-payout-audit",
                &[(receiver(1), [7; 32], now - 2 * 60 * 60)],
                now,
                now - 2 * 60 * 60,
            )
            .unwrap();
        let provider = PayoutStore::open(&provider_path).unwrap();
        assert_eq!(report(&provider, &index, now)["payouts_missing"], 0);
        index.rewind(99, [2; 32]).unwrap();
        drop((provider, index));
        let provider = PayoutStore::open(&provider_path).unwrap();
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
        let mut provider = PayoutStore::open(dir.path().join("providers.sqlite")).unwrap();
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
            .record("near-payout-audit", &payouts, now, early)
            .unwrap();
        let payouts = [(receiver(2), [9; 32], now - 60)];
        provider
            .record("near-payout-audit", &payouts, now, now - 60)
            .unwrap();
        provider
            .record("near-payout-audit", &[], now, now - 30)
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
        assert_eq!(
            report["feeds"],
            serde_json::json!({"near-payout-audit": now - 30, "near-refund-audit": null,
                "near-excess-refund-audit": null})
        );
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
            PayoutStore::open(&path)
                .unwrap()
                .record("near-payout-audit", &payouts, read_at, read_at)
                .unwrap();
        };
        record(began, seen);
        let due = seen + COMPLETION_GRACE_SECS;
        let provider = PayoutStore::open(&path).unwrap();
        let pending = report(&provider, &index, due - 1);
        assert_eq!(pending["feeds"]["near-payout-audit"], began);
        assert_eq!(pending["payouts_checked"], 0);
        assert_eq!(pending["payouts_uncheckable"], 0);
        drop(provider);
        record(seen, seen + 60 * 60);
        let provider = PayoutStore::open(&path).unwrap();
        let checked = report(&provider, &index, due);
        assert_eq!(checked["feeds"]["near-payout-audit"], seen);
        assert_eq!(checked["payouts_missing"], 1);
        assert_eq!(checked["payouts_uncheckable"], 1);
    }

    /// Health reports only the payout feed's reads, null until one completes.
    #[test]
    fn reads_list_every_feed_null_until_read() {
        let reads = Reads::default();
        let health = |reads: &Reads| serde_json::Value::Object(reads.health());
        let expected = |payouts, refunds| {
            serde_json::json!({"near": {"reads": {"near-payout-audit": payouts,
                "near-refund-audit": refunds, "near-excess-refund-audit": null}}})
        };
        let null = serde_json::Value::Null;
        assert_eq!(health(&reads), expected(null.clone(), null.clone()));
        reads.record(Feed::Payouts, 5);
        reads.record(Feed::Refunds, 6);
        assert_eq!(health(&reads), expected(5.into(), 6.into()));
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
    /// The reduced audit keeps real HTTP bounds and commits nothing on a partial read.
    #[tokio::test]
    async fn audit_reads_only_completed_payouts_and_bounds_failed_reads() {
        use axum::{
            extract::{Query, State},
            routing::get,
            Json, Router,
        };
        use std::sync::atomic::AtomicBool;
        let oversized = Arc::new(AtomicBool::new(false));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let app = Router::new().route("/", get(|State(oversized): State<Arc<AtomicBool>>, Query(query): Query<std::collections::HashMap<String,String>>| async move {
            assert_eq!(query["toChainId"], "zec");
            assert_eq!(query["statuses"], SUCCESS);
            let swap = serde_json::json!({"createdAtTimestamp":100,"depositAddress":"d",
                "recipient":"u14nnj43rj7dpf7qh6gu24fuyu8vld9fgatxd32xre27yqgu6p0yq0sf0t3uxnwts4968hf7d8nvyh4wfzqtmcdt6xzk7el6pn0ufx6pdg",
                "status":"SUCCESS","destinationChainTxHashes":["7f79a9262c301e3da38907fdbfca663c1fcb04b6a890ea737c281fc2ff707e7f"]});
            Json(vec![swap; if oversized.load(Ordering::SeqCst) { Feed::Payouts.page() } else { 1 }])
        })).with_state(oversized.clone());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mut explorer = Explorer::at(origin);
        explorer.max_pages = 1;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.sqlite");
        let mut store = PayoutStore::open(&path).unwrap();
        assert_eq!(
            explorer.sync(&mut store, Feed::Payouts, 0).await.unwrap(),
            1
        );
        let before = store.payouts(i64::MAX).unwrap();
        assert_eq!(before.len(), 1);
        let read = store.read("near-payout-audit").unwrap();
        oversized.store(true, Ordering::SeqCst);
        assert!(explorer.sync(&mut store, Feed::Payouts, 0).await.is_err());
        assert_eq!(store.payouts(i64::MAX).unwrap(), before);
        assert_eq!(store.read("near-payout-audit").unwrap(), read);
        server.abort();
    }

    /// A read leaves the previous release's own feed and receiver set as they were, so
    /// after a rollback that release resumes its receiver scan from its own cursor,
    /// before every swap this audit read, rather than from this audit's.
    #[tokio::test]
    async fn the_audit_leaves_the_previous_releases_receiver_coverage() {
        use axum::{routing::get, Json, Router};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        // A swap created two hours after the previous release's cursor.
        let (cursor, created) = (1_000_000, 1_000_000 + 2 * 60 * 60);
        let app = Router::new().route("/", get(move || async move {
            Json(vec![serde_json::json!({"createdAtTimestamp": created, "depositAddress": "d",
                "recipient": "u14nnj43rj7dpf7qh6gu24fuyu8vld9fgatxd32xre27yqgu6p0yq0sf0t3uxnwts4968hf7d8nvyh4wfzqtmcdt6xzk7el6pn0ufx6pdg",
                "status": "SUCCESS",
                "destinationChainTxHashes": ["7f79a9262c301e3da38907fdbfca663c1fcb04b6a890ea737c281fc2ff707e7f"]})])
        }));
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("providers.sqlite");
        // The previous release's state: its payout feed and a receiver it ingested.
        let previous = |db: &rusqlite::Connection| -> (i64, i64, i64, i64) {
            let (started, cursor, read) = db
                .query_row(
                    "SELECT started_at,cursor,read_at FROM feeds WHERE feed='near-payouts'",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .unwrap();
            let receivers = db
                .query_row("SELECT COUNT(*) FROM receivers", [], |r| r.get(0))
                .unwrap();
            (started, cursor, read, receivers)
        };
        let db = rusqlite::Connection::open(&path).unwrap();
        db.execute_batch(
            "CREATE TABLE receivers (receiver BLOB NOT NULL, payout INTEGER NOT NULL,
                seen_at INTEGER NOT NULL, PRIMARY KEY(receiver,payout)) WITHOUT ROWID;
            CREATE TABLE feeds (feed TEXT PRIMARY KEY, started_at INTEGER,
                cursor INTEGER NOT NULL, read_at INTEGER NOT NULL);
            INSERT INTO feeds VALUES ('near-payouts', 900000, 1000000, 1000000);",
        )
        .unwrap();
        db.execute(
            "INSERT INTO receivers VALUES (?1, 1, 1000000)",
            [receiver(1).as_slice()],
        )
        .unwrap();
        let before = previous(&db);
        let mut store = PayoutStore::open(&path).unwrap();
        let mut explorer = Explorer::at(origin);
        assert_eq!(
            explorer.sync(&mut store, Feed::Payouts, 0).await.unwrap(),
            1
        );
        assert_eq!(store.cursor(Feed::Payouts.name()).unwrap(), Some(created));
        assert_eq!(store.payouts(i64::MAX).unwrap().len(), 1);
        assert_eq!(previous(&db), before);
        // The previous release rescans from its own cursor, less its overlap, which
        // reaches the swap this audit read.
        assert!(before.1 == cursor && cursor < created);
        server.abort();
    }

    /// A mainnet unified address with an Orchard receiver, from a Vizor swap.
    const UA: &str = "u14nnj43rj7dpf7qh6gu24fuyu8vld9fgatxd32xre27yqgu6p0yq0sf0t3uxnwts4968hf7d8nvyh4wfzqtmcdt6xzk7el6pn0ufx6pdg";

    /// A refund of `swap` to `receiver` with `candidates`, created at `created`.
    fn refund(
        swap: u8,
        receiver: ReceiverBytes,
        candidates: &[Hash],
        created: i64,
    ) -> ReportedRefund {
        ReportedRefund {
            swap: [swap; 32],
            receiver,
            deposit_address: format!("deposit-{swap}"),
            deposit_memo: None,
            created_at: created,
            candidates: candidates.to_vec(),
        }
    }

    /// An explorer record for `feed`'s status with `refund_type`, `refund_to` and
    /// `origin` hashes.
    fn swap(status: &str, refund_type: &str, refund_to: &str, origin: &[&str]) -> Swap {
        serde_json::from_value(serde_json::json!({
            "createdAtTimestamp": 5, "depositAddress": "d", "status": status,
            "refundType": refund_type, "refundTo": refund_to, "originChainTxHashes": origin,
        }))
        .unwrap()
    }

    /// Only origin-chain refunds to an Orchard receiver in the feed's status count; an
    /// excess refund needs its deposit and its refund, so at least two hashes.
    #[test]
    fn refund_feeds_select_origin_chain_refunds_to_orchard_receivers() {
        let one = ["7f79a9262c301e3da38907fdbfca663c1fcb04b6a890ea737c281fc2ff707e7f"];
        let two = [
            one[0],
            "1f79a9262c301e3da38907fdbfca663c1fcb04b6a890ea737c281fc2ff707e7f",
        ];
        let pick = |feed, s: Swap| reported_refund(feed, &s, 5).map(|r| r.candidates.len());
        assert_eq!(
            pick(Feed::Refunds, swap(REFUNDED, ORIGIN_CHAIN, UA, &one)),
            Some(1)
        );
        assert_eq!(
            pick(Feed::Refunds, swap(REFUNDED, ORIGIN_CHAIN, UA, &[])),
            Some(0)
        );
        assert_eq!(
            pick(Feed::Refunds, swap(REFUNDED, "INTENTS", UA, &one)),
            None
        );
        assert_eq!(
            pick(
                Feed::Refunds,
                swap(
                    REFUNDED,
                    ORIGIN_CHAIN,
                    "t1KfwsnwJeNRVjQGBDZhwKskpQbih2qx5Ua",
                    &one
                )
            ),
            None
        );
        assert_eq!(
            pick(Feed::Refunds, swap(SUCCESS, ORIGIN_CHAIN, UA, &one)),
            None
        );
        assert_eq!(
            pick(Feed::ExcessRefunds, swap(SUCCESS, ORIGIN_CHAIN, UA, &one)),
            None
        );
        assert_eq!(
            pick(Feed::ExcessRefunds, swap(SUCCESS, ORIGIN_CHAIN, UA, &two)),
            Some(2)
        );
        assert_eq!(
            pick(Feed::ExcessRefunds, swap(REFUNDED, ORIGIN_CHAIN, UA, &two)),
            None
        );
    }

    /// NEAR sometimes lists a ZEC excess refund under the destination and the other
    /// chain's payout under the origin (seen in production), so both are candidates.
    #[test]
    fn refund_candidates_include_destination_hashes() {
        let refund = "6f2e39f1aa4294f3356f8f0c6eac9c023ebaf73031039d1d628116449564c8f2";
        let mut record = swap(
            SUCCESS,
            ORIGIN_CHAIN,
            UA,
            &[
                "f3391d7d43d634eaad675225fbe68aa8d4090b0270d7cb3f4b0016fe559b55b2",
                "60466152ed1ff222c58eb928b346b515363fdd96b31364abb8b64d2eacd9b662",
            ],
        );
        record.destination_chain_tx_hashes = Some(vec![refund.into()]);
        let candidates = reported_refund(Feed::ExcessRefunds, &record, 5)
            .unwrap()
            .candidates;
        assert_eq!(candidates.len(), 3);
        assert!(candidates.contains(&zakura_server_chain::hash::parse_display(refund).unwrap()));
    }

    /// A refunded swap is found when any candidate pays its receiver, in any order,
    /// missing when none does, and uncheckable without candidates. Discrepancies count
    /// as new for 3 h and toward the day for 24 h.
    #[test]
    fn refund_report_matches_any_candidate_and_ages_discrepancies() {
        let dir = tempfile::tempdir().unwrap();
        let mut provider = PayoutStore::open(dir.path().join("providers.sqlite")).unwrap();
        let (mut index, block) = paid_index(&dir.path().join("directory.sqlite"));
        index.append(&block).unwrap();
        let now = 1_000_000;
        let seen = now - 2 * 60 * 60;
        let refunds = [
            // Deposit then refund, refund then deposit: both found.
            refund(1, receiver(1), &[[6; 32], [7; 32]], seen),
            refund(2, receiver(1), &[[7; 32], [6; 32]], seen),
            // Deposit only, or a refund sent where Receiver PIR cannot find it.
            refund(3, receiver(2), &[[8; 32]], seen),
            refund(4, receiver(2), &[], seen),
        ];
        provider
            .record_refunds(Feed::Refunds.name(), &refunds, seen, seen, seen)
            .unwrap();
        let first = report(&provider, &index, now);
        assert_eq!(first["refunds_checked"], 3);
        assert_eq!(first["refunds_missing"], 1);
        assert_eq!(first["refunds_missing_new"], 1);
        assert_eq!(first["refunds_missing_day"], 1);
        assert_eq!(
            first["refunds_missing_deposits"],
            serde_json::json!(["deposit-3"])
        );
        assert_eq!(first["refunds_uncheckable_new"], 1);
        let later = report(&provider, &index, now + 3 * 60 * 60);
        assert_eq!(later["refunds_missing"], 1);
        assert_eq!(later["refunds_missing_new"], 0);
        assert_eq!(later["refunds_missing_day"], 1);
        assert_eq!(later["refunds_uncheckable_new"], 0);
        let next_day = report(&provider, &index, now + 24 * 60 * 60);
        assert_eq!(next_day["refunds_missing_day"], 0);
        assert_eq!(next_day["refunds_missing"], 1);
    }

    /// A candidate added later restarts the swap's grace, so a deposit-only list that
    /// gains its refund is pending, then found, never missing on the new hash.
    #[test]
    fn a_late_candidate_restarts_the_grace() {
        let dir = tempfile::tempdir().unwrap();
        let mut provider = PayoutStore::open(dir.path().join("providers.sqlite")).unwrap();
        let (mut index, block) = paid_index(&dir.path().join("directory.sqlite"));
        let now = 1_000_000;
        let seen = now - 3 * 60 * 60;
        provider
            .record_refunds(
                Feed::Refunds.name(),
                &[refund(1, receiver(1), &[[6; 32]], seen)],
                seen,
                seen,
                seen,
            )
            .unwrap();
        assert_eq!(report(&provider, &index, now)["refunds_missing"], 1);
        assert!(provider
            .merge_candidates(&[1; 32], &[[7; 32]], now)
            .unwrap());
        assert!(!provider
            .merge_candidates(&[1; 32], &[[7; 32]], now)
            .unwrap());
        let report_at = |at| report(&provider, &index, at);
        assert_eq!(report_at(now + 30 * 60)["refunds_missing"], 0);
        assert_eq!(report_at(now + 30 * 60)["refunds_pending"], 1);
        // Missing once due, until the refund is indexed.
        assert_eq!(report_at(now + COMPLETION_GRACE_SECS)["refunds_missing"], 1);
        index.append(&block).unwrap();
        assert_eq!(
            report(&provider, &index, now + COMPLETION_GRACE_SECS)["refunds_checked"],
            1
        );
        assert_eq!(
            report(&provider, &index, now + COMPLETION_GRACE_SECS)["refunds_missing"],
            0
        );
    }

    /// An unmatched excess refund is unconfirmed until 1Click answers, pending on a
    /// zero, and missing once 1Click reports a refund. A positive answer is kept when a
    /// new candidate restarts the grace, but a zero is cleared.
    #[test]
    fn excess_refunds_are_missing_only_once_confirmed() {
        let dir = tempfile::tempdir().unwrap();
        let mut provider = PayoutStore::open(dir.path().join("providers.sqlite")).unwrap();
        let (index, _) = paid_index(&dir.path().join("directory.sqlite"));
        let now = 1_000_000;
        let seen = now - 2 * 60 * 60;
        let feed = Feed::ExcessRefunds.name();
        provider
            .record_refunds(
                feed,
                &[refund(1, receiver(2), &[[8; 32], [9; 32]], seen)],
                seen,
                seen,
                seen,
            )
            .unwrap();
        let standing = |provider: &PayoutStore, at| {
            let r = report(provider, &index, at);
            (
                r["refunds_missing"].clone(),
                r["refunds_unconfirmed_new"].clone(),
                r["refunds_pending"].clone(),
            )
        };
        assert_eq!(standing(&provider, now), (0.into(), 1.into(), 1.into()));
        let answer = |provider: &mut PayoutStore, answer| {
            provider
                .settle_refunds(
                    &[(
                        [1; 32],
                        RefundChange::Confirmed(answer, retry_at(answer, 0, now)),
                    )],
                    now,
                )
                .unwrap()
        };
        answer(&mut provider, Confirmation::Failed);
        assert_eq!(standing(&provider, now), (0.into(), 1.into(), 1.into()));
        answer(&mut provider, Confirmation::Zero);
        assert_eq!(standing(&provider, now), (0.into(), 0.into(), 1.into()));
        // A new candidate clears the zero.
        provider
            .merge_candidates(&[1; 32], &[[10; 32]], seen)
            .unwrap();
        assert_eq!(standing(&provider, now), (0.into(), 1.into(), 1.into()));
        answer(&mut provider, Confirmation::Refunded);
        assert_eq!(standing(&provider, now), (1.into(), 0.into(), 0.into()));
        provider
            .merge_candidates(&[1; 32], &[[11; 32]], seen)
            .unwrap();
        assert_eq!(standing(&provider, now), (1.into(), 0.into(), 0.into()));
        assert_eq!(
            report(&provider, &index, now)["excess_refunds_checked_through"],
            seen + COMPLETION_GRACE_SECS
        );
    }

    /// Found once, reopened by a miss, retired only by a find at least two hours after
    /// the first; a miss never retires.
    #[test]
    fn items_retire_only_on_a_late_find() {
        let at = 1_000_000;
        assert_eq!(step(None, true, at), Some(Step::Found));
        assert_eq!(step(Some(at), true, at + RETIRE_AFTER_SECS - 1), None);
        assert_eq!(
            step(Some(at), true, at + RETIRE_AFTER_SECS),
            Some(Step::Retire)
        );
        assert_eq!(
            step(Some(at), false, at + RETIRE_AFTER_SECS),
            Some(Step::Reopen)
        );
        assert_eq!(step(None, false, at + 10 * RETIRE_AFTER_SECS), None);
    }

    /// Zeros back off from 30 min to 6 h, failures from 5 min to 2 h; a refund is final.
    #[test]
    fn confirmations_back_off() {
        assert_eq!(retry_at(Confirmation::Zero, 0, 0), Some(30 * 60));
        assert_eq!(retry_at(Confirmation::Zero, 20, 0), Some(6 * 60 * 60));
        assert_eq!(retry_at(Confirmation::Failed, 1, 0), Some(10 * 60));
        assert_eq!(retry_at(Confirmation::Failed, 20, 0), Some(2 * 60 * 60));
        assert_eq!(retry_at(Confirmation::Refunded, 0, 0), None);
    }

    /// An audit reviewing against `index`, with clients that are never called.
    fn audit(providers: &std::path::Path, index: &std::path::Path) -> Audit {
        Audit {
            explorer: Explorer::at("http://127.0.0.1:9".into()),
            status: Status::new("http://127.0.0.1:9".into()).unwrap(),
            providers: providers.into(),
            index: index.into(),
            since: 0,
            polls: 0,
            reconciled: false,
        }
    }

    /// A found payout retires after a late find and stays counted as checked, so a
    /// rewind after retirement no longer reports it; within the window a rewind does.
    /// Retiring needs a find at the deadline, not just time.
    #[test]
    fn found_payouts_retire_and_stay_checked() {
        let dir = tempfile::tempdir().unwrap();
        let (provider_path, index_path) = (
            dir.path().join("providers.sqlite"),
            dir.path().join(receiver_store::STORE_FILE),
        );
        let (mut index, block) = paid_index(&index_path);
        index.append(&block).unwrap();
        let now = 1_000_000;
        let mut provider = PayoutStore::open(&provider_path).unwrap();
        let payouts = [
            (receiver(1), [7; 32], now - 7200),
            (receiver(1), [8; 32], now - 7200),
        ];
        provider
            .record(Feed::Payouts.name(), &payouts, now, now)
            .unwrap();
        let audit = audit(&provider_path, &index_path);
        let review = |provider: &mut PayoutStore, at| {
            let (changes, _, _) = audit.review(provider, at).unwrap();
            provider.settle_payouts(&changes, at).unwrap();
            changes.into_iter().map(|c| c.2).collect::<Vec<_>>()
        };
        assert_eq!(review(&mut provider, now), [PayoutChange::Found]);
        assert_eq!(review(&mut provider, now + 60), []);
        // A rewind inside the window reopens it.
        index.rewind(99, [2; 32]).unwrap();
        assert_eq!(review(&mut provider, now + 120), [PayoutChange::Reopen]);
        assert_eq!(report(&provider, &index, now)["payouts_missing"], 2);
        index.append(&block).unwrap();
        let found = now + 180;
        assert_eq!(review(&mut provider, found), [PayoutChange::Found]);
        assert_eq!(
            review(&mut provider, found + RETIRE_AFTER_SECS),
            [PayoutChange::Retire]
        );
        let checked = report(&provider, &index, now);
        assert_eq!(checked["payouts_checked"], 2);
        assert_eq!(checked["payouts_missing"], 1);
        // After retirement a rewind is not noticed; the missing payout never retires.
        index.rewind(99, [2; 32]).unwrap();
        assert_eq!(report(&provider, &index, now)["payouts_missing"], 1);
        assert_eq!(review(&mut provider, found + 10 * RETIRE_AFTER_SECS), []);
        assert_eq!(report(&provider, &index, now)["payouts_checked"], 2);
    }

    /// Reviewing refunds: a due refunded swap with no paying candidate is marked
    /// missing once; an unmatched excess refund is queued for confirmation unless its
    /// retry is pending; and an unmatched swap older than its feed's overlap is queued
    /// for a refresh at most hourly.
    #[test]
    fn review_marks_missing_refunds_and_queues_follow_ups() {
        let dir = tempfile::tempdir().unwrap();
        let (provider_path, index_path) = (
            dir.path().join("providers.sqlite"),
            dir.path().join(receiver_store::STORE_FILE),
        );
        let (mut index, block) = paid_index(&index_path);
        index.append(&block).unwrap();
        drop(index);
        let now = 2_000_000;
        let seen = now - 2 * 60 * 60;
        let mut provider = PayoutStore::open(&provider_path).unwrap();
        // The overlap starts a week before each cursor; swap 2 was created before it.
        let cursor = now;
        provider
            .record_refunds(
                Feed::Refunds.name(),
                &[
                    refund(1, receiver(1), &[[7; 32]], cursor),
                    refund(
                        2,
                        receiver(2),
                        &[[8; 32]],
                        cursor - REFUND_LOOKBACK_SECS - 1,
                    ),
                ],
                cursor,
                seen,
                seen,
            )
            .unwrap();
        provider
            .record_refunds(
                Feed::ExcessRefunds.name(),
                &[refund(3, receiver(2), &[[8; 32], [9; 32]], cursor)],
                cursor,
                seen,
                seen,
            )
            .unwrap();
        let audit = audit(&provider_path, &index_path);
        let (_, changes, followups) = audit.review(&provider, now).unwrap();
        assert_eq!(
            changes,
            [
                ([1; 32], RefundChange::Found),
                ([2; 32], RefundChange::Missing)
            ]
        );
        let swaps = |rs: &[OpenRefund]| rs.iter().map(|r| r.swap[0]).collect::<Vec<_>>();
        assert_eq!(swaps(&followups.confirm), [3]);
        assert_eq!(swaps(&followups.refresh), [2]);
        provider.settle_refunds(&changes, now).unwrap();
        provider
            .settle_refunds(
                &[
                    (
                        [3; 32],
                        RefundChange::Confirmed(Confirmation::Zero, Some(now + 60)),
                    ),
                    ([2; 32], RefundChange::Refreshed),
                ],
                now,
            )
            .unwrap();
        let (_, changes, followups) = audit.review(&provider, now + 30).unwrap();
        assert!(changes.is_empty());
        assert!(followups.confirm.is_empty() && followups.refresh.is_empty());
        assert_eq!(
            provider.checked_through(Feed::Refunds.name()).unwrap(),
            Some(seen)
        );
    }

    /// The durable floor is set once and bounds both reads and stored rows.
    #[test]
    fn the_refund_floor_is_durable_and_bounds_stored_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("providers.sqlite");
        let mut provider = PayoutStore::open(&path).unwrap();
        let feed = Feed::Refunds.name();
        assert_eq!(provider.floor(feed, 100).unwrap(), 100);
        assert_eq!(provider.floor(feed, 50).unwrap(), 100);
        provider
            .record_refunds(
                feed,
                &[
                    refund(1, receiver(1), &[[7; 32]], 150),
                    refund(2, receiver(1), &[[7; 32]], 250),
                ],
                250,
                300,
                300,
            )
            .unwrap();
        drop(provider);
        let provider = PayoutStore::open(&path).unwrap();
        assert_eq!(provider.floor_of(feed).unwrap(), Some(100));
        assert_eq!(provider.open_refunds(0).unwrap().len(), 2);
        // Raising it, as a release would for excluded history, drops stored rows below it.
        rusqlite::Connection::open(&path)
            .unwrap()
            .execute(
                "UPDATE audit_state SET value=200 WHERE key='floor:near-refund-audit'",
                [],
            )
            .unwrap();
        let open = provider.open_refunds(0).unwrap();
        assert_eq!(open.iter().map(|r| r.swap[0]).collect::<Vec<_>>(), [2]);
    }

    /// A refund feed reads from its floor on its first read, then from
    /// `max(floor, cursor - lookback)`, and records only refunds it selects.
    #[tokio::test]
    async fn refund_feeds_read_from_their_floor() {
        use axum::{extract::Query, routing::get, Json, Router};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let created = unix_now() - 60;
        let app = Router::new().route("/", get(move |Query(query): Query<std::collections::HashMap<String, String>>| async move {
            assert_eq!(query["fromChainId"], "zec");
            assert_eq!(query["statuses"], REFUNDED);
            assert_eq!(query["numberOfTransactions"], "100");
            Json(vec![
                serde_json::json!({"createdAtTimestamp": created, "depositAddress": "d", "status": REFUNDED,
                    "refundType": ORIGIN_CHAIN, "refundTo": UA,
                    "originChainTxHashes": ["7f79a9262c301e3da38907fdbfca663c1fcb04b6a890ea737c281fc2ff707e7f"]}),
                serde_json::json!({"createdAtTimestamp": created, "depositAddress": "e", "status": REFUNDED,
                    "refundType": "INTENTS", "refundTo": UA}),
            ])
        }));
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let dir = tempfile::tempdir().unwrap();
        let mut store = PayoutStore::open(dir.path().join("providers.sqlite")).unwrap();
        let mut explorer = Explorer::at(origin);
        assert_eq!(
            explorer
                .sync_refunds(&mut store, Feed::Refunds)
                .await
                .unwrap(),
            1
        );
        let floor = store.floor_of(Feed::Refunds.name()).unwrap().unwrap();
        assert!(floor <= unix_now() - REFUND_LOOKBACK_SECS);
        assert_eq!(store.cursor(Feed::Refunds.name()).unwrap(), Some(created));
        assert_eq!(
            explorer
                .sync_refunds(&mut store, Feed::Refunds)
                .await
                .unwrap(),
            1
        );
        assert_eq!(store.open_refunds(0).unwrap().len(), 1);
        server.abort();
    }

    /// 1Click's refunded amount parses as an integer string, with the origin hashes it
    /// lists; the memo is passed; a missing amount is `None`; a 404 fails only the swap
    /// and a 5xx or 429 fails the service.
    #[tokio::test]
    async fn status_reads_refunded_amounts() {
        use axum::{extract::Query, http::StatusCode, routing::get, Json, Router};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let app = Router::new().route("/", get(|Query(query): Query<std::collections::HashMap<String, String>>| async move {
            let body = |amount: serde_json::Value| Json(serde_json::json!({"status": "SUCCESS",
                "swapDetails": {"refundedAmount": amount, "originChainTxHashes": [
                    {"hash": "7f79a9262c301e3da38907fdbfca663c1fcb04b6a890ea737c281fc2ff707e7f", "explorerUrl": ""}]}}));
            match query["depositAddress"].as_str() {
                "refunded" => {
                    assert_eq!(query["depositMemo"], "m");
                    Ok(body("1183460".into()))
                }
                "zero" => Ok(body("0".into())),
                "absent" => Ok(body(serde_json::Value::Null)),
                "unknown" => Err(StatusCode::NOT_FOUND),
                "limited" => Err(StatusCode::TOO_MANY_REQUESTS),
                _ => Err(StatusCode::BAD_GATEWAY),
            }
        }));
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let status = Status::new(origin).unwrap();
        let ask = |address: &str, memo: Option<&str>| {
            let mut r = OpenRefund {
                swap: [0; 32],
                feed: Feed::ExcessRefunds.name().into(),
                receiver: receiver(1),
                deposit_address: address.into(),
                deposit_memo: memo.map(Into::into),
                created_at: 0,
                seen_at: 0,
                candidates_changed_at: None,
                found_at: None,
                checked_at: None,
                refreshed_at: None,
                refund_confirmed_at: None,
                confirm_after: None,
                confirm_attempts: 0,
                confirm_last: None,
                candidates: Vec::new(),
            };
            r.created_at = 0;
            r
        };
        let (amount, hashes) = status.refunded(&ask("refunded", Some("m"))).await.unwrap();
        assert_eq!((amount, hashes.len()), (Some(1_183_460), 1));
        assert_eq!(
            status.refunded(&ask("zero", None)).await.unwrap().0,
            Some(0)
        );
        assert_eq!(status.refunded(&ask("absent", None)).await.unwrap().0, None);
        assert!(matches!(
            status.refunded(&ask("unknown", None)).await,
            Err(StatusError::Swap(_))
        ));
        assert!(matches!(
            status.refunded(&ask("limited", None)).await,
            Err(StatusError::Service(_))
        ));
        assert!(matches!(
            status.refunded(&ask("down", None)).await,
            Err(StatusError::Service(_))
        ));
        server.abort();
    }

    /// Pre-merge replay against production data (ignored; run by hand). Set
    /// `REFUND_REPLAY_DIR` to a directory holding a read-only copy of the directory
    /// store (`directory-v3.sqlite`) and saved explorer pages (`refunds.json` for
    /// REFUNDED, `excess.json` for SUCCESS swaps out of ZEC). Each eligible swap is
    /// recorded as first seen at its creation, unmatched excess refunds are confirmed
    /// with the real 1Click, and the report at the current time must check refunds and
    /// find none missing.
    #[tokio::test]
    #[ignore]
    async fn refund_replay() {
        let dir = std::path::PathBuf::from(std::env::var("REFUND_REPLAY_DIR").unwrap());
        let scratch = tempfile::tempdir().unwrap();
        let providers = scratch.path().join("providers.sqlite");
        let index = dir.join(receiver_store::STORE_FILE);
        let mut store = PayoutStore::open(&providers).unwrap();
        let now = unix_now();
        let mut eligible = 0;
        for (feed, file) in [
            (Feed::Refunds, "refunds.json"),
            (Feed::ExcessRefunds, "excess.json"),
        ] {
            let swaps: Vec<Swap> =
                serde_json::from_slice(&std::fs::read(dir.join(file)).unwrap()).unwrap();
            let (mut count, mut newest) = (0, 0);
            for swap in &swaps {
                let created = swap.created_at_timestamp;
                if created < now - COHORT_SECS {
                    continue;
                }
                if let Some(refund) = reported_refund(feed, swap, created) {
                    store
                        .record_refunds(feed.name(), &[refund], created, created, created)
                        .unwrap();
                    count += 1;
                    newest = newest.max(created);
                }
            }
            println!(
                "{}: {} swaps read, {count} eligible",
                feed.name(),
                swaps.len()
            );
            eligible += count;
        }
        let mut audit = Audit {
            status: Status::new(STATUS_ENDPOINT.to_owned()).unwrap(),
            ..audit(&providers, &index)
        };
        for _ in 0..100 {
            let (payouts, refunds, mut followups) = audit.review(&store, now).unwrap();
            store.settle_payouts(&payouts, now).unwrap();
            store.settle_refunds(&refunds, now).unwrap();
            followups.refresh.clear();
            if followups.confirm.is_empty() {
                break;
            }
            audit.follow_up(&mut store, followups).await.unwrap();
        }
        let reader = PaymentIndex::open(&index).unwrap();
        let report = capture(&store, now)
            .unwrap()
            .report_with(|receiver, txid| reader.paid_in(receiver, txid))
            .unwrap();
        println!("{}", serde_json::to_string_pretty(&report).unwrap());
        // Each unmatched due swap, with whether any receiver is paid in each candidate.
        let db = rusqlite::Connection::open_with_flags(
            &index,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .unwrap();
        for r in store.open_refunds(now - COHORT_SECS).unwrap() {
            let found = matched(&r, |receiver, txid| reader.paid_in(receiver, txid)).unwrap();
            if found || r.candidates.is_empty() {
                continue;
            }
            let candidates: Vec<_> = r
                .candidates
                .iter()
                .map(|txid| {
                    let paid: i64 = db
                        .query_row(
                            "SELECT COUNT(*) FROM payments WHERE txid=?1",
                            [txid.as_slice()],
                            |row| row.get(0),
                        )
                        .unwrap();
                    format!("{}:{paid}", zakura_server_chain::hash::display(txid))
                })
                .collect();
            println!(
                "MISSING {} {} created={} confirmed={:?} last={:?} receiver={} {:?}",
                r.feed,
                r.deposit_address,
                r.created_at,
                r.refund_confirmed_at,
                r.confirm_last,
                hex::encode(&r.receiver[..8]),
                candidates
            );
        }
        let count = |field: &str| report[field].as_u64().unwrap();
        let uncheckable = store
            .open_refunds(now - COHORT_SECS)
            .unwrap()
            .iter()
            .filter(|r| r.candidates.is_empty())
            .count() as u64;
        assert!(count("refunds_checked") > 0);
        assert_eq!(
            count("refunds_checked") + count("refunds_pending") + uncheckable,
            eligible
        );
        assert_eq!(
            count("refunds_missing"),
            0,
            "inspect each missing swap by hand"
        );
    }
}
