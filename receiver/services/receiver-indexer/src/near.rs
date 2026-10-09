//! NEAR Intents explorer feed for the receiver directory's recent and seen filters
//! (`receiver_directory::filter`). It reads every swap into or out of ZEC, newest first,
//! whichever app created it, and records the Orchard receiver of each payout
//! (`recipient`) and refund (`refundTo`) address with the swap's creation time. Unfunded
//! quotes count: they hold an address until their deadline. The explorer allows one
//! request every five seconds per partner key, which this module never logs.
use receiver_directory::{
    filter::{RECENT, SEEN},
    snapshot::ProviderSet,
    store::{ProviderStore, Store},
    Receiver,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::time::Duration;
use zcash_address::{
    unified::{self, Container},
    ConversionError, TryFromAddress, ZcashAddress,
};
use zcash_protocol::consensus::NetworkType;

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
/// How far back a read starts before the cursor, for swaps the explorer lists late. A
/// payout read also rescans successful swaps back [`RECENT_SECS`] before the cursor, so
/// it sees a payout complete up to a day after its swap was created.
const OVERLAP_SECS: i64 = 3600;
/// Bound on one read, about a month of swaps, so an explorer that ignores paging cannot
/// loop forever. It bounds a first read too: one from a `since` too far back fails
/// rather than record a feed that skipped unread history.
const MAX_PAGES: usize = 500;
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
fn uncheckable_receiver() -> Receiver {
    let mut bytes = [0; 43];
    bytes[11] = 1;
    Receiver::from_bytes(bytes).expect("a valid address")
}

/// The NEAR filter sets: `near-intents/recent` once both feeds have completed a read,
/// and `near-intents/seen` once the payout feed has. A feed that never completed a read
/// contributes no set, so wallets do not mistake its absence for an empty set. Each set
/// declares when its feeds started and when their last complete read began, the point
/// from which the recent window reaches back. They are read from one state of `store`.
pub fn provider_sets(store: &ProviderStore) -> Result<Vec<ProviderSet>> {
    store.view(sets_in)
}

/// See [`provider_sets`].
fn sets_in(store: &ProviderStore) -> Result<Vec<ProviderSet>> {
    // A feed's start and the beginning of its last complete read.
    let span = |feed: Feed| -> Result<Option<(i64, i64)>> {
        let span = store.started(feed.name())?.zip(store.read(feed.name())?);
        Ok(span.filter(|(since, until)| since <= until))
    };
    let payouts = span(Feed::Payouts)?;
    let both = payouts
        .zip(span(Feed::Refunds)?)
        .map(|(p, r)| (p.0.max(r.0), p.1.min(r.1)))
        .filter(|(since, until)| since <= until);
    let (recent, seen) = store.sets(both.map_or(i64::MAX, |(_, until)| until - RECENT_SECS))?;
    let mut sets = Vec::new();
    if let Some((since_unix, until_unix)) = both {
        sets.push(ProviderSet {
            label: format!("{PROVIDER}/{RECENT}"),
            window_secs: Some(RECENT_SECS as u64),
            since_unix,
            until_unix,
            receivers: recent,
        });
    }
    if let Some((since_unix, until_unix)) = payouts {
        sets.push(ProviderSet {
            label: format!("{PROVIDER}/{SEEN}"),
            window_secs: None,
            since_unix,
            until_unix,
            receivers: seen,
        });
    }
    Ok(sets)
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
        let mut receivers: Vec<_> = set.receivers.iter().map(Receiver::as_bytes).collect();
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
    /// When each feed's last complete read began.
    feeds: serde_json::Map<String, serde_json::Value>,
    /// Checkable payouts first seen complete more than [`COMPLETION_GRACE_SECS`]
    /// before the terminal block's time.
    payouts: Vec<(Receiver, receiver_directory::Hash)>,
    /// Uncheckable payouts first seen in that span but within [`RECENT_SECS`].
    uncheckable: usize,
}

/// Reads [`provider_sets`] and the inputs of a report on a publication whose terminal
/// block has time `anchor_time`, in Unix seconds, in one read transaction of `store`.
pub fn capture(store: &ProviderStore, anchor_time: i64) -> Result<Capture> {
    store.view(|store| {
        let mut feeds = serde_json::Map::new();
        for feed in [Feed::Payouts, Feed::Refunds] {
            feeds.insert(feed.name().into(), store.read(feed.name())?.into());
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
            payouts,
            uncheckable: count(anchor_time - COMPLETION_GRACE_SECS)?
                - count(anchor_time - RECENT_SECS)?,
        })
    })
}

impl Capture {
    /// The feed's health for monitoring: when each feed's last complete read began, and
    /// how many payouts first seen complete more than an hour before the terminal
    /// block's time, however long ago, have no payment to their receiver in the
    /// transaction NEAR reported in `index` (`payouts_missing`), out of all of them
    /// (`payouts_checked`). A missing payout means the indexer missed it, or NEAR paid
    /// it without the zero OVK, which a seed restore cannot find. Payouts to an Orchard
    /// receiver that NEAR reported complete without a parsable transaction cannot be
    /// looked up; `payouts_uncheckable` counts those first seen in the same span but
    /// within the last day, so it clears on its own. Payouts to other recipients are not
    /// checked.
    pub fn report(&self, index: &Store) -> Result<serde_json::Value> {
        let mut missing = 0;
        for (receiver, txid) in &self.payouts {
            if !index.paid_in(receiver, txid)? {
                missing += 1;
            }
        }
        Ok(serde_json::json!({
            "feeds": self.feeds,
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

    /// Reads `feed` back to an hour before its cursor (`OVERLAP_SECS`), or to `since` on
    /// the first read, and records each swap's Orchard receiver in `store`, with when the read
    /// began, and each completed payout to one with its reported transactions, dated
    /// when both scans finished so a long read cannot use up its grace. A payout
    /// read that found a cursor also rescans successful swaps created up to
    /// [`RECENT_SECS`] before it, for their completions only. Both scans are recorded in
    /// one transaction once both finish (see [`ProviderStore::record`]), so a failed or
    /// over-long scan records nothing. Only a read that began without a cursor records
    /// `since` as where the feed started, so a restart with another `since` cannot move
    /// the start past history it never read. Times are capped at the read's start, so a
    /// record dated in the future cannot hide later swaps. Returns how many receivers
    /// it recorded.
    pub async fn sync(
        &mut self,
        store: &mut ProviderStore,
        feed: Feed,
        since: i64,
    ) -> Result<usize> {
        let read_at = unix_now()?;
        let cursor = store.cursor(feed.name())?;
        // Decided before fetching: a read that found a cursor reads back only to it.
        let initial_since = cursor.is_none().then_some(since);
        let floor = cursor.map_or(since, |c| c - OVERLAP_SECS);
        let mut newest = cursor.unwrap_or(since).min(read_at);
        let mut found = Vec::new();
        let mut completed = Vec::new();
        self.scan(
            feed,
            STATUSES,
            floor,
            read_at,
            cursor.is_none(),
            |swap, created| {
                newest = newest.max(created);
                let address = match feed {
                    Feed::Payouts => &swap.recipient,
                    Feed::Refunds => &swap.refund_to,
                };
                if let Some(receiver) = address.as_deref().and_then(orchard_receiver) {
                    found.push((receiver, feed == Feed::Payouts, created));
                    if feed == Feed::Payouts {
                        completions(swap, receiver, &mut completed);
                    }
                }
            },
        )
        .await?;
        if let (Feed::Payouts, Some(cursor)) = (feed, cursor) {
            self.scan(
                feed,
                SUCCESS,
                cursor - RECENT_SECS,
                read_at,
                false,
                |swap, _| {
                    if let Some(receiver) = swap.recipient.as_deref().and_then(orchard_receiver) {
                        completions(swap, receiver, &mut completed);
                    }
                },
            )
            .await?;
        }
        let seen_at = unix_now()?;
        let completed: Vec<_> = completed
            .into_iter()
            .map(|(r, t)| (r, t, seen_at))
            .collect();
        // Only a completed initial read says where the feed started.
        store.record(
            feed.name(),
            initial_since,
            &found,
            &completed,
            newest,
            read_at,
        )?;
        Ok(found.len())
    }

    /// Pages `feed`'s swaps in `statuses`, newest first, passing `each` every swap with
    /// its creation time, capped at `read_at`, from `floor` on. Past [`MAX_PAGES`] it
    /// fails, suggesting a later `--near-since` when the read is the feed's `first`.
    async fn scan(
        &mut self,
        feed: Feed,
        statuses: &str,
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
            let swaps = self.page(feed, statuses, after.as_ref()).await?;
            for swap in &swaps {
                let created = swap.created_at_timestamp.min(read_at);
                if created >= floor {
                    each(swap, created);
                }
            }
            match swaps.last() {
                Some(last) if swaps.len() == PAGE && last.created_at_timestamp >= floor => {
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

    /// One page of `feed`'s swaps in `statuses`, newest first, older than `after` when
    /// given.
    async fn page(
        &mut self,
        feed: Feed,
        statuses: &str,
        after: Option<&(String, Option<String>)>,
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

/// Seconds since the Unix epoch.
fn unix_now() -> Result<i64> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs() as i64)
}

/// The Orchard receiver of a mainnet unified address, the only kind a per-swap key has.
fn orchard_receiver(address: &str) -> Option<Receiver> {
    struct Orchard([u8; 43]);
    impl TryFromAddress for Orchard {
        type Error = ();
        fn try_from_unified(
            _: NetworkType,
            address: unified::Address,
        ) -> std::result::Result<Self, ConversionError<()>> {
            address
                .items()
                .into_iter()
                .find_map(|item| match item {
                    unified::Receiver::Orchard(bytes) => Some(Self(bytes)),
                    _ => None,
                })
                .ok_or(ConversionError::User(()))
        }
    }
    let Orchard(bytes) = ZcashAddress::try_from_encoded(address)
        .ok()?
        .convert_if_network(NetworkType::Main)
        .ok()?;
    Receiver::from_bytes(bytes).ok()
}

/// Adds the payouts of `swap` to `receiver` if it succeeded: one per parsable reported
/// transaction, or else one uncheckable record (see [`UNCHECKABLE_ZEROS`]).
fn completions(
    swap: &Swap,
    receiver: Receiver,
    out: &mut Vec<(Receiver, receiver_directory::Hash)>,
) {
    if swap.status.as_deref() != Some(SUCCESS) {
        return;
    }
    let before = out.len();
    out.extend(
        swap.destination_chain_tx_hashes
            .iter()
            .flatten()
            .filter_map(|hash| hash.parse::<zakura_chain::transaction::Hash>().ok())
            .map(|txid| (receiver, txid.0)),
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

    /// A mainnet payout transaction from 2026-10-07, displayed hex.
    const PAYOUT_TXID: &str = "7f79a9262c301e3da38907fdbfca663c1fcb04b6a890ea737c281fc2ff707e7f";

    #[test]
    fn only_mainnet_unified_orchard_receivers_count() {
        // A Vizor swap address from 2026-10-07.
        let swap = "u14nnj43rj7dpf7qh6gu24fuyu8vld9fgatxd32xre27yqgu6p0yq0sf0t3uxnwts4968hf7d8nvyh4wfzqtmcdt6xzk7el6pn0ufx6pdg";
        assert_eq!(
            &orchard_receiver(swap).unwrap().as_bytes()[..4],
            &[0x15, 0x5f, 0x51, 0xfe]
        );
        assert!(orchard_receiver("t1KfwsnwJeNRVjQGBDZhwKskpQbih2qx5Ua").is_none());
        assert!(orchard_receiver("check-alice.near").is_none());
    }

    #[test]
    fn provider_sets_reach_back_from_the_last_complete_read() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = ProviderStore::open(dir.path().join("provider.sqlite")).unwrap();
        let (payouts, refunds) = (Feed::Payouts.name(), Feed::Refunds.name());
        // A feed that never finished a read publishes nothing.
        assert!(provider_sets(&store).unwrap().is_empty());
        let until = 2_000 + RECENT_SECS;
        let old = (receiver(1), true, 1_500);
        let recent = (receiver(2), true, until - 60);
        store
            .record(payouts, Some(1_000), &[old, recent], &[], until, until + 30)
            .unwrap();
        let sets = provider_sets(&store).unwrap();
        assert_eq!(sets.len(), 1);
        assert_eq!(sets[0].label, "near-intents/seen");
        assert_eq!(
            (sets[0].since_unix, sets[0].until_unix),
            (1_000, until + 30)
        );
        // Recent needs both feeds, and reaches back from the older read.
        let refund = (receiver(3), false, until - 120);
        store
            .record(refunds, Some(2_000), &[refund], &[], until, until)
            .unwrap();
        let sets = provider_sets(&store).unwrap();
        assert_eq!(sets[0].label, "near-intents/recent");
        assert_eq!((sets[0].since_unix, sets[0].until_unix), (2_000, until));
        assert_eq!(sets[0].receivers.len(), 2);
        assert_eq!(sets[1].receivers.len(), 2);
    }

    /// A failed first read, including one over the page bound, records no start, so a
    /// restart that begins later cannot leave the feed claiming coverage it never read.
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
        let mut store = ProviderStore::open(dir.path().join("provider.sqlite")).unwrap();
        let feed = Feed::Payouts;
        for route in ["fail", "large"] {
            let mut failing = Explorer::at(format!("{origin}/{route}"));
            assert!(failing.sync(&mut store, feed, 1_000).await.is_err());
            assert_eq!(store.started(feed.name()).unwrap(), None);
        }
        let mut working = Explorer::at(format!("{origin}/ok"));
        working.sync(&mut store, feed, 2_000).await.unwrap();
        assert_eq!(store.started(feed.name()).unwrap(), Some(2_000));
    }

    /// A restart with an earlier or later `since` follows the saved cursor, so the
    /// published seen set keeps declaring the first read's start rather than claim
    /// history no read fetched.
    #[tokio::test]
    async fn a_restart_with_another_since_keeps_the_published_start() {
        use axum::{routing::get, Router};
        let app = Router::new().route("/", get(|| async { "[]" }));
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}/", socket.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("provider.sqlite");
        // The seen set's declared start.
        let since_unix = |store: &ProviderStore| {
            let sets = provider_sets(store).unwrap();
            assert_eq!(sets[0].label, "near-intents/seen");
            sets[0].since_unix
        };
        let mut store = ProviderStore::open(&path).unwrap();
        Explorer::at(origin.clone())
            .sync(&mut store, Feed::Payouts, 2_000)
            .await
            .unwrap();
        assert_eq!(since_unix(&store), 2_000);
        for since in [1_000, 3_000] {
            drop(store);
            store = ProviderStore::open(&path).unwrap();
            Explorer::at(origin.clone())
                .sync(&mut store, Feed::Payouts, since)
                .await
                .unwrap();
            assert_eq!(since_unix(&store), 2_000);
        }
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
        // and a completed payout, then an empty page.
        #[derive(Clone)]
        struct Pages {
            served: Arc<AtomicUsize>,
            full: usize,
            len: usize,
        }
        async fn page(State(feed): State<Pages>) -> String {
            let page = feed.served.fetch_add(1, Ordering::SeqCst);
            let len = if page < feed.full { feed.len } else { 0 };
            let records: Vec<_> = (0..len)
                .map(|i| {
                    serde_json::json!({"recipient": SWAP, "createdAtTimestamp": 9_000_000 - page,
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
            });
            let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let origin = format!("http://{}/", socket.local_addr().unwrap());
            tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
            origin
        };
        let dir = tempfile::tempdir().unwrap();
        let mut store = ProviderStore::open(dir.path().join("provider.sqlite")).unwrap();
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
            assert!(explorer
                .sync(&mut store, Feed::Payouts, 1_000)
                .await
                .is_err());
            assert!(nothing_recorded(&store));
        }
        // The last allowed page ends the read.
        let mut explorer = Explorer::at(serve(2, PAGE).await);
        explorer.max_pages = 3;
        assert_eq!(
            explorer
                .sync(&mut store, Feed::Payouts, 1_000)
                .await
                .unwrap(),
            2 * PAGE
        );
        assert_eq!(store.started(feed).unwrap(), Some(1_000));
        assert_eq!(store.payouts(i64::MAX).unwrap().len(), 1);
    }

    /// A payout read that follows a cursor also rescans successful swaps back a day
    /// before that cursor, however long ago it was, for their completions only: swaps
    /// older than the overlap that have since succeeded are recorded, without their
    /// receivers, and one older than the day is not. If the rescan fails, neither scan
    /// records anything. The first read scans all statuses alone and sets the start.
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
        use zcash_address::unified::Encoding;
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
            let address = unified::Address::try_from_items(vec![unified::Receiver::Orchard(
                *receiver(seed).as_bytes(),
            )])
            .unwrap()
            .encode(&NetworkType::Main);
            json!({"recipient": address, "createdAtTimestamp": created,
                "depositAddress": seed.to_string(), "status": status,
                "destinationChainTxHashes": [format!("{seed:02x}").repeat(32)]})
        };
        let serve = |all: Value, successes: Option<Value>| {
            *served.pages.lock().unwrap() = (all, successes);
        };
        let asked = || served.asked.lock().unwrap().clone();
        let dir = tempfile::tempdir().unwrap();
        let mut store = ProviderStore::open(dir.path().join("provider.sqlite")).unwrap();
        let feed = Feed::Payouts;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        // Three days of downtime follow the first read.
        let since = now - 3 * RECENT_SECS;
        let cursor = since + 100;
        serve(json!([swap(1, cursor, "PENDING_DEPOSIT")]), Some(json!([])));
        explorer.sync(&mut store, feed, since).await.unwrap();
        assert_eq!(asked(), [STATUSES]);
        assert_eq!(store.cursor(feed.name()).unwrap(), Some(cursor));
        let read = store.read(feed.name()).unwrap();

        serve(json!([swap(2, now - 600, SUCCESS)]), None);
        assert!(explorer.sync(&mut store, feed, since).await.is_err());
        assert_eq!(asked()[1..], [STATUSES, SUCCESS]);
        assert_eq!(store.cursor(feed.name()).unwrap(), Some(cursor));
        assert_eq!(store.read(feed.name()).unwrap(), read);
        assert!(store.payouts(i64::MAX).unwrap().is_empty());
        assert_eq!(store.sets(0).unwrap().1, [receiver(1)]);

        let successes = json!([
            swap(1, cursor, SUCCESS),
            swap(3, cursor - 2 * OVERLAP_SECS, SUCCESS),
            swap(4, cursor - RECENT_SECS - 1, SUCCESS),
        ]);
        serve(json!([swap(2, now - 600, SUCCESS)]), Some(successes));
        assert_eq!(explorer.sync(&mut store, feed, since).await.unwrap(), 1);
        assert_eq!(asked()[3..], [STATUSES, SUCCESS]);
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
        let page = serde_json::json!([
            {"recipient": swap, "refundTo": null, "createdAtTimestamp": 9_999_999_999i64,
             "depositAddress": "a", "depositMemo": null, "status": "SUCCESS",
             "destinationChainTxHashes": [PAYOUT_TXID, "not hex"]},
            {"recipient": null, "createdAtTimestamp": 2_000, "depositAddress": "b"},
            {"recipient": swap, "createdAtTimestamp": 3_000, "depositAddress": "c",
             "status": "SUCCESS", "destinationChainTxHashes": ["not hex"]},
            {"recipient": swap, "createdAtTimestamp": 3_000, "depositAddress": "d",
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
        let mut store = ProviderStore::open(dir.path().join("provider.sqlite")).unwrap();
        let read = Explorer::at(origin)
            .sync(&mut store, Feed::Payouts, 1_000)
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
        use zcash_address::{unified::Encoding, ToAddress};
        let swap = "u14nnj43rj7dpf7qh6gu24fuyu8vld9fgatxd32xre27yqgu6p0yq0sf0t3uxnwts4968hf7d8nvyh4wfzqtmcdt6xzk7el6pn0ufx6pdg";
        let orchard = orchard_receiver(swap).unwrap();
        let unified = |items, network| {
            unified::Address::try_from_items(items)
                .unwrap()
                .encode(&network)
        };
        let excluded = [
            None,
            Some("not an address".to_owned()),
            Some(unified(
                vec![unified::Receiver::Orchard(*orchard.as_bytes())],
                NetworkType::Test,
            )),
            Some("t1KfwsnwJeNRVjQGBDZhwKskpQbih2qx5Ua".to_owned()),
            Some(ZcashAddress::from_transparent_p2sh(NetworkType::Main, [1; 20]).encode()),
            Some(ZcashAddress::from_sapling(NetworkType::Main, [1; 43]).encode()),
            Some(unified(
                vec![
                    unified::Receiver::Sapling([1; 43]),
                    unified::Receiver::P2pkh([1; 20]),
                ],
                NetworkType::Main,
            )),
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
        let path = dir.path().join("provider.sqlite");
        let mut store = ProviderStore::open(&path).unwrap();
        explorer
            .sync(&mut store, Feed::Payouts, 1_000)
            .await
            .unwrap();
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
        explorer
            .sync(&mut store, Feed::Payouts, 1_000)
            .await
            .unwrap();
        assert_eq!(store.payouts(i64::MAX).unwrap(), first);
        assert_eq!(store.payouts(first_seen).unwrap(), first);
        let counted = |anchor_time| capture(&store, anchor_time).unwrap().uncheckable;
        assert_eq!(counted(first_seen + COMPLETION_GRACE_SECS), 2);
        assert_eq!(counted(first_seen + RECENT_SECS + 1), 0);
    }

    /// [`Capture::report`] for the current state of `provider` on a terminal block with
    /// time `now`.
    fn report(provider: &ProviderStore, index: &Store, now: i64) -> serde_json::Value {
        capture(provider, now).unwrap().report(index).unwrap()
    }

    /// A receiver from the spending key with every byte `seed`.
    fn receiver(seed: u8) -> Receiver {
        let sk = orchard::keys::SpendingKey::from_bytes([seed; 32]).unwrap();
        let fvk = orchard::keys::FullViewingKey::from(&sk);
        let address = fvk.address_at(0u32, orchard::keys::Scope::External);
        Receiver::from_bytes(address.to_raw_address_bytes()).unwrap()
    }

    /// An index from height 100 whose first block pays `receiver(1)` in transaction
    /// `[7; 32]`, and that block.
    fn paid_index(path: &std::path::Path) -> (Store, receiver_directory::store::IndexedBlock) {
        use receiver_directory::{
            store::{Config, IndexedBlock},
            Payment,
        };
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
            dir.path().join("provider.sqlite"),
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
        let mut provider = ProviderStore::open(dir.path().join("provider.sqlite")).unwrap();
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
        let path = dir.path().join("provider.sqlite");
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
        let mut store = ProviderStore::open(dir.path().join("provider.sqlite")).unwrap();
        let mut explorer = Explorer::new(key).unwrap();
        let since = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
            - 3600;
        let payouts = explorer
            .sync(&mut store, Feed::Payouts, since)
            .await
            .unwrap();
        let refunds = explorer
            .sync(&mut store, Feed::Refunds, since)
            .await
            .unwrap();
        let (recent, seen) = store.sets(since).unwrap();
        eprintln!(
            "payouts={payouts} refunds={refunds} recent={} seen={}",
            recent.len(),
            seen.len()
        );
        assert!(payouts > 0 && refunds > 0);
        assert!(store.cursor(Feed::Payouts.name()).unwrap().unwrap() >= since);
        // A second read starts from the cursor and finds nothing new to lose.
        explorer
            .sync(&mut store, Feed::Payouts, since)
            .await
            .unwrap();
        assert!(store.sets(since).unwrap().0.len() >= recent.len());
    }
}
