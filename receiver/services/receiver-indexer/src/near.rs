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
const PAGE: usize = 1000;
/// Bound on one page's body: a record is about 1 KB, so eight times a full page.
const MAX_PAGE_BYTES: usize = 8 * 1024 * 1024;
/// The explorer's per-partner rate limit, with margin.
const REQUEST_INTERVAL: Duration = Duration::from_millis(5_500);
/// How far back a refund read starts before the cursor, for swaps the explorer lists
/// late. A payout read reaches back [`RECENT_SECS`] instead, so it sees a payout
/// complete up to a day after its swap was created, the window [`Capture::report`]
/// checks. At about 5,500 swaps a day into ZEC (October 2026) that is six pages, 33
/// seconds at the explorer's rate limit, on each poll.
const OVERLAP_SECS: i64 = 3600;
/// Bound on one read, about a month of swaps, so an explorer that ignores paging cannot
/// loop forever. It bounds a first read too: one from a `since` too far back fails
/// rather than record a feed that skipped unread history.
const MAX_PAGES: usize = 500;
/// The provider name in the directory's filter set labels.
pub const PROVIDER: &str = "near-intents";
/// How far back the recent set reaches from the feeds' last complete read.
pub const RECENT_SECS: i64 = 24 * 60 * 60;
/// How long after a read first sees a payout complete its payment must be indexed.
const COMPLETION_GRACE_SECS: i64 = 60 * 60;

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
    /// Payouts first seen complete more than an hour before the capture's time that no
    /// check has matched yet.
    unmatched: Vec<(Receiver, receiver_directory::Hash)>,
}

/// Reads [`provider_sets`] and the inputs of a report at `now`, in Unix seconds, in
/// one read transaction of `store`.
pub fn capture(store: &ProviderStore, now: i64) -> Result<Capture> {
    store.view(|store| {
        let mut feeds = serde_json::Map::new();
        for feed in [Feed::Payouts, Feed::Refunds] {
            feeds.insert(feed.name().into(), store.read(feed.name())?.into());
        }
        Ok(Capture {
            sets: sets_in(store)?,
            feeds,
            unmatched: store.unmatched(now - COMPLETION_GRACE_SECS)?,
        })
    })
}

impl Capture {
    /// The feed's health for monitoring: when each feed's last complete read began, and
    /// how many payouts first seen complete more than an hour before the capture's time,
    /// however long ago, still have no payment to their receiver in the transaction NEAR
    /// reported in `index`. Also returns the payouts it matched, for
    /// [`ProviderStore::match_payouts`] once the report is published; they are not
    /// checked again until a [`rewind`], and one that stays missing stays in the count.
    /// A missing payout means the indexer missed it, or NEAR paid it without the zero
    /// OVK, which a seed restore cannot find.
    pub fn report(
        &self,
        index: &Store,
    ) -> Result<(serde_json::Value, Vec<(Receiver, receiver_directory::Hash)>)> {
        let mut matched = Vec::new();
        for payout in &self.unmatched {
            if index.paid_in(&payout.0, &payout.1)? {
                matched.push(*payout);
            }
        }
        let report = serde_json::json!({
            "feeds": self.feeds,
            "payouts_checked": self.unmatched.len(),
            "payouts_missing": self.unmatched.len() - matched.len(),
        });
        Ok((report, matched))
    }
}

/// Rewinds `index` to the saved block at `height` with `hash` (see [`Store::rewind`]),
/// first forgetting every payout match (see [`ProviderStore::forget_matches`]), since
/// the rewind can remove a matched payment. In that order a crash between the two
/// databases' writes only makes the next [`Capture::report`] check payouts again.
pub fn rewind(
    provider: &mut ProviderStore,
    index: &mut Store,
    height: u32,
    hash: receiver_directory::Hash,
) -> Result<()> {
    provider.forget_matches()?;
    index.rewind(height, hash)?;
    Ok(())
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

    /// How far before the cursor a read starts; see [`OVERLAP_SECS`].
    fn overlap(self) -> i64 {
        match self {
            Self::Payouts => RECENT_SECS,
            Self::Refunds => OVERLAP_SECS,
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
/// required, so a record missing an address or status is skipped rather than failing
/// the read.
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

    /// Reads `feed` back to its cursor, or to `since` on the first read, and records
    /// each swap's Orchard receiver in `store`, with when the read began, and each
    /// completed payout with its reported transactions, all in one transaction (see
    /// [`ProviderStore::record`]). Only a read that began without a cursor records
    /// `since` as where the feed started, so a restart with another `since` cannot
    /// move the start past history it never read. Times are capped at the read's start, so a record
    /// dated in the future cannot hide later swaps. A read past its page bound fails
    /// and records nothing. Returns how many receivers it recorded.
    pub async fn sync(
        &mut self,
        store: &mut ProviderStore,
        feed: Feed,
        since: i64,
    ) -> Result<usize> {
        let read_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs() as i64;
        let cursor = store.cursor(feed.name())?;
        // Decided before fetching: a read that found a cursor reads back only to it.
        let initial_since = cursor.is_none().then_some(since);
        let floor = cursor.map_or(since, |c| c - feed.overlap());
        let mut newest = cursor.unwrap_or(since).min(read_at);
        let mut found = Vec::new();
        let mut completed = Vec::new();
        let mut after: Option<(String, Option<String>)> = None;
        for page in 0.. {
            if page == self.max_pages {
                return Err(match cursor {
                    Some(_) => "NEAR explorer read exceeded its page bound".into(),
                    None => "NEAR explorer first read exceeded its page bound; \
                        start it later with --near-since"
                        .into(),
                });
            }
            let swaps = self.page(feed, after.as_ref()).await?;
            for swap in &swaps {
                let created = swap.created_at_timestamp.min(read_at);
                if created < floor {
                    continue;
                }
                newest = newest.max(created);
                let address = match feed {
                    Feed::Payouts => &swap.recipient,
                    Feed::Refunds => &swap.refund_to,
                };
                if let Some(receiver) = address.as_deref().and_then(orchard_receiver) {
                    found.push((receiver, feed == Feed::Payouts, created));
                    if feed == Feed::Payouts && swap.status.as_deref() == Some("SUCCESS") {
                        // A payout without a parsable transaction cannot be checked.
                        let txids = swap.destination_chain_tx_hashes.iter().flatten();
                        completed.extend(
                            txids
                                .filter_map(|hash| {
                                    hash.parse::<zakura_chain::transaction::Hash>().ok()
                                })
                                .map(|txid| (receiver, txid.0)),
                        );
                    }
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

    /// One page of `feed`, newest first, older than `after` when given.
    async fn page(
        &mut self,
        feed: Feed,
        after: Option<&(String, Option<String>)>,
    ) -> Result<Vec<Swap>> {
        tokio::time::sleep_until(self.next_request).await;
        let mut query = vec![
            (feed.chain_filter(), "zec".to_owned()),
            ("statuses", STATUSES.to_owned()),
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
        let mut response = response?;
        let status = response.status();
        if !status.is_success() {
            return Err(format!("NEAR explorer returned HTTP {status}").into());
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            if body.len() + chunk.len() > MAX_PAGE_BYTES {
                return Err("NEAR explorer page exceeds its size bound".into());
            }
            body.extend_from_slice(&chunk);
        }
        let swaps: Vec<Swap> = serde_json::from_slice(&body)?;
        // A longer page is not one this read asked for, so it cannot end the read.
        if swaps.len() > PAGE {
            return Err("NEAR explorer page exceeds the requested length".into());
        }
        Ok(swaps)
    }
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
                && store.unmatched(i64::MAX).unwrap().is_empty()
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
        assert_eq!(store.unmatched(i64::MAX).unwrap().len(), 1);
    }

    /// A record missing its address is skipped, a future date is capped at the read's
    /// start, and a completed payout is recorded.
    #[tokio::test]
    async fn a_read_skips_bad_records_caps_future_dates_and_notes_completions() {
        use axum::{routing::get, Router};
        let swap = "u14nnj43rj7dpf7qh6gu24fuyu8vld9fgatxd32xre27yqgu6p0yq0sf0t3uxnwts4968hf7d8nvyh4wfzqtmcdt6xzk7el6pn0ufx6pdg";
        let page = serde_json::json!([
            {"recipient": swap, "refundTo": null, "createdAtTimestamp": 9_999_999_999i64,
             "depositAddress": "a", "depositMemo": null, "status": "SUCCESS",
             "destinationChainTxHashes": [PAYOUT_TXID, "not hex"]},
            {"recipient": null, "createdAtTimestamp": 2_000, "depositAddress": "b"},
        ])
        .to_string();
        let app = Router::new().route(
            "/",
            get(move || {
                let page = page.clone();
                async move { page }
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
        assert_eq!(read, 1);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        assert!(store.cursor(Feed::Payouts.name()).unwrap().unwrap() <= now);
        let completed = store.unmatched(now + 60).unwrap();
        assert_eq!(completed.len(), 1);
        // The explorer's displayed hex is reversed into protocol byte order.
        assert_eq!(completed[0].1[0], 0x7f);
        assert_eq!(completed[0].1[31], 0x7f);
        assert_eq!(completed[0].1[1], 0x7e);
    }

    /// [`Capture::report`] for the current state of `provider` at `now`, recording the
    /// payouts it matched.
    fn report(provider: &mut ProviderStore, index: &Store, now: i64) -> Result<serde_json::Value> {
        let (report, matched) = capture(provider, now)?.report(index)?;
        provider.match_payouts(&matched)?;
        Ok(report)
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
            ephemeral_key: [0; 32],
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

    /// A payout matched before a rewind removed its payment is reported missing,
    /// after both databases reopen, until reindexing restores the payment.
    #[test]
    fn a_rewind_rechecks_matched_payouts() {
        let dir = tempfile::tempdir().unwrap();
        let (provider_path, index_path) = (
            dir.path().join("provider.sqlite"),
            dir.path().join("directory.sqlite"),
        );
        let mut provider = ProviderStore::open(&provider_path).unwrap();
        let (mut index, block) = paid_index(&index_path);
        index.append(&block).unwrap();
        let now = 1_000_000;
        provider
            .record(
                "near-payouts",
                Some(0),
                &[],
                &[(receiver(1), [7; 32])],
                now,
                now - 2 * 60 * 60,
            )
            .unwrap();
        assert_eq!(
            report(&mut provider, &index, now).unwrap()["payouts_missing"],
            0
        );
        rewind(&mut provider, &mut index, 99, [2; 32]).unwrap();
        drop((provider, index));
        let mut provider = ProviderStore::open(&provider_path).unwrap();
        let (mut index, block) = paid_index(&index_path);
        assert_eq!(
            report(&mut provider, &index, now).unwrap()["payouts_missing"],
            1
        );
        index.append(&block).unwrap();
        assert_eq!(
            report(&mut provider, &index, now).unwrap()["payouts_missing"],
            0
        );
    }

    /// A matched payout in a provider store at `provider_path` and its payment indexed at
    /// `index_path`, with `trigger` installed on the database at `fail` so that
    /// [`rewind`] fails there. Returns the result of rewinding below the payment, after
    /// both databases closed.
    fn failing_rewind(
        provider_path: &std::path::Path,
        index_path: &std::path::Path,
        fail: &std::path::Path,
        trigger: &str,
    ) -> Result<()> {
        let mut provider = ProviderStore::open(provider_path).unwrap();
        let (mut index, block) = paid_index(index_path);
        index.append(&block).unwrap();
        let now = 1_000_000;
        provider
            .record(
                "near-payouts",
                Some(0),
                &[],
                &[(receiver(1), [7; 32])],
                now,
                0,
            )
            .unwrap();
        report(&mut provider, &index, now).unwrap();
        assert!(provider.unmatched(i64::MAX).unwrap().is_empty());
        rusqlite::Connection::open(fail)
            .unwrap()
            .execute_batch(trigger)
            .unwrap();
        rewind(&mut provider, &mut index, 99, [2; 32])
    }

    /// A failure to forget matches leaves the index un-rewound, so no matched payout
    /// can outlive its payment.
    #[test]
    fn a_failed_forget_leaves_the_index_unrewound() {
        let dir = tempfile::tempdir().unwrap();
        let (provider_path, index_path) = (
            dir.path().join("provider.sqlite"),
            dir.path().join("directory.sqlite"),
        );
        let failed = failing_rewind(
            &provider_path,
            &index_path,
            &provider_path,
            "CREATE TRIGGER fail BEFORE DELETE ON matched_payouts
             BEGIN SELECT RAISE(ABORT, 'injected'); END;",
        );
        assert!(failed.is_err());
        let provider = ProviderStore::open(&provider_path).unwrap();
        let (index, _) = paid_index(&index_path);
        assert_eq!(index.tip().unwrap().height, 100);
        assert!(provider.unmatched(i64::MAX).unwrap().is_empty());
    }

    /// A failure during the index rewind leaves every match durably forgotten.
    #[test]
    fn a_failed_index_rewind_leaves_matches_forgotten() {
        let dir = tempfile::tempdir().unwrap();
        let (provider_path, index_path) = (
            dir.path().join("provider.sqlite"),
            dir.path().join("directory.sqlite"),
        );
        let failed = failing_rewind(
            &provider_path,
            &index_path,
            &index_path,
            "CREATE TRIGGER fail BEFORE DELETE ON blocks
             BEGIN SELECT RAISE(ABORT, 'injected'); END;",
        );
        assert!(failed.is_err());
        let provider = ProviderStore::open(&provider_path).unwrap();
        let (index, _) = paid_index(&index_path);
        assert_eq!(index.tip().unwrap().height, 100);
        assert_eq!(provider.unmatched(i64::MAX).unwrap().len(), 1);
    }

    /// A payout first seen complete more than an hour ago with no indexed payment in its
    /// transaction is reported missing, even to a receiver paid before; a newer one is
    /// not checked yet.
    #[test]
    fn report_counts_completed_payouts_missing_from_the_index() {
        let dir = tempfile::tempdir().unwrap();
        let mut provider = ProviderStore::open(dir.path().join("provider.sqlite")).unwrap();
        let (mut index, block) = paid_index(&dir.path().join("directory.sqlite"));
        index.append(&block).unwrap();
        let now = 1_000_000;
        // The indexed payout, then a later one to the same receiver that is not indexed.
        let payouts = [(receiver(1), [7; 32]), (receiver(1), [8; 32])];
        provider
            .record(
                "near-payouts",
                Some(0),
                &[],
                &payouts,
                now,
                now - 2 * 60 * 60,
            )
            .unwrap();
        let payouts = [(receiver(2), [9; 32])];
        provider
            .record("near-payouts", None, &[], &payouts, now, now - 60)
            .unwrap();
        provider
            .record("near-payouts", None, &[], &[], now, now - 30)
            .unwrap();
        let first = report(&mut provider, &index, now).unwrap();
        assert_eq!(first["payouts_checked"], 2);
        assert_eq!(first["payouts_missing"], 1);
        // A day later the matched payout is not checked again, while the missing one,
        // though old, is still reported, now beside the newer unindexed payout.
        let report = report(&mut provider, &index, now + 24 * 60 * 60).unwrap();
        assert_eq!(report["payouts_checked"], 2);
        assert_eq!(report["payouts_missing"], 2);
        assert_eq!(report["feeds"]["near-payouts"], now - 30);
        assert!(report["feeds"]["near-refunds"].is_null());
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
