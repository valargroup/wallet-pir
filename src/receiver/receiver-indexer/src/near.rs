//! Optional NEAR payout audit. Provider data never governs issuance or publication.
use receiver_store::{PayoutStore, Store};
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
use zakura_pir_publisher::receiver::crypto::{Network, Receiver};
use zakura_pir_publisher::receiver::ReceiverBytes;

const ENDPOINT: &str = "https://explorer.near-intents.org/api/v0/transactions";
/// The status of a completed swap, whose payout transactions the explorer reports.
const SUCCESS: &str = "SUCCESS";
const PAGE: usize = 1000;
/// Bound on one page's body: a record is about 1 KB, so eight times a full page.
const MAX_PAGE_BYTES: usize = 8 * 1024 * 1024;
/// The explorer's per-partner rate limit, with margin.
const REQUEST_INTERVAL: Duration = Duration::from_millis(5_500);
/// Bound on one read, about a month of swaps, so an explorer that ignores paging cannot
/// loop forever. It bounds a first read too: one from a `since` too far back fails
/// rather than record a feed that skipped unread history.
const MAX_PAGES: usize = 500;
/// Overlap for late completions, and retention for uncheckable audit records.
pub const AUDIT_LOOKBACK_SECS: i64 = 24 * 60 * 60;
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
/// within [`AUDIT_LOOKBACK_SECS`].
const UNCHECKABLE_ZEROS: usize = 16;

/// Whether `txid` is the record of an uncheckable payout; see [`UNCHECKABLE_ZEROS`].
fn uncheckable(txid: &zakura_pir_publisher::receiver::Hash) -> bool {
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

/// Payout audit inputs read from one consistent database snapshot.
pub struct Capture {
    /// When each feed's last complete read began.
    feeds: serde_json::Map<String, serde_json::Value>,
    /// Checkable payouts first seen complete more than [`COMPLETION_GRACE_SECS`]
    /// before the terminal block's time.
    payouts: Vec<(ReceiverBytes, zakura_pir_publisher::receiver::Hash)>,
    /// Uncheckable payouts first seen in that span but within [`AUDIT_LOOKBACK_SECS`].
    uncheckable: usize,
}

/// Captures the audit for chain time `anchor_time` in one read transaction.
pub fn capture(store: &PayoutStore, anchor_time: i64) -> Result<Capture> {
    store.view(|store| {
        let mut feeds = serde_json::Map::new();
        let feed = Feed::Payouts.name();
        feeds.insert(feed.into(), store.read(feed)?.into());
        // Those first seen by the earlier bound are a subset of those seen by the later.
        let count = |until| -> Result<usize> {
            let payouts = store.payouts(until)?;
            Ok(payouts.iter().filter(|p| uncheckable(&p.1)).count())
        };
        let mut payouts = store.payouts(anchor_time - COMPLETION_GRACE_SECS)?;
        payouts.retain(|p| !uncheckable(&p.1));
        Ok(Capture {
            feeds,
            payouts,
            uncheckable: count(anchor_time - COMPLETION_GRACE_SECS)?
                - count(anchor_time - AUDIT_LOOKBACK_SECS)?,
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
}

impl Feed {
    /// The feed's cursor name in the provider store. The previous release's
    /// `near-payouts` cursor also bounds its receiver set, so this audit keeps its own
    /// and leaves that one for a rollback to resume from.
    pub fn name(self) -> &'static str {
        match self {
            Self::Payouts => "near-payout-audit",
        }
    }

    /// The explorer parameter that selects this direction's ZEC swaps.
    fn chain_filter(self) -> &'static str {
        match self {
            Self::Payouts => "toChainId",
        }
    }
}

/// This process's evidence that the explorer served its key: when the last payout read
/// this process completed began, in Unix seconds. Health reports it as `near.reads`,
/// null until this process completes a read, so it stays null without a key. The
/// report's `feeds` cannot serve as this evidence: they come from `providers.sqlite`,
/// which may hold an earlier process's reads.
#[derive(Clone, Default)]
pub struct Reads(Arc<[AtomicI64; 1]>);

impl Reads {
    /// Records that a read of `feed` that began at `began` completed.
    fn record(&self, feed: Feed, began: i64) {
        self.0[feed as usize].store(began, Ordering::SeqCst);
    }

    /// Health's `near` field: `{"reads": {"near-payout-audit": <began>|null}}`.
    pub fn health(&self) -> serde_json::Map<String, serde_json::Value> {
        let reads: serde_json::Map<_, _> = [Feed::Payouts]
            .into_iter()
            .map(|feed| {
                let began = self.0[feed as usize].load(Ordering::SeqCst);
                (feed.name().into(), (began != 0).then_some(began).into())
            })
            .collect();
        serde_json::Map::from_iter([("near".into(), serde_json::json!({ "reads": reads }))])
    }
}

/// Reads the payout audit once into the audit store at `path` (see
/// [`Explorer::sync`]), recording each completed read in `reads`. A failure is logged
/// and leaves its feed's entry unchanged.
pub async fn poll(explorer: &mut Explorer, path: &Path, since: i64, reads: &Reads) {
    let mut store = match PayoutStore::open(path) {
        Ok(store) => store,
        Err(error) => return tracing::warn!(%error, "near provider store unavailable"),
    };
    for feed in [Feed::Payouts] {
        let began = unix_now();
        match explorer.sync(&mut store, feed, since).await {
            Ok(payout_records) => {
                reads.record(feed, began);
                tracing::info!(feed = feed.name(), payout_records, "near feed read")
            }
            Err(error) => tracing::warn!(feed = feed.name(), %error, "near feed deferred"),
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

    /// Audits completed ZEC payouts with a bounded overlap for late completion, returning
    /// how many payout records the read found. A failed or oversized read commits
    /// neither payouts nor its cursor.
    pub async fn sync(&mut self, store: &mut PayoutStore, feed: Feed, since: i64) -> Result<usize> {
        let read_at = unix_now();
        let cursor = store.cursor(feed.name())?;
        let floor = cursor.map_or(since, |c| c - AUDIT_LOOKBACK_SECS);
        let mut newest = cursor.unwrap_or(since).min(read_at);
        let mut completed = Vec::new();
        self.scan(
            feed,
            SUCCESS,
            floor,
            read_at,
            cursor.is_none(),
            |swap, created| {
                newest = newest.max(created);
                if let Some(receiver) = swap.recipient.as_deref().and_then(orchard_receiver) {
                    completions(swap, receiver, &mut completed);
                }
            },
        )
        .await?;
        let seen_at = unix_now();
        let completed: Vec<_> = completed
            .into_iter()
            .map(|(r, t)| (r, t, seen_at))
            .collect();
        store.record(feed.name(), &completed, newest, read_at)?;
        Ok(completed.len())
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
    out: &mut Vec<(ReceiverBytes, zakura_pir_publisher::receiver::Hash)>,
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
            serde_json::json!({"near-payout-audit": now - 30})
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
    fn reads_report_only_the_payout_feed() {
        let reads = Reads::default();
        let health = |reads: &Reads| serde_json::Value::Object(reads.health());
        let expected = |began| serde_json::json!({"near": {"reads": {"near-payout-audit": began}}});
        assert_eq!(health(&reads), expected(serde_json::Value::Null));
        reads.record(Feed::Payouts, 5);
        assert_eq!(health(&reads), expected(5.into()));
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
            Json(vec![swap; if oversized.load(Ordering::SeqCst) { PAGE } else { 1 }])
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
}
