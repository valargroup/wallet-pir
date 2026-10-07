//! NEAR Intents explorer feed for the receiver directory's recent and seen filters
//! (`receiver_directory::filter`). It reads every swap into or out of ZEC, newest first,
//! whichever app created it, and records the Orchard receiver of each payout
//! (`recipient`) and refund (`refundTo`) address with the swap's creation time. Unfunded
//! quotes count: they hold an address until their deadline. The explorer allows one
//! request every five seconds per partner key, which this module never logs.
use receiver_directory::{store::ProviderStore, Receiver};
use serde::Deserialize;
use std::time::Duration;
use zcash_address::{
    unified::{self, Container},
    ConversionError, TryFromAddress, ZcashAddress,
};
use zcash_protocol::consensus::NetworkType;

const ENDPOINT: &str = "https://explorer.near-intents.org/api/v0/transactions";
const STATUSES: &str = "FAILED,INCOMPLETE_DEPOSIT,PENDING_DEPOSIT,PROCESSING,REFUNDED,SUCCESS";
const PAGE: usize = 1000;
/// The explorer's per-partner rate limit, with margin.
const REQUEST_INTERVAL: Duration = Duration::from_millis(5_500);
/// How far back each read starts before the cursor, for swaps the explorer lists late.
const OVERLAP_SECS: i64 = 600;

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

/// One explorer record, reduced to what the feed reads.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Swap {
    recipient: String,
    refund_to: String,
    created_at_timestamp: i64,
    deposit_address: String,
    deposit_memo: Option<String>,
}

/// Explorer access with a partner key.
pub struct Explorer {
    http: reqwest::Client,
    key: String,
    next_request: tokio::time::Instant,
}

impl Explorer {
    /// An explorer client that authenticates with `key`.
    pub fn new(key: String) -> Result<Self> {
        Ok(Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(60))
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            key,
            next_request: tokio::time::Instant::now(),
        })
    }

    /// Reads `feed` back to its cursor, or to `since` on the first read, and records
    /// each swap's Orchard receiver in `store`. Returns how many receivers it recorded.
    pub async fn sync(
        &mut self,
        store: &mut ProviderStore,
        feed: Feed,
        since: i64,
    ) -> Result<usize> {
        let cursor = store.cursor(feed.name())?;
        let floor = cursor.map_or(since, |c| c - OVERLAP_SECS);
        let mut newest = cursor.unwrap_or(since);
        let mut found = Vec::new();
        let mut after: Option<(String, Option<String>)> = None;
        loop {
            let swaps = self.page(feed, after.as_ref()).await?;
            for swap in &swaps {
                if swap.created_at_timestamp < floor {
                    continue;
                }
                newest = newest.max(swap.created_at_timestamp);
                let address = match feed {
                    Feed::Payouts => &swap.recipient,
                    Feed::Refunds => &swap.refund_to,
                };
                if let Some(receiver) = orchard_receiver(address) {
                    found.push((receiver, feed == Feed::Payouts, swap.created_at_timestamp));
                }
            }
            match swaps.last() {
                Some(last) if swaps.len() == PAGE && last.created_at_timestamp >= floor => {
                    after = Some((last.deposit_address.clone(), last.deposit_memo.clone()));
                }
                _ => break,
            }
        }
        store.record(feed.name(), &found, newest)?;
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
            .get(ENDPOINT)
            .bearer_auth(&self.key)
            .query(&query)
            .send()
            .await;
        self.next_request = tokio::time::Instant::now() + REQUEST_INTERVAL;
        let response = response?;
        let status = response.status();
        if !status.is_success() {
            return Err(format!("NEAR explorer returned HTTP {status}").into());
        }
        Ok(response.json().await?)
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
