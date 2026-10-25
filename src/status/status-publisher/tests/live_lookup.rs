//! Live encrypted Status lookups against a deployed wallet origin, without node RPC.
//!
//! The oracle is a mined txid and height from an independent block source, passed
//! in the environment. The manifest anchor is accepted on the mainnet network only;
//! use `pir-publisher status probe-live` where a canonical node is available.
#![cfg(feature = "cli")]

use status_publisher::cli::http_client::HttpClient;
use status_shared::protocol::*;
use std::time::Duration;

/// Mainnet genesis hash in internal byte order.
const MAINNET: &str = "08ce3d9731b000c08338455c8a4a6bd05da16e26b11daa1b917184ece80f0400";

fn internal(display_hex: &str) -> Hash {
    let mut bytes: [u8; 32] = hex::decode(display_hex).unwrap().try_into().unwrap();
    bytes.reverse();
    bytes
}

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} is required"))
}

#[tokio::test]
#[ignore = "contacts a deployed Status origin"]
async fn status_live_lookup() {
    let origin = env("STATUS_LIVE_URL");
    let txid = internal(&env("STATUS_LIVE_TXID"));
    let height: u32 = env("STATUS_LIVE_HEIGHT").parse().unwrap();
    let network: Hash = hex::decode(MAINNET).unwrap().try_into().unwrap();
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .unwrap();
    let mut wallet = HttpClient::new(origin, http);
    let accept = |m: &Manifest| {
        if m.network != network {
            return Err(Error::Malformed);
        }
        Ok(AcceptedAnchor {
            network,
            height: m.anchor_height,
            hash: m.anchor_hash,
        })
    };
    let mined = wallet.lookup(txid, Some(height), accept).await.unwrap();
    assert_eq!(mined, Observation::Mined(height));
    let absent = wallet
        .lookup([0x5a; 32], Some(height), accept)
        .await
        .unwrap();
    assert_eq!(absent, Observation::NotFound);
}
