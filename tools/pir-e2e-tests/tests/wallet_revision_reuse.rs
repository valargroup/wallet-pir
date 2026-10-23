//! A tail revision reused across a reorg, as the pinned wallet client meets it.
//!
//! One companion, kept on disk and reopened between passes, recovers from a
//! publication whose unsealed tail is at revision 3. A reorg of the tail's last
//! block is then published under the same revision 3 (what the publisher did
//! before its revision floor), and the client withdraws it as an equivocation.
//! The repaired publication numbers the replacement above every revision the
//! shard was exposed under, and the same companion recovers from it over real
//! HTTP PIR without being deleted or recreated.
#![cfg(feature = "transparent-history")]

#[path = "../../../src/pir-query-server/tests/transparent/common/mod.rs"]
mod common;
use common::*;
use std::path::Path;
use transparent::address::TransparentAddress;
use zakura_pir_client::transparent::engine::StaticChain;
use zakura_pir_client::transparent::recovery_types::{
    AccountLifecycle, ChainPoint, RecoveryBlockHash, TransparentAddressWatchSet, WatchOrigin,
    WatchedAddress,
};
use zakura_pir_client::transparent::{
    BatchState, HttpExchange, HttpFailure, HttpMethod, HttpReply, HttpRequest, Outcome,
    TransparentHistoryRecovery, TransparentHistoryRecoveryConfig, WithdrawnCause,
};
use zakura_pir_protocol::transparent::filter::{BlockHash, ShardMap};
use zakura_pir_publisher::transparent::layout::DECOUPLED_2K_8K;

/// The tail's last block, where the reorg lands.
const TIP: u64 = FIRST + SHARDS * SPAN - 1;

/// The local service over blocking HTTP, as a wallet's transport reaches it.
struct Local {
    base: String,
    client: reqwest::blocking::Client,
}

impl HttpExchange for Local {
    fn send(&self, request: &HttpRequest) -> Result<HttpReply, HttpFailure> {
        let url = format!("{}{}", self.base, request.path);
        let builder = match request.method {
            HttpMethod::Get => self.client.get(url),
            HttpMethod::Post => self.client.post(url).body(request.body.clone()),
        };
        let response = builder.send().map_err(|_| HttpFailure::Unreachable)?;
        let header = |name: &str| {
            response
                .headers()
                .get(name)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned)
        };
        let (status, retry_after, map_sha256) = (
            response.status().as_u16(),
            header("retry-after"),
            header("x-shard-map-sha256"),
        );
        let body = response.bytes().map_err(|_| HttpFailure::Unreachable)?;
        if body.len() > request.limit.max(request.error_limit) {
            return Err(HttpFailure::TooLarge);
        }
        Ok(HttpReply {
            status,
            retry_after,
            map_sha256,
            body: body.to_vec(),
        })
    }
}

fn config() -> TransparentHistoryRecoveryConfig {
    TransparentHistoryRecoveryConfig {
        source: b"wallet-revision-reuse".to_vec(),
        account_binding: b"account-0".to_vec(),
        origin: "https://pir.example".to_owned(),
        scripts: 16,
        shards: 64,
        events: 10_000,
        queries: 10_000,
        private_bytes: u64::MAX / 2,
    }
}

/// Script 2's address: the harness's scripts are P2PKH over the tag.
fn watch(map: &ShardMap) -> TransparentAddressWatchSet<u32> {
    let mut hash = [0u8; 20];
    hash[..4].copy_from_slice(&2u32.to_le_bytes());
    let last = map.shards.last().unwrap();
    let mut terminal = hex::decode(&last.terminal_block_hash).unwrap();
    terminal.reverse();
    TransparentAddressWatchSet {
        account: 0,
        lifecycle: AccountLifecycle::Candidate,
        policy_generation: 0,
        target: Some(ChainPoint {
            height: (last.end_height as u32).into(),
            hash: RecoveryBlockHash(terminal.try_into().unwrap()),
        }),
        addresses: vec![WatchedAddress {
            address: TransparentAddress::PublicKeyHash(hash),
            origin: WatchOrigin::Standalone,
            required_from: (FIRST as u32).into(),
        }],
        pending_pages: vec![],
    }
}

/// One pass of a companion reopened from `path` against the service at `base`.
fn pass(path: &Path, map: &ShardMap, base: &str) -> zakura_pir_client::transparent::PassReport {
    let mut recovery = TransparentHistoryRecovery::open(path, config()).unwrap();
    let exchange = Local {
        base: base.to_owned(),
        client: reqwest::blocking::Client::new(),
    };
    recovery
        .pass(
            &watch(map),
            &StaticChain::from_map(map),
            exchange,
            64 << 20,
            &|| false,
        )
        .unwrap()
}

fn tail(map: &ShardMap) -> (u64, u32, &str) {
    let tail = map.shards.last().unwrap();
    (tail.shard_id, tail.revision, tail.manifest_digest.as_str())
}

#[test]
fn a_reused_tail_revision_is_withdrawn_and_the_repaired_one_recovers_the_same_companion() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let root = tempfile::tempdir().unwrap();
    let events = chain();
    let publish_at = |name: &str, revision: u32, hash: &dyn Fn(u64) -> BlockHash| {
        let dir = root.path().join(name);
        std::fs::create_dir(&dir).unwrap();
        let map = publish_with(&dir, &events, |_| &DECOUPLED_2K_8K, revision, "", hash);
        let base = runtime.block_on(serve(&dir));
        (map, base)
    };
    let forked = hash_forked(TIP);
    let (accepted, accepted_at) = publish_at("accepted", 3, &hash_at);
    let (reused, reused_at) = publish_at("reused", 3, &forked);
    let (repaired, repaired_at) = publish_at("repaired", 5, &forked);
    assert_eq!(tail(&accepted).0, tail(&reused).0);
    assert_eq!(tail(&accepted).1, tail(&reused).1);
    assert_ne!(tail(&accepted).2, tail(&reused).2);

    let companion = root.path().join("companion.sqlite");
    let first = pass(&companion, &accepted, &accepted_at);
    assert_eq!(first.state, BatchState::Ready);
    assert_eq!(first.progress.outcome, Outcome::Complete);
    assert_eq!(first.progress.covered_through, TIP);
    assert!(first.commits > 0, "the pass exported history");

    // The reorg republished under revision 3: the kept catalog refuses it.
    let second = pass(&companion, &reused, &reused_at);
    assert_eq!(
        second.state,
        BatchState::Withdrawn(WithdrawnCause::Equivocation)
    );
    assert_eq!(second.commits, 0);

    // The repaired publication numbers the replacement above revision 3. The
    // same companion file, never deleted, recovers through private queries.
    let third = pass(&companion, &repaired, &repaired_at);
    assert_eq!(third.state, BatchState::Ready, "{third:?}");
    assert_eq!(third.progress.outcome, Outcome::Complete);
    assert_eq!(third.progress.covered_through, TIP);
    assert!(companion.exists());

    // The repaired publication stays accepted, and the reused one stays refused.
    assert_eq!(
        pass(&companion, &repaired, &repaired_at).state,
        BatchState::Ready
    );
    assert_eq!(
        pass(&companion, &reused, &reused_at).state,
        BatchState::Withdrawn(WithdrawnCause::Equivocation)
    );
}
