//! HTTPS transaction-detail publisher for one real prepared display
//! publication: discovery, the current pointer, the digest-bound inventory, its
//! artifacts and canonical anchors, behind the publisher distribution TLS
//! terminator. The tables, shipped runtimes and inventory are the shared
//! producer's; only the transport is a fixture.
use super::display_world::{params, records, spec, write_shard};
use axum::{
    http::{HeaderMap, StatusCode, Uri},
    response::IntoResponse,
    Router,
};
use std::{collections::BTreeMap, sync::Arc};
use transparent_shard::txid::DisplayRecord;
use transparent_shared::publication::{self, AnchorCheck, Current, Eligibility};
use transparent_shared::tx_detail::{
    distribution::{Finalized, PREFIX, SCHEMA},
    prebuild::build_shipped_verified,
    set::DisplaySet,
    synth, DisplayRole,
};
use zakura_pir_publisher::transparent as transparent_shard;

#[path = "../../../../src/pir-query-server/tests/support/status_tls.rs"]
mod tls;

pub struct DisplayPublisher {
    pub origin: String,
    pub certificate: std::path::PathBuf,
    /// Records of the sealed archive shard over 100..=199.
    pub archive: Vec<DisplayRecord>,
    /// Records of the recent shard over 200..=260.
    pub recent: Vec<DisplayRecord>,
    _root: tempfile::TempDir,
    _tls: tls::Tls,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for DisplayPublisher {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl DisplayPublisher {
    /// Publish one archive and one recent shard, with their shipped native
    /// runtimes, as revision 3.
    pub async fn start() -> Self {
        let root = tempfile::tempdir().unwrap();
        let archive = records(30, &[]);
        let recent = records(40, &[]);
        let a0 = write_shard(root.path(), &spec(0, 100, 199, true, ""), &archive);
        let r0 = write_shard(root.path(), &spec(1, 200, 260, false, &a0.digest), &recent);
        let (dir, _) = synth::write_candidate(root.path(), &params(1), &[a0, r0], "p0").unwrap();
        let finalized = tokio::task::block_in_place(|| {
            let set = DisplaySet::open(&dir, 0, DisplayRole::Combined).unwrap();
            for revision in set.current() {
                build_shipped_verified(revision, &dir, &[]).unwrap();
            }
            Finalized::prepare_set(&dir, &set).unwrap()
        });
        let current = Current {
            schema: SCHEMA.into(),
            revision: 3,
            recovery_epoch: 0,
            inventory_sha256: finalized.sha256.clone(),
            network: synth::NETWORK.into(),
            genesis_hash: synth::genesis_hash(),
            anchor_height: finalized.anchor_height,
            anchor_hash: finalized.anchor_hash.clone(),
            eligibility: Eligibility::Canonical,
            invalidated_from: None,
        };
        let current_bytes = serde_json::to_vec(&current).unwrap();
        let token = format!("\"{}\"", publication::digest(&current_bytes));
        let mut replies = BTreeMap::<String, Vec<u8>>::new();
        replies.insert(format!("{PREFIX}current"), current_bytes);
        replies.insert(
            format!("{PREFIX}inventories/{}", finalized.sha256),
            finalized.bytes.clone(),
        );
        for (sha256, (path, _)) in &finalized.files {
            replies.insert(
                format!("{PREFIX}artifacts/{sha256}"),
                std::fs::read(path).unwrap(),
            );
        }
        let set = DisplaySet::open(&dir, 0, DisplayRole::Combined).unwrap();
        for shard in &set.map.shards {
            let check = AnchorCheck {
                schema: SCHEMA.into(),
                revision: current.revision,
                recovery_epoch: current.recovery_epoch,
                genesis_hash: current.genesis_hash.clone(),
                height: shard.end_height,
                hash: shard.terminal_block_hash.clone(),
                canonical: true,
            };
            replies.insert(
                format!(
                    "{PREFIX}anchors/{}/{}",
                    shard.end_height, shard.terminal_block_hash
                ),
                serde_json::to_vec(&check).unwrap(),
            );
        }
        let replies = Arc::new(replies);
        let app = Router::new()
            .route(
                pir_service_discovery::PATH,
                axum::routing::get(|| async {
                    pir_service_discovery::Document::publisher().to_json()
                }),
            )
            .fallback(axum::routing::get(move |uri: Uri, headers: HeaderMap| {
                let unchanged = uri.path() == format!("{PREFIX}current")
                    && headers
                        .get("if-none-match")
                        .is_some_and(|h| h.as_bytes() == token.as_bytes());
                let body = replies.get(uri.path()).cloned();
                async move {
                    if unchanged {
                        return StatusCode::NOT_MODIFIED.into_response();
                    }
                    body.map_or(StatusCode::NOT_FOUND.into_response(), |b| b.into_response())
                }
            }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let tls = tls::Tls::start(&upstream).await;
        Self {
            origin: tls.origin.clone(),
            certificate: tls.certificate.clone(),
            archive,
            recent,
            _root: root,
            _tls: tls,
            server,
        }
    }
}
