//! Independent publication source shared by TLS regression targets.
use enhance_publisher::{distribution::Distribution, zakura::ZakuraClient};
use enhance_query_server::{
    protocol::{self, Geometry, Lifecycle, Manifest},
    publication::Inventory,
};
use pir_e2e_tests::enhance_fixture::{record, serve, tls, Listening, Tls};
pub(crate) struct Published {
    pub(crate) scratch: tempfile::TempDir,
    pub(crate) distribution: Distribution,
    pub(crate) inventory: Inventory,
    pub(crate) manifest: Manifest,
    pub(crate) transport: Tls,
    pub(crate) _node: Listening,
    pub(crate) _publisher: Listening,
}
impl Published {
    pub(crate) async fn new() -> Self {
        Self::with_records(67).await
    }
    pub(crate) async fn with_records(count: u64) -> Self {
        let scratch = tempfile::tempdir().unwrap();
        let root = scratch.path().join("publication");
        let node = serve(axum::Router::new().route("/", axum::routing::post(|axum::Json(request): axum::Json<serde_json::Value>| async move {
            axum::Json(serde_json::json!({"id": request["id"], "error": null, "result": "01".repeat(32)}))
        }))).await;
        let cookie = scratch.path().join("cookie");
        std::fs::write(&cookie, "fixture:fixture").unwrap();
        let distribution = Distribution::open(
            &root,
            ZakuraClient::from_cookie_file(&node.url, cookie).unwrap(),
        )
        .unwrap();
        let publisher = serve(distribution.router()).await;
        let transport = tls(scratch.path(), &publisher.url).await;
        let inventory = tokio::task::spawn_blocking(move || {
            enhance_publisher::publication::prepare(
                &root,
                Lifecycle::default()
                    .coverage(count, Geometry::default())
                    .unwrap(),
                &[(0, "0".into())].into(),
                |start, count| Ok((start..start + count as u64).flat_map(record).collect()),
            )
        })
        .await
        .unwrap()
        .unwrap();
        let domain = &inventory.domains[0];
        let manifest = Manifest {
            recovery_epoch: 0,
            placement_revision: 1,
            domain_recovery_epochs: [(0, "0".into())].into(),
            schema_version: protocol::SCHEMA_VERSION,
            protocol_revision: protocol::PROTOCOL_REVISION.into(),
            network: "main".into(),
            pool: "ironwood".into(),
            generation: 1,
            anchor_height: 3428143,
            anchor_block_hash: "01".repeat(32),
            geometry: Geometry::default(),
            coverage: inventory.coverage.clone(),
            sessions: vec![domain.reference().unwrap()],
            unit_identities: [(0, domain.plan.units.clone())].into(),
        };
        distribution
            .publish(manifest.clone(), inventory.clone())
            .await
            .unwrap();
        Self {
            scratch,
            distribution,
            inventory,
            manifest,
            transport,
            _node: node,
            _publisher: publisher,
        }
    }
    pub(crate) fn source(&self) -> enhance_query_server::synchronization::Config {
        enhance_query_server::synchronization::Config {
            publisher_origin: self.transport.origin.clone(),
            trusted_ca: Some(self.scratch.path().join("ca.pem")),
            download_budget_bytes: 2 * 1024 * 1024 * 1024,
            cache_limit_bytes: 2 * 1024 * 1024 * 1024,
        }
    }
}
