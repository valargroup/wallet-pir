//! The combined Enhance product the all-products shared-listener case adds to
//! the spawned host: its own HTTPS fixture publisher and the unchanged native
//! Enhance wallet.
use pir_e2e_tests::enhance_fixture::Publication;

/// Records served by the fixture generation, below one 2048-row unit.
const RECORDS: u64 = 67;

pub struct Enhance {
    publication: Publication,
    anchor_hash: String,
    /// The combined role's private control listener.
    pub control: String,
}

impl Enhance {
    pub async fn start() -> Self {
        let anchor_hash = "01".repeat(32);
        let publication = Publication::start(
            (0..RECORDS).map(super::enhance_wallet::record).collect(),
            enhance_shared::ACTIVATION_HEIGHT,
            anchor_hash.clone(),
        )
        .await;
        let control = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .to_string();
        Self {
            publication,
            anchor_hash,
            control,
        }
    }

    /// The host's `enhance` section: one combined role for domain 0, with its
    /// own state directory below the case root.
    pub fn section(&self, root: &std::path::Path) -> serde_json::Value {
        let mut synchronization = serde_json::to_value(&self.publication.source).unwrap();
        synchronization["shards"] = serde_json::json!([0]);
        serde_json::json!({"enabled": true, "settings": {
            "role": "combined",
            "data_dir": root.join("enhance-state"),
            "control_listen": self.control,
            "synchronization": synchronization,
        }})
    }

    /// The unchanged wallet decodes through the shared listener, and both
    /// Enhance prefixes serve the same manifest there.
    pub async fn wallet(&self, public: &str) {
        super::enhance_wallet::wallet_decodes(public, RECORDS, &self.anchor_hash).await;
        let canonical = reqwest::get(format!("{public}/enhance/v1/init"))
            .await
            .unwrap();
        assert_eq!(canonical.status(), 200);
        let deployed = reqwest::get(format!("{public}/v1/enhance/init"))
            .await
            .unwrap();
        assert_eq!(deployed.status(), 200);
        assert_eq!(
            deployed.bytes().await.unwrap(),
            canonical.bytes().await.unwrap()
        );
    }
}
