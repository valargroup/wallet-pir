//! `PIR_APM_TRANSPARENT_CONFIG`: the monitored Transparent services and the
//! host snapshot, validated once at start.
//!
//! Each service is one published wallet API: a `history` service (a publisher
//! with a worker roster) or a `txid` service (a display controller with its
//! display workers). Rules choose by kind, never by name; every incident key
//! carries the service name.
use super::Target;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, path::PathBuf};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    #[default]
    History,
    Txid,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    #[default]
    Shadow,
    Active,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub host_snapshot: Option<PathBuf>,
    /// Rollout mode of the Transparent capacity family (forecasts and unit memory).
    #[serde(default)]
    pub capacity_alert_mode: Mode,
    pub services: Vec<Service>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Service {
    pub service: String,
    pub kind: Kind,
    #[serde(default)]
    pub public_origins: Vec<String>,
    /// Public API version prefix, such as `/v2`; empty serves from the root.
    #[serde(default)]
    pub path_prefix: String,
    #[serde(default)]
    pub alert_mode: Mode,
    // History services.
    pub publisher_url: Option<String>,
    pub status_url: Option<String>,
    pub roster: Option<PathBuf>,
    pub router_metrics_url: Option<String>,
    pub synthetic_status: Option<PathBuf>,
    /// Ready recent replicas below which the redundancy warning fires; 2 when
    /// absent. A fleet built with one recent replica sets 1, which leaves only
    /// the no-replica critical.
    pub min_recent_replicas: Option<u32>,
    // Txid services.
    pub controller_url: Option<String>,
    pub workers: Option<Vec<Worker>>,
}

/// One roster or configured worker. History rosters carry operator fields APM
/// does not read, so unknown fields are ignored here.
#[derive(Clone, Debug, Deserialize)]
pub struct Worker {
    pub id: String,
    pub role: String,
    pub upstream: String,
}

pub fn validate_url(value: &str) -> Result<()> {
    let u = reqwest::Url::parse(value)?;
    if u.scheme() == "file" {
        anyhow::ensure!(
            u.host_str().is_none()
                && u.to_file_path().is_ok()
                && u.query().is_none()
                && u.fragment().is_none(),
            "invalid local metrics snapshot"
        );
        return Ok(());
    }
    anyhow::ensure!(
        matches!(u.scheme(), "http" | "https")
            && u.host_str().is_some()
            && u.username().is_empty()
            && u.password().is_none()
            && u.query().is_none()
            && u.fragment().is_none(),
        "metrics URL must be credential-free HTTP(S)"
    );
    Ok(())
}

fn valid_name(name: &str) -> bool {
    (1..=32).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

impl Config {
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        let config: Self = serde_json::from_slice(bytes)?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            !self.services.is_empty() && self.services.len() <= 8,
            "services must list 1..8 services"
        );
        let mut names = BTreeSet::new();
        for s in &self.services {
            anyhow::ensure!(
                valid_name(&s.service),
                "service name must match [a-z0-9-]{{1,32}}"
            );
            anyhow::ensure!(
                names.insert(s.service.clone()),
                "duplicate service {}",
                s.service
            );
            s.validate()?;
        }
        Ok(())
    }
}

impl Service {
    fn validate(&self) -> Result<()> {
        let name = &self.service;
        anyhow::ensure!(
            self.path_prefix.is_empty()
                || self.path_prefix.strip_prefix("/v").is_some_and(|v| {
                    !v.is_empty() && v.len() <= 4 && v.bytes().all(|b| b.is_ascii_digit())
                }),
            "{name}: path_prefix must be empty or /v<digits>"
        );
        anyhow::ensure!(
            self.public_origins.len() <= 2,
            "{name}: at most two public origins are supported"
        );
        for origin in &self.public_origins {
            validate_url(origin)?;
        }
        match self.kind {
            Kind::History => {
                anyhow::ensure!(
                    self.publisher_url.is_some() && self.roster.is_some(),
                    "{name}: a history service needs publisher_url and roster"
                );
                anyhow::ensure!(
                    self.controller_url.is_none() && self.workers.is_none(),
                    "{name}: controller_url and workers belong to txid services"
                );
                anyhow::ensure!(
                    self.min_recent_replicas
                        .is_none_or(|n| (1..=32).contains(&n)),
                    "{name}: min_recent_replicas must be 1..32"
                );
            }
            Kind::Txid => {
                anyhow::ensure!(
                    self.controller_url.is_some()
                        && self
                            .workers
                            .as_ref()
                            .is_some_and(|w| (1..=32).contains(&w.len())),
                    "{name}: a txid service needs controller_url and 1..32 workers"
                );
                anyhow::ensure!(
                    self.publisher_url.is_none()
                        && self.status_url.is_none()
                        && self.roster.is_none()
                        && self.router_metrics_url.is_none()
                        && self.synthetic_status.is_none()
                        && self.min_recent_replicas.is_none(),
                    "{name}: publisher, roster, edge and synthetic fields belong to history services"
                );
            }
        }
        for url in [
            &self.publisher_url,
            &self.status_url,
            &self.router_metrics_url,
            &self.controller_url,
        ]
        .into_iter()
        .flatten()
        {
            validate_url(url)?;
        }
        if self.kind == Kind::Txid {
            // Static workers are checked at start, not only at the first inventory.
            self.targets()?;
        }
        Ok(())
    }

    /// The origin this service's coordinator answers `/v1/status` on.
    pub fn status_url(&self) -> String {
        match self.kind {
            Kind::History => self.status_url.clone().unwrap_or_else(|| {
                let url = self.publisher_url.as_deref().unwrap_or_default();
                format!("{}/v1/status", url.strip_suffix("/metrics").unwrap_or(url))
            }),
            Kind::Txid => format!(
                "{}/v1/status",
                self.controller_url
                    .as_deref()
                    .unwrap_or_default()
                    .trim_end_matches('/')
            ),
        }
    }

    /// The public path whose body names the published map digest.
    pub fn public_init_path(&self) -> String {
        match self.kind {
            Kind::History => format!("{}/shards/init", self.path_prefix),
            Kind::Txid => format!("{}/txid/init", self.path_prefix),
        }
    }

    fn target(&self, id: String, role: &str, url: String, ready: Option<String>) -> Target {
        Target {
            id,
            service: self.service.clone(),
            role: role.into(),
            url,
            ready,
            status_operation: None,
            endpoint: None,
        }
    }

    /// Current scrape targets. History services reread their roster each call.
    pub fn targets(&self) -> Result<Vec<Target>> {
        let name = &self.service;
        let (mut targets, workers, roles): (Vec<Target>, Vec<Worker>, &[&str]) = match self.kind {
            Kind::History => {
                let path = self.roster.as_ref().expect("validated");
                let bytes = std::fs::read(path)?;
                anyhow::ensure!(bytes.len() <= 64 * 1024, "roster too large");
                (
                    vec![self.target(
                        format!("{name}-publisher"),
                        "publisher",
                        self.publisher_url.clone().expect("validated"),
                        None,
                    )],
                    serde_json::from_slice(&bytes)?,
                    &["archive-owner", "recent-replica"],
                )
            }
            Kind::Txid => {
                let base = self.controller_url.as_deref().expect("validated");
                let base = base.trim_end_matches('/');
                (
                    vec![self.target(
                        format!("{name}-controller"),
                        "display-controller",
                        format!("{base}/metrics"),
                        None,
                    )],
                    self.workers.clone().expect("validated"),
                    &["display-worker"],
                )
            }
        };
        anyhow::ensure!(
            !workers.is_empty() && workers.len() <= 32,
            "roster must contain 1..32 workers"
        );
        let mut ids = BTreeSet::new();
        for w in workers {
            anyhow::ensure!(
                w.id.len() <= 80
                    && w.id
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
                    && ids.insert(w.id.clone())
                    && roles.contains(&w.role.as_str()),
                "invalid worker identity or role"
            );
            let origin = format!("http://{}", w.upstream);
            let u = reqwest::Url::parse(&origin)?;
            anyhow::ensure!(u.path() == "/", "worker upstream must be host:port");
            targets.push(self.target(
                w.id,
                &w.role,
                format!("{origin}/metrics"),
                Some(format!("{origin}/v1/ready")),
            ));
        }
        if let Some(url) = &self.router_metrics_url {
            targets.push(self.target(format!("{name}-edge"), "edge", url.clone(), None));
        }
        for target in &targets {
            validate_url(&target.url)?;
        }
        Ok(targets)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn example(roster: &std::path::Path) -> serde_json::Value {
        serde_json::json!({
            "host_snapshot": "/var/lib/pir-apm/hosts.json",
            "services": [
                {"service": "transparent", "kind": "history",
                 "publisher_url": "http://127.0.0.1:8094/metrics",
                 "status_url": "http://127.0.0.1:8094/v1/status",
                 "roster": roster,
                 "router_metrics_url": null, "synthetic_status": null,
                 "public_origins": ["https://transparent-pir.valargroup.dev"],
                 "path_prefix": "/v2", "alert_mode": "shadow"},
                {"service": "txid", "kind": "txid",
                 "controller_url": "http://127.0.0.1:8098",
                 "workers": [{"id": "transparent-pir-txid-k8-01", "role": "display-worker", "upstream": "10.142.0.18:8095"}],
                 "public_origins": ["https://transparent-pir.valargroup.dev"],
                 "path_prefix": "/v2", "alert_mode": "shadow"}
            ]
        })
    }

    fn roster() -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "pir-apm-roster-{}-{:?}.json",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::write(
            &path,
            r#"[{"id":"transparent-pir-recent-01","role":"recent-replica","upstream":"10.0.0.10:8093","ssh_host":"10.0.0.10"},
                {"id":"transparent-pir-archive-03","role":"archive-owner","upstream":"10.0.0.7:8093","ssh_host":"10.0.0.7","archive_range":[0,81]}]"#,
        )
        .unwrap();
        path
    }

    #[test]
    fn both_services_parse_into_kind_targets() {
        let path = roster();
        let config = Config::parse(&serde_json::to_vec(&example(&path)).unwrap()).unwrap();
        assert_eq!(config.capacity_alert_mode, Mode::Shadow);
        let [history, txid] = &config.services[..] else {
            panic!("two services")
        };
        assert_eq!(history.kind, Kind::History);
        assert_eq!(history.status_url(), "http://127.0.0.1:8094/v1/status");
        assert_eq!(history.public_init_path(), "/v2/shards/init");
        let t = history.targets().unwrap();
        assert_eq!(
            t.iter()
                .map(|t| (t.id.as_str(), t.role.as_str()))
                .collect::<Vec<_>>(),
            [
                ("transparent-publisher", "publisher"),
                ("transparent-pir-recent-01", "recent-replica"),
                ("transparent-pir-archive-03", "archive-owner")
            ]
        );
        assert!(t.iter().all(|t| t.service == "transparent"));
        assert_eq!(txid.status_url(), "http://127.0.0.1:8098/v1/status");
        assert_eq!(txid.public_init_path(), "/v2/txid/init");
        let t = txid.targets().unwrap();
        assert_eq!(t[0].id, "txid-controller");
        assert_eq!(t[0].url, "http://127.0.0.1:8098/metrics");
        assert_eq!(t[1].role, "display-worker");
        assert_eq!(
            t[1].ready.as_deref(),
            Some("http://10.142.0.18:8095/v1/ready")
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn invalid_configs_are_refused() {
        let path = roster();
        let base = example(&path);
        let refused = |edit: &dyn Fn(&mut serde_json::Value)| {
            let mut v = base.clone();
            edit(&mut v);
            Config::parse(&serde_json::to_vec(&v).unwrap()).is_err()
        };
        assert!(!refused(&|_| {}));
        assert!(refused(
            &|v| v["services"][1]["service"] = "transparent".into()
        ));
        assert!(refused(
            &|v| v["services"][0]["service"] = "Transparent".into()
        ));
        assert!(refused(
            &|v| v["services"][0]["service"] = "a".repeat(33).into()
        ));
        assert!(refused(&|v| v["services"][0]["kind"] = "display".into()));
        assert!(refused(&|v| v["services"][0]["path_prefix"] = "/v2/".into()));
        assert!(refused(&|v| v["services"][0]["path_prefix"] = "/api".into()));
        assert!(refused(&|v| v["services"][0]["alert_mode"] = "loud".into()));
        assert!(refused(
            &|v| v["services"][1]["workers"] = serde_json::json!([])
        ));
        assert!(refused(
            &|v| v["services"][1]["workers"][0]["role"] = "archive-owner".into()
        ));
        assert!(refused(
            &|v| v["services"][1]["roster"] = "/tmp/roster.json".into()
        ));
        assert!(refused(
            &|v| v["services"][0]["controller_url"] = "http://127.0.0.1:1".into()
        ));
        assert!(refused(&|v| {
            v["services"][0]
                .as_object_mut()
                .unwrap()
                .remove("publisher_url");
        }));
        // The old single-service shape is not accepted.
        assert!(refused(&|v| {
            *v = serde_json::json!({"publisher_url": "http://127.0.0.1:8094/metrics", "roster": "/x"})
        }));
        assert!(refused(&|v| v["services"][0]["unknown"] = 1.into()));
        let _ = std::fs::remove_file(path);
    }
}
