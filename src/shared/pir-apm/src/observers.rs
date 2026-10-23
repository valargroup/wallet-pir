//! Central collection of per-host sidecar observations.
//!
//! Every 5 seconds the APM reads each enrolled host's `GET /v1/observations`
//! over mutually authenticated HTTPS, as the collector identity, trusting only
//! the dedicated monitoring CA and the host's expected TLS name. Sidecars decide
//! check status; this module only propagates it into durable incidents in the
//! `host` family, keyed `host:<host>:check:<name>` and `host:<host>:coverage`.
//!
//! A check is unknown, never healthy, when it is older than 45 seconds or the
//! document is unreachable, unauthenticated, malformed, of another schema, from
//! another host, rendered from another configuration, or lacks a configured
//! check. Coverage warns once 45 seconds pass without a valid document, from
//! process start for hosts never observed. Recovery needs two distinct healthy
//! observations: a repeated document carries the same timestamps and counts
//! once. Enrollment is persisted and survives a host's removal from the
//! inventory; only an explicit `decommissioned` entry retires its incidents.
use crate::incidents::{Condition, Incident, Store};
use anyhow::{Context, Result};
use pir_host_observations::{Observations, Status, MAX_AGE_SECONDS, MAX_BODY_BYTES, PATH};
use serde::Deserialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

pub const CONFIG_VAR: &str = "PIR_APM_OBSERVERS";
pub const FAMILY: &str = "host";
pub const POLL_SECONDS: u64 = 5;
const ENROLLMENT_KEY: &str = "observer_enrollment";
const CONCURRENCY: usize = 16;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    /// `shadow` records incidents without Slack delivery; `active` delivers.
    pub mode: String,
    pub collector: Collector,
    pub hosts: Vec<Host>,
    #[serde(default)]
    pub decommissioned: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Collector {
    /// Relative paths resolve against `$CREDENTIALS_DIRECTORY`.
    pub cert: PathBuf,
    pub key: PathBuf,
    pub ca: PathBuf,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Host {
    pub host_id: String,
    pub address: SocketAddr,
    pub tls_name: String,
    pub config_digest: String,
    pub checks: Vec<String>,
    /// Where to write the host's aggregate Caddy metrics for the edge rules.
    #[serde(default)]
    pub edge_output: Option<PathBuf>,
}

fn credential(path: &Path) -> PathBuf {
    if path.is_absolute() {
        return path.to_path_buf();
    }
    std::env::var_os("CREDENTIALS_DIRECTORY")
        .map(PathBuf::from)
        .unwrap_or_default()
        .join(path)
}

impl Settings {
    pub fn load(path: &Path) -> Result<Self> {
        let settings: Self = serde_json::from_slice(&std::fs::read(path)?)
            .context("parsing observer inventory")?;
        anyhow::ensure!(
            matches!(settings.mode.as_str(), "shadow" | "active"),
            "observer mode must be shadow or active"
        );
        let mut ids = BTreeSet::new();
        for h in &settings.hosts {
            anyhow::ensure!(
                pir_host_observations::valid_name(&h.host_id, 80) && ids.insert(&h.host_id),
                "invalid or duplicate observer host id"
            );
            anyhow::ensure!(
                !settings.decommissioned.contains(&h.host_id),
                "a decommissioned host is still in the observer inventory"
            );
        }
        Ok(settings)
    }

    pub fn shadow(&self) -> bool {
        self.mode != "active"
    }
}

/// The result of one poll; failure reasons are fixed strings safe for Slack.
#[derive(Clone, Debug)]
pub enum Poll {
    Valid(Observations),
    Failed(&'static str),
}

pub fn client(settings: &Settings, host: &Host) -> Result<reqwest::Client> {
    let c = &settings.collector;
    let mut identity = std::fs::read(credential(&c.cert)).context("reading collector cert")?;
    identity.extend(std::fs::read(credential(&c.key)).context("reading collector key")?);
    let ca = std::fs::read(credential(&c.ca)).context("reading monitoring CA")?;
    reqwest::Client::builder()
        .use_rustls_tls()
        .tls_built_in_root_certs(false)
        .add_root_certificate(reqwest::Certificate::from_pem(&ca)?)
        .identity(reqwest::Identity::from_pem(&identity)?)
        .https_only(true)
        .redirect(reqwest::redirect::Policy::none())
        .resolve(&host.tls_name, host.address)
        .timeout(Duration::from_secs(5))
        .build()
        .context("building observer client")
}

async fn read_limited(mut response: reqwest::Response) -> Option<Vec<u8>> {
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.ok()? {
        body.extend_from_slice(&chunk);
        if body.len() > MAX_BODY_BYTES {
            return None;
        }
    }
    Some(body)
}

fn url(host: &Host, path: &str) -> String {
    format!("https://{}:{}{path}", host.tls_name, host.address.port())
}

pub async fn poll(client: &reqwest::Client, host: &Host) -> Poll {
    let Ok(response) = client.get(url(host, PATH)).send().await else {
        return Poll::Failed("sidecar unreachable or authentication failed");
    };
    if !response.status().is_success() {
        return Poll::Failed("sidecar returned an error status");
    }
    let Some(body) = read_limited(response).await else {
        return Poll::Failed("observation body unreadable or over 2 MiB");
    };
    match Observations::parse(&body, &host.host_id, &host.config_digest) {
        Err(rejection) => Poll::Failed(rejection.reason()),
        Ok(doc) if host.checks.iter().any(|c| !doc.checks.iter().any(|d| &d.name == c)) => {
            Poll::Failed("observation lacks configured checks")
        }
        Ok(doc) => Poll::Valid(doc),
    }
}

/// Copy the host's aggregate Caddy metrics where the edge quality rules read them.
pub async fn edge(client: &reqwest::Client, host: &Host) -> Result<()> {
    let Some(output) = &host.edge_output else {
        return Ok(());
    };
    let response = client.get(url(host, "/v1/edge-metrics")).send().await?;
    anyhow::ensure!(response.status().is_success(), "edge metrics unavailable");
    let body = read_limited(response).await.context("edge metrics over limit")?;
    let next = output.with_extension("next");
    std::fs::write(&next, body)?;
    std::fs::rename(next, output)?;
    Ok(())
}

#[derive(Default)]
struct HostState {
    latest: Option<Observations>,
    last_valid_at: Option<u64>,
    failure: Option<&'static str>,
    /// Identity of the latest distinct document and when it first arrived.
    distinct: Option<(String, u64)>,
    distinct_at: u64,
}

pub struct Tracker {
    started: u64,
    hosts: BTreeMap<String, HostState>,
}

impl Tracker {
    pub fn new(started: u64) -> Self {
        Self {
            started,
            hosts: BTreeMap::new(),
        }
    }

    pub fn record(&mut self, host_id: &str, poll: Poll, now: u64) {
        let state = self.hosts.entry(host_id.to_string()).or_default();
        match poll {
            Poll::Valid(doc) => {
                let identity = (doc.incarnation.clone(), doc.generated_at);
                if state.distinct.as_ref() != Some(&identity) {
                    state.distinct = Some(identity);
                    state.distinct_at = now;
                }
                state.latest = Some(doc);
                state.last_valid_at = Some(now);
                state.failure = None;
            }
            Poll::Failed(reason) => state.failure = Some(reason),
        }
    }

    pub fn conditions(
        &self,
        settings: &Settings,
        enrolled: &BTreeSet<String>,
        existing: &[Incident],
        now: u64,
    ) -> Vec<Condition> {
        let mut out = Vec::new();
        let empty = HostState::default();
        for host_id in enrolled {
            let host = settings.hosts.iter().find(|h| &h.host_id == host_id);
            let state = self.hosts.get(host_id).unwrap_or(&empty);
            let since = state.last_valid_at.unwrap_or(self.started);
            let missing = now.saturating_sub(since) > MAX_AGE_SECONDS;
            let reason = match host {
                None => "enrolled host is missing from the monitoring inventory",
                Some(_) => state.failure.unwrap_or("no observation yet"),
            };
            let current = state
                .latest
                .as_ref()
                .filter(|_| state.failure.is_none() && host.is_some());
            out.push(Condition {
                retired: false,
                key: format!("host:{host_id}:coverage"),
                resource: host_id.clone(),
                severity: "warning".into(),
                firing: if missing {
                    Some(true)
                } else {
                    state.last_valid_at.map(|_| false)
                },
                observed: if missing || current.is_none() {
                    reason.into()
                } else {
                    "valid observation".into()
                },
                threshold: "no valid sidecar observation for 45s".into(),
                hold_seconds: 0,
                sample: if missing { now } else { state.distinct_at },
            });
            let mut names: BTreeSet<&str> = host
                .map(|h| h.checks.iter().map(String::as_str).collect())
                .unwrap_or_default();
            if let Some(doc) = &state.latest {
                names.extend(doc.checks.iter().map(|c| c.name.as_str()));
            }
            for name in names {
                let check = current.and_then(|d| d.checks.iter().find(|c| c.name == name));
                let status = check.map(|c| Observations::status_at(c, now));
                let firing = match status {
                    Some(Status::Healthy) => Some(false),
                    Some(Status::Warning | Status::Critical) => Some(true),
                    Some(Status::Unknown) | None => None,
                };
                out.push(Condition {
                    retired: false,
                    key: format!("host:{host_id}:check:{name}"),
                    resource: host_id.clone(),
                    severity: if status == Some(Status::Critical) {
                        "critical"
                    } else {
                        "warning"
                    }
                    .into(),
                    firing,
                    observed: match (check, status) {
                        (Some(c), Some(Status::Unknown)) if c.status != Status::Unknown => {
                            "observation older than 45s".into()
                        }
                        (Some(c), _) => c.reason.clone(),
                        (None, _) => reason.into(),
                    },
                    threshold: format!("sidecar check {name}"),
                    hold_seconds: 0,
                    sample: check.map(|c| c.observed_at).unwrap_or(0),
                });
            }
        }
        for host_id in &settings.decommissioned {
            let prefix = format!("host:{host_id}:");
            for incident in existing {
                if let Some(c) = incident.condition.as_ref().filter(|c| c.key.starts_with(&prefix))
                {
                    let mut retired = c.clone();
                    retired.retired = true;
                    retired.sample = now;
                    out.push(retired);
                }
            }
        }
        out
    }
}

/// Persisted enrollment: inventory hosts join it; only decommissioning leaves it.
pub fn enrollment(store: &Store, settings: &Settings) -> Result<BTreeSet<String>> {
    let mut enrolled: BTreeSet<String> = store
        .metadata_value(ENROLLMENT_KEY)?
        .map(|v| serde_json::from_str(&v))
        .transpose()?
        .unwrap_or_default();
    enrolled.extend(settings.hosts.iter().map(|h| h.host_id.clone()));
    for gone in &settings.decommissioned {
        enrolled.remove(gone);
    }
    store.metadata(ENROLLMENT_KEY, &serde_json::to_string(&enrolled)?)?;
    Ok(enrolled)
}

pub fn from_env() -> Result<Option<Settings>> {
    std::env::var_os(CONFIG_VAR)
        .map(|path| Settings::load(Path::new(&path)))
        .transpose()
}

/// Poll every enrolled sidecar and evaluate the `host` family until stopped.
pub async fn run(
    settings: Settings,
    state_path: PathBuf,
    environment: String,
    dashboard: String,
) -> Result<()> {
    let mut store = Store::open(&state_path)?.with_family(FAMILY);
    let clients = settings
        .hosts
        .iter()
        .map(|h| Ok((h.clone(), client(&settings, h)?)))
        .collect::<Result<Vec<_>>>()?;
    let mut tracker = Tracker::new(crate::incidents::unix_time());
    let permits = Arc::new(tokio::sync::Semaphore::new(CONCURRENCY));
    let mut interval = tokio::time::interval(Duration::from_secs(POLL_SECONDS));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        let mut set = tokio::task::JoinSet::new();
        for (host, client) in clients.iter().cloned() {
            let permits = permits.clone();
            set.spawn(async move {
                let _permit = permits.acquire_owned().await;
                let result = poll(&client, &host).await;
                if matches!(result, Poll::Valid(_)) && edge(&client, &host).await.is_err() {
                    eprintln!("edge metrics from {} unavailable", host.host_id);
                }
                (host.host_id, result)
            });
        }
        while let Some(joined) = set.join_next().await {
            if let Ok((host_id, result)) = joined {
                tracker.record(&host_id, result, crate::incidents::unix_time());
            }
        }
        let now = crate::incidents::unix_time();
        let enrolled = enrollment(&store, &settings)?;
        let conditions = tracker.conditions(&settings, &enrolled, &store.incidents()?, now);
        store.evaluate(&conditions, now, settings.shadow(), &environment, &dashboard)?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pir_host_observations::{Check, SCHEMA_VERSION};

    fn settings(hosts: &[&str], decommissioned: &[&str]) -> Settings {
        Settings {
            mode: "active".into(),
            collector: Collector {
                cert: "c".into(),
                key: "k".into(),
                ca: "a".into(),
            },
            hosts: hosts
                .iter()
                .map(|id| Host {
                    host_id: id.to_string(),
                    address: "127.0.0.1:9443".parse().unwrap(),
                    tls_name: format!("{id}.pir-monitoring"),
                    config_digest: format!("sha256:{}", "0".repeat(64)),
                    checks: vec!["disk:/".into()],
                    edge_output: None,
                })
                .collect(),
            decommissioned: decommissioned.iter().map(|s| s.to_string()).collect(),
        }
    }

    fn doc(host: &str, at: u64, status: Status) -> Poll {
        Poll::Valid(Observations {
            schema_version: SCHEMA_VERSION,
            host_id: host.into(),
            incarnation: "i1".into(),
            config_digest: format!("sha256:{}", "0".repeat(64)),
            generated_at: at,
            checks: vec![Check {
                name: "disk:/".into(),
                status,
                reason: "12.0% available (warn below 15%)".into(),
                observed_at: at,
                values: vec![],
            }],
        })
    }

    struct Harness {
        _dir: tempfile::TempDir,
        path: PathBuf,
        store: Store,
        tracker: Tracker,
    }

    impl Harness {
        fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("state.db");
            Self {
                store: Store::open(&path).unwrap().with_family(FAMILY),
                path,
                _dir: dir,
                tracker: Tracker::new(1000),
            }
        }

        fn tick(&mut self, s: &Settings, now: u64) {
            let enrolled = enrollment(&self.store, s).unwrap();
            let existing = self.store.incidents().unwrap();
            let c = self.tracker.conditions(s, &enrolled, &existing, now);
            self.store.evaluate(&c, now, s.shadow(), "test", "").unwrap();
        }

        fn active(&self, key: &str) -> bool {
            self.store
                .incidents()
                .unwrap()
                .iter()
                .any(|i| i.active && i.condition.as_ref().is_some_and(|c| c.key == key))
        }

        fn outbox(&self) -> Vec<String> {
            let db = rusqlite::Connection::open(&self.path).unwrap();
            let mut stmt = db.prepare("SELECT body FROM outbox ORDER BY id").unwrap();
            stmt.query_map([], |r| r.get(0))
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap()
        }
    }

    const DISK: &str = "host:w1:check:disk:/";
    const COVERAGE: &str = "host:w1:coverage";

    #[test]
    fn warning_propagates_and_two_distinct_healthy_samples_recover() {
        let s = settings(&["w1"], &[]);
        let mut h = Harness::new();
        h.tracker.record("w1", doc("w1", 1000, Status::Warning), 1000);
        h.tick(&s, 1000);
        assert!(h.active(DISK));
        assert!(h.outbox()[0].contains("12.0% available"));
        h.tracker.record("w1", doc("w1", 1015, Status::Healthy), 1015);
        h.tick(&s, 1015);
        // The same document again, as a poll between collections returns it.
        h.tracker.record("w1", doc("w1", 1015, Status::Healthy), 1020);
        h.tick(&s, 1020);
        assert!(h.active(DISK), "a repeated snapshot is not a second sample");
        // Loss of the host between samples never recovers.
        h.tracker.record("w1", Poll::Failed("sidecar unreachable or authentication failed"), 1025);
        h.tick(&s, 1025);
        assert!(h.active(DISK));
        h.tracker.record("w1", doc("w1", 1030, Status::Healthy), 1030);
        h.tick(&s, 1030);
        h.tracker.record("w1", doc("w1", 1045, Status::Healthy), 1045);
        h.tick(&s, 1045);
        assert!(!h.active(DISK));
        assert!(h.outbox().last().unwrap().contains("RECOVERED"));
        assert!(!h.active(COVERAGE));
    }

    #[test]
    fn rejected_and_stale_observations_are_unknown_and_coverage_warns_after_45s() {
        let s = settings(&["w1"], &[]);
        let mut h = Harness::new();
        h.tracker.record("w1", doc("w1", 1000, Status::Warning), 1000);
        h.tick(&s, 1000);
        for reason in [
            pir_host_observations::Rejection::ConfigMismatch.reason(),
            pir_host_observations::Rejection::Schema.reason(),
            "sidecar unreachable or authentication failed",
        ] {
            h.tracker.record("w1", Poll::Failed(reason), 1040);
            h.tick(&s, 1040);
            assert!(h.active(DISK), "unknown never recovers");
            assert!(!h.active(COVERAGE), "{reason}: still within 45s");
        }
        h.tick(&s, 1045);
        assert!(!h.active(COVERAGE));
        h.tick(&s, 1046);
        assert!(h.active(COVERAGE), "no second dwell after 45s");
        assert!(h.outbox().last().unwrap().contains("authentication failed"));
        // A valid but stale document is unknown for its checks.
        let s2 = settings(&["w1"], &[]);
        h.tracker.record("w1", doc("w1", 1000, Status::Healthy), 1050);
        let c = h
            .tracker
            .conditions(&s2, &enrollment(&h.store, &s2).unwrap(), &[], 1050);
        let disk = c.iter().find(|c| c.key == DISK).unwrap();
        assert_eq!(disk.firing, None);
        assert_eq!(disk.observed, "observation older than 45s");
    }

    #[test]
    fn missing_initial_hosts_alert_and_enrollment_outlives_the_inventory() {
        let mut h = Harness::new();
        let s = settings(&["w1", "w2"], &[]);
        h.tracker.record("w2", doc("w2", 1000, Status::Healthy), 1000);
        h.tick(&s, 1040);
        assert!(!h.active(COVERAGE));
        h.tracker.record("w2", doc("w2", 1045, Status::Healthy), 1045);
        h.tick(&s, 1046);
        assert!(h.active(COVERAGE), "a never-observed host is detected");
        assert!(!h.active("host:w2:coverage"));
        // w1 leaves the roster without decommissioning: still expected.
        let shrunk = settings(&["w2"], &[]);
        h.tracker.record("w2", doc("w2", 1050, Status::Healthy), 1050);
        h.tick(&shrunk, 1050);
        assert!(h.active(COVERAGE));
        assert!(h.outbox().last().unwrap().contains("coverage"));
        // Explicit decommission retires its incidents and enrollment.
        let gone = settings(&["w2"], &["w1"]);
        h.tick(&gone, 1055);
        assert!(!h.active("host:w2:coverage"));
        assert!(!h.active(COVERAGE));
        assert!(h.outbox().last().unwrap().contains("RETIRED"));
        assert_eq!(
            enrollment(&h.store, &shrunk).unwrap(),
            BTreeSet::from(["w2".to_string()])
        );
    }

    #[test]
    fn incidents_and_pending_delivery_survive_an_apm_restart() {
        let s = settings(&["w1"], &[]);
        let mut h = Harness::new();
        h.tracker.record("w1", doc("w1", 1000, Status::Critical), 1000);
        h.tick(&s, 1000);
        let pending = h.outbox().len();
        // Restart: a new tracker and store on the same database.
        h.store = Store::open(&h.path).unwrap().with_family(FAMILY);
        h.tracker = Tracker::new(1010);
        h.tick(&s, 1010);
        assert!(h.active(DISK), "unknown after restart keeps the incident");
        assert_eq!(h.outbox().len(), pending, "no duplicate FIRED");
        h.tracker.record("w1", doc("w1", 1015, Status::Critical), 1015);
        h.tick(&s, 1015);
        assert_eq!(h.outbox().len(), pending);
        assert!(h.outbox()[0].contains("critical"));
    }
}
