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
use pir_host_observations::{
    Observations, Status, MAX_AGE_SECONDS, MAX_BODY_BYTES, MAX_FUTURE_SECONDS, PATH,
};
use serde::Deserialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
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
    /// Dynamic Transparent workers: each enrolled or draining roster member is
    /// an expected host, enrolled with the `monitoring` record its bootstrap wrote.
    #[serde(default)]
    pub roster: Option<PathBuf>,
    /// Host resources in the former sampler's `hosts.json` shape, for the
    /// Transparent capacity views (`host_snapshot` in their config).
    #[serde(default)]
    pub host_snapshot: Option<PathBuf>,
    /// How roster members appear in `host_snapshot`, as the former sampler's
    /// `roster` block did: under their own id, with the unit of their role.
    #[serde(default)]
    pub roster_snapshot: Option<RosterSnapshot>,
    /// Content digest the renderer stamps; readiness reports it so an install
    /// proves this process loaded exactly the installed inventory.
    #[serde(default)]
    pub revision: Option<String>,
}

/// What `GET /observers/ready` reports. Ready means collection works: the
/// latest poll round, finished within the last 45 seconds, holds a fresh,
/// authenticated document from every expected host. A host reporting warning
/// or critical checks still counts; an unreachable, rejected or frozen host
/// does not, and a collector that stops completing rounds stops being ready.
#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct Readiness {
    pub configured: bool,
    pub ready: bool,
    pub revision: Option<String>,
    pub rounds: u64,
    pub expected_hosts: usize,
    pub valid_hosts: usize,
    /// Unix time the latest poll round finished.
    pub evaluated_at: u64,
    /// Earliest expiry among the documents accepted in the completed round.
    pub valid_until: u64,
}

static READINESS: std::sync::Mutex<Option<(Readiness, Instant)>> = std::sync::Mutex::new(None);

pub fn readiness() -> Readiness {
    let r = READINESS
        .lock()
        .map(|r| match r.as_ref() {
            Some((report, at)) if at.elapsed().as_secs() <= MAX_AGE_SECONDS => report.clone(),
            Some((report, _)) => Readiness {
                ready: false,
                ..report.clone()
            },
            None => Readiness::default(),
        })
        .unwrap_or_default();
    current_readiness(r, crate::incidents::unix_time())
}

/// `r` as seen at `now`: a stored readiness expires 45 seconds after its round.
pub fn current_readiness(mut r: Readiness, now: u64) -> Readiness {
    r.ready = r.ready && fresh(r.evaluated_at, now) && now <= r.valid_until;
    r
}

/// Readiness after a poll round that finished at `now`.
pub fn round_readiness(
    settings: &Settings,
    tracker: &Tracker,
    expected: &Expected,
    rounds: u64,
    now: u64,
) -> Readiness {
    let valid_hosts = expected
        .hosts
        .iter()
        .filter(|h| tracker.collecting(&h.host_id, now))
        .count();
    Readiness {
        configured: true,
        ready: !expected.hosts.is_empty()
            && expected.unenrolled.is_empty()
            && valid_hosts == expected.hosts.len(),
        revision: settings.revision.clone(),
        rounds,
        expected_hosts: expected.hosts.len(),
        valid_hosts,
        evaluated_at: now,
        valid_until: expected
            .hosts
            .iter()
            .filter_map(|h| tracker.current(&h.host_id))
            .map(|d| d.generated_at.saturating_add(MAX_AGE_SECONDS))
            .min()
            .unwrap_or(now),
    }
}

pub fn set_readiness(r: Readiness) {
    if let Ok(mut slot) = READINESS.lock() {
        *slot = Some((r, Instant::now()));
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RosterSnapshot {
    pub unit: String,
    #[serde(default)]
    pub role_units: BTreeMap<String, String>,
    #[serde(default = "root_path")]
    pub data_path: String,
    #[serde(default)]
    pub size: Option<String>,
}

impl RosterSnapshot {
    fn for_member(&self, id: &str, role: Option<&str>) -> Snapshot {
        Snapshot {
            sources: vec![id.to_string()],
            unit: role
                .and_then(|r| self.role_units.get(r))
                .unwrap_or(&self.unit)
                .clone(),
            data_path: self.data_path.clone(),
            size: self.size.clone(),
        }
    }
}

fn valid_unit(unit: &str) -> bool {
    (1..=128).contains(&unit.len())
        && unit
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'@'))
}

fn valid_snapshot(unit: &str, size: Option<&String>, data_path: &str) -> bool {
    valid_unit(unit)
        && data_path.starts_with('/')
        && pir_host_observations::valid_name(data_path, 128)
        && size.is_none_or(|s| {
            (1..=40).contains(&s.len())
                && s.bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        })
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
    /// Names and main unit under which this host appears in `host_snapshot`.
    #[serde(default)]
    pub snapshot: Option<Snapshot>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    pub sources: Vec<String>,
    pub unit: String,
    /// The filesystem reported as the host's disk (a `disk:` or `volume:` check path).
    #[serde(default = "root_path")]
    pub data_path: String,
    #[serde(default)]
    pub size: Option<String>,
}

fn root_path() -> String {
    "/".into()
}

/// A roster member's monitoring enrollment, written when its sidecar is installed.
#[derive(Clone, Debug, Deserialize)]
struct RosterMember {
    id: String,
    #[serde(default)]
    intent: Option<String>,
    #[serde(default)]
    role: Option<String>,
    #[serde(default)]
    monitoring: Option<RosterMonitoring>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RosterMonitoring {
    address: SocketAddr,
    config_digest: String,
    checks: Vec<String>,
}

/// Hosts the roster expects: enrolled members with their endpoints, and ids of
/// members whose sidecar enrollment is missing. An unreadable roster is an error.
/// `snapshot`, when set, maps each enrolled member into the host snapshot.
pub fn roster_hosts(path: &Path, snapshot: Option<&RosterSnapshot>) -> Result<Roster> {
    let members: Vec<RosterMember> =
        serde_json::from_slice(&std::fs::read(path)?).context("parsing roster")?;
    let mut roster = Roster::default();
    for m in members {
        if !pir_host_observations::valid_name(&m.id, 80) {
            continue;
        }
        match m.intent.as_deref().unwrap_or("enrolled") {
            "enrolled" | "draining" => {}
            // The actuator's deliberate retirement is an explicit decommission.
            "retired" => {
                roster.retired.push(m.id);
                continue;
            }
            _ => continue,
        }
        match m.monitoring {
            Some(record) => roster.hosts.push(Host {
                tls_name: format!("{}.pir-monitoring", m.id),
                snapshot: snapshot.map(|s| s.for_member(&m.id, m.role.as_deref())),
                host_id: m.id,
                address: record.address,
                config_digest: record.config_digest,
                checks: record.checks,
                edge_output: None,
            }),
            None => roster.unenrolled.push(m.id),
        }
    }
    Ok(roster)
}

#[derive(Debug, Default)]
pub struct Roster {
    pub hosts: Vec<Host>,
    /// Expected members whose bootstrap recorded no sidecar enrollment.
    pub unenrolled: Vec<String>,
    pub retired: Vec<String>,
}

/// Every host expected this round: the inventory's and the roster's.
pub struct Expected {
    pub hosts: Vec<Host>,
    pub unenrolled: Vec<String>,
    pub decommissioned: Vec<String>,
}

impl Expected {
    pub fn of(settings: &Settings, roster: Option<Roster>) -> Self {
        let mut expected = Self {
            hosts: settings.hosts.clone(),
            unenrolled: Vec::new(),
            decommissioned: settings.decommissioned.clone(),
        };
        if let Some(roster) = roster {
            let known: BTreeSet<String> =
                expected.hosts.iter().map(|h| h.host_id.clone()).collect();
            expected.hosts.extend(
                roster
                    .hosts
                    .into_iter()
                    .filter(|h| !known.contains(&h.host_id)),
            );
            expected.unenrolled = roster.unenrolled;
            expected.decommissioned.extend(roster.retired);
        }
        expected
    }
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
        let settings: Self =
            serde_json::from_slice(&std::fs::read(path)?).context("parsing observer inventory")?;
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
            if let Some(s) = &h.snapshot {
                anyhow::ensure!(
                    !s.sources.is_empty()
                        && s.sources.iter().all(|n| valid_unit(n))
                        && valid_snapshot(&s.unit, s.size.as_ref(), &s.data_path),
                    "invalid host snapshot mapping"
                );
            }
        }
        if let Some(s) = &settings.roster_snapshot {
            anyhow::ensure!(
                s.role_units.values().all(|u| valid_unit(u))
                    && valid_snapshot(&s.unit, s.size.as_ref(), &s.data_path),
                "invalid roster snapshot mapping"
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
        Ok(doc)
            if host
                .checks
                .iter()
                .any(|c| !doc.checks.iter().any(|d| &d.name == c)) =>
        {
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
    let body = read_limited(response)
        .await
        .context("edge metrics over limit")?;
    let next = output.with_extension("next");
    std::fs::write(&next, body)?;
    std::fs::rename(next, output)?;
    Ok(())
}

#[derive(Default)]
struct HostState {
    latest: Option<Observations>,
    failure: Option<&'static str>,
    /// The latest fresh document's identity, its `generated_at`, and when the
    /// APM first saw it. Only a new, fresh document renews coverage: a cached
    /// or frozen document served again does not.
    progress: Option<((String, u64), u64)>,
    /// Per check: the newest `observed_at` with a fresh known status, and when
    /// the APM first saw it.
    known: BTreeMap<String, (u64, u64)>,
}

pub struct Tracker {
    started: u64,
    hosts: BTreeMap<String, HostState>,
}

fn fresh(at: u64, now: u64) -> bool {
    at <= now + MAX_FUTURE_SECONDS && now.saturating_sub(at) <= MAX_AGE_SECONDS
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
                let seen = state.progress.as_ref().map(|(i, _)| i);
                if fresh(doc.generated_at, now) && seen != Some(&identity) {
                    state.progress = Some((identity, now));
                }
                for c in &doc.checks {
                    let newer = state
                        .known
                        .get(&c.name)
                        .is_none_or(|(at, _)| c.observed_at > *at);
                    if newer && Observations::status_at(c, now) != Status::Unknown {
                        state.known.insert(c.name.clone(), (c.observed_at, now));
                    }
                }
                state.latest = Some(doc);
                state.failure = None;
            }
            Poll::Failed(reason) => state.failure = Some(reason),
        }
    }

    pub fn conditions(
        &self,
        expected: &Expected,
        enrolled: &BTreeSet<String>,
        existing: &[Incident],
        now: u64,
    ) -> Vec<Condition> {
        let mut out = Vec::new();
        let empty = HostState::default();
        for host_id in enrolled {
            let host = expected.hosts.iter().find(|h| &h.host_id == host_id);
            let state = self.hosts.get(host_id).unwrap_or(&empty);
            let progress = state.progress.as_ref().map(|((_, at), seen)| (*at, *seen));
            let since = progress.map_or(self.started, |(at, _)| at);
            let missing = now.saturating_sub(since) > MAX_AGE_SECONDS;
            let reason = match host {
                None if expected.unenrolled.contains(host_id) => {
                    "roster member has no sidecar enrollment"
                }
                None => "enrolled host is missing from the monitoring inventory",
                Some(_) if state.failure.is_none() && state.latest.is_some() => {
                    "sidecar serves no fresh observation"
                }
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
                    progress.map(|_| false)
                },
                observed: if missing || current.is_none() {
                    reason.into()
                } else {
                    "valid observation".into()
                },
                threshold: "no fresh sidecar observation for 45s".into(),
                hold_seconds: 0,
                sample: if missing {
                    now
                } else {
                    progress.map_or(0, |(_, seen)| seen)
                },
            });
            // Each configured check is covered on its own, so healthy checks
            // never mask one that stays unknown. While the whole host is
            // missing its coverage incident alone reports it.
            for name in host.iter().flat_map(|h| &h.checks) {
                let known = state.known.get(name).copied();
                let since = known.map_or(self.started, |(at, _)| at);
                let stale = now.saturating_sub(since) > MAX_AGE_SECONDS;
                out.push(Condition {
                    retired: false,
                    key: format!("host:{host_id}:coverage:{name}"),
                    resource: host_id.clone(),
                    severity: "warning".into(),
                    firing: match (missing, stale) {
                        (true, _) => None,
                        (false, true) => Some(true),
                        (false, false) => known.map(|_| false),
                    },
                    observed: if stale {
                        format!("no fresh known status for check {name}")
                    } else {
                        "fresh known status".into()
                    },
                    threshold: format!("check {name} unknown or stale for 45s"),
                    hold_seconds: 0,
                    sample: if stale {
                        now
                    } else {
                        known.map_or(0, |(_, seen)| seen)
                    },
                });
            }
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
        for host_id in &expected.decommissioned {
            let prefix = format!("host:{host_id}:");
            for incident in existing {
                if let Some(c) = incident
                    .condition
                    .as_ref()
                    .filter(|c| c.key.starts_with(&prefix))
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
pub fn enrollment(store: &Store, expected: &Expected) -> Result<BTreeSet<String>> {
    let mut enrolled: BTreeSet<String> = store
        .metadata_value(ENROLLMENT_KEY)?
        .map(|v| serde_json::from_str(&v))
        .transpose()?
        .unwrap_or_default();
    enrolled.extend(expected.hosts.iter().map(|h| h.host_id.clone()));
    enrolled.extend(expected.unenrolled.iter().cloned());
    for gone in &expected.decommissioned {
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

/// One host's entry in the former sampler's `hosts.json` shape. Properties a
/// check does not report, or reports unknown, stale or from the future as of
/// `now`, are left out, never filled in.
pub fn snapshot_entry(
    host: &Host,
    snapshot: &Snapshot,
    doc: Option<&Observations>,
    now: u64,
) -> serde_json::Value {
    let mut entry = serde_json::Map::new();
    entry.insert("host_id".into(), host.host_id.clone().into());
    if let Some(size) = &snapshot.size {
        entry.insert("size".into(), size.clone().into());
    }
    let Some(doc) = doc else {
        entry.insert("at".into(), 0.into());
        return entry.into();
    };
    let value = |check: &str, name: &str| {
        doc.checks
            .iter()
            .find(|c| c.name == check && Observations::status_at(c, now) != Status::Unknown)
            .and_then(|c| c.values.iter().find(|v| v.name == name))
            .map(|v| serde_json::json!(v.value as u64))
    };
    let mut put = |key: &str, v: Option<serde_json::Value>| {
        if let Some(v) = v {
            entry.insert(key.into(), v);
        }
    };
    put("at", Some(doc.generated_at.into()));
    put("unit", Some(snapshot.unit.clone().into()));
    put("memory_available_bytes", value("memory", "available_bytes"));
    put("memory_total_bytes", value("memory", "total_bytes"));
    put("swap_total_bytes", value("swap", "total_bytes"));
    put("swap_free_bytes", value("swap", "free_bytes"));
    for disk in [
        format!("volume:{}", snapshot.data_path),
        format!("disk:{}", snapshot.data_path),
    ] {
        if let (Some(a), Some(t)) = (
            value(&disk, "available_bytes"),
            value(&disk, "usable_bytes"),
        ) {
            put("disk_available_bytes", Some(a));
            put("disk_total_bytes", Some(t));
            break;
        }
    }
    let main = snapshot.unit.to_lowercase();
    put(
        "restarts",
        value(&format!("unit:{main}:restarts"), "restarts"),
    );
    put("oom_kills", value(&format!("unit:{main}:oom"), "oom_kills"));
    let mut units = serde_json::Map::new();
    for check in &doc.checks {
        let Some(unit) = check
            .name
            .strip_prefix("unit:")
            .and_then(|n| n.strip_suffix(":active"))
        else {
            continue;
        };
        if Observations::status_at(check, now) == Status::Unknown {
            continue;
        }
        let mut u = serde_json::Map::new();
        for v in &check.values {
            u.insert(v.name.clone(), serde_json::json!(v.value as u64));
        }
        if !u.contains_key("memory_max_bytes") && u.contains_key("memory_current_bytes") {
            u.insert("memory_max_bytes".into(), serde_json::Value::Null);
        }
        if let Some(oom) = value(&format!("unit:{unit}:oom"), "oom_kills") {
            u.insert("oom_kills".into(), oom);
        }
        units.insert(unit.to_string(), u.into());
    }
    entry.insert("units".into(), units.into());
    entry.into()
}

impl Tracker {
    /// The valid, current document of a host, if any.
    pub fn current(&self, host_id: &str) -> Option<&Observations> {
        self.hosts
            .get(host_id)
            .filter(|s| s.failure.is_none())
            .and_then(|s| s.latest.as_ref())
    }

    /// Whether the latest poll of a host authenticated and returned a valid
    /// document that is new and fresh: a frozen or replayed document stops
    /// counting once 45 seconds pass after it was generated.
    pub fn collecting(&self, host_id: &str, now: u64) -> bool {
        self.hosts.get(host_id).is_some_and(|s| {
            s.failure.is_none()
                && s.latest
                    .as_ref()
                    .is_some_and(|d| fresh(d.generated_at, now))
                && s.progress
                    .as_ref()
                    .is_some_and(|((_, at), _)| fresh(*at, now))
        })
    }
}

fn write_snapshot(path: &Path, tracker: &Tracker, hosts: &[Host], now: u64) -> Result<()> {
    let mut out = serde_json::Map::new();
    for host in hosts {
        let Some(snapshot) = &host.snapshot else {
            continue;
        };
        let doc = tracker
            .current(&host.host_id)
            .filter(|d| fresh(d.generated_at, now));
        let entry = snapshot_entry(host, snapshot, doc, now);
        for source in &snapshot.sources {
            out.insert(source.clone(), entry.clone());
        }
    }
    let next = path.with_extension("next");
    std::fs::write(&next, serde_json::to_vec(&out)?)?;
    std::fs::rename(next, path)?;
    Ok(())
}

/// Poll every enrolled sidecar and evaluate the `host` family until stopped.
pub async fn run(
    settings: Settings,
    state_path: PathBuf,
    environment: String,
    dashboard: String,
) -> Result<()> {
    let mut store = Store::open(&state_path)?.with_family(FAMILY);
    let mut clients: BTreeMap<(String, SocketAddr), reqwest::Client> = BTreeMap::new();
    let mut tracker = Tracker::new(crate::incidents::unix_time());
    let permits = Arc::new(tokio::sync::Semaphore::new(CONCURRENCY));
    let mut interval = tokio::time::interval(Duration::from_secs(POLL_SECONDS));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut rounds = 0u64;
    set_readiness(Readiness {
        configured: true,
        revision: settings.revision.clone(),
        ..Readiness::default()
    });
    loop {
        interval.tick().await;
        // An unreadable roster keeps the inventory's hosts and enrollment.
        let roster = match &settings.roster {
            Some(path) => match roster_hosts(path, settings.roster_snapshot.as_ref()) {
                Ok(roster) => Some(roster),
                Err(_) => {
                    eprintln!("observer roster unavailable");
                    None
                }
            },
            None => None,
        };
        let roster_readable = settings.roster.is_none() || roster.is_some();
        let expected = Expected::of(&settings, roster);
        let mut set = tokio::task::JoinSet::new();
        for host in expected.hosts.iter().cloned() {
            let key = (host.tls_name.clone(), host.address);
            let client = match clients.get(&key) {
                Some(c) => c.clone(),
                None => {
                    let c = client(&settings, &host)?;
                    clients.insert(key, c.clone());
                    c
                }
            };
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
        if let Some(path) = &settings.host_snapshot {
            if write_snapshot(path, &tracker, &expected.hosts, now).is_err() {
                eprintln!("host snapshot could not be written");
            }
        }
        let enrolled = enrollment(&store, &expected)?;
        let conditions = tracker.conditions(&expected, &enrolled, &store.incidents()?, now);
        store.evaluate(
            &conditions,
            now,
            settings.shadow(),
            &environment,
            &dashboard,
        )?;
        rounds += 1;
        let mut report = round_readiness(&settings, &tracker, &expected, rounds, now);
        // Persisted enrollment cannot disappear from readiness merely because a
        // roster or inventory omitted the host. Explicit retirement is required.
        let absent = enrolled
            .iter()
            .filter(|id| {
                !expected.decommissioned.contains(id)
                    && !expected.hosts.iter().any(|h| &h.host_id == *id)
            })
            .count();
        report.expected_hosts += absent;
        report.ready &= roster_readable && absent == 0;
        set_readiness(report);
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
                    snapshot: None,
                })
                .collect(),
            decommissioned: decommissioned.iter().map(|s| s.to_string()).collect(),
            roster: None,
            host_snapshot: None,
            roster_snapshot: None,
            revision: None,
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
            let expected = Expected::of(s, None);
            let enrolled = enrollment(&self.store, &expected).unwrap();
            let existing = self.store.incidents().unwrap();
            let c = self
                .tracker
                .conditions(&expected, &enrolled, &existing, now);
            self.store
                .evaluate(&c, now, s.shadow(), "test", "")
                .unwrap();
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
        h.tracker
            .record("w1", doc("w1", 1000, Status::Warning), 1000);
        h.tick(&s, 1000);
        assert!(h.active(DISK));
        assert!(h.outbox()[0].contains("12.0% available"));
        h.tracker
            .record("w1", doc("w1", 1015, Status::Healthy), 1015);
        h.tick(&s, 1015);
        // The same document again, as a poll between collections returns it.
        h.tracker
            .record("w1", doc("w1", 1015, Status::Healthy), 1020);
        h.tick(&s, 1020);
        assert!(h.active(DISK), "a repeated snapshot is not a second sample");
        // Loss of the host between samples never recovers.
        h.tracker.record(
            "w1",
            Poll::Failed("sidecar unreachable or authentication failed"),
            1025,
        );
        h.tick(&s, 1025);
        assert!(h.active(DISK));
        h.tracker
            .record("w1", doc("w1", 1030, Status::Healthy), 1030);
        h.tick(&s, 1030);
        h.tracker
            .record("w1", doc("w1", 1045, Status::Healthy), 1045);
        h.tick(&s, 1045);
        assert!(!h.active(DISK));
        assert!(h.outbox().last().unwrap().contains("RECOVERED"));
        assert!(!h.active(COVERAGE));
    }

    #[test]
    fn readiness_counts_alerting_hosts_but_not_failed_stale_or_missing_ones() {
        let s = settings(&["w1", "w2"], &["w3"]);
        let expected = Expected::of(&s, None);
        let mut t = Tracker::new(1000);
        t.record("w1", doc("w1", 1000, Status::Critical), 1000);
        let r = round_readiness(&s, &t, &expected, 1, 1000);
        assert_eq!((r.ready, r.valid_hosts, r.expected_hosts), (false, 1, 2));
        t.record("w2", doc("w2", 1000, Status::Warning), 1000);
        assert!(round_readiness(&s, &t, &expected, 1, 1000).ready);
        t.record(
            "w2",
            Poll::Failed("sidecar unreachable or authentication failed"),
            1005,
        );
        assert!(!round_readiness(&s, &t, &expected, 2, 1005).ready);
        t.record("w2", doc("w2", 1005, Status::Healthy), 1010);
        assert!(round_readiness(&s, &t, &expected, 3, 1010).ready);
        let r = round_readiness(&s, &t, &expected, 3, 1045);
        assert!(r.ready);
        assert!(
            !current_readiness(r, 1046).ready,
            "the oldest host expires before the next round"
        );
        t.record("w1", doc("w1", 2000, Status::Healthy), 1010);
        assert!(
            !round_readiness(&s, &t, &expected, 3, 1010).ready,
            "a future document cannot reuse an earlier fresh observation"
        );
        // w1 keeps serving its first document: it stops counting after 45s.
        t.record("w1", doc("w1", 1000, Status::Critical), 1046);
        t.record("w2", doc("w2", 1046, Status::Healthy), 1046);
        assert!(!round_readiness(&s, &t, &expected, 4, 1046).ready);
        let empty = settings(&[], &[]);
        let none = Expected::of(&empty, None);
        assert!(!round_readiness(&empty, &t, &none, 1, 1046).ready);
        t.record("w1", doc("w1", 1046, Status::Critical), 1046);
        let mut missing = Expected::of(&s, None);
        missing.unenrolled.push("w4".to_string());
        assert!(!round_readiness(&s, &t, &missing, 5, 1046).ready);
    }

    #[test]
    fn rejected_and_stale_observations_are_unknown_and_coverage_warns_after_45s() {
        let s = settings(&["w1"], &[]);
        let mut h = Harness::new();
        h.tracker
            .record("w1", doc("w1", 1000, Status::Warning), 1000);
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
        h.tracker
            .record("w1", doc("w1", 1000, Status::Healthy), 1050);
        let e2 = Expected::of(&s2, None);
        let c = h
            .tracker
            .conditions(&e2, &enrollment(&h.store, &e2).unwrap(), &[], 1050);
        let disk = c.iter().find(|c| c.key == DISK).unwrap();
        assert_eq!(disk.firing, None);
        assert_eq!(disk.observed, "observation older than 45s");
    }

    #[test]
    fn missing_initial_hosts_alert_and_enrollment_outlives_the_inventory() {
        let mut h = Harness::new();
        let s = settings(&["w1", "w2"], &[]);
        h.tracker
            .record("w2", doc("w2", 1000, Status::Healthy), 1000);
        h.tick(&s, 1040);
        assert!(!h.active(COVERAGE));
        h.tracker
            .record("w2", doc("w2", 1045, Status::Healthy), 1045);
        h.tick(&s, 1046);
        assert!(h.active(COVERAGE), "a never-observed host is detected");
        assert!(!h.active("host:w2:coverage"));
        // w1 leaves the roster without decommissioning: still expected.
        let shrunk = settings(&["w2"], &[]);
        h.tracker
            .record("w2", doc("w2", 1050, Status::Healthy), 1050);
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
            enrollment(&h.store, &Expected::of(&shrunk, None)).unwrap(),
            BTreeSet::from(["w2".to_string()])
        );
    }

    #[test]
    fn incidents_and_pending_delivery_survive_an_apm_restart() {
        let s = settings(&["w1"], &[]);
        let mut h = Harness::new();
        h.tracker
            .record("w1", doc("w1", 1000, Status::Critical), 1000);
        h.tick(&s, 1000);
        let pending = h.outbox().len();
        // Restart: a new tracker and store on the same database.
        h.store = Store::open(&h.path).unwrap().with_family(FAMILY);
        h.tracker = Tracker::new(1010);
        h.tick(&s, 1010);
        assert!(h.active(DISK), "unknown after restart keeps the incident");
        assert_eq!(h.outbox().len(), pending, "no duplicate FIRED");
        h.tracker
            .record("w1", doc("w1", 1015, Status::Critical), 1015);
        h.tick(&s, 1015);
        assert_eq!(h.outbox().len(), pending);
        assert!(h.outbox()[0].contains("critical"));
    }

    #[test]
    fn roster_members_enroll_dynamically_and_retirement_decommissions() {
        let dir = tempfile::tempdir().unwrap();
        let roster = dir.path().join("roster.json");
        let digest = format!("sha256:{}", "0".repeat(64));
        std::fs::write(
            &roster,
            serde_json::json!([
                {"id": "w1", "intent": "enrolled", "monitoring": {"address": "10.0.0.9:9443",
                    "config_digest": digest, "checks": ["disk:/"]}},
                {"id": "w3", "intent": "draining"},
                {"id": "w4", "intent": "retired"}
            ])
            .to_string(),
        )
        .unwrap();
        let s = settings(&[], &[]);
        let expected = Expected::of(&s, Some(roster_hosts(&roster, None).unwrap()));
        assert_eq!(expected.hosts[0].tls_name, "w1.pir-monitoring");
        assert_eq!(expected.unenrolled, ["w3"]);
        assert_eq!(expected.decommissioned, ["w4"]);
        let mut h = Harness::new();
        let enrolled = enrollment(&h.store, &expected).unwrap();
        assert_eq!(enrolled, BTreeSet::from(["w1".into(), "w3".into()]));
        let c = h.tracker.conditions(&expected, &enrolled, &[], 1046);
        let w3 = c.iter().find(|c| c.key == "host:w3:coverage").unwrap();
        assert_eq!(w3.firing, Some(true));
        assert_eq!(w3.observed, "roster member has no sidecar enrollment");
        h.tracker
            .record("w1", doc("w1", 1046, Status::Healthy), 1046);
        assert!(roster_hosts(&dir.path().join("absent.json"), None).is_err());
    }

    #[test]
    fn snapshot_entries_keep_the_sampler_shape_and_omit_unreported_values() {
        let host = settings(&["w1"], &[]).hosts.remove(0);
        let snapshot = Snapshot {
            sources: vec!["transparent-publisher".into()],
            unit: "pir-publisher.service".into(),
            data_path: "/".into(),
            size: Some("m-8vcpu-64gb".into()),
        };
        let Poll::Valid(mut d) = doc("w1", 1000, Status::Healthy) else {
            unreachable!()
        };
        let v = |name: &str, value: f64| pir_host_observations::Value {
            name: name.into(),
            value,
            unit: pir_host_observations::Unit::Bytes,
        };
        d.checks[0].values = vec![v("available_bytes", 10.), v("usable_bytes", 40.)];
        d.checks.push(Check {
            name: "unit:pir-publisher.service:active".into(),
            status: Status::Healthy,
            reason: "active".into(),
            observed_at: 1000,
            values: vec![v("memory_current_bytes", 5.)],
        });
        d.checks.push(Check {
            name: "unit:pir-publisher.service:oom".into(),
            status: Status::Healthy,
            reason: "none".into(),
            observed_at: 1000,
            values: vec![v("oom_kills", 2.)],
        });
        let entry = snapshot_entry(&host, &snapshot, Some(&d), 1000);
        assert_eq!(entry["at"], 1000);
        assert_eq!(entry["disk_available_bytes"], 10);
        assert_eq!(entry["disk_total_bytes"], 40);
        assert_eq!(entry["oom_kills"], 2);
        assert_eq!(entry["size"], "m-8vcpu-64gb");
        assert!(entry.get("memory_available_bytes").is_none());
        let unit = &entry["units"]["pir-publisher.service"];
        assert_eq!(unit["memory_current_bytes"], 5);
        assert!(unit["memory_max_bytes"].is_null());
        assert_eq!(unit["oom_kills"], 2);
        let gone = snapshot_entry(&host, &snapshot, None, 1000);
        assert_eq!(
            (gone["at"].as_u64(), gone["host_id"].as_str()),
            (Some(0), Some("w1"))
        );
    }

    fn two_checks(at: u64, other_at: u64, other: Status) -> Poll {
        let Poll::Valid(mut d) = doc("w1", at, Status::Healthy) else {
            unreachable!()
        };
        d.checks.push(Check {
            name: "memory".into(),
            status: other,
            reason: "hook failed".into(),
            observed_at: other_at,
            values: vec![],
        });
        Poll::Valid(d)
    }

    #[test]
    fn frozen_documents_and_persistently_unknown_checks_lose_coverage() {
        let mut s = settings(&["w1"], &[]);
        s.hosts[0].checks.push("memory".into());
        let mut h = Harness::new();
        // The same document served for 46 seconds never renews coverage.
        for now in [1000, 1015, 1030, 1046] {
            h.tracker
                .record("w1", two_checks(1000, 1000, Status::Healthy), now);
            h.tick(&s, now);
        }
        assert!(h.active(COVERAGE), "a frozen snapshot is not progress");
        assert!(
            !h.active("host:w1:coverage:memory"),
            "host coverage alone reports a host that is missing"
        );
        // Fresh documents, but one check stays unknown: its own coverage warns
        // while the healthy check cannot mask it.
        let mut h = Harness::new();
        for now in (1000..=1050).step_by(5) {
            h.tracker
                .record("w1", two_checks(now, now, Status::Unknown), now);
            h.tick(&s, now);
        }
        assert!(!h.active(COVERAGE));
        assert!(h.active("host:w1:coverage:memory"));
        assert!(!h.active("host:w1:coverage:disk:/"));
        // One check's observation frozen while the document advances.
        let mut h = Harness::new();
        for now in (1000..=1050).step_by(5) {
            h.tracker
                .record("w1", two_checks(now, 1000, Status::Healthy), now);
            h.tick(&s, now);
        }
        assert!(h.active("host:w1:coverage:memory"));
        // Two distinct fresh known observations recover it; a repeat does not.
        h.tracker
            .record("w1", two_checks(1055, 1055, Status::Healthy), 1055);
        h.tick(&s, 1055);
        h.tracker
            .record("w1", two_checks(1055, 1055, Status::Healthy), 1058);
        h.tick(&s, 1058);
        assert!(h.active("host:w1:coverage:memory"));
        h.tracker
            .record("w1", two_checks(1060, 1060, Status::Healthy), 1060);
        h.tick(&s, 1060);
        assert!(!h.active("host:w1:coverage:memory"));
    }

    #[test]
    fn future_documents_are_not_progress() {
        let s = settings(&["w1"], &[]);
        let mut h = Harness::new();
        for now in [1000, 1020, 1046] {
            h.tracker
                .record("w1", doc("w1", now + 60, Status::Healthy), now);
            h.tick(&s, now);
        }
        assert!(h.active(COVERAGE));
    }

    #[test]
    fn snapshots_omit_stale_checks_and_map_dynamic_workers() {
        let host = settings(&["w1"], &[]).hosts.remove(0);
        let snapshot = Snapshot {
            sources: vec!["w1".into()],
            unit: "transparent-pir.service".into(),
            data_path: "/".into(),
            size: None,
        };
        let Poll::Valid(mut d) = doc("w1", 1040, Status::Healthy) else {
            unreachable!()
        };
        let bytes = |name: &str| pir_host_observations::Value {
            name: name.into(),
            value: 7.,
            unit: pir_host_observations::Unit::Bytes,
        };
        d.checks[0].observed_at = 990;
        d.checks[0].values = vec![bytes("available_bytes"), bytes("usable_bytes")];
        d.checks.push(Check {
            name: "memory".into(),
            status: Status::Healthy,
            reason: "ok".into(),
            observed_at: 1040,
            values: vec![bytes("available_bytes")],
        });
        // The document is fresh but its slow disk measurement is not.
        let entry = snapshot_entry(&host, &snapshot, Some(&d), 1040);
        assert!(entry.get("disk_available_bytes").is_none());
        assert_eq!(entry["memory_available_bytes"], 7);

        let dir = tempfile::tempdir().unwrap();
        let roster = dir.path().join("roster.json");
        std::fs::write(
            &roster,
            serde_json::json!([{"id": "w1", "intent": "enrolled", "role": "recent",
                "monitoring": {"address": "10.0.0.9:9443",
                    "config_digest": format!("sha256:{}", "0".repeat(64)),
                    "checks": ["disk:/"]}}])
            .to_string(),
        )
        .unwrap();
        let spec = RosterSnapshot {
            unit: "transparent-pir.service".into(),
            role_units: BTreeMap::from([("recent".into(), "transparent-recent.service".into())]),
            data_path: "/".into(),
            size: Some("m-4vcpu-32gb".into()),
        };
        let mut s = settings(&[], &[]);
        s.roster_snapshot = Some(spec.clone());
        let expected = Expected::of(&s, Some(roster_hosts(&roster, Some(&spec)).unwrap()));
        let mut tracker = Tracker::new(1000);
        tracker.record("w1", doc("w1", 1040, Status::Healthy), 1040);
        let out = dir.path().join("hosts.json");
        write_snapshot(&out, &tracker, &expected.hosts, 1041).unwrap();
        let written: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&out).unwrap()).unwrap();
        // The quality loop looks hosts up by the member's target id.
        assert_eq!(written["w1"]["unit"], "transparent-recent.service");
        assert_eq!(written["w1"]["size"], "m-4vcpu-32gb");
        assert_eq!(written["w1"]["at"], 1040);
    }
}
