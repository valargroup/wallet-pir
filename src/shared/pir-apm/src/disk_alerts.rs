//! Fleet disks are observed independently of product metrics and alert modes.
//! Missing hosts or filesystems are unknown, never evidence of reclaimed space.
use anyhow::Result;
use pir_apm::incidents::{Condition, Store};
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    io::Read,
    path::{Path, PathBuf},
};

pub const SNAPSHOT_VAR: &str = "PIR_APM_DISK_SNAPSHOT";
const MODE_VAR: &str = "PIR_APM_DISK_ALERT_MODE";
const MAX_BYTES: u64 = 2 * 1024 * 1024;
const STALE: u64 = 45;

pub struct Settings {
    path: PathBuf,
    shadow: bool,
}

impl Settings {
    pub fn from_env() -> Result<Option<Self>> {
        let path = std::env::var_os(SNAPSHOT_VAR).map(PathBuf::from);
        let mode = std::env::var(MODE_VAR).unwrap_or_else(|_| "shadow".into());
        anyhow::ensure!(
            matches!(mode.as_str(), "shadow" | "active"),
            "{MODE_VAR} must be shadow or active"
        );
        let Some(path) = path else {
            anyhow::ensure!(
                mode != "active",
                "{MODE_VAR}=active requires {SNAPSHOT_VAR}"
            );
            return Ok(None);
        };
        anyhow::ensure!(
            path.is_absolute(),
            "{SNAPSHOT_VAR} must be an absolute path"
        );
        Ok(Some(Self {
            path,
            shadow: mode == "shadow",
        }))
    }
}

pub fn configured() -> bool {
    std::env::var_os(SNAPSHOT_VAR).is_some()
}

#[derive(Deserialize)]
struct Snapshot {
    at: u64,
    inventory_ok: bool,
    hosts: BTreeMap<String, Host>,
    #[serde(default)]
    retired_hosts: Vec<String>,
    #[serde(default)]
    retired_filesystems: BTreeMap<String, Vec<String>>,
}

#[derive(Deserialize)]
struct Host {
    at: u64,
    filesystems: Vec<Filesystem>,
    error: Option<String>,
}

#[derive(Deserialize)]
struct Filesystem {
    id: String,
    mount_point: String,
    device: String,
    available_bytes: Option<u64>,
    total_bytes: Option<u64>,
}

fn fresh(at: u64, now: u64) -> bool {
    at > 0 && at <= now && now - at <= STALE
}

fn valid_host(host: &str) -> bool {
    !host.is_empty()
        && host.len() <= 80
        && host
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_.@-".contains(&b))
}

fn text(value: &str) -> String {
    value
        .chars()
        .take(256)
        .filter(|c| !c.is_control())
        .collect::<String>()
}

fn disk_key(host: &str, id: &str) -> String {
    // Hex is injective: punctuation in a device path cannot alias another disk.
    let id: String = id.bytes().map(|b| format!("{b:02x}")).collect();
    format!("disk_space_{host}_{id}")
}

fn coverage(key: String, host: &str, healthy: bool, now: u64) -> Condition {
    Condition {
        retired: false,
        key,
        resource: host.into(),
        severity: "warning".into(),
        firing: Some(!healthy),
        observed: if healthy {
            "fresh"
        } else {
            "disk observation unavailable"
        }
        .into(),
        threshold: "disk observation unavailable for 45s".into(),
        hold_seconds: STALE,
        sample: now,
    }
}

fn conditions(snapshot: Option<&Snapshot>, now: u64, previous: &[Condition]) -> Vec<Condition> {
    let snapshot = snapshot.filter(|s| fresh(s.at, now));
    let mut inventory = coverage(
        "disk_inventory_coverage".into(),
        "fleet",
        snapshot.is_some_and(|s| {
            s.inventory_ok && !s.hosts.is_empty() && s.hosts.keys().all(|h| valid_host(h))
        }),
        now,
    );
    if inventory.firing == Some(false) {
        inventory.sample = snapshot.unwrap().at;
    }
    let mut out = vec![inventory];
    if let Some(s) = snapshot {
        for (host, h) in &s.hosts {
            if !valid_host(host) {
                continue;
            }
            let mut seen = std::collections::BTreeSet::new();
            let valid = fresh(h.at, now)
                && h.error.is_none()
                && !h.filesystems.is_empty()
                && h.filesystems
                    .iter()
                    .all(|f| !f.id.is_empty() && f.id.len() <= 256 && seen.insert(&f.id));
            let mut complete = valid;
            if valid {
                for f in &h.filesystems {
                    let bytes = f
                        .available_bytes
                        .zip(f.total_bytes)
                        .filter(|(a, t)| *t > 0 && a <= t);
                    complete &= bytes.is_some();
                    let mount = text(&f.mount_point);
                    let observed = bytes.map_or_else(
                        || format!("host {host}, mount {mount}: unavailable"),
                        |(available, total)| format!(
                            "host {host}, mount {mount}, device {}, filesystem {}: {:.2}% available ({available} / {total} bytes)",
                            text(&f.device), text(&f.id), available as f64 / total as f64 * 100.
                        ),
                    );
                    out.push(Condition {
                        retired: false,
                        key: disk_key(host, &f.id),
                        resource: host.clone(),
                        severity: "warning".into(),
                        // Integer arithmetic keeps the exact 15% boundary unambiguous.
                        firing: bytes.map(|(a, t)| (a as u128) * 100 < (t as u128) * 15),
                        observed,
                        threshold: "less than 15% disk space available".into(),
                        hold_seconds: 0,
                        sample: h.at,
                    });
                }
            }
            let mut c = coverage(format!("disk_host_coverage_{host}"), host, complete, now);
            if complete {
                c.sample = h.at;
            }
            out.push(c);
        }
    }
    // Recreate a missing disk's coverage before visiting its saved coverage
    // condition, otherwise that condition would overwrite firing with unknown.
    let missing = |c: &&Condition| c.key.starts_with("disk_missing_");
    for old in previous
        .iter()
        .filter(|c| !missing(c))
        .chain(previous.iter().filter(missing))
    {
        if !old.key.starts_with("disk_") || out.iter().any(|c| c.key == old.key) {
            continue;
        }
        let retired = snapshot.is_some_and(|s| {
            s.inventory_ok
                && (s.retired_hosts.contains(&old.resource)
                    || s.retired_filesystems.get(&old.resource).is_some_and(|ids| {
                        ids.iter().any(|id| {
                            disk_key(&old.resource, id)
                                == old.key.strip_prefix("disk_missing_").unwrap_or(&old.key)
                        })
                    }))
        });
        let mut unknown = old.clone();
        unknown.retired = retired;
        unknown.firing = None;
        unknown.observed = "disk observation unavailable".into();
        unknown.sample = 0;
        out.push(unknown);
        if old.key.starts_with("disk_space_") {
            let key = format!("disk_missing_{}", old.key);
            if !out.iter().any(|c| c.key == key) {
                let mut c = coverage(key, &old.resource, retired, now);
                c.retired = retired;
                out.push(c);
            }
        }
    }
    // A previously missing filesystem has returned: clear its coverage only
    // with a fresh valid measurement, using distinct sampler timestamps.
    for c in &mut out {
        if let Some(key) = c.key.strip_prefix("disk_missing_") {
            if let Some(measured) = snapshot
                .and_then(|s| s.hosts.get(&c.resource))
                .filter(|h| fresh(h.at, now) && h.error.is_none())
            {
                if measured.filesystems.iter().any(|f| {
                    disk_key(&c.resource, &f.id) == key
                        && f.total_bytes.is_some_and(|t| t > 0)
                        && f.available_bytes
                            .zip(f.total_bytes)
                            .is_some_and(|(a, t)| a <= t)
                }) {
                    c.firing = Some(false);
                    c.sample = measured.at;
                }
            }
        }
    }
    out
}

pub struct Family {
    settings: Settings,
    store: Store,
}

impl Family {
    pub fn open(settings: Settings, database: &Path) -> Result<Self> {
        Ok(Self {
            settings,
            store: Store::open(database)?.with_family("disk"),
        })
    }

    pub fn evaluate(&mut self, now: u64, environment: &str, dashboard: &str) -> Result<()> {
        let snapshot = (|| -> Option<Snapshot> {
            let file = std::fs::File::open(&self.settings.path).ok()?;
            let mut bytes = Vec::new();
            file.take(MAX_BYTES + 1).read_to_end(&mut bytes).ok()?;
            if bytes.len() as u64 > MAX_BYTES {
                return None;
            }
            serde_json::from_slice(&bytes).ok()
        })();
        let previous: Vec<_> = self
            .store
            .incidents()?
            .into_iter()
            .filter_map(|i| i.condition)
            .collect();
        let mut conditions = conditions(snapshot.as_ref(), now, &previous);
        for old in previous.iter().filter(|c| {
            c.key == "disk_usage"
                || c.key.starts_with("quality_disk_")
                || (c.key.starts_with("quality.") && c.key.contains(".disk_"))
        }) {
            let mut retired = old.clone();
            retired.retired = true;
            retired.sample = now;
            conditions.push(retired);
        }
        self.store.evaluate(
            &conditions,
            now,
            self.settings.shadow,
            environment,
            dashboard,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    fn snapshot(at: u64, root: u64, volume: u64) -> Value {
        json!({"at": at, "inventory_ok": true, "hosts": {"details": {
        "at": at, "filesystems": [
            {"id": "root", "mount_point": "/", "device": "/dev/vda1", "available_bytes": root, "total_bytes": 100},
            {"id": "volume", "mount_point": "/mnt/data", "device": "/dev/sda", "available_bytes": volume, "total_bytes": 100}
        ]}}})
    }

    fn evaluate(v: Value, now: u64, previous: &[Condition]) -> Vec<Condition> {
        conditions(Some(&serde_json::from_value(v).unwrap()), now, previous)
    }

    fn disk(conditions: &[Condition], id: &str) -> Option<bool> {
        conditions
            .iter()
            .find(|c| c.key == disk_key("details", id))
            .unwrap()
            .firing
    }

    #[test]
    fn root_and_volumes_have_independent_exact_thresholds() {
        let c = evaluate(snapshot(100, 15, 14), 100, &[]);
        assert_eq!(disk(&c, "root"), Some(false));
        assert_eq!(disk(&c, "volume"), Some(true));
        let c = evaluate(snapshot(100, 0, 80), 100, &[]);
        assert_eq!(disk(&c, "root"), Some(true));
        assert_eq!(disk(&c, "volume"), Some(false));
        assert_eq!(
            c.iter()
                .find(|c| c.key == disk_key("details", "root"))
                .unwrap()
                .hold_seconds,
            0
        );
        assert!(c
            .iter()
            .find(|c| c.key == disk_key("details", "volume"))
            .unwrap()
            .observed
            .contains("/mnt/data"));
    }

    #[test]
    fn stale_invalid_and_disappeared_disks_never_recover() {
        let old = evaluate(snapshot(100, 50, 0), 100, &[]);
        for now in [146, 300] {
            let c = evaluate(snapshot(100, 50, 0), now, &old);
            assert_eq!(disk(&c, "volume"), None);
            assert!(c
                .iter()
                .any(|c| c.key.starts_with("disk_missing_") && c.firing == Some(true)));
        }
        let mut absent = snapshot(115, 50, 50);
        absent["hosts"]["details"]["filesystems"]
            .as_array_mut()
            .unwrap()
            .pop();
        let c = evaluate(absent, 115, &old);
        assert_eq!(disk(&c, "volume"), None);
        for invalid in [json!(0), json!(null)] {
            let mut v = snapshot(115, 50, 50);
            v["hosts"]["details"]["filesystems"][1]["total_bytes"] = invalid;
            assert_eq!(disk(&evaluate(v, 115, &old), "volume"), None);
        }
        let mut retired = snapshot(115, 50, 50);
        retired["hosts"] = json!({});
        retired["retired_hosts"] = json!(["details"]);
        assert!(
            evaluate(retired, 115, &old)
                .iter()
                .find(|c| c.key == disk_key("details", "volume"))
                .unwrap()
                .retired
        );
    }

    struct Fixture {
        root: PathBuf,
        path: PathBuf,
        database: PathBuf,
    }
    impl Fixture {
        fn new() -> Self {
            let root = std::env::temp_dir().join(format!("pir-disks-{}", rand::random::<u64>()));
            std::fs::create_dir(&root).unwrap();
            Self {
                path: root.join("disks.json"),
                database: root.join("incidents.sqlite"),
                root,
            }
        }
        fn write(&self, v: &Value) {
            std::fs::write(&self.path, serde_json::to_vec(v).unwrap()).unwrap();
        }
        fn sample(&self, v: &Value) {
            // Exercise the actual Python snapshot producer before the Rust
            // evaluator and HTTP Slack sink, without SSH or a real full disk.
            let script = Path::new(env!("CARGO_MANIFEST_DIR"))
                .ancestors()
                .nth(3)
                .unwrap()
                .join("tools/enhance/scripts/quality-host-sampler.py");
            let status = std::process::Command::new("python3")
                .arg("-B")
                .arg("-c")
                .arg(
                    r#"
import importlib.util, json, sys
from pathlib import Path
spec = importlib.util.spec_from_file_location('sampler', sys.argv[1])
sampler = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sampler)
entry = json.loads(sys.argv[3])['hosts']['details']
sampler.sample = lambda target, ssh: entry
sampler.collect({'targets': [{'sources': ['details'], 'host_id': 'details'}],
                'ssh': {}, 'output': str(Path(sys.argv[2]).with_name('hosts.json')),
                'disk_output': sys.argv[2]})
"#,
                )
                .arg(script)
                .arg(&self.path)
                .arg(v.to_string())
                .status()
                .unwrap();
            assert!(status.success());
        }
        fn family(&self, shadow: bool) -> Family {
            Family::open(
                Settings {
                    path: self.path.clone(),
                    shadow,
                },
                &self.database,
            )
            .unwrap()
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    #[tokio::test]
    async fn first_sample_retries_slack_survives_restart_and_recovers_once() {
        use axum::{http::StatusCode, routing::post, Json, Router};
        use std::sync::{Arc, Mutex};
        use std::time::Duration;
        let f = Fixture::new();
        let now = pir_apm::incidents::unix_time();
        f.sample(&snapshot(now, 50, 0));
        let mut family = f.family(false);
        family
            .evaluate(now, "test", "http://dashboard/apm/")
            .unwrap();
        let store = Store::open(&f.database).unwrap();
        assert_eq!(store.health(true, now).unwrap().pending, 1);
        family.evaluate(now + 1, "test", "").unwrap();
        assert_eq!(store.health(true, now).unwrap().pending, 1);
        drop(family);
        let mut family = f.family(false);
        std::fs::write(&f.path, b"invalid JSON").unwrap();
        family.evaluate(now + 2, "test", "").unwrap();
        assert!(store.incidents().unwrap().iter().any(
            |i| i.active && i.condition.as_ref().unwrap().key == disk_key("details", "volume")
        ));
        f.write(&snapshot(now + 3, 50, 50));
        family.evaluate(now + 3, "test", "").unwrap();
        family.evaluate(now + 4, "test", "").unwrap();
        assert_eq!(
            store.health(true, now).unwrap().pending,
            1,
            "the same healthy sample cannot recover"
        );
        f.write(&snapshot(now + 5, 50, 50));
        family.evaluate(now + 5, "test", "").unwrap();
        assert_eq!(store.health(true, now).unwrap().pending, 2);

        let received = Arc::new(Mutex::new(Vec::<String>::new()));
        let calls = received.clone();
        let app = Router::new().route(
            "/",
            post(move |Json(body): Json<Value>| {
                let calls = calls.clone();
                async move {
                    let mut calls = calls.lock().unwrap();
                    calls.push(body["text"].as_str().unwrap().to_string());
                    (
                        if calls.len() == 1 {
                            StatusCode::TOO_MANY_REQUESTS
                        } else {
                            StatusCode::OK
                        },
                        [("retry-after", "1")],
                    )
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let worker = tokio::spawn(pir_apm::incidents::deliver(
            f.database.clone(),
            Some(format!("http://{address}/")),
        ));
        let result = tokio::time::timeout(Duration::from_secs(12), async {
            while store
                .health(true, pir_apm::incidents::unix_time())
                .unwrap()
                .pending
                != 0
            {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await;
        worker.abort();
        server.abort();
        result.unwrap();
        let messages = received.lock().unwrap();
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0], messages[1]);
        assert!(messages[0].contains("/mnt/data"));
        assert!(messages[2].contains("RECOVERED"));
    }

    #[test]
    fn shadow_and_explicit_retirement_preserve_other_families() {
        let f = Fixture::new();
        f.write(&snapshot(100, 50, 0));
        let mut family = f.family(true);
        family.evaluate(100, "test", "").unwrap();
        let mut store = Store::open(&f.database).unwrap();
        assert_eq!(store.health(true, 100).unwrap().pending, 0);
        let mut unrelated = coverage("unrelated".into(), "other", false, 101);
        unrelated.hold_seconds = 0;
        store
            .evaluate(&[unrelated], 101, false, "test", "")
            .unwrap();
        let mut retired = snapshot(102, 50, 50);
        retired["hosts"]["details"]["filesystems"]
            .as_array_mut()
            .unwrap()
            .pop();
        retired["retired_filesystems"] = json!({"details": ["volume"]});
        f.write(&retired);
        family.evaluate(102, "test", "").unwrap();
        assert!(!store.incidents().unwrap().iter().any(
            |i| i.active && i.condition.as_ref().unwrap().key == disk_key("details", "volume")
        ));
        assert!(
            store
                .incidents()
                .unwrap()
                .iter()
                .find(|i| i.condition.as_ref().unwrap().key == "unrelated")
                .unwrap()
                .active
        );
        assert_eq!(store.health(true, 102).unwrap().pending, 1);
    }

    #[test]
    fn a_missing_volume_alerts_and_only_real_samples_recover_it() {
        let f = Fixture::new();
        let mut family = f.family(false);
        f.write(&snapshot(100, 50, 0));
        family.evaluate(100, "test", "").unwrap();
        for at in [115, 130, 145, 160] {
            let mut v = snapshot(at, 50, 50);
            v["hosts"]["details"]["filesystems"]
                .as_array_mut()
                .unwrap()
                .pop();
            f.write(&v);
            family.evaluate(at, "test", "").unwrap();
        }
        let store = Store::open(&f.database).unwrap();
        let active = || {
            store
                .incidents()
                .unwrap()
                .iter()
                .filter(|i| i.active)
                .count()
        };
        assert_eq!(
            active(),
            2,
            "missing volume coverage and original low-space incident"
        );
        f.write(&snapshot(175, 50, 50));
        family.evaluate(175, "test", "").unwrap();
        family.evaluate(180, "test", "").unwrap();
        assert_eq!(active(), 2, "repeated evaluation is not another sample");
        f.write(&snapshot(190, 50, 50));
        family.evaluate(190, "test", "").unwrap();
        assert_eq!(active(), 0);
    }
}
