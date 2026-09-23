//! Private inventory discovery and independent worker monitoring.
use crate::{dashboard::SharedDashboard, metrics::parse_line};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime},
};

#[derive(Clone, Debug, Default)]
pub struct Worker {
    pub name: String,
    pub group: String,
    pub url: String,
    pub values: BTreeMap<String, f64>,
    pub attempted: Option<SystemTime>,
    pub success: Option<SystemTime>,
    pub error: Option<String>,
}
impl Worker {
    pub fn status(&self) -> &'static str {
        if self
            .success
            .is_some_and(|t| t.elapsed().unwrap_or_default().as_secs() > 45)
        {
            "stale"
        } else if self.error.is_some() {
            "unreachable"
        } else if self.success.is_none() {
            "awaiting sample"
        } else {
            "reachable"
        }
    }
}
#[derive(Clone, Debug, Default)]
pub struct Group {
    pub role: Option<String>,
    pub shards: Option<f64>,
    pub published: Option<f64>,
}

pub fn inventory(text: &str) -> Result<BTreeMap<String, Worker>, String> {
    let value: serde_json::Value =
        serde_json::from_str(text).map_err(|_| "Invalid inventory JSON")?;
    let groups = value["groups"].as_array().ok_or("Missing groups")?;
    let mut out = BTreeMap::new();
    let mut names = BTreeSet::new();
    let valid = |s: &str| {
        !s.is_empty()
            && s.len() <= 128
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
    };
    for group in groups {
        let name = group["name"]
            .as_str()
            .filter(|s| valid(s))
            .ok_or("Invalid group name")?;
        if !names.insert(name) {
            return Err("Duplicate group".into());
        }
        for replica in group["replicas"].as_array().ok_or("Missing replicas")? {
            let worker = replica["name"]
                .as_str()
                .filter(|s| valid(s))
                .ok_or("Invalid worker name")?;
            let url = replica["url"].as_str().ok_or("Missing worker URL")?;
            let parsed = reqwest::Url::parse(url).map_err(|_| "Invalid worker URL")?;
            if !matches!(parsed.scheme(), "http" | "https")
                || parsed.host_str().is_none()
                || !parsed.username().is_empty()
                || parsed.password().is_some()
                || parsed.query().is_some()
                || parsed.fragment().is_some()
                || parsed.path() != "/"
            {
                return Err("Invalid worker origin".into());
            }
            if out
                .insert(
                    worker.into(),
                    Worker {
                        name: worker.into(),
                        group: name.into(),
                        url: url.trim_end_matches('/').into(),
                        ..Default::default()
                    },
                )
                .is_some()
            {
                return Err("Duplicate worker".into());
            }
        }
    }
    if out.is_empty() {
        return Err("Worker inventory has no replicas".into());
    }
    Ok(out)
}

pub fn worker_metrics(text: &str) -> Result<BTreeMap<String, f64>, String> {
    let mut out: BTreeMap<String, f64> = BTreeMap::new();
    for line in text
        .lines()
        .filter(|s| !s.trim().is_empty() && !s.starts_with('#'))
    {
        let sample = parse_line(line)?;
        if let Some(name) = sample.name.strip_prefix("enhance_worker_") {
            if sample.labels.is_empty() && sample.value.is_finite() {
                out.insert(name.into(), sample.value);
            }
        }
    }
    if out.get("up") != Some(&1.0) {
        return Err("Worker health metric unavailable".into());
    }
    if out.get("memory_sample_available") != Some(&1.0) {
        out.retain(|k, _| !k.ends_with("database_bytes"));
    }
    if out.get("memory_model_available") != Some(&1.0) {
        out.retain(|k, _| !k.starts_with("model_"));
    }
    Ok(out)
}

pub fn groups(text: &str, health: Option<&str>) -> BTreeMap<String, Group> {
    let mut out: BTreeMap<String, Group> = BTreeMap::new();
    for line in text
        .lines()
        .filter(|s| !s.trim().is_empty() && !s.starts_with('#'))
    {
        if let Ok(sample) = parse_line(line) {
            if let Some(name) = sample.labels.get("group") {
                let group = out.entry(name.clone()).or_default();
                if sample.name == "enhance_group_assigned_shards" {
                    group.shards = Some(sample.value);
                }
                if sample.name == "enhance_group_role" && sample.value == 1.0 {
                    group.role = sample.labels.get("role").cloned();
                }
            }
        }
    }
    if let Some(value) = health.and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok()) {
        if let Some(counts) = value["published_replica_counts"].as_object() {
            for (name, count) in counts {
                out.entry(name.clone()).or_default().published = count.as_f64();
            }
        }
    }
    out
}

pub async fn monitor(path: PathBuf, dashboard: SharedDashboard) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("worker HTTP client");
    let semaphore = Arc::new(tokio::sync::Semaphore::new(8));
    let mut interval = tokio::time::interval(Duration::from_secs(15));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        let configured = match tokio::fs::read_to_string(&path).await {
            Ok(text) => inventory(&text),
            Err(_) => Err("Worker inventory cannot be read".into()),
        };
        {
            let mut view = dashboard.write().await;
            match configured {
                Ok(mut workers) => {
                    for (name, worker) in &mut workers {
                        if let Some(old) = view
                            .fleet
                            .get(name)
                            .filter(|old| old.url == worker.url && old.group == worker.group)
                        {
                            *worker = old.clone();
                        }
                    }
                    view.fleet = workers;
                    view.inventory_error = None;
                }
                Err(error) => view.inventory_error = Some(error),
            }
        }
        let workers = dashboard.read().await.fleet.clone();
        let mut tasks = tokio::task::JoinSet::new();
        for (name, worker) in workers {
            let client = client.clone();
            let semaphore = semaphore.clone();
            let dashboard = dashboard.clone();
            tasks.spawn(async move {
                let _permit = semaphore.acquire().await.expect("scrape semaphore");
                let attempted = SystemTime::now();
                let result = async {
                    let response = client
                        .get(format!("{}/internal/metrics", worker.url))
                        .send()
                        .await
                        .map_err(|_| "Worker request failed".to_string())?;
                    if !response.status().is_success() {
                        return Err(format!(
                            "Worker returned HTTP {}",
                            response.status().as_u16()
                        ));
                    }
                    let body = response
                        .text()
                        .await
                        .map_err(|_| "Worker response failed".to_string())?;
                    worker_metrics(&body).map_err(|_| "Worker metrics unavailable".to_string())
                }
                .await;
                let mut view = dashboard.write().await;
                if let Some(sample) = view.fleet.get_mut(&name) {
                    sample.attempted = Some(attempted);
                    match result {
                        Ok(values) => {
                            sample.values = values;
                            sample.success = Some(SystemTime::now());
                            sample.error = None;
                        }
                        Err(error) => sample.error = Some(error),
                    }
                }
            });
        }
        while tasks.join_next().await.is_some() {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn inventory_rejects_duplicates_and_embedded_credentials() {
        assert!(inventory(r#"{"groups":[]}"#).is_err());
        assert!(inventory(r#"{"groups":[{"name":"g","replicas":[]}]}"#).is_err());
        assert!(inventory(r#"{"groups":[{"name":"g","replicas":[{"name":"w","url":"http://user:secret@localhost"}]}]}"#).is_err());
        assert!(inventory(r#"{"groups":[{"name":"g","replicas":[{"name":"w","url":"http://localhost"},{"name":"w","url":"http://localhost"}]}]}"#).is_err());
    }
    #[test]
    fn unavailable_memory_is_not_zero_or_previous_sample() {
        let values = worker_metrics("enhance_worker_up 1\nenhance_worker_memory_sample_available 0\nenhance_worker_live_database_bytes 99\n").unwrap();
        assert!(!values.contains_key("live_database_bytes"));
        assert!(worker_metrics("unrelated_metric 1").is_err());
    }
    #[test]
    fn groups_preserve_labels_and_published_counts() {
        let values = groups("enhance_group_role{group=\"a\",role=\"active\"} 1\nenhance_group_assigned_shards{group=\"a\"} 2\nenhance_group_assigned_shards{group=\"b\"} 3", Some(r#"{"published_replica_counts":{"a":2}}"#));
        assert_eq!(values["a"].role.as_deref(), Some("active"));
        assert_eq!(values["a"].published, Some(2.));
        assert_eq!(values["b"].shards, Some(3.));
    }
    #[test]
    fn stale_values_remain_distinct_from_reachability() {
        let worker = Worker {
            success: Some(SystemTime::now() - Duration::from_secs(60)),
            error: Some("failed".into()),
            ..Default::default()
        };
        assert_eq!(worker.status(), "stale");
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;
    use crate::{dashboard::DashboardData, host::HostHealth, schema::Schema};
    use axum::{routing::get, Router};

    #[tokio::test]
    async fn failed_and_slow_workers_do_not_block_pages_or_good_samples() {
        async fn serve(app: Router) -> (String, tokio::task::JoinHandle<()>) {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let task = tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            });
            (url, task)
        }
        let (good, good_task) = serve(Router::new().route(
            "/internal/metrics",
            get(|| async { "enhance_worker_up 1\nenhance_worker_queries_in_flight 2\n" }),
        ))
        .await;
        let (bad, bad_task) = serve(Router::new().route(
            "/internal/metrics",
            get(|| async { axum::http::StatusCode::SERVICE_UNAVAILABLE }),
        ))
        .await;
        let (slow, slow_task) = serve(Router::new().route(
            "/internal/metrics",
            get(|| async {
                tokio::time::sleep(Duration::from_secs(10)).await;
                "enhance_worker_up 1\n"
            }),
        ))
        .await;
        let path = std::env::temp_dir().join(format!(
            "pir-apm-test-{}-{}.json",
            std::process::id(),
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        tokio::fs::write(&path, serde_json::json!({"groups":[{"name":"g","replicas":[{"name":"good","url":good},{"name":"bad","url":bad},{"name":"slow","url":slow}]}]}).to_string()).await.unwrap();
        let mut data = DashboardData::new(
            "APM".into(),
            Schema::enhance_default(),
            "test".into(),
            "coordinator".into(),
            HostHealth::default(),
        );
        data.fleet_enabled = true;
        let state = Arc::new(tokio::sync::RwLock::new(data));
        let monitor_task = tokio::spawn(monitor(path.clone(), state.clone()));
        let (base, page_task) = serve(crate::dashboard_router(state.clone())).await;
        let client = reqwest::Client::new();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if state
                    .read()
                    .await
                    .fleet
                    .get("good")
                    .is_some_and(|w| w.success.is_some())
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("good sample should not wait for slow worker");
        for route in [
            "/apm/",
            "/coordinator/",
            "/apm/coordinator/",
            "/workers/good/",
            "/apm/workers/good/",
        ] {
            let response = client.get(format!("{base}{route}")).send().await.unwrap();
            assert_eq!(response.status(), 200, "{route}");
            let html = response.text().await.unwrap();
            assert!(!html.contains(&good));
        }
        assert_eq!(
            client
                .get(format!("{base}/workers/unknown/"))
                .send()
                .await
                .unwrap()
                .status(),
            404
        );
        tokio::time::timeout(Duration::from_secs(6), async {
            loop {
                let view = state.read().await;
                if view.fleet["slow"].error.is_some() && view.fleet["bad"].error.is_some() {
                    break;
                }
                drop(view);
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("worker failures must time out");
        tokio::fs::write(&path, "invalid JSON").await.unwrap();
        tokio::time::timeout(Duration::from_secs(16), async {
            loop {
                if state.read().await.inventory_error.is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("inventory changes must be reloaded");
        assert_eq!(
            state.read().await.fleet.len(),
            3,
            "retain last valid inventory"
        );
        monitor_task.abort();
        page_task.abort();
        good_task.abort();
        bad_task.abort();
        slow_task.abort();
        tokio::fs::remove_file(path).await.unwrap();
    }
}
