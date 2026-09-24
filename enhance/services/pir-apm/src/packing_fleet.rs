//! Discover packing hosts from the coordinator's router inventory. Only fixed
//! health fields and aggregate metrics reach the public dashboard.
use crate::{dashboard::SharedDashboard, fleet::Worker, metrics::parse_line};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime},
};

#[derive(Clone, Debug, Default)]
pub struct PackingRouter {
    /// Empty means all domains, matching RouterRegistration.
    pub domains: std::collections::BTreeSet<u64>,
    pub sample: Worker,
    pub ready: Option<bool>,
}
impl PackingRouter {
    pub fn serves(&self, domain: u64) -> bool {
        self.domains.is_empty() || self.domains.contains(&domain)
    }
    pub fn status(&self) -> &'static str {
        match self.sample.status() {
            "reachable" if self.ready == Some(true) => "ready",
            "reachable" => "not ready",
            other => other,
        }
    }
}

pub fn inventory(text: &str) -> Result<BTreeMap<String, PackingRouter>, String> {
    let value: serde_json::Value =
        serde_json::from_str(text).map_err(|_| "Invalid packing router inventory JSON")?;
    let entries = value
        .as_array()
        .ok_or("Packing router inventory must be an array")?;
    if entries.len() > 64 {
        return Err("Too many packing routers".into());
    }
    let mut result = BTreeMap::new();
    let mut origins = std::collections::BTreeSet::new();
    for entry in entries {
        let name = entry["name"]
            .as_str()
            .filter(|s| {
                !s.is_empty()
                    && s.len() <= 128
                    && s.bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
            })
            .ok_or("Invalid packing router name")?;
        let url = entry["url"]
            .as_str()
            .ok_or("Missing packing router origin")?;
        let parsed = reqwest::Url::parse(url).map_err(|_| "Invalid packing router origin")?;
        if !matches!(parsed.scheme(), "http" | "https")
            || parsed.host_str().is_none()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
            || parsed.path() != "/"
            || !origins.insert(parsed.to_string())
        {
            return Err("Invalid or duplicate packing router origin".into());
        }
        let domains = match entry.get("domains") {
            None => std::collections::BTreeSet::new(),
            Some(domains) => domains
                .as_array()
                .ok_or("Invalid router domains")?
                .iter()
                .map(|v| v.as_u64().ok_or("Invalid router domain"))
                .collect::<Result<_, _>>()?,
        };
        let node = PackingRouter {
            domains,
            sample: Worker {
                name: name.into(),
                url: parsed.as_str().trim_end_matches('/').into(),
                ..Default::default()
            },
            ready: None,
        };
        if result.insert(name.into(), node).is_some() {
            return Err("Duplicate packing router name".into());
        }
    }
    Ok(result)
}

fn measurements(metrics: &str, health: &str) -> Result<(BTreeMap<String, f64>, bool), String> {
    let mut values = BTreeMap::new();
    for line in metrics
        .lines()
        .filter(|l| !l.trim().is_empty() && !l.trim().starts_with('#'))
    {
        let sample = parse_line(line)?;
        if let Some(name) = sample.name.strip_prefix("enhance_packing_router_") {
            if sample.labels.is_empty() && sample.value.is_finite() && sample.value >= 0.0 {
                values.insert(name.to_string(), sample.value);
            }
        }
    }
    if !values.contains_key("resident_objects") {
        return Err("Packing router metrics unavailable".into());
    }
    let health: serde_json::Value =
        serde_json::from_str(health).map_err(|_| "Invalid packing router health")?;
    let ready = health["ready"]
        .as_bool()
        .ok_or("Packing router readiness unavailable")?;
    for key in ["available_requests", "controller_epoch"] {
        if let Some(n) = health[key].as_u64() {
            values.insert(key.into(), n as f64);
        }
    }
    Ok((values, ready))
}

pub async fn monitor(path: PathBuf, dashboard: SharedDashboard) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("packing router HTTP client");
    let semaphore = Arc::new(tokio::sync::Semaphore::new(8));
    let mut interval = tokio::time::interval(Duration::from_secs(15));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        let configured = match tokio::fs::read_to_string(&path).await {
            Ok(text) => inventory(&text),
            Err(_) => Err("Packing router inventory cannot be read".into()),
        };
        {
            let mut view = dashboard.write().await;
            match configured {
                Ok(mut routers) => {
                    for (name, router) in &mut routers {
                        if let Some(old) = view
                            .packing_routers
                            .get(name)
                            .filter(|old| old.sample.url == router.sample.url)
                        {
                            router.sample = old.sample.clone();
                            router.ready = old.ready;
                        }
                    }
                    view.packing_routers = routers;
                    view.packing_inventory_error = None;
                }
                Err(error) => view.packing_inventory_error = Some(error),
            }
        }
        let routers = dashboard.read().await.packing_routers.clone();
        let mut tasks = tokio::task::JoinSet::new();
        for (name, router) in routers {
            let (client, semaphore, dashboard) =
                (client.clone(), semaphore.clone(), dashboard.clone());
            tasks.spawn(async move {
                let _permit = semaphore.acquire().await.expect("packing scrape semaphore");
                let attempted = SystemTime::now();
                let fetch = |path| {
                    let client = client.clone();
                    let url = format!("{}{path}", router.sample.url);
                    async move {
                        let response = client
                            .get(url)
                            .send()
                            .await
                            .map_err(|_| "Packing router request failed")?;
                        if !response.status().is_success() {
                            return Err("Packing router returned an unsuccessful HTTP status");
                        }
                        response
                            .text()
                            .await
                            .map_err(|_| "Packing router response failed")
                    }
                };
                let (metrics, health) =
                    tokio::join!(fetch("/internal/metrics"), fetch("/internal/health"));
                let result = match (metrics, health) {
                    (Ok(metrics), Ok(health)) => measurements(&metrics, &health)
                        .map_err(|_| "Packing router measurements unavailable"),
                    _ => Err("Packing router health or metrics request failed"),
                };
                let mut view = dashboard.write().await;
                if let Some(current) = view
                    .packing_routers
                    .get_mut(&name)
                    .filter(|r| r.sample.url == router.sample.url)
                {
                    current.sample.attempted = Some(attempted);
                    match result {
                        Ok((values, ready)) => {
                            current.sample.values = values;
                            current.ready = Some(ready);
                            current.sample.success = Some(SystemTime::now());
                            current.sample.error = None;
                        }
                        Err(error) => current.sample.error = Some(error.into()),
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
    fn inventory_accepts_coordinator_format_and_rejects_unsafe_names_and_origins() {
        let routers = inventory(r#"[{"name":"packing-01","url":"http://10.0.0.1:8093","query_url":"http://10.0.0.1:8092","domains":[]}]"#).unwrap();
        assert_eq!(routers["packing-01"].sample.url, "http://10.0.0.1:8093");
        for text in [
            r#"[{"name":"../bad","url":"http://localhost"}]"#,
            r#"[{"name":"p","url":"http://user:secret@localhost"}]"#,
            r#"[{"name":"p","url":"http://localhost/path"}]"#,
            r#"[{"name":"p","url":"http://localhost"},{"name":"p","url":"http://elsewhere"}]"#,
            r#"[{"name":"p","url":"http://localhost"},{"name":"q","url":"http://localhost/"}]"#,
        ] {
            assert!(inventory(text).is_err(), "{text}");
        }
    }
    #[test]
    fn readiness_is_distinct_from_reachability_and_stale_values() {
        let (values, ready) = measurements("enhance_packing_router_resident_objects 2\nenhance_packing_router_charged_bytes 123\n", r#"{"ready":false,"controller_epoch":3,"available_requests":4,"outstanding":{"http://private":1}}"#).unwrap();
        assert!(!ready);
        assert_eq!(values["charged_bytes"], 123.0);
        assert_eq!(values["available_requests"], 4.0);
        assert!(!format!("{values:?}").contains("private"));
        let mut router = PackingRouter {
            domains: Default::default(),
            sample: Worker {
                success: Some(SystemTime::now()),
                values,
                ..Default::default()
            },
            ready: Some(ready),
        };
        assert_eq!(router.status(), "not ready");
        router.ready = Some(true);
        assert_eq!(router.status(), "ready");
        router.sample.error = Some("failed".into());
        assert_eq!(router.status(), "unreachable");
        router.sample.success = Some(SystemTime::now() - Duration::from_secs(46));
        assert_eq!(router.status(), "stale");
        assert!(measurements("unrelated_metric 1", r#"{"ready":true}"#).is_err());
        assert!(measurements("enhance_packing_router_resident_objects 2", "{}").is_err());
    }
    #[tokio::test]
    async fn discovery_pages_and_failed_scrapes_preserve_last_valid_sample() {
        use axum::{http::StatusCode, routing::get, Router};
        use std::sync::atomic::{AtomicBool, Ordering};
        async fn serve(app: Router) -> (String, tokio::task::JoinHandle<()>) {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let origin = format!("http://{}", listener.local_addr().unwrap());
            (
                origin,
                tokio::spawn(async move {
                    axum::serve(listener, app).await.unwrap();
                }),
            )
        }
        let failed = Arc::new(AtomicBool::new(false));
        let flag = failed.clone();
        let (origin, server) = serve(Router::new().route("/internal/metrics", get(|| async { "enhance_packing_router_resident_objects 2\nenhance_packing_router_charged_bytes 1048576\n" }))
            .route("/internal/health", get(move || { let failed = flag.load(Ordering::Relaxed); async move { (if failed {StatusCode::SERVICE_UNAVAILABLE} else {StatusCode::OK}, r#"{"ready":true,"available_requests":4}"#) } }))).await;
        let path = std::env::temp_dir().join(format!(
            "packing-apm-{}-{}.json",
            std::process::id(),
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        tokio::fs::write(
            &path,
            serde_json::json!([{"name":"packing-01","url":origin}]).to_string(),
        )
        .await
        .unwrap();
        let mut data = crate::dashboard::DashboardData::new(
            "APM".into(),
            crate::schema::Schema::enhance_default(),
            "test".into(),
            "coord".into(),
            crate::host::HostHealth::default(),
        );
        data.packing_enabled = true;
        let state = Arc::new(tokio::sync::RwLock::new(data));
        let monitor = tokio::spawn(monitor(path.clone(), state.clone()));
        let (base, pages) = serve(crate::dashboard_router(state.clone())).await;
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if state
                    .read()
                    .await
                    .packing_routers
                    .get("packing-01")
                    .is_some_and(|r| r.status() == "ready")
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        let client = reqwest::Client::new();
        for path in [
            "/apm/",
            "/packing-routers/packing-01/",
            "/apm/packing-routers/packing-01/",
        ] {
            let response = client.get(format!("{base}{path}")).send().await.unwrap();
            assert_eq!(response.status(), 200);
            let html = response.text().await.unwrap();
            assert!(html.contains("packing-01"));
            assert!(!html.contains(&origin));
            if path == "/apm/" {
                assert!(html.contains("/apm/packing-routers/packing-01/"));
                assert!(html.contains("Entrypoint APM"));
            } else {
                assert!(html.contains("Charged packing memory"));
                assert!(html.contains("1.0 MiB"));
            }
        }
        assert_eq!(
            client
                .get(format!("{base}/apm/packing-routers/unknown/"))
                .send()
                .await
                .unwrap()
                .status(),
            404
        );
        let success = state.read().await.packing_routers["packing-01"]
            .sample
            .success;
        failed.store(true, Ordering::Relaxed);
        tokio::fs::write(&path, "invalid").await.unwrap();
        tokio::time::timeout(Duration::from_secs(17), async {
            loop {
                if state.read().await.packing_routers["packing-01"]
                    .sample
                    .error
                    .is_some()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap();
        {
            let data = state.read().await;
            assert!(data.packing_inventory_error.is_some());
            assert_eq!(data.packing_routers["packing-01"].sample.success, success);
            assert_eq!(
                data.packing_routers["packing-01"].sample.values["charged_bytes"],
                1048576.0
            );
        }
        monitor.abort();
        server.abort();
        pages.abort();
        tokio::fs::remove_file(path).await.unwrap();
    }
}
