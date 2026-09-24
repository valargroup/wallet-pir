//! Public entrypoint metrics are sampled independently of fleet health probes.
//! Query ingress targets replace coordinator query metrics to avoid double counting.
use crate::{
    config::Config,
    dashboard::{EntrypointData, SharedDashboard},
    metrics::{self, RollingMetrics},
};
use std::{
    collections::BTreeMap,
    time::{Instant, SystemTime},
};

struct Source {
    url: String,
    rolling: RollingMetrics,
    success: BTreeMap<String, SystemTime>,
    ok: bool,
}

pub async fn run(config: Config, dashboard: SharedDashboard) {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .expect("entrypoint HTTP client");
    let mut urls = vec![format!("{}{}", config.scrape_url, config.metrics_path)];
    urls.extend(config.query_scrape_urls.clone());
    let mut sources: Vec<_> = urls
        .into_iter()
        .map(|url| Source {
            url,
            rolling: RollingMetrics::new(config.schema.clone()),
            success: BTreeMap::new(),
            ok: false,
        })
        .collect();
    let mut interval = tokio::time::interval(config.interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        let mut tasks = tokio::task::JoinSet::new();
        for (index, source) in sources.iter().enumerate() {
            let client = client.clone();
            let url = source.url.clone();
            let schema = config.schema.clone();
            tasks.spawn(async move {
                let result = async {
                    let response = client
                        .get(url)
                        .send()
                        .await
                        .map_err(|_| ())?
                        .error_for_status()
                        .map_err(|_| ())?;
                    let text = response.text().await.map_err(|_| ())?;
                    metrics::parse_prometheus(&schema, &text, Instant::now()).map_err(|_| ())
                }
                .await;
                (index, result)
            });
        }
        for source in &mut sources {
            source.ok = false;
        }
        while let Some(Ok((index, result))) = tasks.join_next().await {
            if let Ok(snapshot) = result {
                for endpoint in ["init", "query"] {
                    if snapshot.endpoints.get(endpoint).is_some_and(|e| {
                        e.upload_bytes.is_some()
                            && e.download_bytes.is_some()
                            && !e.processing.buckets.is_empty()
                    }) {
                        sources[index]
                            .success
                            .insert(endpoint.into(), SystemTime::now());
                    }
                }
                sources[index].rolling.push(snapshot);
                sources[index].ok = true;
            }
        }
        let init = summarize(&[&sources[0]], "init");
        let query_sources: Vec<_> = if sources.len() == 1 {
            vec![&sources[0]]
        } else {
            sources[1..].iter().collect()
        };
        let query = summarize(&query_sources, "query");
        dashboard.write().await.entrypoints =
            BTreeMap::from([("init".into(), init), ("query".into(), query)]);
    }
}

fn summarize(sources: &[&Source], endpoint: &str) -> EntrypointData {
    let available = sources.iter().all(|s| {
        s.ok && s
            .rolling
            .latest()
            .and_then(|m| m.endpoints.get(endpoint))
            .is_some_and(|e| {
                e.upload_bytes.is_some()
                    && e.download_bytes.is_some()
                    && !e.processing.buckets.is_empty()
            })
    });
    EntrypointData {
        window: RollingMetrics::entrypoint(
            &sources.iter().map(|s| &s.rolling).collect::<Vec<_>>(),
            endpoint,
        ),
        last_success: sources
            .iter()
            .map(|s| s.success.get(endpoint).copied())
            .collect::<Option<Vec<_>>>()
            .and_then(|v| v.into_iter().min()),
        error: !available,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn missing_instrumentation_and_failed_sources_are_unavailable() {
        let mut source = Source {
            url: String::new(),
            rolling: RollingMetrics::new(crate::schema::Schema::enhance_default()),
            success: BTreeMap::new(),
            ok: true,
        };
        source.rolling.push(
            metrics::parse_prometheus(
                &crate::schema::Schema::enhance_default(),
                "enhance_query_active 0\n",
                Instant::now(),
            )
            .unwrap(),
        );
        let absent = summarize(&[&source], "query");
        assert!(absent.error);
        assert!(absent.last_success.is_none());
        let when = SystemTime::now();
        source.success.insert("query".into(), when);
        source.ok = false;
        let failed = summarize(&[&source], "query");
        assert!(failed.error);
        assert_eq!(failed.last_success, Some(when));
    }
    #[tokio::test]
    async fn separate_ingress_replaces_coordinator_query_and_failed_source_hides_aggregate() {
        use axum::{routing::get, Router};
        use std::sync::{
            atomic::{AtomicBool, AtomicU64, Ordering},
            Arc,
        };
        async fn fixture(
            endpoint: &'static str,
            bytes: u64,
        ) -> (String, tokio::task::JoinHandle<()>, Arc<AtomicBool>) {
            let count = Arc::new(AtomicU64::new(0));
            let failed = Arc::new(AtomicBool::new(false));
            let failure = failed.clone();
            let app = Router::new().route(
                "/metrics",
                get(move || {
                    let n = count.fetch_add(1, Ordering::Relaxed);
                    let status = if failure.load(Ordering::Relaxed) {
                        axum::http::StatusCode::SERVICE_UNAVAILABLE
                    } else {
                        axum::http::StatusCode::OK
                    };
                    async move {
                        (
                            status,
                            format!(
                                r#"process_start_time_seconds 1
enhance_http_request_body_bytes_total{{endpoint="{endpoint}"}} {}
enhance_http_response_body_bytes_total{{endpoint="{endpoint}"}} {}
enhance_http_request_processing_duration_seconds_bucket{{endpoint="{endpoint}",le="1"}} {n}
enhance_http_request_processing_duration_seconds_bucket{{endpoint="{endpoint}",le="+Inf"}} {n}
enhance_http_request_processing_duration_seconds_count{{endpoint="{endpoint}"}} {n}
"#,
                                n * bytes,
                                n * bytes
                            ),
                        )
                    }
                }),
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let task = tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            });
            (url, task, failed)
        }
        // Coordinator deliberately exposes query too: selecting ingress must exclude it.
        let (coordinator, coord_task, _) = fixture("query", 999999).await;
        let (a, task_a, _) = fixture("query", 1024).await;
        let (b, task_b, failed_b) = fixture("query", 2048).await;
        let mut config = Config::from_map(&std::collections::HashMap::new()).unwrap();
        config.scrape_url = coordinator;
        config.query_scrape_urls = vec![format!("{a}/metrics"), format!("{b}/metrics")];
        config.interval = std::time::Duration::from_secs(1);
        let dashboard = Arc::new(tokio::sync::RwLock::new(
            crate::dashboard::DashboardData::new(
                "test".into(),
                config.schema.clone(),
                "test".into(),
                "test".into(),
                crate::host::HostHealth::default(),
            ),
        ));
        let monitor = tokio::spawn(run(config, dashboard.clone()));
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if dashboard
                    .read()
                    .await
                    .entrypoints
                    .get("query")
                    .is_some_and(|e| e.window.upload_per_second.is_some())
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        {
            let data = dashboard.read().await;
            let query = &data.entrypoints["query"];
            assert!(!query.error);
            assert!((2500.0..3600.0).contains(&query.window.upload_per_second.unwrap()));
            assert_eq!(query.window.processing.p50, Some(0.5));
            assert!(data.entrypoints["init"].error);
        }
        failed_b.store(true, Ordering::Relaxed);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if dashboard.read().await.entrypoints["query"].error {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        monitor.abort();
        coord_task.abort();
        task_a.abort();
        task_b.abort();
    }
}
