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
    run_with_period(config, dashboard, std::time::Duration::from_secs(5)).await;
}

async fn run_with_period(config: Config, dashboard: SharedDashboard, period: std::time::Duration) {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(4))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("entrypoint HTTP client");
    let coordinator = format!("{}{}", config.scrape_url, config.metrics_path);
    let query_urls = if config.query_scrape_urls.is_empty() {
        vec![coordinator.clone()]
    } else {
        config.query_scrape_urls.clone()
    };
    let mut sources: BTreeMap<String, Source> = BTreeMap::new();
    let mut interval = tokio::time::interval(period);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut minute = Instant::now();
    let (mut init_ok, mut query_ok, mut timing_ok) = (true, true, true);
    let mut previous_routers = Vec::new();
    loop {
        interval.tick().await;
        let (router_urls, inventory_ok) = {
            let view = dashboard.read().await;
            (
                view.packing_routers
                    .values()
                    .map(|r| format!("{}/internal/metrics", r.sample.url))
                    .collect::<Vec<_>>(),
                view.packing_inventory_error.is_none(),
            )
        };
        if router_urls != previous_routers {
            timing_ok = false;
            previous_routers = router_urls.clone();
        }
        let desired: std::collections::BTreeSet<_> = std::iter::once(coordinator.clone())
            .chain(query_urls.iter().cloned())
            .chain(router_urls.iter().cloned())
            .collect();
        sources.retain(|url, _| desired.contains(url));
        for url in desired {
            sources.entry(url.clone()).or_insert_with(|| Source {
                url,
                rolling: RollingMetrics::new(config.schema.clone()),
                success: BTreeMap::new(),
                ok: false,
            });
        }
        let mut tasks = tokio::task::JoinSet::new();
        for source in sources.values_mut() {
            source.ok = false;
            let (url, client, schema) = (source.url.clone(), client.clone(), config.schema.clone());
            tasks.spawn(async move {
                let result = async {
                    let response = client
                        .get(&url)
                        .send()
                        .await
                        .map_err(|_| ())?
                        .error_for_status()
                        .map_err(|_| ())?;
                    let body = response.text().await.map_err(|_| ())?;
                    metrics::parse_prometheus(&schema, &body, Instant::now()).map_err(|_| ())
                }
                .await;
                (url, result)
            });
        }
        while let Some(task) = tasks.join_next().await {
            if let Ok((url, Ok(snapshot))) = task {
                let source = sources.get_mut(&url).expect("configured source");
                for endpoint in ["init", "query"] {
                    if snapshot.endpoints.get(endpoint).is_some_and(|e| {
                        e.upload_bytes.is_some()
                            && e.download_bytes.is_some()
                            && !e.processing.buckets.is_empty()
                    }) {
                        source.success.insert(endpoint.into(), SystemTime::now());
                    }
                }
                if source.rolling.latest().is_some_and(|previous| {
                    previous.process_start_time_seconds != snapshot.process_start_time_seconds
                        || snapshot.at.duration_since(previous.at).as_secs() > 12
                }) {
                    if url == coordinator {
                        init_ok = false;
                    }
                    if query_urls.contains(&url) {
                        query_ok = false;
                    }
                    if router_urls.contains(&url) {
                        timing_ok = false;
                    }
                }
                source.rolling.push(snapshot);
                source.ok = true;
            }
        }
        let init_sources = vec![&sources[&coordinator]];
        let query_sources: Vec<_> = query_urls.iter().map(|url| &sources[url]).collect();
        let routers: Vec<_> = router_urls.iter().map(|url| &sources[url]).collect();
        let mut init = summarize(&init_sources, "init");
        let mut query = summarize(&query_sources, "query");
        let timings_available = inventory_ok
            && !routers.is_empty()
            && routers.iter().all(|s| {
                s.ok && ["worker", "packing", "total"].iter().all(|stage| {
                    s.rolling
                        .latest()
                        .and_then(|m| m.endpoints.get(&format!("query_stage_{stage}")))
                        .is_some_and(|e| !e.processing.buckets.is_empty())
                })
            });
        let router_metrics: Vec<_> = routers.iter().map(|s| &s.rolling).collect();
        if timings_available {
            for stage in ["worker", "packing", "total"] {
                query.stages.insert(
                    stage.into(),
                    RollingMetrics::entrypoint(&router_metrics, &format!("query_stage_{stage}"))
                        .processing,
                );
            }
        }
        query.timing_error = !timings_available;
        query.window.processing = query.stages.get("total").cloned().unwrap_or_default();
        init_ok &= !init.error;
        query_ok &= !query.error;
        timing_ok &= timings_available;
        let emit = minute.elapsed().as_secs() >= 60;
        let now = SystemTime::now();
        let mut view = dashboard.write().await;
        for (name, data) in [("init", &mut init), ("query", &mut query)] {
            if let Some(previous) = view.entrypoints.get(name) {
                data.history = previous.history.clone();
            }
            if emit {
                let (entry_sources, traffic_ok) = if name == "init" {
                    (&init_sources, init_ok)
                } else {
                    (&query_sources, query_ok)
                };
                let arrivals = if traffic_ok {
                    entry_sources.iter().try_fold(0.0, |sum, s| {
                        s.rolling.arrivals_period(name, 60).map(|n| sum + n)
                    })
                } else {
                    None
                };
                let mut latencies = BTreeMap::new();
                if name == "init" && init_ok {
                    latencies.insert(
                        "total".into(),
                        RollingMetrics::entrypoint_period(
                            &[&sources[&coordinator].rolling],
                            "init",
                            60,
                        )
                        .processing,
                    );
                } else if name == "query" && timing_ok {
                    for stage in ["worker", "packing", "total"] {
                        latencies.insert(
                            stage.into(),
                            RollingMetrics::entrypoint_period(
                                &router_metrics,
                                &format!("query_stage_{stage}"),
                                60,
                            )
                            .processing,
                        );
                    }
                }
                crate::history::push(
                    &mut data.history,
                    crate::history::Point {
                        at: now,
                        latencies,
                        arrivals,
                    },
                );
            }
        }
        view.entrypoints = BTreeMap::from([("init".into(), init), ("query".into(), query)]);
        if emit {
            minute = Instant::now();
            (init_ok, query_ok, timing_ok) = (true, true, true);
        }
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
        arrivals_10s: if available {
            sources.iter().try_fold(0.0, |sum, s| {
                s.rolling
                    .latest()?
                    .endpoints
                    .get(endpoint)?
                    .arrivals_last_10s
                    .map(|n| sum + n)
            })
        } else {
            None
        },
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
        ..Default::default()
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
        let monitor = tokio::spawn(run_with_period(
            config,
            dashboard.clone(),
            std::time::Duration::from_secs(1),
        ));
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
            assert!(
                query.window.processing.p50.is_none(),
                "ingress latency must not masquerade as router total"
            );
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
