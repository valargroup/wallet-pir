use std::{
    collections::{BTreeMap, VecDeque},
    time::{Duration, Instant, SystemTime},
};

use crate::{fleet::Group, host::HostHealth, metrics::EndpointWindow, schema::Schema, thresholds};

#[derive(Clone, Debug)]
pub struct Alert {
    pub check: String,
    pub observed: String,
    pub threshold: String,
    pub fired_at: SystemTime,
}

#[derive(Clone, Debug)]
pub enum AlertTransition {
    Fired(Alert),
    Recovered(Alert),
}

pub struct AlertEngine {
    schema: Schema,
    active: BTreeMap<String, Alert>,
    recent: VecDeque<(SystemTime, String)>,
    scrape_failures: u32,
    ready_failed_since: Option<Instant>,
    worker_groups: BTreeMap<String, BTreeMap<String, BTreeMap<String, f64>>>,
    serving_groups: BTreeMap<String, Group>,
    serving_groups_enabled: bool,
    fleet_observed: bool,
    fleet_enabled: bool,
    fleet_inventory_error: bool,
    fleet_statuses: BTreeMap<String, String>,
    workers: BTreeMap<String, BTreeMap<String, f64>>,
    tables: BTreeMap<String, BTreeMap<String, f64>>,
    coordinator_gauges: BTreeMap<String, f64>,
    last_rejected_total: Option<f64>,
    last_rejection_at: Option<Instant>,
}

pub struct AlertInput<'a> {
    pub now: Instant,
    pub scrape_ok: bool,
    pub ready_ok: bool,
    pub endpoints: &'a BTreeMap<String, EndpointWindow>,
    pub host: &'a HostHealth,
}

impl AlertEngine {
    pub fn new(schema: Schema) -> Self {
        Self {
            schema,
            active: BTreeMap::new(),
            recent: VecDeque::new(),
            scrape_failures: 0,
            ready_failed_since: None,
            worker_groups: BTreeMap::new(),
            serving_groups: BTreeMap::new(),
            serving_groups_enabled: false,
            fleet_observed: false,
            fleet_enabled: false,
            fleet_inventory_error: false,
            fleet_statuses: BTreeMap::new(),
            workers: BTreeMap::new(),
            tables: BTreeMap::new(),
            coordinator_gauges: BTreeMap::new(),
            last_rejected_total: None,
            last_rejection_at: None,
        }
    }

    pub fn set_worker_groups(
        &mut self,
        worker_groups: BTreeMap<String, BTreeMap<String, BTreeMap<String, f64>>>,
    ) {
        self.worker_groups = worker_groups;
    }

    pub fn set_serving_groups(&mut self, groups: Option<BTreeMap<String, Group>>) {
        self.serving_groups_enabled = groups.is_some();
        self.serving_groups = groups.unwrap_or_default();
    }

    pub fn set_fleet(
        &mut self,
        enabled: bool,
        inventory_error: bool,
        statuses: BTreeMap<String, String>,
    ) {
        self.fleet_observed = true;
        self.fleet_enabled = enabled;
        self.fleet_inventory_error = inventory_error;
        self.fleet_statuses = statuses;
    }

    pub fn set_tables(&mut self, tables: BTreeMap<String, BTreeMap<String, f64>>) {
        self.tables = tables;
    }

    pub fn set_workers(&mut self, workers: BTreeMap<String, BTreeMap<String, f64>>) {
        self.workers = workers;
    }

    pub fn set_coordinator_gauges(&mut self, gauges: BTreeMap<String, f64>) {
        self.coordinator_gauges = gauges;
    }

    pub fn evaluate(&mut self, input: AlertInput<'_>) -> Vec<AlertTransition> {
        self.scrape_failures = if input.scrape_ok {
            0
        } else {
            self.scrape_failures.saturating_add(1)
        };
        self.ready_failed_since = if input.ready_ok {
            None
        } else {
            Some(self.ready_failed_since.unwrap_or(input.now))
        };

        let mut conditions = BTreeMap::new();
        conditions.insert(
            "scrape_failure".to_string(),
            (
                self.scrape_failures >= thresholds::SCRAPE_FAILURE_TICKS,
                format!("{} consecutive failed ticks", self.scrape_failures),
                format!("{} ticks", thresholds::SCRAPE_FAILURE_TICKS),
            ),
        );
        let ready_failed_for = self
            .ready_failed_since
            .map(|since| input.now.duration_since(since))
            .unwrap_or_default();
        conditions.insert(
            "ready".to_string(),
            (
                !input.ready_ok
                    && ready_failed_for >= Duration::from_secs(thresholds::READY_FAILURE_SECONDS),
                format!("non-200 for {}s", ready_failed_for.as_secs()),
                format!("{}s", thresholds::READY_FAILURE_SECONDS),
            ),
        );

        if self.schema.prefix == "enhance" && input.scrape_ok {
            // Pool placement has no legacy group assignments, but still requires
            // independent worker inventory and reachability observation.
            if self.fleet_observed {
                conditions.insert(
                    "enhance_worker_monitoring_unconfigured".to_string(),
                    (
                        !self.fleet_enabled,
                        "no private worker inventory configured".into(),
                        "worker inventory configured".into(),
                    ),
                );
            }
            if self.serving_groups_enabled {
                conditions.insert(
                    "enhance_group_observation_missing".to_string(),
                    (
                        self.serving_groups.is_empty(),
                        "no serving group observed".into(),
                        "at least one published group".into(),
                    ),
                );
                for (name, group) in &self.serving_groups {
                    if group.shards.is_some_and(|shards| shards > 0.0) {
                        let published = group.published.unwrap_or(0.0);
                        conditions.insert(
                            format!("enhance_group_redundancy_{name}"),
                            (
                                published < 2.0,
                                format!("{published:.0}/2 replicas published"),
                                "2 replicas published".into(),
                            ),
                        );
                    }
                }
            }
            let gauge = |name: &str| self.coordinator_gauges.get(name).copied();
            if let Some(failed) = gauge("enhance_ingestion_failed") {
                conditions.insert(
                    "enhance_ingestion_failed".to_string(),
                    (
                        failed >= 1.0,
                        format!("ingestion_failed={failed:.0}"),
                        "0".into(),
                    ),
                );
            }
            if let Some(blocked) = gauge("enhance_publication_blocked") {
                conditions.insert(
                    "enhance_publication_blocked".to_string(),
                    (
                        blocked >= 1.0,
                        format!("publication_blocked={blocked:.0}"),
                        "0".into(),
                    ),
                );
            }
            if let (Some(available), Some(current), Some(pending)) = (
                gauge("enhance_publication_target_available"),
                gauge("enhance_publication_target_current"),
                gauge("enhance_publication_pending_seconds"),
            ) {
                conditions.insert(
                    "enhance_publication_stale".to_string(),
                    (
                        available >= 1.0
                            && current < 1.0
                            && pending > thresholds::PUBLICATION_STALE_SECONDS as f64,
                        format!("unpublished target pending for {pending:.0}s"),
                        format!("> {}s", thresholds::PUBLICATION_STALE_SECONDS),
                    ),
                );
            }
            if let Some(rejected) = gauge("enhance_query_rejected_total") {
                if self.last_rejected_total.is_some_and(|last| rejected > last) {
                    self.last_rejection_at = Some(input.now);
                }
                self.last_rejected_total = Some(rejected);
                conditions.insert(
                    "enhance_query_rejections".to_string(),
                    (
                        self.last_rejection_at.is_some_and(|at| {
                            input.now.duration_since(at)
                                < Duration::from_secs(thresholds::REJECTION_ALERT_HOLD_SECONDS)
                        }),
                        format!("query_rejected_total={rejected:.0}"),
                        format!(
                            "no new rejections for {}s",
                            thresholds::REJECTION_ALERT_HOLD_SECONDS
                        ),
                    ),
                );
            }
        }

        if self.schema.prefix == "enhance" && self.fleet_enabled {
            conditions.insert(
                "enhance_worker_inventory_error".to_string(),
                (
                    self.fleet_inventory_error,
                    "private worker inventory unreadable or invalid".into(),
                    "valid private worker inventory".into(),
                ),
            );
            for (name, status) in &self.fleet_statuses {
                conditions.insert(
                    format!("enhance_worker_reachability_{name}"),
                    (
                        status == "unreachable" || status == "stale",
                        format!("private worker {status}"),
                        "private worker reachable".into(),
                    ),
                );
            }
        }

        for (endpoint, window) in input.endpoints {
            let endpoint = endpoint.as_str();
            if self.schema.is_informational(endpoint) {
                continue;
            }
            let window = window.clone();
            conditions.insert(
                format!("{endpoint}_5xx"),
                (
                    window.requests >= thresholds::HTTP_5XX_MIN_REQUESTS
                        && window.error_ratio > thresholds::HTTP_5XX_RATIO,
                    format!(
                        "{:.1}% over {:.0} requests",
                        window.error_ratio * 100.0,
                        window.requests
                    ),
                    format!(
                        "> {:.0}% over 5m, min {:.0}",
                        thresholds::HTTP_5XX_RATIO * 100.0,
                        thresholds::HTTP_5XX_MIN_REQUESTS
                    ),
                ),
            );
            let latency_check = format!("{endpoint}_high_latency");
            let uses_processing =
                self.schema.uses_processing(endpoint) || window.processing_available;
            let latency_label = if uses_processing {
                "post-body server p99"
            } else {
                "p99"
            };
            let latency_threshold = self
                .schema
                .latency_budget(endpoint)
                .unwrap_or(thresholds::DEFAULT_LATENCY_P99_SECONDS);
            let latency = window.alert_latency(uses_processing);
            conditions.insert(
                latency_check,
                (
                    latency.samples >= thresholds::LATENCY_MIN_REQUESTS
                        && latency.p99.is_some_and(|p99| p99 > latency_threshold),
                    latency
                        .p99
                        .map(|p99| {
                            format!(
                                "{latency_label} {p99:.3}s over {:.0} samples",
                                latency.samples
                            )
                        })
                        .unwrap_or_else(|| format!("{latency_label} unavailable")),
                    format!(
                        "{latency_label} > {latency_threshold:.3}s over 5m, min {:.0}",
                        thresholds::LATENCY_MIN_REQUESTS
                    ),
                ),
            );
        }

        conditions.insert(
            "disk_usage".to_string(),
            (
                input.host.disk_used_ratio > thresholds::DISK_USED_RATIO,
                format!("{:.1}% used", input.host.disk_used_ratio * 100.0),
                format!("> {:.0}%", thresholds::DISK_USED_RATIO * 100.0),
            ),
        );
        conditions.insert(
            "memory_available".to_string(),
            (
                input.host.available_memory_bytes < thresholds::MEMORY_AVAILABLE_BYTES,
                format!(
                    "{} MiB available",
                    input.host.available_memory_bytes / 1024 / 1024
                ),
                format!("< {} MiB", thresholds::MEMORY_AVAILABLE_BYTES / 1024 / 1024),
            ),
        );

        for (table, gauges) in &self.tables {
            if let (Some(positions), Some(groups), Some(shards), Some(per_shard)) = (
                gauges.get("positions"),
                gauges.get("pool_workers"),
                gauges.get("shards_per_worker"),
                gauges.get("shard_positions"),
            ) {
                let capacity = groups * shards * per_shard;
                if capacity > 0.0 {
                    for (suffix, threshold) in [("warning", 0.75), ("critical", 0.90)] {
                        conditions.insert(
                            format!("capacity_{table}_{suffix}"),
                            (
                                positions / capacity >= threshold,
                                format!(
                                    "{positions:.0}/{capacity:.0} positions ({:.1}%)",
                                    positions / capacity * 100.0
                                ),
                                format!(
                                    ">= {:.0}% of configured range capacity",
                                    threshold * 100.0
                                ),
                            ),
                        );
                    }
                    conditions.insert(
                        format!("capacity_{table}_ceiling"),
                        (
                            *groups >= 4.0 && positions / capacity >= 0.80,
                            format!("{groups:.0} groups; automatic fleet ceiling reached"),
                            "four groups at >= 80%".into(),
                        ),
                    );
                }
            }
        }
        for (table, groups) in &self.worker_groups {
            for (group, gauges) in groups {
                let configured = gauges.get("configured_replicas").copied().unwrap_or(0.0);
                let ready = gauges.get("ready_replicas").copied().unwrap_or(0.0);
                conditions.insert(
                    format!("worker_group_{table}_{group}"),
                    (
                        configured > 0.0 && ready < configured,
                        format!("{ready:.0}/{configured:.0} replicas ready"),
                        "all configured replicas ready".to_string(),
                    ),
                );
            }
        }
        for (worker, gauges) in &self.workers {
            let rss = gauges.get("process_rss_bytes").copied().unwrap_or(0.0);
            conditions.insert(
                format!("worker_rss_{worker}"),
                (
                    rss > thresholds::WORKER_RSS_LIMIT_BYTES as f64,
                    format!("{:.0} MiB resident", rss / 1024.0 / 1024.0),
                    format!(
                        "<= {} MiB",
                        thresholds::WORKER_RSS_LIMIT_BYTES / 1024 / 1024
                    ),
                ),
            );
        }

        let mut transitions = Vec::new();
        for (check, (firing, observed, threshold)) in conditions {
            match (firing, self.active.get(&check).cloned()) {
                (true, None) => {
                    let alert = Alert {
                        check: check.clone(),
                        observed,
                        threshold,
                        fired_at: SystemTime::now(),
                    };
                    self.active.insert(check, alert.clone());
                    self.record(format!("FIRED {}: {}", alert.check, alert.observed));
                    transitions.push(AlertTransition::Fired(alert));
                }
                (false, Some(alert)) => {
                    self.active.remove(&check);
                    self.record(format!("RECOVERED {}", alert.check));
                    transitions.push(AlertTransition::Recovered(alert));
                }
                _ => {}
            }
        }
        transitions
    }

    fn record(&mut self, message: String) {
        self.recent.push_front((SystemTime::now(), message));
        self.recent.truncate(20);
    }

    pub fn active(&self) -> Vec<Alert> {
        self.active.values().cloned().collect()
    }

    pub fn recent(&self) -> Vec<(SystemTime, String)> {
        self.recent.iter().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::LatencyWindow;

    fn healthy_host() -> HostHealth {
        HostHealth {
            load_one: 0.1,
            load_five: 0.1,
            load_fifteen: 0.1,
            total_memory_bytes: 8 * 1024 * 1024 * 1024,
            available_memory_bytes: 4 * 1024 * 1024 * 1024,
            disk_total_bytes: 100,
            disk_available_bytes: 50,
            disk_used_ratio: 0.5,
            data_dir: "/data".into(),
        }
    }

    #[test]
    fn v6_ingestion_publication_and_rejection_alerts_fire_and_recover() {
        let mut engine = AlertEngine::new(Schema::enhance_default());
        let host = healthy_host();
        let endpoints = BTreeMap::new();
        let now = Instant::now();
        let gauges = |failed, blocked, pending, rejected| {
            BTreeMap::from([
                ("enhance_ingestion_failed".into(), failed),
                ("enhance_publication_blocked".into(), blocked),
                ("enhance_publication_target_available".into(), 1.0),
                ("enhance_publication_target_current".into(), 0.0),
                ("enhance_publication_pending_seconds".into(), pending),
                ("enhance_query_rejected_total".into(), rejected),
            ])
        };
        let input = |at| AlertInput {
            now: at,
            scrape_ok: true,
            ready_ok: true,
            endpoints: &endpoints,
            host: &host,
        };
        engine.set_coordinator_gauges(gauges(0.0, 0.0, 10.0, 0.0));
        assert!(engine.evaluate(input(now)).is_empty());
        engine.set_coordinator_gauges(gauges(1.0, 1.0, 301.0, 1.0));
        let fired = engine.evaluate(input(now + Duration::from_secs(15)));
        assert_eq!(fired.len(), 4);
        assert_eq!(engine.active().len(), 4);
        engine.set_coordinator_gauges(gauges(0.0, 0.0, 0.0, 1.0));
        let recovered = engine.evaluate(input(now + Duration::from_secs(316)));
        assert_eq!(recovered.len(), 4);
        assert!(engine.active().is_empty());
    }

    #[test]
    fn capacity_counts_groups_not_replicas_and_recovers_after_append() {
        let mut engine = AlertEngine::new(Schema::enhance_default());
        let host = healthy_host();
        let endpoints = BTreeMap::new();
        let gauges = |groups| {
            BTreeMap::from([(
                "enhance".into(),
                BTreeMap::from([
                    ("positions".into(), 1_100_000.0),
                    ("pool_workers".into(), groups),
                    ("shards_per_worker".into(), 16.0),
                    ("shard_positions".into(), 73728.0),
                ]),
            )])
        };
        engine.set_tables(gauges(1.0));
        let events = engine.evaluate(AlertInput {
            now: Instant::now(),
            scrape_ok: true,
            ready_ok: true,
            endpoints: &endpoints,
            host: &host,
        });
        assert_eq!(events.len(), 2);
        assert!(events
            .iter()
            .all(|e| matches!(e, AlertTransition::Fired(_))));
        engine.set_tables(gauges(2.0));
        let events = engine.evaluate(AlertInput {
            now: Instant::now(),
            scrape_ok: true,
            ready_ok: true,
            endpoints: &endpoints,
            host: &host,
        });
        assert_eq!(events.len(), 2);
        assert!(events
            .iter()
            .all(|e| matches!(e, AlertTransition::Recovered(_))));
    }

    #[test]
    fn fires_once_per_episode_and_recovers_once() {
        let now = Instant::now();
        let host = healthy_host();
        let endpoints = BTreeMap::new();
        let mut engine = AlertEngine::new(Schema::enhance_default());
        assert!(engine
            .evaluate(AlertInput {
                now,
                scrape_ok: false,
                ready_ok: true,
                endpoints: &endpoints,
                host: &host,
            })
            .is_empty());
        assert!(matches!(
            engine
                .evaluate(AlertInput {
                    now: now + Duration::from_secs(15),
                    scrape_ok: false,
                    ready_ok: true,
                    endpoints: &endpoints,
                    host: &host,
                })
                .as_slice(),
            [AlertTransition::Fired(_)]
        ));
        assert!(engine
            .evaluate(AlertInput {
                now: now + Duration::from_secs(30),
                scrape_ok: false,
                ready_ok: true,
                endpoints: &endpoints,
                host: &host,
            })
            .is_empty());
        assert!(matches!(
            engine
                .evaluate(AlertInput {
                    now: now + Duration::from_secs(45),
                    scrape_ok: true,
                    ready_ok: true,
                    endpoints: &endpoints,
                    host: &host,
                })
                .as_slice(),
            [AlertTransition::Recovered(_)]
        ));
    }

    #[test]
    fn ready_requires_five_continuous_minutes() {
        let now = Instant::now();
        let host = healthy_host();
        let endpoints = BTreeMap::new();
        let mut engine = AlertEngine::new(Schema::enhance_default());
        engine.evaluate(AlertInput {
            now,
            scrape_ok: true,
            ready_ok: false,
            endpoints: &endpoints,
            host: &host,
        });
        assert!(engine
            .evaluate(AlertInput {
                now: now + Duration::from_secs(299),
                scrape_ok: true,
                ready_ok: false,
                endpoints: &endpoints,
                host: &host,
            })
            .is_empty());
        assert!(matches!(
            engine
                .evaluate(AlertInput {
                    now: now + Duration::from_secs(300),
                    scrape_ok: true,
                    ready_ok: false,
                    endpoints: &endpoints,
                    host: &host,
                })
                .as_slice(),
            [AlertTransition::Fired(_)]
        ));
    }

    #[test]
    fn applies_minimum_volume_to_thresholds() {
        let now = Instant::now();
        let host = healthy_host();
        let mut endpoints = BTreeMap::new();
        endpoints.insert(
            "metadata".into(),
            EndpointWindow {
                requests: 9.0,
                errors_5xx: 9.0,
                error_ratio: 1.0,
                observed: LatencyWindow {
                    samples: 9.0,
                    p99: Some(9.0),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
        let mut engine = AlertEngine::new(Schema::enhance_default());
        assert!(engine
            .evaluate(AlertInput {
                now,
                scrape_ok: true,
                ready_ok: true,
                endpoints: &endpoints,
                host: &host,
            })
            .is_empty());
        endpoints.get_mut("metadata").unwrap().requests = 20.0;
        endpoints.get_mut("metadata").unwrap().observed.samples = 20.0;
        let fired = engine.evaluate(AlertInput {
            now: now + Duration::from_secs(15),
            scrape_ok: true,
            ready_ok: true,
            endpoints: &endpoints,
            host: &host,
        });
        assert_eq!(fired.len(), 2);
    }

    #[test]
    fn observed_total_is_informational_but_post_body_latency_pages() {
        let now = Instant::now();
        let host = healthy_host();
        let mut endpoints = BTreeMap::new();
        endpoints.insert(
            "query".into(),
            EndpointWindow {
                requests: 20.0,
                observed: LatencyWindow {
                    samples: 20.0,
                    p99: Some(10.0),
                    ..Default::default()
                },
                processing: LatencyWindow {
                    samples: 20.0,
                    p99: Some(0.3),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
        let mut engine = AlertEngine::new(Schema::enhance_default());
        assert!(engine
            .evaluate(AlertInput {
                now,
                scrape_ok: true,
                ready_ok: true,
                endpoints: &endpoints,
                host: &host,
            })
            .is_empty());

        endpoints.get_mut("query").unwrap().requests = 100.0;
        endpoints.get_mut("query").unwrap().processing.samples = 19.0;
        endpoints.get_mut("query").unwrap().processing.p99 = Some(6.0);
        assert!(engine
            .evaluate(AlertInput {
                now: now + Duration::from_secs(15),
                scrape_ok: true,
                ready_ok: true,
                endpoints: &endpoints,
                host: &host,
            })
            .is_empty());

        endpoints.get_mut("query").unwrap().processing.samples = 20.0;
        let fired = engine.evaluate(AlertInput {
            now: now + Duration::from_secs(30),
            scrape_ok: true,
            ready_ok: true,
            endpoints: &endpoints,
            host: &host,
        });
        let [AlertTransition::Fired(alert)] = fired.as_slice() else {
            panic!("expected one processing-latency alert");
        };
        assert_eq!(alert.check, "query_high_latency");
        assert!(alert.observed.contains("post-body server p99"));
        assert!(alert.threshold.contains("5.000s"));
    }

    #[test]
    fn discovered_and_informational_endpoints_follow_their_rules() {
        let now = Instant::now();
        let host = healthy_host();
        let mut endpoints = BTreeMap::new();
        // Not configured anywhere: pages on post-body p99 at the default budget.
        endpoints.insert(
            "witness_query".into(),
            EndpointWindow {
                requests: 50.0,
                observed: LatencyWindow {
                    samples: 50.0,
                    p99: Some(0.1),
                    ..Default::default()
                },
                processing: LatencyWindow {
                    samples: 50.0,
                    p99: Some(1.5),
                    ..Default::default()
                },
                processing_available: true,
                ..Default::default()
            },
        );
        // Informational: 100% 5xx and never pages.
        endpoints.insert(
            "health".into(),
            EndpointWindow {
                requests: 50.0,
                errors_5xx: 50.0,
                error_ratio: 1.0,
                ..Default::default()
            },
        );
        let mut engine = AlertEngine::new(Schema::enhance_default());
        let fired = engine.evaluate(AlertInput {
            now,
            scrape_ok: true,
            ready_ok: true,
            endpoints: &endpoints,
            host: &host,
        });
        let [AlertTransition::Fired(alert)] = fired.as_slice() else {
            panic!("expected exactly one alert, got {}", fired.len());
        };
        assert_eq!(alert.check, "witness_query_high_latency");
        assert!(alert.observed.contains("post-body server p99"));
    }

    #[test]
    fn budgets_come_from_the_schema() {
        let now = Instant::now();
        let host = healthy_host();
        let mut endpoints = BTreeMap::new();
        endpoints.insert(
            "init".into(),
            EndpointWindow {
                requests: 50.0,
                observed: LatencyWindow {
                    samples: 50.0,
                    p99: Some(1.5),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
        // Default schema: init is budgeted at 2.0s, so 1.5s is quiet.
        let mut engine = AlertEngine::new(Schema::enhance_default());
        assert!(engine
            .evaluate(AlertInput {
                now,
                scrape_ok: true,
                ready_ok: true,
                endpoints: &endpoints,
                host: &host,
            })
            .is_empty());
        // Same numbers under a schema with the 1.0s default budget page.
        let tight = Schema::new(
            "enhance",
            vec!["init".to_string()],
            Default::default(),
            Default::default(),
            1.0,
            Default::default(),
        )
        .unwrap();
        let mut engine = AlertEngine::new(tight);
        let fired = engine.evaluate(AlertInput {
            now,
            scrape_ok: true,
            ready_ok: true,
            endpoints: &endpoints,
            host: &host,
        });
        let [AlertTransition::Fired(alert)] = fired.as_slice() else {
            panic!("expected one latency alert");
        };
        assert_eq!(alert.check, "init_high_latency");
    }

    #[test]
    fn missing_processing_metrics_do_not_fall_back_to_observed_latency() {
        let now = Instant::now();
        let host = healthy_host();
        let mut endpoints = BTreeMap::new();
        endpoints.insert(
            "query".into(),
            EndpointWindow {
                requests: 100.0,
                observed: LatencyWindow {
                    samples: 100.0,
                    p99: Some(10.0),
                    ..Default::default()
                },
                ..Default::default()
            },
        );

        let mut engine = AlertEngine::new(Schema::enhance_default());
        assert!(engine
            .evaluate(AlertInput {
                now,
                scrape_ok: true,
                ready_ok: true,
                endpoints: &endpoints,
                host: &host,
            })
            .is_empty());
    }

    #[test]
    fn loss_of_shard_group_redundancy_pages_without_failing_readiness() {
        let now = Instant::now();
        let host = healthy_host();
        let endpoints = BTreeMap::new();
        let mut engine = AlertEngine::new(Schema::enhance_default());
        engine.set_worker_groups(BTreeMap::from([(
            "enhance".to_string(),
            BTreeMap::from([(
                "group-1".to_string(),
                BTreeMap::from([
                    ("configured_replicas".to_string(), 2.0),
                    ("ready_replicas".to_string(), 1.0),
                ]),
            )]),
        )]));

        let fired = engine.evaluate(AlertInput {
            now,
            scrape_ok: true,
            ready_ok: true,
            endpoints: &endpoints,
            host: &host,
        });
        let [AlertTransition::Fired(alert)] = fired.as_slice() else {
            panic!("expected one redundancy alert");
        };
        assert_eq!(alert.check, "worker_group_enhance_group-1");
        assert_eq!(alert.observed, "1/2 replicas ready");
    }

    #[test]
    fn v6_published_replica_loss_fires_from_current_health_and_recovers() {
        let now = Instant::now();
        let host = healthy_host();
        let endpoints = BTreeMap::new();
        let metrics = "enhance_group_assigned_shards{group=\"shard-group-01\"} 1\n\
                       enhance_group_role{group=\"shard-group-01\",role=\"active\"} 1\n";
        let mut engine = AlertEngine::new(Schema::enhance_default());
        let input = || AlertInput {
            now,
            scrape_ok: true,
            ready_ok: true,
            endpoints: &endpoints,
            host: &host,
        };
        engine.set_serving_groups(Some(crate::fleet::groups(
            metrics,
            Some(r#"{"published_replica_counts":{"shard-group-01":2}}"#),
        )));
        assert!(engine.evaluate(input()).is_empty());

        engine.set_serving_groups(Some(crate::fleet::groups(
            metrics,
            Some(r#"{"published_replica_counts":{"shard-group-01":1}}"#),
        )));
        let fired = engine.evaluate(input());
        let [AlertTransition::Fired(alert)] = fired.as_slice() else {
            panic!("expected v6 replica-loss alert");
        };
        assert_eq!(alert.check, "enhance_group_redundancy_shard-group-01");
        assert_eq!(alert.observed, "1/2 replicas published");

        engine.set_serving_groups(Some(crate::fleet::groups(
            metrics,
            Some(r#"{"published_replica_counts":{"shard-group-01":2}}"#),
        )));
        let recovered = engine.evaluate(input());
        let [AlertTransition::Recovered(alert)] = recovered.as_slice() else {
            panic!("expected v6 replica-recovery alert");
        };
        assert_eq!(alert.check, "enhance_group_redundancy_shard-group-01");
    }

    #[test]
    fn v6_missing_group_data_alerts_only_when_v6_is_enabled() {
        let now = Instant::now();
        let host = healthy_host();
        let endpoints = BTreeMap::new();
        let mut engine = AlertEngine::new(Schema::enhance_default());
        let input = || AlertInput {
            now,
            scrape_ok: true,
            ready_ok: true,
            endpoints: &endpoints,
            host: &host,
        };
        assert!(engine.evaluate(input()).is_empty());
        engine.set_serving_groups(Some(BTreeMap::new()));
        let fired = engine.evaluate(input());
        let [AlertTransition::Fired(alert)] = fired.as_slice() else {
            panic!("expected missing group alert");
        };
        assert_eq!(alert.check, "enhance_group_observation_missing");
    }

    #[test]
    fn private_worker_loss_alerts_even_when_published_routes_still_show_two() {
        let now = Instant::now();
        let host = healthy_host();
        let endpoints = BTreeMap::new();
        let mut engine = AlertEngine::new(Schema::enhance_default());
        let input = || AlertInput {
            now,
            scrape_ok: true,
            ready_ok: true,
            endpoints: &endpoints,
            host: &host,
        };
        let status = |value: &str| BTreeMap::from([("worker-01".into(), value.into())]);
        engine.set_fleet(true, false, status("reachable"));
        assert!(engine.evaluate(input()).is_empty());
        engine.set_fleet(true, false, status("unreachable"));
        let fired = engine.evaluate(input());
        let [AlertTransition::Fired(alert)] = fired.as_slice() else {
            panic!("expected worker reachability alert");
        };
        assert_eq!(alert.check, "enhance_worker_reachability_worker-01");
        engine.set_fleet(true, false, status("reachable"));
        let recovered = engine.evaluate(input());
        let [AlertTransition::Recovered(alert)] = recovered.as_slice() else {
            panic!("expected worker recovery alert");
        };
        assert_eq!(alert.check, "enhance_worker_reachability_worker-01");
    }

    #[test]
    fn pool_without_legacy_groups_still_requires_private_worker_inventory() {
        let now = Instant::now();
        let host = healthy_host();
        let endpoints = BTreeMap::new();
        let mut engine = AlertEngine::new(Schema::enhance_default());
        engine.set_serving_groups(None);
        engine.set_fleet(false, false, BTreeMap::new());
        let fired = engine.evaluate(AlertInput {
            now,
            scrape_ok: true,
            ready_ok: true,
            endpoints: &endpoints,
            host: &host,
        });
        let [AlertTransition::Fired(alert)] = fired.as_slice() else {
            panic!("expected only missing inventory alert for pool mode");
        };
        assert_eq!(alert.check, "enhance_worker_monitoring_unconfigured");
    }

    #[test]
    fn v6_without_private_worker_inventory_alerts() {
        let now = Instant::now();
        let host = healthy_host();
        let endpoints = BTreeMap::new();
        let mut engine = AlertEngine::new(Schema::enhance_default());
        engine.set_serving_groups(Some(BTreeMap::from([(
            "shard-group-01".into(),
            Group {
                shards: Some(1.0),
                published: Some(2.0),
                ..Default::default()
            },
        )])));
        engine.set_fleet(false, false, BTreeMap::new());
        let fired = engine.evaluate(AlertInput {
            now,
            scrape_ok: true,
            ready_ok: true,
            endpoints: &endpoints,
            host: &host,
        });
        let [AlertTransition::Fired(alert)] = fired.as_slice() else {
            panic!("expected unconfigured worker monitoring alert");
        };
        assert_eq!(alert.check, "enhance_worker_monitoring_unconfigured");
    }
}
