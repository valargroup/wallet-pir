use super::*;
use axum::{extract::Path, http::StatusCode};

const EXTRA_STYLE: &str = r#"
a{color:var(--gold);text-decoration:none}a:hover{text-decoration:underline}
.breadcrumb{margin:22px 0;color:var(--p62);font-size:13px}
.topology{margin:28px 0;padding:26px;border:1px solid var(--p16)}
.topology h2{font-size:18px;margin-bottom:24px}.coord-link{display:block;max-width:360px;margin:auto;text-align:center}
.node-link{display:block;padding:18px;border:1px solid var(--p22);border-radius:5px;background:var(--p02);min-width:0}
.node-link:hover{border-color:var(--gold);text-decoration:none}.node-link strong{display:block;color:var(--ink);overflow-wrap:anywhere;font-size:14px}
.node-link span{display:block;color:var(--p62);font-size:12px;margin-top:6px}.node-link .ok{color:var(--ok)}.node-link .bad{color:var(--bad)}
.groups{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,300px),1fr));gap:20px}
.group{border:1px solid var(--p16);padding:18px;min-width:0}.group h3{font-size:14px;overflow-wrap:anywhere}.group .summary{font-size:12px;color:var(--p62);margin:8px 0 16px}.replicas{display:grid;gap:12px}
.section-title{font-size:24px;margin:8px 0 12px}.intro{color:var(--p62);font-size:13px;margin-bottom:24px}
.notice{border-left:3px solid var(--warn);padding:12px 16px;background:var(--p05);margin:18px 0;font-size:13px}
details{border:1px solid var(--p16);padding:18px;margin:20px 0}summary{cursor:pointer;color:var(--ink)}details .card{margin-top:18px}.rows .k{overflow-wrap:anywhere}.rows .v{white-space:nowrap}
"#;

fn page(data: &DashboardData, title: &str, body: String) -> String {
    format!("<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><title>{title} · {brand}</title><noscript><meta http-equiv=\"refresh\" content=\"15\"></noscript><style>{STYLE}{EXTRA_STYLE}</style></head><body><main id=\"app\">{masthead}{body}</main><script>{SCRIPT}</script></body></html>", title=escape(title), brand=escape(&data.title), masthead=masthead(data))
}
fn value(values: &BTreeMap<String, f64>, key: &str) -> String {
    values
        .get(key)
        .map(|v| {
            if key.ends_with("bytes") {
                bytes_human((*v).max(0.) as u64)
            } else if key.ends_with("seconds") {
                format!("{} s", format_number(*v))
            } else {
                format_number(*v)
            }
        })
        .unwrap_or_else(|| "Unavailable".into())
}
fn rows(values: &BTreeMap<String, f64>, fields: &[(&str, &str)]) -> String {
    fields
        .iter()
        .map(|(key, label)| {
            format!(
                "<li><span class=\"k\">{}</span><span class=\"v\">{}</span></li>",
                escape(label),
                value(values, key)
            )
        })
        .collect()
}
fn card(title: &str, body: String) -> String {
    format!("<section class=\"card\"><p class=\"eyebrow\">{}</p><ul class=\"rows\">{body}</ul></section>", escape(title))
}
fn breadcrumb(label: &str) -> String {
    format!(
        "<nav class=\"breadcrumb\"><a href=\"/apm/\">Fleet overview</a> / {}</nav>",
        escape(label)
    )
}
fn health_ok(data: &DashboardData) -> bool {
    data.scrape_error.is_none()
        && data.ready_status == Some(200)
        && data.health_status == Some(200)
        && data
            .last_scrape
            .is_some_and(|t| t.elapsed().unwrap_or_default().as_secs() <= 45)
}

pub(super) fn overview(data: &DashboardData) -> String {
    let reachable = data
        .fleet
        .values()
        .filter(|w| w.status() == "reachable")
        .count();
    let redundancy_ok = data.groups.iter().all(|(name, group)| {
        let expected = data
            .fleet
            .values()
            .filter(|worker| &worker.group == name)
            .count();
        group.shards == Some(0.0)
            || (expected > 0 && group.published.is_some_and(|n| n >= expected as f64))
    });
    let healthy = redundancy_ok
        && !data.groups.is_empty()
        && health_ok(data)
        && !data.fleet.is_empty()
        && reachable == data.fleet.len()
        && data.inventory_error.is_none();
    let mut body = String::from("<h2 class=\"section-title\" style=\"margin-top:28px\">Fleet overview</h2><p class=\"intro\">Choose a node to inspect its health, workload, and capacity.</p><section class=\"kpis\">");
    body.push_str(&kpi(
        "Fleet health",
        if healthy { "Healthy" } else { "Attention" },
        "monitoring status",
        if healthy { "" } else { "warn" },
    ));
    body.push_str(&kpi(
        "Published anchor",
        &value(&data.snapshot_gauges, "enhance_published_anchor_height"),
        "block height",
        "",
    ));
    body.push_str(&kpi(
        "Generation",
        &value(&data.snapshot_gauges, "enhance_published_generation"),
        "published",
        "",
    ));
    body.push_str(&kpi(
        "Workers reachable",
        &format!("{reachable} / {}", data.fleet.len()),
        "private metrics checks",
        if reachable == data.fleet.len() {
            ""
        } else {
            "warn"
        },
    ));
    body.push_str("</section>");
    if let Some(error) = &data.inventory_error {
        body.push_str(&format!(
            "<p class=\"notice\">Inventory warning: {}. Showing the last valid inventory.</p>",
            escape(error)
        ));
    }
    if !redundancy_ok {
        body.push_str("<p class=\"notice\">Published replica coverage needs attention. Review the group counts below.</p>");
    }
    if !health_ok(data) {
        body.push_str("<p class=\"notice\">Coordinator monitoring needs attention. Open its details for health and readiness checks.</p>");
    }
    body.push_str(&format!("<section class=\"topology\"><h2>Deployment topology</h2><a class=\"node-link coord-link\" href=\"/apm/coordinator/\"><strong>Coordinator</strong><span>{}</span><span class=\"{}\">{}</span></a><div class=\"trunk\" style=\"margin:auto\"></div><div class=\"groups\">",escape(&data.hostname),if health_ok(data){"ok"}else{"bad"},if health_ok(data){"Healthy"}else{"Needs attention"}));
    let mut groups: std::collections::BTreeSet<&str> =
        data.fleet.values().map(|w| w.group.as_str()).collect();
    groups.extend(data.groups.keys().map(String::as_str));
    for name in groups {
        let group = data.groups.get(name).cloned().unwrap_or_default();
        let role = group.role.as_deref().unwrap_or("unknown").replace('_', " ");
        body.push_str(&format!("<section class=\"group\"><h3>{}</h3><p class=\"summary\">{} · {} assigned shards · {} published replicas</p><div class=\"replicas\">",escape(name),escape(&role),group.shards.map(format_number).unwrap_or_else(||"unknown".into()),group.published.map(format_number).unwrap_or_else(||"unknown".into())));
        let mut count = 0;
        for worker in data.fleet.values().filter(|w| w.group == name) {
            count += 1;
            body.push_str(&format!("<a class=\"node-link\" href=\"/apm/workers/{}/\"><strong>{}</strong><span class=\"{}\">{}</span><span>Last sample: {}</span></a>",escape(&worker.name),escape(&worker.name),if worker.status()=="reachable"{"ok"}else{"bad"},worker.status(),worker.success.map(relative_time).unwrap_or_else(||"awaiting first sample".into())));
        }
        if count == 0 {
            body.push_str(
                "<p class=\"notice\">Worker inventory is unavailable for this group.</p>",
            );
        }
        body.push_str("</div></section>");
    }
    if data.fleet.is_empty() {
        body.push_str("<p class=\"notice\">No workers discovered. Check the dashboard inventory configuration.</p>");
    }
    body.push_str("</div></section><p class=\"note\">Reachability comes from worker metrics. Published replica counts come from the coordinator and describe serving redundancy. Refreshes every 15 seconds.</p>");
    page(data, "Fleet overview", body)
}

pub async fn coordinator_page(State(state): State<SharedDashboard>) -> Html<String> {
    Html(coordinator(&state.read().await.clone()))
}
fn coordinator(data: &DashboardData) -> String {
    let mut body = breadcrumb("Coordinator");
    body.push_str("<h2 class=\"section-title\">Coordinator</h2>");
    body.push_str(&statusbar(data));
    body.push_str(&current_kpis(data));
    body.push_str("<div class=\"grid\">");
    body.push_str(&card(
        "Publication",
        rows(
            &data.snapshot_gauges,
            &[
                ("enhance_publication_blocked", "Blocked (1 = yes)"),
                (
                    "enhance_last_publication_attempt_succeeded",
                    "Last attempt succeeded (1 = yes)",
                ),
                ("enhance_publication_pending_seconds", "Pending duration"),
                (
                    "enhance_publication_target_height_delta",
                    "Blocks remaining",
                ),
            ],
        ),
    ));
    body.push_str(&card(
        "Capacity",
        rows(
            &data.snapshot_gauges,
            &[
                ("enhance_capacity_remaining_rows", "Remaining rows"),
                ("enhance_capacity_forecast_seconds", "Capacity forecast"),
                (
                    "enhance_capacity_observation_age_seconds",
                    "Observation age",
                ),
            ],
        ),
    ));
    body.push_str(&card(
        "Queries",
        rows(
            &data.snapshot_gauges,
            &[
                ("enhance_query_active", "Active"),
                ("enhance_query_waiting", "Waiting"),
                (
                    "enhance_query_primary_success_total",
                    "Primary successes · cumulative",
                ),
                (
                    "enhance_query_fallback_success_total",
                    "Fallback successes · cumulative",
                ),
                ("enhance_query_rejected_total", "Rejected · cumulative"),
                (
                    "enhance_query_worker_failure_total",
                    "Worker failures · cumulative",
                ),
            ],
        ),
    ));
    body.push_str(&host_card(&data.host));
    body.push_str("</div>");
    body.push_str(&active_alerts_card(&data.active_alerts));
    body.push_str(
        "<details id=\"coordinator-secondary\"><summary>Additional coordinator metrics</summary>",
    );
    let all = data
        .snapshot_gauges
        .iter()
        .map(|(name, v)| {
            format!(
                "<li><span class=\"k\">{}</span><span class=\"v\">{}</span></li>",
                escape(&humanize_gauge(name, "enhance_")),
                format_number(*v)
            )
        })
        .collect();
    body.push_str(&card("Metrics", all));
    body.push_str(
        "</details><details id=\"coordinator-alert-history\"><summary>Alert history</summary>",
    );
    body.push_str(&recent_alerts_card(&data.recent_alerts));
    body.push_str("</details>");
    page(data, "Coordinator", body)
}

pub async fn worker_page(
    State(state): State<SharedDashboard>,
    Path(name): Path<String>,
) -> Result<Html<String>, StatusCode> {
    let data = state.read().await.clone();
    let worker = data.fleet.get(&name).ok_or(StatusCode::NOT_FOUND)?;
    Ok(Html(worker_detail(&data, worker)))
}
fn worker_detail(data: &DashboardData, worker: &crate::fleet::Worker) -> String {
    let mut body = breadcrumb(&worker.name);
    body.push_str(&format!("<h2 class=\"section-title\">{}</h2><p class=\"intro\">Replica in {}</p><div class=\"statusbar\">{}<span>Last successful sample: {}</span></div>",escape(&worker.name),escape(&worker.group),chip("metrics",worker.status(),worker.status()=="reachable"),worker.success.map(relative_time).unwrap_or_else(||"never".into())));
    body.push_str(&format!(
        "<p class=\"intro\">Last check: {}</p>",
        worker
            .attempted
            .map(relative_time)
            .unwrap_or_else(|| "not yet checked".into())
    ));
    if let Some(error) = &worker.error {
        body.push_str(&format!(
            "<p class=\"notice\">{}. Values below are from the last successful sample.</p>",
            escape(error)
        ));
    }
    if worker.status() == "stale" {
        body.push_str(
            "<p class=\"notice\">These measurements are stale (more than 45 seconds old).</p>",
        );
    }
    body.push_str("<div class=\"grid\" style=\"margin-top:24px\">");
    body.push_str(&card(
        "Worker state",
        rows(
            &worker.values,
            &[
                ("epoch", "Epoch"),
                ("revision", "Placement revision"),
                ("retained_generations", "Retained generations"),
                ("preparation_busy", "Preparing (1 = yes)"),
                ("queries_in_flight", "Queries in flight"),
            ],
        ),
    ));
    body.push_str(&card(
        "Database memory",
        rows(
            &worker.values,
            &[
                ("live_database_bytes", "Live database"),
                ("published_database_bytes", "Published database"),
                ("latest_database_bytes", "Latest generation database"),
            ],
        ),
    ));
    body.push_str(&card(
        "Modeled capacity",
        rows(
            &worker.values,
            &[
                ("model_total_bytes", "Modeled total"),
                ("model_limit_bytes", "Model limit"),
                ("model_within_limit", "Within limit (1 = yes)"),
            ],
        ),
    ));
    body.push_str("</div><p class=\"note\">Database bytes describe loaded PIR data. Modeled capacity includes reservations and overhead; it is not measured process RAM or total machine memory. Unavailable memory samples can occur during preparation.</p><details id=\"worker-memory-detail\"><summary>Memory breakdown and reservations</summary>");
    body.push_str(&card(
        "Memory details",
        rows(
            &worker.values,
            &[
                ("candidate_database_bytes", "Candidate database"),
                ("query_only_database_bytes", "Query-only database"),
                (
                    "source_reclamation_database_bytes",
                    "Source reclamation database",
                ),
                ("model_union_database_bytes", "Model database union"),
                ("model_growth_reserved_bytes", "Growth reserved"),
                ("model_transition_reserved_bytes", "Transition reserved"),
                ("model_overhead_bytes", "Model overhead"),
            ],
        ),
    ));
    body.push_str("</details>");
    page(data, &worker.name, body)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn detail_navigation_and_unknown_worker() {
        let mut data = super::super::tests::sample();
        data.fleet.insert(
            "worker-1".into(),
            crate::fleet::Worker {
                name: "worker-1".into(),
                group: "group-1".into(),
                url: "http://10.0.0.1:8091".into(),
                ..Default::default()
            },
        );
        let html = overview(&data);
        assert!(html.contains("/apm/workers/worker-1/"));
        assert!(html.contains("Deployment topology"));
        assert!(!html.contains("10.0.0.1"));
        assert!(!html.contains("Additional coordinator metrics"));
        let state = Arc::new(RwLock::new(data));
        assert_eq!(
            worker_page(State(state), Path("unknown".into()))
                .await
                .unwrap_err(),
            StatusCode::NOT_FOUND
        );
    }
}
