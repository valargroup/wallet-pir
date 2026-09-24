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
            if key.ends_with("bytes") || key.ends_with("bytes_total") {
                bytes_human((*v).max(0.) as u64)
            } else if key.ends_with("microseconds_total") {
                format!("{:.3} s", v / 1_000_000.0)
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
    let domain_mode = data.packing_enabled || data.placement.is_some();
    let redundancy_ok = if domain_mode {
        data.placement.as_ref().is_some_and(|p| {
            !p.domains.is_empty()
                && p.domains.iter().all(|(id, workers)| {
                    !workers.is_empty()
                        && workers.iter().all(|w| data.fleet.contains_key(w))
                        && (!data.packing_enabled
                            || data.packing_routers.values().any(|r| r.serves(*id)))
                })
        }) && !data.placement_error
            && data
                .placement_success
                .is_some_and(|t| t.elapsed().is_ok_and(|age| age.as_secs() <= 45))
    } else {
        data.groups.iter().all(|(name, group)| {
            let expected = data
                .fleet
                .values()
                .filter(|worker| &worker.group == name)
                .count();
            group.shards == Some(0.0)
                || (expected > 0 && group.published.is_some_and(|n| n >= expected as f64))
        })
    };
    let packing_ok = !data.packing_enabled
        || (!data.packing_routers.is_empty()
            && data.packing_inventory_error.is_none()
            && data.packing_routers.values().all(|r| r.status() == "ready"));
    let healthy = packing_ok
        && redundancy_ok
        && (domain_mode || !data.groups.is_empty())
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
        body.push_str("<p class=\"notice\">Domain placement or replica coverage needs attention. Review the assignments below.</p>");
    }
    if !health_ok(data) {
        body.push_str("<p class=\"notice\">Coordinator monitoring needs attention. Open its details for health and readiness checks.</p>");
    }
    if let Some(error) = &data.packing_inventory_error {
        body.push_str(&format!(
            "<p class=\"notice\">{}. Showing the last valid packing router inventory.</p>",
            escape(error)
        ));
    }
    body.push_str(&entrypoint_apm(data));
    body.push_str(&format!("<section class=\"topology\"><h2>Deployment topology</h2><a class=\"node-link coord-link\" href=\"/apm/coordinator/\"><strong>Coordinator</strong><span>{}</span><span class=\"{}\">{}</span></a><div class=\"trunk\" style=\"margin:auto\"></div>",escape(&data.hostname),if health_ok(data){"ok"}else{"bad"},if health_ok(data){"Healthy"}else{"Needs attention"}));
    if domain_mode {
        body.push_str(&domain_topology(data));
    } else {
        body.push_str("<div class=\"groups\">");
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
        body.push_str("</div>");
    }
    body.push_str("</section><p class=\"note\">Worker assignments come from coordinator domain placement; packing-router assignments come from its router inventory. A shared host can appear in more than one domain. Refreshes every 15 seconds.</p>");
    page(data, "Fleet overview", body)
}

fn packing_link(router: &crate::packing_fleet::PackingRouter) -> String {
    let sample = &router.sample;
    format!("<a class=\"node-link\" href=\"/apm/packing-routers/{}/\"><strong>{}</strong><span class=\"{}\">{}</span><span>Last sample: {}</span></a>", escape(&sample.name), escape(&sample.name), if router.status() == "ready" { "ok" } else { "bad" }, router.status(), sample.success.map(relative_time).unwrap_or_else(|| "awaiting first sample".into()))
}
fn worker_link(data: &DashboardData, name: &str) -> String {
    match data.fleet.get(name) {
        Some(worker) => format!("<a class=\"node-link\" href=\"/apm/workers/{}/\"><strong>{}</strong><span class=\"{}\">{}</span><span>Last sample: {}</span></a>", escape(name), escape(name), if worker.status() == "reachable" { "ok" } else { "bad" }, worker.status(), worker.success.map(relative_time).unwrap_or_else(|| "awaiting first sample".into())),
        None => format!("<div class=\"node-link\"><strong>{}</strong><span class=\"bad\">Worker monitoring unavailable</span></div>", escape(name)),
    }
}
fn domain_topology(data: &DashboardData) -> String {
    let mut out = String::new();
    let fresh = !data.placement_error
        && data
            .placement_success
            .is_some_and(|t| t.elapsed().is_ok_and(|age| age.as_secs() <= 45));
    if let Some(placement) = &data.placement {
        if !fresh {
            out.push_str("<p class=\"notice\">Domain placement is stale or unavailable. Showing the last successful assignment.</p>");
        }
        out.push_str(&format!(
            "<p class=\"summary\">Placement revision {} · Last sample: {}</p>",
            placement
                .revision
                .map(|n| n.to_string())
                .unwrap_or_else(|| "unknown".into()),
            data.placement_success
                .map(relative_time)
                .unwrap_or_else(|| "never".into())
        ));
        out.push_str("<div class=\"groups\">");
        for (id, workers) in &placement.domains {
            out.push_str(&format!("<section class=\"group domain-group\" data-domain=\"{id}\"><h3>Shard / domain {id}</h3><p class=\"summary\">Packing routers</p><div class=\"replicas\">"));
            let routers: Vec<_> = data
                .packing_routers
                .values()
                .filter(|r| r.serves(*id))
                .collect();
            for router in &routers {
                out.push_str(&packing_link(router));
            }
            if routers.is_empty() {
                out.push_str("<p class=\"notice\">Packing-router assignment unavailable.</p>");
            }
            out.push_str(&format!("</div><div class=\"trunk\" style=\"margin:auto\"></div><p class=\"summary\">Evaluation workers · {} replicas</p><div class=\"replicas\">", workers.len()));
            for name in workers {
                out.push_str(&worker_link(data, name));
            }
            if workers.is_empty() {
                out.push_str("<p class=\"notice\">No evaluation workers assigned.</p>");
            }
            out.push_str("</div></section>");
        }
        out.push_str("</div>");
        if placement.domains.is_empty() {
            out.push_str("<p class=\"notice\">No domains are currently placed.</p>");
        }
    } else {
        out.push_str("<p class=\"notice\">Domain placement is unavailable. Registered hosts are listed below without inferred assignments.</p>");
    }
    let unused_routers: Vec<_> = data
        .packing_routers
        .values()
        .filter(|r| {
            data.placement
                .as_ref()
                .is_none_or(|p| !p.domains.keys().any(|id| r.serves(*id)))
        })
        .collect();
    let unused_workers: Vec<_> = data
        .fleet
        .keys()
        .filter(|name| {
            data.placement
                .as_ref()
                .is_none_or(|p| !p.domains.values().any(|w| w.contains(name)))
        })
        .collect();
    if !unused_routers.is_empty() || !unused_workers.is_empty() {
        out.push_str("<details id=\"other-registered-hosts\"><summary>Other registered hosts</summary><p class=\"summary\">Not associated with a domain in the displayed placement.</p><div class=\"groups\">");
        for router in unused_routers {
            out.push_str(&packing_link(router));
        }
        for name in unused_workers {
            out.push_str(&worker_link(data, name));
        }
        out.push_str("</div></details>");
    }
    out
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

pub async fn packing_router_page(
    State(state): State<SharedDashboard>,
    Path(name): Path<String>,
) -> Result<Html<String>, StatusCode> {
    let data = state.read().await.clone();
    let router = data
        .packing_routers
        .get(&name)
        .ok_or(StatusCode::NOT_FOUND)?;
    Ok(Html(packing_router_detail(&data, router)))
}
fn packing_router_detail(
    data: &DashboardData,
    router: &crate::packing_fleet::PackingRouter,
) -> String {
    let sample = &router.sample;
    let mut body = breadcrumb(&sample.name);
    let assignments = if router.domains.is_empty() {
        "All domains".to_string()
    } else {
        router
            .domains
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    };
    body.push_str(&format!("<h2 class=\"section-title\">{}</h2><p class=\"intro\">Packing router · dispatches worker evaluations and packs query responses.</p><div class=\"statusbar\">{}<span>Last successful sample: {}</span></div>", escape(&sample.name), chip("router", router.status(), router.status() == "ready"), sample.success.map(relative_time).unwrap_or_else(|| "never".into())));
    body.push_str(&format!(
        "<p class=\"intro\">Last check: {}</p>",
        sample
            .attempted
            .map(relative_time)
            .unwrap_or_else(|| "not yet checked".into())
    ));
    body.push_str(&format!(
        "<p class=\"intro\">Configured domains: {}</p>",
        escape(&assignments)
    ));
    if let Some(error) = &sample.error {
        body.push_str(&format!(
            "<p class=\"notice\">{}. Values below are from the last successful sample.</p>",
            escape(error)
        ));
    }
    if router.status() == "stale" {
        body.push_str(
            "<p class=\"notice\">These measurements are stale (more than 45 seconds old).</p>",
        );
    }
    if router.status() == "not ready" {
        body.push_str(
            "<p class=\"notice\">The router is reachable but is not ready to serve queries.</p>",
        );
    }
    body.push_str("<div class=\"grid\" style=\"margin-top:24px\">");
    body.push_str(&card(
        "Serving state",
        rows(
            &sample.values,
            &[
                ("controller_epoch", "Controller epoch"),
                ("available_requests", "Available request slots"),
                ("evaluations_outstanding", "Outstanding evaluations"),
            ],
        ),
    ));
    body.push_str(&card(
        "Queries · cumulative",
        rows(
            &sample.values,
            &[
                ("successful_queries_total", "Successful"),
                ("rejected_queries_total", "Rejected"),
                ("failed_queries_total", "Failed"),
                ("evaluation_retries_total", "Evaluation retries"),
            ],
        ),
    ));
    body.push_str(&card(
        "Packing",
        rows(
            &sample.values,
            &[
                ("resident_objects", "Resident objects"),
                ("charged_bytes", "Charged packing memory"),
                (
                    "intermediate_bytes_total",
                    "Intermediate payload bytes · cumulative",
                ),
                ("packing_microseconds_total", "Packing time · cumulative"),
            ],
        ),
    ));
    body.push_str("</div><p class=\"note\">Charged packing memory tracks packing allocations, not total host RAM. Query and transfer counters are cumulative since process start. Public Init / Query latency and transfer rates remain on the fleet overview. Refreshes every 15 seconds.</p>");
    page(data, &sample.name, body)
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
    let membership = data
        .placement
        .as_ref()
        .map(|p| {
            let ids: Vec<_> = p
                .domains
                .iter()
                .filter(|(_, workers)| workers.contains(&worker.name))
                .map(|(id, _)| id.to_string())
                .collect();
            if ids.is_empty() {
                "Evaluation worker · no current domain assignment".into()
            } else {
                format!(
                    "Evaluation worker · Domains {}{}",
                    ids.join(", "),
                    if data.placement_error {
                        " (last known placement)"
                    } else {
                        ""
                    }
                )
            }
        })
        .unwrap_or_else(|| format!("Evaluation worker · inventory container {}", worker.group));
    body.push_str(&format!("<h2 class=\"section-title\">{}</h2><p class=\"intro\">{}</p><div class=\"statusbar\">{}<span>Last successful sample: {}</span></div>",escape(&worker.name),escape(&membership),chip("metrics",worker.status(),worker.status()=="reachable"),worker.success.map(relative_time).unwrap_or_else(||"never".into())));
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

#[cfg(test)]
mod domain_tests {
    use super::*;
    #[test]
    fn each_domain_contains_only_its_assigned_routers_and_workers() {
        let mut data = super::super::tests::sample();
        data.packing_enabled = true;
        data.packing_routers = crate::packing_fleet::inventory(
            r#"[
            {"name":"shared","url":"http://10.0.0.1:8093","domains":[]},
            {"name":"domain-two","url":"http://10.0.0.2:8093","domains":[2]},
            {"name":"spare","url":"http://10.0.0.3:8093","domains":[9]}]"#,
        )
        .unwrap();
        data.fleet = crate::fleet::inventory(r#"{"groups":[{"name":"legacy-group","replicas":[{"name":"worker-a","url":"http://10.0.1.1:8091"},{"name":"worker-b","url":"http://10.0.1.2:8091"}]}]}"#).unwrap();
        crate::placement::update(
            &mut data,
            Some(
                r#"{"placement_revision":4,"pool":{"placements":{"0":["worker-a"],"2":["worker-b","unknown-worker"]}}}"#,
            ),
        );
        let html = overview(&data);
        let domain_zero = html
            .split("data-domain=\"0\"")
            .nth(1)
            .unwrap()
            .split("</section>")
            .next()
            .unwrap();
        let domain_two = html
            .split("data-domain=\"2\"")
            .nth(1)
            .unwrap()
            .split("</section>")
            .next()
            .unwrap();
        assert!(domain_zero.contains("/packing-routers/shared/"));
        assert!(domain_two.contains("/packing-routers/shared/"));
        assert!(!domain_zero.contains("domain-two"));
        assert!(domain_two.contains("/packing-routers/domain-two/"));
        assert!(domain_zero.contains("/workers/worker-a/"));
        assert!(!domain_zero.contains("worker-b"));
        assert!(domain_two.contains("/workers/worker-b/"));
        assert!(domain_two.contains("Worker monitoring unavailable"));
        assert!(!domain_two.contains("/workers/unknown-worker/"));
        assert!(!html.contains("legacy-group"));
        assert!(!html.contains("10.0."));
        assert!(html.contains("Other registered hosts"));
        assert!(!domain_zero.contains("spare"));
        data.placement_error = true;
        assert!(overview(&data).contains("Showing the last successful assignment"));
        data.placement = None;
        let missing = overview(&data);
        assert!(missing.contains("without inferred assignments"));
        assert!(!missing.contains("data-domain="));
        assert!(missing.contains("/packing-routers/shared/"));
    }
}
