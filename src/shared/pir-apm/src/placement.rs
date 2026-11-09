//! Router-owned serving assignments, separate from latest-publication adoption.
use std::{
    collections::{BTreeMap, BTreeSet},
    time::SystemTime,
};

#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct Placement {
    pub domains: BTreeMap<u64, Vec<String>>,
    pub revision: Option<u64>,
    pub published_revision: Option<u64>,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub struct Published {
    generation: u64,
    manifest_sha256: String,
    domains: BTreeSet<u64>,
}
impl Published {
    pub fn parse(body: &str) -> Option<Self> {
        let value: Self = serde_json::from_str(body).ok()?;
        (value.generation > 0 && valid_digest(&value.manifest_sha256) && !value.domains.is_empty())
            .then_some(value)
    }
}
/// The wallet protocol's domain ceiling (`enhance_shared::protocol::MAX_QUERY_SHARDS`).
const MAX_DOMAINS: u64 = 24;
/// Domains a worker admits while one is growing
/// (`enhance_shared::placement::PlacementPolicy::limit`).
const MAX_GROWING_DOMAINS: usize = 5;

/// Whether one domain will be served: some fresh router names it with a
/// fresh worker configured for it. `assigned` is `None` while any router or
/// named worker that could change the answer is stale, failed or predates
/// the configuration fields. `sample` is the oldest observation the answer
/// rests on, so recovery needs fresh evidence and an unrelated stale router
/// cannot hold it back.
#[derive(Clone, Debug, PartialEq)]
pub struct Assignment {
    pub assigned: Option<bool>,
    pub sample: u64,
}

/// Domain headroom for the newest publication. A domain opens the moment the
/// newest one fills, and no role serves a domain it was not configured for,
/// so assignment must precede growth.
#[derive(Clone, Debug, PartialEq)]
pub struct Rollover {
    /// The domain the next carve-out opens.
    pub next: u64,
    /// `next` is at or past the protocol ceiling: no assignment can serve it.
    pub exhausted: bool,
    /// Rows the newest domain can still take, from a publisher scrape of the
    /// same generation as the published domains.
    pub rows_left: Option<f64>,
    /// When `rows_left` was observed.
    pub rows_sample: u64,
    pub next_domain: Assignment,
    /// Published domains that will not be served; `None` while unknown.
    pub unassigned: Option<BTreeSet<u64>>,
    pub unassigned_sample: u64,
    /// Some domain's answer, published or projected, was unknown, even if a
    /// proven gap elsewhere already decides `unassigned` or `next_domain`.
    pub incomplete: bool,
}

/// Router and worker assignment headroom for the publisher's published
/// domains, judged at whole-second `now` like every other condition. Needs a
/// fresh publisher health read: its domain list decides what must be served.
pub fn rollover(data: &crate::dashboard::DashboardData, now: u64) -> Option<Rollover> {
    let fresh = |at: Option<SystemTime>| {
        at.and_then(|at| at.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_secs())
            .filter(|at| *at <= now && now - at <= 45)
    };
    let published = data.enhance_publication.as_ref()?;
    let published_at = fresh(data.enhance_publication_sample)?;
    let next = published.domains.iter().next_back()? + 1;
    let coordinator = fresh(data.last_scrape).filter(|_| data.scrape_error.is_none());
    // Publisher health (domains) and metrics (rows) are separate reads that
    // can straddle a publication; combine them only for one generation.
    let gauge = |name: &str| data.snapshot_gauges.get(name).copied();
    let rows_left = coordinator
        .filter(|_| gauge("enhance_published_generation") == Some(published.generation as f64))
        .and(gauge("enhance_rows_before_next_carveout"))
        .filter(|v| v.is_finite() && *v >= 0.);
    // Router evidence alone can prove a domain unassigned; worker evidence
    // only matters for the workers a router actually names.
    // A freshly loaded inventory, even an empty one, makes router evidence
    // complete; disabled or failing router monitoring leaves it unknown.
    let mut routers_complete =
        data.packing_inventory_error.is_none() && fresh(data.packing_inventory_loaded).is_some();
    let mut routes = Vec::new();
    for router in data.packing_routers.values() {
        match (
            fresh(router.sample.success).filter(|_| router.sample.error.is_none()),
            router.configured.as_ref(),
        ) {
            (Some(at), Some(configured)) => {
                routes.push((at, configured, router.combined, origin(&router.sample.url)))
            }
            _ => routers_complete = false,
        }
    }
    let workers_trusted = data.inventory_error.is_none();
    let workers: BTreeMap<String, (u64, &BTreeMap<String, f64>)> = data
        .fleet
        .values()
        .filter_map(|w| {
            let at = fresh(w.success).filter(|_| w.error.is_none())?;
            w.values
                .contains_key("configured_domains")
                .then_some((origin(&w.url)?, (at, &w.values)))
        })
        .collect();
    // Whether a router's named workers will serve `domain`: `Some(at)` with the
    // deciding worker's sample, `None` when proven not, `Err(())` if unknown.
    // The newest published domain is growing while rows remain in it; a full
    // one may already be sealed.
    let newest_has_rows = rows_left.is_some_and(|rows| rows > 0.);
    // A worker admits at most five domains while one of its own is growing
    // (`PlacementPolicy::limit`) and refuses the whole publication otherwise,
    // so one configured for more of `publication`, including its growing
    // newest domain, cannot serve any of it.
    let worker_for = |urls: &BTreeSet<String>,
                      domain: u64,
                      publication: &BTreeSet<u64>,
                      own: Option<&String>,
                      samples: &mut Vec<u64>| {
        let mut unknown = !workers_trusted;
        for url in urls {
            let url_origin = origin(url);
            // Another router can use a worker only through the remote
            // readiness a standalone worker reports (1). A combined role's
            // embedded worker reports 0; a worker reporting neither
            // predates the marker and leaves the answer unknown.
            let remote = url_origin.as_ref().is_some_and(|o| Some(o) != own);
            match url_origin.and_then(|o| workers.get(&o)) {
                Some((at, values)) if remote && values.get("remote_readiness") == Some(&0.) => {
                    samples.push(*at);
                }
                Some((_, values)) if remote && values.get("remote_readiness") != Some(&1.) => {
                    unknown = true;
                }
                Some((at, values))
                    if publication
                        .iter()
                        .filter(|d| values.get(&format!("configured_domain_{d}")) == Some(&1.))
                        .count()
                        > MAX_GROWING_DOMAINS =>
                {
                    // A worker holding a growing domain is limited to five. The
                    // projected next domain always grows, and the current
                    // newest one does while rows remain in it. Otherwise the
                    // worker's domains may all be sealed (limit
                    // `sealed_shards`) or some still provisional, which APM
                    // cannot see.
                    let newest = publication.last().copied();
                    let growing = newest.is_some_and(|d| d == next || newest_has_rows);
                    if growing
                        && newest.is_some_and(|d| {
                            values.get(&format!("configured_domain_{d}")) == Some(&1.)
                        })
                    {
                        samples.push(*at);
                    } else {
                        unknown = true;
                    }
                }
                Some((at, values))
                    if workers_trusted
                        && values.get(&format!("configured_domain_{domain}")) == Some(&1.) =>
                {
                    return Ok(Some(*at));
                }
                Some((at, _)) => samples.push(*at),
                None => unknown = true,
            }
        }
        if unknown {
            Err(())
        } else {
            Ok(None)
        }
    };
    // A router serves a publication only when every published domain it owns
    // has a ready worker (`PackingRouter::peers`), so `domain` counts as
    // assigned only through a router that can serve all of `required` it owns.
    let assignment = |domain: u64, required: &BTreeSet<u64>| {
        let publication: BTreeSet<u64> = required.iter().copied().chain([domain]).collect();
        let mut known = routers_complete;
        let mut samples = vec![published_at];
        for (router_at, configured, combined, own) in &routes {
            samples.push(*router_at);
            if !configured.contains_key(&domain) {
                continue;
            }
            let mut proof = vec![published_at, *router_at];
            // A combined role's ingress refuses a publication it does not
            // cover in full (`CoverageMismatch`), and no other ingress can use
            // its embedded router.
            let mut verdict =
                if *combined && required.iter().any(|owned| !configured.contains_key(owned)) {
                    Some(false)
                } else {
                    Some(true)
                };
            for owned in required.iter().chain([&domain]) {
                if verdict == Some(false) {
                    break;
                }
                let Some(urls) = configured.get(owned) else {
                    continue;
                };
                match worker_for(urls, *owned, &publication, own.as_ref(), &mut samples) {
                    Ok(Some(at)) => proof.push(at),
                    Ok(None) => verdict = Some(false),
                    Err(()) => {
                        if verdict == Some(true) {
                            verdict = None;
                        }
                    }
                }
                if verdict == Some(false) {
                    break;
                }
            }
            match verdict {
                Some(true) => {
                    return Assignment {
                        assigned: Some(true),
                        sample: proof.into_iter().min().unwrap_or(published_at),
                    };
                }
                Some(false) => {}
                None => known = false,
            }
        }
        Assignment {
            assigned: known.then_some(false),
            sample: samples.into_iter().min().unwrap_or(published_at),
        }
    };
    let exhausted = next >= MAX_DOMAINS;
    let mut projected_unknown = false;
    // Rollover asks whether the publication that exists once `next` opens can
    // be served: every domain of it, judged against all of it, since a router
    // that gains an unservable domain stops serving the ones it had.
    let next_domain = if exhausted {
        Assignment {
            assigned: Some(false),
            sample: published_at,
        }
    } else {
        let mut projected = published.domains.clone();
        projected.insert(next);
        let answers: Vec<_> = projected
            .iter()
            .map(|d| assignment(*d, &projected))
            .collect();
        projected_unknown = answers.iter().any(|a| a.assigned.is_none());
        let pick = |want: Option<bool>| {
            answers
                .iter()
                .filter(|a| a.assigned == want)
                .map(|a| a.sample)
                .min()
        };
        if let Some(sample) = pick(Some(false)) {
            Assignment {
                assigned: Some(false),
                sample,
            }
        } else if answers.iter().any(|a| a.assigned.is_none()) {
            Assignment {
                assigned: None,
                sample: now,
            }
        } else {
            Assignment {
                assigned: Some(true),
                sample: pick(Some(true)).unwrap_or(published_at),
            }
        }
    };
    // A proven gap is reported even while another domain is unknown; only a
    // healthy answer needs every domain known.
    let (mut missing, mut missing_sample) = (BTreeSet::new(), u64::MAX);
    let (mut unknown, mut healthy_sample) = (false, u64::MAX);
    for domain in &published.domains {
        let a = assignment(*domain, &published.domains);
        match a.assigned {
            Some(true) => healthy_sample = healthy_sample.min(a.sample),
            Some(false) => {
                missing.insert(*domain);
                missing_sample = missing_sample.min(a.sample);
            }
            None => unknown = true,
        }
    }
    let (unassigned, unassigned_sample) = if !missing.is_empty() {
        (Some(missing), missing_sample)
    } else if unknown {
        (None, now)
    } else {
        (Some(missing), healthy_sample)
    };
    Some(Rollover {
        next,
        exhausted,
        rows_left,
        rows_sample: coordinator.unwrap_or(now).min(published_at),
        next_domain,
        unassigned,
        unassigned_sample,
        incomplete: unknown || projected_unknown,
    })
}

/// The scheme, host and explicit non-default port of `url`, so equivalent
/// spellings of one worker origin (a trailing slash, `:80` on http) match.
fn origin(url: &str) -> Option<String> {
    let url = reqwest::Url::parse(url).ok()?;
    url.host_str()?;
    Some(url.origin().ascii_serialization())
}

/// A router's configured responsibilities from `/internal/health`
/// `configured_domains` (under `packing` for a combined role), including
/// unpublished domains, as worker origins per domain. Read apart from active
/// placement, so a router provisioned only for future domains still reports
/// them. A combined role names its in-process worker `local`, which resolves
/// to `own_origin`, the origin the role was scraped at. `None` when absent or
/// malformed.
pub fn configured(
    health: &serde_json::Value,
    own_origin: &str,
) -> Option<BTreeMap<u64, BTreeSet<String>>> {
    let map = health
        .get("configured_domains")
        .or_else(|| health.get("packing")?.get("configured_domains"))?
        .as_object()?;
    if map.is_empty() || map.len() > MAX_DOMAINS as usize {
        return None;
    }
    map.iter()
        .map(|(id, urls)| {
            let domain = id
                .parse::<u64>()
                .ok()
                .filter(|d| d.to_string() == *id && *d < MAX_DOMAINS)?;
            let urls = urls
                .as_array()?
                .iter()
                .map(|u| {
                    let u = u.as_str()?;
                    Some(if u == "local" { own_origin } else { u }.to_string())
                })
                .collect::<Option<BTreeSet<_>>>()?;
            (!urls.is_empty()).then_some((domain, urls))
        })
        .collect()
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Private origins stay inside collection; dashboard placement uses inventory names.
#[derive(Clone, Debug)]
pub struct Report {
    pub generation: u64,
    pub active: String,
    pub domains: BTreeMap<u64, BTreeMap<String, bool>>,
    pub handover: Option<serde_json::Value>,
}
impl Report {
    pub fn parse(body: &str) -> Option<Self> {
        let value: serde_json::Value = serde_json::from_str(body).ok()?;
        let generation = value["generation"].as_u64().filter(|n| *n > 0)?;
        let active = value["active"].as_str()?;
        if !valid_digest(active) {
            return None;
        }
        let ready = value["ready"].as_bool()?;
        let assignments = value["domain_workers"].as_object()?;
        if assignments.is_empty() || assignments.len() > 6 {
            return None;
        }
        let mut domains = BTreeMap::new();
        for (id, workers) in assignments {
            let domain = id.parse::<u64>().ok()?;
            if domain.to_string() != *id {
                return None;
            }
            let workers = workers.as_object()?;
            if workers.is_empty() || workers.len() > 2 {
                return None;
            }
            let mut replicas = BTreeMap::new();
            for (origin, eligible) in workers {
                let url = reqwest::Url::parse(origin).ok()?;
                let eligible = eligible.as_bool()?;
                if !matches!(url.scheme(), "http" | "https")
                    || url.host_str().is_none()
                    || !url.username().is_empty()
                    || url.password().is_some()
                    || url.query().is_some()
                    || url.fragment().is_some()
                    || url.path() != "/"
                    || (eligible && !ready)
                {
                    return None;
                }
                if replicas
                    .insert(url.as_str().trim_end_matches('/').to_string(), eligible)
                    .is_some()
                {
                    return None;
                }
            }
            domains.insert(domain, replicas);
        }
        Some(Self {
            generation,
            active: active.into(),
            domains,
            handover: value.get("handover").cloned(),
        })
    }
}

/// Keep the last valid placement for display, but never refresh it from stale,
/// inconsistent or unconfigured serving observations. A coherent older serving
/// generation is observable, but never described as the newest publication.
pub fn update(data: &mut crate::dashboard::DashboardData, now: SystemTime) {
    if let Some((placement, replicas, at)) = collect(data, now) {
        data.placement = Some(placement);
        data.domain_ready_replicas = replicas;
        data.placement_success = Some(at);
        data.placement_error = false;
    } else {
        data.placement_error = true;
    }
}
fn collect(
    data: &crate::dashboard::DashboardData,
    now: SystemTime,
) -> Option<(Placement, BTreeMap<u64, f64>, SystemTime)> {
    let fresh = |at: SystemTime| {
        now.duration_since(at)
            .ok()
            .is_some_and(|age| age.as_secs() <= 45)
    };
    if data.scrape_error.is_some()
        || !data.last_scrape.is_some_and(fresh)
        || data.inventory_error.is_some()
        || data.packing_inventory_error.is_some()
        || data.packing_routers.is_empty()
    {
        return None;
    }
    let generation = *data.snapshot_gauges.get("enhance_published_generation")?;
    let published = data.enhance_publication.as_ref()?;
    if !generation.is_finite() || generation <= 0. || generation.fract() != 0. {
        return None;
    }
    if generation != published.generation as f64 {
        return None;
    }
    let mut at = data.last_scrape?;
    let mut serving: Option<(u64, &str)> = None;
    let mut assignments: BTreeMap<u64, BTreeSet<String>> = BTreeMap::new();
    let mut eligible: BTreeMap<u64, BTreeSet<String>> = BTreeMap::new();
    for router in data.packing_routers.values() {
        let sampled = router.sample.success.filter(|at| fresh(*at))?;
        if router.sample.error.is_some() {
            return None;
        }
        // A freshly scraped router provisioned only for unpublished domains has
        // no active assignments yet; it neither serves nor blocks this
        // publication. An unreachable one stays unknown, since its cached
        // configuration may no longer be what it serves.
        if router.placement.is_none()
            && router
                .configured
                .as_ref()
                .is_some_and(|c| c.keys().all(|d| !published.domains.contains(d)))
        {
            continue;
        }
        at = at.min(sampled);
        let report = router.placement.as_ref()?;
        if report.generation > published.generation
            || (report.generation == published.generation
                && report.active != published.manifest_sha256)
            || serving.is_some_and(|s| s != (report.generation, report.active.as_str()))
        {
            return None;
        }
        serving = Some((report.generation, report.active.as_str()));
        for (domain, workers) in &report.domains {
            for (origin, ready) in workers {
                let names: Vec<_> = data
                    .fleet
                    .iter()
                    .filter(|(_, worker)| worker.url == *origin)
                    .map(|(name, _)| name.clone())
                    .collect();
                if names.len() != 1 {
                    return None;
                }
                let name = names.into_iter().next()?;
                assignments.entry(*domain).or_default().insert(name.clone());
                if *ready {
                    eligible.entry(*domain).or_default().insert(name);
                }
            }
        }
    }
    if assignments.keys().copied().collect::<BTreeSet<_>>() != published.domains {
        return None;
    }
    let replicas = assignments
        .keys()
        .map(|domain| {
            (
                *domain,
                eligible.get(domain).map_or(0, BTreeSet::len) as f64,
            )
        })
        .collect();
    let domains = assignments
        .into_iter()
        .map(|(domain, workers)| (domain, workers.into_iter().collect()))
        .collect();
    Some((
        Placement {
            domains,
            revision: serving.map(|s| s.0),
            published_revision: Some(generation as u64),
        },
        replicas,
        at,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        dashboard::DashboardData,
        monitoring::{self, Chain, Policy},
    };
    use pir_apm::incidents::Store;
    use std::time::{Duration, UNIX_EPOCH};

    fn at(t: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(t)
    }
    fn report() -> String {
        serde_json::json!({"generation":28049,"active":"ef33dcbb13d3c0b1f2a782dda4e5111666ba2f84e94a55d9ae1bcd5159e7f799","ready":true,
            "domain_workers":{"0":{"http://10.142.0.15:8091":true,"http://10.142.0.16:8091":true}}}).to_string()
    }
    fn view(t: u64) -> DashboardData {
        let mut d = DashboardData::new(
            "APM".into(),
            crate::schema::Schema::enhance_default(),
            "test".into(),
            "host".into(),
            Default::default(),
        );
        d.last_scrape = Some(at(t));
        d.scrape_error = None;
        d.ready_status = Some(200);
        d.snapshot_gauges = BTreeMap::from([
            ("enhance_published_generation".into(), 28049.),
            ("enhance_published_anchor_height".into(), 3527386.),
            (
                "enhance_publication_last_advancement_unix_seconds".into(),
                (t - 5) as f64,
            ),
            ("enhance_publication_consecutive_failures".into(), 0.),
            ("enhance_publication_blocked".into(), 0.),
        ]);
        d.enhance_publication = Published::parse(
            r#"{"generation":28049,"manifest_sha256":"ef33dcbb13d3c0b1f2a782dda4e5111666ba2f84e94a55d9ae1bcd5159e7f799","domains":[0]}"#,
        );
        d.enhance_publication_sample = Some(at(t));
        d.packing_inventory_loaded = Some(at(t));
        d.monitoring.chain = Chain {
            sampled_at: t,
            height: 3527386,
            ..Default::default()
        };
        d.fleet = crate::fleet::inventory(r#"{"groups":[{"name":"cpu","replicas":[{"name":"worker-a","url":"http://10.142.0.15:8091"},{"name":"worker-b","url":"http://10.142.0.16:8091"}]},{"name":"old-gpu","replicas":[{"name":"gpu","url":"http://10.250.90.3:8091"}]}]}"#).unwrap();
        d.packing_routers = crate::packing_fleet::inventory(
            r#"[{"name":"router","url":"http://10.142.0.14:8093"}]"#,
        )
        .unwrap();
        let r = d.packing_routers.get_mut("router").unwrap();
        r.sample.success = Some(at(t));
        r.ready = Some(true);
        r.placement = Report::parse(&report());
        r.sample.values.insert("domain_eligible_0".into(), 2.);
        update(&mut d, at(t));
        d
    }
    fn condition(d: &DashboardData, now: u64, key: &str) -> pir_apm::incidents::Condition {
        monitoring::conditions(d, now, &Policy::default())
            .into_iter()
            .chain(crate::assignment_alerts::conditions(d, now))
            .find(|c| c.key == key)
            .unwrap_or_else(|| panic!("{key}"))
    }
    fn firing(d: &DashboardData, now: u64, key: &str) -> Option<bool> {
        condition(d, now, key).firing
    }
    const WORKERS: [&str; 2] = ["http://10.142.0.15:8091", "http://10.142.0.16:8091"];
    /// Router "router" names both CPU workers for `router`; each worker is
    /// fresh at `now` and configured for `workers`.
    fn provision(d: &mut DashboardData, now: u64, router: &[u64], workers: &[u64]) {
        let r = d.packing_routers.get_mut("router").unwrap();
        r.configured = Some(
            router
                .iter()
                .map(|id| (*id, WORKERS.iter().map(|u| u.to_string()).collect()))
                .collect(),
        );
        for name in ["worker-a", "worker-b"] {
            let w = d.fleet.get_mut(name).unwrap();
            w.success = Some(at(now));
            w.values = BTreeMap::from([
                ("configured_domains".into(), workers.len() as f64),
                ("remote_readiness".into(), 1.),
            ]);
            for id in workers {
                w.values.insert(format!("configured_domain_{id}"), 1.);
            }
        }
    }
    fn publish(d: &mut DashboardData, domains: &str) {
        d.enhance_publication = Published::parse(&format!(
            r#"{{"generation":28049,"manifest_sha256":"ef33dcbb13d3c0b1f2a782dda4e5111666ba2f84e94a55d9ae1bcd5159e7f799","domains":{domains}}}"#
        ));
    }

    /// The next domain must be configured on a router and its workers before
    /// the newest one fills, and a published domain no one serves pages by
    /// name instead of surfacing only as unavailable coverage, as it did on
    /// 2026-11-09.
    #[test]
    fn rollover_warns_before_an_unassigned_domain_opens() {
        let now = pir_apm::incidents::unix_time();
        let mut d = view(now);
        d.snapshot_gauges
            .insert("enhance_rows_before_next_carveout".into(), 3_000.);
        // Routers that predate `configured_domains` leave the answer unknown,
        // unless enough rows remain that assignment does not matter yet.
        for key in ["assignment_rollover_warning", "assignment_unassigned"] {
            assert_eq!(firing(&d, now, key), None, "{key}");
        }
        assert_eq!(firing(&d, now, "assignment_rollover_critical"), Some(false));
        provision(&mut d, now, &[0], &[0]);
        assert_eq!(firing(&d, now, "assignment_rollover_warning"), Some(true));
        assert_eq!(firing(&d, now, "assignment_rollover_critical"), Some(false));
        assert_eq!(firing(&d, now, "assignment_unassigned"), Some(false));
        d.snapshot_gauges
            .insert("enhance_rows_before_next_carveout".into(), 500.);
        assert_eq!(firing(&d, now, "assignment_rollover_critical"), Some(true));
        // A router naming workers that are not configured for the domain
        // cannot serve it either.
        provision(&mut d, now, &[0, 1], &[0]);
        assert_eq!(firing(&d, now, "assignment_rollover_critical"), Some(true));
        // Assigning the next domain everywhere clears both, even with the row
        // count unknown.
        provision(&mut d, now, &[0, 1, 2], &[0, 1, 2]);
        d.snapshot_gauges
            .remove("enhance_rows_before_next_carveout");
        assert_eq!(firing(&d, now, "assignment_rollover_warning"), Some(false));
        assert_eq!(firing(&d, now, "assignment_rollover_critical"), Some(false));
        // The incident: domain 1 is published, router and workers have only 0.
        provision(&mut d, now, &[0], &[0]);
        publish(&mut d, "[0,1]");
        assert_eq!(firing(&d, now, "assignment_unassigned"), Some(true));
        // A stale router cannot prove a domain is missing.
        d.packing_routers.get_mut("router").unwrap().sample.success = Some(at(now - 120));
        assert_eq!(firing(&d, now, "assignment_unassigned"), None);
    }

    /// Scrapes carry subsecond times and conditions use whole seconds, so a
    /// scrape inside the current second is fresh, not future-dated.
    #[test]
    fn subsecond_samples_are_fresh_at_whole_second_evaluation() {
        let now = pir_apm::incidents::unix_time();
        let mut d = view(now);
        provision(&mut d, now, &[0], &[0]);
        let later = at(now) + Duration::from_millis(800);
        d.last_scrape = Some(later);
        d.packing_routers.get_mut("router").unwrap().sample.success = Some(later);
        for name in ["worker-a", "worker-b"] {
            d.fleet.get_mut(name).unwrap().success = Some(later);
        }
        d.snapshot_gauges
            .insert("enhance_rows_before_next_carveout".into(), 500.);
        assert_eq!(firing(&d, now, "assignment_unassigned"), Some(false));
        assert_eq!(firing(&d, now, "assignment_rollover_critical"), Some(true));
    }

    /// A router provisioned only for future domains has no active placement,
    /// but its configuration still counts.
    #[test]
    fn a_router_for_future_domains_counts_before_they_open() {
        let now = pir_apm::incidents::unix_time();
        let mut d = view(now);
        d.snapshot_gauges
            .insert("enhance_rows_before_next_carveout".into(), 500.);
        provision(&mut d, now, &[0], &[0, 1]);
        let mut future = d.packing_routers["router"].clone();
        future.placement = None;
        future.configured = Some(BTreeMap::from([(1, [WORKERS[1].to_string()].into())]));
        d.packing_routers.insert("future".into(), future);
        assert_eq!(firing(&d, now, "assignment_rollover_critical"), Some(false));
        // Placement and domain eligibility still observe the routers that
        // serve the publication, using what a real future-only scrape has:
        // no active placement and no domain eligibility values.
        let mut only_future = d.packing_routers["future"].clone();
        only_future.configured = Some(BTreeMap::from([(5, [WORKERS[1].to_string()].into())]));
        only_future
            .sample
            .values
            .retain(|k, _| !k.starts_with("domain_eligible_"));
        d.packing_routers.insert("future".into(), only_future);
        update(&mut d, at(now));
        assert!(!d.placement_error);
        let quiet = |d: &DashboardData| {
            for key in [
                "coverage_placement_warning",
                "coverage_domain_0_eligibility_warning",
                "domain_0_unavailable",
            ] {
                assert_eq!(firing(d, now, key), Some(false), "{key}");
            }
        };
        quiet(&d);
        // Unreachable, its cached configuration is not trusted, so placement
        // is unknown until it answers again; its reachability alert fires too.
        d.packing_routers.get_mut("future").unwrap().sample.error = Some("down".into());
        update(&mut d, at(now));
        assert!(d.placement_error);
        assert_eq!(
            configured(
                &serde_json::json!({"ready": false, "generation": null,
                "domain_workers": {}, "configured_domains": {"5": [WORKERS[0]]}}),
                "http://10.142.0.14:8093"
            ),
            Some(BTreeMap::from([(5, [WORKERS[0].to_string()].into())]))
        );
    }

    /// Publisher health and metrics can straddle a publication: rows left in
    /// a full domain 0 must not be read as rows before domain 2 opens.
    #[test]
    fn headroom_needs_rows_and_domains_from_one_generation() {
        let now = pir_apm::incidents::unix_time();
        let mut d = view(now);
        provision(&mut d, now, &[0, 1], &[0, 1]);
        publish(&mut d, "[0,1]");
        d.snapshot_gauges
            .insert("enhance_rows_before_next_carveout".into(), 0.);
        d.snapshot_gauges
            .insert("enhance_published_generation".into(), 28048.);
        assert_eq!(firing(&d, now, "assignment_rollover_critical"), None);
        d.snapshot_gauges
            .insert("enhance_published_generation".into(), 28049.);
        assert_eq!(firing(&d, now, "assignment_rollover_critical"), Some(true));
    }

    /// A stale router that does not decide the answer cannot freeze its sample,
    /// so recovery still sees distinct fresh observations.
    #[test]
    fn unrelated_stale_routers_do_not_hold_back_recovery() {
        let now = pir_apm::incidents::unix_time();
        let mut d = view(now);
        provision(&mut d, now, &[0, 1], &[0, 1]);
        let mut stale = d.packing_routers["router"].clone();
        stale.sample.success = Some(at(now - 600));
        d.packing_routers.insert("stale".into(), stale);
        d.snapshot_gauges
            .insert("enhance_rows_before_next_carveout".into(), 500.);
        let c = condition(&d, now, "assignment_rollover_critical");
        assert_eq!(c.firing, Some(false));
        assert_eq!(c.sample, now);
        assert_eq!(condition(&d, now, "assignment_unassigned").sample, now);
    }

    /// Past domain 23 no assignment can help; the alert says so.
    #[test]
    fn protocol_exhaustion_is_reported_as_such() {
        let now = pir_apm::incidents::unix_time();
        let mut d = view(now);
        let all: Vec<u64> = (0..24).collect();
        provision(&mut d, now, &all, &all);
        publish(&mut d, &serde_json::to_string(&all).unwrap());
        d.snapshot_gauges
            .insert("enhance_rows_before_next_carveout".into(), 500.);
        let c = condition(&d, now, "assignment_rollover_critical");
        assert_eq!(c.firing, Some(true));
        assert!(c.observed.contains("24-domain limit"), "{}", c.observed);
    }

    /// One domain whose worker cannot be observed must not hide a different
    /// domain that no router names at all.
    #[test]
    fn an_unknown_domain_does_not_hide_a_proven_missing_one() {
        let now = pir_apm::incidents::unix_time();
        let mut d = view(now);
        provision(&mut d, now, &[0], &[0]);
        publish(&mut d, "[0,1]");
        d.fleet.get_mut("worker-a").unwrap().error = Some("unreachable".into());
        d.fleet.get_mut("worker-b").unwrap().error = Some("unreachable".into());
        let c = condition(&d, now, "assignment_unassigned");
        assert_eq!(c.firing, Some(true));
        assert!(c.observed.contains("{1}"), "{}", c.observed);
    }

    /// The published domain list decides what must be served, so a stale
    /// publisher read leaves every assignment answer unknown, and fresh
    /// answers carry the publisher's sample time.
    #[test]
    fn assignment_needs_a_fresh_publisher_read() {
        let now = pir_apm::incidents::unix_time();
        let mut d = view(now);
        provision(&mut d, now, &[0], &[0]);
        d.enhance_publication_sample = Some(at(now - 30));
        assert_eq!(condition(&d, now, "assignment_unassigned").sample, now - 30);
        d.enhance_publication_sample = Some(at(now - 120));
        for key in [
            "assignment_unassigned",
            "assignment_rollover_warning",
            "assignment_rollover_critical",
        ] {
            assert_eq!(firing(&d, now, key), None, "{key}");
        }
    }

    /// A combined role reports its router fields under `packing` and names its
    /// in-process worker `local`, which is the origin it was scraped at.
    #[test]
    fn combined_roles_and_equivalent_origins_resolve() {
        let health = serde_json::json!({"role": "combined",
            "packing": {"configured_domains": {"0": ["local"], "1": ["local"]}}});
        assert_eq!(
            configured(&health, "http://10.142.0.20:8093"),
            Some(BTreeMap::from([
                (0, ["http://10.142.0.20:8093".to_string()].into()),
                (1, ["http://10.142.0.20:8093".to_string()].into()),
            ]))
        );
        assert_eq!(origin("http://10.0.0.1:80/"), origin("http://10.0.0.1"));
        let now = pir_apm::incidents::unix_time();
        let mut d = view(now);
        provision(&mut d, now, &[0, 1], &[0, 1]);
        d.packing_routers.get_mut("router").unwrap().configured = Some(BTreeMap::from([
            (0, ["http://10.142.0.15:8091/".to_string()].into()),
            (1, ["http://10.142.0.15:8091/".to_string()].into()),
        ]));
        d.snapshot_gauges
            .insert("enhance_rows_before_next_carveout".into(), 500.);
        assert_eq!(firing(&d, now, "assignment_rollover_critical"), Some(false));
    }

    /// A router serves a publication only when it can serve every published
    /// domain it owns, so two half-working routers do not add up to coverage.
    #[test]
    fn a_router_counts_only_when_it_serves_all_its_published_domains() {
        let now = pir_apm::incidents::unix_time();
        let mut d = view(now);
        provision(&mut d, now, &[0, 1], &[0]);
        d.fleet.get_mut("worker-b").unwrap().values = BTreeMap::from([
            ("configured_domains".into(), 1.),
            ("configured_domain_1".into(), 1.),
            ("remote_readiness".into(), 1.),
        ]);
        let routes = |url: &str| -> BTreeMap<u64, BTreeSet<String>> {
            [(0, [url.to_string()].into()), (1, [url.to_string()].into())].into()
        };
        d.packing_routers.get_mut("router").unwrap().configured = Some(routes(WORKERS[0]));
        let mut second = d.packing_routers["router"].clone();
        second.configured = Some(routes(WORKERS[1]));
        d.packing_routers.insert("second".into(), second);
        d.snapshot_gauges
            .insert("enhance_rows_before_next_carveout".into(), 500.);
        // Domain 0 published: the second router's worker lacks domain 0, so it
        // cannot carry domain 1 when it opens.
        assert_eq!(firing(&d, now, "assignment_rollover_critical"), Some(true));
        publish(&mut d, "[0,1]");
        assert_eq!(firing(&d, now, "assignment_unassigned"), Some(true));
    }

    /// A broken worker inventory cannot hide a domain that no router names.
    #[test]
    fn a_worker_inventory_error_does_not_hide_a_router_gap() {
        let now = pir_apm::incidents::unix_time();
        let mut d = view(now);
        provision(&mut d, now, &[0], &[0]);
        publish(&mut d, "[0,1]");
        d.inventory_error = Some("invalid worker inventory".into());
        let c = condition(&d, now, "assignment_unassigned");
        assert_eq!(c.firing, Some(true));
        assert!(c.observed.contains("{1}"), "{}", c.observed);
    }

    /// An unknown evaluation must not advance the store's sample watermark, or
    /// a later proven failure resting on slightly older scrapes never pages.
    #[test]
    fn unknown_answers_do_not_block_a_later_proven_page() {
        let key = "assignment_rollover_critical";
        let mut store = Store::open(std::path::Path::new(":memory:")).unwrap();
        let selected = |d: &DashboardData, t| {
            crate::assignment_alerts::conditions(d, t)
                .into_iter()
                .filter(|c| c.key == key)
                .collect::<Vec<_>>()
        };
        let mut d = view(1000);
        provision(&mut d, 990, &[0], &[0]);
        d.packing_routers.get_mut("router").unwrap().sample.success = Some(at(990));
        d.snapshot_gauges
            .insert("enhance_rows_before_next_carveout".into(), 500.);
        d.enhance_publication_sample = Some(at(900));
        store
            .evaluate(&selected(&d, 1000), 1000, true, "test", "")
            .unwrap();
        assert!(store.incidents().unwrap().iter().all(|i| !i.active));
        d.enhance_publication_sample = Some(at(1005));
        d.last_scrape = Some(at(1005));
        store
            .evaluate(&selected(&d, 1005), 1005, true, "test", "")
            .unwrap();
        assert!(store
            .incidents()
            .unwrap()
            .iter()
            .any(|i| i.active && i.condition.as_ref().is_some_and(|c| c.key == key)));
    }

    /// After a deployment, an unknown answer still breaks a pending hold.
    #[test]
    fn an_unknown_answer_breaks_a_hold_after_a_deployment() {
        let key = "assignment_unassigned";
        let mut store = Store::open(std::path::Path::new(":memory:")).unwrap();
        store
            .begin_deployment("d", vec!["enhance".into()], 60, 800)
            .unwrap();
        store.finish_deployment("d", false, 900).unwrap();
        let mut step = |t: u64, known: bool| {
            let mut d = view(t);
            provision(&mut d, t, &[0], &[0]);
            publish(&mut d, "[0,1]");
            d.enhance_publication_sample = Some(at(t));
            if !known {
                d.packing_routers.get_mut("router").unwrap().sample.error = Some("down".into());
            }
            let conditions: Vec<_> = crate::assignment_alerts::conditions(&d, t)
                .into_iter()
                .filter(|c| c.key == key)
                .collect();
            store.evaluate(&conditions, t, false, "test", "").unwrap();
            store
                .incidents()
                .unwrap()
                .iter()
                .any(|i| i.active && i.condition.as_ref().is_some_and(|c| c.key == key))
        };
        assert!(!step(1000, true));
        assert!(!step(1015, true));
        assert!(!step(1030, false));
        assert!(!step(1045, true));
        // Without the reset the hold would date from 1000 and page here.
        assert!(!step(1060, true));
        assert!(step(1105, true));
    }

    /// Split combined roles each serve only a publication they cover in full,
    /// so two of them owning one domain each cannot serve two domains.
    #[test]
    fn split_combined_roles_do_not_cover_a_publication() {
        let now = pir_apm::incidents::unix_time();
        let mut d = view(now);
        provision(&mut d, now, &[0, 1], &[0, 1]);
        d.snapshot_gauges
            .insert("enhance_rows_before_next_carveout".into(), 500.);
        let r = d.packing_routers.get_mut("router").unwrap();
        r.combined = true;
        r.configured = Some(BTreeMap::from([(0, [WORKERS[0].to_string()].into())]));
        let mut b = d.packing_routers["router"].clone();
        b.configured = Some(BTreeMap::from([(1, [WORKERS[1].to_string()].into())]));
        d.packing_routers.insert("b".into(), b);
        assert_eq!(firing(&d, now, "assignment_rollover_critical"), Some(true));
        publish(&mut d, "[0,1]");
        assert_eq!(firing(&d, now, "assignment_unassigned"), Some(true));
        // One combined role owning both serves both.
        d.packing_routers.remove("b");
        d.packing_routers.get_mut("router").unwrap().configured = Some(BTreeMap::from([
            (0, [WORKERS[0].to_string()].into()),
            (1, [WORKERS[0].to_string()].into()),
        ]));
        assert_eq!(firing(&d, now, "assignment_unassigned"), Some(false));
    }

    /// A router that gains an unservable domain stops serving the ones it had,
    /// so rollover checks the whole publication that will exist afterwards.
    #[test]
    fn rollover_checks_the_projected_publication() {
        let now = pir_apm::incidents::unix_time();
        let mut d = view(now);
        provision(&mut d, now, &[0, 1], &[0]);
        d.fleet.get_mut("worker-b").unwrap().values = BTreeMap::from([
            ("configured_domains".into(), 1.),
            ("configured_domain_1".into(), 1.),
            ("remote_readiness".into(), 1.),
        ]);
        // Router A owns 0 and 1 with worker A (domain 0 only); router B owns
        // only 1 with worker B.
        d.packing_routers.get_mut("router").unwrap().configured = Some(BTreeMap::from([
            (0, [WORKERS[0].to_string()].into()),
            (1, [WORKERS[0].to_string()].into()),
        ]));
        let mut b = d.packing_routers["router"].clone();
        b.configured = Some(BTreeMap::from([(1, [WORKERS[1].to_string()].into())]));
        d.packing_routers.insert("b".into(), b);
        d.snapshot_gauges
            .insert("enhance_rows_before_next_carveout".into(), 500.);
        assert_eq!(firing(&d, now, "assignment_rollover_critical"), Some(true));
    }

    /// Promotion from shadow during an unknown answer still announces an
    /// active incident once fresh input confirms it.
    #[test]
    fn promotion_survives_an_unknown_answer() {
        let key = "assignment_rollover_warning";
        let mut store = Store::open(std::path::Path::new(":memory:")).unwrap();
        store
            .begin_deployment("d", vec!["enhance".into()], 60, 800)
            .unwrap();
        store.finish_deployment("d", false, 900).unwrap();
        let step = |store: &mut Store, t: u64, known: bool, shadow: bool| {
            let mut d = view(t);
            provision(&mut d, t, &[0], &[0]);
            d.snapshot_gauges
                .insert("enhance_rows_before_next_carveout".into(), 3_000.);
            if !known {
                d.enhance_publication_sample = None;
            }
            let conditions: Vec<_> = crate::assignment_alerts::conditions(&d, t)
                .into_iter()
                .filter(|c| c.key == key)
                .collect();
            store.evaluate(&conditions, t, shadow, "test", "").unwrap();
        };
        step(&mut store, 1000, true, true);
        step(&mut store, 1005, false, false);
        // The finished deployment queued its own notice; count only the page.
        let before = store.health(true, 1005).unwrap().pending;
        step(&mut store, 1015, true, false);
        assert_eq!(store.health(true, 1015).unwrap().pending, before + 1);
    }

    /// A loaded but empty router inventory proves nothing is assigned, while
    /// disabled router monitoring stays unknown.
    #[test]
    fn an_empty_router_inventory_proves_no_assignment() {
        let now = pir_apm::incidents::unix_time();
        let mut d = view(now);
        d.packing_routers.clear();
        assert_eq!(firing(&d, now, "assignment_unassigned"), Some(true));
        d.packing_inventory_loaded = None;
        assert_eq!(firing(&d, now, "assignment_unassigned"), None);
    }

    /// Another router cannot use a combined role's embedded worker.
    #[test]
    fn a_combined_worker_is_not_a_remote_replica() {
        let now = pir_apm::incidents::unix_time();
        let mut d = view(now);
        provision(&mut d, now, &[0], &[0]);
        // worker-a's origin is a combined role's control origin, whose
        // embedded worker reports no remote readiness.
        d.fleet
            .get_mut("worker-a")
            .unwrap()
            .values
            .insert("remote_readiness".into(), 0.);
        let mut combined = d.packing_routers["router"].clone();
        combined.combined = true;
        combined.sample.url = WORKERS[0].into();
        combined.configured = Some(BTreeMap::from([(0, [WORKERS[0].to_string()].into())]));
        d.packing_routers.insert("combined".into(), combined);
        d.packing_routers.get_mut("router").unwrap().configured =
            Some(BTreeMap::from([(0, [WORKERS[0].to_string()].into())]));
        // The combined role owns domain 0 in full, so domain 0 is served.
        assert_eq!(firing(&d, now, "assignment_unassigned"), Some(false));
        // With domain 1 published, the combined role no longer covers it all,
        // and the standalone router cannot use its embedded worker.
        d.fleet
            .get_mut("worker-a")
            .unwrap()
            .values
            .insert("configured_domain_1".into(), 1.);
        d.packing_routers.get_mut("router").unwrap().configured = Some(BTreeMap::from([
            (0, [WORKERS[0].to_string()].into()),
            (1, [WORKERS[0].to_string()].into()),
        ]));
        publish(&mut d, "[0,1]");
        assert_eq!(firing(&d, now, "assignment_unassigned"), Some(true));
    }

    /// A combined role's embedded worker reports no remote readiness, so it
    /// never counts for another router, even when APM cannot tell the role
    /// is combined (absent from the router inventory, or health unreadable).
    #[test]
    fn an_unidentified_combined_worker_is_not_a_remote_replica() {
        let now = pir_apm::incidents::unix_time();
        let mut d = view(now);
        provision(&mut d, now, &[0], &[0]);
        d.fleet
            .get_mut("worker-a")
            .unwrap()
            .values
            .insert("remote_readiness".into(), 0.);
        d.packing_routers.get_mut("router").unwrap().configured =
            Some(BTreeMap::from([(0, [WORKERS[0].to_string()].into())]));
        assert_eq!(firing(&d, now, "assignment_unassigned"), Some(true));
        d.packing_routers.get_mut("router").unwrap().configured = Some(BTreeMap::from([(
            0,
            [WORKERS[0].to_string(), WORKERS[1].to_string()].into(),
        )]));
        assert_eq!(firing(&d, now, "assignment_unassigned"), Some(false));
    }

    /// A worker that predates the remote-readiness marker is unknown, not
    /// proven unable to serve.
    #[test]
    fn a_worker_without_the_marker_is_unknown() {
        let now = pir_apm::incidents::unix_time();
        let mut d = view(now);
        provision(&mut d, now, &[0], &[0]);
        for name in ["worker-a", "worker-b"] {
            d.fleet
                .get_mut(name)
                .unwrap()
                .values
                .remove("remote_readiness");
        }
        assert_eq!(firing(&d, now, "assignment_unassigned"), None);
    }

    /// The warning gives a full domain of lead time, independent of rows; the
    /// critical waits for the row threshold; missing evidence pages on its own
    /// coverage condition; and none of this lives in the main family.
    #[test]
    fn assignment_family_warns_early_and_reports_missing_evidence() {
        let now = pir_apm::incidents::unix_time();
        let mut d = view(now);
        provision(&mut d, now, &[0], &[0]);
        d.snapshot_gauges
            .insert("enhance_rows_before_next_carveout".into(), 30_000.);
        assert_eq!(firing(&d, now, "assignment_rollover_warning"), Some(true));
        assert_eq!(firing(&d, now, "assignment_rollover_critical"), Some(false));
        d.snapshot_gauges
            .remove("enhance_rows_before_next_carveout");
        assert_eq!(firing(&d, now, "assignment_rollover_warning"), Some(true));
        assert_eq!(firing(&d, now, "assignment_rollover_critical"), None);
        // The critical cannot be decided without rows, and coverage says so.
        assert_eq!(firing(&d, now, "assignment_coverage_warning"), Some(true));
        // Once the next domain is assigned, rows no longer matter.
        provision(&mut d, now, &[0, 1], &[0, 1]);
        assert_eq!(firing(&d, now, "assignment_coverage_warning"), Some(false));
        // Routers without the new fields: unknown, and coverage says so.
        d.packing_routers.get_mut("router").unwrap().configured = None;
        assert_eq!(firing(&d, now, "assignment_rollover_warning"), None);
        assert_eq!(firing(&d, now, "assignment_coverage_warning"), Some(true));
        assert!(monitoring::conditions(&d, now, &Policy::default())
            .iter()
            .all(|c| !c.key.starts_with("assignment_")));
    }

    /// A worker configured for more than five domains of the publication
    /// refuses all of it, so it does not count as serving any of them.
    #[test]
    fn a_worker_over_the_five_domain_limit_cannot_serve() {
        let now = pir_apm::incidents::unix_time();
        let mut d = view(now);
        let six: Vec<u64> = (0..6).collect();
        provision(&mut d, now, &six, &six);
        publish(&mut d, "[0,1,2,3,4,5]");
        // Rows remain, so the newest domain is growing.
        d.snapshot_gauges
            .insert("enhance_rows_before_next_carveout".into(), 500.);
        assert_eq!(firing(&d, now, "assignment_unassigned"), Some(true));
        publish(&mut d, "[0,1,2,3,4]");
        assert_eq!(firing(&d, now, "assignment_unassigned"), Some(false));
        // The rollover sees that domain 5 would push them past the limit.
        d.snapshot_gauges
            .insert("enhance_rows_before_next_carveout".into(), 500.);
        assert_eq!(firing(&d, now, "assignment_rollover_critical"), Some(true));
    }

    /// Coverage recovers only on two distinct healthy scrapes, not on repeated
    /// evaluations of one.
    #[test]
    fn coverage_recovery_needs_two_distinct_scrapes() {
        let key = "assignment_coverage_warning";
        let mut store = Store::open(std::path::Path::new(":memory:")).unwrap();
        let active = |store: &Store| {
            store
                .incidents()
                .unwrap()
                .iter()
                .any(|i| i.active && i.condition.as_ref().is_some_and(|c| c.key == key))
        };
        let step = |store: &mut Store, scraped: u64, t: u64, known: bool| {
            let mut d = view(scraped);
            provision(&mut d, scraped, &[0, 1], &[0, 1]);
            if !known {
                d.packing_routers.get_mut("router").unwrap().configured = None;
            }
            let conditions: Vec<_> = crate::assignment_alerts::conditions(&d, t)
                .into_iter()
                .filter(|c| c.key == key)
                .collect();
            store.evaluate(&conditions, t, false, "test", "").unwrap();
        };
        for t in (1000..=1060).step_by(15) {
            step(&mut store, t, t, false);
        }
        assert!(active(&store));
        step(&mut store, 1075, 1075, true);
        step(&mut store, 1075, 1080, true);
        assert!(active(&store), "one scrape evaluated twice");
        step(&mut store, 1090, 1090, true);
        assert!(!active(&store));
    }

    /// A worker with six domains, none of them the growing newest, may be
    /// admitting all six sealed, so APM cannot prove it unable: unknown, not a
    /// false page.
    #[test]
    fn six_domains_without_the_growing_one_are_unknown() {
        let now = pir_apm::incidents::unix_time();
        let mut d = view(now);
        let six: Vec<u64> = (0..6).collect();
        provision(&mut d, now, &six, &six);
        // worker-b alone takes the growing domain 6.
        d.fleet.get_mut("worker-b").unwrap().values = BTreeMap::from([
            ("configured_domains".into(), 1.),
            ("configured_domain_6".into(), 1.),
            ("remote_readiness".into(), 1.),
        ]);
        let mut routes: BTreeMap<u64, BTreeSet<String>> = six
            .iter()
            .map(|id| (*id, [WORKERS[0].to_string()].into()))
            .collect();
        routes.insert(6, [WORKERS[1].to_string()].into());
        d.packing_routers.get_mut("router").unwrap().configured = Some(routes);
        publish(&mut d, "[0,1,2,3,4,5,6]");
        assert_eq!(firing(&d, now, "assignment_unassigned"), None);
        assert_eq!(firing(&d, now, "assignment_coverage_warning"), Some(true));
    }

    /// A proven gap on one domain must not hide missing evidence for another.
    #[test]
    fn a_proven_gap_does_not_hide_missing_evidence() {
        let now = pir_apm::incidents::unix_time();
        let mut d = view(now);
        provision(&mut d, now, &[0, 1], &[0, 1]);
        publish(&mut d, "[0,1,2]");
        d.snapshot_gauges
            .insert("enhance_rows_before_next_carveout".into(), 500.);
        // Domain 2 has no router: proven. Domain 1's only worker goes missing
        // from the worker inventory: unknown.
        let r = d.packing_routers.get_mut("router").unwrap();
        r.configured = Some(BTreeMap::from([
            (0, [WORKERS[0].to_string()].into()),
            (1, ["http://10.142.0.99:8091".to_string()].into()),
        ]));
        assert_eq!(firing(&d, now, "assignment_unassigned"), Some(true));
        assert_eq!(firing(&d, now, "assignment_coverage_warning"), Some(true));
    }

    /// A full newest domain may already be sealed, so six domains that include
    /// it are unknown rather than proven unservable.
    #[test]
    fn a_full_newest_domain_is_not_assumed_growing() {
        let now = pir_apm::incidents::unix_time();
        let mut d = view(now);
        let six: Vec<u64> = (0..6).collect();
        provision(&mut d, now, &six, &six);
        publish(&mut d, "[0,1,2,3,4,5]");
        d.snapshot_gauges
            .insert("enhance_rows_before_next_carveout".into(), 0.);
        assert_eq!(firing(&d, now, "assignment_unassigned"), None);
        d.snapshot_gauges
            .insert("enhance_rows_before_next_carveout".into(), 10.);
        assert_eq!(firing(&d, now, "assignment_unassigned"), Some(true));
    }

    #[test]
    fn configured_domains_reject_malformed_reports() {
        for bad in [
            serde_json::json!({}),
            serde_json::json!({"0": []}),
            serde_json::json!({"24": [WORKERS[0]]}),
            serde_json::json!({"01": [WORKERS[0]]}),
            serde_json::json!([0]),
        ] {
            assert_eq!(
                configured(&serde_json::json!({"configured_domains": bad}), "http://r"),
                None,
                "{bad}"
            );
        }
        assert_eq!(
            configured(&serde_json::json!({"ready": true}), "http://r"),
            None
        );
    }

    #[test]
    fn current_router_assignments_exclude_unused_workers_and_deduplicate_replicas() {
        let mut d = view(1000);
        assert!(!d.placement_error);
        assert_eq!(
            d.placement.as_ref().unwrap().domains[&0],
            ["worker-a", "worker-b"]
        );
        assert_eq!(d.domain_ready_replicas[&0], 2.);
        let second = d.packing_routers["router"].clone();
        d.packing_routers.insert("second".into(), second);
        update(&mut d, at(1000));
        assert!(!d.placement_error);
        assert_eq!(d.domain_ready_replicas[&0], 2.);
    }
    #[test]
    fn coherent_older_serving_is_observable_and_has_a_separate_lag_alert() {
        let now = pir_apm::incidents::unix_time();
        let mut d = view(now);
        let report = d
            .packing_routers
            .get_mut("router")
            .unwrap()
            .placement
            .as_mut()
            .unwrap();
        report.generation -= 1;
        report.active = "ab".repeat(32);
        update(&mut d, at(now + 1));
        assert!(!d.placement_error);
        assert_eq!(d.placement.as_ref().unwrap().revision, Some(28048));
        assert_eq!(
            d.placement.as_ref().unwrap().published_revision,
            Some(28049)
        );
        let conditions = monitoring::conditions(&d, now + 1, &Policy::default());
        for key in [
            "coverage_placement_warning",
            "coverage_domain_0_eligibility_warning",
            "domain_0_unavailable",
        ] {
            assert_eq!(
                conditions.iter().find(|c| c.key == key).unwrap().firing,
                Some(false),
                "{key}"
            );
        }
        let lag = conditions
            .iter()
            .find(|c| c.key == "publication_handover_warning")
            .unwrap();
        assert_eq!(lag.firing, Some(true));
        assert_eq!(
            lag.hold_seconds,
            Policy::default().publication_warning_seconds
        );
        d.packing_routers.get_mut("router").unwrap().sample.error = Some("unreachable".into());
        update(&mut d, at(now + 2));
        let conditions = monitoring::conditions(&d, now + 2, &Policy::default());
        assert_eq!(
            conditions
                .iter()
                .find(|c| c.key == "publication_handover_warning")
                .unwrap()
                .firing,
            None
        );
        assert_eq!(
            conditions
                .iter()
                .find(|c| c.key == "coverage_placement_warning")
                .unwrap()
                .firing,
            Some(true)
        );
    }
    #[test]
    fn future_generations_and_conflicting_older_manifests_are_unknown() {
        for future in [false, true] {
            let mut d = view(1000);
            let mut second = d.packing_routers["router"].clone();
            if future {
                second.placement.as_mut().unwrap().generation += 1;
            } else {
                for r in [&mut second, d.packing_routers.get_mut("router").unwrap()] {
                    r.placement.as_mut().unwrap().generation -= 1;
                }
                second.placement.as_mut().unwrap().active = "ab".repeat(32);
            }
            d.packing_routers.insert("second".into(), second);
            update(&mut d, at(1001));
            assert!(d.placement_error);
        }
    }
    #[test]
    fn handover_alert_waits_for_the_existing_budget_and_recovers_on_current_reports() {
        let mut store = Store::open(std::path::Path::new(":memory:")).unwrap();
        let evaluate = |store: &mut Store, t, behind| {
            let mut d = view(t);
            if behind {
                d.packing_routers
                    .get_mut("router")
                    .unwrap()
                    .placement
                    .as_mut()
                    .unwrap()
                    .generation -= 1;
            }
            update(&mut d, at(t));
            let conditions = monitoring::conditions(&d, t, &Policy::default())
                .into_iter()
                .filter(|c| c.key == "publication_handover_warning")
                .collect::<Vec<_>>();
            store.evaluate(&conditions, t, true, "test", "").unwrap();
        };
        for t in (1000..1120).step_by(5) {
            evaluate(&mut store, t, true);
            assert!(!store.incidents().unwrap()[0].active);
        }
        evaluate(&mut store, 1120, true);
        assert!(store.incidents().unwrap()[0].active);
        evaluate(&mut store, 1125, false);
        assert!(store.incidents().unwrap()[0].active);
        evaluate(&mut store, 1130, false);
        assert!(!store.incidents().unwrap()[0].active);
    }
    #[test]
    fn an_older_generation_with_no_eligible_workers_is_known_unavailability() {
        let mut d = view(1000);
        let r = d.packing_routers.get_mut("router").unwrap();
        let report = r.placement.as_mut().unwrap();
        report.generation -= 1;
        report
            .domains
            .get_mut(&0)
            .unwrap()
            .values_mut()
            .for_each(|ready| *ready = false);
        r.sample.values.insert("domain_eligible_0".into(), 0.);
        update(&mut d, at(1000));
        let conditions = monitoring::conditions(&d, 1000, &Policy::default());
        assert_eq!(
            conditions
                .iter()
                .find(|c| c.key == "domain_0_unavailable")
                .unwrap()
                .firing,
            Some(true)
        );
    }
    #[test]
    fn stale_wrong_generation_or_conflicting_publications_cannot_refresh_placement() {
        for damaged in [
            "stale",
            "generation",
            "publication",
            "unknown-worker",
            "malformed",
        ] {
            let mut d = view(1000);
            let before = d.placement_success;
            let mut second = d.packing_routers["router"].clone();
            match damaged {
                "stale" => second.sample.success = Some(at(954)),
                "generation" => second.placement.as_mut().unwrap().generation -= 1,
                "publication" => second.placement.as_mut().unwrap().active = "f".repeat(64),
                "unknown-worker" => {
                    second
                        .placement
                        .as_mut()
                        .unwrap()
                        .domains
                        .get_mut(&0)
                        .unwrap()
                        .insert("http://other:8091".into(), true);
                }
                "malformed" => second.placement = None,
                _ => unreachable!(),
            }
            d.packing_routers.insert("second".into(), second);
            update(&mut d, at(1001));
            assert!(d.placement_error, "{damaged}");
            assert_eq!(d.placement_success, before, "{damaged}");
            let c = monitoring::conditions(&d, 1001, &Policy::default());
            assert_eq!(
                c.iter()
                    .find(|c| c.key == "coverage_placement_critical")
                    .unwrap()
                    .firing,
                Some(true)
            );
        }
    }
    #[test]
    fn malformed_health_is_not_a_healthy_empty_assignment() {
        for field in ["generation", "active", "ready", "domain_workers"] {
            let mut value: serde_json::Value = serde_json::from_str(&report()).unwrap();
            value.as_object_mut().unwrap().remove(field);
            assert!(Report::parse(&value.to_string()).is_none(), "{field}");
        }
        let invalid = report().replace("\"0\"", "\"00\"");
        assert!(Report::parse(&invalid).is_none());
        let invalid = report().replace("\"ready\":true", "\"ready\":false");
        assert!(Report::parse(&invalid).is_none());
    }
    #[test]
    fn no_eligible_workers_is_known_unavailability() {
        let mut d = view(1000);
        let r = d.packing_routers.get_mut("router").unwrap();
        for ready in r
            .placement
            .as_mut()
            .unwrap()
            .domains
            .get_mut(&0)
            .unwrap()
            .values_mut()
        {
            *ready = false;
        }
        r.sample.values.insert("domain_eligible_0".into(), 0.);
        update(&mut d, at(1000));
        assert!(!d.placement_error);
        let cs = monitoring::conditions(&d, 1000, &Policy::default());
        assert_eq!(
            cs.iter()
                .find(|c| c.key == "domain_0_unavailable")
                .unwrap()
                .firing,
            Some(true)
        );
    }
    #[test]
    fn every_published_domain_and_the_exact_manifest_must_be_observed() {
        let mut d = view(1000);
        d.enhance_publication.as_mut().unwrap().domains.insert(1);
        update(&mut d, at(1001));
        assert!(
            d.placement_error,
            "an unassigned published domain must fail coverage"
        );
        let mut d = view(1000);
        d.packing_routers
            .get_mut("router")
            .unwrap()
            .placement
            .as_mut()
            .unwrap()
            .active = "f".repeat(64);
        update(&mut d, at(1001));
        assert!(
            d.placement_error,
            "agreement on generation cannot substitute for publication identity"
        );
        let mut d = view(1000);
        d.enhance_publication = None;
        update(&mut d, at(1001));
        assert!(
            d.placement_error,
            "missing publisher health cannot refresh placement"
        );
    }
    #[test]
    fn both_live_incidents_recover_only_after_two_distinct_healthy_samples() {
        let keys = [
            "coverage_placement_critical",
            "coverage_publication_metrics_critical",
        ];
        let mut store = Store::open(std::path::Path::new(":memory:")).unwrap();
        let selected = |d: &DashboardData, t| {
            monitoring::conditions(d, t, &Policy::default())
                .into_iter()
                .filter(|c| keys.contains(&c.key.as_str()))
                .collect::<Vec<_>>()
        };
        for t in (1000..=1120).step_by(15) {
            let mut d = view(t);
            d.placement_error = true;
            d.snapshot_gauges.remove("enhance_publication_blocked");
            store
                .evaluate(&selected(&d, t), t, true, "test", "")
                .unwrap();
        }
        let before = store.incidents().unwrap();
        assert!(before.iter().all(|i| i.active));
        for t in [1135, 1136] {
            store
                .evaluate(&selected(&view(1135), t), t, true, "test", "")
                .unwrap();
            assert!(store.incidents().unwrap().iter().all(|i| i.active));
        }
        store
            .evaluate(&selected(&view(1150), 1150), 1150, true, "test", "")
            .unwrap();
        let after = store.incidents().unwrap();
        assert!(after.iter().all(|i| !i.active));
        assert_eq!(
            before.iter().map(|i| &i.id).collect::<Vec<_>>(),
            after.iter().map(|i| &i.id).collect::<Vec<_>>()
        );
    }
    #[test]
    fn a_publisher_withdrawal_holds_placement_coverage_but_reports_a_stuck_one() {
        // A reorg withdrawal: the refused router still lists its workers, so its
        // report is unusable while the publisher reports itself ineligible.
        let withdrawn = |t| {
            let mut d = view(t);
            d.snapshot_gauges
                .insert("enhance_publication_eligible".into(), 0.);
            d.snapshot_gauges
                .insert("enhance_publication_blocked".into(), 1.);
            let r = d.packing_routers.get_mut("router").unwrap();
            r.ready = Some(false);
            r.placement = Report::parse(&report().replace("\"ready\":true", "\"ready\":false"));
            update(&mut d, at(t));
            d
        };
        let keys = [
            "coverage_domain_0_eligibility_warning",
            "coverage_placement_warning",
            "publication_failure_warning",
        ];
        // APM confirms placement 20-30 s after the publisher is eligible again,
        // since it samples the router and the coordinator every 15 s.
        let catching_up = |t| {
            let mut d = withdrawn(t);
            d.snapshot_gauges
                .insert("enhance_publication_eligible".into(), 1.);
            d.snapshot_gauges
                .insert("enhance_publication_blocked".into(), 0.);
            d
        };
        let mut store = Store::open(std::path::Path::new(":memory:")).unwrap();
        let mut run = |from: u64, to: u64, view: &dyn Fn(u64) -> DashboardData| {
            for t in (from..=to).step_by(5) {
                let d = view(t);
                assert!(d.placement_error);
                let conditions = monitoring::conditions(&d, t, &Policy::default())
                    .into_iter()
                    .filter(|c| keys.contains(&c.key.as_str()))
                    .collect::<Vec<_>>();
                store.evaluate(&conditions, t, true, "test", "").unwrap();
            }
            store
                .incidents()
                .unwrap()
                .into_iter()
                .filter(|i| i.active)
                .filter_map(|i| i.condition.map(|c| c.key))
                .collect::<Vec<_>>()
        };
        assert_eq!(run(1000, 1120, &withdrawn), ["publication_failure_warning"]);
        // The catch-up restarts the coverage hold, which a stuck router still exceeds.
        assert!(run(1125, 1165, &catching_up).is_empty());
        assert_eq!(
            run(1170, 1170, &catching_up),
            [
                "coverage_domain_0_eligibility_warning",
                "coverage_placement_warning"
            ]
        );
        // Without a fresh publisher scrape, the withdrawal explains nothing.
        let mut d = withdrawn(1125);
        d.scrape_error = Some("timeout".into());
        let conditions = monitoring::conditions(&d, 1125, &Policy::default());
        for key in &keys[..2] {
            assert_eq!(
                conditions.iter().find(|c| c.key == *key).unwrap().firing,
                Some(true),
                "{key}"
            );
        }
    }
}
