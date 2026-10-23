//! Router-owned domain assignments and eligibility for the published generation.
use std::{
    collections::{BTreeMap, BTreeSet},
    time::SystemTime,
};

#[derive(Clone, Debug, Default)]
pub struct Placement {
    pub domains: BTreeMap<u64, Vec<String>>,
    pub revision: Option<u64>,
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
        })
    }
}

/// Keep the last valid placement for display, but never refresh it from stale,
/// inconsistent or unconfigured serving observations.
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
    let mut assignments: BTreeMap<u64, BTreeSet<String>> = BTreeMap::new();
    let mut eligible: BTreeMap<u64, BTreeSet<String>> = BTreeMap::new();
    for router in data.packing_routers.values() {
        let sampled = router.sample.success.filter(|at| fresh(*at))?;
        if router.sample.error.is_some() {
            return None;
        }
        at = at.min(sampled);
        let report = router.placement.as_ref()?;
        if report.generation != published.generation || report.active != published.manifest_sha256 {
            return None;
        }
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
            revision: Some(generation as u64),
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
}
