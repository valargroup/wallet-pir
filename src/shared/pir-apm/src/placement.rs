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
    pub generation: u64,
    manifest_sha256: String,
    domains: BTreeSet<u64>,
    eligible: Option<bool>,
}
#[derive(Clone, Copy, Debug, serde::Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Unavailable {
    MissingSample,
    StaleSample,
    GenerationDisagreement,
    ManifestDisagreement,
    MissingAssignments,
    Withdrawn,
}
impl Unavailable {
    fn text(self) -> &'static str {
        match self {
            Self::MissingSample => "missing sample",
            Self::StaleSample => "stale sample",
            Self::GenerationDisagreement => "generation disagreement",
            Self::ManifestDisagreement => "manifest disagreement",
            Self::MissingAssignments => "missing assignments",
            Self::Withdrawn => "known withdrawal; recovery in progress",
        }
    }
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
    match collect(data, now) {
        Ok((placement, replicas, at)) => {
            data.placement = Some(placement);
            data.domain_ready_replicas = replicas;
            data.placement_success = Some(at);
            data.placement_error = false;
            data.placement_unavailable = None;
        }
        Err(reason) => {
            data.placement_error = true;
            data.placement_unavailable = Some(reason);
        }
    }
}
fn collect(
    data: &crate::dashboard::DashboardData,
    now: SystemTime,
) -> Result<(Placement, BTreeMap<u64, f64>, SystemTime), Unavailable> {
    use Unavailable::*;
    let fresh = |at: SystemTime| {
        now.duration_since(at)
            .ok()
            .is_some_and(|age| age.as_secs() <= 45)
    };
    if data.scrape_error.is_some()
        || data.inventory_error.is_some()
        || data.packing_inventory_error.is_some()
    {
        return Err(MissingSample);
    }
    let mut at = data.last_scrape.ok_or(MissingSample)?;
    if !fresh(at) {
        return Err(StaleSample);
    }
    let published = data.enhance_publication.as_ref().ok_or(MissingSample)?;
    match published.eligible {
        Some(false) => return Err(Withdrawn),
        None => return Err(MissingSample),
        Some(true) => {}
    }
    if data.packing_routers.is_empty() {
        return Err(MissingAssignments);
    }
    let generation = *data
        .snapshot_gauges
        .get("enhance_published_generation")
        .ok_or(MissingSample)?;
    if !generation.is_finite() || generation <= 0. || generation.fract() != 0. {
        return Err(MissingSample);
    }
    if generation != published.generation as f64 {
        return Err(GenerationDisagreement);
    }
    let mut serving: Option<(u64, &str)> = None;
    let mut assignments: BTreeMap<u64, BTreeSet<String>> = BTreeMap::new();
    let mut eligible: BTreeMap<u64, BTreeSet<String>> = BTreeMap::new();
    for router in data.packing_routers.values() {
        let sampled = router.sample.success.ok_or(MissingSample)?;
        if !fresh(sampled) {
            return Err(StaleSample);
        }
        if router.sample.error.is_some() {
            return Err(MissingSample);
        }
        at = at.min(sampled);
        let report = router.placement.as_ref().ok_or(MissingAssignments)?;
        if report.generation > published.generation
            || serving.is_some_and(|s| s.0 != report.generation)
        {
            return Err(GenerationDisagreement);
        }
        if (report.generation == published.generation && report.active != published.manifest_sha256)
            || serving.is_some_and(|s| s.1 != report.active)
        {
            return Err(ManifestDisagreement);
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
                    return Err(MissingAssignments);
                }
                let name = names.into_iter().next().ok_or(MissingAssignments)?;
                assignments.entry(*domain).or_default().insert(name.clone());
                if *ready {
                    eligible.entry(*domain).or_default().insert(name);
                }
            }
        }
    }
    if assignments.keys().copied().collect::<BTreeSet<_>>() != published.domains {
        return Err(MissingAssignments);
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
    Ok((
        Placement {
            domains,
            revision: serving.map(|s| s.0),
            published_revision: Some(generation as u64),
        },
        replicas,
        at,
    ))
}

/// Bounded serving evidence; reachability remains a separate observation.
pub fn evidence(data: &crate::dashboard::DashboardData, now: u64) -> String {
    let age = |at: Option<SystemTime>| {
        at.and_then(|at| {
            at.duration_since(SystemTime::UNIX_EPOCH)
                .ok()
                .map(|at| now.saturating_sub(at.as_secs()))
        })
    };
    let number = |n: Option<u64>| n.map_or_else(|| "unknown".into(), |n| n.to_string());
    let mut text = format!(
        "placement {}; publisher generation {}, age {}s",
        data.placement_unavailable
            .map_or("observed", Unavailable::text),
        number(data.enhance_publication.as_ref().map(|p| p.generation)),
        number(age(data.last_scrape))
    );
    for (name, router) in data.packing_routers.iter().take(8) {
        text.push_str(&format!(
            "; router {name}: generation {}, age {}s",
            number(router.placement.as_ref().map(|p| p.generation)),
            number(age(router.sample.success))
        ));
    }
    text
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
            r#"{"generation":28049,"manifest_sha256":"ef33dcbb13d3c0b1f2a782dda4e5111666ba2f84e94a55d9ae1bcd5159e7f799","domains":[0],"eligible":true}"#,
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
    fn unavailable_placement_explains_withdrawal_staleness_and_missing_evidence() {
        let mut d = view(1000);
        d.enhance_publication.as_mut().unwrap().eligible = Some(false);
        update(&mut d, at(1001));
        assert_eq!(d.placement_unavailable, Some(Unavailable::Withdrawn));
        assert!(evidence(&d, 1001).contains("publisher generation 28049, age 1s"));
        d.enhance_publication.as_mut().unwrap().eligible = Some(true);
        update(&mut d, at(1046));
        assert_eq!(d.placement_unavailable, Some(Unavailable::StaleSample));
        d.last_scrape = None;
        update(&mut d, at(1046));
        assert_eq!(d.placement_unavailable, Some(Unavailable::MissingSample));
        let mut d = view(1000);
        d.packing_routers.get_mut("router").unwrap().placement = None;
        update(&mut d, at(1001));
        assert_eq!(
            d.placement_unavailable,
            Some(Unavailable::MissingAssignments)
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
            assert_eq!(
                d.placement_unavailable,
                Some(if future {
                    Unavailable::GenerationDisagreement
                } else {
                    Unavailable::ManifestDisagreement
                })
            );
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
}
