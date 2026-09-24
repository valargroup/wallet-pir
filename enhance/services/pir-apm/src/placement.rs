//! Current per-domain worker placement from coordinator health. Router domain
//! assignments come from its configured router inventory, not worker groups.
use std::{
    collections::{BTreeMap, BTreeSet},
    time::SystemTime,
};

#[derive(Clone, Debug, Default)]
pub struct Placement {
    pub domains: BTreeMap<u64, Vec<String>>,
    pub revision: Option<u64>,
}

pub fn parse(body: &str) -> Result<Option<Placement>, &'static str> {
    let value: serde_json::Value =
        serde_json::from_str(body).map_err(|_| "Invalid coordinator health")?;
    if !value.is_object() {
        return Err("Invalid coordinator health");
    }
    let Some(pool) = value.get("pool").filter(|p| !p.is_null()) else {
        return Ok(None);
    };
    let assignments = pool["placements"]
        .as_object()
        .ok_or("Domain placements unavailable")?;
    let mut result = Placement {
        revision: value["placement_revision"].as_u64(),
        ..Default::default()
    };
    for (id, workers) in assignments {
        let domain = id.parse::<u64>().map_err(|_| "Invalid domain ID")?;
        if id != &domain.to_string() {
            return Err("Invalid domain ID");
        }
        let mut names = BTreeSet::new();
        let workers = workers
            .as_array()
            .ok_or("Invalid domain workers")?
            .iter()
            .map(|worker| {
                let name = worker
                    .as_str()
                    .filter(|s| {
                        !s.is_empty()
                            && s.len() <= 128
                            && s.bytes()
                                .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
                    })
                    .ok_or("Invalid worker name")?;
                if !names.insert(name.to_string()) {
                    return Err("Duplicate domain worker");
                }
                Ok(name.to_string())
            })
            .collect::<Result<Vec<_>, _>>()?;
        result.domains.insert(domain, workers);
    }
    Ok(Some(result))
}

pub fn update(data: &mut crate::dashboard::DashboardData, body: Option<&str>) {
    match body.ok_or("Coordinator health unavailable").and_then(parse) {
        Ok(placement) => {
            data.placement = placement;
            data.placement_success = Some(SystemTime::now());
            data.placement_error = false;
        }
        Err(_) => data.placement_error = true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parses_numeric_domains_and_rejects_ambiguous_membership() {
        let placement = parse(r#"{"placement_revision":7,"pool":{"placements":{"10":["worker-b"],"2":["worker-a","worker-b"]}}}"#).unwrap().unwrap();
        assert_eq!(placement.revision, Some(7));
        assert_eq!(
            placement.domains.keys().copied().collect::<Vec<_>>(),
            vec![2, 10]
        );
        for input in [
            r#"{"pool":{"placements":{"01":["w"]}}}"#,
            r#"{"pool":{"placements":{"1":["w","w"]}}}"#,
            r#"{"pool":{"placements":{"1":["../w"]}}}"#,
            r#"{"pool":{}}"#,
        ] {
            assert!(parse(input).is_err());
        }
        assert!(parse(r#"{"pool":null}"#).unwrap().is_none());
    }
    #[test]
    fn failed_health_keeps_assignments_without_refreshing_timestamp() {
        let mut data = crate::dashboard::DashboardData::new(
            "APM".into(),
            crate::schema::Schema::enhance_default(),
            "test".into(),
            "host".into(),
            crate::host::HostHealth::default(),
        );
        update(&mut data, Some(r#"{"pool":{"placements":{"0":["w"]}}}"#));
        let success = data.placement_success;
        update(&mut data, None);
        assert!(data.placement_error);
        assert_eq!(data.placement_success, success);
        assert_eq!(data.placement.as_ref().unwrap().domains[&0], vec!["w"]);
        update(&mut data, Some(r#"{"pool":{"placements":{}}}"#));
        assert!(!data.placement_error);
        assert!(data.placement.as_ref().unwrap().domains.is_empty());
    }
}
