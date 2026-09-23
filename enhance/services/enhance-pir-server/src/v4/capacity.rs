//! Durable expansion demand. This predicts placement limits; workers still enforce memory admission.
use super::control::{assign, consolidate, Group, GrowthForecast};
use enhance_pir::{
    v4::{Geometry, Lifecycle, MAX_QUERY_SHARDS},
    RECORDS_PER_ROW,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug)]
pub struct Policy {
    pub fallback_rows_per_second: f64,
    pub readiness_seconds: f64,
    pub burst_rows: u64,
}
impl Default for Policy {
    fn default() -> Self {
        Self {
            fallback_rows_per_second: 1.,
            readiness_seconds: 21600.,
            burst_rows: 4096,
        }
    }
}
impl Policy {
    pub fn validate(self) -> Result<(), String> {
        if !self.fallback_rows_per_second.is_finite()
            || self.fallback_rows_per_second <= 0.
            || !self.readiness_seconds.is_finite()
            || self.readiness_seconds < 21600.
        {
            return Err("capacity policy needs positive finite fallback growth and at least six hours readiness".into());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Observation {
    pub records: u64,
    pub at: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Expansion {
    pub id: String,
    pub successor_ordinal: u64,
    pub target_groups: usize,
    pub requested_at: u64,
    pub boundary_records: u64,
    pub registered: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct MemoryLimit {
    pub records: u64,
    pub groups: usize,
    pub observed_at: u64,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Capacity {
    #[serde(default)]
    pub memory_limit: Option<MemoryLimit>,
    #[serde(default)]
    pub readiness_seconds: Option<f64>,
    #[serde(default)]
    pub burst_rows: Option<u64>,
    pub observation: Option<Observation>,
    pub peak_rows_per_second: f64,
    pub remaining_rows: Option<u64>,
    pub effective_rows_per_second: f64,
    pub fleet_ceiling_reached: bool,
    pub requested: Option<String>,
    pub requests: BTreeMap<String, Expansion>,
}

impl Capacity {
    /// A reservation's explicit memory refusal is an earlier limit, not a
    /// transport/availability failure. Keep one demand per next pair across retries.
    pub fn memory_refused(&mut self, records: u64, now: u64, groups: usize) -> Result<(), String> {
        if !(1..=4).contains(&groups) {
            return Err("invalid fleet inventory".into());
        }
        let records = self
            .memory_limit
            .as_ref()
            .filter(|limit| limit.groups == groups)
            .map_or(records, |limit| limit.records.min(records));
        self.memory_limit = Some(MemoryLimit {
            records,
            groups,
            observed_at: now,
        });
        self.remaining_rows = Some(0);
        self.fleet_ceiling_reached = groups == 4;
        for request in self.requests.values_mut() {
            request.registered |= groups >= request.target_groups;
        }
        self.requested = self
            .requests
            .values()
            .find(|r| !r.registered)
            .map(|r| r.id.clone());
        if groups < 4 && self.requested.is_none() {
            let target_groups = groups + 1;
            let ordinal = records / (Geometry::default().max_shard_rows * RECORDS_PER_ROW as u64);
            let id = format!("successor-{ordinal}-pair-{target_groups}");
            self.requests.insert(
                id.clone(),
                Expansion {
                    id: id.clone(),
                    successor_ordinal: ordinal,
                    target_groups,
                    requested_at: now,
                    boundary_records: records,
                    registered: false,
                },
            );
            self.requested = Some(id);
        }
        Ok(())
    }

    /// Reconcile on observations and timers. Outstanding requests survive reorgs;
    /// only registered capacity satisfies one. Never manufacture a second request
    /// for an already requested pair when the projected boundary changes.
    pub fn observe(
        &mut self,
        records: u64,
        now: u64,
        policy: Policy,
        groups: &[Group],
        lifecycle: &Lifecycle,
        previous: &BTreeMap<u64, String>,
    ) -> Result<(), String> {
        policy.validate()?;
        if groups.is_empty() || groups.len() > 4 {
            return Err("invalid fleet inventory".into());
        }
        self.readiness_seconds = Some(policy.readiness_seconds);
        self.burst_rows = Some(policy.burst_rows);
        if let Some(last) = &self.observation {
            if now > last.at && records >= last.records {
                let rate = (records - last.records) as f64
                    / RECORDS_PER_ROW as f64
                    / (now - last.at) as f64;
                self.peak_rows_per_second = self.peak_rows_per_second.max(rate);
            }
        }
        // A monotonic peak is intentionally conservative until an operator replaces
        // the policy/state. Zero appends and reorgs cannot erase a recent burst.
        self.effective_rows_per_second = self
            .peak_rows_per_second
            .max(policy.fallback_rows_per_second);
        // Preserve the baseline across same-second observations and clock rollback.
        if self.observation.as_ref().is_none_or(|last| now > last.at) {
            self.observation = Some(Observation { records, at: now });
        }
        for request in self.requests.values_mut() {
            request.registered |= groups.len() >= request.target_groups;
        }
        self.requested = self
            .requests
            .values()
            .find(|r| !r.registered)
            .map(|r| r.id.clone());
        let (mut boundary, mut ordinal) =
            placement_limit(records.max(1), groups, lifecycle, previous)?;
        if let Some(limit) = self
            .memory_limit
            .as_ref()
            .filter(|limit| limit.groups == groups.len())
        {
            if limit.records < boundary {
                boundary = limit.records;
                ordinal = boundary / (Geometry::default().max_shard_rows * RECORDS_PER_ROW as u64);
            }
        }
        let remaining = boundary
            .saturating_sub(records)
            .div_ceil(RECORDS_PER_ROW as u64);
        self.remaining_rows = Some(remaining);
        let due = GrowthForecast {
            rows_per_second: self.effective_rows_per_second,
            readiness_seconds: policy.readiness_seconds,
            burst_rows: policy.burst_rows,
            observed_at: now,
        }
        .expansion_due(remaining, now)?;
        self.fleet_ceiling_reached = groups.len() == 4 && due;
        if due && groups.len() < 4 && self.requested.is_none() {
            let target_groups = groups.len() + 1;
            let id = format!("successor-{ordinal}-pair-{target_groups}");
            self.requests.entry(id.clone()).or_insert(Expansion {
                id: id.clone(),
                successor_ordinal: ordinal,
                target_groups,
                requested_at: now,
                boundary_records: boundary,
                registered: false,
            });
            self.requested = Some(id);
        }
        Ok(())
    }
}

/// Preview the same count/role planner through successive loan and return states.
/// This is a capacity forecast, not evidence that retained runtime memory fits.
fn placement_limit(
    records: u64,
    groups: &[Group],
    lifecycle: &Lifecycle,
    previous: &BTreeMap<u64, String>,
) -> Result<(u64, u64), String> {
    let geometry = Geometry::default();
    let span = geometry.max_shard_rows * RECORDS_PER_ROW as u64;
    let loan = geometry.min_shard_rows * RECORDS_PER_ROW as u64;
    for group in groups {
        group.placement_policy.validate()?;
    }
    let ceiling: u64 = groups
        .iter()
        .map(|g| g.placement_policy.sealed_shards as u64)
        .sum::<u64>()
        .min(MAX_QUERY_SHARDS);
    if records >= ceiling * span {
        return Ok((ceiling * span, ceiling));
    }
    let mut lifecycle = lifecycle.clone();
    let coverage = lifecycle.coverage(records, geometry)?;
    let Ok(mut placements) = assign(&coverage, groups, previous) else {
        return Ok((records, records / span));
    };
    consolidate(&coverage, groups, &mut placements)?;
    // If observing an incoming loan, include its return before the next split.
    if let Some(active) = coverage.loan {
        let returned = lifecycle.coverage(active.return_at_records, geometry)?;
        placements = assign(&returned, groups, &placements)?;
        consolidate(&returned, groups, &mut placements)?;
    }
    for ordinal in records / span + 1..ceiling {
        let boundary = ordinal * span;
        let coverage = lifecycle.coverage(boundary, geometry)?;
        let Ok(next) = assign(&coverage, groups, &placements) else {
            return Ok((boundary, ordinal));
        };
        placements = next;
        let returned = lifecycle.coverage(boundary + loan, geometry)?;
        placements = assign(&returned, groups, &placements)?;
        consolidate(&returned, groups, &mut placements)?;
    }
    Ok((ceiling * span, ceiling))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn groups(count: usize) -> Vec<Group> {
        (0..count)
            .map(|i| Group {
                placement_policy: Default::default(),
                id: format!("g{i}"),
                sequence: i as u64,
                replicas: vec![],
                settling: false,
            })
            .collect()
    }
    #[test]
    fn seven_sealed_extends_historical_capacity_not_first_active_pair() {
        let span = 32768 * 33;
        for count in 1..=4 {
            let six = groups(count);
            let mut seven = six.clone();
            for g in &mut seven {
                g.placement_policy.sealed_shards = 7;
            }
            let before = placement_limit(1, &six, &Lifecycle::default(), &BTreeMap::new())
                .unwrap()
                .0;
            let after = placement_limit(1, &seven, &Lifecycle::default(), &BTreeMap::new())
                .unwrap()
                .0;
            assert_eq!(before, (5 + 6 * (count as u64 - 1)) * span);
            assert_eq!(
                after,
                (5 + 7 * (count as u64 - 1)).min(MAX_QUERY_SHARDS) * span
            );
        }
    }

    #[test]
    fn memory_limit_requests_once_survives_restart_and_expires_for_larger_fleet() {
        let mut capacity = Capacity::default();
        let policy = Policy {
            fallback_rows_per_second: 0.001,
            ..Policy::default()
        };
        capacity
            .observe(
                67,
                1,
                policy,
                &groups(1),
                &Lifecycle::default(),
                &BTreeMap::new(),
            )
            .unwrap();
        assert!(capacity.requested.is_none());
        capacity.memory_refused(67, 2, 1).unwrap();
        let id = capacity.requested.clone().unwrap();
        capacity.memory_refused(100, 3, 1).unwrap();
        assert_eq!(capacity.requests.len(), 1);
        assert_eq!(capacity.memory_limit.as_ref().unwrap().records, 67);
        let mut restored: Capacity =
            serde_json::from_slice(&serde_json::to_vec(&capacity).unwrap()).unwrap();
        restored
            .observe(
                67,
                4,
                policy,
                &groups(1),
                &Lifecycle::default(),
                &BTreeMap::new(),
            )
            .unwrap();
        assert_eq!(restored.remaining_rows, Some(0));
        assert_eq!(restored.requested, Some(id.clone()));
        restored
            .observe(
                67,
                5,
                policy,
                &groups(2),
                &Lifecycle::default(),
                &BTreeMap::new(),
            )
            .unwrap();
        assert!(restored.requests[&id].registered);
        assert!(restored.requested.is_none());
        assert!(restored.remaining_rows.unwrap() > 0);
        restored.memory_refused(67, 6, 4).unwrap();
        assert!(restored.fleet_ceiling_reached);
        assert!(restored.requested.is_none());
        assert_eq!(restored.requests.len(), 1);
    }

    #[test]
    fn clock_resolution_and_restarts_preserve_growth_and_demand() {
        let directory = tempfile::tempdir().unwrap();
        let mut store = super::super::control::Store::open(directory.path()).unwrap();
        store
            .update(|state| {
                state.groups = groups(1);
                for (records, now) in [(33, 100), (330, 100), (660, 99), (3300, 101)] {
                    state.capacity.observe(
                        records,
                        now,
                        Policy::default(),
                        &state.groups,
                        &state.lifecycle,
                        &state.assignments,
                    )?;
                }
                Ok(())
            })
            .unwrap();
        let saved = store.state().capacity.clone();
        assert_eq!(saved.peak_rows_per_second, 99.);
        assert!(saved.requested.is_some());
        drop(store);
        let reopened = super::super::control::Store::open(directory.path()).unwrap();
        assert_eq!(reopened.state().capacity, saved);
    }

    #[test]
    fn ceiling_never_requests_a_fifth_pair() {
        let mut capacity = Capacity::default();
        capacity
            .observe(
                24 * 32768 * 33,
                100,
                Policy::default(),
                &groups(4),
                &Lifecycle::default(),
                &BTreeMap::new(),
            )
            .unwrap();
        assert!(capacity.fleet_ceiling_reached);
        assert_eq!(capacity.remaining_rows, Some(0));
        assert!(capacity.requests.is_empty());
    }
    #[test]
    fn forecasts_lender_limit_and_uses_existing_spare_pair() {
        let span = 32768 * 33;
        let mut capacity = Capacity::default();
        let lifecycle = Lifecycle::default();
        let policy = Policy::default();
        capacity
            .observe(
                4 * span + 16000 * 33,
                100,
                policy,
                &groups(1),
                &lifecycle,
                &BTreeMap::new(),
            )
            .unwrap();
        let request = &capacity.requests[capacity.requested.as_ref().unwrap()];
        assert_eq!(request.boundary_records, 5 * span);
        assert_eq!(request.target_groups, 2);
        let id = request.id.clone();
        capacity
            .observe(
                4 * span + 17000 * 33,
                200,
                policy,
                &groups(1),
                &lifecycle,
                &BTreeMap::new(),
            )
            .unwrap();
        assert_eq!(capacity.requested.as_deref(), Some(id.as_str()));
        capacity
            .observe(span, 300, policy, &groups(1), &lifecycle, &BTreeMap::new())
            .unwrap();
        assert_eq!(
            capacity.requests.len(),
            1,
            "reorg must not forget resource demand identity"
        );
        capacity
            .observe(span, 400, policy, &groups(2), &lifecycle, &BTreeMap::new())
            .unwrap();
        assert!(capacity.requests[&id].registered);
        assert!(capacity.requested.is_none());
    }
    #[test]
    fn missing_growth_is_nonzero_and_requests_survive_serialization() {
        let mut c = Capacity::default();
        c.observe(
            5 * 32768 * 33 - 1,
            100,
            Policy::default(),
            &groups(1),
            &Lifecycle::default(),
            &BTreeMap::new(),
        )
        .unwrap();
        assert_eq!(c.effective_rows_per_second, 1.);
        let mut restored: Capacity =
            serde_json::from_slice(&serde_json::to_vec(&c).unwrap()).unwrap();
        restored
            .observe(
                5 * 32768 * 33 - 1,
                10000,
                Policy::default(),
                &groups(1),
                &Lifecycle::default(),
                &BTreeMap::new(),
            )
            .unwrap();
        assert_eq!(c.requests, restored.requests);
        assert!(Policy {
            fallback_rows_per_second: 0.,
            ..Policy::default()
        }
        .validate()
        .is_err());
    }
}
