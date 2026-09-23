//! Read-only snapshots. No query positions, session IDs or request-derived labels.
use super::control::{self, Role, State};
use enhance_pir::v4::{Manifest, ShardState};
use prometheus::{core::Collector, Encoder, GaugeVec, Opts, TextEncoder};
use std::collections::BTreeMap;
use std::time::Instant;

#[derive(Default)]
pub(super) struct Metrics(BTreeMap<&'static str, GaugeVec>);
impl Metrics {
    pub fn set(&mut self, name: &'static str, labels: &[(&str, &str)], value: impl Into<f64>) {
        self.0
            .entry(name)
            .or_insert_with(|| {
                GaugeVec::new(
                    Opts::new(name, name),
                    &labels.iter().map(|(k, _)| *k).collect::<Vec<_>>(),
                )
                .expect("static metric definition")
            })
            .with_label_values(&labels.iter().map(|(_, v)| *v).collect::<Vec<_>>())
            .set(value.into());
    }
    pub fn number(&mut self, name: &'static str, value: u64) {
        self.set(name, &[], value as f64);
    }
    pub fn finish(self) -> String {
        let families = self
            .0
            .values()
            .flat_map(|v| v.collect())
            .collect::<Vec<_>>();
        let mut bytes = Vec::new();
        TextEncoder::new()
            .encode(&families, &mut bytes)
            .expect("encode metric snapshot");
        String::from_utf8(bytes).expect("metrics are UTF-8")
    }
}

pub(super) struct Target {
    height: u64,
    records: u64,
    hash: String,
    first_attempt: Instant,
}
#[derive(Default)]
pub(super) struct Publication {
    target: Option<Target>,
    pub last_attempt_seconds: Option<f64>,
    pub last_attempt_succeeded: Option<bool>,
}
impl Publication {
    pub fn observe(&mut self, height: u64, records: u64, hash: &str) {
        if self
            .target
            .as_ref()
            .is_none_or(|t| t.height != height || t.records != records || t.hash != hash)
        {
            self.target = Some(Target {
                height,
                records,
                hash: hash.into(),
                first_attempt: Instant::now(),
            });
        }
    }
}

pub(super) fn coordinator(
    state: &State,
    manifest: Option<&Manifest>,
    routes: &BTreeMap<u64, usize>,
    blocked: bool,
    publication: &Publication,
    now: u64,
) -> String {
    let mut m = Metrics::default();
    for (name, value) in [
        (
            "enhance_v4_retained_generations",
            state.published.len() as u64,
        ),
        ("enhance_v4_publication_blocked", u64::from(blocked)),
        ("enhance_v4_placement_revision", state.revision),
        ("enhance_v4_controller_epoch", state.epoch),
        ("enhance_v4_registered_groups", state.groups.len() as u64),
        (
            "enhance_v4_draining_operations",
            state.draining.len() as u64,
        ),
        (
            "enhance_v4_pending_commit_notifications",
            state.pending_commits.len() as u64,
        ),
        (
            "enhance_v4_pending_abort_notifications",
            state.pending_aborts.len() as u64,
        ),
        (
            "enhance_v4_fleet_ceiling_reached",
            u64::from(state.capacity.fleet_ceiling_reached),
        ),
        (
            "enhance_v4_expansion_requests_pending",
            state
                .capacity
                .requests
                .values()
                .filter(|r| !r.registered)
                .count() as u64,
        ),
        (
            "enhance_v4_capacity_observation_available",
            u64::from(state.capacity.observation.is_some()),
        ),
        (
            "enhance_v4_publication_target_available",
            u64::from(publication.target.is_some()),
        ),
    ] {
        m.number(name, value);
    }
    let phase = state
        .operation
        .as_ref()
        .map_or("NONE".into(), |op| format!("{:?}", op.phase).to_uppercase());
    for label in [
        "NONE",
        "PLANNED",
        "RESERVED",
        "PREPARING",
        "READY",
        "COMMITTED",
        "DRAINING",
        "COMPLETE",
        "ABORTED",
    ] {
        m.set(
            "enhance_v4_operation_phase",
            &[("phase", label)],
            f64::from(label == phase),
        );
    }
    if let Some(observation) = &state.capacity.observation {
        m.number(
            "enhance_v4_capacity_observation_age_seconds",
            now.saturating_sub(observation.at),
        );
        m.number(
            "enhance_v4_capacity_clock_regression",
            u64::from(now < observation.at),
        );
        m.set(
            "enhance_v4_growth_rows_per_second",
            &[],
            state.capacity.effective_rows_per_second,
        );
        m.set(
            "enhance_v4_peak_growth_rows_per_second",
            &[],
            state.capacity.peak_rows_per_second,
        );
    }
    if let Some(seconds) = state.capacity.readiness_seconds {
        m.set(
            "enhance_v4_expansion_readiness_budget_seconds",
            &[],
            seconds,
        );
    }
    if let Some(rows) = state.capacity.burst_rows {
        m.number("enhance_v4_expansion_burst_budget_rows", rows);
    }
    if let Some(remaining) = state.capacity.remaining_rows {
        m.number("enhance_v4_capacity_remaining_rows", remaining);
        if state.capacity.effective_rows_per_second > 0. {
            m.set(
                "enhance_v4_capacity_forecast_seconds",
                &[],
                remaining as f64 / state.capacity.effective_rows_per_second,
            );
        }
    }
    if let Some(request) = state
        .capacity
        .requested
        .as_ref()
        .and_then(|id| state.capacity.requests.get(id))
    {
        m.number(
            "enhance_v4_expansion_target_groups",
            request.target_groups as u64,
        );
        m.number(
            "enhance_v4_expansion_request_age_seconds",
            now.saturating_sub(request.requested_at),
        );
    }
    if let Some(seconds) = publication.last_attempt_seconds {
        m.set("enhance_v4_last_publication_attempt_seconds", &[], seconds);
    }
    if let Some(succeeded) = publication.last_attempt_succeeded {
        m.number(
            "enhance_v4_last_publication_attempt_succeeded",
            u64::from(succeeded),
        );
    }
    if let Some(target) = &publication.target {
        let current = manifest.is_some_and(|p| {
            p.anchor_height == target.height
                && p.anchor_block_hash == target.hash
                && p.coverage.records == target.records
        });
        m.number("enhance_v4_publication_target_current", u64::from(current));
        m.number("enhance_v4_publication_target_height", target.height);
        m.set(
            "enhance_v4_publication_pending_seconds",
            &[],
            if current {
                0.
            } else {
                target.first_attempt.elapsed().as_secs_f64()
            },
        );
        if let Some(published) = manifest {
            m.set(
                "enhance_v4_publication_target_height_delta",
                &[],
                (target.height as i128 - published.anchor_height as i128) as f64,
            );
            m.set(
                "enhance_v4_publication_target_records_delta",
                &[],
                (target.records as i128 - published.coverage.records as i128) as f64,
            );
        }
    }
    let pending = state.pending_replicas();
    if let Some(manifest) = manifest {
        m.number("enhance_v4_published_generation", manifest.generation);
        m.number("enhance_v4_published_anchor_height", manifest.anchor_height);
        m.number(
            "enhance_v4_loan_active",
            u64::from(manifest.coverage.loan.is_some()),
        );
        let span = manifest.geometry.max_shard_rows * enhance_pir::RECORDS_PER_ROW as u64;
        m.number(
            "enhance_v4_rows_before_next_carveout",
            (span - manifest.coverage.records % span).div_ceil(enhance_pir::RECORDS_PER_ROW as u64),
        );
        if let Some(loan) = &manifest.coverage.loan {
            m.number(
                "enhance_v4_rows_before_loan_return",
                loan.return_at_records
                    .saturating_sub(manifest.coverage.records)
                    .div_ceil(enhance_pir::RECORDS_PER_ROW as u64),
            );
        }
        for shard in &manifest.coverage.shards {
            let id = shard.id.to_string();
            for (name, value) in [
                ("enhance_v4_shard_logical_rows", shard.logical_rows),
                (
                    "enhance_v4_shard_used_rows",
                    shard.records.div_ceil(enhance_pir::RECORDS_PER_ROW as u64),
                ),
                (
                    "enhance_v4_shard_published_ready_replicas",
                    *routes.get(&shard.id).unwrap_or(&0) as u64,
                ),
            ] {
                m.set(name, &[("shard", &id)], value as f64);
            }
            for (label, status) in [
                ("growing", ShardState::Growing),
                ("lending", ShardState::Lending),
                ("sealed", ShardState::Sealed),
            ] {
                m.set(
                    "enhance_v4_shard_state",
                    &[("shard", &id), ("state", label)],
                    f64::from(shard.state == status),
                );
            }
            let borrower = manifest
                .coverage
                .loan
                .as_ref()
                .is_some_and(|l| l.borrower == shard.id);
            m.set(
                "enhance_v4_shard_borrowing",
                &[("shard", &id)],
                f64::from(borrower),
            );
            for unit in &shard.units {
                let start = unit.local_row_start.to_string();
                m.set(
                    "enhance_v4_unit_allocated_rows",
                    &[("shard", &id), ("local_row_start", &start)],
                    unit.allocated_rows as f64,
                );
                m.set(
                    "enhance_v4_unit_used_rows",
                    &[("shard", &id), ("local_row_start", &start)],
                    unit.used_rows as f64,
                );
            }
        }
        let mut planned = state.assignments.clone();
        let preview = control::consolidate(&manifest.coverage, &state.groups, &mut planned);
        m.number(
            "enhance_v4_consolidation_preview_available",
            u64::from(preview.is_ok()),
        );
        if preview.is_ok() {
            m.number(
                "enhance_v4_consolidation_planner_moves",
                planned
                    .iter()
                    .filter(|(shard, group)| state.assignments.get(shard) != Some(group))
                    .count() as u64,
            );
        }
    }
    for group in &state.groups {
        let role = manifest.map_or(Ok(Role::Standby), |p| {
            group.role(&p.coverage, &state.assignments)
        });
        m.set(
            "enhance_v4_group_role_available",
            &[("group", &group.id)],
            f64::from(role.is_ok()),
        );
        if let Ok(role) = role {
            for (label, value) in [
                ("standby", Role::Standby),
                ("active", Role::Active),
                ("settling", Role::Settling),
                ("sealed_open", Role::SealedOpen),
                ("sealed_full", Role::SealedFull),
            ] {
                m.set(
                    "enhance_v4_group_role",
                    &[("group", &group.id), ("role", label)],
                    f64::from(role == value),
                );
            }
        }
        m.set(
            "enhance_v4_group_assigned_shards",
            &[("group", &group.id)],
            state
                .assignments
                .values()
                .filter(|g| **g == group.id)
                .count() as f64,
        );
        m.set(
            "enhance_v4_group_sealed_shards",
            &[("group", &group.id)],
            manifest.map_or(0, |p| {
                p.coverage
                    .shards
                    .iter()
                    .filter(|s| {
                        s.state == ShardState::Sealed
                            && state.assignments.get(&s.id) == Some(&group.id)
                    })
                    .count()
            }) as f64,
        );
        m.set(
            "enhance_v4_group_pending_replicas",
            &[("group", &group.id)],
            group
                .replicas
                .iter()
                .filter(|r| pending.contains(&r.name))
                .count() as f64,
        );
    }
    m.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use enhance_pir::v4::{Geometry, Lifecycle, PROTOCOL_REVISION, SCHEMA_VERSION};
    fn manifest(records: u64) -> Manifest {
        Manifest {
            schema_version: SCHEMA_VERSION,
            protocol_revision: PROTOCOL_REVISION.into(),
            network: "main".into(),
            pool: "ironwood".into(),
            generation: 1,
            anchor_height: 10,
            anchor_block_hash: "01".repeat(32),
            geometry: Geometry::default(),
            coverage: Lifecycle::default()
                .coverage(records, Geometry::default())
                .unwrap(),
            sessions: vec![],
            unit_identities: BTreeMap::new(),
        }
    }
    #[test]
    fn geometry_roles_and_escaped_labels_survive_loan_return() {
        let mut state = State::default();
        let name = "group\"\\\nname";
        state.groups.push(control::Group {
            placement_policy: Default::default(),
            id: name.into(),
            sequence: 0,
            replicas: vec![],
            settling: false,
        });
        for (records, loan) in [
            (4 * 32768 * 33 + 4096 * 33 / 2, true),
            (4 * 32768 * 33 + 4096 * 33, false),
        ] {
            let published = manifest(records);
            state.assignments =
                control::assign(&published.coverage, &state.groups, &BTreeMap::new()).unwrap();
            state.published = vec![published.clone()];
            let text = coordinator(
                &state,
                Some(&published),
                &BTreeMap::new(),
                false,
                &Publication::default(),
                100,
            );
            assert!(text.contains(&format!("enhance_v4_loan_active {}\n", u8::from(loan))));
            assert!(text
                .contains("enhance_v4_group_assigned_shards{group=\"group\\\"\\\\\\nname\"} 5\n"));
            assert!(text.contains(
                "enhance_v4_group_role{group=\"group\\\"\\\\\\nname\",role=\"active\"} 1\n"
            ));
            assert!(
                text.contains(
                    "enhance_v4_unit_allocated_rows{local_row_start=\"0\",shard=\"4\"} 4096\n"
                ) || text.contains(
                    "enhance_v4_unit_allocated_rows{local_row_start=\"0\",shard=\"4\"} 8192\n"
                )
            );
            assert!(text
                .lines()
                .filter(|line| !line.starts_with('#'))
                .all(|line| line.starts_with("enhance_v4_")));
            assert!(!text.contains("enhance_v4_growth_rows_per_second "));
        }
    }
    #[test]
    fn publication_targets_detect_reorg_and_preserve_retry_age() {
        let published = manifest(67);
        let mut observations = Publication::default();
        let state = State::default();
        let text = coordinator(
            &state,
            Some(&published),
            &BTreeMap::new(),
            false,
            &observations,
            100,
        );
        assert!(text.contains("enhance_v4_publication_target_available 0\n"));
        assert!(!text.contains("enhance_v4_publication_target_height_delta "));
        observations.observe(10, 67, &published.anchor_block_hash);
        let at = observations.target.as_ref().unwrap().first_attempt;
        observations.observe(10, 67, &published.anchor_block_hash);
        assert_eq!(observations.target.as_ref().unwrap().first_attempt, at);
        let text = coordinator(
            &state,
            Some(&published),
            &BTreeMap::new(),
            false,
            &observations,
            100,
        );
        assert!(text.contains("enhance_v4_publication_target_current 1\n"));
        observations.observe(10, 67, &"02".repeat(32));
        let text = coordinator(
            &state,
            Some(&published),
            &BTreeMap::new(),
            true,
            &observations,
            100,
        );
        assert!(text.contains("enhance_v4_publication_target_current 0\n"));
        observations.observe(9, 33, &"03".repeat(32));
        let text = coordinator(
            &state,
            Some(&published),
            &BTreeMap::new(),
            true,
            &observations,
            100,
        );
        assert!(text.contains("enhance_v4_publication_target_height_delta -1\n"));
        assert!(text.contains("enhance_v4_publication_target_records_delta -34\n"));
    }
}
