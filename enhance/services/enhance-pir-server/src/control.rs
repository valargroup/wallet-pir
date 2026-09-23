//! Durable single-writer publication decisions and conservative per-replica admission.
use enhance_pir::protocol::{
    digest, Coverage, Geometry, Lifecycle, Manifest, ShardState, RETAINED_GENERATIONS,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};

pub const MIB: u64 = 1024 * 1024;
pub const RESIDENT_LIMIT: u64 = 7 * 1024 * MIB - 512 * MIB;
pub const OVERHEAD: u64 = 728 * MIB;

/// Versioned deployment policy. Seven sealed shards require hardware qualification.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PlacementPolicy {
    pub sealed_shards: usize,
}
impl Default for PlacementPolicy {
    fn default() -> Self {
        Self { sealed_shards: 6 }
    }
}
impl PlacementPolicy {
    pub fn validate(self) -> Result<(), String> {
        if !matches!(self.sealed_shards, 6 | 7) {
            return Err("sealed shard limit must be six or seven".into());
        }
        Ok(())
    }
    pub fn limit(self, active: bool) -> usize {
        if active {
            5
        } else {
            self.sealed_shards
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Ledger {
    /// Runtime IDs include complete cache identity; shared allocations are counted once.
    pub runtimes: BTreeMap<String, u64>,
    pub reservations: BTreeMap<String, u64>,
    pub pinned: BTreeSet<String>,
}

impl Ledger {
    pub fn total(&self) -> Result<u64, String> {
        self.runtimes
            .values()
            .chain(self.reservations.values())
            .try_fold(OVERHEAD, |sum, n| {
                sum.checked_add(*n)
                    .ok_or_else(|| "memory accounting overflow".into())
            })
    }
    pub fn reserve(&mut self, id: &str, bytes: u64, limit: u64) -> Result<(), String> {
        if let Some(existing) = self.reservations.get(id) {
            return if *existing == bytes {
                Ok(())
            } else {
                Err("conflicting reservation".into())
            };
        }
        if self
            .total()?
            .checked_add(bytes)
            .ok_or("memory accounting overflow")?
            > limit
        {
            return Err("worker memory admission refused".into());
        }
        self.reservations.insert(id.into(), bytes);
        Ok(())
    }
    pub fn materialize(
        &mut self,
        reservation: &str,
        runtime: &str,
        bytes: u64,
    ) -> Result<(), String> {
        if let Some(existing) = self.runtimes.get(runtime) {
            return if *existing == bytes {
                Ok(())
            } else {
                Err("conflicting runtime size".into())
            };
        }
        let reserved = self
            .reservations
            .get_mut(reservation)
            .ok_or("missing reservation")?;
        *reserved = reserved
            .checked_sub(bytes)
            .ok_or("runtime exceeds reservation")?;
        self.runtimes.insert(runtime.into(), bytes);
        Ok(())
    }
    pub fn reclaim(&mut self, runtime: &str, referenced: bool) -> Result<(), String> {
        if referenced || self.pinned.contains(runtime) {
            return Err("runtime still referenced".into());
        }
        self.runtimes.remove(runtime);
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Role {
    Standby,
    Active,
    Settling,
    SealedOpen,
    SealedFull,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Replica {
    pub name: String,
    pub url: String,
    pub incarnation: String,
    pub ledger: Ledger,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Group {
    #[serde(default)]
    pub placement_policy: PlacementPolicy,
    pub id: String,
    pub sequence: u64,
    pub replicas: Vec<Replica>,
    pub settling: bool,
}

impl Group {
    pub fn role(
        &self,
        coverage: &Coverage,
        assignments: &BTreeMap<u64, String>,
    ) -> Result<Role, String> {
        self.placement_policy.validate()?;
        let shards: Vec<_> = coverage
            .shards
            .iter()
            .filter(|s| assignments.get(&s.id) == Some(&self.id))
            .collect();
        let growing = shards
            .iter()
            .filter(|s| s.state == ShardState::Growing)
            .count();
        let active = shards.iter().any(|s| s.state != ShardState::Sealed);
        if growing > 1 || shards.len() > self.placement_policy.limit(active) {
            return Err("group shard limit exceeded".into());
        }
        Ok(if growing > 0 {
            Role::Active
        } else if active || self.settling {
            Role::Settling
        } else if shards.is_empty() {
            Role::Standby
        } else if shards.len() == self.placement_policy.sealed_shards {
            Role::SealedFull
        } else {
            Role::SealedOpen
        })
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Phase {
    Planned,
    Reserved,
    Preparing,
    Ready,
    Committed,
    Draining,
    Complete,
    Aborted,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReadyAck {
    pub replica: String,
    pub incarnation: String,
    pub candidate_digest: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Operation {
    pub id: String,
    pub attempt: u64,
    pub epoch: u64,
    pub expected_revision: u64,
    pub candidate_digest: String,
    pub phase: Phase,
    pub assignments: BTreeMap<u64, String>,
    pub affected_groups: BTreeSet<String>,
    pub source_generations: Vec<u64>,
    pub require_both: BTreeSet<String>,
    pub readiness: Vec<ReadyAck>,
    pub infrastructure_resources: BTreeMap<String, String>,
}

/// Durable notification of an already committed decision. At most one per worker;
/// until acknowledged that worker cannot accept another candidate or retention set.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PendingCommit {
    pub replica: String,
    pub operation: String,
    pub attempt: u64,
    pub manifest: Manifest,
    pub retained: Vec<u64>,
}

/// A cancelled attempt remains charged on this worker until it acknowledges
/// the abort. The worker is excluded from new candidates and retention updates.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PendingAbort {
    pub replica: String,
    pub operation: String,
    pub attempt: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct State {
    #[serde(default)]
    pub capacity: super::capacity::Capacity,
    pub version: u16,
    pub epoch: u64,
    pub revision: u64,
    pub next_generation: u64,
    pub next_attempt: u64,
    pub lifecycle: Lifecycle,
    pub groups: Vec<Group>,
    pub assignments: BTreeMap<u64, String>,
    pub published: Vec<Manifest>,
    pub operation: Option<Operation>,
    pub draining: Vec<Operation>,
    #[serde(default)]
    pub pending_commits: Vec<PendingCommit>,
    #[serde(default)]
    pub pending_aborts: Vec<PendingAbort>,
    pub completed: BTreeMap<String, String>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            capacity: super::capacity::Capacity::default(),
            version: 6,
            epoch: 0,
            revision: 0,
            next_generation: 1,
            next_attempt: 0,
            lifecycle: Lifecycle::default(),
            groups: Vec::new(),
            assignments: BTreeMap::new(),
            published: Vec::new(),
            operation: None,
            draining: Vec::new(),
            pending_commits: Vec::new(),
            pending_aborts: Vec::new(),
            completed: BTreeMap::new(),
        }
    }
}

impl State {
    pub fn pending_replicas(&self) -> BTreeSet<String> {
        self.pending_commits
            .iter()
            .map(|p| p.replica.clone())
            .chain(self.pending_aborts.iter().map(|p| p.replica.clone()))
            .collect()
    }

    pub(super) fn cancel_participants(&mut self, op: &Operation, committed: &BTreeSet<String>) {
        let pending = self.pending_replicas();
        for replica in self
            .groups
            .iter()
            .filter(|g| op.affected_groups.contains(&g.id))
            .flat_map(|g| &g.replicas)
        {
            // An older pending decision excluded this worker from the attempt.
            // Preserve that decision rather than issuing a conflicting abort.
            if !pending.contains(&replica.name) && !committed.contains(&replica.name) {
                self.pending_aborts.push(PendingAbort {
                    replica: replica.name.clone(),
                    operation: op.id.clone(),
                    attempt: op.attempt,
                });
            }
        }
    }
}

/// The lock remains held for the store's lifetime. All publication state shares one atomic file.
pub struct Store {
    path: PathBuf,
    _lock: File,
    state: State,
}

impl Store {
    pub fn open(directory: &Path) -> Result<Self, String> {
        fs::create_dir_all(directory).map_err(|e| e.to_string())?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(directory.join("controller.lock"))
            .map_err(|e| e.to_string())?;
        lock.try_lock()
            .map_err(|e| format!("controller already running: {e}"))?;
        let path = directory.join("controller.json");
        let state = if path.exists() {
            serde_json::from_slice(&fs::read(&path).map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?
        } else {
            State::default()
        };
        let mut store = Self {
            path,
            _lock: lock,
            state,
        };
        store.update(|s| {
            if s.version != 6 {
                return Err(
                    "incompatible controller state; rebuild protocol v6 in a separate data directory"
                        .into(),
                );
            }
            s.epoch = s.epoch.checked_add(1).ok_or("controller epoch exhausted")?;
            Ok(())
        })?;
        Ok(store)
    }
    pub fn state(&self) -> &State {
        &self.state
    }
    pub fn update<T>(
        &mut self,
        apply: impl FnOnce(&mut State) -> Result<T, String>,
    ) -> Result<T, String> {
        let mut next = self.state.clone();
        let value = apply(&mut next)?;
        crate::artifact::write_atomic(self.path.parent().unwrap(), "controller.json", |file| {
            serde_json::to_writer(file, &next).map_err(std::io::Error::other)
        })
        .map_err(|e| e.to_string())?;
        File::open(self.path.parent().unwrap())
            .and_then(|f| f.sync_all())
            .map_err(|e| e.to_string())?;
        self.state = next;
        Ok(value)
    }

    pub fn plan(&mut self, mut operation: Operation) -> Result<(), String> {
        self.update(|s| {
            if operation.epoch != s.epoch || operation.expected_revision != s.revision {
                return Err("stale controller epoch or revision".into());
            }
            if let Some(done) = s.completed.get(&operation.id) {
                return if done == &operation.candidate_digest {
                    Ok(())
                } else {
                    Err("conflicting completed operation".into())
                };
            }
            if let Some(existing) = &s.operation {
                return if existing == &operation {
                    Ok(())
                } else {
                    Err("only one candidate may prepare".into())
                };
            }
            if operation.phase != Phase::Planned || !operation.readiness.is_empty() {
                return Err("operation must start planned".into());
            }
            operation.readiness.clear();
            if operation.attempt != s.next_attempt {
                return Err("stale attempt identity".into());
            }
            s.next_attempt = s
                .next_attempt
                .checked_add(1)
                .ok_or("attempt identity exhausted")?;
            s.operation = Some(operation);
            Ok(())
        })
    }

    pub fn advance(
        &mut self,
        epoch: u64,
        id: &str,
        attempt: u64,
        phase: Phase,
    ) -> Result<(), String> {
        self.update(|s| {
            let op = s.operation.as_mut().ok_or("no operation")?;
            if epoch != s.epoch || op.epoch != epoch || op.id != id || op.attempt != attempt {
                return Err("stale operation command".into());
            }
            if op.phase == phase {
                return Ok(());
            }
            if !matches!(
                (op.phase, phase),
                (Phase::Planned, Phase::Reserved)
                    | (Phase::Reserved, Phase::Preparing)
                    | (Phase::Preparing, Phase::Ready)
                    | (Phase::Committed, Phase::Draining)
                    | (Phase::Draining, Phase::Complete)
            ) {
                return Err("invalid operation transition".into());
            }
            op.phase = phase;
            Ok(())
        })
    }

    /// Persist abort delivery before releasing the controller's candidate slot.
    /// Unacknowledged workers keep their own candidate/reservation and are excluded
    /// from new attempts. A committed decision can never take this path.
    pub fn abort_operation(&mut self) -> Result<(), String> {
        self.update(|s| {
            let Some(op) = s.operation.take() else {
                return Ok(());
            };
            if matches!(
                op.phase,
                Phase::Committed | Phase::Draining | Phase::Complete
            ) {
                return Err("cannot abort a committed operation".into());
            }
            s.cancel_participants(&op, &BTreeSet::new());
            Ok(())
        })
    }

    pub fn commit(&mut self, manifest: Manifest, lifecycle: Lifecycle) -> Result<(), String> {
        manifest.validate()?;
        self.update(|s| {
            let op = s.operation.as_mut().ok_or("no candidate")?;
            if op.phase != Phase::Ready
                || op.epoch != s.epoch
                || op.expected_revision != s.revision
                || op.candidate_digest != digest(&manifest)
                || manifest.generation != s.next_generation
            {
                return Err("candidate is stale or not ready".into());
            }
            if op.assignments.len() != manifest.coverage.shards.len()
                || manifest
                    .coverage
                    .shards
                    .iter()
                    .any(|shard| !op.assignments.contains_key(&shard.id))
            {
                return Err("incomplete assignment".into());
            }
            for group_id in op.assignments.values().collect::<BTreeSet<_>>() {
                let group = s
                    .groups
                    .iter()
                    .find(|g| &g.id == group_id)
                    .ok_or("unknown group")?;
                if group.replicas.len() != 2 {
                    return Err("group must have two replicas".into());
                }
                group.role(&manifest.coverage, &op.assignments)?;
                let ready = group
                    .replicas
                    .iter()
                    .filter(|r| {
                        op.readiness.iter().any(|ack| {
                            ack.replica == r.name
                                && ack.incarnation == r.incarnation
                                && ack.candidate_digest == op.candidate_digest
                        })
                    })
                    .count();
                let new_group = !s.assignments.values().any(|id| id == group_id);
                if ready
                    < if new_group || op.require_both.contains(group_id) {
                        2
                    } else {
                        1
                    }
                {
                    return Err("insufficient complete-assignment readiness".into());
                }
            }
            s.next_generation = s
                .next_generation
                .checked_add(1)
                .ok_or("generation exhausted")?;
            s.revision = s
                .revision
                .checked_add(1)
                .ok_or("placement revision exhausted")?;
            s.lifecycle = lifecycle;
            s.assignments = op.assignments.clone();
            s.published.insert(0, manifest.clone());
            s.published.truncate(RETAINED_GENERATIONS);
            for ack in &op.readiness {
                if s.pending_commits
                    .iter()
                    .any(|pending| pending.replica == ack.replica)
                    || s.pending_aborts
                        .iter()
                        .any(|pending| pending.replica == ack.replica)
                {
                    return Err("worker has an unacknowledged decision".into());
                }
                s.pending_commits.push(PendingCommit {
                    replica: ack.replica.clone(),
                    operation: op.id.clone(),
                    attempt: op.attempt,
                    manifest: manifest.clone(),
                    retained: s.published.iter().map(|m| m.generation).collect(),
                });
            }
            op.phase = Phase::Committed;
            s.completed
                .insert(op.id.clone(), op.candidate_digest.clone());
            let op = op.clone();
            s.cancel_participants(
                &op,
                &op.readiness.iter().map(|ack| ack.replica.clone()).collect(),
            );
            Ok(())
        })
    }
}

/// Pure placement preview: no resources or reservations are changed here.
/// New groups and destinations of published-shard moves require complete pairs.
/// An unchanged assignment can still publish through its surviving replica.
pub fn required_destination_pairs(
    assignments: &BTreeMap<u64, String>,
    previous: &BTreeMap<u64, String>,
) -> BTreeSet<String> {
    assignments
        .iter()
        .filter(|(shard, group)| {
            !previous.values().any(|old| old == *group)
                || previous.get(shard).is_some_and(|old| old != *group)
        })
        .map(|(_, group)| group.clone())
        .collect()
}

pub fn assign(
    coverage: &Coverage,
    groups: &[Group],
    previous: &BTreeMap<u64, String>,
) -> Result<BTreeMap<u64, String>, String> {
    coverage.validate(Geometry::default())?;
    if groups.is_empty() || groups.len() > 4 {
        return Err("requires one to four groups".into());
    }
    let mut assignments = BTreeMap::new();
    for shard in &coverage.shards {
        let preferred = previous.get(&shard.id);
        let mut ordered: Vec<_> = groups.iter().collect();
        ordered.sort_by_key(|g| (preferred != Some(&g.id), g.sequence));
        let mut placed = false;
        for group in ordered {
            assignments.insert(shard.id, group.id.clone());
            if group.role(coverage, &assignments).is_ok() {
                placed = true;
                break;
            }
            assignments.remove(&shard.id);
        }
        if !placed {
            return Err("fleet capacity exhausted; preserve published generation".into());
        }
    }
    Ok(assignments)
}

/// Fill historical slots only by moving sealed shards toward older eligible groups.
/// Memory admission remains mandatory on both source and destination before publication.
pub fn consolidate(
    coverage: &Coverage,
    groups: &[Group],
    assignments: &mut BTreeMap<u64, String>,
) -> Result<BTreeSet<String>, String> {
    let mut destinations = BTreeSet::new();
    let mut ordered: Vec<_> = groups.iter().collect();
    ordered.sort_by_key(|g| g.sequence);
    for destination in &ordered {
        while destination.role(coverage, assignments)? == Role::SealedOpen {
            let shard = coverage
                .shards
                .iter()
                .filter(|s| s.state == ShardState::Sealed)
                .filter_map(|s| {
                    let source = groups
                        .iter()
                        .find(|g| assignments.get(&s.id) == Some(&g.id))?;
                    (source.sequence > destination.sequence).then_some((s.id, source))
                })
                .min_by_key(|(id, source)| {
                    (
                        source.role(coverage, assignments).ok() != Some(Role::Active),
                        *id,
                    )
                });
            let Some((id, _)) = shard else {
                break;
            };
            assignments.insert(id, destination.id.clone());
            destinations.insert(destination.id.clone());
        }
    }
    Ok(destinations)
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct GrowthForecast {
    pub rows_per_second: f64,
    pub readiness_seconds: f64,
    pub burst_rows: u64,
    pub observed_at: u64,
}

impl GrowthForecast {
    pub fn expansion_due(self, remaining_rows: u64, now: u64) -> Result<bool, String> {
        if !self.rows_per_second.is_finite()
            || self.rows_per_second < 0.0
            || !self.readiness_seconds.is_finite()
            || self.readiness_seconds <= 0.0
        {
            return Err("invalid capacity forecast".into());
        }
        // A six-hour minimum readiness window includes the mandatory qualification campaign.
        let rate = if now.saturating_sub(self.observed_at) > 300 {
            self.rows_per_second.max(1.0)
        } else {
            self.rows_per_second
        };
        Ok(remaining_rows as f64
            <= rate * self.readiness_seconds.max(21600.0) + self.burst_rows as f64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn seven_sealed_policy_preserves_active_limit_and_reorg_relocation() {
        let span = 32768 * 33;
        let mut lifecycle = Lifecycle::default();
        let mut groups: Vec<_> = (0..2)
            .map(|i| Group {
                placement_policy: PlacementPolicy { sealed_shards: 7 },
                id: format!("g{i}"),
                sequence: i,
                replicas: vec![],
                settling: false,
            })
            .collect();
        let coverage = lifecycle
            .coverage(8 * span + 4096 * 33, Geometry::default())
            .unwrap();
        let mut placement = assign(&coverage, &groups, &BTreeMap::new()).unwrap();
        consolidate(&coverage, &groups, &mut placement).unwrap();
        assert_eq!(placement.values().filter(|id| **id == "g0").count(), 7);
        assert_eq!(
            groups[0].role(&coverage, &placement).unwrap(),
            Role::SealedFull
        );
        let eighth = coverage.shards[7].id;
        let mut invalid = placement.clone();
        invalid.insert(eighth, "g0".into());
        assert!(groups[0].role(&coverage, &invalid).is_err());
        groups[0].placement_policy.sealed_shards = 6;
        assert!(groups[0].role(&coverage, &placement).is_err());
        groups[0].placement_policy.sealed_shards = 7;
        // Reopening historical coverage must not leave an active seventh shard in place.
        let reorg = lifecycle
            .coverage(6 * span + 1, Geometry::default())
            .unwrap();
        let relocated = assign(&reorg, &groups, &placement).unwrap();
        for group in &groups {
            group.role(&reorg, &relocated).unwrap();
        }
        assert_ne!(relocated[&reorg.shards.last().unwrap().id], "g0");
        assert!(PlacementPolicy { sealed_shards: 8 }.validate().is_err());
    }

    #[test]
    fn q46_controller_state_is_rejected_without_rewriting_it() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("controller.json");
        let old = State {
            version: 5,
            ..State::default()
        };
        let bytes = serde_json::to_vec(&old).unwrap();
        fs::write(&path, &bytes).unwrap();
        assert!(Store::open(root.path()).is_err());
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }

    #[test]
    fn reorg_move_into_existing_group_requires_both_destination_replicas() {
        let groups: Vec<_> = ["a", "b"]
            .into_iter()
            .enumerate()
            .map(|(sequence, id)| Group {
                placement_policy: Default::default(),
                id: id.into(),
                sequence: sequence as u64,
                replicas: Vec::new(),
                settling: false,
            })
            .collect();
        let mut lifecycle = Lifecycle::default();
        let old = lifecycle
            .coverage((6 * 32768 + 4096) * 33, Geometry::default())
            .unwrap();
        let previous: BTreeMap<_, _> = old
            .shards
            .iter()
            .enumerate()
            .map(|(i, shard)| (shard.id, if i < 6 { "a" } else { "b" }.into()))
            .collect();
        assert_eq!(groups[0].role(&old, &previous).unwrap(), Role::SealedFull);
        assert_eq!(groups[1].role(&old, &previous).unwrap(), Role::Active);
        let reorg = lifecycle
            .coverage((5 * 32768 + 4096) * 33, Geometry::default())
            .unwrap();
        let assignments = assign(&reorg, &groups, &previous).unwrap();
        let frontier = reorg.shards.last().unwrap().id;
        assert_eq!(previous[&frontier], "a");
        assert_eq!(assignments[&frontier], "b");
        assert_eq!(
            required_destination_pairs(&assignments, &previous),
            ["b".to_owned()].into()
        );
        assert!(required_destination_pairs(&assignments, &assignments).is_empty());
        assert_eq!(
            required_destination_pairs(&assignments, &BTreeMap::new()),
            ["a".to_owned(), "b".to_owned()].into()
        );
    }

    #[test]
    fn reservations_materialize_once_and_pins_block_reclamation() {
        let mut l = Ledger::default();
        l.reserve("op", 100, OVERHEAD + 100).unwrap();
        l.materialize("op", "unit", 60).unwrap();
        l.materialize("op", "unit", 60).unwrap();
        assert_eq!(l.total().unwrap(), OVERHEAD + 100);
        assert!(l.reserve("other", 1, OVERHEAD + 100).is_err());
        l.pinned.insert("unit".into());
        assert!(l.reclaim("unit", false).is_err());
        l.pinned.clear();
        assert!(l.reclaim("unit", true).is_err());
        l.reclaim("unit", false).unwrap();
        assert_eq!(l.total().unwrap(), OVERHEAD + 40);
    }

    #[test]
    fn durable_epoch_fences_restarts_and_failed_transactions() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = Store::open(dir.path()).unwrap();
        assert!(Store::open(dir.path()).is_err());
        assert!(s
            .update::<()>(|state| {
                state.revision = 100;
                Err("abort".into())
            })
            .is_err());
        assert_eq!(s.state().revision, 0);
        let epoch = s.state().epoch;
        drop(s);
        let s = Store::open(dir.path()).unwrap();
        assert_eq!(s.state().epoch, epoch + 1);
        assert_eq!(s.state().revision, 0);
    }

    #[test]
    fn lender_counts_and_new_successor_cannot_be_sixth_active_shard() {
        let g = Geometry::default();
        let mut l = Lifecycle::default();
        let c = l.coverage(5 * 32768 * 33, g).unwrap();
        let groups = vec![
            Group {
                placement_policy: Default::default(),
                id: "a".into(),
                sequence: 0,
                replicas: vec![],
                settling: false,
            },
            Group {
                placement_policy: Default::default(),
                id: "b".into(),
                sequence: 1,
                replicas: vec![],
                settling: false,
            },
        ];
        let a = assign(&c, &groups, &BTreeMap::new()).unwrap();
        assert_eq!(a[&c.shards[4].id], "a");
        assert_eq!(a[&c.shards[5].id], "b");
        assert_eq!(groups[0].role(&c, &a).unwrap(), Role::Settling);
        assert_eq!(groups[1].role(&c, &a).unwrap(), Role::Active);
    }

    #[test]
    fn restart_at_every_phase_preserves_decision_and_fences_old_writer() {
        for phase in [
            Phase::Planned,
            Phase::Reserved,
            Phase::Preparing,
            Phase::Ready,
            Phase::Committed,
            Phase::Draining,
        ] {
            let directory = tempfile::tempdir().unwrap();
            let mut store = Store::open(directory.path()).unwrap();
            let epoch = store.state().epoch;
            let operation = Operation {
                id: "test-operation".into(),
                attempt: 0,
                epoch,
                expected_revision: 0,
                candidate_digest: "candidate".into(),
                phase: Phase::Planned,
                assignments: BTreeMap::new(),
                affected_groups: BTreeSet::new(),
                source_generations: vec![],
                require_both: BTreeSet::new(),
                readiness: vec![],
                infrastructure_resources: BTreeMap::new(),
            };
            store.plan(operation.clone()).unwrap();
            store.plan(operation).unwrap();
            store
                .update(|s| {
                    s.operation.as_mut().unwrap().phase = phase;
                    Ok(())
                })
                .unwrap();
            drop(store);
            let mut restarted = Store::open(directory.path()).unwrap();
            assert_eq!(restarted.state().operation.as_ref().unwrap().phase, phase);
            assert!(restarted
                .advance(epoch, "test-operation", 0, Phase::Complete)
                .is_err());
            assert_eq!(restarted.state().next_attempt, 1);
        }
    }

    #[test]
    fn abort_intent_survives_restart_without_overwriting_older_decisions() {
        for phase in [
            Phase::Planned,
            Phase::Reserved,
            Phase::Preparing,
            Phase::Ready,
        ] {
            let directory = tempfile::tempdir().unwrap();
            let mut store = Store::open(directory.path()).unwrap();
            let older = PendingAbort {
                replica: "a".into(),
                operation: "older".into(),
                attempt: 0,
            };
            store
                .update(|s| {
                    s.pending_aborts.push(older.clone());
                    s.groups.push(Group {
                        placement_policy: Default::default(),
                        id: "g".into(),
                        sequence: 0,
                        settling: false,
                        replicas: ["a", "b"]
                            .into_iter()
                            .map(|name| Replica {
                                name: name.into(),
                                url: "unused".into(),
                                incarnation: "i".into(),
                                ledger: Ledger {
                                    reservations: [("held".into(), 100)].into(),
                                    ..Ledger::default()
                                },
                            })
                            .collect(),
                    });
                    Ok(())
                })
                .unwrap();
            let operation = Operation {
                id: "current".into(),
                attempt: 0,
                epoch: store.state().epoch,
                expected_revision: 0,
                candidate_digest: "candidate".into(),
                phase: Phase::Planned,
                assignments: BTreeMap::new(),
                affected_groups: ["g".into()].into(),
                source_generations: vec![],
                require_both: BTreeSet::new(),
                readiness: vec![],
                infrastructure_resources: BTreeMap::new(),
            };
            store.plan(operation).unwrap();
            store
                .update(|s| {
                    s.operation.as_mut().unwrap().phase = phase;
                    Ok(())
                })
                .unwrap();
            store.abort_operation().unwrap();
            let expected = vec![
                older,
                PendingAbort {
                    replica: "b".into(),
                    operation: "current".into(),
                    attempt: 0,
                },
            ];
            assert_eq!(store.state().pending_aborts, expected);
            drop(store);
            let mut store = Store::open(directory.path()).unwrap();
            assert!(store.state().operation.is_none());
            assert_eq!(store.state().pending_aborts, expected);
            assert_eq!(store.state().next_generation, 1);
            assert_eq!(store.state().next_attempt, 1);
            assert!(store.state().published.is_empty());
            assert!(store.state().groups[0]
                .replicas
                .iter()
                .all(|r| r.ledger.reservations["held"] == 100));
            store.abort_operation().unwrap();
            assert_eq!(store.state().pending_aborts, expected);
        }
    }

    #[test]
    fn consolidation_converges_across_multiple_boundaries_without_active_sixth_slot() {
        let groups: Vec<_> = (0..3)
            .map(|i| Group {
                placement_policy: Default::default(),
                id: format!("g{i}"),
                sequence: i,
                replicas: vec![],
                settling: false,
            })
            .collect();
        let mut lifecycle = Lifecycle::default();
        let mut assignments = BTreeMap::new();
        for full in 1..=13 {
            for offset in [0, 4096] {
                let coverage = lifecycle
                    .coverage((full * 32768 + offset) * 33, Geometry::default())
                    .unwrap();
                assignments = assign(&coverage, &groups, &assignments).unwrap();
                consolidate(&coverage, &groups, &mut assignments).unwrap();
                for group in &groups {
                    group.role(&coverage, &assignments).unwrap();
                }
                if full >= 7 && offset == 4096 {
                    assert_eq!(
                        groups[0].role(&coverage, &assignments).unwrap(),
                        Role::SealedFull
                    );
                }
                if full == 13 && offset == 4096 {
                    assert_eq!(
                        groups[1].role(&coverage, &assignments).unwrap(),
                        Role::SealedFull
                    );
                }
            }
        }
        let forecast = GrowthForecast {
            rows_per_second: 0.,
            readiness_seconds: 60.,
            burst_rows: 4096,
            observed_at: 0,
        };
        assert!(forecast.expansion_due(20000, 301).unwrap());
    }

    #[test]
    fn publication_commits_manifest_placement_and_outcome_together() {
        use enhance_pir::protocol::{
            parameter_id, setup_seed, unit_parameter_id, SessionRef, UnitIdentity,
            PROTOCOL_REVISION, SCHEMA_VERSION,
        };
        use sha2::{Digest, Sha256};
        let directory = tempfile::tempdir().unwrap();
        let mut store = Store::open(directory.path()).unwrap();
        let mut lifecycle = Lifecycle::default();
        let coverage = lifecycle.coverage(1, Geometry::default()).unwrap();
        let manifest = Manifest {
            schema_version: SCHEMA_VERSION,
            protocol_revision: PROTOCOL_REVISION.into(),
            network: "main".into(),
            pool: "ironwood".into(),
            generation: 1,
            anchor_height: 3428143,
            anchor_block_hash: "01".repeat(32),
            geometry: Geometry::default(),
            coverage,
            sessions: vec![SessionRef {
                shard_id: 0,
                public_params_sha256: "00".repeat(32),
                parameter_id: parameter_id(4096).unwrap(),
            }],
            unit_identities: [(
                0,
                vec![UnitIdentity {
                    table: "enhance".into(),
                    shard_id: 0,
                    local_row_start: 0,
                    allocated_rows: 2048,
                    setup_sha256: hex::encode(Sha256::digest(setup_seed(0))),
                    parameter_id: unit_parameter_id(2048).unwrap(),
                    content_sha256: "00".repeat(32),
                }],
            )]
            .into(),
        };
        store
            .update(|s| {
                s.groups = vec![Group {
                    placement_policy: Default::default(),
                    id: "group".into(),
                    sequence: 0,
                    settling: false,
                    replicas: ["a", "b"]
                        .into_iter()
                        .map(|name| Replica {
                            name: name.into(),
                            url: format!("http://{name}:8091"),
                            incarnation: name.into(),
                            ledger: Ledger::default(),
                        })
                        .collect(),
                }];
                Ok(())
            })
            .unwrap();
        let operation = Operation {
            id: "publish".into(),
            attempt: 0,
            epoch: store.state().epoch,
            expected_revision: 0,
            candidate_digest: digest(&manifest),
            phase: Phase::Planned,
            assignments: [(0, "group".into())].into(),
            affected_groups: ["group".into()].into(),
            source_generations: vec![],
            require_both: BTreeSet::new(),
            readiness: vec![],
            infrastructure_resources: BTreeMap::new(),
        };
        store.plan(operation).unwrap();
        store
            .update(|s| {
                let op = s.operation.as_mut().unwrap();
                op.phase = Phase::Ready;
                op.readiness = vec![ReadyAck {
                    replica: "a".into(),
                    incarnation: "a".into(),
                    candidate_digest: digest(&manifest),
                }];
                Ok(())
            })
            .unwrap();
        assert!(store.commit(manifest.clone(), lifecycle.clone()).is_err());
        assert!(store.state().published.is_empty());
        store
            .update(|s| {
                s.operation.as_mut().unwrap().readiness.push(ReadyAck {
                    replica: "b".into(),
                    incarnation: "stale".into(),
                    candidate_digest: digest(&manifest),
                });
                Ok(())
            })
            .unwrap();
        assert!(store.commit(manifest.clone(), lifecycle.clone()).is_err());
        store
            .update(|s| {
                s.operation.as_mut().unwrap().readiness[1].incarnation = "b".into();
                Ok(())
            })
            .unwrap();
        store.commit(manifest.clone(), lifecycle.clone()).unwrap();
        drop(store);
        let store = Store::open(directory.path()).unwrap();
        assert_eq!(store.state().published, vec![manifest.clone()]);
        assert_eq!(store.state().pending_commits.len(), 2);
        for notification in &store.state().pending_commits {
            assert_eq!(notification.manifest, manifest);
            assert_eq!(notification.retained, vec![manifest.generation]);
            assert_eq!(notification.operation, "publish");
        }
        assert_eq!(store.state().assignments[&0], "group");
        assert_eq!(store.state().completed["publish"], digest(&manifest));
        assert_eq!(store.state().lifecycle, lifecycle);
        assert_eq!(
            store.state().operation.as_ref().unwrap().phase,
            Phase::Committed
        );
        // Even if a caller misses the scheduler quarantine, the atomic commit
        // cannot queue a second decision for a worker that still holds the first.
        let mut store = store;
        let original_pending = store.state().pending_commits.clone();
        let mut next_manifest = manifest.clone();
        next_manifest.generation += 1;
        store
            .update(|s| {
                let op = s.operation.as_mut().unwrap();
                op.id = "next".into();
                op.phase = Phase::Ready;
                op.epoch = s.epoch;
                op.expected_revision = s.revision;
                op.candidate_digest = digest(&next_manifest);
                for ack in &mut op.readiness {
                    ack.candidate_digest = digest(&next_manifest);
                }
                Ok(())
            })
            .unwrap();
        assert!(store
            .commit(next_manifest.clone(), lifecycle.clone())
            .is_err());
        assert_eq!(store.state().pending_commits, original_pending);
        assert_eq!(store.state().published, vec![manifest.clone()]);
        store
            .update(|s| {
                s.pending_commits.clear();
                s.pending_aborts.push(PendingAbort {
                    replica: "a".into(),
                    operation: "earlier".into(),
                    attempt: 0,
                });
                Ok(())
            })
            .unwrap();
        assert!(store
            .commit(next_manifest.clone(), lifecycle.clone())
            .is_err());
        assert_eq!(store.state().published, vec![manifest]);
        store
            .update(|s| {
                s.pending_aborts.clear();
                s.operation
                    .as_mut()
                    .unwrap()
                    .readiness
                    .retain(|ack| ack.replica == "a");
                Ok(())
            })
            .unwrap();
        store.commit(next_manifest, lifecycle).unwrap();
        assert_eq!(store.state().pending_commits.len(), 1);
        assert_eq!(store.state().pending_commits[0].replica, "a");
        assert_eq!(
            store.state().pending_aborts,
            vec![PendingAbort {
                replica: "b".into(),
                operation: "next".into(),
                attempt: 0
            }]
        );
        assert!(
            store.abort_operation().is_err(),
            "publication is irrevocable"
        );
        assert_eq!(
            store.state().operation.as_ref().unwrap().phase,
            Phase::Committed
        );
    }
}
