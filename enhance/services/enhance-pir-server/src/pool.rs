//! Per-domain replica placement. Inventory groups are only legacy containers;
//! neither readiness nor placement depends on keeping their workers together.
use crate::control::{Group, MIB};
use enhance_pir::protocol::{Coverage, Geometry, ShardState};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub type Placements = BTreeMap<u64, BTreeSet<String>>;
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Policy {
    #[serde(default)]
    pub domain_replication: BTreeMap<u64, usize>,
    #[serde(default)]
    pub optional_mirrors: BTreeMap<String, String>,
    #[serde(default)]
    pub domain_optional_workers: BTreeMap<u64, BTreeSet<String>>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Pool {
    pub replication: usize,
    pub frontier_replication: usize,
    #[serde(default)]
    pub domain_replication: BTreeMap<u64, usize>,
    /// Optional workers follow a required worker's assignments. They do not
    /// contribute to the publication quorum.
    #[serde(default)]
    pub optional_mirrors: BTreeMap<String, String>,
    #[serde(default)]
    pub domain_optional_workers: BTreeMap<u64, BTreeSet<String>>,
    pub placements: Placements,
}
impl Pool {
    pub fn validate(&self, groups: &[Group]) -> Result<(), String> {
        let names: BTreeSet<_> = groups
            .iter()
            .flat_map(|g| g.replicas.iter().map(|r| r.name.clone()))
            .collect();
        let count = names.len().saturating_sub(self.optional_workers().len());
        if self.replication < 2
            || self.frontier_replication < self.replication
            || self.frontier_replication > count
            || self
                .domain_replication
                .values()
                .any(|n| *n < 2 || *n > count)
            || self.optional_mirrors.iter().any(|(worker, source)| {
                worker == source
                    || !names.contains(worker)
                    || !names.contains(source)
                    || self.optional_workers().contains(source)
            })
            || self
                .domain_optional_workers
                .values()
                .any(|workers| workers.iter().any(|w| !names.contains(w)))
        {
            return Err("pool replication exceeds registered workers".into());
        }
        Ok(())
    }
    pub fn factor(&self, id: u64, state: ShardState) -> usize {
        if let Some(n) = self.domain_replication.get(&id) {
            *n
        } else if state == ShardState::Sealed {
            self.replication
        } else {
            self.frontier_replication
        }
    }
    pub fn optional_workers(&self) -> BTreeSet<String> {
        self.optional_mirrors
            .keys()
            .cloned()
            .chain(
                self.domain_optional_workers
                    .values()
                    .flat_map(|v| v.iter().cloned()),
            )
            .collect()
    }
    pub fn required(&self, id: u64, state: ShardState, placements: &BTreeSet<String>) -> bool {
        placements
            .iter()
            .filter(|w| !self.optional_workers().contains(*w))
            .count()
            == self.factor(id, state)
    }
}
fn fits(coverage: &Coverage, groups: &[Group], placements: &Placements, worker: &str) -> bool {
    let Some(group) = groups
        .iter()
        .find(|g| g.replicas.iter().any(|r| r.name == worker))
    else {
        return false;
    };
    let own: Vec<_> = coverage
        .shards
        .iter()
        .filter(|s| placements.get(&s.id).is_some_and(|p| p.contains(worker)))
        .collect();
    own.iter()
        .filter(|s| s.state == ShardState::Growing)
        .count()
        <= 1
        && own.len()
            <= group
                .placement_policy
                .limit(own.iter().any(|s| s.state != ShardState::Sealed))
}
pub fn validate(
    coverage: &Coverage,
    groups: &[Group],
    pool: &Pool,
    placements: &Placements,
) -> Result<(), String> {
    pool.validate(groups)?;
    coverage.validate(Geometry::default())?;
    let workers: BTreeSet<_> = groups
        .iter()
        .flat_map(|g| g.replicas.iter().map(|r| r.name.clone()))
        .collect();
    if placements.len() != coverage.shards.len() {
        return Err("incomplete pool placement".into());
    }
    for shard in &coverage.shards {
        let copies = placements.get(&shard.id).ok_or("unplaced domain")?;
        let optional = pool.optional_workers();
        if !pool.required(shard.id, shard.state, copies)
            || !copies.is_subset(&workers)
            || copies.iter().filter(|w| optional.contains(*w)).any(|w| {
                !pool
                    .domain_optional_workers
                    .get(&shard.id)
                    .is_some_and(|v| v.contains(w))
                    && !pool
                        .optional_mirrors
                        .get(w)
                        .is_some_and(|source| copies.contains(source))
            })
        {
            return Err("invalid domain replication".into());
        }
    }
    if workers
        .iter()
        .any(|w| !fits(coverage, groups, placements, w))
    {
        return Err("per-worker role limit exceeded".into());
    }
    Ok(())
}
/// Preserve valid replicas; place additions by projected reserved bytes, then name.
/// Local /internal/admit and reservation remain authoritative for retained pins.
pub fn place(
    coverage: &Coverage,
    groups: &[Group],
    pool: &Pool,
    forbidden: &BTreeSet<(u64, String)>,
) -> Result<Placements, String> {
    pool.validate(groups)?;
    let mut placements = Placements::new();
    let optional = pool.optional_workers();
    let workers: Vec<_> = groups
        .iter()
        .flat_map(|g| &g.replicas)
        .filter(|r| !optional.contains(&r.name))
        .collect();
    for shard in &coverage.shards {
        let old = pool.placements.get(&shard.id);
        let mut selected = BTreeSet::new();
        for _ in 0..pool.factor(shard.id, shard.state) {
            let mut eligible: Vec<_> = workers
                .iter()
                .filter(|w| {
                    !selected.contains(&w.name) && !forbidden.contains(&(shard.id, w.name.clone()))
                })
                .collect();
            eligible.sort_by_key(|w| {
                let owned = placements.values().filter(|p| p.contains(&w.name)).count() as u64;
                (
                    !old.is_some_and(|p| p.contains(&w.name)),
                    w.ledger
                        .total()
                        .unwrap_or(u64::MAX)
                        .saturating_add(owned * 768 * MIB),
                    w.name.clone(),
                )
            });
            let mut found = false;
            for worker in eligible {
                selected.insert(worker.name.clone());
                placements.insert(shard.id, selected.clone());
                if fits(coverage, groups, &placements, &worker.name) {
                    found = true;
                    break;
                }
                selected.remove(&worker.name);
                placements.insert(shard.id, selected.clone());
            }
            if !found {
                return Err("worker pool capacity exhausted; preserve published placement".into());
            }
        }
        for worker in groups
            .iter()
            .flat_map(|g| &g.replicas)
            .filter(|r| optional.contains(&r.name))
        {
            let assigned = pool
                .domain_optional_workers
                .get(&shard.id)
                .is_some_and(|v| v.contains(&worker.name))
                || pool
                    .optional_mirrors
                    .get(&worker.name)
                    .is_some_and(|source| selected.contains(source));
            if assigned && !forbidden.contains(&(shard.id, worker.name.clone())) {
                selected.insert(worker.name.clone());
                placements.insert(shard.id, selected.clone());
                if !fits(coverage, groups, &placements, &worker.name) {
                    selected.remove(&worker.name);
                    placements.insert(shard.id, selected.clone());
                }
            }
        }
    }
    validate(coverage, groups, pool, &placements)?;
    Ok(placements)
}
/// Empty one sealed-only source if every copy can be placed on already-used
/// workers. Sources and destinations retain their own reservations until drain.
pub fn consolidate(
    coverage: &Coverage,
    groups: &[Group],
    pool: &Pool,
    placements: &Placements,
) -> Placements {
    let mut usage: BTreeMap<String, usize> = BTreeMap::new();
    for set in placements.values() {
        for name in set {
            *usage.entry(name.clone()).or_default() += 1;
        }
    }
    let mut sources: Vec<_> = usage.iter().collect();
    sources.sort_by_key(|(name, n)| (**n, (*name).clone()));
    for (source, _) in sources {
        let own: Vec<_> = coverage
            .shards
            .iter()
            .filter(|s| placements[&s.id].contains(source))
            .collect();
        if own.iter().any(|s| s.state != ShardState::Sealed) {
            continue;
        }
        let mut proposed = placements.clone();
        let mut complete = true;
        for shard in own {
            let mut destinations: Vec<_> = usage
                .iter()
                .filter(|(name, _)| *name != source && !proposed[&shard.id].contains(*name))
                .collect();
            destinations.sort_by_key(|(name, n)| (std::cmp::Reverse(**n), (*name).clone()));
            let mut moved = false;
            for (destination, _) in destinations {
                proposed.get_mut(&shard.id).unwrap().remove(source);
                proposed
                    .get_mut(&shard.id)
                    .unwrap()
                    .insert(destination.clone());
                if fits(coverage, groups, &proposed, destination) {
                    moved = true;
                    break;
                }
                proposed.get_mut(&shard.id).unwrap().remove(destination);
                proposed.get_mut(&shard.id).unwrap().insert(source.clone());
            }
            if !moved {
                complete = false;
                break;
            }
        }
        if complete && validate(coverage, groups, pool, &proposed).is_ok() {
            return proposed;
        }
    }
    placements.clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::{Ledger, Replica};
    fn groups(n: usize) -> Vec<Group> {
        (0..n)
            .map(|i| Group {
                placement_policy: Default::default(),
                id: format!("inventory-{i}"),
                sequence: i as u64,
                replicas: vec![Replica {
                    name: format!("worker-{i}"),
                    url: format!("http://127.0.0.1:{}", 8100 + i),
                    incarnation: String::new(),
                    ledger: Ledger::default(),
                }],
            })
            .collect()
    }
    #[test]
    fn individual_workers_support_frontier_replication_and_repair() {
        let groups = groups(4);
        let mut lifecycle = enhance_pir::protocol::Lifecycle::default();
        let coverage = lifecycle.coverage(67, Geometry::default()).unwrap();
        let mut pool = Pool {
            replication: 2,
            frontier_replication: 3,
            domain_replication: BTreeMap::new(),
            optional_mirrors: BTreeMap::new(),
            domain_optional_workers: BTreeMap::new(),
            placements: Placements::new(),
        };
        let first = place(&coverage, &groups, &pool, &BTreeSet::new()).unwrap();
        assert_eq!(first[&0].len(), 3);
        pool.placements = first.clone();
        assert_eq!(
            place(&coverage, &groups, &pool, &BTreeSet::new()).unwrap(),
            first
        );
        let rejected = first[&0].first().unwrap().clone();
        let repaired = place(&coverage, &groups, &pool, &[(0, rejected.clone())].into()).unwrap();
        assert_eq!(repaired[&0].len(), 3);
        assert!(!repaired[&0].contains(&rejected));
        assert_eq!(repaired[&0].intersection(&first[&0]).count(), 2);
    }
    #[test]
    fn missing_replication_is_refused_instead_of_publishing_partial_placement() {
        let groups = groups(2);
        let coverage = enhance_pir::protocol::Lifecycle::default()
            .coverage(67, Geometry::default())
            .unwrap();
        let pool = Pool {
            replication: 2,
            frontier_replication: 2,
            domain_replication: BTreeMap::new(),
            optional_mirrors: BTreeMap::new(),
            domain_optional_workers: BTreeMap::new(),
            placements: Placements::new(),
        };
        assert!(place(&coverage, &groups, &pool, &[(0, "worker-0".into())].into()).is_err());
        assert!(validate(
            &coverage,
            &groups,
            &pool,
            &[(0, ["worker-0".into()].into())].into()
        )
        .is_err());
    }

    #[test]
    fn optional_gpu_mirrors_cpu_without_entering_required_quorum() {
        let groups = groups(3);
        let coverage = enhance_pir::protocol::Lifecycle::default()
            .coverage(67, Geometry::default())
            .unwrap();
        let pool = Pool {
            replication: 2,
            frontier_replication: 2,
            domain_replication: [(0, 2)].into(),
            optional_mirrors: [("worker-2".into(), "worker-1".into())].into(),
            domain_optional_workers: BTreeMap::new(),
            placements: Placements::new(),
        };
        let with_gpu = place(&coverage, &groups, &pool, &BTreeSet::new()).unwrap();
        assert_eq!(
            with_gpu[&0],
            ["worker-0", "worker-1", "worker-2"]
                .into_iter()
                .map(str::to_owned)
                .collect()
        );
        let without_gpu =
            place(&coverage, &groups, &pool, &[(0, "worker-2".into())].into()).unwrap();
        assert_eq!(
            without_gpu[&0],
            ["worker-0", "worker-1"]
                .into_iter()
                .map(str::to_owned)
                .collect()
        );
        validate(&coverage, &groups, &pool, &without_gpu).unwrap();
    }
}
