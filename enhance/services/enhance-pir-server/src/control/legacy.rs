//! Offline compatibility conversion; preserve publication and recovery decisions.
use super::State;

pub(super) fn restore_placement(s: &mut State) -> Result<(), String> {
    if s.operation.is_some() || !s.pending_commits.is_empty() || !s.pending_aborts.is_empty() {
        return Err("reconcile decisions before rollback".into());
    }
    if s.groups.len() > 4 || s.groups.iter().any(|g| g.replicas.len() != 2) {
        return Err("current inventory cannot be represented as legacy replica pairs".into());
    }
    if let Some(pool) = &s.pool {
        for (id, workers) in &pool.placements {
            let group = s
                .groups
                .iter()
                .find(|g| Some(&g.id) == s.assignments.get(id))
                .ok_or("missing legacy placement")?;
            if *workers != group.replicas.iter().map(|r| r.name.clone()).collect() {
                return Err(
                    "pool placements must first be moved back onto complete legacy pairs".into(),
                );
            }
        }
    }
    s.pool = None;
    s.version = 7;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::control::{Group, PendingAbort, Replica, Store};

    #[test]
    fn rollback_refuses_pending_decisions_and_split_pairs_without_rewriting_state() {
        let root = tempfile::tempdir().unwrap();
        let mut store = Store::open(root.path()).unwrap();
        store
            .update(|s| {
                s.groups = vec![Group {
                    id: "pair".into(),
                    sequence: 0,
                    placement_policy: Default::default(),
                    replicas: ["a", "b"]
                        .into_iter()
                        .map(|name| Replica {
                            name: name.into(),
                            url: format!("http://{name}"),
                            incarnation: String::new(),
                            ledger: Default::default(),
                        })
                        .collect(),
                }];
                s.assignments.insert(0, "pair".into());
                s.pool = Some(crate::pool::Pool {
                    replication: 2,
                    frontier_replication: 2,
                    domain_replication: Default::default(),
                    optional_mirrors: Default::default(),
                    domain_optional_workers: Default::default(),
                    placements: [(0, ["a".into(), "b".into()].into_iter().collect())]
                        .into_iter()
                        .collect(),
                });
                s.version = 8;
                s.recovery.revoked.insert("revoked-session".into());
                s.recovery.epoch = 4;
                s.pending_aborts.push(PendingAbort {
                    replica: "a".into(),
                    operation: "old".into(),
                    attempt: 1,
                });
                Ok(())
            })
            .unwrap();
        let before = std::fs::read(root.path().join("controller.json")).unwrap();
        assert!(store.restore_legacy_placement().is_err());
        assert_eq!(
            std::fs::read(root.path().join("controller.json")).unwrap(),
            before
        );
        store
            .update(|s| {
                s.pending_aborts.clear();
                s.pool
                    .as_mut()
                    .unwrap()
                    .placements
                    .insert(0, ["a".into(), "c".into()].into_iter().collect());
                Ok(())
            })
            .unwrap();
        let before = std::fs::read(root.path().join("controller.json")).unwrap();
        assert!(store.restore_legacy_placement().is_err());
        assert_eq!(
            std::fs::read(root.path().join("controller.json")).unwrap(),
            before
        );
        store
            .update(|s| {
                s.pool
                    .as_mut()
                    .unwrap()
                    .placements
                    .insert(0, ["a".into(), "b".into()].into_iter().collect());
                Ok(())
            })
            .unwrap();
        store.restore_legacy_placement().unwrap();
        assert!(store.state().pool.is_none());
        assert_eq!(store.state().version, 7);
        assert_eq!(store.state().recovery.epoch, 4);
        assert!(store.state().recovery.revoked.contains("revoked-session"));
        drop(store);
        let reopened = Store::open(root.path()).unwrap();
        assert!(reopened
            .state()
            .recovery
            .revoked
            .contains("revoked-session"));
    }
}
