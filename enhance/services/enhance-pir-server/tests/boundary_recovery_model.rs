//! Executable D8 transition contract. The composed-routing protocol is not yet
//! implemented; this small row-level model pins its expected publication states.

use std::collections::{BTreeMap, BTreeSet};

const M: usize = 8;
const MIN: usize = 2;
const DEPTH: u64 = 2;

#[derive(Clone)]
struct Block {
    height: u64,
    hash: &'static str,
    rows: Vec<u64>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct Session {
    domain: usize,
    recovery_epoch: u64,
    own_rows: Vec<u64>,
    tail_rows: Vec<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Route {
    domain: usize,
    local_row: usize,
}

#[derive(Clone)]
struct Domain {
    sealed: bool,
    provisional: bool,
    session: Session,
}

#[derive(Default)]
struct Publication {
    anchor: Option<&'static str>,
    routes: Vec<Route>,
    domains: BTreeMap<usize, Domain>,
}

#[derive(Default)]
struct Model {
    blocks: Vec<Block>,
    recovery_epoch: u64,
    recovery_events: Vec<(usize, u64)>,
    known_sessions: BTreeMap<usize, BTreeSet<Session>>,
    revoked: BTreeSet<Session>,
    publication: Publication,
}

impl Model {
    fn append(&mut self, height: u64, hash: &'static str, rows: Vec<u64>) {
        assert_eq!(height, self.blocks.last().map_or(1, |b| b.height + 1));
        self.blocks.push(Block { height, hash, rows });
        self.publish();
    }

    fn rewind(&mut self, ancestor_height: u64) {
        let retained_rows: usize = self
            .blocks
            .iter()
            .filter(|b| b.height <= ancestor_height)
            .map(|b| b.rows.len())
            .sum();
        let first_changed_domain = retained_rows / M;
        let invalidates_seal = self
            .publication
            .domains
            .iter()
            .any(|(id, domain)| *id >= first_changed_domain && domain.sealed);
        if invalidates_seal {
            self.recovery_epoch += 1;
            self.recovery_events
                .push((first_changed_domain, self.recovery_epoch));
            self.revoked.extend(
                self.known_sessions
                    .iter()
                    .filter(|(id, _)| **id >= first_changed_domain)
                    .flat_map(|(_, sessions)| sessions.iter().cloned()),
            );
        }
        self.blocks.retain(|b| b.height <= ancestor_height);
        self.publish();
    }

    fn session(&self, domain: usize) -> Session {
        self.publication.domains[&domain].session.clone()
    }

    fn current(&self, session: &Session) -> bool {
        !self.revoked.contains(session)
            && self
                .publication
                .domains
                .values()
                .any(|domain| &domain.session == session)
    }

    fn is_revoked(&self, session: &Session) -> bool {
        self.revoked.contains(session)
    }

    fn publish(&mut self) {
        let mut rows = Vec::new();
        let mut completion_height = BTreeMap::new();
        for block in &self.blocks {
            for row in &block.rows {
                rows.push(*row);
                if rows.len() % M == 0 {
                    completion_height.insert(rows.len() / M, block.height);
                }
            }
        }
        let tip = self.blocks.last().map(|b| b.height);
        let seals: BTreeSet<_> = completion_height
            .into_iter()
            .filter(|(_, height)| tip.is_some_and(|tip| tip - height >= DEPTH))
            .map(|(boundary, _)| boundary)
            .collect();
        let mut routes: Vec<_> = (0..rows.len())
            .map(|position| Route {
                domain: position / M,
                local_row: position % M,
            })
            .collect();
        let mut domains = BTreeMap::new();
        for id in 0..rows.len().div_ceil(M) {
            let start = id * M;
            let end = (start + M).min(rows.len());
            let occupied = end - start;
            let tail_rows = if id > 0 && occupied < MIN {
                let predecessor_suffix = &rows[start - MIN..start];
                for offset in 0..MIN {
                    routes[start - MIN + offset] = Route {
                        domain: id,
                        local_row: MIN + offset,
                    };
                }
                predecessor_suffix.to_vec()
            } else {
                Vec::new()
            };
            let recovery_epoch = self
                .recovery_events
                .iter()
                .rev()
                .find(|(first, _)| id >= *first)
                .map_or(0, |(_, epoch)| *epoch);
            domains.insert(
                id,
                Domain {
                    sealed: seals.contains(&(id + 1)),
                    provisional: id > 0 && !seals.contains(&id),
                    session: Session {
                        domain: id,
                        recovery_epoch,
                        own_rows: rows[start..end].to_vec(),
                        tail_rows,
                    },
                },
            );
        }
        self.publication = Publication {
            anchor: self.blocks.last().map(|b| b.hash),
            routes,
            domains,
        };
        for (id, domain) in &self.publication.domains {
            self.known_sessions
                .entry(*id)
                .or_default()
                .insert(domain.session.clone());
        }
        self.assert_routing(&rows);
    }

    fn assert_routing(&self, rows: &[u64]) {
        assert_eq!(self.publication.routes.len(), rows.len());
        let mut seen = BTreeSet::new();
        for (position, route) in self.publication.routes.iter().enumerate() {
            assert!(seen.insert((route.domain, route.local_row)));
            let domain = &self.publication.domains[&route.domain];
            let actual = if route.local_row < domain.session.own_rows.len() {
                domain.session.own_rows[route.local_row]
            } else {
                domain.session.tail_rows[route.local_row - MIN]
            };
            assert_eq!(actual, rows[position]);
            assert!(self.current(&domain.session));
        }
    }
}

fn rows(range: std::ops::Range<u64>) -> Vec<u64> {
    range.collect()
}

#[test]
fn crossing_opens_provisional_tail_then_plain_successor_before_confirmation() {
    let mut model = Model::default();
    model.append(1, "crossing", rows(0..9));
    assert_eq!(model.publication.anchor, Some("crossing"));
    assert!(!model.publication.domains[&0].sealed);
    assert!(model.publication.domains[&1].provisional);
    assert_eq!(
        model.publication.routes[6],
        (Route {
            domain: 1,
            local_row: 2
        })
    );
    assert_eq!(
        model.publication.routes[8],
        (Route {
            domain: 1,
            local_row: 0
        })
    );
    let tail_session = model.session(1);
    assert_eq!(tail_session.tail_rows, vec![6, 7]);

    model.append(2, "plain", rows(9..10));
    assert!(model.publication.domains[&1].provisional);
    assert!(model.session(1).tail_rows.is_empty());
    assert_eq!(
        model.publication.routes[6],
        (Route {
            domain: 0,
            local_row: 6
        })
    );
    assert_eq!(
        model.publication.routes[9],
        (Route {
            domain: 1,
            local_row: 1
        })
    );
    assert!(!model.current(&tail_session));
    assert!(!model.is_revoked(&tail_session));

    model.append(3, "confirmed", rows(10..11));
    assert!(model.publication.domains[&0].sealed);
    assert!(!model.publication.domains[&1].provisional);
    assert_eq!(
        model.publication.routes[10],
        (Route {
            domain: 1,
            local_row: 2
        })
    );
    assert_eq!(model.session(1).recovery_epoch, 0);
}

#[test]
fn confirmation_can_promote_a_successor_while_its_tail_is_still_open() {
    let mut model = Model::default();
    model.append(1, "crossing", rows(0..9));
    let tail_session = model.session(1);
    model.append(2, "depth-1", vec![]);
    model.append(3, "depth-2", vec![]);

    assert!(model.publication.domains[&0].sealed);
    assert!(!model.publication.domains[&1].provisional);
    assert_eq!(model.session(1), tail_session);
    assert_eq!(model.publication.routes[7].domain, 1);

    model.append(4, "tail-ends", rows(9..10));
    assert_eq!(model.publication.routes[7].domain, 0);
    assert_eq!(model.publication.routes[9].local_row, 1);
    assert_eq!(model.recovery_epoch, 0);
}

#[test]
fn provisional_rollback_and_multi_boundary_block_preserve_complete_coverage() {
    let mut model = Model::default();
    model.append(1, "first", rows(0..8));
    model.append(2, "provisional", rows(8..9));
    let removed = model.session(1);
    model.rewind(1);
    assert_eq!(model.recovery_epoch, 0);
    assert!(!model.current(&removed));
    assert!(!model.is_revoked(&removed));
    assert_eq!(model.publication.routes.len(), 8);

    model.append(2, "two-crossings", rows(80..100));
    assert_eq!(model.publication.anchor, Some("two-crossings"));
    assert_eq!(model.publication.routes.len(), 28);
    assert!(model.publication.domains[&1].provisional);
    assert!(model.publication.domains[&2].provisional);
    assert!(model.publication.domains[&3].provisional);
    assert_eq!(
        model.publication.routes[8],
        (Route {
            domain: 1,
            local_row: 0
        })
    );
    assert_eq!(
        model.publication.routes[16],
        (Route {
            domain: 2,
            local_row: 0
        })
    );
    assert_eq!(
        model.publication.routes[24],
        (Route {
            domain: 3,
            local_row: 0
        })
    );
}

#[test]
fn reorg_across_sealed_boundary_revokes_affected_sessions_even_for_identical_replay() {
    let mut model = Model::default();
    model.append(1, "prefix", rows(0..8));
    model.append(2, "prefix-depth-1", vec![]);
    model.append(3, "prefix-depth-2", vec![]);
    let unaffected = model.session(0);
    assert!(model.publication.domains[&0].sealed);

    model.append(4, "crossing-old", rows(8..17));
    let retained_tail = model.session(2);
    model.append(5, "plain-old", rows(17..18));
    model.append(6, "sealed-old", rows(18..19));
    let orphaned = model.session(1);
    let orphaned_successor = model.session(2);
    assert!(model.publication.domains[&1].sealed);

    model.rewind(3);
    assert_eq!(model.recovery_epoch, 1);
    assert!(model.current(&unaffected));
    assert!(model.is_revoked(&orphaned));
    assert!(model.is_revoked(&orphaned_successor));
    assert!(model.is_revoked(&retained_tail));
    assert_eq!(model.publication.routes.len(), 8);

    model.append(4, "crossing-new", rows(8..17));
    model.append(5, "plain-new", rows(17..18));
    model.append(6, "sealed-new", rows(18..19));
    let replacement = model.session(1);
    assert_eq!(replacement.own_rows, orphaned.own_rows);
    assert_eq!(replacement.tail_rows, orphaned.tail_rows);
    assert_ne!(replacement, orphaned);
    assert_eq!(replacement.recovery_epoch, 1);
    assert!(model.current(&replacement));
    assert!(model.is_revoked(&orphaned));
    assert_eq!(model.session(0), unaffected);
    assert_eq!(model.publication.anchor, Some("sealed-new"));
}
