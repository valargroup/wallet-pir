use super::*;
#[cfg(feature = "qualification-tests")]
use crate::prepared::{coverage, tables, tables_at};
use crate::prepared::{empty, prepare};

/// A one-shard publication of `tables`.
fn of(tables: crate::prepared::Tables) -> Publication {
    prepare(tables).unwrap()
}

/// Selective rejection fences held requests without closing unrelated
/// current admission or changing the global recovery epoch.
#[test]
fn invalidating_one_inventory_preserves_other_approved_requests() {
    let publications = Publications::default();
    let first = of(empty(1));
    let id = first.id();
    let token = first.authorization();
    assert!(publications.publish(first, 0));
    let held = publications.live()[0].clone();
    let next = of(empty(2));
    let approved = next.authorization();
    assert!(publications.publish(next, 0));
    // The request outlives its inventory's retention, before a sweep has
    // transferred the expired entry into the released set.
    publications.0.write().unwrap().retained[0].1 = Instant::now() - Duration::from_secs(1);
    publications.invalidate(&std::collections::HashSet::from([id]), None);
    assert!(!token.load(std::sync::atomic::Ordering::Acquire));
    assert!(!held
        .authorization()
        .load(std::sync::atomic::Ordering::Acquire));
    assert!(approved.load(std::sync::atomic::Ordering::Acquire));
    assert_eq!(publications.epoch(), 0);
    assert_eq!(publications.live().len(), 1);
    assert_eq!(publications.current_anchor(), Some((101, [2; 32])));
}

/// A check that found an anchor off the chain revokes only while its epoch is
/// current and the anchor still accepts requests.
#[test]
fn a_stale_check_revokes_only_while_its_anchor_is_served() {
    let (a, b) = ((101, [1; 32]), (101, [2; 32]));
    let publications = Publications::default();
    assert!(publications.publish(of(empty(1)), 0));
    // A check reads A, then a revocation and B's publication overtake it.
    let (epoch, anchors) = publications.serving();
    assert_eq!(anchors, [a]);
    publications.revoke();
    assert!(publications.publish(of(empty(2)), epoch + 1));
    assert!(!publications.revoke_serving(epoch, a));
    assert_eq!(publications.serving(), (epoch + 1, vec![b]));
    // An ordinary rotation keeps the epoch, but once A's retention ends a check that
    // read A no longer revokes the publication that replaced it. Reuse B's prepared
    // table rather than preprocess a third empty one.
    let publications = Publications::default();
    assert!(publications.publish(of(empty(1)), 0));
    let (epoch, _) = publications.serving();
    assert!(publications.publish(of(empty(2)), 0));
    let ended = Instant::now()
        .checked_sub(Duration::from_millis(1))
        .unwrap();
    publications.0.write().unwrap().retained[0].1 = ended;
    assert!(!publications.revoke_serving(epoch, a));
    assert_eq!(publications.anchors(), [b]);
    // A served anchor off the chain still revokes every session.
    assert!(publications.revoke_serving(epoch, b));
    assert_eq!(publications.serving(), (epoch + 1, vec![]));
}

/// A preparation overtaken by a newer publish cannot replace it, even once the
/// newer one's predecessor has left its retention. Fast already refuses a stale
/// check once a newer anchor is current.
#[cfg(feature = "qualification-tests")]
#[test]
fn an_overtaken_preparation_is_not_published() {
    let publications = Publications::default();
    assert!(publications.publish(of(empty(1)), 0));
    let mut newer = coverage(2);
    newer.end_height = 102;
    let newer = of(tables(newer, &[], 2048));
    assert!(publications.publish(newer, 0));
    let ended = Instant::now()
        .checked_sub(Duration::from_millis(1))
        .unwrap();
    publications.0.write().unwrap().retained[0].1 = ended;
    assert!(!publications.publish(of(empty(3)), 0));
    assert_eq!(publications.anchors(), [(102, [2; 32])]);
}

/// A retained anchor revokes the current inventory with it.
#[test]
fn a_retained_anchor_still_revokes_every_session() {
    let publications = Publications::default();
    assert!(publications.publish(of(empty(1)), 0));
    let (epoch, _) = publications.serving();
    assert!(publications.publish(of(empty(2)), 0));
    assert!(publications.revoke_serving(epoch, (101, [1; 32])));
    assert!(publications.anchors().is_empty());
}

/// A displaced inventory is dropped once its retention ends, not kept until the next
/// publication, and its id is then 410. Fast already ends that retention on the
/// stale-anchor runtime.
#[cfg(feature = "qualification-tests")]
#[test]
fn an_expired_previous_publication_is_dropped() {
    let publications = Publications::default();
    assert!(publications.publish(of(empty(1)), 0));
    let first = Arc::downgrade(publications.0.read().unwrap().current.as_ref().unwrap());
    let id = first.upgrade().unwrap().segments()[0].directory.id();
    assert!(publications.publish(of(empty(2)), 0));
    assert!(publications.select(&id).is_ok());
    assert!(first.upgrade().is_some());
    let ended = Instant::now()
        .checked_sub(Duration::from_millis(1))
        .unwrap();
    publications.0.write().unwrap().retained[0].1 = ended;
    assert_eq!(publications.anchors().len(), 1);
    assert!(first.upgrade().is_none());
    assert!(matches!(publications.select(&id), Err(StatusCode::GONE)));
}

/// Republishing the current inventory replaces it without a new retention or epoch, keeps
/// a predecessor's deadline, and still honors the epoch fence.
#[cfg(feature = "qualification-tests")]
#[test]
fn republishing_the_current_session_replaces_it_in_place() {
    let publications = Publications::default();
    assert!(publications.publish(of(empty(1)), 0));
    assert!(!publications.publish(of(empty(1)), 1));
    assert!(publications.publish(of(empty(1)), 0));
    assert_eq!(publications.epoch(), 0);
    assert_eq!(publications.anchors().len(), 1);
    // A new inventory activates at once and retains its predecessor.
    assert!(publications.publish(of(empty(2)), 0));
    assert!(publications.publish(of(empty(2)), 0));
    assert!(publications.ready_at().is_none());
    assert_eq!(publications.anchors().len(), 2);
}

/// Once [`MAX_RETAINED`] displaced inventories are within their retention, the next
/// publication waits for the oldest to expire instead of dropping it early.
#[test]
fn a_full_retained_set_holds_back_the_next_publication() {
    let publications = Publications::default();
    // Prepare before arming retention. A unique manifest built afterward can
    // outlast the deadline, and `publish` then expires the set it should refuse.
    let newer = of(empty(2));
    assert!(publications.publish(of(empty(1)), 0));
    let held = publications.current().unwrap();
    let until = Instant::now() + Duration::from_secs(5);
    publications.0.write().unwrap().retained = vec![(held, until); MAX_RETAINED];
    assert_eq!(publications.ready_at(), Some(until));
    assert!(!publications.publish(newer, 0));
    publications.0.write().unwrap().retained[0].1 = Instant::now();
    assert!(publications.ready_at().is_none());
}
/// A report update keeps the current session and its prepared PIR state, and is
/// refused for another anchor or after a revocation.
#[test]
fn a_report_update_keeps_the_prepared_session() {
    let publications = Publications::default();
    assert!(publications.publish(of(empty(1)), 0));
    let (before, _) = publications
        .current()
        .map(|p| (p, publications.epoch()))
        .unwrap();
    let anchor = (101, [1; 32]);
    assert!(!publications.update_report((101, [2; 32]), serde_json::json!(1), 0));
    assert!(!publications.update_report(anchor, serde_json::json!(1), 1));
    assert!(publications.update_report(anchor, serde_json::json!(2), 0));
    let (after, epoch) = publications
        .current()
        .map(|p| (p, publications.epoch()))
        .unwrap();
    assert_eq!((after.id(), epoch), (before.id(), 0));
    assert!(std::ptr::eq(
        before.segments()[0].directory.public().as_ptr(),
        after.segments()[0].directory.public().as_ptr()
    ));
    assert_eq!(
        publications.health().0,
        Some((before.id(), Some(serde_json::json!(2))))
    );
    assert!(publications.ready_at().is_none());
    publications.revoke();
    assert!(!publications.update_report(anchor, serde_json::json!(3), 0));
}
/// A companion constructed at a later block than its directory makes that block
/// a served anchor too, so finding it off the chain revokes every session.
#[cfg(feature = "qualification-tests")]
#[test]
fn a_later_companion_context_is_a_served_anchor() {
    let tree = zakura_pir_query_server::receiver::Tree {
        height: 105,
        hash: [5; 32],
        size: 300,
    };
    let publication = of(tables_at(coverage(3), &[], 2048, tree));
    let publications = Publications::default();
    assert!(publications.publish(publication, 0));
    let (epoch, anchors) = publications.serving();
    assert_eq!(anchors, [(101, [3; 32]), (105, [5; 32])]);
    assert!(publications.revoke_serving(epoch, (105, [5; 32])));
    assert!(publications.anchors().is_empty());
}

/// A new companion beside an unchanged directory session displaces the old one
/// through the ordinary retention, so a wallet that captured the old pair can finish.
#[cfg(feature = "qualification-tests")]
#[test]
fn a_companion_only_replacement_keeps_the_displaced_companion() {
    let paid = [crate::common::paid(0)];
    let at = |height, hash| zakura_pir_query_server::receiver::Tree {
        height,
        hash: [hash; 32],
        size: 300,
    };
    let first = of(tables_at(coverage(3), &paid, 2048, at(101, 3)));
    let second = of(tables_at(coverage(3), &paid, 2048, at(105, 5)));
    let witness = |p: &Publication| p.segments()[0].companion.id();
    let (old, new) = (witness(&first), witness(&second));
    assert_eq!(
        first.segments()[0].directory.id(),
        second.segments()[0].directory.id()
    );
    assert_ne!(old, new);
    assert_ne!(first.id(), second.id());
    let publications = Publications::default();
    assert!(publications.publish(first, 0));
    assert!(publications.publish(second, 0));
    assert!(publications.select(&old).is_ok());
    assert!(publications.select(&new).is_ok());
}

/// An inventory a request selected keeps naming its tables after it leaves service,
/// whether its retention ended or a revocation removed it, so their files outlive the
/// request; once the request releases it, its tables are no longer referenced.
#[test]
fn a_held_inventory_stays_referenced_until_released() {
    let publications = Publications::default();
    assert!(publications.publish(of(empty(1)), 0));
    let first = publications.current().unwrap().segments()[0].directory.id();
    let (held, _) = publications.select(&first).unwrap();
    assert!(publications.publish(of(empty(2)), 0));
    publications.0.write().unwrap().retained[0].1 = Instant::now();
    assert!(matches!(publications.select(&first), Err(StatusCode::GONE)));
    assert!(publications.references().contains(&first));
    drop(held);
    assert!(!publications.references().contains(&first));
    // An instance a report update replaced stays referenced while held, through a
    // later displacement and expiry of its replacement.
    let middle = publications.current().unwrap().segments()[0].directory.id();
    let (held, _) = publications.select(&middle).unwrap();
    assert!(publications.update_report((101, [2; 32]), serde_json::json!(1), 0));
    // Same prepared inventory as the first publication: a different id from the
    // current one, so it displaces rather than replacing in place, and the
    // payment table is the one already cached.
    assert!(publications.publish(of(empty(1)), 0));
    publications
        .0
        .write()
        .unwrap()
        .retained
        .last_mut()
        .unwrap()
        .1 = Instant::now();
    assert!(matches!(
        publications.select(&middle),
        Err(StatusCode::GONE)
    ));
    assert!(publications.references().contains(&middle));
    drop(held);
    assert!(!publications.references().contains(&middle));
    let second = publications.current().unwrap().segments()[0].directory.id();
    let (held, _) = publications.select(&second).unwrap();
    publications.revoke();
    assert!(publications.references().contains(&second));
    drop(held);
    assert!(publications.references().is_empty());
}

/// A correction fences direct lookups before the replacement PIR table is ready.
#[cfg(feature = "qualification-tests")]
#[test]
fn correction_revokes_old_sessions_and_refuses_old_preparations() {
    let publications = Publications::default();
    let old = of(empty(1));
    let old_id = old.segments()[0].directory.id();
    assert!(publications.publish(old, 0));
    assert!(publications.advance_history_revision(1));
    assert!(publications.select(&old_id).is_err());
    assert!(publications
        .current()
        .map(|p| (p, publications.epoch()))
        .is_err());
    assert!(!publications.advance_history_revision(0));
    assert!(!publications.publish(of(empty(2)), publications.epoch()));
    let mut corrected = coverage(3);
    corrected.history_revision = 1;
    let corrected = of(tables(corrected, &[], 2048));
    assert!(publications.publish(corrected, publications.epoch()));
    assert!(publications
        .current()
        .map(|p| (p, publications.epoch()))
        .is_ok());
    assert!(publications.select(&old_id).is_err());
}
