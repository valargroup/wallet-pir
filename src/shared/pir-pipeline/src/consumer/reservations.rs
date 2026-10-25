//! Bounded promises for a serial consumer and its serving peer.
use super::PeerReservation;

/// Protect the last acknowledged view and at most one incoming candidate.
/// Products validate authority, expiry and incarnation before these transitions.
pub struct Reservations<K, V> {
    acknowledged: Option<K>,
    serving: Option<(K, V)>,
    incoming: Option<(K, V)>,
}
impl<K, V> Default for Reservations<K, V> {
    fn default() -> Self {
        Self {
            acknowledged: None,
            serving: None,
            incoming: None,
        }
    }
}
impl<K: Eq + Clone, V> Reservations<K, V> {
    pub fn acknowledged(&self) -> Option<&K> {
        self.acknowledged.as_ref()
    }
    pub fn iter(&self) -> impl Iterator<Item = &V> {
        self.serving
            .iter()
            .chain(self.incoming.iter())
            .map(|(_, value)| value)
    }
    pub fn len(&self) -> usize {
        self.iter().count()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn clear(&mut self) {
        *self = Self::default();
    }
    /// Expire physical holds without forgetting an acknowledged identity.
    pub fn retain(&mut self, mut keep: impl FnMut(&V) -> bool) {
        if self.serving.as_ref().is_some_and(|(_, value)| !keep(value)) {
            self.serving = None;
        }
        if self
            .incoming
            .as_ref()
            .is_some_and(|(_, value)| !keep(value))
        {
            self.incoming = None;
        }
    }
    /// Negative authority evidence withdraws both the hold and its acknowledgment.
    pub fn revoke(&mut self, key: &K) {
        if self.serving.as_ref().is_some_and(|(held, _)| held == key) {
            self.serving = None;
        }
        if self.incoming.as_ref().is_some_and(|(held, _)| held == key) {
            self.incoming = None;
        }
        if self.acknowledged.as_ref() == Some(key) {
            self.acknowledged = None;
        }
    }
    /// Replace abandoned incoming work. A delayed serving-view retry changes
    /// nothing; older incoming work cannot displace a newer promise.
    pub fn reserve<R: Ord>(&mut self, key: K, value: V, rank: impl Fn(&V) -> R) -> PeerReservation {
        if self.acknowledged.as_ref() == Some(&key) {
            return PeerReservation::Reserved;
        }
        if self.iter().any(|held| rank(held) > rank(&value)) {
            return PeerReservation::PeerAdvanced;
        }
        if self.incoming.as_ref().is_none_or(|(held, _)| held != &key) {
            self.incoming = Some((key, value));
        }
        PeerReservation::Reserved
    }
    /// Acknowledge only a held identity. Retrying the serving acknowledgment
    /// leaves newer incoming work protected and is idempotent.
    pub fn acknowledge(&mut self, key: &K) -> bool {
        if self.acknowledged.as_ref() == Some(key) {
            return self.serving.as_ref().is_some_and(|(held, _)| held == key);
        }
        if !self.incoming.as_ref().is_some_and(|(held, _)| held == key) {
            return false;
        }
        self.serving = self.incoming.take();
        self.acknowledged = Some(key.clone());
        true
    }
}
/// Explicit reservation service for consumers without downstream peers.
#[derive(Clone, Copy, Default)]
pub struct NoPeers;
impl NoPeers {
    pub fn reserve(&self) -> super::PeerReservation {
        super::PeerReservation::Reserved
    }
    pub fn complete(&self) -> super::PeerHandover {
        super::PeerHandover::Complete
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn abandoned_candidates_are_bounded_and_retries_preserve_the_serving_view() {
        let mut holds = Reservations::default();
        holds.reserve(1, 1, |v| *v);
        assert!(holds.acknowledge(&1));
        for candidate in 2..100 {
            assert_eq!(
                holds.reserve(candidate, candidate, |v| *v),
                PeerReservation::Reserved
            );
            assert_eq!(holds.len(), 2);
            assert!(holds.acknowledge(&1));
            assert_eq!(holds.reserve(1, 1, |v| *v), PeerReservation::Reserved);
            assert_eq!(holds.iter().copied().collect::<Vec<_>>(), [1, candidate]);
        }
        assert!(!holds.acknowledge(&98));
        assert_eq!(holds.reserve(98, 98, |v| *v), PeerReservation::PeerAdvanced);
        assert!(holds.acknowledge(&99));
        assert_eq!(holds.iter().copied().collect::<Vec<_>>(), [99]);
        holds.revoke(&99);
        assert!(holds.is_empty());
        assert_eq!(holds.acknowledged(), None);
    }
}
