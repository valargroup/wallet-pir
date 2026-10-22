//! When a publisher actually observed its source, and the one age rule that
//! turns such an observation into serving authority.
//!
//! Only a real source lookup (for example a chain-node query) sets an
//! observation. Receiving, caching or replaying a response that carries one,
//! a 304, or a durable floor never renews it: authority ends a fixed lifetime
//! after the source was observed, however recently the bytes arrived.
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// How long one source observation grants serving authority.
pub const SOURCE_AUTHORITY_LIFETIME: Duration = pir_control::CONTROL_WATCHDOG;

/// Wall-clock time, in Unix milliseconds, of one actual source observation.
/// Zero means the publisher made none.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SourceObservation {
    pub observed_unix_ms: u64,
}

impl SourceObservation {
    /// Whatever the publisher did, it made no source observation.
    pub const NONE: Self = Self {
        observed_unix_ms: 0,
    };

    /// An observation starting now. Publishers take it before the lookup, so
    /// the lifetime also covers the lookup's own duration.
    pub fn starting_now() -> Self {
        Self {
            observed_unix_ms: unix_ms(SystemTime::now()),
        }
    }

    /// Authority this observation still grants at `now`: none when no
    /// observation was made, when it claims a time after `now`, or when its
    /// lifetime ended.
    pub fn remaining_at(self, now: SystemTime) -> Option<Duration> {
        if self == Self::NONE {
            return None;
        }
        let age = unix_ms(now).checked_sub(self.observed_unix_ms)?;
        SOURCE_AUTHORITY_LIFETIME.checked_sub(Duration::from_millis(age))
    }

    /// [`Self::remaining_at`] the current wall-clock time.
    pub fn remaining(self) -> Option<Duration> {
        self.remaining_at(SystemTime::now())
    }
}

fn unix_ms(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|since| u64::try_from(since.as_millis()).ok())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(unix_ms: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_millis(unix_ms)
    }

    #[test]
    fn authority_ends_a_fixed_lifetime_after_the_source_observation() {
        let observed = SourceObservation {
            observed_unix_ms: 1_000_000,
        };
        assert_eq!(
            observed.remaining_at(at(1_000_000)),
            Some(SOURCE_AUTHORITY_LIFETIME)
        );
        assert_eq!(
            observed.remaining_at(at(1_004_000)),
            Some(Duration::from_secs(1))
        );
        assert_eq!(observed.remaining_at(at(1_005_000)), Some(Duration::ZERO));
        assert_eq!(observed.remaining_at(at(1_005_001)), None);
    }

    #[test]
    fn absent_and_future_observations_grant_nothing() {
        assert_eq!(SourceObservation::NONE.remaining_at(at(1)), None);
        let future = SourceObservation {
            observed_unix_ms: 1_000_001,
        };
        assert_eq!(future.remaining_at(at(1_000_000)), None);
        assert!(SourceObservation::starting_now().remaining().is_some());
    }
}
