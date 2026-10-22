//! When a publisher actually observed its source, and the age rule that turns
//! such an observation into serving authority for a product whose contract
//! leases authority from source observations.
//!
//! Only a real source lookup (for example a chain-node query) sets an
//! observation. Receiving, caching or replaying a response that carries one,
//! a 304, or a durable floor never renews it: authority ends the product's
//! lifetime after the source was observed, however recently the bytes arrived.
//! The lifetime is the consuming product's policy; nothing here chooses one.
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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

    /// Authority this observation still grants at `now` under a product's
    /// `lifetime`: none when no observation was made, when it claims a time
    /// after `now`, or when the lifetime ended.
    pub fn remaining_at(self, lifetime: Duration, now: SystemTime) -> Option<Duration> {
        if self == Self::NONE {
            return None;
        }
        let age = unix_ms(now).checked_sub(self.observed_unix_ms)?;
        lifetime.checked_sub(Duration::from_millis(age))
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

    const FIVE_SECONDS: Duration = Duration::from_secs(5);

    #[test]
    fn authority_ends_the_product_lifetime_after_the_source_observation() {
        let observed = SourceObservation {
            observed_unix_ms: 1_000_000,
        };
        assert_eq!(
            observed.remaining_at(FIVE_SECONDS, at(1_000_000)),
            Some(FIVE_SECONDS)
        );
        assert_eq!(
            observed.remaining_at(FIVE_SECONDS, at(1_004_000)),
            Some(Duration::from_secs(1))
        );
        assert_eq!(
            observed.remaining_at(FIVE_SECONDS, at(1_005_000)),
            Some(Duration::ZERO)
        );
        assert_eq!(observed.remaining_at(FIVE_SECONDS, at(1_005_001)), None);
        assert_eq!(
            observed.remaining_at(Duration::from_secs(20), at(1_005_001)),
            Some(Duration::from_millis(14_999)),
            "the lifetime is the product's, not a global constant"
        );
    }

    #[test]
    fn absent_and_future_observations_grant_nothing() {
        assert_eq!(
            SourceObservation::NONE.remaining_at(FIVE_SECONDS, at(1)),
            None
        );
        let future = SourceObservation {
            observed_unix_ms: 1_000_001,
        };
        assert_eq!(future.remaining_at(FIVE_SECONDS, at(1_000_000)), None);
        assert!(SourceObservation::starting_now()
            .remaining_at(FIVE_SECONDS, SystemTime::now())
            .is_some());
    }
}
