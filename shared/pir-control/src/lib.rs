//! What every PIR serving process reports about itself, and how long a
//! controller's authority lasts without renewal.
//!
//! [`Identity`] is the same across Enhance, Status and Transparent, so one
//! deploy tool can confirm which executable answers on each host after a
//! restart. [`Watchdog`] is the single definition of the five-second serving
//! authority that Enhance's packing router and query ingress and Status's
//! roles each enforced with their own copy of the constant.
//!
//! The contract these implement is `docs/serving-contract.md`.

use serde::Serialize;
use std::sync::OnceLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// How long serving authority lasts after the controller's last successful
/// activation, refresh or heartbeat.
pub const CONTROL_WATCHDOG: Duration = Duration::from_secs(5);

/// This process: the executable it runs and a value that changes on restart.
#[derive(Clone, Debug, Serialize)]
pub struct Identity {
    /// SHA-256 of the executable captured at first use, or `None` if it could
    /// not be read. A replaced file on disk does not change it.
    pub binary_sha256: Option<String>,
    /// Random per process; two processes never share one.
    pub incarnation: String,
    pub started_unix: u64,
}

impl Identity {
    /// The identity of the running process, computed once.
    pub fn process() -> &'static Identity {
        static IDENTITY: OnceLock<Identity> = OnceLock::new();
        IDENTITY.get_or_init(|| Identity {
            binary_sha256: binary_sha256().clone(),
            incarnation: hex::encode(rand::random::<[u8; 16]>()),
            started_unix: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        })
    }
}

/// SHA-256 of the running executable, read once.
///
/// On Linux it reads `/proc/self/exe`, which stays the started binary even
/// after the path is replaced by a deploy; elsewhere it reads the current
/// executable path.
pub fn binary_sha256() -> &'static Option<String> {
    static DIGEST: OnceLock<Option<String>> = OnceLock::new();
    DIGEST.get_or_init(|| {
        use sha2::{Digest, Sha256};
        use std::io::Read;
        #[cfg(target_os = "linux")]
        let mut file = std::fs::File::open("/proc/self/exe").ok()?;
        #[cfg(not(target_os = "linux"))]
        let mut file = std::fs::File::open(std::env::current_exe().ok()?).ok()?;
        let mut hash = Sha256::new();
        let mut bytes = [0u8; 65536];
        loop {
            let n = file.read(&mut bytes).ok()?;
            if n == 0 {
                break;
            }
            hash.update(&bytes[..n]);
        }
        Some(hex::encode(hash.finalize()))
    })
}

/// When the controller last renewed serving authority.
///
/// Starts expired: a process serves only after a controller activates it.
#[derive(Clone, Copy, Debug, Default)]
pub struct Watchdog(Option<Instant>);

impl Watchdog {
    /// Renews authority from now.
    pub fn renew(&mut self) {
        self.0 = Some(Instant::now());
    }

    /// Withdraws authority until the next renewal.
    pub fn clear(&mut self) {
        self.0 = None;
    }

    /// Whether authority was renewed within [`CONTROL_WATCHDOG`].
    pub fn live(&self) -> bool {
        self.0.is_some_and(|t| t.elapsed() <= CONTROL_WATCHDOG)
    }

    /// A watchdog last renewed at `at`, for tests that need an expired one.
    pub fn renewed_at(at: Instant) -> Self {
        Self(Some(at))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_watchdog_starts_expired_and_lives_for_the_window() {
        let mut w = Watchdog::default();
        assert!(!w.live());
        w.renew();
        assert!(w.live());
        w.clear();
        assert!(!w.live());
        let stale = Instant::now() - CONTROL_WATCHDOG - Duration::from_millis(1);
        assert!(!Watchdog::renewed_at(stale).live());
    }

    #[test]
    fn identity_is_stable_within_a_process() {
        let a = Identity::process();
        let b = Identity::process();
        assert_eq!(a.incarnation, b.incarnation);
        assert_eq!(a.incarnation.len(), 32);
        assert_eq!(a.binary_sha256.as_ref().map(String::len), Some(64));
    }
}
