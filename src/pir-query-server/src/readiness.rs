//! Typed in-process readiness the host reports on its management listener.
//!
//! Each running product registers its own serving-eligibility check once it
//! has opened its state. The host calls that check directly instead of
//! probing the product's private health route over loopback HTTP. A product
//! that has not registered yet is not ready.
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, OnceLock},
};

type Check = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = bool> + Send>> + Send + Sync>;

/// One product's serving eligibility, as the product itself decides it.
#[derive(Clone, Default)]
pub struct Readiness(Arc<OnceLock<Check>>);

impl Readiness {
    /// Report readiness through `check`. Only the first registration counts.
    pub fn register<F, R>(&self, check: F)
    where
        F: Fn() -> R + Send + Sync + 'static,
        R: Future<Output = bool> + Send + 'static,
    {
        let _ = self.0.set(Arc::new(move || Box::pin(check())));
    }

    /// Whether the product can currently answer wallets with an authorized,
    /// usable view.
    pub async fn ready(&self) -> bool {
        let check = self.0.get().cloned();
        match check {
            Some(check) => check().await,
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[tokio::test]
    async fn unregistered_products_are_not_ready() {
        let readiness = Readiness::default();
        assert!(!readiness.ready().await);
        let flag = Arc::new(AtomicBool::new(false));
        let observed = flag.clone();
        readiness.register(move || {
            let ready = observed.load(Ordering::Acquire);
            async move { ready }
        });
        assert!(!readiness.ready().await);
        flag.store(true, Ordering::Release);
        assert!(readiness.clone().ready().await);
    }
}
