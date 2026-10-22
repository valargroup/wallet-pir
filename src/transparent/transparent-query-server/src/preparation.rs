//! Typed refusals and preparation records shared by the publication owners
//! of the shard and txid display workers.

use serde::Serialize;
use std::sync::RwLock;

/// Why a publication command was refused, by what the controller must do.
///
/// Internal: a refused command reports only the text. The kind is what the
/// private status report shows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Refusal {
    /// The supplied publication or assignment does not verify. Retrying the
    /// same input cannot succeed.
    InvalidSource(String),
    /// Building or warming runtimes failed. The input verified; the
    /// publication may prepare on retry or on another worker.
    Runtime(String),
    /// The worker cannot take the candidate as asked: its budget, or a
    /// conflicting hold of the same publication.
    Admission(String),
    /// The predecessor or the chain moved on while the command waited or ran.
    Superseded(String),
}

impl Refusal {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::InvalidSource(_) => "invalid_source",
            Self::Runtime(_) => "runtime",
            Self::Admission(_) => "admission",
            Self::Superseded(_) => "superseded",
        }
    }

    pub fn message(&self) -> &str {
        match self {
            Self::InvalidSource(message)
            | Self::Runtime(message)
            | Self::Admission(message)
            | Self::Superseded(message) => message,
        }
    }
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message())
    }
}

impl From<Refusal> for String {
    fn from(refusal: Refusal) -> Self {
        refusal.message().to_string()
    }
}

/// How the most recent preparation ended, for the private status report.
/// In memory only: a restart reports none rather than a stale outcome.
#[derive(Clone, Debug, Serialize)]
pub struct PreparationOutcome {
    map_sha256: String,
    outcome: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    verification: Option<transparent_shared::history::shardset::VerificationReport>,
    finished_unix: u64,
}

impl PreparationOutcome {
    pub fn new<T>(
        map_sha256: String,
        outcome: &Result<T, Refusal>,
        verification: Option<transparent_shared::history::shardset::VerificationReport>,
    ) -> Self {
        let finished_unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |since| since.as_secs());
        let (outcome, error) = match outcome {
            Ok(_) => ("prepared", None),
            Err(refusal) => (refusal.kind(), Some(refusal.message().to_string())),
        };
        Self {
            map_sha256,
            outcome,
            error,
            verification,
            finished_unix,
        }
    }
}

/// Records a preparation in progress for status reports, and clears the
/// record however the preparation ends. Shared by the shard and display
/// publication owners.
pub struct PreparingGuard<'a, T>(&'a RwLock<Option<T>>);
impl<'a, T> PreparingGuard<'a, T> {
    pub fn record(slot: &'a RwLock<Option<T>>, preparing: T) -> Self {
        *slot.write().unwrap() = Some(preparing);
        Self(slot)
    }
}
impl<T> Drop for PreparingGuard<'_, T> {
    fn drop(&mut self) {
        *self.0.write().unwrap() = None;
    }
}
