//! Stored publisher and distribution identities, shared with offline handoff.
//! These types describe bytes on disk; they do not grant serving authority.
use enhance_query_server::{
    protocol::{Lifecycle, Manifest},
    publication::{Current, Object},
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Recovery fences survive restarts and never imply current chain authority.
pub(crate) struct RecoveryState {
    pub(crate) epoch: u64,
    pub(crate) domain_epochs: BTreeMap<u64, String>,
    pub(crate) revoked: BTreeSet<String>,
    pub(crate) sealed: BTreeMap<u64, (u64, String)>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PublisherState {
    pub(crate) version: u16,
    pub(crate) next_generation: u64,
    pub(crate) lifecycle: Lifecycle,
    pub(crate) recovery: RecoveryState,
}
impl Default for PublisherState {
    fn default() -> Self {
        Self {
            version: 1,
            next_generation: 1,
            lifecycle: Default::default(),
            recovery: Default::default(),
        }
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RetainedPublication {
    pub(crate) manifest: Manifest,
    pub(crate) inventory: Object,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DistributionState {
    pub(crate) current: Current,
    pub(crate) retained: Vec<RetainedPublication>,
}

#[cfg(test)]
mod tests;
