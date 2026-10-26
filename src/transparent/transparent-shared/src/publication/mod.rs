//! History-only prepared-publication distribution. Verified bytes alone never
//! grant serving authority. This inventory is not a wallet format.
use zakura_pir_protocol::transparent::filter as transparent_filter;
use zakura_pir_protocol::transparent::shard as transparent_shard;

use crate::{
    history::shardset::ShardSet,
    runtime::{disk::ShippedRuntimes, SharedParams},
};
use pir_publication::{
    hash_object_file, open_object_file, verify_object_file, FileIdentity, ObjectFileError,
    ObjectReference,
};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    path::{Path, PathBuf},
};

mod contracts;
pub use contracts::{
    received_artifact, AnchorBatch, AnchorBatchRequest, AnchorCheck, AnchorPair, AnchorResult,
    Artifact, Current, Eligibility, Error, Inventory, Kind, ReceivedArtifact, ANCHOR_BATCH_PATH,
    MAX_ANCHOR_BATCH, MAX_ANCHOR_BATCH_BYTES, MAX_METADATA_BYTES, PREFIX, PROFILE, RUNTIME, SCHEMA,
};
mod artifacts;
#[cfg(test)]
use artifacts::hash_artifacts;
use artifacts::listed_digest;
#[cfg(test)]
pub(crate) use artifacts::listed_hashes;
pub(crate) use artifacts::resolve;
use artifacts::resolve_path;
pub(crate) use artifacts::Known;
pub(crate) use artifacts::Listed;
pub use artifacts::{digest, open_artifact, record_received_file, safe_path, verify_file, Stopped};
pub(crate) use artifacts::{hash_listed, hash_until};
mod finalization;
use finalization::plan;
pub use finalization::Finalized;
mod preparation;
pub use preparation::{PreparationReuse, PREPARATION_FILE};
mod restoration;
pub use restoration::{RestoreExpectation, RestoreRecord, Restored, RESTORE_FILE, RESTORE_SCHEMA};
mod validation;
pub use crate::history::shardset::OBJECT_STORE;
pub use pir_publication::is_object_digest as is_digest;
pub use validation::{CheckedRuntimes, CorrectnessError, IntegrityError, Validation, Verified};

#[cfg(test)]
mod tests;
