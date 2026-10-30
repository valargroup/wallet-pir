//! Receiver material carried by the shared publisher-query authority contract.
use pir_publication::{
    ObjectReference, PublicationEnvelope, PublicationReference, PublicationScope,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use zakura_pir_query_server::receiver::inventory;
use zakura_pir_query_server::receiver::{self, native::public_bytes, DIRECTORY_ROWS, ROW_BYTES};
use zakura_pir_query_server::receiver::{blocks, Hash};

pub const PREFIX: &str = "/provider/receiver/v1/";
pub const SCHEMA: &str = "receiver-prepared-distribution-v1";
pub const METADATA_LIMIT: u64 = 128 * 1024 * 1024;
pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Current = PublicationEnvelope<Descriptor>;
/// All anchors one immutable view depends on, including its companion contexts.
pub type Subject = Vec<(u32, Hash)>;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Descriptor {
    pub history_revision: u64,
    pub directory: Option<Hash>,
    pub filters: Option<blocks::Head>,
    /// The publisher's recovery epoch when it exported this publication. A
    /// restarted publisher opens a new epoch, so the same tables published
    /// again name a new publication rather than the one query servers saw
    /// withdrawn while it restarted.
    pub recovery_epoch: u64,
}

/// Table bytes, choice object and setups already checked and prepared by the publisher.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Segment {
    pub publication: receiver::Publication,
    pub directory: ObjectReference,
    pub directory_public: ObjectReference,
    pub choice: ObjectReference,
    pub companion: ObjectReference,
    pub companion_public: ObjectReference,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Directory {
    pub head: inventory::Head,
    pub shards: Vec<inventory::Shard>,
    pub segments: Vec<Segment>,
    pub filters: Vec<ObjectReference>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Filters {
    pub head: blocks::Head,
    /// Consecutive encoded ranges from the head's first height through its tip, each a
    /// separate object of at most [`blocks::MAX_RANGE_BYTES`].
    pub ranges: Vec<ObjectReference>,
    /// Every block's hash from the head's first height through its tip, concatenated,
    /// so a response can end at any height.
    pub hashes: ObjectReference,
}

/// Hints may be published before the first directory, and advance without
/// rebuilding an already completed directory's immutable table objects.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Inventory {
    pub genesis: Hash,
    pub history_revision: u64,
    pub directory: Option<Directory>,
    pub filters: Option<Filters>,
    /// The publisher's recovery epoch when it exported this inventory. A
    /// restarted publisher opens a new epoch, so the same tables published
    /// again name a new publication rather than the one query servers saw
    /// withdrawn while it restarted.
    pub recovery_epoch: u64,
}
pub fn scope(genesis: Hash, activation: u32) -> PublicationScope {
    PublicationScope {
        product: SCHEMA.into(),
        chain: genesis,
        source: Sha256::digest(activation.to_le_bytes()).into(),
    }
}
impl Inventory {
    pub fn descriptor(&self) -> Descriptor {
        Descriptor {
            history_revision: self.history_revision,
            directory: self.directory.as_ref().map(|d| d.head.inventory),
            filters: self.filters.as_ref().map(|f| f.head.clone()),
            recovery_epoch: self.recovery_epoch,
        }
    }
    pub fn objects(&self) -> Vec<ObjectReference> {
        let mut objects = Vec::new();
        if let Some(directory) = &self.directory {
            for segment in &directory.segments {
                objects.extend([
                    segment.directory.clone(),
                    segment.directory_public.clone(),
                    segment.choice.clone(),
                    segment.companion.clone(),
                    segment.companion_public.clone(),
                ]);
            }
            objects.extend(directory.filters.iter().cloned());
        }
        if let Some(filters) = &self.filters {
            objects.extend(filters.ranges.iter().cloned());
            objects.push(filters.hashes.clone());
        }
        objects
    }
    pub fn reference(&self, object: ObjectReference) -> PublicationReference<Descriptor> {
        let material =
            Sha256::digest(serde_json::to_vec(self).expect("Receiver inventory serializes")).into();
        PublicationReference::new(material, object, self.descriptor())
    }
    /// Validate structure and all cross-object bindings before loading data.
    pub fn validate(&self, activation: u32) -> Result<(), Error> {
        if self.directory.is_none() && self.filters.is_none() {
            return Err("empty Receiver distribution".into());
        }
        if let Some(directory) = &self.directory {
            let checked = inventory::Inventory::build(
                self.genesis,
                self.history_revision,
                directory.shards.clone(),
            )?;
            if serde_json::to_vec(checked.head())? != serde_json::to_vec(&directory.head)?
                || directory.head.start_height != activation
                || directory
                    .shards
                    .iter()
                    .map(|s| s.segments.len())
                    .sum::<usize>()
                    != directory.segments.len()
            {
                return Err("Receiver directory coverage or identity differs".into());
            }
            for (named, segment) in directory
                .shards
                .iter()
                .flat_map(|s| &s.segments)
                .zip(&directory.segments)
            {
                let p = &segment.publication;
                if named.directory.id()? != p.directory.id()?
                    || named.companion.id()? != p.companion.id()?
                {
                    return Err("Receiver session binding differs".into());
                }
                let (d, c) = (&p.directory, &p.companion);
                let public = public_bytes() as u64;
                for (object, digest, expected) in [
                    (
                        &segment.directory,
                        d.manifest.data_sha256,
                        u64::from(DIRECTORY_ROWS) * ROW_BYTES as u64,
                    ),
                    (&segment.directory_public, d.public_digest, public),
                    (
                        &segment.choice,
                        d.manifest.choice.digest,
                        u64::from(d.manifest.choice.bytes),
                    ),
                    (
                        &segment.companion,
                        c.manifest.data_sha256,
                        u64::from(c.manifest.rows) * ROW_BYTES as u64,
                    ),
                    (&segment.companion_public, c.public_digest, public),
                ] {
                    object.validate(expected)?;
                    if object.bytes != expected || object.sha256 != hex::encode(digest) {
                        return Err("Receiver object differs from its session commitment".into());
                    }
                }
            }
            let named: Vec<_> = directory
                .shards
                .iter()
                .filter_map(|s| s.filter.as_ref())
                .collect();
            if named.len() != directory.filters.len() {
                return Err("Receiver shard filters differ".into());
            }
            for (named, object) in named.into_iter().zip(&directory.filters) {
                object.validate(u64::from(named.bytes))?;
                if object.bytes != u64::from(named.bytes)
                    || object.sha256 != hex::encode(named.digest)
                {
                    return Err("Receiver filter length differs".into());
                }
            }
        }
        if let Some(filters) = &self.filters {
            filters.head.validate(&self.genesis)?;
            let heights = u64::from(filters.head.tip.height - filters.head.first) + 1;
            if filters.head.revision != self.history_revision
                || filters.ranges.is_empty()
                // A `usize` bound always fits in `u64` on supported targets.
                || heights > crate::blocks::MAX_WINDOW_BLOCKS as u64
                || filters.ranges.len() as u64 > heights
            {
                return Err("Receiver block hints differ".into());
            }
            for range in &filters.ranges {
                range.validate(blocks::MAX_RANGE_BYTES as u64)?;
            }
            filters.hashes.validate(heights * 32)?;
            if filters.hashes.bytes != heights * 32 {
                return Err("Receiver block hint hashes differ".into());
            }
        }
        Ok(())
    }
    pub fn anchors(&self) -> Subject {
        let mut anchors = Vec::new();
        if let Some(directory) = &self.directory {
            for shard in &directory.shards {
                anchors.push((shard.end_height, shard.end_hash));
                for segment in &shard.segments {
                    let tree = &segment.companion.manifest.tree;
                    anchors.push((tree.height, tree.hash));
                }
            }
        }
        if let Some(filters) = &self.filters {
            anchors.push((filters.head.tip.height, filters.head.tip.hash));
        }
        anchors.sort_unstable();
        anchors.dedup();
        anchors
    }
}
