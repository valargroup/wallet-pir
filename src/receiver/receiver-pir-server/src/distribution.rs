//! Receiver material carried by the shared publisher-query authority contract.
use pir_publication::{
    ObjectReference, PublicationEnvelope, PublicationReference, PublicationScope,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use zakura_pir_query_server::receiver::{blocks, inventory, session, Hash};

pub const PREFIX: &str = "/provider/receiver/v1/";
pub const SCHEMA: &str = "receiver-prepared-distribution-v1";
pub const METADATA_LIMIT: u64 = 128 * 1024 * 1024;
pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Current = PublicationEnvelope<Descriptor>;
/// All anchors one immutable view depends on, including its witness contexts.
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
    /// withdrawn while it restarted. Publishers before this field send none.
    #[serde(default)]
    pub recovery_epoch: u64,
}

/// Table bytes and setup already checked and prepared by the publisher.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Segment {
    pub publication: session::Publication,
    pub payments: ObjectReference,
    pub payment_public: ObjectReference,
    pub witnesses: ObjectReference,
    pub witness_public: ObjectReference,
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
    /// Consecutive bounded wallet batch encodings, each a separate object.
    pub batches: Vec<ObjectReference>,
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
    /// withdrawn while it restarted. Publishers before this field send none.
    #[serde(default)]
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
                    segment.payments.clone(),
                    segment.payment_public.clone(),
                    segment.witnesses.clone(),
                    segment.witness_public.clone(),
                ]);
            }
            objects.extend(directory.filters.iter().cloned());
        }
        if let Some(filters) = &self.filters {
            objects.extend(filters.batches.iter().cloned());
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
                if named.payments.id()? != segment.publication.payments.id()?
                    || named.witnesses.id()? != segment.publication.witnesses.id()?
                {
                    return Err("Receiver session binding differs".into());
                }
                let rows = &segment.publication;
                for (object, digest, expected) in [
                    (
                        &segment.payments,
                        rows.payments.directory.data_sha256,
                        u64::from(rows.payments.directory.rows) * 4096,
                    ),
                    (
                        &segment.witnesses,
                        rows.witnesses.table.data_sha256,
                        u64::from(rows.witnesses.table.rows) * 4096,
                    ),
                    (
                        &segment.payment_public,
                        rows.payments.public_digest,
                        zakura_pir_query_server::receiver::native::public_bytes(
                            rows.payments.directory.rows,
                        )? as u64,
                    ),
                    (
                        &segment.witness_public,
                        rows.witnesses.public_digest,
                        zakura_pir_query_server::receiver::native::public_bytes(
                            rows.witnesses.table.rows,
                        )? as u64,
                    ),
                ] {
                    object.validate(expected)?;
                    if object.bytes != expected || object.sha256 != hex::encode(digest) {
                        return Err("Receiver object differs from its session commitment".into());
                    }
                }
                for (object, expected) in [
                    (
                        &segment.payments,
                        u64::from(rows.payments.directory.rows) * 4096,
                    ),
                    (
                        &segment.witnesses,
                        u64::from(rows.witnesses.table.rows) * 4096,
                    ),
                ] {
                    object.validate(expected)?;
                    if object.bytes != expected {
                        return Err("Receiver table length differs".into());
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
            if filters.head.revision != self.history_revision
                || filters.batches.is_empty()
                || filters.batches.len() > 24192
            {
                return Err("Receiver rolling filters differ".into());
            }
            for batch in &filters.batches {
                batch.validate(blocks::MAX_BATCH_BYTES as u64)?;
            }
        }
        for object in self.objects() {
            object.validate(u64::MAX)?;
        }
        Ok(())
    }
    pub fn anchors(&self) -> Subject {
        let mut anchors = Vec::new();
        if let Some(directory) = &self.directory {
            for shard in &directory.shards {
                anchors.push((shard.end_height, shard.end_hash));
                for segment in &shard.segments {
                    let context = &segment.witnesses.table;
                    anchors.push((context.tree_height, context.tree_hash));
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
