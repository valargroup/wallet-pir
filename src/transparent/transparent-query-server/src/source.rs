//! Authenticated HTTPS publication source and durable-write helpers shared by
//! history and transaction-detail synchronization.
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use transparent_shared::publication::{self, Current, Error};

pub struct Config {
    pub publisher: String,
    pub network: String,
    pub genesis_hash: String,
    pub directory: PathBuf,
    pub cache_bytes: u64,
    /// Hard transfer budget for one bootstrap, including metadata.
    pub max_download_bytes: u64,
    /// Additional trusted roots, never a TLS verification bypass.
    pub roots: Vec<reqwest::Certificate>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HighWater {
    origin: String,
    current: Current,
}

pub(crate) struct Source {
    transport: pir_sync_manager::HttpsSource,
    observed: Option<Current>,
    schema: &'static str,
}
impl Source {
    #[cfg_attr(not(feature = "transparent-history"), allow(dead_code))]
    pub(crate) fn new(config: &Config) -> Result<Self, Error> {
        Self::for_product(config, publication::PREFIX, publication::SCHEMA)
    }
    pub(crate) fn for_product(
        config: &Config,
        prefix: &'static str,
        schema: &'static str,
    ) -> Result<Self, Error> {
        Ok(Self {
            transport: pir_sync_manager::HttpsSource::new(
                &config.publisher,
                prefix,
                config.max_download_bytes,
                &config.roots,
            )?,
            observed: None,
            schema,
        })
    }
    pub(crate) fn downloaded(&self) -> u64 {
        self.transport.downloaded()
    }
    pub(crate) async fn get(
        &mut self,
        suffix: &str,
    ) -> Result<pir_sync_manager::Response<'_>, Error> {
        let mut response = self.transport.request(suffix, None).await?;
        if response.status() != reqwest::StatusCode::OK {
            response.discard().await?;
            return Err(format!(
                "publisher refused distribution with HTTP {}",
                response.status()
            )
            .into());
        }
        Ok(response)
    }
    pub(crate) async fn small(&mut self, suffix: &str, limit: usize) -> Result<Vec<u8>, Error> {
        self.get(suffix).await?.read_limited(limit).await
    }
    pub(crate) fn observed_revision(&self, directory: &std::path::Path) -> Result<u64, Error> {
        let mut revision = 0;
        for name in [
            "publisher-high-water.json",
            "publisher-high-water-fallback.json",
        ] {
            match std::fs::read(directory.join(name)) {
                Ok(bytes) => {
                    let saved: HighWater = serde_json::from_slice(&bytes)?;
                    if saved.origin != self.transport.origin().as_str() {
                        return Err("publisher origin change".into());
                    }
                    revision = revision.max(saved.current.revision);
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(revision)
    }
    // Storage errors are distinct from contradictory/replayed evidence. Once
    // this exclusively owned Source has a trusted full observation, it can
    // order a negative response despite an unreadable record, but may not use
    // that response to activate. Check EVERY readable record before returning.
    pub(crate) fn validate_observation(
        &self,
        directory: &std::path::Path,
        current: &Current,
    ) -> Result<Option<Error>, Error> {
        if self
            .observed
            .as_ref()
            .is_some_and(|saved| !current.succeeds(saved))
        {
            return Err("publisher replay or contradictory in-memory current record".into());
        }
        let mut storage_error = None;
        for name in [
            "publisher-high-water.json",
            "publisher-high-water-fallback.json",
        ] {
            let saved = match std::fs::read(directory.join(name)) {
                Ok(bytes) => match serde_json::from_slice::<HighWater>(&bytes) {
                    Ok(saved) => Some(saved),
                    Err(error) => {
                        storage_error.get_or_insert_with(|| error.into());
                        None
                    }
                },
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => {
                    storage_error.get_or_insert_with(|| error.into());
                    None
                }
            };
            if let Some(saved) = saved {
                if saved.origin != self.transport.origin().as_str()
                    || !current.succeeds(&saved.current)
                {
                    return Err(
                        "publisher origin change, replay or contradictory current record".into(),
                    );
                }
            }
        }
        if self.observed.is_none() {
            if let Some(error) = storage_error.take() {
                return Err(error);
            }
        }
        Ok(storage_error)
    }
    pub(crate) fn observe(
        &mut self,
        directory: &std::path::Path,
        current: &Current,
    ) -> Result<(), Error> {
        let storage_error = self.validate_observation(directory, current)?;
        // A failed durable write cannot erase a positive observation in this
        // process. The manager still applies its exact revocation to the owner.
        self.observed = Some(current.clone());
        if let Some(error) = storage_error {
            return Err(error);
        }
        // Keep the full source identity as well as its revision: an equal
        // revision must not substitute another inventory after a write failure.
        let saved = HighWater {
            origin: self.transport.origin().to_string(),
            current: current.clone(),
        };
        let primary = crate::control::persist(&directory.join("publisher-high-water.json"), &saved);
        let fallback = directory.join("publisher-high-water-fallback.json");
        if primary.is_err() || fallback.exists() {
            crate::control::persist(&fallback, &saved)?;
        }
        primary?;
        Ok(())
    }

    #[cfg(feature = "transparent-history")]
    pub(crate) async fn check_anchor(
        &mut self,
        directory: &std::path::Path,
        current: &Current,
    ) -> Result<(), Error> {
        let check: publication::AnchorCheck = serde_json::from_slice(
            &self
                .small(
                    &format!("anchors/{}/{}", current.anchor_height, current.anchor_hash),
                    16 << 10,
                )
                .await?,
        )?;
        if check.revision != current.revision || check.recovery_epoch != current.recovery_epoch {
            return Err("publication superseded during canonicality validation".into());
        }
        if check.schema != self.schema
            || check.genesis_hash != current.genesis_hash
            || check.height != current.anchor_height
            || check.hash != current.anchor_hash
        {
            return Err("canonicality attestation has another identity/recovery snapshot".into());
        }
        if !check.canonical {
            let mut rejected = current.clone();
            rejected.eligibility = publication::Eligibility::Noncanonical;
            self.observe(directory, &rejected)?;
            return Err("candidate anchor is noncanonical".into());
        }
        Ok(())
    }

    pub(crate) fn reset_budget(&mut self) {
        self.transport.reset_budget();
    }
    pub(crate) async fn poll(
        &mut self,
        directory: &std::path::Path,
        previous: Option<&Current>,
        network: &str,
        genesis: &str,
    ) -> Result<Option<(Current, Option<Error>)>, Error> {
        let conditional = previous
            .map(|previous| {
                serde_json::to_vec(previous)
                    .map(|bytes| format!("\"{}\"", publication::digest(&bytes)))
            })
            .transpose()?;
        let mut response = self
            .transport
            .request("current", conditional.as_deref())
            .await?;
        if response.status() == reqwest::StatusCode::NOT_MODIFIED {
            if previous.is_none() {
                return Err("unexpected unchanged response".into());
            }
            return Ok(None);
        }
        let status = response.status();
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            if chunk.len() > (16usize << 10).saturating_sub(bytes.len()) {
                return Err("current exceeds limit".into());
            }
            bytes.extend_from_slice(&chunk);
        }
        if status != reqwest::StatusCode::OK {
            return Err(format!("publisher refused current with HTTP {status}").into());
        }
        drop(response);
        let current: Current = serde_json::from_slice(&bytes)?;
        if current.schema != self.schema
            || current.revision == 0
            || !publication::is_digest(&current.inventory_sha256)
            || !publication::is_digest(&current.anchor_hash)
            || current.network != network
            || current.genesis_hash != genesis
        {
            return Err("invalid publisher current chain identity".into());
        }
        // Persistence belongs to the manager after returning the authenticated,
        // replay-checked snapshot, so write failure cannot hide invalidation.
        let storage_error = self.validate_observation(directory, &current)?;
        // Carry the FIRST read failure across the manager handoff, even if a
        // second read succeeds. Negative evidence still fences; activation fails.
        Ok(Some((current, storage_error)))
    }
    #[cfg(feature = "transparent-detail")]
    pub(crate) async fn anchor(
        &mut self,
        current: &Current,
        height: u64,
        hash: &str,
    ) -> Result<bool, Error> {
        let check: publication::AnchorCheck = serde_json::from_slice(
            &self
                .small(&format!("anchors/{height}/{hash}"), 16 << 10)
                .await?,
        )?;
        if check.schema != self.schema
            || check.revision != current.revision
            || check.recovery_epoch != current.recovery_epoch
            || check.genesis_hash != current.genesis_hash
            || check.height != height
            || check.hash != hash
        {
            return Err("canonicality attestation identity/snapshot superseded".into());
        }
        Ok(check.canonical)
    }
    #[cfg(feature = "transparent-history")]
    /// Fresh canonicality of every anchor in one batch, for exactly
    /// `current`'s snapshot. Bodies, refused ones included, consume the same
    /// budget as every other source read; any failure answers for no anchor.
    pub(crate) async fn anchors(
        &mut self,
        current: &Current,
        anchors: &[publication::AnchorPair],
    ) -> Result<std::collections::BTreeMap<publication::AnchorPair, bool>, Error> {
        let request = publication::AnchorBatchRequest::new(current, anchors.to_vec())?;
        let body = serde_json::to_vec(&request)?;
        if body.len() > publication::MAX_ANCHOR_BATCH_BYTES {
            return Err("anchor batch request exceeds limit".into());
        }
        let mut response = self
            .transport
            .post_json(publication::ANCHOR_BATCH_PATH, body)
            .await?;
        if response.status() != reqwest::StatusCode::OK {
            response.discard().await?;
            return Err(format!(
                "publisher refused distribution with HTTP {}",
                response.status()
            )
            .into());
        }
        let raw = response
            .read_limited(publication::MAX_ANCHOR_BATCH_BYTES)
            .await?;
        let batch: publication::AnchorBatch = serde_json::from_slice(&raw)?;
        batch.answers(&request, &current.genesis_hash)
    }
    #[cfg(feature = "transparent-history")]
    pub(crate) async fn current(&mut self, directory: &std::path::Path) -> Result<Current, Error> {
        let current: Current = serde_json::from_slice(&self.small("current", 16 << 10).await?)?;
        if current.schema != self.schema
            || current.revision == 0
            || !publication::is_digest(&current.inventory_sha256)
            || !publication::is_digest(&current.genesis_hash)
            || !publication::is_digest(&current.anchor_hash)
        {
            return Err("invalid publisher current record".into());
        }
        self.observe(directory, &current)?;
        if !current.eligible() {
            return Err("publisher current publication is withdrawn or noncanonical".into());
        }
        Ok(current)
    }
}

// Persist child entries before their parents and before the owner's active
// pointer. Linked objects and fallback copies are synced like downloads.
pub(crate) fn sync_tree(path: &std::path::Path) -> Result<(), Error> {
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            sync_tree(&entry.path())?;
        } else {
            std::fs::File::open(entry.path())?.sync_all()?;
        }
    }
    std::fs::File::open(path)?.sync_all()?;
    std::fs::File::open(path.parent().ok_or("stage has no parent")?)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod observation_tests {
    use super::*;
    use publication::Eligibility;

    #[test]
    fn damaged_high_water_requires_memory_and_never_hides_a_readable_contradiction() {
        // The HTTP tests cover propagation into live serving and activation.
        // Exercise both file locations and error kinds here without PIR setup.
        for failed in [
            "publisher-high-water.json",
            "publisher-high-water-fallback.json",
        ] {
            for unreadable in [false, true] {
                let root = tempfile::tempdir().unwrap();
                let current = Current {
                    schema: publication::SCHEMA.into(),
                    revision: 10,
                    recovery_epoch: 0,
                    inventory_sha256: "11".repeat(32),
                    network: "main".into(),
                    genesis_hash: "00".repeat(32),
                    anchor_height: 2,
                    anchor_hash: "22".repeat(32),
                    eligibility: Eligibility::Canonical,
                    invalidated_from: None,
                };
                let mut source = Source::new(&Config {
                    publisher: "https://publisher.invalid".into(),
                    network: current.network.clone(),
                    genesis_hash: current.genesis_hash.clone(),
                    directory: root.path().into(),
                    cache_bytes: 0,
                    max_download_bytes: 1024,
                    roots: Vec::new(),
                })
                .unwrap();
                let path = root.path().join(failed);
                if unreadable {
                    std::fs::create_dir(&path).unwrap();
                } else {
                    std::fs::write(&path, b"{").unwrap();
                }
                assert!(
                    source.validate_observation(root.path(), &current).is_err(),
                    "startup must refuse a damaged record: {failed}, unreadable={unreadable}"
                );
                source.observed = Some(current.clone());
                let mut next = current.clone();
                next.revision += 1;
                next.eligibility = Eligibility::Withdrawn;
                let storage = source.validate_observation(root.path(), &next).unwrap();
                assert!(
                    storage.is_some(),
                    "trusted memory orders evidence but does not forgive storage"
                );
                assert!(source.observe(root.path(), &next).is_err());
                assert_eq!(source.observed.as_ref(), Some(&next));
                assert!(
                    source.validate_observation(root.path(), &current).is_err(),
                    "a failed write cannot erase the in-process revision floor"
                );
                let mut reenable = next.clone();
                reenable.eligibility = Eligibility::Canonical;
                assert!(
                    source.validate_observation(root.path(), &reenable).is_err(),
                    "same-revision recovery cannot erase a withdrawal"
                );

                let companion = if failed.contains("fallback") {
                    "publisher-high-water.json"
                } else {
                    "publisher-high-water-fallback.json"
                };
                let mut saved = HighWater {
                    origin: source.transport.origin().to_string(),
                    current: next.clone(),
                };
                crate::control::persist(&root.path().join(companion), &saved).unwrap();
                assert!(source
                    .validate_observation(root.path(), &next)
                    .unwrap()
                    .is_some());
                saved.current.revision += 1;
                crate::control::persist(&root.path().join(companion), &saved).unwrap();
                assert!(
                    source.validate_observation(root.path(), &next).is_err(),
                    "a damaged record must not hide a newer readable companion"
                );
                saved.current = next.clone();
                saved.current.inventory_sha256 = "33".repeat(32);
                crate::control::persist(&root.path().join(companion), &saved).unwrap();
                assert!(
                    source.validate_observation(root.path(), &next).is_err(),
                    "equal revisions cannot substitute another inventory"
                );
                saved.current = next.clone();
                saved.origin = "https://another.invalid/".into();
                crate::control::persist(&root.path().join(companion), &saved).unwrap();
                assert!(
                    source.validate_observation(root.path(), &next).is_err(),
                    "a damaged record must not hide a readable origin mismatch"
                );
            }
        }
    }
}
