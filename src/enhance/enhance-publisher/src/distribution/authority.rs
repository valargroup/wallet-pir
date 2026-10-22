//! Fresh source checks and withdrawal fences govern every authority response.
use super::PublicationDistribution;
use enhance_shared::{
    protocol::{canonical_hash, digest, Manifest},
    publication::{AuthorityResponse, SOURCE_AUTHORITY_LIFETIME},
};
use pir_publication::SourceObservation;
use std::sync::atomic::Ordering;

impl PublicationDistribution {
    /// Observe the chain node: the manifest's anchor must still be canonical.
    /// The observation starts before the lookup, so its lifetime covers it.
    pub(super) async fn check_canonical_anchor(
        &self,
        manifest: &Manifest,
    ) -> Result<SourceObservation, String> {
        let observation = SourceObservation::starting_now();
        let elapsed = std::time::Instant::now();
        if self
            .rpc
            .block_hash_display(manifest.anchor_height)
            .await
            .map_err(|e| e.to_string())?
            != manifest.anchor_block_hash
        {
            return Err("Enhance publication anchor is no longer canonical".into());
        }
        if elapsed.elapsed() > SOURCE_AUTHORITY_LIFETIME {
            return Err("Enhance source check exceeded authority lifetime".into());
        }
        Ok(observation)
    }

    pub(crate) async fn authority(
        &self,
        requested: Option<&str>,
    ) -> Result<AuthorityResponse, String> {
        if self.poisoned.load(Ordering::Acquire) {
            return Err("Enhance distribution persistence failed".into());
        }
        let (current, target) = {
            let disk = self.disk.lock().unwrap();
            let current = disk.current.clone();
            let target = match requested {
                Some(id) if canonical_hash(id) => disk
                    .retained
                    .iter()
                    .find(|r| digest(&r.manifest) == id)
                    .map(|r| r.manifest.clone()),
                Some(_) => return Err("invalid requested manifest identity".into()),
                None => current.manifest.clone(),
            };
            (current, target)
        };
        // Return durable withdrawal metadata without granting fresh authority.
        if !current.eligible && requested.is_none() {
            return Ok(AuthorityResponse {
                current,
                authorized_manifest_sha256: String::new(),
                source: SourceObservation::NONE,
            });
        }
        let manifest = target.ok_or("Enhance publication is no longer retained")?;
        if !current.eligible
            || manifest.coverage.shards.iter().any(|s| {
                manifest
                    .session_id(s.id)
                    .is_ok_and(|id| current.revoked.contains(&hex::encode(id)))
            })
        {
            return Err("Enhance publication is withdrawn".into());
        }
        let source = self.check_canonical_anchor(&manifest).await?;
        // Recheck after the RPC: a withdrawal concurrent with the node check
        // must be visible before the response grants any authority.
        let disk = self.disk.lock().unwrap();
        let latest = &disk.current;
        if self.poisoned.load(Ordering::Acquire)
            || !latest.eligible
            || (requested.is_none() && latest.manifest != current.manifest)
            || latest.withdrawal_revision != current.withdrawal_revision
            || !disk
                .retained
                .iter()
                .any(|retained| retained.manifest == manifest)
            || manifest.coverage.shards.iter().any(|shard| {
                manifest
                    .session_id(shard.id)
                    .is_ok_and(|id| latest.revoked.contains(&hex::encode(id)))
            })
        {
            return Err("Enhance authority changed during source validation".into());
        }
        // Ordinary publication advancement does not revoke a retained binding.
        // Return the latest floors together with this real canonical observation.
        Ok(AuthorityResponse {
            current: latest.clone(),
            authorized_manifest_sha256: digest(&manifest),
            source,
        })
    }
}
