//! Fresh source checks and withdrawal fences govern every authority response.
use super::PublicationDistribution;
use enhance_shared::{
    protocol::{canonical_hash, digest, Manifest},
    publication::AuthorityResponse,
};
use std::{
    sync::atomic::Ordering,
    time::{SystemTime, UNIX_EPOCH},
};

impl PublicationDistribution {
    pub(super) async fn check_canonical_anchor(&self, manifest: &Manifest) -> Result<u64, String> {
        let started = now_ms()?;
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
        if elapsed.elapsed() > pir_control::CONTROL_WATCHDOG {
            return Err("Enhance source check exceeded authority lifetime".into());
        }
        Ok(started)
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
                observed_unix_ms: 0,
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
        let observed_unix_ms = self.check_canonical_anchor(&manifest).await?;
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
            observed_unix_ms,
        })
    }
}
fn now_ms() -> Result<u64, String> {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| e.to_string())?
            .as_millis(),
    )
    .map_err(|e| e.to_string())
}
