//! Product policy and state supplied to the shared publisher lifecycle.
use super::*;

/// Identical observations need not replace live authority on every source
/// poll. Keep three quarters of the original lifetime available to consumers;
/// the next refresh must still carry a newly checked source observation.
fn keep_published(active: &Manifest, observed: &source::Observation, now: u64) -> bool {
    let snapshot = &observed.snapshot;
    now.saturating_sub(active.observed_ms) < MAX_AGE_MS / 4
        && active.network == snapshot.network
        && active.salt == snapshot.salt
        && active.coverage_start == snapshot.start
        && active.anchor_height == snapshot.height
        && active.anchor_hash == snapshot.anchor
        && active.entries == snapshot.entries
        && active.rows_digest == snapshot.digest
}

/// Observation capture remains independent while publication prepares material.
pub(super) struct Publication<'a> {
    pub(super) shutdown: pir_pipeline::service::Shutdown,
    pub(super) publisher: &'a Publisher,
    pub(super) rpc: ZakuraClient,
    pub(super) rx: watch::Receiver<Option<Arc<source::Observation>>>,
    pub(super) salt: [u8; 32],
    pub(super) authority: Option<authority::Authority>,
    pub(super) previous: Option<Generation>,
    pub(super) last_observed: u64,
    pub(super) server: tokio::task::JoinHandle<std::io::Result<()>>,
    pub(super) observer:
        &'a tokio::task::JoinHandle<Result<(), super::ingestion::ObservationError>>,
}
impl pir_pipeline::publisher::Publisher for Publication<'_> {
    type Error = Failure;
    type Report = Generation;
    type Publication<'a>
        = super::preparation::StatusPublication<'a>
    where
        Self: 'a;
    async fn acquire(&mut self) -> Result<PublicationInput<Self::Publication<'_>>, Failure> {
        if self.server.is_finished() || self.observer.is_finished() {
            return Err("Status background service stopped".into());
        }
        let observed = self.rx.borrow().clone();
        if let Some(observed) = observed.filter(|o| o.observed_ms > self.last_observed) {
            self.publisher
                .open_authority(observed.snapshot.network, self.salt)?;
            // Detect invalidation independently of the publication outcome.
            let active = self.publisher.published.read().unwrap().manifest.clone();
            if let Some(active) = active {
                if self.rpc.block_hash(u64::from(active.anchor_height)).await? != active.anchor_hash
                {
                    self.publisher.withdraw();
                }
            }
            // Candidate validation can also withdraw. After any withdrawal
            // earlier answers may no longer be trusted: start a new recovery
            // epoch and prepare from scratch.
            if self.publisher.last_transition_withdrew() {
                self.publisher.recover();
                self.previous = None;
            }
            let unchanged = self
                .publisher
                .published
                .read()
                .unwrap()
                .manifest
                .as_ref()
                .is_some_and(|active| keep_published(active, &observed, now_ms()));
            if unchanged {
                self.last_observed = observed.observed_ms;
                return Ok(PublicationInput::Unchanged);
            }
            if self.authority.is_none() {
                self.authority = Some(authority::Authority::open(
                    self.publisher.config.state_dir.join("authority"),
                    observed.snapshot.network,
                    self.salt,
                )?);
            }
            self.last_observed = observed.observed_ms;
            let refresh = self.previous.as_ref().is_some_and(|previous| {
                previous.manifest().network == observed.snapshot.network
                    && previous.manifest().salt == observed.snapshot.salt
                    && previous.manifest().rows_digest == observed.snapshot.digest
            });
            let publication = super::preparation::StatusPublication::new(
                self.publisher,
                observed,
                self.authority.as_mut().unwrap(),
                self.previous.clone(),
                &self.rpc,
                self.shutdown.clone(),
            )?;
            return Ok(if refresh {
                PublicationInput::MetadataRefresh(publication)
            } else {
                PublicationInput::NewMaterial(publication)
            });
        }
        Ok(PublicationInput::Unchanged)
    }
    fn completed(&mut self, generation: Generation) {
        self.previous = Some(generation);
    }
    fn failed(&mut self, error: &Failure) -> pir_pipeline::FailurePolicy {
        if self.server.is_finished() || self.observer.is_finished() {
            return pir_pipeline::FailurePolicy::Stop;
        }
        if error.downcast_ref::<authority::AuthorityError>().is_some()
            || error
                .downcast_ref::<pir_publication::PublicationAuthorityError>()
                .is_some()
        {
            self.publisher.withdraw();
            self.authority = None;
        }
        tracing::warn!(%error, "Status publication deferred");
        pir_pipeline::FailurePolicy::NextPoll
    }
    async fn wait(&mut self) -> Result<(), Failure> {
        self.rx.changed().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_observations_coalesce_without_delaying_changes_or_renewing_age() {
        let snapshot = Arc::new(status_shared::fixture::snapshot(8, false).unwrap());
        let observed = source::Observation {
            snapshot: snapshot.clone(),
            observed_ms: 2000,
            collect_ms: 0.,
            blocks: 64,
            mempool: 1,
            retained_forks: 1,
        };
        let active = Manifest {
            protocol: PROTOCOL.into(),
            network: snapshot.network,
            salt: snapshot.salt,
            generation: 1,
            recovery_epoch: 1,
            coverage_start: snapshot.start,
            anchor_height: snapshot.height,
            anchor_hash: snapshot.anchor,
            entries: snapshot.entries,
            rows_digest: snapshot.digest,
            public_digest: [1; 32],
            observed_ms: 1000,
        };
        assert!(keep_published(
            &active,
            &observed,
            1000 + MAX_AGE_MS / 4 - 1
        ));
        assert!(!keep_published(&active, &observed, 1000 + MAX_AGE_MS / 4));
        assert_eq!(active.observed_ms, 1000);
        for field in [
            "network", "salt", "start", "height", "anchor", "entries", "rows",
        ] {
            let mut changed = active.clone();
            match field {
                "network" => changed.network[0] ^= 1,
                "salt" => changed.salt[0] ^= 1,
                "start" => changed.coverage_start += 1,
                "height" => changed.anchor_height += 1,
                "anchor" => changed.anchor_hash[0] ^= 1,
                "entries" => changed.entries += 1,
                "rows" => changed.rows_digest[0] ^= 1,
                _ => unreachable!(),
            }
            assert!(!keep_published(&changed, &observed, 2000), "{field}");
        }
    }
}
