//! Observe the publication envelope and advance the durable floor.
use super::{Envelope, PublicationAdapter, PublicationLifecycle, PublicationLifecycleError};
use crate::Error;
use pir_publication::{conditional_token, CURRENT_PATH};

/// Bound on an envelope body; envelopes name objects, never contain material.
pub(super) const ENVELOPE_LIMIT: usize = 64 << 10;

impl<A: PublicationAdapter> PublicationLifecycle<A> {
    /// Fetch the envelope, refuse malformed envelopes and replays, fence
    /// serving on negative evidence, then persist the raised floor. A 304
    /// reuses the last received body and renews nothing.
    pub(super) async fn observe_envelope(&mut self) -> Result<Envelope<A>, Error> {
        let token = self.last_received_envelope.as_ref().map(conditional_token);
        let mut response = self.source.request(CURRENT_PATH, token.as_deref()).await?;
        let envelope = if response.status() == reqwest::StatusCode::NOT_MODIFIED {
            response.discard().await?;
            self.last_received_envelope
                .clone()
                .ok_or(PublicationLifecycleError::UnexpectedNotModified)?
        } else if response.status().is_success() {
            serde_json::from_slice::<Envelope<A>>(&response.read_limited(ENVELOPE_LIMIT).await?)?
        } else {
            let status = response.status().as_u16();
            response.discard().await?;
            return Err(Box::new(PublicationLifecycleError::EnvelopeUnavailable(
                status,
            )));
        };
        drop(response);
        envelope
            .validate()
            .map_err(|_| PublicationLifecycleError::MalformedEnvelope)?;
        self.adapter.validate_envelope(&envelope)?;
        let checked = self.floor.classify(envelope.clone())?;
        // Negative evidence fences serving before anything fallible is persisted.
        let fenced = if !envelope.eligible || checked.succession().fences_earlier_views() {
            self.fence_serving().await
        } else {
            Vec::new()
        };
        self.floor.record(checked, fenced)?;
        // A dirty floor (a new binding, a change, or an earlier failed save) is
        // saved before this observation counts, even if the envelope is unchanged.
        if !self.floor.is_durable() {
            self.persist_floor().await?;
        }
        self.last_received_envelope = Some(envelope.clone());
        Ok(envelope)
    }
}
