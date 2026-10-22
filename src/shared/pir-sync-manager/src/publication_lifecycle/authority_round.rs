//! One live authority check against the publisher.
use super::{
    envelope_observation::ENVELOPE_LIMIT, AuthorityAdvanced, Envelope, PublicationAdapter,
    PublicationLifecycle, PublicationLifecycleError,
};
use crate::Error;
use pir_publication::{
    AuthorityCheckRequest, AuthorityCheckResponse, ViewIdentity, AUTHORITY_CHECK_PATH,
};

impl<A: PublicationAdapter> PublicationLifecycle<A> {
    /// Ask whether `subjects` are still authoritative under exactly the
    /// publication and fence of the eligible `envelope`. A conflict
    /// re-observes the envelope, so a withdrawal or new fence fences serving
    /// before the pass defers. Every negative answer fences serving, then
    /// permanently invalidates that view in the persisted floor.
    pub(super) async fn check_authority(
        &mut self,
        envelope: &Envelope<A>,
        subjects: Vec<(ViewIdentity, A::AuthoritySubject)>,
    ) -> Result<(), Error> {
        let authority = envelope
            .authority()
            .ok_or(PublicationLifecycleError::WithdrawnBeforeCommit)?;
        let (identities, subjects): (Vec<_>, Vec<_>) = subjects.into_iter().unzip();
        let request = AuthorityCheckRequest {
            authority,
            subjects,
        };
        let mut response = self
            .source
            .post_json(AUTHORITY_CHECK_PATH, serde_json::to_vec(&request)?)
            .await?;
        if response.status() == reqwest::StatusCode::CONFLICT {
            response.discard().await?;
            drop(response);
            self.observe_envelope().await?;
            return Err(Box::new(AuthorityAdvanced));
        }
        if !response.status().is_success() {
            let status = response.status().as_u16();
            response.discard().await?;
            return Err(Box::new(PublicationLifecycleError::AuthorityCheckRefused(
                status,
            )));
        }
        let answer: AuthorityCheckResponse =
            serde_json::from_slice(&response.read_limited(ENVELOPE_LIMIT).await?)?;
        drop(response);
        if answer.authority != authority || answer.still_authoritative.len() != identities.len() {
            return Err(Box::new(
                PublicationLifecycleError::AuthorityResponseMismatch,
            ));
        }
        let refuted: Vec<ViewIdentity> = identities
            .into_iter()
            .zip(answer.still_authoritative)
            .filter_map(|(identity, authoritative)| (!authoritative).then_some(identity))
            .collect();
        if !refuted.is_empty() {
            self.fence_serving().await;
            self.floor.invalidate(refuted);
        }
        if !self.floor.is_durable() {
            self.persist_floor().await?;
        }
        Ok(())
    }
}
