//! One live authority check against the publisher.
use super::{
    pointer_observation::POINTER_LIMIT, PointerAdvanced, PublicationAdapter, PublicationLifecycle,
    PublicationLifecycleError,
};
use crate::Error;
use pir_publication::{
    AuthorityCheckRequest, AuthorityCheckResponse, MaterialIdentity, AUTHORITY_CHECK_PATH,
};

impl<A: PublicationAdapter> PublicationLifecycle<A> {
    /// Ask whether `subjects` are still authoritative under `pointer`.
    /// A conflict re-observes the pointer, so a withdrawal or new epoch fences
    /// before the pass defers. Every negative answer fences serving, then
    /// permanently invalidates that view in the persisted floor.
    pub(super) async fn check_authority(
        &mut self,
        pointer: &A::Pointer,
        subjects: Vec<(MaterialIdentity, A::AuthoritySubject)>,
    ) -> Result<(), Error> {
        let (identities, subjects): (Vec<_>, Vec<_>) = subjects.into_iter().unzip();
        let request = AuthorityCheckRequest {
            pointer: pointer.clone(),
            subjects,
        };
        let mut response = self
            .source
            .post_json(AUTHORITY_CHECK_PATH, serde_json::to_vec(&request)?)
            .await?;
        if response.status() == reqwest::StatusCode::CONFLICT {
            response.discard().await?;
            drop(response);
            self.observe_pointer().await?;
            return Err(Box::new(PointerAdvanced));
        }
        if !response.status().is_success() {
            let status = response.status().as_u16();
            response.discard().await?;
            return Err(Box::new(PublicationLifecycleError::AuthorityCheckRefused(
                status,
            )));
        }
        let answer: AuthorityCheckResponse<A::Pointer> =
            serde_json::from_slice(&response.read_limited(POINTER_LIMIT).await?)?;
        drop(response);
        if answer.pointer != *pointer || answer.still_authoritative.len() != identities.len() {
            return Err(Box::new(
                PublicationLifecycleError::AuthorityResponseMismatch,
            ));
        }
        let refuted: Vec<MaterialIdentity> = identities
            .into_iter()
            .zip(answer.still_authoritative)
            .filter_map(|(identity, authoritative)| (!authoritative).then_some(identity))
            .collect();
        if !refuted.is_empty() {
            self.adapter.fence_serving().await;
            if self.floor.invalidate(refuted) {
                self.persist_floor().await?;
            }
        }
        Ok(())
    }
}
