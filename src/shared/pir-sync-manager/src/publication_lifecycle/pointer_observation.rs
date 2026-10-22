//! Observe the publication pointer and advance the durable floor.
use super::{PublicationAdapter, PublicationLifecycle, PublicationLifecycleError};
use crate::Error;
use pir_publication::{conditional_token, PublicationPointer, CURRENT_PATH};

/// Bound on a pointer body; pointers name objects, never contain material.
pub(super) const POINTER_LIMIT: usize = 64 << 10;

impl<A: PublicationAdapter> PublicationLifecycle<A> {
    /// Fetch the pointer, refuse replays, fence serving on negative evidence,
    /// then persist the raised floor. A 304 reuses the last received body and
    /// renews nothing.
    pub(super) async fn observe_pointer(&mut self) -> Result<A::Pointer, Error> {
        let token = self.last_received_pointer.as_ref().map(conditional_token);
        let mut response = self.source.request(CURRENT_PATH, token.as_deref()).await?;
        let pointer = if response.status() == reqwest::StatusCode::NOT_MODIFIED {
            response.discard().await?;
            self.last_received_pointer
                .clone()
                .ok_or(PublicationLifecycleError::UnexpectedNotModified)?
        } else if response.status().is_success() {
            serde_json::from_slice::<A::Pointer>(&response.read_limited(POINTER_LIMIT).await?)?
        } else {
            let status = response.status().as_u16();
            response.discard().await?;
            return Err(Box::new(PublicationLifecycleError::PointerUnavailable(
                status,
            )));
        };
        drop(response);
        self.adapter.validate_pointer(&pointer)?;
        let checked = self.floor.classify(pointer.clone())?;
        let fenced = if !pointer.eligible() || checked.succession().fences_earlier_views() {
            self.fence_serving().await
        } else {
            Vec::new()
        };
        self.floor.record(checked, fenced)?;
        // A dirty floor (a new binding, a change, or an earlier failed save) is
        // saved before this observation counts, even if the pointer is unchanged.
        if !self.floor.is_durable() {
            self.persist_floor().await?;
        }
        self.last_received_pointer = Some(pointer.clone());
        Ok(pointer)
    }
}
