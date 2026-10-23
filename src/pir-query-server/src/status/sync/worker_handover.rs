//! A router's loopback coordination with its worker: which inventory the
//! worker serves, pinning it during the authority check, and releasing the pin.
use super::super::{serving, Service};
use crate::synchronization::publication_lifecycle::{PeerHandover, PeerReservation};
use pir_publication::ObjectReference;
use pir_sync_manager::Error;
use status_query_server::distributed::ProviderStatus;
use status_shared::{now_ms, protocol::Manifest, publication::METADATA_LIMIT};

/// The inventory the worker serves, or `None` while it is still loading. A
/// loading worker is ordinary coordination, not a transport failure.
pub(super) async fn serving_inventory(service: &Service) -> Result<Option<ObjectReference>, Error> {
    let response = service
        .serving_http
        .get(format!("{}/v1/provider-status", service.worker_origin))
        .send()
        .await?;
    let bytes = serving::bounded(response, METADATA_LIMIT)
        .await
        .map_err(|_| "Status worker status unavailable")?;
    let status: ProviderStatus = serde_json::from_slice(&bytes)?;
    if status
        .manifest
        .as_ref()
        .is_none_or(|m| m.fresh(now_ms()).is_err())
    {
        return Ok(None);
    }
    Ok(status.inventory)
}

/// Ask the worker to keep `manifest` until the router commits it.
pub(super) async fn pin(service: &Service, manifest: &Manifest) -> Result<PeerReservation, Error> {
    let response = service
        .serving_http
        .post(format!(
            "{}/v1/provider-pin/{}",
            service.worker_origin,
            hex::encode(manifest.id())
        ))
        .send()
        .await?;
    if response.status() == reqwest::StatusCode::CONFLICT {
        return Ok(PeerReservation::PeerAdvanced);
    }
    let bytes = serving::bounded(response, 16)
        .await
        .map_err(|_| "Status worker cannot evaluate candidate generation")?;
    if !serde_json::from_slice::<bool>(&bytes)? {
        return Err("Status worker generation not ready".into());
    }
    Ok(PeerReservation::Reserved)
}

/// Tell the worker the router committed `manifest`. A failed acknowledgment
/// leaves the worker's pin until a later pass releases it.
pub(super) async fn release(service: &Service, manifest: &Manifest) -> PeerHandover {
    let release = service
        .serving_http
        .post(format!(
            "{}/v1/provider-commit/{}",
            service.worker_origin,
            hex::encode(manifest.id())
        ))
        .send()
        .await;
    match release {
        Ok(response) if response.status().is_success() => PeerHandover::Complete,
        _ => {
            tracing::warn!("Status router handover acknowledgment deferred");
            PeerHandover::Pending
        }
    }
}
