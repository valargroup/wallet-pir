//! Serve only artifacts authorized by the captured publication.
use super::*;

/// A public request the authority cannot answer consistently right now. The
/// `retry-after` makes wallets retry it shortly instead of failing the sync.
pub(super) fn unavailable() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [("retry-after", "1"), ("cache-control", "no-store")],
    )
        .into_response()
}

/// Use the existing owner canonicality input, not prepared-file parsing. A
/// source outage/identity failure proves nothing; a same-network hash mismatch
/// is positive noncanonical evidence. Snapshot fencing brackets both RPC reads.
pub(super) async fn check_distribution_anchor(
    authority: &Authority,
    current: &transparent_shared::publication::Current,
    height: u64,
    hash: &str,
) -> Result<bool, BoxError> {
    let _permit = authority
        .0
        .distribution_slots
        .clone()
        .try_acquire_owned()
        .map_err(|_| "distribution canonicality admission full")?;
    let rpc = authority
        .0
        .canonicality
        .as_ref()
        .ok_or("canonicality source unavailable")?;
    if rpc.block_hash_display(0).await? != current.genesis_hash {
        return Err("canonicality source changed network".into());
    }
    let canonical = rpc.block_hash_display(height).await? == hash;
    // As in the existing canonical guard, a repointed/wrong-network node
    // cannot supply positive reorg evidence.
    if !canonical && rpc.block_hash_display(0).await? != current.genesis_hash {
        return Err("canonicality source changed network".into());
    }
    let _gate = authority.0.publication_gate.lock().unwrap();
    if authority.0.epoch.load(Ordering::Acquire) != current.recovery_epoch
        || authority.0.distribution_revision.load(Ordering::Acquire) != current.revision
    {
        return Err("canonicality snapshot superseded".into());
    }
    Ok(canonical)
}

/// [`check_distribution_anchor`] for a whole batch: one admission and genesis
/// check before and after every hash read, then one fence under the
/// publication gate against any epoch, revision, map, inventory or withdrawal
/// change. Any failure answers for no anchor.
pub(super) async fn check_distribution_anchors(
    authority: &Authority,
    current: &transparent_shared::publication::Current,
    map_sha256: &str,
    anchors: &[transparent_shared::publication::AnchorPair],
) -> Result<Vec<bool>, BoxError> {
    let _permit = authority
        .0
        .distribution_slots
        .clone()
        .try_acquire_owned()
        .map_err(|_| "distribution canonicality admission full")?;
    let rpc = authority
        .0
        .canonicality
        .as_ref()
        .ok_or("canonicality source unavailable")?;
    if rpc.block_hash_display(0).await? != current.genesis_hash {
        return Err("canonicality source changed network".into());
    }
    let mut canonical = Vec::with_capacity(anchors.len());
    for anchor in anchors {
        canonical.push(rpc.block_hash_display(anchor.height).await? == anchor.hash);
    }
    // A repointed/wrong-network node cannot supply positive reorg evidence.
    if rpc.block_hash_display(0).await? != current.genesis_hash {
        return Err("canonicality source changed network".into());
    }
    let _gate = authority.0.publication_gate.lock().unwrap();
    let active = authority.0.active.read().unwrap();
    if authority.0.epoch.load(Ordering::Acquire) != current.recovery_epoch
        || authority.0.distribution_revision.load(Ordering::Acquire) != current.revision
        || authority.0.withdrawn.load(Ordering::Acquire) != !current.eligible()
        || active.publication.map_sha256 != map_sha256
        || active.distribution.as_ref().map(|d| d.sha256())
            != Some(current.inventory_sha256.as_str())
    {
        return Err("canonicality snapshot superseded".into());
    }
    Ok(canonical)
}

pub(super) async fn anchor_batch(
    authority: &Authority,
    current: &transparent_shared::publication::Current,
    map_sha256: &str,
    request: Request,
) -> Response {
    use transparent_shared::publication::{self, AnchorBatch, AnchorBatchRequest, AnchorResult};
    let Ok(body) =
        axum::body::to_bytes(request.into_body(), publication::MAX_ANCHOR_BATCH_BYTES).await
    else {
        return StatusCode::PAYLOAD_TOO_LARGE.into_response();
    };
    let Ok(batch) = serde_json::from_slice::<AnchorBatchRequest>(&body) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    if batch.validate().is_err() {
        return StatusCode::BAD_REQUEST.into_response();
    }
    // Never answer for this snapshot what the consumer asked about another.
    if !batch.names(current) {
        return (StatusCode::CONFLICT, [("cache-control", "no-store")]).into_response();
    }
    let canonical =
        match check_distribution_anchors(authority, current, map_sha256, &batch.anchors).await {
            Ok(canonical) => canonical,
            Err(_) => return unavailable(),
        };
    (
        [("cache-control", "no-store")],
        axum::Json(AnchorBatch {
            schema: publication::SCHEMA.into(),
            revision: current.revision,
            recovery_epoch: current.recovery_epoch,
            inventory_sha256: current.inventory_sha256.clone(),
            genesis_hash: current.genesis_hash.clone(),
            results: batch
                .anchors
                .into_iter()
                .zip(canonical)
                .map(|(anchor, canonical)| AnchorResult {
                    height: anchor.height,
                    hash: anchor.hash,
                    canonical,
                })
                .collect(),
        }),
    )
        .into_response()
}

pub(super) async fn distribution_request(authority: Authority, request: Request) -> Response {
    use transparent_shared::publication::{self, Current, Eligibility};
    let batch = request.uri().path().strip_prefix(publication::PREFIX)
        == Some(publication::ANCHOR_BATCH_PATH);
    let method = if batch {
        axum::http::Method::POST
    } else {
        axum::http::Method::GET
    };
    if request.method() != method || request.uri().query().is_some() {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    // Capture current, withdrawal and epoch under the same gate that commits
    // the public pointer and withdraws orphaned coverage.
    let (active, mut current) = {
        let _gate = authority.0.publication_gate.lock().unwrap();
        let active = authority.0.active.read().unwrap().clone();
        let Some(finalized) = &active.distribution else {
            return unavailable();
        };
        let current = Current {
            schema: publication::SCHEMA.into(),
            revision: authority.0.distribution_revision.load(Ordering::Acquire),
            recovery_epoch: authority.0.epoch.load(Ordering::Acquire),
            inventory_sha256: finalized.sha256().to_string(),
            network: active.filters.map().network.clone(),
            genesis_hash: active.filters.map().genesis_hash.clone(),
            anchor_height: active.publication.height,
            anchor_hash: active.publication.hash.clone(),
            eligibility: if authority.0.withdrawn.load(Ordering::Acquire) {
                Eligibility::Withdrawn
            } else {
                Eligibility::Canonical
            },
            invalidated_from: match authority.0.invalidated_from.load(Ordering::Acquire) {
                u64::MAX => None,
                h => Some(h),
            },
        };
        (active, current)
    };
    let suffix = request
        .uri()
        .path()
        .strip_prefix(publication::PREFIX)
        .unwrap();
    if batch {
        let map_sha256 = active.publication.map_sha256.clone();
        return anchor_batch(&authority, &current, &map_sha256, request).await;
    }
    if let Some(anchor) = suffix.strip_prefix("anchors/") {
        let Some((height, hash)) = anchor.split_once('/') else {
            return StatusCode::NOT_FOUND.into_response();
        };
        let Ok(height) = height.parse::<u64>() else {
            return StatusCode::NOT_FOUND.into_response();
        };
        if !publication::is_digest(hash) {
            return StatusCode::NOT_FOUND.into_response();
        }
        let canonical = match check_distribution_anchor(&authority, &current, height, hash).await {
            Ok(canonical) => canonical,
            Err(_) => return unavailable(),
        };
        return (
            [("cache-control", "no-store")],
            axum::Json(publication::AnchorCheck {
                schema: publication::SCHEMA.into(),
                revision: current.revision,
                recovery_epoch: current.recovery_epoch,
                genesis_hash: current.genesis_hash,
                height,
                hash: hash.into(),
                canonical,
            }),
        )
            .into_response();
    }
    if suffix == "current" {
        if current.eligible() {
            match check_distribution_anchor(
                &authority,
                &current,
                current.anchor_height,
                &current.anchor_hash,
            )
            .await
            {
                Ok(true) => {}
                Ok(false) => current.eligibility = Eligibility::Noncanonical,
                Err(_) => return unavailable(),
            }
            // Never attest a candidate across withdrawal, recovery or a pointer
            // change that happened while the canonicality request was pending.
            let _gate = authority.0.publication_gate.lock().unwrap();
            if authority.0.epoch.load(Ordering::Acquire) != current.recovery_epoch
                || authority.0.distribution_revision.load(Ordering::Acquire) != current.revision
                || authority.0.active.read().unwrap().publication.map_sha256
                    != active.publication.map_sha256
            {
                return unavailable();
            }
            if authority.0.withdrawn.load(Ordering::Acquire) {
                current.eligibility = Eligibility::Withdrawn;
            }
        }
        let etag = format!(
            "\"{}\"",
            publication::digest(&serde_json::to_vec(&current).expect("serializable current"))
        );
        if request
            .headers()
            .get("if-none-match")
            .and_then(|h| h.to_str().ok())
            == Some(etag.as_str())
        {
            return (
                StatusCode::NOT_MODIFIED,
                [("cache-control", "no-store"), ("etag", etag.as_str())],
            )
                .into_response();
        }
        return (
            [("cache-control", "no-store"), ("etag", etag.as_str())],
            axum::Json(current),
        )
            .into_response();
    }
    if !current.eligible() {
        return unavailable();
    }
    let finalized = active.distribution.as_ref().unwrap();
    if suffix == format!("inventories/{}", finalized.sha256()) {
        return (
            [
                ("content-type", "application/json"),
                ("cache-control", "public, max-age=31536000, immutable"),
            ],
            finalized.bytes().to_vec(),
        )
            .into_response();
    }
    let Some(sha256) = suffix.strip_prefix("artifacts/") else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let file = match finalized.open_file(sha256) {
        Ok(Some(file)) => tokio::fs::File::from_std(file),
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(_) => return unavailable(),
    };
    // Immutable file handles survive ordinary pointer retirement; they never
    // authorize activation. The consumer rechecks current after verification.
    let permit = match authority.0.distribution_slots.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => return unavailable(),
    };
    (
        [
            ("content-type", "application/octet-stream"),
            ("cache-control", "public, max-age=31536000, immutable"),
        ],
        pir_control::http::retain(
            axum::body::Body::from_stream(tokio_util::io::ReaderStream::new(file)),
            (permit, active),
        ),
    )
        .into_response()
}
