//! One trusted terminal archive. No wallet destination, redirect, retry or fallback.
use crate::{
    request::{json, RequestError},
    runtime::SharedParams,
    service::ServiceState,
    shardset::TableRef,
};
use axum::{
    extract::Request,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{sync::Arc, time::Duration};
use transparent_shard::manifest::query_binding;

pub struct Archive {
    origin: reqwest::Url,
    client: reqwest::Client,
    observation: std::sync::RwLock<Value>,
}
struct Shape {
    shared: SharedParams,
    segments: usize,
    rows: usize,
}
struct Setup {
    bytes: Vec<u8>,
    epoch: [u8; 8],
}
// A completed reply still owns admission until its transport releases it.
// Completing on drop avoids counting a successfully produced reply as cancelled.
struct ReplyAdmission(Option<transparent_server_backend::admission::Pending>);
impl Drop for ReplyAdmission {
    fn drop(&mut self) {
        if let Some(pending) = self.0.take() {
            pending.complete();
        }
    }
}
enum Failure {
    Unavailable(String),
    Capacity {
        retry: axum::http::HeaderValue,
        body: Vec<u8>,
    },
}
impl From<&str> for Failure {
    fn from(message: &str) -> Self {
        Self::Unavailable(message.into())
    }
}
impl From<String> for Failure {
    fn from(message: String) -> Self {
        Self::Unavailable(message)
    }
}
impl From<Failure> for String {
    fn from(failure: Failure) -> Self {
        match failure {
            Failure::Unavailable(message) => message,
            Failure::Capacity { .. } => "archive capacity refused qualification".into(),
        }
    }
}
impl Failure {
    fn into_response(self) -> Response {
        match self {
            Self::Unavailable(_) => Archive::unavailable(),
            Self::Capacity { retry, body } => {
                let mut response = (
                    StatusCode::SERVICE_UNAVAILABLE,
                    [("content-type", "application/json")],
                    body,
                )
                    .into_response();
                response.headers_mut().insert("retry-after", retry);
                response
            }
        }
    }
}
impl Archive {
    /// HTTPS in deployment; plain HTTP is restricted to isolated loopback fixtures.
    pub fn new(origin: &str, roots: Vec<reqwest::Certificate>) -> Result<Arc<Self>, String> {
        let origin = reqwest::Url::parse(origin).map_err(|e| e.to_string())?;
        let loopback = origin.host_str().is_some_and(|host| {
            host.parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
        });
        if (origin.scheme() != "https" && !(origin.scheme() == "http" && loopback))
            || !origin.username().is_empty()
            || origin.password().is_some()
            || origin.path() != "/"
            || origin.query().is_some()
            || origin.fragment().is_some()
        {
            return Err("archive must be a fixed HTTPS origin (HTTP only on loopback)".into());
        }
        let mut client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30));
        for root in roots {
            client = client.add_root_certificate(root);
        }
        Ok(Arc::new(Self {
            origin,
            observation: std::sync::RwLock::new(serde_json::json!({"state":"unobserved"})),
            client: client.build().map_err(|e| e.to_string())?,
        }))
    }
    fn shape(
        state: &ServiceState,
        id: u64,
        digest: &str,
        table: TableRef,
    ) -> Result<Shape, String> {
        let (manifest, _, _) = state
            .set()
            .meta_by_digest(digest)
            .ok_or("archive revision no longer served")?;
        if manifest.shard_id != id {
            return Err("revision belongs to another shard".into());
        }
        let geometry = state
            .set()
            .geometries()
            .into_iter()
            .find(|g| g.name == manifest.geometry)
            .ok_or("unknown geometry")?;
        let shared = SharedParams::build(geometry, table.kind())?;
        let (segments, rows) = match table {
            TableRef::Directory => (manifest.directory_segments.len(), shared.profile.rows),
            TableRef::Pages(child) => {
                let child = manifest
                    .page_children
                    .get(child as usize)
                    .ok_or("unknown page child")?;
                (child.page_segments.len(), child.query_rows() as usize)
            }
        };
        if segments == 0 {
            return Err("table capability unavailable".into());
        }
        Ok(Shape {
            shared,
            segments,
            rows,
        })
    }
    async fn bytes(mut response: reqwest::Response, limit: usize) -> Result<Vec<u8>, Failure> {
        if response.content_length().is_some_and(|n| n > limit as u64) {
            return Err("archive response too large".into());
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| "archive body interrupted")?
        {
            if chunk.len() > limit.saturating_sub(bytes.len()) {
                return Err("archive response too large".into());
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(bytes)
    }
    async fn get(&self, path: &str, limit: usize) -> Result<Vec<u8>, Failure> {
        let response = self
            .client
            .get(self.origin.join(path).map_err(|e| e.to_string())?)
            .header("x-transparent-archive-hop", "1")
            .send()
            .await
            .map_err(|_| "archive unavailable")?;
        if response.status() != StatusCode::OK {
            if response.status() == StatusCode::SERVICE_UNAVAILABLE {
                if let Some(retry) = response.headers().get("retry-after").cloned() {
                    let body = Self::bytes(response, 4096).await?;
                    return Err(Failure::Capacity { retry, body });
                }
            }
            return Err("archive refused qualification".into());
        }
        Self::bytes(response, limit).await
    }
    async fn ready(&self) -> Result<Value, Failure> {
        let ready: Value = serde_json::from_slice(&self.get("/v1/ready", 65536).await?)
            .map_err(|_| "invalid archive readiness")?;
        // A configured provider is never a terminal archive, even if its address
        // happens to be reachable or some of its metadata matches.
        if ready["ready"] != true || ready["mode"] != "warm" || ready["archive_forwarding"] != false
        {
            return Err("archive is unready or not a terminal owner".into());
        }
        if !ready["map_sha256"].as_str().is_some_and(|s| {
            s.len() == 64
                && s.bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        }) || !ready["incarnation"].as_str().is_some_and(|s| !s.is_empty())
        {
            return Err("archive readiness omits publication/process identity".into());
        }
        Ok(ready)
    }
    pub(crate) fn observation(&self) -> Value {
        self.observation.read().unwrap().clone()
    }
    async fn check(&self, state: &ServiceState, id: u64, digest: &str) -> Result<Value, Failure> {
        let result = self.check_identity(state, id, digest).await;
        *self.observation.write().unwrap() = serde_json::json!({
            "state":if result.is_ok() {"identity-checked"} else {"refused"},
            "checked_unix":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_secs()),
            "shard_id":id,"manifest_digest":digest,
            "terminal_identity":result.as_ref().ok(),
        });
        result
    }
    async fn check_identity(
        &self,
        state: &ServiceState,
        id: u64,
        digest: &str,
    ) -> Result<Value, Failure> {
        let ready = self.ready().await?;
        let init: Value = serde_json::from_slice(&self.get("/v2/shards/init", 1 << 20).await?)
            .map_err(|_| "invalid archive initialization")?;
        if init["schema"] != transparent_shard::manifest::SCHEMA
            || init["network"] != state.set().map.network
            || init["genesis_hash"] != state.set().map.genesis_hash
            || init["profile"] != state.set().map.profile
            || init["map_sha256"] != ready["map_sha256"]
        {
            return Err("archive identity/profile mismatch".into());
        }
        let (manifest, canonical, _) = state
            .set()
            .meta_by_digest(digest)
            .ok_or("unknown archive revision")?;
        if manifest.shard_id != id {
            return Err("revision belongs to another shard".into());
        }
        let geometry = state
            .set()
            .geometries()
            .into_iter()
            .find(|g| g.name == manifest.geometry)
            .ok_or("unknown archive geometry")?;
        let entry = init["geometries"]
            .as_array()
            .and_then(|entries| entries.iter().find(|e| e["name"] == geometry.name))
            .ok_or("archive omits required geometry")?;
        if entry["directory_rows"] != geometry.directory_rows
            || entry["directory_row_bytes"] != geometry.directory_row_bytes
            || entry["page_rows"] != geometry.page_rows
            || entry["page_row_bytes"] != geometry.page_row_bytes
        {
            return Err("archive geometry mismatch".into());
        }
        for (table, prefix) in [
            (crate::shardset::Table::Directory, "directory"),
            (crate::shardset::Table::Pages, "pages"),
        ] {
            let shared = SharedParams::build(geometry, table)?;
            if entry[format!("{prefix}_setup_seed")] != shared.setup_seed
                || entry[format!("{prefix}_scheme")]
                    != serde_json::to_value(shared.scheme()).map_err(|e| e.to_string())?
                || entry[format!("{prefix}_scheme_dq44")]
                    != serde_json::to_value(shared.dithered_scheme()).map_err(|e| e.to_string())?
            {
                return Err("archive native profile mismatch".into());
            }
        }
        let remote = self
            .get(
                &format!("/v2/shards/{id}/revisions/{digest}/manifest"),
                canonical.len(),
            )
            .await?;
        if remote != canonical {
            return Err("archive manifest mismatch".into());
        }
        Ok(ready)
    }
    async fn unchanged(&self, ready: &Value) -> Result<(), Failure> {
        let after = self.ready().await?;
        if after["map_sha256"] != ready["map_sha256"]
            || after["incarnation"] != ready["incarnation"]
            || after["assignment_sha256"] != ready["assignment_sha256"]
        {
            return Err("archive changed during qualification".into());
        }
        Ok(())
    }
    async fn segment(
        &self,
        state: &ServiceState,
        id: u64,
        digest: &str,
        table: TableRef,
        segment: usize,
        shape: &Shape,
    ) -> Result<Setup, Failure> {
        if segment >= shape.segments {
            return Err("unknown segment".into());
        }
        let bytes = self
            .get(
                &format!(
                    "/v2/shards/{id}/revisions/{digest}/setup/{}/{segment}",
                    table.name()
                ),
                1 << 20,
            )
            .await?;
        let setup: Value = serde_json::from_slice(&bytes).map_err(|_| "invalid archive setup")?;
        let geometry = state
            .set()
            .meta_by_digest(digest)
            .ok_or("unknown revision")?
            .0
            .geometry
            .as_str();
        if setup["shard_id"] != id
            || setup["manifest_digest"] != digest
            || setup["geometry"] != geometry
            || setup["table"] != table.name()
            || setup["segment"] != segment
            || setup["segments"] != shape.segments
        {
            return Err("archive setup binding mismatch".into());
        }
        let params = STANDARD
            .decode(
                setup["public_params"]
                    .as_str()
                    .ok_or("missing archive masks")?,
            )
            .map_err(|_| "invalid archive masks")?;
        if params.len() != shape.shared.scheme().public_bytes {
            return Err("archive masks have the wrong length".into());
        }
        let hash = Sha256::digest(&params);
        if setup["public_params_sha256"] != hex::encode(hash)
            || setup["public_params_epoch"] != hex::encode(&hash[..8])
        {
            return Err("archive setup digest mismatch".into());
        }
        let epoch = hash[..8].try_into().unwrap();
        Ok(Setup { bytes, epoch })
    }
    pub(crate) async fn qualify(
        &self,
        state: &ServiceState,
        id: u64,
        digest: &str,
    ) -> Result<(), String> {
        let ready = self.check(state, id, digest).await?;
        let manifest = state
            .set()
            .meta_by_digest(digest)
            .ok_or("unknown revision")?
            .0;
        for table in std::iter::once(TableRef::Directory)
            .chain((0..manifest.page_children.len() as u32).map(TableRef::Pages))
        {
            if let TableRef::Pages(child) = table {
                if manifest.page_children[child as usize]
                    .page_segments
                    .is_empty()
                {
                    continue;
                }
            }
            let shape = Self::shape(state, id, digest, table)?;
            for segment in 0..shape.segments {
                self.segment(state, id, digest, table, segment, &shape)
                    .await?;
            }
        }
        self.unchanged(&ready).await.map_err(Into::into)
    }
    fn unavailable() -> Response {
        // The frozen wallet retries 502/504 and 503 with Retry-After. A lost
        // answer may follow acceptance: this is the existing non-capacity 503,
        // with NO delay, so it cannot trigger an uncertain encrypted replay.
        json(
            StatusCode::SERVICE_UNAVAILABLE,
            serde_json::json!({"error":"fixed archive unavailable or incompatible"}),
        )
    }
    pub(crate) async fn setup(
        &self,
        state: &ServiceState,
        id: u64,
        digest: &str,
        table: TableRef,
        segment: u32,
    ) -> Response {
        let worker = state.worker();
        let shape = match Self::shape(state, id, digest, table) {
            Ok(shape) if (segment as usize) < shape.segments => shape,
            Ok(_) => {
                return RequestError::Bad("unknown segment".into()).into_response(worker.map_digest)
            }
            Err(error) => return RequestError::Bad(error).into_response(worker.map_digest),
        };
        let pending = match state.forwarding_admission().try_enter(None) {
            Ok(p) => p,
            Err(e) => return RequestError::from(e).into_response(worker.map_digest),
        };
        let result = tokio::time::timeout(pending.remaining(), async {
            let ready = self.check(state, id, digest).await?;
            let setup = self
                .segment(state, id, digest, table, segment as usize, &shape)
                .await?;
            self.unchanged(&ready).await?;
            Ok::<_, Failure>(setup.bytes)
        })
        .await;
        match result {
            Ok(Ok(bytes)) => (
                StatusCode::OK,
                [("content-type", "application/json")],
                pir_control::http::retain(
                    axum::body::Body::from(bytes),
                    (ReplyAdmission(Some(pending)), state.clone()),
                ),
            )
                .into_response(),
            Ok(Err(failure)) => failure.into_response(),
            Err(_) => Self::unavailable(),
        }
    }
    pub(crate) async fn query(
        &self,
        state: &ServiceState,
        id: u64,
        digest: &str,
        table: TableRef,
        request: Request,
    ) -> Response {
        let worker = state.worker();
        let shape = match Self::shape(state, id, digest, table) {
            Ok(s) => s,
            Err(e) => {
                return worker
                    .refuse_unread(
                        request,
                        RequestError::Bad(e).into_response(worker.map_digest),
                    )
                    .await
            }
        };
        let declared = request
            .headers()
            .get("content-length")
            .and_then(|s| s.to_str().ok())
            .and_then(|s| s.parse::<usize>().ok());
        let Some(expected) = declared else {
            return RequestError::LengthRequired.into_response(worker.map_digest);
        };
        if !shape.shared.accepts_query_bytes(shape.rows, expected) {
            return worker
                .refuse_unread(
                    request,
                    RequestError::Bad("query has the wrong fixed length".into())
                        .into_response(worker.map_digest),
                )
                .await;
        }
        let pending = match state.forwarding_admission().try_enter(Some(expected)) {
            Ok(p) => p,
            Err(e) => {
                return worker
                    .refuse_unread(
                        request,
                        RequestError::from(e).into_response(worker.map_digest),
                    )
                    .await
            }
        };
        let upload = worker
            .admission
            .config()
            .upload_deadline
            .min(pending.remaining());
        let body =
            match pir_control::admission::read_body(request.into_body(), expected, upload).await {
                Ok(body) if body.len() == expected => body,
                Err(pir_control::admission::BodyError::Timeout) => {
                    return RequestError::UploadTimeout.into_response(worker.map_digest)
                }
                _ => {
                    return RequestError::Bad("invalid query upload".into())
                        .into_response(worker.map_digest)
                }
            };
        let binding = query_binding(digest, &table.name());
        if body.get(..8) != Some(binding.as_slice()) {
            return RequestError::Bad("query binding mismatch".into())
                .into_response(worker.map_digest);
        }
        let response_bytes = match shape
            .shared
            .response_bytes()
            .checked_mul(shape.segments)
            .and_then(|n| u64::try_from(n).ok())
            .and_then(|n| n.checked_mul(2))
        {
            Some(n) => n,
            None => return Self::unavailable(),
        };
        let memory = match worker
            .work_memory
            .reserve_query(response_bytes, pending.remaining())
            .await
        {
            Some(memory) => memory,
            None => return RequestError::Overloaded.into_response(worker.map_digest),
        };
        let result = tokio::time::timeout(pending.remaining(), async {
            let ready = self.check(state, id, digest).await?;
            let mut epochs = Vec::new();
            for segment in 0..shape.segments {
                epochs.push(
                    self.segment(state, id, digest, table, segment, &shape)
                        .await?
                        .epoch,
                );
            }
            self.unchanged(&ready).await?;
            // Exactly one send. A lost response may follow acceptance; never retry.
            let response = self
                .client
                .post(
                    self.origin
                        .join(&format!(
                            "/v2/shards/{id}/revisions/{digest}/query/{}",
                            table.name()
                        ))
                        .map_err(|e| e.to_string())?,
                )
                .header("x-transparent-archive-hop", "1")
                .header("content-type", "application/octet-stream")
                .body(body)
                .send()
                .await
                .map_err(|_| "archive query failed")?;
            let status = response.status();
            let retry = response.headers().get("retry-after").cloned();
            if status != StatusCode::OK {
                let bytes = Self::bytes(response, 4096).await?;
                if !matches!(status.as_u16(), 400 | 408 | 409 | 411 | 421 | 503) {
                    return Err("unexpected archive refusal".into());
                }
                let mut response =
                    (status, [("content-type", "application/json")], bytes).into_response();
                if let Some(retry) = retry {
                    response.headers_mut().insert("retry-after", retry);
                }
                return Ok(response);
            }
            let width = shape.shared.response_bytes();
            let bytes = Self::bytes(
                response,
                width
                    .checked_mul(shape.segments)
                    .ok_or("response size overflow")?,
            )
            .await?;
            if bytes.len() != width * shape.segments {
                return Err("archive response framing mismatch".into());
            }
            for (segment, epoch) in bytes.chunks_exact(width).zip(epochs) {
                if segment[..8] != binding || segment[8..16] != epoch {
                    return Err("archive response binding mismatch".into());
                }
            }
            Ok::<_, Failure>(
                (
                    StatusCode::OK,
                    [("content-type", "application/octet-stream")],
                    bytes,
                )
                    .into_response(),
            )
        })
        .await;
        match result {
            Ok(Ok(mut response)) => {
                let body = std::mem::replace(response.body_mut(), axum::body::Body::empty());
                *response.body_mut() = pir_control::http::retain(
                    body,
                    (ReplyAdmission(Some(pending)), memory, state.clone()),
                );
                response
            }
            Ok(Err(failure)) => failure.into_response(),
            Err(_) => Self::unavailable(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Archive;
    #[test]
    fn archive_origin_is_trusted_configuration_only() {
        for origin in [
            "http://archive.example",
            "https://user@archive.example",
            "https://archive.example/path",
            "https://archive.example/?next=wallet",
            "https://archive.example/#fragment",
        ] {
            assert!(Archive::new(origin, vec![]).is_err(), "{origin}");
        }
        assert!(Archive::new("https://archive.example", vec![]).is_ok());
        assert!(Archive::new("http://127.0.0.1:1", vec![]).is_ok());
    }
}
