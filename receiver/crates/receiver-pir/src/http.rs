//! Optional reqwest adapter. Configure the client with a timeout and no redirects.
use crate::{
    transport::{DirectoryClient, Transport},
    Error,
};

/// Convenience alias for applications using reqwest directly.
pub type HttpClient = DirectoryClient<reqwest::Client>;

impl Transport for reqwest::Client {
    async fn get(&self, url: &str, limit: usize) -> Result<Vec<u8>, Error> {
        read_bounded(self.get(url), "get", 0, limit).await
    }
    async fn post(&self, url: &str, body: Vec<u8>, limit: usize) -> Result<Vec<u8>, Error> {
        let sent = body.len();
        read_bounded(
            self.post(url)
                .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
                .body(body),
            "post",
            sent,
            limit,
        )
        .await
    }
}

async fn read_bounded(
    request: reqwest::RequestBuilder,
    kind: &'static str,
    sent: usize,
    limit: usize,
) -> Result<Vec<u8>, Error> {
    let started = std::time::Instant::now();
    let mut received = 0usize;
    let result = async {
        let mut response = request.send().await?;
        if matches!(
            response.status(),
            reqwest::StatusCode::CONFLICT | reqwest::StatusCode::GONE
        ) {
            return Err(Error::Revision);
        }
        response = response.error_for_status()?;
        if response.content_length().is_some_and(|n| n > limit as u64) {
            return Err(Error::Malformed);
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            if chunk.len() > limit - body.len() {
                return Err(Error::Malformed);
            }
            received += chunk.len();
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    }
    .await;
    log::info!(
        "pir_http component=receiver kind={} sent_bytes={} received_bytes={} elapsed_us={} ok={}",
        kind,
        sent,
        received,
        started.elapsed().as_micros(),
        result.is_ok()
    );
    result
}
