//! HTTP session handling for the standalone Status diagnostic commands.
use status_material::protocol::*;

#[derive(Debug, thiserror::Error)]
pub enum HttpError {
    #[error(transparent)]
    Transport(#[from] reqwest::Error),
    #[error("Status endpoint returned HTTP {0}")]
    Status(u16),
    #[error(transparent)]
    Protocol(#[from] Error),
}

/// A wallet-owned session. The anchor callback must consult independently
/// accepted chain state on every initialization, including a conflict retry.
pub struct HttpClient {
    origin: String,
    http: reqwest::Client,
    session: Option<Client>,
}

impl HttpClient {
    pub fn new(origin: impl Into<String>, http: reqwest::Client) -> Self {
        Self {
            origin: origin.into().trim_end_matches('/').to_owned(),
            http,
            session: None,
        }
    }

    async fn initialize<F, A>(&mut self, accepted_anchor: &mut F) -> Result<(), HttpError>
    where
        F: FnMut(&Manifest) -> Result<A, Error>,
        A: AnchorVerifier,
    {
        let response = self
            .http
            .get(format!("{}/v1/status/init", self.origin))
            .send()
            .await?;
        let response = successful(response)?;
        let manifest: Manifest = response.json().await?;
        manifest.fresh(current_ms())?;
        let accepted = accepted_anchor(&manifest)?;
        let response = self
            .http
            .get(format!(
                "{}/v1/status/session/{}",
                self.origin,
                hex::encode(manifest.id())
            ))
            .send()
            .await?;
        let response = successful(response)?;
        let public_len = expected_public_len()?;
        let public = read_bounded(response, public_len).await?;
        self.session = Some(Client::new(manifest, &public, &accepted)?);
        Ok(())
    }

    /// One bounded retry for an evicted or revoked session. A retry always
    /// reinitializes and encrypts a new query; it never replays the old body.
    pub async fn lookup<F, A>(
        &mut self,
        txid: Hash,
        earliest: Option<u32>,
        mut accepted_anchor: F,
    ) -> Result<Observation, HttpError>
    where
        F: FnMut(&Manifest) -> Result<A, Error>,
        A: AnchorVerifier,
    {
        for attempt in 0..2 {
            if let Some(session) = &self.session {
                let accepted = accepted_anchor(&session.manifest)?;
                if !accepted.accepts_manifest(&session.manifest) {
                    self.session = None;
                }
            }
            if self
                .session
                .as_ref()
                .is_some_and(|s| s.manifest.fresh(current_ms()).is_err())
            {
                self.session = None;
            }
            let result = async {
                if self.session.is_none() {
                    self.initialize(&mut accepted_anchor).await?;
                }
                let session = self.session.as_ref().expect("initialized session");
                let query = session.prepare(&txid, earliest, current_ms())?;
                let response = self
                    .http
                    .post(format!("{}/v1/status/query", self.origin))
                    .body(query.body.clone())
                    .send()
                    .await?;
                let response = successful(response)?;
                let bytes = read_bounded(response, expected_response_len()?).await?;
                let result = session.decode(query, &bytes, current_ms())?;
                let accepted = accepted_anchor(&session.manifest)?;
                if !accepted.accepts_manifest(&session.manifest) {
                    return Err(HttpError::Protocol(Error::Malformed));
                }
                Ok::<_, HttpError>(result)
            }
            .await;
            if matches!(result, Err(HttpError::Status(409 | 410))) && attempt == 0 {
                self.session = None;
                continue;
            }
            return result;
        }
        unreachable!("bounded retry returns on second attempt")
    }
}

fn successful(response: reqwest::Response) -> Result<reqwest::Response, HttpError> {
    if response.status().is_success() {
        Ok(response)
    } else {
        Err(HttpError::Status(response.status().as_u16()))
    }
}

async fn read_bounded(mut response: reqwest::Response, max: usize) -> Result<Vec<u8>, HttpError> {
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if chunk.len() > max.saturating_sub(bytes.len()) {
            return Err(HttpError::Protocol(Error::Malformed));
        }
        bytes.extend_from_slice(&chunk);
    }
    if bytes.len() != max {
        return Err(HttpError::Protocol(Error::Malformed));
    }
    Ok(bytes)
}

fn current_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
