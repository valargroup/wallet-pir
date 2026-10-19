use crate::Error;
use std::time::Duration;

/// HTTPS requests to one explicit origin and distribution prefix. The byte
/// budget spans metadata, artifacts and refused bodies until `reset_budget`.
pub struct HttpsSource {
    origin: reqwest::Url,
    base: reqwest::Url,
    client: reqwest::Client,
    budget: u64,
    downloaded: u64,
}
impl HttpsSource {
    pub fn new(
        origin: &str,
        prefix: &str,
        budget: u64,
        roots: &[reqwest::Certificate],
    ) -> Result<Self, Error> {
        let origin = reqwest::Url::parse(origin)?;
        if origin.scheme() != "https"
            || origin.host_str().is_none()
            || !origin.username().is_empty()
            || origin.password().is_some()
            || origin.path() != "/"
            || origin.query().is_some()
            || origin.fragment().is_some()
        {
            return Err("publisher must be an explicit HTTPS origin without credentials, path, query or fragment".into());
        }
        // Prefixes are application constants; refuse URL syntax and normalized
        // traversal rather than letting a relative request escape its namespace.
        if !prefix.starts_with('/') || !prefix.ends_with('/') || !safe_path(&prefix[1..]) {
            return Err("distribution prefix must be an absolute path ending in a slash".into());
        }
        let base = origin.join(prefix)?;
        let mut builder = reqwest::Client::builder()
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(120));
        for root in roots {
            builder = builder.add_root_certificate(root.clone());
        }
        Ok(Self {
            origin,
            base,
            client: builder.build()?,
            budget,
            downloaded: 0,
        })
    }
    pub fn origin(&self) -> &reqwest::Url {
        &self.origin
    }
    pub fn downloaded(&self) -> u64 {
        self.downloaded
    }
    /// Start a new complete attempt. Outstanding response borrows prevent resetting.
    pub fn reset_budget(&mut self) {
        self.downloaded = 0;
    }
    /// Tokens are opaque to transport. The product decides when a conditional
    /// request and a 304 response are valid; no request is retried here.
    pub async fn request(
        &mut self,
        suffix: &str,
        conditional: Option<&str>,
    ) -> Result<Response<'_>, Error> {
        self.send(suffix, conditional, None).await
    }
    /// Send a product-encoded JSON request within the same origin and byte budget.
    /// The product bounds and validates the request and response schemas.
    pub async fn post_json(&mut self, suffix: &str, body: Vec<u8>) -> Result<Response<'_>, Error> {
        self.send(suffix, None, Some(body)).await
    }
    async fn send(
        &mut self,
        suffix: &str,
        conditional: Option<&str>,
        body: Option<Vec<u8>>,
    ) -> Result<Response<'_>, Error> {
        if self.downloaded >= self.budget {
            return Err(budget_error(self.downloaded, self.budget));
        }
        if !safe_path(suffix) {
            return Err("distribution request must stay inside its prefix".into());
        }
        let url = self.base.join(suffix)?;
        if url.origin() != self.origin.origin() || !url.path().starts_with(self.base.path()) {
            return Err("distribution request escapes its origin or prefix".into());
        }
        let mut request = match body {
            Some(body) => self
                .client
                .post(url)
                .header("content-type", "application/json")
                .body(body),
            None => self.client.get(url),
        }
        .header("cache-control", "no-cache");
        if let Some(token) = conditional {
            request = request.header("if-none-match", token);
        }
        let response = request.send().await?;
        Ok(Response {
            response,
            downloaded: &mut self.downloaded,
            budget: self.budget,
        })
    }
}
fn safe_path(path: &str) -> bool {
    !path.starts_with('/')
        && !path.contains(['\\', '?', '#', '%', ':'])
        && path.split('/').all(|part| part != "." && part != "..")
}
fn budget_error(downloaded: u64, budget: u64) -> Error {
    format!("prepared publication exceeds download budget ({downloaded} body bytes consumed, cap {budget})").into()
}

/// A response whose only body access accounts bytes against its source budget.
/// Holding it also prevents another request or a budget reset on that source.
pub struct Response<'a> {
    response: reqwest::Response,
    downloaded: &'a mut u64,
    budget: u64,
}
impl Response<'_> {
    pub fn status(&self) -> reqwest::StatusCode {
        self.response.status()
    }
    pub async fn chunk(&mut self) -> Result<Option<bytes::Bytes>, Error> {
        let chunk = self.response.chunk().await?;
        if let Some(bytes) = &chunk {
            *self.downloaded = self
                .downloaded
                .checked_add(bytes.len() as u64)
                .ok_or("download budget overflow")?;
            if *self.downloaded > self.budget {
                return Err(budget_error(*self.downloaded, self.budget));
            }
        }
        Ok(chunk)
    }
    pub async fn read_limited(&mut self, limit: usize) -> Result<Vec<u8>, Error> {
        let mut bytes = Vec::new();
        while let Some(chunk) = self.chunk().await? {
            if chunk.len() > limit.saturating_sub(bytes.len()) {
                return Err("distribution metadata exceeds limit".into());
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(bytes)
    }
    /// Refused responses consume the same budget as successful responses.
    pub async fn discard(&mut self) -> Result<(), Error> {
        while self.chunk().await?.is_some() {}
        Ok(())
    }
}
