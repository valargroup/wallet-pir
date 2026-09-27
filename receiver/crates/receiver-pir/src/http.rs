//! Bounded HTTP transport. The caller chooses a proxy and timeout on its HTTP client.
use crate::{check_next, public_bytes, response_bytes, AcceptedCoverage, Client, Error, Manifest};
use receiver_directory::{Payment, Receiver};
use std::{collections::BTreeSet, num::NonZeroU32};

pub struct HttpClient {
    origin: String,
    http: reqwest::Client,
    session: Client,
}

impl HttpClient {
    /// Initialize against an independently accepted terminal anchor. Reconnect for a new revision.
    pub async fn connect(
        origin: &str,
        http: reqwest::Client,
        accepted: AcceptedCoverage,
    ) -> Result<Self, Error> {
        let origin = origin.trim_end_matches('/').to_owned();
        let manifest = Self::fetch_manifest(&origin, &http).await?;
        accepted.check(&manifest.directory)?;
        let public = read_bounded(
            http.get(format!("{origin}/v1/receiver/public"))
                .send()
                .await?,
            public_bytes(),
        )
        .await?;
        let session = Client::new(manifest, &public, accepted)?;
        Ok(Self {
            origin,
            http,
            session,
        })
    }

    /// Fetch bounded public metadata before selecting the corresponding local chain anchor.
    pub async fn fetch_manifest(origin: &str, http: &reqwest::Client) -> Result<Manifest, Error> {
        let bytes = read_bounded(
            http.get(format!("{}/v1/receiver/init", origin.trim_end_matches('/')))
                .send()
                .await?,
            16384,
        )
        .await?;
        let manifest: Manifest = serde_json::from_slice(&bytes)?;
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn manifest(&self) -> &Manifest {
        self.session.manifest()
    }

    /// Download identical proof bytes for all wallets. This never contains a receiver in the request.
    pub async fn witnesses(&self) -> Result<receiver_directory::witness::WitnessSnapshot, Error> {
        let bytes = read_bounded(
            self.http
                .get(format!("{}/v1/receiver/witness", self.origin))
                .send()
                .await?,
            receiver_directory::witness::MAX_WITNESS_BYTES,
        )
        .await?;
        Ok(receiver_directory::witness::WitnessSnapshot::decode(
            &bytes,
            &self.manifest().directory,
        )?)
    }

    /// Return every payment from one publication, or an error. No partial success or cleartext fallback.
    /// Revalidate the anchor before crediting results if the wallet's chain changes while awaiting I/O.
    pub async fn lookup(
        &self,
        receiver: Receiver,
        max_pages: NonZeroU32,
        accepted: AcceptedCoverage,
    ) -> Result<Vec<Payment>, Error> {
        accepted.check(&self.manifest().directory)?;
        let mut records = Vec::new();
        let mut outputs = BTreeSet::new();
        for page in 0..max_pages.get() {
            // Each attempt, including caller retries, uses fresh encryption.
            let query = self.session.prepare(receiver, page)?;
            let response = self
                .http
                .post(format!("{}/v1/receiver/query", self.origin))
                .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
                .body(query.body().to_vec())
                .send()
                .await?;
            let body = read_bounded(response, response_bytes()).await?;
            let Some(record) = self.session.decode(query, &body)? else {
                // The row decoder permits absence only on page zero.
                return Ok(Vec::new());
            };
            if record.total > max_pages.get() {
                return Err(Error::PageBudget);
            }
            if let Some(previous) = records.last() {
                check_next(previous, &record)?;
            }
            if !outputs.insert((record.payment.txid, record.payment.action_index)) {
                return Err(Error::Malformed);
            }
            let complete = record.page + 1 == record.total;
            records.push(record);
            if complete {
                return Ok(records.into_iter().map(|r| r.payment).collect());
            }
        }
        Err(Error::PageBudget)
    }
}

async fn read_bounded(mut response: reqwest::Response, limit: usize) -> Result<Vec<u8>, Error> {
    if response.status() == reqwest::StatusCode::CONFLICT {
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
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}
