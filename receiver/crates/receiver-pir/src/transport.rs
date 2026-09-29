//! Receiver discovery over a caller-supplied, bounded transport.
use crate::{check_next, public_bytes, response_bytes, AcceptedCoverage, Client, Error, Manifest};
use receiver_directory::{Payment, Receiver};
use std::{collections::BTreeSet, num::NonZeroU32};

/// Hosts supply their route policy, cancellation, and timeout for every request.
/// Enforce `limit` while streaming, reject non-success responses, and map HTTP
/// 409/410 to [`Error::Revision`]. Do not follow redirects or retry in cleartext.
#[allow(async_fn_in_trait)]
pub trait Transport {
    async fn get(&self, url: &str, limit: usize) -> Result<Vec<u8>, Error>;
    async fn post(&self, url: &str, body: Vec<u8>, limit: usize) -> Result<Vec<u8>, Error>;
}

impl<T: Transport> Transport for &T {
    async fn get(&self, url: &str, limit: usize) -> Result<Vec<u8>, Error> {
        T::get(self, url, limit).await
    }
    async fn post(&self, url: &str, body: Vec<u8>, limit: usize) -> Result<Vec<u8>, Error> {
        T::post(self, url, body, limit).await
    }
}

/// Validated receiver session, independent of the wallet's networking stack.
pub struct DirectoryClient<T> {
    origin: String,
    http: T,
    session: Client,
}

impl<T: Transport> DirectoryClient<T> {
    /// Initialize against an independently accepted terminal anchor. Reconnect for a new revision.
    pub async fn connect(origin: &str, http: T, accepted: AcceptedCoverage) -> Result<Self, Error> {
        let origin = origin.trim_end_matches('/').to_owned();
        let manifest = Self::fetch_manifest(&origin, &http).await?;
        accepted.check(&manifest.directory)?;
        let public = http
            .get(
                &format!(
                    "{origin}/v1/receiver/public/{}",
                    hex::encode(manifest.id()?)
                ),
                public_bytes(),
            )
            .await?;
        if public.len() > public_bytes() {
            return Err(Error::Malformed);
        }
        let session = Client::new(manifest, &public, accepted)?;
        Ok(Self {
            origin,
            http,
            session,
        })
    }

    /// Fetch bounded public metadata before selecting the corresponding local chain anchor.
    pub async fn fetch_manifest(origin: &str, http: &T) -> Result<Manifest, Error> {
        let bytes = http
            .get(
                &format!("{}/v1/receiver/init", origin.trim_end_matches('/')),
                16384,
            )
            .await?;
        if bytes.len() > 16384 {
            return Err(Error::Malformed);
        }
        let manifest: Manifest = serde_json::from_slice(&bytes)?;
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn manifest(&self) -> &Manifest {
        self.session.manifest()
    }

    /// Download identical proof bytes for all wallets. This never contains a receiver in the request.
    pub async fn witnesses(&self) -> Result<receiver_directory::witness::WitnessSnapshot, Error> {
        let bytes = self
            .http
            .get(
                &format!(
                    "{}/v1/receiver/witness/{}",
                    self.origin,
                    hex::encode(self.manifest().id()?)
                ),
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
            let started = std::time::Instant::now();
            let body = self
                .http
                .post(
                    &format!("{}/v1/receiver/query", self.origin),
                    query.body().to_vec(),
                    response_bytes(),
                )
                .await?;
            if body.len() > response_bytes() {
                return Err(Error::Malformed);
            }
            log::info!(
                "pir_metric component=receiver stage=lookup_page elapsed_us={}",
                started.elapsed().as_micros()
            );
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
