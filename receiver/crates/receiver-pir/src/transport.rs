//! Receiver discovery over a caller-supplied, bounded transport.
use crate::{
    check_next, public_bytes, response_bytes, AcceptedCoverage, Client, Error, Manifest,
    MAX_MANIFEST_BYTES,
};
use receiver_directory::{
    snapshot::{lookup_row, row_for, ROW_BYTES},
    Payment, Receiver,
};
use sha2::{Digest, Sha256};
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
    /// Hex session ID that pins the setup, witness and row routes.
    id: String,
    session: DiscoverySession,
}

// One session per client, so the variant size difference does not matter.
#[allow(clippy::large_enum_variant)]
enum DiscoverySession {
    Pir(Client),
    File { manifest: Manifest, rows: Vec<u8> },
}

/// Compares remaining uncached discovery traffic, including PIR uploads. Note
/// retrieval is separate. Historical swap count and sunk traffic are irrelevant.
pub fn prefer_directory_file(manifest: &Manifest, remaining_lookups: usize) -> Result<bool, Error> {
    manifest.validate()?;
    let file_bytes = u64::from(manifest.directory.rows) * ROW_BYTES as u64;
    let pir_bytes = (remaining_lookups as u64).saturating_mul(
        (crate::query_bytes(manifest.directory.rows)? + response_bytes(manifest.directory.rows)?)
            as u64,
    );
    Ok(pir_bytes >= file_bytes)
}

impl<T: Transport> DirectoryClient<T> {
    /// Initialize against an independently accepted terminal anchor. Reconnect for a new revision.
    pub async fn connect(origin: &str, http: T, accepted: AcceptedCoverage) -> Result<Self, Error> {
        Self::connect_for_work(origin, http, accepted, 0).await
    }

    /// Select a transport for the whole remaining job and reuse it across batches.
    /// A file failure is an error, never a receiver-dependent public request.
    pub async fn connect_for_work(
        origin: &str,
        http: T,
        accepted: AcceptedCoverage,
        remaining_lookups: usize,
    ) -> Result<Self, Error> {
        let manifest = Self::fetch_manifest(origin, &http).await?;
        Self::connect_manifest(origin, http, accepted, manifest, remaining_lookups).await
    }

    /// Use the exact advertised revision whose anchor the caller independently checked.
    pub async fn connect_manifest(
        origin: &str,
        http: T,
        accepted: AcceptedCoverage,
        manifest: Manifest,
        remaining_lookups: usize,
    ) -> Result<Self, Error> {
        let origin = origin.trim_end_matches('/').to_owned();
        accepted.check(&manifest.directory)?;
        let id = hex::encode(manifest.id()?);
        let session = if prefer_directory_file(&manifest, remaining_lookups)? {
            let rows = download_rows(&http, &origin, &id, &manifest).await?;
            DiscoverySession::File { manifest, rows }
        } else {
            let public = http
                .get(
                    &format!("{origin}/v1/receiver/public/{id}"),
                    public_bytes(manifest.directory.rows)?,
                )
                .await?;
            DiscoverySession::Pir(Client::new(manifest, &public, accepted)?)
        };
        Ok(Self {
            origin,
            http,
            id,
            session,
        })
    }

    /// Re-evaluate newly discovered work without discarding setup or a verified file.
    pub async fn use_file_for_work(&mut self, remaining_lookups: usize) -> Result<(), Error> {
        if matches!(self.session, DiscoverySession::File { .. })
            || !prefer_directory_file(self.manifest(), remaining_lookups)?
        {
            return Ok(());
        }
        let manifest = self.manifest().clone();
        let rows = download_rows(&self.http, &self.origin, &self.id, &manifest).await?;
        self.session = DiscoverySession::File { manifest, rows };
        Ok(())
    }

    /// Fetch bounded public metadata before selecting the corresponding local chain anchor.
    pub async fn fetch_manifest(origin: &str, http: &T) -> Result<Manifest, Error> {
        let bytes = http
            .get(
                &format!("{}/v1/receiver/init", origin.trim_end_matches('/')),
                MAX_MANIFEST_BYTES,
            )
            .await?;
        if bytes.len() > MAX_MANIFEST_BYTES {
            return Err(Error::Malformed);
        }
        let manifest: Manifest = serde_json::from_slice(&bytes)?;
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn manifest(&self) -> &Manifest {
        match &self.session {
            DiscoverySession::Pir(client) => client.manifest(),
            DiscoverySession::File { manifest, .. } => manifest,
        }
    }

    /// Download identical proof bytes for all wallets. This never contains a receiver in the request.
    pub async fn witnesses(&self) -> Result<receiver_directory::witness::WitnessSnapshot, Error> {
        let bytes = self
            .http
            .get(
                &format!("{}/v1/receiver/witness/{}", self.origin, self.id),
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
            let record = match &self.session {
                DiscoverySession::File { manifest, rows } => {
                    let row = row_for(&manifest.directory, &receiver, page)?;
                    lookup_row(
                        &manifest.directory,
                        &receiver,
                        page,
                        &rows[row * ROW_BYTES..(row + 1) * ROW_BYTES],
                    )?
                }
                DiscoverySession::Pir(client) => {
                    let query = client.prepare(receiver, page)?;
                    let body = self
                        .http
                        .post(
                            &format!("{}/v1/receiver/query", self.origin),
                            query.body().to_vec(),
                            response_bytes(client.manifest().directory.rows)?,
                        )
                        .await?;
                    client.decode(query, &body)?
                }
            };
            let Some(record) = record else {
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

// Every file entry point uses the same bound and digest check before decoding rows.
async fn download_rows(
    http: &impl Transport,
    origin: &str,
    id: &str,
    manifest: &Manifest,
) -> Result<Vec<u8>, Error> {
    let limit = manifest.directory.rows as usize * ROW_BYTES;
    let data = http
        .get(&format!("{origin}/v1/receiver/rows/{id}"), limit)
        .await?;
    if data.len() != limit
        || <[u8; 32]>::from(Sha256::digest(&data)) != manifest.directory.data_sha256
    {
        return Err(Error::Malformed);
    }
    Ok(data)
}
