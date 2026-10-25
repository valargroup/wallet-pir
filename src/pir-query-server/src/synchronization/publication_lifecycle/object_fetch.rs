//! Bounded, verified transfer of publication objects into the role's cache.
use pir_publication::{ObjectCache, ObjectReference, OBJECTS_PATH};
use pir_sync_manager::{Error, HttpsSource, Verification};
use std::path::PathBuf;

/// Fetches objects for one pass within the source's aggregate byte budget.
/// Every object is verified against its full length and SHA-256 before it is
/// promoted; a cached object is verified before reuse, with settled integrity
/// evidence reused only while its exact file identity remains unchanged.
pub struct ObjectFetcher<'a> {
    source: &'a mut HttpsSource,
    cache: &'a ObjectCache,
}
impl<'a> ObjectFetcher<'a> {
    pub(super) fn new(source: &'a mut HttpsSource, cache: &'a ObjectCache) -> Self {
        Self { source, cache }
    }
    /// The verified cached path of `object`, downloading it when absent.
    pub async fn fetch(&mut self, object: &ObjectReference) -> Result<PathBuf, Error> {
        let cache = self.cache.clone();
        let expected = object.clone();
        if let Some(path) =
            tokio::task::spawn_blocking(move || cache.verified_cached(&expected)).await??
        {
            return Ok(path);
        }
        let mut partial = self.cache.begin_partial(object)?;
        let mut file = tokio::fs::File::from_std(partial.take_file()?);
        let response = self
            .source
            .request(&format!("{OBJECTS_PATH}{}", object.sha256), None)
            .await?;
        response
            .copy_to(
                &mut file,
                object.bytes,
                Verification::Sha256(&object.sha256),
            )
            .await?;
        let written = file.into_std().await;
        Ok(tokio::task::spawn_blocking(move || partial.promote(written)).await??)
    }
    /// Fetch a metadata object of at most `limit` bytes and return its bytes.
    pub async fn fetch_metadata(
        &mut self,
        object: &ObjectReference,
        limit: u64,
    ) -> Result<Vec<u8>, Error> {
        object.validate(limit)?;
        let path = self.fetch(object).await?;
        let bytes = tokio::fs::read(path).await?;
        object.verify(&bytes)?;
        Ok(bytes)
    }
}
