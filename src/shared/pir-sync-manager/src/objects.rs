//! Bounded artifact transfer with explicit verification and corruption policy.
use crate::{Error, HttpsSource, Verification};
use pir_publication::{
    ObjectCache, ObjectReference, PartialObject, ReceivedObjectFile, VerifiedObjectFile,
    OBJECTS_PATH,
};
use std::{fs::File, io};

#[derive(Clone, Copy)]
pub enum CorruptionPolicy {
    Refuse,
    ReplaceFile,
    RepairEntry,
}
pub struct Fetched<T> {
    pub file: T,
    pub downloaded: bool,
}

/// One source budget and cache owner. A received handle proves length and
/// physical identity; only a verified handle proves its complete digest too.
pub struct ObjectFetcher<'a> {
    source: &'a mut HttpsSource,
    cache: &'a ObjectCache,
    corruption: CorruptionPolicy,
    cache_limit: Option<u64>,
    prefix: &'a str,
}
impl<'a> ObjectFetcher<'a> {
    pub fn new(source: &'a mut HttpsSource, cache: &'a ObjectCache) -> Self {
        Self {
            source,
            cache,
            corruption: CorruptionPolicy::Refuse,
            cache_limit: None,
            prefix: OBJECTS_PATH,
        }
    }
    pub fn with_corruption(mut self, policy: CorruptionPolicy) -> Self {
        self.corruption = policy;
        self
    }
    pub fn with_cache_limit(mut self, bytes: u64) -> Self {
        self.cache_limit = Some(bytes);
        self
    }
    pub fn with_prefix(mut self, prefix: &'a str) -> Self {
        self.prefix = prefix;
        self
    }
    async fn cached<T: Send + 'static>(
        &self,
        object: &ObjectReference,
        open: fn(&ObjectCache, &ObjectReference) -> io::Result<Option<T>>,
    ) -> Result<Option<T>, Error> {
        let (cache, expected) = (self.cache.clone(), object.clone());
        match tokio::task::spawn_blocking(move || open(&cache, &expected)).await? {
            Ok(file) => Ok(file),
            Err(error) => {
                let path = self.cache.object_path(object)?;
                match self.corruption {
                    CorruptionPolicy::Refuse => return Err(error.into()),
                    CorruptionPolicy::RepairEntry if path.symlink_metadata()?.is_dir() => {
                        std::fs::remove_dir_all(path)?
                    }
                    _ => std::fs::remove_file(path)?,
                }
                Ok(None)
            }
        }
    }
    async fn download(&mut self, object: &ObjectReference) -> Result<(PartialObject, File), Error> {
        if let Some(limit) = self.cache_limit {
            if self
                .cache
                .occupied_bytes()?
                .checked_add(object.bytes)
                .is_none_or(|sum| sum > limit)
            {
                return Err("artifact cache capacity reached".into());
            }
        }
        let mut partial = self.cache.begin_partial(object)?;
        let mut file = tokio::fs::File::from_std(partial.take_file()?);
        self.source
            .request(&format!("{}{}", self.prefix, object.sha256), None)
            .await?
            .copy_to(&mut file, object.bytes, Verification::Length)
            .await?;
        Ok((partial, file.into_std().await))
    }
    pub async fn fetch(
        &mut self,
        object: &ObjectReference,
    ) -> Result<Fetched<VerifiedObjectFile>, Error> {
        if let Some(file) = self.cached(object, ObjectCache::open_verified).await? {
            return Ok(Fetched {
                file,
                downloaded: false,
            });
        }
        let (partial, written) = self.download(object).await?;
        let file = tokio::task::spawn_blocking(move || partial.promote_verified(written)).await??;
        Ok(Fetched {
            file,
            downloaded: true,
        })
    }
    pub async fn receive(
        &mut self,
        object: &ObjectReference,
    ) -> Result<Fetched<ReceivedObjectFile>, Error> {
        if let Some(file) = self.cached(object, ObjectCache::open_received).await? {
            return Ok(Fetched {
                file,
                downloaded: false,
            });
        }
        let (partial, written) = self.download(object).await?;
        let file = tokio::task::spawn_blocking(move || partial.promote_received(written)).await??;
        Ok(Fetched {
            file,
            downloaded: true,
        })
    }
    pub async fn fetch_metadata(
        &mut self,
        object: &ObjectReference,
        limit: u64,
    ) -> Result<Vec<u8>, Error> {
        object.validate(limit)?;
        let checked = self.fetch(object).await?.file;
        Ok(tokio::task::spawn_blocking(move || checked.read_bytes()).await??)
    }
}
