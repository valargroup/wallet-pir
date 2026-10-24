//! Transport-independent answering of prepared Transparent tables.
//!
//! A serving snapshot resolves what a request names — revision, table and
//! segment — into a [`PreparedQuery`] or [`PreparedSetup`] whose fields stay
//! private: the native parameters, the binding, the rows a selection spans
//! and where each segment's runtime comes from. The query server keeps the
//! HTTP, admission, deadlines and response presentation; it asks a prepared
//! query which body lengths it accepts, takes its runtimes and hands the body
//! to an [`Evaluator`]. A refusal is a [`ServingError`], which the server maps
//! to the wallet-facing status codes.
//!
//! [`history::serving`](crate::history::serving) and
//! [`tx_detail::serving`](crate::tx_detail::serving) resolve their own
//! addresses into these types.

use crate::history::shardset::SegmentSource;
use crate::runtime::disk::ShippedRuntimes;
use crate::runtime::metrics::Metrics;
use crate::runtime::{CacheError, Produced, RuntimeCache, RuntimeHandle, RuntimeKey, SharedParams};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Why a serving snapshot will not answer what a request names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServingError {
    /// The request is malformed for this publication: an unknown table, a
    /// segment past the table's, a revision of another shard, a failed
    /// runtime. Not retryable as-is.
    Invalid(String),
    /// The revision named is no longer served; the wallet refreshes the map
    /// `retry` names.
    Stale {
        message: String,
        retry: &'static str,
    },
    /// The revision exists but this snapshot does not hold it: an unassigned
    /// shard, the wrong tier or the wrong role. A routing fault.
    WrongHoldings(String),
    /// No cache capacity could be freed. Retryable as-is.
    Capacity,
}

impl From<CacheError> for ServingError {
    fn from(error: CacheError) -> Self {
        match error {
            CacheError::Overloaded => ServingError::Capacity,
            CacheError::Failed(message) => ServingError::Invalid(message),
        }
    }
}

/// One file background validation hashes, and the runtime it backs.
pub struct Artifact {
    pub digest: String,
    pub shard_id: u64,
    pub path: PathBuf,
    pub key: RuntimeKey,
}

/// Where a prepared request's runtimes come from.
enum Acquisition {
    /// Found, restored or built under the cache budget; `shipped` is tried
    /// before a restore or build.
    Build {
        segments: Vec<(RuntimeKey, SegmentSource)>,
        shipped: Option<ShippedRuntimes>,
        /// A snapshot can retire while its request uploads or waits for
        /// admission. Recheck this shared state when acquiring a runtime.
        retirement: Option<(Arc<AtomicBool>, ServingError)>,
    },
    /// Only runtimes already resident, for a superseded snapshot that must
    /// not take build capacity; a missing one makes the request stale.
    Resident {
        keys: Vec<RuntimeKey>,
        stale: ServingError,
    },
}

/// What a runtime acquisition needs beside the address.
struct Source {
    cache: Arc<RuntimeCache>,
    shared: Arc<SharedParams>,
    metrics: Arc<Metrics>,
    acquisition: Acquisition,
}

impl Source {
    fn segments(&self) -> usize {
        match &self.acquisition {
            Acquisition::Build { segments, .. } => segments.len(),
            Acquisition::Resident { keys, .. } => keys.len(),
        }
    }

    async fn runtime(&self, segment: usize) -> Result<(RuntimeHandle, Produced), ServingError> {
        match &self.acquisition {
            Acquisition::Build {
                segments,
                shipped,
                retirement,
            } => {
                let (key, source) = segments
                    .get(segment)
                    .cloned()
                    .ok_or_else(|| ServingError::Invalid(format!("no segment {segment}")))?;
                if let Some((retired, stale)) = retirement {
                    if retired.load(Ordering::Acquire) {
                        return self
                            .cache
                            .cached(&key)
                            .map(|handle| (handle, Produced::Joined))
                            .ok_or_else(|| {
                                Metrics::incr(&self.metrics.stale_revisions);
                                stale.clone()
                            });
                    }
                }
                Ok(self
                    .cache
                    .get_from(key, self.shared.clone(), source, shipped.clone())
                    .await?)
            }
            Acquisition::Resident { keys, stale } => keys
                .get(segment)
                .and_then(|key| self.cache.cached(key))
                .map(|handle| (handle, Produced::Joined))
                .ok_or_else(|| {
                    Metrics::incr(&self.metrics.stale_revisions);
                    stale.clone()
                }),
        }
    }

    /// Whether every segment is resident, for a resident-only source.
    fn check_resident(&self) -> Result<(), ServingError> {
        if let Acquisition::Resident { keys, stale } = &self.acquisition {
            if !keys.iter().all(|key| self.cache.cached(key).is_some()) {
                Metrics::incr(&self.metrics.stale_revisions);
                return Err(stale.clone());
            }
        }
        Ok(())
    }
}

/// A query whose revision and table a snapshot resolved.
pub struct PreparedQuery {
    source: Source,
    query_rows: usize,
    binding: [u8; 8],
    table: String,
    revision: String,
}

impl PreparedQuery {
    /// The table as refusals and traces name it.
    pub fn table(&self) -> &str {
        &self.table
    }

    /// The revision, for traces.
    pub fn revision(&self) -> &str {
        &self.revision
    }

    /// The geometry the table was cut to.
    pub fn geometry(&self) -> &'static str {
        self.source.shared.geometry.name
    }

    /// Whether `len` is one of the exact body lengths this table accepts.
    pub fn accepts_len(&self, len: usize) -> bool {
        self.source.shared.accepts_query_bytes(self.query_rows, len)
    }

    /// Every exact body length this table accepts, for refusals.
    pub fn lengths(&self) -> String {
        self.source.shared.query_lengths(self.query_rows)
    }

    /// Work memory one evaluation reserves beyond the held runtimes.
    pub fn work_bytes(&self) -> u64 {
        self.source.shared.reserved_bytes().saturating_mul(2)
    }

    /// How many segments answer the query, each one runtime.
    pub fn segments(&self) -> usize {
        self.source.segments()
    }

    /// One segment's runtime, found, restored or built under the cache
    /// budget, or only if resident for a superseded snapshot.
    pub async fn runtime(&self, segment: usize) -> Result<RuntimeHandle, ServingError> {
        self.source.runtime(segment).await.map(|(handle, _)| handle)
    }

    /// What evaluation needs, detached from the snapshot so it can move to a
    /// blocking thread.
    pub fn evaluator(&self) -> Evaluator {
        Evaluator {
            shared: self.source.shared.clone(),
            binding: self.binding,
            query_rows: self.query_rows,
        }
    }
}

/// Evaluates one query body against every segment's runtime.
pub struct Evaluator {
    shared: Arc<SharedParams>,
    binding: [u8; 8],
    query_rows: usize,
}

impl Evaluator {
    /// Checks the binding and length, parses the key and selection once and
    /// answers them from every segment, concatenated in segment order.
    pub fn evaluate(&self, runtimes: &[RuntimeHandle], body: &[u8]) -> Result<Vec<u8>, String> {
        // Every segment shares the table's packing setup and query masks, so
        // one parse serves them all.
        let query = self.shared.parse(self.binding, body, self.query_rows)?;
        let mut answer = Vec::with_capacity(self.shared.response_bytes() * runtimes.len());
        for runtime in runtimes {
            answer.extend(runtime.get().answer(self.binding, &query)?);
        }
        Ok(answer)
    }
}

/// A setup request a snapshot resolved: one segment of one table.
pub struct PreparedSetup {
    source: Source,
    segment: u32,
    segments: u32,
    table: String,
    bucket: Option<u32>,
}

/// One segment's public setup, with its runtime held while it is read.
pub struct Setup {
    pub runtime: RuntimeHandle,
    pub geometry: &'static str,
    pub table: String,
    /// The bucket a transaction-detail table belongs to.
    pub bucket: Option<u32>,
    pub segment: u32,
    pub segments: u32,
}

impl PreparedSetup {
    /// Takes the segment's runtime, building it if needed.
    pub async fn load(self) -> Result<Setup, ServingError> {
        let (runtime, _) = self.source.runtime(0).await?;
        Ok(Setup {
            runtime,
            geometry: self.source.shared.geometry.name,
            table: self.table,
            bucket: self.bucket,
            segment: self.segment,
            segments: self.segments,
        })
    }
}

/// Builders the history and detail snapshots use.
pub(crate) struct Prepared {
    pub cache: Arc<RuntimeCache>,
    pub shared: Arc<SharedParams>,
    pub metrics: Arc<Metrics>,
}

impl Prepared {
    pub(crate) fn build(
        self,
        segments: Vec<(RuntimeKey, SegmentSource)>,
        shipped: Option<ShippedRuntimes>,
    ) -> SourceParts {
        SourceParts(Source {
            cache: self.cache,
            shared: self.shared,
            metrics: self.metrics,
            acquisition: Acquisition::Build {
                segments,
                shipped,
                retirement: None,
            },
        })
    }

    pub(crate) fn resident(self, keys: Vec<RuntimeKey>, stale: ServingError) -> SourceParts {
        SourceParts(Source {
            cache: self.cache,
            shared: self.shared,
            metrics: self.metrics,
            acquisition: Acquisition::Resident { keys, stale },
        })
    }
}

/// A resolved runtime source, before it becomes a query or setup.
pub(crate) struct SourceParts(Source);

impl SourceParts {
    /// A source prepared while active must still become resident-only if
    /// its snapshot retires before the request acquires runtime handles.
    pub(crate) fn until_retired(mut self, retired: Arc<AtomicBool>, stale: ServingError) -> Self {
        if let Acquisition::Build { retirement, .. } = &mut self.0.acquisition {
            *retirement = Some((retired, stale));
        }
        self
    }

    /// A query; a resident-only source is refused now if a segment is gone,
    /// so the body is not queued for a request that cannot be answered.
    pub(crate) fn query(
        self,
        query_rows: usize,
        binding: [u8; 8],
        table: String,
        revision: String,
    ) -> Result<PreparedQuery, ServingError> {
        self.0.check_resident()?;
        Ok(PreparedQuery {
            source: self.0,
            query_rows,
            binding,
            table,
            revision,
        })
    }

    pub(crate) fn setup(
        self,
        segment: u32,
        segments: u32,
        table: String,
        bucket: Option<u32>,
    ) -> PreparedSetup {
        PreparedSetup {
            source: self.0,
            segment,
            segments,
            table,
            bucket,
        }
    }
}
