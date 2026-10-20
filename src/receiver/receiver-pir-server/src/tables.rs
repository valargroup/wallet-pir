//! A byte-budgeted cache of prepared PIR tables over durable sources.
//!
//! A [`Source`] names one payment or witness table: its session manifest and public
//! setup, which inventories and the public route need, and a loader that rebuilds its
//! prepared state, normally from files on disk. Prepared state is resident only while
//! [`Tables`] caches it or a query uses it. Every resident table counts against one
//! budget until its last user drops it, whether the cache still holds it or evicted it
//! during an in-flight query, so retained inventories and running queries share it.
//!
//! A [`FilterSource`] does the same for a sealed shard's receiver filter: every inventory
//! naming the filter shares one source, and its bytes are read from disk into the same
//! budget only while a request serves them or the cache keeps them.
use receiver_directory::{
    shards::ShardFilter,
    snapshot::{MAX_ROWS, ROW_BYTES},
    Hash,
};
use receiver_pir::{
    server::{Server, WitnessServer},
    Error, Manifest, WitnessManifest,
};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

/// The smallest budget that can always load a table cold once queries in flight finish:
/// a witness table prepares beside its payment table's server, so a cold witness load
/// holds both, each at the largest supported geometry.
pub const MIN_BUDGET: usize = 2 * resident_bytes(MAX_ROWS);

/// The bytes a `rows`-row table's prepared state is budgeted at: the rows, their 16-bit
/// scan copy and the preprocessed hint, about three times the rows.
pub const fn resident_bytes(rows: u32) -> usize {
    3 * rows as usize * ROW_BYTES
}

/// A table's prepared state.
#[derive(Clone)]
pub enum Loaded {
    Payments(Server),
    Witnesses(WitnessServer),
}

impl Loaded {
    /// The session ID it answers under.
    fn id(&self) -> Hash {
        match self {
            Loaded::Payments(s) => s.id(),
            Loaded::Witnesses(s) => s.id(),
        }
    }
}

/// What a source's loader rebuilds its table from. A table it needs in turn, such as a
/// companion's payment table, comes from the same cache, so a full budget is
/// [`LoadError::Busy`] rather than a failed load.
type Load = dyn Fn(&Tables) -> Result<Loaded, LoadError> + Send + Sync;

/// One table's identity, public setup and loader. Cloning shares the loader.
pub struct Source {
    id: Hash,
    rows: u32,
    public: Arc<[u8]>,
    manifest: Described,
    /// Whether the loader holds prepared state in memory, outside the budget.
    held: bool,
    load: Box<Load>,
}

/// A source's session manifest.
enum Described {
    Payments(Manifest),
    Witnesses(WitnessManifest),
}

impl Source {
    /// A payment table described by `manifest` with `public` setup, rebuilt by `load`.
    /// The loaded server must answer under the manifest's session ID.
    pub fn payments(
        manifest: Manifest,
        public: Vec<u8>,
        load: impl Fn() -> Result<Server, Error> + Send + Sync + 'static,
    ) -> Result<Self, Error> {
        Ok(Self {
            id: manifest.id()?,
            rows: manifest.directory.rows,
            public: public.into(),
            manifest: Described::Payments(manifest),
            held: false,
            load: Box::new(move |_| Ok(Loaded::Payments(load()?))),
        })
    }

    /// A companion witness table described by `manifest` with `public` setup, bound to
    /// the payment table `payments`; `load` supplies its table, which is prepared beside
    /// that payment table's server.
    pub fn witnesses(
        manifest: WitnessManifest,
        public: Vec<u8>,
        payments: Arc<Source>,
        load: impl Fn() -> Result<receiver_directory::witness::Table, Error> + Send + Sync + 'static,
    ) -> Result<Self, Error> {
        Ok(Self {
            id: manifest.id()?,
            rows: manifest.table.rows,
            public: public.into(),
            manifest: Described::Witnesses(manifest),
            held: false,
            load: Box::new(move |tables| {
                let resident = tables.get(&payments)?;
                let server = resident.payments().ok_or(Error::Malformed)?;
                Ok(Loaded::Witnesses(WitnessServer::new(load()?, server)?))
            }),
        })
    }

    /// A source over already prepared state held in memory, for tables with no durable
    /// copy: the cache can drop its entry, but this source keeps the state alive.
    pub fn memory(loaded: Loaded) -> Self {
        let (id, rows, public, manifest) = match &loaded {
            Loaded::Payments(s) => (
                s.id(),
                s.manifest().directory.rows,
                s.public().to_vec(),
                Described::Payments(s.manifest().clone()),
            ),
            Loaded::Witnesses(s) => (
                s.id(),
                s.manifest().table.rows,
                s.public().to_vec(),
                Described::Witnesses(s.manifest().clone()),
            ),
        };
        Self {
            id,
            rows,
            public: public.into(),
            manifest,
            held: true,
            load: Box::new(move |_| Ok(loaded.clone())),
        }
    }

    /// The session ID.
    pub fn id(&self) -> Hash {
        self.id
    }

    /// The table's row count, which fixes its query length.
    pub fn rows(&self) -> u32 {
        self.rows
    }

    /// The public setup clients download once per session.
    pub fn public(&self) -> &[u8] {
        &self.public
    }

    /// The payment session manifest, if this is a payment table.
    pub fn payment_manifest(&self) -> Option<&Manifest> {
        match &self.manifest {
            Described::Payments(m) => Some(m),
            Described::Witnesses(_) => None,
        }
    }

    /// The witness session manifest, if this is a companion table.
    pub fn witness_manifest(&self) -> Option<&WitnessManifest> {
        match &self.manifest {
            Described::Witnesses(m) => Some(m),
            Described::Payments(_) => None,
        }
    }

    /// Bytes it keeps resident outside the budget: its prepared state if held in memory.
    pub fn held_bytes(&self) -> usize {
        match self.held {
            true => self.resident_bytes(),
            false => 0,
        }
    }

    /// The bytes its prepared state is budgeted at; see [`resident_bytes`].
    pub fn resident_bytes(&self) -> usize {
        resident_bytes(self.rows)
    }
}

/// One sealed shard filter's digest, encoded length and receiver count, which
/// inventories list, and a loader that reads its encoding, normally from disk.
pub struct FilterSource {
    digest: Hash,
    bytes: u32,
    receivers: u64,
    /// The encoding held outside the cache, for a filter with no durable copy.
    held: Option<Arc<[u8]>>,
    load: Box<dyn Fn() -> Result<Vec<u8>, Error> + Send + Sync>,
}

impl FilterSource {
    /// A filter with `digest`, `bytes` long and naming `receivers`, whose encoding `load`
    /// reads. A load that does not decode to that digest and length fails.
    pub fn stored(
        digest: Hash,
        bytes: u32,
        receivers: u64,
        load: impl Fn() -> Result<Vec<u8>, Error> + Send + Sync + 'static,
    ) -> Self {
        Self {
            digest,
            bytes,
            receivers,
            held: None,
            load: Box::new(load),
        }
    }

    /// A source over `filter`'s encoding held in memory, for a filter with no durable
    /// copy; those bytes stay resident outside the budget.
    pub fn memory(filter: &ShardFilter) -> Self {
        let held: Arc<[u8]> = filter.bytes().into();
        let encoding = held.clone();
        Self {
            digest: filter.digest(),
            bytes: filter.bytes().len() as u32,
            receivers: filter.receivers(),
            held: Some(held),
            load: Box::new(move || Ok(encoding.to_vec())),
        }
    }

    /// The digest that names it.
    pub fn digest(&self) -> Hash {
        self.digest
    }

    /// Its encoded length.
    pub fn bytes(&self) -> u32 {
        self.bytes
    }

    /// The receivers it was built from.
    pub fn receivers(&self) -> u64 {
        self.receivers
    }

    /// Bytes it keeps resident outside the budget: its encoding if held in memory.
    pub fn held_bytes(&self) -> usize {
        self.held.as_ref().map_or(0, |h| h.len())
    }
}

/// A prepared table or filter encoding in use or cached, counted against the budget
/// until dropped.
pub struct Resident {
    content: Content,
    bytes: usize,
    live: Arc<AtomicUsize>,
}

/// What a resident entry holds.
enum Content {
    Table(Box<Loaded>),
    Filter(Box<[u8]>),
}

impl Resident {
    /// The payment server, if this is a payment table.
    pub fn payments(&self) -> Option<&Server> {
        match &self.content {
            Content::Table(table) => match table.as_ref() {
                Loaded::Payments(s) => Some(s),
                Loaded::Witnesses(_) => None,
            },
            Content::Filter(_) => None,
        }
    }

    /// The filter encoding, if this is a filter.
    pub fn filter(&self) -> Option<&[u8]> {
        match &self.content {
            Content::Filter(bytes) => Some(bytes),
            Content::Table(_) => None,
        }
    }

    /// Answers one encrypted query for this table's session.
    pub fn respond(&self, body: &[u8]) -> Result<Vec<u8>, Error> {
        match &self.content {
            Content::Table(table) => match table.as_ref() {
                Loaded::Payments(s) => s.respond(body),
                Loaded::Witnesses(s) => s.respond(body),
            },
            Content::Filter(_) => Err(Error::Malformed),
        }
    }
}

impl Drop for Resident {
    /// Releases its share of the budget.
    fn drop(&mut self) {
        self.live.fetch_sub(self.bytes, Ordering::SeqCst);
    }
}

/// Why a table could not be made resident.
#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    /// Tables in use fill the budget; retry once queries finish.
    #[error("prepared tables fill the memory budget")]
    Busy,
    #[error(transparent)]
    Load(#[from] Error),
}

impl From<LoadError> for Error {
    /// A failed load as the PIR error a caller reports.
    fn from(error: LoadError) -> Self {
        match error {
            LoadError::Busy => Error::Transport("prepared tables fill the memory budget".into()),
            LoadError::Load(error) => error,
        }
    }
}

/// The least recently used entries first out, within one byte budget.
pub struct Tables {
    budget: usize,
    live: Arc<AtomicUsize>,
    loads: AtomicU64,
    entries: Mutex<Entries>,
}

/// Cached entries by session ID, or by digest for a filter (`true`), with their last use.
#[derive(Default)]
struct Entries {
    tables: HashMap<(Hash, bool), (Arc<Resident>, u64)>,
    clock: u64,
}

impl Tables {
    /// A cache keeping at most `budget` bytes of prepared tables resident.
    pub fn new(budget: usize) -> Self {
        Self {
            budget,
            live: Arc::new(AtomicUsize::new(0)),
            loads: AtomicU64::new(0),
            entries: Mutex::new(Entries::default()),
        }
    }

    /// The budget in bytes.
    pub fn budget(&self) -> usize {
        self.budget
    }

    /// Bytes of prepared tables resident now, cached or in use.
    pub fn resident(&self) -> usize {
        self.live.load(Ordering::SeqCst)
    }

    /// How many tables a loader has rebuilt.
    pub fn loads(&self) -> u64 {
        self.loads.load(Ordering::SeqCst)
    }

    /// `source`'s prepared table, loading it if it is not cached. Loading first evicts the
    /// least recently used cached tables until it fits; tables still in use keep their
    /// bytes until their queries finish, and if they alone fill the budget the load is
    /// [`LoadError::Busy`].
    pub fn get(&self, source: &Source) -> Result<Arc<Resident>, LoadError> {
        if let Some(found) = self.touch(&(source.id, false)) {
            return Ok(found);
        }
        self.reserve(source.resident_bytes())?;
        let bytes = source.resident_bytes();
        let loaded = match (source.load)(self) {
            Ok(loaded) if loaded.id() == source.id => loaded,
            failed => {
                self.live.fetch_sub(bytes, Ordering::SeqCst);
                return Err(failed.err().unwrap_or(Error::Malformed.into()));
            }
        };
        self.loads.fetch_add(1, Ordering::SeqCst);
        Ok(self.cache((source.id, false), Content::Table(Box::new(loaded)), bytes))
    }

    /// `source`'s filter encoding, reading it if it is not cached, within the budget as
    /// [`Self::get`] loads a table. An encoding that does not decode to the source's digest
    /// and length fails to load.
    pub fn filter(&self, source: &FilterSource) -> Result<Arc<Resident>, LoadError> {
        let key = (source.digest, true);
        if let Some(found) = self.touch(&key) {
            return Ok(found);
        }
        let bytes = source.bytes as usize;
        self.reserve(bytes)?;
        let loaded = (source.load)().and_then(|encoding| {
            let filter = ShardFilter::decode(&encoding)?;
            match filter.digest() == source.digest && encoding.len() == bytes {
                true => Ok(encoding),
                false => Err(Error::Malformed),
            }
        });
        let encoding = match loaded {
            Ok(encoding) => encoding,
            Err(error) => {
                self.live.fetch_sub(bytes, Ordering::SeqCst);
                return Err(error.into());
            }
        };
        self.loads.fetch_add(1, Ordering::SeqCst);
        Ok(self.cache(key, Content::Filter(encoding.into()), bytes))
    }

    /// Caches `loaded`, prepared already for `source`, evicting as [`Self::get`] does.
    /// A table that does not fit beside those in use is still returned and counted, so
    /// a publication's own tables never fail to serve; the budget sheds cached tables
    /// first.
    pub fn insert(&self, source: &Source, loaded: Loaded) -> Arc<Resident> {
        if let Some(found) = self.touch(&(source.id, false)) {
            return found;
        }
        let bytes = source.resident_bytes();
        if self.reserve(bytes).is_err() {
            self.live.fetch_add(bytes, Ordering::SeqCst);
        }
        self.cache((source.id, false), Content::Table(Box::new(loaded)), bytes)
    }

    /// The cached entry for `id`, marked used.
    fn touch(&self, id: &(Hash, bool)) -> Option<Arc<Resident>> {
        let mut entries = self.entries.lock().unwrap();
        entries.clock += 1;
        let clock = entries.clock;
        let (resident, used) = entries.tables.get_mut(id)?;
        *used = clock;
        Some(resident.clone())
    }

    /// Counts `bytes` against the budget, evicting idle and then any least recently used
    /// cached tables to make room.
    fn reserve(&self, bytes: usize) -> Result<(), LoadError> {
        let mut entries = self.entries.lock().unwrap();
        loop {
            let live = self.live.load(Ordering::SeqCst);
            if live + bytes <= self.budget {
                self.live.fetch_add(bytes, Ordering::SeqCst);
                return Ok(());
            }
            let oldest = entries
                .tables
                .iter()
                .min_by_key(|(_, (resident, used))| (Arc::strong_count(resident) > 1, *used))
                .map(|(id, _)| *id);
            match oldest {
                // Dropping the cache's reference frees an idle table at once and an
                // in-use one when its query finishes.
                Some(id) => {
                    let evicted = entries.tables.remove(&id);
                    drop(entries);
                    drop(evicted);
                    entries = self.entries.lock().unwrap();
                }
                None => return Err(LoadError::Busy),
            }
        }
    }

    /// Records `content` under `id`, its `bytes` already counted.
    fn cache(&self, id: (Hash, bool), content: Content, bytes: usize) -> Arc<Resident> {
        let resident = Arc::new(Resident {
            content,
            bytes,
            live: self.live.clone(),
        });
        let mut entries = self.entries.lock().unwrap();
        entries.clock += 1;
        let clock = entries.clock;
        match entries.tables.get(&id) {
            // A concurrent load won; drop this copy and its count.
            Some((found, _)) => found.clone(),
            None => {
                entries.tables.insert(id, (resident.clone(), clock));
                resident
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use receiver_directory::snapshot::Snapshot;
    use receiver_pir::MIN_ROWS;

    /// A minimum-geometry payment table whose salt starts with `salt`.
    fn snapshot(salt: u8) -> Snapshot {
        let mut manifest = crate::common::manifest(MIN_ROWS);
        manifest.salt[0] = salt;
        Snapshot::build(manifest, &[]).unwrap()
    }

    /// A payment source over `snapshot` that prepares it on every load.
    fn payments(snapshot: Snapshot) -> Arc<Source> {
        let server = crate::prepared::payments(snapshot.clone()).unwrap();
        Arc::new(
            Source::payments(
                server.manifest().clone(),
                server.public().to_vec(),
                move || crate::prepared::payments(snapshot.clone()),
            )
            .unwrap(),
        )
    }

    /// A placeholder companion source beside `payments`, built from `snapshot`.
    fn witnesses(snapshot: &Snapshot, payments: Arc<Source>) -> Source {
        let table = crate::prepared::placeholder(snapshot);
        let server = crate::prepared::payments(snapshot.clone()).unwrap();
        let prepared = crate::prepared::companion(table.clone(), &server).unwrap();
        Source::witnesses(
            prepared.manifest().clone(),
            prepared.public().to_vec(),
            payments,
            move || Ok(table.clone()),
        )
        .unwrap()
    }

    /// At a budget of exactly its geometry's pair, a cold witness load evicts an idle
    /// table and succeeds; a table in use leaves no room, which is `Busy` (a retry), not a
    /// failed load, and the same load succeeds once that query finishes.
    #[test]
    fn a_cold_witness_load_fits_the_pair_budget_and_waits_for_queries() {
        let tables = Tables::new(2 * resident_bytes(MIN_ROWS));
        let (one, other) = (snapshot(1), snapshot(2));
        let unrelated = payments(other);
        tables.get(&unrelated).unwrap();
        let paid = payments(one.clone());
        let companion = witnesses(&one, paid.clone());
        let resident = tables.get(&companion).unwrap();
        assert!(resident.payments().is_none());
        assert!(tables.resident() <= tables.budget());
        drop(resident);
        // A query holding the unrelated table leaves room for one table, not the pair.
        let tables = Tables::new(2 * resident_bytes(MIN_ROWS));
        let held = tables.get(&unrelated).unwrap();
        assert!(matches!(tables.get(&companion), Err(LoadError::Busy)));
        assert_eq!(tables.resident(), resident_bytes(MIN_ROWS));
        drop(held);
        assert!(tables.get(&companion).is_ok());
    }
}
