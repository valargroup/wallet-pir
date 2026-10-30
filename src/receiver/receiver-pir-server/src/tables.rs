//! A byte-budgeted cache of prepared P8 tables and served objects over durable sources.
//!
//! A [`Source`] names one directory or companion table: its session and public setup,
//! which inventories and the public route need, and a loader that rebuilds its prepared
//! state, normally from files on disk. Directories and companions prepare
//! independently. A load is charged its kind's preparation peak ([`peak_bytes`]) inside
//! the blocking work that runs it, before it starts, so a caller that stops waiting
//! frees nothing while the worker runs. Once prepared, the charge shrinks to the table's
//! [`resident_bytes`] and stays until the last owner drops it, whether the cache still
//! holds it or evicted it during an in-flight query.
//!
//! An [`ObjectSource`] does the same for a sealed shard's receiver filter or a
//! segment's choice object: every inventory naming it shares one source, and its bytes
//! are read from disk into the same budget only while a request serves them or the
//! cache keeps them.
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};
use zakura_pir_query_server::receiver::session::Error;
use zakura_pir_query_server::receiver::Table as _;
use zakura_pir_query_server::receiver::{
    self, server::Server, ChoiceRef, Companion, Directory, Kind, Session, DIRECTORY_ROWS,
    MAX_COMPANION_ROWS, ROW_BYTES,
};
use zakura_pir_query_server::receiver::{shards::ShardFilter, Hash};

/// Bytes a prepared P8 table keeps beside its scan copy, at most: its two packing
/// blocks' preprocessing and public setup. Measured with `examples/measure.rs`
/// (53 to 71 MB at 2,048 to 65,536 rows; `docs/receiver/README.md`).
const FIXED_BYTES: usize = 72 << 20;
/// Bytes preparation holds beyond the rows and their scan copy, at most: the hint and
/// the analysis that builds the preprocessing (at most about 300 MB, measured).
const SCRATCH_BYTES: usize = 320 << 20;

/// The bytes preparing a `kind` table of `rows` rows holds at its peak: the rows read
/// from disk and the scan copy made from them, a companion's directory, which its
/// load reads to check it, and the preparation scratch.
pub const fn peak_bytes(kind: Kind, rows: u32) -> usize {
    let directory = match kind {
        Kind::Directory => 0,
        Kind::Companion => DIRECTORY_ROWS as usize * ROW_BYTES,
    };
    2 * rows as usize * ROW_BYTES + directory + SCRATCH_BYTES
}

/// The bytes a prepared table of `rows` rows keeps: its scan copy and fixed parts.
pub const fn resident_bytes(rows: u32) -> usize {
    rows as usize * ROW_BYTES + FIXED_BYTES
}

/// The smallest budget that can always prepare a table cold once queries in flight
/// finish: the largest directory resident beside the largest companion's preparation.
pub const MIN_BUDGET: usize =
    resident_bytes(DIRECTORY_ROWS) + peak_bytes(Kind::Companion, MAX_COMPANION_ROWS);

/// Refuses a public setup that is not P8's length with the digest `session` binds.
fn check_public(public: &[u8], digest: &Hash) -> Result<(), Error> {
    use sha2::Digest;
    match public.len() == receiver::native::public_bytes()
        && sha2::Sha256::digest(public)[..] == digest[..]
    {
        true => Ok(()),
        false => Err(Error::Malformed),
    }
}

/// A table's prepared state.
#[derive(Clone)]
pub enum Loaded {
    Directory(Server<Directory>),
    Companion(Server<Companion>),
}

impl Loaded {
    /// The session ID it answers under.
    fn id(&self) -> Hash {
        match self {
            Loaded::Directory(s) => s.id(),
            Loaded::Companion(s) => s.id(),
        }
    }

    /// Answers one encrypted query for this table's session.
    fn respond(&self, body: &[u8]) -> Result<Vec<u8>, Error> {
        match self {
            Loaded::Directory(s) => s.respond(body),
            Loaded::Companion(s) => s.respond(body),
        }
    }
}

/// A table's session as an inventory lists it.
#[derive(Clone)]
pub enum Described {
    Directory(Session<Directory>),
    Companion(Session<Companion>),
}

/// What a source's loader rebuilds its table from.
type Load = dyn Fn() -> Result<Loaded, Error> + Send + Sync;

/// One table's identity, public setup and loader.
pub struct Source {
    id: Hash,
    public: Arc<[u8]>,
    session: Described,
    load: Box<Load>,
}

impl Source {
    /// A directory with `session` and `public` setup, rebuilt by `load`, which must
    /// answer under the session's ID. A `public` the session does not bind is refused.
    pub fn directory(
        session: Session<Directory>,
        public: Vec<u8>,
        load: impl Fn() -> Result<Server<Directory>, Error> + Send + Sync + 'static,
    ) -> Result<Self, Error> {
        check_public(&public, &session.public_digest)?;
        Ok(Self {
            id: session.id()?,
            public: public.into(),
            session: Described::Directory(session),
            load: Box::new(move || Ok(Loaded::Directory(load()?))),
        })
    }

    /// A companion with `session` and `public` setup, rebuilt by `load` as
    /// [`Self::directory`] is.
    pub fn companion(
        session: Session<Companion>,
        public: Vec<u8>,
        load: impl Fn() -> Result<Server<Companion>, Error> + Send + Sync + 'static,
    ) -> Result<Self, Error> {
        check_public(&public, &session.public_digest)?;
        Ok(Self {
            id: session.id()?,
            public: public.into(),
            session: Described::Companion(session),
            load: Box::new(move || Ok(Loaded::Companion(load()?))),
        })
    }

    /// A source over already prepared state held in memory, for tables with no durable
    /// copy: the cache can drop its entry, but this source keeps the state alive.
    pub fn memory(loaded: Loaded) -> Self {
        let (session, public) = match &loaded {
            Loaded::Directory(s) => (Described::Directory(s.session().clone()), s.public()),
            Loaded::Companion(s) => (Described::Companion(s.session().clone()), s.public()),
        };
        Self {
            id: loaded.id(),
            public: public.into(),
            session,
            load: Box::new(move || Ok(loaded.clone())),
        }
    }

    /// The session ID.
    pub fn id(&self) -> Hash {
        self.id
    }

    /// The bytes preparing this table is charged at its peak; see [`peak_bytes`].
    fn peak(&self) -> usize {
        match &self.session {
            Described::Directory(s) => peak_bytes(Kind::Directory, s.manifest.rows()),
            Described::Companion(s) => peak_bytes(Kind::Companion, s.manifest.rows()),
        }
    }

    /// The table's row count, which fixes a full query's length.
    pub fn rows(&self) -> u32 {
        match &self.session {
            Described::Directory(s) => s.manifest.rows(),
            Described::Companion(s) => s.manifest.rows(),
        }
    }

    /// The only shorter prefix a query may cover: a companion's `query_rows`, or the
    /// directory's rows.
    pub fn query_rows(&self) -> u32 {
        match &self.session {
            Described::Directory(s) => s.manifest.query_rows(),
            Described::Companion(s) => s.manifest.query_rows(),
        }
    }

    /// The public setup clients download once per session.
    pub fn public(&self) -> &[u8] {
        &self.public
    }

    /// The directory session, if this is a directory.
    pub fn directory_session(&self) -> Option<&Session<Directory>> {
        match &self.session {
            Described::Directory(s) => Some(s),
            Described::Companion(_) => None,
        }
    }

    /// The companion session, if this is a companion.
    pub fn companion_session(&self) -> Option<&Session<Companion>> {
        match &self.session {
            Described::Companion(s) => Some(s),
            Described::Directory(_) => None,
        }
    }
}

/// What a served object is, which fixes how its loaded bytes are checked.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ObjectKind {
    /// A sealed shard's receiver filter.
    Filter,
    /// A segment's choice object.
    Choice,
}

/// One served object's digest, encoded length and count (a filter's receivers, a
/// choice object's keys), which inventories list, and a loader that reads its
/// encoding, normally from disk.
pub struct ObjectSource {
    kind: ObjectKind,
    digest: Hash,
    bytes: u32,
    count: u64,
    load: Box<dyn Fn() -> Result<Vec<u8>, Error> + Send + Sync>,
}

impl ObjectSource {
    /// A filter with `digest`, `bytes` long and naming `receivers`, whose encoding
    /// `load` reads. A load that does not decode to that digest and length fails.
    pub fn filter(
        digest: Hash,
        bytes: u32,
        receivers: u64,
        load: impl Fn() -> Result<Vec<u8>, Error> + Send + Sync + 'static,
    ) -> Self {
        Self {
            kind: ObjectKind::Filter,
            digest,
            bytes,
            count: receivers,
            load: Box::new(load),
        }
    }

    /// The choice object `reference` names, whose encoding `load` reads. A load whose
    /// SHA-256 or length differs fails.
    pub fn choice(
        reference: &ChoiceRef,
        load: impl Fn() -> Result<Vec<u8>, Error> + Send + Sync + 'static,
    ) -> Self {
        Self {
            kind: ObjectKind::Choice,
            digest: reference.digest,
            bytes: reference.bytes,
            count: reference.keys.into(),
            load: Box::new(load),
        }
    }

    /// A source over a choice object held in memory, for one with no durable copy;
    /// those bytes stay resident outside the budget.
    pub fn memory_choice(reference: &ChoiceRef, bytes: &[u8]) -> Self {
        let held: Arc<[u8]> = bytes.into();
        Self::choice(reference, move || Ok(held.to_vec()))
    }

    /// The digest that names it.
    pub fn digest(&self) -> Hash {
        self.digest
    }

    /// Its encoded length.
    pub fn bytes(&self) -> u32 {
        self.bytes
    }

    /// A filter's receivers or a choice object's keys.
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Whether `encoding` is this object.
    fn matches(&self, encoding: &[u8]) -> bool {
        use sha2::Digest;
        encoding.len() == self.bytes as usize
            && match self.kind {
                ObjectKind::Filter => {
                    ShardFilter::decode(encoding).is_ok_and(|f| f.digest() == self.digest)
                }
                ObjectKind::Choice => sha2::Sha256::digest(encoding)[..] == self.digest[..],
            }
    }
}

/// Bytes counted against a budget until dropped.
struct Charge {
    bytes: usize,
    live: Arc<AtomicUsize>,
}

impl Charge {
    /// Lowers the charge to `bytes`, releasing the rest.
    fn shrink(&mut self, bytes: usize) {
        if bytes < self.bytes {
            self.live.fetch_sub(self.bytes - bytes, Ordering::SeqCst);
            self.bytes = bytes;
        }
    }
}

impl Drop for Charge {
    /// Releases the charge.
    fn drop(&mut self) {
        self.live.fetch_sub(self.bytes, Ordering::SeqCst);
    }
}

/// A preparation's peak charge, reserved before preparing a table outside the cache
/// and held until the table is cached or the preparation stops; see
/// [`Tables::reserve_peak`].
pub struct Reservation(Charge);

/// A prepared table or object encoding in use or cached, charged until dropped.
pub struct Resident {
    content: Content,
    charge: Charge,
}

/// What a resident entry holds.
enum Content {
    Table(Box<Loaded>),
    Object(Box<[u8]>),
}

impl Resident {
    /// The object encoding, if this is a filter or choice object.
    pub fn object(&self) -> Option<&[u8]> {
        match &self.content {
            Content::Object(bytes) => Some(bytes),
            Content::Table(_) => None,
        }
    }

    /// Answers one encrypted query for this table's session.
    pub fn respond(&self, body: &[u8]) -> Result<Vec<u8>, Error> {
        match &self.content {
            Content::Table(table) => table.respond(body),
            Content::Object(_) => Err(Error::Malformed),
        }
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

/// A cache key: a table's session ID, or an object's digest and kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Key {
    Table(Hash),
    Object(ObjectKind, Hash),
}

/// The least recently used entries first out, within one byte budget.
pub struct Tables {
    budget: usize,
    live: Arc<AtomicUsize>,
    loads: AtomicU64,
    entries: Mutex<Entries>,
    /// One gate per key being loaded; see [`Tables::once`].
    loading: Mutex<HashMap<Key, Arc<Mutex<()>>>>,
}

/// Cached entries with their last use.
#[derive(Default)]
struct Entries {
    tables: HashMap<Key, (Arc<Resident>, u64)>,
    clock: u64,
}

impl Tables {
    /// A cache keeping at most `budget` bytes charged.
    pub fn new(budget: usize) -> Self {
        Self {
            budget,
            live: Arc::new(AtomicUsize::new(0)),
            loads: AtomicU64::new(0),
            entries: Mutex::new(Entries::default()),
            loading: Mutex::new(HashMap::new()),
        }
    }

    /// The budget in bytes.
    pub fn budget(&self) -> usize {
        self.budget
    }

    /// Bytes charged now: preparations running and entries cached or in use.
    pub fn resident(&self) -> usize {
        self.live.load(Ordering::SeqCst)
    }

    /// How many tables or objects a loader has rebuilt.
    pub fn loads(&self) -> u64 {
        self.loads.load(Ordering::SeqCst)
    }

    /// `source`'s prepared table, preparing it if it is not cached. Call it from the
    /// blocking work that serves the request. Preparing first evicts the least
    /// recently used idle entries until its [`peak_bytes`] fit. Entries in use are
    /// never evicted: if evicting every idle one could not make room, the load is
    /// [`LoadError::Busy`] and nothing is evicted.
    pub fn get(&self, source: &Source) -> Result<Arc<Resident>, LoadError> {
        let key = Key::Table(source.id);
        if let Some(found) = self.touch(&key) {
            return Ok(found);
        }
        self.once(key, || {
            let mut charge = self.reserve(source.peak())?;
            let loaded = (source.load)()?;
            if loaded.id() != source.id {
                return Err(Error::Malformed.into());
            }
            charge.shrink(resident_bytes(source.rows()));
            self.loads.fetch_add(1, Ordering::SeqCst);
            Ok(self.cache(key, Content::Table(Box::new(loaded)), charge))
        })
    }

    /// `source`'s encoding, reading it if it is not cached, within the budget as
    /// [`Self::get`] prepares a table. An encoding that is not the source's fails.
    pub fn object(&self, source: &ObjectSource) -> Result<Arc<Resident>, LoadError> {
        let key = Key::Object(source.kind, source.digest);
        if let Some(found) = self.touch(&key) {
            return Ok(found);
        }
        self.once(key, || {
            let charge = self.reserve(source.bytes as usize)?;
            let encoding = (source.load)()?;
            if !source.matches(&encoding) {
                return Err(Error::Malformed.into());
            }
            self.loads.fetch_add(1, Ordering::SeqCst);
            Ok(self.cache(key, Content::Object(encoding.into()), charge))
        })
    }

    /// Reserves the [`peak_bytes`] of preparing a `kind` table of `rows` rows outside
    /// the cache, evicting as [`Self::get`] does. While tables in use leave no room it
    /// retries until `wait` has passed, then is [`LoadError::Busy`]. Blocking: call it
    /// from the blocking work that prepares, so the charge lasts as long as that work
    /// even if the request that started it is cancelled.
    pub fn reserve_peak(
        &self,
        kind: Kind,
        rows: u32,
        wait: std::time::Duration,
    ) -> Result<Reservation, LoadError> {
        let deadline = std::time::Instant::now() + wait;
        loop {
            match self.reserve(peak_bytes(kind, rows)) {
                Err(LoadError::Busy) if std::time::Instant::now() < deadline => {
                    std::thread::sleep(std::time::Duration::from_millis(20));
                }
                reserved => return reserved.map(Reservation),
            }
        }
    }

    /// Caches `loaded`, prepared already for `source` under `reservation`, whose charge
    /// shrinks to the table's [`resident_bytes`].
    pub fn insert(
        &self,
        source: &Source,
        loaded: Loaded,
        reservation: Reservation,
    ) -> Arc<Resident> {
        let mut charge = reservation.0;
        charge.shrink(resident_bytes(source.rows()));
        self.cache(
            Key::Table(source.id),
            Content::Table(Box::new(loaded)),
            charge,
        )
    }

    /// The entry for `key`, running `load` only if no concurrent caller is loading it:
    /// one that is makes this one wait and share its cached result, so concurrent misses
    /// for one key reserve and load once. After a failed load the next waiter tries.
    fn once(
        &self,
        key: Key,
        load: impl FnOnce() -> Result<Arc<Resident>, LoadError>,
    ) -> Result<Arc<Resident>, LoadError> {
        let gate = self.loading.lock().unwrap().entry(key).or_default().clone();
        let result = {
            let _turn = gate.lock().unwrap_or_else(|e| e.into_inner());
            match self.touch(&key) {
                Some(found) => Ok(found),
                None => load(),
            }
        };
        let mut loading = self.loading.lock().unwrap();
        // Only the map and this caller hold the gate: no one waits on it.
        if Arc::strong_count(&gate) == 2 {
            loading.remove(&key);
        }
        result
    }

    /// The cached entry for `key`, marked used.
    fn touch(&self, key: &Key) -> Option<Arc<Resident>> {
        let mut entries = self.entries.lock().unwrap();
        entries.clock += 1;
        let clock = entries.clock;
        let (resident, used) = entries.tables.get_mut(key)?;
        *used = clock;
        Some(resident.clone())
    }

    /// Charges `bytes` against the budget, evicting the least recently used idle
    /// entries to make room; see [`Self::get`].
    fn reserve(&self, bytes: usize) -> Result<Charge, LoadError> {
        let mut entries = self.entries.lock().unwrap();
        loop {
            let live = self.live.load(Ordering::SeqCst);
            if live + bytes <= self.budget {
                self.live.fetch_add(bytes, Ordering::SeqCst);
                return Ok(Charge {
                    bytes,
                    live: self.live.clone(),
                });
            }
            let idle = |resident: &Arc<Resident>| Arc::strong_count(resident) == 1;
            let freeable: usize = entries
                .tables
                .values()
                .filter(|(resident, _)| idle(resident))
                .map(|(resident, _)| resident.charge.bytes)
                .sum();
            if live.saturating_sub(freeable) + bytes > self.budget {
                return Err(LoadError::Busy);
            }
            let oldest = entries
                .tables
                .iter()
                .filter(|(_, (resident, _))| idle(resident))
                .min_by_key(|(_, (_, used))| *used)
                .map(|(key, _)| *key);
            match oldest {
                // Dropping the cache's only reference frees an idle entry at once.
                Some(key) => {
                    let evicted = entries.tables.remove(&key);
                    drop(entries);
                    drop(evicted);
                    entries = self.entries.lock().unwrap();
                }
                None => return Err(LoadError::Busy),
            }
        }
    }

    /// Records `content` under `key` with its `charge`, or marks used and returns the
    /// entry already cached there.
    fn cache(&self, key: Key, content: Content, charge: Charge) -> Arc<Resident> {
        let resident = Arc::new(Resident { content, charge });
        let mut entries = self.entries.lock().unwrap();
        entries.clock += 1;
        let clock = entries.clock;
        match entries.tables.get_mut(&key) {
            // A concurrent load won; drop this copy and its charge.
            Some((found, used)) => {
                *used = clock;
                found.clone()
            }
            None => {
                entries.tables.insert(key, (resident.clone(), clock));
                resident
            }
        }
    }
}

#[cfg(test)]
mod tests;
