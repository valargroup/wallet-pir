//! Immutable prepared inventories and atomic, reorg-fenced serving state.
use crate::tables::{Loaded, ObjectSource, Source, Tables};
use axum::http::StatusCode;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock, Weak},
    time::{Duration, Instant},
};
use zakura_pir_query_server::receiver::inventory::FilterRef;
use zakura_pir_query_server::receiver::inventory::{Head, Inventory, Shard};
use zakura_pir_query_server::receiver::session::Error;
use zakura_pir_query_server::receiver::Hash;
use zakura_pir_query_server::receiver::{self, server::Server, Companion, Directory};

/// One segment for serving: its directory and companion sources and its choice object.
#[derive(Clone)]
pub struct Segment {
    pub directory: Arc<Source>,
    pub companion: Arc<Source>,
    pub choice: Arc<ObjectSource>,
}

impl Segment {
    /// A segment over prepared servers kept in memory, for tables with no durable copy.
    pub fn resident(
        directory: Server<Directory>,
        choice: &[u8],
        companion: Server<Companion>,
    ) -> Self {
        let reference = directory.session().manifest.choice;
        Self {
            directory: Arc::new(Source::memory(Loaded::Directory(directory))),
            companion: Arc::new(Source::memory(Loaded::Companion(companion))),
            choice: Arc::new(ObjectSource::memory_choice(&reference, choice)),
        }
    }

    /// The segment publication an inventory lists.
    pub fn publication(&self) -> receiver::Publication {
        receiver::Publication {
            directory: self
                .directory
                .directory_session()
                .expect("a directory")
                .clone(),
            companion: self
                .companion
                .companion_session()
                .expect("a companion")
                .clone(),
        }
    }
}

/// One immutable inventory prepared for serving: its head and chunks, every segment's
/// sessions and choice object, every sealed shard's filter, and the owner's report on
/// the index it was built from. Sessions and objects shared with another inventory
/// share their sources, so their prepared rows and bytes are cached once.
pub struct Publication {
    inventory: Inventory,
    authorized: Arc<std::sync::atomic::AtomicBool>,
    head: Vec<u8>,
    chunks: Vec<Vec<u8>>,
    segments: Vec<Segment>,
    /// Session ID to segment index and whether it is the companion session.
    sessions: HashMap<Hash, (usize, bool)>,
    filters: HashMap<Hash, Arc<ObjectSource>>,
    choices: HashMap<Hash, Arc<ObjectSource>>,
    pub(crate) report: Option<serde_json::Value>,
}

impl Publication {
    /// Prepares `inventory` for serving with `segments`, one per segment in inventory
    /// order, and the `filters` its sealed shards name. The caller has already verified
    /// every record and payload cryptographically and accepted the inventory's chain
    /// anchor; this checks only that the sessions, choice objects and filters are
    /// exactly those the inventory lists, and the chunk bound.
    pub fn verified(
        inventory: Inventory,
        segments: Vec<Segment>,
        filters: Vec<Arc<ObjectSource>>,
    ) -> Result<Self, Error> {
        let listed = inventory.shards().iter().flat_map(|s| &s.segments);
        if listed.clone().count() != segments.len() {
            return Err(Error::Malformed);
        }
        let (mut sessions, mut choices) = (HashMap::new(), HashMap::new());
        for (index, (listed, segment)) in listed.zip(&segments).enumerate() {
            let choice = &listed.directory.manifest.choice;
            if listed.directory.id()? != segment.directory.id()
                || listed.companion.id()? != segment.companion.id()
                || segment.choice.digest() != choice.digest
                || segment.choice.bytes() != choice.bytes
            {
                return Err(Error::Malformed);
            }
            sessions.insert(segment.directory.id(), (index, false));
            sessions.insert(segment.companion.id(), (index, true));
            choices.insert(choice.digest, segment.choice.clone());
        }
        let mut served: HashMap<_, _> = filters.into_iter().map(|f| (f.digest(), f)).collect();
        let mut named = HashMap::new();
        for reference in inventory.shards().iter().filter_map(|s| s.filter.as_ref()) {
            let filter = served.remove(&reference.digest).ok_or(Error::Malformed)?;
            if filter.bytes() != reference.bytes || filter.count() != u64::from(reference.receivers)
            {
                return Err(Error::Malformed);
            }
            named.insert(reference.digest, filter);
        }
        if !served.is_empty() {
            return Err(Error::Malformed);
        }
        let chunks = inventory
            .chunks()?
            .iter()
            .map(serde_json::to_vec)
            .collect::<Result<_, _>>()?;
        Ok(Self {
            head: serde_json::to_vec(inventory.head())?,
            inventory,
            chunks,
            segments,
            sessions,
            filters: named,
            choices,
            report: None,
            authorized: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        })
    }

    /// Lists `shards` in chain order as one inventory and prepares it as [`Self::verified`]
    /// does. Each shard is sealed or not, with its prepared segments, which all cover its
    /// blocks, and its filter; its coverage is its first segment's.
    pub fn assemble(
        genesis: Hash,
        history_revision: u64,
        shards: Vec<(bool, Vec<Segment>, Option<Arc<ObjectSource>>)>,
    ) -> Result<Self, Error> {
        let (mut listed, mut segments, mut filters) = (Vec::new(), Vec::new(), Vec::new());
        for (sealed, prepared, filter) in shards {
            let c = prepared
                .first()
                .and_then(|s| s.directory.directory_session())
                .ok_or(Error::Malformed)?
                .manifest
                .coverage
                .clone();
            listed.push(Shard {
                index: 0,
                sealed,
                start_height: c.start_height,
                start_parent: c.start_parent,
                start_position: c.start_position,
                end_height: c.end_height,
                end_hash: c.end_hash,
                end_position: c.end_position,
                segments: prepared.iter().map(Segment::publication).collect(),
                filter: filter.as_ref().map(|f| FilterRef {
                    digest: f.digest(),
                    bytes: f.bytes(),
                    receivers: f.count() as u32,
                }),
            });
            segments.extend(prepared);
            filters.extend(filter);
        }
        let inventory = Inventory::build(genesis, history_revision, listed)?;
        Self::verified(inventory, segments, filters)
    }

    /// A request holds this token through loading and evaluation. Selective
    /// withdrawal fences only requests using the rejected inventory.
    pub fn authorization(&self) -> Arc<std::sync::atomic::AtomicBool> {
        self.authorized.clone()
    }

    /// Attaches the owner's report, such as feed freshness, which health serves with
    /// this inventory's ID for monitoring, so the two activate together.
    pub fn with_report(mut self, report: serde_json::Value) -> Self {
        self.report = Some(report);
        self
    }

    /// The head that identifies this inventory.
    pub fn head(&self) -> &Head {
        self.inventory.head()
    }

    /// The inventory this publication serves.
    pub fn inventory(&self) -> &Inventory {
        &self.inventory
    }

    /// The prepared segments, in inventory order.
    pub fn segments(&self) -> &[Segment] {
        &self.segments
    }

    /// The inventory identity.
    pub fn id(&self) -> Hash {
        self.inventory.head().inventory
    }

    /// The serialized head.
    pub fn head_bytes(&self) -> &[u8] {
        &self.head
    }

    /// Serialized chunk `index`, if the inventory has it.
    pub fn chunk(&self, index: usize) -> Option<&[u8]> {
        self.chunks.get(index).map(Vec::as_slice)
    }

    /// The source of the shard filter with digest `digest`, if a shard names it.
    pub fn filter(&self, digest: &Hash) -> Option<&Arc<ObjectSource>> {
        self.filters.get(digest)
    }

    /// The source of the choice object with digest `digest`, if a segment names it.
    pub fn choice(&self, digest: &Hash) -> Option<&Arc<ObjectSource>> {
        self.choices.get(digest)
    }

    /// The source of the table whose session ID is `id`, if one of this inventory's
    /// segments has it.
    pub fn table(&self, id: &Hash) -> Option<&Arc<Source>> {
        let (index, companion) = *self.sessions.get(id)?;
        let segment = &self.segments[index];
        Some(match companion {
            false => &segment.directory,
            true => &segment.companion,
        })
    }

    /// The anchor it ends at.
    fn anchor(&self) -> (u32, Hash) {
        (self.head().end_height, self.head().end_hash)
    }

    /// Every block a reorg of which invalidates it: its end and each companion's
    /// construction block later than that. Earlier ones are below the end, so a reorg
    /// replacing them moves the end too.
    pub fn anchors(&self) -> Vec<(u32, Hash)> {
        let mut anchors = vec![self.anchor()];
        for context in self.contexts() {
            if context.0 > self.anchor().0 && !anchors.contains(&context) {
                anchors.push(context);
            }
        }
        anchors
    }

    /// Every distinct companion construction block, for a caller that checks each is
    /// on its chain before activation.
    pub fn contexts(&self) -> Vec<(u32, Hash)> {
        let mut contexts = Vec::new();
        for segment in &self.segments {
            let tree = segment
                .companion
                .companion_session()
                .expect("a companion")
                .manifest
                .tree;
            if !contexts.contains(&(tree.height, tree.hash)) {
                contexts.push((tree.height, tree.hash));
            }
        }
        contexts
    }

    /// A copy sharing every prepared table, with `report`.
    fn reported(&self, report: serde_json::Value) -> Self {
        Self {
            inventory: self.inventory.clone(),
            head: self.head.clone(),
            chunks: self.chunks.clone(),
            segments: self.segments.clone(),
            sessions: self.sessions.clone(),
            filters: self.filters.clone(),
            choices: self.choices.clone(),
            report: Some(report),
            authorized: self.authorized.clone(),
        }
    }
}

/// How long a displaced inventory keeps serving by default: long enough for a wallet to
/// capture it and finish a restore pass of several hundred encrypted queries.
pub const DEFAULT_RETENTION: Duration = Duration::from_secs(600);
/// The default budget for resident prepared tables, within the deployed unit's 7 GiB
/// `MemoryMax` beside preparation, the index and witness paths.
pub const DEFAULT_TABLE_BUDGET: usize = 4 << 30;
/// Most displaced inventories retained at once. A publication waits while this many are
/// still within their retention, so a captured inventory is never dropped early.
pub const MAX_RETAINED: usize = 16;

/// The directory and block hints [`Publications::install_view`] installed.
pub type Installed = (
    Option<Arc<Publication>>,
    Option<Arc<crate::blocks::BlockWindow>>,
);

struct State {
    withdrawal: Option<Arc<dyn Fn() + Send + Sync>>,
    tables: Arc<Tables>,
    epoch: u64,
    history_revision: u64,
    retention: Duration,
    filters: Option<Arc<crate::blocks::BlockWindow>>,
    current: Option<Arc<Publication>>,
    /// Displaced inventories and the end of their retention, oldest first.
    retained: Vec<(Arc<Publication>, Instant)>,
    /// Inventories no longer accepting requests, which requests that selected them may
    /// still hold; see [`Publications::references`].
    released: Vec<Weak<Publication>>,
}

impl State {
    /// The displaced inventories whose retention has not ended.
    fn live_retained(&self) -> impl Iterator<Item = &(Arc<Publication>, Instant)> {
        let now = Instant::now();
        self.retained.iter().filter(move |(_, until)| *until > now)
    }

    /// When a publication may next activate, if the retained set is full.
    fn ready_at(&self) -> Option<Instant> {
        let live: Vec<_> = self.live_retained().map(|(_, until)| *until).collect();
        (live.len() >= MAX_RETAINED).then(|| live.into_iter().min().expect("full"))
    }

    /// The inventories that accept new requests, current first.
    fn live(&self) -> impl Iterator<Item = &Arc<Publication>> {
        self.current
            .iter()
            .chain(self.live_retained().map(|(p, _)| p))
    }

    /// See [`Publications::anchors`].
    fn anchors(&self) -> Vec<(u32, Hash)> {
        self.live()
            .flat_map(|p| p.anchors())
            .chain(
                self.filters
                    .iter()
                    .map(|f| (f.head().tip.height, f.head().tip.hash)),
            )
            .collect()
    }

    /// Drops displaced inventories whose retention has ended, freeing tables no live
    /// inventory shares; queries still running on one keep their own reference.
    fn expire(&mut self) {
        let now = Instant::now();
        let (live, ended) = std::mem::take(&mut self.retained)
            .into_iter()
            .partition(|(_, until)| *until > now);
        self.retained = live;
        self.release(ended.into_iter().map(|(p, _)| p));
    }

    /// Stops serving the block hints, refusing responses still being built from them.
    fn drop_filters(&mut self) {
        if let Some(filters) = self.filters.take() {
            filters.revoke();
        }
    }

    /// See [`Publications::revoke`].
    fn revoke(&mut self) {
        if let Some(withdrawal) = &self.withdrawal {
            withdrawal();
        }
        self.epoch = self.epoch.checked_add(1).expect("recovery epoch exhausted");
        let current = self.current.take();
        let retained = std::mem::take(&mut self.retained);
        self.release(
            current
                .into_iter()
                .chain(retained.into_iter().map(|(p, _)| p)),
        );
        self.drop_filters();
    }

    /// Records `publications`, which no longer accept requests, in case a request still
    /// holds one.
    fn release(&mut self, publications: impl Iterator<Item = Arc<Publication>>) {
        self.released.retain(|p| p.strong_count() > 0);
        self.released
            .extend(publications.map(|p| Arc::downgrade(&p)));
    }
}

/// The current inventory and those displaced within their retention. Revocation also
/// fences in-flight work.
#[derive(Clone)]
pub struct Publications(Arc<RwLock<State>>);

impl Default for Publications {
    /// Retains displaced inventories for [`DEFAULT_RETENTION`].
    fn default() -> Self {
        Self::with_retention(DEFAULT_RETENTION)
    }
}

impl Publications {
    /// Serving state that keeps each displaced inventory, with every session, chunk and
    /// filter it names, for `retention` after it is replaced, and the default table budget.
    pub fn with_retention(retention: Duration) -> Self {
        Self::new(retention, DEFAULT_TABLE_BUDGET)
    }

    /// Serving state retaining displaced inventories for `retention` and keeping at most
    /// `budget` bytes of prepared tables resident; see [`Tables`].
    pub fn new(retention: Duration, budget: usize) -> Self {
        Self(Arc::new(RwLock::new(State {
            withdrawal: None,
            tables: Arc::new(Tables::new(budget)),
            epoch: 0,
            history_revision: 0,
            retention,
            filters: None,
            current: None,
            retained: Vec::new(),
            released: Vec::new(),
        })))
    }

    /// The prepared table cache every inventory's sessions are served from.
    pub fn tables(&self) -> Arc<Tables> {
        self.0.read().unwrap().tables.clone()
    }

    /// Publisher-owned durable withdrawal, executed under the same guard that
    /// fences local answers. The callback must never reenter this owner.
    pub fn on_withdrawal(&self, callback: Arc<dyn Fn() + Send + Sync>) {
        self.0.write().unwrap().withdrawal = Some(callback);
    }

    /// Commit external authority only while the captured preparation epoch is
    /// still current. Revocation cannot interleave with the guarded operation.
    pub fn in_epoch<T>(&self, expected: u64, commit: impl FnOnce() -> T) -> Option<T> {
        let state = self.0.read().unwrap();
        (state.epoch == expected).then(commit)
    }

    /// Every table session ID and shard filter digest that a live inventory names, or
    /// a displaced one that a request selected before it left service still holds: what
    /// an owner of their durable copies must keep. A request that has selected a table
    /// may still have to load it from its files, which are released for deletion once
    /// the last such request finishes.
    pub fn references(&self) -> std::collections::HashSet<Hash> {
        self.expire();
        let state = self.0.read().unwrap();
        let held: Vec<_> = state.released.iter().filter_map(Weak::upgrade).collect();
        // Every retained inventory counts, even one whose retention ended since the
        // expiry above, as it has not been released yet.
        let retained = state.retained.iter().map(|(p, _)| p);
        state
            .current
            .iter()
            .chain(retained)
            .chain(&held)
            .flat_map(|p| {
                p.sessions
                    .keys()
                    .chain(p.filters.keys())
                    .chain(p.choices.keys())
                    .copied()
            })
            .collect()
    }

    /// Changes the correction generation before collecting, immediately revoking older
    /// sessions so direct-PIR callers cannot finish on stale negatives during preparation.
    pub fn advance_history_revision(&self, revision: u64) -> bool {
        let mut state = self.0.write().unwrap();
        if revision < state.history_revision {
            return false;
        }
        if revision > state.history_revision {
            state.revoke();
            state.history_revision = revision;
        }
        true
    }

    /// Activates hints before PIR preparation, fenced by the same canonical guard.
    /// The serial owner checks the new anchor independently before calling this.
    pub fn publish_filters(
        &self,
        filters: crate::blocks::BlockWindow,
        expected_epoch: u64,
    ) -> bool {
        let mut state = self.0.write().unwrap();
        if state.epoch != expected_epoch
            || filters.head().revision != state.history_revision
            || state.filters.as_ref().is_some_and(|old| {
                old.head().tip.height > filters.head().tip.height
                    || old.head().revision > filters.head().revision
            })
        {
            return false;
        }
        state.filters = Some(Arc::new(filters));
        true
    }

    /// The rolling window served now, if any; a revocation clears it.
    pub fn filter_window(&self) -> Option<Arc<crate::blocks::BlockWindow>> {
        self.0.read().unwrap().filters.clone()
    }

    /// Install one fully authorized HTTPS view atomically. All refusals happen
    /// before changing serving state; the lifecycle has already fenced revisions
    /// that invalidate earlier answers. Returning the installed directory lets
    /// its owner retain files until the final request releases it, and returning
    /// the installed hints lets it revoke them if the view is later rejected.
    pub fn install_view(
        &self,
        directory: Option<Publication>,
        filters: Option<crate::blocks::BlockWindow>,
        revision: u64,
        expected_epoch: u64,
    ) -> Result<Installed, &'static str> {
        let mut state = self.0.write().unwrap();
        if state.epoch != expected_epoch || revision < state.history_revision {
            return Err("Receiver view was fenced");
        }
        if directory
            .as_ref()
            .is_some_and(|p| p.head().history_revision != revision)
            || filters
                .as_ref()
                .is_some_and(|f| f.head().revision != revision)
        {
            return Err("Receiver view revisions differ");
        }
        let correction = revision > state.history_revision;
        if !correction
            && (directory.as_ref().is_some_and(|p| {
                state.current.as_ref().is_some_and(|old| {
                    old.head().end_height > p.head().end_height
                        || (old.id() != p.id() && state.ready_at().is_some())
                })
            }) || filters.as_ref().is_some_and(|f| {
                state
                    .filters
                    .as_ref()
                    .is_some_and(|old| old.head().tip.height > f.head().tip.height)
            }))
        {
            return Err("Receiver view advanced during preparation or retention is full");
        }
        if correction {
            state.revoke();
            state.history_revision = revision;
        }
        state.expire();
        if let Some(directory) = directory {
            let directory = Arc::new(directory);
            if let Some(old) = state.current.replace(directory) {
                if state.current.as_ref().unwrap().id() == old.id() {
                    state.release(std::iter::once(old));
                } else {
                    let until = Instant::now() + state.retention;
                    state.retained.push((old, until));
                }
            }
        }
        let filters = filters.map(Arc::new);
        if let Some(filters) = &filters {
            state.filters = Some(filters.clone());
        }
        Ok((state.current.clone(), filters))
    }

    /// The current block hints, whether or not a PIR publication is ready, with the
    /// recovery epoch, read together so a response built afterwards can be refused if a
    /// revocation came in between.
    pub fn filters_in_epoch(&self) -> Result<(Arc<crate::blocks::BlockWindow>, u64), StatusCode> {
        let state = self.0.read().unwrap();
        let window = state
            .filters
            .clone()
            .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
        Ok((window, state.epoch))
    }

    /// Runs [`State::expire`]. Every read path calls it, including the owner's periodic
    /// [`Self::anchors`] check, so an idle service frees expired inventories within a
    /// poll; [`Self::publish`] expires under its own lock.
    fn expire(&self) {
        let state = self.0.read().unwrap();
        let now = Instant::now();
        if state.retained.iter().any(|(_, until)| *until <= now) {
            drop(state);
            self.0.write().unwrap().expire();
        }
    }

    /// The highest history correction revision accepted so far.
    pub fn history_revision(&self) -> u64 {
        self.0.read().unwrap().history_revision
    }

    /// The recovery epoch. Every revocation advances it.
    pub fn epoch(&self) -> u64 {
        self.0.read().unwrap().epoch
    }

    /// The current inventory's anchor, excluding filter-only coverage and retained ones.
    pub fn current_anchor(&self) -> Option<(u32, Hash)> {
        self.0.read().unwrap().current.as_ref().map(|p| p.anchor())
    }

    /// Anchors that still accept new requests, current first. Every dependency of an
    /// inventory, witness completion blocks included, is at or below its anchor, so a
    /// reorg that changes any of them moves the anchor off the chain. Validate
    /// independently of expensive preparation.
    pub fn anchors(&self) -> Vec<(u32, Hash)> {
        self.serving().1
    }

    /// The recovery epoch and [`Self::anchors`], read together for
    /// [`Self::revoke_serving`].
    pub fn serving(&self) -> (u64, Vec<(u32, Hash)>) {
        self.expire();
        let state = self.0.read().unwrap();
        (state.epoch, state.anchors())
    }

    /// The current inventory's ID with its report, and the recovery epoch, read together
    /// so health never pairs one state's inventory with another's epoch.
    pub fn health(&self) -> (Option<(Hash, Option<serde_json::Value>)>, u64) {
        let state = self.0.read().unwrap();
        let serving = state.current.as_ref().map(|p| (p.id(), p.report.clone()));
        (serving, state.epoch)
    }

    /// When the next inventory may activate, if [`MAX_RETAINED`] displaced ones are still
    /// within their retention. Activating only once one expires keeps every captured
    /// inventory for its full retention.
    pub fn ready_at(&self) -> Option<Instant> {
        self.expire();
        self.0.read().unwrap().ready_at()
    }

    /// Install only if no revocation occurred since the caller began preparing and
    /// validating, the retained set has room (see [`Self::ready_at`]), and the current
    /// inventory ends no higher: within one epoch the chain only extends, so a lower end
    /// comes from a preparation that a newer publish overtook. An inventory with the
    /// current one's ID replaces it in place, keeping the epoch and the retained set. It
    /// expects one serial publisher (the indexer awaits each publish), so equal-height
    /// preparations need no further ordering. The caller must verify the new canonical
    /// anchor and revoke before replacing forked coverage.
    pub fn publish(&self, publication: Publication, expected_epoch: u64) -> bool {
        let mut state = self.0.write().unwrap();
        if state.epoch != expected_epoch {
            return false;
        }
        let revision = publication.head().history_revision;
        if revision < state.history_revision {
            return false;
        }
        if revision > state.history_revision {
            state.revoke();
            state.history_revision = revision;
        }
        state.expire();
        if state
            .current
            .as_ref()
            .is_some_and(|p| p.id() == publication.id())
        {
            let replaced = state.current.replace(Arc::new(publication));
            state.release(replaced.into_iter());
            return true;
        }
        if state.ready_at().is_some()
            || state
                .current
                .as_ref()
                .is_some_and(|p| p.head().end_height > publication.head().end_height)
        {
            return false;
        }
        let until = Instant::now() + state.retention;
        if let Some(previous) = state.current.take() {
            state.retained.push((previous, until));
        }
        state.current = Some(Arc::new(publication));
        true
    }

    /// Replaces the report of the current inventory anchored at `anchor`, sharing its
    /// prepared tables and keeping its ID, epoch and the retained set. Refuses, returning
    /// false, after a revocation since `expected_epoch` or once another inventory is
    /// current.
    pub fn update_report(
        &self,
        anchor: (u32, Hash),
        report: serde_json::Value,
        expected_epoch: u64,
    ) -> bool {
        let mut state = self.0.write().unwrap();
        let Some(current) = state.current.as_ref().filter(|p| p.anchor() == anchor) else {
            return false;
        };
        if state.epoch != expected_epoch {
            return false;
        }
        let updated = Arc::new(current.reported(report));
        let replaced = state.current.replace(updated);
        state.release(replaced.into_iter());
        true
    }

    /// Withdraw only the named inventories and rolling window. The owner has
    /// already removed identities that an independently approved view still uses.
    pub fn invalidate(
        &self,
        inventories: &std::collections::HashSet<Hash>,
        filters: Option<&zakura_pir_query_server::receiver::blocks::Head>,
    ) {
        let mut state = self.0.write().unwrap();
        let held: Vec<_> = state.released.iter().filter_map(Weak::upgrade).collect();
        for publication in state
            .current
            .iter()
            .chain(state.retained.iter().map(|(p, _)| p))
            .chain(held.iter())
        {
            if inventories.contains(&publication.id()) {
                publication
                    .authorized
                    .store(false, std::sync::atomic::Ordering::Release);
            }
        }
        let removed_current = if state
            .current
            .as_ref()
            .is_some_and(|p| inventories.contains(&p.id()))
        {
            state.current.take()
        } else {
            None
        };
        let (removed, kept): (Vec<_>, Vec<_>) = std::mem::take(&mut state.retained)
            .into_iter()
            .partition(|(p, _)| inventories.contains(&p.id()));
        state.retained = kept;
        state.release(
            removed_current
                .into_iter()
                .chain(removed.into_iter().map(|(p, _)| p)),
        );
        if filters.is_some_and(|head| state.filters.as_ref().is_some_and(|f| f.head() == head)) {
            state.drop_filters();
        }
    }

    /// Conservatively invalidate all sessions, including CPU work that started before the reorg.
    pub fn revoke(&self) {
        self.0.write().unwrap().revoke();
    }

    /// For a check that read `epoch` and `anchor` from [`Self::serving`] and found the
    /// anchor off the chain: revokes as [`Self::revoke`] does, but atomically only if
    /// `epoch` is still current and `anchor` still accepts requests, so a publication
    /// made after a revocation, or after `anchor`'s retention ended, survives the stale
    /// result. A retained anchor still revokes every session. Returns whether it revoked.
    pub fn revoke_serving(&self, epoch: u64, anchor: (u32, Hash)) -> bool {
        let mut state = self.0.write().unwrap();
        if state.epoch != epoch || !state.anchors().contains(&anchor) {
            return false;
        }
        state.revoke();
        true
    }

    /// The live inventory holding session `id`, directory or companion, with the current
    /// epoch. An id not served now, whether unknown, revoked or expired, is `GONE`.
    pub fn select(&self, id: &Hash) -> Result<(Arc<Publication>, u64), StatusCode> {
        self.find(|p| p.table(id).is_some())
    }

    /// The live inventory `inventory` names, with the current epoch, or `GONE`.
    pub fn inventory(&self, inventory: &Hash) -> Result<(Arc<Publication>, u64), StatusCode> {
        self.find(|p| p.id() == *inventory)
    }

    /// A live inventory naming shard filter `digest`, with the current epoch, or `GONE`.
    pub fn with_filter(&self, digest: &Hash) -> Result<(Arc<Publication>, u64), StatusCode> {
        self.find(|p| p.filters.contains_key(digest))
    }

    /// A live inventory naming choice object `digest`, with the current epoch, or `GONE`.
    pub fn with_choice(&self, digest: &Hash) -> Result<(Arc<Publication>, u64), StatusCode> {
        self.find(|p| p.choices.contains_key(digest))
    }

    /// The inventories that accept new requests, current first.
    pub fn live(&self) -> Vec<Arc<Publication>> {
        self.expire();
        self.0.read().unwrap().live().cloned().collect()
    }

    /// The current inventory, or `SERVICE_UNAVAILABLE` before one is published.
    pub fn current(&self) -> Result<Arc<Publication>, StatusCode> {
        self.0
            .read()
            .unwrap()
            .current
            .clone()
            .ok_or(StatusCode::SERVICE_UNAVAILABLE)
    }

    /// The first live inventory, current first, that `found` accepts.
    fn find(
        &self,
        found: impl Fn(&Publication) -> bool,
    ) -> Result<(Arc<Publication>, u64), StatusCode> {
        self.expire();
        let state = self.0.read().unwrap();
        let selected = state.live().find(|p| found(p)).cloned();
        selected.map(|p| (p, state.epoch)).ok_or(StatusCode::GONE)
    }
}

#[cfg(test)]
mod tests;
