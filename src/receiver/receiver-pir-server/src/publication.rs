//! Immutable prepared inventories and atomic, reorg-fenced serving state.
use crate::tables::{FilterSource, Loaded, Source, Tables};
use axum::http::StatusCode;
use receiver_directory::Hash;
use receiver_pir::{
    inventory::{FilterRef, Head, Inventory, Shard},
    server::{Server, WitnessServer},
};
use std::{
    collections::HashMap,
    sync::{Arc, RwLock, Weak},
    time::{Duration, Instant},
};

/// One segment for serving: the sources of its payment session and of the companion
/// witness session bound to it.
#[derive(Clone)]
pub struct Segment {
    pub payments: Arc<Source>,
    pub witnesses: Arc<Source>,
}

impl Segment {
    /// A segment over prepared servers kept in memory, for tables with no durable copy.
    pub fn resident(server: Server, witnesses: WitnessServer) -> Self {
        Self {
            payments: Arc::new(Source::memory(Loaded::Payments(server))),
            witnesses: Arc::new(Source::memory(Loaded::Witnesses(witnesses))),
        }
    }

    /// The payment and witness session manifests an inventory lists.
    pub fn publication(&self) -> receiver_pir::Publication {
        receiver_pir::Publication {
            payments: self
                .payments
                .payment_manifest()
                .expect("a payment source")
                .clone(),
            witnesses: self
                .witnesses
                .witness_manifest()
                .expect("a witness source")
                .clone(),
        }
    }
}

/// One immutable inventory prepared for serving: its head and chunks, every segment's
/// sessions, every sealed shard's filter, and the owner's report on the index it was
/// built from. Sessions and filters shared with another inventory share their sources,
/// so their prepared rows and filter bytes are cached once.
pub struct Publication {
    inventory: Inventory,
    head: Vec<u8>,
    chunks: Vec<Vec<u8>>,
    segments: Vec<Segment>,
    /// Session ID to segment index and whether it is the witness session.
    sessions: HashMap<Hash, (usize, bool)>,
    filters: HashMap<Hash, Arc<FilterSource>>,
    pub(crate) report: Option<serde_json::Value>,
}

impl Publication {
    /// Prepares `inventory` for serving with `segments`, one per segment in inventory
    /// order, and the `filters` its sealed shards name. The caller has already verified
    /// every record and witness payload cryptographically and accepted the inventory's
    /// chain anchor; this checks only that the sessions and filters are exactly those
    /// the inventory lists, and the chunk bound.
    pub fn verified(
        inventory: Inventory,
        segments: Vec<Segment>,
        filters: Vec<Arc<FilterSource>>,
    ) -> Result<Self, receiver_pir::Error> {
        use receiver_pir::Error::Malformed;
        let listed = inventory.shards().iter().flat_map(|s| &s.segments);
        if listed.clone().count() != segments.len() {
            return Err(Malformed);
        }
        let mut sessions = HashMap::new();
        for (index, (listed, segment)) in listed.zip(&segments).enumerate() {
            if listed.payments.id()? != segment.payments.id()
                || listed.witnesses.id()? != segment.witnesses.id()
                || segment.payments.payment_manifest().is_none()
                || segment.witnesses.witness_manifest().is_none()
            {
                return Err(Malformed);
            }
            sessions.insert(segment.payments.id(), (index, false));
            sessions.insert(segment.witnesses.id(), (index, true));
        }
        let mut served = HashMap::new();
        for filter in filters {
            served.insert(filter.digest(), filter);
        }
        let mut named = HashMap::new();
        for reference in inventory.shards().iter().filter_map(|s| s.filter.as_ref()) {
            let filter = served.remove(&reference.digest).ok_or(Malformed)?;
            if filter.bytes() != reference.bytes
                || filter.receivers() != u64::from(reference.receivers)
            {
                return Err(Malformed);
            }
            named.insert(reference.digest, filter);
        }
        if !served.is_empty() {
            return Err(Malformed);
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
            report: None,
        })
    }

    /// Lists `shards` in chain order as one inventory and prepares it as [`Self::verified`]
    /// does. Each shard is sealed or not, with its prepared segments, which all cover its
    /// blocks, and its filter; its coverage is its first segment's.
    pub fn assemble(
        genesis: Hash,
        history_revision: u64,
        shards: Vec<(bool, Vec<Segment>, Option<Arc<FilterSource>>)>,
    ) -> Result<Self, receiver_pir::Error> {
        let (mut listed, mut segments, mut filters) = (Vec::new(), Vec::new(), Vec::new());
        for (sealed, prepared, filter) in shards {
            let m = prepared
                .first()
                .and_then(|s| s.payments.payment_manifest())
                .ok_or(receiver_pir::Error::Malformed)?
                .directory
                .clone();
            listed.push(Shard {
                index: 0,
                sealed,
                start_height: m.start_height,
                start_parent: m.start_parent,
                start_position: m.start_position,
                end_height: m.end_height,
                end_hash: m.end_hash,
                end_position: m.end_position,
                segments: prepared.iter().map(Segment::publication).collect(),
                filter: filter.as_ref().map(|f| FilterRef {
                    digest: f.digest(),
                    bytes: f.bytes(),
                    receivers: f.receivers() as u32,
                }),
            });
            segments.extend(prepared);
            filters.extend(filter);
        }
        let inventory = Inventory::build(genesis, history_revision, listed)?;
        Self::verified(inventory, segments, filters)
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
    pub(crate) fn head_bytes(&self) -> &[u8] {
        &self.head
    }

    /// Serialized chunk `index`, if the inventory has it.
    pub(crate) fn chunk(&self, index: usize) -> Option<&[u8]> {
        self.chunks.get(index).map(Vec::as_slice)
    }

    /// The source of the shard filter with digest `digest`, if a shard names it.
    pub fn filter(&self, digest: &Hash) -> Option<&Arc<FilterSource>> {
        self.filters.get(digest)
    }

    /// Bytes its tables and filters keep resident outside the table budget: none unless a
    /// source holds its state in memory (see [`Source::memory`]).
    pub fn held_bytes(&self) -> usize {
        let tables = self
            .segments
            .iter()
            .map(|s| s.payments.held_bytes() + s.witnesses.held_bytes());
        tables.sum::<usize>() + self.filters.values().map(|f| f.held_bytes()).sum::<usize>()
    }

    /// The source of the table whose session ID is `id`, if one of this inventory's
    /// segments has it.
    pub(crate) fn table(&self, id: &Hash) -> Option<&Arc<Source>> {
        let (index, witness) = *self.sessions.get(id)?;
        let segment = &self.segments[index];
        Some(match witness {
            false => &segment.payments,
            true => &segment.witnesses,
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
            let table = &segment
                .witnesses
                .witness_manifest()
                .expect("a witness source")
                .table;
            let context = (table.tree_height, table.tree_hash);
            if !contexts.contains(&context) {
                contexts.push(context);
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
            report: Some(report),
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

struct State {
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

    /// See [`Publications::revoke`].
    fn revoke(&mut self) {
        self.epoch = self.epoch.checked_add(1).expect("recovery epoch exhausted");
        let current = self.current.take();
        let retained = std::mem::take(&mut self.retained);
        self.release(
            current
                .into_iter()
                .chain(retained.into_iter().map(|(p, _)| p)),
        );
        self.filters = None;
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
            .flat_map(|p| p.sessions.keys().chain(p.filters.keys()).copied())
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

    /// The current rolling window, whether or not a PIR publication is ready.
    pub(crate) fn filters(&self) -> Result<Arc<crate::blocks::BlockWindow>, StatusCode> {
        self.0
            .read()
            .unwrap()
            .filters
            .clone()
            .ok_or(StatusCode::SERVICE_UNAVAILABLE)
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
    pub(crate) fn health(&self) -> (Option<(Hash, Option<serde_json::Value>)>, u64) {
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

    /// The live inventory holding session `id`, payment or witness, with the current
    /// epoch. An id not served now, whether unknown, revoked or expired, is `GONE`.
    pub(crate) fn select(&self, id: &Hash) -> Result<(Arc<Publication>, u64), StatusCode> {
        self.find(|p| p.table(id).is_some())
    }

    /// The live inventory `inventory` names, with the current epoch, or `GONE`.
    pub(crate) fn inventory(
        &self,
        inventory: &Hash,
    ) -> Result<(Arc<Publication>, u64), StatusCode> {
        self.find(|p| p.id() == *inventory)
    }

    /// A live inventory naming shard filter `digest`, with the current epoch, or `GONE`.
    pub(crate) fn with_filter(&self, digest: &Hash) -> Result<(Arc<Publication>, u64), StatusCode> {
        self.find(|p| p.filters.contains_key(digest))
    }

    /// The inventories that accept new requests, current first.
    pub fn live(&self) -> Vec<Arc<Publication>> {
        self.expire();
        self.0.read().unwrap().live().cloned().collect()
    }

    /// The current inventory, or `SERVICE_UNAVAILABLE` before one is published.
    pub(crate) fn current(&self) -> Result<Arc<Publication>, StatusCode> {
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
mod tests {
    use super::*;

    /// An empty prepared publication whose salt starts with `salt`.
    #[cfg(feature = "qualification-tests")]
    fn publication(salt: u8) -> Publication {
        publication_at(salt, 3)
    }

    /// An empty prepared publication whose salt starts with `salt`, ending at a block
    /// hash of `hash` bytes.
    fn publication_at(salt: u8, hash: u8) -> Publication {
        let snapshot = if salt == hash {
            crate::empty_snapshot(salt)
        } else {
            let mut manifest = crate::common::manifest(receiver_pir::MIN_ROWS);
            manifest.salt[0] = salt;
            manifest.end_hash = [hash; 32];
            receiver_directory::snapshot::Snapshot::build(manifest, &[]).unwrap()
        };
        crate::prepared::prepare_payments(snapshot).unwrap()
    }

    /// A check that found an anchor off the chain revokes only while its epoch is
    /// current and the anchor still accepts requests.
    #[test]
    fn a_stale_check_revokes_only_while_its_anchor_is_served() {
        let (a, b) = ((101, [1; 32]), (101, [2; 32]));
        let publications = Publications::default();
        assert!(publications.publish(publication_at(1, 1), 0));
        // A check reads A, then a revocation and B's publication overtake it.
        let (epoch, anchors) = publications.serving();
        assert_eq!(anchors, [a]);
        publications.revoke();
        assert!(publications.publish(publication_at(2, 2), epoch + 1));
        assert!(!publications.revoke_serving(epoch, a));
        assert_eq!(publications.serving(), (epoch + 1, vec![b]));
        // An ordinary rotation keeps the epoch, but once A's retention ends a check that
        // read A no longer revokes the publication that replaced it. Reuse B's prepared
        // table rather than preprocess a third empty one.
        let publications = Publications::default();
        assert!(publications.publish(publication_at(1, 1), 0));
        let (epoch, _) = publications.serving();
        assert!(publications.publish(publication_at(2, 2), 0));
        let ended = Instant::now()
            .checked_sub(Duration::from_millis(1))
            .unwrap();
        publications.0.write().unwrap().retained[0].1 = ended;
        assert!(!publications.revoke_serving(epoch, a));
        assert_eq!(publications.anchors(), [b]);
        // A served anchor off the chain still revokes every session.
        assert!(publications.revoke_serving(epoch, b));
        assert_eq!(publications.serving(), (epoch + 1, vec![]));
    }

    /// A preparation overtaken by a newer publish cannot replace it, even once the
    /// newer one's predecessor has left its retention. Fast already refuses a stale
    /// check once a newer anchor is current.
    #[cfg(feature = "qualification-tests")]
    #[test]
    fn an_overtaken_preparation_is_not_published() {
        let publications = Publications::default();
        assert!(publications.publish(publication_at(1, 1), 0));
        let mut newer = crate::common::manifest(receiver_pir::MIN_ROWS);
        (newer.end_height, newer.end_hash) = (102, [2; 32]);
        let snapshot = receiver_directory::snapshot::Snapshot::build(newer, &[]).unwrap();
        let newer = crate::prepared::prepare_payments(snapshot).unwrap();
        assert!(publications.publish(newer, 0));
        let ended = Instant::now()
            .checked_sub(Duration::from_millis(1))
            .unwrap();
        publications.0.write().unwrap().retained[0].1 = ended;
        assert!(!publications.publish(publication_at(3, 3), 0));
        assert_eq!(publications.anchors(), [(102, [2; 32])]);
    }

    /// A retained anchor revokes the current inventory with it.
    #[test]
    fn a_retained_anchor_still_revokes_every_session() {
        let publications = Publications::default();
        assert!(publications.publish(publication_at(1, 1), 0));
        let (epoch, _) = publications.serving();
        assert!(publications.publish(publication_at(2, 2), 0));
        assert!(publications.revoke_serving(epoch, (101, [1; 32])));
        assert!(publications.anchors().is_empty());
    }

    /// A displaced inventory is dropped once its retention ends, not kept until the next
    /// publication, and its id is then 410. Fast already ends that retention on the
    /// stale-anchor runtime.
    #[cfg(feature = "qualification-tests")]
    #[test]
    fn an_expired_previous_publication_is_dropped() {
        let publications = Publications::default();
        assert!(publications.publish(publication(1), 0));
        let first = Arc::downgrade(publications.0.read().unwrap().current.as_ref().unwrap());
        let id = first.upgrade().unwrap().segments()[0].payments.id();
        assert!(publications.publish(publication(2), 0));
        assert!(publications.select(&id).is_ok());
        assert!(first.upgrade().is_some());
        let ended = Instant::now()
            .checked_sub(Duration::from_millis(1))
            .unwrap();
        publications.0.write().unwrap().retained[0].1 = ended;
        assert_eq!(publications.anchors().len(), 1);
        assert!(first.upgrade().is_none());
        assert!(matches!(publications.select(&id), Err(StatusCode::GONE)));
    }

    /// Republishing the current inventory replaces it without a new retention or epoch, keeps
    /// a predecessor's deadline, and still honors the epoch fence.
    #[cfg(feature = "qualification-tests")]
    #[test]
    fn republishing_the_current_session_replaces_it_in_place() {
        let publications = Publications::default();
        assert!(publications.publish(publication(1), 0));
        assert!(!publications.publish(publication(1), 1));
        assert!(publications.publish(publication(1), 0));
        assert_eq!(publications.epoch(), 0);
        assert_eq!(publications.anchors().len(), 1);
        // A new inventory activates at once and retains its predecessor.
        assert!(publications.publish(publication(2), 0));
        assert!(publications.publish(publication(2), 0));
        assert!(publications.ready_at().is_none());
        assert_eq!(publications.anchors().len(), 2);
    }

    /// Once [`MAX_RETAINED`] displaced inventories are within their retention, the next
    /// publication waits for the oldest to expire instead of dropping it early.
    #[test]
    fn a_full_retained_set_holds_back_the_next_publication() {
        let publications = Publications::default();
        // Prepare before arming retention. A unique manifest built afterward can
        // outlast the deadline, and `publish` then expires the set it should refuse.
        let newer = publication_at(2, 2);
        assert!(publications.publish(publication_at(1, 1), 0));
        let held = publications.current().unwrap();
        let until = Instant::now() + Duration::from_secs(5);
        publications.0.write().unwrap().retained = vec![(held, until); MAX_RETAINED];
        assert_eq!(publications.ready_at(), Some(until));
        assert!(!publications.publish(newer, 0));
        publications.0.write().unwrap().retained[0].1 = Instant::now();
        assert!(publications.ready_at().is_none());
    }
    /// A report update keeps the current session and its prepared PIR state, and is
    /// refused for another anchor or after a revocation.
    #[test]
    fn a_report_update_keeps_the_prepared_session() {
        let publications = Publications::default();
        assert!(publications.publish(publication_at(1, 1), 0));
        let (before, _) = publications
            .current()
            .map(|p| (p, publications.epoch()))
            .unwrap();
        let anchor = (101, [1; 32]);
        assert!(!publications.update_report((101, [2; 32]), serde_json::json!(1), 0));
        assert!(!publications.update_report(anchor, serde_json::json!(1), 1));
        assert!(publications.update_report(anchor, serde_json::json!(2), 0));
        let (after, epoch) = publications
            .current()
            .map(|p| (p, publications.epoch()))
            .unwrap();
        assert_eq!((after.id(), epoch), (before.id(), 0));
        assert!(std::ptr::eq(
            before.segments()[0].payments.public().as_ptr(),
            after.segments()[0].payments.public().as_ptr()
        ));
        assert_eq!(
            publications.health().0,
            Some((before.id(), Some(serde_json::json!(2))))
        );
        assert!(publications.ready_at().is_none());
        publications.revoke();
        assert!(!publications.update_report(anchor, serde_json::json!(3), 0));
    }
    /// A companion constructed at a later block than its payment table makes that block
    /// a served anchor too, so finding it off the chain revokes every session.
    #[cfg(feature = "qualification-tests")]
    #[test]
    fn a_later_witness_context_is_a_served_anchor() {
        let manifest = crate::common::manifest(receiver_pir::MIN_ROWS);
        let snapshot = receiver_directory::snapshot::Snapshot::build(manifest, &[]).unwrap();
        let mut table = crate::prepared::placeholder(&snapshot).manifest;
        (table.tree_height, table.tree_hash) = (105, [5; 32]);
        let table = receiver_directory::witness::Table::build(table, &[]).unwrap();
        let publication = crate::prepared::prepare(snapshot, table).unwrap();
        let publications = Publications::default();
        assert!(publications.publish(publication, 0));
        let (epoch, anchors) = publications.serving();
        assert_eq!(anchors, [(101, [3; 32]), (105, [5; 32])]);
        assert!(publications.revoke_serving(epoch, (105, [5; 32])));
        assert!(publications.anchors().is_empty());
    }

    /// A new companion beside an unchanged payment session displaces the old one
    /// through the ordinary retention, so a wallet that captured the old pair can finish.
    #[cfg(feature = "qualification-tests")]
    #[test]
    fn a_witness_only_replacement_keeps_the_displaced_companion() {
        let manifest = crate::common::manifest(receiver_pir::MIN_ROWS);
        let snapshot =
            receiver_directory::snapshot::Snapshot::build(manifest, &[crate::common::record(0, 1)])
                .unwrap();
        let first = crate::prepared::prepare(
            snapshot.clone(),
            crate::prepared::companion_table(&snapshot, &[[1; 512]]),
        )
        .unwrap();
        let second = crate::prepared::prepare(
            snapshot.clone(),
            crate::prepared::companion_table(&snapshot, &[[2; 512]]),
        )
        .unwrap();
        let witness = |p: &Publication| p.segments()[0].witnesses.id();
        let (old, new) = (witness(&first), witness(&second));
        assert_eq!(
            first.segments()[0].payments.id(),
            second.segments()[0].payments.id()
        );
        assert_ne!(old, new);
        assert_ne!(first.id(), second.id());
        let publications = Publications::default();
        assert!(publications.publish(first, 0));
        assert!(publications.publish(second, 0));
        assert!(publications.select(&old).is_ok());
        assert!(publications.select(&new).is_ok());
    }

    /// An inventory a request selected keeps naming its tables after it leaves service,
    /// whether its retention ended or a revocation removed it, so their files outlive the
    /// request; once the request releases it, its tables are no longer referenced.
    #[test]
    fn a_held_inventory_stays_referenced_until_released() {
        let publications = Publications::default();
        assert!(publications.publish(publication_at(1, 1), 0));
        let first = publications.current().unwrap().segments()[0].payments.id();
        let (held, _) = publications.select(&first).unwrap();
        assert!(publications.publish(publication_at(2, 2), 0));
        publications.0.write().unwrap().retained[0].1 = Instant::now();
        assert!(matches!(publications.select(&first), Err(StatusCode::GONE)));
        assert!(publications.references().contains(&first));
        drop(held);
        assert!(!publications.references().contains(&first));
        // An instance a report update replaced stays referenced while held, through a
        // later displacement and expiry of its replacement.
        let middle = publications.current().unwrap().segments()[0].payments.id();
        let (held, _) = publications.select(&middle).unwrap();
        assert!(publications.update_report((101, [2; 32]), serde_json::json!(1), 0));
        // Same prepared inventory as the first publication: a different id from the
        // current one, so it displaces rather than replacing in place, and the
        // payment table is the one already cached.
        assert!(publications.publish(publication_at(1, 1), 0));
        publications
            .0
            .write()
            .unwrap()
            .retained
            .last_mut()
            .unwrap()
            .1 = Instant::now();
        assert!(matches!(
            publications.select(&middle),
            Err(StatusCode::GONE)
        ));
        assert!(publications.references().contains(&middle));
        drop(held);
        assert!(!publications.references().contains(&middle));
        let second = publications.current().unwrap().segments()[0].payments.id();
        let (held, _) = publications.select(&second).unwrap();
        publications.revoke();
        assert!(publications.references().contains(&second));
        drop(held);
        assert!(publications.references().is_empty());
    }

    /// A correction fences direct lookups before the replacement PIR table is ready.
    #[cfg(feature = "qualification-tests")]
    #[test]
    fn correction_revokes_old_sessions_and_refuses_old_preparations() {
        let publications = Publications::default();
        let old = publication(1);
        let old_id = old.segments()[0].payments.id();
        assert!(publications.publish(old, 0));
        assert!(publications.advance_history_revision(1));
        assert!(publications.select(&old_id).is_err());
        assert!(publications
            .current()
            .map(|p| (p, publications.epoch()))
            .is_err());
        assert!(!publications.advance_history_revision(0));
        assert!(!publications.publish(publication(2), publications.epoch()));
        let mut manifest = crate::common::manifest(receiver_pir::MIN_ROWS);
        manifest.history_revision = 1;
        let snapshot = receiver_directory::snapshot::Snapshot::build(manifest, &[]).unwrap();
        let corrected = crate::prepared::prepare_payments(snapshot).unwrap();
        assert!(publications.publish(corrected, publications.epoch()));
        assert!(publications
            .current()
            .map(|p| (p, publications.epoch()))
            .is_ok());
        assert!(publications.select(&old_id).is_err());
    }
}
