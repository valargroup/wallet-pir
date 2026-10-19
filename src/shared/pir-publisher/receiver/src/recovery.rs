//! The recovery objects a publication's manifest commits to, built from the index and
//! the provider sets; their formats are wallet-libraries' `receiver_directory::paid`,
//! `seen` and `catalog`.
//!
//! Buckets are the closed aligned intervals of
//! [`BUCKET_BLOCKS`] from the first aligned height at or above the index's start
//! boundary, each of every receiver paid at heights in it, deduplicated within it.
//! Empty buckets are published too. The catalog retains the latest `retained` of them.
//! The cumulative paid set covers `(0, end]`, `end` being the latest closed bucket's
//! end, or the index's start boundary before one closes: the index holds every
//! payment from Ironwood activation, before which there are none.
//!
//! [`RecoveryStore`] records each interval's membership as published, the payment-history
//! epoch and the seen union. Before a publication, a recorded interval with the same
//! boundary hashes but other members is a correction of published history, which
//! increments the epoch; changed boundary hashes are a reorg, rebuilt without one.
//! The seen set is the recorded union with the values of every receiver the provider
//! seen sets now hold, so it never loses one, whatever the provider store or index
//! later holds.
use receiver_directory::{
    catalog::{BucketIndex, CatalogHead},
    filter::SEEN,
    paid::{bucket_start, PaidSet, Span, BUCKET_BLOCKS},
    seen::{SeenSet, MAX_SEEN_VALUES},
    snapshot::ProviderSet,
    Hash, ReceiverBytes,
};
use receiver_pir_server::Recovery;
use receiver_store::{
    recovery::{RecoveryStore, Segment},
    Store,
};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// The recovery database's file in the configured data directory; see
/// [`RecoveryStore`].
pub const RECOVERY: &str = "recovery.sqlite";
/// Buckets a catalog retains by default: 30 days at 75-second spacing.
pub const RETAINED_BUCKETS: u32 = 720;

/// The paid history through a publication's cumulative end, by interval.
#[derive(Debug)]
struct History {
    /// The first aligned bucket start at or above the index's start boundary.
    first: u32,
    /// The end of the latest closed bucket.
    latest_end: u32,
    /// The cumulative span and its distinct receivers, ascending.
    paid: (Span, Vec<ReceiverBytes>),
    /// Every interval from the start boundary to `paid`'s end, ascending, with its
    /// distinct receivers: the span up to `first` when it is not empty, then each
    /// bucket.
    segments: Vec<(Span, Vec<ReceiverBytes>)>,
}

/// Groups `payments` (height and receiver, at heights above `start_height - 1`) into
/// the [`History`] of an index from `start_height` whose tip is `tip`, with `hash`
/// giving the block hash at a height at or above `start_height - 1`, and `genesis` at
/// zero.
fn history(
    genesis: Hash,
    start_height: u32,
    tip: u32,
    payments: &[(u32, ReceiverBytes)],
    hash: impl Fn(u32) -> Result<Hash>,
) -> Result<History> {
    let boundary = start_height - 1;
    let first = boundary.next_multiple_of(BUCKET_BLOCKS);
    let latest_end = tip - tip % BUCKET_BLOCKS;
    let end = latest_end.max(boundary);
    let mut starts = Vec::new();
    if latest_end >= first {
        if first > boundary {
            starts.push(boundary);
        }
        starts.extend((first..latest_end).step_by(BUCKET_BLOCKS as usize));
    }
    let mut segments = Vec::with_capacity(starts.len());
    for (i, &start) in starts.iter().enumerate() {
        let end = starts.get(i + 1).copied().unwrap_or(latest_end);
        let span = Span {
            start,
            start_hash: hash(start)?,
            end,
            end_hash: hash(end)?,
        };
        segments.push((span, Vec::new()));
    }
    let mut all = Vec::new();
    for &(height, receiver) in payments.iter().filter(|(h, _)| *h <= end) {
        if height <= boundary {
            return Err("a stored payment precedes the index's start".into());
        }
        let segment = match bucket_start(height).filter(|s| *s >= first) {
            Some(start) => starts.binary_search(&start),
            None => Ok(0),
        };
        let segment = segment.map_err(|_| "a payment outside every segment")?;
        segments[segment].1.push(receiver);
        all.push(receiver);
    }
    for (_, members) in &mut segments {
        members.sort_unstable();
        members.dedup();
    }
    all.sort_unstable();
    all.dedup();
    let paid = Span {
        start: 0,
        start_hash: genesis,
        end,
        end_hash: hash(end)?,
    };
    Ok(History {
        first,
        latest_end,
        paid: (paid, all),
        segments,
    })
}

impl History {
    /// Each interval as [`RecoveryStore::commit`] records it.
    fn records(&self) -> Vec<Segment> {
        self.segments
            .iter()
            .map(|(span, members)| {
                let mut digest = Sha256::new();
                digest.update(b"receiver-indexer/segment-members\0");
                digest.update((members.len() as u64).to_le_bytes());
                members.iter().for_each(|m| digest.update(m));
                Segment {
                    start: span.start,
                    start_hash: span.start_hash,
                    end_hash: span.end_hash,
                    members: digest.finalize().into(),
                }
            })
            .collect()
    }

    /// The catalog of the latest `retained` buckets and the cumulative set under
    /// `epoch`, with `seen`, and the head naming them.
    fn catalog(
        &self,
        genesis: Hash,
        epoch: u64,
        retained: u32,
        seen: &SeenSet,
    ) -> Result<(CatalogHead, Recovery)> {
        let oldest_start = match self.latest_end >= self.first {
            true => self
                .latest_end
                .saturating_sub(retained.saturating_mul(BUCKET_BLOCKS))
                .max(self.first),
            false => self.latest_end,
        };
        let buckets = self
            .segments
            .iter()
            .filter(|(span, _)| span.is_bucket() && span.start >= oldest_start)
            .map(|(span, members)| PaidSet::build(genesis, epoch, *span, members))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let index = BucketIndex::new(
            genesis,
            buckets
                .iter()
                .map(|b| (b.span().start, b.digest()))
                .collect(),
        )?;
        let paid = PaidSet::build(genesis, epoch, self.paid.0, &self.paid.1)?;
        let head = CatalogHead {
            genesis,
            epoch,
            oldest_start,
            latest_end: self.latest_end,
            paid: paid.digest(),
            seen: seen.digest()?,
            index: index.digest(),
        };
        let recovery = Recovery::new(&head, &index, &buckets, &paid, seen)?;
        Ok((head, recovery))
    }
}

/// `previous`, or an empty set on the network with `genesis`, with the values of every
/// receiver in `sets`' seen sets added. Its feed times are those the seen sets share
/// (the latest start and the earliest read), or `previous`'s while there are none.
fn seen(previous: Option<SeenSet>, genesis: Hash, sets: &[ProviderSet]) -> SeenSet {
    let suffix = format!("/{SEEN}");
    let sets: Vec<_> = sets.iter().filter(|s| s.label.ends_with(&suffix)).collect();
    let base = previous.unwrap_or_else(|| SeenSet::new(genesis, 0));
    let mut next = base.union(sets.iter().flat_map(|s| &s.receivers));
    if let (Some(since), Some(until)) = (
        sets.iter().map(|s| s.since_unix).max(),
        sets.iter().map(|s| s.until_unix).min(),
    ) {
        (next.since_unix, next.until_unix) = (since, until);
    }
    next
}

/// Builds the recovery objects of a publication at `store`'s tip on the network with
/// `genesis`, from an index starting at `start_height`, retaining `retained` buckets,
/// with the seen sets of `sets`, after recording them in `recovery`; see the module
/// docs. A seen set over [`MAX_SEEN_VALUES`] fails and records nothing.
pub fn build(
    store: &Store,
    recovery: &mut RecoveryStore,
    genesis: Hash,
    start_height: u32,
    retained: u32,
    sets: &[ProviderSet],
) -> Result<(CatalogHead, Recovery)> {
    let tip = store.tip()?.height;
    let history = history(genesis, start_height, tip, &store.paid(tip)?, |height| {
        Ok(store.checkpoint(height)?.hash)
    })?;
    let seen = seen(recovery.seen()?, genesis, sets);
    if seen.count() > MAX_SEEN_VALUES {
        return Err(format!(
            "the seen set's {} values exceed its limit of {MAX_SEEN_VALUES}; no publication created",
            seen.count()
        )
        .into());
    }
    let epoch = recovery.commit(&history.records(), &seen)?;
    history.catalog(genesis, epoch, retained, &seen)
}

/// `receiver-directory recovery`: maintain the recovery database while the service is
/// stopped.
#[derive(clap::Parser)]
#[command(name = "receiver-directory recovery")]
pub struct Args {
    /// The service's data directory, which holds `recovery.sqlite`.
    #[arg(long)]
    pub data_dir: PathBuf,
    #[command(subcommand)]
    pub command: Command,
}

#[derive(clap::Subcommand)]
pub enum Command {
    /// Increment the payment-history epoch after correcting already published history
    /// in a way the indexer cannot detect, so wallets recheck it. The indexer increments
    /// it itself when a published interval's members change.
    BumpEpoch,
    /// Take over from the live publication, as on a replacement host: raise the epoch
    /// to the catalog head's and adopt its seen set, both as served at
    /// `/v1/receiver/catalog/<digest>` and `/v1/receiver/seen/<digest>`.
    Adopt {
        #[arg(long)]
        catalog: PathBuf,
        #[arg(long)]
        seen: PathBuf,
    },
}

/// Runs `args` on the mainnet recovery database, holding the data directory's lock
/// so no service runs meanwhile, and prints the resulting epoch.
pub fn run(args: Args) -> Result<()> {
    let _lock = pir_pipeline::service::lock(&args.data_dir)?;
    let genesis = zakura_server_chain::Network::Mainnet.genesis_hash();
    let mut store = RecoveryStore::open(args.data_dir.join(RECOVERY), genesis)?;
    if let Command::Adopt { catalog, seen } = &args.command {
        let read = |path: &Path| std::fs::read(path);
        let head = CatalogHead::decode(&read(catalog)?)?;
        store.adopt(&head, &SeenSet::decode(&read(seen)?)?)?;
    } else {
        store.bump_epoch()?;
    }
    println!("{}", serde_json::json!({ "epoch": store.epoch()? }));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use receiver_store::{Config, IndexedBlock};
    use serde_json::Value;

    fn hex32(value: &Value) -> Hash {
        hex::decode(value.as_str().unwrap())
            .unwrap()
            .try_into()
            .unwrap()
    }

    /// Payments to the vectors' receivers grouped from heights reproduce every pinned
    /// encoding of the recovery bucket format in wallet-libraries: a bucket, an empty
    /// bucket, the cumulative set, the seen set, the index and the head.
    #[test]
    fn buckets_reproduce_the_library_vectors() {
        let v: Value =
            serde_json::from_str(include_str!("../tests/fixtures/recovery-vectors.json")).unwrap();
        let genesis = hex32(&v["genesis"]);
        let r: Vec<ReceiverBytes> = v["receivers"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| {
                hex::decode(r.as_str().unwrap())
                    .unwrap()
                    .try_into()
                    .unwrap()
            })
            .collect();
        // Receiver 3 is paid long before retention, 1 twice in the first bucket, and the
        // tip lies inside the open bucket after the latest closed one.
        let payments = [
            (100, r[3]),
            (4993, r[1]),
            (5000, r[0]),
            (5001, r[1]),
            (5040, r[2]),
        ];
        let hashes = |h| -> Result<Hash> {
            Ok(match h {
                4992 => hex32(&v["bucket"]["start_hash"]),
                5040 => hex32(&v["bucket"]["end_hash"]),
                5088 => hex32(&v["empty_bucket"]["end_hash"]),
                h => [h as u8; 32],
            })
        };
        let history = history(genesis, 97, 5100, &payments, hashes).unwrap();
        let sv = &v["seen"];
        let sets = [ProviderSet {
            label: format!("near-intents/{SEEN}"),
            window_secs: None,
            since_unix: sv["since_unix"].as_i64().unwrap(),
            until_unix: sv["until_unix"].as_i64().unwrap(),
            receivers: r.clone(),
        }];
        let seen = seen(None, genesis, &sets);
        let (head, recovery) = history.catalog(genesis, 1, 2, &seen).unwrap();
        assert_eq!(hex::encode(head.digest().unwrap()), v["head"]["digest"]);
        let encodings: Vec<String> = recovery.objects().map(|(_, b)| hex::encode(b)).collect();
        for name in [
            "bucket",
            "empty_bucket",
            "cumulative",
            "seen",
            "index",
            "head",
        ] {
            assert!(
                encodings.contains(&v[name]["encoding"].as_str().unwrap().into()),
                "{name}"
            );
        }
        assert_eq!(encodings.len(), 6);
    }

    /// The tip and blocks of a test index from height 49, whose start boundary 48 is a
    /// bucket start.
    struct Index {
        dir: tempfile::TempDir,
        store: Store,
    }

    const GENESIS: Hash = [1; 32];

    impl Index {
        fn new() -> Self {
            Self::open(tempfile::tempdir().unwrap())
        }

        /// Opens the index in `dir`, as a restart does.
        fn open(dir: tempfile::TempDir) -> Self {
            let store = Store::open(
                dir.path().join("directory.sqlite"),
                Config {
                    genesis: GENESIS,
                    start_height: 49,
                    start_parent: [0; 32],
                    start_position: 0,
                },
            )
            .unwrap();
            Self { dir, store }
        }

        /// Appends blocks up to `height`, each hashed from its height and `fork`, with a
        /// payment to `paid(height)`, if any.
        fn extend(&mut self, height: u32, fork: u8, paid: impl Fn(u32) -> Option<u8>) {
            let mut tip = self.store.tip().unwrap();
            while tip.height < height {
                let h = tip.height + 1;
                let hash = block_hash(h, fork);
                let mut payments = Vec::new();
                let mut commitments = Vec::new();
                if let Some(receiver) = paid(h) {
                    let mut record = crate::probe::common::record(0, 1);
                    record.payment.height = h;
                    record.payment.block_hash = hash;
                    record.payment.txid = hash;
                    record.payment.position = tip.position;
                    payments.push(([receiver; 43], record.payment));
                    commitments.push([6; 32]);
                }
                self.store
                    .append(&IndexedBlock {
                        height: h,
                        hash,
                        parent: tip.hash,
                        start_position: tip.position,
                        end_position: tip.position + commitments.len() as u64,
                        coinbase_actions: 0,
                        payments,
                        commitments,
                    })
                    .unwrap();
                tip = self.store.tip().unwrap();
            }
        }

        fn recovery(&self) -> RecoveryStore {
            RecoveryStore::open(self.dir.path().join(RECOVERY), GENESIS).unwrap()
        }

        /// Builds with `retained` buckets and `sets`.
        fn build(&self, retained: u32, sets: &[ProviderSet]) -> (CatalogHead, Recovery) {
            build(
                &self.store,
                &mut self.recovery(),
                GENESIS,
                49,
                retained,
                sets,
            )
            .unwrap()
        }
    }

    /// Block `height`'s hash on `fork`.
    fn block_hash(height: u32, fork: u8) -> Hash {
        let mut hash = [fork; 32];
        hash[..4].copy_from_slice(&height.to_le_bytes());
        hash
    }

    /// The buckets `recovery` serves, by start.
    fn buckets(recovery: &Recovery) -> Vec<PaidSet> {
        recovery
            .objects()
            .filter(|(o, _)| matches!(o, receiver_pir_server::Object::Bucket(..)))
            .map(|(_, b)| PaidSet::decode(b).unwrap())
            .collect()
    }

    /// A seen set from one provider, read from `since` through `until`.
    fn provider(receivers: &[u8], since: i64, until: i64) -> Vec<ProviderSet> {
        vec![ProviderSet {
            label: format!("near-intents/{SEEN}"),
            window_secs: None,
            since_unix: since,
            until_unix: until,
            receivers: receivers.iter().map(|r| [*r; 43]).collect(),
        }]
    }

    /// Only closed buckets are published, the latest `retained` of them, empty ones
    /// included; the cumulative set ends at the latest closed end and holds every
    /// payment through it.
    #[test]
    fn the_catalog_retains_the_latest_closed_buckets() {
        let mut index = Index::new();
        // Before a bucket closes, the cumulative set ends at the start boundary.
        index.extend(90, 1, |h| (h == 60).then_some(60));
        let (head, recovery) = index.build(3, &[]);
        assert_eq!((head.oldest_start, head.latest_end), (48, 48));
        assert!(buckets(&recovery).is_empty());
        // A payment at 60, and one at 200 in the open bucket (192, 240].
        index.extend(200, 1, |h| (h == 200).then_some(200));
        let (head, recovery) = index.build(2, &[]);
        assert_eq!((head.oldest_start, head.latest_end), (96, 192));
        let spans: Vec<_> = buckets(&recovery)
            .iter()
            .map(|b| (b.span().start, b.count()))
            .collect();
        assert_eq!(spans, [(96, 0), (144, 0)]);
        let paid = recovery
            .objects()
            .find(|(o, _)| matches!(o, receiver_pir_server::Object::Paid(_)))
            .map(|(_, b)| PaidSet::decode(b).unwrap())
            .unwrap();
        assert_eq!(
            (paid.span().start, paid.span().end, paid.count()),
            (0, 192, 1)
        );
        assert_eq!(paid.matches(&[[60; 43], [200; 43]]), [true, false]);
        // A longer retention reaches back to the first bucket and no further.
        let (head, recovery) = index.build(100, &[]);
        assert_eq!(head.oldest_start, 48);
        assert_eq!(buckets(&recovery)[0].matches(&[[60; 43]]), [true]);
    }

    /// A restart rebuilds the same objects, so the catalog digest is unchanged.
    #[test]
    fn a_restart_reproduces_every_digest() {
        let mut index = Index::new();
        index.extend(300, 1, |h| (h % 7 == 0).then_some(h as u8));
        let sets = provider(&[1, 2], 10, 20);
        let (first, _) = index.build(4, &sets);
        let index = Index::open(index.dir);
        assert_eq!(index.build(4, &sets).0, first);
    }

    /// A reorg rebuilds the buckets above the fork with their new boundary hashes and
    /// payments, keeps those below, and leaves the epoch.
    #[test]
    fn a_reorg_rebuilds_buckets_above_the_fork_without_an_epoch() {
        let mut index = Index::new();
        index.extend(300, 1, |h| (h == 250).then_some(1));
        let (before, old) = index.build(10, &[]);
        let fork = 220;
        index.store.rewind(fork, block_hash(fork, 1)).unwrap();
        index.extend(300, 2, |h| (h == 260).then_some(2));
        let (after, new) = index.build(10, &[]);
        assert_eq!(after.epoch, before.epoch);
        let (old, new) = (buckets(&old), buckets(&new));
        assert_eq!(old.len(), new.len());
        for (old, new) in old.iter().zip(&new) {
            assert_eq!(old == new, old.span().end <= fork, "{:?}", new.span());
        }
        let last = new.iter().find(|b| b.span().start == 240).unwrap();
        assert_eq!(last.matches(&[[1; 43], [2; 43]]), [false, true]);
    }

    /// Changing the members of published history under the same hashes, even past
    /// retention, increments the epoch, which reopening keeps; the operator's bump does
    /// too. Rebuilding the same history does not.
    #[test]
    fn corrections_bump_a_durable_epoch() {
        let mut index = Index::new();
        index.extend(400, 1, |h| (h == 60).then_some(1));
        assert_eq!(index.build(2, &[]).0.epoch, 0);
        assert_eq!(index.build(2, &[]).0.epoch, 0);
        // The block at 60 is reindexed with the same hash and another payment, outside
        // the two retained buckets.
        index.store.rewind(59, block_hash(59, 1)).unwrap();
        index.extend(400, 1, |h| (h == 60).then_some(2));
        let (head, recovery) = index.build(2, &[]);
        assert_eq!(head.epoch, 1);
        assert!(buckets(&recovery).iter().all(|b| b.epoch() == 1));
        assert_eq!(index.recovery().epoch().unwrap(), 1);
        assert_eq!(index.recovery().bump_epoch().unwrap(), 2);
        assert_eq!(index.build(2, &[]).0.epoch, 2);
    }

    /// The seen set keeps every value it ever held across a rebuilt index and a provider
    /// store that no longer reports them, and takes the feeds' current times.
    #[test]
    fn the_seen_union_never_shrinks() {
        let mut index = Index::new();
        index.extend(100, 1, |_| None);
        let (_, recovery) = index.build(2, &provider(&[1, 2], 10, 20));
        let seen_of = |recovery: &Recovery| {
            recovery
                .objects()
                .find(|(o, _)| matches!(o, receiver_pir_server::Object::Seen(_)))
                .map(|(_, b)| SeenSet::decode(b).unwrap())
                .unwrap()
        };
        let first = seen_of(&recovery);
        // A rebuilt index, and a provider store holding only a new receiver.
        let Index { dir, store } = index;
        drop(store);
        for file in [
            "directory.sqlite",
            "directory.sqlite-wal",
            "directory.sqlite-shm",
        ] {
            let _ = std::fs::remove_file(dir.path().join(file));
        }
        let mut rebuilt = Index::open(dir);
        rebuilt.extend(100, 1, |_| None);
        let (_, recovery) = rebuilt.build(2, &provider(&[3], 10, 30));
        let next = seen_of(&recovery);
        assert!(next.is_superset(&first));
        assert_eq!(next.matches(&[[1; 43], [2; 43], [3; 43]]), [true; 3]);
        assert_eq!((next.since_unix, next.until_unix), (10, 30));
        // Without provider sets the union and its times stay.
        let (_, recovery) = rebuilt.build(2, &[]);
        assert_eq!(seen_of(&recovery), next);
        // The store refuses recording a smaller union.
        let smaller = SeenSet::new(GENESIS, 0).union(&[[1; 43]]);
        assert!(rebuilt.recovery().commit(&[], &smaller).is_err());
    }

    /// Adoption raises the epoch to the live catalog's and takes its seen set when it
    /// holds the recorded union.
    #[test]
    fn a_replacement_host_adopts_the_live_epoch_and_seen_set() {
        let mut index = Index::new();
        index.extend(100, 1, |_| None);
        let (mut head, recovery) = index.build(2, &provider(&[1], 10, 20));
        let live = SeenSet::decode(
            recovery
                .objects()
                .find(|(o, _)| matches!(o, receiver_pir_server::Object::Seen(_)))
                .unwrap()
                .1,
        )
        .unwrap()
        .union(&[[9; 43]]);
        head.seen = live.digest().unwrap();
        head.epoch = 5;
        let other = tempfile::tempdir().unwrap();
        let mut fresh = RecoveryStore::open(other.path().join(RECOVERY), GENESIS).unwrap();
        fresh.adopt(&head, &live).unwrap();
        assert_eq!(fresh.epoch().unwrap(), 5);
        assert_eq!(fresh.seen().unwrap(), Some(live.clone()));
        // A seen set that is not the one the head names is refused.
        let mut wrong = head.clone();
        wrong.seen = [0; 32];
        assert!(fresh.adopt(&wrong, &live).is_err());
        // An older seen set leaves the recorded union.
        let older = SeenSet::new(GENESIS, 0).union(&[[1; 43]]);
        head.seen = older.digest().unwrap();
        fresh.adopt(&head, &older).unwrap();
        assert_eq!(fresh.seen().unwrap(), Some(live));
    }
}
