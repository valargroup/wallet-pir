//! The receiver indexer's durable stores: atomic contiguous coverage with explicit
//! rewind (empty blocks are retained too), and the provider feeds' `ProviderStore`.
//!
//! Records, placement and publications are wallet-libraries'
//! `zakura-receiver-directory`; this crate only persists them.
use receiver_directory::{
    snapshot::{self, Manifest, ProviderSet, Snapshot, PROFILE, TREE_SIZE},
    Hash, Payment, RawRecord, ReceiverBytes,
};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::Path;

/// Why directory data, coverage or the database was rejected: the directory's own
/// errors, unchanged, and the database's.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("malformed receiver directory data")]
    Malformed,
    #[error("receiver directory coverage is incomplete or uses a different anchor")]
    Coverage,
    #[error("receiver directory table or commitment history limit exceeded")]
    Capacity,
    #[error("missing continuation page")]
    MissingPage,
    #[error(transparent)]
    Sql(#[from] rusqlite::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

impl From<receiver_directory::Error> for Error {
    fn from(error: receiver_directory::Error) -> Self {
        match error {
            receiver_directory::Error::Malformed => Error::Malformed,
            receiver_directory::Error::Coverage => Error::Coverage,
            receiver_directory::Error::Capacity => Error::Capacity,
            receiver_directory::Error::MissingPage => Error::MissingPage,
            receiver_directory::Error::Json(error) => Error::Json(error),
        }
    }
}

/// Encodes `record` after checking its page bounds.
fn encode(record: &RawRecord) -> Result<[u8; receiver_directory::RECORD_BYTES], Error> {
    record.encode().map_err(|_| Error::Malformed)
}

/// Decodes one slot structurally. Only an entirely zero slot is empty; any other
/// malformed slot is an error, so corruption can never advance a gap limit.
fn decode(bytes: &[u8]) -> Result<Option<RawRecord>, Error> {
    RawRecord::decode(bytes).map_err(|_| Error::Malformed)
}

/// Salts a publication tries at one row count before the caller grows the table.
pub const SALT_ATTEMPTS: u32 = 16;

/// The network and chain boundary that a database is bound to.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Config {
    pub genesis: Hash,
    pub start_height: u32,
    pub start_parent: Hash,
    pub start_position: u64,
}

/// A block's height and hash, with the note position after it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Checkpoint {
    pub height: u32,
    pub hash: Hash,
    pub position: u64,
}

/// Construct only after verifying the raw block, canonical hash and tree size.
pub struct IndexedBlock {
    pub height: u32,
    pub hash: Hash,
    pub parent: Hash,
    pub start_position: u64,
    pub end_position: u64,
    pub coinbase_actions: u64,
    pub payments: Vec<(ReceiverBytes, Payment)>,
    pub commitments: Vec<Hash>,
}

/// One publication's commitment history and payment positions; see
/// [`Store::witness_inputs`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WitnessInputs {
    /// Every commitment from position zero, in position order.
    pub commitments: Vec<Hash>,
    /// The positions of the publication's payments.
    pub positions: std::collections::BTreeSet<u32>,
}

/// The indexer's SQLite database of contiguous public coverage.
pub struct Store {
    db: Connection,
    config: Config,
}

impl Store {
    /// A database is permanently bound to its network and starting chain boundary.
    /// Positions stay within [`TREE_SIZE`], so any stored tip can be published.
    pub fn open(path: impl AsRef<Path>, config: Config) -> Result<Self, Error> {
        if config.start_height == 0 || config.start_position > TREE_SIZE {
            return Err(Error::Malformed);
        }
        let mut db = Connection::open(path)?;
        db.busy_timeout(std::time::Duration::from_secs(5))?;
        db.execute_batch(
            "PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;",
        )?;
        let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let version: u32 = tx.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        if version > 2 {
            return Err(Error::Malformed);
        }
        tx.execute_batch("CREATE TABLE IF NOT EXISTS config (id INTEGER PRIMARY KEY CHECK(id=1), value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS blocks (height INTEGER PRIMARY KEY, hash BLOB NOT NULL, end_position INTEGER NOT NULL, coinbase_actions INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS payments (height INTEGER NOT NULL REFERENCES blocks(height) ON DELETE CASCADE,
                receiver BLOB NOT NULL, position INTEGER NOT NULL UNIQUE, txid BLOB NOT NULL, action INTEGER NOT NULL, record BLOB NOT NULL,
                UNIQUE(txid,action));
            CREATE INDEX IF NOT EXISTS receiver_payments ON payments(receiver,position);
            CREATE TABLE IF NOT EXISTS commitments (position INTEGER PRIMARY KEY, height INTEGER NOT NULL REFERENCES blocks(height) ON DELETE CASCADE, cmx BLOB NOT NULL CHECK(length(cmx)=32));
            PRAGMA user_version=2;")?;
        let encoded = serde_json::to_string(&config)?;
        tx.execute("INSERT OR IGNORE INTO config VALUES (1,?1)", [&encoded])?;
        let saved: String =
            tx.query_row("SELECT value FROM config WHERE id=1", [], |r| r.get(0))?;
        if serde_json::from_str::<Config>(&saved)? != config {
            return Err(Error::Coverage);
        }
        tx.commit()?;
        Ok(Self { db, config })
    }

    /// Every note commitment from the pool's empty tree to `manifest`'s end, and the
    /// note position of every stored payment up to its end height, read from one
    /// database view so the two always describe the same chain. These are what the
    /// publication's common inclusion proofs are built from; this crate does not hash
    /// them. A history of more than `max_commitments` commitments is
    /// [`Error::Capacity`], before any is read.
    pub fn witness_inputs(
        &self,
        manifest: &Manifest,
        max_commitments: u64,
    ) -> Result<WitnessInputs, Error> {
        manifest.validate()?;
        if manifest.end_position > max_commitments {
            return Err(Error::Capacity);
        }
        // Bind the commitments and payment positions to a single database view.
        let tx = self.db.unchecked_transaction()?;
        let checkpoint = self.checkpoint(manifest.end_height)?;
        if self.config.start_position != 0
            || checkpoint.hash != manifest.end_hash
            || checkpoint.position != manifest.end_position
        {
            return Err(Error::Coverage);
        }
        let mut query = self
            .db
            .prepare("SELECT position,cmx FROM commitments WHERE position<?1 ORDER BY position")?;
        let mut commitments = Vec::new();
        for row in query.query_map([manifest.end_position], |r| {
            Ok((r.get::<_, u64>(0)?, r.get::<_, Vec<u8>>(1)?))
        })? {
            let (position, cmx) = row?;
            if position != commitments.len() as u64 {
                return Err(Error::Coverage);
            }
            commitments.push(cmx.try_into().map_err(|_| Error::Malformed)?);
        }
        let mut query = self
            .db
            .prepare("SELECT position FROM payments WHERE height<=?1")?;
        let positions = query
            .query_map([manifest.end_height], |r| r.get::<_, u32>(0))?
            .collect::<Result<std::collections::BTreeSet<_>, _>>()?;
        drop(query);
        tx.commit()?;
        Ok(WitnessInputs {
            commitments,
            positions,
        })
    }

    /// The last stored block, or the configured boundary when none is stored.
    pub fn tip(&self) -> Result<Checkpoint, Error> {
        tip(&self.db, &self.config)
    }

    /// Reject gaps, changed parents, coinbase payments, malformed records and payments
    /// a snapshot would refuse before advancing coverage: the block's payments must
    /// share its hash and follow chain order by position, transaction index and action
    /// index, with one txid per transaction. Heights and position ranges order the
    /// blocks, so each receiver's pages continue as [`snapshot::check_next`] requires.
    pub fn append(&mut self, block: &IndexedBlock) -> Result<(), Error> {
        let tx = self
            .db
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let previous = tip(&tx, &self.config)?;
        if previous.height.checked_add(1) != Some(block.height)
            || previous.hash != block.parent
            || previous.position != block.start_position
            || block.commitments.len() as u64
                != block.end_position.saturating_sub(block.start_position)
            || block.end_position < block.start_position
            || block.end_position > TREE_SIZE
            || block.coinbase_actions > block.end_position - block.start_position
            || block.payments.len() as u64
                > block.end_position - block.start_position - block.coinbase_actions
        {
            return Err(Error::Coverage);
        }
        tx.execute(
            "INSERT INTO blocks VALUES (?1,?2,?3,?4)",
            params![
                block.height,
                block.hash.as_slice(),
                block.end_position,
                block.coinbase_actions
            ],
        )?;
        for (offset, cmx) in block.commitments.iter().enumerate() {
            tx.execute(
                "INSERT INTO commitments VALUES (?1,?2,?3)",
                params![
                    block.start_position + offset as u64,
                    block.height,
                    cmx.as_slice()
                ],
            )?;
        }
        let mut last: Option<&Payment> = None;
        for (receiver, p) in &block.payments {
            // Coinbase recipients are excluded: the coinbase is transaction zero, and its
            // Actions take the block's first positions.
            if p.height != block.height
                || p.block_hash != block.hash
                || p.tx_index == 0
                || p.position < block.start_position + block.coinbase_actions
                || p.position >= block.end_position
                || block
                    .commitments
                    .get((p.position - block.start_position) as usize)
                    != Some(&p.cmx)
                || last.is_some_and(|q| {
                    p.position <= q.position
                        || (p.tx_index, p.action_index) <= (q.tx_index, q.action_index)
                        || (p.tx_index == q.tx_index && p.txid != q.txid)
                })
            {
                return Err(Error::Malformed);
            }
            last = Some(p);
            // Stored as a lone page; a snapshot numbers the pages.
            let record = encode(&RawRecord {
                receiver: *receiver,
                page: 0,
                total: 1,
                payment: p.clone(),
            })?;
            tx.execute(
                "INSERT INTO payments VALUES (?1,?2,?3,?4,?5,?6)",
                params![
                    block.height,
                    receiver.as_slice(),
                    p.position,
                    p.txid.as_slice(),
                    p.action_index,
                    record.as_slice()
                ],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Rewind only to an exact saved ancestor, including the configured starting boundary.
    pub fn rewind(&mut self, height: u32, hash: Hash) -> Result<(), Error> {
        let tx = self
            .db
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let saved = if height == self.config.start_height - 1 {
            Some(self.config.start_parent.to_vec())
        } else {
            tx.query_row("SELECT hash FROM blocks WHERE height=?1", [height], |r| {
                r.get::<_, Vec<u8>>(0)
            })
            .optional()?
        };
        if saved.as_deref() != Some(hash.as_slice()) {
            return Err(Error::Coverage);
        }
        tx.execute("DELETE FROM blocks WHERE height>?1", [height])?;
        tx.commit()?;
        Ok(())
    }

    /// The stored block at `height`, or the boundary just below the configured start.
    pub fn checkpoint(&self, height: u32) -> Result<Checkpoint, Error> {
        if height == self.config.start_height - 1 {
            return Ok(boundary(&self.config));
        }
        self.db
            .query_row(
                "SELECT height,hash,end_position FROM blocks WHERE height=?1",
                [height],
                checkpoint_row,
            )?
            .ok_or(Error::Malformed)
    }

    /// Build all receiver pages from one SQLite read transaction and immutable anchor.
    /// A crowded bucket retries the next of [`SALT_ATTEMPTS`] salts derived from the
    /// anchor, so the result is deterministic; [`Error::Capacity`] means every salt
    /// overflowed at `rows`, or the history has more payments than `rows` has slots,
    /// found without loading more records than that. `provider` holds the swap provider
    /// filter sets (see [`ProviderStore::sets`]).
    pub fn snapshot(&mut self, rows: u32, provider: &[ProviderSet]) -> Result<Snapshot, Error> {
        let capacity = snapshot::capacity(rows)?;
        let tx = self.db.transaction()?;
        let anchor = tip(&tx, &self.config)?;
        if anchor.height < self.config.start_height {
            return Err(Error::Coverage);
        }
        let mut records = Vec::new();
        {
            let mut query = tx.prepare("SELECT record FROM payments ORDER BY receiver,position")?;
            let mut result = query.query([])?;
            while let Some(row) = result.next()? {
                if records.len() as u64 == capacity {
                    return Err(Error::Capacity);
                }
                let bytes: Vec<u8> = row.get(0)?;
                records.push(decode(&bytes)?.ok_or(Error::Malformed)?);
            }
        }
        let mut start = 0;
        while start < records.len() {
            let end = start
                + records[start..]
                    .iter()
                    .take_while(|r| r.receiver == records[start].receiver)
                    .count();
            let total = u32::try_from(end - start).map_err(|_| Error::Capacity)?;
            for (page, record) in records[start..end].iter_mut().enumerate() {
                record.page = page as u32;
                record.total = total;
            }
            start = end;
        }
        tx.commit()?;
        for attempt in 0..SALT_ATTEMPTS {
            let manifest = Manifest {
                profile: PROFILE.into(),
                genesis: self.config.genesis,
                start_height: self.config.start_height,
                start_parent: self.config.start_parent,
                start_position: self.config.start_position,
                end_height: anchor.height,
                end_hash: anchor.hash,
                end_position: anchor.position,
                rows,
                salt: salt(&anchor.hash, attempt),
                records: 0,
                data_sha256: [0; 32],
                filters: Vec::new(),
                filters_sha256: [0; 32],
            };
            match Snapshot::build(manifest, &records, provider) {
                Err(receiver_directory::Error::Capacity) => continue,
                built => return Ok(built?),
            }
        }
        Err(Error::Capacity)
    }

    /// Counts are over contiguous stored coverage, including excluded coinbase Actions.
    pub fn counts(&self) -> Result<(u64, u64), Error> {
        Ok((
            self.db
                .query_row("SELECT COUNT(*) FROM payments", [], |r| r.get(0))?,
            self.db.query_row(
                "SELECT COALESCE(SUM(coinbase_actions),0) FROM blocks",
                [],
                |r| r.get(0),
            )?,
        ))
    }

    /// Whether the index holds a payment to `receiver` in transaction `txid`.
    pub fn paid_in(&self, receiver: &ReceiverBytes, txid: &Hash) -> Result<bool, Error> {
        Ok(self.db.query_row(
            "SELECT EXISTS(SELECT 1 FROM payments WHERE receiver=?1 AND txid=?2)",
            params![receiver.as_slice(), txid.as_slice()],
            |r| r.get(0),
        )?)
    }
}
/// Receivers a swap provider was given, for the recent and seen filters. It is kept
/// apart from chain coverage: the provider's data is not chain data and survives rewinds.
/// Its receivers are an append-only union: no rewind, correction, later read or restart
/// removes one, and the database refuses deleting a receiver or changing its identity.
pub struct ProviderStore {
    db: Connection,
}

/// Where a feed's next page starts: the deposit address and memo of the last swap read,
/// after which the explorer continues.
pub type Token = (String, Option<String>);

/// A walk of a feed from its newest swap down to `floor`, recorded page by page so it
/// resumes where it stopped; see [`ProviderStore::sweep_page`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Sweep {
    /// The oldest creation time it covers.
    pub floor: i64,
    /// When it began, the newest creation time it covers.
    pub began: i64,
    /// Where its next page starts, `None` before its first.
    pub after: Option<Token>,
    /// The oldest creation time its pages reached.
    pub reached: Option<i64>,
    /// The pages it recorded.
    pub pages: u64,
}

impl ProviderStore {
    /// Opens or creates the provider database. Earlier development schemas are not
    /// migrated: the indexer keeps this one in `providers.sqlite` and ignores the
    /// `provider.sqlite` of earlier builds. Deleting `providers.sqlite` loses the
    /// receiver union, which a new read of the provider need not reproduce.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let db = Connection::open(path)?;
        db.busy_timeout(std::time::Duration::from_secs(5))?;
        db.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS receivers (receiver BLOB NOT NULL CHECK(length(receiver)=43),
                payout INTEGER NOT NULL CHECK(payout IN (0,1)), seen_at INTEGER NOT NULL,
                PRIMARY KEY(receiver,payout)) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS feeds (feed TEXT PRIMARY KEY, started_at INTEGER,
                cursor INTEGER NOT NULL, read_at INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS payouts (receiver BLOB NOT NULL CHECK(length(receiver)=43),
                txid BLOB NOT NULL CHECK(length(txid)=32), seen_at INTEGER NOT NULL,
                PRIMARY KEY(receiver,txid)) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS history (feed TEXT PRIMARY KEY, since INTEGER NOT NULL,
                through INTEGER NOT NULL, CHECK(since<=through));
            CREATE TABLE IF NOT EXISTS sweeps (feed TEXT PRIMARY KEY, floor INTEGER NOT NULL,
                began INTEGER NOT NULL, after TEXT, memo TEXT, reached INTEGER,
                pages INTEGER NOT NULL, CHECK(floor<=began));
            CREATE TRIGGER IF NOT EXISTS receivers_kept BEFORE DELETE ON receivers
                BEGIN SELECT RAISE(ABORT, 'provider receivers are append-only'); END;
            CREATE TRIGGER IF NOT EXISTS receivers_fixed BEFORE UPDATE OF receiver, payout ON receivers
                BEGIN SELECT RAISE(ABORT, 'provider receivers are append-only'); END;",
        )?;
        Ok(Self { db })
    }

    /// Records one complete live read of `feed` that began at `read_at`, atomically:
    /// the receivers from swaps it created up to `cursor` and the completed payouts it
    /// saw (see `insert`), and, when the read starts the feed's continuous live
    /// coverage, `started`, where it began. The cursor, read time and any new start move
    /// together and never backwards, so a stale read cannot make the feed look fresher.
    pub fn record(
        &mut self,
        feed: &str,
        started: Option<i64>,
        receivers: &[(ReceiverBytes, bool, i64)],
        completions: &[(ReceiverBytes, Hash, i64)],
        cursor: i64,
        read_at: i64,
    ) -> Result<(), Error> {
        let tx = self.db.transaction()?;
        insert(&tx, receivers, completions)?;
        tx.execute(
            "INSERT INTO feeds VALUES (?1,?2,?3,?4) ON CONFLICT(feed)
             DO UPDATE SET cursor=excluded.cursor, read_at=excluded.read_at,
                started_at=COALESCE(excluded.started_at,started_at)
             WHERE excluded.cursor>cursor OR (excluded.cursor=cursor AND excluded.read_at>read_at)",
            params![feed, started, cursor, read_at],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// `feed`'s sweep in progress, if any.
    pub fn sweep(&self, feed: &str) -> Result<Option<Sweep>, Error> {
        Ok(self
            .db
            .query_row(
                "SELECT floor,began,after,memo,reached,pages FROM sweeps WHERE feed=?1",
                [feed],
                |r| {
                    let (after, memo): (Option<String>, Option<String>) = (r.get(2)?, r.get(3)?);
                    Ok(Sweep {
                        floor: r.get(0)?,
                        began: r.get(1)?,
                        after: after.map(|address| (address, memo)),
                        reached: r.get(4)?,
                        pages: r.get(5)?,
                    })
                },
            )
            .optional()?)
    }

    /// Begins a sweep of `feed` at `began` down to `floor`, which must not follow it.
    /// A feed has at most one sweep: beginning another while one runs is
    /// [`Error::Coverage`].
    pub fn begin_sweep(&mut self, feed: &str, floor: i64, began: i64) -> Result<Sweep, Error> {
        if floor > began {
            return Err(Error::Malformed);
        }
        let inserted = self.db.execute(
            "INSERT OR IGNORE INTO sweeps VALUES (?1,?2,?3,NULL,NULL,NULL,0)",
            params![feed, floor, began],
        )?;
        if inserted == 0 {
            return Err(Error::Coverage);
        }
        Ok(Sweep {
            floor,
            began,
            after: None,
            reached: None,
            pages: 0,
        })
    }

    /// Records one page of `feed`'s sweep that began at `began`, atomically: its
    /// receivers and completed payouts (see `insert`), the oldest creation time it
    /// reached, and `next`, where the following page starts. A page with no `next`
    /// ends the sweep, which extends `feed`'s history (see [`Self::history`]) and is
    /// removed. A page of another sweep than the one in progress is [`Error::Coverage`]
    /// and records nothing.
    pub fn sweep_page(
        &mut self,
        feed: &str,
        began: i64,
        receivers: &[(ReceiverBytes, bool, i64)],
        completions: &[(ReceiverBytes, Hash, i64)],
        reached: Option<i64>,
        next: Option<&Token>,
    ) -> Result<(), Error> {
        let tx = self.db.transaction()?;
        let floor: Option<i64> = tx
            .query_row(
                "SELECT floor FROM sweeps WHERE feed=?1 AND began=?2",
                params![feed, began],
                |r| r.get(0),
            )
            .optional()?;
        let Some(floor) = floor else {
            return Err(Error::Coverage);
        };
        insert(&tx, receivers, completions)?;
        match next {
            Some((address, memo)) => {
                tx.execute(
                    "UPDATE sweeps SET after=?2, memo=?3, pages=pages+1,
                        reached=MIN(COALESCE(reached,?4),COALESCE(?4,reached)) WHERE feed=?1",
                    params![feed, address, memo, reached],
                )?;
            }
            None => {
                // A sweep that reaches into the history extends it. One that would leave
                // a gap replaces it, so the history never claims a span no sweep read.
                tx.execute(
                    "INSERT INTO history VALUES (?1,?2,?3) ON CONFLICT(feed) DO UPDATE SET
                        since=CASE WHEN excluded.since<=through AND excluded.through>=since
                            THEN MIN(since,excluded.since) ELSE excluded.since END,
                        through=CASE WHEN excluded.since<=through AND excluded.through>=since
                            THEN MAX(through,excluded.through) ELSE excluded.through END",
                    params![feed, floor, began],
                )?;
                tx.execute("DELETE FROM sweeps WHERE feed=?1", [feed])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// The creation-time span `(since, through)` over which `feed`'s completed sweeps
    /// read every swap the provider listed by then, without a gap, if one completed.
    pub fn history(&self, feed: &str) -> Result<Option<(i64, i64)>, Error> {
        Ok(self
            .db
            .query_row(
                "SELECT since,through FROM history WHERE feed=?1",
                [feed],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?)
    }

    /// Runs `read` in one read transaction, so every query it makes sees the same
    /// committed state even while a feed records.
    pub fn view<T, E: From<Error>>(
        &self,
        read: impl FnOnce(&Self) -> Result<T, E>,
    ) -> Result<T, E> {
        let tx = self.db.unchecked_transaction().map_err(Error::from)?;
        let value = read(self)?;
        tx.commit().map_err(Error::from)?;
        Ok(value)
    }

    /// Every payout first seen complete by `until`, however old; see [`Self::record`].
    pub fn payouts(&self, until: i64) -> Result<Vec<(ReceiverBytes, Hash)>, Error> {
        let mut query = self
            .db
            .prepare("SELECT receiver,txid FROM payouts WHERE seen_at <= ?1")?;
        let rows = query.query_map([until], |r| {
            Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, Vec<u8>>(1)?))
        })?;
        rows.map(|row| {
            let (receiver, txid) = row?;
            Ok((
                receiver.try_into().map_err(|_| Error::Malformed)?,
                txid.try_into().map_err(|_| Error::Malformed)?,
            ))
        })
        .collect()
    }

    /// The creation time from which `feed`'s continuous live coverage began, if it
    /// started; see [`Self::record`].
    pub fn started(&self, feed: &str) -> Result<Option<i64>, Error> {
        self.feed(feed, "started_at")
    }

    /// When the last complete read of `feed` began, if one finished.
    pub fn read(&self, feed: &str) -> Result<Option<i64>, Error> {
        self.feed(feed, "read_at")
    }

    /// The creation time of the newest swap recorded from `feed`.
    pub fn cursor(&self, feed: &str) -> Result<Option<i64>, Error> {
        self.feed(feed, "cursor")
    }

    /// `feed`'s `column`, or `None` when the feed or the value is absent.
    fn feed(&self, feed: &str, column: &str) -> Result<Option<i64>, Error> {
        Ok(self
            .db
            .query_row(
                &format!("SELECT {column} FROM feeds WHERE feed=?1"),
                [feed],
                |r| r.get(0),
            )
            .optional()?
            .flatten())
    }

    /// The receivers seen at or after `since`, and every receiver seen at all, payout or
    /// refund.
    pub fn sets(&self, since: i64) -> Result<(Vec<ReceiverBytes>, Vec<ReceiverBytes>), Error> {
        let read = |sql: &str, args: &[i64]| -> Result<Vec<ReceiverBytes>, Error> {
            let mut query = self.db.prepare(sql)?;
            let rows =
                query.query_map(rusqlite::params_from_iter(args), |r| r.get::<_, Vec<u8>>(0))?;
            rows.map(|bytes| bytes?.try_into().map_err(|_| Error::Malformed))
                .collect()
        };
        Ok((
            read(
                "SELECT DISTINCT receiver FROM receivers WHERE seen_at>=?1",
                &[since],
            )?,
            read("SELECT DISTINCT receiver FROM receivers", &[])?,
        ))
    }
}

/// Inserts the receivers a provider was given, `true` marking a payout address, each
/// with its swap's creation time, and the completed payouts a read saw, each a payout
/// receiver, the transaction (protocol byte order) that paid it and when the read saw it
/// complete. A receiver keeps its latest time and a payout its earliest.
fn insert(
    tx: &rusqlite::Transaction<'_>,
    receivers: &[(ReceiverBytes, bool, i64)],
    completions: &[(ReceiverBytes, Hash, i64)],
) -> Result<(), Error> {
    for (receiver, payout, seen_at) in receivers {
        tx.execute(
            "INSERT INTO receivers VALUES (?1,?2,?3) ON CONFLICT(receiver,payout)
             DO UPDATE SET seen_at=MAX(seen_at,excluded.seen_at)",
            params![receiver.as_slice(), payout, seen_at],
        )?;
    }
    for (receiver, txid, seen_at) in completions {
        tx.execute(
            "INSERT INTO payouts VALUES (?1,?2,?3) ON CONFLICT(receiver,txid)
             DO UPDATE SET seen_at=MIN(seen_at,excluded.seen_at)",
            params![receiver.as_slice(), txid.as_slice(), seen_at],
        )?;
    }
    Ok(())
}

/// The anchor hash first, so an uncrowded publication keeps its earlier salt.
fn salt(anchor: &Hash, attempt: u32) -> Hash {
    if attempt == 0 {
        return *anchor;
    }
    let mut h = Sha256::new();
    h.update(b"ironwood-receiver/v1/salt\0");
    h.update(anchor);
    h.update(attempt.to_le_bytes());
    h.finalize().into()
}

/// The checkpoint just below the configured start height.
fn boundary(c: &Config) -> Checkpoint {
    Checkpoint {
        height: c.start_height - 1,
        hash: c.start_parent,
        position: c.start_position,
    }
}
/// A checkpoint from a `(height, hash, position)` row, or `None` for a malformed hash.
fn checkpoint_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<Option<Checkpoint>> {
    let hash: Vec<u8> = r.get(1)?;
    let height = r.get(0)?;
    let position = r.get(2)?;
    Ok(hash.try_into().ok().map(|hash| Checkpoint {
        height,
        hash,
        position,
    }))
}
/// See [`Store::tip`].
fn tip(db: &Connection, c: &Config) -> Result<Checkpoint, Error> {
    match db
        .query_row(
            "SELECT height,hash,end_position FROM blocks ORDER BY height DESC LIMIT 1",
            [],
            checkpoint_row,
        )
        .optional()?
    {
        None => Ok(boundary(c)),
        Some(Some(p)) => Ok(p),
        Some(None) => Err(Error::Malformed),
    }
}
