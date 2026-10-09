//! Atomic contiguous coverage with explicit rewind. Empty blocks are retained too.
use crate::{
    snapshot::{self, Manifest, ProviderSet, Snapshot, PROFILE, TREE_SIZE},
    Error, Hash, Payment, Receiver, Record,
};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::Path;

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
    pub payments: Vec<(Receiver, Payment)>,
    pub commitments: Vec<Hash>,
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

    /// Build the publication's common proofs, reusing unchanged subtrees from `cache`.
    /// A history of more than `max_commitments` commitments is [`Error::Capacity`],
    /// before any is read; building peaks at about 200 bytes per commitment.
    pub fn witnesses(
        &self,
        manifest: &Manifest,
        cache: &mut crate::witness::WitnessCache,
        max_commitments: u64,
    ) -> Result<crate::witness::WitnessSnapshot, Error> {
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
        cache.build(manifest, &commitments, &positions)
    }

    /// The last stored block, or the configured boundary when none is stored.
    pub fn tip(&self) -> Result<Checkpoint, Error> {
        tip(&self.db, &self.config)
    }

    /// Reject gaps, changed parents, coinbase payments, malformed records and payments
    /// a snapshot would refuse before advancing coverage: the block's payments must
    /// follow chain order by position, transaction index and action index and agree on
    /// their block hash and transaction locations, and a txid already stored must keep
    /// its height and transaction index. Heights and position ranges order the blocks,
    /// so each receiver's pages continue as [`snapshot::check_next`] requires.
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
        let mut previous_output = None;
        let mut locations = snapshot::Locations::default();
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
                || previous_output.is_some_and(|(position, output)| {
                    p.position <= position || (p.tx_index, p.action_index) <= output
                })
            {
                return Err(Error::Malformed);
            }
            previous_output = Some((p.position, (p.tx_index, p.action_index)));
            // Stored as a lone page; a snapshot numbers the pages.
            let record = Record {
                receiver: *receiver,
                page: 0,
                total: 1,
                payment: p.clone(),
            };
            if !locations.add(&(&record).into()) {
                return Err(Error::Malformed);
            }
            let stored: Option<Vec<u8>> = tx
                .query_row(
                    "SELECT record FROM payments WHERE txid=?1 LIMIT 1",
                    [p.txid.as_slice()],
                    |r| r.get(0),
                )
                .optional()?;
            if let Some(stored) = stored {
                let stored = Record::decode(&stored)?.ok_or(Error::Malformed)?.payment;
                if (stored.height, stored.tx_index) != (p.height, p.tx_index) {
                    return Err(Error::Malformed);
                }
            }
            let record = record.encode()?;
            tx.execute(
                "INSERT INTO payments VALUES (?1,?2,?3,?4,?5,?6)",
                params![
                    block.height,
                    receiver.as_bytes().as_slice(),
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
                records.push(Record::decode(&bytes)?.ok_or(Error::Malformed)?);
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
                Err(Error::Capacity) => continue,
                built => return built,
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

    /// Whether the index holds a payment to `receiver`.
    pub fn paid(&self, receiver: &Receiver) -> Result<bool, Error> {
        Ok(self.db.query_row(
            "SELECT EXISTS(SELECT 1 FROM payments WHERE receiver=?1)",
            [receiver.as_bytes()],
            |r| r.get(0),
        )?)
    }
}
/// Receivers a swap provider was given, for the recent and seen filters. It is kept
/// apart from chain coverage: the provider's data is not chain data and survives rewinds.
pub struct ProviderStore {
    db: Connection,
}

impl ProviderStore {
    /// Opens or creates the provider database.
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
            CREATE TABLE IF NOT EXISTS completions (receiver BLOB PRIMARY KEY CHECK(length(receiver)=43),
                seen_at INTEGER NOT NULL) WITHOUT ROWID;",
        )?;
        Ok(Self { db })
    }

    /// Records one complete read of `feed` that began at `read_at`, atomically: the
    /// receivers from swaps it created up to `cursor`, `true` marking a payout address,
    /// each with its swap's creation time; the payout receivers whose swaps it saw
    /// complete; and, for the feed's first read, `initial_since`, where it began. A
    /// receiver keeps its latest time and a completion its earliest. The cursor and read
    /// time move as one pair and never backwards, so a stale read cannot make the feed
    /// look fresher.
    pub fn record(
        &mut self,
        feed: &str,
        initial_since: Option<i64>,
        receivers: &[(Receiver, bool, i64)],
        completions: &[Receiver],
        cursor: i64,
        read_at: i64,
    ) -> Result<(), Error> {
        let tx = self.db.transaction()?;
        for (receiver, payout, seen_at) in receivers {
            tx.execute(
                "INSERT INTO receivers VALUES (?1,?2,?3) ON CONFLICT(receiver,payout)
                 DO UPDATE SET seen_at=MAX(seen_at,excluded.seen_at)",
                params![receiver.as_bytes(), payout, seen_at],
            )?;
        }
        tx.execute(
            "INSERT INTO feeds VALUES (?1,?2,?3,?4) ON CONFLICT(feed)
             DO UPDATE SET cursor=excluded.cursor, read_at=excluded.read_at
             WHERE excluded.cursor>cursor OR (excluded.cursor=cursor AND excluded.read_at>read_at)",
            params![feed, initial_since, cursor, read_at],
        )?;
        for receiver in completions {
            tx.execute(
                "INSERT INTO completions VALUES (?1,?2) ON CONFLICT(receiver)
                 DO UPDATE SET seen_at=MIN(seen_at,excluded.seen_at)",
                params![receiver.as_bytes(), read_at],
            )?;
        }
        tx.commit()?;
        Ok(())
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

    /// Payout receivers first seen complete from `from` through `until`.
    pub fn completed(&self, from: i64, until: i64) -> Result<Vec<Receiver>, Error> {
        let mut query = self
            .db
            .prepare("SELECT receiver FROM completions WHERE seen_at BETWEEN ?1 AND ?2")?;
        let rows = query.query_map([from, until], |r| r.get::<_, Vec<u8>>(0))?;
        rows.map(|bytes| Receiver::from_bytes(bytes?.try_into().map_err(|_| Error::Malformed)?))
            .collect()
    }

    /// The creation time from which `feed` first read swaps, if it ever started.
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

    /// The receivers seen at or after `since`, payout or refund, and every payout
    /// receiver.
    pub fn sets(&self, since: i64) -> Result<(Vec<Receiver>, Vec<Receiver>), Error> {
        let read = |sql: &str, arg: i64| -> Result<Vec<Receiver>, Error> {
            let mut query = self.db.prepare(sql)?;
            let rows = query.query_map([arg], |r| r.get::<_, Vec<u8>>(0))?;
            rows.map(|bytes| Receiver::from_bytes(bytes?.try_into().map_err(|_| Error::Malformed)?))
                .collect()
        };
        Ok((
            read(
                "SELECT DISTINCT receiver FROM receivers WHERE seen_at>=?1",
                since,
            )?,
            read("SELECT receiver FROM receivers WHERE payout=?1", 1)?,
        ))
    }
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
