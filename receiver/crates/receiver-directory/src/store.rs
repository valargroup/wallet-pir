//! Atomic contiguous coverage with explicit rewind. Empty blocks are retained too.
use crate::{
    snapshot::{self, Manifest, ProviderSet, Snapshot, PROFILE},
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
    pub fn open(path: impl AsRef<Path>, config: Config) -> Result<Self, Error> {
        if config.start_height == 0 || config.start_position > i64::MAX as u64 {
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
    /// Old indexes without all commitments must be rebuilt before producing proofs.
    pub fn witnesses(
        &self,
        manifest: &Manifest,
        cache: &mut crate::witness::WitnessCache,
    ) -> Result<crate::witness::WitnessSnapshot, Error> {
        let (commitments, positions) = self.witness_inputs(manifest)?;
        cache.build(manifest, &commitments, &positions)
    }

    /// The commitments below `manifest`'s end and the indexed payment positions, read
    /// from one database view.
    fn witness_inputs(
        &self,
        manifest: &Manifest,
    ) -> Result<(Vec<Hash>, std::collections::BTreeSet<u32>), Error> {
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
        Ok((commitments, positions))
    }

    /// The last stored block, or the configured boundary when none is stored.
    pub fn tip(&self) -> Result<Checkpoint, Error> {
        tip(&self.db, &self.config)
    }

    /// Reject gaps, changed parents, coinbase payments and malformed records before
    /// advancing coverage.
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
            || block.end_position > i64::MAX as u64
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
        let mut previous_position = None;
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
                || previous_position.is_some_and(|pos| p.position <= pos)
            {
                return Err(Error::Malformed);
            }
            previous_position = Some(p.position);
            let record = Record {
                receiver: *receiver,
                page: 0,
                total: 1,
                payment: p.clone(),
            }
            .encode()?;
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
    /// overflowed at `rows`, or the history has more payments than `rows` has slots, which
    /// is found before any record is loaded. `provider` holds the swap provider filter
    /// sets (see [`ProviderStore::sets`]).
    pub fn snapshot(&mut self, rows: u32, provider: &[ProviderSet]) -> Result<Snapshot, Error> {
        let capacity = snapshot::capacity(rows)?;
        let tx = self.db.transaction()?;
        let anchor = tip(&tx, &self.config)?;
        if anchor.height < self.config.start_height {
            return Err(Error::Coverage);
        }
        // Counting stops one past capacity, so an oversized history is not scanned.
        let stored: u64 = tx.query_row(
            "SELECT COUNT(*) FROM (SELECT 1 FROM payments LIMIT ?1)",
            [capacity + 1],
            |r| r.get(0),
        )?;
        if stored > capacity {
            return Err(Error::Capacity);
        }
        let mut records = Vec::new();
        {
            let mut query = tx.prepare("SELECT record FROM payments ORDER BY receiver,position")?;
            let mut result = query.query([])?;
            while let Some(row) = result.next()? {
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

    /// Whether the index holds a payment to `receiver` in transaction `txid`.
    pub fn paid_in(&self, receiver: &Receiver, txid: &Hash) -> Result<bool, Error> {
        Ok(self.db.query_row(
            "SELECT EXISTS(SELECT 1 FROM payments WHERE receiver=?1 AND txid=?2)",
            params![receiver.as_bytes(), txid.as_slice()],
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
            CREATE TABLE IF NOT EXISTS cursors (feed TEXT PRIMARY KEY, created_at INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS starts (feed TEXT PRIMARY KEY, started_at INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS reads (feed TEXT PRIMARY KEY, read_at INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS payouts (receiver BLOB NOT NULL CHECK(length(receiver)=43),
                txid BLOB NOT NULL CHECK(length(txid)=32), seen_at INTEGER NOT NULL,
                PRIMARY KEY(receiver,txid)) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS matched_payouts (receiver BLOB NOT NULL,
                txid BLOB NOT NULL, PRIMARY KEY(receiver,txid)) WITHOUT ROWID;",
        )?;
        Ok(Self { db })
    }

    /// Records one complete read of `feed` that began at `read_at`, in one transaction,
    /// so a failed or interrupted read records nothing: receivers from swaps it created
    /// up to `cursor`, its new position, where `true` marks a payout address and `false`
    /// a refund address, each with its swap's creation time; the completed payouts it
    /// saw, each a payout receiver and the transaction (protocol byte order) that paid
    /// it, so each payment can be checked against the index; and `since`, where the
    /// feed's first read began. A receiver keeps its latest time, a payout the earliest
    /// start of any read that saw it complete, whatever order they commit in (a
    /// receiver reused across swaps has one payout per transaction), and the feed its
    /// first start. The cursor and read time move as one pair: a read that advances the
    /// cursor also sets the read time, one that reaches the same cursor can only
    /// advance the read time, and one behind the cursor changes neither, so a stale
    /// read cannot make the feed look fresher.
    pub fn record(
        &mut self,
        feed: &str,
        since: i64,
        receivers: &[(Receiver, bool, i64)],
        completions: &[(Receiver, Hash)],
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
        let stored: Option<i64> = tx
            .query_row(
                "SELECT created_at FROM cursors WHERE feed=?1",
                [feed],
                |r| r.get(0),
            )
            .optional()?;
        let read_sql = match stored {
            Some(stored) if cursor < stored => None,
            Some(stored) if cursor == stored => Some(
                "INSERT INTO reads VALUES (?1,?2) ON CONFLICT(feed)
                 DO UPDATE SET read_at=MAX(read_at,excluded.read_at)",
            ),
            _ => Some(
                "INSERT INTO reads VALUES (?1,?2) ON CONFLICT(feed)
                 DO UPDATE SET read_at=excluded.read_at",
            ),
        };
        if let Some(read_sql) = read_sql {
            tx.execute(
                "INSERT INTO cursors VALUES (?1,?2) ON CONFLICT(feed)
                 DO UPDATE SET created_at=excluded.created_at",
                params![feed, cursor],
            )?;
            tx.execute(read_sql, params![feed, read_at])?;
        }
        for (receiver, txid) in completions {
            tx.execute(
                "INSERT INTO payouts VALUES (?1,?2,?3) ON CONFLICT(receiver,txid)
                 DO UPDATE SET seen_at=MIN(seen_at,excluded.seen_at)",
                params![receiver.as_bytes(), txid.as_slice(), read_at],
            )?;
        }
        tx.execute(
            "INSERT INTO starts VALUES (?1,?2) ON CONFLICT(feed)
             DO UPDATE SET started_at=MIN(started_at,excluded.started_at)",
            params![feed, since],
        )?;
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

    /// Payouts first seen complete by `until` that no check has matched to the index
    /// yet, however old; see [`Self::record`] and [`Self::match_payouts`].
    pub fn unmatched(&self, until: i64) -> Result<Vec<(Receiver, Hash)>, Error> {
        let mut query = self.db.prepare(
            "SELECT receiver,txid FROM payouts p WHERE seen_at <= ?1 AND NOT EXISTS(
                SELECT 1 FROM matched_payouts m WHERE m.receiver=p.receiver AND m.txid=p.txid)",
        )?;
        let rows = query.query_map([until], |r| {
            Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, Vec<u8>>(1)?))
        })?;
        rows.map(|row| {
            let (receiver, txid) = row?;
            Ok((
                Receiver::from_bytes(receiver.try_into().map_err(|_| Error::Malformed)?)?,
                txid.try_into().map_err(|_| Error::Malformed)?,
            ))
        })
        .collect()
    }

    /// Forgets every payout [`Self::match_payouts`] recorded, so later checks look each
    /// up again. A rewind of the index can remove a matched payment, so call this
    /// before rewinding.
    pub fn forget_matches(&mut self) -> Result<(), Error> {
        self.db.execute("DELETE FROM matched_payouts", [])?;
        Ok(())
    }

    /// Records payouts found in the index, so later checks skip them.
    pub fn match_payouts(&mut self, payouts: &[(Receiver, Hash)]) -> Result<(), Error> {
        let tx = self.db.transaction()?;
        for (receiver, txid) in payouts {
            tx.execute(
                "INSERT OR IGNORE INTO matched_payouts VALUES (?1,?2)",
                params![receiver.as_bytes(), txid.as_slice()],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// The creation time from which `feed` first read swaps, if it ever started.
    pub fn started(&self, feed: &str) -> Result<Option<i64>, Error> {
        Ok(self
            .db
            .query_row("SELECT started_at FROM starts WHERE feed=?1", [feed], |r| {
                r.get(0)
            })
            .optional()?)
    }

    /// When the last complete read of `feed` began, if one finished.
    pub fn read(&self, feed: &str) -> Result<Option<i64>, Error> {
        Ok(self
            .db
            .query_row("SELECT read_at FROM reads WHERE feed=?1", [feed], |r| {
                r.get(0)
            })
            .optional()?)
    }

    /// The creation time of the newest swap recorded from `feed`.
    pub fn cursor(&self, feed: &str) -> Result<Option<i64>, Error> {
        Ok(self
            .db
            .query_row(
                "SELECT created_at FROM cursors WHERE feed=?1",
                [feed],
                |r| r.get(0),
            )
            .optional()?)
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
