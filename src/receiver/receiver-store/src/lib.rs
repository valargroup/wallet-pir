//! The receiver indexer's durable stores: atomic contiguous coverage with explicit
//! rewind (empty blocks are retained too), completed Ironwood shards with their final
//! witness payloads, and the provider feeds' `PayoutStore`.
//!
//! Records, placement and publications are wallet-libraries'
//! `zakura-receiver-directory`; this crate only persists them.
pub mod blocks;

use receiver_directory::{
    snapshot::{self, Manifest, Snapshot, PROFILE, TREE_SIZE},
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
    #[error("receiver directory table limit exceeded")]
    Capacity,
    #[error("missing continuation page")]
    MissingPage,
    #[error(transparent)]
    Sql(#[from] rusqlite::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
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
#[derive(Clone)]
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

/// Leaves in one Ironwood shard, the most commitments one read returns.
pub const SHARD_LEAVES: u64 = 1 << 16;
/// Bytes of one companion witness payload.
pub const PAYLOAD_BYTES: usize = 512;

/// A sealed Receiver shard: its inclusive blocks and how many fixed-geometry segments
/// its payments span.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SealedShard {
    pub start_height: u32,
    pub end_height: u32,
    pub segments: u32,
    /// The pinned payment table rows it was sealed under.
    pub rows: u32,
    /// The most companion payloads one of its segments was allowed.
    pub payloads: u64,
}

/// A completed Ironwood shard: its root and the block holding its last leaf.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompletedShard {
    pub root: Hash,
    pub height: u32,
    pub hash: Hash,
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
        if version > 3 {
            return Err(Error::Malformed);
        }
        // Earlier versions hold shorter records; see [`upgrade`].
        if version < 3 {
            let tables: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='payments')",
                [],
                |r| r.get(0),
            )?;
            let legacy = tables
                && tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM payments WHERE length(record)!=?1)",
                    [receiver_directory::RECORD_BYTES],
                    |r| r.get::<_, bool>(0),
                )?;
            if legacy {
                return Err(Error::Malformed);
            }
        }
        tx.execute_batch("CREATE TABLE IF NOT EXISTS filter_revision (revision INTEGER NOT NULL);
            INSERT INTO filter_revision SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM filter_revision);
            CREATE TABLE IF NOT EXISTS config (id INTEGER PRIMARY KEY CHECK(id=1), value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS blocks (height INTEGER PRIMARY KEY, hash BLOB NOT NULL, end_position INTEGER NOT NULL, coinbase_actions INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS payments (height INTEGER NOT NULL REFERENCES blocks(height) ON DELETE CASCADE,
                receiver BLOB NOT NULL, position INTEGER NOT NULL UNIQUE, txid BLOB NOT NULL, action INTEGER NOT NULL, record BLOB NOT NULL,
                UNIQUE(txid,action));
            CREATE INDEX IF NOT EXISTS receiver_payments ON payments(receiver,position);
            CREATE INDEX IF NOT EXISTS payments_height ON payments(height);
            CREATE TABLE IF NOT EXISTS commitments (position INTEGER PRIMARY KEY, height INTEGER NOT NULL REFERENCES blocks(height) ON DELETE CASCADE, cmx BLOB NOT NULL CHECK(length(cmx)=32));
            CREATE TABLE IF NOT EXISTS ironwood_shards (shard INTEGER PRIMARY KEY,
                height INTEGER NOT NULL REFERENCES blocks(height) ON DELETE CASCADE, root BLOB NOT NULL CHECK(length(root)=32));
            CREATE TABLE IF NOT EXISTS witness_payloads (position INTEGER PRIMARY KEY REFERENCES payments(position) ON DELETE CASCADE,
                shard INTEGER NOT NULL REFERENCES ironwood_shards(shard) ON DELETE CASCADE, payload BLOB NOT NULL CHECK(length(payload)=512));
            CREATE TABLE IF NOT EXISTS receiver_shards (shard INTEGER PRIMARY KEY, start_height INTEGER NOT NULL,
                end_height INTEGER NOT NULL REFERENCES blocks(height) ON DELETE CASCADE, segments INTEGER NOT NULL CHECK(segments>0),
                rows INTEGER NOT NULL, payloads INTEGER NOT NULL);
            PRAGMA user_version=3;")?;
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

    /// The network and chain boundary this database is bound to.
    pub fn config(&self) -> &Config {
        &self.config
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
        insert_payments(&tx, block)?;
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

    /// Replaces the payments of stored blocks after an indexing correction on an
    /// unchanged chain, advancing the stored correction revision to `revision`, which must
    /// exceed it. Offline only: a serving indexer would keep answering from the old
    /// payments, so callers hold the data directory's lock, as the indexer's
    /// `application::correct` does. Each block must match its stored hash, parent, positions, coinbase count
    /// and commitments; its payments are checked as [`Self::append`] checks them. Final
    /// witness payloads of the replaced payments go with them and are rebuilt from the
    /// retained commitments when next requested, and sealed shards ending at or after
    /// the first corrected block are unsealed.
    pub fn correct(&mut self, revision: u64, blocks: &[IndexedBlock]) -> Result<(), Error> {
        let tx = self
            .db
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let previous: u64 =
            tx.query_row("SELECT revision FROM filter_revision", [], |r| r.get(0))?;
        if revision <= previous || revision > i64::MAX as u64 {
            return Err(Error::Coverage);
        }
        for block in blocks {
            let stored = checkpoint_at(&tx, &self.config, block.height)?;
            let parent = checkpoint_at(&tx, &self.config, block.height - 1)?;
            let coinbase: Option<u64> = tx
                .query_row(
                    "SELECT coinbase_actions FROM blocks WHERE height=?1",
                    [block.height],
                    |r| r.get(0),
                )
                .optional()?;
            let mut query =
                tx.prepare("SELECT cmx FROM commitments WHERE height=?1 ORDER BY position")?;
            let commitments = query
                .query_map([block.height], |r| r.get::<_, Vec<u8>>(0))?
                .collect::<Result<Vec<_>, _>>()?;
            drop(query);
            if stored.hash != block.hash
                || parent.hash != block.parent
                || parent.position != block.start_position
                || stored.position != block.end_position
                || coinbase != Some(block.coinbase_actions)
                || commitments.len() != block.commitments.len()
                || commitments
                    .iter()
                    .zip(&block.commitments)
                    .any(|(a, b)| a != b)
            {
                return Err(Error::Coverage);
            }
            tx.execute("DELETE FROM payments WHERE height=?1", [block.height])?;
            insert_payments(&tx, block)?;
        }
        // Partition boundaries through the affected suffix are recomputed.
        if let Some(first) = blocks.iter().map(|b| b.height).min() {
            tx.execute("DELETE FROM receiver_shards WHERE end_height>=?1", [first])?;
        }
        tx.execute("UPDATE filter_revision SET revision=?1", [revision])?;
        tx.commit()?;
        Ok(())
    }

    /// The correction revision the store last accepted.
    pub fn history_revision(&self) -> Result<u64, Error> {
        Ok(self
            .db
            .query_row("SELECT revision FROM filter_revision", [], |r| r.get(0))?)
    }

    /// The commitments at positions `start..end`, at most [`SHARD_LEAVES`], all stored.
    pub fn commitments(&self, start: u64, end: u64) -> Result<Vec<Hash>, Error> {
        if end < start || end - start > SHARD_LEAVES {
            return Err(Error::Capacity);
        }
        let mut query = self.db.prepare(
            "SELECT position,cmx FROM commitments WHERE position>=?1 AND position<?2 ORDER BY position",
        )?;
        let mut commitments = Vec::new();
        for row in query.query_map([start, end], |r| {
            Ok((r.get::<_, u64>(0)?, r.get::<_, Vec<u8>>(1)?))
        })? {
            let (position, cmx) = row?;
            if position != start + commitments.len() as u64 {
                return Err(Error::Coverage);
            }
            commitments.push(cmx.try_into().map_err(|_| Error::Malformed)?);
        }
        if commitments.len() as u64 != end - start {
            return Err(Error::Coverage);
        }
        Ok(commitments)
    }

    /// Payment note positions in `start..end`, ascending.
    pub fn payment_positions(&self, start: u64, end: u64) -> Result<Vec<u64>, Error> {
        let mut query = self.db.prepare(
            "SELECT position FROM payments WHERE position>=?1 AND position<?2 ORDER BY position",
        )?;
        let positions = query
            .query_map([start, end], |r| r.get(0))?
            .collect::<Result<_, _>>()?;
        Ok(positions)
    }

    /// How many Ironwood shards are recorded complete. They are always shards zero onward,
    /// recorded in order, and a rewind removes those its blocks completed.
    pub fn completed_shards(&self) -> Result<u32, Error> {
        let (count, last): (u32, Option<u32>) =
            self.db
                .query_row("SELECT COUNT(*),MAX(shard) FROM ironwood_shards", [], |r| {
                    Ok((r.get(0)?, r.get(1)?))
                })?;
        if last.map_or(0, |last| last + 1) != count {
            return Err(Error::Malformed);
        }
        Ok(count)
    }

    /// Completed shard `shard`'s root and the block holding its last leaf, if recorded.
    pub fn completed_shard(&self, shard: u32) -> Result<Option<CompletedShard>, Error> {
        self.db
            .query_row(
                "SELECT s.root,s.height,b.hash FROM ironwood_shards s JOIN blocks b ON b.height=s.height WHERE s.shard=?1",
                [shard],
                |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get(1)?, r.get::<_, Vec<u8>>(2)?)),
            )
            .optional()?
            .map(|(root, height, hash)| {
                Ok(CompletedShard {
                    root: root.try_into().map_err(|_| Error::Malformed)?,
                    height,
                    hash: hash.try_into().map_err(|_| Error::Malformed)?,
                })
            })
            .transpose()
    }

    /// Records Ironwood shard `shard`, the next incomplete one or one already recorded
    /// with the same `root`, as completed by the block holding its last leaf, and stores
    /// final `payloads` for payments in it. Payloads are the caller's to check.
    pub fn complete_shard(
        &mut self,
        shard: u32,
        root: Hash,
        payloads: &[(u64, [u8; PAYLOAD_BYTES])],
    ) -> Result<(), Error> {
        let tx = self.db.transaction()?;
        let start = u64::from(shard) * SHARD_LEAVES;
        let last = start + SHARD_LEAVES - 1;
        let height: u32 = tx
            .query_row(
                "SELECT height FROM commitments WHERE position=?1",
                [last],
                |r| r.get(0),
            )
            .optional()?
            .ok_or(Error::Coverage)?;
        let saved: Option<Vec<u8>> = tx
            .query_row(
                "SELECT root FROM ironwood_shards WHERE shard=?1",
                [shard],
                |r| r.get(0),
            )
            .optional()?;
        match saved {
            Some(saved) if saved != root => return Err(Error::Coverage),
            Some(_) => {}
            None => {
                let count: u32 =
                    tx.query_row("SELECT COUNT(*) FROM ironwood_shards", [], |r| r.get(0))?;
                if count != shard {
                    return Err(Error::Coverage);
                }
                tx.execute(
                    "INSERT INTO ironwood_shards VALUES (?1,?2,?3)",
                    params![shard, height, root.as_slice()],
                )?;
            }
        }
        for (position, payload) in payloads {
            if !(start..=last).contains(position) {
                return Err(Error::Coverage);
            }
            tx.execute(
                "INSERT OR REPLACE INTO witness_payloads VALUES (?1,?2,?3)",
                params![position, shard, payload.as_slice()],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// The final payloads recorded for `positions`, `None` where absent.
    pub fn payloads(&self, positions: &[u64]) -> Result<Vec<Option<[u8; PAYLOAD_BYTES]>>, Error> {
        let mut query = self
            .db
            .prepare("SELECT payload FROM witness_payloads WHERE position=?1")?;
        positions
            .iter()
            .map(|position| {
                query
                    .query_row([position], |r| r.get::<_, Vec<u8>>(0))
                    .optional()?
                    .map(|payload| payload.try_into().map_err(|_| Error::Malformed))
                    .transpose()
            })
            .collect()
    }

    /// The stored block at `height`, or the boundary just below the configured start.
    pub fn checkpoint(&self, height: u32) -> Result<Checkpoint, Error> {
        checkpoint_at(&self.db, &self.config, height)
    }

    /// Build all receiver pages from one SQLite read transaction and immutable anchor.
    /// A crowded bucket retries the next of [`SALT_ATTEMPTS`] salts derived from the
    /// anchor, so the result is deterministic; [`Error::Capacity`] means every salt
    /// overflowed at `rows`, or the history has more payments than `rows` has slots,
    /// found without loading more records than that. Correction revisions bind every session.
    pub fn snapshot(&mut self, rows: u32, history_revision: u64) -> Result<Snapshot, Error> {
        self.snapshot_from(self.config.start_height, rows, history_revision)
    }

    /// [`Self::snapshot`] of the blocks from `from` through the tip: a recent tier
    /// after the last sealed shard.
    pub fn snapshot_from(
        &mut self,
        from: u32,
        rows: u32,
        history_revision: u64,
    ) -> Result<Snapshot, Error> {
        let capacity = snapshot::capacity(rows)?;
        let tx = self.db.transaction()?;
        let stored_revision: u64 =
            tx.query_row("SELECT revision FROM filter_revision", [], |r| r.get(0))?;
        if history_revision != stored_revision {
            return Err(Error::Coverage);
        }
        let anchor = tip(&tx, &self.config)?;
        if from < self.config.start_height || anchor.height < from {
            return Err(Error::Coverage);
        }
        let count: u64 = tx.query_row(
            "SELECT COUNT(*) FROM payments WHERE height>=?1",
            [from],
            |r| r.get(0),
        )?;
        if count > capacity {
            return Err(Error::Capacity);
        }
        let start = checkpoint_at(&tx, &self.config, from - 1)?;
        let records = records(&tx, from, anchor.height)?;
        tx.commit()?;
        let template = Manifest {
            profile: PROFILE.into(),
            genesis: self.config.genesis,
            history_revision,
            start_height: from,
            start_parent: start.hash,
            start_position: start.position,
            end_height: anchor.height,
            end_hash: anchor.hash,
            end_position: anchor.position,
            rows,
            salt: [0; 32],
            records: 0,
            data_sha256: [0; 32],
        };
        table(
            template,
            records,
            (0..SALT_ATTEMPTS).map(|a| salt(&anchor.hash, a)),
        )
    }

    /// The stored payments in blocks `from..=to`, each a lone page, in position order.
    pub fn records(&self, from: u32, to: u32) -> Result<Vec<RawRecord>, Error> {
        records(&self.db, from, to)
    }

    /// The sealed Receiver shards, contiguous from the configured start.
    pub fn sealed_shards(&self) -> Result<Vec<SealedShard>, Error> {
        let mut query = self.db.prepare(
            "SELECT start_height,end_height,segments,rows,payloads FROM receiver_shards ORDER BY shard",
        )?;
        let shards: Vec<SealedShard> = query
            .query_map([], |r| {
                Ok(SealedShard {
                    start_height: r.get(0)?,
                    end_height: r.get(1)?,
                    segments: r.get(2)?,
                    rows: r.get(3)?,
                    payloads: r.get(4)?,
                })
            })?
            .collect::<Result<_, _>>()?;
        let mut next = self.config.start_height;
        for shard in &shards {
            if shard.start_height != next || shard.end_height < shard.start_height {
                return Err(Error::Malformed);
            }
            next = shard.end_height + 1;
        }
        Ok(shards)
    }

    /// Records `shard` as the next sealed shard: it must start right after the last
    /// one, or at the configured start, and end at a stored block. A rewind past its end
    /// or a correction at or before its end removes it.
    pub fn seal(&mut self, shard: &SealedShard) -> Result<(), Error> {
        let next = self
            .sealed_shards()?
            .last()
            .map_or(self.config.start_height, |s| s.end_height + 1);
        if shard.start_height != next
            || shard.end_height < shard.start_height
            || shard.end_height > self.tip()?.height
            || shard.segments == 0
        {
            return Err(Error::Coverage);
        }
        self.db.execute(
            "INSERT INTO receiver_shards (start_height,end_height,segments,rows,payloads) VALUES (?1,?2,?3,?4,?5)",
            params![shard.start_height, shard.end_height, shard.segments, shard.rows, shard.payloads],
        )?;
        Ok(())
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
/// The store's file in a data directory.
pub const STORE_FILE: &str = "directory-v3.sqlite";
/// The version 2 store an earlier release kept in a data directory, with 285-byte
/// records and no witness tables. [`upgrade`] copies it and never changes it.
pub const LEGACY_STORE_FILE: &str = "directory.sqlite";
/// Bytes of a version 2 record: today's record without its witness ordinal.
const LEGACY_RECORD_BYTES: usize = 285;

/// Builds the store at `path` from the version 2 store at `legacy` once, if `path` does
/// not exist yet and `legacy` does. The copy's records gain a zero witness ordinal,
/// which every snapshot reassigns, and it becomes version 3; completed Ironwood shards
/// are recorded from its retained commitments when next needed. `legacy` is left
/// unchanged, so a rollback to the earlier binary keeps working from it. Interrupted
/// conversions restart from the beginning. Returns whether it converted.
pub fn upgrade(legacy: &Path, path: &Path) -> Result<bool, Error> {
    if path.exists() || !legacy.exists() {
        return Ok(false);
    }
    let temp = path.with_extension("upgrading");
    match std::fs::remove_file(&temp) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e.into()),
        _ => {}
    }
    let source = Connection::open_with_flags(legacy, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    source.busy_timeout(std::time::Duration::from_secs(5))?;
    let version: u32 = source.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    if version != 2 {
        return Err(Error::Malformed);
    }
    let destination = temp.to_str().ok_or(Error::Malformed)?;
    source.execute("VACUUM INTO ?1", [destination])?;
    drop(source);
    let mut copy = Connection::open(&temp)?;
    let tx = copy.transaction()?;
    {
        let mut read = tx.prepare("SELECT position,record FROM payments")?;
        let mut write = tx.prepare("UPDATE payments SET record=?2 WHERE position=?1")?;
        let rows = read
            .query_map([], |r| Ok((r.get::<_, u64>(0)?, r.get::<_, Vec<u8>>(1)?)))?
            .collect::<Result<Vec<_>, _>>()?;
        for (position, mut record) in rows {
            if record.len() == LEGACY_RECORD_BYTES {
                record.resize(receiver_directory::RECORD_BYTES, 0);
                write.execute(params![position, record])?;
            }
        }
    }
    let malformed: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM payments WHERE length(record)!=?1)",
        [receiver_directory::RECORD_BYTES],
        |r| r.get(0),
    )?;
    if malformed {
        return Err(Error::Malformed);
    }
    tx.execute_batch("PRAGMA user_version=3;")?;
    tx.commit()?;
    drop(copy);
    std::fs::File::open(&temp)?.sync_all()?;
    std::fs::rename(&temp, path)?;
    if let Some(dir) = path.parent() {
        let _ = std::fs::File::open(dir).and_then(|dir| dir.sync_all());
    }
    Ok(true)
}

/// Optional provider-reported payouts, independent of chain coverage and publication.
pub struct PayoutStore {
    db: Connection,
}

impl PayoutStore {
    /// Opens the audit store, creating its tables if absent.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let db = Connection::open(path)?;
        db.busy_timeout(std::time::Duration::from_secs(5))?;
        // The previous release still reads `feeds.started_at` and keeps its global
        // `receivers` table here. Rolling back reuses this file, so this release keeps
        // both and never reads or writes them. A later release drops them.
        db.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS feeds (feed TEXT PRIMARY KEY, started_at INTEGER,
                cursor INTEGER NOT NULL, read_at INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS payouts (receiver BLOB NOT NULL CHECK(length(receiver)=43),
                txid BLOB NOT NULL CHECK(length(txid)=32), seen_at INTEGER NOT NULL,
                PRIMARY KEY(receiver,txid)) WITHOUT ROWID;",
        )?;
        Ok(Self { db })
    }

    /// Commits completed payouts and their cursor atomically, keeping the first observation.
    pub fn record(
        &mut self,
        feed: &str,
        completions: &[(ReceiverBytes, Hash, i64)],
        cursor: i64,
        read_at: i64,
    ) -> Result<(), Error> {
        let tx = self.db.transaction()?;
        tx.execute(
            "INSERT INTO feeds (feed,cursor,read_at) VALUES (?1,?2,?3) ON CONFLICT(feed)
             DO UPDATE SET cursor=excluded.cursor, read_at=excluded.read_at
             WHERE excluded.cursor>cursor OR (excluded.cursor=cursor AND excluded.read_at>read_at)",
            params![feed, cursor, read_at],
        )?;
        for (receiver, txid, seen_at) in completions {
            tx.execute(
                "INSERT INTO payouts VALUES (?1,?2,?3) ON CONFLICT(receiver,txid)
                 DO UPDATE SET seen_at=MIN(seen_at,excluded.seen_at)",
                params![receiver.as_slice(), txid.as_slice(), seen_at],
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
}

/// Checks and stores `block`'s payments as [`Store::append`] describes, each as a lone
/// page whose snapshot numbers its pages and witness ordinal.
fn insert_payments(tx: &Connection, block: &IndexedBlock) -> Result<(), Error> {
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
        let record = encode(&RawRecord {
            receiver: *receiver,
            page: 0,
            total: 1,
            payment: p.clone(),
            ordinal: 0,
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
    Ok(())
}

/// See [`Store::records`].
fn records(db: &Connection, from: u32, to: u32) -> Result<Vec<RawRecord>, Error> {
    let mut query = db
        .prepare("SELECT record FROM payments WHERE height>=?1 AND height<=?2 ORDER BY position")?;
    let records = query
        .query_map([from, to], |r| r.get::<_, Vec<u8>>(0))?
        .map(|bytes| decode(&bytes?)?.ok_or(Error::Malformed))
        .collect();
    records
}

/// Numbers each receiver's pages in position order, ranks every record's witness
/// ordinal by position, and places `records` under the first of `salts` whose buckets
/// all fit `template`'s rows. [`Error::Capacity`] when none does.
pub fn table(
    template: Manifest,
    mut records: Vec<RawRecord>,
    salts: impl IntoIterator<Item = Hash>,
) -> Result<Snapshot, Error> {
    records.sort_by_key(|r| (r.receiver, r.payment.position));
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
    snapshot::assign_ordinals(&mut records);
    for salt in salts {
        let manifest = Manifest {
            salt,
            ..template.clone()
        };
        match Snapshot::build(manifest, &records) {
            Err(receiver_directory::Error::Capacity) => continue,
            built => return Ok(built?),
        }
    }
    Err(Error::Capacity)
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
/// See [`Store::checkpoint`].
fn checkpoint_at(db: &Connection, c: &Config, height: u32) -> Result<Checkpoint, Error> {
    if height == c.start_height - 1 {
        return Ok(boundary(c));
    }
    db.query_row(
        "SELECT height,hash,end_position FROM blocks WHERE height=?1",
        [height],
        checkpoint_row,
    )?
    .ok_or(Error::Malformed)
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
