//! Atomic contiguous coverage with explicit rewind. Empty blocks are retained too.
use crate::{
    snapshot::{Manifest, Snapshot, PROFILE},
    Error, Hash, Payment, Receiver, Record,
};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Config {
    pub genesis: Hash,
    pub start_height: u32,
    pub start_parent: Hash,
    pub start_position: u64,
}

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
}

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
        if version > 1 {
            return Err(Error::Malformed);
        }
        tx.execute_batch("CREATE TABLE IF NOT EXISTS config (id INTEGER PRIMARY KEY CHECK(id=1), value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS blocks (height INTEGER PRIMARY KEY, hash BLOB NOT NULL, end_position INTEGER NOT NULL, coinbase_actions INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS payments (height INTEGER NOT NULL REFERENCES blocks(height) ON DELETE CASCADE,
                receiver BLOB NOT NULL, position INTEGER NOT NULL UNIQUE, txid BLOB NOT NULL, action INTEGER NOT NULL, record BLOB NOT NULL,
                UNIQUE(txid,action));
            CREATE INDEX IF NOT EXISTS receiver_payments ON payments(receiver,position);
            PRAGMA user_version=1;")?;
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

    pub fn tip(&self) -> Result<Checkpoint, Error> {
        tip(&self.db, &self.config)
    }

    /// Reject gaps, changed parents and malformed records before advancing coverage.
    pub fn append(&mut self, block: &IndexedBlock) -> Result<(), Error> {
        let tx = self
            .db
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let previous = tip(&tx, &self.config)?;
        if previous.height.checked_add(1) != Some(block.height)
            || previous.hash != block.parent
            || previous.position != block.start_position
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
        let mut previous_position = None;
        for (receiver, p) in &block.payments {
            if p.height != block.height
                || p.block_hash != block.hash
                || p.position < block.start_position
                || p.position >= block.end_position
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
    pub fn snapshot(&mut self, rows: u32) -> Result<Snapshot, Error> {
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
            salt: anchor.hash,
            records: 0,
            data_sha256: [0; 32],
        };
        tx.commit()?;
        Snapshot::build(manifest, &records)
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
}
fn boundary(c: &Config) -> Checkpoint {
    Checkpoint {
        height: c.start_height - 1,
        hash: c.start_parent,
        position: c.start_position,
    }
}
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
