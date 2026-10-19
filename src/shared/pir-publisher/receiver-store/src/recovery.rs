//! The recovery directory's durable state, kept in its own database apart from chain
//! coverage so that no rewind, rebuild of the index or restart loses it: the
//! payment-history epoch, the seen set's union and the payment records of every
//! interval published. Carry the database to a replacement host and
//! [`RecoveryStore::import`] it there, or, without it, [`RecoveryStore::adopt`] the
//! live publication.
use crate::Error;
use receiver_directory::{catalog::CatalogHead, seen::SeenSet, Hash};
use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;

/// One interval of payment history as last published: its start, the hashes of the
/// blocks at both ends, a digest of its payment records in position order, and each
/// block in it holding payments.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Segment {
    pub start: u32,
    pub start_hash: Hash,
    pub end_hash: Hash,
    pub records: Hash,
    pub blocks: Vec<Block>,
}

/// A block holding payments: its height, its hash and a digest of its payment
/// records in position order.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Block {
    pub height: u32,
    pub hash: Hash,
    pub records: Hash,
}

/// The payment-history epoch, the seen union and the published segments of one
/// network.
pub struct RecoveryStore {
    db: Connection,
    genesis: Hash,
}

impl RecoveryStore {
    /// Opens or creates the database, permanently bound to the network with `genesis`
    /// at epoch zero. The database itself refuses lowering the epoch or deleting its
    /// state.
    pub fn open(path: impl AsRef<Path>, genesis: Hash) -> Result<Self, Error> {
        let mut db = Connection::open(path)?;
        db.busy_timeout(std::time::Duration::from_secs(5))?;
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")?;
        let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        tx.execute_batch(
            "CREATE TABLE IF NOT EXISTS state (id INTEGER PRIMARY KEY CHECK(id=1),
                genesis BLOB NOT NULL CHECK(length(genesis)=32), epoch INTEGER NOT NULL CHECK(epoch>=0),
                seen BLOB);
            CREATE TABLE IF NOT EXISTS segments (start INTEGER PRIMARY KEY,
                start_hash BLOB NOT NULL CHECK(length(start_hash)=32),
                end_hash BLOB NOT NULL CHECK(length(end_hash)=32),
                records BLOB NOT NULL CHECK(length(records)=32)) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS blocks (height INTEGER PRIMARY KEY,
                start INTEGER NOT NULL REFERENCES segments(start),
                hash BLOB NOT NULL CHECK(length(hash)=32),
                records BLOB NOT NULL CHECK(length(records)=32)) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS segment_blocks ON blocks(start);
            CREATE TRIGGER IF NOT EXISTS epoch_rises BEFORE UPDATE OF epoch ON state
                WHEN NEW.epoch<OLD.epoch BEGIN SELECT RAISE(ABORT, 'the epoch only increases'); END;
            CREATE TRIGGER IF NOT EXISTS state_kept BEFORE DELETE ON state
                BEGIN SELECT RAISE(ABORT, 'recovery state is permanent'); END;",
        )?;
        tx.execute(
            "INSERT OR IGNORE INTO state VALUES (1,?1,0,NULL)",
            [genesis.as_slice()],
        )?;
        let saved: Vec<u8> = tx.query_row("SELECT genesis FROM state", [], |r| r.get(0))?;
        if saved != genesis {
            return Err(Error::Coverage);
        }
        tx.commit()?;
        Ok(Self { db, genesis })
    }

    /// The payment-history epoch.
    pub fn epoch(&self) -> Result<u64, Error> {
        epoch(&self.db)
    }

    /// The seen union last recorded, if any.
    pub fn seen(&self) -> Result<Option<SeenSet>, Error> {
        recorded_seen(&self.db)
    }

    /// Records a publication's history and seen set before it is published, atomically,
    /// and returns the epoch to build it under. `hash_at` gives the publication's block
    /// hash at a height it covers.
    ///
    /// A changed published payment record increments the epoch: a segment with the
    /// recorded boundary hashes but other records, or, in a segment whose start hash
    /// is recorded but whose end hash changed (a reorg inside it), a change in the
    /// blocks up to the last recorded payment block still on the chain, below which
    /// every block is unchanged. A changed start hash is a reorg of the whole segment,
    /// which only replaces the record. A correction that adds payments to a block
    /// without recorded ones, above that last block, in a segment a reorg also
    /// changed, goes unseen and needs [`Self::bump_epoch`].
    ///
    /// `seen` must hold every value of the recorded union, or nothing is recorded and
    /// the result is [`Error::Coverage`].
    pub fn commit(
        &mut self,
        segments: &[Segment],
        seen: &SeenSet,
        hash_at: impl Fn(u32) -> Result<Hash, Error>,
    ) -> Result<u64, Error> {
        let encoded = seen.encode()?;
        let tx = self
            .db
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        check_superset(&tx, &self.genesis, seen)?;
        let mut corrected = false;
        for segment in segments {
            let Some(recorded) = recorded_segment(&tx, segment.start)? else {
                write_segment(&tx, segment)?;
                continue;
            };
            if recorded == (segment.start_hash, segment.end_hash, segment.records) {
                continue;
            }
            if (recorded.0, recorded.1) == (segment.start_hash, segment.end_hash) {
                corrected = true;
            } else if recorded.0 == segment.start_hash {
                let old = recorded_blocks(&tx, segment.start)?;
                let mut unchanged = None;
                for block in &old {
                    if hash_at(block.height)? == block.hash {
                        unchanged = Some(block.height);
                    }
                }
                if let Some(last) = unchanged {
                    let below = |blocks: &[Block]| -> Vec<Block> {
                        blocks
                            .iter()
                            .filter(|b| b.height <= last)
                            .cloned()
                            .collect()
                    };
                    corrected |= below(&old) != below(&segment.blocks);
                }
            }
            write_segment(&tx, segment)?;
        }
        if corrected {
            tx.execute("UPDATE state SET epoch=epoch+1", [])?;
        }
        tx.execute("UPDATE state SET seen=?1", [encoded])?;
        let epoch = epoch(&tx)?;
        tx.commit()?;
        Ok(epoch)
    }

    /// Increments the epoch, for a correction to published history that the recorded
    /// segments cannot reveal (see [`Self::commit`]), and returns the new epoch.
    pub fn bump_epoch(&mut self) -> Result<u64, Error> {
        let tx = self.db.transaction()?;
        tx.execute("UPDATE state SET epoch=epoch+1", [])?;
        let epoch = epoch(&tx)?;
        tx.commit()?;
        Ok(epoch)
    }

    /// Takes over from a publication made elsewhere whose recovery database is lost:
    /// merges `seen`, which must be the set `head` names (see [`merge_seen`]), and
    /// raises the epoch above `head`'s, since a correction made since cannot be compared
    /// with the unknown published records. Any refusal changes nothing.
    pub fn adopt(&mut self, head: &CatalogHead, seen: &SeenSet) -> Result<(), Error> {
        head.check_seen(seen)?;
        let tx = self.db.transaction()?;
        merge_seen(&tx, &self.genesis, seen)?;
        let above = head.epoch.checked_add(1).ok_or(Error::Malformed)?;
        raise_epoch(&tx, above)?;
        tx.commit()?;
        Ok(())
    }

    /// Takes over another host's recovery database at `path`, on the same network:
    /// merges its seen union (see [`merge_seen`]), raises the epoch to its own, and
    /// takes the segments this database lacks as the baseline [`Self::commit`] compares
    /// later publications with. A segment both hold that differs in any way, even only
    /// in its boundary hashes, as when the two hosts published different forks, keeps
    /// this database's record and increments the epoch above both, since a later
    /// correction to either fork cannot be compared with the other's published records.
    /// Any refusal changes nothing.
    pub fn import(&mut self, path: impl AsRef<Path>) -> Result<(), Error> {
        let other = Self::open(path, self.genesis)?;
        let tx = self.db.transaction()?;
        if let Some(seen) = other.seen()? {
            merge_seen(&tx, &self.genesis, &seen)?;
        }
        let mut corrected = false;
        let starts = other
            .db
            .prepare("SELECT start FROM segments")?
            .query_map([], |r| r.get::<_, u32>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        for start in starts {
            let theirs = recorded_segment(&other.db, start)?.expect("a listed segment");
            match recorded_segment(&tx, start)? {
                None => write_segment(
                    &tx,
                    &Segment {
                        start,
                        start_hash: theirs.0,
                        end_hash: theirs.1,
                        records: theirs.2,
                        blocks: recorded_blocks(&other.db, start)?,
                    },
                )?,
                Some(ours) => corrected |= ours != theirs,
            }
        }
        raise_epoch(&tx, other.epoch()?)?;
        if corrected {
            tx.execute("UPDATE state SET epoch=epoch+1", [])?;
        }
        tx.commit()?;
        Ok(())
    }
}

/// The epoch recorded in `db`.
fn epoch(db: &Connection) -> Result<u64, Error> {
    Ok(db.query_row("SELECT epoch FROM state", [], |r| r.get(0))?)
}

/// Raises the epoch recorded in `db` to `at_least` if it is lower.
fn raise_epoch(db: &Connection, at_least: u64) -> Result<(), Error> {
    let at_least = i64::try_from(at_least).map_err(|_| Error::Malformed)?;
    db.execute("UPDATE state SET epoch=MAX(epoch,?1)", [at_least])?;
    Ok(())
}

/// The seen union recorded in `db`, if any.
fn recorded_seen(db: &Connection) -> Result<Option<SeenSet>, Error> {
    let bytes: Option<Vec<u8>> = db.query_row("SELECT seen FROM state", [], |r| r.get(0))?;
    bytes.map(|b| Ok(SeenSet::decode(&b)?)).transpose()
}

/// Fails with [`Error::Coverage`] unless `seen` is on the network with `genesis` and
/// holds every value of the union recorded in `db`.
fn check_superset(db: &Connection, genesis: &Hash, seen: &SeenSet) -> Result<(), Error> {
    if seen.genesis() != *genesis
        || recorded_seen(db)?.is_some_and(|recorded| !seen.is_superset(&recorded))
    {
        return Err(Error::Coverage);
    }
    Ok(())
}

/// Records `seen` in `db` if it holds every value of the recorded union, and keeps a
/// recorded union holding every value of it. Since the format has no union of two
/// sets, any other pair is [`Error::Coverage`].
fn merge_seen(db: &Connection, genesis: &Hash, seen: &SeenSet) -> Result<(), Error> {
    match check_superset(db, genesis, seen) {
        Ok(()) => {
            db.execute("UPDATE state SET seen=?1", [seen.encode()?])?;
            Ok(())
        }
        Err(Error::Coverage)
            if seen.genesis() == *genesis
                && recorded_seen(db)?.is_some_and(|recorded| recorded.is_superset(seen)) =>
        {
            Ok(())
        }
        Err(error) => Err(error),
    }
}

/// The boundary hashes and records digest recorded in `db` for the segment at `start`.
fn recorded_segment(db: &Connection, start: u32) -> Result<Option<(Hash, Hash, Hash)>, Error> {
    let row: Option<(Vec<u8>, Vec<u8>, Vec<u8>)> = db
        .query_row(
            "SELECT start_hash,end_hash,records FROM segments WHERE start=?1",
            [start],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?;
    row.map(|(s, e, r)| Ok((hash(s)?, hash(e)?, hash(r)?)))
        .transpose()
}

/// The blocks recorded in `db` for the segment at `start`, ascending.
fn recorded_blocks(db: &Connection, start: u32) -> Result<Vec<Block>, Error> {
    let mut query =
        db.prepare("SELECT height,hash,records FROM blocks WHERE start=?1 ORDER BY height")?;
    let rows = query.query_map([start], |r| {
        Ok((r.get(0)?, r.get::<_, Vec<u8>>(1)?, r.get::<_, Vec<u8>>(2)?))
    })?;
    rows.map(|row| {
        let (height, h, records) = row?;
        Ok(Block {
            height,
            hash: hash(h)?,
            records: hash(records)?,
        })
    })
    .collect()
}

/// Replaces the segment at `segment.start` and its blocks in `db`.
fn write_segment(db: &Connection, segment: &Segment) -> Result<(), Error> {
    db.execute("DELETE FROM blocks WHERE start=?1", [segment.start])?;
    db.execute(
        "INSERT OR REPLACE INTO segments VALUES (?1,?2,?3,?4)",
        params![
            segment.start,
            segment.start_hash.as_slice(),
            segment.end_hash.as_slice(),
            segment.records.as_slice()
        ],
    )?;
    for block in &segment.blocks {
        db.execute(
            "INSERT OR REPLACE INTO blocks VALUES (?1,?2,?3,?4)",
            params![
                block.height,
                segment.start,
                block.hash.as_slice(),
                block.records.as_slice()
            ],
        )?;
    }
    Ok(())
}

/// A stored 32-byte hash.
fn hash(bytes: Vec<u8>) -> Result<Hash, Error> {
    bytes.try_into().map_err(|_| Error::Malformed)
}
