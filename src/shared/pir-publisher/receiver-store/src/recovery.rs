//! The recovery directory's durable state, kept in its own database apart from chain
//! coverage so that no rewind, rebuild of the index or restart loses it: the
//! payment-history epoch, the seen set's union and the membership of every bucket
//! published. Carry the database to a replacement host, or rebuild it there from the
//! live publication with [`RecoveryStore::adopt`].
use crate::Error;
use receiver_directory::{catalog::CatalogHead, seen::SeenSet, Hash};
use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;

/// One interval of payment history as last published: its start, the hashes of the
/// blocks at both ends, and a digest of the distinct receivers paid in it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Segment {
    pub start: u32,
    pub start_hash: Hash,
    pub end_hash: Hash,
    pub members: Hash,
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
                members BLOB NOT NULL CHECK(length(members)=32)) WITHOUT ROWID;
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
        Ok(self
            .db
            .query_row("SELECT epoch FROM state", [], |r| r.get(0))?)
    }

    /// The seen union last recorded, if any.
    pub fn seen(&self) -> Result<Option<SeenSet>, Error> {
        let bytes: Option<Vec<u8>> = self
            .db
            .query_row("SELECT seen FROM state", [], |r| r.get(0))?;
        bytes.map(|b| Ok(SeenSet::decode(&b)?)).transpose()
    }

    /// Records a publication's history and seen set before it is published, atomically,
    /// and returns the epoch to build it under. A segment whose start and boundary
    /// hashes match the recorded one but whose members differ is a correction of
    /// published history, which increments the epoch. Different boundary hashes are a
    /// reorg and only replace the record. `seen` must hold every value of the recorded
    /// union, or nothing is recorded and the result is [`Error::Coverage`].
    pub fn commit(&mut self, segments: &[Segment], seen: &SeenSet) -> Result<u64, Error> {
        let encoded = seen.encode()?;
        let tx = self
            .db
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        check_superset(&tx, &self.genesis, seen)?;
        let mut corrected = false;
        for segment in segments {
            let recorded: Option<(Vec<u8>, Vec<u8>, Vec<u8>)> = tx
                .query_row(
                    "SELECT start_hash,end_hash,members FROM segments WHERE start=?1",
                    [segment.start],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .optional()?;
            corrected |= recorded.is_some_and(|(start, end, members)| {
                start == segment.start_hash && end == segment.end_hash && members != segment.members
            });
            tx.execute(
                "INSERT OR REPLACE INTO segments VALUES (?1,?2,?3,?4)",
                params![
                    segment.start,
                    segment.start_hash.as_slice(),
                    segment.end_hash.as_slice(),
                    segment.members.as_slice()
                ],
            )?;
        }
        if corrected {
            tx.execute("UPDATE state SET epoch=epoch+1", [])?;
        }
        tx.execute("UPDATE state SET seen=?1", [encoded])?;
        let epoch = tx.query_row("SELECT epoch FROM state", [], |r| r.get(0))?;
        tx.commit()?;
        Ok(epoch)
    }

    /// Increments the epoch, for a correction to published history that the recorded
    /// segments cannot reveal, and returns the new epoch.
    pub fn bump_epoch(&mut self) -> Result<u64, Error> {
        let tx = self.db.transaction()?;
        tx.execute("UPDATE state SET epoch=epoch+1", [])?;
        let epoch = tx.query_row("SELECT epoch FROM state", [], |r| r.get(0))?;
        tx.commit()?;
        Ok(epoch)
    }

    /// Takes over from a publication made elsewhere, as on a replacement host: raises
    /// the epoch to `head`'s if it is lower, and adopts `seen`, which must be the set
    /// `head` names, when it holds every value of the recorded union. A recorded union
    /// holding every value of `seen` is kept. Any other combination is
    /// [`Error::Coverage`] and changes nothing.
    pub fn adopt(&mut self, head: &CatalogHead, seen: &SeenSet) -> Result<(), Error> {
        head.check_seen(seen)?;
        if head.genesis != self.genesis {
            return Err(Error::Coverage);
        }
        let encoded = seen.encode()?;
        let tx = self.db.transaction()?;
        match check_superset(&tx, &self.genesis, seen) {
            Ok(()) => {
                tx.execute("UPDATE state SET seen=?1", [encoded])?;
            }
            Err(Error::Coverage) => {
                let recorded = recorded_seen(&tx)?.expect("a recorded union");
                if !recorded.is_superset(seen) {
                    return Err(Error::Coverage);
                }
            }
            Err(error) => return Err(error),
        }
        tx.execute(
            "UPDATE state SET epoch=MAX(epoch,?1)",
            [i64::try_from(head.epoch).map_err(|_| Error::Malformed)?],
        )?;
        tx.commit()?;
        Ok(())
    }
}

/// The seen union recorded in `tx`'s view, if any.
fn recorded_seen(tx: &rusqlite::Transaction<'_>) -> Result<Option<SeenSet>, Error> {
    let bytes: Option<Vec<u8>> = tx.query_row("SELECT seen FROM state", [], |r| r.get(0))?;
    bytes.map(|b| Ok(SeenSet::decode(&b)?)).transpose()
}

/// Fails with [`Error::Coverage`] unless `seen` is on the network with `genesis` and
/// holds every value of the union recorded in `tx`'s view.
fn check_superset(
    tx: &rusqlite::Transaction<'_>,
    genesis: &Hash,
    seen: &SeenSet,
) -> Result<(), Error> {
    if seen.genesis() != *genesis
        || recorded_seen(tx)?.is_some_and(|recorded| !seen.is_superset(&recorded))
    {
        return Err(Error::Coverage);
    }
    Ok(())
}
