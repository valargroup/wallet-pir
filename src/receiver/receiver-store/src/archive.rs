//! Durable canonical filter history, separate from the rebuildable chain database.
//!
//! Every block keeps a record fingerprint, including empty blocks. Comparing these
//! before publication detects a correction below a fork even when it adds the first
//! payment to a formerly empty block. Replacing this database creates a new archive
//! identity rather than reusing an epoch whose continuity was lost.
use crate::{Config, Error};
use receiver_directory::{
    archive::{Coverage, Head, Index, Page, SeenRef, PAGE_BLOCKS},
    blocks::{BlockFilter, BlockId},
    seen::SeenSet,
    Hash, ReceiverBytes,
};
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use std::{collections::BTreeMap, path::Path};

/// Bound on the server's in-memory range index. Refuse growth, never truncate history.
pub const MAX_ARCHIVE_PAGES: usize = 65_536;
/// Keep immutable heads and their pages available for at least three minutes.
pub const HEAD_GRACE_SECS: i64 = 180;

/// One validated source block, including a fingerprint of all ordered payment records.
pub struct Block {
    pub id: BlockId,
    pub parent: Hash,
    pub records: Hash,
    pub receivers: Vec<ReceiverBytes>,
}

/// State that must travel with a publisher during failover, including the seen union.
pub struct ArchiveStore {
    db: Connection,
    config: Config,
}

impl ArchiveStore {
    /// Opens a network-bound archive, generating an identity only for a new database.
    pub fn open(path: impl AsRef<Path>, config: Config) -> Result<Self, Error> {
        let mut db = Connection::open(path)?;
        db.busy_timeout(std::time::Duration::from_secs(5))?;
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")?;
        let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        tx.execute_batch("CREATE TABLE IF NOT EXISTS archive_state (
            id INTEGER PRIMARY KEY CHECK(id=1), config TEXT NOT NULL,
            identity BLOB NOT NULL CHECK(length(identity)=32), epoch INTEGER NOT NULL CHECK(epoch>=0),
            seen BLOB, active BLOB, staged BLOB);
            CREATE TABLE IF NOT EXISTS archive_blocks (height INTEGER PRIMARY KEY,
                hash BLOB NOT NULL CHECK(length(hash)=32), records BLOB NOT NULL CHECK(length(records)=32));
            CREATE TABLE IF NOT EXISTS archive_fingerprints (
                height INTEGER NOT NULL, hash BLOB NOT NULL CHECK(length(hash)=32), records BLOB NOT NULL CHECK(length(records)=32),
                PRIMARY KEY(height,hash));
            INSERT OR IGNORE INTO archive_fingerprints SELECT height,hash,records FROM archive_blocks;
            CREATE TABLE IF NOT EXISTS archive_pages (digest BLOB PRIMARY KEY CHECK(length(digest)=32), bytes BLOB NOT NULL);
            CREATE TABLE IF NOT EXISTS archive_seen (digest BLOB PRIMARY KEY CHECK(length(digest)=32), bytes BLOB NOT NULL);
            CREATE TABLE IF NOT EXISTS archive_heads (digest BLOB PRIMARY KEY CHECK(length(digest)=32),
                bytes BLOB NOT NULL, leaves BLOB NOT NULL, created INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS archive_coverage (identity BLOB NOT NULL, epoch INTEGER NOT NULL,
                session BLOB NOT NULL, bytes BLOB NOT NULL, PRIMARY KEY(identity,epoch,session));
            CREATE TRIGGER IF NOT EXISTS archive_epoch_rises BEFORE UPDATE OF epoch ON archive_state
                WHEN NEW.epoch<OLD.epoch BEGIN SELECT RAISE(ABORT, 'archive epoch cannot decrease'); END;
            CREATE TRIGGER IF NOT EXISTS archive_identity_fixed BEFORE UPDATE OF identity,config ON archive_state
                BEGIN SELECT RAISE(ABORT, 'archive identity is permanent'); END;
            CREATE TRIGGER IF NOT EXISTS archive_state_kept BEFORE DELETE ON archive_state
                BEGIN SELECT RAISE(ABORT, 'archive continuity is permanent'); END;")?;
        let encoded = serde_json::to_string(&config)?;
        tx.execute(
            "INSERT OR IGNORE INTO archive_state VALUES (1,?1,randomblob(32),0,NULL,NULL,NULL)",
            [&encoded],
        )?;
        let saved: String = tx.query_row("SELECT config FROM archive_state", [], |r| r.get(0))?;
        if serde_json::from_str::<Config>(&saved)? != config {
            return Err(Error::Coverage);
        }
        tx.commit()?;
        Ok(Self { db, config })
    }

    /// Opens an existing archive without taking a write lock or creating missing history.
    pub fn read(path: impl AsRef<Path>, config: Config) -> Result<Self, Error> {
        let db = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        db.busy_timeout(std::time::Duration::from_secs(5))?;
        let saved: String = db.query_row("SELECT config FROM archive_state", [], |r| r.get(0))?;
        if serde_json::from_str::<Config>(&saved)? != config {
            return Err(Error::Coverage);
        }
        Ok(Self { db, config })
    }

    /// Revokes only the head checked by an independent guard, preserving a newer activation.
    pub fn revoke_if_head(&mut self, head: &Head, now: i64) -> Result<(), Error> {
        let tx = self
            .db
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        retire(&tx, now)?;
        tx.execute(
            "UPDATE archive_state SET active=NULL WHERE active=?1",
            [head.digest()?.as_slice()],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Stages a complete canonical source snapshot and its index in one transaction.
    /// Callers independently recheck the frontier before `activate`. Missing provider
    /// coverage omits seen from the head but never blocks payment publication.
    pub fn stage(
        &mut self,
        blocks: impl IntoIterator<Item = Result<Block, Error>>,
        seen: Option<SeenRef>,
        now: i64,
    ) -> Result<Head, Error> {
        let tx = self
            .db
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let (identity, old_epoch): (Vec<u8>, u64) =
            tx.query_row("SELECT identity,epoch FROM archive_state", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })?;
        let mut expected = Some(self.config.start_height);
        let mut parent = self.config.start_parent;
        let mut page_parent = parent;
        let mut filters = Vec::new();
        let mut leaves = BTreeMap::new();
        let mut corrected = false;
        let mut forked = false;
        let mut frontier = None;
        for block in blocks {
            let block = block?;
            if Some(block.id.height) != expected || block.parent != parent {
                return Err(Error::Coverage);
            }
            if !filters.is_empty() && block.id.height % PAGE_BLOCKS == 0 {
                store_page(&tx, page_parent, std::mem::take(&mut filters), &mut leaves)?;
                page_parent = parent;
            }
            let old: Option<Vec<u8>> = tx
                .query_row(
                    "SELECT records FROM archive_fingerprints WHERE height=?1 AND hash=?2",
                    params![block.id.height, block.id.hash.as_slice()],
                    |r| r.get(0),
                )
                .optional()?;
            corrected |= old.is_some_and(|records| records != block.records);
            let canonical: Option<Vec<u8>> = tx
                .query_row(
                    "SELECT hash FROM archive_blocks WHERE height=?1",
                    [block.id.height],
                    |r| r.get(0),
                )
                .optional()?;
            forked |= canonical.is_some_and(|hash| hash != block.id.hash);
            // A shortened rebuild or an intervening fork must not erase the comparison
            // baseline a returning offline wallet may still rely on.
            tx.execute(
                "INSERT INTO archive_fingerprints VALUES (?1,?2,?3)
                ON CONFLICT(height,hash) DO UPDATE SET records=excluded.records",
                params![
                    block.id.height,
                    block.id.hash.as_slice(),
                    block.records.as_slice()
                ],
            )?;
            tx.execute(
                "INSERT INTO archive_blocks VALUES (?1,?2,?3) ON CONFLICT(height)
                DO UPDATE SET hash=excluded.hash,records=excluded.records",
                params![
                    block.id.height,
                    block.id.hash.as_slice(),
                    block.records.as_slice()
                ],
            )?;
            filters.push(BlockFilter::build(
                self.config.genesis,
                block.id,
                &block.receivers,
            )?);
            parent = block.id.hash;
            frontier = Some(block.id);
            // A final block at the maximum height is allowed, but no following item.
            expected = block.id.height.checked_add(1);
        }
        let frontier = frontier.ok_or(Error::Coverage)?;
        let removed = tx.execute(
            "DELETE FROM archive_blocks WHERE height>?1",
            [frontier.height],
        )?;
        store_page(&tx, page_parent, filters, &mut leaves)?;
        let epoch = old_epoch
            .checked_add(u64::from(corrected))
            .filter(|v| *v <= i64::MAX as u64)
            .ok_or(Error::Capacity)?;
        if corrected || forked || removed > 0 {
            retire(&tx, now)?;
        }
        tx.execute(
            "UPDATE archive_state SET epoch=?1, active=CASE WHEN ?2 THEN NULL ELSE active END",
            params![epoch, corrected || forked || removed > 0],
        )?;
        if let Some(reference) = &seen {
            let bytes: Vec<u8> = tx.query_row(
                "SELECT bytes FROM archive_seen WHERE digest=?1",
                [reference.digest.as_slice()],
                |r| r.get(0),
            )?;
            reference.check(&self.config.genesis, &SeenSet::decode(&bytes)?)?;
        }
        let index = index(&leaves)?;
        let head = Head {
            genesis: self.config.genesis,
            archive: hash(identity)?,
            epoch,
            first_height: self.config.start_height,
            frontier,
            index_root: index.root(),
            seen,
        };
        let digest = head.digest()?;
        tx.execute(
            "INSERT OR IGNORE INTO archive_heads VALUES (?1,?2,?3,?4)",
            params![
                digest.as_slice(),
                head.encode()?,
                encode_leaves(&leaves),
                now
            ],
        )?;
        tx.execute("UPDATE archive_state SET staged=?1", [digest.as_slice()])?;
        tx.commit()?;
        Ok(head)
    }

    /// Persists a complete, monotone provider union independently of payment history.
    /// A rejected or unavailable provider capture must not stop callers staging paid filters.
    pub fn record_seen(&mut self, seen: &SeenSet) -> Result<SeenRef, Error> {
        if seen.genesis() != self.config.genesis {
            return Err(Error::Coverage);
        }
        let tx = self
            .db
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let previous: Option<Vec<u8>> =
            tx.query_row("SELECT seen FROM archive_state", [], |r| r.get(0))?;
        if previous
            .as_ref()
            .map(|b| SeenSet::decode(b))
            .transpose()?
            .as_ref()
            .is_some_and(|old| !seen.is_superset(old))
        {
            return Err(Error::Coverage);
        }
        let bytes = seen.encode()?;
        let digest = seen.digest()?;
        tx.execute(
            "INSERT OR IGNORE INTO archive_seen VALUES (?1,?2)",
            params![digest.as_slice(), &bytes],
        )?;
        tx.execute("UPDATE archive_state SET seen=?1", [&bytes])?;
        tx.commit()?;
        Ok(SeenRef {
            digest,
            since_unix: seen.since_unix,
            until_unix: seen.until_unix,
        })
    }

    /// Activates only the most recent durable candidate after independent chain validation.
    pub fn activate(&mut self, head: &Head, now: i64) -> Result<(), Error> {
        let digest = head.digest()?;
        let tx = self
            .db
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        retire(&tx, now)?;
        let changed = tx.execute(
            "UPDATE archive_state SET active=staged WHERE staged=?1 AND identity=?2 AND epoch=?3",
            params![digest.as_slice(), head.archive.as_slice(), head.epoch],
        )?;
        if changed != 1 {
            return Err(Error::Coverage);
        }
        tx.commit()?;
        Ok(())
    }

    /// Returns the complete current head, or none while history is pending or revoked.
    pub fn head(&self) -> Result<Option<Head>, Error> {
        let row: Option<(Vec<u8>, Vec<u8>)> = self.db.query_row(
            "SELECT h.digest,h.bytes FROM archive_state s JOIN archive_heads h ON h.digest=s.active",
            [], |r| Ok((r.get(0)?,r.get(1)?)),
        ).optional()?;
        row.map(|(digest, bytes)| {
            let head = Head::decode(&bytes)?;
            if head.digest()?.as_slice() != digest {
                return Err(Error::Malformed);
            }
            Ok(head)
        })
        .transpose()
    }

    /// Revokes the advertised frontier while retaining immutable history for the grace period.
    pub fn revoke(&mut self, now: i64) -> Result<(), Error> {
        let tx = self
            .db
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        retire(&tx, now)?;
        tx.execute("UPDATE archive_state SET active=NULL,staged=NULL", [])?;
        tx.commit()?;
        Ok(())
    }

    /// Reads one pinned head and page in a transaction that protects them from concurrent GC.
    pub fn range(&mut self, digest: Hash, number: u32) -> Result<Vec<u8>, Error> {
        let tx = self.db.transaction()?;
        let (bytes, leaves): (Vec<u8>, Vec<u8>) = tx
            .query_row(
                "SELECT bytes,leaves FROM archive_heads WHERE digest=?1",
                [digest.as_slice()],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?
            .ok_or(Error::Coverage)?;
        let head = Head::decode(&bytes)?;
        if head.digest()? != digest {
            return Err(Error::Malformed);
        }
        head.page_span(number)?;
        let leaves = decode_leaves(&leaves)?;
        let page_digest = leaves.get(&number).ok_or(Error::Coverage)?;
        let bytes: Vec<u8> = tx.query_row(
            "SELECT bytes FROM archive_pages WHERE digest=?1",
            [page_digest.as_slice()],
            |r| r.get(0),
        )?;
        let page = Page::decode(&bytes)?;
        if page.digest()? != *page_digest {
            return Err(Error::Malformed);
        }
        let index = index(&leaves)?;
        let proof = index.proof(number)?;
        proof.verify(&head, &page)?;
        let response = proof.encode(&page)?;
        tx.commit()?;
        Ok(response)
    }

    /// Reads a content-addressed seen object without inferring completeness from absence.
    pub fn seen(&self, digest: Hash) -> Result<Vec<u8>, Error> {
        let bytes: Vec<u8> = self
            .db
            .query_row(
                "SELECT bytes FROM archive_seen WHERE digest=?1",
                [digest.as_slice()],
                |r| r.get(0),
            )
            .optional()?
            .ok_or(Error::Coverage)?;
        if SeenSet::decode(&bytes)?.digest()? != digest {
            return Err(Error::Malformed);
        }
        Ok(bytes)
    }

    /// Records a prepared session only for the current archive revision and an indexed anchor.
    /// The caller must supply the epoch captured before preparing those PIR rows.
    pub fn record_coverage(&mut self, coverage: &Coverage) -> Result<(), Error> {
        let tx = self
            .db
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let (identity, epoch): (Vec<u8>, u64) =
            tx.query_row("SELECT identity,epoch FROM archive_state", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })?;
        let anchor: Option<Vec<u8>> = tx
            .query_row(
                "SELECT hash FROM archive_blocks WHERE height=?1",
                [coverage.frontier.height],
                |r| r.get(0),
            )
            .optional()?;
        if coverage.genesis != self.config.genesis
            || coverage.archive != hash(identity)?
            || coverage.epoch != epoch
            || coverage.first_height != self.config.start_height
            || anchor.as_deref() != Some(coverage.frontier.hash.as_slice())
        {
            return Err(Error::Coverage);
        }
        let bytes = coverage.encode()?;
        let existing: Option<Vec<u8>> = tx
            .query_row(
                "SELECT bytes FROM archive_coverage WHERE identity=?1 AND epoch=?2 AND session=?3",
                params![
                    coverage.archive.as_slice(),
                    epoch,
                    coverage.session.as_slice()
                ],
                |r| r.get(0),
            )
            .optional()?;
        if existing.is_some_and(|b| b != bytes) {
            return Err(Error::Coverage);
        }
        tx.execute(
            "INSERT OR IGNORE INTO archive_coverage VALUES (?1,?2,?3,?4)",
            params![
                coverage.archive.as_slice(),
                epoch,
                coverage.session.as_slice(),
                bytes
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Reads the immutable declaration for one archive revision and session.
    pub fn coverage(&self, identity: Hash, epoch: u64, session: Hash) -> Result<Vec<u8>, Error> {
        let bytes: Vec<u8> = self
            .db
            .query_row(
                "SELECT bytes FROM archive_coverage WHERE identity=?1 AND epoch=?2 AND session=?3",
                params![identity.as_slice(), epoch, session.as_slice()],
                |r| r.get(0),
            )
            .optional()?
            .ok_or(Error::Coverage)?;
        let coverage = Coverage::decode(&bytes)?;
        if (coverage.archive, coverage.epoch, coverage.session) != (identity, epoch, session) {
            return Err(Error::Malformed);
        }
        Ok(bytes)
    }

    /// Removes only expired unreferenced versions, keeping current/staged heads and protected sessions.
    /// Callers include current, previous and staged or in-flight PIR sessions.
    /// An admitted range read owns a SQLite snapshot until its response bytes are buffered.
    pub fn prune(&mut self, now: i64, protected_sessions: &[Hash]) -> Result<(), Error> {
        let tx = self
            .db
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        tx.execute("DELETE FROM archive_heads WHERE created<?1 AND digest NOT IN
            (SELECT active FROM archive_state WHERE active IS NOT NULL UNION SELECT staged FROM archive_state WHERE staged IS NOT NULL)",[now.saturating_sub(HEAD_GRACE_SECS)])?;
        let mut pages = std::collections::BTreeSet::new();
        let mut seen = std::collections::BTreeSet::new();
        let mut revisions = std::collections::BTreeSet::new();
        {
            let mut query = tx.prepare("SELECT bytes,leaves FROM archive_heads")?;
            for row in query.query_map([], |r| {
                Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, Vec<u8>>(1)?))
            })? {
                let (bytes, leaves) = row?;
                pages.extend(decode_leaves(&leaves)?.into_values());
                let head = Head::decode(&bytes)?;
                revisions.insert((head.archive, head.epoch));
                if let Some(reference) = head.seen {
                    seen.insert(reference.digest);
                }
            }
        }
        let union: Option<Vec<u8>> =
            tx.query_row("SELECT seen FROM archive_state", [], |r| r.get(0))?;
        if let Some(bytes) = union {
            seen.insert(SeenSet::decode(&bytes)?.digest()?);
        }
        for (table, retained) in [("archive_pages", pages), ("archive_seen", seen)] {
            let mut query = tx.prepare(&format!("SELECT digest FROM {table}"))?;
            let stale = query
                .query_map([], |r| r.get::<_, Vec<u8>>(0))?
                .collect::<Result<Vec<_>, _>>()?;
            drop(query);
            for digest in stale {
                if !retained.contains(&hash(digest.clone())?) {
                    tx.execute(&format!("DELETE FROM {table} WHERE digest=?1"), [digest])?;
                }
            }
        }
        let mut query = tx.prepare("SELECT identity,epoch,session FROM archive_coverage")?;
        let sessions = query
            .query_map([], |r| {
                Ok((
                    r.get::<_, Vec<u8>>(0)?,
                    r.get::<_, u64>(1)?,
                    r.get::<_, Vec<u8>>(2)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        drop(query);
        for (identity, epoch, session) in sessions {
            if !protected_sessions.contains(&hash(session.clone())?)
                || !revisions.contains(&(hash(identity.clone())?, epoch))
            {
                tx.execute(
                    "DELETE FROM archive_coverage WHERE identity=?1 AND epoch=?2 AND session=?3",
                    params![identity, epoch, session],
                )?;
            }
        }
        tx.commit()?;
        Ok(())
    }
}

/// Starts the old active head's grace at retirement, not when it was first published.
fn retire(tx: &Transaction<'_>, now: i64) -> Result<(), Error> {
    tx.execute(
        "UPDATE archive_heads SET created=MAX(created,?1)
        WHERE digest=(SELECT active FROM archive_state)",
        [now],
    )?;
    Ok(())
}

/// Stores canonical immutable page bytes and records their position in the staged index.
fn store_page(
    tx: &Transaction<'_>,
    parent: Hash,
    filters: Vec<BlockFilter>,
    leaves: &mut BTreeMap<u32, Hash>,
) -> Result<(), Error> {
    let page = Page::new(parent, filters)?;
    let digest = page.digest()?;
    leaves.insert(page.number(), digest);
    if leaves.len() > MAX_ARCHIVE_PAGES {
        return Err(Error::Capacity);
    }
    tx.execute(
        "INSERT OR IGNORE INTO archive_pages VALUES (?1,?2)",
        params![digest.as_slice(), page.encode()?],
    )?;
    Ok(())
}

/// Reconstructs the bounded publisher index from its immutable ordered leaves.
fn index(leaves: &BTreeMap<u32, Hash>) -> Result<Index, Error> {
    let mut index = Index::default();
    for (&number, &digest) in leaves {
        index.update(number, Some(digest))?;
    }
    Ok(index)
}

/// Encodes bounded ordered page positions and their content digests.
fn encode_leaves(leaves: &BTreeMap<u32, Hash>) -> Vec<u8> {
    let mut bytes = (leaves.len() as u32).to_le_bytes().to_vec();
    for (number, digest) in leaves {
        bytes.extend(number.to_le_bytes());
        bytes.extend(digest);
    }
    bytes
}

/// Rejects oversized or noncanonical index records before allocating their map.
fn decode_leaves(bytes: &[u8]) -> Result<BTreeMap<u32, Hash>, Error> {
    let (count, rest) = bytes.split_first_chunk::<4>().ok_or(Error::Malformed)?;
    let count = u32::from_le_bytes(*count) as usize;
    if count > MAX_ARCHIVE_PAGES || rest.len() != count * 36 {
        return Err(Error::Malformed);
    }
    let mut leaves = BTreeMap::new();
    let mut previous = None;
    for leaf in rest.chunks_exact(36) {
        let number = u32::from_le_bytes(leaf[..4].try_into().unwrap());
        if number >= 1 << 24 || previous.is_some_and(|p| p >= number) {
            return Err(Error::Malformed);
        }
        leaves.insert(number, leaf[4..].try_into().unwrap());
        previous = Some(number);
    }
    Ok(leaves)
}

/// Converts a stored hash, rejecting malformed database content.
fn hash(bytes: Vec<u8>) -> Result<Hash, Error> {
    bytes.try_into().map_err(|_| Error::Malformed)
}
