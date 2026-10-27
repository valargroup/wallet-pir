//! The provider audit's own state beside the feeds: which reported payouts are still
//! open, the refund feeds' swaps and candidate transactions, durable floors, counters
//! and watermarks, and read-only payment lookups for the audit task.
//!
//! An item is open until the audit task has found it in the index and found it again
//! at least [`RETIRE_AFTER_SECS`] later; then it retires and is never checked again.
//! Rechecking a found item only detects the index later losing data, which the
//! canary, not this audit, owns. A miss reopens a found item, and a missing item never
//! retires.
use crate::{Error, PayoutStore};
use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;
use zakura_pir_publisher::receiver::{Hash, ReceiverBytes};

/// How long a found item stays open, covering a late reorg.
pub const RETIRE_AFTER_SECS: i64 = 2 * 60 * 60;

/// Tables this release adds to the provider store. The previous release writes
/// `payouts` positionally, so the open set lives in its own table, not a new column.
pub(crate) const SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS payout_open (receiver BLOB NOT NULL, txid BLOB NOT NULL,
        found_at INTEGER, retired_at INTEGER, PRIMARY KEY(receiver,txid)) WITHOUT ROWID;
    CREATE INDEX IF NOT EXISTS payout_active ON payout_open(receiver) WHERE retired_at IS NULL;
    CREATE INDEX IF NOT EXISTS payouts_seen ON payouts(seen_at);
    CREATE TABLE IF NOT EXISTS audit_state (key TEXT PRIMARY KEY, value INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS refunds (swap BLOB PRIMARY KEY CHECK(length(swap)=32),
        feed TEXT NOT NULL, receiver BLOB NOT NULL CHECK(length(receiver)=43),
        deposit_address TEXT NOT NULL, deposit_memo TEXT, created_at INTEGER NOT NULL,
        seen_at INTEGER NOT NULL, candidates_changed_at INTEGER, found_at INTEGER,
        retired_at INTEGER, checked_at INTEGER, refreshed_at INTEGER,
        refund_confirmed_at INTEGER, confirm_after INTEGER,
        confirm_attempts INTEGER NOT NULL DEFAULT 0, confirm_last TEXT) WITHOUT ROWID;
    CREATE INDEX IF NOT EXISTS refunds_open ON refunds(seen_at) WHERE retired_at IS NULL;
    CREATE TABLE IF NOT EXISTS refund_txs (swap BLOB NOT NULL REFERENCES refunds(swap),
        txid BLOB NOT NULL CHECK(length(txid)=32), PRIMARY KEY(swap,txid)) WITHOUT ROWID;";

/// The `audit_state` key counting retired checkable payouts.
const PAYOUTS_RETIRED: &str = "payouts-retired";

/// An open payout: reported complete, not yet retired.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenPayout {
    pub receiver: ReceiverBytes,
    pub txid: Hash,
    pub seen_at: i64,
    pub found_at: Option<i64>,
}

/// What the audit task learned about an open payout.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PayoutChange {
    /// First found in the index.
    Found,
    /// Found before, missing now.
    Reopen,
    /// Found again after [`RETIRE_AFTER_SECS`]; counted toward `payouts_checked`.
    Retire,
    /// An uncheckable record past its reporting window; not counted.
    Expire,
}

/// A refund one feed read reported.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReportedRefund {
    /// The swap's identity, a digest of its deposit address and memo.
    pub swap: Hash,
    pub receiver: ReceiverBytes,
    pub deposit_address: String,
    pub deposit_memo: Option<String>,
    pub created_at: i64,
    /// Every parsable reported origin transaction, deposits and refunds alike.
    pub candidates: Vec<Hash>,
}

/// The last 1Click answer for an excess-refund swap.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Confirmation {
    /// NEAR reports a positive refunded amount; kept.
    Refunded,
    /// NEAR reports nothing refunded yet; retried.
    Zero,
    /// The status could not be read; retried.
    Failed,
}

/// An unretired refund swap with its candidates.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenRefund {
    pub swap: Hash,
    pub feed: String,
    pub receiver: ReceiverBytes,
    pub deposit_address: String,
    pub deposit_memo: Option<String>,
    pub created_at: i64,
    pub seen_at: i64,
    /// When a new candidate was last inserted; due a grace after this.
    pub candidates_changed_at: Option<i64>,
    pub found_at: Option<i64>,
    pub checked_at: Option<i64>,
    pub refreshed_at: Option<i64>,
    pub refund_confirmed_at: Option<i64>,
    pub confirm_after: Option<i64>,
    pub confirm_attempts: u32,
    pub confirm_last: Option<Confirmation>,
    pub candidates: Vec<Hash>,
}

/// What the audit task learned about an open refund swap.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RefundChange {
    /// A candidate is first found paying the receiver.
    Found,
    /// Found before, missing now.
    Reopen,
    /// Found again after [`RETIRE_AFTER_SECS`].
    Retire,
    /// Due with no candidate paying the receiver, and known to be a refund.
    Missing,
    /// A 1Click answer, with when to ask again unless it is [`Confirmation::Refunded`].
    Confirmed(Confirmation, Option<i64>),
    /// Re-read from the explorer.
    Refreshed,
}

impl Confirmation {
    fn name(self) -> &'static str {
        match self {
            Self::Refunded => "refunded",
            Self::Zero => "zero",
            Self::Failed => "failed",
        }
    }

    fn parse(name: &str) -> Option<Self> {
        [Self::Refunded, Self::Zero, Self::Failed]
            .into_iter()
            .find(|c| c.name() == name)
    }
}

impl PayoutStore {
    /// Opens every stored payout that is neither open nor retired, such as those a
    /// rolled-back release recorded, and returns how many. One full scan, so the audit
    /// task runs it once per process.
    pub fn reconcile(&mut self) -> Result<usize, Error> {
        Ok(self.db.execute(
            "INSERT OR IGNORE INTO payout_open (receiver,txid) SELECT receiver,txid FROM payouts",
            [],
        )?)
    }

    /// Every unretired payout.
    pub fn open_payouts(&self) -> Result<Vec<OpenPayout>, Error> {
        let mut query = self.db.prepare(
            "SELECT o.receiver,o.txid,p.seen_at,o.found_at FROM payout_open o
             JOIN payouts p ON p.receiver=o.receiver AND p.txid=o.txid
             WHERE o.retired_at IS NULL",
        )?;
        let rows = query.query_map([], |r| {
            Ok((
                r.get::<_, Vec<u8>>(0)?,
                r.get::<_, Vec<u8>>(1)?,
                r.get(2)?,
                r.get(3)?,
            ))
        })?;
        rows.map(|row| {
            let (receiver, txid, seen_at, found_at) = row?;
            Ok(OpenPayout {
                receiver: receiver.try_into().map_err(|_| Error::Malformed)?,
                txid: txid.try_into().map_err(|_| Error::Malformed)?,
                seen_at,
                found_at,
            })
        })
        .collect()
    }

    /// Applies the audit task's verdicts at `now` in one transaction.
    pub fn settle_payouts(
        &mut self,
        changes: &[(ReceiverBytes, Hash, PayoutChange)],
        now: i64,
    ) -> Result<(), Error> {
        let tx = self.db.transaction()?;
        for (receiver, txid, change) in changes {
            let (receiver, txid) = (receiver.as_slice(), txid.as_slice());
            let changed = match change {
                PayoutChange::Found => tx.execute(
                    "UPDATE payout_open SET found_at=?3 WHERE receiver=?1 AND txid=?2
                     AND found_at IS NULL AND retired_at IS NULL",
                    params![receiver, txid, now],
                )?,
                PayoutChange::Reopen => tx.execute(
                    "UPDATE payout_open SET found_at=NULL WHERE receiver=?1 AND txid=?2
                     AND retired_at IS NULL",
                    params![receiver, txid],
                )?,
                PayoutChange::Retire | PayoutChange::Expire => tx.execute(
                    "UPDATE payout_open SET retired_at=?3 WHERE receiver=?1 AND txid=?2
                     AND retired_at IS NULL",
                    params![receiver, txid, now],
                )?,
            };
            if *change == PayoutChange::Retire && changed == 1 {
                tx.execute(
                    "INSERT INTO audit_state VALUES (?1,1) ON CONFLICT(key) DO UPDATE SET value=value+1",
                    [PAYOUTS_RETIRED],
                )?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Unretired payouts first seen complete by `until`, for the report's check.
    pub fn due_payouts(&self, until: i64) -> Result<Vec<(ReceiverBytes, Hash)>, Error> {
        Ok(self
            .open_payouts()?
            .into_iter()
            .filter(|p| p.seen_at <= until)
            .map(|p| (p.receiver, p.txid))
            .collect())
    }

    /// Payouts first seen in `(from, until]` whose transaction id starts with `zeros`
    /// zero bytes, the audit's uncheckable records.
    pub fn uncheckable_payouts(&self, from: i64, until: i64, zeros: usize) -> Result<usize, Error> {
        Ok(self.db.query_row(
            "SELECT COUNT(*) FROM payouts WHERE seen_at>?1 AND seen_at<=?2
             AND substr(txid,1,?3)=zeroblob(?3)",
            params![from, until, zeros],
            |r| r.get(0),
        )?)
    }

    /// How many checkable payouts have retired.
    pub fn retired_payouts(&self) -> Result<u64, Error> {
        Ok(self.state(PAYOUTS_RETIRED)?.unwrap_or(0) as u64)
    }

    /// `feed`'s durable floor, set to `default` on first use and never lowered.
    pub fn floor(&mut self, feed: &str, default: i64) -> Result<i64, Error> {
        let key = format!("floor:{feed}");
        self.db.execute(
            "INSERT OR IGNORE INTO audit_state VALUES (?1,?2)",
            params![key, default],
        )?;
        Ok(self.state(&key)?.unwrap_or(default))
    }

    /// `feed`'s floor if one was set.
    pub fn floor_of(&self, feed: &str) -> Result<Option<i64>, Error> {
        self.state(&format!("floor:{feed}"))
    }

    /// The latest due-time base (`candidates_changed_at`) among `feed`'s checked
    /// swaps. It never moves backwards; see [`Self::settle_refunds`].
    pub fn checked_through(&self, feed: &str) -> Result<Option<i64>, Error> {
        self.state(&format!("through:{feed}"))
    }

    /// Commits a refund feed's read atomically with its cursor. New swaps are added;
    /// for known ones, new candidates are merged (see [`Self::merge_candidates`]).
    pub fn record_refunds(
        &mut self,
        feed: &str,
        refunds: &[ReportedRefund],
        cursor: i64,
        read_at: i64,
        seen_at: i64,
    ) -> Result<(), Error> {
        let tx = self.db.transaction()?;
        record_feed(&tx, feed, cursor, read_at)?;
        for refund in refunds {
            tx.execute(
                "INSERT INTO refunds (swap,feed,receiver,deposit_address,deposit_memo,
                    created_at,seen_at) VALUES (?1,?2,?3,?4,?5,?6,?7)
                 ON CONFLICT(swap) DO NOTHING",
                params![
                    refund.swap.as_slice(),
                    feed,
                    refund.receiver.as_slice(),
                    refund.deposit_address,
                    refund.deposit_memo,
                    refund.created_at,
                    seen_at
                ],
            )?;
            merge(&tx, &refund.swap, &refund.candidates, seen_at)?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Adds `candidates` to a known swap at `now`. Any new one restarts the swap's
    /// grace and clears its found and checked state and any pending zero answer; a
    /// positive confirmation is kept. Returns whether one was new.
    pub fn merge_candidates(
        &mut self,
        swap: &Hash,
        candidates: &[Hash],
        now: i64,
    ) -> Result<bool, Error> {
        let tx = self.db.transaction()?;
        let added = merge(&tx, swap, candidates, now)?;
        tx.commit()?;
        Ok(added)
    }

    /// Unretired refund swaps first seen at or after `since`, from or above their
    /// feed's floor, with their candidates.
    pub fn open_refunds(&self, since: i64) -> Result<Vec<OpenRefund>, Error> {
        let mut query = self.db.prepare(
            "SELECT swap,feed,receiver,deposit_address,deposit_memo,created_at,seen_at,
                candidates_changed_at,found_at,checked_at,refreshed_at,refund_confirmed_at,
                confirm_after,confirm_attempts,confirm_last FROM refunds r
             WHERE retired_at IS NULL AND seen_at>=?1 AND created_at>=COALESCE(
                (SELECT value FROM audit_state WHERE key='floor:'||r.feed), created_at)",
        )?;
        let mut candidates = self
            .db
            .prepare("SELECT txid FROM refund_txs WHERE swap=?1")?;
        let rows = query.query_map([since], |r| {
            Ok((
                r.get::<_, Vec<u8>>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, Vec<u8>>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, Option<String>>(4)?,
                [r.get(5)?, r.get(6)?],
                [
                    r.get(7)?,
                    r.get(8)?,
                    r.get(9)?,
                    r.get(10)?,
                    r.get(11)?,
                    r.get(12)?,
                ],
                r.get::<_, u32>(13)?,
                r.get::<_, Option<String>>(14)?,
            ))
        })?;
        rows.map(|row| {
            let (swap, feed, receiver, address, memo, [created_at, seen_at], times, attempts, last) =
                row?;
            let swap: Hash = swap.try_into().map_err(|_| Error::Malformed)?;
            let [changed, found, checked, refreshed, confirmed, after] = times;
            Ok(OpenRefund {
                candidates: candidates
                    .query_map([swap.as_slice()], |r| r.get::<_, Vec<u8>>(0))?
                    .map(|txid| txid?.try_into().map_err(|_| Error::Malformed))
                    .collect::<Result<_, Error>>()?,
                swap,
                feed,
                receiver: receiver.try_into().map_err(|_| Error::Malformed)?,
                deposit_address: address,
                deposit_memo: memo,
                created_at,
                seen_at,
                candidates_changed_at: changed,
                found_at: found,
                checked_at: checked,
                refreshed_at: refreshed,
                refund_confirmed_at: confirmed,
                confirm_after: after,
                confirm_attempts: attempts,
                confirm_last: last.as_deref().and_then(Confirmation::parse),
            })
        })
        .collect()
    }

    /// Applies the audit task's verdicts at `now` in one transaction. A swap that
    /// becomes checked (found, missing or confirmed) raises its feed's watermark to
    /// its `candidates_changed_at` before it can retire.
    pub fn settle_refunds(
        &mut self,
        changes: &[(Hash, RefundChange)],
        now: i64,
    ) -> Result<(), Error> {
        let tx = self.db.transaction()?;
        for (swap, change) in changes {
            let swap = swap.as_slice();
            let checked = match change {
                RefundChange::Found => {
                    tx.execute(
                        "UPDATE refunds SET found_at=?2 WHERE swap=?1 AND found_at IS NULL",
                        params![swap, now],
                    )?;
                    true
                }
                RefundChange::Reopen => {
                    tx.execute("UPDATE refunds SET found_at=NULL WHERE swap=?1", [swap])?;
                    false
                }
                RefundChange::Retire => {
                    tx.execute(
                        "UPDATE refunds SET retired_at=?2 WHERE swap=?1 AND retired_at IS NULL",
                        params![swap, now],
                    )?;
                    false
                }
                RefundChange::Missing => true,
                RefundChange::Confirmed(answer, retry) => {
                    tx.execute(
                        "UPDATE refunds SET confirm_last=?2, confirm_after=?3,
                            confirm_attempts=confirm_attempts+1,
                            refund_confirmed_at=CASE WHEN ?2='refunded'
                                THEN COALESCE(refund_confirmed_at,?4) ELSE refund_confirmed_at END
                         WHERE swap=?1",
                        params![swap, answer.name(), retry, now],
                    )?;
                    *answer == Confirmation::Refunded
                }
                RefundChange::Refreshed => {
                    tx.execute(
                        "UPDATE refunds SET refreshed_at=?2 WHERE swap=?1",
                        params![swap, now],
                    )?;
                    false
                }
            };
            if checked {
                tx.execute(
                    "UPDATE refunds SET checked_at=?2 WHERE swap=?1 AND checked_at IS NULL",
                    params![swap, now],
                )?;
                tx.execute(
                    "INSERT INTO audit_state SELECT 'through:'||feed, candidates_changed_at
                     FROM refunds WHERE swap=?1 AND candidates_changed_at IS NOT NULL
                     ON CONFLICT(key) DO UPDATE SET value=MAX(value,excluded.value)",
                    [swap],
                )?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// An `audit_state` value.
    fn state(&self, key: &str) -> Result<Option<i64>, Error> {
        Ok(self
            .db
            .query_row("SELECT value FROM audit_state WHERE key=?1", [key], |r| {
                r.get(0)
            })
            .optional()?)
    }
}

/// Upserts `feed`'s cursor and read time, keeping the newest; see
/// [`PayoutStore::record`].
pub(crate) fn record_feed(
    tx: &Connection,
    feed: &str,
    cursor: i64,
    read_at: i64,
) -> Result<(), Error> {
    tx.execute(
        "INSERT INTO feeds (feed,cursor,read_at) VALUES (?1,?2,?3) ON CONFLICT(feed)
         DO UPDATE SET cursor=excluded.cursor, read_at=excluded.read_at
         WHERE excluded.cursor>cursor OR (excluded.cursor=cursor AND excluded.read_at>read_at)",
        params![feed, cursor, read_at],
    )?;
    Ok(())
}

/// See [`PayoutStore::merge_candidates`]. A retired swap keeps its candidates but not
/// its state, since a later refund on a settled swap is not checked separately.
fn merge(tx: &Connection, swap: &Hash, candidates: &[Hash], now: i64) -> Result<bool, Error> {
    let mut added = 0;
    for txid in candidates {
        added += tx.execute(
            "INSERT OR IGNORE INTO refund_txs SELECT ?1,?2 WHERE EXISTS
                (SELECT 1 FROM refunds WHERE swap=?1)",
            params![swap.as_slice(), txid.as_slice()],
        )?;
    }
    if added > 0 {
        tx.execute(
            "UPDATE refunds SET candidates_changed_at=?2, found_at=NULL, checked_at=NULL,
                confirm_after=NULL, confirm_attempts=0,
                confirm_last=CASE WHEN confirm_last='refunded' THEN 'refunded' END
             WHERE swap=?1 AND retired_at IS NULL",
            params![swap.as_slice(), now],
        )?;
    }
    Ok(added > 0)
}

/// Read-only payment lookups for the audit task, which runs beside the writer and so
/// must never change the file; [`crate::Store::open`] writes schema and config.
pub struct PaymentIndex {
    db: Connection,
}

impl PaymentIndex {
    /// Opens an existing store read-only.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let db = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        db.busy_timeout(std::time::Duration::from_secs(5))?;
        Ok(Self { db })
    }

    /// Runs `read` in one read transaction, released before this returns, so callers
    /// finish reading before any network wait.
    pub fn view<T>(&self, read: impl FnOnce(&Self) -> Result<T, Error>) -> Result<T, Error> {
        let tx = self.db.unchecked_transaction()?;
        let value = read(self)?;
        tx.commit()?;
        Ok(value)
    }

    /// See [`crate::Store::paid_in`].
    pub fn paid_in(&self, receiver: &ReceiverBytes, txid: &Hash) -> Result<bool, Error> {
        crate::paid_in(&self.db, receiver, txid)
    }
}
