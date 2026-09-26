use crate::{Error, Hash, Receiver, Record, RECORD_BYTES};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const PROFILE: &str = "ironwood-zero-ovk-receiver-v1";
pub const ROW_BYTES: usize = 4096;
pub const SLOTS: usize = ROW_BYTES / RECORD_BYTES;
pub const MAX_ROWS: u32 = 65536;

/// Coverage includes empty blocks and excludes coinbase recipients, not their note positions.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    pub profile: String,
    pub genesis: Hash,
    pub start_height: u32,
    pub start_parent: Hash,
    pub start_position: u64,
    pub end_height: u32,
    pub end_hash: Hash,
    pub end_position: u64,
    pub rows: u32,
    pub salt: Hash,
    pub records: u64,
    pub data_sha256: Hash,
}

impl Manifest {
    pub fn validate(&self) -> Result<(), Error> {
        if self.profile != PROFILE
            || self.start_height > self.end_height
            || self.start_position > self.end_position
            || !self.rows.is_power_of_two()
            || self.rows > MAX_ROWS
            || self.records > u64::from(self.rows) * SLOTS as u64
        {
            return Err(Error::Malformed);
        }
        Ok(())
    }

    /// Bind row data, coverage, network, and geometry into one immutable revision.
    pub fn revision(&self) -> Result<Hash, Error> {
        self.validate()?;
        Ok(Sha256::digest(serde_json::to_vec(self)?).into())
    }

    /// The caller supplies an independently accepted chain anchor and required history start.
    pub fn accept(&self, genesis: Hash, start: u32, height: u32, hash: Hash) -> Result<(), Error> {
        self.validate()?;
        if start > height
            || self.genesis != genesis
            || self.start_height > start
            || self.end_height != height
            || self.end_hash != hash
        {
            return Err(Error::Coverage);
        }
        Ok(())
    }
}

/// Stable receiver identity; derivation index and purpose never leave the wallet.
pub fn receiver_tag(genesis: &Hash, receiver: &Receiver) -> Hash {
    let mut h = Sha256::new();
    h.update(b"ironwood-receiver/v1/tag\0");
    h.update(genesis);
    h.update(receiver.as_bytes());
    h.finalize().into()
}

/// This row number must be selected inside PIR, never in a public HTTP route.
pub fn row_for(manifest: &Manifest, receiver: &Receiver, page: u32) -> Result<usize, Error> {
    manifest.validate()?;
    let mut h = Sha256::new();
    h.update(b"ironwood-receiver/v1/bucket\0");
    h.update(manifest.salt);
    h.update(receiver_tag(&manifest.genesis, receiver));
    h.update(page.to_le_bytes());
    let digest = h.finalize();
    Ok((u32::from_le_bytes(digest[..4].try_into().unwrap()) & (manifest.rows - 1)) as usize)
}

pub struct Snapshot {
    pub manifest: Manifest,
    pub data: Vec<u8>,
}

impl Snapshot {
    /// Bucket overflow fails the whole candidate. It never drops records or coverage.
    pub fn build(mut manifest: Manifest, records: &[Record]) -> Result<Self, Error> {
        manifest.records = records.len() as u64;
        manifest.validate()?;
        let mut data = vec![0; manifest.rows as usize * ROW_BYTES];
        let mut counts = vec![0; manifest.rows as usize];
        let mut sorted: Vec<_> = records.iter().collect();
        sorted.sort_by_key(|r| (r.receiver, r.page));
        let mut previous: Option<&Record> = None;
        let mut outputs = std::collections::BTreeSet::new();
        let mut positions = std::collections::BTreeSet::new();
        for record in sorted {
            // Every advertised page must exist in this revision, in chain order.
            match previous {
                Some(p) if p.receiver == record.receiver => {
                    if record.total != p.total
                        || record.page != p.page + 1
                        || record.payment.position <= p.payment.position
                    {
                        return Err(Error::Malformed);
                    }
                }
                p => {
                    if record.page != 0 || p.is_some_and(|r| r.page + 1 != r.total) {
                        return Err(Error::Malformed);
                    }
                }
            }
            previous = Some(record);
            if !outputs.insert((record.payment.txid, record.payment.action_index))
                || !positions.insert(record.payment.position)
            {
                return Err(Error::Malformed);
            }
            validate_location(&manifest, record)?;
            let row = row_for(&manifest, &record.receiver, record.page)?;
            if counts[row] == SLOTS {
                return Err(Error::Capacity);
            }
            let offset = row * ROW_BYTES + counts[row] * RECORD_BYTES;
            data[offset..offset + RECORD_BYTES].copy_from_slice(&record.encode()?);
            counts[row] += 1;
        }
        if previous.is_some_and(|r| r.page + 1 != r.total) {
            return Err(Error::Malformed);
        }
        manifest.data_sha256 = Sha256::digest(&data).into();
        Ok(Self { manifest, data })
    }
}

fn validate_location(m: &Manifest, r: &Record) -> Result<(), Error> {
    let p = &r.payment;
    if p.height < m.start_height
        || p.height > m.end_height
        || p.position < m.start_position
        || p.position >= m.end_position
        || (p.height == m.end_height && p.block_hash != m.end_hash)
    {
        return Err(Error::Malformed);
    }
    Ok(())
}

/// Decode a privately retrieved row from an accepted revision. None is scheme-scoped absence.
/// Subsequent pages must also repeat page zero's total before recovery can complete.
pub fn lookup_row(
    m: &Manifest,
    receiver: &Receiver,
    page: u32,
    bytes: &[u8],
) -> Result<Option<Record>, Error> {
    let wanted_row = row_for(m, receiver, page)?;
    if bytes.len() != ROW_BYTES || bytes[SLOTS * RECORD_BYTES..].iter().any(|b| *b != 0) {
        return Err(Error::Malformed);
    }
    let mut found = None;
    for slot in bytes[..SLOTS * RECORD_BYTES].chunks_exact(RECORD_BYTES) {
        if let Some(record) = Record::decode(slot)? {
            validate_location(m, &record)?;
            if row_for(m, &record.receiver, record.page)? != wanted_row {
                return Err(Error::Malformed);
            }
            if &record.receiver == receiver && record.page == page {
                if found.is_some() {
                    return Err(Error::Malformed);
                }
                found = Some(record);
            }
        }
    }
    if page != 0 && found.is_none() {
        return Err(Error::MissingPage);
    }
    Ok(found)
}
