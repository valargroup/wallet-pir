//! Immutable publications: manifests, row placement and row decoding.
use crate::{
    filter::{self, Filter, Filters},
    Error, Hash, Receiver, Record, RECORD_BYTES,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// The directory format a manifest commits to.
pub const PROFILE: &str = "ironwood-zero-ovk-receiver-v1";
/// Bytes per row. Unused trailing bytes are zero.
pub const ROW_BYTES: usize = 4096;
/// Records per row.
pub const SLOTS: usize = ROW_BYTES / RECORD_BYTES;
/// Largest supported row count.
pub const MAX_ROWS: u32 = 65536;

/// Coverage includes empty blocks and excludes coinbase recipients, not their note positions.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
    /// The sets in the publication's [`Filters`] file, in label order.
    pub filters: Vec<FilterSet>,
    /// Commits to the publication's [`Filters`] file.
    pub filters_sha256: Hash,
}

/// One labeled set in a publication's [`Filters`] file; see [`crate::filter`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FilterSet {
    /// [`filter::PAID`], or `<provider>/recent` or `<provider>/seen`.
    pub label: String,
    /// Distinct receivers in the set.
    pub count: u32,
    /// For a recent set, how far back the provider's swaps reach from `until_unix`, in
    /// seconds.
    pub window_secs: Option<u64>,
    /// For a provider set, when the provider's feed started, in Unix seconds. Receivers
    /// the provider was given earlier are missing from it.
    pub since_unix: Option<i64>,
    /// For a provider set, when the feed's last complete read of the provider began, in
    /// Unix seconds. Receivers the provider was given later are missing from it.
    pub until_unix: Option<i64>,
}

/// Receivers from a swap provider's feed for one labeled set; see [`FilterSet`].
pub struct ProviderSet {
    /// `<provider>/recent` or `<provider>/seen`.
    pub label: String,
    /// See [`FilterSet::window_secs`]; required for a recent set.
    pub window_secs: Option<u64>,
    /// See [`FilterSet::since_unix`].
    pub since_unix: i64,
    /// See [`FilterSet::until_unix`].
    pub until_unix: i64,
    pub receivers: Vec<Receiver>,
}

impl Manifest {
    /// Check the profile, coverage order and geometry bounds.
    pub fn validate(&self) -> Result<(), Error> {
        let labels_ordered = self
            .filters
            .windows(2)
            .all(|pair| pair[0].label < pair[1].label);
        let sets_valid = self.filters.iter().all(|set| {
            let recent = set.label.ends_with(&format!("/{}", filter::RECENT));
            let span = match (set.since_unix, set.until_unix) {
                (Some(since), Some(until)) => since <= until,
                (None, None) => set.label == filter::PAID,
                _ => false,
            };
            filter::valid_label(&set.label)
                && span
                && (set.label == filter::PAID) == set.since_unix.is_none()
                && recent == set.window_secs.is_some()
        });
        if self.profile != PROFILE
            || !labels_ordered
            || !sets_valid
            || !self.filters.iter().any(|set| set.label == filter::PAID)
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

    /// Checks that `filters` holds exactly the sets this manifest declares. Its bytes must
    /// already match [`Self::filters_sha256`].
    pub fn check_filters(&self, filters: &Filters) -> Result<(), Error> {
        let mut sets = filters.iter();
        for declared in &self.filters {
            match sets.next() {
                Some((label, set)) if label == declared.label && set.count() == declared.count => {}
                _ => return Err(Error::Malformed),
            }
        }
        if sets.next().is_some() {
            return Err(Error::Malformed);
        }
        Ok(())
    }

    /// Bind row data, coverage, network, geometry and filters into one immutable
    /// revision: a domain-separated SHA-256 of every field at fixed width, with strings
    /// and the set list length-prefixed and each optional value tagged, as Enhance and
    /// Status hash their manifests.
    pub fn revision(&self) -> Result<Hash, Error> {
        self.validate()?;
        let mut h = Sha256::new();
        h.update(b"ironwood-receiver/v1/manifest\0");
        update_str(&mut h, &self.profile);
        h.update(self.genesis);
        h.update(self.start_height.to_le_bytes());
        h.update(self.start_parent);
        h.update(self.start_position.to_le_bytes());
        h.update(self.end_height.to_le_bytes());
        h.update(self.end_hash);
        h.update(self.end_position.to_le_bytes());
        h.update(self.rows.to_le_bytes());
        h.update(self.salt);
        h.update(self.records.to_le_bytes());
        h.update(self.data_sha256);
        h.update((self.filters.len() as u64).to_le_bytes());
        for set in &self.filters {
            update_str(&mut h, &set.label);
            h.update(set.count.to_le_bytes());
            for value in [
                set.window_secs.map(u64::to_le_bytes),
                set.since_unix.map(i64::to_le_bytes),
                set.until_unix.map(i64::to_le_bytes),
            ] {
                match value {
                    Some(bytes) => {
                        h.update([1]);
                        h.update(bytes);
                    }
                    None => h.update([0]),
                }
            }
        }
        h.update(self.filters_sha256);
        Ok(h.finalize().into())
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
fn receiver_tag(genesis: &Hash, receiver: &Receiver) -> Hash {
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

/// A manifest and the row data and filters it commits to.
#[derive(Clone)]
pub struct Snapshot {
    pub manifest: Manifest,
    pub data: Vec<u8>,
    /// The encoded [`Filters`].
    pub filters: Vec<u8>,
}

impl Snapshot {
    /// Bucket overflow fails the whole candidate. It never drops records or coverage.
    /// The paid filter holds the records' receivers; `provider` sets come from the
    /// publisher's swap provider feeds (see [`crate::filter`]).
    pub fn build(
        mut manifest: Manifest,
        records: &[Record],
        provider: &[ProviderSet],
    ) -> Result<Self, Error> {
        manifest.records = records.len() as u64;
        let filters = Filters::new(
            Filter::build(&manifest.salt, records.iter().map(|r| &r.receiver)),
            provider.iter().map(|set| {
                (
                    set.label.clone(),
                    Filter::build(&manifest.salt, &set.receivers),
                )
            }),
        )?;
        manifest.filters = filters
            .iter()
            .map(|(label, set)| {
                let declared = provider.iter().find(|p| p.label == label);
                FilterSet {
                    label: label.to_owned(),
                    count: set.count(),
                    window_secs: declared.and_then(|p| p.window_secs),
                    since_unix: declared.map(|p| p.since_unix),
                    until_unix: declared.map(|p| p.until_unix),
                }
            })
            .collect();
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
        let filters = filters.encode();
        manifest.filters_sha256 = Sha256::digest(&filters).into();
        Ok(Self {
            manifest,
            data,
            filters,
        })
    }
}

/// Rejects a record whose payment lies outside the manifest's blocks or positions.
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
    for slot in bytes[..SLOTS * RECORD_BYTES].as_chunks::<RECORD_BYTES>().0 {
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

/// Hashes `value` with its length, so adjacent strings cannot run together.
fn update_str(h: &mut Sha256, value: &str) {
    h.update((value.len() as u64).to_le_bytes());
    h.update(value.as_bytes());
}
