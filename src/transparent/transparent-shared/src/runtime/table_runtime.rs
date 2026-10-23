//! Prepared P8 tables and encrypted query execution.
use super::*;

/// A uniformly random row among the leading rows that hold any nonzero
/// byte, or row 0 of an empty table. Random per call, so a fault is not
/// hidden behind a row the publisher could predict.
pub(super) fn sample_row(rows: &[u8], row_bytes: usize, table_rows: usize) -> usize {
    use std::hash::BuildHasher;
    let used = rows
        .iter()
        .rposition(|byte| *byte != 0)
        .map_or(1, |at| at / row_bytes + 1)
        .min(table_rows);
    let random = std::collections::hash_map::RandomState::new().hash_one(rows.len());
    (random % used as u64) as usize
}

/// Columns [`first_differing_column`] compares per pass over the rows: one
/// interleaved block of a byte database, so both it and the row-major
/// segment are read a cache line at a time instead of one line per byte.
pub(super) const SELF_CHECK_COLUMNS: usize = 16;

/// The lowest column with a row where `differs`, visiting every cell of a
/// `cols` x `rows` table. Columns are taken in tiles of
/// [`SELF_CHECK_COLUMNS`] and every row is read within a tile; a column-major
/// walk of row-major segment bytes touches a new page for every byte.
pub(super) fn first_differing_column(
    cols: usize,
    rows: usize,
    differs: impl Fn(usize, usize) -> bool,
) -> Option<usize> {
    (0..cols).step_by(SELF_CHECK_COLUMNS).find_map(|start| {
        let tile = start..(start + SELF_CHECK_COLUMNS).min(cols);
        (0..rows)
            .any(|row| tile.clone().any(|col| differs(row, col)))
            .then(|| {
                tile.clone()
                    .find(|&col| (0..rows).any(|row| differs(row, col)))
            })
            .flatten()
    })
}

/// An 8-bit table's bytes in column-major order: entry `c` of row `r` is byte
/// `c` of that row.
pub(super) fn column_major_bytes(rows: &[u8], row_count: usize, row_bytes: usize) -> Vec<u8> {
    let mut db = vec![0u8; row_count * row_bytes];
    db.par_chunks_mut(row_count)
        .enumerate()
        .for_each(|(col, column)| {
            for (row, entry) in column.iter_mut().enumerate() {
                *entry = rows[row * row_bytes + col];
            }
        });
    db
}

/// Rearranges column-major bytes into the byte kernel's tiles, band by band
/// of 16 columns. The layout is the same on every CPU: the kernel's portable
/// path scans it too, so a disk-cache entry restores anywhere.
pub(crate) fn interleave(db: &mut [u8], rows: usize) {
    db.par_chunks_mut(rows * 16)
        .for_each(|band| PreparedU16Query::interleave_u8_columns(band, rows));
}

/// Where entry (`col`, `row`) of a `rows`-row table sits in [`interleave`]'s
/// layout: its 16-column band, then four rows of each of the band's columns.
pub(crate) fn interleaved_index(rows: usize, col: usize, row: usize) -> usize {
    (col / 16) * rows * 16 + (row - row % 4) * 16 + (col % 16) * 4 + row % 4
}

/// One segment of one shard revision's table, prepared to answer queries.
pub struct TableRuntime {
    pub(crate) native: Profile,
    pub(crate) preprocessed: Vec<NativePreprocessed>,
    pub(crate) db: Vec<u8>,
    pub public_params: Vec<u8>,
    pub public_params_sha256: String,
    pub public_params_epoch: [u8; 8],
}

impl TableRuntime {
    /// Builds the runtime for one segment.
    ///
    /// `rows` is the segment's verified plaintext. It is borrowed, not kept:
    /// what survives is the encoded database inside the server, and holding the
    /// plaintext beside it would double the resident cost of every shard for no
    /// benefit.
    pub fn build(shared: &SharedParams, rows: &[u8]) -> Result<Self, String> {
        let profile = &shared.profile;
        let row_count = profile.rows;
        let mut db = column_major_bytes(rows, row_count, profile.row_bytes);
        let hint =
            zakura_pir_server_core::native::hint(&profile.masks, row_count, profile.cols, |col| {
                &db[col * row_count..(col + 1) * row_count]
            })?;
        let preprocessed = zakura_pir_server_core::native::preprocess(&profile.setup, &hint)?;
        drop(hint);
        interleave(&mut db, row_count);
        Self::assemble(profile.native, db, preprocessed)
    }

    /// Bytes this runtime holds, priced by the same terms as
    /// [`reserved_bytes`] but at the compiled matrix's actual word width.
    ///
    /// Each block's charge is its id and width word plus the library's
    /// retained coefficient storage (both masks and the matrix), which is the
    /// bound's block term with the real matrix in place of the eight-byte one.
    /// A restored runtime's matrix is mapped from its cache file and is
    /// charged the same.
    pub fn held_bytes(&self) -> u64 {
        const BLOCK_ID_AND_WIDTH_BYTES: u64 = 32 + 8;
        let database = self.db.len() as u64;
        database
            + self.public_params.len() as u64
            + native::PREPARED_HEADER_BYTES
            + self
                .preprocessed
                .iter()
                .map(|block| BLOCK_ID_AND_WIDTH_BYTES + block.coefficient_bytes() as u64)
                .sum::<u64>()
    }

    /// Publishes the masks of prepared blocks and derives their epoch.
    pub(crate) fn assemble(
        native: Profile,
        db: Vec<u8>,
        preprocessed: Vec<NativePreprocessed>,
    ) -> Result<Self, String> {
        let public_params = native.publish(&preprocessed)?;
        let digest = Sha256::digest(&public_params);
        let mut epoch = [0u8; 8];
        epoch.copy_from_slice(&digest[..8]);
        Ok(Self {
            native,
            preprocessed,
            db,
            public_params,
            public_params_sha256: hex::encode(digest),
            public_params_epoch: epoch,
        })
    }

    /// Answers one query. The row selected is never known to this function.
    ///
    /// `binding` is the prefix the query must carry and the response repeats:
    /// it names the revision and table this runtime belongs to, so a body
    /// routed to the wrong one fails here rather than returning rows from a
    /// range the wallet did not ask about.
    pub fn evaluate(
        &self,
        shared: &SharedParams,
        binding: [u8; 8],
        body: &[u8],
    ) -> Result<Vec<u8>, String> {
        self.answer(binding, &shared.parse(binding, body, shared.profile.rows)?)
    }

    /// Checks a runtime this process did not build against the segment's
    /// verified plaintext `rows`, end to end.
    ///
    /// The encoded database must equal `rows` coefficient for coefficient,
    /// and a fresh client query for one sampled populated row must decode,
    /// under the published masks, to that row. The first proves the database
    /// is the segment's; the second that it answers under the preprocessing
    /// it was loaded with, which a database built from other rows does not,
    /// at any row. Together they cover what [`disk::DiskCache::load`] does not
    /// check. Returns the row checked.
    pub fn self_check(&self, shared: &SharedParams, rows: &[u8]) -> Result<usize, String> {
        let profile = &shared.profile;
        if rows.len() != profile.rows * profile.row_bytes {
            return Err("self-check rows have the wrong length".into());
        }
        let db = &self.db;
        if db.len() != rows.len() {
            return Err("database length differs from the segment".into());
        }
        if let Some(col) = first_differing_column(profile.cols, profile.rows, |row, col| {
            db[interleaved_index(profile.rows, col, row)] != rows[row * profile.row_bytes + col]
        }) {
            return Err(format!("database column {col} differs from the segment"));
        }
        let row = sample_row(rows, profile.row_bytes, profile.rows);
        let binding = *b"selfchck";
        let (secret, upload) =
            profile
                .native
                .prepare(&profile.setup, &profile.masks, profile.rows, row)?;
        let mut body = binding.to_vec();
        body.extend(upload);
        let answer = self.evaluate(shared, binding, &body)?;
        let mut decoded = profile.native.decode_cols(
            &secret,
            &self.public_params,
            &answer[16..],
            profile.cols,
        )?;
        decoded.truncate(profile.row_bytes);
        let at = row * profile.row_bytes;
        if decoded != rows[at..at + profile.row_bytes] {
            return Err(format!("row {row} does not decode to the segment's row"));
        }
        Ok(row)
    }

    /// Answers a query already parsed by [`SharedParams::parse`].
    pub fn answer(&self, binding: [u8; 8], query: &ParsedQuery) -> Result<Vec<u8>, String> {
        let db = &self.db;
        let rows = query.query.len();
        let prepared = PreparedU16Query::new(&query.query, native::Q).map_err(|e| e.to_string())?;
        let mut intermediate = vec![0u64; self.preprocessed.len() * native::D];
        if db.len() != rows * intermediate.len() {
            return Err("query rows do not match the database".into());
        }
        intermediate
            .par_chunks_mut(16)
            .zip(db.par_chunks(rows * 16))
            .for_each(|(out, band)| prepared.multiply_interleaved_u8(band, out));
        let body = self
            .native
            .pack(&self.preprocessed, &query.keys, &intermediate)?;
        let mut response = Vec::with_capacity(16 + body.len());
        response.extend_from_slice(&binding);
        response.extend_from_slice(&self.public_params_epoch);
        response.extend_from_slice(&body);
        Ok(response)
    }
}
