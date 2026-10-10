//! Transparent's native PIR profile: ReinspiRING packing with two public masks.
//!
//! The helpers themselves are the shared `pir-native` crate, which Enhance and
//! Status use as well; this crate re-exports them and adds what is
//! Transparent's own. One plaintext instance is one 2,048-coefficient
//! polynomial of 16-bit values, so a 4,096-byte row is exactly one packing
//! block.
//!
//! Unlike Enhance, the setup is not bound to a shard: query masks and the
//! packing setup are derived per geometry and table, so one query is answered
//! by every segment of every shard of that geometry. The masks use a zero
//! snapshot id, so they are stable across publications; each segment's
//! published masks follow from its own database.
//!
//! Every table publishes two schemes over the same setup and masks, differing
//! only in how the selection is uploaded: [`PROFILE`] at 49 bits rounded to
//! nearest, which every wallet deployed before dithering sends, and
//! [`DITHERED_PROFILE`] at 44 dithered bits (see `pir-native`). A server
//! accepts both, telling them apart by the body's exact length, so a wallet
//! that does not know the dithered scheme is unaffected. A wallet that
//! re-derives the dithered scheme the service advertises sends 44 bits;
//! otherwise it sends 49.
//!
//! Correctness certificates for this mode are snapshot-specific, and a
//! snapshot is served at both widths, so it needs both: see
//! `examples/native_certificate.rs` in the shard server.

use pir_native::hint as pir_hint;
pub use pir_native::*;

pub mod batched_hint;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Name of this profile, as the service publishes it: the 49-bit
/// nearest-rounded query.
pub const PROFILE: &str = "reinspiring-two-mask-m29-v1";
/// Name of the dithered-query profile: the same setup, masks and response, with
/// the selection uploaded at [`DITHERED_QUERY_BITS`] dithered bits.
pub const DITHERED_PROFILE: &str = "reinspiring-two-mask-m29-dq44-v1";

/// Upload bytes a wallet sends to a `rows`-row table under the dithered
/// scheme, after the 8-byte binding: `K_g` then the 44-bit selection.
pub const fn dithered_request_len(rows: usize) -> usize {
    request_len_bits(rows, DITHERED_QUERY_BITS)
}

/// Domain-separated 32-byte seeds for one geometry's table.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TableSeeds {
    /// Expands to the public first-dimension query masks.
    pub query_masks: [u8; 32],
    /// Seeds the packing setup the `K_g` key is generated under.
    pub packing: [u8; 32],
}

/// Seeds for `table` of `geometry` under `schema`.
///
/// Distinct per geometry and table, and domain-separated by the schema string
/// and this profile, so neither a future schema nor another profile reuses
/// them. Both sides derive them locally; the service's published identity is
/// checked against the derivation, never adopted.
pub fn table_seeds(schema: &str, geometry: &str, table: &str) -> TableSeeds {
    let derive = |purpose: &[u8]| -> [u8; 32] {
        let mut hash = Sha256::new();
        hash.update(schema.as_bytes());
        hash.update(b"/native-reinspiring-m29/");
        hash.update(purpose);
        hash.update(b"\0");
        hash.update(geometry.as_bytes());
        hash.update(b"\0");
        hash.update(table.as_bytes());
        hash.finalize().into()
    };
    TableSeeds {
        query_masks: derive(b"query-masks"),
        packing: derive(b"packing-setup"),
    }
}

/// What the service publishes about one table's native parameters.
///
/// Every field is re-derived by the client from the pinned geometry and
/// compared whole. A service that could move any field independently could
/// choose parameters for the wallet — and a mask seed or setup of its choosing
/// is a query it can read.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeScheme {
    pub profile: String,
    pub d: usize,
    pub q_bits: usize,
    pub p_bits: usize,
    pub gadget_bits: usize,
    pub ell: usize,
    pub secret: String,
    pub mask_bits: usize,
    pub query_bits: usize,
    pub response_bits: usize,
    pub rows: u64,
    pub row_bytes: u32,
    pub cols: usize,
    /// `NativeParams::encoding`, hex.
    pub native_encoding: String,
    /// Seed of the public query masks, hex.
    pub query_mask_seed: String,
    /// Packing setup identifier, hex.
    pub packing_setup_id: String,
    /// Upload bytes after the 8-byte binding: `K_g` and the selection.
    pub request_bytes: usize,
    /// Published mask bytes per segment.
    pub public_bytes: usize,
    /// Response body bytes per segment, after the 16-byte header.
    pub response_bytes: usize,
}

impl NativeScheme {
    /// A stable digest of the whole identity.
    pub fn digest(&self) -> String {
        hex::encode(Sha256::digest(
            serde_json::to_vec(self).expect("scheme serializes"),
        ))
    }
}

/// Everything one table's queries need on either side, derived once per
/// geometry and table.
pub struct TableProfile {
    pub rows: usize,
    pub row_bytes: usize,
    pub cols: usize,
    pub setup: NativeSetup,
    pub masks: Vec<Vec<u64>>,
    /// The 49-bit nearest-rounded scheme, [`PROFILE`].
    pub scheme: NativeScheme,
    /// The 44-bit dithered scheme, [`DITHERED_PROFILE`]: `scheme` with only
    /// the profile name, query width and request length changed.
    pub dithered_scheme: NativeScheme,
}

impl TableProfile {
    /// Derives the profile for `table` of `geometry` at `rows` by `row_bytes`.
    pub fn new(
        schema: &str,
        geometry: &str,
        table: &str,
        rows: u64,
        row_bytes: u32,
    ) -> Result<Self, String> {
        let rows_usize = usize::try_from(rows).map_err(|_| "native rows exceed usize")?;
        let row_bytes_usize = row_bytes as usize;
        if rows_usize == 0 || !rows_usize.is_multiple_of(D) {
            return Err(format!(
                "{rows} rows: the native profile needs a positive multiple of {D}"
            ));
        }
        if row_bytes_usize == 0 || !row_bytes_usize.is_multiple_of(INSTANCE_BYTES) {
            return Err(format!(
                "{row_bytes}-byte rows: the native profile needs whole {INSTANCE_BYTES}-byte instances"
            ));
        }
        let cols = row_bytes_usize / 2;
        let seeds = table_seeds(schema, geometry, table);
        let setup = NativeSetup::new(params(), seeds.packing);
        let masks = public_query_masks(seeds.query_masks, rows_usize, cols)?;
        let scheme = NativeScheme {
            profile: PROFILE.to_string(),
            d: D,
            q_bits: Q_BITS,
            p_bits: P_BITS,
            gadget_bits: GADGET_BITS,
            ell: ELL,
            secret: "gaussian".to_string(),
            mask_bits: MASK_BITS,
            query_bits: QUERY_BITS,
            response_bits: RESPONSE_BITS,
            rows,
            row_bytes,
            cols,
            native_encoding: hex::encode(params().encoding()),
            query_mask_seed: hex::encode(seeds.query_masks),
            packing_setup_id: hex::encode(setup.id()),
            request_bytes: request_len(rows_usize),
            public_bytes: public_len(cols),
            response_bytes: response_len(cols),
        };
        let dithered_scheme = NativeScheme {
            profile: DITHERED_PROFILE.to_string(),
            query_bits: DITHERED_QUERY_BITS,
            request_bytes: dithered_request_len(rows_usize),
            ..scheme.clone()
        };
        Ok(Self {
            rows: rows_usize,
            row_bytes: row_bytes_usize,
            cols,
            setup,
            masks,
            scheme,
            dithered_scheme,
        })
    }

    pub fn blocks(&self) -> usize {
        self.cols / D
    }

    /// Fresh secret and 49-bit upload selecting row `target`, under
    /// [`Self::scheme`].
    pub fn prepare(&self, target: usize) -> Result<(NativeSecret, Vec<u8>), String> {
        prepare_with(&self.setup, &self.masks, self.rows, target)
    }

    /// Fresh secret and 44-bit dithered upload selecting row `target`, under
    /// [`Self::dithered_scheme`].
    pub fn prepare_dithered(&self, target: usize) -> Result<(NativeSecret, Vec<u8>), String> {
        self.prepare_dithered_prefix(self.rows, target)
    }

    /// [`Self::prepare`] selecting among only the first `query_rows`, under
    /// the leading full-shape masks. `query_rows` must be public and the same
    /// for every query to the table.
    pub fn prepare_prefix(
        &self,
        query_rows: usize,
        target: usize,
    ) -> Result<(NativeSecret, Vec<u8>), String> {
        if query_rows > self.rows {
            return Err("native query shape".into());
        }
        prepare_with(&self.setup, &self.masks, query_rows, target)
    }

    /// [`Self::prepare_dithered`] selecting among only the first `query_rows`;
    /// see [`Self::prepare_prefix`].
    pub fn prepare_dithered_prefix(
        &self,
        query_rows: usize,
        target: usize,
    ) -> Result<(NativeSecret, Vec<u8>), String> {
        if query_rows > self.rows {
            return Err("native query shape".into());
        }
        prepare_dithered(&self.setup, &self.masks, query_rows, target)
    }

    /// Whether an upload of `len` bytes, after the binding, has one of the two
    /// lengths this table accepts.
    pub fn accepts_request_len(&self, len: usize) -> bool {
        accepted_query_bits(self.rows, len).is_some()
    }

    /// Whether an upload of `len` bytes, after the binding, selects every row
    /// or only the first `query_rows` at an accepted width.
    pub fn accepts_selection_len(&self, query_rows: usize, len: usize) -> bool {
        accepted_query_shape(self.rows, query_rows, len).is_some()
    }

    /// Parses an upload of either scheme into its key and lifted selection.
    /// The width is the one the exact length names; any other length is
    /// refused.
    pub fn parse(&self, bytes: &[u8]) -> Result<(NativeKeys, Vec<u64>), String> {
        parse_accepted(&self.setup, bytes, self.rows)
    }

    /// Parses an upload of either scheme over every row or over only the
    /// first `query_rows`, whichever its exact length names, zero-filled back
    /// to every row.
    pub fn parse_selection(
        &self,
        bytes: &[u8],
        query_rows: usize,
    ) -> Result<(NativeKeys, Vec<u64>), String> {
        parse_selection(&self.setup, bytes, query_rows, self.rows)
    }

    /// Decodes one segment's body under that segment's published masks,
    /// truncated to the row width.
    pub fn decode(
        &self,
        secret: &NativeSecret,
        public: &[u8],
        body: &[u8],
    ) -> Result<Vec<u8>, String> {
        let mut row = decode_cols(secret, public, body, self.cols)?;
        row.truncate(self.row_bytes);
        Ok(row)
    }

    /// Bytes one segment's database occupies as u16 coefficients.
    pub fn database_bytes(&self) -> u64 {
        self.rows as u64 * self.cols as u64 * 2
    }

    /// Upper bound on one segment's `prepared_native` encoding.
    pub fn prepared_max_bytes(&self) -> u64 {
        PREPARED_HEADER_BYTES + self.blocks() as u64 * PREPARED_BLOCK_MAX_BYTES
    }

    /// Lower bound on one segment's `prepared_native` encoding.
    pub fn prepared_min_bytes(&self) -> u64 {
        PREPARED_HEADER_BYTES + self.blocks() as u64 * PREPARED_BLOCK_MIN_BYTES
    }
}

/// Column-major u16 coefficients of a row-major table: coefficient `c` of row
/// `r` is the little-endian u16 at byte `r * row_bytes + 2 * c`, exactly the
/// bytes a client decodes. Rows shorter than `row_bytes` (never in practice)
/// read as zero.
pub fn row_coefficient(rows: &[u8], row_bytes: usize, row: usize, col: usize) -> u16 {
    let at = row * row_bytes + 2 * col;
    match rows.get(at..at + 2) {
        Some(bytes) => u16::from_le_bytes([bytes[0], bytes[1]]),
        None => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::RngCore;
    use rand_chacha::{rand_core::SeedableRng, ChaCha20Rng};

    fn database(rows: usize, cols: usize, seed: u8) -> Vec<u16> {
        // Column-major, deterministic, spanning the whole u16 range.
        let mut rng = ChaCha20Rng::from_seed([seed; 32]);
        (0..rows * cols).map(|_| rng.next_u32() as u16).collect()
    }

    fn scan(db: &[u16], rows: usize, cols: usize, query: &[u64]) -> Vec<u64> {
        (0..cols)
            .map(|c| {
                db[c * rows..(c + 1) * rows]
                    .iter()
                    .zip(query)
                    .fold(0u64, |a, (&x, &q)| {
                        a.wrapping_add((x as u64).wrapping_mul(q))
                    })
                    & (Q - 1)
            })
            .collect()
    }

    #[test]
    fn sizes_are_the_deployed_profile() {
        assert_eq!(public_len(D), 14_848);
        assert_eq!(response_len(D), 5_632);
        // The 49-bit query every deployed wallet sends.
        assert_eq!(request_len(2_048), 27_648 + 12_544);
        assert_eq!(request_len(65_536), 27_648 + 401_408);
        // The 44-bit dithered query.
        assert_eq!(dithered_request_len(2_048), 27_648 + 11_264);
        assert_eq!(dithered_request_len(4_096), 27_648 + 22_528);
        assert_eq!(dithered_request_len(32_768), 27_648 + 180_224);
        assert_eq!(dithered_request_len(65_536), 27_648 + 360_448);
    }

    /// The dithered scheme is the legacy one with the query alone changed, and
    /// the legacy scheme is what it always was.
    #[test]
    fn the_dithered_scheme_differs_only_in_its_query() {
        let p = TableProfile::new("s", "g", "t", 8_192, 4_096).unwrap();
        assert_eq!(p.scheme.profile, PROFILE);
        assert_eq!(p.scheme.query_bits, 49);
        assert_eq!(p.scheme.request_bytes, 27_648 + 50_176);
        assert_eq!(
            p.dithered_scheme.profile,
            "reinspiring-two-mask-m29-dq44-v1"
        );
        assert_eq!(p.dithered_scheme.query_bits, 44);
        assert_eq!(p.dithered_scheme.request_bytes, 27_648 + 45_056);
        let mut back = p.dithered_scheme.clone();
        back.profile = p.scheme.profile.clone();
        back.query_bits = p.scheme.query_bits;
        back.request_bytes = p.scheme.request_bytes;
        assert_eq!(back, p.scheme);
        assert_ne!(p.dithered_scheme.digest(), p.scheme.digest());
    }

    #[test]
    fn seeds_are_distinct_per_geometry_table_and_schema() {
        let a = table_seeds("s", "g", "directory");
        assert_ne!(a, table_seeds("s", "g", "pages"));
        assert_ne!(a, table_seeds("s", "h", "directory"));
        assert_ne!(a, table_seeds("t", "g", "directory"));
        assert_ne!(a.query_masks, a.packing);
    }

    #[test]
    fn a_profile_refuses_shapes_the_scheme_would_not_serve() {
        assert!(TableProfile::new("s", "g", "t", 3_000, 4_096).is_err());
        assert!(TableProfile::new("s", "g", "t", 2_048, 3_584).is_err());
        let p = TableProfile::new("s", "g", "t", 4_096, 4_096).unwrap();
        assert_eq!(p.scheme.request_bytes, request_len(4_096));
        assert_eq!(p.masks.len(), 2);
    }

    /// End-to-end over the primitives the server and wallet use.
    #[test]
    fn two_mask_rounded_roundtrip() {
        let profile = TableProfile::new("test", "g", "directory", 4_096, 4_096).unwrap();
        let (rows, cols) = (profile.rows, profile.cols);
        let db = database(rows, cols, 7);
        let hint = hint(&profile.masks, rows, cols, |c| {
            &db[c * rows..(c + 1) * rows]
        })
        .unwrap();
        let blocks = preprocess(&profile.setup, &hint).unwrap();
        let published = publish(&blocks).unwrap();
        assert_eq!(published.len(), profile.scheme.public_bytes);
        for target in [0, 1234, rows - 1] {
            let (secret, body) = profile.prepare(target).unwrap();
            assert_eq!(body.len(), profile.scheme.request_bytes);
            let (keys, query) = profile.parse(&body).unwrap();
            let response = pack(&blocks, &keys, &scan(&db, rows, cols, &query)).unwrap();
            assert_eq!(response.len(), profile.scheme.response_bytes);
            let row = profile.decode(&secret, &published, &response).unwrap();
            let expected: Vec<u8> = (0..cols)
                .flat_map(|c| db[c * rows + target].to_le_bytes())
                .collect();
            assert_eq!(row, expected, "target {target}");
        }
        // The dithered query decodes the same rows, through the same parse.
        for target in [0, 2_047, rows - 1] {
            let (secret, body) = profile.prepare_dithered(target).unwrap();
            assert_eq!(body.len(), profile.dithered_scheme.request_bytes);
            assert!(profile.accepts_request_len(body.len()));
            let (keys, query) = profile.parse(&body).unwrap();
            let response = pack(&blocks, &keys, &scan(&db, rows, cols, &query)).unwrap();
            let row = profile.decode(&secret, &published, &response).unwrap();
            let expected: Vec<u8> = (0..cols)
                .flat_map(|c| db[c * rows + target].to_le_bytes())
                .collect();
            assert_eq!(row, expected, "dithered target {target}");
        }
        // Only those two lengths parse.
        for len in [
            profile.scheme.request_bytes - 1,
            profile.scheme.request_bytes + 1,
            profile.dithered_scheme.request_bytes - 1,
            profile.dithered_scheme.request_bytes + 1,
        ] {
            assert!(!profile.accepts_request_len(len));
            assert!(profile.parse(&vec![0; len]).is_err(), "{len} bytes");
        }
        // A key from another table's setup is refused rather than packed.
        let other = TableProfile::new("test", "g", "pages", 4_096, 4_096).unwrap();
        let (_, body) = other.prepare(0).unwrap();
        let (keys, query) = other.parse(&body).unwrap();
        assert!(pack(&blocks, &keys, &scan(&db, rows, cols, &query)).is_err());
    }

    #[test]
    fn row_coefficients_match_the_bit_reader() {
        let rows: Vec<u8> = (0..2 * 4_096).map(|i| (i * 37 % 251) as u8).collect();
        for (row, col) in [(0, 0), (0, 2_047), (1, 5), (1, 2_047)] {
            assert_eq!(
                row_coefficient(&rows, 4_096, row, col) as u64,
                ipir_sp::bits::read_bits(&rows[row * 4_096..(row + 1) * 4_096], col * 16, 16)
            );
        }
    }
}
