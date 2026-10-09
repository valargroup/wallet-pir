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
//! Correctness certificates for this mode are snapshot-specific: see
//! `examples/native_certificate.rs` in the shard server.

use pir_native::hint as pir_hint;
pub use pir_native::*;

pub mod batched_hint;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Name of this profile, as the service publishes it.
pub const PROFILE: &str = "reinspiring-two-mask-m29-v1";

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
    pub scheme: NativeScheme,
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
        Ok(Self {
            rows: rows_usize,
            row_bytes: row_bytes_usize,
            cols,
            setup,
            masks,
            scheme,
        })
    }

    pub fn blocks(&self) -> usize {
        self.cols / D
    }

    /// Fresh secret and upload selecting row `target`.
    pub fn prepare(&self, target: usize) -> Result<(NativeSecret, Vec<u8>), String> {
        prepare_with(&self.setup, &self.masks, self.rows, target)
    }

    /// Fresh secret and upload selecting row `target` among only the first
    /// `query_rows`, under the leading full-shape masks. `query_rows` must be
    /// public and the same for every query to the table.
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

    /// Parses an upload into its key and lifted selection.
    pub fn parse(&self, bytes: &[u8]) -> Result<(NativeKeys, Vec<u64>), String> {
        parse_with(&self.setup, bytes, self.rows)
    }

    /// Parses an upload over every row or, at the shorter length, over only
    /// the first `query_rows`, zero-filled back to every row.
    pub fn parse_selection(
        &self,
        bytes: &[u8],
        query_rows: usize,
    ) -> Result<(NativeKeys, Vec<u64>), String> {
        if bytes.len() == request_len(self.rows) {
            return self.parse(bytes);
        }
        parse_prefix_with(&self.setup, bytes, query_rows, self.rows)
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
        assert_eq!(request_len(2_048), 27_648 + 12_544);
        assert_eq!(request_len(65_536), 27_648 + 401_408);
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
