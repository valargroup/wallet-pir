//! Transparent's native PIR profile: ReinspiRING packing with two public masks.
//!
//! The same profile Enhance and Status deploy — `d` = 2,048, `q` = 2^54,
//! `p` = 2^16, two-limb Gaussian key packing with 19-bit limbs, and two public
//! masks rounded to 29 bits — applied to Transparent's fixed-width tables. One
//! plaintext instance is one 2,048-coefficient polynomial of 16-bit values, so a
//! 4,096-byte row is exactly one packing block.
//!
//! Per query the client uploads one `K_g` packing key (27,648 bytes) and a
//! 49-bit selection vector (`rows * 49 / 8` bytes). Per block the server
//! publishes both masks (14,848 bytes, once per segment) and answers with a
//! 22-bit body (5,632 bytes).
//!
//! The helpers here are adapted from `enhance/crates/enhance-pir/src/native.rs`
//! (wallet-pir, commit 46a82a62) rather than imported from it: enabling
//! `enhance-pir/native-reinspiring` from a Transparent crate would switch the
//! q48 Enhance binaries through Cargo feature unification. The byte framing is
//! the same — mask, query and response bit-packing, rounding and the `K_g`
//! encoding — and `crosscheck/` verifies that against the Enhance helper in a
//! separate target directory.
//!
//! Unlike Enhance, the setup is not bound to a shard: query masks and the
//! packing setup are derived per geometry and table, so one query is answered
//! by every segment of every shard of that geometry. The masks use a zero
//! snapshot id, so they are stable across publications; each segment's
//! published masks follow from its own database.
//!
//! Correctness certificates for this mode are snapshot-specific: see
//! `examples/native_certificate.rs` in the shard server.

use ipir_sp::bits::{contiguous_bytes_to_u64s, u64s_to_contiguous_bytes};
use ipir_sp::native::{NativeProfile, NativePublicSetup};
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha20Rng;
use rayon::prelude::*;
use reinspiring::native::{NativeParams, NativeTwoMaskCiphertext, SecretDistribution};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::borrow::Borrow;

pub use reinspiring::native::{NativeKeys, NativePreprocessed, NativeSecret, NativeSetup};

/// Name of this profile, as the service publishes it.
pub const PROFILE: &str = "reinspiring-two-mask-m29-v1";
/// Ciphertext modulus.
pub const Q: u64 = 1 << 54;
pub const Q_BITS: usize = 54;
/// Ring degree, which is also the row and column quantum.
pub const D: usize = 2048;
/// Plaintext bits per coefficient.
pub const P_BITS: usize = 16;
/// Gadget limb width and count of the packing key.
pub const GADGET_BITS: usize = 19;
pub const ELL: usize = 2;
/// Bits each published mask coefficient is rounded to.
pub const MASK_BITS: usize = 29;
/// Bits each selection coefficient is uploaded at.
pub const QUERY_BITS: usize = 49;
/// Bits each response body coefficient is returned at.
pub const RESPONSE_BITS: usize = 22;
/// Uploaded `K_g` packing key.
pub const KEY_BYTES: usize = ELL * D * Q_BITS / 8;
/// Plaintext bytes one block carries: one row width quantum.
pub const INSTANCE_BYTES: usize = D * P_BITS / 8;
/// Published bytes per block: both masks at `MASK_BITS`.
pub const BLOCK_PUBLIC_BYTES: usize = 2 * D * MASK_BITS / 8;
/// Response body bytes per block at `RESPONSE_BITS`.
pub const BLOCK_RESPONSE_BYTES: usize = D * RESPONSE_BITS / 8;

const _: () = assert!(KEY_BYTES == 27_648);
const _: () = assert!(INSTANCE_BYTES == 4_096);
const _: () = assert!(BLOCK_PUBLIC_BYTES == 14_848);
const _: () = assert!(BLOCK_RESPONSE_BYTES == 5_632);

/// The fixed packing parameters.
pub fn params() -> NativeParams {
    NativeParams::new(
        D,
        Q_BITS as u32,
        P_BITS as u32,
        GADGET_BITS as u32,
        ELL,
        SecretDistribution::Gaussian,
    )
    .expect("fixed native profile")
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

/// Domain-separated first-dimension masks for a `rows` by `cols` database.
///
/// Uses a zero snapshot id so the masks, and therefore every published hint
/// contribution, stay stable across publications of the same shape.
pub fn public_query_masks(
    seed: [u8; 32],
    rows: usize,
    cols: usize,
) -> Result<Vec<Vec<u64>>, String> {
    let profile = NativeProfile::new(params(), rows, cols).map_err(|e| e.to_string())?;
    Ok(NativePublicSetup::new(profile, seed, [0; 32])
        .query_masks()
        .to_vec())
}

/// Canonical published bytes for `cols` coefficients of two-mask blocks.
pub const fn public_len(cols: usize) -> usize {
    (2 * cols * MASK_BITS).div_ceil(8)
}

/// Response body bytes for `cols` coefficients.
pub const fn response_len(cols: usize) -> usize {
    (cols * RESPONSE_BITS).div_ceil(8)
}

/// Upload bytes for a `rows`-row table: `K_g` then the selection.
pub const fn request_len(rows: usize) -> usize {
    KEY_BYTES + (rows * QUERY_BITS).div_ceil(8)
}

fn round(x: u64, bits: usize) -> u64 {
    (((x as u128 * (1u128 << bits) + (Q / 2) as u128) / Q as u128) as u64) & ((1 << bits) - 1)
}

/// Canonical published bytes for two-mask blocks: every first mask, then every
/// second mask, each coefficient rounded to `MASK_BITS`.
pub fn publish<B: Borrow<NativePreprocessed>>(blocks: &[B]) -> Result<Vec<u8>, String> {
    let others = blocks
        .iter()
        .map(|b| {
            b.borrow()
                .other_mask()
                .ok_or("one-mask native preprocessing")
        })
        .collect::<Result<Vec<_>, _>>()?;
    let words: Vec<_> = blocks
        .iter()
        .flat_map(|b| b.borrow().mask().iter())
        .chain(others.into_iter().flatten())
        .map(|&x| round(x, MASK_BITS))
        .collect();
    Ok(u64s_to_contiguous_bytes(&words, MASK_BITS))
}

/// Fresh secret, one-key packing upload and 49-bit selection query.
pub fn prepare_with(
    setup: &NativeSetup,
    masks: &[Vec<u64>],
    rows: usize,
    target: usize,
) -> Result<(NativeSecret, Vec<u8>), String> {
    if target >= rows || !rows.is_multiple_of(D) || masks.len() < rows / D {
        return Err("native query shape".into());
    }
    let mut entropy = [0; 32];
    rand::rngs::OsRng.fill_bytes(&mut entropy);
    let mut rng = ChaCha20Rng::from_seed(entropy);
    let secret = NativeSecret::sample(&params(), &mut rng);
    let keys = NativeKeys::generate_one_key(setup, &secret, &mut rng).map_err(|e| e.to_string())?;
    let query = secret
        .encrypt_selection(&masks[..rows / D], target, &mut rng)
        .map_err(|e| e.to_string())?;
    let mut bytes = u64s_to_contiguous_bytes(&keys.kg_words(), Q_BITS);
    let switched: Vec<_> = query.iter().map(|&x| round(x, QUERY_BITS)).collect();
    bytes.extend(u64s_to_contiguous_bytes(&switched, QUERY_BITS));
    Ok((secret, bytes))
}

/// Parses an upload into its packing key and the selection lifted back to `q`.
pub fn parse_with(
    setup: &NativeSetup,
    bytes: &[u8],
    rows: usize,
) -> Result<(NativeKeys, Vec<u64>), String> {
    if rows == 0 || !rows.is_multiple_of(D) || bytes.len() != request_len(rows) {
        return Err("native query framing".into());
    }
    let keys = NativeKeys::from_kg_words(
        setup,
        &contiguous_bytes_to_u64s(&bytes[..KEY_BYTES], Q_BITS),
    )
    .map_err(|e| e.to_string())?;
    let mut query: Vec<u64> = contiguous_bytes_to_u64s(&bytes[KEY_BYTES..], QUERY_BITS)
        .into_iter()
        .map(|x| x << (Q_BITS - QUERY_BITS))
        .collect();
    // The packing leaves up to seven padding bits, which decode as one extra
    // coefficient when they fill a whole word. They are never part of the query.
    query.truncate(rows);
    if query.len() != rows {
        return Err("native query framing".into());
    }
    Ok((keys, query))
}

/// Decodes every little-endian u16 plaintext coefficient of a two-mask response.
pub fn decode_cols(
    secret: &NativeSecret,
    public: &[u8],
    body: &[u8],
    cols: usize,
) -> Result<Vec<u8>, String> {
    if cols == 0
        || !cols.is_multiple_of(D)
        || public.len() != public_len(cols)
        || body.len() != response_len(cols)
    {
        return Err("native response shape".into());
    }
    let masks: Vec<_> = contiguous_bytes_to_u64s(public, MASK_BITS)
        .into_iter()
        .map(|x| x << (Q_BITS - MASK_BITS))
        .collect();
    let (a, a_other) = masks[..2 * cols].split_at(cols);
    let b: Vec<_> = contiguous_bytes_to_u64s(body, RESPONSE_BITS)
        .into_iter()
        .map(|x| x << (Q_BITS - RESPONSE_BITS))
        .collect();
    let mut out = Vec::with_capacity(cols * 2);
    for ((a, a_other), b) in a
        .chunks_exact(D)
        .zip(a_other.chunks_exact(D))
        .zip(b[..cols].chunks_exact(D))
    {
        let ct =
            NativeTwoMaskCiphertext::from_rows(&params(), a.to_vec(), a_other.to_vec(), b.to_vec())
                .map_err(|e| e.to_string())?;
        for x in secret.decrypt_two_mask(&ct).map_err(|e| e.to_string())? {
            out.extend((x as u16).to_le_bytes());
        }
    }
    Ok(out)
}

/// Prepares one uploaded key for every block, then finishes each block with
/// its scan result. Returns the switched response bodies.
pub fn pack<B: Borrow<NativePreprocessed> + Sync>(
    blocks: &[B],
    keys: &NativeKeys,
    intermediate: &[u64],
) -> Result<Vec<u8>, String> {
    if blocks.is_empty()
        || intermediate.len() != blocks.len() * D
        || intermediate.iter().any(|&x| x >= Q)
    {
        return Err("native intermediate shape".into());
    }
    let prepared = blocks[0]
        .borrow()
        .prepare_keys(keys)
        .map_err(|e| e.to_string())?;
    let packed = blocks
        .par_iter()
        .zip(intermediate.par_chunks_exact(D))
        .map(|(p, b)| p.borrow().prepare_pack(&prepared)?.finish_two_mask(b))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    let values = packed
        .iter()
        .flat_map(|ct| ct.rows().2.iter().copied())
        .collect::<Vec<_>>();
    Ok(response(&values))
}

/// Rounds response bodies to `RESPONSE_BITS` and bit-packs them.
pub fn response(bodies: &[u64]) -> Vec<u8> {
    let switched: Vec<_> = bodies.iter().map(|&x| round(x, RESPONSE_BITS)).collect();
    u64s_to_contiguous_bytes(&switched, RESPONSE_BITS)
}

/// The public hint `H = A * D` for one segment, one block of `D` rows per
/// packing block, using exact lifted native-ring products.
///
/// `column(c)` is column `c` of the database: `rows` plaintext coefficients.
pub fn hint<'a>(
    masks: &[Vec<u64>],
    rows: usize,
    cols: usize,
    column: impl Fn(usize) -> &'a [u16] + Sync,
) -> Result<Vec<Vec<Vec<u64>>>, String> {
    if rows == 0 || !rows.is_multiple_of(D) || !cols.is_multiple_of(D) || masks.len() != rows / D {
        return Err("native hint shape".into());
    }
    let lift = reinspiring::lift_ntt::LiftContext::new(D, Q).map_err(|e| e.to_string())?;
    let public = lift
        .prepare_public_dot(masks, 65535)
        .map_err(|e| e.to_string())?;
    (0..cols)
        .step_by(D)
        .map(|start| {
            (start..start + D)
                .into_par_iter()
                .map(|col| {
                    let data = column(col);
                    if data.len() != rows {
                        return Err("native hint column length".to_string());
                    }
                    let polys = data
                        .chunks_exact(D)
                        .map(|p| p.iter().map(|&x| x as u64).collect::<Vec<_>>())
                        .collect::<Vec<_>>();
                    lift.public_dot(&public, &polys).map_err(|e| e.to_string())
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .collect()
}

/// Two-mask preprocessing for every block of a hint.
pub fn preprocess(
    setup: &NativeSetup,
    hint: &[Vec<Vec<u64>>],
) -> Result<Vec<NativePreprocessed>, String> {
    hint.par_iter()
        .map(|block| NativePreprocessed::build_two_mask(setup, block))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())
}

/// Upper bound on one block's `reinspiring::prepared_native` encoding: block id,
/// both masks, a width word and the compiled matrix at eight-byte words.
pub const PREPARED_BLOCK_MAX_BYTES: u64 = (32 + 2 * D * 8 + 8 + D * D * ELL * 8) as u64;
/// Lower bound, at the four-byte words used when every entry fits an `i32`.
pub const PREPARED_BLOCK_MIN_BYTES: u64 = (32 + 2 * D * 8 + 8 + D * D * ELL * 4) as u64;
/// The encoding's magic and block count.
pub const PREPARED_HEADER_BYTES: u64 = 16;

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

    /// Parses an upload into its key and lifted selection.
    pub fn parse(&self, bytes: &[u8]) -> Result<(NativeKeys, Vec<u64>), String> {
        parse_with(&self.setup, bytes, self.rows)
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
