//! The native ReinspiRING two-mask m29 PIR profile Enhance, Status and
//! Transparent deploy: `d` = 2,048, `q` = 2^54, `p` = 2^16, two-limb Gaussian
//! `K_g` key packing with 19-bit limbs, and two public masks rounded to 29 bits.
//!
//! These are the helpers every product computes identically: parameters,
//! query masks, the public hint, two-mask preprocessing, published mask bytes,
//! request framing, packing and decoding. What differs between products — the
//! seeds for the query masks and packing setup, and how a database maps onto
//! rows and columns — stays with each product and is passed in.
//!
//! Per query the client uploads one `K_g` packing key (27,648 bytes) and a
//! 49-bit selection vector (`rows * 49 / 8` bytes). Per 2,048-coefficient block
//! the server publishes both masks (14,848 bytes) and answers with a 22-bit
//! body (5,632 bytes). Correctness certificates for this mode are
//! snapshot-specific.
//!
//! `tests/golden.rs` pins the bytes. Its digests were produced by the Enhance
//! and Transparent copies this crate replaced, which agreed byte for byte.

use ipir_sp::bits::{contiguous_bytes_to_u64s, u64s_to_contiguous_bytes};
use ipir_sp::native::{NativeProfile, NativePublicSetup};
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha20Rng;
use rayon::prelude::*;
use reinspiring::native::{NativeParams, NativeTwoMaskCiphertext, SecretDistribution};
use std::borrow::Borrow;

pub use reinspiring::native::{NativeKeys, NativePreprocessed, NativeSecret, NativeSetup};

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
