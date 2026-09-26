//! Experimental native packing profile, with snapshot/request binding supplied
//! by the versioned Enhance envelope. Query masks remain stable across publications
//! so unchanged worker units can reuse their public hint contributions.
//!
//! Responses use ipir-sp's two-mask output: the client uploads only the `K_g`
//! key and decodes under both published masks, which are rounded to
//! `MASK_BITS`. Correctness certificates for this mode are snapshot-specific.
use ipir_sp::{
    bits::{contiguous_bytes_to_u64s, u64s_to_contiguous_bytes},
    native::{NativeProfile, NativePublicSetup},
};
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha20Rng;
use reinspiring::native::*;
use std::borrow::Borrow;
pub const Q: u64 = 1 << 54;
pub const Q_BITS: usize = 54;
pub const D: usize = 2048;
pub const COLS: usize = 12288;
pub const MASK_BITS: usize = 29;
pub const QUERY_BITS: usize = 49;
pub const RESPONSE_BITS: usize = 22;
pub const KEY_WORDS: usize = 2 * D;
pub const KEY_BYTES: usize = KEY_WORDS * Q_BITS / 8;
pub fn params() -> NativeParams {
    NativeParams::new(D, 54, 16, 19, 2, SecretDistribution::Gaussian)
        .expect("fixed experimental profile")
}
pub fn packing_setup() -> NativeSetup {
    NativeSetup::new(params(), crate::types::setup_seed_bytes())
}
pub fn query_masks(shard: u64) -> Vec<Vec<u64>> {
    public_query_masks(crate::protocol::setup_seed(shard), 32768, COLS)
}
/// Domain-separated first-dimension masks for a `rows` by `cols` database.
pub fn public_query_masks(seed: [u8; 32], rows: usize, cols: usize) -> Vec<Vec<u64>> {
    NativePublicSetup::new(
        NativeProfile::new(params(), rows, cols).unwrap(),
        seed,
        [0; 32],
    )
    .query_masks()
    .to_vec()
}
/// Canonical published bytes for two-mask blocks: every first mask, then
/// every second mask, each coefficient rounded to `MASK_BITS`.
pub fn public_len(cols: usize) -> usize {
    (2 * cols * MASK_BITS).div_ceil(8)
}
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
fn round(x: u64, bits: usize) -> u64 {
    (((x as u128 * (1u128 << bits) + (Q / 2) as u128) / Q as u128) as u64) & ((1 << bits) - 1)
}
pub fn response_len(cols: usize) -> usize {
    (cols * RESPONSE_BITS).div_ceil(8)
}
pub fn request_len(rows: usize) -> usize {
    KEY_BYTES + (rows * QUERY_BITS).div_ceil(8)
}
pub fn prepare(shard: u64, rows: usize, target: usize) -> Result<(NativeSecret, Vec<u8>), String> {
    if rows > 32768 {
        return Err("native query shape".into());
    }
    prepare_with(&packing_setup(), &query_masks(shard), rows, target)
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
pub fn parse(bytes: &[u8], rows: usize) -> Result<(NativeKeys, Vec<u64>), String> {
    if rows > 32768 {
        return Err("native query framing".into());
    }
    parse_with(&packing_setup(), bytes, rows)
}
pub fn parse_with(
    setup: &NativeSetup,
    bytes: &[u8],
    rows: usize,
) -> Result<(NativeKeys, Vec<u64>), String> {
    if !rows.is_multiple_of(D) || bytes.len() != request_len(rows) {
        return Err("native query framing".into());
    }
    let keys = NativeKeys::from_kg_words(
        setup,
        &contiguous_bytes_to_u64s(&bytes[..KEY_BYTES], Q_BITS),
    )
    .map_err(|e| e.to_string())?;
    let query = contiguous_bytes_to_u64s(&bytes[KEY_BYTES..], QUERY_BITS)
        .into_iter()
        .map(|x| x << (Q_BITS - QUERY_BITS))
        .collect();
    Ok((keys, query))
}
/// Decode an Enhance row, truncated to its logical width.
pub fn decode(secret: &NativeSecret, public: &[u8], body: &[u8]) -> Result<Vec<u8>, String> {
    let mut out = decode_cols(secret, public, body, COLS)?;
    out.truncate(crate::ROW_BYTES);
    Ok(out)
}
/// Decode every little-endian u16 plaintext coefficient of a two-mask response.
pub fn decode_cols(
    secret: &NativeSecret,
    public: &[u8],
    body: &[u8],
    cols: usize,
) -> Result<Vec<u8>, String> {
    if !cols.is_multiple_of(D)
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
/// Prepare one uploaded key for every block, then finish each block with its
/// scan result. Returns the switched response bodies.
pub fn pack<B: Borrow<NativePreprocessed> + Sync>(
    blocks: &[B],
    keys: &NativeKeys,
    intermediate: &[u64],
) -> Result<Vec<u8>, String> {
    use rayon::prelude::*;
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
pub fn response(bodies: &[u64]) -> Vec<u8> {
    let switched: Vec<_> = bodies.iter().map(|&x| round(x, RESPONSE_BITS)).collect();
    u64s_to_contiguous_bytes(&switched, RESPONSE_BITS)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;

    /// End-to-end over the library primitives the server uses: two-mask
    /// preprocessing, one-key upload, 29-bit masks and 22-bit responses.
    #[test]
    fn two_mask_rounded_roundtrip() {
        let rows = D;
        let cols = D;
        let mut rng = ChaCha20Rng::from_seed([7; 32]);
        let db: Vec<Vec<u16>> = (0..cols)
            .map(|_| (0..rows).map(|_| rng.gen()).collect())
            .collect();
        let masks = public_query_masks([3; 32], rows, cols);
        let lift = reinspiring::lift_ntt::LiftContext::new(D, Q).unwrap();
        let public = lift.prepare_public_dot(&masks, 65535).unwrap();
        let hint: Vec<_> = db
            .iter()
            .map(|col| {
                let poly = vec![col.iter().map(|&x| x as u64).collect::<Vec<_>>()];
                lift.public_dot(&public, &poly).unwrap()
            })
            .collect();
        let setup = NativeSetup::new(params(), [9; 32]);
        let blocks = vec![NativePreprocessed::build_two_mask(&setup, &hint).unwrap()];
        let published = publish(&blocks).unwrap();
        assert_eq!(published.len(), public_len(cols));
        assert_eq!(public_len(COLS), 89_088);
        assert_eq!(KEY_BYTES, 27_648);
        let target = 1234;
        let (secret, body) = prepare_with(&setup, &masks, rows, target).unwrap();
        assert_eq!(body.len(), request_len(rows));
        let (keys, query) = parse_with(&setup, &body, rows).unwrap();
        let scan: Vec<u64> = db
            .iter()
            .map(|col| {
                col.iter().zip(&query).fold(0u64, |a, (&x, &q)| {
                    a.wrapping_add((x as u64).wrapping_mul(q))
                }) & (Q - 1)
            })
            .collect();
        let response = pack(&blocks, &keys, &scan).unwrap();
        let row = decode_cols(&secret, &published, &response, cols).unwrap();
        let expected: Vec<u8> = db.iter().flat_map(|c| c[target].to_le_bytes()).collect();
        assert_eq!(row, expected);
    }
}
