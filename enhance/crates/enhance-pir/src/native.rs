//! Experimental native packing profile, with snapshot/request binding supplied
//! by the versioned Enhance envelope. Query masks remain stable across publications
//! so unchanged worker units can reuse their public hint contributions.
use ipir_sp::{
    bits::{contiguous_bytes_to_u64s, u64s_to_contiguous_bytes},
    native::{NativeProfile, NativePublicSetup},
};
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha20Rng;
use reinspiring::native::*;
pub const Q: u64 = 1 << 54;
pub const D: usize = 2048;
pub const COLS: usize = 12288;
pub const KEY_WORDS: usize = 2 * 2 * D;
pub const KEY_BYTES: usize = KEY_WORDS * 54 / 8;
pub fn params() -> NativeParams {
    NativeParams::new(D, 54, 16, 19, 2, SecretDistribution::Gaussian)
        .expect("fixed experimental profile")
}
pub fn packing_setup() -> NativeSetup {
    NativeSetup::new(params(), crate::types::setup_seed_bytes())
}
pub fn query_masks(shard: u64) -> Vec<Vec<u64>> {
    NativePublicSetup::new(
        NativeProfile::new(params(), 32768, COLS).unwrap(),
        crate::protocol::setup_seed(shard),
        [0; 32],
    )
    .query_masks()
    .to_vec()
}
pub fn prepare(shard: u64, rows: usize, target: usize) -> Result<(NativeSecret, Vec<u8>), String> {
    if target >= rows || rows > 32768 || rows % D != 0 {
        return Err("native query shape".into());
    }
    let mut entropy = [0; 32];
    rand::rngs::OsRng.fill_bytes(&mut entropy);
    let mut rng = ChaCha20Rng::from_seed(entropy);
    let secret = NativeSecret::sample(&params(), &mut rng);
    let setup = packing_setup();
    let keys = NativeKeys::generate(&setup, &secret, &mut rng).map_err(|e| e.to_string())?;
    let masks = query_masks(shard);
    let query = secret
        .encrypt_selection(&masks[..rows / D], target, &mut rng)
        .map_err(|e| e.to_string())?;
    let mut bytes = u64s_to_contiguous_bytes(&keys.words(), 54);
    let switched: Vec<_> = query
        .iter()
        .map(|&x| {
            (((x as u128 * (1u128 << 49) + (Q / 2) as u128) / Q as u128) as u64) & ((1 << 49) - 1)
        })
        .collect();
    bytes.extend(u64s_to_contiguous_bytes(&switched, 49));
    Ok((secret, bytes))
}
pub fn parse(bytes: &[u8], rows: usize) -> Result<(NativeKeys, Vec<u64>), String> {
    if rows > 32768 || rows % D != 0 || bytes.len() != KEY_BYTES + (rows * 49).div_ceil(8) {
        return Err("native query framing".into());
    }
    let keys = NativeKeys::from_words(
        &packing_setup(),
        &contiguous_bytes_to_u64s(&bytes[..KEY_BYTES], 54),
    )
    .map_err(|e| e.to_string())?;
    let query = contiguous_bytes_to_u64s(&bytes[KEY_BYTES..], 49)
        .into_iter()
        .map(|x| x << 5)
        .collect();
    Ok((keys, query))
}
pub fn decode(secret: &NativeSecret, public: &[u8], body: &[u8]) -> Result<Vec<u8>, String> {
    if public.len() != COLS * 8 || body.len() != COLS * 22 / 8 {
        return Err("native response shape".into());
    }
    let a: Vec<_> = public
        .chunks_exact(8)
        .map(|x| u64::from_le_bytes(x.try_into().unwrap()))
        .collect();
    if a.iter().any(|&x| x >= Q) {
        return Err("native public coefficient".into());
    }
    let b: Vec<_> = contiguous_bytes_to_u64s(body, 22)
        .into_iter()
        .map(|x| x << 32)
        .collect();
    let mut out = Vec::with_capacity(COLS * 2);
    for (a, b) in a.chunks_exact(D).zip(b.chunks_exact(D)) {
        let ct = NativeCiphertext::from_rows(&params(), a.to_vec(), b.to_vec())
            .map_err(|e| e.to_string())?;
        for x in secret.decrypt(&ct).map_err(|e| e.to_string())? {
            out.extend((x as u16).to_le_bytes());
        }
    }
    out.truncate(crate::ROW_BYTES);
    Ok(out)
}
pub fn response(bodies: &[u64]) -> Vec<u8> {
    let switched: Vec<_> = bodies
        .iter()
        .map(|&x| {
            (((x as u128 * (1u128 << 22) + (Q / 2) as u128) / Q as u128) as u64) & ((1 << 22) - 1)
        })
        .collect();
    u64s_to_contiguous_bytes(&switched, 22)
}
