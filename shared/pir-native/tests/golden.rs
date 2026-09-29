//! Byte-for-byte pins for the shared native helpers.
//!
//! The digests were produced on 2026-09-29 by the two copies this crate
//! replaced, `enhance_pir::native` and `transparent_native`, which agreed on
//! every value (source revision 8448d274). A change here changes what Enhance,
//! Status and Transparent servers publish or answer, and what wallets send.

use ipir_sp::bits::u64s_to_contiguous_bytes;
use pir_native::*;
use rand::RngCore;
use rand_chacha::{rand_core::SeedableRng, ChaCha20Rng};
use sha2::{Digest, Sha256};

fn digest(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn digest_words(words: &[u64]) -> String {
    digest(
        &words
            .iter()
            .flat_map(|w| w.to_le_bytes())
            .collect::<Vec<_>>(),
    )
}

/// `prepare_with`'s framing over a seeded secret, so the upload is fixed.
fn upload(
    setup: &NativeSetup,
    masks: &[Vec<u64>],
    rows: usize,
    target: usize,
) -> (NativeSecret, Vec<u8>) {
    let round = |x: u64, bits: usize| {
        (((x as u128 * (1u128 << bits) + (Q / 2) as u128) / Q as u128) as u64) & ((1 << bits) - 1)
    };
    let mut rng = ChaCha20Rng::from_seed([14; 32]);
    let secret = NativeSecret::sample(&params(), &mut rng);
    let keys = NativeKeys::generate_one_key(setup, &secret, &mut rng).unwrap();
    let query = secret
        .encrypt_selection(&masks[..rows / D], target, &mut rng)
        .unwrap();
    let mut bytes = u64s_to_contiguous_bytes(&keys.kg_words(), Q_BITS);
    let switched: Vec<_> = query.iter().map(|&x| round(x, QUERY_BITS)).collect();
    bytes.extend(u64s_to_contiguous_bytes(&switched, QUERY_BITS));
    (secret, bytes)
}

#[test]
fn helpers_reproduce_the_replaced_copies() {
    let (rows, cols, target) = (4_096usize, 2 * D, 4_089usize);
    let masks = public_query_masks([11; 32], rows, cols).unwrap();
    let mut rng = ChaCha20Rng::from_seed([12; 32]);
    let db: Vec<u16> = (0..rows * cols).map(|_| rng.next_u32() as u16).collect();
    let hint = hint(&masks, rows, cols, |c| &db[c * rows..(c + 1) * rows]).unwrap();
    let setup = NativeSetup::new(params(), [13; 32]);
    let blocks = preprocess(&setup, &hint).unwrap();
    let public = publish(&blocks).unwrap();
    let (secret, upload) = upload(&setup, &masks, rows, target);
    assert_eq!(upload.len(), request_len(rows));
    let (keys, query) = parse_with(&setup, &upload, rows).unwrap();
    let scan: Vec<u64> = (0..cols)
        .map(|c| {
            db[c * rows..(c + 1) * rows]
                .iter()
                .zip(&query)
                .fold(0u64, |a, (&x, &q)| {
                    a.wrapping_add((x as u64).wrapping_mul(q))
                })
                & (Q - 1)
        })
        .collect();
    let response = pack(&blocks, &keys, &scan).unwrap();
    let decoded = decode_cols(&secret, &public, &response, cols).unwrap();
    let expected: Vec<u8> = (0..cols)
        .flat_map(|c| db[c * rows + target].to_le_bytes())
        .collect();
    assert_eq!(decoded, expected);

    let masks: Vec<u64> = masks.into_iter().flatten().collect();
    let hint: Vec<u64> = hint.into_iter().flatten().flatten().collect();
    let got = [
        ("masks", digest_words(&masks)),
        ("hint", digest_words(&hint)),
        ("public", digest(&public)),
        ("upload", digest(&upload)),
        ("query", digest_words(&query)),
        ("scan", digest_words(&scan)),
        ("response", digest(&response)),
        ("decoded", digest(&decoded)),
    ];
    let want = [
        (
            "masks",
            "aef3721ff659ceaa1f7ed7276319fd40cc7fe629881f66fe2ae9c951cf1a59aa",
        ),
        (
            "hint",
            "21f8d5421240f1a20c9ceb343255320127f47d1d45db378cc9c111860bd3a137",
        ),
        (
            "public",
            "155f977c9e4517aaca352a8f0cbee05f01ce3c62e8c8024feb51338314efe960",
        ),
        (
            "upload",
            "103331c5c9a85d22403881eb67237c3c5c24d90d9afee0f735630aa1f1a82eae",
        ),
        (
            "query",
            "00689956544bf2b717e57cd11b35d01a27d67c6d65648bcf8effc5eb86dfbc17",
        ),
        (
            "scan",
            "a0836382fface222d434aa29d20f39a26c14f4f66c322539b961530e17a54021",
        ),
        (
            "response",
            "c5f48bce370133e159b2a81de3396619017a25ace7c9872a8a3b0e08bcd18241",
        ),
        (
            "decoded",
            "3069d9e5e6f4c406ce30d7a2425a4d6fee38b123ef5206bdf0c13ca3aba66361",
        ),
    ];
    for ((name, got), (_, want)) in got.iter().zip(want) {
        assert_eq!(got, want, "{name}");
    }
}

#[test]
fn sizes_are_the_deployed_profile() {
    assert_eq!(KEY_BYTES, 27_648);
    assert_eq!(public_len(12_288), 89_088);
    assert_eq!(request_len(8_192), 27_648 + 50_176);
    assert_eq!(response_len(D), BLOCK_RESPONSE_BYTES);
}
