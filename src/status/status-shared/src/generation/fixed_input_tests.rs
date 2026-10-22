use super::*;
use crate::fixture;

/// Deterministic bytes for fixed-input regression vectors.
fn stream(label: &[u8], len: usize) -> Vec<u8> {
    (0u64..)
        .flat_map(|i| {
            Sha256::new()
                .chain_update(label)
                .chain_update(i.to_le_bytes())
                .finalize()
        })
        .take(len)
        .collect()
}

/// Pins public material, the hint and the packed answer for a fixed
/// snapshot and fixed query bytes. The digests were captured on the base
/// commit before the shared runtime extraction; they must never change.
#[test]
fn fixed_inputs_pin_public_material_and_answer() {
    let snapshot = fixture::snapshot(32, false).unwrap();
    let (generation, _) =
        Generation::prepare(&snapshot, 1, 1, 1, None, MatvecBackend::Cpu).unwrap();
    let p = profile();
    let q = p.rlwe().q;
    let keys_len = ipir_sp::serialize::serialized_packing_keys_len(p.rlwe());
    let keys: Vec<u64> = stream(b"keys", keys_len * 2)
        .chunks_exact(8)
        .map(|c| u64::from_le_bytes(c.try_into().unwrap()) % q)
        .collect();
    let keys =
        ipir_sp::bits::u64s_to_contiguous_bytes(&keys, ipir_sp::modulus_switch::modulus_bits(q));
    let mut body = QUERY_MAGIC.to_vec();
    body.extend(generation.manifest().id());
    body.resize(HEADER_BYTES, 9);
    body.extend(&keys[..keys_len]);
    body.extend(stream(b"query", ROWS * 48 / 8));
    let coefficients = generation.coefficients(&body).unwrap();
    let answer = generation
        .pack(&body, &generation.evaluate(&coefficients).unwrap())
        .unwrap();
    assert_eq!(
        [
            hex::encode(Sha256::digest(generation.hint_bytes().unwrap())),
            hex::encode(Sha256::digest(generation.public().as_slice())),
            hex::encode(Sha256::digest(&answer)),
        ],
        [
            "ef108747cdf42c14c9c325f095a8f1c83daf12646fc6bf29309c8a0ced551967",
            "239f00cb0991ec663dc0dd57fa78b2944d0a766a222e7f666b356915652bc7b0",
            "1d34ae677cedc62f9be5dacb537ccc6311d212020c6a29663de05065eda5ea43",
        ]
    );
}
