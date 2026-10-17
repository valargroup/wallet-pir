//! Query packing keys and response packing.

use inspiring::{InspiringError, QueryPackPreprocessed, RlweCiphertext, RlweParams, TopKeyImages};

/// A query's decoded packing keys. Decoding validates every coefficient, so a
/// request can be refused before any evaluation work is sent for it.
pub struct PackingKeys<'a> {
    keys: inspiring::PackingKeys<'a>,
}

impl<'a> PackingKeys<'a> {
    /// The encoded length of one query's packing keys under `rlwe`.
    pub fn encoded_len(rlwe: &RlweParams) -> usize {
        ipir_sp::serialize::serialized_packing_keys_len(rlwe)
    }

    /// Decode exactly [`Self::encoded_len`] bytes.
    pub fn decode(rlwe: &'a RlweParams, bytes: &[u8]) -> Result<Self, InspiringError> {
        ipir_sp::serialize::deserialize_packing_keys(rlwe, bytes).map(|keys| Self { keys })
    }

    /// The upstream keys, for packing material this crate does not own, such
    /// as a mapped prepared artifact.
    pub fn upstream(&self) -> &inspiring::PackingKeys<'a> {
        &self.keys
    }
}

/// Pack a complete intermediate with owned packing material and serialize the
/// response bodies at `q_prime_1`.
pub fn pack<'a>(
    intermediate: &[u64],
    keys: &PackingKeys<'a>,
    top: &TopKeyImages<'a>,
    preprocessed: &'a [QueryPackPreprocessed<'a>],
    q_prime_1: u64,
) -> Result<Vec<u8>, InspiringError> {
    let packed =
        ipir_sp::server::pack_intermediate_blocks(intermediate, &keys.keys, top, preprocessed)?;
    Ok(response_bodies(&packed, q_prime_1))
}

/// Serialize packed ciphertexts, modulus-switched to `q_prime_1`.
pub fn response_bodies(packed: &[RlweCiphertext<'_>], q_prime_1: u64) -> Vec<u8> {
    ipir_sp::modulus_switch::serialize_rlwe_response_bodies(packed, q_prime_1)
}
