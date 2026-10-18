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

/// Public-mask images reused across prepared packing blocks.
#[derive(Clone)]
pub struct PackingSetup<'a> {
    top: std::sync::Arc<TopKeyImages<'a>>,
}

impl<'a> PackingSetup<'a> {
    pub fn new(rlwe: &'a RlweParams) -> Self {
        Self {
            top: std::sync::Arc::new(TopKeyImages::build(rlwe)),
        }
    }
}

/// Owned immutable packing material, separate from the scan database. Routers
/// can prepare it from hints without receiving the database or query selection.
pub struct PreparedPacking<'a> {
    setup: PackingSetup<'a>,
    blocks: Vec<QueryPackPreprocessed<'a>>,
}

impl<'a> PreparedPacking<'a> {
    /// Prepare hint blocks, optionally copying unchanged blocks from a previous
    /// candidate. The product owns the digest comparison in `unchanged`.
    pub fn prepare(
        rlwe: &'a RlweParams,
        hints: &[ipir_sp::server::CrsBlock],
        setup: PackingSetup<'a>,
        previous: Option<&Self>,
        unchanged: impl Fn(usize) -> bool + Sync,
    ) -> Result<Self, InspiringError> {
        use rayon::prelude::*;
        setup.top.validate(rlwe)?;
        // Only reuse under the same immutable public-mask images.
        let previous = previous.filter(|old| std::sync::Arc::ptr_eq(&setup.top, &old.setup.top));
        let blocks = hints
            .par_iter()
            .enumerate()
            .map(|(i, hint)| {
                if let Some(old) = previous
                    .filter(|_| unchanged(i))
                    .and_then(|p| p.blocks.get(i))
                {
                    return Ok(QueryPackPreprocessed {
                        params: old.params,
                        collapse_a_final_ntt: old.collapse_a_final_ntt.clone(),
                        digits_ntt: old.digits_ntt.clone(),
                    });
                }
                QueryPackPreprocessed::build_with_top(rlwe, &hint.to_ntt(rlwe), &setup.top)
            })
            .collect::<Result<_, _>>()?;
        Ok(Self { setup, blocks })
    }

    pub fn public_rows(&self, modulus: u64) -> Vec<u8> {
        ipir_sp::server::published_c1_rows(&self.blocks, modulus)
    }

    pub fn coefficient_bytes(&self) -> usize {
        self.blocks
            .iter()
            .map(|block| {
                (block.collapse_a_final_ntt.data.len()
                    + block.digits_ntt.iter().map(|m| m.data.len()).sum::<usize>())
                    * 8
            })
            .sum()
    }

    /// Preserve the upstream prepared artifact format verbatim.
    pub fn write(&self, out: &mut impl std::io::Write, rlwe: &RlweParams) -> std::io::Result<()> {
        inspiring::prepared::write(out, rlwe, &self.blocks, &self.setup.top)
    }

    /// Pack a complete intermediate and serialize its response bodies.
    pub fn pack(
        &self,
        intermediate: &[u64],
        keys: &PackingKeys<'a>,
        q_prime_1: u64,
    ) -> Result<Vec<u8>, InspiringError> {
        let packed = ipir_sp::server::pack_intermediate_blocks(
            intermediate,
            &keys.keys,
            &self.setup.top,
            &self.blocks,
        )?;
        Ok(response_bodies(&packed, q_prime_1))
    }
}

/// Serialize packed ciphertexts, modulus-switched to `q_prime_1`.
pub fn response_bodies(packed: &[RlweCiphertext<'_>], q_prime_1: u64) -> Vec<u8> {
    ipir_sp::modulus_switch::serialize_rlwe_response_bodies(packed, q_prime_1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reuse_matches_fresh_material_and_preserves_prepared_bytes() {
        let rlwe = RlweParams::new(
            8,
            12289,
            4,
            3.2,
            inspiring::GadgetParams {
                bits_per: 3,
                ell: 5,
            },
        )
        .unwrap();
        let setup = PackingSetup::new(&rlwe);
        let mut hints = vec![ipir_sp::server::CrsBlock {
            rows: vec![vec![1; 8]; 8],
        }];
        let first =
            PreparedPacking::prepare(&rlwe, &hints, setup.clone(), None, |_| false).unwrap();
        let reused =
            PreparedPacking::prepare(&rlwe, &hints, setup.clone(), Some(&first), |_| true).unwrap();
        let mut first_bytes = Vec::new();
        let mut reused_bytes = Vec::new();
        first.write(&mut first_bytes, &rlwe).unwrap();
        // Preserve the reference preprocessing and persisted bytes Enhance
        // used before extraction, independently of the reusable-image path.
        let reference = ipir_sp::server::build_pack_preprocessed_blocks(&rlwe, &hints).unwrap();
        let mut reference_bytes = Vec::new();
        inspiring::prepared::write(&mut reference_bytes, &rlwe, &reference, &first.setup.top)
            .unwrap();
        assert_eq!(first_bytes, reference_bytes);
        reused.write(&mut reused_bytes, &rlwe).unwrap();
        assert_eq!(first_bytes, reused_bytes);
        hints[0].rows[7][7] = 2;
        let changed =
            PreparedPacking::prepare(&rlwe, &hints, setup.clone(), Some(&first), |_| false)
                .unwrap();
        let fresh = PreparedPacking::prepare(&rlwe, &hints, setup, None, |_| false).unwrap();
        assert_eq!(changed.public_rows(rlwe.q), fresh.public_rows(rlwe.q));
        let bytes = vec![0; PackingKeys::encoded_len(&rlwe)];
        let keys = PackingKeys::decode(&rlwe, &bytes).unwrap();
        assert_eq!(
            changed.pack(&[3; 8], &keys, 256).unwrap(),
            fresh.pack(&[3; 8], &keys, 256).unwrap()
        );
        assert!(changed.pack(&[3; 7], &keys, 256).is_err());
        assert!(PackingKeys::decode(&rlwe, &bytes[..bytes.len() - 1]).is_err());
        assert!(PackingKeys::decode(&rlwe, &vec![0xff; bytes.len()]).is_err());
    }
}
