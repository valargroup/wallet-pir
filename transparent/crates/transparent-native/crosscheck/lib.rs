//! Cross-checks `transparent-native` against `enhance_pir::native`.
//!
//! Transparent adapts the Enhance helper instead of depending on it (see the
//! `transparent-native` crate docs). These tests hold the two to the same
//! bytes for the same inputs: parameters, query masks, published masks,
//! request framing in both directions, packed responses and decoding.

#[cfg(test)]
mod tests {
    use enhance_pir::native as enhance;
    use rand::RngCore;
    use rand_chacha::{rand_core::SeedableRng, ChaCha20Rng};
    use reinspiring::native::{NativePreprocessed, NativeSetup};
    use transparent_native as transparent;

    const D: usize = 2048;

    #[test]
    fn constants_and_parameters_agree() {
        assert_eq!(transparent::Q, enhance::Q);
        assert_eq!(transparent::D, enhance::D);
        assert_eq!(transparent::MASK_BITS, enhance::MASK_BITS);
        assert_eq!(transparent::QUERY_BITS, enhance::QUERY_BITS);
        assert_eq!(transparent::RESPONSE_BITS, enhance::RESPONSE_BITS);
        assert_eq!(transparent::KEY_BYTES, enhance::KEY_BYTES);
        assert_eq!(
            transparent::params().encoding(),
            enhance::params().encoding()
        );
        for rows in [2_048, 4_096, 8_192, 32_768, 65_536] {
            assert_eq!(transparent::request_len(rows), enhance::request_len(rows));
        }
        for cols in [D, 6 * D] {
            assert_eq!(transparent::public_len(cols), enhance::public_len(cols));
            assert_eq!(transparent::response_len(cols), enhance::response_len(cols));
        }
    }

    #[test]
    fn framing_is_byte_identical() {
        for (rows, seed) in [(2_048usize, 3u8), (4_096, 5)] {
            let cols = D;
            let masks = transparent::public_query_masks([seed; 32], rows, cols).unwrap();
            assert_eq!(masks, enhance::public_query_masks([seed; 32], rows, cols));
            let mut rng = ChaCha20Rng::from_seed([seed ^ 0x55; 32]);
            let db: Vec<u16> = (0..rows * cols).map(|_| rng.next_u32() as u16).collect();
            let hint =
                transparent::hint(&masks, rows, cols, |c| &db[c * rows..(c + 1) * rows]).unwrap();
            let setup = NativeSetup::new(transparent::params(), [seed.wrapping_add(1); 32]);
            let blocks: Vec<NativePreprocessed> = transparent::preprocess(&setup, &hint).unwrap();
            let public = transparent::publish(&blocks).unwrap();
            assert_eq!(public, enhance::publish(&blocks).unwrap());

            let target = rows - 7;
            // A Transparent upload parses identically under the Enhance helper,
            // and an Enhance upload under the Transparent one.
            let (secret, upload) = transparent::prepare_with(&setup, &masks, rows, target).unwrap();
            let (ours_keys, ours_query) = transparent::parse_with(&setup, &upload, rows).unwrap();
            let (theirs_keys, theirs_query) = enhance::parse_with(&setup, &upload, rows).unwrap();
            assert_eq!(ours_query, theirs_query);
            assert_eq!(ours_keys.kg_words(), theirs_keys.kg_words());
            let (_, enhance_upload) = enhance::prepare_with(&setup, &masks, rows, target).unwrap();
            assert_eq!(enhance_upload.len(), upload.len());
            let (_, query) = transparent::parse_with(&setup, &enhance_upload, rows).unwrap();
            assert_eq!(
                query,
                enhance::parse_with(&setup, &enhance_upload, rows)
                    .unwrap()
                    .1
            );

            let scan: Vec<u64> = (0..cols)
                .map(|c| {
                    db[c * rows..(c + 1) * rows]
                        .iter()
                        .zip(&ours_query)
                        .fold(0u64, |a, (&x, &q)| {
                            a.wrapping_add((x as u64).wrapping_mul(q))
                        })
                        & (transparent::Q - 1)
                })
                .collect();
            let response = transparent::pack(&blocks, &ours_keys, &scan).unwrap();
            assert_eq!(
                response,
                enhance::pack(&blocks, &theirs_keys, &scan).unwrap()
            );
            let decoded = transparent::decode_cols(&secret, &public, &response, cols).unwrap();
            assert_eq!(
                decoded,
                enhance::decode_cols(&secret, &public, &response, cols).unwrap()
            );
            let expected: Vec<u8> = (0..cols)
                .flat_map(|c| db[c * rows + target].to_le_bytes())
                .collect();
            assert_eq!(decoded, expected);
        }
    }
}
