use super::*;

#[test]
fn shard_sum_wraps_without_u64_overflow() {
    let q = u64::MAX;
    let mut sum = [q - 1, 0, 1];
    add_intermediate_assign_mod(&mut sum, &[q - 1, q - 1, q - 1], q).unwrap();
    assert_eq!(sum, [q - 2, q - 1, 0]);
}

#[test]
fn unequal_shards_match_monolithic_hints_and_answers() {
    // One column block and three row blocks are sufficient to exercise
    // unequal shards, a nonzero setup offset, and zero-padded source rows.
    // The deployed six-column Engine/composed-tail oracle runs in
    // runtime::tests::partial_preparation_failure_preserves_published_and_retries.
    let layout = DatabaseLayout {
        record_bytes: 4096,
        records_per_row: 1,
        shard_rows: 6144,
        pir_profile: ipir_sp::SimplePirProfile::P16Q48,
    };
    let (rlwe, params) = shard_parameters(&layout).unwrap();
    assert_eq!(params.instances, 1);
    assert_eq!(params.db_rows, 6144);
    let mut rows = vec![0; layout.shard_bytes()];
    for (index, row) in rows
        .chunks_exact_mut(layout.row_bytes())
        .take(4097)
        .enumerate()
    {
        for (column, word) in row.chunks_exact_mut(2).enumerate() {
            let value = ((index * 17 + column * 31 + 1) % 65536) as u16;
            word.copy_from_slice(&value.to_le_bytes());
        }
    }
    let seed = DatabaseId::Enhance.setup_seed_bytes();
    #[cfg(feature = "native-reinspiring")]
    let setup = crate::native::public_query_masks(seed, params.db_rows, params.db_cols);
    #[cfg(not(feature = "native-reinspiring"))]
    let setup = ipir_sp::IPIRClient::from_profile(
        params.num_items,
        params.item_size_bits,
        layout.pir_profile,
    )
    .unwrap()
    .generate_public_query_setup_simplepir_from_seed(seed)
    .polys()
    .to_vec();
    let monolithic =
        PreparedShard::build(&layout, 0, 0, "whole".into(), &rows, &rlwe, &setup).unwrap();
    let shards: Vec<_> = [(0, 2048), (2048, 4096)]
        .into_iter()
        .map(|(start, count)| {
            PreparedShard::build(
                &DatabaseLayout {
                    shard_rows: count,
                    ..layout
                },
                0,
                start,
                "part".into(),
                &rows[start * layout.row_bytes()..(start + count) * layout.row_bytes()],
                &rlwe,
                &setup,
            )
            .unwrap()
        })
        .collect();
    let mut hint = shards[0].crs_blocks().to_vec();
    add_crs_blocks_assign_mod(&mut hint, shards[1].crs_blocks(), &rlwe).unwrap();
    assert_eq!(hint, monolithic.crs_blocks());
    let modulus = crate::runtime::modulus();
    for target in [0, 2047, 2048, 4096, 4097, 6143] {
        let mut query = vec![0; params.db_rows];
        query[target] = modulus - 1;
        let mut combined = vec![0; params.db_cols];
        for shard in &shards {
            let runtime = shard.runtime();
            let start = runtime.query_row_start;
            let end = start + runtime.server.params().db_rows;
            let answer = runtime.evaluate(&rlwe, &query[start..end]).unwrap();
            add_intermediate_assign_mod(&mut combined, &answer, modulus).unwrap();
        }
        assert_eq!(
            combined,
            monolithic.runtime().evaluate(&rlwe, &query).unwrap(),
            "query row {target}"
        );
    }
}

#[test]
fn shard_combiners_reject_malformed_contributions() {
    let (rlwe, _) = shard_parameters(&crate::types::ENHANCE_LAYOUT).unwrap();
    assert!(add_intermediate_assign_mod(&mut [0; 8], &[0; 7], rlwe.q).is_err());
    assert!(add_intermediate_assign_mod(&mut [0], &[rlwe.q], rlwe.q).is_err());
    assert!(add_intermediate_assign_mod(&mut [rlwe.q], &[0], rlwe.q).is_err());
    assert!(add_intermediate_assign_mod(&mut [0], &[0], 1).is_err());
    let mut malformed = [CrsBlock { rows: vec![] }];
    assert!(add_crs_blocks_assign_mod(&mut malformed, &[], &rlwe).is_err());
    assert!(
        add_crs_blocks_assign_mod(&mut malformed, &[CrsBlock { rows: vec![] }], &rlwe).is_err()
    );
    let malformed_row = CrsBlock {
        rows: vec![vec![]; rlwe.d],
    };
    assert!(
        add_crs_blocks_assign_mod(&mut [malformed_row.clone()], &[malformed_row], &rlwe).is_err()
    );
}

/// Every served layout, with the instance count its rows need. One instance
/// carries d * log2(p) plaintext bits.
const LAYOUTS: &[(&str, DatabaseLayout, usize)] = &[("enhance", crate::types::ENHANCE_LAYOUT, 6)];

#[test]
fn every_layout_has_the_expected_instance_count() {
    for (name, layout, instances) in LAYOUTS {
        let (rlwe, shard) = shard_parameters(layout).expect("shard params");
        let (global_rlwe, global) = global_parameters(
            layout.logical_rows_for(layout.shard_rows as u64 * 4),
            layout,
        )
        .expect("global params");
        assert_eq!(rlwe.d, 2_048, "{name}");
        assert_eq!(shard.p, 1 << 16, "{name}");
        assert_eq!(shard.query_bits, 48, "{name}");
        assert_eq!(shard.instances, *instances, "{name}");
        assert_eq!(shard.db_cols, instances * rlwe.d, "{name}");
        // Shard and global parameters must agree on everything but row count,
        // or partials from shards could not be summed into the global answer.
        assert_eq!((global_rlwe.d, global_rlwe.q), (rlwe.d, rlwe.q), "{name}");
        assert_eq!(global.db_cols, shard.db_cols, "{name}");
        assert!(layout.shard_rows.is_multiple_of(rlwe.d), "{name}");
    }
}

#[test]
fn enhance_rows_use_six_ipir_instances() {
    // A 21,549-byte suffix row is 172,392 bits. It still requires six
    // 32,768-bit instances; compact storage does not reduce PIR dimensions.
    assert_eq!(crate::types::ENHANCE_LAYOUT.row_bytes(), 21_549);
    let (_, params) = shard_parameters(&crate::types::ENHANCE_LAYOUT).expect("params");
    assert_eq!(params.instances, 6);
    assert_eq!(params.db_cols, 12_288);
    assert_eq!(params.p, 1 << 16);
    assert_eq!(params.query_bits, 48);
}

#[test]
fn sixteen_bit_row_encoding_preserves_the_odd_tail_byte_and_zero_padding() {
    let layout = crate::types::ENHANCE_LAYOUT;
    let (_, params) = shard_parameters(&layout).expect("params");
    let mut row = vec![0_u8; layout.row_bytes()];
    row[0] = 0x34;
    row[1] = 0x12;
    *row.last_mut().expect("nonempty row") = 0xab;

    let mut coefficients = RowPlaintextIter::new(
        &row,
        layout.row_bytes(),
        2,
        params.db_cols,
        layout.pir_profile.plaintext_bits(),
    );
    assert_eq!(coefficients.next(), Some(0x1234));
    assert_eq!(coefficients.nth(layout.row_bytes() / 2 - 1), Some(0x00ab));
    assert_eq!(coefficients.next(), Some(0));
    assert_eq!(
        coefficients.nth(params.db_cols - layout.row_bytes().div_ceil(2) - 1),
        Some(0),
        "the padded second row is zero"
    );
}
