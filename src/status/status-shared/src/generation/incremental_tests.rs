use super::*;
#[cfg(feature = "qualification-tests")]
use crate::{fixture, now_ms};
#[cfg(feature = "qualification-tests")]
#[test]
fn sparse_hint_matches_full_reconstruction_for_signed_negacyclic_boundaries() {
    let mut snapshot = fixture::snapshot(32, false).unwrap();
    let (base, _) = Generation::prepare_worker(&snapshot, 1, 1, now_ms(), None).unwrap();
    let locations = [
        0,
        ROW_BYTES - 2,
        (profile().rlwe().d - 1) * ROW_BYTES,
        ROWS * ROW_BYTES - 2,
    ];
    let original: Vec<_> = locations
        .iter()
        .map(|i| [snapshot.rows[*i], snapshot.rows[*i + 1]])
        .collect();
    for i in locations {
        snapshot.rows[i..i + 2].copy_from_slice(&u16::MAX.to_le_bytes());
    }
    snapshot.digest = Sha256::digest(&snapshot.rows).into();
    let (changed, stats) =
        Generation::prepare_worker(&snapshot, 2, 1, now_ms(), Some(&base)).unwrap();
    assert_eq!(stats.incremental_units, 2);
    changed.verify_full_hint(&snapshot).unwrap();
    for (i, bytes) in locations.into_iter().zip(original) {
        snapshot.rows[i..i + 2].copy_from_slice(&bytes);
    }
    snapshot.digest = Sha256::digest(&snapshot.rows).into();
    let (restored, stats) =
        Generation::prepare_worker(&snapshot, 3, 1, now_ms(), Some(&changed)).unwrap();
    assert_eq!(stats.incremental_units, 2);
    restored.verify_full_hint(&snapshot).unwrap();
    assert_eq!(base.hint_bytes().unwrap(), restored.hint_bytes().unwrap());
}

/// Dense schoolbook convolution, followed by X^d = -1 reduction. This oracle
/// does not share the sparse updater's rotations or signed-delta calculation.
fn dense_hint(rows: &[u8], setup: &[u64], shape: HintShape) -> Vec<CrsBlock> {
    let d = shape.degree;
    let q = u128::from(shape.modulus);
    (0..shape.columns / d)
        .map(|block| CrsBlock {
            rows: (0..d)
                .map(|column| {
                    let mut convolution = vec![0u128; 2 * d];
                    for row in 0..d {
                        let at = 2 * (row * shape.columns + block * d + column);
                        let coefficient = u16::from_le_bytes(rows[at..at + 2].try_into().unwrap());
                        for (j, mask) in setup.iter().enumerate() {
                            convolution[row + j] += u128::from(coefficient) * u128::from(*mask);
                        }
                    }
                    (0..d)
                        .map(|i| ((convolution[i] % q + q - convolution[i + d] % q) % q) as u64)
                        .collect()
                })
                .collect(),
        })
        .collect()
}

#[test]
fn sparse_update_matches_independent_dense_convolution() {
    for q in [profile().rlwe().q, 1u64 << 54] {
        let shape = HintShape {
            degree: 8,
            columns: 16,
            modulus: q,
        };
        let setup: Vec<_> = (0..shape.degree).map(|i| q - 1 - i as u64 * 13).collect();
        let rows: Vec<_> = (0..shape.degree * shape.columns)
            .flat_map(|i| ((i * 17 + 3) as u16).to_le_bytes())
            .collect();
        let before = dense_hint(&rows, &setup, shape);
        let mut changed = rows.clone();
        for (row, column) in [(0, 0), (7, 7), (0, 8), (7, 15)] {
            let at = 2 * (row * shape.columns + column);
            changed[at..at + 2].copy_from_slice(&u16::MAX.to_le_bytes());
        }
        let updated = sparse_hint_update(&rows, &before, &changed, &setup, shape).unwrap();
        assert_eq!(updated, dense_hint(&changed, &setup, shape));
        let restored = sparse_hint_update(&changed, &updated, &rows, &setup, shape).unwrap();
        assert_eq!(
            restored, before,
            "inverse changes restore the original hint"
        );
        assert_eq!(
            sparse_hint_update(&rows, &before, &rows, &setup, shape).unwrap(),
            before
        );
        assert!(
            sparse_hint_update(&rows, &before, &changed[..changed.len() - 2], &setup, shape)
                .is_none()
        );

        // Exercise both sides of the production sparse-update cutoff without
        // allocating or preprocessing a deployed database.
        let wide = HintShape {
            columns: 1024,
            ..shape
        };
        let rows = vec![0; wide.degree * wide.columns * 2];
        let hint = dense_hint(&rows, &setup, wide);
        let mut changed = rows.clone();
        changed[..4096 * 2].fill(0xff);
        assert_eq!(
            sparse_hint_update(&rows, &hint, &changed, &setup, wide).unwrap(),
            dense_hint(&changed, &setup, wide)
        );
        changed[4096 * 2..4097 * 2].fill(0xff);
        assert!(sparse_hint_update(&rows, &hint, &changed, &setup, wide).is_none());
    }
}
