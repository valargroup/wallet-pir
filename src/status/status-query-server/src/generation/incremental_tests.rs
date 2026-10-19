use super::*;
use crate::{fixture, now_ms};
#[test]
fn sparse_hint_matches_full_reconstruction_for_signed_negacyclic_boundaries() {
    let mut snapshot = fixture::snapshot(32, false).unwrap();
    let (base, _) =
        Generation::prepare_worker(&snapshot, 1, 1, now_ms(), None, MatvecBackend::Cpu).unwrap();
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
        Generation::prepare_worker(&snapshot, 2, 1, now_ms(), Some(&base), MatvecBackend::Cpu)
            .unwrap();
    assert_eq!(stats.incremental_units, 2);
    changed.verify_full_hint(&snapshot).unwrap();
    for (i, bytes) in locations.into_iter().zip(original) {
        snapshot.rows[i..i + 2].copy_from_slice(&bytes);
    }
    snapshot.digest = Sha256::digest(&snapshot.rows).into();
    let (restored, stats) = Generation::prepare_worker(
        &snapshot,
        3,
        1,
        now_ms(),
        Some(&changed),
        MatvecBackend::Cpu,
    )
    .unwrap();
    assert_eq!(stats.incremental_units, 2);
    restored.verify_full_hint(&snapshot).unwrap();
    assert_eq!(base.hint_bytes().unwrap(), restored.hint_bytes().unwrap());
}
