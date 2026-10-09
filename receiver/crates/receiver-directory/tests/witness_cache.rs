use incrementalmerkletree::frontier::CommitmentTree;
use orchard::{note::ExtractedNoteCommitment, tree::MerkleHashOrchard};
use receiver_directory::{
    snapshot::{Manifest, MIN_ROWS, PROFILE},
    witness::{WitnessCache, WitnessSnapshot},
    Hash,
};
use std::collections::BTreeSet;

/// A manifest ending at `len` commitments that promises `records` records.
fn manifest(len: usize, records: usize) -> Manifest {
    Manifest {
        profile: PROFILE.into(),
        genesis: [1; 32],
        start_height: 100,
        start_parent: [2; 32],
        start_position: 0,
        end_height: 110,
        end_hash: [3; 32],
        end_position: len as u64,
        rows: MIN_ROWS,
        salt: [3; 32],
        records: records as u64,
        data_sha256: [0; 32],
        filters: vec![receiver_directory::snapshot::FilterSet {
            label: receiver_directory::filter::PAID.into(),
            count: 0,
            window_secs: None,
            since_unix: None,
            until_unix: None,
        }],
        filters_sha256: [0; 32],
    }
}
fn leaves(len: usize) -> Vec<Hash> {
    (0..len)
        .map(|i| {
            let mut cmx = [0; 32];
            cmx[..8].copy_from_slice(&(i as u64 + 1).to_le_bytes());
            cmx
        })
        .collect()
}
/// A reused cache must match a fresh build, whose root must match an independent tree.
fn compare(cache: &mut WitnessCache, cmxs: &[Hash], positions: BTreeSet<u32>) {
    let manifest = manifest(cmxs.len(), positions.len());
    let full = WitnessSnapshot::build(&manifest, cmxs, &positions).unwrap();
    let cached = cache.build(&manifest, cmxs, &positions).unwrap();
    assert_eq!(cached.encode(), full.encode());
    let mut tree = CommitmentTree::<MerkleHashOrchard, 32>::empty();
    for cmx in cmxs {
        let cmx = ExtractedNoteCommitment::from_bytes(cmx).unwrap();
        tree.append(MerkleHashOrchard::from_cmx(&cmx)).unwrap();
    }
    assert_eq!(full.root(), tree.root().to_bytes());
    for position in positions {
        assert_eq!(
            cached.path(position, cmxs[position as usize]).unwrap(),
            full.path(position, cmxs[position as usize]).unwrap()
        );
    }
}
#[test]
fn appends_rewinds_and_forks_match_full_build_byte_for_byte() {
    let mut cache = WitnessCache::default();
    let all = leaves(130);
    // Odd pairs, power-of-two crossings, same-size forks, shorter forks and reappend.
    for len in [
        1, 2, 3, 7, 8, 9, 31, 32, 33, 64, 65, 129, 130, 129, 65, 64, 63, 1, 130,
    ] {
        compare(&mut cache, &all[..len], (0..len as u32).collect());
    }
    for (offset, len) in [(128, 130), (64, 130), (0, 130), (17, 65), (0, 1)] {
        let mut fork = all[..len].to_vec();
        fork[offset][8] = 1;
        compare(&mut cache, &fork, (0..len as u32).collect());
    }
}
#[test]
fn positions_and_manifest_are_never_cached() {
    let mut cache = WitnessCache::default();
    let cmxs = leaves(31);
    compare(&mut cache, &cmxs, [0].into_iter().collect());
    compare(&mut cache, &cmxs, [1, 19, 30].into_iter().collect());
    compare(&mut cache, &cmxs, BTreeSet::new());
    let mut m = manifest(cmxs.len(), 1);
    m.end_hash[0] ^= 1;
    m.end_height += 1;
    assert_eq!(
        cache
            .build(&m, &cmxs, &[30].into_iter().collect())
            .unwrap()
            .encode(),
        WitnessSnapshot::build(&m, &cmxs, &[30].into_iter().collect())
            .unwrap()
            .encode()
    );
}
#[test]
fn rejected_inputs_do_not_poison_later_builds() {
    let mut cache = WitnessCache::default();
    let cmxs = leaves(17);
    compare(&mut cache, &cmxs, [0, 16].into_iter().collect());
    let mut malformed = cmxs.clone();
    malformed[8] = [255; 32];
    assert!(cache
        .build(&manifest(17, 0), &malformed, &BTreeSet::new())
        .is_err());
    assert!(cache
        .build(&manifest(17, 1), &cmxs, &[17].into_iter().collect())
        .is_err());
    assert!(cache
        .build(&manifest(18, 0), &cmxs, &BTreeSet::new())
        .is_err());
    assert!(cache.build(&manifest(0, 0), &[], &BTreeSet::new()).is_err());
    compare(&mut cache, &cmxs[..9], [0, 8].into_iter().collect());
}
#[test]
fn positions_must_cover_exactly_the_manifest_records() {
    let cmxs = leaves(17);
    let two: BTreeSet<u32> = [0, 16].into_iter().collect();
    let mut cache = WitnessCache::default();
    compare(&mut cache, &cmxs[..9], [0, 8].into_iter().collect());
    // An omitted position and an extra one, through the cached and fresh entrypoints.
    for (records, positions) in [(3, &two), (1, &two)] {
        assert!(matches!(
            cache.build(&manifest(17, records), &cmxs, positions),
            Err(receiver_directory::Error::Coverage)
        ));
        assert!(matches!(
            WitnessSnapshot::build(&manifest(17, records), &cmxs, positions),
            Err(receiver_directory::Error::Coverage)
        ));
    }
    // The rejections left the primed cache usable.
    compare(&mut cache, &cmxs, two);
}
