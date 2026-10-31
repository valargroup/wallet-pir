use super::*;
use crate::probe::common;
use receiver_store::Config;
use zakura_pir_publisher::receiver::{Coverage, Paid, Segment as Tables};

/// Fixture coverage: heights 100 to 101, positions 200 to 300, ending at `[3; 32]`.
fn coverage() -> Coverage {
    crate::prepared::coverage(3)
}

/// Rehashes `tables`' companion after an edit to its rows.
fn rehash(tables: &mut Tables) {
    use sha2::Digest;
    tables.companion.manifest.data_sha256 = sha2::Sha256::digest(&tables.companion.data).into();
}

/// A companion built for another directory, or with a payload supplying a sibling its
/// position does not have, is refused before serving.
#[test]
fn companions_must_serve_their_directory() {
    let paid: Vec<Paid> = (0..2).map(common::paid).collect();
    let ours = crate::prepared::tables(coverage(), &paid, 2048);
    crate::prepared::segment(ours.clone()).unwrap();
    let theirs = crate::prepared::tables(coverage(), &paid[..1], 2048);
    let swapped = Tables {
        directory: ours.directory.clone(),
        companion: theirs.companion,
    };
    assert!(crate::prepared::segment(swapped).is_err());
    // Position 200's level-8 sibling, positions 0 to 255, is not complete at 300 leaves.
    let mut extra = ours;
    extra.companion.data[zakura_pir_publisher::receiver::RECORD_BYTES + 8 * 32] = 1;
    rehash(&mut extra);
    assert!(crate::prepared::segment(extra).is_err());
}

/// A payment with an invalid receiver, commitment, nullifier or ephemeral key is
/// refused once, before it is stored, so no publication can hold it.
#[test]
fn cryptographically_invalid_records_are_never_stored() {
    for field in 0..4 {
        let (mut receiver, mut payment, _) = common::paid(0);
        match field {
            0 => receiver = [255; 43],
            1 => payment.cmx = [255; 32],
            2 => payment.action_nullifier = [255; 32],
            _ => payment.ephemeral_key = [255; 32],
        }
        assert!(matches!(
            check_payment(&receiver, &payment),
            Err(Error::Malformed)
        ));
    }
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(
        dir.path().join("directory.sqlite"),
        Config {
            genesis: [1; 32],
            start_height: 100,
            start_parent: [2; 32],
            start_position: 0,
        },
    )
    .unwrap();
    let mut payment = common::record(0, 1).payment;
    (payment.height, payment.position, payment.cmx) = (100, 1, [255; 32]);
    let block = IndexedBlock {
        height: 100,
        hash: [3; 32],
        parent: [2; 32],
        start_position: 0,
        end_position: 2,
        coinbase_actions: 1,
        payments: vec![(common::receiver(), payment.clone())],
        commitments: vec![[1; 32], payment.cmx],
    };
    assert!(matches!(append(&mut store, &block), Err(Error::Malformed)));
    assert_eq!(store.tip().unwrap().height, 99);
}
