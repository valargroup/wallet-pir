use receiver_directory::{
    extract::Action,
    snapshot::{lookup_row, row_for, Manifest, Snapshot, PROFILE, ROW_BYTES},
    Error, Payment, Receiver, Record, RECORD_BYTES,
};

fn action() -> Action {
    let v: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/zero-ovk-action.json")).unwrap();
    fn field<const N: usize>(v: &serde_json::Value, key: &str) -> [u8; N] {
        hex::decode(v["action"][key].as_str().unwrap())
            .unwrap()
            .try_into()
            .unwrap()
    }
    Action {
        cv: field(&v, "cv"),
        nullifier: field(&v, "nullifier"),
        cmx: field(&v, "cmx"),
        ephemeral_key: field(&v, "ephemeralKey"),
        enc_ciphertext: field(&v, "encCiphertext"),
        out_ciphertext: field(&v, "outCiphertext"),
    }
}
fn receiver() -> Receiver {
    action().recover_receiver().unwrap().unwrap()
}
fn manifest() -> Manifest {
    Manifest {
        profile: PROFILE.into(),
        genesis: [1; 32],
        start_height: 100,
        start_parent: [2; 32],
        start_position: 200,
        end_height: 101,
        end_hash: [3; 32],
        end_position: 300,
        rows: 8,
        salt: [4; 32],
        records: 0,
        data_sha256: [0; 32],
    }
}
fn record(page: u32, total: u32) -> Record {
    Record {
        receiver: receiver(),
        page,
        total,
        payment: Payment {
            height: 101,
            block_hash: [3; 32],
            txid: [page as u8; 32],
            tx_index: page,
            action_index: 0,
            position: 200 + u64::from(page),
            action_nullifier: [5; 32],
            cmx: [6; 32],
            ephemeral_key: [7; 32],
            ciphertext_prefix: [8; 52],
        },
    }
}
fn row<'a>(s: &'a Snapshot, r: &Receiver, page: u32) -> &'a [u8] {
    let n = row_for(&s.manifest, r, page).unwrap();
    &s.data[n * ROW_BYTES..(n + 1) * ROW_BYTES]
}

#[test]
fn public_refund_requires_authenticated_recovery() {
    assert!(action().recover_receiver().unwrap().is_some());
    let mut a = action();
    a.out_ciphertext[0] ^= 1;
    assert!(a.recover_receiver().unwrap().is_none());
    let mut a = action();
    a.enc_ciphertext[579] ^= 1;
    assert!(a.recover_receiver().unwrap().is_none());
    let mut a = action();
    a.cmx = [255; 32];
    assert!(a.recover_receiver().is_err());
}

#[test]
fn wire_layout_and_strict_empty_slots() {
    let r = record(0, 1);
    let b = r.encode().unwrap();
    assert_eq!(b.len(), 285);
    assert_eq!(&b[129..137], &200u64.to_le_bytes());
    assert_eq!(Record::decode(&b).unwrap(), Some(r));
    assert!(Record::decode(&[0; RECORD_BYTES]).unwrap().is_none());
    let mut corrupt = [0; RECORD_BYTES];
    corrupt[284] = 1;
    assert!(Record::decode(&corrupt).is_err());
    assert!(Record::decode(&b[..284]).is_err());
}

#[test]
fn pages_share_one_revision_and_build_order_is_stable() {
    let records = [record(0, 2), record(1, 2)];
    let s = Snapshot::build(manifest(), &records).unwrap();
    let reversed = Snapshot::build(manifest(), &[records[1].clone(), records[0].clone()]).unwrap();
    assert_eq!(
        s.manifest.revision().unwrap(),
        reversed.manifest.revision().unwrap()
    );
    for r in &records {
        assert_eq!(
            lookup_row(
                &s.manifest,
                &r.receiver,
                r.page,
                row(&s, &r.receiver, r.page)
            )
            .unwrap(),
            Some(r.clone())
        );
    }
    let empty = Snapshot::build(manifest(), &[]).unwrap();
    assert!(lookup_row(
        &empty.manifest,
        &records[0].receiver,
        0,
        row(&empty, &records[0].receiver, 0)
    )
    .unwrap()
    .is_none());
    assert!(matches!(
        lookup_row(
            &empty.manifest,
            &records[0].receiver,
            1,
            row(&empty, &records[0].receiver, 1)
        ),
        Err(Error::MissingPage)
    ));
    assert!(Snapshot::build(manifest(), &records[..1]).is_err());
    assert!(Snapshot::build(manifest(), &records[1..]).is_err());
    assert!(Snapshot::build(manifest(), &[record(0, 1), record(1, 2)]).is_err());
    assert!(Snapshot::build(manifest(), &[record(0, 1), record(0, 1)]).is_err());
}

#[test]
fn coverage_anchor_and_position_are_required() {
    let m = manifest();
    m.accept([1; 32], 100, 101, [3; 32]).unwrap();
    for (network, start, height, hash) in [
        ([9; 32], 100, 101, [3; 32]),
        ([1; 32], 99, 101, [3; 32]),
        ([1; 32], 100, 102, [3; 32]),
        ([1; 32], 100, 101, [9; 32]),
    ] {
        assert!(m.accept(network, start, height, hash).is_err());
    }
    let mut r = record(0, 1);
    r.payment.position = 300;
    assert!(Snapshot::build(m.clone(), &[r]).is_err());
    let mut r = record(0, 1);
    r.payment.block_hash = [9; 32];
    assert!(Snapshot::build(m, &[r]).is_err());
}

#[test]
fn overflow_and_bad_padding_fail_closed() {
    let records: Vec<_> = (0..50).map(|p| record(p, 50)).collect();
    let mut m = manifest();
    m.rows = 4;
    let overflow = (0..100).any(|i| {
        m.salt[0] = i;
        matches!(Snapshot::build(m.clone(), &records), Err(Error::Capacity))
    });
    assert!(overflow);
    let s = Snapshot::build(manifest(), &[record(0, 1)]).unwrap();
    let r = receiver();
    let mut b = row(&s, &r, 0).to_vec();
    b[ROW_BYTES - 1] = 1;
    assert!(lookup_row(&s.manifest, &r, 0, &b).is_err());
}
