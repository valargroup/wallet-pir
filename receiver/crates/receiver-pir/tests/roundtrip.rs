#![cfg(feature = "server")]
use receiver_directory::{
    extract::Action,
    snapshot::{Manifest, Snapshot, PROFILE},
    Payment, Receiver, Record,
};
use receiver_pir::{server::Server, AcceptedCoverage, Client, Error, ROWS};

fn receiver() -> Receiver {
    let v: serde_json::Value = serde_json::from_str(include_str!(
        "../../receiver-directory/tests/fixtures/zero-ovk-action.json"
    ))
    .unwrap();
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
    .recover_receiver()
    .unwrap()
    .unwrap()
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
        rows: ROWS as u32,
        salt: [4; 32],
        records: 0,
        data_sha256: [0; 32],
    }
}
fn accepted() -> AcceptedCoverage {
    AcceptedCoverage {
        genesis: [1; 32],
        required_start: 100,
        height: 101,
        hash: [3; 32],
    }
}
fn record(page: u32) -> Record {
    Record {
        receiver: receiver(),
        page,
        total: 2,
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

#[test]
fn encrypted_publication_roundtrip_and_fail_closed() {
    let records = [record(0), record(1)];
    let server = Server::new(Snapshot::build(manifest(), &records).unwrap()).unwrap();
    let client = Client::new(server.manifest().clone(), server.public(), accepted()).unwrap();
    let first = client.prepare(receiver(), 0).unwrap();
    let second = client.prepare(receiver(), 0).unwrap();
    assert_ne!(
        first.body(),
        second.body(),
        "fresh encryption even for the same receiver"
    );
    assert_eq!(first.body().len(), receiver_pir::query_bytes());
    let answer = server.respond(first.body()).unwrap();
    assert_eq!(answer.len(), receiver_pir::response_bytes());
    assert!(
        client.decode(second, &answer).is_err(),
        "reject another request's answer"
    );
    assert_eq!(
        client.decode(first, &answer).unwrap(),
        Some(records[0].clone())
    );
    let q = client.prepare(receiver(), 1).unwrap();
    let a = server.respond(q.body()).unwrap();
    assert_eq!(client.decode(q, &a).unwrap(), Some(records[1].clone()));
    let q = client.prepare(receiver(), 2).unwrap();
    let a = server.respond(q.body()).unwrap();
    assert!(
        client.decode(q, &a).is_err(),
        "missing continuation is not absence"
    );

    let mut anchor = accepted();
    anchor.hash[0] ^= 1;
    assert!(Client::new(server.manifest().clone(), server.public(), anchor).is_err());
    anchor = accepted();
    anchor.required_start = 99;
    assert!(Client::new(server.manifest().clone(), server.public(), anchor).is_err());
    let mut public = server.public().to_vec();
    public[0] ^= 1;
    assert!(Client::new(server.manifest().clone(), &public, accepted()).is_err());
    let mut stale = server.manifest().clone();
    stale.directory.salt[0] ^= 1;
    let stale = Client::new(stale, server.public(), accepted()).unwrap();
    let q = stale.prepare(receiver(), 0).unwrap();
    assert!(matches!(server.respond(q.body()), Err(Error::Revision)));
    assert!(server.respond(&[]).is_err());
    let mut malformed = client.prepare(receiver(), 0).unwrap().body().to_vec();
    malformed[52..60].fill(255);
    assert!(server.respond(&malformed).is_err());
    println!(
        "public_bytes={} query_bytes={} response_bytes={}",
        server.public().len(),
        receiver_pir::query_bytes(),
        receiver_pir::response_bytes()
    );
}

#[test]
fn reject_corrupt_rows_before_preprocessing() {
    let mut snapshot = Snapshot::build(manifest(), &[]).unwrap();
    snapshot.data[0] ^= 1;
    assert!(matches!(Server::new(snapshot), Err(Error::Malformed)));
    let mut m = manifest();
    m.rows = 4096;
    assert!(matches!(
        Server::new(Snapshot::build(m, &[]).unwrap()),
        Err(Error::Unsupported)
    ));
}
