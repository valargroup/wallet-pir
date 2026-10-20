//! The cryptographic checks the receiver protocol crates leave to their caller: every
//! payment's receiver and Action fields before it is stored, and every record and the
//! common witness file before a publication is served. The checks are
//! `zakura-receiver-crypto`'s; this module maps the local protocol types onto its
//! types and its canonical record bytes.
use receiver_directory::{
    snapshot::{Manifest, Snapshot, ROW_BYTES, SLOTS},
    Payment, RawRecord, ReceiverBytes, RECORD_BYTES,
};
#[cfg(not(test))]
use receiver_pir::server::Server;
use receiver_pir_server::Publication;
use receiver_store::{Error, IndexedBlock, Store};
use zakura_receiver_crypto::{
    witness::{self, WitnessCache, WitnessSnapshot},
    Record,
};

/// The library's refusal, as the directory error a caller already handles.
pub fn directory_error(error: zakura_receiver_crypto::Error) -> Error {
    match error {
        zakura_receiver_crypto::Error::Malformed => Error::Malformed,
        zakura_receiver_crypto::Error::Coverage => Error::Coverage,
        zakura_receiver_crypto::Error::Capacity => Error::Capacity,
    }
}

/// Checks one encoded record slot: an entirely zero slot is empty, and any other must
/// be a well-formed record with a valid receiver, note commitment, Action nullifier and
/// ephemeral key.
pub fn check_slot(slot: &[u8]) -> Result<bool, Error> {
    Ok(Record::decode(slot).map_err(directory_error)?.is_some())
}

/// Checks a payment to `receiver` before it is stored.
pub fn check_payment(receiver: &ReceiverBytes, payment: &Payment) -> Result<(), Error> {
    let slot = RawRecord {
        receiver: *receiver,
        page: 0,
        total: 1,
        payment: payment.clone(),
    }
    .encode()
    .map_err(|_| Error::Malformed)?;
    check_slot(&slot).map(|_| ())
}

/// Checks every payment of `block`, then appends it to `store`.
pub fn append(store: &mut Store, block: &IndexedBlock) -> Result<(), Error> {
    for (receiver, payment) in &block.payments {
        check_payment(receiver, payment)?;
    }
    store.append(block)
}

/// The fields of a validated `manifest` that its witness file binds to.
pub fn publication(manifest: &Manifest) -> Result<witness::Publication, Error> {
    Ok(witness::Publication {
        genesis: manifest.genesis,
        directory_revision: manifest.revision()?,
        end_height: manifest.end_height,
        end_hash: manifest.end_hash,
        start_position: manifest.start_position,
        end_position: manifest.end_position,
        records: manifest.records,
    })
}

/// Builds the publication's common witness file from `store`'s commitment history,
/// reusing unchanged subtrees from `cache`. A history of more than `max_commitments`
/// commitments, or a file over the node limit, is [`Error::Capacity`].
pub fn build_witnesses(
    store: &Store,
    manifest: &Manifest,
    cache: &mut WitnessCache,
    max_commitments: u64,
) -> Result<Vec<u8>, Error> {
    let inputs = store.witness_inputs(manifest, max_commitments)?;
    let proof = cache
        .build(
            &publication(manifest)?,
            &inputs.commitments,
            &inputs.positions,
        )
        .map_err(directory_error)?;
    Ok(proof.encode())
}

/// The `(position, cmx)` of every record in `rows`, each slot checked by
/// [`check_slot`].
fn leaves(rows: &[u8]) -> Result<Vec<(u32, [u8; 32])>, Error> {
    let mut leaves = Vec::new();
    for row in rows.as_chunks::<ROW_BYTES>().0 {
        // Bytes after the last slot are row padding, not a record.
        for slot in row[..SLOTS * RECORD_BYTES].as_chunks::<RECORD_BYTES>().0 {
            if let Some(record) = Record::decode(slot).map_err(directory_error)? {
                // A validated publication holds positions within the tree.
                let position =
                    u32::try_from(record.payment.position).map_err(|_| Error::Malformed)?;
                leaves.push((position, record.payment.cmx));
            }
        }
    }
    Ok(leaves)
}

/// Checks every record of `snapshot`, and that `witnesses`, when given, binds to its
/// manifest and proves every record.
pub fn check_publication(snapshot: &Snapshot, witnesses: Option<&[u8]>) -> Result<(), Error> {
    let leaves = leaves(&snapshot.data)?;
    if let Some(proof) = witnesses {
        WitnessSnapshot::decode(proof, &publication(&snapshot.manifest)?)
            .and_then(|proof| proof.check_paths(leaves))
            .map_err(directory_error)?;
    }
    Ok(())
}

/// Checks a publication before serving, returning the PIR protocol's error type.
/// Both fresh preparation and test-fixture reuse use this gate; database errors
/// cannot arise from the check and are refused as malformed data.
pub fn check_publication_for_serving(
    snapshot: &Snapshot,
    witnesses: Option<&[u8]>,
) -> Result<(), receiver_pir::Error> {
    check_publication(snapshot, witnesses).map_err(|error| {
        receiver_pir::Error::Directory(match error {
            Error::Malformed => receiver_directory::Error::Malformed,
            Error::Coverage => receiver_directory::Error::Coverage,
            Error::Capacity => receiver_directory::Error::Capacity,
            Error::MissingPage => receiver_directory::Error::MissingPage,
            Error::Json(error) => receiver_directory::Error::Json(error),
            // A publication check reads no database.
            Error::Sql(_) => receiver_directory::Error::Malformed,
        })
    })
}

/// Prepares `snapshot` for serving once [`check_publication_for_serving`] accepts it.
pub fn prepare(
    snapshot: Snapshot,
    witnesses: Option<Vec<u8>>,
) -> Result<Publication, receiver_pir::Error> {
    check_publication_for_serving(&snapshot, witnesses.as_deref())?;
    #[cfg(test)]
    {
        crate::prepared::prepare(snapshot, witnesses)
    }
    #[cfg(not(test))]
    {
        Ok(Publication::verified(Server::new(snapshot)?, witnesses))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::probe::common;
    use receiver_directory::{snapshot::TREE_SIZE, Hash};
    use receiver_store::{Config, IndexedBlock};

    /// A publication with records at positions 0 and 5 of an 8-leaf tree, and the
    /// witness file built from `tree` for those positions.
    fn witnessed(tree: &[Hash]) -> (Snapshot, Vec<u8>) {
        let mut manifest = common::manifest(receiver_pir::MIN_ROWS);
        manifest.start_position = 0;
        manifest.end_position = 8;
        let records: Vec<_> = [0u8, 5]
            .into_iter()
            .enumerate()
            .map(|(page, position)| {
                let mut record = common::record(page as u32, 2);
                record.payment.position = position.into();
                record.payment.cmx = [position + 1; 32];
                record
            })
            .collect();
        let snapshot = Snapshot::build(manifest, &records).unwrap();
        let positions = [0, 5].into_iter().collect();
        let proof =
            WitnessSnapshot::build(&publication(&snapshot.manifest).unwrap(), tree, &positions)
                .unwrap()
                .encode();
        (snapshot, proof)
    }

    /// A file that binds to the publication but lacks a sibling a record needs, or
    /// whose tree holds another commitment at a record's position, is refused before
    /// serving.
    #[test]
    fn witnesses_must_prove_every_served_record() {
        use receiver_directory::Error::{Coverage, Malformed};
        use receiver_pir::Error::Directory;
        let tree: Vec<Hash> = (1..=8).map(|i| [i; 32]).collect();
        let (snapshot, proof) = witnessed(&tree);
        prepare(snapshot.clone(), Some(proof.clone())).unwrap();
        // Drop position 5's level-0 sibling, (0, 4), which position 0 does not use, and
        // correct the node count, without going through the builder.
        let mut incomplete = proof;
        let node = incomplete[152..]
            .chunks_exact(37)
            .position(|n| n[0] == 0 && n[1..5] == 4u32.to_le_bytes())
            .unwrap();
        incomplete.drain(152 + node * 37..152 + (node + 1) * 37);
        let count = u32::from_le_bytes(incomplete[148..152].try_into().unwrap()) - 1;
        incomplete[148..152].copy_from_slice(&count.to_le_bytes());
        assert!(matches!(
            prepare(snapshot, Some(incomplete)),
            Err(Directory(Coverage))
        ));
        let mut other = tree;
        other[5] = [9; 32];
        let (snapshot, proof) = witnessed(&other);
        assert!(matches!(
            prepare(snapshot, Some(proof)),
            Err(Directory(Malformed))
        ));
    }

    /// A publication whose row holds a structurally valid record with an invalid
    /// receiver, commitment, nullifier or ephemeral key is refused before serving, and
    /// such a payment is refused before it is stored.
    #[test]
    fn cryptographically_invalid_records_are_never_stored_or_served() {
        let mut manifest = common::manifest(receiver_pir::MIN_ROWS);
        manifest.end_position = 300;
        prepare(
            Snapshot::build(manifest.clone(), &[common::record(0, 1)]).unwrap(),
            None,
        )
        .unwrap();
        for field in 0..4 {
            let mut record = common::record(0, 1);
            let p = &mut record.payment;
            match field {
                0 => record.receiver = [255; 43],
                1 => p.cmx = [255; 32],
                2 => p.action_nullifier = [255; 32],
                _ => p.ephemeral_key = [255; 32],
            }
            let snapshot = Snapshot::build(manifest.clone(), &[record.clone()]).unwrap();
            assert!(prepare(snapshot, None).is_err(), "{field}");
            assert!(matches!(
                check_payment(&record.receiver, &record.payment),
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

    /// Proofs built from the store through a reused cache match fresh builds byte for
    /// byte across appends, rewinds, reopening and replacement blocks.
    #[test]
    fn cached_store_proofs_follow_rewinds_reopen_and_replacement_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("directory.sqlite");
        let config = Config {
            genesis: [1; 32],
            start_height: 100,
            start_parent: [2; 32],
            start_position: 0,
        };
        let mut store = Store::open(&path, config.clone()).unwrap();
        let mut payment = common::record(0, 1).payment;
        (payment.height, payment.block_hash, payment.position) = (100, [3; 32], 2);
        let mut block = IndexedBlock {
            height: 100,
            hash: [3; 32],
            parent: [2; 32],
            start_position: 0,
            end_position: 3,
            coinbase_actions: 1,
            payments: vec![(common::receiver(), payment.clone())],
            commitments: vec![[1; 32], [2; 32], payment.cmx],
        };
        append(&mut store, &block).unwrap();
        let mut cache = WitnessCache::default();
        let check = |store: &Store, cache: &mut WitnessCache, snapshot: &Snapshot| {
            let fresh = build_witnesses(
                store,
                &snapshot.manifest,
                &mut WitnessCache::default(),
                TREE_SIZE,
            )
            .unwrap();
            let cached = build_witnesses(store, &snapshot.manifest, cache, TREE_SIZE).unwrap();
            assert_eq!(cached, fresh);
            check_publication(snapshot, Some(&cached)).unwrap();
            cached
        };
        let first = store.snapshot(8, 0).unwrap();
        let expected = check(&store, &mut cache, &first);
        let len = first.manifest.end_position;
        assert!(matches!(
            build_witnesses(&store, &first.manifest, &mut cache, len - 1),
            Err(Error::Capacity)
        ));
        (block.height, block.hash, block.parent) = (101, [4; 32], [3; 32]);
        (block.start_position, block.end_position) = (3, 5);
        block.commitments = vec![[3; 32], [4; 32]];
        block.payments.clear();
        append(&mut store, &block).unwrap();
        let second = store.snapshot(8, 0).unwrap();
        check(&store, &mut cache, &second);
        store.rewind(100, [3; 32]).unwrap();
        assert_eq!(check(&store, &mut cache, &first), expected);
        drop(store);
        let mut store = Store::open(&path, config).unwrap();
        block.hash = [5; 32];
        block.commitments[0] = [9; 32];
        append(&mut store, &block).unwrap();
        let replacement = store.snapshot(8, 0).unwrap();
        let proof = check(&store, &mut cache, &replacement);
        // The replaced block's proof does not prove the replacement publication.
        assert!(check_publication(&second, Some(&proof)).is_err());
    }

    /// A wallet's transport fetches the served file over real HTTP, and the library
    /// verifies the served record's path under the publication.
    #[tokio::test]
    async fn served_witnesses_verify_through_the_wallet_transport() {
        use receiver_pir::{
            transport::{DirectoryClient, Transport},
            AcceptedCoverage, Error as PirError,
        };
        struct Http(reqwest::Client);
        impl Transport for Http {
            async fn get(&self, url: &str, limit: usize) -> Result<Vec<u8>, PirError> {
                let response = self.0.get(url).send().await;
                let response = response.map_err(|e| PirError::Transport(e.to_string()))?;
                crate::read_limited(
                    response
                        .error_for_status()
                        .map_err(|e| PirError::Transport(e.to_string()))?,
                    limit,
                )
                .await
                .map_err(|e| PirError::Transport(e.to_string()))
            }
            async fn post(&self, _: &str, _: Vec<u8>, _: usize) -> Result<Vec<u8>, PirError> {
                unreachable!("this test reads only files")
            }
        }
        let tree: Vec<Hash> = (1..=8).map(|i| [i; 32]).collect();
        let (snapshot, proof) = witnessed(&tree);
        let manifest = snapshot.manifest.clone();
        let publications = receiver_pir_server::Publications::default();
        assert!(publications.publish(prepare(snapshot, Some(proof)).unwrap(), 0));
        let app = crate::router::router_with_publications(publications);
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", socket.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
        let http = Http(reqwest::Client::new());
        let served = DirectoryClient::fetch_manifest(&origin, &http)
            .await
            .unwrap();
        let accepted = AcceptedCoverage {
            genesis: manifest.genesis,
            required_start: manifest.start_height,
            height: manifest.end_height,
            hash: manifest.end_hash,
        };
        let client = DirectoryClient::connect_manifest(&origin, http, accepted, served)
            .await
            .unwrap();
        let bytes = client.witnesses().await.unwrap();
        let proof = WitnessSnapshot::decode(&bytes, &publication(&manifest).unwrap()).unwrap();
        proof.path(5, tree[5]).unwrap();
        assert!(proof.path(5, [9; 32]).is_err());
    }
}
