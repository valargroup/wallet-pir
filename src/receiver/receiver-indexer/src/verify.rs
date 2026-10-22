//! The cryptographic checks the receiver protocol crates leave to their caller: every
//! payment's receiver and Action fields before it is stored, and every record and its
//! companion witness payload before a publication is served. The checks are
//! `zakura-receiver-crypto`'s; this module maps the local protocol types onto its
//! types and its canonical record bytes.
use receiver_directory::{
    snapshot::{Snapshot, ROW_BYTES, SLOTS},
    witness::{self, PAYLOAD_BYTES},
    Hash, Payment, RawRecord, ReceiverBytes, RECORD_BYTES,
};
#[cfg(not(test))]
use receiver_pir::server::{Server, WitnessServer};
use receiver_pir_server::Publication;
use receiver_store::{Error, IndexedBlock, Store, SHARD_LEAVES};
use std::collections::BTreeMap;
use zakura_receiver_crypto::Record;

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
        ordinal: 0,
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

/// The `(position, cmx, ordinal)` of every record in `rows`, each slot checked by
/// [`check_slot`].
fn leaves(rows: &[u8]) -> Result<Vec<(u64, Hash, u32)>, Error> {
    let mut leaves = Vec::new();
    for row in rows.as_chunks::<ROW_BYTES>().0 {
        // Bytes after the last slot are row padding, not a record.
        for slot in row[..SLOTS * RECORD_BYTES].as_chunks::<RECORD_BYTES>().0 {
            if let Some(record) = Record::decode(slot).map_err(directory_error)? {
                leaves.push((record.payment.position, record.payment.cmx, record.ordinal));
            }
        }
    }
    Ok(leaves)
}

/// Checks every record of `snapshot` and that `witnesses` is its companion table: bound
/// to it, one payload per record by ordinal, each supplying exactly the siblings its
/// position has at the table's tree size, and every final payload folding from its
/// record's commitment to one root per Ironwood shard. Returns those roots for the caller
/// to compare with its own.
pub fn check_publication(
    snapshot: &Snapshot,
    witnesses: &witness::Table,
) -> Result<BTreeMap<u64, Hash>, Error> {
    let leaves = leaves(&snapshot.data)?;
    witnesses.validate()?;
    let table = &witnesses.manifest;
    table.check_payments(&snapshot.manifest)?;
    let payloads = witnesses.data.as_chunks::<PAYLOAD_BYTES>().0;
    let mut roots = BTreeMap::new();
    for (position, cmx, ordinal) in leaves {
        let payload = payloads.get(ordinal as usize).ok_or(Error::Malformed)?;
        if let Some(root) = crate::witness::shard_root(payload, position, &cmx, table.tree_size)? {
            if *roots.entry(position / SHARD_LEAVES).or_insert(root) != root {
                return Err(Error::Coverage);
            }
        }
    }
    Ok(roots)
}

/// Checks a publication before serving, returning the PIR protocol's error type.
/// Both fresh preparation and test-fixture reuse use this gate; database errors
/// cannot arise from the check and are refused as malformed data.
pub fn check_publication_for_serving(
    snapshot: &Snapshot,
    witnesses: &witness::Table,
) -> Result<(), receiver_pir::Error> {
    check_publication(snapshot, witnesses)
        .map(|_| ())
        .map_err(|error| {
            receiver_pir::Error::Directory(match error {
                Error::Malformed => receiver_directory::Error::Malformed,
                Error::Coverage => receiver_directory::Error::Coverage,
                Error::Capacity => receiver_directory::Error::Capacity,
                Error::MissingPage => receiver_directory::Error::MissingPage,
                Error::Json(error) => receiver_directory::Error::Json(error),
                // A publication check reads no database or file.
                Error::Sql(_) | Error::Io(_) => receiver_directory::Error::Malformed,
            })
        })
}

/// Prepares `snapshot` and its companion `witnesses` for serving once
/// [`check_publication_for_serving`] accepts them.
pub fn prepare(
    snapshot: Snapshot,
    witnesses: witness::Table,
) -> Result<Publication, receiver_pir::Error> {
    check_publication_for_serving(&snapshot, &witnesses)?;
    #[cfg(test)]
    {
        crate::prepared::prepare(snapshot, witnesses)
    }
    #[cfg(not(test))]
    {
        let server = Server::new(snapshot)?;
        let witnesses = WitnessServer::new(witnesses, &server)?;
        Ok(Publication::verified(server, witnesses))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::probe::common;
    use receiver_store::{Config, IndexedBlock};
    use zakura_receiver_crypto::witness::{combine, empty_root, payloads};

    /// A provisional companion table for `snapshot`: zero payloads built at its end, whose
    /// positions lie in a filling shard well past the records' supplied siblings.
    fn placeholder(snapshot: &Snapshot) -> witness::Table {
        let payloads = vec![[0; PAYLOAD_BYTES]; snapshot.manifest.records as usize];
        crate::witness::table(snapshot, &payloads).unwrap()
    }

    /// An 8-leaf tree of canonical commitments.
    fn tree() -> Vec<Hash> {
        (1..=8u8).map(|i| [i; 32]).collect()
    }

    /// A publication over `tree` with records at positions 0 and 5, and its companion
    /// table built at its end from `served`.
    fn witnessed(tree: &[Hash], served: &[Hash]) -> (Snapshot, witness::Table) {
        let mut manifest = common::manifest(receiver_pir::MIN_ROWS);
        manifest.start_position = 0;
        manifest.end_position = tree.len() as u64;
        let mut records: Vec<_> = [0u64, 5]
            .into_iter()
            .enumerate()
            .map(|(page, position)| {
                let mut record = common::record(page as u32, 2);
                record.payment.position = position;
                record.payment.cmx = tree[position as usize];
                record
            })
            .collect();
        receiver_directory::snapshot::assign_ordinals(&mut records);
        let snapshot = Snapshot::build(manifest, &records).unwrap();
        let table = crate::witness::table(&snapshot, &payloads(served, &[0, 5]).unwrap()).unwrap();
        (snapshot, table)
    }

    /// A companion table bound to another payment table, one supplying a sibling its
    /// position does not have at the table's tree size, or one with a payload missing is
    /// refused before serving.
    #[test]
    fn companion_tables_must_serve_their_payment_table() {
        use receiver_directory::Error::{Coverage, Malformed};
        use receiver_pir::Error::Directory;
        let (snapshot, table) = witnessed(&tree(), &tree());
        prepare(snapshot.clone(), table.clone()).unwrap();
        let mut other = snapshot.clone();
        other.manifest.salt[0] ^= 1;
        let (_, stale) = witnessed(&tree(), &tree());
        let stale = witness::Table::build(
            witness::Manifest {
                payments: other.manifest.revision().unwrap(),
                ..stale.manifest
            },
            &payloads(&tree(), &[0, 5]).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            prepare(snapshot.clone(), stale),
            Err(Directory(Coverage))
        ));
        // Position 0's level-3 sibling, positions 8 to 15, is not in an 8-leaf tree.
        let mut extra = payloads(&tree(), &[0, 5]).unwrap();
        extra[0][3 * 32] = 1;
        let extra = crate::witness::table(&snapshot, &extra).unwrap();
        assert!(matches!(
            prepare(snapshot.clone(), extra),
            Err(Directory(Malformed))
        ));
        let short = crate::witness::table(&snapshot, &payloads(&tree(), &[0]).unwrap()).unwrap();
        assert!(prepare(snapshot, short).is_err());
    }

    /// A publication whose row holds a structurally valid record with an invalid
    /// receiver, commitment, nullifier or ephemeral key is refused before serving, and
    /// such a payment is refused before it is stored.
    #[test]
    fn cryptographically_invalid_records_are_never_stored_or_served() {
        let mut manifest = common::manifest(receiver_pir::MIN_ROWS);
        manifest.end_position = 300;
        let valid = Snapshot::build(manifest.clone(), &[common::record(0, 1)]).unwrap();
        let table = placeholder(&valid);
        prepare(valid, table).unwrap();
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
            let table = placeholder(&snapshot);
            assert!(prepare(snapshot, table).is_err(), "{field}");
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

    /// A wallet's transport reads the record over encrypted PIR, then its companion
    /// payload by the record's ordinal over encrypted PIR, and the payload's siblings with
    /// the empty subtrees above the tree reach the root an independent full-tree
    /// computation gives, which another commitment at the position does not reach.
    #[tokio::test]
    async fn served_witnesses_verify_through_the_wallet_transport() {
        use receiver_pir::{
            transport::{DirectoryClient, Transport},
            AcceptedCoverage, Error as PirError,
        };
        use zakura_receiver_crypto::wire::decode_payload;
        struct Http(reqwest::Client);
        /// Reads one response within `limit`.
        async fn read(request: reqwest::RequestBuilder, limit: usize) -> Result<Vec<u8>, PirError> {
            let failed = |e: reqwest::Error| PirError::Transport(e.to_string());
            let response = request.send().await.map_err(failed)?;
            crate::read_limited(response.error_for_status().map_err(failed)?, limit)
                .await
                .map_err(|e| PirError::Transport(e.to_string()))
        }
        impl Transport for Http {
            async fn get(&self, url: &str, limit: usize) -> Result<Vec<u8>, PirError> {
                read(self.0.get(url), limit).await
            }
            async fn post(
                &self,
                url: &str,
                body: Vec<u8>,
                limit: usize,
            ) -> Result<Vec<u8>, PirError> {
                read(self.0.post(url).body(body), limit).await
            }
        }
        let tree = tree();
        let (snapshot, table) = witnessed(&tree, &tree);
        let manifest = snapshot.manifest.clone();
        let publications = receiver_pir_server::Publications::default();
        assert!(publications.publish(prepare(snapshot, table).unwrap(), 0));
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
        let record = client.page(common::receiver(), 1).await.unwrap().unwrap();
        assert_eq!((record.payment.position, record.ordinal), (5, 1));
        let payload = client.witness(record.ordinal).await.unwrap();
        // The independent oracle: every level of the 8-leaf tree, then empty subtrees.
        let mut level = tree.clone();
        for height in 0..3 {
            level = level
                .chunks(2)
                .map(|pair| combine(height, &pair[0], &pair[1]).unwrap())
                .collect();
        }
        let mut root = level[0];
        for height in 3..32 {
            root = combine(height, &root, &empty_root(height).unwrap()).unwrap();
        }
        let fold = |cmx: Hash| {
            let siblings = decode_payload(&payload, 5, 8).unwrap();
            let mut node = cmx;
            for height in 0..32u8 {
                let sibling = match siblings.get(usize::from(height)).copied().flatten() {
                    Some(sibling) => sibling,
                    None => empty_root(height).unwrap(),
                };
                node = match (5u64 >> height) & 1 {
                    0 => combine(height, &node, &sibling),
                    _ => combine(height, &sibling, &node),
                }
                .unwrap();
            }
            node
        };
        assert_eq!(fold(tree[5]), root);
        assert_ne!(fold([9; 32]), root);
    }
}
