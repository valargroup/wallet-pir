use super::*;
use crate::wire::{encode_crs_blocks, read_crs_blocks};
use sha2::{Digest, Sha256};
use std::io::{Cursor, Write};

fn layout() -> DatabaseLayout {
    DatabaseLayout {
        record_bytes: 3584,
        records_per_row: 1,
        shard_rows: 2048,
        pir_profile: ipir_sp::SimplePirProfile::P14,
    }
}

#[test]
fn streamed_database_rejects_short_corrupt_and_failed_reads_without_panicking() {
    let (_, params) = shard_parameters(&layout()).unwrap();
    let count = params.db_rows * params.db_cols;
    let bytes = vec![0u8; count * 2];
    let digest = hex::encode(Sha256::digest(&bytes));
    let input = BufReader::with_capacity(
        IO_BUFFER_BYTES,
        VerifiedReader::new(Cursor::new(&bytes), bytes.len() as u64, digest.clone()),
    );
    assert!(read_database(input, params.clone()).is_ok());
    for data in [&bytes[..bytes.len() - 1], &bytes[..1]] {
        assert!(read_database(data, params.clone()).is_err());
    }
    let mut corrupt = bytes;
    corrupt[0] = 1;
    let input = BufReader::with_capacity(
        IO_BUFFER_BYTES,
        VerifiedReader::new(Cursor::new(&corrupt), corrupt.len() as u64, digest),
    );
    assert!(read_database(input, params.clone()).is_err());
    struct Failed;
    impl Read for Failed {
        fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::other("injected read failure"))
        }
    }
    assert!(read_database(Failed, params).is_err());
}

#[test]
fn legacy_v7_artifacts_are_rejected_and_streamed_persistence_preserves_queries_and_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let layout = layout();
    let (rlwe, params) = shard_parameters(&layout).unwrap();
    // Native hints are exact lifted products over the native ring, so they
    // take the native query masks rather than q48 setup polynomials.
    #[cfg(feature = "native-reinspiring")]
    let setup = crate::native::public_query_masks(
        DatabaseId::Enhance.setup_seed_bytes(),
        params.db_rows,
        params.db_cols,
    );
    #[cfg(not(feature = "native-reinspiring"))]
    let setup = ipir_sp::IPIRClient::from_profile(
        params.num_items,
        params.item_size_bits,
        layout.pir_profile,
    )
    .unwrap()
    .generate_public_query_setup_simplepir_from_seed(DatabaseId::Enhance.setup_seed_bytes())
    .polys()
    .to_vec();
    let rows = vec![3; layout.shard_bytes()];
    let prepared =
        PreparedShard::build(&layout, 0, 0, "fixture".into(), &rows, &rlwe, &setup).unwrap();
    let query = vec![1; params.db_rows];
    let expected_query = prepared.runtime.evaluate(&rlwe, &query).unwrap();
    // Independently reproduce the pre-refactor artifact writer: whole byte
    // arrays and hashes, including the exact metadata schema and version.
    let db: Vec<u8> = prepared
        .runtime
        .server
        .db()
        .iter()
        .flat_map(|v| v.to_le_bytes())
        .collect();
    let hint = encode_crs_blocks(&prepared.crs_blocks);
    fs::write(dir.path().join("database.u16le"), &db).unwrap();
    fs::write(dir.path().join("partial-crs.bin"), &hint).unwrap();
    let metadata = serde_json::json!({
        "version": 7, "table": "enhance", "rlwe_degree": rlwe.d, "rlwe_modulus": rlwe.q,
        "db_rows": params.db_rows, "db_cols": params.db_cols, "plaintext_modulus": params.p,
        "shard_id": 0, "query_row_start": 0, "rows_sha256": "fixture",
        "database_sha256": hex::encode(Sha256::digest(&db)),
        "crs_sha256": hex::encode(Sha256::digest(&hint)),
    });
    fs::write(
        dir.path().join("metadata.json"),
        serde_json::to_vec_pretty(&metadata).unwrap(),
    )
    .unwrap();
    let load = || {
        ShardRuntime::load(
            dir.path(),
            DatabaseId::Enhance,
            &layout,
            0,
            0,
            "fixture",
            &rlwe,
        )
    };
    assert!(load().is_err(), "profile-less v7 artifacts must be rebuilt");
    let cached = prepared
        .persist(dir.path(), DatabaseId::Enhance, &layout, &rlwe)
        .unwrap();
    assert_eq!(fs::read(dir.path().join("database.u16le")).unwrap(), db);
    assert_eq!(fs::read(dir.path().join("partial-crs.bin")).unwrap(), hint);
    assert_eq!(
        cached.runtime.evaluate(&rlwe, &query).unwrap(),
        expected_query
    );
    let path = dir.path().join("metadata.json");
    let current = fs::read(&path).unwrap();
    let mut old: serde_json::Value = serde_json::from_slice(&current).unwrap();
    old["version"] = 8.into();
    fs::write(&path, serde_json::to_vec(&old).unwrap()).unwrap();
    assert!(
        load().is_err(),
        "schema-10 artifacts must be rebuilt even at equal PIR dimensions"
    );
    fs::write(&path, current).unwrap();
    assert_eq!(
        load().unwrap().runtime.evaluate(&rlwe, &query).unwrap(),
        expected_query
    );
    assert_eq!(
        read_crs_blocks(Cursor::new(&hint), 1, rlwe.d).unwrap()[0].rows,
        read_crs_blocks(cached.publication.reader(), 1, rlwe.d).unwrap()[0].rows
    );
    let file = File::options()
        .write(true)
        .open(dir.path().join("partial-crs.bin"))
        .unwrap();
    file.set_len(hint.len() as u64 - 1).unwrap();
    assert!(load().is_err());
    // Restore CRS and independently corrupt the database and metadata.
    fs::write(dir.path().join("partial-crs.bin"), &hint).unwrap();
    File::options()
        .write(true)
        .open(dir.path().join("database.u16le"))
        .unwrap()
        .write_all(&[0])
        .unwrap();
    assert!(load().is_err());
    fs::write(dir.path().join("database.u16le"), &db).unwrap();
    let mut bad_metadata: serde_json::Value =
        serde_json::from_slice(&fs::read(dir.path().join("metadata.json")).unwrap()).unwrap();
    bad_metadata["pir_profile"] = serde_json::json!("simplepir-p16-q46-v1");
    fs::write(
        dir.path().join("metadata.json"),
        serde_json::to_vec(&bad_metadata).unwrap(),
    )
    .unwrap();
    assert!(load().is_err());
}
