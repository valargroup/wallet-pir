//! Independent wallet SDK wire compatibility with the production server engine.
use enhance_pir::types::RECORD_BYTES;
use enhance_pir::v4::*;
use enhance_pir_server::v4::runtime::{plan, Engine, Packing};
use zakura_pir_enhance::{
    AcceptedAnchor, ClientResourceLimits, GenerationAcceptance, QuerySession,
};

fn records(start: u64, count: usize) -> Result<Vec<u8>, String> {
    let mut bytes = vec![0; count * RECORD_BYTES];
    for (i, record) in bytes.chunks_exact_mut(RECORD_BYTES).enumerate() {
        record[..8].copy_from_slice(&(start + i as u64).to_le_bytes());
    }
    Ok(bytes)
}

fn main() {
    let budget = enhance_pir_server::PackingBudget::coordinator();
    // First occupied row of every supported allocation, including partial rows.
    for occupied_rows in [
        1u64, 2049, 4097, 8193, 10241, 12289, 16385, 18433, 20481, 24577, 26625, 28673,
    ] {
        let count = (occupied_rows - 1) * 33 + 17;
        let coverage = Lifecycle::default()
            .coverage(count, Geometry::default())
            .unwrap();
        let shard = coverage.shards[0].clone();
        let domain = plan(shard.clone(), records).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let mut engine = Engine::new(directory.path());
        let evaluation = engine.prepare(domain.clone(), records).unwrap();
        let packing =
            Packing::new(shard.logical_rows, &evaluation.hint().unwrap(), &budget).unwrap();
        let manifest = Manifest {
            recovery_epoch: 0,
            placement_revision: 0,
            domain_recovery_epochs: [(0, "0".into())].into(),
            schema_version: SCHEMA_VERSION,
            protocol_revision: PROTOCOL_REVISION.into(),
            network: "main".into(),
            pool: "ironwood".into(),
            generation: 1,
            anchor_height: 3428143,
            anchor_block_hash: "42".repeat(32),
            geometry: Geometry::default(),
            coverage,
            sessions: vec![packing.reference(0).unwrap()],
            unit_identities: [(0, domain.units.clone())].into(),
        };
        // JSON is the boundary: do not share server types with the SDK.
        let wallet_manifest =
            serde_json::from_value(serde_json::to_value(&manifest).unwrap()).unwrap();
        let wallet_session =
            serde_json::from_value(serde_json::to_value(packing.session(&manifest, 0)).unwrap())
                .unwrap();
        let acceptance = GenerationAcceptance::new(
            "main",
            3428143,
            AcceptedAnchor::new(3428143, [0x42; 32], count),
            ClientResourceLimits::new(32768),
        );
        let client =
            QuerySession::from_session(&wallet_manifest, wallet_session, &acceptance).unwrap();
        let mut positions = vec![0, 16, count - 1];
        for unit in &domain.units {
            let boundary = unit.local_row_start * 33;
            if boundary > 0 {
                positions.extend([boundary - 1, boundary]);
            }
        }
        positions.sort_unstable();
        positions.dedup();
        let mut upload = 0;
        let mut download = 0;
        for &position in &positions {
            let (query, slot) = client.prepare_position(position).unwrap();
            let binding = QueryBinding::decode(query.body()).unwrap();
            let coefficients = packing.query_coefficients(query.body(), binding).unwrap();
            let response = packing
                .pack(query.body(), &evaluation.evaluate(&coefficients).unwrap())
                .unwrap();
            upload = query.body().len();
            download = response.len();
            let row = client.decode(query, &response).unwrap();
            assert_eq!(
                &row[slot * RECORD_BYTES..(slot + 1) * RECORD_BYTES],
                records(position, 1).unwrap()
            );
        }
        println!(
            "{}",
            serde_json::json!({"logical_rows": shard.logical_rows,
            "occupied_rows": occupied_rows, "allocated_units": domain.units.iter().map(|u| u.allocated_rows).collect::<Vec<_>>(),
            "queries": positions.len(), "incorrect": 0, "upload_bytes": upload, "download_bytes": download,
            "wallet_revision": "9b190657d129d08e964623d0ecc1d8e4ffb31b1d"})
        );
    }
}
