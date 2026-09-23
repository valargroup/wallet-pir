//! Local width comparison at equal selectable row geometry. Does not contact a service.
use enhance_pir_server::{
    ipir::{deserialize_first_dim_query, PreparedShard},
    types::{DatabaseId, DatabaseLayout},
};
use inspiring::TopKeyImages;
use ipir_sp::{
    modulus_switch::{recover_published_c1, serialize_rlwe_response_bodies},
    serialize::serialize_packing_keys,
    server::{build_pack_preprocessed_blocks, pack_intermediate_blocks, published_c1_rows},
    IPIRClient, SimplePirProfile,
};
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let width: usize = std::env::args().nth(1).ok_or("pass 737 or 653")?.parse()?;
    if ![737, 653].contains(&width) {
        return Err("pass 737 or 653".into());
    }
    let rows_count: usize = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "4096".into())
        .parse()?;
    if ![4096, 32768].contains(&rows_count) {
        return Err("rows must be 4096 or 32768".into());
    }
    let layout = DatabaseLayout {
        record_bytes: width,
        records_per_row: 33,
        shard_rows: rows_count,
        pir_profile: SimplePirProfile::P16Q48,
    };
    let (rlwe, params) = ipir_sp::params_for_simplepir_profile(
        rows_count as u64,
        layout.item_size_bits(),
        layout.pir_profile,
    )?;
    let client = IPIRClient::from_profile(
        rows_count as u64,
        layout.item_size_bits(),
        layout.pir_profile,
    )?;
    let setup =
        client.generate_public_query_setup_simplepir_from_seed(enhance_pir::v4::setup_seed(0));
    let mut rows = vec![0; layout.shard_bytes()];
    for (i, byte) in rows.iter_mut().enumerate() {
        *byte = i.wrapping_mul(17).wrapping_add(i / layout.row_bytes()) as u8;
    }
    let start = Instant::now();
    let prepared = PreparedShard::build(
        &layout,
        0,
        0,
        "width-study".into(),
        &rows,
        &rlwe,
        setup.polys(),
    )?;
    let preprocessing_ms = start.elapsed().as_secs_f64() * 1000.0;
    let packing = build_pack_preprocessed_blocks(&rlwe, &prepared.crs_blocks)?;
    let top = TopKeyImages::build(&rlwe);
    let public_bytes = published_c1_rows(&packing, rlwe.q);
    let public = recover_published_c1(&public_bytes, rlwe.d, params.instances, rlwe.q);
    let temp = tempfile::tempdir()?;
    let cached = prepared.persist(temp.path(), DatabaseId::Enhance, &layout, &rlwe)?;
    let artifacts: u64 = std::fs::read_dir(temp.path())?
        .map(|p| p.unwrap().metadata().unwrap().len())
        .sum();
    let mut timings = Vec::new();
    let mut response_bytes = 0;
    let mut query_bytes = 0;
    for i in 0..13 {
        let target = i * 311 % rows_count;
        let (query, keys, secret) = client.generate_fresh_query_simplepir(&setup, target);
        let query = query.to_switched_bytes(rlwe.q, params.query_bits);
        query_bytes = 28 + serialize_packing_keys(&rlwe, &keys)?.len() + query.len();
        let start = Instant::now();
        let coefficients = deserialize_first_dim_query(&rlwe, &params, &query)?;
        let intermediate = cached.runtime.evaluate(&rlwe, &coefficients)?;
        let packed = pack_intermediate_blocks(&intermediate, &keys, &top, &packing)?;
        let response = serialize_rlwe_response_bodies(&packed, params.q_prime_1);
        let elapsed = start.elapsed().as_secs_f64() * 1000.0;
        response_bytes = 28 + response.len();
        let decoded = client.decode_response_simplepir(secret, &public, &response);
        assert_eq!(
            &decoded[..layout.row_bytes()],
            &rows[target * layout.row_bytes()..(target + 1) * layout.row_bytes()]
        );
        if i >= 3 {
            timings.push(elapsed);
        }
    }
    timings.sort_by(f64::total_cmp);
    println!(
        "{}",
        serde_json::json!({
            "record_bytes": width, "rows": rows_count, "records_per_row": 33,
            "raw_row_bytes": layout.row_bytes(), "raw_shard_bytes": rows.len(),
            "instances": params.instances, "db_cols": params.db_cols,
            "database_u16_bytes": params.db_rows * params.db_cols * 2,
            "artifact_bytes": artifacts, "public_params_bytes": public_bytes.len(),
            "query_bytes": query_bytes, "response_bytes": response_bytes,
            "preprocessing_ms": preprocessing_ms, "queries": timings.len(),
            "server_ms_median": (timings[4] + timings[5]) / 2.0, "server_ms_max": timings[9],
            "all_queries_exact": true,
        })
    );
    Ok(())
}
