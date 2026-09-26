//! Export a `native-noise-two-mask-rounded-v1` report for a live native snapshot,
//! for ipir-sp's `certify_native.py`.
//!
//! The packing weights depend only on the public hint and packing setup. Before
//! exporting, the tool rebuilds two-mask preprocessing and requires the rounded
//! public bytes to equal the bytes actually served, binding the report to that
//! snapshot.
//!
//! `--public <file>` supplies the served bytes; `--public-sha256 <hex>` instead
//! checks the manifest digest that clients verify.
//!
//! Enhance: `enhance --hint <MPH1 hint> --public <served public> --rows <rows>`.
//! The query term uses the worst case for any u16 database of `rows` rows.
//! Status: `status --rows-bin <rows.bin> --public <served public>
//! --network-hex <hex> --salt-hex <hex> [--worst-case-query true]`. The hint
//! comes from the actual database; the query term does too unless
//! `--worst-case-query true` bounds it for any u16 database.
use enhance_pir::native as n;
use ipir_sp::server::CrsBlock;
use rayon::prelude::*;
use reinspiring::{
    native::{NativePreprocessed, NativeSetup},
    noise::{gaussian_cdf_sha256, gaussian_counts, WeightNorms},
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

fn norms(w: WeightNorms) -> Value {
    json!({"l1": w.l1.to_string(), "l2_squared": w.l2_squared.to_string(), "max": w.max.to_string()})
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|x| format!("{x:02x}")).collect()
}

fn hash32(value: &str) -> [u8; 32] {
    let bytes: Vec<u8> = (0..value.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&value[i..i + 2], 16).expect("hex"))
        .collect();
    bytes.try_into().expect("32-byte hex")
}

/// Status hint H = A * D for the complete database, one polynomial per column.
fn status_hint(rows_bin: &[u8], network: &[u8; 32], salt: &[u8; 32]) -> Vec<CrsBlock> {
    use enhance_pir::status::{native_query_masks, COLS, ROWS, ROW_BYTES};
    assert_eq!(rows_bin.len(), ROWS * ROW_BYTES, "status rows.bin size");
    let masks = native_query_masks(network, salt);
    let lift = reinspiring::lift_ntt::LiftContext::new(n::D, n::Q).unwrap();
    let public = lift.prepare_public_dot(&masks, 65535).unwrap();
    (0..COLS / n::D)
        .map(|block| CrsBlock {
            rows: (block * n::D..(block + 1) * n::D)
                .into_par_iter()
                .map(|col| {
                    let column: Vec<u64> = (0..ROWS)
                        .map(|r| {
                            let at = r * ROW_BYTES + 2 * col;
                            u16::from_le_bytes([rows_bin[at], rows_bin[at + 1]]) as u64
                        })
                        .collect();
                    let polys: Vec<Vec<u64>> =
                        column.chunks_exact(n::D).map(<[u64]>::to_vec).collect();
                    lift.public_dot(&public, &polys).unwrap()
                })
                .collect(),
        })
        .collect()
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let product = args.first().expect("enhance|status").clone();
    let flags: HashMap<_, _> = args[1..]
        .chunks_exact(2)
        .map(|kv| (kv[0].trim_start_matches("--").to_owned(), kv[1].clone()))
        .collect();
    let served_sha256: [u8; 32] = match (flags.get("public"), flags.get("public-sha256")) {
        (Some(path), None) => Sha256::digest(std::fs::read(path).expect("served public")).into(),
        (None, Some(digest)) => hash32(digest),
        _ => panic!("pass exactly one of --public or --public-sha256"),
    };
    let (setup, hint, rows, query, binding): (NativeSetup, Vec<CrsBlock>, usize, Vec<WeightNorms>, String) =
        match product.as_str() {
            "enhance" => {
                let rows: usize = flags["rows"].parse().unwrap();
                let bytes = std::fs::read(&flags["hint"]).expect("hint");
                let blocks = n::COLS / n::D;
                let hint = enhance_pir_server::wire::decode_crs_blocks(&bytes, blocks, n::D)
                    .expect("MPH1 hint");
                // Any u16 column: L1 <= 65535*rows, L2^2 <= 65535^2*rows.
                let worst = WeightNorms {
                    l1: 65535 * rows as u128,
                    l2_squared: 65535u128 * 65535 * rows as u128,
                    max: 65535,
                };
                (
                    n::packing_setup(),
                    hint,
                    rows,
                    vec![worst; blocks],
                    format!("hint_sha256:{}", hex(&Sha256::digest(&bytes))),
                )
            }
            "status" => {
                use enhance_pir::status::{native_packing_setup, COLS, ROWS, ROW_BYTES};
                let rows_bin = std::fs::read(&flags["rows-bin"]).expect("rows.bin");
                let network = hash32(&flags["network-hex"]);
                let salt = hash32(&flags["salt-hex"]);
                let hint = status_hint(&rows_bin, &network, &salt);
                let worst_case = flags.get("worst-case-query").is_some_and(|v| v == "true");
                let query = (0..COLS / n::D)
                    .map(|block| {
                        if worst_case {
                            return WeightNorms {
                                l1: 65535 * ROWS as u128,
                                l2_squared: 65535u128 * 65535 * ROWS as u128,
                                max: 65535,
                            };
                        }
                        (block * n::D..(block + 1) * n::D)
                            .map(|col| {
                                WeightNorms::measure((0..ROWS).map(|r| {
                                    let at = r * ROW_BYTES + 2 * col;
                                    u16::from_le_bytes([rows_bin[at], rows_bin[at + 1]]) as i128
                                }))
                            })
                            .fold(WeightNorms::default(), WeightNorms::envelope)
                    })
                    .collect();
                (
                    native_packing_setup(&network, &salt),
                    hint,
                    ROWS,
                    query,
                    format!("rows_sha256:{}", hex(&Sha256::digest(&rows_bin))),
                )
            }
            _ => panic!("product must be enhance or status"),
        };
    let cols = hint.len() * n::D;
    let analyzed: Vec<_> = hint
        .par_iter()
        .map(|b| NativePreprocessed::build_two_mask_analyzed(&setup, &b.rows).unwrap())
        .collect();
    let pre: Vec<_> = analyzed.iter().map(|(p, _)| p).collect();
    let public = n::publish(&pre).unwrap();
    assert_eq!(public.len(), n::public_len(cols));
    assert!(
        <[u8; 32]>::from(Sha256::digest(&public)) == served_sha256,
        "rebuilt public masks differ from the served bytes; wrong snapshot"
    );
    let blocks: Vec<Value> = analyzed
        .iter()
        .zip(&query)
        .map(|((_, a), &q)| {
            let screens = a
                .public_mask_screens
                .iter()
                .map(|(bits, w)| json!({"bits": bits, "weights": norms(w.independent(q))}))
                .collect::<Vec<_>>();
            let selected = a
                .public_mask_screens
                .iter()
                .find(|(bits, _)| *bits as usize == n::MASK_BITS)
                .expect("selected precision screen")
                .1;
            json!({
                "weights": norms(selected.independent(q)),
                "query_l1": q.l1.to_string(),
                "kh_l1": "0",
                "public_mask_screens": screens,
                "one_limb": [],
            })
        })
        .collect();
    println!(
        "{}",
        json!({
            "format": "native-noise-two-mask-rounded-v1",
            "d": n::D, "q_bits": 54, "p_bits": 16,
            "published_mask_bits": n::MASK_BITS,
            // RNP3-equivalent size; the wallet-pir wire omits RNP3's 36-byte
            // magic/setup header because its envelope binds the setup.
            "published_bytes": 36 + public.len(),
            "served_public_bytes": public.len(),
            "served_public_sha256": hex(&served_sha256),
            "product": product,
            "rows": rows, "cols": cols,
            "query_bits": n::QUERY_BITS, "response_bits": n::RESPONSE_BITS, "kh_bits": 54,
            "setup_id": hex(&setup.id()),
            "database_sha256": binding,
            "sampler_sha256": hex(&gaussian_cdf_sha256()),
            "sampler_counts": gaussian_counts().into_iter().map(|(x, c)| json!([x, c.to_string()])).collect::<Vec<_>>(),
            "blocks": blocks,
        })
    );
}
