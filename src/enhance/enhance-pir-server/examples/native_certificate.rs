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
//!
//! Synthetic (a shape screen, not a snapshot certificate): `--fill random|max
//! [--seed 1]` in place of the hint or rows and the served bytes —
//! `enhance --fill max [--rows 32768] [--shard 0]` or `status --fill max
//! --network-hex <hex> --salt-hex <hex>`. The database is a splitmix64
//! full-range one (`random`) or all 0xffff (`max`), hinted under the product's
//! own masks and packing setup, with the worst-case query term.
//!
//! `--query-rounding dithered` exports the 44-bit dithered query's report,
//! `native-noise-two-mask-rounded-dithered-v1`, with each block's
//! `query_l2_squared`; the checker budgets dithered rounding from it as a
//! variance term. Servers accept both widths over one snapshot, so a snapshot
//! needs both certificates. Without the flag the report is unchanged.
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

/// A `rows` x `cols` database, column-major: splitmix64 full-range values
/// (`random`) or every value 0xffff (`max`).
fn synthetic_columns(fill: &str, seed: u64, rows: usize, cols: usize) -> Vec<Vec<u16>> {
    let mut state = seed;
    (0..cols)
        .map(|_| {
            (0..rows)
                .map(|_| match fill {
                    "max" => u16::MAX,
                    "random" => {
                        state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
                        let mut z = state;
                        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
                        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
                        (z ^ (z >> 31)) as u16
                    }
                    other => panic!("unknown fill {other}"),
                })
                .collect()
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
    let dithered = match flags.get("query-rounding").map(String::as_str) {
        None | Some("nearest") => false,
        Some("dithered") => true,
        Some(other) => panic!("unknown query rounding {other}"),
    };
    // A synthetic database has no served bytes to bind to.
    let synthetic = flags.get("fill").map(|fill| {
        (
            fill.clone(),
            flags.get("seed").map_or(1, |s| s.parse().unwrap()),
        )
    });
    let served_sha256: Option<[u8; 32]> =
        match (flags.get("public"), flags.get("public-sha256"), &synthetic) {
            (Some(path), None, None) => {
                Some(Sha256::digest(std::fs::read(path).expect("served public")).into())
            }
            (None, Some(digest), None) => Some(hash32(digest)),
            (None, None, Some(_)) => None,
            _ => panic!("pass exactly one of --public, --public-sha256 or --fill"),
        };
    let (setup, hint, rows, query, binding): (
        NativeSetup,
        Vec<CrsBlock>,
        usize,
        Vec<WeightNorms>,
        String,
    ) = match product.as_str() {
        "enhance" => {
            let blocks = n::COLS / n::D;
            let (rows, hint, binding) = match &synthetic {
                None => {
                    let rows: usize = flags["rows"].parse().unwrap();
                    let bytes = std::fs::read(&flags["hint"]).expect("hint");
                    let hint = enhance_pir_server::wire::decode_crs_blocks(&bytes, blocks, n::D)
                        .expect("MPH1 hint");
                    let binding = format!("hint_sha256:{}", hex(&Sha256::digest(&bytes)));
                    (rows, hint, binding)
                }
                Some((fill, seed)) => {
                    let rows: usize = flags.get("rows").map_or(32768, |r| r.parse().unwrap());
                    let shard: u64 = flags.get("shard").map_or(0, |s| s.parse().unwrap());
                    let columns = synthetic_columns(fill, *seed, rows, n::COLS);
                    let masks = n::query_masks(shard);
                    let hint =
                        pir_native::hint(&masks[..rows / n::D], rows, n::COLS, |c| &columns[c])
                            .unwrap()
                            .into_iter()
                            .map(|rows| CrsBlock { rows })
                            .collect();
                    let binding = format!("synthetic:{fill}:seed={seed}:rows={rows}:shard={shard}");
                    (rows, hint, binding)
                }
            };
            // Any u16 column: L1 <= 65535*rows, L2^2 <= 65535^2*rows.
            let worst = WeightNorms {
                l1: 65535 * rows as u128,
                l2_squared: 65535u128 * 65535 * rows as u128,
                max: 65535,
            };
            (n::packing_setup(), hint, rows, vec![worst; blocks], binding)
        }
        "status" => {
            use enhance_pir::status::{native_packing_setup, COLS, ROWS, ROW_BYTES};
            let rows_bin = match &synthetic {
                None => std::fs::read(&flags["rows-bin"]).expect("rows.bin"),
                // Row-major bytes of the column-major synthetic database.
                Some((fill, seed)) => {
                    let columns = synthetic_columns(fill, *seed, ROWS, COLS);
                    (0..ROWS)
                        .flat_map(|r| {
                            let columns = &columns;
                            (0..COLS).flat_map(move |c| columns[c][r].to_le_bytes())
                        })
                        .collect()
                }
            };
            let network = hash32(&flags["network-hex"]);
            let salt = hash32(&flags["salt-hex"]);
            let hint = status_hint(&rows_bin, &network, &salt);
            let worst_case =
                synthetic.is_some() || flags.get("worst-case-query").is_some_and(|v| v == "true");
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
            let binding = match &synthetic {
                None => format!("rows_sha256:{}", hex(&Sha256::digest(&rows_bin))),
                Some((fill, seed)) => format!("synthetic:{fill}:seed={seed}:rows={ROWS}"),
            };
            (
                native_packing_setup(&network, &salt),
                hint,
                ROWS,
                query,
                binding,
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
    let public_sha256: [u8; 32] = Sha256::digest(&public).into();
    if let Some(served) = served_sha256 {
        assert!(
            public_sha256 == served,
            "rebuilt public masks differ from the served bytes; wrong snapshot"
        );
    }
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
            let mut block = json!({
                "weights": norms(selected.independent(q)),
                "query_l1": q.l1.to_string(),
                "kh_l1": "0",
                "public_mask_screens": screens,
                "one_limb": [],
            });
            if dithered {
                block["query_l2_squared"] = json!(q.l2_squared.to_string());
            }
            block
        })
        .collect();
    let mut report = json!({
        "format": "native-noise-two-mask-rounded-v1",
        "d": n::D, "q_bits": 54, "p_bits": 16,
        "published_mask_bits": n::MASK_BITS,
        // RNP3-equivalent size; the wallet-pir wire omits RNP3's 36-byte
        // magic/setup header because its envelope binds the setup.
        "published_bytes": 36 + public.len(),
        "served_public_bytes": public.len(),
        "served_public_sha256": hex(&public_sha256),
        "product": product,
        "rows": rows, "cols": cols,
        "query_bits": n::QUERY_BITS, "response_bits": n::RESPONSE_BITS, "kh_bits": 54,
        "setup_id": hex(&setup.id()),
        "database_sha256": binding,
        "sampler_sha256": hex(&gaussian_cdf_sha256()),
        "sampler_counts": gaussian_counts().into_iter().map(|(x, c)| json!([x, c.to_string()])).collect::<Vec<_>>(),
        "blocks": blocks,
    });
    if synthetic.is_some() {
        report["served_public_checked"] = json!(false);
    }
    if dithered {
        // A distinct format, so a checker that predates dithering, and would
        // budget the rounding as nearest, refuses the report.
        report["format"] = json!("native-noise-two-mask-rounded-dithered-v1");
        report["query_rounding"] = json!("dithered");
        report["query_bits"] = json!(n::DITHERED_QUERY_BITS);
    }
    println!("{report}");
}
