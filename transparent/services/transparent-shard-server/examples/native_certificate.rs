//! Export a `native-noise-two-mask-rounded-v1` report for one Transparent
//! segment, for ipir-sp's `reinspiring/tools/security/certify_native.py`, or
//! with `--query-rounding dithered` a `native-noise-two-mask-rounded-dithered-v1`
//! report for the 44-bit dithered query.
//!
//! Modelled on `enhance/services/enhance-pir-server/examples/native_certificate.rs`.
//! The packing weights depend only on the public hint (query masks times the
//! segment's database) and the table's packing setup, so the tool rebuilds the
//! hint and two-mask preprocessing from the segment bytes exactly as the server
//! does and, when given the served masks, requires the rounded public bytes to
//! equal them — binding the report to that snapshot.
//!
//! Segment mode (the per-snapshot certificate):
//!
//! ```text
//! cargo run --release -p transparent-shard-server --example native_certificate -- \
//!   segment --geometry recent-8k --table directory --rows-bin directory.0.bin \
//!   [--public-sha256 <hex> | --public <file>] [--worst-case-query true]
//! ```
//!
//! The query term is measured from the segment's columns unless
//! `--worst-case-query true` bounds it for any u16 column of that height.
//!
//! Synthetic mode (a shape screen, not a snapshot certificate):
//!
//! ```text
//! ... --example native_certificate -- synthetic --rows 65536 [--seed 1] [--fill random|max]
//! ... --example native_certificate -- synthetic --geometry archive-wide --table pages [...]
//! ```
//!
//! Uses the worst-case query term for any u16 database of `rows` rows, which
//! is data-independent. The packing weights, however, are a function of the
//! hint and therefore of the data: they are measured for a pseudo-random
//! full-range database (`random`, representative of a dense table) or an
//! all-0xffff one (`max`), under a synthetic table's setup — or, with
//! `--geometry` and `--table`, under that registry or display table's own
//! setup and masks. A pass here says a shape has margin; it does not certify a
//! published segment.
//!
//! Every mode takes `--query-rounding nearest|dithered` (default `nearest`).
//! The served masks and packing weights are the same for either: a table
//! publishes one snapshot that answers both query widths. What differs is how
//! the query's rounding is budgeted. Nearest rounding at 49 bits reserves its
//! worst case, `2^4` times each column's L1 norm. Dithered rounding at 44 bits
//! is independent and zero mean, so the checker budgets it as a variance term
//! from each block's `query_l2_squared`, the largest per-column sum of squared
//! entries. A snapshot served at both widths needs both certificates. The
//! nearest report is byte-for-byte what this tool produced before dithering.
use rayon::prelude::*;
use reinspiring::noise::{gaussian_cdf_sha256, gaussian_counts, WeightNorms};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use transparent_native::{self as native, NativePreprocessed, TableProfile, D};

fn norms(w: WeightNorms) -> Value {
    json!({"l1": w.l1.to_string(), "l2_squared": w.l2_squared.to_string(), "max": w.max.to_string()})
}

fn worst(rows: usize) -> WeightNorms {
    // Any u16 column: L1 <= 65535*rows, L2^2 <= 65535^2*rows.
    WeightNorms {
        l1: 65535 * rows as u128,
        l2_squared: 65535u128 * 65535 * rows as u128,
        max: 65535,
    }
}

/// The profile of `--table` at `--geometry`, a history registry geometry or a
/// display one, derived exactly as the server and wallet derive it.
fn table_profile(flags: &HashMap<String, String>) -> TableProfile {
    let name = &flags["geometry"];
    let geometry = transparent_shard::layout::by_name(name)
        .or_else(|| transparent_shard::display::display_by_name(name))
        .expect("registry or display geometry");
    let table = transparent_shard_server::shardset::Table::parse(&flags["table"])
        .expect("directory, pages or txdirectory");
    TableProfile::new(
        transparent_shard::manifest::SCHEMA,
        geometry.name,
        table.as_str(),
        table.rows(geometry),
        table.row_bytes(geometry),
    )
    .unwrap()
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mode = args.first().expect("segment|synthetic").clone();
    let flags: HashMap<_, _> = args[1..]
        .chunks_exact(2)
        .map(|kv| (kv[0].trim_start_matches("--").to_owned(), kv[1].clone()))
        .collect();
    let dithered = match flags.get("query-rounding").map(String::as_str) {
        None | Some("nearest") => false,
        Some("dithered") => true,
        Some(other) => panic!("unknown query rounding {other}"),
    };
    let (profile, columns, served, binding, worst_case) = match mode.as_str() {
        "segment" => {
            let profile = table_profile(&flags);
            let rows_bin = std::fs::read(&flags["rows-bin"]).expect("segment table");
            assert_eq!(
                rows_bin.len(),
                profile.rows * profile.row_bytes,
                "segment table size"
            );
            let served: Option<[u8; 32]> = match (flags.get("public"), flags.get("public-sha256")) {
                (Some(path), None) => {
                    Some(Sha256::digest(std::fs::read(path).expect("served public")).into())
                }
                (None, Some(digest)) => Some(
                    hex::decode(digest)
                        .expect("hex")
                        .try_into()
                        .expect("32-byte digest"),
                ),
                (None, None) => None,
                _ => panic!("pass at most one of --public or --public-sha256"),
            };
            let columns = columns_of(&rows_bin, &profile);
            let binding = format!("rows_sha256:{}", hex::encode(Sha256::digest(&rows_bin)));
            let worst_case = flags.get("worst-case-query").is_some_and(|v| v == "true");
            (profile, columns, served, binding, worst_case)
        }
        "synthetic" => {
            let seed: u64 = flags.get("seed").map_or(1, |s| s.parse().unwrap());
            let fill = flags
                .get("fill")
                .map_or("random", String::as_str)
                .to_owned();
            let named = flags.contains_key("geometry");
            let profile = if named {
                table_profile(&flags)
            } else {
                TableProfile::new(
                    transparent_shard::manifest::SCHEMA,
                    "certificate-synthetic",
                    "directory",
                    flags["rows"].parse().unwrap(),
                    native::INSTANCE_BYTES as u32,
                )
                .unwrap()
            };
            let rows = profile.rows;
            let mut state = seed;
            let columns: Vec<Vec<u16>> = (0..profile.cols)
                .map(|_| {
                    (0..profile.rows)
                        .map(|_| match fill.as_str() {
                            "max" => u16::MAX,
                            "random" => {
                                // splitmix64: deterministic, full-range, cheap.
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
                .collect();
            let binding = if named {
                format!(
                    "synthetic:{fill}:seed={seed}:geometry={}:table={}",
                    flags["geometry"], flags["table"]
                )
            } else {
                format!("synthetic:{fill}:seed={seed}:rows={rows}")
            };
            (profile, columns, None, binding, true)
        }
        _ => panic!("mode must be segment or synthetic"),
    };

    let started = std::time::Instant::now();
    let hint = native::hint(&profile.masks, profile.rows, profile.cols, |c| &columns[c]).unwrap();
    let analyzed: Vec<_> = hint
        .par_iter()
        .map(|block| NativePreprocessed::build_two_mask_analyzed(&profile.setup, block).unwrap())
        .collect();
    let pre: Vec<_> = analyzed.iter().map(|(p, _)| p).collect();
    let public = native::publish(&pre).unwrap();
    assert_eq!(public.len(), native::public_len(profile.cols));
    let public_sha256: [u8; 32] = Sha256::digest(&public).into();
    if let Some(served) = served {
        assert!(
            public_sha256 == served,
            "rebuilt public masks differ from the served bytes; wrong snapshot"
        );
    }
    let query: Vec<WeightNorms> = (0..profile.blocks())
        .map(|block| {
            if worst_case {
                return worst(profile.rows);
            }
            columns[block * D..(block + 1) * D]
                .iter()
                .map(|column| WeightNorms::measure(column.iter().map(|&x| x as i128)))
                .fold(WeightNorms::default(), WeightNorms::envelope)
        })
        .collect();
    let blocks: Vec<Value> = analyzed
        .iter()
        .zip(&query)
        .map(|((_, a), &q)| {
            let screens = a
                .public_mask_screens
                .iter()
                .filter(|(bits, _)| (27..=32).contains(bits))
                .map(|(bits, w)| json!({"bits": bits, "weights": norms(w.independent(q))}))
                .collect::<Vec<_>>();
            let selected = a
                .public_mask_screens
                .iter()
                .find(|(bits, _)| *bits as usize == native::MASK_BITS)
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
    eprintln!(
        "analysed {} rows x {} cols in {:.1}s",
        profile.rows,
        profile.cols,
        started.elapsed().as_secs_f64()
    );
    let mut report = json!({
        "format": "native-noise-two-mask-rounded-v1",
        "d": D, "q_bits": native::Q_BITS, "p_bits": native::P_BITS,
        "published_mask_bits": native::MASK_BITS,
        // RNP3-equivalent size; the Transparent wire omits RNP3's 36-byte
        // magic/setup header because the init document binds the setup.
        "published_bytes": 36 + public.len(),
        "served_public_bytes": public.len(),
        "served_public_sha256": hex::encode(public_sha256),
        "served_public_checked": served.is_some(),
        "product": format!("transparent-{mode}"),
        "rows": profile.rows, "cols": profile.cols,
        "query_bits": native::QUERY_BITS, "response_bits": native::RESPONSE_BITS, "kh_bits": 54,
        "worst_case_query": worst_case,
        "setup_id": hex::encode(profile.setup.id()),
        "database_sha256": binding,
        "sampler_sha256": hex::encode(gaussian_cdf_sha256()),
        "sampler_counts": gaussian_counts().into_iter().map(|(x, c)| json!([x, c.to_string()])).collect::<Vec<_>>(),
        "blocks": blocks,
    });
    if dithered {
        // A distinct format, so a checker that predates dithering, and would
        // budget the rounding as nearest, refuses the report.
        report["format"] = json!("native-noise-two-mask-rounded-dithered-v1");
        report["query_rounding"] = json!("dithered");
        report["query_bits"] = json!(native::DITHERED_QUERY_BITS);
    }
    println!("{report}");
}

/// Column-major u16 coefficients of a row-major segment table, as the server
/// encodes them.
fn columns_of(rows_bin: &[u8], profile: &TableProfile) -> Vec<Vec<u16>> {
    (0..profile.cols)
        .into_par_iter()
        .map(|col| {
            (0..profile.rows)
                .map(|row| native::row_coefficient(rows_bin, profile.row_bytes, row, col))
                .collect()
        })
        .collect()
}
