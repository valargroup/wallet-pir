//! What a table geometry costs a wallet on the wire.
//!
//! Every figure is a pure function of `(rows, row_bytes)`, so a candidate
//! geometry can be costed before anything is built at it — which is the only
//! reason the storage and bandwidth sides of a geometry choice can be weighed
//! against each other at all.
//!
//! The numbers pinned below are calibrated against
//! `transparent/evidence/baselines/shard-utilisation/measurement-v4.json`,
//! which measured 96,264 bytes for a directory query, 19,325 for a published
//! setup (14,336 bytes base64-encoded, plus its JSON envelope), and an average
//! of 127,863 for the largest history's mix of directory and page queries.

use ipir_sp::modulus_switch::{published_c1_len, response_body_len};
use ipir_sp::serialize::serialized_packing_keys_len;
use transparent_shard::layout::{Geometry, PROFILES, RECENT_8K};
use transparent_shard::PAGE_ROW_BYTES;

/// One table's per-query and per-setup wire cost.
struct Cost {
    setup: usize,
    query: usize,
    response: usize,
    packing_keys: usize,
}

/// Which table of a geometry to cost. Local to this test: costing a table
/// means reading its own two dimensions, and taking one from one table and one
/// from the other is precisely the mistake worth making impossible here.
#[derive(Clone, Copy)]
enum Table {
    Directory,
    Pages,
}

fn table_cost(geometry: &Geometry, table: Table) -> Cost {
    match table {
        Table::Directory => cost(geometry.directory_rows, geometry.directory_row_bytes as u64),
        Table::Pages => cost(geometry.page_rows, geometry.page_row_bytes as u64),
    }
}

fn cost(rows: u64, row_bytes: u64) -> Cost {
    let (rlwe, params) = ipir_sp::params_for_simplepir(rows, row_bytes * 8).expect("parameters");
    let blocks = params.db_cols / rlwe.d;
    let packing_keys = serialized_packing_keys_len(&rlwe);
    Cost {
        setup: blocks * published_c1_len(rlwe.d, rlwe.q),
        // The shard id, the packing keys, and the first-dimension query.
        query: 8 + packing_keys + (params.db_rows * params.query_bits).div_ceil(8),
        // The shard id, the parameter epoch, and the response bodies.
        response: 16 + blocks * response_body_len(rlwe.d, params.q_prime_1),
        packing_keys,
    }
}

/// Query size grows with the row count; setup and response do not.
///
/// This is what decides the bandwidth half of a geometry choice. Setup is a
/// function of `db_cols`, which the row width fixes, so every candidate pays
/// the same 14,336 bytes per table per generation opened — halving a table
/// buys nothing there. What it buys is a smaller query, and only for the table
/// that shrank.
#[test]
fn a_narrower_table_buys_a_smaller_query_and_nothing_else() {
    let row_bytes = PAGE_ROW_BYTES as u64;
    let costs: Vec<Cost> = [2_048, 4_096, 8_192]
        .into_iter()
        .map(|rows| cost(rows, row_bytes))
        .collect();

    for pair in costs.windows(2) {
        assert!(
            pair[1].query > pair[0].query,
            "a taller table must cost a larger query"
        );
        assert_eq!(pair[0].setup, pair[1].setup, "setup follows db_cols alone");
        assert_eq!(pair[0].response, pair[1].response);
    }

    assert_eq!(costs[0].query, 96_264);
    assert_eq!(costs[1].query, 106_504);
    assert_eq!(costs[2].query, 128_008);
    assert_eq!(costs[0].setup, 14_336);
    assert_eq!(costs[0].response, 5_136);
}

/// Most of a query is evaluation keys, and no geometry touches them.
///
/// 86,016 bytes of every query are packing keys, which is 89% of a directory
/// query and 81% of a page query at the tallest candidate. Geometry moves the
/// remainder. This is why bounded evaluation-key reuse, which is specified but
/// not built, is the change that would matter more than any row count — and why
/// a geometry argument must not be presented as the answer to query cost.
#[test]
fn evaluation_keys_dominate_every_query_whatever_the_geometry() {
    for rows in [2_048u64, 4_096, 8_192] {
        let c = cost(rows, PAGE_ROW_BYTES as u64);
        assert_eq!(c.packing_keys, 86_016);
        assert!(
            c.packing_keys * 100 / c.query >= 67,
            "packing keys should dominate at {rows} rows"
        );
    }
}

/// Pin the baseline geometry's per-query upload size.
///
/// Both recent-8k tables have 8,192 rows and upload 128,008 bytes per query.
/// This is a wire-size check, not a complete wallet-sync measurement; see
/// `transparent/evidence/README.md` for dataset-scoped comparisons.
#[test]
fn the_pinned_geometry_costs_what_it_did() {
    // Each table is costed at *its own* width. They agree across the whole
    // registry today, and costing the directory at the page width was harmless
    // for exactly that reason — right up until a geometry separated them, at
    // which point this test would have gone on passing while reporting the
    // wrong number.
    let directory = table_cost(&RECENT_8K, Table::Directory);
    let pages = table_cost(&RECENT_8K, Table::Pages);
    assert_eq!(directory.query, 128_008, "directory query");
    assert_eq!(pages.query, 128_008, "page query");
    assert_eq!(directory.setup, 14_336);
    assert_eq!(pages.setup, 14_336);
    // Setup follows the row width, which neither table changed, so widening
    // the row *count* left the per-shard setup exactly where it was.
    assert_eq!(directory.response, pages.response);
}

/// Every registry geometry must be costable, and the archive tier must cost
/// what the deployment plan says it does.
///
/// The plan's whole case for `archive-wide` is that a narrower directory keeps
/// directory-only restoration at 258,056 upload bytes rather than the 430,088 a
/// 65,536-row directory would cost, while the page table still absorbs dense
/// old history. Those two numbers decide the tier, so they are pinned here
/// rather than left in prose.
#[test]
fn the_archive_candidates_cost_what_the_plan_claims() {
    let wide = table_cost(&transparent_shard::ARCHIVE_WIDE, Table::Directory);
    assert_eq!(wide.query, 258_056, "archive-wide directory query");
    let square = table_cost(&transparent_shard::ARCHIVE_32K, Table::Directory);
    assert_eq!(square.query, 258_056, "archive-32k directory query");
    // What a 65,536-row directory would have cost, and the reason neither
    // candidate has one.
    assert_eq!(cost(65_536, PAGE_ROW_BYTES as u64).query, 430_088);

    for geometry in PROFILES {
        for table in [Table::Directory, Table::Pages] {
            let cost = table_cost(geometry, table);
            // Setup and response follow the row *width*, which the registry
            // holds constant, so every geometry pays the same for both and
            // differs only in the query it uploads.
            assert_eq!(cost.setup, 14_336, "{}", geometry.name);
            assert_eq!(cost.response, 5_136, "{}", geometry.name);
            assert_eq!(cost.packing_keys, 86_016, "{}", geometry.name);
        }
    }
}
