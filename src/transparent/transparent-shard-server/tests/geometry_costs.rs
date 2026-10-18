//! What a table geometry costs a wallet on the wire.
//!
//! Every figure is a pure function of `(rows, row_bytes)`, so a candidate
//! geometry can be costed before anything is built at it — which is the only
//! reason the storage and bandwidth sides of a geometry choice can be weighed
//! against each other at all.
//!
//! The numbers pinned below are the native ReinspiRING two-mask 8-bit profile
//! with a one-digit packing key, which the directory and pages use: one byte
//! per coefficient, so a 4,096-byte row is two 2,048-byte blocks; a
//! 13,824-byte `K_g` packing key and a 28-bit dithered selection per query,
//! 11,264 bytes of published masks per block, and a 2,560-byte response body
//! per block behind a 16-byte header. The 16-bit profile they replaced (a
//! 27,648-byte key, 44- or 49-bit selections, 14,848 bytes of masks and a
//! 5,648-byte response) is measured against this one in
//! `evidence/transparent/p8-trial-2026-10-12/`. The earlier SimplePIR P14
//! figures (96,264 bytes for a 2,048-row query, 14,336 bytes of setup, 5,136
//! bytes of response) are recorded in
//! `evidence/transparent/baselines/shard-utilisation/measurement-v4.json`.

use transparent_native::Profile;
use transparent_shard::layout::{Geometry, DECOUPLED_2K_8K, PROFILES};
use transparent_shard::PAGE_ROW_BYTES;
use transparent_shard_server::runtime::SharedParams;

/// The profile every history table uses.
fn native() -> Profile {
    let profile = transparent_native::profile_for("directory");
    assert!(profile == transparent_native::profile_for("pages"));
    profile
}

/// One table's per-query and per-setup wire cost.
struct Cost {
    setup: usize,
    /// The 28-bit dithered query a wallet sends.
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
    let native = native();
    let blocks = row_bytes as usize / native.instance_bytes();
    Cost {
        setup: blocks * native.block_public_bytes(),
        // The revision binding, the packing key, and the selection.
        query: 8 + native.request_len_bits(rows as usize, native.primary_bits()),
        // The binding, the parameter epoch, and the response bodies.
        response: 16 + blocks * native.block_response_bytes(),
        packing_keys: native.key_bytes(),
    }
}

/// Query size grows with the row count; setup and response do not.
///
/// This is what decides the bandwidth half of a geometry choice. Setup is a
/// function of the row width, so every candidate pays the same 22,528 bytes
/// per table per segment opened — halving a table buys nothing there. What it
/// buys is a smaller query, and only for the table that shrank.
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
        assert_eq!(
            pair[0].setup, pair[1].setup,
            "setup follows the width alone"
        );
        assert_eq!(pair[0].response, pair[1].response);
    }

    assert_eq!(costs[0].query, 21_000);
    assert_eq!(costs[1].query, 28_168);
    assert_eq!(costs[2].query, 42_504);
    assert_eq!(costs[0].setup, 22_528);
    assert_eq!(costs[0].response, 5_136);
}

/// The packing key is fixed and small; the selection is what the row count
/// buys.
///
/// 13,824 bytes of every query are the `K_g` key — 65% of a 2,048-row query but
/// only 32% at 8,192 rows and 5% at 65,536. Unlike the P14 scheme, where 86,016
/// bytes of evaluation keys dominated every geometry, the native profile makes
/// row count the lever on query cost.
#[test]
fn the_selection_outgrows_the_packing_key_as_tables_grow() {
    for (rows, share) in [(2_048u64, 65), (8_192, 32), (65_536, 5)] {
        let c = cost(rows, PAGE_ROW_BYTES as u64);
        assert_eq!(c.packing_keys, 13_824);
        assert_eq!(c.packing_keys * 100 / c.query, share, "{rows} rows");
    }
}

/// Pin the baseline geometry's per-query upload size.
///
/// The decoupled-2k-8k directory has 2,048 rows and uploads 21,000 bytes per
/// query; its page shards have 8,192 rows and upload 42,504.
/// This is a wire-size check, not a complete wallet-sync measurement; see
/// `evidence/transparent/README.md` for dataset-scoped comparisons.
#[test]
fn the_pinned_geometry_costs_what_it_did() {
    // Each table is costed at *its own* width. They agree across the whole
    // registry today, and costing the directory at the page width was harmless
    // for exactly that reason — right up until a geometry separated them, at
    // which point this test would have gone on passing while reporting the
    // wrong number.
    let directory = table_cost(&DECOUPLED_2K_8K, Table::Directory);
    let pages = table_cost(&DECOUPLED_2K_8K, Table::Pages);
    assert_eq!(directory.query, 21_000, "directory query");
    assert_eq!(pages.query, 42_504, "page query");
    assert_eq!(directory.setup, 22_528);
    assert_eq!(pages.setup, 22_528);
    // Setup follows the row width, which neither table changed, so widening
    // the row *count* left the per-shard setup exactly where it was.
    assert_eq!(directory.response, pages.response);
}

/// Every registry geometry must be costable, and the archive tier must cost
/// what the deployment plan says it does.
///
/// The case for `decoupled-32k-32k` is that a narrower directory keeps
/// directory-only restoration at 128,520 upload bytes rather than the 243,208
/// a 65,536-row directory would cost, while the page table still absorbs
/// dense old history.
#[test]
fn the_archive_candidates_cost_what_the_plan_claims() {
    let wide = table_cost(&transparent_shard::DECOUPLED_32K_32K, Table::Directory);
    assert_eq!(wide.query, 128_520, "decoupled-32k-32k directory query");
    // What a 65,536-row directory would have cost, and the reason the archive
    // has none.
    assert_eq!(cost(65_536, PAGE_ROW_BYTES as u64).query, 243_208);

    for geometry in PROFILES {
        for table in [Table::Directory, Table::Pages] {
            let cost = table_cost(geometry, table);
            // Setup and response follow the row *width*, which the registry
            // holds constant, so every geometry pays the same for both and
            // differs only in the query it uploads.
            assert_eq!(cost.setup, 22_528, "{}", geometry.name);
            assert_eq!(cost.response, 5_136, "{}", geometry.name);
            assert_eq!(cost.packing_keys, 13_824, "{}", geometry.name);
        }
    }
}

/// The costs above are what the server actually enforces and publishes, not a
/// parallel calculation that could drift from it.
#[test]
fn the_server_enforces_the_costed_sizes() {
    use transparent_shard_server::shardset::Table as ServerTable;
    for geometry in PROFILES {
        for (table, server) in [
            (Table::Directory, ServerTable::Directory),
            (Table::Pages, ServerTable::Pages),
        ] {
            let cost = table_cost(geometry, table);
            let shared = SharedParams::build(geometry, server).unwrap();
            assert_eq!(shared.dithered_query_bytes(), None, "{}", geometry.name);
            assert_eq!(shared.query_bytes(), cost.query, "{}", geometry.name);
            assert!(shared.accepts_query_bytes(shared.profile.rows, cost.query));
            assert!(!shared.accepts_query_bytes(shared.profile.rows, cost.query + 1));
            assert_eq!(shared.response_bytes(), cost.response, "{}", geometry.name);
            assert_eq!(
                shared.scheme().public_bytes,
                cost.setup,
                "{}",
                geometry.name
            );
        }
    }
}
