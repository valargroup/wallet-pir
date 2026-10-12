# v12 whole-chain census, 2026-10-12

This census sizes the v12 worker host and sets the boundary between the
archive and recent tiers. It covers the two layouts Roman chose:

- decoupled archive: 32,768 directory rows and 32,768 page rows;
- decoupled recent: 2,048 directory rows and 8,192 page rows.

## Provenance

| Item | Value |
|---|---|
| Journal | v2 journal on the coordinator, `/srv/zakura/transparent-event-data-v2`, read-only; it covers heights 0–3,505,130 |
| Census range | heights 0–3,505,000 inclusive; anchor `000000000022cfabf3b73ebe48abcffd0a343a959dbc8dc7625ee1aaf751299a` |
| Tool | `shard-census`, `release-fast`, built on the coordinator from `e29cda78` (branch `v12/decoupled-pages`), sha256 in [raw/BIN.sha256](raw/BIN.sha256); Rust 1.97.1 |
| Runs | [raw/run.sh](raw/run.sh), both layouts in parallel under `nice -n 19 ionice -c3`. Archive took 17 min 54 s with 0.9 GB RSS; recent took 10 min 40 s with 0.3 GB RSS. |
| Sample | `transparent/evidence/workload-sample-2026-09-08/sample.json` |

A first attempt used anchor 3,514,700. It failed at once because that height is
past the journal's end. Its empty outputs were not kept.

[raw/extra-2k/](raw/extra-2k/) holds the extra decoupled 2k/8k runs from the
prototype gate: the recent window and catch-up anchors. They were copied
unchanged from the hub's `decoupled-pages-census/extra-2k/`, except that the
large text outputs (over 100 kB, and the per-anchor census and pairs files)
were gzipped with their bytes unchanged.

## Results

**Archive, 32k/32k, whole chain**

- 12 directory shards. 11 are sealed: 10 by the directory-bytes target and
  shard 0 by the 32-page-shard limit. Shard 11 is the open tail, starting at
  3,076,108.
- 169 page shards: 164 under sealed directory shards and 5 under the tail.
- Page row fill is 92.9%. Directory byte fill is about 0.857 at seal.
- Chains: of 11,892,831 script-shard pairs, 2,965,949 have pages. Runs per
  chain are p50 3, p95 16, p99 21, max 32. The full histogram is in the raw
  output.

**Recent, 2k/8k, whole chain**

- 927 directory shards and 1,109 page shards.
- Runs per chain are p50 1, p95 2, p99 4, max 25.

**Boundary**

- Archive shards must seal full, so the last sealed archive directory shard
  ends at 3,076,107. Recent covers 3,076,108 to the tip, about 429k blocks at
  the census anchor.
- Over that span the recent layout has 20 directory shards and 25 page shards.
  That is 40,960 directory rows plus 204,800 page rows.

## Memory estimate

Prepared memory is estimated at about 4.8 KB per 4,096-byte row. That rate is
v11's archive owner (34 GiB for 77 coupled shards of 98,304 rows), scaled to
the v12 row counts:

- Archive sealed tables: (11 + 164) × 32,768 rows = 5.73M rows, about 27 GiB.
- Recent from the boundary: about 246k rows, about 1.2 GiB. That is under the
  2 GB stop line, but recent now spans more than a year rather than six months.
- Total: about 28 GiB, so one `m-8vcpu-64gb` host fits.

The worker's measured RSS will replace this estimate in the qualification
report.
