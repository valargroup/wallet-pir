# Coordinator journal census handoff

This command is an offline analysis tool. It does not open the archive database,
call RPC, fetch parents, build a UTXO set, contact a service or change the journal.
Run it after the existing ingest exits and releases `writer.lock`. A running
writer is refused. Retain the original journal and ingest receipt unchanged.

From a checkout containing this change, build and run with the repository's
Rust 1.97.1 toolchain. On this managed workspace, prefix both Cargo commands with
the assigned `tool-exec --repo wallet-pir --`; on the coordinator use its existing
sanctioned offline build environment and dependency configuration. No new access
or credential selector is required. Choose a scratch filesystem outside the
journal with several GiB free for the disk-backed unique-txid index. The default
is the existing system temporary directory. Its private temporary directory is
removed on success or ordinary error; abrupt termination can leave scratch files.

```bash
cargo test --locked --profile release-fast \
  --manifest-path transparent/tools/txid-sizing/export/Cargo.toml
nice -n 19 ionice -c 3 cargo run --locked --profile release-fast \
  --manifest-path transparent/tools/txid-sizing/export/Cargo.toml -- \
  --journal-census /srv/zakura/txid-display-genesis/journal \
  3508673 --anchor-from-journal > /tmp/txid-display-genesis-census.json
```

The fixed height is Roman's reported ingest target, **not a measured anchor in
this checkout**. `--anchor-from-journal` pins the hash at that height using the
existing committed block index and prints it in the JSON; it does not obtain an
independent chain attestation. If an independently retained ingest anchor hash is
available, replace that argument with its display hex; a mismatch is refused.
An optional fourth census argument selects an existing scratch directory outside
the journal. Missing committed heights or sidecars, a block without exactly one
coinbase record, and ineligible sidecar records are errors, not exclusions.

Only stdout contains the result JSON. Progress goes to stderr every 10,000 blocks.
Publish the JSON atomically after exit zero; redirected partial output on failure
is not evidence. The result contains no raw scripts, individual txids, credentials
or query ciphertexts. Record the checkout SHA, exact command, compiler version,
start/end times, JSON SHA256, ingest executable/source pins and anchor receipt
alongside it. Compiled reader/codec/metadata/tool/lock digests, binary digest,
checkpoint bytes, block-membership digest and validated-sidecar digest chain are
included. The checkpoint can cover more blocks than the selected anchor.

## What the JSON establishes

- `domains`: distinct eligible display records, output/input counts, mixed and
  input-only shapes, coinbase/noncoinbase and activation-era breakdowns; actual
  stored size histograms; conservative hypothetical exact-fee size histograms;
  inline/overflow and fragment/entry-byte counts at 128/192/256/384/512/768/1024;
  empirical 85/90/95/99 frontiers. Unknown, exact zero, nonzero exact and
  non-applicable fees remain separate. Unknown fees are bounded using the shared
  encoder's ULEB lengths for zero through `MAX_MONEY`.
- `pages`: chronological 40,000-distinct-record archives, with a final partial
  archive included. Whole-archive page-row demand p50/p99/max by era; archives
  crossing an activation boundary are labeled `mixed-era`, rather than assigned
  arbitrarily to either era. Current v1 fragment framing and per-archive txid
  order are replayed. Hypothetical exact-fee endpoint packings are **scenarios**;
  separate safe lower/upper row bounds cover interior fee widths. Adjacent page
  tables shared across k=1/4/16/64 archives are compared at 256/512/1024/4096 rows.
  They concatenate per-archive packing, conservatively retaining partial rows.
  Memory includes independent per-archive 4096-row lookup directories for size
  scenarios, allocated page bytes and the existing preprocessing reservation
  formula. Safe page bounds do not assert directory allocation bounds. These
  are table byte/reservation estimates, **not native RSS or latency**.
- `joint_routes`: chronological archive lookup versus independent 1/4/16/64
  hash buckets, with global or 4/16-bucket overflow, plus the public group ID of
  pages shared across k=1/4/16/64 chronological archives. Each is reported for route
  observables alone and with era, logical fragment count and deduplicated initial
  directory-choice count. One fixed snapshot/revision is assumed. Distinct txids
  are checked globally in SQLite; padding, outputs and fragments are never real
  candidates. Inline classes retain global membership when no page request
  exposes a group; shared overflow group IDs can narrow a hash lookup.
  Definite/possible memberships cover every unknown-fee width,
  including branch and fragment-count discontinuities. Possible classes with
  zero definite candidates need not exist after fees are known. Five-candidate
  controls and K=1000/10000 counts are diagnostic engineering policies, not
  anonymity guarantees. Global overflow cannot erase a narrow lookup route.

Timing, retries, actual shared-page request coalescing, page segment counts and
changing publication revisions remain unmodeled and can narrow intersections.
Hash-directory budgets use actual bucket assignments and stored/scenario/safe
entry-byte totals with an explicit 75% planning density at 4096 rows. They are
allocation projections, not hashed-row packing or guaranteed segment counts.
Compare those directory budgets with each page allocation; the per-archive
replay uses chronological directories.

Shared pages, independent routing, alternate thresholds and smaller geometries
are layout proposals; the tool does not claim current servers implement them.
Compact script/manifest/envelope proposals retain their separate sampled findings.

## Experimental v2x journal (`--v2x`)

`--journal-census --v2x JOURNAL ANCHOR_HEIGHT ANCHOR [SCRATCH_DIR]` reads a journal
written by `event-ingest --txid-display-inputs` (`display-v2x/` sidecars, the
unpublished `transparent-txid-display-v2x` codec in
[the display contract](../../docs/txid-display.md#experimental-input-listing-record-v2x-unpublished)).
The same anchor, lock, eligibility and duplicate-txid checks apply; a v1 journal is
refused in this mode and a v2x journal without the flag.

```bash
nice -n 19 ionice -c 3 cargo run --locked --profile release-fast \
  --manifest-path transparent/tools/txid-sizing/export/Cargo.toml -- \
  --journal-census --v2x /srv/txid-display-genesis/v2x/journal \
  3508673 --anchor-from-journal > /tmp/txid-display-v2x-census.json
```

The JSON has schema `txid-display-journal-census-v2x` and two populations over the
same records: `populations.with_inputs` (v2x bytes) and
`populations.inputs_stripped` (the identical records' v1 encoding). Each carries
the v1 report's `domains` (size distributions, exact-fee scenarios, inline
coverage at 128/192/256/384/512/768/1024, frontiers) and `pages` (per-40,000-record
archive page-row demand p50/p99/max by era, and k=1/4/16/64 shared pages at
256/512/1024/4096 rows). A v2x record's bytes are its v1 bytes plus its encoded
input list, so exact-fee scenarios add the same input bytes. `input_overhead`
reports, per domain, inputs, extra bytes per record (histogram, mean) and encoded
bytes per input. Joint route models are not computed in this mode.

## Required receipt and bounded follow-up

**Display sidecars do not retain shielded-only exclusions or total transaction
counts.** Those output fields are null. Supply the ingest's independently counted
canonical total/eligible/exclusion inventory and source/executable pins before
calling the result a complete canonical transaction-population qualification.
Reading history events cannot repair this gap. This command measures all committed
eligible sidecars, conditional on the ingest's eligibility and completeness.

Roman supplies the completed coordinator JSON and receipt. Then compare the
conservative inline coverage and archive demand with the retained sampled
128-byte recommendation; change that recommendation or geometry only if those
measurements support a different decision. Preserve PR #124's retained inputs
and checkpoints. No gateway crawl, native hardware/concurrency campaign, wallet
recovery or deployment is authorized by this handoff.

Focused validation is the exporter `release-fast` tests, Python sizing tests,
documentation links and whitespace check. The bounded ignored test
`journal_census::tests::profile_synthetic_40000_records` profiles this analysis
algorithm only; run it with `-- --ignored --nocapture`. It neither simulates
coordinator disk throughput nor benchmarks native PIR.
