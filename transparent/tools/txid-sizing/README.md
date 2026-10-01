# Offline txid sizing and routing intersection tools

These are analysis helpers, not a production codec, migration, or PIR client.
Read the [findings](../../../docs/transparent-txid-sizing-and-anonymity-findings.md)
and [source/discovery pins](../../evidence/txid-sizing/sources.json) before interpreting
results. `UNQUALIFIED` is intentional. No secrets or encrypted query bodies are inputs.

## Reproduce on this hub

Run in the assigned wallet-pir checkout through its existing `../../tool-exec`.
Use the pinned Zakura source cache or a scratch directory containing the 41 exact
public text vectors named by `sources.json`. Raw inputs remain immutable in the
pinned upstream Git commit and the existing read-only cache. `fetch_vectors.py`
verifies every text checksum and selection, and can fetch just those public files
with `--fetch`; it never contacts a production node. The exporter records both
text and decoded-block checksums, block hashes, inclusion counts and exclusions.

```bash
../../tool-exec --repo wallet-pir -- python3 transparent/tools/txid-sizing/fetch_vectors.py \
  transparent/evidence/txid-sizing/sources.json /tmp/txid-sizing-vectors --fetch
../../tool-exec --repo wallet-pir -- cargo run --locked --offline --profile release-fast \
  --manifest-path transparent/tools/txid-sizing/export/Cargo.toml -- \
  /tmp/txid-sizing-vectors /tmp/txid-sizing-canonical.json
../../tool-exec --repo wallet-pir -- python3 transparent/tools/txid-sizing/analyze.py \
  /tmp/txid-sizing-canonical.json /tmp/txid-sizing-analysis.json
../../tool-exec --repo wallet-pir -- python3 transparent/tools/txid-sizing/geometry.py \
  /tmp/txid-sizing-geometry.json
../../tool-exec --repo wallet-pir -- python3 -m unittest discover \
  -s transparent/tools/txid-sizing -p "test_*.py"
../../tool-exec --repo wallet-pir -- python3 transparent/tools/txid-sizing/verify_evidence.py
```

Omit `--fetch` to verify an existing source directory offline. Cargo needs its
locked dependencies fetched; this hub already has them. Retain the same target
lane and keep Cargo writers sequential. Do not change the root workspace/packages.
The standalone exporter directly compiles the existing canonical `extract.rs`,
uses shared `TransactionMetadata` and the implemented display-v1 encoder/packer,
and resolves prevouts only from full outputs in the chosen blocks. Missing
external prevouts produce exclusions, never guessed fee metadata. This bounds
work to 354,315 raw block bytes and 158 transactions with no production RPC crawl.

`analyze.py` verifies every Python display-v1 byte against the Rust extraction
and tests implemented packing against the Rust table builder. Other thresholds,
compact codecs/envelopes, independent placement, and geometry sweeps are explicit
counterfactuals. Row widths remain 4096. Percentiles use nearest-rank order
statistics. Packing uses deterministic sorted txids, two hashed choices in bucket
0, and next-fit page fragments; only the 128-byte display-v1 run is implemented.
Distinct txids, outputs, fragments, occupied rows, and encrypted requests are
separate measures. Segment evaluations multiply responses/work, not uploads.

`geometry.py` sweeps illustrative population, coverage, lookup/overflow bucket
counts, segment geometries and count cover. Its 17-million input is an assumed
scale, **not** a display census. Change assumptions only in a new report with
explicit provenance. The scan-byte proxy is neither measured CPU time nor latency.

Routing classes intersect lookup bucket, overflow route, revision/time cohort,
segment vector, observable request counts, and optional timing cohorts. Only
real distinct txids contribute. `cover_3` means at least three page row requests
for every opening, including inline transactions; records exceeding three still
reveal their excess. Hash lookup and hash overflow use different domains; broad
overflow means a million-height time bucket. Frozen revision/no timing is an
assumption, not evidence that a real observer lacks timing or history transcripts.
The 50,000-height temporal routing is an analytical stand-in, not a replay of the
live shard map. Minimum populations 1000/10000 are policy tests, not guarantees.

The helpers load a bounded JSON dataset in memory. They are not yet a streaming
17-million-record census pipeline. Complete-data export, chain continuity,
large-scale packing and observed request workload are next gates in
[remaining work](../../docs/remaining-work.md#txid-display-sizing-and-independent-routing).
