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
for every opening, including inline transactions, plus two lookup queries even
when the hashed choices coincide; records exceeding three still
reveal their excess. Hash lookup and hash overflow use different domains; broad
overflow means a million-height time bucket. Frozen revision/no timing is an
assumption, not evidence that a real observer lacks timing or history transcripts.
The 50,000-height temporal routing is an analytical stand-in, not a replay of the
live shard map. Minimum populations 1000/10000 are policy tests, not guarantees.

The helpers load a bounded JSON dataset in memory. They are not yet a streaming
17-million-record census pipeline. Complete-data export, chain continuity,
large-scale packing and observed request workload are next gates in
[remaining work](../../docs/remaining-work.md#txid-display-sizing-and-independent-routing).


## Anchored probability sample and streaming stage

`census.py` pins the canonical archive anchor and a reproducible, disjoint
nine-stratum SRSWOR block plan (256 blocks per stratum). It owns one persistent
SSH gateway, executes only allowed read methods sequentially with a 40/s ceiling,
resolves full external parent transactions into an on-disk SQLite cache, writes
atomic per-block public raw checkpoints **outside the repository**, and rechecks
all sampled block hashes plus the anchor after acquisition. It never reconnects
a refused/unreachable session. The required credential environment variable is
used only for a mode-0600 temporary file in `/tmp`, erased when the session closes.
Never include the value in arguments, logs, evidence or a repository file.

```bash
../../tool-exec --repo wallet-pir -- python3 transparent/tools/txid-sizing/census.py \
  /outside/repository/census --status /outside/repository/status.json
../../tool-exec --repo wallet-pir -- cargo build --locked --offline --profile release-fast \
  --manifest-path transparent/tools/txid-sizing/export/Cargo.toml
../../tool-exec --repo wallet-pir -- python3 transparent/tools/txid-sizing/survey.py extract \
  /outside/repository/census /assigned/target/release-fast/txid-sizing-export
```

The exporter `--stream` protocol takes one block-bounded JSON frame per line:
`height`, `hash`, `raw_block` hex and `parents` containing requested `txid` and raw
`hex`. Each frame is capped at 256 MiB. It validates canonical block/parent hashes,
rejects trailing bytes and missing prevouts, uses the shared extractor/encoder,
and emits one complete canonical record inventory per block. Python independently
cross-checks RPC transaction identities, exact amounts/scripts and eligibility,
Rust/Python display bytes and transparent-only fee arithmetic. Global sampled
transaction deduplication and raw checksums are checkpointed on disk.

`survey.sufficient(checkpoint)` compacts verified per-block histograms and
per-stratum routing moments; `survey.report(data)` deterministically derives HT
population totals, linearized ratios with finite-population correction, weighted
frontiers and approximate inverted pointwise CDF intervals. Confidence units are
blocks, not transactions. Sparse/extreme-tail normal intervals are limited;
zero-observed tails do not establish absence. Aggregate route counts intersect
all modeled transcript fields and deduplicate real txids before estimation.
Sampled minima and estimated observed-class counts **cannot qualify the true
minimum** or classes absent from the sample. Frozen segments, refresh/timing
cohorts and uniform openings remain explicit analytical assumptions.

`geometry.survey_projection(report)` budgets independent lookup/overflow
geometries using the survey estimates, separately for implemented display-v1 and
compact proposals. It does not claim a full-chain packing replay or native RSS/
latency. The old bounded vector reports remain retained as conformance oracles;
they are never pooled into probability-population estimates.


## Retained prior full-chain pipeline (inactive in the one-day study)

`fullchain.py` is the full-chain acquisition path. Its raw preflight verifies
retained inputs. The inherited raw-only preflight used height-string
`getblock(["height", 0])`, a 200-request window and a paced 45 requests/s limit.
New acquisition follows the required height/hash pin followed by raw block fetch
on the same session and shared request ceiling. The fixed anchor remains height 3,502,662,
`0000000000228603173bfeb3650b51ceb92b1ac6b8b6392fa1fa9e9bac3acf7f`.
Raw binary bundles, per-block checksums, SQLite state and checkpoints live in
`/home/ai-dev/.cache/wallet-pir-census/`; only sanitized receipts belong in Git.

The Rust exporter's `--census DATABASE` mode checks canonical raw parsing,
height/parent continuity, mainnet genesis and coinbase placement. It resolves
inputs sequentially from disk, deletes spent outputs, preserves exact output
bytes and checks every transaction identity, including shielded-only exclusions.
The canonical genesis output is included in display records and excluded from
spendable state, following the pinned state implementation. Missing prevouts
retain eligible display records with shared unknown fees; exact zero and
coinbase non-applicable fees stay distinct. Normal extraction uses the existing
extractor and shared value-balance fee logic. Compact codec sizes remain
analysis proposals, separate from implemented display-v1.

The retained raw preflight reached 44.51 blocks/s; this is acquisition throughput,
not full-chain completion. The verified resume checkpoint contains 27,812 parsed blocks, an incomplete
prefix. Full coverage and anonymity conclusions remain unqualified until the
anchored scan and final aggregate/replay validation finish.

The resumed acquisition pins each height using `getblockhash` before fetching
`getblock(hash, 0)`. Both phases share a 45 request/s ceiling on one SSH session.
The first 3,000 new blocks measure end-to-end raw acquisition, canonical extraction
and checkpoint persistence; subsequent batches contain 10,000 blocks. The prior task used a 72-hour projection stop. That historical procedure is
not inherited by the one-day study, and `fullchain.py` is not run here.
`resume-throughput.json` records this measurement separately from the inherited
raw-only preflight. Verified retained raw blocks are reused without refetching.


## One-working-day parent-free study

`day_sample.py` selects 1,024 blocks by SRSWOR in each of nine disjoint strata
across genesis through the fixed anchor. Its separate seed, exact selected
heights and inclusion probabilities are retained. It opens one existing gateway
session at <=40 requests/s after checking process identity, uses height/hash pins,
raw and verbose blocks, and rechecks every selected height/hash plus the anchor.
It requests no parent transactions, does not change services or access, and never
writes the previous census cache. Credentials use the inherited temporary-key
lifecycle outside Git.

`--shape-stream` canonically parses every transaction and includes all any-input/
output transactions. Shared flags and the current display encoder are authoritative.
Coinbase is non-applicable; no-transparent-input eligible transactions reuse the
canonical extractor for exact fees. Other fees remain unknown. Their hypothetical
exact-fee population sizes add 1..8 actual LEB128 bytes, bounded by MAX_MONEY;
unknown records are never labeled actual exact-fee measurements.

`day_analysis.py` independently checks RPC identities, input counts, shielded
presence, exact values/scripts and Rust/Python codec bytes. It uses block-cluster
HT totals, ratios and FPC uncertainty. The smallest of seven prespecified cutoffs
is eligible for recommendation only when both the conservative one-sided
Bonferroni normal lower bound and the stratified rescaled block-bootstrap
Bonferroni lower bound exceed 80%. These are asymptotic sizing estimates, not formal guarantees.

Fee-size ambiguity is retained in joint routing as guaranteed/possible distinct
candidate membership. Three page requests and two initial queries cover ordinary
count differences, while excess pages, narrow lookup, refresh/time and unmodeled
history remain limitations. Uniform segment vectors are an analytical assumption;
no actual full-chain layout, true anonymity minimum or native hardware capacity
is claimed. Per-record packing bounds explicitly handle the directory-byte drop
when a record crosses the inline boundary.

```bash
../../tool-exec --repo wallet-pir -- python3 transparent/tools/txid-sizing/day_sample.py \
  /outside/git/day-sample --status /outside/git/status.json
../../tool-exec --repo wallet-pir -- cargo build --locked --offline --profile release-fast \
  --manifest-path transparent/tools/txid-sizing/export/Cargo.toml
../../tool-exec --repo wallet-pir -- python3 transparent/tools/txid-sizing/day_analysis.py extract \
  /outside/git/day-sample /assigned/target/release-fast/txid-sizing-export --status /outside/git/status.json
../../tool-exec --repo wallet-pir -- python3 transparent/tools/txid-sizing/day_analysis.py summarize /outside/git/day-sample
../../tool-exec --repo wallet-pir -- python3 transparent/tools/txid-sizing/day_analysis.py statistics \
  /outside/git/day-sample /outside/git/day-statistics.json.gz
../../tool-exec --repo wallet-pir -- python3 transparent/tools/txid-sizing/day_analysis.py bootstrap \
  /outside/git/day-statistics.json.gz /outside/git/day-bootstrap.json
../../tool-exec --repo wallet-pir -- python3 transparent/tools/txid-sizing/day_analysis.py report \
  /outside/git/day-statistics.json.gz /outside/git/day-analysis.json --bootstrap /outside/git/day-bootstrap.json
../../tool-exec --repo wallet-pir -- python3 transparent/tools/txid-sizing/day_analysis.py geometry \
  /outside/git/day-analysis.json /outside/git/day-geometry.json
```

Raw source frames and canonical record inventories are retained outside Git and
checksum-pinned. Only sanitized compact statistics, public source inventories,
reports and provenance belong in the evidence directory. Prior vector and prefix
reports are not pooled into probability estimates.
