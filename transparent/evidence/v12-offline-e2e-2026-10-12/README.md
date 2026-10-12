# v12 offline end to end on the hub, 2026-10-12

A `transparent-shard-v12` set published from the read-only recent journal slice
on the development hub, verified, served locally, and every sample wallet the
slice covers recovered over HTTP. **Development host only: nothing deployed,
no production host touched.**

## Provenance

| Item | Value |
|---|---|
| Source | `v12/decoupled-pages` at `b255c1ed` (the commit that adds this directory adds only it, the sample-recovery example and docs) |
| Journal | the recent slice of the v3 journal, heights 3,262,749–3,473,686 (210,938 blocks, 4,185,700 events), anchor `0000000000755137…0be1d`, on the hub at `ai-dev/evidence/decoupled-pages-journal/recent`; provenance in [the step-1 census](../decoupled-pages-prototype-2026-10-11/README.md). Linked into a scratch directory, never written. |
| Parent block | 3,262,748 = `0000000000b5d4b7…99b4176a`, as recorded by [continuous-publication-2026-09-08](../continuous-publication-2026-09-08/) |
| Workload | the frozen [2026-09-08 sample](../workload-sample-2026-09-08/README.md), restricted by [slice-sample.py](slice-sample.py) to the 716 wallets whose whole range lies in the slice ([sample-recent-slice.json](sample-recent-slice.json)); restore-old and unused start at genesis and are out, and so are 14 small-active and 110 reused-tail wallets that begin before the slice |
| Digests | the sample's digests are over an older event codec; `sample-redigest` re-derived them from the slice and refused unless every wallet's event count was unchanged: all 716 counts equal ([sample-recent-slice-redigested.json](sample-recent-slice-redigested.json)) |
| Command | [run.sh](run.sh) `SLICE WORK`, `release-fast`, rustc 1.97.1, on roman-dev-2 |

## Results

**Publication** ([publication.json](publication.json), [publish.log](publish.log),
[shards.json](shards.json)). `shard-publish` at `decoupled-2k-8k` with
`--directory-choice all`, as production publishes: 8 directory shards (7
sealed, sealed by directory bytes, plus the tail) holding 11 page shards, in
42 s with 309 MB peak RSS. `shard-verify` loaded the set, checked range,
anchor and map, and rebuilt 6 of the 8 directory shards from the journal at
their published page-shard boundaries: filter, directory and every page shard
byte-identical ([verify.json](verify.json)).

**Recovery** ([wallets.ndjson](wallets.ndjson), [results.json](results.json)).
`sample_recovery` (an example of `transparent-loadtest`) synced each wallet
once from its `required_from` into a fresh store, eight at a time, against
the local `transparent-shard-server`, using the wallet crate's HTTP adapters
and 44-bit queries. **716 of 716 wallets are exact**: the events each store
holds over its range digest to the expected value, with the expected count.
243 syncs report `Incomplete { UnresolvedSpends }`: they spend outputs
received before the slice begins, which a set starting at 3,262,749 cannot
hold. That is coverage, not retrieval, and leaves the event digest unchanged.
All 4 min 33 s, client peak RSS 264 MB.

Bills: every byte a wallet's sync moved (map, filters, manifests with choice
tables, setups, queries both ways), MB p50 / p95 / p99, against the census's
one-shot bill for the same wallets
([decoupled 2k/8k census log](../v12-census-2026-10-12/raw/extra-2k/), priced by
[bill-table.py](../decoupled-pages-prototype-2026-10-11/bill-table.py)):

| Class | Wallets | Measured | Census | Page queries p50 / max |
|---|---:|---|---|---|
| catch-up-1d | 120 | 0.34 / 0.41 / 0.54 | 0.32 / 0.40 / 0.56 | 1 / 5 |
| catch-up-7d | 120 | 0.25 / 0.41 / 0.41 | 0.23 / 0.40 / 0.40 | 0 / 6 |
| catch-up-30d | 120 | 0.44 / 0.74 / 1.16 | 0.40 / 0.73 / 1.28 | 0 / 18 |
| restore-6m | 120 | 1.66 / 2.26 / 2.43 | 1.55 / 2.12 / 2.35 | 1 / 9 |
| multi-script | 120 | 4.75 / 6.46 / 10.47 | 4.60 / 6.48 / 11.16 | 13 / 980 |
| small-active | 106 | 0.83 / 1.29 / 1.38 | 0.78 / 1.21 / 1.31 | 0 / 1 |
| reused-tail | 10 | 6.35 / 54.29 / 75.51 | 5.53 / 82.56 / 82.56 | 63.5 / 1,042 |

Measured bills sit within about 7% of the census at the median, a little
above it because they include the map and the whole manifests the census does
not price. These are one-shot recoveries from `required_from`; the run-cache
catch-up is covered by `a_tip_catch_up_stops_at_the_first_cached_run` in the
server suite, not measured here.

## Limits

- One slice, one local server, no load: this establishes correctness and
  per-wallet bytes, not latency or capacity.
- The archive tier is not exercised: the archive slice (heights
  200,000–419,999) starts mid-chain and holds no sample wallet.
- The v12 regression and canary fixtures were not exported: `regression-export`
  requires a journal from genesis, which only the coordinator holds.
