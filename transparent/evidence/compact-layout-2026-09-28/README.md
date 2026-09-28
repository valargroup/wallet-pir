# Compact schema v10 source qualification, 2026-09-28

Schema v10 retains 4,096-byte rows and the version-2 logical event journal. It
uses 51-byte receive records, 79-byte spend records and 43-byte spend records
when the immediately preceding receive supplies the outpoint. Variable directory
entries, byte-bounded fragments and deterministic mixed-size packing share one
capacity calculation between the builder and sealer. The wallet checks canonical
encoding and greedy fragment boundaries, including after a SQLite restart.

## Measured sample

The retained mainnet day (`../baselines/mainnet-study/mainnet-day.jsonl.gz`,
SHA-256 `4d34d011d5190195e030175da1e4f04cdc88bddb4788d4d1f0ede794a536689a`)
contains 1,152 blocks, heights 3,470,268–3,471,419, 70,988 events and 13,394 scripts.
The actual v10 builder used **848 page rows versus 1,185 under the v9 model**:
28.44% fewer required rows, or **39.74% more event capacity per required page row**.
Directory entry bytes fell from 2,571,648 to 1,521,703 (40.83%). Every one of the
13,394 decoded histories matched its logical input. See [sample-builder.json](sample-builder.json).
Both layouts still allocate one fixed-size segment per table for this small
sample; this is not a claim of reduced allocated fleet storage. A coverage-matched
full-journal census is recorded separately when complete.

## Validation

- Core shard, wallet and SQLite suites: 169 tests passed.
- Filter and shard service suites passed apart from three HTTP timeouts in the
  first concurrent run. The serial wallet suite then passed 31 tests, with one
  existing timing test ignored. The failed attempt is retained.
- The added native-PIR test reconstructs local outpoints over two page fragments,
  shares the long history's tail row with a short history, and checks exact ledgers.
- Linux release wallet suite: 31 passed, one existing ignored; added local-outpoint
  test: one passed. These runs use the same source overlay on an isolated target.
- Regression HTTP integration: three passed. Journal-seed test: one passed.
  A first macOS regression process stalled in dyld before its harness; the sample
  and failed attempt are retained alongside the successful rerun.
- Clippy across the changed crates and services passed with warnings denied.
  Operator jq payload contracts passed.

## Tradeoffs and qualification boundary

Variable records cost parser and placement complexity. Greedy packing depends on
receive/spend composition and local adjacency, so the savings are workload-dependent.
The bounded directory relocation can miss a feasible arrangement and spill into
another segment. Fragment counts can reveal coarse history composition through
query counts, as accepted in the protocol contract. Exact byte-aware packing also
costs builder/sealer CPU; these results do not establish a publication-speed gain.

Schema v9 clients fail closed on v10. A separate publication, actual-table native
correctness certificates, coordinated service/client cutover, and production
recovery/load checks are separate from this source qualification. At creation of
this record no v10 production cutover had been performed. Existing journal data
is not rewritten.

## Provenance

[manifest.json](manifest.json) describes the run. Logs retain unsuccessful attempts.
The census source bundle and its file checksums preserve the exact independent
read-only replay runner used before the final source commit. Its production shard
code matches this implementation; subsequent differences are comments, formatting
and tests. The census reads a pinned committed prefix and checks every v9 shard's
terminal hash before replay. It never opens the journal writer lock.
