# Receiver publication cache

Measured September 28, 2026 on an Apple M3 Ultra. Implementation is
`8bfb120c211d2674fbf0a49f4d8455a0f54e6790`, the direct child of the merge
`2f99300debde3d65a0c0d263a5129042eddf12ab` of main at
`5337a43c619039a39c35b54d9251a5131ebb93ba` into the baseline
`207053dc02453d43ebfd3ba60c758f99f848c509`. This is local validation, not a
deployment.
The run metadata is in [`manifest.json`](manifest.json). `SHA256SUMS.original`
is the capture's checksum manifest, from before this note's navigation edits;
`SHA256SUMS` hashes the current files.

This record is historical. The `witness_refresh` example and the opt-in mainnet
tests named below existed at the implementation revision but are not part of the
current tree; reproduce the run from that revision. The serving daemon then wrote
publication files and reloaded them before serving; it now prepares publications
in memory, and `serve.log` keeps its key=value logs from before the move to
`tracing`.

## Matched witness comparison

The frozen public index covers heights 3428143–3497894, with 623845 commitments
and 20916 payment records. The final block adds eleven commitments. The source
file hash and publication identity are in `results.json`. No wallet data is used.

The `witness_refresh` example (at the implementation revision) rewinds an isolated
copy by one block, warms the
cache at the previous height, re-appends the same block, and alternates the order
of full versus cached preparation. Both paths use the same database, manifest,
compiler profile and commitment hashing. Each trial requires byte-identical
witness output. Preparation includes loading commitments and payment positions
from SQLite, building witnesses and encoding them.

| Trial | Full preparation | Cached preparation |
| --- | ---: | ---: |
| 1 | 13.404 s | 86.186 ms |
| 2 | 13.354 s | 84.250 ms |
| 3 | 13.447 s | 85.337 ms |

Median witness preparation fell from 13.404 seconds to 85.337 milliseconds.
The proof stayed 4034558 bytes, SHA-256
`162fb3ee81090a3b5a050290a20a9a506a3da87e6c80ba1fe433438cfa995f7b`.
Cold cache preparation still took 13.33–14.45 seconds. The cache saves hashing
on subsequent publications, not on startup. Comparing and loading all stored
commitments remains linear in history size.

## Complete publication check

The final committed daemon was run locally on a second isolated index copy.
A loopback RPC proxy exposed a controlled tip while forwarding block/hash reads
to the canonical mainnet node. It advanced from 3497893 to 3497894 immediately
after the first publication became available. The daemon used its ordinary
10-second poll interval, zero confirmations, witnesses and 8192 PIR rows.

The new publication became available over HTTP in **12.400 seconds** after the
controlled tip advance. Processing through activation took **2.367 seconds**.
Witness preparation was 104.680 milliseconds. Ingestion, directory construction,
file writes, PIR preparation and activation timings are in `serve.log`.
The served proof matched the full-build reference hash above, and both row files
matched their manifest SHA-256. No daemon restart was needed for the update.

This includes local polling and RPC round trips. It is not actual block-arrival
latency on the deployed server, nor wallet retry or end-to-end restore time.
The first local publication took 12.651 seconds in this separate run. Cold-cache
figures vary with concurrent host load and should not be interpreted as a
steady-state speedup.

## Validation and release boundary

Twelve directory/cache tests passed, including byte comparisons for appends,
odd pairs, power-of-two boundaries, rewinds, same-size and shorter replacement
forks, malformed inputs, changed receiver sets, database reopen and stale anchors.
Four receiver HTTP tests passed, including common witness binding and reorg
publication fencing. Two opt-in mainnet tests were not run in that suite.
Locked daemon check/build and directory Clippy with warnings denied passed.

Read-only live logs still showed roughly 27-second publications on the existing
4-vCPU service. That service was not changed. Verify stage timings and external
availability on the deployed hardware after an approved deployment before claiming
its under-20-second target is met. A cold start still does a full tree build.

The existing common-witness size, memory growth, omission trust and wallet restore
scaling limits remain separate work. These measurements do not cover 50-address
wallet recovery.
