# Enhance production readiness checks — September 23, 2026

**Disposition: not qualified for the pilot.** This record accompanies the
[2 QPS pilot gates](../../docs/pilot-readiness.md). It records completed checks
and preserves failures; it is not an approval to admit wallets.

## Current candidate and live qualification (September 23, 23:59 UTC)

The coordinator and both physical workers now run the uniform server/load
source revision `527048217f8cb83d27c838e94dbaac21ac25837d`; the deployed
Linux server binary SHA-256 is
`3c10840380c306b0564b4a171d81f51c3ca35089fbc624cba7c7e9b4198e3b29`.
The previous releases and data remain available for rollback, and cleanup
timers remain disabled. Commit `7ca1988` changes the journal append recovery
path after that deployed revision: an uncommitted record suffix is now trimmed
to the manifest's committed length before appending. The running public load
still measures the earlier deployed binary. It remains useful diagnostic
evidence, but it cannot qualify the updated server candidate; rebuild and
repeat affected production gates after deploying that exact new binary.

The fully observed public HTTPS run has passed its 30-minute 1, 2 and 4 QPS
stages: 1,800, 3,600 and 7,200 exact answers respectively, with zero request
errors or missed arrivals and scheduled p99 of 252.671, 393.983 and
233.983 ms. Its six-hour 4 QPS soak is running, followed by a five-minute
eight-client burst. Both worker samplers and the coordinator freshness
observer started before the relevant full stages; the first full soak hour
passed the conservative freshness assessment with 42 bounded tip advances,
70.001-second worst lag and no findings. These public answers still use a
journal-derived oracle. Full worker traces and the complete six-hour freshness
window must be assessed after the run.
The [first 90-minute assessment](soak-first90min-freshness.json) also passed:
57 bounded tip advances, 70.001-second worst lag and no findings. Its copied
trace digest is recorded in the report; the continuous worker-local and
coordinator traces remain on their hosts until the run completes.
The [first three-hour assessment](soak-first3h-freshness.json) passed with
132 conservatively bounded tip advances, 70.001-second worst lag, and no
findings. This still does not cover the remaining soak, burst, or worker
hardware traces.
The [first four-hour assessment](soak-first4h-freshness.json) bounded all 176
observed tip advances within 70.001 seconds, with no ambiguous advances or
sample gaps. It remains `evidence_incomplete` because the observer recorded a
brief publication block during the same-height fork documented in the
[reorg incident](operations/soak-reorg-2026-09-24.md). The full load report,
canonical-hash check and independent public answers are still required.

Fresh rc.2 ARM64 and native Linux noise matrices each completed 432 cases and
55,296 queries with zero decoding failures. Deterministic cross-platform
comparison, independent numerical review and a current published snapshot
check remain open. The pinned PR #29 wallet client was built from a clean
checkout on Linux with no packages outside its lockfile; it has not yet queried
production with a chain-derived anchor. The portable APM update is installed,
with the old binary retained; after restart it reported both private workers
reachable and zero active alerts. Its build and rollout checks are recorded in
[portable tool validation](portable-tools-validation.txt).
The [rc.2 review packet](numerical-review-packet.md) identifies the exact
matrix inputs, source hashes, assumptions and required independent checks.

The remaining gates include the completed public soak and burst, independent
chain extraction and wallet release recovery over HTTPS, full hardware active
and six-sealed campaigns, recovery/alert exercises, independent cryptographic
review, and the 24-hour opt-in observation. The two serving workers cannot host
either isolated campaign concurrently with canonical serving. The required
active and six-sealed profiles need at least 12 hours of interrupted serving
when run sequentially on these workers, plus setup and restoration. The
[existing-worker campaign plan](operations/existing-c4-campaign-plan.md) is
prepared but not dispatched; no extra workers have been provisioned and no
production outage has been scheduled.
The [provider inventory](operations/c4-provider-inventory.json) records the
two serving hosts as the only c-4 droplets at its observation time.

The sections below preserve earlier checks and failures in their original
context. Their earlier fleet revisions and incomplete-run statements do not
describe the current deployment.

Implementation started from `8b9bacf33c49522be1b1d437566f34cd33535d7d` in an
isolated worktree. Commit `69840f3df91ebde3f893a77d867e40e8088a8ced` repaired the
wallet/noise tools and added the public qualifier and successful latency fields.
Commit `73233a1a6f44e4149bcf387f826ed3ba40406bc7` corrected the open-loop measurement
window. The [manifest](manifest.json) records identities and outstanding gates.

## Completed local checks

- [Repository checks](make-check.log): `make check` exited zero, including
  633 passing Rust tests and five ignored tests. This ran before the final
  load-driver interval adjustment; [targeted tests](load-tests.log) and
  [clippy](load-clippy.log) cover the load-driver changes.
- [Wallet HTTP/SQLite](wallet-interop.log): all three tests passed with wallet
  `9b190657d129d08e964623d0ecc1d8e4ffb31b1d`, including actual note recovery,
  atomic corrupted-record rejection, expiry and fresh generation acceptance.
- [Independent q48 SDK](q48-interop.log): all 12 allocation cases passed. This is
  a wire/engine test, not an actual wallet release over public HTTPS.
- [Operations](ops-tests.log): 100 tests, three skipped. The new public qualifier
  rejects missing/slow latency, wrong answers, incomplete demand and hidden failures.
- [Noise verifier](noise-verifier-tests.log): nine tests passed; [Rust tests](noise-rust-tests.log)
  and the explicit [full-degree reference equivalence](noise-equivalence.log) passed.

The additional [full-size loan/return test](full-shard.log) passed in 139.32 seconds,
including retained queries and reorg/alternate-branch recovery. The six-sealed
consolidation test was still running at this initial capture and later passed
locally. Final helper checks have
[101 operations tests](ops-final-tests.log), three skipped, and
[10 noise-verifier/comparison tests](noise-verifier-final-tests.log), all passing.
The [release build](release-build.log) completed; [binary hashes](release-binaries.json)
identify the ARM64 artifacts, not production Linux binaries.

## Historical noise matrices

The previously unfinished documentation lagged the completed raw runs. Reverification
checked every case hash, executable identity and analytical result:

- ARM64: 432 cases and 55,296 queries, zero failures.
- Native Linux: 288 cases on `roman-ipir-bench-8vcpu` plus 144 on
  `roman-pir-avx512-test`, 55,296 queries total, zero failures.
- All cases meet the original 78-bit sufficient bound; all 432 deterministic
  inputs, public setup values and grouped moments match across platforms.

[Reverified case identities](noise-historical-reverified.json) and
[cross-platform comparison](noise-historical-cross-platform.json) retain exact
hashes. Raw ARM results remain in `/tmp/p16-q48-arm64`; Linux originals remain in
`/root/p16-q48-results-s01` and `/root/p16-q48-results-s2` on their respective hosts,
with local copies under `/tmp/wallet-pir-readiness/noise/`.

These are the historical `6f74a2d7` results. They do not qualify the later rc.2
executable. A fresh rc.2 ARM run uses
`/tmp/wallet-pir-readiness/noise/rc2-arm64`; current matrix/snapshot acceptance
must be recorded separately after completion. The verifier does not authenticate
extraction or grant cryptographic review.

## External HTTPS smoke and retained failures

Both smoke runs originated on the macOS operator machine through the public
HTTPS origin, using nine records from the canonical journal. That oracle checks
retrieval against ingestion output, not independent chain extraction. Local
qualification work was concurrent, and the second run overlapped a snapshot
transfer; neither run is an uncontended sustained capacity result.

| Run | Correct / offered | Request errors | Successful p99 | Result |
|---|---:|---:|---:|---|
| [Initial](public-smoke/rate-1.json) | 30 / 30 | 0 | 1,455.103 ms | Failed latency and exposed short measurement window |
| [Interval corrected](public-smoke-interval-fixed/rate-1.json) | 30 / 30 | 0 | 1,517.567 ms | Failed latency; full 30.027-second window recorded |

The qualifier correctly stopped after the 1 QPS stage. No 2/4 QPS or soak result
is claimed by these two historical smoke runs. Neither recorded 429, 502,
wrong answers or warmup failures.
The initial driver's last arrival completed before 30 seconds; the corrected
driver includes the complete requested interval in throughput and duration.
The one-second p99 threshold remains unchanged.

## Captured production snapshot

The rc.2 extractor checked generation 116, shard 0, containing 586,953 records.
Every padded unit hash matched the captured manifest, and the reconstructed
public setup matched its published digest. All 128 fresh queries decoded exactly;
the analytical result met the 78-bit target. See the [capture identity](snapshot/capture.json),
[manifest](snapshot/manifest.json) and [summary](snapshot/summary.json.gz). The
compressed raw numerical result is retained alongside the summary. The full
record file remains outside Git at the path and hash in the capture identity.
This result covers the captured generation, not future publication or an
independent review of the extractor.

The [campaign progress snapshot](current-campaign-progress.json) records
partial rc.2 ARM64/Linux coverage at its capture time. Both matrices later
completed as described in the current-candidate section above; this frozen
partial snapshot is retained as historical evidence.

## Read-only fleet checks and rollback retention

Before the uniform deployment, the coordinator ran
`682ee124621fb155033200c6a1565a8cae75c863`; both workers ran
`afdb4b6d70d6947fa60688a3cf4df21fe5eeae69`. Those were not the uniform
candidate required for final acceptance. Both workers reported zero restarts,
zero current swap and more than 32 GiB free disk at those sampled points.
This is a historical spot check.

[External probes](public-worker-ports.json) timed out on both workers' public
port 8091. Provider rules restrict 8091 to the coordinator tag. Coordinator
listeners for PIR, RPC and APM were loopback-only; public node P2P remains separate.
The later [public edge probe](operations/public-edge-2026-09-23.txt) verified
TLS hostname validation and that `/metrics` and `/ready` return 404 externally
while the intended health and APM health routes remain reachable.

The old-release retention timers would delete rollback state on September 24 at
17:05 UTC, before a new qualification plus 24-hour pilot window could finish.
They were disabled and verified inactive on the
[coordinator](operations/coordinator-retention-timer.txt),
[first worker](operations/worker-1-retention-timer.txt) and
[second worker](operations/worker-2-retention-timer.txt). At that check, no
serving process, binary, canonical state or worker data was changed. Re-enable
cleanup only after
rollback retention is no longer required by the final acceptance decision.

Full hardware campaigns require isolated workers or an explicitly planned
maintenance window. The current provider inventory contains only the two serving
c-4 workers. Current-snapshot qualification, deterministic matrix comparison,
wallet release acceptance, sustained physical-worker traces, fault/alert
exercises and pilot observation remain separate gates.
