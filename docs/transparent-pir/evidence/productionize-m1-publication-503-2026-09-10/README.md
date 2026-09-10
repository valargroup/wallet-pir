# M1 public 503 investigation — 2026-09-10

## Investigation plan

1. Preserve the failed canary and identify the request and timestamp.
2. Correlate router reloads, membership changes, activation, SSH failures and worker health.
3. Reproduce transport failure at the membership/activation boundary without live fault injection.
4. Specify a correction that preserves canonicality, readiness and fail-closed routing.
5. Run targeted regression tests and required checks; deploy verified operations code.
6. Verify public service and start a fresh matching six-hour/300-block canary.

## Findings

The `f2f351c` canary failed at 2026-09-09 22:03:05.775 UTC on HTTP 503 from
`https://transparent-pir.valargroup.dev/v1/shards`, after 4,023.457 s and 51 blocks.
There were 57,386 exact queries and 28 retryable responses. Recorded public and
replica visibility maxima were 27.488 s and 40.148 s. No restart or memory-limit
/OOM event was recorded before failure.

At 22:03:04.928 the reconciler removed recent-01 from the current publication's
membership. With no recent member left, it installed the unavailable router
configuration. At 22:03:05.993 the subsequent activation's SSH command failed with
exit 255 and no stderr; activation quorum failed. At 22:03:08.030 the reconciler
re-admitted recent-01, restoring routing. The worker had finished preparation at
22:02:59.473 and kept answering exact queries.

Status and activation used shared multiplexed SSH sessions. The membership path
swallowed its status exception, so the original transport failure's precise cause
cannot be proven from these logs. The evidence identifies the availability failure
chain, not a cryptographic/query correctness failure or an observer false positive.

## Solution plan

- Use independent SSH connections for control operations, isolating them from
  shared transfer sessions and cancellation of other commands.
- Give read-only status one retry on transport failure, with 1 s then 1.5 s
  command budgets inside the existing 3 s membership timeout. Reparse and
  revalidate the returned status; never reuse stale status as proof.
- Do not retry mutating operations whose outcome might be ambiguous.
- Log worker, operation and failure stage without payloads. Log membership
  status failure explicitly instead of discarding its cause.
- Preserve withdrawal when both status attempts fail, status is not warm/current,
  canonicality is unknown, or activation quorum is unavailable.
- Keep observer acceptance thresholds unchanged. A new fleet-script digest
  requires a fresh acceptance result, even with the same worker binary.

## Validation and deployment

The baseline fails the injected transient-status regression; corrected code passes
all 75 transparent operations tests. Tests retain fail-closed behavior for two
failed connections, cold or wrong-publication status, unknown canonicality,
failed activation quorum and reorg withdrawal. Semantic rejection, malformed
status and ambiguous mutations are not retried; cancellation stops retrying.

A read-only probe with corrected code completed two status reads against each of
six live workers. All twelve returned warm, non-invalidated status. The slowest
complete operation took 2.168 s; four initial 1 s connection budgets expired and
their fresh retry succeeded. These are control-read observations, not live load
acceptance. Raw investigation logs and probe results are in `investigation/`.


## Verified correction

Commit `7fedd79` implements the operations correction. `make check` passed 587
Rust tests, zero failures and two ignored manual benchmarks, including the
repository's operations, documentation and report checks. The separately run
transparent operations suite passed 75 tests.

Installed at 2026-09-10 04:22:10 UTC. Fleet script SHA-256 changed from
`f239f4a33dacf464a4492a97b73ccd0118257b705b77ad9ee97efa4d03e28df6` to
`34365b000f598c37e13fcd837434203062df8dd4fdb57cc2f8d53760d99a9d7b`.
Only the preparation reconciler was restarted. The worker binary remains
`c6f169a3bdbd0cc5f110309ac70ee089b9336b507f46493b8fe80aaa51861bea`
(source `f2f351c`); workers and the publication controller were not restarted.
The observer and acceptance thresholds were not changed.


## Fresh canary

After installation, both public map origins returned matching publications at
height 3,478,132; recent-01 was warm 28/28 on the selected binary. The fresh gate
began at **2026-09-10 04:22:46 UTC** under `transparent-m1-control-rollout.service`.
Its coordinator output is `/opt/transparent-publisher-build/control-20260910/rollout`.
It uses `--observe-installed-canary` because only operations code changed; the
acceptance samples and fleet-script identity are new. M1 remains incomplete.


## Reorg interrupted the first corrected run

The 04:22:46 run published block 3,478,133 in 16.453 s. At 04:24:01 the node
replaced block 3,478,134 at the same height. The observer recorded
`chain_reorganized`, reset the block count and then failed on the required public
withdrawal's HTTP 503. The controller rejected obsolete preparation and exposed
canonical replacement coverage at 04:24:37.716, recording 37.613 s freshness.
This is a separate reorg path, not evidence of recurring membership withdrawal
from one failed status read. `reorg-run.tar.gz` preserves that run and correlated
controller/reconciler logs.

The observer was not changed to ignore the outage, and this run is not accepted.
Reorg recovery took longer than the ordinary 30 s budget; this evidence does not
claim that recovery meets an ordinary-block SLO. After checking canonical public
coverage and 28/28 warm readiness, a new strict run was launched under
`transparent-m1-control-postreorg-rollout.service`, with output at
`/opt/transparent-publisher-build/control-20260910/postreorg-rollout`.
Neither previous run contributes samples. A later reorg can still stop the strict
gate; this correction does not change that observer policy.


At the 2026-09-10 04:28:39 UTC handoff check, the new strict run (started at
04:26:57 UTC) was active, with one new block publicly visible in 16.140 s,
1,470 exact queries and one retry. `handoff-status.json` records that check;
`canary-start/` is an earlier start snapshot, not a completed acceptance report.
