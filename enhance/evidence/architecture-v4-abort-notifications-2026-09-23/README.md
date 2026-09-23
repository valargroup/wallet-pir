# V4 abort-notification recovery — September 23, 2026

The controller now persists abort delivery before releasing a failed attempt's
candidate slot. Unacknowledged workers keep their candidates and reservations,
and receive neither a new candidate nor a newer retention set. Healthy peers may
continue ordinary publications. New-group and elective-move replication rules
remain enforced. Later cancellations preserve earlier pending decisions.

Successful publication also atomically queues aborts for affected participants
excluded from activation readiness. This prevents an accepted reservation with
a lost response from falling out of recovery accounting. Worker aborts fence
late reservations, and a lost abort response leaves an idempotent retry pending.

## Fault test

The real encrypted HTTP test
`abort_recovery_fences_ambiguous_reservations_without_blocking_healthy_peer`
passed in 54.78 seconds on macOS. It checks:

- Preparation failure preserves the previous published generation.
- The abort decision survives coordinator restart while its participant is
  offline; another coordinator restart preserves the pending notification.
- Worker restart retains the cancelled attempt until the abort is applied.
- Healthy-peer publication serves exact answers while the unreachable worker
  retains its old candidate and retention set.
- Aborting a later candidate does not overwrite the older abort notification.
- A lost response after an applied abort retains retry intent, and replaying the
  original reservation is rejected after restart.
- A worker whose reservation succeeded but whose response was lost is excluded
  from publication readiness and receives a durable abort notification.
- Both workers can rejoin complete publication after recovery, with zero pending
  aborts exposed in health and metrics.

All 18 v4 library tests passed. Persistence tests cover abort decisions at planned,
reserved, preparing and ready phases, preservation of earlier pending decisions
and reservation accounting, rejection of a commit on a pending worker, and the
atomic split between commit notifications and excluded-participant aborts.
Strict Clippy passed for the library, v4 binary and HTTP test target.

The [separate-process smoke report](process-smoke/exercise.json) records
5,217 correct answers, zero background errors, six publications,
36 boundary/retention probes and two expiry refreshes over
57.339 measured seconds. Background query p99 was
66.495 ms; maximum fixture mutation/publication time was
10,009 ms. The [run record](process-smoke/run.json)
identifies the native binary and dirty source state. Timings were collected on a
shared local host during regression testing and do not establish a performance SLO.

[Validation commands](validation.json) record observed checks and exit codes.
Formatting, documentation links (115 files) and diff whitespace checks passed.

All five existing HTTP cases passed in the [regression log](http-regression.log),
with no ignored cases and a total duration of 295.25 seconds. This includes
committed-participant recovery, retention/failover/restart, inventory expansion,
full-size loan/return/reorg and seven-domain/four-worker consolidation with
reservation-failure fallback. Together with the new abort test, all six HTTP
cases passed across the recorded invocations.

[Source hashes](source-sha256.json) identify implementation, tests and instructions.
[Evidence hashes](evidence-sha256.json) cover saved reports and logs.

## Scope

These tests use real cryptographic evaluation and HTTP listeners with controlled
fault middleware. They do not establish 8 GiB hardware capacity, the six-hour
qualification campaigns, lost-artifact repair, all distributed failure phases,
or production deployment readiness. No cloud or production mutation occurred.
See the [implementation status](../../docs/architecture_2-implementation.md) and
[operating instructions](../../ops/deploy/v4-candidate.md).
