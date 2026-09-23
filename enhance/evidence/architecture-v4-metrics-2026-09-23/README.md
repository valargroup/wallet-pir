# V4 runtime-metrics validation — September 23, 2026

Coordinator metrics now cover published shard/unit geometry, loans, group roles
and occupancy, durable operations and pending decisions, capacity forecasts,
consolidation planning, and lag against an observed publication target. Worker
metrics separate database buffers from the unchanged admission model and expose
retained, candidate, query-only and source-reclamation references.

All 21 v4 library tests passed. New checks cover loan/return geometry, escaped
inventory labels, absent observations, matching-target retry age, same-height
reorgs, signed rollback deltas, and worker scrapes while the engine lock is held.
The busy-engine test verifies that unavailable memory samples are omitted rather
than replaced by zero, and that scraping does not write worker state.

The workload runner now saves final Prometheus snapshots with SHA-256 digests.
Its smoke helper verifies that both workers have memory samples, no candidate,
and a model within its limit, and that the coordinator reached the final target.
Those checks are state assertions, not hardware qualification.

The [separate-process report](process-smoke/exercise.json) records
5,269 correct background answers, zero errors, six publications,
36 boundary/retention probes and two expiry refreshes over
58.285 measured seconds. Query p99 was
68.735 ms. Timings were collected on a shared local host while
regression tests ran and are not a performance qualification result.

The report indexes checksummed [coordinator metrics](process-smoke/metrics/coordinator.prom)
and [first-worker metrics](process-smoke/metrics/worker-0.prom), alongside the
[second worker](process-smoke/metrics/worker-1.prom). The process helper verified
these snapshots against the completed workload and the digests in the report.
The [run record](process-smoke/run.json) identifies the binary and dirty-source state.

Strict Clippy, formatting, documentation links (116 files), and diff whitespace
checks passed. [Validation commands](validation.json) record observed outcomes.

All six HTTP tests passed with no ignored cases in the [full regression log](http-tests.log)
(357.26 seconds). They cover abort/commit notification recovery, retention/failover,
inventory expansion, full-size loan/return/reorg and seven-domain/four-worker
consolidation. The retention test additionally verifies private-worker metric
content type, actual initial database-buffer bytes and admission-model availability.

[Source hashes](source-sha256.json) identify the implementation, tests and operating
instructions. [Evidence hashes](evidence-sha256.json) cover the saved reports,
metric snapshots and captured log.

## Scope

Buffer-byte accounting is not process RSS, PSS or cgroup charge. The model still
uses the unqualified 728 MiB overhead allowance. Reference subsets overlap and
must not be summed. Count/role capacity and consolidation previews do not prove
memory admission or live replica readiness. Continuous scraping, actual 8 GiB
hardware campaigns, six-hour qualification and deployment remain outstanding.
No cloud or production changes occurred.

See the [metric semantics and scrape instructions](../../docs/qualification.md)
and [implementation status](../../docs/qualification.md).
