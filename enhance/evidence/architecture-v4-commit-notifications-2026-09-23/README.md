# V4 committed-participant recovery — September 23, 2026

Commit notifications are now part of the atomic controller publication decision.
Their retry state survives controller restart. An unreachable activation
participant no longer prevents notification delivery to its healthy peer or
blocks subsequent ordinary publications. Pending participants are excluded from
new reservations and retention updates until their original decision is applied.
The worker retains and charges its old runtimes and committed candidate meanwhile.

## Recovery validation

The real encrypted HTTP test
`committed_offline_participant_does_not_block_and_recovers_after_expiry` passed
in 68.82 seconds on macOS. It verifies:

- Both workers acknowledge activation, but the first commit notification fails;
  the second worker still commits and the publication remains successful.
- Pending notification identity survives coordinator restart. Five more
  publications serve exact PIR answers while the first worker is offline.
- Aborting a later candidate preserves the older committed candidate.
- The offline generation expires and its coordinator snapshot is reclaimed;
  the worker still retains its original candidate and old retention set.
- The worker restarts from disk, reconstructs preparation, applies the old
  decision, synchronizes retention, and then catches up to the latest assignment.
  With its peer disabled, it answers a newly appended record exactly.
- A lost response after durable worker commit leaves a retry record. A matching
  generation with a conflicting manifest digest cannot clear that record; the
  actual matching digest resolves it.

All 17 v4 server library tests passed, including atomic persistence of the
notification set and rejection of a second committed decision for a pending
worker. Strict Clippy passed for the library, v4 binary and HTTP test target.

The [HTTP regression log](http-regression.log) records two passing existing tests
(retention/failover/restart and inventory expansion). Both full-size cases also
passed in the [separate full-size log](full-size-http.log), completing in 377.72
seconds: seven-domain/four-worker consolidation with reservation-failure fallback,
and full-shard loan/return/reorg with old-query preservation. Thus all five HTTP
cases passed across the recorded invocations.
[Validation commands](validation.json) record the observed checks and exit codes.

The [separate-process smoke report](process-smoke/exercise.json) records 4,941
correct background answers, zero errors, six publications, 36 boundary/retention
probes and two expiry refreshes over 52.736 measured seconds. Query p99 was
55.615 ms; maximum fixture mutation/publication time was 8,879 ms. These timings
were collected on a shared local host and do not establish a performance SLO.
The [run record](process-smoke/run.json) identifies binary and dirty-source state.

## Scope

The HTTP tests use real cryptographic computation and listeners with controlled
fault middleware; they are not cloud or actual 8 GiB hardware qualification.
This closes the cached-state committed-participant recovery case. It does not
establish recovery from lost worker artifacts, unavailable pre-commit abort
participants, all distributed failure phases, or full-size six-hour capacity.
No production deployment or cloud mutation occurred.

See the [implementation status](../../docs/qualification.md)
and [operating semantics](../../docs/qualification.md).

[Source hashes](source-sha256.json) identify the changed implementation, tests and
operating instructions. [Evidence hashes](evidence-sha256.json) cover the reports
and captured logs.
