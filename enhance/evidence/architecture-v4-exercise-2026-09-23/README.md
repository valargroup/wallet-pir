# V4 workload-driver validation — September 23, 2026

The isolated `enhance-pir-v4 exercise` command now drives synthetic journal
publications through the real coordinator and worker HTTP processes while
concurrent clients verify exact PIR answers. It checks current unit/loan
boundaries, retained tails and expired-session refresh, and persists publication
traces and a report explicitly marked unqualified.

## Recorded process result

The [successful report](passed/exercise.json) and [publication trace](passed/publications.jsonl)
record six publications over 50.199 measured seconds, 5,320 correct background
answers, zero background errors, 36 exact boundary/retention probes and two
expired-session refreshes. Background p99 was 48.351 ms; the maximum fixture
mutation/publication duration was 8,618 ms. The requested ten-second run continued
until its six-publication minimum was reached. The [run record](passed/run.json)
identifies the native binary and dirty-checkout provenance.

The [initial failed report](initial-429/exercise.json) retains an HTTP 429 failure:
two background queries plus an independent probe exceeded the coordinator's
existing two-query limit. The driver now shares its declared concurrency budget
between probes and background traffic. The server limit was unchanged.

All [17 v4 server tests](rust-tests.log) passed. Two targeted tests verify full-size active-five/sealed-six placement over
24 schedule steps and borrowed/owned/unit boundary probe selection. These are
planning tests, not full-size process or memory qualification. The local process
run uses the small smoke fixture on macOS; it has no off-host hardware claim.

The active and sealed six-hour campaigns have not been run. This evidence does
not establish 8 GiB worker capacity, resident guard, sustained publication SLOs,
open-loop throughput, outage recovery, independent canonical wallet correctness,
or deployment readiness. See the [workload instructions](../../ops/deploy/v4-candidate.md)
and [implementation status](../../docs/architecture_2-implementation.md).

Strict Clippy for the v4 binary, workspace formatting, documentation-link checking
(113 files), Python helper compilation and diff whitespace checks passed.
[Source hashes](source-sha256.json) identify the workload implementation and instructions.
