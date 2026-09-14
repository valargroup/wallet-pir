# M3 public regression suite correction — September 13, 2026

**PASS: all eleven cases and 68 checkpoints passed** from one frozen executable.
Across preflight and cases, 4,320 logical requests used 4,324 HTTP attempts;
4 attempts failed, 4 requests recovered, and 0 logical requests failed.
No event, UTXO, balance or history mismatch was reported. This closes the deployed
public-fixture regression subgate; **M3 as a whole remains open**. Execution: Codex for Roman on macOS 26.2 arm64,
Rust 1.91.0, release profile, against the existing public mainnet services.
No fleet deployment, configuration or fixture change was made for this correction.

## Problem and correction

The [original run](../productionize-m3-live-2026-09-13/README.md) passed six of eleven
cases and failed five on HTTP 503, connection reset or interrupted response bodies.
The regression runner used the shared HTTP adapters' one-attempt default despite
an existing opt-in retry mechanism. The shared mechanism also granted a fresh
request timeout to each attempt and did not consistently preserve public-path
`Retry-After` without an observer.

The runner now explicitly selects at most three attempts for eligible transient
HTTP failures on all map, filter, init, manifest, setup and query paths. Attempts
and backoff share the existing **60-second logical-request budget**. The case
budget stays **1,800 seconds**. Private retries replay the same buffered encrypted
body at the same URL/revision; there is no new locator or plaintext fallback.
Application decoding and identity/revision validation remain outside HTTP retry.
The frozen fixture and exact comparison rules are unchanged.

Every attempt is appended to `preflight.http.ndjson` or `case-<id>.http.ndjson`,
including status, duration, failure/transport error and payload byte counts.
Per-case and preflight summaries distinguish requests, attempts, recovered
requests and terminal failures. Failure to retain an attempt record fails the
worker. `--http-attempts 1` remains available for strict first-attempt diagnostics.
See the maintained [testing procedure](../../../docs/transparent-pir/testing.md).

## Reproduction and source identity

The [source manifest](source.json) records base `e98286bf345718b29e22ef81076cd22633405d83`
and SHA-256 of every modified compiled/test source. [Source patch](source.patch)
contains the tracked-file changes; [new observer module](http_evidence.rs) belongs
at `server/transparent-regression/src/http_evidence.rs`. The working-tree build
is identified explicitly rather than attributed to the unchanged base commit.
The repository commit containing this bundle preserves that exact source.

After workspace checks completed, the executable was copied out of Cargo's
mutable target directory. [Frozen binary manifest](frozen-binary.json) records
its hash and copy time. Both supervisor and child cases use that frozen path.

```sh
/tmp/transparent-suite-fix-20260913/transparent-regression-frozen \
  --fixture server/transparent-regression/fixtures/mainnet.json \
  --shard-url https://transparent-pir.valargroup.dev \
  --filter-url https://enhance-pir.valargroup.dev \
  --source-sha e98286b+suite-retry-working-tree \
  --http-attempts 3 --request-timeout-secs 60 --case-timeout-secs 1800 \
  --out-dir /tmp/transparent-suite-fix-20260913/accepted
```

## Accepted run artifacts

[Summary](summary.json), [full report](report.json), [JUnit](junit.xml) and
[command output](accepted-command.log) retain the final result. The
[raw report/attempt bundle](accepted-run.tar.gz) includes every case report,
per-attempt HTTP log and stdout/stderr. The report's binary hash matches the
frozen manifest and the unchanged executable after the run.

SQLite stores and the large executable remain locally under
`/tmp/transparent-suite-fix-20260913/`; they are not committed. The
[store inventory](local-store-inventory.json) records local paths, sizes and
hashes. The fixture groups public scripts; it contains no supplied wallet secret
or private-wallet history. `SHA256SUMS` covers the committed bundle.

## Local qualification

The complete `make check` passed: operations contracts/tests, documentation links,
report tests, Rust formatting, strict workspace Clippy across all targets/features,
and release workspace tests (**596 passed, zero failed, two ignored**).
[Complete check log](make-check.log.gz). The new executable-level tests inject
one `/init` 503 and verify recovery with retained failed-attempt evidence, then
persistent 503 and verify bounded failure. Existing sealed-map substitution,
shrinking/re-parented tail and lineage drift refusals still pass.

HTTP tests cover identical private POST replay, established connection and
truncated-body failures with/without telemetry, fatal statuses, shared timeout
across backoff/body reads, and `Retry-After` too large for the remaining budget.
An initial local test caught missing root policy metadata; an initial Clippy
check caught a test socket read that ignored its length. Both were corrected
before the passing complete check and the frozen live run.

## Earlier repaired run and retained limits

The [first repaired run](initial-repaired-run.tar.gz) passed all eleven cases:
4,332 logical requests, 4,342 attempts, ten failed attempts, eight recovered
requests and no terminal request failures. [Command output](initial-command.log).
Concurrent workspace checks replaced its executable in `target/release` during
execution. The sampled initial and final binary hashes differ; source hashes
were unchanged, but this run has no unambiguous run-wide binary identity.
It is preserved as diagnostic evidence and not used as the acceptance run.

Success with retries establishes exact recovery under this bounded policy, not
uninterrupted availability. Submitted payload bytes are not a measurement of
socket delivery; completed body counts omit partial bytes lost on failed reads,
and neither count includes complete TLS/wire overhead. Failed attempts remain
visible and need server-side attribution under M5.

The native wallet candidate still pins client `22e6bec`; its binary and retry
configuration were not changed here. M3's application UI and stronger live
interruption checks remain in [remaining work](../../../docs/transparent-pir/remaining-work.md).
Private-wallet evidence stays in its [separate sanitized bundle](../productionize-m3-private-wallet-2026-09-13/README.md).
