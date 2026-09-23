# V4 continuous runtime metrics — September 23, 2026

Hardware sample version 2 now includes bounded worker metric values, a raw
exposition digest, timing windows, and health observations bracketing the scrape.
Candidate identity hashing detects replacements even when publication revision
is unchanged. Missing memory while the engine is busy is distinct from zero
usage. Runtime-scrape failures retain independently collected host evidence.

The off-host observer requires the new successful-sample contract and separately
counts unavailable memory and inconsistent runtime state. Its result remains
unqualified. Samples are not atomic host/runtime memory snapshots; use the
recorded windows when assessing them.

## Validation

- [Local v4 suite](local-tests.log): 64 tests ran, 62 passed and two Linux-only
  tests were skipped. New cases reject malformed/duplicate/labeled/nonfinite,
  out-of-range and availability-inconsistent metrics, detect candidate/revision
  changes, preserve kernel evidence on failure, and count observation gaps.
- [Linux v4 suite](linux-tests.log): all 64 tests passed without skips in a
  disposable read-only-mounted Ubuntu 24.04 container, including live procfs and
  generated systemd unit parsing. Container effects were removed on exit.
- [Operations and tooling regressions](regression-tests.log): the complete enhance
  operations suite and 17 release/tooling tests passed, with the two expected
  Linux-only skips on macOS. Bundle-contract checks are included.
- [Live worker RPC record](live-runtime-rpc.json): the actual native v4 worker
  accepted a disposable empty reservation and abort. The sampler fetched and
  parsed fresh/reserved/aborted metric responses over HTTP, verified bracketing
  state, and checked candidate/model flags. The worker and temporary state were
  removed afterward. No unrelated listener was adopted.

The [Prometheus fixture](../../ops/fixtures/v4-worker-metrics.prom) comes from the
previous real separate-process workload's [worker scrape](../architecture-v4-metrics-2026-09-23/process-smoke/metrics/worker-0.prom).
It contains no query positions or request-derived labels.

## Limits

The live RPC test runs on macOS and does not measure Linux cgroups or 8 GiB
capacity. Linux host/systemd parser tests do not constitute deployment of the
actual service. No six-hour worker campaign, coordinator collector deployment,
hardware qualification, live SSH rollout or production mutation occurred.
See the [collection contract](../../ops/deploy/v4-candidate.md) and
[remaining implementation gates](../../docs/architecture_2-implementation.md).
