# V4 hardware observation validation — September 23, 2026

The candidate now includes a [read-only worker sampler](../../ops/scripts/sample-v4-worker.py).
It binds measurements to the running binary and bootstrap limits, records
boot/process identities, cgroup memory/peaks/events, swap, pressure, host memory,
per-process RSS/PSS and kernel RSS high-water accounting, disk space, and worker
retention/candidate observations. It never resets counters or substitutes zero
usage when collection fails.

The [off-host observer](../../ops/scripts/observe-v4-pair.py) selects the persisted
bootstrapped pair, uses pinned-key SSH, retains every scheduled slot (including
transport failures and missed deadlines), fsyncs the trace, and records its final
SHA-256 digest. Output remains `qualification: unqualified`; no workload is
started and no passing qualification receipt or registration is produced.

## Evidence

- [Sampler tests](sampling-tests.log): five passed locally; the live Linux procfs
  test was skipped on macOS. They check memory units and cumulative counters,
  read-only behavior, inactive services, identity/limit changes, PID reuse,
  missing evidence, and explicit collection-error output.
- [Observer tests](observation-tests.log): four passed, covering retained SSH
  errors, trace integrity, missing sampling deadlines, malformed/nonfinite JSON,
  and invalid capture configuration.
- [Local checks](local-checks.log): 91 operations tests ran, with 89 passing and
  two Linux-only checks skipped; all 17 release/tooling, 3 filter and 4 parent-filter
  tests passed. Documentation links, Python compilation, and diff whitespace
  checks passed.
- [Final Linux checks](linux-final-tests.log): all 57 v4 operations tests passed
  without skips in a disposable Ubuntu 24.04 container. This includes generated
  service parsing and live procfs memory parsing; cgroup parser tests also use
  controlled fixtures. The [image record](linux-environment.json) identifies the
  arm64 Linux environment. The earlier [Linux log](linux-tests.log) precedes the
  final RSS high-water collection addition; the final log covers that addition.

Filesystem/process/SSH fixtures do not constitute measurements from deployed v4
workers. No six-hour workload, real service capacity campaign, cloud operation,
or production change occurred. The disposable containers were removed on exit.
The resident guard, reclaim behavior, full-size active/sealed profiles, transitions,
load/oracle results, publication timing, qualification assessment, and registration
still require implementation/integration and actual hardware execution.

[Source hashes](source-sha256.json) identify sampler, observer, packaging, tests,
and instructions. See the [implementation status](../../docs/architecture_2-implementation.md)
for the remaining complete release gates and the [measurement instructions](../../ops/deploy/v4-candidate.md)
for units, counter/peak interpretation, error records, and off-host limitations.
