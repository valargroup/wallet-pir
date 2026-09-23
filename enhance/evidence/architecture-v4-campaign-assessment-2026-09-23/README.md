# V4 campaign evidence assessment — September 23, 2026

The read-only assessor checks recorded workload and hardware evidence against
frozen bootstrap inputs. The observer now binds successful samples to the
selected bundle, policy and bootstrap receipt, including artifact/boot identities,
private address, measured RAM and effective limits.

## Validation

- [Local regression tests](local-tests.log): 105 enhance operations tests ran,
  with 103 passing and two Linux-only skips; the filter suites (3 and 4 tests)
  and all 17 release/tooling tests passed.
- [Linux v4 tests](linux-tests.log): all 71 passed in a disposable Ubuntu 24.04
  container, with a read-only repository mount. Includes procfs and generated
  systemd unit checks; this does not install or deploy the actual worker service.
- Assessment fixtures cover a synthetic six-hour/300-publication trace,
  bootstrap drift, missing observation slots, OOM/reset counters, resident guard,
  host identity, trace corruption, duplicate keys and insufficient duration.
  The passing fixture remains unqualified and is not measured PIR performance.
- [Real short-smoke rejection](local-smoke-rejected.json): the assessor rejects
  the previously recorded separate-process smoke for insufficient duration,
  publication count, assignment profile/transitions and missing worker hardware
  observations. This exercises the assessment function directly, without a
  fabricated bootstrap bundle or a claim that the full CLI campaign succeeded.

## Limits

No live rollout, six-hour active/sealed hardware campaign, independent wallet
conformance run, or production mutation was performed. Exit zero means only that
automated evidence checks passed; all assessor results retain
`qualification: unqualified`. Peak resident memory between samples, overhead
calibration, hard-cap/reclaim acceptance, workload host/clock identity, latency
and open-loop acceptance, and protocol/wallet conformance remain unproven.

See the [usage and evidence contract](../../ops/deploy/v4-candidate.md) and
[remaining implementation gates](../../docs/architecture_2-implementation.md).
