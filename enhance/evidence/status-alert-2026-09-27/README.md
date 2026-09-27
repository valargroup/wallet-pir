# Status HTTP alert coverage and live retest

The user requested closing the Status latency/error alert gap, committing and
pushing directly to main, and rerunning the tests.

Runtime source commits, both pushed directly to main:

- `76590592ea93d6a6ba8e75fd4f4701dccbc6377b`: Status HTTP counters, dedicated
  init/query 5xx and latency rules, coverage rules, regression tests and docs.
- `c5a6486476d91c88b692838c3449f4ddb917201c`: instrument the separate publisher's
  init/session/diagnostic-query routes, with a regression test. Live verification
  caught that this route was distinct from the monolithic controller route.

## Deployment

Updated the live Status worker/router and APM to 76590592, then updated the live
Status controller to c5a64864. The latter changes only publisher instrumentation;
APM and worker/router behavior is identical to the final source revision.
Actual running process hashes are recorded in `services-after.json`, matched
against `binaries.json` and `publisher-binary.json`. All four services were active
with zero automatic restarts. The other Enhance processes, synthetic Status
fixture and external checker were not changed by this targeted rollout.

CPU releases use Ubuntu 24.04/Haswell; CUDA Status uses Ubuntu 22.04, Haswell and
CUDA 12.2. Source archive and follow-up source hashes are in `build-inputs.json`.
CPU, CUDA and final-controller distributed native preflights passed mined,
mempool, forked, not-found and remote-fence cases. Prior binaries, systemd
settings and incident databases remain preserved. Per-service rollback records
are under `/root/wallet-pir-rollout/<service-commit>/<unit>/` on each host.
The previous immutable release path is recorded in that unit's `before.json`.

## Tests

- APM: 96 tests passed (5 library and 91 binary tests).
- Status server: 40 tests passed in both default/native configurations for the
  first change; all 41 native Status tests passed after the publisher follow-up.
- Formatting, diff checks and all-target clippy passed.
- Tests cover real distributed router/worker HTTP responses on split and merged
  listeners, publisher init rejections, HTTP 4xx versus 5xx accounting, histogram
  overflow, schema mismatch/malformed input, missing/stale scrapes, observation
  gaps, counter resets and process reincarnation, latency hold time, and recovery
  only after two distinct healthy samples. Unknown input does not recover alerts.
- GitHub fast CI passed on final commit c5a64864. The full workflow
  [36300124397](https://github.com/valargroup/wallet-pir/actions/runs/36300124397)
  was still queued behind earlier shared-runner work at this evidence capture;
  this record does not claim that workflow passed.

## Live concurrent load

Ran 2026-09-27 06:31:58–06:36:35 UTC (10:31–10:36 Dubai). Status ran alongside
both Enhance phases. Independent canonical answer oracles were used.

| Service | Offered load | Duration | Correct / offered | End-to-end scheduled p99 |
| --- | --- | --- | --- | --- |
| Enhance | 5 QPS | 60 s | 300 / 300 | 133.887 ms |
| Enhance | 20 QPS | 180 s | 3600 / 3600 | 145.791 ms |
| Status direct router | 20 QPS | 270 s | 5400 / 5400 | 68 ms |

All 9,300 measured logical queries completed correctly, with zero failed or
unstarted lookups. The new router HTTP counter recorded 5,400 successful
responses plus one HTTP 4xx session-expiry response, automatically retried by
the existing probe's 409/410 retry policy. There were zero query HTTP 5xx.
`http-reconciliation.json` reconciles these counts; raw per-query records are
kept on the host and are not included here. This is bounded load validation,
not saturation testing or long-duration qualification.

The controller restart produced two temporary init 503s before readiness.
The real `status_init_5xx` rule fired in shadow at 2/35 completed responses
(>5%) and subsequently recovered after fresh healthy samples. Its durable ID is
`status_init_5xx-1790490725`. No latency, query-error, or coverage incident fired.
All sampled external canaries and monitoring-progress checks passed. The final
aggregate has no active incidents, firing conditions or unknown checks.
The load harness stopped successfully and has a six-minute hard runtime limit.

This run demonstrated a real error firing/recovery transition. Latency firing
and missing-data behavior were tested deterministically, without manufacturing
production latency or lowering production thresholds. New-rule Slack sending
remains disabled in shadow, so this does not constitute a new Slack receipt test.

## Observation

A fresh 72-hour collector started at **2026-09-27 06:37:33 UTC**
(**10:37 Dubai**), under `pir-observability-evidence-c5a64864.service`, writing
`/var/lib/pir-monitor/rollout-evidence-c5a64864/`. Earliest shadow review:
**2026-09-28 06:37:33 UTC / 10:37 Dubai**. Earlier evidence is retained, and
incident databases were not cleared. The new rules remain shadow; existing
legacy alerting stays enabled. Review precedes activation, which requires
another 24 hours of active observation. No automatic promotion is configured.
