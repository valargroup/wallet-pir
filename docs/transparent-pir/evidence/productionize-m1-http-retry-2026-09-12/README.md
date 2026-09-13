# M1 public HTTP disconnect qualification

Current disposition: [M1 accepted for the user-confirmed six-hour window](../productionize-m1-six-hour-acceptance-2026-09-13/README.md).
The original 24-hour supervisor later failed; the launch and checkpoint records
below retain the information available at their timestamps.

The all-worker observation started September 11 at 22:12:40 UTC and failed
September 12 at 00:31:21 UTC. The recent-03 monitor received a connection reset
in the TLS handshake for the public `/v1/shards` GET. The supervisor cancelled
all other monitors. The [closed raw observation](failed-observation-20260912T003121Z.tar.gz)
contains 127,133 exact query responses, zero mismatches and 84 query retries.
This interrupted run has no 24-hour acceptance credit.

Router membership additions and Caddy reloads coincide with the reset. The
router and recent-03 service identities did not restart. Public DNS resolved
directly to the dedicated router, not an intervening CDN.

## Isolated reproduction

Tests ran on the existing router using separate loopback ports 19443 and 12019,
isolated Caddy data/config directories and a local test CA. Test clients bypassed
trust verification for that test CA only; production observer verification is
unchanged. Test processes were stopped after each run. Serving configuration
and binary were unchanged. [Raw results and Caddy logs](tls-reload-tests.tar.gz)
exclude generated private keys. Scripts: [baseline](reload-repro.py) and
[observer retry](observed-reload-repro.py).

| Variant | Reloads | Successful requests | Failed requests | Recovered disconnects |
| --- | ---: | ---: | ---: | ---: |
| Installed 2.6.2, no retry | 100 | 2,216 | 12 | 0 |
| Candidate 2.11.4, no retry | 100 | 3,646 | 221 | 0 |
| Installed 2.6.2, bounded observer retry | 100 | 1,282 | 6 | 5 |
| Candidate 2.11.4, bounded observer retry | 100 | 2,051 | 0 | 117 |

These are four-client reload stress tests, not capacity benchmarks or acceptance
observations. Different request counts reflect run duration and throughput;
error counts must not be interpreted as production rates. A five-reload serial
handshake test passed but did not reproduce the concurrent race.

Candidate archive was verified against the upstream SHA-512 checksum manifest.
Candidate binary SHA-256:
`b7105518e3ed1c0761f232e44fc09345535533c9cb0abf0e12809416c7ac64d9`.
The archive and version identities are in the raw bundle. Upstream
[issue 5589](https://github.com/caddyserver/caddy/issues/5589) describes a
certificate-cache reload failure, but upgrading alone did not pass our test.

## Candidate observer behavior

Read-only HTTP GETs retry at most one reset, broken pipe, remote disconnect or
unexpected TLS EOF. Both attempts share eight seconds, with 50 ms between them.
Every caught transport failure and recovery is recorded. HTTP status errors,
certificate validation failures, arbitrary TLS alerts, timeouts and malformed
content are not retried. Publication identity, routing withdrawal and freshness
checks are unchanged; elapsed retry time is included in freshness. Results record
the monitor hash and retry policy. The deployed observer uses this policy; acceptance is still pending.

Ten focused monitor tests and the [full repository check](make-check.log) passed.
The [guarded upgrade script](upgrade-router.py) completed successfully at
00:48:57 UTC on September 12. [Deployment and observation-start evidence](upgrade-and-observation-start.tar.gz)
contains the saved configuration, original unit, running-binary verification,
terminal upgrade result, supervisor, initial readiness and provenance.

Caddy 2.11.4 runs via `/etc/systemd/system/caddy.service.d/transparent-version.conf`
from `/usr/local/lib/transparent-router/caddy-2.11.4`. The packaged `/usr/bin/caddy`
2.6.2 remains intact for rollback; the override pins both start and reload.
The initial restart was guarded. Canonical warm advertised service was verified
before reopening, followed by public-origin verification.

The fresh all-six-worker observation began at `2026-09-12T00:49:59.418333+00:00`,
unit `transparent-m1-http-retry-observation.service`, initial PID 697911.
Operations commit is `0e2c003`; worker source remains `a5f79ed` and worker binary
remains `200ca85065c8096d344749d5e51a2db569ec369c71bd2ffff8cf0e9fd014ff62`.
Monitor SHA-256 is `16be140a77dec3d3e67b76acf6a7f5b424f65c6f362a7becb3f6253ad896d23b`.
All six monitors advanced at 00:50 UTC. The supervisor requires 86,400 seconds
and 300 canonical blocks per monitor. Existing maintenance restoration remains
scheduled with no exemptions. Earliest finish is September 13 at approximately
00:50 UTC (September 12, 18:50 Edmonton). M1 remains open pending completion
and audit; the failed observation is not credited.

## Operator checkpoint: 2026-09-12, 03:31 UTC

Read-only SSH checks of the coordinator confirmed the existing systemd unit
active with `MainPID=697911` and `ExecMainStatus=0`. All six `samples.ndjson`
logs advanced between checks; their latest timestamps were 03:31:21 UTC.
No worker `result.json` existed. This checkpoint was reported in the operator
session; it is not a closed raw acceptance bundle or proof of a completed pass.
Raw ongoing output remains under
`/opt/transparent-publisher-build/caddy-http-retry-20260912/observation`.

The operator subsequently requested checks once every two hours. The next check
is due at 05:31 UTC; continuous server-side monitoring, failure handling and
acceptance thresholds are unchanged. Preserve and audit the closed raw output
before claiming completion. No new fleet poll was made for this documentation
update; the checkpoint above is explicitly the last verified state.
