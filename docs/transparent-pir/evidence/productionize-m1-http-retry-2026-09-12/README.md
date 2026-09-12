# M1 public HTTP disconnect qualification

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
the monitor hash and retry policy. This is a candidate, not deployed acceptance.

Ten focused monitor tests and the [full repository check](make-check.log) passed.
The [guarded upgrade script](upgrade-router.py) is prepared; deployment is pending.
M1 remains open.
