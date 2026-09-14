# M1 six-hour fleet acceptance

Recorded 2026-09-13 by Codex for Roman. Roman confirmed in the operator
conversation that the post-rollout requirement had been updated to six hours
and that the fleet observation passed. This record reconciles that decision
with the retained raw data; it does not claim a contemporaneous written
acceptance record was found. It supersedes the stale twelve-hour requirement.

**M1 accepted:** the matching loaded canary and corrected six-worker rollout
passed, followed by the first six continuous hours of the replacement fleet
observation, September 12 approximately 00:49:59–06:49:59 UTC. Every worker
recorded 300 public and 300 replica canonical block observations in that window
(heights 3,480,250–3,480,549). No block-counter reset occurred.

## Evidence and scope

- [Complete closed observation](observation.tar.gz), copied read-only from the
  coordinator's `/opt/transparent-publisher-build/caddy-http-retry-20260912/observation`.
  Includes unmodified launch provenance, worker samples, query logs and failure.
- [Prior stage results](prior-stages.tar.gz): canary and corrected fleet upgrade,
  both `passed: true`, with the same worker binary digest as all six observed workers.
- [Per-worker six-hour summary](six-hour-summary.json): select each monitor's
  `start` timestamp through `start + 21600 seconds`, inclusive, from
  `samples.ndjson`; count `block_visible` and `canary_block_visible`, take their
  maximum `seconds`, and inspect every `worker` sample's memory and service identity.
  The logs continue beyond each endpoint; these are retrospective window metrics,
  not synthesized terminal monitor results.

Across this window maximum public visibility was **20.080 seconds**, maximum
replica visibility **22.915 seconds**, and minimum available host memory
**24.387%**. All six service start identities stayed constant, restart counters
were zero, and no OOM was recorded. All 60 metadata transport failures in the
window recovered under the qualified bounded retry policy. The continuously
running monitor checked routing withdrawal, canonical readiness and managed
worker conditions without a terminal failure in the accepted window.

The two complete query logs record 398,933 exact responses, zero mismatches and
372 retries. These totals cover the longer run, not just six hours: individual
query records have no wall-clock timestamp, so they cannot provide an exact
six-hour query count. They preserve the sustained clients' work and show no
inexact answer anywhere in the encompassing run.

Worker source `a5f79ed`, binary
`200ca85065c8096d344749d5e51a2db569ec369c71bd2ffff8cf0e9fd014ff62`,
operations `0e2c003`, Caddy 2.11.4 and monitor/configuration digests are retained
in `observation/provenance.json`. The canary roster digest matches observation
provenance. The upgrade's thin result contains the binary and all six worker
names but no roster digest; the original
[upgrade and recovery record](../productionize-m1-deferred-collection-2026-09-11/README.md)
and [router qualification](../productionize-m1-http-retry-2026-09-12/README.md)
retain the broader deployment provenance.

## Later failure and disposition

The supervisor was still configured for 24 hours and continued past the accepted
window. At **08:05:01 UTC** recent-01 failed the replica freshness gate at block
3,480,596: the monitor abandoned the read at **60.715 seconds** against 60 seconds.
This is not the eventual recovery time. The other five monitors and both query
clients were cancelled. The command remains failed; no raw result is relabelled.

Roman's confirmed six-hour acceptance applies to the earlier completed window.
The subsequent incident is retained as an **open M5 reliability follow-up**:
diagnose the delay, qualify its correction and demonstrate sustained capacity
and bounded recovery before beta. M1 acceptance does not close M3, M5, release
readiness or general availability.

The existing whole-run audit rejects the failed 24-hour command, even with a
shorter duration flag: it requires terminal worker results and includes the later
failure. It is not a retrospective-window acceptance tool. This separate dated
operator decision and bounded sample summary are the transition record; the
whole-run rejection remains valid for its own scope.
