# Deployment runtime follow-up — 2026-09-08

Further validation confirms four-slot restore on the live fleet, but does **not**
validate a full-fleet deployment under ten minutes. This session made read-only
worker inspections and three encrypted public directory-row queries. It did not
restart workers, alter routing, pause publication or prune cache entries.

## Four-slot evidence

The current process journals independently confirm the earlier publisher rollout:

| Worker | Disk restore slots | Startup prewarm | Cache hits | Misses at rollout |
|---|---:|---:|---:|---:|
| recent-01 | 4 | 17.644 s | 28 | Not captured at startup |
| archive-01 | 4 | 125.416 s | 160 | 0 |

The archive's earlier single-slot prewarm was 481.353 seconds: four-slot prewarm
was about 74% shorter. The publisher helper's complete archive-01 upgrade took
362.398 seconds. That includes different staging and verification work from the
fixed-publication workflow; it is not a full-fleet deployment time. See the
[original rollout log](../continuous-publication-2026-09-08/roll-retention-archives.log)
and [procedure](../continuous-publication-2026-09-08/roll-retention.py).

`further-detailed.json` captures current process IDs, units, executable digests,
startup journal records, runtime counters, filesystem capacity and cgroup events.
The common deployed executable digest is
`a2ed225699fdee84a155dba28a94e85decc917f8382da0258faf03c02cc6682a`,
matching the earlier worker rollout from source `6e0c65c`. Current readiness
`prewarm_seconds` may describe a later publication preparation; startup timing
above is taken from the first prewarm record after the current process started.
The filesystem page cache was not flushed. All six captured service cgroups
reported zero OOM and OOM-kill events; reclamation at the cap is recorded separately.

## Current deployment blockers

- recent-02, recent-03, recent-04 and archive-02 report `runtime_cache: null`;
  their units omit disk-cache configuration. Source support is deployed, but
  persistence has only been enabled on the original two canaries.
- recent-01 uses 10,734,801,920 of its 10,737,418,240-byte cache budget. Its
  cache-write failures rose from 596 to 600 between the initial captures.
  The journal explicitly reports `runtime disk cache budget exhausted`.
  It continues serving newly built runtimes but cannot persist new tail entries.
- Continuous publication remains active. Publisher-aware reclamation must retain
  active, prepared and rollback/retired revisions. The fixed-publication deploy
  guard correctly refuses this mode; bypassing it would invalidate the test.

The fleet-wide cache rollout, safe cache reclamation under continuous publication,
paired activation, unchanged-rollout verification and full timing gate remain open.

## Public correctness probes

An existing adapter test executable issued and decoded one encrypted directory
row from shard 0, shard 100 and the recent tail, shard 173. All three passed:
1.219 s, 1.177 s and 0.952 s query time respectively, excluding setup and client
preparation. The probe verifies setup digest, expected geometry and row decoding;
it does not compare the selected row with an independent chain ledger or validate
wallet balances. The executable hash and exact test name are pinned in
`further-query-provenance.json`; the existing executable was not rebuilt or edited.
This is additional serving correctness evidence, not the full accepted-anchor
wallet regression, throughput measurement, or a restart performed by this session.

Seven sequential paired public-map samples agreed, with zero HTTP errors, at
height 3,476,640. No map advancement occurred within that short sampling window;
this is not a new publication-latency test. All six workers remained ready with
unchanged PIDs and cgroup memory-event counters between the before/after captures.

Repository `make check` passed at inspection source `47a58e7`, including operations
checks, documentation links, formatting, strict workspace Clippy and release tests.
The full output is retained in `make-check.txt`.
