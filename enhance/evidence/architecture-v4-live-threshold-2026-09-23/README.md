# Isolated live v4 forecast and automatic provisioning, 2026-09-23

This campaign used a fresh synthetic coordinator and four new DigitalOcean `c-4`
workers in the Valargroup wallet-pir project. It did not query the production PIR
service. The Linux candidate was built from clean revision
`238a6c213f92755620214ffa8a0fd63ffa3dda7e`; the test-only timing driver
reached revision `71f777c650272d3bc9389847cd8202db7e6b288c` during the
campaign. All provisioned workers used the same
candidate bundle and cgroup limits. The Terraform root used its own Spaces state
key and a pinned coordinator-host lock.

## Forecast crossing and provider handoff

The next limiting boundary was 5,406,720 records. At the configured one-row-per-
second fallback rate, six-hour readiness window and 4,096-row burst allowance,
the inclusive request threshold was **25,696 remaining rows**. The fixture
started 60 rows farther away, at 25,756 remaining. Because a single-row PIR
publication takes much longer than the five-second poll, the coordinator was
restarted with one controlled append to reach 25,697 remaining rows. A final
one-row append crossed the threshold. The one-use watcher retained its outside
sample across the controlled restart and could invoke the provider adapter only
for `successor-5-pair-2`.

| UTC time | Observation |
| --- | --- |
| 15:28:37 | 25,697 rows remained; no request existed. |
| 15:34:25 | Coordinator requested `successor-5-pair-2` at exactly 25,696 remaining rows. |
| 15:34:26 | Watcher detected the request and started real DigitalOcean provisioning. |
| 15:35:23 | Two new `c-4` Droplets were provisioned; Terraform state and provider IDs matched. |
| 15:41:40 | Pair 2 was registered while shard 5 did not yet exist. |
| 15:51:23 | Shard 5 first published on pair 2, with both replicas published. |
| 15:55:58 | Shard 5 moved to pair 1's sixth sealed slot; active shard 6 published on pair 2. |

The trace has 657 outside-threshold samples with no request. Pair 2 was
registered at least **9 minutes 42 seconds** before shard 5 first published.
Placement had no blocked sample through the move. During accelerated growth the
coordinator forecast a later pair-3 request; the pinned watcher had already
exited, and no pair-3 resource was created. The trace establishes advance
**worker provisioning** and published shard movement. It does not establish that
shard 5 was preloaded into worker memory before its first publication.

The raw, one-second capacity trace includes health-unavailable entries for the
deliberate coordinator restarts. Placement samples taken while serving had zero
errors; three trailing observer errors occurred only after the coordinator was
stopped to freeze growth. The sanitized [summary.json](summary.json) includes
the trace hashes and exact timestamps. Raw traces remain outside Git.

## Thirty-minute query and RAM result

The first fixture-oracle load ran for 1,800.127 seconds at concurrency 2. All
**22,985 completed queries returned correct answers**, with zero HTTP errors.
Median, p95 and p99 latency were 162.303, 192.127 and 204.031 ms. Direct
queries at records 5,406,720 and 6,488,064 returned the exact position-encoded
fixture records after shard 5 moved.

Each worker had 1,859 or 1,860 one-second RAM samples with zero sampling errors,
one boot ID and one worker process. Peak cgroup charges stayed below the
7,599,996,928-byte `memory.max` setting; there were zero OOM kills and zero
observed worker-cgroup swap bytes. One host recorded 122 system-level swap-out
pages (about 0.5 MiB), and its free host swap fell about 0.8 MiB during the
first 39 seconds. That host-level activity was outside the worker service
cgroup. To remove that ambiguity, a second, otherwise identical run disabled
swap on all four disposable hosts. Before and after this second run, every host
had zero swap devices, zero `SwapTotal`, and zero worker-cgroup swap.

The no-swap run lasted 1,800.058 seconds at concurrency 2. All **23,400
completed queries returned correct answers**, with zero HTTP errors. Median,
p95 and p99 latency were 159.615, 191.359 and 205.183 ms. The two direct shard
queries again returned exact fixture records. Each worker had 1,859 or 1,860
one-second RAM samples with zero sampling errors, zero host swap-in or swap-out
pages, zero worker-cgroup swap, zero OOM kills, one boot ID and one worker
process. Peak cgroup charges remained below `memory.max`. This establishes
sufficient RAM for this four-worker placement and 30-minute concurrency-2 load;
it does not establish a six-hour hardware qualification.

## Cleanup and limits

After saving the evidence, the exact provider IDs were checked and all four
workers, the coordinator, the dedicated firewall and tag were removed. The
isolated Terraform Spaces state key was deleted and verified absent. A final
provider scan found no v4 test Droplets. This is an isolated test receipt, not
a six-hour hardware qualification or a production promotion gate. Registration
receipts remain `qualification: unqualified`.
