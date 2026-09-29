# Continuous transparent PIR query load, 2026-09-29

The user requested continuous 5 QPS load and monitoring. The enabled
`transparent-5qps-continuous.service` on the coordinator runs indefinitely until
stopped or its health gates pause admission. Production service binaries were not
changed. Runtime source remains `8e69ea75`; the new operator example is built on
that source with its recorded overlay hash in [manifest.json](manifest.json).

## Workload and rate

Five means **encrypted private query POSTs per second**, with a fresh key for every attempt. A connection interruption may retry up to
three total attempts; each retry consumes an existing scheduled slot and prepares
a new key. Every raw failure remains logged. Protocol or plaintext failures are
never retried. Public setup and operator health probes are separate.
A global send gate prevents queued work from catching up in a burst. The next
slot is based on actual admission time, avoiding accumulated timer drift; eight client
workers bound in-flight requests, with a 15-second HTTP deadline. Missed slots,
errors and schedule lag are logged rather than silently omitted.

The explicit synthetic mix is 40% recent directory, 40% recent pages, 10% archive
directory and 10% archive pages. All 85 sealed shards rotate through 5,440 sampled
rows, including occupied rows. The [fixture](fixture.json) contains independent
row hashes read from the previously verified v10 plaintext publication. Every
successful response is decrypted and compared with those hashes. The fixture has
no selected scripts or plaintext row bytes. This is not a wallet-population model,
whole-wallet recovery test, or a claim of five completed wallets per second.

The moving tail is covered by publication and readiness probes, not this fixed
sealed-row oracle. Replacing a sealed revision makes this workload fail closed
until its fixture is requalified. The client runs on the coordinator through the
public HTTPS origin; it shares the host with ingest/publication, under `Nice=10`,
a two-core CPU quota and a 3 GiB memory limit. This is not a WAN-client benchmark.

## Latest follow-up

The [04:16:35 UTC follow-up](followup/analysis.json) contains **9,375 exact decoded
results**, six raw transport failures and coverage of all 85 sealed shards. Four
failures occurred before bounded retries were added; the two later connection
interruptions both recovered on their second attempts with exact decoded rows.
The underlying error chain was `connection closed before message completed`.
Raw failed attempts remain in the denominators and logs.

The corrected scheduler's final portion contains **993 exact queries, zero errors
and zero missed slots**, at **4.982 started QPS** over 199.108 seconds between
first and last starts. It covers all 85 sealed shards. HTTP latency is 17.46 ms p50,
39.72 ms p95 and 396.08 ms p99; the maximum is 1.233 s. Maximum admission lag is
165 ms, with no accumulated queue drift. The complete follow-up contains 144
health samples, no OOM or automatic service restarts, a maximum reported
publication cycle of 15.311 s and freshness of 21.694 s. The earlier single init
503 remains the only sampled init error. The service is enabled and active in
[the follow-up metadata](followup/snapshot.json), with the live sampled state in
[followup/status.json](followup/status.json).

These are frozen observations of a continuing run, not a terminal pass. The
[checksum inventory](SHA256SUMS) covers retained artifacts except itself.

## Frozen initial observations

The [snapshot analysis](snapshot/analysis.json), captured at **04:05:46 UTC**,
contains **6,248 successful exact decoded queries**, four transport failures,
and coverage of all 85 sealed shards. Health sampling spans 03:42:42–04:05:38.
There were no wrong decoded rows. This prefix includes the operator restarts
and the 58-second health-gate pause described below.

The first runner version with bounded retries has a captured portion containing **2,044 exact queries and zero errors**
over 409.466 seconds between first and last query starts: **4.989 started QPS**.
HTTP latency was 17.43 ms p50, 37.93 ms p95, 374.99 ms p99, and 2.449 s maximum.
No retry was needed in that portion; retry classification is covered by the tests,
not by a claim of observed successful live retry recovery. All 85 sealed shards
were queried in that portion too. No missed slots were recorded. The largest
schedule lag was 1.400 s, retained independently from HTTP latency.

Across 98 health samples, minimum available host memory was **71.78%** and no OOM
or automatic service restart was observed. All six workers stayed ready in the
samples. One router process sample coincided with its `reloading` state and was
reported as not active by the conservative systemd probe; it is not a worker
readiness failure. Publication advanced from height 3,499,875 to 3,499,894.
Maximum sampled publication-cycle duration was **15.166 s** and maximum reported
freshness was **21.694 s**. One public init probe returned 503 at 03:56:40.875;
the next sample at 03:56:56.451 returned 200. The independently checked published
hash matched the local node during follow-up.

[snapshot/status.json](snapshot/status.json) and
[snapshot/snapshot.json](snapshot/snapshot.json) record the last sampled state,
enabled service and binary digest. The raw compressed logs remain alongside them.
The process was still running when this snapshot was captured.

## Persistent controls

[supervise.py](supervise.py) samples all six workers, the router, the coordinator
and public init about every 15 seconds. It monitors exact-query outcomes, achieved
rate, latency, schedule lag, memory, OOM counters, restarts, readiness, cache writes,
publication lag and public init. Hourly compressed query, health, summary and
incident logs remain under `/opt/transparent-5qps-20260929/` on the coordinator.
`status.json` is replaced atomically with the latest summary.

The query process requires a refreshed allow file; a missing, denied or older-than-
45-second permit stops admission. Two consecutive unhealthy samples pause load;
two healthy samples resume noncritical pauses. Three query errors or missed slots
in a trailing minute count as unhealthy. More than two blocks of publication lag
also counts as unhealthy. Critical incidents persist in `latched.json`: wrong
plaintext or protocol decoding, worker binary changes, increased OOM/restarts,
available memory below 15%, or log-filesystem space below 10%. A correctness failure
immediately terminates query generation. Critical latches require investigation;
the watchdog does not rewrite production code or automatically restart workers.

The [systemd unit](transparent-5qps-continuous.service) is enabled across reboot.
It does not automatically restart after a process failure. A persisted critical
latch still prevents query admission when the service starts again.

Operator commands on the coordinator:

```sh
systemctl status transparent-5qps-continuous
cat /opt/transparent-5qps-20260929/status.json
systemctl stop transparent-5qps-continuous
```

The runner initially used a transient service. Four deliberate load-client restarts
installed the permanent unit, the tested watchdog update, bounded transport
recovery, and a scheduler correction. Production services
were not restarted. These control gaps and the initial health-gate pauses remain
in the logs; this is not an uninterrupted-window claim. The watchdog update fixes
an operator-stop race that could otherwise record a false critical child-exit
incident, and preserves critical latches across future restarts.

## Validation and observation boundary

Linux release and macOS native example tests passed (four each); five watchdog
tests verify wrong-row termination, transient-error classification, decode/prepare
failure classification, first-incident retention and stop behavior. macOS Clippy
passed with warnings denied. Linux Clippy was unavailable in that toolchain.
Tests include refusing retries for protocol/plaintext failures and identifying
closed connections as retryable. The normal live run checks both the real rate and
actual native decoded responses.
Full CI was not used for this operator-only rollout.

The retained snapshot is an initial observation prefix of an ongoing load run,
not a terminal result or accepted sustained operating envelope. The watchdog
continues monitoring after the snapshot; future health must be read from the live
status and incident logs. [analyze.py](analyze.py) summarizes retained raw query and
health records without counting intentional control gaps as active-rate time.

## Transport incident and recovery change

The initial single-attempt runner observed four transport failures. The first,
at `1790653975.207106` (03:52:55.207 UTC), overlaps a Caddy 2.6.2 reload at
millisecond resolution; the [router log](transport-incident-router.json) preserves
that observation. The original error omitted its underlying cause, so this is
correlation rather than a traced proof of the failure mechanism. Three later
transport errors within one minute caused the watchdog to pause at 03:55:14.930
and resume at 03:56:13.231 after healthy samples. All raw failures remain retained.

The updated runner records complete error chains and retries connection/request/
body transport interruptions up to three total attempts, scheduling retries within
the existing five-POST-per-second budget. Every retry prepares a fresh key.
Application decoding, revision binding and plaintext mismatches are not retried.
This makes the load resilient to recoverable transport interruption without
concealing raw availability errors or claiming that router reloads never interrupt
a connection. It does not change the production wallet's existing retry API.

## Scheduler correction

The original ideal-time producer slowly advanced ahead of the minimum-spacing
send gate: recorded admission lag grew despite approximately five actual starts
per second. The final scheduler acknowledges each actual request start and places
the next slot relative to it. It waits for admission, not completion, so HTTP
requests still overlap up to the worker limit. At most one job waits for admission,
and an admission stall remains visible as a missed slot. This prevents a growing
client-only queue from undermining a continuous run. The initial compiler error in
this operator-only change is retained with the passing corrected build and tests;
no failing build was installed.

## Live handoff check

[handoff-check.json](handoff-check.json) records the later enabled/running service
and outcomes after the frozen snapshot. Two more connections closed before their
responses completed; the corrected scheduler retried each on the next scheduled
slot, approximately 200 ms later, and both rows decoded exactly. Two delayed
client-admission slots are also retained. These observations supersede any inference
that the final runner never sees transport interruptions or scheduling jitter;
they show recovery without exceeding the configured request rate. No health gate
was active at the handoff check. Automated load and monitoring remain running.
