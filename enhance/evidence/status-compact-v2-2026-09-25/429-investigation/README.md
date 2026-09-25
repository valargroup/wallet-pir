# Live 429 investigation

## Confirmed rejection source

A 180-second passive loopback capture on the P4000 observed **10 HTTP 429
responses from router port 8482 and zero from worker port 8481**, with zero
kernel capture drops. All ten correspond to client errors within 21 ms. Five
were in a single 75-ms router response burst. The coordinator forwards downstream
status codes unchanged, explaining why clients report its port 8480.

The capture filter selected only HTTP response starts with status 429:

```text
tcp and (src port 8481 or src port 8482)
and tcp[((tcp[12] & 0xf0) >> 2):4] = 0x48545450
and tcp[((tcp[12] & 0xf0) >> 2)+8:4] = 0x20343239
```

Only tcpdump packet summaries were retained (96-byte snap length, no payload
printing). `status-429-only.txt` and its stats record the observation.

The deployed router has four shared query permits. `query()` calls
`try_acquire_owned()` and returns 429 immediately if all four are held. Each
permit remains held through query decoding, the worker HTTP request, and CPU
response packing. There is no waiting queue. Coordinator admission has 16
permits; worker admission has four. Preparation has a separate mutex and can
also return control-plane 429s, but those are not directly forwarded as client
query responses. The captured query failures originated at router admission.

Earlier failures cannot all be attributed individually from the original client
log because it records neither rejection role nor phase. They have the same
query-429 signature; direct attribution above applies to the captured window.

## Contributors

### Preparation shares query packing resources

In a 636.959-second snapshot covering 12,738 arrivals and 28 errors, 17 errors
coincided with router preparation, none with worker preparation, and 11 were
outside either interval (100-ms timestamp tolerance). Router preparation covered
147.688 seconds, or 23.19% of the window. `analysis.json`,
`errors-snapshot.jsonl`, and `preparation-intervals.jsonl` retain the calculation.

Router preparation uses Rayon parallel preprocessing. Query response packing
also uses the same process-global Rayon pool in ipir-sp rc.5. The deployed pool
has four threads. Heavy preparation therefore competes with admitted query
packing, extending how long the four admission permits remain held. The code
and timing correlation support this as a contributor; no CPU profile or
controlled isolated-pool experiment has yet established its exact causal share.

### The offered schedule sometimes becomes a launch burst

The harness schedules one arrival every 50 ms, but overdue `sleep_until` calls
return immediately and launch catch-up work. In the matched failure burst:

| Arrival | Scheduled epoch ms | Inferred actual task start epoch ms |
| --- | ---: | ---: |
| 14218 | 1790350104063 | 1790350104370.805 |
| 14219 | 1790350104113 | 1790350104370.859 |
| 14222 | 1790350104263 | 1790350104370.595 |

Three requests scheduled across 200 ms started within 0.3 ms. Actual start is
inferred as `completed_ms - duration_ms`, subject to clock rounding. The cause
of the scheduler stall itself is not yet established. Synchronous wallet work
runs inside the async tasks and warrants profiling.

This also exposes a measurement limitation: `duration_ms` starts inside the
spawned job and excludes scheduled-to-start delay. Report scheduled-to-completed
latency separately; do not silently omit that delay from the complete-lookup gate.

## Recommended changes

1. Add rejection counters by role/reason, active/queued gauges, and admission-wait
   and packing histograms. Preserve origin attribution across forwarding.
2. Give router preparation a bounded dedicated CPU pool/budget so it cannot
   monopolize query packing threads.
3. Add a small bounded admission queue with a deadline charged to total lookup
   latency. Size it from measurements; increasing concurrency alone may worsen
   CPU contention and memory pressure.
4. Record launch lateness and scheduled-to-completed latency, move heavy wallet
   work off async executor threads, and distinguish paced load from deliberate
   burst tests. Account for every offered arrival in both cases.
5. Repeat the joint 20-QPS test before starting a new qualification soak.

No serving code, concurrency limit, workload rate, or running service was changed
for this investigation. The original soak remains diagnostic and public Status
remains disabled. Full raw request evidence remains on the coordinator at
`/var/lib/status-pir-controller/compact-v2-soak/requests.jsonl`.
