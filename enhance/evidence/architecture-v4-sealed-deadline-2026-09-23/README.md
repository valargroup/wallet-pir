# Sealed campaign cold-preparation deadline failure

The first sealed campaign failed before any measured publications. The report
records a send error on worker 2's `/internal/v4/prepare` request. The coordinator
used a shared 180-second HTTP timeout. One-second samples show worker 2 busy for
approximately 186 seconds in its final preparation interval; the send failure
occurred just before that interval ended. This supports a preparation-deadline
failure. The original error string does not expose the reqwest source chain, so
it alone does not establish the cause.

Both worker traces recorded no OOM, hard-limit or swap events. Sampled resident
plus kernel peaks were 5,046,218,752 and 4,926,734,336 bytes. These are initial
preparation measurements, not six-sealed placement or six-hour qualification.

The supervisor observed the terminal failed service, stopped the helper processes
and samplers, restored canonical worker state and restarted canonical serving.
All four public/private restoration cases completed successfully; their
reports and completion marker are included.

The fix gives only preparation requests a bounded 600-second deadline, in both
ordinary publication and committed-candidate recovery. Existing default control
and explicit query deadlines remain unchanged. The regression test demonstrates
that a slow response exceeds the client's default deadline but succeeds through
the preparation request path. Server library regression: 75 tests passed. The
new binary has now been deployed as described below.

## Fixed candidate deployment and new campaign

Revision `b1863b1d270df52d213d7dd389e5b8bf96bca224` was built in the local Linux
container with locked offline dependencies and the full release profile. All
65 selected Rust source/Cargo inputs matched the committed checkout. Unchanged
CLI/load-test binaries were reused from the earlier verified clean build. The
bundle was assembled and independently extracted with the release verifier, then
checksummed and its native CLI executed on the coordinator and both workers.
`identity.json` and `build-inputs.json` record provenance; this is not remote CI.

Services and worker sampling receipts now name this revision and its binary hash.
Canonical public load after deployment passed 460 measured and 48 warmup answers,
zero wrong answers/errors, with 234.623 ms measured p99 at concurrency two.

A fresh six-sealed campaign is running as
`enhance-pir-v4-sealed-retry-campaign.service`, initially `building`. Its directory
is `/srv/enhance-pir-v4/validation/sealed-retry`, and worker samplers write
`/srv/enhance-pir-v4/validation/sealed-retry-samples`. It requests 21,600 measured
seconds and 300 publications. Canonical serving is temporarily stopped again,
with state preserved in `worker.canonical`; the supervisor restores it afterward.
Coordinator-hosted helper replicas remain test support, not qualified c-4 hosts.
The failed synthetic worker directories were removed only after retaining their
measurement evidence, to restore at least 32 GiB free disk before this new run.
No passing sealed qualification result exists yet.

The retry completed initialization and started measurement at
2026-09-23 13:17:31 UTC (`retry-measurement-start.json`). Initial ready health
records two published replicas for both the physical-worker group and the
coordinator-hosted support group. This advances beyond the first attempt's
preparation failure. The earliest six-hour completion is 19:17:31 UTC; 300
measured publications and final resource/correctness assessment are still required.

## Retry initialization sampling

`retry-initial-worker-{1,2}-summary.json` summarizes complete prefixes of the
worker-local traces during cold preparation: 532 and 533 samples respectively.
The original traces remain in the remote sampling directories above; exact
prefix snapshots are retained locally under
`.local-pir/v4-production-rollout/sealed-retry-initial-snapshots/worker-{1,2}.jsonl`.
Each summary binds the consumed prefix with SHA-256 and records its wall-clock
endpoints. These are incomplete initialization observations, not a measured
six-hour campaign or proof that all six sealed shards are serving.

No sample errors or gaps over three seconds were observed in these prefixes.
RSS plus kernel peaked at 5,034,516,480 and 4,829,184,000 bytes; worker swap was
zero. Both workers had memory.high reclaim events, and host-wide swap counters
increased slightly. Host counters include other processes and must not be
reported as worker swap. Hard-limit/OOM event deltas were zero.

The read-only summary tool accepts a copied trace (never a concurrently modified
input):

```sh
python3 enhance/ops/scripts/summarize-v4-samples.py worker-1.jsonl
# Optional endpoint coverage check, using nanoseconds from the workload report:
python3 enhance/ops/scripts/summarize-v4-samples.py worker-1.jsonl --window START_NS END_NS
```

The optional window only checks capture endpoints; statistics cover the entire
supplied trace. This tool does not replace placement/publication checks or issue
qualification. Eight tests cover gaps, collection errors, restart/counter reset,
resident guard violations, swap/OOM, pressure deltas, missing window coverage,
invalid JSON and incomplete records.
