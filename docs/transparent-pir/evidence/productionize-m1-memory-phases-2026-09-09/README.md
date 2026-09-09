# M1 memory phases — 2026-09-09

Decision: retain the source improvements, but do not promote two build slots or
start a replacement canary. Only one of three two-slot repetitions passes the
combined timing/query-availability screen. M1 remains open.

## Changes

After dropping completed construction buffers and plaintext, atomically shrink
transient admission accounting to one retained runtime allowance plus 2 MiB of
writer scratch. Release the construction slot before snapshot persistence. The
blocking task owns the reduced reservation and runtime pin through disk locking,
saving and cancellation. The original four-runtime construction reservation,
90%-of-MemoryMax admission ceiling and memory limits are unchanged.

Linux advises release of consumed source/cache files and synced snapshot output
with safe `rustix::fs::fadvise(..., DontNeed)`. This affects only opened files,
can be ignored by the kernel, and is never subtracted from actual admission usage.
Non-Linux builds omit the hint. Source loads stop at expected length plus one byte
before length/SHA validation. Snapshot checksums, coefficient/identity validation,
format, atomic rename and durability barriers remain unchanged.

Handoff alone did not resolve contention: two-slot visibility was 12.373–12.776 s,
retry fractions 35.5–43.6%, and completion gaps 4.076–5.105 s. All six ablation
runs are retained in `handoff/` and included in `comparison.json`.

## Benchmark correction

The first file-cache trial exposed shutdown closing HTTP during a client's final
request. The worker omitted that record, while the external process correctly
failed. `filecache-undrained/` preserves three failed completed attempts and the
aborted remaining work. These attempts do not qualify.

Version 4 keeps HTTP serving until clients flush final JSONL records and write done
markers. Readers drain again after the marker. Missing completion, fatal errors
and truncated records fail qualification. `filecache/` contains six fresh runs
with successful client exit and drain proof. Every final report was compared with
all client records, allowing only floating-point JSON round-trip differences.

The old construction reference reported 211 successes; complete client logs
contain 213 plus 18 retries. Its retry reference is therefore 18/231 (7.79%), and
its maximum completion gap remains 2.089 s. Original raw evidence is unchanged.
`summarize.py` recomputes counts and gaps from complete logs; old report latency
percentiles remain as recorded. This no-regression screen is descriptive, based
on one reference run, not a statistical SLO.

## Final results

| Run | Worst worker visibility (s) | Exact queries | Retry attempts | Max client completion gap (s) | Modeled host headroom | Combined screen |
|---|---:|---:|---:|---:|---:|---|
| repeat-1-slots-1 | 14.209 | 137 | 0 (0.00%) | 1.740 | 26.96% | Fail |
| repeat-1-slots-2 | 11.566 | 95 | 7 (6.86%) | 1.354 | 26.51% | Pass |
| repeat-2-slots-2 | 10.966 | 86 | 7 (7.53%) | 2.371 | 27.28% | Fail |
| repeat-2-slots-1 | 14.417 | 138 | 0 (0.00%) | 1.802 | 26.49% | Fail |
| repeat-3-slots-1 | 14.230 | 146 | 0 (0.00%) | 1.693 | 26.48% | Fail |
| repeat-3-slots-2 | 10.765 | 85 | 5 (5.56%) | 2.182 | 27.37% | Fail |

All 687 successful queries decoded exactly. All runs started with 28 warm runtimes,
overlapped external queries with preparation, completed both publications, and
drained both clients. Cgroup high/max/OOM counters were zero. Peak cgroup usage
was 5.434–5.510 GB; final file cache was 86–111 kB. Headroom models an 8 GiB host
with 768 MiB reserved outside the worker; it is not measured live-host headroom.

One slot avoids retries but misses the provisional 14 s worker screen. Two-slot
retry fractions pass, but two repetitions exceed the reference completion gap.
Successful-query p95 is 0.825–1.013 s for two slots, versus about 0.069 s in the
old reference: lower retries do not establish uniformly faster queries.
In repetition two, both largest client gaps involve a directory query at retained
revision `5b7ee984…`. Logs also record restores of that revision and later memory
admission refusals. This is evidence to isolate those phases, not proof that disk
I/O alone caused the gap.

Next separate retained-revision restore, admission waiting and evaluation during
preparation; optimize the limiting phase and repeat the unchanged coupled screen.
The public 30 s / replica 60 s and actual-host memory targets remain unchanged.
A matching six-hour AND 300-block loaded canary, fleet rollout and observation
remain open. No live binary, configuration or supervisor changed in this work.

## Reproduction and provenance

Existing Amsterdam generator: 16-vCPU Xeon 8280, 32 GiB, Linux 6.8.0-124-generic.
Worker CPUs 0–3; external clients CPUs 4–7 outside its memory cgroup.
MemoryHigh 5,905,580,032 bytes; MemoryMax 7,516,192,768; swap disabled.
Fresh process/cache each run; 5 GiB runtime cache and 10 GiB disk cache.
Fixture: fourteen recent shards 160–173, 28 runtimes, canonical publications
3,477,140 / 3,477,141 / 3,477,142. Manifest records the frozen fixture hash.

Final supervisor `transparent-m1-filecache-drain.service` completed all six runs
and exited 1 because one-slot timing failed. The additional combined screen in
`comparison.json` also rejects two of the two-slot runs.

```sh
python3 ops/scripts/run-transparent-burst.py --systemd --external-clients \
  --worker-budget-seconds 14 --host-overhead-bytes 805306368 \
  --fixture /opt/transparent-full-fixture-20260909/fixture-canonical.json \
  --test-binary /opt/transparent-filecache-drain-20260909/burst-test \
  --source-sha 14f3919a86fab1c2be44877026a9d50270f70e88 \
  --build-slots 1 2 --repetitions 3 \
  --out /opt/transparent-filecache-drain-20260909/measured
python3 docs/transparent-pir/evidence/productionize-m1-memory-phases-2026-09-09/summarize.py
```

Build: Rust 1.91.0 release, `-C target-cpu=x86-64-v3 -C target-feature=+pclmulqdq`.
Root base `14f3919a86fab1c2be44877026a9d50270f70e88` plus captured working source;
packing dependency `61dc83e7410ff13ccfdd9ad1e830b711bd9080ed`.
Three source archives preserve all manifest-listed files, verified against hashes.
Final test executable SHA:
`86013bf092aadddbc0abfdb688f94e10cbc086189806fc31ad7b78f5c1d45ec5`.
Each trial manifest is authoritative for its source, fixture and binary identity.

## Validation

- `make check`: 584 Rust tests pass, zero failures, two ignored manual benchmarks;
  output in `make-check.log`.
- Seven Python runner tests pass, including rejection of missing drain proof.
- Targeted Linux tests cover file advice, snapshot ownership/cancellation,
  disk compatibility/corruption and client drain/error handling; logs in
  `filecache-linux-tests.log` and `drain-linux-tests.log`.
- Three source archives matched manifest hashes. All six complete report/client
  pairs matched, allowing only floating-point JSON round-trip differences.
- `SHA256SUMS` covers the preserved evidence files.
