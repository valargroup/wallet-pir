# Pilot cold pass, 2026-09-08 (run b)

The Gate 2 correctness pass against the pilot worker serving the whole
full-chain set through the public origin, from a laptop over TLS. `report.json`
is the harness report, `harness.log` its console output, `pilot-journal.txt`
the pilot's service journal and unit for the window. This is the cold bound:
the pilot holds all 174 shards behind an 8 GiB cache that fits about
fourteen archive shards, so every old-history restore rebuilds runtimes as
it goes.

| Field | Value |
|---|---|
| Harness commit | `4ba90197e71bd17f33b07b256c811099a68f4a84`; `transparent-loadtest`, steps 2 (and 8, not reached), 4 min, 5 completions per class, 1,500 queries per sync |
| Origin | `https://transparent-pir.valargroup.dev`, pilot `transparent-pir-worker-01` (`s-4vcpu-16gb-amd`, ams3), release `00afb3e`, `--cache-bytes 8589934592`, MemoryMax 12 GiB |
| Set | 174 shards, map `06e5fa2b…` as published; sample `workload-sample-2026-09-08` |
| Client | macOS laptop, residential network, TLS; the wallet crate's `reqwest` adapters |

## Result

| Class | Syncs | Exact | Sync time | Private bytes up + down | Outcome |
|---|---:|---:|---:|---:|---|
| multi-script (40 scripts, from the cutoff) | 1 | 1 | 68 s | 21.3 MB | 532 events recovered, digest and count equal to the journal |
| unused (4 scripts, from genesis) | 1 | 1 | 69 s | 0 (filters 63.2 MB) | no private work; every filter fetched once |
| restore-old (3 scripts, from genesis) | 1 | 0 | 244 s | 14.7 MB before failure | failed: HTTP 502 on shard 35 |
| reused-tail (1 heavy script) | 1 | 0 | 381 s | 421 MB before failure | failed: HTTP 502 on shard 156 after 977 page queries |

Two exact recoveries, two failures; the step stopped on the error rate. The
502s were the pilot's shard server being killed by the cgroup OOM killer at
08:32:04Z after 1 h 46 min under cold load (this run and the abandoned run a
before it): anonymous RSS 12.56 GB against MemoryMax 12 GiB with an 8 GiB
cache budget, so roughly 4.5 GB of build scratch, request state and allocator
overhead on top of the reservation under continuous eviction and rebuild.
Caddy answered 502 for the ten seconds until systemd restarted the service and
the three minutes it took to load the set.

## What this establishes and does not

- Exact recovery through the public origin for a fresh multi-script wallet and
  an unused wallet, with the wallet's own manifest verification in the path.
- The cold bound: a single old-history restore against a cache that cannot
  hold the archive costs minutes and rebuilds per shard, and two concurrent
  ones push the process past a 12 GiB limit within two hours. The fleet holds
  every assigned shard resident, and its warm figures are the operating ones.
- A client gap: the wallet treated the edge's 502 as a hard error. It is now
  treated like an overload signal (stop incomplete, keep pending work), which
  is what the contract asks of a sync under an unavailable service.
- Not established here: correctness of old-history restores end to end; that
  is measured on the warm fleet.
