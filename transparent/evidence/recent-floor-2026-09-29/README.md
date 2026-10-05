# Recent tier from four replicas to two, 2026-09-29

Track A phases 1 and 1b ([remaining work](../../docs/remaining-work.md#track-a-elastic-recent-replicas-approved-2026-09-29)).
Production, deployed over SSH at the owner's request without CI or soak.

## Phase 1: inventory and pinned placement

At 13:13 UTC the fleet inventory was seeded from the live roster and the active
assignment (`transparent-fleet-inventory.py init`, source `f990cd97`). The
generated roster matched the live hosts exactly and pinned the archive owners
to the ranges they already held (0–38 and 39–76). The first publications planned
from the inventory produced worker rows byte-identical to the last plan before
it; every worker stayed routed and freshness stayed at 14–15 s.

## Phase 1b: the 20 QPS gate on two replicas

recent-03 and recent-04 were drained (`drain`) and stayed warm; the router sent
recent traffic to recent-01 and recent-02 only. The client ran on the build host
(8 vCPU, `roman-ipir-bench-8vcpu`) against the public URL, with the fixture of
the continuous 5 QPS load (80% recent, 20% archive, sealed revisions, fresh keys,
exact row hashes). Worker counters were sampled on the coordinator before and
after each run (`snap-metrics.sh`); `analyze-gate.py` summarizes a run.

| Run | Worker build | Clients | Achieved QPS | Queries (exact) | Errors | p50 | p99 | Max | Recent p99 |
|---|---|---|---|---|---|---|---|---|---|
| `gate-n2-aborted-coordinator-client` | `dbeb3960` | 1 on the coordinator | stopped | – | – | – | – | – | – |
| `gate-n2-before-build-pool` | `dbeb3960` | 1 | 18.2 | 10,918 (all) | 0 | 20 ms | 0.95 s | 3.28 s | 1.06 s |
| `gate-n2-build-pool` | `a704616c` | 3 × 7 QPS | 20.9 | 12,519 (all) | 2 transport, retried | 19 ms | 53 ms | 0.24 s | 52 ms |

- The first attempt ran the client on the coordinator under a 3-core quota and
  without refreshing its permit file; the client paused itself after 45 s. It is
  kept as a failed attempt.
- The single-client run was client-bound: preparing an archive page query takes
  about 96 ms, longer than the 50 ms interval, and the client's admission loop
  handles one preparation at a time (5,677 missed slots). Three clients at 7 QPS
  each remove that limit.
- Every slow query of the first full run fell in the roughly ten seconds before
  a publication's activation, while every recent replica rebuilt its tail
  runtimes. Construction shared the global rayon pool with query evaluation and
  took every core of the 4-vCPU hosts. `130c84b8` moves it to a dedicated pool of
  half the cores at nice 10; the second run measures that build.
- Recent-replica slots were busy 7–9% of the time at about 7 QPS each before the
  build-pool change and 3–4% after it, at about 8.3 QPS each.
- The two errors of the second run were `connection closed before message
  completed` on keep-alive connections, each within 50 ms of a router
  configuration reload at a publication (router journal), and both were retried
  exactly. Every worker counted zero errors. Reloads happen whenever routing
  changes at activation; they predate this change and are not specific to two
  replicas.

The gate (p99 under 2 s, p50 under 700 ms, every query exact) passed on two
replicas, so the floor stays at two and `gate_min` is not raised.

## Retirement

After the second run recent-03 and recent-04 were retired (drained for more than
600 s), their services disabled, and `transparent_recent_count` reduced from 4 to
2 with a saved plan through `wallet-pir-terraform.sh` under the production lock.
The plan was checked before apply to destroy exactly those two droplets and only
remove their URNs from the project (`terraform-recent-2/`): 0 added, 1 changed,
2 destroyed. The continuous 5 QPS load was restarted on the roster-following
supervisor with the new worker identities.

## Rollback

The drains were cancellable (`undrain`) until retirement. The inventory's
revisions are in `state/inventory.d/`; the pre-inventory fleet script, roster,
`fleet.json`, known hosts and `shard-assign` are in
`/opt/transparent-publisher/rollback/track-a-f990cd97/`.
