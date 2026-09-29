# Archive tier from two owners to one, 2026-09-29

Production, deployed over SSH without CI at the owner's request. The archive
(shards 0–76) moved from `transparent-pir-archive-01` (0–38) and
`transparent-pir-archive-02` (39–76), both worker `0ece0ae1`, to one owner,
`transparent-pir-archive-03` (0–76, droplet 604693648, `m-8vcpu-64gb`, worker
`a704616c`). The same combined 20 QPS measurement ran before and after. The
recent tier stayed at two replicas (`recent-01`, `recent-02`): the scaler ran in
`observe` for both arms and returned to `act` afterwards.

## Measurement

Harness in [harness/](harness/). Clients ran on the 8 vCPU build host against
the public URL, as independent processes with their own admission loops, on top
of the continuous 5 QPS load. The mixed run used the committed `rate-query`
(default mix: 80% recent, 20% archive); the archive-heavy run used a client
built from an uncommitted `--mix archive` change (sources and binary digests in
`armA-archive/client.sha256` and below). Each run lasted 10 minutes.
`cmp-sample.py` sampled every worker, the publisher and the scaler every 15 s;
`cmp-analyze.py` produced each `analysis.json`.

| Run | Topology | Clients | QPS | Queries (all exact) | Failed | Missed slots | p50 | p95 | p99 | Max |
|---|---|---|---|---|---|---|---|---|---|---|
| [armA-mixed](armA-mixed/analysis.json) | 2 archive owners | 3 × 7, mixed | 20.9 | 12,530 | 0 | 46 | 17 ms | 37 ms | 50 ms | 95 ms |
| [armA-archive](armA-archive/analysis.json) | 2 archive owners | 4 × 5, archive | 19.5 | 11,711 | 0 | 0 | 16 ms | 28 ms | 33 ms | 58 ms |
| [armB-mixed](armB-mixed/analysis.json) | 1 archive owner | 3 × 7, mixed | 20.9 | 12,518 | 0 | 60 | 14 ms | 31 ms | 45 ms | 97 ms |
| [armB-archive](armB-archive/analysis.json) | 1 archive owner | 4 × 5, archive | 19.6 | 11,730 | 0 | 0 | 18 ms | 29 ms | 35 ms | 139 ms |

Archive workers during the runs (server-side, from the samples):

| Run | Owner | Queries | Mean evaluation | CPU | RSS | Min available memory |
|---|---|---|---|---|---|---|
| armA-mixed | archive-01 / archive-02 | 1,678 / 1,444 | 9.1 / 11.5 ms | 0.17 / 0.18 cores | 17.3 / 17.0 GiB | 75% / 75% |
| armA-archive | archive-01 / archive-02 | 6,254 / 6,077 | 9.3 / 11.7 ms | 0.60 / 0.71 cores | 17.4 / 17.0 GiB | 75% / 75% |
| armB-mixed | archive-03 | 3,121 | 10.4 ms | 0.34 cores | 34.1 GiB | 52% |
| armB-archive | archive-03 | 12,350 | 11.3 ms | 1.41 cores | 34.1 GiB | 52% |

- Every run passed the gate (p99 under 2 s, p50 under 700 ms, every query
  exact, no failed query, no worker restart, archive memory above 25%). Missed
  slots stayed under 0.5%, so the client did not bound the rate; the earlier
  single-client prototype comparison was client-bound at about 13 QPS.
- The single owner carries the whole archive load at 1.4 cores of 8 and a 12%
  query-slot duty cycle at about 20 archive queries/s. Evaluation per query is
  unchanged. The confounder is the worker release: the old owners ran
  `0ece0ae1`, the new one `a704616c` (runtime builds in a separate low-priority
  pool); archive owners rarely rebuild, so it matters little here.
- Publication freshness had the same median in both arms (15–17 s). The 29.5 s
  maximum in armB-mixed was block 3500499, whose publish cycle took a normal
  14.5 s; the delay preceded publication.
- Offered load of 25 QPS would have made the scaler run four recent replicas
  (three under archive-heavy load), recorded as `scaler_desired_seen`.

## Cutover

| UTC | Event |
|---|---|
| 15:30–15:35 | Terraform: `count` to named archive owners as a moves-only plan (0/0/0), then `transparent-pir-archive-03` added (1 add, 1 in-place project change) ([plans](cutover/terraform-plans.txt)) |
| 15:35 | Host key pinned from the VPC (the worker firewall closes public SSH); the fleet deploy key installed by hand, since the production tfvars do not set `transparent_worker_deploy_public_key` |
| 15:36–16:10 | Standby warm (`transparent-archive-standby.py`). The first copy failed when its source publication was pruned (the coordinator keeps about nine); the rerun hard-linked what had arrived and finished. Warm from service start: 1,596 s at one build slot ([summaries](cutover/standby-summaries.json)) |
| 16:14–16:20 | Standby rerun onto a current publication: restart from the disk runtime cache, warm in 291 s. `repartition` still refused: that publication had been pruned too ([first attempt](cutover/repartition-first-attempt.txt)). Fixed in `548f541e`: the standby check reads the served map's shard list from the worker |
| 16:22:10 | `repartition` wrote inventory revision 19 (from 18): partition `a2` 0–76, archive-03 enrolled, archive-01/02 retired ([18](cutover/inventory-revision-18.json), [19](cutover/inventory-revision-19.json)) |
| 16:24:16–17 | Block 3500495: archive-03 prepared in 0.19 s from hard links with all 154 runtimes warm; activated; router reloaded to archive-03 only |
| 16:24:17–16:24:29 | Router dials to `10.142.0.7:8093` timed out until 16:24:25 ([router log](cutover/router-health-16-24.jsonl)); passive health kept the host out a few seconds longer. Twelve synthetic archive queries (shards 2–7, directory and pages) failed with the router's 503, and the 5 QPS supervisor paused from 16:24:39 to 16:25:51 ([records](cutover/synthetic-5qps-16-24.jsonl)). No wrong row; the recent tier was unaffected |
| 16:26–16:47 | Arm B (after Arm A, 15:02–15:24, on the two-owner topology) |
| 16:47–16:52 | `pir-apm` restarted; it samples archive-03. Scaler back in `act` under the production policy |

The dial timeouts are attributed, not proven: archive-03 reused the VPC address
of `transparent-pir-recent-03`, destroyed at 13:3x, and a router that still held
its neighbour entry would send the first packets to the old MAC until probing
failed over (about 5 + 3 s). Neither host nor cloud firewall filters 8093, and
the worker logged no refusal. `dbfb722c` makes the standby tool end by dialling
the new owner's `/v1/ready` from the router, which proves the path and
refreshes the entry before a cutover.

## Rollback and removal

Until archive-01 and archive-02 are stopped, `transparent-fleet-inventory.py
--archive restore --revision 18` returns the archive to them at the next
publication. They were left running and unrouted after the measurement pending
the owner's confirmation to remove them.

## Provenance

Scripts `3bbf35a3`, `548f541e`, `dbfb722c`; worker `a704616c`
(`transparent-shard-server` sha256 `4e3f8d9d…e81c`). Mixed client: committed
`rate-query` built at `a704616c`. Archive client: sha256 `dccd6ef0…7b49`, built
from `rate-query.rs` sha256 `3e9b5e83…5b2a` (an uncommitted `--mix` change) on
the `a704616c` tree. The archive-heavy runs depend on that uncommitted source.
