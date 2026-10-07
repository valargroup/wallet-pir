# Tiered txid display proof of concept: production, 2026-10-06 to 2026-10-07

The tiered txid display publication
([design](../../docs/txid-display.md#tiered-display-publication-proof-of-concept),
[local evidence](../txid-display-tiered-2026-10-05/README.md)) was deployed on
production beside history on 2026-10-06. This run holds the deploy record, the
live controller data through 2026-10-07 20:04 UTC, and the 20 QPS load and
bandwidth measurement of 2026-10-07. It is not an acceptance result. Anonymity,
archive immutability and growth were not measured live
([gates](../../docs/remaining-work.md#tiered-txid-display-proof-of-concept-2026-10-05)).

## Deployment, 2026-10-06

- **Release.** `d191f86b` (CI full run 37388223909). `e8178fc4` points the
  deploy tool at history's v11 adapter, fleet and membership paths. The
  2026-10-03 v11 cutover had moved them; the tool still named the dead pre-v11
  paths. `d191f86b` only fixed a clippy lint on top.
- **Request and plan.** [request.json](deploy/request.json), canonical sha256
  `5776cd98…a302ef`; [plan.json](deploy/plan.json), plan sha256 `a4a17d76…51362f`
  (both recomputed here). Rendered files in [deploy/render](deploy/render).
- **Terraform root.** Before the firewall step, the operator synced
  `transparent.tf` and `variables.tf` in the coordinator's Terraform root
  (`/opt/enhance-pir/infra/production`) to `main`. They were one commit behind
  and lacked only the default-off port 8095 variable. Backups are in
  `.backup-txid-display-20261006` there. Operator report; no log is retained.
- **Ten transactions**, started between 12:31 and 13:23 UTC and all committed
  by 13:26, in order:
  `stage`, `ingest-smoke`, `ingest-start`, `firewall`, `router-hook`, `bootstrap`,
  `workers`, `route`, `measure-start`, `controller`. Journals with ids and undo
  steps are in [deploy/transactions](deploy/transactions); command output in
  [deploy/log](deploy/log).
  - `firewall` applied saved plan `04706a36…cf5fdf516`: one in-place update of
    `digitalocean_firewall.transparent_worker`, adding TCP 8095 from the
    `transparent-pir-router` and `wallet-pir-coordinator` tags. Nothing removed.
  - `router-hook` replaced the live v11 adapter `transparent-live-fleet.py`
    (sha256 `f3df5c53…`, identical to git `4c85b6c2`) with main's (`35b6b436…`,
    +20/−2: the opt-in `route_imports` key). It also added `route_imports` to
    `/opt/transparent-publisher/v11/fleet.json`. History was otherwise unchanged.
  - `controller`: a 280 s shell timeout killed the operator's command after the
    transaction had committed (13:26:18 UTC journal time). Only the tool's final
    printout was lost; the log for that command is empty.
- **Backfill.** The ingest covered 3,407,000 to 3,507,335: 100,336 blocks in
  695 s ([log](deploy/log/20261006T125709Z-txid-display-ingest-status.log)).
- **Bootstrap.** `plan-start` chose 3,407,001–3,489,633: 556,658 records,
  9 archives at bootstrap, 4 replay seals, 0 expected drops. Bootstrap map
  `03cbd94c…`; its recent shard held 50,004 records in 1 directory segment.
- **Replay.** The controller replayed to the node tip with 4 seals (shards 9–12),
  13 archives, 0 failures and 0 drops. Its last lag record is at 13:49:47 UTC.
- **Seal parameters.** `archive_target` 40,000 (the local bench recommendation;
  the plan default is 20,000), `recent_floor` 10,000, `reorg_margin` 100,
  `max_archive_shards` 24, N=1 buckets, `txid-2k`.
- **W0, before deploy** (2026-10-05 23:05 UTC, [w0](deploy/w0-20261005T2305Z.json)):
  history 5 QPS with p99 33 ms, 300/300 exact; history freshness 12.5 s.

## Live controller data

[live/timeline.jsonl.gz](live/timeline.jsonl.gz) is the controller timeline
from 2026-10-06 12:59 to 2026-10-07 20:04 UTC. [summary.json](summary.json) is
computed from it by [harness/live_freshness.py](harness/live_freshness.py).
Block to serving (`freshness_ms`) is the time from the controller first seeing
a block to the recent worker serving a map that includes it.

A cycle counts as live when its tip is above 3,508,457, the height the deploy
session recorded as replay reaching live. Single-block cycles started earlier,
after about 13:55 UTC, so this start is conservative. The window ends at
2026-10-07 13:40 UTC. That is before the load windows and the recent-01 drop-in
([2026-10-07 change](../txid-display-freshness-2026-10-07/README.md)).

| Live window, 2026-10-06 15:27 to 2026-10-07 13:38 UTC | Value |
|---|---|
| Cycles (worst block per cycle) | 1,054 |
| Block to serving p50 / p95 / max | 16.0 / 18.8 / 97.2 s |
| Cycles over 20 s | 22 |
| Blocks over 20 s | 25 of 1,076 |

The operating session counted 1,055 cycles over the same window, with the same
p50, p95 and max. The difference was not resolved.

- **Worst cycle.** 97.2 s at 2026-10-06 20:00:39. It followed the only
  controller error: "recent-replica recent: adapter failed: exit status: 1" at
  20:00:03. The map watcher saw two map errors at 20:00:03 and 20:00:13.
- **Seals.** 13 in all: 9 at bootstrap and 4 during replay. None live: the
  recent shard held 48,761 records at 20:04 UTC, and a seal needs about 50,000
  (40,000 plus the 10,000 floor).
- **Reorgs.** 7, all one block deep: 1 during replay and 6 live (5 inside the
  window). No drops: 13 archives against a window of 24.
- **Map watcher.** 1,525 maps observed. 6 map errors: 2 at the controller error,
  1 at 2026-10-06 23:02, and 3 at 2026-10-07 14:47 during the recent-01 restart.
  6 manifest 409s for the recent shard, after its revision moved.
  Raw: [mapwatch.jsonl.gz](live/mapwatch.jsonl.gz), the observed maps and
  manifests in [mapwatch-objects.tar.gz](live/mapwatch-objects.tar.gz), and the
  node tip observer [observer.jsonl.gz](live/observer.jsonl.gz).

## Load and bandwidth, 2026-10-07

Raw capture in [measure](measure). Its [README](measure/README.md) is the
measurement agent's capture note. It was written at 14:44 UTC, after the
window A stop, and is not updated for the later windows. Its
[SHA256SUMS.capture](measure/SHA256SUMS.capture) predates the rerun:
`events.jsonl`, `history-samples.jsonl`, `recent-cpu.jsonl`, `supervisor.log`
and `tools/supervise.py` were appended or edited afterwards, and
`runs/BW-bandwidth/` is not in it. Large `.jsonl` and fixture files are
gzipped here; decompressed, the unchanged ones match that file.

Client: `txid-rate` and `txid-bandwidth` from `d191f86b`, built on
`roman-ipir-bench-8vcpu` (Amsterdam, 8 vCPU) with target-cpu=native, against
`https://transparent-pir.valargroup.dev`. Four processes, mix 80% recent and 20%
archive, fixture of 4,000 natural samples plus 200 absent controls.
A supervisor on the Mac read history load status, controller status and
recent-01 CPU, and stopped load on the brief's rules.

| Window (UTC) | Workload | Result |
|---|---|---|
| V0, 14:04:15–14:05:15 | 5 lookups/s, cold every 25 | 304/304 exact, 0 errors, p99 136 ms |
| A, 14:32:33–14:41:33 | 20 queries/s (one directory query each), warm | 540 of 600 s. 10,800/10,800 exact, 0 errors. p50 42 ms, p99 130 ms, max 1.14 s. **Hard stop**, below |
| B, 15:01:34–15:11:34 | 20 lookups/s, cold every 25, absent every 20 | 12,004/12,004 exact (11,404 found, 600 absent), 0 errors, 0 missed. p50 55 ms, p99 187 ms, max 532 ms. 24,311 queries |
| BW, 15:43:40–15:44:48 | `txid-bandwidth`, per-class fixture | 149 lookups, below |

- **Window A stop.** At 14:41:18 router-01 reloaded Caddy twice, a routine
  history activation. One history pages query got "Connection reset by peer"
  and recovered on retry. The supervisor's rule stopped on any history error.
  Roman then approved B and bandwidth with an amended rule: only unrecovered
  history errors stop.
- **Window B by lookup.** Inline p99 183 ms, one page 234 ms, absent 189 ms.
  Recent p99 198 ms, archive 98 ms. Cold p99 121 ms over 484 cold lookups.
  Recent-01's display worker used 0.65 cores during B, against 0.20 before.
- **History during load.** Trailing-60 s p99 at most 60 ms in A and 75 ms in B,
  with 0 errors in B. Between windows with no txid load, its median p99 was
  38–48 ms. W0's single sample was 33 ms.
- **Block to serving during A.** Four cycles at 20.4–24.0 s, all over 20 s;
  prepare 19.5 s p50. Window B ran after the recent-01 drop-in: 8 cycles, max
  12.7 s. B is not a measurement of that change.
- **BW run timing.** `events.jsonl` records a bandwidth window at 15:23:24–15:26:19.
  The supervisor tripped at 15:26:36 when status became unreadable. The
  retained `bandwidth.json` is from a later run at 15:43:40–15:44:48
  ([window.json](measure/runs/BW-bandwidth/window.json)); no supervisor samples
  cover it, and its driver script on the bench host was not captured.

Bandwidth per lookup, over HTTPS through the public router, all requests for
one txid, maximum measured bytes per class
([bandwidth.json](measure/runs/BW-bandwidth/bandwidth.json)):

| Class | Warm | Cold | Under 300 KB |
|---|---:|---:|---|
| inline | 92.8 KB | 132.4 KB | both |
| pages-1 | 139.2 KB | 199.2 KB | both |
| pages-2 | 185.6 KB | 245.6 KB | both |
| pages-3 | 232.0 KB | 292.9 KB | both |
| pages-4 | 278.5 KB | 343.7 KB | warm only |
| pages-5 | 325.7 KB | 386.6 KB | neither |
| pages-6 | 372.2 KB | 445.1 KB | neither |
| pages-7 | 417.7 KB | 496.0 KB | neither |

Measured bytes exceed computed bytes by 220 B (warm inline) to 25.1 KB (cold
pages-7); `computed_differs_from_measured` is true for all 149 rows. Stale-map
retries: 0. Lookups with a stale recent map (`recent-stale`) cost at most
0.8 KB more than warm. Recent has no pages-5 or pages-6 records; recent pages-2 has 1
sample and pages-4 has 2. Derived from the fixture's class counts at
3,509,536: 94 of 567,323 published txids (0.017%) are pages-4 or more, and 31
(0.005%) are pages-5 or more.

## Supervisor trips with no txid load

| Time (UTC) | Trip | Cause |
|---|---|---|
| 14:11:55 | history exact 225 | history load self-paused 14:11:32–14:12:02: "publication more than two blocks behind node" |
| 14:18:39 | status unreadable twice | two 12 s SSH timeouts from the Mac (RTT about 270 ms) |
| 14:53:21 | history p99 845 ms | history queries to shards 71–88 at 0.3–1.45 s; no reload or membership event |
| 15:26:36 | status unreadable twice | after the first bandwidth window |

The operating session reports, from history load status not retained here,
history p99 over 300 ms in 213 minutes of about 40 hours without txid load,
maximum 2.77 s.

## History router reloads

[measure/reloads](measure/reloads) holds router-01's Caddy reloads and the
coordinator reconciler's `worker_membership` events from 00:00 to 14:41 UTC.
[tools/pairing.py](measure/tools/pairing.py) assigns each reload to the nearest
membership event within 15 s: 84 reloads for 42 activations, exactly 2 each.

Diagnosis, from the session that made the fix: recent-01 finished prepare after
the 3.5 s `prepare_grace_seconds`, and its activation waited on the
reconciler's worker lock and was cancelled at the 0.5 s default activation
grace. Changes to `/opt/transparent-publisher/v11/fleet.json`, approved by Roman:

1. 14:48:40, `activation_grace_seconds` 3.0 added.
2. 15:45:35, `prepare_grace_seconds` 3.5 → 7.0. The same edit removed
   `activation_grace_seconds` by mistake. The diagnosis at that point
   attributed the pairs to prepare alone.
3. 16:53:10, `activation_grace_seconds` 3.0 restored.

With both settings, that session reports 147 publishes, 0 cancelled
activations, 0 rejoins and 0 router reloads through 19:57 UTC. Those counts
are not retained here.

## Open at capture

At 2026-10-07 19:53:04 UTC history's 5 QPS load latched on
"transparent-coordinator: disk headroom below 20%"
([latched.json](live/latched.json), read at 20:08). The operating session
attributes it to a separate full-chain genesis display ingest into
`/srv/zakura/txid-display-genesis` (25 GB at 12%), stopped at about 20:00 UTC.
It reports `/srv/zakura` at 80% used with 208 GB free. The latch remains.
At 20:12 UTC, after that journal moved to its own 150 GB volume
(`/srv/txid-display-genesis`), `/srv/zakura` had 233 GB free (78% used); the latch
was unchanged and clearing it awaited Roman's decision.

## Provenance

- Deploy state was kept on the operator's Mac outside the repository
  (`~/.config/wallet-pir-deploy`); `deploy/` copies its request, plan, render,
  journals, logs and W0. The release tarball is not copied; its sha256 is in
  the request.
- Live data was copied read-only from the coordinator at 20:05–20:08 UTC on
  2026-10-07 with pinned SSH options. Nothing on any host was changed by this
  capture.
- The deploy journals and logs contain host paths and digests only; no
  credentials. The `router-hook` journal holds the previous adapter source and
  the previous history `fleet.json`.
- Machine-readable metadata: [manifest.json](manifest.json); checksums:
  [SHA256SUMS](SHA256SUMS).
