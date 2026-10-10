# Transparent history and txid display on `9d2cbda0`, 2026-10-10

Second roll of the night, at Roman's request to deploy the latest `main`. History and
txid display moved from [`06a972db`](../fleet-redeploy-06a972db-2026-10-09/README.md)
to the full-CI release of `9d2cbda0` (run 38011976059). New for clients: servers also
accept a selection over only the leading rows of an unsealed tail's pages (2,048-row
blocks, either width) and zero-fill the rest; the scan and the response equal the
full upload's. Sealed shards, directories and txid display keep the full selection.
The shared-crate refactor from the receiver merges changed every binary, so this time
the publish controller was swapped too.

| Component | Before | After |
|---|---|---|
| history `transparent-shard-server` (recent-01, recent-02, archive-03) | `2490c09d…` | `6ab86d6a…` |
| `shard-control` on the workers | `9208555a…` | `ff3c0264…` |
| `transparent-publish-controller`, `controller.json` `source_sha` | `13af048e…`, `a317455e` | `6041ea47…`, `9d2cbda0` |
| txid display workers `transparent-txid-server` | `4194b7af…` | `f4751961…` |
| txid display controller | `6bfec14e…` | `b522bb84…` |

Procedure as before ([steps](raw/coordinator/steps/), numbered logs in
`raw/coordinator/logs/`), plus the morning's controller swap
(`steps/p6-swap-inner.sh` under the production lock). Txid display used request v4
(`raw/txid/request-v4.json`, digest `f5d6996a…`, plan `d6a5e1b4…`).

| UTC | Step | Result |
|---|---|---|
| 03:22 | Health baseline | 3 serving, warm 18/18/164, controller at the tip (14 s), load running |
| 03:23–03:27 | Stage and `--verify-only` | recent 7 s each, archive-03 218 s |
| 03:27 | Load paused; recent-01, recent-02 rolled (other-serving floor 1); re-pinned; load restarted | 23 s and 24 s, warm 18/18, no recent outage |
| 03:28–03:30 | Load on mixed versions | 600 exact, 0 errors, p99 ≤ 32 ms |
| 03:30:27 | Controller swapped and restarted | both public origins 502 for one 2-second sample, 200 from 03:30:29; serving at the tip; 90 sealed entries identical |
| 03:30–03:40 | archive-03 from its disk cache, re-pinned, load restarted | install 567 s; archive shards 0–81 out of service 03:34:17–03:40:00 (**343 s**) |
| 03:40–03:43 | Load on the new fleet | 863 exact, 0 errors, freshness 9–20 s |
| 03:40–04:14 | txid display `stage`, `stop`, `workers`, `route`, `controller` | `/v1/txid/` 503 from about 03:50 to 04:08 (**about 18 min**); archive worker warm 425/425, recent 1/1; display tip equals history |

## Validation

- **Public wallet regression**, `transparent-regression` built at `9d2cbda0` (release-fast,
  `66ee2f02…`) on `roman-ipir-bench-8vcpu`, fixture `a6115f1a…` (schema v11), both
  public origins, 04:03:32–04:04:20: **11/11 cases passed**, 2,667 requests, 0 failed,
  0 retried ([report](raw/regression/report.json)). Every query was 44-bit dithered
  (directory 50,184 and 207,880 B; pages 72,712 and 388,104 B), and 12 tail-pages
  queries used the new prefix upload, 38,920 B (2,048 rows at 44 bits), with exact
  results. The SQLite stores (480 MB) stay on the bench host.
- **Txid lookup**, `txid_live_lookup` at `9d2cbda0`: passed, archive tier, 77,840 B up
  ([log](raw/txid/txid-live-post.log)).
- **49-bit load.** The 5 QPS load stayed exact through the mixed window and after.

## Operational notes

- The deploy tool's lock session again outlived it on this client
  ([`06a972db` notes](../fleet-redeploy-06a972db-2026-10-09/README.md#operational-notes)).
  [`lock-clean.sh`](raw/txid/lock-clean.sh) now finds the holder with `lslocks`: the
  earlier `pgrep -f` version matched its own command line and ended its own session
  once (no effect on production; the lock was already free).

## Rollback

History: `/opt/transparent-publisher/rollback/roll-9d2cbda0/<worker>/` on each worker,
`steps/p7-rollback.sh` for archive-03, `steps/p6-rollback.sh` for the controller (restores
`transparent-publish-controller.before` and `controller.json.before`); re-pin the load
to `2490c09d…`. Txid display: `txid-display-rollback` newest first with the ids in
`raw/txid/`; `06a972db`'s release is still staged.

## Not established

Not a soak or a 20 QPS gate. The served segments were certified at `06a972db`'s
snapshot; prefix uploads do not change what a segment holds, and the tip remains
covered by the shape screen only.
