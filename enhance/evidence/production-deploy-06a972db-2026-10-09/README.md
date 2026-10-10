# Enhance and Status production deploy of `06a972db`, 2026-10-09

All five Enhance roles and all three Status roles were moved to the full-CI release
of `main` at `06a972db` (run 37974719227) with `ops/scripts/wallet-pir-deploy.py`.
The release adds acceptance of 44-bit dithered selections next to the 49-bit ones
([protocol](../../docs/protocol.md)). Nothing advertises the new width and the in-repo
clients still send 49 bits, so served documents and client behaviour are unchanged.
APM and the external monitor were in shadow alert mode from 21:52 to 23:49 UTC and
returned to `active` with no open incident.

| Service | Before | After (binary SHA-256) | Transactions |
|---|---|---|---|
| Enhance (`enhance-pir-native`) | workers, router, ingress `397d8ba9…`; coordinator `cfeaa3ba…` | `a70b5fba…` on all five | `enhance-20261009T215358Z-…-f2c7a0` (worker-01), `enhance-20261009T233025Z-…-30a6d0` (worker-02, router, ingress, coordinator) |
| Status (`status-pir`) | `29b34ed3…` on all three | `cb259772…` on all three | `status-20261009T234307Z-…-23e9f7` |

Restart to verified readiness, from the journals in `raw/`: workers 25 s each (one at
a time, the other replica serving); packing router 30 s; query ingress 28 s;
coordinator 13 s; Status worker 18 s, router 16 s, controller 10 s. The router,
ingress, coordinator and all Status roles are single instances, so each restart
interrupted its path for about that long. Public availability was not probed
separately.

## Acceptance

| Check | Result |
|---|---|
| Enhance exact answers after the final transaction, 28-record oracle of 2026-10-06 (`public-oracle.new.json`, `284ceff7…`) through `https://enhance-pir.valargroup.dev` | 120/120 correct, 0 errors, p50 89 ms, p99 153 ms ([json](raw/exact-final.json)) |
| Same check after worker-02's first roll | 120/120 correct, p99 121 ms ([json](raw/exact-worker-02-interrupted.json)) |
| Status live mined-answer probe, independent oracle, 20 QPS for 60 s through the query tunnel | 1,200/1,200 correct, 0 failed, p99 64 ms ([summary](raw/status-probe-summary.jsonl)) |
| Running executables | every role's `/proc/<pid>/exe` and health `binary_sha256` equal the release ([deploy output](raw/enhance-deploy-rest.txt), [Status](raw/status-deploy.txt)) |
| Unit drift | none; Status worker and router units differ from the live ones only in the release path |

## Interrupted worker-02 transaction

The first worker-02 transaction (`…-8ff547`) restarted and verified the worker at
21:58:57 and its exact check finished at 22:00 (120/120), but the workstation's SSH
connection for that check stalled and never returned: `run_exact_check` has no
client-side timeout. The deploy process was killed with SIGKILL at 23:24 so that no
rollback would run, and its orphaned lock session on the coordinator was ended. The
tool then refused new deploys while that transaction was `verifying`, so it was
finished with `rollback` (worker-02 back to `397d8ba9…` at 23:28) and worker-02 was
rolled forward again in the final transaction. A second orphaned lock session was
left behind after the final Enhance commit and was ended the same way at 23:42.

`--skip-exact-check` only applies when the inventory has no `exact_check`; with one
configured, the check runs on every transaction, including single-worker ones.

## Rollback

- Enhance: `wallet-pir-deploy.py --state-dir <state> rollback enhance --transaction
  enhance-20261009T233025Z-a70b5fbaabe5-30a6d0`, then the same for `…-f2c7a0`.
  Each host keeps the previous drop-in under `/opt/enhance-pir/transactions/<id>/`.
- Status: `rollback status --transaction status-20261009T234307Z-cb259772123b-23e9f7`.

## Not established

No load test beyond the exact-answer checks and the 60-second Status probe; no
44-bit query was sent to either production service.
