# Enhance and Status production deploy of `9d2cbda0`, 2026-10-10

Second deploy of the night, at Roman's request to deploy the latest `main`: every
Enhance and Status role moved from
[`06a972db`](../production-deploy-06a972db-2026-10-09/README.md) to the full-CI
release of `9d2cbda0` (run 38011976059) with `ops/scripts/wallet-pir-deploy.py`. New
since `06a972db`: the servers also accept a growing domain's selection over only its
[query rows](../../docs/protocol.md), and the admission code moved to
`shared/pir-control` (receiver merges). The in-repo monitor and load-test clients still
send full-length queries. APM and the external monitor were in shadow from 03:03 to
03:22 UTC and returned to `active` with no open incident.

| Service | Before | After (binary SHA-256) | Transactions |
|---|---|---|---|
| Enhance (`enhance-pir-native`) | `a70b5fba…` on all five | `0bc560dd…` | `enhance-20261010T030432Z-…-0d9e2e` (worker-01), `enhance-20261010T030908Z-…-386a35` (worker-02, router, ingress, coordinator) |
| Status (`status-pir`) | `cb259772…` on all three | `78975f99…` | `status-20261010T031744Z-…-da3751` |

Restart to verified readiness: workers 31 and 30 s (one at a time), packing router
30 s, query ingress 28 s, coordinator 14 s; Status worker 18 s, router 17 s,
controller 10 s.

| Check | Result |
|---|---|
| Enhance exact answers after worker-01, 2026-10-06 oracle (`284ceff7…`) | 120/120, p99 133 ms ([json](raw/exact-worker-01.json)) |
| Enhance exact answers after the final transaction | 120/120, 0 errors, p50 89 ms, p99 129 ms ([json](raw/exact-final.json)) |
| Status live mined-answer probe, independent oracle, 20 QPS for 60 s | 1,200/1,200, p99 61 ms ([summary](raw/status-probe-summary.jsonl)) |

One orphaned lock session was left on the coordinator after the final Enhance
transaction (the pattern recorded for `06a972db`) and was ended at 03:16.

Rollback: `rollback enhance --transaction enhance-20261010T030908Z-0bc560dda0f8-386a35`
then `…-0d9e2e`; `rollback status --transaction status-20261010T031744Z-78975f99759b-da3751`.

Not established: no prefix-length Enhance query was sent to production (the in-repo
clients send full length), and no load beyond the exact checks.
