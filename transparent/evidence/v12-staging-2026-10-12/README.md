# v12 staging next to v11, 2026-10-12

A second transparent PIR system, schema `transparent-shard-v12` with decoupled
directory and page shards, deployed beside production v11 and served at
`https://transparent-pir-v12.valargroup.dev`. It follows production's live
journal read-only. It passed the recovery regression and the sustained load
run below, and v11 was not changed. **This is staging. Production clients
still use v11; the switch and v11's retirement are not done.**

## Provenance

| Item | Value |
|---|---|
| Source | `v12/decoupled-pages` at `fb90519b` for every deployed binary ([SHA256SUMS](raw/coordinator/transparent-publisher/v12/releases/fb90519b/SHA256SUMS)), `release-fast`, rustc 1.97.1, built on the coordinator in `/srv/wallet-pir-build-fb90519b`. The fleet adapter runs from `29ba1936` (per-worker control socket and the concurrent hard-link fix, both on this branch). |
| Journal | the live v3 journal on the coordinator, `/srv/transparent-activity/full-v3/journal`, written by v11's publish controller. The v12 controller opens it read-only (`follow_journal`), takes no writer lock and runs at Nice 10 with idle I/O. The initial publication was built from an XFS reflink snapshot of it. |
| Initial publication | through 3,514,891, anchor `0000000000156f83…02893ec8b2`, 354,145,221 events; 38 min 58 s, 2.3 GB peak RSS ([s1-record.json](raw/coordinator/transparent-publisher/v12/deploy-20261012/s1-record.json), [s1.time](raw/coordinator/transparent-publisher/v12/deploy-20261012/s1.time)) |
| Layout | archive `decoupled-32k-32k`, 12 directory shards; recent `decoupled-2k-8k` from 3,076,108, 22 directory shards; 201 page shards in all; range profile `zcash-transparent-range-v2`; choice tables on every directory shard |
| Coordinator | `enhance-pir-coordinator-01`: unit [transparent-v12-publish-controller.service](raw/coordinator/systemd/system/transparent-v12-publish-controller.service), [controller.json](raw/coordinator/transparent-publisher/v12/controller.json), [fleet.json](raw/coordinator/transparent-publisher/v12/fleet.json), [roster.json](raw/coordinator/transparent-publisher/v12/roster.json); dated step scripts in [deploy-20261012/](raw/coordinator/transparent-publisher/v12/deploy-20261012/) |
| Worker | `transparent-pir-v12-01`, DigitalOcean `m-8vcpu-64gb` in ams3 (8 vCPU Xeon Platinum 8358, 64 GB, Ubuntu 24.04), private 10.142.0.9. Archive worker on :8093 owns directory shards 0–11 and their pages; recent worker on :8193 serves the rest ([units](raw/coordinator/v12-units/)). Defined in `ops/infra/digitalocean/production/transparent-v12.tf` on `main` (`f7a2202a`, `72cefe14`). |
| Site | Caddy 2.6.2 on the worker serves the hostname, with the site file written by the v12 controller ([Caddyfile](raw/worker/Caddyfile)). `/v1/txid/*` is forwarded to production ([txid.caddy](raw/worker/etc/caddy/v12-routes/txid.caddy)); v12 adds no txid service. |

## Results

**Serving.** The controller activated its first publication at 3,515,009 and
has kept up since: at 08:40 UTC both v11 and v12 reported `covered_through`
3,515,025. `init`, `shards` and `txid/map` return 200 over HTTPS.

**Memory (measured).** After the cold warm-up the archive worker holds 186
prepared tables in 28.5 GiB RSS; the cold warm-up took 722 s. The recent worker
holds 49 tables in 2.9 GiB ([rss.txt](raw/worker/rss.txt),
[free.txt](raw/worker/free.txt)). The census estimated about 28 GiB in all; the
measured total is 31.4 GiB, which leaves about 30 GiB of the host free.

**Recovery regression (passed).** `transparent-regression` from roman-dev-2
over HTTPS against the staging hostname: 11 of 11 cases passed. The fixture
`mainnet-v12.json` (sha256 `d5d5946f…`) was exported from the live journal by
[s3-regression-export.sh](raw/coordinator/transparent-publisher/v12/deploy-20261012/s3-regression-export.sh)
with cases in [mainnet-cases-v12.json](raw/coordinator/transparent-publisher/v12/deploy-20261012/mainnet-cases-v12.json).
The served map was `dbdcc27e…`, with 33 sealed shards in the prefix
([report.json.gz](raw/regression/report.json.gz), [junit.xml](raw/regression/junit.xml)).

**Sustained load (20 wallets, three runs).** `transparent-loadtest` from
roman-dev-2 over HTTPS, scenario [mixed-20-sustained-v12.json](raw/load/mixed-20-sustained-v12.json):
20 concurrent wallets from the frozen 2026-09-08 sample (1,080 wallets, digests
re-derived from the live journal by `sample-redigest`: 960 digests changed, every
event count equal), 10 minutes each. Each run's summary and every non-exact
wallet are in `raw/load/summary-run*.json`; the full reports (14–24 MB) are kept
gzipped on the development hub at
`/mnt/roman-dev-2-data/ai-dev/evidence/v12-staging-2026-10-12/`
([sha256s](raw/load/archive-SHA256SUMS)).

| Run (UTC) | Wallet syncs | Exact | `init` 503 | Timed out | PIR queries (failed) | Exact syncs/s |
|---|---|---|---|---|---|---|
| 1, 13:20–13:30, before the firewall fix | 3,009 | 2,996 | 12 | 1 | 22,304 (0) | 4.96 |
| 2, 13:36–13:47, fix applied at 13:38 | 3,038 | 3,030 | 7 | 1 | 22,518 (0) | 5.02 |
| 3, 15:50–16:01 | 3,015 | 3,007 | 7 | 1 | 22,279 (0) | 4.98 |

Run 3 mean request times: directory query 0.239 s, page query 0.203 s, `init`
0.456 s, filters 0.558 s. Recovery time per wallet, p50 / p99: small-active 2.83 /
5.09 s, catch-up-1d 3.01 / 3.91 s, catch-up-7d 2.57 / 3.51 s, catch-up-30d 2.95 /
5.27 s, restore-6m 3.75 / 6.23 s, unused 4.27 / 6.31 s, restore-old 6.61 / 50.1 s,
multi-script 11.0 / 110.5 s. The one reused-tail wallet in each run exceeded the
600 s recovery deadline.

This tool reports per-stage means, not per-request percentiles, so these runs
do not evaluate the v11 query gate (p99 under 2 s, p50 under 700 ms at 20 QPS,
measured by `rate-query`). That gate has not been run against v12.

**Firewall fix.** In run 1 most `init` 503s came from a firewall gap. The v12
controller answers `init` by reading every worker's own `init`, and the worker
firewall opened only 8093 (the archive worker's port), so each read of the
recent worker on 8193 waited out its 5 s timeout; whenever the archive worker's
answer alone did not cover every geometry, the request returned 503. A probe
from the coordinator ([init-probe.txt.gz](raw/probe/init-probe.txt.gz)) saw 8193 time
out until 13:38:01 UTC. `ops/infra/digitalocean/production/transparent-v12.tf`
on `main` (`857b1710`) opens 8193 to the coordinator and load generators; the
saved plan changed only the v12 firewall (0 added, 1 changed, 0 destroyed) and
was applied with the coordinator wrapper under the production lock.

**Open: `init` 503 at publication switches.** Every 503 in runs 2 and 3 started
within about one second before the controller activated the next block's
publication (for example 15:53:41 against 15:53:43, 15:59:12 against
15:59:13). A request that spans an activation is answered 503 and asked to
retry; the load tool does not retry `init`, so each counts as a failed sync.
Whether the wallet retries it, and how often v11 sees the same, is not yet
checked.

**v11 not regressed.** Checked against [v11-baseline.txt](raw/coordinator/transparent-publisher/v12/deploy-20261012/v11-baseline.txt),
taken before any v12 step, and again after the load run ([v11-after.txt](raw/v11-after.txt)):

- the same PIDs and start times for v11's publish controller, filter server and
  replica reconciler;
- the same sha256 for v11's controller unit, `controller.json`, `fleet.json`
  and `roster.json`;
- v11 `init` returns 200, and after run 3 v11 and v12 both covered 3,515,375;
- at 08:44 UTC the router had served 2 responses with status 503 since
  2026-09-12, the same count as before the v12 work;
- `/srv/transparent-activity` is 51% used.

The baseline also recorded the hash of v11's active publication record, which
changes with every block, so it is not compared.

## Deviations from the plan

- **The staging site is on the v12 host, not the router.** The v11 controller
  rewrites the router's whole Caddyfile on every publication, so a second site
  there would be removed. DNS for the staging hostname points at the v12 host,
  and the firewall opens 80 and 443 there. The switch must route production
  traffic differently.
- **v11 writes the v3 journal, not the v2 journal.** The v2 journal stopped at
  3,505,130; the census used it, and the deployment follows the v3 journal.
- **Range profile.** The first initial publication used `shard-publish`'s
  default range profile, v1. The controller uses v2 and rejected it ("previous
  publication identity mismatch"). It was rebuilt under v2, which changed
  every shard digest, and then restaged.
- **Archive shard 11 is a tail cut at the boundary.** It ends at 3,076,107,
  where recent starts, instead of sealing full.
- **Disk reservation exceeds the census estimate.** The assignment reserves
  37.5 GB for the archive worker and 4.4 GB for the recent worker.
- **Fleet adapter settings.** `manage_all_workers`, `control_sessions` and
  `status_socket_forwarding` are off, because two workers share one host with
  per-worker control sockets.

## Limits

- One staging host, no high availability, no elastic recent tier.
- The regression report and the worker vmstat are stored gzipped.
- The load run uses the synthetic 2026-09-08 sample with digests re-derived
  from the live journal, not population weights.
- No Vizor device build or restore has been run against staging; the draft PR
  only compiles.
- The canary and APM are not pointed at staging.
