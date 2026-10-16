# Wallet API v2 with the 8-bit history profile, 2026-10-12

The v12 fleet (schema `transparent-shard-v12`) now serves its history tables at
the 8-bit, one-digit-key profile, and the public origin
`https://transparent-pir.valargroup.dev` serves it under `/v2/`. The one-query
txid display is also under `/v2/txid/`. `/v1/` is unchanged production: schema
v11 history and the txid-2k display. **Production wallets still use `/v1/`.**
Only clients built from wallet-pir `ff27d649` or later speak `/v2/`
(zakura-core/wallet-libraries#165, merged; chainapsis/vizor-wallet#977, not merged).

## Provenance

| Item | Value |
|---|---|
| 8-bit serving | wallet-pir `f7debf84` on `main`. Deployed 22:07 UTC from source `921016dc`, whose non-test code equals `f7debf84` apart from comments. Script [deploy-p8.sh](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/api-v2-p8-2026-10-12/raw/coordinator/p8/deploy-p8.sh); hashes in [SHA256SUMS](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/api-v2-p8-2026-10-12/raw/coordinator/p8/SHA256SUMS). |
| `/v2` paths | wallet-pir `ff27d649` on `main`. Worker, `shard-control`, `shard-assign` and publish controller deployed to v12 at 23:18 UTC by [deploy-v2api.sh](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/api-v2-p8-2026-10-12/raw/coordinator/v2api/deploy-v2api.sh), built on the coordinator with `-C target-cpu=x86-64-v3 -C target-feature=+pclmulqdq` ([SHA256SUMS](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/api-v2-p8-2026-10-12/raw/coordinator/v2api/SHA256SUMS)). |
| Fleet configuration | v12 [fleet.json](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/api-v2-p8-2026-10-12/raw/coordinator/v2api/v12-fleet.json) sets `"api_version": "v2"`; [controller.json](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/api-v2-p8-2026-10-12/raw/coordinator/v2api/v12-controller.json) runs the fleet adapter from the same commit. |
| Public router | `transparent-pir-router-01` imports [api-v2-routes.caddy](https://github.com/valargroup/wallet-pir/blob/1b1d8c442b37f61917f7f2f74743bef19c690e0e/transparent/ops/deploy/api-v2-routes.caddy) through the v11 fleet's `route_imports` ([value](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/api-v2-p8-2026-10-12/raw/coordinator/v2api/v11-route-imports.json), applied by [prod-v2-route.sh](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/api-v2-p8-2026-10-12/raw/coordinator/v2api/prod-v2-route.sh), rendered 23:31 UTC, [router Caddyfile](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/api-v2-p8-2026-10-12/raw/router/Caddyfile)). `/v2/txid/*` goes to the one-query display site at 10.142.0.18:8095; every other `/v2/*` path goes to the v12 worker's internal listener. Terraform opened that listener to the router's tag (`transparent-v12.tf`). |
| Clients | `transparent-regression` built from `ff27d649` on roman-dev-2 (`release-fast`). |

## Results

**Serving.** Public `init` under `/v2/` reports schema v12 with every directory and
pages table at `reinspiring-two-mask-p8-g27x1-m22-dq28-r10-v1`: two 22-bit masks,
gadget 27×1 (one key), 28-bit queries only, 10-bit responses
([init](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/api-v2-p8-2026-10-12/raw/init/init-v2-public.json)). After the move, `/v1/shards/init`,
`/v1/shards`, `/v1/txid/init` and `/v1/txid/map` answer 200 exactly as before.
`/v2/shards/init`, `/v2/shards`, `/v2/filters/shards`, `/v2/txid/init` and
`/v2/txid/map` answer 200.

**Memory and warm-up (8-bit, measured).**
- Cold warm-up on new cache directories: 186 archive tables at 22:24 UTC (17 min); cgroup memory 36.9 GB for the archive worker and 4.9 GB for the recent worker.
- After the load run, process RSS was 35.3 GiB for the archive worker (23.7 GiB anonymous, 11.6 GiB mapped cache files) and 4.73 GiB for the recent worker (1.8 / 2.9 GiB). That totals 40.1 GiB against 31.4 GiB at 16 bits (28.5 and 2.87 GiB; [v12 staging](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/v12-staging-2026-10-12/README.md)): the archive worker grew 24% and the recent worker 65% ([rss-after-load](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/api-v2-p8-2026-10-12/raw/worker/rss-after-load.txt)). The planner's estimate was 37.5 GB and 4.4 GB.
- The runtime caches on disk are 35 GB (archive) and 5.3 GB (recent), against 30 and 3.3 GB at 16 bits. The 16-bit cache directories are still on the host as rollback material.
- After the `/v2` restart, both workers were ready from the disk cache in about 3.5 min.
- The recent worker's cache budget rose to 8 GiB and its memory limit to 9G/10G ([rss](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/api-v2-p8-2026-10-12/raw/worker/rss.txt)).

**Recovery regression.** 11 of 11 cases passed in each of two runs:
- on the v12 host before the move ([run](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/api-v2-p8-2026-10-12/raw/regression-p8-v12-host/run.log));
- through `https://transparent-pir.valargroup.dev` on `/v2/`, 6:09 wall time, 135 MB peak RSS ([junit](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/api-v2-p8-2026-10-12/raw/regression-v2-public/junit.xml), [report](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/api-v2-p8-2026-10-12/raw/regression-v2-public/report.json.gz)).

**Bytes per wallet against the same fixture at 16 bits.** The baseline is the
08:23 UTC run on the same v12 fleet before the switch
([16-bit](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/api-v2-p8-2026-10-12/raw/regression-16bit-baseline-http-summary.txt),
[8-bit](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/api-v2-p8-2026-10-12/raw/regression-v2-public/http-summary.txt), [script](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/api-v2-p8-2026-10-12/raw/httpsum.py)).

| Case | Upload 16 → 8-bit | Setup download 16 → 8-bit | PIR bytes in all |
|---|---|---|---|
| small-active | 311 → 168 KB | 81 → 122 KB | 437 → 331 KB (−24%) |
| coinbase | 8.34 → 4.91 MB | 0.52 → 0.79 MB | 9.63 → 6.40 MB (−34%) |
| multi-script-self-transfer | 16.0 → 9.27 MB | 1.85 → 2.82 MB | 19.2 → 13.3 MB (−31%) |
| reused-pages | 2.29 → 1.30 MB | 0.32 → 0.55 MB | 2.85 → 2.06 MB (−28%) |

- **Requests and responses:** a directory request at 2,048 rows drops from 38,920 B to 21,000 B, and a 32,768-row request from 207,880 B to 128,520 B. Responses drop from 5,648 B to 5,136 B.
- **Setup:** one table's setup grows from about 20.1 KB to 30,365 B. The two masks are 22,528 B and travel base64-encoded in JSON.
- **Total download:** about 36 MB for a full-history wallet, almost all public membership filters, which this change does not touch.
- **Latency:** median query latency is unchanged (0.15–0.75 s by case).

**Sustained load (20 wallets, 10 minutes, 01:32–01:43 UTC on 2026-10-13).**
`transparent-loadtest` from `ff27d649` on roman-dev-2, over HTTPS to the v12 host's
own `/v2/` listener (`https://transparent-pir-v12.valargroup.dev`, not the public
router). It used the same scenario and frozen sample as the 16-bit runs
([scenario](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/v12-staging-2026-10-12/raw/load/mixed-20-sustained-v12.json)).
The summary and the one non-exact wallet are in [summary-v2.json](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/api-v2-p8-2026-10-12/raw/load/summary-v2.json).
The full 16 MB report is kept gzipped on the development hub at
`/mnt/roman-dev-2-data/ai-dev/evidence/api-v2-p8-2026-10-12/`
([sha256s](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/api-v2-p8-2026-10-12/raw/load/archive-SHA256SUMS)).

The run synced 2,949 wallets: 2,948 exact and 1 timed out, with no failed requests
and every response 200. That is the same outcome as all four 16-bit runs
([v12 staging](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/v12-staging-2026-10-12/README.md)). The timed-out wallet is
sample 965 (reused-tail), the same wallet that missed the 600 s recovery deadline at
16 bits.

| Against 16-bit run 4 | 16-bit | 8-bit |
|---|---|---|
| Exact syncs/s | 5.00 | 4.88 |
| Directory query mean / upload | 0.240 s / 529 MB | 0.239 s / 290 MB |
| Pages query mean / upload | 0.201 s / 1,484 MB | 0.186 s / 904 MB |
| Directory setup mean / download | 0.382 s / 119 MB | 0.435 s / 176 MB |
| Pages setup mean / download | 0.217 s / 58 MB | 0.224 s / 87 MB |
| `init` / filters mean | 0.456 / 0.558 s | 0.458 / 0.560 s |

Recovery time per wallet, p50 / p99, was within a few percent of the 16-bit runs for
every profile, for example small-active 2.94 / 5.28 s, restore-6m 3.82 / 6.07 s and
restore-old 6.73 / 48.8 s. Multi-script p99 was 21 s against 113 s at 16 bits; about
100 wallets is too few to call that tail a change. Throughput is 2.4% below run 4 and
1.6% below the slowest 16-bit run.

Preparation is not measured. All 104 reused-tail wallets prepared one at a time
because that class's limit equals its user count, so the first attempt stalled. The
run that produced these numbers raised the limit to 24
(`--preparation-class-concurrency 24`) and reused the 736 finished preparations.

## Not done

- The served tables have not been certified under the served seeds at the 50-bit floor.
- Vizor #977 is not merged, so no shipped wallet uses `/v2/`.
- The v11 query latency gate (20 QPS) has not been run against v12.
