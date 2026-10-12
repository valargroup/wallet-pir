# Wallet API v2 with the 8-bit history profile, 2026-10-12

The v12 fleet (schema `transparent-shard-v12`) now serves its history tables at
the 8-bit, one-digit-key profile, and the public origin
`https://transparent-pir.valargroup.dev` serves it under `/v2/`. The one-query
txid display is also under `/v2/txid/`. `/v1/` is unchanged production: schema
v11 history and the txid-2k display. **Production wallets still use `/v1/`.**
Only clients built from wallet-pir `ff27d649` or later speak `/v2/`
(zakura-core/wallet-libraries#165, chainapsis/vizor-wallet#977, neither merged).

## Provenance

| Item | Value |
|---|---|
| 8-bit serving | wallet-pir `f7debf84` on `main`. Deployed 22:07 UTC from source `921016dc`, whose non-test code equals `f7debf84` apart from comments. Script [deploy-p8.sh](raw/coordinator/p8/deploy-p8.sh); hashes in [SHA256SUMS](raw/coordinator/p8/SHA256SUMS). |
| `/v2` paths | wallet-pir `ff27d649` on `main`. Worker, `shard-control`, `shard-assign` and publish controller deployed to v12 at 23:18 UTC by [deploy-v2api.sh](raw/coordinator/v2api/deploy-v2api.sh), built on the coordinator with `-C target-cpu=x86-64-v3 -C target-feature=+pclmulqdq` ([SHA256SUMS](raw/coordinator/v2api/SHA256SUMS)). |
| Fleet configuration | v12 [fleet.json](raw/coordinator/v2api/v12-fleet.json) sets `"api_version": "v2"`; [controller.json](raw/coordinator/v2api/v12-controller.json) runs the fleet adapter from the same commit. |
| Public router | `transparent-pir-router-01` imports [api-v2-routes.caddy](../../ops/deploy/api-v2-routes.caddy) through the v11 fleet's `route_imports` ([value](raw/coordinator/v2api/v11-route-imports.json), applied by [prod-v2-route.sh](raw/coordinator/v2api/prod-v2-route.sh), rendered 23:31 UTC, [router Caddyfile](raw/router/Caddyfile)). `/v2/txid/*` goes to the one-query display site at 10.142.0.18:8095; every other `/v2/*` path goes to the v12 worker's internal listener. Terraform opened that listener to the router's tag (`transparent-v12.tf`). |
| Clients | `transparent-regression` built from `ff27d649` on roman-dev-2 (`release-fast`). |

## Results

**Serving.** Public `init` under `/v2/` reports schema v12 with every directory and
pages table at `reinspiring-two-mask-p8-g27x1-m22-dq28-r10-v1`: two 22-bit masks,
gadget 27×1 (one key), 28-bit queries only, 10-bit responses
([init](raw/init/init-v2-public.json)). After the move, `/v1/shards/init`,
`/v1/shards`, `/v1/txid/init` and `/v1/txid/map` answer 200 exactly as before.
`/v2/shards/init`, `/v2/shards`, `/v2/filters/shards`, `/v2/txid/init` and
`/v2/txid/map` answer 200.

**Memory and warm-up (8-bit, measured).**
- Cold warm-up on new cache directories: 186 archive tables at 22:24 UTC (17 min), RSS 36.9 GB; 49 recent tables, 4.9 GB.
- After the `/v2` restart, both workers were ready from the disk cache in about 3.5 min.
- The recent worker's cache budget rose to 8 GiB and its memory limit to 9G/10G ([rss](raw/worker/rss.txt)).

**Recovery regression.** 11 of 11 cases passed in each of two runs:
- on the v12 host before the move ([run](raw/regression-p8-v12-host/run.log));
- through `https://transparent-pir.valargroup.dev` on `/v2/`, 6:09 wall time, 135 MB peak RSS ([junit](raw/regression-v2-public/junit.xml), [report](raw/regression-v2-public/report.json.gz)).

**Bytes per wallet against the same fixture at 16 bits.** The baseline is the
08:23 UTC run on the same v12 fleet before the switch
([16-bit](raw/regression-16bit-baseline-http-summary.txt),
[8-bit](raw/regression-v2-public/http-summary.txt), [script](raw/httpsum.py)).

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

## Not done

- The 20-wallet sustained load run on `/v2` was started at 23:43 UTC against the v12 host. Its result is not recorded here.
- The served tables have not been certified under the served seeds at the 50-bit floor.
- Neither client PR is merged, so no shipped wallet uses `/v2/`.
- The v11 query latency gate (20 QPS) has not been run against v12.
