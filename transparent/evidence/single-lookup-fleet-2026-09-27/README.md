# Single-lookup directory fleet benchmark, 2026-09-27

Fleet-scale comparison of directory choice tables, run on a **temporary bench
fleet**, not the production fleet. Production was not changed: publishing
tables there would break wallets built before the `directory_choice` field (see
[architecture](../../docs/architecture.md)).

The bench fleet mirrored the production recent tier and the r3 load client:

- 4× `s-4vcpu-8gb` workers;
- one `s-2vcpu-4gb` Caddy router, with the production health-check settings;
- one `c-16` load generator (Xeon 8280), all in ams3 on one VPC over plain HTTP.

It served the same recent journal slice without (`off`) and with (`all`)
directory choice tables, alternating. It was destroyed after the run.

| Field | Value |
|---|---|
| Source | `72c9031d`, built on `roman-ipir-bench-8vcpu` with the release flags `-C target-cpu=x86-64-v3 -C target-feature=+pclmulqdq`. Binary sha256: server `2b748b0c…638e`, loadtest `23aec6a6…8428`. |
| Shard sets | The two publications from the [local measurement](../single-lookup-measure-2026-09-27/README.md) (`off` map `0702ebea…94b3`, `all` map `6458d612…e9bd`): 14 recent-8k shards, heights 3,262,749–3,473,686, byte-identical directory tables. |
| Workers | `serve.sh`: every worker holds the whole set, `--cache-bytes 5 GiB --build-slots 1 --query-slots 2 --pilot-cold`, `LimitNOFILE=1048576` (the rerun only), `MemoryMax=7G`. Warmed by an unrecorded 5-minute pass before each run. `ready-*.txt` records warm runtimes. |
| Router | `Caddyfile`: round robin over the 4 workers, `health_uri /v1/ready`, 5 s interval, 3 s timeout, 30 s fail duration (the production router's values). Journal: `router-caddy.log.gz`. |
| Load | `run.sh`, driven from the load generator: the derived recent sample (716 clients, sha256 `be4eb6c2…98e6`), classes restore-6m, multi-script and catch-up-1d/7d/30d. `--min-completed-per-class 100 --max-queries 6000 --step-duration 10m`, seed 1. Order off, all, all, off. |
| Passes | First pass: steps 8, 32 and 128. Its 128 step failed on the client (open-file limit 1,024: "Too many open files", then "builder error") and is excluded. Rerun: step 128 only, with `ulimit -n 1048576`, files `*-c128.*`. |

## Results

**Steps 8 and 32** (valid, 4 runs each):

- Every sync was exact: 4,578 syncs, 0 failed, 0 mismatched, no 503s.
- Byte counts were identical between repetitions.
- Directory queries per sync were exactly halved in every class.

| Class | Wallets | off bytes/sync | all bytes/sync | Change | p50 / p95 s, off → all (rep 1) |
|---|---:|---:|---:|---:|---|
| restore-6m | 8 | 3,141,097 | 2,393,427 | −23.8% | 0.65/1.26 → 0.43/0.83 |
| restore-6m | 32 | 3,159,248 | 2,404,616 | −23.9% | 1.36/2.76 → 0.83/1.71 |
| multi-script | 8 | 20,063,972 | 13,104,420 | −34.7% | 5.78/8.65 → 3.52/5.92 |
| multi-script | 32 | 19,875,471 | 12,882,654 | −35.2% | 11.87/18.32 → 6.59/11.30 |
| catch-up-1d | 32 | 1,055,624 | 699,644 | −33.7% | 0.68/1.06 → 0.40/0.81 |
| catch-up-7d | 32 | 813,945 | 539,233 | −33.8% | |
| catch-up-30d | 32 | 1,130,569 | 830,006 | −26.6% | |

The off-variant restore-6m baseline, 3.14–3.16 MB, agrees with
[fleet series r3](../runs/fleet-series-2026-09-08-r3/README.md) (3.15 MB).

**Throughput** (completed syncs/s): 4.67 and 4.55 (off) against 5.56 and 5.82 (all)
at 8 wallets; 5.26 and 5.34 against 6.38 and 6.10 at 32 wallets.

**Step 128 (rerun)** was unstable in both variants:

| Run | Completed | Failed | Rate/s | restore-6m p50/p95 s | multi-script p50/p95 s |
|---|---:|---:|---:|---|---|
| off-1 | 1,313 | 28,287 | 2.15 | failure spiral | |
| all-1 | 999 | 0 | 7.74 | 3.00/6.39 | 24.9/39.9 |
| all-2 | 1,645 | 10,844 | 4.61 | failure spiral | |
| off-2 | 997 | 0 | 5.91 | 5.19/10.57 | 42.1/68.9 |

Both failed runs are router 503s ("no upstreams available", 39,131 log lines).
Under saturation, the router marked all four workers down at once. Failed syncs
retried immediately, which kept the router refusing. This occurred once in each
variant, so it does not distinguish them.

**Correction (2026-09-28).** The first reading blamed the active health checks'
3 s timeout. The router journal does not support that: during both spirals every
worker passed every active check (8 "host is up" lines per 10 s, no failed check
after startup). The ejections were **passive**. `fail_duration 30s` with Caddy's
default `max_fails 1` takes a worker out for 30 s on any single proxy error. The
journal shows such errors from saturated workers just before each spiral, e.g.
"readfrom … write: broken pipe": a worker refused a query with 503 before
reading its 128 KB body and closed the connection under the upload. The router's
own 503 carried no `Retry-After`, so wallets treated it as terminal and the load
client resubmitted at once. The health settings here are the ones `shard-assign`
rendered; the live publisher renders the production router separately, with
different settings. The router policy, worker upload drain and client backoff that
address each step were rolled out on 2026-09-28; see [status](../../docs/status.md).

In the two clean 128 runs, bytes per sync compare as follows:

| Class | off | all | Change |
|---|---:|---:|---:|
| restore-6m | 3,167,785 | 2,412,602 | −23.8% |
| multi-script | 20,330,895 | 13,320,027 | −34.5% |

In those same two runs, completed syncs/s were 5.91 (off) and 7.74 (all).

## Limits

- It is a bench copy of the recent tier only: 14 shards with ids from 0, and no
  archive owners or public TLS edge. It measures no production traffic, WAN
  clients or mobile devices.
- Each step ends once every class has 100 completions, so steps last 1.5–10
  minutes, not the full 10.
- The 128-wallet comparison rests on one clean run per variant.
- Wallets run this branch's `transparent-wallet`; wallet-libraries is not
  updated.

## Outcome

At 8 and 32 concurrent wallets, the fleet confirms the local and projected
results:
- about 24% fewer bytes for a six-month restore and 26–35% for the other
  classes;
- directory queries halved;
- p50 and p95 sync times about 35–45% lower;
- 15–25% more completed syncs per second.

Enabling tables in production still requires, first:
- every worker and wallet (including wallet-libraries) upgraded to understand
  the field;
- a merge to `main` and the publisher config change.
