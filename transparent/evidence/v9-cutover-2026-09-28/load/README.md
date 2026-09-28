# Pinned-anchor load run against schema v9, 2026-09-28

This repeats run 2 of the [v7 cluster qualification baseline](../../cluster-qualification-2026-09-28/README.md)
against the v9 publication, with the same host, sample clients and flags.

| Field | Value |
|---|---|
| Target | `https://transparent-pir.valargroup.dev`, the live production cluster, after the [v9 cutover](../README.md) |
| Client host | `roman-ipir-bench-8vcpu` (8 vCPU Xeon Platinum 8358, 31 GiB), over the public internet |
| Client | `transparent-loadtest` built from `70cb5924` (`86d97566…`), x86-64-v3 |
| Command | `--steps 4 --step-duration 300s --min-completed-per-class 3 --max-queries 6000 --timeout 600s --http-attempts 3` |
| Anchor | Every wallet synced to the sample's anchor 3,473,686; served tip 3,499,410 |

## Stale oracle in the first run

[`v9-pinned-live-1.json`](v9-pinned-live-1.json) completed 45 of 47 syncs with
none failed, but only the 6 `unused` syncs matched. The expected digests hash
each event's encoded bytes, and the 2026-09-08 sample was written with the
96-byte v1 codec, so every non-empty history digested differently under the
87-byte codec. The public regression, which compares decoded events, had
passed 68/68 before this run.

`sample-redigest` (`4d4b8981`) re-derived the sample from the version-2
journal: same 1,080 clients, scripts and ranges; 960 digests changed; every
client's event count equals the recorded count ([log](sample-redigest.log)).
The result, [`sample-v2codec.json`](sample-v2codec.json) (`d9ce4f84…`), is now
the load workflow's and scenarios' default sample. The original sample is
unchanged.

## Result

[`v9-pinned-live-2.json`](v9-pinned-live-2.json): **47 syncs, 45 completed, 45
exact, 0 failed**. The v7 baseline run 2 had 47 / 45 / 45 / 0. The 2
incompletes are `reused-tail` syncs at the 600 s timeout, as in the baseline.

| Class | v7 exact | v9 exact | p50 s v7 → v9 | p95 s v7 → v9 | MB/sync v7 → v9 | queries/sync v7 → v9 |
|---|---:|---:|---:|---:|---:|---:|
| catch-up-1d | 5/5 | 5/5 | 0.4 → 0.1 | 1.4 → 0.2 | 1.2 → 0.3 (-75%) | 7 → 3 |
| catch-up-30d | 5/5 | 5/5 | 0.4 → 0.2 | 2.6 → 0.5 | 1.5 → 0.6 (-61%) | 8 → 4 |
| catch-up-7d | 5/5 | 5/5 | 0.4 → 0.2 | 1.2 → 0.4 | 1.3 → 0.5 (-56%) | 7 → 4 |
| multi-script | 5/5 | 5/5 | 7.5 → 2.7 | 12.9 → 3.6 | 18.5 → 5.5 (-70%) | 125 → 66 |
| restore-6m | 5/5 | 5/5 | 0.8 → 0.3 | 0.9 → 1.1 | 3.0 → 1.2 (-59%) | 11 → 5 |
| restore-old | 5/5 | 5/5 | 4.2 → 2.6 | 23.9 → 17.2 | 88.4 → 45.6 (-48%) | 82 → 42 |
| reused-tail | 4/4 | 4/4 | 29.6 → 22.0 | 653.8 → 785.9 | 1653.8 → 1529.9 (-7%) | 3865 → 3643 |
| small-active | 5/5 | 5/5 | 0.2 → 0.1 | 0.4 → 0.2 | 0.9 → 0.4 (-53%) | 2 → 1 |
| unused | 6/6 | 6/6 | 1.8 → 1.0 | 2.2 → 2.4 | 63.3 → 31.6 (-50%) | 0 → 0 |

Bytes are wallet-level payloads per completed sync, summed over every stage;
queries count directory and page requests.

## Limits

- One run per schema and 4–6 completed syncs per class. This shows exactness and
  a per-sync direction, not capacity: 4 concurrent clients, not a sustained rate.
- The byte and query reductions combine three changes: v9 records, choice tables
  on every shard (v7 had them only on revisions after 2026-09-27 22:43) and the
  v2 range-filter profile. This run does not separate them.
- The two runs were about 13 hours apart against a changing tip and fleet load.
  Latencies include public-internet transport from one host.
