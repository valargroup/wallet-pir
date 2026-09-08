# Transparent PIR status

Source inspection: 2026-09-08. Accepted-anchor recovery is in the current working tree; earlier implementation commits are recorded in [remaining work](remaining-work.md). Live state is observed separately below.

## Source-verified implementation

| Capability | Observed state |
|---|---|
| Shard schema | `transparent-shard-v7` in `pir/transparent-shard/src/manifest.rs` |
| Registry | `recent-8k` 8192/8192; `recent-4k` 4096/4096; `archive-32k` 32768/32768; `archive-wide` 32768/65536 |
| Desired optional recent pairing | 4096/8192 absent; add a new name, never reinterpret `recent-4k` |
| Census ranges | `--start-height`, `--end-height`, geometry overrides, `--placement`, `--per-shard`, exact script matches exist in `shard-census.rs` |
| Two-tier publisher | `--recent-geometry`, `--archive-geometry`, `--recent-from` exist in `shard-publish.rs` |
| Workflow exposure | `publish-transparent-shards.yml` exposes commit, journal, output directory, anchor, `recent_from` (re-derived and checked) and both geometries; the backfill workflow's `inventory` action records journal identity, cutoff and an independent event spot-check |
| Loading/cache | `ShardSet::open_with` loads the whole set or an assignment's subset: every shard's manifest and filter, tables only for assigned ids, global ids and manifest chain intact; bounded runtime cache and file-backed plaintext sources |
| Revision handling | Revision-addressed setup/query, 409 refresh, retryable cache pressure, bounded wallet refresh exist |
| Retention | Three superseded revisions per shard beyond current, and optionally a byte bound (`--retain-bytes`), newest first; excess is refused or, with `--prune-excess`, reported and removed by `shard-prune` after activation |
| Observability | Every metric series labelled with map and assignment digests, worker id and role; cold-build histogram; process RSS and cgroup memory gauges; queue depth and body bytes in flight; unassigned refusals; prewarm progress |
| Query validation | The query route refuses a body whose declared length is not the selected table's exact length before buffering, queueing or building; a missing length is 411; the runtime re-checks length and binding as defence in depth; the global HTTP cap remains the ceiling for the widest geometry |
| Admission | `admission.rs` bounds waiting requests (`--query-waiters`), buffered body bytes (`--body-bytes`), upload time (`--upload-deadline-secs`) and total wait (`--query-deadline-secs`); a full queue, exhausted body budget or expired deadline is 503 with `retry-after`, a stalled upload 408; a dropped connection releases its place and is counted |
| Deployment acceleration (working tree) | Persistent public runtime snapshots, stable per-worker assignment identity, binary identity in readiness, selective staging/restarts, healthy replica pairs and transaction-scoped rollback implemented locally. Host rollout and timing acceptance remain in [remaining work](remaining-work.md). |
| Readiness | `/v1/ready` reports mode, map and assignment digests, warm and target runtime counts; loaded-only mode (`--pilot-cold`, whole-set default) is ready once loaded, warm mode (assignment default) only once every assigned runtime is prewarmed |
| Fleet assignment/router | `transparent-assignment-v1` planned by `shard-assign` from a roster; router is Caddy rendered from the assignment, routing on the shard id in the path only; fleet deploy modes prepare, verify with the staged binary, activate owners then replicas, switch the router, verify publicly and over the VPC, prune, and roll back on failure; filter service has its own deploy workflow. Not yet exercised on real hosts |
| Wallet manifest binding | `GET /v1/shards/:id/revisions/:digest/manifest` serves canonical manifest bytes; before any private request on a matched shard the wallet recomputes the digest, checks every field against the map entry, the previous entry's digest and the registry, and takes the geometry from the verified manifest |
| Accepted-anchor recovery | `sync_into` and the facade require a wallet-accepted height/hash. Partial-shard coverage retains a distinct source endpoint, events above the target are discarded after validation, and rollback accepts an exact ancestor. SQLite schema 2 migrates legacy progress conservatively. See [tests](testing.md); deployed regression remains a separate gate. |
| Persisted continuation | `sync_into` over a `WalletStore` (reference `MemoryStore`; SQLite in `pir/transparent-wallet-store`): per-script per-shard coverage with the block hash it rests on, settled or provisional; atomic idempotent shard commits; reorg rollback to the accepted ancestor via the wallet's `ChainView`; provisional tail truncation and promotion; durable pending page work; scripts added by the wallet's rules discovered over their required range; a budget or outage ends a call incomplete with no anchor. `sync()` is a wrapper over a fresh memory store. `TransparentSync` facade takes plain data for a host binding. The store contract suite is exported under the `testing` feature and `parse_init` is available without `reqwest` (`5758ffd`); the Zakura wallet (`valargroup/wallet-libraries`) implements the store over its own database and passes it |

Relevant sources: [store](../../pir/transparent-wallet/src/store.rs), [publisher](../../server/transparent-filter-server/src/bin/shard-publish.rs), [census](../../server/transparent-filter-server/src/bin/shard-census.rs), [service](../../server/transparent-shard-server/src/service.rs), [runtime](../../server/transparent-shard-server/src/runtime.rs), [loader](../../server/transparent-shard-server/src/shardset.rs), [wallet](../../pir/transparent-wallet/src/sync.rs).

## Live deployment: observed 2026-09-08

Recorded by [inventory-transparent-dataset.yml run 34181566124](https://github.com/valargroup/enhance-pir/actions/runs/34181566124); raw files and findings in [evidence/inventory-2026-09-08](evidence/inventory-2026-09-08/README.md). This is the pilot, not the accepted fleet.

| Field | Observed |
|---|---|
| Journal | `/srv/zakura/transparent-event-data`, mainnet, start 0, committed through 3,473,686, 352,873,356 events, hashes agree with the node |
| Anchor / cutoff | 3,473,686 (`0000000000755137…be1d`, 2026-09-06T07:39:01Z); cutoff 3,262,749 (`0000000000b300ab…e8d9`); archive `[0, 3262748]`, recent `[3262749, 3473686]` |
| Spot-check | 11 blocks re-derived from verbose RPC, 0 disagree |
| Coordinator | `enhance-pir-coordinator-01`, 8 vCPU, 62 GiB, `/srv/zakura` 447 GB free; filter service unit reads `/srv/zakura/transparent-shards-v7`, binary `ddac2cde…` |
| Pilot worker | `transparent-pir-worker-01`, 4 vCPU, 15 GiB, release `d1f86c3`, binary `b95baba2…`, unit `--shard-dir /srv/transparent-pir/shards --cache-bytes 8589934592 --build-slots 1 --query-slots 2`, `MemoryMax=12G`, 3 `recent-8k` shards through 3,473,474, 3 revisions held, rollback binary/unit/Caddyfile present |
| Public map | Served digest `581cb0f5…` identical at worker loopback, `https://transparent-pir.valargroup.dev/v1/shards` and `https://enhance-pir.valargroup.dev/v1/filters/shards` |
| Not yet observed | A revision-bound query result on the public path; the full-chain publication (does not exist yet) |

Record later observations the same way: a dated evidence directory from the workflow's artifact, and this table updated to the newest run. Keep secrets out; keep hostnames only after verification.

## Live deployment: observed 2026-09-08, full set and fleet

Later the same day. Sources: the pilot deploy ([run 34194516569](https://github.com/valargroup/enhance-pir/actions/runs/34194516569)), the fleet deploys ([34199606073](https://github.com/valargroup/enhance-pir/actions/runs/34199606073), [34206126956](https://github.com/valargroup/enhance-pir/actions/runs/34206126956)), the router-only deploy ([34204103032](https://github.com/valargroup/enhance-pir/actions/runs/34204103032)), and the load runs under [evidence/runs](evidence/runs/). Hosts were read over SSH at the times stated in those records.

| Field | Observed |
|---|---|
| Full-chain set | `/srv/zakura/transparent-shards-v7-full` on the coordinator: 174 shards (160 `archive-wide`, 14 `recent-8k`), map `06e5fa2b…`, [published](evidence/publication-2026-09-08/README.md) and [verified](evidence/publication-2026-09-08/README.md) |
| Pilot worker | `transparent-pir-worker-01` serves the full set at release `00afb3e` behind an 8 GiB cache, `MemoryMax=12G`; the public name still points here. OOM-killed once under two concurrent cold archive restores ([record](evidence/runs/pilot-cold-2026-09-08-b/README.md)) |
| Fleet | `transparent-pir-recent-01..04` (`s-4vcpu-8gb`, 10.142.0.10/.8/.7/.12), `transparent-pir-archive-01..02` (`m-8vcpu-64gb`, 10.142.0.6/.9), `transparent-pir-router-01` (`s-2vcpu-4gb`, 10.142.0.11, public 209.38.42.220); all ams3, Terraform-managed, `needrestart` exemption on every host |
| Fleet activation | Workers at `8802cd0` under assignment `baf1e664…` (headroom 0.06: 80 archive shards per owner, 14 recent shards per replica); every worker warm; router Caddyfile rendered from the assignment with the public site and an internal listener on `10.142.0.11:8080` (VPC, coordinator only) |
| Resident memory, warm and idle | archive owners 46.7–47.0 GB RSS (45.0 GiB reserved); replicas 4.2–4.4 GB (3.5 GiB reserved). Under an 8-wallet pass: owners 49.7 GB RSS / 51.7 GB cgroup of 56 GiB; replicas 4.6–4.9 GB of 7 GiB |
| Warm-up | 4.5 s per table runtime with one build slot, and the same with two: about 12 min per owner on `archive-01`, about 20 on `archive-02`; about 4 min per replica |
| Warm serving, 8 wallets, through the router | every covered sync exact; catch-ups p50 0.25–0.40 s, six-month restore 0.8 s, old-birthday restore 3.2–3.8 s (p99 23 s), 40-script wallet 6–7 s; no queue rejections ([run c](evidence/runs/fleet-warm-2026-09-08-c/README.md)) |
| Public name | `transparent-pir.valargroup.dev` → pilot worker. The router holds no certificate until the name moves to it (Terraform `transparent_public_dns_target=router`) |
| Not yet observed | The fleet under 32, 128 and 512 wallets (series running); the public name on the router; the filter origin on the coordinator serving the full set |


## Decision and evidence state

The user accepted the two-tier target on 2026-09-07. [Deployment](deployment.md) owns its settings. Full-chain uniform geometry and single-c8 evaluation/residency evidence exist; mixed publication costs, target-host HTTP capacity, cross-host scaling and mobile wallet latency remain unmeasured. [Remaining work](remaining-work.md) is the authoritative checklist.
