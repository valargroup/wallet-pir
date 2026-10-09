# Transparent PIR status

Updated 2026-09-30 UTC (recent replicas on `6360f0d8` below); milestone acceptance remains separately scoped. The target remains an opt-in, recovery-only macOS beta on
existing infrastructure. **M0, M1 and M2 are accepted; M3 is partially validated;
M4–M6 are open.** This records observed progress, not a new live fleet health
check. [Remaining work](remaining-work.md) is the authoritative outstanding
checklist; [deployment](deployment.md) owns operating targets.

## Fleet redeploy to a317455e, 2026-10-09

The history workers and the publisher run `main` at `a317455e`, the release from full-CI run
37921342549. Roman approved the deploy. Nothing that clients see changed: the publisher still
declares no re-cuts, and every sealed map entry is the same as before. New on history is runtime
code that had not served it before: the batched recent hint, admission against memory in use,
held-byte accounting, restore-slot release and the tail sealer
([evidence](../evidence/fleet-redeploy-2026-10-09/README.md)).

- **Binaries.**
  - recent-01, recent-02 and archive-03: `transparent-shard-server` `6db1fa05…` → `34ba7ebb…`,
    `shard-control` `6dfe78fa…` → `9208555a…`.
  - Coordinator: `transparent-publish-controller` `a68dca01…` → `13af048e…`. `controller.json`
    `source_sha` is now `a317455e`, and new publications record it as `tool_sha`.
  - Unchanged: the coordinator's `shard-assign`, `shard-control` and `transparent-filter-server`,
    the router, and txid display.
- **Order.** Each worker was staged and verified with no restart. Then, between 11:59 and 13:08
  UTC:
  - recent-01 was rolled, with a 33-minute exact load while the recent tier ran mixed versions;
  - recent-02 was rolled;
  - the controller was swapped;
  - archive-03 was restarted from its disk runtime cache.

  The two-replica roll used `roll-recent-replicas.py` with its other-serving floor set to 1 for
  this run only; the committed default stays 2.
- **Outages.**
  - Recent shards: none. Each replica took 24 s.
  - Public metadata: at most 3.3 s of 502/503 at the controller restart, not the expected
    30–60 s.
  - Archive shards 0–81: 339 s. The shard set loaded in 224 s, then all 164 runtimes were
    restored from disk in 112 s with no misses.
  - Publication stalled for the archive window; the first activation after it had 290 s
    freshness, and the next was back to 12 s.
- **Validation.**
  - The 5 QPS exact load ran 30 minutes on the new fleet: 9,088 exact, 0 errors.
  - Freshness p50 was 12.8 s and max 23.4 s.
  - Both origins served byte-identical maps, and the sealed prefix equals the pre-deploy map.
  - Cross-replica setups are identical, and the new `shard-assign` re-serializes the live map.
  - `txid_live_lookup` passed.
  - APM opened no incidents; only shadow alerts fired during the archive window.
  - The independent canary, moved to the v11 fixture by a separate change at 13:50 (below), has
    passed every sample since, all of them against the fully redeployed fleet: 30 successes and
    no failures at 14:19.
  - All workers are warm. archive-03 uses 33.6 GB and has 30.5 GiB available.
  - Coordinator `/srv/zakura` has 22.8% free.
- **20 QPS.** 10 minutes on top of the 5 QPS load: 12,290/12,290 exact, 0 errors, 20.51 QPS,
  p50 17 ms, p99 59 ms. On 2026-09-30 (v10, three recent replicas) p99 was 48 ms; on 2026-09-29
  (v10, two replicas) it was 53 ms. The 423 missed slots came from the client host, which was
  also running CI builds.
- **Not covered.**
  - The wallet regression still stops with publication drift: its fixture pins the v10 map. No
    wallet sync ran against production.
  - Txid display was not redeployed.
- **Deviation.** The 5 QPS load stayed stopped about 21 minutes longer than needed after the
  archive was warm.
- **Rollback.** Material is under `/opt/transparent-publisher/rollback/roll-a317455e/` on each
  worker. For the controller, copies of the previous binary (`a68dca01…`, also in the `12ce1291`
  release artifacts) and of `controller.json` are in `/root/deploy-a317455e` on the coordinator.
- **Pins.** Operations source now selects this release's worker pair (`34ba7ebb…`,
  `9208555a…`) and controller (`13af048e…`) for version-1 schema operations, and
  attributes reconciler controls to the live adapter `35b6b436…` and worker `shard-control`.
  Neither that source nor the portable copies are staged on the coordinator, where the
  bundle sits read-only under `/opt/transparent-publisher/releases/a317455e…/binaries`.
  A read-only look at the coordinator on 2026-10-09 found:
  - `portable-workers/releases` holds only `6db1fa05…` and `6dfe78fa…`;
  - the `4c85b6c2` tree's adapter is `35b6b436…`, while its staged receipt still records
    `f3df5c53…`;
  - the reconciler runs that adapter from its pinned fragment `869e2606…`, active since
    2026-10-07 15:45 UTC.

  So the 2026-10-03 transaction's recorded rollback, which runs from `4c85b6c2`, fails that
  tree's receipt check (see
  [remaining work](remaining-work.md#transparent-operations-after-the-2026-10-09-redeploy)).

## Quality monitoring after the v11 cutover, 2026-10-09

A read-only production inspection on 2026-10-09 found two monitoring inputs that
the 2026-10-03 v11 cutover left behind. Serving was not affected.

- **Transparent canary failing.** The independent canary on `wallet-pir-monitor-01`
  reported `oracle_invalid` on every sample: 129 failures, no successes. Its fixture
  is the 2026-09-29 v10 load fixture, and the canary refuses a fixture whose schema
  differs from the service's. Its binary is also from before v11: it binds every
  query to schema v10, so a v11 fixture alone would not make it pass.
- **APM Transparent view stale.** pir-apm read the pre-v11 roster and the v10
  load's `status.json`, last written 2026-10-03 17:35 UTC.
  `/etc/pir-quality/qualified-workers.json` still names the destroyed recent-08;
  it is the stopped v10 load's pin file, which nothing current reads.
- **Source fix.** The canary now binds queries to its fixture's schema. A new
  script derives its pins from the v11 load fixture, and another moves the APM and
  probe configs. See [observability alerting](../../enhance/docs/observability-alerting.md#seven-day-quality-history-and-transparent-page).
- **Deployed 13:50 UTC.** The canary was rebuilt from `854f5677`, given the v11
  load fixture with its anchor at block 3,488,499, and has passed every sample
  since. Its two shadow incidents closed. APM now reads the v11 roster and load
  status, which is fresh. Only `pir-monitor` and `pir-apm` were restarted
  ([evidence](../evidence/quality-monitoring-v11-2026-10-09/README.md)).
- **Open.** The service and quality alert families stay in shadow pending their
  24-hour review.

## Sealed tier boundary rule, 2026-10-08

`main` at `569f68e6` changes how `shard-cutoff` picks the boundary between the archive and recent
tiers. It still finds the height six calendar months before the anchor. It then moves the boundary
back to the start of the archive shard that is still filling at that height, so the last archive
shard is sealed when it is full rather than cut off part-way. No publication or cutover was made:
v11 keeps serving with `recent_from` 3,289,805.

- **Checked on the live journal.** The new tool was run read-only on the coordinator at v11's
  anchor, 3,500,738
  ([evidence](../evidence/tier-boundary-sealed-2026-10-08/README.md)):
  - the six-month height is 3,289,805, the same as v11's;
  - the boundary moves back to **3,231,753**, with 81 archive shards before it;
  - this matches the live map: shards 0–80 are sealed and full, and shard 81 starts at
    3,231,753, with the same hash on both sides of the boundary;
  - live shard 81, cut short by the old rule, is 23.6% full.
- **Next full publication.** At the journal tip, 3,511,195, the six-month height is 3,301,394,
  but the boundary is still 3,231,753, again with 81 archive shards. A full publication made now
  would start the recent tier there. The boundary only moves in whole archive shards.
- **Cost.** Each run took 20–24 minutes on the coordinator, under a 12 GiB, two-core limit at
  idle priority, most of it replaying 3.3 million blocks. The process peaked under 700 MiB.
- **Recent replicas.** The recent tier would gain heights 3,231,753–3,289,804, about two more
  recent shards (11 instead of 9; the count could be one higher or lower). Each recent shard
  reserves 176.1 MiB of a replica's 5 GiB runtime cache, and uses about 112 MiB once built.
  - Eleven shards fill 38% of the cache; the planner allows 27.
  - Available host memory would fall from 71% to about 68% on recent-01, and from 77% to about
    75% on recent-02.
  - The shard server would use about 1.5 GB of its 7 GiB `MemoryMax`.
  - Both replicas have room. The bytes per shard are high confidence and the shard count medium.

## Txid display v2 in production from genesis, 2026-10-08

Production `/v1/txid/` serves [txid display v2](txid-display.md) from height 1.
v1 is retired. Roman approved the host, the deploy and v1's retirement. There
were no clients, so v2 replaced v1 directly, with no side-by-side lineage.

- **Root.** Bootstrapped offline on the coordinator from the v2x genesis
  journal (`/srv/txid-display-genesis/journal-v2x`, 3,508,674 blocks) into
  `/srv/txid-display-genesis/v2/root`, map `d5ab07a7…`:
  - `txid-2k`, 40,000-entry archives, `max_archive_shards` 1,000;
  - 425 sealed archives plus recent, 17,024,723 entries;
  - every archive is one table, at most 54.5% full.

  Census:
  - all history: 62.3% of entries complete;
  - the last six months (29 archives): 86.0% complete, 7.6% with more than two
    outputs, 6.4% with several source scripts (not an omission for a wallet's
    own multi-address send).
- **Hosts.**
  - Archive owner: the new `transparent-pir-txid-display-01`
    (`m-8vcpu-64gb`, 10.142.0.6; `transparent-txid-display.tf`). Unit: 24 GiB
    cache, 40G `MemoryMax`, 4 build threads.
  - Recent replica: recent-01, 2 build threads, `CPUQuota` 200%, no drop-ins.
  - Controller: the coordinator, `replay-then-live`.
  - Release: CI full `d730ed6b`.

  Expected memory is about 426 tables × 40.05 MiB = 16.7 GiB, plus about
  2.5 GiB a year.
- **Deploy.** `txid-display-*` phases `stage`, `workers`, `route` and
  `controller` with a v2 request (local deploy state). The v1 firewall rule and
  router hook stayed in place. Three faults surfaced, all fixed:
  - the coordinator's publisher known_hosts lacked the new host;
  - recent-01 kept v1 worker state, which the v2 worker refuses;
  - the route check read the full map through a 64 KiB helper (`33a63988`).
- **Verified.** Vizor's `txid_live` passed against production:
  - a found lookup in 4.0 s with two queries;
  - an absent txid sends the identical transcript;
  - a height below the window sends no query;
  - the txid is in no request.

  `covered_through` followed the tip after the controller started.
- **Retired.** archive-03's display worker is removed, which returns up to 6G
  to history.

  Deleted:
  - the v1 journal and root on the coordinator;
  - the v1 genesis journal and build directories on the dedicated volume;
  - v1 worker data on archive-03 and recent-01;
  - superseded releases.

  Kept: `/srv/zakura/txid-display-poc/measure`.
- **Not done.**
  - Shipped runtimes are not enabled for v2.
  - Wallet clients ship with zakura-core/wallet-libraries#127 and
    chainapsis/vizor-wallet#885.
  - Wallet databases created by #879 builds keep the pre-v2 display table; use a fresh wallet database.

## Txid display v2 on `main`, 2026-10-08

`main` now carries [txid display v2](txid-display.md): fixed 113-byte entries
with the first address-shaped source, outputs 0–1 and named omissions, in one
table per bucket. History-attached display is removed.

- **Deploy gate.** Do not redeploy txid display from `main` until the v2
  cutover. The v1 proof of concept keeps running its deployed binaries.
  `main` derives entries only from v2x source sidecars and serves only v2
  tables. It reads v1 sidecars only for the oversized history events they
  hold. The last v1 source is `a368f19b`.
- **Local evidence.** `make transparent-txid-demo` passed on the frozen
  mainnet and genesis vectors. All 14 eligible entries matched the independent
  Python derivation, both after extraction and through PIR lookups. Every
  lookup, found or absent, sent two queries of 40,200 B and received two
  replies of 5,648 B. `--corrupt-oracle` failed as required.
- **Superseded** by the production deployment above.

## Shipped txid display runtimes deployed, 2026-10-08

`--ship-runtimes` ran on the tiered proof of concept from 10:49 to 13:21 UTC, when
the proof of concept was stopped for the txid display v2 cutover
([evidence](../evidence/txid-display-shipped-runtimes-2026-10-08/README.md)). Over
121 cycles: controller prebuild p50 2.8 s, worker prepare p50 1.4 s (was 7.0 s),
worker CPU 1.5 CPU-s per block (was 11.5–15), 242 shipped loads and 0 fallbacks,
recent lookup p99 during loads 1.37× the quiet tail (was 2.5×). Block to serving
p50 6.7 s (was 8.5 s) but p95 29.9 s (was 12.4 s): the 96 MiB candidate copy to
recent-01 took p50 1.8 s and p95 10.2 s, with 53 of 121 copies over 2 s, so the
copy tail now owns the block-to-serving tail. History on recent-01 stayed within
its hourly prewarm range except during another session's bootstrap on the
coordinator; its multi-second query stalls did not cluster in display copy or
load intervals. v2 runs without `--ship-runtimes`; the follow-ups are in
[remaining work](remaining-work.md#txid-display-shipped-runtimes).

## Shipped txid display runtimes, 2026-10-07

`txid-display-controller run --ship-runtimes` ([design](txid-display.md#shipped-recent-runtimes))
is on `main`, off by default and not deployed. With it the controller builds
the recent revision's runtimes and ships them at the candidate root; the
recent replica loads and self-checks them instead of building, and builds
locally, counted, when a file is missing or refused.

[Pre-deploy bench](../evidence/txid-display-shipped-runtimes-bench-2026-10-07/README.md)
on roman-dev-2, synthetic chain, controller and worker each on two CPUs:
- 40.05 MiB per file, 80.09 MiB per block (estimate 80–140 MiB).
- Controller prebuild p50 4.36 s, p95 5.19 s; worker prepare p50 3.62 → 1.33 s
  and CPU per block 5.80 → 1.15 CPU-s; 0 fallbacks in 48 loads.
- Local rsync of one block's delta: 96 MiB in p50 0.33 s. The private-network
  copy is not measured; production copied a 16 MiB delta in p95 2.37 s.
- On equal CPUs the controller cycle got slower, p50 4.2 → 6.4 s: the prebuild
  is on the critical path. Whether production gets faster depends on the
  coordinator out-building recent-01 under history load, which only the
  deploy can show.

## Txid display genesis census and layout, 2026-10-08

The genesis display journal completed on the coordinator at 22:56:45 UTC on
2026-10-07: 3,508,674 blocks and 17,024,724 display records. A read-only
journal census ran over it
([evidence](../evidence/txid-display-genesis-census-2026-10-07/README.md)),
and the layout analysis was derived offline from that census.

- **Inline.** 92.5% of records are at most 128 bytes. No fee is unknown.
- **Pages.** Old archives need far more page rows than live ones. A 40,000-record
  archive needs a median of 8,034 rows in Sprout and 116–197 from NU6.1 on, and
  up to 17,375. At `txid-2k` that is 899 page segments, so genesis needs
  **1,325 runtimes and 51.8 GiB** built, not 850 and 33.2 GiB.
- **Host.** The analysis recommends `m-16vcpu-128gb` with today's layout:
  a 71G `MemoryMax` with a year of growth. On the 64 GB host, only 80,000-record
  archives before NU6 fit.
- **Anonymity.** Sharing pages changes no anonymity class under per-archive
  lookup.

Nothing was built or deployed. Runtime sizes above 4,096 rows are unmeasured.

## Split txid display map, 2026-10-07

The split map ([design](txid-display.md#split-map)) is on `main`, not deployed.
Workers serve `/v1/txid/map` and `/v1/txid/map/{base}/{sha256}` beside the
unchanged `/v1/txid/shards`; `transparent-txid-client` reads only the split.
Live workers and already-built clients are unaffected until a release is
deployed, and the ignored `txid_live_lookup` test fails against production
until then, because production does not serve `/v1/txid/map`.

Map body bytes per lookup, from `transparent-txid-client/tests/map_bytes.rs`
on synthetic maps with the live entry shape. Gzip is level 6, as the sizing
used; the route snippet gzips the two new routes when the client accepts it.

| Entries | Full map, raw / gzip | Cold archive lookup | Cold recent lookup | Warm | After a 409 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 14 (13 archives, 1 chunk) | 8,854 / 1,941 | 7,074 / 2,171 | 912 / 547 | 0 | 912 / 547 |
| 426 (425 archives, 14 chunks) | 259,264 / 46,258 | 17,300 / 4,817 | 2,227 / 1,134 | 0 | 2,227 / 1,134 |

A cold archive lookup fetches the recent map and one chunk; a full 32-archive
chunk is 15,073 B raw and 3,683 B gzipped. A 409 refetches the recent map
alone; a chunk is refetched only after a seal or window drop changes its
digest. The recent map holds one 64-hex digest per chunk, so after a 409 it
costs about 1.1 KB gzipped at 426 entries, not the 0.5 KB the plan assumed.

With the sizing's formula (104,700 B for a cold inline lookup without its
map), a cold inline lookup at 426 entries costs 109.5 KB gzipped and 122.0 KB
raw, against 364 KB with the full map; a cold 4-page lookup costs 315.2 KB
gzipped. Warm lookups do not change.

In the in-process tests (`release-fast`), 22 client tests pass and one live
test is ignored. They include a new chunk-tampering test, and the transcript,
409, cancellation and differential tests requalified for the chunk request.
On the two-entry fixture a cold inline archive lookup moves 80,400 B up and
35,902 B down, and a 409 refetch moves 880 B. Seven tiered display tests
pass, including the split map served beside the unchanged full map across a
seal, from a retired snapshot for a client on the old recent map.

## Wallet txid client, 2026-10-07

The synchronous wallet client `transparent-txid-client`
([design](txid-display.md#wallet-client)) passed its unit tests and 20
in-process tests in `transparent-shard-server/tests/txid_client.rs`. The
in-process tests ran in `release-fast` against the two-worker display fixture.
They cover the transcript, 409/503/421 handling, tampering, cancellation, route
whitelisting and a differential check against the reference client on 95
lookups. Body bytes per lookup, excluding HTTP headers (up / down):

| Lookup | Up | Down |
| --- | ---: | ---: |
| Inline or absent, cold (init, map, manifest, setup) | 80,400 | 36,031 |
| Inline or absent, warm | 80,400 | 11,296 |
| 1 page, cold | 120,600 | 61,804 |
| 2 pages, warm | 160,800 | 22,592 |
| 5 pages, warm | 281,400 | 39,536 |

The ignored `txid_live_lookup` test passed once against production at 16:38 UTC.
It found mainnet txid `fd4667e1…effbf` at height 3,410,000 in archive shard 0,
with a cold transfer of 80,400 B up and 43,379 B down. These are not acceptance
measurements.

## Scaler inputs after the v11 cutover, 2026-10-06

History was cut over to schema v11 on 2026-10-03 at 17:35 UTC. The status
entries below do not record that cutover.

- **Scaler blind since the cutover.** The cutover seeded
  `/opt/transparent-publisher/v11/state` without `inventory.json`. The scaler
  reads worker upstreams only from the inventory, so it scraped nothing. All
  3,951 decisions it logged from its start at 2026-10-03 17:56 UTC to
  2026-10-06 12:31 UTC held on "inventory missing"; nearly all also held on
  "metrics: not scraped". Routing was unaffected: v11
  membership showed both recent replicas and archive-03 serving.
- **Inventory seeded.** At 13:26 UTC, `transparent-fleet-inventory.py init`
  wrote revision 1 from the v11 roster and active assignment: three static
  members and archive range `a0` 0–81. It regenerated `v11/roster.json` with
  identical content. The original bytes of that file and `credentials/known_hosts`
  were then restored. From 13:28 the scaler, still in `observe`, reported
  "steady: desired 2, serving 2, offered 4.0 qps" with p50 8 ms and p99 24 ms.
  The cutover seed now writes the inventory itself.
- **Actuator disabled.** `transparent-fleet-actuator` still used the pre-v11
  `scaler/actuator.json` with the pre-v11 fleet configuration, and its
  `scaler/policy.json` was `act`. A manual scale-out would have created a
  droplet and then failed against the stale pre-v11 membership. At 13:26 UTC
  `/opt/transparent-publisher/scaler/disabled` was created; each run now logs
  `actuator_disabled`.
- **APM re-pointed.** pir-apm's `scaling` family (shadow mode) had read the
  pre-v11 `scaler/status.json`, last written 2026-10-03 17:35:37 UTC. At 13:27 UTC
  its drop-in was changed to `v11/scaler/status.json` and pir-apm was restarted.
  Backups are in `/root/v11-inventory-seed-20261006` on the coordinator.
- **Open.**
  - The actuator stays disabled and the scaler stays in `observe` until the
    schema recipe covers the actuator unit.
  - `v11/state` held 12,436 per-publication files after three days. The recipe
    caps a captured candidate namespace at 512 entries.

## Txid display block-to-serving change, 2026-10-07

At 14:47 UTC the recent display worker on recent-01 was restarted with `07f90675`
(batched hint for `txid-2k`) and two build threads on up to two CPUs, through
a systemd drop-in rather than `txid-display-deploy`. Display still loses CPU to
history: weight 50 against 100, and nice 10.

- **Block to serving.** p50 16.1 s and p95 18.9 s over the previous 24 hours
  (30 of 1,152 blocks over 20 s, worst 97 s). Over the first 40 cycles after:
  p50 8.5 s, p95 12.4 s, worst 12.9 s, none over 20 s.
- **History on recent-01.** Prewarm p50 10.06 to 10.33 s and p95 11.67 to
  12.28 s over 40 rebuilds; not yet distinguishable from noise.
- **Drift.** The reviewed deploy request still records the old limits (100%
  quota, weight 20, one build thread) and release `d191f86b`; the drop-in
  overrides both.

[Evidence](../evidence/txid-display-freshness-2026-10-07/README.md); per-block
incremental update is [valargroup/wallet-pir#128](https://github.com/valargroup/wallet-pir/issues/128).

## Tiered txid display in production, 2026-10-07

The tiered txid display proof of concept has served beside history since
2026-10-06. Deployed from release `d191f86b` (CI full run 37388223909,
success), with `archive_target` 40,000 and N=1 buckets. History changed only
where the router hook replaced its v11 adapter with main's, which adds the
opt-in `route_imports` key, and added that key to its `fleet.json`.

- **Deploy.** Ten `txid-display-*` transactions started between 12:31 and
  13:23 UTC on 2026-10-06 and committed by 13:26, in order: stage, ingest
  smoke, ingest start, firewall, router hook, bootstrap, workers, route,
  measure start, controller. Each has a recorded rollback. The journals are
  retained in the [evidence](../evidence/txid-display-tiered-2026-10-07/README.md).
- **Serving.** `/v1/txid/` on transparent-pir.valargroup.dev routes to port
  8095 on archive-03 (archive owner) and recent-01 (recent replica). The
  controller, observer and map watcher run on the coordinator.
- **Health at 13:55 UTC on 2026-10-07.** All txid units active with 0 restarts.
  Both workers were ready and warm: archive-03 with 26 of 26 runtimes through
  height 3,502,508, recent-01 with 2 of 2 through 3,509,538, against a public
  height of 3,509,537. The controller reported 13 archives and 47,268 recent
  records, with 15.7 s from block to serving on its latest cycle.
- **Block to serving, live.** From the controller timeline, 2026-10-06 15:27 to
  2026-10-07 13:40 UTC (tip above 3,508,457): p50 16.0 s, p95 18.8 s, max
  97.2 s over 1,054 cycles; 22 cycles over 20 s. The 97.2 s cycle followed the
  only controller error, a failed recent-replica adapter call at 2026-10-06
  20:00 UTC. During 20 QPS window A, 4 cycles took 20.4–24.0 s. Roman accepted
  the 20 s miss for the proof of concept. A later recent-01 change is recorded
  [above](#txid-display-block-to-serving-change-2026-10-07).
- **Load, 2026-10-07, from Amsterdam over HTTPS.** Window A, 20 queries/s:
  10,800/10,800 exact, p99 130 ms, 540 of 600 s. It ended on a hard stop: a
  routine router Caddy reload reset one history connection, which recovered on
  retry. Window B, 20 lookups/s for 600 s: 12,004/12,004 exact, 0 errors, p99
  187 ms; history p99 at most 75 ms (W0: 33 ms) with 0 errors.
- **Bandwidth per lookup.** Inline 92.8 KB warm and 132.4 KB cold. One to three
  pages stay under 300 KB cold. Four pages are under 300 KB only warm
  (343.7 KB cold), and five to seven pages are over (to 496.0 KB). Measured
  bytes are 0.2–25.1 KB above computed; 0 stale retries. 94 of 567,323
  published txids have four or more pages.
- **Seals and reorgs.** 13 archives: 9 at bootstrap and 4 during replay. No live
  seal yet: the recent shard held 48,761 records at 20:04 UTC, and a seal
  needs about 50,000. 6 one-block reorgs live, 0 drops.
- **History router reloads.** Before the fix, router-01 reloaded Caddy 84 times
  for 42 history activations on 2026-10-07 (00:00–14:41 UTC), always in pairs. recent-01 finished
  prepare after the 3.5 s prepare grace, and its activation waited on the
  reconciler's worker lock until the 0.5 s activation grace cancelled it.
  Roman approved two settings in `/opt/transparent-publisher/v11/fleet.json`:
  - `activation_grace_seconds` 3.0, set at 14:48:40 UTC;
  - `prepare_grace_seconds` 3.5 → 7.0 at 15:45:35. That edit wrongly removed
    `activation_grace_seconds`, on a diagnosis that blamed prepare alone. It
    was restored at 16:53:10.

  With both, the session that made the change counted 147 publishes,
  0 cancelled activations, 0 rejoins and 0 router reloads through 19:57 UTC.
- **History without txid load.** The measurement supervisor tripped three
  times with no txid load: history self-paused once, SSH from the Mac timed
  out once, and history p99 spiked to 845 ms once. Over about 40 hours
  without txid load, history p99 exceeded 300 ms in 213 minutes, max 2.77 s
  (the operating session's count; its samples are not retained).
- **Disk headroom incident: resolved.** At 19:53:04 UTC on
  2026-10-07, history's 5 QPS load latched on "transparent-coordinator: disk
  headroom below 20%". A separate session's full-chain genesis display ingest
  into `/srv/zakura/txid-display-genesis` (25 GB at 12%) caused it.
  - **Cause fixed.** The ingest was stopped at about 20:00 UTC. Roman approved
    a dedicated 150 GB volume (Terraform, wallet-pir `983cbedb`), mounted at
    `/srv/txid-display-genesis`. The partial journal was moved there, and
    `/srv/zakura/txid-display-genesis` was deleted.
  - **Headroom restored.** At 20:12 UTC `/srv/zakura` was 78% used, with
    233 GB free.
  - **Latch cleared.** With Roman's approval, `latched.json` was moved to
    `latched.json.cleared-20261007T2020Z` and `transparent-5qps-continuous`
    restarted at 20:19:36 UTC (the process also holds the latch in memory).
    At 20:21:59 the load was running: 299 exact, 0 errors, 0 missed slots,
    p99 34 ms over the trailing minute.
- **Open.** Not measured live: the anonymity census and audit, `verify` of
  sealed digests, and growth across live seals. Roman's keep, stop or retire
  decision is open
  ([evidence](../evidence/txid-display-tiered-2026-10-07/README.md),
  [gates](remaining-work.md#tiered-txid-display-proof-of-concept-2026-10-05)).

## Txid display backfill sizing, 2026-10-07

**Genesis display ingest running.** The orchestrator reports that a genesis
display ingest has run on the coordinator since 16:47 UTC on 2026-10-07:

- unit `transparent-txid-display-genesis-ingest`;
- `event-ingest --txid-display --start-height 0`, under the proof of concept's
  ingest limits;
- into `/srv/zakura/txid-display-genesis/journal`;
- expected to finish around 21:15 UTC.

It was stopped at about 20:00 UTC, after it pushed the coordinator's disk
headroom below 20% and latched history's load. Its partial journal now sits
on a dedicated 150 GB volume at `/srv/txid-display-genesis`
([above](#tiered-txid-display-in-production-2026-10-07)).

This task did not observe it: it had no deploy inventory or SSH. Nothing
serves from that journal yet. Bootstrap, the cutover and the host remain
proposals.

**Host decision deferred** (as of 2026-10-07). The display-archive host was
not chosen yet; it waited on layout experiments on real data. Those are now
done ([above](#txid-display-genesis-census-and-layout-2026-10-08)).

Planning only; nothing was changed. At 16:05 UTC the public maps had these
values:

- **Display map.** It covered 3,407,001–3,509,639 with 567,880 records:
  13 sealed archives of 40,001–40,013 records and a recent shard of 47,820.
  Every archive has one directory and one page segment, and uses at most 776 of
  2,048 page rows.
- **History map.** Its txids over the same heights match the display records
  within 0.4%. That makes history txids a usable proxy below 3,407,001, where no
  display data exists.
- **Map transfer.** The display map is served uncompressed, at 632 B per shard.

The live root's window of 24 archives starts dropping archives around
mid-December 2026 at the current seal rate. Roman then chose a genesis floor.
That is about 425 archives, which do not fit archive-03. The plan is a fresh
lineage on a dedicated 64 GB host, after a reservation true-up and a split
display map. Six runtimes per table on roman-dev-2 measured `txid-2k` and
`txid-4k` runtimes at the four-byte matrix size, 44% and 40% below the
reservation
([plan](deployment.md#txid-display-backfill-below-3407001-proposed),
[evidence](../evidence/txid-display-backfill-sizing-2026-10-07/README.md),
[gates](remaining-work.md#txid-display-backfill-below-3407001-proposed-2026-10-07)).
No production host was logged into: archive-03's current memory and disk were
not measured.

## Tiered txid display proof of concept, 2026-10-05

Source for a separately published, time-tiered and hash-bucketed txid display
([design](txid-display.md#tiered-display-publication-proof-of-concept)) passed
its package and ops tests at `1f89cac0`. Only local, synthetic evidence existed
on this date; the production deployment is recorded above.

- **End to end.** Two workers behind a local proxy and the controller replaying
  12,000 synthetic blocks: 4 seals and 2 window drops reproduced by `verify`;
  0 audit violations over 112 maps; 4,931 of 4,931 lookups exact.
- **Rebuild.** Under recent-01's planned limits (1 CPU, 1 build thread), per-block
  freshness stayed at 7–12 s up to 60k recent records. It reached 22 s at 70k,
  where the directory needs a second segment.
- **Bandwidth.** Metered bytes equal computed bytes. Warm transcripts are
  92.6 KB inline and 277.8 KB at 4 pages. Cold transcripts reach 279.1 KB at
  3 pages and 325.4 KB at 4 pages.
- **Buckets.** No bandwidth change at this shard size. N=4 costs more memory per
  txid and gives smaller classes than N=1.

Page-count classes stay below 10k.
[Evidence](../evidence/txid-display-tiered-2026-10-05/README.md);
[gates](remaining-work.md#tiered-txid-display-proof-of-concept-2026-10-05).

## Publication freshness profile, 2026-10-03

A local benchmark profiled attempt 14's failed freshness, measured at serial
cycles of 23–25 s. The tail used is a deterministic synthetic `recent-8k` tail
derived from the retained mainnet day. Most native publication time was the
sealer's exact page-row count, recomputed over cloned maps for every tail block
at every publication.

Source `921e1642` keeps output byte-identical and cuts release-fast publication
at 97% fill from 9.67 s to 3.78 s on the hub. Recent workers also skip empty
page-hint blocks: 0–0.7 s locally, depending on fill.

A projection still left burst visibility above 30 s for full tails, because
the recent workers' two sequential native tail builds remained.
[Evidence](../evidence/publication-freshness-2026-10-03/README.md).

Those builds now compute the public hint in column-batched exact NTTs. The
hint is unchanged byte for byte, and the published masks and query answers
are identical. On the same retained tails, a two-thread directory-plus-pages
build fell from 4.81 s to 3.05 s at 97% fill, and from 4.19 s to 2.91 s at
24%. Peak memory is unchanged. Arithmetic puts burst visibility at about
23–29 s, with little margin at the top. Nothing is deployed, and freshness
acceptance stays open.
[Evidence](../evidence/prewarm-hint-2026-10-03/README.md).

Review on 2026-10-04 asked for assurance before approval. The batched hint is
now dispatched for recent geometries only; archives keep the reference
product. Its source documents reference provenance and a derivation of root
and evaluation order, residue ranges, the signed CRT bound and the reduction
modulo `q`. Fixed known-answer vectors computed in Python without NTT or CRT
pass through both the batched path and a forced reference fallback. Recent
timing is unchanged from the record above and was not re-measured.
[Evidence](../evidence/prewarm-hint-assurance-2026-10-04/README.md).

## Changed-native activity candidate `c3c66b9b`, 2026-10-04

Operations source now prepares and guards the changed-native candidate
`c3c66b9b` separately from the historical 12ce publication, assignment, samples,
journal and rollback identities. The 18 artifacts are pinned: five roles from
comprehensive CI 37173250956, and 13 supplemental fat-LTO tools whose archive
digest is in the manifest.

A wrapper-only `schema-candidate-*` path verifies those artifacts and retains them
inertly in a separate coordinator namespace. Candidate worker executables stage
beside, not into, the immutable worker publication.

Version-2 service, cutover and product inputs bind candidate bytes. They require
four new candidate-bound gate reports, and they refuse 12ce reports, relabelled or
mixed reports, and changed certificate floors.

The supplemental reader accepted the retained archive read-only. Focused fixture
tests and mutations passed.

Later on 2026-10-04, operations source added a guarded
`schema-candidate-upload-*` path. It moves the three root-held archives to the
coordinator's fixed candidate archive namespace. Fixture tests covered real
pipes, truncation, extra bytes, changed sources, foreign archives, interruption
and explicit reconciliation. No archive was transferred and no host was
contacted. No production host was accessed. Nothing was staged
or deployed, and no candidate gate was run. Freshness, capacity and every other
acceptance gate below remain as recorded.
[Evidence](../evidence/activity-metadata-2026-10-04/README.md).

Operations source then added a guarded `schema-snapshot-*` path for an immutable
snapshot of the full journal. It stops only the reviewed writer, takes the
existing `writer.lock` itself, copies the committed prefix and committed display
sidecars, and restores the same writer with proof. Fixture tests covered:

- the Rust `try_lock` lock protocol, with a probe built by the pinned `rustc`;
- malformed, truncated, checkpoint and reorganization cases;
- writer identity drift and active writer refusal;
- interruption, transport loss and every stop, copy and restore failure;
- resource floors and namespace misuse.

Root's read-only audit later on 2026-10-04 reported the following. This task
did not observe them itself.

- **Writer.** The installed `transparent-publish-controller.service` owns the
  writable journal. Its `data_dir` is `/srv/transparent-activity/full-v3/journal`,
  with `meta.json` at version 3 from height 0.
- **Lock.** `/proc/locks` lists the controller PID as the `FLOCK ADVISORY WRITE`
  holder of the existing `writer.lock`.
- **Size.** `events.bin` is 42,699,881,014 bytes and `blocks.bin` 168,301,056
  bytes, which is 3,506,272 blocks.

The snapshot now covers display sidecars as well. Native `events_at` silently
reads a missing sidecar as no oversized events, and writers outside display mode
write no sidecar. The path therefore:

- requires an explicit sidecar policy and sidecars at root-chosen coverage
  heights;
- pre-copies the immutable sidecars while the controller runs, then re-observes
  every committed block's sidecar and block hash under quiescence;
- refuses controller units with stop-propagating or restarting dependents.

A hub-only probe projects about 4.5 minutes of copying while the controller is
stopped. That projection is not a coordinator measurement.

Root's provisional review then asked for more assurance, and the path now:

- reads every pinned host afresh before any effect, with exact machine pins, the
  shared owner fence, source-staging receipts and recorded live owner or
  descendant processes, keeping raw receipts;
- carries one total deadline, with sampled floors, through every scan, hash,
  remote read, anchor RPC and the final re-verification;
- reserves disk for the exact bytes still to be copied under quiescence;
- re-runs the record-boundary and anchor checks over the copied blocks.

A second provisional review found that the all-host check could still miss
orphaned descendants and older owners. The path now:

- probes every retained owner record and every live process on each host;
- refuses live recorded owners and members of their sessions or process groups,
  with an exact fixture for a dead recorded parent and its live orphaned native
  child;
- refuses unrecorded live processes that hold the production lock, carry the
  inherited-lock variable or run the wrapper;
- bounds the anchor RPC by one aggregate deadline, with no redirects and
  sanitized errors;
- exposes a full-verification contract for root's locked consumer.

A third provisional review then added further requirements, and the path now:

- counts owned `launch`, `child` and `children` identities with `start_ticks` and
  boot identity;
- walks the whole owner namespace within finite bounds;
- closes over live descendants by parent PID;
- refuses unattributable survivors of operation executable and argument classes
  outside reviewed service units;
- refuses non-finite deadlines.

Eighty-three focused tests passed, and all 81 single-guard mutations, including
the helper's, were caught.

Root's fourth review found three owner-contract gaps, and the path now:

- owns a candidate child's `guardian` and the native process nested under
  `native`, which inherits its container's boot;
- treats missing, null or malformed boot evidence as the current boot, so it
  can no longer suppress a matching live-owner refusal;
- refuses owned containers nested beyond the depth bound instead of dropping
  them.

Eighty-five focused tests passed; each new case failed against the previous
source. No host was contacted and no snapshot was taken; see
[the snapshot path](deployment.md#candidate-journal-snapshot).
Operations source then added the locked `schema-candidate-execute-*` path for
the artifact-verification and native-certificate executions. It retains raw
captures for the offline report producer. Root rejected the first version.
The revision adds three controls: an all-host survey under the lock before
mutation, kernel-enforced per-child limits within a finite per-stage budget,
and recovery through a claim with pidfd signals. Root rejected that revision
too: the survey read only candidate owners and latest fences, and recovery
signalled before holding the production lock. The second revision surveys every
retained record of every owner namespace and associates recorded processes with
live ones by start identity, session, group, cgroup, orphan and reparented
windows and descendants. It refuses unattributed processes of the closed
operational classes outside baseline service cgroups. These are fixed
heuristics, not complete descendant clearance. The survey is a reusable
stdlib-only component for root's source bootstrap. Each
child now runs under a guardian with its own deadline that kills an IO-blocked
child and its descendants after receiver death. Reconciliation acquires the
production lock before any owner write or signal. Fixture tests and
single-guard mutations passed on the development hub. Root set all budgets as
explicit ceilings; the 60-second guardian launch window awaits its review. No
candidate native program ran on a production host, and no gate report exists.

Root's review of the third revision found that retained surveys kept raw
command-line operands of unattributed processes. Two fixture tests also failed
on root's Linux run. The revision keeps arguments and token values in memory
only and retains the command line's SHA-256 and length, the token's SHA-256,
the matched class entries and flags. The test failures came from a fixture
lock holder that shared the test runner's session. When that session's leader
was gone or outside the fixture cgroup, the production orphan-window rule
correctly refused it on the coordinator before worker-a. The fixture holder
now leads its own session; production scans are unchanged.

The locally integrated operations source received further ownership hardening:
argument byte/count overflow refuses incomplete process classification;
escaped-process refusals retain argument digests rather than arbitrary operands;
and malformed boot IDs cannot suppress a matching live process identity. Native
capture now records the observed monotonic start/end and repeats its kernel
start identity in the terminal result. These fields support the oracle's temporal
checks; they do not supply its missing whole locked execution owner, safe snapshot
execution, canonical RPC evidence or qualification report. No candidate gate or
production acceptance result follows from these source changes.

Native qualification requests and client inventories now require the exact
coordinator, router and three production worker host names. A pinned partial
inventory refuses before transport construction or owner creation; surveying all
entries in a partial inventory is insufficient. Local subprocess fixtures use
an explicit test-only two-host override. Source and upload bootstrap guards still
need the complete locked fleet handshake before staging can proceed.
Snapshot inventory validation now requires that same complete host set and
distinct machine pins, including a coordinator pin equal to its production
lock identity. A partial inventory refuses before any host probe or transport;
the snapshot rechecks this requirement when it surveys retained owners.

Oracle report retention now carries the same checked owner budget through
bounded canonical encoding, evidence hashing, chunked writes and file/directory
fsync. Capture closure uses that checked writer too. Evidence remains preserved
when ownership is lost after a write; such a refusal cannot finish report
assembly. These cooperative checks now run within the gated post-handshake workflow
worker described below, whose supervisor supplies the independent hard deadline.

A local executor draft now forks one gated qualification workflow under the
receiving process acting as a Linux child subreaper. The worker inherits the
production lock and applies address-space, CPU and core limits; its PID/start,
boot identity and aggregate deadline are durable before it starts. The receiver
waits independently of workflow hashing, validation and evidence writes, and
uses pidfds to stop and reap its owned descendants at that deadline. It refuses
preexisting receiver children before launch. Per-native guardians remain in
place. Its Linux suite passed 56 tests, including all 180 fixture certificate
dispatches and interrupted-owner recovery; a subsequent additional fixture
proved deadline cleanup of a detached child without its token or lock descriptor.
The exact pinned native guardian code now runs from a source file, because its
inline form exceeded the process survey's unchanged argument bound. The receiver
restores its earlier subreaper state after cleanup. Pre-handshake planning,
surveys and terminal owner retention still use cooperative bounds.

The unpublished oracle mode now uses that worker. Its closed request pins the
canonical snapshot request identity and exact owner and manifest bytes. The
full snapshot verifier proves independent bytes, committed boundaries and owned
writer restoration before the native reader starts. The reader uses only the
verified copy, fixed 17-block selection and batch size256, and an owned ephemeral
loopback capture whose upstream and runtime cookie pathname are fixed. Canonical
boundaries bracket the native run; raw attempts and refusals remain retained.
Report assembly fully verifies the snapshot again and binds native kernel and
clock identities, original RPC bytes and capture closure. Resource checks from
the capture thread are serialized with native sampling. Original native failures
remain authoritative if capture closure also fails. The combined Linux source
suite passed114tests, including existing artifact/certificate/lifecycle fixtures
and oracle refusal/composition tests. It qualifies no candidate or production
gate. Final source publication and actual execution remain pending.

On 2026-10-05 root reported that an actual candidate-upload preflight refused in
`bootstrap.verify` on the coordinator with 18 associated processes. Root's
diagnostic names the reply that an earlier successful locked source survey
retained at
`input-staging/fleet-surveys/5b002a24…/f5a20957…/coordinator.json`. It produced
recorded-process, session-or-group and cgroup associations. They covered the
reconciler and SSH processes and also the verified prototype, the canonical load
and its native child. These are listed in the reply's process summaries and
ancillary units. Root holds the raw diagnostics outside Git. Upload status was
`absent` before and after the preflight, and no upload owner or production bytes
were created. Operations source now accepts exact retained bootstrap replies and
attribution snapshots as forensic evidence only at their exact path with a
matching binding. It keeps durable owner records, including their fleet proof
and result, fully checked (see
[deployment](deployment.md#immutable-operation-source-staging-over-ssh)).
Linux fixture tests on the development hub cover:

- a locked run followed by the next survey;
- a reply shaped like the production one, with the prototype, load and load child;
- refusals for live owners, misplaced, tampered or nested replies, malformed
  files, links and foreign operational processes.

No production host was accessed, the preflight has not been rerun, and the
recognizer has not run against root's raw file. Candidate execution surveys do
not use this recognizer.

## Activity metadata candidate, 2026-09-30

Attempt 12 activated a fresh v11 publication after the archive roster correction,
then canonical verification refused a worker warm-publication attestation.
The failed native status body was not retained, so future preparation is a
possible cause, not a confirmed historical diagnosis. Local SSH transport failed
separately; the remote owner completed automatic cold rollback in 389.565 seconds.
Independent private/both canonical native proofs, five reopened SQLite stores
and owner exit checks passed. V11 serving verification now checks the exact
warm, non-invalidated active map while a future candidate prepares, as the native
status producer distinguishes these states. HTTP readiness, assignment, release,
all workers and every independently canonical advertised anchor remain required.
Control and HTTP observations are retained privately before refusal. Initial
candidate prewarm, all phase bounds and all final qualification gates remain
unchanged and open. [Evidence](../evidence/activity-metadata-2026-09-30/attempt12-warm-refusal-cold-rollback-6f37db70.json).

Attempt 11 passed candidate preparation and activation, then the closed startup
guard refused a real publication error: the cloned predecessor roster pinned
archive shards 0–76 while the v11 map requires 0–81. Native `shard-assign`
confirmed the mismatch. Automatic cold rollback passed all five phases in
390.229 seconds; independent private/both canonical native proofs and five
reopened SQLite stores passed, and all owners exited. Service preparation now
requires pinned archive ranges to cover the qualified map exactly, rejecting
stale ranges, gaps, overlap, foreign shards, invalid endpoints and changed roles.
The private generator derives this fleet's single owner's range from the
checksum-verified qualified v11 map. Corrected cutover and all final gates remain
open. [Evidence](../evidence/activity-metadata-2026-09-30/attempt11-archive-roster-refusal-cold-rollback-7bdbcbb9.json).

Attempt 10 passed candidate preparation and independent worker activation, but
canonical verification failed with HTTP 503. Automatic cold rollback passed in
379.941 seconds; private and each canonical native/query/SQLite proof passed,
and all owners exited. The historical response endpoint/body were not retained;
controller startup reconciliation is an inference. A closed startup observation
now permits only the native controller's exact local reconciliation refusal,
with pinned unit/PID/binary, no restart/OOM, 20% resource floors and both public
origins still withdrawn. It is bounded to 300 seconds inside the unchanged
canonical phase; all full canonical gates still follow. Actual corrected cutover
and all final qualification remain open.
[Evidence](../evidence/activity-metadata-2026-09-30/attempt10-canonical-refusal-cold-rollback-9164c2c7.json).

Attempt 9 completed the remote v11 cutover and exact private/both canonical
query proofs, but continuous publication failed: operations inputs used `v2`
instead of the native profile name `zcash-transparent-range-v2`. Load remained
paused with zero completed queries. Explicit ordinary cold rollback passed in
377.309 seconds; private and both canonical queries and reopened SQLite stores
passed, and all remote owners exited. The forward SSH reset and subsequent local
rollback transport timeout remain failures, separately reconciled against the
remote journal. The service-input guard now binds the canonical profile from the
reviewed publication geometry and rejects shorthand/foreign names. Corrected
redeployment and every final sustained/freshness/capacity/lifecycle gate remain
open. [Evidence](../evidence/activity-metadata-2026-09-30/attempt9-cutover-freshness-failure-cold-rollback-57e71be1.json).

Attempt 8 failed archive cache preparation at its unchanged 1,200-second bound.
Automatic rollback then refused eight unexpected Python bytecode files created
by a read-only diagnostic that omitted `-B`; all retained source payload hashes
are unchanged. The original automatic rollback failed before restoration; a subsequent
closed repair completed cold recovery in 382.237 seconds, with exact private and
both canonical query/SQLite proofs and all remote owners exited. The first fresh-source repair also refused the original source inventory before
withdrawal; that failure remains recorded. A closed rollback repair now retains
only bounded compiler-proved import caches outside the source tree with private
intent/completion, then requires the original strict source check. The retained attempt-8 recovery evidence records this separately from the
failed forward deadline. [Focused evidence](../evidence/activity-metadata-2026-09-30/closed-bytecode-retention-focused.json).
Diagnostics must use `python3 -B` before importing retained libraries. Source
file refusal now reports unexpected and missing file counts without exposing
paths. The exact guard remains intact; prior source receipts are preserved.
[Failure evidence](../evidence/activity-metadata-2026-09-30/attempt8-cache-deadline-source-drift-139a54f3.json).

Attempt 7 at `762e81f3` failed guarded candidate cache preparation. The new
reader expected bare Prometheus metric names, while the native exporter attaches
worker labels. Host owners retained `ValueError` without exception text; that
exact exception is not inferred. Automatic cold rollback passed all phases in
376.654 seconds and the transaction is `rolled-back`. Private and each canonical
encrypted-query/reopened SQLite proofs passed; both independently observed
metadata responses were HTTP 200. All remote host owners exited and quality
remains failed with PID 0. [Evidence](../evidence/activity-metadata-2026-09-30/attempt7-cache-reader-failure-cold-rollback-762e81f3.json)
preserves the failed forward gate separately from passing recovery.

The corrected reader uses persistence counters from the native readiness
`runtime_cache` object, with bounded JSON, duplicate-field rejection and strict
nonnegative integer counters. Missing/unavailable observations refuse progress.
Private host owners now retain bounded validation failure details for diagnosis;
public replies retain their existing minimal shape. Actual candidate preparation,
independent activation, successful v11 redeployment and all sustained/lifecycle
gates remain open. All earlier failures below remain historical failures.


Attempt 6 at `721dac6a` failed candidate archive prewarming after 402.587
seconds. Its local SSH transport failed separately, while the locked remote
wrapper survived and completed automatic cold rollback. Transaction
`transparent-schema-20261001T191953Z-73028c5af3dd-c36dd1` is `rolled-back`;
all rollback phases passed in 376.247 seconds, within the 15-minute ceiling.
The private and each canonical origin's encrypted-query/reopened SQLite proofs
completed exact syncs with zero failures. Fresh metadata returned HTTP 200 on
both origins. All remote host owners exited; quality remains failed with PID 0.
[Retained evidence](../evidence/activity-metadata-2026-09-30/attempt6-cold-rollback-721dac6a.json)
preserves the failed forward gate and separate local transport outcome.
This cold rollback result does not accept v11, successful redeployment, sustained
capacity, freshness or the remaining lifecycle faults. Earlier failures below
remain failed history.

Archive candidate runtime-cache preparation was incomplete. The next reviewed
wrapper prepares missing runtimes only after staging while both origins are
withdrawn and the predecessor is stopped, then stops the candidate after cache
writes finish. Its bounded preparation is separate from the unchanged
300-second independent activation warm gate. Actual preparation and v11 cutover
remain pending.


Latest attempt `7401ac97` passed locked preflight, then failed coordinator
baseline capture: the retained v11 activation pointer was misclassified as a
v10 predecessor generation. Coordinator writers stopped; no remote capture or
new candidate staging was reached. The new transaction is `rollback-failed`
because withdrawal required a complete baseline. The reviewed partial-capture
repair verifies unchanged copies and stopped live state before completion;
actual guarded recovery is pending. The `16f806df` repair refused two changed
v11 routing audit files written by the earlier owned withdrawal. Its
[failure](../evidence/activity-metadata-2026-09-30/partial-guard-drift-failure-16f806df.json)
remains failed. A closed reconciliation retains those withdrawn audit bytes and
restores only the independently captured originals before completing capture;
unrelated drift refuses. No new recovery has passed. The next repair also refused the owned public
Caddy guard against the copied original. Its [failure](../evidence/activity-metadata-2026-09-30/partial-public-guard-failure-0dcc7d02.json)
is retained; exact captured guard derivation now permits completing capture
while both origins remain closed, with the original copied bytes unchanged.
Actual `9abd2381` recovery then completed capture/withdrawal, restoration and
private verification, but canonical reopen failed HTTP 503. The late router
capture already contained withdrawn handlers; restoring it overwrote the
regenerated healthy routing. The [failure](../evidence/activity-metadata-2026-09-30/late-router-capture-reopen-failure-9abd2381.json)
is retained. Separate exact captured-guard handling and actual paired canonical
proof remain required. The [failure](../evidence/activity-metadata-2026-09-30/partial-capture-failure-7401ac97.json)
remains recorded.

The [owned warm v10 recovery](../evidence/activity-metadata-2026-09-30/paired-origin-coherent-recovery-a8e6b63b.json)
passed in 92.912 seconds at `a8e6b63b`. The journal is `rolled-back`. Separate
private and both canonical encrypted-query proofs each completed one exact sync
with reopened SQLite stores and no failures. Both fresh canonical metadata
observations returned HTTP 200 through height 3502797. Quality remains stopped.
This is a brief recovery proof, not v11 or sustained acceptance. The preceding
[cold archive deadline failure](../evidence/activity-metadata-2026-09-30/protected-policy-cold-deadline-a8e6b63b.json)
and every original timing failure remain failed. The next fresh attempt failed preflight on a retained candidate publication pointer,
before creating a transaction. Its exact presence and byte capture must be reconciled
before a fresh reviewed v11 deployment.

Latest actual cutover `c28901b4` captured protected coherent baselines on all
five hosts and passed maintenance and v11 staging, then failed candidate warm
verification. Automatic rollback restored v10 but missed its cold archive
readiness deadline. The reviewed `4b474d21` repair subsequently passed exact
restoration and the private native/reopened SQLite proof. The Transparent-origin
native proof also passed, but the Enhance-origin proof failed: transparent
revision setup requests reached its default backend and returned HTTP 404.
Both origins were re-guarded. The [paired-origin failure](../evidence/activity-metadata-2026-09-30/paired-origin-recovery-failure-4b474d21.json)
retains the failed native report, logs and SQLite archive hashes. A closed
checksum-bound routing correction now sends only transparent revision setup/query
paths to the captured private shard router, preserving the Enhance query handler.
Focused validation and actual guarded recovery must precede reopening. No v11
cutover or rollback timing acceptance has passed.

Latest observed recovery on 2026-10-01: the reviewed `09a9f140` relay correction
completed [coherent recovery at a newer v10 revision](../evidence/activity-metadata-2026-09-30/coherent-newer-v10-recovery-09a9f140.json).
The journal is `reconciled-v10`; private verification, reopen and service
verification passed. The owned preparation/resume took 120.465 seconds. Each
of its three native reference reports completed one exact sync with no failures
and no stop reason, with reopened SQLite stores. Both canonical metadata origins
returned HTTP 200, genesis through height 3502677, with 86 shards. This restored
predecessor service at that earlier observation. The public client proof currently
queries Transparent while fetching filters from Enhance. Separate encrypted
query proofs through each canonical origin remain required. The original
rollback and missed 15-minute acceptance remain failed; a fresh protected
baseline and actual rollback/redeploy rehearsal remain mandatory. The failures
below are retained history, not the latest service state.

Actual transaction `transparent-schema-20261001T112410Z-15d5de8189d2-c99edc`
started after locked preflight on 2026-10-01 at 11:24 UTC. All five complete v10
baselines were captured, but the coordinator timed out waiting for the archive
capture's repeated native verification. Remote owners subsequently completed and
exited. Recovery restored v10 bytes and services, then failed warm verification:
authority had restarted publication before workers were proved. Public origins
remained guarded at that failed attempt; the transaction was `rollback-failed`. The 15-minute recovery
acceptance was missed and must be rerun. The [reviewed recovery repair](../evidence/activity-metadata-2026-09-30/interrupted-cutover-recovery-repair-focused.json)
preserves original recipe/baseline bytes and all failures. Its focused check
passed. The repair source was staged on all five hosts, but its first actual
rollback refused at the CLI source identity check before service effects. The
[entrypoint correction](../evidence/activity-metadata-2026-09-30/repair-entrypoint-focused.json)
passed focused checks. Its actual retry also refused at routing source identity
before service effects; the [routing repair binding](../evidence/activity-metadata-2026-09-30/repair-routing-focused.json)
passed focused checks. The [actual repaired attempt](../evidence/activity-metadata-2026-09-30/repair-retention-failure-16f2077d.json)
passed withdrawal but refused restoration because the captured publication was
collected on all three workers. All remote owners exited. The workers are warm
on one newer v10 publication and its coordinator activation bytes remain in
preserved displacements. Guarded adoption of the newer activation completed,
but client reconciliation initially failed. The readiness URL and continuous
current-map verification defects were fixed with focused evidence. The earlier
[real client attempt](../evidence/activity-metadata-2026-09-30/private-recovery-port-failure-ef6f7d59.json)
made two incomplete syncs with ten HTTP 503 responses; it did not pass despite
native process exit 0. The recovery relay incorrectly names private router port
8093 while the retained fleet listens on 8080. Direct worker and private 8080
setup requests returned 200; the relay returned 502. Both public origins were then
503 and coordinator writers were stopped. The original failed recipe/baselines
remain immutable; a reviewed relay correction and guarded predecessor authority
restart must precede the full HTTP/SQLite/HTTPS proof. This is not a completed
v11 deployment or a passing rollback timing qualification.

At 03:31 UTC the full v3 journal committed genesis through 3500738 and its
independent ingestion guard passed. The v11 publication then completed at
05:14 UTC under the original PID 1876447, staged operations `6e8337fa` and frozen
fat-LTO `12ce1291`. All three native stages exited 0; all artifact checks and
four journal rebuilds passed. The immutable map is
`34e3ebe3510206617460cebc87528e56f01949b971797e6f17cc8ca958f23f5d`:
82 archive and eight recent shards, 353,831,243 events, 33,451,520,000 allocated
bytes. The [terminal evidence](../evidence/activity-metadata-2026-09-30/full-publication-terminal.json)
binds every raw report. The process exited with zero restarts and an empty
cgroup; RemainAfterExit keeps the unit active and the older immutable owner
record still says running. Those are retained observations, not an active job.
Last health: 81.88% available memory, 60.24% candidate disk, 22.91% chain disk and
30.14% root disk. All 180 actual table-segment certificates subsequently passed
the approved floors (83 bits for archive pages, 128 otherwise); the minimum
measured result was 95 bits. The [certificate evidence](../evidence/activity-metadata-2026-09-30/full-native-certificates-801d7a62.json)
binds the terminal owner and raw report. Agreement with installed setup/public
hashes remains open. The independent chain oracle then matched all 52,550 events
across 17 sampled blocks, including the three densest, genesis and the fixed
anchor. Raw verbose transactions covered Sprout, Sapling and Ironwood; no
Orchard component appeared in this sample. Four batch-too-large refusals were
retained before exact smaller retries. The [chain evidence](../evidence/activity-metadata-2026-09-30/full-chain-oracle-801d7a62.json)
records sampling limits and raw provenance. No schema cutover occurred.

Actual immutable input preparation produced the complete native assignment and
reviewed units on the coordinator. The first recent-worker copy received all
candidate bytes but native verification failed with SIGILL: the coordinator's
native-CPU build uses instructions absent on the AVX2 recent hosts. The wrapper
reconciled both failed owners and retained abandoned bytes. Live units and
routing remain v10. A separately checksummed portable worker pair from successful
comprehensive CI 36819961986 replaces candidate server/control selection; native
source inputs match the retained publication tools. See
[portable artifact evidence](../evidence/activity-metadata-2026-09-30/portable-worker-release.json).
The portable server ran on the recent worker; verification then refused the
auxiliary `.inputs` directory as a shard without a manifest. Both failed owners
were reconciled preserving partial data. The corrected flat candidate layout
passed fresh native verification on both recent workers and the archive owner.
The [recent](../evidence/activity-metadata-2026-09-30/recent-worker-inputs-801d7a62.json)
and [archive](../evidence/activity-metadata-2026-09-30/archive-worker-inputs-801d7a62.json)
receipts retain every command, source and input identity. These are candidate
files; live binaries, units and routing remain v10. All three current workers
were independently observed warm, without preparation, candidates or invalidation.
Three actual checksum-bound worker host templates now bind refreshed machine,
old active/map/cache sentinels and staged flat inputs. Refreshed plans retain the
whole predecessor publication namespace and recent static sets. The twelve
[coordinator inputs](../evidence/activity-metadata-2026-09-30/coordinator-service-inputs-1d3372ee.json)
are staged and reverified. The
[candidate/predecessor samples](../evidence/activity-metadata-2026-09-30/cutover-recovery-samples-1d3372ee.json)
are nonempty journal-derived fixtures; real HTTP/SQLite completeness remains open.
Actual plan rendering exposed and fixed the full fixture reader bound, publication
control-record backup, filter quiescence and packaged router unit capture. The
[focused checks](../evidence/activity-metadata-2026-09-30/ready-cutover-inputs-focused.json)
also cover a closed proof/specification input staging path. SSH descendant
qualification, full reviewed recipe/preflight and live transition still precede
production validation. No coherent baseline has been captured.

A bounded wrapper qualification now exercises parent exit, inherited descriptors
and interrupted-owner reconciliation without touching live services. Its local
process tests cover remote-parent and relay survival. Actual SSH qualification
reproduced that OpenSSH closes inherited descriptors, losing the coordinator
lock after the relay exited. Both failed owners were explicitly reconciled;
no service changed. A separate timeout-safe transport keeper now retains the
lock outside SSH. The corrected real gate then passed on both recent workers,
the archive worker and the router at `ccbfd35f`, including both lock refusals,
interrupted-owner fences and reconciliation. [Retained evidence](../evidence/activity-metadata-2026-09-30/ssh-lock-qualified-ccbfd35f.json)
binds each actual process and request. Matching immutable coordinator inputs are
also [staged and reverified](../evidence/activity-metadata-2026-09-30/ready-coordinator-inputs-ccbfd35f.json).
The user subsequently authorized proceeding without waiting for queued operations
CI. The prepared proof retains successful full CI at `801d7a62` with its actual
head and all 12 jobs, explicitly records the current candidate CI as pending,
and verifies unchanged native inputs. The five-host specification and complete
740-second rollback recipe were staged. Service preflight refused before any
baseline or maintenance; its detailed error was discarded. A subsequent plain
Python diagnostic invocation exposed bytecode writes inside immutable operation
sources; that diagnostic failure does not establish the original failure cause. The deployment entry point now disables
bytecode before imports; a real subprocess regression reproduces 14 cache files
before the fix and zero afterward. The failed source is retained and a fresh
source export is required. See the [failed preflight and correction](../evidence/activity-metadata-2026-09-30/accelerated-cutover-preflight-ccbfd35f.json).
A later corrected five-host preflight passed, including native verification on
all three workers, after refreshing collected active-map sentinels and retaining
the two recent workers' actual static assignment files. The followup now binds
all 180 measured setup hashes and refuses authority startup until every assigned
installed setup's bytes and identity agree, including both recent replicas.
Recovery samples also require complete predecessor/candidate executable pins.
The [focused evidence](../evidence/activity-metadata-2026-09-30/installed-setup-binding-focused.json)
is source evidence; installed agreement and the rebound live recipe still need
execution. Canonical service remains v10; guarded deployment and all final
qualification remain open.

October 1 input preparation review found and reproduced a raw-file/protocol
map identity mismatch. The corrected protocol serializer matches the retained
native verifier's actual full-map digest (`fd4dcadb...`), separate from the
immutable file (`34e3ebe3...`). Worker plans now bind both identities and seeded
activation uses the protocol identity. Concrete coordinator preparation renders
native assignments and separate v11 worker units through the wrapper. Read-only
fleet inspection found three active workers with no fragment drop-ins and
existing cache budgets 5/5/48 GiB. The reviewed prepared bundle and worker transfer
still require plan/preflight and staged exact operations sources. Production stays
v10; installed certificate agreement, canonical recovery and qualification remain open.

October 1 routing follow-up: concrete withdrawal/private relay/reopen programs
now require all workers and independently accepted anchors, checksum-bound
manifests, unchanged sealed history, nonempty real HTTP recovery and independent
SQLite reopen. Canonical HTTPS recovery follows reopening and failures withdraw
both origins again. Legacy metadata remains unavailable; unresolved spends and
missing classes cannot pass as complete. Fixture tests passed; no live routing
change, baseline or schema transaction occurred. Coordinated product phases now
provide transaction-bound templates, pinned remote owners/locks, complete v11
activation/fleet seeding, filter/load/scaler alignment, an installed mount
namespace probe and original-router restoration during rollback. Original unit
states are retained before the first stop. Lost replies/exit 75 require
reconciliation rather than automatic recovery. The exact live plans, immutable
worker inputs, SSH descendant qualification and production execution remain open.

At 04:38 UTC comprehensive main CI 36812153673 at `beffffc6` completed
SUCCESS, including every lint/test/helper gate and CPU/CUDA artifact preparation.
The native release remains frozen at `12ce1291`; later operations inputs require
their own focused and exact-head CI evidence before deployment.

The frozen prototype observation ended at 03:41 UTC and was independently
checked against its terminal unit and owner at 04:42 UTC: 107,740 exact queries
in six hours, 4.988 QPS, zero logical/transport failures, p99 29.81 ms and five
missed slots. It is inactive with exit 0 and no restarts. This does not satisfy
canonical freshness/300-block or whole-wallet capacity qualification.

At 03:26 UTC backfill reached 3,440,000 blocks and 352,186,053 events. Its guard
reported 87.73% available memory, 72.77% candidate-volume disk, 22.91% chain disk
and 29.76% root disk with zero restarts. Comprehensive CI 36807464393 at
`26245bd0` passed Transparent lint/tests, shared and selected helper checks;
Enhance tests remain active and the aggregate is pending. Superseded
36804203576 is confirmed cancelled, which is not passing evidence.



October 1 host-transition follow-up: concrete product host programs now stop
writer units, require empty cgroups, capture complete v10 mutable state, install
separate v11 unit/config/cache namespaces and restore warm predecessor state
while routing, load and scaler remain deferred. Twenty new focused tests passed;
59 combined schema/staging/preparation/host tests passed. This is fixture evidence.
Reviewed host plans, pinned remote owners/descendant qualification and the
complete coordinated cutover/reopen recipe remain unfinished; no live baseline,
worker binary stage or service transition was applied.

At 02:25 UTC the owned v3 backfill reached 2,300,000 blocks and 322,428,603 events.
Its guard at 02:38 UTC reported 87.56% available memory, 73.84% candidate-volume
disk, 22.91% chain disk and 30.16% root disk, with zero restarts. Ingestion, its
independent guard and the frozen prototype observation retain their original
PIDs and remain active. The quality supervisor remains stopped (failed state,
MainPID 0). Exact-head comprehensive CI 36804203576 at `6e8337fa` remains queued;
no aggregate pass is inferred. Canonical publication and load remain v10.


October 1 follow-up: the rollback dependency now copies private mutable baseline
bytes independently, preserves ownership/modes and rejects partial or corrupted
snapshots before restoration. Deployment subprocesses preserve inherited lock
FDs; a red/green descriptor regression and 19 recovery/process tests passed,
including a surviving-grandchild lock test. This is fixture evidence, with no
production baseline capture or schema transaction. Complete trusted service
phases and host plans remain open. At 01:02 UTC the owned backfill reached
1,470,000 blocks and 271,661,352 events; the guard reported 88% available memory,
76% candidate-volume disk, 23% chain disk, 30% root disk and zero restarts.
Current main comprehensive run 36797707889 remains queued; it is not passing
evidence. Native artifacts remain frozen at `12ce1291` and canonical service v10.

At 01:45 UTC backfill reached 1,880,000 blocks and 301,822,008 events. The guard
reported 88% available memory, 75% candidate-volume disk, 23% chain disk and
30% root disk with zero restarts. The repaired comprehensive run 36795213620
passed at `aa16dc24`, including CPU and CUDA artifact preparation. Later main
run 36800108126 has active Enhance tests and remains pending; 9212's checks and
CUDA artifact preparation passed while its CPU preparation remains queued.
No aggregate pass is inferred for those pending runs.

Full-publication preparation is now implemented through the deployment wrapper,
with a fixed job that refuses unfinished ingestion or any existing owner/output,
rechecks fat-LTO identities, retains stage attempts and enforces 20% resource
floors. Publisher config/state and worker persistence support separate v11 paths;
worker transfers verify both retained binary hashes before loading. Focused
fixture checks passed. This job has not started, the operations-only source at `6e8337fa` has been staged and all 275 retained
files reverified, and the complete trusted cutover/recovery
phases and reviewed recipe remain open.


A separate v3/v11 candidate on the coordinator recovered 26,868 events from
1,000 real blocks, matching the independent block/prevout oracle. Frozen recent
and archive geometries passed 5 QPS for two minutes and 20 QPS for ten minutes:
596 and 11,965 exact queries, zero failed attempts, and p99 22 ms and 31 ms.
All 61 retained SQLite recoveries at concurrency 1/4/8 matched fixture events
and independently recomputed metadata and summaries after reopen. These brief
loopback runs establish the prototype milestone, not canonical HTTPS or sustained
whole-wallet capacity. [Retained evidence](../evidence/activity-metadata-2026-09-30/README.md).

Genesis ingestion is running separately under
`transparent-activity-full-ingest-release-a1c4b809`, fixed anchor 3500738, on the
new 250 GiB volume. At 22:14 UTC the controlled release-binary handoff retained
checkpoint 324000 and 145,862,121 events. By 22:56 UTC the durable checkpoint
reached 350000 with 167,594,544 events. At 22:23 UTC its health guard observed
88% available host memory, 82% available candidate-volume disk, 23% chain-disk
headroom and 30% root-disk headroom, with no ingest restart. Canonical origins
remain v10.

The library metadata, summaries, bounded HTTP adapter and headless shadow SQLite
harness are in [PR 77](https://github.com/zakura-core/wallet-libraries/pull/77).
Its full CI passed at `deac2aed495ee3ca6f4f68bdaff044cb17583a88`, including all
feature configurations and separate facade modes. Real HTTP recovered 9 receives
and 7 spends, independently matched to retained raw block/prevout inputs, with
identical SQLite reopen and no qualification or activation. These controlled
public-script fixtures do not establish financial ownership or shielded recovery.
The follow-up review fixed local mixed-send fee suppression and crash-before-ack
withdrawal reconciliation loss. All 29 history and six adapter tests passed.
Full CI passed at `cc6656f23`; PR 77 merged to main as `9dabaa68` at
22:52 UTC, with an identical tree. Wallet-pir store lint repairs are on main at
`a65c618e`; fast CI and the 16-artifact fat-LTO build passed. Comprehensive CI
found two additional shard-cache sort lints. The complete transparent package
group then passed strict all-target/all-feature Clippy after equivalent filter
and test-only corrections; the affected check passed in 39.55 seconds. The
final `12ce1291` fat-LTO build subsequently passed in 329.79 seconds with all
18 retained artifact hashes verified. A read-only worker plan/preflight through
the deployment wrapper passed for all three workers; none has the release
staged. Current worker disk headroom was 94%, 94% and 66%. At 00:09 UTC
on October 1 all comprehensive check jobs passed at `12ce1291`, while CUDA
artifact preparation failed on a toolchain-selection guard and CPU artifact
preparation was still active. The CUDA job now explicitly selects its recorded
Rust 1.91 compiler; its regression failed before the repair and passed after it.
The aggregate run is not passing evidence. Complete publication and the locked
schema cutover remain open.

The wrapper now implements a journaled coordinator-only schema recipe boundary,
with input hashes, inherited production lock descriptors, durable phase intent
and bounded recovery. Seventeen failure/recovery tests and 45 existing shared
deployment tests passed. Actual production phase programs and a reviewed recipe
remain incomplete. Wrapper-mediated immutable source staging now has exact
archive/commit checks, bounded extraction and a single root process holding the
production lock throughout receipt and extraction. All 30 schema/staging tests
passed, including a real lock-contention fixture. Those focused tests made no
production changes. Subsequently the wrapper staged the 536659-byte reviewed
operations export at `56b67ba0` in 4.52 seconds. A second status check verified
all 269 retained files, and the staged wrapper ran on the coordinator with a
pinned inventory supplied through `/dev/stdin`. No schema transaction exists.
The client now rejects oversized archives before hashing or SSH, and future
root staging receipts capture the process PID; 31 combined tests passed. At 00:38 UTC backfill reached 1210000.
At 00:39 its guard observed 88%
available host memory, 77% candidate-volume disk, 23% chain-disk and 30% root-disk
headroom with zero ingest restarts. The three owned jobs stayed active and the
quality supervisor stayed inactive.

A one-hour candidate 5 QPS observation passed 17,920 exact queries with no
failures, p50 5 ms and p99 33 ms. The next bounded observation runs under
`transparent-activity-observe-5qps-3cfbc484-2`; frozen-table observation does not
count toward canonical continuous publication qualification. The 16-artifact
fat-LTO build at `3cfbc484` passed. A block-local, strictly bounded parent-output
cache at `a1c4b809` passed focused checks and reproduced an uncached 100-block,
83,730-event journal byte for byte. The independent early-chain oracle matched
all events, and its 16-artifact fat-LTO build passed. Ingestion now uses that
immutable binary after a controlled checkpoint handoff. The complete publication,
canonical SSH cutover and final sustained qualification remain open. The
previously paused quality supervisor stays inactive and new quality alerts stay
in shadow.

## Recent replicas on `6360f0d8`, 2026-09-30

- 04:46–04:48 UTC: recent-01, recent-02 and the elastic `transparent-pir-recent-08`
  (added at 04:41 so two replicas stayed routed during each restart) were rolled
  without maintenance to worker `291cd504` (`6360f0d8`, shared `pir-native`
  crate, `incarnation`/`started_unix` in `/v1/ready`). The archive owner still
  runs `a704616c`. A 10-minute 20 QPS gate on three routed replicas returned
  12,528/12,528 exact, p99 48 ms
  ([evidence](../evidence/shared-native-rollout-2026-09-30/README.md)).
- recent-08 remains until the scaler, back in `act`, removes it once the daily
  destroy budget allows.
- The APM host sampler now derives worker targets from the roster; it had
  sampled archive-03 under the name recent-03.

## Single archive owner, 2026-09-29

- 16:24 UTC: the archive (shards 0–76) moved from `transparent-pir-archive-01`
  and `-02` (`0ece0ae1`) to one owner, `transparent-pir-archive-03`
  (`m-8vcpu-64gb`, worker `a704616c`), through `repartition` at a publication
  boundary. Twelve synthetic archive queries failed in the twelve seconds after
  the switch while the router could not yet dial the new owner (a reused VPC
  address); no row was wrong and the recent tier was unaffected.
- The same combined 20 QPS measurement before and after passed on both
  topologies: mixed 20.9 QPS, p99 50 ms before and 45 ms after; archive-only
  19.5–19.6 QPS, p99 33 ms before and 35 ms after, every query exact
  ([evidence](../evidence/archive-consolidation-2026-09-29/README.md)). The single owner used 1.4 of 8 cores and 34 GiB.
- archive-01 and archive-02 were retired in the inventory, stopped and destroyed
  through a checked saved plan after the owner confirmed (2026-09-30); the
  archive now has one copy and `restore` is no longer possible.

## Elastic recent tier, 2026-09-29

- 13:13 UTC: the fleet inventory owns membership; archive owners are pinned to
  0–38 and 39–76 and the first inventory plans matched the live worker rows.
- 13:3x UTC: recent replicas run worker `a704616c`, which builds runtimes in a
  dedicated low-priority pool. Two recent replicas passed the 20 QPS gate at
  20.9 QPS (p99 53 ms, 12,519 exact); recent-03 and recent-04 were retired and
  destroyed through a checked saved plan. The recent tier is two replicas.
- 13:12–13:24 UTC: the actuator created `transparent-pir-recent-05` from the
  elastic root, bootstrapped it with the fleet release, routed it, then drained,
  stopped, retired and destroyed it (droplet 604659753). The continuous 5 QPS
  load stayed exact throughout; it paused for five minutes while the new
  replica booted, fixed since.
- 13:47–14:35 UTC: the scaler ran in `act` under a validation policy. It
  scaled out to three replicas under extra load, replaced a deliberately
  stopped replica make-before-break, and scaled back to two after the load
  ([evidence](../evidence/elastic-recent-validation-2026-09-29/README.md)).
  Two actuator defects found there were fixed (`f76a3172`, `a5c77d40`).
- Since 14:35 UTC the scaler acts under the production policy (two to six
  recent replicas). APM scaling alerts run in shadow. The archive moved to one
  owner on worker `a704616c` at 16:24 (above).

See the [evidence](../evidence/recent-floor-2026-09-29/README.md).

## Replica membership fix, 2026-09-29

Before 11:20 UTC only one recent replica served each publication: over the two
preceding hours 91 of 93 publications activated exactly one (recent-04),
recent-01 rejoined routing 92 times, and recent-02/03 sat warm on older digests.
Only recent-01 was managed, the unmanaged catch-up loop was disabled, and a
publisher redeploy had dropped status forwarding from `fleet.json`, so each
status read opened a new SSH connection and often timed out.

Deployed over SSH at the owner's request, without CI or soak:

- 11:20 UTC, fleet adapter `e0b19e91`: every member managed, probes off the
  routing lock, failure hysteresis, write-once plans, `membership.json`; status
  forwarding re-enabled.
- 11:22 UTC, `prepare_grace_seconds: 3.5` (`89ceb3ee`): 17 of the next 20
  publications activated all four recent replicas, with no status transport
  failures. Freshness was 12–16 s.
- 12:02 UTC, publish controller and `shard-assign` from `dbeb3960`:
  `ready_replicas` follows the routed recent count.
- 12:03–12:05 UTC, recent replicas rolled one at a time to worker `dbeb3960`
  (slot metrics, assignment guard) without maintenance; each was routed again
  20–48 s after its roll began. Archive owners still run `0ece0ae1` and need a
  maintenance window.

The 24-hour 4-of-4 routing gate in [remaining work](remaining-work.md) is open.
See the [evidence](../evidence/replica-membership-2026-09-29/README.md).

## Service-quality rollout, 2026-09-29

The separate [Transparent APM page](https://enhance-pir.valargroup.dev/apm/transparent/)
is live, with seven-day aggregate history shared with Enhance and Status quality
views. The dedicated monitor now runs native Status and Transparent exact-query
checks; initial positive checks and wrong-pin/wrong-answer negative controls
passed. This does not establish whole-wallet availability or a capacity milestone.

The publisher and `transparent-pir-recent-01` have the new HTTP instrumentation.
The remaining five workers are still predecessors while a fresh six-hour / 300-block
canary gate runs. The supervised rollout may advance only after that gate, then
observes the full fleet for 24 hours. Continuous 5 QPS load pauses during upgrades
and resumes only after exact-query verification and per-worker identity checks.
A critical load latch prevents progression and is never cleared automatically.

The first observer attempt failed because its historical default shard no longer
exists in v10. The observer now selects from the current worker assignment; the
replacement observation starts fresh and does not reuse that failed interval.
Existing alerts remain active. New quality/probe alert families remain in shadow;
24-hour shadow review, notification-path validation, activation and 24-hour active
observation are still outstanding. See the [rollout evidence](../../enhance/evidence/service-quality-2026-09-29/README.md)
and [operations guide](../../enhance/docs/observability-alerting.md).

## Continuous query load, 2026-09-29

At the user's request, `transparent-5qps-continuous.service` was enabled on the
coordinator. It targets five fresh encrypted PIR queries per second through the
public origin, split 80% recent / 20% archive and equally between directory/page
tables. Each decoded row is checked against an independent published-plaintext
hash. The workload rotates across all 85 sealed shards; the moving tail is
monitored separately. This is a query-serving workload, not five wallet syncs/s.

The service retains query and health logs and samples workers, router, coordinator
and publication state about every 15 seconds. Its watchdog pauses admission on
sustained failures, and critical incidents remain latched for investigation.
The [initial evidence](../evidence/continuous-5qps-2026-09-29/README.md) records
observed rates, correctness and resources; the process continues beyond that
snapshot. It does not establish a sustained-capacity milestone or guarantee
future health. At that initial snapshot, production service binaries were at `8e69ea75`; the later telemetry rollout is described above.

## Compact layout production cutover, 2026-09-28

Schema v10 is deployed on all six workers at runtime source `8e69ea75`, with
operations overlays from `4905d3a3`. Direct SSH activation and public verification
completed at **22:17:04 UTC**; long-running CI was bypassed at the user's request.
The version-2 event journal is unchanged. The new publication has 77 archive and
9 recent shards; the same pinned history uses **38.79% fewer allocated table
bytes / 63.36% more capacity** than v9. This is a storage result, not throughput.

Both public origins agreed, all six workers were warm on the expected binary,
170 sealed public setup digests matched their certificates, and the served hash
matched the node. The public native regression passed **11 cases / 68 checkpoints**
with **2,319 HTTP attempts and no failed attempts**. A one-client smoke completed
**124 exact synthetic range syncs**, zero failures. Four-client load finished with
**47 attempts, 46 exact completions, zero failures and one query-budget incomplete**.
Minimum sampled available memory was 73.84%,
maximum reported freshness 23.249 s, and no OOM or automatic restart was observed.
An independent init probe returned one transient 503 across publication; it
recovered at the next five-second sample while both maps stayed consistent.
Raw load and resource observations are recorded in the
[cutover evidence](../evidence/v10-cutover-2026-09-28/README.md).

The successful maintenance interval was **22:01:18–22:17:04**. An earlier backup
attempt failed on transient Unix sockets and automatically resumed v9 before any
worker binary changed; the corrected backup preserves durable files and excludes
sockets. V9 publications, state, binaries and caches remain separate for rollback.
A post-cutover rollback rehearsal is not claimed.

The reference SQLite store migrates to schema 3 and preserves compact-fragment
validation through restart; native tests cover the fragment-boundary outpoint
case. Existing v9 clients fail closed on v10, and pending work needs the new
publication lineage. This rollout does not qualify downstream wallet applications.
All 172 actual-table native certificates pass the configured floor; seven archive
page segments are below 128 correctness bits (minimum 96, accepted floor 83).
These are conditional decryption-failure bounds, not security levels. Full
publication took 80 min 23 s and peaked at 4.45 GiB RSS. See the evidence for
commands, raw failures, exact hashes, limitations and the retained rollback paths.
[Remaining work](remaining-work.md#schema-v10-qualification-and-republication)
keeps consumer, sustained-capacity and controlled-comparison gates open.

## Current milestone evidence

| Milestone | Result and evidence | Scope and limits |
|---|---|---|
| M0 — Baseline | [Accepted September 9](../evidence/productionize-m0-2026-09-09/README.md): source, native/UI capability and checkpoint-write inventory | The later M1 record resolves the baseline's fleet unknowns |
| M1 — Fleet | [Accepted six-hour window](../evidence/productionize-m1-six-hour-acceptance-2026-09-13/README.md), reconciled with Roman's updated requirement | Matching loaded canary, corrected rollout and six-hour observation; later freshness failure remains an M5 incident |
| M2 — macOS recovery | [Accepted September 9](../evidence/productionize-m2-2026-09-09/README.md), wallet source `0ce6d158f` | Real native recovery-only application, isolated profile, sending disabled; not a signed distribution or beta acceptance |
| M3 — Correctness | [Fixture validation](../evidence/productionize-m3-2026-09-10/README.md) at wallet `7937d48df`; [real-wallet native recovery and resume](../evidence/productionize-m3-private-wallet-2026-09-13/README.md) compare exactly with independent reduction | Release-app UI and stronger live interruption evidence remain outstanding; public regression passed as recorded below |
| M4 — Whole-wallet benefit | [80 exact paired recoveries](../evidence/block-comparison-2026-09-09/README.md) give a derived 99.19% reduction in additional transparent payload | Representation-size comparison is not whole-wallet incremental latency or capacity |
| M5 — Capacity and recovery | Historical [fleet series](../evidence/runs/fleet-series-2026-09-08-r1/README.md) and M1 observations exist | No accepted sustained beta operating envelope or complete failure-recovery rehearsal |
| M6 — Release and beta | No acceptance evidence | Review, versioned distribution and tester observation follow M3–M5 |

## Release rollout, 2026-09-27

Observed by the operator (Claude, for Roman) on 2026-09-27; times UTC.

**Public metadata outage, 14:18–18:55.** The Enhance deploy re-rendered the
coordinator Caddyfile from `ops/deploy/coordinator/Caddyfile`, which lacked the
continuous-publication route. `/v1/shards`, `/v1/shards/init` and manifests
returned 404, and `/v1/filters/shards` served the stale static map. The route
was restored by hand at 18:55 and added to the template in `2c658d43`.

**Worker and publisher release.**
- Workers were on source `a5f79ed` (binary `200ca850…`, ipir-sp `accc424`).
- `9d47b05b` passed full CI and was rolled out with `deploy-transparent-publisher.yml`
  in shadow mode (recent-01, archive-01, archive-02, recent-02..04, 22:26–22:40)
  and activated at 22:40:42. All six workers now report binary `c735a6b3…` and
  are warm with every runtime. The publisher serves source `9d47b05b`, with
  directory choice tables **off**.
- Before the rollout:
  - A local test restored a runtime cache written by the `a5f79ed` worker in the
    `9d47b05b` worker. All 28 runtimes restored, and 52/52 syncs were exact.
  - A rc.6 client synced against the old fleet with no transport failures.
- After activation, 17 public syncs completed with no failures. Archive restores
  were 6/6 exact. The recent mismatches are consistent with activity since the
  sample's 3,473,686 anchor: 30/120 restore-6m and 32/120 catch-up-30d sample
  clients have later events.

**Failed first attempt, 22:20–22:25.**
- The shadow run failed before touching workers. The rotated
  `WALLET_PIR_DEPLOY_SSH_KEY` (`enhance-pir-deploy`) was not authorized on any
  transparent host.
- By then the script had already stopped the controller and overwritten its
  working credentials, so public metadata returned 502 until the previous
  controller config was restored by hand.
- The rotated key's public half was then appended to `authorized_keys` on the
  router and all six workers; each host keeps an
  `authorized_keys.before-deploy-key-2026-09-27` backup.
- The deploy script now:
  - checks every host accepts the identity before stopping anything;
  - keeps the previous controller config;
  - restores the controller if it fails before any worker changes.

Shadow mode itself withdraws public metadata while workers upgrade, here about
14 minutes, as the maintenance path does.

**Directory choice tables enabled, 22:43.**
- `directory_choice: "all"` was added to `/opt/transparent-publisher/controller.json`
  (backup `controller.json.before-directory-choice`) and the controller restarted.
- The first new tail revision (shard 175, revision 1172, height 3,498,503)
  carries a 4,321-byte table for 27,989 scripts. Workers loaded it, and loading
  verifies every entry's route.
- 25 public syncs against it completed with no failures.
- Existing sealed shards keep their table-free manifests until a full
  republication. Wallets built before the field cannot read tabled manifests;
  none is shipped.
- To revert: remove the field and restart the controller. The next tail
  revision is then published without a table.

## Router health and worker drain rollout, 2026-09-28

Times UTC.

- **Rollout.** `3317cd01` (C1 router policy, worker upload drain, A3 client
  concurrency) went out in shadow mode 10:45–11:00 and was activated at 11:12
  with `directory_choice: all`. All six workers now report binary `3e9bae97…`,
  warm with every runtime.
- **Activation failure.** Activation missed its 180 s deadline. The new router
  policy used `health_fails` and `handle_errors 502 503 504`, which the router's
  **Caddy 2.6.2** rejects, so every route attempt failed validation. Public
  metadata returned 503 from the start of the shadow rollout until 11:16.
- **Recovery.**
  - The live `/opt/transparent-publisher/transparent-live-fleet.py` was patched
    to 2.6-compatible directives (original kept as `…3317cd01-orig`). The
    controller then routed and served at 11:16.
  - The long-running replica reconciler still ran the fleet code it had loaded
    before the upgrade, and re-routed with the old policy at 11:16:18. It was
    restarted, and the controller's next activation installed the new policy
    at 11:19:10.
  - Both renderers were fixed in `24b988db`, with an ops test against newer
    directives, and activation now restarts the reconciler. On 2.6.2, a bare
    `respond` inside `handle_errors` keeps the proxy's status (verified 503
    with `Retry-After`).

## M1 accepted observation

The matching loaded canary ran for more than six hours and 300 blocks with
336,603 exact private responses. The corrected six-worker upgrade passed at
September 11 22:10:59 UTC. The replacement fleet observation's first six hours,
September 12 approximately 00:49:59–06:49:59 UTC, recorded **300 public and 300
replica blocks on each of six workers**, maximum public visibility **20.080 s**,
maximum replica visibility **22.915 s**, minimum available host memory **24.387%**,
and no OOM or service restart. Roman confirmed acceptance under the revised
six-hour requirement on September 13.

Worker source `a5f79ed`, binary
`200ca85065c8096d344749d5e51a2db569ec369c71bd2ffff8cf0e9fd014ff62`,
operations `0e2c003`, router and monitor identities, query logs and all worker
samples are retained in the [acceptance bundle](../evidence/productionize-m1-six-hour-acceptance-2026-09-13/README.md).
The fleet comprises four recent replicas and two archive owners in Amsterdam;
this observation does not measure its sustainable wallet throughput.

The supervisor still requested 24 hours. At September 12 08:05:01 UTC, recent-01
failed replica freshness at block 3,480,596 after **60.715 s** against its 60 s
budget; the other monitors were cancelled. That command remains failed. The
six-hour decision is a retrospective operator acceptance of the completed window,
not a rewritten terminal result. The later failure must receive an M5 disposition.
Earlier failed canaries and their corrections remain in the
[evidence ledger](../evidence/README.md); they earn no additional acceptance credit.

## M3 public regression and HTTP correction

The [first September 13 deployed run](../evidence/productionize-m3-live-2026-09-13/README.md)
attempted all eleven cases: six passed, five failed on transient public-path
transport or availability. It found no ledger mismatch in completed comparisons.
The repaired runner explicitly uses up to three attempts for eligible HTTP
failures, with one total deadline per logical request, exact replay of buffered
requests and a record of every failed and successful attempt. Identity checks,
fixture contents, accepted-anchor comparisons and case deadlines are unchanged.
See [testing](testing.md) for the policy and its limits.

The [frozen-executable repaired run](../evidence/productionize-m3-suite-fix-2026-09-13/README.md)
passed **all eleven cases and 68 checkpoints**. Its 4,320 logical requests used
4,324 attempts: 4 failed attempts, 4 recovered requests, zero terminal request
failures and no ledger mismatch. The source/binary manifests, full checks and
per-attempt evidence are retained. An earlier all-pass repaired run had its
build output replaced by concurrent checks; it remains diagnostic evidence.

The wallet release candidate still pins client `22e6bec` and does not inherit the
suite's explicit retry configuration. Suite correctness under bounded retries
is not proof of uninterrupted availability or a deployed wallet change.

## M3 native wallet and application boundary

The M3 wallet source `7937d48df` extends M2 with independently reduced fixture
histories, reorgs and forks, persistence interruption tests, imported-script and
unsupported-script handling, and shadow comparison. Its fixture-loopback kills
are separate from the real public-service trial.

For the user-supplied recent-history wallet, the actual release bridge (sending
and fixture-loopback disabled) recovered into a new isolated profile from
birthday **3,480,000**, a conservatively earlier height than the requested
September 12 date. Recovery completed at **3,482,317** with no pending pages or
unresolved spends. Independent compact-block reduction matched exact events,
UTXOs and balance. A second fresh profile was killed during the reported
transparent sync phase, reopened as interrupted, resumed to **3,482,322** in
**50.853 s**, and again matched independent reduction. The first profile stayed
unchanged. [Sanitized evidence and reproducible harness](../evidence/productionize-m3-private-wallet-2026-09-13/README.md).

This exercised native code and Dart bindings, not the release application's
screens. The kill was phase-observed, not directly proven to occur during an
HTTP request, and the interrupted profile had no committed transparent anchor.
The oracle obtains watched scripts from wallet discovery and trusts complete
transparent compact-block data; it is not an independent proof of discovery or
index completeness. No mnemonic, private ledger or raw wallet log is published.

## Implementation and prior deployment


| Capability | Observed state |
|---|---|
| Shard schema | **Live: `transparent-shard-v9` since 2026-09-28 16:31 UTC** (4,096-byte rows, 21 directory slots, 46 events per page, 14-byte salted tags, 87-byte events), `zcash-transparent-range-v2`, `archive-wide` + `recent-4k-8k`, choice tables on every shard, published from the version-2 journal. Cutover, one automatic rollback, 60-minute metadata window and regression 11/11 are in the [cutover evidence](../evidence/v9-cutover-2026-09-28/README.md). A [pinned-anchor load run](../evidence/v9-cutover-2026-09-28/load/README.md) against it was 45/45 exact, matching the v7 baseline, with 48–75% fewer bytes per sync in eight of nine classes (4 clients, one run; not a capacity result). Wallets built before v9 cannot read it. The v7 rollback material (v7 set, v7 publication root, version-1 journal, v7 filter binary, worker v7 sets and active records) was deleted on 2026-09-28 after the cutover; rolling back to v7 now needs a republication |
| Registry | `recent-8k` 8192/8192; `recent-4k` 4096/4096; `archive-32k` 32768/32768; `archive-wide` 32768/65536 |
| Optional recent pairing | `recent-4k-8k` 4096/8192 registered; not published. Real shards and a [placement study](../evidence/directory-placement-4k-2026-09-28/README.md) fit it in one segment with no overflow up to 96% load |
| Single-lookup directory | **Live for new tail and sealed revisions since 2026-09-27 22:43 UTC** (see the release rollout above). Optional manifest `directory_choice`; publisher `--directory-choice off\|sealed\|all` (controller config `directory_choice`, default `off`); the builder verifies every route against the encoded rows, and the server refuses a repeated script tag and checks the entry count (rows carry tags, so it cannot recompute routes); wallet sends one directory query per matched script when present. [Measured locally](../evidence/single-lookup-measure-2026-09-27/README.md): 939/939 exact syncs, directory queries halved, restore-6m payload −24%; a [temporary bench fleet](../evidence/single-lookup-fleet-2026-09-27/README.md) reproduced this at 8 and 32 wallets. wallet-libraries not updated |
| Census ranges | `--start-height`, `--end-height`, `--first-shard-id`, geometry overrides, `--placement`, `--single-lookup`, `--per-shard`, exact script matches exist in `shard-census.rs` |
| Two-tier publisher | `--recent-geometry`, `--archive-geometry`, `--recent-from` exist in `shard-publish.rs` |
| Workflow exposure | `publish-transparent-shards.yml` exposes commit, journal, output directory, anchor, `recent_from` (re-derived and checked) and both geometries; the backfill workflow's `inventory` action records journal identity, cutoff and an independent event spot-check |
| Loading/cache | `ShardSet::open_with` loads the whole set or an assignment's subset: every shard's manifest and filter, tables only for assigned ids, global ids and manifest chain intact; bounded runtime cache and file-backed plaintext sources |
| Continuous publication | Controller, incremental suffix publisher, worker prepare/activate/invalidate control, and shadow/activate deployment workflow implemented and deployed; see the dated rollout below. RPC supplies non-finalized tip blocks; the RocksDB secondary remains for historical backfill. |
| Revision handling | Revision-addressed setup/query, 409 refresh, retryable cache pressure, bounded wallet refresh exist |
| Retention | Three superseded revisions per shard beyond current, with optional byte bound (`--retain-bytes`); live collection preserves the newest three retired snapshots and individually held readers, collects other idle snapshots, and trims unpinned retired runtimes before preparation. Current-only prewarm preserves build headroom |
| Observability | Every metric series labelled with map and assignment digests, worker id and role; cold-build histogram; process RSS and cgroup memory gauges; queue depth and body bytes in flight; unassigned refusals; prewarm progress |
| Query validation | The query route refuses a body whose declared length is not the selected table's exact length before buffering, queueing or building; a missing length is 411; the runtime re-checks length and binding as defence in depth; the global HTTP cap remains the ceiling for the widest geometry |
| Admission | `admission.rs` bounds waiting requests (`--query-waiters`), buffered body bytes (`--body-bytes`), upload time (`--upload-deadline-secs`) and total wait (`--query-deadline-secs`); a full queue, exhausted body budget or expired deadline is 503 with `retry-after`, a stalled upload 408; a dropped connection releases its place and is counted |
| Deployment acceleration | Persistent public runtime snapshots, stable per-worker assignment identity, binary identity in readiness, selective staging/restarts, healthy replica pairs and transaction-scoped rollback implemented; cache/managed-preparation progression is recorded by M1. Broader timing acceptance remains in [remaining work](remaining-work.md). |
| Readiness | `/v1/ready` reports mode, map and assignment digests, warm and target runtime counts; loaded-only mode (`--pilot-cold`, whole-set default) is ready once loaded, warm mode (assignment default) only once every assigned runtime is prewarmed |
| Fleet assignment/router | `transparent-assignment-v1` planned by `shard-assign` from a roster; router is Caddy rendered from the assignment, routing on the shard id in the path only; fleet deploy modes prepare, verify with the staged binary, activate owners then replicas, switch the router, verify publicly and over the VPC, prune, and roll back on failure; filter service has its own deploy workflow. Exercised on the fleet; failure and capacity rehearsals remain separately tracked |
| Wallet manifest binding | `GET /v1/shards/:id/revisions/:digest/manifest` serves canonical manifest bytes; before any private request on a matched shard the wallet recomputes the digest, checks every field against the map entry, the previous entry's digest and the registry, and takes the geometry from the verified manifest |
| Accepted-anchor recovery | `sync_into` and the facade require a wallet-accepted height/hash. Partial-shard coverage retains a distinct source endpoint, events above the target are discarded after validation, and rollback accepts an exact ancestor. SQLite schema 2 migrates legacy progress conservatively. See [tests](testing.md); deployed regression remains a separate gate. |
| Persisted continuation | `sync_into` over a `WalletStore` (reference `MemoryStore`; SQLite in `transparent/crates/transparent-wallet-store`): per-script per-shard coverage with the block hash it rests on, settled or provisional; atomic idempotent shard commits; reorg rollback to the accepted ancestor via the wallet's `ChainView`; provisional tail truncation and promotion; durable pending page work; scripts added by the wallet's rules discovered over their required range; a budget or outage ends a call incomplete with no anchor. `sync()` is a wrapper over a fresh memory store. `TransparentSync` facade takes plain data for a host binding. The store contract suite is exported under the `testing` feature and `parse_init` is available without `reqwest` (`5758ffd`); the Zakura wallet (`valargroup/wallet-libraries`) implements the store over its own database and passes it |

Relevant sources: [store](../../transparent/crates/transparent-wallet/src/store.rs), [publisher](../../transparent/services/transparent-filter-server/src/bin/shard-publish.rs), [census](../../transparent/services/transparent-filter-server/src/bin/shard-census.rs), [service](../../transparent/services/transparent-shard-server/src/service.rs), [runtime](../../transparent/services/transparent-shard-server/src/runtime.rs), [loader](../../transparent/services/transparent-shard-server/src/shardset.rs), [wallet](../../transparent/crates/transparent-wallet/src/sync.rs).


The [September 8 continuous-publication rollout](../evidence/continuous-publication-2026-09-08/README.md)
established mainnet publication from a journal starting at height zero, mixed
archive/recent geometry, both public origins and warm fleet routing. Its exact
174-shard inventory, cutoff, IPs, memory measurements and component versions are
historical snapshots; M1 above is the later accepted operational record.
The [inventory](../evidence/inventory-2026-09-08/README.md),
[census](../evidence/census-2026-09-08/README.md) and
[publication verification](../evidence/publication-2026-09-08/README.md) retain the
dataset and independent spot-check provenance.

The native adapter at `bca43b343`, client `22e6bec`, established accepted-anchor
persistence, exact rollback, atomic balance/coverage and safe old-layout refusal;
its [hardening evidence](https://github.com/valargroup/wallet-pir/blob/42b5f9c145cc3f2a400c428938c566d2cc9699db/docs/transparent-pir/evidence/hardening-2026-09-08/README.md) precedes M2/M3.
M2 closed the measured append amplification, misleading near-tip completion and
demo-fallback/custody findings. Its application artifact was ad-hoc signed and
not distributed through an accepted release channel.

Persistent runtime caches, managed preparation and revision-aware collection
progressed from the [cache canaries](https://github.com/valargroup/wallet-pir/blob/42b5f9c145cc3f2a400c428938c566d2cc9699db/docs/transparent-pir/evidence/deployment-runtime-live-2026-09-08/README.md)
through the [matching M1 rollout](../evidence/productionize-m1-deferred-collection-2026-09-11/README.md).
Those old canary-only configuration notes are not today's rollout checklist.
A sub-ten-minute compatible fleet update and failed-batch rollback still need
separate timing/recovery evidence. The optional
[parent-filter artifact rollout](../evidence/parent-filters-production-2026-09-08/README.md)
passed a bounded canary; its heavy-wallet performance comparison did not finish.

On 2026-10-01, the attempt5 transaction `transparent-schema-20261001T184408Z-a3a74bbb161e-491857`
passed coherent capture, maintenance and staging, then failed candidate prewarm
at 303.431 seconds. Automatic restoration passed, but its 250.862-second cold
readiness check failed. A distinct exact warm-restored repair passed in 81.087
seconds, including private and separate canonical encrypted-query proofs with
reopened SQLite stores. Those cold failures remain failed; this brief repair
does not qualify v11, capacity or cold recovery. The bounded startup allocation
fix has focused evidence; a fresh actual cutover and all final gates remain open.
