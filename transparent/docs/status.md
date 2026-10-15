# Transparent PIR status

Latest retained observations are dated 2026-10-13; milestone acceptance remains separately scoped. The target remains an opt-in, recovery-only macOS beta on
existing infrastructure. **M0, M1 and M2 are accepted; M3 is partially validated;
M4–M6 are open.** This records observed progress, not a new live fleet health
check. [Remaining work](remaining-work.md) is the authoritative outstanding
checklist; [deployment](deployment.md) owns operating targets.

## Prepared history runtimes measured, not accepted, 2026-10-14

Source only; nothing deployed and `ship_runtimes` stays off everywhere. The
[design as built](architecture.md#prepared-history-runtimes): the publication controller
can prepare every history runtime of a candidate on the publisher, link unchanged ones
from retained candidates, and offer them with the candidate; the fleet adapter ships each
worker only its shards' files and links what the worker already holds; the worker loads
and self-checks them through the same cache operation as a restore or build, and falls
back to building per runtime.

- **Implementation:** done on the task branch (see the commit that adds this entry).
- **Correctness:** focused `release-fast` tests pass: exact wallet recovery against an
  independent traversal with no worker build (two page shards per directory shard, and a
  shard needing two page segments); byte-identical setup and answers against a locally
  building worker at populated, empty, first, last and block-boundary rows, each decoded
  to the segment file's row; a file one row off refused by the full database comparison
  whatever row is sampled; every wrong identity (revision, page shard, digest, geometry,
  kind, native schema), truncation, corruption and missing file refused once and built;
  corrupt sources fail; concurrent callers share one load; a refusal counted once across
  admission retries; cancelled loads keep their reservations until their blocking work
  ends; restart and reorg fences; assignment-specific inventories; the controller's real
  path, link reuse, stop on invalidation, refusal of an unverified candidate and reported
  write failures; the fleet adapter's staging.
- **Measured benefit:** [six paired arms](../evidence/history-prepared-runtimes-2026-10-14/README.md)
  on the shared development hub, by Roman's direction, without an exclusive allocation.
  Worker CPU fell 51–55% in every pair (build pool −94%), 144,000 of 144,000 answers were
  exact, and shipping refused nothing and built nothing on the worker. **Not accepted**:
  the worst shipped query p95/p99 (96.9/162.8 ms against 90.7/115.4 ms) and activation-delay
  quantiles (p99 41.2 s against 36.6 s) are worse, every arm of both modes breaks the 30 s
  freshness bound, and every arm is contaminated by other work on the host. The
  publisher's preparation sits on the publication's critical path. `ship_runtimes`
  stays off.
- **Production adoption:** not started; it needs the measurement, a reviewed plan and
  Roman's confirmation. See [remaining work](remaining-work.md#prepared-history-runtimes).

## Wallet API v1 retired, 2026-10-13

`/v1/*` on `https://transparent-pir.valargroup.dev` answers 410 since 17:13:50 UTC.
`/v2/` is the only wallet API ([evidence](../evidence/v1-sunset-2026-10-13/README.md)).
Vizor main pins the `/v2/` client (chainapsis/vizor-wallet#977), and over 30 minutes no
wallet traffic reached the v11 workers beyond our own load.

- **Router.** A shell installed by `transparent-public-router.py` holds one route file
  per version. No fleet adapter writes it any more.
- **Journal.** The v12 controller now writes the shared journal that the retired v11
  controller used to write.
- **Stopped.** The v11 controller, scaler, reconciler, control sessions and 5 QPS
  load; the `txid-2k` controller; and the serving units on recent-01, recent-02,
  archive-03 and txid-display-01. The droplets are kept 48 hours for rollback.
- **Incident.** `/v2` history metadata (`/v2/shards/init`, `/v2/filters/shards`)
  answered 502 from 17:13:49 to about 17:24:45. The v12 unit's sandbox mounted the
  journal read-only, so the controller crash-looped as writer. Queries and `/v2/txid`
  kept serving.
- **Load.** The continuous 5 QPS load runs on `/v2`. The 20 QPS gate passed at 20.99 QPS:
  all 12,573 queries exact, p50 11 ms, p99 38 ms.
- **Open.** Destroying the droplets after 48 hours
  ([remaining work](remaining-work.md#wallet-api-versions-on-one-origin)).

## Monitoring on `/v2`, in shadow, 2026-10-13

pir-apm and pir-monitor watch the v12 history service and the one-query txid
display under `/v2`; v1 is no longer monitored
([evidence](../evidence/observability-v2-2026-10-13/README.md)).

- **APM.** pir-apm `5ad1ef61` on the coordinator since about 04:00 UTC. It shows
  `transparent` (v12 history) and `txid` pages with publication freshness and
  recent-rebuild timing, and `/apm/capacity/` with per-host and per-unit memory,
  growth forecasts and next droplet sizes. All 7 sources were fresh at switch.
  v12 publication cycle p50 19.8 s, p95 29.0 s.
- **Probes.** On `wallet-pir-monitor-01`, the `transparent` probe runs the v12
  canary (233-table fixture, anchor 3,499,356) and a new `txid` probe looks up a
  pinned sealed entry; both pass.
- **Alerts.** Every Transparent and txid family is in shadow; nothing has been
  sent to Slack. Enhance and Status alerting are unchanged and active.
- **Open.** v12 workers lack the prepare histogram until their next rollout;
  capacity forecasts need three days; activation follows a 24-hour shadow review
  ([remaining work](remaining-work.md#transparent-monitoring-on-v2)).

## Txid display `/v2/txid` on `txid-4k8` tables, 2026-10-13

The one-query site serves 4,096-row `txid-4k8` tables since about 02:45 UTC
([evidence](../evidence/txid-display-4k8-2026-10-13/README.md),
[deployment](deployment.md#txid-display-one-query-site-2026-10-12)).
`/v1/txid/` was not touched.

- **Serving.** Release `e1f94158` (the code of `transparent-v0.1.0-rc.3`).
  `/v2/txid/init` publishes `txid-4k8`, 28,160-byte request bodies (28,168 B
  with the binding) and 10,240-byte answers. 94 archives of 180,000–180,787
  entries and a 180,000-entry recent window; root
  `/srv/txid-display-genesis/v4/root`, the `txid-2k8` root kept for rollback.
- **Checks.** 95 of 95 served tables certified at 246 bits or more; five cold
  lookups through the public origin returned their exact entries in about
  1.7 s.
- **Memory.** 96 runtimes of 160.17 MiB: the worker holds 17.0 GB of its 48G
  limit, against 30.0 GB on `txid-2k8`.
- **Downtime.** About six minutes. The bootstrap ran beside the live lineage
  from a hardlinked snapshot of the journal, in 36 minutes.
- **Load.** 20 lookups/s for ten minutes through the public origin, 03:31–03:41
  UTC: 11,999 of 11,999 answers exact, no errors. Warm lookups p50 0.21 s and
  p99 0.77 s, fresh clients p50 1.56 s; the worker used 0.56 cores on average.
- **Wallets.** transparent 0.1.0-rc.3 is on crates.io; wallet-libraries main
  pins it (zakura-core/wallet-libraries#174, #175), and Vizor's v12 branch
  (chainapsis/vizor-wallet#977) pins that main.

## Txid display `/v2/txid` on the 8-bit profile, 2026-10-13

The one-query site serves the display table at the 8-bit profile since
01:10 UTC ([txid display](txid-display.md#lookup-one-row-of-one-table-per-bucket),
[deployment](deployment.md#txid-display-one-query-site-2026-10-12)).
`/v1/txid/` and its `txid-2k` lineage were not touched.

- **Serving.** Release `3e705bb3`, built on a development host with the CI
  release flags (bundle `0c1045df…`). `/v2/txid/init` on
  `https://transparent-pir.valargroup.dev` and on the site hostname publishes
  native schema `transparent-txid-display-p8-v1`, profile
  `reinspiring-two-mask-p8-g27x1-m22-dq28-r10-v1`, 21,000-byte queries and no
  `scheme_dq44`; the setup seed is 392291412234735037. The worker serves map
  `213f30c0…` (tip 3515811) warm.
- **Limits.** The worker unit runs at 48G `MemoryMax` with a 32 GiB cache and
  a 40 GiB disk cache. It held 28.0 GiB (page cache included) once warm,
  against 26.9 GiB planned for 191 runtimes.
- **Order.** `stage`, `stop` (controller stopped, site route withdrawn),
  `workers`, `route`, `controller`. `/v2/txid/` was down from 01:02 to about
  01:10 UTC while the worker rebuilt its runtimes. The 65-minute `verify` was
  not rerun: publication bytes did not change, and it passed at 00:45 UTC.
- **Lookups.** Three cold lookups of a pinned archive entry with the
  `txid-quality-canary` built from `3e705bb3` returned exactly that entry
  through the public origin, in about 1.7 s each.
- **Open.** Certificates of the served tables and the rebuild-time measurement
  ([remaining work](remaining-work.md#8-bit-txid-display-profile)); a deployed
  txid canary must be rebuilt from `3e705bb3` or later; wallet-libraries and
  Vizor repins.

## Wallet API v2 with the 8-bit history profile, 2026-10-12

`https://transparent-pir.valargroup.dev` serves two wallet API versions side by
side ([evidence](../evidence/api-v2-p8-2026-10-12/README.md)):
- `/v1/` is unchanged production: schema v11 history and the txid-2k display.
- `/v2/` is the v12 fleet with its history tables at the 8-bit, one-digit-key
  profile, plus the one-query txid display.

No shipped wallet uses `/v2/` yet. zakura-core/wallet-libraries#165 carries the
client and is merged; chainapsis/vizor-wallet#977 is not.

- **Serving:** all four v12 tables use `reinspiring-two-mask-p8-g27x1-m22-dq28-r10-v1`
  (wallet-pir `f7debf84`, deployed 22:07 UTC). The `/v2/` paths come from `ff27d649`,
  deployed 23:18 UTC, and the public router has routed `/v2/` since 23:31 UTC.
  `/v1/` init and txid answered 200 before and after.
- **Memory:** after the load run, RSS was 35.3 GiB for the archive worker (186 tables)
  and 4.73 GiB for the recent worker (49), 40.1 GiB in all against 31.4 GiB at 16 bits.
  The recent worker's budget rose to 8 GiB of cache and a 10G limit.
- **Regression:** 11 of 11 on the v12 host and again through the public `/v2/` route.
- **Bytes against the same fixture at 16 bits:** uploads 40–46% smaller, table setup
  about 51% larger (30.4 KB), responses 9% smaller. A wallet's PIR bytes fall 24–34%.
  Public membership filters, about 36 MB for a full-history wallet, are unchanged.
- **Load:** 20 wallets for 10 minutes against the v12 host's `/v2/` (01:32 UTC,
  2026-10-13): 2,948 of 2,949 syncs exact at 4.88/s, with no failed requests. As in
  all four 16-bit runs, one reused-tail wallet missed the 600 s deadline. Query means
  matched or improved on 16 bits; directory setup mean rose from 0.38 to 0.44 s.
- **Open:** certificates under the served seeds at the 50-bit
  floor, `/v2/` monitoring, and the move of wallets from `/v1/` to `/v2/`.

## Schema v12 in staging next to v11, 2026-10-12

Branch `v12/decoupled-pages` (PR #150, not merged) carries schema
`transparent-shard-v12`: directory shards with up to 32 page shards, at
`decoupled-2k-8k` (recent) and `decoupled-32k-32k` (archive)
([design](decoupled-pages-design.md), [architecture](architecture.md#identity-and-geometry)).
The publisher, controller (which can follow a journal another process writes),
shard server and wallet chain walk with its run cache are ported; wallet-libraries
follows on its own `v12/decoupled-pages` (draft PR zakura-core/wallet-libraries#156).
It runs in staging beside production v11 at `transparent-pir-v12.valargroup.dev`,
following production's journal read-only
([evidence](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/v12-staging-2026-10-12/README.md)). Production clients
still use v11; the switch and v11's retirement are not done.

- **Tests:** CI on PR #150 green through the staging deployment record; `make
  check-fast` from the branch base green.
  New acceptance tests: block-by-block publication equals a fresh build, sealed page
  and directory shards survive tail growth and reorgs at both boundaries, a history
  across three page shards recovers an exact ledger, and a tip catch-up stops at the
  first cached run.
- **Certificates:** all four v12 tables meet 128 bits at both query widths, lowest
  163 ([evidence](../evidence/v12-native-certificates-2026-10-12/README.md)).
- **8-bit profile trial** on all 241 staging tables (measurement only, nothing served
  changed): upload per table query −11% to −46%, response −9%, worker memory 30.8 GiB
  to 37.1 GiB (two-digit key) or 39.5 GiB (one-digit key), archive cold build +13–16%,
  recent rebuild about twice as long; every table certifies at least 125 bits at
  28-bit queries ([evidence](../evidence/p8-trial-2026-10-12/README.md)).
- **Offline end to end** on the hub's recent journal slice: 716 of 716 covered sample
  wallets recovered exactly over HTTP, bills within about 7% of the census
  ([evidence](../evidence/v12-offline-e2e-2026-10-12/README.md)).
- **Staging** ([evidence](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/v12-staging-2026-10-12/README.md)): archive
  and recent tiers serve from one worker; the recovery regression passed 11 of 11
  against a fixture exported from the live journal; three 10-minute runs of 20
  concurrent wallets each completed about 3,000 syncs at about 5 exact syncs/s.
- **Fixed:** `init` answered 503 for requests spanning a publication switch in
  the first three runs. The controller at `322c4f07`, deployed to v12 only on
  2026-10-12 at 16:29 UTC, serves the last `init` built for the active map
  during a switch; a fourth run crossed 8 switches with no 503
  ([evidence](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/v12-staging-2026-10-12/README.md)).
- **Open:** one reused-tail wallet exceeds the 600 s recovery deadline in every
  run; the v11 query latency gate (p99 under 2 s, p50 under 700 ms at 20 QPS)
  has not been run against v12; the v12 canary fixture is not built.

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

The [frozen-executable repaired run](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/productionize-m3-suite-fix-2026-09-13/README.md)
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
unchanged. [Sanitized evidence and reproducible harness](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/productionize-m3-private-wallet-2026-09-13/README.md).

This exercised native code and Dart bindings, not the release application's
screens. The kill was phase-observed, not directly proven to occur during an
HTTP request, and the interrupted profile had no committed transparent anchor.
The oracle obtains watched scripts from wallet discovery and trusts complete
transparent compact-block data; it is not an independent proof of discovery or
index completeness. No mnemonic, private ledger or raw wallet log is published.


## Earlier observations

The complete dated development and incident history, including failed activity
cutovers and their recovery limits, is preserved in the [pre-cleanup status record](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/docs/status.md). These are historical observations, not a fresh fleet check.
