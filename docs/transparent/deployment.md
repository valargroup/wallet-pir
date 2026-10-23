# Transparent PIR deployment

Accepted target: 2026-09-07. Implement and validate through [remaining work](remaining-work.md). Live state is recorded only in [status](status.md). This supersedes the nine-small-archive-worker plan and the single-global-geometry proposals.

If an unstaged cutover loses its captured publication to predecessor collection,
`schema-reconcile-plan` and `schema-reconcile-preflight` can bind one newer,
already warm v10 map on every original worker to the original transaction and
recipe digest. `schema-reconcile-deploy --expect-plan-sha256 <reviewed-digest>`
retains the displaced coordinator activation records and prior destination
bytes, then runs guarded private HTTP, reopened SQLite, and both canonical HTTPS
proofs before resuming predecessor load. It accepts only the original captured
namespace and complete pinned predecessor fleet; it cannot install v11 or choose
arbitrary source files. A partial adoption refuses replay. Successful newer
revision recovery is recorded as `reconciled-v10`, preserving the original failed
rollback and any missed recovery deadline. It does not satisfy rollback timing
acceptance; a fresh coherent baseline and actual rollback rehearsal remain
required before final qualification.

Fresh schema captures protect each named collector-owned publication generation
with hard links in a private sibling outside the controller and worker collection
roots, on the original filesystem. Completion binds the captured map checksum
and full file inode/size/mode/ownership/mtime inventory. This protects immutable
tables against unlink collection; native table verification remains required.
Rollback validates the entire baseline and protection before reconstructing a
missing generation. Changed generations, links, special files and unfinished
restore directories refuse recovery. Coordinator authority remains stopped until
every restored worker proves the captured exact assignment, then private proof
waits for the guarded predecessor authority endpoint.

Product input staging pins the reviewed generations before the long native
preflight, using the input-preparation owner and each worker's remote owner and
lock. This changes only private retention links. Capture then derives the actual
active generation from the independently copied activation records after writers
are quiet, and protects that generation as well if publication advanced during
preflight. The complete baseline binds those additional generations to the copied
records. An older protected child alone cannot satisfy a worker's captured active
identity. Unfinished pins refuse reuse; unknown remote outcomes require owner
observation and explicit reconciliation before another staging attempt.

After capture and before withdrawal, the coordinator compares every captured
worker's exact warm map and assignment checksum to its protected native map and
coordinator activation target. It independently checks genesis and every current
and retained advertised anchor against the node. A mismatch fails preservation
before v11 staging. Rollback allocates 60/140/300/100/140 seconds to withdrawal,
restore, cold-cache/private verification, reopen and public verification,
respectively: 740 seconds total, within the 900-second recovery ceiling.

Candidate worker runtime caches are prepared within the guarded staging phase,
using only the installed checksum-bound worker unit and reviewed active map,
assignment and binary. The predecessor worker is stopped first. Preparation
has a 1200-second bound within the existing 1800-second staging phase, checks
host memory and disk availability at or above 20 percent, rejects OOM/restarts,
and reads persistence counters from the bounded native readiness cache object,
rejecting duplicate/missing/malformed fields. It waits for native warm completion
with zero cache-write failures and pending writes. It then stops the candidate under the same remote owner and lock,
retaining disk caches. Missing observations, foreign identity or interrupted
owners refuse progress. Preparation never reopens routes or qualifies serving;
subsequent activation still performs its independent 300-second exact warm proof,
installed setup checks and private/both canonical encrypted-query proofs.
Rollback deadlines and resource floors are unchanged.

Public recovery now runs two independent native client proofs with separate
reopened SQLite stores, using each canonical origin as the encrypted query
origin in turn. A successful metadata request or filter request alone does not
qualify that origin's setup and query path. Both client results must pass before
the public proof receipt is recorded; either failure withdraws both origins.

After a completed adoption, an ordinary `verify-rollback` failure may use
`schema-reconcile-resume-plan` / `schema-reconcile-resume-preflight` /
`schema-reconcile-resume-deploy`. The `--map-sha256` argument binds the original
adoption plan digest in this mode; deploy additionally binds the new preflight
digest. Resume verifies retained adoption completion, preserved prior bytes,
the complete current canonical predecessor fleet and the accepted adoption
anchor. It executes only the final private proof, reopen, and public proof
phases, without replaying restoration. Partial installation, uncertain remote
outcomes, and failures in other phases refuse this path.

If private proof stopped predecessor authority, use the closed
`schema-reconcile-resume-prepare-plan` / `schema-reconcile-resume-prepare-preflight`
/ `schema-reconcile-resume-prepare-deploy` path first. It checks the retained
current publication, manifests, every pinned warm worker and canonical anchor
without requiring the stopped authority HTTP endpoint. It derives the private
relay from the checksum-bound captured fleet's internal listener and requires
the same captured router IP. Only captured predecessor filter and authority
units may start; public origins remain guarded. Preparation is recorded
separately and is not client acceptance. Then run the ordinary closed resume
proof. An uncertain preparation refuses further actions until
`schema-reconcile-resume-prepare-reconcile` observes its owner gone and either
its exact completion receipt or complete coordinator quiescence.

## Node RPC policy

The JSON-RPC path uses the server-only `zakura-server-chain` adapter
(`src/shared/pir-chain`). It reads the RPC cookie when constructed, maps HTTP 401 to an invalid
cookie, and allows 120 seconds for the whole request, including connecting and
reading the body. There is no separate connection timeout. The whole response,
including a batch, is limited to 64 MiB with or without Content-Length. Valid
large historical-transaction batches can exceed this cap and fail; there is no
automatic retry, splitting or endpoint fallback. Valid batch replies are matched
by integer ID and returned in request order. Chain interpretation, canonical
checks and the direct RocksDB reader remain in that chain adapter.
These source policies do not identify the release currently deployed.

## Target configuration

| Parameter | Recent | Archive |
|---|---|---|
| Initial range | From the start of the archive shard open six calendar months before the pinned anchor time | Genesis through block before recent range, in sealed full shards |
| Selected profile | `recent-4k-8k` | `archive-wide` |
| Directory/page rows | 4096 / 8192 | 32768 / 65536 |
| Row bytes; inline events | 4096; 2 | 4096; 2 |
| V10 seal target:capacity (directory bytes) | `14366428:16760832` | `114931420:134086656` |
| V10 absolute script target:capacity | `203630:237568` | `1629038:1900544` |
| Page-row target:capacity | `7936:8192` | `63488:65536` |
| Workers | 2 full recent replicas | 1 static owner holding every archive shard (0–76) |
| Host target | 4 vCPU / 8 GiB | 8 vCPU / 64 GiB, memory optimized (`m-8vcpu-64gb`) |
| Runtime cache (RAM) | 5 GiB = 5368709120 bytes | 48 GiB = 51539607552 bytes |
| Runtime cache (disk limit) | 10 GiB = 10737418240 bytes | 96 GiB = 103079215104 bytes |
| Process MemoryMax | 7 GiB | 56 GiB |
| Process MemoryHigh (hardening rollout target) | 5.5 GiB = 5905580032 bytes | 48 GiB = 51539607552 bytes |
| Swap | Disabled for service | Disabled for service |
| Build/query slots initially | 1 / 2 | 1 / 2 |
| Disk restore slots | 4 | 4 |
| Revision retention | 3 superseded plus current, bounded disk policy | Same where revisions apply |
| Local SSD target | Included disk (160 GiB price baseline) | 200 GiB each |
| Warm state | All held tables and advertised tail; budget retained tail runtimes | Every archive shard before readiness |

These are proposed operating budgets within the accepted architecture, not target-host RSS measurements. Cache reservations omit overhead. Verify full-process and cgroup memory during cold builds, queued HTTP traffic, revision overlap and restart. Do not raise limits automatically to fit a failed census.

Each worker runs `pir-query-server --config /etc/transparent-pir/shard-server.json` as a canonical HTTPS consumer: its `transparent_history` section names the publisher's HTTPS origin (`publisher`, `publisher_network`, `publisher_genesis`, optional `publisher_ca`), its `shard_dir`, `cache_bytes` and `build_slots`, and either an `archive_upstream` (with `archive_ca`) or `full_holdings`. The worker downloads and verifies the publication itself; no roster, assignment, `worker_id` or push setting configures it, and every section rejects unknown fields. Field reference: [query server](../query-server.md#transparent) and [HTTPS consumer](prepared-distribution.md#https-consumer). The service binds only its configured private `listen` address (`pir-query-server` refuses unspecified addresses), and worker-local probes read it from `/etc/transparent-pir/shard-server.json`.

Use one 2 vCPU / 4 GiB routing host initially. This is a single point of failure. The existing chain node/indexer/publisher remains separate from retrieval capacity budgeting. Keep archive restores off recent workers. Serve immutable public filters and setup from an object/CDN origin, with a refreshable map; verify cross-origin map consistency and retain an independent wallet anchor source.

### Elastic recent replicas

> **Historical, not a current deployment instruction.** This section and the
> commands below describe the push-fleet elastic proof of concept, whose roster
> (`TRANSPARENT_FLEET_JSON`), assignment and `worker_id` settings HTTPS-only
> publication removed on 2026-10-22. They are retained as the record behind the
> two-replica floor evidence and the retained source; do not use them to
> configure an HTTPS consumer.

The accepted recent tier is two full copies; four hosts was a load target, not a
requirement. Two replicas passed the 20 QPS gate at 20.9 QPS, p99 53 ms, with
runtime construction in its own low-priority pool
([evidence](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/recent-floor-2026-09-29/README.md)). Elastic copies are
added and removed by the actuator from `tools/infra/digitalocean/transparent/worker-pool/`
(one droplet and project entry per member, its own state and host lock), never
by the production root; the scaler decides within `scaler/policy.json`
(`min_recent` 2, `max_recent`, budgets, cost cap). Serving recent replicas never
drop below two outside maintenance. Archive owners stay static and manual: the
scaler and actuator never change them, the partition or `recent_from`. The fleet
inventory owns membership once it exists; `TRANSPARENT_FLEET_JSON` only seeds it.
Formats and invariants: [elastic recent replicas](elastic-recent.md).

Operator commands on the coordinator:

- `transparent-fleet-inventory.py drain|undrain|retire|quarantine <id>`: intent;
  drain needs two other serving recent replicas.
- `transparent-fleet-actuator status|scale-out --count N|scale-in [--member id]|replace <id>`:
  journaled operations with the runtime credential; `touch scaler/disabled` stops
  every side effect. `resolve-apply` and `abandon` clear a fenced operation after
  Terraform state and DigitalOcean are reconciled by hand.
- Pause the scaler (`mode: observe`) during a canary soak or full-fleet upgrade:
  the canary gate binds the whole roster.
- `transparent-fleet-inventory.py --archive repartition|restore`: replace archive
  owners; see [archive owner changes](#archive-owner-changes).

Each worker schema has its own fleet configuration, state and scaler directories
(v11: `/opt/transparent-publisher/v11/fleet.json`, `v11/state`, `v11/scaler`).
The scaler reads worker upstreams only from that schema's `state/inventory.json`;
without it, it scrapes nothing and holds. The schema cutover seeds that inventory
from the installed roster and initial assignment. To seed it by hand, run
`transparent-fleet-inventory.py --fleet-config <schema>/fleet.json init`,
then restore the roster's bytes, since the cutover recipe pins them.
APM's `PIR_APM_SCALER_STATUS`, the actuator's `actuator.json` (`fleet_config`,
`scaler_dir`) and the kill switch must name the same schema's directories as the
scaler. The schema recipe does not manage the actuator's units, so a rollback
would leave an actuator pointed at the wrong schema. Keep the actuator disabled
and the scaler in `observe` until the recipe covers the actuator.

## Geometry and schema cutovers

Use `recent-4k-8k` (4096 directory / 8192 page rows) for the recent tier and
`archive-wide` for archive. `recent-4k` continues to mean 4096 / 4096; do not
change an existing profile's meaning. The v10 targets above derive from
`SealPolicy::for_geometry` plus the directory-byte limit: reserve one seventh
of directory bytes and one thirty-second of page rows. The absolute script
limit is independent; reaching it is not proof that variable entries fit.

The compact schema reuses the unchanged version-2 journal at
`/srv/zakura/transparent-event-data-v2`. Publish into a separate lineage, with
matching clients and a new pending-page context; never reinterpret v9 bytes or
resume its pending pages against new shard identifiers. Actual-table native
correctness certificates and public recovery checks are required in addition
to the storage census. [Status](status.md) records the SSH v10 rollout.

A schema cutover runs the fixed-publication rollout with
`TRANSPARENT_SCHEMA_CUTOVER=true` and `TRANSPARENT_DEFER_PUBLIC_VERIFY=true`,
then the publisher deployment in shadow and activate. Public metadata is
withdrawn during the switch. Keep schema caches separate using
`TRANSPARENT_RUNTIME_CACHE_DIR`; later deployments preserve the installed path.
The old live collector must not prune the candidate cache, and the new collector
must not prune rollback data. Preserve the old publication root, active records,
units, binaries and cache outside the new collector's namespace.

Place the initial publication within the live publication's writable parent,
or verify equivalent hard-link behavior inside the controller's systemd sandbox.
A read-only source mount can make hard linking fail and silently cause a full
copy on each revision. Measure free space on the actual target filesystem,
including resolved symlinks, before allocating the candidate and rollback copies.

Do not publish `archive-32k` as the selected target: uniform-chain evidence favors `archive-wide`. Preserve the tested registry entry for compatibility/research. Geometry fallback is an explicit decision with re-census, storage, client and capacity review; never silently rewrite an already published profile.

## Sizing and availability

Uniform full-chain `archive-wide` evidence is 162 shards and 57.1 decimal GB plaintext. Applying measured c-8 per-shard RSS gives approximately 93.2 GiB prepared residency, or 81 shards / 46.6 GiB per half. This is a sizing proxy, not the mixed-tier census. The mixed-tier archive is 77 shards (0–76). A single-owner prototype on 2026-09-29 reserved 41.35 GiB of its 48 GiB cache for all of them at planning headroom 0.05 (the live adapter's; 0.15 refuses it), with about 36 GiB resident. One owner therefore holds the whole archive: 48 GiB runtime cache in RAM, 96 GiB on disk, `MemoryMax` 56G, `MemoryHigh` 48 GiB, one build slot. In production (`transparent-pir-archive-03`, worker `a704616c`) it runs at 34.1 GiB RSS with 52% of host memory available, and at about 20 archive queries/s uses 1.4 of 8 cores and 12% of its query slots, with the same per-query evaluation time (about 11 ms) as the two-owner split ([evidence](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/archive-consolidation-2026-09-29/README.md)).

Retain space for the current assignment, candidate publication and rollback artifacts plus at least 20% disk headroom. The publisher needs independent peak-RSS and temporary-disk measurements; census RSS is not publisher RSS. Do not duplicate immutable sealed bytes per tail revision unnecessarily.

There is one copy of the archive. Losing or restarting its owner makes every archive shard unavailable until it is warm again: about 100–300 s from its disk runtime cache (291 s measured, including a restage), about 27 minutes cold at one build slot (1,596 s measured). With two owners the same event took out half. This is an accepted risk (2026-09-29). Recent replication does not make archive or router highly available. If archive availability requirements rise, the path is two 128 GiB hosts each holding a complete archive copy, or replicated assignments, before claiming failover. Do not apply the old nine-host 219 restores/s estimate to this fleet.

### Archive owner changes

Archive owners are static and changed only by an operator, under the production
lock, with the scaler out of `act` mode (`scaler/policy.json` `mode: observe`).
The planner cannot mix old and new owners of overlapping ranges, so owners
switch at one publication boundary: the old ones keep serving the previous
publication until the new ones activate.

1. **Host.** Add the name to `transparent_archive_names` in the coordinator's
   production tfvars and apply a saved plan reviewed as one droplet create and
   one in-place project change. The first plan after the `count` to `for_each`
   change must show only moves.
   Set `transparent_worker_deploy_public_key` in the same tfvars so the fleet
   key reaches the new host at first boot; it was unset on 2026-09-29 and the
   key was installed by hand.
2. **Standby.** The new owner must serve the active publication warm before
   the cutover. The SSH standby installer (`transparent-archive-standby.py`),
   which copied a publication to the host, was removed with HTTPS-only
   publication on 2026-10-22, and no replacement HTTPS standby procedure is
   documented yet. Measured on 2026-09-29: 1,596 s cold at one build slot;
   291 s to restart onto a newer publication from the disk runtime cache.
   `repartition` checks the standby against the worker's own copy of the map
   it serves.
3. **Cutover.** `transparent-fleet-inventory.py --archive repartition --ranges a2:0-76 --owner transparent-pir-archive-NN=<ssh_host>,<ssh_host>:8093,<host key or file>,51539607552,56G,1`.
   It refuses unless the new owner is ready and warm on the active archive
   shards. The next publication plans the new owner; the reconciler stages it
   by hard links, prepares and activates it, and the router switches owners at
   that activation. Watch freshness (at most 20 s), router 5xx and the
   continuous-load supervisor.
4. **Rollback.** If the new owner is not prepared within two publications, a
   wrong row appears or freshness exceeds 60 s:
   `transparent-fleet-inventory.py --archive restore --revision <repartition.from_revision>`.
   It is refused once an old owner is stopped or unreachable, and after any
   later archive change.
5. **Removal.** Only after the measurement gates pass and the change is
   confirmed: stop the old owners (`systemctl disable --now transparent-shard-server`),
   remove their names from `transparent_archive_names` and apply a saved plan
   reviewed as exactly one destroy per name and one project change. After that
   `restore` is no longer possible.

Combined 20 QPS results (2026-09-29, 10 minutes each on top of the continuous
5 QPS load, two recent replicas, every query exact, [evidence](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/archive-consolidation-2026-09-29/README.md)):

| Topology | Mixed 3 × 7 QPS: p50 / p99 | Archive-only 4 × 5 QPS: p50 / p99 |
|---|---|---|
| Two archive owners | 17 / 50 ms at 20.9 QPS | 16 / 33 ms at 19.5 QPS |
| One archive owner | 14 / 45 ms at 20.9 QPS | 18 / 35 ms at 19.6 QPS |

The cutover itself failed twelve synthetic archive queries over about twelve
seconds while the router could not yet dial the new owner; the standby tool now
checks that path first.

## Proposed wallet objectives from the 2026-09-08 fleet series

Proposed, not adopted: they come from three repetitions of the load series
against the activated fleet ([r1](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/runs/fleet-series-2026-09-08-r1/README.md),
[r2](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/runs/fleet-series-2026-09-08-r2/README.md),
[r3](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/runs/fleet-series-2026-09-08-r3/README.md)) at 8 and 32
concurrent wallets from a client inside the VPC over plain HTTP, and a
wallet over the internet pays TLS and its own client work on top. The 128-
and 512-wallet steps are not yet cleanly measured (owner restarts by hand
during both 128-wallet steps), so no throughput objective is proposed.

| Wallet class | Observed p50 / p95 at 32 wallets (r2, r3) | Proposed objective | Private bytes per sync |
|---|---|---|---:|
| Catch-up, 1–30 days, 4 scripts | 0.6–0.9 s / 1.0–1.8 s | p95 under 5 s | about 1 MB |
| Small active wallet, 1 script | 0.4 s / 1.2–1.4 s | p95 under 5 s | about 2 MB |
| Six-month restore, 10 scripts | 1.8–1.9 s / 3.5–3.6 s | p95 under 10 s | about 3 MB |
| Old-birthday restore, 3 scripts | 8–9 s / 37–38 s | p95 under 60 s | 73 MB, 63 MB of it filters |
| Forty-script wallet from the cutoff | 14–15 s / 22 s | p95 under 60 s | about 20 MB |
| Unused wallet from genesis | 6 s / 7–8 s | p95 under 15 s | 63 MB of filters, no private work |

Outside any objective: the heaviest reused scripts (hundreds of thousands
of events) take minutes and hundreds of megabytes and are a per-script tail
a product must bound with its own work budget. Every covered sync in every
step of every repetition digested exactly to the journal.

Capacity findings the objectives rest on: no worker refused a query at 8 or
32 wallets, queue depth stayed under 6 on the hot owner, memory was flat
(owners 49.7 GB resident against 56 GiB, replicas 4.9 GB against 7 GiB).
Demand between the two archive owners was seven to one, set by where the
reused scripts live, not by shard count: the archive split by resident
bytes is even, the split by demand is not. Balancing owners by demand, or a
third owner, is the lever if archive-02's queue grows at higher load.

## Budget baseline

Public DigitalOcean list prices checked 2026-09-07: four regular Basic 8 GiB/4 vCPU hosts at $48, two regular memory-optimized 64 GiB/8 vCPU hosts at $336, one Basic 4 GiB router at $24: **$888/month compute**. [Provider pricing](https://www.digitalocean.com/pricing/droplets). At the same prices the current floor, two recent replicas, one archive owner and the router, is $456/month before elastic copies.

This excludes existing node/indexer/publisher, CDN/object storage, backups, taxes and transfer overages. Basic shared workers are a cost baseline, not guaranteed sustained bandwidth. Recheck SKU/region availability and benchmark both shared and dedicated alternatives before provisioning. Additional vCPUs do not imply independent memory bandwidth. Two full-copy 128 GiB archive hosts alone would cost $1344/month at the same listed family.

## Publication and rollout procedure

Operational timeouts do not establish or relax the beta's 15-minute
single-failure recovery target, nor the canary freshness limits or observation
durations.

1. Verify live state and journal identity. Pin source SHA, schema, anchor hash/time, explicit cutoff height and cutoff derivation algorithm in candidate metadata.
2. Use a new publication directory; never overwrite the rollback set or switch an existing schema in place. Publish through `.github/workflows/publish-transparent-shards.yml`, which takes the commit, journal, fresh output directory, anchor height, recorded `recent_from` and both geometries, re-derives the cutoff with `shard-cutoff --expect-cutoff` and refuses a disagreement, publishes `--through` the anchor, and stores `publication.json` and `cutoff.json` beside `shards.json`.
3. Validate complete coverage, manifests, tables, filters, true directory placement and exact replay before touching running services. Stage verified bytes at assigned owners.
4. Query servers fetch the publication from the publisher over HTTPS. The shard-push worker workflow (`deploy-transparent-shard.yml` and its shell installer) was removed with HTTPS-only publication on 2026-10-22; no repository workflow ships shard sets to workers.
5. Canary real wallet recovery, tail republishing and outage handling, then expand only after workload gates pass. Record actual deployment state and measurement artifacts.
6. Roll back binary, publication and routing together. Preserve the previously valid revision/coverage relationship; a wallet must explicitly recover from an incompatible or lower anchor rather than silently accept it.

The standalone filter deployment workflow and its installer have been removed.
They targeted the superseded standalone deployment; current v12 publication and
filter authority are managed together. Use the [operator guide](../operations.md)
and the product's reviewed deployment procedure. Retained source revisions and
artifacts remain the source for historical rollback; this cleanup changes no host.

## Runtime persistence and deployment identity

Fleet units set `runtime_cache_dir` to `/srv/transparent-pir/runtime-cache` and
`runtime_cache_max_bytes` at twice the configured RAM cache budget. Standalone
servers leave persistence disabled unless a cache directory is supplied. The
cache stores public encoded databases and packing matrices, keyed by revision,
table, segment, plaintext digest, parameters, setup seed and cache compatibility
version. Application release SHAs and publication directory names do not
invalidate compatible entries. Changes to the pinned PIR/NTT representation or
setup derivation require a compatibility-version change and a cold-cache rehearsal.

The first cache-enabled activation builds and writes its runtimes. Later
compatible restarts restore them, while still verifying source tables. Cache
restoration consumes the existing RAM reservation and a separate bounded pool
(`--runtime-restore-slots`, default 4). Cold fallbacks still use the configured
build slots. The qualified M1 rollout selects two cold-build slots for recent
replicas; archive settings are unchanged. This target does not imply every worker
has upgraded: [status](status.md) records the dated rollout inventory.
Prewarm schedules enough jobs for both pools; blocking jobs retain
their slot and reservation even when the caller is cancelled. Missing,
corrupt or incompatible entries rebuild; a failed cache write is counted and
logged but does not prevent serving a successfully built runtime. This reduces
warm-up work; it does not provide interruption-free archive handover.

The prepared-subset gate checks cache capacity before stopping a worker. Keep
current, candidate and rollback runtime entries, one largest-entry temporary and
at least 20% filesystem headroom. The disk limit is a cap on persistent cache
entries; the atomic-write temporary needs additional free space. Entries are
written atomically under a process-shared lock. After verified activation, cache
pruning retains all revision directories in the active and rollback sets. When
both units name the same physical set, plaintext revision pruning is deferred.
Never delete a rollback set to make a capacity check pass.

For a schema cutover, use a separate static-set parent and set
`TRANSPARENT_RUNTIME_CACHE_DIR` to a separate cache directory. The fleet deploy
preserves that explicit cache path on later runs and checks disk headroom on its
actual filesystem. A live publisher's collector retains only its active,
prepared and retired runtime digests; sharing a cache with a different schema
would let either publisher remove the other's prepared or rollback entries.
After the old controller stops and the fixed fleet switches, move the old
worker publication roots aside before restoring publication control. Save the
active and revocation records, and restore paths, records, binaries and routing
together on rollback. Static cutover pruning being disabled alone does not
protect data from the live collector.

`assignment_sha256` remains the full assignment-document digest. The additive
`worker_assignment_sha256` readiness field excludes generation timestamp, source
SHA and other workers while binding the publication and this worker's assignment.
`binary_sha256` identifies the executable captured at service startup. An unchanged
worker can therefore retain its older audit document. New publications still
restart workers because map metadata is held in memory. Unmanaged systemd drop-ins
must be reconciled before deployment. `force_redeploy=true` disables worker skips.

Workers take no control commands: each query server synchronizes publications
from its HTTPS publisher and reports status on its loopback `management_listen`.
Readiness reports disk cache bytes/hits/misses/write failures, and Prometheus
reports restore counts and cumulative restore time separately from cold builds.

Acceptance is a measured compatible-binary deployment below ten minutes for the
deployment phases, excluding build/CI time. Validate one replica and then one
owner before expanding, then repeat with compatible cache entries and verify the
wallet regression. Record peak RSS/cgroup memory, disk use, errors and warm/cold filesystem
cache state alongside phase timings. See [remaining work](remaining-work.md) for
open rollout gates and [evidence](../../evidence/transparent/README.md) for recorded measurements.

### Verification carried across revisions

HTTPS consumers check exact byte counts while receiving bulk artifacts and
store the publisher's expected digest with the completed file. Small metadata
is hashed immediately. Bulk SHA-256 checks run once after serving starts, on
one process-wide thread that pauses between chunks while queries wait or run.
A failed check withdraws only dependent shards until a refetched replacement
passes validation. Unchanged checked files reuse their saved validation across
restarts. See [prepared distribution](prepared-distribution.md) for the trust
boundary, recovery policy and operational status. No filesystem-specific
integrity feature is required; macOS and Linux use the same policy.

Local sidecar receipts carry the download identity to staged files. Publisher
preparation owns semantic source and runtime checks; query activation checks
receipt identity, regular-file status and size without repeating those checks.
Copied files receive their own background hash check. Files used directly by
the publisher or developer, without network receipts, retain their full
verification path.

Existing catalog copies must match the downloaded artifact before receiving
its identity. Receipt generation 2 invalidates older download and semantic
receipts, which could have been assigned to different same-size catalog bytes.
Older receipt identities cannot inherit a new validation result.

Receipts establish checked content, not serving authority. Fresh publisher
information, canonical anchors, withdrawal records and activation fences still
apply. Changes to semantic verification must bump the local receipt version.

For local source files without a network receipt, a candidate publication within a running process
inherits verification from the publication it replaces, per table segment:
only when the candidate's segment is the same committed segment (runtime
identity — the revision digest for a directory, the chained child identity
for a page shard — table kind, index, digest and geometry) and resolves to the
very file the process hashed, by device, inode, length, modification time and
change time sampled before and after hashing, with both filesystem clocks older
than the read by more than a timestamp tick. Creating or removing hard links
changes change time and conservatively requires hashing again; a restored
modification time cannot hide an in-place write. Files already linked before
verification can carry their verification into another revision. The
candidate's sources name its own links, so collecting the old publication
breaks no read. A replaced file, an in-place write, a missing file or a file
written just before it was hashed is verified in full; cold builds and cache
restores still re-check their sources. Display revisions inherit whole, by
digest, and skip their row checks only when every segment is inherited. Each
load reports segments and bytes hashed and inherited, row-check bytes, wall and
thread CPU time; `prepare` replies and the control `status` (`last_preparation`)
carry it, with the refusal kind (`invalid_source`, `runtime`, `admission`,
`superseded`) when a prepare fails. The on-disk runtime and publication formats
are unchanged.

Required history and transaction-detail runtime checks also accept
process-local byte-checksum evidence from an earlier successful self-check.
For received files the saved network digest supplies the byte identity;
standalone local runtimes still require a full checksum scan.

Measure baseline and candidate on the same isolated Linux host with the same
publication sequence, data size, query load, build flags and initial page-cache
condition. Use at least three alternating runs each. Record median and min/max
for first load, staging, preparation, warming, activation and public
advertisement; include CPU seconds, source verification counters, runtime
checked/reused counts, peak memory and disk overhead. Keep first load and
restart separate from steady updates. Capture CPU and off-CPU profiles separately
from unprofiled timings and identify the top three paths before and after.
The improvement target is under 30 seconds for each representative update with
one of 96 source segments changed; the existing 60-second readiness budget is
unchanged. Retain raw runs under ignored `.local-pir/` and report exact commits
and host identity. Production catch-up remains a separate deployment check.

## Ingest, infrastructure and aging

The journal's start height is persistent identity, not a resume cursor. Read metadata before starting ingestion; a genesis journal requires `start_height=0`. A mismatched start can set the old journal aside. Keep the workflow mismatch guard and resume from committed next height. Publication uses the read-only journal-opening path.

Ingest reads the chain from the node's own RocksDB (`--state-dir`, the
workflow's `state_dir` input, `/root/.cache/zakura` on the coordinator) as a
read-only secondary instance; previous outputs resolve through the node's
transaction index. Omitting `state_dir` selects the JSON-RPC path, which is
roughly a hundred times slower and exists only so an old dispatch keeps its
meaning. Resume a genesis journal with `start_height=0` and `state_dir` set.

Historical reports mention Terraform state drift, unintended transparent-spend provisioning, SSH firewall drift, volume lookup mismatch and stale SSH host keys. Re-verify these as preflight findings rather than blindly following archived repair commands. Inspect the saved plan and protect the shared node/volume and Enhance services.

Freeze the initial cutoff for the pilot. `shard-cutoff` derives it: the cutoff time is the anchor block's header time minus six calendar months, day clamped to the target month's last day, time of day kept; the cutoff height is one more than the highest height whose header time is before the cutoff time, which is well defined under non-monotone block times and is proved final by eleven consecutive blocks at or after it. That is the calendar height. The recorded cutoff moves back from it to the first height of the archive shard still open there, found by replaying the journal through the archive geometry's sealer (`--archive-geometry`, default `archive-wide`), or stays at the calendar height when the last block before it sealed a shard. Every archive shard is therefore sealed by a threshold, none is cut short at the tier change, and the recent tier covers at least six calendar months. The live v11 publication predates this rule: its recorded cutoff 3289805 is the calendar height and left shard 81 part-filled, so re-deriving it with the current tool gives a different height. The inventory action of the backfill workflow records the height, hashes and times in `cutoff.json`; a publish passes the recorded height back and the tool refuses a disagreement. Do not use approximate block counts as calendar time.

Old recent shards keep their geometry within the publication lineage until a declared re-cut replaces them. They can move to archive ownership after verified copying and routing handoff. Budget their actual growth; the previous 6.5 GiB/year figure is an extrapolation, not a retention guarantee. Re-cutting sealed recent shards into wider archive shards is possible only as a [declared re-cut](architecture.md#declared-re-cuts): the map names every entry it replaced, and wallets that read declarations keep the history they hold. No publisher builds re-cut maps yet, and production must not be re-cut until every client in use reads declarations; the publisher work is open in [remaining work](remaining-work.md).


## Continuous publication

The unified publisher changes through `publisher-cutover-*`
([publisher](../publisher.md)). The standalone continuous-publisher workflow
(`deploy-transparent-publisher.yml` and its installer), which pushed worker
binaries over SSH, was removed with HTTPS-only publication on 2026-10-22;
retained source revisions keep it for historical rollback. Deep reorg recovery
should normally be left to the controller rather than restoring historical
artifacts.

The service polls the node's best-chain height/hash every second, commits each
block to the event journal and rebuilds only the affected publication suffix.
Near-tip extraction uses raw-block RPC, because the RocksDB secondary cannot
observe non-finalized blocks. Publication has a 30-second freshness target from
node acceptance to warm public coverage. Rapid blocks may share a publication;
coverage remains contiguous. Historical catch-up and deep reorg rebuilding are
reported separately from steady-state freshness.

Activation requires all archive owners and at least one warm recent replica.
With `manage_all_workers: true` the reconciler prepares every member, archive
owners included; the foreground adapter only waits for those jobs and never
stages a worker itself. Preparation returns at quorum plus
`prepare_grace_seconds`, so recent replicas that finish their tail build moments
after the first one activate together; the grace costs freshness directly. A
replica that misses it is activated and routed by the reconciler as soon as it
attests the current publication. Activation also returns at quorum plus a short
grace and waits at most `activation_lock_seconds` for a worker lock.

Membership probes run without the routing lock. A change is applied under it
only if no activation, invalidation or withdrawal happened since the probe
(`state/routing-generation.json`). A transport failure, or an unknown canonical
endpoint, unroutes a recent replica only after three consecutive failures
spanning five seconds (`membership_failures`, `membership_failure_seconds`); a
status that answers but does not attest (not warm, another digest, a fork)
removes it at once. Archive owners leave routing only through activation quorum
or invalidation. The reconciler writes observed member states and the routed
recent count to `state/membership.json`; the controller's `ready_replicas`
reads it while it is under ten seconds old.

An `https-recent` roster member is a provider that updates itself over HTTPS;
its entry also names `status_listen`, the provider's loopback listener. The
adapter never plans, stages, prepares, activates, invalidates or opens a control
session for it, and it never counts toward quorum, `routed_recent` or
`ready_replicas`. The reconciler reads its `/v1/provider-status` and
`/<api_version>/shards/init` on its host over one direct SSH command, every half
second. It is rendered in the recent group only while its own status shows the
fleet's exact active `map_sha256`, warm and not invalidated, with every recent
shard local and unrevoked at the published manifest digest, and its `init` shows
the same map, network, genesis, profile, start height and covered range.
The publication endpoint must also be canonical. The first refusing observation
withdraws it; transport failures follow the replica rule above. Activation
drops it until it attests the new publication. Its admission or withdrawal
never routes a fleet without quorum, so it can never force a withdrawal.

Artifact transfers use their own SSH master for up to 60 seconds, never the
owned control session: a transfer on that connection delayed status past its
budget. Assignment plans are written once per publication, locally and on each
worker; a worker refuses a prepare that names a publication it already holds
under a different row of the assignment. A publisher redeploy carries
operational `fleet.json` settings over (`OPERATIONAL_KEYS` in
`transparent-live-fleet.py`). Cancelling a slower replica does not delay an
already warm quorum. An unchanged router configuration skips
reload only when its successful application marker matches; an interrupted
rename/reload is retried.
Workers synchronize each publication from their HTTPS publisher, keep current runtimes
resident within their existing cache budgets, and swap snapshots without a
restart. Preparation evicts unpinned runtimes from retired generations before
building and warms only current revisions. Retained revisions remain available
on demand; filling spare cache capacity with them consumes the next build's
scratch-memory headroom. Both public map URLs, filters and initialization use the same active
publication. The controller keeps three unused candidate directories; workers
keep the newest three retired serving snapshots plus any older snapshot still
held by a request. An old reader does not prevent collection of other idle
snapshots. The server releases its startup snapshot after launching prewarm;
provider status reports `retired_snapshots` so a growing backlog is
visible. Three superseded normal tail revisions may also accompany the current set.
History workers stage under `worker_root/workers/<worker-id>/`. Each revision
is transferred once into `.sources/<manifest-digest>`; publication directories
contain relative links to those revisions. Creating or collecting a publication
therefore leaves table inode timestamps unchanged, allowing verification reuse.
The worker accepts only links to this sibling catalog with a real revision
directory, then performs its normal manifest and table verification. Staging and
collection share `.sources.lock`; collection defers during a transfer and removes
catalog revisions only after no retained publication references them. Worker
namespaces keep one worker's collection from deleting another worker's sources.
The first publication may seed the catalog from the prior layout by hard link;
that operation requires fresh verification once. Prior-layout directories remain
available for rollback and are not removed by the new namespace's collection.

A reorg affecting served coverage withdraws it before rebuilding. A fork
strictly above served coverage may retain that coverage only after the fleet
independently verifies its canonical endpoint and warm routed workers; otherwise
it withdraws. Workers refuse orphaned revisions and discard affected preparation; the publisher preserves the valid
sealed prefix and creates a new immutable suffix publication. Retry and restart
must revalidate the candidate against the node before exposing it. Wallets still
accept anchors against their own chain view and rederive affected ledger state.

The coordinator exposes private `/v1/status` and `/metrics` on port 8094.
Freshness, node/journal/public heights, lag, ready replicas, withdrawal and reorg
depth are observable. The controller emits a structured stale-publication alert
when a pending observed block exceeds 30 seconds. No Prometheus runs: `pir-apm`
scrapes the coordinator's `/metrics` and owns the publication alerts
([observability and alerting](../enhance/observability-alerting.md)).
The production deployment key is supplied by the GitHub Environment and stored
only in the controller's root-readable runtime credential directory.

### Deployment identity and coordinator routing

- **Deploy identity.** The transparent router and every worker must authorise
  the public half of `WALLET_PIR_DEPLOY_SSH_KEY` before that secret is rotated.
  Publisher shadow mode now checks every host first and changes nothing if one
  refuses it. On 2026-09-27 a rotated key was authorised nowhere; append the new
  key and verify from the coordinator before removing the old one.
- **Coordinator routing.** Public transparent metadata depends on the
  continuous-publication route in the coordinator Caddyfile. Enhance deploys
  render that file from `tools/deploy/coordinator/Caddyfile`, which carries the
  route; `tools/tests/operations/test_coordinator_caddyfile.py` fails if it is removed. After
  any coordinator deploy, confirm that
  `https://transparent-pir.valargroup.dev/v1/shards/init` returns 200.

## Hardening rollout gate

Continuous-publication recent worker upgrades preserve the 5.5 GiB MemoryHigh
setting in both deployment paths. It triggers file-cache reclamation below the
hard limit; cache and transient allocation admission remain separate. The
[canary evidence](https://github.com/valargroup/wallet-pir/blob/42b5f9c145cc3f2a400c428938c566d2cc9699db/docs/transparent-pir/evidence/hardening-2026-09-08/README.md) records the observed
tradeoff and failed lower threshold. Other workers retain their installed
settings until their rollout.

The push-fleet hardening tools were removed with HTTPS-only publication on
2026-10-22: the maintenance upgrader (`upgrade-transparent-fleet.py`), its
canary supervisor (`run-transparent-hardening-rollout.py`), the read-only
observer (`observe-transparent-hardening.py`), the one-at-a-time recent roll
(`roll-recent-replicas.py`) and the worker pre-start helpers they installed for
the `headless_console` and `storage_nodiscard` fleet settings. Retained source
revisions keep them for the evidence they produced. Binary rollback on a worker
never rebound the framebuffer or changed serial-console configuration; restoring
graphics or online discard is a separate, explicit host operation. The
[bounded disk-stall experiment](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/productionize-m1-shoup-reduction-2026-09-11/README.md)
records the discard policy's qualification.

The M1 post-rollout requirement is **six continuous hours and at least 300 new
canonical blocks per worker**, confirmed by Roman on 2026-09-13 as the updated
requirement under which the fleet observation had passed. This supersedes the
previously documented twelve-hour requirement. The initial loaded canary
remains six hours and 300 blocks. Correctness, freshness, routing, memory,
restart/OOM and provenance requirements remain unchanged.

For the September 12 observation, accept the first six-hour window ending
approximately 06:49:59 UTC under the separate
[dated transition record](../../evidence/transparent/productionize-m1-six-hour-acceptance-2026-09-13/README.md). Preserve the original 24-hour launch
and its later failed terminal result; do not relabel that command successful.
The later freshness incident remains an M5 follow-up before beta. This specific
operator-confirmed transition does not permit arbitrary successful prefixes to
be selected from future failed observations. Future runs must be launched with
the approved duration and preserve complete results and query logs.
The full-fleet gate must match the tested binary, fleet script, configuration
and roster digest; elapsed time alone is insufficient. The foreground adapter and
reconciler must use the same `fleet.json`. Enable `managed_recent_workers` for
recent-01 at the canary stage; once every worker's binary reports the control
status fields, set `manage_all_workers: true` instead.
With `control_sessions: true`, short control commands use authenticated SSH
masters owned by `transparent-control-sessions.service`, separate from artifact
transfers and long-running preparation. Install and start that unit, then verify
status through every owned connection before enabling the configuration flag
and restarting the reconciler. Restart the session unit after roster or SSH
authentication configuration changes. It owns foreground SSH processes, reconnects
after exit and uses a separate private socket namespace. Clients cannot silently
fall back to fresh connections; cancellation cannot terminate the shared master
or wait indefinitely for its output descriptors. Bootstrap configurations without
the flag retain direct SSH. Preparation always uses direct SSH. Read-only status
retries one transport failure (1 s then 1.5 s command budgets within the 3 s
membership timeout).
The shared supervisor library, `tools/lib/wallet_pir_ops/control_sessions.py`
([control sessions](../control-sessions.md)), builds the same socket
names and SSH arguments; a test holds the two equal. This script stays
standalone on the coordinator and does not import the library.
The optional `status_socket_forwarding: true` flag requires `control_sessions`
and carries status over root-only Unix forwards owned by the same supervisor.
It is disabled by default; consult [status](status.md) for whether it is deployed.
Each request has its own stream, a 1 MiB newline-framed response limit and the
same status timeout/retry policy. Missing forwards do not fall back to helper
execution. Mutations keep their existing SSH paths. Verify all worker forwards
before enabling clients; changes require matching acceptance evidence.

Returned status still must attest the current warm publication. Transport
failures change membership only under the hysteresis above; invalid or rejected
status is not retried. Mutating control
commands are never blindly retried after an ambiguous transport failure.
One daemon task owns each managed worker's preparation across foreground quorum
cancellation; it coalesces queued targets and reattests status after restart.
A superseded but canonical prepared candidate may advance an unrouted worker
privately when its height is already covered by the public authority. The worker
remains excluded until it attests the exact current public map. This prevents a
burst from discarding every completed preparation and indefinitely delaying
replica coverage. Future candidates and withdrawn/orphaned publications cannot
use this path.


Audit a closed observation with `tools/transparent/scripts/audit-transparent-observation.py`
against the extracted bundle. The supervisor cannot certify a gate shorter than
the one it was launched with, and a monitor that fails writes a thin result with
no provenance, so acceptance is a separate audit rather than an exit code. Every
threshold is a flag. The current audit still defaults to the historical
twelve-hour duration, so pass `--seconds 21600` for the six-hour requirement.
The report names each threshold that
departs from the default or from the value the run was launched with, so a
relaxed gate is visible in the evidence. It reads a directory, contacts no host
and changes nothing.

```sh
python3 tools/transparent/scripts/audit-transparent-observation.py \
  --observation <extracted>/observation --seconds 21600 \
  --canary-result <canary>/result.json --upgrade-result <upgrade>/result.json \
  --out <evidence>/audit.json
```

Cross-stage reconciliation compares the worker binary and roster digests, which
must agree across the canary, the upgrade and the observation. It deliberately
does not compare `fleet_config_sha256` across stages: the canary and the full
fleet legitimately run different configurations, and that digest is instead
required to agree among the six observation workers. A worker whose result
reports identity its own `/v1/ready` attestation contradicts fails.

A freshness sample at or just past its budget records the monitor abandoning the
read. It bounds the true recovery time from below and is never a catch-up
measurement; establish the actual time from the worker log before quoting one.

The retained observations were taken by the removed hardening observer, which
made at most two attempts for a read-only HTTP GET after a reset, broken pipe,
remote disconnect or unexpected TLS EOF, within one eight-second budget, and
counted all elapsed time in freshness. Its results record monitor identity and
retry policy. See [qualification evidence](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/productionize-m1-http-retry-2026-09-12/README.md)
and [status](status.md) for deployment state.

## Opt-in archive parent filters

The user approved publishing the evaluated archive configuration after accepting the large-script overhead. Use **8 consecutive sealed archive shards per parent, M=100, P=6**. Keep recent and provisional filters direct. Parent-enabled clients accept the coarse-activity leakage described in the contract. The production manifest URL is `https://enhance-pir.valargroup.dev/v1/filters/parents/archive-wide.json`.

The public bundle contains only `archive-wide.json` and `artifacts/<sha256>.bin`. Stage a previously evaluated bundle with `tools/transparent/scripts/stage-transparent-parents.py --candidate /path/to/archive-wide-k8-m100.json --map-url https://enhance-pir.valargroup.dev/v1/filters/shards --out /srv/transparent-parent-filters/releases/NEW_RELEASE`. The script checks exact live sealed archive descriptors, complete archive coverage, precision/group sizes and evaluated body digests before creating the serving tree. It does not regenerate filters or independently prove their journal completeness; that comes from the full-journal evaluator.

The coordinator Caddy template routes `/v1/filters/parents/*` to `/srv/transparent-parent-filters/public` with a five-minute HTTP cache lifetime. Point that symlink to the staged immutable release atomically. When first adding the route, preserve existing routing, validate the candidate Caddyfile before installation, save the predecessor, reload Caddy, and verify the manifest and every parent digest through public HTTPS. Restore the predecessor on activation failure. Do not restart the PIR fleet to publish static parent artifacts.

The bundle is tied to exact sealed child revisions. Refresh it after archive membership or revisions change; wallets reject stale parent descriptors and fall back to children. To withdraw a release, unlink its public symlink after checking the current target. Existing validated client caches can remain usable for unchanged child revisions. See the [production evidence](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/parent-filters-production-2026-09-08/README.md) for the deployed release, canary and rollback record.

Reference clients opt in with `HttpFilterSource::with_parent_experiment(manifest_url, "archive-wide".into())`; loadtest scenarios use `experimental_parent_manifests.archive-wide`. Hosting the bundle does not change already-installed wallet applications. No script-count bypass is enabled.

## macOS recovery beta acceptance targets

Agreed planning targets, 2026-09-09; **not measured acceptance or a general
production SLO**. The [execution plan](remaining-work.md) defines the bounded,
recovery-only beta. Existing hardening/publication gates above still apply.

- Capacity protocol: 8, 20 and 40 concurrent wallets; advance to 80 only if 40
  passes. At each level perform three 60-minute sustained runs with the existing
  mixed profile distribution and publication active. Collect at least 100
  completed observations per ordinary profile before accepting its p95; extend
  measurement if the three runs do not supply enough samples.
- Freeze the ordinary-wallet candidate thresholds from
  [the proposed wallet objectives](#proposed-wallet-objectives-from-the-2026-09-08-fleet-series)
  before running: p95 below 5 seconds for catch-up/small active, 10 seconds for
  six-month restore, 60 seconds for old-birthday and cutoff forty-script wallets,
  and 15 seconds for unused wallets. These thresholds must pass the new protocol;
  the older measurements do not establish their acceptance.
- Heavy reused histories remain outside those ordinary-profile latency targets.
  Use the existing scenario's 600-second recovery deadline for a bounded attempt;
  preserve explicit incomplete state and test continuation. Do not exclude such
  attempts from failure/incomplete reporting or claim their completion SLO.
- Stop load escalation on correctness failure, OOM, sustained available host
  memory below 20%, or publication freshness violation. Planned beta demand must
  be at most 50% of measured sustainable completed-sync throughput at the accepted
  workload mix, with bounded queues and resources.
- Outage rehearsal budget: restore canonical service and exact completion within
  15 minutes for each tested single failure. Temporary archive-owner unavailability
  is accepted for this beta; redundant general availability is a separate decision.
- Begin with five internal testers. Expand to at most 20 after 48 passing hours
  only if the measured capacity envelope permits it. Require seven consecutive
  days after the last release-affecting fix; such fixes restart observation.

These are release-test criteria, not automatic deployment, fault-injection or
infrastructure-provisioning actions. Any revised target needs an explicit recorded
change before retesting, with the previous failed result retained.


## Service-quality monitoring

The dedicated page is `/apm/transparent/` on the Enhance APM origin. The shared
[observability guide](../enhance/observability-alerting.md) owns collector,
history, guardrail and independent-canary configuration. Metrics stay on private
service endpoints or loopback Caddy admin; no public metrics route is introduced.

Worker binary upgrades coordinate the continuous load through
`transparent-binary-*` ([binary upgrades](#binary-upgrades-of-the-current-v12-history-and-4k8-display-fleets)).
The existing six-hour / 300-block and full-fleet observation gates are unchanged.

## Activity metadata v3/v11 prototype and cutover

The candidate journal and publication share the 250 GiB XFS coordinator volume
at `/srv/transparent-activity`. V10 data, binaries, units, active records and caches
remain outside that namespace. Prototype data covers only blocks 3499739–3500738;
it cannot replace the complete canonical publication.

The prototype shard service is loopback port 8192, with CPU quota 400%, 8 GiB
process memory, no swap, 4 GiB runtime reservation, 8 GiB disk-cache limit and
1 build / 4 query slots. Release-fast artifacts are disposable prototype artifacts.
The frozen candidate repeats metadata on events and uses unchanged 4096-byte PIR
rows and registry geometries. Recalculate shard occupancy from actual v11 encoded
bytes; v10 capacity arithmetic does not establish a v11 capacity benefit.

Genesis ingestion uses the RocksDB secondary reader, journal v3, fixed anchor
3500738, four workers, CPU quota 400%, a 16 GiB memory ceiling and no swap.
The current owner is `transparent-activity-full-ingest-release-a1c4b809`, using
the immutable, checksum-verified fat-LTO release binary; its independent oracle
and uncached journal comparison passed before checkpoint handoff.
The ceiling includes RocksDB file cache; the initial 6 GiB limit caused direct
reclaim despite roughly 89% available host memory. Its dedicated health guard retains
source SHA, executable hash, PID/unit, checkpoint and terminal result, sampling
memory and disk every five seconds. Stop candidate ingestion at less than 20%
available memory or disk, or an unexpected process replacement. Independent
chain comparisons and a complete publication remain separate acceptance gates.

Build in the persistent coordinator lane with sequential Cargo writers, a 12 GiB
memory ceiling, no swap and low CPU/I/O priority. Compress immutable source exports
before SSH transfer. Production binaries require the fat-LTO `release` profile,
executable checksums verified after transfer, compiler/profile/dependency provenance,
and matching publication identities and native certificates.

The publisher shadow installer accepts `--publication-root`. For v11, use
`/srv/transparent-activity/full-v11/publications` alongside
`--data-dir /srv/transparent-activity/full-v3/journal` and an initial publication
inside that publication parent. The installer renders sandbox write paths for
those exact locations and the initial source, retaining `ProtectSystem=strict`.
Verify hard-link behavior under the resulting unit before activation.

For canonical cutover, coordinate with publication/deployment owners, take the
existing deployment lock and put the scaler in observe mode. Pause maintenance
load, withdraw metadata at both public origins, stage the complete fleet assignment,
prewarm workers, align public filter and shard origins, verify accepted anchors,
setup and directory/page answers, then reopen. Verify schema-separated caches and
hard links inside the publisher sandbox. Resume canonical 5-QPS traffic only with
updated fixtures and worker pins. The paused quality supervisor remains inactive;
new quality alerts remain in shadow. Restore the coherent v10 fleet and origins
before reopening if a cutover gate fails; migrated clients need compatible readers.

The loaded prototype and early production cutover do not establish qualification.
Preserve the final six-hour/300-block freshness window, three sustained 60-minute
runs at each of 8/20/40 wallets, every failed or incomplete attempt, lifecycle faults,
and rollback/redeploy gates in the approved activity metadata plan. Supported
completed-sync demand is at most 50% of measured sustainable throughput.

### Guarded schema operation API

All production changes, including source staging, binary transfers and unit
changes, run through `tools/scripts/wallet-pir-deploy.py`. The wrapper's
`schema-plan`, `schema-preflight`, `schema-deploy`, `schema-status` and
`schema-rollback` commands coordinate a checksum-bound recipe on the inventory's
pinned root coordinator under `/run/lock/wallet-pir-production.lock`.
Use the same `--inventory` and the private coordinator state directory
`/srv/transparent-activity/ops/schema` for every command. Production schema
transactions refuse a different state directory: generic deployment and source
bootstrap check this durable ownership fence under the production lock. Plan and
preflight precede deployment; `schema-deploy --recipe FILE
--expect-recipe-sha256 HASH` requires the digest printed by the reviewed plan.
Status reports the durable journal state, and must be paired with live canonical
checks. After deployment the CLI prints status and a complete rollback command.

Recipe version 1 pins the native source SHA, publication SHA-256, forward inputs
and separately retained rollback inputs. Each input is an absolute regular file
with a SHA-256 checksum. Commands invoke those bound executables or bound scripts
through Python/Bash, without inline interpreter programs. Recipes contain no
credentials; phase programs read runtime credentials without printing them.
Preflight commands and verification phases must be read-only.

The forward phases are `preserve-v10`, `maintenance`, `stage-v11`,
`activate-prewarm`, `align-origins`, `verify-canonical`, `resume-load` and
`verify-service`. The recovery phases are `withdraw-origins`, `restore-v10`,
`verify-rollback`, `reopen-v10` and `verify-service`, with a combined deadline of
at most 900 seconds. Programs must be idempotent after partial execution,
including a partially completed predecessor backup. They must reject an
incomplete backup before withdrawal and preserve separate v10 data and caches.
Verification must fail before reopening when the restored service is incoherent.

The wrapper records phase intent before side effects, rechecks input hashes and
the lock between phases, and retains private journals and phase logs. Child
programs receive the lock descriptors through `pass_fds` and
`WALLET_PIR_PRODUCTION_LOCK_FDS`; they must preserve them in any descendant that
can mutate production. A nonzero phase exit triggers coherent recovery while
the lock is held. Timeout, interruption or lock loss require reconciliation and
explicit recovery, so a possibly live descendant cannot race an automatic
rollback. An unfinished transaction blocks a new deployment.

This API is an implemented coordination boundary. The actual production recipe,
trusted phase programs, complete publication and live acceptance checks must be
prepared and verified before deployment.

### Coordinated product service phases

`schema-input-service-{plan,preflight,stage,status,reconcile}` retains the twelve
reviewed coordinator inputs before preparing a product recipe: controller,
fleet, roster, fixture, worker pins, observe policy and the six product units.
The checksum-bound request contains `version=1`, `source_sha`,
`release_result_sha256`, `attempt` and a closed `files` object of UTF-8 bytes.
Run plan/preflight first; stage requires `--expect-plan-sha256`. It uses the
same coordinator production lock, ownership fence, atomic private bundle and
explicit partial-output reconciliation as assignment preparation. All output
remains under `/srv/transparent-activity/full-v11/inputs/<requestSHA>`; no unit,
configuration, credential, active record or routing is installed at this step.

The receiver checks the completed publication/release identities, its actual
cutoff/profile, separate v11 controller/fleet/control/cache paths, portable
worker pins, observe policy, all four traffic groups and immutable script
commands. Requests are bounded to 8 MiB, with 2 MiB reserved per fixture and
256 KiB per other file. Full product preflight still verifies actual host
inputs, recovery samples and all independent release reports before maintenance.
`activity-query-fixture.py --out -` exports the exact sealed-table row fixture
to stdout, keeping read-only preparation free of host output files; its digest
summary goes to stderr. Include provisional rows only for frozen disposable
prototype workloads.

`schema-product-recipe --spec FILE --spec-sha256 HASH` constructs the complete
ordered forward/recovery recipe from a reviewed private product specification.
Retain its exact JSON, then run `schema-plan`, `schema-preflight` and
`schema-deploy` with that recipe. This constructs service transitions; it does
not prepare missing native artifacts or worker publication bytes. Product
preflight refuses until those immutable inputs exist on every pinned host and
the full-publication artifact, native-certificate, independent-chain-oracle and
comprehensive-CI gate reports match the frozen native source and publication.
No report may be generated merely to satisfy this shape; retain and review its
independent raw inputs before binding it into the specification.

The specification includes one coordinator, one router and every assigned
worker, with distinct machine pins, complete host baseline/install plans, a
routing plan, the native assignment, and checksummed load/scaler inputs. Host
and routing templates permit `{transaction}` only in the transaction identity
and its prescribed rollback root. Commands, install targets and arbitrary JSON
strings are never interpolated. Runtime phases bind the actual schema journal
ID and require its current durable phase intent and the spec digest. They cannot
be called as free-standing host mutations.

The coordinator captures original service states before stopping writers, then
captures router and warm workers before withdrawing both metadata origins.
It stages all workers, installs the coordinator and seeds separate v11 worker
active records, controller activation, native assignment/request/desired/active
fleet records and `maintenance=true` before any restart. All workers must warm
and prove identity before authority startup. The measured certificate report retains each table's exact public-setup hash.
After every worker is warm, the coordinator reads every assigned setup on every
replica, checks its shard/revision/table/segment/count identity, decodes its bytes
and compares their SHA-256 with those measured bindings. Missing, duplicate or
changed certificate coverage refuses authority startup; private receipts retain
all comparisons. Both recovery samples must bind every worker executable, and
candidate pins must equal the reviewed host plans before maintenance.
The filter, controller, fleet,
scaler and load units must refer to the reviewed v11 paths. The scaler uses an
observe-only policy; load uses the retained fat-LTO rate client, exact worker
pins and a fixture covering all four recent/archive directory/page groups.
The source load supervisor now stops on memory or disk headroom below 20%,
including actual worker publication, coordinator candidate and chain disks.
Existing production load continues using its installed predecessor until cutover.

Private real HTTP/SQLite proof precedes reopening. A hard-link probe enters the
**running installed publisher's mount namespace**, checks its running executable
and root identity, links the immutable candidate map on its publication
filesystem and removes the probe link. Canonical HTTPS recovery must pass before
load/scaler startup. Recovery restores all workers and coordinator, verifies old
anchors and exact recovery, restores the original router Caddy bytes while the
coordinator remains guarded, then reopens and resumes only previously active
predecessor load/scaler units. Generated recovery phases reserve 740 seconds;
the outer runner enforces the total deadline and never lowers acceptance gates.

Remote `schema-host-run`, `schema-host-status` and `schema-host-reconcile` are
internal wrapper commands. The coordinator retains its FD in the local SSH
process; each remote root actor acquires its **own** pinned host production lock.
Closed request/plan hashes bind its durable private intent, PID and result.
No persistent SSH master is created. Parallel worker calls always join all
outcomes. A lost or unstructured reply, remote timeout or exit 75 leaves the
coordinator interrupted; exit 75 propagates through the runner rather than
triggering automatic rollback. Inspect the exact retained request on that host,
use status, reconcile under its lock only after surviving descendants release
it, then run explicit schema recovery. A request with a retained result is never
silently repeated. Partial baselines and capture intents require explicit
reconciliation; they cannot authorize reopening or be overwritten.

These programs have local process, ordering and failure-injection tests. Actual
SSH descendant qualification, immutable worker input staging, reviewed live
plans, complete release gates and a production transaction remain prerequisites;
source implementation is not evidence of a successful cutover or rollback.

### Immutable v11 worker input staging

After the owned full-publication preparation terminates successfully, stage the
reviewed operations export on the coordinator. The coordinator can then use
`schema-source-{plan,preflight,stage,status,reconcile} --host HOST` with its pinned-host
inventory to stage the same export on each router/worker. This form runs only
as root on the pinned coordinator, holds its production lock through SSH and
makes the remote helper acquire its own host lock. New source staging retains
the exact export at `/srv/transparent-activity/ops/staging/<SHA>.tar.gz` for this
step; older receipts without a retained archive are preserved as recorded.
Persistent SSH masters are disabled. Preserve earlier sources and receipts.

`schema-input-prepare-{plan,preflight,stage,status,reconcile}` prepares the
concrete native assignment and worker units on the pinned root coordinator.
Provide `--request FILE --request-sha256 HASH`; stage additionally requires
`--expect-plan-sha256 HASH` from the reviewed plan. The closed private request
binds the operations source, frozen release receipt, review time, attempt and
complete worker identities, roles, VPC upstreams, cache budgets and predecessor
unit bytes. At least two recent replicas and one archive owner are required.
Preflight rechecks every running peer's fragment and refuses drop-ins or pending
unit reloads until reviewed. It uses the completed publication's actual cutoff
and the native planner with 15% assignment headroom. Plan uses disposable private
tmpfs scratch; it leaves no retained candidate or owner. Only native provenance
time is normalized to the request's review time for deterministic plan hashes;
native placement and identities are preserved and checked again by `shard-assign
check`. Stage repeats the plan under the lock, retaining inherited descriptors
through preflight and native children, then atomically retains assignment,
roster, inventory and units as mode 0400 under
`/srv/transparent-activity/full-v11/inputs/<request SHA>`. This changes no live
units or records. Failed/unknown owners fence other mutations and require explicit
reconciliation, preserving partial output in a same-filesystem abandoned directory.

Publication file and protocol identities are separate. The full publication's
file SHA is `34e3ebe3...`; native Serde's compact served-map SHA is `fd4dcadb...`.
Immutable worker directories and file checks use the former; native assignment,
active records, readiness and fleet/controller activation use the latter. Host
worker plans bind both `map_file_sha256` and `map_sha256`. The protocol serializer
rejects unsupported fields/types and matches the independently retained native
verifier's served digest. Do not replace an immutable file merely to make these
identities equal. The existing 48 GiB archive cache budget is supported by input
requests; the bounded maximum is 64 GiB, with the unchanged native admission and
20% host memory/disk gates.

`schema-input-build --host HOST --worker-id ID --source-sha REV --assignment FILE
--unit FILE --release-result-sha256 HASH --cache-bytes BYTES` renders a private
request from the completed full publication and frozen 18-artifact release.
Native `shard-assign files` selects all manifests/filters and the assigned shard
directories; the renderer expands those directories into exact files. It also
includes the complete assignment, two retained worker executables and reviewed
worker unit. Retain the request JSON with a final newline and its canonical JSON
SHA-256, which is printed by the subsequent plan. Host aliases and native worker
IDs may differ. Requests are limited to 65536 files, 192 GiB and an 8 MiB header.

Use the staged wrapper with `--inventory FILE` and the same
`--host HOST --request FILE --request-sha256 HASH` for
`schema-input-plan`, `schema-input-preflight`, `schema-input-stage` and
`schema-input-status`. Plan rechecks the native file list, full publication and
release identities; preflight checks the remote pinned source/machine, free
candidate namespace, memory and disk reserve before a byte is sent. Stage
repeats these checks under the coordinator lock. The pinned receiver journals
intent, owns its host lock and streams exactly the declared bytes in 1 MiB
chunks without a temporary whole-publication archive. It rejects truncation,
extra bytes, links, duplicates, traversal, file/directory collisions and changed
hashes/sizes/modes. Available memory and disk must stay at least 20%; initial disk
reserve includes the entire incoming candidate. Raw receiver health, transfer
counts and the native verifier's PID/log/result are retained privately.

Each complete candidate is renamed on its original filesystem into
`/srv/transparent-pir/v11/publications/<map SHA>`. Prepared binaries and unit are
as flat `.input-*` files, for the later reviewed host install plan. Native
`--verify-only` checks the assignment and requested cache budget before the
completed receipt; later product preflight checks the actual reviewed unit
configuration again. This changes no live executable, unit, controller active
record, runtime cache or public route. File preparation is not warm service,
canonical recovery or qualification.

An interrupted or failed receiver remains fenced. Both the coordinator and
remote host retain the exact request SHA, original PID, private request and
result. Uncertain SSH replies return exit 75 and never authorize automatic
retry. Generic deployment, source staging, publication start and schema deploy
check these owners even when no schema journal exists. Inspect status, then run
`schema-input-reconcile` with the exact request under both locks. A remotely
completed, reverified candidate can be acknowledged; a failed/partial candidate
is displaced into an `.abandoned-<request SHA>` namespace on its original
filesystem. Evidence is preserved. A new attempt requires an explicitly rendered
request with a higher `--attempt` and a new checksum; old requests never replay.
Actual SSH surviving-descendant qualification and production lifecycle exercises
remain separate acceptance gates.

### Immutable operation source staging over SSH

The wrapper's `schema-source-plan`, `schema-source-preflight`,
`schema-source-stage`, `schema-source-status` and `schema-source-reconcile` commands bootstrap reviewed
operation sources on the coordinator. They require a remote-lock inventory with
the coordinator's machine ID pinned in its host entry, exact `--source-sha` and
`--sha256` identities, `--attempt N` (1–100), and `--archive FILE` except for status or reconciliation. Create the archive
with `git archive --format=tar.gz` from that exact commit, exporting
`tools/scripts`, `tools/lib`, `tools/deploy`, `tools/infra`, `tools/receiver`,
`tools/transparent`, `tools/enhance`, `tools/ci` and `tools/dev`. Candidate archive
transfer and preparation refuse a source whose receipt lacks `tools/ci/release.py`. This contains the
operation dependencies without historical evidence bundles. The reviewed
operations export at `56b67ba0` was 536659 bytes; its whole-tree archive was
81439424 bytes and exceeded the 64 MiB transfer limit. The client now rejects
an oversized archive before hashing or making an SSH request. Run plan and preflight
before stage; stage also repeats preflight. The helper receives the archive and
performs every write in the same root process holding the production lock.
It does not depend on a separate SSH lock process surviving the transfer.

Source staging and candidate upload use the closed bootstrap survey in
`activity_bootstrap_fleet.py`. It verifies the immutable retained full-v11
inventory by its physical file hash and the existing `4c85b6c2` runtime receipt
before importing its pinned SSH transport in an isolated package namespace.
That receipt check accepts exactly one recorded deviation: the txid display
router hook (committed 2026-10-06) replaced
`tools/transparent/scripts/transparent-live-fleet.py` in that tree in place, from
`f3df5c53…` (git `4c85b6c2`) to `35b6b436…` (git `d191f86b`), and the receipt
still records the original. Any other file, or either adapter's bytes
restored or changed again, still refuses. The runtime never imports the adapter.
There are exactly five named hosts with their reviewed machine pins. The survey
uses the shared bounded owner reader across schema, host-action, input and source
receipts, then associates live processes and descendants. It preserves the
existing baseline list plus the exact ancillary reader described below. A
service name alone does not authorize an exception.

The installed replica reconciler (`transparent-live-fleet.py` from `4c85b6c2`)
runs `shard-control` on workers over SSH; there it lives in an SSH session scope,
not a baseline unit, and its operation (stdin) is not observable, so it is never
treated as read-only. `tools/lib/wallet_pir_ops/control_attribution.py`, embedded
with the survey, attributes ownership only. A worker reports a control as pending
only for an exact chain: the fixed argv (SHA-256 `c5827d6f…`) and pinned
executable bytes (`9208555a…`, the portable worker pin production runs since
2026-10-09), root's login shell
running exactly the reconciler's `-c` command as its session leader with no other
child, an sshd session process in the same root `session-N.scope` holding exactly
one established TCP connection, and stable start ticks. Everything else stays
unattributed. Candidates come from the same complete process scan used by the
ownership survey, rather than an earlier PID listing. Before each worker survey and after a reply with pending controls,
the coordinating survey snapshots the reconciler: unit active, pinned fragment
`869e2606…`, no drop-ins, main argv equal to the fragment's literal `ExecStart`,
pinned script `35b6b436…` (the router hook's in-place replacement) at the
`4c85b6c2` source path
(`/srv/transparent-activity/ops/sources/<full-source-sha>/ops/transparent/scripts/transparent-live-fleet.py`),
with the exact `/usr/bin/python3 -B` prefix and resolved interpreter executable,
unchanged since the main process started, and direct
`ssh` children with the exact unmultiplexed control argv. A pending control passes
only if one such snapshot shows a client holding exactly the reverse connection,
one control per client; both snapshots are retained beside the raw reply before
any refusal. An address, key, name or argv alone never admits anything. Controls
carried by a shared `transparent-control-sessions` master, controls whose client
has exited (for example after the reconciler's preparation timeout), and any host
checking only itself, including worker source staging, still refuse. A changed
reconciler fragment or script, or a new worker `shard-control`, requires reviewing
new pins. Qualification owner surveys do not use this attribution and still
refuse such controls.

A locked bootstrap run retains every raw reply and attribution snapshot under
`input-staging/fleet-surveys/<request>/<nonce>/<host>[.attribution].json`.
Those replies list what the survey observed, with PIDs, start ticks, sessions
and cgroups: baseline services, the verified prototype and canonical load and
its native child, controls and reconciler clients. A retained survey is
forensic evidence, not ownership. The bootstrap survey still reads, bounds,
hashes and parses every file, and refuses links and malformed JSON. Its
`evidence` recognizer, `activity_bootstrap_fleet.retained`, accepts a file
whole, and only when all of these hold:

- It is in the input-staging namespace at exactly that path, with no
  directory before or after it, and the host is one of the five pinned hosts.
- A reply binding names that request, nonce and host, and its machine is
  that host's pin.
- A reply has exactly the shape `local` writes. That means the `observe` keys
  plus the bootstrap keys, the same bounds, classes, baseline, namespaces and
  inventory pin, and counts that agree with listings holding only retained
  summary fields. Its ancillary proof must have exactly its written keys and
  the current pins, checked at its own observation time, and its controls
  must agree with `pending`.
- An attribution is a worker's two snapshots in the exact shape `authority`
  writes, with the pinned reconciler fragment and script.

A recognized reply contributes only its lock holders, bound to its boot. Every
other file is walked as before, and every PID it names is a reference. That
includes each durable owner record: the lease or upload owner, with its fleet
proof `owner` and any embedded `result` or `surveys`, completed or not. Its
top-level identity, sessions, groups, cgroups, start windows and descendants
stay in force. Misplaced, edited or partial replies are walked the same way.

Recognized replies hide nothing a survey needs. A process a reply lists as
associated was associated through another retained record, which is still
walked. Unattributed, marked, pending and lock-holding processes are found
again from the live scan.

Limits: a pin change (bounds, classes, baseline, inventory, ancillary pins or
reconciler pins) makes older retained replies unrecognized, so later surveys
refuse again until root reviews that change. The shape and pins match those
written by every revision from `b82e111b` to `c0f4eee5`; replies from older
revisions are not recognized. A reply rewritten at its own path into another
exact shape can hide only PIDs that no other record names. Callers without
`evidence`, including qualification surveys, still treat every recorded PID as an
owner and refuse when retained replies name live services.
Worker replies are checked by control shape, so an executable repin keeps them
recognized; attribution files holding verified snapshots of an older script pin
are walked as owner records.

Read-only preflight checks every host without creating a lock or owner.
Use `schema-source-preflight --private-evidence-file FILE` on the local
coordinator client to preserve exact bounded wire replies even on refusal.
The file must be new, with no links in its path; creation precedes transport,
its final permissions are 0400, and survey contents are never included in the
refusal message. The remote helper retains at most five one-MiB replies in
memory, including the reply that fails verification; it writes no host file.
Truncated or malformed replies remain failure evidence, never clearance.
Refusal diagnostics identify only a reviewed component, function and line.

Stage first holds the coordinator production lock and persists its request, real
PID/start/boot identity and latest input fence. Only a new nonce bound to the
request, complete five-host replies, absence of foreign owners and verified lock
holder permit archive reception. Raw replies are retained privately before
refusal; the complete survey has a 300-second ceiling with 60-second remote
read limits. Worker source staging joins the coordinator's inherited lock,
receives the compact fresh coordinator proof, acquires its own host lock and
rechecks local owners. Full read-only preflight evidence is returned separately
from the bounded forwarded request. Inline source transport is compressed,
bounded to 512 KiB after decompression and SHA-256 verified before execution;
the entire SSH command remains below the Linux per-argument ceiling.

Transport timeout, nonzero SSH exit or invalid reply means unknown outcome
(exit 75). Observe both source receipt and coordinator owner before reconciliation;
no owner is signalled. Reconciliation requires its exact PID/start/boot to have
exited and a new locked complete-fleet survey. It retains the original failed
owner and moves only the matching failed source receipt to its immutable
abandoned name, preserving partial archive bytes. A new attempt is required.
Before promotion, the source receiver persists the independently hashed file
manifest. Recovery verifies that exact file set and any retained archive hash
before moving an inert failed promotion to an abandoned namespace. A durable
recovery intent permits resuming after either rename; failed receipts and partial
archives remain retained. A legacy failed promotion without a pre-promotion
manifest refuses. Completed source receipts still require full file-set verification.

Sources are retained under `/srv/transparent-activity/ops/sources/<SHA>` with
private receipts under `/srv/transparent-activity/ops/staging/`. The helper
verifies the received checksum and Git commit marker, rejects links, special
files, traversal and duplicates, bounds compressed/expanded data and entry count,
and enforces disk reserve, 20% disk headroom and 20% available memory. Completed staging is
idempotent only when every retained source file still matches. Failed or
interrupted receipts require reconciliation; neither a retry nor a new archive
may silently overwrite the retained source. Source staging does not activate a
service, switch a publication or establish canonical validation. Preserve these
sources and receipts as rollback material.
Invoke the staged wrapper with `/usr/bin/python3 -B` so imports do not alter its
verified file set. Schema phase subprocesses also receive
`PYTHONDONTWRITEBYTECODE=1`; their descendant programs must preserve it.


### Schema baseline dependencies

The trusted phase programs can invoke the checksum-bound
`tools/transparent/scripts/transparent-activity-baseline.py` dependency through the
schema runner. Its `capture` and `restore` actions require the inherited local
production-lock descriptor to name `/run/lock/wallet-pir-production.lock` and
require root. `verify` reads the retained private receipt. Do not invoke mutation
outside the wrapper or treat this file utility as the complete schema transaction.
The owner must quiesce every writer before capture/restore and bind the explicit
host plans and all imported program files in the reviewed recipe.

The private baseline copies mutable files independently, including executable,
unit/drop-in, controller/filter configuration and active-record/state targets
selected by the reviewed host plan. It preserves modes and ownership, records
optional missing targets, bounds copies to 1 GiB and 65,536 entries per host,
and checks that copying retains at least 20% disk headroom. SSH sockets, Python
bytecode and flock files are excluded. A completion receipt is fsynced only after
all selected bytes match their source. Partial, corrupted, extra-file and changed
retention snapshots fail closed. Large v10 journal/publication/cache trees must
remain outside candidate collection namespaces; identity and sentinel checks
fence these retained paths rather than copying or linking mutable state.
The direct `active.json`, `withdrawn.json` and `activation.json` files at a
retained publication root are copied independently; this exception permits only
regular files, including explicitly absent optional records. Nested paths,
symlinks, directories and table files cannot use it. Coordinator capture also
stops the filter writer before copying its store. Router baselines bind Caddy's
actual `/etc`, `/usr/lib` or `/lib` unit fragment, the absent `/etc` override and
both applicable drop-in directories; runtime fragment drift refuses preflight.

The `schema-input-cutover-{plan,preflight,stage,status,reconcile}` commands retain
two closed input bundles through the same locked immutable preparation path.
First supply `inventory.json`, nonempty `v10-sample.json` and `v11-sample.json`,
and the four actual passed gate reports named `artifact-verification.json`,
`native-certificates.json`, `independent-chain-oracle.json` and
`comprehensive-ci.json`. The inventory must equal the pinned staging inventory;
pending or mismatched reports refuse. Then supply only `product.json`, referring
to that retained bundle. Its complete product identities, inputs and service
units are checked before retention. Both requests use the service preparation's
source/release/attempt/files envelope, request digest and expected plan digest.
Neither stage captures a baseline or invokes a service phase. Reports must be
derived from retained results; a document shaped like a passing report supplies
no independent evidence. The full load fixture has a separate 2 MiB reader bound;
host plans and specifications keep their 256 KiB bound.

### SSH descendant lock qualification

Before the first service transaction, use `schema-lock-plan` and
`schema-lock-preflight` on the pinned root coordinator with the staged
`--source-sha`, reviewed `--host` and a new `--attempt`. The inventory must be
the retained coordinator input file, rather than stdin, because the bounded
relay rereads it. `schema-lock-qualify` requires `--expect-plan-sha256`, the
canonical digest of the plan. No service, routing or executable is changed.

The relay exits while a keeper retains the coordinator descriptor for real SSH. The remote
wrapper acquires its own host lock and exits while a child retains that lock
for fifteen seconds. Competing lock acquisitions must refuse on both machines;
the remote interrupted owner must also fence unrelated wrapper operations.
Private receipts and the SSH log live under the existing input-staging owner
namespace. Success requires child completion, released locks and reconciliation
on both hosts. `schema-lock-status` reports the retained coordinator owner;
`schema-lock-reconcile` resolves an interrupted test only after the remote and
coordinator locks can both be acquired. Never replay an owned attempt or infer
remote assurance from the local process tests. Reconcile failures before any
source staging or deployment; the shared input-owner fence remains in force.

OpenSSH closes inherited nonstandard descriptors at startup. Guarded transport
commands therefore run under a separate keeper which holds the descriptor until
SSH exits. A forked keeper survives termination or timeout of its launcher; the
transport receives normal signal handling and no descriptor environment claim.
The first real qualification reproduced the missing coordinator lock and was
reconciled on both hosts before this correction. Passing local keeper tests does
not replace rerunning the real SSH gate with the corrected staged source.

Restoration verifies the entire snapshot before changing any target and preserves
displaced candidate state alongside its original target on the live filesystem.
Routing, scaler and load files can be deferred until the canonical verifier passes.
An interrupted temporary restore or a second conflicting displaced state requires
explicit reconciliation. No file-level restore starts services or reopens origins.
Recovery must separately verify every advertised revision's accepted chain anchor,
binary/publication identity and exact private retrieval before restoring routing.
The actual complete host plans, service orchestration and reviewed cutover recipe
remain open; no production baseline has been captured with this dependency yet.

### Full v11 publication preparation

The deployment wrapper owns a separate preparation job, before any schema
maintenance. Use its `schema-publication-plan`, `schema-publication-preflight`,
`schema-publication-start` and `schema-publication-status` commands on the pinned
root coordinator. Pass the immutable staged operations `--source-sha` and
`--release-result-sha256`; start additionally requires the printed
`--expect-plan-sha256`. The frozen release receipt checksum is
`2f7fbd4a7dcddfe7be1853f697f2e541812b748479a4c305526742e0bd3c5ff8`.
Do not launch it until the existing ingest and independent guard finish.

The fixed job publishes `/srv/transparent-activity/full-v3/journal` from genesis
through 3500738 into `/srv/transparent-activity/full-v11/publications/initial`.
It derives the six-calendar-month boundary from independently accepted node
headers, keeps `recent-4k-8k` / `archive-wide`, choice tables `all`, and the v2
range profile. It uses the 18 retained fat-LTO artifacts at `12ce1291`, rechecking
receipt, dependency, compiler, profile and every executable hash. Native
publication is followed by complete artifact verification and four journal
rebuild samples, final independent genesis/anchor checks and measured allocation,
table counts and occupancy. Extraction oracle comparisons and native certificates
remain additional gates before service cutover.

`transparent-activity-full-publication-v11.service` owns the detached work with
400% CPU, 14 GiB MemoryHigh, 16 GiB MemoryMax, no swap/restart, Nice 10 and IOWeight
20. The child reacquires the production lock after bounded startup handoff and
repeats preflight before writing; its native descendants inherit production and
journal locks. Every five seconds the owner records memory and all three disk
fractions, stopping its own stage below 20%. The job does not switch units,
origins, scaler or load. Its private owner, per-stage PID/log/result, health and
terminal evidence are retained under `full-v11/preparation`. Any existing owner
or output refuses another start; failed or interrupted preparation requires
explicit reconciliation, preserving its evidence rather than automatic overwrite.
Job startup or preparation success is not canonical deployment or qualification.

The publisher, fleet subprocesses and shared SSH executor now pass inherited
production-lock descriptors and `PYTHONDONTWRITEBYTECODE=1` to children. A real
local grandchild fixture retained the lock after its parent and original owner
exited. This is local process evidence; qualification must also exercise the
actual SSH/remote phase boundary and reconcile surviving remote descendants
before explicit recovery after timeout or interruption.

### Product host transitions for the v11 recipe

The checksum-bound `tools/transparent/scripts/transparent-activity-host.py` now
implements host `preflight`, `capture`, `stage`, `activate`, `restore` and warm
worker identity checks. Run it only as a dependency of the schema wrapper recipe,
from its pinned immutable operations source on the reviewed machine. Mutations
require the inherited host production-lock descriptor. The coordinator must keep
its own production lock while invoking pinned SSH descendants; remote owners and
locks must be qualified before any live use.

A closed host plan binds role, machine ID, operations SHA, transaction ID, private
baseline plan and each install's source hash, target and mode. Coordinator
coverage includes binaries/units/drop-ins, controller/fleet/roster/credentials,
fleet and scaler state, both controller activation records and the entire load
tree and worker pins. Worker coverage includes both binaries, unit/drop-ins,
active/invalidation records and prestart helpers. The installed predecessor's
static publication, active publication, assignments and cache must be retained or
copied as applicable. Large journals/publications/caches keep their namespace
and sentinel fences. Units loaded from uncaptured vendor fragments or external
drop-ins are refused.

Capture first stops coordinator publication/reconciliation/control, scaler and
load writers and checks zero main PIDs **and empty cgroups**. The recipe then
captures the original router routing and quiescent warm worker state, before
public withdrawal. Workers must have no pending prepare/candidate operation and
match their durable active record; their bytes remain independently checked
against the saved copies while the services stay warm. It records prior unit
states privately beside the baseline, binding them to the host plan. The coordinator owner must keep all authority writers stopped throughout this
cross-host capture. Stage requires both origins withdrawn, verifies candidate publication
bytes with the native worker's `--verify-only`, stops and verifies empty cgroups
for every replaced product service (including the filter), then atomically installs retained
executables/configuration/units and displaces old unit drop-ins on their existing
filesystem. Worker units must agree on the separate v11 static publication,
assignment, active record, control socket and disk cache. Controller and fleet
services must use the separate v11 configuration/state/publication paths.

Recovery verifies the entire baseline before stopping services or restoring any
byte. It restores controller/filter/worker state and starts only previously
active authority units. Caddy, continuous load and scaler state remain deferred;
load and scaler remain stopped. Warm worker checks compare the **running**
executable hash, complete active publication identity, no preparing/candidate
state and every advertised revision anchor. Their output is input to the
coordinator's independent canonical-anchor and exact-query/SQLite verification;
it cannot authorize reopening. The quality supervisor is never started.

These are concrete host transition dependencies, **not a complete reviewed
fleet cutover**. Coordinated withdrawal/reopen, scaler observe policy, initial
fleet/controller activation records, all-worker prewarm, filter alignment,
installed-sandbox hard-link verification, independent canonical retrieval,
updated load fixtures, complete host plans/recipe and actual SSH descendant
qualification still gate production use. The full publication preparation and
native certificates/oracle gates precede maintenance. No host baseline or service
transition has been applied by these fixture checks.

### Guarded routing and reference recovery phases

`tools/transparent/scripts/transparent-activity-routing.py` implements the schema
recipe's withdrawal, private routing, verification, reopening and public
verification dependencies. Invoke only under the coordinator wrapper's inherited
production lock, with a closed plan binding both fleet configurations, original
Caddy bytes, complete coordinator baseline and checksum-bound v10/v11 samples.
Samples bind every worker executable independently; the compatible retained
`12ce1291` fat-LTO reader serves both schemas. These dependencies do not supply
the complete reviewed host plans, remote dispatch or cutover recipe.

Withdrawal guards the coordinator authority before changing router routes. Both
canonical metadata origins must return 503. A temporary Caddy relay bound only
to `127.0.0.1:18193` forwards metadata to the local candidate authority and PIR
setup/query traffic to the reviewed private router. It permits a real HTTP
reference-wallet recovery while public metadata stays withdrawn. One-shot fleet
phases use direct SSH without persistent control masters.

Verification requires every reviewed worker, including both recent replicas and
all archive owners, to attest its warm publication and running executable pin.
The HTTP worker scope and canonical assignment digest must also match the complete
reviewed assignment, including worker identity, role and assigned shard count.
Every advertised retained revision is compared with accepted node anchors. Each
authority manifest's actual response bytes must match its map digest, schema,
range, geometry, chain identity and preceding manifest. Sealed entries remain
identical across recovery and reopening; only a monotonically advancing tail
may change. A new failed verification retires its predecessor's passing proof.

The brief native recovery retains every report, attempt and SQLite store. An
independent read-only reopen checks the reader fence, source attribution, exact
nonempty events, agreeing transaction metadata, complete coverage, no pending
work and the committed accepted anchor. Unresolved-spend completions cannot
pass. Legacy v10 metadata remains unavailable. Each reviewed class must complete
exactly. Owner/result records are atomic, private and durable. This brief gate
does not replace sustained qualification or the raw-chain extraction oracle.

Reopening requires a complete baseline and a matching proof no older than five
minutes, then repeats live worker and anchor checks. After reopening, a fresh
reference recovery uses the two canonical HTTPS origins and independently
reopens its stores. Origin disagreement, router failure or public recovery
failure withdraws both origins again. Rollback restoration writes the old
fleet's maintenance fence before starting the restored authority; load and
scaler stay stopped until the outer approved phases restore them.

### Portable activity worker executables

The full-chain journal, publication tools and reference reader retain the 12ce
fat-LTO release identity. Worker server/control instead use a checksummed
`transparent-publisher` bundle from comprehensive main CI.

- **Selected now:** the bundle of the morning
  [2026-10-09 redeploy](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/fleet-redeploy-2026-10-09/README.md), full CI
  37921342549 at `a317455e9feecdd2639d5348188c6a2341819a4d`, built with
  `-C target-cpu=x86-64-v3`: `transparent-shard-server` (`34ba7ebb…`) and
  `shard-control` (`9208555a…`). Version-1 products also install its
  `transparent-publish-controller` (`13af048e…`) on the coordinator, and its v11
  `controller.json` must record `source_sha` `a317455e`, as the live one does.
  Production has since moved on: since 2026-10-10 the workers run `9d2cbda0`'s
  `transparent-shard-server` (`6ab86d6a…`) and `shard-control` (`ff3c0264…`), and the
  coordinator its `transparent-publish-controller` (`6041ea47…`) with `source_sha`
  `9d2cbda0` ([redeploy](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/fleet-redeploy-9d2cbda0-2026-10-10/README.md)).
- **Still 12ce on the coordinator:** filter, `shard-assign` and `shard-control`.
- **Staged for the 2026-10-03 cutover:** CI 36819961986 at `80c94f32`
  (`6db1fa05…`, `6dfe78fa…`). Its native Rust, Cargo and toolchain inputs
  matched 12ce.

The coordinator native-CPU build required instructions unavailable on the recent
workers and failed candidate verification with SIGILL. Preserve that failed
owner, native result and abandoned bytes after locked reconciliation.

Stage the portable executables through wrapper plan/preflight and
`preflight --stage` into checksum-named releases under
`/srv/transparent-activity/portable-workers/releases/<sha256>/<name>`, mode 0755,
before a version-1 worker-input, service-input or product preflight. A bundle
extracted for a manual redeploy is not this layout. This only prepares
candidate files. The input builder and product preflight pin the exact
executable hashes, modes and source paths; they refuse the coordinator-native
worker bytes. Use a fresh input request/attempt after reconciliation. Prove
native verification on an actual recent worker before starting the archive copy.
The original release receipt still binds publication/assignment tools; worker
file records bind the separately compiled portable artifacts. Never rewrite the
old build receipt or claim that reusing CI artifacts establishes fleet or load
qualification.

A recorded schema transaction's rollback runs from the operations source its
recipe was built with, and every product phase verifies that source's receipt.
An in-place edit to that tree therefore disables the recorded rollback until
the tree verifies again; the 2026-10-03 transaction's case is in
[remaining work](remaining-work.md#transparent-operations-after-the-2026-10-09-redeploy).

Candidate preparation executables and unit text are flat `.input-*` files beside
`shards.json`. The native server inspects every immediate child directory as a
shard, so an auxiliary `.inputs` directory is invalid. The first portable
verification exposed that error after the earlier SIGILL was fixed. Reconcile
that failed request through its retained source, preserve the partial copy and
use a new request/attempt; never weaken native shard discovery or bypass it.
The generic `shard-control --help` staging check returns a JSON EOF error because
this executable uses stdin JSON and a socket argument; retain that failed check.
Its actual read-only status protocol must be checked before service cutover.

## Public router and API versions

`https://transparent-pir.valargroup.dev` serves each wallet API version under its
own prefix ([architecture](architecture.md#api-versions)). Its router,
`transparent-pir-router-01`, holds no version of its own. It is a shell (both
sites, the 1 MB body bound, a catch-all 404, proxy errors with `Retry-After`)
around one route file per version in [tools/deploy/api](../../tools/transparent/deploy/api):

- [v2.caddy](../../tools/transparent/deploy/api/v2.caddy): `/v2/txid/*` goes to the one-query
  display site at `10.142.0.18:8095`, and every other `/v2/*` path goes to the
  v12 fleet's internal listener at `10.142.0.9:8080` with the upstream's own host
  header. The v12 fleet's `fleet.json` sets `"api_version": "v2"`, so its own
  adapter renders `/v2/` on `transparent-pir-v12.valargroup.dev` and on that
  listener.
- [v1.caddy](../../tools/transparent/deploy/api/v1.caddy): `/v1/*` answers 410 with a JSON body
  naming `/v2`, once v1 is retired.

`transparent-public-router.py` inlines the files in version order and installs
the result as one file through the adapter's swap step, which validates it,
keeps the previous file and restores it if the reload fails. A file may only
handle paths under its own prefix. Its config is a JSON file with `public_host`,
`internal_listen`, `router_host`, `ssh_key` and `known_hosts`; it runs from the
coordinator under the production lock:

- `render --config <file>` prints the router;
- `apply --config <file>` installs it;
- `--withdraw vN` answers that prefix 503 and leaves the others serving.

Adding a version means one more `v<N>.caddy` that sends `/v<N>/*` to the new
fleet's router, then `apply`. Each version's fleet keeps rendering its own
router; no fleet writes the public one. **Firewall:** the v12 worker's firewall
admits the router's tag on 8080 (`transparent-v12.tf`), and the display site's
worker rule admits it on 8095.

**Rollback:** reinstall the previous router file (`Caddyfile.live-previous` on
the router) and reload Caddy.

## Txid display one-query site (2026-10-12)

Vizor `main` ships the `txid-2k` client, which refuses the one-query map, so
the cutover below would leave those builds without private display. Instead
the `txid-2k8` lineage runs beside the live one, on its own host and public
hostname, `https://transparent-pir-txid.valargroup.dev`. Its release serves
`/v2/txid/`, and the public router forwards `/v2/txid/` on
`transparent-pir.valargroup.dev` to its worker port, so newer wallet builds use
the shared origin's `/v2` while `/v1/txid/` stays on the `txid-2k` lineage.

- **Host.** `transparent-pir-txid-k8-01` (`m-8vcpu-64gb-intel`), from
  `transparent-txid-display-site.tf` with `transparent_txid_display_site_enabled`.
  Its own firewall opens 80 and 443 publicly and 8095 to the coordinator, and
  Terraform owns its unproxied Cloudflare record. Caddy on the host serves the
  site; the router and its Caddyfile are untouched.
- **Request.** A site request (`site.name`, `site.hostname`) has no `router`,
  `terraform` or router host, and runs the combined layout. Its coordinator
  paths and unit names carry the name: `/opt/transparent-txid-display-k8`,
  `transparent-txid-display-k8-controller.service` and so on, with controller
  status port 8098. The site's Caddy handles, probes and map watcher use
  `/v2/txid/`; a router request keeps `/v1/txid/`. Its phases are `stage`, `workers`, `route` (the host's whole
  Caddyfile) and `controller`; `stop` and `retire` write the withdrawn site.
  The worker host's own paths are the usual ones, so it must hold no other
  display worker.
- **Profile and limits.** Since 2026-10-13 the site serves `txid-4k8` tables
  at the 8-bit display profile (release `e1f94158`, the code of
  `transparent-v0.1.0-rc.3`), with `archive_target` and `recent_window`
  180,000: 96 runtimes of 160.17 MiB, about 15 GiB. The worker keeps the 48G
  `MemoryMax`, 32 GiB cache and 40 GiB disk cache it had for `txid-2k8`'s 191
  runtimes. Moving a site to a release whose runtimes change runs `stage`,
  `stop`, `workers` (with any current `candidate-*` map; a worker that serves
  a map keeps it), `route` and `controller`; `/v2/txid/` is down while the
  worker rebuilds.
- **Geometry change.** A root fixes its geometry and seal, so a new geometry
  is a fresh root and lineage: bootstrap it, then `stop`, `workers
  --replace-active <served map>`, `route`, `controller`. Bootstrap cannot read
  the journal while the site's controller writes it, so the `txid-4k8` cutover
  bootstrapped from a hardlinked snapshot of the journal (its data files
  hardlinked, its checkpoint and metadata copied, its sidecar directory
  linked) through 100 blocks below the snapshot's end, while `txid-2k8` kept
  serving; the controller then replayed the rest. `/v2/txid/` was down about
  six minutes. The bootstrap reads the journal twice and is limited by the
  volume's read latency; warming the page cache ahead of it, 32 reads at a
  time, took it from 67 to 36 minutes
  ([evidence](../../evidence/txid/txid-display-4k8-2026-10-13/README.md)).
- **Journal.** A live controller appends to its journal, so the site needs its
  own: `/srv/txid-display-genesis/journal-v3`, a copy of `journal-v2x` taken
  while the `txid-2k` controller is stopped for the copy. The volume is ext4, so
  the copy is a full second journal (66 GB), not a reflink. Its root is
  `/srv/txid-display-genesis/v4/root` (`txid-4k8`); the `txid-2k8` root,
  `v3/root`, is kept for rollback.
- **Order.** `terraform apply` for the host and record; add the host to the
  deploy inventory and to the coordinator's publisher `known_hosts`; copy the
  journal; then `plan`, `preflight`, `stage`, `bootstrap`, `workers`, `route`,
  `controller`, `verify`. Nothing in this order stops or changes the `txid-2k`
  lineage except the controller pause for the copy.

Retiring the `txid-2k` lineage, once no supported wallet build uses it, is a
separate decision: its `retire` closes the shared port variable, which this
site does not need.

## Independent canaries under /v2

`pir-monitor` runs one canary per service once a minute
([observability](../enhance/observability-alerting.md)); only v12 history
and the one-query txid display are monitored, v1 is not.

- **History (`transparent`).** `quality-canary`, a binary in `transparent-tools`
  (`cargo build --release -p transparent-tools --bin quality-canary`). Its wallet transport already asks for
  `/v2/shards`, so `--url https://transparent-pir.valargroup.dev` reaches v12 with
  no extra flag. It refuses with `oracle_invalid` until it has a v12 fixture.
- **Txid display (`txid`).** `txid-quality-canary`, a binary in the service-owned
  `transparent-loadtest` package
  (`cargo build --release -p transparent-tools --bin txid-quality-canary`).
  `--url` defaults to `https://transparent-pir.valargroup.dev`; the client appends
  `/v2/txid/`. `--fixture FILE` holds `{"txid": "<display hex>", "height": N,
  "entry_sha256": "<hex>"}`: copy one archive-tier sample from a
  `txid-inventory fixture` document, built on the coordinator from the site's
  publication and heights index, never from a server answer. Each run is one
  cold private lookup that must return exactly that entry; it prints
  `{"passed", "category", "duration_seconds"}` and exits 1 otherwise. A different
  or missing entry is `answer_mismatch`; an unreadable fixture or a schema this
  build does not know is `oracle_invalid`; anything else (`placement_unknown`,
  `recent_below_floor`, `unavailable`, `protocol_failure`) counts as a failed
  request. It prints neither the txid nor any query material.

Add it to `PIR_MONITOR_SERVICE_PROBES_CONFIG` as
`{"service": "txid", "command": ["/opt/pir-quality/.../txid-quality-canary",
"--fixture", "/etc/pir-quality/txid-fixture.json"], "timeout_seconds": 45}`.

## Txid display one-query cutover (prepared 2026-10-11)

Superseded by the [one-query site](#txid-display-one-query-site-2026-10-12):
Vizor `main` already ships the `txid-2k` client.

Prepared, not run: Roman approves and runs it. Nothing below is deployed until
[status](status.md) records it. The layout and its gates are in
[txid display](../txid/txid-display.md#lookup-one-row-of-one-table-per-bucket) and the
[gate evidence](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/txid-display-2k8-gates-2026-10-11/README.md).

**Target.** A fresh display lineage from height 1, bootstrapped from the v2x
genesis journal (`/srv/txid-display-genesis/journal-v2x`) into a new root,
`/srv/txid-display-genesis/v3/root`:

- `txid-2k8`, one bucket, `archive_target` 90,000, `recent_window` 90,000,
  `reorg_margin` 100, `max_archive_shards` 1,000;
- about 189 archives plus the recent shard at the 2026-10-11 tip, 191
  runtimes of 80.09 MiB held (15.2 GiB with four builds in flight);
- one combined worker on `transparent-pir-txid-display-01` (`m-8vcpu-64gb`),
  unchanged limits: 40G `MemoryMax`, 24 GiB cache, 32 GiB disk cache, 4 build
  threads, 2 build slots;
- the controller on the coordinator, `replay-then-live`.

This is a direct cutover: the fleet has no users, so no client of the
`txid-2k` layout is kept working and the old lineage is not served beside the
new one.

**Request.** `request-v6.json` in the
[evidence](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/txid-display-2k8-gates-2026-10-11/deploy/README.md) is
the combined request with the new controller keys and paths. Its `release`,
`source_sha` and `baseline` are filled from the CI run of the merged commit
and from `txid-display-preflight`, as for the previous requests.

**Phases**, each through `wallet-pir-deploy.py txid-display-*` with the v6
request and its digest:

| Step | Command | Effect | Expected duration |
|---|---|---|---|
| 1 | `plan`, `preflight` | Review token; read-only checks | minutes |
| 2 | `deploy --phase stage --archive <bundle>` | Installs the release on the coordinator and display-01; nothing restarts | minutes |
| 3 | `stop` | `/v1/txid/` answers 503 and the old controller stops. **Outage starts** | under 1 min |
| 4 | `bootstrap --start-height 1 --through-height <journal end>` | Publishes about 189 archives and the recent window into the new root | 30–45 min (a count pass over 3.51M sidecars, then the builds) |
| 5 | `deploy --phase workers --publication-map-sha256 <new map> --replace-active <old map>` | Stops the worker, removes its active record (journaled), ships and verifies the new candidate, restarts it on the new release and prepares 191 runtimes cold | 10–20 min |
| 6 | `deploy --phase route` | Restores the display route. **Outage ends** | under 1 min |
| 7 | `deploy --phase controller` | Starts the controller on the new root; it replays to the tip | minutes |
| 8 | `verify`, then `status` | Rebuilds every seal from the journal into a scratch root and compares digests | 30–45 min, while serving |

Expected outage: about 40–65 minutes of display 503, from step 3 to step 6.
History is untouched.

**Checks after step 7.**
- The public recent map has schema `transparent-txid-display-shard-v3`,
  `recent_window` 90,000, a recent shard holding at least 90,000 entries that
  starts at or before `unsealed_start`, and every archive one segment.
- A found and an absent lookup with `transparent-txid-client` at the merged
  commit each send exactly one query to the right tier.
- The worker is ready and warm on 191 runtimes, at or above 20% available
  memory.

**Rollback material**, all kept until acceptance:
- the old root `/srv/txid-display-genesis/v2/root` and its active candidate,
  whose map digest `status` records before step 3;
- the v5 combined request and its staged release;
- the journaled `workers`, `route` and `controller` transactions of steps 5–7.

To roll back: `stop` with the v6 request, then `rollback` the controller,
route and workers transactions, newest first. That restores the old units and
the worker's old active record. If the worker has already collected the old
publication, run the v5 request's `workers` phase instead with
`--publication-map-sha256 <old active map> --replace-active <new map>`, which
ships the old candidate again, then its `route` and `controller` phases.

**What gets deleted.** The deploy deletes nothing. After the swap the worker's
own collection removes the old lineage's publication on display-01, and its
disk cache evicts old runtimes as new ones are written. Deleting the old root
on the coordinator, about 3.6 GB, needs Roman's separate approval after
acceptance.

## Txid display backfill below 3,407,001 (proposed)

Superseded on 2026-10-11 by the
[one-query cutover](#txid-display-one-query-cutover-prepared-2026-10-11): the
display layout is `txid-2k8` with 90,000-entry archives and a 90,000-entry
recent window. The figures below are the `txid-2k` proposal as made.

Proposed on 2026-10-07; nothing here is approved or deployed. The serving
target stays as recorded in
[status](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/docs/transparent/status.md#tiered-txid-display-in-production-2026-10-07). Roman chose
a **genesis** coverage floor. Counts, formulas and measurements come from the
[sizing evidence](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/txid-display-backfill-sizing-2026-10-07/README.md),
including its genesis addendum. Open gates are in
[remaining work](remaining-work.md#txid-display-backfill-below-3407001-proposed-2026-10-07).

Height 0 cannot be a display record. Bootstrap needs the display start above
the journal start, because the journal start's block is the display's parent.
So the journal starts at 0 and the display at 1. The genesis coinbase is the
one transaction left out, and it is unspendable.

### Recommendation

The [genesis journal census](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/txid-display-genesis-census-2026-10-07/README.md)
measured page demand on real data and settled the layout question. Old
archives hold many more large transactions than the live ones, so their page
tables need up to 9 segments of 2,048 rows rather than one. The earlier
estimate of 850 runtimes and 33.2 GiB was therefore too low.

**Recommended:** publish a fresh lineage from height 1 with today's layout on a
**dedicated `m-16vcpu-128gb` display archive host**, about $672 a month at list
price; check the price before approval. Three changes come first:

1. **Reservation true-up.** Charge each runtime its built size rather than the
   8-byte-word bound. Done in source; not yet released or deployed. Required:
   without it, today's layout needs a 127G `MemoryMax`, more than any host here.
2. **Split display map.** A small recent map plus immutable archive index
   chunks. Done in source.
3. **Ops support.** A larger window and cache cap, the new host, and a way to
   move workers to a new lineage.

Keep `txid-2k`, N=1, `archive_target` 40,000, `recent_floor` 10,000,
`reorg_margin` 100 and the 128-byte inline cutoff everywhere. Do not share page
tables. Set `max_archive_shards` to 1,000. Recent-01 keeps the recent role, and
archive-03 goes back to history alone.

What this gives:

- **Size at cutover.** About 425 sealed archives with 17.02M records. Their
  pages need 899 segments, so there are 1,325 runtimes.
- **Memory.** 51.8 GiB held at built size. With a year of growth (63.7 archives,
  5.0 GiB) the display unit needs a 71G `MemoryMax` against 100.9G usable on the
  host. That leaves about 5.9 years before a resize.
- **Disk on the host.** About 67 GB for tables and the disk runtime cache, on a
  400 GB disk.
- **Lookups.** Warm lookups cost 92.5 KB inline everywhere. A lookup of 4 pages
  costs 277.5 KB in single-segment archives, and up to 458 KB in the largest
  Sprout archive, which has 9 segments. Cold lookups with the split map cost
  about 120.5 KB inline.
- **Coverage.** Every transparent transaction since height 1. A 1,000-archive
  window lasts about 9 years at 63.7 seals a year, so nothing is dropped in
  practice.

**The one decision (G1)** is whether to pay that host's extra ~$336 a month, or
fund about two weeks of layout work to stay on the `m-8vcpu-64gb` host
(variant C below). Variant C:

- uses 80,000-record archives below NU6 (2,726,400), each with a 4,096-row
  directory and a new 8,192-row page geometry;
- holds 30.5 GiB and needs a 45G `MemoryMax`, so the 64 GB host needs a resize
  after about 1.9 years;
- costs 25 KB more on every pre-NU6 lookup;
- needs a client release that knows the new geometry;
- doubles the pre-NU6 anonymity sets.

Shared page tables do not help: under per-archive lookup they change no
anonymity class but add a response per segment of the group to every page
query, and they fit 64 GB only with 0.1G spare. Page tables smaller than 2,048
rows cannot be built.

Genesis does not fit archive-03 under any variant. Display can take at most
14G there (see [the archive-03 budget](#archive-03-budget)), and the smallest
genesis configuration needs a 44G `MemoryMax` at cutover plus a year.

### Measured runtime memory

`shard-residency`, extended to display geometries, built six runtimes of each
table on roman-dev-2 ([raw output](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/txid-display-backfill-sizing-2026-10-07/residency)).
The steady increments match the four-byte-word size rather than the
reservation:

| Geometry | Reservation per runtime | Four-byte size | Measured steady increments |
|---|---:|---:|---|
| `txid-2k` | 72.05 MiB | 40.05 MiB | 40.3–41.3 MiB (directory); pages within allocator noise |
| `txid-4k` | 80.05 MiB | 48.05 MiB | 48.2–48.3 MiB (both tables) |

Released workers charge every runtime its reservation. Without the true-up,
genesis needs 59.8 GiB held and an `m-16vcpu-128gb` host at about $672 a month.
With it (source only; no release carries it yet), the cache still
admits each build against the reservation but charges a built runtime what it
holds, so the worker's charge per runtime is:

| Geometry | Charged before | Charged after (built, four-byte words) | Measured steady increments |
|---|---:|---:|---|
| `txid-2k` | 72.05 MiB | 40.05 MiB | 40.3–41.3 MiB |
| `txid-4k` | 80.05 MiB | 48.05 MiB | 48.2–48.3 MiB |

At genesis the warm-fit check plans 1,325 `txid-2k` runtimes, one per
directory and page segment, at about 51.9 GiB. That is 51.8 GiB built, plus
one bound's excess for each of four runtimes in flight. The
rows were synthetic: the tool's premise is that the size depends only on the
geometry. P0 confirms this on real tables by reading the length of the display
disk-cache entries on archive-03.

### Host and memory options for genesis

From the [census layout analysis](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/txid-display-genesis-census-2026-10-07/README.md#per-era-options):
426 archives at cutover, plus one year of growth at 63.7 seals a year. "Usable"
means the host's memory minus the 20% floor and 1.5 GiB for the system: 49.7G
at 64 GB and 100.9G at 128 GB.

| Option | Runtimes | Held | `MemoryMax` with a year of growth | Fits | Reserved, no true-up | Disk | Pre-NU6 warm lookup, inline / 4 pages in the median archive |
|---|---:|---:|---:|---|---:|---:|---|
| **A. `txid-2k` (recommended)** | 1,325 | 51.8 GiB | 71G | 128 GB | 93.2 GiB, fits none | 67 GB | 92.5 / 277.5–345.3 KB |
| `txid-4k` everywhere | 1,014 | 47.6 GiB | 67G | 128 GB | 79.3 GiB, fits none | 68 GB | 117.6 / 352.8–375.3 KB |
| 2k directory, page geometry chosen per archive | 895 | 39.4 GiB | 56G | 128 GB | 67.4 GiB | 55 GB | 92.5 / 277.5–428.0 KB |
| B. 80k below NU6 as `txid-4k` | 756 | 34.2 GiB | 49G | 64 GB, 0.8G spare | 57.9 GiB | 48 GB | 117.6 / 352.8–443.1 KB |
| C. 80k below NU6, 4k directory, 8k pages | 590 | 30.5 GiB | 45G | 64 GB, 5.3G spare | 48.9 GiB | 46 GB | 117.6 / 453.1–498.3 KB |
| archive-03, any variant | — | — | needs ≥44G | no: 14G available | — | — | — |

The 4-page range runs from single-segment archives to the median Sprout
archive. In option A, every NU6-and-later lookup costs what it does today
(92.5 / 277.5 KB).

**Code for B and C.** Both need a seal rule, root and controller that take a
geometry and `archive_target` per height range; today all three take one per
root. C also needs a new display geometry and a client release, since today's
client returns `Unsupported` for a geometry it does not know. The worker already
serves several geometries in one set. B uses `txid-4k` and needs no client
change, but its 0.8G spare is too thin to plan on. Keep C as the lever for when
growth outgrows a host.

### The map

Today one map lists every shard: 632 B per entry, served uncompressed. A client
fetches it cold and again after every 409. Recent rebuilds move the recent
revision every block, so a wallet loop over recent transactions pays that
refetch often. At 426 entries:

| Design | Cold map | Refetch after a 409 | Cold inline lookup | Cold 4-page lookup |
|---|---:|---:|---:|---:|
| One map, uncompressed (today) | 259 KB | 259 KB | 364 KB | 569 KB |
| One map, gzip at the router | 60 KB | 60 KB | 164 KB | 370 KB |
| **Recent map + 32-entry archive index chunks, gzip** | 5.4 KB | 0.5 KB | 110 KB | 316 KB |

Gzip is a one-line `encode` in `txid-display-routes.caddy.in` and passes the
inline criterion. But a busy client still pays 60 KB per block crossing, and
the map grows by about 6 KB gzipped each year.

The proposed fix splits the map:

- **Recent map.** Small: the seal parameters, the start, the archive count, the
  digests of the index chunks, and the recent entry.
- **Index chunks.** Immutable, of 32 archives each, addressed by absolute shard
  id. A client caches a chunk forever and fetches only the one that covers its
  height.

A chunk discloses a 32-archive range, which is coarser than the shard id that
queries already name, so it adds no leakage. The work:

- the display map format and its shape check;
- the controller writing chunks on each seal;
- a worker route for chunks;
- `transparent-txid-client` support;
- the bandwidth formula, tests and a requalification.

Estimate: 2–4 days. The cold 4-page lookup stays above 300 KB, as it already
does today.

The split is built on `main` and not deployed
([measurements](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/docs/transparent/status.md#split-txid-display-map-2026-10-07)). At 426 entries
it costs 4.8 KB gzipped cold and 1.1 KB after a 409; the recent map's 14 chunk
digests make the refetch about twice the 0.5 KB sized above. Workers keep
serving `/v1/txid/shards` unchanged beside it.

### Ingest from genesis on the coordinator

- **Node.** `event-ingest --txid-display --start-height 0` reads the
  coordinator's Zebra state through `--state-dir` (`<cache_dir>/state/v29/mainnet`),
  the same input the full v3 journal was built from. The display journal must
  be new: the full v3 journal has no sidecars, and `display_at` refuses a block
  without one.
- **Disk.**
  - About 42.7 GB of events (120.6 B per event).
  - 3,509,640 sidecar files: at least 14.4 GB allocated at 4 KiB each, holding
    about 1.05 GB of payload.
  - About 11.1 GB for the publication root (1,325 segments of 8 MiB).
  - Recent revisions.

  Plan for at least 70 GB free with 20% headroom, beside the current display
  journal and root until acceptance.
- **Inodes.** At least 3.51M, all in one flat `display-v1/` directory, with
  three fsyncs per block (10.5M in total). Check `df -i` on the journal
  filesystem in P0.
- **Time.** At least 5.8 h at the full v3 history rate of 167 blocks/s.
  Sidecar fsyncs are unmeasured, so plan for 6–12 h. The ingest runs under the
  existing ingest unit limits beside the live display, which keeps serving.
- **Bootstrap.** A count pass over 3.51M sidecars, then about 6.4 min to publish
  425 shards, then `verify`, which repeats both. Worker prepare on the new host
  takes about 43–73 min cold, done serially: 1,325 runtimes at the 2026-10-05
  bench rates of 2.0–3.3 s each. That is within `ready_timeout_seconds` (at most
  7,200 s). Restarts warm from the disk runtime cache.

### Why a fresh lineage

Prepending archives to the live publication is not possible:

- **The journal cannot grow downward.** The display journal's start (3,407,000)
  is fixed in its `meta.json`, and `event-ingest` only appends.
- **The root is fixed at bootstrap.** `display-root.json` pins `start_height`,
  the seal parameters, `max_archive_shards` and the geometry. `bootstrap`
  refuses an existing root, and the controller's `run` reads the root file.
- **Shard ids and the chain start at the root.** Shard 0 starts at
  `start_height` and ids are contiguous. Each manifest chains its predecessor's
  terminal block hash and manifest digest. The controller halts if a later map
  changes a sealed entry.
- **Boundaries would not line up.** The seal rule is monotone forward from its
  start, so a lower start places boundaries that do not fall on 3,407,001.

The 13 live archives are therefore rebuilt under new ids. The live root is
fixed at 24 archives. Without a cutover it starts dropping archives, and its
floor rises above 3,407,001, around mid-December 2026.

### Effect on clients

At cutover the map's start drops to 1, shard ids restart at 0, and every
digest changes. Each worker prepares the new lineage beside the live one and
swaps at activation, so the service keeps answering. `transparent-txid-client`
follows the swap without downtime, as
[txid display](../txid/txid-display.md#a-fresh-publication) describes:

- A lookup on the old map is answered from the worker's retired snapshot while
  the old revision is resident. After that it gets a 409, refreshes the map and
  retries once on the new lineage.
- A height under the old start is `PlacementUnknown(Below)` from a cached map
  only while that map is less than 30 s old by the wall clock. An older map is
  fetched again first, as for a height above the tip.
- Native profiles and setup seeds are per geometry and do not change. Setups
  are per revision, so the new revisions' are fetched on first use.
- The first map of the new lineage drops the old lineage's cached manifests
  and setups.
- A registered geometry the cached init does not list, such as variant B's
  `txid-4k`, costs one init request once the serving init lists it, plus a map
  request when the lookup had not fetched the map. A geometry the client does
  not know, such as variant C's, still needs a client release.
- The new lineage must keep the network and genesis hash: the client refuses
  a map that changes them.
- A split-map client from before these rules also follows the 409. But it
  answers `Below` from its cached map until something else refreshes it, and
  returns `Unsupported` for a geometry its cached init lacks until the wallet
  replaces the client.
- A wallet row recorded as below coverage becomes eligible only once the wallet
  retries it. The brief says rows are re-armed when the map digest changes.
  That logic is in wallet-libraries and is not verified here. The digest also
  changes at every recent rebuild.
- A client built before the split keeps working through `/v1/txid/shards`,
  but pays the full map, about 259 KB at cutover, cold and after every 409.
  Wallets should carry the split-map client before the cutover.

### Code before any production step

1. **Reservation true-up** (`transparent-shard-server` runtime cache). Done in
   source: the cache reserves the bound before a build and releases the
   difference once the runtime exists; the disk cache and the warm-fit and
   disk preflight checks charge entries the same way. See
   [the architecture](architecture.md#serving-and-publication).
2. **Split display map**, as described in [The map](#the-map).
3. **Ops** (`tools/txid/lib/txid_display_poc.py`):
   - raise the `max_archive_shards` and `archives` caps from 64 to 1,024;
   - raise the per-unit `cache_bytes` cap from 64 GiB. The archive cache needs
     about 63.2 GiB with a year of growth; use 96 GiB with `MemoryMax` 71G or more;
   - refuse a request whose archive cache cannot hold its window at the
     true-up size with a 10% margin. Count each archive's directory and page
     segments from its manifest, not two runtimes per archive;
   - add `workers --replace-active <old map sha256>`. It backs up and removes a
     worker's active record, restarts it on new limits, prepares and activates
     the new candidate, and rolls back by restoring the record;
   - point the request's archive host and the route's archive upstream at the
     new host.
4. **Infrastructure** (`tools/infra/digitalocean/fleet`): an
   `m-16vcpu-128gb` display-archive droplet resource and its firewall rule for
   port 8095 from the router.
5. **`shard-residency`** reads display geometries. Done with code item 1: the
   measurement's
   [patch](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/txid-display-backfill-sizing-2026-10-07/residency/shard-residency-display.patch)
   is on `main`, and the tool also prints each runtime's charge after the
   true-up.

### Ordered steps

Each production step runs through `wallet-pir-deploy.py txid-display-*` or
the Terraform wrapper, holding the production lock and recording its rollback.

| Step | What happens | Duration | Rollback |
|---|---|---|---|
| P0 Preflight, read-only | Read archive-03's display disk-cache entry lengths to confirm four-byte runtimes on real tables. Read the coordinator's free disk, `df -i` and node state, and record the history W0 baseline. | minutes | none |
| P1 Provision (spend) | Terraform plan and apply for the display-archive host and its firewall rule. | about 30 min | Terraform destroy of the new resources |
| P2 `deploy --phase stage` | Install the release carrying code items 1–3 on the coordinator, the new host and recent-01. | minutes | `rollback` of the transaction |
| P3 Ingest from 0 | **Done** at 22:56:45 UTC on 2026-10-07 into `/srv/txid-display-genesis/journal`: 3,508,674 blocks and 17,024,724 display records ([census](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/txid-display-genesis-census-2026-10-07/README.md)). The census found a sidecar for every committed block, with exactly one coinbase each. Remaining: a spot check against independently decoded transactions. | done | none; the journal is inert |
| P4 `bootstrap --start-height 1 --through-height <journal end>`, then `verify` | Publish about 425 archives into a new root on the coordinator and reproduce every sealed digest. | about 15–40 min | none needed; the new root is unused |
| P5 Cutover, in one maintenance window | `stop` withdraws `/v1/txid/` and stops the old controller. `workers --replace-active` runs on the new host, then recent-01. `route` names the new archive upstream. The `controller` phase starts on the new root and catches up. | about 45–75 min of display 503, dominated by the cold prepare of 1,325 runtimes, unless the new host prepares before `stop`; history untouched | restore the old route, worker records and controller; `stop` withdraws in under 60 s |
| P6 `measure-start` | 20 QPS with samples spread from height 1 to the tip. | the existing criteria's duration | `measure-stop` |
| P7 Retire the display worker on archive-03, then delete the old journal and root | After acceptance only. | minutes | deletion is irreversible; separate approval |

Acceptance adds four checks to the existing
[criteria](remaining-work.md#tiered-txid-display-proof-of-concept-2026-10-05):

- sampled lookups in every era match an independent decode of the canonical
  transaction;
- cold and refetch map bytes match the split-map figures;
- the new host stays at or above 20% available memory;
- history p99 is within W0.

### Cost

- **Host:** about $672 a month for the `m-16vcpu-128gb` display-archive host
  (list price, to be checked). Variant C would keep it to the $336
  `m-8vcpu-64gb` for about two more weeks of engineering. Without the true-up,
  no listed host fits today's layout.
- **Coordinator:** 6–12 h of niced ingest CPU, and about 70 GB of disk while the
  old journal and root are kept.
- **Archive-03:** loses its display unit, freeing up to 6G for history.
- **Downtime:** one display outage of about 45–75 minutes.
- **Engineering:** code items 1–4, with CI and a release.

### Gates that need Roman

| Gate | Decision |
|---|---|
| G1 | **Spend:** approve the dedicated `m-16vcpu-128gb` display-archive host (about $672 a month) for genesis coverage with today's layout, or fund variant C (about two weeks) to use `m-8vcpu-64gb` (about $336) |
| G2 | Merge code items 1–4 after full CI and cut a release |
| G3 | Approve this change sheet for P0–P6, including the outage and the route change |
| G4 | Live acceptance, then keep, stop or retire (the proof of concept's existing gate) |
| G5 | Retire the archive-03 display worker and delete the old display journal and root |

### Archive-03 budget

Archive-03 is shared with the history archive owner. That owner has a 48 GiB
cache, 41.35 GiB of it reserved, with `MemoryMax` 56G. It was observed at about
52% available on 2026-09-29. Taking off the 20% floor and leaving room for
history to fill its cache leaves at most a 14G display `MemoryMax`. At the
reservation, that is a 75-archive window, which today reaches back to about
2.82M. With the true-up it is about 135 archives, about 2.0–2.1M.

That is the most a no-spend option can cover, and it is the fallback if G1 is
declined. The first-pass sizing for that fallback (a 3,000,000 floor) remains
in the evidence.

This plan does not build a frozen-prefix lineage, per-height geometry, a second
display archive owner, client changes beyond the split map, or any wallet
change.

## Binary upgrades of the current v12 history and 4k8 display fleets

`tools/scripts/wallet-pir-deploy.py transparent-binary-{plan,preflight,baseline,deploy,status,rollback}
--request F --request-sha256 H` (`tools/transparent/lib/binary_upgrade.py`) changes the workers'
executable per transaction through the shared engine: immutable `releases/<binary sha256>` staging
under the service root (for example `/opt/transparent-publisher`), the managed ExecStart drop-in,
the production lock, the service's journal sequence and touched-target rollback.

HTTPS History requests may set `selected_workers` to a nonempty list of distinct
keys from `workers`. Keep the complete fleet and its worker pins in the request;
only the selected workers are changed, while all workers and the publisher still
pass the normal checks. Omitting this field selects every worker.
The optional `publication.publication_root` pins a reviewed prepared-storage
directory from the already pinned publisher configuration, instead of the
original publication directory. A mismatch or a relative directory refuses.

For a recent worker already refusing history after a reorganization,
`unready_predecessors` may map a selected worker key to
`{binary_sha256, invocation_id}`. Both pins must match its running predecessor,
and `/v2/shards/init` must return 503 with `publication invalidated by reorg`.
When a restarted owner has not opened that listener yet, a nonempty valid
`active.invalid.json` in its pinned provider root supplies the revocation evidence.
This exception admits only that predecessor; its replacement must meet the
current-publication and warm-readiness requirements. It cannot combine with
`repair`, which remains specific to a recorded failed rollback.

- **Unified owner.** New transactions run only when the inventory's `publisher.managed_products`
  names the product: the one `pir-publisher.service` server on `controller.host` is the controller
  authority; pass `--descriptors tools/deploy/publisher/transparent-services.toml`. The request pins
  that unit and `/etc/wallet-pir/publisher.json`; the enabled product section holds the settings
  checked below, and its `listen` (history) or `status_listen` (display) serves the product's
  `/v1/status`. Only `workers` transactions run, never restarting the publisher, and `baseline`
  records only the workers; a `controller` request refuses and the publisher executable changes
  through `publisher-cutover-*` ([publisher](../publisher.md)). A standalone controller still
  running or still in the inventory beside it refuses.
- **History providers.** The publisher distributes History only over HTTPS, so its History workers
  are autonomous providers: each pins its `--config` file (`config_sha256`) and unit (`unit_sha256`),
  the request has `load: null`, and fleet-controlled worker pins refuse. Every provider's publisher
  must serve a compatible discovery document.
- **Release identity** is the request's `release.git_sha` and `release.binary_sha256`; a mutating
  command also requires the installed operations package to name that Git revision.
- **Publication identity** is pinned separately: v12 `publication.source_sha` (the History
  section's `source_sha`, for example `fb90519b`), or the 4k8 display `root`
  (`<root>/display-root.json`, pinned under `controller.files`, fixes txid-4k8); the publisher's
  normalized effective unit and the digests of its configuration; each worker's ready identity
  (display: `role` `combined`), cache/limit unit properties and argument pairs.
- **Current authority.** The publisher's map advances during a rollout, so nothing map-dependent is
  pinned. A worker's `map_sha256` must equal the current map (history `/v1/status` `map_sha256` with
  phase `serving`; display `published.map_sha256` with phase `running`, `halted` null and `stalled`
  false), read before and after the worker. A recent History provider's assignment fields must match
  the `assignment.json` its own active publication records; `warm_runtimes` must equal
  `target_runtimes` with no `prewarm_failed`. Display workers must be `ready`, `warm` and report
  `covered_through`. After a worker becomes HTTP-ready, the operator waits within the authority's
  readiness budget for publication to recover, then repeats the publication and ownership checks.
  An authority that remains stalled causes rollback; worker HTTP readiness alone cannot commit the
  upgrade.
- History refuses unless its section holds `follow_journal: false`, the full-v3 journal and the
  full-v12 publication root. Display refuses unless its section names the pinned `root`.
- Worker readiness uses the existing product warmup budgets: 1,800 seconds for history and 7,200
  seconds for the 4k8 Display request. Full warm readiness remains required; the generic binary
  engine's 180-second default is insufficient for the documented archive restore times.
- **Retired standalone controllers.** `transparent-v12-publish-controller.service` and
  `transparent-txid-display-k8-controller.service` drove their workers directly; neither executable
  exists in this repository any more. Against such a topology only `status` and `rollback` of a
  transaction it already recorded run (`plan`, `preflight`, `baseline` and `deploy` refuse before
  any host change). The rollback keeps the pinned unit, file and settings checks and allows an
  unhealthy process to be restored. A recorded History worker transaction carries its continuous
  load pins (`load.host`, `load.resume_after_upgrade` and the `pins.json` digest of
  `/srv/transparent-activity/canonical-load/v12`): its explicit rollback stops
  `transparent-v2-5qps-continuous.service`, restores the workers, writes pins naming the restored
  binaries (journaled with both digests) and leaves the load stopped until the exact-answer gate
  passes. The latch and incident records are never removed. The former `staging_adapter` request
  field, which changed only a standalone History controller's `fleet_command`, is no longer
  accepted.
- **Alert delivery is reported, not inferred.** `plan`, `preflight` and `status` print `notifications:` lines
  for this service:
  - the `alert_mode` configured in `/etc/pir-quality/transparent-production.json`, labelled as read only at
    APM start;
  - the mode the running APM last evaluated in, from its incident database;
  - a warning when the two disagree;
  - delivery health from APM's `monitor-status`.

  Anything unreadable is `unknown`. When the running mode is not `active`, the output says that incidents
  during the upgrade are recorded only. pir-apm keeps running through a worker upgrade. Once a service is
  active, the unavailability a restart causes fires and recovers real incidents (archive warm-up takes
  minutes). The tool sends no deployment notification of its own.
- Supported topology only: history workers exactly `transparent-v12-archive.service` and
  `transparent-v12-recent.service`; display worker exactly `transparent-txid-display-worker.service`.
  External workers or other units refuse.
- The checks run in preflight, under the lock before any change, after every restart (a changed
  identity or an unhealthy authority fails the transaction and rolls back every touched target) and
  after rollback. Publications, assignments, journals and routing are never written; unit drift
  beyond the binary refuses.
- Tests: `tools/transparent/tests/test_binary_upgrade.py`, run by `make check-ops-deploy` through
  `tools/tests/operations/deploy/test_transparent_binary_upgrade.py`.

## Recent provider containment and offline collection

`transparent-provider-maintenance-{plan,preflight,deploy,status}` operates only on
the installed `transparent-v12-recent.service`. Requests use schema
`transparent-provider-maintenance/1` and name `git_sha`, `worker` (the qualified
inventory key), `action`, `expected`, and `retained_publications` (absolute rollback
publication paths, or an empty list). `expected` pins `unit_sha256` (the normalized
effective unit digest), `config_sha256`, `binary_sha256` and `invocation_id` (null
only when systemd reports no invocation). The operations release must match
`git_sha`. Pass `--request` and `--request-sha256` to every command; deploy also
requires `--expect-plan-sha256`.

- `action: quiesce` stops only the exact pinned invocation already returning the
  reorganization refusal above. Archive must remain ready. This contains growth
  while the replacement is validated; it does not establish recovery.
- `action: collect` requires the recent worker stopped, no unfinished History
  upgrade, and exclusive bootstrap and source-catalog locks. Plan/preflight with
  `--output FILE` saves the complete private deletion manifest. Retain the saved
  active publication and its inputs, explicit rollback publications, and three
  newest unused trees. Collection removes older trees, then unreferenced catalog
  revisions, then unlinked objects. It preserves safety records and foreign files,
  refuses malformed metadata, and never follows publication symlinks. Changed
  manifest identities refuse before deletion. Deleted cache bytes have no rollback.
- `action: resume` starts the unchanged pinned predecessor after collection. It
  does not assert readiness. Capture its new invocation and use a recent-only
  binary-upgrade request to install the validated fix; do not leave the old retry
  loop running unattended.

Each action is a separate approved, journaled transaction under the production
lock. Recheck the unit and configuration pins between actions. On interruption,
inspect `status` and actual process state before continuing. An unknown stop or
partial collection must not trigger an automatic restart. Neither archive nor
publisher is restarted; no incident latch or safety floor is reset.

The autonomous worker also collects failed publication attempts before allocating
another tree and after a refused pass. It protects active, retained, held and
candidate trees plus three unused trees. A blocked collector refuses further
staging; successful activation is not required to reclaim abandoned versions.
Check filesystem free space rather than summing hard-linked directory sizes.

## Continuous load repair

`transparent-load-repair-{plan,preflight,deploy,status,rollback}` (`tools/transparent/lib/load_repair.py`)
restores a stopped `transparent-v2-5qps-continuous.service` when its checker script is at fault. It
installs this operations release's `transparent-quality-load.py` at the immutable
`/srv/transparent-activity/ops/load-scripts/<sha256>/` and changes only the script path in the
file that owns the effective `ExecStart`. On the coordinator that is the publisher's
`zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz-publisher.conf` drop-in, not the fragment. The rewritten
effective unit must equal the current one except for that path, so arguments, limits and controller
bindings are kept. It starts only that unit. The request (`transparent-load-repair/1`) pins:

- the fragment bytes, every drop-in as `previous.drop_ins` `{path: sha256}` (`{}` for none), and
  the script the effective `ExecStart` runs now;
- the load's `pins.json`, `rate-query` and `fixture.json`;
- each pinned worker's `{host, unit}`;
- an independent encrypted-answer `exact_check` `{host, argv, timeout}`. Copy it from the
  inventory's History exact check.

If a prior upgrade left stale load pins, `load.replacement_pins` may explicitly map
every worker ID to its reviewed running binary hash. The operation still pins the
old `pins.json` bytes and refuses worker or input drift. It writes replacement pins
only after the independent encrypted-answer check, alongside the repaired unit file.
Rollback restores both previous files and leaves the checker stopped. Without this
field, the existing worker pins must match and are never changed.

Plan and preflight are read-only; preflight also runs the exact check. Deploy takes the reviewed
`plan_sha256` and holds the production lock. It refuses a `latched.json`, a worker that is not
running its pinned binary, changed load inputs or unit files, a running unit or an unfinished
transaction. It then stages the script and runs the exact check, and only after that writes the
owning file and starts the unit. It waits up to 300 s for `status.json` to report `running`. A
failure or `rollback` stops the unit and restores the journaled previous bytes of that file;
rollback refuses a file changed outside the transaction unless `--force`. The fragment and other
drop-ins, old script, pins, permit, latch and incident logs are never written. Run from an unpacked
operations release, deploy requires that release to be `script.git_sha`; a checkout without
`operations.json` is accepted as for other recipes.

```sh
D=tools/scripts/wallet-pir-deploy.py; R=(--request repair.json --request-sha256 "$(sha256sum < repair.json | cut -d' ' -f1)")
$D --inventory "$INV" --state-dir "$STATE" transparent-load-repair-plan "${R[@]}"
$D --inventory "$INV" --state-dir "$STATE" transparent-load-repair-preflight "${R[@]}"
$D --inventory "$INV" --state-dir "$STATE" transparent-load-repair-deploy "${R[@]}" --expect-plan-sha256 <plan_sha256>
$D --inventory "$INV" --state-dir "$STATE" transparent-load-repair-status "${R[@]}"
```

Tests: `tools/transparent/tests/test_load_repair.py`, in `make check-ops-deploy`.

## Transparent alert activation

`transparent-alerts-{plan,preflight,deploy,status,rollback,test}` (`tools/transparent/lib/alert_activation.py`)
is the supported way to upgrade the coordinator's running pir-apm and change only these alert modes:

- the `alert_mode` of the named services (`transparent`, `txid`) in pir-apm's Transparent configuration;
- the external pir-monitor's `PIR_MONITOR_SERVICE_ALERT_MODE`.

The second setting is one family covering the Receiver, Status, Transparent and txid probes together. The
monitor's global `PIR_MONITOR_ALERT_MODE` and pir-apm's other families are not changed.

Production activated Transparent and txid quality alerts and the external service probes on
2026-10-19 at about 17:39 UTC with source `653793f985c5b17622d581e4d616a341c2aea613`.
Transaction `transparent-alerts-20261019T173754Z-ea059a45242d-4d2ecc` committed; subsequent status verified
both running modes and preserved credential wrappers. The APM binary is
`ea059a45242dd83ac60db8569a7828ce841840a0670a8f88ad1e50d2fce5c0cd`. Operations qualification
[39293602410](https://github.com/valargroup/wallet-pir/actions/runs/39293602410), main CI, and all 154 APM
and 5 monitor tests passed at that source. The binary came from the successful release-build job in
[39293606439](https://github.com/valargroup/wallet-pir/actions/runs/39293606439); the unrelated Enhance
publisher lifecycle timeout was explicitly excluded from this APM-only acceptance. Full Enhance
qualification is not claimed.

The isolated live-source check passed before activation. Final checks found ready workers, fresh canonical
publications agreeing across origins, four successful public exact probes, no active disabled-scaler incident,
and empty outboxes. Both labelled firing/recovery pairs reached Slack:
[APM](https://zcashzcaloors.slack.com/archives/C0C3L7DDKC2/p1792431633161929) and
[external monitor](https://zcashzcaloors.slack.com/archives/C0C3L7DDKC2/p1792431673654559).
Capacity and scaling modes retain their prior shadow settings. The 24-hour active observation window
starts at activation; the delivery tests do not complete that window or the separate policy-threshold drills.
Rollback uses the pinned request and the committed transaction above, with the command shown below.

The request (`transparent-alert-activation/1`, bound by `--request-sha256`) has two components. `apm` is
`pir-apm.service` and `monitor` is `pir-monitor.service`; `monitor` may be `null`. Each component pins:

- the host;
- `unit_files_sha256`, the exact loaded fragment and drop-ins;
- `wrapper_sha256`, the `credential-exec.py` digest;
- `running_sha256`, the executable after the wrapper execs it;
- its incident database (`state_path`) and loopback `listen`, which must equal the running process's own
  `*_STATE_PATH` and `*_LISTEN`;
- the `environment` label for test messages.

`apm` also pins:

- `release`: `git_sha` (40 lowercase hex) and `binary_sha256`;
- `config`: `path` (which must be the running `PIR_APM_TRANSPARENT_CONFIG`), `previous_sha256`, and
  `alert_modes`, which names only `transparent` and `txid`.

`monitor` also pins `service_alert_mode`.

Every host observation goes through `alert_host_probe.py`. Its source is sent to `python3 -c` on the host, and it
prints only small whitelisted JSON fields:

- the running process's settings, read from `/proc/<pid>/environ` and limited to path, listen and mode keys;
- each family's mode from the incident database, opened read-only;
- a summary of `monitor-status`, read with a 16 MiB bound. An oversized or invalid body is an error, never a
  truncated parse. Delivery failures are reported as present or absent, never as text.

Secrets in the environment or in error messages never leave the host. `plan`, `preflight` and `status` print the
observed values next to the pins.

What a deploy changes:

- **pir-apm.** The release is staged immutably at `/opt/pir-quality/releases/<sha256>/pir-apm` and must pass
  `--help` there. Before any live write, under the lock, it then runs as an isolated shadow process beside the
  running one:
  - it uses the running process's nonsecret `PIR_APM_*` settings, so it reads the real configured sources;
  - its state, history and configuration copy are temporary, with every mode set to `shadow`;
  - it uses loopback port 3092, has no webhook and no scaler, and runs as the unit's user;
  - it is stopped and removed within 120 s.

  It must evaluate with the Transparent configuration enabled and record `shadow` for `transparent` and `txid`.
  Through its `/api/quality`, with the same bound, each service must also have:
  - at least one configured source;
  - every source sampled without error, within APM's own freshness limits (45 s, or 90 s for the independent
    probe);
  - every source that reports readiness ready;
  - a publication sampled within 90 s, canonical and agreed across origins.

  Only source names reach the runner, never their error text. If any of this fails, the deploy fails with nothing
  live changed.

  Only the final binary argument of `python3 <credential-exec.py> <pir-apm>` changes, through the managed
  `zzz…-wallet-pir-release.conf` ExecStart drop-in. A deploy refuses when:
  - any other unit setting would change;
  - a later drop-in sets ExecStart;
  - the unit does not run through the wrapper.

  The configuration changes only the named services' `alert_mode`.
- **pir-monitor.** A deploy writes `/etc/pir-monitor/service-alert-mode.env` and the drop-in
  `pir-monitor.service.d/95-service-alert-mode.conf`, which adds that file as the last `EnvironmentFile` so it
  applies over `Environment=` and `monitor.env`. Credential drop-ins, wrappers, `monitor.env` and every SQLite
  database are left as they are.
- **Under the production lock.** The deploy checks the schema fence, then reruns every pin and process-binding
  check. The runner journal records each previous byte and the previous running modes before any write. Each
  component is restarted and verified in turn:
  - a new systemd invocation running the expected executable, still bound to the request's paths;
  - an `evaluated_at` later than before the restart;
  - `progress_ok`, working storage, and configured delivery with no failure;
  - the requested running modes.

  A lost restart reply is recorded as uncertain and resolved by observation. Any failure after a write restores
  every touched component. Each restored process must have evaluated again and recorded its previous modes; its
  delivery health is reported but not required.
- **Rollback.** `rollback` restores a committed or interrupted transaction. It refuses when a file changed after
  the transaction, unless given `--force` after review.
- **Status.** With the same request, `status` checks the live state against that request's committed transaction
  (new state) or rolled-back transaction (previous state), not against the pre-deploy pins, and reports drift.
  Without a finished transaction, it checks the pins.

`test --component apm|monitor` sends real labelled test messages:

- It queues one labelled `PIR test · FIRED` / `RECOVERED` pair into that component's existing outbox. It runs the
  checksum-qualified `pir-apm --enqueue-test-alert` through `runuser` as the unit's `User` and `Group`, so no
  root-owned WAL or SHM file is created. The command writes the outbox and exits without starting a daemon.
- The monitor host gets a copy of that binary at `/opt/pir-monitor/tools/<sha256>/pir-apm`.
- The running service's own delivery worker sends the messages: pir-monitor reads `PIR_APM_SLACK_WEBHOOK_URL`
  through its wrapper.
- The command waits until `monitor-status` shows the queue drained with a newer `last_success`. Messages that are
  not delivered stay queued in the durable outbox, and the command fails.

Production order, from the operator's checkout of the qualified revision, with the reviewed request:

```bash
D=tools/scripts/wallet-pir-deploy.py; R=(--request activation.json --request-sha256 "$(sha256sum activation.json | cut -d' ' -f1)")
$D --inventory "$INV" --state-dir "$STATE" transparent-alerts-plan "${R[@]}"
$D --inventory "$INV" --state-dir "$STATE" transparent-alerts-preflight "${R[@]}" --binary pir-apm
$D --inventory "$INV" --state-dir "$STATE" transparent-alerts-deploy "${R[@]}" --binary pir-apm
$D --inventory "$INV" --state-dir "$STATE" transparent-alerts-test "${R[@]}" --component apm
$D --inventory "$INV" --state-dir "$STATE" transparent-alerts-test "${R[@]}" --component monitor
$D --inventory "$INV" --state-dir "$STATE" transparent-alerts-status "${R[@]}"
# Back out (restores binary selection, configuration, drop-ins and processes):
$D --inventory "$INV" --state-dir "$STATE" transparent-alerts-rollback "${R[@]}"
```

The monitor host is reached through the coordinator (`"jump": "coordinator"` in the inventory). Rolling back to
shadow without changing the binary uses a new request with `alert_modes` and `service_alert_mode` set to
`shadow` and `release.binary_sha256` equal to the running binary.

Once a service is active, a Transparent maintenance restart pages for real: archive warm-up takes minutes and the
public path is not uninterrupted.

Tests: `tools/transparent/tests/test_alert_activation.py`, through
`tools/tests/operations/deploy/test_transparent_alert_activation.py` in `make check-ops-deploy`.

Configure the inventory's `exact_check` with the product's encrypted independent-answer probe.
Deployment runs it after restart and rolls back on failure. After the continuous load resumes,
verify its health and run the existing public query gate before declaring the rollout complete.
