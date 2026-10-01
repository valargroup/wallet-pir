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

## Target configuration

| Parameter | Recent | Archive |
|---|---|---|
| Initial range | Last six calendar months of pinned anchor time | Genesis through block before recent range |
| Selected profile | `recent-4k-8k` | `archive-wide` |
| Directory/page rows | 4096 / 8192 | 32768 / 65536 |
| Row bytes; inline events | 4096; 2 | 4096; 2 |
| V10 seal target:capacity (directory bytes) | `14366428:16760832` | `114931420:134086656` |
| V10 absolute script target:capacity | `203630:237568` | `1629038:1900544` |
| Page-row target:capacity | `7936:8192` | `63488:65536` |
| Workers | 2 full recent replicas, plus elastic copies under load (up to `max_recent`) | 1 static owner holding every archive shard (0–76) |
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
| Warm state | All assigned tables and advertised tail; budget retained tail runtimes | Entire current assignment before fleet readiness |

These are proposed operating budgets within the accepted architecture, not target-host RSS measurements. Cache reservations omit overhead. Verify full-process and cgroup memory during cold builds, queued HTTP traffic, revision overlap and restart. Do not raise limits automatically to fit a failed census.

Fleet configuration lives in one place: a roster (repository variable `TRANSPARENT_FLEET_JSON`, one entry per worker with `id`, `role`, `replica_group`, `ssh_host`, `upstream`, `cache_bytes`, `memory_max` and optionally `build_slots`, default 1) and the assignment `shard-assign plan` derives from it and the published set at the recorded cutoff. The assignment is the durable record of who holds what; its digest is reported by every worker and asserted by the deploy. The router's Caddyfile is rendered from the assignment alone. Worker units are rendered from the committed template with the roster's cache and memory budgets and `--assignment … --worker-id … --prune-excess`.

Use one 2 vCPU / 4 GiB routing host initially. This is a single point of failure. The existing chain node/indexer/publisher remains separate from retrieval capacity budgeting. Keep archive restores off recent workers. Serve immutable public filters and setup from an object/CDN origin, with a refreshable map; verify cross-origin map consistency and retain an independent wallet anchor source.

### Elastic recent replicas

The accepted recent tier is two full copies; four hosts was a load target, not a
requirement. Two replicas passed the 20 QPS gate at 20.9 QPS, p99 53 ms, with
runtime construction in its own low-priority pool
([evidence](../evidence/recent-floor-2026-09-29/README.md)). Elastic copies are
added and removed by the actuator from `ops/infra/digitalocean/transparent-elastic/`
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
- `transparent-archive-standby.py`, then `transparent-fleet-inventory.py --archive
  repartition|restore`: replace archive owners; see
  [archive owner changes](#archive-owner-changes).

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

Uniform full-chain `archive-wide` evidence is 162 shards and 57.1 decimal GB plaintext. Applying measured c-8 per-shard RSS gives approximately 93.2 GiB prepared residency, or 81 shards / 46.6 GiB per half. This is a sizing proxy, not the mixed-tier census. The mixed-tier archive is 77 shards (0–76). A single-owner prototype on 2026-09-29 reserved 41.35 GiB of its 48 GiB cache for all of them at planning headroom 0.05 (the live adapter's; 0.15 refuses it), with about 36 GiB resident. One owner therefore holds the whole archive: 48 GiB runtime cache in RAM, 96 GiB on disk, `MemoryMax` 56G, `MemoryHigh` 48 GiB, one build slot. In production (`transparent-pir-archive-03`, worker `a704616c`) it runs at 34.1 GiB RSS with 52% of host memory available, and at about 20 archive queries/s uses 1.4 of 8 cores and 12% of its query slots, with the same per-query evaluation time (about 11 ms) as the two-owner split ([evidence](../evidence/archive-consolidation-2026-09-29/README.md)).

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
2. **Standby.** Pin the new host's key (verified out of band) in a known_hosts
   file and run, from `/opt/transparent-publisher/releases/current/repo`:
   `transparent/ops/scripts/transparent-archive-standby.py --id transparent-pir-archive-NN --host <vpc ip> --droplet-id <id> --known-hosts <file> --release <sha>`.
   It checks the droplet id and x86-64-v3, installs the release against its
   SHA256SUMS, plans the active publication with the new host pinned to the
   whole archive, copies at `--bwlimit-kbps` (pausing while freshness is over
   20 s), starts the archive unit and waits (40 minutes by default) until the
   host attests the publication warm, then checks that the router reaches
   the host's `/v1/ready` (a reused VPC address can leave a stale neighbour
   entry on the router). Rerunning is safe: it hard-links what the host already
   holds. Qualify it with low-rate exact queries to its private `:8093` and
   record memory and warm time. Measured on 2026-09-29: 1,596 s cold at one
   build slot; 291 s to restart onto a newer publication from the disk runtime
   cache. The coordinator keeps only about nine publications, so a copy that
   outlives its source fails and is rerun; `repartition` checks the standby
   against the worker's own copy of the map it serves.
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
5 QPS load, two recent replicas, every query exact, [evidence](../evidence/archive-consolidation-2026-09-29/README.md)):

| Topology | Mixed 3 × 7 QPS: p50 / p99 | Archive-only 4 × 5 QPS: p50 / p99 |
|---|---|---|
| Two archive owners | 17 / 50 ms at 20.9 QPS | 16 / 33 ms at 19.5 QPS |
| One archive owner | 14 / 45 ms at 20.9 QPS | 18 / 35 ms at 19.6 QPS |

The cutover itself failed twelve synthetic archive queries over about twelve
seconds while the router could not yet dial the new owner; the standby tool now
checks that path first.

## Proposed wallet objectives from the 2026-09-08 fleet series

Proposed, not adopted: they come from three repetitions of the load series
against the activated fleet ([r1](../evidence/runs/fleet-series-2026-09-08-r1/README.md),
[r2](../evidence/runs/fleet-series-2026-09-08-r2/README.md),
[r3](../evidence/runs/fleet-series-2026-09-08-r3/README.md)) at 8 and 32
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

The hardening upgrader repairs legacy units missing both runtime-cache options
using the fleet generator's cache directory and a disk limit of twice the worker's
memory-cache reservation. Explicit cache settings are preserved; a partial pair
is rejected. Check disk headroom before activating this repair. The first build
populates the cache; only subsequent compatible starts benefit from restoration.

For planned hardening maintenance, installation is bounded at 40 minutes,
including staging and validation, with a 30-minute post-start warm-up allowance.
Rollback verification has a 30-minute recovery allowance. These operational
timeouts do not establish or relax the beta's 15-minute single-failure recovery
target, nor the canary freshness limits or observation durations. The original
failure is recorded before rollback begins, and public maintenance remains in
place until canonical warm service and exact private queries are verified.

1. Verify live state and journal identity. Pin source SHA, schema, anchor hash/time, explicit cutoff height and cutoff derivation algorithm in candidate metadata.
2. Use a new publication directory; never overwrite the rollback set or switch an existing schema in place. Publish through `.github/workflows/publish-transparent-shards.yml`, which takes the commit, journal, fresh output directory, anchor height, recorded `recent_from` and both geometries, re-derives the cutoff with `shard-cutoff --expect-cutoff` and refuses a disagreement, publishes `--through` the anchor, and stores `publication.json` and `cutoff.json` beside `shards.json`.
3. Validate complete coverage, manifests, tables, filters, true directory placement and exact replay before touching running services. Stage verified bytes at assigned owners.
4. Run `.github/workflows/deploy-transparent-shard.yml` in `fleet-preflight` with the tested main SHA, the set, `recent_from` = the recorded cutoff height and the roster variable set. It plans the assignment, ships each worker its subset, and has the staged binary load and verify that subset under the worker's own unit arguments while the running service is untouched.
5. Run `fleet-deploy`. The deploy compares each worker's actual binary, normalized unit, recorded unit digest, publication, stable worker assignment and warm readiness. Unchanged workers skip staging, subset verification and restart; helper-only changes install the tool without a restart. Changed workers stage and verify in parallel before activation. Archive owners activate first (serial by default; `owner_activation=parallel` accepts simultaneous archive interruption), then recent replicas in healthy pairs (`replica_activation=serial` restores single-worker batches). Each group retains at least half its members healthy; degraded groups reduce concurrency or block. A batch must finish readiness and the router health settling interval before the next batch. The router reloads only when effective configuration changes. Public/internal verification and a final readiness check include skipped workers. Pruning follows verification and protects rollback publications. Failures restore only hosts touched by the persisted deployment transaction; `fleet-rollback` restores that same transaction and checks previous readiness.
6. Canary real wallet recovery, tail republishing and outage handling, then expand only after workload gates pass. Record actual deployment state and measurement artifacts.
7. Roll back binary, publication and routing together. Preserve the previously valid revision/coverage relationship; a wallet must explicitly recover from an incompatible or lower anchor rather than silently accept it.

The filter service deploys on its own through `.github/workflows/deploy-transparent-filter.yml`, which asks the staged binary to read the named set (`--check-shard-dir`) before the running service is touched, compares that map digest with the set the fleet serves, and verifies both public origins afterwards. It uses the `enhance-production` concurrency group and the `/opt/enhance-pir/rollback` paths the removed Enhance workflow used, and touches nothing of Enhance. Do not copy a transparent Caddy configuration onto the Enhance coordinator.

## Runtime persistence and deployment identity

Fleet units enable `--runtime-cache-dir /srv/transparent-pir/runtime-cache` and
`--runtime-cache-max-bytes` at twice the configured RAM cache budget. Standalone
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

After the continuous-publisher workflow enables worker control sockets, binary
rollouts preserve its canonical control socket, active-record path and runtime
directory. This fixed-publication workflow requires the publisher to remain in
shadow and every controlled worker to serve the planned map and logical assignment. It refuses active
continuous publication and routing changes; publication advancement belongs to
the publisher. Binary rollouts leave controlled workers' prepared and retired
publication files and runtime cache entries intact. Coordinate a shadow-mode
validation window before activation of the publisher.

The runner retains transaction JSON under
`~/.local/state/transparent-pir-deploy` (override with
`TRANSPARENT_TRANSACTION_DIR`); each touched host retains its previous files under
`/opt/transparent-pir/transactions/<transaction-id>`. An interrupted transaction is
recoverable via `fleet-rollback`; a no-op does not replace the last rollback target.
The workflow preserves `deploy-plan.json` and `deploy-timings.tsv`. Readiness
reports disk cache bytes/hits/misses/write failures, and Prometheus reports restore
counts and cumulative restore time separately from cold builds.

Acceptance is a measured compatible-binary deployment below ten minutes for the
deployment phases, excluding build/CI time. Validate one replica and one owner
before expanding: use `canary_worker_ids` with one worker ID per run, first a
replica and then an owner. Canary mode requires the same publication, healthy
existing workers and unchanged routing; deferred workers retain their state and
are verified afterwards. Clear the input for the fleet rollout, then repeat with compatible cache entries and verify the wallet
regression. Record peak RSS/cgroup memory, disk use, errors and warm/cold filesystem
cache state alongside phase timings. See [remaining work](remaining-work.md) for
open rollout gates and [evidence](../evidence/README.md) for recorded measurements.

## Ingest, infrastructure and aging

The journal's start height is persistent identity, not a resume cursor. Read metadata before starting ingestion; a genesis journal requires `start_height=0`. A mismatched start can set the old journal aside. Keep the workflow mismatch guard and resume from committed next height. Publication uses the read-only journal-opening path.

Ingest reads the chain from the node's own RocksDB (`--state-dir`, the
workflow's `state_dir` input, `/root/.cache/zakura` on the coordinator) as a
read-only secondary instance; previous outputs resolve through the node's
transaction index. Omitting `state_dir` selects the JSON-RPC path, which is
roughly a hundred times slower and exists only so an old dispatch keeps its
meaning. Resume a genesis journal with `start_height=0` and `state_dir` set.

Historical reports mention Terraform state drift, unintended transparent-spend provisioning, SSH firewall drift, volume lookup mismatch and stale SSH host keys. Re-verify these as preflight findings rather than blindly following archived repair commands. Inspect the saved plan and protect the shared node/volume and Enhance services.

Freeze the initial cutoff for the pilot. `shard-cutoff` derives it: the cutoff time is the anchor block's header time minus six calendar months, day clamped to the target month's last day, time of day kept; the cutoff height is one more than the highest height whose header time is before the cutoff time, which is well defined under non-monotone block times and is proved final by eleven consecutive blocks at or after it. The inventory action of the backfill workflow records the height, hashes and times in `cutoff.json`; a publish passes the recorded height back and the tool refuses a disagreement. Do not use approximate block counts as calendar time.

Old recent shards keep their geometry forever within the publication lineage. They can move to archive ownership after verified copying and routing handoff. Budget their actual growth; the previous 6.5 GiB/year figure is an extrapolation, not a retention guarantee. Re-cutting into wider shards is deferred until epoch identity and wallet replay semantics are specified and tested.


## Continuous publication

The continuous publisher uses `deploy-transparent-publisher.yml`: deploy the
same tested main SHA in `shadow`, then `activate`. `shadow` first aligns the
static filter origin with the verified full-chain set, upgrades a recent canary,
upgrades archive owners serially and then the remaining replicas, and builds a
candidate without advancing public coverage. `activate` routes both public map
origins through the coordinator's publication authority and enables the loop.
Before activation, verify the router directly over HTTPS and switch
`transparent_public_dns_target=router` with a reviewed DNS-only Terraform plan.
The pilot can forward its wallet routes to the router during DNS propagation;
preserve its previous Caddyfile for rollback. Confirm both public map URLs reach
the authority, including clients that still resolve the pilot. A warm internal
router does not establish that the public hostname points to it.
Use `rollback` with the deployment SHA to restore the saved binaries, units and
routing; rollback refuses an orphaned predecessor. Deep reorg recovery should
normally be left to the controller rather than restoring historical artifacts.

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
Workers prepare through a root-only Unix control socket, keep current runtimes
resident within their existing cache budgets, and swap snapshots without a
restart. Preparation evicts unpinned runtimes from retired generations before
building and warms only current revisions. Retained revisions remain available
on demand; filling spare cache capacity with them consumes the next build's
scratch-memory headroom. Both public map URLs, filters and initialization use the same active
publication. The controller keeps three unused candidate directories; workers
keep the newest three retired serving snapshots plus any older snapshot still
held by a request. An old reader does not prevent collection of other idle
snapshots. The server releases its startup snapshot after launching prewarm;
private control status reports `retired_snapshots` so a growing backlog is
visible. Three superseded normal tail revisions may also accompany the current set.
Immutable files are shared by hard link rather than copied per block.

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
when a pending observed block exceeds 30 seconds. Prometheus alert rules are in
`transparent/ops/deploy/transparent-publication-alerts.yml`.
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
  render that file from `ops/deploy/coordinator/Caddyfile`, which carries the
  route; `ops/tests/test_coordinator_caddyfile.py` fails if it is removed. After
  any coordinator deploy, confirm that
  `https://transparent-pir.valargroup.dev/v1/shards/init` returns 200.

## Hardening rollout gate

Continuous-publication recent worker upgrades preserve the 5.5 GiB MemoryHigh
setting in both deployment paths. It triggers file-cache reclamation below the
hard limit; cache and transient allocation admission remain separate. The
[canary evidence](https://github.com/valargroup/wallet-pir/blob/42b5f9c145cc3f2a400c428938c566d2cc9699db/docs/transparent-pir/evidence/hardening-2026-09-08/README.md) records the observed
tradeoff and failed lower threshold. Other workers retain their installed
settings until their rollout.

The optional `headless_console: true` fleet setting installs
`transparent-headless-console.py` as a privileged worker `ExecStartPre`. It
unbinds a virtio framebuffer console after requiring an enabled serial console;
it refuses unknown framebuffer drivers. Preflight is read-only. The worker must
be enabled across boot, and acceptance checks the loaded pre-start command,
helper digest and unbound state every worker sample. The full-fleet gate also
requires the matching helper digest. See [status](status.md) for deployment state.
This prevents the virtual GPU console-update path observed during the M1
investigation from running console refresh work on worker CPUs; sustained
availability still requires the full gate below.

Binary rollback restores the previous worker unit and any saved helper. It does
not rebind the framebuffer at runtime. Disabling the option removes the managed
pre-start line on the next installation; restoring graphics is a separate,
explicit host operation. Serial-console configuration is never changed.

The optional `storage_nodiscard` fleet setting installs a privileged worker
prestart that disables online discard on the shared ext4 root used by the
runtime cache and publication record. It validates both paths before remounting,
preserves all other mount options, and changes neither fstab nor file flushes,
journaling, barriers or scheduled fstrim. The enabled worker unit reapplies the
policy at boot. The observer verifies the loaded hook, current mount and helper
hash; the full-fleet gate requires that same helper identity. Upgrade rollback
restores the saved discard setting without reverting unrelated mount options.
This policy is qualified initially on a single worker; see the
[bounded disk-stall experiment](../evidence/productionize-m1-shoup-reduction-2026-09-11/README.md).

Run `transparent/ops/scripts/observe-transparent-hardening.py` on the coordinator with the
expected worker binary digest and release `soak-query` executable. Its defaults
require **both six hours and 300 new blocks**, two sustained query clients with
exact source-byte comparisons, public visibility within 30 seconds and recent
replica catch-up within 60 seconds from independent node observation,
canonical endpoints, no OOM or restart, and 20% host memory headroom. A reorg
resets the counted block samples. The monitor is read-only and cannot trigger a
rollout; preserve its result and raw NDJSON before rolling the next workers.

The observer also requires `state/routing-availability.json`, initialized by a
successful router application using the matching fleet script. Its persistent
epoch and cumulative unavailable-event count prevent a withdrawal and recovery
between polls from escaping acceptance. Withdrawals are recorded before router
application, so even a failed withdrawal attempt conservatively invalidates the
run. Missing, malformed, replaced or regressed evidence fails the gate. Applying
a warm route may initialize this record; do not synthesize or reset it to rescue
an existing acceptance window. This records routing changes, not every possible
network outage; the ordinary public and private probes remain required.


The approved rollout accepts downtime and upgrades all six workers in one
maintenance batch on the existing hardware. `upgrade-transparent-fleet.py`
preflights every selected worker before guarding both public transparent origins.
It gives restarted workers fifteen minutes to warm, verifies current canonical
publications and exact directory/page queries, then reopens public service.
Rollback preserves current publication records and revocations and reopens only
after verification. Unverified recovery keeps the maintenance response.

`run-transparent-hardening-rollout.py` supervises the canary upgrade, the loaded
gate, the whole-fleet batch and observation of all six workers.
The M1 post-rollout requirement is **six continuous hours and at least 300 new
canonical blocks per worker**, confirmed by Roman on 2026-09-13 as the updated
requirement under which the fleet observation had passed. This supersedes the
previously documented twelve-hour requirement. The initial loaded canary
remains six hours and 300 blocks. Correctness, freshness, routing, memory,
restart/OOM and provenance requirements remain unchanged.

For the September 12 observation, accept the first six-hour window ending
approximately 06:49:59 UTC under the separate
[dated transition record](../evidence/productionize-m1-six-hour-acceptance-2026-09-13/README.md). Preserve the original 24-hour launch
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
`transparent/ops/scripts/roll-recent-replicas.py` upgrades recent replicas one at
a time without a maintenance window, only while two others are routed, and
waits for each to be routed again before the next. Archive owners still use the
maintenance upgrade. Stop observers that latch on worker restarts, such as the
continuous-load supervisor, before a roll and re-qualify their binaries after.
The installer applies an explicit roster `build_slots` value to the staged worker
unit before verification; an omitted value preserves the installed setting.
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
The shared supervisor library, `ops/lib/wallet_pir_ops/control_sessions.py`
([control sessions](../../docs/control-sessions.md)), builds the same socket
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


Audit a closed observation with `transparent/ops/scripts/audit-transparent-observation.py`
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
python3 transparent/ops/scripts/audit-transparent-observation.py \
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

The hardening observer performs at most two attempts for a read-only
HTTP GET after a reset, broken pipe, remote disconnect or unexpected TLS EOF.
Both attempts share the original eight-second budget; the retry waits 50 ms.
It records each caught failure and recovery and includes all elapsed time in
freshness. HTTP rejection, certificate validation failures, arbitrary TLS alerts,
timeouts and malformed content are not retried. Routing withdrawal and exactness
checks remain mandatory. The result records monitor identity and retry policy.
See [qualification evidence](../evidence/productionize-m1-http-retry-2026-09-12/README.md)
and [status](status.md) for deployment state.

## Opt-in archive parent filters

The user approved publishing the evaluated archive configuration after accepting the large-script overhead. Use **8 consecutive sealed archive shards per parent, M=100, P=6**. Keep recent and provisional filters direct. Parent-enabled clients accept the coarse-activity leakage described in the contract. The production manifest URL is `https://enhance-pir.valargroup.dev/v1/filters/parents/archive-wide.json`.

The public bundle contains only `archive-wide.json` and `artifacts/<sha256>.bin`. Stage a previously evaluated bundle with `transparent/ops/scripts/stage-transparent-parents.py --candidate /path/to/archive-wide-k8-m100.json --map-url https://enhance-pir.valargroup.dev/v1/filters/shards --out /srv/transparent-parent-filters/releases/NEW_RELEASE`. The script checks exact live sealed archive descriptors, complete archive coverage, precision/group sizes and evaluated body digests before creating the serving tree. It does not regenerate filters or independently prove their journal completeness; that comes from the full-journal evaluator.

The coordinator Caddy template routes `/v1/filters/parents/*` to `/srv/transparent-parent-filters/public` with a five-minute HTTP cache lifetime. Point that symlink to the staged immutable release atomically. When first adding the route, preserve existing routing, validate the candidate Caddyfile before installation, save the predecessor, reload Caddy, and verify the manifest and every parent digest through public HTTPS. Restore the predecessor on activation failure. Do not restart the PIR fleet to publish static parent artifacts.

The bundle is tied to exact sealed child revisions. Refresh it after archive membership or revisions change; wallets reject stale parent descriptors and fall back to children. To withdraw a release, unlink its public symlink after checking the current target. Existing validated client caches can remain usable for unchanged child revisions. See the [production evidence](../evidence/parent-filters-production-2026-09-08/README.md) for the deployed release, canary and rollback record.

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
[observability guide](../../enhance/docs/observability-alerting.md) owns collector,
history, guardrail and independent-canary configuration. Metrics stay on private
service endpoints or loopback Caddy admin; no public metrics route is introduced.

For telemetry worker upgrades, the existing supervised hardening rollout accepts
`--production-lock`, `--load-service`, `--load-root`, and `--load-identity-file`.
The three load options must be supplied together. The identity JSON must cover
exactly the current roster. The runner checks predecessors, pauses continuous
load for each upgrade, verifies the expected post-upgrade identities, writes new
pins atomically, then resumes load. Failures leave load stopped for investigation.
An existing correctness/resource latch blocks mutation. The observer chooses a
query shard from the selected worker's current assignment unless explicitly pinned.
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
changes, run through `ops/scripts/wallet-pir-deploy.py`. The wrapper's
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
`schema-source-{plan,preflight,stage,status} --host HOST` with its pinned-host
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
`schema-source-stage` and `schema-source-status` commands bootstrap reviewed
operation sources on the coordinator. They require a remote-lock inventory with
the coordinator's machine ID pinned in its host entry, exact `--source-sha` and
`--sha256` identities, and `--archive FILE` except for status. Create the archive
with `git archive --format=tar.gz` from that exact commit, exporting `ops`,
`transparent/ops`, `enhance/ops`, `tools/ci` and `shared/dev`. This contains the
operation dependencies without historical evidence bundles. The reviewed
operations export at `56b67ba0` was 536659 bytes; its whole-tree archive was
81439424 bytes and exceeded the 64 MiB transfer limit. The client now rejects
an oversized archive before hashing or making an SSH request. Run plan and preflight
before stage; stage also repeats preflight. The helper receives the archive and
performs every write in the same root process holding the production lock.
It does not depend on a separate SSH lock process surviving the transfer.

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
`transparent/ops/scripts/transparent-activity-baseline.py` dependency through the
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

The checksum-bound `transparent/ops/scripts/transparent-activity-host.py` now
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

`transparent/ops/scripts/transparent-activity-routing.py` implements the schema
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
fat-LTO release identity. Worker server/control instead use the checksummed
`transparent-publisher` bundle from successful comprehensive main CI 36819961986
at `80c94f32d7d8cde6615226d41b9cdd7627cc774b`. Native Rust, Cargo and toolchain
inputs match 12ce; the CPU flags explicitly select `x86-64-v3` plus `pclmulqdq`.
The coordinator native-CPU build required instructions unavailable on the recent
workers and failed candidate verification with SIGILL. Preserve that failed
owner, native result and abandoned bytes after locked reconciliation.

Stage the two portable executables through wrapper plan/preflight and
`preflight --stage` into checksum-named releases under
`/srv/transparent-activity/portable-workers`. This only prepares candidate files.
The input builder and product preflight pin both exact executable hashes, modes
and source paths; they refuse the coordinator-native worker bytes. Use a fresh
input request/attempt after reconciliation. Prove native verification on an
actual recent worker before starting the archive copy. The original release
receipt still binds publication/assignment tools; worker file records bind the
separately compiled portable artifacts. Never rewrite the old build receipt or
claim that reusing CI artifacts establishes fleet or load qualification.

Candidate preparation executables and unit text are flat `.input-*` files beside
`shards.json`. The native server inspects every immediate child directory as a
shard, so an auxiliary `.inputs` directory is invalid. The first portable
verification exposed that error after the earlier SIGILL was fixed. Reconcile
that failed request through its retained source, preserve the partial copy and
use a new request/attempt; never weaken native shard discovery or bypass it.
The generic `shard-control --help` staging check returns a JSON EOF error because
this executable uses stdin JSON and a socket argument; retain that failed check.
Its actual read-only status protocol must be checked before service cutover.

### Failed-transaction source repair

For an interrupted or failed rollback, immutable source staging may bind
`--recovery-transaction` and `--recovery-recipe-sha256` to the exact latest failed
journal. This exception stages source only; running phases, different recipes,
remote unfinished owners and unfinished input owners still refuse. Run ordinary
source plan/preflight before stage, preserving every earlier source receipt.

`schema-repair-rollback --transaction ID --expect-recipe-sha256 SHA` retains the
original recipe and input hashes and records a separately verified repair source
receipt. It substitutes only the closed rollback wrapper, never a forward
program, and retains the original 740-second total budget. It quiesces restored
authority before restoring workers, permits bounded cold-cache warming before
exact identity/recovery proof, and resumes authority after guarded reopening.

Explicit repair preserves prior restore displacements on their original
filesystem with private intent/completion receipts; temporary interrupted restore
files refuse. If a publisher collected an older retention sentinel, the only
accepted replacement is the protocol map named by the independently copied
worker active record in the complete baseline, with unchanged retained namespace
identity. Missing or changed active publication bytes refuse. Original baseline
receipts are never rewritten. Repair startup and focused tests are not live
recovery acceptance.

A reviewed failed-rollback source repair may reuse an already warm restored worker only when every captured file still matches byte/hash/mode/owner inventory and the live executable, active publication and assignment prove the captured predecessor. Ordinary restore still stops and restores workers. Candidate cold-start verification has a bounded 300-second wait and 330-second transport; the approved rollback budget and readiness gates are unchanged.

Canonical reopen binds transparent revision setup/query routes on the captured
Enhance site to the same checksum-bound private shard router used for recovery.
The owned transparent handler must occur exactly once; an existing query handler
must match that exact router or the operation refuses. Other handlers, including
`/v1/enhance/query`, retain their captured bytes. Maintenance guards the added
transparent query handler too. Each canonical query origin requires its own fresh
native report and reopened SQLite proof; process exit zero alone is insufficient.

Protected predecessor continuation is re-established from the complete validated
baseline in each separate rollback reopen/service process. Future preparation may
coexist with the exact warm current map; current map, binary, assignment, HTTP
readiness and independently canonical anchors remain mandatory. Forward and
unprotected rollback phases retain static preparation checks.

### Retained candidate namespaces and restored startup

Fresh forward host plans explicitly capture presence or absence of the complete
`/opt/transparent-publisher/v11` tree and, on the coordinator, the candidate
canonical-load tree. After quiescence, stage binds their full bounded file
inventories, including lock files, to the complete baseline. It retains any
captured tree in a transaction-named sibling on its original filesystem before
installing fresh candidate files. Worker stale activation must match the reviewed
candidate directory, assignment and map exactly. Drift, aliases, special files,
uncaptured state or a prior reconciliation intent refuse staging. Original
baselines, recipes and displaced candidate bytes remain intact.

Restoration records up to 40 seconds of worker startup observations inside the
existing restore phase. These observations do not establish qualification; exact
worker identity, assignment, warm publication, anchors and real client proofs
remain mandatory. The remote restore budget is 90 seconds, restore phase 140
seconds, global warm wait 250 seconds, and total rollback budget 740 seconds.

The coordinator separately captures the direct initial-publication `active.json`
pointer. A prior failed candidate can leave this outside its fleet-state tree.
Preflight accepts its presence only when directory, served map, terminal height
and hash, and complete upstream roster equal the reviewed initial publication.
Seed revalidates its byte inventory against the complete baseline and records a
private source/baseline-bound adoption receipt before writing fleet state. Missing
capture declarations, byte drift, foreign identities and symlinks refuse.

A captured initial-v11 pointer is retained candidate bookkeeping, not a v10
predecessor generation. Discovery recognizes only its fixed path and closed
record shape; product validation still binds its exact reviewed target.

If preserve fails after all coordinator copies but before completion, explicit
failed-preserve repair may complete that partial capture only when private intent,
plan identity, candidate inventories and every copied byte/mode/owner still match
the stopped live state. It does not recopy or change original payloads. Only an
owned repair of the latest failed preserve, with no later forward phase, can
capture the unchanged remote predecessors without repeating the already passed
native preflight. Complete warm binary/assignment/canonical-anchor coherence is
then proved before withdrawal. All remote locks, durable owners and ordinary
rollback budgets remain in force; partial drift or unknown descendants refuse.

An explicit failed-preserve repair may reconcile the two inactive candidate
routing audit files only after an owned withdrawal failed. The complete partial
copies, private capture intent and entire candidate inventory must match apart
from one withdrawal counter increment and an empty-route timestamp. Both newer
audit files are retained with intent/completion outside the immutable partial
baseline before restoring its original bytes. Unrelated drift and prior repair
intents refuse; capture and canonical recovery remain separate gates.

The same failed-preserve repair keeps the installed public withdrawal in place
while completing capture. It derives exact guard bytes from the independently
copied Caddy file and checksum-bound private router, requires both origins still
503, and records both captured and guarded file identities. Only that fixed
Caddy transition may differ from the copied baseline; it never substitutes
guarded bytes for the captured original or reopens routing during capture.

If the sole forward phase was a failed preserve and the complete coordinator
baseline records its owned partial-capture guard, explicit repair attests the
late router snapshot against exact withdrawn bytes from the captured predecessor
fleet config and the shared native routing producer. The pinned router verifies
its entire baseline and captured Caddy hash under a checksum-bound remote owner.
Only this attested case keeps the regenerated predecessor routing instead of
restoring a snapshot already withdrawn. Original snapshots remain unchanged;
private and both canonical encrypted-query/SQLite proofs still gate recovery.

Worker activation and restoration retain up to 100 seconds of startup
observations before their separate warm proof. The remote owner transport allows
130 seconds for those actions, within the existing 140-second restore phase.
This accounts for archive native loading before cache prewarm. An observation is
not qualification: candidate warm verification remains 300 seconds, restored
worker waiting remains 250 seconds, and the total rollback budget remains 740
seconds. Cold recovery and actual cutover must still pass their original gates.

### Retaining diagnostic import bytecode during rollback repair

A current checksum-bound rollback repair may retain at most 16 unexpected import
cache files from the original operations source in a private sibling on the same
filesystem. Every original payload hash must still match. Each cache must use
the running interpreter tag and magic and contain exactly the code compiled from
its reviewed source file. Foreign entries, payload drift, aliases, hardlinks and
unfinished retention refuse. Private intent/completion binds the failed
transaction, immutable recipe and reviewed repair source; rename preserves the
cache bytes and inodes outside the source tree. The original source receipt is
unchanged and strict verification must pass after retention. Ordinary source
verification never permits extra files. Read-only diagnostics must use Python
`-B` before importing retained operations libraries.

The activity service-input controller uses the full native range profile name
`zcash-transparent-range-v2`, bound to the reviewed publication geometry.
Shorthand `v2` and foreign names refuse before service staging. A committed
cutover and brief canonical proof do not accept continuous-publication freshness.
