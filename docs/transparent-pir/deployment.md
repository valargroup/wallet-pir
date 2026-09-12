# Transparent PIR deployment

Accepted target: 2026-09-07. Implement and validate through [remaining work](remaining-work.md). Live state is recorded only in [status](status.md). This supersedes the nine-small-archive-worker plan and the single-global-geometry proposals.

## Target configuration

| Parameter | Recent | Archive |
|---|---|---|
| Initial range | Last six calendar months of pinned anchor time | Genesis through block before recent range |
| Initial profile | `recent-8k` | `archive-wide` |
| Directory/page rows | 8192 / 8192 | 32768 / 65536 |
| Row bytes; inline events | 3584; 2 | 3584; 2 |
| Seal target:capacity (scripts, pages) | Derive with `SealPolicy::for_geometry` | `393216:458752,63488:65536`, verify against derivation |
| Workers | 4 full recent replicas | 2 disjoint archive assignments |
| Host target | 4 vCPU / 8 GiB | 8 vCPU / 64 GiB, memory optimized |
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

## Geometry optimization, not a launch dependency

Add optional `recent-4k-8k` (4096 directory / 8192 page rows), seal policy `49152:57344,7936:8192`. Do not change the meaning of `recent-4k`, which already means 4096/4096. Promote only on same-range census, real placement, and total wallet-byte/latency evidence. Smaller directory upload alone does not establish a smaller sync. Initial full-chain deployment proceeds with `recent-8k`.

Do not publish `archive-32k` as the selected target: uniform-chain evidence favors `archive-wide`. Preserve the tested registry entry for compatibility/research. Geometry fallback is an explicit decision with re-census, storage, client and capacity review; never silently rewrite an already published profile.

## Sizing and availability

Uniform full-chain `archive-wide` evidence is 162 shards and 57.1 decimal GB plaintext. Applying measured c-8 per-shard RSS gives approximately 93.2 GiB prepared residency, or 81 shards / 46.6 GiB per half. This is a sizing proxy, not the mixed-tier census. Two 48 GiB cache reservations can hold the approximate halves at 576 MiB reserved per shard, but actual RSS, retained revisions and assignment imbalance must pass validation.

Retain space for the current assignment, candidate publication and rollback artifacts plus at least 20% disk headroom. The publisher needs independent peak-RSS and temporary-disk measurements; census RSS is not publisher RSS. Do not duplicate immutable sealed bytes per tail revision unnecessarily.

Losing an archive host makes its range unavailable until recovery. Recent replication does not make archive or router highly available. The initial target accepts that explicit interruption. If archive availability requirements change, evaluate two 128 GiB hosts with complete archive copies, or replicated assignments, before claiming failover. Do not apply the old nine-host 219 restores/s estimate to this fleet.

## Proposed wallet objectives from the 2026-09-08 fleet series

Proposed, not adopted: they come from three repetitions of the load series
against the activated fleet ([r1](evidence/runs/fleet-series-2026-09-08-r1/README.md),
[r2](evidence/runs/fleet-series-2026-09-08-r2/README.md),
[r3](evidence/runs/fleet-series-2026-09-08-r3/README.md)) at 8 and 32
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

Public DigitalOcean list prices checked 2026-09-07: four regular Basic 8 GiB/4 vCPU hosts at $48, two regular memory-optimized 64 GiB/8 vCPU hosts at $336, one Basic 4 GiB router at $24: **$888/month compute**. [Provider pricing](https://www.digitalocean.com/pricing/droplets).

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

The filter service deploys on its own through `.github/workflows/deploy-transparent-filter.yml`, which asks the staged binary to read the named set (`--check-shard-dir`) before the running service is touched, compares that map digest with the set the fleet serves, and verifies both public origins afterwards. It shares the Enhance workflow's concurrency group and rollback paths and touches nothing of Enhance. The Enhance workflow still stages the filter binary too; remove that once the filter-only path has deployed successfully, keeping the Enhance script's rollback lines. Do not copy a transparent Caddy configuration onto the Enhance coordinator.

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
open rollout gates and [evidence](evidence/README.md) for recorded measurements.

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
Lagging replicas leave current routing and are retried on later publications.
Artifact transfer commands may reuse authenticated SSH sessions for up to 60
seconds. Control commands use independent connections; bounded read-only status
retry is described in the hardening gate below. Cancelling a slower replica
does not delay an already warm quorum. An unchanged router configuration skips
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
`ops/infra/digitalocean/production/deploy/transparent-publication-alerts.yml`.
The production deployment key is supplied by the GitHub Environment and stored
only in the controller's root-readable runtime credential directory.

## Hardening rollout gate

Continuous-publication recent worker upgrades preserve the 5.5 GiB MemoryHigh
setting in both deployment paths. It triggers file-cache reclamation below the
hard limit; cache and transient allocation admission remain separate. The
[canary evidence](evidence/hardening-2026-09-08/README.md) records the observed
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
[bounded disk-stall experiment](evidence/productionize-m1-shoup-reduction-2026-09-11/README.md).

Run `ops/scripts/observe-transparent-hardening.py` on the coordinator with the
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
gate, the whole-fleet batch and 24-hour observation of all six workers. It stops
on failure. The full-fleet gate must match the tested binary, fleet script, configuration
and roster digest; elapsed time alone is insufficient. The foreground adapter and
reconciler must use the same `fleet.json`. Enable `managed_recent_workers` for
recent-01 at the canary stage and all four recent replicas after their upgrade.
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
The optional `status_socket_forwarding: true` flag requires `control_sessions`
and carries status over root-only Unix forwards owned by the same supervisor.
It is disabled by default; consult [status](status.md) for whether it is deployed.
Each request has its own stream, a 1 MiB newline-framed response limit and the
same status timeout/retry policy. Missing forwards do not fall back to helper
execution. Mutations keep their existing SSH paths. Verify all worker forwards
before enabling clients; changes require matching acceptance evidence.

Returned status still must attest the current warm publication. Two failed reads
withdraw membership; invalid or rejected status is not retried. Mutating control
commands are never blindly retried after an ambiguous transport failure.
One daemon task owns each managed worker's preparation across foreground quorum
cancellation; it coalesces queued targets and reattests status after restart.
A superseded but canonical prepared candidate may advance an unrouted worker
privately when its height is already covered by the public authority. The worker
remains excluded until it attests the exact current public map. This prevents a
burst from discarding every completed preparation and indefinitely delaying
replica coverage. Future candidates and withdrawn/orphaned publications cannot
use this path.


The candidate hardening observer performs at most two attempts for a read-only
HTTP GET after a reset, broken pipe, remote disconnect or unexpected TLS EOF.
Both attempts share the original eight-second budget; the retry waits 50 ms.
It records each caught failure and recovery and includes all elapsed time in
freshness. HTTP rejection, certificate validation failures, arbitrary TLS alerts,
timeouts and malformed content are not retried. Routing withdrawal and exactness
checks remain mandatory. The result records monitor identity and retry policy.
See [qualification evidence](evidence/productionize-m1-http-retry-2026-09-12/README.md)
and [status](status.md) for deployment state.

## Opt-in archive parent filters

The user approved publishing the evaluated archive configuration after accepting the large-script overhead. Use **8 consecutive sealed archive shards per parent, M=100, P=6**. Keep recent and provisional filters direct. Parent-enabled clients accept the coarse-activity leakage described in the contract. The production manifest URL is `https://enhance-pir.valargroup.dev/v1/filters/parents/archive-wide.json`.

The public bundle contains only `archive-wide.json` and `artifacts/<sha256>.bin`. Stage a previously evaluated bundle with `ops/scripts/stage-transparent-parents.py --candidate /path/to/archive-wide-k8-m100.json --map-url https://enhance-pir.valargroup.dev/v1/filters/shards --out /srv/transparent-parent-filters/releases/NEW_RELEASE`. The script checks exact live sealed archive descriptors, complete archive coverage, precision/group sizes and evaluated body digests before creating the serving tree. It does not regenerate filters or independently prove their journal completeness; that comes from the full-journal evaluator.

The coordinator Caddy template routes `/v1/filters/parents/*` to `/srv/transparent-parent-filters/public` with a five-minute HTTP cache lifetime. Point that symlink to the staged immutable release atomically. When first adding the route, preserve existing routing, validate the candidate Caddyfile before installation, save the predecessor, reload Caddy, and verify the manifest and every parent digest through public HTTPS. Restore the predecessor on activation failure. Do not restart the PIR fleet to publish static parent artifacts.

The bundle is tied to exact sealed child revisions. Refresh it after archive membership or revisions change; wallets reject stale parent descriptors and fall back to children. To withdraw a release, unlink its public symlink after checking the current target. Existing validated client caches can remain usable for unchanged child revisions. See the [production evidence](evidence/parent-filters-production-2026-09-08/README.md) for the deployed release, canary and rollback record.

Reference clients opt in with `HttpFilterSource::with_parent_experiment(manifest_url, "archive-wide".into())`; loadtest scenarios use `experimental_parent_manifests.archive-wide`. Hosting the bundle does not change already-installed wallet applications. No script-count bypass is enabled.

## macOS recovery beta acceptance targets

Agreed planning targets, 2026-09-09; **not measured acceptance or a general
production SLO**. The [execution plan](productionize-plan.md) defines the bounded,
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
