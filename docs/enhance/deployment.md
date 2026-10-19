# Enhance PIR deployment

The most recent production change is the [October 11 deploy of `eae5641a`](../../evidence/enhance/production-deploy-eae5641a-2026-10-11/README.md): every Enhance role, now including the GPU worker on its CUDA build, moved to that release, which adds worker and router memory reporting and the shadow `capacity` alerts. Before it came the [October 10 deploy of `9d2cbda0`](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/enhance/production-deploy-9d2cbda0-2026-10-10/README.md): every Enhance and Status role moved to that full-CI release through the deploy CLI, which adds acceptance of a growing domain's query-row prefix. It followed the [October 9 deploy of `06a972db`](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/enhance/production-deploy-06a972db-2026-10-09/README.md), which added acceptance of 44-bit dithered selections without advertising them. Before them, the [October 6 mixed-transaction fee repair](../../evidence/enhance/mixed-fee-production-2026-10-06/README.md) was a coordinator-only deploy of `2474cdbb` (native v9), with historical records rebuilt from chain and adopted. The record includes rollback for the binary, the data and the pinned oracles. An earlier full release was the [September 24 cleanup deployment](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/enhance/production-cleanup-2026-09-24/README.md), built from `71be21f` (PR #111). All five server roles use the same verified binary; existing v7 state, runtime arguments, and the APM sidecar were preserved. The record includes exact-answer checks and guarded rollback instructions.

The initial v7 SSH rollout used `/opt/enhance-pir-v7/releases/2a83c21` and fresh state
under `/srv/enhance-pir-v7`. The [dated evidence](../../evidence/enhance/immutable-v7-2026-09-24/README.md)
records the final deployment status, checksums, tests and capacity limitations.
The previous release and `/srv/enhance-pir-v6` data remain available for rollback.

The subsequent [cache-reclaim update](../../evidence/enhance/worker-cache-reclaim-2026-09-24/README.md)
deployed source `ac7cf9d` in `/opt/enhance-pir-v7/releases/ac7cf9d` on the same
three hosts. Its focused workload was stopped early at the operator's request;
the memory observation is promising but not a completed qualification.

The binaries are `enhance-pir-server` (package `enhance-publisher`),
`pir-query-server` (package `pir-query-server`),
`enhance-pir-cli` (`enhance-pir-client`), `enhance-pir-load-test` and the local
`enhance-pir-exercise`. `enhance-pir-server` commands are `coordinator`,
`rebuild-journal`, `prepare-packing-artifacts`, `restore-legacy-placement`, and
`build-info`. `build-info` reports native reinspiring and the source revision
and has no CUDA field. Each Enhance serving role (`worker`, `query-ingress`,
`packing-router`) is its own `pir-query-server --config FILE` process.
`pir-query-server check-backend [--matvec-backend cpu|cuda] [--cuda-device N]`
checks the evaluator without opening worker state.
`pir-query-server repair-rows --data-dir DIR --source-rows DIR` is the offline
row repair. Protocol v7 retains schema-11 records and q48, but changes
routing, session identity and framing. v6 clients and serving state are incompatible.
Use fresh controller, worker, hint and cache directories. A schema-11 canonical
journal may be copied while stopped and validated; older-width journals must be
re-ingested. Never open v6 controller or worker state with the v7 binary.

## Node RPC policy

The current source reads the node through wallet-libraries' server-only
`zakura-server-chain` adapter. Enhance and Status read their RPC cookie when the
client is constructed and report HTTP 401 as an invalid cookie. Each request has
a 120-second total deadline, including connection establishment and reading the
body, with no separate connection timeout. Each whole response, including a
batch, is limited to 64 MiB even without Content-Length. A valid large batch can
exceed this deliberate cap and fail; the transport does not retry, split batches
or fall back to another endpoint. Batch results are returned in request order.
These are source policies; the deployed release is established by its transaction
and artifact receipt.

## Build and local validation

```sh
cargo build --locked --release -p enhance-publisher -p enhance-pir-client -p enhance-pir-load-test --bins
python3 tools/enhance/scripts/test-local.py --help
python3 tools/enhance/schema11-interop/run.py /absolute/path/to/pinned/wallet-libraries
```

The wallet counterpart must implement the v7/q48 contract described in
[integration](integration.md). Use the
checksummed `enhance-pir` release artifact; its `candidate.json` records the
source revision and declares qualification `unqualified`. A green build or
bundle verification does not grant hardware qualification.

## Fresh deployment layout

Install the same checked binary revision on the coordinator and worker hosts.
Use new `/srv/enhance-pir-v7/canonical` and `/srv/enhance-pir-v7/worker` directories,
with an inventory at `/etc/enhance-pir/workers.json`. The inventory contains
ordered groups, each with a name and two replicas carrying `name` and private
`url`; [workers.example.json](../../tools/enhance/deploy/workers.example.json) shows the shape.
Keep worker port 8091 on the private network and expose only the coordinator's
client routes through the public origin. Network isolation is the primary
control; the optional shared token below is an additional layer, not a
replacement.

### Internal control token

Setting `ENHANCE_INTERNAL_TOKEN` in the environment of every role enables a
shared bearer token on the private APIs. Worker routes, packing-router control
routes and query-ingress control routes (all `/internal/*` paths except the
ingress `/internal/metrics` scrape) then reject requests without
`Authorization: Bearer <token>` with HTTP 401. The coordinator, packing router
and `enhance-pir-exercise` clients read the same variable and attach the header. Set it on
all roles together, or on none: a worker with the token and a coordinator
without it stops publication. Unset keeps the previous behaviour. Provide the
value through the unit's environment file, never on the command line.

### Worker preparation outcomes and refusals

A worker's preparation reports how it produced each unit's runtime: reused
live, restored from its verified artifact, or rebuilt from staged rows bound to
the committed digest. Its private `/internal/health` carries the latest
`preparation` (generation, unit counts, or the refusal), in memory only.
Control refusals are classified once, as invalid source, runtime, memory,
admission or superseded, and reach the coordinator with the same statuses and
text as before: 507 for a reservation that does not fit, 503 otherwise. An
operator-added worker is the `enhance.external-worker` role
([operations](../operations.md#external-workers)); it serves only after
the coordinator activates it and a packing router lists it.

### Query ingress uploads

The ingress role (`query-ingress`, loopback 8082) buffers each complete request
body (at most 512 KiB, 30-second deadline) before it takes one of its
`requests` forwarding permits (default 16) and forwards the buffered body to a
packing router. Buffering is bounded by `uploads` (default 256 concurrent
bodies) and by four concurrent uploads per client, identified by the first
`X-Forwarded-For` address, else `X-Real-IP`, else the socket peer. Either limit
answers HTTP 429 `overloaded` without charging a forwarding permit. Caddy must
keep sending `X-Forwarded-For` so the per-client cap does not collapse onto the
proxy's loopback address.

The coordinator admits four active queries and lets up to 16 more wait for two
seconds. A full or expired queue returns HTTP 429 with `Retry-After: 1`; a busy
replica is retried on its peer. The public Caddy query route disables upstream
keepalive so overload responses are not lost on stale local connections. Check
the `enhance_query_*` metrics and public 429/502 counts after rollout.

The coordinator example is
[enhance-pir-server.service](../../tools/enhance/deploy/enhance-pir-server.service):
`enhance-pir-server coordinator`. A worker, query ingress, or packing router is
a separate `pir-query-server --config` process, one Enhance role in that file.
The role file carries the listen address, absolute data directory, sealed-shard
count, and, for a worker, the matrix backend. `ENHANCE_INTERNAL_TOKEN`, when
used, stays in the unit environment file and never on the command line. Point
`/opt/enhance-pir/current` to the verified release on each host and create the
non-root worker account and its private data directory. The default sealed-shard
count is six; seven requires separate hardware qualification. Keep existing
Terraform resource addresses, droplet names, and tags when reconciling
infrastructure.

## Cutover checks

1. Confirm the wallet fleet accepts schema 11 and the recorded protocol revision.
   Preserve the old release and data for rollback from its pinned checkout.
2. Prepare fresh state from canonical RPC and start both workers before the
   coordinator. Verify their private health and matching placement policy.
3. Check the public manifest's schema, anchor, coverage, and two-replica
   publication. Run exact-answer queries against an independent record oracle,
   including a row boundary, a retained session, and a refreshed expired session.
4. Check worker memory, disk, swap, publication time, and query latency under
   the target placement. Use [qualification](qualification.md) for the remaining
   sustained acceptance evidence.
5. Switch traffic together with compatible wallets. Rollback switches traffic
   and clients to the preserved old release and data; it does not reuse new state.

The physical hosts have recorded `v4` resource names in Terraform. Those names
are live identities and are deliberately left unchanged. The direct SSH
cutover did not run Terraform.

## Deploy CLI

`tools/scripts/wallet-pir-deploy.py` is the repository's transactional deploy tool
for Enhance and Status. It has deployed production releases since 2026-10-06
(coordinator only) and 2026-10-09 (every Enhance and Status role); the manual
runbook below remains the fallback.

The tool runs on the coordinator, or on a workstation with the production SSH
access, and takes a local inventory (`--inventory` or
`WALLET_PIR_DEPLOY_INVENTORY`) of hosts, SSH settings and the production lock.
The repository does not describe the live fleet; see
[deploy-inventory.example.json](../../tools/enhance/deploy/deploy-inventory.example.json)
for the shape. Roles, units, readiness checks and rollout order (workers,
packing router, query ingress, coordinator) are in
[deploy.toml](../../tools/enhance/deploy/deploy.toml).

```sh
wallet-pir-deploy.py capture-baseline enhance
wallet-pir-deploy.py plan enhance --binary ./enhance-pir-server
wallet-pir-deploy.py preflight enhance --binary ./enhance-pir-server [--stage]
wallet-pir-deploy.py deploy enhance --binary ./enhance-pir-server --retire-historical
wallet-pir-deploy.py status enhance
wallet-pir-deploy.py rollback enhance [--transaction ID]
```

`--archive <bundle> --sha <rev>` takes a checksummed `tools/ci/release.py` bundle
instead of `--binary`. `--sha256 <digest>` names a binary without supplying it,
which is enough for a no-op check or an already staged release.

- `capture-baseline` records each unit's running executable digest and complete
  unit text. `preflight` and `deploy` refuse if anything changed since, and
  `deploy` refreshes the baseline after it commits or rolls back.
- The coordinator binary is installed as
  `/opt/enhance-pir/releases/<sha256>/enhance-pir-server` and must pass `--help`
  on each host before any unit changes. A serving role installs
  `pir-query-server` the same way and starts with `--config`. The release
  directory is named by the sha256 of the file that host runs.
- Each unit gets one managed drop-in whose name (32 `z`s, then
  `-wallet-pir-release.conf`) sorts after the drop-ins the manual rollouts
  stacked. It clears `ExecStart` and sets it to the release binary with the live
  argument tail. A serving role does not keep an `enhance-pir-server` `worker`,
  `query-ingress`, or `packing-router` tail: its `ExecStart` is
  `{release}/pir-query-server --config /etc/enhance-pir/<role>.json`.
  Earlier drop-ins that set `ExecStart` stay in place and are
  shadowed; one that would sort after the managed drop-in is refused. With
  `--retire-historical`, the ExecStart-only `zz-cleanup-*.conf` drop-ins are
  instead moved into the host's transaction directory.
- `--only ROLE[@HOST]` (repeatable) limits a plan, preflight or deploy to those
  targets, so the replicated workers can roll while single-instance roles keep
  running.
- The GPU worker runs the CUDA `pir-query-server` from the
  `enhance-pir-native-cuda` build. That host does not run `enhance-pir-server`
  as the worker. Its inventory entry names `"artifact_kind":
  "enhance-pir-native-cuda"`, and every plan, preflight or deploy that selects it
  needs `--extra-archive enhance-pir-native-cuda=<bundle>` with the same `--sha`;
  without it the command refuses rather than skipping the GPU. The GPU then rolls
  in its own transaction, in rollout order, with its own exact-answer check. Leave
  it out deliberately with `--only`. The GPU is the packing router's preferred
  worker, so a release is not complete until it runs too.
- A worker built from `main` after 2026-09-30 stops gracefully on SIGTERM: it
  closes its listener, so the packing router retries new evaluations once on
  the other replica, and it finishes and delivers the evaluations it already
  accepted. Rolling the workers one at a time with `--only` then loses no
  admitted query. Earlier worker binaries stop immediately, so replacing one of
  them fails the few evaluations in flight at that moment. The packing router,
  query ingress, coordinator and the Status roles are single instances, and
  restarting any of them interrupts service.
- In `ssh.mode = "config"`, `ssh.config_file` names an SSH config whose aliases
  may jump through the coordinator to private addresses.
- A unit whose running executable and effective configuration already match is
  skipped, so deploying the running binary is a no-op.
- Every host must accept the deploy identity before anything changes. Mutating
  commands hold `/run/lock/wallet-pir-production.lock` on the coordinator.
- `enhance-pir-server build-info` has no CUDA field. Staging a GPU worker runs
  `pir-query-server check-backend --matvec-backend cuda --cuda-device N` on
  that host, using its configured device, before restarting the working service.
  This checks compiled support and driver initialization without opening worker
  state; publication readiness and encrypted answers remain separate activation
  checks.
- After each restart the tool requires the unit to be active, `/proc/<MainPID>/exe`
  to have the release digest, and the role's health endpoint to report ready
  (and the same `binary_sha256`, when the endpoint reports one). After all roles
  it runs the inventory's exact-answer command. `--skip-exact-check` applies only when the
  inventory has no `exact_check`; a configured check runs on every transaction. The
  check's SSH session has no client-side timeout, so a dropped connection leaves the
  transaction `verifying` until the process is stopped and the transaction rolled back.
- Every step is journaled in `~/.local/state/wallet-pir-deploy/<id>.json` before
  it happens; each host keeps the previous files in
  `/opt/enhance-pir/transactions/<id>/`. A failure restores the touched units in
  reverse order and checks that the previous executable is running again.
  `rollback` does the same for a committed or interrupted transaction, and is
  safe to repeat.

The tool does not drain or pause public queries, so plan the query-route
maintenance window as in the September 24 rollout.

## Repairing historical fees

Coordinators built before the [whole-transaction fee rule](architecture.md#record-metadata-and-fees)
published fee-absent records for Ironwood transactions with transparent, Sapling or
Orchard parts. The journal stores only encoded records, so installing and restarting
the fixed binary repairs new blocks only. History is repaired offline from chain and
then adopted by the coordinator at startup. Neither step touches `control/`,
workers, routers or ingress.

1. Build the server with its source revision recorded in the receipt:
   `WALLET_PIR_SOURCE_REVISION=$(git rev-parse HEAD) cargo build --locked --release -p enhance-publisher --bin enhance-pir-server`.
2. While the coordinator keeps serving, rebuild into the staged directory of its
   data directory:

   ```sh
   enhance-pir-server rebuild-journal \
     --source <data-dir> --output <data-dir>/enhance-staged \
     --zakura-rpc-url <archive RPC> --zakura-cookie <cookie> \
     [--through-height H] [--concurrency 8] [--cache-outputs 250000]
   ```

   It reads `<data-dir>/enhance` without the coordinator's journal lock, stops at
   the source tip (or `H`), checks every block hash with batched `getblockhash`,
   re-derives every non-empty block with the coordinator's producer and the final
   Ironwood tree size with `getblock` verbosity 2. A regenerated record may differ
   from the old one only in flag bit 2 and bytes 645..653, and only where the old
   record had no fee. Any other difference, a hash or action-count mismatch, or an
   unresolvable spent output aborts the run. Choose `H` a few blocks below the tip
   if a reorganization near the tip is likely. An interrupted run resumes from the
   staged tip when started again with the same arguments. Progress is printed to
   stderr as JSON lines every 30 seconds.
3. On success the tool writes `enhance-staged/rebuild.json` last, after the staged
   journal is durable, and prints it. The receipt records the source revision and
   binary SHA-256, `H`, its block hash and tree size, SHA-256 digests of
   `records.bin` and `manifest.json`, the changed-record count and the count still
   without a fee (coinbase outputs). A completed output is never overwritten.
4. Restart the coordinator. Before opening its journal it verifies the receipt
   digests, that the staged blocks (height, hash, first position, action count) are a
   prefix of the live journal, that every staged record differs from the live one
   only in the fee fields, and that no changed position lies in a sealed shard
   (`recovery.sealed` in the control state). It then renames `enhance` to
   `enhance.before-rebuild-<receipt id>`, renames `enhance-staged` to `enhance` and
   syncs the data directory. A crash between the renames completes on the next start.
   It logs `adopted staged Enhance journal rebuild` with the receipt id, `H` and the
   changed count, then ingests `H+1` to the tip with the fixed producer. Repaired
   records reach wallets with the next publication, which follows the next block.
5. If any check fails, it logs `rejected staged Enhance journal rebuild` at error
   level with the reason, renames the staged directory to
   `enhance-staged.rejected-<unix time>`, and continues on the live journal. A
   staged directory without `rebuild.json` (a rebuild in progress) is left alone.

Adoption verification is one streaming pass over both journals: fixed-size chunks,
two reused buffers, the staged records digest computed during the comparison, and
no in-memory copy of either journal. It runs before the coordinator opens its
control state and binds its HTTP listener, so the adopted journal is in place before
serving resumes; block catch-up and spent-output fetching stay in the poll loop after
the listener serves the restored snapshot. A restart without a completed staged
rebuild only checks for `enhance-staged/rebuild.json`. A locally measured adoption of
700,000 records in 50,000 blocks took 2.1 seconds with a warm page cache and grew peak
memory by under 1 MiB ([evidence](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/enhance/mixed-fee-publication-2026-10-06/README.md));
a cold cache reads about 0.9 GB more slowly, and that time adds to the restart's
listener gap.

### Exact-record oracles during a repair

Two operator checks pin exact record bytes: the deploy exact-answer check, which runs
`enhance-pir-load-test --oracle` against a pinned oracle file, and the external
`pir-monitor` canary oracle (`PIR_MONITOR_ORACLE`, pinned by
`PIR_MONITOR_ORACLE_SHA256`; see [alerting](observability-alerting.md)). A record
whose fee is repaired no longer matches an oracle captured before the repair. For any
historical record repair:

1. Regenerate both oracles from an independent chain oracle (canonical transactions
   read from the node and decoded without the coordinator's producer), not from the
   repaired journal or a served answer. Record the new oracle digests.
2. Put the external monitor in shadow mode (`PIR_MONITOR_ALERT_MODE=shadow`) for the
   whole window, from the coordinator restart until the repaired generation is
   published and verified.
3. Deploy with `--skip-exact-check`, because before adoption and the next publication
   the coordinator still serves the old bytes and neither oracle can pass.
4. After the coordinator logs the adoption and publishes the next block's
   generation, run the exact-answer check by hand with the regenerated oracle. Then
   install the regenerated, re-pinned monitor oracle and return the monitor to
   active mode only after its canary passes.

Adoption refuses changes in sealed shards; repairing a sealed shard requires a
recovery-fenced republication that this procedure does not provide.

To roll back the data, stop the coordinator, move `enhance` aside, rename
`enhance.before-rebuild-<receipt id>` back to `enhance` and start the coordinator.
Its next publication serves the old records again. Keep the preserved directory
until the repaired publication has been checked; it is the only copy of the
pre-rebuild journal.

## Direct SSH rollout and rollback

The September 24 rollout builds with Rust 1.91, locked dependencies,
`RUSTFLAGS="-Dwarnings -C target-cpu=x86-64-v3"` and `CFLAGS/CXXFLAGS=-mpclmul`.
One checked server binary is copied to all hosts. CI waiting is intentionally
skipped; native tests and the external wallet harness run directly on Linux.

Existing systemd units are retained. `90-v7.conf` overrides `ExecStart` on
`enhance-pir-coordinator.service` and `enhance-pir-worker.service`; the worker
override also grants write access to the new worker directory. Old unit files,
drop-ins and binary checksums are saved under `/srv/enhance-pir-v7/rollback` on
each host. The coordinator listens on loopback port 8080 and Caddy keeps the
existing public origin. Workers listen on their private addresses at port 8091.

To roll back this layout, stop the coordinator, stop both workers, move only the
`90-v7.conf` overrides out of their `.service.d` directories, reload systemd,
start both preserved v6 workers and then the v6 coordinator. Verify the public
v6 manifest and exact-answer oracle. Restore matching v6 clients if any are in
use. Do not delete or reinterpret v7 state. To return to v7, stop the v6 services,
restore the v7 overrides, reload, start both v7 workers and then the coordinator.

The isolated workload uses separate `qualification-worker` directories and an
`enhance-v7-qualification` unit on loopback port 8280. Stop it before changing
worker state back to canonical serving. Preserve its samples, including memory
pressure and failures. A focused 30-minute run never grants the six-hour
hardware qualification or permission to increase placement/admission limits.
