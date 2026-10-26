# Unified publisher

`pir-publisher` is one process that runs any configured subset of Enhance,
Status, Transparent history, transaction-detail and Receiver publishing. Its
management server owns listener binding and process shutdown. For Status, Enhance,
History and transaction details, the host also owns the preparation sequence and
publication scheduler. Product libraries supply chain acquisition, journals,
material construction, semantic checks and guarded authority updates. Products do not wait for a common publication height.

## Source and dependency ownership

The publisher host lives at `src/pir-publisher`. Each product publisher lives
beside that product's query server: `src/enhance/enhance-publisher`,
`src/status/status-publisher`, `src/transparent/transparent-publisher`, and
`src/receiver/receiver-indexer`. Those packages keep their existing names.
`src/shared/pir-pipeline` is the shared lifecycle library: it also owns the
publication engine, the transport-free chain follower and the live and backfill
ingestion schedulers, so products and fast tests use them without the host or a
node stack. `src/shared/pir-node-rpc` is the bounded node JSON-RPC transport
(credentials, deadlines, response bounds, batches) with no chain decoding;
`zakura-server-chain` builds its `NodeClient` on it and the root follower uses it
alone. `src/shared` otherwise holds shared libraries only. `src/receiver/receiver-store` holds the
indexer's SQLite stores. History and transaction details remain separate modes
of `transparent-publisher`. Enhance, Status, and Transparent wallet-client and
chain-fixture regressions run with the host, not as a second copy of that stack
on each product crate.

Query serving is the separate [query-server host](query-server.md) at
`src/pir-query-server`. Product query libraries remain under their product
directories. Serving libraries must not depend on publisher packages. Shared
protocol, verification and runtime libraries remain under `src/shared`. The
pipeline depends only on Tokio: its HTTP adapter is
generic over a product's router, and the application supplies the actual server.

`pir-publisher` compiles a product only when that product's Cargo feature is
enabled: `enhance`, `status`, `transparent-history`, `transparent-detail`, or `receiver`. History and transaction-detail publishing compile independently. `native-reinspiring`
selects native packing for Enhance and Status and does nothing unless that
product feature is also enabled. The publisher host has no CUDA feature. A
configuration section for a product that was not compiled is rejected.

This layout changes no wallet protocol, record codec, publication identity,
journal, database or runtime-cache format. Maintenance is available through
root subcommands; the product crates have no standalone executables. Crate relocation does not relocate deployed data or change upstream
wallet-libraries revisions.

The binary composes product service facades into `PublisherDefinition` values.
Each definition declares listeners, writable paths and readiness probes, and owns
a startup factory. Static checks inspect those declarations without opening
credentials or product state. The generic `run_registered` host reserves all
listeners before starting any factory; it contains no product-specific routing,
controller settings or readiness response schemas.

`pir-publisher::publisher::PublisherEngine` executes build, store, verify and
candidate assembly in that order. Its prepared token retains the product's
borrowed state or preparation permit. Commit rechecks the exact source, checks
local invalidation and shutdown again after the await, then enters the product's
guarded durable update. Preparation alone grants no distribution authority.

The contracts live below products in `pir-pipeline::publisher`: `Publication`
supplies bounded material and state operations; `Publisher` supplies input,
wake conditions and failure policy. The root injects its executor into product
construction, so production and integration callers use the same sequence
without a normal dependency from product libraries back to the host. Product
code keeps its algorithms, codecs, settings, routes and recovery rules; the
root contains no product implementation modules. The common file verifier in
`pir-publication` reuses exact-file evidence without weakening semantic checks.

The engine finishes started operations before observing a service stop and
always runs the product drain. Filters retain the ingestion-only pipeline;
Receiver uses the root ingestion scheduler while retaining its existing table
preparation and serving mechanisms.

## Configuration and local use

Build the native CPU application used by the deployed protocols:

```sh
cargo build --locked --profile release-fast -p pir-publisher \
  --features enhance,status,transparent-history,transparent-detail,receiver,native-reinspiring
pir-publisher --config /etc/wallet-pir/publisher.json --check
pir-publisher --config /etc/wallet-pir/publisher.json
```

The [all-product example](../tools/deploy/publisher/publisher.example.json)
is a typed configuration example, not a production inventory. Replace its
addresses, state paths, bootstrap inputs, inventories, public Status salt and
release identity with the operator's existing values. Preserve each product's
extraction and confirmation policy; all products use the root node credentials.

The five product sections are `enhance`, `status`, `transparent_history`,
`transaction_details` and `receiver`. Each has `enabled` and `settings`.
Omitted sections are disabled. A disabled section may omit settings and starts
no task, opens no store and reads no credential. Product settings retain their
existing names and semantics; non-optional settings must be explicit. Receiver
is continuous, and Enhance's embedded Status setting is replaced
by the separate Status section. No configuration reload is performed.

The root `chain` section (`rpc_url`, `rpc_cookie` or explicit `no_auth: true`
for Receiver-only nodes, and `poll_ms`, default
1000) names the one node every chain-following product reads. Enhance, Status,
Transparent History and Transparent Filters require it and no longer accept
their own node endpoint, cookie or polling settings (`zakura_rpc_url`,
`zakura_cookie`, `poll_seconds`, `rpc_url`, `rpc_cookie`, `cookie`). Transaction
details also use the root follower and require the root chain settings.
Receiver also requires it. Its service settings no longer accept `rpc_url`,
`cookie`, `no_auth`, `poll_seconds`, `serve`, `start_height`, or `end_height`.
Receiver always starts at Ironwood activation and targets two confirmations
by default (`depth`); canonical committed progress above that target is kept.
Its independent canonical guard consumes root observations while tables prepare.

### Root chain following

When a chain-following product is enabled, the host runs one follower that
reads the node's genesis and tip once per pass and delivers every pass,
including an unchanged tip. Each such product supplies its journal's writer
operations through its registration; the host runs a live scheduler and a
separate backfill scheduler over that one writer. On every pass the live
scheduler compares the journal's own durable checkpoints with the node, so a
restart or a skipped pass cannot hide a reorg. Before removing any orphaned
blocks it lets the product withdraw publications that depend on them. Short
gaps extend live; longer gaps go to the backfill scheduler, which commits
bounded batches toward a fixed target and releases the writer between batches
so reorg withdrawal stays responsive. A product offers new publication work
only after its coverage equals the observed tip. A node behind the journal
removes nothing. Existing journals are opened in place and resume at their
committed coverage; at an unchanged tip no block is read or rewritten.

`--check` parses and validates the configuration, listener declarations and
writable paths. It neither reads credentials nor contacts nodes or workers,
opens stores or binds sockets. It cannot establish input validity or readiness.
Credential values must never appear in configuration: use installed credential
files or the existing runtime environment. Receiver's optional partner feed
still reads `NEAR_INTENTS_EXPLORER` at runtime. The service optionally reads
`/etc/wallet-pir/publisher.env` for those existing environment settings, including
`ENHANCE_INTERNAL_TOKEN`. Install that file with restricted permissions from
Infisical; never commit its contents.

## HTTP and supervision

All enabled listener addresses are reserved before any publisher starts. Keep
existing loopback proxy listeners, private control/artifact listeners and public
edge rules. Listeners need distinct fixed private or loopback addresses;
management is loopback only. The application does not move public routes or
make control routes public. It uses the existing product routers and admission
rules without a new public API or protocol prefix.

The management listener exposes `/health` and `/ready`. Health reports each
product's lifecycle state and observed readiness; it remains available when a
product fails. `/ready` answers 503 unless every enabled product passes its
product probes. Running a task alone does not establish readiness. Existing
product health and metrics endpoints retain their response contracts. An empty
configuration is useful for management diagnostics and has no product work.

### Service discovery

The management listener also serves `GET /.well-known/pir-service-discovery`
with `{"service":"pir-publisher","version":"1.0.0"}` and `Cache-Control:
no-store`. The version comes from the
[`pir-service-discovery`](../src/shared/pir-service-discovery/README.md) crate
the publisher is built with; there is no configuration override. Query servers
that synchronize over HTTPS (Transparent history and transaction details) read
it from the configured publisher origin before they start and before every
synchronization pass, and refuse to start without it.

This is a cutover: before upgrading those query servers, the publisher's HTTPS
edge must proxy exactly that path, for `GET` only, to the management listener.
Do not proxy `/health`, `/ready` or any other management or control route. The
document is compatibility metadata only; it is not health and grants no
publication authority.

Status retains a bounded dedicated runtime. Receiver also uses a dedicated
runtime thread because its SQLite connections belong to their owner thread.
Other product tasks remain independently scheduled with existing preparation
and admission limits. These are scheduling boundaries, not process memory
isolation: an out-of-memory kill affects every enabled product.

A terminal product error stops that product without automatically reopening its
state; ordinary failures retain the product's retry policy. History withdraws
its public authority on fatal input failure. Status permanently fences admission
when its source task exits. Receiver keeps its canonical guard active while
publication completes. Transaction-detail halts retain their persisted reason
and status endpoint; shutdown waits for background builds and transports before
releasing their owner locks. Unexpected task panics stop the unified process
rather than attempting recovery over potentially unfinished work.

SIGINT and SIGTERM request cooperative shutdown. Publishers finish their current
publication operations and stop owned auxiliary tasks before releasing state.
History candidate preparation is the exception: the candidate build checks the
stop request before each journal block and each shard that block completes;
source-set verification before each revision, each 1 MiB read of a hash and
route checks; runtime preparation between segments; and inventory finalization
before each file, each 1 MiB read and each runtime self-check. A stopped
candidate is abandoned as a normal outcome without beginning activation or
changing the active publication; the build writes no map, and an interrupted
hash records no evidence or receipt. Display stops the same way while it
restores its saved distribution at startup (verification, row checks, runtime
segments, finalization) and between the recent-shard build, candidate write and
runtime preparation of a cycle. Blocking work is awaited before state is released:
the unit already running when the stop arrives, one shard's files, one runtime
build or self-check, or one Display recent-shard build or runtime preparation, finishes
first. Activation that has begun still completes; inventory preparation is
already complete at that boundary.
Application lock files for Enhance, Status and Receiver prevent two updated
processes from owning the same state. Older binaries do not know these locks:
operator cutover must stop their services first. Do not forcibly remove locks
or start a replacement while the previous process is still stopping.

## Autonomous Status HTTPS publication

Status captures complete source observations independently of consumers, prepares
worker database units and router packing material on the publisher, and offers
`/provider/status/v1/` behind the existing TLS edge. Its private configuration
keeps `state_dir`, `salt_hex`, `window_blocks` and `private_listen`; worker/router
control origins are removed. It never waits for consumer acknowledgments.

Only a verified source observation supplies a new timestamp. Immutable artifacts
and prepared allocations are reused across unchanged observations. Identical
observations keep the current publication until it is five seconds old, then
refresh it from a newly checked observation; changed data, anchors and recovery
publish immediately. This avoids replacing authority on every source poll while
consumers finish their live checks. Query servers independently fetch,
verify, load and activate material through `pir-sync-manager`; Status no longer
uses publisher heartbeats. The twenty-second wallet observation-age limit is
unchanged. See [Status distribution](status/status_distributed.md).
This describes source capability, not a production cutover record.

## Transparent HTTPS publication

History and transaction details always publish full prepared publications over
HTTPS. Runtime preparation and the distribution API are unconditional, and the
publisher never contacts, prepares or activates query servers. History has no
fleet command, fleet configuration or shadow mode; transaction details has no
workers file and publishes only from its live node (`rpc_cookie` is required).
The retired `http_only`, `distribute_prepared`, `ship_runtimes`, `shadow`,
`fleet_command` and `fleet_config` History settings and the `http_only`,
`ship_runtimes`, `workers`, `mode`, `blocks_per_step`, `step_interval_ms`,
`replay_end`, `exit_when_idle` and `trust_sealed` transaction-detail settings are rejected at
startup. Installed settings must already use the current format. Ordinary updates
preserve every product setting exactly; the original configuration remains in
the deployment journal for rollback.
The publisher still needs its existing journal, initial publication or display
controller root, node access and durable state. It finalizes data without waiting
for consumers, and refuses to publish incomplete prepared material.

History and Display finalization list a source table with the digest their
verified set proved when this process hashed that same file and its device, inode, length,
modification and change times are unchanged; any other table is hashed again,
and every listed table must match its manifest digest. A runtime self-check
reads its source rows under the same rule. Runtimes use the same exact-file evidence: unchanged objects are neither
rehashed nor loaded again after this process has verified and self-checked them.

Both products store large immutable files once in `.objects/`, with checked
relative references from publication directories. Runtime preparation verifies
source tables and runtimes before publication. Active views and HTTP readers
retain their publication directories while collection removes only unowned
material. Object installation and reference creation hold a shared store lock;
collection holds its exclusive lock throughout the reference scan and deletion.

Storage conversion and historical ownership/HTTPS cutover commands are retired.
This tool version requires current publication roots with completed storage-layout
records and current four-product configuration. It does not translate legacy
settings or load historical migration requests for recovery.

Before updating to this tooling, finish storage migration and qualification,
explicitly retire old-root rollback and clean up retained roots using the pinned
migration package, and establish an ordinary `publisher-cutover/3` deployment as
the recovery baseline. Retain the original package, requests and evidence for
historical inspection. Current deployment, restart, interrupted-installation
recovery and rollback to the immediate predecessor remain supported.

Transaction-detail activation first saves a durable intent containing the next
restart record and HTTP authority, then commits both records before switching the
in-memory view. Startup completes any intent under the root lock before opening
state or collecting files. Recovered material still needs preparation and a live
chain check before serving.

History keeps `/provider/v1/`. Display offers the same discovery/immutable-file
pattern under `/provider/txid/v1/`, with a distinct inventory schema. Each product
has its own revision and live canonicality checks. These are distribution APIs;
the wallet's `/v2/` routes, payloads and protocol identities are unchanged.
A TLS edge may expose both distribution prefixes from one origin while the
products retain separate loopback listeners.

For complete local history, run `pir-query-server --config FILE` with a
`transparent_history` section whose settings name `"publisher": "<HTTPS-origin>"`,
its existing managed settings and `"full_holdings": true`, without an archive
upstream. For display, run a separate `pir-query-server --config FILE` with a
`transaction_details` section: `"role": "combined"`, `"publisher"`, an absolute
`"state_dir"`, `"publisher_network": "main"` and `"publisher_genesis"`, with an
optional `"publisher_ca"` PEM file. See [Query server host](query-server.md#transparent).
Both processes need explicit cache and download budgets sized for their complete
holdings, including active/candidate overlap. Full holdings are not a claim that
production history fits on one machine.

Each consumer owns a separate durable directory. It downloads missing immutable
objects, verifies and warms a new serving view, rechecks publisher authority,
then persists and atomically activates it. Required runtimes include archives;
missing or invalid files never trigger a local PIR build. A failed ordinary
update preserves a valid predecessor. Restart reuses verified completed files
but still requires fresh HTTPS authority; cached bytes alone cannot start a
wallet listener. `/v1/provider-status` is private operational status, separate
from wallet routes. Synchronization runs independently of wallet requests.
A withdrawal observed during preparation fences affected active and retained
views immediately, even when the incoming candidate has different revision
identities. Every authority recheck also validates the exact anchors of active
and retained views before rejecting a superseded candidate. This fences orphaned
answers when a consumer skips a withdrawal and sees the recovered canonical
publication directly; a still-canonical predecessor remains available. A display publisher restarted after a recent reorg leaves its saved
noncanonical view unpublished while normal journal reconciliation rebuilds it;
invalid material, a different chain identity or RPC failures still refuse startup.

Journal reuse is independent of distribution state. Existing version-2 journals
retain fixed 87-byte records when appended; version-3 journals retain their
length-prefixed records. Version-2 metadata is restored from committed display
source sidecars when available and otherwise remains unknown. Existing source
sidecars must be copied with the journal. Unsupported metadata, a different
chain identity or a different start height is refused without moving data aside.
No conversion, journal consolidation or historical re-ingestion is part of this
prototype. Derived publications and prepared runtimes may be rebuilt locally
from the retained source data.

Qualification must include complete, consistent production snapshots and the
unchanged current released wallet at its existing origin with valid TLS. Synthetic
HTTPS tests and client-library tests do not establish either gate. Never run a
second writer against a live journal or test against a real wallet database.

## Release and deployment preparation

The `pir-publisher` artifact is built in the native CPU release lane and contains
the binary, example configuration, unit template and opt-in descriptor. Its
release revision and checksums are verified through the existing artifact
resolver. No CUDA build is required to run the CPU publisher.

The [descriptor](../tools/deploy/publisher/services.toml) represents the shared
process once as service `publisher`, role `server`. Use it explicitly with
`wallet-pir-deploy.py --descriptors tools/deploy/publisher/services.toml`.
The existing default fleet registry and product worker inventories are unchanged.
The unit reads operator-managed `/etc/wallet-pir/publisher.json`; the example
must never replace a live configuration automatically. This descriptor prepares
binary/service deployment, not an automatic consolidation of old fleet inventories.

The deployment inventory's `services.publisher` entry must contain one server,
`vars.management_listen`, a `managed_products` list matching enabled sections,
and an `exact_check` that runs the existing independent encrypted-answer probes
for every enabled product. Inventory loading rejects standalone publisher roles
for those products in the same inventory. Separate inventories cannot discover
one another: the operator must review the full live fleet for old owners.

Before any production activation:

1. Capture fresh units, configuration, process identities, chain sources,
   private/public addresses, probes and current publication state.
2. Map existing writable paths and worker/artifact origins into the unified
   configuration. Retain schemas, runtime-shipping policy and publication modes.
3. Review enabled products against `managed_products`, credential access,
   memory/cpu headroom and every product's existing qualification requirements.
4. Stage the configuration and run its static check, then the deploy tool's
   plan and preflight under the normal production contract. Prepare initial
   unit/configuration installation explicitly; binary deployment is not data migration.
5. Obtain production approval. Stop the old owners before the unified process
   acquires state. Install through the locked deploy tooling, with existing
   public edges and query-worker placement preserved.
6. Check aggregate readiness, each product's publication/canonicality and the
   independent encrypted-answer probes. Report deployment status and rollback.
7. To reverse the cutover, first stop and join the unified process, then restore
   the retained standalone unit configurations and binaries over the unchanged
   data formats. Rerun all affected probes.

Receiver currently runs on another host. Enabling it on the coordinator needs a
separately reviewed relocation of its index, witnesses, provider state,
credentials, node access and serving edge. This implementation does not perform
that relocation, modify DNS or change any production host.

## Updating the unified coordinator

New shared discovery-contract releases use the topology-independent
[`fleet-release-*` transaction](operations.md#coordinated-contract-releases).
Same-major updates select only changed processes. A publisher major change
requires compatible candidates for every registered consumer across Enhance,
Status and Transparent products, including each enabled product in a unified
query process. Shared discovery compatibility and private query-to-query
protocol qualification are separate requirements. The following
`publisher-cutover-*` interface remains for retained transactions and its existing
product-specific rollout; it refuses shared contract-major changes.

Production publishing runs through `pir-publisher --config FILE`; wallet routes
belong to query ingress. Use `publisher-cutover-*` in
`tools/scripts/wallet-pir-deploy.py` from a clean pinned checkout or a matching
verified operations bundle. New plans require the running shared publisher,
its current product settings, and already-installed wallet routes. Four-product
updates and five-product updates including Receiver use the same request; historical
ownership and HTTPS migrations have no recovery entry points in this tooling.

Prepare a private `publisher-cutover/3` request with the coordinator `host` and
`machine_id`, `release` (`git_sha`, `binary_sha256`, `binary_bytes`), the installed
four-product `configuration`, a complete `inventory`, and independent exact-answer
`checks` for Enhance, Status, Transparent history and transaction details. Both
Transparent settings preserve their installed distribution mode. The inventory uses
`tools/deploy/publisher/all-services.toml` and has one publisher server managing
all four products, with no standalone publisher roles. Its `exact_check.products`
matches the request checks. Credentials stay in their existing files.

To adopt Receiver into that process, add its enabled `receiver` configuration,
independent `checks.receiver`, and `receiver` managed product. Keep the Receiver
service entry with no `server` role and retain its routing ancillary entry.
The optional `receiver_adoption` object has `host`, `machine_id`,
`config_path` (`/etc/receiver-pir-publisher.json`), `data_dir`
(`/srv/receiver-pir/index`) and `publisher_origin` (the main publisher's existing
HTTPS origin), and an optional `mode` (`transfer` or `reingest`). It is accepted only when the installed shared configuration has
no Receiver. Preserve all Receiver settings except changing `bind` to
`127.0.0.1:18380`; the shared chain connection replaces its separate node.
Provision the source unit's existing Infisical environment file at the same path
on the destination before preflight. The plan compares its digest without
reading credential values into requests or journals.

With `mode: reingest`, the shared publisher starts with a fresh Receiver index
while the old publisher and its HTTPS origin keep serving. After ingestion is
ready, the transaction stops both writers briefly, carries forward only the
authority counters and withdrawal floor, and restarts the shared publisher.
It then switches the HTTPS provider edge and disables the old owner. Databases
and publication objects are rebuilt on the destination. Rollback returns the
authority floor before restarting the old owner against its retained index.

The default transfer transaction stops and disables the old Receiver owner, verifies its state
lock, streams its complete stopped directory through gzip with a checked file manifest and
starts Receiver inside the shared process. It adds the exact writable path and
increases the shared service's memory ceiling from 40 to 47 GiB. Receiver's
hostname keeps its wallet upstream and forwards provider requests and discovery
over verified HTTPS to the shared publisher. Query replicas keep their existing
Receiver origin. Verify fresh synchronization and an encrypted Receiver lookup
on both replicas; a cached inventory is not evidence of a successful move.
Rollback returns the latest Receiver state before restarting the old owner, so
publication authority does not return to pre-cutover counters. Interrupted
adoption uses `reconcile` or `rollback`, followed by a fresh plan; ordinary
four-product installation recovery remains unchanged. Later publisher requests
retain Receiver but omit `receiver_adoption`.

For an explicitly scoped operational rollout, publisher and fleet-release
requests may set `probe_products: ["receiver"]` to run only Receiver retrieval
acceptance. All enabled product configuration and process readiness checks
remain in place; receipts record the requested scope. Identical fleet probes
are run once in this mode. `publisher-cutover-rollback --skip-exact-checks`
restores services and verifies readiness without repeating retrieval probes,
for example when a retired query hostname invalidates the original probe.

Publisher updates preserve the installed product settings exactly. Retired
transport, replay and trust settings must already be absent from the installed
configuration. Planning never silently converts settings or moves storage roots
without the explicit Receiver adoption above.
The transaction retains its predecessor's configuration, unit and executable
identities for drift checks and rollback.

Private control changes require the publisher, Enhance packing router and query
ingress to change together. The `serving` object pins the matching query-server
`git_sha`, `binary_sha256`, `binary_bytes` and installed `caddy_sha256`. Supply
`--serving-binary` to plan, preflight and deploy. Existing workers retain their
wallet wire protocol. Ordinary publisher updates do not restart Transparent workers.

An ordinary `publisher-cutover/3` request may set `resume_load: false` when the
continuous checker is already inactive or failed with `MainPID=0` and has no
critical latch. The update and subsequent restart leave it stopped; rollback
restores its captured activity. Omitting the option retains the running-checker
requirement and resumes it after the independent encrypted-answer probes.


Run plan and preflight with `--request` and its exact `--request-sha256`;
preflight also takes the clean `pir-publisher` `--archive`. Review the returned
`plan_sha256`, then deploy with `--expect-plan-sha256` and the same inputs.
The tool pins consumed inventories and pool policies, preserves the installed
sandbox, environment-file references and resource limits, and requires 50 GiB
host memory plus twenty percent available memory and disk. It refuses changes
to product policy, foreign file changes, competing owners or altered binaries.

Under one production lock, deployment stages and checks both releases before
stopping the publisher. It pauses continuous load, starts matching serving roles
and publisher, verifies readiness, and runs all four encrypted-answer probes
before resuming load and committing. The wallet routes remain unchanged and
unmatched requests remain 404. Every effect is journaled on the coordinator.

Deployment leaves History completion records to the publisher. Native startup
validates the active publication's existing `history-restore.json` and its
authority before serving, then verifies bodies in the background. If no record
exists, startup performs full preparation. Deployment does not pin or rescan
History artifacts before stopping the predecessor; readiness and all four
encrypted-answer probes still gate completion. Recovery of older transactions
that already prepared a pinned record retains their recorded install and
rollback behavior.

`publisher-cutover-status` reports the transaction and observed identities.
`publisher-cutover-restart` repeats cooperative shutdown, readiness and exact
checks without replacing the rollback baseline. `publisher-cutover-rollback`
and `publisher-cutover-reconcile` restore the saved edge, serving roles and
publisher together, then repeat readiness and product probes. A failed deploy
rolls back automatically; if that rollback also fails, the journal's `failure`
keeps `deploy_error` and `rollback_error` as separate fields. Only `publisher-cutover/3` journals are supported. Retired storage and HTTPS
handoff fields are rejected, including for recovery. Historical migrations must
be closed before installing this tool version; use their pinned original package
when inspecting their retained records.

Publisher updates do not deploy GPU artifacts or replaces production-snapshot, native-wallet
and sustained-service qualification.

The APM sidecar must match the wallet ownership change: its init and query
metrics come from query ingress, while publisher health and publication metrics
stay on the publisher. Update its binary through the opt-in descriptor
`tools/enhance/deploy/apm.toml` with the normal baseline, plan, preflight, deploy,
status and rollback commands. This preserves its unit arguments, credential
files and durable incident/delivery state. Acceptance requires fresh monitoring
coverage; a stale sidecar can report missing init coverage despite correct
wallet answers.

## Fast branch builds over SSH

Branch-built artifacts can use the existing deployment transactions directly;
they do not need to be downloaded from CI. Check SSH reachability and use a clean,
visible build checkout. Fetch the selected branch once, pin its full SHA, and
reuse the host's existing target directory, toolchain and native build flags.
Do not change a live checkout or delete its build cache. Build the two hosts
sequentially within one target lane, preserving all enabled product features:

```sh
deploy_sha=$(git rev-parse HEAD)
bounded-run --name publisher-branch-build --limit 30 --max 1800 -- \
  cargo build --locked --release -p pir-publisher \
  --features enhance,status,transparent-history,transparent-detail,receiver,native-reinspiring
python3 tools/ci/release.py assemble --sha "$deploy_sha" --kind pir-publisher \
  --target "$CARGO_TARGET_DIR" --output .local-pir/publisher-branch-artifact
bounded-run --name query-branch-build --limit 30 --max 1800 -- \
  cargo build --locked --release -p pir-query-server \
  --features enhance,status,transparent-history,transparent-detail,receiver,native-reinspiring
python3 tools/ci/release.py assemble --sha "$deploy_sha" --kind pir-query-server \
  --target "$CARGO_TARGET_DIR" --output .local-pir/query-branch-artifact
```

If a bounded build is still running, finish it with the printed `--wait` command
before assembling or starting another Cargo writer. Choose fresh output paths
for subsequent revisions. Preserve any additional feature required by the
installed query variant; CPU artifacts do not replace CUDA artifacts.

Run `publisher-cutover-plan` and `publisher-cutover-preflight` with the pinned
request and publisher archive. After production confirmation, use
`publisher-cutover-deploy` with that plan digest. For a publisher-only update,
pin the installed Enhance serving binary and set `preserve_serving: true`.
Then update each combined query host sequentially using `fleet-release-*` and
the query archive. That binary-only path retains the installed configuration,
environment, thread settings and resource policy. Prepare its immutable binary
only after deployment is authorized. Each Receiver-enabled combined host must
have a separate `receiver` encrypted-answer check in `exact_check.products`.
Keep `status` and `rollback` inputs alongside each transaction. These are
unqualified branch artifacts: focused offline checks, readiness and the existing
encrypted probes remain required; a full integration campaign is not part of
this SSH iteration.

## Product libraries and commands

`enhance-publisher`, `status-publisher` and `transparent-publisher` are libraries
with no executable targets. Build and run `pir-publisher` for publishing and
maintenance. Its independent product features are `enhance`, `status`,
`transparent-history`, `transparent-detail` and `receiver`; defaults enable none.
The two Transparent features select separate configuration sections and owners.

Long-running services use `pir-publisher --config FILE`. Existing history and
display settings live under `transparent_history.settings` and
`transaction_details.settings`. The former per-block `transparent_filters`
section is removed, and a configuration that still names it is refused; delete
it, and the filter unit that ran it, before upgrading.

Maintenance uses root subcommands:

| Former executable | Root command |
|---|---|
| `enhance-pir-server` | `pir-publisher enhance` |
| `status-pir` | `pir-publisher status` |
| `shard-publish` | `pir-publisher publish-history` |
| `transparent-event-ingest` | `pir-publisher ingest-events` (requires `rocksdb-reader`) |
| `txid-display-controller` offline commands | `pir-publisher detail` |
| `transparent-publisher --check-shard-dir` | `pir-publisher verify-filters --shard-dir DIR` |

The standalone history controller and display `run` command are removed. Their
services run through the root configuration and publish live over HTTPS.
No executable aliases or data conversion are needed. Install the matching binary,
configuration and unit together through the supported deployment tooling.

For a stopped-checker repair, `publisher-cutover/3` may pin an existing disk-capacity incident with `expected_disk_latch_sha256` and `resume_load: false`. Only the known History worker/router disk-headroom reasons are accepted. The latch must remain byte-for-byte identical; it is never removed or acknowledged, and the checker stays stopped through deployment and rollback. Answer failures, memory incidents, OOMs and changed latches still refuse the operation.

A `publisher-cutover/3` request may set `preserve_serving: true` for a publisher-only update. Its serving release keeps its own exact source and binary pins; every serving role must already match the requested binary and configuration. Any required serving change refuses the update. The publisher and wallet probes still run, and the discovery edge route is journaled normally.

If the connection was lost while stopping the predecessor, `publisher-cutover-continue` can finish that exact stopped-checker transaction. It requires the recorded stop intent, unchanged publisher configuration and inputs, unchanged serving processes, and the original staged executable. It refuses a running publisher or any other interrupted phase; ordinary verification recovery still uses `publisher-cutover-verify`.

If restoration reached its recorded publisher start and both serving roles passed
their restored identity checks, but an older Status diagnostic client fails on
expired sessions, use `publisher-cutover-repair-probe-plan` and then
`publisher-cutover-repair-probe-verify --expect-plan-sha256 <digest>` with the same
request and checksum. This recovery selects only the staged publisher executable
as the Status probe client and updates only its digest literal in the harness.
It keeps the original harness logic, independent oracle,
load and deadlines, records the client digest, and repeats all four exact-answer
checks without restarting any service. It refuses pending effects, changed
restored files or process identities, a changed probe executable, and a stale
plan. The stopped checker and its incident latch remain unchanged.
