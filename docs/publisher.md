# Unified publisher

`pir-publisher` is one process that runs any configured subset of Enhance,
Status, Transparent history, transaction-detail and Receiver publishing. Its
management server owns listener binding and process shutdown. Product libraries
continue to own chain acquisition, journals, preparation, distribution, canonical
checks and activation. Products do not wait for a common publication height.

## Source and dependency ownership

The publisher host lives at `src/pir-publisher`. Each product publisher lives
beside that product's query server: `src/enhance/enhance-publisher`,
`src/status/status-publisher`, `src/transparent/transparent-publisher`, and
`src/receiver/receiver-indexer`. Those packages keep their existing names.
`src/shared/pir-pipeline` is the shared lifecycle library, and `src/shared`
otherwise holds shared libraries only. `src/receiver/receiver-store` holds the
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
enabled: `enhance`, `status`, `transparent`, or `receiver`. `transparent`
compiles both history and transaction-detail publishing. `native-reinspiring`
selects native packing for Enhance and Status and does nothing unless that
product feature is also enabled. The publisher host has no CUDA feature. A
configuration section for a product that was not compiled is rejected.

This layout changes no wallet protocol, record codec, publication identity,
journal, database or runtime-cache format. Existing standalone commands remain
available. Crate relocation does not relocate deployed data or change upstream
wallet-libraries revisions.

## Configuration and local use

Build the native CPU application used by the deployed protocols:

```sh
cargo build --locked --profile release-fast -p pir-publisher \
  --features enhance,status,transparent,receiver,native-reinspiring
pir-publisher --config /etc/wallet-pir/publisher.json --check
pir-publisher --config /etc/wallet-pir/publisher.json
```

The [all-product example](../tools/deploy/publisher/publisher.example.json)
is a typed configuration example, not a production inventory. Replace its
addresses, state paths, bootstrap inputs, inventories, public Status salt and
release identity with the operator's existing values. Preserve each product's
node-selection and authentication policy; Receiver may read several nodes.

The five product sections are `enhance`, `status`, `transparent_history`,
`transaction_details` and `receiver`. Each has `enabled` and `settings`.
Omitted sections are disabled. A disabled section may omit settings and starts
no task, opens no store and reads no credential. Product settings retain their
existing names and semantics; non-optional settings must be explicit. Receiver
must use continuous serving, and Enhance's embedded Status setting is replaced
by the separate Status section. No configuration reload is performed.

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
Application lock files for Enhance, Status and Receiver prevent two updated
processes from owning the same state. Older binaries do not know these locks:
operator cutover must stop their services first. Do not forcibly remove locks
or start a replacement while the previous process is still stopping.

## Autonomous Transparent HTTP prototype

History and transaction details can use `http_only: true` in their existing
settings. History also requires `ship_runtimes: true` and
`distribute_prepared: true`; transaction details requires `ship_runtimes: true`.
In this mode the publisher does not contact or activate query servers, so the
history fleet command/configuration and display workers file can be omitted.
The publisher still needs its existing journal, initial publication or display
controller root, node access and durable state. It finalizes data without waiting
for consumers, and refuses to publish incomplete prepared material.

History keeps `/provider/v1/`. Display offers the same discovery/immutable-file
pattern under `/provider/txid/v1/`, with a distinct inventory schema. Each product
has its own revision and live canonicality checks. These are distribution APIs;
the wallet's `/v2/` routes, payloads and protocol identities are unchanged.
A TLS edge may expose both distribution prefixes from one origin while the
products retain separate loopback listeners.

For complete local history, run `transparent-query-server --publisher <HTTPS-origin>`
with its existing managed settings and `--full-holdings`, without an archive
upstream. For display, run `transparent-txid-server --role combined --publisher
<HTTPS-origin> --state-dir <absolute-directory> --publisher-network main
--publisher-genesis <hash>`, with an optional `--publisher-ca <PEM-file>`.
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

## Transparent coordinator cutover

The existing v12 history and 4k8 display controllers can be replaced by one
Transparent-only process without moving their journals or publications. Use the
`publisher-cutover-*` commands of `tools/scripts/wallet-pir-deploy.py` from a clean
pinned checkout or matching verified operations bundle. This path deliberately
keeps fleet-controlled distribution; both products require `http_only: false`.
It does not deploy or move query workers, change public routes, or enable the
other publishers.

Prepare a private request with schema `transparent-publisher-cutover/1`, the
inventory coordinator `host` and its `machine_id`, and `release` containing the
full `git_sha`, `binary_sha256` and `binary_bytes`. `configuration` is the typed
publisher configuration with exactly `transparent_history` and
`transaction_details` enabled and management at `127.0.0.1:18080`. The history
settings must match the current controller configuration; display settings must
match the existing run command, whose initial replay must already have reached
live mode. Make existing false distribution/runtime defaults explicit. Keep the
publication's existing `source_sha`; the executable's release identity is recorded
separately. Unknown display command options are refused rather than ignored.

`inventory` is the complete Transparent fleet under
`tools/deploy/publisher/transparent-services.toml`: retain the history/display
workers and routing ancillary records, replace their controller roles with one
publisher server, and set its `managed_products` to
`["transparent_history", "transaction_details"]`. Each entry of `checks` uses
one of those two product names and contains `host`, `argv` and `timeout` for an
independent encrypted-answer probe. Timeouts are at most 900 seconds. Credentials
remain in the existing runtime files, never in the request. Operator requests
and inventories stay outside Git.

Run `publisher-cutover-plan` and `publisher-cutover-preflight` with `--request`
and its exact `--request-sha256`; preflight additionally verifies `--archive`.
Review the returned `plan_sha256`, then pass it as `--expect-plan-sha256` to
`publisher-cutover-deploy` with the same inputs. The archive is the clean,
checksum-verified `pir-publisher` artifact for that source revision, built with
`--features transparent`. Its actual typed configuration check runs on the
coordinator before any existing publisher is stopped. This path does not require
main CI status and does not change the CI-qualified artifact resolver's policy.

The deployment holds the normal production lock, stages the executable, pauses
the continuous load, stops the old owners and checks that their state locks are
free. It installs the restricted 12 GiB shared service budget, publisher
configuration and `/etc/wallet-pir/transparent-fleet.json`. Existing node and
worker locations remain unchanged. The capacity sampler is retargeted to the
shared process and the continuous-load controller-unit reference is updated.
The load resumes only after aggregate readiness and both encrypted probes pass.
Keep at least 20 percent memory and filesystem headroom; the executable does not
provide memory isolation between products.

Use `publisher-cutover-status` to inspect observed unit identities and the
coordinator's journal. Every mutation records intent and completion in
`/opt/pir-publisher/transactions/<id>/journal.json`; a runner-side copy is retained
in the chosen state directory. A failed or interrupted deployment is unfinished,
not accepted. `publisher-cutover-reconcile` restores the captured predecessor
ownership, as does explicit `publisher-cutover-rollback`. Both take the original
request and digest. They refuse foreign file changes, stop and join the unified
process before restoring the old owners, and verify both products before
resuming load. They restore absent files to absence and never rewind journals or
publications. Retained executable releases are kept for recovery.

Deployment acceptance additionally requires independent canonical publication
progress, the deployed wallet regression suite with SQLite reopen/recovery,
recent/archive and absent display checks, a graceful restart, rollback and
recutover, and the existing bounded load gate. A configuration check, management
readiness or a passing CI run alone does not establish that acceptance.

During a cutover or rollback rehearsal, monitoring stays enabled. Stopping the
publisher can therefore fire several coverage warnings after the 45-second
observation deadline: source, host, HTTP and canonical/public-origin verification
are separate checks. Each emits its own recovery once fresh observations return.
These warnings describe missing observations, not a detected wrong-chain
publication or incorrect answer. Inspect the incident records and verify recovery
before continuing; do not infer that every alert during a deployment is expected.
Tell the operator before starting a live recovery rehearsal.

The focused `CI full` dispatch with `publisher-only: true` also packages the
current `rate-query` and `txid-rate` reference clients. Use those pinned clients
with the independently exported fixtures for bounded live checks, including
recent/archive found records and absent display controls. A legacy client that
rejects the fixture before querying provides no deployment correctness evidence.

## Expand the coordinator to Enhance and Status

The versioned request `publisher-cutover/2` adds Enhance and Status to the
running Transparent publisher. It enables exactly `enhance`, `status`,
`transparent_history` and `transaction_details`; both Transparent settings
retain `http_only: false`. Use `tools/deploy/publisher/all-services.toml` for
one complete inventory with the existing query workers, routers and supervision.
Remove Enhance coordinator and Status controller roles from that inventory;
`publisher.managed_products` lists the four sections in the order above. Its
`exact_check` is a `products` map containing the same four host/argv/timeout
checks as the request, so later binary deployments also verify every product.

The tool translates the effective commands of `enhance-pir-coordinator.service`
and `status-controller-qualification.service`. Unknown options, embedded Status,
changed Transparent settings and conflicting owners are refused. Retain the
existing Enhance inventory/pool policy, Status controller configuration, node
cookie and environment-file references. The reviewed plan also pins referenced
configuration contents. Credentials stay in their existing files.

Run the same `publisher-cutover-plan`, `preflight` and `deploy` commands with
this request. Capture a fresh plan before each cutover. The transaction records
the running Transparent-only unit and both standalone publishers, so rollback
restores that mixed deployment rather than starting retired Transparent owners.
`publisher-cutover-restart` cooperatively restarts the committed shared process
and repeats every product's exact probe without replacing its rollback baseline.
An interrupted restart is reconciled through the same rollback operation.

The expanded unit uses a 40 GiB combined memory ceiling with no swap, matching
the sum of the predecessor limits. Preflight requires at least 50 GiB total RAM
and twenty percent available memory and disk. The limit is a ceiling, not a
reservation or hardware qualification. Query workers and public routes are not
changed. Announce intentional cutover and rollback rehearsals before execution;
keep their expected coverage alerts separate from the healthy acceptance window.
Shared listener ownership and graceful shutdown continue to use the existing
`pir-pipeline` HTTP adapter. The HTTPS synchronization manager is unchanged.

## Retired standalone production commands

Production publishing runs through `pir-publisher --config FILE`. The removed
commands are `enhance-pir-server coordinator` and `status-pir serve-distributed`;
Enhance's unused `status_config` field is also rejected. Maintenance and probe
commands remain available under their existing binary names. Current inventories
use the publisher role; historical commands are parsed only for cutover. Rollback
restores captured units and old executables, which must remain available through
the rollback rehearsal. CUDA smoke callers now pass `--publisher` and use isolated
Enhance fixture state.

## Enhance publisher and wallet route cutover

Enhance private control version changes require the publisher, packing router
and query ingress to change together. Use `publisher-cutover-*` with schema
`publisher-cutover/3`, the four-product configuration and complete inventory.
The additional `serving` object pins `git_sha`, `binary_sha256`, `binary_bytes`
and the installed `caddy_sha256`. The publisher and serving source revisions
must match. Supply `--serving-binary` to plan, preflight and deploy. Existing
workers retain their wallet wire protocol and are not restarted by this command.

The plan captures both serving units, configurations, old executable identities
and the entire Caddyfile. The only accepted publisher configuration changes are
removal of the retired Enhance `status_config: null` and Status wallet gateway
fields (`public_listen`, `public_enabled`, `query_router_origin`). Product
publication settings and Transparent distribution policy remain unchanged.
Recognized historical ingress command and writable-path overlays are saved for
rollback; unrecognized service directives still refuse adoption.

Deploy stages both releases and checks their configurations before stopping any
owner. Under one production lock it starts the matching serving roles, starts
the publisher, verifies aggregate and role readiness, then adds the existing
Enhance wallet prefix to the ingress. The dedicated query route and every other
edge route remain unchanged. Unmatched requests return 404, keeping publisher
control endpoints private. Independent encrypted-answer probes must pass for
all four products before commit. The serving journals are retained inside the
coordinator's publisher journal before effects.

`publisher-cutover-status` reports the transaction. `publisher-cutover-rollback`
(or `publisher-cutover-reconcile` after interruption) restores the edge, both
serving roles and the old publisher as one group, then verifies readiness and
all product probes. Foreign file changes or altered rollback binaries refuse
restoration. This path does not change DNS, add HTTPS distribution, update GPU
artifacts or qualify wallet recovery by itself; retain native wallet, public
HTTPS and sustained-service evidence separately.
