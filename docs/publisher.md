# Unified publisher

`pir-publisher` is one process that runs any configured subset of Enhance,
Status, Transparent history, transaction-detail and Receiver publishing. Its
management server owns listener binding and process shutdown. Product libraries
continue to own chain acquisition, journals, preparation, distribution, canonical
checks and activation. Products do not wait for a common publication height.

## Source and dependency ownership

Publisher packages live under `src/shared/pir-publisher/`: `server` is the new
`pir-publisher` application; `pipeline` retains the `pir-pipeline` package;
`enhance`, `status`, `transparent` and `receiver` retain their existing publisher
package names. `receiver-store` holds the indexer's SQLite stores. History and
transaction details remain separate modes of `transparent-publisher`.

Query servers remain under their product directories. Serving libraries must
not depend on publisher packages, even though the latter now live under
`shared`. Shared protocol, verification and runtime libraries remain outside the
publisher subtree. The pipeline depends only on Tokio: its HTTP adapter is
generic over a product's router, and the application supplies the actual server.

This layout changes no wallet protocol, record codec, publication identity,
journal, database or runtime-cache format. Existing standalone commands remain
available. Crate relocation does not relocate deployed data or change upstream
wallet-libraries revisions.

## Configuration and local use

Build the native CPU application used by the deployed protocols:

```sh
cargo build --locked --profile release-fast -p pir-publisher --features native-reinspiring
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
still reads `NEAR_INTENTS_EXPLORER` at runtime.

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
