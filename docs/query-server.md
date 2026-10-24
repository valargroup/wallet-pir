# Query server host

`pir-query-server` is one process that serves any compiled subset of Enhance,
Status, Transparent history, and transaction details. The host crate is
`src/pir-query-server`. Its management server
reserves listeners and owns process shutdown. The host owns Status roles
directly: role configuration, HTTP routes, admission, telemetry, the generation
controller and publisher synchronization live in `pir_query_server::status`.
`status-shared` supplies generation material and the wire protocol. The
private router-worker messages live in `pir_query_server::status`, and the
root owns the public route mounts of every product. Enhance roles,
synchronization, HTTP surfaces and private evaluation messages live in
`pir_query_server::enhance`, over `enhance-shared` material. Transparent roles, publication managers, activation, validation,
archive forwarding and HTTP surfaces live in `pir_query_server::transparent`
over `transparent-shared` material. A Transparent provider selects its own
holdings (the whole recent tier within its cache budget); fleet planning, file
lists and edge rendering belong to `shard-assign` in `transparent-tools`.

## Public routes

One selected server base serves every product under its own namespace. Wallets
should use the canonical paths; the deployed paths remain explicit aliases of
the same handlers and return identical responses. Each product declares its
public operations once and mounts them at both prefixes through
`pir_control::public_routes::PublicMount`.

| Product | Canonical | Legacy alias | Operations |
| --- | --- | --- | --- |
| Enhance | `/enhance/v1/<op>` | `/v1/enhance/<op>` | `init`, `session/:id`, `sessions/:generation/:shard`, `query` |
| Status | `/status/v1/<op>` | `/v1/status/<op>` | `init`, `session/:id`, `query` |
| Transparent | `/transparent/v1/<op>` | `/v2/<op>` | `shards`, `shards/init`, `shards/:id/revisions/...`, `filters/shards[/:id/filter]`, `txid/...` |
| Transparent filters | `/transparent/v1/filters/<op>` | `/v1/filters/<op>` | `info`, `chain`, `digests`, `range` |
| Receiver | `/receiver/v1/<op>` | `/v1/receiver/<op>` | see the Receiver docs |

The two Transparent alias groups share the canonical namespace; their suffixes
are disjoint (`filters/shards...` versus `filters/{info,chain,digests,range}`)
and route construction panics if they ever overlap. An alias serves only its own
group, so `/v2/filters/info` and `/v1/filters/shards` stay unrouted. Health,
readiness, metrics, `/control/*`, `/internal/*`, `/evaluate`, `/v1/provider-*`
and publisher status are never mounted under a canonical prefix and stay on
their private paths. Peer-to-peer calls between roles, deploy probes and the
`pir-apm` quality probes keep the legacy paths that deployed binaries already
serve.

`https://wallet-pir.valargroup.dev` is the unified base for the canonical paths.
It is a second site on the coordinator's Caddy
([`tools/deploy/coordinator/Caddyfile`](../tools/deploy/coordinator/Caddyfile),
rendered by `wallet_pir_ops.deploy.coordinator_edge`, DNS in
`tools/infra/digitalocean/fleet/shared.tf`). Enhance, Status and Transparent
compact-filter operations reach the coordinator's loopback services. History
shard maps, shard filters, Transparent queries and Receiver cross to `transparent-pir` and
`receiver-pir` over verified HTTPS, naming each origin as Host and SNI. The edge
translates canonical paths to existing backend aliases where needed. Recovery
metadata and encrypted History queries share the query server's active publication. It
serves no legacy alias and no operator path. The product hosts keep serving both
forms for released wallets. Receiver's edge trusts only the coordinator's
address to forward `X-Forwarded-For`, so its per-client admission still keys on
the wallet. The site reaches the live coordinator file only through the
transactional `wallet-pir-deploy coordinator-edge-{plan,preflight,deploy,status,rollback}`
operation (`wallet_pir_ops.deploy.unified_edge`). Under the production lock it pins the
live file and coordinator identity, adds or replaces only the unified site, validates,
reloads, probes the host locally (canonical routes 200, legacy and operator paths 404)
and restores the exact previous file on any failure. Its request is
`{"schema": "coordinator-unified-edge/1", "host", "machine_id", "enhance_public_url"}`.
Product cutovers edit their own site in place and keep it. Whether the host is
live is recorded in the product status pages, not here. For an explicitly
forward-only rollout, pass `--preserve-on-failure` to `coordinator-edge-deploy`;
it leaves installed routes in place if verification fails. Complete that same
transaction with `coordinator-edge-verify` after repairing the cause.

When the coordinator still has the older flat Terraform root, use
`coordinator-edge-dns-plan` and `coordinator-edge-dns-stage` to stage only the
unified DNS resource under the production lock. Review and apply its saved
Terraform plan through the existing coordinator Terraform wrapper. This preserves
the resource address used by the fleet root and does not update any service.
Remove the staged `wallet-unified-dns.tf` when installing a fleet root whose
`shared.tf` already declares that record, keeping its existing Terraform state address.

## Features

The host compiles a product only when that product's Cargo feature is enabled:
`enhance`, `status`, `transparent-history`, or `transparent-detail`. The default
is none. The two Transparent features compile history and transaction-detail
serving independently; Transparent release kinds enable both.
`native-reinspiring` and `cuda` select those backends and do nothing unless the
matching product feature is also enabled. A configuration section for a product
that was not compiled is rejected, even when it is disabled.

```sh
cargo build --locked --profile release-fast -p pir-query-server \
  --features enhance,status,transparent-history,transparent-detail,native-reinspiring
pir-query-server --config /etc/wallet-pir/query-server.json --check
pir-query-server --config /etc/wallet-pir/query-server.json
pir-query-server --config /etc/transparent-pir/shard-server.json --verify-only
pir-query-server check-backend [--matvec-backend cpu|cuda] [--cuda-device N]
pir-query-server repair-rows --data-dir DIR --source-rows DIR
```

A process includes at most one Enhance role. Workers, query ingress and packing
routers run only as `pir-query-server --config FILE`. Offline Enhance maintenance
runs as `pir-publisher enhance` (`build-info`, `prepare-publication`,
`rebuild-journal` and the HTTPS cutover's `handoff-https-state`). `build-info`
reports native reinspiring and the source revision; it has no CUDA field.

`check-backend` validates the selected evaluator, and for CUDA the driver,
without opening worker state. `repair-rows` repairs journal-referenced rows
from a peer row archive while the worker is stopped. `status-pir` remains its
own command. Transparent history and transaction details have no separate
server executables; they run only as `pir-query-server --config FILE`. The
Transparent administration tools `shard-assign` and `shard-prune` are built
from `transparent-tools`. `shard-assign` needs the `query-host` feature.

## Configuration

The [example](../tools/deploy/query-server/query-server.example.json) is a typed
configuration, not a production inventory. Sections are `enhance`, `status`,
`transparent_history`, and `transaction_details`. Each has `enabled` and
`settings`. An omitted or disabled section starts nothing.

Wallets reach every enabled product on one top-level `public_listen`, behind
the external HTTPS edge that terminates TLS for each existing origin. It is
required when an enabled role answers wallets (Enhance `query-ingress` or
`combined`, Status `router` or `combined`) and refused when none does. Each
product serves only its own wallet namespaces there, at both the canonical and
the deployed prefixes: `/enhance/v1` and `/v1/enhance`, `/status/v1` and
`/v1/status`, and below `/transparent/v1` and `/v2` the history `/shards` and
`/filters` operations and the transaction-detail `/txid` operations. Health,
readiness, metrics, control, provider and evaluation routes never appear on it;
they stay on each product's private listener. A product's namespaces answer 503
until it can serve and after it stops, while other products continue. The edge
keeps each origin's existing route allowlist; the shared listener does not
widen what an origin exposes.

Enhance settings name one role: `worker`, `query-ingress`, `packing-router`
or `combined`.
Every role takes a `synchronization` object naming its HTTPS
`publisher_origin`, optional `trusted_ca`, `download_budget_bytes` and
`cache_limit_bytes`: workers add their `shards`, query ingress its `routers`
and packing routers their `workers` (and optional `preferred` workers) per
domain. A worker also needs `listen`, an absolute `data_dir`, and optional
`sealed_shards` (default 6) and `matvec` (`matvec_backend` `cpu` or `cuda`,
and `cuda_device` only for CUDA). Query ingress needs `data_dir`,
`control_listen`, and optional `requests` (default 16) and `uploads` (default
256). A packing router needs `data_dir`, `listen` (where its ingress reaches it),
`control_listen`, and
optional `max_objects` (default 6) and `requests` (default 4). A `combined`
Enhance role runs a query ingress, packing router and worker for the worker's
`shards` in one process, calling each other directly instead of over private
HTTP, so it names no routers or workers and needs no bearer token between
them. Its `shards` must name every shard the publication covers: wallets
learn all of them from `init` and query them at this one origin, so a host
owning fewer refuses the publication by name in its log and `/internal/health`
(`ingress.blocked`), and stops before it has served any generation rather than
idling unready. Hosts that split shards run the distributed roles. It takes the worker's `synchronization`, `sealed_shards` and `matvec`, the
ingress's `requests` and `uploads`, the router's `max_objects` and
`packing_requests` (the router's `requests`), an absolute `data_dir` holding
`worker`, `router` and `ingress` state directories and the private
`control_listen`; wallets reach it on `public_listen`. Wallets see a generation only after all three hold
fresh authority for it, and its health is ready only while all three are.
Status settings
name `worker`, `router` or `combined`. Each requires `publisher` (an HTTPS
origin), `network_hex`, an exclusively owned `state_dir`, and loopback listeners.
`publisher_ca` optionally adds an installed PEM trust root. The router also
requires its loopback `worker_origin`. Router and combined roles answer wallets
on `public_listen`; their own `listen` keeps control, provider and telemetry
routes, and a worker's `listen` keeps evaluation. A `combined` role loads the evaluation units and the packing
material of the publisher's latest inventory and answers wallets by evaluating
in process, so it names no `worker_origin` and has no private evaluation or
handover routes. Status evaluation is CPU-only and has no `cuda` setting. The
publisher prepares the material; no query role computes hints or packing
matrices. See
[Status distribution](status/status_distributed.md). History and
transaction-detail settings are described under [Transparent](#transparent).

Listeners need distinct fixed loopback or private addresses. State directories
are absolute and must not overlap. `--check` validates the file and does not
bind sockets or open stores.

Deployment renders the optional `combined` roles of the `enhance` and `status`
services (`enhance-pir-combined.service`, `status-combined.service`) from the
inventory like the other role units, and checks each rendered file with
`--check` before restarting. The Enhance role takes its wallet listener as
`listen` and the Status role as `query_listen`; both become `public_listen`.
Neither accepts a peer origin.

## Transparent

A serving `transparent_history` or `transaction_details` section synchronizes
from its HTTPS publisher: it requires `publisher`, `publisher_network` and
`publisher_genesis`, with optional `publisher_ca` and `bootstrap_max_bytes`.
History keeps its downloaded state in `shard_dir` and may name an
`archive_upstream` (with `archive_ca`) or `full_holdings`; transaction details
keep theirs in `state_dir` and serve only `role` `combined`. Numbers are
integers and every object rejects unknown fields. A deployment renders one
Transparent section per file, so history and detail stay separate processes.
Their `listen` is the provider listener that fleet routers and private probes
reach; when `public_listen` is also set, the same process additionally answers
its wallet operations there.

```json
{
  "management_listen": "127.0.0.1:18190",
  "transparent_history": {
    "enabled": true,
    "settings": {
      "listen": "127.0.0.1:8093",
      "shard_dir": "/srv/transparent-pir/shards",
      "publisher": "https://publisher.example",
      "publisher_network": "main",
      "publisher_genesis": "00040fe8ec8471911baa1db1266ea15dd06b4a8a5c453883c000b031973dce08",
      "cache_bytes": 4294967296
    }
  }
}
```

```json
{
  "management_listen": "127.0.0.1:18195",
  "transaction_details": {
    "enabled": true,
    "settings": {
      "listen": "127.0.0.1:8095",
      "role": "combined",
      "state_dir": "/srv/transparent-pir/txid-state",
      "publisher": "https://publisher.example",
      "publisher_network": "main",
      "publisher_genesis": "00040fe8ec8471911baa1db1266ea15dd06b4a8a5c453883c000b031973dce08"
    }
  }
}
```

`pir-query-server --config FILE --verify-only` replaces the former servers'
`--verify-only`. The file must enable exactly one Transparent section and no
other product. With a publisher it synchronizes into the configured state
before exiting; without one it verifies a local `shard_dir` or
`publication_dir`. It binds no listener, prints the same JSON report and keeps the
same exit status. It conflicts with `--check` and the Enhance subcommands.
Configuration files never set `verify_only`; ordinary startup rejects
`verify_only: true`.

### Publisher compatibility

A section with `publisher` set synchronizes over HTTPS. Before the host binds
any listener, opens state or starts a product, it fetches the publisher's
`/.well-known/pir-service-discovery` document from that origin, trusting the
configured `publisher_ca` roots, following no redirect, within 10 seconds and
16 KiB. The publisher's contract version must have the same major version as
the query server's
[`pir-service-discovery`](../src/shared/pir-service-discovery/README.md)
version; minor and patch versions may differ in either direction. Any failure
(unreachable publisher, HTTP error or missing endpoint, another service, a
malformed document or version, or a different major version) stops the whole
query server with the publisher origin and reason. A mismatch names both
versions: an older query server is told to upgrade; a newer one is told the
publisher is older and must be upgraded, or replaced by a query server release
for the publisher's major version. There is no bypass. `--check` stays offline;
`--verify-only` also checks discovery.

Every synchronization pass, unchanged ones included, checks discovery again
before any product request. A network error or invalid response skips that
pass, keeps the current serving view and retries with the usual backoff. A
confirmed major version mismatch stops wallet serving and exits the whole
process with that error; a saved predecessor publication cannot mask it.
Sections without `publisher` are not checked.

An autonomous Enhance worker, packing router or ingress (an Enhance role with
`synchronization`) follows the same rules for its `publisher_origin`, trusting
its `trusted_ca`. On-demand authority refreshes for readiness and admission also
check discovery first. Legacy push-controlled Enhance roles have no publisher
origin and are not checked.

## Readiness

Management `/health` stays up while a product is failing. `/ready` answers 503
until every enabled product is running and its own in-process check reports an
authorized, usable serving view; the host calls that check directly rather
than probing the product's private routes over loopback HTTP. The response
lists each product's phase and readiness. A product that stops with an
ordinary error withdraws its wallet namespaces and stays not ready while the
other products continue; a confirmed publisher incompatibility, or a panicked
product task, stops the whole host. The products' private health routes
(Enhance `GET /internal/health`, Status `GET /control/health`, Transparent
`GET /v1/ready`) are unchanged for deployment checks.
