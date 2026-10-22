# Query server host

`pir-query-server` is one process that serves any compiled subset of Enhance,
Status, Transparent history, and transaction details. The host crate is
`src/pir-query-server`. Its management server
reserves listeners and owns process shutdown. Product libraries continue to own
evaluation, admission, and readiness. Status HTTP routes, synchronization,
and role validation live in `status-query-server::service` and its product
configuration; the host delegates to that crate, as it does for Enhance and
Transparent.

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
filter operations reach the coordinator's loopback services; Transparent
queries, history filter shards and Receiver cross to `transparent-pir` and
`receiver-pir` over verified HTTPS, naming each origin as Host and SNI. It
serves no legacy alias and no operator path. The product hosts keep serving both
forms for released wallets. Receiver's edge trusts only the coordinator's
address to forward `X-Forwarded-For`, so its per-client admission still keys on
the wallet. Whether the host is live is recorded in the product status pages,
not here.

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

A process includes at most one Enhance role. `enhance-pir-server` does not start a
worker, query ingress, or packing router. Those roles are only
`pir-query-server --config FILE`. `enhance-pir-server` keeps `rebuild-journal`, `prepare-packing-artifacts`, `restore-legacy-placement`, and
`build-info`. `build-info` reports native reinspiring and the source revision;
it has no CUDA field.

`check-backend` validates the selected evaluator, and for CUDA the driver,
without opening worker state. `repair-rows` repairs journal-referenced rows
from a peer row archive while the worker is stopped. `status-pir` remains its
own command. Transparent history and transaction details have no separate
server executables; they run only as `pir-query-server --config FILE`. The
Transparent administration tools `shard-assign`, `shard-prune`,
`shard-control`, and `txid-control` are built from `transparent-tools`.

## Configuration

The [example](../tools/deploy/query-server/query-server.example.json) is a typed
configuration, not a production inventory. Sections are `enhance`, `status`,
`transparent_history`, and `transaction_details`. Each has `enabled` and
`settings`. An omitted or disabled section starts nothing.

Enhance settings name one role: `worker`, `query-ingress`, or `packing-router`.
A worker needs `listen`, an absolute `data_dir`, and optional `sealed_shards`
(default 6) and `matvec` (`matvec_backend` `cpu` or `cuda`, and `cuda_device`
only for CUDA). Query ingress needs `data_dir`, `listen`, `control_listen`,
and optional `requests` (default 16) and `uploads` (default 256). A packing
router needs `data_dir`, `artifact_origin`, `listen`, `control_listen`, and
optional `max_objects` (default 6) and `requests` (default 4). Status settings
name `worker` or `router`. Both require `publisher` (an HTTPS origin),
`network_hex`, an exclusively owned `state_dir`, and loopback listeners.
`publisher_ca` optionally adds an installed PEM trust root. The router also
requires its loopback `worker_origin`; workers use `cuda` only when compiled for
that backend. Those roles stay separate processes. The publisher prepares their
material; neither query role computes hints or packing matrices. See
[Status distribution](status/status_distributed.md). History and
transaction-detail settings are described under [Transparent](#transparent).

Listeners need distinct fixed loopback or private addresses. State directories
are absolute and must not overlap. `--check` validates the file and does not
bind sockets or open stores.

## Transparent

A `transparent_history` or `transaction_details` section takes the former server
settings: each command-line long flag becomes a field with `-` replaced by `_`.
Boolean flags are `true` when set and omitted otherwise, numbers are integers,
and repeated flags (`runtime_cache_prune_set`, `collect_root`) are arrays.
Transaction details require `role` (`archive-owner`, `recent-replica`, or
`combined`). Every object rejects unknown fields. A deployment renders one
Transparent section per file, so history and detail stay separate processes.

```json
{
  "management_listen": "127.0.0.1:18190",
  "transparent_history": {
    "enabled": true,
    "settings": {
      "listen": "127.0.0.1:8093",
      "shard_dir": "/srv/transparent-pir/shards",
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
      "publication_dir": "/srv/transparent-pir/txid",
      "state_dir": "/srv/transparent-pir/txid-state"
    }
  }
}
```

`pir-query-server --config FILE --verify-only` replaces the former servers'
`--verify-only`. The file must enable exactly one Transparent section and no
other product. It binds no listener, prints the same JSON report and keeps the
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
until every enabled product returns success from its existing probe: Enhance
`GET /internal/health`, Status `GET /control/health`, and Transparent
`GET /v1/ready`.
