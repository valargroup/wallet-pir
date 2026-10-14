# Operating the Wallet PIR fleet

Start here for the shared operating model. Product runbooks own their publication,
authority, resource and encrypted-answer checks. Source and release preparation do
not establish what is running; inspect the fleet and retained deployment evidence.

## Architecture and ownership

| Product | What runs | Product authority |
|---|---|---|
| Enhance | Ingestion/publication coordinator, query ingress, packing router, evaluation workers | [Architecture](../enhance/docs/architecture.md), [deployment](../enhance/docs/deployment.md) |
| Status | Separate observation controller, packing router and evaluation worker; implementation shares the Enhance package | [Architecture](../enhance/docs/architecture_status.md), [distributed topology](../enhance/docs/status_distributed.md) |
| Transparent history | Event journal writer, publication controller, recent/archive workers and public routing | [Architecture](../transparent/docs/architecture.md), [deployment](../transparent/docs/deployment.md) |
| Transparent display | Its own transaction journal, publication controller and display workers | [Display contract](../transparent/docs/txid-display.md) |
| Receiver | One process indexes, publishes and serves receiver payments | [Overview](../receiver/README.md), [deployment](../receiver/ops/digitalocean/README.md) |

The [serving contract](serving-contract.md) records their shared process identity
and deliberate differences. A running process is not necessarily authorized,
warm, publishing or answering correctly. Preserve those distinctions in status
reports. The shared control-session supervisor remains source-only as recorded in
[control sessions](control-sessions.md); packaging it does not deploy it.

Shared operations live in `ops/lib/wallet_pir_ops`. Product adapters live with the
product, including `transparent/ops/lib/deploy_cli.py`. The shared
[role registry](../ops/deploy/services.toml) includes product-owned descriptors.
Each inventory role names its actual host, unit, private addresses and artifact
variant. The [example inventory](../enhance/ops/deploy/deploy-inventory.example.json)
uses documentation addresses and is not the live fleet.

## Operations releases

Full CI on main produces `wallet-pir-ops-<sha>` containing
`wallet-pir-ops.tar.gz`, alongside the binary artifacts. It contains the standard
library Python modules, wrappers, product descriptors, templates, remote helpers
and this guide. It contains no operator inventories, credentials, runtime state,
tests or retained evidence. It needs Python 3.11+, OpenSSH and the existing host
tools; it does not need a source checkout, Cargo or a Python dependency install.

Use the existing artifact resolver to select the successful full-CI artifact for
the exact source revision. Verify and unpack it into a new release directory:

```sh
python3 tools/ci/release.py resolve --sha <full-revision> --kind wallet-pir-ops
python3 ops/scripts/wallet-pir-bundle.py verify --sha <full-revision> --archive wallet-pir-ops.tar.gz
python3 ops/scripts/wallet-pir-bundle.py extract --sha <full-revision> --archive wallet-pir-ops.tar.gz --output /path/to/new-release
```

Verification refuses unsafe paths, links, duplicates, incomplete inventories,
wrong revisions and checksum mismatches before creating the destination. These
checks establish artifact integrity; the resolver's full-CI provenance and product
qualification remain separate requirements. The unpacked tool checks its resources
before operating. Dirty operations releases support inspection only. When a binary
archive names a source revision, it must match the operations release's revision.

For local development, assemble an inspection bundle without compiling Rust:

```sh
python3 ops/scripts/wallet-pir-bundle.py assemble --sha <checkout-revision> --archive /tmp/wallet-pir-ops.tar.gz
```

Use wrappers from the unpacked release's directory tree. Do not copy individual
Python files out of it. Retained installed standalone scripts have their recorded
source identities; adopting this package is a separately reviewed deployment.

## Fleet configuration and lifecycle

The inventory owns operator choices: hosts, units, connection settings and artifact
variants. Descriptors own role order, templates and readiness checks. Dynamic
membership, publication assignments and their persisted revisions remain owned by
the current controllers and Transparent inventory; the shared view does not write
them or introduce another state database.

```sh
python3 <release>/ops/scripts/wallet-pir-deploy.py --inventory /path/to/inventory.json fleet
python3 <release>/ops/scripts/wallet-pir-deploy.py --inventory /path/to/inventory.json --json status enhance
python3 <release>/ops/scripts/wallet-pir-deploy.py --inventory /path/to/inventory.json --json status transparent-history
```

`fleet` reads configured roles without contacting hosts. `status` reports observed
systemd state and executable identity plus the latest transaction, not encrypted
query correctness. Structured results name the service, operation, roles, hosts,
phases, changes and refusal reasons. The importable `deploy.lifecycle.Lifecycle`
interface supports plan, preflight, deploy, status and rollback over the existing
engine; the CLI consumes that interface. Mixed CPU/GPU releases emit one JSON
result per artifact group, each with its binary digest and separate transaction.

Enhance, Status and Receiver use the ordinary binary lifecycle:

```sh
python3 <release>/ops/scripts/wallet-pir-deploy.py --inventory /path/to/inventory.json capture-baseline enhance
python3 <release>/ops/scripts/wallet-pir-deploy.py --inventory /path/to/inventory.json --json plan enhance --archive enhance-pir-native.tar.gz --kind enhance-pir-native --sha <full-revision>
python3 <release>/ops/scripts/wallet-pir-deploy.py --inventory /path/to/inventory.json preflight enhance --archive enhance-pir-native.tar.gz --kind enhance-pir-native --sha <full-revision>
```

Review the complete captured unit and its drop-ins, the planned differences and
the binary identities. Supply the matching CUDA artifact for selected GPU roles;
missing variants are refused. Staging preflight honors baseline and configuration
refusals before uploading anything. Deployment holds the existing production lock,
records effects in the existing journal and retains the existing rollback rules.
After deployment, report status and the rollback command from that product's runbook.

Transparent history and display appear in the same role view and process status,
but their mutation drivers remain product recipes: `schema-*` and
`txid-display-*`. Ordinary binary deploys refuse those services because changing
a publication is more than replacing an executable. Their retained request,
plan-digest, authority, quiescence and recovery requirements continue to apply.

## Configure a publisher and its sandbox together

`role_config.RenderedRole` holds unit text and the configuration files that unit
consumes. Transparent's publisher adapter derives journal access from
`follow_journal`: a writer gets a writable journal; a follower gets a read-only
journal separate from writable publication/snapshot/state paths. It preserves the
template's other service settings and credential-file restrictions.

Render both for review without writing files or contacting a host:

```sh
python3 <release>/ops/scripts/wallet-pir-roles.py --config /path/to/controller.json --config-path /etc/transparent-pir/controller.json --state-dir /srv/transparent-publisher-state
```

Configuration must live outside the role's writable directories. Invalid paths,
ambiguous writer/follower modes and writable access through a follower journal's
ancestor are refused. The publisher installer consumes the same renderer.
Adopt its generated unit and configuration together through the product's locked,
journaled deployment; rendering alone changes no process.

Other binary roles retain their captured complete effective units during this
pass. The shared renderer is available when an owning product adopts complete
configuration rendering; it does not silently replace captured production units.

## Qualification and retirement

Qualification, candidate-build, census and benchmark tooling is advanced product
tooling, not the normal fleet lifecycle. The retained Transparent source-bound
recipes still require their exact reviewed source trees and inputs; an operations
bundle does not bypass those requirements or qualify a new runtime.

The [October 13 retirement](../transparent/evidence/v1-sunset-2026-10-13/README.md)
records v1 withdrawal and the v12 journal handover. This consolidation removes the
superseded standalone filter deploy workflow/installer and historical implicit
publication-path defaults. Worker verification helpers and source-bound recovery
tools remain because current tests and retained procedures consume them. The v1
410 route remains an explicit retirement response, not a second serving runtime.
No retired host, journal, rollback artifact or immutable evidence is deleted here.

Rust packages, serving protocols and persisted journals remain product-owned.
The next fleet-management frontend can consume the lifecycle and inventory APIs;
this pass adds no new provisioning service, paid infrastructure or serving lease.
