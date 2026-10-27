# Receiver deployment

Receiver ingestion and publishing run inside the shared `pir-publisher`, alongside
Enhance, Status, Transparent history and transaction details. Wallet requests run
inside `pir-query-server`. Use the [shared publisher deployment](../../../docs/publisher.md)
and [fleet operator workflow](../../../docs/operations.md) for builds, preparation,
activation, status and rollback.

## One publishing owner

Enable Receiver in the shared publisher configuration and managed-product list.
Do not assign a standalone `receiver.server` role to the same fleet. Query hosts
use the existing Receiver HTTPS publisher origin and keep their own serving state;
they receive no node or payout-audit credentials.

For first adoption, the shared publisher transaction owns source shutdown,
reingestion, authority handover and edge routing.
Reingestion keeps the old owner serving until the new index is ready. Later
publisher updates retain Receiver and omit the one-time adoption section.

The old three-phase Receiver routing commands and separate Caddy cutover helper
have been removed. Retained operations packages contain the commands needed for
their historical transactions. Current shared-publisher transactions retain their
own recovery journal and rollback paths.

## Infrastructure

The coordinator's Caddy serves `receiver-pir.valargroup.dev`, and the public DNS
record in `receiver.tf` names the coordinator. The earlier `receiver-pir-poc-01`
Droplet, which kept a standalone HTTPS edge and rollback location after adoption,
was retired on 2026-10-27. The retained cloud-init and standalone role resources
document that host; they are not the shared deployment recipe.

## HTTPS routing

The [edge template](Caddyfile) accepts two existing HTTPS origins:

- `RECEIVER_PUBLISHER_UPSTREAM`: the shared publisher, for discovery and
  `/provider/receiver/v1/*`, including object downloads and authority checks.
- `RECEIVER_QUERY_UPSTREAM`: the shared query host, for wallet Receiver routes.

TLS verification remains enabled. Operator health and metrics stay private.
Changing the publisher upstream preserves the wallet upstream, request paths,
methods and bodies. Render and activate edge changes through the locked publisher
transaction rather than editing a live Caddyfile by hand.

## Optional payout audit credentials

Infisical is the source of truth. Provision the audit environment file on the
shared publisher at the path referenced by its unit, under `/etc/receiver-pir/`.
Systemd reads that file before starting the publisher; query hosts need no copy.
Never put credential values in requests, inventories, archives or command
arguments. The old installer targeted a standalone publishing role and has been
removed. Credential activation belongs to the normal locked publisher deployment.

## Validation and diagnostics

The shared offline `--check` validates product configuration before activation.
Use the existing `receiver-probe` for encrypted retrieval and independent chain
anchors, and publication identities to establish a fresh fetch. Cached inventory
alone does not prove synchronization. Scope operational retrieval checks with the
existing deployment request's `probe_products` when appropriate; keep readiness
for every enabled product.

Receiver-only examples and role renderers remain useful for isolated
qualification and shared configuration composition. They do not require a second
production publisher. The historical [deployment evidence](../../../evidence/receiver/deployment-2026-10-08/README.md)
describes the former topology.
