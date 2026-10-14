# Wallet PIR production infrastructure

The c-4 three-shard target below is not yet production-deployed.
Follow the [migration and qualification gates](../../../../enhance/docs/qualification.md); do not apply this root to the legacy pair without the state moves described there.

This Terraform root manages the shared Wallet PIR production infrastructure in
the `wallet-pir` DigitalOcean project:

- one `m-8vcpu-64gb-intel` coordinator with a 1 TiB XFS volume at
  `/srv/zakura`, running Zakura (archive), ingest, the coordinator, Caddy, and
  the `pir-apm` sidecar;
- one logical shard group with two `c-4` private PIR replicas; and
- the unproxied Cloudflare DNS record `enhance-pir.valargroup.dev`; and
- a dedicated VPC and firewalls. Only the coordinator may reach worker port
  8091. Worker SSH is restricted to `allowed_ssh_cidrs`; the coordinator has no firewall.

Service resources live in the sibling `enhance/`, `status/`, `transparent/`
and `transparent-tx-detail/` modules. This fleet root owns the backend,
providers, coordinator, VPC, node storage, monitoring, Receiver and all existing
project-membership resource sets. `services.tf` passes shared references and
`moves.tf` extracts the resources without changing their instance keys;
`shared.tf` retains the earlier rename and archive-key move chains.

`receiver.tf` holds the opt-in receiver
directory Droplet, firewall and DNS record; import the existing Droplet and record as the
[receiver deployment](../../../../receiver/ops/digitalocean/README.md#infrastructure)
describes before enabling it. Use the [transparent deployment target](../../../../transparent/docs/deployment.md)
and [verified-status record](../../../../transparent/docs/status.md) before changing them.
The proposed transparent fleet is not established by this README. Worker pools per table, a second
coordinator, a separate ingest host, and artifact publishing to Spaces are
documented as a growth path in the historical `docs/archive/pir_deployment_architecture.md` §6 and
are deliberately not built.

## State

State is not committed. `.gitignore` excludes `terraform.tfstate*`, `*.tfplan`,
and populated `*.tfvars`. The production backend is the private, versioned Spaces bucket
`enhance-pir-terraform`, key `production/terraform.tfstate`, in `ams3`. The
bucket name is a documented historical exception and is not migrated for branding.
Use the bucket-scoped `WALLET_PIR_TF_STATE_ACCESS_KEY` and
`WALLET_PIR_TF_STATE_SECRET_KEY` from Infisical production. The repository path changed from `production/` to `fleet/`; the backend key
and lineage did not change. Saved local plans and state backups from this
checkout were retained privately outside the source tree. Never execute those
saved plans or copy a historical local state over the remote state.

The committed `backend.tf` enables versioned remote state. Spaces did not
enforce conditional lock writes in the rollout test; native S3 locking is
disabled. All writes must execute under the coordinator's
`/run/lock/wallet-pir-production.lock`, shared with deployment and autoscaling.
Use `ops/scripts/wallet-pir-terraform.sh` on the coordinator for manual operations;
do not run a direct apply from another host.
The service extraction needs no backend migration. Initialize a fresh staging
copy without changing the backend configuration. Follow the
[activation procedure](activation.md) before writing production state.

## Plan and apply

Supply the API token at runtime. Never commit it or a populated tfvars file:

```bash
infisical run --projectId=40862c6d-a089-4355-b405-0477be0ee3b1 --env=prod --path=/ -- \
  sh -c 'export TF_VAR_digitalocean_token="$DO_TOKEN_NEW_ORG" TF_VAR_cloudflare_api_token="$CF_API_TOKEN"; terraform init && terraform plan'
```

The Cloudflare record remains DNS-only so Caddy can obtain and renew the
origin certificate directly.

Always save and inspect the production plan before applying it. Renaming the
fleet must not replace the coordinator, workers, VPC, volume, or attachment.
This repository reorganization must contain only declared state-address moves;
no tag or other provider resource may change. The Zakura volume keeps its historical provider name and
has `prevent_destroy` because DigitalOcean cannot rename it in place.

Transparent archive owners are listed by name in `transparent_archive_names`
(for example `["transparent-pir-archive-03"]`), not counted, so an owner can be
added or removed without renumbering the others. The `moved` blocks in
`shared.tf` carry the two owners created under `count` to their names; the first
plan after that change must show only moves. Add a name before the inventory's
`repartition` enrolls the host, and remove one only after the inventory has
retired it and the operator has stopped it; the saved plan must then show
exactly one destroy per removed name and one in-place project change. The
[deployment target](../../../../transparent/docs/deployment.md) has the full
procedure.

`transparent_txid_display_port_enabled` (default false) opens port 8095 on the
transparent worker firewall. The public router reaches the one-query txid site
(`/v2/txid/`) through it, because the site's own firewall admits 8095 only from
the coordinator. Production sets it in `txid-display.auto.tfvars` in the
coordinator's root. Keep that file when the root is refreshed, or the next plan
cuts the router off from `/v2/txid/`. No deploy phase writes it any more.

`transparent_txid_display_site_enabled` (default false) creates the one-query
txid display site: its host, firewall and DNS record
(the `transparent-tx-detail` module). Set it in an `*.auto.tfvars` file in the
coordinator's root, like the display archive count.

## Capacity

Each ordered shard group owns three shards and has two active-active replicas.
The coordinator sends a query to one ready replica per group and retries its
peer on failure. Publication requires one ready replica in every used group;
the second copy provides redundancy without contributing a duplicate PIR
partial.

Keep group order append-only. Add the next replica pair before the database
crosses a three-shard boundary; adding or replacing a replica within an existing
group does not move shards. Worker memory includes retained frontier generations and rebuild overlap.
The target c-4 service has a 6-GiB soft limit, 7-GiB hard limit and 2-GiB swap
allowance. The full working set must fit RAM in qualification. Each pair costs
$168/month in worker compute at the verified list price.


The public origin and dashboard are served at
`https://enhance-pir.valargroup.dev`.

The Enhance coordinator intentionally has no DigitalOcean cloud firewall, so SSH
access does not depend on the operator or VPN source IP. Private coordinator
services must bind to loopback. Worker and auxiliary-host firewalls remain managed
here; `allowed_ssh_cidrs` applies to those hosts.
