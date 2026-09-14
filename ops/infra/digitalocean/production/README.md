# Ironwood PIR production infrastructure

The c-4/16-shard target below is not yet production-qualified or deployed.
Follow the [migration and qualification gates](../../../../docs/enhance-autoscaling.md); do not apply this root to the legacy pair without the state moves described there.

This Terraform root manages the Enhance PIR production fleet in the
`enhance-pir` DigitalOcean project:

- one `m-8vcpu-64gb-intel` coordinator with a 1 TiB XFS volume at
  `/srv/zakura`, running Zakura (archive), ingest, the coordinator, Caddy, and
  the `pir-apm` sidecar;
- one logical shard group with two `c-4` private PIR replicas; and
- the unproxied Cloudflare DNS record `enhance-pir.valargroup.dev`; and
- a dedicated VPC and firewalls. Only the coordinator may reach worker port
  8091. Worker SSH is restricted to `allowed_ssh_cidrs`; the coordinator has no firewall.

That describes the Enhance shape only. Transparent shard resources also live in
this Terraform root; use the [transparent deployment target](../../../../docs/transparent-pir/deployment.md)
and [verified-status record](../../../../docs/transparent-pir/status.md) before changing them.
The proposed transparent fleet is not established by this README. Worker pools per table, a second
coordinator, a separate ingest host, and artifact publishing to Spaces are
documented as a growth path in the historical `docs/archive/pir_deployment_architecture.md` §6 and
are deliberately not built.

## State

State is not committed. `.gitignore` excludes `terraform.tfstate*`, `*.tfplan`,
and populated `*.tfvars`. The production backend is the private, versioned Spaces bucket
`enhance-pir-terraform`, key `production/terraform.tfstate`, in `ams3`.
Use the bucket-scoped `ENHANCE_TF_STATE_ACCESS_KEY` and
`ENHANCE_TF_STATE_SECRET_KEY` from Infisical production. When this root was
renamed from `memo-poc`, the untracked state stayed in the old directory of
whichever checkout ran the last apply; move `terraform.tfstate`,
`terraform.tfstate.backup`, and `.terraform/` into `production/` before the
next plan.

The committed `backend.tf` enables versioned remote state. Spaces did not
enforce conditional lock writes in the rollout test; native S3 locking is
disabled. All writes must execute under the coordinator's
`/run/lock/enhance-production.lock`, shared with deployment and autoscaling.
Use `ops/scripts/enhance-terraform.sh` on the coordinator for manual operations;
do not run a direct apply from another host.
For an existing local checkout, migrate once after backing up and reconciling
its state; fresh checkouts use plain `terraform init`:

```bash
infisical run --projectId=40862c6d-a089-4355-b405-0477be0ee3b1 --env=prod --path=/ -- \
  sh -c 'export AWS_ACCESS_KEY_ID="$ENHANCE_TF_STATE_ACCESS_KEY" AWS_SECRET_ACCESS_KEY="$ENHANCE_TF_STATE_SECRET_KEY"; terraform init -migrate-state'
```

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
The tag resources are the only expected replacements because DigitalOcean tag
names are immutable. The Zakura volume keeps its historical provider name and
has `prevent_destroy` because DigitalOcean cannot rename it in place.

## Capacity

Each ordered shard group owns 16 shards and has two active-active replicas.
The coordinator sends a query to one ready replica per group and retries its
peer on failure. Publication requires one ready replica in every used group;
the second copy provides redundancy without contributing a duplicate PIR
partial.

Keep group order append-only. Add the next replica pair before the database
crosses a 16-shard boundary; adding or replacing a replica within an existing
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
