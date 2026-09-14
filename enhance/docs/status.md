# Status

## September 14, 2026: public origin

A read-only observation of `https://enhance-pir.valargroup.dev` found schema 7,
protocol `ironwood-enhance-pir-v2`, five shards in `shard-group-01`, and two
configured workers. Public health reported serving with eight retained
generations. The initialization response advertised 737-byte records, nine
records per row, 8,192 rows per shard and 65,536 logical rows.

The [bounded public baseline](../evidence/public-baseline-2026-09-14/README.md)
contains captured health/init responses and query measurements. This establishes
that the public origin serves schema 7. It does not establish worker SKU, binary
revision, release receipts, wallet adoption, autoscaler state or full hardware
qualification; those were not inspected through private host access.

## September 13, 2026: retained operator report

The earlier record said the versioned Spaces state migration and creation of a
new c-4 pair were complete, with a no-change Terraform plan afterward. It said
the old pair still served production, the autoscaler was disabled, and the c-4
expansion target was not yet qualified or activated. These are historical
observations, not a claim that the fleet remains in that state today.

That report also recorded restored coordinator SSH access after removal of its
DigitalOcean firewall and disabling UFW at boot, and use of systemd encrypted
runtime credentials with Infisical as the source for rotation and recovery.

The [isolated preflight](../evidence/preflight-2026-09-13/README.md) reported exact
answers at a named revision. Its raw bundle was not committed. It does not satisfy
full hardware, failover, expansion or production-observation acceptance.

## Unresolved deployment evidence

The public schema observation supersedes the older uncertainty about whether
schema 7 is served at this origin. It does not resolve older conflicting claims
about Vizor migration or whether any wallet depended on the former API. Obtain
a dated client conformance/adoption record before relying on either claim.

Reconcile installed release, current inventory and qualification or operator
acceptance receipts before a future migration. Do not repeat historical state
moves based solely on the September 13 note. The [remaining work](remaining-work.md)
tracks evidence still needed; [deployment](deployment.md) owns release procedures.
