# Enhance status

Recorded September 13, 2026; reorganized September 14 without querying production.
Code and workflow defaults establish implementation, not live deployment.

Implemented target, **not yet qualified for production or activated**. As of
2026-09-13 (America/Edmonton), the versioned Spaces state migration and creation
of the new c-4 pair are complete. A full Terraform plan after creation showed
no changes. The old pair continues serving production and remains available
for rollback. Coordinator SSH access was restored by removing its DigitalOcean
firewall and disabling UFW at boot, as requested by the operator; Terraform
no longer creates that firewall.

The [isolated preflight](../../evidence/enhance/preflight-2026-09-13/README.md)
reported exact answers at a named revision, but does not satisfy full hardware,
failover, expansion, or production-observation acceptance. Raw preflight data
was not committed alongside the original report.

The autoscaler was disabled in that record. Runtime credentials use systemd
encrypted credentials, with Infisical remaining the source for rotation/recovery.
See [remaining work](remaining-work.md) for rollout gates.

The schema-7 protocol is implemented. Earlier integration prose said rollout was
blocked on Vizor migration, while the old README said no deployed wallet depended
on the former API. Neither statement supplies a dated client adoption receipt.
Verify the intended client and deployed schema before the next cutover; this
cleanup makes no new schema-7 production or wallet-adoption claim.
