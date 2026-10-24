# Enhance v4 replica infrastructure

This Terraform root owns the v4 worker fleet: zero to four pairs of dedicated
four-vCPU, 8 GiB (`c-4`) workers, a dedicated tag, and additive membership
in the existing Valargroup wallet-pir project. Stable resource addresses and
Droplet IDs let the infrastructure controller reconcile interrupted operations.
Deletion/replacement is prohibited by lifecycle guards. Increasing `group_count`
adds complete pairs. This root owns no coordinator, DNS, or existing service.

Use an independent remote state key for this root. Never initialize it against
the legacy production state. Backend credentials and the DigitalOcean token are
runtime environment inputs; do not put them in tfvars, backend files, or plans
that leave the protected operation directory. Use the verified Valargroup account
and include Roman's registered `~/.ssh/id_ed25519.pub` in `ssh_key_ids`.

Required nonsecret inputs are the existing wallet-pir project and VPC UUIDs and
registered SSH key IDs. The region defaults to `ams3`. This root creates no
cloud firewall. Bind private worker RPC listeners to the VPC interface.
Cloud-init prepares the host and bounded swap, but does not start
a PIR service. Bootstrap and hardware qualification must complete before inventory
registration. No private SSH key is copied onto workers.

Before applying an expansion, review a saved plan: it may add only the next pair
and its project membership, plus initial shared resources for bootstrap, and
must not update, replace or remove anything. Terraform plans may contain
credentials and must remain private. The isolated expansion journal,
provisioning, pair-bootstrap and fixture-registration drivers that once
automated this were retired; they remain at revision
`f26b9476fbcd37aca4b4882b9264e90c1896be16` under `tools/enhance/scripts/`.
Production registration still requires hardware qualification.

Local provider/schema checks use no cloud credentials:

```sh
terraform init -backend=false
terraform validate
terraform test
```

An initialized copy with a real remote backend and runtime credentials is needed
for live operation. Hardware qualification must measure host/cgroup limits and
swap behavior; the machine profile and cloud-init settings do not constitute a
passing receipt.
## Per-host monitoring

Set `apm_sidecar_boot = { url = "https://<public-artifact>", sha256 = "<digest>" }`
to install the pinned sidecar through the shared first-boot installer on new
workers. Existing hosts ignore user-data changes. The
`monitoring_enrollment_required` output lists hosts still requiring the normal
deploy-managed certificate installation and explicit observer enrollment.
Pass the worker outputs to `wallet-pir-monitoring.py enroll --write`, then
install credentials and deploy the observer role. Boot installation alone does
not establish monitoring coverage; no private monitoring keys enter Terraform.
