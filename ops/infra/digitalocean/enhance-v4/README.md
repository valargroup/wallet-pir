# Enhance v4 replica infrastructure

This Terraform root owns the v4 worker fleet: zero to four pairs of dedicated
four-vCPU, 8 GiB (`c-4`) workers, an isolated firewall/tag, and additive membership
in the existing Valargroup wallet-pir project. Stable resource addresses and
Droplet IDs let the infrastructure controller reconcile interrupted operations.
Deletion/replacement is prohibited by lifecycle guards. Increasing `group_count`
adds complete pairs. This root owns no coordinator, DNS, or existing service.

Use an independent remote state key for this root. Never initialize it against
the legacy production state. Backend credentials and the DigitalOcean token are
runtime environment inputs; do not put them in tfvars, backend files, or plans
that leave the protected operation directory. Use the verified Valargroup account
and include Roman's registered `~/.ssh/id_ed25519.pub` in `ssh_key_ids`.

Required nonsecret inputs are the existing wallet-pir project and VPC UUIDs, the
v4 coordinator's private IPv4, registered SSH key IDs, and operator SSH CIDRs.
The region defaults to `ams3`. Worker RPC on port 8291 is accessible only from the
coordinator address; SSH is accessible from that address and the explicit
operator CIDRs. Cloud-init prepares the host and bounded swap, but does not start
a PIR service. Bootstrap and hardware qualification must complete before inventory
registration. No private SSH key is copied onto workers.

Before applying an expansion, freeze the operation's policy and existing Droplet
IDs, persist one requested target count, save a Terraform plan, and validate its
JSON using `enhance/ops/scripts/v4-infra-plan.py` from the repository root. The
validator allows only the next pair and its project membership, plus initial
shared resources for bootstrap. It rejects updates, replacements, removals,
unexpected resources, changed identities, profile/ingress changes, and drift.
Terraform plans may contain credentials and must remain private.

The command validates only: it does not apply, bootstrap, qualify, or register.
The durable demand journal and provisioning adapter are implemented. Pair bootstrap orchestration is implemented; live validation, qualification,
registration, and automatic orphan recovery remain unfinished. Initialization, planning, and applying against a live account have
not been performed as part of the local mock tests.

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

## Durable expansion journal

`enhance/ops/scripts/v4-expansion-journal.py` consumes live coordinator health
(`--coordinator-url`) or a captured response (`--health`), the current worker
inventory (`--inventory`), and immutable operation policy (`--policy`). Supply a
dedicated `--state-dir` on persistent storage. Each invocation holds an exclusive
nonblocking file lock and writes mode-0600 state with file and directory fsync.
Only operation identity, phase, and target count are printed.

The journal retains the same pending request across coordinator reorgs and
consumer restarts. Policy and original inventory hashes cannot change mid-flight.
Recorded Droplet IDs cannot be replaced or assigned to another replica slot.
The adapter API requires established identities and a saved-plan digest before
persisting apply intent. An interrupted apply stays ambiguous until the adapter
reconciles remote Terraform state **and provider inventory**, records partial
resources, and supplies a digest of that evidence. A successful reconciliation
must cover the whole target fleet. Provider-side creations missing from Terraform
state must be imported or resolved before retry; a failed process alone cannot
establish that nothing was created.

This journal does not itself verify provider observations or run Terraform.
The provisioning adapter below performs those checks and resource execution.
A host-local bootstrap installer is included in the candidate bundle and the
pair driver below invokes it. Live validation, hardware qualification receipt
verification, and pair registration remain separate unfinished steps. `provisioned` describes resource
reconciliation only and never implies worker readiness or qualification.

The disposable local runner accepts `--journal-script enhance/ops/scripts/v4-expansion-journal.py` with `--expand-inventory` to check
that two separate journal processes consume the same real coordinator request.
Its later direct fixture registration does not exercise the unfinished live
adapter or bypass production qualification.

## Provisioning adapter

`enhance/ops/scripts/v4-provision.py` executes one pending expansion under the
journal lock. It requires an already initialized isolated S3 backend with verified locking,
the default workspace, and an established first v4 pair. Native S3 locking uses
`use_lockfile=true`. DigitalOcean Spaces requires the pinned-host mode below;
setting `use_lockfile=true` there is rejected because it does not establish
mutual exclusion.
Initial bootstrap is not performed by this consumer. Run only after selecting
and confirming the Valargroup project, VPC, worker profile, and Roman's registered
SSH public key. Supply credentials through runtime environment variables.

Its nonsecret policy contains the six Terraform inputs listed above plus:

- `account_uuid`: the verified Valargroup account UUID.
- `state_lineage`: the existing isolated Terraform state lineage.
- `module_sha256`: `module_digest(root)` from the provisioning script, covering
  the three Terraform files, cloud-init, and provider lock. Extra Terraform or
  variable input files are rejected.
- `backend`: the exact initialized `bucket`, `key`, and `region`, plus explicit
  endpoint settings when using a nondefault S3 endpoint. Keep backend credentials
  in the environment, never in this policy.

Observe coordinator demand with that same policy and current inventory before
running the adapter. Pass `--state-dir`, `--terraform-dir`, `--policy`, and
`--inventory`; `--token-env` selects a runtime variable and defaults to
`DIGITALOCEAN_ACCESS_TOKEN`. The script never initializes a backend, changes a
workspace, or edits coordinator inventory. It strips ambient Terraform argument
and variable overrides and passes the pinned inputs explicitly.

Before planning, the adapter verifies the account, project ownership, VPC region
and coordinator address, Terraform lineage, provider Droplet identities/profile,
and existing inventory origins. It checks the whole paginated provider fleet
for duplicate names and orphaned v4 resources. It saves and validates the next
pair's plan, persists its digest/apply intent, verifies the digest again, and
applies exactly that saved plan. A final no-change plan and matching provider
identities are required before recording `provisioned`.

A restart after all resources were created can reconcile without another apply.
A partial interrupted apply, provider orphan, or unfinished membership stays
fenced for explicit recovery. Automatic import/resumption of those ambiguous
cases is not implemented. Never clear the journal to retry: resolve provider
resources and Terraform state first, preserving recorded IDs. The adapter leaves
private plan JSON and reconciliation evidence beneath the journal directory;
these may contain sensitive infrastructure data and must not be published.

Tests use synthetic provider/state responses and Terraform-generated mock plan
fixtures. No live cloud execution has been validated yet. The API checks follow
DigitalOcean's [account](https://docs.digitalocean.com/reference/api/reference/account/),
[project](https://docs.digitalocean.com/reference/api/reference/projects/),
[VPC](https://docs.digitalocean.com/reference/api/reference/vpcs/), and
[Droplet](https://docs.digitalocean.com/reference/api/reference/droplets/) contracts.

The [candidate bootstrap instructions](../../../../enhance/docs/qualification.md)
describe the packaged host-local installer and its unqualified receipt. It runs through the explicit pair-bootstrap step below after provisioning.

## Pair bootstrap driver

`enhance/ops/scripts/v4-bootstrap-pair.py` advances the same journal from
`provisioned` through `bootstrapping` to `bootstrapped`. It rechecks the live
provider/account/project/VPC/state binding and selects only the new pair by
recorded Droplet IDs and private IPv4 origins. It never provisions additional
resources, changes coordinator inventory, or marks qualification as passed.

Supply the existing `--state-dir`, `--terraform-dir`, `--policy`, `--inventory`,
and runtime token environment selection, plus `--bundle`, `--bootstrap-policy`,
`--ssh-key`, and `--known-hosts`. The verified extracted bundle must be the clean
Linux candidate. Run from a host allowed by the workers' SSH firewall rules.
No private key is copied and agent forwarding is disabled.

The separate nonsecret bootstrap policy contains:

- `revision` and `manifest_sha256`, pinned from trusted release verification.
- `known_hosts_sha256`, covering a dedicated known-hosts file whose worker private
  address keys were verified out of band. The driver never performs trust-on-first-use
  or uses `ssh-keyscan` as proof of identity.
- `limits`, an object keyed by exactly the two new worker names, each containing
  the installer's four explicit integer byte limits.

This policy can be prepared after provisioning and host inspection. It is frozen
in the journal before remote bootstrap begins, independently of the unchanged
infrastructure policy. Changed artifact, host-key, limit, or target bindings are
rejected during recovery.

The driver waits for cloud-init, transfers the candidate into a dedicated
root-only bootstrap directory, checks its manifest digest and every transferred
file **before** executing the installer, and validates the resulting receipt.
Each validated replica receipt is persisted before contacting the next replica.
After an interruption, both hosts are rechecked through the installer's idempotent
path; a previously successful installation is not restarted or stopped. Receipts
must report distinct worker process identities. Successful pair bootstrap still
means `qualification: unqualified`.

Seven driver tests cover partial-pair recovery, frozen inputs, receipt binding,
duplicate process rejection, recorded-ID/private-origin selection, strict host-key
checking, remote argument quoting, and transfer verification order. SSH and cloud
operations in these tests are synthetic. Live fleet bootstrap remains unverified.

## Spaces state locking

The live wallet-pir backend documents failed conditional lock-write enforcement
on Spaces. The v4 root must use a separate state key (never
`production/terraform.tfstate`). For Spaces, set `use_lockfile=false` explicitly
and include this nonsecret object in the immutable provisioning policy:

```json
"state_lock": {"type": "pinned_host", "machine_id": "<designated host /etc/machine-id>"}
```

Both provisioning and pair bootstrap acquire
`/run/lock/enhance-pir-v4-terraform.lock` before reading state and hold it through
reconciliation and remote operations. They require root on the pinned host and
reject concurrent writers, replaced lock files, and a mismatched machine ID.
Terraform inherits the lock descriptor: a controller exit does not release the
lock while a Terraform child is still running. Do not unlink the lock file to
recover contention; inspect its holders and the provider/state first.

This is a single-host operational lock, not a distributed lock. Restrict state
write credentials to that host and require **all** manual operations, including
initial bootstrap, import and repair, to use the same `StateLock` context from
`v4-provision.py`, passing its `fd` through `subprocess.run(pass_fds=(lock.fd,))`.
A second workstation or direct Terraform command can otherwise bypass it.
Provisioning remains gated until that host and credential boundary are established.
Changing the writer host requires stopping existing writers and reconciling state
before changing the pinned policy; changing a machine ID is not failover fencing.

Validation: the provisioning suite exercises backend rejection, missing locks,
real kernel lock contention, descriptor inheritance after controller close,
wrong-host rejection and lock-path replacement. These checks do not prove the
live credential boundary; record that separately during deployment.
