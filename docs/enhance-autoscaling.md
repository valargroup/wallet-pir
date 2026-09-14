# Enhance c-4 capacity expansion

## Status and scope

Implemented target, **not yet hardware-qualified or deployed**. Coordinator SSH
access was restored on 2026-09-13 by removing its DigitalOcean firewall and
disabling UFW at boot, as requested by the operator. The Terraform definition
no longer creates a coordinator firewall. No autoscaling state migration,
Droplet creation, Slack message or fleet activation has been performed. The
six-hour c-4 qualification and 24-hour initial observation remain mandatory gates.

Each ordered group owns 16 consecutive 8,192-row shards: 1,179,648 Ironwood
positions, two identical replicas, $168/month worker compute at the verified
c-4 list price. The automatic ceiling is four groups/eight workers ($672/month).
Every query still evaluates every populated group; this is position-capacity
expansion, not a throughput autoscaler. There is no recent/archive split.

The worker template uses `MemoryHigh=6G`, `MemoryMax=7G`, `MemorySwapMax=2G`,
two evaluation slots and one preparation slot. Cloud-init provisions a 2-GiB
swap file with swappiness 10. Serving residency must fit RAM. Coordinator
retention remains eight generations; workers additionally pin the unpublished
candidate so failed publication cannot evict a still-published generation.

## Initial migration (operator gate)

1. Reconcile the latest authoritative local/runner Terraform state with the
   live fleet. Back it up privately; do not assume this checkout is current.
   Save and inspect a baseline plan using `worker_size=s-4vcpu-8gb` and
   `enhance_group_count=1`; it must have no resource changes. Preserve all
   transparent-fleet inputs, DNS and volume identities.
2. Create the private Spaces backend described in the infrastructure README,
   copy `backend.tf.example` to `backend.tf`, and migrate state using runtime
   Infisical credentials. Use Terraform >=1.10 and `use_lockfile=true`. Verify
   a second writer cannot acquire the lock and verify another no-change plan.
   All production entrypoints must use this backend; retire stale local state
   from operational use, retaining private backups.
3. Hold `/run/lock/enhance-production.lock` on the coordinator for the entire
   migration. Set the nonsecret infrastructure inputs to
   `enhance_legacy_worker_count=2`, `enhance_group_count=1`, `worker_size=c-4`.
   Move the two original resource addresses before planning creation:

   ```sh
   terraform state mv 'digitalocean_droplet.worker[0]' 'digitalocean_droplet.enhance_legacy_worker[0]'
   terraform state mv 'digitalocean_droplet.worker[1]' 'digitalocean_droplet.enhance_legacy_worker[1]'
   ```

   This is an initial migration, not the autoscaler's allowlisted apply. Its
   saved plan must create two c-4 workers, retain both old Droplet IDs, and
   only add project membership. Reject any legacy resize/replacement, volume
   or transparent-fleet change. Retain the old pair for rollback. DO names
   temporarily overlap; identify the pairs by their recorded Droplet IDs and
   private addresses, not names alone.
4. Provision through the Valargroup project with Roman's registered public key
   and the deployment public key. No private GitHub key is needed on workers.
   Verify VPC/firewall membership, included disk, swap and cgroup configuration.
5. Build the tested main revision with the deployment workflow input
   `artifact_only=true`; this uploads the checksummed binaries without deploying.
   Qualify the new pair with that exact release intended for the coordinator.
   Do not aim the fixture command at a serving worker: it activates synthetic
   generations and changes that worker's assignment. Use a new local work dir:

   ```sh
   /path/to/downloaded-release/enhance-pir-qualify --isolated-workers \
     --worker-url http://NEW_PRIVATE_IP_A:8091 \
     --worker-url http://NEW_PRIVATE_IP_B:8091 \
     --shards 16 --seconds 21600 --min-publications 300 \
     --work-dir /srv/enhance-pir/qualification/run-001 \
     --output /srv/enhance-pir/qualification/run-001.json
   ```

   This verifies exact answers with two concurrent query clients and changing
   frontier data. It does **not** by itself certify memory, failover, online
   activation or publication lag. Collect per-worker `memory.current`,
   `memory.peak`, `memory.events`, `memory.swap.current`, swap I/O, RSS,
   systemd restart counts and CPU during the run. Include cold/warm starts,
   one-replica failure, old-generation queries and a range-boundary online
   append rehearsal. Never infer these gates from the small bootstrap fixture.
6. Require six hours/300 publications, exact answers, zero memory kills,
   steady memory below 6 GiB, peaks below 7 GiB, no sustained swap I/O,
   p99 <=5 seconds, and publication delay no more than 60 seconds above
   baseline. Online append must not restart existing processes or fail probes.
   If qualification fails, leave production unchanged; do not loosen limits.
7. Preserve the raw evidence and create a private qualification receipt with
   `passed`, `full_capacity`, `failover`, `online_append`, and `memory` all true,
   the exact 40-character `revision`, `worker_size: "c-4"`,
   `shards_per_group: 16`, `seconds >= 21600`, `publications >= 300`, and paths
   to the evidence. Install it at `/etc/enhance-pir/qualification.json`; the
   deployment helper verifies it before stopping any service. This receipt is operator attestation of the combined
   evidence, not output automatically granted by the fixture utility.
8. Clean only the new workers' disposable fixture data, then deploy using the
   new pair's inventory. One initial coordinator restart is allowed. Record
   old inventory and binaries for rollback. Normal CI builds and preserves
   checksummed worker, CLI and qualification artifacts on the coordinator.
9. Observe the c-4 pair for 24 hours with auto-provisioning disabled. On failure,
   restore the old pair and release; preserve the failed evidence. On acceptance,
   inspect a plan setting `enhance_legacy_worker_count=0` that removes only the
   recorded old pair and their project membership. Retire them before enabling
   automatic expansion. No recurring automatic scale-down is implemented.

## Controller installation and operation

The normal deployment installs the controller, systemd service/timer and a
disabled example config. It does not enable the timer or overwrite existing
operator configuration. Install Terraform, Python 3, OpenSSH and Infisical on
the coordinator before starting observation mode.

Configure `/etc/enhance-pir/autoscale.json` from the supplied example. The
Terraform directory must contain the initialized locked remote backend and
complete current production inputs; tfvars must not contain credentials.
Give the host a narrowly scoped Infisical machine identity in the correct
Valargroup production project. The service runs through Infisical. Configure
`runtime_env_aliases` to map existing project key names to Terraform/Spaces
environment variables; these aliases contain names only. The notifier consumes
`PIR_APM_SLACK_WEBHOOK_URL`, using the existing PIR destination. Do not copy
secrets between projects or print secret values.

Inject `ENHANCE_DEPLOY_SSH_KEY` through Infisical; the controller writes it to a
0600 temporary file for SSH and removes it on exit. The configured key path is
a fallback for an existing operator-managed key. Set a root-private known-hosts file. The
controller checks new Droplet IDs, c-4 size, region and VPC IP against the DO API,
then pins their first observed ed25519 key over the VPC. This is explicitly
trust on first use inside the verified VPC, not a provider-signed host-key
attestation. Later SSH connections require that pinned key; it is never replaced
automatically. Existing workers are never re-bootstrapped by the controller.

Start the timer with `enabled: false` for observation. APM warns at 75% and
raises critical capacity alerts at 90%. After the qualification/observation
gates, set `enabled: true`. Automatic provisioning additionally requires 24
hours of healthy observations, matching qualification/release and zero legacy
workers. Three five-minute observations at >=80% trigger one pair, with a
one-hour cooldown. Chain RPC and replica probes must show fresh serving state.
At four groups the controller notifies Slack and refuses further creation.

All normal deployment/infrastructure operations must hold the same host lock.
The GitHub deployment already does so. For manual operations on the coordinator:

```sh
flock -n /run/lock/enhance-production.lock ops/scripts/deploy-enhance-pir.sh deploy
python3 /opt/enhance-pir/ops/enhance-autoscale.py --config /etc/enhance-pir/autoscale.json status
python3 /opt/enhance-pir/ops/enhance-autoscale.py --config /etc/enhance-pir/autoscale.json plan
```

Run credential-dependent commands through the configured Infisical identity.
The `plan` command obtains the persisted desired count, holds the common lock,
and suppresses Terraform output to avoid leaking sensitive values. Do not run
a bare apply with the default group count after automatic expansion.

## Online activation and recovery

The private Unix socket accepts one newline-delimited JSON request per
connection (64-KiB maximum, five-second deadline):

```json
{"command":"status"}
```

An `append-group` request contains `request.operation_id`, `expected_revision`
and the complete `groups` inventory using the same `name`/`replicas`/`url`
shape as the worker config. Existing groups and replicas must match exactly;
one pair may be appended. Requests are idempotent. This is not a public API and
has no Caddy route. Committed topology survives restart and stale CI inventory
is rejected before stopping services.

The controller retains a private operation journal, saved plans, worker IDs,
host keys, fixture reports and a Slack outbox. Provisioning starts only after
Slack accepts its initial notification, which includes the $168/month cost
increment. Provisioned, activated, completed and failed transitions include
the operation ID. Webhooks lack idempotency: after a crash between delivery and
journal fsync, a repeated message can appear with the same operation/event ID.

Only the next two c-4 creations and their additive project associations pass the
Terraform allowlist. Replacement, removal, unrelated drift and transparent
changes are rejected. Failed/partial applies reconcile the same names and
state addresses; resources missing from Terraform state require operator import
before proceeding rather than risking duplicate creation. Provisioning has a
30-minute budget, qualification 15 minutes and activation 10 minutes. Three
failed attempts pause automation. No provisioned resource is deleted on failure.

Qualification runs only on new workers. They restart clean after the fixture,
then join as standby capacity. The coordinator publishes the new topology
without restarting and keeps old-generation routing. The first assigned shard
is prepared normally when its range is reached. Failed candidate publication
does not remove the old topology. Once committed, a group is never automatically
removed. The controller observes service for 30 minutes before completion.

After correcting a recorded failure, use the controller's `resume` command
under the runtime identity. It revalidates the release/receipt and resets retry
budgets; it does not delete resources or discard operation progress. If the
controller cannot reconcile resource IDs, repair/import the existing resources
under the shared lock first. Never clear the state journal to force another pair.

At actual range exhaustion, publication stops before allocating overflow shards.
Existing generations remain answerable, but health reports the failure and
coverage is stale. An appended group can recover publication of the backlog.

## Local verification

`make check` includes controller safety tests and release-mode Rust tests. The
online-topology integration test verifies standby append, retained encrypted
queries through repeatedly rejected candidates, and peer failover. Terraform
format/validate is also required. These checks do not replace the c-4 hardware
qualification or establish production deployment.
