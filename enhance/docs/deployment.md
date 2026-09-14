# Enhance PIR deployment

Scope: Enhance PIR. For transparent script-history recovery, use the [current transparent PIR index](../../transparent/docs/README.md); the retained outpoint-keyed transparent-spend protocol is a different component.

The coordinator publishes the Enhance table for one best-chain-tip generation,
on its replicated workers. A tip reorg truncates the journal to the common
ancestor before a replacement generation is published.

The transparent-spend tables are not served and no worker is provisioned for
them; see `enhance/docs/architecture.md`.

The schema-7 release uses protocol `ironwood-enhance-pir-v2` at the existing
`/v1/enhance/*` paths. This is a coordinated cutover: schema-6 clients reject it.
Source support is not deployment evidence; record the activated SHA and public
initialization metadata in the release evidence.

The manual workflow accepts a full commit SHA that must be the current `main`
revision and must already have a successful CI run. It builds and checksums:

- `enhance-pir-server`
- `enhance-pir-worker`
- `enhance-pir-cli`
- `pir-apm`
- the matching systemd and Caddy configuration

Operational configuration uses the `ENHANCE_` prefix. The deployment helper
expects `WALLET_PIR_COORDINATOR_HOST`, `WALLET_PIR_DEPLOY_USER`,
`ENHANCE_PUBLIC_URL`, and `ENHANCE_WORKERS_JSON`; preflight/deploy modes also
require the artifact, SSH, release, and service-file variables validated by the
script.

```sh
WALLET_PIR_COORDINATOR_HOST=coordinator.example.net \
WALLET_PIR_DEPLOY_USER=deploy \
ENHANCE_PUBLIC_URL=https://enhance.example.net \
ENHANCE_WORKERS_JSON='[{"name":"shard-group-01","replicas":[{"name":"worker-01a","ssh_host":"worker-01a.example.net","service_url":"http://10.0.0.2:8091"},{"name":"worker-01b","ssh_host":"worker-01b.example.net","service_url":"http://10.0.0.3:8091"}]}]' \
enhance/ops/scripts/deploy-enhance-pir.sh validate
```

Each ordered shard group owns 16 shards and has exactly two active-active
replicas. Group order is append-only because it determines shard placement;
replicas inside an existing group may be replaced without moving shards. A
generation publishes once at least one replica in every used group is ready.
The first rollout from the legacy flat inventory is an intentional topology
format migration and requires `ENHANCE_ALLOW_TOPOLOGY_CHANGE=true`; later
replica replacements do not require that override.
Each c-4 worker uses MemoryHigh=6G, MemoryMax=7G and MemorySwapMax=2G.
The host has a 2-GiB swap file for transient peaks, not additional serving
capacity. These settings and the 16-shard range require full qualification
before production adoption. See [capacity expansion](#capacity-expansion-target).

Current runtime paths are `/etc/enhance-pir`, `/opt/enhance-pir`,
`/srv/zakura/enhance-data`, and `/srv/enhance-pir/artifacts`. Active
DigitalOcean resources use Enhance names. The attached Zakura data volume is
the sole exception: DigitalOcean cannot rename it in place, so Terraform keeps
its historical provider name and protects it from replacement.

The deploy verifies `GET /v1/health`, retrieves and validates the atomic
`GET /v1/enhance/init` response, and completes a dummy query through the
public origin before declaring the rollout successful. It reads the former
generation endpoint only when capturing rollback metadata from a legacy
deployment.

## Schema-7 preparation and cutover

Build the tested current-main revision with `artifact_only=true`. Then dispatch
`Deploy Enhance PIR` with `prepare_only=true` and `artifact_only=false`. Preparation
replays canonical blocks from Ironwood activation into `/srv/zakura/enhance-data-v7`,
resumes an interrupted journal, verifies its anchor and records the prepared release.
It does not contact workers or stop production. The preparation job permits up to
six hours; it is separate from the bounded deployment job.

After qualification and client conformance pass, dispatch the same release with
both switches false. Deployment requires its exact preparation receipt, uses
`/srv/enhance-pir/artifacts-v7` on workers, and verifies schema, row width, health,
and private queries. Preserve the old data directories. Rollback restores the old
binaries and systemd units, whose paths select the untouched schema-6 data.
Re-run preparation before retrying a changed schema-transition release. Later
compatible schema-7 releases resume the active journal without preparation. Never point schema 7 at the
old journal or manually change its manifest version: the metadata must be derived
from canonical transactions.

Direct preparation, on the archive host with the matching binary:

```sh
enhance-pir-server --prepare-only --zakura-cookie /root/.cache/zakura/.cookie \
  --data-dir /srv/zakura/enhance-data-v7
```

Fee and expiry are trusted indexer metadata. A pure Ironwood fee is derived from
its public value balance; mixed transactions encode an absent fee and keep the
wallet's ordinary enhancement route. Expiry zero is preserved as a known value.

## Capacity expansion target

The following is the implemented target. See [dated status](status.md) for qualification and rollout state.

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
   Save and inspect a baseline plan using `enhance_worker_size=s-4vcpu-8gb` and
   `enhance_group_count=1`; it must have no resource changes. Preserve all
   transparent-fleet inputs, DNS and volume identities.
2. Create the private Spaces backend described in the infrastructure README,
   copy `backend.tf.example` to `backend.tf`, and migrate state using runtime
   Infisical credentials. Use Terraform >=1.10. The tested Spaces endpoint does not enforce native
   conditional S3 lock writes, so `use_lockfile=false` is intentional. Every
   writer must hold the coordinator's `/run/lock/wallet-pir-production.lock`;
   verify a second writer cannot acquire that host lock and verify another
   no-change plan.
   All production entrypoints must use this backend; retire stale local state
   from operational use, retaining private backups.
3. Hold `/run/lock/wallet-pir-production.lock` on the coordinator for the entire
   migration. Set the nonsecret infrastructure inputs to
   `enhance_legacy_worker_count=2`, `enhance_group_count=1`, `enhance_worker_size=c-4`.
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

   This verifies exact answers with two concurrent query clients, retained
   sessions and changing frontier data, and records publication duration. It does **not** by itself certify memory, failover, online
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
operator configuration. Install Terraform, Python 3, OpenSSH and systemd with encrypted credential
support on the coordinator before starting observation mode.

Configure `/etc/enhance-pir/autoscale.json` from the supplied example. The
Terraform directory must contain the initialized locked remote backend and
complete current production inputs; tfvars must not contain credentials.
Install the six required production secrets as a JSON object encrypted with
`systemd-creds encrypt --name=runtime - /etc/credstore.encrypted/enhance-pir-runtime`.
Supply the JSON over stdin directly from the authorized secret source, never a
checked-in file or terminal output. Keep the encrypted file root-owned mode
0600 in a root-only directory. The host encryption key stays on the coordinator;
recover or rotate by fetching the authoritative secrets and re-encrypting them.
`LoadCredentialEncrypted` exposes the decrypted credential only to the service,
and `wallet-pir-runtime.py` injects an exact allowlist of six environment keys.
This arrangement replaces the previously attempted custom Infisical role.
Configure
`runtime_env_aliases` to map existing project key names to Terraform/Spaces
environment variables; these aliases contain names only. The notifier consumes
`PIR_APM_SLACK_WEBHOOK_URL`, using the existing PIR destination. Do not copy
secrets between projects or print secret values.

Include `WALLET_PIR_DEPLOY_SSH_KEY` in the encrypted runtime credential; the controller writes it to a
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
flock -n /run/lock/wallet-pir-production.lock enhance/ops/scripts/deploy-enhance-pir.sh deploy
python3 /opt/enhance-pir/ops/enhance-autoscale.py --config /etc/enhance-pir/autoscale.json status
python3 /opt/enhance-pir/ops/enhance-autoscale.py --config /etc/enhance-pir/autoscale.json plan
```

Run credential-dependent manual Terraform operations through
`/opt/enhance-pir/ops/wallet-pir-terraform.sh`; it holds the common lock and creates
a transient systemd service with the same encrypted credential.
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

## Supervised hardware evidence

`enhance/ops/scripts/enhance-qualification.py` runs the six-hour/300-publication fixture
on an isolated pair, samples each worker's cgroup memory and swap I/O every five
seconds, and stops/restarts one replica for a controlled 90-second outage. Run
it on the coordinator with the checksummed candidate artifact directory, the
two new private addresses, a temporary deployment key and pinned known-hosts.
Its `summary.json` evaluates memory, exact answers and failover; it is not the
combined deployment receipt. Real online-expansion and production publication
lag still require separate evidence. The raw hardware samples and publication
progress remain in the run directory even if the fixture fails.


For a combined range-boundary rehearsal, pass two additional isolated worker
origins as repeated `--append-worker-url http://HOST:PORT` arguments to either
the fixture or supervisor. After its retention/load phase, the fixture fills
shard 15, requests an append-only second group, and publishes the first position
in shard 16 while continuously verifying an earlier session. It then verifies
old and new sessions across the boundary and records `online_append` evidence.
The two additional processes may run on an isolated test host; record that
placement explicitly. Such a run tests the protocol transition, not the second
pair's full-capacity hardware qualification or the cloud provisioning controller.
The supervisor samples only the original c-4 pair. Preserve process uptime and
controller lifecycle evidence separately before attesting the combined receipt.


## Explicit operator acceptance

An operator may explicitly accept a deployed release without completing the
qualification and initial observation periods. Record that decision in the
configured qualification receipt using `acceptance: "operator"`, the exact
running `revision`, `worker_size: "c-4"`, `shards_per_group: 16`, both
`waive_qualification: true` and `waive_initial_observation: true`, and nonempty
`authorized_by`, `authorized_at`, and `reason` fields. Keep the file root-only.
This receipt must not claim that waived tests passed. It applies only to the
named release; a subsequent binary revision requires its own acceptance.

Operator acceptance leaves release checksum verification, removal of legacy
workers, live chain/replica checks, three capacity samples, cooldown, the fleet
ceiling, additive-plan validation, and new-worker bootstrap checks in force.
