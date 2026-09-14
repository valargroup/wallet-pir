# Enhance PIR deployment

Scope: Enhance PIR. For transparent script-history recovery, use the [current transparent PIR index](transparent-pir/README.md); the retained outpoint-keyed transparent-spend protocol is a different component.

The coordinator publishes the Enhance table for one best-chain-tip generation,
on its replicated workers. A tip reorg truncates the journal to the common
ancestor before a replacement generation is published.

The transparent-spend tables are not served and no worker is provisioned for
them; see `docs/architecture.md`.

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
expects `ENHANCE_COORDINATOR_HOST`, `ENHANCE_DEPLOY_USER`,
`ENHANCE_PUBLIC_URL`, and `ENHANCE_WORKERS_JSON`; preflight/deploy modes also
require the artifact, SSH, release, and service-file variables validated by the
script.

```sh
ENHANCE_COORDINATOR_HOST=coordinator.example.net \
ENHANCE_DEPLOY_USER=deploy \
ENHANCE_PUBLIC_URL=https://enhance.example.net \
ENHANCE_WORKERS_JSON='[{"name":"shard-group-01","replicas":[{"name":"worker-01a","ssh_host":"worker-01a.example.net","service_url":"http://10.0.0.2:8091"},{"name":"worker-01b","ssh_host":"worker-01b.example.net","service_url":"http://10.0.0.3:8091"}]}]' \
ops/scripts/deploy-enhance-pir.sh validate
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
before production adoption. See [automatic expansion](enhance-autoscaling.md).

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
