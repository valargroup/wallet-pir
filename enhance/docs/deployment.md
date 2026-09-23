# Deployment

## Schema-11 clean cutover

The supported runtime is `enhance-pir-v4`, now advertising schema 11 and
`ironwood-enhance-pir-v6`. Binary and module names are retained; state formats
are not. `enhance-pir-server`, `enhance-pir-worker`, and the legacy deployment
scripts refuse operation. The old journal migration utility only changes row
packing and cannot create suffix records.

1. Build and test the candidate, including the wallet interoperability harness
   in [integration](integration.md). Keep the current deployment running.
2. Create separate schema-11 directories for canonical ingestion, controller
   state, worker rows, hints and caches. Re-ingest canonical blocks using this
   build. Never copy an old journal or controller/worker state into these paths.
3. Start a separate worker pair and coordinator using the architecture-2 commands
   in [the runtime guide](architecture_2-implementation.md), with all data paths
   explicitly changed to the new schema-11 directories. Bootstrap tooling still
   uses v4 service/path names; use fresh hosts or a separately configured service
   layout rather than installing over the previous deployment.
4. Confirm schema 11 / protocol v5 from initialization and health; verify the
   canonical anchor and tree size, wallet note recovery, duplicate row batching,
   generation expiry and worker restart before routing clients to the candidate.
5. Measure actual capacity on deployment hardware. The raw journal is 11.4%
   smaller, but six PIR instances remain; do not lower memory reservations or
   increase worker assignments on the basis of record width alone.
6. Switch traffic together with compatible wallets. Keep the previous binary,
   configuration and original data directories intact. Rollback switches both
   clients and traffic to that deployment; it never opens new state with an old
   binary or old state with the new binary.

No deployment is performed by implementing or testing this change. New candidate
bundles remain unqualified until the hardware and operational gates pass.

## Historical schema-9 operations

The following procedures describe the retired serving path and are retained as
rollback reference for its original revision. They are not commands for a
schema-11 deployment; use the previous release checkout for historical rollback.


The production workflow builds a tested revision, prepares schema-transition data
when needed, deploys the coordinator and workers, then verifies a private query
through the public origin. A build or workflow default does not establish what
is running; retain release identity and public initialization metadata with each
rollout. See [status](status.md) for dated observations.

## Local operation

Build from the repository root. Release mode is required for practical PIR
execution and full-shard tests.

```sh
cargo build --locked --release -p enhance-pir-server -p enhance-pir --bins --features enhance-pir/cli
```

A local server still needs a compatible Zakura archive RPC with canonical blocks
from Ironwood activation. Embedded mode runs the worker in the coordinator process:

```sh
./target/release/enhance-pir-server --mode embedded \
  --listen 127.0.0.1:8080 --zakura-rpc-url http://127.0.0.1:8232 \
  --zakura-cookie /path/to/zakura/.cookie --data-dir ./enhance-local-data
```

For a distributed smoke test, start a worker in another terminal and use its
loopback origin instead of embedded mode:

```sh
./target/release/enhance-pir-worker --listen 127.0.0.1:8091 \
  --data-dir ./enhance-local-worker
./target/release/enhance-pir-server --mode distributed \
  --worker-url http://127.0.0.1:8091 --listen 127.0.0.1:8080 \
  --zakura-cookie /path/to/zakura/.cookie --data-dir ./enhance-local-data
```

The simple `--worker-url` form creates single-replica groups for development.
Production uses ordered named groups with two replicas through `--worker-config`.
Worker APIs are private and unauthenticated at the application layer; restrict
their network access. Do not expose port 8091 as a public client endpoint.
The [integration guide](integration.md#cli-smoke-checks) supplies smoke commands;
substitute `http://127.0.0.1:8080` for the public origin after initialization is ready.

## Release inputs and topology

[Deploy Enhance PIR](../../.github/workflows/deploy-enhance-pir.yml) accepts a full
commit SHA that must equal current `main` and already have a successful
`ci-full.yml` run. Its production job uses the self-hosted deploy runner and
builds checksummed server, worker, CLI, qualification and APM binaries. The artifact
also carries the transparent filter binary used by the shared deployment setup.
Matching service and ingress configuration is assembled with the release.

The workflow has three operating modes:

| Inputs | Effect |
|---|---|
| `artifact_only=true` | Fetch and verify the full-CI artifact without deploying |
| `artifact_only=false`, `prepare_only=true` | Prepare the schema-9 journal alongside the active data |
| Both false | Deploy and verify the release |

The deployment helper uses `WALLET_PIR_COORDINATOR_HOST`,
`WALLET_PIR_DEPLOY_USER`, `ENHANCE_PUBLIC_URL` and `ENHANCE_WORKERS_JSON`.
Its preflight/deploy modes additionally validate artifact, release, SSH and
service-file inputs; use the [script](../ops/scripts/deploy-enhance-pir.sh) as the
complete input reference. This example validates inventory shape only:

```sh
WALLET_PIR_COORDINATOR_HOST=coordinator.example.net \
WALLET_PIR_DEPLOY_USER=deploy \
ENHANCE_PUBLIC_URL=https://enhance.example.net \
ENHANCE_WORKERS_JSON='[{"name":"shard-group-01","replicas":[{"name":"worker-01a","ssh_host":"worker-01a.example.net","service_url":"http://10.0.0.2:8091"},{"name":"worker-01b","ssh_host":"worker-01b.example.net","service_url":"http://10.0.0.3:8091"}]}]' \
enhance/ops/scripts/deploy-enhance-pir.sh validate
```

Deployment inventory uses `ssh_host` and `service_url`; the generated server
worker-config uses a top-level `groups` list and replica `name`/`url` fields.
Each group owns three consecutive shards and has two active-active replicas.
Group order is append-only. The first migration from the legacy flat inventory
requires the explicit topology-change override (`allow_topology_change` in the
workflow, `ENHANCE_ALLOW_TOPOLOGY_CHANGE=true` in the helper). Replacing a replica
inside an existing group does not itself move shard ownership.

## Schema-9 16-bit preparation and cutover

Schema 9 uses `ironwood-enhance-pir-v3` and profile
`simplepir-p16-q46-v1`. It changes the row from 29 to 33 records, the plaintext
modulus from 16,384 to 65,536, and the minimum query width from 41 to 46 bits.
Treat these as one atomic migration. The generation document, client parameter
derivation, parameter ID and persisted worker metadata all bind the profile.
Schema-9 clients reject a schema-8 generation, and workers rebuild version-7
artifacts because artifact version 8 includes the profile identity.

Run the old and new profiles as parallel deployments during adoption. Each
deployment keeps its own journal, artifact root, coordinator and service origin;
route old clients to the schema-8 origin and upgraded clients to the schema-9
origin. Do not serve both profiles from one generation or infer the profile from
query length. Retire the old origin only after client adoption and rollback
criteria are satisfied.

Before production activation, generate and independently review a correctness
certificate for the actual public snapshot and pinned setup. The retained
synthetic fixtures establish at most `2^-143` per query in the stated
independent-sampler model at 46 bits; they are evidence for the profile choice,
not a certificate for a different production snapshot. See
[16-bit expansion](plaintext16-expansion.md).

## Historical schema-8 preparation and cutover

Schema 8 uses `ironwood-enhance-pir-v2` on `/v1/enhance/*` and packs 29 records
into a 21,373-byte row. Schema-7 clients reject it, and a schema-8 client rejects
schema 7: there is no overlap and no compatibility alias. Confirm intended wallet
compatibility before cutover, not after. Build the tested artifact first, then
prepare that same release with `prepare_only=true` and `artifact_only=false`.

Preparation replays canonical blocks from activation into
`/srv/zakura/enhance-data-r29`, resumes an interrupted journal, validates its
anchor and records the prepared release and layout. It neither contacts workers
nor stops production, which continues to serve schema 7 from the untouched
`/srv/zakura/enhance-data-v7`. The preparation job allows six hours, separately
from the bounded deployment job. On the archive host, the equivalent preparation
entrypoint is:

```sh
enhance-pir-server --prepare-only --zakura-cookie /root/.cache/zakura/.cookie \
  --data-dir /srv/zakura/enhance-data-r29
```

After qualification and client conformance, deploy the same release with both
mode switches false. The transition requires a preparation receipt naming both
the exact release and `records-per-row=29`, and uses
`/srv/enhance-pir/artifacts-r29` for worker artifacts. A changed transition
release requires preparation again. Later releases serving the same layout resume
the active journal without repeating the transition.

The deployment gate compares the **live layout** against the release's target
layout, not the schema number alone. That distinction is the reason this section
exists: the 9-to-29 record widening was at one point planned inside schema 7, and
a gate keyed on "schema differs from 7" would have skipped preparation and started
the new binary on a journal it cannot read.

Never point a release at a journal of another layout or edit the manifest to make
it open. The journal manifest carries `records_per_row`, worker artifacts carry
their column count, and both refuse a mismatch; forcing either open produces rows
that decode to the wrong record rather than an error. Preserve the old data and
artifact directories through cutover. Rollback restores the old binaries and
systemd units, whose paths select the untouched old data. Retain the old worker
inventory as well when changing topology.

## Verification and monitoring

Deployment checks `/v1/health`, validates the atomic `/v1/enhance/init` response,
and completes a dummy query through the public HTTPS origin. The old generation
endpoint is read only to capture rollback metadata from legacy deployments.
Record the activated release SHA, schema, geometry, anchor and query result;
successful preparation alone is not a successful rollout.

The coordinator serves `/ready` and `/metrics` for local operations. Readiness can
remain successful during a rebuild while a generation is still answerable. Health
reports phase, coverage, retained generations, shards and configured worker count;
it is not proof that every replica is healthy or that coverage matches chain tip.
The source applies a grace period to syncing/building before health becomes 503.
Use the archive chain tip and replica observations to check freshness and redundancy.

[APM](../services/pir-apm/README.md) scrapes the coordinator's loopback metrics,
shows aggregate and per-worker query timings and can deliver configured alerts.
Client HTTP time includes transport and server processing; APM worker RPC time is
only a subset. Do not subtract independently calculated percentiles to estimate
other stages. Metrics and worker APIs should remain private.

## Infrastructure and recovery

Runtime configuration and binaries live under `/etc/enhance-pir` and
`/opt/enhance-pir`. The archive volume is mounted at `/srv/zakura`; old data and
artifacts use `/srv/zakura/enhance-data` and `/srv/enhance-pir/artifacts`, while the
schema transition uses the `-v7` paths above. Inspect installed units for the
actual active paths before recovery. The attached volume retains its historical
provider name and is protected from replacement.

Use the [shared infrastructure runbook](../../ops/infra/digitalocean/production/README.md)
for state/backend handling. Deployment, manual infrastructure operations and the
expansion controller must hold the same coordinator lock:
`/run/lock/wallet-pir-production.lock`. Use the credential-aware Terraform wrapper
for manual changes; do not apply stale local state or default group counts after
expansion. Credentials come from the authorized runtime source and must not appear
in committed inputs or logs.

## Capacity expansion target

The implemented target assigns three shards per c-4 replica, with two replicas per
group and a four-group ceiling. Qualification, initial migration, controller
installation, online append, failure recovery and operator acceptance are covered
in [capacity expansion](capacity-expansion.md). A short public load test does not
satisfy its six-hour hardware qualification or production-observation gates.

## Validation

Run `make check` before submitting changes. It includes documentation links,
operations safety tests, formatting, Clippy and release-mode workspace tests.
Infrastructure changes additionally require Terraform format/validate and a
reviewed plan. These checks do not establish hardware qualification or a live
rollout.
