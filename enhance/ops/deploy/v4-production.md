# Direct v4 deployment on existing production hosts

The user authorized direct SSH deployment and testing on production, shutdown of
legacy Enhance services, no new hosts, and CI bypass where possible. Do not run
the old Enhance deployment workflow or autoscaler over this deployment.

## Inventory and artifact

| Role | Host | Private address |
|---|---|---|
| Coordinator and generator | wallet-pir-coordinator-01 | 10.142.0.3 |
| Worker replica 1 | enhance-pir-worker-01 | 10.142.0.15:8091 |
| Worker replica 2 | enhance-pir-worker-02 | 10.142.0.16:8091 |

Workers are c-4, four dedicated vCPUs and nominal 8 GiB RAM. The coordinator is
m-8vcpu-64gb-intel. All are existing wallet-pir resources in ams3. Worker SSH uses
Roman's local key through the coordinator; no private key was copied to a host.

Deployed candidate revision: `b1863b1d270df52d213d7dd389e5b8bf96bca224`.
Worker binary SHA-256:
`4c3b0a0f12c4a275afd5fd5e89ea7556627c4fc3fb2969b275ec02b499464478`.
Archive SHA-256:
`fec6141be3512dbf5646f3dd4912ea6015c7079cf8fef8d53e9996645f4e6f66`.

This candidate adds the bounded 600-second preparation-request deadline. Its
binary hash was rechecked on the coordinator and both workers after sealed
measurement began. Earlier evidence and rollback paths naming `9718a6dc` are
historical and must not be treated as the current deployed runtime identity.

The release directory is `/opt/enhance-pir-v4/releases/<revision>` on each host.
The local archive is under `.local-pir/v4-candidates/<revision>/`. Runtime secrets
remain on the host; the coordinator uses its existing Zakura cookie file.

## Qualification campaign in progress

The sealed retry uses `enhance-pir-v4-sealed-retry-campaign.service` and private
loopback port 8280. Measurement began at 2026-09-23 13:17:31 UTC. Canonical serving
is stopped for the campaign; the canonical worker directories are preserved as
`worker.canonical`. Do not start the canonical coordinator against workers while
they hold the synthetic fixture.

The first measured publication confirms six sealed shards on `shard-group-01`,
one active shard on `test-support-active`, and two published-ready replicas for
each group. The support group's workers run on the coordinator and are not
qualified c-4 hosts. The physical worker samplers are
`enhance-pir-v4-sealed-retry-sampling.service` and retain raw traces under
`/srv/enhance-pir-v4/validation/sealed-retry-samples`.

The local `finish-sealed-retry.py` supervisor monitors the campaign and sampler
freshness, restores canonical directories/services after termination, then runs
four canonical public/private load cases. Inspect its live PID and the remote
systemd MainPID, not just its status file, when checking supervision. Its source
and launch instructions are preserved in the
[deadline evidence](../../evidence/architecture-v4-sealed-deadline-2026-09-23/README.md).

Keep overload and fault injection outside the six-hour zero-error measurement.
After preserving final reports and sample hashes, use a separate fixture window
for offered-load saturation, replica recovery and delayed reclamation. Record
scheduled latency and unstarted work as well as completed query latency. An
allowed-error overload run cannot replace the zero-error acceptance campaign.

## Services and persistent state

- New coordinator: `enhance-pir-v4-coordinator.service`, loopback port 8080,
  canonical data `/srv/enhance-pir-v4/canonical`, inventory
  `/etc/enhance-pir-v4/workers.json`.
- New workers: `enhance-pir-v4-worker.service`, bound only to their private IP on
  port 8091, with data `/srv/enhance-pir-v4/worker`. The worker runs as the dedicated
  `enhance-pir-v4` system user.
- Worker memory.high is 7 GiB. Measured host reserve determines memory.max:
  7,609,516,032 bytes on replica 1 and 7,610,564,608 on replica 2. Swap is bounded
  at 2 GiB. `/srv/enhance-pir-v4/host-limits.json` records the host measurements.
- Old coordinator, workers, autoscale timer and APM are disabled. Zakura, Caddy
  and unrelated services remain running.
- Rollback copies are at `/var/backups/enhance-pir-v4/direct-9718a6dc` on each
  relevant host. Original legacy data and service definitions are preserved.

The legacy journal packed 29 records per row. Use `migrate-v4-journal.py` only on
a stopped, exclusively locked source and a new destination directory. It checks
record length/block continuity and copies the raw records unchanged, changing
only packing metadata to 33. It never copies compiled serving artifacts or
publication state. A migrated coordinator revalidates its anchor against RPC.

## Completed active capacity campaign

The September 23 campaign completed six hours and 320 publications. Canonical
serving is restored; synthetic worker state is archived as `worker.active-completed`.
The commands below describe the completed run and restoration procedure, not
current running jobs. Worker 1 has a measurement gap; qualification remains open.

Before stopping a transient unit, inspect LoadState. A completed unit may have
been unloaded: treat `not-found` plus MainPID zero as already stopped. Do not
retry a failing stop indefinitely. Sample restarts must use new output directories
and retain explicit gap timestamps; never overwrite prior samples.


`enhance-pir-v4-active-campaign.service` runs on the coordinator. Its output is
`/srv/enhance-pir-v4/validation/active`. It requests 21,600 measured seconds,
300 minimum publications and concurrency 2. Fixtures bind only to loopback 8280.
The real public coordinator is stopped during this campaign.

Each worker runs `enhance-pir-v4-active-sampling.service`, writing one-second
samples to `/srv/enhance-pir-v4/validation/active-samples`. Sampling lasts up to
23,400 seconds to cover initial preparation and final collection. Retain failed
samples. `recorded` means collection ended, not qualification passed.

The canonical worker directories have been renamed to
`/srv/enhance-pir-v4/worker.canonical`. They are separate from fresh synthetic
worker state. Never merge the directories or register synthetic state for serving.

## Restore canonical v4 serving after testing

First inspect the campaign service and its `exercise.json`; an observation timeout
does not mean a running campaign has stopped. Preserve results before cleanup.
Then stop the campaign on the coordinator and sampling on both workers.

On each worker, with the worker service stopped, move the synthetic `worker`
directory to a uniquely named test archive and rename `worker.canonical` back to
`worker`. Refuse to overwrite any existing archive. Start
`enhance-pir-v4-worker.service` on both replicas, then start
`enhance-pir-v4-coordinator.service` on the coordinator.

Verify both worker health endpoints, coordinator `/v1/health`, published replica
counts, a current canonical source anchor, and exact-answer queries through
`https://enhance-pir.valargroup.dev`. Ensure fixture port 8280 is no longer serving.
Keep legacy autoscaling disabled because the user requested no new hosts.

## Legacy rollback rehearsal

Rehearsed successfully on September 23: nine exact schema-8 public queries, then
443 exact v4 public load queries after returning to v4. See the production
evidence `completed/rollback-rehearsal.json`. The preserved schema-8 CLI at
`/opt/enhance-pir-v4/legacy-validation/enhance-pir-cli` is compatible with the
legacy server; the current v4 load client must not be used to validate v2 wire.


Stop the v4 coordinator and both v4 worker services first. Start the existing
`enhance-pir-worker.service` on both workers, then `enhance-pir-server.service` on
the coordinator. Their original binaries, units and data remain in place. Verify
legacy readiness and canonical queries before declaring rollback successful.
Do not automatically re-enable autoscaling or old APM during a brief rehearsal.

To return to v4, stop the legacy coordinator/workers, start both v4 workers with
their canonical data restored, then start the v4 coordinator. Record exact-answer
queries and live health after the return. Unit enablement must match the final
selected service; backups alone are not rollback evidence.
