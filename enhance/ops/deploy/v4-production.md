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

Deployed candidate revision: `9718a6dcf9801385f69f31bb71f02efd261914f0`.
Worker binary SHA-256:
`258fc7d0070b821d34710d5c25edbf9176472c6e39defca020f8944a045c1def`.
Archive SHA-256:
`21a5f0dd9319bb8077f64eddd9baf5f6961d2cae8d0ac658d46e2823db4e36a7`.

The release directory is `/opt/enhance-pir-v4/releases/<revision>` on each host.
The local archive is under `.local-pir/v4-candidates/<revision>/`. Runtime secrets
remain on the host; the coordinator uses its existing Zakura cookie file.

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

Stop the v4 coordinator and both v4 worker services first. Start the existing
`enhance-pir-worker.service` on both workers, then `enhance-pir-server.service` on
the coordinator. Their original binaries, units and data remain in place. Verify
legacy readiness and canonical queries before declaring rollback successful.
Do not automatically re-enable autoscaling or old APM during a brief rehearsal.

To return to v4, stop the legacy coordinator/workers, start both v4 workers with
their canonical data restored, then start the v4 coordinator. Record exact-answer
queries and live health after the return. Unit enablement must match the final
selected service; backups alone are not rollback evidence.
