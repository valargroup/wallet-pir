# Existing c-4 worker campaign plan

Status: prepared, **not dispatched**. This plan does not qualify the candidate or
authorize a production outage. The current pilot gate requires both an active
and a six-sealed profile for at least six measured hours and 300 publications
each. The two existing 8 GiB c-4 workers cannot run either isolated profile
beside their canonical service. With no additional hosts, the two profiles
require at least **12 hours of interrupted serving**, plus preparation and
restoration. Obtain an explicit outage decision before stopping a service.

Read-only preflight at 2026-09-24 02:20 UTC confirmed both workers still run as
`enhance-pir-v4` with `ProtectSystem=strict`, only the canonical worker directory
in `ReadWritePaths`, and the stated memory limits. Each had about 38 GB free on
its root filesystem. Recheck these values immediately before the campaign;
this observation does not reserve disk or authorize the outage.

## Pinned deployment and isolation

- Candidate server revision: `527048217f8cb83d27c838e94dbaac21ac25837d`;
  deployed binary SHA-256:
  `3c10840380c306b0564b4a171d81f51c3ca35089fbc624cba7c7e9b4198e3b29`.
- Coordinator: `167.99.42.60` (`10.142.0.3`); workers:
  `10.142.0.15` and `10.142.0.16`. The workers' existing service is
  `enhance-pir-worker.service`, listening on each private address at port 8091.
- Leave `/srv/enhance-pir-v6/worker` on both workers and
  `/srv/enhance-pir-v6/canonical` on the coordinator untouched. A temporary
  higher-priority systemd drop-in can point the **same** worker service and port
  to a fresh, separately owned directory for each profile. Remove only that
  temporary drop-in during restoration. The existing candidate drop-in at
  `50-candidate-5270482.conf` and the canonical data are rollback inputs.
- The worker unit runs as `enhance-pir-v4` with `ProtectSystem=strict` and
  `ReadWritePaths=/srv/enhance-pir-v6/worker`. Create each new profile directory
  under `/srv/enhance-pir-v6/qualification/` with ownership for that service
  user. The temporary drop-in must **also** add that exact profile directory to
  `ReadWritePaths`; changing `--data-dir` alone leaves the directory read-only
  inside the service. Verify the effective `ReadWritePaths` and a service-owned
  write in the isolated directory before starting the exercise. Preserve the
  canonical path's existing access for restoration.
- The active profile uses one physical replica group. The sealed profile uses
  the same physical pair plus two coordinator-local helper workers solely to
  exercise placement. Helper samples do not substitute for either physical
  worker's complete trace.

## Preflight, after the public load and post-load checks

1. Record UTC start, service unit and drop-in checksums, exact binary and
   manifest checksums, current public generation/anchor, two-replica health,
   APM state, disk free space, effective cgroup limits, and a correct public
   chain-derived query. Retain the operator transcript outside secret-bearing
   shell history. Confirm the worker sample policies still bind the intended
   binary, addresses, port and memory limits.
2. Ensure no public load, wallet pilot, chain extraction, snapshot copy or
   cleanup timer is active. Retain the older release and all canonical data.
   Ensure the restoration operator and incident owner can reach all three hosts
   throughout the outage; do not rely only on a local laptop process. Record
   the planned window so APM's expected missing-replica alerts are distinguishable
   from an unplanned incident; keep the APM process observable.
3. Reserve new, nonexistent profile directories on both workers and the
   coordinator. Check available space before each profile. Do not reuse the
   prior v4 campaign directories, results, or samplers.
4. Install and checksum-verify this PR's `sample-loop.py` and
   `sample-worker.py` together on both workers. The earlier sampler does not
   bind the isolated worker data directory or campaign profile, so it cannot
   supply complete direct-campaign evidence.

## Per-profile sequence

1. Stop the canonical coordinator, then both canonical worker services. Record
   the public outage start. Install the temporary worker service override with
   the **same verified binary, private listen address and port 8091**, changing
   only `--data-dir` and adding that profile's directory to `ReadWritePaths`.
   Use a drop-in ordered after `50-candidate-5270482.conf`; reset `ExecStart`
   before setting its replacement, and inspect `systemctl show` after daemon
   reload. Preserve the existing
   `MemoryHigh=7516192768`, `MemoryMax=7609516032`, and
   `MemorySwapMax=2147479552` limits. Reload systemd and start the two worker
   services; verify their actual command lines, binary digests, cgroups,
   private health, effective writable paths, service-user write access, and
   empty isolated state.
2. Write a new root-only direct sampling policy on each worker with the verified
   release identity, existing memory limits, `sealed_shards: 6`, and the exact
   isolated `data_dir` and `campaign_profile` (`active` or `sealed`). The
   sampler checks these against the running service's binary and command line.
   Start a new `sample-loop.py --direct-policy` process
   with that policy on each worker before starting the exercise. Use a fresh
   output directory and a nine-hour window
   of one-second sampling to allow for initialization before the six measured
   hours. The sampler deliberately inspects
   `enhance-pir-worker.service`; using a separately named transient worker
   service would leave this hardware gate unobserved.
3. Start `enhance-pir-server --sealed-shards 6 exercise
   --isolated-workers --profile PROFILE --data-dir NEW_EXERCISE_DIRECTORY
   --worker-config PROFILE_INVENTORY_JSON --seconds 21600
   --min-publications 300 --publication-interval 60 --concurrency 2`
   on the coordinator with a new exercise data directory and the appropriate
   private worker inventory. For `sealed`, start and verify the two fresh
   coordinator-local helpers and include them as a second group. Set an
   explicit runtime deadline that permits preparation and the measured six
   hours while preventing an orphan exercise. The exercise creates its own
   `--data-dir`; pass a path that does not exist yet.
4. Observe the exercise and both samplers throughout initialization and
   measurement. Stop and restore on wrong answers, failed publication, service
   restart, missing or invalid worker sample, OOM, swap, or uncontrolled disk
   growth. Preserve the raw failure evidence. A report's `unqualified` field
   is intentional: `assess-campaign.py` and operator review still decide the
   physical gates.
5. At the measured end, stop the exercise and samplers, copy immutable reports,
   complete worker traces, and the exact sampling policies, then run
   `assess-direct-campaign.py` for that profile. The older
   `assess-campaign.py` accepts bootstrap-managed pair observations and cannot
   audit these directly deployed worker traces.
   Run the direct assessor on immutable local copies with one `--worker`
   argument per physical worker:

   ```sh
   python3 enhance/ops/scripts/assess-direct-campaign.py \
     --workload /absolute/path/to/exercise \
     --worker /absolute/path/to/worker-01-samples /absolute/path/to/worker-01-policy.json \
     --worker /absolute/path/to/worker-02-samples /absolute/path/to/worker-02-policy.json \
     --out /absolute/path/to/new-assessment.json
   ```

   Review its unproven gates, require at least 300 publications with two
   replicas, and check each worker has uninterrupted samples spanning
   initialization and all six measured hours. Do not start the next profile
   if the first failed.

## Restoration

Stop the exercise and any helper workers. Stop both isolated worker services,
remove only the temporary campaign drop-in, reload systemd, and verify the
effective `ExecStart` again points at the untouched canonical data directory.
Start both canonical workers, then the canonical coordinator. Confirm the
original binary checksums and cgroup limits, two published replicas, no
ingestion or publication error, advancing anchor, APM recovery, and a correct
public chain-derived query. Record the time from restoration start to correct
serving; the release gate requires recovery within ten minutes. Keep the
isolated state and raw traces until the final evidence review, but never let
the canonical service open them.

This plan requires an operator-tested supervisor and exact commands before
execution. Production serving must remain available until the outage decision
is made and the public load and post-load checks are complete.
