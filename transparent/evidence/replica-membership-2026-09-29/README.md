# Replica membership fix, 2026-09-29

Track A phase 0 ([remaining work](../../docs/remaining-work.md#track-a-elastic-recent-replicas-approved-2026-09-29)):
keep every healthy recent replica routed. Deployed to production over SSH at the
owner's request, without CI or a soak. This is not a capacity measurement and does
not close the 24-hour routing gate.

## Before

`membership-before.json`, 09:20–11:20 UTC, from the reconciler journal and the
per-publication `*.prepared.json` records on the coordinator:

- 93 publications; 91 prepared exactly one recent replica (recent-04 in all 93).
- recent-01, the only managed replica, rejoined routing 92 times and logged 98
  first-attempt status transport failures.
- recent-02 and recent-03 held warm candidates for the current publication but
  stayed active on older digests: nothing activated them.

Causes, from `fleet-config-before.json` and the fleet adapter at `c255ee54`:

1. `managed_recent_workers` named only recent-01 and `reconcile_workers: []`
   disabled unmanaged catch-up, so the other replicas joined only if they won
   the foreground race to quorum plus 0.5 s.
2. A publisher redeploy had regenerated `fleet.json` without `control_sessions`
   and `status_socket_forwarding` (enabled on 2026-09-11 per
   `/opt/transparent-publisher-build/storage-policy-20260911/fleet-before-storage.json`),
   so every status read opened a fresh SSH connection within a 1 s budget.
3. Activation reset routing to the replicas prepared in that window at every
   publication; `ready_replicas` reported only that set.

## Changes and deployment

| UTC | Change | Source |
|---|---|---|
| 11:20 | Fleet adapter: every member managed, probes off the routing lock, failure hysteresis, write-once plans, `membership.json`; `fleet.json` gains `manage_all_workers`, `control_sessions`, `status_socket_forwarding` | `e0b19e91` |
| 11:22 | `prepare_grace_seconds: 3.5` | `89ceb3ee` |
| 12:02 | Publish controller (`ready_replicas` from membership) and `shard-assign` (atomic plan write) | `dbeb3960` |
| 12:03–12:05 | Recent replicas rolled one at a time to worker `dbeb3960` with `roll-recent-replicas.py` (`ff772282`), no maintenance window; each routed again 20–48 s after its roll began | `dbeb3960` |

`fleet-config-after.json` is the resulting configuration (credentials omitted).
`roll.log` is the rolling upgrade's output. Build: `roman-ipir-bench-8vcpu`,
Rust 1.91.0, `cargo build --locked --release`, `RUSTFLAGS=-C target-cpu=haswell`.
Artifact digests are in `manifest.json`. Archive owners were not upgraded and
still run `0ece0ae1…`.

## After

`membership-after-grace.json`, 11:23–12:02 UTC (before the controller restart):
20 publications, 17 prepared all four recent replicas, 7 late rejoins in total and
no status transport failures. Publication freshness observed at 12–16 s (30 s
target). The continuous 5 QPS load showed no errors in the trailing
minutes sampled after each step.

`membership-after-roll.json`, 12:06–12:19 UTC: 11 publications, 9 prepared all four
recent replicas, 2 late rejoins. `load-after-roll.json` is the continuous load's
trailing minute at 12:19:30: 299 exact queries, no errors, `ready_replicas` 4,
freshness 14.5 s.

## Rollback

- Fleet adapter and `fleet.json`: `/opt/transparent-publisher/rollback/phase0-membership-20260929T112011Z/`,
  then restart `transparent-control-sessions` and `transparent-replica-reconciler`.
- Prepare grace: `/opt/transparent-publisher/rollback/fleet.json.before-prepare-grace`.
- Controller: remove `/etc/systemd/system/transparent-publish-controller.service.d/95-track-a.conf`,
  `daemon-reload`, restart. `shard-assign`: `/opt/transparent-publisher/rollback/track-a-dbeb3960/shard-assign`.
- Workers: `/opt/transparent-publisher/rollback/track-a-dbeb3960/workers/<id>/` on each
  recent replica holds the previous binary, `shard-control` and unit.

## Limits

- Load observations are trailing-minute snapshots of the existing 5 QPS client,
  not a capacity or tail-latency measurement.
- `wallet_sync::a_filter_replaced_after_the_map_was_read_refreshes_before_matching`
  failed once in a release run that competed with a concurrent build for CPU and
  passed three times alone; it does not exercise the changed code.
