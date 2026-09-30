# Shared layers: production checks and fixes — 2026-09-30

Source for the shared layers of the
[serving contract](../../../docs/serving-contract.md): `shared/pir-native`,
`shared/pir-control`, the shared admission module, the transactional deploy CLI
and the control-session supervisor. This records what was checked or changed
in production. No Enhance or Status serving process was restarted: every
current role either is a single instance or (the workers) stops without
draining, so a restart would interrupt service or fail in-flight queries.

## Deploy CLI against production (read-only)

`ops/scripts/wallet-pir-deploy.py` ran from a workstation over SSH jumps
through the coordinator (`ssh.config_file` inventory; the inventory stays
outside the repository).

- `capture-baseline` recorded every Enhance and Status unit
  ([enhance](raw/capture-baseline-enhance.txt), [status](raw/capture-baseline-status.txt)).
  The coordinator unit is `enhance-pir-coordinator.service`; roles run from
  `/opt/wallet-pir/releases/<rev>/native/` under stacks of up to 13 drop-ins.
- The first plan showed the managed drop-in would have lost to those stacks, so
  the CLI was changed before any write (`41e05de6`): its drop-in now sorts after
  them and shadows them.
- `plan` with the router's running binary: workers, router and ingress
  unchanged; only the coordinator, on another binary (`b6857a65…`), would
  restart ([plan](raw/plan-enhance-running-router-binary.txt)). Status: worker
  and router unchanged, controller on another binary
  ([plan](raw/plan-status-running-role-binary.txt)).
- `preflight --only` with the running binaries planned no restart for the
  Enhance workers, router and ingress
  ([preflight](raw/preflight-enhance-workers-router-ingress.txt)) or the Status
  worker and router ([preflight](raw/preflight-status-roles.txt)). The live Status
  role units match their templates.

The router's only preferred worker is a GPU worker at `10.250.90.3`, outside
this inventory; the two CPU workers serve overflow.

## Status control account

`AllowStreamLocalForwarding no` was added to the `status-control` Match block on
`status-pir-01` and sshd reloaded (about 21:56 UTC on 2026-09-29). Both tunnels
stayed active, a fresh TCP forward answered and a Unix-socket `-R` was refused;
the previous file is `/root/60-status-control.conf.before-streamlocal-20260930`
on that host. Soak batch 83 (21:51:51–22:01:56) covers the reload: Status
12,000/12,000 correct.

## Alert fixes

- **Scaler budget** (`55291f11`): the actuator refused a replace request that the
  scaler then retried, and both requests were charged, so `destroys_left` read -1.
  Deployed to the coordinator's scaler at 21:51 UTC; it read 0 at the next cycle
  ([status](raw/scaler-status-after-fix.json)).
- **Host sampler** (`a8c83991`): its static list still named removed members and
  sampled archive-03 at 10.142.0.7 as `transparent-pir-recent-03`. It now builds
  worker targets from the roster every cycle; deployed and verified fresh for all
  seven sources.
- **Retired incidents** (`70366868`): see below.

## Load

The continuous joint soak on the coordinator (`prod-soak-continuous-20260929`,
started 07:54 UTC on 2026-09-29 by another session; 20 QPS Enhance through the
public URL and 20 QPS Status through the router query tunnel, 600 s batches,
gate p99 < 2 s and p50 < 700 ms) covers this work. Captured batches and its
manifest are in [raw](raw/soak-continuous-batches.jsonl).
