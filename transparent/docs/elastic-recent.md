# Elastic recent replicas

Track A of [remaining work](remaining-work.md#track-a-elastic-recent-replicas-approved-2026-09-29):
the recent tier is replicated, grows and shrinks automatically, and replaces
failed replicas; the archive stays static, one host per range (one owner
holding 0–76 after the 2026-09-29 consolidation), changed only by operators. This document describes the source: the files each
process owns, their formats and the invariants every change must keep.
[Deployment](deployment.md) owns operating targets; [status](status.md) owns
what is live.

## Invariants

1. Wallets see no change: one router URL, the shard id in the path, 409/421/503
   semantics, metadata from the publication authority.
2. A member is routed only if it attests the active publication warm, is in that
   publication's assignment, and its intent is `enrolled`. A `draining` recent
   replica is routed only while no enrolled recent replica attests.
3. Inventory, scaler or actuator activity never withdraws the service.
4. One assignment per publication, written once. A member's shard set never
   changes during its life. Assignments stay `transparent-assignment-v1`.
5. At most one Terraform apply per root, even if its client dies; at most one
   actuator operation at a time.
6. Serving recent replicas never drop below two outside withdrawal or
   maintenance. A serving replica is stopped only if two others serve; a failed
   one is destroyed only after its replacement serves. Only elastic droplet ids
   recorded in the inventory can be destroyed, within a daily cap.
7. Stale or unknown signals mean hold, never act.
8. Enhance is unchanged: its production Terraform root, wrapper, CLIs and tests.
9. The scaler and actuator never change archive members, their ranges, the
   archive partition or `recent_from`, and never touch archive droplets.
10. Every current archive range has exactly one live owner. The partition
    changes only through the operator's `repartition`, which swaps partition
    and owners in one revision; the previous owners keep running until an
    operator stops them, so `restore` can put them back.

## Processes and files

All paths are on the coordinator.

| Process | Writes | Reads |
|---|---|---|
| Operator CLI `transparent-fleet-inventory.py` | inventory (any member, archive only with `--archive` and never in `act` mode) | – |
| Operator `transparent-archive-standby.py` (root) | a host not yet in the inventory; `state/standby/<id>/` plans | inventory, `roster.json`, `state/active.json`, publisher `/v1/status` |
| Actuator `transparent-fleet-actuator.py` (root, credentials) | inventory (elastic recent members only), `scaler/journal/`, elastic Terraform state, `credentials/known_hosts` entries of elastic members | `scaler/request.json`, `scaler/policy.json`, membership |
| Reconciler (`transparent-live-fleet.py --reconcile`) | `state/membership.json`, `state/active.json`, per-publication plans | inventory projection (`roster.json`) |
| Scaler `transparent-fleet-scaler.py` (no credentials) | `scaler/state.json`, `scaler/status.json`, `scaler/decisions.jsonl`, `scaler/request.json` | membership, worker `/metrics`, publisher `/v1/status`, `scaler/policy.json` |
| APM `scaling` family | incidents | `scaler/status.json` |

`/opt/transparent-publisher/state/inventory.json` is the durable intent:

```json
{"schema": "transparent-fleet-inventory-v1", "revision": 12,
 "updated_by": "operator:roman", "updated_unix": 1790680000.0,
 "partition": {"ranges": [{"id": "a2", "first": 0, "last": 76}]},
 "partition_history": [{"id": "a0", "first": 0, "last": 38},
                       {"id": "a1", "first": 39, "last": 76}],
 "repartition": {"from_revision": 11, "revision": 12, "unix": 1790680000.0},
 "members": [
  {"id": "transparent-pir-archive-01", "role": "archive-owner", "group": "a0",
   "origin": "static", "intent": "retired", "retired_reason": "repartition",
   "ssh_host": "10.142.0.6", "upstream": "10.142.0.6:8093",
   "cache_bytes": 51539607552, "memory_max": "56G", "build_slots": 1},
  {"id": "transparent-pir-archive-03", "role": "archive-owner", "group": "a2",
   "origin": "static", "intent": "enrolled", "ssh_host": "10.142.0.9",
   "upstream": "10.142.0.9:8093", "cache_bytes": 51539607552, "memory_max": "56G",
   "build_slots": 1, "ssh_host_key": "ssh-ed25519 AAAA…"},
  {"id": "transparent-pir-recent-05", "role": "recent-replica", "group": "recent",
   "origin": "elastic", "intent": "enrolled", "ssh_host": "10.142.0.20",
   "upstream": "10.142.0.20:8093", "cache_bytes": 5368709120, "memory_max": "7G",
   "build_slots": 1, "droplet_id": "512345678", "size": "s-4vcpu-8gb",
   "ssh_host_key": "ssh-ed25519 AAAA…", "installed_release": "dbeb3960"}]}
```

- Writes hold `state/inventory.lock`, compare the expected `revision`, write
  atomically, append the change to `state/inventory.log.jsonl` and keep the
  revision as `state/inventory.d/<revision>.json`.
- Intents: `enrolled`, `draining`, `retired`, `quarantined`. `retired` is
  terminal and kept so ids are never reused, except that `restore` returns the
  owners the last repartition retired. `draining → enrolled` cancels a drain.
  Host facts never change after enrollment.
- `partition.ranges` is contiguous from shard 0. `partition_history` keeps the
  ranges a repartition replaced; their ids are never reused, and only a
  retired archive owner may name one. Live and quarantined owners name a
  current range.
- `repartition --ranges ID:FIRST-LAST,… --owner ID=…` (with `--archive`, never
  in `act` mode) probes each new owner (`/v1/ready`, and a control-socket
  status warm on a publication whose archive shards are the active one's),
  then in one compare-and-swap revision sets the new partition, enrolls one
  static owner per range, retires every other archive owner with
  `retired_reason: repartition`, and records `repartition.from_revision`.
  The archive's last shard cannot move.
- `restore --revision N` accepts only that `from_revision`, only while no later
  revision changed archive state, and only while every old owner still runs
  (not `unreachable` in `membership.json`, service active over SSH, `/v1/ready`
  ready; `--force` skips this). It writes a new revision with N's partition and
  archive owners, retires the owners the repartition added, keeps their ranges
  in history and keeps later recent-tier changes.
- The fleet `known_hosts` carries the pinned key of every live member that has
  `ssh_host_key`: elastic replicas and archive owners a repartition enrolled.
- The inventory regenerates `roster.json`: every `enrolled` or `draining`
  member, archive owners with `archive_range: [first, last]` from their group.
  `shard-assign plan` gives pinned owners exactly those ranges, so adding or
  removing recent replicas never moves an archive cut.
- A reader that cannot parse or validate the inventory keeps the last good
  revision and never withdraws. `reconstruct` rebuilds it from the live roster,
  the active assignment and DigitalOcean tags.

`state/membership.json` (reconciler, every second):

```json
{"schema": 1, "updated_unix": 1790680000.5, "active_map_sha256": "…",
 "routed": ["…"], "rendered": ["…"], "routed_recent": 2, "rendered_recent": 2,
 "routing_generation": 14,
 "members": {"transparent-pir-recent-05": {"role": "recent-replica",
   "intent": "enrolled", "state": "serving", "routed": true, "rendered": true,
   "map_sha256": "…", "warm": true, "candidate_map_sha256": null,
   "preparing": false, "transport_failures": 0, "observed_unix": 1790680000.2}}}
```

`state` is one of `booting unreachable invalidated serving prepared warming
lagging`. `routed` means the member attests the active publication;
`rendered` means the router currently sends it traffic (routed and not
filtered out as draining). A draining member is *drained* once it has been
absent from `rendered` for 120 s.

`scaler/policy.json` (operator):

```json
{"schema": 1, "mode": "observe", "min_recent": 2, "max_recent": 6,
 "size": "s-4vcpu-8gb", "capacity_qps_per_replica": 8.0,
 "capacity_evidence": "transparent/evidence/…", "headroom": 1.5,
 "max_step": 3, "stale_seconds": 45, "window_seconds": 300,
 "backstop": {"error_rate": 0.05, "error_seconds": 120, "p99_seconds": 1.4, "p99_hold_seconds": 300},
 "scale_in": {"hold_seconds": 3600, "p99_seconds": 0.8},
 "cooldown_out_seconds": 900, "cooldown_in_seconds": 3600,
 "replace_unhealthy_seconds": 900,
 "daily_actions": 6, "daily_destroys": 2, "monthly_cost_cap_usd": 400,
 "paused_until_unix": null}
```

Modes: `observe` (decide and log only), `recommend` (also publish the
recommendation in `status.json`), `act-dry` (the actuator plans but never
applies), `act`.

`scaler/request.json` (scaler → actuator; replaced atomically, consumed once
by `decision_id`):

```json
{"schema": 1, "decision_id": "…", "created_unix": 1790680000.0,
 "action": "scale_out", "count": 1, "reason": "offered 21.4 qps > 2 × 8.0 / 1.5"}
```

`action` is `scale_out` (with `count`), `scale_in` or `replace` (with
`member`). The scaler never names an archive member.

`scaler/status.json` (scaler, every cycle; read by APM):

```json
{"schema": 1, "updated_unix": 1790680000.0, "mode": "observe",
 "heartbeat_unix": 1790680000.0, "decision": {"action": "hold", "reason": "…"},
 "desired_recent": 2, "serving_recent": 2, "offered_qps": 3.1,
 "holds": ["stale signal: transparent-pir-recent-02"],
 "operation": {"id": "…", "phase": "bootstrapping", "age_seconds": 120,
               "deadline_exceeded": false, "fenced": false},
 "budget": {"actions_left": 5, "destroys_left": 2, "monthly_cost_usd": 168.0},
 "forecast": {"days_to_recent_budget": 240.0},
 "awaiting_operator": [], "orphans": []}
```

`operation` is `null` when none is open. `awaiting_operator` lists, as short
strings, what the scaler or actuator cannot do without an operator (for example a
fenced apply awaiting `resolve-apply`); `orphans` lists droplet ids tagged as
elastic members but absent from the inventory. Both are empty lists when there is
nothing to report. The whole file is rewritten every cycle, so `updated_unix` dates
every field. The APM `scaling` family treats contents older than 90 s, and any
absent or `null` field it reads, as unknown
([alert rules](../../enhance/docs/observability-alerting.md#transparent-scaler-alerts)),
so the scaler publishes a finite `forecast.days_to_recent_budget`, large when
demand is flat.

## Actuator operations

`scaler/journal/operation.json` holds at most one open operation; completed
ones move to `scaler/journal/done/`. Every phase is written before its side
effect. An interrupted Terraform apply is *fenced*: nothing proceeds until
Terraform state and the DigitalOcean API agree (`resolve-apply`).

- **scale_out**: `requested → planned → applying → provisioned → enrolled →
  bootstrapping → installed → serving`. The member is enrolled before it boots,
  so the next publication's assignment names it; the reconciler then warms and
  routes it.
- **scale_in**: `requested → draining → drained → stopped → retired → planned →
  applying → destroyed`. Only elastic members; refused unless two other recent
  replicas serve.
- **replace**: a scale_out, then the failed member is quarantined; an elastic
  one is destroyed once its replacement serves.

Terraform for elastic members lives in `ops/infra/digitalocean/transparent-elastic/`,
its own state and lock, one droplet and one project-resources entry per member,
the image pinned per member. The saved-plan validator allows only creates and
destroys of named elastic recent droplets and their project entries.
