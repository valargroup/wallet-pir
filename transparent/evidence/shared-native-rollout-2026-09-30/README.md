# Recent replicas on the shared native crate — 2026-09-30

The three recent replicas were rolled, without a maintenance window, to worker
binary `291cd504…` built by full CI from `6360f0d8`. That source serves queries
through the shared `shared/pir-native` crate (the helpers Enhance and Status also
use, pinned by its golden vectors) and reports the process identity in
`docs/serving-contract.md`. The archive owner was not upgraded and still runs
`4e3f8d9d…` (`a704616c`); the wire protocol is unchanged.

## Procedure (UTC)

| Time | Step |
|---|---|
| 04:40 | Scaler policy `mode` set to `observe`; the previous file is kept as `policy.json.before-6360f0d8-roll` |
| 04:41 | Operator `transparent-fleet-actuator.sh scale-out --count 1`: elastic `transparent-pir-recent-08` (10.142.0.6) created, enrolled, installed with the current release (`a704616c`) and routed at 04:45 |
| 04:46 | `transparent-5qps-continuous` stopped (it latches on worker restarts) |
| 04:46–04:48 | `roll-recent-replicas.py --artifacts …/releases/6360f0d8/binaries`: recent-01, recent-02 and recent-08 in turn, each only while the other two were routed; 25.4 s, 23.0 s and 23.2 s ([log](raw/roll-recent-replicas.log)) |
| 04:48 | `qualified-workers.json` re-pinned from each member's `/v1/ready` ([before](raw/qualified-workers-before.json), [after](raw/qualified-workers-after.json)); the 5 QPS load restarted and returned to `running` |
| 04:48–04:58 | 20 QPS gate below |
| 04:59 | Scaler policy restored byte for byte apart from `mode: act` |

Artifacts: the `transparent-publisher` bundle of full-CI run 36638226167,
checked with `tools/ci/release.py extract` (`transparent-shard-server`
`291cd5043e35…`, `shard-control` `04f73ad4ab0c…`).

## 20 QPS gate

Three `rate-query` clients at 7 QPS each for 600 s from the build host
(`roman-ipir-bench-8vcpu`) against `https://transparent-pir.valargroup.dev`,
with the fixture and scripts of the
[recent-floor gate](../recent-floor-2026-09-29/README.md) (80% recent, 20%
archive, fresh keys, exact row hashes). Three recent replicas were routed, not
two, so this is not a like-for-like repeat of that gate's two-replica result.

| Queries | Exact | Errors | Missed slots | Achieved QPS | p50 | p95 | p99 | Max |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 12,528 | 12,528 | 2 | 42 | 20.88 | 15.7 ms | 34.7 ms | 47.6 ms | 2.16 s |

Both errors were first-attempt send failures at the public edge
(`error sending request`), retried by the client; every logical query completed
exactly. Raw events: [gate-queries.jsonl.gz](raw/gate-queries.jsonl.gz); client
window in [gate-client-window-unix.txt](raw/gate-client-window-unix.txt). Every
member attested warm on the active map afterwards ([ready-after.jsonl](raw/ready-after.jsonl)).

## Left in place

- `transparent-pir-recent-08` stays enrolled. The daily destroy budget was spent
  on 2026-09-29, so the scaler (back in `act`) removes it once a destroy leaves
  the 24-hour window and demand allows.
- The archive owner needs a maintenance window to change binary.
