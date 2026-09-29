# Elastic recent tier validation, 2026-09-29

Track A phases 2–4 ([remaining work](../../docs/remaining-work.md#track-a-elastic-recent-replicas-approved-2026-09-29))
exercised in production against real droplets, with the scaler in `act` mode
under a temporary validation policy (`validation-policy.json`: capacity 3 QPS
per replica, `max_recent` 3, one host per step, 300 s cooldowns and scale-in
hold, 180 s replacement window). The production policy was restored afterwards
(`production-policy.json`). Deployed over SSH without CI at the owner's
request. Operations are the actuator's journal records (`journal/`); decisions
are the scaler's log (`decisions.jsonl`).

## Operator-driven (phase 2)

| UTC | Operation | Result |
|---|---|---|
| 13:00 | `scale-out --count 1 --dry` | Plan: create exactly `digitalocean_droplet.recent["transparent-pir-recent-05"]` and its project entry; validator accepted |
| 13:12–13:19 | `scale-out --count 1` | recent-05 created in 40 s, bootstrapped over its pinned host key with release `a704616c`, caught up and routed (`serving`) |
| 13:19–13:24 | `scale-in --member transparent-pir-recent-05` | Drained, waited out 120 s drained plus 30 s quiet, stopped, retired, destroyed droplet 604659753 |

## Scaler-driven (phases 3 and 4)

| UTC | Event |
|---|---|
| 13:49 | Extra load from the build host (2 × 5 QPS) on top of the continuous 5 QPS |
| 13:51 | `scale_out`: offered 12.0 QPS × 1.5 / 3.0 needs 3 > 2 serving, after a 60 s confirmation |
| 13:51–13:59 | Actuator created, bootstrapped and routed recent-06 |
| 13:59:38 | recent-06's worker stopped by hand to simulate a failure; the router dropped it within 10 s |
| 14:03 | `replace`: recent-06 had not attested for 210 s without warm progress. The actuator refused it (`max_recent` counted the member being replaced) |
| 14:05 | Actuator fixed (`f76a3172`: a replacement keeps the group size; `a5c77d40`: refusals are answered in the journal history) |
| 14:07–14:15 | `replace` again: recent-07 created and routed first; only then was recent-06 quarantined, retired and destroyed |
| 14:15 | Extra load stopped |
| 14:30–14:34 | `scale_in` of recent-07 after the 300 s hold: drained, stopped, retired, destroyed droplet 604672625 |

Serving recent replicas never dropped below two. The continuous 5 QPS load
paused only while a deliberately stopped replica was still enrolled; no query
returned a wrong row.

## Findings fixed during validation

- The actuator refused a replacement at `max_recent`: it counted the member
  being replaced. A replacement keeps the group's size.
- A refused request was never answered, so the scaler waited out its 600 s
  request TTL. Refusals are now recorded in the journal's history.
- The scaler's `serving_recent` briefly read 1 at 13:55: the status was written
  during the second in which an activation had unrouted a replica the
  reconciler re-added at once. It is a one-sample artefact, not a drop in
  routing.

## After

At 14:35 the production policy was restored (`production-policy.json`: `act`,
10 QPS per replica with 1.5× headroom, `max_recent` 6, three hosts per step,
15-minute scale-out and 60-minute scale-in cooldowns, 60-minute scale-in hold,
15-minute replacement window, six actions and two destroys per day, a $400
monthly cap). The scaler held at two replicas with 4.0 QPS offered. The APM
`scaling` family was deployed in shadow (`pir-apm` `a5c77d40`,
`PIR_APM_SCALER_STATUS`, `PIR_APM_SCALING_ALERT_MODE=shadow`); its status and
heartbeat conditions evaluate healthy and the forecast is unknown until daily
samples accumulate.
