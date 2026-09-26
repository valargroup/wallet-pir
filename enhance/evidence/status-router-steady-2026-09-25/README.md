# Sustained direct-router Status load — 2026-09-25

Started a six-hour open-loop run at **20 offered lookups per second**, with a
maximum of sixteen concurrent clients and the existing five-second lookup budget.
Live publication continues independently. The systemd unit is
`status-router-steady-load` on the Enhance coordinator; it does not restart on
failure and stops after the requested duration.

Encrypted queries use the dedicated SSH tunnel on loopback 8492 directly to the
router, bypassing coordinator query forwarding. Initialization and public material
still come from the coordinator on 8480; the local chain RPC supplies the independent
mined-answer oracle. Client timing still includes the remote transport, initialization,
client preparation, and decoding; it is not router processing p99.

The load binary adds optional `probe-live-load --query-origin` without changing any
serving binary or restarting serving roles. Strict Clippy passed. Runtime validation
confirmed that the coordinator forwarding count stayed at 1,558 while router and
worker counts increased, and the public Status APM showed approximately 20
completions per second.

**This is a diagnostic run, not passing qualification.** At the first 877 recorded
arrivals there were 683 correct, 12 overall lookup timeouts, and 182 unstarted.
The latest ten seconds at that snapshot completed all 200 lookups. All startup
failures remain in the report; the offered rate and gate have not been relaxed.

Run metadata is in `run.json`. Full per-arrival records and the eventual summary
remain under the recorded report directory on the coordinator. The planned end is
2026-09-25 22:51:42 UTC. Public Status serving remains disabled.

Stop command, if needed:

```sh
ssh root@167.99.42.60 'systemctl stop status-router-steady-load'
```

Inspect service state:

```sh
ssh root@167.99.42.60 'systemctl status status-router-steady-load --no-pager'
```
