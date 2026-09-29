# Serving contract

Enhance, Status and Transparent PIR serve differently: Enhance and Status run a
separate packing router in front of evaluation workers, while Transparent packs
inside the worker. Their controllers still answer the same questions about a
serving process: which executable it runs, whether it has restarted, whether it
holds current authority, and what a refusal means to a caller. This document
records the shared answers and the deliberate differences. Code that implements
the shared parts lives in `shared/pir-control`.

## Process identity

| Field | Meaning | Enhance roles | Status roles | Transparent worker |
|---|---|---|---|---|
| `binary_sha256` | SHA-256 of the started executable (`/proc/self/exe` on Linux) | `/internal/health` | `/control/identity` | `/v1/ready`, `/v1/health` |
| `incarnation` | Random per serving instance; changes on restart | `/internal/health` | `/control/health` (`binding.incarnation`), `/control/identity` | `/v1/ready`, `/v1/health` |
| `started_unix` | Process start, seconds | `/internal/health` | `/control/identity` | `/v1/ready`, `/v1/health` |

A deploy confirms a restart by a changed `incarnation` and the expected
`binary_sha256`. The Enhance coordinator's public `/v1/health` does not carry
identity; deploys read the executable hash through systemd instead. Status keeps
identity off `/control/health` because its `Health` type refuses unknown fields,
so adding one would break an older controller during a rolling deploy.

## Serving authority

Enhance's packing router and query ingress, and every Status role, serve only
while a controller has renewed their authority within five seconds
(`pir_control::CONTROL_WATCHDOG`). The controller renews every second.

| | Enhance router and ingress | Status roles | Transparent worker |
|---|---|---|---|
| Handshake | prepare → `Ack{incarnation, digest, controller_epoch}` → activate → refresh | fence → prepare → activate → heartbeat | control-socket `prepare`/`activate` compare-and-swap on the predecessor `map_sha256` |
| Renewal checks | incarnation, epoch and view digest | binding (epoch, incarnation) and generation; no digest | none; no lease |
| Fence | monotonic controller epoch, recovery epoch and revoked-session superset | recovery epoch bumped on every controller open | reorg epoch; invalidation persisted |
| Control errors | 503 | 409 binding mismatch, 429 busy prepare | CLI error |

Transparent has no lease. The reconciler routes a worker only while it attests
the active publication warm; transport failures remove a recent replica after
three consecutive failures spanning five seconds, and an answer that does not
attest removes it at once.

## Refusals seen by callers

These are wire behavior. Wallets and routers key retries on them, so shared
code maps to each product's codes rather than unifying them.

| Condition | Enhance | Status | Transparent |
|---|---|---|---|
| Overload | 429, `Retry-After: 1` | 429, no `Retry-After` | 503, `Retry-After` |
| Stale routing or revision | 409 | 409 | 409 with `map_sha256` |
| Revoked or expired session | 410 | 410 | — |
| Not assigned here | — | — | 421 |
| No authority or not ready | 503 | 503 | 503 |
| Worker refused before acceptance | 503 with `x-enhance-evaluation: not-accepted`; the router may retry once elsewhere | — | — |
