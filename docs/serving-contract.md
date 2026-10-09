# Serving contract

Enhance, Status, Transparent and Receiver PIR serve differently: Enhance and
Status run a separate packing router in front of evaluation workers, while
Transparent and Receiver pack inside the worker. Their controllers still answer
the same questions about a serving process: which executable it runs, whether it has restarted, whether it
holds current authority, and what a refusal means to a caller. This document
records the shared answers and the deliberate differences. Code that implements
the shared parts lives in `shared/pir-control`.

## Process identity

| Field | Meaning | Enhance roles | Status roles | Transparent worker | Receiver server |
|---|---|---|---|---|---|
| `binary_sha256` | SHA-256 of the started executable (`/proc/self/exe` on Linux) | `/internal/health` | `/control/identity` | `/v1/ready`, `/v1/health` | `/v1/receiver/health` |
| `incarnation` | Random per serving instance; changes on restart | `/internal/health` | `/control/health` (`binding.incarnation`), `/control/identity` | `/v1/ready`, `/v1/health` | `/v1/receiver/health` |
| `started_unix` | Process start, seconds | `/internal/health` | `/control/identity` | `/v1/ready`, `/v1/health` | `/v1/receiver/health` |

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

Receiver has no controller or lease either: one process indexes and serves, and
its canonical guard revokes every session when an anchor it still serves leaves
the chain.

## Publication identity

Status and Receiver name a publication by a domain-separated SHA-256 of their
manifest's fields at fixed width, strings length-prefixed: Status's
`Manifest::id` (`enhance/crates/enhance-pir/src/status.rs`), and Receiver's
directory revision and session ID. Their manifests, like Enhance's protocol
values, refuse unknown fields, so a new field is a new profile or protocol rather
than one an older reader skips.

## Refusals seen by callers

These are wire behavior. Wallets and routers key retries on them, so shared
code maps to each product's codes rather than unifying them.

| Condition | Enhance | Status | Transparent | Receiver |
|---|---|---|---|---|
| Overload | 429, `Retry-After: 1` | 429, no `Retry-After` | 503, `Retry-After` | 429, `Retry-After: 1` |
| Stale routing or revision | 409 | 409 | 409 with `map_sha256` | 409 |
| Revoked or expired session | 410 | 410 | — | 410 when revoked, and a session file or query answer still being sent is cut short at its next frame; 409 once a displaced session's 60 s grace ends |
| Not assigned here | — | — | 421 | — |
| No authority or not ready | 503 | 503 | 503 | 503 |
| Worker refused before acceptance | 503 with `x-enhance-evaluation: not-accepted`; the router may retry once elsewhere | — | — | — |

## Admission

Enhance's roles, Status and Receiver share one implementation of the bounded
wait queue, bounded body reception and the per-client concurrency cap
(`shared/pir-control/src/admission.rs`, the `admission` feature). Each caller
keeps its own limits and refusal mapping:

| | Executing / waiting | Wait | Queue without a free permit | Per-client cap |
|---|---|---|---|---|
| Packing router | configured, at most 4 / 16 | 2 s | always takes a waiting slot | — |
| Coordinator (legacy local serving) | 4 / 16 | 2 s | always takes a waiting slot | — |
| Query ingress | try-only | — | — | 4 uploads, keyed on forwarded headers then the peer |
| Status roles | 4 / 32 | 1 s | free permit taken directly | — |
| Status coordinator routes | 16 / 8 | 250 ms | free permit taken directly | 2, keyed on forwarded headers then `unknown` |
| Receiver query route | 2 / 8 | 2 s | free permit taken directly | 2 queries, uploads included, keyed on forwarded headers then `unknown` |

Receiver reads the upload within 15 seconds, capped at the largest query, before
the query waits for a permit. It answers a client at its cap or a full queue with
429 and `Retry-After: 1`, a slow upload with 408 and an oversized one with 413.
A query longer than its session's query is 413 and a shorter one 400, both
refused before it waits for a permit.

Transparent keeps its own admission (`transparent-shard-server/src/admission.rs`).
It counts running and queued requests together, starts the deadline before the
upload so reception time counts against it, budgets body bytes before reading,
and answers overload with 503 and `Retry-After`. Folding it into the shared
queue would change what wallets see.
