# Serving contract

Enhance, Status, Transparent and Receiver PIR serve differently: Enhance and
Status run a separate packing router in front of evaluation workers, while
Transparent and Receiver pack inside the worker. Their controllers still answer
the same questions about a serving process: which executable it runs, whether it
has restarted, whether it holds current authority, and what a refusal means to a
caller. This document records the shared answers and the deliberate differences.
Code that implements the shared parts lives in `src/shared/pir-control`; the raw
PIR parameter, evaluation and packing integration they share lives in
`src/shared/pir-runtime`.

## Process identity

| Field | Meaning | Enhance roles | Status roles | Transparent worker | Receiver server |
|---|---|---|---|---|---|
| `binary_sha256` | SHA-256 of the started executable (`/proc/self/exe` on Linux) | `/internal/health` | `/control/identity` | `/v1/ready`, `/v1/health` | `/v1/receiver/health` |
| `incarnation` | Random per serving instance; changes on restart | `/internal/health` | `/control/health` (`binding.incarnation`), `/control/identity` | `/v1/ready`, `/v1/health` | `/v1/receiver/health` |
| `started_unix` | Process start, seconds | `/internal/health` | `/control/identity` | `/v1/ready`, `/v1/health` | `/v1/receiver/health` |

A deploy confirms a restart by a changed `incarnation` and the expected
`binary_sha256`. The Enhance coordinator's private operator `/v1/health` does not carry
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
the chain. In `serve-publication` mode the operator's local publication record
takes the indexer's place as the source of what to serve; the process verifies
each record and its anchor before activating it, and the same guard applies.

## Preparation and refusal kinds

Each product classifies why a preparation or control command did not complete,
internally, as invalid source (the input does not verify), runtime (restoring
or building failed on verified input), admission (budget or a conflicting
hold) or superseded (the predecessor, epoch or record moved on). The wire is
unchanged: Enhance workers still answer 507 or 503 with the same text,
Transparent control replies carry the same `error` string, and Receiver's
public refusals below are untouched. The kind appears only in private reports
(`preparation` on Enhance worker health, `last_preparation` in Transparent
control status, `publication.refusal` on Receiver health) and in the
operations `worker-report`, which keeps process, identity, publication,
preparation and readiness separate and never infers encrypted correctness.

## Publication owners and serving handles

In each product one owner turns prepared candidates into the active view and
decides retention and revocation; request handlers only borrow an immutable
handle to what is active. Prepared state keeps its fields private, so callers
read it through accessors and change it only through the owner's operations:

| | Prepared candidate | Owner | Handle given to requests |
|---|---|---|---|
| Enhance | `EnhanceJournal` appends and rewinds (continuity checked); `PreparedShard` → `CachedShard` on persist or verified load | coordinator; router and ingress `ControlledView` | `Arc` of the activated view or shard |
| Status | `Generation`; identity changes only by `reidentified`/`reobserved`, content only by preparing anew | `Controller`, distributed roles | `Arc<Generation>` |
| Transparent | staged candidate under the control socket | shard and display `live` modules | active publication runtimes |
| Receiver | `PublicationRecord::read` | `serve-publication` loop | `Publication` |

Constructors say which checks they make. Structural checks (lengths, ordering,
hex form, layout) happen when a record or manifest is read or prepared;
cryptographic checks (artifact digests, public parameters) happen when an
artifact is loaded or a view is prepared; chain-anchor acceptance happens only
in the owner, immediately before activation. Shared code provides only the
durability mechanisms in `src/shared/pir-control`; each owner keeps its own
authority and retention rules above.

Transparent history and txid display share a private activation transaction
in `transparent-shard-server`. The adapters stay private to that crate.
The transaction holds command serialization in a blocking job,
so a disconnected caller cannot release it while persistence is in progress.
After checking already-active success and the expected predecessor, it locks
the candidate and then the publication gate. Under that gate it checks the
candidate's existence, reorg epoch, digest, readiness and product eligibility,
then writes the unchanged publication record through the existing persistence
function. Only a successful write consumes the candidate and installs it in
memory. Serving-state locks are released before disk I/O.

A persistence error leaves the in-memory active view and candidate unchanged,
including an error syncing the parent directory after rename. The durable file
may already name the new publication in that case; restart follows the existing
record recovery and revocation checks. Activation reports the error and does
not automatically retry.

Each product keeps its own adapter and installation rules. History still
qualifies archive retirement before activation, obtains the manager's fresh
authority decision under command serialization, and retains its separate
initial activation and required-runtime checks. It releases predecessor pins
after installation. Display still starts empty, stages independently, retires
predecessors, and removes covered staging entries only after successful
activation. Request handles and collection retain their existing lifetimes.

## Publication identity

Status and Receiver name a publication by a domain-separated SHA-256 of their
manifest's fields at fixed width, strings length-prefixed: Status's
`Manifest::id` (`src/status/status-query-server/src/protocol.rs`), and Receiver's
directory revision and session ID. Their manifests, like Enhance's protocol
values, refuse unknown fields, so a new field is a new profile or protocol rather
than one an older reader skips.

## Refusals seen by callers

These are wire behavior. Wallets and routers key retries on them, so shared
code maps to each product's codes rather than unifying them.

| Condition | Enhance | Status | Transparent | Receiver |
|---|---|---|---|---|
| Overload | 429, `Retry-After: 1` | 429, no `Retry-After` | 503, `Retry-After` | 429, `Retry-After: 1` |
| Stale routing or revision | 409 | 409 | 409 with `map_sha256` | — |
| Revoked or expired session | 410 | 410 | — | 410, as is any session not served |
| Not assigned here | — | — | 421 | — |
| No authority or not ready | 503 | 503 | 503 | 503 |
| Worker refused before acceptance | 503 with `x-enhance-evaluation: not-accepted`; the router may retry once elsewhere | — | — | — |

## Admission

Enhance's roles, Status and Receiver share one implementation of the bounded
wait queue, bounded body reception and the per-client concurrency cap
(`src/shared/pir-control/src/admission.rs`, the `admission` feature). Each caller
keeps its own limits and refusal mapping:

| | Executing / waiting | Wait | Queue without a free permit | Per-client cap |
|---|---|---|---|---|
| Packing router | configured, at most 4 / 16 | 2 s | always takes a waiting slot | — |
| Query ingress | try-only | — | — | 4 uploads, keyed on forwarded headers then the peer |
| Status roles | 4 / 32 | 1 s | free permit taken directly | 2 on the router's query route, keyed on forwarded headers then `unknown` |
| Receiver query route | 2 / 8 | 2 s | free permit taken directly | 2 queries, uploads included, keyed on forwarded headers then `unknown` |

Receiver reads the upload within 15 seconds, capped at the largest query, before
the query waits for a permit. It answers a client at its cap or a full queue with
429 and `Retry-After: 1`, a slow upload with 408 and an oversized one with 413.
A query longer than its session's query is 413 and a shorter one 400, both
refused before it waits for a permit.

Transparent and txid share their own admission (`transparent-server-backend/src/admission.rs`).
It counts running and queued requests together, starts the deadline before the
upload so reception time counts against it, budgets body bytes before reading,
and answers overload with 503 and `Retry-After`. Folding it into the shared
queue would change what wallets see.

## HTTP resource lifetimes and observations

`pir-control` supplies bounded body reading and `admission::spawn_admitted`,
which moves admission and prepared handles into blocking execution and returns
ownership with the result. Dropping the awaiting request cannot release resources
while that work continues. Products decide when completed work releases admission.
`http::retain` holds resources until the transport drops a response body, preserving
its framing and hints; `http::guarded` also checks a product fence before emitting
buffered bytes. Enhance uses the fenced body; Receiver retains query admission and
its publication, and Transparent retains prepared handles and query memory.

`pir-observability::ObservedBody` counts polled frames through product callbacks.
Its counters and rendering are runtime-independent; the Axum middleware and
`ObservedBody` are the optional host-only `http` feature.
Enhance keeps its existing metric families and starts processing time after upload;
Transparent and Receiver retain their handler-duration and cancellation accounting.
Their adapters classify paths into fixed labels before calling shared observation;
shared code never recognizes product URLs. Adapters retain the historical fixed
categories for product-shaped unmatched paths too, preserving their 404 metrics.
Limits, deadline origins, admission
ordering, revocation checks, retries and HTTP refusals remain product policy.
