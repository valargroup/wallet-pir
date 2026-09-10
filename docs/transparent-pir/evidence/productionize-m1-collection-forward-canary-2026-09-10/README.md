# M1 collection and forwarded-status canary

Observed 2026-09-10. This is a deployment/start capture, not acceptance.

Worker source `d8f5217`, binary SHA-256
`fee415d93ad15dbd18612777c9d05b065ed71f5214d1bb86aefe6bb9b3fec3c5`,
is deployed on recent-01. Operations source `eb116e9` is deployed with
`control_sessions: true` and `status_socket_forwarding: true`. Other workers
await the gated rollout. No timing, freshness or acceptance budget changed.

## Qualification and deployment

The [collection qualification](../productionize-m1-collection-qualification-2026-09-10/README.md)
records three passing Amsterdam screens. The
[forwarding evidence](../productionize-m1-forwarded-status-2026-09-10/README.md)
records implementation, reconnect validation and full checks: 588 Rust tests
passed, two ignored, and 86 operations tests passed.

`diagnostic-final.tar.gz` includes six-worker forwarding checks (all statuses
warm, socket mode 600), the maintenance-protected forwarding deployment and
its result. Both public origins were reopened after validation. Collection
worker upgrade then completed with 42 exact maintenance queries and 6.537 s
warm readiness; `forward-start.tar.gz` preserves its verification and artifact
manifest, public maps, process state and initial query/sample logs.

## Preceding diagnostic failure

The preceding bounded diagnostic observer failed at **21:35:50 UTC** after
4,187.375 seconds and 56 blocks with HTTP 503. It was already terminal before
operator cleanup. `preceding-diagnostic-result.json` and the archived
`before-units.txt` establish this. The archived `stop.json` describes the later
operator cleanup; it must not be interpreted as a successful or merely
interrupted diagnostic run.

Final memory-backed worker probes measured a **3.902 s Unix status request**
and a **3.883 s HTTP request**, around 21:35:45 UTC. The earlier partial capture's
202 ms maximum was not the final maximum. These requests completed before the
probe's four-second timeout, so zero probe errors does not mean no stall.
Thread and CPU samples were captured during the pause; its complete cause is
not established. Neither forwarding trials nor the cleanup regression prove
that every production pause is fixed. Raw probes are nested in
`diagnostic-final.tar.gz`; none count toward acceptance.

## Fresh acceptance

`transparent-m1-collection-forward-rollout.service` entered canary observation
at **21:41:13 UTC** on coordinator `167.99.42.60`. Output root:
`/opt/transparent-publisher-build/collection-20260910/forward-rollout`.
At the saved 21:45:27 capture it was active; it was independently rechecked
active at 21:53 UTC, with 13 new blocks, about 10,700 exact queries, 11 retries,
no recorded worker restart and no OOM. These are partial observations.
Captured files were copied sequentially while clients were appending; counts
are not an atomic snapshot across files.

The supervisor must pass both six hours and 300 new blocks before fleet
promotion, then complete the full 24-hour all-worker observation. M1 remains
open. A separate bounded memory-backed candidate probe records status timing,
thread states and host pressure; it is diagnostic only.
