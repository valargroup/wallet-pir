# Status PIR

Status PIR serves note-status records. Its Cargo packages live under
`src/status/`; operations under `tools/status/`, scripts under `tools/status/` and
retained evidence alongside Enhance under [Enhance evidence](../../evidence/enhance/README.md).

The two packages have separate responsibilities:

- `status-publisher` reads blocks and the mempool, checkpoints source state,
  owns publication authority, and prepares and activates datasets on serving
  workers. Its diagnostic commands run through `pir-publisher status`.
- `status-query-server` serves prepared datasets through controllers, routers and
  workers. Its `service` module owns HTTP routes, admission, telemetry, and
  publisher synchronization; the root query host reserves listeners and
  supervises the role. Its `protocol` module defines the wire contract and probe
  client; its pure snapshot helpers encode datasets without reading the chain.

The publisher depends on query-server; query-server never depends on the
publisher or chain readers. Ingestion and publication run independently inside
the publisher, handing off the latest snapshot through an in-memory channel.
Enhance can embed the publisher. This package layout does not change the wire
protocol, saved state, configuration or deployed binary name.

- [Architecture](architecture_status.md): generations, controller, workers and observation publication.
- [Distributed runbook](status_distributed.md): HTTPS publication, autonomous consumers and operating guidance.
- [Qualification gate](status_qualification.md): release qualification and its tooling.
- [APM rollout](status_apm_rollout.md): dashboard and alert coverage.
- [Synthetic backend](status_backend.md): test-only serving mode.
- [Changelog](../enhance/CHANGELOG.md): Status releases are recorded with Enhance.
