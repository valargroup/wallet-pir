# Status PIR

Status PIR serves note-status records using CPU evaluation only. CUDA support
is limited to Enhance; enabling the query host's `cuda` feature does not change
Status evaluation. Status role configuration has no `cuda` field.

Its Cargo packages live under
`src/status/`; operations under `tools/status/`, scripts under `tools/status/` and
retained evidence alongside Enhance under [Enhance evidence](../../evidence/enhance/README.md).

The two packages have separate responsibilities:

- `status-publisher` reads blocks and the mempool, checkpoints source state,
  owns publication authority, and prepares and activates datasets on serving
  workers. Its diagnostic commands run through `pir-publisher status`.
- `pir-query-server` serves prepared datasets through Status routers and
  workers. Its `status` module owns role configuration, the generation
  controller, HTTP routes, admission, telemetry and publisher synchronization.
  `status-query-server` holds only the private router-worker messages, with no
  HTTP stack; the root mounts the public routes. `status-shared` defines generations, the wire contract
  and probe client, and encodes datasets without reading the chain.

The publisher depends on query-server; query-server never depends on the
publisher or chain readers. Ingestion and publication run independently inside
the publisher, handing off the latest snapshot through an in-memory channel.
Enhance can embed the publisher. This package layout does not change the wire
protocol, saved state, configuration or deployed binary name.

- [Architecture](architecture_status.md): generations, controller, workers and observation publication.
- [Distributed runbook](status_distributed.md): HTTPS publication, autonomous consumers and operating guidance.
- [Qualification gate](status_qualification.md): release qualification and its tooling.
- [APM rollout](status_apm_rollout.md): dashboard and alert coverage.
- [Historical backend experiments](status_backend.md): retired modes and retained evidence.
- [Changelog](../enhance/CHANGELOG.md): Status releases are recorded with Enhance.
