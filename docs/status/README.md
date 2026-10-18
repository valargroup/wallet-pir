# Status PIR

Status PIR serves note-status records. Its Cargo packages live under
`src/status/`; operations under `ops/status/`, scripts under `tools/status/` and
retained evidence alongside Enhance under [Enhance evidence](../../evidence/enhance/README.md).

- [Architecture](architecture_status.md): generations, controller, workers and observation publication.
- [Distributed runbook](status_distributed.md): deploy and operate the distributed controller and workers.
- [Qualification gate](status_qualification.md): release qualification and its tooling.
- [APM rollout](status_apm_rollout.md): dashboard and alert coverage.
- [Synthetic backend](status_backend.md): test-only serving mode.
- [Changelog](../enhance/CHANGELOG.md): Status releases are recorded with Enhance.
