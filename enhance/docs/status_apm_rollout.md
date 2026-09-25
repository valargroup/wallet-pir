# Unified PIR APM rollout

The public [PIR APM dashboard](https://enhance-pir.valargroup.dev/apm/) now
has Enhance and Status panes above one deployment topology. Status is the
synthetic, isolated service. Its coordinator, router, and worker are one process
on `status-pir-p4000-ams1`; the connection to the production Enhance
coordinator is labelled planned. No wallet application was changed.
The [deployment check](../evidence/status-backend-2026-09-25/README.md) records
the build hashes, probe results, public page checks, and outage exercise.

## Deployed components

- Status service: `/opt/status-pir/status-pir` on
  `status-pir-p4000-ams1`, with monitoring on `127.0.0.1:8383`.
- Private monitoring tunnel: `status-pir-apm-tunnel.service` on the Enhance
  coordinator, forwarding `127.0.0.1:8384` to Status monitoring port 8383.
- APM sidecar: `/opt/enhance-pir/releases/apm-status-d07ab140/pir-apm` on
  `enhance-pir-coordinator-01`, selected by
  `/etc/systemd/system/pir-apm.service.d/zzzz-status-apm-20260925.conf`.
- Status URL, host label, and page title in `/etc/default/pir-apm`. The
  original environment file is saved as
  `/etc/default/pir-apm.before-status-apm-20260925`.

The tunnel key is in ValarGroup Infisical project `spendability-pir deploy`,
production environment, `/status-pir/apm`, key
`STATUS_APM_TUNNEL_SSH_PRIVATE_KEY`. The APM host keeps a root-only deployed
copy under `/etc/status-pir-apm/`; the Status host authorizes that key only
for forwarding to its loopback monitoring port. The host key is pinned in
`/etc/status-pir-apm/known_hosts`. No secret value is in this repository.

## Checks

```sh
cargo test --locked -p pir-apm
cargo test --locked -p enhance-pir-server --lib status::telemetry::tests
curl -fsS 'https://enhance-pir.valargroup.dev/apm/?pane=enhance'
curl -fsS 'https://enhance-pir.valargroup.dev/apm/?pane=status'
ssh root@167.99.42.60 'systemctl is-active pir-apm status-pir-apm-tunnel'
ssh status-pir-p4000-ams1 'systemctl is-active status-pir'
```

The Status host can run the encrypted backend probe from the
[Status runbook](status_backend.md). The private APM endpoint should report one
coordinator Query arrival per probe, with separate router and worker stage
arrivals. Public HTML should include one deployment topology on either pane.

Status request histograms and bytes are measured at the three Status listeners.
The Status pane uses one-hour in-memory history and five-minute histogram
windows. Restarts reset Status counters and the APM sidecar's chart history.
The host resource card describes the colocated process and GPU once.

The Status fixture reaffirms an unchanged synthetic source. Its observed age
is not proof of live block publication. The service's five-second publication
gate remains unmet; see the [original GPU result](../evidence/status-backend-2026-09-25/README.md).

## Rollback

Remove `zzzz-status-apm-20260925.conf`, restore the saved APM environment
file, reload systemd, and restart `pir-apm`. The previous binary remains at
`/opt/enhance-pir/releases/apm-3685438/pir-apm`. Stop and disable
`status-pir-apm-tunnel.service` if Status monitoring is no longer needed.
The previous Status binary is retained on the host for rollback.
