# Historical Status backend experiments

The single-process synthetic Status server and its GPU deployment are retired.
Status now uses CPU evaluation through `pir-query-server` worker and router roles;
publication and diagnostics run through `pir-publisher`.

Use the [distributed runbook](status_distributed.md) for current role configuration
and publication, and the [qualification gate](status_qualification.md) for release
validation. The [architecture](architecture_status.md) describes the serving
contract and links retained measurements. Historical v1/v2 and GPU measurements
do not qualify the native v3 CPU service.

The [September 25 backend experiment](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/enhance/status-backend-2026-09-25/README.md)
and [compact v2 measurements](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/enhance/status-compact-v2-2026-09-25/README.md)
remain immutable historical evidence.
