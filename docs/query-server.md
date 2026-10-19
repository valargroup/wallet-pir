# Query server host

`pir-query-server` is one process that serves any compiled subset of Enhance,
Status, Transparent history, and transaction details. The host crate is
`src/pir-query-server`. Its management server
reserves listeners and owns process shutdown. Product libraries continue to own
evaluation, admission, and readiness.

## Features

The host compiles a product only when that product's Cargo feature is enabled:
`enhance`, `status`, or `transparent`. `transparent` compiles both history and
transaction-detail serving. `native-reinspiring` and `cuda` select those
backends and do nothing unless the matching product feature is also enabled.
A configuration section for a product that was not compiled is rejected.

```sh
cargo build --locked --profile release-fast -p pir-query-server \
  --features enhance,status,transparent,native-reinspiring
pir-query-server --config /etc/wallet-pir/query-server.json --check
pir-query-server --config /etc/wallet-pir/query-server.json
pir-query-server check-backend [--matvec-backend cpu|cuda] [--cuda-device N]
pir-query-server repair-rows --data-dir DIR --source-rows DIR
```

A process includes at most one Enhance role. `enhance-pir-server` does not start a
worker, query ingress, or packing router. Those roles are only
`pir-query-server --config FILE`. `enhance-pir-server` keeps `coordinator`,
`rebuild-journal`, `prepare-packing-artifacts`, `restore-legacy-placement`, and
`build-info`. `build-info` reports native reinspiring and the source revision;
it has no CUDA field.

`check-backend` validates the selected evaluator, and for CUDA the driver,
without opening worker state. `repair-rows` repairs journal-referenced rows
from a peer row archive while the worker is stopped. `status-pir`,
`transparent-query-server`, and `transparent-txid-server` remain their own
commands.

## Configuration

The [example](../tools/deploy/query-server/query-server.example.json) is a typed
configuration, not a production inventory. Sections are `enhance`, `status`,
`transparent_history`, and `transaction_details`. Each has `enabled` and
`settings`. An omitted or disabled section starts nothing.

Enhance settings name one role: `worker`, `query-ingress`, or `packing-router`.
A worker needs `listen`, an absolute `data_dir`, and optional `sealed_shards`
(default 6) and `matvec` (`matvec_backend` `cpu` or `cuda`, and `cuda_device`
only for CUDA). Query ingress needs `data_dir`, `listen`, `control_listen`,
and optional `requests` (default 16) and `uploads` (default 256). A packing
router needs `data_dir`, `artifact_origin`, `listen`, `control_listen`, and
optional `max_objects` (default 6) and `requests` (default 4). Status settings
name `worker` or `router`. Those roles stay separate processes. History and
transaction details keep their existing settings.

Listeners need distinct fixed loopback or private addresses. State directories
are absolute and must not overlap. `--check` validates the file and does not
bind sockets or open stores.

## Readiness

Management `/health` stays up while a product is failing. `/ready` answers 503
until every enabled product returns success from its existing probe: Enhance
`GET /internal/health`, Status `GET /control/health`, and Transparent
`GET /v1/ready`.
