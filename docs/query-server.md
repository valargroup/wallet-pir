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
```

The existing product commands (`enhance-pir-server` worker, query ingress, and
packing router; `status-pir`; `transparent-query-server`;
`transparent-txid-server`) call the same startup functions.

## Configuration

The [example](../tools/deploy/query-server/query-server.example.json) is a typed
configuration, not a production inventory. Sections are `enhance`, `status`,
`transparent_history`, and `transaction_details`. Each has `enabled` and
`settings`. An omitted or disabled section starts nothing.

Enhance settings name one role: `worker`, `query-ingress`, or `packing-router`.
Status settings name `worker` or `router`. Those roles stay separate processes.
History and transaction details keep their existing settings.

Listeners need distinct fixed loopback or private addresses. State directories
are absolute and must not overlap. `--check` validates the file and does not
bind sockets or open stores.

## Readiness

Management `/health` stays up while a product is failing. `/ready` answers 503
until every enabled product returns success from its existing probe: Enhance
`GET /internal/health`, Status `GET /control/health`, and Transparent
`GET /v1/ready`.
