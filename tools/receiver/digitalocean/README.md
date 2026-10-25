# Receiver PIR on DigitalOcean

Receiver uses the shared publisher and query hosts. The existing Receiver
Droplet remains an ingestion host with its current SQLite index and sealed
material. Query serving can join the existing unified query host or use the
Receiver-only root configuration. No infrastructure change is applied by these
examples.

## Infrastructure

The existing Terraform resource names, Droplet size, DNS identity, account and
installation root remain unchanged. The Receiver host has 8 GiB of memory; its
unit retains `MemoryHigh=6G`, `MemoryMax=7G` and no swap. Run query serving on an
appropriately budgeted existing host rather than duplicating the full publisher
and query budgets on that Droplet.

The Receiver edge exposes `/.well-known/pir-service-discovery` and
`/provider/receiver/v1/*` from the local root publisher. Wallet routes proxy to
`RECEIVER_QUERY_UPSTREAM`, set to the chosen existing query listener during the
separately approved cutover. Operator routes remain absent from the public edge.
Validate the rendered edge with that environment before deploying it.

## Release and deploy

The `receiver-pir` bundle contains `pir-publisher`, `pir-query-server` and
`receiver-probe`. The `receiver.server` role retains `receiver-pir.service` and
`/opt/receiver-pir`, and renders the root publisher configuration from inventory.
Its root chain section owns RPC settings. Use the existing
`/srv/receiver-pir/index` to resume ingestion without reindexing.

The optional `receiver.query` role renders a Receiver-only root query host;
alternatively enable Receiver in the root query-server role's combined settings.
Both query forms accept HTTPS publisher settings and local state only. They
return an offline `--check` command, with precise writable state and read-only
certificate paths. The publisher retains the optional audit environment file;
the query unit has none.

Use `tools/scripts/wallet-pir-deploy.py` for plan, preflight, install and rollback.
Root offline checks precede installation. Publisher readiness, query readiness
and the independent encrypted `receiver-probe` are separate observations. Keep
all changes draft until full qualification and separate production approval.

### Installing or rotating the NEAR key

Use `near-key.py` and Infisical-backed credentials as before. The publisher's
`near_key_id` publisher-role setting selects `/etc/receiver-pir/near-<id>.env`; the optional
file is read by the publisher only. Never put a partner key in query settings,
release archives, command arguments or repository files. Optional audit failure
does not grant or revoke wallet publication authority.

## Host provisioning

Existing cloud-init provisions the same system account and state directories.
New provisioning is outside this integration. The examples in `../deploy/`
show the root publisher and Receiver-only query configurations; the combined
example is under `tools/deploy/query-server/`.

## Monitoring

`receiver-probe --origin URL --rpc-url INDEPENDENT_NODE --no-auth` checks real
wallet retrieval and chain anchors. A node cookie may replace `--no-auth` on the
monitor. Query configuration itself contains no node credentials. The optional
private health check uses the query host management listener's
`/v1/receiver/health`; aggregate readiness uses `/ready` on that listener.

The historical [deployment evidence](../../../evidence/receiver/deployment-2026-10-08/README.md)
describes the former runtime and does not establish deployment of this stack.
