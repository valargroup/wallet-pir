# Enhance PIR deployment

The recorded production fleet serves v6/q48 from commit `afdb4b6` on the
unversioned coordinator and worker units. Its state is under
`/srv/enhance-pir-v6`; see the [cutover evidence](../evidence/protocol-v6-production-2026-09-23/README.md).
The later `ipir-sp` rc.2 dependency pin on `main` has not been deployed by that
cutover.

The release binary is `enhance-pir-server`. It has `coordinator`, `worker`,
`exercise`, and `repair-rows` subcommands. The CLI and exact-answer load driver
are `enhance-pir-cli` and `enhance-pir-load-test`. Schema 11 uses fresh canonical,
controller, worker, hint, and cache state. Protocol v6 changes query precision
to q48 while retaining schema-11 records; v5/q46 state and clients are
incompatible. Do not open a previous release's data
directory or try to repack an old journal: older records lack the suffix format.
Existing production services and their data stay in place until an operator
coordinates a separate cutover.

## Build and local validation

```sh
cargo build --locked --release -p enhance-pir-server -p enhance-pir -p enhance-pir-load-test --bins --features enhance-pir/cli
python3 enhance/ops/scripts/test-local.py --help
python3 enhance/tools/schema11-interop/run.py /absolute/path/to/pinned/wallet-libraries
```

The wallet counterpart must implement the v6/q48 follow-up described in
[integration](integration.md). Use the
checksummed `enhance-pir` release artifact; its `candidate.json` records the
source revision and declares qualification `unqualified`. A green build or
bundle verification does not grant hardware qualification.

## Fresh deployment layout

Install the same checked binary revision on the coordinator and worker hosts.
Use new `/srv/enhance-pir/canonical` and `/srv/enhance-pir/worker` directories,
with an inventory at `/etc/enhance-pir/workers.json`. The inventory contains
ordered groups, each with a name and two replicas carrying `name` and private
`url`; [workers.example.json](../ops/deploy/workers.example.json) shows the shape.
Keep worker port 8091 on the private network and expose only the coordinator's
client routes through the public origin. The worker API has no application-layer
authentication.

The coordinator admits four active queries and lets up to 16 more wait for two
seconds. A full or expired queue returns HTTP 429 with `Retry-After: 1`; a busy
replica is retried on its peer. The public Caddy query route disables upstream
keepalive so overload responses are not lost on stale local connections. Check
the `enhance_query_*` metrics and public 429/502 counts after rollout.

The service examples are [coordinator](../ops/deploy/enhance-pir-server.service)
and [worker](../ops/deploy/enhance-pir-worker.service). Point
`/opt/enhance-pir/current` to the verified release on each host, create the
non-root worker account and its private data directory, and provide the worker
listen address and sealed-shard policy in `/etc/enhance-pir/worker.env`.
The default policy is six sealed shards; seven requires separate hardware
qualification. Keep existing Terraform resource addresses, droplet names, and
tags when reconciling infrastructure.

## Cutover checks

1. Confirm the wallet fleet accepts schema 11 and the recorded protocol revision.
   Preserve the old release and data for rollback from its pinned checkout.
2. Prepare fresh state from canonical RPC and start both workers before the
   coordinator. Verify their private health and matching placement policy.
3. Check the public manifest's schema, anchor, coverage, and two-replica
   publication. Run exact-answer queries against an independent record oracle,
   including a row boundary, a retained session, and a refreshed expired session.
4. Check worker memory, disk, swap, publication time, and query latency under
   the target placement. Use [qualification](qualification.md) for the remaining
   sustained acceptance evidence.
5. Switch traffic together with compatible wallets. Rollback switches traffic
   and clients to the preserved old release and data; it does not reuse new state.

The physical hosts have recorded `v4` resource names in Terraform. Those names
are live identities and are deliberately left unchanged. The direct SSH
cutover did not run Terraform.
