# Enhance PIR deployment

The current production server release is the [September 24 cleanup deployment](../evidence/production-cleanup-2026-09-24/README.md), built from `71be21f` (PR #111). All five server roles use the same verified binary; existing v7 state, runtime arguments, and the APM sidecar were preserved. The record includes exact-answer checks and guarded rollback instructions.

The initial v7 SSH rollout used `/opt/enhance-pir-v7/releases/2a83c21` and fresh state
under `/srv/enhance-pir-v7`. The [dated evidence](../evidence/immutable-v7-2026-09-24/README.md)
records the final deployment status, checksums, tests and capacity limitations.
The previous release and `/srv/enhance-pir-v6` data remain available for rollback.

The subsequent [cache-reclaim update](../evidence/worker-cache-reclaim-2026-09-24/README.md)
deployed source `ac7cf9d` in `/opt/enhance-pir-v7/releases/ac7cf9d` on the same
three hosts. Its focused workload was stopped early at the operator's request;
the memory observation is promising but not a completed qualification.

The binaries are `enhance-pir-server`, `enhance-pir-cli` and
`enhance-pir-load-test`. Server subcommands are `coordinator`, `worker`, `exercise`
and `repair-rows`. Protocol v7 retains schema-11 records and q48, but changes
routing, session identity and framing. v6 clients and serving state are incompatible.
Use fresh controller, worker, hint and cache directories. A schema-11 canonical
journal may be copied while stopped and validated; older-width journals must be
re-ingested. Never open v6 controller or worker state with the v7 binary.

## Build and local validation

```sh
cargo build --locked --release -p enhance-pir-server -p enhance-pir -p enhance-pir-load-test --bins --features enhance-pir/cli
python3 enhance/ops/scripts/test-local.py --help
python3 enhance/tools/schema11-interop/run.py /absolute/path/to/pinned/wallet-libraries
```

The wallet counterpart must implement the v7/q48 contract described in
[integration](integration.md). Use the
checksummed `enhance-pir` release artifact; its `candidate.json` records the
source revision and declares qualification `unqualified`. A green build or
bundle verification does not grant hardware qualification.

## Fresh deployment layout

The coordinator normally uses `--zakura-cookie PATH`. For an explicitly selected
canonical node with authentication disabled, use `--zakura-no-auth` instead.
It sends no Authorization header and cannot be combined with a cookie or
`--isolated-fixture`. This option also supplies the RPC connection to an enabled
Status publisher. Verify the node's network and canonical chain independently;
the option does not authenticate an HTTP endpoint or change wallet anchor checks.

Install the same checked binary revision on the coordinator and worker hosts.
Use new `/srv/enhance-pir-v7/canonical` and `/srv/enhance-pir-v7/worker` directories,
with an inventory at `/etc/enhance-pir/workers.json`. The inventory contains
ordered groups, each with a name and two replicas carrying `name` and private
`url`; [workers.example.json](../ops/deploy/workers.example.json) shows the shape.
Keep worker port 8091 on the private network and expose only the coordinator's
client routes through the public origin. Network isolation is the primary
control; the optional shared token below is an additional layer, not a
replacement.

### Internal control token

Setting `ENHANCE_INTERNAL_TOKEN` in the environment of every role enables a
shared bearer token on the private APIs. Worker routes, packing-router control
routes and query-ingress control routes (all `/internal/*` paths except the
ingress `/internal/metrics` scrape) then reject requests without
`Authorization: Bearer <token>` with HTTP 401. The coordinator, packing router
and `exercise` clients read the same variable and attach the header. Set it on
all roles together, or on none: a worker with the token and a coordinator
without it stops publication. Unset keeps the previous behaviour. Provide the
value through the unit's environment file, never on the command line.

### Query ingress uploads

The ingress (`query-ingress`, loopback 8082) now buffers each complete request
body (at most 512 KiB, 30-second deadline) before it takes one of its
`--requests` forwarding permits (default 16) and forwards the buffered body to a
packing router. Buffering is bounded by `--uploads` (default 256 concurrent
bodies) and by four concurrent uploads per client, identified by the first
`X-Forwarded-For` address, else `X-Real-IP`, else the socket peer. Either limit
answers HTTP 429 `overloaded` without charging a forwarding permit. Caddy must
keep sending `X-Forwarded-For` so the per-client cap does not collapse onto the
proxy's loopback address.

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

## Direct SSH rollout and rollback

The September 24 rollout builds with Rust 1.91, locked dependencies,
`RUSTFLAGS="-Dwarnings -C target-cpu=x86-64-v3"` and `CFLAGS/CXXFLAGS=-mpclmul`.
One checked server binary is copied to all hosts. CI waiting is intentionally
skipped; native tests and the external wallet harness run directly on Linux.

Existing systemd units are retained. `90-v7.conf` overrides `ExecStart` on
`enhance-pir-coordinator.service` and `enhance-pir-worker.service`; the worker
override also grants write access to the new worker directory. Old unit files,
drop-ins and binary checksums are saved under `/srv/enhance-pir-v7/rollback` on
each host. The coordinator listens on loopback port 8080 and Caddy keeps the
existing public origin. Workers listen on their private addresses at port 8091.

To roll back this layout, stop the coordinator, stop both workers, move only the
`90-v7.conf` overrides out of their `.service.d` directories, reload systemd,
start both preserved v6 workers and then the v6 coordinator. Verify the public
v6 manifest and exact-answer oracle. Restore matching v6 clients if any are in
use. Do not delete or reinterpret v7 state. To return to v7, stop the v6 services,
restore the v7 overrides, reload, start both v7 workers and then the coordinator.

The isolated workload uses separate `qualification-worker` directories and an
`enhance-v7-qualification` unit on loopback port 8280. Stop it before changing
worker state back to canonical serving. Preserve its samples, including memory
pressure and failures. A focused 30-minute run never grants the six-hour
hardware qualification or permission to increase placement/admission limits.
