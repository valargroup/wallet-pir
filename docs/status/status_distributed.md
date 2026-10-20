# Distributed live Status qualification

Public Status remains disabled. This implementation adds a live publication path
and independent serving roles; it does not establish production qualification.

## Deployment

Status publication runs in `pir-publisher --config FILE`, with an enabled
`status` section. Its controller uses a dedicated bounded runtime and its own
journal and source cache. The publisher reads the coordinator-local Zakura
cookie; query workers and routers never receive that credential.

The standalone production controller command has been removed. Follow the
[unified publisher cutover](../publisher.md#expand-the-coordinator-to-enhance-and-status)
to adopt an existing standalone controller. Rollback uses its captured old
executable and unit; current source does not rebuild that historical entrypoint.

The publisher never faces wallets. Its private listener serves candidate
transfer (`/artifact/:id`), `/control/active` (the active manifest, 503 until a
healthy, unfenced and fresh publication exists) and telemetry. Worker and router
run as `pir-query-server` roles, each from an operator-installed
`/etc/status-pir/<role>.json` (`tools/status/deploy/status-worker.example.json`
and `status-router.example.json` show them). The router's `query_listen` carries
the wallet routes `/v1/status/init`, `/v1/status/session/:id` and
`/v1/status/query` and nothing else; its control listener never carries them.
The router answers 503 until the controller has fenced and activated it, and
stops serving when the controller's heartbeat lapses. The query route admits
four executing and 32 waiting requests per router and caps each client (first
`X-Forwarded-For` address, else `X-Real-IP`) at two concurrent requests,
answering 429 beyond that.

The intended HTTPS ingress sends `/v1/status/*` to the router's query listener,
which the coordinator reaches through the `status-query-tunnel` forward
(local 8492):

```
handle /v1/status/* {
    reverse_proxy 127.0.0.1:8492
}
```

Enabling that route is a deployment decision gated by `status_qualification.md`;
this section documents the wiring, not a live configuration.

| Endpoint | Owner | Reachability |
| --- | --- | --- |
| Coordinator `127.0.0.1:8480` | Publisher: candidate transfer, `/control/active`, telemetry | Coordinator loopback |
| Status host `127.0.0.1:8481` | CPU evaluation worker and private control | Loopback; coordinator SSH forward |
| Status host `127.0.0.1:8482` | CPU packing router private control and telemetry | Loopback; coordinator SSH forward (`status-control-tunnel`, local 8482) |
| Status host `127.0.0.1:8484` | CPU packing router wallet routes (`/v1/status/*`, `query_listen`) | Loopback; coordinator SSH forward (`status-query-tunnel`, local 8492) |
| Status host `127.0.0.1:8495` | Reverse forward to coordinator artifacts | Status host loopback |
| Status host `127.0.0.1:18481`, `18482` | Each role process's own `/health` and `/ready` (`management_listen`) | Status host loopback |

Both router listeners must be loopback. The controller's `router_origin` keeps
the control path on 8482.

`status-control-tunnel.service` uses a dedicated forwarding-only account, pinned
host key, and explicit permitted destinations/listen address. Its service key is
stored in the production `/status-pir/control` folder of the `spendability-pir
deploy` Infisical project as `STATUS_CONTROL_SSH_PRIVATE_KEY`. The installed copy
is root-only under `/etc/status-pir-control/`. It is separate from the APM key.
The service-unit templates are under `tools/status/deploy/` and substitute a
checksummed release directory (`@RELEASE@`) and, for the two tunnels, the Status
host's private VPC address (`@STATUS_HOST@`). The canonical network genesis hash
is in each role's config file.

The Status host is the Terraform `status-pir-01` droplet (`status.tf`, enabled by
`status_host_count`). Worker and router run CPU-only, as the dedicated
`status-pir` user, from a binary built without the `cuda` feature. Its cloud-init
installs the forwarding-only `status-control` key with explicit `permitopen`
(8481, 8482, 8484) and `permitlisten` (8495) restrictions. The CUDA worker
remains an optional build (`--features cuda`, `"cuda": true`) and is not deployed.

### Supervised control sessions

`wallet-pir-control-sessions.service` replaces `status-control-tunnel` and
`status-query-tunnel`. It is
[the shared control-session supervisor](../control-sessions.md). The
Status baselines retained on 2026-10-09 and 2026-10-10 show the controller
ordered after it, with a drop-in stating that the tunnels run under it since
2026-09-30; that is dated evidence, not a current check. No record shows the
restricted `wallet-pir-control` account or the fault drill of
[rollout step 1](../control-sessions.md#rollout-order), so treat those
as not adopted. `tools/status/deploy/control-sessions.example.json`
carries the two tunnels' forwards unchanged: 8481, 8482 and 8495 on one master
with compression, and 8492 to 8484 on another. It keeps the same
forwarding-only account, key and known-hosts file, and `ServerAliveCountMax 4`.
The tests compare it with both unit templates.

The swap changes only the transport:

- a supervised master per tunnel, with bounded restart backoff and a status
  file, instead of systemd restarting bare `ssh`;
- a known-hosts digest pin;
- no system SSH configuration.

Before the swap, add `AllowStreamLocalForwarding no` to the host's
`status-control` Match block: the current key line does not stop the key from
creating remote Unix-socket listeners. The rollout and rollback steps are in
[the control-session guide](../control-sessions.md#rollout-order).

### Deploy CLI

`tools/scripts/wallet-pir-deploy.py` can deploy Status as the `status` service.
Read-only runs against production on 2026-09-30 found the live worker and router
units identical to their templates; it has not yet restarted a Status role, so
the manual rollout remains the procedure and the fallback. Commands, inventory, locking, journal
and rollback are described in
[the Enhance deployment guide](../enhance/deployment.md#deploy-cli).

For the Status worker and router the tool renders the whole unit from its
`.service.in` template, with `@RELEASE@` set to `/opt/status-pir/releases/<sha256>`
of the release's `pir-query-server`. The units read the operator-installed role
configs; the tool neither writes nor checks them. Roles and controller run
different binaries, so they deploy as separate transactions: worker and router
first, then the coordinator's controller, whose `status-pir` binary alone is
swapped. The tunnels are not managed.
[services.toml](../../tools/status/deploy/services.toml) records each role's `/control/health`
or `/internal/health` readiness check. The `status-pir` release kind in
`tools/ci/release.py` bundles `status-pir` and `pir-query-server` (built with
`status`) with these templates and example configs. It is assembled from the
`native-reinspiring` target directory; full CI uploads it as the
`status-pir-<sha>` artifact.

Cutting over from the earlier `status-pir role` units needs, before the first
role deploy, each role's `/etc/status-pir/<role>.json` installed from the
examples with the live network hash, and the controller's config without the
removed `public_enabled` and `query_router_origin` fields.

The tool compares each rendered unit's effective configuration with the live
one and refuses any difference beyond the binary unless `--allow-unit-drift` is
given. On 2026-09-30 the live worker and router units matched their templates
(`MemoryMax=12G` and 3G, `STATUS_PREPARATION_THREADS=6`), and a preflight with
the running binary planned no restart. Publication is managed by the unified publisher deployment.

## Publication and recovery

Control binds network, recovery epoch, generation, process incarnation and
material digests. Roles start unready. The coordinator persists its epoch,
obtains role fence acknowledgments, prepares worker and router material, persists
the serving decision, activates both roles, then advertises the manifest. A
failed activation causes a new epoch before further publication.

The worker owns database units and hint construction; the router owns packing
material and public material. Requests pin immutable generations. Active and
previous sessions are bounded; an older session returns 409, while an explicit
recovery fence revokes already-pinned controllers and returns 410 where an old
identity remains known. Unready roles return 503. The wallet permits one bounded
reinitialization retry with fresh encryption; it never switches to plaintext
lookup or payload retrieval.

Five-second control watchdogs stop admission without changing source timestamps.
The coordinator checks canonical anchors before publication and during control
heartbeats. Reorg or heartbeat failure removes local admission. Restart never
uses cached material as serving authority. Durable files use exclusive locks,
fsync and atomic rename. Ambiguous persistence failure poisons the role.

Live observation and preparation run independently with a latest-value pending
snapshot. Complete identical observations may reaffirm existing material after
five seconds. Changed snapshots retain their original observation timestamp;
preparation cannot extend freshness. Superseded mempool snapshots must remain
visible in qualification evidence, as specified in `status_qualification.md`.

## Preparation and transfer

- Private transfers use either canonical full rows or ordered changed-row
  records bound to a base digest. The receiver bounds length and indices and
  verifies the reconstructed full digest before preparing material.
- At most 4,096 changed plaintext coefficients per polynomial unit use the
  linear update `H' = H + A * (D' - D)` in `Z_q[X]/(X^d+1)`. Wraparound changes
  sign. Larger updates use upstream full NTT reconstruction. Candidate GPU data
  is separately allocated; active data is never modified.
- Sparse arithmetic is checked against full reconstruction, including positive
  and negative deltas at first/last row and coefficient boundaries.
- Failed preparation may retain one calculation cache. Its content digest can
  seed a subsequent delta, but it cannot authorize queries or restore readiness.
- Router preparation reuses immutable upstream public key images and exact
  precomputation for unchanged, content-hashed hint blocks. Public PIR
  geometry, q48 arithmetic, setup domain and encrypted response format remain
  unchanged.

These optimizations require joint hardware measurement. Passing a sparse update
does not qualify dense block bursts or the 75% occupancy ceiling.

## Compatibility and qualification

The server and wallet use `status-pir-v2-q48`, a 20,000-ms maximum age, and
ipir-sp rc.6. A `native-reinspiring` build serves the incompatible
`status-pir-v3-native-two-mask-m29` profile instead, which the wallet library
does not implement. Wallet-library commit
`e153ad393ac4bfb12ac1e06a57a31f631fb9ce3e` carries the matching contract. Vizor's
wallet-library pins must move together to avoid distinct Rust types from two
copies of the Status crate. Its release-ready feature gate remains disabled.

`status-pir validate-distributed --entries 1572864 --state-dir <fresh-dir>` (add `--cuda` on a GPU host)
starts real `pir-query-server` worker/router child processes (`--query-server`
names the binary; by default the one beside `status-pir`) and verifies encrypted
fixture answers and remote fencing. It does not use a live source.

`status-pir probe-live-load --origin http://127.0.0.1:8492 --cookie
/root/.cache/zakura/.cookie --seconds 60 --report-dir <new-dir>` offers 20 complete
lookups per second, including initialization and bounded session retries. It
checks mined answers against canonical node blocks and writes arrival evidence.
This is a live mined-answer smoke campaign, not a complete publication oracle or
six-hour qualification. It intentionally reports `production_qualified: false`.

Production also requires a complete six-hour publication/resource capture,
explicit supersession evidence, independent block/mempool publication checks,
the qualified update envelope, fault recovery tests, protocol review, and a
restricted HTTPS ingress rehearsal. Those gates must pass before public routes
or `VIZOR_STATUS_PIR_RELEASE_READY` are enabled.

Rollback stops private Status admission and preserves its latest durable
journals. Never restore an older authority directory to match an older binary.

## Compact v2 qualification boundary

The `status-pir-v2-q48` contract uses 40-byte slots, 12,288-byte padded rows,
6,144 u16 columns and a 96 MiB database. Setup, bucket and manifest domains
use `status-pir/v2/`; request envelopes use `SPQ2`. The HTTP route prefix
remains `/v1/status/`, but v1 manifests and material are incompatible.
The 1,572,864-entry admission ceiling and 20-second freshness limit are unchanged.
Block hashes remain internal source/publication metadata, not wallet-visible
inclusion evidence. All retained v1 timing/resource captures are historical and
protocol-incompatible; they cannot qualify v2.


### Installing CPU query-role configurations

The Status worker and router deployment roles render their configuration files
with their units. Supply each role's `settings` in the private fleet inventory:
`network_hex`, `state_dir`, `listen`, `artifact_origin`, and `worker_origin`;
the router may also supply `query_listen`. Retain the current network identity,
loopback peers and durable state paths. The deployment stages each configuration,
runs the new executable's `--check` before restarting, and restores the previous
configuration (or its absence) on rollback. The examples beside the unit templates
show the resulting files; do not install their placeholder network hash.

For aggregate live-load acceptance against the router's loopback wallet listener,
run `tools/status/lib/live_probe.py` with the installed `status-pir` diagnostic
client, node cookie reference, router origin and a fresh report directory. It
runs the unchanged encrypted probe at twenty lookups per second across eight
simulated client addresses. The router allows two concurrent queries per client;
a single client's overload test is a separate check. The wrapper preserves
failures without retrying and records its transport and client count beside the
probe's raw arrival records. It refuses public upstream origins and never
changes the server's admission policy.

### Cutting over the existing wallet edge

The query router and its public route must move together. The retired publisher
wallet gateway does not preserve forwarded client addresses, so leaving it in
front of the updated router collapses unrelated clients into one concurrency
limit. Keep the shared publisher running and send the existing `/v1/status/*`
edge route directly to the router's loopback query tunnel at `127.0.0.1:8492`.

Use `wallet-pir-deploy.py status-routing-plan`, `status-routing-preflight` and
`status-routing-deploy`, with the private `--request` and `--request-sha256`.
The request schema is `status-wallet-route/1`, with `host`, `machine_id`,
`router_host`, `router_sha256` and the existing HTTPS `public_origin`. The private
Status inventory also supplies `route_probe_client`, the checksummed diagnostic
client at `/opt/status-pir/releases/<sha256>/status-pir`, and its independent
`exact_check`. Deployment requires the reviewed `--expect-plan-sha256`.

Plan can run before the new router starts. Preflight and deployment require its
exact executable and wallet listener to be active. The operation validates Caddy,
changes only the Status upstream in the installed file, reloads rather than
restarts Caddy, and checks encrypted answers through the existing public TLS
origin and the aggregate private load. It preserves every other route and TLS
setting. An interrupted or failed operation is recovered with
`status-routing-rollback`; `status-routing-status` reports its journal and
current configuration digest. For a complete service rollback, restore routing
first and then restore the router executable through its retained transaction.
