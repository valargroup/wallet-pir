# Distributed live Status qualification

Public Status remains disabled. This implementation adds a live publication path
and independent serving roles; it does not establish production qualification.

## Deployment

The coordinator integration is `enhance-pir-server coordinator --status-config
<file>`. Omission disables Status. Its Status controller runs on a dedicated,
bounded Tokio runtime with its own journal and source cache. It uses the
coordinator-local Zakura cookie; the Status host never receives that credential.

The initial SSH rollout runs the same controller through `status-pir
serve-distributed` in `status-controller-qualification.service` on the coordinator
host. This isolates qualification from the existing Enhance process. Switching
the deployed Enhance executable to the integrated entry point remains a separate
cutover gate: the deployed native Enhance binary has an older upstream revision,
so its serving compatibility must be checked before replacement.

When a controller config sets `public_enabled`, `serve-distributed` requires
`--public-listen <loopback addr>` and serves the public `/v1/status/init`,
`/v1/status/session/:id` and `/v1/status/query` routes only there; the private
listener keeps telemetry and candidate transfer. `public_enabled` without
`--public-listen`, or the flag without `public_enabled`, is refused. Public
admission still depends on the publisher's live-publication health flag, so an
unpublished or fenced controller answers 503 on the public listener. The
forwarded query route also caps each client (first `X-Forwarded-For` address,
else `X-Real-IP`) at two concurrent requests and answers 429 beyond that. The
intended HTTPS ingress wiring is:

```
status-pir serve-distributed --config /etc/status-pir-control/controller.json \
  --cookie /root/.cache/zakura/.cookie --public-listen 127.0.0.1:8489
```

```
handle /v1/status/* {
    reverse_proxy 127.0.0.1:8489
}
```

Enabling `public_enabled` is a deployment decision gated by
`status_qualification.md`; this section documents the wiring, not a live
configuration.

| Endpoint | Owner | Reachability |
| --- | --- | --- |
| Coordinator `127.0.0.1:8480` | Private Status routes and immutable candidate transfer | Coordinator loopback |
| Coordinator `127.0.0.1:8489` | Public `/v1/status/*` when `public_enabled` (`--public-listen`) | Coordinator loopback; Caddy `handle /v1/status/*` |
| Status host `127.0.0.1:8481` | CPU evaluation worker and private control | Loopback; coordinator SSH forward |
| Status host `127.0.0.1:8482` | CPU packing router private control, public material, telemetry | Loopback; coordinator SSH forward (`status-control-tunnel`, local 8482) |
| Status host `127.0.0.1:8484` | CPU packing router query route only (`/v1/status/query`, `--query-listen`) | Loopback; coordinator SSH forward (`status-query-tunnel`, local 8492) |
| Status host `127.0.0.1:8495` | Reverse forward to coordinator artifacts | Status host loopback |

The router's `--query-listen` splits the forwarded query route from its control
listener. Both listeners must be loopback; the control listener never carries
`/v1/status/query`, and the query listener carries nothing else. Without
`--query-listen` the router serves one merged loopback listener as before. The
controller's `query_router_origin` (`http://127.0.0.1:8492`) reaches the query
listener through `status-query-tunnel.service`; `router_origin` keeps the
control path on 8482.

`status-control-tunnel.service` uses a dedicated forwarding-only account, pinned
host key, and explicit permitted destinations/listen address. Its service key is
stored in the production `/status-pir/control` folder of the `spendability-pir
deploy` Infisical project as `STATUS_CONTROL_SSH_PRIVATE_KEY`. The installed copy
is root-only under `/etc/status-pir-control/`. It is separate from the APM key.
The service-unit templates are under `enhance/ops/deploy/` and substitute a
checksummed release directory (`@RELEASE@`), the canonical network genesis hash
(`@NETWORK@`) and, for the two tunnels, the Status host's private VPC address
(`@STATUS_HOST@`).

The Status host is the Terraform `status-pir-01` droplet (`status.tf`, enabled by
`status_host_count`). Worker and router run CPU-only, as the dedicated
`status-pir` user, from a binary built without the `cuda` feature. Its cloud-init
installs the forwarding-only `status-control` key with explicit `permitopen`
(8481, 8482, 8484) and `permitlisten` (8495) restrictions. The CUDA worker
remains an optional build (`--features cuda`, `--cuda`) and is not deployed.

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
starts real worker/router child processes and verifies encrypted fixture answers
and remote fencing. It does not use a live source.

`status-pir probe-live-load --origin http://127.0.0.1:8480 --cookie
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
