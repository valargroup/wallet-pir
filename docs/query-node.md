# Unified query node

The opt-in query node adds a separate candidate endpoint at
`wallet-pir-query-01.valargroup.dev`. It runs the root query binary with Enhance,
Status, Transparent history and transaction details together. It downloads
publisher-prepared material over HTTPS; it does not index the chain or publish
new data. The existing public endpoints keep their current routing.

Receiver is not implemented in the root query binary. Its current external role
needs a staged publication and node RPC access. This node therefore cannot yet
satisfy an all-products deployment: Receiver requests receive an explicit
unavailable response. Do not declare all-product acceptance or route the existing
wallet endpoint here until Receiver has its own HTTPS consumer integration.

## Capacity and complete holdings

The initial node has 24 dedicated vCPUs, 192 GiB RAM and a separate 1 TiB XFS
volume. The service has `MemoryHigh=144G`, `MemoryMax=160G` and no swap allowance,
leaving 32 GiB outside its hard limit for the OS, TLS proxy and operating tools.
These are limits, not evidence that the current full datasets fit.

The replacement node `wallet-pir-query-02` has 16 dedicated vCPUs, 128 GiB RAM and a
512 GiB volume; Amsterdam offers no 16 vCPU size with 96 GiB. Its inventory
sets `limits` of `MemoryHigh=104G` and `MemoryMax=112G`, and cache budgets of
48 GiB for history, 20 GiB for detail and 8 GiB each for Enhance and
Receiver, so the caches together stay below the memory limit. On 2026-10-25 the complete active sets were 39.1 GiB of history and
15.0 GiB of detail tables, growing by about 3.9 and 2.3 GiB a year. The
first node measured 57.7 GiB resident at steady state, about half of it
memory-mapped preprocessing that the kernel can reclaim under pressure.
The unit leaves Rayon's thread count to the host's CPU count.

The [inventory settings example](../tools/deploy/query-server/query-node.example.json)
carries the settings installed on `wallet-pir-query-01` on 2026-10-24. All four
products synchronize from the coordinator publisher, `https://enhance-pir.valargroup.dev`,
which serves discovery and the `/provider/{enhance/v1,status/v1,v1,txid/v1}`
feeds. It reserves 8 GiB for Enhance's prepared cache, 96 GiB for history and
24 GiB for detail, with one history build slot and one history query slot. At
activation the live publications were one Enhance shard (0) of about 0.9 M
records, about 63.5 GiB of history and 18.0 GiB of detail artifacts; the previous
64 GiB and 16 GiB budgets could not hold them. A cold start reached readiness in
about 21 minutes with a 92 GiB service memory peak and 106 GiB used on the volume.
Status, queries, download buffers, retained revisions and publication replacement
overlap also consume memory. Measure their simultaneous peak before increasing
concurrency. An allocation failure must refuse work; never count swap as serving
capacity.

Recheck the publisher origins and network identities against the current
verified publications before reuse. List every Enhance shard in the current
publisher's domain (`coverage.shards` of `/enhance/v1/init`), and set
`sealed_shards` and cache/download budgets to hold that entire set; a combined
role holding a different set refuses the publication.
History uses `full_holdings=true` with no remote archive forwarding. Transaction
details keeps its complete selected publication locally. Status serves the
current publication; it does not acquire a new historical-status API.

Before deployment, compare current and replacement publication sizes, retained
revisions, prepared caches and temporary downloads against both RAM and disk.
Retain at least the deployment tool's 20 GiB free-space floor, and budget the
largest publication replacement separately. If complete holdings exceed a budget,
stop and report the missing capacity rather than reducing history silently.
Record steady-state and replacement peaks, exact wallet answers, refusal rates,
HTTPS transfer progress and shutdown completion on the candidate.

## Installing on an operator host

An operator, or an agent acting for one, installs the same four-product node on
their own Linux host with one command:

```sh
curl -fsSL https://wallet-pir.valargroup.dev/install | sudo bash
curl -fsSL https://wallet-pir.valargroup.dev/install | sudo bash -s -- --domain pir.example.org
```

`/install` on the unified site redirects to
[`tools/deploy/query-server/install.sh`](../tools/deploy/query-server/install.sh)
on `main`. There is one configuration: the settings in
[`install-node.json`](../tools/deploy/query-server/install-node.json), which are
this page's example with `wallet-pir-query-02`'s cache budgets and no Receiver.
The installer never prompts. It:

1. Refuses anything but Linux on an x86-64-v3 CPU with `pclmulqdq`, systemd and
   root, and a host already provisioned as a fleet query node (one with
   `wallet-pir-query-data.service`), which the deployment transaction owns.
2. Refuses the host unless it has **16 vCPUs, 92 GiB RAM and 500 GB** (10^9
   bytes) for `/srv/wallet-pir-query`, counting container CPU and memory limits
   instead of the host's totals. Every shortfall is listed together; there is
   no override.
3. Checks the publisher's discovery document and reads the current Enhance
   shard list from `/enhance/v1/init`, since a combined role must hold them all.
4. Downloads the pinned `query-server-v*` release archive and checks its
   SHA-256, which `install.sh` embeds. `--version TAG` installs another release
   and trusts that release's published `SHA256SUMS` over HTTPS only.
5. Writes `/etc/wallet-pir-query/query-server.json` with those shards, runs the
   binary's `--check`, installs `wallet-pir-query.service` with `MemoryMax` 12 GiB
   below the host's memory and `MemoryHigh` 8 GiB below that, and starts it.
6. With `--domain`, installs Caddy and serves the wallet paths over HTTPS. It
   refuses to overwrite a Caddyfile it did not write.

It waits for `/health`, not for readiness: a cold start downloads tens of GiB
(about 21 minutes on the first node). `--wait-ready` waits up to three hours.
`--check-only` runs steps 1 and 2 without root and changes nothing; `--dry-run`
prints every change instead of making it. The last stdout line is always
`wallet-pir-install: ok ...` or `wallet-pir-install: error <class>: <reason>`.
Exit codes: 2 usage, 3 hardware, 4 platform or prerequisite, 5 download or
verification, 6 configuration check or start.

Re-running upgrades the binary and rewrites the configuration. To remove it,
`systemctl disable --now wallet-pir-query`, delete the unit, `/opt/wallet-pir-query`,
`/etc/wallet-pir-query` and `/srv/wallet-pir-query`, and remove the Caddyfile
if the installer wrote it.

The 92 GiB floor is below the first node's measured 92 GiB cold-start service
peak, which used larger caches; a 92 GiB host is not yet qualified.

To release, push a `query-server-v<version>` tag. The
[release workflow](../.github/workflows/release-query-server.yml) builds the
CPU native binary, renders the unit and configuration from `install-node.json`
with this page's role renderer, checks them with the binary and publishes the
archive and `SHA256SUMS` as a GitHub release. Then pin its tag and archive
checksum in `install.sh`; until then the one-liner needs `--version`.

## Provisioning from a branch

The query module can be added to the installed Terraform root without moving
existing fleet addresses. Prefer this additive path when the installed root
still uses the flat layout. Under the production lock, copy its configuration,
templates, provider lock and operator variable files into a private staging
root, then add only `fleet/query-serving.tf` and its sibling `query-serving`
module. Initialize and save a fresh plan with the query role enabled. The plan
must contain exactly the six approved creations, no existing-resource actions,
no state moves and no drift. Reconcile unrelated drift separately; never apply
a broader fleet migration as part of this operation.

Use `wallet-pir-deploy.py query-node-plan` and `query-node-preflight` on the
coordinator with a private request and receipt path. The request binds the
coordinator machine ID, exact query-extension source SHA, staged root, saved plan
path and digest, Wallet PIR project ID and the installed configuration's file
hashes (`wallet_pir_ops.deploy.query_provision.inputs`), external production
variables digest and current desired worker-group count. Both commands return a
plan digest. Run `query-node-provision --expect-plan-sha256 <digest>` through the
coordinator's existing encrypted runtime credential loader. It rechecks the
source and inputs under the production lock, adds only the query module and its
enabled setting to the maintained root, initializes that root and applies the
pinned plan. The existing backend, variables, resource addresses and services
remain in place. `query-node-status` reads the retained receipt after a lost
connection. An interrupted apply retains source and reports `needs-reconcile`;
do not remove that source or retry creation blindly. Persistent storage has no
automatic destruction rollback.

The broader [service-module activation](../tools/infra/digitalocean/fleet/activation.md)
remains a separate operation with its own moves-only comparator. Its source and
interface gates do not need to be waived to create this candidate.

The fleet root includes `../query-serving` only when
`query_serving_enabled=true`. Defaults provision nothing. The module reuses the
existing Wallet PIR project, VPC, region and public SSH key IDs. Ensure Roman's
existing public key is in that key set; never copy a private key onto the node.
The persistent volume refuses Terraform destruction. Cloud-init mounts only the
named volume, checks its filesystem identity and leaves serving stopped if it
cannot mount it. Caddy obtains the candidate's HTTPS certificate and forwards
wallet paths to loopback; management and private APIs stay inaccessible publicly.

1. Pin the source branch revision and stage its complete Terraform module tree
   on the current coordinator. Discover the current inventory and trusted host
   pins first. Retain private inputs and receipts outside Git.
2. Run that revision's `tools/scripts/wallet-pir-terraform.sh --root
   /absolute/staged/tools/infra/digitalocean/fleet init -input=false` on the
   coordinator. The wrapper uses the existing encrypted runtime credential and
   production lock. It preserves the existing backend key and autoscaling count.
   Never initialize a separate local production state or overwrite remote state.
3. Save a plan through the same wrapper with `plan -input=false
   -var=query_serving_enabled=true -out=/absolute/private/query.tfplan`.
   Review the saved plan with `tools/scripts/wallet-pir-query-plan.py --root
   /absolute/staged/tools/infra/digitalocean/fleet --plan
   /absolute/private/query.tfplan --project-id <verified-wallet-pir-project-id>`.
   It prints only the digest and allowed creation addresses. Saved plans contain
   sensitive inputs and must remain private. The guard rejects drift, partial
   plans, replacements and changes to existing resources.
4. Use the query-node deployment transaction above to install the additive source
   and enabled setting and apply that exact saved plan under one production lock.
   An ephemeral staging tree alone is insufficient: the next autoscaling or
   regular fleet operation must know about the new node. If the guard refuses,
   stop; do not target resources to evade a fleet change. Retain the transaction
   receipt and saved plan privately.
5. Verify cloud-init completion, the node's actual RAM and CPU capacity, volume identity,
   mount, free space and TLS before staging the application. Obtain the new host's
   SSH pins through the trusted provisioning channel; never use unverified
   `ssh-keyscan` output as trust.

To add `wallet-pir-query-02` beside an installed first node, stage the
maintained root with the reviewed `query-serving.tf` and module, save the plan
with `-var=query_serving_02_enabled=true`, review it with
`wallet-pir-query-plan.py --node 02`, and set `"node": "02"` in the
`query-node-*` request. The transaction refuses a second node without the
installed module, replaces the module and declaration with the reviewed source,
keeps the previous module beside it for inspection and adds
`query-serving-02.auto.tfvars`. Its plan may create only the second node's six
resources; the first node and the rest of the fleet must stay unchanged.

The wrapper's default still operates the installed legacy Terraform root. Its
`--root` option allows a pinned staging tree while retaining the same credential,
lock and production variables. Provisioning never activates a query application.

## Application deployment

Dispatch the full-CI workflow with `query-candidate=true` on the pinned branch.
It runs full qualification and also builds a CPU native binary with
`enhance,status,transparent-history,transparent-detail,native-reinspiring`, renders
the node settings and runs the binary's offline configuration check. It packages
the query executable and operations release from the same revision. An artifact
and an offline check do not establish dataset fit or live correctness.

Add the candidate host and the optional `query-server` / `combined` role to the
maintained inventory, with the example's settings replaced by verified live
publication identities, complete holdings, capacities and trusted HTTPS CA paths.
Keep the coordinator as the lock owner. Use the new operations package's existing
`wallet-pir-deploy.py` commands for `plan`, `preflight --stage`, `deploy`, `status`
and `rollback`, selecting service `query-server`. Use the exact candidate archive
and source revision; inspect each subcommand's help for its inventory and artifact
arguments. Do not install units or copy executables to production by hand.

Configure `services.query-server.exact_check.products` with exactly four entries:
`enhance`, `status`, `transparent_history`, and `transaction_details`. Each entry
contains `host` (a known, non-retired inventory host), `argv` (the independent
wallet client's command), and `timeout` (1–900 seconds). Retain the existing
clients, fixtures and answer oracles, changing their wallet destination to the
candidate HTTPS origin. Keep publisher and oracle destinations unchanged.
Checks may run on different hosts; all their SSH identities are verified before
deployment changes begin. The transaction commits only after every check passes;
a failed check triggers rollback. Do not substitute readiness for decrypted
answer verification or skip these checks to accept the candidate.

The role renderer composes the product-owned renderers, checks listener collisions
and disjoint state below `/srv/wallet-pir-query`, mounts CA files read-only, and
returns `--config ... --check` for the actual binary before activation. The unit
requires the mounted volume. Its shutdown does not force-kill admitted work at a
timeout; a stalled drain must be reported by the deployment transaction.

For a quick encrypted smoke check without the deployment transaction, build the
wallet clients with the production protocol (`native-reinspiring`; without it
they refuse the live manifests) and compare against the existing endpoints:
`enhance-pir-cli --server <origin> query <position>`, the ignored
`txid_live_lookup` test with `TXID_LIVE_URL`, `frozen-provider-client <origin>
<script> <birthday>`, and the ignored Status `live_lookup` test of
`status-publisher` (`--features cli,native-reinspiring`) with `STATUS_LIVE_URL`,
`STATUS_LIVE_TXID` and `STATUS_LIVE_HEIGHT` taken from an independent block
source. Transparent compact filters (`/transparent/v1/filters/*`) are not
served by the query binary and answer 404 on this node.

After activation, retain transaction status and the exact rollback command.
Verify independent encrypted answers for every configured product and archive
range over the candidate HTTPS endpoint, then observe a replacement publication,
withdrawal/recovery and restart. Confirm private routes are refused externally.
Receiver remains an unmet all-products gate. Production endpoint cutover and
retirement of existing hosts require their own explicit rollout decision.
