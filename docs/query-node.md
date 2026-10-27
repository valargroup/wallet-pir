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
56 GiB for history, 24 GiB for detail and 8 GiB each for Enhance and
Receiver, so the caches together stay below the memory limit. On 2026-10-25
the first node held 39.3 GiB of history and filled its 24 GiB detail budget;
history grows by about 3.9 GiB and detail by about 2.3 GiB a year. The
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
A first history download takes longer than the publisher keeps superseded
artifacts, so it is usually refused partway and retried. Each retry keeps the
verified objects already downloaded and fetches only the rest, so allow most of
an hour for a cold start on a 16 vCPU node; the deployment waits up to an hour.
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

An operator, or an agent acting for one, installs the five-product node (the four
above plus Receiver) on their own Linux host with one command:

```sh
curl -fsSL https://wallet-pir.valargroup.dev/install | sudo bash
```

`/install` on the unified site redirects to
[`tools/deploy/query-server/install.sh`](../tools/deploy/query-server/install.sh)
on `main`. There is one configuration: the settings in
[`install-node.json`](../tools/deploy/query-server/install-node.json), which are
this page's example with `wallet-pir-query-02`'s cache budgets. Receiver
synchronizes from its own publisher, `https://receiver-pir.valargroup.dev`.
The installer asks one question, for the public domain, and only when it has a
terminal to ask on. It:

1. Refuses anything but Linux on an x86-64-v3 CPU with `pclmulqdq`, systemd and
   root, and a host already provisioned as a fleet query node (one with
   `wallet-pir-query-data.service`), which the deployment transaction owns.
2. Refuses the host unless it has **16 vCPUs, 92 GiB RAM and 500 GB** (10^9
   bytes) for `/srv/wallet-pir-query`, counting container CPU and memory limits
   instead of the host's totals. Every shortfall is listed together; there is
   no override.
3. Checks both publishers' discovery documents and reads the current Enhance
   shard list from `/enhance/v1/init`, since a combined role must hold them all.
   Before that, it settles the domain wallets reach the node at. From a
   terminal it asks for one; an empty answer serves this machine only, on
   `127.0.0.1:8090`, and a re-run offers the domain it already serves. Without
   a terminal (cloud-init, CI, non-interactive `ssh`) it never waits: a re-run
   keeps its domain and a new host serves this machine only. `--domain NAME`
   and `--local` answer without asking. A domain that does not resolve yet
   draws a warning, since Caddy cannot get its certificate until it does.
4. Downloads the pinned `query-server-v*` release archive and checks its
   SHA-256, which `install.sh` embeds. `--version TAG` installs another release
   and trusts that release's published `SHA256SUMS` over HTTPS only.
5. Writes `/etc/wallet-pir-query/query-server.json` with those shards, runs the
   binary's `--check`, installs `wallet-pir-query.service` with `MemoryMax` 12 GiB
   below the host's memory and `MemoryHigh` 8 GiB below that, and starts it.
6. With a domain, installs Caddy and serves the wallet paths, Receiver's
   included, over HTTPS. It refuses to overwrite a Caddyfile it did not write.

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
checksum in `install.sh`, which the plain one-liner then installs.

## Infrastructure ownership

The maintained fleet Terraform root owns the shared query node through
`module.query_serving_02[0]`, enabled by its existing
`query_serving_02_enabled` input. Keep that address, persistent volume, DNS and
installed production inputs unchanged during application deployments.

The one-time Query 01/02 additive provisioning commands and creation-only plan
checker have been retired. Query 01's infrastructure declaration is removed;
application releases use [`pir-deploy`](operations.md#deploying-a-build). Historical
provisioning receipts and their original operations packages remain available
for inspection.

For an explicitly approved infrastructure change, use the existing
[production Terraform root](../tools/infra/digitalocean/fleet/README.md) and its
locked `wallet-pir-terraform.sh` wrapper. Save and review a complete plan using
the existing backend and production variables before applying it. Persistent
storage is protected from destruction. Do not recreate a retired query host or
apply a broad fleet migration as part of an application release.

## Application deployment

Every main commit publishes the build release `build-<sha12>`, whose
`pir-query-server` carries every query product. Roll it onto the unified nodes
with [`pir-deploy`](operations.md#deploying-a-build): `pir-deploy --dry-run unified
<sha>` first, then `pir-deploy unified <sha>`. On each node `pirctl` runs the new
binary's `--check` against the installed configuration, restarts the unit, waits for
`/health` and runs the release's `pir-probe`, which sends one encrypted query per
enabled product and compares the answers with pinned ones; a failure restores the
previous release. Do not install units or copy executables to production by hand.
An offline check does not establish dataset fit or live correctness.

A new node needs its rendered unit and configuration installed first and a
`/etc/wallet-pir/pirctl.conf` line naming it (with `config=` and `probe=`/`products=`)
before `pir-deploy` can roll it.

The role renderer composes the product-owned renderers, checks listener collisions
and disjoint state below `/srv/wallet-pir-query`, mounts CA files read-only, and
returns `--config ... --check` for the actual binary before activation. The unit
requires the mounted volume. Its shutdown does not force-kill admitted work at a
timeout; a stalled drain must be reported by the deploy.

For a quick encrypted smoke check outside a deploy, build the
wallet clients with the production protocol (`native-reinspiring`; without it
they refuse the live manifests) and compare against the existing endpoints:
`enhance-pir-cli --server <origin> query <position>`, the ignored
`txid_live_lookup` test with `TXID_LIVE_URL`, `frozen-provider-client <origin>
<script> <birthday>`, and the ignored Status `live_lookup` test of
`status-publisher` (`--features cli,native-reinspiring`) with `STATUS_LIVE_URL`,
`STATUS_LIVE_TXID` and `STATUS_LIVE_HEIGHT` taken from an independent block
source. Transparent compact filters (`/transparent/v1/filters/*`) are not
served by the query binary and answer 404 on this node.

After activation, `pir-deploy status` shows the release each node runs and
`pir-deploy rollback unified` returns to the previous one. Verify independent encrypted answers for every configured product and archive
range over the candidate HTTPS endpoint, then observe a replacement publication,
withdrawal/recovery and restart. Confirm private routes are refused externally.
Receiver remains an unmet all-products gate. Production endpoint cutover and
retirement of existing hosts require their own explicit rollout decision.
