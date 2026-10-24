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

The [inventory settings example](../tools/deploy/query-server/query-node.example.json)
reserves 24 GiB for Enhance's prepared cache, 64 GiB for history and 16 GiB for
detail, with one history build slot and one history query slot. Status, queries,
download buffers, retained revisions and publication replacement overlap also
consume memory. Measure their simultaneous peak before increasing concurrency.
An allocation failure must refuse work; never count swap as serving capacity.

Replace example publisher origins and network identities with the current
verified publication identities. List every Enhance shard in the current
publisher's domain, and set `sealed_shards` and cache/download budgets to hold
that entire set. The six-shard example is not an assertion about live coverage.
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

## Provisioning from a branch

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
4. Apply that exact saved plan through the locked wrapper. If the guard refuses,
   stop; do not target resources to evade a fleet change. Retain Terraform output
   privately and enable the candidate in the maintained production variables so
   the next regular fleet plan does not propose removing it.
5. Verify cloud-init completion, actual 192 GiB/24 CPU capacity, volume identity,
   mount, free space and TLS before staging the application. Obtain the new host's
   SSH pins through the trusted provisioning channel; never use unverified
   `ssh-keyscan` output as trust.

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

The role renderer composes the product-owned renderers, checks listener collisions
and disjoint state below `/srv/wallet-pir-query`, mounts CA files read-only, and
returns `--config ... --check` for the actual binary before activation. The unit
requires the mounted volume. Its shutdown does not force-kill admitted work at a
timeout; a stalled drain must be reported by the deployment transaction.

After activation, retain transaction status and the exact rollback command.
Verify independent encrypted answers for every configured product and archive
range over the candidate HTTPS endpoint, then observe a replacement publication,
withdrawal/recovery and restart. Confirm private routes are refused externally.
Receiver remains an unmet all-products gate. Production endpoint cutover and
retirement of existing hosts require their own explicit rollout decision.
