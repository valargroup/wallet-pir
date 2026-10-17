# Prepared history publication bootstrap

Status: PR1 implementation checkpoint, verification only. This is an opt-in
publisher/provider interface, separate from wallet protocols. It does **not**
complete PR1 activation acceptance or implement the update manager, embedded
archive router, standalone packaging, or unchanged-application qualification.
The [approved plan](recent-provider-plan.md) retains those outcomes and order.
The fixed [Vizor baseline and API trace](wallet-adapter.md#fixed-vizor-provider-qualification-baseline)
remain unchanged.

## Producer and inventory

The history publication controller accepts `distribute_prepared: true` only
with `ship_runtimes: true`. Both default false. Existing deployments keep their
optional shipped-runtime fallback policy. Distribution finalization uses the
same verified candidate and existing publication/chain checks; no Display
publisher or universal artifact format is introduced.

The publisher builds and verifies recent runtimes before attaching a finalized
inventory to its active pointer, under `publication_gate`, after the existing
fleet activation, epoch and canonical-hash checks. A missing, unfinished, corrupt
or incompatible recent runtime prevents finalization. It verifies native body
checksums, public-mask binding and source self-consistency through the production
loader; a header/length match alone is insufficient.

All routes are GET-only under `/provider/v1/`:

| Route | Response |
|---|---|
| `current` | Small `transparent-prepared-distribution-v1` record: durable monotonic `revision`, process-local `recovery_epoch`, immutable `inventory_sha256` |
| `inventories/{sha256}` | Exact digest-bound inventory JSON for the current publication |
| `artifacts/{sha256}` | Exact immutable file bytes referenced locally by that inventory |

Current responses use `Cache-Control: no-store`; immutable responses use a
content digest and immutable cache policy. Only the current inventory is offered
for bootstrap. Ordinary supersession does not revoke previously held material.
Unknown or retired targets return an explicit refusal. Artifacts common to the
current publication remain reusable. Four artifact bodies may be open at once;
permits and publication handles remain held until transport drop.

The inventory records network/genesis, canonical map digest, existing shard
schema, exact native runtime format/profile and first recent shard. The existing
map binds every exact shard, revision, range, geometry and terminal hash; no
wallet-visible generation is added. Routing metadata describes public holdings,
not archive readiness or an authorized destination. Files are classified as
metadata, recent table, recent runtime or archive table. All current manifests
and filters are local metadata; archive evaluation tables are never fetched by
the bootstrap consumer. Runtime filenames/identities and table formats are
unchanged. No signing keys or distribution credentials are added.

A controller open, pointer commit or invalidation reserves a durable monotonic
revision before offering that state. Withdrawal refuses current/bootstrap. An
invalidation of a journal-only suffix may retain the older canonical publication;
it still advances revision/recovery state. Epoch and pointer capture share the
existing publication gate. Recovery epoch is local to the controller lifetime;
the persistent revision orders records across restarts. Do not delete or restore
the publisher revision file independently of its publication root.

## HTTPS consumer

The existing binary has a one-shot verification entry point:

```sh
transparent-shard-server --publisher https://publisher.example \
  --publisher-network main --publisher-genesis <configured-genesis-hash> \
  --shard-dir ./provider-inputs --cache-bytes 4294967296 --verify-only
```

The configured origin is the trust root: HTTPS only, normal hostname/certificate
verification, no credentials in URLs, no path/query/fragment and no redirects,
including same-origin redirects. An optional `--publisher-ca` adds an explicitly
trusted PEM root without disabling TLS. TLS termination belongs to the operator;
the controller retains its existing loopback HTTP listener. No production proxy
routing, DNS, listener or deployment is changed by this checkpoint.

The consumer caps each metadata artifact at 16 MiB and uses a configured total download budget
(`--bootstrap-max-bytes`, default 64 GiB), sequential streaming transfers and
connect/whole-request deadlines. The map is fetched first; inventory
classifications must agree with its exact recent/archive boundary before tables
are downloaded. Paths use a restricted relative alphabet and reject escapes,
dot components, URL syntax, duplicate paths and symlinks in producer sources.
Every file is checked for exact length and SHA-256.

An exclusive bootstrap lock protects each operator-owned directory. An atomic,
fsynced `publisher-high-water.json` binds the normalized HTTPS origin and entire
last-observed current record. Older revisions and contradictory equal revisions
are refused. Observing a newer revision fences the old one even if the new
download fails. Do not copy/restore an older high-water file to bypass replay
protection or change publishers inside that directory. A clean directory trusts
the explicitly configured origin for its first current record.

Each attempt stages in a new private tree. The production assignment planner
selects the complete recent tier; its inert archive row is cost/accounting only
and does not establish a route. The production shard loader verifies metadata
and recent tables; unassigned archive tables may be absent. Prewarm uses
`ShippedRuntimes::required`: missing, incompatible, corrupt or failed-self-check
prepared files fail without disk-restore or local-build fallback. That view
retains its strict policy on subsequent cache misses. Existing optional views
retain their fallback behavior.

After loading, a fresh authenticated `current` must exactly match the starting
record. Outage, withdrawal or supersession fails the attempt. Failed staging
trees are removed; successful trees contain `verified-inventory.json`. On restart,
verification contacts HTTPS again; retained local files never establish serving
authority. The JSON report explicitly says `serving: false`, names the verified
publication and reports preparation/transfer counts. No wallet listener starts.

## Trust and remaining activation gate

Trust is the explicitly configured authenticated publisher plus the existing
publisher completeness/canonicality assumptions and validation. Digests establish
integrity and identity, not chain acceptance or serving authority. The producer
retains its existing journal/node canonicality source and withdrawal ordering.
The verifier needs no indexer, node credentials, publisher disk, fleet credential
or SSH connection.

**PR1 remains incomplete until the actual provider activation owner and ongoing
canonicality/invalidation refusal path are implemented and tested.** A final
current recheck establishes a bootstrap observation, not authority to serve
indefinitely. The current checkpoint deliberately has no activation API or
listener escape hatch. The remaining work must preserve Transparent's existing
no-lease semantics, retained-request ownership, reorg refusals and locally
verified-input distinction; it must close the provider's precise canonicality
source and invalidation ordering with the real producer. This is a PR1 acceptance
gate, not a deployment detail or permission to defer validity to a later PR.

Real TLS tests terminate HTTPS with a fresh localhost certificate and forward to
the actual controller router. The actual provider/production loader receives
those bytes. They compare original producer files with downloaded files and
exercise missing material, wrong network/runtime, path escape, corruption,
truncation, redirect, untrusted certificate, withdrawal and replay. These tests
do not execute the fixed wallet application or qualify archive routing,
production TLS, distribution scale, hardware or the complete service.
