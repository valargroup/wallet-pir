# Prepared history publication bootstrap

Status: PR1 implementation checkpoint, one-shot verification only; independent
review and qualification remain pending. This opt-in publisher/provider interface
is separate from wallet protocols. PR2 owns continuous reconciliation and atomic
activation; this command does not start a wallet listener or implement an archive
router, standalone packaging, or unchanged-application qualification.
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
| `current` | `transparent-prepared-distribution-v1`: durable `revision`, process-local `recovery_epoch`, `inventory_sha256`, network/genesis, exact anchor, eligibility and `invalidated_from` |
| `anchors/{height}/{hash}` | Live canonicality attestation for a public anchor, bound to genesis and the publisher revision/recovery snapshot; no serving permission |
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
revision before offering that state. Epoch, invalidation range and pointer capture
share `publication_gate`. The current record explicitly distinguishes `canonical`,
`withdrawn` and `noncanonical`. Blocked records identify the former target for
recovery; they do not offer it for bootstrap. A journal-only fork beyond the
active anchor can retain that canonical publication while advancing recovery
state. Invalidation at/below its anchor refuses it even if a malformed record
incorrectly labels it canonical.

Current eligibility and the anchor endpoint use the **same existing NodeClient**
and configured producer chain RPC as controller publication acceptance. They
check genesis and the exact block hash, recheck network identity before treating
a mismatch as positive noncanonical evidence, and fence the reply against epoch/
revision changes while RPC was pending. A source outage, wrong-network node or
node behind the anchor cannot attest a fresh candidate. No RPC URL, node cookie,
indexer access or fleet credential is given to the provider.

An immediate withdrawal can precede ancestor discovery/epoch advancement. Thus
an equal-revision record may only move from eligible to blocked, with identical
publication/recovery identity; it may never re-enable an observed blocked target.
A new good poll alone cannot clear that fence: recovery requires a higher durable
revision and fresh successful checks. Epoch is process-local, never a durable
cross-restart authority floor; the persistent revision orders controller opens.
Do not delete/restore the revision file independently of its publication root.

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
last-observed current record. Older revisions, contradictory equal revisions
and equal-revision re-enablement are refused. Explicit withdrawals/noncanonical
observations are persisted before refusal, even when no artifact was downloaded.
Observing a newer revision fences the old one even if its download fails. Do not copy/restore an older high-water file to bypass replay
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

After loading, the owning bootstrap consumer obtains a fresh anchor attestation
and then rechecks authenticated `current`, which must exactly match the starting
record. Network/genesis/height/hash, revision and recovery identity must all bind
the downloaded map. A negative attestation fences that candidate durably before
refusal. Outage, withdrawal, invalidation or supersession fails the attempt. Failed staging
trees are removed; successful trees contain `verified-inventory.json`. On restart,
verification contacts HTTPS again; retained local files never establish serving
authority. The JSON report explicitly says `serving: false`, names the verified
publication and reports preparation/transfer counts. No wallet listener starts.

## Trust, authority and the PR2 owner boundary

The explicit authenticated publisher origin is trusted for indexed completeness
and for truthful attestations from its configured existing chain RPC. An anchor
attestation is a **delegated publisher canonicality check**, not an independent
chain proof. This choice preserves the existing source/trust profile without a
provider indexer/node/SSH/credential dependency. Wallet chain acceptance and
completeness checks remain independent and unchanged. Digests alone establish
neither chain acceptance nor serving authority.

The one-shot command verifies candidate inputs and this live authority context;
it does not grant permission to serve. In PR2 the existing publication owner must
repeat the fresh current/anchor checks immediately before its predecessor-map
prepare/activate CAS, consume applicable invalidation first, and retain admitted
handles under the existing rules. Persisted prepared files and saved attestations
can never bypass that owner boundary on restart. A retained older publication can
be checked through `anchors/{height}/{hash}` after missed revisions; the latest
`invalidated_from` alone is not a cumulative revocation history and must not be
used to infer old-anchor validity. Positive noncanonical evidence is applied via
the existing persisted invalidation path before admitting/returning affected work.

Transparent has **no lease**, expiry extension or watchdog renewal. A fresh HTTPS
response means the publisher checked the offered anchor; it does not promise a
latest chain height, complete indexing or indefinite permission. Routine publisher
advancement is not revocation. Failed polls/source outages cannot establish a new
candidate or refresh authority, and do not by themselves prove that an already
active canonical publication is orphaned. PR2 must preserve those distinctions,
serving-owner canonicality checks, refusal codes and restart fences. Continuous
polling/retry, activation and retained-request lifecycle remain PR2 implementation;
the source, evidence meaning and ordering above are fixed at this PR1 boundary.

Real TLS tests terminate HTTPS with a fresh localhost certificate and forward to
the actual controller router. The actual provider/production loader receives
those bytes. They compare original producer files with downloaded files and
exercise missing material, wrong network/runtime, path escape, corruption,
truncation, redirect, untrusted certificate, withdrawal, replay, real NodeClient
canonicality-source failure/mismatch, higher-revision recovery, journal-only
retention and older-anchor checks. These tests
do not execute the fixed wallet application or qualify archive routing,
production TLS, distribution scale, hardware or the complete service.
