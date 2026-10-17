# Prepared history publication bootstrap

Status: bootstrap, recent-worker reconciliation and opt-in embedded archive
routing implementation; independent review and qualification remain separate gates.
This interface is separate from wallet protocols. Standalone packaging and
unchanged-application qualification remain subsequent gates.
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

Current responses use `Cache-Control: no-store` and an ETag of the entire
serialized current record. `If-None-Match` yields 304 only after the same live
source checks as a full response. A 304 supplies no new provider authority or
activation proof. Immutable responses use a
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
share `publication_gate`. Counter writes fsync the file, rename it, and require
successful directory open/fsync before returning a revision to the owner. A failed
post-rename sync may consume a reservation but cannot offer it; retry/open skips
the uncertain reservation rather than rolling it back. The current record explicitly distinguishes `canonical`,
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
Every file is checked for exact length and SHA-256. One attempt-wide streaming
counter charges all response bodies: initial and final current, inventory, anchor
attestation and artifacts, including bytes consumed before any refusal or failed
validation. An exhausted budget prevents the next request; metadata limits do not
replace the remaining total budget. The successful report includes all charged
body bytes (excluding HTTP/TLS framing); failures abort without resetting the
counter or retrying within that attempt.

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
it does not grant permission to serve. The PR2 publication owner repeats the fresh current/anchor checks immediately before its predecessor-map
prepare/activate CAS, consumes applicable invalidation first, and retains admitted
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
active canonical publication is orphaned. Continuous reconciliation preserves
those distinctions, serving-owner checks, refusal codes and restart fences. The
source, evidence meaning and ordering remain the PR1 trust boundary.

Real TLS tests terminate HTTPS with a fresh localhost certificate and forward to
the actual controller router. The actual provider/production loader receives
those bytes. They compare original producer files with downloaded files and
exercise missing material, wrong network/runtime, path escape, corruption,
truncation, redirect, untrusted certificate, withdrawal, replay, real NodeClient
canonicality-source failure/mismatch, higher-revision recovery, journal-only
retention and older-anchor checks. These tests
do not execute the fixed wallet application or qualify archive routing,
production TLS, distribution scale, hardware or the complete service.

## Following publications with the recent worker

Omit `--verify-only` from the command above to run the manager and existing recent
worker in the existing binary. A successful initial fetch or verified predecessor recovery, strict load, prewarm
and live owner check precede listener binding. Startup source failure refuses to
listen; restart retries through the operator's normal process supervisor. A saved
predecessor can recover through fresh exact-anchor checks while a newer candidate
fails to download; the saved pointer alone grants no authority. After
startup, synchronization runs independently of wallet requests. This is a recent
worker, **not a complete history provider**: the accounting-only archive row does
not qualify a route. PR3 still owns complete-history composition.

The manager holds the same exclusive directory lock as bootstrap. Its serving
owner retains that lock across cancellation and blocking activation work, so a
new manager cannot race an unfinished durable commit. Each attempt
has the existing connect/request deadlines and response-body budget. Transfers
are sequential (concurrency one); successful synchronization waits five seconds
plus up to one second of jitter, while failures back off exponentially to 120
seconds plus jitter. Only an activated snapshot supplies a conditional token.
Unchanged responses do not fetch inventories/artifacts, update a success-time
lease, or authorize an activation. Transparent still has no lease.

Completed objects live under `objects/<sha256>`. Every reuse verifies exact
length and digest; each fresh staging tree receives separate copies, then the
production loader verifies tables and prewarms required native runtimes. On
later preparation, native files are also loaded and self-checked independently of
resident cache hits before recording a replacement directory, even on the same map. Files,
child directories and staging parents are synced before publishing an active
pointer. Incomplete copies have a `.partial` name and are never reuse candidates.
A corrupt object is downloaded again; a bad download never replaces its completed
object. Successful artifacts from an interrupted attempt are reusable without
relying on old inventories, deltas or saved proofs. Interrupted staging and old
objects remain non-authoritative. On activation and every subsequent poll, the
manager releases unused in-memory views with the owner's existing policy: retain
three recent predecessors and any older snapshot still held by an admitted
request. A long request does not retain other unused generations, and completing
it permits collection on an unchanged poll. This collection never deletes source
trees or immutable objects; disk-retention automation is not implemented
in this slice, so operators must budget disk for retained trees and objects.

`publisher-high-water.json` retains replay/withdrawal fencing. A failed write
also attempts `publisher-high-water-fallback.json` with the same complete source
record, so an equal revision cannot substitute another inventory. Subsequent
reads check both records and subsequent writes reconcile both once the fallback
exists. The manager also rejects source revisions below its durable revocation
floor; if both high-water writes failed, an equal eligible revision without a
saved exact source identity cannot activate another map. Before preparation,
positive applicable invalidation is applied through the existing owner's durable
`active.invalid.json` fence. `revocation-floor.json` additionally records the
highest revision and cumulative exact digests at which revocation was observed,
before changing that fence. Startup replays this intent even if the process died
before the owner's fence write. If intent persistence fails, the owner still
fences memory and attempts its own durable record. This also applies when the
high-water write fails after a validated source response, at ordinary polling,
activation rechecks and recovery rechecks; invalidation cannot disappear behind
a control-record storage error. A process retains its observed revision/digest
floor even if both intent writes fail, including a separate exact-anchor negative
following an eligible Current at that revision. All activation and clearing gates
combine this in-memory bound with both durable records. Repairing storage does
not permit same-revision recovery. If every applicable exact safety write is
unavailable, a process or power loss cannot be promised to preserve that negative
observation; the in-process bound is not a universal durability guarantee.

When an already-running Source has a trusted complete in-memory Current, an
unreadable or malformed high-water record does not hide a freshly authenticated,
identity-checked and replay-ordered negative response from the owner. Every
readable companion record must still agree. The owner fences affected material,
while the original storage error remains visible and activation is refused.
Startup without that trusted baseline stays closed. Malformed, replayed or failed
transport responses remain errors rather than revocation evidence. Startup binds otherwise
unrecorded owner revocations to the previously observed source revision before
polling. A failed primary intent write/read also attempts the exact revision and
digests in `revocation-fallback.json`, independently of the high-water write.
Startup reconciles both intent records; once a fallback exists, successful
recovery writes both records so a crash between them remains conservative.
A corrupt primary remains a startup/activation error, even if the fallback
preserved the newly observed negative evidence. This retains exact higher-revision
recovery after simultaneous high-water and primary-intent write failures instead
of estimating a floor from an older saved revision. Only a higher revision with fresh exact candidate-anchor attestations can clear
revoked digest entries. Neither the process-local epoch nor the most recent
invalidation range is a cumulative history. The manager checks exact anchors of
all retained revisions on changed snapshots; source failure cannot prove an
anchor orphaned, while a positive mismatch immediately fences it.

The owner holds activation serialization while re-reading unconditional current,
checking exact anchors and re-reading current again. Only an identical eligible
snapshot may cross its existing predecessor-map CAS. Ordinary supersession or
failed transfer/loading leaves a valid active view and retained admitted handles
available. Recovery prepares a new snapshot; clearing a durable digest entry does
not revive a cancelled in-memory snapshot. Restart re-fetches live current and
checks cached bytes through the same strict loader before any listener is exposed.
The saved active pointer is a crash-consistent record, never offline authority.

Required runtime policy belongs to this mandatory `--publisher` mode and its
owner, including subsequent preparation and query cache misses; it does not add
a field to the existing publication JSON. Restart with that same provider
configuration. Existing fleet/static modes retain their optional-runtime policy.
Do not reinterpret a managed root as a static/fleet publication or restore older
high-water/revocation records to bypass fencing. Before stopping managed mode,
repair/reconcile any otherwise unpersisted exact negative revision/digest bound
held by the live process into durable safety state. Then stop this mode and retain
its safety records; no publisher artifacts or wallet formats require migration.
Before using a reader that ignores either fallback file, reconcile the maximum
revision/digest union into the primary intent and the newest complete source
record into the primary high-water while stopped, preserving negative eligibility
at equal revisions and refusing contradictory identities, or retain a
fallback-aware reader. Code revert or safety-file deletion alone is not a safe
rollback. If all applicable exact safety writes remain unavailable, process or
power loss can still lose the memory-only bound.

A changed inventory is reconciled even when its map digest is unchanged. The
existing same-map/different-worker-assignment fence remains in force. Without an
archive route, required recent-worker mode refuses candidates retiring a local
shard. With the embedded route below, retirement additionally requires the exact
unchanged revision in the candidate metadata and a qualified terminal archive,
both during preparation and before activation. The final publisher checks still
follow archive qualification immediately before the durable predecessor CAS.
No archive cost/accounting row bypasses this boundary.

## Embedded history endpoint and fixed archive

Add `--archive-upstream https://<trusted-Valargroup-archive-origin>` to managed
serving mode to compose routing and recent execution in the existing process.
The operator must supply the trusted deployment address: this document assigns
no public hostname/IP and changes no production routing. `--archive-ca` may add
an explicit trusted PEM root; certificate/hostname verification remains enabled.
Only isolated loopback fixtures may use plain HTTP. The configured upstream must
terminate at an owner running this serving implementation with warm readiness,
not another forwarding provider. Redirects and wallet-supplied destinations are
never used. There is one upstream and no automatic switching or query replay.

Maps, filters, manifests and initialization remain verified local metadata for
the complete publication, including distinct archive geometries. Eligible exact
local revisions execute through the existing worker. Nonlocal current revisions
obtain setup and encrypted answers through the fixed archive. Each operation
checks terminal warm readiness, network/genesis/profile/native parameters, exact
manifest and setup identity. Query replies must have the exact segment framing,
revision/table prefix and qualified mask epochs. The wallet retains its own
validation. Readiness is an observation, not a lease or a completeness proof.

Forwarding uses the existing admission/body budget and deadlines. Setup and
query responses retain the admitted snapshot/resources through transport drop;
forwarded answer buffers reserve shared work memory. A transport failure after
query submission never causes another encrypted submission. Explicit upstream
refusals retain their existing status and retry delay. Failed qualification
returns 503; it never returns successful empty history or performs public lookup.
Independent valid recent operations do not consult the archive and remain
available during its outage. `/v1/ready` describes local readiness and separately
identifies forwarding with `archive_forwarding`; it does not attest complete
archive availability. Promoted local revisions remain available under the
owner's existing retained-handle policy, with unchanged revocation fences.

The fixed original released-client process is in
`transparent/services/transparent-shard-server/tests/frozen-client`. Its isolated lock and source verifier
exclude the server's newer library pin; build and execute it through the required
repository wrapper. From the repository root, run
`python3 transparent/services/transparent-shard-server/tests/frozen-client/run-tests.py --output .local-pir/provider-qualification-<new-id>`
inside that wrapper. It records resolved metadata, the isolated lock digest,
crate/source-byte checks and build preparation times separately, then executes
each HTTP case under the existing 30-second guard. The explicit frozen-process integration case in
`assignment::provider` compares historical receive/spend recovery with original
source events and checks recent recovery during archive outage. Library execution
is not unchanged-application execution. The fixed Vizor build's existing-origin
TLS and same-origin display smoke remains mandatory; no application rebuild,
upgrade, TLS bypass, production DNS or routing change is authorized here.
