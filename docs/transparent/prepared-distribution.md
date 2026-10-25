# Prepared history distribution and standalone provider

Status: bootstrap, recent-worker reconciliation, opt-in embedded archive routing
and standalone release roles are implemented. Independent review, release and
unchanged-application qualification remain separate gates. This interface is
separate from wallet protocols.
The [approved plan](recent-provider-plan.md) retains those outcomes and order.
The fixed [Vizor baseline and API trace](wallet-adapter.md#fixed-vizor-provider-qualification-baseline)
remain unchanged.

Transparent's runtime and publication modules expose small facades over private
modules for parameters, table execution, cache residency, inventory preparation,
restoration and validation. Common table and segment types are owned below the
history and transaction-detail loaders; their established public imports remain
available through the history facade.

Durable metadata replacement uses `pir-control::durable`: each attempt owns its
temporary file, syncs its bytes, atomically replaces the record and syncs the
parent directory. A failure after replacement reports uncertainty to the owner.
Downloads use `pir-publication::ObjectCache` for partial-file ownership and
promotion. Transparent retains its size-only local reuse, receipts, background
hash validation and link-aware collection; storing an object grants no authority.

## Producer and inventory

The history publication controller always prepares runtimes and finalizes a
full prepared distribution; the former `distribute_prepared` and `ship_runtimes`
switches are rejected. Distribution finalization uses the
same verified candidate and existing publication/chain checks; no Display
publisher or universal artifact format is introduced.

The publisher builds and verifies recent runtimes before attaching a finalized
inventory to its active pointer, under `publication_gate`, after the existing
epoch and canonical-hash checks. A missing, unfinished, corrupt
or incompatible recent runtime prevents finalization. It verifies native body
checksums, public-mask binding and source self-consistency through the production
loader; a header/length match alone is insufficient.

Those checks apply to new publication bytes. Once they pass, and before the
active pointer moves, the publisher writes `history-restore.json` beside the
publication: the exact inventory bytes and SHA-256, map, chain and anchor
identity, holdings mode and completed phases. `pir-publisher
prepare-history-restore --config FILE --publication DIR --output RECORD` writes
the same record for a pinned publication after the full checks, without a
journal writer or service. A restarted publisher with such a record restores the
same inventory from metadata digests, safe regular paths, exact lengths and
runtime identity headers, and serves it only after fresh canonicality, intent
and fleet checks. Only a missing record, or unresolved withdrawal or activation
intent, uses the full preparation path. A malformed or incompatible record,
unsafe or incomplete files, or an unreadable failure record refuse to start;
deleting the record clears neither recorded integrity failures nor intent, so
the remedy is a deploy of verified replacement bytes. An ordinary
`publisher-cutover/3` deploy pins the active publication with the snapshot
helper while the predecessor still serves, runs this command on the pin,
refuses before stopping if the active publication moved, and keeps the pin
across the stop. It installs the record only if the active pointer and map are
unchanged after the stop and every pinned file is still the same inode with the
same size in the live publication; otherwise it restarts the predecessor and
refuses so the deploy can be retried. Rollback
restores a replaced record's exact bytes, or its absence. Deploy readiness does
not wait for background validation.
A completed publisher also saves `history-preparation.json` locally before
activation. This versioned record binds the completion record (including the
exact inventory, chain, runtime and profile identities) to every installed
file's device, inode, length, modification time and change time. Restart reuse
requires the same sealed objects, controlled store links and opened-file
identities. Unchanged source preparation and runtime checks can then be reused
by the first fresh publication. Modified or replaced objects take the ordinary
verification path; malformed or incompatible preparation records disable reuse.

For first adoption, only an active, publisher-owned publication with valid
completion evidence and no unresolved intent, withdrawal or recorded failure
may supply preparation. The publisher checks metadata, lengths, runtime headers
and immutable-store links without scanning large bodies. This trusts the
publisher's earlier preparation; it does not establish a fresh content hash.
Latent same-size corruption is therefore detected by background validation.
Preparation evidence never becomes a query server's verified-file proof and
never authorizes serving. Fresh canonicality and final activation checks still
apply.

After serving starts, one low-priority background thread, yielding to
distribution and candidate preparation, hashes every listed artifact
(`integrity_validation` in `/v1/status`) and then loads and self-checks each
runtime under memory admission (`correctness_validation`), using only the files
it hashed unchanged. A newer publication or a withdrawal stops the running
job, but completed per-artifact hashes and runtime checks survive supersession
for the same unchanged files and chain/profile context. Retained progress is
pruned to incoming inventories and through ordinary object collection; it
holds no files against collection.
Inherited preparation leaves validation pending, including after a successor
activates, until these independent checks finish. A correctness
failure is reported and the publication keeps serving. A confirmed integrity
mismatch is recorded in `integrity-failures.json` and withdraws every
publication listing those bytes, in this and later processes, until freshly
verified replacement bytes are published; `recorded` is false in its status
when the record could not be made durable, so only this process withdraws it.
Completion and failure records are synced with their directory before they are
acted on. Withdrawing the publisher's own
routes would leave the router proxying setup and query requests to workers
that still serve the publication, so unless the controller is HTTP-only or a
shadow, it also sends the fleet `withdraw` operation, retried each publication
pass until it succeeds; a restarted process with such a record sends it before
anything else. A later activation of verified replacement bytes supersedes a
withdrawal still pending.

All routes are under `/provider/v1/`, GET-only except the POSTed batch check:

| Route | Response |
|---|---|
| `current` | `transparent-prepared-distribution-v1`: durable `revision`, process-local `recovery_epoch`, `inventory_sha256`, network/genesis, exact anchor, eligibility and `invalidated_from` |
| `anchors/{height}/{hash}` | Live canonicality attestation for a public anchor, bound to genesis and the publisher revision/recovery snapshot; no serving permission |
| `POST anchors/check` | The same attestation for at most 256 distinct anchors in one request of at most 64 KiB naming schema, `revision`, `recovery_epoch` and `inventory_sha256`; any other snapshot is 409. One admission and genesis read before and after the hash reads, then one fence against epoch, revision, map, inventory and withdrawal changes; the response repeats the snapshot and genesis with exactly one result per anchor, or answers none (503) |
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

History and transaction-detail display use the shared `pir-sync-manager` Rust
library for HTTPS transport and the query server's `synchronization` module for
sequential polling. Their Transparent adapters
retain publication validation, replay and revocation records, recovery and
activation. This extraction does not change their wire formats, stored records
or serving authority, and does not migrate other PIR products.

`pir-query-server` has a one-shot verification entry point over a
`transparent_history` configuration ([query server host](../query-server.md#transparent)):

```json
{
  "management_listen": "127.0.0.1:18193",
  "transparent_history": {
    "enabled": true,
    "settings": {
      "publisher": "https://publisher.example",
      "publisher_network": "main",
      "publisher_genesis": "<configured-genesis-hash>",
      "shard_dir": "/srv/transparent-provider/provider-inputs",
      "cache_bytes": 4294967296
    }
  }
}
```

```sh
pir-query-server --config history-provider.json --verify-only
```

The configured origin is the trust root: HTTPS only, normal hostname/certificate
verification, no credentials in URLs, no path/query/fragment and no redirects,
including same-origin redirects. An optional `publisher_ca` adds an explicitly
trusted PEM root without disabling TLS. TLS termination belongs to the operator;
the controller retains its existing loopback HTTP listener. No production proxy
routing, DNS, listener or deployment is changed by this checkpoint.

The consumer caps each metadata artifact at 16 MiB and uses a configured total download budget
(`bootstrap_max_bytes`, default 64 GiB), sequential streaming transfers and
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
and recent table sizes; unassigned archive tables may be absent. Required
prepared files must be present and have their recorded sizes before activation.
On demand, the loader parses their formats without a checksum scan or self-test.
Missing or incompatible required files fail without local-build fallback. That view
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
be checked through `anchors/check` after missed revisions; the latest
`invalidated_from` alone is not a cumulative revocation history and must not be
used to infer old-anchor validity. Positive noncanonical evidence is applied via
the existing persisted invalidation path before admitting/returning affected work. The manager asks
each distinct anchor once, in batches of 256, and revokes every completed batch's
negatives before requesting the next, so a later failure cannot discard them.
There is no fallback to per-anchor requests: release the publisher with this
route before or together with its query servers.

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
worker in `pir-query-server`. A successful initial fetch or verified predecessor recovery, metadata and size checks
and a live owner check precede listener binding. Every assigned range is then
queryable; runtimes load on first use and prewarming runs in the background. Startup source failure refuses to
listen; restart retries through the operator's normal process supervisor. A saved
predecessor can recover through fresh exact-anchor checks while a newer candidate
fails to download; the saved pointer alone grants no authority. When the
recovered publication's persisted verified inventory, map, holdings and runtime
directory all equal the fresh current inventory and none of its digests is
revoked, recovery records that snapshot and finishes without staging, loading or
warming it again; an absent or damaged record, or any difference, takes the full
update while the recovered predecessor keeps serving. After
startup, synchronization runs independently of wallet requests. Without the
fixed archive option below, this remains a recent worker, **not a complete history
provider**: the accounting-only archive row does not qualify a route.

The manager holds the same exclusive directory lock as bootstrap. Its serving
owner retains that lock across cancellation and blocking activation work, so a
new manager cannot race an unfinished durable commit. Each attempt
has the existing connect/request deadlines and response-body budget. Transfers
are sequential (concurrency one); successful synchronization waits five seconds
plus up to one second of jitter, while failures back off exponentially to 120
seconds plus jitter. The acquisition, strict preparation and checked owner publication phases compose
`src/shared/pir-pipeline` using private borrowed tokens; no phase token is persisted.
Shared boundary waiting preserves this delay-after-pass schedule and never drops
an in-progress activation. Every failed pass starts the next attempt with source
reconciliation rather than retrying an uncertain activation directly.
Only an activated snapshot supplies a conditional token.
Unchanged responses do not fetch inventories/artifacts, update a success-time
lease, or authorize an activation. Transparent still has no lease.

Completed objects live under `objects/<sha256>`, read-only. Downloads stream into
unique partial files, enforce the declared byte count and body budget, and are
synced before and after atomic promotion. Metadata hashes remain immediate;
large table and runtime bodies are not hashed during receipt or activation.
Their `.received.json` records contain the publisher's expected identity, not
proof of a completed body hash. Local reloads check expected size. Staging uses
hard links, with size-checked copies only when linking is unsupported. Recorded
download identities cannot be changed to label existing bytes as another artifact.
The serving loader checks metadata and sizes, then parses runtimes on demand
without checksum scans or encrypted self-tests. Interrupted partials are never
reused. A size mismatch refuses a transfer before promotion; a same-size digest
mismatch is detected by the background validator after serving starts.
Successful artifacts from an interrupted attempt are reusable without
relying on old inventories, deltas or saved proofs. Interrupted staging and old
objects remain non-authoritative. On activation and every subsequent poll, the
manager releases unused in-memory views with the owner's existing policy: retain
three recent predecessors and any older snapshot still held by an admitted
request. A long request does not retain other unused generations, and completing
it permits collection on an unchanged poll. After each successful activation the
manager also collects disk with the owner's `Collect`: it keeps the active tree,
every retained predecessor (including one an admitted request still holds), any
candidate, the three most recent other trees and every catalog revision those
reference, and removes other staging trees and catalog revisions. It then
unlinks each completed object that no remaining tree links (link count one). It
skips objects when the active stage was copied rather than linked, and defers
while a stage holds the catalog lock. The publication switch has already committed
and serving continues during collection. The synchronization call waits for
collection before returning, delaying its next publisher poll; collection errors
cannot fail or reverse the committed activation. A deferred or failed collection
is logged and retried after the next activation. Unchanged polls and restarts do
not collect disk. Retention has no fixed tree-count limit: every older view still
held by a request is kept in addition to the three recent predecessors.

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

Add `"archive_upstream": "https://<trusted-Valargroup-archive-origin>"` to the
managed serving settings to compose routing and recent execution in the existing process.
The operator must supply the trusted deployment address: this document assigns
no public hostname/IP and changes no production routing. `archive_ca` may add
an explicit trusted PEM root; certificate/hostname verification remains enabled.
Only isolated loopback fixtures may use plain HTTP. The configured upstream must
terminate at an owner running this serving implementation with loaded readiness,
not another forwarding provider. Redirects and wallet-supplied destinations are
never used. There is one upstream and no automatic switching or query replay.

Maps, filters, manifests and initialization remain verified local metadata for
the complete publication, including distinct archive geometries. Eligible exact
local revisions execute through the existing worker. Nonlocal current revisions
obtain setup and encrypted answers through the fixed archive. Each operation
checks terminal readiness, network/genesis/profile/native parameters, exact
manifest and setup identity. Query replies must have the exact segment framing,
revision/table prefix and qualified mask epochs. The wallet retains its own
validation. Readiness is an observation, not a lease or a completeness proof.

Forwarding has one separate process-wide admission place for setup or query,
including upload, every remote wait and reply-body lifetime, with no forwarding
queue. Recent execution keeps its original configured slots and waiting places;
another archive operation receives the existing capacity refusal before remote
work. Active, candidate and retained snapshots share both admission lanes.
The aggregate upload-byte semaphore and upload/query deadlines are unchanged.
Forwarding may use only the surplus after reserving the largest compiled history
query (rounded to KiB) for each local execution slot. Local requests can still use
the full configured body budget when forwarding is idle. If the configured budget
has no sufficient surplus, archive queries fail capacity admission; it is never
enlarged implicitly. Thus there are at most `query_slots + max_waiters + 1`
admitted requests, of which at most one forwards, with the original aggregate
body-byte ceiling and local evaluation concurrency. Queue/body/cancellation
counters include both roles; local slot utilization excludes remote waits.
Setup and query responses retain admission and the snapshot through transport
drop; forwarded answer buffers still reserve the same shared work memory and
respect its total cgroup headroom guard. This adds a bounded forwarding control
operation, not a local evaluation or a second memory ceiling. Operators must
include its buffers and bounded metadata/setup replies in the total host budget. A transport failure after
query submission never causes another encrypted submission. Explicit upstream
refusals retain their existing status and retry delay. Failed qualification or an
uncertain/invalid answer returns the existing non-capacity 503 without a retry
delay, so the released client does not replay an uncertain encrypted query. It
never returns successful empty history or performs public lookup.
Independent valid recent operations do not consult the archive and remain
available during its outage. `/v1/ready` describes local readiness and separately
identifies forwarding with `archive_forwarding`; it does not attest complete
archive availability. Promoted local revisions remain available under the
owner's existing retained-handle policy, with unchanged revocation fences.

The fixed original released-client process is in
`src/pir-query-server/tests/transparent/frozen-client`. Its isolated lock and source verifier
exclude the server's newer library pin; build and execute it through the required
repository wrapper. From the repository root, run
`python3 src/pir-query-server/tests/transparent/frozen-client/run-tests.py --output .local-pir/provider-qualification-<new-id>`
inside that wrapper. It records resolved metadata, the isolated lock digest,
crate/source-byte checks and build preparation times separately, then executes
each HTTP case under the existing 30-second guard. The runner enforces that fixed
limit explicitly, independently of inherited `WALLET_PIR_CASE_SECONDS`, and runs
lightweight override/receipt/failure controls before building. These hermetic
controls do not replace the real client cases. It retains executable hashes,
build times, resolved sources, case results and before/after raw host/cgroup
observations in a new ignored output directory. Those host snapshots are shared-host
diagnostics, not peak-memory measurement or proof of isolation.
The independent-process case retains the actual service/operations archives,
prepared producer files and current/inventory bytes before temporary teardown;
the runner hashes every retained input. Its source guard uses the shared
correctness identity, including tracked changes and untracked source inputs.

The frozen-process cases in `assignment::provider` compare complete historical
receive/spend recovery with source-event oracles, including actual 32k archive
directory/page execution with 2k recent execution. Directory corruption, page-only
answer corruption and page-only setup-epoch mismatch must refuse recovery without
replaying uncertain submissions. The installation fixture also starts two independent
provider processes with separate state directories, recovers through each endpoint,
restarts one while the other answers encrypted work, and checks active service after
publisher loss plus recent success/explicit historical refusal after archive loss.
This installation fixture serves production-finalized artifacts over HTTPS; it is
not the controller publication/update campaign. Ordinary CPU runs use exact source
rows; the explicit frozen harness additionally checks each endpoint's source-event
ledger oracle. Library execution
is not unchanged-application execution. The fixed Vizor build's existing-origin
TLS and same-origin display smoke remains mandatory; no application rebuild,
upgrade, TLS bypass, production DNS or routing change is authorized here.

On a worker limited to three CPUs, both `released391_*` archive cases exceed this
harness's 30-second guard. Neither is a client hang or a wrong answer. In
`released391_recovers_32k_archive_pages_with_2k_recent` (58.6 s unguarded), the
in-process 32k archive terminal spends 49 s building its native runtimes from
tables before any client starts. The whole frozen recovery takes 5.9 s, and each
refused corrupt recovery takes under 0.3 s. Two build slots did not change this.
`released391_recovers_archive_and_recent_then_recent_during_archive_outage` takes
18.4 s alone. The guard kills the whole test process at the first deadline, so this
case is reported missing rather than timing out itself. Shipping prepared runtimes
to that terminal, or a host with more CPU, is the remaining fix for the 30-second
gate.

## Controller update campaign

The `controller_update_campaign` test and its `https-update-campaign.py` runner
were deleted after `bee37567`: they depended on a fleet-era archive owner serving
a planned assignment from local files, which query servers no longer support.
Read them at that revision. The unified HTTPS campaign
(`unified_http_sync_campaign`) covers current provider interruption, publication
advancement and archive/recent recovery with the frozen released clients. The
dated result below remains a historical observation of that removed campaign.

### 2026-10-19 local run

Source `d0f41852` (clean), release-fast, on a shared x86-64 worker with 8 visible
CPUs, a 3-CPU cgroup quota and 31 GiB RAM. Raw run:
`.local-pir/https-update-campaign/20261019T001951Z-d0f418525e12/` (not committed).
The in-process tests passed 38/38. The campaign passed in 527.6 s: controller
revisions 2→11, journal tip 13, dataset SHA256 `82cc443a…`.

| Observation | Result |
|---|---|
| Frozen recoveries | 1,448 exact, 0 wrong, 0 refusals, 0 timeouts, 0 errors |
| Unreachable recoveries | 418, all against provider-b while the campaign had stopped it |
| Recovery p50 / p99, unchanged window | 386 / 441 ms (68 recoveries) |
| Recovery p50 / p99, update windows | 461–487 / 803–900 ms |
| Recovery p50 / p99, during 10-consumer waves | 671–741 / 1,100–1,309 ms |
| Changed-pass stages, median (max) of 14 | transfer 3.80 (4.74) s, staging 1.95 (2.38) s, loading 2.92 (3.41) s, prewarm 2.26 (2.99) s, activation 0.05 (0.10) s |
| Supersession | Stalled revision never activated; providers refused the withdrawn inventory with 404, then converged |
| Publisher TLS restart | Providers resynchronized 2.1 s and 5.1 s after restart; one more revision converged on both |

These recovery latencies are whole fresh-process restores on a shared host.
They are not matched query p99 measurements.

## Distribution load diagnostics

`tools/transparent/loadtest/distribution.py` is the corrected PR #201 driver
(head `1437b2a9`), ported unchanged. It runs explicit artifact waves of 10 or 100
logical consumers against the real controller's HTTPS terminator. Each consumer
streams one object at a time and checks the actual body length and digest. It
then polls `current` with `If-None-Match` and requires 304 responses. Every
status, retry, timeout and partial body is kept in per-consumer receipts. It
verifies neither native serving nor provider activation. A shared
`--cache-budget-bytes` ceiling refuses a wave before any artifact request when
the candidate does not fit. Publisher admission (four artifact bodies, shared
with canonicality checks) is measured, never tuned, by the driver. The campaign
runs it after bootstrap and after an update, and samples controller and
terminator CPU, RSS and character I/O.

In the 2026-10-19 run, each 10-consumer wave completed for 8 of 10 consumers. The
other two used all eight attempts on HTTP 503 admission refusals.

| 10 consumers | Bootstrap | Update |
|---|---:|---:|
| Elapsed | 40.7 s | 39.2 s |
| HTTP 200 / 304 / 503 | 102 / 16 / 60 | 80 / 16 / 31 |
| Body bytes read | 1,477,881,768 | 1,477,861,144 |
| Client body rate | 36.3 MB/s | 37.7 MB/s |
| Controller / terminator CPU | 12.5 / 7.2 s | 12.7 / 7.0 s |
| Controller / terminator peak RSS | 223 / 54 MB | 225 / 44 MB |

During both waves the providers' own `current` polls were also refused with 503
(6 and 8 times), and recovery latency rose as shown above. That is the shared
admission and serving interference this diagnostic exists to expose. The
100-consumer waves were refused by the 12 GiB guard before any artifact request:
the candidate alone is 18,473,074,600 bytes, more than this task's 40 GiB disk
reservation can hold beside builds and evidence. Run that step on existing hub
hardware with enough disk.

### Coverage

| Gate | State |
|---|---|
| Real controller, two providers, separate state roots, HTTPS | Covered (small fixture) |
| Fixed archive upstream with its own state root | Covered (small fixture) |
| Frozen-client oracle independent of the controller | Covered (journal ledger) |
| Publisher TLS restart and recovery, then a new revision | Covered |
| Supersession mid-transfer; restarts during transfer and activation | Covered; fencing stays in-process |
| Stage timings; recovery latency, refusal, error and timeout counts | Recorded |
| Dataset identity in the manifest | Recorded |
| 10-consumer distribution with actual bytes and 304 polling | Run; 8/10 complete under admission refusals |
| 100-consumer distribution | **Open**: refused by the disk bound here; needs a hub run |
| Original Vizor build 195 application | **Open** |
| 64 GB Recent Only holdings and matched p99 ≤ 10% | **Open** |
| Suitable isolated hardware | **Open** |
| Release provenance (authenticated release artifacts) | **Open** |
| Reorg, ownership-only moves and archive outage in this campaign | **Open** (covered elsewhere only as in-process or fixture cases) |

## Standalone installation from release artifacts

This prepares an operator-owned **Transparent history only** installation. It
neither installs on our fleet nor establishes qualified release performance.
Use Linux x86-64 with the release's supported CPU/OS ABI, Python 3.11+ (standard
library only), a service supervisor, and an operator TLS reverse proxy. Keep the
current [Vizor baseline](wallet-adapter.md#fixed-vizor-provider-qualification-baseline)
fixed; its existing origin/configuration constraints still apply. Do not change
an application binary, disable TLS, or assume it can select a new provider.

Obtain `wallet-pir-ops-<full-sha>` and `transparent-shard-<full-sha>` from a
qualified existing service/full release run at the **same immutable revision**.
Use that run's authenticated artifact download and digest/provenance verification
as described in the [release workflow](../operations.md). Fast CI and
locally assembled `release-fast` artifacts are test smokes, not qualified fat-LTO
`release` binaries. No source checkout, Rust toolchain, private inventory,
controller credential, chain RPC credential or deployment file is needed at
runtime. The operations archive carries the renderer, registry, Python imports,
service templates, this guide and public examples. The binary archive retains
its existing `transparent-shard` kind and binary names.

In a new operator staging directory, unpack the authenticated operations archive
(`wallet-pir-ops.tar.gz`) into `ops-release`, then verify its installed contents
and extract the binary archive using that release's verifier. Replace `SHA` with
the full approved revision, and use absolute paths for `PWD` and all settings:

```sh
mkdir ops-release
# Run only after authenticating the downloaded artifact and its recorded digest.
tar -xzf wallet-pir-ops.tar.gz -C ops-release
python3 ops-release/tools/scripts/wallet-pir-bundle.py self-check --sha "$SHA"
python3 ops-release/tools/ci/release.py extract --sha "$SHA" --kind transparent-shard \
  --archive transparent-shard.tar.gz --output "$PWD/history-release"
python3 ops-release/tools/scripts/wallet-pir-roles.py --help
cp ops-release/tools/transparent/deploy/history-provider.example.json provider.json
```

Edit `provider.json`: replace the reserved `.example.invalid` publisher and
archive names with **explicitly approved trusted HTTPS origins**. No production
publisher/archive hostname is assigned by this plan. Production distribution
origin selection is a rollout input, not something inferred from source. The
archive is one fixed trusted terminal Valargroup address, with no discovery,
wallet-supplied destinations or switching. The example's mainnet genesis must
match the approved publisher. `publisher_ca`/`archive_ca`, when needed, name
operator-installed PEM trust roots outside writable state; ordinary certificate
and hostname validation stays enabled.

Canonicality comes from the publisher's live `anchors/check` HTTPS
attestations, delegated to its existing NodeClient chain checks. The provider
trusts that configured publisher for these checks and indexed completeness;
locally verified files/digests are not chain proofs or offline authority. There
is no second provider RPC implementation or local node requirement.

`state_dir` owns downloads, immutable object reuse, staging and replay/revocation
records. Keep it on durable local storage, writable only by the service account,
separate from the release and CA files. Its object store is the download cache;
there is no optional local runtime cache/build fallback in provider mode. Disk
retention remains manual and unbounded: reserve space for objects, active and
candidate trees and retained predecessors. Preserve all safety records during
upgrade/restart; do not reinterpret this root as a static shard directory.

The example reserves a 32 GiB resident runtime cache, two local query slots,
eight waiters, a 64 MiB aggregate body budget, and a 64 GiB per-attempt transfer
cap. Its 48G MemoryHigh/56G MemoryMax leave room on a 64 GB host for the OS and
TLS proxy. These are initial **budgets**, not a measured supported load or proof
that any publisher's holdings fit. Count candidate overlap, setup/reply buffers,
workspace and scratch within the total host budget; PR5 resource and interference
qualification remains required. Too little cache/transfer budget refuses loading.

Render without contacting a host or reading private inventory:

```sh
python3 ops-release/tools/scripts/wallet-pir-roles.py --service transparent-history \
  --role provider --settings provider.json --release "$PWD/history-release" > role.json
```

The role is operator-managed: generic fleet `plan/preflight/deploy/rollback`
refuses it. That engine runs staged checks as the deploy identity before stopping
live units, which cannot safely preflight this exclusive state root. Render and
private reporting remain supported. Use the dedicated unit/user procedure below;
this does not authorize any fleet or production change.

`role.json` describes the unit, its configuration `files`, writable
`directories`, and the exact loader `check` arguments. Create only those
directories, owned by a dedicated `transparent-provider` user/group; install each
file root-owned and readable but not writable by that user, and the rendered `unit` as
`transparent-history-provider.service` through your local service manager.
Before enabling it, run the rendered argument array as that user:

```sh
python3 -c 'import json,subprocess; r=json.load(open("role.json")); subprocess.run(["./history-release/pir-query-server",*r["check"]],check=True)'
```

This preflight checks downloaded bytes, local file sizes and live authority
through the managed owner;
it may prime state but opens no listener. The unit repeats the same check in
`ExecStartPre` on every start/restart. Missing or incorrectly sized required
files refuse preparation/start/restart; malformed formats fail when loaded,
without local-build fallback. Preparation correctness is tested by the publisher.
Existing fleet/static optional preparation policy is unchanged.

Use `history-provider.caddy` from the same operations release as the TLS route
example. Set your owned hostname, listener and certificate policy. For fixed Vizor
integration at its existing origin, set `TRANSPARENT_DISPLAY_UPSTREAM` to the
approved existing HTTPS display service (or an operator-managed loopback owner),
preserving `/v2/txid/*` on that same origin. Do not point it back to this proxy.
This forwards the existing display service and installs no display process.
A history-only endpoint used separately by a library client can import the
history handler into its own TLS site; it cannot replace Vizor's complete origin
by dropping display. Keep `/v1/*` and `/metrics` on loopback. The
renderer refuses public listeners and runs no control socket or additional
product service. Keep other wallet products on their existing services.
For a disposable smoke without systemd, run the extracted binary with `check`
minus its final `--verify-only`, then stop that subprocess and repeat the same
command with the same state root; wallet queries must succeed after both starts.

Inspect `http://127.0.0.1:8092/v1/provider-status` locally. Its independent axes are:

- `synchronization`: the manager's last completed attempt/progress or error and
  whether another attempt is in progress; a failure may coexist with valid service.
- `verified_publication`: the actual owner's loaded map digest, not a downloaded
  candidate or an assertion that the publisher still offers it.
- `runtime_readiness`: owner prewarm state, required-material policy, preparation
  counters and invalidation. Warmth alone is not archive readiness or authority.
- `active_coverage`: each current range/revision, local ownership and revocation;
  nonlocal metadata alone does not prove an eligible archive route.
- `archive_reachability`: initially `unobserved`, then the last real terminal
  readiness/network/profile/exact-manifest identity check's result and time.
  `identity-checked` does not promise future reachability or successful setup/query;
  each operation still checks setup/runtime/session identity independently.

Use `/v1/ready` for local readiness, and logs plus these axes to diagnose failed
updates. Check HTTPS trust, publisher availability/canonicality, disk permissions,
budgets and prepared material before restarting. Publisher outage at startup
refuses listening; an ordinary failed update preserves an otherwise valid active
view. Archive failure explicitly refuses dependent requests while independent
recent queries remain local. No endpoint infers readiness from an address string.

To upgrade, authenticate same-revision operations/binary artifacts in new release
directories, render/preflight against the existing state root while the service
is stopped (its directory lock forbids concurrent consumers), then install the
new unit and restart. Check identity/status and repeat recent/historical wallet
queries. Preserve the prior binaries, unit and durable state for rollback; follow
the safety-record reconciliation requirements above before using any older
reader. Code reversion alone must not erase revocation observations.

To remove an installation, stop/disable its history provider unit and withdraw
only its operator TLS route. Retain state/safety records, artifacts and unit for
review and recovery. No data or resource deletion is part of this procedure.

## Preparation trust and post-start smoke validation

The publisher owns preparation correctness, including comparing prepared
material with its sources and testing encrypted answers before publication.
Query servers trust those prepared bytes. HTTPS downloads check exact length;
local reloads check expected size. After serving starts, one process-wide OS
thread validates each new table/runtime body against its recorded SHA-256. It
pauses before each 1-MiB read/hash chunk while retrieval requests are pending or
executing. A chunk already running may briefly overlap a new query; sustained
query traffic can delay validation indefinitely. Successful `.validation.json`
records are reused across publications and restarts, with separate checks for
copied files. Same-size local content changes are outside this integrity policy. Normal format parsing and
publication authority, replay, withdrawal and canonicality checks still apply.

Recent, Archive and transaction detail become ready before background warming
finishes. A first query can pay the runtime loading cost. Loading remains
single-flight per runtime and bounded by cache and work-memory budgets. One
background load at a time shares the cache across snapshots; a separate restore
slot remains available for foreground requests.

A background hash failure withdraws only dependent shard revisions: their
requests return 503, including responses whose evaluation was already running.
Unrelated shards remain available and immutable public maps are not rewritten.
Failures persist across restart. After a fresh successful publisher reconciliation,
the synchronization manager refetches failed artifacts under its existing TLS and
transfer budgets. Replacements are hashed by the same query-prioritized thread;
affected idle runtime entries and their persisted derivatives are evicted before
the repaired shard is re-enabled. Validation covers retained revisions as well as
the current map. A withdrawn shared file can be linked into a newer publication,
but its dependent revisions remain unavailable; other shards can advance.
Startup removes interrupted repair files while holding the consumer's exclusive
directory lock.
A shared artifact can withdraw more than one shard. `/v1/ready`, `/metrics` and
health output report pending, validated and failed artifacts and withdrawals.
Validation covers every held revision that can answer a request, including
retained predecessors; their saved failures remain enforced after restart.

Run correctness validation as a separate process after the serving API is ready.
`quality-canary --all` checks every table in its pinned fixture and each table's
segments against expected row hashes. Supply a fixture covering the assigned
history directory and page tables. Transaction detail's `txid-quality-canary`
accepts repeated `--fixture` arguments for pinned lookups covering its assigned
material. Both tools return nonzero on an invalid oracle, an unavailable query
or an incorrect answer. A failed run fails deployment acceptance; neither tool
stops or restarts the service. These probes are outside startup timing.

The startup target is under 15 seconds with the assigned files already downloaded.
Measure readiness separately from first-query loading latency and background
warming, recording cold filesystem-cache results on an isolated host. This policy
does not establish a measured fleet startup time by itself.
