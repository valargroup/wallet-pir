# Prepared history distribution and standalone provider

Status: bootstrap, recent-worker reconciliation, opt-in embedded archive routing
and standalone release roles are implemented. Independent review, release and
unchanged-application qualification remain separate gates. This interface is
separate from wallet protocols.
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
`shared/pir-pipeline` using private borrowed tokens; no phase token is persisted.
Shared boundary waiting preserves this delay-after-pass schedule and never drops
an in-progress activation. Every failed pass starts the next attempt with source
reconciliation rather than retrying an uncertain activation directly.
Only an activated snapshot supplies a conditional token.
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
`transparent/services/transparent-shard-server/tests/frozen-client`. Its isolated lock and source verifier
exclude the server's newer library pin; build and execute it through the required
repository wrapper. From the repository root, run
`python3 transparent/services/transparent-shard-server/tests/frozen-client/run-tests.py --output .local-pir/provider-qualification-<new-id>`
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

## Distribution load diagnostics

`transparent/tools/transparent-loadtest/distribution.py` runs explicit artifact
waves against the **real controller's configured HTTPS terminator**. It does not
verify native serving authority or replace the two actual provider processes.
Invoke it through the repository's development wrapper:

```sh
python3 transparent/tools/transparent-loadtest/distribution.py \
  --publisher https://approved-publisher.example --output .local-pir/distribution-10 \
  --consumers 10 --phase bootstrap
# Advance the actual producer, then reuse these same private consumer caches:
python3 transparent/tools/transparent-loadtest/distribution.py \
  --publisher https://approved-publisher.example --output .local-pir/distribution-10 \
  --consumers 10 --phase update
```

Use `--ca` for the explicitly trusted fixture/operator PEM. Repeat with 100 and
a fresh output root only after the 10-consumer campaign. Each consumer streams
one object at a time, verifies lengths/digests, checks retained objects before
reuse, and performs two conditional polls five seconds apart. Each request has
a 30-second deadline and at most eight attempts with bounded jitter/backoff.
The per-consumer body allowance is 64 GiB. Every HTTP status, partial/error body
byte count, digest, retry and timeout remains in per-consumer JSONL receipts.
Archive evaluation tables are excluded after checking their map classification.
An update must observe a producer change; an unchanged response cannot count as
an update. Reusing an output phase refuses overwriting its receipts.

Before a wave, separate control requests retain the observed current/inventory
and conservatively budget a complete candidate beside retained cache objects.
The default total cache bound is 8 GiB (`--cache-budget-bytes`); choose a larger
explicit bound only within the execution host/task's authorized disk allocation.
This affects load clients only. Publisher admission remains four artifact bodies,
shared with canonicality checks, and is never tuned by this tool. Failed client
attempts remain failures in the report even when bounded retries complete a wave.

The October 18 fixture diagnostic used server source `c50db84c`, release-fast,
the original frozen391/c085 executable `24da21b0…`, a real controller, an actual
archive owner, HTTPS forwarding and two independent strict-manager processes.
The deterministic followed journal has one archive receive and, initially, two
recent events for one script; adding one receive creates a recent page table.
The archive directory has 32,768 rows; the recent directory has 2,048 and its
page table 8,192, all at the unchanged 4,096-byte row/profile geometry.
This is not production-chain or representative full-host data.

| 10 logical consumers | Synchronized bootstrap | Ordinary update transfers/polls |
|---|---:|---:|
| Completed | 10/10 | 10/10 |
| Wave elapsed | 15.64 s | 26.57 s |
| HTTP attempts: 200 / 304 / 503 | 100 / 20 / 18 | 100 / 20 / 52 |
| Body bytes read, including errors/partials | 672,047,290 | 1,847,326,430 |
| Retries / timeouts | 18 / 0 | 52 / 0 |
| Unchanged objects reused | 0 | 20 |
| Archive evaluation requests | 0 | 0 |
| Concurrent exact frozen-client recoveries | 36/36 | 54/54 |
| Fresh-client recovery p50 / p99 | 0.362 / 0.557 s | 0.479 / 0.690 s |

These request windows sampled publisher CPU at 0.38 / 0.80 cores and publisher
RSS at most 334 / 471 MB; TLS termination used another 0.23 / 0.50 cores.
Client body throughput was 42.97 / 69.52 MB/s, excluding HTTP/TLS framing and
other provider traffic. Recovery latency includes the whole fresh client process;
the update adds a page query, so these are not matched query-p99 measurements.
The producer update construction is additional work outside the transfer window:
its recorded cycle was 10.67 seconds. Raw host/process/cgroup samples include
construction and candidate overlap. On the shared 67.42 GB host, physical
nonfree memory peaked at 55.55 GB, including up to 49.41 GB of buffers/cache;
`MemTotal - MemAvailable` peaked at 4.15 GB. These overlapping observations
include unrelated host activity. Neither sampled memory nor these short shared-host
latencies establishes the 64 GB holdings contract or the 10% p99 bound.

Raw provenance is retained outside Git in task `t-ddab1d18147e47c4` under
`.local-pir/provider-qualification/`: `binaries.json`, `helper-versions/`,
`probe-04/` (exact inputs, requests, controller cycles and 0.5-second resource
samples), and `10-consumer-audit.json`. The audit independently rehashes all
140 retained consumer objects (2,519,311,230 bytes) and compares complete frozen
client ledgers with the explicit source-event oracle. The first two distribution
phases used preserved driver `fd268532…`; the subsequent disk guard is a later
tool revision. Their 60 metadata replies were initially retained as parsed JSON;
compact reconstruction later matched every original receipt SHA (four unique
bodies), recorded in `metadata-reconstruction.json`, rather than being labelled
original raw captures. The final tool retains those exact response bytes directly.
The original journal checkpoint before its first append was not separately
captured; original immutable publication bytes and event/block prefixes remain.
Later lifecycle runs retain journal before-images. Setup/control failures remain
retained separately and do not count as passing campaigns. The final driver
also completed one real-controller integration control at revision 12, retaining
exact metadata bodies; that control is not another 10-consumer wave.

The related continuity diagnostic initially assigned the sealed 32k shard 0
locally, then moved only its ownership to the archive while the 2k tail advanced.
Its entry, source tables and manifests remained byte-identical. Both strict
managers refused retirement while the archive was unavailable, then qualified
the actual terminal owner before activation. A withheld upload observed the
128,520-byte aggregate reservation on the old map and returned its exact original
row after activation and renewed archive loss. That aggregate observation under
other traffic does not establish exclusive per-request attribution. Fresh
same-revision requests may also use retained local data; restart into the new
recent-only view establishes actual archive dependence.

After those fresh restarts, both providers returned archive setup 503 while
independent recent encrypted rows stayed exact. Deterministic reorg observations
caused recent setup 409; a higher controller revision recovered both strict
workers with zero local builds/fallbacks. Online restart, valid recent serving
during publisher loss and offline-start refusal were exercised. Retained raw
receipts are in `lifecycle-03/resume-04/`, `lifecycle-03/recovery-06/` and
`lifecycle-audit.json`. The handover interval had 124 successful restores; the
later fault campaign had 94 successes and 191 failures, including three still
unclassified failures. All successful ledgers matched a complete explicit fixture
branch, but five raw transition oracle flags remain visible. The frozen CLI
uses a map-derived StaticChain and a fresh ledger each time: these results do
not qualify independent wallet-chain acceptance or retained wallet-state sync.

The 100-consumer bootstrap was **not run**: its 18,473,074,600-byte candidate
alone, plus existing builds/evidence and later update overlap, exceeds this
task's 24 GiB disk reservation. The local guard refused before artifact requests.
Continue on existing authorized storage without changing data or geometry.
100-consumer updates, actual unchanged Mac execution, authenticated fat-LTO
release artifacts, representative 64 GB holdings, isolated matched p99 and
complete CPU30/ALL-warm180 acceptance remain separate gates in
[remaining work](remaining-work.md#independent-history-provider-qualification).

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
as described in the [release workflow](../../docs/operations.md). Fast CI and
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
python3 ops-release/ops/scripts/wallet-pir-bundle.py self-check --sha "$SHA"
python3 ops-release/tools/ci/release.py extract --sha "$SHA" --kind transparent-shard \
  --archive transparent-shard.tar.gz --output "$PWD/history-release"
python3 ops-release/ops/scripts/wallet-pir-roles.py --help
cp ops-release/transparent/ops/deploy/history-provider.example.json provider.json
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

Canonicality comes from the publisher's live `anchors/{height}/{hash}` HTTPS
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
python3 ops-release/ops/scripts/wallet-pir-roles.py --service transparent-history \
  --role provider --settings provider.json --release "$PWD/history-release" > role.json
```

The role is operator-managed: generic fleet `plan/preflight/deploy/rollback`
refuses it. That engine runs staged checks as the deploy identity before stopping
live units, which cannot safely preflight this exclusive state root. Render and
private reporting remain supported. Use the dedicated unit/user procedure below;
this does not authorize any fleet or production change.

`role.json` describes the unit, writable `directories`, and the exact loader
`check` arguments. Create only those directories, owned by a dedicated
`transparent-provider` user/group; install the rendered `unit` as
`transparent-history-provider.service` through your local service manager.
Before enabling it, run the rendered argument array as that user:

```sh
python3 -c 'import json,subprocess; r=json.load(open("role.json")); subprocess.run(["./history-release/transparent-shard-server",*r["check"]],check=True)'
```

 This real preflight downloads/verifies and
prewarms required material and checks live authority through the managed owner;
it may prime state but opens no listener. The unit repeats the same check in
`ExecStartPre` on every start/restart. Missing, corrupt or incompatible runtimes
refuse preparation/start/restart and later cache misses, without local builds.
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
