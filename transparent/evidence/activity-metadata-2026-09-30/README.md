# Activity metadata implementation evidence

This directory retains observations from the v3/v11 activity metadata implementation
starting from wallet-pir `22fe04c5f5e6f781dbb697bea24850f2632907b5` and
zakura-core/wallet-libraries `908913c7c8ed69ee1a35cc001172a0800d236d3a`.

[Storage observation](storage.json) records the new 250 GiB coordinator volume and
unchanged live service state. The live node uses database format 29; the previous
reader used format 28. The transparent publisher dependency pin must therefore
track the observed running node before secondary ingestion.

Initial iteration found and corrected the obsolete maximum-entry-size assertion,
a missing schema destructuring field and a missing store codec dependency. These
failed checks do not establish passing validation. Final check outputs and exact
candidate identities are retained separately when available.

The frozen [e47bdf79 prototype](prototype-e47bdf79/publication.json) publishes
1,000 real blocks (3499739–3500738), 26,868 events, and both selected geometries.
The [independent RPC oracle](prototype-e47bdf79/oracle-all.json) matched every
block's events and transaction metadata. [Artifact verification](prototype-e47bdf79/verify.json)
reproduced both shards' filters, directories and pages exactly.

The [5 QPS gate](prototype-e47bdf79/query-5-result.json) completed 596 exact
queries in 120 seconds; the [20 QPS gate](prototype-e47bdf79/query-20-result.json)
completed 11,965 in 600 seconds across four processes. Both had zero failed
attempts, logical failures and missed slots. These are loopback HTTP candidate
measurements on the coordinator with frozen revisions, including the frozen tail;
they do not qualify canonical HTTPS or whole-wallet sustained capacity.

The first [SQLite run](prototype-e47bdf79/wallet-one.json) retained six failures
caused by a missing store directory. Its [corrected run](prototype-e47bdf79/wallet-one-retry.json)
completed 17 exact recoveries with no failures. That tool deleted each completed
database. The [retained 1/4/8-concurrency run](prototype-e47bdf79/wallet-retained-48d08a73.json)
completed 61 recoveries without failures. [Reopen comparison](prototype-e47bdf79/reopen-check-03c3c754.json)
independently decoded every retained event, matched fixture digests, and recomputed
transaction metadata, account movement, unresolved inputs and aggregate payments.
Older receives outside the bounded publication remain unresolved and partial.
No production cutover or sustained qualification is claimed.

The [library HTTP harness](prototype-e47bdf79/library-http-53af85202.json)
recovered 9 receives and 7 spends through real HTTP into library SQLite candidate
commits. Reopen preserved every fact, reader version 7 was enforced, and no
revision or account was qualified or activated. The [independent check](prototype-e47bdf79/library-independent-53af85202.json)
matched every event and its persisted metadata against retained raw block and
prevout RPC inputs, including shared funding and shielded presence. The harness
uses public-script ownership fixtures and empty shielded scan blocks; it proves
transparent delivery, not financial ownership or shielded recovery.

[Raw retention](prototype-e47bdf79/raw-retention-e24a25db.json) identifies the
226 MB immutable input bundle, retained read-only in this evidence directory and
on the coordinator. Its size exceeds GitHub's file limit, so this clone's local
`info/exclude` keeps the bundle out of Git. Preserve it before archiving the
worktree. The committed manifest pins its SHA-256 and remote recovery path.
Every RPC attempt is retained, including two global batch-size refusals that the
oracle retried. All 17 wallet HTTP attempts used filter or private PIR routes;
there was no lookup fallback, transport failure, header capture or PIR body
retention. The [raw-input oracle rerun](prototype-e47bdf79/oracle-raw-e24a25db.json)
again matched all 1,000 blocks and 26,868 journal events.

[One-hour candidate observation](prototype-e47bdf79/observe-5qps-3cfbc484.json)
completed 17,920 exact queries at 4.978 QPS with no failures, p50 5 ms and p99
33 ms. The [fat-LTO artifact build](release-3cfbc484.json) retained hashes for
all 16 required binaries/examples. [Backfill handoff](ingest-handoff-release-3cfbc484.json)
records checkpoint 269000 and the controlled move from two release-fast workers
to four fat-LTO workers. The full publication and production cutover remain open.

The bounded parent-output cache at `a1c4b809` passed the
[independent dense early-chain oracle](cache-oracle-a1c4b809.json): 100 blocks
and 83,730 events matched. The cached and uncached journals are byte-identical
([comparison](cache-comparison-a1c4b809.json)). Database lookups fell
from 51,239 to 31,987; different build profiles and rounded timing prevent a
precise speedup claim. The [fat-LTO release build](release-a1c4b809.json) passed
all three stages and retained 16 executable hashes. The
[controlled backfill handoff](ingest-handoff-cache-a1c4b809.json) records checkpoint
324000, the exact new binary hash and its source identity.

[Raw cache-oracle retention](raw-cache-retention-a1c4b809.json) pins the immutable
2.11 GB RPC input bundle, including all 486 HTTP attempts and the separate first
capture-start failure. The bundle exceeds GitHub's file limit and stays outside
Git through this clone's local exclusion, with a remote recovery path and digest.
Preserve retained raw inputs before archiving the worktree. The passed retry does
not remove the failed setup attempt from the evidence.

[Earlier full CI failure](ci-3cfbc484-failure.json) retains the v10 operator-golden
fixture mismatch and reference-wallet lint findings. The fixtures were regenerated
from the actual v11 service response, and the deploy script's jq contract check
passed against them. A named observation key and equivalent key-based sorting
resolve the three Clippy findings; the focused wallet Clippy check passed.
Repaired comprehensive CI remains a separate pending qualification gate.

The follow-up review found one remaining store lint and two wallet-library bugs:
metadata could suppress an independently known mixed local-send fee, and a crash
before export acknowledgment could omit later withdrawal reconciliation. Both
library regressions reproduced before repair. The store's equivalent key-based
sort passed all-target Clippy and the [affected check](review-store-lint-fix.json).
The library fixes retain local construction facts and durably record conservative
export intent before returning a batch; repaired-source CI remains required.

[Library review and repaired-source record](review-library-cc6656f23.json) pins
the review findings, red/green regressions and unchanged code after the doc-only
base update. PR 77 subsequently passed exact-head comprehensive CI and merged as recorded below.
[Local raw retention](raw-cache-local-retention-a1c4b809.json) confirms the 2.11 GB
independent input bundle is read-only and checksum verified. The transfer helper
reported a Git-head mismatch because documentation advanced during copying;
its exit was zero and retention was accepted against the immutable source-pinned
bundle hash, not as a code-test result.

[Pre-merge CI snapshot](library-premerge-cc6656f23.json) records every required
configuration and repository check successful at the repaired library head.
[PR 77 merge](library-merge-77.json) records main `9dabaa68`, whose tree is identical
to the verified head. The [a65c618e fat-LTO build](release-a65c618e.json) passed
and all 16 retained hashes were verified. Its comprehensive CI nevertheless
found [two further shard-cache sort lints](ci-a65c618e-lint-failure.json); the
artifact build does not establish comprehensive qualification.

[Transparent lint follow-up](review-transparent-lint-followup.json) records the
equivalent corrections and hashes of all affected source files. Strict Clippy
for the complete transparent package group and the 39.55-second affected check
passed. Comprehensive CI and fat-LTO artifacts must match the committed repair.

[Publisher deployment review](review-v11-publisher-paths.json) records an explicit
publication root and sandbox mounts for the new volume, with 31 focused tests
and ops contracts passing. The [coordinator hard-link probe](publisher-hardlink-probe.json)
passed in the planned strict sandbox; repeat it against the installed unit and
complete publication before activation. The build driver now retains the two
deployment helpers as well, making an 18-artifact release bundle. Prior
16-artifact build reports remain valid build observations, not complete
deployment bundles. Library post-merge main CI run 36787992346 also passed.

[Final 18-artifact release build](release-12ce1291.json) passed in 329.79 seconds
with all retained hashes independently verified. The preceding
[16-artifact build](release-0abce392.json) is retained separately.
[Wrapper worker preflight](wrapper-workers-preflight-12ce1291.json) passed with
an in-memory baseline of the three current units, without staging or service
changes. It records current worker disk headroom and explicitly excludes the
complete schema transaction. Recapture a durable baseline before deployment.
The replacement operating rule requires all production changes through
`ops/scripts/wallet-pir-deploy.py`, plan/preflight first, under its production
lock. The complete controller/filter/schema orchestration and coherent rollback
remain deployment gates; the earlier manual SSH procedure cannot bypass this rule.

[Schema operation focused evidence](schema-operation-focused.json) records 17
failure/recovery tests, real inherited-lock fixture coverage and 45 shared
deployment tests. The runner is a coordination foundation; production phase
programs, the recipe and wrapper-mediated source staging remain incomplete.
[Candidate comprehensive CI snapshot](ci-12ce1291-cuda-toolchain.json) records all
comprehensive check jobs passing, a failed CUDA artifact compiler guard and
pending CPU artifact preparation. The CUDA selection regression reproduced the
configuration gap and passed after explicitly pinning the recorded compiler.
This snapshot is not aggregate passing CI or deployment evidence.

[Source staging focused evidence](source-staging-focused.json) records the
immutable SSH bootstrap boundary and 30 combined schema/staging tests. Transfer,
extraction and receipt writes share one root lock owner. No production source
was staged by these fixture tests; the actual recipe, phase programs and live
cutover gates remain open.

[Coordinator source staging](source-stage-coordinator-56b67ba0.json) records the
536659-byte operations export, guarded SSH staging, all 269 retained file hashes
reverified and the staged root wrapper successfully reading journal status.
The original whole-tree archive was retained locally without transfer.
[Archive admission follow-up](source-staging-admission-followup.json) moves the
64 MiB guard before client hashing/SSH and adds PID capture to future receipts;
31 combined tests passed. Canonical service remains v10 and no schema transaction
has been applied.

[Affected-check routing follow-up](affected-routing-followup.json) records a
red/green regression: `parent` inside the product name `transparent` wrongly
narrowed generic operation checks to parent filters. A delimited-token match
restores deployment/publication consumer coverage and retains narrow parent
checks. The direct schema/staging and shared deployment tests remain independently
recorded; earlier affected checks alone did not cover every operation consumer.


[Schema baseline dependency evidence](schema-baseline-focused.json) records the
red/green publisher subprocess descriptor regression and 19 focused real-file
and process tests. Independent private copies, partial/corrupt/changed-retention
refusal, coherent active-record/configuration restoration, deferred routing,
ownership/modes and a surviving-grandchild lock are covered. These are fixtures;
actual reviewed host plans, service orchestration, SSH-descendant qualification
and the complete production cutover remain pending. The
[superseded CI cancellation record](superseded-ci-9212afee.json) confirms final
cancellation of the owned queued 56/f43 runs; cancellation is not passing evidence.

[Second coordinator source staging](source-stage-coordinator-3d08c15b.json)
records the 544153-byte reviewed operations export, receipt PID 1797420 and all
273 retained hashes reverified. It is source preparation; no schema transaction
or native fleet deployment was performed.

[Publication preparation and namespace evidence](publication-preparation-focused.json) has focused
fixture coverage: incomplete journal/guard refusal, bound plan identities,
resource floors, real child descriptor propagation, failed-stage retention,
v10 manifest rejection, separate v11 publisher config/state, worker active/cache
namespace replacement and transfer corruption rejection. The complete service
phase programs and recipe remain open. The job is not started against an active
journal writer. The repaired comprehensive CI at `aa16dc24` passed all jobs;
later exact-head CI remains pending.
The batched affected check passed in 13.717 seconds, including 39 combined
schema/staging/preparation tests, 33 publisher/fleet tests, 13 worker upgrade and
namespace tests, 19 baseline tests and 45 shared deployment tests. A first
constructor-fixture failure and a cancelled overly broad local selection remain
retained; neither is counted as passing evidence. Registering preparation through
the existing schema suite avoids a central Makefile change selecting all Rust
packages. Native inputs remain unchanged at `12ce1291`.

[Third coordinator source staging](source-stage-coordinator-6e8337fa.json) records
wrapper plan/preflight/stage/status for the 554361-byte operations export,
receipt PID 1827147 and all 275 retained hashes reverified. Read-only publication
planning succeeded; preflight correctly refused the unfinished ingest owner.
No publication job, binary/fleet stage or schema transaction was started.

[Product host transition evidence](schema-host-transitions-focused.json) records
concrete writer quiescence, private
baseline capture, atomic product installation, displaced stale unit drop-ins,
separate v11 config/control/cache/assignment binding and warm v10 recovery with
routing/load/scaler deferred. Twenty new host tests and 59 combined
schema/staging/preparation/host tests passed. Actual reviewed host plans, pinned
remote ownership, SSH descendant qualification, complete coordinated withdrawal,
prewarm/alignment/verification/reopen and qualification remain open. Fixture
success cannot authorize public reopening or establish a production baseline.
The final affected check passed in 13.257 seconds. A red/green ordering
regression and the initial fixture isolation failure are retained. The earlier
13.189-second passing check predates the corrections and is not accepted for the
final inputs. Capture preserves original router routes and warm worker state;
staging stops all replaced services, including the filter, before activation.

[Routing and reference recovery evidence](schema-routing-recovery-focused.json)
records 20 new routing tests, seven real SQLite/native-report tests and 87
combined schema/staging/preparation/host/routing/recovery tests. The affected
final check passed in 13.693 seconds against exact retained implementation hashes.
The first 13.534-second
check predates the additional running-assignment scope review and is superseded.
Three red/green regressions reproduce same-chain history rewrite and authority
startup without a maintenance fence. A corrected fixture API error is retained
separately and is not passing evidence. These are fixtures, without any live
routing or service transition.

The concrete dependencies guard both public origins, preserve a loopback-only
private verification relay, check every worker and accepted retained anchor,
verify actual manifest bytes and reject sealed-history changes. Nonempty private
and canonical HTTPS native recoveries independently reopen SQLite and refuse
unresolved effects, pending work and missing classes. Reopening failures restore
withdrawal. Old v10 metadata remains unavailable. Complete reviewed plans, remote
lock/owner dispatch, initial controller/fleet records and the cutover recipe are
still required. No production or sustained qualification is inferred.

[Completed journal and publication launch](full-ingestion-publication-start.json)
records the terminal v3 genesis-through-3500738 checkpoint and passing independent
ingestion guard. Wrapper plan/preflight/start/status used the reviewed staged
`6e8337fa` operations and all 18 frozen `12ce1291` fat-LTO hashes. The new owner
PID 1876447 is running cutoff/publication/verification preparation under its
production lock; cutoff passed and publication was active at the observation.
Starting preparation establishes no publication completion, canonical cutover
or qualification. Two local prelaunch evidence/parser failures are retained;
both occurred before start, and no duplicate job was launched.

The running-worker scope review binds the complete canonical native assignment
digest, worker identity/role and assigned shard count. A read-only comparison
against the coordinator's actual v10 assignment matched its installed native
serializer exactly; binary and raw evidence hashes are retained. An additional
public-snapshot fence rejects a newer public map that has not passed the final
all-worker observation. Earlier passing checks predate those fixes and are
explicitly excluded from final-input evidence.
