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
