# Changed-native activity candidate `c3c66b9b`, 2026-10-04

This directory holds evidence for candidate
`c3c66b9b51a5e2f4a6ac9261a941a397d119e920` and the operations path that
qualifies it. Everything here was produced on the development hub.

None of it is a native, hardware, fleet or production qualification:

- No production host was accessed.
- No artifact was staged or transferred, no candidate gate or candidate native
  program was run, and nothing was deployed.

The earlier activity record is
[activity-metadata-2026-09-30](../activity-metadata-2026-09-30/README.md); its
12ce and 80c94f32 identities stay historical.

## Files

| File | What it records |
| --- | --- |
| [build-tools.py](build-tools.py) | Driver that built only the 13 activity tools absent from the exact-head CI release bundles. It works from an immutable `git archive` export with Rust 1.97.1, `--locked`, fat-LTO `release` and `x86-64-v3` plus `pclmulqdq`. |
| [manifest.json](manifest.json) | Sanitized result of that build: source and tree, toolchain, Cargo input digests, stages and flag checks. It also records each artifact's SHA-256, ELF and maximum glibc, the argument-handling probe, CI run 37173250956's roles and the retained archive digest `d3a8f60a...`. The binaries, logs and archive stay outside Git. |
| [focused.log](focused.log) | Run on the working tree committed with this record. Focused operations suites on the candidate preparation change: 25 new candidate tests, the touched input, product and routing suites, the schema runner, host and baseline suites, and the deploy wrapper tests. All passed. |
| [mutations.log](mutations.log) | Nineteen single-guard mutations of the new checks. Each made `test_activity_candidate.py` fail, and each source was restored. |
| [upload-focused.log](upload-focused.log) | Focused suites on the guarded archive transfer change, run on the committed working tree: the new `test_activity_candidate_upload.py`, the candidate, input, preparation, product, lock, schema runner, baseline and host suites, and the deploy wrapper tests. All passed. |
| [upload-mutations.log](upload-mutations.log) | Fourteen single-guard mutations of the transfer checks. Each made `test_activity_candidate_upload.py` fail, and each source was restored. |
| [qualification-r5-focused.log](qualification-r5-focused.log) | The 78-test `test_activity_deployed_qualification.py` suite for the [deployed qualification interface](../../docs/deployment.md#deployed-candidate-qualification), the deploy wrapper tests, every `test_activity_*` suite and the documentation link check, on the committed tree. It records the module, test and wrapper digests. All passed. Evaluators ran on synthetic receipts in the native output formats. Unit effects ran against an in-memory systemd model, and continuation ran against a fake that speaks the `--scenario-worker` line protocol. No client, candidate executable, host, HTTPS origin or systemd unit was used. It supersedes `qualification-r3-focused.log` (at commit 6ef57ea549ed1fa371783bbd0bbdaa345a0db226) and `qualification-r4-focused.log` (at commit 03ca164f8caad1520e5592bf628e34f1735cf8df). |
| [qualification-r3-mutations.log](qualification-r3-mutations.log) | Fifty-five single-guard mutations covering the qualification gates, deadlines, complete owner reconciliation, escaped descendants, unit restoration, capacity exit and event checks, publication identity, freshness bounds and the continuation adapter. Each made the suite fail, and the source was restored. Earlier logs were superseded and removed. The attempt-1 `qualification-focused.log` and `qualification-mutations.log` remain at commit 66f56ee906b9878f08762d5fa06d67bc1acfc09e. The first revision's `qualification-r2-*.log` remain at a529c8fe4e28b5ca83442eccc5684f82da4ab54e. |
| [qualification-r5-mutations.log](qualification-r5-mutations.log) | Eleven single-guard mutations. Eight cover the publication-interruption pre-stop checks: the refreshed all-host survey, its bound and separate write-once file, the same-preparation recheck (digest, advancing age and unchanged active map), the remaining stop and recovery budget, and recovery timed from the stop. Three cover the refusal of an unmatched surviving service: its start ticks and unit main PID in the refusal, and no exemption for a deleted owner-root build. Each made the suite fail, and the source was restored. The fifty-five r3 mutations target guards that this change did not alter. It supersedes `qualification-r4-mutations.log` at commit 03ca164f8caad1520e5592bf628e34f1735cf8df. |
| [execution-focused.log](execution-focused.log) | Attempt 1 (3177f18c, rejected by root's review). Focused suites on the locked native gate execution change, run on the committed working tree: the new `test_activity_candidate_execution.py`, the report, upload, candidate, input, preparation, product, lock, schema runner, baseline and host suites, and the deploy wrapper tests. All passed. `make check-fast BASE=286b8c0c` also passed, running only the helper stage. |
| [execution-mutations.log](execution-mutations.log) | Attempt 1 (3177f18c, rejected). Eighteen single-guard mutations of the execution checks. Each made `test_activity_candidate_execution.py` fail, and each source was restored. |
| [execution-revision-focused.log](execution-revision-focused.log) | Attempt 2 (a790a3f3, rejected by root's review): all-host surveys under the lock, hard limits and claimed recovery. Focused suites run unprivileged on the committed tree named in the log: the execution suite (33 tests), the report, upload, candidate, input, preparation, product, lock, schema runner, baseline and host suites, and the deploy wrapper tests. All passed. `make check-fast BASE=286b8c0c` passed, running only the helper stage. |
| [execution-revision-mutations.log](execution-revision-mutations.log) | Attempt 2 (rejected). Thirty-four single-guard mutations of the revised checks. Each was applied to a scratch copy, and each made `test_activity_candidate_execution.py` fail. The source was never edited. |
| [execution-revision2-focused.log](execution-revision2-focused.log) | Attempt 3, first pass (58b63b21; root's checkpoint review found the detached-child gap): complete owner-namespace surveys with process association, the guardian deadline and lock-first recovery. Focused suites run unprivileged on the committed tree named in the log: the execution suite, the report, upload, candidate, input, preparation, product, lock, schema runner, baseline and host suites, and the deploy wrapper tests. All 42 execution tests and every other suite passed. `make check-fast BASE=286b8c0c` passed, running only the helper stage, and `git diff --check` is clean. |
| [execution-revision2-mutations.log](execution-revision2-mutations.log) | Attempt 3, first pass. Thirty-one single-guard mutations of the new survey, guardian and recovery checks, each applied to a scratch copy with the named suite classes run. Each made the suite fail. The source was never edited. |
| [execution-revision3-focused.log](execution-revision3-focused.log) | Attempt 3 after root's checkpoint review: the reusable stdlib `owner_survey` component, descendant closure, the reparented-window rule, closed operational classes and baseline unit binding. Focused suites run unprivileged on the committed tree named in the log: the execution suite (46 tests) and every other suite listed above, and the deploy wrapper tests. All passed. `make check-fast BASE=286b8c0c` and `git diff --check` results are in the log. |
| [execution-revision3-mutations.log](execution-revision3-mutations.log) | Attempt 3 after the checkpoint review. Forty-one single-guard mutations of the execution module and `owner_survey.py`, each applied to a scratch copy with the named suite classes run. Each made the suite fail. The source was never edited. |
| [execution-revision4-focused.log](execution-revision4-focused.log) | Attempt 3 after root's 0b5f9b05 checkpoint review: unfinished-owner decisions on the complete selection, a selection bound, and the complete count and digest in each reply. Focused suites run unprivileged on the committed tree named in the log: the execution suite (49 tests) and every other suite listed above, and the deploy wrapper tests. All passed. `make check-fast BASE=286b8c0c` and `git diff --check` results are in the log. |
| [execution-revision4-mutations.log](execution-revision4-mutations.log) | The same tree. Forty-five single-guard mutations of the execution module and `owner_survey.py`, including decisions on the display only, an ignored selection bound and an unverified count. Each was applied to a scratch copy with the named suite classes run, and each made the suite fail. The source was never edited. |
| [execution-revision5-focused.log](execution-revision5-focused.log) | Attempt 4 after root's af377c9b review: retained surveys keep command-line and token digests, matched class entries and flags, never argument or token values; the fixture lock holder leads its own session. Focused suites run unprivileged on the committed tree named in the log: the execution suite (50 tests), every other suite listed above and the deploy wrapper tests. All passed. The execution suite also passed with its session leader gone, root's failing condition, and in a separate cgroup scope. `make check-fast BASE=286b8c0c` and `git diff --check` results are in the log. |
| [execution-revision5-mutations.log](execution-revision5-mutations.log) | The same code. Five single-guard mutations of `owner_survey.py` that retain arguments, the token value or a matched argument, or digest an unbounded read, each applied in the working tree and then restored. Each made the suite fail. The log also records the reproduction of root's two failures with the previous fixture holder. |

## How the pieces fit

The candidate's 18 artifacts are the five CI serving roles plus these 13 tools.
`transparent/ops/lib/activity_candidate.py` pins them:

- **CI roles.** `transparent-filter-server`, `transparent-shard-server`,
  `transparent-publish-controller`, `shard-control` and `shard-assign`, from run
  37173250956.
- **Supplemental tools.** Pinned from `manifest.json` and the archive digest.

Before the reported release pins were committed, the supplemental reader was run
read-only against the retained archive. It accepted all 13 tools, their ABI and
the build result. The candidate provenance digest is `bc109c95...`. The release
`[profile.release]` at `c3c66b9b` is `lto = "fat"`, `codegen-units = 1`, and its
Cargo manifest, lock and toolchain digests equal the manifest's.

Deployment documents the [preparation and qualification
path](../../docs/deployment.md#changed-native-activity-candidate-c3c66b9b) that
root runs. Root supplies the actual candidate certificate, oracle,
artifact-verification and CI reports from retained raw results. This directory
contains no passing gate report and none may be inferred from it.

## Limitations

- **Loadability only.** The glibc 2.39 ceiling comes from the five production
  hosts' read-only ABI report. It bounds loadability only.
- **Fixture source evidence.** Fixture archives stand in for real ones, and the
  tests use fixture artifact digests. The real pins are checked against
  `manifest.json` and the prompted CI role digests.
- **Open gates.** Installed-setup agreement, warm serving, canonical recovery,
  rollback timing, freshness, sustained capacity and lifecycle all remain open,
  with unchanged floors and deadlines.


## Root bootstrap review status

The source and archive receivers now share a complete retained-inventory fleet
survey before archive reception. The coordinator holds the lock and retains a
durable request/PID/start/boot fence before surveying; worker source staging
joins that owner and rechecks local ownership under its own lock. Source transfer
uses bounded decompression with exact reviewed-code hash verification to fit
Linux's per-argument ceiling. Unknown transport outcomes require explicit owner
observation and reconciliation. Failed owners and partial bytes remain retained.

These are source assurance interfaces, not actual production passes. Root keeps
exact source hashes, Linux fixture logs and all failed checks in the private
qualification evidence directory. Prototype/load baseline disposition and the
failed already-promoted source recovery case remain separate prerequisites.
No candidate hardware, snapshot/oracle, cutover, freshness, capacity or lifecycle
acceptance follows from these fixture checks.
